/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.delta.ranger

import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.spark.sql.execution.QueryExecution
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendTransactionSource {
  @Test
  def realNamedFrontendsPrepareActualInputInOneTransaction(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      attempt("frontend-" + frontend, frontend, "SELECT id, secret, region FROM b.db.t") { e =>
        assertNull(e.failure)
        assertEquals(1, e.transactions.size)
        assertTrue(e.scalarSeen)
        assertEquals(1, e.sourceEntries)
        assertTrue(e.sourceActions > 0)
        assertTrue(e.targetActions > 0)
        assertActualPreparedInput(e)
      }
    }
  }

  @Test
  def nativeReadTrackingKeepsSameLogAndCrossLogBranches(): Unit = {
    val cases = Vector(
      ("self", "SELECT id, secret, region FROM a.db.t WHERE id = 1", 1, true, false),
      (
        "join",
        "SELECT l.id, l.secret, l.region FROM a.db.t l JOIN a.db.t r ON l.id = r.id",
        2,
        true,
        false
      ),
      ("cross", "SELECT id, secret, region FROM b.db.t", 1, false, false),
      (
        "mixed",
        "SELECT l.id, l.secret, l.region FROM a.db.t l JOIN b.db.t r ON l.id = r.id",
        2,
        true,
        false
      ),
      ("no-skipping", "SELECT id, secret, region FROM a.db.t", 1, true, true),
      ("empty", "SELECT id, secret, region FROM a.db.t WHERE false", 1, false, false)
    )
    cases.foreach { case (name, sql, occurrences, sameLog, noSkipping) =>
      attempt(
        name,
        "insertInto",
        sql,
        noSkipping = noSkipping
      ) { e =>
        assertNull(e.failure)
        assertEquals(1, e.transactions.size)
        assertTrue(e.scalarSeen)
        assertActualPreparedInput(e)
        assertEquals(occurrences, e.input.receipt.obligations.size)
        val transaction = e.transactions.head
        e.prepared.occurrences.foreach { occurrence =>
          occurrence.index match {
            case index: PreparedDeltaFileIndex =>
              val snapshot = index.preparedScan.scannedSnapshot
              val sameLog = occurrence.obligation.binding.log() eq transaction.deltaLog
              if (sameLog) assertSame(transaction.snapshot, snapshot)
              else assertNotSame(transaction.snapshot, snapshot)
              assertSame(occurrence.obligation.binding.log(), snapshot.deltaLog)
              e.trace.record(
                "snapshot-lineage",
                s"sameLog=$sameLog snapshot=${e.trace.id(snapshot)} targetSnapshot=${e.trace.id(transaction.snapshot)} originalLog=${e.trace.id(occurrence.obligation.binding.log())} snapshotLog=${e.trace.id(snapshot.deltaLog)}"
              )
            case _: DeltaRangerLogIndex => assertTrue(noSkipping)
            case _                      => fail("Unexpected issued native index")
          }
        }
        if (sameLog && !noSkipping) assertTrue(e.generatorObserved)
        assertTrue(e.before.nonEmpty)
        assertTrue(e.after.nonEmpty)
        if (noSkipping) {
          assertFalse(e.before.get.whole)
          assertTrue(e.after.get.whole)
          assertTrue(e.after.get.predicates.size > e.before.get.predicates.size)
        } else if (sameLog) {
          assertTrue(e.after.get.predicates.size > e.before.get.predicates.size)
          assertFalse(e.after.get.files.isEmpty)
          assertTrue(e.after.get.files.subsetOf(e.originalFiles))
          if (name == "self") {
            assertFalse(e.expectedSelectiveFiles.isEmpty)
            assertEquals(e.expectedSelectiveFiles, e.after.get.files)
            assertTrue(e.after.get.predicates.exists(_.contains(" = 1)")))
          }
        } else {
          assertEquals(e.before.get.files, e.after.get.files)
          assertEquals(e.before.get.predicates, e.after.get.predicates)
        }
        e.trace.record(
          "native-read-delta",
          s"beforePredicates=${e.before.get.predicates.size} afterPredicates=${e.after.get.predicates.size} beforeFiles=${e.before.get.files.toVector.sorted.mkString(",")} afterFiles=${e.after.get.files.toVector.sorted.mkString(",")} beforeWhole=${e.before.get.whole} afterWhole=${e.after.get.whole} expectedSelective=${e.expectedSelectiveFiles.toVector.sorted.mkString(",")} originalFiles=${e.originalFiles.toVector.sorted.mkString(",")}"
        )
        assertEquals(if (name == "empty") 0 else occurrences, e.prepared.occurrences.size)
        assertReason(
          assertThrows(
            classOf[SecurityException],
            () => e.prepared.verify(e.input.queryExecution.optimizedPlan, e.spark)
          ),
          "expired source owner"
        )
        assertReason(
          assertThrows(classOf[SecurityException], () => e.input.requireLive()),
          "expired actual input lease"
        )
      }
    }
  }

  @Test
  def seededDuplicatesRevalidateBothActionsWithoutPreparingInput(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      attempt(
        "duplicate-" + frontend,
        frontend,
        "SELECT id, secret, region FROM b.db.t",
        duplicate = true
      ) { e =>
        assertNull(e.failure)
        assertEquals(1, e.transactions.size)
        assertTrue(e.sourceActions > 0)
        assertTrue(e.targetActions > 0)
        assertNull(e.input)
        assertNull(e.prepared)
        assertEquals(0, e.sourceEntries)
        assertEquals(0, e.helpers.size())
        assertEquals(0, e.commits.size())
        assertTrue(e.scalarSeen)
      }
    }
  }

  @Test
  def changedAuthorityAfterScalarRejectsBothOutcomesBeforeSourceEntry(): Unit = {
    for (
      duplicate <- Seq(false, true);
      change <- Seq("source", "target", "mask", "filter", "publication")
    ) {
      val expected = change match {
        case "source" | "target" => "Delta Ranger access denied"
        case "mask" | "filter"   => "action decision changed"
        case "publication"       => "publication changed"
      }
      attempt(
        s"authority-$duplicate-$change",
        "insertInto",
        "SELECT id, secret, region FROM b.db.t",
        duplicate = duplicate,
        mutate = (e, phase) =>
          if (phase == "after-scalar" && !e.mutationFired) {
            e.mutationFired = true
            change match {
              case "source" =>
                e.source.denied.add("b:id"); e.restore = () => e.source.denied.clear()
              case "target" =>
                e.target.denied.add("a:*"); e.restore = () => e.target.denied.clear()
              case "mask" =>
                val previous = e.source.mask; e.source.mask = "'changed'";
                e.restore = () => e.source.mask = previous
              case "filter" =>
                val previous = e.source.filter; e.source.filter = "id = 1";
                e.restore = () => e.source.filter = previous
              case "publication" => e.source.start()
            }
          }
      ) { e =>
        assertTrue(e.mutationFired)
        assertReason(e.failure, expected)
        assertEquals(1, e.transactions.size)
        assertEquals(0, e.sourceEntries)
        assertNull(e.prepared)
      }
    }
  }

  @Test
  def ownedPreparationRejectsContextChangesAndKeepsPrimaryFailure(): Unit = {
    for (
      phase <- Seq("transaction", "source-entry", "source-issued");
      change <- Seq("settings", "publication", "exception")
    ) {
      val injected = new IllegalStateException("injected owned phase " + phase)
      attempt(
        s"context-$phase-$change",
        "insertInto",
        "SELECT id, secret, region FROM b.db.t",
        mutate = (e, at) =>
          if (at == phase && !e.mutationFired) {
            e.mutationFired = true
            change match {
              case "settings" =>
                val prior = e.spark.conf.get("spark.sql.shuffle.partitions")
                e.restore = () => e.spark.conf.set("spark.sql.shuffle.partitions", prior)
                e.spark.conf.set("spark.sql.shuffle.partitions", "19")
              case "publication" => e.source.start()
              case "exception"   => throw injected
            }
          }
      ) { e =>
        assertTrue(e.mutationFired)
        change match {
          case "settings" if phase == "transaction" =>
            assertEquals("Native Delta registration binding is unavailable", e.failure.getMessage)
          case "settings"    => assertReason(e.failure, "configuration changed")
          case "publication" => assertReason(e.failure, "publication changed")
          case "exception" if phase == "transaction" => assertSame(injected, e.failure)
          // The existing metadata reader deliberately conceals provider exceptions.
          case "exception" =>
            assertEquals("Published registration metadata is unavailable", e.failure.getMessage)
        }
        if (phase != "source-issued") assertNull(e.prepared)
      }
    }
  }

  @Test
  def distinctAndBorrowedTrackerQueriesCannotTakeOverActualInput(): Unit = {
    Seq(false, true).foreach { borrowed =>
      var nested: Throwable = null
      attempt(
        "tracker-" + borrowed,
        "insertInto",
        "SELECT id, secret, region FROM b.db.t",
        mutate = (e, phase) =>
          if (phase == "source-entry" && !e.mutationFired) {
            e.mutationFired = true
            val tracker =
              if (borrowed) e.input.queryExecution.tracker
              else new QueryPlanningTracker
            val foreign = new QueryExecution(e.spark, e.input.analyzed, tracker)
            try foreign.optimizedPlan
            catch { case failure: Throwable => nested = failure }
          }
      ) { e =>
        assertTrue(e.mutationFired)
        if (borrowed) {
          assertReason(e.failure, "source preparation claim changed")
        } else {
          assertNotNull(nested)
          assertReason(nested, "source planning tracker changed")
          assertNull(e.failure)
        }
      }
    }
  }

  @Test
  def sourceOwnerRejectsForeignTransactionAndPhysicalTagCopies(): Unit = {
    attempt(
      "owned-negatives",
      "saveAsTable",
      "SELECT id, secret, region FROM b.db.t",
      mutate = (e, phase) =>
        if (phase == "source-issued" && !e.mutationFired) {
          e.mutationFired = true
          assertReason(
            assertThrows(classOf[SecurityException], () => e.sourceOwner.requireGenerator(null)),
            "foreign source transaction generator"
          )
          val active = OptimisticTransaction.getActive.get
          val foreign =
            new OptimisticTransaction(active.deltaLog, active.catalogTable, active.snapshot)
          assertReason(
            assertThrows(classOf[SecurityException], () => e.sourceOwner.requireGenerator(foreign)),
            "foreign source transaction generator"
          )
          OptimisticTransaction.withActive(foreign, overrideExistingTransaction = true) {
            assertReason(
              assertThrows(classOf[SecurityException], () => e.sourceOwner.requireLive()),
              "Native Delta registration binding is unavailable"
            )
          }
          assertSame(active, OptimisticTransaction.getActive.get)
          val copied = e.input.analyzed.clone()
          copied.setTagValue(
            TreeNodeTag[DeltaRangerNativeScans.Reference](
              "gravitino.delta.ranger.native-preparation"
            ),
            new DeltaRangerNativeScans.Reference(e.prepared)
          )
          // Copying valid evidence cannot turn preparation-only indexes into SELECT authority.
          assertReason(
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerPhysicalProvenance.user(null, copied, e.prepared, e.spark)
            ),
            "preparation-only"
          )
        }
    ) { e =>
      assertTrue(e.mutationFired)
      assertNull(e.failure)
    }
  }

  @Test
  def resetTrueRejectsBeforeTransactionScalarAndPreparation(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      attempt(
        "reset-" + frontend,
        frontend,
        "SELECT id, secret, region FROM b.db.t",
        reset = true
      ) { e =>
        assertEquals("Native idempotency entry rejected", e.failure.getMessage)
        assertTrue(e.transactions.isEmpty)
        assertFalse(e.scalarSeen)
        assertEquals(0, e.sourceEntries)
      }
    }
  }

  @Test
  def preparationRetainsSessionGroupsRegistrationAndLifetime(): Unit = {
    Seq(
      "session",
      "groups",
      "source-registration",
      "target-registration",
      "closed-source",
      "closed-input"
    ).foreach { change =>
      attempt(
        "lifetime-" + change,
        "insertInto",
        "SELECT id, secret, region FROM b.db.t",
        mutate = (e, phase) =>
          if (phase == "source-entry" && !e.mutationFired) {
            e.mutationFired = true
            change match {
              case "session" =>
                e.restore = () => SparkSession.setActiveSession(e.spark)
                SparkSession.clearActiveSession()
              case "groups" =>
                val user = UserGroupInformation.getCurrentUser
                val groups = user.getGroupNames
                e.restore = () => {
                  UserGroupInformation.createUserForTesting(user.getShortUserName, groups); ()
                }
                UserGroupInformation.createUserForTesting(
                  user.getShortUserName,
                  Array("changed-owned-source")
                )
              case "source-registration" | "target-registration" =>
                val metadata = NativeSelectFixture.bindingFixture(
                  e.spark,
                  if (change.startsWith("source")) "b" else "a"
                )
                val prior = new java.util.LinkedHashMap[String, String](metadata.properties)
                e.restore = () => { metadata.properties.clear(); metadata.properties.putAll(prior) }
                metadata.properties.clear()
              case "closed-source" => e.sourceOwner.close()
              case "closed-input"  => e.input.close()
            }
          }
      ) { e =>
        assertTrue(e.mutationFired)
        val expected = change match {
          case "session" => "session changed"
          case "groups"  => "subject changed"
          case "source-registration" | "target-registration" =>
            "Published registration metadata is unavailable"
          case "closed-source" => "expired source owner"
          case "closed-input"  => "expired actual input lease"
        }
        assertReason(e.failure, expected)
        assertNull(e.prepared)
      }
    }
  }

  @Test
  def retainedInputCannotBeUsedFromAnotherThreadOrReentered(): Unit = {
    attempt(
      "thread-and-reentry",
      "insertInto",
      "SELECT id, secret, region FROM b.db.t",
      mutate = (e, phase) =>
        if (phase == "source-issued" && !e.mutationFired) {
          e.mutationFired = true
          val result = new LinkedBlockingQueue[Throwable]()
          val thread = new Thread(() => {
            SparkSession.setActiveSession(e.spark)
            try e.input.requireLive()
            catch { case failure: Throwable => result.add(failure) }
            finally SparkSession.clearActiveSession()
          })
          thread.start()
          val failure = result.poll(10, TimeUnit.SECONDS)
          thread.join(10000)
          assertNotNull(failure)
          assertReason(failure, "expired actual input lease")
          assertReason(
            assertThrows(classOf[SecurityException], () => e.input.association.coordinate(e.input)),
            "active native"
          )
        }
    ) { e =>
      assertTrue(e.mutationFired)
      assertNull(e.failure)
    }
  }

  @Test
  def aClaimCannotAcceptForeignPreparedEvidence(): Unit = {
    attempt(
      "foreign-prepared",
      "insertInto",
      "SELECT id, secret, region FROM b.db.t",
      mutate = (e, phase) =>
        if (
          phase == "source-entry" && !e.mutationFired &&
          field(e.sourceOwner, "claimed").asInstanceOf[java.lang.Boolean].booleanValue()
        ) {
          e.mutationFired = true
          val foreign =
            new DeltaRangerNativeScans.Prepared(null, null, Vector.empty, Nil, None, null)
          assertReason(
            assertThrows(classOf[SecurityException], () => e.sourceOwner.complete(foreign)),
            "source preparation issuance changed"
          )
        }
    ) { e =>
      assertTrue(e.mutationFired)
      assertNull(e.failure)
    }
  }

  @Test
  def publicationCallbacksCannotCloseTheLeaseDuringItsOwnValidation(): Unit = {
    Seq("input", "source").foreach { kind =>
      var closing = false
      attempt(
        "close-during-validation-" + kind,
        "insertInto",
        "SELECT id, secret, region FROM b.db.t",
        mutate = (e, phase) =>
          if (phase == "source-issued") {
            if (closing) {
              if (kind == "input") e.input.close() else e.sourceOwner.close()
            } else if (!e.mutationFired) {
              e.mutationFired = true
              closing = true
              try {
                val failure = assertThrows(
                  classOf[SecurityException],
                  () => {
                    if (kind == "input") e.input.requireLive() else e.sourceOwner.requireLive()
                  }
                )
                assertReason(
                  failure,
                  if (kind == "input") "expired actual input lease" else "expired source owner"
                )
              } finally closing = false
            }
          }
      ) { e =>
        assertTrue(e.mutationFired)
        assertReason(
          e.failure,
          if (kind == "input") "expired actual input lease" else "expired source owner"
        )
      }
    }
  }

  @Test
  def cachedActualQueryExecutionCannotSatisfyANewPreparationClaim(): Unit = {
    attempt(
      "cached-live-qe",
      "insertInto",
      "SELECT id, secret, region FROM b.db.t",
      mutate = (e, phase) =>
        if (phase == "after-scalar" && e.prepared != null && !e.mutationFired) {
          e.mutationFired = true
          assertNull(DeltaRangerNativeOperation.current())
          val originalSource = e.sourceOwner
          val constructor =
            classOf[DeltaRangerAppendCoordinator.Source].getDeclaredConstructors.head
          constructor.setAccessible(true)
          val claim = constructor
            .newInstance(e.input, field(originalSource, "transactionOwner"), e.transactions.head)
            .asInstanceOf[DeltaRangerAppendCoordinator.Source]
          try {
            val failure = assertThrows(classOf[SecurityException], () => claim.prepare())
            assertEquals(
              "Delta append source rejected: actual input preparation evidence changed",
              failure.getMessage
            )
            assertFalse(field(claim, "claimed").asInstanceOf[java.lang.Boolean].booleanValue())
            assertNull(field(claim, "issued"))
            e.trace.record(
              "cached-claim-rejection",
              s"sameQE=true qe=${e.trace.id(e.input.queryExecution)} newlyClaimed=false newlyIssued=false diagnostic=actual-input-preparation-evidence-changed"
            )
          } finally {
            claim.close()
            e.sourceOwner = originalSource
          }
        }
    ) { e =>
      assertTrue(e.mutationFired)
      assertNull(e.failure)
      assertActualPreparedInput(e)
    }
  }

  @Test
  def actualRetainedDatasetCollectionIsDeniedWhileTransactionIsLive(): Unit = {
    attempt(
      "actual-input-collect",
      "saveAsTable",
      "SELECT id, secret, region FROM b.db.t",
      mutate = (e, phase) =>
        if (phase == "after-scalar" && e.prepared != null && !e.mutationFired) {
          e.mutationFired = true
          assertNull(DeltaRangerNativeOperation.current())
          val failure = assertThrows(classOf[Exception], () => { e.data.collect(); () })
          val causes =
            Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
          val denial = causes.find(_.isInstanceOf[SecurityException]).getOrElse(throw failure)
          assertEquals("Delta append source is preparation-only", denial.getMessage)
          assertTrue(
            denial.getStackTrace.exists(f =>
              f.getClassName.contains(
                "DeltaRangerPhysicalProvenance"
              ) && f.getMethodName == "capture"
            )
          )
          assertSame(e.transactions.head, OptimisticTransaction.getActive.get)
          assertNull(DeltaRangerNativeOperation.current())
          e.trace.record(
            "actual-collection-denied",
            s"input=${e.trace.id(e.data)} qe=${e.trace.id(e.input.queryExecution)} txn=${e.trace.id(e.transactions.head)} diagnostic=preparation-only firstBoundary=physical-capture transactionStillActive=true noNative=true"
          )
          e.trace.record("actual-collection-stack", denial.getStackTrace.mkString("\n"))
        }
    ) { e =>
      assertTrue(e.mutationFired)
      assertNull(e.failure)
      assertActualPreparedInput(e)
    }
  }

  private def assertActualPreparedInput(e: Evidence): Unit = {
    assertNotNull(e.input)
    assertNotNull(e.prepared)
    assertSame(e.spark, e.data.sparkSession)
    assertSame(e.spark, e.input.queryExecution.sparkSession)
    assertSame(e.input.queryExecution, e.data.queryExecution)
    assertSame(e.input.analyzed, e.data.queryExecution.analyzed)
    assertSame(e.input.receipt.obligations, e.prepared.owner.obligations)
    assertSame(e.prepared, DeltaRangerNativeScans.prepared(e.input.queryExecution.optimizedPlan))
    e.trace.record(
      "actual-return",
      s"samePrepared=true prepared=${e.trace.id(e.prepared)} input=${e.trace.id(e.data)} qe=${e.trace.id(e.input.queryExecution)} receipt=${e.trace.id(e.input.receipt)} txn=${e.trace.id(e.transactions.head)} originalOccurrences=${e.input.receipt.obligations.size} preparedOccurrences=${e.prepared.occurrences.size}"
    )
  }

  private case class Reads(predicates: Vector[String], files: Set[String], whole: Boolean)
  private class Evidence(val spark: SparkSession, val trace: InstalledAppendBoundaryProbe.Trace) {
    var failure: Throwable = _
    var returned = false
    var assertionsPassed = false
    var failures = Vector.empty[Throwable]
    var input: DeltaRangerAppendV1Input.RetainedV1Input = _
    var data: Dataset[Row] = _
    var prepared: DeltaRangerNativeScans.Prepared = _
    var before: Option[Reads] = None
    var after: Option[Reads] = None
    var sourceActions = 0
    var targetActions = 0
    var sourceEntries = 0
    var generatorObserved = false
    var scalarSeen = false
    var mutationFired = false
    var sourceOwner: DeltaRangerAppendCoordinator.Source = _
    var target: TestDeltaRangerNativeSelect.Fixture = _
    var source: TestDeltaRangerNativeSelect.Fixture = _
    var restore: () => Unit = () => ()
    var originalFiles = Set.empty[String]
    var expectedSelectiveFiles = Set.empty[String]
    val helpers = new java.util.IdentityHashMap[DeltaRangerAppendHelper, java.lang.Boolean]()
    val commits = new java.util.IdentityHashMap[DeltaRangerAppendCommit, java.lang.Boolean]()
    val transactions = scala.collection.mutable.ArrayBuffer.empty[OptimisticTransaction]
  }

  private def field(value: AnyRef, suffix: String): AnyRef = {
    val classes = Iterator.iterate[Class[_]](value.getClass)(_.getSuperclass).takeWhile(_ != null)
    val found = classes
      .flatMap(_.getDeclaredFields)
      .find(f => f.getName == suffix || f.getName.endsWith("$$" + suffix))
      .getOrElse(throw new AssertionError("Missing native field: " + suffix))
    found.setAccessible(true)
    found.get(value)
  }

  private def readState(transaction: OptimisticTransaction): Reads = {
    val predicates = field(transaction, "readPredicates")
      .asInstanceOf[java.util.Collection[Any]]
      .asScala
      .map(_.toString)
      .toVector
    val files = field(transaction, "readFiles")
      .asInstanceOf[scala.collection.Iterable[AddFile]]
      .map(_.path)
      .toSet
    Reads(
      predicates,
      files,
      field(transaction, "readTheWholeTable").asInstanceOf[java.lang.Boolean].booleanValue()
    )
  }

  private def assertReason(failure: Throwable, reason: String): Unit =
    assertTrue(
      Iterator
        .iterate(failure)(_.getCause)
        .takeWhile(_ != null)
        .exists(t => Option(t.getMessage).exists(_.contains(reason))),
      failure.toString
    )

  private def attempt(
      name: String,
      frontend: String,
      sql: String,
      duplicate: Boolean = false,
      noSkipping: Boolean = false,
      reset: Boolean = false,
      mutate: (Evidence, String) => Unit = (_, _) => ()
  )(check: Evidence => Unit): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("transaction-source-" + name)
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    val previous = NativeSelectFixture.catalogPolicies
    val previousPolicy = NativeSelectFixture.policy
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    var originalFiles = Set.empty[String]
    var expectedSelectiveFiles = Set.empty[String]
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set(
            "spark.databricks.delta.write.txnAppId",
            if (duplicate) "seeded-app" else "fresh-app"
          )
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          trace.activate(spark)
          val e = new Evidence(spark, trace)
          e.target = target
          e.source = source
          e.originalFiles = originalFiles
          e.expectedSelectiveFiles = expectedSelectiveFiles
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          Files.createDirectories(
            Paths.get(new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")))
          )
          val before =
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
          val objects = inventory(root)
          Files.write(
            trace.directory.resolve("before-objects.txt"),
            objects.mkString("\n").getBytes(StandardCharsets.UTF_8)
          )
          spark.sessionState.analyzer.batches
          val settings = spark.sessionState.conf.getAllConfs.toMap
          Files.write(
            trace.directory.resolve("before-settings.txt"),
            settings.toVector.sorted.mkString("\n").getBytes(StandardCharsets.UTF_8)
          )
          def policy(access: String): Unit = OptimisticTransaction.getActive.foreach { txn =>
            if (!e.transactions.exists(_ eq txn)) {
              e.transactions += txn
              trace.record(
                "transaction-observed",
                s"txn=${trace.id(txn)} log=${trace.id(txn.deltaLog)} snapshot=${trace.id(txn.snapshot)} descriptor=${trace.id(txn.catalogTable.get)} session=${trace.id(spark)} nativeScope=${Option(DeltaRangerNativeOperation.current()).map(trace.id).getOrElse("none")}"
              )
            }
            if (access == "update") e.targetActions += 1
            if (access == "select") e.sourceActions += 1
            mutate(e, if (e.scalarSeen) "after-scalar" else "transaction")
          }
          target.onRequest = request => policy(request.getAccessType)
          source.onRequest = request => policy(request.getAccessType)
          val metadata = Seq("a", "b").map(NativeSelectFixture.bindingFixture(spark, _))
          val preparing = field(DeltaRangerAppendCoordinator, "preparing")
            .asInstanceOf[ThreadLocal[DeltaRangerAppendCoordinator.Source]]
          def observe(): Unit = {
            val helper = field(DeltaRangerAppendHelper, "active")
              .asInstanceOf[ThreadLocal[DeltaRangerAppendHelper]]
              .get()
            if (helper != null) e.helpers.put(helper, true)
            DeltaRangerAppendCommit.current.foreach(e.commits.put(_, true))
            val operation = DeltaRangerNativeOperation.current()
            if (
              operation != null && Thread
                .currentThread()
                .getStackTrace
                .exists(_.getMethodName == "readInTransaction")
            ) {
              if (!e.scalarSeen)
                trace.record(
                  "scalar-entry",
                  s"txn=${trace.id(OptimisticTransaction.getActive.get)} operation=${trace.id(operation)}"
                )
              e.scalarSeen = true
            }
            val owner = preparing.get()
            if (owner != null) {
              e.sourceOwner = owner
              if (
                !e.generatorObserved && Thread
                  .currentThread()
                  .getStackTrace
                  .exists(_.getMethodName == "getDeltaScanGenerator") && Thread
                  .currentThread()
                  .getStackTrace
                  .exists(f =>
                    f.getClassName.endsWith(
                      "DeltaRangerAppendCoordinator$Source"
                    ) && f.getMethodName == "requireGenerator"
                  )
              ) {
                val retained = field(owner, "transaction").asInstanceOf[OptimisticTransaction]
                assertSame(retained, OptimisticTransaction.getActive.get)
                e.generatorObserved = true
                trace.record(
                  "transaction-generator-check",
                  s"sameActive=true retainedTxn=${trace.id(retained)} activeTxn=${trace.id(OptimisticTransaction.getActive.get)} observation=metadata-callback-inside-production-identity-check"
                )
              }
              if (e.input == null) {
                e.sourceEntries += 1
                e.input =
                  field(owner, "input").asInstanceOf[DeltaRangerAppendV1Input.RetainedV1Input]
                e.data = field(e.input, "data").asInstanceOf[Dataset[Row]]
                e.before = Some(readState(OptimisticTransaction.getActive.get))
                trace.record(
                  "source-entry",
                  s"input=${trace.id(e.data)} qe=${trace.id(e.input.queryExecution)} tracker=${trace.id(e.input.queryExecution.tracker)} receipt=${trace.id(e.input.receipt)} txn=${trace.id(OptimisticTransaction.getActive.get)} originalOccurrences=${e.input.receipt.obligations.size}"
                )
              }
              mutate(e, "source-entry")
              val issued = field(owner, "issued").asInstanceOf[DeltaRangerNativeScans.Prepared]
              if (issued != null) {
                e.prepared = issued
                e.after = Some(readState(OptimisticTransaction.getActive.get))
                mutate(e, "source-issued")
                assertReason(
                  assertThrows(
                    classOf[SecurityException],
                    () => DeltaRangerPhysicalProvenance.user(null, null, issued, spark)
                  ),
                  "preparation-only"
                )
              }
            }
          }
          metadata.foreach(_.onRead = _ => observe())
          try {
            val data = spark.sql(sql)
            trace.source(
              DeltaRangerDecisionReceipt.consumeOwned(
                data.queryExecution.analyzed,
                spark,
                optimized = false
              )
            )
            try {
              if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
              else data.write.mode("append").saveAsTable("a.db.t")
              e.returned = true
              trace.record("write-return", "success=true")
            } catch {
              case failure: Exception =>
                val causes =
                  Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
                e.failure = causes.last
                e.failures = causes
                trace.record("failure", causes.mkString(" | "))
                trace.record(
                  "failure-stack",
                  causes.map(_.getStackTrace.mkString("\n")).mkString("\nCaused by:\n")
                )
            }
            trace.record(
              "read-state",
              s"before=${e.before} after=${e.after} expectedSelective=$expectedSelectiveFiles originalFiles=$originalFiles"
            )
            trace.record(
              "phase-evidence",
              s"transactions=${e.transactions.map(trace.id).mkString(",")} scalarSeen=${e.scalarSeen} sourceEntries=${e.sourceEntries} sourceActions=${e.sourceActions} targetActions=${e.targetActions} helperOwners=${e.helpers.size()} commitOwners=${e.commits.size()}"
            )
            trace.requests(target, "target-policy-requests")
            trace.requests(source, "source-policy-requests")
            check(e)
            e.assertionsPassed = true
            trace.record("case-complete", name)
          } finally {
            metadata.foreach(_.onRead = _ => ())
            e.restore()
            target.onRequest = _ => ()
            source.onRequest = _ => ()
            assertTrue(spark.sql("SELECT id FROM b.db.t").collect().nonEmpty)
            trace.record("healthy-select", "rowsPresent=true")
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            val committed = e.returned && !duplicate
            if (committed) {
              assertNotEquals(before, after)
              val transaction = e.transactions.head
              val published = transaction.deltaLog.unsafeVolatileSnapshot
              assertEquals(transaction.readVersion + 1, published.version)
              assertEquals(transaction.snapshot.metadata, published.metadata)
              assertEquals(transaction.snapshot.protocol, published.protocol)
              trace.record(
                "native-state",
                s"beforeVersion=${transaction.readVersion} afterVersion=${published.version}"
              )
            } else assertEquals(before, after)
            val afterObjects = inventory(root)
            Files.write(
              trace.directory.resolve("after-objects.txt"),
              afterObjects.mkString("\n").getBytes(StandardCharsets.UTF_8)
            )
            Files.write(
              trace.directory.resolve("after-settings.txt"),
              spark.sessionState.conf.getAllConfs.toVector.sorted
                .mkString("\n")
                .getBytes(StandardCharsets.UTF_8)
            )
            if (!committed) assertEquals(objects, afterObjects)
            val expectedSettings =
              if (committed)
                settings + ("spark.databricks.delta.lastCommitVersionInSession" -> (e.transactions.head.readVersion + 1).toString)
              else settings
            assertEquals(expectedSettings, spark.sessionState.conf.getAllConfs.toMap)
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            assertNull(DeltaRangerNativeOperation.current())
            assertNull(preparing.get())
            trace.record(
              "cleanup",
              s"noTxn=true noNative=true noSource=true fullSQLConfEqual=${settings == spark.sessionState.conf.getAllConfs.toMap} allFilesEqual=${before == after} objectsEqual=${objects == afterObjects} returned=${e.returned} committed=$committed"
            )
            val method = Thread
              .currentThread()
              .getStackTrace
              .find(frame =>
                frame.getClassName == classOf[TestDeltaRangerAppendTransactionSource].getName &&
                  !frame.getMethodName.contains("$") && frame.getMethodName != "attempt"
              )
              .get
              .getMethodName
            val outcome = Map[String, Any](
              "schemaVersion" -> 1,
              "testMethod" -> method,
              "scenario" -> ("transaction-source-" + name),
              "frontend" -> frontend,
              "duplicate" -> duplicate,
              "returned" -> e.returned,
              "assertionsPassed" -> e.assertionsPassed,
              "failureCauses" -> e.failures
                .map(error =>
                  Map("class" -> error.getClass.getName, "message" -> error.getMessage).asJava
                )
                .asJava,
              "sourcePreparationCount" -> e.sourceEntries,
              "helperOwnerCount" -> e.helpers.size(),
              "commitOwnerCount" -> e.commits.size(),
              "confBefore" -> settings.asJava,
              "confAfter" -> spark.sessionState.conf.getAllConfs.asJava,
              "cleanup" -> Map(
                "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
                "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
                "helperActive" -> (field(DeltaRangerAppendHelper, "active")
                  .asInstanceOf[ThreadLocal[DeltaRangerAppendHelper]]
                  .get() != null),
                "commitOwnerActive" -> DeltaRangerAppendCommit.current.nonEmpty
              ).asJava
            )
            Files.write(
              trace.directory.resolve("outcome.json"),
              new ObjectMapper().writeValueAsBytes(outcome.asJava)
            )
          }
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> reset.toString,
          "spark.databricks.delta.stats.skipping" -> (!noSkipping).toString
        ),
        spark => {
          val location = NativeSelectFixture.locations("a")
          if (duplicate)
            spark
              .sql("SELECT 4L id, 'seed' secret, 'keep' region")
              .write
              .format("delta")
              .mode("append")
              .option("txnAppId", "seeded-app")
              .option("txnVersion", "1")
              .save(location)
          val files = org.apache.spark.sql.delta.DeltaLog
            .forTable(spark, location)
            .update()
            .allFiles
            .collect()
            .toVector
          originalFiles = files.map(_.path).toSet
          // Native fixture writes each UNION branch to its own file. Read stats independently.
          expectedSelectiveFiles =
            files.filter(f => f.stats.contains("\"minValues\":{\"id\":1,")).map(_.path).toSet
        }
      )
    finally {
      NativeSelectFixture.catalogPolicies = previous
      NativeSelectFixture.policy = previousPolicy
      target.engine.close()
      source.engine.close()
      trace.close()
    }
  }

  private def inventory(root: Path): Vector[String] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .map(p =>
          root.relativize(p).toString + ":" +
            (if (Files.isSymbolicLink(p)) "link"
             else if (Files.isDirectory(p)) "directory"
             else "file")
        )
        .toVector
        .sorted
    finally paths.close()
  }

}
