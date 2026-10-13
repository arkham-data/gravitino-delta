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
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.mockito.Mockito.{mock, verifyNoInteractions, when}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import org.apache.spark.sql.{Dataset, Row, SparkSession, SparkSessionExtensions}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.delta.OptimisticTransaction
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendV1Input {
  @Test
  def realNamedFrontendsReachTerminalInputValidation(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      val trace = InstalledAppendBoundaryProbe.fresh("v1-input-" + frontend)
      val target = new TestDeltaRangerNativeSelect.Fixture
      val source = new TestDeltaRangerNativeSelect.Fixture
      val previous = NativeSelectFixture.catalogPolicies
      val previousPolicy = NativeSelectFixture.policy
      source.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
      try
        NativeSelectFixture.withSpark(
          spark => {
            trace.activate(spark)
            trace.record(
              "analyzer-inventory",
              spark.sessionState.analyzer.batches
                .map(b =>
                  s"${b.name}|${b.strategy.getClass.getName}|${b.strategy.maxIterations}|${b.rules.map(_.getClass.getName).mkString(",")}"
                )
                .mkString("\n")
            )
            trace.record(
              "resolution-inventory",
              spark.sessionState.analyzer.extendedResolutionRules
                .map(_.getClass.getName)
                .mkString("\n")
            )
            trace.record(
              "posthoc-inventory",
              spark.sessionState.analyzer.postHocResolutionRules
                .map(_.getClass.getName)
                .mkString("\n")
            )
            trace.record(
              "check-inventory",
              spark.sessionState.analyzer.extendedCheckRules.map(_.getClass.getName).mkString("\n")
            )
            trace.record(
              "normalization-inventory",
              spark.sessionState.planNormalizationRules.map(_.getClass.getName).mkString("\n")
            )
            val failures = new LinkedBlockingQueue[QueryExecution]()
            val listener = new QueryExecutionListener {
              override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = ()
              override def onFailure(name: String, qe: QueryExecution, failure: Exception): Unit = {
                failures.add(qe)
              }
            }
            spark.listenerManager.register(listener)
            val root = Paths.get(NativeSelectFixture.locations("a")).getParent
            val before =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
            val objects = inventory(root)
            Files.write(
              trace.directory.resolve("before-objects.txt"),
              objects.mkString("\n").getBytes(StandardCharsets.UTF_8)
            )
            var originalReceipt: DeltaRangerDecisionReceipt = null
            var inputObserved = false
            source.onRequest = _ =>
              if (atInsert && !inputObserved) {
                inputObserved = true
                trace.record(
                  "original-obligations",
                  s"receipt=${trace.id(originalReceipt)} occurrences=${originalReceipt.obligations.size} sameReceipt=true"
                )
                assertNull(DeltaRangerNativeOperation.current())
                assertTrue(OptimisticTransaction.getActive.isEmpty)
                trace.record(
                  "actual-input-observation",
                  "frontend=" + frontend + " noTxn=true noNative=true"
                )
              }
            try {
              val data = spark.sql("SELECT id, secret, region FROM b.db.t")
              val original = data.queryExecution.analyzed
              originalReceipt =
                DeltaRangerDecisionReceipt.consumeOwned(original, spark, optimized = false)
              trace.source(originalReceipt)
              spark.sessionState.analyzer.batches
              val settings = spark.sessionState.conf.getAllConfs.toMap
              val error = assertThrows(
                classOf[Exception],
                () => {
                  if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
                  else data.write.mode("append").saveAsTable("a.db.t")
                }
              )
              val causes =
                Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
              trace.record("failure", causes.map(_.toString).mkString(" | "))
              trace.record(
                "failure-stack",
                causes.map(c => c.getStackTrace.mkString("\n")).mkString("\nCaused by:\n")
              )
              assertTrue(
                causes.exists(
                  _.getMessage == "Delta append input validated; writes remain disabled"
                ),
                causes.mkString(" | ")
              )
              val terminal = causes
                .find(_.getMessage == "Delta append input validated; writes remain disabled")
                .get
              assertTrue(terminal.getStackTrace.exists(_.getClassName.contains("SupportsV1Write")))
              assertTrue(
                terminal.getStackTrace.exists(_.getClassName.contains("V1FallbackWriters"))
              )
              val qe = failures.poll(10, TimeUnit.SECONDS)
              assertNotNull(qe)
              val command = qe.analyzed.asInstanceOf[AppendData]
              assertSame(original, command.analyzedQuery.get)
              val optimized = qe.optimizedPlan.asInstanceOf[AppendData]
              val planned = qe.sparkPlan.collectFirst { case guard: DeltaRangerAppendInputExec =>
                guard
              }.get
              val executed =
                qe.executedPlan.collectFirst { case guard: DeltaRangerAppendInputExec => guard }.get
              assertTrue(optimized.write.nonEmpty)
              assertSame(planned.delegate, executed.delegate)
              assertSame(
                optimized.table.asInstanceOf[DataSourceV2Relation].table,
                executed.delegate.table
              )
              assertSame(planned.delegate.plan, executed.delegate.plan)
              assertSame(planned.delegate.refreshCache, executed.delegate.refreshCache)
              assertNotSame(optimized.write.get, executed.delegate.write)
              DeltaRangerDecisionReceipt
                .consumeOwned(original, spark, optimized = false)
                .consume(executed.delegate.plan, spark, optimized = false)
              assertTrue(inputObserved)
              trace.record(
                "physical-transition",
                s"stored=${trace.id(command.analyzedQuery.get)} optimizedStored=${trace.id(optimized.analyzedQuery.get)} opaqueWriter=${trace.id(optimized.write.get)} target=${trace.id(executed.delegate.table)} fallback=${trace.id(executed.delegate.plan)} refresh=${trace.id(executed.delegate.refreshCache)} terminalWriter=${trace.id(executed.delegate.write)} samePlannedExecutedDelegate=true"
              )
              trace.record("physical", qe.executedPlan.treeString)
              trace.record("case-complete", "case=" + frontend + " terminalInputValidated=true")
              assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
            } finally {
              spark.listenerManager.unregister(listener)
              assertEquals(
                before,
                InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
              )
              assertTrue(OptimisticTransaction.getActive.isEmpty)
              assertNull(DeltaRangerNativeOperation.current())
              val afterObjects = inventory(root)
              Files.write(
                trace.directory.resolve("after-objects.txt"),
                afterObjects.mkString("\n").getBytes(StandardCharsets.UTF_8)
              )
              assertEquals(objects, afterObjects)
              source.onRequest = _ => ()
              trace.record(
                "cleanup",
                "case=" + frontend + " noTxn=true noNative=true fullSQLConfEqual=true allFilesEqual=true objectsEqual=true"
              )
            }
          },
          Some(NativeSelectFixture.extension(target))
        )
      finally {
        NativeSelectFixture.catalogPolicies = previous
        NativeSelectFixture.policy = previousPolicy
        target.engine.close()
        source.engine.close()
        trace.close()
      }
    }
  }
  @Test
  def failedEntryValidationExpiresTheActualExecutionOwner(): Unit = withCase("entry-lifetime") {
    c =>
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      var retained: DeltaRangerAppendInputExec = null
      var fired = false
      val before = c.spark.conf.get("spark.sql.shuffle.partitions")
      c.source.onRequest = _ => {
        if (!fired && atOwnerRun) {
          fired = true
          retained = activeGuard(c.spark)
          c.spark.conf.set("spark.sql.shuffle.partitions", "19")
        }
      }
      try {
        val failure = appendFailure(c, data)
        assertReason(failure, "configuration changed")
        assertTrue(fired)
        assertNotNull(retained)
      } finally {
        c.source.onRequest = _ => ()
        c.spark.conf.set("spark.sql.shuffle.partitions", before)
      }
      assertReason(
        assertThrows(classOf[SecurityException], () => retained.run()),
        "expired execution owner"
      )
      c.trace.record(
        "lifetime-rejection",
        "mutationFired=true entry=configuration-changed retry=expired-execution-owner"
      )
  }

  @Test
  def actualInputRechecksPolicyPublicationRegistrationAndContext(): Unit = {
    Seq(
      "select",
      "update",
      "mask",
      "filter",
      "source-registration",
      "target-registration",
      "policy",
      "roles",
      "subject",
      "configuration"
    ).foreach { change =>
      withCase("input-" + change) { c =>
        val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
        var fired = false
        var retained: DeltaRangerAppendInputExec = null
        val conf = c.spark.conf.get("spark.sql.shuffle.partitions")
        c.source.onRequest = _ => {
          if (!fired && atInsert) {
            fired = true
            retained = activeGuard(c.spark)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            c.trace.record("actual-input-mutation", "mutationFired=true change=" + change)
            change match {
              case "select" => c.source.denied.add("b:secret")
              case "update" => c.target.denied.add("a:*")
              case "mask"   => c.source.mask = "upper({col})"
              case "filter" => c.source.filter = "id > 1"
              case "source-registration" | "target-registration" =>
                val name = if (change == "source-registration") "b" else "a"
                NativeSelectFixture
                  .bindingFixture(c.spark, name)
                  .properties
                  .put("delta.registration.receipt.generation", "2")
              case "policy" =>
                val next = c.source.plugin.getPoliciesVersion + 1L
                when(c.source.plugin.getPoliciesVersion).thenReturn(next)
                c.source.start()
              case "roles" =>
                val next = c.source.plugin.getRolesVersion + 1L
                val roles = c.source.plugin.getRoles
                roles.setRoleVersion(next)
                when(c.source.plugin.getRolesVersion).thenReturn(next)
                c.source.plugin.getCurrentRangerAuthContext.setRoles(roles)
                c.source.listener.contextChanged()
              case "subject" =>
                c.spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-subject")
              case "configuration" => c.spark.conf.set("spark.sql.shuffle.partitions", "19")
            }
          }
        }
        try {
          val failure = appendFailure(c, data)
          assertTrue(fired, "mutation never reached actual insert: " + change)
          val reason = change match {
            case "select" | "update"   => "denied"
            case "mask" | "filter"     => "action decision changed"
            case "source-registration" => "retained registration validation rejected"
            case "target-registration" => "registration binding is unavailable"
            case "policy" | "roles"    => "publication changed"
            case "subject"             => "identity contradicts"
            case "configuration"       => "configuration changed"
          }
          assertReason(failure, reason)
          c.trace.record("intended-rejection", reason)
          assertReason(
            assertThrows(classOf[SecurityException], () => retained.run()),
            "expired execution owner"
          )
        } finally {
          c.source.onRequest = _ => ()
          c.spark.conf.set("spark.sql.shuffle.partitions", conf)
          c.spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
        }
      }
    }
  }

  @Test
  def originalEmptyRepeatedAndSelfSourceObligationsSurvive(): Unit = {
    Seq(
      ("empty", "SELECT id, secret, region FROM b.db.t WHERE id < 0", 1),
      ("repeated", "SELECT l.id, l.secret, r.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id", 2),
      ("self", "SELECT id, secret, region FROM a.db.t", 1)
    ).foreach { case (name, sql, occurrences) =>
      withCase(name) { c =>
        val data = c.spark.sql(sql)
        val receipt = DeltaRangerDecisionReceipt.consumeOwned(
          data.queryExecution.analyzed,
          c.spark,
          optimized = false
        )
        assertEquals(occurrences, receipt.obligations.size)
        var reached = false
        val policy = if (name == "self") c.target else c.source
        policy.onRequest = request =>
          if (atInsert) {
            reached = true
            c.trace.record(
              "original-obligations",
              s"receipt=${c.trace.id(receipt)} occurrences=$occurrences sameReceipt=true"
            )
            assertEquals(occurrences, receipt.obligations.size)
            assertTrue(receipt.obligations.forall(_.columns.nonEmpty))
          }
        assertReason(appendFailure(c, data), DeltaRangerAppendV1Input.terminalMessage)
        assertTrue(reached)
        c.trace.record(
          "original-obligations",
          s"case=$name occurrences=$occurrences columns=${receipt.obligations.map(_.columns)}"
        )
      }
    }
  }

  @Test
  def sameLogCrossCatalogRetainsIndependentSourceObligation(): Unit =
    withCase("same-log-cross-catalog") { c =>
      val locations = NativeSelectFixture.locations
      NativeSelectFixture.locations += "b" -> locations("a")
      try {
        val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
        val receipt = DeltaRangerDecisionReceipt.consumeOwned(
          data.queryExecution.analyzed,
          c.spark,
          optimized = false
        )
        val target = NativeSelectFixture
          .wrapper(c.spark, "a")
          .loadTable(org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "t"))
          .asInstanceOf[org.apache.spark.sql.delta.catalog.DeltaTableV2]
        assertEquals(1, receipt.obligations.size)
        assertEquals("b", receipt.obligations.head.scan.catalog)
        assertSame(target.deltaLog, receipt.obligations.head.binding.log())
        var fired = false
        c.source.onRequest = _ =>
          if (!fired && atInsert) {
            fired = true
            c.source.denied.add("b:secret")
            c.trace.record(
              "original-obligations",
              s"receipt=${c.trace.id(receipt)} occurrences=1 sameReceipt=true source=b target=a sameLog=true mutationFired=true"
            )
          }
        try {
          assertReason(appendFailure(c, data), "denied")
          assertTrue(fired)
        } finally { c.source.denied.clear(); c.source.onRequest = _ => () }
      } finally NativeSelectFixture.locations = locations
    }

  @Test
  def realIssuedGuardRejectsNestedForeignExpiredAndSubstitutedExecution(): Unit =
    withCase("guard-negatives") { c =>
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      var retained: DeltaRangerAppendInputExec = null
      var fired = false
      c.source.onRequest = _ =>
        if (!fired && atInsert) {
          fired = true
          retained = activeGuard(c.spark)
          assertReason(
            assertThrows(classOf[SecurityException], () => retained.run()),
            "nested execution"
          )
          assertReason(
            assertThrows(classOf[SecurityException], () => retained.verify(c.spark.newSession())),
            "session changed"
          )
          val failures = new LinkedBlockingQueue[Throwable]()
          val thread = new Thread(() => {
            SparkSession.setActiveSession(c.spark)
            try retained.run()
            catch { case failure: Throwable => failures.add(failure) }
            finally SparkSession.clearActiveSession()
          })
          thread.start()
          thread.join(10000)
          assertFalse(thread.isAlive)
          assertReason(failures.poll(), "creator thread changed")
          assertReason(
            assertThrows(
              classOf[SecurityException],
              () => retained.canonicalized.asInstanceOf[DeltaRangerAppendInputExec].run()
            ),
            "canonicalized execution"
          )
          val qe = SQLExecution.getQueryExecution(
            c.spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
          )
          val command = qe.analyzed.asInstanceOf[AppendData]
          val tag =
            TreeNodeTag[DeltaRangerAppendV1Input.Reference]("gravitino.delta.ranger.append-input")
          val reference = command.analyzedQuery.get.getTagValue(tag).get
          val foreign = mock(classOf[V1Write])
          val mutations = Seq(
            retained.delegate.copy(write = foreign),
            retained.delegate
              .copy(refreshCache = () => throw new AssertionError("foreign refresh executed")),
            retained.delegate.copy(plan = Project(Nil, retained.delegate.plan))
          )
          mutations.foreach { native =>
            val forged = DeltaRangerAppendInputExec()(native, reference)
            forged.copyTagsFrom(retained)
            retained.logicalLink.foreach(forged.setLogicalLink)
            assertReason(
              assertThrows(classOf[SecurityException], () => forged.verify(c.spark)),
              "issued physical leaf changed"
            )
          }
          verifyNoInteractions(foreign)
          val bytes = new ByteArrayOutputStream()
          val output = new ObjectOutputStream(bytes)
          try output.writeObject(reference)
          finally output.close()
          val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
          val restored =
            try input.readObject().asInstanceOf[DeltaRangerAppendV1Input.Reference]
            finally input.close()
          assertReason(
            assertThrows(classOf[SecurityException], () => restored.get),
            "inert phase reference"
          )
          c.trace.record(
            "guard-negatives",
            "nested/session/thread/canonicalized/serialized-reference/writer/refresh/plan rejected"
          )
        }
      assertReason(appendFailure(c, data), DeltaRangerAppendV1Input.terminalMessage)
      assertTrue(fired)
      c.source.onRequest = _ => ()
      assertReason(
        assertThrows(classOf[SecurityException], () => retained.run()),
        "expired execution owner"
      )
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => retained.delegate.write.toInsertableRelation()
        ),
        "expired execution owner"
      )
      assertReason(
        appendFailure(c, c.spark.sql("SELECT id, secret, region FROM b.db.t")),
        DeltaRangerAppendV1Input.terminalMessage
      )
      assertEquals(2, c.spark.sql("SELECT id, secret, region FROM b.db.t").collect().length)
    }

  @Test
  def datasetConstructionFailureClosesWholeNativeRun(): Unit = withCase("dataset-construction") {
    c =>
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      val metadata = NativeSelectFixture.bindingFixture(c.spark, "b")
      val injected = new AssertionError("fixture Dataset construction failure")
      var fired = false
      var inserted = false
      var reset = false
      var originalAnalyzed = false
      var retained: DeltaRangerAppendInputExec = null
      c.source.onRequest = _ => {
        if (atInsert) inserted = true
        if (
          !reset && Thread
            .currentThread()
            .getStackTrace
            .exists(f =>
              f.getClassName
                .endsWith("DeltaRangerAppendV1Input$Owner") && f.getMethodName == "terminalRelation"
            )
        ) {
          retained = activeGuard(c.spark)
          originalAnalyzed = retained.delegate.plan.analyzed
          assertTrue(originalAnalyzed)
          // Fault injection only: native ofRows otherwise skips analysis for this already analyzed
          // real fallback plan. The public JVM setter changes no source recipe or production hook.
          retained.delegate.plan.getClass
            .getMethod(
              "org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$_analyzed_$eq",
              java.lang.Boolean.TYPE
            )
            .invoke(retained.delegate.plan, java.lang.Boolean.FALSE)
          assertFalse(retained.delegate.plan.analyzed)
          reset = true
          c.trace.record("fault-injection", "actual-fallback-analyzed-bit-reset=true")
        }
      }
      metadata.onRead = _ => {
        val stack = Thread.currentThread().getStackTrace
        if (
          !fired && stack.exists(f =>
            f.getClassName == "org.apache.spark.sql.Dataset$" && f.getMethodName == "ofRows"
          ) &&
          stack.exists(_.getClassName.contains("SupportsV1Write")) && !atInsert
        ) {
          fired = true
          assertSame(retained, activeGuard(c.spark))
          c.trace.record("dataset-construction-failure", stack.mkString("\n"))
          throw injected
        }
      }
      try {
        val error =
          assertThrows(classOf[Throwable], () => data.write.mode("append").insertInto("a.db.t"))
        assertTrue(reset)
        assertTrue(fired, "fault-injected native Dataset analysis did not revisit metadata")
        assertFalse(inserted)
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
        assertTrue(causes.exists(_ eq injected), causes.mkString(" | "))
      } finally {
        metadata.onRead = _ => ()
        c.source.onRequest = _ => ()
        if (retained != null)
          retained.delegate.plan.getClass
            .getMethod(
              "org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$_analyzed_$eq",
              java.lang.Boolean.TYPE
            )
            .invoke(retained.delegate.plan, java.lang.Boolean.valueOf(originalAnalyzed))
      }
      assertReason(
        assertThrows(classOf[SecurityException], () => retained.run()),
        "expired execution owner"
      )
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => retained.delegate.write.toInsertableRelation()
        ),
        "expired execution owner"
      )
      assertReason(
        appendFailure(c, c.spark.sql("SELECT id, secret, region FROM b.db.t")),
        DeltaRangerAppendV1Input.terminalMessage
      )
  }

  @Test
  def foreignAnalyzerRulesRejectBeforeLaterCallbacks(): Unit = {
    Seq("resolution", "post-hoc", "check", "normalization", "earlier-resolution").foreach { kind =>
      val callbacks = new AtomicInteger()
      val foreign: SparkSessionExtensions => Unit = extensions => {
        def rule = new Rule[LogicalPlan] {
          override def apply(plan: LogicalPlan): LogicalPlan = {
            if (plan.isInstanceOf[AppendData]) callbacks.incrementAndGet()
            plan
          }
        }
        kind match {
          case "resolution" | "earlier-resolution" => extensions.injectResolutionRule(_ => rule)
          case "post-hoc"      => extensions.injectPostHocResolutionRule(_ => rule)
          case "normalization" => extensions.injectPlanNormalizationRule(_ => rule)
          case "check" =>
            extensions.injectCheckRule(_ =>
              plan => {
                if (plan.isInstanceOf[AppendData]) callbacks.incrementAndGet()
                ()
              }
            )
        }
      }
      withCase(
        "foreign-" + kind,
        before = if (kind == "earlier-resolution") Some(foreign) else None,
        after = if (kind == "earlier-resolution") None else Some(foreign)
      ) { c =>
        val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
        val expected = if (kind == "earlier-resolution") "resolution" else kind
        assertReason(
          appendFailure(c, data),
          "Delta append inventory rejected: " + expected + " rules"
        )
        if (kind == "earlier-resolution") assertTrue(callbacks.get() > 0)
        else assertEquals(0, callbacks.get())
        c.trace.record(
          "foreign-inventory",
          s"kind=$kind appendCallbacks=${callbacks.get()} authorityIssued=false"
        )
      }
    }
  }

  @Test
  def inputGroupChangeRejectsAuthenticatedSource(): Unit = withCase("input-groups") { c =>
    val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
    val user = UserGroupInformation.getCurrentUser
    val groups = user.getGroupNames
    var fired = false
    c.source.onRequest = _ =>
      if (!fired && atInsert) {
        fired = true
        UserGroupInformation.createUserForTesting(
          user.getShortUserName,
          Array("changed-input-group")
        )
      }
    try {
      assertReason(appendFailure(c, data), "subject changed")
      assertTrue(fired)
      c.trace.record("group-rejection", "mutationFired=true diagnostic=subject-changed")
    } finally {
      c.source.onRequest = _ => ()
      UserGroupInformation.createUserForTesting(user.getShortUserName, groups)
    }
  }

  @Test
  def nativeLeafCannotReenterItsTerminalWriter(): Unit = withCase("nested-native-leaf") { c =>
    val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
    var fired = false
    c.source.onRequest = _ =>
      if (!fired && atInsert) {
        fired = true
        val guard = activeGuard(c.spark)
        assertReason(
          assertThrows(classOf[SecurityException], () => guard.delegate.run()),
          "terminal writer already issued"
        )
        c.trace.record(
          "native-reentry-rejection",
          "mutationFired=true diagnostic=terminal-writer-already-issued"
        )
      }
    assertReason(appendFailure(c, data), DeltaRangerAppendV1Input.terminalMessage)
    assertTrue(fired)
  }

  @Test
  def changedOptimizedFieldsCannotBorrowCapturedAuthority(): Unit = withCase("logical-mutations") {
    c =>
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      var fired = false
      c.source.onRequest = _ =>
        if (!fired && atInsert) {
          fired = true
          val qe = SQLExecution.getQueryExecution(
            c.spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
          )
          val command = qe.optimizedPlan.asInstanceOf[AppendData]
          val foreign = mock(classOf[V1Write])
          val falseLiteral = Literal(false)
          val cases = Seq(
            command.copy(query =
              Filter(falseLiteral, command.query)
            ) -> "executable logical binding changed",
            command.copy(analyzedQuery =
              Some(
                org.apache.spark.sql.catalyst.plans.logical
                  .Filter(falseLiteral, command.analyzedQuery.get)
              )
            ) -> "logical binding changed",
            command.copy(writeOptions =
              Map("mergeSchema" -> "true")
            ) -> "command shell changed: options",
            command.copy(write = Some(foreign)) -> "captured inert writer changed"
          )
          cases.foreach { case (candidate, reason) =>
            candidate.copyTagsFrom(command)
            assertReason(
              assertThrows(
                classOf[SecurityException],
                () => DeltaRangerAppendV1Input.plan(candidate, c.spark)
              ),
              reason
            )
            c.trace.record("logical-mutation", "mutationFired=true expected=" + reason)
          }
          verifyNoInteractions(foreign)
          assertReason(
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerAppendV1Input.plan(command.clone().asInstanceOf[AppendData], c.spark)
            ),
            "planner stage changed"
          )
        }
      assertReason(appendFailure(c, data), DeltaRangerAppendV1Input.terminalMessage)
      assertTrue(fired)
  }

  @Test
  def missingAmbiguousAndExpiredReferencesCannotIssueAuthority(): Unit =
    withCase("reference-negatives") { c =>
      val prior = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      assertReason(appendFailure(c, prior), DeltaRangerAppendV1Input.terminalMessage)
      val tag =
        TreeNodeTag[DeltaRangerAppendV1Input.Reference]("gravitino.delta.ranger.append-input")
      val expired = prior.queryExecution.analyzed.getTagValue(tag).get
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      var fired = false
      c.source.onRequest = _ =>
        if (!fired && atInsert) {
          fired = true
          val command = SQLExecution
            .getQueryExecution(
              c.spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
            )
            .optimizedPlan
            .asInstanceOf[AppendData]
          val missing = command.copy(
            query = command.query.clone(),
            analyzedQuery = Some(command.analyzedQuery.get.clone())
          )
          missing.copyTagsFrom(command)
          Vector(missing, missing.query, missing.analyzedQuery.get).foreach(_.unsetTagValue(tag))
          assertReason(
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerAppendV1Input.plan(missing, c.spark)
            ),
            "missing or ambiguous phase owner"
          )
          val ambiguous = command.copy(query = command.query.clone())
          ambiguous.copyTagsFrom(command)
          ambiguous.query.setTagValue(tag, expired)
          assertReason(
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerAppendV1Input.plan(ambiguous, c.spark)
            ),
            "missing or ambiguous phase owner"
          )
          assertReason(
            assertThrows(classOf[SecurityException], () => expired.get.plan(command, c.spark)),
            "expired execution owner"
          )
          c.trace.record("phase-reference-negatives", "missing=true ambiguous=true expired=true")
        }
      assertReason(appendFailure(c, data), DeltaRangerAppendV1Input.terminalMessage)
      assertTrue(fired)
      c.source.onRequest = _ => ()
      assertReason(appendFailure(c, data), "source already has an append owner")
    }

  @Test
  def qualifiedInventoryRejectsExcludedFrontends(): Unit = {
    val cases = Seq(
      "values" -> "missing receipt",
      "insert-select" -> "missing receipt",
      "raw-append" -> "unnamed target",
      "raw-select" -> "Raw Delta paths are not admitted",
      "streaming" -> "Unqualified Delta Ranger route: unresolved or streaming plan",
      "overwrite" -> "Derived or reused Delta plans are not qualified",
      "create" -> "Unqualified Delta Ranger route: CreateTableAsSelect",
      "replace" -> "Unqualified Delta Ranger route: ReplaceTableAsSelect"
    )
    assertAll(cases.map { case (route, reason) =>
      new org.junit.jupiter.api.function.Executable {
        override def execute(): Unit = qualifiedFrontend(route, reason)
      }
    }.asJava)
  }

  @Test
  def qualifiedWriteToIsExcluded(): Unit =
    qualifiedFrontend("writeTo", "Delta decision receipt rejected: missing receipt")

  private def qualifiedFrontend(route: String, expected: String): Unit =
    withCase("qualified-" + route) { c =>
      DeltaRangerAppendInventory.qualify(c.spark)
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(
        data.queryExecution.analyzed,
        c.spark,
        optimized = false
      )
      c.trace.source(receipt)
      c.trace.record(
        "qualified-source",
        s"route=$route analyzedReceipt=${c.trace.id(receipt)} logicalIsAnalyzed=${data.queryExecution.logical eq data.queryExecution.analyzed} logicalHasReceipt=${DeltaRangerDecisionReceipt.present(data.queryExecution.logical)}"
      )
      val catalog = NativeSelectFixture.wrapper(c.spark, "a")
      val table = catalog
        .loadTable(org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "t"))
        .asInstanceOf[org.apache.spark.sql.delta.catalog.DeltaTableV2]
      val target = DeltaRangerAppendTargetReceipt.capture(
        c.spark,
        catalog.registrationBindings().resolveAppendTarget(table)
      )
      val registrationFixture = NativeSelectFixture.bindingFixture(c.spark, "a")
      val registrationProperties = registrationFixture.properties.asScala.toMap
      val metadataTables = registrationFixture.client.loadCatalog("a").asTableCatalog()
      var inserted = false
      c.source.onRequest = _ =>
        if (atInsert) { inserted = true; c.trace.record("qualified-input-entry", "route=" + route) }
      val failures = new LinkedBlockingQueue[QueryExecution]()
      val listener = new QueryExecutionListener {
        override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = ()
        override def onFailure(name: String, qe: QueryExecution, failure: Exception): Unit =
          failures.add(qe)
      }
      c.spark.listenerManager.register(listener)
      try {
        val error = assertThrows(
          classOf[Exception],
          () =>
            route match {
              case "writeTo" => data.writeTo("a.db.t").append()
              case "values" =>
                c.spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')").collect()
              case "insert-select" =>
                c.spark.sql("INSERT INTO a.db.t SELECT id, secret, region FROM b.db.t").collect()
              case "raw-append" =>
                c.spark
                  .sql(
                    s"INSERT INTO delta.`${NativeSelectFixture.locations("a")}` VALUES (9L, 'new', 'keep')"
                  )
                  .collect()
              case "raw-select" =>
                c.spark
                  .sql(s"SELECT * FROM delta.`${NativeSelectFixture.locations("a")}`")
                  .collect()
              case "streaming" => c.spark.readStream.format("rate").load()
              case "overwrite" => data.write.mode("overwrite").insertInto("a.db.t")
              case "create" =>
                c.spark
                  .sql(
                    "CREATE TABLE a.db.created USING delta AS SELECT 9L id, 'new' secret, 'keep' region"
                  )
                  .collect()
              case "replace" =>
                c.spark
                  .sql(
                    "REPLACE TABLE a.db.t USING delta AS SELECT 9L id, 'new' secret, 'keep' region"
                  )
                  .collect()
            }
        )
        c.trace.record(
          "qualified-frontier",
          s"route=$route inputEntered=$inserted diagnostic=${Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).map(_.toString).mkString(" | ")}"
        )
        c.trace.record(
          "qualified-failure-stack",
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .map(e => e.getStackTrace.mkString("\n"))
            .mkString("\nCaused by:\n")
        )
        if (inserted) {
          val qe = failures.poll(10, TimeUnit.SECONDS)
          assertNotNull(qe)
          val command = qe.analyzed.asInstanceOf[AppendData]
          c.trace.record(
            "qualified-native-shell",
            s"route=$route class=${command.getClass.getName} byName=${command.isByName} options=${command.writeOptions} originalQuery=${command.query eq data.queryExecution.analyzed} storedOriginal=${command.analyzedQuery.exists(_ eq data.queryExecution.analyzed)} physical=${qe.executedPlan.treeString}"
          )
        }
        assertEquals(registrationProperties, registrationFixture.properties.asScala.toMap)
        val metadataMutations = org.mockito.Mockito
          .mockingDetails(metadataTables)
          .getInvocations
          .asScala
          .filter(i =>
            Set("createTable", "alterTable", "dropTable", "purgeTable", "renameTable")(
              i.getMethod.getName
            )
          )
        assertTrue(metadataMutations.isEmpty, metadataMutations.toString)
        c.trace.record(
          "qualified-metadata",
          s"route=$route mutationCalls=0 registrationPropertiesEqual=true"
        )
        target.validate(c.spark)
        receipt.validateAction(c.spark)
        assertReason(error, expected)
        assertFalse(inserted, "Excluded frontend reached actual input")
      } finally { c.source.onRequest = _ => (); c.spark.listenerManager.unregister(listener) }
    }

  @Test
  def nativeSourceNormalizationRemainsExplicitlyDenied(): Unit = withCase("normalization-denied") {
    c =>
      Seq(
        "SELECT CAST(id AS INT) id, secret, region FROM b.db.t",
        "SELECT id AS renamed, secret, region FROM b.db.t",
        "SELECT secret, id, region FROM b.db.t"
      ).foreach { sql =>
        val data = c.spark.sql(sql)
        assertReason(appendFailure(c, data), "native source normalization is not qualified")
      }
  }

  @Test
  def deniedInitialSourceAndTargetNeverReachInput(): Unit = withCase("early-permissions") { c =>
    Seq("select", "update").foreach { access =>
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      var inserted = false
      c.source.onRequest = _ => if (atInsert) inserted = true
      val policy = if (access == "select") c.source else c.target
      policy.denied.add(if (access == "select") "b:secret" else "a:*")
      try {
        assertReason(appendFailure(c, data), "denied")
        assertFalse(inserted)
        c.trace.record("early-permission-denial", "access=" + access + " inputReached=false")
      } finally {
        policy.denied.clear()
        c.source.onRequest = _ => ()
      }
    }
    assertEquals(2, c.spark.sql("SELECT id, secret, region FROM b.db.t").collect().length)
  }

  private def atInsert: Boolean = Thread
    .currentThread()
    .getStackTrace
    .exists(f =>
      f.getClassName
        .endsWith("DeltaRangerAppendV1Input$TerminalRelation") && f.getMethodName == "insert"
    )

  private case class Case(
      spark: SparkSession,
      target: TestDeltaRangerNativeSelect.Fixture,
      source: TestDeltaRangerNativeSelect.Fixture,
      trace: InstalledAppendBoundaryProbe.Trace
  )

  private def withCase(
      name: String,
      before: Option[SparkSessionExtensions => Unit] = None,
      after: Option[SparkSessionExtensions => Unit] = None
  )(body: Case => Unit): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("v1-input-" + name)
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    val previous = NativeSelectFixture.catalogPolicies
    val previousPolicy = NativeSelectFixture.policy
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          Files.createDirectories(
            Paths.get(new java.net.URI(spark.conf.get("spark.sql.warehouse.dir")))
          )
          trace.record("fixture-setup", "warehouseDirectoryInitializedBeforeSnapshot=true")
          val before =
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
          val objects = inventory(root)
          Files.write(
            trace.directory.resolve("before-objects.txt"),
            objects.mkString("\n").getBytes(StandardCharsets.UTF_8)
          )
          spark.sessionState.analyzer.batches
          val settings = spark.sessionState.conf.getAllConfs.toMap
          try {
            body(Case(spark, target, source, trace))
            trace.record("case-complete", "case=" + name)
          } finally {
            assertEquals(
              before,
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            )
            val afterObjects = inventory(root)
            Files.write(
              trace.directory.resolve("after-objects.txt"),
              afterObjects.mkString("\n").getBytes(StandardCharsets.UTF_8)
            )
            assertEquals(objects, afterObjects)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
            trace.record(
              "cleanup",
              "case=" + name + " noTxn=true noNative=true fullSQLConfEqual=true allFilesEqual=true objectsEqual=true"
            )
          }
        },
        Some({
          val install = NativeSelectFixture.extension(target)
          extensions => {
            before.foreach(_(extensions))
            install(extensions)
            after.foreach(_(extensions))
          }
        })
      )
    finally {
      NativeSelectFixture.catalogPolicies = previous
      NativeSelectFixture.policy = previousPolicy
      target.engine.close()
      source.engine.close()
      trace.close()
    }
  }
  private def atOwnerRun: Boolean = Thread
    .currentThread()
    .getStackTrace
    .exists(f =>
      f.getClassName.endsWith("DeltaRangerAppendV1Input$Owner") && f.getMethodName == "run"
    )
  private def activeGuard(spark: SparkSession): DeltaRangerAppendInputExec = {
    val id = spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
    val qe = SQLExecution.getQueryExecution(id)
    qe.executedPlan.collectFirst { case guard: DeltaRangerAppendInputExec => guard }.get
  }
  private def appendFailure(c: Case, data: Dataset[Row]): Exception = {
    val root = Paths.get(NativeSelectFixture.locations("a")).getParent
    val name = "attempt-" + java.util.UUID.randomUUID().toString
    val before =
      InstalledAppendBoundaryProbe.manifest(root, c.trace.directory.resolve(name + "-before"))
    val objects = inventory(root)
    val receipt = DeltaRangerDecisionReceipt.consumeOwned(
      data.queryExecution.analyzed,
      c.spark,
      optimized = false
    )
    c.trace.source(receipt)
    c.trace.record(
      "original-source",
      s"attempt=$name occurrences=${receipt.obligations.size} columns=${receipt.obligations.map(_.columns)}"
    )
    try {
      val failure =
        assertThrows(classOf[Exception], () => data.write.mode("append").insertInto("a.db.t"))
      c.trace.record(
        "attempt-rejected",
        s"attempt=$name diagnostic=${Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).map(_.toString).mkString(" | ")}"
      )
      failure
    } finally {
      assertEquals(
        before,
        InstalledAppendBoundaryProbe.manifest(root, c.trace.directory.resolve(name + "-after"))
      )
      assertEquals(objects, inventory(root))
      assertTrue(OptimisticTransaction.getActive.isEmpty)
      assertNull(DeltaRangerNativeOperation.current())
      c.trace.record(
        "attempt-cleanup",
        s"attempt=$name allFilesEqual=true objectsEqual=true noTxn=true noNative=true"
      )
    }
  }
  private def assertReason(error: Throwable, reason: String): Unit = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
    assertTrue(
      causes.exists(e => Option(e.getMessage).exists(_.contains(reason))),
      causes.mkString(" | ")
    )
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
