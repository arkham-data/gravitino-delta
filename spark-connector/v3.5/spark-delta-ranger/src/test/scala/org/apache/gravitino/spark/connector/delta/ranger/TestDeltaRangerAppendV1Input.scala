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
import java.security.PrivilegedExceptionAction
import org.apache.gravitino.spark.connector.authorization.{
  GravitinoAuthorizationSparkSessionExtensions,
  RequiredPrivilegesCheck
}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.hadoop.fs.{Path => HadoopPath}
import com.fasterxml.jackson.databind.ObjectMapper
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
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendV1Input {
  @Test
  def unownedOrdinaryReceiptCannotInitializeAppendOwner(): Unit =
    withCase("unowned-ordinary-source") { c =>
      import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
      asApp {
        val source = c.spark.table("b.db.t").queryExecution.normalized
        assertTrue(DeltaRangerDecisionReceipt.present(source))
        assertFalse(DeltaRangerOrdinaryRead.pending(source))
        val catalog = c.spark.sessionState.catalogManager.catalog("a").asInstanceOf[TableCatalog]
        val id = Identifier.of(Array("db"), "t")
        val target = DataSourceV2Relation.create(catalog.loadTable(id), Some(catalog), Some(id))
        val command = AppendData.byPosition(target, source, Map.empty)
        assertReason(
          assertThrows(
            classOf[SecurityException],
            () =>
              DeltaRangerAppendV1Input
                .resolve(command, c.spark, NativeSelectFixture.wrapper(c.spark, "a"))
          ),
          "missing original pending source"
        )
        val tag =
          TreeNodeTag[DeltaRangerAppendV1Input.Reference]("gravitino.delta.ranger.append-input")
        assertTrue(command.getTagValue(tag).isEmpty)
        assertTrue(source.getTagValue(tag).isEmpty)
      }
    }

  @Test
  def originalAppendDemandAllowsFilteredProjectionsAndUsing(): Unit =
    withCase("original-demand-forms") { c =>
      asApp {
        appendSuccess(c, c.spark.sql("SELECT id, secret, region FROM b.db.t WHERE id > 0"))
        val left = c.spark.table("b.db.t").alias("x")
        val right = c.spark.table("b.db.t").alias("y")
        val using = left.join(right, Seq("id")).select("x.id", "x.secret", "x.region")
        assertEquals(
          2,
          DeltaRangerReadRule
            .extract(
              DeltaRangerOrdinaryAdmission.authoredDemandPlan(using.queryExecution.analyzed),
              c.spark
            )
            .size
        )
        using.write.mode("append").insertInto("a.db.t")
        c.successfulReturns += 1
        c.committed += 1
      }
    }

  @Test
  def constantProjectionOverPredicateCannotBecomeAnAppendSource(): Unit =
    withCase("constant-predicate-source") { c =>
      asApp {
        appendSuccess(c, c.spark.table("b.db.t"))
        c.trace.record("constant-source-target", NativeSelectFixture.locations("a"))
        val constants = c.spark
          .table("b.db.t")
          .filter("region = 'keep'")
          .selectExpr("9L AS id", "'constant' AS secret", "'keep' AS region")
        val demand = DeltaRangerReadRule.extract(
          DeltaRangerOrdinaryAdmission.demandPlan(constants.queryExecution.analyzed),
          c.spark
        )
        assertEquals(Set("region"), demand.head._2)
        appendFailure(c, constants, recordSource = false)
      }
    }

  @Test
  def columnlessProjectionCannotBecomeAnAppendSource(): Unit = withCase("columnless-source") { c =>
    asApp {
      appendSuccess(c, c.spark.table("b.db.t"))
      val constants =
        c.spark.table("b.db.t").selectExpr("9L AS id", "'constant' AS secret", "'keep' AS region")
      val demands = DeltaRangerReadRule.extract(constants.queryExecution.analyzed, c.spark)
      assertEquals(1, demands.size)
      assertTrue(demands.head._2.isEmpty)
      assertReason(appendFailure(c, constants), "columnless original source demand")
    }
  }

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
            val completed = new LinkedBlockingQueue[QueryExecution]()
            val listener = new QueryExecutionListener {
              override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
                if (qe.analyzed.isInstanceOf[AppendData]) completed.add(qe)
              }
              override def onFailure(name: String, qe: QueryExecution, failure: Exception): Unit =
                ()
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
                val command = SQLExecution
                  .getQueryExecution(
                    spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
                  )
                  .analyzed
                  .asInstanceOf[AppendData]
                originalReceipt = DeltaRangerDecisionReceipt
                  .consumeOwned(command.analyzedQuery.get, spark, optimized = false)
                trace.source(originalReceipt)
                trace.record(
                  "original-obligations",
                  s"receipt=${trace.id(originalReceipt)} occurrences=${originalReceipt.obligations.size} issuedSource=true"
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
              assertTrue(DeltaRangerOrdinaryRead.pending(original))
              assertFalse(DeltaRangerDecisionReceipt.present(original))
              spark.sessionState.analyzer.batches
              val settings = spark.sessionState.conf.getAllConfs.toMap
              Files.write(
                trace.directory.resolve("before-settings.txt"),
                settings.toVector.sorted.mkString("\n").getBytes(StandardCharsets.UTF_8)
              )
              if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
              else data.write.mode("append").saveAsTable("a.db.t")
              trace.record("write-return", "success=true")
              val qe = completed.poll(10, TimeUnit.SECONDS)
              assertNotNull(qe)
              val command = qe.analyzed.asInstanceOf[AppendData]
              assertNotSame(original, command.analyzedQuery.get)
              assertSame(command.query, command.analyzedQuery.get)
              assertEquals(
                DeltaRangerReadRule.extract(original, spark).map(_._2).toSet,
                originalReceipt.obligations.map(_.columns).toSet
              )
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
              assertReason(
                assertThrows(
                  classOf[SecurityException],
                  () =>
                    DeltaRangerDecisionReceipt
                      .consumeOwned(command.analyzedQuery.get, spark, optimized = false)
                ),
                "configuration changed"
              )
              assertTrue(inputObserved)
              trace.record(
                "physical-transition",
                s"stored=${trace.id(command.analyzedQuery.get)} optimizedStored=${trace.id(optimized.analyzedQuery.get)} opaqueWriter=${trace.id(optimized.write.get)} target=${trace.id(executed.delegate.table)} fallback=${trace.id(executed.delegate.plan)} refresh=${trace.id(executed.delegate.refreshCache)} terminalWriter=${trace.id(executed.delegate.write)} samePlannedExecutedDelegate=true"
              )
              trace.record("physical", qe.executedPlan.treeString)
              trace.record("case-complete", "case=" + frontend + " terminalInputValidated=true")
              Files.write(
                trace.directory.resolve("after-settings.txt"),
                spark.sessionState.conf.getAllConfs.toVector.sorted
                  .mkString("\n")
                  .getBytes(StandardCharsets.UTF_8)
              )
              assertEquals(
                settings + ("spark.databricks.delta.lastCommitVersionInSession" -> "1"),
                spark.sessionState.conf.getAllConfs.toMap
              )
              val outcome = Map[String, Any](
                "schemaVersion" -> 1,
                "testMethod" -> "realNamedFrontendsReachTerminalInputValidation",
                "scenario" -> ("v1-frontend-" + frontend),
                "frontend" -> frontend,
                "returned" -> true,
                "assertionsPassed" -> true,
                "failureCauses" -> java.util.Collections.emptyList[AnyRef](),
                "confBefore" -> settings.asJava,
                "confAfter" -> spark.sessionState.conf.getAllConfs.asJava,
                "cleanup" -> Map(
                  "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
                  "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
                  "commitOwnerActive" -> DeltaRangerAppendCommit.current.nonEmpty
                ).asJava
              )
              Files.write(
                trace.directory.resolve("outcome.json"),
                new ObjectMapper().writeValueAsBytes(outcome.asJava)
              )
            } finally {
              spark.listenerManager.unregister(listener)
              assertNotEquals(
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
              assertNotEquals(objects, afterObjects)
              source.onRequest = _ => ()
              trace.record(
                "cleanup",
                "case=" + frontend + " noTxn=true noNative=true nativeCommitVersion=1 allFilesEqual=false objectsEqual=false"
              )
            }
          },
          Some(NativeSelectFixture.extension(target)),
          Map("spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false")
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

  /** Literal public bare-table insertInto must reach the owned native append input. */
  @Test
  def bareTableInsertIntoCommitsMaskedFilteredRows(): Unit = bareFrontend("insertInto")

  /** Literal public bare-table saveAsTable must append to the existing native target. */
  @Test
  def bareTableSaveAsTableCommitsMaskedFilteredRows(): Unit = bareFrontend("saveAsTable")

  /** The literal insertInto frontend must continue through the installed authorization rule. */
  @Test
  def installedAuthorizationBareTableInsertIntoCommits(): Unit =
    bareFrontend("insertInto", installedAuthorization = true)

  /** The literal saveAsTable frontend must continue through the installed authorization rule. */
  @Test
  def installedAuthorizationBareTableSaveAsTableCommits(): Unit =
    bareFrontend("saveAsTable", installedAuthorization = true)

  /** Each implicit output column must be authorized before target access or source rewrites. */
  @Test
  def bareTableDeniesEverySourceColumnBeforeSideEffects(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      Seq("id", "secret", "region").foreach { column =>
        withCase(s"bare-denied-$frontend-$column") { c =>
          val registration = NativeSelectFixture.bindingFixture(c.spark, "a")
          val properties = registration.properties.asScala.toMap
          c.source.denied.add("b:" + column)
          c.source.requests.clear(); c.source.events.clear()
          c.target.requests.clear(); c.target.events.clear()
          asApp {
            val failure = assertThrows(
              classOf[SecurityException],
              () => {
                if (frontend == "insertInto")
                  c.spark.table("b.db.t").write.mode("append").insertInto("a.db.t")
                else c.spark.table("b.db.t").write.mode("append").saveAsTable("a.db.t")
              }
            )
            assertReason(failure, "denied")
          }
          assertTrue(
            c.source.requests.asScala.exists(r =>
              r.getAccessType == "select" && r.getResource.getValue(
                "column"
              ) == column && r.getUser == "APP"
            )
          )
          assertFalse(c.source.events.contains("mask"));
          assertFalse(c.source.events.contains("filter"))
          assertFalse(c.target.requests.asScala.exists(_.getAccessType == "update"))
          assertEquals(properties, registration.properties.asScala.toMap)
          assertFalse(
            Files.exists(
              Paths.get(
                NativeSelectFixture.locations("a"),
                "_delta_log",
                "00000000000000000001.json"
              )
            )
          )
          c.trace.record(
            "bare-column-denial",
            s"frontend=$frontend column=$column user=APP noUpdate=true noRewrite=true"
          )
        }
      }
    }
  }

  /** Bare-source authority is revalidated when retained input reaches a writer. */
  @Test
  def bareIssuedSourceCannotReuseChangedMaskOrPublication(): Unit = {
    Seq("mask", "publication").foreach { change =>
      withCase("bare-retained-" + change) { c =>
        asApp {
          val data = c.spark.table("b.db.t")
          val receipt = DeltaRangerDecisionReceipt.consumeOwned(
            data.queryExecution.normalized,
            c.spark,
            optimized = false
          )
          assertEquals(Set("id", "secret", "region"), receipt.obligations.head.columns)
          var fired = false
          c.source.onRequest = _ =>
            if (!fired && atInsert) {
              fired = true
              if (change == "mask") c.source.mask = "upper({col})"
              else {
                val next = c.source.plugin.getPoliciesVersion + 1L
                when(c.source.plugin.getPoliciesVersion).thenReturn(next)
                c.source.start()
              }
            }
          try {
            assertReason(
              appendFailure(c, data, recordSource = false),
              if (change == "mask") "action decision changed" else "publication changed"
            )
            assertTrue(fired, "change must reach the actual issued source input")
            assertThrows(classOf[SecurityException], () => receipt.validateAction(c.spark))
          } finally c.source.onRequest = _ => ()
        }
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
          data.queryExecution.normalized,
          c.spark,
          optimized = false
        )
        assertEquals(occurrences, receipt.obligations.size)
        var reached = false
        val policy = if (name == "self") c.target else c.source
        policy.onRequest = request =>
          if (!reached && atInsert) {
            reached = true
            val actual = activeSourceReceipt(c.spark)
            assertEquals(occurrences, actual.obligations.size)
            assertEquals(
              receipt.obligations.map(_.columns).toSet,
              actual.obligations.map(_.columns).toSet
            )
            assertTrue(actual.obligations.forall(_.columns.nonEmpty))
            c.trace.record(
              "original-obligations",
              s"receipt=${c.trace.id(actual)} occurrences=$occurrences sameOriginalDemands=true"
            )
          }
        appendSuccess(c, data, publishes = name != "empty")
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
          data.queryExecution.normalized,
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
            val actual = activeSourceReceipt(c.spark)
            assertEquals(1, actual.obligations.size)
            assertEquals("b", actual.obligations.head.scan.catalog)
            assertSame(target.deltaLog, actual.obligations.head.binding.log())
            c.source.denied.add("b:secret")
            c.trace.record(
              "original-obligations",
              s"receipt=${c.trace.id(actual)} occurrences=1 source=b target=a sameLog=true mutationFired=true"
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
      appendSuccess(c, data)
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
      appendSuccess(c, c.spark.sql("SELECT id, secret, region FROM b.db.t"))
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
      appendSuccess(c, c.spark.sql("SELECT id, secret, region FROM b.db.t"))
  }

  @Test
  def foreignAnalyzerRulesRejectBeforeLaterCallbacks(): Unit =
    foreignAnalyzerProfiles(installedAuthorization = false)

  /** The installed authorization profile still rejects foreign rules before later callbacks. */
  @Test
  def installedAuthorizationRejectsForeignRulesBeforeLaterCallbacks(): Unit =
    foreignAnalyzerProfiles(installedAuthorization = true)

  private def foreignAnalyzerProfiles(installedAuthorization: Boolean): Unit = {
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
        "foreign-" + kind + "-authorization-" + installedAuthorization,
        before = Some(extensions => {
          if (installedAuthorization) installAuthorization(extensions)
          if (kind == "earlier-resolution") foreign(extensions)
        }),
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
    appendSuccess(c, data)
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
      appendSuccess(c, data)
      assertTrue(fired)
  }

  @Test
  def missingAmbiguousAndExpiredReferencesCannotIssueAuthority(): Unit =
    withCase("reference-negatives") { c =>
      val prior = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      val tag =
        TreeNodeTag[DeltaRangerAppendV1Input.Reference]("gravitino.delta.ranger.append-input")
      var expired: DeltaRangerAppendV1Input.Reference = null
      c.source.onRequest = _ =>
        if (expired == null && atInsert) {
          val command = SQLExecution
            .getQueryExecution(
              c.spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
            )
            .analyzed
            .asInstanceOf[AppendData]
          expired = command.analyzedQuery.get.getTagValue(tag).get
        }
      appendSuccess(c, prior)
      c.source.onRequest = _ => ()
      assertNotNull(expired)
      assertTrue(prior.queryExecution.analyzed.getTagValue(tag).isEmpty)
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
      appendSuccess(c, data)
      assertTrue(fired)
      c.source.onRequest = _ => ()
      assertReason(
        appendFailure(c, data, recordSource = false),
        "configuration changed"
      )
    }

  @Test
  def qualifiedInventoryRejectsExcludedFrontends(): Unit = {
    val cases = Seq(
      "values" -> "missing original pending source",
      "insert-select" -> "missing original pending source",
      "raw-append" -> "unnamed target",
      "raw-select" -> "Raw Delta paths are not admitted",
      "streaming" -> "Unqualified Delta Ranger route: unresolved or streaming plan",
      "overwrite" -> "Unqualified Delta Ranger route: OverwriteByExpression",
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
    qualifiedFrontend("writeTo", "Delta append input rejected: missing original pending source")

  private def qualifiedFrontend(route: String, expected: String): Unit =
    withCase("qualified-" + route) { c =>
      DeltaRangerAppendInventory.qualify(c.spark)
      val data = c.spark.sql("SELECT id, secret, region FROM b.db.t")
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(
        data.queryExecution.normalized,
        c.spark,
        optimized = false
      )
      c.trace.source(receipt)
      c.trace.record(
        "qualified-source",
        s"route=$route normalizedParentReceipt=${c.trace.id(receipt)} logicalIsAnalyzed=${data.queryExecution.logical eq data.queryExecution.analyzed} logicalHasReceipt=${DeltaRangerDecisionReceipt.present(data.queryExecution.logical)}"
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

  private def installAuthorization(extensions: SparkSessionExtensions): Unit = {
    new GravitinoAuthorizationSparkSessionExtensions().apply(extensions)
    ()
  }

  private def bareFrontend(frontend: String, installedAuthorization: Boolean = false): Unit =
    withCase(
      "bare-" + frontend + (if (installedAuthorization) "-authorization" else ""),
      before = if (installedAuthorization) Some(installAuthorization) else None
    ) { c =>
      val rules = c.spark.sessionState.analyzer.postHocResolutionRules
      assertEquals(if (installedAuthorization) 12 else 11, rules.size)
      if (installedAuthorization) {
        assertEquals(classOf[RequiredPrivilegesCheck], rules(7).getClass)
        assertEquals(1, rules.count(_.getClass == classOf[RequiredPrivilegesCheck]))
      }
      DeltaRangerAppendInventory.qualify(c.spark)
      val registration = NativeSelectFixture.bindingFixture(c.spark, "a")
      val properties = registration.properties.asScala.toMap
      assertEquals("17", System.getProperty("java.specification.version"))
      val sparkOrigin = classOf[SparkSession].getProtectionDomain.getCodeSource.getLocation
      val deltaOrigin = classOf[DeltaLog].getProtectionDomain.getCodeSource.getLocation
      val scalaOrigin = classOf[Option[_]].getProtectionDomain.getCodeSource.getLocation
      assertTrue(sparkOrigin.toString.endsWith(s"spark-sql_2.12-${c.spark.version}.jar"))
      assertTrue(scalaOrigin.toString.endsWith("scala-library-2.12.21.jar"))
      Option(System.getProperty("bare.source.spark")).foreach(expected =>
        assertEquals(expected, c.spark.version)
      )
      Option(System.getProperty("bare.source.delta")).foreach(expected =>
        assertTrue(deltaOrigin.toString.endsWith(s"delta-spark_2.12-$expected.jar"))
      )
      c.trace.record(
        "loaded-runtime",
        s"spark=${c.spark.version} sparkOrigin=$sparkOrigin deltaOrigin=$deltaOrigin scalaOrigin=$scalaOrigin java=17"
      )
      var inputObserved = false
      c.source.onRequest = _ =>
        if (atInsert && !inputObserved) {
          inputObserved = true
          val executionId = c.spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong
          val command =
            SQLExecution.getQueryExecution(executionId).analyzed.asInstanceOf[AppendData]
          assertTrue(command.writeOptions.isEmpty)
          val receipt = DeltaRangerDecisionReceipt.consumeOwned(
            command.analyzedQuery.get,
            c.spark,
            optimized = false
          )
          assertEquals(1, receipt.obligations.size)
          assertEquals("b", receipt.obligations.head.scan.catalog)
          assertEquals(Set("id", "secret", "region"), receipt.obligations.head.columns)
          c.trace.source(receipt)
          assertNull(DeltaRangerNativeOperation.current())
          assertTrue(OptimisticTransaction.getActive.isEmpty)
        }
      c.source.requests.clear(); c.source.events.clear()
      c.target.requests.clear(); c.target.events.clear()
      try
        asApp {
          if (frontend == "insertInto")
            c.spark.table("b.db.t").write.mode("append").insertInto("a.db.t")
          else c.spark.table("b.db.t").write.mode("append").saveAsTable("a.db.t")
          c.successfulReturns += 1
          c.committed += 1
          assertTrue(inputObserved)
          assertEquals("1", c.spark.conf.get("spark.databricks.delta.lastCommitVersionInSession"))
          assertEquals(
            "false",
            c.spark.conf.get("spark.databricks.delta.write.txnVersion.autoReset.enabled")
          )
          val selects = c.source.requests.asScala.filter(_.getAccessType == "select")
          assertEquals(
            Set("id", "secret", "region"),
            selects.flatMap(r => Option(r.getResource.getValue("column")).map(_.toString)).toSet
          )
          assertTrue(
            selects.forall(r => r.getUser == "APP" && r.getResource.getValue("catalog") == "b")
          )
          assertTrue(
            c.target.requests.asScala.exists(r => r.getAccessType == "update" && r.getUser == "APP")
          )
          val events = c.source.events.asScala.toVector
          assertEquals(3, events.takeWhile(_.startsWith("access:")).size)
          assertEquals(properties, registration.properties.asScala.toMap)
          val location = Paths.get(NativeSelectFixture.locations("a"))
          val log = location.resolve("_delta_log")
          assertFalse(Files.exists(log.resolve("00000000000000000002.json")))
          val actions = Files
            .readAllLines(log.resolve("00000000000000000001.json"))
            .asScala
            .map(new ObjectMapper().readTree(_))
            .toVector
          assertEquals(1, actions.count(_.has("commitInfo")))
          assertEquals(
            "WRITE",
            actions.find(_.has("commitInfo")).get.get("commitInfo").get("operation").asText()
          )
          assertEquals(
            "Append",
            actions
              .find(_.has("commitInfo"))
              .get
              .get("commitInfo")
              .get("operationParameters")
              .get("mode")
              .asText()
          )
          assertTrue(actions.forall(a => a.has("commitInfo") || a.has("add")))
          val adds = actions.filter(_.has("add")).map(_.get("add"))
          assertTrue(adds.nonEmpty)
          assertTrue(adds.forall(_.get("dataChange").asBoolean()))
          val rows = adds.flatMap { add =>
            val reader = ParquetReader
              .builder[Group](
                new GroupReadSupport(),
                new HadoopPath(location.resolve(add.get("path").asText()).toUri)
              )
              .withConf(c.spark.sparkContext.hadoopConfiguration)
              .build()
            try
              Iterator
                .continually(reader.read())
                .takeWhile(_ != null)
                .map { group =>
                  Row(
                    group.getLong("id", 0),
                    if (group.getFieldRepetitionCount("secret") == 0) null
                    else group.getString("secret", 0),
                    group.getString("region", 0)
                  )
                }
                .toVector
            finally reader.close()
          }
          assertEquals(
            Vector(Row(1L, "b-one-masked", "keep"), Row(3L, null, "keep")).sortBy(_.getLong(0)),
            rows.sortBy(_.getLong(0))
          )
          assertTrue(OptimisticTransaction.getActive.isEmpty)
          assertNull(DeltaRangerNativeOperation.current())
          assertTrue(DeltaRangerAppendCommit.current.isEmpty)
          c.trace.record(
            "bare-append",
            s"frontend=$frontend user=APP version=1 appendCount=1 appendedRows=2 nullRows=1 fullSourceColumns=true"
          )
        }
      finally c.source.onRequest = _ => ()
    }

  private def asApp(body: => Unit): Unit =
    UserGroupInformation
      .createUserForTesting("APP", Array.empty[String])
      .doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = body
      })

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
  ) {
    var committed = 0
    var successfulReturns = 0
    var assertionsPassed = false
    val attempts = scala.collection.mutable.ArrayBuffer.empty[java.util.Map[String, Any]]
  }

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
          val c = Case(spark, target, source, trace)
          try {
            body(c)
            c.assertionsPassed = true
            trace.record("case-complete", "case=" + name)
          } finally {
            val after =
              InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
            if (c.successfulReturns == 0) assertEquals(before, after)
            else assertNotEquals(before, after)
            val afterObjects = inventory(root)
            Files.write(
              trace.directory.resolve("after-objects.txt"),
              afterObjects.mkString("\n").getBytes(StandardCharsets.UTF_8)
            )
            if (c.successfulReturns == 0) assertEquals(objects, afterObjects)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            val expectedSettings =
              if (c.committed == 0) settings
              else
                settings + ("spark.databricks.delta.lastCommitVersionInSession" -> c.committed.toString)
            assertEquals(expectedSettings, spark.sessionState.conf.getAllConfs.toMap)
            val method = Thread
              .currentThread()
              .getStackTrace
              .find(frame =>
                frame.getClassName == classOf[TestDeltaRangerAppendV1Input].getName &&
                  !frame.getMethodName.contains("$") && frame.getMethodName != "withCase" &&
                  frame.getMethodName != "bareFrontend"
              )
              .get
              .getMethodName
            val outcome = Map[String, Any](
              "schemaVersion" -> 1,
              "testMethod" -> method,
              "scenario" -> ("v1-regression-" + name),
              "assertionsPassed" -> c.assertionsPassed,
              "apiAttempts" -> c.attempts.asJava,
              "successfulAppendCount" -> c.successfulReturns,
              "committedAppendCount" -> c.committed,
              "confBefore" -> settings.asJava,
              "confAfter" -> spark.sessionState.conf.getAllConfs.asJava,
              "cleanup" -> Map(
                "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
                "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
                "commitOwnerActive" -> DeltaRangerAppendCommit.current.nonEmpty
              ).asJava
            )
            Files.write(
              trace.directory.resolve("outcome.json"),
              new ObjectMapper().writeValueAsBytes(outcome.asJava)
            )
            trace.record(
              "cleanup",
              s"case=$name noTxn=true noNative=true commits=${c.committed} allFilesEqual=${before == after} objectsEqual=${objects == afterObjects}"
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
        }),
        Map("spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false")
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
  private def activeSourceReceipt(spark: SparkSession): DeltaRangerDecisionReceipt = {
    val command = SQLExecution
      .getQueryExecution(spark.sparkContext.getLocalProperty("spark.sql.execution.id").toLong)
      .analyzed
      .asInstanceOf[AppendData]
    val tag =
      TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")
    command.analyzedQuery.get.getTagValue(tag).get.receipt
  }

  // Source tracing is metadata-only: tests must not authorize pending input before the
  // actual typed append boundary whose ordering and payload denial they are exercising.
  private def recordPendingSource(c: Case, data: Dataset[Row], name: String): Unit = {
    val scans = DeltaRangerReadRule.extract(data.queryExecution.analyzed, c.spark)
    c.trace.record(
      "pending-source",
      s"attempt=$name occurrences=${scans.size} resources=${scans.map { case (scan, columns) =>
          (scan.catalog, scan.database, scan.table, columns)
        }}"
    )
  }

  private def appendSuccess(c: Case, data: Dataset[Row], publishes: Boolean = true): Unit = {
    val root = Paths.get(NativeSelectFixture.locations("a")).getParent
    val name = "attempt-" + java.util.UUID.randomUUID().toString
    val before =
      InstalledAppendBoundaryProbe.manifest(root, c.trace.directory.resolve(name + "-before"))
    recordPendingSource(c, data, name)
    var returned = false
    try {
      data.write.mode("append").insertInto("a.db.t")
      returned = true
      c.successfulReturns += 1
      if (publishes) c.committed += 1
      c.attempts += Map[String, Any](
        "attempt" -> name,
        "frontend" -> "insertInto",
        "returned" -> true,
        "failureCauses" -> java.util.Collections.emptyList[AnyRef]()
      ).asJava
      if (publishes)
        assertEquals(
          c.committed.toString,
          c.spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
        )
      c.trace.record("attempt-return", s"attempt=$name success=true version=${c.committed}")
    } finally {
      val after =
        InstalledAppendBoundaryProbe.manifest(root, c.trace.directory.resolve(name + "-after"))
      if (returned) assertNotEquals(before, after)
      if (returned && !publishes) {
        assertTrue(before.forall { case (path, stamp) => after.get(path).contains(stamp) })
        assertEquals(
          before.filter(_._1.contains("/_delta_log/")),
          after.filter(_._1.contains("/_delta_log/"))
        )
      }
      assertTrue(OptimisticTransaction.getActive.isEmpty)
      assertNull(DeltaRangerNativeOperation.current())
    }
  }

  private def appendFailure(
      c: Case,
      data: Dataset[Row],
      recordSource: Boolean = true
  ): Exception = {
    val root = Paths.get(NativeSelectFixture.locations("a")).getParent
    val name = "attempt-" + java.util.UUID.randomUUID().toString
    val before =
      InstalledAppendBoundaryProbe.manifest(root, c.trace.directory.resolve(name + "-before"))
    val objects = inventory(root)
    if (recordSource) recordPendingSource(c, data, name)
    try {
      val failure =
        assertThrows(classOf[Exception], () => data.write.mode("append").insertInto("a.db.t"))
      c.attempts += Map[String, Any](
        "attempt" -> name,
        "frontend" -> "insertInto",
        "returned" -> false,
        "failureCauses" -> Iterator
          .iterate[Throwable](failure)(_.getCause)
          .takeWhile(_ != null)
          .map(error =>
            Map("class" -> error.getClass.getName, "message" -> error.getMessage).asJava
          )
          .toVector
          .asJava
      ).asJava
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
