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

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{FutureTask, LinkedBlockingQueue, TimeUnit}
import java.util.LinkedHashMap
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.util.QueryExecutionListener
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{
  Descending,
  AttributeReference,
  Alias,
  ExprId,
  Literal,
  GenericInternalRow,
  NamedExpression,
  ScalaUDF
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  AppendData,
  LogicalPlan,
  LocalRelation,
  MapElements,
  Project,
  RepartitionByExpression,
  Sort
}
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution, SQLExecution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.SingleAction
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerNativeGovernedAppend {
  @Test
  def nativeCheckViolationRetainsItsExceptionBeforeCommit(): Unit = violatedAppend()

  @Test
  def originalQueryExecutionRetainsNativeCheckViolation(): Unit =
    violatedAppend(frontend = "saveAsTable", nullDefault = false)

  @Test
  def helperWitnessRejectsChangedPlansFunctionsExecutionAndExpiredOwners(): Unit = {
    var retained: DeltaRangerAppendHelper = null
    var expected: LogicalPlan = null
    violatedAppend(check = (helper, spark) => {
      retained = helper
      val original = field[LogicalPlan](helper, "original")
      val source = field[LogicalPlan](helper, "expectedSource")
      expected = field[Vector[LogicalPlan]](helper, "statsPlans").last
      assertTrue(corresponds(expected, expected, original))
      assertTrue(corresponds(source, source, original))
      val changedIds = scala.collection.mutable.Map.empty[ExprId, ExprId]
      val changedSource = source.transformAllExpressions { case a: AttributeReference =>
        a.withExprId(changedIds.getOrElseUpdate(a.exprId, NamedExpression.newExprId))
      }
      assertFalse(corresponds(source, changedSource, original))
      assertFalse(
        corresponds(source, source.transformDown { case filter: Filter => filter.child }, original)
      )
      val changedProjection = Project(
        source.output.dropRight(1) :+ Alias(Literal("changed"), source.output.last.name)(),
        original
      )
      assertFalse(corresponds(source, changedProjection, original))
      val local = field[LocalRelation](helper, "local")
      assertFalse(
        helper.analysis(local.copy(data = Seq(new GenericInternalRow(local.output.size))), spark)
      )
      val constraint = field[Vector[LogicalPlan]](helper, "constraintPlans").last
      val nativeWrapper = Class
        .forName("org.apache.spark.sql.delta.constraints.ExpressionLogicalPlanWrapper")
        .getDeclaredConstructors
        .find(_.getParameterCount == 1)
        .get
      nativeWrapper.setAccessible(true)
      val changedConstraint = constraint.expressions.head.transform {
        case Literal(_: UTF8String, _) => Literal("changed-pattern")
      }
      val changedWrapper = nativeWrapper.newInstance(changedConstraint).asInstanceOf[LogicalPlan]
      assertReason(
        assertThrows(classOf[SecurityException], () => helper.analysis(changedWrapper, spark)),
        "constraint expression changed"
      )
      assertReason(
        assertThrows(
          classOf[Exception],
          () =>
            new QueryExecution(
              spark,
              nativeWrapper.newInstance(constraint.expressions.head).asInstanceOf[LogicalPlan]
            ).toRdd
        ),
        "analysis-only helper cannot execute"
      )
      val changedStats = expected.transformAllExpressions { case Literal(32, _) => Literal(31) }
      assertFalse(corresponds(expected, changedStats, original))
      val arbitraryFunction = expected.transformAllExpressions { case udf: ScalaUDF =>
        udf.copy(function = (value: String) => value)
      }
      assertFalse(corresponds(expected, arbitraryFunction, original))
      val changedCapture = expected.transformAllExpressions { case udf: ScalaUDF =>
        val function = serializedCopy(udf.function)
        val captureField = function.getClass.getDeclaredField("arg$1")
        captureField.setAccessible(true)
        val capture = captureField.get(function)
        val prefix = capture.getClass.getDeclaredField("stringPrefix$1")
        prefix.setAccessible(true)
        prefix.setInt(capture, prefix.getInt(capture) + 1)
        udf.copy(function = function)
      }
      assertFalse(corresponds(expected, changedCapture, original))
      val foreign = spark.newSession()
      assertReason(
        assertThrows(classOf[SecurityException], () => helper.analysis(expected, foreign)),
        "helper session changed"
      )
      assertReason(
        assertThrows(classOf[SecurityException], () => helper.write()),
        "nested or expired helper"
      )
      assertReason(
        assertThrows(classOf[Exception], () => new QueryExecution(spark, expected).optimizedPlan),
        "analysis-only helper cannot execute"
      )
      assertReason(
        assertThrows(classOf[Exception], () => new QueryExecution(spark, source).optimizedPlan),
        "analysis-only helper cannot execute"
      )
    })
    assertNotNull(retained)
    assertReason(
      assertThrows(classOf[SecurityException], () => retained.requireLive()),
      "expired helper owner"
    )
  }

  @Test
  def executingHelperRejectsFirstIssuanceWithBorrowedTracker(): Unit = violatedAppend(
    executingOnly = true,
    check = (helper, spark) => {
      assertNull(field[AnyRef](helper, "tracker"))
      assertNull(field[AnyRef](helper, "scanOwner"))
      val tracker =
        QueryPlanningTracker.get.getOrElse(fail("Expected actual native analysis tracker"))
      val source = field[LogicalPlan](helper, "expectedSource")
      val foreign = new QueryExecution(spark, source, tracker)
      assertReason(assertThrows(classOf[Exception], () => foreign.toRdd), "helper analysis reentry")
      assertNull(field[AnyRef](helper, "scanOwner"))
    }
  )

  @Test
  def executingHelperRejectsFirstPreparationReentryWithBorrowedTracker(): Unit = violatedAppend(
    executingOnly = true,
    executingPhase = "prepare",
    check = (helper, spark) => {
      val tracker = field[QueryPlanningTracker](helper, "tracker")
      assertNotNull(tracker)
      assertNull(field[AnyRef](helper, "scanOwner"))
      val source = field[LogicalPlan](helper, "expectedSource")
      val foreign = new QueryExecution(spark, source, tracker)
      assertReason(assertThrows(classOf[Exception], () => foreign.toRdd), "helper analysis reentry")
      assertNull(field[AnyRef](helper, "scanOwner"))
    }
  )

  private def violatedAppend(
      frontend: String = "insertInto",
      nullDefault: Boolean = true,
      executingOnly: Boolean = false,
      executingPhase: String = "analysis",
      check: (DeltaRangerAppendHelper, SparkSession) => Unit = (_, _) => ()
  ): Unit = {
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val trace = InstalledAppendBoundaryProbe.fresh("native-check-violation")
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          val helperOwners =
            field[ThreadLocal[DeltaRangerAppendHelper]](DeltaRangerAppendHelper, "active")
          var checked = false
          val metadata = NativeSelectFixture.bindingFixture(spark, "a")
          metadata.onRead = _ => {
            val helper = helperOwners.get()
            if (
              helper != null && !checked && (!executingOnly || (
                field[Boolean](helper, "executing") && Thread
                  .currentThread()
                  .getStackTrace
                  .exists(frame =>
                    frame.getClassName == classOf[
                      DeltaRangerAppendHelper
                    ].getName && frame.getMethodName == executingPhase
                  )
              ))
            ) {
              checked = true
              if (executingOnly)
                trace.record(
                  "helper-first-issuance-before",
                  s"phase=$executingPhase executing=${field[Boolean](helper, "executing")} trackerPresent=${field[AnyRef](helper, "tracker") != null} scanOwnerPresent=${field[AnyRef](helper, "scanOwner") != null}"
                )
              check(helper, spark)
              if (executingOnly)
                trace.record(
                  "helper-first-issuance-after",
                  s"phase=$executingPhase foreignQueryRejected=true scanOwnerPresent=${field[AnyRef](helper, "scanOwner") != null}"
                )
            }
          }
          val root = Paths.get(NativeSelectFixture.locations("a"))
          InstalledAppendBoundaryProbe.manifest(root.getParent, trace.directory.resolve("before"))
          spark.sessionState.analyzer.batches
          val confBefore = spark.sessionState.conf.getAllConfs
          val before = InstalledAppendBoundaryProbe.manifest(
            root.resolve("_delta_log"),
            trace.directory.resolve("before-log")
          )
          val failure = assertThrows(
            classOf[Exception],
            () => {
              val writer = spark.sql("SELECT id, secret, region FROM b.db.t").write.mode("append")
              frontend match {
                case "insertInto"  => writer.insertInto("a.db.t")
                case "saveAsTable" => writer.saveAsTable("a.db.t")
                case other         => throw new IllegalArgumentException(other)
              }
            }
          )
          InstalledAppendBoundaryProbe.manifest(root.getParent, trace.directory.resolve("after"))
          assertTrue(checked, "helper callback must run")
          metadata.onRead = _ => ()
          trace.record("frontend", s"name=$frontend nullAsDefault=$nullDefault")
          val causes =
            Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
          trace.record("failure", causes.mkString(" | "))
          assertTrue(
            causes.exists(_.getClass.getSimpleName == "DeltaInvariantViolationException"),
            causes.mkString(" | ")
          )
          assertEquals(
            before,
            InstalledAppendBoundaryProbe.manifest(
              root.resolve("_delta_log"),
              trace.directory.resolve("after-log")
            )
          )
          assertTrue(OptimisticTransaction.getActive.isEmpty)
          assertNull(DeltaRangerNativeOperation.current())
          val confAfter = spark.sessionState.conf.getAllConfs
          assertEquals(confBefore, confAfter)
          val healthy = spark.sql("SELECT id FROM b.db.t").collect().toVector
          assertTrue(healthy.nonEmpty)
          val method = Thread
            .currentThread()
            .getStackTrace
            .find(frame =>
              frame.getClassName == classOf[TestDeltaRangerNativeGovernedAppend].getName &&
                !frame.getMethodName.contains("$") && frame.getMethodName != "violatedAppend"
            )
            .get
            .getMethodName
          val outcome = Map[String, Any](
            "schemaVersion" -> 1,
            "testMethod" -> method,
            "scenario" -> "native-check-violation",
            "frontend" -> frontend,
            "nullAsDefault" -> nullDefault,
            "returned" -> false,
            "helperObserved" -> checked,
            "failureCauses" -> causes
              .map(error =>
                Map("class" -> error.getClass.getName, "message" -> error.getMessage).asJava
              )
              .asJava,
            "confBefore" -> confBefore.asJava,
            "confAfter" -> confAfter.asJava,
            "cleanup" -> Map[String, Any](
              "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
              "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
              "helperActive" -> (helperOwners.get() != null),
              "commitOwnerActive" -> DeltaRangerAppendCommit.current.nonEmpty,
              "healthySelectRows" -> healthy.map(_.toSeq.asJava).asJava
            ).asJava
          )
          Files.write(
            trace.directory.resolve("outcome.json"),
            new ObjectMapper().writeValueAsBytes(outcome.asJava)
          )
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> nullDefault.toString
        ),
        native =>
          native
            .sql(
              "ALTER TABLE delta.`" + NativeSelectFixture.locations(
                "a"
              ) + "` ADD CONSTRAINT native_secret CHECK (secret IS NULL OR secret NOT LIKE '%-masked')"
            )
            .collect()
      )
    finally {
      target.engine.close()
      source.engine.close()
      trace.close()
    }
  }

  @Test
  def installedNamedAppendCommitsMaskedFilteredSource(): Unit = successfulAppend()

  @Test
  def commitWitnessRejectsChangedReconstructionCopyAndExpiredOwner(): Unit = {
    var retained: DeltaRangerAppendCommit = null
    successfulAppend(
      testMethod = "commitWitnessRejectsChangedReconstructionCopyAndExpiredOwner",
      inspect = (commit, spark) => {
        retained = commit
        val cache =
          field[scala.collection.mutable.ArrayBuffer[(LogicalRDD, Vector[Any], QueryExecution)]](
            commit.metadata,
            "caches"
          ).head
        val original = cache._1
        val qe = cache._3
        assertTrue(commit.contains(original))
        val parentField = original.rdd.getClass.getDeclaredField("prev")
        parentField.setAccessible(true)
        val parent = parentField.get(original.rdd)
        parentField.set(original.rdd, spark.sparkContext.emptyRDD)
        try
          assertReason(
            assertThrows(classOf[SecurityException], () => commit.contains(original)),
            "cached state parent changed"
          )
        finally parentField.set(original.rdd, parent)
        assertTrue(commit.contains(original))
        val changedOrder = qe.analyzed.transformDown { case sort: Sort =>
          sort.copy(order = sort.order.map(_.copy(direction = Descending)))
        }
        assertThrows(classOf[SecurityException], () => commit.verifyReconstruction(changedOrder))
        val changedOutput = original.copy(output =
          original.output.map(_.withExprId(NamedExpression.newExprId))
        )(spark, None, None)
        assertReason(
          assertThrows(classOf[SecurityException], () => commit.contains(changedOutput)),
          "cached state output changed"
        )
        val changedCopy = LogicalRDD(original.output, qe.toRdd.map(row => row.copy()))(spark)
        assertReason(
          assertThrows(classOf[SecurityException], () => commit.contains(changedCopy)),
          "nonnative cache function"
        )
        val extraParent = LogicalRDD(original.output, original.rdd.map(row => row.copy()))(spark)
        assertFalse(commit.contains(extraParent))
        val previous = spark.conf.getOption("spark.sql.shuffle.partitions")
        spark.conf.set("spark.sql.shuffle.partitions", "7")
        try assertThrows(classOf[SecurityException], () => commit.requireLive())
        finally previous.foreach(spark.conf.set("spark.sql.shuffle.partitions", _))
        assertReason(
          assertThrows(classOf[Exception], () => new QueryExecution(spark, original).toRdd),
          "unqualified commit consumer"
        )
      }
    )
    assertNotNull(retained)
    assertReason(
      assertThrows(classOf[SecurityException], () => retained.requireLive()),
      "expired commit owner"
    )
  }

  @Test
  def nativeCheckpointHookWritesThresholdArtifacts(): Unit = successfulAppend(
    scenario = "checkpoint-threshold",
    testMethod = "nativeCheckpointHookWritesThresholdArtifacts",
    seedCommits = 9,
    inspectCheckpoint = (commit, plan) => {
      assertTrue(commit.checkpointPlan(plan, complete = true))
      val changedPartitioning = plan.transformDown { case repartition: RepartitionByExpression =>
        repartition.copy(optNumPartitions = Some(2))
      }
      assertReason(
        assertThrows(classOf[SecurityException], () => commit.checkpointPlan(changedPartitioning)),
        "complete checkpoint recipe changed"
      )
      val changedFunction = plan.transformDown { case map: MapElements =>
        map.copy(func = (action: SingleAction) => action)
      }
      assertReason(
        assertThrows(classOf[SecurityException], () => commit.checkpointPlan(changedFunction)),
        "nonnative cache function"
      )
      val changedProjection = plan
        .asInstanceOf[Project]
        .copy(projectList = plan.asInstanceOf[Project].projectList.reverse)
      assertReason(
        assertThrows(classOf[SecurityException], () => commit.checkpointPlan(changedProjection)),
        "complete checkpoint recipe changed"
      )
    }
  )

  @Test
  def configuredMultipartCheckpointWritesAllNativeParts(): Unit = successfulAppend(
    scenario = "checkpoint-configured-multipart",
    testMethod = "configuredMultipartCheckpointWritesAllNativeParts",
    seedCommits = 9,
    checkpointPartSize = Some(1L),
    checkpointTail = true,
    inspectCheckpoint = checkpointMutations
  )

  @Test
  def checkpointPartitionCountIncludesRetainedRemovesAndRoundsUp(): Unit = successfulAppend(
    frontend = "saveAsTable",
    scenario = "checkpoint-tombstone-rounding",
    testMethod = "checkpointPartitionCountIncludesRetainedRemovesAndRoundsUp",
    seedCommits = 9,
    checkpointPartSize = Some(5L),
    rewriteTarget = true,
    inspectCheckpoint = checkpointMutations
  )

  @Test
  def checkpointCanOmitJsonStatsWithoutDisablingCollection(): Unit = successfulAppend(
    scenario = "checkpoint-json-disabled",
    testMethod = "checkpointCanOmitJsonStatsWithoutDisablingCollection",
    seedCommits = 9,
    checkpointJson = false,
    inspectCheckpoint = checkpointMutations
  )

  @Test
  def partitionedCheckpointHonorsBothStatisticsFlags(): Unit = {
    for (json <- Seq(true, false); struct <- Seq(true, false)) {
      successfulAppend(
        frontend = if (struct) "insertInto" else "saveAsTable",
        scenario = s"checkpoint-partitioned-json-$json-struct-$struct",
        testMethod = "partitionedCheckpointHonorsBothStatisticsFlags",
        seedCommits = 9,
        checkpointJson = json,
        checkpointStruct = struct,
        partitionedTarget = true,
        inspectCheckpoint = checkpointMutations
      )
    }
  }

  @Test
  def nativeCheckpointFailureKeepsPublishedCommitAndReturnsNormally(): Unit = successfulAppend(
    scenario = "native-checkpoint-failure-after-publication",
    testMethod = "nativeCheckpointFailureKeepsPublishedCommitAndReturnsNormally",
    seedCommits = 9,
    checkpointFailure = true
  )

  @Test
  def priorOtherTableCommitVersionDoesNotImplyPublication(): Unit = successfulAppend(
    scenario = "prior-equal-session-version",
    testMethod = "priorOtherTableCommitVersionDoesNotImplyPublication",
    priorCommitVersion = Some(1L)
  )

  @Test
  def originalNamedQueryExecutionCommitsWithoutNullDefaults(): Unit = successfulAppend(
    frontend = "saveAsTable",
    nullDefault = false,
    scenario = "original-qe-no-null-defaults",
    testMethod = "originalNamedQueryExecutionCommitsWithoutNullDefaults"
  )

  @Test
  def completedNativeCommandResultCannotReplayAppend(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      successfulAppend(
        frontend = frontend,
        scenario = "completed-result-no-replay-" + frontend,
        testMethod = "completedNativeCommandResultCannotReplayAppend",
        repeatCompletedResult = true
      )
    }
  }

  @Test
  def sameTargetRepeatedAndMixedSourcesPreserveLiteralRows(): Unit = {
    Vector(
      (
        "same-target",
        "SELECT id, secret, region FROM a.db.t WHERE id = 1",
        Vector(Row(1L, "a-one-masked", "keep"))
      ),
      (
        "repeated-target",
        "SELECT l.id, l.secret, l.region FROM a.db.t l JOIN a.db.t r ON l.id = r.id",
        Vector(Row(1L, "a-one-masked", "keep"), Row(3L, null, "keep"))
      ),
      (
        "mixed-target",
        "SELECT l.id, l.secret, l.region FROM a.db.t l JOIN b.db.t r ON l.id = r.id",
        Vector(Row(1L, "a-one-masked", "keep"), Row(3L, null, "keep"))
      )
    ).foreach { case (name, sql, rows) =>
      successfulAppend(
        scenario = name + "-literal-readback",
        testMethod = "sameTargetRepeatedAndMixedSourcesPreserveLiteralRows",
        sourceSql = sql,
        expectedAppendedRows = rows
      )
    }
  }

  @Test
  def insertedNamedQueryExecutionCommitsWithoutNullDefaults(): Unit = successfulAppend(
    nullDefault = false,
    scenario = "insert-original-qe-no-null-defaults",
    testMethod = "insertedNamedQueryExecutionCommitsWithoutNullDefaults"
  )

  @Test
  def checkpointAndJsonTailRemainExactCommitInputs(): Unit = successfulAppend(
    scenario = "checkpoint-json-tail",
    testMethod = "checkpointAndJsonTailRemainExactCommitInputs",
    seedCommits = 10
  )

  @Test
  def nativeWarmCacheRebuildFailsClosedAfterPublication(): Unit = successfulAppend(
    scenario = "warm-cache-rebuild-denied-after-publication",
    testMethod = "nativeWarmCacheRebuildFailsClosedAfterPublication",
    warmCache = true
  )

  @Test
  def nativeWarmCacheRebuildWithDeployment(): Unit = successfulAppend(
    scenario = "warm-cache-rebuild-enabled",
    testMethod = "nativeWarmCacheRebuildWithDeployment",
    warmCache = true,
    rawMaintenance = true
  )

  @Test
  def postpublicationFailureKeepsPrimaryAndUnconditionallyClosesOwners(): Unit = {
    val primary = new AssertionError("native-commit-callback-failure")
    successfulAppend(
      scenario = "postpublication-primary-and-cleanup-failure",
      testMethod = "postpublicationFailureKeepsPrimaryAndUnconditionallyClosesOwners",
      failureCheck = Some(error => {
        val causes = Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).toVector
        assertTrue(causes.exists(_ eq primary))
        assertTrue(causes.exists(_.getSuppressed.exists(_.isInstanceOf[SecurityException])))
      }),
      inspect = (_, _) => {
        val current = classOf[DeltaRangerNativeOperation].getDeclaredField("CURRENT")
        current.setAccessible(true)
        current.get(null).asInstanceOf[ThreadLocal[DeltaRangerNativeOperation]].remove()
        throw primary
      }
    )
  }

  @Test
  def sourceRevocationAfterHelperPreservesUncommittedFiles(): Unit = successfulAppend(
    scenario = "source-revoked-after-helper",
    testMethod = "sourceRevocationAfterHelperPreservesUncommittedFiles",
    revokeAfterHelper = true,
    failureCheck = Some(error => assertReason(error, "denied"))
  )

  @Test
  def sourceRegistrationChangeAfterHelperPreservesUncommittedFiles(): Unit = successfulAppend(
    scenario = "source-registration-changed-after-helper",
    testMethod = "sourceRegistrationChangeAfterHelperPreservesUncommittedFiles",
    invalidateSourceRegistrationAfterHelper = true,
    failureCheck =
      Some(error => assertReason(error, "Published registration metadata is unavailable"))
  )

  @Test
  def emptyAuthorizedSourceReturnsWithoutAnEmptyCommit(): Unit = successfulAppend(
    scenario = "empty-authorized-source",
    testMethod = "emptyAuthorizedSourceReturnsWithoutAnEmptyCommit",
    emptySource = true
  )

  @Test
  def nativeCheckConstraintAcceptsGovernedRows(): Unit = successfulAppend(
    scenario = "native-check-success",
    testMethod = "nativeCheckConstraintAcceptsGovernedRows",
    checkConstraint = true
  )

  private def checkpointMutations(commit: DeltaRangerAppendCommit, plan: LogicalPlan): Unit = {
    val jobsBefore = checkpointJobIds(commit.session())
    assertTrue(commit.checkpointPlan(plan, complete = true))
    val changed = plan.transformDown { case repartition: RepartitionByExpression =>
      repartition.copy(optNumPartitions = repartition.optNumPartitions.map(_ + 1))
    }
    assertReason(
      assertThrows(classOf[SecurityException], () => commit.checkpointPlan(changed)),
      "complete checkpoint recipe changed"
    )
    val reordered = plan.asInstanceOf[Project]
    assertReason(
      assertThrows(
        classOf[SecurityException],
        () => commit.checkpointPlan(reordered.copy(projectList = reordered.projectList.reverse))
      ),
      "complete checkpoint recipe changed"
    )
    for (name <- Seq("stats", "partitionValues_parsed")) {
      val projection = plan.asInstanceOf[Project]
      val changedFields = projection.copy(projectList =
        projection.projectList.map(expression =>
          expression
            .transform {
              case Literal(value, StringType) if value != null && value.toString == name =>
                Literal("foreign_" + name)
            }
            .asInstanceOf[NamedExpression]
        )
      )
      if (changedFields != plan)
        assertReason(
          assertThrows(classOf[SecurityException], () => commit.checkpointPlan(changedFields)),
          "complete checkpoint recipe changed"
        )
    }
    assertEquals(
      jobsBefore,
      checkpointJobIds(commit.session()),
      "checkpoint verification must not start Spark jobs"
    )
  }

  private def checkpointJobIds(spark: SparkSession): Set[Int] = {
    val bus = spark.sparkContext.getClass.getMethod("listenerBus").invoke(spark.sparkContext)
    bus.getClass.getMethod("waitUntilEmpty", java.lang.Long.TYPE).invoke(bus, Long.box(10000L))
    spark.sparkContext.statusTracker.getJobIdsForGroup(null).toSet
  }

  private def successfulAppend(
      frontend: String = "insertInto",
      nullDefault: Boolean = true,
      scenario: String = "masked-filtered-separate-source",
      testMethod: String = "installedNamedAppendCommitsMaskedFilteredSource",
      seedCommits: Int = 0,
      checkpointFailure: Boolean = false,
      checkpointPartSize: Option[Long] = None,
      checkpointJson: Boolean = true,
      checkpointStruct: Boolean = true,
      partitionedTarget: Boolean = false,
      rewriteTarget: Boolean = false,
      checkpointTail: Boolean = false,
      priorCommitVersion: Option[Long] = None,
      warmCache: Boolean = false,
      rawMaintenance: Boolean = false,
      repeatCompletedResult: Boolean = false,
      emptySource: Boolean = false,
      sourceSql: String = "SELECT id, secret, region FROM b.db.t",
      expectedAppendedRows: Vector[Row] =
        Vector(Row(1L, "b-one-masked", "keep"), Row(3L, null, "keep")),
      checkConstraint: Boolean = false,
      revokeAfterHelper: Boolean = false,
      invalidateSourceRegistrationAfterHelper: Boolean = false,
      failureCheck: Option[Throwable => Unit] = None,
      inspectCheckpoint: (DeltaRangerAppendCommit, LogicalPlan) => Unit = (_, _) => (),
      inspect: (DeltaRangerAppendCommit, SparkSession) => Unit = (_, _) => ()
  ): Unit = {
    val rawConfig = if (rawMaintenance) {
      val path = Files.createTempFile("raw-maintenance-integration", ".properties")
      Files.write(
        path,
        ("version=1\nprofile=raw-current-head-filter-project-v1\n" +
          "table.count=1\ntable.0.metalake=lake\ntable.0.catalog=a\n" +
          "table.0.database=db\ntable.0.table=t\n").getBytes(
          java.nio.charset.StandardCharsets.UTF_8
        )
      )
      Some(path)
    } else None
    val failBeforeCommit = revokeAfterHelper || invalidateSourceRegistrationAfterHelper
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    var firstCheckpointCandidate: LogicalPlan = null
    var firstMalformedObserved = false
    var checkpointReentryObserved = false
    var firstNativeJobs: Set[Int] = null
    var firstNativeJobsChecked = false
    var bootstrapRows = seedCommits
    var beforeVersion = -1L
    var beforeMetadata = ""
    var beforeProtocol = ""
    val trace = InstalledAppendBoundaryProbe.fresh(
      if (warmCache || failureCheck.nonEmpty) "native-governed-failure"
      else "native-governed-success"
    )
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          trace.record(
            "runtime-owners",
            s"applicationId=${spark.sparkContext.applicationId} session=${trace.id(spark)} targetEngine=${trace.id(target.engine)} sourceEngine=${trace.id(source.engine)}"
          )
          trace.record(
            "worker-java",
            s"version=${System.getProperty("java.version")} home=${System.getProperty("java.home")}"
          )
          val json = new ObjectMapper()
          priorCommitVersion.foreach(version =>
            spark.conf.set("spark.databricks.delta.lastCommitVersionInSession", version.toString)
          )
          val warmFrames = if (warmCache) {
            val extensions = new SparkSessionExtensions
            new DeltaSparkSessionExtension().apply(extensions)
            val constructor =
              classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
            constructor.setAccessible(true)
            val native = constructor
              .newInstance(
                spark.sparkContext,
                Some(spark.sharedState),
                None,
                extensions,
                Map.empty[String, String]
              )
              .asInstanceOf[SparkSession]
            assertSame(spark.sharedState, native.sharedState)
            SparkSession.setActiveSession(native)
            val frames =
              try {
                val targetFrame =
                  native.read.format("delta").load(NativeSelectFixture.locations("a")).cache()
                val rows = targetFrame.collect().toVector
                assertEquals(
                  Set(Row(1L, "a-one", "keep"), Row(2L, "a-two", "drop"), Row(3L, null, "keep")),
                  rows.toSet
                )
                trace.record(
                  "warm-target-rows-json",
                  json.writeValueAsString(rows.map(_.toSeq.asJava).asJava)
                )
                val dependent = targetFrame.select("id", "region").cache()
                val dependentRows = dependent.collect().toVector
                assertEquals(3, dependentRows.size)
                trace.record(
                  "warm-dependent-rows-json",
                  json.writeValueAsString(dependentRows.map(_.toSeq.asJava).asJava)
                )
                Vector(targetFrame, dependent)
              } finally SparkSession.setActiveSession(spark)
            val table = spark.sessionState.catalogManager
              .catalog("a")
              .asInstanceOf[TableCatalog]
              .loadTable(Identifier.of(Array("db"), "t"))
              .asInstanceOf[DeltaTableV2]
            frames.foreach { frame =>
              val cached = spark.sharedState.cacheManager.lookupCachedData(frame).get
              assertTrue(cached.plan.exists(_.sameResult(table.toLogicalRelation)))
              assertTrue(cached.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
            }
            trace.record(
              "warm-cache-before",
              "targetAndDependentLoaded=true exactNativeRecacheMatch=true entries=2"
            )
            frames
          } else Vector.empty[DataFrame]
          val warmEntries =
            warmFrames.map(frame => spark.sharedState.cacheManager.lookupCachedData(frame).get)
          val input = spark.sql(
            sourceSql + (if (emptySource) " WHERE false" else "")
          )
          val registration = NativeSelectFixture.bindingFixture(spark, "a")
          val sourceRegistration = NativeSelectFixture.bindingFixture(spark, "b")
          val sourceRegistrationBefore =
            new java.util.LinkedHashMap[String, String](sourceRegistration.properties)
          val registrationBefore = json.writeValueAsString(registration.properties)
          trace.record("registration-before", registrationBefore)
          trace.record("scenario", s"name=$scenario frontend=$frontend nullAsDefault=$nullDefault")
          val confBefore = spark.sessionState.conf.getAllConfs
          var helperValidationCallbacks = 0
          var helperObserved = false
          var commitObserved = false
          var inspected = false
          var checkpointInspected = false
          var revoked = false
          def dataFiles(): Set[String] = {
            val files = Files.list(Paths.get(NativeSelectFixture.locations("a")))
            try
              files
                .iterator()
                .asScala
                .filter(_.getFileName.toString.endsWith(".parquet"))
                .map(_.getFileName.toString)
                .toSet
            finally files.close()
          }
          val originalDataFiles = dataFiles()
          val helpers = new java.util.IdentityHashMap[DeltaRangerAppendHelper, java.lang.Boolean]()
          val commits = new java.util.IdentityHashMap[DeltaRangerAppendCommit, java.lang.Boolean]()
          val nativeQueries = new java.util.IdentityHashMap[QueryExecution, java.lang.Boolean]()
          registration.onRead = _ => {
            val helper =
              field[ThreadLocal[DeltaRangerAppendHelper]](DeltaRangerAppendHelper, "active").get()
            helperObserved ||= helper != null
            if (helper != null) {
              helpers.put(helper, true)
              helperValidationCallbacks += 1
            }
            if (
              failBeforeCommit && !revoked && helperObserved && helper == null &&
              DeltaRangerAppendCommit.current.isEmpty && OptimisticTransaction.getActive.nonEmpty &&
              dataFiles().diff(originalDataFiles).nonEmpty
            ) {
              revoked = true
              if (invalidateSourceRegistrationAfterHelper) sourceRegistration.properties.clear()
              else source.denied.add("b:secret")
              trace.record(
                "after-helper-revocation",
                s"registrationChanged=$invalidateSourceRegistrationAfterHelper sourceColumn=secret generatedFilesPresent=true"
              )
            }
            DeltaRangerAppendCommit.current.foreach { commit =>
              if (
                checkpointTail && !firstMalformedObserved && !field[Boolean](
                  commit.metadata,
                  "checkpointAdmitting"
                ) &&
                field[AnyRef](commit.metadata, "checkpointFunction") == null
              ) {
                val caches = field[
                  scala.collection.mutable.ArrayBuffer[(LogicalRDD, Vector[Any], QueryExecution)]
                ](commit.metadata, "caches")
                if (caches.nonEmpty) {
                  firstMalformedObserved = true
                  val encoder = Encoders.product[SingleAction]
                  val malformed = MapElements[SingleAction, SingleAction](
                    (action: SingleAction) => action,
                    caches.head._1
                  )(encoder, encoder)
                  val jobsBefore = checkpointJobIds(spark)
                  assertReason(
                    assertThrows(
                      classOf[SecurityException],
                      () => commit.checkpointPlan(malformed)
                    ),
                    "nonnative cache function"
                  )
                  assertNull(field[AnyRef](commit.metadata, "checkpointSnapshot"))
                  assertNull(field[AnyRef](commit.metadata, "checkpointFunction"))
                  assertNull(field[AnyRef](commit.metadata, "checkpointCounter"))
                  assertFalse(field[Boolean](commit.metadata, "checkpointAdmitting"))
                  assertEquals(jobsBefore, checkpointJobIds(spark))
                  firstCheckpointCandidate = malformed
                  trace.record(
                    "checkpoint-first-nonnative-candidate",
                    "rejected=true snapshot=false function=false counter=false additionalJobs=0"
                  )
                }
              }
              if (
                checkpointTail && firstCheckpointCandidate != null && firstNativeJobs == null &&
                field[Boolean](commit.metadata, "checkpointAdmitting") && field[AnyRef](
                  commit.metadata,
                  "checkpointFunction"
                ) == null
              ) {
                firstNativeJobs = checkpointJobIds(spark)
              }
              if (
                checkpointTail && firstNativeJobs != null && !firstNativeJobsChecked &&
                field[AnyRef](commit.metadata, "checkpointFunction") != null
              ) {
                firstNativeJobsChecked = true
                assertEquals(firstNativeJobs, checkpointJobIds(spark))
                trace.record("checkpoint-first-native-admission-jobs", "additionalJobs=0")
              }
              if (
                checkpointTail && firstCheckpointCandidate != null && !checkpointReentryObserved &&
                field[Boolean](commit.metadata, "checkpointAdmitting")
              ) {
                checkpointReentryObserved = true
                val foreignCall = new FutureTask[SecurityException](() =>
                  assertThrows(
                    classOf[SecurityException],
                    () => commit.checkpointPlan(firstCheckpointCandidate)
                  )
                )
                val foreignThread = new Thread(foreignCall, "checkpoint-foreign-owner")
                foreignThread.start()
                val foreignFailure = foreignCall.get(10, TimeUnit.SECONDS)
                foreignThread.join(10000L)
                assertFalse(foreignThread.isAlive)
                trace.record("checkpoint-foreign-owner", foreignFailure.getMessage)
                assertReason(foreignFailure, "expired commit owner")
                assertTrue(field[Boolean](commit.metadata, "checkpointAdmitting"))
                assertNull(field[AnyRef](commit.metadata, "checkpointSnapshot"))
                assertNull(field[AnyRef](commit.metadata, "checkpointFunction"))
                assertNull(field[AnyRef](commit.metadata, "checkpointCounter"))
                assertReason(
                  assertThrows(
                    classOf[SecurityException],
                    () => commit.checkpointPlan(firstCheckpointCandidate)
                  ),
                  "checkpoint admission reentry"
                )
                trace.record(
                  "checkpoint-first-admission-reentry",
                  "rejected=true snapshot=false function=false counter=false"
                )
              }
              commitObserved = true
              commits.put(commit, true)
              Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).foreach {
                id =>
                  val qe = SQLExecution.getQueryExecution(id.toLong)
                  if (qe != null && !nativeQueries.containsKey(qe)) {
                    nativeQueries.put(qe, true)
                    trace.record("commit-query-execution", qe.analyzed.treeString)
                  }
                  if (
                    qe != null && !checkpointInspected && qe.analyzed.exists {
                      case map: MapElements => map.argumentClass == classOf[SingleAction]
                      case _                => false
                    }
                  ) {
                    checkpointInspected = true
                    val jobsBeforeInspection = checkpointJobIds(spark)
                    inspectCheckpoint(commit, qe.analyzed)
                    val jobsAfterInspection = checkpointJobIds(spark)
                    assertEquals(jobsBeforeInspection, jobsAfterInspection)
                    trace.record(
                      "checkpoint-inspection-jobs",
                      s"before=${jobsBeforeInspection.toVector.sorted.mkString(",")} after=${jobsAfterInspection.toVector.sorted.mkString(",")}"
                    )
                    if (checkpointFailure) {
                      trace.record("checkpoint-callback-failure", "injectedAfterPublication=true")
                      throw new IllegalStateException("native-checkpoint-callback-failure")
                    }
                  }
              }
              if (
                !inspected && field[
                  scala.collection.mutable.ArrayBuffer[(LogicalRDD, Vector[Any], QueryExecution)]
                ](commit.metadata, "caches").nonEmpty
              ) {
                inspected = true
                inspect(commit, spark)
              }
            }
          }
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          if (seedCommits == 9) {
            val logRoot = Paths.get(NativeSelectFixture.locations("a"), "_delta_log")
            assertFalse(
              Files.exists(logRoot.resolve(f"${beforeVersion + 1}%020d.checkpoint.parquet"))
            )
            assertFalse(Files.exists(logRoot.resolve("_last_checkpoint")))
          }
          InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
          val completed = new LinkedBlockingQueue[QueryExecution]()
          val listener = new QueryExecutionListener {
            override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit =
              if (qe.analyzed.isInstanceOf[AppendData]) completed.add(qe)
            override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = ()
          }
          val jobs = new AtomicInteger()
          val jobListener = new SparkListener {
            override def onJobStart(event: SparkListenerJobStart): Unit = {
              jobs.incrementAndGet(); ()
            }
          }
          def drainSparkEvents(): Unit = {
            val bus =
              spark.sparkContext.getClass.getMethod("listenerBus").invoke(spark.sparkContext)
            bus.getClass
              .getMethod("waitUntilEmpty", java.lang.Long.TYPE)
              .invoke(bus, Long.box(10000L))
          }
          if (repeatCompletedResult) {
            spark.listenerManager.register(listener)
            spark.sparkContext.addSparkListener(jobListener)
          }
          var failure: Throwable = null
          try {
            def write(): Unit = {
              val writer = input.write.mode("append")
              frontend match {
                case "insertInto"  => writer.insertInto("a.db.t")
                case "saveAsTable" => writer.saveAsTable("a.db.t")
              }
            }
            if ((warmCache && !rawMaintenance) || failureCheck.nonEmpty) {
              failure = assertThrows(classOf[Throwable], () => write())
              val causes =
                Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
              trace.record("failure", causes.mkString(" | "))
              trace.record(
                "failure-stack",
                causes.map(_.getStackTrace.mkString("\n")).mkString("\nCaused by:\n")
              )
              trace.record(
                "suppressed-failures-json",
                json.writeValueAsString(
                  causes
                    .flatMap(_.getSuppressed)
                    .map(t =>
                      Map(
                        "class" -> t.getClass.getName,
                        "message" -> Option(t.getMessage).getOrElse("")
                      ).asJava
                    )
                    .asJava
                )
              )
              failureCheck.foreach(_(failure))
              if (warmCache) {
                assertTrue(causes.exists(_.isInstanceOf[SecurityException]))
                assertTrue(
                  causes.exists(_.getStackTrace.exists(_.getClassName.contains("CacheManager")))
                )
                val buffersLoaded = warmEntries.map(
                  _.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded
                )
                assertTrue(buffersLoaded.contains(false))
                warmFrames.foreach(frame =>
                  assertTrue(spark.sharedState.cacheManager.lookupCachedData(frame).isEmpty)
                )
                trace.record(
                  "warm-cache-after-json",
                  json.writeValueAsString(
                    warmEntries
                      .zip(buffersLoaded)
                      .map { case (entry, loaded) =>
                        Map[String, Any](
                          "entry" -> trace.id(entry),
                          "oldBuffersLoaded" -> loaded,
                          "registered" -> false
                        ).asJava
                      }
                      .asJava
                  )
                )
              }
            } else write()
            if (repeatCompletedResult) {
              val qe = completed.poll(10, TimeUnit.SECONDS)
              assertNotNull(qe)
              val issued = qe.executedPlan.collectFirst { case guard: DeltaRangerAppendInputExec =>
                guard
              }.get
              drainSparkEvents()
              val jobsBefore = jobs.get()
              assertTrue(jobsBefore > 0)
              val callbacksBefore = helperValidationCallbacks
              assertTrue(callbacksBefore > 0)
              val sourceRequestsBefore = source.requests.size()
              val targetRequestsBefore = target.requests.size()
              val replayBefore = InstalledAppendBoundaryProbe
                .manifest(root, trace.directory.resolve("before-result-collection"))
              assertEquals(0, issued.executeCollect().length)
              assertEquals(0, issued.executeCollect().length)
              drainSparkEvents()
              assertEquals(jobsBefore, jobs.get())
              assertEquals(callbacksBefore, helperValidationCallbacks)
              assertEquals(sourceRequestsBefore, source.requests.size())
              assertEquals(targetRequestsBefore, target.requests.size())
              assertEquals(
                replayBefore,
                InstalledAppendBoundaryProbe
                  .manifest(root, trace.directory.resolve("after-result-collection"))
              )
              val resultVersion = DeltaLog
                .forTable(spark, NativeSelectFixture.locations("a"))
                .unsafeVolatileSnapshot
                .version
              assertEquals(beforeVersion + 1, resultVersion)
              trace.record(
                "completed-result-collections-json",
                json.writeValueAsString(
                  Map[String, Any](
                    "queryExecution" -> trace.id(qe),
                    "issuedLeaf" -> trace.id(issued),
                    "collections" -> 2,
                    "rowsPerCollection" -> Vector(0, 0).asJava,
                    "sparkJobsBefore" -> jobsBefore,
                    "sparkJobsAfter" -> jobs.get(),
                    "helperValidationCallbacksBefore" -> callbacksBefore,
                    "helperValidationCallbacksAfter" -> helperValidationCallbacks,
                    "sourceRequestsBefore" -> sourceRequestsBefore,
                    "sourceRequestsAfter" -> source.requests.size(),
                    "targetRequestsBefore" -> targetRequestsBefore,
                    "targetRequestsAfter" -> target.requests.size(),
                    "nativeVersion" -> resultVersion
                  ).asJava
                )
              )
            }
          } finally {
            if (repeatCompletedResult) {
              spark.listenerManager.unregister(listener)
              spark.sparkContext.removeSparkListener(jobListener)
            }
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
          }
          registration.onRead = _ => ()
          trace.record(
            "source-registration-at-exit",
            json.writeValueAsString(sourceRegistration.properties)
          )
          sourceRegistration.properties.clear()
          sourceRegistration.properties.putAll(sourceRegistrationBefore)
          source.denied.clear()
          assertTrue(helperObserved)
          assertEquals(!failBeforeCommit, commitObserved)
          assertEquals(!failBeforeCommit && !emptySource, inspected)
          assertEquals(1, helpers.size())
          assertEquals(if (failBeforeCommit) 0 else 1, commits.size())
          val generatedUncommitted =
            if (failBeforeCommit || emptySource) dataFiles().diff(originalDataFiles)
            else Set.empty[String]
          if (failBeforeCommit || emptySource) {
            if (failBeforeCommit) assertTrue(revoked)
            val generated = generatedUncommitted
            assertTrue(generated.nonEmpty)
            trace.record(
              "uncommitted-files-json",
              json.writeValueAsString(generated.toVector.sorted.asJava)
            )
          }
          trace.record("write-return", s"success=${failure == null}")
          if (seedCommits == 9) {
            val logRoot = Paths.get(NativeSelectFixture.locations("a"), "_delta_log")
            val checkpoint = logRoot.resolve(f"${beforeVersion + 1}%020d.checkpoint.parquet")
            if (checkpointFailure) {
              assertFalse(Files.exists(checkpoint))
              assertFalse(Files.exists(logRoot.resolve("_last_checkpoint")))
              trace.record(
                "native-checkpoint-hook-result",
                "checkpointPresent=false lastCheckpointPresent=false"
              )
            } else {
              val artifacts = Files.list(logRoot)
              try
                trace.record(
                  "checkpoint-artifacts-after-return",
                  artifacts
                    .iterator()
                    .asScala
                    .map(_.getFileName.toString)
                    .filter(_.contains("checkpoint"))
                    .toVector
                    .sorted
                    .mkString(",")
                )
              finally artifacts.close()
              assertTrue(
                Files.isRegularFile(logRoot.resolve("_last_checkpoint")),
                "native checkpoint hook must publish its pointer after normal append return"
              )
              assertTrue(checkpointInspected)
              if (checkpointTail) {
                assertTrue(firstMalformedObserved)
                assertTrue(checkpointReentryObserved)
                assertTrue(firstNativeJobsChecked)
              }
              val last = Files.readString(logRoot.resolve("_last_checkpoint"))
              trace.record("last-checkpoint-json", last)
              assertEquals(beforeVersion + 1, json.readTree(last).get("version").asLong())
            }
          }
          val confAfter = spark.sessionState.conf.getAllConfs
          val expectedConf =
            if (failBeforeCommit || emptySource) confBefore
            else
              confBefore + ("spark.databricks.delta.lastCommitVersionInSession" -> (beforeVersion + 1).toString)
          assertEquals(expectedConf, confAfter)
          val helperActive =
            field[ThreadLocal[DeltaRangerAppendHelper]](DeltaRangerAppendHelper, "active")
              .get() != null
          assertFalse(helperActive)
          assertTrue(DeltaRangerAppendCommit.current.isEmpty)
          trace.requests(target, "target-authorization")
          trace.requests(source, "source-authorization")
          val healthyRows = spark.sql("SELECT id FROM b.db.t").collect().toVector
          val outcome: Map[String, Any] = Map(
            "schemaVersion" -> 1,
            "testMethod" -> testMethod,
            "scenario" -> scenario,
            "frontend" -> frontend,
            "nullAsDefault" -> nullDefault,
            "returned" -> (failure == null),
            "failureCauses" -> Iterator
              .iterate[Throwable](failure)(_.getCause)
              .takeWhile(_ != null)
              .map(error =>
                Map("class" -> error.getClass.getName, "message" -> error.getMessage).asJava
              )
              .toVector
              .asJava,
            "helperObserved" -> helperObserved,
            "commitOwnerObserved" -> commitObserved,
            "helperOwnerCount" -> helpers.size(),
            "commitOwnerCount" -> commits.size(),
            "confBefore" -> confBefore.asJava,
            "confAfter" -> confAfter.asJava,
            "cleanup" -> Map[String, Any](
              "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
              "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
              "helperActive" -> helperActive,
              "commitOwnerActive" -> DeltaRangerAppendCommit.current.nonEmpty,
              "healthySelectRows" -> healthyRows.map(_.toSeq.asJava).asJava
            ).asJava
          )
          Files.write(
            trace.directory.resolve("outcome.json"),
            json.writeValueAsString(outcome.asJava).getBytes(StandardCharsets.UTF_8)
          )
          trace.record("registration-after", json.writeValueAsString(registration.properties))
          assertEquals(registrationBefore, json.writeValueAsString(registration.properties))
          assertTrue(OptimisticTransaction.getActive.isEmpty)
          assertNull(DeltaRangerNativeOperation.current())
          if (rawMaintenance) {
            val replacements =
              warmFrames.map(frame => spark.sharedState.cacheManager.lookupCachedData(frame).get)
            replacements.zip(warmEntries).foreach { case (fresh, old) =>
              assertSame(old.plan, fresh.plan)
              assertNotSame(
                old.cachedRepresentation.cacheBuilder,
                fresh.cachedRepresentation.cacheBuilder
              )
              assertFalse(old.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertFalse(fresh.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertTrue(
                fresh.cachedRepresentation.cacheBuilder.cachedPlan
                  .isInstanceOf[DeltaRangerRawProducer]
              )
            }
            val consumer = warmFrames.head.sparkSession
            SparkSession.setActiveSession(consumer)
            try {
              val expected = Vector(
                Row(1L, "a-one", "keep"),
                Row(2L, "a-two", "drop"),
                Row(3L, null, "keep")
              ) ++ expectedAppendedRows
              def counted(rows: Vector[Row]) =
                rows.groupBy(identity).map { case (row, xs) => row -> xs.size }
              def freshTarget =
                consumer.read.format("delta").load(NativeSelectFixture.locations("a"))
              assertEquals(counted(expected), counted(freshTarget.collect().toVector))
              assertSame(consumer, SparkSession.active)
              assertEquals(
                counted(expected.map(row => Row(row.getLong(0), row.getString(2)))),
                counted(freshTarget.select("id", "region").collect().toVector)
              )
              assertEquals(counted(expected), counted(freshTarget.collect().toVector))
              trace.record(
                "raw-warm-late-readback",
                "target=5 dependent=5 repeated=true capturedSessionRestored=true"
              )
            } finally SparkSession.setActiveSession(spark)
          }
          val conf = spark.sparkContext.getConf
          spark.stop()
          SparkSession.clearActiveSession()
          SparkSession.clearDefaultSession()
          val native = SparkSession
            .builder()
            .config(conf)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .getOrCreate()
          try {
            DeltaLog.clearCache()
            val location = NativeSelectFixture.locations("a")
            val snapshot = DeltaLog.forTable(native, location).update()
            trace.record(
              "native-state",
              s"beforeVersion=$beforeVersion afterVersion=${snapshot.version} metadata=${snapshot.metadata.json} protocol=${snapshot.protocol.json}"
            )
            snapshot.allFiles.collect().foreach(f => trace.record("native-add-file", f.json))
            if (failBeforeCommit || emptySource) {
              val orphanRows = generatedUncommitted.toVector.sorted.flatMap { path =>
                val rows =
                  native.read.parquet(Paths.get(location, path).toString).collect().toVector
                trace.record(
                  "uncommitted-file-rows-json",
                  json.writeValueAsString(
                    Map[String, Any](
                      "path" -> path,
                      "rows" -> rows.map(_.toSeq.asJava).asJava
                    ).asJava
                  )
                )
                rows
              }
              val expectedOrphans = if (emptySource) Vector.empty else expectedAppendedRows
              assertEquals(
                expectedOrphans.groupBy(identity).map { case (row, rows) => row -> rows.size },
                orphanRows.groupBy(identity).map { case (row, rows) => row -> rows.size }
              )
            }
            assertEquals(
              beforeVersion + (if (failBeforeCommit || emptySource) 0 else 1),
              snapshot.version
            )
            assertEquals(beforeMetadata, snapshot.metadata.json)
            assertEquals(beforeProtocol, snapshot.protocol.json)
            trace.record(
              "native-checkpoint-provider-version",
              snapshot.logSegment.checkpointProvider.version.toString
            )
            trace.record(
              "native-log-segment-deltas-json",
              json.writeValueAsString(
                snapshot.logSegment.deltas.map(_.getPath.toString).toVector.asJava
              )
            )
            if (seedCommits == 10) assertEquals(10L, snapshot.logSegment.checkpointProvider.version)
            if (seedCommits == 9 && checkpointFailure)
              assertEquals(-1L, snapshot.logSegment.checkpointProvider.version)
            if (seedCommits == 9 && !checkpointFailure) {
              assertEquals(beforeVersion + 1, snapshot.logSegment.checkpointProvider.version)
              val logRoot = Paths.get(location, "_delta_log")
              val last = json.readTree(Files.readString(logRoot.resolve("_last_checkpoint")))
              val adds = snapshot.numOfFiles
              val removes = snapshot.numOfRemoves
              val parts = checkpointPartSize
                .map(size => ((adds + removes + size - 1) / size).toInt)
                .getOrElse(1)
              if (rewriteTarget) {
                assertTrue(removes > 0)
                assertNotEquals(0L, (adds + removes) % checkpointPartSize.get)
                assertNotEquals(
                  (adds + checkpointPartSize.get - 1) / checkpointPartSize.get,
                  parts.toLong
                )
              }
              if (parts > 1) assertEquals(parts, last.get("parts").asInt())
              else assertFalse(last.has("parts"))
              val checkpoints =
                if (parts == 1)
                  Vector(logRoot.resolve(f"${beforeVersion + 1}%020d.checkpoint.parquet"))
                else
                  (1 to parts)
                    .map(part =>
                      logRoot.resolve(
                        f"${beforeVersion + 1}%020d.checkpoint.$part%010d.$parts%010d.parquet"
                      )
                    )
                    .toVector
              checkpoints.foreach(path => assertTrue(Files.size(path) > 0))
              assertEquals(
                checkpoints.map(_.toString).toSet,
                snapshot.logSegment.checkpointProvider.topLevelFiles
                  .map(_.getPath.toUri.getPath)
                  .toSet
              )
              assertEquals(checkpoints.map(Files.size(_)).sum, last.get("sizeInBytes").asLong())
              assertEquals(adds, last.get("numOfAddFiles").asLong())
              trace.record("checkpoint-counts", s"adds=$adds removes=$removes parts=$parts")
              val checkpointRows = native.read.parquet(checkpoints.map(_.toString): _*)
              val addSchema = checkpointRows
                .schema("add")
                .dataType
                .asInstanceOf[StructType]
              val fields = Vector(
                "path",
                "partitionValues",
                "size",
                "modificationTime",
                "dataChange",
                "tags",
                "deletionVector",
                "baseRowId",
                "defaultRowCommitVersion",
                "clusteringProvider"
              ) ++
                (if (checkpointJson) Vector("stats") else Vector.empty) ++
                (if (partitionedTarget && checkpointStruct) Vector("partitionValues_parsed")
                 else Vector.empty)
              assertEquals(fields, addSchema.fieldNames.toVector)
              trace.record("checkpoint-schema-json", checkpointRows.schema.json)
              assertEquals(removes, checkpointRows.filter("remove IS NOT NULL").count())
              val checkpointActions = checkpointRows.toJSON.collect().toVector.map(json.readTree)
              assertEquals(
                json.readTree(snapshot.metadata.json).get("metaData"),
                checkpointActions.find(_.hasNonNull("metaData")).get.get("metaData")
              )
              assertEquals(
                json.readTree(snapshot.protocol.json).get("protocol"),
                checkpointActions.find(_.hasNonNull("protocol")).get.get("protocol")
              )
              val expectedAdds = snapshot.allFiles
                .collect()
                .map(action => {
                  val value = json.readTree(action.json).get("add")
                  value.get("path").asText() -> value
                })
                .toMap
              val actualAdds = checkpointActions.filter(_.hasNonNull("add")).map(_.get("add"))
              assertEquals(expectedAdds.size, actualAdds.size)
              actualAdds.foreach(action => {
                val expected = expectedAdds(action.get("path").asText())
                fields
                  .filterNot(_ == "partitionValues_parsed")
                  .foreach(name =>
                    assertEquals(
                      Option(expected.get(name)).filterNot(_.isNull),
                      Option(action.get(name)).filterNot(_.isNull),
                      "checkpoint add field " + name
                    )
                  )
              })
              val expectedRemoves = snapshot.tombstones
                .collect()
                .map(action => {
                  val value = json.readTree(action.json).get("remove")
                  value.get("path").asText() -> value
                })
                .toMap
              val actualRemoves =
                checkpointActions.filter(_.hasNonNull("remove")).map(_.get("remove"))
              assertEquals(expectedRemoves.size, actualRemoves.size)
              assertEquals(expectedRemoves.keySet, actualRemoves.map(_.get("path").asText()).toSet)
              actualRemoves.foreach(action => {
                val expected = expectedRemoves(action.get("path").asText())
                val names =
                  (expected.fieldNames().asScala.toSet ++ action.fieldNames().asScala.toSet) -- Set(
                    "tags",
                    "stats"
                  )
                names.foreach(name =>
                  assertEquals(
                    Option(expected.get(name)).filterNot(_.isNull),
                    Option(action.get(name)).filterNot(_.isNull),
                    "checkpoint remove field " + name
                  )
                )
                assertFalse(action.has("tags"))
                assertFalse(action.has("stats"))
              })
              if (partitionedTarget && checkpointStruct) {
                val values = checkpointRows
                  .filter("add IS NOT NULL")
                  .selectExpr("add.partitionValues['region']", "add.partitionValues_parsed.region")
                  .collect()
                  .toVector
                assertTrue(values.nonEmpty)
                values.foreach(row => assertEquals(row.get(0), row.get(1)))
                assertEquals(Set("keep", "drop"), values.map(_.getString(1)).toSet)
              }
              val commitRows = native.read
                .json(logRoot.resolve(f"${beforeVersion + 1}%020d.json").toString)
                .filter("add IS NOT NULL")
                .select("add.stats")
                .collect()
              assertTrue(commitRows.nonEmpty)
              commitRows.foreach(row =>
                assertTrue(json.readTree(row.getString(0)).get("numRecords").asLong() > 0)
              )
              checkpointRows.toJSON.collect().foreach(trace.record("native-checkpoint-row-json", _))
              assertEquals(1L, checkpointRows.filter("metaData IS NOT NULL").count())
              assertEquals(1L, checkpointRows.filter("protocol IS NOT NULL").count())
              assertEquals(
                snapshot.allFiles.collect().map(_.path).toSet,
                checkpointRows
                  .filter("add IS NOT NULL")
                  .select("add.path")
                  .collect()
                  .map(_.getString(0))
                  .toSet
              )
              assertEquals(checkpointRows.count(), last.get("size").asLong())
            }
            val actualRows = native.read.format("delta").load(location).collect().toVector
            trace.record(
              "native-rows-json",
              json.writeValueAsString(actualRows.map(_.toSeq.asJava).asJava)
            )
            assertEquals(
              (Vector.fill(bootstrapRows)(Row(-1L, "bootstrap", "keep")) ++ Vector(
                Row(1L, "a-one", "keep"),
                Row(2L, "a-two", "drop"),
                Row(3L, null, "keep")
              ) ++ (if (failBeforeCommit || emptySource) Vector.empty
                    else expectedAppendedRows))
                .groupBy(identity)
                .map { case (k, v) => k -> v.size },
              actualRows
                .groupBy(identity)
                .map { case (k, v) => k -> v.size }
            )
          } finally native.stop()
          if (checkpointTail)
            appendCheckpointTail(
              conf,
              trace,
              Vector.fill(bootstrapRows)(Row(-1L, "bootstrap", "keep")) ++
                Vector(Row(1L, "a-one", "keep"), Row(2L, "a-two", "drop"), Row(3L, null, "keep")) ++
                expectedAppendedRows ++ expectedAppendedRows,
              beforeMetadata,
              beforeProtocol,
              new LinkedHashMap[String, String](registration.properties),
              new LinkedHashMap[String, String](sourceRegistration.properties)
            )
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> nullDefault.toString
        ) ++ rawConfig.map(path =>
          "spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> path.toString
        ) ++ checkpointPartSize.map(size =>
          "spark.databricks.delta.checkpoint.partSize" -> size.toString
        ),
        native => {
          if (checkConstraint)
            native
              .sql(
                "ALTER TABLE delta.`" + NativeSelectFixture.locations("a") +
                  "` ADD CONSTRAINT positive_id CHECK (id > 0)"
              )
              .collect()
          if (partitionedTarget || rewriteTarget) {
            val writer = native
              .sql(
                "SELECT 1L id, 'a-one' secret, 'keep' region UNION ALL SELECT 2L, 'a-two', 'drop' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep'"
              )
              .write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
            if (partitionedTarget)
              writer.partitionBy("region").save(NativeSelectFixture.locations("a"))
            else writer.save(NativeSelectFixture.locations("a"))
          }
          if (!checkpointJson || !checkpointStruct) {
            native
              .sql(
                "ALTER TABLE delta.`" + NativeSelectFixture.locations("a") +
                  "` SET TBLPROPERTIES ('delta.checkpoint.writeStatsAsJson'='" + checkpointJson +
                  "', 'delta.checkpoint.writeStatsAsStruct'='" + checkpointStruct + "')"
              )
              .collect()
          }
          val setupVersion =
            DeltaLog.forTable(native, NativeSelectFixture.locations("a")).update().version
          bootstrapRows =
            if (seedCommits == 9 || seedCommits == 10) seedCommits - setupVersion.toInt
            else seedCommits
          (0 until bootstrapRows).foreach { _ =>
            native
              .sql("SELECT -1L id, 'bootstrap' secret, 'keep' region")
              .write
              .format("delta")
              .mode("append")
              .save(NativeSelectFixture.locations("a"))
          }
          val snapshot = DeltaLog.forTable(native, NativeSelectFixture.locations("a")).update()
          beforeVersion = snapshot.version
          beforeMetadata = snapshot.metadata.json
          beforeProtocol = snapshot.protocol.json
          trace.record(
            "native-state-before",
            s"version=$beforeVersion metadata=$beforeMetadata protocol=$beforeProtocol"
          )
        }
      )
    finally {
      target.engine.close()
      source.engine.close()
      trace.close()
      rawConfig.foreach(Files.deleteIfExists)
    }
  }
  private def appendCheckpointTail(
      conf: SparkConf,
      trace: InstalledAppendBoundaryProbe.Trace,
      expectedRows: Vector[Row],
      metadata: String,
      protocol: String,
      targetPublication: LinkedHashMap[String, String],
      sourcePublication: LinkedHashMap[String, String]
  ): Unit = {
    val json = new ObjectMapper()
    val location = NativeSelectFixture.locations("a")
    val root = Paths.get(location).getParent
    val pointer = Files.readString(Paths.get(location, "_delta_log", "_last_checkpoint"))
    DeltaLog.clearCache()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    NativeSelectFixture.installer = Some(NativeSelectFixture.extension(target))
    val installed = SparkSession
      .builder()
      .config(conf)
      .config(
        "spark.sql.extensions",
        classOf[NativeSelectInstalledExtension].getName +
          ",io.delta.sql.DeltaSparkSessionExtension"
      )
      .getOrCreate()
    val inputs = scala.collection.mutable.Set.empty[String]
    val segments = scala.collection.mutable.ArrayBuffer.empty[Vector[Vector[String]]]
    val seen = new java.util.IdentityHashMap[QueryExecution, java.lang.Boolean]()
    try {
      Seq("a", "b", "q").foreach(c =>
        installed.conf.set("spark.sql.catalog." + c, classOf[NativeBoundForwardingCatalog].getName)
      )
      installed.sessionState.conf.setConfString(
        "spark.sql.extensions",
        DeltaRangerCatalogSpark35.EXTENSION + "," +
          installed.conf.get("spark.sql.extensions")
      )
      SparkSession.setActiveSession(installed)
      val registration = NativeSelectFixture.bindingFixture(installed, "a")
      val sourceRegistration = NativeSelectFixture.bindingFixture(installed, "b")
      registration.properties.clear()
      registration.properties.putAll(targetPublication)
      sourceRegistration.properties.clear()
      sourceRegistration.properties.putAll(sourcePublication)
      val registrationBefore = json.writeValueAsString(registration.properties)
      assertEquals(json.writeValueAsString(targetPublication), registrationBefore)
      assertEquals(sourcePublication, sourceRegistration.properties)
      val input = installed.sql("SELECT id, secret, region FROM b.db.t")
      val confBefore = installed.sessionState.conf.getAllConfs
      trace.record(
        "tail-runtime-owners",
        s"applicationId=${installed.sparkContext.applicationId} session=${trace.id(installed)} targetEngine=${trace.id(target.engine)} sourceEngine=${trace.id(source.engine)}"
      )
      trace.record("tail-registration-before", registrationBefore)
      registration.onRead = _ =>
        DeltaRangerAppendCommit.current.foreach { commit =>
          field[scala.collection.mutable.ArrayBuffer[QueryExecution]](
            commit.metadata,
            "reconstructions"
          )
            .foreach(qe =>
              if (!seen.containsKey(qe)) {
                seen.put(qe, true)
                val segment = qe.analyzed
                  .collect { case relation: LogicalRelation => relation.relation }
                  .map {
                    case fs: HadoopFsRelation =>
                      fs.location match {
                        case index: DeltaLogFileIndex =>
                          val files = index.files.map(_.getPath.toUri.getPath).toVector
                          inputs ++= files
                          files
                        case _ => fail("Expected native log index")
                      }
                    case _ => fail("Expected native log relation")
                  }
                  .toVector
                segments += segment
              }
            )
        }
      InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before-tail"))
      input.write
        .mode("append")
        .insertInto("a.db.t")
      registration.onRead = _ => ()
      assertTrue(seen.size() > 0)
      assertTrue(DeltaRangerAppendCommit.current.isEmpty)
      assertTrue(OptimisticTransaction.getActive.isEmpty)
      assertNull(DeltaRangerNativeOperation.current())
      assertNull(
        field[ThreadLocal[DeltaRangerAppendHelper]](DeltaRangerAppendHelper, "active").get()
      )
      assertEquals(
        confBefore + ("spark.databricks.delta.lastCommitVersionInSession" -> "11"),
        installed.sessionState.conf.getAllConfs
      )
      assertEquals(registrationBefore, json.writeValueAsString(registration.properties))
      assertEquals(sourcePublication, sourceRegistration.properties)
      trace.record("tail-registration-after", json.writeValueAsString(registration.properties))
      trace.record(
        "tail-conf-after",
        json.writeValueAsString(installed.sessionState.conf.getAllConfs.asJava)
      )
      trace.requests(target, "tail-target-authorization")
      trace.requests(source, "tail-source-authorization")
      assertEquals(Set(Row(1L), Row(3L)), installed.sql("SELECT id FROM b.db.t").collect().toSet)
      assertEquals(pointer, Files.readString(Paths.get(location, "_delta_log", "_last_checkpoint")))
    } finally {
      InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after-tail"))
      installed.stop()
      target.engine.close()
      source.engine.close()
    }
    DeltaLog.clearCache()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val native = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .getOrCreate()
    try {
      val snapshot = DeltaLog.forTable(native, location).update()
      assertEquals(11L, snapshot.version)
      assertEquals(10L, snapshot.logSegment.checkpointProvider.version)
      assertEquals(metadata, snapshot.metadata.json)
      assertEquals(protocol, snapshot.protocol.json)
      val checkpointFiles =
        snapshot.logSegment.checkpointProvider.topLevelFiles.map(_.getPath.toUri.getPath).toSet
      assertTrue(checkpointFiles.size > 1)
      val tail = snapshot.logSegment.deltas.map(_.getPath.toUri.getPath).toSet
      assertEquals(
        Set(Paths.get(location, "_delta_log", "00000000000000000011.json").toString),
        tail
      )
      assertEquals(checkpointFiles ++ tail, inputs.toSet)
      assertTrue(segments.nonEmpty)
      segments.foreach(segment =>
        assertEquals(Vector(checkpointFiles.toVector.sorted, tail.toVector.sorted), segment)
      )
      val rows = native.read.format("delta").load(location).collect().toVector
      assertEquals(
        expectedRows.groupBy(identity).map { case (row, values) => row -> values.size },
        rows.groupBy(identity).map { case (row, values) => row -> values.size }
      )
      trace.record(
        "same-lineage-checkpoint-tail-json",
        json.writeValueAsString(
          Map[String, Any](
            "version" -> snapshot.version,
            "checkpointVersion" -> snapshot.logSegment.checkpointProvider.version,
            "checkpointFiles" -> checkpointFiles.toVector.sorted.asJava,
            "tailFiles" -> tail.toVector.sorted.asJava,
            "observedReconstructionFiles" -> inputs.toVector.sorted.asJava,
            "orderedReconstructionSegments" -> segments.map(_.map(_.asJava).asJava).asJava,
            "rows" -> rows.map(_.toSeq.asJava).asJava,
            "returned" -> true,
            "ownersClosed" -> true
          ).asJava
        )
      )
    } finally native.stop()
  }

  private def field[T](value: AnyRef, suffix: String): T = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == suffix || f.getName.endsWith("$$" + suffix))
      .get
    member.setAccessible(true)
    member.get(value).asInstanceOf[T]
  }

  private def corresponds(
      expected: LogicalPlan,
      actual: LogicalPlan,
      source: LogicalPlan
  ): Boolean = {
    val method = DeltaRangerAppendHelper.getClass.getDeclaredMethods
      .find(m => m.getName.endsWith("matches") && m.getParameterCount == 3)
      .get
    method.setAccessible(true)
    method
      .invoke(DeltaRangerAppendHelper, expected, actual, source)
      .asInstanceOf[java.lang.Boolean]
      .booleanValue()
  }

  private def serializedCopy(value: AnyRef): AnyRef = {
    val bytes = new ByteArrayOutputStream()
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try input.readObject()
    finally input.close()
  }

  private def assertReason(failure: Throwable, reason: String): Unit =
    assertTrue(
      Iterator
        .iterate(failure)(_.getCause)
        .takeWhile(_ != null)
        .exists(t => Option(t.getMessage).exists(_.contains(reason))),
      failure.toString
    )

}
