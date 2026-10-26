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

import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  AQEShuffleReadExec,
  ShuffleQueryStageExec
}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerTrustedAqeRead {
  @Test
  def namedJoinUsesStockAqe(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark(DeltaRangerTrustedAqeFixture.coalescing) {
      (spark, _, _) =>
        val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
        val actualRows = frame.collect().toVector
        assertEquals(
          Map(
            Row(1L, "MASKED", "x") -> 1,
            Row(1L, "MASKED", "y") -> 1,
            Row(3L, "MASKED", "v") -> 1
          ),
          actualRows.groupBy(identity).map { case (row, rows) => row -> rows.length }
        )
        val nodes = DeltaRangerTrustedAqeFixture.nodes(frame.queryExecution.executedPlan)
        val finalAdaptivePlan = nodes.collectFirst { case plan: AdaptiveSparkPlanExec => plan }.get
        val materializedPayloadShuffleCount = nodes.count {
          case stage: ShuffleQueryStageExec => stage.isMaterialized
          case _                            => false
        }
        val coalescedPayloadReadCount = nodes.count {
          case read: AQEShuffleReadExec => read.hasCoalescedPartition
          case _                        => false
        }
        assertTrue(finalAdaptivePlan.isFinalPlan)
        assertTrue(materializedPayloadShuffleCount > 0)
        assertTrue(coalescedPayloadReadCount > 0)
        org.slf4j.LoggerFactory
          .getLogger(getClass)
          .warn(
            "TRUSTED_AQE_FINAL shuffles=" +
              materializedPayloadShuffleCount + " coalesced=" + coalescedPayloadReadCount + "\n" +
              frame.queryExecution.executedPlan.treeString
          )
        val owner = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner
        assertEquals(
          Map("a" -> Set("id", "secret"), "b" -> Set("id", "secret")),
          owner.obligations.map(o => o.scan.catalog -> o.columns).toMap
        )
    }

  @Test
  def namedJoinUsesDefaultAqe(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, _, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      assertEquals(
        DeltaRangerTrustedAqeFixture.expected,
        frame.collect().toVector.groupBy(identity).map { case (row, rows) => row -> rows.length }
      )
      assertTrue(DeltaRangerTrustedAqeFixture.nodes(frame.queryExecution.executedPlan).exists {
        case plan: AdaptiveSparkPlanExec => plan.isFinalPlan
        case _                           => false
      })
    }

  @Test
  def preparedReadRejectsBeforePayload(): Unit = {
    Seq(Map.empty[String, String], DeltaRangerTrustedAqeFixture.coalescing).foreach { options =>
      DeltaRangerTrustedAqeFixture.withSpark(options) { (spark, people, _) =>
        for (
          reused <- Seq(false, true); mode <- Seq("predicate", "mask", "filter", "registration")
        ) {
          val frame = spark.sql(DeltaRangerTrustedAqeFixture.query + " WHERE p.region = 'east'")
          val root = frame.queryExecution.executedPlan
          assertTrue(root.isInstanceOf[DeltaRangerCollectGuardExec], root.treeString)
          if (options.isEmpty)
            assertTrue(
              DeltaRangerTrustedAqeFixture.nodes(root).exists(_.isInstanceOf[BroadcastHashJoinExec])
            )
          if (reused) assertEquals(3, frame.collect().length)
          else assertUnstartedBroadcasts(root)
          val binding = NativeSelectFixture.bindingFixture(spark, "a")
          val token = binding.properties.get("delta.registration.receipt.token")
          mode match {
            case "predicate" => people.denied.add("a:region")
            case "mask"      => people.mask = "'CHANGED'"
            case "filter"    => people.filter = "region = 'west'"
            case "registration" =>
              binding.properties.put("delta.registration.receipt.token", "replacement")
          }
          try {
            rejectedBeforeJobs(frame) { frame.collectAsList(); () }
            if (!reused) assertUnstartedBroadcasts(root)
            if (mode == "predicate")
              assertThrows(
                classOf[SecurityException],
                () => {
                  spark.sql(DeltaRangerTrustedAqeFixture.query + " WHERE p.region = 'east'"); ()
                }
              )
          } finally {
            people.denied.clear(); people.mask = "'MASKED'"; people.filter = "region = 'east'"
            binding.properties.put("delta.registration.receipt.token", token)
          }
        }
        val python = spark.sql(DeltaRangerTrustedAqeFixture.query)
        python.queryExecution.executedPlan
        people.denied.add("a:secret")
        try
          rejectedBeforeJobs(python) {
            try { python.getClass.getMethod("collectToPython").invoke(python); () }
            catch {
              case failure: java.lang.reflect.InvocationTargetException => throw failure.getCause
            }
          }
        finally people.denied.clear()
      }
    }
  }

  @Test
  def inFlightReadUsesCapturedDecisions(): Unit = {
    Seq(
      Map.empty[String, String],
      DeltaRangerTrustedAqeFixture.coalescing,
      DeltaRangerTrustedAqeFixture.coalescing + ("spark.databricks.delta.stats.skipping" -> "false")
    ).foreach { options =>
      DeltaRangerTrustedAqeFixture.withSpark(
        options +
          ("spark.plugins" -> classOf[DeltaRangerTrustedAqeBarrierPlugin].getName)
      ) { (spark, people, _) =>
        val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
        frame.queryExecution.executedPlan
        val owner = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner
        val executor = Executors.newSingleThreadExecutor()
        DeltaRangerTrustedAqeBarrier.arm(frame.queryExecution)
        try {
          val action = executor.submit(new Callable[Vector[Row]] {
            override def call(): Vector[Row] = frame.collect().toVector
          })
          assertTrue(
            DeltaRangerTrustedAqeBarrier.awaitPayload(),
            "Application payload did not reach the bounded barrier"
          )
          assertTrue(owner.readContext.nonEmpty, "Payload must follow action acceptance")
          people.mask = "'CHANGED'"
          people.filter = "region = 'west'"
          people.start()
          DeltaRangerTrustedAqeBarrier.release()
          assertEquals(
            DeltaRangerTrustedAqeFixture.expected,
            action.get(45, TimeUnit.SECONDS).groupBy(identity).map { case (row, rows) =>
              row -> rows.length
            }
          )
          rejectedBeforeJobs(frame) { frame.collect(); () }
          assertEquals(
            Vector(Row(2L, "CHANGED", "z")),
            spark.sql(DeltaRangerTrustedAqeFixture.query).collect().toVector
          )
        } finally {
          DeltaRangerTrustedAqeBarrier.release()
          executor.shutdownNow()
          assertTrue(executor.awaitTermination(45, TimeUnit.SECONDS))
        }
      }

    }
  }

  @Test
  def catalogAndPrunedDemandsRemainDistinct(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, people, dimension) =>
      Seq(
        DeltaRangerTrustedAqeFixture.query + " WHERE false",
        "SELECT p.secret FROM a.db.t p INNER JOIN a.db.t d ON p.id=d.id WHERE false",
        "SELECT secret FROM a.db.t WHERE id=999L"
      ).foreach { sql =>
        val frame = spark.sql(sql)
        frame.queryExecution.executedPlan
        val obligations =
          DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner.obligations
        if (sql.contains("JOIN")) assertEquals(2, obligations.size)
        if (sql.contains("b.db.t"))
          assertEquals(Set("a", "b"), obligations.map(_.scan.catalog).toSet)
        assertEquals(Vector.empty, frame.collect().toVector)
        people.denied.add("a:secret")
        try rejectedBeforeJobs(frame) { frame.collect(); () }
        finally people.denied.clear()
        people.denied.add("a:id")
        try rejectedBeforeJobs(frame) { frame.collect(); () }
        finally people.denied.clear()
        if (sql.contains("b.db.t")) {
          dimension.denied.add("b:id")
          try rejectedBeforeJobs(frame) { frame.collect(); () }
          finally dimension.denied.clear()
        }
      }
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      frame.queryExecution.executedPlan
      dimension.denied.add("b:id")
      try rejectedBeforeJobs(frame) { frame.collect(); () }
      finally dimension.denied.clear()
    }

  @Test
  def foreignExtensionAndContextDriftReject(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, _, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      frame.queryExecution.executedPlan
      val prepared = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan)
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(
        frame.queryExecution.analyzed,
        spark,
        optimized = false
      )
      val recipe = prepared.readRecipe(spark)
      assertThrows(
        classOf[SecurityException],
        () => {
          DeltaRangerExecutionContext.forRead(receipt, spark.newSession(), recipe); ()
        }
      )
      assertTrue(prepared.owner.readContext.isEmpty)
      val zone = spark.conf.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try rejectedBeforeJobs(frame) { frame.collect(); () }
      finally spark.conf.set("spark.sql.session.timeZone", zone)
      val original = DeltaRangerQuerySubject.current(spark)
      try
        UserGroupInformation
          .createUserForTesting("different-user", Array("different-group"))
          .doAs(new PrivilegedExceptionAction[Unit] {
            override def run(): Unit = rejectedBeforeJobs(frame) { frame.collect(); () }
          })
      finally
        UserGroupInformation.createUserForTesting(
          original.user(),
          original.groups().asScala.toArray
        )
      val strategies = spark.experimental.extraStrategies
      spark.experimental.extraStrategies = strategies :+ new SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = Nil
      }
      try rejectedBeforeJobs(frame) { frame.collect(); () }
      finally spark.experimental.extraStrategies = strategies
      spark.conf.set(
        "spark.sql.adaptive.customCostEvaluatorClass",
        "org.apache.spark.sql.execution.adaptive.SimpleCostEvaluator"
      )
      try rejectedBeforeJobs(frame) { frame.collect(); () }
      finally spark.conf.unset("spark.sql.adaptive.customCostEvaluatorClass")
    }

  @Test
  def namedJoinWithSkippingDisabled(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark(
      DeltaRangerTrustedAqeFixture.coalescing +
        ("spark.databricks.delta.stats.skipping" -> "false")
    ) { (spark, _, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      assertEquals(
        DeltaRangerTrustedAqeFixture.expected,
        frame.collect().toVector.groupBy(identity).map { case (row, rows) => row -> rows.length }
      )
    }

  @Test
  def preparedIndexReselectionUsesCapturedReadContext(): Unit =
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, people, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      assertEquals(3, frame.collect().length)
      val prepared = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan)
      val index = prepared.occurrences
        .find(_.obligation.scan.catalog == "a")
        .get
        .index
        .asInstanceOf[DeltaRangerPreparedIndex]
      people.start()
      val files =
        index.matchingFiles(Nil, Seq(org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral))
      assertFalse(files.isEmpty)
    }

  private def assertUnstartedBroadcasts(root: SparkPlan): Unit = {
    // Test-only observation of Spark's lazy future; reading its backing field does not submit it.
    // A non-null future means doPrepare/relationFuture has already submitted payload work.
    DeltaRangerTrustedAqeFixture
      .nodes(root)
      .collect { case exchange: org.apache.spark.sql.execution.exchange.BroadcastExchangeExec =>
        exchange
      }
      .foreach { exchange =>
        val field = classOf[org.apache.spark.sql.execution.exchange.BroadcastExchangeExec]
          .getDeclaredField("relationFuture")
        field.setAccessible(true)
        assertNull(field.get(exchange), "Static broadcast was submitted before action acceptance")
      }
  }

  private def rejectedBeforeJobs(frame: DataFrame)(action: => Unit): Unit = {
    val spark = frame.sparkSession
    val group = "trusted-aqe-denial-" + UUID.randomUUID().toString
    spark.sparkContext.setJobGroup(group, group)
    try {
      val failure = assertThrows(classOf[SecurityException], () => action)
      assertTrue(
        failure.getStackTrace.exists(element =>
          element.getClassName.endsWith(
            "DeltaRangerCollectGuardExec"
          ) && Set("executeCollect", "resetMetrics").contains(element.getMethodName)
        ),
        failure.toString
      )
      assertEquals(
        0,
        spark.sparkContext.statusTracker.getJobIdsForGroup(group).length,
        "Rejected action submitted a job after preparation"
      )
    } finally spark.sparkContext.clearJobGroup()
  }
}
