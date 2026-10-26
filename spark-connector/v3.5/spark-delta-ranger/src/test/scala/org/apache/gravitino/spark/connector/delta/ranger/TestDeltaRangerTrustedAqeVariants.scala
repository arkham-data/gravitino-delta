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

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.actions.{AddFile, AddCDCFile, RemoveFile, CommitInfo}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  AQEShuffleReadExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Observed stock optimizer branches over the admitted named read grammar. */
class TestDeltaRangerTrustedAqeVariants {
  import DeltaRangerTrustedAqeFixture._

  @Test
  def stockDefaults(): Unit = check("stockDefaults") { nodes =>
    assertTrue(nodes.exists(_.isInstanceOf[BroadcastHashJoinExec]))
  }
  @Test
  def coalescedShuffle(): Unit = check("coalescedShuffle", coalescing) { nodes =>
    assertTrue(nodes.exists { case s: ShuffleQueryStageExec => s.isMaterialized; case _ => false })
    assertTrue(nodes.exists {
      case r: AQEShuffleReadExec => r.hasCoalescedPartition; case _ => false
    })
  }
  @Test
  def staticBroadcast(): Unit =
    check("staticBroadcast", Map("spark.sql.autoBroadcastJoinThreshold" -> "10485760")) { nodes =>
      assertTrue(nodes.exists(_.isInstanceOf[BroadcastHashJoinExec]))
      val adaptive = nodes.collectFirst { case a: AdaptiveSparkPlanExec => a }.get
      assertTrue(
        DeltaRangerTrustedAqeFixture
          .nodes(adaptive.initialPlan)
          .exists(_.isInstanceOf[BroadcastHashJoinExec])
      )
    }
  @Test
  def adaptiveBroadcastAndLocalReader(): Unit = check(
    "adaptiveBroadcastAndLocalReader",
    coalescing ++ Map(
      "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "10485760",
      "spark.sql.adaptive.localShuffleReader.enabled" -> "true",
      "spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin" -> "0.0"
    )
  ) { nodes =>
    val adaptive = nodes.collectFirst { case a: AdaptiveSparkPlanExec => a }.get
    assertTrue(
      DeltaRangerTrustedAqeFixture
        .nodes(adaptive.initialPlan)
        .exists(_.isInstanceOf[SortMergeJoinExec])
    )
    assertTrue(nodes.exists(_.isInstanceOf[BroadcastHashJoinExec]))
    assertTrue(nodes.exists { case r: AQEShuffleReadExec => r.isLocalRead; case _ => false })
  }
  @Test
  def intraQueryExchangeReuse(): Unit = check(
    "intraQueryExchangeReuse",
    Map("spark.sql.autoBroadcastJoinThreshold" -> "10485760", "spark.sql.exchange.reuse" -> "true"),
    "SELECT p.id, p.secret, d.secret AS label, e.secret AS second FROM a.db.t p INNER JOIN b.db.t d ON p.id = d.id INNER JOIN b.db.t e ON p.id = e.id",
    Map(
      Row(1L, "MASKED", "x", "x") -> 1,
      Row(1L, "MASKED", "x", "y") -> 1,
      Row(1L, "MASKED", "y", "x") -> 1,
      Row(1L, "MASKED", "y", "y") -> 1,
      Row(2L, "MASKED", "z", "z") -> 1,
      Row(3L, "MASKED", "v", "v") -> 1
    ),
    Map(
      "a" -> "SELECT id, CAST(id AS STRING) secret, 'east' region FROM range(20000)"
    )
  ) { nodes =>
    assertTrue(nodes.exists(_.isInstanceOf[ReusedExchangeExec]))
  }
  @Test
  def skewedJoin(): Unit = check(
    "skewedJoin",
    coalescing ++ Map(
      "spark.sql.shuffle.partitions" -> "8",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "false",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1024",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "1",
      "spark.sql.adaptive.skewJoin.skewedPartitionFactor" -> "1.1",
      "spark.sql.adaptive.forceOptimizeSkewedJoin" -> "true"
    ),
    query,
    Map(
      Row(1L, "MASKED", "x") -> 20000,
      Row(2L, "MASKED", "z") -> 200,
      Row(3L, "MASKED", "v") -> 200
    ),
    Map(
      "a" -> "SELECT CASE WHEN id < 20000 THEN 1L WHEN id < 20200 THEN 2L ELSE 3L END id, CAST(id AS STRING) secret, 'east' region FROM range(0, 20400, 1, 8)",
      "b" -> "SELECT 1L id, 'x' secret, 'east' region UNION ALL SELECT 2L, 'z', 'east' UNION ALL SELECT 3L, 'v', 'east'"
    )
  ) { nodes =>
    assertTrue(nodes.exists { case join: SortMergeJoinExec => join.isSkewJoin; case _ => false })
    assertTrue(nodes.exists {
      case read: AQEShuffleReadExec => read.hasSkewedPartition; case _ => false
    })
  }
  @Test
  def runtimeFiltering(): Unit = check(
    "runtimeFiltering",
    coalescing ++ Map(
      "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "true",
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0",
      "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> "10485760"
    ),
    query + " WHERE d.secret = 'x'",
    Map(Row(1L, "MASKED", "x") -> 1)
  ) { nodes =>
    assertTrue(
      nodes.exists(
        _.expressions.exists(_.exists(e => e.getClass.getSimpleName == "BloomFilterMightContain"))
      )
    )
  }

  @Test
  def runtimeFilteringUsesCapturedActionAndRechecksNextAction(): Unit = {
    val options = coalescing ++ Map(
      "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "true",
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0",
      "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> "10485760",
      "spark.plugins" -> classOf[DeltaRangerTrustedAqeBarrierPlugin].getName
    )
    withSpark(options) { (spark, people, _) =>
      val sql = query + " WHERE d.secret = 'x'"
      val frame = spark.sql(sql)
      frame.queryExecution.executedPlan
      val workers = java.util.concurrent.Executors.newSingleThreadExecutor()
      DeltaRangerTrustedAqeBarrier.arm(frame.queryExecution)
      try {
        val action = workers.submit(new java.util.concurrent.Callable[Vector[Row]] {
          override def call(): Vector[Row] = frame.collect().toVector
        })
        assertTrue(DeltaRangerTrustedAqeBarrier.awaitPayload())
        people.mask = "'CHANGED'"
        people.start()
        DeltaRangerTrustedAqeBarrier.release()
        assertEquals(
          Vector(Row(1L, "MASKED", "x")),
          action.get(45, java.util.concurrent.TimeUnit.SECONDS)
        )
        assertTrue(
          nodes(frame.queryExecution.executedPlan).exists(
            _.expressions.exists(_.exists(_.getClass.getSimpleName == "BloomFilterMightContain"))
          )
        )
        org.slf4j.LoggerFactory
          .getLogger(getClass)
          .warn("TRUSTED_AQE_BLOOM_IN_FLIGHT\n" + frame.queryExecution.executedPlan)
        spark.sparkContext.setJobGroup("bloom-stale-action", "bloom-stale-action")
        try {
          assertThrows(classOf[SecurityException], () => frame.collect())
          assertEquals(
            0,
            spark.sparkContext.statusTracker.getJobIdsForGroup("bloom-stale-action").length
          )
        } finally spark.sparkContext.clearJobGroup()
        assertEquals(Vector(Row(1L, "CHANGED", "x")), spark.sql(sql).collect().toVector)
        val prepared = spark.sql(sql)
        prepared.queryExecution.executedPlan
        people.denied.add("a:id")
        spark.sparkContext.setJobGroup("bloom-pre-action", "bloom-pre-action")
        try {
          assertThrows(classOf[SecurityException], () => prepared.collect())
          assertEquals(
            0,
            spark.sparkContext.statusTracker.getJobIdsForGroup("bloom-pre-action").length
          )
        } finally { spark.sparkContext.clearJobGroup(); people.denied.clear() }
        assertThrows(
          classOf[SecurityException],
          () => spark.sql("SELECT secret FROM a.db.t WHERE id IN (SELECT id FROM b.db.t)")
        )
      } finally {
        DeltaRangerTrustedAqeBarrier.release()
        workers.shutdownNow()
        assertTrue(workers.awaitTermination(45, java.util.concurrent.TimeUnit.SECONDS))
      }
    }
  }

  @Test
  def nativeMergeStockDefaults(): Unit =
    nativeMergeVariant("stockDefaults", Map.empty, filtered = false)

  @Test
  def nativeMergeRuntimeFiltering(): Unit = nativeMergeVariant(
    "runtimeFiltering",
    coalescing ++ Map(
      "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "true",
      "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold" -> "0",
      "spark.sql.optimizer.runtime.bloomFilter.creationSideThreshold" -> "10485760"
    ),
    filtered = true
  )

  private def nativeMergeVariant(
      name: String,
      options: Map[String, String],
      filtered: Boolean
  ): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        PartitionedMergeFixture.origins(spark)
        val policies = new TestDeltaRangerMergePolicies
        if (filtered) policies.publish(plugin, filter = Some("region = 'keep'"))
        val executions = new ConcurrentLinkedQueue[QueryExecution]()
        spark.sparkContext.addSparkListener(new SparkListener {
          override def onJobStart(event: SparkListenerJobStart): Unit = {
            Option(event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)).foreach { id =>
              Option(SQLExecution.getQueryExecution(id.toLong)).foreach(executions.add)
            }
          }
        })
        spark.sql(TestDeltaRangerMergeSource.sql).collect()
        assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
        policies.publish(plugin)
        val expectedRows =
          Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")) ++
            (if (filtered) Set(Row(2L, "b-two", "drop")) else Set.empty[Row])
        assertEquals(
          expectedRows.map(_ -> 1).toMap,
          spark.table("b.db.t").collect().toVector.groupBy(identity).map { case (row, copies) =>
            row -> copies.size
          }
        )
        val expectedChanges = Vector(
          Row(1L, "b-one", "keep", "update_preimage"),
          Row(1L, "changed", "keep", "update_postimage"),
          Row(4L, "new", "keep", "insert")
        ) ++
          (if (filtered) Vector.empty else Vector(Row(2L, "b-two", "drop", "delete")))
        assertEquals(
          PartitionedMergeFixture.bag(expectedChanges),
          PartitionedMergeFixture.bag(
            spark
              .sql("SELECT id, secret, region, _change_type FROM table_changes('b.db.t', 1, 1)")
              .collect()
              .toVector
          )
        )
        val actions = PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), 1L)
        val original = PartitionedMergeFixture
          .actions(NativeSelectFixture.locations("b"), 0L)
          .collect { case add: AddFile => add.path }
          .toSet
        assertEquals(original, actions.collect { case remove: RemoveFile => remove.path }.toSet)
        assertEquals(1, actions.count(_.isInstanceOf[CommitInfo]))
        assertEquals(1, actions.count(_.isInstanceOf[RemoveFile]))
        assertTrue(actions.exists(_.isInstanceOf[AddCDCFile]))
        assertTrue(actions.forall {
          case _: AddFile | _: AddCDCFile | _: RemoveFile | _: CommitInfo => true
          case _                                                          => false
        })
        val adds = actions.collect { case add: AddFile => add }
        assertTrue(adds.forall(add => add.partitionValues.isEmpty && add.dataChange))
        assertEquals(
          expectedRows.size.toLong,
          adds
            .map(add =>
              new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(add.stats)
                .get("numRecords")
                .longValue()
            )
            .sum
        )
        val info = actions.collect { case info: CommitInfo => info }.head
        assertEquals("MERGE", info.operation)
        val metrics = info.operationMetrics.get
        assertEquals("1", metrics("numTargetRowsUpdated"))
        assertEquals("1", metrics("numTargetRowsInserted"))
        assertEquals(if (filtered) "0" else "1", metrics("numTargetRowsDeleted"))
        assertEquals(if (filtered) "2" else "1", metrics("numTargetRowsCopied"))
        org.slf4j.LoggerFactory
          .getLogger(getClass)
          .warn("TRUSTED_NATIVE_ACTIONS " + name + "\n" + actions.map(_.json).mkString("\n"))
        val phases = executions.asScala.toVector.distinct.flatMap { execution =>
          nodes(execution.executedPlan).collect {
            case phase: DeltaRangerMergeNative.Exec => "touched" -> phase.child
            case phase: DeltaRangerMergeWrite.Exec  => "write" -> phase.child
          }
        }
        assertEquals(Set("touched", "write"), phases.map(_._1).toSet)
        phases.foreach { case (phase, plan) =>
          val plans = nodes(plan)
          assertTrue(plans.exists {
            case a: AdaptiveSparkPlanExec => a.isFinalPlan; case _ => false
          })
          val bloom = plans.exists(
            _.expressions.exists(_.exists(_.getClass.getSimpleName == "BloomFilterMightContain"))
          )
          org.slf4j.LoggerFactory
            .getLogger(getClass)
            .warn(
              "TRUSTED_NATIVE_VARIANT " + name +
                " phase=" + phase + " bloom=" + bloom + " settings=" + options.toSeq.sorted
                  .mkString(",") + "\n" + plan
            )
        }
        if (filtered) assertTrue(phases.exists { case (phase, plan) =>
          phase == "touched" && nodes(plan).exists(
            _.expressions.exists(_.exists(_.getClass.getSimpleName == "BloomFilterMightContain"))
          )
        })
        if (!filtered) assertTrue(phases.exists { case (phase, plan) =>
          phase == "touched" && nodes(plan).exists(_.isInstanceOf[BroadcastHashJoinExec])
        })
      },
      prepareTarget = spark => PartitionedMergeFixture.reset(spark, keys = Nil),
      startupOptions = options + ("spark.sql.adaptive.enabled" -> "true")
    )

  private def check(
      name: String,
      options: Map[String, String] = Map.empty,
      sql: String = query,
      expectedBag: Map[Row, Int] = expected,
      data: Map[String, String] = Map.empty
  )(verify: Vector[SparkPlan] => Unit): Unit =
    withSpark(
      options ++ Map(
        "spark.sql.parquet.fieldId.read.enabled" -> "true",
        "spark.sql.parquet.fieldId.write.enabled" -> "true"
      ),
      data
    ) { (spark, _, _) =>
      PartitionedMergeFixture.origins(spark)
      val settings = spark.sessionState.conf.getAllConfs
      val frame = spark.sql(sql)
      val rows = frame.collect().toVector
      assertEquals(
        expectedBag,
        rows.groupBy(identity).map { case (row, copies) => row -> copies.size }
      )
      val plans = nodes(frame.queryExecution.executedPlan)
      assertTrue(plans.exists { case a: AdaptiveSparkPlanExec => a.isFinalPlan; case _ => false })
      org.slf4j.LoggerFactory
        .getLogger(getClass)
        .warn(
          "TRUSTED_AQE_VARIANT " + name +
            " settings=" + options.toSeq.sorted
              .mkString(",") + "\n" + frame.queryExecution.executedPlan
        )
      verify(plans)
      assertEquals(settings, spark.sessionState.conf.getAllConfs)
    }
}
