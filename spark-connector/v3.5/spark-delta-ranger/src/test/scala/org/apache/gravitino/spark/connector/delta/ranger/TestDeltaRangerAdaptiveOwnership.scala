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

import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/** Installed feasibility probe; this does not grant governed payload authority. */
class TestDeltaRangerAdaptiveOwnership {
  @Test
  def stockHooksCannotObserveExactShuffleBeforeMaterialization(): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    val observed = new ConcurrentLinkedQueue[(String, ShuffleQueryStageExec, Boolean, Boolean)]()
    val calls = new ConcurrentLinkedQueue[String]()
    // Read the backing field only: calling the lazy accessor would itself submit work.
    val future = classOf[ShuffleQueryStageExec].getDeclaredField("shuffleFuture")
    future.setAccessible(true)
    def started(stage: ShuffleQueryStageExec): Boolean = future.get(stage) != null
    def rule(name: String): Rule[SparkPlan] = new Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        calls.add(name)
        val stages =
          DeltaRangerAdaptiveFixture.nodes(plan).collect { case s: ShuffleQueryStageExec => s }
        stages.foreach(s => observed.add((name, s, started(s), s.isMaterialized)))
        log.warn(
          "AQE_HOOK name={} thread={} activeSession={} stages={}",
          name,
          Thread.currentThread().getName,
          SparkSession.getActiveSession.isDefined.toString,
          stages
            .map(s =>
              s"${s.id}:${System.identityHashCode(s)}:started=${started(s)}:completed=${s.isMaterialized}"
            )
            .mkString(",")
        )
        plan
      }
    }
    val install: SparkSessionExtensions => Unit = extensions => {
      extensions.injectQueryPostPlannerStrategyRule(_ => rule("post-planner"))
      extensions.injectQueryStagePrepRule(_ => rule("stage-preparation"))
      extensions.injectQueryStageOptimizerRule(_ => rule("stage-optimizer"))
      extensions.injectColumnar(_ =>
        new ColumnarRule {
          override def preColumnarTransitions: Rule[SparkPlan] = rule("pre-columnar")
          override def postColumnarTransitions: Rule[SparkPlan] = rule("post-columnar")
        }
      )
    }
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName("adaptive-hook-probe")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .withExtensions(install)
    DeltaRangerAdaptiveFixture.profile.foreach { case (k, v) => builder.config(k, v) }
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try {
      log.warn(
        "AQE_PROVIDER spark={} java={} scala={} origin={}",
        spark.version,
        System.getProperty("java.version"),
        scala.util.Properties.versionNumberString,
        classOf[AdaptiveSparkPlanExec].getProtectionDomain.getCodeSource.getLocation
      )
      Seq(
        "org.apache.spark.sql.delta.DeltaLog",
        "org.apache.ranger.plugin.service.RangerBasePlugin"
      ).foreach { name =>
        val provider = Class.forName(name)
        val origin = provider.getProtectionDomain.getCodeSource.getLocation
        log.warn(s"AQE_DEPENDENCY class=$name origin=$origin")
      }
      val query = spark.sql(
        "SELECT l.id FROM range(0, 128, 1, 8) l INNER JOIN range(0, 128, 1, 8) r ON l.id = r.id"
      )
      val settings = spark.conf.getAll
      assertEquals((0L until 128L).toVector, query.collect().toVector.map(_.getLong(0)).sorted)
      val adaptive = query.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val completed = DeltaRangerAdaptiveFixture.materializedShuffleStages(adaptive)
      assertTrue(adaptive.isFinalPlan)
      assertTrue(completed.nonEmpty)
      assertTrue(DeltaRangerAdaptiveFixture.coalescedShuffleReads(adaptive).nonEmpty)
      assertEquals(settings, spark.conf.getAll)
      assertEquals(
        Set(
          "post-planner",
          "stage-preparation",
          "stage-optimizer",
          "pre-columnar",
          "post-columnar"
        ),
        calls.asScala.toSet
      )
      completed.foreach { stage =>
        assertTrue(started(stage))
        assertFalse(started(stage.copy()), "A fresh stage must not inherit the started marker")
        val sightings = observed.asScala.filter(x => x._2 eq stage).toVector
        assertTrue(sightings.nonEmpty, "Must observe exact completed stage, not only an equal copy")
        assertTrue(
          sightings.forall(_._3),
          "Unexpected pre-materialization stage callback requires investigation"
        )
        log.warn(
          "AQE_COMPLETED stage={} identity={} materialized={} sightings={}",
          stage.id.toString,
          System.identityHashCode(stage).toString,
          stage.isMaterialized.toString,
          sightings.map(x => s"${x._1}:started=${x._3}:completed=${x._4}").mkString(",")
        )
      }
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
