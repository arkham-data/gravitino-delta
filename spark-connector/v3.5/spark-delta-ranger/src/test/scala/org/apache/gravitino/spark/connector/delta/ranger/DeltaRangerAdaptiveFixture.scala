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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{
  AQEShuffleReadExec,
  AdaptiveSparkPlanExec,
  QueryStageExec,
  ShuffleQueryStageExec
}

private[ranger] object DeltaRangerAdaptiveFixture {
  val profile: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold" -> "0",
    "spark.sql.adaptive.skewJoin.enabled" -> "false",
    "spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled" -> "false",
    "spark.sql.adaptive.localShuffleReader.enabled" -> "false",
    "spark.sql.exchange.reuse" -> "false",
    "spark.sql.execution.reuseSubquery" -> "false",
    "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "false",
    "spark.sql.optimizer.runtime.bloomFilter.enabled" -> "false",
    "spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.shuffle.partitions" -> "32",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1048576"
  )

  def nodes(plan: SparkPlan): Vector[SparkPlan] = {
    val children = plan match {
      case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
      case stage: QueryStageExec           => Seq(stage.plan)
      case other                           => other.children
    }
    Vector(plan) ++ children.toVector.flatMap(nodes)
  }

  def materializedShuffleStages(plan: SparkPlan): Vector[ShuffleQueryStageExec] =
    nodes(plan).collect { case stage: ShuffleQueryStageExec if stage.isMaterialized => stage }

  def coalescedShuffleReads(plan: SparkPlan): Vector[AQEShuffleReadExec] =
    nodes(plan).collect { case read: AQEShuffleReadExec if read.hasCoalescedPartition => read }
}
