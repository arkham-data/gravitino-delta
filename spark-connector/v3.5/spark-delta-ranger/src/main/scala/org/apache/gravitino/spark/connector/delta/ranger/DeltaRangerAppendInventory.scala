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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/** Pinned extension inventories plus independently constructed stock analyzer batches. */
private[ranger] object DeltaRangerAppendInventory {
  private val resolution = Vector(
    "org.apache.spark.sql.execution.datasources.FindDataSourceTable",
    "org.apache.spark.sql.execution.datasources.ResolveSQLOnFile",
    "org.apache.spark.sql.execution.datasources.FallBackFileSourceV2",
    "org.apache.spark.sql.execution.aggregate.ResolveEncodersInScalaAgg$",
    "org.apache.spark.sql.catalyst.analysis.ResolveSessionCatalog",
    "org.apache.spark.sql.execution.streaming.ResolveWriteToStream$",
    "org.apache.spark.sql.catalyst.analysis.EvalSubqueriesForTimeTravel",
    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerReadRule",
    "org.apache.spark.sql.delta.ResolveDeltaPathTable",
    "org.apache.spark.sql.delta.PreprocessTimeTravel",
    "org.apache.spark.sql.delta.DeltaAnalysis"
  )
  private val postHoc = Vector(
    "org.apache.spark.sql.execution.analysis.DetectAmbiguousSelfJoin$",
    "org.apache.spark.sql.execution.datasources.QualifyLocationWithWarehouse",
    "org.apache.spark.sql.execution.datasources.PreprocessTableCreation",
    "org.apache.spark.sql.execution.datasources.PreprocessTableInsertion$",
    "org.apache.spark.sql.execution.datasources.DataSourceAnalysis$",
    "org.apache.spark.sql.execution.datasources.ApplyCharTypePadding$",
    "org.apache.spark.sql.catalyst.analysis.ReplaceCharWithVarchar$",
    "org.apache.spark.sql.delta.PreprocessTableUpdate",
    "org.apache.spark.sql.delta.PreprocessTableMerge",
    "org.apache.spark.sql.delta.PreprocessTableDelete",
    "org.apache.spark.sql.delta.PostHocResolveUpCast"
  )
  private val postHocWithAuthorization = postHoc.patch(
    7,
    Vector("org.apache.gravitino.spark.connector.authorization.RequiredPrivilegesCheck"),
    0
  )
  private val checks = Vector(
    "org.apache.spark.sql.execution.datasources.PreWriteCheck$",
    "org.apache.spark.sql.execution.datasources.PreReadCheck$",
    "org.apache.spark.sql.execution.datasources.HiveOnlyCheck$",
    "org.apache.spark.sql.execution.datasources.v2.TableCapabilityCheck$",
    "org.apache.spark.sql.execution.command.CommandCheck$",
    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerAppendCheck",
    "org.apache.spark.sql.delta.CheckUnresolvedRelationTimeTravel",
    "org.apache.spark.sql.delta.DeltaUnsupportedOperationsCheck"
  )
  private val normalization = Vector(
    "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawNormalization",
    "org.apache.spark.sql.delta.GenerateRowIDs$"
  )

  def qualify(spark: SparkSession): Unit = {
    if (!SparkSession.getActiveSession.exists(_ eq spark))
      throw new SecurityException("Delta append inventory rejected: session")
    val settings = spark.sessionState.conf.getAllConfs.toMap
    val actual = spark.sessionState.analyzer
    def requireClasses(values: Seq[AnyRef], names: Vector[String], stage: String): Unit = {
      val expected = names.map(Class.forName(_, false, getClass.getClassLoader))
      if (values.map(_.getClass).toVector != expected)
        throw new SecurityException("Delta append inventory rejected: " + stage)
    }
    requireClasses(actual.extendedResolutionRules, resolution, "resolution rules")
    val expectedPostHoc = Vector(postHoc, postHocWithAuthorization).map(
      _.map(Class.forName(_, false, getClass.getClassLoader))
    )
    if (!expectedPostHoc.contains(actual.postHocResolutionRules.map(_.getClass).toVector))
      throw new SecurityException("Delta append inventory rejected: post-hoc rules")
    requireClasses(actual.extendedCheckRules, checks, "check rules")
    requireClasses(spark.sessionState.planNormalizationRules, normalization, "normalization rules")
    val expected = new Analyzer(spark.sessionState.catalogManager) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] = actual.extendedResolutionRules
      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] = actual.postHocResolutionRules
    }
    def strategy(value: AnyRef): Vector[Any] = value match {
      case product: Product => product.productIterator.toVector
      case _                => Vector.empty
    }
    val observed = actual.batches.map(b =>
      (
        b.name,
        b.strategy.getClass,
        b.strategy.maxIterations,
        strategy(b.strategy),
        b.rules.map(_.getClass)
      )
    )
    val stock = expected.batches.map(b =>
      (
        b.name,
        b.strategy.getClass,
        b.strategy.maxIterations,
        strategy(b.strategy),
        b.rules.map(_.getClass)
      )
    )
    if (observed != stock)
      throw new SecurityException("Delta append inventory rejected: analyzer batches")
    DeltaRangerPhysicalPlanning.qualify(spark)
    if (settings != spark.sessionState.conf.getAllConfs.toMap)
      throw new SecurityException("Delta append inventory rejected: configuration changed")
  }
}

private[ranger] final class DeltaRangerAppendCheck(spark: SparkSession)
    extends (LogicalPlan => Unit) {
  override def apply(plan: LogicalPlan): Unit = DeltaRangerReadRule.check(plan, spark)
}
