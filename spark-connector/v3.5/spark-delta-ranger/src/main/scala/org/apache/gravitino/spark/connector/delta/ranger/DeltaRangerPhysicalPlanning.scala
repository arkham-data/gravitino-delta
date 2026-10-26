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
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.delta.stats.PrepareDeltaScan
import org.apache.spark.sql.delta.PreprocessTableWithDVsStrategy
import org.apache.spark.sql.execution.{
  SparkOptimizer,
  SparkPlanner,
  SparkStrategy,
  SparkPlan,
  QueryExecution,
  ApplyColumnarRulesAndInsertTransitions
}
import org.apache.spark.sql.execution.adaptive.InsertAdaptiveSparkPlan
import org.apache.spark.sql.execution.CollapseCodegenStages
import org.apache.spark.sql.execution.reuse.ReuseExchangeAndSubquery
import org.apache.spark.sql.execution.bucketing.{
  CoalesceBucketsInJoin,
  DisableUnnecessaryBucketedScan
}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.{
  PlanSubqueries,
  RemoveRedundantProjects,
  ReplaceHashWithSortAgg,
  RemoveRedundantSorts,
  RemoveRedundantWindowGroupLimits
}
import org.apache.spark.sql.internal.SQLConf

/** Seals the complete prepared recipe before the qualified stock optimizer suffix. */
private[ranger] object DeltaRangerPhysicalPlanning {
  final class LogicalWitness(plan: LogicalPlan, session: SparkSession) {
    private val original = plan.clone()
    private val frozen = DeltaRangerNativeProvenance.binding(original)
    private val configuration = session.sessionState.conf.clone()
    private val settings = configuration.getAllConfs

    def replay(spark: SparkSession, verifySource: LogicalPlan => Unit): LogicalPlan = {
      if (!(spark eq session) || settings != spark.sessionState.conf.getAllConfs)
        denied("planning session or configuration changed")
      def checkOriginal(): Unit = {
        verifySource(original)
        if (DeltaRangerNativeProvenance.binding(original) != frozen)
          denied("sealed preparation changed")
      }
      checkOriginal()
      val expected = SQLConf.withExistingConf(configuration) {
        val suffix = new Suffix(spark)
        suffix.qualify()
        suffix.execute(original.clone())
      }
      checkOriginal()
      if (settings != spark.sessionState.conf.getAllConfs)
        denied("planning configuration changed")
      expected
    }

    def verify(candidate: LogicalPlan, spark: SparkSession): LogicalPlan = {
      val expected = replay(spark, _ => ())
      if (
        DeltaRangerNativeProvenance.binding(candidate) !=
          DeltaRangerNativeProvenance.binding(expected)
      ) {
        denied("executable logical binding changed")
      }
      expected
    }
    override def toString: String = "Sealed Delta executable recipe"
  }

  final class NativeWitness private[DeltaRangerPhysicalPlanning] (
      plan: LogicalPlan,
      spark: SparkSession,
      authority: DeltaRangerNativeProvenance.Witness
  ) {
    private val original = plan.clone()
    private val executable = new LogicalWitness(plan, spark)
    def requireSession(session: SparkSession): Unit =
      if (!(session eq spark)) denied("native planning session changed")
    def requireLive(): Unit = authority.requireValid(original, spark)
    def replay(): LogicalPlan =
      executable.replay(spark, plan => authority.requireValid(plan, spark))
    def verify(candidate: LogicalPlan): LogicalPlan = {
      requireLive()
      val expected = executable.verify(candidate, spark)
      requireLive()
      expected
    }
    override def toString: String = "Native Delta planning evidence"
  }
  final class NativeReference(@transient private val authority: NativeWitness)
      extends Serializable {
    def get: NativeWitness = {
      if (authority == null) denied("inert native planning reference")
      authority
    }
    override def toString: String = "Native Delta planning reference"
  }
  private val nativeTag = TreeNodeTag[NativeReference]("gravitino.delta.ranger.native-planning")
  def native(plan: LogicalPlan): Option[NativeWitness] = plan.getTagValue(nativeTag).map(_.get)
  def sealNative(
      plan: LogicalPlan,
      spark: SparkSession,
      authority: DeltaRangerNativeProvenance.Witness
  ): LogicalPlan = {
    authority.requireValid(plan, spark)
    plan.setTagValue(nativeTag, new NativeReference(new NativeWitness(plan, spark, authority)))
    plan
  }

  def qualify(spark: SparkSession): Unit = {
    new Suffix(spark).qualify()
    planner(spark)
    val adaptive = spark.sessionState.adaptiveRulesHolder
    if (
      adaptive.queryPostPlannerStrategyRules.nonEmpty || adaptive.queryStagePrepRules.nonEmpty ||
      adaptive.runtimeOptimizerRules.nonEmpty || adaptive.queryStageOptimizerRules.nonEmpty ||
      spark.conf.getOption("spark.sql.adaptive.customCostEvaluatorClass").nonEmpty
    )
      denied("unqualified adaptive rules or evaluator")
    if (spark.sessionState.columnarRules.exists(_.getClass != classOf[DeltaRangerColumnarRule]))
      denied("unqualified columnar rules")
  }

  def raw(spark: SparkSession, logical: LogicalPlan): SparkPlan = {
    if (logical.exists(_.expressions.exists(_.exists(_.isInstanceOf[SubqueryExpression]))))
      denied("executable subquery")
    val qualified = planner(spark)
    QueryExecution.createSparkPlan(spark, qualified, logical.clone())
  }

  def appendRaw(
      spark: SparkSession,
      logical: AppendData
  ): SparkPlan =
    QueryExecution.createSparkPlan(spark, planner(spark), logical)

  /** Stock preparation of an already authorized native phase recipe. */
  def captured(spark: SparkSession, context: DeltaRangerExecutionContext.Context): SparkPlan = {
    qualify(spark)
    val recipe = new LogicalWitness(context.logicalPlan(), spark).replay(spark, _ => ())
    DeltaRangerPhysicalProvenance.captureRecipe(recipe, context, spark)
    // The qualified stock optimizer may add runtime-filter subqueries to this already
    // authorized native recipe. The unrelated legacy raw route retains its subquery gate.
    val physical = QueryExecution.createSparkPlan(spark, planner(spark), recipe.clone())
    new PhysicalPrefix(spark, recipe).applyCaptured(physical)
  }

  def lower(spark: SparkSession, logical: LogicalPlan): SparkPlan =
    new PhysicalPrefix(spark, logical).applyTo(raw(spark, logical))

  def tail(spark: SparkSession, logical: LogicalPlan, plan: SparkPlan): SparkPlan =
    new PhysicalPrefix(spark, logical).applyTail(plan)

  private def planner(spark: SparkSession): SparkPlanner = {
    if (spark.experimental.extraStrategies.nonEmpty)
      denied("unqualified planner strategies")
    val expected = new SparkPlanner(spark, spark.experimental) {
      override def extraPlanningStrategies: Seq[SparkStrategy] =
        Seq(new PreprocessTableWithDVsStrategy(spark))
    }
    if (
      spark.sessionState.planner.strategies.map(_.getClass) !=
        (classOf[DeltaRangerAppendInputStrategy] +: expected.strategies.map(_.getClass))
    )
      denied("unqualified planner strategies")
    expected
  }

  private final class PhysicalPrefix(spark: SparkSession, logical: LogicalPlan)
      extends QueryExecution(spark, logical) {
    def applyCaptured(plan: SparkPlan): SparkPlan =
      super.preparations
        .takeWhile(_.getClass != classOf[ApplyColumnarRulesAndInsertTransitions])
        .foldLeft(plan)((current, rule) => rule(current))

    def applyTail(plan: SparkPlan): SparkPlan = {
      val tail =
        super.preparations.dropWhile(_.getClass != classOf[ApplyColumnarRulesAndInsertTransitions])
      if (
        tail.map(_.getClass).toVector != Vector(
          classOf[ApplyColumnarRulesAndInsertTransitions],
          classOf[CollapseCodegenStages],
          ReuseExchangeAndSubquery.getClass
        )
      )
        denied("unqualified physical tail")
      // The actual companion callback contributes only the outer guard. Bind its child using
      // the same stock transitions/codegen/reuse order, without recursively invoking admission.
      val transitions = ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false)
      (transitions +: tail.tail).foldLeft(plan.clone())((current, rule) => rule(current))
    }

    def applyTo(plan: SparkPlan): SparkPlan = {
      val prefix = super.preparations
        .takeWhile(_.getClass != classOf[ApplyColumnarRulesAndInsertTransitions])
        .filterNot(_.getClass == classOf[InsertAdaptiveSparkPlan])
      val expected: Vector[Class[_]] = Vector(
        CoalesceBucketsInJoin.getClass,
        classOf[PlanDynamicPruningFilters],
        classOf[PlanSubqueries],
        RemoveRedundantProjects.getClass,
        classOf[EnsureRequirements],
        ReplaceHashWithSortAgg.getClass,
        RemoveRedundantSorts.getClass,
        RemoveRedundantWindowGroupLimits.getClass,
        DisableUnnecessaryBucketedScan.getClass
      )
      if (prefix.map(_.getClass).toVector != expected)
        denied("unqualified physical preparation prefix")
      prefix.foldLeft(plan)((current, rule) => rule(current))
    }
  }

  private final class Suffix(spark: SparkSession)
      extends SparkOptimizer(
        spark.sessionState.catalogManager,
        spark.sessionState.catalog,
        spark.experimental
      ) {
    override def preCBORules: Seq[Rule[LogicalPlan]] =
      super.preCBORules ++ Seq(new PrepareDeltaScan(spark), ConstantFolding)
    override def defaultBatches: Seq[Batch] = {
      val suffix = super.defaultBatches.dropWhile(_.name != "Pre CBO Rules")
      Batch(
        suffix.head.name,
        suffix.head.strategy,
        suffix.head.rules.drop(super.preCBORules.size): _*
      ) +: suffix.tail
    }

    def qualify(): Unit = {
      if (spark.experimental.extraOptimizations.nonEmpty)
        denied("unqualified optimizer suffix")
      val actual = spark.sessionState.optimizer.batches.dropWhile(_.name != "Pre CBO Rules")
      if (
        actual.isEmpty || actual.head.rules.count(_.getClass == classOf[DeltaRangerPreCboRule]) != 1
      )
        denied("optimizer consumer changed")
      val expected = batches
      def strategyFields(value: AnyRef): Vector[Any] = value match {
        case product: Product => product.productIterator.toVector
        case _                => Vector.empty
      }
      val actualManifest = actual.zipWithIndex.map { case (batch, index) =>
        val rules =
          if (index == 0)
            batch.rules.dropWhile(_.getClass != classOf[DeltaRangerPreCboRule]).drop(1)
          else batch.rules
        (
          batch.name,
          batch.strategy.getClass,
          batch.strategy.maxIterations,
          strategyFields(batch.strategy),
          rules.map(_.getClass).toVector
        )
      }
      val expectedManifest = expected.map { batch =>
        (
          batch.name,
          batch.strategy.getClass,
          batch.strategy.maxIterations,
          strategyFields(batch.strategy),
          batch.rules.map(_.getClass).toVector
        )
      }
      if (actualManifest != expectedManifest) denied("unqualified optimizer suffix")
    }
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta physical provenance rejected: " + reason)
}
