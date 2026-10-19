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

import scala.collection.mutable
import scala.reflect.ClassTag
import java.lang.reflect.Modifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{DataType, Metadata, StructField}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.{Analyzer, UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{
  ColumnWithDefaultExprUtils,
  DeltaColumnMappingMode,
  OptimisticTransaction
}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.commands.WriteIntoDelta
import org.apache.spark.sql.delta.constraints.{Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetStructField, Literal}
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.unsafe.types.UTF8String

/** Finite native helper lifetime; every admitted plan is reconstructed from retained inputs. */
private[ranger] final class DeltaRangerAppendHelper(
    input: DeltaRangerAppendV1Input.RetainedV1Input,
    sourceOwner: DeltaRangerAppendCoordinator.Source,
    transaction: OptimisticTransaction,
    native: WriteIntoDelta
) {
  import DeltaRangerAppendHelper._
  private val spark = input.session
  private var closed = false
  private var executing = false
  private var reconstructing = true
  // Validation callbacks cannot claim the first native tracker or scan while admission is open.
  private var admitting = false
  private var tracker: QueryPlanningTracker = _
  private var scanOwner: DeltaRangerNativeScans = _
  private var prepared: DeltaRangerNativeScans.Prepared = _
  private val original = input.analyzed
  private val nullAsDefault = spark.sessionState.conf.useNullsForMissingDefaultColumnValues
  private val defaults = ColumnWithDefaultExprUtils.tableHasDefaultExpr(
    transaction.protocol,
    transaction.metadata,
    nullAsDefault
  )
  // Matching schema is the retained frontend contract. Native public field expressions determine
  // the complete additional projection, including column order and quoting.
  private val projection = Project(
    transaction.metadata.schema
      .map(SchemaUtils.fieldToColumn)
      .map(_.expr.asInstanceOf[NamedExpression]),
    original
  )
  private val sourcePlans = analysisStages(spark, projection)
  private val expectedSource = sourcePlans.last
  private val receipt = input.receipt.forHelper(expectedSource)
  private val output = expectedSource.output.map(_.withNullability(true))
  private val partitionNames = transaction.metadata.partitionSchema.fieldNames.toSet
  private val statsOutput = output.filterNot(a => partitionNames(a.name))
  private val statsInputSchema = StructType(
    statsOutput.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
  )
  private val collection = new StatisticsCollection {
    override val columnMappingMode: DeltaColumnMappingMode = transaction.metadata.columnMappingMode
    override def tableSchema: StructType = transaction.metadata.schema
    override def outputTableStatsSchema: StructType = if (
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)
    )
      StructType(tableSchema.filterNot(f => partitionNames(f.name)))
    else statsInputSchema
    override def outputAttributeSchema: StructType = statsInputSchema
    override val spark: SparkSession = input.session
    override val statsColumnSpec =
      StatisticsCollection.configuredDeltaStatsColumnSpec(transaction.metadata)
    override val protocol: Protocol = transaction.protocol
  }
  private val local = LocalRelation(statsOutput)
  private val statsProjection = Project(
    Seq(
      UnresolvedAlias(
        to_json(collection.statsCollector).expr,
        Some(e => toPrettySQL(e))
      )
    ),
    local
  )
  private val statsPlans = Vector(local) ++ analysisStages(spark, statsProjection)

  private val constraints = Constraints.getAll(transaction.metadata, spark)
  // Mirrors only native extractor substitution; resolution and enforcement remain native.
  // Native invariant analysis runs both before planning and over the qualified optimized output.
  private val constraintOutputs = Vector(
    expectedSource.output,
    DeltaRangerDecisionReceipt.nativePrefix(expectedSource, spark).output
  ).distinct
  private val constraintPlans = constraintOutputs.flatMap { constraintOutput =>
    constraints
      .collect { case Constraints.Check(_, expression) =>
        val extracted = expression.transformUp { case attribute: UnresolvedAttribute =>
          val top = constraintOutput.collectFirst {
            case a: AttributeReference
                if SchemaUtils.DELTA_COL_RESOLVER(a.name, attribute.nameParts.head) =>
              a
          }
          top
            .map { start =>
              attribute.nameParts.tail.foldLeft[Expression](start) { case (child, name) =>
                child.dataType match {
                  case StructType(fields) =>
                    val ordinal =
                      fields.indexWhere(f => SchemaUtils.DELTA_COL_RESOLVER(f.name, name))
                    if (ordinal < 0) denied("constraint field is unavailable")
                    GetStructField(child, ordinal, Some(name))
                  case _ => denied("constraint field is not a struct")
                }
              }
            }
            .getOrElse(Literal(null))
        }
        analysisStages(spark, ConstraintRecipe(extracted))
      }
      .flatten
      .toVector
  }

  def requireLive(): Unit = {
    if (closed || !(active.get() eq this)) denied("expired helper owner")
    sourceOwner.requireLive()
  }

  def analysis(plan: LogicalPlan, session: SparkSession): Boolean = {
    if (admitting) denied("helper analysis reentry")
    admitting = true
    try {
      if (!(session eq spark)) denied("helper session changed")
      if (!plan.resolved) return false
      requireLive()
      if (statsPlans.exists(matches(_, plan, original))) return true
      if (
        plan.getClass == constraintWrapperClass && plan.children.isEmpty && plan.output.isEmpty &&
        plan.productArity == 1 && plan.expressions.size == 1 &&
        (plan.productElement(0).asInstanceOf[AnyRef] eq plan.expressions.head)
      ) {
        if (constraintPlans.exists(matches(_, ConstraintRecipe(plan.expressions.head), original)))
          return true
        denied("constraint expression changed")
      }
      if (matches(original, plan, original)) {
        input.receipt.consume(plan, spark, false); return true
      }
      if (!sourcePlans.exists(matches(_, plan, original))) return false
      receipt.consume(plan, spark, optimized = false)
      if (!reconstructing) {
        QueryPlanningTracker.get.foreach { observed =>
          if (tracker != null && !(tracker eq observed)) denied("helper tracker changed")
          tracker = observed
        }
      }
      true
    } finally admitting = false
  }

  def prepare(plan: LogicalPlan, session: SparkSession): Option[DeltaRangerNativeScans] = {
    if (admitting) denied("helper analysis reentry")
    admitting = true
    try {
      requireLive()
      if (!executing || reconstructing) denied("analysis-only helper cannot execute")
      if (!(session eq spark) || tracker == null || !QueryPlanningTracker.get.exists(_ eq tracker))
        denied("helper planning tracker changed")
      if (scanOwner != null) denied("helper preparation already claimed")
      receipt.consume(plan, spark, optimized = true)
      scanOwner = DeltaRangerNativeScans.forAppend(spark, receipt, sourceOwner)
      Some(scanOwner)
    } finally admitting = false
  }

  def complete(value: DeltaRangerNativeScans.Prepared): Unit = {
    requireLive()
    if (prepared != null || scanOwner == null || !(value.owner eq scanOwner))
      denied("helper preparation issuance changed")
    prepared = value
  }

  def requireExecutable(
      owner: DeltaRangerNativeScans,
      originalOwner: DeltaRangerNativeScans
  ): Unit = {
    requireLive()
    if (
      !executing || reconstructing ||
      !(if (defaults) owner eq scanOwner else owner eq originalOwner)
    )
      denied("helper execution owner changed")
  }

  def write() = {
    if (active.get() != null || closed) denied("nested or expired helper")
    active.set(this)
    try {
      requireLive()
      val normalized = SchemaUtils.normalizeColumnNames(
        transaction.deltaLog,
        transaction.metadata.schema,
        input.retainedData
      )
      val defaulted =
        if (defaults)
          ColumnWithDefaultExprUtils
            .addDefaultExprsOrReturnConstraints(
              transaction.deltaLog,
              transaction.protocol,
              input.queryExecution,
              transaction.metadata.schema,
              normalized,
              nullAsDefault
            )
            ._1
        else normalized
      val cleaned = SchemaUtils.dropNullTypeColumns(defaulted)
      if (defaults) receipt.consume(cleaned.queryExecution.analyzed, spark, optimized = false)
      else input.receipt.consume(cleaned.queryExecution.analyzed, spark, optimized = false)
      DeltaInvariantCheckerExec.buildInvariantChecks(expectedSource.output, constraints, spark)
      reconstructing = false
      executing = true
      native.writeAndReturnCommitData(transaction, spark)
    } finally {
      executing = false
      active.remove()
    }
  }
  def close(): Unit = { closed = true }
}

private[ranger] object DeltaRangerAppendHelper {
  private final case class ConstraintRecipe(expression: Expression) extends LeafNode {
    override def output: Seq[Attribute] = Nil
  }
  // The native wrapper is private to Delta. Its pinned sole expression field is compared against
  // our independently analyzed target recipe; no native private method or constructor is invoked.
  private val constraintWrapperClass = Class.forName(
    "org.apache.spark.sql.delta.constraints.ExpressionLogicalPlanWrapper",
    false,
    classOf[DeltaInvariantCheckerExec].getClassLoader
  )
  private val active = new ThreadLocal[DeltaRangerAppendHelper]()
  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean =
    DeltaRangerNativeOperation.current() == null && Option(active.get())
      .exists(_.analysis(plan, spark))
  def source(plan: LogicalPlan, spark: SparkSession): Option[DeltaRangerNativeScans] =
    Option(active.get()).flatMap(_.prepare(plan, spark))

  /** Stock analyzer runs only on independently built recipes, never on observed candidates. */
  private def analysisStages(spark: SparkSession, plan: LogicalPlan): Vector[LogicalPlan] = {
    val stages = mutable.ArrayBuffer[LogicalPlan](plan)
    val analyzer = new Analyzer(spark.sessionState.catalogManager) {
      override def batches: Seq[Batch] = super.batches.map { batch =>
        Batch(
          batch.name,
          batch.strategy,
          batch.rules.map { rule =>
            new Rule[LogicalPlan] {
              override def apply(input: LogicalPlan): LogicalPlan = {
                val result = rule(input)
                if (result.resolved) stages += result
                result
              }
            }
          }: _*
        )
      }
    }
    stages += analyzer.execute(plan)
    stages.toVector
  }

  /** Fresh helper aliases/aggregate IDs form one bijection; original source IDs never change. */
  private def matches(
      expected: LogicalPlan,
      actual: LogicalPlan,
      source: LogicalPlan
  ): Boolean = {
    if (!expected.resolved || !actual.resolved || expected.getClass != actual.getClass) return false
    def ids(value: Any): Set[ExprId] = value match {
      case id: ExprId  => Set(id)
      case seq: Seq[_] => seq.flatMap(ids).toSet
      case Some(v)     => ids(v)
      case _           => Set.empty
    }
    val retained = ids(DeltaRangerNativeProvenance.binding(source))
    val forward = mutable.Map.empty[ExprId, ExprId]
    val reverse = mutable.Map.empty[ExprId, ExprId]
    def equal(a: Any, b: Any): Boolean = (a, b) match {
      case (x: ExprId, y: ExprId) if retained(x) || retained(y) => x == y
      case (x: ExprId, y: ExprId) =>
        forward.getOrElseUpdate(x, y) == y && reverse.getOrElseUpdate(y, x) == x
      case (x: Seq[_], y: Seq[_]) =>
        x.size == y.size && x.zip(y).forall { case (l, r) => equal(l, r) }
      case (Some(x), Some(y)) => equal(x, y)
      case _                  => a == b
    }
    val functions: Set[Class[_]] = expected
      .flatMap(_.expressions.flatMap(_.collect { case udf: ScalaUDF =>
        udf.function.getClass
      }))
      .toSet
    try equal(helperBinding(expected, functions), helperBinding(actual, functions))
    catch { case _: SecurityException => false }
  }
  private def helperBinding(value: Any, functions: Set[Class[_]]): Any = value match {
    case relation: LogicalRelation => DeltaRangerNativeProvenance.binding(relation)
    case attribute: AttributeReference =>
      Vector(
        attribute.getClass,
        attribute.name,
        attribute.exprId,
        attribute.qualifier.toVector,
        attribute.dataType.json,
        attribute.nullable,
        attribute.metadata.json
      )
    case alias: Alias =>
      Vector(
        alias.getClass,
        helperBinding(alias.child, functions),
        alias.name,
        alias.exprId,
        alias.qualifier.toVector,
        alias.explicitMetadata.map(_.json),
        alias.nonInheritableMetadataKeys.toVector
      )
    case expression: Expression =>
      Vector(
        expression.getClass,
        expression.productIterator.map(v => helperBinding(v, functions)).toVector
      )
    case plan: LogicalPlan =>
      Vector(
        plan.getClass,
        plan.productIterator.map(v => helperBinding(v, functions)).toVector,
        plan.output.map(v => helperBinding(v, functions))
      )
    case encoder: ExpressionEncoder[_] =>
      Vector(
        encoder.getClass,
        helperBinding(encoder.objSerializer, functions),
        helperBinding(encoder.objDeserializer, functions),
        encoder.clsTag.runtimeClass
      )
    case tag: ClassTag[_]   => tag.runtimeClass
    case data: DataType     => data.json
    case metadata: Metadata => metadata.json
    case seq: Seq[_]        => seq.map(v => helperBinding(v, functions)).toVector
    case Some(v)            => Some(helperBinding(v, functions))
    case array: Array[_]    => array.map(v => helperBinding(v, functions)).toVector
    case text: UTF8String   => text.getBytes.toVector
    case map: scala.collection.Map[_, _] =>
      map.map { case (k, v) => helperBinding(k, functions) -> helperBinding(v, functions) }.toMap
    case function: Function1[_, _] =>
      if (!functions(function.getClass)) denied("foreign native statistics function")
      val fields =
        function.getClass.getDeclaredFields.filterNot(f => Modifier.isStatic(f.getModifiers))
      if (
        fields.length != 1 || fields.head.getName != "arg$1" ||
        fields.head.getType.getName != "org.apache.spark.sql.delta.stats.StatisticsCollection$$anonfun$3"
      )
        denied("native statistics closure shape changed")
      val field = fields.head
      field.setAccessible(true)
      val captured = field.get(function)
      if (captured == null || captured.getClass != field.getType)
        denied("native statistics capture changed")
      val captures =
        captured.getClass.getDeclaredFields.filterNot(f => Modifier.isStatic(f.getModifiers))
      if (
        captures.length != 1 || captures.head.getName != "stringPrefix$1" || captures.head.getType != java.lang.Integer.TYPE
      )
        denied("native statistics prefix shape changed")
      captures.head.setAccessible(true)
      Vector(
        function.getClass,
        field.getName,
        captured.getClass,
        captures.head.getName,
        captures.head.getInt(captured)
      )
    case other => other
  }

  private def denied(reason: String): Nothing = throw new SecurityException(
    "Delta append helper rejected: " + reason
  )
}
