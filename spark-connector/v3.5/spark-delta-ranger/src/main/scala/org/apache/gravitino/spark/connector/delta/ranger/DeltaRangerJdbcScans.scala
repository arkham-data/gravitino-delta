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
import scala.collection.mutable
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{
  SparkPlan,
  SparkStrategy,
  ProjectExec,
  FilterExec,
  RowDataSourceScanExec
}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.connector.expressions.{
  NamedReference,
  Literal,
  GeneralScalarExpression,
  Expression => V2Expression
}
import org.apache.spark.sql.connector.expressions.filter.{
  Predicate,
  And => V2And,
  Or => V2Or,
  Not => V2Not
}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JDBCRelation}
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation,
  V1ScanWrapper
}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

/** Finite stock JDBC scan proof. Neither class names nor schemas issue source authority. */
private[ranger] object DeltaRangerJdbcScans {

  /** JDBC evidence carries no native Delta operation, index, snapshot or metadata permission. */
  final class Prepared private[DeltaRangerJdbcScans] (
      owner: DeltaRangerNativeScans,
      plan: LogicalPlan
  ) {
    // The stock Early Filter and Projection Push-Down batch follows Pre CBO Rules.
    // Retain issued relations here; the complete suffix derives their native scans later.
    private val relations = plan.collect { case relation: DataSourceV2Relation =>
      relation
    }.toVector
    private val frozen = relations.map(logicalBinding)
    val obligations: Vector[DeltaRangerDecisionReceipt.Obligation] = relations.map { relation =>
      val found = owner.obligations.filter { obligation =>
        obligation.source match {
          case source: DeltaRangerInputSources.RegisteredJdbc =>
            (source.relation.table eq relation.table) && relation.output.forall(attribute =>
              source.relation.output.exists(_.exprId == attribute.exprId)
            )
          case _ => false
        }
      }
      if (found.size != 1) denied()
      found.head
    }
    private var replay: () => LogicalPlan = _
    private val continuations = relations.zip(frozen).map { case (relation, expected) =>
      val continuation = new Continuation(owner, this, expected)
      relation.setTagValue(tag, new Reference(continuation))
      continuation
    }

    def bindRecipe(value: () => LogicalPlan): Unit = {
      if (replay != null) denied()
      replay = value
    }

    def verify(plan: LogicalPlan): Unit = {
      val actual = plan.collect {
        case relation: DataSourceV2Relation => logicalBinding(relation)
        case scan: DataSourceV2ScanRelation =>
          binding(scan)
          logicalBinding(scan.relation)
      }.toVector
      if (actual != frozen) denied()
    }

    private[DeltaRangerJdbcScans] def requireRecipe(
        candidate: LogicalPlan,
        expectedScan: Any
    ): Unit = {
      if (replay == null || !continuations.exists(_.expectedScan == expectedScan)) denied()
      val executable = replay()
      val permitted = executable.collect {
        case operation @ PhysicalOperation(_, _, scan: DataSourceV2ScanRelation)
            if logicalBinding(scan.relation) == expectedScan =>
          DeltaRangerDecisionReceipt.binding(operation)
      }
      if (!permitted.contains(DeltaRangerDecisionReceipt.binding(candidate))) denied()
    }
  }

  private final class Continuation(
      owner: DeltaRangerNativeScans,
      prepared: Prepared,
      val expectedScan: Any
  ) {
    private val native = mutable.Map.empty[Vector[Any], SparkPlan]
    def physical(
        plan: LogicalPlan,
        scan: DataSourceV2ScanRelation,
        session: SparkSession
    ): SparkPlan = synchronized {
      if ((session ne owner.session) || logicalBinding(scan.relation) != expectedScan) denied()
      owner.requireLive()
      prepared.requireRecipe(plan, expectedScan)
      val key = DeltaRangerDecisionReceipt.binding(plan)
      val result = native.getOrElseUpdate(
        key, {
          // Stock fallback allocates its anonymous JDBC relation and JDBCRDD exactly once here.
          val emitted = new DataSourceV2Strategy(session).apply(plan)
          if (emitted.size != 1) denied()
          val nodes = emitted.head.collect { case node => node }
          if (
            nodes.count(_.getClass == classOf[RowDataSourceScanExec]) != 1 || nodes.exists(node =>
              !Set[Class[_]](
                classOf[RowDataSourceScanExec],
                classOf[ProjectExec],
                classOf[FilterExec]
              )(node.getClass)
            )
          ) denied()
          emitted.head
        }
      )
      if (
        DeltaRangerDecisionReceipt
          .binding(plan) != key || logicalBinding(scan.relation) != expectedScan
      ) denied()
      result
    }
  }

  private final class Reference(@transient private val value: Continuation) extends Serializable {
    def continuation: Continuation = {
      if (value == null) denied()
      value
    }
    override def toString: String = "Registered JDBC continuation reference"
  }
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.registered-jdbc-scan")

  def prepare(plan: LogicalPlan, owner: DeltaRangerNativeScans): Prepared =
    new Prepared(owner, plan)

  def plan(logical: LogicalPlan, spark: SparkSession): Seq[SparkPlan] = logical match {
    case operation @ PhysicalOperation(_, _, scan: DataSourceV2ScanRelation) =>
      Seq(
        scan.relation
          .getTagValue(tag)
          .getOrElse(denied())
          .continuation
          .physical(operation, scan, spark)
      )
    case _ => Nil
  }

  final class Strategy(spark: SparkSession) extends SparkStrategy {
    override def apply(logical: LogicalPlan): Seq[SparkPlan] = plan(logical, spark)
  }

  private final class Identity(private val value: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => value eq that.value
      case _              => false
    }
    override def hashCode(): Int = System.identityHashCode(value)
    override def toString: String = "Issued JDBC identity"
  }

  private final class Frozen(private val fields: Vector[Any]) {
    override def equals(other: Any): Boolean = other match {
      case that: Frozen => fields == that.fields
      case _            => false
    }
    override def hashCode(): Int = fields.hashCode()
    override def toString: String = "Bound stock JDBC scan"
  }

  def logicalBinding(relation: DataSourceV2Relation): Any = {
    val source = DeltaRangerInputSources.retainedJdbc(relation, SparkSession.active)
    new Frozen(
      Vector(
        new Identity(source.binding),
        relation.identifier.map(id => (id.namespace().toVector, id.name())),
        relation.schema.json
      )
    )
  }

  def binding(relation: DataSourceV2ScanRelation): Any = {
    if (
      relation.getClass != classOf[
        DataSourceV2ScanRelation
      ] || relation.isStreaming || relation.keyGroupedPartitioning.nonEmpty || relation.ordering.nonEmpty
    ) denied()
    val source = DeltaRangerInputSources.retainedJdbc(relation.relation, SparkSession.active)
    val wrapper = relation.scan match {
      case value: V1ScanWrapper if value.getClass == classOf[V1ScanWrapper] => value
      case _                                                                => denied()
    }
    val scan = wrapper.v1Scan match {
      case value: JDBCScan if value.getClass == classOf[JDBCScan] => value
      case _                                                      => denied()
    }
    if (
      scan.relation.getClass != DeltaRangerJdbcSourceBindings
        .nativeRelationClass() || (scan.relation.sqlContext.sparkSession ne SparkSession.active)
    ) denied()
    source.binding.requireOptions(scan.relation.jdbcOptions)
    if (scan.relation.schema != relation.relation.table.schema() || scan.relation.parts.length != 1)
      denied()
    scan.relation.parts.head match {
      case partition: JDBCPartition
          if partition.getClass == classOf[
            JDBCPartition
          ] && partition.idx == 0 && partition.whereClause == null =>
      case _ => denied()
    }
    val operators = wrapper.pushedDownOperators
    if (
      scan.pushedAggregateColumn.nonEmpty || scan.groupByColumns.nonEmpty || scan.tableSample.nonEmpty || scan.pushedLimit != 0 || scan.pushedOffset != 0 || scan.sortOrders.nonEmpty || operators.aggregation.nonEmpty || operators.sample.nonEmpty || operators.limit.nonEmpty || operators.offset.nonEmpty || operators.sortValues.nonEmpty
    ) denied()
    val declared = relation.relation.table.schema().fields.toVector
    if (
      !scan.prunedSchema.fields.forall(declared.contains) || wrapper
        .readSchema() != scan.prunedSchema || relation.schema != scan.prunedSchema
    ) denied()
    new Frozen(
      Vector(
        logicalBinding(relation.relation),
        scan.relation.schema.json,
        scan.prunedSchema.json,
        scan.pushedPredicates.toVector.map(v2),
        wrapper.handledFilters.toVector.map(v1),
        operators.pushedPredicates.toVector.map(v2)
      )
    )
  }

  private def v2(expression: V2Expression): Any = expression match {
    case predicate: Predicate
        if Set[Class[_]](classOf[Predicate], classOf[V2And], classOf[V2Or], classOf[V2Not])(
          predicate.getClass
        ) =>
      if (
        !Set(
          "=",
          "<>",
          "<=>",
          ">",
          ">=",
          "<",
          "<=",
          "AND",
          "OR",
          "NOT",
          "IS_NULL",
          "IS_NOT_NULL",
          "IN",
          "ALWAYS_TRUE",
          "ALWAYS_FALSE"
        )(predicate.name())
      ) denied()
      Vector(predicate.getClass, predicate.name(), predicate.children().toVector.map(v2))
    case reference: NamedReference
        if reference.getClass == DeltaRangerJdbcSourceBindings.fieldReferenceClass() =>
      if (reference.fieldNames().length != 1) denied()
      Vector(reference.getClass, reference.fieldNames().toVector)
    case literal: Literal[_]
        if literal.getClass == DeltaRangerJdbcSourceBindings.literalValueClass() =>
      Vector(literal.getClass, literal.dataType().json, scalar(literal.value()))
    case expression: GeneralScalarExpression
        if expression.getClass == classOf[GeneralScalarExpression] && Set("+", "-", "*", "/")(
          expression.name()
        ) =>
      Vector(expression.getClass, expression.name(), expression.children().toVector.map(v2))
    case _ => denied()
  }

  private val filters: Set[Class[_]] = Set(
    classOf[sources.EqualTo],
    classOf[sources.EqualNullSafe],
    classOf[sources.GreaterThan],
    classOf[sources.GreaterThanOrEqual],
    classOf[sources.LessThan],
    classOf[sources.LessThanOrEqual],
    classOf[sources.IsNull],
    classOf[sources.IsNotNull],
    classOf[sources.In],
    classOf[sources.And],
    classOf[sources.Or],
    classOf[sources.Not],
    classOf[sources.AlwaysTrue],
    classOf[sources.AlwaysFalse]
  )
  def filterBinding(filter: sources.Filter): Any = v1(filter)

  private def v1(filter: sources.Filter): Any = {
    if (!filters(filter.getClass)) denied()
    Vector(filter.getClass, filter.asInstanceOf[Product].productIterator.map(scalar).toVector)
  }

  private def scalar(value: Any): Any = value match {
    case null                          => null
    case filter: sources.Filter        => v1(filter)
    case text: String                  => text
    case text: UTF8String              => text.getBytes.toVector
    case boolean: Boolean              => boolean
    case number: java.lang.Byte        => number
    case number: java.lang.Short       => number
    case number: java.lang.Integer     => number
    case number: java.lang.Long        => number
    case number: java.lang.Float       => java.lang.Float.floatToRawIntBits(number)
    case number: java.lang.Double      => java.lang.Double.doubleToRawLongBits(number)
    case number: Decimal               => number.toJavaBigDecimal
    case number: java.math.BigDecimal  => number
    case date: java.sql.Date           => date.getTime
    case timestamp: java.sql.Timestamp => (timestamp.getTime, timestamp.getNanos)
    case values: Array[_]              => values.toVector.map(scalar)
    case _                             => denied()
  }

  private def denied(): Nothing = throw new SecurityException(
    "Registered JDBC scan does not match the finite stock profile"
  )
}
