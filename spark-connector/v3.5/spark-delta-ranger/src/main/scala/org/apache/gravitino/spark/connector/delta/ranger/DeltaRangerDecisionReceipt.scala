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

import java.time.Instant

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.optimizer.RangePartitionIdRewrite
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** Driver authority is deliberately neither a Product nor Serializable. */
private[ranger] final class DeltaRangerDecisionReceipt private (
    spark: SparkSession,
    subject: DeltaRangerQuerySubject,
    val obligations: Vector[DeltaRangerDecisionReceipt.Obligation],
    stamps: Map[DeltaRangerPolicyEngine, DeltaRangerPolicyEngine.SnapshotStamp],
    val instant: Instant,
    approved: LogicalPlan,
    configuration: SQLConf
) {
  import DeltaRangerDecisionReceipt._
  private val conf = configuration.clone()
  private val settings = conf.getAllConfs
  private val original = detached(approved)
  private val analyzed = binding(original)
  SQLConf.withExistingConf(conf) { new Prefix(spark).qualify(original) }

  def consume(candidate: LogicalPlan, session: SparkSession, optimized: Boolean): Unit = {
    if (!(session eq spark)) denied("session changed")
    requireConfiguration()
    if (binding(original) != analyzed) denied("retained logical binding changed")
    SQLConf.withExistingConf(conf) { new Prefix(spark).qualify(original) }
    val expected = if (optimized) {
      val replay = new Prefix(spark)
      val result = SQLConf.withExistingConf(conf) { replay.expectedPlan(detached(original)) }
      requireConfiguration()
      binding(result)
    } else analyzed
    if (binding(candidate) != expected) denied("logical binding changed")
    revalidate(() => {
      if (binding(candidate) != expected) denied("logical binding changed")
    })
  }

  def revalidate(checkCandidate: () => Unit = () => ()): Unit = {
    obligations.foreach(_.binding.recheckPublication())
    // Reader callbacks and shared mutable literal payloads cannot change already-compared trees.
    if (binding(original) != analyzed) denied("retained logical binding changed")
    checkCandidate()
    stamps.foreach { case (engine, stamp) => engine.requireSnapshot(stamp) }
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("subject changed")
    requireConfiguration()
  }

  private def requireConfiguration(): Unit =
    if (settings != spark.sessionState.conf.getAllConfs) denied("configuration changed")

  override def toString: String = "Delta decision receipt"
}

private[ranger] object DeltaRangerDecisionReceipt {
  final class Obligation(
      val scan: DeltaRangerReadRule.Scan,
      val columns: Set[String],
      val binding: DeltaRangerRegistrationBindings.BoundRegistrationScan,
      val decisions: DeltaRangerMaskRules.Decisions
  ) {
    override def toString: String = "Original Delta scan obligation"
  }

  final class Reference(@transient private val authority: DeltaRangerDecisionReceipt)
      extends Serializable {
    def receipt: DeltaRangerDecisionReceipt = {
      if (authority == null) denied("inert receipt reference")
      authority
    }
    override def toString: String = "Delta authorization reference"
  }
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.decision-receipt")

  def present(plan: LogicalPlan): Boolean = plan.getTagValue(tag).nonEmpty

  def attach(
      plan: LogicalPlan,
      spark: SparkSession,
      subject: DeltaRangerQuerySubject,
      obligations: Seq[Obligation],
      stamps: Map[DeltaRangerPolicyEngine, DeltaRangerPolicyEngine.SnapshotStamp],
      instant: Instant,
      configuration: SQLConf
  ): Unit = {
    val receipt =
      new DeltaRangerDecisionReceipt(
        spark,
        subject,
        obligations.toVector,
        stamps,
        instant,
        plan,
        configuration
      )
    receipt.revalidate()
    plan.setTagValue(tag, new Reference(receipt))
  }

  def check(plan: LogicalPlan, spark: SparkSession, optimized: Boolean): Unit =
    plan
      .getTagValue(tag)
      .getOrElse(denied("missing receipt"))
      .receipt
      .consume(plan, spark, optimized)

  private def detached(plan: LogicalPlan): LogicalPlan = {
    val copy = plan.clone()
    copy.foreach(_.unsetTagValue(tag))
    copy
  }

  private final class Identity(value: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => value eq that.valueRef
      case _              => false
    }
    private def valueRef: AnyRef = value
    override def hashCode(): Int = System.identityHashCode(value)
  }

  // The finite expression classes are exact: subclasses must not introduce evaluation code.
  private val expressionClasses: Set[Class[_]] = Set(
    classOf[AttributeReference],
    classOf[Literal],
    classOf[Alias],
    classOf[Cast],
    classOf[EqualTo],
    classOf[EqualNullSafe],
    classOf[GreaterThan],
    classOf[GreaterThanOrEqual],
    classOf[LessThan],
    classOf[LessThanOrEqual],
    classOf[And],
    classOf[Or],
    classOf[Not],
    classOf[IsNull],
    classOf[IsNotNull],
    classOf[In],
    classOf[InSet],
    classOf[Upper],
    classOf[Lower],
    classOf[Concat],
    classOf[Coalesce],
    classOf[Add],
    classOf[Subtract],
    classOf[Multiply],
    classOf[Divide]
  )

  private def expression(expr: Expression): Vector[Any] = {
    if (!expressionClasses(expr.getClass)) denied("unqualified expression class")
    // Validate the complete subtree before any permitted constant evaluation.
    val fields = expr.productIterator.map(value).toVector
    expr match {
      case cast: Cast
          if cast.child.dataType == StringType &&
            Set[DataType](DateType, TimestampType, TimestampNTZType)(
              cast.dataType
            ) && cast.child.foldable =>
        val evaluated = cast.child.eval()
        if (
          evaluated != null && Set("now", "today", "tomorrow", "yesterday")
            .contains(
              evaluated.toString.trim.toLowerCase(java.util.Locale.ROOT).split("\\s+", 2).head
            )
        )
          denied("context-dependent datetime cast")
      case _ =>
    }
    val extra: Vector[Any] = expr match {
      case attr: AttributeReference =>
        Vector(
          attr.exprId.id,
          attr.exprId.jvmId,
          attr.qualifier.toVector,
          attr.name,
          attr.dataType.json,
          attr.nullable,
          attr.metadata.json
        )
      case alias: Alias =>
        Vector(
          alias.exprId.id,
          alias.exprId.jvmId,
          alias.qualifier.toVector,
          alias.explicitMetadata.map(_.json),
          alias.nonInheritableMetadataKeys.toVector
        )
      case _ => Vector.empty
    }
    Vector(expr.getClass, fields, extra)
  }

  private def value(item: Any): Any = item match {
    case null                    => null
    case expr: Expression        => expression(expr)
    case text: UTF8String        => text.getBytes.toVector
    case bytes: Array[Byte]      => bytes.toVector
    case decimal: Decimal        => decimal.toJavaBigDecimal
    case dataType: DataType      => dataType.json
    case metadata: Metadata      => metadata.json
    case id: ExprId              => (id.id, id.jvmId)
    case values: Seq[_]          => values.map(value).toVector
    case values: Set[_]          => values.map(value)
    case optional: Option[_]     => optional.map(value)
    case mode: Enumeration#Value => mode
    case text: String            => text
    case flag: Boolean           => flag
    case number: java.lang.Double =>
      (classOf[java.lang.Double], java.lang.Double.doubleToRawLongBits(number))
    case number: java.lang.Float =>
      (classOf[java.lang.Float], java.lang.Float.floatToRawIntBits(number))
    case number: java.lang.Byte    => number
    case number: java.lang.Short   => number
    case number: java.lang.Integer => number
    case number: java.lang.Long    => number
    case other                     => denied("unqualified expression value")
  }

  private def binding(plan: LogicalPlan): Vector[Any] = {
    val details: Any = plan match {
      case relation: LogicalRelation if relation.getClass == classOf[LogicalRelation] =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            Vector(
              new Identity(fs),
              new Identity(fs.location),
              new Identity(fs.fileFormat),
              fs.location match {
                case index: TahoeLogFileIndex => new Identity(index.deltaLog)
                case _                        => denied("source changed")
              },
              relation.catalogTable.map(t => new Identity(t)),
              relation.isStreaming,
              relation.catalogTable.map(t =>
                (
                  t.identifier,
                  t.owner,
                  t.provider,
                  t.schema.json,
                  t.storage,
                  t.properties,
                  t.partitionColumnNames.toVector
                )
              ),
              fs.dataSchema.json,
              fs.partitionSchema.json,
              fs.options
            )
          case _ => denied("source changed")
        }
      case project: Project if project.getClass == classOf[Project] =>
        project.projectList.map(expression).toVector
      case filter: Filter if filter.getClass == classOf[Filter] => expression(filter.condition)
      case alias: SubqueryAlias if alias.getClass == classOf[SubqueryAlias] =>
        Vector(alias.identifier.name, alias.identifier.qualifier.toVector)
      case join: Join if join.getClass == classOf[Join] =>
        Vector(join.joinType, join.hint, join.condition.map(expression))
      case _: InMemoryRelation => denied("cache substitution")
      case local: LocalRelation
          if local.getClass == classOf[LocalRelation] && local.data.isEmpty && !local.isStreaming =>
        Vector.empty
      case _ => denied("logical binding changed")
    }
    if (plan.children.nonEmpty) {
      val producers = plan.children.flatMap(_.output)
      plan.expressions.foreach(_.references.foreach { reference =>
        if (
          !producers.exists(a => a.exprId == reference.exprId && a.dataType == reference.dataType)
        )
          denied("attribute producer changed")
      })
    }
    Vector(
      plan.getClass,
      details,
      plan.output.map(expression).toVector,
      plan.children.map(binding).toVector
    )
  }

  private final class Prefix(spark: SparkSession)
      extends SparkOptimizer(
        spark.sessionState.catalogManager,
        spark.sessionState.catalog,
        spark.experimental
      ) {
    override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] = Seq(
      new RangePartitionIdRewrite(spark)
    )
    override def defaultBatches: Seq[Batch] =
      super.defaultBatches.takeWhile(_.name != "Pre CBO Rules")

    def qualify(plan: LogicalPlan): Unit = {
      val actual = spark.sessionState.optimizer.batches
      if (actual.count(_.name == "Pre CBO Rules") != 1) denied("optimizer boundary changed")
      val boundary = actual.find(_.name == "Pre CBO Rules").get
      if (
        boundary.rules.take(super.preCBORules.size).map(_.getClass) != super.preCBORules.map(
          _.getClass
        ) ||
        boundary.rules
          .drop(super.preCBORules.size)
          .headOption
          .forall(_.getClass != classOf[DeltaRangerPreCboRule])
      )
        denied("receipt optimizer consumer must precede native preparation")
      def strategyFields(strategy: AnyRef): Vector[Any] = strategy match {
        case product: Product => product.productIterator.toVector
        case _                => Vector.empty
      }
      def manifest(batch: Batch): Any =
        (
          batch.name,
          batch.strategy.getClass,
          batch.strategy.maxIterations,
          strategyFields(batch.strategy),
          batch.rules.map(_.getClass).toVector
        )
      if (
        actual
          .takeWhile(_.name != "Pre CBO Rules")
          .map(b =>
            (
              b.name,
              b.strategy.getClass,
              b.strategy.maxIterations,
              strategyFields(b.strategy),
              b.rules.map(_.getClass).toVector
            )
          ) != batches.map(manifest)
      )
        denied("unqualified optimizer prefix")
      if (
        plan.collect { case _: Join => 1 }.size > 1 &&
        spark.sessionState.conf
          .getConfString("spark.sql.cbo.starSchemaDetection", "false")
          .toBoolean &&
        !spark.sessionState.conf.getConfString("spark.sql.cbo.enabled", "false").toBoolean
      )
        denied("statistics-producing star-schema reordering")
    }

    def expectedPlan(plan: LogicalPlan): LogicalPlan = {
      qualify(plan)
      execute(plan)
    }
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta decision receipt rejected: " + reason)
}
