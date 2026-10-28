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

import java.util.IdentityHashMap

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  Expression,
  ExprId,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec,
  ShuffleQueryStageExec,
  BroadcastQueryStageExec,
  AQEShuffleReadExec
}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.exchange.{
  ReusedExchangeExec,
  ShuffleExchangeExec,
  BroadcastExchangeExec
}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String

/** Exact driver-local physical bindings; no plan text or canonical equality is authority. */
private[ranger] object DeltaRangerPhysicalProvenance {
  final class Witness private[DeltaRangerPhysicalProvenance] (
      expected: SparkPlan,
      alternatives: Vector[SparkPlan] = Vector.empty,
      ordinaryLogicalIds: Option[Set[ExprId]] = None
  ) {
    private val recipes = expected +: alternatives
    private val frozen = recipes.map(binding)
    private def comparable(plan: SparkPlan): Vector[Any] = ordinaryLogicalIds match {
      case Some(ids) => bindingWithGrouping(plan, generatedGrouping(plan, ids))
      case None      => binding(plan)
    }
    private val comparisons = recipes.map(comparable)
    def verify(candidate: SparkPlan): Unit = {
      if (recipes.map(binding) != frozen) denied("sealed physical recipe changed")
      if (!comparisons.contains(comparable(candidate))) {
        denied("executable physical binding changed")
      }
    }
    override def toString: String = "Sealed Delta physical recipe"
  }

  def user(
      plan: SparkPlan,
      logical: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      prepared: DeltaRangerNativeScans.Prepared,
      spark: SparkSession
  ): Witness = {
    prepared.owner.requireExecutable()
    val expectedLogical = prepared.verifyExecutable(logical, spark)
    val witness = userWitness(expectedLogical, spark, ordinary = !prepared.owner.appendInput)
    witness.verify(plan)
    witness
  }

  private def userWitness(
      logical: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: SparkSession,
      ordinary: Boolean = false
  ): Witness = {
    val expected = DeltaRangerPhysicalPlanning.lower(spark, logical)
    val tail = DeltaRangerPhysicalPlanning.tail(spark, logical, expected)
    val ids =
      if (ordinary) Some(logical.flatMap { node =>
        node.output.map(_.exprId) ++ node.expressions.flatMap { expression =>
          expression.collect {
            case attribute: AttributeReference  => attribute.exprId
            case alias: Alias                   => alias.exprId
            case aggregate: AggregateExpression => aggregate.resultId
          }
        }
      }.toSet)
      else None
    new Witness(expected, Vector(tail), ids)
  }

  sealed trait Ticket {
    def requireSession(session: SparkSession): Unit
    def verify(plan: SparkPlan): Unit
  }
  final class UserTicket(
      private val witness: Witness,
      private val prepared: DeltaRangerNativeScans.Prepared,
      private val spark: SparkSession
  ) extends Ticket
      with DeltaRangerCollectValidationContext {
    override def requireSession(session: SparkSession): Unit =
      if (!(session eq spark)) denied("user planning session changed")
    override def verify(plan: SparkPlan): Unit = {
      prepared.owner.requireExecutable()
      if (spark.sessionState.conf.adaptiveExecutionEnabled)
        denied("unsupported user AQE")
      witness.verify(plan)
    }
    override def validateCollection(plan: SparkPlan): Unit = {
      verify(plan)
      prepared.owner.validateAction(spark)
      verify(plan)
    }
    override def validateExecution(plan: SparkPlan): Unit = validateCollection(plan)

    /** Initializes only the exact verified append scans before asynchronous broadcast preparation.
      */
    def prepareInputs(plan: SparkPlan): Unit = if (prepared.owner.appendInput) {
      validateCollection(plan)
      prepared.owner.prepareInputs(plan)
      validateCollection(plan)
    }
    override def toString: String = "Delta user collection evidence"
  }

  /** Ordinary read: policy checks belong to the outer supported action only. */
  final class TrustedReadTicket(
      private val prepared: DeltaRangerNativeScans.Prepared,
      private val spark: SparkSession,
      private val recipe: LogicalPlan,
      private val physicalWitness: Option[Witness] = None
  ) extends Ticket
      with DeltaRangerCollectValidationContext {
    private var installed = false
    private var root: SparkPlan = _
    private var action: Option[(Long, QueryExecution)] = None
    private var directCollection = false

    override def beginDatasetAction(actualRoot: SparkPlan, child: SparkPlan): Unit = {
      val binding = currentAction(actualRoot).getOrElse(denied("read has no active Dataset action"))
      // Stock physical preparation may copy the guard after columnar-rule installation.
      // Only the exact active QueryExecution root with this issued owner can bind here.
      root = actualRoot
      verify(child)
      if (!action.contains(binding) || prepared.owner.readContext.isEmpty) {
        action = None
        prepared.owner.endRead()
        DeltaRangerPhysicalPlanning.qualify(spark)
        prepared.owner.beginRead(
          recipe,
          () => SQLExecution.getQueryExecution(binding._1) eq binding._2
        )
        action = Some(binding)
      }
    }

    override def endCollection(): Unit = if (directCollection) {
      directCollection = false
      prepared.owner.endRead()
    }
    override def requireSession(session: SparkSession): Unit =
      if (session ne spark) denied("read planning session changed")
    // Stock adaptive stage trees are execution products, not authorization evidence.
    override def verify(plan: SparkPlan): Unit = physicalWitness.foreach(_.verify(plan))
    override def validateCollection(plan: SparkPlan): Unit = {
      verify(plan)
      if (currentAction().exists(binding => action.contains(binding))) validateExecution(plan)
      else {
        // A direct physical collection may run inside a native SQLExecution wrapper.
        // It owns only this synchronous collection, never a Dataset/Arrow binding.
        DeltaRangerPhysicalPlanning.qualify(spark)
        prepared.owner.beginRead(recipe)
        directCollection = true
      }
    }
    override def validateExecution(plan: SparkPlan): Unit = {
      verify(plan)
      if (prepared.owner.readContext.isEmpty) denied("read has no accepted action")
      if (!directCollection && !currentAction().exists(binding => action.contains(binding)))
        denied("read execution belongs to another action")
    }
    def finish(plan: SparkPlan): SparkPlan = synchronized {
      if (installed) {
        validateExecution(plan)
        plan
      } else {
        installed = true
        root = DeltaRangerCollectGuardExec(plan)(this)
        root
      }
    }

    private def currentAction(expectedRoot: SparkPlan = root): Option[(Long, QueryExecution)] =
      Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).flatMap { value =>
        scala.util.Try(value.toLong).toOption.flatMap { id =>
          Option(SQLExecution.getQueryExecution(id))
            .filter { query =>
              (query.sparkSession eq spark) && (query.executedPlan eq expectedRoot) &&
              DeltaRangerNativeScans.evidence(query.optimizedPlan).exists(_.owner eq prepared.owner)
            }
            .map(query => (id, query))
        }
      }
  }

  /** Captured native phase: the owner checks current authority before entering Spark. */
  final class CapturedTicket(val context: DeltaRangerExecutionContext.Context, spark: SparkSession)
      extends Ticket {
    override def requireSession(session: SparkSession): Unit =
      if (session ne spark) denied("captured planning session changed")
    override def verify(plan: SparkPlan): Unit = { context.logicalPlan(); () }
  }
  private val capturedTag = TreeNodeTag[Reference]("gravitino.delta.ranger.captured-recipe")
  def captureRecipe(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      context: DeltaRangerExecutionContext.Context,
      spark: SparkSession
  ): Unit = {
    val reference = new Reference(new CapturedTicket(context, spark))
    def attach(recipe: LogicalPlan): Unit = recipe.foreach { node =>
      node.setTagValue(capturedTag, reference)
      node.expressions.foreach(_.foreach {
        case subquery: SubqueryExpression => attach(subquery.plan)
        case _                            =>
      })
    }
    attach(plan)
  }

  final class AppendTicket(private val spark: SparkSession) extends Ticket {
    override def requireSession(session: SparkSession): Unit =
      if (!(session eq spark)) denied("append planning session changed")
    override def verify(plan: SparkPlan): Unit = plan match {
      case guard: DeltaRangerAppendInputExec => guard.verify(spark)
      case _                                 => denied("append physical product changed")
    }
  }
  final class CdfTicket(private val spark: SparkSession) extends Ticket {
    override def requireSession(session: SparkSession): Unit =
      if (session ne spark) denied("CDF planning session changed")
    override def verify(plan: SparkPlan): Unit = plan match {
      case command: DeltaRangerCdfPropertyExec => command.verify(spark)
      case _                                   => denied("CDF physical command changed")
    }
  }
  final class NativeTicket(
      private val expected: SparkPlan,
      private val raw: SparkPlan,
      private val native: DeltaRangerPhysicalPlanning.NativeWitness
  ) extends Ticket {
    private var mergeBoundaryInstalled = false
    def installMergeBoundary(): Boolean = synchronized {
      if (mergeBoundaryInstalled) false else { mergeBoundaryInstalled = true; true }
    }
    private val expectedBinding = binding(expected)
    private val rawBinding = binding(raw)
    private val subtrees = nodes(expected).map(binding).toSet
    override def requireSession(session: SparkSession): Unit = native.requireSession(session)
    override def verify(plan: SparkPlan): Unit = {
      native.requireLive()
      if (binding(expected) != expectedBinding || binding(raw) != rawBinding)
        denied("sealed native physical recipe changed")
      plan match {
        case adaptive: AdaptiveSparkPlanExec if plan.getClass == classOf[AdaptiveSparkPlanExec] =>
          new Witness(raw).verify(adaptive.inputPlan)
          new Witness(expected).verify(adaptive.initialPlan)
        case _: AdaptiveSparkPlanExec => denied("unknown adaptive wrapper")
        case _ =>
          if (!subtrees.contains(binding(plan))) {
            denied("native stage is not an issued subtree")
          }
      }
    }
    override def toString: String = "Native Delta physical evidence"
  }
  final class RawMetadataContext private[DeltaRangerPhysicalProvenance] (
      native: NativeTicket,
      metadata: DeltaRangerRawMetadata
  ) extends DeltaRangerCollectValidationContext {
    override def validateCollection(child: SparkPlan): Unit = native.verify(child)
    override def validateExecution(child: SparkPlan): Unit = {
      native.verify(child)
      metadata.recordExecution(child)
    }
  }

  final class Reference(@transient private val authority: Ticket) extends Serializable {
    def get: Ticket = {
      if (authority == null) denied("inert physical reference")
      authority
    }
    override def toString: String = "Delta physical evidence reference"
  }
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.physical-evidence")

  def capture(plan: SparkPlan, spark: SparkSession): SparkPlan = {
    DeltaRangerMergeWrite.forbidConstructionAction()
    DeltaRangerNativeOperation.requireNoRawSelection()
    if (DeltaRangerOrdinaryViews.checkExecution(plan, spark)) return plan
    plan match {
      case result: DeltaRangerMergeResult.Exec        => result.verify(); return result
      case writer: DeltaRangerMergeWrite.Exec         => writer.verify(); return writer
      case metric: DeltaRangerMergeNative.Exec        => metric.verify(); return metric
      case merge: DeltaRangerMergeCommand.Exec        => merge.verify(); return merge
      case source: DeltaRangerMergeSource.SourceExec  => source.owner.before(source); return source
      case helper: DeltaRangerCdfReadScans.HelperExec => helper.verify(); return helper
      case cdf: DeltaRangerCdfProducer => cdf.receipt.beforeTransitions(cdf); return cdf
      case raw: DeltaRangerRawProducer => raw.owner.beforeTransitions(raw.child); return raw
      case _                           =>
    }
    plan match {
      case command: DeltaRangerCdfPropertyExec =>
        val ticket = new CdfTicket(spark)
        ticket.verify(command)
        command.setTagValue(tag, new Reference(ticket))
        return command
      case guard: DeltaRangerAppendInputExec =>
        val ticket = new AppendTicket(spark)
        ticket.verify(guard)
        guard.setTagValue(tag, new Reference(ticket))
        return guard
      case _ =>
    }
    val captured = nodes(plan)
      .flatMap(node =>
        node.getTagValue(tag).map(_.get).collect { case ticket: CapturedTicket => ticket }.toSeq ++
          node.logicalLink.toSeq.flatMap(_.getTagValue(capturedTag).map(_.get))
      )
      .distinct
    if (captured.nonEmpty) {
      if (captured.size != 1) denied("mixed captured recipes")
      captured.head.requireSession(spark)
      captured.head.verify(plan)
      nodes(plan).foreach(_.setTagValue(tag, new Reference(captured.head)))
      return plan
    }
    val inherited = nodes(plan).flatMap(_.getTagValue(tag).map(_.get)).distinct
    if (inherited.nonEmpty) {
      if (inherited.size == 1 && inherited.head.isInstanceOf[TrustedReadTicket]) {
        val read = inherited.head.asInstanceOf[TrustedReadTicket]
        read.requireSession(spark)
        read.validateExecution(plan)
        nodes(plan).foreach(_.setTagValue(tag, new Reference(read)))
        return plan
      }
      if (inherited.size != 1 || !inherited.head.isInstanceOf[NativeTicket])
        denied("mixed native stage evidence")
      inherited.head.requireSession(spark)
      inherited.head.verify(plan)
      nodes(plan).foreach(_.setTagValue(tag, new Reference(inherited.head)))
      return plan
    }
    if (nodes(plan).exists(_.isInstanceOf[QueryStageExec])) denied("missing native stage evidence")
    val logical = nodes(plan).flatMap(_.logicalLink)
    val users = logical.flatMap(node => DeltaRangerNativeScans.evidence(node).map(node -> _))
    val natives = logical.flatMap(node => DeltaRangerPhysicalPlanning.native(node).map(node -> _))
    val ticket: Ticket = if (users.nonEmpty && natives.isEmpty) {
      val authorities = users.map(_._2).distinct
      if (authorities.size != 1) denied("mixed user preparation evidence")
      val prepared = authorities.head
      prepared.owner.requireExecutable()
      if (!prepared.owner.appendInput) {
        val witness =
          if (spark.sessionState.conf.adaptiveExecutionEnabled) None
          else {
            val exact = userWitness(prepared.recoverExecutable(spark), spark, ordinary = true)
            exact.verify(plan)
            Some(exact)
          }
        new TrustedReadTicket(prepared, spark, prepared.readRecipe(spark), witness)
      } else {
        val logical = prepared.recoverExecutable(spark)
        val witness = userWitness(logical, spark)
        witness.verify(plan)
        new UserTicket(witness, prepared, spark)
      }
    } else if (natives.nonEmpty && users.isEmpty) {
      val authorities = natives.map(_._2).distinct
      if (authorities.size != 1) denied("mixed native planning evidence")
      val native = authorities.head
      native.requireSession(spark)
      val logical = native.replay()
      val expected = DeltaRangerPhysicalPlanning.lower(spark, logical)
      val raw = DeltaRangerPhysicalPlanning.raw(spark, logical)
      val ticket = new NativeTicket(expected, raw, native)
      ticket.verify(plan)
      ticket
    } else denied("missing or mixed physical origin")
    nodes(plan).foreach(_.setTagValue(tag, new Reference(ticket)))
    ticket match {
      case read: TrustedReadTicket => bindReadSubqueries(plan, read)
      case _                       =>
    }
    plan
  }

  def finish(plan: SparkPlan, spark: SparkSession = SparkSession.active): SparkPlan = {
    DeltaRangerNativeOperation.requireNoRawSelection()
    if (DeltaRangerOrdinaryViews.checkExecution(plan, spark)) return plan
    plan match {
      case result: DeltaRangerMergeResult.Exec        => result.verify(); return result
      case writer: DeltaRangerMergeWrite.Exec         => writer.verify(); return writer
      case metric: DeltaRangerMergeNative.Exec        => metric.verify(); return metric
      case merge: DeltaRangerMergeCommand.Exec        => merge.verify(); return merge
      case source: DeltaRangerMergeSource.SourceExec  => source.owner.after(source); return source
      case helper: DeltaRangerCdfReadScans.HelperExec => helper.verify(); return helper
      case cdf: DeltaRangerCdfProducer => cdf.receipt.afterTransitions(cdf); return cdf
      case raw: DeltaRangerRawProducer => raw.owner.afterTransitions(raw.child); return raw
      case _                           =>
    }
    val tickets = nodes(plan).flatMap(_.getTagValue(tag).map(_.get)).distinct
    if (tickets.size != 1) denied("missing or mixed physical transition evidence")
    val ticket = tickets.head
    ticket.requireSession(spark)
    ticket.verify(plan)
    ticket match {
      case _: CapturedTicket       => plan
      case read: TrustedReadTicket => read.finish(plan)
      case user: UserTicket =>
        user.prepareInputs(plan)
        DeltaRangerCollectGuardExec(plan)(user)
      case native: NativeTicket =>
        if (
          spark.sessionState.conf.adaptiveExecutionEnabled &&
          DeltaRangerMergeCommit.current.nonEmpty && !native.installMergeBoundary()
        ) return plan
        DeltaRangerRawMetadata.current.foreach { metadata =>
          return DeltaRangerRawMetadataGuardExec(plan)(new RawMetadataContext(native, metadata))
        }
        DeltaRangerAppendCommit.current
          .map(_.metadata)
          .orElse(DeltaRangerCdfPropertyMutation.current.map(_.metadata))
          .orElse(DeltaRangerMergeCommit.current.map(_.metadata)) match {
          case Some(commit) =>
            DeltaRangerCollectGuardExec(plan)(new DeltaRangerCollectValidationContext {
              override def validateCollection(child: SparkPlan): Unit = native.verify(child)
              override def validateExecution(child: SparkPlan): Unit = {
                native.verify(child)
                commit.recordExecution(child)
              }
            })
          case None => plan
        }
      case _: AppendTicket => plan
      case _: CdfTicket    => plan
    }
  }

  /** Explicit bounded walk includes Spark's hidden executable references. */
  def nodes(plan: SparkPlan): Vector[SparkPlan] = {
    val active = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val result = Vector.newBuilder[SparkPlan]
    var count = 0
    def visit(node: SparkPlan): Unit = {
      count += 1
      if (count > 10000 || active.containsKey(node)) denied("cyclic or oversized physical tree")
      active.put(node, true)
      result += node
      node match {
        case adaptive: AdaptiveSparkPlanExec =>
          visit(adaptive.inputPlan)
          visit(adaptive.initialPlan)
        case stage: QueryStageExec      => visit(stage.plan)
        case reused: ReusedExchangeExec => visit(reused.child)
        case _                          => node.children.foreach(visit)
      }
      active.remove(node)
    }
    visit(plan)
    result.result()
  }

  // Spark's aggregate strategy names non-NamedExpression DISTINCT keys while planning.
  // Only these concrete no-function HashAggregate grouping producers may be renamed. The
  // complete physical binding below still checks the producer expression and every consumer;
  // original logical IDs, scan backing and output IDs never enter this map.
  private def generatedGrouping(plan: SparkPlan, logicalIds: Set[ExprId]): Map[ExprId, Int] = {
    val producers = nodes(plan).collect {
      case aggregate: HashAggregateExec
          if aggregate.getClass == classOf[HashAggregateExec] &&
            aggregate.aggregateExpressions.isEmpty && aggregate.aggregateAttributes.isEmpty =>
        aggregate.groupingExpressions.collect {
          case alias: Alias if alias.getClass == classOf[Alias] && !logicalIds(alias.exprId) =>
            alias.exprId
        }
    }.flatten
    if (producers.distinct.size != producers.size) denied("duplicate generated grouping producer")
    producers.zipWithIndex.toMap
  }

  private[ranger] def binding(plan: SparkPlan): Vector[Any] = bindingWithGrouping(plan, Map.empty)

  private def bindingWithGrouping(plan: SparkPlan, grouping: Map[ExprId, Int]): Vector[Any] = {
    def expressionId(id: ExprId): Any = grouping.get(id).map(GeneratedGrouping).getOrElse(id)
    nodes(plan).foreach {
      case _: InMemoryTableScanExec => denied("physical cache substitution")
      case _: AdaptiveSparkPlanExec => denied("nested adaptive subtree")
      case node if node.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])) =>
        denied("executable subquery")
      case _ =>
    }
    // Child plans occur in both constructor fields and children. Store their complete records
    // once per snapshot so deep native plans do not expand exponentially during capture or equals.
    // Structural interning preserves equality of shared and independently copied subtrees.
    val memo = new IdentityHashMap[SparkPlan, PhysicalRef]()
    val records = mutable.ArrayBuffer.empty[Vector[Any]]
    val interned = mutable.HashMap.empty[Vector[Any], PhysicalRef]
    def physical(node: SparkPlan): PhysicalRef = {
      val previous = memo.get(node)
      if (previous != null) return previous
      val result = record(node)
      memo.put(node, result)
      result
    }
    def record(node: SparkPlan): PhysicalRef = node match {
      case stage: ShuffleQueryStageExec if node.getClass == classOf[ShuffleQueryStageExec] =>
        if (
          stage.plan.getClass != classOf[ShuffleExchangeExec] || stage.output != stage.plan.output
        )
          denied("unqualified shuffle stage")
        physical(stage.plan)
      case stage: BroadcastQueryStageExec if node.getClass == classOf[BroadcastQueryStageExec] =>
        if (
          stage.plan.getClass != classOf[BroadcastExchangeExec] || stage.output != stage.plan.output
        )
          denied("unqualified broadcast stage")
        physical(stage.plan)
      case _: QueryStageExec => denied("unknown query stage")
      case read: AQEShuffleReadExec if node.getClass == classOf[AQEShuffleReadExec] =>
        val stage = read.child match {
          case stage: ShuffleQueryStageExec if stage.getClass == classOf[ShuffleQueryStageExec] =>
            stage
          case _ => denied("unqualified adaptive shuffle child")
        }
        if (stage.plan.getClass != classOf[ShuffleExchangeExec])
          denied("unqualified adaptive exchange")
        var next = 0
        read.partitionSpecs.foreach {
          case spec: CoalescedPartitionSpec
              if spec.getClass == classOf[CoalescedPartitionSpec] &&
                spec.startReducerIndex == next && spec.endReducerIndex > next =>
            next = spec.endReducerIndex
          case _ => denied("partial or unknown adaptive shuffle coverage")
        }
        if (next != stage.plan.outputPartitioning.numPartitions)
          denied("incomplete adaptive shuffle coverage")
        physical(stage)

      case codegen: WholeStageCodegenExec if node.getClass == classOf[WholeStageCodegenExec] =>
        physical(codegen.child)
      case adapter: InputAdapter if node.getClass == classOf[InputAdapter] =>
        physical(adapter.child)
      case transition: ColumnarToRowExec if node.getClass == classOf[ColumnarToRowExec] =>
        physical(transition.child)
      case transition: RowToColumnarExec if node.getClass == classOf[RowToColumnarExec] =>
        physical(transition.child)
      case _ =>
        val fields = Vector(
          node.getClass,
          node.productIterator.map(value).toVector,
          node.output.map(value).toVector,
          node.children.map(physical).toVector
        )
        interned.getOrElseUpdate(
          fields, {
            val reference = PhysicalRef(records.size)
            records += fields
            reference
          }
        )
    }
    def value(item: Any): Any = item match {
      case null            => null
      case node: SparkPlan => physical(node)
      case attribute: AttributeReference =>
        Vector(
          attribute.getClass,
          attribute.name,
          expressionId(attribute.exprId),
          attribute.qualifier,
          attribute.dataType.json,
          attribute.nullable,
          attribute.metadata.json
        )
      case alias: Alias if grouping.nonEmpty =>
        Vector(
          alias.getClass,
          alias.productIterator.map(value).toVector,
          expressionId(alias.exprId),
          alias.qualifier.toVector,
          alias.explicitMetadata.map(_.json),
          alias.nonInheritableMetadataKeys.toVector
        )
      case expression: Expression =>
        Vector(expression.getClass, expression.productIterator.map(value).toVector)
      case fs: HadoopFsRelation =>
        Vector(
          new Identity(fs),
          new Identity(fs.location),
          new Identity(fs.fileFormat),
          fs.dataSchema.json,
          fs.partitionSchema.json,
          fs.options
        )
      case rdd: RDD[_]         => new Identity(rdd)
      case data: DataType      => data.json
      case metadata: Metadata  => metadata.json
      case text: UTF8String    => text.getBytes.toVector
      case id: ExprId          => expressionId(id)
      case values: Seq[_]      => values.map(value).toVector
      case optional: Option[_] => optional.map(value)
      case array: Array[_]     => array.map(value).toVector
      case text: String        => text
      case number: java.lang.Double =>
        (classOf[java.lang.Double], java.lang.Double.doubleToRawLongBits(number))
      case number: java.lang.Float =>
        (classOf[java.lang.Float], java.lang.Float.floatToRawIntBits(number))
      case decimal: org.apache.spark.sql.types.Decimal => decimal.toJavaBigDecimal
      case number: java.lang.Byte                      => number
      case number: java.lang.Short                     => number
      case number: java.lang.Integer                   => number
      case number: java.lang.Long                      => number
      case flag: Boolean                               => flag
      case function: Function0[_]                      => new Identity(function)
      case function: Function1[_, _]                   => new Identity(function)
      case function: Function2[_, _, _]                => new Identity(function)
      case product: Product => Vector(product.getClass, product.productIterator.map(value).toVector)
      case other: AnyRef    => new Identity(other)
    }
    val root = physical(plan)
    Vector(root, records.toVector)
  }

  private final case class GeneratedGrouping(position: Int)

  private final case class PhysicalRef(index: Int)

  private final class Identity(private val item: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => item eq that.item
      case _              => false
    }
    override def hashCode(): Int = System.identityHashCode(item)
  }

  // Stock runtime filters create executable subqueries after named logical admission. They
  // consume the enclosing action's captured decisions, including asynchronous AQE callbacks.
  private def bindReadSubqueries(plan: SparkPlan, read: TrustedReadTicket): Unit = {
    nodes(plan).foreach { node =>
      node.expressions.foreach(_.foreach {
        case subquery: ExecSubqueryExpression =>
          val children = nodes(subquery.plan)
          if (children.flatMap(_.getTagValue(tag).map(_.get)).exists(_ ne read))
            denied("mixed runtime-filter origin")
          children.foreach(_.setTagValue(tag, new Reference(read)))
          bindReadSubqueries(subquery.plan, read)
        case _ =>
      })
    }
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta physical provenance rejected: " + reason)
}
