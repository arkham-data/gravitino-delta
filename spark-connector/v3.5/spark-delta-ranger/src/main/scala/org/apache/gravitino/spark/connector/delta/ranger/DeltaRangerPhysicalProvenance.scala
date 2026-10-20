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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExprId}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec,
  ShuffleQueryStageExec,
  BroadcastQueryStageExec,
  AQEShuffleReadExec
}
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
      alternatives: Vector[SparkPlan] = Vector.empty
  ) {
    private val recipes = expected +: alternatives
    private val frozen = recipes.map(binding)
    def verify(candidate: SparkPlan): Unit = {
      if (recipes.map(binding) != frozen) denied("sealed physical recipe changed")
      if (!frozen.contains(binding(candidate))) {
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
    val witness = userWitness(expectedLogical, spark)
    witness.verify(plan)
    witness
  }

  private def userWitness(
      logical: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: SparkSession
  ): Witness = {
    val expected = DeltaRangerPhysicalPlanning.lower(spark, logical)
    val tail = DeltaRangerPhysicalPlanning.tail(spark, logical, expected)
    new Witness(expected, Vector(tail))
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
    DeltaRangerNativeOperation.requireNoRawSelection()
    plan match {
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
    val inherited = nodes(plan).flatMap(_.getTagValue(tag).map(_.get)).distinct
    if (inherited.nonEmpty) {
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
      val logical = prepared.recoverExecutable(spark)
      val witness = userWitness(logical, spark)
      witness.verify(plan)
      new UserTicket(witness, prepared, spark)
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
    plan
  }

  def finish(plan: SparkPlan, spark: SparkSession = SparkSession.active): SparkPlan = {
    DeltaRangerNativeOperation.requireNoRawSelection()
    plan match {
      case raw: DeltaRangerRawProducer => raw.owner.afterTransitions(raw.child); return raw
      case _                           =>
    }
    val tickets = nodes(plan).flatMap(_.getTagValue(tag).map(_.get)).distinct
    if (tickets.size != 1) denied("missing or mixed physical transition evidence")
    val ticket = tickets.head
    ticket.requireSession(spark)
    ticket.verify(plan)
    ticket match {
      case user: UserTicket =>
        user.prepareInputs(plan)
        DeltaRangerCollectGuardExec(plan)(user)
      case native: NativeTicket =>
        DeltaRangerRawMetadata.current.foreach { metadata =>
          return DeltaRangerRawMetadataGuardExec(plan)(new RawMetadataContext(native, metadata))
        }
        DeltaRangerAppendCommit.current
          .map(_.metadata)
          .orElse(DeltaRangerCdfPropertyMutation.current.map(_.metadata)) match {
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

  private def binding(plan: SparkPlan): Vector[Any] = {
    nodes(plan).foreach {
      case _: InMemoryTableScanExec => denied("physical cache substitution")
      case _: AdaptiveSparkPlanExec => denied("nested adaptive subtree")
      case node if node.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])) =>
        denied("executable subquery")
      case _ =>
    }
    def physical(node: SparkPlan): Vector[Any] = node match {
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
        Vector(
          node.getClass,
          node.productIterator.map(value).toVector,
          node.output.map(value).toVector,
          node.children.map(physical).toVector
        )
    }
    def value(item: Any): Any = item match {
      case null            => null
      case node: SparkPlan => physical(node)
      case attribute: AttributeReference =>
        Vector(
          attribute.getClass,
          attribute.name,
          attribute.exprId,
          attribute.qualifier,
          attribute.dataType.json,
          attribute.nullable,
          attribute.metadata.json
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
      case id: ExprId          => id
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
    physical(plan)
  }

  private final class Identity(private val item: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => item eq that.item
      case _              => false
    }
    override def hashCode(): Int = System.identityHashCode(item)
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta physical provenance rejected: " + reason)
}
