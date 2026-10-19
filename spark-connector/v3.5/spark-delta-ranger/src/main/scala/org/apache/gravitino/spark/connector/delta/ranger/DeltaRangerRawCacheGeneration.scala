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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  Expression,
  ExprId,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel

/** One compilation's immutable source and physical evidence; each copied root stays inert. */
private[ranger] final class DeltaRangerRawCacheGeneration(
    val session: SparkSession,
    val key: LogicalPlan,
    serializer: DeltaRangerRawCacheSerializer.Observation,
    val observation: DeltaRangerRawNativeObservation.Observation,
    private val native: DeltaRangerRawNativeScans,
    private val dependency: Option[DeltaRangerRawProducer],
    prepared: LogicalPlan
) extends DeltaRangerRawMaintenance.Compilation {
  import DeltaRangerRawCacheGeneration._
  private val settings = session.sessionState.conf.getAllConfs
  private val original = DeltaRangerNativeProvenance.binding(key)
  private val logical = new DeltaRangerPhysicalPlanning.LogicalWitness(prepared, session)
  private var prefixBinding: Option[Vector[Any]] = None
  private var coreBinding: Option[Any] = None
  private var finalBinding: Option[Vector[Any]] = None
  @volatile private var winner: Option[(DeltaRangerRawProducer, AnyRef, StorageLevel)] = None
  @volatile private var retired = false

  private def live(): Unit = {
    if (
      retired || settings != session.sessionState.conf.getAllConfs ||
      DeltaRangerNativeProvenance.binding(key) != original
    ) denied("raw compilation changed")
    serializer.requireCurrent(session)
    observation.requireCurrent(session)
    dependency.foreach(p => p.owner.requireInstalled(p))
  }

  override def prepare(plan: LogicalPlan): LogicalPlan = denied(
    "final raw generation already prepared"
  )

  override def plan(child: LogicalPlan): SparkPlan = {
    live()
    val expected = logical.verify(child, session)
    val prefix = DeltaRangerPhysicalPlanning.lower(session, expected)
    safe(prefix)
    val tail = DeltaRangerPhysicalPlanning.tail(session, expected, prefix)
    safe(tail)
    if (prefixBinding.nonEmpty) denied("raw physical compilation repeated")
    prefixBinding = Some(binding(prefix))
    coreBinding = Some(core(prefix))
    finalBinding = Some(binding(tail))
    DeltaRangerRawProducer(prefix)(this)
  }

  def candidate(child: SparkPlan): Unit = {
    if (prefixBinding.isEmpty) denied("unissued raw candidate")
    safe(child)
    // This rejects changed sources and expressions even before Spark's child-first prepare.
    if (!coreBinding.contains(core(child))) denied("raw candidate child changed")
  }

  def beforeTransitions(child: SparkPlan): Unit = {
    candidate(child)
    if (!prefixBinding.contains(binding(child))) denied("raw physical prefix changed")
  }

  def afterTransitions(child: SparkPlan): Unit = candidate(child)

  def requireInstalled(root: DeltaRangerRawProducer): Unit =
    DeltaRangerNativeOperation.withRawSession(
      native,
      () => {
        live()
        candidate(root.child)
        if (!finalBinding.contains(binding(root.child))) denied("incomplete raw physical tail")
        def lookup(): InMemoryRelation = {
          val entry = session.sharedState.cacheManager
            .lookupCachedData(key)
            .getOrElse { retired = winner.nonEmpty; denied("raw candidate is not installed") }
          val builder = entry.cachedRepresentation.cacheBuilder
          winner.foreach { case (installedRoot, installedBuilder, _) =>
            if (
              (entry.plan ne key) || (builder ne installedBuilder) ||
              (builder.cachedPlan ne installedRoot)
            ) retired = true
          }
          if ((entry.plan ne key) || (builder.cachedPlan ne root) || (root.owner ne this))
            denied("exact raw installed winner changed")
          if (
            attributes(entry.cachedRepresentation.output) != attributes(key.output) ||
            attributes(root.output) != attributes(key.output)
          ) denied("installed raw output changed")
          serializer.requireSerializer(session, builder.serializer)
          entry.cachedRepresentation
        }
        val builder = lookup().cacheBuilder
        live()
        if (lookup().cacheBuilder ne builder) denied("raw builder changed during observation")
        synchronized {
          if (retired) denied("retired raw generation")
          winner match {
            case None => winner = Some((root, builder, builder.storageLevel))
            case Some((previous, installed, storage)) =>
              if ((previous ne root) || (installed ne builder) || storage != builder.storageLevel)
                denied("raw generation cannot rebind")
          }
        }
      }
    )
}

private[ranger] object DeltaRangerRawCacheGeneration {
  def validateRecipe(
      key: LogicalPlan,
      plan: LogicalPlan,
      observation: DeltaRangerRawNativeObservation.Observation
  ): Option[DeltaRangerRawProducer] = {
    val original = key.collectLeaves().head.asInstanceOf[LogicalRelation]
    def visit(node: LogicalPlan): Option[DeltaRangerRawProducer] = node match {
      case p: Project if p.getClass == classOf[Project] =>
        val dependency = visit(p.child)
        if (
          !p.projectList
            .forall(a => a.getClass == classOf[AttributeReference] && p.child.output.contains(a))
        )
          denied("raw projection mapping changed")
        dependency
      case r: LogicalRelation
          if r.getClass == classOf[LogicalRelation] && r.catalogTable.isEmpty && !r.isStreaming =>
        if (
          (r.relation ne original.relation) || attributes(r.output) != attributes(original.output)
        )
          denied("raw source changed")
        None
      case cache: InMemoryRelation if cache.getClass == classOf[InMemoryRelation] =>
        val producer = cache.cacheBuilder.cachedPlan match {
          case p: DeltaRangerRawProducer if p.getClass == classOf[DeltaRangerRawProducer] => p
          case _ => denied("foreign raw dependency")
        }
        producer.owner.requireInstalled(producer)
        if (
          (producer.owner.observation.rawIndex ne observation.rawIndex) ||
          attributes(cache.output) != attributes(original.output) ||
          attributes(producer.output) != attributes(original.output)
        )
          denied("raw dependency output mapping changed")
        Some(producer)
      case _ => denied("raw source recipe changed")
    }
    val result = visit(plan)
    if (attributes(plan.output) != attributes(key.output)) denied("raw recipe output changed")
    result
  }

  private final class Identity(val value: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => value eq that.value
      case _              => false
    }
    override def hashCode(): Int = System.identityHashCode(value)
  }
  private def attributes(values: Seq[Attribute]): Vector[Any] = values.map(value).toVector

  private def safe(plan: SparkPlan): Unit = {
    val allowed = Set[Class[_]](
      classOf[FileSourceScanExec],
      classOf[ProjectExec],
      classOf[WholeStageCodegenExec],
      classOf[InputAdapter],
      classOf[ColumnarToRowExec],
      classOf[InMemoryTableScanExec]
    )
    def visit(node: SparkPlan): Unit = {
      if (!allowed(node.getClass)) denied("unsafe raw physical child")
      if (node.expressions.exists(_.exists(_.isInstanceOf[ExecSubqueryExpression])))
        denied("raw executable subquery")
      node match {
        case p: ProjectExec if !p.projectList.forall(_.getClass == classOf[AttributeReference]) =>
          denied("raw physical expression changed")
        case scan: FileSourceScanExec
            if scan.partitionFilters.nonEmpty || scan.dataFilters.nonEmpty =>
          denied("raw physical filter changed")
        case scan: InMemoryTableScanExec if scan.predicates.nonEmpty =>
          denied("raw cached filter changed")
        case _ =>
      }
      node.children.foreach(visit)
    }
    visit(plan)
  }

  private def core(plan: SparkPlan): Any = plan match {
    case node: WholeStageCodegenExec => core(node.child)
    case node: InputAdapter          => core(node.child)
    case node: ColumnarToRowExec     => core(node.child)
    case node =>
      Vector(
        node.getClass,
        node.productIterator.map {
          case child: SparkPlan => core(child)
          case other            => value(other)
        }.toVector,
        attributes(node.output)
      )
  }

  private def binding(plan: SparkPlan): Vector[Any] = Vector(
    plan.getClass,
    plan.productIterator.map(value).toVector,
    attributes(plan.output),
    plan.outputOrdering.map(value).toVector,
    plan match { case node: WholeStageCodegenExec => node.codegenStageId; case _ => 0 }
  )

  private def value(item: Any): Any = item match {
    case null            => null
    case node: SparkPlan => binding(node)
    case a: AttributeReference =>
      Vector(
        a.getClass,
        a.name,
        a.exprId,
        a.qualifier.toVector,
        a.dataType.json,
        a.nullable,
        a.metadata.json
      )
    case e: Expression => Vector(e.getClass, e.productIterator.map(value).toVector)
    case fs: HadoopFsRelation =>
      Vector(
        new Identity(fs),
        new Identity(fs.location),
        new Identity(fs.fileFormat),
        fs.dataSchema.json,
        fs.partitionSchema.json,
        fs.options,
        fs.bucketSpec
      )
    case cache: InMemoryRelation =>
      Vector(
        new Identity(cache.cacheBuilder),
        attributes(cache.output),
        cache.outputOrdering.map(value).toVector
      )
    case id: ExprId               => id
    case t: DataType              => t.json
    case m: Metadata              => m.json
    case xs: Seq[_]               => xs.map(value).toVector
    case xs: Array[_]             => xs.map(value).toVector
    case Some(v)                  => Some(value(v))
    case None                     => None
    case text: String             => text
    case flag: Boolean            => flag
    case number: java.lang.Number => number
    case product: Product => Vector(product.getClass, product.productIterator.map(value).toVector)
    case other: AnyRef    => new Identity(other)
  }
  private def denied(reason: String): Nothing = throw new SecurityException(
    "Raw generation rejected: " + reason
  )
}

/** Visible, non-codegen row root whose copies carry no installed permission. */
private[ranger] final case class DeltaRangerRawProducer(child: SparkPlan)(
    @transient private val generation: DeltaRangerRawCacheGeneration
) extends UnaryExecNode {
  if (generation != null) generation.candidate(child)
  def owner: DeltaRangerRawCacheGeneration = {
    if (generation == null || isCanonicalizedPlan) throw new SecurityException("Inert raw producer")
    generation
  }
  override def output: Seq[Attribute] = child.output
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def supportsColumnar: Boolean = false
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(generation)
  override def executeCollect(): Array[InternalRow] = {
    owner.requireInstalled(this)
    super.executeCollect()
  }
  override protected def doExecute(): RDD[InternalRow] = {
    owner.requireInstalled(this)
    child.execute()
  }
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    owner.requireInstalled(this)
    throw new SecurityException("Raw direct columnar conversion is not qualified")
  }
  override protected def withNewChildInternal(newChild: SparkPlan): DeltaRangerRawProducer =
    DeltaRangerRawProducer(newChild)(generation)
}
