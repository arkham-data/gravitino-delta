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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, IsNotNull}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, HadoopFsRelation}
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, ColumnWithDefaultExprUtils}
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  NamedExpression,
  Literal,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeBatchFileIndex}

/** Owns native row construction, without granting actions while a generator is running. */
private[ranger] final class DeltaRangerMergeWrite(
    receipt: DeltaRangerMergeReceipt,
    transaction: OptimisticTransaction,
    source: DeltaRangerMergeSource.Attempt,
    files: Seq[AddFile],
    condition: Expression,
    clauses: Seq[DeltaMergeIntoClause]
) extends DeltaRangerCdfReadScans.MatchOwner {
  import DeltaRangerMergeCommand.denied
  private val thread = Thread.currentThread()
  private val structure = new DeltaRangerMetadataStructure("Delta MERGE write recipe rejected: ")
  private val sourcePlan = source.logical
  private var targetPlan: LogicalPlan = null
  private var index: TahoeBatchFileIndex = null
  private val manifest = files.map(_.json).toVector
  private var joined: LogicalPlan = null
  private var precomputed: LogicalPlan = null
  private var rewrittenClauses: Seq[DeltaMergeIntoClause] = null
  private var outputColumns: Seq[Column] = null
  private var finalOutput: LogicalPlan = null
  private var recipes = Vector.empty[LogicalPlan]
  private var statistics = Vector.empty[LogicalPlan]
  private var constructing = false
  private var anchor: LogicalPlan = null
  private var anchorBinding: Vector[Any] = null
  private val observations = new IdentityHashMap[LogicalPlan, Vector[Any]]()
  private var writing = false
  private var writerPlan: LogicalPlan = null
  private var writerOriginal: LogicalPlan = null
  private var writerCarrier: DeltaRangerMergeWrite.Carrier = null
  private var writerRoot: DeltaRangerMergeWrite.Exec = null
  private var physicalBinding: Vector[Any] = null
  private var physicalShapes = Set.empty[Vector[Class[_]]]
  private var started = false
  private var matching: DeltaRangerCdfReadScans.Match = null
  private var wrappedIndex: DeltaRangerMergeWrite.OwnedIndex = null
  private var closed = false
  if (DeltaRangerMergeWrite.active.get() != null) denied("nested write phase")
  DeltaRangerMergeWrite.active.set(this)

  private def requireLive(): Unit = {
    if (
      closed || (Thread.currentThread() ne thread) ||
      (DeltaRangerMergeWrite.active.get() ne this)
    ) denied("expired write phase")
    receipt.requireRetainedTransaction(transaction)
    if (!source.ownsRetained(sourcePlan)) denied("changed write checkpoint")
    if (index != null && index.addFiles.map(_.json).toVector != manifest)
      denied("changed native rewrite manifest")
  }
  private def retained: LogicalPlan =
    if (targetPlan == null) sourcePlan
    else Join(sourcePlan, targetPlan, FullOuter, None, JoinHint.NONE)
  private def stages(plan: LogicalPlan): LogicalPlan = {
    val result = structure.analysisStages(plan, receipt.spark)
    recipes ++= result
    result.last
  }
  private def named(expression: Expression, name: String): NamedExpression =
    new Column(expression).as(name).expr.asInstanceOf[NamedExpression]
  def target(plan: LogicalPlan, candidate: TahoeFileIndex): Unit = {
    requireLive()
    if (targetPlan != null) denied("repeated rewrite target")
    index = candidate match {
      case batch: TahoeBatchFileIndex if batch.getClass == classOf[TahoeBatchFileIndex] => batch
      case _ => denied("foreign rewrite index")
    }
    if (
      (index.deltaLog ne receipt.target.log()) || (index.snapshot ne transaction.snapshot) ||
      index.path != receipt.target.log().dataPath || index.actionType != "batch" ||
      index.partitionFiltersGenerated || index.addFiles.map(_.json).toVector != manifest
    )
      denied("foreign rewrite target")
    targetPlan = plan
    recipes :+= plan
  }
  def metric(name: String, expression: Expression): Unit = {
    requireLive()
    if (name == "numSourceRowsInSecondScan") {
      if (targetPlan == null || joined != null) denied("invalid second source scan")
      val projected =
        Project(sourcePlan.output :+ named(expression, "_source_row_present_"), sourcePlan)
      val filtered = Filter(projected.output.last, projected)
      val right =
        Project(targetPlan.output :+ named(Literal.TrueLiteral, "_target_row_present_"), targetPlan)
      joined = stages(Join(filtered, right, FullOuter, Some(condition), JoinHint.NONE))
      recipes ++= Vector(projected, filtered, right, joined)
    }
  }
  private def same(expected: LogicalPlan, candidate: LogicalPlan): Boolean =
    structure.corresponds(expected, candidate, retained)
  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean = {
    requireLive()
    if ((spark ne receipt.spark) || !plan.resolved) return false
    if (matching != null) return matching.analysis(plan)
    if (statistics.exists(DeltaRangerStatisticsStructure.matches(_, plan, retained))) return true
    if (constructing) {
      if (DeltaRangerNativeProvenance.binding(anchor) != anchorBinding)
        denied("native construction input changed")
      def derived(candidate: LogicalPlan): Boolean = {
        if (DeltaRangerNativeProvenance.binding(candidate) == anchorBinding) true
        else
          candidate match {
            case _: Project | _: Generate
                if candidate.children.size == 1 &&
                  !candidate.expressions.exists(_.exists(_.isInstanceOf[SubqueryExpression])) =>
              derived(candidate.children.head)
            case _ => false
          }
      }
      if (!derived(plan)) return false
      observations.put(plan, DeltaRangerNativeProvenance.binding(plan))
      true
    } else recipes.exists(same(_, plan))
  }
  private def construct[T](input: LogicalPlan, frame: T => DataFrame)(body: => T): T = {
    requireLive()
    if (constructing) denied("nested native row construction")
    receipt.requireTransaction(transaction)
    anchor = input
    anchorBinding = DeltaRangerNativeProvenance.binding(input)
    observations.clear()
    constructing = true
    try {
      val result = body
      val returned = frame(result).queryExecution.analyzed
      if (
        !observations.containsKey(returned) ||
        observations.get(returned) != DeltaRangerNativeProvenance.binding(returned)
      )
        denied("unobserved native construction successor")
      recipes :+= returned
      result
    } finally { constructing = false; anchor = null; anchorBinding = null; observations.clear() }
  }
  def precompute(input: DataFrame, original: Seq[DeltaMergeIntoClause])(
      body: => (DataFrame, Seq[DeltaMergeIntoClause])
  ): (DataFrame, Seq[DeltaMergeIntoClause]) = {
    requireLive()
    val plan = input.queryExecution.analyzed
    if (joined == null || precomputed != null || original != clauses || !same(joined, plan))
      denied("foreign native precomputation input")
    val result = construct(plan, (value: (DataFrame, Seq[DeltaMergeIntoClause])) => value._1)(body)
    precomputed = result._1.queryExecution.analyzed
    rewrittenClauses = result._2
    result
  }
  def columns(original: Seq[DeltaMergeIntoClause], cdc: Boolean, columns: Seq[Column]): Unit = {
    requireLive()
    if (precomputed == null || outputColumns != null || original != rewrittenClauses)
      denied("foreign native output generation")
    outputColumns = columns
    if (!cdc)
      finish(stages(Project(columns.map(_.expr.asInstanceOf[NamedExpression]), precomputed)))
  }
  def cdc(input: DataFrame, columns: Seq[Column])(body: => DataFrame): DataFrame = {
    requireLive()
    if (
      precomputed == null || outputColumns == null || columns != outputColumns ||
      (input.queryExecution.analyzed ne precomputed) || finalOutput != null
    )
      denied("foreign native CDC generation")
    val result = construct(precomputed, (frame: DataFrame) => frame)(body)
    finish(result.queryExecution.analyzed)
    result
  }
  private def finish(preOutput: LogicalPlan): Unit = {
    val filtered = stages(
      Filter(new Column(UnresolvedAttribute("_row_dropped_")).equalTo(false).expr, preOutput)
    )
    finalOutput = stages(Project(filtered.output.filterNot(_.name == "_row_dropped_"), filtered))
  }
  def output(frame: DataFrame): Unit = {
    requireLive()
    if (constructing || finalOutput == null || !same(finalOutput, frame.queryExecution.analyzed))
      denied("foreign native write output")
    receipt.requireTransaction(transaction)
  }
  def write[T](frame: DataFrame)(body: => T): T = {
    output(frame)
    if (writing || writerPlan != null) denied("repeated native writer")
    if (
      transaction.metadata.partitionColumns.nonEmpty ||
      transaction.metadata.columnMappingMode.name != "none" ||
      ColumnWithDefaultExprUtils.tableHasDefaultExpr(
        transaction.protocol,
        transaction.metadata,
        false
      ) ||
      Constraints.getAll(transaction.metadata, receipt.spark).nonEmpty
    )
      denied("unsupported native writer profile")
    val original = frame.queryExecution.analyzed
    val cdc = CDCReader.isCDCEnabledOnTable(transaction.metadata, receipt.spark)
    writerPlan =
      if (cdc)
        stages(
          Project(
            original.output :+
              named(
                IsNotNull(UnresolvedAttribute(CDCReader.CDC_TYPE_COLUMN_NAME)),
                CDCReader.CDC_PARTITION_COL
              ),
            original
          )
        )
      else original
    val statsOutput = writerPlan.output
      .map(_.withNullability(true))
      .filterNot(_.name == CDCReader.CDC_PARTITION_COL)
    val inputStatsSchema = StructType(
      statsOutput.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata))
    )
    val collection = new StatisticsCollection {
      override val columnMappingMode: DeltaColumnMappingMode =
        transaction.metadata.columnMappingMode
      override def tableSchema: StructType = transaction.metadata.schema
      override def outputTableStatsSchema: StructType =
        if (
          receipt.spark.sessionState.conf.getConf(
            DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA
          )
        ) tableSchema
        else inputStatsSchema
      override def outputAttributeSchema: StructType = inputStatsSchema
      override val spark: SparkSession = receipt.spark
      override val statsColumnSpec =
        StatisticsCollection.configuredDeltaStatsColumnSpec(transaction.metadata)
      override val protocol: Protocol = transaction.protocol
    }
    val local = LocalRelation(statsOutput)
    statistics = Vector(local) ++ structure.analysisStages(
      Project(
        Seq(UnresolvedAlias(to_json(collection.statsCollector).expr, Some(e => toPrettySQL(e)))),
        local
      ),
      receipt.spark
    )
    writing = true
    try body
    finally writing = false
  }
  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] = {
    requireLive()
    forbidAction()
    if (spark ne receipt.spark) denied("foreign writer session")
    if (matching != null) return matching.normalize(plan)
    if (!writing || writerPlan == null || !same(writerPlan, plan)) return None
    if (writerCarrier == null) {
      writerOriginal = plan
      writerCarrier = DeltaRangerMergeWrite.Carrier(plan.output)(this)
    } else if (writerOriginal ne plan) denied("replaced native writer query")
    Some(writerCarrier)
  }
  def check(candidate: DeltaRangerMergeWrite.Carrier): Unit = {
    requireLive()
    if (
      !writing || constructing || (candidate ne writerCarrier) || writerOriginal == null ||
      !same(writerPlan, writerOriginal)
    ) denied("foreign writer carrier")
  }
  private def shape(plan: SparkPlan): Vector[Class[_]] =
    DeltaRangerPhysicalProvenance.nodes(plan).map(_.getClass)
  def plan(candidate: DeltaRangerMergeWrite.Carrier): SparkPlan = {
    check(candidate)
    if (writerRoot != null) denied("writer planned twice")
    wrappedIndex = new DeltaRangerMergeWrite.OwnedIndex(this, index, receipt.spark)
    var replacements = 0
    val logical = org.apache.spark.sql.catalyst.analysis
      .EliminateSubqueryAliases(writerOriginal)
      .transformDown { case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation if fs.location eq index =>
            replacements += 1
            relation.copy(relation = fs.copy(location = wrappedIndex)(receipt.spark))
          case _ => denied("foreign writer target relation")
        }
      }
    if (replacements != 1) denied("writer target coverage changed")
    val child = DeltaRangerPhysicalPlanning.lower(receipt.spark, logical)
    physicalBinding = DeltaRangerPhysicalProvenance.binding(child)
    physicalShapes += shape(child)
    physicalShapes += shape(
      org.apache.spark.sql.execution
        .ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false)(child.clone())
    )
    physicalShapes += shape(DeltaRangerPhysicalPlanning.tail(receipt.spark, logical, child))
    writerRoot = DeltaRangerMergeWrite.Exec(child)(this)
    writerRoot
  }
  def verify(candidate: DeltaRangerMergeWrite.Exec): Unit = {
    check(writerCarrier)
    if (
      (candidate ne writerRoot) || physicalBinding == null || !physicalShapes(
        shape(candidate.child)
      ) ||
      DeltaRangerPhysicalProvenance.binding(candidate.child) != physicalBinding
    )
      denied("writer physical substitution")
  }
  def replace(
      previous: DeltaRangerMergeWrite.Exec,
      child: SparkPlan
  ): DeltaRangerMergeWrite.Exec = {
    verify(previous)
    if (previous.child eq child) return previous
    if (
      started || !physicalShapes(shape(child)) || DeltaRangerPhysicalProvenance.binding(
        child
      ) != physicalBinding
    )
      denied("writer transition changed")
    writerRoot = DeltaRangerMergeWrite.Exec(child)(this)
    writerRoot
  }
  def execute(candidate: DeltaRangerMergeWrite.Exec): org.apache.spark.rdd.RDD[InternalRow] = {
    verify(candidate)
    receipt.requireTransaction(transaction)
    DeltaRangerPhysicalProvenance.nodes(candidate.child).foreach {
      case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
        requireLive(); scan.inputRDD; requireLive()
      case _ =>
    }
    started = true
    val result = candidate.child.execute()
    requireLive()
    result
  }
  override def requireHelper(candidate: DeltaRangerCdfReadScans.Match): Unit = {
    requireLive()
    if (!writing || (matching ne candidate)) denied("foreign writer manifest helper")
  }
  def matchingFiles(
      candidate: DeltaRangerMergeWrite.OwnedIndex,
      partitions: Seq[Expression],
      data: Seq[Expression]
  ): Seq[AddFile] = {
    requireLive()
    if (!writing || (candidate ne wrappedIndex) || matching != null)
      denied("foreign writer manifest")
    partitions.foreach(DeltaRangerPlanAdmission.expression)
    data.foreach(DeltaRangerPlanAdmission.expression)
    matching =
      new DeltaRangerCdfReadScans.Match(this, index, files.toVector, partitions, receipt.spark)
    try {
      val result = index.matchingFiles(partitions, data)
      matching.requireReturned(result)
      result
    } finally matching = null
  }
  def forbidAction(): Unit = if (constructing) denied("action during native row construction")
  def close(): Unit = { closed = true; DeltaRangerMergeWrite.active.remove() }
}
private[ranger] object DeltaRangerMergeWrite {
  final case class Carrier(override val output: Seq[Attribute])(
      @transient private val authority: DeltaRangerMergeWrite
  ) extends LeafNode {
    def owner: DeltaRangerMergeWrite = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert writer carrier")
      authority.check(this); authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): LogicalPlan = this
    override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(Long.MaxValue))
  }
  final case class Exec(child: SparkPlan)(@transient private val authority: DeltaRangerMergeWrite)
      extends UnaryExecNode {
    def verify(): Unit = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert writer execution")
      authority.verify(this)
    }
    override def output = child.output
    override def outputPartitioning = child.outputPartitioning
    override def outputOrdering = child.outputOrdering
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): SparkPlan = this
    override def executeCollect(): Array[InternalRow] =
      DeltaRangerMergeCommand.denied("writer payload collect")
    override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] =
      authority.execute(this)
    override protected def withNewChildInternal(child: SparkPlan): Exec =
      authority.replace(this, child)
  }
  final class OwnedIndex(
      owner: DeltaRangerMergeWrite,
      original: TahoeBatchFileIndex,
      spark: SparkSession
  ) extends TahoeFileIndexWithSnapshotDescriptor(
        spark,
        original.deltaLog,
        original.path,
        original.snapshot
      ) {
    override def partitionSchema = original.partitionSchema
    override def inputFiles: Array[String] = original.inputFiles
    override def sizeInBytes: Long = original.sizeInBytes
    override def refresh(): Unit = ()
    override def matchingFiles(partitions: Seq[Expression], data: Seq[Expression]): Seq[AddFile] =
      owner.matchingFiles(this, partitions, data)
  }
  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] =
    Option(active.get()).flatMap(_.normalize(plan, spark))
  private val active = new ThreadLocal[DeltaRangerMergeWrite]()
  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean =
    DeltaRangerNativeOperation.current() == null && Option(active.get())
      .exists(_.analysis(plan, spark))
  def forbidConstructionAction(): Unit = Option(active.get()).foreach(_.forbidAction())
}
