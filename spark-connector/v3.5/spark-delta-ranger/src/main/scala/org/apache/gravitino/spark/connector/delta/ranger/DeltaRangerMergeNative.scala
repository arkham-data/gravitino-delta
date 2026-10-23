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

import java.util.Collections
import java.util.function.Predicate
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, coalesce, count, lit, sum}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, ScalaUDF}
import org.apache.spark.sql.delta.DeltaUDF
import org.apache.spark.sql.delta.commands.merge.ClassicMergeExecutor
import org.apache.spark.sql.delta.util.SetAccumulator
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{LogicalRelation, HadoopFsRelation}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  InputFileName,
  MonotonicallyIncreasingID
}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, JoinHint, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}

/** Finite metadata prefix of one inherited native file-selection call. */
private[ranger] final class DeltaRangerMergeNative(
    receipt: DeltaRangerMergeReceipt,
    transaction: OptimisticTransaction,
    source: DeltaRangerMergeSource.Attempt,
    condition: Expression,
    matchedPredicate: Expression
) extends DeltaRangerNativeOperation.ScanOwner
    with DeltaRangerCdfReadScans.MatchOwner {
  import DeltaRangerMergeCommand.denied
  private val thread = Thread.currentThread()
  if (DeltaRangerMergeNative.active.get() != null) denied("nested native selection")
  DeltaRangerMergeNative.active.set(this)
  private val structure = new DeltaRangerMetadataStructure("Delta MERGE native helper rejected: ")
  private val sourcePlan = source.logical
  private var recipes = Vector.empty[LogicalPlan]
  private var countedSource: LogicalPlan = null
  private var nativeTarget: LogicalPlan = null
  private var joined: LogicalPlan = null
  private var metricQuery: LogicalPlan = null
  private var touchedAccumulator: SetAccumulator[String] = null
  private var targetIndex: TahoeBatchFileIndex = null
  private var targetFiles = Vector.empty[String]
  private var matching: DeltaRangerCdfReadScans.Match = null
  private var wrappedIndex: DeltaRangerMergeNative.OwnedIndex = null
  private var metricAnalyzed: LogicalPlan = null
  private val metricEncoder =
    org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[(Long, Long)]()
  private var metricOriginal: LogicalPlan = null
  private var metricCarrier: DeltaRangerMergeNative.Carrier = null
  private var metricRoot: DeltaRangerMergeNative.Exec = null
  private var metricPhysical: Vector[Any] = null
  private var metricShapes = Set.empty[Vector[Class[_]]]
  private var metricStarted = false
  private var metricCollected = false
  private var closed = false
  private var opened = false
  private var metadata: DeltaRangerNativeOperation = null
  override def session(): SparkSession = receipt.spark
  override def bindings() = Collections.singletonList(receipt.target)
  override def requireLive(): Unit = {
    if (
      closed || (Thread.currentThread() ne thread) || (DeltaRangerMergeNative.active.get() ne this)
    ) denied("expired native selection")
    receipt.requireRetainedTransaction(transaction)
  }
  def sourceMetric(expression: Expression): Unit = {
    requireLive()
    if (metadata != null || countedSource != null) denied("invalid source metric order")
    val projected =
      Project(sourcePlan.output :+ named(expression, "_source_row_present_"), sourcePlan)
    val filtered = Filter(projected.output.last, projected)
    countedSource = Project(sourcePlan.output, filtered)
    recipes = Vector(sourcePlan, projected, filtered, countedSource)
  }
  def targetPlan(plan: LogicalPlan, index: TahoeFileIndex): Unit = {
    requireLive()
    if (metadata != null || countedSource == null || nativeTarget != null)
      denied("invalid native target construction order")
    targetIndex = index match {
      case batch: TahoeBatchFileIndex if batch.getClass == classOf[TahoeBatchFileIndex] => batch
      case _ => denied("non-native target index")
    }
    if (
      (targetIndex.deltaLog ne receipt.target.log()) ||
      (targetIndex.snapshot ne transaction.snapshot) ||
      targetIndex.path != receipt.target.log().dataPath || targetIndex.actionType != "batch" ||
      targetIndex.partitionFiltersGenerated
    )
      denied("foreign native target index")
    nativeTarget = plan
    targetFiles = targetIndex.addFiles.map(_.json).toVector
    val rowIds = Project(plan.output :+ named(MonotonicallyIncreasingID(), "_row_id_"), plan)
    val fileNames = Project(rowIds.output :+ named(InputFileName(), "_file_name_"), rowIds)
    val unresolvedRecipe = Join(countedSource, fileNames, Inner, Some(condition), JoinHint.NONE)
    joined = analyzed(unresolvedRecipe)
    recipes ++= Vector(plan, rowIds, fileNames, joined)
  }
  private def retained: LogicalPlan =
    if (nativeTarget == null) sourcePlan
    else Join(sourcePlan, nativeTarget, Inner, None, JoinHint.NONE)
  def analysis(plan: LogicalPlan, session: SparkSession): Boolean = {
    requireLive()
    if ((session ne receipt.spark) || metadata != null || !plan.resolved) return false
    if (matching != null) return matching.analysis(plan)
    if (
      metricAnalyzed != null && structure.encoderBinding(plan, metricEncoder, metricAnalyzed.output)
    )
      return true
    plan match {
      case carrier: DeltaRangerMergeNative.Carrier => check(carrier); return true
      case _                                       =>
    }
    if (targetIndex != null && targetIndex.addFiles.map(_.json).toVector != targetFiles)
      denied("native target files changed")
    if (!source.owns(sourcePlan)) denied("native source ownership changed")
    if (acceptRecipe(plan)) return true
    if (joined != null && metricQuery == null) captureMetrics(plan)
    acceptRecipe(plan)
  }
  private def acceptRecipe(plan: LogicalPlan): Boolean = {
    val valid = recipes.exists(expected => structure.corresponds(expected, plan, retained))
    if (valid && metricQuery != null && structure.corresponds(metricQuery, plan, retained)) {
      if (metricAnalyzed == null) metricAnalyzed = plan
      else if (metricAnalyzed.output != plan.output) denied("metric output identity changed")
    }
    valid
  }
  private def named(expression: Expression, name: String): NamedExpression =
    new Column(expression).as(name).expr.asInstanceOf[NamedExpression]
  private def analyzed(plan: LogicalPlan): LogicalPlan = {
    val stages = structure.analysisStages(plan, receipt.spark)
    recipes ++= stages
    stages.last
  }
  private def captureMetrics(plan: LogicalPlan): Unit = plan match {
    case projected: Project if structure.corresponds(joined, projected.child, retained) =>
      val functions = projected.projectList.flatMap(_.collect { case udf: ScalaUDF => udf })
      if (functions.size != 1) return
      val function = functions.head.function.asInstanceOf[AnyRef]
      val descriptor = structure.lambda(
        function,
        classOf[ClassicMergeExecutor],
        "org/apache/spark/sql/delta/commands/merge/ClassicMergeExecutor",
        "scala/Function2",
        "$anonfun$findTouchedFiles$10$adapted",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
        "(Lorg/apache/spark/sql/delta/util/SetAccumulator;Ljava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;",
        "(Ljava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;",
        1
      )
      descriptor.getCapturedArg(0) match {
        case accumulator: SetAccumulator[_]
            if accumulator.getClass == classOf[SetAccumulator[_]] &&
              accumulator.name.contains("internal.metrics.MergeIntoDelta.touchedFiles") &&
              accumulator.value.isEmpty =>
          touchedAccumulator = accumulator.asInstanceOf[SetAccumulator[String]]
        case _ => denied("foreign touched-files accumulator")
      }
      val touched = DeltaUDF
        .intFromStringBoolean(function.asInstanceOf[(String, Boolean) => Int])
        .asNondeterministic()
      val touchedExpression =
        touched(col("_file_name_"), new Column(matchedPredicate)).as("one").expr
      val output = analyzed(
        Project(
          Seq(
            UnresolvedAttribute("_row_id_"),
            touchedExpression.asInstanceOf[NamedExpression]
          ),
          joined
        )
      )
      // Native duplicate-count metadata is reconstructed; row/action generation remains native.
      if (!recipes.exists(structure.corresponds(_, projected, retained))) return
      val grouped = analyzed(
        org.apache.spark.sql.catalyst.plans.logical.Aggregate(
          Seq(UnresolvedAttribute("_row_id_")),
          Seq(
            UnresolvedAttribute("_row_id_"),
            sum(col("one")).as("count").expr.asInstanceOf[NamedExpression]
          ),
          output
        )
      )
      val filtered = analyzed(Filter((col("count") > lit(1)).expr, grouped))
      metricQuery = analyzed(
        Project(
          Seq(
            UnresolvedAlias(
              coalesce(count(col("*")), lit(0)).expr,
              Some(e => org.apache.spark.sql.catalyst.util.toPrettySQL(e))
            ),
            UnresolvedAlias(
              coalesce(sum(col("count")), lit(0)).expr,
              Some(e => org.apache.spark.sql.catalyst.util.toPrettySQL(e))
            )
          ),
          filtered
        )
      )
      recipes ++= Vector(output, grouped, filtered, metricQuery)
    case _ =>
  }
  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] = {
    requireLive()
    if (spark ne receipt.spark) denied("foreign helper session")
    if (matching != null) return matching.normalize(plan)
    if (metricQuery == null || !structure.corresponds(metricQuery, plan, retained)) return None
    if (metricCarrier == null) {
      metricOriginal = plan
      metricCarrier = DeltaRangerMergeNative.Carrier(plan.output)(this)
    } else if (metricOriginal ne plan) denied("replaced metric query")
    Some(metricCarrier)
  }
  def check(candidate: DeltaRangerMergeNative.Carrier): Unit = {
    requireLive()
    if (
      (candidate ne metricCarrier) || metricOriginal == null ||
      !structure.corresponds(metricQuery, metricOriginal, retained)
    ) denied("foreign metric carrier")
    if (!source.owns(sourcePlan)) denied("foreign metric source")
  }
  private def shape(plan: SparkPlan): Vector[Class[_]] =
    DeltaRangerPhysicalProvenance.nodes(plan).map(_.getClass)
  def plan(candidate: DeltaRangerMergeNative.Carrier): SparkPlan = {
    check(candidate)
    if (metricRoot != null) denied("metric helper planned twice")
    wrappedIndex = new DeltaRangerMergeNative.OwnedIndex(this, targetIndex, receipt.spark)
    var replacements = 0
    val logical = org.apache.spark.sql.catalyst.analysis
      .EliminateSubqueryAliases(metricOriginal)
      .transformDown { case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation if fs.location eq targetIndex =>
            replacements += 1
            relation.copy(relation = fs.copy(location = wrappedIndex)(receipt.spark))
          case _ => denied("foreign metric target relation")
        }
      }
    if (replacements != 1) denied("metric target coverage changed")
    val child = DeltaRangerPhysicalPlanning.lower(receipt.spark, logical)
    metricPhysical = DeltaRangerPhysicalProvenance.binding(child)
    metricShapes += shape(child)
    metricShapes += shape(
      org.apache.spark.sql.execution
        .ApplyColumnarRulesAndInsertTransitions(Nil, outputsColumnar = false)(child.clone())
    )
    metricShapes += shape(DeltaRangerPhysicalPlanning.tail(receipt.spark, logical, child))
    metricRoot = DeltaRangerMergeNative.Exec(child)(this)
    metricRoot
  }
  def verify(candidate: DeltaRangerMergeNative.Exec): Unit = {
    check(metricCarrier)
    if (
      (candidate ne metricRoot) || metricPhysical == null ||
      !metricShapes(shape(candidate.child)) ||
      DeltaRangerPhysicalProvenance.binding(candidate.child) != metricPhysical
    )
      denied("metric physical substitution")
  }
  def replace(
      previous: DeltaRangerMergeNative.Exec,
      child: SparkPlan
  ): DeltaRangerMergeNative.Exec = {
    verify(previous)
    if (child eq previous.child) return previous
    if (
      metricStarted || !metricShapes(shape(child)) ||
      DeltaRangerPhysicalProvenance.binding(child) != metricPhysical
    )
      denied("metric transition changed")
    metricRoot = DeltaRangerMergeNative.Exec(child)(this)
    metricRoot
  }
  def begin(candidate: DeltaRangerMergeNative.Exec): Unit = {
    verify(candidate)
    if (metricCollected) denied("metric action repeated")
    receipt.requireTransaction(transaction)
    // Resolve lazy file-list metadata under the issuing driver before Spark starts broadcast
    // workers. This constructs executor RDDs; it does not collect target or source payload here.
    DeltaRangerPhysicalProvenance.nodes(candidate.child).foreach {
      case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
        requireLive()
        scan.inputRDD
        requireLive()
      case _ =>
    }
    metricStarted = true
  }
  def collected(candidate: DeltaRangerMergeNative.Exec, rows: Array[InternalRow]): Unit = {
    verify(candidate)
    if (
      !metricStarted || metricCollected || rows.length != 1 || rows.head.numFields != 2 ||
      rows.head.isNullAt(0) || rows.head.isNullAt(1) ||
      rows.head.getLong(0) < 0 || rows.head.getLong(1) < 0
    )
      denied("invalid native metric result")
    receipt.requireTransaction(transaction)
    metricCollected = true
  }
  override def requireHelper(candidate: DeltaRangerCdfReadScans.Match): Unit = {
    requireLive()
    if (matching ne candidate) denied("foreign target manifest helper")
  }
  def matchingFiles(
      index: DeltaRangerMergeNative.OwnedIndex,
      partitions: Seq[Expression],
      data: Seq[Expression]
  ): Seq[AddFile] = {
    requireLive()
    if (
      (index ne wrappedIndex) || matching != null ||
      targetIndex.addFiles.map(_.json).toVector != targetFiles
    )
      denied("foreign target manifest")
    partitions.foreach(DeltaRangerPlanAdmission.expression)
    data.foreach(DeltaRangerPlanAdmission.expression)
    matching = new DeltaRangerCdfReadScans.Match(
      this,
      targetIndex,
      targetIndex.addFiles.toVector,
      partitions,
      receipt.spark
    )
    try {
      val files = targetIndex.matchingFiles(partitions, data)
      matching.requireReturned(files)
      files
    } finally matching = null
  }
  def returned(files: Seq[AddFile], resolve: (Seq[String], Seq[AddFile]) => Seq[AddFile]): Unit = {
    requireLive()
    if (
      !metricCollected || touchedAccumulator == null || files
        .map(_.json)
        .distinct
        .size != files.size ||
      !files.forall(file => targetFiles.contains(file.json))
    )
      denied("unqualified native selection result")
    val expected = resolve(touchedAccumulator.value.asScala.toSeq, targetIndex.addFiles)
    if (files.map(_.json).toSet != expected.map(_.json).toSet)
      denied("native selected files differ from the retained accumulator")
    receipt.requireTransaction(transaction)
  }
  override def freezeCache(actual: LogicalRDD): Predicate[LogicalRDD] = {
    requireLive()
    if (!DeltaRangerNativeOperation.stockCacheWrapper(actual, receipt.spark))
      denied("non-native target metadata cache")
    val frozen = DeltaRangerNativeProvenance.binding(actual)
    (candidate: LogicalRDD) => {
      requireLive()
      (candidate.rdd eq actual.rdd) &&
      DeltaRangerNativeProvenance.binding(actual) == frozen &&
      DeltaRangerNativeProvenance.binding(candidate) == frozen
    }
  }
  def beforeFileSelection(): Unit = {
    requireLive()
    if (opened || metadata != null) denied("repeated selection metadata prefix")
    opened = true
    metadata = DeltaRangerNativeOperation.prepare(this)
    metadata.capture(receipt.target.index(), transaction.snapshot)
    // Filtered file selection need not compute the snapshot summary itself. Native selection
    // reads these metrics later, so initialize the exact captured summary inside this prefix.
    transaction.snapshot.numOfFiles
    transaction.snapshot.sizeInBytes
  }
  def afterFileSelection(): Unit = {
    requireLive()
    if (!opened || metadata == null) denied("missing selection metadata prefix")
    metadata.succeed()
    metadata.close()
    metadata = null
    receipt.requireTransaction(transaction)
  }
  def close(): Unit = {
    try { if (metadata != null) metadata.close() }
    finally { metadata = null; closed = true; DeltaRangerMergeNative.active.remove() }
  }
}

private[ranger] object DeltaRangerMergeNative {
  final case class Carrier(override val output: Seq[Attribute])(
      @transient private val authority: DeltaRangerMergeNative
  ) extends LeafNode {
    def owner: DeltaRangerMergeNative = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert metric carrier")
      authority.check(this); authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): LogicalPlan = this
    override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(16))
  }
  final case class Exec(child: SparkPlan)(@transient private val authority: DeltaRangerMergeNative)
      extends UnaryExecNode {
    def verify(): Unit = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert metric execution")
      authority.verify(this)
    }
    override def output = child.output
    override def outputPartitioning = child.outputPartitioning
    override def outputOrdering = child.outputOrdering
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): SparkPlan = this
    override def executeCollect(): Array[InternalRow] = {
      authority.begin(this)
      val rows = super.executeCollect()
      authority.collected(this, rows)
      rows
    }
    override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = {
      authority.begin(this); child.execute()
    }
    override protected def withNewChildInternal(child: SparkPlan): Exec =
      authority.replace(this, child)
  }
  final class OwnedIndex(
      owner: DeltaRangerMergeNative,
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
    if (DeltaRangerNativeOperation.current() != null) None
    else Option(active.get()).flatMap(_.normalize(plan, spark))
  private val active = new ThreadLocal[DeltaRangerMergeNative]()
  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean =
    DeltaRangerNativeOperation.current() == null && Option(active.get())
      .exists(_.analysis(plan, spark))
}
