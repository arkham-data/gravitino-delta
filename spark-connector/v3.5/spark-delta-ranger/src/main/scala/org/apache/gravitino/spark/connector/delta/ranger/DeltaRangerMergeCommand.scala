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

import org.apache.spark.sql.{Row, SparkSession, DataFrame, Column}
import org.apache.spark.sql.catalyst.plans.logical.{
  DeltaMergeInto,
  DeltaMergeIntoClause,
  LogicalPlan,
  MergeIntoTable
}
import org.apache.spark.sql.delta.{DeltaAnalysis, PreprocessTableMerge}
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  Literal,
  NamedExpression,
  SubqueryExpression
}
import org.apache.spark.sql.delta.commands.{MergeIntoCommand, MergeIntoCommandBase}
import org.apache.spark.sql.delta.commands.merge.{
  ClassicMergeExecutor,
  InsertOnlyMergeExecutor,
  DeduplicateCDFDeletes
}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.actions.FileAction

/** One exact preprocessed SQL command, retaining authored demands separately from native defaults.
  */
private[ranger] final case class DeltaRangerMergeCommand private (
    @transient private val native: MergeIntoCommand
)(@transient private val authority: DeltaRangerMergeReceipt)
    extends MergeIntoCommandBase
    with ClassicMergeExecutor
    with InsertOnlyMergeExecutor {
  private var nativePhase: DeltaRangerMergeNative = null
  private var writePhase: DeltaRangerMergeWrite = null
  override val source = native.source
  override val target = native.target
  override val targetFileIndex = native.targetFileIndex
  override val condition = native.condition
  // MergeIntoCommandBase initializes its unconditional-delete flag from this abstract val.
  // Lazy initialization exposes the exact native clauses during trait initialization too.
  override lazy val matchedClauses = native.matchedClauses
  override val notMatchedClauses = native.notMatchedClauses
  override val notMatchedBySourceClauses = native.notMatchedBySourceClauses
  override val migratedSchema = native.migratedSchema
  override val schemaEvolutionEnabled = false
  override val output = native.output
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
  override def clone(): LogicalPlan = this

  def receipt: DeltaRangerMergeReceipt = {
    if (authority == null || isCanonicalizedPlan) DeltaRangerMergeCommand.denied("inert command")
    authority.requireCommand(this)
    authority
  }
  override def run(spark: SparkSession): Seq[Row] = {
    val owner = receipt
    owner.begin(this)
    try super.run(spark)
    finally owner.end()
  }
  override protected def getTargetOnlyPredicates(spark: SparkSession): Seq[Expression] = {
    val result = super.getTargetOnlyPredicates(spark)
    if (nativePhase == null) DeltaRangerMergeCommand.denied("unowned target selection")
    nativePhase.beforeFileSelection()
    result
  }
  override protected def incrementMetricAndReturnBool(
      name: String,
      valueToReturn: Boolean
  ): Expression = {
    if (name == "numSourceRows") {
      if (nativePhase == null || !valueToReturn)
        DeltaRangerMergeCommand.denied("unowned selection metric")
      // Pinned ClassicMergeExecutor calls this immediately after filterFiles returns,
      // before constructing any source/target payload helper plans.
      nativePhase.afterFileSelection()
    }
    val result = super.incrementMetricAndReturnBool(name, valueToReturn)
    if (name == "numSourceRows") nativePhase.sourceMetric(result)
    if (writePhase != null) writePhase.metric(name, result)
    result
  }
  override protected def buildTargetPlanWithIndex(
      spark: SparkSession,
      index: org.apache.spark.sql.delta.files.TahoeFileIndex,
      columnsToDrop: Seq[String]
  ): LogicalPlan = {
    if (nativePhase == null && writePhase == null)
      DeltaRangerMergeCommand.denied("unowned target index")
    val result = super.buildTargetPlanWithIndex(spark, index, columnsToDrop)
    if (nativePhase != null) nativePhase.targetPlan(result, index)
    else writePhase.target(result, index)
    result
  }
  override protected def generatePrecomputedConditionsAndDF(
      input: DataFrame,
      clauses: Seq[DeltaMergeIntoClause]
  ): (DataFrame, Seq[DeltaMergeIntoClause]) = {
    if (writePhase == null) DeltaRangerMergeCommand.denied("unowned precomputation")
    writePhase.precompute(input, clauses) {
      super.generatePrecomputedConditionsAndDF(input, clauses)
    }
  }
  override protected def generateWriteAllChangesOutputCols(
      targetWriteCols: Seq[Expression],
      rowId: Option[NamedExpression],
      rowVersion: Option[NamedExpression],
      names: Seq[String],
      noops: Seq[Expression],
      clauses: Seq[DeltaMergeIntoClause],
      cdc: Boolean,
      countDeleted: Boolean
  ): IndexedSeq[Column] = {
    if (writePhase == null || rowId.nonEmpty || rowVersion.nonEmpty || !countDeleted)
      DeltaRangerMergeCommand.denied("unsupported output generation")
    val result = super.generateWriteAllChangesOutputCols(
      targetWriteCols,
      rowId,
      rowVersion,
      names,
      noops,
      clauses,
      cdc,
      countDeleted
    )
    writePhase.columns(clauses, cdc, result)
    result
  }
  override protected def generateCdcAndOutputRows(
      input: DataFrame,
      columns: Seq[Column],
      names: Seq[String],
      noops: Seq[Expression],
      rowId: Option[String],
      rowVersion: Option[String],
      deduplicate: DeduplicateCDFDeletes
  ): DataFrame = {
    if (
      writePhase == null || rowId.nonEmpty || rowVersion.nonEmpty || deduplicate != writePhase.deduplicate
    )
      DeltaRangerMergeCommand.denied("unsupported CDC generation")
    writePhase.cdc(input, columns) {
      super.generateCdcAndOutputRows(input, columns, names, noops, rowId, rowVersion, deduplicate)
    }
  }
  override protected def writeFiles(
      spark: SparkSession,
      transaction: OptimisticTransaction,
      output: DataFrame
  ): Seq[FileAction] = {
    if (writePhase == null) DeltaRangerMergeCommand.denied("unowned native file output")
    writePhase.write(output) { super.writeFiles(spark, transaction, output) }
  }
  override protected def runMerge(spark: SparkSession): Seq[Row] = {
    val owned = receipt
    owned.validate()
    if (attempt != 1 || isInsertOnly || canMergeSchema || canOverwriteSchema)
      DeltaRangerMergeCommand.denied("unsupported native branch")
    targetDeltaLog.withNewTransaction(native.catalogTable) { transaction =>
      owned.requireTransaction(transaction)
      if (org.apache.spark.sql.delta.RowTracking.isSupported(transaction.protocol))
        DeltaRangerMergeCommand.denied("row tracking is not qualified")
      val sourceAttempt = DeltaRangerMergeSource.begin(owned, attempt)
      try {
        sourceAttempt.materialize {
          prepareMergeSource(
            spark,
            source,
            condition,
            matchedClauses,
            notMatchedClauses,
            isInsertOnly
          )
          sourceAttempt.returned(getMergeSource.df.queryExecution.analyzed, materializedSourceRDD)
        }
        nativePhase = new DeltaRangerMergeNative(
          owned,
          transaction,
          sourceAttempt,
          condition,
          if (isMatchedOnly) clauseDisjunction(matchedClauses) else Literal.TrueLiteral
        )
        val selected =
          try {
            val result = findTouchedFiles(spark, transaction)
            nativePhase.returned(
              result._1,
              (names, candidates) => {
                val mapping = generateCandidateFileMap(targetDeltaLog.dataPath, candidates)
                names.map(getTouchedFile(targetDeltaLog.dataPath, _, mapping))
              }
            )
            result
          } finally { nativePhase.close(); nativePhase = null }
        if (
          shouldWritePersistentDeletionVectors(
            spark,
            transaction
          )
        )
          DeltaRangerMergeCommand.denied("unsupported native rewrite branch")
        writePhase = new DeltaRangerMergeWrite(
          owned,
          transaction,
          sourceAttempt,
          selected._1,
          condition,
          matchedClauses ++ notMatchedClauses,
          shouldOptimizeMatchedOnlyMerge(spark),
          selected._2
        )
        val written =
          try
            writeAllChanges(
              spark,
              transaction,
              selected._1,
              selected._2,
              writeUnmodifiedRows = true
            )
          finally { writePhase.close(); writePhase = null }
        owned.requireTransaction(transaction)
        checkNonDeterministicSource(spark)
        transaction.registerSQLMetrics(spark, metrics)
        val actions = written ++ selected._1.map(_.remove)
        owned.closeSource()
        sourceAttempt.close()
        val operation = org.apache.spark.sql.delta.DeltaOperations.Merge(
          predicate = Some(condition),
          matchedPredicates =
            matchedClauses.map(org.apache.spark.sql.delta.DeltaOperations.MergePredicate(_)),
          notMatchedPredicates =
            notMatchedClauses.map(org.apache.spark.sql.delta.DeltaOperations.MergePredicate(_)),
          notMatchedBySourcePredicates = Nil
        )
        new DeltaRangerMergeCommit(
          owned,
          transaction,
          actions,
          operation,
          Map.empty[String, String]
        ).run()
        sendDriverMetrics(spark, metrics)
        Seq(
          Row(
            metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value + metrics(
              "numTargetRowsInserted"
            ).value,
            metrics("numTargetRowsUpdated").value,
            metrics("numTargetRowsDeleted").value,
            metrics("numTargetRowsInserted").value
          )
        )
      } finally sourceAttempt.close()
    }
  }
}

private[ranger] object DeltaRangerMergeCommand {
  def resolve(
      plan: LogicalPlan,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): Option[LogicalPlan] = plan match {
    case command: DeltaRangerMergeCommand                => command.receipt; Some(command)
    case merge: MergeIntoTable if merge.childrenResolved =>
      // Retain the complete SQL command before Delta's nested expression-resolution requests.
      // The stock analyzer below resolves only native-supplied expressions over these already
      // resolved operands; it is not an executable query admission path.
      DeltaRangerCdfRead
        .pending(merge.sourceTable, spark)
        .getOrElse(denied("one fresh named bounded CDF source required"))
      if (merge.expressions.exists(_.exists(_.isInstanceOf[SubqueryExpression])))
        denied("subquery expression")
      val resolver = new DeltaAnalysis(spark) {
        override protected def tryResolveReferencesForExpressions(
            session: SparkSession
        )(expressions: Seq[Expression], operands: Seq[LogicalPlan]): Seq[Expression] = {
          if ((session ne spark) || operands.exists(!_.resolved))
            denied("unresolved native operand")
          new Analyzer(spark.sessionState.catalogManager)
            .execute(FakeLogicalPlan(expressions, operands)) match {
            case FakeLogicalPlan(resolved, _) => resolved
            case _ => denied("unexpected expression-resolution successor")
          }
        }
      }
      resolver(merge) match {
        case resolved: DeltaMergeInto if resolved.resolved => resolve(resolved, spark, catalogs)
        case _ => denied("native SQL resolution did not produce a resolved merge")
      }
    case merge: DeltaMergeInto if merge.resolved =>
      val receipt = DeltaRangerMergeReceipt.issue(spark, merge, catalogs)
      val rewritten = receipt.rewrite()
      val native = PreprocessTableMerge(spark.sessionState.conf)(rewritten) match {
        case command: MergeIntoCommand => command
        case _                         => denied("unexpected native preprocessing successor")
      }
      Some(receipt.bind(native))
    case _: MergeIntoTable | _: DeltaMergeInto => Some(plan)
    case _: MergeIntoCommand                   => denied("unissued native command")
    case _                                     => None
  }
  final case class Exec()(@transient private val authority: DeltaRangerMergeReceipt)
      extends org.apache.spark.sql.execution.datasources.v2.V2CommandExec
      with org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec {
    private def owner: DeltaRangerMergeReceipt = {
      if (authority == null || isCanonicalizedPlan) denied("inert physical command")
      authority.requirePhysical(this); authority
    }
    def verify(): Unit = { owner; () }
    def completed(result: org.apache.spark.sql.catalyst.plans.logical.CommandResult): LogicalPlan =
      owner.result(result)
    override def output = owner.output
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): org.apache.spark.sql.execution.SparkPlan = this
    override def run(): Seq[org.apache.spark.sql.catalyst.InternalRow] = owner.execute(this)
  }
  def denied(reason: String): Nothing = throw new SecurityException(
    "Delta MERGE rejected: " + reason
  )
}
