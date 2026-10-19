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

import scala.collection.JavaConverters._
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerNativeOperation.Callback

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.stats.{DeltaScan, PreparedDeltaFileIndex}
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}

/** Complete receipt owner retained outside Spark products and executor serialization. */
private[ranger] final class DeltaRangerNativeScans private (
    val session: SparkSession,
    private val receipt: DeltaRangerDecisionReceipt,
    private val source: Option[DeltaRangerAppendCoordinator.Source] = None
) extends DeltaRangerNativeOperation.ScanOwner {
  val obligations = receipt.obligations
  private var frozen = false
  private val transitions = new java.util.IdentityHashMap[TahoeFileIndex, TahoeLogFileIndex]()

  def issued(index: TahoeFileIndex, original: TahoeLogFileIndex): Unit = {
    occurrences(original)
    if (
      frozen || DeltaRangerNativeOperation.current() == null ||
      !DeltaRangerNativeOperation.current().active() || !DeltaRangerNativeOperation
        .current()
        .ownedBy(this)
    )
      throw new SecurityException("Native index issuance is closed")
    transitions.put(index, original)
  }

  def finish(plan: LogicalPlan, operation: DeltaRangerNativeOperation): LogicalPlan = {
    val relations = plan.collect { case relation: LogicalRelation => relation }.toVector
    val mapped = relations.map { relation =>
      val fs = relation.relation.asInstanceOf[HadoopFsRelation]
      val original = transitions.get(fs.location)
      if (original == null) throw new SecurityException("Unissued native index transition")
      val matches = occurrences(original).filter { obligation =>
        relation.catalogTable.exists(_ eq obligation.binding.descriptor()) &&
        relation.output.nonEmpty && relation.output.forall { attribute =>
          obligation.scan.relation.output.exists(_.exprId == attribute.exprId)
        }
      }
      if (matches.size != 1) throw new SecurityException("Ambiguous native scan occurrence")
      new DeltaRangerNativeScans.Occurrence(matches.head, relation, fs.location)
    }
    val emptyBinding =
      if (mapped.isEmpty) DeltaRangerNativeProvenance.emptyPreparationBinding(plan) else None
    if (mapped.isEmpty) {
      if (emptyBinding.isEmpty)
        throw new SecurityException("Unqualified empty native preparation")
    } else if (
      mapped.map(_.obligation).toSet != obligations.toSet || mapped.size != obligations.size
    )
      throw new SecurityException("Incomplete original native occurrences")
    if (frozen || !operation.ownedBy(this) || !operation.active())
      throw new SecurityException("Native preparation issuance is closed")
    frozen = true
    val evidence = new DeltaRangerNativeScans.Prepared(
      this,
      operation,
      mapped,
      plan.output,
      emptyBinding,
      new DeltaRangerPhysicalPlanning.LogicalWitness(plan, session)
    )
    DeltaRangerNativeScans.attach(plan, evidence)
    source.foreach(_.complete(evidence))
    plan
  }
  override def bindings(): java.util.List[DeltaRangerRegistrationBindings.BoundRegistrationScan] =
    obligations.map(_.binding).asJava
  override def requireLive(): Unit = {
    source.foreach(_.requireLive())
    obligations.foreach(_.binding.requireLive())
  }
  def requireExecutable(): Unit =
    source.foreach(_.requireExecutable(this))

  /** Identifies the retained append-source lifetime for eager native file-list planning. */
  def appendInput: Boolean = source.nonEmpty

  /** Resolves lazy native file listing on the issuing driver before broadcast workers start. */
  def prepareInputs(plan: SparkPlan): Unit = if (source.nonEmpty) {
    requireExecutable()
    DeltaRangerPhysicalProvenance.nodes(plan).foreach {
      case scan: FileSourceScanExec =>
        requireLive()
        scan.inputRDD
        requireLive()
      case _ =>
    }
    requireExecutable()
  }

  def requireGenerator(transaction: OptimisticTransaction): Unit = source match {
    case Some(owner) => owner.requireGenerator(transaction)
    case None =>
      throw new SecurityException("Native transactional scan preparation is not qualified")
  }

  override def freezeCache(
      actual: org.apache.spark.sql.execution.LogicalRDD
  ): java.util.function.Predicate[org.apache.spark.sql.execution.LogicalRDD] = {
    val frozen = DeltaRangerNativeProvenance.binding(actual)
    candidate => {
      requireLive()
      if (DeltaRangerNativeProvenance.binding(actual) != frozen)
        throw new SecurityException("Original native cache binding changed")
      DeltaRangerNativeProvenance.binding(candidate) == frozen
    }
  }

  override def sealReconstruction(
      operation: DeltaRangerNativeOperation,
      actual: LogicalPlan,
      stats: Boolean
  ): Runnable = {
    if (!operation.ownedBy(this))
      throw new SecurityException("Native reconstruction has a different scan owner")
    val witness =
      DeltaRangerNativeProvenance.returnedReconstruction(actual, operation, session, stats)
    () => witness.requireValid(actual, session)
  }

  def run[T](body: DeltaRangerNativeOperation => T): T = {
    val operation = DeltaRangerNativeOperation.prepare(this)
    try {
      val result = body(operation)
      operation.succeed()
      result
    } finally operation.close()
  }

  def invoke[T](
      index: TahoeFileIndex,
      partitions: Seq[Expression],
      data: Seq[Expression],
      callback: Callback = Callback.MATCHING_FILES
  )(
      body: DeltaRangerNativeOperation => T
  ): T = {
    val operation = DeltaRangerNativeOperation.scan(this, index, partitions, data, callback)
    try {
      val result = body(operation)
      operation.succeed()
      result
    } finally operation.close()
  }

  def occurrences(index: TahoeLogFileIndex): Vector[DeltaRangerDecisionReceipt.Obligation] = {
    val matched = obligations.filter(_.binding.index() eq index)
    if (matched.isEmpty) throw new SecurityException("Native index has no original obligation")
    matched
  }
  def validateAction(session: SparkSession): Unit = receipt.validateAction(session)

  override def toString: String = "Native Delta scan owner"
}

private[ranger] object DeltaRangerNativeScans {
  final class Occurrence(
      val obligation: DeltaRangerDecisionReceipt.Obligation,
      val relation: LogicalRelation,
      val index: FileIndex
  ) {
    override def toString: String = "Prepared native occurrence"
  }

  final class Prepared private[ranger] (
      val owner: DeltaRangerNativeScans,
      private val preparation: DeltaRangerNativeOperation,
      val occurrences: Vector[Occurrence],
      private val output: Seq[Attribute],
      private val emptyBinding: Option[Vector[Any]],
      private val executable: DeltaRangerPhysicalPlanning.LogicalWitness
  ) {

    /** Verifies occurrence bindings; the later physical gate must also verify executable behavior.
      */
    def verify(plan: LogicalPlan, session: SparkSession): Unit = {
      preparation.requireEvidence(session)
      if (!preparation.succeeded() || plan.output != output)
        throw new SecurityException("Unsealed or changed native preparation")
      val actual = plan.collect { case relation: LogicalRelation => relation }
      if (actual.size != occurrences.size)
        throw new SecurityException("Native occurrence count changed")
      actual.zip(occurrences).foreach { case (relation, expected) =>
        if (
          !(relation.relation eq expected.relation.relation) ||
          relation.catalogTable != expected.relation.catalogTable ||
          !relation.catalogTable.exists(_ eq expected.obligation.binding.descriptor()) ||
          relation.output != expected.relation.output ||
          !(relation.relation.asInstanceOf[HadoopFsRelation].location eq expected.index)
        )
          throw new SecurityException("Native prepared backing identity changed")
      }
      // Spark's later ConvertToLocalRelation may evaluate the recorded empty projection. Its
      // exact final output is checked above; no rows or additional executable operators survive.
      val collapsedEmpty = plan match {
        case local: LocalRelation =>
          local.getClass == classOf[LocalRelation] && local.data.isEmpty && !local.isStreaming
        case _ => false
      }
      if (
        actual.isEmpty && (emptyBinding.isEmpty ||
          (!collapsedEmpty && DeltaRangerNativeProvenance.emptyPreparationBinding(
            plan
          ) != emptyBinding))
      )
        throw new SecurityException("Empty native preparation changed")
    }
    def recoverExecutable(session: SparkSession): LogicalPlan =
      executable.replay(session, plan => verify(plan, session))

    def verifyExecutable(plan: LogicalPlan, session: SparkSession): LogicalPlan = {
      verify(plan, session)
      executable.verify(plan, session)
    }

    override def toString: String = "Prepared native occurrence evidence"
  }

  final class Reference(@transient private val authority: Prepared) extends Serializable {
    def get: Prepared = {
      if (authority == null) throw new SecurityException("Inert native preparation reference")
      authority
    }
    override def toString: String = "Native preparation reference"
  }

  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.native-preparation")
  private def attach(plan: LogicalPlan, prepared: Prepared): Unit =
    plan.setTagValue(tag, new Reference(prepared))

  def evidence(plan: LogicalPlan): Option[Prepared] = plan.getTagValue(tag).map(_.get)

  def prepared(plan: LogicalPlan): Prepared =
    plan.getTagValue(tag).getOrElse(throw new SecurityException("Missing native preparation")).get

  def forAppend(
      spark: SparkSession,
      receipt: DeltaRangerDecisionReceipt,
      source: DeltaRangerAppendCoordinator.Source
  ): DeltaRangerNativeScans = new DeltaRangerNativeScans(spark, receipt, Some(source))

  def consume(
      plan: LogicalPlan,
      spark: SparkSession,
      optimized: Boolean = true
  ): DeltaRangerNativeScans =
    new DeltaRangerNativeScans(
      spark,
      DeltaRangerDecisionReceipt.consumeOwned(plan, spark, optimized)
    )
}

/** Native factory product with inert serialization and identity-bound driver continuation. */
private[ranger] final class DeltaRangerPreparedIndex private (
    owner: DeltaRangerNativeScans,
    original: TahoeLogFileIndex,
    scan: DeltaScan,
    operation: DeltaRangerNativeOperation
) extends PreparedDeltaFileIndex(
      owner.session,
      original.deltaLog,
      original.path,
      scan,
      original.versionToUse
    ) {
  @transient private val authority =
    new DeltaRangerPreparedIndex.Continuation(this, owner, original, scan, operation)

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]
  ): Seq[AddFile] = {
    if (authority == null) throw new SecurityException("Inert native prepared index")
    authority.invoke(this, partitionFilters, dataFilters) {
      super.matchingFiles(partitionFilters, dataFilters)
    }
  }
}

private[ranger] object DeltaRangerPreparedIndex {
  def create(
      owner: DeltaRangerNativeScans,
      original: TahoeLogFileIndex,
      scan: DeltaScan,
      operation: DeltaRangerNativeOperation
  ): DeltaRangerPreparedIndex = {
    assert(
      original.partitionFilters.isEmpty,
      "Partition filters should have been extracted by DeltaAnalysis."
    )
    owner.occurrences(original)
    if (
      !operation.active() || !operation
        .ownedBy(owner) || !operation.contains(original, scan.scannedSnapshot)
    )
      throw new SecurityException("Prepared snapshot was not captured by its owner")
    val issued = new DeltaRangerPreparedIndex(owner, original, scan, operation)
    owner.issued(issued, original)
    issued
  }

  private final class Continuation(
      installed: DeltaRangerPreparedIndex,
      owner: DeltaRangerNativeScans,
      original: TahoeLogFileIndex,
      scan: DeltaScan,
      preparation: DeltaRangerNativeOperation
  ) {
    private val occurrences = owner.occurrences(original)
    def invoke[T](
        index: DeltaRangerPreparedIndex,
        partitions: Seq[Expression],
        data: Seq[Expression]
    )(body: => T): T = {
      if (
        !(index eq installed) || !(index.preparedScan eq scan) ||
        !(index.preparedScan.scannedSnapshot eq scan.scannedSnapshot) ||
        !preparation.succeeded() || occurrences != owner.occurrences(original)
      )
        throw new SecurityException("Native prepared continuation does not bind this index")
      preparation.requireEvidence(owner.session)
      owner.invoke(index, partitions, data) { operation =>
        operation.captureRetained(original, scan.scannedSnapshot, preparation)
        body
      }
    }
  }
}

/** Owns the ordinary native no-skipping callbacks without replacing Delta's snapshot selection. */
private[ranger] final class DeltaRangerLogIndex private (
    owner: DeltaRangerNativeScans,
    original: TahoeLogFileIndex,
    preparation: DeltaRangerNativeOperation
) extends TahoeLogFileIndex(
      owner.session,
      original.deltaLog,
      original.path,
      original.snapshotAtAnalysis,
      original.partitionFilters,
      original.isTimeTravelQuery
    ) {
  @transient private val authority =
    new DeltaRangerLogIndex.Continuation(this, owner, original, preparation)

  override def matchingFiles(partitions: Seq[Expression], data: Seq[Expression]): Seq[AddFile] =
    invocation(partitions, data, Callback.MATCHING_FILES) { _ =>
      super.matchingFiles(partitions, data)
    }

  override def getSnapshot: Snapshot = invocation(Nil, Nil, Callback.GET_SNAPSHOT) { operation =>
    val selected = super.getSnapshot
    operation.capture(authority.original, selected)
    selected
  }

  override def inputFiles: Array[String] = invocation(partitionFilters, Nil, Callback.INPUT_FILES) {
    _ =>
      super.inputFiles
  }

  override def sizeInBytes: Long = invocation(Nil, Nil, Callback.SIZE_IN_BYTES) { operation =>
    val selected = deltaLog.unsafeVolatileSnapshot
    operation.capture(authority.original, selected)
    selected.sizeInBytes
  }

  private def invocation[T](partitions: Seq[Expression], data: Seq[Expression], callback: Callback)(
      body: DeltaRangerNativeOperation => T
  ): T = {
    if (authority == null) throw new SecurityException("Inert native log index")
    authority.invoke(this, partitions, data, callback)(body)
  }
}

private[ranger] object DeltaRangerLogIndex {
  def create(
      owner: DeltaRangerNativeScans,
      original: TahoeLogFileIndex,
      operation: DeltaRangerNativeOperation
  ): DeltaRangerLogIndex = {
    owner.occurrences(original)
    if (
      !operation.active() || !operation
        .ownedBy(owner) || original.getClass != classOf[TahoeLogFileIndex]
    )
      throw new SecurityException("Unqualified original native log index")
    val issued = new DeltaRangerLogIndex(owner, original, operation)
    owner.issued(issued, original)
    issued
  }

  private final class Continuation(
      installed: DeltaRangerLogIndex,
      owner: DeltaRangerNativeScans,
      val original: TahoeLogFileIndex,
      preparation: DeltaRangerNativeOperation
  ) {
    def invoke[T](
        index: DeltaRangerLogIndex,
        partitions: Seq[Expression],
        data: Seq[Expression],
        callback: Callback
    )(
        body: DeltaRangerNativeOperation => T
    ): T = {
      if (
        !(index eq installed) || !(index.snapshotAtAnalysis eq original.snapshotAtAnalysis) ||
        !(index.deltaLog eq original.deltaLog)
      )
        throw new SecurityException("Native log continuation does not bind this index")
      preparation.requireEvidence(owner.session)
      val active = DeltaRangerNativeOperation.current()
      if (active != null) {
        if ((active eq preparation) || (active.invokedIndex() eq installed)) {
          active.requireEvidence(owner.session)
          body(active)
        } else throw new SecurityException("Native log callback has a different active owner")
      } else {
        if (!preparation.succeeded())
          throw new SecurityException("Unsealed native log continuation")
        owner.invoke(index, partitions, data, callback)(body)
      }
    }
  }
}
