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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.stats.{
  DeltaScan,
  DeltaScanGenerator,
  PrepareDeltaScan,
  PreparedDeltaFileIndex
}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Separately typed owner of finite metadata callbacks for one final selected raw source. */
private[ranger] final class DeltaRangerRawNativeScans(
    val observation: DeltaRangerRawNativeObservation.Observation,
    private val spark: SparkSession
) extends DeltaRangerNativeOperation.RawOwner {
  import DeltaRangerNativeOperation.Callback
  val metadata = new DeltaRangerRawMetadata(this)
  private val issued = new IdentityHashMap[TahoeFileIndex, DeltaScan]()
  override def session(): SparkSession = spark
  override def requireLive(): Unit = {
    val operation = DeltaRangerNativeOperation.current()
    if (operation == null) observation.requireCurrent(spark)
    else if (operation.rawOwner() eq this) observation.requireCurrent(spark, this)
    else throw new SecurityException("Conflicting raw native owner")
  }
  override def contains(candidate: LogicalRDD, stats: Boolean): Boolean =
    metadata.contains(candidate, stats)

  private[DeltaRangerRawNativeScans] def prepared(
      original: TahoeLogFileIndex,
      scan: DeltaScan
  ): PreparedDeltaFileIndex = {
    requireLive()
    val index = new DeltaRangerRawNativeScans.RawPrepared(this, original, scan)
    issued.put(index, scan)
    index
  }

  private[DeltaRangerRawNativeScans] def invoke[A](
      index: TahoeFileIndex,
      scan: DeltaScan,
      callback: Callback
  )(body: => A): A = DeltaRangerNativeOperation.withRawSession(
    this,
    () => {
      if (!issued.containsKey(index) || (issued.get(index) ne scan))
        throw new SecurityException("Unissued raw metadata index")
      val current = DeltaRangerNativeOperation.current()
      if (current != null) {
        if (
          (current.rawOwner() ne this) ||
          (current.callback() != Callback.PREPARE && (current.invokedIndex() ne index))
        )
          throw new SecurityException("Conflicting raw metadata callback")
        current.requireEvidence(spark)
        body
      } else {
        requireLive()
        val operation = DeltaRangerNativeOperation.rawScan(this, index, callback)
        DeltaRangerRawNativeScans.complete(operation) {
          metadata.capture()
          val result = body
          requireLive()
          result
        }
      }
    }
  )
}

private[ranger] object DeltaRangerRawNativeScans {
  import DeltaRangerNativeOperation.Callback
  private val selecting = new ThreadLocal[RawPlanning]()

  final class SelectionResult private[DeltaRangerRawNativeScans] (
      val input: DeltaRangerRawNativeObservation.Observation,
      val selected: Snapshot
  )

  def prepare(
      input: DeltaRangerRawMaintenance.SelectionInput,
      plan: LogicalPlan,
      dependency: Option[DeltaRangerRawProducer]
  ): (LogicalPlan, DeltaRangerRawNativeScans) =
    DeltaRangerNativeOperation.withRawSession(
      input.session,
      () => {
        if (
          DeltaRangerNativeOperation.current() != null || OptimisticTransaction.getActive.nonEmpty
        )
          throw new SecurityException("Conflicting raw preparation")
        var owner: DeltaRangerRawNativeScans = null
        var operation: DeltaRangerNativeOperation = null
        var failure: Throwable = null
        var planning: RawPlanning = null
        def start(observation: DeltaRangerRawNativeObservation.Observation): Unit = {
          if (owner != null) throw new SecurityException("Repeated raw final selection")
          input.unchanged()
          owner = new DeltaRangerRawNativeScans(observation, input.session)
          operation = DeltaRangerNativeOperation.rawPrepare(owner)
          owner.metadata.capture()
        }
        def copy(node: LogicalPlan): LogicalPlan = node match {
          case project: Project => project.copy(child = copy(project.child))
          case relation: LogicalRelation =>
            if (planning != null) throw new SecurityException("Multiple raw planning indices")
            val fs = relation.relation.asInstanceOf[HadoopFsRelation]
            planning = new RawPlanning(input.session, input.observation.rawIndex)
            relation.copy(relation = fs.copy(location = planning)(input.session))
          case cache: InMemoryRelation => cache
          case _ => throw new SecurityException("Unqualified raw planning recipe")
        }
        try {
          input.observation.requireCurrent(input.session)
          val copied = copy(plan)
          val prepared = new PrepareDeltaScan(input.session) {
            override protected def getDeltaScanGenerator(
                index: TahoeLogFileIndex
            ): DeltaScanGenerator = {
              if ((index ne planning) || owner != null || selecting.get() != null)
                throw new SecurityException("Unexpected raw stock selection")
              input.unchanged()
              input.observation.requireCurrent(input.session)
              selecting.set(planning)
              val generator =
                try DeltaRangerNativeOperation.selectRaw(() => super.getDeltaScanGenerator(index))
                finally selecting.remove()
              if (generator.getClass != classOf[Snapshot])
                throw new SecurityException("Foreign raw selected head")
              val selected = generator.asInstanceOf[Snapshot]
              val fresh = input.observation.afterSelection(
                input.session,
                new SelectionResult(input.observation, selected)
              )
              start(fresh)
              generator
            }
            override protected def getPreparedIndex(
                scan: DeltaScan,
                index: TahoeLogFileIndex
            ): PreparedDeltaFileIndex = {
              if (
                owner == null || (index ne planning) || (scan.scannedSnapshot ne owner.observation.selectedHead)
              )
                throw new SecurityException("Raw prepared scan changed")
              owner.prepared(index, scan)
            }
            override protected def prepareDeltaScanWithoutFileSkipping(
                candidate: LogicalPlan
            ): LogicalPlan =
              throw new SecurityException("Raw no-skipping path is not qualified")
          }.apply(copied)
          if (owner == null) {
            if (planning != null || dependency.isEmpty)
              throw new SecurityException("Missing raw stock selection")
            dependency.foreach(p => p.owner.requireInstalled(p))
            input.observation.requireCurrent(input.session)
            val fresh =
              DeltaRangerRawNativeObservation.observe(input.session, input.observation.rawIndex)
            if (
              (fresh.selectedHead ne input.observation.selectedHead) ||
              dependency.exists(_.owner.observation.selectedHead ne fresh.selectedHead)
            )
              throw new SecurityException("Cached raw dependency head changed")
            input.observation.requireCurrent(input.session)
            start(fresh)
          }
          owner.requireLive()
          operation.succeed()
          (prepared, owner)
        } catch {
          case error: Throwable => failure = error; throw error
        } finally if (operation != null) close(operation, failure)
      }
    )

  private def complete[A](operation: DeltaRangerNativeOperation)(body: => A): A = {
    var failure: Throwable = null
    try { val result = body; operation.succeed(); result }
    catch { case error: Throwable => failure = error; throw error }
    finally close(operation, failure)
  }

  private def close(operation: DeltaRangerNativeOperation, failure: Throwable): Unit =
    try operation.close()
    catch {
      case closing: Throwable =>
        if (failure != null) failure.addSuppressed(closing) else throw closing
    }

  private final class RawPlanning(spark: SparkSession, original: TahoeLogFileIndex)
      extends TahoeLogFileIndex(
        spark,
        original.deltaLog,
        original.path,
        original.snapshotAtAnalysis,
        Nil,
        false
      ) {
    private var selected = false
    override def getSnapshot: Snapshot = {
      if ((selecting.get() ne this) || selected)
        throw new SecurityException("Unowned raw planning callback")
      selected = true
      super.getSnapshot
    }
    override def matchingFiles(partitions: Seq[Expression], data: Seq[Expression]): Seq[AddFile] =
      throw new SecurityException("Raw planning file selection is not qualified")
    override def inputFiles: Array[String] = throw new SecurityException(
      "Raw planning inputFiles is not qualified"
    )
    override def sizeInBytes: Long = throw new SecurityException(
      "Raw planning size is not qualified"
    )
  }

  private final class RawPrepared(
      @transient private val authority: DeltaRangerRawNativeScans,
      original: TahoeLogFileIndex,
      scan: DeltaScan
  ) extends PreparedDeltaFileIndex(
        authority.session(),
        original.deltaLog,
        original.path,
        scan,
        original.versionToUse
      ) {
    private val files = scan.files.map { file =>
      if (
        file.getClass != classOf[AddFile] || file.deletionVector != null ||
        file.baseRowId.nonEmpty || file.defaultRowCommitVersion.nonEmpty
      ) throw new SecurityException("Foreign raw AddFile")
      file.json
    }.toVector
    private val sizes = (scan.total, scan.partition, scan.scanned)
    private def verify(): Unit = {
      if (authority == null) throw new SecurityException("Inert raw prepared index")
      authority.requireLive()
      if (
        (preparedScan ne scan) || (scan.scannedSnapshot ne authority.observation.selectedHead) ||
        scan.version != authority.observation.installedVersion || scan.partitionFilters.nonEmpty ||
        scan.dataFilters.nonEmpty || scan.partitionLikeDataFilters.nonEmpty ||
        scan.rewrittenPartitionLikeDataFilters.nonEmpty || scan.unusedFilters.nonEmpty ||
        scan.files
          .map(_.json)
          .toVector != files || (scan.total, scan.partition, scan.scanned) != sizes
      )
        throw new SecurityException("Raw prepared scan binding changed")
    }
    private def invoke[A](callback: Callback)(body: => A): A = {
      if (authority == null) throw new SecurityException("Inert raw prepared index")
      authority.invoke(this, scan, callback) { verify(); val result = body; verify(); result }
    }
    override def matchingFiles(partitions: Seq[Expression], data: Seq[Expression]): Seq[AddFile] = {
      if (partitions.nonEmpty || data.nonEmpty)
        throw new SecurityException("Raw prepared filters changed")
      invoke(Callback.MATCHING_FILES) { super.matchingFiles(partitions, data) }
    }
    override def inputFiles: Array[String] = invoke(Callback.INPUT_FILES) { super.inputFiles }
    override def sizeInBytes: Long = invoke(Callback.SIZE_IN_BYTES) { super.sizeInBytes }
  }
}
