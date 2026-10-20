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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD

import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, coalesce, struct, when}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.apache.spark.sql.catalyst.analysis.{Analyzer, CleanupAliases}

import org.apache.spark.sql.catalyst.expressions.{IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  GlobalLimit,
  Filter,
  MapElements,
  LogicalPlan,
  Project,
  RepartitionByExpression,
  SerializeFromObject
}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.delta.{
  Checkpoints,
  DeltaLog,
  DeltaLogFileIndex,
  OptimisticTransaction,
  Snapshot
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.actions.{Action, AddFile, SingleAction, Metadata, Protocol}
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Shared finite metadata recipes; publication authority remains with each native operation. */
private[ranger] final class DeltaRangerCommitMetadata(
    transaction: OptimisticTransaction,
    spark: SparkSession,
    validate: () => Unit,
    publication: () => Option[Vector[Any]],
    expectedMetadata: () => Metadata,
    expectedProtocol: () => Protocol
) {
  private val structure = new DeltaRangerMetadataStructure("Delta commit metadata rejected: ")
  import structure._
  private val version = transaction.readVersion + 1
  private val segment = transaction.snapshot.logSegment
  private val oldDeltas = segment.deltas.map(fileStamp).toVector
  private val checkpoints = segment.checkpointProvider.topLevelFiles.map(fileStamp).toVector
  private val minRetention = DeltaRangerNativeOperation.retentionTimestamp(transaction.snapshot)
  private val minTxnRetention =
    DeltaRangerNativeOperation.transactionRetentionTimestamp(transaction.snapshot)
  private val reconstructions = ArrayBuffer.empty[QueryExecution]
  private val caches = ArrayBuffer.empty[(LogicalRDD, Vector[Any], QueryExecution)]
  private val reconstructionBindings = new IdentityHashMap[QueryExecution, Vector[Any]]()
  private val qualifiedReconstructionRecipes = mutable.Set.empty[Vector[Any]]
  private val copyGraphs = new IdentityHashMap[RDD[_], Vector[AnyRef]]()
  private var checkpointSnapshot: Snapshot = null
  private var checkpointAdmitting = false
  private var checkpointFunction: AnyRef = null
  private var checkpointCounter: LongAccumulator = null

  def ownsLog(fsRelation: HadoopFsRelation, index: DeltaLogFileIndex): Boolean = {
    requireLive()
    val files = index.files.toVector.map(fileStamp)
    val expectedRelation =
      transaction.deltaLog.indexToRelation(index).relation.asInstanceOf[HadoopFsRelation]
    val expectedDelta = next.map(oldDeltas :+ _)
    fsRelation.getClass == classOf[HadoopFsRelation] &&
    index.getClass == classOf[DeltaLogFileIndex] &&
    fsRelation.partitionSchema == index.partitionSchema &&
    fsRelation.dataSchema == Action.logSchema && fsRelation.bucketSpec.isEmpty &&
    fsRelation.fileFormat == index.format && fsRelation.options == expectedRelation.options &&
    (fsRelation.sparkSession eq spark) && index.rootPaths.toVector == index.files.toVector.map(
      _.getPath
    ) &&
    ((index.format eq DeltaLogFileIndex.COMMIT_FILE_FORMAT) && expectedDelta.contains(files) ||
      (index.format eq DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET) &&
      checkpoints.nonEmpty && files == checkpoints)
  }

  def recordExecution(plan: SparkPlan): Unit = {
    requireLive()
    val id = Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .getOrElse(denied("missing native execution"))
    val qe = SQLExecution.getQueryExecution(id.toLong)
    if (qe == null || !(qe.sparkSession eq spark)) denied("foreign native execution")
    if (!DeltaRangerPhysicalProvenance.nodes(qe.executedPlan).exists(_ eq plan))
      denied("unqualified commit consumer")
    val logical = qe.analyzed
    DeltaRangerNativeProvenance.requireExecutable(logical, spark)
    if (logical.collectLeaves().exists(_.isInstanceOf[LogicalRelation])) {
      verifyReconstruction(logical)
      if (reconstructions.nonEmpty && !reconstructions.exists(_ eq qe))
        denied("reconstruction execution replaced")
      val binding = DeltaRangerNativeProvenance.binding(logical)
      if (reconstructionBindings.containsKey(qe) && reconstructionBindings.get(qe) != binding)
        denied("reconstruction execution binding changed")
      reconstructionBindings.put(qe, binding)
      if (!reconstructions.exists(_ eq qe)) reconstructions += qe
    } else verifyConsumer(logical)
  }

  private def requireLive(): Unit = validate()
  private def next: Option[Vector[Any]] = publication()

  private def verifyConsumer(actual: LogicalPlan): Unit = actual match {
    case _: Aggregate | _: GlobalLimit if DeltaRangerPlanAdmission.nativeShape(actual) =>
    case checkpoint if checkpointPlan(checkpoint, complete = true)                     =>
    case serialized: SerializeFromObject =>
      val leaves = serialized.collectLeaves()
      val maps = serialized.collect { case map: MapElements => map }
      if (leaves.size != 1 || maps.size != 1 || !leaves.head.isInstanceOf[LogicalRDD])
        denied("unqualified commit consumer")
      val cache = leaves.head.asInstanceOf[LogicalRDD]
      if (!contains(cache)) denied("unowned commit consumer")
      val function = maps.head.func
      lambda(
        function,
        classOf[Dataset[_]],
        "org/apache/spark/sql/Dataset",
        "scala/Function1",
        "$anonfun$select$5",
        "(Ljava/lang/Object;)Ljava/lang/Object;",
        "(Lscala/Tuple1;)Ljava/lang/Object;",
        "(Lscala/Tuple1;)Ljava/lang/Object;",
        0
      )
      val add = cache.output.find(_.name == "add").getOrElse(denied("missing add action"))
      val selected = Project(Seq(add), Filter(IsNotNull(add), cache))
      val encoder = Encoders.product[AddFile].asInstanceOf[ExpressionEncoder[AddFile]]
      val tuple = ExpressionEncoder.tuple(encoder)
      val expected = new Analyzer(spark.sessionState.catalogManager)
        .execute(MapElements[Tuple1[AddFile], AddFile](function, selected)(tuple, encoder))
      if (!corresponds(expected, serialized, cache)) denied("commit file-list recipe changed")
    case _ => denied("unqualified commit consumer")
  }

  // Dataset operations analyze each prefix while native code constructs the final checkpoint.
  // Admission permits only these complete prefixes; execution requires the final projection.
  def checkpointPlan(actual: LogicalPlan, complete: Boolean = false): Boolean = {
    val maps = actual.collect { case map: MapElements => map }
    if (maps.size != 1 || maps.head.argumentClass != classOf[SingleAction]) return false
    requireLive()
    if (checkpointAdmitting) denied("checkpoint admission reentry")
    checkpointAdmitting = true
    try {
      requireLive()
      val leaves = actual.collectLeaves()
      if (leaves.size != 1 || !leaves.head.isInstanceOf[LogicalRDD])
        denied("checkpoint input changed")
      val cache = leaves.head.asInstanceOf[LogicalRDD]
      if (!contains(cache)) denied("unowned checkpoint input")
      val function = maps.head.func
      val descriptor = lambda(
        function,
        classOf[DeltaLog],
        "org/apache/spark/sql/delta/Checkpoints$",
        "scala/Function1",
        "$anonfun$writeCheckpoint$5",
        "(Ljava/lang/Object;)Ljava/lang/Object;",
        "(Lorg/apache/spark/util/LongAccumulator;Lorg/apache/spark/sql/delta/actions/SingleAction;)Lorg/apache/spark/sql/delta/actions/SingleAction;",
        "(Lorg/apache/spark/sql/delta/actions/SingleAction;)Lorg/apache/spark/sql/delta/actions/SingleAction;",
        1
      )
      val captured = descriptor.getCapturedArg(0)
      if (captured.getClass != classOf[LongAccumulator]) denied("checkpoint counter changed")
      val counter = captured.asInstanceOf[LongAccumulator]
      if (checkpointFunction == null) {
        if (counter.value.longValue() != 0L) denied("checkpoint counter was already used")
      } else if ((checkpointFunction ne function) || (checkpointCounter ne counter))
        denied("checkpoint function or counter replaced")
      val snapshot = publishedCheckpointSnapshot(cache)
      val numParts = spark.sessionState.conf
        .getConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE)
        .map(size =>
          math.ceil((snapshot.numOfFiles + snapshot.numOfRemoves).toDouble / size).toLong
        )
        .getOrElse(1L)
        .toInt
      val analyzer = new Analyzer(spark.sessionState.catalogManager)
      def analyzed(plan: LogicalPlan): LogicalPlan = analyzer.execute(plan)
      def replace(child: LogicalPlan, name: String, value: Column): LogicalPlan =
        analyzed(
          Project(
            child.output.map { attribute =>
              if (attribute.name == name) value.as(name).expr.asInstanceOf[NamedExpression]
              else attribute
            },
            child
          )
        )
      val partitioned = analyzed(
        RepartitionByExpression(
          Seq(coalesce(col("add.path"), col("remove.path")).expr),
          cache,
          Some(numParts)
        )
      )
      val encoder = Encoders.product[SingleAction]
      val counted = analyzed(
        MapElements[SingleAction, SingleAction](function, partitioned)(encoder, encoder)
      )
      val dropped = analyzed(
        Project(
          counted.output.filterNot(a =>
            Set("commitInfo", "cdc", "checkpointMetadata", "sidecar")(a.name)
          ),
          counted
        )
      )
      val removed = replace(dropped, "remove", col("remove").dropFields("tags", "stats"))
      val additional =
        (if (Checkpoints.shouldWriteStatsAsJson(snapshot)) Seq(col("add.stats").as("stats"))
         else Seq.empty) ++
          (if (Checkpoints.shouldWriteStatsAsStruct(spark.sessionState.conf, snapshot))
             Checkpoints
               .extractPartitionValues(snapshot.metadata.partitionSchema, "add.partitionValues")
               .toSeq
           else Seq.empty)
      val added = replace(
        removed,
        "add",
        when(
          col("add").isNotNull,
          struct(
            (Seq(
              col("add.path"),
              col("add.partitionValues"),
              col("add.size"),
              col("add.modificationTime"),
              col("add.dataChange"),
              col("add.tags"),
              col("add.deletionVector"),
              col("add.baseRowId"),
              col("add.defaultRowCommitVersion"),
              col("add.clusteringProvider")
            ) ++ additional): _*
          )
        )
      )
      val candidates = if (complete) Seq(added) else Seq(counted, dropped, removed, added)
      // Resolution visits this native withColumn before Spark removes its nested stats alias.
      // Apply that same stock cleanup to both construction stages, retaining output aliases/IDs.
      val construction = candidates.exists(corresponds(_, CleanupAliases(actual), cache))
      val optimized = !complete && !construction && corresponds(
        DeltaRangerDecisionReceipt.nativePrefix(added, spark),
        actual,
        cache
      )
      if (!construction && !optimized) denied("complete checkpoint recipe changed")
      requireLive()
      if (!(transaction.deltaLog.unsafeVolatileSnapshot eq snapshot))
        denied("checkpoint snapshot replaced")
      checkpointSnapshot = snapshot
      checkpointFunction = function
      checkpointCounter = counter
      true
    } finally checkpointAdmitting = false
  }

  def contains(candidate: LogicalRDD): Boolean = {
    requireLive()
    caches.find(_._1.rdd eq candidate.rdd) match {
      case Some((original, binding, qe)) =>
        verifyCopy(original, qe)
        verifyCopy(candidate, qe)
        if (
          DeltaRangerNativeProvenance.binding(original) != binding ||
          DeltaRangerNativeProvenance.binding(candidate) != binding
        )
          denied("cached state binding changed")
        true
      case None =>
        reconstructions.find { qe =>
          candidate.rdd.dependencies.size == 1 &&
          (candidate.rdd.dependencies.head.rdd eq qe.toRdd)
        } match {
          case Some(qe) =>
            verifyCopy(candidate, qe)
            caches += ((candidate, DeltaRangerNativeProvenance.binding(candidate), qe))
            true
          case None => false
        }
    }
  }

  def verifyReconstruction(actual: LogicalPlan): Unit = {
    requireLive()
    val leaves = actual.collectLeaves()
    leaves.foreach {
      case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex if ownsLog(fs, index) =>
              case _ => denied("reconstruction log input changed")
            }
          case _ => denied("reconstruction relation changed")
        }
      case _ => denied("reconstruction leaf changed")
    }
    qualifiedReconstructionRecipes += reconstruction(
      actual,
      spark,
      checkpoints.nonEmpty,
      minRetention,
      DeltaRangerNativeOperation.retentionTimestamp(transaction.snapshot),
      minTxnRetention,
      DeltaRangerNativeOperation.transactionRetentionTimestamp(transaction.snapshot),
      transaction.snapshot.deltaLog
        .newDeltaHadoopConf()
        .iterator()
        .asScala
        .map(e => e.getKey -> e.getValue)
        .toMap,
      DeltaRangerNativeOperation.snapshotPartitions(transaction.snapshot),
      qualifiedReconstructionRecipes.toSet
    )
  }

  private def publishedCheckpointSnapshot(cache: LogicalRDD): Snapshot = {
    val snapshot = transaction.deltaLog.unsafeVolatileSnapshot
    if (
      snapshot == null || snapshot.getClass != classOf[Snapshot] ||
      !(snapshot.deltaLog eq transaction.deltaLog) || snapshot.path != transaction.deltaLog.logPath ||
      snapshot.version != version || snapshot.metadata != expectedMetadata() ||
      snapshot.protocol != expectedProtocol() ||
      snapshot.logSegment.deltas
        .map(fileStamp)
        .toVector != next.map(oldDeltas :+ _).getOrElse(Vector.empty) ||
      snapshot.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector != checkpoints ||
      (checkpointSnapshot != null && !(checkpointSnapshot eq snapshot))
    )
      denied("checkpoint snapshot binding changed")
    // The native hook has already evaluated this exact snapshot's counts and cached state.
    // getDF reuses that Dataset; unlike stateDS it does not introduce a new typed analysis.
    val state = snapshot.stateDF
    if (!(state.sparkSession eq spark)) denied("checkpoint state session changed")
    state.queryExecution.analyzed match {
      case independent: LogicalRDD
          if (independent.rdd eq cache.rdd) && contains(independent) &&
            DeltaRangerNativeProvenance
              .binding(independent) == DeltaRangerNativeProvenance.binding(cache) =>
      case _ => denied("checkpoint snapshot state changed")
    }
    snapshot
  }

  private def verifyCopy(candidate: LogicalRDD, qe: QueryExecution): Unit = {
    if (reconstructionBindings.get(qe) != DeltaRangerNativeProvenance.binding(qe.analyzed))
      denied("reconstruction execution binding changed")
    verifyReconstruction(qe.analyzed)
    if (
      candidate.getClass != classOf[LogicalRDD] || candidate.isStreaming ||
      candidate.output != qe.analyzed.output ||
      candidate.outputPartitioning != UnknownPartitioning(0) ||
      candidate.outputOrdering.nonEmpty ||
      !DeltaRangerNativeOperation.stockCacheWrapper(candidate, spark)
    )
      denied("cached state output changed")
    val rdd = candidate.rdd
    if (
      rdd.dependencies.size != 1 ||
      rdd.dependencies.head.getClass != classOf[OneToOneDependency[_]] ||
      !(rdd.dependencies.head.rdd eq qe.toRdd) || !(field(rdd, "prev") eq qe.toRdd) ||
      rdd.partitioner.nonEmpty ||
      field(rdd, "isFromBarrier") != java.lang.Boolean.FALSE ||
      field(rdd, "isOrderSensitive") != java.lang.Boolean.FALSE
    )
      denied("cached state parent changed")
    val wrapper = lambda(
      field(rdd, "f"),
      classOf[RDD[_]],
      "org/apache/spark/rdd/RDD",
      "scala/Function3",
      "$anonfun$map$2$adapted",
      "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
      "(Lscala/Function1;Lorg/apache/spark/TaskContext;Ljava/lang/Object;Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      "(Lorg/apache/spark/TaskContext;Ljava/lang/Object;Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      1
    )
    lambda(
      wrapper.getCapturedArg(0),
      classOf[DeltaLog],
      "org/apache/spark/sql/delta/util/StateCache$CachedDS",
      "scala/Function1",
      "$anonfun$cachedDs$3",
      "(Ljava/lang/Object;)Ljava/lang/Object;",
      "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
      "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
      0
    )
    val graph = Vector(rdd.dependencies.head, qe.toRdd, field(rdd, "f"), wrapper.getCapturedArg(0))
    val previous = copyGraphs.get(rdd)
    if (previous != null && !previous.zip(graph).forall { case (a, b) => a eq b })
      denied("cached state function graph replaced")
    copyGraphs.put(rdd, graph)
  }
  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta commit metadata rejected: " + reason)
}
