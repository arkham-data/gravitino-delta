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
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.security.MessageDigest

import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
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
  DeltaOperations,
  DeltaLogFileIndex,
  OptimisticTransaction,
  Snapshot
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.actions.{Action, AddFile, SingleAction}
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Authority limited to one native commit and its exact target-state continuation. */
private[ranger] final class DeltaRangerAppendCommit(
    owner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
    transaction: OptimisticTransaction,
    actions: Seq[Action],
    nativeOperation: DeltaOperations.Write,
    tags: Map[String, String]
) extends DeltaRangerRegistrationBindings.AppendPublication
    with DeltaRangerNativeOperation.CommitOwner {
  import DeltaRangerAppendCommit._
  private val structure = new DeltaRangerMetadataStructure("Delta append commit rejected: ")
  import structure._

  private val spark = owner.session()
  private val context = owner.beginCommit(this)
  private val intent = new Intent(transaction, actions, nativeOperation, tags, spark)
  private val segment = transaction.snapshot.logSegment
  private val oldDeltas = segment.deltas.map(fileStamp).toVector
  private val checkpoints = segment.checkpointProvider.topLevelFiles.map(fileStamp).toVector
  private val minRetention = DeltaRangerNativeOperation.retentionTimestamp(transaction.snapshot)
  private val minTxnRetention =
    DeltaRangerNativeOperation.transactionRetentionTimestamp(transaction.snapshot)
  private val version = transaction.readVersion + 1
  private val nextPath = new Path(transaction.deltaLog.logPath, f"$version%020d.json")
  private val fs = nextPath.getFileSystem(spark.sessionState.newHadoopConf())
  if (fs.exists(nextPath)) denied("next commit already exists")
  private var next: Option[Vector[Any]] = None
  private var closed = false
  private var invoked = false
  private var publication: Option[(Vector[Any], Vector[Byte])] = None
  // Outer None means native commit is running; Some(None) records a native skipped result.
  private var nativeResult: Option[Option[Long]] = None
  private val reconstructions = ArrayBuffer.empty[QueryExecution]
  private val caches = ArrayBuffer.empty[(LogicalRDD, Vector[Any], QueryExecution)]
  private val reconstructionBindings = new IdentityHashMap[QueryExecution, Vector[Any]]()
  private val qualifiedReconstructionRecipes = mutable.Set.empty[Vector[Any]]
  private val copyGraphs = new IdentityHashMap[RDD[_], Vector[AnyRef]]()
  private var checkpointSnapshot: Snapshot = null
  private var checkpointAdmitting = false
  private var checkpointFunction: AnyRef = null
  private var checkpointCounter: LongAccumulator = null

  override def session(): SparkSession = spark

  override def requireLive(): Unit = {
    requireOwner()
    context.requireContext()
    observePublication()
    context.requireLive()
    next = publication.map(_._1)
    nativeResult.foreach { result =>
      if (result.exists(_ != version)) denied("native committed version changed")
      if (result.isDefined != publication.nonEmpty)
        denied("native result and publication proof differ")
    }
  }

  // Only this exact finite owner can furnish the Java context's publication evidence.
  override private[ranger] def requirePublication(
      candidate: DeltaRangerRegistrationBindings.BoundAppendCommit,
      nativeTransaction: OptimisticTransaction
  ): Boolean = {
    requireOwner()
    if (!(candidate eq context) || !(nativeTransaction eq transaction))
      denied("foreign publication consumer")
    observePublication().nonEmpty
  }

  private def requireOwner(): Unit =
    if (closed || !(active.get() eq this)) denied("expired commit owner")

  private def observePublication(): Option[Vector[Any]] = {
    if (!fs.exists(nextPath)) {
      if (publication.nonEmpty) denied("published commit disappeared")
      return None
    }
    if (!invoked) denied("publication precedes native invocation")
    val before = fs.getFileStatus(nextPath)
    if (!before.isFile) denied("publication is not a file")
    val input = fs.open(nextPath)
    val bytes =
      try input.readAllBytes()
      finally input.close()
    val after = fs.getFileStatus(nextPath)
    if (fileStamp(before) != fileStamp(after) || bytes.length.toLong != after.getLen)
      denied("publication changed while reading")
    intent.verify(bytes)
    val digest = MessageDigest.getInstance("SHA-256").digest(bytes).toVector
    val observed = fileStamp(after) -> digest
    if (publication.exists(_ != observed)) denied("published commit content or status changed")
    publication = Some(observed)
    Some(fileStamp(after))
  }

  def run(): Option[Long] = {
    if (active.get() != null || closed) denied("nested commit owner")
    active.set(this)
    var operation: DeltaRangerNativeOperation = null
    var primary: Throwable = null
    try {
      operation = DeltaRangerNativeOperation.commit(this)
      intent.requireArguments(actions, nativeOperation, tags)
      invoked = true
      val result = transaction.commitIfNeeded(actions, nativeOperation, tags = tags)
      nativeResult = Some(result)
      requireLive()
      operation.succeed()
      result
    } catch {
      case failure: Throwable => primary = failure; throw failure
    } finally {
      try {
        if (operation != null) operation.close()
      } catch {
        case cleanup: Throwable =>
          if (primary == null) throw cleanup
          if (cleanup ne primary) primary.addSuppressed(cleanup)
      } finally {
        closed = true
        active.remove()
        context.close()
      }
    }
  }

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
    if (closed || !(active.get() eq this)) denied("expired commit owner")
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

  override def contains(candidate: LogicalRDD): Boolean = {
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
      snapshot.version != version || snapshot.metadata != transaction.snapshot.metadata ||
      snapshot.protocol != transaction.snapshot.protocol ||
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
}

private[ranger] object DeltaRangerAppendCommit {
  private val active = new ThreadLocal[DeltaRangerAppendCommit]()

  def current: Option[DeltaRangerAppendCommit] = Option(active.get())

  /** Immutable full intent; it never substitutes actions passed to the native writer. */
  private final class Intent(
      transaction: OptimisticTransaction,
      actions: Seq[Action],
      operation: DeltaOperations.Write,
      tags: Map[String, String],
      spark: SparkSession
  ) {
    private val mapper = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    private val frozenActions = actions.map(action => parse(action.json)).toVector
    private val frozenTags = tags.toVector.toMap
    private val frozenParameters = operation.jsonEncodedValues.map { case (key, value) =>
      key -> parse(value)
    }
    private val frozenOperation = operation.name
    private val suppliedMetadata = operation.userMetadata
    private val userMetadata = suppliedMetadata.orElse(
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_USER_METADATA)
    )
    private val metrics =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)
    private val nativeId = transaction.txnId.toString
    private val readVersion = transaction.readVersion
    private val engine = s"Apache-Spark/${spark.version} Delta-Lake/${io.delta.VERSION}"
    if (
      readVersion < 0 || frozenActions
        .exists(node => node.size() != 1 || !Set("add", "txn")(node.fieldNames().next()))
    ) denied("unqualified submitted actions")

    def requireArguments(
        currentActions: Seq[Action],
        currentOperation: DeltaOperations.Write,
        currentTags: Map[String, String]
    ): Unit = {
      if (
        !(currentOperation eq operation) ||
        currentActions.map(action => parse(action.json)).toVector != frozenActions ||
        currentOperation.name != frozenOperation ||
        currentOperation.jsonEncodedValues.map { case (key, value) => key -> parse(value) } !=
          frozenParameters || currentOperation.userMetadata != suppliedMetadata ||
          currentTags != frozenTags
      ) denied("submitted intent changed")
    }

    def verify(bytes: Array[Byte]): Unit = {
      val text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString
      val lines = text.split("\n", -1).toVector
      if (lines.size < 2 || lines.last.nonEmpty || lines.init.exists(_.trim.isEmpty))
        denied("incomplete publication")
      val all = lines.init.map(parse)
      if (
        all.exists(node => !node.isObject || node.size() != 1) ||
        !all.head.has("commitInfo") || all.tail.exists(_.has("commitInfo"))
      )
        denied("publication envelopes changed")
      if (all.tail != frozenActions) denied("published actions differ from submitted intent")
      val info = all.head.get("commitInfo")
      val required = Set(
        "timestamp",
        "operation",
        "operationParameters",
        "readVersion",
        "isolationLevel",
        "isBlindAppend",
        "engineInfo",
        "txnId"
      ) ++
        (if (metrics) Set("operationMetrics") else Set.empty[String]) ++
        (if (frozenTags.nonEmpty) Set("tags") else Set.empty[String]) ++
        (if (userMetadata.nonEmpty) Set("userMetadata") else Set.empty[String])
      if (!info.isObject || info.fieldNames().asScala.toSet != required)
        denied("native CommitInfo profile changed")
      def exactString(name: String, expected: String): Unit =
        if (!info.get(name).isTextual || info.get(name).textValue() != expected)
          denied("native CommitInfo binding changed")
      exactString("txnId", nativeId)
      exactString("operation", frozenOperation)
      exactString("engineInfo", engine)
      if (
        !info.get("readVersion").isIntegralNumber ||
        !info
          .get("readVersion")
          .canConvertToLong || info.get("readVersion").longValue() != readVersion
      )
        denied("native read version changed")
      val params = info.get("operationParameters")
      if (
        !params.isObject || params.fields().asScala.map(e => e.getKey -> e.getValue).toMap !=
          frozenParameters
      ) denied("native operation parameters changed")
      if (frozenTags.nonEmpty && stringMap(info.get("tags")) != frozenTags)
        denied("native tags changed")
      userMetadata.foreach(exactString("userMetadata", _))
      if (
        !info.get("timestamp").isIntegralNumber || !info.get("timestamp").canConvertToLong ||
        info.get("timestamp").longValue() < 0 || !info.get("isBlindAppend").isBoolean ||
        !info.get("isolationLevel").isTextual ||
        !Set("Serializable", "WriteSerializable", "SnapshotIsolation")(
          info.get("isolationLevel").textValue()
        )
      ) denied("native CommitInfo values changed")
      if (metrics) {
        val observed = stringMap(info.get("operationMetrics"))
        if (
          !observed.keySet.subsetOf(operation.operationMetrics) ||
          observed.values.exists(value => !value.matches("[0-9]+"))
        )
          denied("native operation metrics changed")
      }
    }

    private def stringMap(node: JsonNode): Map[String, String] = {
      if (!node.isObject || node.elements().asScala.exists(value => !value.isTextual))
        denied("native string map changed")
      node.fields().asScala.map(e => e.getKey -> e.getValue.textValue()).toMap
    }

    private def parse(json: String): JsonNode =
      try {
        val value = mapper.readTree(json)
        if (value == null) denied("empty publication value")
        value
      } catch {
        case _: JsonProcessingException =>
          denied("invalid publication JSON")
      }
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta append commit rejected: " + reason)
}
