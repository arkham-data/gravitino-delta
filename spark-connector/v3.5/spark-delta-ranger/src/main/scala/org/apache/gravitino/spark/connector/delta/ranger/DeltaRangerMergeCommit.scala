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

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.security.MessageDigest
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaOperations, DeltaLogFileIndex, OptimisticTransaction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

/** Authority limited to one native commit and its exact target-state continuation. */
private[ranger] final class DeltaRangerMergeCommit(
    private val permit: DeltaRangerMergeCommand.PublicationPermit
) extends DeltaRangerNativeOperation.CommitOwner {
  private val receipt = permit.consume()
  private val transaction = permit.transaction
  private val actions = permit.actions
  private val nativeOperation = permit.operation
  private val tags = permit.tags
  import DeltaRangerMergeCommit._
  private val structure = new DeltaRangerMetadataStructure("Delta MERGE commit rejected: ")
  import structure._

  private val spark = receipt.spark
  receipt.beginCommit(this, transaction)
  private val intent = new Intent(transaction, actions, nativeOperation, tags, spark)
  private val version = transaction.readVersion + 1
  private val journal = receipt.journal
  journal.prepare(intent.encoded, version)
  private val nextPath = new Path(transaction.deltaLog.logPath, f"$version%020d.json")
  private val fs = nextPath.getFileSystem(spark.sessionState.newHadoopConf())
  if (fs.exists(nextPath)) denied("next commit already exists")
  private var next: Option[Vector[Any]] = None
  private var reconciled = false
  private val snapshot = transaction.snapshot
  private val oldDeltas = snapshot.logSegment.deltas.map(fileStamp).toVector
  private val checkpoints =
    snapshot.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector
  private var closed = false
  private var invoked = false
  private var publication: Option[(Vector[Any], Vector[Byte])] = None
  // Outer None means native commit is running; Some(None) records a native skipped result.
  private var nativeResult: Option[Option[Long]] = None

  override def session(): SparkSession = spark

  override def requireLive(): Unit = {
    requireOwner()
    observePublication()
    reconciled = receipt.requireCommitContext(this, transaction, publication.nonEmpty) || reconciled
    requireHead()
    next = publication.map(_._1)
    nativeResult.foreach { result =>
      if (result.exists(_ != version)) denied("native committed version changed")
      if (result.isDefined != publication.nonEmpty)
        denied("native result and publication proof differ")
    }
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
    journal.committed(bytes)
    publication = Some(observed)
    Some(fileStamp(after))
  }

  private def requireHead(): Unit = {
    val head = transaction.deltaLog.unsafeVolatileSnapshot
    if (
      head == null || head.getClass != classOf[org.apache.spark.sql.delta.Snapshot] ||
      (head.deltaLog ne transaction.deltaLog) || head.path != transaction.deltaLog.logPath ||
      ((head eq snapshot) && (head.logSegment.deltas.map(fileStamp).toVector != oldDeltas ||
        head.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector != checkpoints)) ||
      ((head ne snapshot) && !(publication.nonEmpty && head.version == version &&
        head.metadata == snapshot.metadata && head.protocol == snapshot.protocol &&
        head.logSegment.deltas
          .map(fileStamp)
          .toVector == oldDeltas ++ publication.map(_._1).toVector &&
        head.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector == checkpoints))
    )
      denied("native head changed")
  }

  def run(): Option[Long] = {
    if (active.get() != null || closed) denied("nested commit owner")
    active.set(this)
    var operation: DeltaRangerNativeOperation = null
    var primary: Throwable = null
    try {
      operation = DeltaRangerNativeOperation.commit(this)
      intent.requireArguments(actions, nativeOperation, tags)
      requireLive()
      invoked = true
      val result = transaction.commitIfNeeded(actions, nativeOperation, tags = tags)
      nativeResult = Some(result)
      requireLive()
      operation.succeed()
      val skipped =
        result.isEmpty && actions.isEmpty && publication.isEmpty && receipt.transactionIdentity.isEmpty
      if (!skipped && (!result.contains(version) || !reconciled))
        denied("publication is not reconciled")
      operation.close()
      operation = null
      if (skipped) {
        receipt.completeNoop(this, transaction)
        requireLive()
        journal.skipped()
      } else {
        receipt.recache(this, transaction)
        requireLive()
        journal.reconciled()
      }
      result
    } catch {
      case failure: Throwable =>
        primary = failure
        try {
          if (invoked) observePublication()
          if (publication.isEmpty) journal.unknown()
        } catch {
          case evidence: Throwable => if (evidence ne failure) failure.addSuppressed(evidence)
        }
        throw failure
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
        receipt.endCommit(this)
      }
    }
  }

  private[ranger] val metadata = new DeltaRangerCommitMetadata(
    transaction,
    spark,
    () => requireLive(),
    () => next,
    () => transaction.snapshot.metadata,
    () => transaction.snapshot.protocol
  )
  def ownsLog(relation: HadoopFsRelation, index: DeltaLogFileIndex): Boolean =
    metadata.ownsLog(relation, index)
  def checkpointPlan(plan: LogicalPlan, complete: Boolean = false): Boolean =
    metadata.checkpointPlan(plan, complete)
  def recordExecution(plan: SparkPlan): Unit = metadata.recordExecution(plan)
  def verifyReconstruction(plan: LogicalPlan): Unit = metadata.verifyReconstruction(plan)
  override def contains(candidate: LogicalRDD): Boolean = metadata.contains(candidate)

}

private[ranger] object DeltaRangerMergeCommit {
  private val active = new ThreadLocal[DeltaRangerMergeCommit]()

  def current: Option[DeltaRangerMergeCommit] = Option(active.get())

  /** Immutable full intent; it never substitutes actions passed to the native writer. */
  private final class Intent(
      transaction: OptimisticTransaction,
      actions: Seq[Action],
      operation: DeltaOperations.Merge,
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
        .exists(node =>
          node.size() != 1 || !Set("add", "remove", "cdc", "txn")(node.fieldNames().next())
        )
    ) denied("unqualified submitted actions")

    def requireArguments(
        currentActions: Seq[Action],
        currentOperation: DeltaOperations.Merge,
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

    val encoded: String = {
      val node = mapper.createObjectNode()
      val array = node.putArray("actions"); frozenActions.foreach(array.add)
      val parameters = node.putObject("parameters")
      frozenParameters.toVector.sortBy(_._1).foreach { case (key, value) =>
        parameters.set[JsonNode](key, value)
      }
      val tagNode = node.putObject("tags");
      frozenTags.toVector.sorted.foreach { case (key, value) => tagNode.put(key, value) }
      node.put("operation", frozenOperation)
      node.put("userMetadata", userMetadata.orNull)
      node.put("metrics", metrics)
      node.put("nativeId", nativeId)
      node.put("readVersion", readVersion)
      node.put("engine", engine)
      val allowed = node.putArray("allowedMetrics");
      operation.operationMetrics.toVector.sorted.foreach(allowed.add)
      node.toString
    }
    def verify(bytes: Array[Byte]): Unit = verifyPublication(bytes, encoded)
    private def parse(json: String): JsonNode = mapper.readTree(json)
  }

  def verifyPublication(bytes: Array[Byte], encoded: String): Unit =
    new Proof(encoded).verify(bytes)

  private final class Proof(encoded: String) {
    private val mapper = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    private val node = parse(encoded)
    private val frozenActions = node.get("actions").elements().asScala.toVector
    private val frozenTags = stringMap(node.get("tags"))
    private val frozenParameters =
      node.get("parameters").fields().asScala.map(e => e.getKey -> e.getValue).toMap
    private val frozenOperation = node.get("operation").textValue()
    private val userMetadata =
      Option(node.get("userMetadata")).filterNot(_.isNull).map(_.textValue())
    private val metrics = node.get("metrics").booleanValue()
    private val nativeId = node.get("nativeId").textValue()
    private val readVersion = node.get("readVersion").longValue()
    private val engine = node.get("engine").textValue()
    private val allowedMetrics =
      node.get("allowedMetrics").elements().asScala.map(_.textValue()).toSet
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
          !observed.keySet.subsetOf(allowedMetrics) ||
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
    throw new SecurityException("Delta MERGE commit rejected: " + reason)
}
