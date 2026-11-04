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
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

/** Authority limited to one native commit and its exact target-state continuation. */
private[ranger] final class DeltaRangerAppendCommit(
    owner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
    transaction: OptimisticTransaction,
    actions: Seq[Action],
    nativeOperation: DeltaOperations.Write,
    tags: Map[String, String],
    schemaIntent: Option[DeltaRangerAppendSchema.Intent] = None
) extends DeltaRangerRegistrationBindings.AppendPublication
    with DeltaRangerNativeOperation.CommitOwner {
  import DeltaRangerAppendCommit._
  private val structure = new DeltaRangerMetadataStructure("Delta append commit rejected: ")
  import structure._

  private val spark = owner.session()
  private val expectedMetadata =
    schemaIntent.map(_.expectedMetadata).getOrElse(transaction.snapshot.metadata)
  private val context = owner.beginCommit(this, expectedMetadata)
  private val intent =
    new Intent(transaction, actions, nativeOperation, tags, spark, expectedMetadata)
  private val nativeTransactionId = transaction.txnId.toString
  private val readVersion = transaction.readVersion
  private val version = readVersion + 1
  private val nextPath = new Path(transaction.deltaLog.logPath, f"$version%020d.json")
  private val fs = nextPath.getFileSystem(spark.sessionState.newHadoopConf())
  if (fs.exists(nextPath)) denied("next commit already exists")
  private var next: Option[Vector[Any]] = None
  private var closed = false
  private var invoked = false
  private var publication: Option[(Vector[Any], Vector[Byte])] = None
  // Outer None means native commit is running; Some(None) records a native skipped result.
  private var nativeResult: Option[Option[Long]] = None

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

  def run(): Outcome = {
    if (active.get() != null || closed) denied("nested commit owner")
    active.set(this)
    var operation: DeltaRangerNativeOperation = null
    var primary: Throwable = null
    try {
      operation = DeltaRangerNativeOperation.commit(this)
      intent.requireArguments(actions, nativeOperation, tags)
      if (
        transaction.metadata != expectedMetadata || transaction.protocol != transaction.snapshot.protocol
      )
        denied("pending metadata or protocol changed")
      invoked = true
      nativeResult = Some(transaction.commitIfNeeded(actions, nativeOperation, tags = tags))
      requireLive()
      operation.succeed()
    } catch {
      case failure: Throwable => primary = failure
    } finally {
      def cleanup(body: => Unit): Unit = try body
      catch {
        case failure: Throwable =>
          if (primary == null) primary = failure
          else if (failure ne primary) primary.addSuppressed(failure)
      }
      cleanup { if (operation != null) operation.close() }
      // Diagnose while the captured native transaction is still present, without renewing authority.
      if (primary != null && invoked && publication.isEmpty) {
        try {
          intent.requireTransactionIdentity()
          if (observePublication().isEmpty) denied("native publication is unconfirmed")
          intent.requireTransactionIdentity()
          observePublication()
          intent.requireTransactionIdentity()
        } catch {
          case proof: Throwable => if (proof ne primary) primary.addSuppressed(proof)
        }
      }
      closed = true
      active.remove()
      cleanup { context.close() }
    }
    val result = publication
      .map { witness =>
        new Outcome(
          "committed",
          Some(
            new Evidence(
              nativeTransactionId,
              readVersion,
              version,
              nextPath.toString,
              expectedMetadata.id,
              witness._1,
              witness._2
            )
          )
        )
      }
      .getOrElse(
        if (!invoked) NotCommitted
        else if (nativeResult.contains(None)) Skipped
        else new Outcome("unknown", None, Some(nativeTransactionId), Some(version))
      )
    if (primary != null) throw result.failure(primary)
    result
  }

  private[ranger] val metadata = new DeltaRangerCommitMetadata(
    transaction,
    spark,
    () => requireLive(),
    () => next,
    () => expectedMetadata,
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

private[ranger] object DeltaRangerAppendCommit {
  private val active = new ThreadLocal[DeltaRangerAppendCommit]()

  def current: Option[DeltaRangerAppendCommit] = Option(active.get())

  /** Detached native proof for diagnosis only; it grants no read or execution authority. */
  final class Evidence private[DeltaRangerAppendCommit] (
      val transactionId: String,
      val readVersion: Long,
      val version: Long,
      val path: String,
      val tableId: String,
      val fileStamp: Vector[Any],
      val digest: Vector[Byte]
  )

  final class Outcome private[DeltaRangerAppendCommit] (
      val status: String,
      val evidence: Option[Evidence],
      val transactionId: Option[String] = None,
      val attemptedVersion: Option[Long] = None
  ) {
    def version: Option[Long] = evidence.map(_.version)
    def failure(cause: Throwable): Throwable = cause match {
      case prior: Failure => prior
      case _              => new Failure(this, cause)
    }
  }
  final class Failure private[DeltaRangerAppendCommit] (val outcome: Outcome, cause: Throwable)
      extends SecurityException(
        "Delta append outcome: " + outcome.status +
          (if (outcome.evidence.nonEmpty) "; observation unconfirmed" else "") + "; do not replay",
        cause
      )
  val NotCommitted: Outcome = new Outcome("not committed", None)
  val Skipped: Outcome = new Outcome("skipped", None)

  /** Immutable full intent; it never substitutes actions passed to the native writer. */
  private final class Intent(
      transaction: OptimisticTransaction,
      actions: Seq[Action],
      operation: DeltaOperations.Write,
      tags: Map[String, String],
      spark: SparkSession,
      expectedMetadata: Metadata
  ) {
    private val mapper = new ObjectMapper()
      .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
    private val frozenActions = actions.map(action => parse(action.json)).toVector
    private val frozenMetadata =
      if (expectedMetadata != transaction.snapshot.metadata)
        Vector(parse(expectedMetadata.json))
      else Vector.empty
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
    private val originalLog = transaction.deltaLog
    private val originalSnapshot = transaction.snapshot
    private val originalMetadata = transaction.snapshot.metadata
    private val originalProtocol = transaction.snapshot.protocol
    private val originalDescriptor = transaction.catalogTable
    private val nativeId = transaction.txnId.toString
    private val readVersion = transaction.readVersion
    private val engine = s"Apache-Spark/${spark.version} Delta-Lake/${io.delta.VERSION}"
    if (
      readVersion < 0 || frozenActions
        .exists(node => node.size() != 1 || !Set("add", "txn")(node.fieldNames().next()))
    ) denied("unqualified submitted actions")

    def requireTransactionIdentity(): Unit = {
      if (
        (transaction.deltaLog ne originalLog) || (transaction.snapshot ne originalSnapshot) ||
        transaction.txnId.toString != nativeId || transaction.readVersion != readVersion ||
        transaction.catalogTable != originalDescriptor || transaction.snapshot.metadata != originalMetadata ||
        transaction.snapshot.protocol != originalProtocol || transaction.metadata != expectedMetadata ||
        transaction.protocol != originalProtocol || OptimisticTransaction.getActive.exists(
          _ ne transaction
        )
      )
        denied("captured native outcome identity changed")
    }

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
      if (all.tail != frozenMetadata ++ frozenActions)
        denied("published actions differ from submitted intent")
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
