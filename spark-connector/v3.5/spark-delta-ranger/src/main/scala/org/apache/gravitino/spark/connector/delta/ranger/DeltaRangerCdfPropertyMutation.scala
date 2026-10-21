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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaOperations, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.LogicalRDD

/** Owns exactly one native property transaction and its independently verified publication. */
private[ranger] final class DeltaRangerCdfPropertyMutation(
    target: DeltaRangerRegistrationBindings.BoundCdfPropertyMutation,
    spark: SparkSession,
    properties: Map[String, String],
    validateCatalog: () => Unit,
    committed: () => Unit
) extends DeltaRangerNativeOperation.CommitOwner {
  import DeltaRangerCdfPropertyMutation._
  private val structure = new DeltaRangerMetadataStructure("CDF property mutation rejected: ")
  import structure._
  private val thread = Thread.currentThread()
  private val subject = DeltaRangerQuerySubject.current(spark)
  private val settings = spark.sessionState.conf.getAllConfs.toMap
  private val engine = target.engine()
  private val stamp = engine.snapshotStamp()
  private val log = target.log()
  private val descriptor = target.descriptor()
  private val snapshot = target.table().initialSnapshot
  private val readVersion = snapshot.version
  private val originalMetadata = snapshot.metadata
  private val originalProtocol = snapshot.protocol
  private val userMetadata = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_USER_METADATA)
  private val metrics = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)
  private val oldDeltas = snapshot.logSegment.deltas.map(fileStamp).toVector
  private val checkpoints =
    snapshot.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector
  private val version = readVersion + 1
  private val path = new Path(log.logPath, f"$version%020d.json")
  private val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  private var nativeId: String = null
  private var transaction: OptimisticTransaction = null
  private var operation: DeltaRangerNativeOperation = null
  private var recipes: DeltaRangerCommitMetadata = null
  private var publication: Option[(Vector[Any], Vector[Byte])] = None
  private var expectedMetadata: Metadata = null
  private var expectedProtocol: Protocol = null
  private var closed = false
  private var invoked = false
  if (fs.exists(path)) denied("next publication already exists")

  override def session(): SparkSession = spark
  def metadata: DeltaRangerCommitMetadata = {
    requireLive()
    if (recipes == null) denied("transaction not captured")
    recipes
  }
  override def contains(candidate: LogicalRDD): Boolean = metadata.contains(candidate)

  override def requireLive(): Unit = {
    requireContext()
    observePublication()
    target.requirePublicationContext(publication.nonEmpty, version)
    observePublication()
    val actual = spark.sessionState.conf.getAllConfs.toMap
    val after =
      settings.updated("spark.databricks.delta.lastCommitVersionInSession", version.toString)
    if (actual != settings && !(publication.nonEmpty && actual == after))
      denied("configuration changed")
    if (
      transaction != null && ((transaction.deltaLog ne target.log()) ||
        (transaction.snapshot ne snapshot) || transaction.readVersion != snapshot.version ||
        !transaction.catalogTable.exists(_ eq target.descriptor()) ||
        OptimisticTransaction.getActive.exists(_ ne transaction))
    ) denied("transaction changed")
    val head = target.log().unsafeVolatileSnapshot
    if (
      head == null || head.getClass != classOf[Snapshot] || (head.deltaLog ne target.log()) ||
      head.path != target.log().logPath ||
      ((head eq snapshot) && (head.logSegment.deltas.map(fileStamp).toVector != oldDeltas ||
        head.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector != checkpoints)) ||
      ((head ne snapshot) && !(publication.nonEmpty && head.version == version &&
        head.metadata == expectedMetadata && head.protocol == expectedProtocol &&
        head.logSegment.deltas.map(fileStamp).toVector ==
        publication.map(p => oldDeltas :+ p._1).getOrElse(Vector.empty) &&
        head.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector == checkpoints))
    ) denied("native head changed")
    requireContext()
  }

  def run(): Unit = {
    if (
      active.get() != null || DeltaRangerNativeOperation.current() != null ||
      OptimisticTransaction.getActive.nonEmpty || closed
    ) denied("nested mutation")
    DeltaRangerCdfPropertyCommand.validateProperties(properties)
    val validated = DeltaConfigs.validateConfigurations(properties)
    if (validated != properties) denied("native property normalization changed intent")
    active.set(this)
    var primary: Throwable = null
    try {
      requireLive()
      val command = new AlterTableSetPropertiesDeltaCommand(target.table(), validated) {
        override def startTransaction(): OptimisticTransaction = {
          if (transaction != null) denied("repeated transaction capture")
          requireContext()
          val captured = target.captureTransaction(() => {
            requireContext()
            val native = super.startTransaction()
            requireContext()
            native
          })
          requireContext()
          transaction = captured
          nativeId = captured.txnId
          recipes = new DeltaRangerCommitMetadata(
            transaction,
            spark,
            () => requireLive(),
            () => publication.map(_._1),
            () => expectedMetadata,
            () => expectedProtocol
          )
          invoked = true
          operation =
            DeltaRangerNativeOperation.cdfPropertyMutation(DeltaRangerCdfPropertyMutation.this)
          captured
        }
      }
      command.run(spark)
      requireLive()
      if (publication.isEmpty) denied("missing native publication")
      operation.succeed()
    } catch {
      case failure: Throwable =>
        primary = classifyFailure(failure)
        throw primary
    } finally {
      try { if (operation != null) operation.close() }
      catch {
        case cleanup: Throwable =>
          if (primary == null) throw cleanup
          if (cleanup ne primary) primary.addSuppressed(cleanup)
      } finally { closed = true; active.remove() }
    }
  }

  private def requireContext(): Unit = {
    if (
      closed || (active.get() ne this) || (Thread.currentThread() ne thread) ||
      !SparkSession.getActiveSession.exists(_ eq spark)
    ) denied("expired owner")
    validateCatalog()
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("subject changed")
    engine.requireSnapshot(stamp)
  }

  private def observePublication(): Unit = {
    if (!fs.exists(path)) {
      if (publication.nonEmpty) denied("publication disappeared")
      return
    }
    val (observed, metadata, protocol) = readPublication()
    expectedMetadata = metadata
    expectedProtocol = protocol
    publication = Some(observed)
    committed()
  }

  // Classify only the outcome of the captured invocation. This grants no execution authority.
  private def classifyFailure(failure: Throwable): Throwable = {
    if (!invoked) return failure
    try {
      requireOutcomeIdentity()
      val observed = readPublication()
      requireOutcomeIdentity()
      if (readPublication() != observed) denied("native outcome publication changed")
      requireOutcomeIdentity()
      committed()
      failure
    } catch {
      case proof: Throwable =>
        val uncertain = new DeltaRangerCatalogSpark35.CdfAlterOutcomeUncertain(failure)
        if (proof ne failure) uncertain.addSuppressed(proof)
        uncertain
    }
  }

  private def requireOutcomeIdentity(): Unit = {
    if (
      transaction == null || nativeId == null || transaction.txnId != nativeId ||
      (transaction.deltaLog ne log) || (transaction.snapshot ne snapshot) ||
      transaction.readVersion != readVersion || !transaction.catalogTable.exists(_ eq descriptor) ||
      (target.log() ne log) || (target.descriptor() ne descriptor) ||
      (target.table().initialSnapshot ne snapshot) || (snapshot.deltaLog ne log) ||
      snapshot.path != log.logPath || snapshot.version != readVersion ||
      snapshot.metadata != originalMetadata || snapshot.protocol != originalProtocol ||
      snapshot.logSegment.deltas.map(fileStamp).toVector != oldDeltas ||
      snapshot.logSegment.checkpointProvider.topLevelFiles.map(fileStamp).toVector != checkpoints ||
      OptimisticTransaction.getActive.exists(_ ne transaction)
    ) denied("captured native outcome identity changed")
  }

  private def readPublication(): ((Vector[Any], Vector[Byte]), Metadata, Protocol) = {
    if (!invoked || transaction == null) denied("publication precedes native transaction")
    val before = fs.getFileStatus(path)
    if (!before.isFile) denied("publication is not a file")
    val input = fs.open(path)
    val bytes =
      try input.readAllBytes()
      finally input.close()
    val after = fs.getFileStatus(path)
    if (fileStamp(before) != fileStamp(after) || bytes.length.toLong != after.getLen)
      denied("publication changed while reading")
    val metadata = transaction.metadata
    val protocol = transaction.protocol
    if (
      metadata != originalMetadata
        .copy(configuration = originalMetadata.configuration ++ properties)
    )
      denied("unrelated native metadata change")
    if (expectedMetadata != null && (metadata != expectedMetadata || protocol != expectedProtocol))
      denied("native transaction intent changed")
    verifyPublication(bytes, metadata, protocol)
    val observed = fileStamp(after) -> MessageDigest.getInstance("SHA-256").digest(bytes).toVector
    if (publication.exists(_ != observed)) denied("publication bytes or status changed")
    (observed, metadata, protocol)
  }

  private def verifyPublication(
      bytes: Array[Byte],
      metadata: Metadata,
      protocol: Protocol
  ): Unit = {
    val text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString
    val lines = text.split("\n", -1).toVector
    if (lines.size < 3 || lines.last.nonEmpty || lines.init.exists(_.trim.isEmpty))
      denied("incomplete publication")
    val actions = lines.init.map(parse)
    if (
      actions.exists(a => !a.isObject || a.size() != 1) || !actions.head.has("commitInfo") ||
      actions.tail.exists(_.has("commitInfo"))
    ) denied("publication envelopes changed")
    val expected = Vector(parse(metadata.json)) ++
      (if (protocol != originalProtocol) Vector(parse(protocol.json)) else Vector.empty)
    if (actions.tail.size != expected.size || actions.tail.toSet != expected.toSet)
      denied("foreign or unexpected native actions")
    val info = actions.head.get("commitInfo")
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
      (if (userMetadata.nonEmpty) Set("userMetadata") else Set.empty[String])
    if (
      !info.isObject || info.fieldNames().asScala.toSet != required ||
      !info.path("timestamp").isIntegralNumber || !info.path("timestamp").canConvertToLong ||
      info.path("timestamp").longValue() < 0 || !info.path("isBlindAppend").isBoolean ||
      !info.path("isolationLevel").isTextual ||
      !Set("Serializable", "WriteSerializable", "SnapshotIsolation")(
        info.path("isolationLevel").textValue()
      ) ||
      info
        .path("engineInfo")
        .textValue() != s"Apache-Spark/${spark.version} Delta-Lake/${io.delta.VERSION}" ||
      userMetadata.exists(value => info.path("userMetadata").textValue() != value)
    )
      denied("native CommitInfo profile changed")
    val operation = DeltaOperations.SetTableProperties(properties)
    val params = operation.jsonEncodedValues.map { case (key, value) => key -> parse(value) }
    if (
      !info.isObject || !info
        .path("txnId")
        .isTextual || info.path("txnId").textValue() != nativeId ||
      !info
        .path("readVersion")
        .isIntegralNumber || !info.path("readVersion").canConvertToLong ||
      info.path("readVersion").longValue() != readVersion ||
      info.path("operation").textValue() != operation.name || !info
        .path("operationParameters")
        .isObject ||
      info
        .path("operationParameters")
        .fields()
        .asScala
        .map(e => e.getKey -> e.getValue)
        .toMap != params
    )
      denied("foreign native commit identity or intent")
    if (metrics) {
      val observed = info.path("operationMetrics")
      if (
        !observed.isObject || !observed
          .fieldNames()
          .asScala
          .toSet
          .subsetOf(operation.operationMetrics) ||
        observed
          .elements()
          .asScala
          .exists(value => !value.isTextual || !value.textValue().matches("[0-9]+"))
      )
        denied("native operation metrics changed")
    }
  }

  private def parse(json: String): JsonNode = {
    val node = mapper.readTree(json)
    if (node == null) denied("empty publication value")
    node
  }
}

private[ranger] object DeltaRangerCdfPropertyMutation {
  private val active = new ThreadLocal[DeltaRangerCdfPropertyMutation]()
  def current: Option[DeltaRangerCdfPropertyMutation] = Option(active.get())
  private def denied(reason: String): Nothing =
    throw new SecurityException("CDF property mutation rejected: " + reason)
}
