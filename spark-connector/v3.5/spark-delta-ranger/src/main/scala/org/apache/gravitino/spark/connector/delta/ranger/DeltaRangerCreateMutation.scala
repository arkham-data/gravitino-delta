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

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import scala.collection.JavaConverters._
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaOperationMarker,
  DeltaRegistrationCommand,
  DeltaRegistrationReceipt,
  DeltaSchemaContract
}
import org.apache.gravitino.spark.connector.delta.DeltaCreateLifecycle
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Reservation-bound continuation of the base-owned closed native schema CREATE invocation. */
private[ranger] final class DeltaRangerCreateMutation(
    intent: DeltaCreateLifecycle.Intent,
    catalog: DeltaRangerCatalogSpark35,
    spark: SparkSession,
    command: DeltaRangerCreateCommand.Owner,
    declaration: DeltaRangerCreateDeclaration
) extends DeltaCreateLifecycle
    with DeltaRangerNativeOperation.CommitOwner {
  import DeltaRangerCreateMutation._
  private val input = command.externalInput
  private val engine = catalog.policyEngine()
  private val subject = DeltaRangerQuerySubject.current(spark)
  private val key = new DeltaRangerResourceKey(
    intent.catalog(),
    intent.namespace().get(0),
    intent.table(),
    null,
    "create",
    null
  )
  private val structure = new DeltaRangerMetadataStructure("Schema CREATE rejected: ")
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  private var receipt: DeltaRegistrationReceipt = null
  private var operation: DeltaRangerNativeOperation = null
  private var transaction: OptimisticTransaction = null
  private var snapshot: Snapshot = null
  private var nativeId: String = null
  private var expectedMetadata: Metadata = null
  private var expectedProtocol: Protocol = null
  private var recipes: DeltaRangerCommitMetadata = null
  private var empty: DeltaRangerCreateMetadata = null
  private var publication: Option[(Vector[Any], Vector[Byte])] = None
  private var reserved = false
  private var nativeEntered = false
  private var nativeClosed = false
  private var closed = false
  private var published = false
  private var publicationChecked = false
  private var confirmed = false

  def nativeConfirmed: Boolean = confirmed
  def publishedCreate: Boolean = published
  def requireExternalPublication(candidate: DeltaRangerMetadataInput): Unit = {
    context()
    if (
      !input.exists(_ eq candidate) || !closed || !published || !confirmed ||
      !nativeClosed || !publicationChecked || OptimisticTransaction.getActive.nonEmpty
    )
      denied("unconfirmed external CREATE publication")
    catalog.requireCreateMetadataLive()
    observePublication()
    val head = transaction.deltaLog.unsafeVolatileSnapshot
    if (head.version != 0 || head.metadata != expectedMetadata || head.protocol != expectedProtocol)
      denied("external CREATE head changed")
  }
  override def session(): SparkSession = spark

  override def beforeReserve(
      mode: DeltaSchemaContract.OperationMode,
      schemaJson: String,
      options: java.util.Map[String, String]
  ): Unit = {
    if (
      reserved || nativeEntered || closed || mode != DeltaSchemaContract.OperationMode.SCHEMA_ONLY ||
      mapper.readTree(schemaJson) != mapper.readTree(intent.schemaJson()) || !options.isEmpty
    )
      denied("materialization differs from issued declaration")
    input.foreach(_.requireLive())
    authorize()
    reserved = true
  }
  override def reserved(value: DeltaRegistrationReceipt): Unit = {
    context()
    if (
      !reserved || receipt != null || closed || value
        .state() != DeltaRegistrationReceipt.State.RESERVED ||
      value.operation() != DeltaRegistrationCommand.Operation.RESERVE || value
        .identity()
        .generation() != 0 ||
      !value.marker().isPresent
    ) denied("invalid reservation")
    val marker = value.marker().get()
    if (
      marker.operation() != DeltaOperationMarker.Operation.CREATE || marker.generation() != 0 ||
      marker.id() != value.identity().id() || marker.token() != value.identity().token() ||
      marker.schemaContract().isPresent || mapper
        .readTree(marker.schemaJson()) != mapper.readTree(intent.schemaJson())
    )
      denied("reservation intent changed")
    receipt = value
  }
  override def beforeNative(value: DeltaRegistrationReceipt): DeltaCreateLifecycle.Scope = {
    if (
      (value ne receipt) || receipt == null || nativeEntered || closed || current.nonEmpty ||
      DeltaRangerNativeOperation.current() != null || OptimisticTransaction.getActive.nonEmpty
    )
      denied("unbound native invocation")
    authorize()
    if (
      current.nonEmpty || DeltaRangerNativeOperation
        .current() != null || OptimisticTransaction.getActive.nonEmpty
    )
      denied("native context changed during authorization")
    input.foreach(_.requireLive())
    nativeEntered = true
    active.set(this)
    try operation = DeltaRangerNativeOperation.commit(this)
    catch {
      case failure: Throwable =>
        // The base has not received a scope and cannot have invoked native commit.
        nativeEntered = false
        active.remove()
        throw failure
    }
    new DeltaCreateLifecycle.Scope {
      override def close(): Unit = {
        if (nativeClosed) return
        try { if (transaction != null) observePublication() }
        finally {
          try operation.close()
          finally { nativeClosed = true; active.remove() }
        }
      }
    }
  }
  override def beforePublish(value: DeltaRegistrationReceipt): Unit = {
    if ((value ne receipt) || !nativeClosed || !confirmed || closed)
      denied("native completion unconfirmed")
    authorize()
    observePublication()
    publicationChecked = true
  }
  override def published(value: DeltaRegistrationReceipt): Unit = {
    if (
      closed || value.state() != DeltaRegistrationReceipt.State.PUBLISHED ||
      value.identity() != receipt.identity() || value.runId() != receipt.runId() ||
      !value.nativeVersion().isPresent || value.nativeVersion().getAsLong != 0 ||
      !value.nativeId().isPresent || value.nativeId().get() != expectedMetadata.id
    )
      denied("publication receipt differs")
    published = true
  }
  override def close(): Unit = {
    if (closed) return
    try { if (operation != null && !nativeClosed) operation.close() }
    finally { if (active.get() eq this) active.remove(); closed = true }
  }

  override def requireLive(): Unit = {
    context()
    if (closed || !nativeEntered || nativeClosed || (active.get() ne this))
      denied("expired native owner")
    authorize()
    if (transaction != null) {
      if (
        (transaction.snapshot ne snapshot) || transaction.readVersion != -1 ||
        transaction.txnId != nativeId || !OptimisticTransaction.getActive.exists(_ eq transaction)
      )
        denied("foreign transaction")
      observePublication()
    }
  }
  def metadata: DeltaRangerCommitMetadata = {
    requireLive()
    if (recipes == null) {
      val actual = OptimisticTransaction.getActive.getOrElse(denied("missing native transaction"))
      if (
        actual.getClass != classOf[OptimisticTransaction] || actual.readVersion != -1 ||
        actual.snapshot.version != -1 || actual.catalogTable.nonEmpty ||
        actual.snapshot.getClass != Class.forName(
          "org.apache.spark.sql.delta.DummySnapshot",
          false,
          classOf[Snapshot].getClassLoader
        ) ||
        !DeltaRangerRegistrationBindings.locationMatches(
          receipt.marker().get().normalizedLocation(),
          actual.deltaLog.dataPath.toUri
        ) ||
        actual.snapshot.logSegment.deltas.nonEmpty || actual.snapshot.logSegment.checkpointProvider.topLevelFiles.nonEmpty
      )
        denied("foreign initial transaction")
      transaction = actual
      snapshot = actual.snapshot
      nativeId = actual.txnId
      expectedMetadata = actual.metadata
      expectedProtocol = actual.protocol
      observePublication()
      empty = new DeltaRangerCreateMetadata(spark, () => requireLive())
      recipes = new DeltaRangerCommitMetadata(
        actual,
        spark,
        () => requireLive(),
        () => publication.map(_._1),
        () => expectedMetadata,
        () => expectedProtocol,
        candidate => empty.contains(candidate)
      )
    }
    recipes
  }
  override def contains(candidate: LogicalRDD): Boolean = {
    metadata
    empty.contains(candidate)
  }
  def prefix(plan: LogicalPlan): Boolean = {
    metadata
    empty.prefix(plan)
  }

  def failure(cause: Throwable): Throwable = {
    if (!nativeEntered) return cause
    val native = if (confirmed) "CREATE0_CONFIRMED" else "UNKNOWN"
    val publication =
      if (published) "PUBLISHED" else if (publicationChecked) "STATUS_NEEDED" else "NOT_ATTEMPTED"
    new IllegalStateException(
      s"Schema CREATE failed: native=$native publication=$publication; do not replay CREATE",
      cause
    )
  }

  private def context(): Unit = command.requireMutation(this)

  private def authorize(): Unit = {
    context()
    if (closed || (catalog.policyEngine() ne engine)) denied("expired catalog")
    // This fixed schema-only operation has no policy-derived plan. Each check reauthorizes the
    // same literal CREATE; the engine requires a stable current publication during evaluation.
    engine.authorize(subject, key)
    context()
  }
  private def observePublication(): Unit = {
    if (transaction == null || receipt == null) denied("missing captured transaction")
    val marker = receipt.marker().get()
    if (
      (transaction.snapshot ne snapshot) || transaction.txnId != nativeId ||
      transaction.metadata != expectedMetadata || transaction.protocol != expectedProtocol ||
      transaction.readVersion != -1 || snapshot.version != -1 ||
      !DeltaRangerRegistrationBindings.locationMatches(
        marker.normalizedLocation(),
        transaction.deltaLog.dataPath.toUri
      )
    )
      denied("native identity changed")
    val expected = declaration.configuration ++
      Map(
        DeltaOperationMarker.PROPERTY_KEY -> marker.encode(),
        "delta.columnMapping.mode" -> "none"
      )
    if (
      mapper.readTree(expectedMetadata.schemaString) != mapper.readTree(intent.schemaJson()) ||
      expectedMetadata.configuration != expected || expectedMetadata.partitionColumns.toVector != declaration.partitionColumns ||
      Option(
        expectedMetadata.description
      ) != declaration.description || expectedMetadata.format.provider != "parquet" ||
      expectedMetadata.format.options.nonEmpty || expectedMetadata.id.isEmpty
    )
      denied("native metadata differs from reservation")
    val path = new Path(transaction.deltaLog.logPath, "00000000000000000000.json")
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(new Path(transaction.deltaLog.logPath, "00000000000000000001.json")))
      denied("native version advanced beyond CREATE0")
    val before = fs.getFileStatus(path)
    if (!before.isFile) denied("CREATE0 missing")
    val input = fs.open(path)
    val bytes =
      try input.readAllBytes()
      finally input.close()
    val after = fs.getFileStatus(path)
    val file = structure.fileStamp(after)
    if (structure.fileStamp(before) != file || bytes.length.toLong != after.getLen)
      denied("CREATE0 changed while reading")
    val lines = new String(bytes, UTF_8).split("\n", -1).toVector
    if (lines.size != 4 || lines.last.nonEmpty) denied("unexpected CREATE0 actions")
    val actions = lines.init.map(mapper.readTree)
    val info = actions.head.path("commitInfo")
    if (
      actions.head.fieldNames().asScala.toSet != Set("commitInfo") ||
      actions.tail.toSet != Set(
        mapper.readTree(expectedMetadata.json),
        mapper.readTree(expectedProtocol.json)
      ) ||
      info.path("operation").asText() != "CREATE TABLE" || info
        .path("txnId")
        .asText() != nativeId ||
      info.has("readVersion") || info.has("userMetadata") ||
      mapper.readTree(info.path("operationParameters").path("partitionBy").asText()) !=
        mapper.readTree(mapper.writeValueAsString(declaration.partitionColumns.asJava)) ||
        info.path("operationParameters").path("clusterBy").asText() != "[]" ||
        mapper.readTree(info.path("operationParameters").path("properties").asText()) !=
        mapper.readTree(mapper.writeValueAsString(expected.asJava))
    )
      denied("CREATE0 does not correlate to the captured invocation")
    val observed = file -> MessageDigest.getInstance("SHA-256").digest(bytes).toVector
    if (publication.exists(_ != observed)) denied("CREATE0 publication drift")
    val head = transaction.deltaLog.unsafeVolatileSnapshot
    if (
      head == null || (head.deltaLog ne transaction.deltaLog) ||
      ((head ne snapshot) && (head.getClass != classOf[Snapshot] || head.version != 0 ||
        head.metadata != expectedMetadata || head.protocol != expectedProtocol ||
        head.logSegment.deltas.map(structure.fileStamp).toVector != Vector(file) ||
        head.logSegment.checkpointProvider.topLevelFiles.nonEmpty))
    )
      denied("native snapshot advanced or changed")
    publication = Some(observed)
    confirmed = true
  }
  private def denied(reason: String): Nothing =
    throw new SecurityException("Schema CREATE rejected: " + reason)
}
private[ranger] object DeltaRangerCreateMutation {
  private val active = new ThreadLocal[DeltaRangerCreateMutation]()
  def current: Option[DeltaRangerCreateMutation] = Option(active.get())
}
