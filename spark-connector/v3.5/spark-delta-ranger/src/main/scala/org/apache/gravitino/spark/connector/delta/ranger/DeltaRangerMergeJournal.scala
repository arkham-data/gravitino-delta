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
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{
  FileAlreadyExistsException,
  Files,
  LinkOption,
  Path,
  StandardCopyOption,
  StandardOpenOption
}
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.UUID
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Private durable evidence. No journal state grants query or publication authority. */
private[ranger] final class DeltaRangerMergeJournal private (
    directory: Path,
    identity: Option[(String, Long)],
    command: String,
    evolution: Option[DeltaRangerMergeEvolutionIntent]
) {
  import DeltaRangerMergeJournal._

  /** Existing fixed-schema constructor retains its format-1 contract. */
  def this(directory: Path, identity: Option[(String, Long)], command: String) =
    this(directory, identity, command, None)
  private val root = directory.resolve("merge")
  private val permissions = PosixFilePermissions.fromString("rwx------")
  if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS))
    Files.createDirectory(root, PosixFilePermissions.asFileAttribute(permissions))
  if (
    Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) ||
    Files.getPosixFilePermissions(root, LinkOption.NOFOLLOW_LINKS) != permissions ||
    Files.getOwner(root) != Files.getOwner(directory)
  ) rejected("invalid private directory")
  private val parent = FileChannel.open(directory, StandardOpenOption.READ)
  try parent.force(true)
  finally parent.close()
  private val key = identity
    .map { case (app, version) => hash(vector(Vector(app, version.toString))) }
    .getOrElse(UUID.randomUUID().toString)
  private val file = root.resolve(key + ".json")
  evolution.foreach { value =>
    if (
      value.fingerprint != command || value.transactionIdentity != identity ||
      value.baseMetadata == value.expectedMetadata
    ) rejected("invalid evolving command binding")
  }
  private var retained: Option[Entry] = readFile(root, file, identity)
  retained.foreach(entry =>
    if (entry.command != command || entry.evolution.map(_.encoded) != evolution.map(_.encoded))
      rejected("transaction identity intent changed")
  )

  def existing: Option[Entry] = retained

  def prepare(proof: String, version: Long): Unit = {
    if (retained.nonEmpty || Files.exists(file, LinkOption.NOFOLLOW_LINKS))
      rejected("attempt already recorded")
    if (evolution.isEmpty && mapper.readTree(proof).has("expectedMetadata"))
      rejected("fixed-schema proof cannot publish metadata")
    evolution.foreach(_.requireProofBinding(proof, version))
    save(
      Entry(Intent(command, proof, version), Prepared, None, evolution = evolution),
      initial = true
    )
  }

  def committed(bytes: Array[Byte]): Unit = {
    val digest = verifiedDigest(bytes)
    val entry = retained.get
    if (entry.state == Prepared) save(entry.copy(state = Committed, digest = Some(digest)))
  }

  /** Detached exact-publication evidence; validation does not acknowledge or authorize execution.
    */
  def verifiedDigest(bytes: Array[Byte]): String = {
    val entry = retained.getOrElse(rejected("missing prepared intent"))
    if (entry.state == Unknown) rejected("unknown publication")
    if (entry.evolution.isEmpty && mapper.readTree(entry.proof).has("expectedMetadata"))
      rejected("fixed-schema proof cannot publish metadata")
    entry.evolution.foreach { value =>
      value.requireProofBinding(entry.proof, entry.version)
    }
    DeltaRangerMergeCommit.verifyPublication(bytes, entry.proof)
    val digest = hash(bytes)
    if (entry.digest.exists(_ != digest)) rejected("publication digest changed")
    digest
  }

  def reconciled(): Unit = {
    val entry = retained.getOrElse(rejected("missing committed intent"))
    if (!Set[Phase](Committed, Reconciled)(entry.state) || entry.digest.isEmpty)
      rejected("unproven reconciliation")
    if (entry.state != Reconciled) save(entry.copy(state = Reconciled))
  }

  def skipped(): Unit = {
    val entry = retained.getOrElse(rejected("missing prepared no-op"))
    if (
      entry.evolution.nonEmpty || identity.nonEmpty || entry.state != Prepared || entry.digest.nonEmpty ||
      mapper.readTree(entry.proof).get("actions").size() != 0
    ) rejected("invalid native no-op")
    save(entry.copy(state = Reconciled, skipped = true))
  }

  def unknown(): Unit = retained.foreach { entry =>
    if (entry.state == Prepared) save(entry.copy(state = Unknown))
  }

  private def save(entry: Entry, initial: Boolean = false): Unit = {
    if (!initial && retained.isEmpty) rejected("missing owned claim")
    val node = mapper.createObjectNode()
    node.put("format", if (entry.evolution.isDefined) 2 else 1); node.put("command", entry.command);
    node.put("proof", entry.proof)
    node.put("version", entry.version); node.put("state", entry.state.name);
    node.put("digest", entry.digest.orNull)
    node.put("skipped", entry.skipped)
    entry.evolution.foreach { value =>
      node.put("kind", "additive-merge-v1")
      node.set[JsonNode]("evolution", mapper.readTree(value.encoded))
    }
    val bytes = mapper.writeValueAsBytes(node)
    if (bytes.length > MaxBytes) rejected("evidence exceeds bounded encoding")
    val temporary = Files.createTempFile(
      root,
      ".merge-",
      ".tmp",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
    )
    try {
      val channel = FileChannel.open(temporary, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
      try {
        val buffer = ByteBuffer.wrap(bytes); while (buffer.hasRemaining) channel.write(buffer);
        channel.force(true)
      } finally channel.close()
      if (initial) {
        // Link creation atomically claims an absent key without replacing another writer's intent.
        try Files.createLink(file, temporary)
        catch {
          case _: FileAlreadyExistsException => rejected("attempt already recorded")
        }
      } else {
        Files.move(
          temporary,
          file,
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING
        )
      }
      val folder = FileChannel.open(root, StandardOpenOption.READ)
      try folder.force(true)
      finally folder.close()
      retained = Some(entry)
    } finally Files.deleteIfExists(temporary)
  }
}

private[ranger] object DeltaRangerMergeJournal {
  sealed abstract class Phase(val name: String)
  case object Prepared extends Phase("PREPARED")
  case object Committed extends Phase("COMMITTED")
  case object Reconciled extends Phase("RECONCILED")
  case object Unknown extends Phase("UNKNOWN")
  final case class Intent(command: String, proof: String, version: Long)
  final case class Entry(
      intent: Intent,
      state: Phase,
      digest: Option[String],
      skipped: Boolean = false,
      evolution: Option[DeltaRangerMergeEvolutionIntent] = None
  ) {
    def command: String = intent.command
    def proof: String = intent.proof
    def version: Long = intent.version
  }
  private def readFile(root: Path, file: Path, identity: Option[(String, Long)]): Option[Entry] = {
    if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) return None
    if (
      Files.isSymbolicLink(file) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) ||
      Files.size(file) > MaxBytes || Files.getPosixFilePermissions(file) != PosixFilePermissions
        .fromString("rw-------") ||
      Files.getOwner(file) != Files.getOwner(root)
    ) rejected("invalid evidence file")
    val input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)
    val bytes =
      try input.readNBytes(MaxBytes + 1)
      finally input.close()
    if (bytes.length > MaxBytes) rejected("evidence exceeds bounded encoding")
    val node =
      try mapper.readTree(bytes)
      catch {
        case NonFatal(error) =>
          throw new SecurityException("Delta MERGE journal rejected: invalid evidence JSON", error)
      }
    val format = Option(node).flatMap(n => Option(n.get("format")))
    if (!format.exists(n => n.isIntegralNumber && n.canConvertToInt && Set(1, 2)(n.intValue())))
      rejected("invalid evidence format")
    val evolving = format.get.intValue() == 2
    if (
      node == null || !node.isObject || node.fieldNames().asScala.toSet !=
        (Set("format", "command", "proof", "version", "state", "digest", "skipped") ++
          (if (evolving) Set("kind", "evolution") else Set.empty[String])) || !node
          .get("version")
          .isIntegralNumber ||
        !node.get("version").canConvertToLong || node.get("version").longValue() < 0 ||
        Seq("command", "proof", "state").exists(k => !node.get(k).isTextual) ||
        !(node.get("digest").isNull || node.get("digest").isTextual)
    ) rejected("invalid evidence encoding")
    val state = node.get("state").textValue() match {
      case "PREPARED"   => Prepared
      case "COMMITTED"  => Committed
      case "RECONCILED" => Reconciled
      case "UNKNOWN"    => Unknown
      case _            => rejected("invalid evidence state")
    }
    if (!node.get("skipped").isBoolean) rejected("invalid no-op encoding")
    val skipped = node.get("skipped").booleanValue()
    val digest = Option(node.get("digest")).filterNot(_.isNull).map(_.textValue())
    if (
      digest.exists(!_.matches("[0-9a-f]{64}")) ||
      (Set[Phase](Committed, Reconciled)(state) && digest.isEmpty && !skipped) ||
      (skipped && (state != Reconciled || digest.nonEmpty || identity.nonEmpty))
    ) rejected("invalid evidence state")
    val evolution = if (evolving) {
      if (
        !node
          .get("kind")
          .isTextual || node.get("kind").textValue() != "additive-merge-v1" || skipped ||
        (Set[Phase](Prepared, Unknown)(state) && digest.nonEmpty)
      ) rejected("invalid evolution state")
      val value = DeltaRangerMergeEvolutionIntent.decode(node.get("evolution").toString)
      if (
        value.fingerprint != node
          .get("command")
          .textValue() || value.transactionIdentity != identity ||
        value.baseMetadata == value.expectedMetadata
      ) rejected("invalid evolution command binding")
      value.requireProofBinding(node.get("proof").textValue(), node.get("version").longValue())
      Some(value)
    } else None
    Some(
      Entry(
        Intent(
          node.get("command").textValue(),
          node.get("proof").textValue(),
          node.get("version").longValue()
        ),
        state,
        digest,
        skipped,
        evolution
      )
    )
  }

  /** Strict read-only lookup: absent identity or file never creates a claim or a directory. */
  def peek(directory: Path, identity: Option[(String, Long)]): Option[Entry] = {
    if (identity.isEmpty) return None
    val root = directory.resolve("merge")
    if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) return None
    if (
      Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS) ||
      Files.getPosixFilePermissions(root, LinkOption.NOFOLLOW_LINKS) != PosixFilePermissions
        .fromString("rwx------") ||
      Files.getOwner(root) != Files.getOwner(directory)
    ) rejected("invalid private directory")
    val (app, version) = identity.get
    readFile(root, root.resolve(hash(vector(Vector(app, version.toString))) + ".json"), identity)
  }

  def evolving(directory: Path, intent: DeltaRangerMergeEvolutionIntent): DeltaRangerMergeJournal =
    new DeltaRangerMergeJournal(
      directory,
      intent.transactionIdentity,
      intent.fingerprint,
      Some(intent)
    )

  /** Reopen only the exact peeked record after comparison with fresh, independently captured data.
    */
  def recovering(
      directory: Path,
      entry: Entry,
      candidate: DeltaRangerMergeEvolutionIntent
  ): DeltaRangerMergeJournal = {
    val intent = entry.evolution.getOrElse(rejected("missing evolution evidence"))
    if (entry.state == Unknown || entry.skipped || intent.transactionIdentity.isEmpty)
      rejected("unknown evolution recovery")
    intent.requireCurrent(candidate)
    val current =
      peek(directory, intent.transactionIdentity).getOrElse(rejected("missing retained evolution"))
    def exact(value: Entry): Boolean = value.intent == entry.intent && value.state == entry.state &&
      value.digest == entry.digest && value.skipped == entry.skipped && value.evolution.map(
        _.encoded
      ) == entry.evolution.map(_.encoded)
    if (!exact(current)) rejected("peeked evidence changed")
    val journal = evolving(directory, intent)
    if (!journal.existing.exists(exact)) rejected("reopened evidence changed")
    journal
  }

  private val MaxBytes = 16 * 1024 * 1024
  private val mapper = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  def hash(value: String): String = hash(value.getBytes(StandardCharsets.UTF_8))
  def hash(value: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(value).map(b => f"${b & 0xff}%02x").mkString
  def vector(values: Seq[String]): String = {
    val node = mapper.createArrayNode(); values.foreach(node.add); node.toString
  }
  def plan(value: LogicalPlan): String = vector(
    Vector(
      value.nodeName,
      vector(value.output.map(a => vector(Seq(a.name, a.dataType.json, a.nullable.toString)))),
      vector(value.expressions.map(_.sql)),
      vector(value.children.map(plan))
    )
  )
  private def rejected(reason: String): Nothing = throw new SecurityException(
    "Delta MERGE journal rejected: " + reason
  )
}
