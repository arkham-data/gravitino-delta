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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Private durable evidence. No journal state grants query or publication authority. */
private[ranger] final class DeltaRangerMergeJournal(
    directory: Path,
    identity: Option[(String, Long)],
    command: String
) {
  import DeltaRangerMergeJournal._
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
  private var retained: Option[Entry] = read()
  retained.foreach(entry =>
    if (entry.command != command) rejected("transaction identity intent changed")
  )

  def existing: Option[Entry] = retained

  def prepare(proof: String, version: Long): Unit = {
    if (retained.nonEmpty || Files.exists(file, LinkOption.NOFOLLOW_LINKS))
      rejected("attempt already recorded")
    save(Entry(Intent(command, proof, version), Prepared, None), initial = true)
  }

  def committed(bytes: Array[Byte]): Unit = {
    val entry = retained.getOrElse(rejected("missing prepared intent"))
    if (entry.state == Unknown) rejected("unknown publication")
    DeltaRangerMergeCommit.verifyPublication(bytes, entry.proof)
    val digest = hash(bytes)
    if (entry.digest.exists(_ != digest)) rejected("publication digest changed")
    if (entry.state == Prepared) save(entry.copy(state = Committed, digest = Some(digest)))
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
      identity.nonEmpty || entry.state != Prepared || entry.digest.nonEmpty ||
      mapper.readTree(entry.proof).get("actions").size() != 0
    ) rejected("invalid native no-op")
    save(entry.copy(state = Reconciled, skipped = true))
  }

  def unknown(): Unit = retained.foreach { entry =>
    if (entry.state == Prepared) save(entry.copy(state = Unknown))
  }

  private def read(): Option[Entry] = {
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
    val node = mapper.readTree(bytes)
    if (
      node == null || !node.isObject || node.fieldNames().asScala.toSet !=
        Set("format", "command", "proof", "version", "state", "digest", "skipped") ||
        !node.get("format").isIntegralNumber || node.get("format").asInt(-1) != 1 || !node
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
    Some(
      Entry(
        Intent(
          node.get("command").textValue(),
          node.get("proof").textValue(),
          node.get("version").longValue()
        ),
        state,
        digest,
        skipped
      )
    )
  }

  private def save(entry: Entry, initial: Boolean = false): Unit = {
    if (!initial && retained.isEmpty) rejected("missing owned claim")
    val node = mapper.createObjectNode()
    node.put("format", 1); node.put("command", entry.command); node.put("proof", entry.proof)
    node.put("version", entry.version); node.put("state", entry.state.name);
    node.put("digest", entry.digest.orNull)
    node.put("skipped", entry.skipped)
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
      skipped: Boolean = false
  ) {
    def command: String = intent.command
    def proof: String = intent.proof
    def version: Long = intent.version
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
