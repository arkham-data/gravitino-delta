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

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}

import java.lang.{Boolean => BoxedBoolean}
import java.util.IdentityHashMap

import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawDeltaLogCache.CacheObservation
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawMaintenanceDeployment.RegistrationObservation
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, LogSegment, Snapshot}
import org.apache.spark.sql.delta.actions.{Format, Metadata, Protocol}
import org.apache.spark.sql.delta.files.{ShallowSnapshotDescriptor, TahoeLogFileIndex}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Inert association with an already installed stock head and every live deployment observation.
  * Checks sample mutable state optimistically; they are neither atomic nor ABA-proof fences, and do
  * not prove the head is the latest storage version. No state/stats/reconstruction/query authority
  * is granted. Unsupported registrations reject the whole attempt rather than disappearing from it.
  */
private[ranger] object DeltaRangerRawNativeObservation {
  private val schemaMapper =
    new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  def observe(session: SparkSession, index: TahoeLogFileIndex): Observation = sanitized {
    DeltaRangerRawDeltaLogCache.requireSession(session)
    exact(index, classOf[TahoeLogFileIndex])
    val registrations =
      DeltaRangerRawMaintenanceDeployment.observeRegistrations(session).asScala.toVector
    val candidates = registrations.map { registration =>
      registration.requireCurrent(session)
      registration -> DeltaRangerRawDeltaLogCache.observe(session, registration.publication())
    }
    val descriptor = candidate(index)
    val matches = candidates.filter { case (_, cache) => cache.log() eq index.deltaLog }
    if (matches.size != 1) reject()
    val (registration, cache) = matches.head
    val log = cache.log()
    val head = log.unsafeVolatileSnapshot
    exact(head, classOf[Snapshot])
    val segment = head.logSegment
    exact(segment, classOf[LogSegment])
    val evidence =
      new Observation(session, index, descriptor, log, head, segment, registration, candidates)
    evidence.requireCurrent(session)
    evidence
  }

  final class Observation private[DeltaRangerRawNativeObservation] (
      private val session: SparkSession,
      private val index: TahoeLogFileIndex,
      private val descriptor: ShallowSnapshotDescriptor,
      private val log: DeltaLog,
      private val head: Snapshot,
      private val segment: LogSegment,
      private val registration: RegistrationObservation,
      private val candidates: Vector[(RegistrationObservation, CacheObservation)]
  ) {
    private val peerProfile = DeltaRangerRawDeltaLogCache.Profile.capture(index.spark)
    private val metadataJson = checkedMetadata(head.metadata)
    private val protocolJson = checkedProtocol(head.protocol)
    private val selectionDeltaStamps = segment.deltas
      .map(file => (file.getPath, file.getLen, file.getModificationTime, file.isDirectory))
      .toVector
    private val selectionCheckpointStamps = segment.checkpointProvider.topLevelFiles
      .map(file => (file.getPath, file.getLen, file.getModificationTime, file.isDirectory))
      .toVector
    private val selectionDeltas = selectionFiles(segment.deltas)
    private val selectionCheckpoints = selectionFiles(segment.checkpointProvider.topLevelFiles)
    private val selectionCheckpointVersion = segment.checkpointProvider.version
    private val selectionCheckpointClass = segment.checkpointProvider.getClass
    val analysisVersion: Long = descriptor.version
    val installedVersion: Long = head.version

    private[ranger] def rawIndex: TahoeLogFileIndex = index
    private[ranger] def selectedHead: Snapshot = head
    private[ranger] def selectedSegment: LogSegment = segment

    def requireCurrent(actual: SparkSession): Unit = requireCurrent(actual, null)

    private[ranger] def requireCurrent(
        actual: SparkSession,
        owner: DeltaRangerRawNativeScans
    ): Unit = sanitized {
      requireRetained(actual, owner)
      if (log.unsafeVolatileSnapshot ne head) reject()
    }

    private def requireRetained(actual: SparkSession, owner: DeltaRangerRawNativeScans): Unit = {
      if (owner != null && (owner.observation ne this)) reject()
      if (actual ne session) reject()
      DeltaRangerRawDeltaLogCache.requireSession(actual, owner)
      candidates.foreach { case (registration, cache) =>
        registration.requireCurrent(actual)
        cache.requireCurrent(actual, owner)
      }
      val current = candidate(index, owner)
      if (
        (current ne descriptor) || (index.deltaLog ne log) || (descriptor.deltaLog ne log) ||
        (head.deltaLog ne log) || (head.logSegment ne segment) ||
        head.version != installedVersion || segment.version != installedVersion ||
        installedVersion < registration.publication().nativeVersion() || analysisVersion < 0 ||
        analysisVersion > installedVersion
      ) reject()
      val cache = candidates.find(_._1 eq registration).get._2
      peerProfile.requireCurrent(index.spark, owner)
      cache.requirePeer(index.spark, owner)
      if (!index.path.equals(cache.dataPath())) reject()
      if (
        checkedMetadata(descriptor.metadata) != metadataJson || checkedProtocol(
          descriptor.protocol
        ) != protocolJson ||
        checkedMetadata(head.metadata) != metadataJson || checkedProtocol(
          head.protocol
        ) != protocolJson ||
        descriptor.metadata.schemaString != head.metadata.schemaString ||
        head.metadata.id != registration.publication().nativeId() ||
        logicalSchema(head.metadata.schemaString) != logicalSchema(
          registration.publication().schemaJson()
        )
      ) reject()
      candidates.foreach { case (registration, cache) =>
        registration.requireCurrent(actual)
        cache.requireCurrent(actual, owner)
      }
    }

    private[ranger] def afterSelection(
        actual: SparkSession,
        result: DeltaRangerRawNativeScans.SelectionResult
    ): Observation = sanitized {
      if (result.input ne this) reject()
      requireRetained(actual, null)
      val selected = result.selected
      exact(selected, classOf[Snapshot])
      if (
        (selected.deltaLog ne log) || (log.unsafeVolatileSnapshot ne selected) ||
        selected.version != installedVersion || checkedMetadata(
          selected.metadata
        ) != metadataJson ||
        checkedProtocol(selected.protocol) != protocolJson ||
        segment.deltas
          .map(file => (file.getPath, file.getLen, file.getModificationTime, file.isDirectory))
          .toVector != selectionDeltaStamps ||
        segment.checkpointProvider.topLevelFiles
          .map(file => (file.getPath, file.getLen, file.getModificationTime, file.isDirectory))
          .toVector != selectionCheckpointStamps
      )
        reject()
      val next = selected.logSegment
      exact(next, classOf[LogSegment])
      if (
        next.version != installedVersion || next.logPath != segment.logPath ||
        next.checkpointProvider.getClass != selectionCheckpointClass ||
        next.checkpointProvider.version != selectionCheckpointVersion ||
        selectionFiles(next.deltas) != selectionDeltas ||
        selectionFiles(next.checkpointProvider.topLevelFiles) != selectionCheckpoints
      )
        reject()
      val fresh = observe(actual, index)
      if (fresh.selectedHead ne selected) reject()
      requireRetained(actual, null)
      fresh.requireCurrent(actual)
      fresh
    }

    private def selectionFiles(files: Seq[org.apache.hadoop.fs.FileStatus]): Vector[Any] =
      files.map(file => Vector(file.getPath, file.getLen, file.isDirectory)).toVector

    override def toString: String = "Raw native installed-head observation"
  }

  private def candidate(
      index: TahoeLogFileIndex,
      owner: DeltaRangerRawNativeScans = null
  ): ShallowSnapshotDescriptor = {
    exact(index, classOf[TahoeLogFileIndex])
    // These accessors read stock fields. Qualify each supplied value before invoking its behavior.
    DeltaRangerRawDeltaLogCache.requireSession(index.spark, owner)
    DeltaRangerRawDeltaLogCache.requireRuntime(index.path, classOf[Path], classOf[Path])
    exact(index.deltaLog, classOf[DeltaLog])
    if (index.partitionFilters.asInstanceOf[AnyRef] ne Nil) reject()
    if (index.isTimeTravelQuery) reject()
    val descriptor = index.snapshotAtAnalysis
    exact(descriptor, classOf[ShallowSnapshotDescriptor])
    val shallow = descriptor.asInstanceOf[ShallowSnapshotDescriptor]
    exact(shallow.deltaLog, classOf[DeltaLog])
    checkedMetadata(shallow.metadata)
    checkedProtocol(shallow.protocol)
    shallow
  }

  private def exact(value: AnyRef, expected: Class[_]): Unit =
    DeltaRangerRawDeltaLogCache.requireRuntime(value, expected, classOf[DeltaLog])

  // Finite Delta action fields only. No arbitrary Product traversal or generic graph validator.
  private def checkedMetadata(value: Metadata): String = {
    exact(value, classOf[Metadata])
    if (value.id == null || value.schemaString == null) reject()
    exact(value.format, classOf[Format])
    if (value.format.provider == null) reject()
    stringsMap(value.format.options)
    stringsMap(value.configuration)
    stringSequence(value.partitionColumns)
    stockOption(value.createdTime.asInstanceOf[Option[AnyRef]]).foreach(v =>
      if (v == null || v.getClass != classOf[java.lang.Long]) reject()
    )
    value.json
  }

  private def checkedProtocol(value: Protocol): String = {
    exact(value, classOf[Protocol])
    Seq(value.readerFeatures, value.writerFeatures).foreach { option =>
      stockOption(option).foreach { set =>
        val name = set.getClass.getName
        if (
          !Set(
            "scala.collection.immutable.Set$EmptySet$",
            "scala.collection.immutable.Set$Set1",
            "scala.collection.immutable.Set$Set2",
            "scala.collection.immutable.Set$Set3",
            "scala.collection.immutable.Set$Set4",
            "scala.collection.immutable.HashSet$HashSet1",
            "scala.collection.immutable.HashSet$HashTrieSet"
          ).contains(name)
        ) reject()
        setShape(set, new IdentityHashMap[AnyRef, BoxedBoolean]())
        set.iterator.foreach(value =>
          if (value == null || value.getClass != classOf[String]) reject()
        )
      }
    }
    value.json
  }

  private def scalaStock(value: AnyRef): Unit =
    DeltaRangerRawDeltaLogCache.requireRuntime(value, value.getClass, classOf[Option[_]])

  private def stockOption[A](value: Option[A]): Option[A] = {
    if (
      value == null || (value.getClass != classOf[Some[_]] && (value.asInstanceOf[AnyRef] ne None))
    ) reject()
    scalaStock(value)
    value
  }

  private def stringsMap(value: Map[String, String]): Unit = {
    if (value == null) reject()
    val name = value.getClass.getName
    if (
      !Set(
        "scala.collection.immutable.Map$EmptyMap$",
        "scala.collection.immutable.Map$Map1",
        "scala.collection.immutable.Map$Map2",
        "scala.collection.immutable.Map$Map3",
        "scala.collection.immutable.Map$Map4",
        "scala.collection.immutable.HashMap$HashTrieMap",
        "scala.collection.immutable.HashMap$HashMap1",
        "scala.collection.immutable.HashMap$HashMapCollision1"
      ).contains(name)
    ) reject()
    mapShape(value, new IdentityHashMap[AnyRef, BoxedBoolean]())
    value.iterator.foreach { entry =>
      DeltaRangerRawDeltaLogCache.requireRuntime(entry, classOf[Tuple2[_, _]], classOf[Option[_]])
      if (
        entry._1 == null || entry._2 == null || entry._1.getClass != classOf[
          String
        ] || entry._2.getClass != classOf[String]
      ) reject()
    }
  }

  // These are the concrete Scala 2.12 native action map/set descendants, not arbitrary
  // object graphs. Their public stored-node getters do not iterate or invoke supplied callbacks.
  private def node(value: AnyRef, seen: IdentityHashMap[AnyRef, BoxedBoolean]): String = {
    if (value == null || seen.size() >= 10000) reject()
    scalaStock(value)
    val name = value.getClass.getName
    // Empty immutable terminals are legitimately shared by distinct collision lists.
    if (
      name != "scala.collection.immutable.ListMap$EmptyListMap$" &&
      seen.put(value, BoxedBoolean.TRUE) != null
    ) reject()
    name
  }

  private def trieChildren(value: AnyRef, getter: String, element: String): Array[AnyRef] = {
    val children = value.getClass.getMethod(getter).invoke(value)
    val expected = Class.forName(
      "[Lscala.collection.immutable." + element + ";",
      false,
      classOf[Option[_]].getClassLoader
    )
    if (children == null || children.getClass != expected) reject()
    children.asInstanceOf[Array[AnyRef]]
  }

  private def mapShape(
      value: AnyRef,
      seen: IdentityHashMap[AnyRef, BoxedBoolean],
      depth: Int = 0
  ): Unit = {
    if (depth > 32) reject()
    node(value, seen) match {
      case "scala.collection.immutable.HashMap$HashTrieMap" =>
        trieChildren(value, "elems", "HashMap").foreach(child => mapShape(child, seen, depth + 1))
      case "scala.collection.immutable.HashMap$HashMapCollision1" =>
        var list = value.getClass.getMethod("kvs").invoke(value)
        var done = false
        while (!done) {
          node(list, seen) match {
            case "scala.collection.immutable.ListMap$EmptyListMap$" => done = true
            case "scala.collection.immutable.ListMap$Node" =>
              Seq("key", "value").foreach { field =>
                val string = list.getClass.getMethod(field).invoke(list)
                if (string == null || string.getClass != classOf[String]) reject()
              }
              list = list.getClass.getMethod("next").invoke(list)
            case _ => reject()
          }
        }
      case "scala.collection.immutable.Map$EmptyMap$" | "scala.collection.immutable.Map$Map1" |
          "scala.collection.immutable.Map$Map2" | "scala.collection.immutable.Map$Map3" |
          "scala.collection.immutable.Map$Map4" | "scala.collection.immutable.HashMap$HashMap1" =>
        ()
      case _ => reject()
    }
  }

  private def setShape(
      value: AnyRef,
      seen: IdentityHashMap[AnyRef, BoxedBoolean],
      depth: Int = 0
  ): Unit = {
    if (depth > 32) reject()
    node(value, seen) match {
      case "scala.collection.immutable.HashSet$HashTrieSet" =>
        trieChildren(value, "elems", "HashSet").foreach(child => setShape(child, seen, depth + 1))
      case "scala.collection.immutable.Set$EmptySet$" | "scala.collection.immutable.Set$Set1" |
          "scala.collection.immutable.Set$Set2" | "scala.collection.immutable.Set$Set3" |
          "scala.collection.immutable.Set$Set4" | "scala.collection.immutable.HashSet$HashSet1" =>
        ()
      case _ => reject()
    }
  }

  private def stringSequence(value: Seq[String]): Unit = {
    if (value == null) reject()
    var tail = value
    val seen = new IdentityHashMap[AnyRef, BoxedBoolean]()
    while (tail.asInstanceOf[AnyRef] ne Nil) {
      if (tail.getClass != classOf[scala.collection.immutable.::[_]]) reject()
      node(tail, seen)
      val first = tail.head
      if (first == null || first.getClass != classOf[String]) reject()
      tail = tail.tail
    }
    scalaStock(tail)
  }

  // Metadata.schema lazily calls DataType.fromJson, which can instantiate a UDT class. Compare
  // plain JSON instead. This mapper has no polymorphic/default typing and never parses Spark types.
  private def logicalSchema(json: String): JsonNode = {
    def keys(value: JsonNode, expected: Set[String]): Unit = {
      if (value == null || !value.isObject || value.fieldNames().asScala.toSet != expected) reject()
    }
    def boolean(value: JsonNode): Boolean = {
      if (value == null || !value.isBoolean) reject()
      value.booleanValue()
    }
    def strip(value: JsonNode, depth: Int = 0): JsonNode = {
      if (value == null || depth > 100) reject()
      if (value.isTextual) return value
      val kind = value.get("type")
      if (kind == null || !kind.isTextual) reject()
      val result = schemaMapper.createObjectNode()
      result.put("type", kind.textValue())
      kind.textValue() match {
        case "struct" =>
          keys(value, Set("type", "fields"))
          val fields = value.get("fields")
          if (fields == null || !fields.isArray) reject()
          val cleaned = schemaMapper.createArrayNode()
          fields.elements().asScala.foreach { field =>
            keys(field, Set("name", "type", "nullable", "metadata"))
            if (!field.get("name").isTextual || !field.get("metadata").isObject) reject()
            val output = schemaMapper.createObjectNode()
            output.put("name", field.get("name").textValue())
            output.set[JsonNode]("type", strip(field.get("type"), depth + 1))
            output.put("nullable", boolean(field.get("nullable")))
            output.set[JsonNode]("metadata", schemaMapper.createObjectNode())
            cleaned.add(output)
          }
          result.set[JsonNode]("fields", cleaned)
        case "array" =>
          keys(value, Set("type", "elementType", "containsNull"))
          result.set[JsonNode]("elementType", strip(value.get("elementType"), depth + 1))
          result.put("containsNull", boolean(value.get("containsNull")))
        case "map" =>
          keys(value, Set("type", "keyType", "valueType", "valueContainsNull"))
          result.set[JsonNode]("keyType", strip(value.get("keyType"), depth + 1))
          result.set[JsonNode]("valueType", strip(value.get("valueType"), depth + 1))
          result.put("valueContainsNull", boolean(value.get("valueContainsNull")))
        case _ => reject()
      }
      result
    }
    strip(schemaMapper.readTree(json))
  }

  private def reject(): Nothing = throw new SecurityException("Raw native observation rejected")
  private def sanitized[A](body: => A): A = try body
  catch {
    case NonFatal(_)     => reject()
    case _: LinkageError => reject()
  }
}
