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

import java.io.Serializable
import java.lang.ref.{ReferenceQueue, WeakReference}
import java.lang.reflect.Modifier
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Collections, HashMap, LinkedHashMap, Optional, OptionalLong}
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.gravitino.{Catalog, NameIdentifier}
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaRegistrationCommand,
  DeltaRegistrationIdentity,
  DeltaRegistrationReceipt
}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.rel.{Column, Table => MetadataTable, TableCatalog}
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.IdentityKey
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog => NativeTableCatalog}
import org.apache.spark.sql.delta.{DeltaLog, DeltaRelation, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.actions.Protocol
import scala.collection.JavaConverters._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

class TestDeltaRangerRegistrationBindings {
  private val name = Identifier.of(Array("db"), "t")
  private val prefix = "delta.registration.receipt."

  @Test
  def weakIdentityKeysDoNotUseValueEqualityAndClearedKeysOnlyEqualThemselves(): Unit = {
    val queue = new ReferenceQueue[String]
    val first = new String("equal")
    val second = new String("equal")
    val key = new IdentityKey(first, queue)
    val same = new IdentityKey(first, queue)
    val equal = new IdentityKey(second, queue)
    assertEquals(key, same)
    assertEquals(System.identityHashCode(first), key.hashCode())
    assertNotEquals(key, equal)
    val entries = new HashMap[IdentityKey[String], String]
    entries.put(key, "one")
    entries.put(equal, "two")
    assertEquals("one", entries.get(same))
    assertEquals("two", entries.get(new IdentityKey(second, queue)))
    key.clear(); same.clear()
    assertEquals(key, key)
    assertNotEquals(key, same)
    assertTrue(key.enqueue())
    DeltaRangerRegistrationBindings.drain(queue, entries)
    assertEquals(1, entries.size())
    assertEquals("two", entries.get(equal))
  }

  @Test
  def equalIdentityHashesStillKeepDifferentLiveReferentsSeparate(): Unit = {
    val first = new Object
    val second = new Object
    val queue = new ReferenceQueue[Object]
    val collisionHash = 37
    val firstKey = new IdentityKey(first, queue, collisionHash)
    val sameFirst = new IdentityKey(first, queue, collisionHash)
    val secondKey = new IdentityKey(second, queue, collisionHash)
    assertEquals(firstKey.hashCode(), secondKey.hashCode())
    assertEquals(firstKey, sameFirst)
    assertNotEquals(firstKey, secondKey)
    val entries = new HashMap[IdentityKey[Object], String]
    entries.put(firstKey, "first"); entries.put(secondKey, "second")
    assertEquals("first", entries.get(sameFirst))
    assertEquals("second", entries.get(new IdentityKey(second, queue, collisionHash)))
    firstKey.clear(); sameFirst.clear()
    assertEquals(collisionHash, firstKey.hashCode())
    assertEquals(firstKey, firstKey)
    assertNotEquals(firstKey, sameFirst)
    assertTrue(firstKey.enqueue())
    DeltaRangerRegistrationBindings.drain(queue, entries)
    assertEquals(1, entries.size())
    assertEquals("second", entries.get(secondKey))
    assertEquals("second", entries.get(new IdentityKey(second, queue, collisionHash)))
  }

  @Test
  def ordinaryFallbackKeepsEveryAdmittedReferenceAndSeparateAliasOccurrences(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        val loaded = f.bindings.load(name, () => native).asInstanceOf[DeltaTableV2]
        assertTrue(loaded eq native)
        val logical = fallback(loaded)
        val bound = f.bindings.resolve(logical)
        assertNotNull(bound)
        assertTrue(bound.descriptor() eq loaded.catalogTable.get)
        assertTrue(bound.relation() eq loaded.toBaseRelation)
        assertTrue(bound.index() eq loaded.toBaseRelation.asInstanceOf[HadoopFsRelation].location)
        assertTrue(bound.log() eq loaded.deltaLog)
        assertEquals(loaded.initialSnapshot.metadata.id, bound.observation().nativeId())
        assertEquals(loaded.initialSnapshot.metadata.schemaString, bound.nativeSchemaJson())
        assertEquals(loaded.initialSnapshot.version, bound.index().snapshotAtAnalysis.version)
        assertTrue(loaded.toBaseRelation eq loaded.toBaseRelation)
        assertTrue(loaded.withOptions(Map.empty) eq loaded)
        assertTrue(loaded.ttSafeCatalogTable.get eq loaded.catalogTable.get)
        assertTrue(logical.relation eq loaded.toBaseRelation)
        assertTrue(logical.catalogTable.get eq loaded.catalogTable.get)
        val left = SubqueryAlias("left_alias", logical)
        val right = SubqueryAlias("right_alias", logical.newInstance())
        val joined = Join(left, right, Inner, None, JoinHint.NONE)
        val occurrences = joined.collect { case relation: LogicalRelation => relation }
        assertEquals(2, occurrences.size)
        occurrences.foreach(relation =>
          assertTrue(f.bindings.resolve(relation).index() eq bound.index())
        )
        assertFalse(occurrences.head.output.head.exprId == occurrences.last.output.head.exprId)
      } finally f.close()
    }

  @Test
  def equalDescriptorsFreshEqualPathIndicesAndCrossedRelationsNeverResolve(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val logical = fallback(native)
        val descriptorCopy = native.catalogTable.get.copy()
        assertEquals(native.catalogTable.get, descriptorCopy)
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.resolve(logical.copy(catalogTable = Some(descriptorCopy)))
        )
        val relation = native.toBaseRelation.asInstanceOf[HadoopFsRelation]
        val index = relation.location.asInstanceOf[TahoeLogFileIndex]
        val freshIndex = index.copy()
        assertTrue(freshIndex.deltaLog eq index.deltaLog)
        assertEquals(index.path, freshIndex.path)
        val freshRelation = relation.copy(location = freshIndex)(spark)
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.resolve(logical.copy(relation = freshRelation))
        )
        val foreign = table(spark, "b")
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.resolve(logical.copy(relation = foreign.toBaseRelation))
        )
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.resolve(logical.copy(catalogTable = None))
        )
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.resolve(logical.copy(isStreaming = true))
        )
        assertNotNull(f.bindings.resolve(logical))
      } finally f.close()
    }

  @Test
  def ownersAtSamePathAndSameNameReplacementHaveSeparateMembership(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val first = table(spark, "a")
      val second = table(spark, "a")
      val f = new Fixture(first)
      val other = new Fixture(second)
      try {
        f.bindings.load(name, () => first)
        other.bindings.load(name, () => second)
        val logical = fallback(first)
        val otherLogical = fallback(second)
        assertNotNull(f.bindings.resolve(logical))
        assertNotNull(other.bindings.resolve(otherLogical))
        assertThrows(classOf[SecurityException], () => f.bindings.resolve(otherLogical))
        assertThrows(classOf[SecurityException], () => other.bindings.resolve(logical))
        f.bindings.close()
        assertThrows(classOf[SecurityException], () => f.bindings.resolve(logical))
        assertNotNull(other.bindings.resolve(otherLogical))
      } finally { f.close(); other.close() }
    }

  @Test
  def beforeAndAfterReceiptOrSchemaDriftNeverPublishesPartialBinding(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val changes = Seq(
        "id" -> "2",
        "generation" -> "2",
        "token" -> "new-token",
        "location" -> "file:/different-location",
        "nativeId" -> "different-id",
        "nativeVersion" -> (native.initialSnapshot.version + 1).toString
      )
      changes.foreach { case (field, value) =>
        val f = new Fixture(native)
        try {
          assertThrows(
            classOf[SecurityException],
            () =>
              f.bindings.load(
                name,
                () => {
                  f.properties.put(prefix + field, value)
                  native
                }
              )
          )
          assertThrows(classOf[SecurityException], () => f.bindings.resolve(fallback(native)))
        } finally f.close()
      }
      val schema = new Fixture(native)
      try {
        assertThrows(
          classOf[SecurityException],
          () =>
            schema.bindings.load(
              name,
              () => {
                schema.columns = schema.columns.reverse
                native
              }
            )
        )
        assertThrows(classOf[SecurityException], () => schema.bindings.resolve(fallback(native)))
      } finally schema.close()
    }

  @Test
  def invalidNativeEvidenceAndPendingMetadataNeverPublish(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        val descriptor = native.catalogTable.get
        val invalid = Seq(
          native.copy(catalogTable = None),
          native.copy(catalogTable = Some(descriptor.copy(provider = Some("parquet")))),
          native.copy(catalogTable =
            Some(descriptor.copy(identifier = descriptor.identifier.copy(table = "other")))
          ),
          table(spark, "b"),
          native.copy(options = Map("readChangeFeed" -> "true")),
          native.copy(options = Map("versionAsOf" -> "0"))
        )
        invalid.foreach(value =>
          assertThrows(classOf[SecurityException], () => f.bindings.load(name, () => value))
        )
        f.properties.clear()
        var called = false
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.load(name, () => { called = true; native })
        )
        assertFalse(called)
        f.properties.putAll(
          DeltaRegistrationReceipt
            .of(
              DeltaRegistrationIdentity.of(1, 1, "pending-token"),
              DeltaRegistrationCommand.Operation.ADOPT,
              DeltaRegistrationReceipt.State.ADOPTING,
              "run",
              DeltaRegistrationReceipt.Evidence.NONE,
              "NONE",
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              OptionalLong.empty()
            )
            .toProperties()
        )
        assertThrows(
          classOf[SecurityException],
          () => f.bindings.load(name, () => { called = true; native })
        )
        assertFalse(called)
        assertThrows(classOf[SecurityException], () => f.bindings.resolve(fallback(native)))
      } finally f.close()
    }

  @Test
  def closeAndExpiryDuringLoadDenyWithoutSuccessfulInsertionOrRenewal(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      (0 until 3).foreach { mode =>
        val f = new Fixture(native)
        try {
          assertThrows(
            classOf[SecurityException],
            () =>
              f.bindings.load(
                name,
                () => {
                  if (mode == 0) f.bindings.close()
                  else if (mode == 1) f.metadata.close()
                  else f.clock.set(Duration.ofSeconds(1).toNanos)
                  native
                }
              )
          )
          assertThrows(classOf[SecurityException], () => f.bindings.resolve(fallback(native)))
        } finally f.close()
      }
    }

  @Test
  def successfulBindingsRejectLaterLeaseExpiryAndEngineClosure(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      Seq(false, true).foreach { engineClosed =>
        val f = new Fixture(native)
        try {
          f.bindings.load(name, () => native)
          val logical = fallback(native)
          val bound = f.bindings.resolve(logical)
          bound.requireLive()
          if (engineClosed) f.policy.engine.close()
          else f.clock.set(Duration.ofSeconds(1).toNanos)
          assertThrows(classOf[SecurityException], () => bound.requireLive())
          assertThrows(classOf[SecurityException], () => f.bindings.resolve(logical))
          verify(f.client, times(2)).loadCatalog("a")
        } finally f.close()
      }
    }

  @Test
  def boundValuesAreRedactedAndWeakMapValuesCannotStronglyReachNativeOwners(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val bound = f.bindings.resolve(fallback(native))
        assertNotNull(bound)
        assertFalse(classOf[Serializable].isAssignableFrom(bound.getClass))
        assertFalse(classOf[Product].isAssignableFrom(bound.getClass))
        assertFalse(bound.toString.contains("token"))
        assertFalse(bound.toString.contains(native.path.toString))
        val observation = bound.observation()
        val observationFields = observation.getClass.getDeclaredFields.filterNot(field =>
          Modifier.isStatic(field.getModifiers)
        )
        assertTrue(observationFields.forall(field => Modifier.isFinal(field.getModifiers)))
        val owner = observation.getClass.getDeclaredField("owner")
        owner.setAccessible(true)
        assertEquals(classOf[Object], owner.get(observation).getClass)
        val lease = observation.getClass.getDeclaredField("lease")
        lease.setAccessible(true)
        val stamp = lease.get(observation)
        stamp.getClass.getDeclaredFields
          .filterNot(field => Modifier.isStatic(field.getModifiers))
          .foreach { field =>
            field.setAccessible(true)
            assertEquals(classOf[Object], field.get(stamp).getClass)
          }
        assertFalse(
          observationFields.exists(field =>
            Set[Class[_]](
              classOf[DeltaRangerRegistrationReader],
              classOf[DeltaRangerMetadataClient],
              classOf[GravitinoClient],
              classOf[Catalog],
              classOf[MetadataTable],
              classOf[Column]
            ).contains(field.getType)
          )
        )
        val fields =
          classOf[DeltaRangerRegistrationBindings.RegistrationBinding].getDeclaredFields.filterNot(
            field => Modifier.isStatic(field.getModifiers)
          )
        assertEquals(4, fields.count(_.getType == classOf[WeakReference[_]]))
        val allowed = Set[Class[_]](
          classOf[WeakReference[_]],
          classOf[String],
          classOf[Long],
          classOf[java.util.Map[_, _]],
          classOf[DeltaRangerRegistrationReader.PublishedObservation]
        )
        assertTrue(
          fields.forall(field => allowed.contains(field.getType)),
          fields.map(_.getType.getName).mkString(",")
        )
      } finally f.close()
    }

  @Test
  def appendTargetRequiresTheExactCompletedNamedLoad(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(native))
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        assertSame(native, target.table())
        assertSame(native.catalogTable.get, target.descriptor())
        assertSame(native.deltaLog, target.log())
        assertSame(f.policy.engine, target.engine())
        assertEquals("a", target.observation().catalog())
        target.requireRetained()
        target.requireLive()
        target.validateCurrent(spark)
        val equalPath = table(spark, "a")
        assertEquals(native.path, equalPath.path)
        assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(equalPath))
        assertNotNull(f.bindings.resolve(fallback(native)))
        assertTrue(f.policy.requests.isEmpty)
        assertTrue(f.policy.events.isEmpty)
        assertFalse(classOf[Serializable].isAssignableFrom(target.getClass))
        assertFalse(classOf[Product].isAssignableFrom(target.getClass))
        assertTrue(
          target.getClass.getDeclaredConstructors
            .filterNot(_.isSynthetic)
            .forall(c => Modifier.isPrivate(c.getModifiers))
        )
        assertFalse(target.toString.contains("token"))
        assertFalse(target.toString.contains(native.path.toString))
        assertNull(DeltaRangerNativeOperation.current())
      } finally f.close()
    }

  @Test
  def appendTargetRejectsSharedAndEqualDescriptorsRawPathsAndAlteredCopies(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val descriptor = native.catalogTable.get
        val descriptorCopy = descriptor.copy()
        assertEquals(descriptor, descriptorCopy)
        val copies = Seq(
          native.copy(),
          native.copy(catalogTable = Some(descriptorCopy)),
          native.copy(catalogTable = Some(descriptor.copy(owner = "replacement-owner"))),
          native.copy(catalogTable = None),
          native.copy(timeTravelOpt = Some(DeltaTimeTravelSpec(None, Some(0L), Some("test")))),
          native.copy(options = Map("versionAsOf" -> "0")),
          native.copy(options = Map("readChangeFeed" -> "true")),
          native.copy(options = Map("arbitrary-option" -> "value")),
          native.copy(spark = spark.newSession())
        )
        copies.foreach { copy =>
          assertNotSame(native, copy)
          assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(copy))
          assertNull(DeltaRangerNativeOperation.current())
        }
        val filtered = mock(
          classOf[DeltaTableV2],
          withSettings().spiedInstance(native.copy()).defaultAnswer(CALLS_REAL_METHODS)
        )
        when(filtered.hasPartitionFilters).thenReturn(true)
        assertThrows(classOf[SecurityException], () => f.bindings.load(name, () => filtered))
        assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(filtered))
        assertNotNull(f.bindings.resolveAppendTarget(native))
      } finally f.close()
    }

  @Test
  def appendTargetRetainsNativeOwnerOrdinaryLoadAndSessionIdentity(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val original = table(spark, "a")
      val descriptor = mock(
        classOf[CatalogTable],
        withSettings().spiedInstance(original.catalogTable.get).defaultAnswer(CALLS_REAL_METHODS)
      )
      val native = mock(
        classOf[DeltaTableV2],
        withSettings()
          .spiedInstance(original.copy(catalogTable = Some(descriptor)))
          .defaultAnswer(CALLS_REAL_METHODS)
      )
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        assertThrows(classOf[SecurityException], () => target.validateCurrent(spark.newSession()))
        assertNull(DeltaRangerNativeOperation.current())
        when(descriptor.owner).thenReturn("changed-owner")
        assertThrows(classOf[SecurityException], () => target.requireRetained())
        when(descriptor.owner).thenReturn("same-owner")
        target.requireRetained()
        when(native.hasPartitionFilters).thenReturn(true)
        assertThrows(classOf[SecurityException], () => target.requireRetained())
        when(native.hasPartitionFilters).thenReturn(false)
        when(native.options).thenReturn(Map("replaceWhere" -> "id = 1"))
        assertThrows(classOf[SecurityException], () => target.requireRetained())
        when(native.options).thenReturn(Map.empty[String, String])
        val replacementDescriptor = descriptor.copy()
        when(native.catalogTable).thenReturn(Some(replacementDescriptor))
        assertThrows(classOf[SecurityException], () => target.requireRetained())
        when(native.catalogTable).thenReturn(Some(descriptor))
        target.validateCurrent(spark)
        assertTrue(f.policy.events.isEmpty)
        assertNull(DeltaRangerNativeOperation.current())
      } finally f.close()
    }

  @Test
  def appendTargetsAndObservationsCannotCrossOwnersOrCompletedBindings(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val first = table(spark, "a")
      val second = table(spark, "a")
      val f = new Fixture(first)
      val other = new Fixture(second)
      try {
        f.bindings.load(name, () => first)
        other.bindings.load(name, () => second)
        val target = f.bindings.resolveAppendTarget(first)
        val secondTarget = other.bindings.resolveAppendTarget(second)
        assertSame(target.log(), secondTarget.log())
        assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(second))
        assertThrows(classOf[SecurityException], () => other.bindings.resolveAppendTarget(first))
        val observation = target.validateCurrent(spark)
        observation.requireSame(target.validateCurrent(spark))
        assertThrows(
          classOf[SecurityException],
          () => observation.requireSame(secondTarget.validateCurrent(spark))
        )
        assertThrows(
          classOf[SecurityException],
          () =>
            observation.requireSame(f.bindings.resolveAppendTarget(first).validateCurrent(spark))
        )
        assertThrows(classOf[SecurityException], () => observation.requireSame(null))
        f.bindings.load(name, () => first)
        assertThrows(classOf[SecurityException], () => target.requireRetained())
        secondTarget.validateCurrent(spark)
        assertNull(DeltaRangerNativeOperation.current())
      } finally { f.close(); other.close() }
    }

  @Test
  def appendTargetRejectsReplacedRegistrationAndSchemaAtEitherPublicationRead(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        val original = f.properties.asScala.toMap
        val columns = f.columns
        for (
          phase <- Seq(1, 2);
          (key, value) <- Seq(
            "id" -> "2",
            "generation" -> "2",
            "token" -> "replacement",
            "nativeId" -> "replacement-native",
            "location" -> "/tmp/replacement",
            "nativeVersion" -> "999",
            "state" -> "ADOPTING",
            "schema" -> "changed"
          )
        ) {
          val at = f.reads.get() + phase
          f.onRead = count =>
            if (count == at) {
              if (key == "schema") f.columns = columns.dropRight(1)
              else f.properties.put(prefix + key, value)
            }
          assertThrows(
            classOf[SecurityException],
            () => { target.validateCurrent(spark); () },
            key + ":" + phase
          )
          assertNull(DeltaRangerNativeOperation.current())
          f.onRead = _ => ()
          f.properties.clear(); f.properties.putAll(original.asJava); f.columns = columns
          target.validateCurrent(spark)
        }
        assertTrue(f.policy.events.isEmpty)
      } finally f.close()
    }

  @Test
  def appendTargetRejectsClosedOwnersAndExpiredLeasesBeforeAndDuringObservation(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val native = table(spark, "a")
      for (mode <- Seq("catalog", "metadata", "engine", "expiry"); phase <- Seq(0, 1, 2)) {
        val f = new Fixture(native)
        try {
          f.bindings.load(name, () => native)
          val target = f.bindings.resolveAppendTarget(native)
          def close(): Unit = mode match {
            case "catalog"  => f.bindings.close()
            case "metadata" => f.metadata.close()
            case "engine"   => f.policy.engine.close()
            case "expiry"   => f.clock.set(Duration.ofSeconds(1).toNanos)
          }
          if (phase == 0) close()
          else {
            val at = f.reads.get() + phase
            f.onRead = count => if (count == at) close()
          }
          assertThrows(
            classOf[SecurityException],
            () => { target.validateCurrent(spark); () },
            mode + ":" + phase
          )
          assertThrows(classOf[SecurityException], () => target.requireLive())
          assertThrows(classOf[SecurityException], () => target.requireRetained())
          assertThrows(classOf[SecurityException], () => f.bindings.resolveAppendTarget(native))
          assertNull(DeltaRangerNativeOperation.current())
        } finally f.close()
      }
    }

  @Test
  def appendTargetAllowsDataAdvanceButRejectsNativeRegistrationVersionRegression(): Unit =
    withAppendHistory { (spark, publish) =>
      publish("data")
      // This case starts at version 1; initialSnapshot alone may reuse a cached native head.
      val current =
        DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).update(false, None, None)
      assertEquals(1L, current.version)
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        assertEquals(1L, native.initialSnapshot.version)
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        target.validateCurrent(spark)
        f.properties.put(prefix + "nativeVersion", "0")
        assertThrows(classOf[SecurityException], () => target.validateCurrent(spark))
        assertNull(DeltaRangerNativeOperation.current())
      } finally f.close()
    }

  @Test
  def appendTargetCurrentObservationAcceptsCompatibleDataOnlyAdvance(): Unit =
    withAppendHistory { (spark, publish) =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        val initial = target.validateCurrent(spark)
        publish("data")
        f.properties.put(prefix + "nativeVersion", "1")
        val current = target.validateCurrent(spark)
        current.requireSame(target.validateCurrent(spark))
        assertThrows(classOf[SecurityException], () => initial.requireSame(current))
        assertEquals(0L, native.initialSnapshot.version)
        assertTrue(f.policy.requests.isEmpty)
        assertTrue(f.policy.events.isEmpty)
        assertNull(DeltaRangerNativeOperation.current())
      } finally f.close()
    }

  @Test
  def appendTargetAcceptsDataAdvanceDuringFirstPublicationReadBeforeNativeObservation(): Unit =
    withAppendHistory { (spark, publish) =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        val at = f.reads.get() + 1
        f.onRead = count => if (count == at) publish("data")
        val current = target.validateCurrent(spark)
        f.onRead = _ => ()
        current.requireSame(target.validateCurrent(spark))
        assertNull(DeltaRangerNativeOperation.current())
      } finally f.close()
    }

  @Test
  def appendTargetRejectsNativeAdvanceInsideFinalPublicationBracket(): Unit =
    withAppendHistory { (spark, publish) =>
      val native = table(spark, "a")
      val f = new Fixture(native)
      try {
        f.bindings.load(name, () => native)
        val target = f.bindings.resolveAppendTarget(native)
        val at = f.reads.get() + 2
        f.onRead = count => if (count == at) publish("data")
        assertThrows(classOf[SecurityException], () => target.validateCurrent(spark))
        assertEquals(at, f.reads.get())
        assertNull(DeltaRangerNativeOperation.current())
        f.onRead = _ => ()
        target.validateCurrent(spark)
      } finally f.close()
    }

  @Test
  def appendTargetRejectsPropertySchemaProtocolPartitionAndMappingDrift(): Unit = {
    Seq("property", "constraint", "protocol", "partition", "mapping", "schema", "nativeId")
      .foreach { change =>
        withAppendHistory { (spark, publish) =>
          val native = table(spark, "a")
          val f = new Fixture(native)
          try {
            f.bindings.load(name, () => native)
            val target = f.bindings.resolveAppendTarget(native)
            target.validateCurrent(spark)
            publish(change)
            val rejected =
              try { target.validateCurrent(spark); false }
              catch { case _: SecurityException => true }
            val head = native.deltaLog.unsafeVolatileSnapshot
            assertEquals(1L, head.version)
            change match {
              case "property" =>
                assertEquals("true", head.metadata.configuration("delta.appendOnly"))
              case "constraint" =>
                assertEquals("id > 0", head.metadata.configuration("delta.constraints.positive_id"))
              case "protocol"  => assertEquals(Protocol(2, 5), head.protocol)
              case "partition" => assertEquals(Seq("region"), head.metadata.partitionColumns)
              case "mapping" =>
                assertEquals("100", head.metadata.configuration("delta.columnMapping.maxColumnId"))
              case "schema" =>
                assertEquals(Seq("id", "secret", "region", "extra"), head.schema.fieldNames.toSeq)
              case "nativeId" => assertEquals("replacement-native-id", head.metadata.id)
            }
            assertTrue(rejected, change)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(f.policy.requests.isEmpty)
            assertTrue(f.policy.events.isEmpty)
          } finally f.close()
        }
      }
  }

  private def withAppendHistory(test: (SparkSession, String => Unit) => Unit): Unit = {
    var commit: Path = null
    var data: Array[Byte] = null
    NativeSelectFixture.withSpark(
      spark => {
        val original = table(spark, "a").initialSnapshot
        test(
          spark,
          change => {
            val metadata = original.metadata
            val action = change match {
              case "property" =>
                metadata
                  .copy(configuration = metadata.configuration + ("delta.appendOnly" -> "true"))
                  .json
              case "constraint" =>
                metadata
                  .copy(configuration =
                    metadata.configuration + ("delta.constraints.positive_id" -> "id > 0")
                  )
                  .json
              case "protocol"  => Protocol(2, 5).json
              case "partition" => metadata.copy(partitionColumns = Seq("region")).json
              case "mapping" =>
                metadata
                  .copy(configuration =
                    metadata.configuration + ("delta.columnMapping.maxColumnId" -> "100")
                  )
                  .json
              case "schema" =>
                metadata.copy(schemaString = original.schema.add("extra", "string").json).json
              case "nativeId" => metadata.copy(id = "replacement-native-id").json
              case "data"     => ""
            }
            Files.write(
              commit,
              if (change == "data") data else (action + "\n").getBytes(StandardCharsets.UTF_8)
            )
            ()
          }
        )
      },
      beforeExtension = spark => {
        val location = NativeSelectFixture.locations("a")
        spark
          .sql("SELECT 4L id, 'tail' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .save(location)
        commit = Paths.get(location, "_delta_log", "00000000000000000001.json")
        data = Files.readAllBytes(commit)
        Files.delete(commit)
        // The replay may replace the metadata action. A checksum from the original data commit
        // would cause Delta to reconstruct version 1 from that old checksum's embedded metadata.
        Seq(
          "00000000000000000001.crc",
          ".00000000000000000001.crc.crc",
          ".00000000000000000001.json.crc"
        ).foreach(name => Files.deleteIfExists(commit.resolveSibling(name)))
        DeltaLog.clearCache()
      }
    )
  }

  private def table(spark: SparkSession, catalog: String): DeltaTableV2 =
    spark.sessionState.catalogManager
      .catalog(catalog)
      .asInstanceOf[NativeTableCatalog]
      .loadTable(name)
      .asInstanceOf[DeltaTableV2]

  private def fallback(table: DeltaTableV2): LogicalRelation = {
    val source = DataSourceV2Relation.create(table, None, Some(name))
    DeltaRelation.fromV2Relation(
      table,
      source,
      new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
    )
  }

  private class Fixture(table: DeltaTableV2) extends AutoCloseable {
    val clock = new AtomicLong()
    val client = mock(classOf[GravitinoClient])
    val catalog = mock(classOf[Catalog])
    val tables = mock(classOf[TableCatalog])
    val registration = mock(classOf[MetadataTable])
    val metadata =
      new DeltaRangerMetadataClient(() => clock.get(), mock(classOf[ScheduledExecutorService]))
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val reader = new DeltaRangerRegistrationReader(metadata, "lake", "a")
    val bindings = new DeltaRangerRegistrationBindings(reader, policy.engine)
    val properties = new LinkedHashMap[String, String](
      DeltaRegistrationReceipt
        .of(
          DeltaRegistrationIdentity.of(1, 1, "private-token"),
          DeltaRegistrationCommand.Operation.ADOPT,
          DeltaRegistrationReceipt.State.PUBLISHED,
          "run",
          DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT,
          "NONE",
          Optional.empty(),
          Optional.of(table.path.toString),
          Optional.of(table.initialSnapshot.metadata.id),
          OptionalLong.of(table.initialSnapshot.version)
        )
        .toProperties()
    )
    private val converter = new SparkTypeConverter
    var columns: Array[Column] = table.initialSnapshot.schema.fields.map(field =>
      Column.of(
        field.name,
        converter.toGravitinoType(field.dataType),
        null,
        field.nullable,
        false,
        Column.DEFAULT_VALUE_NOT_SET
      )
    )
    when(client.loadCatalog("a")).thenReturn(catalog)
    when(catalog.name()).thenReturn("a")
    when(catalog.provider()).thenReturn("lakehouse-delta")
    when(catalog.asTableCatalog()).thenReturn(tables)
    val reads = new AtomicInteger()
    var onRead: Int => Unit = (_: Int) => ()
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenAnswer(_ => {
      onRead(reads.incrementAndGet()); registration
    })
    when(registration.name()).thenReturn("t")
    when(registration.properties()).thenAnswer(_ => properties)
    when(registration.columns()).thenAnswer(_ => columns)
    metadata.configure("a", Duration.ofSeconds(1), () => client)
    override def close(): Unit = { bindings.close(); metadata.close(); policy.engine.close() }
  }
}
