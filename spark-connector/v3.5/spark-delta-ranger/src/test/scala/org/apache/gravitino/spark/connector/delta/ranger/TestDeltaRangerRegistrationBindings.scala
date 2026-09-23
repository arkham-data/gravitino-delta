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
import java.time.Duration
import java.util.{Collections, HashMap, LinkedHashMap, Optional, OptionalLong}
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicLong

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
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog => NativeTableCatalog}
import org.apache.spark.sql.delta.{DeltaLog, DeltaRelation, Snapshot}
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
        assertEquals(3, fields.count(_.getType == classOf[WeakReference[_]]))
        val allowed = Set[Class[_]](
          classOf[WeakReference[_]],
          classOf[String],
          classOf[Long],
          classOf[DeltaRangerRegistrationReader.PublishedObservation]
        )
        assertTrue(
          fields.forall(field => allowed.contains(field.getType)),
          fields.map(_.getType.getName).mkString(",")
        )
      } finally f.close()
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
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenReturn(registration)
    when(registration.name()).thenReturn("t")
    when(registration.properties()).thenAnswer(_ => properties)
    when(registration.columns()).thenAnswer(_ => columns)
    metadata.configure("a", Duration.ofSeconds(1), () => client)
    override def close(): Unit = { bindings.close(); metadata.close(); policy.engine.close() }
  }
}
