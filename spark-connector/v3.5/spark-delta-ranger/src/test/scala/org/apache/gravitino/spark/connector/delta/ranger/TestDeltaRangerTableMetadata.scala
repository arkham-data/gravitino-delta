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

import java.lang.reflect.InvocationTargetException
import java.nio.file.{Files, Paths}
import java.util.{Collections, LinkedHashMap, Optional, OptionalLong}
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.gravitino.{Catalog, NameIdentifier}
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaRegistrationCommand,
  DeltaRegistrationIdentity,
  DeltaRegistrationReceipt
}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.exceptions.{
  NoSuchCatalogException,
  NoSuchSchemaException,
  NoSuchTableException
}
import org.apache.gravitino.rel.{Column, Table => MetadataTable, TableCatalog}
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{
  CatalogPlugin,
  Identifier,
  TableCatalog => NativeTableCatalog
}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._
import scala.collection.JavaConverters._

class TestDeltaRangerTableMetadata {
  private val mapper = new ObjectMapper

  @Test
  def publicApiExists(): Unit = {
    assertTrue(
      GravitinoDeltaTable.getClass.getMethods.exists(m =>
        m.getName == "metadataJson" && m.getParameterTypes.toSeq == Seq(
          classOf[SparkSession],
          classOf[String]
        )
      ),
      "Public metadataJson(SparkSession, String) API is missing"
    )
  }

  @Test
  def installedMetadataNeedsOnlyNormalVisibilityAndReturnsDetachedNativeAllowlist(): Unit =
    withFixture { f =>
      val before = f.snapshotBytes()
      val value = mapper.readTree(read(f.spark))
      assertEquals(
        Set("schemaJson", "properties", "partitionColumns"),
        value.fieldNames().asScala.toSet
      )
      val schema = DataType.fromJson(value.get("schemaJson").asText()).asInstanceOf[StructType]
      assertEquals(Seq("id", "secret", "region"), schema.fieldNames.toSeq)
      assertEquals("native comment 雪", schema("id").metadata.getString("comment"))
      assertEquals(
        Seq("region"),
        value.get("partitionColumns").elements().asScala.map(_.asText()).toSeq
      )
      val properties = value.get("properties")
      assertEquals(
        Set(
          "primary_key",
          "delta.enableChangeDataFeed",
          "delta.logRetentionDuration",
          "delta.deletedFileRetentionDuration",
          "delta.compatibility.symlinkFormatManifest.enabled"
        ),
        properties.fieldNames().asScala.toSet
      )
      assertEquals("id", properties.get("primary_key").asText())
      assertEquals(
        "false",
        properties.get("delta.compatibility.symlinkFormatManifest.enabled").asText()
      )
      assertFalse(value.toString.contains("private-value"))
      assertFalse(value.toString.contains(f.location))
      assertTrue(f.events.head == "actor")
      assertTrue(f.policy.requests.isEmpty, "Metadata must not request payload SELECT or CREATE")
      assertEquals(before, f.snapshotBytes(), "Metadata must not write log or payload files")
      value.asInstanceOf[ObjectNode].put("schemaJson", "changed")
      assertEquals(
        "id",
        DataType
          .fromJson(mapper.readTree(read(f.spark)).get("schemaJson").asText())
          .asInstanceOf[StructType]
          .fields
          .head
          .name
      )
      verify(f.actorTables, times(2)).loadTable(NameIdentifier.of("db", "t"))
      verifyNoMoreInteractions(f.actorTables)
      assertTrue(f.policy.requests.isEmpty)
    }

  @Test
  def onlyAuthorizedTypedTableAbsenceReturnsNullAndChecksLease(): Unit =
    withFixture { f =>
      f.actorResult = () => throw new NoSuchTableException("authorized absence")
      assertNull(read(f.spark))
      assertEquals(Seq("actor", "metadata-catalog"), f.events.toSeq)
      assertEquals(0, f.reads)
      assertTrue(f.policy.requests.isEmpty)
      f.clock.set(TimeUnit.SECONDS.toNanos(301))
      rejected(f)
      assertEquals(0, f.reads)
    }

  @Test
  def denialSchemaCatalogAndTransportFailureNeverBecomeAbsenceOrPrivilegedReads(): Unit =
    withFixture { f =>
      Seq(
        new SecurityException("private-value denied existing"),
        new SecurityException("private-value denied absent probe"),
        new NoSuchSchemaException("private-value schema"),
        new IllegalStateException("private-value unavailable")
      ).foreach { failure =>
        f.actorResult = () => throw failure
        f.events.clear()
        rejected(f)
        assertEquals(Seq("actor"), f.events.toSeq)
      }
      Seq(
        new NoSuchCatalogException("private-value catalog"),
        new NoSuchTableException("private-value outside table lookup")
      ).foreach { failure =>
        f.actorCatalogResult = () => throw failure
        f.events.clear()
        rejected(f)
        assertTrue(f.events.isEmpty)
      }
      assertEquals(0, f.reads)
    }

  @Test
  def absenceRejectsUnavailableOrExpiringMetadataLease(): Unit =
    withFixture { f =>
      f.actorResult = () => throw new NoSuchTableException("authorized absence")
      when(f.client.loadCatalog("a"))
        .thenThrow(new IllegalStateException("private-value transport"))
      rejected(f)
      doAnswer(_ => {
        f.clock.set(TimeUnit.SECONDS.toNanos(301)); f.catalog
      }).when(f.client).loadCatalog("a")
      rejected(f)
      assertEquals(0, f.reads)
    }

  @Test
  def pendingMalformedReplacedAndInvalidNativeIdentityReject(): Unit =
    withFixture { f =>
      val original = new LinkedHashMap[String, String](f.properties)
      Seq("state" -> "ADOPTING", "id" -> "malformed", "nativeId" -> "wrong-native").foreach {
        case (field, value) =>
          f.properties.put("delta.registration.receipt." + field, value)
          rejected(f)
          f.properties.clear(); f.properties.putAll(original)
      }
      f.onRead = _ => f.properties.put("delta.registration.receipt.generation", "2")
      rejected(f)
      f.properties.clear(); f.properties.putAll(original)
      f.onRead = number =>
        if (number == f.replaceAt) f.properties.put("delta.registration.receipt.generation", "2")
      f.replaceAt = f.reads + 3
      rejected(f)
    }

  @Test
  def exactActiveSessionNameAndContextAreRequiredOnBothExits(): Unit =
    withFixture { f =>
      Seq("db.t", "t", "a.db.t.extra", "missing.db.t", "a.db.`t/other`").foreach { name =>
        assertThrows(classOf[SecurityException], () => read(f.spark, name))
      }
      val other = f.spark.newSession()
      assertThrows(classOf[SecurityException], () => read(other))
      assertEquals(0, f.reads)
      val key = "spark.sql.shuffle.partitions"
      val old = f.spark.conf.get(key)
      f.actorResult = () => { f.spark.conf.set(key, "9"); throw new NoSuchTableException("absent") }
      rejected(f)
      f.spark.conf.set(key, old)
      f.actorResult = () => throw new NoSuchTableException("authorized absence")
      doAnswer(_ => { f.spark.conf.set(key, "9"); f.catalog }).when(f.client).loadCatalog("a")
      rejected(f)
      f.spark.conf.set(key, old)
      doAnswer(_ => { f.replaceCatalog(); f.catalog }).when(f.client).loadCatalog("a")
      try rejected(f)
      finally f.restoreCatalog()
      doAnswer(_ => f.catalog).when(f.client).loadCatalog("a")
      assertEquals(0, f.reads)
      f.actorResult = () => f.registration
      f.onRead = _ => f.spark.conf.set(key, "9")
      rejected(f)
      f.spark.conf.set(key, old)
      f.onRead = _ => f.spark.sparkContext.setLocalProperty("kyuubi.session.user", "other-subject")
      try rejected(f)
      finally f.spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
      f.onRead = _ => ()
      f.actorResult = () => throw new NoSuchTableException("authorized absence")
      f.wrapper.close()
      rejected(f)
    }

  private def read(spark: SparkSession, name: String = "a.db.t"): String = {
    val method = GravitinoDeltaTable.getClass.getMethods.find(_.getName == "metadataJson")
    assertTrue(method.isDefined, "Public metadataJson API is missing")
    try method.get.invoke(GravitinoDeltaTable, spark, name).asInstanceOf[String]
    catch { case failure: InvocationTargetException => throw failure.getCause }
  }

  private def rejected(f: Fixture): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => read(f.spark))
    assertFalse(failure.toString.contains("private-value"))
    assertNull(failure.getCause, "Private underlying metadata failures must not escape")
  }

  private def withFixture(test: Fixture => Unit): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val f = new Fixture(spark, policy);
          try test(f)
          finally f.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark =>
          spark.sql(
            "ALTER TABLE delta.`" + NativeSelectFixture.locations("a") +
              "` SET TBLPROPERTIES ('primary_key'='id', 'delta.enableChangeDataFeed'='true', " +
              "'delta.logRetentionDuration'='interval 45 days', 'delta.deletedFileRetentionDuration'='interval 364 days', " +
              "'delta.compatibility.symlinkFormatManifest.enabled'='false', 'private.property'='private-value')"
          ),
        initialData = (_, data) =>
          data.withColumn(
            "id",
            col("id")
              .as("id", new MetadataBuilder().putString("comment", "native comment 雪").build())
          ),
        initialPartitions = Map("a" -> Seq("region"))
      )
    finally policy.engine.close()
  }

  private class Fixture(val spark: SparkSession, val policy: TestDeltaRangerNativeSelect.Fixture)
      extends AutoCloseable {
    val location = NativeSelectFixture.locations("a")
    private val id = Identifier.of(Array("db"), "t")
    private val native = spark.sessionState.catalogManager
      .catalog("a")
      .asInstanceOf[NativeTableCatalog]
      .loadTable(id)
      .asInstanceOf[DeltaTableV2]
    val clock = new AtomicLong()
    val events = scala.collection.mutable.ArrayBuffer.empty[String]
    var reads = 0
    var replaceAt = Int.MaxValue
    var onRead: Int => Unit = (_: Int) => ()
    val client = mock(classOf[GravitinoClient])
    val catalog = mock(classOf[Catalog])
    private val tables = mock(classOf[TableCatalog])
    val actor = mock(classOf[Catalog])
    val actorTables = mock(classOf[TableCatalog])
    val registration = mock(classOf[MetadataTable])
    var actorResult: () => MetadataTable = () => registration
    var actorCatalogResult: () => Catalog = () => actor
    val properties = new LinkedHashMap[String, String](
      DeltaRegistrationReceipt
        .of(
          DeltaRegistrationIdentity.of(1, 1, "private-value-token"),
          DeltaRegistrationCommand.Operation.ADOPT,
          DeltaRegistrationReceipt.State.PUBLISHED,
          "run",
          DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT,
          "NONE",
          Optional.empty(),
          Optional.of(native.path.toString),
          Optional.of(native.initialSnapshot.metadata.id),
          OptionalLong.of(native.initialSnapshot.version)
        )
        .toProperties()
    )
    when(client.loadCatalog("a")).thenAnswer(_ => { events += "metadata-catalog"; catalog })
    Seq(catalog, actor).foreach { c =>
      when(c.name()).thenReturn("a"); when(c.provider()).thenReturn("lakehouse-delta")
    }
    when(catalog.asTableCatalog()).thenReturn(tables)
    when(actor.asTableCatalog()).thenReturn(actorTables)
    when(actorTables.loadTable(NameIdentifier.of("db", "t"))).thenAnswer(_ => {
      events += "actor"; actorResult()
    })
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenAnswer(_ => {
      reads += 1; onRead(reads); registration
    })
    when(registration.name()).thenReturn("t")
    when(registration.properties()).thenAnswer(_ => properties)
    private val converter = new SparkTypeConverter
    private val columns: Array[Column] =
      native.initialSnapshot.schema.fields.map(field =>
        Column.of(
          field.name,
          converter.toGravitinoType(field.dataType),
          null,
          field.nullable,
          false,
          Column.DEFAULT_VALUE_NOT_SET
        )
      )
    when(registration.columns()).thenReturn(columns)
    private val metadata = new DeltaRangerMetadataClient(
      () => clock.get(),
      mock(classOf[ScheduledExecutorService]),
      _ => client
    )
    private val directory = Files.createTempDirectory("table-metadata-journal")
    val wrapper = new DeltaRangerCatalogSpark35(
      new DeltaRangerCatalogSpark35.State(
        metadata,
        () => actorCatalogResult(),
        () => actor,
        _ => policy.engine
      )
    )
    wrapper.initialize(
      "a",
      new CaseInsensitiveStringMap(
        Collections.singletonMap("journal-directory", directory.resolve("journal").toString)
      )
    )
    private val field = spark.sessionState.catalogManager.getClass.getDeclaredField("catalogs")
    field.setAccessible(true)
    private val catalogs = field
      .get(spark.sessionState.catalogManager)
      .asInstanceOf[scala.collection.mutable.Map[String, CatalogPlugin]]
    private val original = catalogs.put("a", wrapper)

    def replaceCatalog(): Unit = original.foreach(catalogs.put("a", _))

    def restoreCatalog(): Unit = { catalogs.put("a", wrapper); () }

    def snapshotBytes(): Map[String, Vector[Byte]] = {
      val paths = Files.walk(Paths.get(location))
      try
        paths
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .map(path => path.toString -> Files.readAllBytes(path).toVector)
          .toMap
      finally paths.close()
    }

    override def close(): Unit = { original.foreach(catalogs.put("a", _)); wrapper.close() }
  }
}
