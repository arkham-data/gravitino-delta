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
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.util.{Collections, LinkedHashMap, Optional, OptionalLong}
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
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
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog => NativeTableCatalog}
import org.apache.spark.sql.delta.{DeltaLog, DeltaRelation}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import scala.collection.JavaConverters._
import org.mockito.Mockito._

class TestDeltaRangerRegistrationBindingsNative {
  private val temporaryDirectories = scala.collection.mutable.ArrayBuffer.empty[Path]
  private val name = Identifier.of(Array("db"), "t")

  @Test
  def realWrapperUsesOneLeaseAndOrdinaryFallbackResolvesExactNativeObjects(): Unit =
    withWrapperSpark { spark =>
      val policy = new TestDeltaRangerNativeSelect.Fixture
      val otherPolicy = new TestDeltaRangerNativeSelect.Fixture
      val f = new WrapperFixture(spark, policy)
      NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
      val other = new WrapperFixture(spark, otherPolicy, "b")
      try {
        val caller = UserGroupInformation.getCurrentUser.getShortUserName
        val loaded = f.wrapper.loadTable(name).asInstanceOf[DeltaTableV2]
        val logical = fallback(loaded)
        val scan = f.wrapper.registrationBindings().resolve(logical)
        assertTrue(scan.descriptor() eq loaded.catalogTable.get)
        assertTrue(scan.relation() eq loaded.toBaseRelation)
        assertTrue(scan.index() eq logical.relation.asInstanceOf[HadoopFsRelation].location)
        assertTrue(scan.log() eq loaded.deltaLog)
        assertTrue(loaded.withOptions(Map.empty) eq loaded)
        assertTrue(loaded.ttSafeCatalogTable.get eq loaded.catalogTable.get)
        assertTrue(loaded.toBaseRelation eq loaded.toBaseRelation)
        assertEquals(
          loaded.initialSnapshot.metadata.id,
          scan.index().snapshotAtAnalysis.metadata.id
        )
        assertEquals(loaded.initialSnapshot.version, scan.index().snapshotAtAnalysis.version)
        assertEquals(
          loaded.initialSnapshot.metadata.schemaString,
          scan.index().snapshotAtAnalysis.metadata.schemaString
        )
        assertEquals(3, f.reads.get())
        assertEquals(1, f.opens.get())
        assertEquals(caller, UserGroupInformation.getCurrentUser.getShortUserName)
        verify(f.client, times(3)).loadCatalog("a")
        verifyNoMoreInteractions(f.client)
        val second = other.wrapper.loadTable(name).asInstanceOf[DeltaTableV2]
        assertEquals(loaded.path, second.path)
        assertThrows(
          classOf[SecurityException],
          () => other.wrapper.registrationBindings().resolve(logical)
        )
        val secondLogical = fallback(second)
        assertThrows(
          classOf[SecurityException],
          () => f.wrapper.registrationBindings().resolve(secondLogical)
        )
      } finally { other.close(); f.close(); policy.engine.close() }
    }

  @Test
  def realWrapperPreservesMappedSchemaAndMiddlePositionPartitionColumn(): Unit =
    withWrapperSpark { spark =>
      val partitioned = temporaryDirectory("bound-partition-table").resolve("data").toString
      spark.read
        .format("delta")
        .load(NativeSelectFixture.locations("a"))
        .select("id", "region", "secret")
        .write
        .format("delta")
        .partitionBy("region")
        .save(partitioned)
      NativeSelectFixture.locations += "p" -> partitioned
      spark.conf.set("spark.sql.catalog.p", classOf[NativeSelectTestCatalog].getName)
      Seq("p", "q").foreach { catalogName =>
        val f = new WrapperFixture(spark, new TestDeltaRangerNativeSelect.Fixture, catalogName)
        try {
          val loaded =
            try f.wrapper.loadTable(name).asInstanceOf[DeltaTableV2]
            catch {
              case failure: SecurityException =>
                throw new AssertionError("Rejected native fixture " + catalogName, failure)
            }
          val logical = fallback(loaded)
          val scan = f.wrapper.registrationBindings().resolve(logical)
          assertEquals(loaded.initialSnapshot.metadata.schemaString, scan.nativeSchemaJson())
          assertTrue(scan.index() eq logical.relation.asInstanceOf[HadoopFsRelation].location)
          if (catalogName == "p") {
            assertEquals(
              Seq("id", "region", "secret"),
              loaded.initialSnapshot.schema.fieldNames.toSeq
            )
            assertEquals(Seq("region"), scan.index().metadata.partitionColumns)
            assertEquals(Seq("region"), scan.descriptor().partitionColumnNames)
          } else {
            assertTrue(scan.nativeSchemaJson().contains("delta.columnMapping.id"))
            assertFalse(scan.observation().schemaJson().contains("delta.columnMapping.id"))
            assertTrue(scan.mappingConfigurationStamp().contains("delta.columnMapping.mode"))
          }
        } finally f.close()
      }
    }

  @Test
  def realWrapperRejectsFinalReceiptAndSchemaChanges(): Unit =
    withWrapperSpark { spark =>
      val policy = new TestDeltaRangerNativeSelect.Fixture
      try {
        val changes = Seq(
          "id" -> "2",
          "generation" -> "2",
          "token" -> "changed-token",
          "location" -> "file:/different",
          "nativeId" -> "other-native",
          "nativeVersion" -> "99"
        )
        (for (phase <- Seq(2, 3); change <- changes) yield phase -> change).foreach {
          case (phase, (field, value)) =>
            val f = new WrapperFixture(spark, new TestDeltaRangerNativeSelect.Fixture)
            try {
              f.onRead = number =>
                if (number == phase) f.properties.put("delta.registration.receipt." + field, value)
              assertThrows(classOf[SecurityException], () => f.wrapper.loadTable(name))
              assertTrue(f.reads.get() >= phase)
            } finally f.close()
        }
        val f = new WrapperFixture(spark, new TestDeltaRangerNativeSelect.Fixture)
        try {
          f.onRead = number => if (number == 3) f.columns = f.columns.reverse
          assertThrows(classOf[SecurityException], () => f.wrapper.loadTable(name))
        } finally f.close()
      } finally policy.engine.close()
    }

  @Test
  def concurrentCloseDoesNotWaitForFinalMetadataBracketOrPublishAfterClose(): Unit =
    withWrapperSpark { spark =>
      val policy = new TestDeltaRangerNativeSelect.Fixture
      val f = new WrapperFixture(spark, policy)
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val executor = Executors.newSingleThreadExecutor()
      f.onRead = number =>
        if (number == 3) {
          entered.countDown()
          assertTrue(release.await(10, TimeUnit.SECONDS))
        }
      try {
        val bindings = f.wrapper.registrationBindings()
        val loading = executor.submit(new Runnable {
          override def run(): Unit = {
            SparkSession.setActiveSession(spark)
            try assertThrows(classOf[SecurityException], () => f.wrapper.loadTable(name))
            finally SparkSession.clearActiveSession()
          }
        })
        assertTrue(entered.await(10, TimeUnit.SECONDS))
        f.wrapper.close()
        assertEquals(1L, release.getCount)
        release.countDown()
        loading.get(10, TimeUnit.SECONDS)
        assertThrows(classOf[SecurityException], () => bindings.requireLive())
      } finally {
        release.countDown(); executor.shutdownNow(); f.close(); policy.engine.close()
      }
    }

  @Test
  def installedProductionExtensionColdAndWarmLoadsNeedNoAdmissionWidening(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      withWrapperSpark(
        spark => {
          val f = new WrapperFixture(spark, policy)
          try {
            DeltaLog.clearCache()
            val cold = f.wrapper.loadTable(name).asInstanceOf[DeltaTableV2]
            assertNotNull(f.wrapper.registrationBindings().resolve(fallback(cold)))
            val warm = f.wrapper.loadTable(name).asInstanceOf[DeltaTableV2]
            assertNotNull(f.wrapper.registrationBindings().resolve(fallback(warm)))
          } finally f.close()
        },
        Some(extensions => NativeSelectFixture.extension(policy)(extensions))
      )
    finally policy.engine.close()
  }

  private def withWrapperSpark(
      test: SparkSession => Unit,
      extension: Option[SparkSessionExtensions => Unit] = None
  ): Unit = {
    val config =
      temporaryDirectory("registration-binding-startup").resolve("metadata.properties")
    Files.write(
      config,
      "auth-type=simple\nprincipal=separate-metadata-reader\nlifetime-seconds=300\n".getBytes(
        StandardCharsets.UTF_8
      )
    )
    Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-------"))
    try
      NativeSelectFixture.withSpark(
        test,
        extension,
        Map(
          "spark.gravitino.enableDeltaSupport" -> "true",
          "spark.gravitino.enableDeltaRangerSupport" -> "true",
          "spark.sql.gravitino.uri" -> "http://127.0.0.1:1",
          "spark.sql.gravitino.metalake" -> "lake",
          "spark.gravitino.deltaRanger.metadata.config" -> config.toString
        )
      )
    finally {
      temporaryDirectories.reverse.foreach { directory =>
        val paths = Files.walk(directory)
        try
          paths
            .iterator()
            .asScala
            .toSeq
            .sortBy(_.getNameCount)
            .reverse
            .foreach(Files.deleteIfExists(_))
        finally paths.close()
      }
      temporaryDirectories.clear()
    }
  }

  private def temporaryDirectory(prefix: String): Path = {
    val directory = Files.createTempDirectory(prefix)
    temporaryDirectories += directory
    directory
  }

  private def fallback(table: DeltaTableV2): LogicalRelation =
    DeltaRelation.fromV2Relation(
      table,
      DataSourceV2Relation.create(table, None, Some(name)),
      new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
    )

  private class WrapperFixture(
      spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      catalogName: String = "a"
  ) extends AutoCloseable {
    private val native = spark.sessionState.catalogManager
      .catalog(catalogName)
      .asInstanceOf[NativeTableCatalog]
      .loadTable(name)
      .asInstanceOf[DeltaTableV2]
    val reads = new AtomicInteger()
    val opens = new AtomicInteger()
    val clock = new AtomicLong()
    val client = mock(classOf[GravitinoClient])
    private val catalog = mock(classOf[Catalog])
    private val tables = mock(classOf[TableCatalog])
    private val registration = mock(classOf[MetadataTable])
    val metadata = new DeltaRangerMetadataClient(
      () => clock.get(),
      mock(classOf[ScheduledExecutorService]),
      _ => { opens.incrementAndGet(); client }
    )
    val properties = new LinkedHashMap[String, String](
      DeltaRegistrationReceipt
        .of(
          DeltaRegistrationIdentity.of(1, 1, "private-wrapper-token"),
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
    private val converter = new SparkTypeConverter
    var columns: Array[Column] = native.initialSnapshot.schema.fields.map(field =>
      Column.of(
        field.name,
        converter.toGravitinoType(field.dataType),
        null,
        field.nullable,
        false,
        Column.DEFAULT_VALUE_NOT_SET
      )
    )
    var onRead: Int => Unit = (_: Int) => ()
    when(client.loadCatalog(catalogName)).thenReturn(catalog)
    when(catalog.name()).thenReturn(catalogName)
    when(catalog.provider()).thenReturn("lakehouse-delta")
    when(catalog.asTableCatalog()).thenReturn(tables)
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenAnswer(_ => {
      onRead(reads.incrementAndGet()); registration
    })
    when(registration.name()).thenReturn("t")
    when(registration.properties()).thenAnswer(_ => properties)
    when(registration.columns()).thenAnswer(_ => columns)
    private val directory = temporaryDirectory("registration-bindings-wrapper")
    private val extensions = spark.conf.get("spark.sql.extensions")
    spark.sessionState.conf
      .setConfString("spark.sql.extensions", DeltaRangerCatalogSpark35.EXTENSION + "," + extensions)
    val wrapper = new DeltaRangerCatalogSpark35(
      new DeltaRangerCatalogSpark35.State(
        metadata,
        () => throw new AssertionError("Read must not use actor supplier"),
        () => catalog,
        _ => policy.engine
      )
    )
    wrapper.initialize(
      catalogName,
      new CaseInsensitiveStringMap(
        Collections.singletonMap("journal-directory", directory.resolve("journal").toString)
      )
    )
    override def close(): Unit = {
      wrapper.close()
      spark.sessionState.conf.setConfString("spark.sql.extensions", extensions)
    }
  }
}
