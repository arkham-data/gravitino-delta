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

private[ranger] class NativeBoundCatalogFixture(
    spark: SparkSession,
    policy: TestDeltaRangerNativeSelect.Fixture,
    catalogName: String = "a"
) extends AutoCloseable {
  private val name = Identifier.of(Array("db"), "t")
  private val nativeCatalog = new NativeSelectTestCatalog
  nativeCatalog.initialize(
    catalogName,
    new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
  )
  private val native = nativeCatalog.loadTable(name).asInstanceOf[DeltaTableV2]
  private def temporaryDirectory(prefix: String): Path = Files.createTempDirectory(prefix)
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
  }
}
