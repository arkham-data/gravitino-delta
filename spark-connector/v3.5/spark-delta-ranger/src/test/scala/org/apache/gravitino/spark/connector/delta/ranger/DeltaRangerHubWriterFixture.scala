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

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.sql.Connection
import java.util.{LinkedHashMap, UUID}
import scala.collection.JavaConverters._

import org.apache.gravitino.NameIdentifier
import org.apache.gravitino.rel.{Column, Table => MetadataTable}
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.types._
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

/** Actual production extension/catalog with scripted provider transport for Python runner tests. */
final class DeltaRangerHubWriterFixture extends AutoCloseable {
  import InstalledSchemaCreateBoundaryProbe._

  private val evidence = fresh("hub-shared-writer-" + UUID.randomUUID().toString)
  private val postgres = new RegisteredJdbcPostgresFixture
  private val policy = new TestDeltaRangerNativeSelect.Fixture
  private val inputSchema = new StructType(
    new StructType()
      .add("id", LongType)
      .add("secret", StringType)
      .add("region", StringType)
      .fields
      .map(field =>
        field.copy(metadata =
          new MetadataBuilder()
            .putBoolean("isSigned", field.dataType == LongType)
            .putLong("scale", 0)
            .build()
        )
      )
  ).add("__arkham_loaded_at", TimestampType, false)
  private val transport = new Transport(evidence, inputSchema)
  private val metadataConfig = evidence.directory.resolve("metadata.properties")
  write(
    metadataConfig,
    "auth-type=simple\nprincipal=separate-metadata-reader\nlifetime-seconds=300\n"
  )
  Files.setPosixFilePermissions(metadataConfig, PosixFilePermissions.fromString("rw-------"))
  private val ordinary = mapper.readTree(
    Files.readAllBytes(fixturePath.resolveSibling("spark-hub-ordinary-schema.json"))
  )
  private val settings = ordinary
    .get("settings")
    .fields()
    .asScala
    .map(e => e.getKey -> e.getValue.asText())
    .toMap ++ Map(
    "spark.gravitino.enableDeltaSupport" -> "true",
    "spark.gravitino.enableDeltaRangerSupport" -> "true",
    "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
    "spark.databricks.delta.optimizeWrite.enabled" -> "false",
    "spark.databricks.delta.autoCompact.enabled" -> "false",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "false",
    "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "false",
    "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
    "spark.sql.extensions" -> (classOf[
      DeltaRangerSparkExtension
    ].getName + ",io.delta.sql.DeltaSparkSessionExtension"),
    "spark.sql.sources.partitionOverwriteMode" -> "static"
  )
  private val builder = SparkSession
    .builder()
    .master("local[2]")
    .appName("hub-real-shared-writer")
    .config("spark.sql.gravitino.uri", "http://127.0.0.1:1")
    .config("spark.sql.gravitino.metalake", "lake")
    .config("spark.gravitino.deltaRanger.metadata.config", metadataConfig.toString)
    .config("spark.ui.enabled", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.warehouse.dir", evidence.directory.resolve("warehouse").toString)
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.catalog.update.enabled", "false")
  settings.foreach { case (key, value) => builder.config(key, value) }
  private val spark = builder.getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  private val bound = new Wrapper(transport, policy, evidence)
  private val field = spark.sessionState.catalogManager.getClass.getDeclaredField("catalogs")
  field.setAccessible(true)
  private val catalogs = field
    .get(spark.sessionState.catalogManager)
    .asInstanceOf[scala.collection.mutable.Map[String, CatalogPlugin]]
  require(!catalogs.contains("a"), "Fixture catalog name already exists")
  catalogs.put("a", bound.catalog)
  private val registration = mock(classOf[MetadataTable])
  private val properties = new LinkedHashMap[String, String]()
  private var columns = Array.empty[Column]
  private var version = -1L
  private var nativeMetadata: Metadata = null
  private var lock: Connection = null
  when(registration.name()).thenReturn("created")
  when(registration.properties()).thenAnswer(_ => properties)
  when(registration.columns()).thenAnswer(_ => columns)
  when(transport.actor.asTableCatalog().loadTable(any(classOf[NameIdentifier])))
    .thenAnswer(call =>
      transport.metadata.asTableCatalog().loadTable(call.getArgument[NameIdentifier](0))
    )
  transport.publishSuccess = true
  // Model provider load synchronization only when the real wrapper invokes the
  // metadata transport. No Python post-write repair or production replacement.
  transport.afterPublishedResponse = () => refreshPublishedMetadata()
  transport.afterMetadataRead = () => {
    if (transport.receipt != null && transport.receipt.state().name() == "PUBLISHED")
      refreshPublishedMetadata()
  }

  /** Returns the session installed with the real production companion extension. */
  def session(): SparkSession = spark

  /** Returns noncredential runtime settings explicitly passed to the application runtime. */
  def profile(): java.util.Map[String, String] = settings.asJava

  /** Returns source credentials over the local test gateway; callers must not log them. */
  def options(): java.util.Map[String, String] = {
    val values = ExternalJdbcTestFixture.options
    values.put("url", postgres.url()); values.put("password", postgres.password()); values
  }

  /** Returns the actual installed catalog type, not its scripted transport. */
  def catalogClass(): String = spark.sessionState.catalogManager.catalog("a").getClass.getName

  /** Returns the fresh owned native directory for independent file inspection. */
  def targetPath(): String = evidence.target.toString

  /** Holds the explicit payload advisory lock after source schema discovery. */
  def block(): Unit = { require(lock == null); lock = postgres.blockExternalPayload() }

  /** Counts native PostgreSQL statements blocked on the fixture lock. */
  def blocked(): Int = postgres.blockedSchemaReads()

  /** Advances PostgreSQL after a count statement acquired its snapshot. */
  def advance(): Unit = postgres.advanceSource()

  /** Releases the source barrier without retrying an application action. */
  def unblock(): Unit = { if (lock != null) { lock.close(); lock = null } }

  /** Reports the last provider load's exact observed registration and native metadata. */
  def registrationJson(): String = {
    val report = mapper.createObjectNode()
    report.put("version", version)
    report.put("nativeId", nativeMetadata.id)
    report.put("schema", nativeMetadata.schemaString)
    report.put("receipt", mapper.writeValueAsString(properties))
    report.toString
  }

  /** Disposes this fixture's resources while retaining every target/evidence file. */
  override def close(): Unit = {
    unblock()
    try { bound.close(); policy.close(); spark.stop() }
    finally {
      postgres.close(); evidence.close()
      SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
    }
  }

  private def refreshPublishedMetadata(): Unit = synchronized {
    val files = Files.list(evidence.target.resolve("_delta_log"))
    val logs =
      try
        files
          .iterator()
          .asScala
          .filter(_.getFileName.toString.endsWith(".json"))
          .toVector
          .sortBy(_.getFileName.toString)
      finally files.close()
    val latest = logs.last.getFileName.toString.stripSuffix(".json").toLong
    if (latest > version) {
      val metadata = logs
        .flatMap(path => Files.readAllLines(path).asScala.map(Action.fromJson))
        .collect { case value: Metadata => value }
        .last
      if (nativeMetadata != null)
        require(nativeMetadata.id == metadata.id, "Native table identity changed")
      val converter = new SparkTypeConverter
      val observedColumns = metadata.schema.fields.map(f =>
        Column.of(
          f.name,
          converter.toGravitinoType(f.dataType),
          null,
          f.nullable,
          false,
          Column.DEFAULT_VALUE_NOT_SET
        ): Column
      )
      require(
        columns.map(_.name()).sameElements(observedColumns.take(columns.length).map(_.name())),
        "Existing columns changed"
      )
      properties.clear(); properties.putAll(transport.receipt.toProperties())
      properties.put("delta.registration.receipt.nativeVersion", latest.toString)
      columns = observedColumns; version = latest; nativeMetadata = metadata
      transport.visibleTable = registration
      evidence.record(
        "provider-load-native-sync",
        s"version=$version nativeId=${metadata.id} schema=${metadata.schemaString}"
      )
    }
  }
}
