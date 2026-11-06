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

import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._
import org.apache.gravitino.rel.{Column, Table => MetadataTable}
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.mockito.Mockito._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import java.util.concurrent.TimeUnit
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.types.{LongType, StringType, StructType, MetadataBuilder}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

/** Installed native metadata operations must preserve exactly their retained JDBC input. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.externalJdbc.enabled", matches = "true")
class TestDeltaRangerExternalJdbcLifecycle {
  import InstalledSchemaCreateBoundaryProbe._
  private val versionKey = "spark.databricks.delta.lastCommitVersionInSession"
  private val schema =
    new StructType().add("id", LongType).add("secret", StringType).add("region", StringType)
  private def options(postgres: RegisteredJdbcPostgresFixture): java.util.Map[String, String] = {
    val values = ExternalJdbcTestFixture.options
    values.put("url", postgres.url())
    values.put("password", postgres.password())
    values.put("dbtable", "(SELECT id, secret, region FROM public.source) arkham_temp_table")
    values
  }
  private def settings: Map[String, String] = {
    val ordinary = mapper.readTree(
      Files.readAllBytes(fixturePath.resolveSibling("spark-hub-ordinary-schema.json"))
    )
    ordinary
      .get("settings")
      .fields()
      .asScala
      .map(e => e.getKey -> e.getValue.asText())
      .toMap ++ Map(
      "spark.databricks.delta.optimizeWrite.enabled" -> "false",
      "spark.databricks.delta.autoCompact.enabled" -> "false",
      "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "false",
      "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
      "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
    )
  }

  @Test
  def retainedJdbcSchemaCreateThenAppendUsesOneFalseProfile(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      new TestDeltaRangerInstalledSchemaCreateBoundary().withCase(
        "jdbc-lifecycle-create",
        installed = true,
        observed = false,
        declaredSchema = new StructType(
          schema.fields.map(field =>
            field.copy(metadata =
              new MetadataBuilder()
                .putBoolean("isSigned", field.dataType == LongType)
                .putLong("scale", 0)
                .build()
            )
          )
        ),
        settingsOverride = Some(settings)
      ) { (spark, transport, evidence) =>
        spark.conf.set(versionKey, "73")
        val scope = GravitinoJdbcInput.open(spark, 1200)
        val unrelated = GravitinoJdbcInput.open(spark, 1200)
        try {
          val foreign = unrelated.read(options(postgres))
          val frame = scope.read(options(postgres))
          assertEquals(3L, frame.count())
          val payload = postgres.payloadReads()
          evidence.record("source-schema", frame.schema.json)
          transport.publishSuccess = true
          GravitinoDeltaTable.createForInput(
            spark,
            "a.db.created",
            frame.schema,
            Vector.empty[String].asJava,
            Map(
              "delta.enableChangeDataFeed" -> "true",
              "delta.compatibility.symlinkFormatManifest.enabled" -> "false"
            ).asJava,
            null,
            false,
            frame
          )
          assertEquals(payload, postgres.payloadReads())
          assertEquals("PUBLISHED", transport.receipt.state().name())
          assertEquals("0", spark.conf.get(versionKey))
          val nativeSchema = Files
            .readAllLines(evidence.log0)
            .asScala
            .map(mapper.readTree)
            .find(_.has("metaData"))
            .get
            .path("metaData")
            .path("schemaString")
            .asText()
          assertEquals(mapper.readTree(frame.schema.json), mapper.readTree(nativeSchema))
          assertThrows(classOf[SecurityException], () => foreign.count())
          val visible = mock(classOf[MetadataTable])
          when(visible.name()).thenReturn("created")
          when(visible.properties()).thenReturn(transport.receipt.toProperties())
          val converter = new SparkTypeConverter
          when(visible.columns()).thenReturn(
            schema.fields.map(field =>
              Column.of(
                field.name,
                converter.toGravitinoType(field.dataType),
                null,
                field.nullable,
                false,
                Column.DEFAULT_VALUE_NOT_SET
              ): Column
            )
          )
          transport.visibleTable = visible
          val commands = transport.commands.toVector
          val files = evidence.files()
          create(spark, frame, ignore = true)
          assertEquals(commands, transport.commands.toVector)
          assertEquals(files, evidence.files())
          frame.write.mode("append").saveAsTable("a.db.created")
          assertEquals("1", spark.conf.get(versionKey))
          assertWrite(spark, evidence.target, 1)
          assertTrue(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
          assertEquals(3L, frame.count())
        } finally { unrelated.close(); scope.close() }
      }
    finally postgres.close()
  }

  @Test
  def retainedJdbcCdfEnableThenAppendUsesOneScope(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          NativeSelectFixture.bindingFixture(spark, "a")
          spark.conf.set(versionKey, "73")
          val scope = GravitinoJdbcInput.open(spark, 1200)
          val participating = GravitinoJdbcInput.open(spark, 1200)
          try {
            val frame = scope.read(options(postgres))
            assertEquals(3L, frame.count())
            val sibling = scope.read(options(postgres))
            val other = participating.read(options(postgres))
            assertThrows(
              classOf[SecurityException],
              () => frame.as("left").join(sibling.as("right"), "id")
            )
            val payload = postgres.payloadReads()
            GravitinoDeltaTable.enableChangeDataFeed(spark, "a.db.t", frame)
            assertEquals(payload, postgres.payloadReads())
            assertEquals("1", spark.conf.get(versionKey))
            assertEquals(3L, sibling.count())
            assertThrows(classOf[SecurityException], () => other.count())
            frame.write.mode("append").saveAsTable("a.db.t")
            assertEquals("2", spark.conf.get(versionKey))
            assertWrite(spark, Paths.get(NativeSelectFixture.locations("a")), 2)
            assertEquals(3L, frame.count())
          } finally {
            val target = Paths.get(NativeSelectFixture.locations("a"))
            val root = Paths.get(
              System.getenv("GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR"),
              "jdbc-lifecycle-cdf"
            )
            Files.createDirectories(root)
            Files.write(root.resolve("target.txt"), target.toString.getBytes("UTF-8"))
            val logs = Files.list(target.resolve("_delta_log"))
            try
              logs
                .iterator()
                .asScala
                .filter(_.toString.endsWith(".json"))
                .foreach(p => Files.copy(p, root.resolve(p.getFileName)))
            finally logs.close()
            participating.close()
            scope.close()
          }
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
          "spark.databricks.delta.autoCompact.enabled" -> "false",
          "spark.databricks.delta.optimizeWrite.enabled" -> "false",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally { policy.close(); postgres.close() }
  }
  @Test
  def changedClosedExpiredAndForeignSourcesRejectBeforeReserve(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      new TestDeltaRangerInstalledSchemaCreateBoundary().withCase(
        "jdbc-lifecycle-invalid",
        installed = true,
        observed = false,
        declaredSchema = schema,
        settingsOverride = Some(settings)
      ) { (spark, transport, evidence) =>
        spark.conf.set(versionKey, "73")
        var nanos = 0L
        val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => nanos)
        try {
          val bound = scope.load(options(postgres))
          val frame = RegisteredJdbcPostgresFixture.frame(spark, LogicalRelation(bound.relation))
          val reads = postgres.payloadReads()
          spark.conf.set("spark.sql.shuffle.partitions", "7")
          try assertThrows(classOf[SecurityException], () => create(spark, frame))
          finally spark.conf.set("spark.sql.shuffle.partitions", "2")
          spark.sparkContext.setLocalProperty("kyuubi.session.user", "foreign-metadata-writer")
          try assertThrows(classOf[SecurityException], () => create(spark, frame))
          finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
          val foreign = spark.newSession()
          try {
            SparkSession.setActiveSession(foreign)
            assertThrows(classOf[SecurityException], () => create(foreign, frame))
          } finally SparkSession.setActiveSession(spark)
          nanos = TimeUnit.SECONDS.toNanos(1200)
          assertThrows(classOf[SecurityException], () => create(spark, frame))
          scope.close()
          assertThrows(classOf[SecurityException], () => create(spark, frame))
          assertTrue(transport.commands.isEmpty)
          assertTrue(evidence.files().isEmpty)
          assertEquals(reads, postgres.payloadReads())
        } finally scope.close()
      }
    finally postgres.close()
  }

  @Test
  def failedOrExpiredPublishedCreateDoesNotAdvanceInputOrReplay(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      Vector("lost-publication", "closed", "expired", "unrelated-setting").foreach { mode =>
        new TestDeltaRangerInstalledSchemaCreateBoundary().withCase(
          "jdbc-lifecycle-" + mode,
          installed = true,
          observed = false,
          declaredSchema = schema,
          settingsOverride = Some(settings)
        ) { (spark, transport, evidence) =>
          spark.conf.set(versionKey, "73")
          var nanos = 0L
          val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => nanos)
          try {
            val bound = scope.load(options(postgres))
            val frame = RegisteredJdbcPostgresFixture.frame(spark, LogicalRelation(bound.relation))
            transport.publishSuccess = mode != "lost-publication"
            transport.afterPublishedResponse = () =>
              mode match {
                case "closed"            => scope.close()
                case "expired"           => nanos = TimeUnit.SECONDS.toNanos(1200)
                case "unrelated-setting" => spark.conf.set("spark.sql.shuffle.partitions", "7")
                case _                   => ()
              }
            val failure = assertThrows(classOf[Exception], () => create(spark, frame))
            evidence.failed("expected-completion-" + mode, failure)
            assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
            assertTrue(Files.exists(evidence.log0))
            assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
            assertThrows(classOf[SecurityException], () => frame.count())
            assertEquals(
              Vector("RESERVE", "PUBLISH"),
              transport.commands.map(_.operation().name()).toVector
            )
          } finally {
            spark.conf.set("spark.sql.shuffle.partitions", "2")
            scope.close()
          }
        }
      }
    finally postgres.close()
  }

  @Test
  def explicitFalseProfileRejectsMixedAndUnknownMaintenanceBeforeReserve(): Unit =
    new TestDeltaRangerInstalledSchemaCreateBoundary().withCase(
      "jdbc-lifecycle-profiles",
      installed = true,
      observed = false,
      declaredSchema = schema,
      settingsOverride = Some(settings)
    ) { (spark, transport, evidence) =>
      Vector(
        "spark.databricks.delta.optimizeWrite.enabled" -> "true",
        "spark.databricks.delta.autoCompact.enabled" -> "true",
        "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "true",
        "spark.databricks.delta.optimizeWrite.binSize" -> "1"
      ).foreach { case (key, value) =>
        val old = spark.conf.getAll.get(key)
        spark.conf.set(key, value)
        try
          assertThrows(
            classOf[SecurityException],
            () =>
              GravitinoDeltaTable.create(
                spark,
                "a.db.created",
                schema,
                Vector.empty[String].asJava,
                Map(
                  "delta.enableChangeDataFeed" -> "true",
                  "delta.compatibility.symlinkFormatManifest.enabled" -> "false"
                ).asJava,
                null,
                false
              )
          )
        finally
          old match {
            case Some(original) => spark.conf.set(key, original); case None => spark.conf.unset(key)
          }
      }
      assertTrue(transport.commands.isEmpty)
      assertTrue(evidence.files().isEmpty)
    }

  @Test
  def cdfFailedRefreshDoesNotAdvanceSourceAndPreservesCommittedOutcome(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          val fixture = NativeSelectFixture.bindingFixture(spark, "a")
          val next =
            Paths.get(NativeSelectFixture.locations("a"), "_delta_log", "00000000000000000001.json")
          spark.conf.set(versionKey, "73")
          val scope = GravitinoJdbcInput.open(spark, 1200)
          try {
            val frame = scope.read(options(postgres))
            val payload = postgres.payloadReads()
            fixture.onRead = _ => {
              if (Files.exists(next) && DeltaRangerNativeOperation.current() == null)
                throw new IllegalStateException("injected CDF refresh failure")
            }
            val failure = assertThrows(
              classOf[Exception],
              () => GravitinoDeltaTable.enableChangeDataFeed(spark, "a.db.t", frame)
            )
            val causes =
              Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
            assertTrue(
              causes.exists(e =>
                Option(e.getMessage).exists(_.startsWith("CDF property change committed;"))
              )
            )
            assertTrue(Files.exists(next))
            assertFalse(Files.exists(next.resolveSibling("00000000000000000002.json")))
            assertThrows(classOf[SecurityException], () => frame.count())
            assertEquals(payload, postgres.payloadReads())
          } finally { fixture.onRead = _ => (); scope.close() }
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
          "spark.databricks.delta.autoCompact.enabled" -> "false",
          "spark.databricks.delta.optimizeWrite.enabled" -> "false",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally { policy.close(); postgres.close() }
  }

  private def assertWrite(spark: SparkSession, target: Path, version: Int): Unit = {
    val actions = Files
      .readAllLines(target.resolve(f"_delta_log/$version%020d.json"))
      .asScala
      .map(mapper.readTree)
    assertEquals(
      "WRITE",
      actions.find(_.has("commitInfo")).get.path("commitInfo").path("operation").asText()
    )
    assertFalse(actions.exists(_.has("remove")))
    val ids = actions.filter(_.has("add")).flatMap { action =>
      val reader = ParquetReader
        .builder[Group](
          new GroupReadSupport,
          new HadoopPath(target.resolve(action.path("add").path("path").asText()).toUri)
        )
        .withConf(spark.sparkContext.hadoopConfiguration)
        .build()
      try Iterator.continually(reader.read()).takeWhile(_ != null).map(_.getLong("id", 0)).toVector
      finally reader.close()
    }
    assertEquals(Seq(1L, 2L, 3L), ids.sorted.toSeq)
    assertFalse(Files.exists(target.resolve(f"_delta_log/${version + 1}%020d.json")))
  }

  private def create(spark: SparkSession, frame: DataFrame, ignore: Boolean = false): Unit =
    GravitinoDeltaTable.createForInput(
      spark,
      "a.db.created",
      schema,
      Vector.empty[String].asJava,
      Map(
        "delta.enableChangeDataFeed" -> "true",
        "delta.compatibility.symlinkFormatManifest.enabled" -> "false"
      ).asJava,
      null,
      ignore,
      frame
    )

}
