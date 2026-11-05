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
import scala.collection.JavaConverters._
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker
import org.apache.gravitino.spark.connector.delta.DeltaCreateLifecycle
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, TableSpec}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{Expressions, Transform, NamedReference}
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.mockito.Mockito.{mock, verifyNoInteractions}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Exact ordinary declarations through the installed SQL route and real native stage. */
class TestDeltaRangerOrdinaryCreate {
  import InstalledSchemaCreateBoundaryProbe._
  private val fixture = new TestDeltaRangerInstalledSchemaCreateBoundary
  private val ordinary = mapper.readTree(
    Files.readAllBytes(fixturePath.resolveSibling("spark-hub-ordinary-schema.json"))
  )
  private def values(name: String): Map[String, String] =
    ordinary.get(name).fields().asScala.map(e => e.getKey -> e.getValue.asText()).toMap
  private val schema = new StructType()
    .add("id", LongType, false, new MetadataBuilder().putString("comment", "clé 'id'").build())
    .add("region", StringType, true)
    .add("event_time", TimestampType, true)
    .add("amount", DecimalType(18, 4), true)
    .add("tags", ArrayType(StringType, true), true)
    .add("attributes", MapType(StringType, StringType, true), true)

  @Test
  def ordinarySqlCreatePublishesExactDeclaration(): Unit =
    fixture.withCase(
      "ordinary-exact",
      installed = true,
      observed = false,
      declaredSchema = schema,
      settingsOverride = Some(values("settings"))
    ) { (spark, transport, evidence) =>
      val properties = values("tableProperties")
      val entries =
        properties.toSeq.sorted.map { case (key, value) => s"'$key'='$value'" }.mkString(",")
      val sql = s"""CREATE TABLE a.db.created (id BIGINT NOT NULL COMMENT "clé 'id'", region STRING,
        event_time TIMESTAMP, amount DECIMAL(18,4), tags ARRAY<STRING>, attributes MAP<STRING,STRING>)
        USING delta PARTITIONED BY (region, id) COMMENT "table '雪'" TBLPROPERTIES ($entries)"""
      evidence.record("sql", sql)
      evidence.record("ordinary-fixture", ordinary.toString)
      Files.copy(
        fixturePath.resolveSibling("spark-hub-ordinary-schema.json"),
        evidence.directory.resolve("spark-hub-ordinary-schema.json")
      )
      transport.publishSuccess = true
      spark.sql(sql).collect()
      assertEquals(
        Vector("RESERVE", "PUBLISH"),
        transport.commands.map(_.operation().name()).toVector
      )
      assertEquals("PUBLISHED", transport.receipt.state().name())
      assertEquals(0L, transport.receipt.nativeVersion().getAsLong)
      val actions = Files.readAllLines(evidence.log0).asScala.map(mapper.readTree)
      val metadata = actions.find(_.has("metaData")).get.get("metaData")
      assertEquals(
        mapper.readTree(schema.json),
        mapper.readTree(metadata.get("schemaString").asText())
      )
      assertEquals(
        Vector("region", "id"),
        metadata.get("partitionColumns").elements().asScala.map(_.asText()).toVector
      )
      assertEquals("table '雪'", metadata.get("description").asText())
      val configuration = metadata
        .get("configuration")
        .fields()
        .asScala
        .map(e => e.getKey -> e.getValue.asText())
        .toMap
      assertEquals(
        properties ++ Map(
          "delta.columnMapping.mode" -> "none",
          "delta.dataSkippingNumIndexedCols" -> "-1"
        ),
        configuration - DeltaOperationMarker.PROPERTY_KEY
      )
      val marker = DeltaOperationMarker.decode(configuration(DeltaOperationMarker.PROPERTY_KEY))
      assertEquals(transport.receipt.identity().token(), marker.token())
      assertEquals(metadata.get("id").asText(), transport.receipt.nativeId().get())
      assertTrue(Files.exists(evidence.log0.resolveSibling("00000000000000000000.crc")))
      assertFalse(evidence.files().keys.exists(_.endsWith("00000000000000000001.json")))
      assertFalse(evidence.files().keys.exists(_.endsWith(".parquet")))
      assertEquals(0, loads)
    }

  @Test
  def freezesDeclarationAndBindsExactPartitions(): Unit =
    fixture.withCase("ordinary-frozen", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val input = new StructType()
          .add("id", LongType, false)
          .add("region", StringType, true)
          .add("value", StringType, true)
        val transforms =
          Array[Transform](Expressions.identity("REGION"), Expressions.identity("id"))
        val original = command(spark, input, transforms.toSeq)
        val declaration =
          DeltaRangerCreateDeclaration(original, original.tableSpec.asInstanceOf[TableSpec], spark)
        assertEquals(Vector("region", "id"), declaration.partitionColumns)
        def intent(names: String*) = new DeltaCreateLifecycle.Intent(
          "a",
          Vector("db").asJava,
          "created",
          declaration.schemaJson,
          names.map(Expressions.identity).toArray,
          declaration.properties.asJava
        )
        assertTrue(declaration.matches(intent("region", "id")))
        assertFalse(declaration.matches(intent("id", "region")))
        assertFalse(declaration.matches(intent("region", "value")))
        assertFalse(
          declaration.matches(
            new DeltaCreateLifecycle.Intent(
              "a",
              Vector("db").asJava,
              "created",
              declaration.schemaJson,
              2,
              declaration.properties.asJava
            )
          )
        )
        val frozen = declaration.schemaJson
        input.fields(0) = input.fields(0).copy(name = "changed")
        transforms(0) = Expressions.identity("value")
        declaration.schema.fields(0) = input.fields(0)
        val returned = declaration.partitions
        returned(0) = Expressions.identity("value")
        assertEquals(frozen, declaration.schema.json)
        assertEquals(
          Vector("region", "id"),
          DeltaRangerCreateDeclaration.identities(declaration.partitions).get
        )
        assertFalse(declaration.unchanged(original))
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def rejectsUnsupportedTypesMetadataPropertiesAndPartitionsBeforeReserve(): Unit =
    fixture.withCase("ordinary-invalid", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val ordinary = new StructType()
          .add("id", LongType)
          .add("region", StringType)
          .add("tags", ArrayType(StringType, true))
        val unknown = mock(classOf[Transform])
        val schemas = Vector(
          new StructType(),
          new StructType().add("", LongType),
          new StructType().add("bad name", StringType),
          new StructType().add("id", LongType).add("ID", LongType),
          new StructType().add("nested", new StructType().add("id", LongType)),
          new StructType().add("ntz", TimestampNTZType),
          new StructType().add("void", NullType),
          new StructType().add("nested", ArrayType(ArrayType(StringType, true), true)),
          new StructType().add("nonnull", ArrayType(StringType, false)),
          new StructType().add("map", MapType(StringType, StringType, false)),
          new StructType().add("map", MapType(LongType, StringType, true)),
          new StructType()
            .add("id", LongType, true, new MetadataBuilder().putLong("comment", 1L).build())
        ) ++ Vector(
          "delta.generationExpression",
          "delta.identity.start",
          "delta.columnMapping.id",
          "CURRENT_DEFAULT",
          "unknown"
        )
          .map(key =>
            new StructType()
              .add("id", LongType, true, new MetadataBuilder().putString(key, "x").build())
          )
        schemas.foreach(schema => reject(spark, command(spark, schema)))
        Vector(
          Seq(unknown),
          Seq(Expressions.years("id")),
          Seq(Expressions.bucket(2, "id")),
          Seq(Expressions.identity("nested.field")),
          Seq(Expressions.identity("absent")),
          Seq(Expressions.identity("id"), Expressions.identity("ID")),
          Seq(Expressions.identity("tags"))
        ).foreach(partitions => reject(spark, command(spark, ordinary, partitions)))
        verifyNoInteractions(unknown)
        Vector(
          "delta.enableChangeDataFeed" -> "false",
          "delta.compatibility.symlinkFormatManifest.enabled" -> "false",
          "delta.columnMapping.mode" -> "name",
          "delta.deletedFileRetentionDuration" -> "interval 0 days",
          "delta.logRetentionDuration" -> "interval 365 days",
          "delta.logRetentionDuration" -> "interval 1 hour",
          "path" -> "file:/tmp/foreign",
          "delta.registration.marker" -> "foreign",
          "unknown" -> "x",
          "primary_key" -> null
        ).foreach { entry =>
          reject(
            spark,
            command(spark, ordinary, properties = fixtureMap("tableProperties") + entry)
          )
        }
        spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
        try reject(spark, command(spark, ordinary))
        finally spark.conf.unset("spark.databricks.delta.properties.defaults.enableDeletionVectors")
        assertTrue(transport.commands.isEmpty)
        assertEquals(0, transport.reads)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def hubProfileRejectsUnqualifiedNativeTuningBeforeReserve(): Unit =
    fixture.withCase(
      "ordinary-native-tuning",
      installed = true,
      observed = false,
      settingsOverride = Some(values("settings"))
    ) { (spark, transport, evidence) =>
      val source = command(spark, new StructType().add("id", LongType))
      Vector(
        "spark.databricks.delta.optimizeWrite.maxShufflePartitions",
        "spark.databricks.delta.optimizeWrite.binSize",
        "spark.databricks.delta.autoCompact.minNumFiles"
      ).foreach { key =>
        spark.conf.set(key, "9")
        try reject(spark, source)
        finally spark.conf.unset(key)
      }
      assertTrue(transport.commands.isEmpty)
      assertTrue(evidence.files().isEmpty)
    }

  @Test
  def nativeDuplicateNamesAreRejectedEvenWithCaseSensitiveResolver(): Unit =
    fixture.withCase("ordinary-case-sensitive-duplicate", installed = true, observed = false) {
      (spark, transport, evidence) =>
        spark.conf.set("spark.sql.caseSensitive", "true")
        try reject(spark, command(spark, new StructType().add("id", LongType).add("ID", LongType)))
        finally spark.conf.unset("spark.sql.caseSensitive")
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def mutatedPartitionInputAndSettingsInvalidateIssuedCommand(): Unit =
    fixture.withCase("ordinary-mutated-command", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val input =
          new StructType().add("id", LongType).add("region", StringType).add("value", StringType)
        val partitions =
          Array[Transform](Expressions.identity("region"), Expressions.identity("id"))
        val original = command(spark, input, partitions.toSeq)
        val carrier = DeltaRangerCreateCommand
          .resolve(original, spark, _ => wrapper)
          .asInstanceOf[DeltaRangerCreateCommand.Carrier]
        partitions(0) = Expressions.identity("value")
        assertThrows(classOf[SecurityException], () => carrier.owner.plan(carrier, spark))
        partitions(0) = Expressions.identity("region")
        spark.conf.set(
          "spark.databricks.delta.properties.defaults.logRetentionDuration",
          "interval 20 days"
        )
        try assertThrows(classOf[SecurityException], () => carrier.owner.plan(carrier, spark))
        finally spark.conf.unset("spark.databricks.delta.properties.defaults.logRetentionDuration")
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def zeroAndOnePartitionsPreserveScalarsEmptyCommentAndDefaultOverride(): Unit = (0 to 1).foreach {
    count =>
      val all = new StructType()
        .add("id", LongType, false)
        .add("b", BooleanType)
        .add("tiny", ByteType)
        .add("small", ShortType)
        .add("i", IntegerType)
        .add("f", FloatType)
        .add("d", DoubleType)
        .add("text", StringType)
        .add("bytes", BinaryType)
        .add("day.`key", DateType)
        .add("ts", TimestampType)
        .add("amount", DecimalType(38, 18))
        .add("numbers", ArrayType(IntegerType, true))
      val settings = values(
        "settings"
      ) + ("spark.databricks.delta.properties.defaults.logRetentionDuration" -> "interval 45 days")
      fixture.withCase(
        "ordinary-partitions-" + count,
        installed = true,
        observed = false,
        declaredSchema = all,
        settingsOverride = Some(settings)
      ) { (spark, transport, evidence) =>
        val properties = values("tableProperties") - "primary_key" ++ Map(
          "delta.logRetentionDuration" -> "interval 1 days",
          "delta.deletedFileRetentionDuration" -> "interval 364 days",
          "delta.columnMapping.mode" -> "none"
        )
        val source = command(
          spark,
          all,
          if (count == 0) Seq.empty else Seq(Expressions.identity("`day.``key`")),
          properties,
          Some("")
        )
        val carrier = DeltaRangerCreateCommand
          .resolve(source, spark, _ => wrapper)
          .asInstanceOf[DeltaRangerCreateCommand.Carrier]
        transport.publishSuccess = true
        carrier.owner.plan(carrier, spark).run()
        val metadata = Files
          .readAllLines(evidence.log0)
          .asScala
          .map(mapper.readTree)
          .find(_.has("metaData"))
          .get
          .get("metaData")
        assertEquals(
          mapper.readTree(all.json),
          mapper.readTree(metadata.get("schemaString").asText())
        )
        assertEquals("", metadata.get("description").asText())
        assertEquals(count, metadata.get("partitionColumns").size())
        if (count == 1) assertEquals("day.`key", metadata.get("partitionColumns").get(0).asText())
        assertEquals(
          "interval 1 days",
          metadata.get("configuration").get("delta.logRetentionDuration").asText()
        )
        assertFalse(metadata.get("configuration").has("primary_key"))
        assertEquals("PUBLISHED", transport.receipt.state().name())
        assertTrue(Files.exists(evidence.log0.resolveSibling("00000000000000000000.crc")))
        assertFalse(evidence.files().keys.exists(_.endsWith(".parquet")))
        assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
        assertEquals(0, loads)
      }
  }

  @Test
  def ordinaryLiteralDenyAndPostLogFailureNeverReplay(): Unit =
    Vector("deny", "lost-publish").foreach { mode =>
      var real: RangerBasePlugin = null
      try
        fixture.withCase(
          "ordinary-fault-" + mode,
          installed = true,
          observed = false,
          declaredSchema = schema,
          settingsOverride = Some(values("settings")),
          policySetup = policy =>
            real = new TestDeltaRangerCreateCommand().realPolicies(policy, deny = mode == "deny")
        ) { (spark, transport, evidence) =>
          val source = command(
            spark,
            schema,
            Seq(Expressions.identity("region"), Expressions.identity("id")),
            values("tableProperties"),
            Some("table '雪'")
          )
          val carrier = DeltaRangerCreateCommand
            .resolve(source, spark, _ => wrapper)
            .asInstanceOf[DeltaRangerCreateCommand.Carrier]
          val physical = carrier.owner.plan(carrier, spark)
          val failure = assertThrows(classOf[Exception], () => physical.run())
          evidence.failed("expected-ordinary-" + mode, failure)
          if (mode == "deny") {
            assertTrue(transport.commands.isEmpty)
            assertFalse(Files.exists(evidence.log0))
          } else {
            assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
            assertTrue(failure.getMessage.contains("publication=STATUS_NEEDED"), failure.toString)
            assertEquals(
              Vector("RESERVE", "PUBLISH"),
              transport.commands.map(_.operation().name()).toVector
            )
            assertTrue(Files.exists(evidence.log0))
          }
          val commands = transport.commands.toVector
          val files = evidence.files()
          assertThrows(classOf[SecurityException], () => physical.run())
          assertEquals(commands, transport.commands.toVector)
          assertEquals(files, evidence.files())
          assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
          assertEquals(0, loads)
        }
      finally if (real != null) real.cleanup()
    }

  private def command(
      spark: SparkSession,
      schema: StructType,
      partitions: Seq[Transform] = Seq.empty,
      properties: Map[String, String] = fixtureMap("tableProperties"),
      comment: Option[String] = None
  ): CreateTable =
    CreateTable(
      ResolvedIdentifier(
        spark.sessionState.catalogManager.catalog("a"),
        Identifier.of(Array("db"), "created")
      ),
      schema,
      partitions,
      TableSpec(properties, Some("delta"), Map.empty, None, comment, None, false),
      false
    )

  private def reject(spark: SparkSession, command: CreateTable): Unit =
    assertThrows(
      classOf[Exception],
      () => { DeltaRangerCreateCommand.resolve(command, spark, _ => wrapper); () }
    )

}
