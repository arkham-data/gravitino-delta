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
import java.util.{ArrayList, LinkedHashMap}
import scala.collection.JavaConverters._
import io.delta.tables.DeltaTable
import org.apache.gravitino.rel.{Table => MetadataTable}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

/** The installed JVM schema facade executes the real issued command and native Delta stage. */
class TestGravitinoDeltaCreate {
  import InstalledSchemaCreateBoundaryProbe._
  private val fixture = new TestDeltaRangerInstalledSchemaCreateBoundary
  private def schema = new StructType()
    .add("id", LongType, false, new MetadataBuilder().putString("comment", "clé '雪'").build())
    .add("region", StringType)
    .add("tags", ArrayType(StringType, true))
    .add("attributes", MapType(StringType, StringType, true))
  private def create(spark: SparkSession, input: StructType, ignore: Boolean = false): Unit =
    GravitinoDeltaTable.create(
      spark,
      "`a`.`db`.`created`",
      input,
      Vector("region", "id").asJava,
      fixtureMap("tableProperties").asJava,
      "table '雪'",
      ignore
    )

  @Test
  def nativeSchemaCodecCanonicalNameAndCopiedInputsPublishWithoutRead(): Unit = {
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    val original = schema
    fixture.withCase(
      "frontend-native",
      installed = true,
      observed = false,
      declaredSchema = original,
      policySetup = p => policy = p
    ) { (spark, transport, evidence) =>
      evidence.origin(GravitinoDeltaTable.getClass)
      evidence.origin(classOf[TestGravitinoDeltaCreate])
      val input = spark.getClass
        .getMethod("parseDataType", classOf[String])
        .invoke(spark, original.json)
        .asInstanceOf[StructType]
      val partitions = new ArrayList[String](Vector("region", "id").asJava)
      val properties = new LinkedHashMap[String, String](fixtureMap("tableProperties").asJava)
      policy.onRequest = _ => {
        input.fields(0) = StructField("changed", StringType)
        partitions.clear()
        properties.clear()
      }
      transport.publishSuccess = true
      GravitinoDeltaTable.create(
        spark,
        "`a`.`db`.`created`",
        input,
        partitions,
        properties,
        "table '雪'",
        false
      )
      assertEquals(1, GravitinoDeltaTable.apiVersion())
      assertEquals(
        Vector("RESERVE", "PUBLISH"),
        transport.commands.map(_.operation().name()).toVector
      )
      assertEquals("PUBLISHED", transport.receipt.state().name())
      val metadata = Files
        .readAllLines(evidence.log0)
        .asScala
        .map(mapper.readTree)
        .find(_.has("metaData"))
        .get
        .get("metaData")
      assertEquals(
        mapper.readTree(original.json),
        mapper.readTree(metadata.get("schemaString").asText())
      )
      assertEquals(
        Vector("region", "id"),
        metadata.get("partitionColumns").elements().asScala.map(_.asText()).toVector
      )
      assertEquals("table '雪'", metadata.get("description").asText())
      assertEquals(0, loads)
      assertFalse(evidence.files().keys.exists(_.endsWith(".parquet")))
      assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
      val files = evidence.files()
      val commands = transport.commands.toVector
      transport.metadataFailure = new SecurityException("Explicit subsequent named read denied")
      val failure =
        assertThrows(classOf[Exception], () => { spark.table("a.db.created").collect(); () })
      evidence.failed("expected-later-read-denied", failure)
      assertEquals("PUBLISHED", transport.receipt.state().name())
      assertEquals(files, evidence.files())
      assertEquals(commands, transport.commands.toVector)
    }
  }

  @Test
  def facadeNoOpUsesPublishedReceiptWithoutNativeOrRead(): Unit =
    fixture.withCase("frontend-noop", installed = true, observed = false, declaredSchema = schema) {
      (spark, transport, evidence) =>
        transport.publishSuccess = true
        create(spark, schema)
        val table = mock(classOf[MetadataTable])
        when(table.name()).thenReturn("created")
        when(table.properties()).thenReturn(transport.receipt.toProperties())
        transport.visibleTable = table
        val files = evidence.files()
        val commands = transport.commands.toVector
        GravitinoDeltaTable.create(
          spark,
          "a.db.created",
          new StructType().add("different", StringType),
          Vector.empty[String].asJava,
          fixtureMap("tableProperties").asJava,
          null,
          true
        )
        assertEquals(files, evidence.files())
        assertEquals(commands, transport.commands.toVector)
        assertEquals(0, loads)
    }

  @Test
  def rejectsNoncanonicalUnsupportedAndWrongSessionWithoutEffects(): Unit =
    fixture.withCase("frontend-invalid", installed = true, observed = false) {
      (spark, transport, evidence) =>
        Vector("created", "db.created", "a.db.created.extra").foreach { name =>
          assertThrows(
            classOf[IllegalArgumentException],
            () =>
              GravitinoDeltaTable.create(
                spark,
                name,
                schema,
                Vector.empty[String].asJava,
                fixtureMap("tableProperties").asJava,
                null,
                false
              )
          )
        }
        assertThrows(
          classOf[Exception],
          () => create(spark, new StructType().add("bad", new StructType().add("nested", LongType)))
        )
        assertThrows(classOf[Exception], () => create(spark.newSession(), schema))
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
        try assertThrows(classOf[Exception], () => create(spark, schema))
        finally spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def stockDeltaBuilderDoesNotSupportCanonicalMultipartTarget(): Unit =
    fixture.withCase("frontend-stock-builder", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val failure = assertThrows(
          classOf[Exception],
          () => {
            DeltaTable.create(spark).tableName("a.db.created").addColumns(schema).execute(); ()
          }
        )
        evidence.failed("expected-stock-builder", failure)
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
        assertEquals(0, loads)
    }

  @Test
  def absentCompanionCannotExecuteUnissuedCreate(): Unit =
    fixture.withCase("frontend-no-companion", installed = false, observed = false) {
      (spark, transport, evidence) =>
        val failure = assertThrows(classOf[SecurityException], () => create(spark, schema))
        assertTrue(failure.getMessage.contains("issued owned command"))
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def stockBuilderFinalObservationDoesNotConfirmCreation(): Unit =
    fixture.withCase("frontend-stock-final-read", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val catalog = spark.sessionState.catalogManager.currentCatalog.name()
        spark.sessionState.catalogManager.setCurrentCatalog("a")
        try {
          transport.publishSuccess = true
          val builder = DeltaTable
            .create(spark)
            .tableName("db.created")
            .addColumns(new StructType().add("id", LongType))
          fixtureMap("tableProperties").foreach { case (key, value) =>
            builder.property(key, value)
          }
          val failure = assertThrows(classOf[Exception], () => { builder.execute(); () })
          evidence.failed("expected-stock-final-observation", failure)
          assertTrue(failure.getMessage.contains("DELTA_MISSING_DELTA_TABLE"))
          evidence.record(
            "stock-builder-outcome",
            s"commands=${transport.commands.map(_.operation())} reads=${transport.reads} loads=$loads"
          )
          assertEquals(
            Vector("RESERVE", "PUBLISH"),
            transport.commands.map(_.operation().name()).toVector
          )
          assertEquals("PUBLISHED", transport.receipt.state().name())
          assertTrue(Files.exists(evidence.log0))
          assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
        } finally spark.sessionState.catalogManager.setCurrentCatalog(catalog)
    }
}
