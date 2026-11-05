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
import java.util.{Optional, OptionalLong, UUID}
import scala.collection.JavaConverters._
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaOperationMarker,
  DeltaRegistrationCommand,
  DeltaRegistrationIdentity,
  DeltaRegistrationReceipt
}
import org.apache.gravitino.exceptions.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.gravitino.rel.{Table => MetadataTable}
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._

/** Positive-only existing-name observation through the installed CREATE command. */
class TestDeltaRangerCreateIfNotExists {
  import InstalledSchemaCreateBoundaryProbe._
  private val fixture = new TestDeltaRangerInstalledSchemaCreateBoundary
  private def statement(ignore: Boolean = true): String = {
    val properties =
      fixtureMap("tableProperties").toSeq.sorted.map { case (k, v) => s"'$k'='$v'" }.mkString(",")
    s"CREATE TABLE ${if (ignore) "IF NOT EXISTS " else ""}a.db.created (different STRING) USING delta COMMENT 'different' TBLPROPERTIES ($properties)"
  }
  private def published(transport: Transport): MetadataTable = {
    val table = mock(classOf[MetadataTable])
    when(table.name()).thenReturn("created")
    val receipt = DeltaRegistrationReceipt.of(
      DeltaRegistrationIdentity.of(11, 0, UUID.randomUUID().toString),
      DeltaRegistrationCommand.Operation.RESERVE,
      DeltaRegistrationReceipt.State.PUBLISHED,
      UUID.randomUUID().toString,
      DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
      "NONE",
      Optional.empty[DeltaOperationMarker](),
      Optional.of("file:/existing"),
      Optional.of(UUID.randomUUID().toString),
      OptionalLong.of(0)
    )
    when(table.properties()).thenReturn(receipt.toProperties())
    transport.visibleTable = table
    table
  }
  @Test
  def supportedDifferentDeclarationIsZeroEffectNoOp(): Unit =
    fixture.withCase("noop-positive", installed = true, observed = false) {
      (spark, transport, evidence) =>
        published(transport)
        spark.sql(statement()).collect()
        assertEquals(1, transport.reads)
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
        assertEquals(0, loads)
        val journal = evidence.directory.resolve("wrapper-journal")
        val entries = Files.list(journal)
        try assertEquals(0L, entries.count())
        finally entries.close()
    }

  @Test
  def absenceCreatesAndConcurrentReserveWinnerRemainsConflict(): Unit =
    Vector(false, true).foreach { race =>
      fixture.withCase("noop-absent-" + race, installed = true, observed = false) {
        (spark, transport, evidence) =>
          val sql = statement().replace("different STRING", "id BIGINT")
          val carrier = spark.sessionState.analyzer
            .execute(spark.sessionState.sqlParser.parsePlan(sql))
            .asInstanceOf[DeltaRangerCreateCommand.Carrier]
          val physical = carrier.owner.plan(carrier, spark)
          transport.publishSuccess = true
          if (race) transport.reserveFailure = new TableAlreadyExistsException("Concurrent winner")
          if (race) {
            val failure = assertThrows(classOf[Exception], () => physical.run())
            evidence.failed("expected-race", failure)
            assertTrue(failure.getMessage.contains("DELTA_REGISTRATION_STATUS_NEEDED"))
            assertEquals(Vector("RESERVE"), transport.commands.map(_.operation().name()).toVector)
            assertTrue(evidence.files().isEmpty)
          } else {
            physical.run()
            assertEquals(
              Vector("RESERVE", "PUBLISH"),
              transport.commands.map(_.operation().name()).toVector
            )
            assertEquals("PUBLISHED", transport.receipt.state().name())
            assertTrue(Files.exists(evidence.log0))
          }
          val commands = transport.commands.toVector
          val files = evidence.files()
          assertThrows(classOf[SecurityException], () => physical.run())
          assertEquals(commands, transport.commands.toVector)
          assertEquals(files, evidence.files())
      }
    }

  @Test
  def plainCreateConflictsAndUnsupportedNoOpStillRejects(): Unit =
    fixture.withCase("noop-existing-conflict", installed = true, observed = false) {
      (spark, transport, evidence) =>
        published(transport)
        assertThrows(classOf[Exception], () => { spark.sql(statement(false)).collect(); () })
        val reads = transport.reads
        assertThrows(
          classOf[Exception],
          () => {
            spark
              .sql(statement().replace("different STRING", "bad STRUCT<nested:BIGINT>"))
              .collect(); ()
          }
        )
        assertEquals(reads, transport.reads)
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def missingIsCaughtOnlyAtActualTableLoadAndFailuresNeverSucceed(): Unit =
    Vector(
      "denied",
      "unavailable",
      "expired-read",
      "wrong-table",
      "wrong-provider",
      "wrong-catalog",
      "malformed",
      "pending",
      "pending-receipt",
      "missing-properties",
      "missing-catalog-name"
    ).foreach { mode =>
      fixture.withCase("noop-invalid-" + mode, installed = true, observed = false) {
        (spark, transport, evidence) =>
          val table = published(transport)
          mode match {
            case "denied" =>
              transport.visibleTable = null;
              transport.metadataFailure = new SecurityException("metadata denied")
            case "unavailable" =>
              transport.visibleTable = null;
              transport.metadataFailure = new IllegalStateException("metadata unavailable")
            case "expired-read" =>
              transport.afterMetadataRead = () => transport.nanos = 301000000000L
            case "wrong-table" => when(table.name()).thenReturn("foreign")
            case "wrong-provider" =>
              when(transport.metadata.provider()).thenReturn("lakehouse-iceberg")
            case "wrong-catalog" => when(transport.metadata.name()).thenReturn("foreign")
            case "malformed" =>
              when(table.properties()).thenReturn(java.util.Collections.emptyMap[String, String]())
            case "pending" =>
              transport.visibleTable = null;
              transport.metadataFailure =
                new IllegalStateException("provider refuses pending registration")
            case "pending-receipt" =>
              val token = UUID.randomUUID().toString
              val marker = DeltaOperationMarker.create(
                11,
                0,
                token,
                "file:/existing",
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}"
              )
              val pending = DeltaRegistrationReceipt.of(
                DeltaRegistrationIdentity.of(11, 0, token),
                DeltaRegistrationCommand.Operation.RESERVE,
                DeltaRegistrationReceipt.State.RESERVED,
                UUID.randomUUID().toString,
                DeltaRegistrationReceipt.Evidence.NONE,
                "NONE",
                Optional.of(marker),
                Optional.empty[String](),
                Optional.empty[String](),
                OptionalLong.empty()
              )
              when(table.properties()).thenReturn(pending.toProperties())
            case "missing-properties" =>
              when(table.properties()).thenThrow(new NoSuchTableException("Not a load absence"))
            case "missing-catalog-name" =>
              when(transport.metadata.name())
                .thenThrow(new NoSuchTableException("Not a load absence"))
          }
          val failure =
            assertThrows(classOf[Exception], () => { spark.sql(statement()).collect(); () })
          evidence.failed("expected-" + mode, failure)
          assertTrue(transport.commands.isEmpty)
          assertTrue(evidence.files().isEmpty)
      }
    }

  @Test
  def literalCreateAndContextAreRecheckedAfterObservation(): Unit =
    Vector("denied-first", "denied-second", "settings", "subject", "expired-authorization")
      .foreach { mode =>
        var policy: TestDeltaRangerNativeSelect.Fixture = null
        fixture.withCase(
          "noop-context-" + mode,
          installed = true,
          observed = false,
          policySetup = p => policy = p
        ) { (spark, transport, evidence) =>
          published(transport)
          if (mode == "denied-first") policy.denied.add("a:*")
          transport.afterMetadataRead = () =>
            mode match {
              case "denied-second" => policy.denied.add("a:*")
              case "settings"      => spark.conf.set("spark.sql.shuffle.partitions", "99")
              case "subject" =>
                spark.sparkContext.setLocalProperty("kyuubi.session.user", "foreign")
              case "expired-authorization" =>
                policy.onRequest = _ => transport.nanos = 301000000000L
              case _ => ()
            }
          try {
            val failure =
              assertThrows(classOf[Exception], () => { spark.sql(statement()).collect(); () })
            evidence.failed("expected-" + mode, failure)
            assertEquals(if (mode == "denied-first") 0 else 1, transport.reads)
            assertTrue(transport.commands.isEmpty)
            assertTrue(evidence.files().isEmpty)
          } finally {
            spark.conf.set("spark.sql.shuffle.partitions", "2")
            spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
          }
        }
      }

  @Test
  def positiveNoOpCarrierCannotBeCopiedOrReused(): Unit =
    fixture.withCase("noop-one-shot", installed = true, observed = false) {
      (spark, transport, evidence) =>
        published(transport)
        val carrier = spark.sessionState.analyzer
          .execute(spark.sessionState.sqlParser.parsePlan(statement()))
          .asInstanceOf[DeltaRangerCreateCommand.Carrier]
        val physical = carrier.owner.plan(carrier, spark)
        assertThrows(
          classOf[SecurityException],
          () => carrier.owner.plan(carrier.copy()(carrier.owner), spark)
        )
        assertThrows(classOf[SecurityException], () => physical.copy()(carrier.owner).run())
        physical.run()
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(1, transport.reads)
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }
}
