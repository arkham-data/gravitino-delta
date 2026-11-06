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
import java.security.MessageDigest
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.{SQLExecution, QueryExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

/** Native CDF completion with the shared writer's unchanged AQE startup profile. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.externalJdbc.enabled", matches = "true")
class TestDeltaRangerCdfAqeCompletion {
  private val mapper = new ObjectMapper()
  private val versionKey = "spark.databricks.delta.lastCommitVersionInSession"
  private val settings = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
    "spark.databricks.delta.optimizeWrite.enabled" -> "false",
    "spark.databricks.delta.autoCompact.enabled" -> "false",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "false",
    "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "false",
    "spark.databricks.delta.properties.defaults.enableChangeDataFeed" -> "false",
    "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
  )

  @Test
  def retainedJdbcCdfEnableThenAppendCompletesWithAqe(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val evidence = Paths.get(
      System.getenv("GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR"),
      "cdf-aqe-completion"
    )
    Files.createDirectories(evidence)
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          val registration = NativeSelectFixture.bindingFixture(spark, "a")
          val registered = registration.properties.asScala.toMap
          val target = Paths.get(NativeSelectFixture.locations("a"))
          Files.write(evidence.resolve("target.txt"), target.toString.getBytes("UTF-8"))
          assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
          assertFalse(spark.conf.getAll.contains("spark.databricks.delta.snapshotPartitions"))
          settings.foreach { case (key, value) => assertEquals(value, spark.conf.get(key)) }
          val log = DeltaLog.forTable(spark, target.toString)
          val before = log.unsafeVolatileSnapshot
          assertEquals(0L, before.version)
          assertEquals(
            Some("false"),
            before.metadata.configuration.get("delta.enableChangeDataFeed")
          )
          mapper.writeValue(
            evidence.resolve("runtime.json").toFile,
            Map(
              "spark" -> spark.version,
              "scala" -> scala.util.Properties.versionNumberString,
              "java" -> System.getProperty("java.version"),
              "sparkArtifact" -> classOf[
                SparkSession
              ].getProtectionDomain.getCodeSource.getLocation.toString,
              "deltaArtifact" -> classOf[
                DeltaLog
              ].getProtectionDomain.getCodeSource.getLocation.toString,
              "rangerArtifact" -> classOf[
                DeltaRangerPolicyEngine
              ].getProtectionDomain.getCodeSource.getLocation.toString,
              "aqe" -> spark.conf.get("spark.sql.adaptive.enabled"),
              "snapshotOverride" -> spark.conf.getAll
                .contains("spark.databricks.delta.snapshotPartitions")
                .toString
            ).asJava
          )
          val originalFiles = dataFiles(target)
          val originalRows = rows(spark, target, 0)
          val scope = GravitinoJdbcInput.open(spark, 1200)
          try {
            val options = ExternalJdbcTestFixture.options
            options.put("url", postgres.url())
            options.put("password", postgres.password())
            options.put(
              "dbtable",
              "(SELECT id, secret, region FROM public.source) arkham_temp_table"
            )
            val frame = scope.read(options)
            assertEquals(3L, frame.count())
            registration.wrapper.loadTable(Identifier.of(Array("db"), "t"))
            val originalSettings = spark.conf.getAll - versionKey
            val payloadReads = postgres.payloadReads()
            GravitinoDeltaTable.enableChangeDataFeed(spark, "a.db.t", frame)
            assertEquals(payloadReads, postgres.payloadReads())
            assertEquals("1", spark.conf.get(versionKey))
            assertEquals(Vector(0L, 1L), versions(target, ".json"))
            assertEquals(Vector(0L, 1L), versions(target, ".crc"))
            val enabled = log.unsafeVolatileSnapshot
            assertEquals(1L, enabled.version)
            assertEquals(before.metadata.id, enabled.metadata.id)
            assertEquals(before.metadata.schemaString, enabled.metadata.schemaString)
            assertEquals(
              Some("true"),
              enabled.metadata.configuration.get("delta.enableChangeDataFeed")
            )
            assertEquals(originalFiles, dataFiles(target))
            assertEquals(originalRows, rows(spark, target, 0))
            assertEquals(registered, registration.properties.asScala.toMap)
            assertEquals(originalSettings, spark.conf.getAll - versionKey)
            preserve(target, evidence.resolve("after-cdf"))
            assertEquals(3L, frame.count())
            frame.write.mode("append").saveAsTable("a.db.t")
            assertEquals("2", spark.conf.get(versionKey))
            assertEquals(Vector(0L, 1L, 2L), versions(target, ".json"))
            assertEquals(Vector(0L, 1L, 2L), versions(target, ".crc"))
            assertEquals(before.metadata.id, log.unsafeVolatileSnapshot.metadata.id)
            assertEquals(
              before.metadata.schemaString,
              log.unsafeVolatileSnapshot.metadata.schemaString
            )
            assertEquals(originalRows, rows(spark, target, 0))
            assertEquals(
              Vector((1L, "one", "keep"), (2L, "two", "drop"), (3L, null, "keep")),
              rows(spark, target, 2)
            )
            originalFiles.foreach { case (name, hash) =>
              assertEquals(hash, dataFiles(target)(name))
            }
            assertEquals(registered, registration.properties.asScala.toMap)
            assertEquals(originalSettings, spark.conf.getAll - versionKey)
            assertEquals(3L, frame.count())
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            Files.write(
              evidence.resolve("normal-return.txt"),
              "cdf=completed append=completed source=retained".getBytes("UTF-8")
            )
          } finally {
            preserve(target, evidence.resolve("terminal"))
            scope.close()
          }
        },
        Some(NativeSelectFixture.extension(policy)),
        settings,
        snapshotPartitions = None
      )
    finally { policy.close(); postgres.close() }
  }

  @Test
  def cdfAqeTransitionsRejectModifiedForeignAndExpiredEvidence(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val evidence =
      Paths.get(System.getenv("GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR"), "cdf-aqe-authority")
    Files.createDirectories(evidence)
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          val registration = NativeSelectFixture.bindingFixture(spark, "a")
          val target = Paths.get(NativeSelectFixture.locations("a"))
          Files.write(evidence.resolve("target.txt"), target.toString.getBytes("UTF-8"))
          val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
            "gravitino.delta.ranger.physical-evidence"
          )
          val foreignPlan = NativeSelectFixture.withNativeOperation(spark) { index =>
            val snapshot = index.getSnapshot
            DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
            snapshot.stateDF.queryExecution.executedPlan
          }
          val foreign = DeltaRangerPhysicalProvenance
            .nodes(foreignPlan)
            .flatMap(_.getTagValue(tag))
            .head
          val foreignSession = spark.newSession()
          var inspecting = false
          var inspected = false
          var exchange: ShuffleExchangeExec = null
          var collection: DeltaRangerCollectGuardExec = null
          var metadata: DeltaRangerCommitMetadata = null
          registration.onRead = _ => {
            if (
              !inspecting && !inspected && DeltaRangerCdfPropertyMutation.current.nonEmpty &&
              Thread
                .currentThread()
                .getStackTrace
                .exists(frame =>
                  frame.getClassName == classOf[DeltaRangerCollectGuardExec].getName &&
                    frame.getMethodName == "doExecute"
                )
            ) {
              inspecting = true
              try {
                val id = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
                val qe = SQLExecution.getQueryExecution(id.toLong)
                qe.executedPlan match {
                  case guard: DeltaRangerCollectGuardExec
                      if guard.child.isInstanceOf[AdaptiveSparkPlanExec] =>
                    val exchanges = DeltaRangerPhysicalProvenance.nodes(guard.child).collect {
                      case value: ShuffleExchangeExec => value
                    }
                    if (exchanges.nonEmpty) {
                      exchange = exchanges.head
                      collection = guard
                      metadata = DeltaRangerCdfPropertyMutation.current.get.metadata
                      assertSame(exchange, DeltaRangerPhysicalProvenance.finish(exchange, spark))
                      // A complete valid foreign recipe must never borrow CDF's stage skip.
                      assertTrue(
                        foreign.get.isInstanceOf[DeltaRangerPhysicalProvenance.NativeTicket]
                      )
                      (1 to 2).foreach { attempt =>
                        foreign.get.requireSession(spark)
                        foreign.get.verify(foreignPlan)
                        val foreignResult = DeltaRangerPhysicalProvenance.finish(foreignPlan, spark)
                        Files.write(
                          evidence.resolve(s"foreign-transition-$attempt.txt"),
                          foreignResult.getClass.getName.getBytes("UTF-8")
                        )
                        assertTrue(
                          foreignResult.isInstanceOf[DeltaRangerCollectGuardExec],
                          "An unrelated native owner borrowed the active CDF collection boundary"
                        )
                      }
                      val changed = exchange.copy(outputPartitioning = RoundRobinPartitioning(137))
                      changed.copyTagsFrom(exchange)
                      assertThrows(
                        classOf[SecurityException],
                        () => {
                          DeltaRangerPhysicalProvenance.finish(changed, spark); ()
                        }
                      )
                      val borrowed = exchange.clone()
                      DeltaRangerPhysicalProvenance
                        .nodes(borrowed)
                        .foreach(_.setTagValue(tag, foreign))
                      assertThrows(
                        classOf[SecurityException],
                        () => {
                          DeltaRangerPhysicalProvenance.finish(borrowed, spark); ()
                        }
                      )
                      val missing = exchange.clone()
                      DeltaRangerPhysicalProvenance.nodes(missing).foreach(_.unsetTagValue(tag))
                      assertThrows(
                        classOf[SecurityException],
                        () => {
                          DeltaRangerPhysicalProvenance.finish(missing, spark); ()
                        }
                      )
                      assertThrows(
                        classOf[SecurityException],
                        () => {
                          DeltaRangerPhysicalProvenance.finish(exchange, foreignSession); ()
                        }
                      )
                      val changedGuard = guard
                        .withNewChildren(Seq(changed))
                        .asInstanceOf[DeltaRangerCollectGuardExec]
                      assertThrows(classOf[SecurityException], () => changedGuard.executeCollect())
                      assertSame(exchange, DeltaRangerPhysicalProvenance.finish(exchange, spark))
                      inspected = true
                    }
                  case _ =>
                }
              } finally inspecting = false
            }
          }
          try {
            spark
              .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
              .collect()
            assertTrue(inspected, "Must probe an actual active CDF native AQE collection")
            val reconstructions = metadata.getClass.getDeclaredField("reconstructions")
            reconstructions.setAccessible(true)
            assertTrue(
              reconstructions
                .get(metadata)
                .asInstanceOf[scala.collection.mutable.ArrayBuffer[QueryExecution]]
                .nonEmpty,
              "The native guarded reconstruction must be recorded"
            )
            assertThrows(
              classOf[SecurityException],
              () => {
                DeltaRangerPhysicalProvenance.finish(exchange, spark); ()
              }
            )
            assertThrows(classOf[SecurityException], () => collection.executeCollect())
            val target = Paths.get(NativeSelectFixture.locations("a"))
            assertEquals(Vector(0L, 1L), versions(target, ".json"))
            assertEquals(Vector(0L, 1L), versions(target, ".crc"))
            assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
            assertFalse(spark.conf.getAll.contains("spark.databricks.delta.snapshotPartitions"))
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
          } finally {
            registration.onRead = _ => ()
            preserve(target, evidence.resolve("terminal"))
          }
        },
        Some(NativeSelectFixture.extension(policy)),
        settings,
        snapshotPartitions = None
      )
    finally policy.close()
  }

  private def actions(target: Path, version: Int): Vector[JsonNode] =
    Files
      .readAllLines(target.resolve(f"_delta_log/$version%020d.json"))
      .asScala
      .map(mapper.readTree)
      .toVector

  private def rows(
      spark: SparkSession,
      target: Path,
      version: Int
  ): Vector[(Long, String, String)] =
    actions(target, version)
      .filter(_.has("add"))
      .flatMap { action =>
        val reader = ParquetReader
          .builder[Group](
            new GroupReadSupport,
            new HadoopPath(target.resolve(action.path("add").path("path").asText()).toUri)
          )
          .withConf(spark.sparkContext.hadoopConfiguration)
          .build()
        try
          Iterator
            .continually(reader.read())
            .takeWhile(_ != null)
            .map { row =>
              (
                row.getLong("id", 0),
                if (row.getFieldRepetitionCount("secret") == 0) null
                else row.getString("secret", 0),
                row.getString("region", 0)
              )
            }
            .toVector
        finally reader.close()
      }
      .sortBy(_._1)

  private def versions(target: Path, suffix: String): Vector[Long] = {
    val files = Files.list(target.resolve("_delta_log"))
    try
      files
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .filter(name => name.endsWith(suffix) && name.stripSuffix(suffix).matches("[0-9]{20}"))
        .map(_.stripSuffix(suffix).toLong)
        .toVector
        .sorted
    finally files.close()
  }

  private def dataFiles(target: Path): Map[String, String] = {
    val files = Files.walk(target)
    try
      files
        .iterator()
        .asScala
        .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".parquet"))
        .map(p =>
          target.relativize(p).toString -> MessageDigest
            .getInstance("SHA-256")
            .digest(Files.readAllBytes(p))
            .map(b => f"${b & 0xff}%02x")
            .mkString
        )
        .toMap
    finally files.close()
  }

  private def preserve(target: Path, evidence: Path): Unit = {
    Files.createDirectories(evidence)
    val files = Files.list(target.resolve("_delta_log"))
    try
      files
        .iterator()
        .asScala
        .filter(p => p.toString.endsWith(".json") || p.toString.endsWith(".crc"))
        .foreach(p => Files.copy(p, evidence.resolve(p.getFileName)))
    finally files.close()
  }
}
