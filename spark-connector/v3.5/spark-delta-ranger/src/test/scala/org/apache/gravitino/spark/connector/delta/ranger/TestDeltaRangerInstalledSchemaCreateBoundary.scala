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
import java.util.{Collections, LinkedHashMap}
import scala.collection.JavaConverters._
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker
import org.apache.gravitino.spark.connector.delta.{
  DeltaRegistrationJournal,
  GravitinoDeltaCatalogSpark35
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Bounded schema-only CREATE characterization; native failures are evidence, not authorization. */
class TestDeltaRangerInstalledSchemaCreateBoundary {
  import InstalledSchemaCreateBoundaryProbe._
  private val schema = new StructType().add("id", LongType, nullable = true)
  private val identifier = Identifier.of(Array("db"), "created")
  private val lastCommit = "spark.databricks.delta.lastCommitVersionInSession"

  @Test
  def issuedNamedSchemaCreatePublishesWithoutSelect(): Unit =
    withCase("A-issued-named", installed = true, observed = false) { (spark, transport, evidence) =>
      val properties = fixtureMap("tableProperties")
      val declaration = properties.toSeq.sorted
        .map { case (key, value) => "'" + key + "'='" + value + "'" }
        .mkString(",")
      val sql = s"CREATE TABLE a.db.created (id BIGINT) USING delta TBLPROPERTIES ($declaration)"
      evidence.record("sql", sql)
      val parsed = spark.sessionState.sqlParser.parsePlan(sql)
      assertSame(parsed, new ResolutionObserver(spark, evidence).apply(parsed))
      // Scripted registration transport, real wrapper, base stage and installed native execution.
      transport.publishSuccess = true
      spark.sql(sql).collect()
      assertEquals(
        Vector("RESERVE", "PUBLISH"),
        transport.commands.map(_.operation().name()).toVector
      )
      assertEquals("PUBLISHED", transport.receipt.state().name())
      assertEquals(0L, transport.receipt.nativeVersion().getAsLong)
      assertTrue(Files.exists(evidence.log0))
      assertEquals(0, loads, "Issued CREATE must not require a final SELECT/loadTable")
      assertEquals(0, exists, "Provider reserve is authoritative for name occupancy")
      assertEquals(0, creates, "Issued CREATE uses the real stage without convenience loading")

    }

  @Test
  def baseSchemaCreateRecordsInstalledNativeBoundary(): Unit =
    withCase("B-base-installed", installed = true) { (spark, transport, evidence) =>
      baseStage(spark, transport, evidence, installed = true)
    }

  @Test
  def nativeOnlySchemaCreateDifferential(): Unit = withCase("C-native-only", installed = false) {
    (spark, transport, evidence) =>
      evidence.record(
        "differential-reason",
        "Installed B first observes the active transaction after log0; compare public-stage precommit callback reachability without the companion in a separate session"
      )
      baseStage(spark, transport, evidence, installed = false)
  }

  private def baseStage(
      spark: SparkSession,
      transport: Transport,
      evidence: Trace,
      installed: Boolean
  ): Unit = {
    val properties =
      new LinkedHashMap[String, String](fixtureMap("tableProperties").asJava)
    val beforeProperties = properties.asScala.toMap
    val beforeReads = transport.reads
    val beforeCommands = transport.commands.toVector
    val beforeFiles = evidence.files()
    val adapter = new GravitinoDeltaCatalogSpark35(() => transport.metadata, () => transport.actor)
    val journal = evidence.directory.resolve("base-journal")
    adapter.initialize(
      "a",
      new CaseInsensitiveStringMap(Collections.singletonMap("journal-directory", journal.toString))
    )
    try {
      val descriptor = adapter.stageCreate(identifier, schema, Array.empty[Transform], properties)
      evidence.origin(descriptor.getClass)
      evidence.record(
        "descriptor",
        s"id=${evidence.id(descriptor)} properties=$properties schema=${schema.json} transportReads=${transport.reads} commands=${transport.commands.size} nativeFiles=${evidence.files().size}"
      )
      assertEquals(beforeReads, transport.reads)
      assertEquals(beforeCommands, transport.commands.toVector)
      assertEquals(beforeFiles, evidence.files())
      assertTrue(beforeFiles.isEmpty)
      assertEquals(schema, descriptor.schema())
      assertEquals(beforeProperties, descriptor.properties().asScala.toMap)
      var invocations = 0
      val failure = rejected(evidence, "base-commit") {
        invocations += 1
        evidence.observe(spark, "before-commit", None)
        descriptor.commitStagedChanges()
      }
      evidence.observe(spark, "after-commit", None)
      evidence.inspectFiles()
      evidence.record(
        "stage-outcome",
        s"commitInvocations=$invocations commands=${transport.commands.map(_.operation())} observedTransactions=${evidence.observations.size} nativeLog0=${Files.exists(evidence.log0)} published=false"
      )
      assertEquals(1, invocations)
      assertEquals(beforeProperties, properties.asScala.toMap)
      assertEquals(1, transport.commands.count(_.operation().name() == "RESERVE"))
      val reserve = transport.commands.head
      assertEquals(reserve.token().get(), transport.receipt.identity().token())
      assertEquals(reserve.runId().get(), transport.receipt.runId())
      assertEquals(0L, transport.receipt.identity().generation())
      assertEquals(reserve.exactSchemaJson().get(), transport.receipt.marker().get().schemaJson())
      assertEquals("RESERVED", transport.receipt.state().name())
      val entries = Files.list(journal)
      val phases =
        try
          entries
            .iterator()
            .asScala
            .filter(_.getFileName.toString.endsWith(".json"))
            .map(DeltaRegistrationJournal.read)
            .map(_.phase().name())
            .toVector
        finally entries.close()
      evidence.record("journal-final-phases", phases.mkString(","))
      assertFalse(phases.contains("PUBLISHED"))
      assertTrue(
        Files.exists(evidence.log0),
        "Thrown native call must still retain durable CREATE0"
      )
      val actions = Files.readAllLines(evidence.log0).asScala.map(line => mapper.readTree(line))
      assertEquals(
        Set("commitInfo", "metaData", "protocol"),
        actions.flatMap(_.fieldNames().asScala).toSet
      )
      val commit = actions.find(_.has("commitInfo")).get.get("commitInfo")
      val metadata = actions.find(_.has("metaData")).get.get("metaData")
      assertEquals("CREATE TABLE", commit.get("operation").asText())
      assertFalse(commit.has("userMetadata"))
      assertEquals(
        mapper.readTree(schema.json),
        mapper.readTree(metadata.get("schemaString").asText())
      )
      assertFalse(metadata.get("id").asText().isEmpty)
      assertEquals(0, metadata.get("partitionColumns").size())
      val configuration = metadata
        .get("configuration")
        .fields()
        .asScala
        .map(e => e.getKey -> e.getValue.asText())
        .toMap
      assertEquals(
        beforeProperties ++ Map(
          DeltaOperationMarker.PROPERTY_KEY -> transport.receipt.marker().get().encode(),
          "delta.columnMapping.mode" -> "none"
        ),
        configuration
      )
      assertFalse(evidence.files().keys.exists(_.endsWith("00000000000000000001.json")))
      assertFalse(
        evidence.files().keys.exists(_.endsWith(".parquet")),
        "Schema CREATE must not write data"
      )
      if (installed) {
        assertBoundary(failure, "Unqualified Delta Ranger route: LogicalRDD")
        assertEquals(Vector((true, -1L, false)), evidence.observations.toVector)
        assertTrue(
          evidence.events.exists(
            _.contains(
              "snapshotClass=org.apache.spark.sql.delta.DummySnapshot snapshotVersion=-1 readVersion=-1"
            )
          )
        )
        assertEquals(Vector("NATIVE_STARTED"), phases)
        assertEquals(0, transport.commands.count(_.operation().name() == "PUBLISH"))
        assertTrue(evidence.plans.exists(_.nodeName == "LogicalRDD"))
        evidence.record(
          "installed-reachability",
          "precommitObserver=NOT_OBSERVED nativeLog0=COMMITTED nativeCommitReturn=THREW checksumCompletion=NOT_REACHED PUBLISH=NOT_REACHED; no replay"
        )
      } else {
        assertEquals(
          "DELTA_PUBLICATION_STATUS_NEEDED: native data and receipt retained",
          failure.getMessage
        )
        assertEquals(1, transport.commands.count(_.operation().name() == "PUBLISH"))
        assertTrue(Files.exists(evidence.log0))
        assertEquals(Set("NATIVE_COMMITTED", "STATUS_NEEDED"), phases.toSet)
        assertTrue(evidence.observations.nonEmpty)
        assertTrue(
          evidence.observations.forall(_._1),
          "Native-only callback unexpectedly preceded log0"
        )
        evidence.record(
          "native-only-reachability",
          "precommitObserver=NOT_OBSERVED nativeLog0=COMMITTED nativeCommitReturn=RETURNED PUBLISH=DELIBERATE_SENTINEL_STOP published=false; installed deeper phases remain NOT_REACHED"
        )
      }
    } finally adapter.close()
  }

  private[ranger] def withCase(
      name: String,
      installed: Boolean,
      observed: Boolean = true,
      policySetup: TestDeltaRangerNativeSelect.Fixture => Unit = _ => ()
  )(
      body: (SparkSession, Transport, Trace) => Unit
  ): Unit = {
    val evidence = fresh(name)
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policySetup(policy)
    val transport = new Transport(evidence, schema)
    val metadataConfig = evidence.directory.resolve("metadata.properties")
    write(
      metadataConfig,
      "auth-type=simple\nprincipal=separate-metadata-reader\nlifetime-seconds=300\n"
    )
    Files.setPosixFilePermissions(metadataConfig, PosixFilePermissions.fromString("rw-------"))
    loads = 0; exists = 0; creates = 0
    trace = evidence
    installer = extensions => {
      if (observed) observer(evidence)(extensions)
      if (installed) new DeltaRangerSparkExtension((_, _) => wrapper)(extensions)
    }
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName("installed-schema-create-" + name)
      .config("spark.gravitino.enableDeltaSupport", "true")
      .config("spark.gravitino.enableDeltaRangerSupport", "true")
      .config("spark.sql.gravitino.uri", "http://127.0.0.1:1")
      .config("spark.sql.gravitino.metalake", "lake")
      .config("spark.gravitino.deltaRanger.metadata.config", metadataConfig.toString)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.warehouse.dir", evidence.directory.resolve("warehouse").toString)
      .config(
        "spark.sql.extensions",
        classOf[InstalledSchemaCreateExtension].getName + ",io.delta.sql.DeltaSparkSessionExtension"
      )
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.catalog.update.enabled", "false")
      .config("spark.sql.catalog.a", classOf[InstalledSchemaCreateForwardingCatalog].getName)
    fixtureMap("settings").foreach { case (key, value) => builder.config(key, value) }
    DeltaLog.clearCache()
    val spark = builder.getOrCreate()
    var bound: Wrapper = null
    try {
      spark.sparkContext.setLogLevel("WARN")
      assertTrue(Set("3.5.4", "3.5.6")(spark.version))
      if (installed) {
        spark.sessionState.conf.setConfString(
          "spark.sql.extensions",
          DeltaRangerCatalogSpark35.EXTENSION + "," + spark.conf.get("spark.sql.extensions")
        )
        bound = new Wrapper(transport, policy, evidence)
        wrapper = bound.catalog
      }
      evidence.activate(spark)
      // Initialize the ordinary session catalog before taking the settings baseline.
      val beforeCatalog = spark.sessionState.conf.getAllConfs
      evidence.origin(spark.sessionState.catalogManager.catalog("spark_catalog").getClass)
      evidence.record(
        "session-catalog-initialization-settings",
        (spark.sessionState.conf.getAllConfs.toSet -- beforeCatalog.toSet).toSeq.sorted
          .mkString("\n")
      )
      val before = spark.sessionState.conf.getAllConfs
      evidence.record("settings-before", before.toSeq.sorted.mkString("\n"))
      evidence.record("lastCommit-before", spark.conf.getOption(lastCommit).toString)
      evidence.observe(spark, "before-case", None)
      try body(spark, transport, evidence)
      finally {
        evidence.observe(spark, "case-unwound", None)
        evidence.inspectFiles()
        val after = spark.sessionState.conf.getAllConfs
        evidence.record("settings-after", after.toSeq.sorted.mkString("\n"))
        evidence.record("lastCommit-after", spark.conf.getOption(lastCommit).toString)
        evidence.record(
          "forwarding-counters",
          s"loadTable=$loads tableExists=${if (exists == 0) "NOT_REACHED" else exists.toString} createTable=${if (creates == 0) "NOT_REACHED" else creates.toString}"
        )
        assertEquals(before - lastCommit, after - lastCommit)
        fixtureMap("settings").foreach { case (key, value) =>
          assertEquals(value, spark.conf.get(key))
        }
        assertNull(DeltaRangerNativeOperation.current())
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
        assertTrue(OptimisticTransaction.getActive.isEmpty)
        assertTrue(evidence.errors.isEmpty, "Trace instrumentation failed independently")
      }
    } catch {
      case error: Throwable => evidence.failed("test", error); throw error
    } finally {
      if (bound != null) bound.close()
      policy.engine.close()
      spark.stop()
      SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
      DeltaLog.clearCache()
      wrapper = null; installer = null; trace = null
      evidence.close()
    }
  }
  private def rejected(evidence: Trace, phase: String)(body: => Any): Throwable = {
    val failure = assertThrows(classOf[Exception], () => { body; () })
    evidence.failed(phase, failure)
    failure
  }
  private def assertBoundary(error: Throwable, message: String): Unit = {
    val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
    assertTrue(
      causes.exists(e => e.isInstanceOf[SecurityException] && e.getMessage == message),
      causes.mkString("\n")
    )
  }
}
