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
import org.apache.spark.sql.delta.OptimisticTransaction
import java.util.concurrent.TimeUnit
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.types.{LongType, StringType, StructType, MetadataBuilder}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

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

  @ParameterizedTest
  @ValueSource(strings = Array("foreign-binding", "foreign-transaction"))
  def createCompletionRejectsForeignPublicationWhileOwnerActive(mode: String): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      new TestDeltaRangerInstalledSchemaCreateBoundary().withCase(
        "jdbc-create-completion-" + mode,
        installed = true,
        observed = false,
        declaredSchema = schema,
        settingsOverride = Some(settings)
      ) { (spark, transport, evidence) =>
        spark.conf.set(versionKey, "73")
        var onClock: () => Unit = () => ()
        val scope = DeltaRangerExternalJdbcSources.openWithClock(
          spark,
          1200,
          () => {
            onClock(); 0L
          }
        )
        val foreignScope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => 0L)
        try {
          val frame = source(spark, scope, postgres)
          val foreignFrame = source(spark, foreignScope, postgres)
          val input = DeltaRangerMetadataInput.capture(spark, frame)
          val foreignInput = DeltaRangerMetadataInput.capture(spark, foreignFrame)
          def issued(name: String): DeltaRangerCreateCommand.Carrier =
            spark.sessionState.analyzer
              .execute(
                spark.sessionState.sqlParser.parsePlan(
                  s"CREATE TABLE a.db.$name (id BIGINT, secret STRING, region STRING) USING delta " +
                    "TBLPROPERTIES ('delta.enableChangeDataFeed'='true', " +
                    "'delta.compatibility.symlinkFormatManifest.enabled'='false')"
                )
              )
              .asInstanceOf[DeltaRangerCreateCommand.Carrier]
          val carrier = issued("created")
          carrier.owner.bindInput(carrier, input)
          val foreignCarrier = issued("other")
          foreignCarrier.owner.bindInput(foreignCarrier, foreignInput)
          val physical = carrier.owner.plan(carrier, spark)
          val probe = new CompletionProbe[DeltaRangerCreateMutation](
            carrier.owner,
            classOf[DeltaRangerCreateMutation],
            scope,
            foreignScope,
            mode,
            mutation => mutation.requireExternalPublication(input),
            mutation => foreignInput.complete(mutation)
          )
          onClock = () => probe.check()
          transport.publishSuccess = true
          val payload = postgres.payloadReads()
          try {
            if (mode == "foreign-transaction") {
              val failure = assertThrows(classOf[Exception], () => physical.run())
              evidence.failed("expected-foreign-transaction", failure)
              assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
              assertTrue(failure.getMessage.contains("publication=PUBLISHED"), failure.toString)
              assertTrue(
                causes(failure).exists(_.isInstanceOf[SecurityException]),
                causes(failure).mkString("; ")
              )
            } else physical.run()
          } finally clearActiveTransaction()
          assertTrue(probe.exercised)
          probe.assertSnapshots()
          assertEquals(payload, postgres.payloadReads())
          assertEquals("PUBLISHED", transport.receipt.state().name())
          assertTrue(Files.exists(evidence.log0))
          assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
          assertEquals(
            Vector("RESERVE", "PUBLISH"),
            transport.commands.map(_.operation().name()).toVector
          )
          assertThrows(classOf[SecurityException], () => foreignFrame.count())
          if (mode == "foreign-transaction")
            assertThrows(classOf[SecurityException], () => frame.count())
          else assertEquals(3L, frame.count())
        } finally { clearActiveTransaction(); foreignScope.close(); scope.close() }
      }
    finally postgres.close()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("foreign-binding", "foreign-transaction"))
  def cdfCompletionRejectsForeignPublicationWhileOwnerActive(mode: String): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          Seq("a", "b").foreach { catalog =>
            spark.conf
              .set(s"spark.sql.catalog.$catalog", classOf[CdfAlterForwardingCatalog].getName)
            NativeSelectFixture.bindingFixture(spark, catalog)
          }
          spark.conf.set(versionKey, "73")
          var onClock: () => Unit = () => ()
          val scope = DeltaRangerExternalJdbcSources.openWithClock(
            spark,
            1200,
            () => {
              onClock(); 0L
            }
          )
          val foreignScope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => 0L)
          try {
            val frame = source(spark, scope, postgres)
            val foreignFrame = source(spark, foreignScope, postgres)
            val input = DeltaRangerMetadataInput.capture(spark, frame)
            val foreignInput = DeltaRangerMetadataInput.capture(spark, foreignFrame)
            def issued(catalog: String): DeltaRangerCdfPropertyCommand.Carrier =
              spark.sessionState.analyzer
                .execute(
                  spark.sessionState.sqlParser.parsePlan(
                    s"ALTER TABLE $catalog.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
                  )
                )
                .asInstanceOf[DeltaRangerCdfPropertyCommand.Carrier]
            val carrier = issued("a")
            carrier.owner.bindInput(carrier, input)
            val foreignCarrier = issued("b")
            foreignCarrier.owner.bindInput(foreignCarrier, foreignInput)
            val physical = carrier.owner.plan(carrier, spark)
            val probe = new CompletionProbe[DeltaRangerCdfPropertyMutation](
              carrier.owner,
              classOf[DeltaRangerCdfPropertyMutation],
              scope,
              foreignScope,
              mode,
              mutation => mutation.requireExternalPublication(input),
              mutation => foreignInput.complete(mutation)
            )
            onClock = () => probe.check()
            val payload = postgres.payloadReads()
            try {
              if (mode == "foreign-transaction") {
                val failure = assertThrows(classOf[Exception], () => physical.run())
                assertTrue(
                  causes(failure).exists(e =>
                    Option(e.getMessage).exists(
                      _.startsWith("CDF property change committed;")
                    )
                  )
                )
                assertTrue(
                  causes(failure).exists(_.isInstanceOf[SecurityException]),
                  causes(failure).mkString("; ")
                )
              } else physical.run()
            } finally clearActiveTransaction()
            assertTrue(probe.exercised)
            probe.assertSnapshots()
            assertEquals(payload, postgres.payloadReads())
            val target = Paths.get(NativeSelectFixture.locations("a"))
            assertTrue(Files.exists(target.resolve("_delta_log/00000000000000000001.json")))
            assertFalse(Files.exists(target.resolve("_delta_log/00000000000000000002.json")))
            assertFalse(
              Files.exists(
                Paths.get(
                  NativeSelectFixture.locations("b"),
                  "_delta_log",
                  "00000000000000000001.json"
                )
              )
            )
            assertThrows(classOf[SecurityException], () => foreignFrame.count())
            if (mode == "foreign-transaction")
              assertThrows(classOf[SecurityException], () => frame.count())
            else assertEquals(3L, frame.count())
          } finally {
            clearActiveTransaction()
            val root = Paths.get(
              System.getenv("GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR"),
              "jdbc-cdf-completion-" + mode
            )
            Files.createDirectories(root)
            Seq("a", "b").foreach { catalog =>
              val target = Paths.get(NativeSelectFixture.locations(catalog))
              Files.write(root.resolve(catalog + "-target.txt"), target.toString.getBytes("UTF-8"))
              val logs = Files.list(target.resolve("_delta_log"))
              try
                logs
                  .iterator()
                  .asScala
                  .filter(_.toString.endsWith(".json"))
                  .foreach(path =>
                    Files.copy(path, root.resolve(catalog + "-" + path.getFileName.toString))
                  )
              finally logs.close()
            }
            foreignScope.close(); scope.close()
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

  // Only read real owner/mutation state; never fabricate native success or production callbacks.
  private class CompletionProbe[T <: AnyRef](
      owner: AnyRef,
      mutationClass: Class[T],
      scope: DeltaRangerExternalJdbcSources.Scope,
      foreignScope: DeltaRangerExternalJdbcSources.Scope,
      mode: String,
      validate: T => Unit,
      substitute: T => Unit
  ) {
    var exercised = false
    private val before = field(scope, "configuration")
    private val foreignBefore = field(foreignScope, "configuration")
    def check(): Unit = {
      val mutationField = owner.getClass.getDeclaredFields.find(_.getType == mutationClass).get
      mutationField.setAccessible(true)
      val mutation = mutationClass.cast(mutationField.get(owner))
      if (!exercised && mutation != null && field(mutation, "closed") == java.lang.Boolean.TRUE) {
        exercised = true // The successful proof and nested completion must not re-enter this hook.
        validate(mutation) // Proves the real owner is active and native publication is eligible.
        if (mode == "foreign-binding") {
          assertThrows(classOf[SecurityException], () => substitute(mutation))
          assertEquals(foreignBefore, field(foreignScope, "configuration"))
          validate(mutation)
        } else {
          val transactionField = mutation.getClass.getDeclaredFields
            .find(_.getType == classOf[OptimisticTransaction])
            .get
          transactionField.setAccessible(true)
          val transaction = transactionField.get(mutation).asInstanceOf[OptimisticTransaction]
          val foreign = new OptimisticTransaction(
            transaction.deltaLog,
            transaction.catalogTable,
            transaction.snapshot
          )
          assertNotSame(transaction, foreign)
          assertNotEquals(transaction.txnId, foreign.txnId)
          OptimisticTransaction.getClass
            .getMethod("setActive", classOf[OptimisticTransaction])
            .invoke(OptimisticTransaction, foreign)
        }
      }
    }
    def assertSnapshots(): Unit = {
      assertEquals(foreignBefore, field(foreignScope, "configuration"))
      if (mode == "foreign-transaction") assertEquals(before, field(scope, "configuration"))
      else assertNotEquals(before, field(scope, "configuration"))
    }
  }

  // Delta exposes these package-scoped Scala methods as public JVM methods.
  private def clearActiveTransaction(): Unit = {
    OptimisticTransaction.getClass.getMethod("clearActive").invoke(OptimisticTransaction)
    assertTrue(OptimisticTransaction.getActive.isEmpty)
  }

  private def field(value: AnyRef, name: String): AnyRef = {
    val member = value.getClass.getDeclaredField(name)
    member.setAccessible(true)
    member.get(value)
  }

  private def source(
      spark: SparkSession,
      scope: DeltaRangerExternalJdbcSources.Scope,
      postgres: RegisteredJdbcPostgresFixture
  ): DataFrame = {
    val bound = scope.load(options(postgres))
    RegisteredJdbcPostgresFixture.frame(spark, LogicalRelation(bound.relation))
  }

  private def causes(failure: Throwable): Vector[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toVector

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
