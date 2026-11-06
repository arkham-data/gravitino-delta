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

import java.util.concurrent.ScheduledExecutorService
import java.nio.file.{Files, Paths}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation, V1ScanWrapper}
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCScan
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import org.apache.gravitino.{Catalog, NameIdentifier}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.credential.{Credential, JdbcCredential}
import org.apache.gravitino.rel.{Column, Table => MetadataTable, TableCatalog}
import org.apache.gravitino.rel.types.Types
import org.apache.gravitino.spark.connector.catalog.{BaseCatalog, GravitinoCatalogManager}
import org.apache.spark.sql.execution.datasources.v2.jdbc.{JDBCTable, JDBCTableCatalog}
import org.apache.spark.sql.types.{MetadataBuilder, StructType, StructField, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, CatalogPlugin}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import org.mockito.Mockito._

@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.registeredJdbc.enabled", matches = "true")
class TestDeltaRangerRegisteredJdbcNative {
  @Test
  def issuedSourceUsesNativeJdbcWithOriginalColumnDecisions(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertTrue(Set("3.5.4", "3.5.6")(spark.version))
          val batches = spark.sessionState.optimizer.batches
          val preCbo = batches.indexWhere(_.name == "Pre CBO Rules")
          val pushdown = batches.indexWhere(
            _.rules.exists(_.getClass.getSimpleName == "V2ScanRelationPushDown$")
          )
          assertTrue(
            preCbo >= 0 && pushdown > preCbo,
            "Stock JDBC pushdown boundary: " + batches
              .map(batch =>
                batch.name + "=" + batch.rules.map(_.getClass.getSimpleName).mkString("/")
              )
              .mkString(";")
          )
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
          try {
            policy.denied.add("pg:secret")
            val projected = source.frame.select("id")
            assertEquals(Seq(1L, 3L), projected.collect().map(_.getLong(0)).sorted.toSeq)
            val scans = projected.queryExecution.optimizedPlan.collect {
              case scan: DataSourceV2ScanRelation => scan
            }
            assertEquals(1, scans.size)
            assertEquals(classOf[V1ScanWrapper], scans.head.scan.getClass)
            assertEquals(
              classOf[JDBCScan],
              scans.head.scan.asInstanceOf[V1ScanWrapper].v1Scan.getClass
            )
            val physical = projected.queryExecution.executedPlan.collect {
              case scan: RowDataSourceScanExec => scan
            }
            assertEquals(1, physical.size)
            def jdbcRdds(rdd: RDD[_]): Vector[RDD[_]] =
              (if (
                 rdd.getClass.getName == "org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD"
               )
                 Vector(rdd)
               else Vector.empty) ++ rdd.dependencies.toVector.flatMap(d => jdbcRdds(d.rdd))
            assertEquals(1, physical.head.inputRDDs().toVector.flatMap(jdbcRdds).size)
            val retained = physical.head.inputRDDs().head
            assertEquals(Seq(1L, 3L), projected.collect().map(_.getLong(0)).sorted.toSeq)
            assertSame(retained, physical.head.inputRDDs().head)
            assertThrows(
              classOf[SecurityException],
              () => source.frame.filter("secret = 'one'").select("id").collect()
            )
            policy.denied.clear()
            val rows = source.directFrame
              .select("id", "secret")
              .collect()
              .map(row => row.getLong(0) -> row.getString(1))
              .toMap
            assertEquals(Map(1L -> "one-masked", 3L -> null), rows)
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally {
      policy.close()
      postgres.close()
    }
  }
  @Test
  def boundedCompositionAndPruningRetainOriginalDemands(): Unit = {
    for (adaptive <- Seq(false, true); skipping <- Seq(false, true)) {
      val postgres = new RegisteredJdbcPostgresFixture
      val policy = new TestDeltaRangerNativeSelect.Fixture
      policy.customCatalog = "pg"
      try
        NativeSelectFixture.withSpark(
          spark => {
            val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
            try {
              val times = source.frame
                .selectExpr(
                  "id",
                  "current_timestamp() first_time",
                  "current_timestamp() second_time"
                )
                .collect()
              assertEquals(2, times.length)
              assertEquals(1, times.map(_.getTimestamp(1)).distinct.length)
              assertTrue(times.forall(row => row.getTimestamp(1) == row.getTimestamp(2)))
              val joined = source.frame
                .as("p")
                .join(
                  spark.table("b.db.t").as("d"),
                  org.apache.spark.sql.functions.col("p.id") === org.apache.spark.sql.functions
                    .col("d.id")
                )
                .selectExpr("p.id", "p.secret")
              assertEquals(
                Map(1L -> "one-masked", 3L -> null),
                joined.collect().map(row => row.getLong(0) -> row.getString(1)).toMap
              )
              assertEquals(0, source.frame.filter("false").select("id").collect().length)
              policy.denied.add("pg:secret")
              assertThrows(
                classOf[SecurityException],
                () => source.frame.filter("false AND secret = 'one'").select("id").collect()
              )
              policy.denied.clear()
              val retained = source.frame.select("id")
              policy.denied.add("pg:id")
              assertThrows(classOf[SecurityException], () => retained.collect())
              policy.denied.clear()
              val changed = source.frame.select("id")
              source.properties.put("jdbc-url", "jdbc:postgresql://127.0.0.1:1/extraction")
              assertThrows(classOf[SecurityException], () => changed.collect())
              source.properties.put("jdbc-url", postgres.url())
              assertThrows(classOf[SecurityException], () => source.frame.limit(1).collect())
              // Spark 3.5.6 changes aggregate-output Cast timezone reconstruction relative
              // to 3.5.4. Both pins reject this grammar before that stock callback path.
              val beforeAggregate = postgres.payloadReads()
              assertThrows(
                classOf[SecurityException],
                () => source.frame.groupBy("id").count().collect()
              )
              assertEquals(beforeAggregate, postgres.payloadReads())
              assertEquals(adaptive.toString, spark.conf.get("spark.sql.adaptive.enabled"))
              assertEquals(
                skipping.toString,
                spark.conf.get("spark.databricks.delta.stats.skipping")
              )
            } finally source.close()
          },
          Some(NativeSelectFixture.extension(policy)),
          Map(
            "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
            "spark.sql.adaptive.enabled" -> adaptive.toString,
            "spark.databricks.delta.stats.skipping" -> skipping.toString
          )
        )
      finally { policy.close(); postgres.close() }
    }
  }

  @Test
  def sourceAndTargetDenialsPrecedeJdbcPayloadAndNativeMutation(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      NativeSelectFixture.withSpark(
        spark => {
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
          try {
            val target = Paths.get(NativeSelectFixture.locations("a"))
            def objects(): Set[String] = {
              val files = Files.walk(target)
              try files.iterator().asScala.filter(Files.isRegularFile(_)).map(_.toString).toSet
              finally files.close()
            }
            val before = objects()
            for (denial <- Seq("source", "update", "alter", "late-source", "late-config")) {
              policy.denied.clear()
              policy.onRequest = request => {
                if (request.getResource.getValue("catalog") == "a") {
                  if (denial == "alter" && request.getAccessType == "alter")
                    policy.denied.add("a:*")
                  if (request.getAccessType == "update") {
                    if (denial == "late-source") policy.denied.add("pg:secret")
                    if (denial == "late-config") source.properties.put("jdbc-database", "changed")
                  }
                }
              }
              val frame =
                if (denial == "alter")
                  source.frame.selectExpr("id", "secret", "region", "secret AS added")
                else source.frame.select("id", "secret", "region")
              if (denial == "source") policy.denied.add("pg:secret")
              if (denial == "update") policy.denied.add("a:*")
              val payload = postgres.payloadReads()
              val failure = assertThrows(
                classOf[Exception],
                () => frame.write.mode("append").saveAsTable("a.db.t")
              )
              val causes =
                Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
              assertTrue(
                causes.exists(error =>
                  Option(error.getMessage).exists(
                    _.contains(
                      if (denial == "late-config") "Registered JDBC"
                      else "Delta Ranger access denied"
                    )
                  )
                ),
                denial
              )
              if (denial == "alter")
                assertTrue(policy.requests.asScala.exists(_.getAccessType == "alter"))
              assertEquals(payload, postgres.payloadReads(), denial)
              assertEquals(before, objects(), denial)
              policy.denied.clear()
              policy.onRequest = _ => ()
              source.properties.put("jdbc-database", "extraction")
            }
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          "spark.databricks.delta.schema.autoMerge.enabled" -> "true"
        ),
        clearNativeCacheBeforeExtension = false
      )
    finally { policy.close(); postgres.close() }
  }

  @Test
  def copiedForeignAndChangedScansHaveNoExecutionAuthority(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      NativeSelectFixture.withSpark(
        spark => {
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
          try {
            val frame = source.frame.select("id")
            val scan = frame.queryExecution.optimizedPlan.collect {
              case value: DataSourceV2ScanRelation => value
            }.head
            val wrapper = scan.scan.asInstanceOf[V1ScanWrapper]
            val jdbc = wrapper.v1Scan.asInstanceOf[JDBCScan]
            val changed = scan.copy(scan = wrapper.copy(v1Scan = jdbc.copy(pushedLimit = 1)))
            changed.copyTagsFrom(scan)
            val payload = postgres.payloadReads()
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerJdbcScans.plan(changed, spark)
            )
            val relation = source.frame.queryExecution.analyzed.collect {
              case value: DataSourceV2Relation => value
            }.head
            val overrideRelation = relation
              .copy(options = new CaseInsensitiveStringMap(Map("query" -> "select 1").asJava))
            overrideRelation.copyTagsFrom(relation)
            assertThrows(
              classOf[SecurityException],
              () => RegisteredJdbcPostgresFixture.frame(spark, overrideRelation).collect()
            )
            val foreign = spark.newSession()
            try {
              SparkSession.setActiveSession(foreign)
              assertThrows(
                classOf[SecurityException],
                () => RegisteredJdbcPostgresFixture.frame(foreign, relation.clone()).collect()
              )
            } finally SparkSession.setActiveSession(spark)
            assertThrows(
              classOf[SecurityException],
              () =>
                spark.read
                  .format("jdbc")
                  .option("url", postgres.url())
                  .option("driver", "org.postgresql.Driver")
                  .option("dbtable", "public.source")
                  .option("user", "extractor")
                  .option("password", postgres.password())
                  .load()
                  .collect()
            )
            assertEquals(payload, postgres.payloadReads())
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map("spark.gravitino.enableDeltaRangerJdbcSupport" -> "true")
      )
    finally { policy.close(); postgres.close() }
  }

  @Test
  def jdbcAppendPreservesOwnedTargetCacheMaintenance(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      RawNativeObservationFixture.withEnabled(storage =
        Map(
          "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      ) { (spark, native, target) =>
        val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
        try {
          val cached = RawNativeObservationFixture.active(native) {
            val frame = native.read.format("delta").load(NativeSelectFixture.locations("a")).cache()
            assertEquals(3, frame.collect().length)
            frame
          }
          val oldEntry = spark.sharedState.cacheManager.lookupCachedData(cached).get
          var retained: DeltaRangerCdfCacheMaintenance.Scope = null
          var participant: DeltaRangerInputSources.Source = null
          var checking = false
          val activeField = DeltaRangerCdfCacheMaintenance.getClass.getDeclaredFields
            .find(_.getName.endsWith("active"))
            .get
          activeField.setAccessible(true)
          val active = activeField
            .get(DeltaRangerCdfCacheMaintenance)
            .asInstanceOf[ThreadLocal[DeltaRangerCdfCacheMaintenance.Scope]]
          val registration = NativeSelectFixture.bindingFixture(spark, "a")
          registration.onRead = _ => {
            val scope = active.get()
            if (scope != null && !checking && retained == null) {
              checking = true
              try
                RawNativeObservationFixture.active(spark) {
                  val sourcesField =
                    scope.getClass.getDeclaredFields.find(_.getName.endsWith("sources")).get
                  sourcesField.setAccessible(true)
                  participant = sourcesField
                    .get(scope)
                    .asInstanceOf[Vector[DeltaRangerInputSources.Source]]
                    .head
                  scope.requireParticipant(participant)
                  val setting = "spark.sql.shuffle.partitions"
                  val before = spark.conf.get(setting)
                  spark.conf.set(setting, "7")
                  try
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.requireParticipant(participant)
                    )
                  finally spark.conf.set(setting, before)
                  val version = "spark.databricks.delta.lastCommitVersionInSession"
                  val old = spark.conf.getOption(version)
                  spark.conf.set(version, "999")
                  try
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.requireParticipant(participant)
                    )
                  finally
                    old match {
                      case Some(value) => spark.conf.set(version, value);
                      case None        => spark.conf.unset(version)
                    }
                  RawNativeObservationFixture.active(native) {
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.requireParticipant(participant)
                    )
                  }
                  val jdbc = participant.asInstanceOf[DeltaRangerInputSources.RegisteredJdbc]
                  val copied =
                    new DeltaRangerInputSources.RegisteredJdbc(jdbc.relation, jdbc.binding)
                  assertThrows(classOf[SecurityException], () => copied.recheckMaintenance(scope))
                  retained = scope
                }
              finally checking = false
            }
          }
          val payload = postgres.payloadReads()
          try
            source.frame.select("id", "secret", "region").write.mode("append").insertInto("a.db.t")
          finally { registration.onRead = _ => (); nativeEvidence(spark, "cache") }
          assertNotNull(retained)
          assertThrows(classOf[SecurityException], () => participant.recheckMaintenance(retained))
          val freshEntry = spark.sharedState.cacheManager.lookupCachedData(cached).get
          assertSame(oldEntry.plan, freshEntry.plan)
          assertNotSame(
            oldEntry.cachedRepresentation.cacheBuilder,
            freshEntry.cachedRepresentation.cacheBuilder
          )
          assertFalse(oldEntry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          assertEquals(payload + 1, postgres.payloadReads())
          RawNativeObservationFixture.active(native) {
            assertEquals(
              5,
              native.read.format("delta").load(NativeSelectFixture.locations("a")).collect().length
            )
            cached.unpersist()
          }
          val log = Paths.get(NativeSelectFixture.locations("a")).resolve("_delta_log")
          assertTrue(Files.exists(log.resolve("00000000000000000001.json")))
          assertFalse(Files.exists(log.resolve("00000000000000000002.json")))
        } finally source.close()
      }
    finally { policy.close(); postgres.close() }
  }

  @Test
  def maskedJoinKeysAndEmptyAppendPreserveOriginalObligations(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val delta = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    policy.maskColumn = "id"
    policy.mask = "CAST(2 AS BIGINT)"
    delta.customCatalog = "unused"
    val previous = NativeSelectFixture.catalogPolicies
    NativeSelectFixture.catalogPolicies = Map("b" -> delta)
    try
      NativeSelectFixture.withSpark(
        spark => {
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
          try {
            def joined = source.frame
              .as("p")
              .join(
                spark.table("b.db.t").as("d"),
                org.apache.spark.sql.functions.col("p.id") === org.apache.spark.sql.functions
                  .col("d.id")
              )
            val projected = joined.selectExpr("p.id", "d.id", "d.region")
            val rows = projected.collect()
            assertEquals(2, rows.length)
            assertTrue(
              rows.forall(row =>
                row.getLong(0) == 2L && row.getLong(1) == 2L && row.getString(2) == "drop"
              )
            )
            val optimized = projected.queryExecution.optimizedPlan
            val prepared = DeltaRangerNativeScans.prepared(optimized)
            val join = optimized.collect {
              case value: org.apache.spark.sql.catalyst.plans.logical.Join => value
            }.head
            val detached = join.left.clone()
            detached.copyTagsFrom(optimized)
            assertThrows(
              classOf[SecurityException],
              () => prepared.verifyExecutable(detached, spark)
            )
            val copiedLeaf = join.copy(right = join.left.clone())
            copiedLeaf.copyTagsFrom(optimized)
            assertThrows(
              classOf[SecurityException],
              () => prepared.verifyExecutable(copiedLeaf, spark)
            )
            policy.denied.add("pg:id")
            assertThrows(classOf[SecurityException], () => joined.selectExpr("p.secret").collect())
            policy.denied.clear()
            policy.requests.clear()
            val input = source.frame.filter("false").select("id", "secret", "region")
            val payload = postgres.payloadReads()
            try input.write.mode("append").insertInto("a.db.t")
            finally nativeEvidence(spark, "empty")
            assertEquals(payload, postgres.payloadReads())
            val log = Paths.get(NativeSelectFixture.locations("a")).resolve("_delta_log")
            assertFalse(Files.exists(log.resolve("00000000000000000001.json")))
            assertEquals(
              Set("id", "secret", "region"),
              policy.requests.asScala
                .filter(request =>
                  request.getAccessType == "select" && request.getResource
                    .getValue("catalog") == "pg"
                )
                .flatMap(request => Option(request.getResource.getValue("column")).map(_.toString))
                .toSet
            )
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally {
      NativeSelectFixture.catalogPolicies = previous; policy.close(); delta.close();
      postgres.close()
    }
  }

  @Test
  def sameShapedRemoteViewIsRejectedBeforePayloadOrAppend(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      NativeSelectFixture.withSpark(
        spark => {
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy, "source_view")
          try {
            val physical = mock(classOf[MetadataTable])
            when(physical.name()).thenReturn("source_named")
            val physicalColumns = source.table.columns()
            when(physical.columns()).thenReturn(physicalColumns)
            when(physical.properties()).thenReturn(Map.empty[String, String].asJava)
            when(source.tables.loadTable(NameIdentifier.of("public", "source_named")))
              .thenReturn(physical)
            assertEquals(
              Seq(1L, 3L),
              spark
                .table("pg.public.source_named")
                .select("id")
                .collect()
                .map(_.getLong(0))
                .sorted
                .toSeq
            )
            val payload = postgres.payloadReads()
            assertThrows(classOf[SecurityException], () => source.frame)
            assertThrows(
              classOf[SecurityException],
              () => source.frame.write.mode("append").insertInto("a.db.t")
            )
            assertEquals(payload, postgres.payloadReads())
            assertFalse(
              Files.exists(
                Paths
                  .get(NativeSelectFixture.locations("a"))
                  .resolve("_delta_log/00000000000000000001.json")
              )
            )
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map("spark.gravitino.enableDeltaRangerJdbcSupport" -> "true")
      )
    finally { policy.close(); postgres.close() }
  }

  @Test
  def registeredJdbcAppendUsesBothNativeWriters(): Unit = {
    Seq("insertInto", "saveAsTable").foreach { writer =>
      val postgres = new RegisteredJdbcPostgresFixture
      val policy = new TestDeltaRangerNativeSelect.Fixture
      policy.customCatalog = "pg"
      try
        NativeSelectFixture.withSpark(
          spark => {
            val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy)
            try {
              val target = Paths.get(NativeSelectFixture.locations("a"))
              val log = target.resolve("_delta_log")
              assertFalse(Files.exists(log.resolve("00000000000000000001.json")))
              val started = System.currentTimeMillis() * 1000L
              val input =
                if (writer == "insertInto") source.frame
                else
                  source.frame
                    .as("p")
                    .join(
                      spark.table("b.db.t").as("d"),
                      org.apache.spark.sql.functions.col("p.id") === org.apache.spark.sql.functions
                        .col("d.id")
                    )
                    .selectExpr("p.id", "p.secret", "p.region")
              val frame = input.selectExpr(
                "id",
                "secret",
                "region",
                "current_timestamp() first_time",
                "current_timestamp() second_time"
              )
              val payload = postgres.payloadReads()
              try {
                if (writer == "insertInto") frame.write.mode("append").insertInto("a.db.t")
                else frame.write.format("delta").mode("append").saveAsTable("a.db.t")
              } finally nativeEvidence(spark, writer)
              val ended = System.currentTimeMillis() * 1000L
              assertEquals(payload + 1, postgres.payloadReads())
              assertFalse(Files.exists(log.resolve("00000000000000000002.json")))
              val mapper = new ObjectMapper
              val actions = Files
                .readAllLines(log.resolve("00000000000000000001.json"))
                .asScala
                .map(mapper.readTree(_))
                .toVector
              assertEquals(1, actions.count(_.has("commitInfo")))
              assertEquals(
                "WRITE",
                actions.find(_.has("commitInfo")).get.get("commitInfo").get("operation").asText()
              )
              assertFalse(actions.exists(_.has("remove")))
              val timestamps = scala.collection.mutable.ArrayBuffer.empty[Long]
              val rows = actions.filter(_.has("add")).flatMap { action =>
                val reader = ParquetReader
                  .builder[Group](
                    new GroupReadSupport,
                    new HadoopPath(target.resolve(action.get("add").get("path").asText()).toUri)
                  )
                  .withConf(spark.sparkContext.hadoopConfiguration)
                  .build()
                try
                  Iterator
                    .continually(reader.read())
                    .takeWhile(_ != null)
                    .map { group =>
                      val timestamp = group.getLong("first_time", 0)
                      assertEquals(timestamp, group.getLong("second_time", 0))
                      assertTrue(timestamp >= started && timestamp <= ended)
                      timestamps += timestamp
                      (
                        group.getLong("id", 0),
                        if (group.getFieldRepetitionCount("secret") == 0) null
                        else group.getString("secret", 0),
                        group.getString("region", 0)
                      )
                    }
                    .toVector
                finally reader.close()
              }
              assertEquals(
                Vector((1L, "one-masked", "keep"), (3L, null, "keep")),
                rows.sortBy(_._1)
              )
              assertEquals(1, timestamps.distinct.size)
            } finally source.close()
          },
          Some(NativeSelectFixture.extension(policy)),
          Map(
            "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
            "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
            "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
            "spark.sql.adaptive.enabled" -> (writer == "saveAsTable").toString,
            "spark.databricks.delta.stats.skipping" -> (writer == "insertInto").toString
          ),
          initialData = (catalog, data) =>
            if (catalog == "a")
              data.selectExpr(
                "id",
                "secret",
                "region",
                "CAST(NULL AS TIMESTAMP) first_time",
                "CAST(NULL AS TIMESTAMP) second_time"
              )
            else data
        )
      finally { policy.close(); postgres.close() }
    }
  }

  @Test
  def ordinarySourceTypesSupportUnusedProjectionSelectedReadAndNativeAppend(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.customCatalog = "pg"
    try
      NativeSelectFixture.withSpark(
        spark => {
          val source = new RegisteredJdbcCatalogFixture(spark, postgres, policy, "source_typed")
          try {
            policy.denied.add("pg:secret")
            assertEquals(
              Seq(1L, 3L),
              source.frame.select("id").collect().map(_.getLong(0)).sorted.toSeq
            )
            policy.denied.clear()
            val selected = source.frame.select("id", "secret", "region", "plain_time", "zoned_time")
            val rows = selected.collect().sortBy(_.getLong(0))
            assertEquals(Seq(1L, 3L), rows.map(_.getLong(0)).toSeq)
            assertEquals("one-masked", rows(0).getString(1))
            assertNull(rows(1).getString(1))
            // Stock JDBC getTimestamp interprets a zone-less value in the JVM default zone.
            val plain = Seq("2026-01-02 03:04:05", "2026-02-03 04:05:06")
              .map(value => java.sql.Timestamp.valueOf(value).toInstant)
            val zoned =
              Seq("2026-01-02T01:04:05Z", "2026-02-03T02:05:06Z").map(java.time.Instant.parse)
            assertEquals(plain, rows.map(_.getTimestamp(3).toInstant).toSeq)
            assertEquals(zoned, rows.map(_.getTimestamp(4).toInstant).toSeq)
            val payload = postgres.payloadReads()
            try
              selected
                .selectExpr(
                  "id",
                  "secret",
                  "region",
                  "plain_time AS first_time",
                  "zoned_time AS second_time"
                )
                .write
                .mode("append")
                .insertInto("a.db.t")
            finally nativeEvidence(spark, "source-typed")
            assertEquals(payload + 1, postgres.payloadReads())
            val target = Paths.get(NativeSelectFixture.locations("a"))
            val log = target.resolve("_delta_log")
            assertFalse(Files.exists(log.resolve("00000000000000000002.json")))
            val mapper = new ObjectMapper
            val actions = Files
              .readAllLines(log.resolve("00000000000000000001.json"))
              .asScala
              .map(mapper.readTree(_))
            assertFalse(actions.exists(_.has("remove")))
            val actual = actions
              .filter(_.has("add"))
              .flatMap { action =>
                val reader = ParquetReader
                  .builder[Group](
                    new GroupReadSupport,
                    new HadoopPath(target.resolve(action.get("add").get("path").asText()).toUri)
                  )
                  .withConf(spark.sparkContext.hadoopConfiguration)
                  .build()
                try
                  Iterator
                    .continually(reader.read())
                    .takeWhile(_ != null)
                    .map { group =>
                      (
                        group.getLong("id", 0),
                        group.getLong("first_time", 0),
                        group.getLong("second_time", 0)
                      )
                    }
                    .toVector
                finally reader.close()
              }
              .sortBy(_._1)
            assertEquals(
              Seq(
                (1L, plain(0).toEpochMilli * 1000, zoned(0).toEpochMilli * 1000),
                (3L, plain(1).toEpochMilli * 1000, zoned(1).toEpochMilli * 1000)
              ),
              actual.toSeq
            )
          } finally source.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerJdbcSupport" -> "true",
          "spark.sql.session.timeZone" -> "UTC",
          "spark.sql.parquet.outputTimestampType" -> "TIMESTAMP_MICROS",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        ),
        initialData = (catalog, data) =>
          if (catalog == "a")
            data.selectExpr(
              "id",
              "secret",
              "region",
              "CAST(NULL AS TIMESTAMP) first_time",
              "CAST(NULL AS TIMESTAMP) second_time"
            )
          else data
      )
    finally { policy.close(); postgres.close() }
  }

  private def nativeEvidence(spark: SparkSession, scenario: String): Unit = {
    val directory = Paths
      .get(System.getProperty("gravitino.ranger.delta.registeredJdbc.evidence"))
      .resolve(spark.version + "-" + scenario + "-" + java.util.UUID.randomUUID().toString)
    Files.createDirectories(directory)
    val target = Paths.get(NativeSelectFixture.locations("a"))
    val files = Files.walk(target)
    try {
      val entries = files.iterator().asScala.filter(Files.isRegularFile(_)).toVector
      Files.write(
        directory.resolve("native-files.txt"),
        entries.map(path => target.relativize(path).toString + " " + Files.size(path)).sorted.asJava
      )
      entries
        .filter(path =>
          path.getParent.getFileName.toString == "_delta_log" && path.toString.endsWith(".json")
        )
        .foreach(path => Files.copy(path, directory.resolve(path.getFileName)))
    } finally files.close()
  }

}

private[ranger] final class RegisteredJdbcCatalogFixture(
    spark: SparkSession,
    postgres: RegisteredJdbcPostgresFixture,
    policy: TestDeltaRangerNativeSelect.Fixture,
    tableName: String = "source"
) extends AutoCloseable {
  val id: Identifier = Identifier.of(Array("public"), tableName)
  val properties = new java.util.HashMap[String, String]()
  properties.put("jdbc-url", postgres.url())
  properties.put("jdbc-database", "extraction")
  properties.put("jdbc-driver", "org.postgresql.Driver")
  properties.put("authorization-provider", "ranger-delta")
  properties.put("authorization.ranger.delta.catalog", "pg")
  properties.put("authorization.ranger.delta.metalake", "lake")
  properties.put("authorization.ranger.service.name", policy.engine.versions().service())
  properties.put("authorization.ranger.service.type", policy.engine.versions().definition())
  val catalog = mock(classOf[Catalog], RETURNS_DEEP_STUBS)
  val client = mock(classOf[GravitinoClient])
  val tables = mock(classOf[TableCatalog])
  val table = mock(classOf[MetadataTable])
  when(catalog.name()).thenReturn("pg")
  when(catalog.`type`()).thenReturn(Catalog.Type.RELATIONAL)
  when(catalog.provider()).thenReturn("jdbc-postgresql")
  when(catalog.properties()).thenReturn(properties)
  when(catalog.supportsSecrets().getSecrets()).thenReturn(Map.empty[String, String].asJava)
  when(catalog.supportsCredentials().getCredentials())
    .thenReturn(Array[Credential](new JdbcCredential("extractor", postgres.password())))
  when(catalog.asTableCatalog()).thenReturn(tables)
  when(client.loadCatalog("pg")).thenReturn(catalog)
  when(tables.loadTable(NameIdentifier.of("public", tableName))).thenReturn(table)
  when(table.name()).thenReturn(tableName)
  when(table.properties()).thenReturn(Map.empty[String, String].asJava)
  when(table.columns()).thenReturn(
    Array[Column](
      Column.of("id", Types.LongType.get()),
      Column.of("secret", Types.StringType.get()),
      Column.of("region", Types.StringType.get())
    )
  )
  if (tableName == "source_typed")
    when(table.columns()).thenReturn(
      Array[Column](
        Column.of("id", Types.LongType.get()),
        Column.of("secret", Types.VarCharType.of(20)),
        Column.of("region", Types.StringType.get()),
        Column.of("plain_time", Types.TimestampType.withoutTimeZone()),
        Column.of("zoned_time", Types.TimestampType.withTimeZone())
      )
    )
  private val manager = GravitinoCatalogManager.create(
    spark.sparkContext.getConf,
    spark.sparkContext.sparkUser,
    _ => client
  )
  private val metadata = new DeltaRangerMetadataClient(
    () => System.nanoTime(),
    mock(classOf[ScheduledExecutorService]),
    _ => client
  )
  val wrapper = new DeltaRangerPostgreSqlCatalogSpark35(
    new DeltaRangerPostgreSqlCatalogSpark35.State(metadata, () => catalog, _ => policy.engine)
  )
  private val catalogsField =
    spark.sessionState.catalogManager.getClass.getDeclaredField("catalogs")
  catalogsField.setAccessible(true)
  private val registered = catalogsField
    .get(spark.sessionState.catalogManager)
    .asInstanceOf[scala.collection.mutable.Map[String, CatalogPlugin]]
  registered.put("pg", wrapper)
  try wrapper.initialize("pg", new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
  catch { case failure: Throwable => manager.close(); throw failure }

  def frame: DataFrame = spark.table("pg.public." + tableName)

  def directFrame: DataFrame = {
    wrapper.sourceBindings().read(id)
    val field = classOf[BaseCatalog].getDeclaredField("sparkCatalog")
    field.setAccessible(true)
    val native =
      field.get(wrapper).asInstanceOf[JDBCTableCatalog].loadTable(id).asInstanceOf[JDBCTable]
    assertEquals(
      Set("url", "driver", "user", "password", "dbtable"),
      native.jdbcOptions.parameters.originalMap.keySet
    )
    assertEquals("\"public\".\"source\"", native.jdbcOptions.tableOrQuery)
    assertEquals(
      StructType(
        Seq(
          StructField(
            "id",
            LongType,
            true,
            new MetadataBuilder().putBoolean("isSigned", true).putLong("scale", 0).build()
          ),
          StructField(
            "secret",
            StringType,
            true,
            new MetadataBuilder().putBoolean("isSigned", false).putLong("scale", 0).build()
          ),
          StructField(
            "region",
            StringType,
            true,
            new MetadataBuilder().putBoolean("isSigned", false).putLong("scale", 0).build()
          )
        )
      ).json,
      native.schema.json
    )
    val loaded = wrapper.loadTable(id)
    val before = spark.conf.getAll
    try
      RegisteredJdbcPostgresFixture.frame(
        spark,
        DataSourceV2Relation.create(loaded, Some(wrapper), Some(id))
      )
    catch {
      case failure: SecurityException =>
        val after = spark.conf.getAll
        val changedKeys = (before.keySet ++ after.keySet)
          .filter(key => before.get(key) != after.get(key))
          .toVector
          .sorted
        if (changedKeys.nonEmpty)
          throw new AssertionError(
            "Native analysis changed configuration keys: " + changedKeys.mkString(","),
            failure
          )
        throw failure
    }
  }

  override def close(): Unit = {
    registered.remove("pg")
    wrapper.close()
    manager.close()
  }
}
