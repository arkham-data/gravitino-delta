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

import org.apache.spark.sql.DeltaRangerExternalJdbcNative.{Relation => JDBCRelation}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.sources.{EqualTo, In}
import org.apache.spark.sql.types.{DateType, TimestampType}
import org.apache.spark.sql.functions.col
import java.math.BigDecimal
import java.util.concurrent.{Executors, Callable, TimeUnit}
import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.hadoop.security.UserGroupInformation
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.{Path => HadoopPath}

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcRelationProvider, JDBCPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.externalJdbc.enabled", matches = "true")
class TestDeltaRangerExternalJdbcNative {
  @Test
  def issuedSubquerySupportsNativeReadCountAndBothAppends(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val scope = DeltaRangerExternalJdbcSources.open(spark, 1200)
          try {
            val options = ExternalJdbcTestFixture.options
            options.put("url", postgres.url()); options.put("password", postgres.password())
            options.put(
              "dbtable",
              "(SELECT id, secret, region FROM public.source) arkham_temp_table"
            )
            val bound = scope.load(options)
            val beforeConf = spark.conf.getAll
            val beforeEffective =
              spark.sessionState.conf.getAllDefinedConfs.map(v => v._1 -> v._2).toMap
            val frame =
              try RegisteredJdbcPostgresFixture.frame(spark, LogicalRelation(bound.relation))
              catch {
                case failure: SecurityException =>
                  val after = spark.conf.getAll
                  val afterEffective =
                    spark.sessionState.conf.getAllDefinedConfs.map(v => v._1 -> v._2).toMap
                  val changed = (beforeConf.keySet ++ after.keySet).filter(key =>
                    beforeConf.get(key) != after.get(key)
                  )
                  val defaults = (beforeEffective.keySet ++ afterEffective.keySet).filter(key =>
                    beforeEffective.get(key) != afterEffective.get(key)
                  )
                  throw new AssertionError(
                    "Changed explicit keys: " + changed.mkString(
                      ","
                    ) + "; effective keys: " + defaults.mkString(","),
                    failure
                  )
              }
            val selected = frame.filter("id > 1").select("id", "secret", "region")
            assertEquals(Seq(2L, 3L), selected.collect().map(_.getLong(0)).sorted.toSeq)
            val scans = DeltaRangerPhysicalProvenance
              .nodes(selected.queryExecution.executedPlan)
              .collect { case scan: RowDataSourceScanExec => scan }
            assertEquals(1, scans.size)
            assertSame(bound.relation, scans.head.relation)
            val scan = scans.head
            val frozenScan = DeltaRangerPhysicalProvenance.binding(scan)
            val clonedNative = bound.relation.copy(
              bound.relation.schema,
              bound.relation.parts.clone(),
              bound.relation.jdbcOptions
            )(spark)
            assertFalse(
              frozenScan == DeltaRangerPhysicalProvenance.binding(
                scan.copy(relation = clonedNative)
              ),
              "A reconstructed native relation must not borrow physical proof"
            )

            assertEquals(
              frozenScan,
              DeltaRangerPhysicalProvenance.binding(
                scan.copy(
                  filters = scan.filters.iterator.map(identity).toSet,
                  handledFilters = scan.handledFilters.iterator.map(identity).toSet
                )
              )
            )
            assertNotEquals(
              frozenScan,
              DeltaRangerPhysicalProvenance.binding(
                scan.copy(filters = Set(EqualTo("id", 99L)))
              )
            )
            assertNotEquals(
              frozenScan,
              DeltaRangerPhysicalProvenance.binding(scan.copy(rdd = scan.rdd.map(row => row)))
            )
            val literal = Array[Any](1L, 2L)
            val mutableFilter =
              scan.copy(filters = Set(In("id", literal)))
            val frozenLiteral = DeltaRangerPhysicalProvenance.binding(mutableFilter)
            literal(0) = 99L
            assertNotEquals(frozenLiteral, DeltaRangerPhysicalProvenance.binding(mutableFilter))

            def jdbc(rdd: RDD[_]): Vector[RDD[_]] =
              (if (
                 rdd.getClass.getName == "org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD"
               ) Vector(rdd)
               else Vector.empty) ++
                rdd.dependencies.toVector.flatMap(d => jdbc(d.rdd))
            assertEquals(1, scans.head.inputRDDs().toVector.flatMap(jdbc).size)
            assertEquals(3L, frame.count())
            val typedOptions = inputOptions(postgres)
            typedOptions.put("dbtable", "(SELECT * FROM public.source_typed) arkham_temp_table")
            val typed = scope.load(typedOptions)
            val typedRows = RegisteredJdbcPostgresFixture
              .frame(spark, LogicalRelation(typed.relation))
              .collect()
              .sortBy(_.getLong(0))
            assertEquals(Seq(1L, 2L, 3L), typedRows.map(_.getLong(0)).toSeq)
            assertEquals(
              Timestamp.valueOf("2026-01-02 03:04:05").toInstant,
              typedRows(0).getTimestamp(3).toInstant
            )
            assertEquals(
              Instant.parse("2026-01-02T01:04:05Z"),
              typedRows(0).getTimestamp(4).toInstant
            )
            assertTrue(typedRows(1).isNullAt(3) && typedRows(1).isNullAt(4))
            options.put(
              "dbtable",
              "(SELECT count(*) AS total FROM public.source) arkham_temp_table"
            )
            val counted = scope.load(options)
            assertEquals(
              3L,
              RegisteredJdbcPostgresFixture
                .frame(spark, LogicalRelation(counted.relation))
                .collect()
                .head
                .getLong(0)
            )
            Seq("insertInto", "saveAsTable").zipWithIndex.foreach { case (writer, index) =>
              val input = frame.selectExpr(
                "id",
                "secret",
                "region",
                "current_timestamp() first_time",
                "current_timestamp() second_time"
              )
              try {
                if (writer == "insertInto") input.write.mode("append").insertInto("a.db.t")
                else input.write.format("delta").mode("append").saveAsTable("a.db.t")
              } finally evidence(spark, writer)
              val committed = committedRows(spark, index + 1)
              assertEquals(Seq(1L, 2L, 3L), committed.map(_.getLong("id", 0)).sorted)
              assertEquals(
                Set("one", "two"),
                committed
                  .filter(_.getFieldRepetitionCount("secret") > 0)
                  .map(_.getString("secret", 0))
                  .toSet
              )
              committed.foreach(row =>
                assertEquals(row.getInt96("first_time", 0), row.getInt96("second_time", 0))
              )
              assertEquals(3L, frame.count())
              assertTrue(
                Files.exists(
                  Paths
                    .get(NativeSelectFixture.locations("a"))
                    .resolve(f"_delta_log/${index + 1}%020d.json")
                )
              )
            }
          } finally scope.close()
        },
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
          "spark.gravitino.enableDeltaSupport" -> "true",
          "spark.gravitino.enableDeltaRangerSupport" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          "spark.sql.adaptive.enabled" -> "false"
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
  @Test
  def unissuedCopiedForeignClosedAndChangedSourcesCannotExecute(): Unit = withFixture() {
    (spark, postgres, _) =>
      val api = GravitinoJdbcInput.open(spark, 1200)
      try {
        val options = inputOptions(postgres)
        val frame = api.read(options)
        val relation = frame.queryExecution.analyzed.collect { case value: LogicalRelation =>
          value
        }.head
        val native = relation.relation
          .asInstanceOf[JDBCRelation]
        val before = postgres.payloadReads()
        val otherScope = GravitinoJdbcInput.open(spark, 1200)
        try {
          val issuedOther = otherScope
            .read(options)
            .queryExecution
            .analyzed
            .collect { case value: LogicalRelation =>
              value
            }
            .head
          val other =
            LogicalRelation(issuedOther.relation, issuedOther.output, None, isStreaming = false)
          other.copyTagsFrom(relation)
          val locator = TreeNodeTag[Any]("gravitino.delta.ranger.ordinary-origin")
          assertTrue(other.getTagValue(locator).nonEmpty)
          assertSame(relation.getTagValue(locator).get, other.getTagValue(locator).get)
          assertThrows(
            classOf[SecurityException],
            () => RegisteredJdbcPostgresFixture.frame(spark, other).collect()
          )
        } finally otherScope.close()

        assertThrows(
          classOf[SecurityException],
          () => spark.read.format("jdbc").options(options).load().collect()
        )
        val copied = relation.copy(relation =
          native.copy(native.schema, native.parts.clone(), native.jdbcOptions)(spark)
        )
        copied.copyTagsFrom(relation)
        assertThrows(
          classOf[SecurityException],
          () => RegisteredJdbcPostgresFixture.frame(spark, copied).collect()
        )
        val foreign = spark.newSession()
        try {
          SparkSession.setActiveSession(foreign)
          assertThrows(
            classOf[SecurityException],
            () => RegisteredJdbcPostgresFixture.frame(foreign, relation.clone()).collect()
          )
        } finally SparkSession.setActiveSession(spark)
        val original = spark.conf.get("spark.sql.shuffle.partitions")
        spark.conf.set("spark.sql.shuffle.partitions", "7")
        try assertThrows(classOf[SecurityException], () => frame.count())
        finally spark.conf.set("spark.sql.shuffle.partitions", original)
        val version = "spark.databricks.delta.lastCommitVersionInSession"
        val old = spark.conf.getOption(version)
        spark.conf.set(version, "999")
        try assertThrows(classOf[SecurityException], () => frame.count())
        finally
          old match {
            case Some(value) => spark.conf.set(version, value);
            case None        => spark.conf.unset(version)
          }
        assertThrows(
          classOf[SecurityException],
          () =>
            spark
              .createDataFrame(spark.sparkContext.emptyRDD[Row], frame.schema)
              .collect()
        )
        assertThrows(classOf[SecurityException], () => frame.union(frame).count())
        assertEquals(before, postgres.payloadReads())
        val projected = frame.filter("id > 1").select("id")
        assertEquals(2, projected.collect().length)
        val detached = projected.queryExecution.normalized.collectLeaves().head.clone()
        assertThrows(
          classOf[SecurityException],
          () => RegisteredJdbcPostgresFixture.frame(spark, detached).collect()
        )
        val wrongParent =
          projected.queryExecution.normalized.clone().transformUp { case filter: Filter =>
            filter.copy(condition = Literal.FalseLiteral)
          }
        assertThrows(
          classOf[SecurityException],
          () => RegisteredJdbcPostgresFixture.frame(spark, wrongParent).collect()
        )
        assertEquals(3L, frame.count())
        api.close(); api.close()
        assertThrows(classOf[SecurityException], () => frame.count())
        assertThrows(
          classOf[SecurityException],
          () => frame.write.mode("append").insertInto("a.db.t")
        )
        assertNoCommit()
      } finally api.close()
  }

  @Test
  def explicitNativeBoundsPreserveNullAndOutsideRowsAndTypes(): Unit = withFixture() {
    (spark, postgres, _) =>
      val api = GravitinoJdbcInput.open(spark, 1200)
      try {
        val options = inputOptions(postgres)
        options.put(
          "dbtable",
          "(SELECT * FROM (VALUES (-1::bigint, DATE '2025-12-31', TIMESTAMP '2025-12-31 00:00:00', 1.25::decimal(8,2)), (1, DATE '2026-01-01', TIMESTAMP '2026-01-01 00:00:00', 2.50), (2, DATE '2026-01-02', TIMESTAMP '2026-01-02 00:00:00', 3.75), (4, DATE '2026-01-04', TIMESTAMP '2026-01-04 00:00:00', 4.00), (NULL, NULL, NULL, NULL)) AS rows(id, day, moment, amount)) arkham_temp_table"
        )
        options.put("fetchsize", "2")
        options.put("numPartitions", "2")
        Seq(
          ("id", "1", "3"),
          ("day", "2026-01-01", "2026-01-03"),
          ("moment", "2026-01-01 00:00:00", "2026-01-03 00:00:00")
        ).foreach { case (column, lower, upper) =>
          options.put("partitionColumn", column); options.put("lowerBound", lower);
          options.put("upperBound", upper)
          val frame = api.read(options)
          val relation = frame.queryExecution.analyzed
            .collect { case value: LogicalRelation => value }
            .head
            .relation
            .asInstanceOf[JDBCRelation]
          assertEquals(2, relation.jdbcOptions.fetchSize)
          assertEquals(2, relation.parts.length)
          val stock = new JdbcRelationProvider()
            .createRelation(spark.sqlContext, options.asScala.toMap)
            .asInstanceOf[JDBCRelation]
          assertEquals(
            stock.parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSeq,
            relation.parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSeq
          )
          val rows = frame.collect()
          assertEquals(5, rows.length)
          assertEquals(Set(-1L, 1L, 2L, 4L), rows.filterNot(_.isNullAt(0)).map(_.getLong(0)).toSet)
          assertEquals(1, rows.count(_.isNullAt(0)))
          assertEquals(
            new BigDecimal("1.250000000000000000"),
            rows.find(row => !row.isNullAt(0) && row.getLong(0) == -1L).get.getDecimal(3)
          )
          assertEquals(DateType, frame.schema("day").dataType)
          assertEquals(TimestampType, frame.schema("moment").dataType)
        }
      } finally api.close()
  }

  @Test
  def mixedNamedPoliciesRemainRequiredAfterPruningAndAtPrecommit(): Unit = withFixture() {
    (spark, postgres, policy) =>
      policy.customCatalog = "b"
      val api = GravitinoJdbcInput.open(spark, 1200)
      try {
        val frame = api.read(inputOptions(postgres))
        def joined = frame
          .as("p")
          .join(
            spark.table("b.db.t").as("d"),
            col("p.id") === col("d.id")
          )
        assertEquals(
          Seq(1L, 3L),
          joined.selectExpr("p.id").collect().map(_.getLong(0)).sorted.toSeq
        )
        assertEquals(
          Set("b-one-masked", null),
          joined.selectExpr("d.secret").collect().map(_.getString(0)).toSet
        )
        val retained = joined.selectExpr("p.id")
        assertEquals(2, retained.collect().length)
        policy.filter = "id = 1"
        try assertThrows(classOf[SecurityException], () => retained.collect())
        finally policy.filter = "region = 'keep'"
        policy.maskColumn = "id"
        policy.mask = "{col} + CAST(10 AS BIGINT)"
        try {
          val shifted = frame
            .selectExpr("id + 10 AS id", "secret", "region")
            .as("p")
            .join(spark.table("b.db.t").as("d"), col("p.id") === col("d.id"))
            .selectExpr("p.id")
          assertEquals(Seq(11L, 13L), shifted.collect().map(_.getLong(0)).sorted.toSeq)
        } finally { policy.maskColumn = "secret"; policy.mask = "concat({col}, '-masked')" }
        policy.denied.add("b:id")
        assertThrows(
          classOf[SecurityException],
          () =>
            joined
              .filter("false")
              .selectExpr("p.id", "p.secret", "p.region")
              .write
              .mode("append")
              .insertInto("a.db.t")
        )
        assertNoCommit()
        policy.denied.clear()
        policy.denied.add("b:secret")
        assertThrows(
          classOf[SecurityException],
          () => joined.filter("d.secret = 'b-one'").selectExpr("p.id").collect()
        )
        policy.denied.clear()
        policy.denied.add("a:*")
        assertThrows(classOf[Exception], () => frame.write.mode("append").insertInto("a.db.t"))
        assertNoCommit()
        policy.denied.clear()
        val sourcePayload = postgres.payloadReads()
        var revoked = false
        policy.onRequest = request => {
          if (
            !revoked && request.getAccessType == "update" && postgres.payloadReads() > sourcePayload
          ) {
            revoked = true; policy.denied.add("b:id")
          }
        }
        try
          assertThrows(
            classOf[Exception],
            () =>
              joined
                .selectExpr("p.id", "p.secret", "p.region")
                .write
                .mode("append")
                .insertInto("a.db.t")
          )
        finally { policy.onRequest = _ => (); policy.denied.clear() }
        assertTrue(revoked)
        assertNoCommit()
        val payload = postgres.payloadReads()
        var closed = false
        policy.onRequest = request => {
          if (!closed && request.getAccessType == "update" && postgres.payloadReads() > payload) {
            closed = true; api.close()
          }
        }
        assertThrows(classOf[Exception], () => frame.write.mode("append").insertInto("a.db.t"))
        assertTrue(closed)
        assertNoCommit()
      } finally { policy.onRequest = _ => (); api.close() }
  }

  @Test
  def adaptiveAndEmptyInputsPreserveNativeCommitBehavior(): Unit = {
    Seq(false, true).foreach { skipping =>
      withFixture(adaptive = true, skipping = skipping) { (spark, postgres, _) =>
        val api = GravitinoJdbcInput.open(spark, 1200)
        try {
          val frame = api.read(inputOptions(postgres))
          assertEquals(3L, frame.count())
          val empty = frame.filter("false")
          assertEquals(0, empty.collect().length)
          empty.write.mode("append").insertInto("a.db.t")
          assertNoCommit()
          val preplannedEmpty = frame.filter("false")
          preplannedEmpty.queryExecution.optimizedPlan
          try frame.write.format("delta").mode("append").saveAsTable("a.db.t")
          finally evidence(spark, "adaptive-" + skipping)
          assertEquals(Seq(1L, 2L, 3L), committedRows(spark, 1).map(_.getLong("id", 0)).sorted)
          assertEquals(3L, frame.count())
          assertEquals(0, empty.collect().length)
          assertEquals(0, preplannedEmpty.collect().length)
          api.close()
          assertThrows(classOf[SecurityException], () => preplannedEmpty.collect())
        } finally api.close()
      }
    }
  }

  @Test
  def expiredSourceAndRevokedTargetStopBeforeNativePublication(): Unit = {
    Seq("expired", "revoked").foreach { scenario =>
      withFixture() { (spark, postgres, policy) =>
        var now = 0L
        val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => now)
        try {
          val bound = scope.load(inputOptions(postgres))
          val frame = RegisteredJdbcPostgresFixture.frame(spark, LogicalRelation(bound.relation))
          val payload = postgres.payloadReads()
          var fired = false
          policy.onRequest = request => {
            if (!fired && request.getAccessType == "update" && postgres.payloadReads() > payload) {
              fired = true
              if (scenario == "expired") now = TimeUnit.SECONDS.toNanos(1200)
              else policy.denied.add("a:*")
            }
          }
          try
            assertThrows(classOf[Exception], () => frame.write.mode("append").insertInto("a.db.t"))
          finally evidence(spark, scenario)
          assertTrue(fired)
          assertNoCommit()
        } finally { policy.onRequest = _ => (); scope.close() }
      }
    }
  }

  @Test
  def externalAppendRetainsOwnedTargetCacheRefresh(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      RawNativeObservationFixture.withEnabled(storage =
        Map(
          "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      ) { (spark, native, _) =>
        val cached = RawNativeObservationFixture.active(native) {
          val frame = native.read.format("delta").load(NativeSelectFixture.locations("a")).cache()
          assertEquals(3, frame.collect().length)
          frame
        }
        val oldEntry = spark.sharedState.cacheManager.lookupCachedData(cached).get
        val api = GravitinoJdbcInput.open(spark, 1200)
        try {
          val frame = api.read(inputOptions(postgres))
          try frame.write.mode("append").insertInto("a.db.t")
          finally evidence(spark, "target-cache")
          assertEquals(Seq(1L, 2L, 3L), committedRows(spark, 1).map(_.getLong("id", 0)).sorted)
          val replacement = spark.sharedState.cacheManager.lookupCachedData(cached).get
          assertNotSame(
            oldEntry.cachedRepresentation.cacheBuilder,
            replacement.cachedRepresentation.cacheBuilder
          )
          RawNativeObservationFixture.active(native) {
            assertEquals(
              6,
              native.read.format("delta").load(NativeSelectFixture.locations("a")).collect().length
            )
            cached.unpersist()
          }
          assertEquals(3L, frame.count())
        } finally api.close()
      }
    finally postgres.close()
  }

  @Test
  def publicationProgressionBelongsOnlyToItsScopeAndActiveCount(): Unit = withFixture() {
    (spark, postgres, _) =>
      val api = GravitinoJdbcInput.open(spark, 1200)
      val unrelated = GravitinoJdbcInput.open(spark, 1200)
      val executor = Executors.newSingleThreadExecutor()
      try {
        val frame = api.read(inputOptions(postgres))
        val foreign = unrelated.read(inputOptions(postgres))
        assertEquals(3L, frame.count())
        val countOptions = inputOptions(postgres)
        countOptions.put(
          "dbtable",
          "(SELECT source.id FROM public.source CROSS JOIN (SELECT pg_advisory_xact_lock(8675309)) held) arkham_temp_table"
        )
        val countFrame = api.read(countOptions)
        val lock = postgres.blockExternalPayload()
        try {
          val subject = UserGroupInformation.getCurrentUser
          val counted = executor.submit(new Callable[Long] {
            override def call(): Long =
              subject.doAs(new PrivilegedExceptionAction[Long] {
                override def run(): Long = {
                  SparkSession.setActiveSession(spark)
                  try countFrame.count()
                  finally SparkSession.clearActiveSession()
                }
              })
          })
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
          while (
            postgres.blockedSchemaReads() == 0 && !counted.isDone && System.nanoTime() < deadline
          ) Thread.sleep(25)
          if (counted.isDone) counted.get()
          assertEquals(1, postgres.blockedSchemaReads())
          postgres.advanceSource()
          try frame.write.mode("append").insertInto("a.db.t")
          finally evidence(spark, "overlapping-count")
          assertEquals(Seq(1L, 2L, 3L, 4L), committedRows(spark, 1).map(_.getLong("id", 0)).sorted)
          assertFalse(counted.isDone)
          lock.close()
          assertEquals(3L, counted.get(20, TimeUnit.SECONDS))
          assertEquals(4L, frame.count())
          assertThrows(classOf[SecurityException], () => foreign.count())
          val version = "spark.databricks.delta.lastCommitVersionInSession"
          val published = spark.conf.get(version)
          spark.conf.set(version, "999")
          try assertThrows(classOf[SecurityException], () => frame.count())
          finally spark.conf.set(version, published)
          assertEquals(4L, frame.count())
        } finally lock.close()
      } finally { executor.shutdownNow(); unrelated.close(); api.close() }
  }

  @Test
  def sameIssuedInputKeepsConcurrentCountAndAppendOwnersSeparate(): Unit = withFixture() {
    (spark, postgres, _) =>
      val api = GravitinoJdbcInput.open(spark, 1200)
      val executor = Executors.newFixedThreadPool(2)
      try {
        val options = inputOptions(postgres)
        options.put(
          "dbtable",
          "(SELECT source.id, source.secret, source.region FROM public.source CROSS JOIN (SELECT pg_advisory_xact_lock(8675309)) held) arkham_temp_table"
        )
        val frame = api.read(options)
        val subject = UserGroupInformation.getCurrentUser
        def owned[A](body: => A): A = subject.doAs(new PrivilegedExceptionAction[A] {
          override def run(): A = {
            SparkSession.setActiveSession(spark)
            try body
            finally SparkSession.clearActiveSession()
          }
        })
        val lock = postgres.blockExternalPayload()
        try {
          val counted = executor.submit(new Callable[Long] {
            override def call(): Long = owned { frame.count() }
          })
          val appended = executor.submit(new Callable[Unit] {
            override def call(): Unit = owned {
              try frame.write.mode("append").insertInto("a.db.t")
              finally evidence(spark, "same-input-overlap")
            }
          })
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
          while (
            postgres.blockedSchemaReads() != 2 && !counted.isDone && !appended.isDone && System
              .nanoTime() < deadline
          ) Thread.sleep(25)
          if (counted.isDone) counted.get()
          if (appended.isDone) appended.get()
          assertEquals(2, postgres.blockedSchemaReads())
          lock.close()
          assertEquals(3L, counted.get(30, TimeUnit.SECONDS))
          appended.get(30, TimeUnit.SECONDS)
          assertEquals(Seq(1L, 2L, 3L), committedRows(spark, 1).map(_.getLong("id", 0)).sorted)
          assertEquals(3L, frame.count())
        } finally lock.close()
      } finally { executor.shutdownNow(); api.close() }
  }

  private def committedRows(
      spark: SparkSession,
      version: Int
  ): Vector[Group] = {
    val target = Paths.get(NativeSelectFixture.locations("a"))
    val mapper = new ObjectMapper
    val actions = Files
      .readAllLines(target.resolve(f"_delta_log/$version%020d.json"))
      .asScala
      .map(mapper.readTree(_))
    assertEquals(1, actions.count(_.has("commitInfo")))
    assertFalse(actions.exists(_.has("remove")))
    actions.filter(_.has("add")).toVector.flatMap { action =>
      val reader = ParquetReader
        .builder[Group](
          new GroupReadSupport,
          new HadoopPath(
            target.resolve(action.get("add").get("path").asText()).toUri
          )
        )
        .withConf(spark.sparkContext.hadoopConfiguration)
        .build()
      try Iterator.continually(reader.read()).takeWhile(_ != null).toVector
      finally reader.close()
    }
  }

  private def evidence(spark: SparkSession, scenario: String): Unit = {
    val directory = Paths.get(
      "build",
      "external-jdbc-evidence",
      scenario + "-" + UUID.randomUUID().toString
    )
    Files.createDirectories(directory)
    val target = Paths.get(NativeSelectFixture.locations("a"))
    val paths = Files.walk(target)
    try {
      val files = paths.iterator().asScala.filter(Files.isRegularFile(_)).toVector
      Files.write(
        directory.resolve("native-files.txt"),
        files.map(path => target.relativize(path).toString + " " + Files.size(path)).sorted.asJava
      )
      files
        .filter(path =>
          path.getParent.getFileName.toString == "_delta_log" && path.toString.endsWith(".json")
        )
        .foreach(path => Files.copy(path, directory.resolve(path.getFileName)))
      Files.write(
        directory.resolve("runtime.txt"),
        Seq(
          "Spark=" + spark.version,
          "Delta=" + io.delta.VERSION,
          "target=" + target.toString,
          "java=" + System.getProperty("java.version"),
          "driver=" + Class
            .forName("org.postgresql.Driver")
            .getProtectionDomain
            .getCodeSource
            .getLocation
            .toString
        ).asJava
      )
    } finally paths.close()
  }

  private def inputOptions(
      postgres: RegisteredJdbcPostgresFixture
  ): java.util.Map[String, String] = {
    val options = ExternalJdbcTestFixture.options
    options.put("url", postgres.url()); options.put("password", postgres.password())
    options.put("dbtable", "(SELECT id, secret, region FROM public.source) arkham_temp_table")
    options
  }
  private def assertNoCommit(): Unit = assertFalse(
    Files.exists(
      Paths.get(NativeSelectFixture.locations("a")).resolve("_delta_log/00000000000000000001.json")
    )
  )
  private def withFixture(adaptive: Boolean = false, skipping: Boolean = true)(
      body: (
          SparkSession,
          RegisteredJdbcPostgresFixture,
          TestDeltaRangerNativeSelect.Fixture
      ) => Unit
  ): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => body(spark, postgres, policy),
        Some(NativeSelectFixture.extension(policy)),
        Map(
          "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          "spark.sql.adaptive.enabled" -> adaptive.toString,
          "spark.databricks.delta.stats.skipping" -> skipping.toString
        )
      )
    finally { policy.close(); postgres.close() }
  }

}
