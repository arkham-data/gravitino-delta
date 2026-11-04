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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.gravitino.rel.Column
import org.apache.gravitino.spark.connector.SparkTypeConverter
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Installed CDF admission preserves native images, current policy and bounded ownership. */
class TestDeltaRangerCdfSchemaEvolution {
  import DeltaRangerCdfReadFixture._

  private val input = "SELECT id, extra, region FROM table_changes('a.db.t', 0, 6) " +
    "WHERE _commit_version IN (0, 6) AND _change_type = 'insert'"
  private val mergeSql = s"MERGE INTO b.db.t t USING ($input) s ON t.id = s.id " +
    "WHEN MATCHED THEN UPDATE SET secret = s.extra, region = s.region " +
    "WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.extra, s.region)"

  @Test
  def namedAdditionRetainsNullsImagesAndInclusiveBounds(): Unit = positive("named")

  @Test
  def partitionedAdditionsUseCapturedSchemaWithAqe(): Unit =
    positive("partition-aqe", partitioned = true)

  @Test
  def legacyWriterFourStillReadsAdditiveHistory(): Unit = positive("legacy-writer4", legacy = true)

  @Test
  def addedColumnPoliciesRetainOriginalDemandWithoutAlter(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val history = new History
    policy.withSpark(history.prepare) { spark =>
      val before = files("a")
      val rows = history.expected.head._2
      val subject = DeltaRangerQuerySubject.current(spark)
      assertThrows(
        classOf[SecurityException],
        () =>
          policy.engine
            .authorize(subject, new DeltaRangerResourceKey("a", "db", "t", "extra", "alter", null))
      )
      policy.publish(denied = Set("extra"))
      assertEquals(
        rowMultiset(rows.map(r => Row(r.getLong(0)))),
        rowMultiset(read(spark, 1, 8).select("id").collect().toVector)
      )
      assertThrows(classOf[Exception], () => read(spark, 1, 8).select("extra").collect())
      assertThrows(
        classOf[Exception],
        () => read(spark, 1, 8).filter("extra = 'unused' AND false").select("id").collect()
      )
      policy.publish(
        filter = Some("extra IS NULL OR extra = 'visible'"),
        masks = Map("extra" -> "coalesce(concat({col}, '-masked'), 'missing')")
      )
      val filtered = rows
        .filter(r => r.isNullAt(3) || r.getString(3) == "visible")
        .map(r =>
          Row.fromSeq(r.toSeq.updated(3, if (r.isNullAt(3)) "missing" else "visible-masked"))
        )
      val masked = read(spark, 1, 8)
      val actual = masked.collect().toVector
      assertEquals(rowMultiset(filtered), rowMultiset(actual))
      retain(
        spark,
        "policy-null-populated",
        Map(
          "rows.txt" -> encode(actual),
          "expected-rows.txt" -> encode(filtered),
          "schema.json" -> masked.schema.json
        )
      )
      policy.publish()
      val held = read(spark, 1, 8).select("extra")
      held.queryExecution.optimizedPlan
      policy.publish(denied = Set("extra"))
      assertThrows(classOf[Exception], () => held.collect())
      assertThrows(classOf[Exception], () => read(spark, 1, 8).select("extra").collect())
      assertEquals(before, files("a"))
    }
  }

  @Test
  def additiveReceiptsRejectCopiedForeignAndEagerMissingHistory(): Unit = {
    val history = new History
    withGoverned(history.prepare) { spark =>
      val frame = read(spark, 1, 8)
      val relation = frame.queryExecution.analyzed.collectFirst { case r: LogicalRelation => r }.get
      assertThrows(
        classOf[Exception],
        () => TestDeltaRangerNativeSelect.rows(spark, relation.copy())
      )
      assertEquals(rowMultiset(history.expected.head._2), rowMultiset(frame.collect().toVector))
      val carrier = frame.queryExecution.optimizedPlan.asInstanceOf[DeltaRangerCdfRead.Carrier]
      assertThrows(classOf[SecurityException], () => carrier.copy()(carrier.receipt).receipt)
      val other = read(spark, 3, 4)
      other.collect()
      val producer = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfProducer]
      val foreign = other.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfProducer]
      assertThrows(
        classOf[SecurityException],
        () => producer.receipt.validateCollection(foreign.child)
      )
      assertThrows(classOf[SecurityException], () => producer.withNewChildren(Seq(foreign.child)))
      Files.delete(logFile(2L))
      assertThrows(classOf[Exception], () => read(spark, 1, 8).queryExecution.optimizedPlan)
      assertNull(DeltaRangerNativeOperation.current())
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("start-duplicate", "middle-duplicate", "drop-readd", "type-restore"))
  def incompatibleIntermediateMetadataCannotHideBehindRestoration(change: String): Unit = {
    val history = new History
    withGoverned(history.prepare) { spark =>
      spark.table("a.db.t").collect()
      val original = actions(1L).collectFirst { case m: Metadata => m }.get
      val version = if (change == "start-duplicate") 1L else 2L
      val changedSchema =
        if (change == "drop-readd") StructType(original.schema.fields.dropRight(1))
        else
          StructType(
            original.schema.fields
              .updated(0, original.schema.fields.head.copy(dataType = StringType))
          )
      val changed = original.copy(schemaString = changedSchema.json)
      if (change == "drop-readd" || change == "type-restore") {
        replaceActions(2L, actions(2L) :+ changed)
        replaceActions(3L, actions(3L) :+ original)
      } else replaceActions(version, actions(version) ++ Seq(changed, original))
      val failure = assertThrows(
        classOf[SecurityException],
        () => read(spark, 1, 8).queryExecution.optimizedPlan
      )
      assertTrue(failure.getMessage.contains("schema"), failure.getMessage)
      retain(spark, change, Map("failure.txt" -> failure.toString))
    }
  }

  @Test
  def retainedAndFreshAdditiveReadsRejectUnrefreshedSchema(): Unit = {
    val history = new History
    var tail: Path = null
    var bytes: Array[Byte] = null
    withGoverned(spark => {
      history.prepare(spark)
      spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (later STRING)").collect()
      tail = logFile(9L); bytes = Files.readAllBytes(tail)
      Files.delete(tail)
      Files.deleteIfExists(tail.resolveSibling(".00000000000000000009.json.crc"))
      Files.deleteIfExists(tail.resolveSibling("00000000000000000009.crc"))
    }) { spark =>
      val held = read(spark, 1, 8)
      assertEquals(rowMultiset(history.expected.head._2), rowMultiset(held.collect().toVector))
      Files.write(tail, bytes)
      assertThrows(classOf[SecurityException], () => held.collect())
      val failure = assertThrows(classOf[SecurityException], () => read(spark, 1, 8).collect())
      assertTrue(failure.getMessage.contains("registration binding"), failure.getMessage)
      retain(spark, "unrefreshed-schema", Map("failure.txt" -> failure.toString))
    }
  }

  @Test
  def freshSqlMergeConsumesAddedFieldWithOriginalPolicies(): Unit = merge(api = false)

  @Test
  def freshCompanionMergeConsumesAddedFieldWithOriginalPolicies(): Unit = merge(api = true)

  @Test
  def additiveRecoveryOwnerCannotMaterializeOrReplay(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        import TestDeltaRangerMergeSource.{field, version}
        val keys = Seq(
          "spark.databricks.delta.write.txnAppId",
          "spark.databricks.delta.write.txnVersion",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled",
          "spark.databricks.delta.schema.autoMerge.enabled"
        )
        val prior = keys.map(k => k -> spark.conf.getOption(k)).toMap
        spark.conf.set(keys(0), "additive-cdf-recovery")
        spark.conf.set(keys(1), "6")
        spark.conf.set(keys(2), "false")
        spark.conf.set(keys(3), "true")
        new TestDeltaRangerMergePolicies().publish(plugin, alter = true)
        refreshTargetSchema(spark)
        val evolvingInput =
          input.replace("SELECT id, extra, region", "SELECT id, extra AS secret, region, extra")
        val evolvingSql = s"MERGE INTO b.db.t t USING ($evolvingInput) s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        try {
          assertEquals(Seq(Row(3L, 2L, 0L, 1L)), spark.sql(evolvingSql).collect().toSeq)
          val sourceFiles = files("a"); val targetFiles = files("b")
          val query =
            spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(evolvingSql))
          val command = query.analyzed.collectFirst { case value: DeltaRangerMergeCommand =>
            value
          }.get
          val receipt = command.receipt
          assertTrue(receipt.recoveryCandidate.nonEmpty)
          val source = receipt.cdf
          assertThrows(classOf[SecurityException], () => source.openMergeExecution(receipt))
          assertThrows(classOf[SecurityException], () => source.mergeRecipe(source.carrier.output))
          assertThrows(classOf[SecurityException], () => source.plan(source.carrier))
          assertThrows(classOf[SecurityException], () => source.validateCollection(null))
          assertThrows(classOf[SecurityException], () => source.validateExecution(null))
          val registration = NativeSelectFixture.bindingFixture(spark, "b")
          val refresh = registration.onRead
          var observed = false
          registration.onRead = n => {
            observed = true
            assertNull(
              field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
                .get()
            )
            assertNull(
              field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
            )
            refresh(n)
          }
          try query.executedPlan.executeCollect()
          finally registration.onRead = refresh
          assertTrue(observed)
          assertEquals(0, field[Int](receipt, "sourceAttemptNumber"))
          assertNull(field[DeltaRangerMergeSource.Attempt](receipt, "sourceAttempt"))
          assertEquals(1L, version("b"))
          assertEquals(sourceFiles, files("a")); assertEquals(targetFiles, files("b"))
          val expected = Set(
            Row(1L, null, "keep", null),
            Row(2L, null, "drop", null),
            Row(3L, null, "keep", null),
            Row(40L, "payload", "keep", "payload")
          )
          val rows = spark.table("b.db.t").collect().toVector
          assertEquals(expected, rows.toSet)
          new TestDeltaRangerMergePolicies()
            .publish(plugin, sourceDeniedSelect = Seq("extra"), alter = true)
          assertThrows(classOf[SecurityException], () => spark.sql(evolvingSql))
          assertEquals(1L, version("b"))
          retain(
            spark,
            "recovery-no-payload",
            Map(
              "rows.txt" -> encode(rows),
              "expected-rows.txt" -> encode(expected.toVector),
              "source-attempts.txt" -> "0",
              "target-version.txt" -> "1"
            ),
            "b"
          )
        } finally
          prior.foreach { case (key, value) =>
            value.fold(spark.conf.unset(key))(spark.conf.set(key, _))
          }
      },
      prepareTarget = spark => prepareMerge(spark, "recovery")
    )

  private def prepareMerge(spark: SparkSession, label: String): Unit = {
    assertEquals(5L, append(spark, "SELECT 39L id, 'outside' secret, 'keep' region"))
    if (label == "companion") {
      val setting = "spark.databricks.delta.schema.autoMerge.enabled"
      val previous = spark.conf.getOption(setting)
      spark.conf.set(setting, "true")
      try
        spark
          .sql(s"""MERGE INTO delta.`$location` t USING
          |(SELECT 1L id, 'updated' secret, 'keep' region, 'changed' extra
          | UNION ALL SELECT 40L, 'source', 'keep', 'payload') s ON t.id = s.id
          |WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""".stripMargin)
          .collect()
      finally previous.fold(spark.conf.unset(setting))(spark.conf.set(setting, _))
    } else
      spark
        .sql("SELECT 40L id, 'source' secret, 'keep' region, 'payload' extra")
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(location)
    assertEquals(6L, head(spark))
    assertEquals(1, actions(6L).count(_.isInstanceOf[Metadata]))
    val expected = Vector(Row(1L, null, "keep"), Row(2L, null, "drop"), Row(40L, "payload", "keep"))
    val actual = oracle(spark, 0, 6)
      .filter("_commit_version IN (0, 6) AND _change_type = 'insert'")
      .select("id", "extra", "region")
      .collect()
      .toVector
    assertEquals(rowMultiset(expected), rowMultiset(actual))
    retain(
      spark,
      "merge-native-" + label,
      Map(
        "rows.txt" -> encode(actual),
        "expected-rows.txt" -> encode(expected),
        "schema.json" -> oracle(spark, 0, 6).schema.json
      )
    )
  }

  private def merge(api: Boolean): Unit = new TestDeltaRangerInstalledMerge().withPolicies(
    (spark, plugin) => {
      val policies = new TestDeltaRangerMergePolicies
      val before = files("a")
      val target = files("b")
      val setting = "spark.databricks.delta.schema.autoMerge.enabled"
      val prior = spark.conf.getOption(setting)
      def execute(): Unit = if (api) {
        GravitinoDeltaTable
          .forName(spark, "b.db.t")
          .alias("t")
          .merge(spark.sql(input).as("s"), "t.id = s.id")
          .whenMatchedUpdate(Map("secret" -> "s.extra", "region" -> "s.region").asJava)
          .whenNotMatchedInsert(
            Map("id" -> "s.id", "secret" -> "s.extra", "region" -> "s.region").asJava
          )
          .execute()
      } else { assertEquals(Seq(Row(3L, 2L, 0L, 1L)), spark.sql(mergeSql).collect().toSeq); () }
      policies.publish(plugin, sourceDeniedSelect = Seq("extra"))
      assertThrows(classOf[Exception], () => execute())
      assertEquals(0L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(target, files("b"))
      policies.publish(
        plugin,
        sourceMasks = Map("extra" -> "coalesce(concat({col}, '-masked'), 'missing')")
      )
      execute()
      val expected = Set(
        Row(1L, "missing", "keep"),
        Row(2L, "missing", "drop"),
        Row(3L, null, "keep"),
        Row(40L, "payload-masked", "keep")
      )
      val rows = spark.table("b.db.t").collect().toVector
      assertEquals(expected, rows.toSet)
      assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
      assertEquals(before, files("a"))
      assertEquals(prior, spark.conf.getOption(setting))
      retain(
        spark,
        if (api) "companion-merge" else "sql-merge",
        Map(
          "rows.txt" -> encode(rows),
          "expected-rows.txt" -> encode(expected.toVector),
          "schema.json" -> spark.table("b.db.t").schema.json
        ),
        "b"
      )
    },
    prepareTarget = spark => prepareMerge(spark, if (api) "companion" else "sql")
  )

  private def refreshTargetSchema(spark: SparkSession): Unit = {
    // Model the same named registration refresh as the existing evolving MERGE fixture.
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val previous = registration.onRead
    registration.onRead = n => {
      previous(n)
      val metadata = (0L to TestDeltaRangerMergeSource.version("b"))
        .flatMap(version =>
          PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), version)
        )
        .collect { case value: Metadata => value }
        .last
      val converter = new SparkTypeConverter
      registration.columns = metadata.schema.fields.map(field =>
        Column.of(
          field.name,
          converter.toGravitinoType(field.dataType),
          null,
          field.nullable,
          false,
          Column.DEFAULT_VALUE_NOT_SET
        )
      )
    }
  }

  private def positive(
      label: String,
      partitioned: Boolean = false,
      legacy: Boolean = false
  ): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val history = new History(partitioned, legacy)
    policy.withSpark(history.prepare) { spark =>
      if (partitioned) spark.conf.set("spark.sql.adaptive.enabled", "true")
      val before = files("a")
      assertEquals(
        history.schema.dropRight(3).map(_.name),
        spark.table("a.db.t").schema.fieldNames.toSeq
      )
      history.expected.foreach { case ((start, end), rows) =>
        val frame = read(spark, start, end)
        val actual = frame.collect().toVector
        assertEquals(history.schema, frame.schema)
        assertEquals(rowMultiset(rows), rowMultiset(actual))
        assertEquals(
          rowMultiset(rows),
          rowMultiset(
            spark.sql(s"SELECT * FROM table_changes('a.db.t', $start, $end)").collect().toVector
          )
        )
        retain(
          spark,
          s"$label-$start-$end",
          Map(
            "rows.txt" -> encode(actual),
            "expected-rows.txt" -> encode(rows),
            "schema.json" -> frame.schema.json,
            "expected-schema.json" -> history.schema.json,
            "interval.txt" -> s"$start..$end"
          )
        )
      }
      assertEquals(before, files("a"), "standalone reads must not create native files")
      assertEquals(if (legacy || partitioned) 4 else 7, history.protocol.minWriterVersion)
      assertEquals(1, history.protocol.minReaderVersion)
      if (partitioned) assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
    }
  }

  private final class History(partitioned: Boolean = false, legacy: Boolean = false) {
    var expected = Vector.empty[((Long, Long), Vector[Row])]
    var schema: StructType = null
    var protocol: Protocol = null
    def prepare(spark: SparkSession): Unit = {
      if (partitioned || legacy) {
        val stream = Files.walk(Paths.get(location))
        try stream.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
        finally stream.close()
        DeltaLog.clearCache()
        val writer = spark
          .sql("SELECT 1L id, 'base' secret, 'US' region")
          .coalesce(1)
          .write
          .format("delta")
          .option("delta.minReaderVersion", "1")
          .option("delta.minWriterVersion", "4")
        if (partitioned) writer.partitionBy("region").save(location) else writer.save(location)
      }
      assertEquals(1L, enable(spark))
      assertEquals(2L, append(spark, "SELECT 7L id, 'before' secret, 'US' region"))
      spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (extra STRING)").collect()
      assertEquals(
        4L,
        append(
          spark,
          "SELECT 8L id, 'after' secret, 'EU' region, 'visible' extra UNION ALL SELECT 8L, 'after', 'EU', 'visible'"
        )
      )
      spark.sql(s"UPDATE delta.`$location` SET extra = 'changed' WHERE id = 7").collect()
      spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (score INT)").collect()
      spark.sql(s"UPDATE delta.`$location` SET score = 9 WHERE id = 7").collect()
      spark.sql(s"DELETE FROM delta.`$location` WHERE id = 7").collect()
      assertEquals(8L, head(spark))
      protocol = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.protocol
      val full = oracle(spark, 1L, 8L)
      schema = full.schema
      val rows = full.collect().toVector
      assertEquals(
        Map(
          Row(7L, "before", "US", null, null, "insert", 2L) -> 1,
          Row(8L, "after", "EU", "visible", null, "insert", 4L) -> 2,
          Row(7L, "before", "US", null, null, "update_preimage", 5L) -> 1,
          Row(7L, "before", "US", "changed", null, "update_postimage", 5L) -> 1,
          Row(7L, "before", "US", "changed", null, "update_preimage", 7L) -> 1,
          Row(7L, "before", "US", "changed", 9, "update_postimage", 7L) -> 1,
          Row(7L, "before", "US", "changed", 9, "delete", 8L) -> 1
        ),
        rowMultiset(rows.map(r => Row.fromSeq(r.toSeq.dropRight(1))))
      )
      expected = Vector((1L, 8L), (1L, 2L), (3L, 4L), (3L, 3L)).map {
        case interval @ (start, end) =>
          val bounded = oracle(spark, start, end).collect().toVector
          assertEquals(
            rowMultiset(rows.filter(r => r.getLong(6) >= start && r.getLong(6) <= end)),
            rowMultiset(bounded)
          )
          interval -> bounded
      }
    }
  }

  private def files(catalog: String): Set[String] = {
    val root = Paths.get(NativeSelectFixture.locations(catalog))
    val stream = Files.walk(root)
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(p => root.relativize(p).toString)
        .toSet
    finally stream.close()
  }

  private def encode(rows: Seq[Row]): String = rows.map(_.toString).sorted.mkString("\n")

  private def retain(
      spark: SparkSession,
      label: String,
      artifacts: Map[String, String],
      catalog: String = "a"
  ): Unit =
    sys.env.get("GRAVITINO_DELTA_CDF_ADDITIVE_EVIDENCE_DIR").foreach { root =>
      val destination = Files.createDirectory(Paths.get(root).resolve(label))
      def write(name: String, value: String): Unit = {
        Files.write(destination.resolve(name), value.getBytes(StandardCharsets.UTF_8)); ()
      }
      artifacts.foreach { case (name, value) => write(name, value) }
      val stream = Files.list(Paths.get(NativeSelectFixture.locations(catalog), "_delta_log"))
      try
        stream
          .iterator()
          .asScala
          .filter(_.getFileName.toString.matches("[0-9]{20}\\.json"))
          .foreach(p => Files.copy(p, destination.resolve(p.getFileName)))
      finally stream.close()
      val origins = Seq(
        classOf[SparkSession],
        classOf[DeltaLog],
        classOf[scala.Option[_]],
        classOf[RangerBasePlugin]
      ).map { c =>
        val origin = Paths.get(c.getProtectionDomain.getCodeSource.getLocation.toURI)
        val hash = MessageDigest
          .getInstance("SHA-256")
          .digest(Files.readAllBytes(origin))
          .map(b => f"${b & 0xff}%02x")
          .mkString
        s"${c.getName} $origin $hash"
      }
      write(
        "runtime.txt",
        s"Spark=${spark.version} Scala=${scala.util.Properties.versionNumberString} Java=${System.getProperty("java.version")}\n" + origins
          .mkString("\n") +
          s"\nunsafe=${spark.conf.get("spark.databricks.delta.changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled", "false")}\nextensions=${spark.conf.get("spark.sql.extensions")}\nrangerEnabled=${spark.conf.get("spark.gravitino.enableDeltaRangerSupport")}\naqe=${spark.conf.get("spark.sql.adaptive.enabled")}"
      )
    }
}
