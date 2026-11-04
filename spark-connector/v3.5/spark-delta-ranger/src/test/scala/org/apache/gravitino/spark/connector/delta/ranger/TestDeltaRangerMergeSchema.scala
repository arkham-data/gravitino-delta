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

import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.tables.DeltaTable
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/** Stock native controls qualify expectations only; they grant no governed execution authority. */
class TestDeltaRangerMergeSchema {
  private val autoMerge = "spark.databricks.delta.schema.autoMerge.enabled"
  private val original = StructType(
    Seq(
      StructField("id", IntegerType, nullable = true),
      StructField("value", StringType, nullable = true)
    )
  )
  private val evolved = original.add("extra", StringType, nullable = true)
  private val source = """SELECT 1 id, 'new' value, 'matched' extra, 'unused-one' unused
      |UNION ALL SELECT 2, 'inserted', 'insert-extra', 'unused-two'""".stripMargin
  private val oldRows = Seq(Row(1, "old"), Row(3, "untouched"), Row(4, null))
  private val existingRows =
    Seq(Row(1, "new"), Row(2, "inserted"), Row(3, "untouched"), Row(4, null))
  private val evolvedRows = Seq(
    Row(1, "new", "matched"),
    Row(2, "inserted", "insert-extra"),
    Row(3, "untouched", null),
    Row(4, null, null)
  )
  private val mapper = new ObjectMapper()

  @Test
  def nativeSqlStarsUseResolvedFinalSchemaWithEnabledDisabledAndMatchingControls(): Unit =
    withStock { spark =>
      for (enabled <- Seq(true, false)) {
        check(
          spark,
          s"sql-stars-$enabled",
          source.replace(", 'unused-one' unused", "").replace(", 'unused-two'", ""),
          enabled,
          stars,
          if (enabled) evolved else original,
          if (enabled) evolvedRows else existingRows
        )
      }
      check(
        spark,
        "sql-matching-enabled",
        "SELECT 1 id, 'new' value UNION ALL SELECT 2, 'inserted'",
        true,
        stars,
        original,
        existingRows
      )
    }

  @Test
  def nativeBuilderStarsMatchIndependentNativeExpectationsAndExplicitEvolutionFlag(): Unit =
    withStock { spark =>
      val payload = source.replace(", 'unused-one' unused", "").replace(", 'unused-two'", "")
      for (enabled <- Seq(true, false)) {
        check(
          spark,
          s"builder-stars-$enabled",
          payload,
          enabled,
          stars,
          if (enabled) evolved else original,
          if (enabled) evolvedRows else existingRows,
          builder = true
        )
      }
      check(
        spark,
        "builder-matching-enabled",
        "SELECT 1 id, 'new' value UNION ALL SELECT 2, 'inserted'",
        true,
        stars,
        original,
        existingRows,
        builder = true
      )
    }

  @Test
  def nativeFinalSchemaKeepsTargetOrderAndOnlyAssignedAdditionsWithNullableLiteral(): Unit =
    withStock { spark =>
      val reordered = "SELECT extra, value, id FROM (" +
        source.replace(", 'unused-one' unused", "").replace(", 'unused-two'", "") + ")"
      check(spark, "sql-source-order", reordered, true, stars, evolved, evolvedRows)
      val assigned = """WHEN MATCHED THEN UPDATE SET value = s.value, extra = 'literal'
          |WHEN NOT MATCHED THEN INSERT (id, value, extra) VALUES (s.id, s.value, 'literal')""".stripMargin
      val literalRows = Seq(
        Row(1, "new", "literal"),
        Row(2, "inserted", "literal"),
        Row(3, "untouched", null),
        Row(4, null, null)
      )
      check(
        spark,
        "sql-assigned-literal-unused-extra",
        source,
        true,
        assigned,
        evolved,
        literalRows
      )
      val metadata = new MetadataBuilder().putString("comment", "retained id").build()
      val strict =
        StructType(Seq(StructField("id", IntegerType, false, metadata), original("value")))
      check(
        spark,
        "sql-not-null-comment",
        source.replace(", 'unused-one' unused", "").replace(", 'unused-two'", ""),
        true,
        stars,
        strict.add("extra", StringType, nullable = true),
        evolvedRows,
        strictTarget = true
      )
    }

  @Test
  def missingFinalSchemaDisabledActualChangeAndUnsupportedChangesAreRefused(): Unit =
    withStock { spark =>
      val base = Metadata(schemaString = original.json)
      val protocol = Protocol(1, 2)
      def reject(finalSchema: Option[StructType], enabled: Boolean): Unit = {
        assertThrows(
          classOf[IllegalArgumentException],
          () => {
            DeltaRangerMergeSchema.derive(spark, base, protocol, finalSchema, enabled); ()
          }
        )
      }
      reject(None, true)
      reject(null, true)
      reject(Some(null), true)
      reject(Some(evolved), false)
      reject(Some(original.add("extra", StructType(Seq(StructField("child", StringType))))), true)
      reject(Some(StructType(Seq(StructField("id", LongType), original("value")))), true)
      reject(Some(StructType(original.fields.reverse)), true)
      val changedMetadata = StructType(
        original.fields.updated(
          0,
          original("id").copy(metadata =
            new MetadataBuilder().putString("comment", "changed").build()
          )
        )
      )
      reject(Some(changedMetadata), true)
      reject(
        Some(StructType(original.fields.updated(0, original("id").copy(nullable = false)))),
        true
      )
      Seq(true, false).foreach(enabled => {
        assertFalse(
          DeltaRangerMergeSchema.derive(spark, base, protocol, Some(original), enabled).evolves
        )
      })
      assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerMergeSchema.derive(null, base, protocol, Some(original), true); ()
        }
      )
      assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerMergeSchema.derive(spark, null, protocol, Some(original), true); ()
        }
      )
      assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerMergeSchema.derive(spark, base, null, Some(original), true); ()
        }
      )
    }

  @Test
  def nativeMissingRequiredDisabledSourceAndNativeNestedEvolutionStayOutsideFiniteAdapter(): Unit =
    withStock { spark =>
      val missing = seed(spark, "missing-required", strict = false)
      spark.conf.set(autoMerge, false)
      val before = DeltaLog.forTable(spark, missing.toString).update()
      assertThrows(
        classOf[AnalysisException],
        () => {
          spark.sql(query(missing, "SELECT 1 id", stars)).collect(); ()
        }
      )
      val after = DeltaLog.forTable(spark, missing.toString).update()
      assertEquals(before.version, after.version)
      assertEquals(before.metadata, after.metadata)
      assertEquals(
        bag(oldRows),
        bag(spark.read.format("delta").load(missing.toString).collect().toSeq)
      )
      preserve(
        spark,
        "sql-missing-required-disabled-not-committed",
        missing,
        before.metadata,
        after.metadata,
        before.protocol,
        before.version,
        after.version,
        "native analysis rejected missing value"
      )
      val nestedSource = "SELECT 1 id, 'new' value, named_struct('child', 'nested') extra"
      val target = seed(spark, "nested-control", strict = false)
      val nestedBefore = DeltaLog.forTable(spark, target.toString).update()
      spark.conf.set(autoMerge, true)
      val native = resolve(spark, query(target, nestedSource, stars))
      assertEquals(
        StructType(Seq(StructField("child", StringType, nullable = false))),
        native.migratedSchema.get("extra").dataType
      )
      assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerMergeSchema.derive(
            spark,
            nestedBefore.metadata,
            nestedBefore.protocol,
            native.migratedSchema,
            native.schemaEvolutionEnabled
          ); ()
        }
      )
      spark.sql(query(target, nestedSource, stars)).collect()
      val nestedAfter = DeltaLog.forTable(spark, target.toString).update()
      assertEquals(nestedBefore.version + 1, nestedAfter.version)
      assertTrue(nestedAfter.metadata.schema("extra").dataType.isInstanceOf[StructType])
      preserve(
        spark,
        "sql-nested-native-committed-adapter-refused",
        target,
        nestedBefore.metadata,
        nestedAfter.metadata,
        nestedAfter.protocol,
        nestedBefore.version,
        nestedAfter.version,
        native.toString
      )
    }

  private def stars: String = "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"

  private def withStock(test: SparkSession => Unit): Unit = NativeSelectFixture.withSpark(spark => {
    recordRuntime(spark)
    try test(spark)
    finally spark.conf.unset(autoMerge)
  })

  private def seed(spark: SparkSession, name: String, strict: Boolean): Path = {
    val directory =
      Paths.get(NativeSelectFixture.locations("a")).getParent.resolve("merge-schema-" + name)
    if (strict) {
      spark.sql(
        s"CREATE TABLE delta.`$directory` (id INT NOT NULL COMMENT 'retained id', value STRING) USING DELTA"
      )
      spark.sql(s"INSERT INTO delta.`$directory` VALUES (1, 'old'), (3, 'untouched'), (4, NULL)")
    } else {
      spark
        .createDataFrame(oldRows.asJava, original)
        .coalesce(1)
        .write
        .format("delta")
        .save(directory.toString)
    }
    directory
  }

  private def query(target: Path, payload: String, clauses: String): String =
    s"MERGE INTO delta.`$target` t USING ($payload) s ON t.id = s.id $clauses"

  private def resolve(spark: SparkSession, sql: String): MergeIntoCommand = {
    val analyzed =
      spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql)).analyzed
    val commands = analyzed.collect { case command: MergeIntoCommand => command }
    assertEquals(1, commands.size)
    commands.head
  }

  private def check(
      spark: SparkSession,
      name: String,
      payload: String,
      enabled: Boolean,
      clauses: String,
      expectedSchema: StructType,
      expectedRows: Seq[Row],
      builder: Boolean = false,
      strictTarget: Boolean = false
  ): Unit = {
    // Analysis and execution have independently seeded native targets; deriving never executes a plan.
    val analysisTarget = seed(spark, name + "-analysis", strictTarget)
    val executionTarget = seed(spark, name + "-execution", strictTarget)
    spark.conf.set(autoMerge, enabled)
    val command = resolve(spark, query(analysisTarget, payload, clauses))
    assertEquals(enabled, command.schemaEvolutionEnabled)
    val before = DeltaLog.forTable(spark, executionTarget.toString).update()
    val analysisVersion = DeltaLog.forTable(spark, analysisTarget.toString).update().version
    val intent = DeltaRangerMergeSchema.derive(
      spark,
      before.metadata,
      before.protocol,
      command.migratedSchema,
      command.schemaEvolutionEnabled
    )
    assertEquals(expectedSchema, intent.expectedMetadata.schema)
    assertEquals(enabled && expectedSchema.length > original.length, intent.evolves)
    assertEquals(before.metadata, intent.baseMetadata)
    assertEquals(before.metadata.copy(schemaString = expectedSchema.json), intent.expectedMetadata)
    assertEquals(before.protocol, intent.protocol)
    assertEquals(
      before.version,
      DeltaLog.forTable(spark, executionTarget.toString).update().version
    )
    if (builder) {
      // Deliberately disable the session option: the real native builder's flag enables evolution.
      spark.conf.set(autoMerge, false)
      val merge = DeltaTable
        .forPath(spark, executionTarget.toString)
        .as("t")
        .merge(spark.sql(payload).as("s"), "t.id = s.id")
      val effective = if (enabled) merge.withSchemaEvolution() else merge
      effective.whenMatched().updateAll().whenNotMatched().insertAll().execute()
    } else spark.sql(query(executionTarget, payload, clauses)).collect()
    val after = DeltaLog.forTable(spark, executionTarget.toString).update()
    assertEquals(before.version + 1, after.version)
    assertEquals(intent.expectedMetadata, after.metadata)
    assertEquals(intent.protocol, after.protocol)
    intent.requirePending(after.metadata, after.protocol)
    assertEquals(expectedSchema, after.metadata.schema)
    assertEquals(
      bag(expectedRows),
      bag(spark.read.format("delta").load(executionTarget.toString).collect().toSeq)
    )
    assertEquals(
      analysisVersion,
      DeltaLog.forTable(spark, analysisTarget.toString).update().version
    )
    val commit = executionTarget.resolve("_delta_log").resolve(f"${after.version}%020d.json")
    val actions = Files.readAllLines(commit, UTF_8).asScala.map(line => mapper.readTree(line))
    assertEquals(1, actions.count(_.has("commitInfo")))
    assertEquals(
      "MERGE",
      actions.find(_.has("commitInfo")).get.path("commitInfo").path("operation").asText()
    )
    assertEquals(if (intent.evolves) 1 else 0, actions.count(_.has("metaData")))
    actions
      .filter(_.has("metaData"))
      .foreach(node =>
        assertEquals(intent.expectedMetadata, Action.fromJson(node.toString).asInstanceOf[Metadata])
      )
    assertEquals(0, actions.count(_.has("protocol")))
    assertTrue(actions.exists(_.has("add")))
    assertTrue(actions.exists(_.has("remove")))
    assertFalse(Files.exists(commit.resolveSibling(f"${after.version + 1}%020d.json")))
    preserve(
      spark,
      name,
      executionTarget,
      before.metadata,
      after.metadata,
      after.protocol,
      before.version,
      after.version,
      command.toString
    )
  }

  private def bag(rows: Seq[Row]): Map[Row, Int] =
    rows.groupBy(identity).map { case (row, values) => row -> values.size }

  private def preserve(
      spark: SparkSession,
      name: String,
      target: Path,
      before: Metadata,
      after: Metadata,
      protocol: Protocol,
      beforeVersion: Long,
      afterVersion: Long,
      analyzed: String
  ): Unit =
    sys.env
      .get("GRAVITINO_DELTA_MERGE_SCHEMA_EVIDENCE_DIR")
      .foreach(root => {
        val directory = Files.createDirectory(Paths.get(root).resolve(name))
        def save(name: String, value: String): Unit = {
          Files.write(directory.resolve(name), value.getBytes(UTF_8)); ()
        }
        save("before-metadata.json", before.json)
        save("after-metadata.json", after.json)
        save("protocol.json", protocol.json)
        save("versions.txt", s"before=$beforeVersion\nafter=$afterVersion\ntarget=$target\n")
        save("analyzed.txt", analyzed)
        save(
          "rows.jsonl",
          spark.read
            .format("delta")
            .load(target.toString)
            .toJSON
            .collect()
            .sorted
            .mkString("\n") + "\n"
        )
        val logs = Files.list(target.resolve("_delta_log"))
        try
          logs
            .iterator()
            .asScala
            .filter(_.getFileName.toString.endsWith(".json"))
            .foreach(path => Files.copy(path, directory.resolve(path.getFileName)))
        finally logs.close()
      })

  private def recordRuntime(spark: SparkSession): Unit = {
    val origins = Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[MergeIntoCommand],
      classOf[DeltaTable],
      classOf[scala.Option[_]],
      DeltaRangerMergeSchema.getClass
    )
    val details = origins
      .map(clazz => {
        val path = Paths.get(clazz.getProtectionDomain.getCodeSource.getLocation.toURI)
        val hash = if (Files.isRegularFile(path)) {
          val digest = MessageDigest.getInstance("SHA-256")
          val input = Files.newInputStream(path)
          try {
            val buffer = new Array[Byte](65536)
            var count = input.read(buffer)
            while (count != -1) { digest.update(buffer, 0, count); count = input.read(buffer) }
          } finally input.close()
          digest.digest().map(value => f"${value & 0xff}%02x").mkString
        } else "classes-directory"
        s"${clazz.getName}=$path sha256=$hash"
      })
      .mkString("\n")
    val runtime =
      s"Spark=${spark.version}\nScala=${scala.util.Properties.versionNumberString}\nJava=${System.getProperty("java.version")}\n$details\n"
    LoggerFactory.getLogger(getClass).warn("MERGE schema stock runtime:\n" + runtime)
    sys.env
      .get("GRAVITINO_DELTA_MERGE_SCHEMA_EVIDENCE_DIR")
      .foreach(root => {
        val directory = Paths.get(root)
        require(Files.isDirectory(directory), "MERGE schema evidence directory must already exist")
        Files.write(directory.resolve("runtime.txt"), runtime.getBytes(UTF_8))
      })
  }
}
