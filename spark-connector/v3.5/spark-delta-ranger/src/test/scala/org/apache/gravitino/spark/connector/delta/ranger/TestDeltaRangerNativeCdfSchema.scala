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
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.sql.Timestamp
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import scala.collection.JavaConverters._

/** Stock Delta controls: the governed extension is never installed in these sessions. */
class TestDeltaRangerNativeCdfSchema {
  private val unsafe =
    "spark.databricks.delta.changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled"
  private val original = StructType(
    Seq(
      StructField("id", LongType),
      StructField("secret", StringType),
      StructField("region", StringType)
    )
  )

  @Test
  def additionInsideRangeUsesCapturedCurrentSchema(): Unit = withNative { spark =>
    enable(spark)
    append(spark, "SELECT 7L id, 'before' secret, 'US' region")
    alter(spark, "ADD COLUMNS (extra STRING COMMENT 'ordinary comment')")
    append(
      spark,
      "SELECT 4L id, 'new' secret, 'US' region, 'fresh' extra UNION ALL SELECT 4L, 'new', 'US', 'fresh'"
    )
    spark
      .sql(s"UPDATE delta.`$location` SET secret = 'updated', extra = 'changed' WHERE id = 1")
      .collect()
    observe(
      spark,
      "inside",
      1,
      5,
      original.add(
        StructField(
          "extra",
          StringType,
          true,
          new MetadataBuilder().putString("comment", "ordinary comment").build()
        )
      ),
      Vector(
        event(spark, 2, 7L, "before", "US", null, "insert"),
        event(spark, 4, 4L, "new", "US", "fresh", "insert"),
        event(spark, 4, 4L, "new", "US", "fresh", "insert"),
        event(spark, 5, 1L, "a-one", "keep", null, "update_preimage"),
        event(spark, 5, 1L, "updated", "keep", "changed", "update_postimage")
      )
    )
  }

  @Test
  def additionAfterEndDoesNotAddLaterRows(): Unit = withNative { spark =>
    enable(spark)
    append(spark, "SELECT 7L id, 'before' secret, 'US' region")
    alter(spark, "ADD COLUMNS (extra STRING)")
    append(spark, "SELECT 8L id, 'later' secret, 'US' region, 'later' extra")
    observe(
      spark,
      "after-end",
      1,
      2,
      original.add("extra", StringType),
      Vector(event(spark, 2, 7L, "before", "US", null, "insert"))
    )
    assertEquals(4L, DeltaRangerCdfReadFixture.head(spark))
  }

  @Test
  def additionAtStartAndMetadataOnlyRange(): Unit = withNative { spark =>
    enable(spark)
    alter(spark, "ADD COLUMNS (extra STRING)")
    val schema = original.add("extra", StringType)
    observe(spark, "metadata-only", 2, 2, schema, Vector.empty)
    append(spark, "SELECT 8L id, 'new' secret, 'US' region, 'present' extra")
    observe(
      spark,
      "at-start",
      2,
      3,
      schema,
      Vector(event(spark, 3, 8L, "new", "US", "present", "insert"))
    )
  }

  @Test
  def multipleAdditionsPreserveNativeImages(): Unit = withNative { spark =>
    enable(spark)
    append(spark, "SELECT 7L id, 'before' secret, 'US' region")
    alter(spark, "ADD COLUMNS (extra STRING)")
    spark.sql(s"UPDATE delta.`$location` SET extra = 'first' WHERE id = 1").collect()
    alter(spark, "ADD COLUMNS (score BIGINT)")
    spark.sql(s"UPDATE delta.`$location` SET score = 9 WHERE id = 1").collect()
    spark.sql(s"DELETE FROM delta.`$location` WHERE id = 1").collect()
    observe(
      spark,
      "multiple",
      1,
      7,
      original.add("extra", StringType).add("score", LongType),
      Vector(
        event(spark, 2, 7L, "before", "US", null, null, "insert"),
        event(spark, 4, 1L, "a-one", "keep", null, null, "update_preimage"),
        event(spark, 4, 1L, "a-one", "keep", "first", null, "update_postimage"),
        event(spark, 6, 1L, "a-one", "keep", "first", null, "update_preimage"),
        event(spark, 6, 1L, "a-one", "keep", "first", 9L, "update_postimage"),
        event(spark, 7, 1L, "a-one", "keep", "first", 9L, "delete")
      )
    )
  }

  @Test
  def nonadditiveHistoryKeepsNativeCompatibilityChecks(): Unit = withNative { spark =>
    // Nested evolution is stock-compatible, but intentionally outside the finite governed contract.
    val nested = Paths.get(location).resolveSibling("nested-cdf").toString
    spark
      .sql("SELECT 1L id, named_struct('leaf', 'one') detail")
      .write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .save(nested)
    spark.sql(s"ALTER TABLE delta.`$nested` ADD COLUMNS (detail.extra STRING)").collect()
    spark
      .sql("SELECT 2L id, named_struct('leaf', 'two', 'extra', 'new') detail")
      .write
      .format("delta")
      .mode("append")
      .save(nested)
    val nestedSchema = StructType(
      Seq(
        StructField("id", LongType),
        StructField(
          "detail",
          StructType(Seq(StructField("leaf", StringType), StructField("extra", StringType)))
        )
      )
    )
    observe(
      spark,
      "native-nested-accepted",
      0,
      2,
      nestedSchema,
      Vector(
        eventAt(spark, nested, 0, 1L, Row("one", null), "insert"),
        eventAt(spark, nested, 2, 2L, Row("two", "new"), "insert")
      ),
      nested
    )
    enable(spark)
    spark
      .sql("SELECT 'changed-type' id, 'new' secret, 'US' region")
      .write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .save(location)
    val failure = assertThrows(classOf[Exception], () => cdf(spark, 1, 2).collect())
    val messages = Iterator
      .iterate[Throwable](failure)(_.getCause)
      .takeWhile(_ != null)
      .map(_.getMessage)
      .mkString("\n")
    assertTrue(messages.contains("DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA"), messages)
    retain(spark, "native-type-rejected", location, Map("failure.txt" -> messages))
  }

  private def withNative(test: SparkSession => Unit): Unit =
    NativeSelectFixture.withSpark(
      spark => {
        assertFalse(spark.conf.get(unsafe, "false").toBoolean)
        assertEquals("false", spark.conf.get("spark.gravitino.enableDeltaRangerSupport"))
        assertEquals(
          "io.delta.sql.DeltaSparkSessionExtension",
          spark.conf.get("spark.sql.extensions")
        )
        test(spark)
      },
      startupOptions = Map("spark.gravitino.enableDeltaRangerSupport" -> "false", unsafe -> "false")
    )

  private def location: String = DeltaRangerCdfReadFixture.location
  private def enable(spark: SparkSession): Unit = { DeltaRangerCdfReadFixture.enable(spark); () }
  private def append(spark: SparkSession, select: String): Unit = {
    DeltaRangerCdfReadFixture.append(spark, select); ()
  }
  private def alter(spark: SparkSession, change: String): Unit = {
    spark.sql(s"ALTER TABLE delta.`$location` $change").collect(); ()
  }
  private def cdf(spark: SparkSession, start: Long, end: Long, path: String = location): DataFrame =
    spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", start)
      .option("endingVersion", end)
      .load(path)

  private def event(spark: SparkSession, version: Long, values: Any*): Row =
    eventAt(spark, location, version, values: _*)

  private def eventAt(spark: SparkSession, path: String, version: Long, values: Any*): Row = {
    val timestamp = spark
      .sql(s"DESCRIBE HISTORY delta.`$path`")
      .collect()
      .find(_.getAs[Long]("version") == version)
      .get
      .getAs[Timestamp]("timestamp")
    Row.fromSeq(values ++ Seq(version, timestamp))
  }

  private def observe(
      spark: SparkSession,
      label: String,
      start: Long,
      end: Long,
      schema: StructType,
      expected: Vector[Row],
      path: String = location
  ): Unit = {
    val frame = cdf(spark, start, end, path)
    val rows = frame.collect().toVector
    val fullSchema = schema
      .add("_change_type", StringType)
      .add("_commit_version", LongType)
      .add("_commit_timestamp", TimestampType)
    retain(
      spark,
      label,
      path,
      Map(
        "schema.json" -> frame.schema.json,
        "expected-schema.json" -> fullSchema.json,
        "rows.txt" -> rows.map(_.toString).sorted.mkString("\n"),
        "expected-rows.txt" -> expected.map(_.toString).sorted.mkString("\n"),
        "interval.txt" -> s"$start..$end"
      )
    )
    assertEquals(fullSchema, frame.schema)
    assertEquals(
      DeltaRangerCdfReadFixture.rowMultiset(expected),
      DeltaRangerCdfReadFixture.rowMultiset(rows)
    )
    assertTrue(
      rows.forall(row => row.getLong(row.length - 2) >= start && row.getLong(row.length - 2) <= end)
    )
  }

  private def retain(
      spark: SparkSession,
      label: String,
      path: String,
      artifacts: Map[String, String]
  ): Unit =
    sys.env.get("GRAVITINO_DELTA_CDF_SCHEMA_EVIDENCE_DIR").foreach { root =>
      val destination = Files.createDirectory(Paths.get(root).resolve(label))
      def write(name: String, value: String): Unit = {
        Files.write(destination.resolve(name), value.getBytes(StandardCharsets.UTF_8)); ()
      }
      artifacts.foreach { case (name, value) => write(name, value) }
      val stream = Files.list(Paths.get(path, "_delta_log"))
      try
        stream
          .iterator()
          .asScala
          .filter(p => p.getFileName.toString.matches("[0-9]{20}\\.json"))
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
          s"\nunsafe=${spark.conf.get(unsafe)}\nextensions=${spark.conf.get("spark.sql.extensions")}\nrangerEnabled=${spark.conf.get("spark.gravitino.enableDeltaRangerSupport")}"
      )
    }
}
