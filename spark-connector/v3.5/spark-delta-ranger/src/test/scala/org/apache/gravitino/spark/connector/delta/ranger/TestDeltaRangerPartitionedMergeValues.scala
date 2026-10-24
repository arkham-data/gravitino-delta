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

import java.nio.file.Paths
import java.util.TimeZone
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, AddCDCFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/** Native conversion and CDC partition maps are compared with stock Delta. */
class TestDeltaRangerPartitionedMergeValues {
  import PartitionedMergeFixture._
  import TestDeltaRangerMergeSource.version

  @Test
  def orderedEscapedNullAndEmptyStringsMatchNative(): Unit =
    scenario("STRING", "'old/= %雪'", "'next/= %雪'", "''", multiple = true)
  @Test
  def integerPartitionsMatchNative(): Unit = scenario("INT", "-7", "42", "NULL", multiple = true)
  @Test
  def datePartitionsMatchNative(): Unit =
    scenario("DATE", "DATE '2020-02-29'", "DATE '2024-03-10'", "NULL")
  @Test
  def decimalPartitionsMatchNative(): Unit =
    scenario("DECIMAL(12,3)", "-123.450", "987.065", "NULL")
  @Test
  def binaryNonUtf8ConversionMatchesNative(): Unit =
    scenario("BINARY", "X'6f6c64'", "X'ff2f3d25'", "NULL")
  @Test
  def binaryNulRetainsNativeLocalFilesystemFailure(): Unit =
    scenario("BINARY", "X'6f6c64'", "X'00ff2f3d25'", "NULL", nativeFailure = true)
  @Test
  def utcTimestampPartitionsMatchNative(): Unit = timestamp("UTC", true)
  @Test
  def nonUtcTimestampPartitionsMatchNative(): Unit = timestamp("America/Los_Angeles", true)
  @Test
  def nonUtcLegacyTimestampPartitionsMatchNative(): Unit = timestamp("America/Los_Angeles", false)

  @Test
  def timestampSessionAndJvmZonesRemainNativeWhenDifferent(): Unit =
    timestamp("America/Los_Angeles", true, "UTC")

  private def timestamp(zone: String, normalize: Boolean, jvmZone: String = null): Unit = {
    val old = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(Option(jvmZone).getOrElse(zone)))
    try
      scenario(
        "TIMESTAMP",
        "TIMESTAMP '2024-03-09 23:12:13.123456'",
        "TIMESTAMP '2024-03-11 01:02:03.654321'",
        "NULL",
        zone = zone,
        normalize = normalize
      )
    finally TimeZone.setDefault(old)
  }
  private def settings(spark: SparkSession, zone: String, normalize: Boolean)(
      body: => Unit
  ): Unit = {
    val values = Map(
      "spark.sql.session.timeZone" -> zone,
      DeltaSQLConf.UTC_TIMESTAMP_PARTITION_VALUES.key -> normalize.toString
    )
    val previous = spark.sessionState.conf.getAllConfs
    values.foreach { case (key, value) => spark.conf.set(key, value) }
    try {
      assertEquals(zone, spark.sessionState.conf.sessionLocalTimeZone)
      assertEquals(
        normalize,
        spark.sessionState.conf.getConf(DeltaSQLConf.UTC_TIMESTAMP_PARTITION_VALUES)
      )
      LoggerFactory
        .getLogger(getClass)
        .warn(
          "Partition Task2 value settings session=" + zone + " JVM=" + TimeZone.getDefault.getID + " normalize=" + normalize
        )
      body
    } finally
      values.keys.foreach { key =>
        previous.get(key) match {
          case Some(value) => spark.conf.set(key, value)
          case None        => spark.conf.unset(key)
        }
      }
  }
  private def scenario(
      kind: String,
      original: String,
      moved: String,
      inserted: String,
      multiple: Boolean = false,
      zone: String = "UTC",
      normalize: Boolean = true,
      nativeFailure: Boolean = false
  ): Unit = {
    val keys = if (multiple) Seq("region", "secret") else Seq("region")
    val initial =
      s"SELECT 1L id, 'shared/= %雪' secret, CAST($original AS $kind) region UNION ALL SELECT 2L, 'shared/= %雪', CAST($original AS $kind) UNION ALL SELECT 3L, 'untouched', CAST(NULL AS $kind)"
    val statement = TestDeltaRangerMergeSource.sql
      .replace("region = s.region", s"region = CAST($moved AS $kind)")
      .replace(
        "VALUES (s.id, s.secret, s.region)",
        s"VALUES (s.id, s.secret, CAST($inserted AS $kind))"
      )
    val projection = "id, secret, region"
    // Binary comparison is test-side only; Hex is outside the governed SELECT grammar.
    def valueBag(rows: Seq[Row]): Map[Row, Int] = bag(
      rows.map(row =>
        Row.fromSeq(row.toSeq.map {
          case bytes: Array[Byte] => bytes.map(b => f"${b & 0xff}%02X").mkString
          case value              => value
        })
      )
    )
    def execute(spark: SparkSession, statement: String): Unit = {
      if (nativeFailure) {
        val failure = assertThrows(classOf[Exception], () => spark.sql(statement).collect())
        assertTrue(messages(failure).contains("TASK_WRITE_FAILED"), messages(failure))
        assertTrue(messages(failure).contains("Mkdirs failed to create file:"), messages(failure))
        assertTrue(
          Iterator
            .iterate(failure: Throwable)(_.getCause)
            .takeWhile(_ != null)
            .exists(_.isInstanceOf[java.io.IOException]),
          messages(failure)
        )
      } else assertEquals(Vector(Row(3L, 1L, 1L, 1L)), spark.sql(statement).collect().toVector)
    }
    var oracleRows = Map.empty[Row, Int]
    var oracleEvents = Map.empty[Row, Int]
    var oracleAdds = Set.empty[Map[String, String]]
    var oracleCdc = Set.empty[Map[String, String]]
    fixture(
      (spark, _) =>
        settings(spark, zone, normalize) {
          val before = valueBag(spark.sql(s"SELECT $projection FROM b.db.t").collect().toVector)
          var checkpoint = 3L
          execute(spark, statement)
          if (!nativeFailure) checkpoint = 4L
          assertEquals(if (nativeFailure) 3L else 4L, checkpoint)
          assertEquals(if (nativeFailure) 0L else 1L, version("b"))
          val actualRows = valueBag(spark.sql(s"SELECT $projection FROM b.db.t").collect().toVector)
          assertEquals(oracleRows, actualRows)
          if (nativeFailure) assertEquals(before, actualRows)
          else {
            assertEquals(
              oracleEvents,
              valueBag(
                spark
                  .sql(s"SELECT $projection, _change_type FROM table_changes('b.db.t', 1, 1)")
                  .collect()
                  .toVector
              )
            )
            val actual = actions(NativeSelectFixture.locations("b"), 1L)
            val adds = actual.collect { case a: AddFile => a }
            val cdc = actual.collect { case a: AddCDCFile => a }
            assertEquals(oracleAdds, adds.map(_.partitionValues).toSet)
            assertEquals(oracleCdc, cdc.map(_.partitionValues).toSet)
            assertFalse(cdc.isEmpty)
            (adds.map(_.partitionValues) ++ cdc.map(_.partitionValues)).foreach(m =>
              assertEquals(keys.toSet, m.keySet)
            )
            cdc.foreach(a => assertTrue(a.path.startsWith("_change_data/"), a.path))
          }
          LoggerFactory
            .getLogger(getClass)
            .warn(
              s"Partition Task2 native parity type=$kind keys=$keys failure=$nativeFailure adds=$oracleAdds cdc=$oracleCdc rows=$oracleRows events=$oracleEvents"
            )
        },
      prepare = spark =>
        settings(spark, zone, normalize) {
          reset(spark, rows = initial, keys = keys)
          val oracle = Paths
            .get(NativeSelectFixture.locations("b"))
            .resolveSibling("native-values-oracle")
            .toString
          spark
            .sql(initial)
            .coalesce(1)
            .write
            .format("delta")
            .partitionBy(keys: _*)
            .option("delta.enableChangeDataFeed", "true")
            .save(oracle)
          val native = statement
            .replace("b.db.t", s"delta.`$oracle`")
            .replace(
              "table_changes('a.db.t'",
              s"table_changes_by_path('${NativeSelectFixture.locations("a")}'"
            )
          val before =
            valueBag(spark.sql(s"SELECT $projection FROM delta.`$oracle`").collect().toVector)
          execute(spark, native)
          assertEquals(
            if (nativeFailure) 0L else 1L,
            DeltaLog.forTable(spark, oracle).update().version
          )
          oracleRows =
            valueBag(spark.sql(s"SELECT $projection FROM delta.`$oracle`").collect().toVector)
          if (nativeFailure) assertEquals(before, oracleRows)
          else {
            oracleEvents = valueBag(
              spark
                .sql(
                  s"SELECT $projection, _change_type FROM table_changes_by_path('$oracle', 1, 1)"
                )
                .collect()
                .toVector
            )
            val nativeActions = actions(oracle, 1L)
            oracleAdds = nativeActions.collect { case a: AddFile => a.partitionValues }.toSet
            oracleCdc = nativeActions.collect { case a: AddCDCFile => a.partitionValues }.toSet
            val authored = valueBag(
              spark
                .sql(s"SELECT 1L id, 'changed' secret, CAST($moved AS $kind) region")
                .collect()
                .toVector
            ).keys.head
            val persisted = oracleRows.keys.find(_.getLong(0) == 1L).get
            LoggerFactory
              .getLogger(getClass)
              .warn(
                s"Partition Task2 stock round-trip type=$kind authored=$authored persisted=$persisted equal=${authored == persisted}"
              )
          }
        }
    )
  }
}
