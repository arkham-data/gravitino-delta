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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Action, CommitInfo}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._

/** Native source setup and independent CDF oracle, completed before governed admission. */
private[ranger] final class DeltaRangerCdfReadFixture {
  var start: Long = -1L
  var end: Long = -1L
  var expected: Vector[Row] = Vector.empty

  def prepare(spark: SparkSession): Unit = {
    val location = NativeSelectFixture.locations("a")
    spark
      .sql(s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
      .collect()
    start = DeltaLog.forTable(spark, location).update().version
    spark
      .sql(
        "SELECT 7L id, 'duplicate' secret, 'US' region UNION ALL SELECT 8L, 'duplicate', 'US' UNION ALL SELECT 9L, 'outside', 'EU'"
      )
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(location)
    spark.sql(s"UPDATE delta.`$location` SET region = 'EU' WHERE id = 7").collect()
    spark.sql(s"DELETE FROM delta.`$location` WHERE id = 8").collect()
    spark.sql(s"DELETE FROM delta.`$location`").collect()
    end = DeltaLog.forTable(spark, location).update().version
    expected = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", start)
      .option("endingVersion", end)
      .load(location)
      .collect()
      .toVector
  }

  def read(spark: SparkSession): DataFrame = spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", start)
    .option("endingVersion", end)
    .table("a.db.t")

  def multiset(rows: Seq[Row]): Map[Row, Int] =
    DeltaRangerCdfReadFixture.rowMultiset(rows)
}

/** Shared native setup and fault injection into exclusively owned temporary tables. */
private[ranger] object DeltaRangerCdfReadFixture {
  def rowMultiset(rows: Seq[Row]): Map[Row, Int] =
    rows.groupBy(identity).map { case (row, copies) => row -> copies.size }

  def location: String = NativeSelectFixture.locations("a")

  def logFile(version: Long): Path = Paths.get(location, "_delta_log", f"$version%020d.json")

  def actions(version: Long): Vector[Action] =
    Files
      .readAllLines(logFile(version), StandardCharsets.UTF_8)
      .asScala
      .map(Action.fromJson)
      .toVector

  def replaceActions(version: Long, entries: Seq[Action]): Unit = {
    // CommitInfo operationParameters are raw JSON on write and decoded strings on read.
    // Preserve native commit-info bytes instead of serializing a parsed instance again.
    val commitInfo = Files
      .readAllLines(logFile(version), StandardCharsets.UTF_8)
      .asScala
      .filter(line => Action.fromJson(line).isInstanceOf[CommitInfo])
    val lines = commitInfo ++ entries.filterNot(_.isInstanceOf[CommitInfo]).map(_.json)
    Files.write(logFile(version), lines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
    Files.deleteIfExists(logFile(version).resolveSibling(f".$version%020d.json.crc"))
    ()
  }

  def head(spark: SparkSession): Long = DeltaLog.forTable(spark, location).update().version

  def enable(spark: SparkSession): Long = {
    spark
      .sql(s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
      .collect()
    head(spark)
  }

  def append(
      spark: SparkSession,
      select: String = "SELECT 7L id, 'repeat' secret, 'US' region"
  ): Long = {
    spark.sql(select).coalesce(1).write.format("delta").mode("append").save(location)
    head(spark)
  }

  def read(spark: SparkSession, start: Long, end: Long): DataFrame = spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", start)
    .option("endingVersion", end)
    .table("a.db.t")

  def oracle(spark: SparkSession, start: Long, end: Long): DataFrame = spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", start)
    .option("endingVersion", end)
    .load(location)

  def withGoverned(setup: SparkSession => Unit)(test: SparkSession => Unit): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = null
    policy.maskColumn = "unused"
    try
      NativeSelectFixture.withSpark(
        test,
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = setup
      )
    finally policy.engine.close()
  }
}
