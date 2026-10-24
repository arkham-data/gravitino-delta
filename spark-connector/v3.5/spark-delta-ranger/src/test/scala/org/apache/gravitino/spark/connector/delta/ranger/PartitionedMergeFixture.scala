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

import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Action
import org.slf4j.LoggerFactory

/** Shared trusted setup and observations for partitioned MERGE qualification. */
private[ranger] object PartitionedMergeFixture {
  val initial =
    "SELECT 1L id, 'b-one' secret, 'keep' region UNION ALL SELECT 2L, 'b-two', 'drop' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep'"
  val source =
    "(SELECT * FROM table_changes('a.db.t', 0, 4) WHERE _commit_version = 4 AND _change_type = 'update_postimage') s"
  val insert = "WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)"
  def merge(clause: String, on: String = "t.id = s.id"): String =
    s"MERGE INTO b.db.t t USING $source ON $on $clause"
  def bag(rows: Seq[Row]): Map[Row, Int] =
    rows.groupBy(identity).map { case (r, copies) => r -> copies.size }
  def rows(spark: SparkSession, name: String = "b"): Map[Row, Int] =
    bag(spark.sql(s"SELECT id, secret, region FROM $name.db.t").collect().toVector)
  def fixture(
      test: (SparkSession, RangerBasePlugin) => Unit,
      prepare: SparkSession => Unit = _ => (),
      alias: Boolean = false
  ): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => { origins(spark); test(spark, plugin) },
      aliasTarget = alias,
      prepareTarget = spark => { reset(spark); prepare(spark) }
    )
  def reset(
      spark: SparkSession,
      name: String = "b",
      rows: String = initial,
      keys: Seq[String] = Seq("region"),
      cdf: Boolean = true
  ): Unit = {
    val path = Paths.get(NativeSelectFixture.locations(name))
    val files = Files.walk(path)
    try files.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
    finally files.close()
    DeltaLog.clearCache()
    spark
      .sql(rows)
      .coalesce(1)
      .write
      .format("delta")
      .partitionBy(keys: _*)
      .option("delta.enableChangeDataFeed", cdf.toString)
      .save(path.toString)
  }
  def partitionSource(spark: SparkSession): Unit = {
    reset(
      spark,
      "a",
      "SELECT 1L id, 'old' secret, 'keep' region UNION ALL SELECT 2L, 'old', 'drop'"
    )
    val location = NativeSelectFixture.locations("a")
    (1 to 3).foreach(n =>
      spark
        .sql(s"SELECT ${10 + n}L id, 'history' secret, 'keep' region")
        .write
        .format("delta")
        .mode("append")
        .save(location)
    )
    spark
      .sql(s"""MERGE INTO delta.`$location` t USING
      (SELECT 1L id, 'changed' secret UNION ALL SELECT 2L, 'delete' UNION ALL SELECT 4L, 'new') s
      ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN DELETE
      WHEN MATCHED THEN UPDATE SET secret = s.secret
      WHEN NOT MATCHED THEN INSERT (id, secret, region) VALUES (s.id, s.secret, 'keep')""")
      .collect()
  }
  def files(name: String = "b"): Set[String] = {
    val walk = Files.walk(Paths.get(NativeSelectFixture.locations(name)))
    try walk.iterator().asScala.filter(_.toString.endsWith(".parquet")).map(_.toString).toSet
    finally walk.close()
  }
  def actions(location: String, version: Long): Vector[Action] =
    Files
      .readAllLines(Paths.get(location, "_delta_log", f"$version%020d.json"))
      .asScala
      .toVector
      .map(Action.fromJson)
  def messages(error: Throwable): String = Iterator
    .iterate(error)(_.getCause)
    .takeWhile(_ != null)
    .map(_.toString)
    .mkString("\n")
  def origins(spark: SparkSession): Unit = {
    val log = LoggerFactory.getLogger(getClass)
    Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[RangerBasePlugin],
      classOf[scala.Option[_]]
    )
      .foreach { c =>
        val url = c.getProtectionDomain.getCodeSource.getLocation
        val sha = MessageDigest
          .getInstance("SHA-256")
          .digest(Files.readAllBytes(Paths.get(url.toURI)))
          .map(b => f"${b & 0xff}%02x")
          .mkString
        log.warn(
          "Partition Task2 origin: " + c.getName + " " + url + " version=" +
            c.getPackage.getImplementationVersion + " sha256=" + sha
        )
      }
    log.warn(
      "Partition Task2 runtime Spark=" + spark.version + " Scala=" +
        scala.util.Properties.versionNumberString + " Java=" + System.getProperty("java.version")
    )
  }
}
