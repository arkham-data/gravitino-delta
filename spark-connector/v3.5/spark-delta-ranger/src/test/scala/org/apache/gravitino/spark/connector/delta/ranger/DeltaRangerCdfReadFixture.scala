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
    rows.groupBy(identity).map { case (row, copies) => row -> copies.size }
}
