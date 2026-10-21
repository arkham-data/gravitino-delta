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
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{AddCDCFile, AddFile, RemoveFile}
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import scala.collection.JavaConverters._

/** Installed results are compared as bags against independently executed native CDF reads. */
class TestDeltaRangerCdfNativeParity {
  import DeltaRangerCdfReadFixture._

  @Test
  def nativeEventMultisetIsPreserved(): Unit = {
    var expected = Vector.empty[Row]
    var end = -1L
    var emptyVersion = -1L
    var rewriteVersion = -1L
    var deleteVersion = -1L
    var mergeVersion = -1L
    withGoverned(spark => {
      val partitioned = Paths.get(location).resolveSibling("partitioned-cdf").toString
      NativeSelectFixture.locations += "a" -> partitioned
      spark
        .sql("SELECT 1L id, 'one' secret, 'US' region UNION ALL SELECT 2L, 'two', 'EU'")
        .write
        .format("delta")
        .partitionBy("region")
        .option("delta.enableChangeDataFeed", "true")
        .save(location)
      append(
        spark,
        "SELECT 7L id, 'repeat' secret, 'US' region UNION ALL SELECT 7L, 'repeat', 'US' UNION ALL SELECT 8L, 'eight', 'EU'"
      )
      spark.sql(s"UPDATE delta.`$location` SET secret = 'updated' WHERE id = 1").collect()
      spark
        .sql(s"""MERGE INTO delta.`$location` t USING
        (SELECT 1L id, 'merged' secret, 'US' region UNION ALL SELECT 2L, 'deleted', 'EU' UNION ALL SELECT 9L, 'new', 'APAC') s
        ON t.id = s.id WHEN MATCHED AND s.id = 2 THEN DELETE
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
        .collect()
      mergeVersion = head(spark)
      assertTrue(actions(mergeVersion).exists(_.isInstanceOf[AddCDCFile]))
      assertTrue(actions(mergeVersion).exists(_.isInstanceOf[AddFile]))
      assertTrue(actions(mergeVersion).exists(_.isInstanceOf[RemoveFile]))
      spark.sql(s"DELETE FROM delta.`$location` WHERE region = 'EU'").collect()
      deleteVersion = head(spark)
      assertTrue(actions(deleteVersion).exists(_.isInstanceOf[RemoveFile]))
      assertFalse(actions(deleteVersion).exists(_.isInstanceOf[AddCDCFile]))
      val log = DeltaLog.forTable(spark, location)
      val transaction = log.startTransaction(None)
      emptyVersion = transaction.commit(Seq.empty, DeltaOperations.ManualUpdate)
      assertEquals(deleteVersion + 1L, emptyVersion)
      assertFalse(
        actions(emptyVersion).exists(a =>
          a.isInstanceOf[AddFile] || a.isInstanceOf[RemoveFile] || a.isInstanceOf[AddCDCFile]
        )
      )
      val current = spark.read.format("delta").load(location)
      val copied = spark.createDataFrame(current.collect().toVector.asJava, current.schema)
      copied.write.format("delta").mode("overwrite").option("dataChange", "false").save(location)
      rewriteVersion = head(spark)
      assertTrue(actions(rewriteVersion).exists {
        case a: AddFile => !a.dataChange; case _ => false
      })
      assertTrue(actions(rewriteVersion).exists {
        case r: RemoveFile => !r.dataChange; case _ => false
      })
      end = rewriteVersion
      expected = oracle(spark, 0L, end).collect().toVector
      assertEquals(2, rowMultiset(expected).values.max)
    }) { spark =>
      val frame = read(spark, 0L, end)
      assertEquals(StringType, frame.schema("_change_type").dataType)
      assertEquals(LongType, frame.schema("_commit_version").dataType)
      assertEquals(TimestampType, frame.schema("_commit_timestamp").dataType)
      val rows = frame.collect().toVector
      assertEquals(rowMultiset(expected), rowMultiset(rows))
      assertEquals(
        Set("insert", "delete", "update_preimage", "update_postimage"),
        rows.map(_.getString(3)).toSet
      )
      assertTrue(
        rows.forall(row =>
          row.getTimestamp(5) != null && row.getLong(4) >= 0L && row.getLong(4) <= end
        )
      )
      assertFalse(
        rows.exists(row => row.getLong(4) == emptyVersion || row.getLong(4) == rewriteVersion)
      )
      assertEquals(
        Vector(Row(8L, "eight", "EU", "delete")),
        rows.filter(_.getLong(4) == deleteVersion).map(row => Row.fromSeq(row.toSeq.take(4)))
      )
      assertEquals(4, rows.count(_.getLong(4) == mergeVersion))
      assertEquals(2, rows.count(row => row.getLong(0) == 7L && row.getString(3) == "insert"))
      assertEquals(
        rowMultiset(expected.filter(_.getString(2) == "US")),
        rowMultiset(read(spark, 0L, end).filter("region = 'US'").collect().toVector)
      )
      assertTrue(read(spark, emptyVersion, rewriteVersion).collect().isEmpty)
      val deltaOrigin = classOf[DeltaLog].getProtectionDomain.getCodeSource.getLocation.toString
      assertTrue(Set("3.5.4", "3.5.6")(spark.version))
      assertTrue(
        deltaOrigin.endsWith("delta-spark_2.12-3.3.0.jar") || deltaOrigin.endsWith(
          "delta-spark_2.12-3.3.2.jar"
        )
      )
      assertEquals("2.12.21", scala.util.Properties.versionNumberString)
      assertEquals("17", System.getProperty("java.specification.version"))
      org.slf4j.LoggerFactory
        .getLogger(getClass)
        .warn(
          s"CDF parity runtime: spark=${spark.version}, delta=$deltaOrigin, scala=${scala.util.Properties.versionNumberString}, java=${System.getProperty("java.version")}"
        )
    }
  }

  @Test
  def explicitCdcDoesNotRequireSupersededAddOrRemoveFiles(): Unit = {
    var version = -1L
    var expected = Vector.empty[Row]
    var irrelevant = Vector.empty[String]
    withGoverned(spark => {
      enable(spark)
      spark.sql(s"UPDATE delta.`$location` SET secret = 'changed' WHERE id = 1").collect()
      version = head(spark)
      val entries = actions(version)
      assertTrue(entries.exists(_.isInstanceOf[AddCDCFile]))
      irrelevant = entries.collect { case a: AddFile => a.path; case r: RemoveFile => r.path }
      assertFalse(irrelevant.isEmpty)
      expected = oracle(spark, version, version).collect().toVector
      assertEquals(2, expected.size)
    }) { spark =>
      // Warm the named snapshot before removing files which native explicit CDC does not select.
      spark.table("a.db.t").collect()
      irrelevant.foreach(path => Files.delete(Paths.get(location).resolve(path)))
      assertEquals(
        rowMultiset(expected),
        rowMultiset(read(spark, version, version).collect().toVector)
      )
    }
  }
}
