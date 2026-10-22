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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Named SQL uses the same bounded native operation as named DataFrame reads. */
class TestDeltaRangerCdfSql {

  /** Star exposes every CDF column and preserves the native rows through installed resolution. */
  @Test
  def namedSqlStarMatchesCompleteDataFrameAndNativeOutput(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      Seq("a.db.t", "`a`.`db`.`t`").foreach { name =>
        Seq("*" -> "", "changes.*" -> " WHERE region = 'US' AND id >= 7").foreach {
          case (projection, predicate) =>
            val sql = spark.sql(
              s"SELECT $projection FROM table_changes('$name', ${fixture.start}, ${fixture.end}) changes$predicate"
            )
            val frame =
              if (predicate.isEmpty) fixture.read(spark)
              else fixture.read(spark).filter("region = 'US' AND id >= 7")
            assertEquals(
              Seq("id", "secret", "region", "_change_type", "_commit_version", "_commit_timestamp"),
              sql.schema.fieldNames.toSeq
            )
            assertEquals(
              Seq(LongType, StringType, StringType, StringType, LongType, TimestampType),
              sql.schema.fields.map(_.dataType).toSeq
            )
            assertEquals(frame.schema, sql.schema)
            val expected = fixture.multiset(
              if (predicate.isEmpty) fixture.expected
              else fixture.expected.filter(row => row.getString(2) == "US" && row.getLong(0) >= 7)
            )
            assertFalse(expected.isEmpty)
            assertEquals(expected, fixture.multiset(frame.collect().toVector))
            assertEquals(expected, fixture.multiset(sql.collect().toVector))
        }
      }
    }
  }

  /** Star demands metadata SELECT while an explicit payload projection need not demand it. */
  @Test
  def namedSqlStarRejectsDeniedMetadataWithoutDenyingPayloadProjection(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val fixture = new DeltaRangerCdfReadFixture
    policy.withSpark(fixture.prepare) { spark =>
      val source = s"table_changes('a.db.t', ${fixture.start}, ${fixture.end})"
      Seq("_change_type", "_commit_version", "_commit_timestamp").foreach { column =>
        policy.publish(denied = Set(column))
        assertThrows(
          classOf[SecurityException],
          () => { spark.sql(s"SELECT * FROM $source").collect(); () },
          column
        )
        assertEquals(
          fixture.multiset(fixture.expected.map(row => Row.fromSeq(row.toSeq.take(3)))),
          fixture.multiset(spark.sql(s"SELECT id, secret, region FROM $source").collect().toVector),
          column
        )
      }
    }
  }

  @Test
  def namedSqlMatchesDataFrame(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      Seq("a.db.t", "`a`.`db`.`t`").foreach { name =>
        val sql = spark.sql(
          s"SELECT id, _change_type FROM table_changes('$name', ${fixture.start}, ${fixture.end}) WHERE region = 'US' AND id >= 7"
        )
        val frame =
          fixture.read(spark).filter("region = 'US' AND id >= 7").select("id", "_change_type")
        assertEquals(
          fixture.multiset(frame.collect().toVector),
          fixture.multiset(sql.collect().toVector)
        )
      }
    }
  }

  @Test
  def unsupportedSqlAndDataFrameShapesReject(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      val source = s"table_changes('a.db.t', ${fixture.start}, ${fixture.end})"
      val rejected = Seq(
        s"SELECT * FROM table_changes_by_path('${DeltaRangerCdfReadFixture.location}', ${fixture.start}, ${fixture.end})",
        s"SELECT * FROM table_changes('${DeltaRangerCdfReadFixture.location}', ${fixture.start}, ${fixture.end})",
        "SELECT * FROM table_changes('a.db.t', '2020-01-01', '2020-01-02')",
        s"SELECT * FROM table_changes('a.db.t', ${fixture.start})",
        s"SELECT * FROM table_changes('a.db.t', ${fixture.start} + 0, ${fixture.end})",
        s"SELECT * FROM table_changes('a.db.t', 1.0, ${fixture.end})",
        s"SELECT * FROM $source x JOIN $source y ON x.id = y.id",
        s"SELECT count(*) FROM $source",
        s"SELECT * FROM $source ORDER BY id",
        s"CACHE TABLE rejected_cdf AS SELECT * FROM $source"
      )
      rejected.foreach(query =>
        assertThrows(classOf[Exception], () => { spark.sql(query).collect(); () }, query)
      )
      assertThrows(classOf[Exception], () => { fixture.read(spark).cache().collect(); () })
      assertThrows(classOf[Exception], () => { fixture.read(spark).persist().collect(); () })
      assertThrows(classOf[Exception], () => { fixture.read(spark).rdd.collect(); () })
      assertThrows(
        classOf[Exception],
        () => {
          spark.readStream
            .option("readChangeFeed", "true")
            .option("startingVersion", fixture.start)
            .option("endingVersion", fixture.end)
            .table("a.db.t")
            .queryExecution
            .analyzed; ()
        }
      )
    }
  }
}
