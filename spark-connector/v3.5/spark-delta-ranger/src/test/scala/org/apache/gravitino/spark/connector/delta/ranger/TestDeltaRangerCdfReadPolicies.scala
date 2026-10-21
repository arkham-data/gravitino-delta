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

import java.security.PrivilegedExceptionAction
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.model.RangerValiditySchedule
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Current real Ranger decisions apply to every native image and original action obligation. */
class TestDeltaRangerCdfReadPolicies {
  @Test
  def scheduledSelectGrantCannotRefreshAnalyzedQuery(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val fixture = new DeltaRangerCdfReadFixture
    policy.withSpark(fixture.prepare) { spark =>
      val format = DateTimeFormatter
        .ofPattern(RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION)
        .withZone(ZoneOffset.UTC)
      val activation = Instant.now().plusSeconds(8)
      val schedule = new RangerValiditySchedule(
        format.format(activation),
        format.format(activation.plusSeconds(120)),
        "UTC",
        null
      )
      policy.publish(grantSchedule = Some(schedule))
      val subject = DeltaRangerQuerySubject.current(spark)
      val key = new DeltaRangerResourceKey("a", "db", "t", "id", "select", null)
      assertThrows(classOf[SecurityException], () => policy.engine.authorize(subject, key))
      val held = fixture.read(spark).select("id")
      held.queryExecution.analyzed
      val stamp = policy.engine.snapshotStamp()
      while (!Instant.now().isAfter(activation.plusSeconds(1))) Thread.sleep(100)
      policy.engine.requireSnapshot(stamp)
      policy.engine.authorize(subject, key)
      assertThrows(classOf[Exception], () => { held.collect(); () })
      assertEquals(
        fixture.multiset(fixture.expected.map(row => Row(row.getLong(0)))),
        fixture.multiset(fixture.read(spark).select("id").collect().toVector)
      )
    }
  }

  @Test
  def policiesApplyToEveryNativeImage(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    var start = 0L; var end = 0L; var expected = Vector.empty[Row]
    policy.withSpark { spark =>
      import DeltaRangerCdfReadFixture._
      enable(spark)
      append(
        spark,
        "SELECT 7L id, 'same' secret, 'US' region UNION ALL SELECT 8L, 'same', 'US' UNION ALL SELECT 9L, 'same', 'EU' UNION ALL SELECT 10L, 'same', 'EU'"
      )
      spark
        .sql(
          s"UPDATE delta.`$location` SET region = CASE WHEN id IN (7, 9) THEN 'US' ELSE 'EU' END, secret = 'changed' WHERE id >= 7"
        )
        .collect()
      start = head(spark); end = start
      expected = oracle(spark, start, end).collect().toVector
    } { spark =>
      import DeltaRangerCdfReadFixture._
      def read = DeltaRangerCdfReadFixture.read(spark, start, end)
      assertEquals(
        rowMultiset(expected),
        rowMultiset(read.collect().toVector)
      ) // Real wildcard grant.
      policy.publish(filter = Some("region = 'US'"))
      val visible = expected.filter(_.getString(2) == "US")
      assertEquals(
        Set(
          (7L, "update_preimage"),
          (7L, "update_postimage"),
          (8L, "update_preimage"),
          (9L, "update_postimage")
        ),
        visible.map(r => (r.getLong(0), r.getString(3))).toSet
      )
      assertEquals(rowMultiset(visible), rowMultiset(read.collect().toVector))
      policy.publish(filter = Some("region = 'US'"), masks = Map("id" -> "CAST(0 AS BIGINT)"))
      assertEquals(
        rowMultiset(visible.map(r => Row.fromSeq(0L +: r.toSeq.tail))),
        rowMultiset(read.collect().toVector)
      )
      Seq(
        ("_change_type", "'hidden'", "hidden"),
        ("_commit_version", "CAST(0 AS BIGINT)", 0L),
        (
          "_commit_timestamp",
          "TIMESTAMP '2000-01-01 00:00:00'",
          java.sql.Timestamp.valueOf("2000-01-01 00:00:00")
        )
      ).foreach { case (column, expression, value) =>
        policy.publish(denied = Set(column))
        assertThrows(classOf[Exception], () => { read.select(column).collect(); () })
        policy.publish(masks = Map(column -> expression))
        assertEquals(
          rowMultiset(expected.map(_ => Row(value))),
          rowMultiset(read.filter(s"$column = $expression").select(column).collect().toVector)
        )
      }
      policy.publish(
        denied = Set("secret"),
        filter = Some("_change_type = 'update_preimage' AND region = 'US'")
      )
      assertEquals(
        rowMultiset(
          expected
            .filter(r => r.getString(3) == "update_preimage" && r.getString(2) == "US")
            .map(r => Row(r.getString(3), r.getLong(4), r.getTimestamp(5)))
        ),
        rowMultiset(
          read.select("_change_type", "_commit_version", "_commit_timestamp").collect().toVector
        )
      )
      policy.publish(filter = Some("rand() > 0.5"))
      assertThrows(classOf[Exception], () => { read.collect(); () })
      policy.publish(masks = Map("_change_type" -> "sha2({col}, 256)"))
      assertThrows(classOf[Exception], () => { read.collect(); () })
    }
  }

  @Test
  def retainedActionRevalidatesOriginalObligations(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val fixture = new DeltaRangerCdfReadFixture
    policy.withSpark(fixture.prepare) { spark =>
      for (
        acted <- Seq(false, true);
        change <- Seq("grant", "revoke", "role", "filter", "mask", "subject", "configuration")
      ) {
        policy.roles(granted = true); policy.publish()
        val held = fixture.read(spark).selectExpr("id", "secret", "_change_type")
        held.queryExecution.analyzed
        if (acted) assertFalse(held.collect().isEmpty)
        change match {
          case "grant"         => policy.publish()
          case "revoke"        => policy.publish(denied = Set("secret"))
          case "role"          => policy.roles(granted = false)
          case "filter"        => policy.publish(filter = Some("region = 'US'"))
          case "mask"          => policy.publish(masks = Map("id" -> "CAST(0 AS BIGINT)"))
          case "configuration" => spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
          case "subject"       =>
          case _               => fail("unknown mutation")
        }
        def verify(): Unit = {
          assertThrows(classOf[Exception], () => { held.collect(); () }, s"$change acted=$acted")
          if (Set("revoke", "role", "subject")(change))
            assertThrows(classOf[Exception], () => { fixture.read(spark).collect(); () })
          else assertFalse(fixture.read(spark).collect().isEmpty)
        }
        if (change == "subject")
          UserGroupInformation
            .createUserForTesting("denied-user", Array("other"))
            .doAs(new PrivilegedExceptionAction[Unit] { override def run(): Unit = verify() })
        else verify()
        if (change == "configuration") spark.conf.unset("spark.sql.session.timeZone")
      }
      policy.roles(granted = true); policy.publish()
      val earlier = fixture.read(spark).select("id")
      earlier.queryExecution.analyzed
      policy.publish(masks = Map("id" -> "CAST(0 AS BIGINT)"))
      assertFalse(fixture.read(spark).select("id").collect().isEmpty)
      assertThrows(classOf[Exception], () => { earlier.collect(); () })
      policy.publish()
      val base = fixture.read(spark)
      val originalSibling = base.select("id")
      originalSibling.queryExecution.analyzed
      policy.publish(masks = Map("id" -> "CAST(0 AS BIGINT)"))
      val laterSibling = base.select("_commit_version")
      laterSibling.queryExecution.analyzed
      val stale = assertThrows(classOf[Exception], () => { originalSibling.collect(); () })
      assertTrue(stale.getMessage.contains("publication changed"))
      policy.publish()
      val empty = fixture.read(spark).select("secret").filter("false")
      assertEquals(0, empty.collect().length)
      policy.publish(denied = Set("secret"))
      assertThrows(classOf[Exception], () => { empty.collect(); () })
      assertThrows(
        classOf[Exception],
        () => {
          fixture.read(spark).filter("secret = 'unused' AND false").select("id").collect(); ()
        }
      )
    }
  }

  @Test
  def unchangedPublicationStillRechecksTimeEffectiveFilter(): Unit = {
    val policy = new DeltaRangerCdfPolicyFixture
    val fixture = new DeltaRangerCdfReadFixture
    policy.withSpark(fixture.prepare) { spark =>
      val format = DateTimeFormatter
        .ofPattern(RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION)
        .withZone(ZoneOffset.UTC)
      for (acted <- Seq(false, true)) {
        val deadline = Instant.now().plusSeconds(8)
        val schedule = new RangerValiditySchedule(
          format.format(Instant.now().minusSeconds(60)),
          format.format(deadline),
          "UTC",
          null
        )
        policy.publish(filter = Some("region = 'US'"), schedule = Some(schedule))
        val held = fixture.read(spark)
        held.queryExecution.analyzed
        if (acted)
          assertEquals(
            fixture.multiset(fixture.expected.filter(_.getString(2) == "US")),
            fixture.multiset(held.collect().toVector)
          )
        val stamp = policy.engine.snapshotStamp()
        while (!Instant.now().isAfter(deadline.plusSeconds(1))) Thread.sleep(100)
        policy.engine.requireSnapshot(stamp)
        assertThrows(classOf[Exception], () => { held.collect(); () }, s"time acted=$acted")
        assertEquals(
          fixture.multiset(fixture.expected),
          fixture.multiset(fixture.read(spark).collect().toVector)
        )
      }
    }
  }
}
