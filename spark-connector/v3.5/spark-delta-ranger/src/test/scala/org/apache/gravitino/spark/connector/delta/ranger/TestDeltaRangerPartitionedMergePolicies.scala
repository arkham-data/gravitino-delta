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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Partition predicates and writes retain original Ranger column demands. */
class TestDeltaRangerPartitionedMergePolicies {
  import PartitionedMergeFixture._
  import TestDeltaRangerMergeSource.version
  private val policies = new TestDeltaRangerMergePolicies

  @Test
  def maskedPartitionPredicatesAndOmittedFieldsUseGovernedAndRawValues(): Unit = fixture {
    (spark, plugin) =>
      policies.publish(plugin, masks = Map("region" -> "concat({col}, '-masked')"))
      val statement = merge(
        "WHEN MATCHED AND t.region = 'keep-masked' THEN UPDATE SET secret = 'changed'",
        "t.id = s.id AND t.region = 'keep-masked'"
      )
      assertEquals(Vector(Row(1L, 1L, 0L, 0L)), spark.sql(statement).collect().toVector)
      policies.publish(plugin)
      assertEquals(
        bag(Vector(Row(1L, "changed", "keep"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))),
        rows(spark)
      )
      assertEquals(1L, version("b"))
      policies.publish(plugin, masks = Map("region" -> "concat({col}, '-masked')"))
      spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = t.region")).collect()
      policies.publish(plugin)
      assertEquals(
        bag(
          Vector(Row(1L, "changed", "keep-masked"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))
        ),
        rows(spark)
      )
      assertEquals(2L, version("b"))
  }

  @Test
  def literalPartitionUpdateNeedsNoSelectAndMayLeaveTargetFilter(): Unit = fixture {
    (spark, plugin) =>
      policies.publish(
        plugin,
        selected = Seq("id"),
        updated = Seq("region"),
        deniedSelect = Seq("region", "secret"),
        deniedUpdate = Seq("secret"),
        filter = Some("region = 'keep'"),
        masks = Map("region" -> "concat({col}, '-masked')")
      )
      assertEquals(
        Vector(Row(1L, 1L, 0L, 0L)),
        spark.sql(merge("WHEN MATCHED THEN UPDATE SET region = 'outside'")).collect().toVector
      )
      policies.publish(plugin)
      assertEquals(
        bag(Vector(Row(1L, "b-one", "outside"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))),
        rows(spark)
      )
      assertEquals(1L, version("b"))
  }

  @Test
  def hiddenPartitionKeyCollisionInsertsWithoutChangingHiddenRow(): Unit = fixture {
    (spark, plugin) =>
      policies.publish(plugin, filter = Some("region = 'drop'"))
      assertEquals(
        Vector(Row(1L, 0L, 0L, 1L)),
        spark
          .sql(merge("WHEN MATCHED THEN UPDATE SET region = 'outside' " + insert))
          .collect()
          .toVector
      )
      policies.publish(plugin)
      assertEquals(
        bag(
          Vector(
            Row(1L, "b-one", "keep"),
            Row(1L, "changed", "keep"),
            Row(2L, "b-two", "drop"),
            Row(3L, null, "keep")
          )
        ),
        rows(spark)
      )
      assertEquals(1L, version("b"))
  }

  @Test
  def originalPartitionDemandsSurviveEmptySourcesAndUnreachableClauses(): Unit = fixture {
    (spark, plugin) =>
      val before = rows(spark); val beforeFiles = files()
      var checkpoint = 3L
      def denied(statement: String): Unit = {
        val failure = assertThrows(
          classOf[SecurityException],
          () => { spark.sql(statement).collect(); checkpoint = 4L }
        )
        assertTrue(messages(failure).contains("Ranger"), messages(failure))
        assertEquals(3L, checkpoint); assertEquals(0L, version("b"));
        assertEquals(beforeFiles, files())
      }
      policies.publish(plugin, deniedUpdate = Seq("region"))
      Seq(
        "WHEN MATCHED THEN UPDATE SET region = 'denied'",
        "WHEN MATCHED AND false THEN UPDATE SET region = 'denied' WHEN MATCHED THEN UPDATE SET secret = 'ok'",
        "WHEN MATCHED THEN DELETE",
        "WHEN MATCHED THEN UPDATE SET secret = 'ok' WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"
      )
        .foreach(c => {
          denied(merge(c)); denied(merge(c).replace("_commit_version = 4", "_commit_version = -1"))
        })
      policies.publish(plugin, deniedSelect = Seq("region"))
      Seq(
        "WHEN MATCHED THEN UPDATE SET secret = t.region",
        "WHEN MATCHED AND false THEN UPDATE SET secret = t.region WHEN MATCHED THEN UPDATE SET secret = 'ok'",
        "WHEN MATCHED AND t.region = 'keep' THEN UPDATE SET secret = 'ok'"
      )
        .foreach(c => {
          denied(merge(c)); denied(merge(c).replace("_commit_version = 4", "_commit_version = -1"))
        })
      denied(
        merge("WHEN MATCHED THEN UPDATE SET secret = 'ok'", "t.id = s.id AND t.region = s.region")
      )
      policies.publish(plugin)
      assertEquals(before, rows(spark))
      assertEquals(
        Vector(Row(0L, 0L, 0L, 0L)),
        spark
          .sql(
            merge("WHEN MATCHED THEN UPDATE SET region = 'unused'")
              .replace("_commit_version = 4", "_commit_version = -1")
          )
          .collect()
          .toVector
      )
      checkpoint = 4L
      assertEquals(4L, checkpoint); assertEquals(0L, version("b"));
      assertEquals(before, rows(spark))
  }

  @Test
  def nativeAmbiguityRejectsAndUnconditionalDeleteKeepsNativeMultiplicity(): Unit = fixture {
    (spark, plugin) =>
      val before = rows(spark)
      policies.publish(
        plugin,
        masks = Map("id" -> "CAST(1 AS BIGINT)"),
        sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
      )
      val statement = merge("WHEN MATCHED THEN UPDATE SET region = 'ambiguous' " + insert)
        .replace("_change_type = 'update_postimage'", "_change_type <> 'update_preimage'")
      var checkpoint = 3L
      val failure =
        assertThrows(classOf[Exception], () => { spark.sql(statement).collect(); checkpoint = 4L })
      assertTrue(
        messages(failure).contains("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"),
        messages(failure)
      )
      assertEquals(3L, checkpoint); assertEquals(0L, version("b"))
      policies.publish(plugin); assertEquals(before, rows(spark))
      policies.publish(
        plugin,
        masks = Map("id" -> "CAST(1 AS BIGINT)"),
        sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
      )
      assertEquals(
        Vector(Row(3L, 0L, 3L, 0L)),
        spark.sql(statement.replace("UPDATE SET region = 'ambiguous'", "DELETE")).collect().toVector
      )
      policies.publish(plugin); assertTrue(rows(spark).isEmpty); assertEquals(1L, version("b"))
  }
}
