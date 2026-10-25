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

import java.util.{LinkedHashMap, Map => JMap}
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.actions.AddFile
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** SQL and DataFrame frontends must demand and publish the same governed native mutations. */
class TestGravitinoDeltaMergePolicies {
  import PartitionedMergeFixture.{bag, rows, files, messages}
  import GravitinoDeltaMergeTestFixture._
  import TestDeltaRangerMergeSource.{field, version}
  private val policies = new TestDeltaRangerMergePolicies

  @Test
  def hiddenSharedFileRowsAndOmittedOrExplicitMasksHaveExactCdf(): Unit = {
    Seq(false, true).foreach { explicit =>
      paired { (spark, plugin, api) =>
        assertEquals(
          1,
          PartitionedMergeFixture
            .actions(NativeSelectFixture.locations("b"), 0L)
            .count(_.isInstanceOf[AddFile])
        )
        policies.publish(
          plugin,
          filter = Some("region = 'keep'"),
          masks = Map("secret" -> "concat({col}, '-masked')")
        )
        val selected = if (explicit) Set("id", "secret") else Set("id")
        val writes = if (explicit) Set("secret", "region") else Set("region")
        val observed = demands(spark, selected, writes)
        val assignments = values("region" -> "'outside'")
        if (explicit) assignments.put("secret", "t.secret")
        execute(
          spark,
          api,
          "WHEN MATCHED THEN UPDATE SET region = 'outside'" + (if (explicit) ", secret = t.secret"
                                                               else ""),
          _.whenMatchedUpdate(assignments)
        )
        assertTrue(observed())
        policies.publish(plugin)
        val secret = if (explicit) "b-one-masked" else "b-one"
        assertEquals(
          bag(Vector(Row(1L, secret, "outside"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))),
          rows(spark)
        )
        assertEquals(
          bag(
            Vector(
              Row(1L, "b-one", "keep", "update_preimage"),
              Row(1L, secret, "outside", "update_postimage")
            )
          ),
          cdf(spark)
        )
        assertEquals(1L, version("b"))
      }
    }
  }

  @Test
  def literalColumnGrantNeedsNoSelectAndMayLeaveVisibility(): Unit = paired {
    (spark, plugin, api) =>
      policies.publish(
        plugin,
        selected = Seq("id"),
        updated = Seq("region"),
        deniedSelect = Seq("secret", "region"),
        deniedUpdate = Seq("secret"),
        filter = Some("region = 'keep'"),
        masks = Map("region" -> "concat({col}, '-masked')")
      )
      val observed = demands(spark, Set("id"), Set("region"))
      execute(
        spark,
        api,
        "WHEN MATCHED THEN UPDATE SET region = 'outside'",
        _.whenMatchedUpdate(values("region" -> "'outside'"))
      )
      assertTrue(observed())
      policies.publish(plugin)
      assertEquals(
        bag(Vector(Row(1L, "b-one", "outside"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))),
        rows(spark)
      )
      assertEquals(
        bag(
          Vector(
            Row(1L, "b-one", "keep", "update_preimage"),
            Row(1L, "b-one", "outside", "update_postimage")
          )
        ),
        cdf(spark)
      )
  }

  @Test
  def hiddenKeyInsertsWithoutPostimageFilter(): Unit = paired { (spark, plugin, api) =>
    policies.publish(plugin, filter = Some("region = 'drop'"))
    val observed = demands(spark, Set("id"), all)
    execute(
      spark,
      api,
      "WHEN MATCHED THEN UPDATE SET region = 'outside' " + PartitionedMergeFixture.insert,
      _.whenMatchedUpdate(values("region" -> "'outside'")).whenNotMatchedInsert(insertValues)
    )
    assertTrue(observed())
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
    assertEquals(bag(Vector(Row(1L, "changed", "keep", "insert"))), cdf(spark))
  }

  @Test
  def sourceAndTargetMaskedKeysUseGovernedMultiplicity(): Unit = paired { (spark, plugin, api) =>
    policies.publish(
      plugin,
      masks = Map("id" -> "CAST(1 AS BIGINT)"),
      sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
    )
    val observed = demands(spark, Set("id"), Set("region"))
    execute(
      spark,
      api,
      "WHEN MATCHED THEN UPDATE SET region = 'all'",
      _.whenMatchedUpdate(values("region" -> "'all'"))
    )
    assertTrue(observed())
    policies.publish(plugin)
    val before = Vector(Row(1L, "b-one", "keep"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))
    val after = before.map(r => Row(r.getLong(0), r.get(1), "all"))
    assertEquals(bag(after), rows(spark))
    assertEquals(
      bag(
        before.map(r => Row.fromSeq(r.toSeq :+ "update_preimage")) ++ after.map(r =>
          Row.fromSeq(r.toSeq :+ "update_postimage")
        )
      ),
      cdf(spark)
    )
  }

  @Test
  def originalDemandsSurviveWildcardDeniesUnreachableAndEmptySources(): Unit = paired {
    (spark, plugin, api) =>
      val before = rows(spark); val beforeFiles = files()
      val cases
          : Vector[(String, GravitinoDeltaMergeBuilder => GravitinoDeltaMergeBuilder, Boolean)] =
        Vector(
          (
            "WHEN MATCHED THEN UPDATE SET secret = 'denied'",
            _.whenMatchedUpdate(values("secret" -> "'denied'")),
            false
          ),
          (
            "WHEN MATCHED THEN UPDATE SET secret = t.secret",
            _.whenMatchedUpdate(values("secret" -> "t.secret")),
            false
          ),
          (
            "WHEN MATCHED AND false THEN UPDATE SET secret = 'denied' WHEN MATCHED THEN UPDATE SET region = 'ok'",
            _.whenMatchedUpdate("false", values("secret" -> "'denied'"))
              .whenMatchedUpdate(values("region" -> "'ok'")),
            false
          ),
          ("WHEN MATCHED THEN UPDATE SET *", _.whenMatchedUpdateAll(), false),
          ("WHEN MATCHED THEN DELETE", _.whenMatchedDelete(), false),
          (
            "WHEN MATCHED THEN UPDATE SET region = 'ok' WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
            _.whenMatchedUpdate(values("region" -> "'ok'"))
              .whenNotMatchedInsert(values("id" -> "s.id")),
            false
          ),
          (
            "WHEN MATCHED AND false THEN UPDATE SET region = t.secret WHEN MATCHED THEN UPDATE SET region = 'ok'",
            _.whenMatchedUpdate("false", values("region" -> "t.secret"))
              .whenMatchedUpdate(values("region" -> "'ok'")),
            true
          ),
          (
            "WHEN MATCHED AND t.secret = 'b-one' THEN UPDATE SET region = 'ok'",
            _.whenMatchedUpdate("t.secret = 'b-one'", values("region" -> "'ok'")),
            true
          )
        )
      cases.foreach { case (sql, build, select) =>
        Seq(false, true).foreach { empty =>
          if (select) policies.publish(plugin, deniedSelect = Seq("secret"))
          else policies.publish(plugin, deniedUpdate = Seq("secret"))
          var checkpoint = 3L
          val error = assertThrows(
            classOf[Exception],
            () => { execute(spark, api, sql, build, empty = empty); checkpoint = 4L }
          )
          assertTrue(messages(error).contains("Ranger"), messages(error))
          assertEquals(3L, checkpoint); assertEquals(0L, version("b"));
          assertEquals(beforeFiles, files())
        }
      }
      policies.publish(plugin); assertEquals(before, rows(spark))
  }

  @Test
  def starsUseRealSourceColumnsAndNoSynthesizedTargetSelect(): Unit = paired {
    (spark, plugin, api) =>
      policies.publish(
        plugin,
        selected = Seq("id"),
        deniedSelect = Seq("secret", "region"),
        sourceMasks = Map("secret" -> "concat({col}, '-source')")
      )
      val observed = demands(spark, Set("id"), all)
      execute(
        spark,
        api,
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        _.whenMatchedUpdateAll().whenNotMatchedInsertAll()
      )
      assertTrue(observed())
      policies.publish(plugin)
      assertEquals(
        bag(
          Vector(Row(1L, "changed-source", "keep"), Row(2L, "b-two", "drop"), Row(3L, null, "keep"))
        ),
        rows(spark)
      )
      assertEquals(
        bag(
          Vector(
            Row(1L, "b-one", "keep", "update_preimage"),
            Row(1L, "changed-source", "keep", "update_postimage")
          )
        ),
        cdf(spark)
      )
  }

  @Test
  def nativeMultipleSourceAmbiguityLeavesStateAndCheckpoint(): Unit = paired {
    (spark, plugin, api) =>
      val before = rows(spark)
      policies.publish(
        plugin,
        masks = Map("id" -> "CAST(1 AS BIGINT)"),
        sourceMasks = Map("id" -> "CAST(1 AS BIGINT)")
      )
      var checkpoint = 3L
      val failure = assertThrows(
        classOf[Exception],
        () => {
          execute(
            spark,
            api,
            "WHEN MATCHED THEN UPDATE SET region = 'ambiguous'",
            _.whenMatchedUpdate(values("region" -> "'ambiguous'")),
            changes = "_change_type <> 'update_preimage'"
          ); checkpoint = 4L
        }
      )
      assertTrue(
        messages(failure).contains("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"),
        messages(failure)
      )
      assertEquals(3L, checkpoint); assertEquals(0L, version("b"))
      policies.publish(plugin); assertEquals(before, rows(spark))
  }

  private def paired(test: (SparkSession, RangerBasePlugin, Boolean) => Unit): Unit =
    Seq(false, true).foreach { api =>
      PartitionedMergeFixture.fixture(
        (spark, plugin) => test(spark, plugin, api),
        prepare = spark => PartitionedMergeFixture.reset(spark, keys = Nil)
      )
    }

  private def execute(
      spark: SparkSession,
      api: Boolean,
      clause: String,
      build: GravitinoDeltaMergeBuilder => GravitinoDeltaMergeBuilder,
      empty: Boolean = false,
      changes: String = "_change_type = 'update_postimage'"
  ): Unit = {
    val predicate =
      (if (empty) "_commit_version = -1" else "_commit_version = 4") + " AND " + changes
    if (api) build(builder(spark, source(spark, predicate))).execute()
    else
      spark
        .sql(
          s"MERGE INTO b.db.t t USING (SELECT * FROM table_changes('a.db.t', 0, 4) WHERE $predicate) s ON t.id = s.id $clause"
        )
        .collect()
  }

  private def demands(
      spark: SparkSession,
      selected: Set[String],
      writes: Set[String]
  ): () => Boolean = {
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var observed = false
    registration.onRead = n => {
      refresh(n)
      val writer = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (writer != null) {
        val receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
        assertEquals(selected, field[Set[String]](receipt, "selected"))
        assertEquals(writes, field[Set[String]](receipt, "writes"))
        assertEquals(
          all ++ Set("_change_type", "_commit_version", "_commit_timestamp"),
          field[Set[String]](receipt.cdf, "columns")
        )
        observed = true
      }
    }
    () => observed
  }
}

/** Test-only caller construction shared by policy and lifecycle qualification. */
private[ranger] object GravitinoDeltaMergeTestFixture {
  val all: Set[String] = Set("id", "secret", "region")
  def values(entries: (String, Any)*): JMap[String, Any] = {
    val result = new LinkedHashMap[String, Any]()
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }
  def insertValues: JMap[String, Any] =
    values("id" -> "s.id", "secret" -> "s.secret", "region" -> "s.region")
  def source(
      spark: SparkSession,
      predicate: String = "_commit_version = 4 AND _change_type <> 'update_preimage'"
  ): DataFrame =
    spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", "0")
      .option("endingVersion", "4")
      .table("a.db.t")
      .where(predicate)
      .alias("s")
  def builder(
      spark: SparkSession,
      input: DataFrame,
      target: String = "b"
  ): GravitinoDeltaMergeBuilder =
    GravitinoDeltaTable.forName(spark, target + ".db.t").alias("t").merge(input, "t.id = s.id")
  def merge(
      spark: SparkSession,
      target: String = "b",
      region: String = "s.region"
  ): GravitinoDeltaMergeBuilder =
    builder(spark, source(spark), target)
      .whenMatchedDelete("s._change_type = 'delete'")
      .whenMatchedUpdate(values("secret" -> "s.secret", "region" -> region))
      .whenNotMatchedInsert("s._change_type <> 'delete'", insertValues)
  def cdf(spark: SparkSession, target: String = "b", version: Long = 1L): Map[Row, Int] =
    PartitionedMergeFixture.bag(
      spark
        .sql(
          s"SELECT id, secret, region, _change_type FROM table_changes('$target.db.t', $version, $version)"
        )
        .collect()
        .toVector
    )
}
