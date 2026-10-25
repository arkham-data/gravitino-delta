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
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeInto, SubqueryAlias}
import org.apache.spark.sql.execution.ProjectExec
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.{col, lit, rand, udf}
import java.util.{LinkedHashMap, Map => JMap}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Installed DataFrame frontend must retain owned named MERGE semantics. */
class TestGravitinoDeltaMergeBuilder {
  @Test
  def namedDataFrameUsesOwnedMerge(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      PartitionedMergeFixture.origins(spark)
      assertEquals(1, GravitinoDeltaTable.apiVersion())
      LoggerFactory
        .getLogger(getClass)
        .warn(
          "DataFrame API origin: " + classOf[
            GravitinoDeltaMergeBuilder
          ].getProtectionDomain.getCodeSource.getLocation
        )
      val before = version(spark)
      merge(spark, source(spark)).execute()
      assertEquals(before + 1L, version(spark))
      assertEquals(
        Map(
          Row(1L, "changed", "keep") -> 1,
          Row(3L, null, "keep") -> 1,
          Row(4L, "new", "keep") -> 1
        ),
        PartitionedMergeFixture.rows(spark)
      )
      assertTrue(
        PartitionedMergeFixture
          .actions(NativeSelectFixture.locations("b"), before + 1L)
          .exists(_.json.contains("\"operation\":\"MERGE\""))
      )
    }

  @Test
  def stringsAndBoundColumnsPreserveAuthoredAssignments(): Unit = {
    Seq(false, true).foreach { bound =>
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
        val input =
          source(spark).select("id", "secret", "region", "_change_type").where("id > 0").alias("s")
        val verified = verifyOwnedSource(spark, input)
        val table = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
        val builder =
          if (bound) table.merge(input, col("t.id") === input("id"))
          else table.merge(input, "t.id = s.id")
        if (bound) builder.whenMatchedDelete(input("_change_type") === lit("delete"))
        else builder.whenMatchedDelete("s._change_type = 'delete'")
        builder
          .whenMatchedUpdate(
            assignments(
              "secret" -> (if (bound) input("secret") else "s.secret"),
              "region" -> "s.region"
            )
          )
          .whenNotMatchedInsert(
            col("s._change_type") =!= lit("delete"),
            assignments("id" -> input("id"), "secret" -> "s.secret", "region" -> input("region"))
          )
          .execute()
        assertTrue(
          verified(),
          "exact supplied source and physical-child rejection probes did not run"
        )
        assertEquals(1L, version(spark))
        assertEquals(expected, PartitionedMergeFixture.rows(spark))
      }
    }
    Seq(false, true).foreach { explicit =>
      PartitionedMergeFixture.fixture({ (spark, plugin) =>
        val policies = new TestDeltaRangerMergePolicies
        policies.publish(plugin, masks = Map("secret" -> "concat({col}, '-masked')"))
        val input = source(spark).where("_change_type = 'update_postimage'")
        val values = assignments("region" -> "'changed'")
        if (explicit) values.put("secret", col("t.secret"))
        GravitinoDeltaTable
          .forName(spark, "b.db.t")
          .alias("t")
          .merge(input, col("t.id") === input("id"))
          .whenMatchedUpdate(values)
          .execute()
        policies.publish(plugin)
        assertEquals(1L, version(spark))
        assertEquals(
          Map(
            Row(1L, if (explicit) "b-one-masked" else "b-one", "changed") -> 1,
            Row(2L, "b-two", "drop") -> 1,
            Row(3L, null, "keep") -> 1
          ),
          PartitionedMergeFixture.rows(spark)
        )
      })
    }
  }

  @Test
  def rejectsForeignColumnsAndContexts(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val input = source(spark)
      val foreign = source(spark)
      val ancestor = source(spark)
      val derived = ancestor.select("id", "secret", "region", "_change_type").alias("s")
      val table = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
      reject(spark) {
        table.merge(input, col("t.id") === foreign("id"))
      }
      reject(spark) {
        table.merge(derived, col("t.id") === ancestor("id"))
      }
      reject(spark) {
        table
          .merge(input, "t.id = s.id")
          .whenMatchedUpdate(assignments("secret" -> foreign("secret")))
      }
      val changed = table.merge(input, "t.id = s.id").whenMatchedDelete()
      val key = "spark.sql.caseSensitive"
      val original = spark.sessionState.conf.getAllConfs.get(key)
      spark.conf.set(key, (!spark.conf.get(key).toBoolean).toString)
      reject(spark) { changed.whenMatchedUpdate(assignments("region" -> "'foreign settings'")) }
      reject(spark) { changed.execute() }
      original match {
        case Some(value) => spark.conf.set(key, value)
        case None        => spark.conf.unset(key)
      }
      val subject = table.merge(input, "t.id = s.id").whenMatchedDelete()
      UserGroupInformation
        .createUserForTesting("foreign-builder-user", Array("foreign"))
        .doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            assertThrows(classOf[SecurityException], () => subject.execute()); ()
          }
        })
      val session = table.merge(input, "t.id = s.id").whenMatchedDelete()
      val other = spark.newSession()
      // Apply this fixture's installed extension declaration and forwarding catalog settings
      // to the independent session before constructing its named source.
      NativeSelectFixture.rule(other, NativeSelectFixture.policy.get)
      val foreignSessionSource = other.table("b.db.t")
      reject(spark) { table.merge(foreignSessionSource, "t.id = id") }
      try {
        SparkSession.setActiveSession(other)
        assertThrows(classOf[SecurityException], () => session.execute())
        assertThrows(classOf[SecurityException], () => table.merge(input, "t.id = s.id"))
      } finally SparkSession.setActiveSession(spark)
      assertEquals(0L, version(spark))
    }

  @Test
  def assignmentMapsAreSnapshots(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val input = source(spark)
      val updates = assignments("secret" -> "s.secret", "region" -> "s.region")
      val inserts = assignments("id" -> "s.id", "secret" -> "s.secret", "region" -> "s.region")
      val builder = GravitinoDeltaTable
        .forName(spark, "b.db.t")
        .alias("t")
        .merge(input, "t.id = s.id")
        .whenMatchedDelete("s._change_type = 'delete'")
        .whenMatchedUpdate(updates)
        .whenNotMatchedInsert("s._change_type <> 'delete'", inserts)
      updates.clear(); updates.put("id", "999L")
      inserts.clear(); inserts.put("secret", "'wrong'")
      builder.execute()
      assertEquals(1L, version(spark))
      assertEquals(expected, PartitionedMergeFixture.rows(spark))
    }

  @Test
  def invalidClauseDoesNotMutateBuilder(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val input = source(spark)
      val builder = GravitinoDeltaTable
        .forName(spark, "b.db.t")
        .alias("t")
        .merge(input, "t.id = s.id")
      Seq(
        assignments(),
        assignments("secret" -> null),
        assignments("secret" -> Integer.valueOf(1)),
        assignments("secret" -> "s.secret", "region" -> null)
      ).foreach { values =>
        assertThrows(classOf[IllegalArgumentException], () => builder.whenMatchedUpdate(values))
        assertThrows(classOf[IllegalArgumentException], () => builder.whenNotMatchedInsert(values))
      }
      assertThrows(classOf[IllegalArgumentException], () => builder.whenMatchedUpdate(null))
      assertThrows(classOf[IllegalArgumentException], () => builder.whenMatchedDelete(null: String))
      assertThrows(
        classOf[IllegalArgumentException],
        () =>
          builder.whenNotMatchedInsert(
            null: Column,
            assignments("id" -> "s.id")
          )
      )
      builder
        .whenMatchedDelete("s._change_type = 'delete'")
        .whenMatchedUpdate(assignments("secret" -> "s.secret", "region" -> "s.region"))
        .whenNotMatchedInsert(
          "s._change_type <> 'delete'",
          assignments("id" -> "s.id", "secret" -> "s.secret", "region" -> "s.region")
        )
        .execute()
      assertEquals(1L, version(spark)); assertEquals(expected, PartitionedMergeFixture.rows(spark))
    }

  @Test
  def identifiersAndDuplicateAssignmentsUseSessionParser(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      assertFalse(spark.sessionState.conf.caseSensitiveAnalysis)
      Seq("t", "db.t", "delta.`/tmp/foreign`", "b.db.t.extra").foreach { name =>
        reject(spark) { GravitinoDeltaTable.forName(spark, name) }
      }
      val table = GravitinoDeltaTable.forName(spark, "`b`.`db`.`t`")
      assertThrows(classOf[IllegalArgumentException], () => table.alias("two.parts"))
      val builder = table.alias("`target``alias`").merge(source(spark), "`target``alias`.id = s.id")
      assertThrows(
        classOf[IllegalArgumentException],
        () => builder.whenMatchedUpdate(assignments("secret" -> "s.secret", "SECRET" -> "s.secret"))
      )
      assertThrows(
        classOf[IllegalArgumentException],
        () => builder.whenMatchedUpdate(assignments("secret.nested" -> "s.secret"))
      )
      builder
        .whenMatchedUpdate(assignments("`secret`" -> "concat('it', '\\'', 's `quoted`')"))
        .execute()
      assertEquals(1L, version(spark))
      assertEquals(
        Map(
          Row(1L, "it's `quoted`", "keep") -> 1,
          Row(2L, "it's `quoted`", "drop") -> 1,
          Row(3L, null, "keep") -> 1
        ),
        PartitionedMergeFixture.rows(spark)
      )
    }

  @Test
  def executeIsSingleUse(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val input = source(spark)
      val builder = merge(spark, input)
      builder.execute()
      reject(spark) { builder.execute() }
      reject(spark) { merge(spark, input).execute() }
      assertThrows(classOf[IllegalStateException], () => builder.whenMatchedDelete())
      val failed = GravitinoDeltaTable
        .forName(spark, "b.db.t")
        .alias("t")
        .merge(source(spark), "t.missing = s.id")
        .whenMatchedDelete()
      reject(spark) { failed.execute() }
      assertThrows(classOf[IllegalStateException], () => failed.execute())
      assertEquals(1L, version(spark)); assertEquals(expected, PartitionedMergeFixture.rows(spark))
    }

  @Test
  def orderedStarsUseNativeActions(): Unit = {
    (0 to 2).foreach { mode =>
      PartitionedMergeFixture.fixture({ (spark, _) =>
        val input = source(spark)
        val table = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
        val builder = table
          .merge(input, col("t.id") === input("id"))
          .whenMatchedDelete(col("s._change_type") === lit("delete"))
        mode match {
          case 0 => builder.whenMatchedUpdateAll().whenNotMatchedInsertAll()
          case 1 => builder.whenMatchedUpdateAll("s.id > 0").whenNotMatchedInsertAll("s.id > 0")
          case 2 =>
            builder
              .whenMatchedUpdateAll(input("id") > lit(0))
              .whenNotMatchedInsertAll(input("id") > lit(0))
        }
        // Alias calls on another handle cannot alter a builder already constructed from table.
        table.alias("later")
        builder.execute()
        assertEquals(1L, version(spark));
        assertEquals(expected, PartitionedMergeFixture.rows(spark))
      })
    }
  }

  @Test
  def unsupportedSourcesAndExpressionsRejectWithoutMutation(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      GravitinoDeltaTable.forName(spark, "b.db.t")
      val settings = spark.sessionState.conf.getAllConfs
      def target = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
      Seq("t.missing = s.id", "id = id", "t.id = (SELECT 1L)", "rand() > 0.5").foreach {
        predicate =>
          reject(spark) { target.merge(source(spark), predicate).whenMatchedDelete().execute() }
      }
      reject(spark) { target.merge(source(spark), rand() > lit(0)).whenMatchedDelete().execute() }
      val unsupported = udf((id: Long) => id)
      reject(spark) {
        val input = source(spark);
        target.merge(input, col("t.id") === unsupported(input("id"))).whenMatchedDelete().execute()
      }
      reject(spark) {
        target
          .merge(source(spark).join(source(spark), Seq("id")), "t.id = s.id")
          .whenMatchedDelete()
          .execute()
      }
      reject(spark) {
        target
          .merge(source(spark).groupBy("id").count(), "t.id = s.id")
          .whenMatchedDelete()
          .execute()
      }
      reject(spark) {
        target
          .merge(
            source(spark).selectExpr("*", "row_number() OVER (ORDER BY id) AS rn"),
            "t.id = s.id"
          )
          .whenMatchedDelete()
          .execute()
      }
      reject(spark) {
        target
          .merge(
            spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", 0)
              .option("endingVersion", 4)
              .load(NativeSelectFixture.locations("a")),
            "t.id = s.id"
          )
          .whenMatchedDelete()
          .execute()
      }
      reject(spark) {
        target
          .merge(spark.readStream.format("rate").load(), "t.id = s.value")
          .whenMatchedDelete()
          .execute()
      }
      reject(spark) {
        val input = source(spark); input.cache();
        target.merge(input, "t.id = s.id").whenMatchedDelete().execute()
      }
      reject(spark) {
        val input = source(spark);
        target.merge(input.localCheckpoint(), "t.id = s.id").whenMatchedDelete().execute()
      }
      val actioned = source(spark)
      actioned.queryExecution.toRdd.foreachPartition(_ => ())
      reject(spark) { target.merge(actioned, "t.id = s.id").whenMatchedDelete().execute() }
      assertEquals(settings, spark.sessionState.conf.getAllConfs)
      val beforeAdaptiveVersion = version(spark)
      val beforeAdaptiveRows = PartitionedMergeFixture.rows(spark)
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      val adaptive = spark.sessionState.conf.getAllConfs
      assertThrows(
        classOf[Exception],
        () => target.merge(source(spark), "t.id = s.id").whenMatchedDelete().execute()
      )
      assertEquals(adaptive, spark.sessionState.conf.getAllConfs)
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      assertEquals(beforeAdaptiveVersion, version(spark))
      assertEquals(beforeAdaptiveRows, PartitionedMergeFixture.rows(spark))
      assertEquals(0L, version(spark))
    }

  @Test
  def conditionalAssignmentsRetainClauseOrder(): Unit = {
    Seq(false, true).foreach { bound =>
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
        val input = source(spark)
        val builder = GravitinoDeltaTable
          .forName(spark, "b.db.t")
          .alias("t")
          .merge(input, "t.id = s.id")
          .whenMatchedDelete("s._change_type = 'delete'")
        val update = assignments("secret" -> "'first update'", "region" -> "s.region")
        val insert =
          assignments("id" -> "s.id", "secret" -> "'first insert'", "region" -> "s.region")
        if (bound)
          builder
            .whenMatchedUpdate(input("id") === lit(1), update)
            .whenNotMatchedInsert(input("id") === lit(4), insert)
        else builder.whenMatchedUpdate("s.id = 1", update).whenNotMatchedInsert("s.id = 4", insert)
        builder
          .whenMatchedUpdate(assignments("secret" -> "'wrong update'"))
          .whenNotMatchedInsert(
            assignments("id" -> "s.id", "secret" -> "'wrong insert'", "region" -> "s.region")
          )
          .execute()
        assertEquals(1L, version(spark))
        assertEquals(
          Map(
            Row(1L, "first update", "keep") -> 1,
            Row(3L, null, "keep") -> 1,
            Row(4L, "first insert", "keep") -> 1
          ),
          PartitionedMergeFixture.rows(spark)
        )
      }
    }
  }

  @Test
  def unconditionalDeleteAndInsertUseNativeActions(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val input = source(spark)
      GravitinoDeltaTable
        .forName(spark, "b.db.t")
        .alias("t")
        .merge(input, "t.id = s.id")
        .whenMatchedDelete()
        .whenNotMatchedInsert(
          assignments("id" -> "s.id", "secret" -> "s.secret", "region" -> "s.region")
        )
        .execute()
      assertEquals(1L, version(spark))
      assertEquals(
        Map(Row(3L, null, "keep") -> 1, Row(4L, "new", "keep") -> 1),
        PartitionedMergeFixture.rows(spark)
      )
    }

  @Test
  def unnamedSourcesCannotBorrowNativeDeltaExecution(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val input = spark.range(1).toDF("id")
      val table = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
      val before = version(spark)
      val rows = PartitionedMergeFixture.rows(spark)
      assertThrows(classOf[SecurityException], () => table.merge(input, "t.id = id"))
      assertEquals(before, version(spark))
      assertEquals(rows, PartitionedMergeFixture.rows(spark))
    }

  @Test
  def unownedAnalyzedCommandCannotExecute(): Unit =
    NativeSelectFixture.withSpark { spark =>
      val input = spark.range(1, 2).toDF("id").alias("s")
      val analyzed = input.queryExecution.analyzed
      val expressions = new DeltaRangerMergeBuilderExpressions(spark, input, analyzed)
      val builder = new GravitinoDeltaMergeBuilder(
        new GravitinoDeltaTable.Context(spark),
        SubqueryAlias("t", UnresolvedRelation(Seq("b", "db", "t"))),
        analyzed,
        expressions,
        expressions.expression("t.id = s.id")
      )
        .whenMatchedDelete()
      val before = version(spark)
      val rows = PartitionedMergeFixture.rows(spark)
      val failure = assertThrows(classOf[SecurityException], () => builder.execute())
      assertTrue(failure.getMessage.contains("issued owned command"), failure.toString)
      assertEquals(before, version(spark))
      assertEquals(rows, PartitionedMergeFixture.rows(spark))
      assertThrows(classOf[IllegalStateException], () => builder.execute())
    }

  @Test
  def coldFactoryCapturesSettingsBeforeSourceSetup(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val before = spark.sessionState.conf.getAllConfs
      val table = GravitinoDeltaTable.forName(spark, "b.db.t").alias("t")
      val captured = spark.sessionState.conf.getAllConfs
      val input = source(spark)
      val after = spark.sessionState.conf.getAllConfs
      val initialized = (before.keySet ++ captured.keySet)
        .filter(k => before.get(k) != captured.get(k))
        .toVector
        .sorted
      LoggerFactory
        .getLogger(getClass)
        .warn("DataFrame factory native analyzer initialization keys: " + initialized.mkString(","))
      assertEquals(
        captured,
        after,
        "source creation must not invalidate the factory's initialized context"
      )
      table
        .merge(input, "t.id = s.id")
        .whenMatchedDelete("s._change_type = 'delete'")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
      assertEquals(1L, version(spark))
      assertEquals(expected, PartitionedMergeFixture.rows(spark))
    }

  private def verifyOwnedSource(spark: SparkSession, input: DataFrame): () => Boolean = {
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    val analyzed = input.queryExecution.analyzed
    var inspected = false
    registration.onRead = n => {
      refresh(n)
      val attempt =
        field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
      if (attempt != null && !inspected) {
        val root = field[DeltaRangerMergeSource.SourceExec](attempt, "root")
        if (root != null) {
          inspected = true
          val receipt = field[DeltaRangerMergeReceipt](attempt, "receipt")
          assertSame(analyzed, field[DeltaMergeInto](receipt, "original").source)
          assertThrows(
            classOf[SecurityException],
            () => root.withNewChildren(Seq(ProjectExec(root.child.output, root.child)))
          )
          assertThrows(
            classOf[SecurityException],
            () => root.withNewChildren(Seq(ProjectExec(root.child.output.reverse, root.child)))
          )
        }
      }
    }
    () => inspected
  }

  private def field[A](value: AnyRef, name: String): A = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name))
      .get
    member.setAccessible(true)
    member.get(value).asInstanceOf[A]
  }

  private val expected =
    Map(Row(1L, "changed", "keep") -> 1, Row(3L, null, "keep") -> 1, Row(4L, "new", "keep") -> 1)

  private def reject(spark: SparkSession)(body: => Any): Unit = {
    val before = version(spark)
    val rows = PartitionedMergeFixture.rows(spark)
    assertThrows(classOf[Exception], () => { body; () })
    assertEquals(before, version(spark))
    assertEquals(rows, PartitionedMergeFixture.rows(spark))
  }

  private def source(spark: SparkSession): DataFrame = spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 4)
    .table("a.db.t")
    .where("_commit_version = 4 AND _change_type <> 'update_preimage'")
    .alias("s")

  private def assignments(entries: (String, AnyRef)*): JMap[String, AnyRef] = {
    val result = new LinkedHashMap[String, AnyRef]()
    entries.foreach { case (key, value) => result.put(key, value) }
    result
  }

  private def merge(spark: SparkSession, input: DataFrame): GravitinoDeltaMergeBuilder =
    GravitinoDeltaTable
      .forName(spark, "b.db.t")
      .alias("t")
      .merge(input, "t.id = s.id")
      .whenMatchedDelete("s._change_type = 'delete'")
      .whenMatchedUpdate(assignments("secret" -> "s.secret", "region" -> "s.region"))
      .whenNotMatchedInsert(
        "s._change_type <> 'delete'",
        assignments("id" -> "s.id", "secret" -> "s.secret", "region" -> "s.region")
      )

  private def version(spark: SparkSession): Long =
    DeltaLog.forTable(spark, NativeSelectFixture.locations("b")).update().version
}
