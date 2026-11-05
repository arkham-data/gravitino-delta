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
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, udf}
import org.apache.spark.sql.types.{MetadataBuilder, TimestampType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerInputTimestamp {
  @Test
  def namedInsertIntoFreezesTwoTimestampsWithMaskedFilteredRows(): Unit = append("insertInto")

  @Test
  def namedSaveAsTableFreezesTwoTimestampsWithPartitionedAdaptiveExchange(): Unit =
    append("saveAsTable", adaptive = true)

  @Test
  def withColumnAndSelectPreserveOriginalOutputsMetadataAndOneQueryCapture(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val metadata = new MetadataBuilder().putString("lineage", "loaded-at").build()
      val data = spark
        .table("a.db.t")
        .select(col("id"), col("secret"), current_timestamp().as("__arkham_loaded_at", metadata))
        .withColumn("second_time", current_timestamp())
        .select("id", "secret", "__arkham_loaded_at", "second_time")
      val authored = data.queryExecution.analyzed
      val frozen = DeltaRangerInputProjection.freeze(authored)
      assertEquals(authored.output, frozen.output)
      assertEquals(authored.schema, frozen.schema)
      assertEquals(metadata, frozen.output(2).metadata)
      val literal = timeLiterals(frozen).head
      assertEquals(2, timeLiterals(frozen).size)
      assertTrue(timeLiterals(frozen).forall(_ == literal))
      val normalized = data.queryExecution.normalized
      assertSame(normalized, data.queryExecution.normalized)
      val receiptTime = timeLiterals(normalized).head.value
      (1 to 3).foreach { _ =>
        DeltaRangerDecisionReceipt.check(normalized.clone(), spark, optimized = false)
        val replay = DeltaRangerDecisionReceipt.nativePrefix(normalized.clone(), spark)
        DeltaRangerDecisionReceipt.check(replay, spark, optimized = true)
        val prepared = data.queryExecution.optimizedPlan
        DeltaRangerNativeScans.prepared(prepared).verify(prepared.clone(), spark)
        assertEquals(receiptTime, timeLiterals(data.queryExecution.normalized).head.value)
      }
      val first = data.collect().toVector.sortBy(_.getLong(0))
      assertEquals(first, data.collect().toVector.sortBy(_.getLong(0)))
      assertEquals(
        Vector(Row(1L, "a-one-masked"), Row(3L, null)),
        first.map(row => Row(row.getLong(0), row.get(1)))
      )
      first.foreach { row =>
        assertNotNull(row.get(2))
        assertEquals(row.getTimestamp(2), row.getTimestamp(3))
      }
      val fresh = spark.sql("SELECT id, current_timestamp() AS fresh_time FROM a.db.t")
      val freshPlan = fresh.queryExecution.normalized
      // Native captures need independent literal objects, never a distinct-wall-clock promise.
      assertNotSame(timeLiterals(normalized).head, timeLiterals(freshPlan).head)
      assertNotSame(
        DeltaRangerDecisionReceipt.consumeOwned(normalized, spark, optimized = false),
        DeltaRangerDecisionReceipt.consumeOwned(freshPlan, spark, optimized = false)
      )
    }

  @Test
  def plainWithColumnChainKeepsOneCaptureThroughTimestampAttributeReferences(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val frame = spark
        .table("a.db.t")
        .withColumn("first_time", current_timestamp())
        .withColumn("second_time", current_timestamp())
        .select(
          col("id"),
          col("secret"),
          col("first_time"),
          col("second_time"),
          col("first_time").as("copied_time")
        )
      val authored = frame.queryExecution.analyzed
      val frozen = DeltaRangerInputProjection.freeze(authored)
      assertEquals(authored.output, frozen.output)
      assertEquals(authored.schema, frozen.schema)
      val normalized = frame.queryExecution.normalized
      assertEquals(2, timeLiterals(normalized).size)
      assertEquals(1, timeLiterals(normalized).map(_.value).distinct.size)
      val rows = frame.collect().toVector
      assertEquals(Set(1L, 3L), rows.map(_.getLong(0)).toSet)
      rows.foreach { row =>
        assertNotNull(row.get(2))
        assertEquals(row.get(2), row.get(3))
        assertEquals(row.get(2), row.get(4))
      }
    }

  @Test
  def stockNativeQueryExecutionCapturesAtOptimizationAndKeepsReusedActionsStable(): Unit =
    NativeSelectFixture.withSpark { spark =>
      def query = spark.read
        .format("delta")
        .load(NativeSelectFixture.locations("a"))
        .selectExpr("id", "current_timestamp() AS first_time", "current_timestamp() AS second_time")
      val frame = query
      val analyzed = frame.queryExecution.analyzed
      val normalized = frame.queryExecution.normalized
      assertTrue(
        normalized.exists(_.expressions.exists(_.exists(_.isInstanceOf[CurrentTimestamp])))
      )
      val optimized = frame.queryExecution.optimizedPlan
      assertFalse(
        optimized.exists(_.expressions.exists(_.exists(_.isInstanceOf[CurrentTimestamp])))
      )
      assertEquals(analyzed.output, optimized.output)
      assertEquals(1, timeLiterals(optimized).map(_.value).distinct.size)
      assertSame(optimized, frame.queryExecution.optimizedPlan)
      assertEquals(
        frame.collect().toVector.sortBy(_.getLong(0)),
        frame.collect().toVector.sortBy(_.getLong(0))
      )
      assertNotSame(
        timeLiterals(optimized).head,
        timeLiterals(query.queryExecution.optimizedPlan).head
      )
    }

  @Test
  def pendingTimestampCannotBecomeAnExecutableReceiptWithoutFreeze(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val authored =
        spark.sql("SELECT id, current_timestamp() AS ts FROM a.db.t").queryExecution.analyzed
      assertTrue(DeltaRangerDecisionReceipt.binding(authored).nonEmpty)
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => NativeSelectFixture.rule(spark, policy).authorizeComplete(authored)
        ),
        "unfrozen input timestamp"
      )
    }

  @Test
  def timestampLiteralSubstitutionCannotBorrowPendingIntentOrIssuedReceipt(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val data = spark.sql("SELECT id, current_timestamp() AS ts FROM a.db.t")
      val authored = data.queryExecution.analyzed
      val changedPending = authored.transformAllExpressions { case _: CurrentTimestamp =>
        Literal.create(42L, TimestampType)
      }
      changedPending.copyTagsFrom(authored)
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerOrdinaryRead.normalize(changedPending, spark)
        ),
        "original intent changed"
      )
      val approved = data.queryExecution.normalized
      val changedReceipt = approved.transformAllExpressions {
        case literal: Literal if literal.dataType == TimestampType =>
          Literal.create(literal.value.asInstanceOf[Long] + 1L, TimestampType)
      }
      changedReceipt.copyTagsFrom(approved)
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerDecisionReceipt.check(changedReceipt, spark, optimized = false)
        ),
        "logical binding changed"
      )
      DeltaRangerDecisionReceipt.check(approved, spark, optimized = false)
    }

  @Test
  def nestedPredicateForeignAndUdfTimeExpressionsRemainRejected(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      Seq(
        "SELECT id, CAST(current_timestamp() AS STRING) AS ts FROM a.db.t",
        "SELECT id, current_timestamp() + INTERVAL 1 SECOND AS ts FROM a.db.t",
        "SELECT id, current_date() AS ts FROM a.db.t",
        "SELECT id, localtimestamp() AS ts FROM a.db.t",
        "SELECT id FROM a.db.t WHERE current_timestamp() > TIMESTAMP '2000-01-01 00:00:00'"
      ).foreach(sql => assertThrows(classOf[SecurityException], () => spark.sql(sql)))
      val foreign = Alias(new CurrentTimestamp {}, "foreign")()
      assertThrows(classOf[SecurityException], () => DeltaRangerInputProjection.expression(foreign))
      val customFoldable = Alias(UnaryMinus(Literal(1L)), "foreign_foldable")()
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerInputProjection.expression(customFoldable)
      )
      val function = udf((value: Long) => value)
      assertThrows(
        classOf[SecurityException],
        () => spark.table("a.db.t").select(col("id"), function(col("id")).as("custom"))
      )
    }

  @Test
  def sharedPolicyAndCdfGrammarStillRejectCurrentTimestamp(): Unit = {
    assertThrows(
      classOf[SecurityException],
      () => DeltaRangerPlanAdmission.expression(Alias(CurrentTimestamp(), "ts")())
    )
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      policy.mask = "CAST(current_timestamp() AS STRING)"
      assertThrows(
        classOf[SecurityException],
        () =>
          spark
            .sql("SELECT secret, current_timestamp() AS ts FROM a.db.t")
            .queryExecution
            .normalized
      )
      policy.mask = "concat({col}, '-masked')"
      policy.filter = "current_timestamp() > TIMESTAMP '2000-01-01 00:00:00'"
      assertThrows(
        classOf[SecurityException],
        () =>
          spark.sql("SELECT id, current_timestamp() AS ts FROM a.db.t").queryExecution.normalized
      )
    }
    DeltaRangerCdfReadFixture.withGoverned(native => {
      DeltaRangerCdfReadFixture.enable(native)
      ()
    }) { spark =>
      val source = spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", "1")
        .option("endingVersion", "1")
        .table("a.db.t")
      assertReason(
        assertThrows(
          classOf[SecurityException],
          () => source.selectExpr("id", "current_timestamp() AS ts")
        ),
        "current_timestamp"
      )
    }
  }

  @Test
  def mergeSourceAndActionGrammarStillRejectCurrentTimestampWithoutMutation(): Unit = {
    val fixture = new TestDeltaRangerInstalledMerge
    fixture.withPolicies((spark, _) => {
      val trace = InstalledAppendBoundaryProbe.fresh("timestamp-merge-grammar")
      try {
        val root = Paths.get(NativeSelectFixture.locations("a")).getParent
        val before = InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
        val sql = """MERGE INTO b.db.t t USING
          |(SELECT * FROM table_changes('a.db.t', 0, 4)
          | WHERE _commit_version = 4 AND _change_type <> 'update_preimage') s
          |ON t.id = s.id
          |WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
          |WHEN MATCHED THEN UPDATE SET secret = s.secret, region = s.region
          |WHEN NOT MATCHED AND s._change_type <> 'delete'
          |THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
          |""".stripMargin
        Seq(
          sql.replace(
            "SELECT * FROM table_changes",
            "SELECT *, current_timestamp() AS ts FROM table_changes"
          ),
          sql.replace("secret = s.secret", "secret = CAST(current_timestamp() AS STRING)")
        )
          .foreach(query =>
            assertReason(
              assertThrows(classOf[SecurityException], () => spark.sql(query)),
              "current_timestamp"
            )
          )
        assertEquals(
          before,
          InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
        )
      } finally trace.close()
    })
  }

  @Test
  def timestampDoesNotHideColumnlessDeniedPredicateOrTargetDemand(): Unit =
    new TestDeltaRangerAppendV1Input().timestampDemandRejections()

  @Test
  def timestampTerminalInputRechecksPolicyPublicationRegistrationSubjectAndSettings(): Unit =
    new TestDeltaRangerAppendV1Input().inputChanges(
      "SELECT id, secret, region, current_timestamp() AS __arkham_loaded_at, " +
        "current_timestamp() AS second_time FROM b.db.t",
      timestampTarget
    )

  private def timestampTarget(spark: SparkSession): Unit = {
    spark
      .sql(
        "ALTER TABLE delta.`" + NativeSelectFixture.locations("a") +
          "` ADD COLUMNS (__arkham_loaded_at TIMESTAMP, second_time TIMESTAMP)"
      )
      .collect()
    ()
  }

  private def timeLiterals(plan: LogicalPlan): Vector[Literal] =
    plan
      .collect { case project: Project =>
        project.projectList.collect {
          case alias: Alias
              if alias.child.isInstanceOf[Literal] && alias.child.dataType == TimestampType =>
            alias.child.asInstanceOf[Literal]
        }
      }
      .flatten
      .toVector

  private def assertReason(error: Throwable, reason: String): Unit =
    assertTrue(
      Iterator
        .iterate(error)(_.getCause)
        .takeWhile(_ != null)
        .exists(t => Option(t.getMessage).exists(_.contains(reason))),
      error.toString
    )

  private def append(frontend: String, adaptive: Boolean = false): Unit = {
    var captured: java.sql.Timestamp = null
    var authored: LogicalPlan = null
    val source =
      if (adaptive)
        "SELECT x.id, x.secret, x.region, current_timestamp() AS __arkham_loaded_at, " +
          "current_timestamp() AS second_time FROM b.db.t x JOIN b.db.t y ON x.id = y.id"
      else
        "SELECT id, secret, region, current_timestamp() AS __arkham_loaded_at, " +
          "current_timestamp() AS second_time FROM b.db.t"
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      frontend = frontend,
      adaptive = adaptive,
      partitionedTarget = adaptive,
      sourceSql = source,
      scenario = "input-timestamp-" + frontend,
      testMethod = "inputTimestamp-" + frontend,
      timestampColumns = Seq("__arkham_loaded_at", "second_time"),
      inspectInput = input => {
        authored = input.queryExecution.analyzed
        assertEquals(
          2,
          authored.expressions
            .flatMap(_.collect { case value: CurrentTimestamp =>
              value
            })
            .size
        )
      },
      inspectFrozenInput = frozen => {
        assertEquals(authored.output.takeRight(2), frozen.output.takeRight(2))
        assertEquals(authored.output.head, frozen.output.head)
        assertEquals(authored.output(2), frozen.output(2))
        assertEquals(authored.schema, frozen.schema)
        assertFalse(frozen.exists(_.expressions.exists(_.exists(_.isInstanceOf[CurrentTimestamp]))))
        val times = frozen.asInstanceOf[Project].projectList.takeRight(2).map { case alias: Alias =>
          alias.child.asInstanceOf[Literal].value.asInstanceOf[Long]
        }
        assertEquals(times.head, times.last)
        captured = org.apache.spark.sql.catalyst.util.DateTimeUtils.toJavaTimestamp(times.head)
      },
      inspectRows = rows => {
        val appended = rows.filter(row => row.get(3) != null)
        assertEquals(
          Vector(Row(1L, "b-one-masked", "keep"), Row(3L, null, "keep")),
          appended.map(row => Row.fromSeq(row.toSeq.take(3))).sortBy(_.getLong(0))
        )
        assertEquals(3, rows.count(_.isNullAt(3)))
        appended.foreach { row =>
          assertEquals(captured, row.getTimestamp(3))
          assertEquals(captured, row.getTimestamp(4))
        }
      }
    )
  }
}
