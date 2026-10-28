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

import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.delta.actions.{AddFile, AddCDCFile, RemoveFile, CommitInfo}
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Native SQL and JVM MERGE retain source bounds, actions and rows with stock AQE. */
class TestDeltaRangerTrustedAqeMerge {
  private val query = """MERGE INTO b.db.t t USING
    |(SELECT * FROM table_changes('a.db.t', 0, 4)
    | WHERE _commit_version = 4 AND _change_type <> 'update_preimage') s
    |ON t.id = s.id
    |WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
    |WHEN MATCHED THEN UPDATE SET secret = s.secret, region = s.region
    |WHEN NOT MATCHED AND s._change_type <> 'delete'
    |THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
    |""".stripMargin

  private def version: Long = versionOf("b")
  private def versionOf(name: String): Long = {
    val paths = java.nio.file.Files
      .list(java.nio.file.Paths.get(NativeSelectFixture.locations(name), "_delta_log"))
    import scala.collection.JavaConverters._
    try
      paths
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .filter(_.matches("[0-9]{20}\\.json"))
        .map(_.stripSuffix(".json").toLong)
        .max
    finally paths.close()
  }

  @Test
  def namedSqlAndBuilderMergeUseAqe(): Unit = {
    var sqlActions = Map.empty[String, Int]
    var sqlParameters = Map.empty[String, String]
    Seq(false, true).foreach { builder =>
      new TestDeltaRangerInstalledMerge().withPolicies(
        (spark, _) => {
          PartitionedMergeFixture.origins(spark)
          Seq(
            classOf[DeltaRangerMergeReceipt],
            classOf[DeltaRangerCdfReadReceipt],
            classOf[DeltaRangerNativeOperation],
            DeltaRangerExecutionContext.getClass
          ).foreach { kind =>
            val stream = kind.getResourceAsStream("/" + kind.getName.replace('.', '/') + ".class")
            val bytes =
              try stream.readAllBytes()
              finally stream.close()
            val sha = java.security.MessageDigest
              .getInstance("SHA-256")
              .digest(bytes)
              .map(value => f"${value & 0xff}%02x")
              .mkString
            org.slf4j.LoggerFactory
              .getLogger(getClass)
              .info(
                "TRUSTED_MERGE_CLASS " + kind.getName +
                  " " + kind.getProtectionDomain.getCodeSource.getLocation + " sha256=" + sha
              )
          }
          val executions = new ConcurrentLinkedQueue[QueryExecution]()
          val observedJobs = java.util.concurrent.ConcurrentHashMap.newKeySet[Integer]()
          val jobGroup = "phase-observation-" + java.util.UUID.randomUUID().toString
          spark.sparkContext.setJobGroup(jobGroup, jobGroup)
          spark.sparkContext.addSparkListener(new SparkListener {
            override def onJobStart(event: SparkListenerJobStart): Unit = {
              Option(event.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)).foreach { id =>
                Option(SQLExecution.getQueryExecution(id.toLong)).foreach(executions.add)
              }
              observedJobs.add(event.jobId)
            }
          })
          val before = version
          if (builder) {
            val source = spark
              .sql("""SELECT * FROM table_changes('a.db.t', 0, 4)
              |WHERE _commit_version = 4 AND _change_type <> 'update_preimage'""".stripMargin)
              .alias("s")
            val update = new LinkedHashMap[String, String]()
            update.put("secret", "s.secret"); update.put("region", "s.region")
            val insert = new LinkedHashMap[String, String](update)
            insert.put("id", "s.id")
            GravitinoDeltaTable
              .forName(spark, "b.db.t")
              .alias("t")
              .merge(source, "t.id = s.id")
              .whenMatchedDelete("s._change_type = 'delete'")
              .whenMatchedUpdate(update)
              .whenNotMatchedInsert("s._change_type <> 'delete'", insert)
              .execute()
          } else spark.sql(query).collect()
          assertEquals(before + 1, version)
          assertEquals(
            Map(
              Row(1L, "changed", "keep") -> 1,
              Row(3L, null, "keep") -> 1,
              Row(4L, "new", "keep") -> 1
            ),
            PartitionedMergeFixture.rows(spark)
          )
          DeltaRangerTrustedAqeFixture.awaitSubmittedJobs(
            spark.sparkContext.statusTracker.getJobIdsForGroup(jobGroup).toSet,
            id => observedJobs.contains(id),
            30000
          )
          spark.sparkContext.clearJobGroup()
          val phases = executions.asScala.toVector.distinct.flatMap { execution =>
            val plan = execution.executedPlan
            val native = DeltaRangerTrustedAqeFixture.nodes(plan).collect {
              case phase: DeltaRangerMergeNative.Exec => "touched" -> phase.child
              case phase: DeltaRangerMergeWrite.Exec  => "write" -> phase.child
            }
            native
          }
          assertEquals(Set("touched", "write"), phases.map(_._1).toSet)
          phases.foreach { case (phase, plan) =>
            val nodes = DeltaRangerTrustedAqeFixture.nodes(plan)
            assertTrue(
              nodes.exists { case a: AdaptiveSparkPlanExec => a.isFinalPlan; case _ => false },
              phase + plan
            )
            assertTrue(nodes.exists(_.isInstanceOf[QueryStageExec]), phase + plan)
            org.slf4j.LoggerFactory
              .getLogger(getClass)
              .info("TRUSTED_MERGE_PHASE " + phase + "\n" + plan)
          }
          val changes = spark
            .sql("SELECT id, secret, region, _change_type FROM table_changes('b.db.t', 1, 1)")
            .collect()
            .toVector
          assertEquals(
            PartitionedMergeFixture.bag(
              Vector(
                Row(1L, "b-one", "keep", "update_preimage"),
                Row(1L, "changed", "keep", "update_postimage"),
                Row(2L, "b-two", "drop", "delete"),
                Row(4L, "new", "keep", "insert")
              )
            ),
            PartitionedMergeFixture.bag(changes)
          )
          val actions =
            PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), before + 1)
          assertEquals(1, actions.count(_.isInstanceOf[CommitInfo]))
          assertEquals(1, actions.count(_.isInstanceOf[RemoveFile]))
          assertTrue(actions.exists(_.isInstanceOf[AddFile]))
          assertTrue(actions.exists(_.isInstanceOf[AddCDCFile]))
          val files = actions.collect {
            case add: AddFile => add.copy(path = "<native-data-file>", modificationTime = 0L).json
            case remove: RemoveFile =>
              remove.copy(path = "<native-removed-file>", deletionTimestamp = None).json
            case cdc: AddCDCFile => cdc.copy(path = "<native-cdf-file>").json
          }
          val actionBag =
            files.groupBy(identity).map { case (action, copies) => action -> copies.size }
          val info = actions.collect { case info: CommitInfo => info }.head
          assertEquals("MERGE", info.operation)
          if (!builder) {
            sqlActions = actionBag;
            sqlParameters = info.operationParameters.map { case (key, value) =>
              key -> value.replaceAll("#[0-9]+", "#expr")
            }
          } else {
            assertEquals(sqlActions, actionBag);
            assertEquals(
              sqlParameters,
              info.operationParameters.map { case (key, value) =>
                key -> value.replaceAll("#[0-9]+", "#expr")
              }
            )
          }
          assertEquals(actions.size - 1, files.size, "unexpected native action kind")
          assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
        },
        prepareTarget = spark => PartitionedMergeFixture.reset(spark, keys = Nil),
        startupOptions = DeltaRangerTrustedAqeFixture.coalescing +
          ("spark.sql.adaptive.enabled" -> "true")
      )
    }
  }
  @Test
  def partitionedHiddenRowsAndMaskedKeys(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        val policies = new TestDeltaRangerMergePolicies
        val original = PartitionedMergeFixture
          .actions(NativeSelectFixture.locations("b"), 0)
          .collect { case add: AddFile => add }
        val untouched = original.find(_.partitionValues("region") == "drop").get
        policies.publish(
          plugin,
          filter = Some("region = 'keep' AND id <> 2L"),
          masks = Map("id" -> "CAST(1 AS BIGINT)", "secret" -> "concat({col}, '-masked')"),
          deniedSelect = Seq("secret"),
          updated = Seq("region")
        )
        spark
          .sql(
            PartitionedMergeFixture.merge("WHEN MATCHED THEN UPDATE SET region = 'escaped/a=b%'")
          )
          .collect()
        policies.publish(plugin)
        assertEquals(
          Map(
            Row(1L, "b-one", "escaped/a=b%") -> 1,
            Row(2L, "b-two", "keep") -> 1,
            Row(9L, "b-nine", "drop") -> 1,
            Row(3L, null, "escaped/a=b%") -> 1
          ),
          PartitionedMergeFixture.rows(spark)
        )
        val actions = PartitionedMergeFixture.actions(NativeSelectFixture.locations("b"), 1)
        assertFalse(
          actions.collect { case remove: RemoveFile => remove.path }.contains(untouched.path)
        )
        val rewritten = original.find(_.partitionValues("region") == "keep").get
        assertTrue(
          actions.collect { case remove: RemoveFile => remove.path }.contains(rewritten.path)
        )
        assertTrue(
          actions
            .collect { case add: AddFile => add.partitionValues }
            .contains(Map("region" -> "keep"))
        )
        policies.publish(
          plugin,
          filter = Some("region <> 'drop' AND id <> 2L"),
          masks = Map("id" -> "CAST(1 AS BIGINT)", "secret" -> "concat({col}, '-masked')")
        )
        spark
          .sql(
            PartitionedMergeFixture.merge(
              "WHEN MATCHED THEN UPDATE SET region = CAST(NULL AS STRING), secret = t.secret"
            )
          )
          .collect()
        policies.publish(plugin)
        assertEquals(
          Map(
            Row(1L, "b-one-masked", null) -> 1,
            Row(2L, "b-two", "keep") -> 1,
            Row(9L, "b-nine", "drop") -> 1,
            Row(3L, null, null) -> 1
          ),
          PartitionedMergeFixture.rows(spark)
        )
        assertEquals(2L, version)
      },
      prepareTarget = spark =>
        PartitionedMergeFixture.reset(
          spark,
          rows =
            "SELECT 1L id, 'b-one' secret, 'keep' region UNION ALL SELECT 2L, 'b-two', 'keep' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep' UNION ALL SELECT 9L, 'b-nine', 'drop'"
        ),
      startupOptions =
        DeltaRangerTrustedAqeFixture.coalescing + ("spark.sql.adaptive.enabled" -> "true")
    )
  }

  @Test
  def actionedCachedAndDerivedSourcesReject(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val source = spark.sql(
          "SELECT * FROM table_changes('a.db.t', 0, 4) WHERE _commit_version = 4 AND _change_type <> 'update_preimage'"
        )
        source.collect()
        val before = version
        var checkpoint = 4L
        assertThrows(
          classOf[Exception],
          () => {
            GravitinoDeltaTable
              .forName(spark, "b.db.t")
              .alias("t")
              .merge(source.alias("s"), "t.id = s.id")
              .whenMatchedDelete()
              .execute()
            checkpoint = 5L
          }
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
        val fresh = spark.sql("SELECT * FROM table_changes('a.db.t', 0, 4)")
        assertThrows(classOf[Exception], () => fresh.cache().collect())
        assertThrows(classOf[Exception], () => fresh.repartition(2).collect())
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )

  private def field[A](value: AnyRef, name: String): A = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name))
      .get
    member.setAccessible(true); member.get(value).asInstanceOf[A]
  }

  @Test
  def foreignCheckpointRejected(): Unit = new TestDeltaRangerInstalledMerge().withPolicies(
    (spark, _) => {
      val schema = spark.sql("SELECT * FROM table_changes('a.db.t', 0, 4)").schema
      val before = version
      val files = PartitionedMergeFixture.files()
      var checkpoint = 4L
      Seq("local", "rdd", "checkpoint").foreach { sourceKind =>
        assertThrows(
          classOf[Exception],
          () => {
            val rows = java.util.Collections.singletonList(
              Row(99L, "foreign", "keep", "insert", 4L, new java.sql.Timestamp(0L))
            )
            val foreign =
              if (sourceKind == "rdd")
                spark.createDataFrame(spark.sparkContext.parallelize(rows.asScala.toSeq), schema)
              else spark.createDataFrame(rows, schema)
            assertSame(spark, foreign.sparkSession)
            val input = if (sourceKind == "checkpoint") foreign.localCheckpoint() else foreign
            GravitinoDeltaTable
              .forName(spark, "b.db.t")
              .alias("t")
              .merge(input.alias("s"), "t.id = s.id")
              .whenMatchedDelete()
              .execute()
            checkpoint = 5L
          }
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
        assertEquals(files, PartitionedMergeFixture.files())
      }
    },
    startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
  )

  @Test
  def sameRegistrationWriteback(): Unit = new TestDeltaRangerInstalledMerge().withPolicies(
    (spark, _) => {
      spark
        .sql("""MERGE INTO a.db.t t USING
        |(SELECT * FROM table_changes('a.db.t', 0, 4)
        |WHERE _commit_version = 4 AND _change_type = 'update_postimage') s
        |ON t.id = s.id WHEN MATCHED THEN UPDATE SET region = 'writeback'""".stripMargin)
        .collect()
      assertEquals(5L, versionOf("a"))
      assertEquals(
        Map(
          Row(1L, "changed", "writeback") -> 1,
          Row(4L, "new", "keep") -> 1,
          Row(11L, "history", "keep") -> 1,
          Row(12L, "history", "keep") -> 1,
          Row(13L, "history", "keep") -> 1
        ),
        PartitionedMergeFixture.rows(spark, "a")
      )
      assertEquals(
        Map(Row("update_preimage", "keep") -> 1, Row("update_postimage", "writeback") -> 1),
        PartitionedMergeFixture.bag(
          spark
            .sql("SELECT _change_type, region FROM table_changes('a.db.t', 5, 5)")
            .collect()
            .toVector
        )
      )
    },
    startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
  )

  @Test
  def revokedBetweenMergePhasesStopsNextWork(): Unit = {
    revoked("source"); revoked("source", empty = true); revoked("filter")
  }

  @Test
  def revokedBeforeCommitLeavesNoVersion(): Unit = {
    revoked("commit"); revoked("commit", empty = true); revoked("mask")
  }

  private def revoked(boundary: String, empty: Boolean = false): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        val before = version
        var checkpoint = 4L
        var fired = false
        var sourceSeen: DeltaRangerMergeSource.Attempt = null
        val beforeFiles = PartitionedMergeFixture.files()
        registration.onRead = n => {
          refresh(n)
          val source =
            field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
              .get()
          if (source != null) sourceSeen = source
          val commit = DeltaRangerMergeCommit.current
          val trigger =
            if (boundary == "commit" || boundary == "mask") commit.nonEmpty
            else sourceSeen != null && source == null
          if (trigger && !fired) {
            fired = true
            val policies = new TestDeltaRangerMergePolicies
            if (boundary == "commit") policies.publish(plugin, deniedUpdate = Seq("secret"))
            else if (boundary == "mask")
              policies.publish(plugin, masks = Map("secret" -> "concat({col}, '-new')"))
            else if (boundary == "filter")
              policies.publish(plugin, sourceFilter = Some("region = 'keep'"))
            else policies.publish(plugin, sourceDeniedSelect = Seq("secret"))
          }
        }
        assertThrows(
          classOf[Exception],
          () => {
            spark
              .sql(
                if (empty) query.replace("_commit_version = 4", "_commit_version = -1") else query
              )
              .collect(); checkpoint = 5L
          }
        )
        assertTrue(fired, "phase barrier was not reached")
        assertEquals(before, version); assertEquals(4L, checkpoint)
        if (boundary == "source" || boundary == "filter")
          assertEquals(beforeFiles, PartitionedMergeFixture.files())
        else if (!empty) assertTrue(PartitionedMergeFixture.files().diff(beforeFiles).nonEmpty)
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )

  @Test
  def unsupportedCacheRejectsBeforeMutation(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val before = version
        val files = PartitionedMergeFixture.files()
        var checkpoint = 4L
        // Ordinary origin admission rejects cache construction before it can become
        // a reusable payload or a MERGE input.
        val failure = assertThrows(
          classOf[Exception],
          () => {
            spark.table("b.db.t").cache()
            spark.sql(query).collect(); checkpoint = 5L
          }
        )
        assertTrue(
          PartitionedMergeFixture
            .messages(failure)
            .contains("ordinary cache construction is not qualified"),
          failure.toString
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
        assertEquals(
          files,
          PartitionedMergeFixture.files(),
          "cache rejection must precede file generation"
        )
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )

  @Test
  def emptyAndNativeErrorOutcomes(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        val policies = new TestDeltaRangerMergePolicies
        val before = version
        var checkpoint = 4L
        policies.publish(plugin, deniedUpdate = Seq("secret"))
        Seq(
          query.replace("_commit_version = 4", "_commit_version = -1"),
          query.replace("WHEN MATCHED THEN UPDATE", "WHEN MATCHED AND false THEN UPDATE")
        ).foreach { denied =>
          assertThrows(classOf[Exception], () => { spark.sql(denied).collect(); checkpoint = 5L })
          assertEquals(before, version); assertEquals(4L, checkpoint)
        }
        policies.publish(plugin)
        val ambiguous = query.replace(
          "_commit_version = 4 AND _change_type <> 'update_preimage'",
          "_commit_version = 4"
        )
        val failure = assertThrows(
          classOf[Exception],
          () => { spark.sql(ambiguous).collect(); checkpoint = 5L }
        )
        assertTrue(
          PartitionedMergeFixture
            .messages(failure)
            .contains("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"),
          failure.toString
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
        val noOp = query.replace("_commit_version = 4", "_commit_version = -1")
        spark.sql(noOp).collect()
        assertEquals(before, version)
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )
  }

  @Test
  def nativeConstraintFailureLeavesNoVersion(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val before = version
        var checkpoint = 4L
        val failure = assertThrows(
          classOf[Exception],
          () => {
            spark
              .sql(PartitionedMergeFixture.merge("WHEN MATCHED THEN UPDATE SET region = ''"))
              .collect()
            checkpoint = 5L
          }
        )
        assertTrue(
          PartitionedMergeFixture
            .messages(failure)
            .contains("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"),
          failure.toString
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
      },
      prepareTarget = spark => {
        PartitionedMergeFixture.reset(spark)
        spark
          .sql(
            s"ALTER TABLE delta.`${NativeSelectFixture.locations("b")}` ADD CONSTRAINT partition_check CHECK (region IS NOT NULL)"
          )
          .collect()
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )

  @Test
  def deniedBuilderCannotReplayItsSourceAttempt(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        val input = spark
          .sql(
            "SELECT * FROM table_changes('a.db.t', 0, 4) WHERE _commit_version = 4 AND _change_type = 'update_postimage'"
          )
          .alias("s")
        val assignments = new LinkedHashMap[String, String]()
        assignments.put("region", "'new'")
        val builder = GravitinoDeltaTable
          .forName(spark, "b.db.t")
          .alias("t")
          .merge(input, "t.id = s.id")
          .whenMatchedUpdate(assignments)
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var fired = false
        registration.onRead = n => {
          refresh(n)
          if (!fired && DeltaRangerMergeCommit.current.nonEmpty) {
            fired = true
            new TestDeltaRangerMergePolicies().publish(plugin, deniedUpdate = Seq("region"))
          }
        }
        val before = version
        var checkpoint = 4L
        assertThrows(classOf[Exception], () => { builder.execute(); checkpoint = 5L })
        assertTrue(fired); assertEquals(before, version); assertEquals(4L, checkpoint)
        val afterFailure = PartitionedMergeFixture.files()
        spark.sparkContext.setJobGroup("denied-builder-replay", "denied-builder-replay")
        assertThrows(classOf[Exception], () => { builder.execute(); checkpoint = 5L })
        assertTrue(
          spark.sparkContext.statusTracker.getJobIdsForGroup("denied-builder-replay").isEmpty
        )
        assertEquals(before, version); assertEquals(4L, checkpoint)
        assertEquals(afterFailure, PartitionedMergeFixture.files())
      },
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )

}
