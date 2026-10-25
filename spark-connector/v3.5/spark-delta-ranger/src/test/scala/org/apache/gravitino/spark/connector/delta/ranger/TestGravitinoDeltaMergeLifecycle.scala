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

import java.nio.file.{Files, Path, Paths}
import java.security.PrivilegedExceptionAction
import java.util.IdentityHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.execution.LogicalRDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Public builders retain native retries, current authority and durable recovery boundaries. */
class TestGravitinoDeltaMergeLifecycle {
  import PartitionedMergeFixture.{fixture, rows, bag, files, actions, messages, partitionSource}
  import GravitinoDeltaMergeTestFixture._
  import TestDeltaRangerMergeSource.{field, version}
  private val policies = new TestDeltaRangerMergePolicies
  private val mapper = new ObjectMapper

  @Test
  def constructionDriftRejectsWithoutCheckpointOrStaleSourceReuse(): Unit = {
    Seq(
      "subject",
      "settings",
      "source-registration",
      "source-grant",
      "source-mask",
      "source-filter",
      "target-grant"
    ).foreach { change =>
      fixture { (spark, plugin) =>
        val before = rows(spark); val beforeFiles = files()
        val input = source(spark)
        val pending = mergeFrom(spark, input)
        val registration = NativeSelectFixture.bindingFixture(spark, "a")
        val key = "delta.registration.receipt.nativeId"
        val original = registration.properties.get(key)
        val setting = "spark.sql.shuffle.partitions"
        val previous = spark.conf.get(setting)
        var checkpoint = 3L
        try {
          change match {
            case "settings"            => spark.conf.set(setting, "3")
            case "source-registration" => registration.properties.put(key, "foreign-source")
            case "source-grant"        => policies.publish(plugin, sourceDeniedSelect = Seq("id"))
            case "source-mask"   => policies.publish(plugin, sourceMasks = Map("id" -> "MASK_NULL"))
            case "source-filter" => policies.publish(plugin, sourceFilter = Some("region = 'drop'"))
            case "target-grant"  => policies.publish(plugin, deniedUpdate = Seq("secret"))
            case "subject"       => ()
          }
          val failure = assertThrows(
            classOf[Exception],
            () => {
              if (change == "subject")
                UserGroupInformation
                  .createUserForTesting("api-drift", Array("foreign"))
                  .doAs(new PrivilegedExceptionAction[Unit] {
                    override def run(): Unit = pending.execute()
                  })
              else pending.execute()
              checkpoint = 4L
            },
            change
          )
          assertTrue(messages(failure).contains("SecurityException"), messages(failure))
          assertEquals(3L, checkpoint); assertEquals(0L, version("b"));
          assertEquals(beforeFiles, files())
          assertThrows(classOf[IllegalStateException], () => pending.execute())
        } finally {
          spark.conf.set(setting, previous)
          registration.properties.put(key, original)
          policies.publish(plugin)
        }
        assertEquals(before, rows(spark))
        // A rejected CDF observation remains consumed/invalid; restoration never revives it.
        if (change.startsWith("source-"))
          assertThrows(classOf[Exception], () => mergeFrom(spark, input).execute())
        merge(spark).execute()
        assertEquals(1L, version("b")); assertEquals(expected, rows(spark))
      }
    }
  }

  @Test
  def targetNameResolvesCurrentCompatibleRegistrationAtExecute(): Unit = {
    var held: Path = null; var published: Path = null
    fixture(
      (spark, _) => {
        val pending = merge(spark)
        Files.move(held, published)
        assertEquals(1L, version("b"))
        pending.execute()
        assertEquals(2L, version("b"))
        assertEquals(expected + (Row(99L, "later", "other") -> 1), rows(spark))
      },
      prepare = spark => {
        spark
          .sql("SELECT 99L id, 'later' secret, 'other' region")
          .write
          .format("delta")
          .mode("append")
          .save(NativeSelectFixture.locations("b"))
        published =
          Paths.get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
        held = published.resolveSibling("api-held-target.json")
        Files.move(published, held); DeltaLog.clearCache()
      }
    )
  }

  @Test
  def postHelperRevocationAfterFilesCannotPublishOrAdvanceCheckpoint(): Unit = {
    Seq("target", "source").foreach { change =>
      fixture { (spark, plugin) =>
        val before = rows(spark); val beforeFiles = files()
        val pending = merge(spark)
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var writing = false; var revoked = false; var checkpoint = 3L
        var receipt: DeltaRangerMergeReceipt = null
        registration.onRead = n => {
          refresh(n)
          val writer =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (writer != null) {
            writing = true; receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
          }
          if (writing && writer == null && !revoked && files().diff(beforeFiles).nonEmpty) {
            revoked = true
            if (change == "target") policies.publish(plugin, deniedUpdate = Seq("region"))
            else policies.publish(plugin, sourceDeniedSelect = Seq("id"))
          }
        }
        assertThrows(classOf[Exception], () => { pending.execute(); checkpoint = 4L })
        assertTrue(revoked); assertEquals(3L, checkpoint); assertEquals(0L, version("b"))
        assertTrue(files().diff(beforeFiles).nonEmpty)
        registration.onRead = refresh; policies.publish(plugin)
        assertEquals(before, rows(spark));
        assertThrows(classOf[SecurityException], () => receipt.validate())
        assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
      }
    }
  }

  @Test
  def ownedNativeBlockLossRetriesOnceAndPolicyDriftRejectsNextAttempt(): Unit = {
    Seq(false, true).foreach { drift =>
      fixture { (spark, plugin) =>
        val pending = merge(spark)
        val before = rows(spark)
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        val seen = new IdentityHashMap[DeltaRangerMergeWrite, java.lang.Boolean]()
        val attempts = ArrayBuffer.empty[DeltaRangerMergeSource.Attempt]
        val retainedRddIds = ArrayBuffer.empty[Int]
        var changed = false; var checkpoint = 3L
        registration.onRead = n => {
          refresh(n)
          val writer =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (drift && attempts.nonEmpty && attempts.head.isClosed && !changed) {
            changed = true
            policies.publish(plugin, sourceMasks = Map("id" -> "MASK_NULL"))
          }
          if (writer != null && !seen.containsKey(writer)) {
            seen.put(writer, true)
            val attempt = field[DeltaRangerMergeSource.Attempt](writer, "source")
            attempts.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
            attempts += attempt
            val logical = attempt.logical.asInstanceOf[LogicalRDD]
            retainedRddIds += logical.rdd.id
            if (attempts.size == 1) logical.rdd.unpersist(blocking = true)
          }
        }
        if (drift) {
          assertThrows(classOf[SecurityException], () => { pending.execute(); checkpoint = 4L })
          assertTrue(changed); assertEquals(0L, version("b")); assertEquals(3L, checkpoint)
        } else {
          pending.execute(); checkpoint = 4L
          assertEquals(2, attempts.size); assertEquals(1L, version("b"));
          assertEquals(4L, checkpoint)
        }
        attempts.foreach(old => {
          assertTrue(old.isClosed)
          assertThrows(classOf[SecurityException], () => old.validate())
        })
        retainedRddIds.foreach(id => assertFalse(spark.sparkContext.getPersistentRDDs.contains(id)))
        registration.onRead = refresh; policies.publish(plugin)
        assertEquals(if (drift) before else expected, rows(spark))
      }
    }
  }

  @Test
  def exactSameRegistrationFixesSourceBeforePartitionWritebackAndRetry(): Unit = fixture(
    (spark, _) => {
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val refresh = registration.onRead
      val seen = new IdentityHashMap[DeltaRangerMergeWrite, java.lang.Boolean]()
      val attempts = ArrayBuffer.empty[DeltaRangerMergeSource.Attempt]
      var receipt: DeltaRangerMergeReceipt = null
      var closed = false
      registration.onRead = n => {
        refresh(n)
        val writer =
          field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (writer != null && !seen.containsKey(writer)) {
          seen.put(writer, true)
          val attempt = field[DeltaRangerMergeSource.Attempt](writer, "source")
          attempts += attempt; receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
          if (attempts.size == 1)
            attempt.logical.asInstanceOf[LogicalRDD].rdd.unpersist(blocking = true)
        }
        if (receipt != null && field[Boolean](receipt, "sourceClosed") && !closed) {
          closed = true; assertEquals(4L, version("a"))
          assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
        }
      }
      merge(spark, "a", "'moved'").execute()
      assertTrue(closed); assertEquals(2, attempts.size); assertEquals(5L, version("a"))
      assertEquals(
        bag(
          Vector(
            Row(1L, "changed", "moved"),
            Row(4L, "new", "moved"),
            Row(11L, "history", "keep"),
            Row(12L, "history", "keep"),
            Row(13L, "history", "keep")
          )
        ),
        rows(spark, "a")
      )
      assertEquals(
        bag(
          Vector(
            Row(1L, "changed", "keep", "update_preimage"),
            Row(1L, "changed", "moved", "update_postimage"),
            Row(4L, "new", "keep", "update_preimage"),
            Row(4L, "new", "moved", "update_postimage")
          )
        ),
        cdf(spark, "a", 5L)
      )
      assertEquals(
        bag(
          Vector(
            Row(1L, "old", "keep", "update_preimage"),
            Row(1L, "changed", "keep", "update_postimage"),
            Row(2L, "old", "drop", "delete"),
            Row(4L, "new", "keep", "insert")
          )
        ),
        cdf(spark, "a", 4L)
      )
    },
    prepare = partitionSource
  )

  @Test
  def alternateNamedAliasOfSameDatasetRejectsBeforeFiles(): Unit = fixture(
    (spark, _) => {
      val before = rows(spark, "a"); val beforeFiles = files("a")
      assertThrows(classOf[SecurityException], () => merge(spark).execute())
      assertEquals(4L, version("a")); assertEquals(before, rows(spark, "a"));
      assertEquals(beforeFiles, files("a"))
    },
    prepare = partitionSource,
    alias = true
  )

  @Test
  def partitionMovesEscapedNullAndHiddenRawRowsPreserveUntouchedFiles(): Unit = {
    val initial =
      "SELECT 1L id, 'visible-raw' secret, 'old/= %雪' region UNION ALL SELECT 8L, 'hidden-raw', 'old/= %雪' UNION ALL SELECT 3L, 'unchanged-raw', 'old/= %雪' UNION ALL SELECT 9L, 'untouched', CAST(NULL AS STRING)"
    fixture(
      (spark, plugin) => {
        val before = actions(NativeSelectFixture.locations("b"), 0L).collect { case a: AddFile =>
          a
        }
        assertEquals(
          1,
          before.count(_.partitionValues("region") == "old/= %雪"),
          "visible, hidden and unchanged rows share one original file"
        )
        val untouched = before.find(_.partitionValues("region") == null).get
        val untouchedPath = Paths.get(NativeSelectFixture.locations("b"), untouched.path)
        val untouchedBytes = Files.readAllBytes(untouchedPath)
        policies.publish(
          plugin,
          filter = Some("id <> 8L"),
          masks = Map("secret" -> "concat({col}, '-masked')")
        )
        builder(spark, source(spark))
          .whenMatchedDelete("s._change_type = 'delete'")
          .whenMatchedUpdate(values("secret" -> "s.secret", "region" -> "'next/= %雪'"))
          .whenNotMatchedInsert(
            "s._change_type <> 'delete'",
            values("id" -> "s.id", "secret" -> "s.secret", "region" -> "CAST(NULL AS STRING)")
          )
          .execute()
        policies.publish(plugin)
        assertEquals(
          bag(
            Vector(
              Row(1L, "changed", "next/= %雪"),
              Row(8L, "hidden-raw", "old/= %雪"),
              Row(3L, "unchanged-raw", "old/= %雪"),
              Row(9L, "untouched", null),
              Row(4L, "new", null)
            )
          ),
          rows(spark)
        )
        assertEquals(
          bag(
            Vector(
              Row(1L, "visible-raw", "old/= %雪", "update_preimage"),
              Row(1L, "changed", "next/= %雪", "update_postimage"),
              Row(4L, "new", null, "insert")
            )
          ),
          cdf(spark)
        )
        val committed = actions(NativeSelectFixture.locations("b"), 1L)
        val removed = committed.collect { case r: RemoveFile => r.path }.toSet
        assertFalse(removed(untouched.path))
        assertFalse(committed.collect { case a: AddFile => a.path }.contains(untouched.path))
        assertArrayEquals(untouchedBytes, Files.readAllBytes(untouchedPath))
        assertEquals(
          Set(Map("region" -> "old/= %雪"), Map("region" -> "next/= %雪"), Map("region" -> null)),
          committed.collect { case a: AddFile => a.partitionValues }.toSet
        )
      },
      prepare = spark => PartitionedMergeFixture.reset(spark, rows = initial)
    )
  }

  @Test
  def acknowledgementFailureReconcilesFreshHandlesWithoutSecondWrite(): Unit = {
    Seq(false, true).foreach { afterReconciliation =>
      fixture { (spark, plugin) =>
        identity(spark)
        val pending = merge(spark)
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var lost = false; var checkpoint = 3L
        var retired: DeltaRangerMergeReceipt = null
        registration.onRead = n => {
          val owner = DeltaRangerMergeCommit.current
          if (
            !lost && owner.nonEmpty && version("b") == 1L && (!afterReconciliation || field[
              Boolean
            ](field[DeltaRangerMergeReceipt](owner.get, "receipt"), "recached"))
          ) {
            lost = true; retired = field[DeltaRangerMergeReceipt](owner.get, "receipt")
            throw new IllegalStateException("API acknowledgement lost")
          }
          refresh(n)
        }
        assertThrows(classOf[Exception], () => { pending.execute(); checkpoint = 4L })
        assertTrue(lost); assertEquals(3L, checkpoint); assertEquals(1L, version("b"))
        val committed = record(spark)
        assertEquals("COMMITTED", committed.get("state").textValue())
        assertEquals(1L, committed.get("version").longValue())
        assertTrue(committed.get("command").textValue().nonEmpty)
        assertTrue(committed.get("proof").textValue().nonEmpty)
        assertEquals(DeltaRangerCdfRead.Request(0L, 4L), retired.source.request)
        assertThrows(classOf[IllegalStateException], () => pending.execute())
        val afterFiles = files()
        registration.onRead = refresh; assertEquals(expected, rows(spark))
        registration.onRead = n => {
          assertNull(
            field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
              .get()
          )
          assertNull(
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          )
          refresh(n)
        }
        merge(spark).execute(); checkpoint = 4L
        assertEquals(4L, checkpoint);
        val reconciled = record(spark)
        assertEquals("RECONCILED", reconciled.get("state").textValue())
        assertEquals(committed.get("command"), reconciled.get("command"))
        assertEquals(committed.get("proof"), reconciled.get("proof"))
        assertEquals(1L, version("b")); assertEquals(afterFiles, files());
        assertEquals(expected, rows(spark))
        assertThrows(classOf[SecurityException], () => retired.validate())
        assertThrows(
          classOf[SecurityException],
          () => merge(spark, region = "'different'").execute()
        )
        val changedInterval = spark.read
          .format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", "1")
          .option("endingVersion", "4")
          .table("a.db.t")
          .where("_commit_version = 4 AND _change_type <> 'update_preimage'")
          .alias("s")
        assertThrows(classOf[SecurityException], () => mergeFrom(spark, changedInterval).execute())
        policies.publish(plugin, deniedUpdate = Seq("region"))
        assertThrows(classOf[SecurityException], () => merge(spark).execute())
        assertEquals(1L, version("b")); assertEquals(afterFiles, files())
      }
    }
  }

  @Test
  def unknownOutcomeNeverReplaysAfterReauthorization(): Unit = fixture { (spark, plugin) =>
    identity(spark)
    val before = rows(spark)
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var revoked = false; var checkpoint = 3L
    registration.onRead = n => {
      refresh(n)
      if (!revoked && DeltaRangerMergeCommit.current.nonEmpty) {
        revoked = true; policies.publish(plugin, deniedUpdate = Seq("region"))
      }
    }
    assertThrows(classOf[SecurityException], () => { merge(spark).execute(); checkpoint = 4L })
    assertTrue(revoked); assertEquals(0L, version("b")); assertEquals(3L, checkpoint)
    assertEquals("UNKNOWN", record(spark).get("state").textValue())
    val afterFiles = files()
    registration.onRead = refresh; policies.publish(plugin)
    assertThrows(classOf[SecurityException], () => { merge(spark).execute(); checkpoint = 4L })
    assertEquals(3L, checkpoint); assertEquals(0L, version("b")); assertEquals(afterFiles, files());
    assertEquals(before, rows(spark))
  }

  private def mergeFrom(spark: SparkSession, input: DataFrame): GravitinoDeltaMergeBuilder =
    builder(spark, input)
      .whenMatchedDelete("s._change_type = 'delete'")
      .whenMatchedUpdate(values("secret" -> "s.secret", "region" -> "s.region"))
      .whenNotMatchedInsert("s._change_type <> 'delete'", insertValues)
  private def expected: Map[Row, Int] = bag(
    Vector(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep"))
  )
  private def identity(spark: SparkSession): Unit = {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "api-recovery")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
  }
  private def record(spark: SparkSession): JsonNode = {
    val root = NativeSelectFixture
      .bindingFixture(spark, "b")
      .wrapper
      .mergeJournalDirectory()
      .resolve("merge")
    val paths = Files.list(root)
    val journals =
      try paths.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toVector
      finally paths.close()
    assertEquals(1, journals.size)
    mapper.readTree(Files.readAllBytes(journals.head))
  }
}
