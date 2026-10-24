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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.IdentityHashMap
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile, AddCDCFile}
import org.apache.spark.sql.execution.LogicalRDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Partitioned publication revalidates authority after files and never replays uncertain outcomes.
  */
class TestDeltaRangerPartitionedMergeLifecycle {
  import PartitionedMergeFixture._
  import TestDeltaRangerMergeSource.{field, version, sql}
  private val policies = new TestDeltaRangerMergePolicies
  private val mapper = new ObjectMapper

  @Test
  def sameTableRetryClosesFixedSourceBeforePartitionPublication(): Unit = fixture(
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
          val source = field[DeltaRangerMergeSource.Attempt](writer, "source")
          attempts.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
          attempts += source
          receipt = field[DeltaRangerMergeReceipt](writer, "receipt")
          if (attempts.size == 1)
            source.logical.asInstanceOf[LogicalRDD].rdd.unpersist(blocking = true)
        }
        if (receipt != null && field[Boolean](receipt, "sourceClosed") && !closed) {
          closed = true
          assertEquals(4L, version("a"))
          assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
          attempts.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
        }
        DeltaRangerMergeCommit.current.foreach { owner =>
          assertTrue(closed)
          assertTrue(
            field[Boolean](field[DeltaRangerMergeReceipt](owner, "receipt"), "sourceClosed")
          )
        }
      }
      val statement = sql
        .replace("MERGE INTO b.db.t", "MERGE INTO a.db.t")
        .replace("region = s.region", "region = 'moved'")
      var checkpoint = 3L
      assertEquals(Vector(Row(2L, 2L, 0L, 0L)), spark.sql(statement).collect().toVector)
      checkpoint = 4L
      assertEquals(4L, checkpoint); assertEquals(2, attempts.size); assertTrue(closed)
      assertEquals(5L, version("a"))
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
      val fixed = spark
        .sql("SELECT id, _change_type FROM table_changes('a.db.t', 0, 4) WHERE _commit_version = 4")
        .collect()
        .toVector
      assertEquals(
        bag(
          Vector(
            Row(1L, "update_preimage"),
            Row(1L, "update_postimage"),
            Row(2L, "delete"),
            Row(4L, "insert")
          )
        ),
        bag(fixed)
      )
      assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
    },
    prepare = partitionSource
  )

  @Test
  def partitionedAlternateNamedAliasCannotBorrowSource(): Unit = fixture(
    (spark, _) => {
      val before = rows(spark, "a"); val beforeFiles = files("a")
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertEquals(4L, version("a")); assertEquals(beforeFiles, files("a"));
      assertEquals(before, rows(spark, "a"))
    },
    prepare = partitionSource,
    alias = true
  )

  @Test
  def stalePartitionedSourceReceiptRejectsBeforeFiles(): Unit = fixture(
    (spark, _) => {
      val before = rows(spark); val beforeFiles = files()
      val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
      val receipt = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c.receipt }.get
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val key = "delta.registration.receipt.nativeId"
      val original = registration.properties.get(key)
      registration.properties.put(key, "stale-partition-source")
      var checkpoint = 3L
      try {
        assertThrows(
          classOf[SecurityException],
          () => { query.executedPlan.executeCollect(); checkpoint = 4L }
        )
        assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
        assertEquals(3L, checkpoint); assertEquals(0L, version("b"));
        assertEquals(beforeFiles, files())
      } finally registration.properties.put(key, original)
      assertEquals(before, rows(spark))
    },
    prepare = partitionSource
  )

  @Test
  def postFileAuthorityDriftCannotPublishPartitionedRows(): Unit = {
    Seq(
      "target-select",
      "target-update",
      "source-policy",
      "subject",
      "settings",
      "target-registration",
      "source-registration"
    ).foreach { change =>
      fixture { (spark, plugin) =>
        val before = rows(spark); val beforeFiles = files(); val beforeVersion = version("b")
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val source = NativeSelectFixture.bindingFixture(spark, "a")
        val refresh = registration.onRead
        val login = UserGroupInformation.getLoginUser
        val setting = "spark.sql.shuffle.partitions"
        val oldSetting = spark.sessionState.conf.getAllConfs.get(setting)
        val oldTarget = registration.properties.get("delta.registration.receipt.nativeId")
        val oldSource = source.properties.get("delta.registration.receipt.nativeId")
        var writing = false; var changed = false; var checkpoint = 3L
        registration.onRead = n => {
          refresh(n)
          val writer =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          writing ||= writer != null
          if (writing && writer == null && !changed && files().diff(beforeFiles).nonEmpty) {
            changed = true
            change match {
              case "target-select" => policies.publish(plugin, deniedSelect = Seq("id"))
              case "target-update" => policies.publish(plugin, deniedUpdate = Seq("region"))
              case "source-policy" => policies.publish(plugin, sourceDeniedSelect = Seq("id"))
              case "subject" =>
                UserGroupInformation.setLoginUser(
                  UserGroupInformation.createRemoteUser("partition-drift")
                )
              case "settings" => spark.conf.set(setting, "3")
              case "target-registration" =>
                registration.properties.put("delta.registration.receipt.nativeId", "foreign-target")
              case "source-registration" =>
                source.properties.put("delta.registration.receipt.nativeId", "foreign-source")
            }
          }
        }
        try {
          assertThrows(
            classOf[Exception],
            () => { spark.sql(sql).collect(); checkpoint = 4L },
            change
          )
          assertTrue(changed, change); assertEquals(3L, checkpoint, change);
          assertEquals(beforeVersion, version("b"), change)
          assertTrue(
            files().diff(beforeFiles).nonEmpty,
            "must create native orphan files before denial: " + change
          )
        } finally {
          registration.onRead = refresh
          UserGroupInformation.setLoginUser(login)
          oldSetting match {
            case Some(v) => spark.conf.set(setting, v); case None => spark.conf.unset(setting)
          }
          registration.properties.put("delta.registration.receipt.nativeId", oldTarget)
          source.properties.put("delta.registration.receipt.nativeId", oldSource)
          policies.publish(plugin)
        }
        assertEquals(before, rows(spark), change)
      }
    }
  }

  @Test
  def acknowledgementRecoveryAndKnownDuplicateNeverRewritePartitions(): Unit = fixture {
    (spark, plugin) =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "partition-recovery")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
      val receipt = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c.receipt }.get
      val journal = field[Path](receipt.journal, "file")
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var lost = false; var checkpoint = 3L
      registration.onRead = n => {
        if (!lost && DeltaRangerMergeCommit.current.nonEmpty && version("b") == 1L) {
          lost = true; throw new IllegalStateException("partition acknowledgement lost")
        }
        refresh(n)
      }
      assertThrows(
        classOf[Exception],
        () => { query.executedPlan.executeCollect(); checkpoint = 4L }
      )
      assertTrue(lost); assertEquals(3L, checkpoint); assertEquals(1L, version("b"))
      assertEquals(
        "COMMITTED",
        mapper.readTree(Files.readAllBytes(journal)).get("state").textValue()
      )
      val afterFiles = files()
      registration.onRead = refresh
      val committed = rows(spark)
      assertEquals(
        bag(Vector(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep"))),
        committed
      )
      registration.onRead = n => {
        assertNull(
          field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
        )
        assertNull(field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get())
        refresh(n)
      }
      spark.sql(sql).collect(); checkpoint = 4L
      assertEquals(4L, checkpoint);
      assertEquals(
        "RECONCILED",
        mapper.readTree(Files.readAllBytes(journal)).get("state").textValue()
      )
      spark.sql(sql).collect()
      assertEquals(1L, version("b")); assertEquals(afterFiles, files());
      assertEquals(committed, rows(spark))
      policies.publish(plugin, deniedUpdate = Seq("region"))
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertEquals(1L, version("b")); assertEquals(afterFiles, files())
      policies.publish(plugin); assertEquals(committed, rows(spark))
      assertThrows(classOf[SecurityException], () => receipt.validate())
  }

  @Test
  def unknownPartitionPublicationNeverReplaysAfterReauthorization(): Unit = fixture {
    (spark, plugin) =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "partition-unknown")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      val before = rows(spark)
      val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
      val receipt = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c.receipt }.get
      val journal = field[Path](receipt.journal, "file")
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var revoked = false; var checkpoint = 3L
      registration.onRead = n => {
        refresh(n)
        if (!revoked && DeltaRangerMergeCommit.current.nonEmpty) {
          revoked = true; policies.publish(plugin, deniedUpdate = Seq("region"))
        }
      }
      assertThrows(
        classOf[SecurityException],
        () => { query.executedPlan.executeCollect(); checkpoint = 4L }
      )
      assertTrue(revoked); assertEquals(3L, checkpoint); assertEquals(0L, version("b"))
      assertEquals("UNKNOWN", mapper.readTree(Files.readAllBytes(journal)).get("state").textValue())
      val afterFiles = files()
      registration.onRead = refresh; policies.publish(plugin)
      assertThrows(classOf[SecurityException], () => { spark.sql(sql).collect(); checkpoint = 4L })
      assertEquals(3L, checkpoint); assertEquals(0L, version("b"));
      assertEquals(afterFiles, files()); assertEquals(before, rows(spark))
  }

  @Test
  def submittedPathsAndCdcMarkersRemainBoundWithPartitionMapsFixed(): Unit = fixture { (spark, _) =>
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var inspected = false; var proof: String = null
    registration.onRead = n => {
      refresh(n)
      DeltaRangerMergeCommit.current.foreach { owner =>
        if (!inspected) {
          inspected = true
          val actions = field[Seq[Action]](owner, "actions")
          val intent = field[AnyRef](owner, "intent")
          proof = field[String](intent, "encoded")
          val operation = field[DeltaOperations.Merge](owner, "nativeOperation")
          val method = intent.getClass.getDeclaredMethods.find(_.getName == "requireArguments").get
          method.setAccessible(true)
          assertTrue(actions.exists(_.isInstanceOf[AddFile]));
          assertTrue(actions.exists(_.isInstanceOf[RemoveFile]));
          assertTrue(actions.exists(_.isInstanceOf[AddCDCFile]))
          actions.zipWithIndex.foreach { case (action, index) =>
            val changed: Seq[Action] = action match {
              case a: AddFile =>
                Seq(a.copy(path = "foreign.parquet"), AddCDCFile(a.path, a.partitionValues, a.size))
              case a: RemoveFile => Seq(a.copy(path = "foreign.parquet"))
              case a: AddCDCFile =>
                Seq(
                  a.copy(path = "foreign.parquet"),
                  AddFile(a.path, a.partitionValues, a.size, 0L, true)
                )
              case _ => Nil
            }
            changed.foreach { candidate =>
              val failure = assertThrows(
                classOf[java.lang.reflect.InvocationTargetException],
                () => {
                  method.invoke(
                    intent,
                    actions.updated(index, candidate),
                    operation,
                    Map.empty[String, String]
                  ); ()
                }
              )
              assertTrue(failure.getCause.isInstanceOf[SecurityException])
            }
          }
        }
      }
    }
    spark.sql(sql).collect(); assertTrue(inspected)
    val log = Paths
      .get(NativeSelectFixture.locations("b"), "_delta_log", "00000000000000000001.json")
    val bytes = Files.readAllBytes(log)
    DeltaRangerMergeCommit.verifyPublication(bytes, proof)
    def serialize(lines: Array[JsonNode]): Array[Byte] =
      (lines.map(_.toString).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
    val original = new String(bytes, StandardCharsets.UTF_8).split("\n").map(mapper.readTree)
    DeltaRangerMergeCommit.verifyPublication(serialize(original), proof)
    Seq("add" -> "cdc", "cdc" -> "add").foreach { case (from, to) =>
      val lines = new String(bytes, StandardCharsets.UTF_8).split("\n").map(mapper.readTree)
      val line = lines.find(_.has(from)).get.asInstanceOf[ObjectNode]
      val payload = line.remove(from)
      line.set[JsonNode](to, payload)
      val forged = serialize(lines)
      val failure = assertThrows(
        classOf[SecurityException],
        () => DeltaRangerMergeCommit.verifyPublication(forged, proof)
      )
      assertEquals(
        "Delta MERGE commit rejected: published actions differ from submitted intent",
        failure.getMessage
      )
    }
  }
}
