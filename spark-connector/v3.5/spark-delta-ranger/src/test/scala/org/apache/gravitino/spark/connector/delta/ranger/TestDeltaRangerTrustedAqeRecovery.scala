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
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Stock adaptive execution retains native retry and durable recovery boundaries. */
class TestDeltaRangerTrustedAqeRecovery {
  import TestDeltaRangerMergeSource._

  @Test
  def nativeBlockLossRetry(): Unit = retry(exhaust = false)
  @Test
  def nativeRetryExhaustion(): Unit = retry(exhaust = true)
  @Test
  def knownCommitRechecksWithoutReplay(): Unit = acknowledgement(false, false)
  @Test
  def publicationFailureRetainsRecoveryState(): Unit = acknowledgement(true, false)
  @Test
  def policyChangedBeforeRetry(): Unit =
    withPolicies { (spark, plugin) =>
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var saved: DeltaRangerMergeSource.Attempt = null
      var command: DeltaRangerMergeCommand = null
      var changed = false
      registration.onRead = n => {
        refresh(n)
        val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (saved == null && write != null) {
          saved = field[DeltaRangerMergeSource.Attempt](write, "source")
          command = field[DeltaRangerMergeCommand](
            field[DeltaRangerMergeReceipt](write, "receipt"),
            "command"
          )
          saved.logical.asInstanceOf[LogicalRDD].rdd.unpersist(blocking = true)
        } else if (saved != null && saved.isClosed && !changed) {
          changed = true
          assertEquals(2, field[Int](command, "attempt"))
          new TestDeltaRangerMergePolicies().publish(plugin, sourceMasks = Map("id" -> "MASK_NULL"))
        }
      }
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertTrue(changed)
      assertEquals(0L, version("b"))
      assertThrows(classOf[SecurityException], () => saved.validate())
    }

  @Test
  def failedOperationDoesNotAffectNextCommand(): Unit =
    withPolicies { (spark, plugin) =>
      val policies = new TestDeltaRangerMergePolicies
      val thread = Thread.currentThread()
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var changed = false
      registration.onRead = n => {
        refresh(n)
        // Fail the constructor's real policy readback, independently of owner activation.
        val constructing = Thread
          .currentThread()
          .getStackTrace
          .exists(frame =>
            frame.getClassName == classOf[DeltaRangerMergeNative].getName &&
              frame.getMethodName == "<init>"
          )
        if (!changed && constructing) {
          changed = true
          policies.publish(plugin, sourceDeniedSelect = Seq("id"))
        }
      }
      val failure = assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertTrue(changed, "policy change did not reach native selection construction")
      assertTrue(
        failure.getStackTrace.exists(frame =>
          frame.getClassName == classOf[DeltaRangerMergeNative].getName &&
            frame.getMethodName == "<init>"
        ),
        failure.toString
      )
      assertEquals(0L, version("b"), "denied construction must not commit")
      registration.onRead = refresh
      policies.publish(plugin)
      assertSame(thread, Thread.currentThread())
      assertEquals(
        Set(Row(1L, "b-one", "keep"), Row(2L, "b-two", "drop"), Row(3L, null, "keep")),
        spark.table("b.db.t").collect().toSet
      )
      assertEquals(Seq(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toSeq)
      assertEquals(1L, version("b"))
      assertEquals(
        Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")),
        spark.table("b.db.t").collect().toSet
      )
    }

  @Test
  def independentlyAuthorizedCommandsKeepSeparateContexts(): Unit = withPolicies { (spark, _) =>
    val first = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
    val second = spark.sessionState.executePlan(
      spark.sessionState.sqlParser.parsePlan(sql.replace("MERGE INTO b.db.t", "MERGE INTO a.db.t"))
    )
    val firstCommand = first.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c }.get
    val secondCommand = second.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c }.get
    assertNotSame(firstCommand.receipt, secondCommand.receipt)
    first.executedPlan.executeCollect()
    assertEquals(1L, version("b")); assertEquals(4L, version("a"))
    assertThrows(classOf[SecurityException], () => firstCommand.receipt.validate())
    assertThrows(classOf[SecurityException], () => second.executedPlan.executeCollect())
    assertEquals(1L, version("b")); assertEquals(4L, version("a"))
    // Delta updated its postcommit session metadata. A fresh independent command captures it.
    spark.sql(sql.replace("MERGE INTO b.db.t", "MERGE INTO a.db.t")).collect()
    assertEquals(1L, version("b")); assertEquals(5L, version("a"))
    assertThrows(classOf[SecurityException], () => secondCommand.receipt.validate())
    assertNull(DeltaRangerNativeOperation.current())
    assertNull(
      field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
    )
    assertEquals(
      Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")),
      spark.table("b.db.t").collect().toSet
    )
    assertEquals(
      Set(
        Row(1L, "changed", "keep"),
        Row(4L, "new", "keep"),
        Row(11L, "history", "keep"),
        Row(12L, "history", "keep"),
        Row(13L, "history", "keep")
      ),
      spark.table("a.db.t").collect().toSet
    )
  }

  @Test
  def changedIntentAndUnknownOutcomeReject(): Unit = {
    Seq("missing", "unknown", "digest", "actions", "native-log").foreach { mutation =>
      withPolicies { (spark, _) =>
        identity(spark)
        spark.sql(sql).collect()
        val files = dataFiles("b")
        assertThrows(
          classOf[SecurityException],
          () => spark.sql(sql.replace("0, 4", "1, 4")).collect()
        )
        assertThrows(
          classOf[SecurityException],
          () => spark.sql(sql.replace("secret = s.secret", "secret = 'different'")).collect()
        )
        val path = journalFile(spark, "b")
        val node = new ObjectMapper()
          .readTree(Files.readAllBytes(path))
          .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
        mutation match {
          case "missing" => Files.delete(path)
          case "unknown" =>
            node.put("state", "UNKNOWN");
            Files.write(path, node.toString.getBytes(StandardCharsets.UTF_8))
          case "digest" =>
            node.put("digest", "0" * 64);
            Files.write(path, node.toString.getBytes(StandardCharsets.UTF_8))
          case "native-log" =>
            val log = Paths.get(
              NativeSelectFixture.locations("b"),
              "_delta_log",
              "00000000000000000001.json"
            )
            val lines = Files.readAllLines(log).asScala.toVector
            val first = new ObjectMapper().readTree(lines.head)
            first
              .get("commitInfo")
              .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
              .put("txnId", java.util.UUID.randomUUID().toString)
            val nativePath = new org.apache.hadoop.fs.Path(log.toUri)
            val output =
              nativePath.getFileSystem(spark.sessionState.newHadoopConf()).create(nativePath, true)
            try
              output.write(
                (first.toString +: lines.tail)
                  .mkString("", "\n", "\n")
                  .getBytes(StandardCharsets.UTF_8)
              )
            finally output.close()
          case "actions" =>
            val proof = new ObjectMapper()
              .readTree(node.get("proof").textValue())
              .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            proof.putArray("actions")
            node.put("proof", proof.toString)
            Files.write(path, node.toString.getBytes(StandardCharsets.UTF_8))
        }
        assertThrows(classOf[SecurityException], () => { spark.sql(sql).collect(); () }, mutation)
        assertEquals(1L, version("b"), mutation)
        assertEquals(files, dataFiles("b"), mutation)
      }
    }
  }

  @Test
  def capturedCallbackCloseFailurePreservesPrimaryAndIsolation(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = null; policy.maskColumn = "unused"
    val fixture = new DeltaRangerCdfReadFixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val frame = fixture.read(spark)
          assertEquals(
            fixture.multiset(fixture.expected),
            fixture.multiset(frame.collect().toVector)
          )
          val index = DeltaRangerTrustedAqeFixture
            .nodes(frame.queryExecution.executedPlan)
            .collectFirst { case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
              scan.relation.location.asInstanceOf[DeltaRangerCdfReadScans.OwnedIndex]
            }
            .get
          val scans = field[DeltaRangerCdfReadScans](index, "owner")
          val original =
            field[org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor](
              index,
              "original"
            )
          Seq(false, true).foreach { bodyFails =>
            val primary = new IllegalStateException("injected callback body failure")
            val closing = new IllegalStateException("injected native scope close failure")
            var captured: DeltaRangerNativeOperation.CdfReadOwner = null
            var bodyCompleted = false
            val scope = org.mockito.Mockito.mockStatic(
              classOf[DeltaRangerNativeOperation],
              org.mockito.Mockito
                .withSettings()
                .mockMaker("mock-maker-inline")
                .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS)
            )
            try {
              scope
                .when[DeltaRangerNativeOperation](() => {
                  DeltaRangerNativeOperation.cdfRead(
                    org.mockito.ArgumentMatchers
                      .any(classOf[DeltaRangerNativeOperation.CdfReadOwner])
                  ); ()
                })
                .thenAnswer(invocation => {
                  captured = invocation.getArgument[DeltaRangerNativeOperation.CdfReadOwner](0)
                  val operation =
                    invocation.callRealMethod().asInstanceOf[DeltaRangerNativeOperation]
                  try {
                    val injected = org.mockito.Mockito.mock(
                      classOf[DeltaRangerNativeOperation],
                      org.mockito.Mockito
                        .withSettings()
                        .mockMaker("mock-maker-inline")
                        .spiedInstance(operation)
                        .defaultAnswer(org.mockito.Mockito.CALLS_REAL_METHODS)
                    )
                    org.mockito.Mockito
                      .doAnswer(invocation => {
                        invocation.callRealMethod()
                        assertNull(DeltaRangerNativeOperation.current())
                        throw closing
                      })
                      .when(injected)
                      .close()
                    val member = classOf[DeltaRangerNativeOperation].getDeclaredField("CURRENT")
                    member.setAccessible(true)
                    member
                      .get(null)
                      .asInstanceOf[ThreadLocal[DeltaRangerNativeOperation]]
                      .set(injected)
                    injected
                  } catch {
                    case failure: Throwable => operation.close(); throw failure
                  }
                })
              val failure = assertThrows(
                classOf[IllegalStateException],
                () => {
                  if (bodyFails) scans.matchingFiles(original, index, Nil) { throw primary }
                  else
                    scans.matchingFiles(
                      original,
                      index,
                      Seq(org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral)
                    ) {
                      val result = original.matchingFiles(
                        Seq(org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral),
                        Nil
                      )
                      assertTrue(result.isEmpty)
                      bodyCompleted = true
                      result
                    }
                  ()
                }
              )
              assertEquals(!bodyFails, bodyCompleted)
              assertSame(if (bodyFails) primary else closing, failure)
              if (bodyFails) assertEquals(Vector(closing), failure.getSuppressed.toVector)
              assertNotNull(captured)
              assertThrows(classOf[SecurityException], () => captured.requireLive())
              assertNull(DeltaRangerNativeOperation.current())
            } finally scope.close()
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(fixture.read(spark).collect().toVector)
            )
          }
        },
        Some(NativeSelectFixture.extension(policy)),
        startupOptions = Map("spark.sql.adaptive.enabled" -> "true"),
        beforeExtension = fixture.prepare
      )
    finally policy.engine.close()
  }

  private def withPolicies(test: (SparkSession, RangerBasePlugin) => Unit): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, plugin) => {
        PartitionedMergeFixture.origins(spark)
        test(spark, plugin)
      },
      startupOptions = DeltaRangerTrustedAqeFixture.coalescing +
        ("spark.sql.adaptive.enabled" -> "true")
    )

  private def retry(exhaust: Boolean): Unit = withPolicies { (spark, _) =>
    val unrelated = spark.sparkContext.parallelize(Seq(9, 10), 1).cache()
    assertEquals(2L, unrelated.count())
    val contexts = ArrayBuffer.empty[DeltaRangerExecutionContext.Context]
    val sources = ArrayBuffer.empty[DeltaRangerMergeSource.Attempt]
    val plans = ArrayBuffer.empty[LogicalRDD]
    val roots = ArrayBuffer.empty[DeltaRangerMergeSource.SourceExec]
    val observed = new java.util.IdentityHashMap[DeltaRangerMergeWrite, java.lang.Boolean]()
    val registration = NativeSelectFixture.bindingFixture(spark, "b")
    val refresh = registration.onRead
    var rejectedOldAttemptDuringNewMaterialization = false
    registration.onRead = n => {
      refresh(n)
      val materializing =
        field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
      if (
        materializing != null && sources.nonEmpty && sources.last.isClosed && !rejectedOldAttemptDuringNewMaterialization
      ) {
        rejectedOldAttemptDuringNewMaterialization = true
        assertThrows(classOf[SecurityException], () => sources.last.validate())
      }
      val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
      if (write != null && !observed.containsKey(write)) {
        observed.put(write, true)
        val source = field[DeltaRangerMergeSource.Attempt](write, "source")
        val logical = source.logical.asInstanceOf[LogicalRDD]
        sources.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
        assertTrue(logical.rdd.isCheckpointed)
        assertFalse(plans.exists(_.rdd eq logical.rdd))
        roots.foreach(old => assertThrows(classOf[SecurityException], () => old.owner))
        plans.foreach(old => assertFalse(source.ownsRetained(old)))
        val root = field[DeltaRangerMergeSource.SourceExec](source, "root")
        roots += root
        val receipt = field[DeltaRangerMergeReceipt](source, "receipt")
        assertEquals(DeltaRangerCdfRead.Request(0L, 4L), receipt.source.request)
        sources.foreach(old => assertSame(receipt, field[DeltaRangerMergeReceipt](old, "receipt")))
        val context = field[DeltaRangerExecutionContext.Context](source, "accepted")
        assertNotNull(context)
        assertFalse(contexts.exists(_ eq context))
        assertFalse(context.isInstanceOf[java.io.Serializable])
        contexts += context
        sources += source; plans += logical
        if (exhaust || sources.size == 1) logical.rdd.unpersist(blocking = true)
      }
    }
    try {
      if (exhaust) {
        val failure = assertThrows(classOf[Exception], () => spark.sql(sql).collect())
        assertTrue(
          failure.getMessage.contains("DELTA_MERGE_MATERIALIZE_SOURCE_FAILED_REPEATEDLY"),
          failure.toString
        )
        assertEquals(4, sources.size)
        assertEquals(0L, version("b"))
      } else {
        assertEquals(Seq(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toSeq)
        assertEquals(2, sources.size)
        assertEquals(
          Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")),
          spark.table("b.db.t").collect().toSet
        )
      }
      assertTrue(rejectedOldAttemptDuringNewMaterialization)
      assertEquals(if (exhaust) 0L else 1L, version("b"))
      assertEquals(4L, version("a"))
      sources.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
      plans.foreach(plan => assertFalse(spark.sparkContext.getPersistentRDDs.contains(plan.rdd.id)))
      assertTrue(spark.sparkContext.getPersistentRDDs.contains(unrelated.id))
      assertEquals(2L, unrelated.count())
    } finally unrelated.unpersist(blocking = true)
  }

  private def acknowledgement(afterReconciliation: Boolean, sameTable: Boolean): Unit =
    withPolicies { (spark, plugin) =>
      identity(spark)
      val target = if (sameTable) "a" else "b"
      val query = if (sameTable) sql.replace("MERGE INTO b.db.t", "MERGE INTO a.db.t") else sql
      val before = version(target)
      val unrelated = spark.sparkContext.parallelize(Seq(9, 10), 1).cache()
      assertEquals(2L, unrelated.count())
      val registration = NativeSelectFixture.bindingFixture(spark, target)
      val refresh = registration.onRead
      var failed = false
      var retired: DeltaRangerMergeReceipt = null
      var checkpoint = 3L
      registration.onRead = n => {
        val commit = DeltaRangerMergeCommit.current
        if (
          !failed && commit.nonEmpty && version(target) == before + 1 &&
          (!afterReconciliation || field[Boolean](
            field[DeltaRangerMergeReceipt](commit.get, "receipt"),
            "recached"
          ))
        ) {
          failed = true
          retired = field[DeltaRangerMergeReceipt](commit.get, "receipt")
          throw new IllegalStateException("injected acknowledgement loss")
        }
        refresh(n)
      }
      assertThrows(classOf[Exception], () => { spark.sql(query).collect(); checkpoint = 4L })
      assertTrue(failed)
      assertEquals(3L, checkpoint)
      assertEquals(before + 1, version(target))
      assertEquals("COMMITTED", record(spark, target).get("state").textValue())
      val files = dataFiles(target)
      registration.onRead = refresh
      new TestDeltaRangerMergePolicies().publish(plugin, deniedUpdate = Seq("secret"))
      assertThrows(classOf[SecurityException], () => spark.sql(query).collect())
      assertEquals(3L, checkpoint)
      assertEquals(before + 1, version(target))
      assertEquals(files, dataFiles(target))
      assertEquals("COMMITTED", record(spark, target).get("state").textValue())
      new TestDeltaRangerMergePolicies().publish(plugin)
      val rows = spark.table(target + ".db.t").collect().toSeq.groupBy(identityRow).map {
        case (r, copies) => r -> copies.size
      }
      registration.onRead = n => {
        assertNull(
          field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active")
            .get(),
          "recovery materialized source"
        )
        assertNull(
          field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get(),
          "recovery generated files"
        )
        refresh(n)
      }
      spark.sql(query).collect()
      checkpoint = 4L
      assertEquals(4L, checkpoint)
      assertEquals(before + 1, version(target))
      assertEquals(files, dataFiles(target))
      assertEquals(
        rows,
        spark.table(target + ".db.t").collect().toSeq.groupBy(identityRow).map { case (r, copies) =>
          r -> copies.size
        }
      )
      assertEquals("RECONCILED", record(spark, target).get("state").textValue())
      assertThrows(classOf[SecurityException], () => retired.validate())
      val source = field[DeltaRangerMergeSource.Attempt](retired, "sourceAttempt")
      val issued = field[org.apache.spark.rdd.RDD[_]](source, "issued")
      assertFalse(spark.sparkContext.getPersistentRDDs.contains(issued.id))
      assertTrue(spark.sparkContext.getPersistentRDDs.contains(unrelated.id))
      assertEquals(2L, unrelated.count())
      unrelated.unpersist(blocking = true)
    }

  private def identityRow(row: Row): Row = row
  private def identity(spark: SparkSession): Unit = {
    spark.conf.set("spark.databricks.delta.write.txnAppId", "recovery-consumer")
    spark.conf.set("spark.databricks.delta.write.txnVersion", "7")
    spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
  }
  private def journalFile(spark: SparkSession, target: String): Path = {
    val root = NativeSelectFixture
      .bindingFixture(spark, target)
      .wrapper
      .mergeJournalDirectory()
      .resolve("merge")
    val files = Files.list(root)
    try
      files.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toVector match {
        case Vector(file) => file
        case other =>
          fail("expected one durable MERGE intent: " + other.size); throw new AssertionError()
      }
    finally files.close()
  }
  private def record(spark: SparkSession, target: String) =
    new ObjectMapper().readTree(Files.readAllBytes(journalFile(spark, target)))
  private def dataFiles(target: String): Set[String] = {
    val files = Files.walk(Paths.get(NativeSelectFixture.locations(target)))
    try
      files
        .iterator()
        .asScala
        .filter(_.getFileName.toString.endsWith(".parquet"))
        .map(_.toString)
        .toSet
    finally files.close()
  }

}
