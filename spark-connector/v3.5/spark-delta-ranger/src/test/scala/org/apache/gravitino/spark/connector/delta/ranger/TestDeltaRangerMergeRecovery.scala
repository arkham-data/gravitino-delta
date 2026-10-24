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
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Durable retry must prove the previous publication without executing MERGE twice. */
class TestDeltaRangerMergeRecovery {
  import TestDeltaRangerMergeSource._

  @Test
  def duplicateRequiresAuthorizationAndIdenticalIntent(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "merge-consumer")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "4")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
      assertEquals(Seq(Row(3L, 1L, 1L, 1L)), spark.sql(sql).collect().toSeq)
      assertEquals(1L, version("b"))
      spark.sql(sql).collect()
      assertEquals(1L, version("b"))
      assertThrows(classOf[SecurityException], () => spark.sql(sql.replace("0, 4", "1, 4")))
      assertThrows(
        classOf[SecurityException],
        () => spark.sql(sql.replace("secret = s.secret", "secret = 'different'"))
      )
      new TestDeltaRangerMergePolicies().publish(plugin, deniedUpdate = Seq("secret"))
      assertThrows(classOf[SecurityException], () => spark.sql(sql))
      assertEquals(1L, version("b"))
    }
  @Test
  def commitAcknowledgementFailureReconcilesWithoutReplay(): Unit = acknowledgement(false, false)

  @Test
  def reconciliationAcknowledgementFailureReconcilesWithoutReplay(): Unit =
    acknowledgement(true, false)

  @Test
  def sameTableAcknowledgementFailureReconcilesWithoutReplay(): Unit = acknowledgement(false, true)

  private def acknowledgement(afterReconciliation: Boolean, sameTable: Boolean): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
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

  @Test
  def missingUnknownAndMismatchedEvidenceNeverReplay(): Unit = {
    Seq("missing", "unknown", "digest", "actions", "native-log").foreach { mutation =>
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
        identity(spark)
        spark.sql(sql).collect()
        val files = dataFiles("b")
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
  def changedTargetInSharedJournalCannotReuseIdentity(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      identity(spark)
      spark.sql(sql).collect()
      val a = NativeSelectFixture.bindingFixture(spark, "a").wrapper
      val b = NativeSelectFixture.bindingFixture(spark, "b").wrapper
      val member = classOf[DeltaRangerCatalogSpark35].getDeclaredField("mergeJournalDirectory")
      member.setAccessible(true); member.set(a, b.mergeJournalDirectory())
      assertThrows(
        classOf[SecurityException],
        () => spark.sql(sql.replace("MERGE INTO b.db.t", "MERGE INTO a.db.t")).collect()
      )
      assertEquals(4L, version("a")); assertEquals(1L, version("b"))
    }

  @Test
  def incompletePairAndAutoResetRejectBeforeFiles(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val files = dataFiles("b")
      spark.conf.set("spark.databricks.delta.write.txnAppId", "consumer")
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "true")
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertEquals(0L, version("b")); assertEquals(files, dataFiles("b"))
    }

  @Test
  def interruptedPreparedAttemptCannotReplay(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      identity(spark)
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var revoked = false
      registration.onRead = n => {
        refresh(n)
        if (!revoked && DeltaRangerMergeCommit.current.nonEmpty) {
          revoked = true
          new TestDeltaRangerMergePolicies().publish(plugin, deniedUpdate = Seq("secret"))
        }
      }
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertTrue(revoked)
      assertEquals(0L, version("b"))
      assertEquals("UNKNOWN", record(spark, "b").get("state").textValue())
      val files = dataFiles("b")
      registration.onRead = refresh
      new TestDeltaRangerMergePolicies().publish(plugin)
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertEquals(0L, version("b")); assertEquals(files, dataFiles("b"))
    }

  @Test
  def staleRegistrationCannotClaimRecoverySuccess(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      identity(spark)
      spark.sql(sql).collect()
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      registration.properties.put("delta.registration.receipt.nativeVersion", "0")
      registration.onRead = _ => ()
      assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertEquals(1L, version("b"))
    }

  @Test
  def duplicateAfterCompatibleTargetAdvancementPreservesLaterRows(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      identity(spark)
      spark.sql(sql).collect()
      spark.conf.set("spark.databricks.delta.write.txnVersion", "8")
      spark.sql(sql.replace("_commit_version = 4", "_commit_version = 1")).collect()
      assertEquals(2L, version("b"))
      val rows = spark.table("b.db.t").collect().toSeq.groupBy(identityRow).map {
        case (r, copies) => r -> copies.size
      }
      assertEquals(1, rows(Row(11L, "history", "keep")))
      identity(spark)
      val files = dataFiles("b")
      spark.sql(sql).collect()
      assertEquals(2L, version("b"))
      assertEquals(files, dataFiles("b"))
      assertEquals(
        rows,
        spark.table("b.db.t").collect().toSeq.groupBy(identityRow).map { case (r, copies) =>
          r -> copies.size
        }
      )
    }

  @Test
  def finalRecoveryReadbackRechecksSourcePolicy(): Unit = finalRecoveryPolicy(sourcePolicy = true)

  @Test
  def finalRecoveryReadbackRechecksTargetPolicy(): Unit = finalRecoveryPolicy(sourcePolicy = false)

  private def finalRecoveryPolicy(sourcePolicy: Boolean): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      identity(spark)
      spark.sql(sql).collect()
      val files = dataFiles("b")
      val query = spark.sessionState.executePlan(spark.sessionState.sqlParser.parsePlan(sql))
      val command = query.analyzed.collectFirst { case c: DeltaRangerMergeCommand => c }.get
      val receipt = command.receipt
      val registration = NativeSelectFixture.bindingFixture(spark, if (sourcePolicy) "a" else "b")
      val refresh = registration.onRead
      var downloaded = false
      var checkpoint = 3L
      registration.onRead = n => {
        refresh(n)
        if (!downloaded && field[Boolean](receipt, "sourceClosed")) {
          downloaded = true
          assertTrue(DeltaRangerMergeCommit.current.isEmpty)
          if (sourcePolicy)
            new TestDeltaRangerMergePolicies().publish(plugin, sourceDeniedSelect = Seq("id"))
          else new TestDeltaRangerMergePolicies().publish(plugin, deniedUpdate = Seq("secret"))
        }
      }
      val failure =
        try {
          query.executedPlan.executeCollect()
          checkpoint = 4L
          None
        } catch { case error: Throwable => Some(error) }
      assertTrue(downloaded)
      assertEquals(3L, checkpoint)
      assertTrue(failure.exists(_.isInstanceOf[SecurityException]), failure.toString)
      assertEquals(1L, version("b")); assertEquals(files, dataFiles("b"))
      assertThrows(classOf[SecurityException], () => receipt.validate())
    }

  @Test
  def concurrentJournalClaimRejectsSecondIntent(): Unit = {
    import java.nio.file.LinkOption
    import java.nio.file.attribute.PosixFilePermissions
    import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
    import java.util.concurrent.atomic.AtomicInteger
    import org.mockito.Mockito
    import scala.util.Try
    val directory = Files.createTempDirectory(
      "merge-journal-race",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    val executor = Executors.newFixedThreadPool(2)
    try {
      val commands = Vector("target-one", "target-two").map(DeltaRangerMergeJournal.hash)
      val journals = commands.map(command =>
        new DeltaRangerMergeJournal(directory, Some("shared-app" -> 1L), command)
      )
      val file = field[Path](journals.head, "file")
      val gate = new CyclicBarrier(2)
      val absent = new AtomicInteger()
      val futures = journals.zipWithIndex.map { case (journal, index) =>
        executor.submit(new Callable[Try[Unit]] {
          override def call(): Try[Unit] = {
            val scope = Mockito.mockStatic(
              classOf[Files],
              Mockito
                .withSettings()
                .mockMaker("mock-maker-inline")
                .defaultAnswer(Mockito.CALLS_REAL_METHODS)
            )
            try {
              scope
                .when[java.lang.Boolean](() => {
                  Files.exists(file, LinkOption.NOFOLLOW_LINKS); ()
                })
                .thenAnswer(invocation => {
                  val observed = invocation.callRealMethod().asInstanceOf[java.lang.Boolean]
                  assertFalse(observed.booleanValue())
                  absent.incrementAndGet()
                  gate.await(20, TimeUnit.SECONDS)
                  observed
                })
              Try(journal.prepare("{\"actions\":[]}", index + 1L))
            } finally scope.close()
          }
        })
      }
      val outcomes = futures.map(_.get(30, TimeUnit.SECONDS))
      assertEquals(2, absent.get(), outcomes.toString)
      assertEquals(1, outcomes.count(_.isSuccess), "exactly one initial durable claim may win")
      assertEquals(1, outcomes.count(_.isFailure))
      val winner = outcomes.indexWhere(_.isSuccess)
      val before = Files.readAllBytes(file).toVector
      journals.zip(outcomes).foreach { case (journal, outcome) =>
        if (outcome.isFailure) journal.unknown()
      }
      assertEquals(
        before,
        Files.readAllBytes(file).toVector,
        "loser cleanup must not alter winning evidence"
      )
      val reopened =
        new DeltaRangerMergeJournal(directory, Some("shared-app" -> 1L), commands(winner))
      assertEquals(commands(winner), reopened.existing.get.command)
      assertEquals(DeltaRangerMergeJournal.Prepared, reopened.existing.get.state)
      assertEquals(
        PosixFilePermissions.fromString("rw-------"),
        Files.getPosixFilePermissions(file)
      )
      assertThrows(
        classOf[SecurityException],
        () => new DeltaRangerMergeJournal(directory, Some("shared-app" -> 1L), commands(1 - winner))
      )
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(30, TimeUnit.SECONDS)
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
      finally paths.close()
    }
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
