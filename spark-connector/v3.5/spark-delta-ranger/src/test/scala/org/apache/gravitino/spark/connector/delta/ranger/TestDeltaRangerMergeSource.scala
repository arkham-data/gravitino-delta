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

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Native block-loss retries must renew authority without renewing the bounded interval. */
class TestDeltaRangerMergeSource {
  import TestDeltaRangerMergeSource._

  @Test
  def blockLossRetryCreatesFreshAttempt(): Unit = retry(exhaust = false)

  @Test
  def exhaustedBlockLossRetainsNativeFailure(): Unit = retry(exhaust = true)

  @Test
  def failedNativeSelectionConstructionDoesNotPoisonThread(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
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
  def materializedSourceStillRevalidatesBeforeSecondPass(): Unit = {
    Seq(
      "source-revoke",
      "source-filter",
      "source-mask",
      "target-revoke",
      "target-filter",
      "target-mask"
    ).foreach { change =>
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
        val policies = new TestDeltaRangerMergePolicies
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var changed = false
        var saved: DeltaRangerMergeSource.Attempt = null
        registration.onRead = n => {
          refresh(n)
          val write =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (write != null && !changed) {
            changed = true
            saved = field[DeltaRangerMergeSource.Attempt](write, "source")
            assertTrue(saved.logical.asInstanceOf[LogicalRDD].rdd.isCheckpointed)
            change match {
              case "source-revoke" =>
                policies.publish(plugin, sourceDeniedSelect = Seq("id"))
              case "source-filter" =>
                policies.publish(plugin, sourceFilter = Some("region = 'drop'"))
              case "source-mask" => policies.publish(plugin, sourceMasks = Map("id" -> "MASK_NULL"))
              case "target-revoke" => policies.publish(plugin, deniedUpdate = Seq("secret"))
              case "target-filter" => policies.publish(plugin, filter = Some("region = 'drop'"))
              case "target-mask"   => policies.publish(plugin, masks = Map("id" -> "MASK_NULL"))
            }
          }
        }
        assertThrows(classOf[SecurityException], () => { spark.sql(sql).collect(); () }, change)
        assertTrue(changed, change)
        assertEquals(0L, version("b"), change)
        assertThrows(classOf[SecurityException], () => saved.validate())
      }
    }
  }

  @Test
  def blockLossRetryRechecksChangedPolicy(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
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
  def compatibleSourceAdvancementDoesNotExtendFixedInterval(): Unit = {
    import java.nio.file.{Files, Path, Paths}
    import org.apache.spark.sql.delta.DeltaLog
    var hidden: Path = null
    var published: Path = null
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var advanced = false
        registration.onRead = n => {
          refresh(n)
          val write =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (write != null && !advanced) {
            advanced = true
            Files.move(hidden, published)
          }
        }
        assertEquals(
          Seq(Row(3L, 1L, 1L, 1L)),
          spark.sql(sql.replace("_commit_version = 4", "_commit_version >= 4")).collect().toSeq
        )
        assertTrue(advanced)
        assertEquals(5L, version("a"))
        assertEquals(
          Set(Row(1L, "changed", "keep"), Row(3L, null, "keep"), Row(4L, "new", "keep")),
          spark.table("b.db.t").collect().toSet
        )
      },
      prepareTarget = spark => {
        val location = NativeSelectFixture.locations("a")
        spark
          .sql("SELECT 99L id, 'later' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .save(location)
        published = Paths.get(location, "_delta_log", "00000000000000000005.json")
        hidden = published.resolveSibling("task3-held-native-commit.json")
        Files.move(published, hidden)
        DeltaLog.clearCache()
      }
    )
  }

  @Test
  def unrelatedTargetAdvancementCannotBorrowSourceRole(): Unit = {
    import java.nio.file.{Files, Path, Paths}
    import org.apache.spark.sql.delta.DeltaLog
    var hidden: Path = null
    var published: Path = null
    new TestDeltaRangerInstalledMerge().withPolicies(
      (spark, _) => {
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val refresh = registration.onRead
        var advanced = false
        registration.onRead = n => {
          refresh(n)
          val write =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (write != null && !advanced) {
            advanced = true
            Files.move(hidden, published)
          }
        }
        assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
        assertTrue(advanced)
        assertEquals(1L, version("b"))
        assertFalse(
          new String(Files.readAllBytes(published), java.nio.charset.StandardCharsets.UTF_8)
            .contains("\"operation\":\"MERGE\"")
        )
      },
      prepareTarget = spark => {
        val location = NativeSelectFixture.locations("b")
        spark
          .sql("SELECT 99L id, 'unrelated' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .save(location)
        published = Paths.get(location, "_delta_log", "00000000000000000001.json")
        hidden = published.resolveSibling("task3-held-target-commit.json")
        Files.move(published, hidden)
        DeltaLog.clearCache()
      }
    )
  }

  @Test
  def sameTableRetryClosesSourceBeforeOwnPublication(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val refresh = registration.onRead
      val seen = new java.util.IdentityHashMap[DeltaRangerMergeWrite, java.lang.Boolean]()
      val sources = ArrayBuffer.empty[DeltaRangerMergeSource.Attempt]
      var receipt: DeltaRangerMergeReceipt = null
      var closedBeforePublication = false
      registration.onRead = n => {
        refresh(n)
        val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (write != null && !seen.containsKey(write)) {
          seen.put(write, true)
          val source = field[DeltaRangerMergeSource.Attempt](write, "source")
          receipt = field[DeltaRangerMergeReceipt](write, "receipt")
          sources += source
          if (sources.size == 1)
            source.logical.asInstanceOf[LogicalRDD].rdd.unpersist(blocking = true)
        }
        if (
          receipt != null && field[Boolean](receipt, "sourceClosed") && !closedBeforePublication
        ) {
          closedBeforePublication = true
          assertEquals(4L, version("a"))
          assertThrows(classOf[SecurityException], () => receipt.cdf.validateAction(spark))
          sources.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
        }
      }
      assertEquals(
        Seq(Row(2L, 2L, 0L, 0L)),
        spark.sql(sql.replace("MERGE INTO b.db.t", "MERGE INTO a.db.t")).collect().toSeq
      )
      assertEquals(2, sources.size)
      assertTrue(closedBeforePublication)
      assertEquals(5L, version("a"))
    }

  private def retry(exhaust: Boolean): Unit = new TestDeltaRangerInstalledMerge().withPolicies {
    (spark, _) =>
      val unrelated = spark.sparkContext.parallelize(Seq(9, 10), 1).cache()
      assertEquals(2L, unrelated.count())
      val sources = ArrayBuffer.empty[DeltaRangerMergeSource.Attempt]
      val plans = ArrayBuffer.empty[LogicalRDD]
      val roots = ArrayBuffer.empty[DeltaRangerMergeSource.SourceExec]
      val producers = ArrayBuffer.empty[DeltaRangerCdfProducer]
      val observed = new java.util.IdentityHashMap[DeltaRangerMergeWrite, java.lang.Boolean]()
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var rejectedOldProducerDuringNewMaterialization = false
      registration.onRead = n => {
        refresh(n)
        val materializing =
          field[ThreadLocal[DeltaRangerMergeSource.Attempt]](DeltaRangerMergeSource, "active").get()
        if (
          materializing != null && sources.nonEmpty && sources.last.isClosed && !rejectedOldProducerDuringNewMaterialization
        ) {
          rejectedOldProducerDuringNewMaterialization = true
          producers.foreach { old =>
            val failure =
              assertThrows(classOf[SecurityException], () => old.receipt.requireProducer(old))
            assertTrue(
              failure.getMessage.contains("copied CDF physical carrier"),
              failure.getMessage
            )
          }
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
          producers.foreach(old =>
            assertThrows(classOf[SecurityException], () => old.receipt.requireProducer(old))
          )
          plans.foreach(old => assertFalse(source.ownsRetained(old)))
          val root = field[DeltaRangerMergeSource.SourceExec](source, "root")
          roots += root
          producers += DeltaRangerPhysicalProvenance
            .nodes(root.child)
            .collectFirst { case producer: DeltaRangerCdfProducer =>
              producer
            }
            .get
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
        assertTrue(rejectedOldProducerDuringNewMaterialization)
        sources.foreach(old => assertThrows(classOf[SecurityException], () => old.validate()))
        plans.foreach(plan =>
          assertFalse(spark.sparkContext.getPersistentRDDs.contains(plan.rdd.id))
        )
        assertTrue(spark.sparkContext.getPersistentRDDs.contains(unrelated.id))
        assertEquals(2L, unrelated.count())
      } finally unrelated.unpersist(blocking = true)
  }
}

private[ranger] object TestDeltaRangerMergeSource {
  val sql = """MERGE INTO b.db.t t USING
      |(SELECT * FROM table_changes('a.db.t', 0, 4)
      | WHERE _commit_version = 4 AND _change_type <> 'update_preimage') s
      |ON t.id = s.id
      |WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
      |WHEN MATCHED THEN UPDATE SET secret = s.secret, region = s.region
      |WHEN NOT MATCHED AND s._change_type <> 'delete'
      |THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
      |""".stripMargin
  def field[A](value: AnyRef, name: String): A = {
    val member = value.getClass.getDeclaredFields
      .find(f => f.getName == name || f.getName.endsWith("$$" + name))
      .get
    member.setAccessible(true)
    member.get(value).asInstanceOf[A]
  }
  def version(name: String): Long = {
    import java.nio.file.{Files, Paths}
    import scala.collection.JavaConverters._
    val files = Files.list(Paths.get(NativeSelectFixture.locations(name), "_delta_log"))
    try
      files
        .iterator()
        .asScala
        .map(_.getFileName.toString)
        .filter(_.matches("[0-9]{20}\\.json"))
        .map(_.stripSuffix(".json").toLong)
        .max
    finally files.close()
  }
}
