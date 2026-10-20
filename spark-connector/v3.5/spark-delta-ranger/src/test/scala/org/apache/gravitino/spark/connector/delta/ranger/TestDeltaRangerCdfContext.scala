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

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.when

/** Revalidates context after registration callbacks and binds the complete native log segment. */
class TestDeltaRangerCdfContext {
  private val sql = "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"

  @Test
  def commandRechecksSubjectSettingsAndPolicyAfterFinalMetadataCallback(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val before = manifest(NativeSelectFixture.locations("a"))
      Seq("settings", "subject", "policy").foreach { mode =>
        val command =
          spark.sql(sql).queryExecution.analyzed.asInstanceOf[DeltaRangerCdfPropertyCommand.Carrier]
        var changed = false
        fixture.onRead = _ => if (!changed) { changed = true; change(spark, policy, mode) }
        try assertThrows(classOf[SecurityException], () => command.owner.requireCurrent(), mode)
        finally {
          fixture.onRead = _ => ()
          restore(spark)
        }
        assertTrue(changed)
      }
      assertEquals(0, CdfAlterForwardingCatalog.alters.get())
      assertEquals(before, manifest(NativeSelectFixture.locations("a")))
    }

  @ParameterizedTest(name = "native owner rechecks {0} after metadata callback")
  @ValueSource(strings = Array("settings", "subject", "policy"))
  def capturedTransactionRechecksContextBeforePublication(mode: String): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val before = manifest(location)
      var exercised = false
      var rejected = false
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { owner =>
          val field =
            owner.getClass.getDeclaredFields.find(_.getType == classOf[OptimisticTransaction]).get
          field.setAccessible(true)
          if (!exercised && field.get(owner) != null) {
            exercised = true
            fixture.onRead = _ => change(spark, policy, mode)
            try owner.requireLive()
            catch { case _: SecurityException => rejected = true }
            finally fixture.onRead = _ => ()
          }
        }
      try assertThrows(classOf[Exception], () => { spark.sql(sql).collect(); () })
      finally { fixture.onRead = _ => (); restore(spark) }
      assertTrue(exercised)
      assertTrue(rejected, "Final registration callback must not escape native context validation")
      assertEquals(before, manifest(location), "Context rejection must precede publication")
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

  @Test
  def sameVersionSnapshotWithChangedLogSegmentIsRejected(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val log = DeltaLog.forTable(spark, location)
      val next = log.unsafeVolatileSnapshot.version + 1
      var exercised = false
      var rejected = false
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { owner =>
          val head = log.unsafeVolatileSnapshot
          if (!exercised && head.version == next) {
            exercised = true
            val delta = head.logSegment.deltas.last
            val originalPath = delta.getPath
            delta.setPath(new Path(originalPath.toString + ".foreign"))
            try owner.requireLive()
            catch { case _: SecurityException => rejected = true }
            finally delta.setPath(originalPath)
          }
        }
      spark.sql(sql).collect()
      assertTrue(exercised)
      assertTrue(rejected, "Same version and metadata do not prove the native log segment")
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

  private def change(
      spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      mode: String
  ): Unit =
    mode match {
      case "settings" => spark.conf.set("spark.sql.shuffle.partitions", "99")
      case "subject" =>
        spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-subject")
      case "policy" =>
        val nextVersion = policy.plugin.getPoliciesVersion + 1L
        when(policy.plugin.getPoliciesVersion).thenReturn(nextVersion)
        policy.start()
    }

  private def restore(spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
  }

  private def manifest(location: String): Map[String, Vector[Byte]] = {
    val root = Paths.get(location)
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(path => root.relativize(path).toString -> Files.readAllBytes(path).toVector)
        .toMap
    finally paths.close()
  }
}
