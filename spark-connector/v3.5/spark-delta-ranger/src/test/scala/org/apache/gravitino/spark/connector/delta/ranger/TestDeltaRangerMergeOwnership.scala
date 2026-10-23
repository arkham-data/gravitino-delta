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

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream,
  NotSerializableException
}
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.commands.MergeIntoCommand
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndexWithSnapshotDescriptor}
import org.apache.spark.sql.execution.LogicalRDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Materialized task data cannot confer another driver's MERGE authority. */
class TestDeltaRangerMergeOwnership {
  import TestDeltaRangerMergeSource._

  @Test
  def copiedMaterializationCannotBorrowMergeAuthority(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      val registration = NativeSelectFixture.bindingFixture(spark, "b")
      val refresh = registration.onRead
      var inspected = false
      var indexInspected = false
      registration.onRead = n => {
        refresh(n)
        val native =
          field[ThreadLocal[DeltaRangerMergeNative]](DeltaRangerMergeNative, "active").get()
        if (
          native != null && !indexInspected && field[DeltaRangerMergeNative.OwnedIndex](
            native,
            "wrappedIndex"
          ) != null
        ) {
          indexInspected = true
          val index = field[TahoeBatchFileIndex](native, "targetIndex")
          val copied = new DeltaRangerMergeNative.OwnedIndex(native, index, spark)
          assertThrows(classOf[SecurityException], () => copied.matchingFiles(Nil, Nil))
        }
        val write = field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
        if (write != null && !inspected) {
          inspected = true
          val source = field[DeltaRangerMergeSource.Attempt](write, "source")
          val receipt = field[DeltaRangerMergeReceipt](write, "receipt")
          val logical = source.logical.asInstanceOf[LogicalRDD]
          assertFalse(source.owns(logical.copy()(spark, None, None)))
          assertFalse(
            source.owns(
              logical.copy(rdd = spark.sparkContext.emptyRDD[InternalRow])(spark, None, None)
            )
          )
          assertThrows(
            classOf[SecurityException],
            () => source.returned(logical, Some(logical.rdd))
          )
          val carrier = field[DeltaRangerMergeSource.Carrier](source, "carrier")
          assertThrows(classOf[SecurityException], () => carrier.copy()(source).owner)
          val restored = serialized(carrier).asInstanceOf[DeltaRangerMergeSource.Carrier]
          assertThrows(classOf[SecurityException], () => restored.owner)
          assertThrows(classOf[NotSerializableException], () => serialized(source))
          assertThrows(classOf[NotSerializableException], () => serialized(receipt))
          val command = field[DeltaRangerMergeCommand](receipt, "command")
          val original = field[MergeIntoCommand](command, "native")
          assertThrows(classOf[SecurityException], () => command.copy(original)(receipt).receipt)
          assertThrows(classOf[SecurityException], () => receipt.openSourceAttempt(source, 2))
          val producer = field[org.apache.spark.sql.execution.SparkPlan](receipt.cdf, "physical")
            .asInstanceOf[DeltaRangerCdfProducer]
          assertThrows(
            classOf[SecurityException],
            () => receipt.cdf.requireProducer(producer.copy()(receipt.cdf))
          )
          val scans = field[DeltaRangerCdfReadScans](receipt.cdf, "scans")
          val sourceIndex = DeltaRangerPhysicalProvenance
            .nodes(producer.child)
            .collectFirst { case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
              scan.relation.location.asInstanceOf[DeltaRangerCdfReadScans.OwnedIndex]
            }
            .get
          val originalIndex = field[TahoeFileIndexWithSnapshotDescriptor](sourceIndex, "original")
          val foreignIndex = new DeltaRangerCdfReadScans.OwnedIndex(scans, originalIndex, spark)
          assertThrows(classOf[SecurityException], () => foreignIndex.matchingFiles(Nil, Nil))
          val other = spark.newSession()
          try {
            SparkSession.setActiveSession(other)
            assertThrows(classOf[SecurityException], () => source.validate())
          } finally SparkSession.setActiveSession(spark)
          UserGroupInformation
            .createRemoteUser("foreign-merge-user")
            .doAs(new PrivilegedExceptionAction[Unit] {
              override def run(): Unit = {
                assertThrows(classOf[SecurityException], () => source.validate()); ()
              }
            })
          source.validate()
        }
      }
      spark.sql(sql).collect()
      assertTrue(inspected && indexInspected)
      assertEquals(1L, version("b"))
    }

  @Test
  def ordinaryCdfReceiptCannotUseNullMergeOwnerToReopenExecution(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      val frame = fixture.read(spark)
      frame.collect()
      val carrier = frame.queryExecution.optimizedPlan.asInstanceOf[DeltaRangerCdfRead.Carrier]
      val receipt = carrier.receipt
      assertThrows(classOf[SecurityException], () => receipt.closeMergeExecution(null))
      assertThrows(classOf[SecurityException], () => receipt.openMergeExecution(null))
      assertThrows(classOf[SecurityException], () => receipt.plan(carrier))
    }
  }

  @Test
  def userCachePersistAndCheckpointDoNotAcquireMergeAuthority(): Unit =
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
      Seq("cache", "persist", "checkpoint", "localCheckpoint").foreach { action =>
        assertThrows(
          classOf[Exception],
          () => {
            val source = spark.sql("SELECT * FROM table_changes('a.db.t', 0, 4)")
            action match {
              case "cache"           => source.cache()
              case "persist"         => source.persist()
              case "checkpoint"      => source.checkpoint(eager = true)
              case "localCheckpoint" => source.localCheckpoint(eager = true)
            }
            ()
          }
        )
      }
      assertEquals(0L, version("b"))
    }

  @Test
  def staleSourceAndTargetRegistrationRejectMaterializedData(): Unit = {
    for (catalog <- Seq("a", "b"); mutation <- Seq("nativeId", "schema")) {
      new TestDeltaRangerInstalledMerge().withPolicies { (spark, _) =>
        val registration = NativeSelectFixture.bindingFixture(spark, "b")
        val changed = NativeSelectFixture.bindingFixture(spark, catalog)
        val refresh = registration.onRead
        var inspected = false
        registration.onRead = n => {
          refresh(n)
          val write =
            field[ThreadLocal[DeltaRangerMergeWrite]](DeltaRangerMergeWrite, "active").get()
          if (write != null && !inspected) {
            inspected = true
            mutation match {
              case "nativeId" =>
                changed.properties.put("delta.registration.receipt.nativeId", "foreign-native-id")
              case "schema" => changed.columns = changed.columns.dropRight(1)
            }
          }
        }
        assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
        assertTrue(inspected)
        assertEquals(0L, version("b"))
      }
    }
  }

  private def serialized(value: AnyRef): AnyRef = {
    val bytes = new ByteArrayOutputStream
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
    try input.readObject()
    finally input.close()
  }
}
