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
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerRetainedNativeValidation {
  private val tag =
    TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")

  @Test
  def retainedOperationCapturesExactCurrentLogAndKeepsPublicationLowerBoundSeparate(): Unit = {
    val operations = new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
    withHistory("append", operations) { (spark, policy, publish) =>
      val initial = captured(spark, "a")
      initial.validateAction(spark)
      publish("a", 1L)
      initial.validateAction(spark)
      val retained = initial.obligations.head.binding
      val head = retained.log().unsafeVolatileSnapshot
      assertEquals(1L, head.version)
      assertEquals(0L, retained.index().snapshotAtAnalysis.version)
      assertTrue(
        operations.asScala.exists(op => op.succeeded() && op.contains(retained.index(), head))
      )
      assertTrue(
        operations.asScala.forall(
          _.callback() == DeltaRangerNativeOperation.Callback.VALIDATE_RETAINED_BINDING
        )
      )
      assertNull(DeltaRangerNativeOperation.current())
      // Fresh load can admit snapshot 1 while the publication remains a valid lower bound of 0.
      val lagged = captured(spark, "a")
      val binding = lagged.obligations.head.binding
      assertEquals(1L, binding.nativeVersion())
      assertEquals(0L, binding.observation().nativeVersion())
      lagged.validateAction(spark)
      NativeSelectFixture
        .bindingFixture(spark, "a")
        .properties
        .put("delta.registration.receipt.nativeVersion", "1")
      lagged.validateAction(spark)
      val baselineOne = captured(spark, "a")
      baselineOne.validateAction(spark)
      NativeSelectFixture
        .bindingFixture(spark, "a")
        .properties
        .put("delta.registration.receipt.nativeVersion", "0")
      rejected(() => baselineOne.validateAction(spark))
      // The same numeric value is still compatible with the independently retained lower bound.
      lagged.validateAction(spark)
    }
  }

  @Test
  def publicationVersionDriftAcrossPolicyBatchRejectsEvenWithStableNewerHead(): Unit =
    withHistory("append") { (spark, policy, publish) =>
      val receipt = captured(spark, "a")
      publish("a", 1L)
      receipt.validateAction(spark)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      policy.onRequest =
        _ => fixture.properties.put("delta.registration.receipt.nativeVersion", "1")
      val failure = assertThrows(classOf[SecurityException], () => receipt.validateAction(spark))
      assertEquals("Delta action observation changed", failure.getMessage)
      policy.onRequest = _ => ()
      receipt.validateAction(spark)
    }

  @Test
  def innerPublicationDriftRejectsAndFailedNativeEvidenceCannotBeReused(): Unit =
    withHistory("append") { (spark, policy, publish) =>
      val receipt = captured(spark, "a")
      publish("a", 1L)
      receipt.validateAction(spark)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      var failed: DeltaRangerNativeOperation = null
      val afterRead = fixture.reads.get() + 2
      fixture.onRead = count =>
        if (count == afterRead) {
          failed = DeltaRangerNativeOperation.current()
          fixture.properties.put("delta.registration.receipt.nativeVersion", "1")
        }
      rejected(() => receipt.validateAction(spark))
      assertNotNull(failed)
      assertFalse(failed.succeeded())
      assertThrows(classOf[SecurityException], () => failed.requireEvidence(spark))
      assertNull(DeltaRangerNativeOperation.current())
      fixture.onRead = _ => ()
      receipt.validateAction(spark)
    }

  @Test
  def protocolOrderedPartitionsAndMappingChangesRejectWithLogicalSchemaUnchanged(): Unit =
    withHistory("metadata") { (spark, policy, publish) =>
      Seq(("a", Seq(2L)), ("b", Seq(1L)), ("q", Seq(1L, 2L, 3L))).foreach {
        case (catalog, versions) =>
          val receipt = captured(spark, catalog)
          receipt.validateAction(spark)
          val bound = receipt.obligations.head.binding
          val original = bound.index().snapshotAtAnalysis
          versions.foreach(version => publish(catalog, version))
          rejected(() => receipt.validateAction(spark))
          val current = bound.log().unsafeVolatileSnapshot
          assertEquals(
            original.schema.map(f => (f.name, f.dataType, f.nullable)),
            current.schema.map(f => (f.name, f.dataType, f.nullable))
          )
          catalog match {
            case "a" =>
              assertEquals(original.metadata.schemaString, current.metadata.schemaString)
              assertEquals(Seq("region", "id"), original.metadata.partitionColumns)
              assertEquals(Seq("id", "region"), current.metadata.partitionColumns)
            case "b" =>
              assertEquals(original.metadata.schemaString, current.metadata.schemaString)
              assertNotEquals(original.protocol, current.protocol)
            case "q" =>
              assertNotEquals(original.metadata.schemaString, current.metadata.schemaString)
              assertNotEquals(
                original.schema("odd` name").metadata,
                current.schema("odd` name").metadata
              )
          }
          assertNull(DeltaRangerNativeOperation.current())
      }
    }

  private def captured(spark: SparkSession, catalog: String): DeltaRangerDecisionReceipt =
    spark
      .sql(s"SELECT secret FROM $catalog.db.t")
      .queryExecution
      .analyzed
      .getTagValue(tag)
      .get
      .receipt

  private def rejected(action: () => Unit): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => action())
    assertEquals("Delta retained registration validation rejected", failure.getMessage)
  }

  private def withHistory(
      kind: String,
      operations: ConcurrentLinkedQueue[DeltaRangerNativeOperation] =
        new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
  )(
      test: (SparkSession, TestDeltaRangerNativeSelect.Fixture, (String, Long) => Unit) => Unit
  ): Unit = {
    val commits = scala.collection.mutable.Map.empty[(String, Long), (Path, Array[Byte])]
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark =>
          test(
            spark,
            policy,
            (catalog, version) => {
              val (path, bytes) = commits((catalog, version))
              Files.write(path, bytes)
              ()
            }
          ),
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val operation = DeltaRangerNativeOperation.current()
                if (
                  operation != null && operation
                    .callback() == DeltaRangerNativeOperation.Callback.VALIDATE_RETAINED_BINDING
                )
                  operations.add(operation)
                plan
              }
            }
          )
          NativeSelectFixture.extension(policy)(extensions)
        }),
        beforeExtension = spark => {
          val a = NativeSelectFixture.locations("a")
          val withheld = if (kind == "append") {
            spark
              .sql("SELECT 4L id, 'tail' secret, 'keep' region")
              .write
              .format("delta")
              .mode("append")
              .save(a)
            Seq(("a", 1L))
          } else {
            Seq(Seq("region", "id"), Seq("id", "region")).foreach { partitions =>
              spark
                .sql("SELECT 1L id, 'a-one' secret, 'keep' region")
                .write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy(partitions: _*)
                .save(a)
            }
            val b = NativeSelectFixture.locations("b")
            spark
              .sql(
                s"ALTER TABLE delta.`$b` SET TBLPROPERTIES ('delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')"
              )
              .collect()
            val q = NativeSelectFixture.locations("q")
            spark.sql(s"ALTER TABLE delta.`$q` RENAME COLUMN `odd`` name` TO renamed").collect()
            spark.sql(s"ALTER TABLE delta.`$q` DROP COLUMN renamed").collect()
            spark.sql(s"ALTER TABLE delta.`$q` ADD COLUMNS (`odd`` name` STRING)").collect()
            Seq(("a", 2L), ("b", 1L), ("q", 1L), ("q", 2L), ("q", 3L))
          }
          withheld.foreach { case key @ (catalog, version) =>
            val path =
              Paths.get(NativeSelectFixture.locations(catalog), "_delta_log", f"$version%020d.json")
            commits(key) = (path, Files.readAllBytes(path))
            Files.delete(path)
          }
        }
      )
    finally policy.engine.close()
  }
}
