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

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeInto, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.commands.{MergeIntoCommand, MergeIntoCommandBase}
import org.apache.spark.sql.delta.commands.merge.{ClassicMergeExecutor, InsertOnlyMergeExecutor}
import org.apache.spark.sql.delta.util.AnalysisHelper.FakeLogicalPlan
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import scala.collection.mutable.ArrayBuffer

/** Gate evidence only: this native probe does not grant governed execution authority. */
class TestDeltaRangerMergeLinkage {
  @Test
  def builderLosesAuthoredAssignmentBeforeCommandHook(): Unit = {
    val resolved = ArrayBuffer.empty[LogicalPlan]
    val checked = ArrayBuffer.empty[MergeIntoCommand]
    val install: SparkSessionExtensions => Unit = extensions => {
      extensions.injectResolutionRule(_ =>
        new Rule[LogicalPlan] {
          override def apply(plan: LogicalPlan): LogicalPlan = { resolved += plan; plan }
        }
      )
      extensions.injectCheckRule(_ =>
        plan =>
          plan match {
            case merge: MergeIntoCommand => checked += merge
            case _                       =>
          }
      )
    }
    NativeSelectFixture.withSpark(
      spark => {
        val log = org.slf4j.LoggerFactory.getLogger(getClass)
        Seq(classOf[SparkSession], classOf[MergeIntoCommand], classOf[DeltaTable]).foreach { c =>
          log.warn(
            "Task 1 native origin: " + c.getName + " " + c.getProtectionDomain.getCodeSource.getLocation + " version=" + c.getPackage.getImplementationVersion
          )
        }
        assertThrows(
          classOf[org.apache.spark.sql.catalyst.parser.ParseException],
          () => DeltaTable.forName(spark, "b.db.t")
        )
        val target = DeltaTable.forPath(spark, NativeSelectFixture.locations("b")).as("t")
        val source = spark.table("a.db.t").as("s")
        resolved.clear(); checked.clear()
        target
          .merge(source, "t.id = s.id")
          .whenMatched()
          .updateExpr(Map("region" -> "s.region"))
          .execute()
        assertFalse(resolved.exists(_.isInstanceOf[DeltaMergeInto]))
        assertTrue(resolved.exists(_.isInstanceOf[FakeLogicalPlan]))
        assertFalse(checked.isEmpty)
        val omitted = checked.head
        val omittedActions = omitted.matchedClauses.head.resolvedActions
        assertEquals(Seq("id", "secret", "region"), omittedActions.map(_.targetColNameParts.head))
        assertEquals("secret", omittedActions(1).expr.references.head.name)
        resolved.clear(); checked.clear()
        target
          .merge(source, "t.id = s.id")
          .whenMatched()
          .updateExpr(Map("region" -> "s.region", "secret" -> "t.secret"))
          .execute()
        assertFalse(resolved.exists(_.isInstanceOf[DeltaMergeInto]))
        val explicit = checked.head
        assertEquals(omittedActions, explicit.matchedClauses.head.resolvedActions)
        val adapter = NativeMergeTraitProbe(explicit)
        assertSame(explicit.source, adapter.source)
        assertSame(explicit.target, adapter.target)
        assertSame(explicit.targetFileIndex, adapter.targetFileIndex)
        assertEquals(false, adapter.schemaEvolutionEnabled)
        assertTrue(adapter.requiresMaterialization(spark) == false)
      },
      Some(install)
    )
  }
}

/** Compile/linkage probe; deliberately cannot execute a command or confer ownership. */
private[ranger] final case class NativeMergeTraitProbe(native: MergeIntoCommand)
    extends MergeIntoCommandBase
    with ClassicMergeExecutor
    with InsertOnlyMergeExecutor {
  override val source = native.source
  override val target = native.target
  override val targetFileIndex = native.targetFileIndex
  override val condition = native.condition
  override val matchedClauses = native.matchedClauses
  override val notMatchedClauses = native.notMatchedClauses
  override val notMatchedBySourceClauses = native.notMatchedBySourceClauses
  override val migratedSchema = native.migratedSchema
  override val schemaEvolutionEnabled = false
  override val output = native.output
  override protected def runMerge(spark: SparkSession): Seq[Row] =
    throw new SecurityException("Compile probe has no execution authority")

  def requiresMaterialization(spark: SparkSession): Boolean =
    shouldMaterializeSource(spark, source, isInsertOnly)._1

  // Compile every proposed protected seam without invoking row/write operations in this probe.
  def protectedSeams(spark: SparkSession, transaction: OptimisticTransaction): Unit = {
    throw new SecurityException("Compile probe has no execution authority")
    prepareMergeSource(spark, source, condition, matchedClauses, notMatchedClauses, isInsertOnly)
    getMergeSource
    require(materializedSourceRDD.isDefined && attempt > 0)
    val (files, deletes) = findTouchedFiles(spark, transaction)
    buildTargetPlanWithFiles(spark, transaction, files, Seq.empty)
    buildTargetPlanWithIndex(spark, targetFileIndex, Seq.empty)
    writeAllChanges(spark, transaction, files, deletes, writeUnmodifiedRows = true)
    writeOnlyInserts(spark, transaction, filterMatchedRows = false, "numSourceRows")
    checkNonDeterministicSource(spark)
    createSetTransaction(spark, targetDeltaLog)
    hasBeenExecuted(transaction, spark)
    sendDriverMetrics(spark, metrics)
  }
}
