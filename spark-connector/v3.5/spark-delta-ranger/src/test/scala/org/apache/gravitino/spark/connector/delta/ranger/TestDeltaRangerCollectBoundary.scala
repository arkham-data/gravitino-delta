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

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  ColumnarRule,
  SparkPlan,
  UnaryExecNode,
  WholeStageCodegenExec
}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Test-only characterization of the proposed Dataset collection boundary. */
class TestDeltaRangerCollectBoundary {
  @Test
  def staleRepeatedCollectIsRejectedBeforeFurtherChildWork(): Unit = {
    val state = new CollectBoundaryState
    withSpark(state) { spark =>
      val data = query(spark)
      val root = data.queryExecution.executedPlan
      val expected = Seq(Row(1L), Row(2L), Row(3L))
      assertEquals(expected, data.collect().toSeq)
      assertEquals(expected, data.collectAsList().asScala.toSeq)
      assertSame(root, data.queryExecution.executedPlan)
      val prepared = state.prepares.get()
      val executed = state.executes.get()
      assertTrue(prepared > 0)
      assertTrue(executed > 0)
      state.allowed = false
      assertThrows(classOf[CollectBoundaryDenied], () => data.collect())
      assertThrows(classOf[CollectBoundaryDenied], () => data.collectAsList())
      assertEquals(prepared, state.prepares.get())
      assertEquals(executed, state.executes.get())
      assertEquals(4, state.collections.get())
      assertEquals("CollectBoundaryGuard", root.nodeName)
      assertTrue(root.collect { case stage: WholeStageCodegenExec => stage }.nonEmpty)
      state.allowed = true
      assertEquals(expected, data.collect().toSeq)
      assertEquals(5, state.collections.get())
    }
  }

  @Test
  def staleFirstCollectCannotStartPreparationTimeBroadcast(): Unit = {
    val state = new CollectBoundaryState(broadcastOnPrepare = true)
    withSpark(state) { spark =>
      val data = query(spark)
      val root = data.queryExecution.executedPlan
      state.allowed = false
      assertThrows(classOf[CollectBoundaryDenied], () => data.collectAsList())
      assertEquals(0, state.prepares.get())
      assertEquals(0, state.executes.get())
      assertEquals(0, state.broadcasts.get())
      root.asInstanceOf[CollectBoundaryCleanup].cleanupForTest()
      assertTrue(state.cleanups.get() > 0)
      state.allowed = true
      assertEquals(Seq(Row(1L), Row(2L), Row(3L)), data.collect().toSeq)
      assertEquals(1, state.broadcasts.get())
      state.allowed = false
      val prepared = state.prepares.get()
      val executed = state.executes.get()
      assertThrows(classOf[CollectBoundaryDenied], () => data.collect())
      assertEquals(prepared, state.prepares.get())
      assertEquals(executed, state.executes.get())
      root.asInstanceOf[CollectBoundaryCleanup].cleanupForTest()
      assertEquals(1, state.destroyedBroadcasts.get())
    }
  }

  @Test
  def visibleChildCleanupRemainsReachableAfterExecutionFailure(): Unit = {
    val state = new CollectBoundaryState(broadcastOnPrepare = true)
    state.failExecution = true
    withSpark(state) { spark =>
      val data = query(spark)
      val root = data.queryExecution.executedPlan
      val failure = assertThrows(classOf[IllegalStateException], () => data.collect())
      assertEquals("test child execution failed", failure.getMessage)
      assertEquals(1, state.collections.get())
      assertEquals(1, state.broadcasts.get())
      root.asInstanceOf[CollectBoundaryCleanup].cleanupForTest()
      assertEquals(1, state.destroyedBroadcasts.get())
      assertTrue(state.cleanups.get() > 0)
    }
  }

  private def query(spark: SparkSession): Dataset[Row] =
    spark.range(0, 4, 1, 1).where("id > 0").toDF()

  private def withSpark(state: CollectBoundaryState)(test: SparkSession => Unit): Unit = {
    val directory = Files.createTempDirectory("delta-ranger-collect-boundary")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("delta-ranger-collect-boundary")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString)
      .withExtensions { extensions =>
        extensions.injectColumnar { session =>
          new ColumnarRule {
            override def postColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
              override def apply(plan: SparkPlan): SparkPlan = {
                require(
                  !session.conf.get("spark.sql.adaptive.enabled").toBoolean,
                  "Test collect boundary does not support AQE"
                )
                CollectBoundaryGuard(CollectBoundaryProbe(plan, state), state)
              }
            }
          }
        }
      }
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try test(spark)
    finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists(_))
      finally paths.close()
    }
  }
}

private[ranger] final class CollectBoundaryDenied
    extends SecurityException("test collect boundary rejected stale state")

/** Mutable driver-owned validation seam; it is deliberately not a security receipt. */
private[ranger] final class CollectBoundaryState(val broadcastOnPrepare: Boolean = false)
    extends Serializable {
  val collections = new AtomicInteger
  val prepares = new AtomicInteger
  val executes = new AtomicInteger
  val cleanups = new AtomicInteger
  val broadcasts = new AtomicInteger
  val destroyedBroadcasts = new AtomicInteger
  @volatile var allowed = true
  @volatile var failExecution = false

  def validateCollection(): Unit = {
    collections.incrementAndGet()
    validate()
  }

  def validate(): Unit = if (!allowed) throw new CollectBoundaryDenied
}

/** Visible child with driver-observable work, including a preparation-time broadcast. */
private[ranger] case class CollectBoundaryProbe(child: SparkPlan, state: CollectBoundaryState)
    extends UnaryExecNode
    with CollectBoundaryCleanup {
  @transient private var preparedBroadcast: Option[Broadcast[Array[Int]]] = None

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def doPrepare(): Unit = {
    state.prepares.incrementAndGet()
    if (state.broadcastOnPrepare) {
      preparedBroadcast = Some(sparkContext.broadcast(Array(1, 2, 3)))
      state.broadcasts.incrementAndGet()
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    state.executes.incrementAndGet()
    if (state.failExecution) throw new IllegalStateException("test child execution failed")
    child.execute()
  }

  override def cleanupResources(): Unit = {
    state.cleanups.incrementAndGet()
    preparedBroadcast.foreach { broadcast =>
      broadcast.destroy()
      state.destroyedBroadcasts.incrementAndGet()
    }
    preparedBroadcast = None
    super.cleanupResources()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CollectBoundaryProbe =
    copy(child = newChild)
}

/** Exposes inherited cleanup only for this test; no hidden delegate or package bridge. */
private[ranger] trait CollectBoundaryCleanup extends SparkPlan {
  final def cleanupForTest(): Unit = cleanupResources()
}

/** Collection-entry guard only: raw execute still prepares children before doExecute. */
private[ranger] case class CollectBoundaryGuard(child: SparkPlan, state: CollectBoundaryState)
    extends UnaryExecNode
    with CollectBoundaryCleanup {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def executeCollect(): Array[InternalRow] = {
    state.validateCollection()
    super.executeCollect()
  }

  override protected def doExecute(): RDD[InternalRow] = {
    state.validate()
    child.execute()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): CollectBoundaryGuard =
    copy(child = newChild)
}
