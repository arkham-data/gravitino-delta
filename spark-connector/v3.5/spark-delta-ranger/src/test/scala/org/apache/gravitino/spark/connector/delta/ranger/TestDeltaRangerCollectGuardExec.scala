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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  ColumnarRule,
  ExplainMode,
  LeafExecNode,
  SparkPlan,
  UnaryExecNode,
  WholeStageCodegenExec
}
import org.apache.spark.sql.types.LongType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Exercises the production collection guard without installing authorization. */
class TestDeltaRangerCollectGuardExec {
  @Test
  def retainedDatasetRevalidatesEveryCollectionBeforeChildWork(): Unit = {
    val state = new CollectGuardTestState
    withSpark(state) { spark =>
      assertFalse(spark.conf.get("spark.sql.adaptive.enabled").toBoolean)
      val data = query(spark)
      val root = data.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val expected = Seq(Row(1L), Row(2L), Row(3L))

      assertEquals(expected, data.collect().toSeq)
      assertEquals(expected, data.collectAsList().asScala.toSeq)
      assertSame(root, data.queryExecution.executedPlan)
      assertEquals(2, state.context.collectionValidations.get())
      assertTrue(state.context.executionValidations.get() > 0)
      assertTrue(state.child.executes.get() > 0)
      assertTrue(root.collect { case stage: WholeStageCodegenExec => stage }.nonEmpty)

      val prepared = state.child.prepares.get()
      val executed = state.child.executes.get()
      val executionValidations = state.context.executionValidations.get()
      state.context.allowed = false
      root.resetMetrics()
      assertEquals(
        2,
        state.context.collectionValidations.get(),
        "Unrelated metric resets grant no helper authority"
      )
      assertThrows(classOf[CollectGuardTestDenied], () => data.collect())
      assertThrows(classOf[CollectGuardTestDenied], () => data.collectAsList())
      assertEquals(4, state.context.collectionValidations.get())
      assertEquals(executionValidations, state.context.executionValidations.get())
      assertEquals(prepared, state.child.prepares.get())
      assertEquals(executed, state.child.executes.get())

      state.context.allowed = true
      assertEquals(expected, data.collect().toSeq)
      assertEquals(5, state.context.collectionValidations.get())
      assertTrue(state.context.executionValidations.get() >= executionValidations)
      assertTrue(state.child.executes.get() >= executed)
      assertFalse(spark.conf.get("spark.sql.adaptive.enabled").toBoolean)

      assertSanitized(root, state.context)
      val explanation = data.queryExecution.explainString(ExplainMode.fromString("extended"))
      assertFalse(explanation.contains(state.context.sensitiveMarker))
      assertFalse(explanation.contains("driverContext"))
    }
  }

  @Test
  def staleFirstCollectionPreventsBroadcastAndCleanupSurvivesDownstreamFailure(): Unit = {
    val state = new CollectGuardTestState(broadcastOnPrepare = true)
    state.context.allowed = false
    withSpark(state) { spark =>
      val data = query(spark)
      val root = data.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]

      assertThrows(classOf[CollectGuardTestDenied], () => data.collectAsList())
      assertEquals(1, state.context.collectionValidations.get())
      assertEquals(0, state.context.executionValidations.get())
      assertEquals(0, state.child.prepares.get())
      assertEquals(0, state.child.executes.get())
      assertEquals(0, state.child.broadcasts.get())

      root.cleanupResources()
      assertTrue(state.child.cleanups.get() > 0)

      state.context.allowed = true
      state.child.failExecution = true
      val failure = assertThrows(classOf[IllegalStateException], () => data.collect())
      assertEquals("test child execution failed", failure.getMessage)
      assertEquals(2, state.context.collectionValidations.get())
      assertEquals(1, state.context.executionValidations.get())
      assertEquals(1, state.child.prepares.get())
      assertEquals(1, state.child.executes.get())
      assertEquals(1, state.child.broadcasts.get())

      root.cleanupResources()
      assertEquals(1, state.child.destroyedBroadcasts.get())
      assertTrue(state.child.cleanups.get() > 1)
    }
  }

  @Test
  def copiesRevalidateActualChildrenAndCanonicalizedGuardFailsBeforeWork(): Unit = {
    val state = new CollectGuardTestState
    withSpark(state) { spark =>
      val root = query(spark).queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val original = root.child.asInstanceOf[CollectGuardTestProbeExec]
      assertNoValidationOrChildWork(state)

      val cloned = root.clone().asInstanceOf[DeltaRangerCollectGuardExec]
      assertNoValidationOrChildWork(state)
      assertEquals(Seq(1L, 2L, 3L), collectedLongs(cloned))
      assertEquals(1, state.context.collectionValidations.get())
      assertEquals(1, state.context.executionValidations.get())

      val copiedChild = original.copy(child = original.child.clone())
      val copied = root.withNewChildren(Seq(copiedChild)).asInstanceOf[DeltaRangerCollectGuardExec]
      val validationsBeforeCopyExecution = state.context.collectionValidations.get()
      assertEquals(Seq(1L, 2L, 3L), collectedLongs(copied))
      assertEquals(validationsBeforeCopyExecution + 1, state.context.collectionValidations.get())

      val replacementState = new CollectGuardChildState
      val unexpected =
        original.copy(sourceToken = new String("unexpected"), state = replacementState)
      val replaced = root.withNewChildren(Seq(unexpected)).asInstanceOf[DeltaRangerCollectGuardExec]
      val collectionValidations = state.context.collectionValidations.get()
      val executionValidations = state.context.executionValidations.get()
      assertThrows(classOf[CollectGuardTestDenied], () => replaced.executeCollect())
      assertEquals(collectionValidations + 1, state.context.collectionValidations.get())
      assertEquals(executionValidations, state.context.executionValidations.get())
      assertEquals(0, replacementState.prepares.get())
      assertEquals(0, replacementState.executes.get())

      val canonicalized = root.canonicalized.asInstanceOf[DeltaRangerCollectGuardExec]
      val beforeCanonicalCollection = state.context.collectionValidations.get()
      val beforeCanonicalExecution = state.context.executionValidations.get()
      val beforeCanonicalPrepare = state.child.prepares.get()
      val beforeCanonicalExecute = state.child.executes.get()
      assertThrows(classOf[IllegalStateException], () => canonicalized.executeCollect())
      assertEquals(beforeCanonicalCollection, state.context.collectionValidations.get())
      assertEquals(beforeCanonicalExecution, state.context.executionValidations.get())
      assertEquals(beforeCanonicalPrepare, state.child.prepares.get())
      assertEquals(beforeCanonicalExecute, state.child.executes.get())
    }
  }

  @Test
  def serializationDropsSensitiveContextAndRenderingOmitsAuthority(): Unit = {
    val token = new String("serializable-source")
    val childState = new SerializableCollectGuardChildState
    val child =
      SerializableCollectGuardChildExec(
        Seq(AttributeReference("id", LongType, nullable = false)()),
        token,
        childState
      )
    val context =
      new CollectGuardTestContext(token, new CollectGuardChildState, "sensitive-context-marker")
    val guard = DeltaRangerCollectGuardExec(child)(context)

    assertThrows(classOf[java.io.NotSerializableException], () => serialize(context))
    assertSanitized(guard, context)
    assertEquals(Seq(child), guard.productIterator.toSeq)
    assertFalse(
      guard.productIterator.exists(value => value.asInstanceOf[AnyRef] eq context)
    )

    val restored = deserialize[DeltaRangerCollectGuardExec](serialize(guard))
    assertThrows(classOf[IllegalStateException], () => restored.executeCollect())
    val restoredChild = restored.child.asInstanceOf[SerializableCollectGuardChildExec]
    assertEquals(0, restoredChild.state.prepares.get())
    assertEquals(0, restoredChild.state.executes.get())
  }

  private def assertSanitized(
      guard: DeltaRangerCollectGuardExec,
      context: CollectGuardTestContext
  ): Unit = {
    val rendered = Seq(
      guard.toString,
      guard.simpleString(Int.MaxValue),
      guard.treeString,
      guard.toJSON,
      guard.prettyJson
    )
    rendered.foreach { value =>
      assertFalse(value.contains(context.sensitiveMarker), value)
      assertFalse(value.contains(context.getClass.getSimpleName), value)
      assertFalse(value.contains("driverContext"), value)
    }
  }

  private def assertNoValidationOrChildWork(state: CollectGuardTestState): Unit = {
    assertEquals(0, state.context.collectionValidations.get())
    assertEquals(0, state.context.executionValidations.get())
    assertEquals(0, state.child.prepares.get())
    assertEquals(0, state.child.executes.get())
  }

  private def collectedLongs(plan: SparkPlan): Seq[Long] =
    plan.executeCollect().map(_.getLong(0)).toSeq

  private def query(spark: SparkSession): Dataset[Row] =
    spark.range(0, 4, 1, 1).where("id > 0").toDF()

  private def withSpark(state: CollectGuardTestState)(test: SparkSession => Unit): Unit = {
    val directory = Files.createTempDirectory("delta-ranger-collect-guard")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("delta-ranger-collect-guard")
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
                  "Collect-guard mechanics test requires AQE disabled"
                )
                val probe = CollectGuardTestProbeExec(plan, state.sourceToken, state.child)
                DeltaRangerCollectGuardExec(probe)(state.context)
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

  @Test
  def collectionScopeClosesWhenPreparationFails(): Unit = {
    val state = new CollectGuardTestState
    withSpark(state) { spark =>
      val frame = query(spark)
      val root = frame.queryExecution.executedPlan
      state.child.failPreparation = true
      assertThrows(classOf[IllegalStateException], () => root.executeCollect())
      assertEquals(1, state.context.closedCollections.get())
      assertEquals(0, state.child.executes.get())
      state.child.failPreparation = false
      assertEquals(3, root.executeCollect().length)
      assertEquals(2, state.context.closedCollections.get())
    }
  }

  private def serialize(value: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream
    val output = new ObjectOutputStream(bytes)
    try output.writeObject(value)
    finally output.close()
    bytes.toByteArray
  }

  private def deserialize[T](bytes: Array[Byte]): T = {
    val input = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try input.readObject().asInstanceOf[T]
    finally input.close()
  }
}

private[ranger] final class CollectGuardTestDenied(message: String)
    extends SecurityException(message)

private[ranger] trait CollectGuardTestOwnedChild { self: SparkPlan =>
  def sourceToken: AnyRef
}

private[ranger] final class CollectGuardTestContext(
    expectedSourceToken: AnyRef,
    val child: CollectGuardChildState,
    val sensitiveMarker: String
) extends DeltaRangerCollectValidationContext {
  val closedCollections = new AtomicInteger
  override def endCollection(): Unit = { closedCollections.incrementAndGet(); () }
  val collectionValidations = new AtomicInteger
  val executionValidations = new AtomicInteger
  @volatile var allowed = true

  override def validateCollection(actualChild: SparkPlan): Unit = {
    collectionValidations.incrementAndGet()
    validate(actualChild)
  }

  override def validateExecution(actualChild: SparkPlan): Unit = {
    executionValidations.incrementAndGet()
    validate(actualChild)
  }

  override def toString: String = sensitiveMarker

  private def validate(actualChild: SparkPlan): Unit = {
    if (!allowed) throw new CollectGuardTestDenied("test collect guard rejected stale state")
    actualChild match {
      case owned: CollectGuardTestOwnedChild if owned.sourceToken.eq(expectedSourceToken) =>
      case _ => throw new CollectGuardTestDenied("test collect guard rejected a replaced child")
    }
  }
}

private[ranger] final class CollectGuardTestState(val broadcastOnPrepare: Boolean = false) {
  val sourceToken: String = new String("expected-source")
  val child = new CollectGuardChildState(broadcastOnPrepare)
  val context = new CollectGuardTestContext(sourceToken, child, "sensitive-context-marker")
}

private[ranger] final class CollectGuardChildState(val broadcastOnPrepare: Boolean = false) {
  val prepares = new AtomicInteger
  val executes = new AtomicInteger
  val cleanups = new AtomicInteger
  val broadcasts = new AtomicInteger
  val destroyedBroadcasts = new AtomicInteger
  @volatile var failExecution = false
  @volatile var failPreparation = false
}

private[ranger] final case class CollectGuardTestProbeExec(
    child: SparkPlan,
    sourceToken: AnyRef,
    state: CollectGuardChildState
) extends UnaryExecNode
    with CollectGuardTestOwnedChild {
  @transient private var preparedBroadcast: Option[Broadcast[Array[Int]]] = None

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def doPrepare(): Unit = {
    state.prepares.incrementAndGet()
    if (state.failPreparation) throw new IllegalStateException("test preparation failed")
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

  override protected def withNewChildInternal(newChild: SparkPlan): CollectGuardTestProbeExec =
    copy(child = newChild)
}

private[ranger] final class SerializableCollectGuardChildState extends Serializable {
  val prepares = new AtomicInteger
  val executes = new AtomicInteger
}

private[ranger] final case class SerializableCollectGuardChildExec(
    output: Seq[Attribute],
    sourceToken: AnyRef,
    state: SerializableCollectGuardChildState
) extends LeafExecNode
    with CollectGuardTestOwnedChild {
  override protected def doPrepare(): Unit = state.prepares.incrementAndGet()

  override protected def doExecute(): RDD[InternalRow] = {
    state.executes.incrementAndGet()
    throw new AssertionError("Deserialized collect guard reached child execution")
  }
}
