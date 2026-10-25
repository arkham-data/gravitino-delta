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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec,
  ShuffleQueryStageExec
}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import scala.concurrent.Await
import scala.concurrent.duration._

/** Tests an internal-guard candidate's failure; it does not authorize governed data. */
class TestDeltaRangerAdaptiveInvocation {
  @Test
  def stageMonitorAndStockCallerCannotAuthenticateCopiedInvocation(): Unit = {
    val probe = new AdaptiveInvocationProbe
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName("adaptive-invocation-probe")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .withExtensions { extensions =>
        extensions.injectColumnar { _ =>
          new ColumnarRule {
            override def postColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
              override def apply(plan: SparkPlan): SparkPlan = plan match {
                case exchange: ShuffleExchangeExec =>
                  exchange.copy(child = AdaptiveInvocationGuard(exchange.child, new Object, probe))
                case other => other
              }
            }
          }
        }
      }
    DeltaRangerAdaptiveFixture.profile.foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        probe.jobs.incrementAndGet(); ()
      }
      override def onTaskStart(event: SparkListenerTaskStart): Unit = {
        probe.tasks.incrementAndGet(); ()
      }
    })
    try {
      probe.log.warn(
        s"INVOCATION_PROVIDER spark=${spark.version} java=${System.getProperty("java.version")} scala=${scala.util.Properties.versionNumberString} origin=${classOf[AdaptiveSparkPlanExec].getProtectionDomain.getCodeSource.getLocation}"
      )
      val query = spark.sql(
        "SELECT l.id FROM range(0, 128, 1, 8) l INNER JOIN range(0, 128, 1, 8) r ON l.id = r.id"
      )
      probe.root = query.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val settings = spark.conf.getAll
      assertEquals((0L until 128L).toVector, query.collect().toVector.map(_.getLong(0)).sorted)
      assertTrue(probe.root.isFinalPlan)
      assertTrue(DeltaRangerAdaptiveFixture.materializedShuffleStages(probe.root).nonEmpty)
      assertTrue(DeltaRangerAdaptiveFixture.coalescedShuffleReads(probe.root).nonEmpty)
      assertEquals(settings, spark.conf.getAll)
      assertEquals(1, probe.denied.get())
      assertEquals(1, probe.legitimateRecipes.get())
      assertEquals(
        1,
        probe.foreignRecipes.get(),
        "Diagnostic: synchronized copied stage passed the candidate"
      )
      assertNotNull(probe.target)
      assertNotSame(probe.target, probe.forged)
      assertSame(probe.target.plan, probe.forged.plan)
      assertEquals(probe.target.id, probe.forged.id)
      assertTrue(probe.target.isMaterialized)
      assertFalse(probe.forged.isMaterialized)
      // Drain observational events so no already-enqueued legitimate event looks like new work.
      val bus = spark.sparkContext.getClass.getMethod("listenerBus").invoke(spark.sparkContext)
      val drain = bus.getClass.getMethod("waitUntilEmpty", java.lang.Long.TYPE)
      drain.invoke(bus, Long.box(10000L))
      val jobs = probe.jobs.get()
      val tasks = probe.tasks.get()
      val entries = probe.entries.get()
      val cachedCopy = probe.target.copy()
      val borrowed = cachedCopy.materialize()
      assertSame(probe.target.materialize(), borrowed)
      assertNotNull(Await.result(borrowed, 10.seconds))
      drain.invoke(bus, Long.box(10000L))
      assertEquals(entries, probe.entries.get(), "Cached materialization did not consult the guard")
      assertEquals(jobs, probe.jobs.get(), "Cached copy submitted no new job")
      assertEquals(tasks, probe.tasks.get(), "Cached copy submitted no new task")
      assertFalse(cachedCopy.isMaterialized)
      probe.log.warn(
        s"INVOCATION_CACHED rootStage=${System.identityHashCode(probe.target)} copy=${System.identityHashCode(cachedCopy)} sameExchange=true guardDelta=0 jobDelta=0 taskDelta=0 borrowedFuture=true"
      )
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}

private[ranger] final class AdaptiveInvocationProbe {
  val log = LoggerFactory.getLogger(getClass)
  val entries = new AtomicInteger
  val jobs = new AtomicInteger
  val tasks = new AtomicInteger
  val denied = new AtomicInteger
  val legitimateRecipes = new AtomicInteger
  val foreignRecipes = new AtomicInteger
  @volatile var root: AdaptiveSparkPlanExec = _
  var target: ShuffleQueryStageExec = _
  var forged: ShuffleQueryStageExec = _
  private var experimentStarted = false
  private var label = "stock"

  def enter(token: Object): Unit = {
    entries.incrementAndGet()
    val stages = DeltaRangerAdaptiveFixture.nodes(root.executedPlan).collect {
      case stage: ShuffleQueryStageExec => stage
    }
    val owned = stages
      .find(stage =>
        DeltaRangerAdaptiveFixture.nodes(stage.plan).exists {
          case guard: AdaptiveInvocationGuard => guard.token eq token
          case _                              => false
        }
      )
      .get
    val stack = Thread.currentThread().getStackTrace.toVector
    val materialize = stack.indexWhere(frame =>
      frame.getClassName == classOf[QueryStageExec].getName && frame.getMethodName == "materialize"
    )
    val caller = stack.lift(materialize + 1)
    val stockCaller = materialize >= 0 && caller.exists(frame =>
      frame.getClassName == classOf[
        AdaptiveSparkPlanExec
      ].getName && frame.getMethodName == "$anonfun$getFinalPhysicalPlan$5"
    )
    val monitor = Thread.holdsLock(owned)
    log.warn(
      s"INVOCATION_GUARD label=$label rootStage=${System.identityHashCode(owned)} exchange=${System.identityHashCode(owned.plan)} monitor=$monitor stockCaller=$stockCaller recipes=${legitimateRecipes.get() + foreignRecipes.get()}"
    )
    if (!monitor || !stockCaller) {
      denied.incrementAndGet()
      throw new SecurityException("Experimental guard cannot bind this invoking stage")
    }
    if (!experimentStarted) {
      experimentStarted = true
      assertEquals(2, stages.size)
      target = stages.find(stage => !(stage eq owned)).get
      val directCopy = target.copy()
      assertSame(target.plan, directCopy.plan)
      assertEquals(target.id, directCopy.id)
      label = "direct-copy"
      assertThrows(classOf[SecurityException], () => { directCopy.materialize(); () })
      assertEquals(0, legitimateRecipes.get())
      assertEquals(0, foreignRecipes.get())
      assertEquals(0, jobs.get())
      assertEquals(0, tasks.get())
      log.warn("INVOCATION_DENIED beforeOriginalPayload=true recipeDelta=0 jobDelta=0 taskDelta=0")
      forged = target.copy()
      label = "synchronized-copy-stock-helper"
      // This method is public installed Scala bytecode, not a private reflective engine hook.
      val helper = classOf[AdaptiveSparkPlanExec].getMethod(
        "$anonfun$getFinalPhysicalPlan$5",
        classOf[AdaptiveSparkPlanExec],
        classOf[LinkedBlockingQueue[_]],
        classOf[QueryStageExec]
      )
      target.synchronized {
        helper.invoke(null, root, new LinkedBlockingQueue[AnyRef](), forged)
      }
      assertEquals(1, foreignRecipes.get())
      assertTrue(stages.exists(_ eq target))
      log.warn(
        s"INVOCATION_FORGED original=${System.identityHashCode(target)} copy=${System.identityHashCode(forged)} sameExchange=true equalId=true originalStillCurrent=true acceptedRecipeDelta=1"
      )
      label = "stock"
    }
    if (label == "stock") legitimateRecipes.incrementAndGet() else foreignRecipes.incrementAndGet()
  }
}

private[ranger] case class AdaptiveInvocationGuard(
    child: SparkPlan,
    @transient token: Object,
    @transient probe: AdaptiveInvocationProbe
) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override protected def doExecute(): RDD[InternalRow] = {
    probe.enter(token)
    child.execute()
  }
  override protected def withNewChildInternal(newChild: SparkPlan): AdaptiveInvocationGuard =
    copy(child = newChild)
}
