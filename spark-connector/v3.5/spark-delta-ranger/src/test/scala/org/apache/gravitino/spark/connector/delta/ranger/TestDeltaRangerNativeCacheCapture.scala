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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.{ColumnarRule, LogicalRDD, RDDScanExec, SparkPlan}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Characterizes public Delta snapshot-cache getters without granting production admission. */
class TestDeltaRangerNativeCacheCapture {
  @Test
  def publicGettersReenterAnalysisBeforeReturningExactUnplannedCacheRdds(): Unit = {
    val capture = new NativeCacheCapture
    NativeSelectFixture.withSpark(
      spark => {
        assertEquals("true", spark.conf.get("spark.sql.adaptive.enabled"))
        assertEquals(
          "MEMORY_AND_DISK_SER",
          spark.conf.get(
            "spark.databricks.delta.snapshotCache.storageLevel",
            "MEMORY_AND_DISK_SER"
          )
        )
        assertEquals("true", spark.conf.get("spark.sql.inMemoryColumnarStorage.compressed"))

        val index = snapshotIndex(spark, "b")
        val snapshot = index.getSnapshot
        val state = captureGetter(capture, StateCacheGetter, snapshot.stateDF)
        val stats = captureGetter(capture, StatsCacheGetter, snapshot.withStats)

        assertCaptureOrder(capture, state)
        assertCaptureOrder(capture, stats)
        assertSame(
          state.returned.rdd,
          snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD].rdd
        )
        assertEquals(
          state.returned.output,
          snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD].output
        )
        assertSame(
          stats.returned.rdd,
          snapshot.withStats.queryExecution.analyzed.asInstanceOf[LogicalRDD].rdd
        )
        assertEquals(
          stats.returned.output,
          snapshot.withStats.queryExecution.analyzed.asInstanceOf[LogicalRDD].output
        )
        writeEventInventory("standalone", spark, capture, Seq(state, stats))
      },
      Some(extensions => installObservers(extensions, capture)),
      startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
    )
  }

  @Test
  def productionLogicalExtensionCoexistsWithScopedPublicGetterCapture(): Unit = {
    val fixture = new TestDeltaRangerNativeSelect.Fixture
    try {
      NativeSelectFixture.withSpark(
        spark => {
          NativeSelectFixture.withNativeOperation(spark) { index =>
            val snapshot = index.getSnapshot
            DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
            Seq(snapshot.stateDF, snapshot.withStats).foreach { frame =>
              val actual = frame.queryExecution.analyzed.asInstanceOf[LogicalRDD]
              assertNotNull(DeltaRangerNativeProvenance.requireExecutable(actual, spark))
              assertSame(actual.rdd, frame.queryExecution.analyzed.asInstanceOf[LogicalRDD].rdd)
              frame.queryExecution.executedPlan
            }
          }
          assertEquals(
            Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep")),
            spark.sql("SELECT id, region FROM b.db.t").collect().toSet
          )
        },
        Some(extensions => NativeSelectFixture.extension(fixture)(extensions))
      )
    } finally fixture.engine.close()
  }

  private def captureGetter(
      capture: NativeCacheCapture,
      getter: CacheGetter,
      value: => DataFrame
  ): CapturedCache = {
    capture.beginGetter(getter)
    val frame =
      try value
      catch {
        case failure: Throwable =>
          capture.abort(getter)
          throw failure
      }
    capture.getterReturned(getter, frame)
    val returned = capture.returnedLogical(getter)
    val decoy = capture.decoyFor(getter, returned)
    assertEquals(returned.schema, decoy.schema)
    assertNotSame(returned.rdd, decoy.rdd)
    assertThrows(classOf[SecurityException], () => capture.seal(getter, decoy))
    capture.seal(getter, returned)
    assertTrue(capture.isSealed(returned.rdd, returned.output))
    assertFalse(capture.isSealed(decoy.rdd, decoy.output))
    capture.beginPlanning(getter)
    try frame.queryExecution.executedPlan
    finally capture.endPlanning(getter)
    CapturedCache(getter, returned, decoy)
  }

  private def assertCaptureOrder(capture: NativeCacheCapture, cache: CapturedCache): Unit = {
    val events = capture.events(cache.getter)
    assertEquals("getter-enter", events.head.callback)
    assertEquals("planning-exit", events.last.callback)
    assertEquals(events.indices.map(_ + 1), events.map(_.order))

    val returnedResolution = events.filter(event =>
      event.callback == "resolution" && event.logical.exists(candidate =>
        candidate.rdd.eq(cache.returned.rdd) && candidate.output == cache.returned.output
      )
    )
    val returnedCheck = events.filter(event =>
      event.callback == "check" && event.logical.exists(candidate =>
        candidate.rdd.eq(cache.returned.rdd) && candidate.output == cache.returned.output
      )
    )
    val decoyResolution = events.filter(event =>
      event.callback == "resolution" && event.logical.exists(candidate =>
        candidate.rdd.eq(cache.decoy.rdd) && candidate.output == cache.decoy.output
      )
    )
    assertFalse(returnedResolution.isEmpty)
    assertFalse(returnedCheck.isEmpty)
    assertFalse(decoyResolution.isEmpty)
    assertEquals(events.head.threadId, returnedResolution.head.threadId)
    assertEquals(events.head.threadId, returnedCheck.head.threadId)
    assertTrue(returnedResolution.head.order < decoyResolution.head.order)
    assertTrue(decoyResolution.head.order < returnedCheck.head.order)

    val returnedOrder = events.filter(_.callback == "getter-return").map(_.order)
    val rejectedSealOrder = events.filter(_.callback == "seal-rejected").map(_.order)
    val sealOrder = events.filter(_.callback == "seal").map(_.order)
    val planningEnterOrder = events.filter(_.callback == "planning-enter").map(_.order)
    assertEquals(1, returnedOrder.size)
    assertEquals(1, rejectedSealOrder.size)
    assertEquals(1, sealOrder.size)
    assertEquals(1, planningEnterOrder.size)
    assertTrue(returnedCheck.last.order < returnedOrder.head)
    assertTrue(returnedOrder.head < rejectedSealOrder.head)
    assertTrue(rejectedSealOrder.head < sealOrder.head)
    assertTrue(sealOrder.head < planningEnterOrder.head)

    val pendingPhysical = events.filter(event =>
      event.callback.startsWith("physical-") && event.order < returnedOrder.head
    )
    assertFalse(pendingPhysical.isEmpty)
    assertTrue(pendingPhysical.exists(_.callback == "physical-pre"))
    assertTrue(pendingPhysical.exists(_.callback == "physical-post"))
    assertTrue(pendingPhysical.exists(_.order < returnedResolution.head.order))
    assertFalse(
      pendingPhysical.exists(_.physicalRdds.exists(_.eq(cache.returned.rdd))),
      s"${cache.getter.name} cache candidate physically planned before its getter returned"
    )

    val sealedPhysical = events.filter(event =>
      event.callback.startsWith("physical-") && event.order > planningEnterOrder.head
    )
    assertTrue(
      sealedPhysical.exists(_.physicalRdds.exists(_.eq(cache.returned.rdd))),
      s"${cache.getter.name} cache candidate did not physically plan after sealing"
    )
    assertFalse(sealedPhysical.exists(_.physicalRdds.exists(_.eq(cache.decoy.rdd))))
  }

  private def writeEventInventory(
      label: String,
      spark: SparkSession,
      capture: NativeCacheCapture,
      caches: Seq[CapturedCache]
  ): Unit = {
    val settings = Seq(
      s"spark.version=${spark.version}",
      s"spark.sql.adaptive.enabled=${spark.conf.get("spark.sql.adaptive.enabled")}",
      "spark.databricks.delta.snapshotCache.storageLevel=" + spark.conf.get(
        "spark.databricks.delta.snapshotCache.storageLevel",
        "MEMORY_AND_DISK_SER"
      ),
      "spark.sql.inMemoryColumnarStorage.compressed=" + spark.conf.get(
        "spark.sql.inMemoryColumnarStorage.compressed"
      )
    )
    val inventory = caches.flatMap { cache =>
      s"getter=${cache.getter.name}" +: capture.events(cache.getter).map { event =>
        val logical = event.logical
          .map(candidate => identityCategory(candidate.rdd, candidate.output, cache))
          .getOrElse("none")
        val physical = event.physicalRdds
          .map(rdd => identityCategory(rdd, Seq.empty, cache))
          .mkString("[", ",", "]")
        f"${event.order}%02d ${event.callback} logical=$logical " +
          s"physicalRoot=${event.physicalRoot.getOrElse("none")} physicalRdds=$physical"
      }
    }
    Option(System.getProperty("gravitino.delta.nativeCacheCapture.events")).foreach { output =>
      Files.write(
        Paths.get(s"$output-$label.txt"),
        (settings ++ inventory).mkString("\n").concat("\n").getBytes(UTF_8)
      )
    }
  }

  private def identityCategory(
      rdd: RDD[InternalRow],
      output: Seq[Attribute],
      cache: CapturedCache
  ): String = {
    if (rdd.eq(cache.returned.rdd) && (output.isEmpty || output == cache.returned.output)) {
      "returned"
    } else if (rdd.eq(cache.decoy.rdd) && (output.isEmpty || output == cache.decoy.output)) {
      "decoy"
    } else {
      "other"
    }
  }

  private def installObservers(
      extensions: SparkSessionExtensions,
      capture: NativeCacheCapture
  ): Unit = {
    extensions.injectResolutionRule(spark =>
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          capture.observeResolution(plan, spark)
          plan
        }
      }
    )
    extensions.injectCheckRule(_ => plan => capture.observeCheck(plan))
    extensions.injectColumnar(_ =>
      new ColumnarRule {
        override def preColumnarTransitions: Rule[SparkPlan] = physicalRule("physical-pre")
        override def postColumnarTransitions: Rule[SparkPlan] = physicalRule("physical-post")

        private def physicalRule(callback: String): Rule[SparkPlan] = new Rule[SparkPlan] {
          override def apply(plan: SparkPlan): SparkPlan = {
            capture.observePhysical(callback, plan)
            plan
          }
        }
      }
    )
  }

  private def snapshotIndex(spark: SparkSession, catalog: String): TahoeLogFileIndex =
    spark
      .sql(s"SELECT id, region FROM $catalog.db.t")
      .queryExecution
      .analyzed
      .collectFirst { case relation: LogicalRelation =>
        relation.relation.asInstanceOf[HadoopFsRelation].location.asInstanceOf[TahoeLogFileIndex]
      }
      .get
}

private[ranger] sealed abstract class CacheGetter(val name: String)
private[ranger] case object StateCacheGetter extends CacheGetter("stateDF")
private[ranger] case object StatsCacheGetter extends CacheGetter("withStats")

private[ranger] final case class CapturedCache(
    getter: CacheGetter,
    returned: LogicalRDD,
    decoy: LogicalRDD
)

private[ranger] final case class NativeCacheEvent(
    order: Int,
    callback: String,
    threadId: Long,
    logical: Option[LogicalRDD] = None,
    physicalRoot: Option[String] = None,
    physicalRdds: Vector[RDD[InternalRow]] = Vector.empty
)

/** Test-only capture protocol. It records identity and never grants executable admission. */
private[ranger] final class NativeCacheCapture {
  private sealed trait Phase
  private case object GetterPending extends Phase
  private case object CandidatePlanning extends Phase

  private val capturedEvents = mutable.Map.empty[CacheGetter, mutable.ArrayBuffer[NativeCacheEvent]]
  private val candidates = mutable.Map.empty[CacheGetter, mutable.ArrayBuffer[LogicalRDD]]
  private val decoys = mutable.Map.empty[CacheGetter, mutable.ArrayBuffer[(LogicalRDD, LogicalRDD)]]
  private val returnedFrames = mutable.Map.empty[CacheGetter, DataFrame]
  private val sealedCandidates = mutable.ArrayBuffer.empty[(RDD[InternalRow], Seq[Attribute])]
  private var active: Option[(CacheGetter, Phase)] = None
  private var constructingDecoy = false

  def beginGetter(getter: CacheGetter): Unit = synchronized {
    require(active.isEmpty, "A native cache capture is already active")
    require(!capturedEvents.contains(getter), s"${getter.name} capture already exists")
    capturedEvents(getter) = mutable.ArrayBuffer.empty
    candidates(getter) = mutable.ArrayBuffer.empty
    decoys(getter) = mutable.ArrayBuffer.empty
    active = Some(getter -> GetterPending)
    record(getter, "getter-enter")
  }

  def getterReturned(getter: CacheGetter, frame: DataFrame): Unit = synchronized {
    require(active.contains(getter -> GetterPending), s"${getter.name} getter is not pending")
    returnedFrames(getter) = frame
    record(getter, "getter-return")
    active = None
  }

  def abort(getter: CacheGetter): Unit = synchronized {
    if (active.exists(_._1 == getter)) {
      record(getter, "getter-abort")
      active = None
    }
  }

  def returnedLogical(getter: CacheGetter): LogicalRDD =
    returnedFrames(getter).queryExecution.analyzed match {
      case rdd: LogicalRDD => rdd
      case other =>
        throw new IllegalStateException(
          s"${getter.name} returned ${other.nodeName}, expected LogicalRDD"
        )
    }

  def decoyFor(getter: CacheGetter, returned: LogicalRDD): LogicalRDD = synchronized {
    decoys(getter)
      .collectFirst {
        case (source, decoy) if source.rdd.eq(returned.rdd) && source.output == returned.output =>
          decoy
      }
      .getOrElse(
        throw new IllegalStateException(s"${getter.name} returned candidate was not observed")
      )
  }

  def seal(getter: CacheGetter, candidate: LogicalRDD): Unit = synchronized {
    require(active.isEmpty, "A pending getter cannot seal a native cache candidate")
    val returned = returnedLogical(getter)
    val observed = candidates(getter).exists(observed =>
      observed.rdd.eq(returned.rdd) && observed.output == returned.output
    )
    if (
      !observed || !candidate.rdd.eq(returned.rdd) || candidate.output != returned.output ||
      decoys(getter).exists(_._2.rdd.eq(candidate.rdd))
    ) {
      record(getter, "seal-rejected", Some(candidate))
      throw new SecurityException(s"${getter.name} candidate is not the exact returned cache RDD")
    }
    sealedCandidates += candidate.rdd -> candidate.output
    record(getter, "seal", Some(candidate))
  }

  def isSealed(rdd: RDD[InternalRow], output: Seq[Attribute]): Boolean = synchronized {
    sealedCandidates.exists { case (sealedRdd, sealedOutput) =>
      sealedRdd.eq(rdd) && sealedOutput == output
    }
  }

  def beginPlanning(getter: CacheGetter): Unit = synchronized {
    require(active.isEmpty, "A native cache capture is already active")
    require(isSealed(returnedLogical(getter).rdd, returnedLogical(getter).output))
    active = Some(getter -> CandidatePlanning)
    record(getter, "planning-enter")
  }

  def endPlanning(getter: CacheGetter): Unit = synchronized {
    require(active.contains(getter -> CandidatePlanning), s"${getter.name} is not planning")
    record(getter, "planning-exit")
    active = None
  }

  def observeResolution(plan: LogicalPlan, spark: SparkSession): Unit = {
    val sources = observeLogical("resolution", plan)
    sources.foreach { source =>
      val getter = synchronized {
        constructingDecoy = true
        active.get._1
      }
      val decoy =
        try {
          spark
            .createDataFrame(spark.sparkContext.emptyRDD[Row], source.schema)
            .queryExecution
            .analyzed
        } finally
          synchronized {
            constructingDecoy = false
          }
      decoy match {
        case logicalRdd: LogicalRDD =>
          synchronized {
            decoys(getter) += source -> logicalRdd
          }
        case other =>
          throw new IllegalStateException(
            s"Public same-schema DataFrame produced ${other.nodeName}, expected LogicalRDD"
          )
      }
    }
  }

  def observeCheck(plan: LogicalPlan): Unit = {
    observeLogical("check", plan)
    ()
  }

  def observePhysical(callback: String, plan: SparkPlan): Unit = synchronized {
    active.foreach { case (getter, _) =>
      val rdds = physicalNodes(plan).collect { case scan: RDDScanExec => scan.rdd }
      record(
        getter,
        callback,
        physicalRoot = Some(plan.getClass.getSimpleName.stripSuffix("$")),
        physicalRdds = distinctIdentity(rdds)
      )
    }
  }

  def events(getter: CacheGetter): Vector[NativeCacheEvent] = synchronized {
    capturedEvents(getter).toVector
  }

  private def observeLogical(callback: String, plan: LogicalPlan): Vector[LogicalRDD] =
    synchronized {
      active match {
        case Some((getter, GetterPending)) =>
          val logicalRdds = distinctLogical(plan.collect { case rdd: LogicalRDD => rdd })
          logicalRdds.foreach { rdd =>
            candidates(getter) += rdd
            record(getter, callback, Some(rdd))
          }
          if (callback == "resolution" && !constructingDecoy) {
            logicalRdds.filterNot(rdd =>
              decoys(getter).exists(_._2.rdd.eq(rdd.rdd)) ||
                decoys(getter).exists(_._1.rdd.eq(rdd.rdd))
            )
          } else Vector.empty
        case _ => Vector.empty
      }
    }

  private def record(
      getter: CacheGetter,
      callback: String,
      logical: Option[LogicalRDD] = None,
      physicalRoot: Option[String] = None,
      physicalRdds: Vector[RDD[InternalRow]] = Vector.empty
  ): Unit = {
    val buffer = capturedEvents(getter)
    buffer += NativeCacheEvent(
      buffer.size + 1,
      callback,
      Thread.currentThread().getId,
      logical,
      physicalRoot,
      physicalRdds
    )
  }

  private def physicalNodes(root: SparkPlan): Vector[SparkPlan] = {
    val seen = new java.util.IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val result = mutable.ArrayBuffer.empty[SparkPlan]
    def visit(plan: SparkPlan): Unit = {
      if (!seen.containsKey(plan)) {
        seen.put(plan, java.lang.Boolean.TRUE)
        result += plan
        plan.children.foreach(visit)
        plan match {
          case adaptive: AdaptiveSparkPlanExec => visit(adaptive.inputPlan)
          case stage: QueryStageExec           => visit(stage.plan)
          case _                               =>
        }
      }
    }
    visit(root)
    result.toVector
  }

  private def distinctLogical(values: Seq[LogicalRDD]): Vector[LogicalRDD] = {
    val result = mutable.ArrayBuffer.empty[LogicalRDD]
    values.foreach { value =>
      if (!result.exists(_.eq(value))) result += value
    }
    result.toVector
  }

  private def distinctIdentity(
      values: Seq[RDD[InternalRow]]
  ): Vector[RDD[InternalRow]] = {
    val result = mutable.ArrayBuffer.empty[RDD[InternalRow]]
    values.foreach { value =>
      if (!result.exists(_.eq(value))) result += value
    }
    result.toVector
  }
}
