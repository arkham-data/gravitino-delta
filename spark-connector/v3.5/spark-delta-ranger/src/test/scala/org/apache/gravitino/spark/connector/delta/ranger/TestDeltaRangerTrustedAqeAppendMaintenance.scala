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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._
import java.nio.file.Paths
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import java.util.IdentityHashMap

/** Qualifies append-owned native cache refresh while caller AQE stays enabled. */
class TestDeltaRangerTrustedAqeAppendMaintenance {
  @Test
  def targetFirstAndProjectFirstRefresh(): Unit = {
    for (projectFirst <- Seq(false, true))
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "targetFirstAndProjectFirstRefresh",
        adaptive = true,
        warmCache = true,
        warmProjectFirst = projectFirst,
        rawMaintenance = true,
        nullDefault = !projectFirst,
        frontend = if (projectFirst) "saveAsTable" else "insertInto",
        sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id",
        scenario = s"aqe-native-warm-project-first-$projectFirst"
      )
  }

  @Test
  def duplicateAndLateNativeReadback(): Unit = {
    for (projectFirst <- Seq(false, true))
      new TestDeltaRangerRawMaintenanceIntegration().scenario(
        dependentFirst = projectFirst,
        adaptive = true
      )
  }

  @Test
  def cdfEnabledDisabledTargetMaintenance(): Unit = {
    for (enabled <- Seq(true, false); projectFirst <- Seq(false, true))
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "cdfEnabledDisabledTargetMaintenance",
        adaptive = true,
        warmCache = true,
        warmProjectFirst = projectFirst,
        rawMaintenance = true,
        cdfTarget = Some(enabled),
        frontend = if (projectFirst) "saveAsTable" else "insertInto",
        scenario = s"aqe-native-cdf-$enabled-project-first-$projectFirst"
      )
  }

  @Test
  def cdfHistoryContinuedUse(): Unit = {
    for (order <- Seq("target-first", "project-first"))
      new TestDeltaRangerCdfEnablement().scenario(order, appendAdaptive = true)
  }

  @Test
  def partitionCheckpointReopen(): Unit = {
    for (partitioned <- Seq(false, true))
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "partitionCheckpointReopen",
        adaptive = true,
        warmCache = true,
        warmProjectFirst = partitioned,
        rawMaintenance = true,
        seedCommits = 9,
        checkpointPartSize = Some(1L),
        checkpointTail = true,
        partitionedTarget = partitioned,
        scenario = s"aqe-native-warm-checkpoint-partitioned-$partitioned"
      )
  }

  @Test
  def unsupportedCacheRejectedBeforeMutation(): Unit = {
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    val trace = InstalledAppendBoundaryProbe.fresh("aqe-unsupported-cache")
    try
      RawNativeObservationFixture.withEnabled(
        storage = Map(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      ) { (spark, native, target) =>
        trace.activate(spark)
        NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
        val data = RawNativeObservationFixture.active(native) {
          val frame = native.read
            .format("delta")
            .load(NativeSelectFixture.locations("a"))
            .filter("id > 0")
            .cache()
          assertEquals(3, frame.collect().length)
          frame
        }
        val entry = spark.sharedState.cacheManager.lookupCachedData(data).get
        val root = Paths.get(NativeSelectFixture.locations("a")).getParent
        val before = InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
        val error = assertThrows(
          classOf[Exception],
          () => {
            spark
              .sql("SELECT id, secret, region FROM b.db.t")
              .write
              .mode("append")
              .insertInto("a.db.t")
          }
        )
        trace.record(
          "negative-causes",
          Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).mkString(" | ")
        )
        InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("diagnostic-after"))
        assertTrue(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(e =>
              Option(e.getMessage).exists(_.contains("unsupported target cache dependency"))
            )
        )
        assertEquals(
          before,
          InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
        )
        assertSame(entry, spark.sharedState.cacheManager.lookupCachedData(data).get)
        assertTrue(entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
        assertTrue(OptimisticTransaction.getActive.isEmpty)
      }
    finally { trace.close(); source.engine.close() }
  }

  @Test
  def foreignCloneAndCopiedKeyCannotBorrowMaintenance(): Unit = {
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    val trace = InstalledAppendBoundaryProbe.fresh("aqe-native-clone-binding")
    try
      RawNativeObservationFixture.withEnabled(
        storage = Map(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      ) { (spark, native, target) =>
        trace.activate(spark)
        NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
        var project: DataFrame = null
        val frame = RawNativeObservationFixture.active(native) {
          val data = native.read.format("delta").load(NativeSelectFixture.locations("a"))
          project = data.select("id", "region").cache()
          assertEquals(3, project.collect().length)
          data.cache()
          assertEquals(3, data.collect().length)
          data
        }
        val registration = NativeSelectFixture.bindingFixture(spark, "a")
        var inspected = false
        var inspecting = false
        var retained: DeltaRangerCdfCacheMaintenance.Scope = null
        def field[A](value: AnyRef, name: String): A = {
          val member = value.getClass.getDeclaredFields.find(_.getName.endsWith(name)).getOrElse {
            trace.record(
              "reflection-fields",
              value.getClass.getName + ":" + value.getClass.getDeclaredFields
                .map(_.getName)
                .mkString(",")
            )
            throw new AssertionError("Missing field " + name)
          }
          member.setAccessible(true)
          member.get(value).asInstanceOf[A]
        }
        registration.onRead = _ =>
          if (!inspected && !inspecting && DeltaRangerNativeOperation.current() == null) {
            val scope = field[ThreadLocal[DeltaRangerCdfCacheMaintenance.Scope]](
              DeltaRangerCdfCacheMaintenance,
              "active"
            ).get()
            if (scope != null) {
              val sessions = field[IdentityHashMap[TahoeLogFileIndex, SparkSession]](
                scope,
                "compilationSessions"
              )
              if (!sessions.isEmpty) {
                inspecting = true
                try {
                  val session = sessions.values().iterator().next()
                  val key = field[IdentityHashMap[LogicalPlan, Any]](scope, "keys")
                    .keySet()
                    .iterator()
                    .next()
                  retained = scope
                  assertNotSame(spark, session)
                  assertTrue(session.sessionState.conf.adaptiveExecutionEnabled)
                  assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
                  trace.record(
                    "native-clone-settings",
                    Seq(
                      "spark.sql.adaptive.enabled",
                      "spark.sql.optimizer.canChangeCachedPlanOutputPartitioning",
                      "spark.sql.adaptive.applyFinalStageShuffleOptimizations",
                      "spark.sql.sources.bucketing.autoBucketedScan.enabled"
                    )
                      .map(name =>
                        s"$name:original=${spark.conf.get(name)},clone=${session.conf.get(name)}"
                      )
                      .mkString(" ")
                  )
                  RawNativeObservationFixture.active(session) {
                    assertTrue(scope.compilationSession(key, session))
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.compilationSession(key.clone(), session)
                    )
                    val name = "spark.sql.adaptive.applyFinalStageShuffleOptimizations"
                    val previous = session.conf.get(name)
                    session.conf.set(name, "true")
                    try
                      assertThrows(
                        classOf[SecurityException],
                        () => scope.compilationSession(key, session)
                      )
                    finally session.conf.set(name, previous)
                  }
                  RawNativeObservationFixture.active(native) {
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.compilationSession(key, native)
                    )
                  }
                  RawNativeObservationFixture.active(spark) {
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.compilationSession(key, spark)
                    )
                  }
                  inspected = true
                } catch {
                  case error: Throwable =>
                    trace.record("maintenance-probe-failure", error.toString)
                    throw error
                } finally inspecting = false
              }
            }
          }
        spark.sql("SELECT id, secret, region FROM b.db.t").write.mode("append").insertInto("a.db.t")
        assertTrue(inspected)
        assertNotNull(retained)
        assertThrows(classOf[SecurityException], () => retained.requireLive())
        RawNativeObservationFixture.active(native) {
          assertEquals(
            5,
            native.read.format("delta").load(NativeSelectFixture.locations("a")).collect().length
          )
        }
        RawNativeObservationFixture.active(native) {
          assertEquals(
            5,
            native.read
              .format("delta")
              .load(NativeSelectFixture.locations("a"))
              .select("id", "region")
              .collect()
              .length
          )
          val wrapper = spark.sharedState.cacheManager
            .lookupCachedData(project)
            .get
            .cachedRepresentation
            .cacheBuilder
            .cachedPlan
            .asInstanceOf[AdaptiveSparkPlanExec]
          assertTrue(wrapper.isFinalPlan)
          val root = wrapper.executedPlan.asInstanceOf[DeltaRangerRawProducer]
          root.owner.requireInstalled(root)
          assertThrows(
            classOf[SecurityException],
            () => root.clone().asInstanceOf[DeltaRangerRawProducer].executeCollect()
          )
          root.owner.requireInstalled(root)
          assertThrows(
            classOf[Exception],
            () => wrapper.clone().asInstanceOf[AdaptiveSparkPlanExec].executeCollect()
          )
          root.owner.requireInstalled(root)
          assertEquals(
            5,
            native.read
              .format("delta")
              .load(NativeSelectFixture.locations("a"))
              .select("id", "region")
              .collect()
              .length
          )
          trace.record(
            "adaptive-copy-rejection",
            "copiedRootAndWrapperRejected=true genuineWinnerStillLive=true"
          )
        }
        assertFalse(spark.sharedState.cacheManager.lookupCachedData(frame).isEmpty)
      }
    finally { trace.close(); source.engine.close() }
  }
}

private[ranger] object TrustedAqeMaintenanceObservation {
  def producer(plan: SparkPlan): DeltaRangerRawProducer =
    plan match {
      case raw: DeltaRangerRawProducer => raw
      case adaptive: AdaptiveSparkPlanExec =>
        assertTrue(adaptive.inputPlan.isInstanceOf[DeltaRangerRawProducer])
        val raw = adaptive.inputPlan.asInstanceOf[DeltaRangerRawProducer]
        assertThrows(
          classOf[SecurityException],
          () =>
            raw.owner.acceptAdaptive(
              adaptive
                .clone()
                .asInstanceOf[AdaptiveSparkPlanExec]
            )
        )
        raw
      case _ => throw new AssertionError("Missing native raw cache producer: " + plan.treeString)
    }
}
