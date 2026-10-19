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

// Test-only Scala visibility bridge; this file must never enter a production source set.
package org.apache.spark.sql

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.collection.mutable
import scala.util.Properties

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{
  IgnoreCachedData,
  LeafNode,
  LogicalPlan,
  UnaryNode
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.CachedData
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/** Mechanical native cache observations; namespace labels convey no authorization. */
class TestNativeCacheNormalizationCharacterization {
  @Test
  def normalizationRunsForAnalyzedNativeRecacheWithoutAnalyzerRules(): Unit = {
    Seq(false, true).foreach { adaptive =>
      withFixture("native-recache-" + adaptive, adaptive) { (spark, location, probe) =>
        val frame = spark.read.format("delta").load(location)
        val manager = spark.sharedState.cacheManager
        frame.cache()
        assertRows(frame, Seq(1L, 2L), probe)
        val old = manager.lookupCachedData(frame).get
        assertTrue(old.plan.analyzed, "Ordinary stored native recipe must be already analyzed")
        val key = DeltaTableV2(spark, new HadoopPath(location)).toLogicalRelation
        probe.note("exact-native-v1-key", key.treeString)
        assertTrue(old.plan.exists(_.sameResult(key)), "Exact native V1 refresh key must match")
        val oldBuffers = old.cachedRepresentation.cacheBuilder.cachedColumnBuffers
        val originalConf = spark.sessionState.conf.getAllConfs
        probe.recordCache("before", old)

        probe.during("native-recache") { manager.recacheByPlan(spark, key) }
        val events = probe.eventsFor("native-recache")
        val normalization =
          events.filter(e => e.kind == "normalization" && e.plan.exists(_.eq(old.plan)))
        assertEquals(1, normalization.size, "Normalization must see the exact stored recipe")
        assertFalse(events.exists(e => e.kind == "resolution" && e.containsTarget))
        assertFalse(events.exists(e => e.kind == "check" && e.containsTarget))
        val rebuildSession = normalization.head.session
        assertSame(spark.sharedState, rebuildSession.sharedState)
        if (adaptive) assertNotSame(spark, rebuildSession) else assertSame(spark, rebuildSession)
        val expected = originalConf.updated(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
        assertEquals(expected, rebuildSession.sessionState.conf.getAllConfs)
        assertEquals(originalConf, spark.sessionState.conf.getAllConfs)
        assertEquals("false", normalization.head.conf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key))

        val rebuilt = manager.lookupCachedData(old.plan).get
        assertNotSame(old.cachedRepresentation, rebuilt.cachedRepresentation)
        assertNotSame(
          old.cachedRepresentation.cacheBuilder,
          rebuilt.cachedRepresentation.cacheBuilder
        )
        assertEquals(StorageLevel.NONE, oldBuffers.getStorageLevel)
        assertFalse(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
        probe.recordCache("after-recache-before-first-materialization", rebuilt)
        assertRows(spark.read.format("delta").load(location), Seq(1L, 2L), probe)
        assertTrue(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
        assertNotEquals(
          oldBuffers.id,
          rebuilt.cachedRepresentation.cacheBuilder.cachedColumnBuffers.id
        )
        probe.recordCache("after-first-materialization", rebuilt)

        val unrelated = spark.newSession()
        val unrelatedInitial = unrelated.sessionState.conf.getAllConfs
        probe.note("unrelated-initial-config", unrelatedInitial.toSeq.sorted.mkString(","))
        val expectedStatic = expected.filter { case (name, _) => SQLConf.isStaticConfigKey(name) }
        val inheritedStatic = unrelatedInitial.filter { case (name, _) =>
          SQLConf.isStaticConfigKey(name)
        }
        assertEquals(
          expectedStatic,
          inheritedStatic,
          "newSession must inherit exact static settings"
        )
        expected.foreach { case (name, value) =>
          if (!unrelatedInitial.get(name).contains(value)) {
            // RuntimeConfig rejects static keys even when setting their existing value.
            assertFalse(
              SQLConf.isStaticConfigKey(name),
              s"Cannot change inherited static key $name"
            )
            probe.note("unrelated-config-change", s"$name: ${unrelatedInitial.get(name)} -> $value")
            unrelated.conf.set(name, value)
          }
        }
        assertSame(spark.sharedState, unrelated.sharedState)
        assertNotSame(rebuildSession, unrelated)
        assertEquals(expected, unrelated.sessionState.conf.getAllConfs)
        probe.during("unrelated-equal-settings") {
          unrelated.sessionState.executePlan(old.plan).normalized
        }
        val unrelatedEvents = probe
          .eventsFor("unrelated-equal-settings")
          .filter(e => e.kind == "normalization" && e.plan.exists(_.eq(old.plan)))
        assertEquals(1, unrelatedEvents.size)
        assertSame(unrelated, unrelatedEvents.head.session)
        // Record None as None. Matching tracker/config observations do not authenticate a clone.
        probe.note(
          "clone-versus-unrelated-trackers",
          s"native=${probe.identity(normalization.head.tracker.orNull)} unrelated=${probe.identity(unrelatedEvents.head.tracker.orNull)}"
        )
      }
    }
  }

  @Test
  def childlessAnchorsBlockRawRootAndSubtreeLookupWhileUnaryMarkerDoesNot(): Unit =
    withFixture("raw-isolation", adaptive = false) { (spark, location, probe) =>
      val manager = spark.sharedState.cacheManager
      val leaf = spark.read.format("delta").load(location)
      leaf.cache()
      assertRows(leaf, Seq(1L, 2L), probe)
      val projection = spark.read.format("delta").load(location).filter("id = 1").select("id")
      projection.cache()
      assertRows(projection, Seq(1L), probe)
      val rawRoot = manager.lookupCachedData(projection).get
      val rawLeaf = manager.lookupCachedData(leaf).get
      probe.recordCache("raw-root", rawRoot)
      probe.recordCache("raw-leaf", rawLeaf)
      val native = projection.queryExecution.analyzed

      val negative = manager.useCachedData(ProbeUnaryIgnore(native))
      assertTrue(negative.isInstanceOf[ProbeUnaryIgnore])
      assertTrue(negative.children.head.isInstanceOf[InMemoryRelation])
      assertSame(
        rawRoot.cachedRepresentation.cacheBuilder,
        negative.children.head.asInstanceOf[InMemoryRelation].cacheBuilder
      )
      val leafNegative = manager.useCachedData(ProbeUnaryIgnore(leaf.queryExecution.analyzed))
      assertTrue(leafNegative.children.head.isInstanceOf[InMemoryRelation])

      spark.conf.set(CacheNormalizationProbe.NamespaceKey, "alpha")
      val scoped = spark.read.format("delta").load(location).filter("id = 1").select("id")
      val normalized = scoped.queryExecution.normalized
      assertTrue(normalized.isInstanceOf[ProbeScope])
      assertFalse(normalized.sameResult(rawRoot.plan))
      val anchors = normalized.collect { case a: ProbeAnchor => a }
      assertEquals(1, anchors.size)
      assertTrue(anchors.head.children.isEmpty)
      assertFalse(normalized.exists(_.isInstanceOf[LogicalRelation]))
      assertFalse(
        scoped.queryExecution.withCachedData.exists(_.isInstanceOf[InMemoryRelation]),
        "Neither warmed raw root nor warmed raw leaf may substitute"
      )
      assertRows(scoped, Seq(1L), probe)
      assertFalse(scoped.queryExecution.optimizedPlan.exists {
        case _: ProbeScope | _: ProbeAnchor => true
        case _                              => false
      })
      assertEquals(normalized.output, scoped.queryExecution.optimizedPlan.output)
      probe.note("raw-isolation", scoped.queryExecution.toString)
    }

  @Test
  def scopedNamespacesReuseOwnBuildersAndRetainV1NativeRefreshMatching(): Unit =
    withFixture("namespace-and-v1", adaptive = false) { (spark, location, probe) =>
      val manager = spark.sharedState.cacheManager
      def fresh(): DataFrame =
        spark.read.format("delta").load(location).filter("id >= 1").select("id")
      spark.conf.set(CacheNormalizationProbe.NamespaceKey, "alpha")
      val alpha = fresh().cache()
      assertRows(alpha, Seq(1L, 2L), probe)
      val alphaCache = manager.lookupCachedData(alpha).get
      val alphaReuse = fresh()
      assertRows(alphaReuse, Seq(1L, 2L), probe)
      assertSame(
        alphaCache.cachedRepresentation.cacheBuilder,
        alphaReuse.queryExecution.withCachedData.asInstanceOf[InMemoryRelation].cacheBuilder
      )

      spark.conf.set(CacheNormalizationProbe.NamespaceKey, "beta")
      val beta = fresh()
      assertFalse(beta.queryExecution.normalized.sameResult(alphaCache.plan))
      assertFalse(beta.queryExecution.withCachedData.exists(_.isInstanceOf[InMemoryRelation]))
      beta.cache()
      // beta already forced withCachedData before cache creation: materialize a fresh query.
      assertRows(fresh(), Seq(1L, 2L), probe)
      val betaCache = manager.lookupCachedData(beta).get
      assertTrue(betaCache.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      assertNotSame(
        alphaCache.cachedRepresentation.cacheBuilder,
        betaCache.cachedRepresentation.cacheBuilder
      )
      val representation = alphaCache.cachedRepresentation
      val remapped = representation.output.map(_.newInstance())
      val withOutput = representation.withOutput(remapped)
      val newInstance = representation.newInstance()
      assertSame(representation.cacheBuilder, withOutput.cacheBuilder)
      assertSame(representation.cacheBuilder, newInstance.cacheBuilder)
      assertEquals(remapped, withOutput.output)
      assertNotEquals(representation.output.map(_.exprId), newInstance.output.map(_.exprId))

      val v1Key = DeltaTableV2(spark, new HadoopPath(location)).toLogicalRelation
      probe.note("exact-native-v1-key", v1Key.treeString)
      Seq(alphaCache, betaCache).foreach { entry =>
        assertTrue(entry.plan.exists(_.sameResult(v1Key)))
        probe.recordCache("before-v1-refresh", entry)
      }
      val oldRdds =
        Seq(alphaCache, betaCache).map(_.cachedRepresentation.cacheBuilder.cachedColumnBuffers)
      probe.during("scoped-v1-recache") { manager.recacheByPlan(spark, v1Key) }
      Seq(alphaCache, betaCache).zip(oldRdds).foreach { case (old, oldRdd) =>
        val rebuilt = manager.lookupCachedData(old.plan).get
        assertEquals(StorageLevel.NONE, oldRdd.getStorageLevel)
        assertNotSame(
          old.cachedRepresentation.cacheBuilder,
          rebuilt.cachedRepresentation.cacheBuilder
        )
        assertFalse(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
        assertTrue(rebuilt.plan.sameResult(old.plan), "Recache must preserve the stored namespace")
        probe.recordCache("after-v1-refresh-cold", rebuilt)
      }
      Seq("alpha", "beta").foreach { namespace =>
        spark.conf.set(CacheNormalizationProbe.NamespaceKey, namespace)
        val readback = fresh()
        assertRows(readback, Seq(1L, 2L), probe)
        val current = manager.lookupCachedData(readback).get
        assertTrue(current.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
        probe.recordCache("after-v1-refresh-loaded-" + namespace, current)
      }
      val matching = probe
        .eventsFor("scoped-v1-recache")
        .filter(e =>
          e.kind == "normalization" && e.plan.exists(p =>
            p.eq(alphaCache.plan) || p.eq(betaCache.plan)
          )
        )
      assertEquals(2, matching.size)
      // Candidate scopes may rerun analysis because normalization created fresh nodes.
      probe.note(
        "scoped-analysis-observation",
        probe.eventsFor("scoped-v1-recache").map(_.kind).mkString(",")
      )
    }

  @Test
  def directV2KeyFixtureMatchesOuterRecacheAndDoesNotPretendV1Matched(): Unit =
    withFixture("direct-v2-key", adaptive = false) { (spark, location, probe) =>
      val manager = spark.sharedState.cacheManager
      val table = DeltaTableV2(spark, new HadoopPath(location))
      val innerV1 = table.toLogicalRelation
      val outerV2 = DataSourceV2Relation.create(table, None, None)
      probe.note("exact-inner-v1-key", innerV1.treeString)
      probe.note("exact-outer-v2-key", outerV2.treeString)
      assertFalse(innerV1.sameResult(outerV2))
      val scopedV2 = ProbeScope("v2-fixture", ProbeAnchor(outerV2))
      assertTrue(scopedV2.exists(_.sameResult(outerV2)))
      assertFalse(scopedV2.exists(_.sameResult(innerV1)))
      // Public direct CacheManager API: stores the supplied V2 key, unlike ordinary read caching.
      manager.cacheQuery(spark, scopedV2, Some("direct-v2-fixture"), StorageLevel.MEMORY_AND_DISK)
      val old = manager.lookupCachedData(scopedV2).get
      assertRows(Dataset.ofRows(spark, old.cachedRepresentation), Seq(1L, 2L), probe)
      assertTrue(old.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      val oldRdd = old.cachedRepresentation.cacheBuilder.cachedColumnBuffers
      probe.recordCache("v2-before", old)
      probe.during("v1-nonmatch") { manager.recacheByPlan(spark, innerV1) }
      assertSame(
        old.cachedRepresentation.cacheBuilder,
        manager.lookupCachedData(scopedV2).get.cachedRepresentation.cacheBuilder
      )
      assertTrue(old.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      probe.during("outer-v2-recache") { manager.recacheByPlan(spark, outerV2) }
      val rebuilt = manager.lookupCachedData(scopedV2).get
      assertNotSame(
        old.cachedRepresentation.cacheBuilder,
        rebuilt.cachedRepresentation.cacheBuilder
      )
      assertEquals(StorageLevel.NONE, oldRdd.getStorageLevel)
      assertFalse(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      probe.recordCache("v2-after-cold", rebuilt)
      // Materialization occurs after the observation phase has ended.
      assertRows(Dataset.ofRows(spark, rebuilt.cachedRepresentation), Seq(1L, 2L), probe)
      assertRows(Dataset.ofRows(spark, rebuilt.cachedRepresentation), Seq(1L, 2L), probe)
      assertTrue(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      probe.recordCache("v2-after-loaded", rebuilt)
    }

  private def assertRows(
      frame: DataFrame,
      expected: Seq[Long],
      probe: CacheNormalizationProbe
  ): Unit = {
    val actual = frame.collect().map(_.getLong(0)).sorted.toSeq
    probe.note(
      "literal-readback",
      s"actual=$actual expected=${expected.sorted} consumer=${frame.queryExecution.executedPlan.treeString}"
    )
    assertEquals(expected.sorted, actual)
  }

  private def withFixture(name: String, adaptive: Boolean)(
      test: (SparkSession, String, CacheNormalizationProbe) => Unit
  ): Unit = {
    val root = Files.createTempDirectory("native-cache-hook-" + name)
    val evidenceRoot = sys.env
      .get("GRAVITINO_DELTA_CACHE_HOOK_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(root.resolve("evidence"))
    Files.createDirectories(evidenceRoot)
    val probe = new CacheNormalizationProbe(
      evidenceRoot.resolve(name + "-" + System.nanoTime() + ".log")
    )
    probe.note("fixture-directory", root.toString)
    val previousActive = SparkSession.getActiveSession
    val previousDefault = SparkSession.getDefaultSession
    require(
      previousActive.isEmpty && previousDefault.isEmpty,
      "Probe requires exclusive fresh Spark fixture"
    )
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(name)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.databricks.delta.snapshotPartitions", "2")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, adaptive.toString)
      .config(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key, "false")
      .config(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key, "false")
      .config(CacheNormalizationProbe.NamespaceKey, "")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.catalog.update.enabled", "false")
      .withExtensions(probe.install)
      .getOrCreate()
    try {
      probe.note(
        "runtime",
        s"spark=${spark.version} scala=${Properties.versionNumberString} java=${System.getProperty("java.version")}"
      )
      spark.sparkContext.setLogLevel("WARN")
      val location = root.resolve("target").toString
      spark.sql("SELECT 1L id UNION ALL SELECT 2L id").write.format("delta").save(location)
      probe.target = Some(DeltaTableV2(spark, new HadoopPath(location)).toLogicalRelation)
      test(spark, location, probe)
    } catch {
      case failure: Throwable =>
        probe.note("failure", failure.toString)
        throw failure
    } finally {
      // Only after observations: never preclear warmed buffers to manufacture recache success.
      spark.sharedState.cacheManager.clearCache()
      DeltaLog.clearCache()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      previousActive.foreach(SparkSession.setActiveSession)
      previousDefault.foreach(SparkSession.setDefaultSession)
      // Retain all native data/evidence directories, including failures.
    }
  }
}

/** Childless negative-lookup anchor; native canonicalization is matching evidence only. */
private[sql] case class ProbeAnchor(native: LogicalPlan) extends LeafNode with IgnoreCachedData {
  override def output: Seq[Attribute] = native.output
  override def doCanonicalize(): LogicalPlan = native.canonicalized
}

/** Test namespace is part of native sameResult lookup; possession grants no authority. */
private[sql] case class ProbeScope(namespace: String, child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): ProbeScope =
    copy(child = newChild)
  override def doCanonicalize(): LogicalPlan = copy(child = child.canonicalized)
}

/** Negative control: IgnoreCachedData on a unary parent does not stop child substitution. */
private[sql] case class ProbeUnaryIgnore(child: LogicalPlan)
    extends UnaryNode
    with IgnoreCachedData {
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: LogicalPlan): ProbeUnaryIgnore =
    copy(child = newChild)
}

private[sql] case class CacheHookEvent(
    phase: String,
    kind: String,
    session: SparkSession,
    plan: Option[LogicalPlan],
    tracker: Option[QueryPlanningTracker],
    conf: Map[String, String],
    containsTarget: Boolean
)

private[sql] object CacheNormalizationProbe {
  val NamespaceKey = "spark.test.nativeCache.namespace"
}

/** Records exact references for assertions and durable textual observations for failed runs. */
private[sql] class CacheNormalizationProbe(evidence: Path) {
  private val log = LoggerFactory.getLogger(classOf[CacheNormalizationProbe])
  private val events = mutable.ArrayBuffer.empty[CacheHookEvent]
  private var phase = "outside-observation"
  var target: Option[LogicalPlan] = None

  def install(extensions: SparkSessionExtensions): Unit = {
    def observed(kind: String)(spark: SparkSession): Rule[LogicalPlan] = {
      observe("construct-" + kind, spark, None)
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          observe(kind, spark, Some(plan))
          plan
        }
      }
    }
    extensions.injectResolutionRule(observed("resolution"))
    extensions.injectCheckRule { spark =>
      observe("construct-check", spark, None)
      plan => observe("check", spark, Some(plan))
    }
    extensions.injectPlanNormalizationRule { spark =>
      observe("construct-normalization", spark, None)
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          observe("normalization", spark, Some(plan))
          val namespace = spark.conf.get(CacheNormalizationProbe.NamespaceKey, "")
          if (namespace.isEmpty) plan else encode(plan, namespace)
        }
      }
    }
    // Public pre-CBO hook precedes native physical scan planning; only these test nodes lower.
    extensions.injectPreCBORule { spark =>
      observe("construct-pre-cbo", spark, None)
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          observe("pre-cbo", spark, Some(plan))
          plan.transformUp {
            case scope: ProbeScope => scope.child
            case anchor: ProbeAnchor =>
              anchor.native match {
                case relation: DataSourceV2Relation =>
                  relation.table match {
                    case table: DeltaTableV2 =>
                      table.toLogicalRelation.copy(output = relation.output)
                    case _ =>
                      throw new IllegalArgumentException("Probe admits only native Delta V2")
                  }
                case native => native
              }
          }
        }
      }
    }
  }

  def during[A](name: String)(body: => A): A = {
    require(phase == "outside-observation")
    phase = name
    note("phase-enter", name)
    try body
    finally {
      note("phase-exit", name)
      phase = "outside-observation"
    }
  }

  def eventsFor(name: String): Vector[CacheHookEvent] = events.filter(_.phase == name).toVector

  def recordCache(name: String, entry: CachedData): Unit = {
    val relation = entry.cachedRepresentation
    val builder = relation.cacheBuilder
    val loaded = builder.isCachedColumnBuffersLoaded
    // Avoid cachedColumnBuffers on a cold builder: that would create an RDD and perturb evidence.
    val buffers = if (loaded) {
      val rdd = builder.cachedColumnBuffers
      s"rdd=${rdd.id} rddIdentity=${identity(rdd)} storage=${rdd.getStorageLevel}"
    } else "rdd=not-observed-cold"
    note(
      name,
      s"entry=${identity(entry)} recipe=${identity(entry.plan)} analyzed=${entry.plan.analyzed} relation=${identity(relation)} builder=${identity(builder)} loaded=$loaded $buffers serializer=${builder.serializer.getClass.getName} physical=${builder.cachedPlan.treeString} recipeTree=${entry.plan.treeString}"
    )
  }

  def identity(value: AnyRef): String = if (value == null) "None"
  else
    value.getClass.getName + "@" + Integer.toHexString(System.identityHashCode(value))

  def note(name: String, value: String): Unit = synchronized {
    val line = s"$phase\t$name\t${value.replace("\n", "\\n")}\n"
    Files.write(
      evidence,
      line.getBytes(UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
    log.info(s"Native cache probe evidence $evidence: ${line.trim}")
  }

  private def observe(kind: String, spark: SparkSession, plan: Option[LogicalPlan]): Unit = {
    val containsTarget =
      plan.exists(_.exists(p => p.isInstanceOf[ProbeAnchor] || target.exists(_.sameResult(p))))
    // Rule factories can run while SessionState's lazy initializer is still active.
    // Reading spark.conf/sessionState there recursively initializes it; defer config capture.
    val conf = if (plan.nonEmpty) spark.sessionState.conf.getAllConfs else Map.empty[String, String]
    val event =
      CacheHookEvent(phase, kind, spark, plan, QueryPlanningTracker.get, conf, containsTarget)
    events += event
    note(
      kind,
      s"thread=${Thread.currentThread().getId} session=${identity(spark)} sharedState=${identity(spark.sharedState)} plan=${identity(plan.orNull)} tracker=${identity(event.tracker.orNull)} active=${identity(SparkSession.getActiveSession.orNull)} containsTarget=$containsTarget analyzed=${plan.map(_.analyzed)} conf=${event.conf.toSeq.sorted.mkString(",")} tree=${plan.map(_.treeString).getOrElse("None")}"
    )
  }

  private def encode(plan: LogicalPlan, namespace: String): LogicalPlan = plan match {
    case _: ProbeScope | _: ProbeAnchor => plan // Idempotent: preserve stored recipe namespace.
    case relation: LogicalRelation if target.exists(_.sameResult(relation)) =>
      ProbeScope(namespace, ProbeAnchor(relation))
    case relation: DataSourceV2Relation if relation.table.isInstanceOf[DeltaTableV2] =>
      ProbeScope(namespace, ProbeAnchor(relation))
    case other =>
      val children = other.children.map(encode(_, namespace))
      val rewritten = other.withNewChildren(children)
      if (children.exists(_.exists(_.isInstanceOf[ProbeScope]))) ProbeScope(namespace, rewritten)
      else rewritten
  }
}
