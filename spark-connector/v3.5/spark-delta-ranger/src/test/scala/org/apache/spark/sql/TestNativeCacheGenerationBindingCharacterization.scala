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

// Test-only native cache membership characterization; never a production source.
package org.apache.spark.sql

import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.util.IdentityHashMap

import scala.collection.mutable
import scala.util.Properties

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.{
  CachedData,
  CacheManager,
  ColumnarRule,
  ColumnarToRowExec,
  InputAdapter,
  RowToColumnarExec,
  SparkPlan,
  SparkStrategy,
  UnaryExecNode,
  WholeStageCodegenExec
}
import org.apache.spark.sql.execution.columnar.{
  CachedRDDBuilder,
  DefaultCachedBatchSerializer,
  InMemoryRelation
}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.StorageLevel
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

/** Synthetic installed-membership characterization, not governed cache authorization. */
class TestNativeCacheGenerationBindingCharacterization {
  @Test
  def installedNativeBuilderBindsOnlyItsExactDraftAndRetainsRawReadback(): Unit = {
    // Omitted AQE uses the pinned native default. No ancestry inference grants authority.
    Seq(Some(false), None).foreach { adaptive =>
      Seq(false, true).foreach { vectorized =>
        withFixture(adaptive, vectorized) { (spark, location, probe) =>
          // Initialize through the ordinary native factory before synthetic maintenance admission.
          // The private reader never constructs a serializer or resets the module singleton.
          val bootstrap = spark.range(1L).cache()
          assertEquals(1L, bootstrap.count())
          val initialSerializer = probe.observeSerializer(spark, "ordinary-bootstrap-complete")
          assertSame(
            initialSerializer.serializer,
            spark.sharedState.cacheManager
              .lookupCachedData(bootstrap)
              .get
              .cachedRepresentation
              .cacheBuilder
              .serializer
          )
          val native = DeltaTableV2(spark, new HadoopPath(location)).toLogicalRelation
          val supplied = native.copy()
          val frame = Dataset.ofRows(spark, supplied)
          val key = frame.queryExecution.analyzed.asInstanceOf[LogicalRelation]
          // Expected native object and complete output were captured independently of key tags.
          val capability = probe.enroll(spark, native, key)
          val manager = spark.sharedState.cacheManager
          assertTrue(key.sameResult(native), "Must retain the genuine native Delta V1 key")
          assertTrue(manager.lookupCachedData(key).isEmpty)

          // A compatible unrelated session can compile; it gets no installed-cache authority.
          val standaloneSession = spark.newSession()
          capability.expectedConfiguration.foreach { case (name, value) =>
            if (!standaloneSession.sessionState.conf.getAllConfs.get(name).contains(value)) {
              assertFalse(SQLConf.isStaticConfigKey(name), "Cannot change a static setting")
              standaloneSession.conf.set(name, value)
            }
          }
          assertEquals(
            capability.expectedConfiguration,
            standaloneSession.sessionState.conf.getAllConfs
          )
          val standalone = probe.compiling("standalone") {
            standaloneSession.sessionState.executePlan(key).executedPlan
          }
          val standaloneGuard = probe.onlyGuard(standalone)
          assertSame(standalone, standaloneGuard, "The issued producer must be the outer guard")
          assertEquals("DRAFT", standaloneGuard.ticket.state)
          assertFalse(standaloneGuard.ticket.invocation.live)
          assertTrue(standaloneGuard.ticket.invocation.succeeded)
          assertTrue(standaloneGuard.ticket.hasPreparedRoot(standaloneGuard))
          val equivalentChild = standaloneGuard.child.clone()
          assertNotSame(standaloneGuard.child, equivalentChild)
          assertEquals(
            GenerationBindingProbe.physical(standaloneGuard.child),
            GenerationBindingProbe.physical(equivalentChild)
          )
          // Invoke the actual copy seam directly: TreeNode may elide equal-child replacements.
          assertThrows(
            classOf[IllegalStateException],
            () => standaloneGuard.copyPreparedChildForTest(equivalentChild)
          )
          assertTrue(
            standaloneGuard.ticket.hasPreparedRoot(standaloneGuard),
            "A closed-draft copy must not change the original prepared-root record"
          )
          assertEquals("DRAFT", standaloneGuard.ticket.state)
          assertThrows(classOf[IllegalStateException], () => standaloneGuard.executeColumnar())
          assertThrows(classOf[IllegalStateException], () => standalone.executeCollect())
          assertTrue(manager.lookupCachedData(key).isEmpty)

          val copied = key.copy()
          copied.copyTagsFrom(key)
          assertTrue(copied.sameResult(key))
          assertNotSame(key, copied)
          assertThrows(
            classOf[IllegalStateException],
            () => standaloneSession.sessionState.executePlan(copied).normalized
          )
          val changedOutput = key.copy(output = key.output.map(_.newInstance()))
          changedOutput.copyTagsFrom(key)
          assertTrue(changedOutput.sameResult(key), "ExprId changes preserve native matching")
          assertThrows(
            classOf[IllegalStateException],
            () => standaloneSession.sessionState.executePlan(changedOutput).normalized
          )
          assertThrows(
            classOf[IllegalStateException],
            () => probe.checkExpectedRecipe(native, changedOutput)
          )
          val changedSource =
            key.copy(relation = key.relation.asInstanceOf[HadoopFsRelation].copy()(spark))
          changedSource.copyTagsFrom(key)
          assertTrue(
            changedSource.sameResult(key),
            "Equal native relation copy is not source identity"
          )
          assertNotSame(key.relation, changedSource.relation)
          assertThrows(
            classOf[IllegalStateException],
            () => probe.checkExpectedRecipe(native, changedSource)
          )
          assertThrows(
            classOf[IllegalStateException],
            () => standaloneSession.sessionState.executePlan(changedSource).normalized
          )

          // Public native cache API retains this raw key; no scopes, anchors or replacement data.
          probe.compiling("native-cache-create") {
            manager.cacheQuery(
              spark,
              key,
              Some("raw-generation-probe"),
              StorageLevel.MEMORY_AND_DISK
            )
          }
          val first = manager.lookupCachedData(key).get
          assertSame(key, first.plan)
          assertSame(
            initialSerializer.serializer,
            first.cachedRepresentation.cacheBuilder.serializer
          )
          val firstGuard = probe.installedGuard(first)
          assertNotSame(standaloneGuard.ticket, firstGuard.ticket)
          assertNotSame(standaloneGuard, firstGuard)
          assertEquals(0, firstGuard.ticket.rowExecutions + firstGuard.ticket.columnarExecutions)
          assertFalse(first.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          assertFalse(firstGuard.ticket.invocation.live)
          assertEquals("DRAFT", firstGuard.ticket.state)
          assertEquals(0, firstGuard.ticket.lazySeals)
          assertThrows(classOf[IllegalStateException], () => standalone.executeCollect())
          assertEquals("DRAFT", standaloneGuard.ticket.state)
          probe.assertOutputMappings(first.cachedRepresentation)
          probe.record("first-installed-cold", first)

          // A native consumer is unrestricted. It is not an ordinary governed read-rule bypass.
          probe.assertRows(spark.read.format("delta").load(location), Seq((1L, 11L), (2L, 22L)))
          val firstBuilder = first.cachedRepresentation.cacheBuilder
          assertTrue(firstBuilder.isCachedColumnBuffersLoaded)
          val oldBuffers = firstBuilder.cachedColumnBuffers
          assertEquals("INSTALLED", firstGuard.ticket.state)
          assertEquals(1, firstGuard.ticket.lazySeals)
          assertEquals(Some("row"), firstGuard.ticket.lazySealGateway)
          assertTrue(
            firstGuard.ticket.rowExecutions > 0,
            "Stock default serializer uses row input even with a vectorized scan child"
          )
          assertEquals(
            0,
            firstGuard.ticket.columnarExecutions,
            "Normal default serialization must not be mislabeled as columnar input"
          )
          probe.record("first-loaded", first)

          val callerConfiguration = spark.sessionState.conf.getAllConfs
          probe.compiling("native-recache") { manager.recacheByPlan(spark, native) }
          assertEquals(callerConfiguration, spark.sessionState.conf.getAllConfs)
          val rebuilt = manager.lookupCachedData(key).get
          assertSame(
            initialSerializer.serializer,
            rebuilt.cachedRepresentation.cacheBuilder.serializer
          )
          val rebuiltGuard = probe.installedGuard(rebuilt)
          assertSame(key, rebuilt.plan)
          assertNotSame(firstBuilder, rebuilt.cachedRepresentation.cacheBuilder)
          assertNotSame(firstGuard.ticket, rebuiltGuard.ticket)
          assertNotSame(firstGuard, rebuiltGuard)
          assertEquals(StorageLevel.NONE, oldBuffers.getStorageLevel)
          assertFalse(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          assertEquals(
            0,
            rebuiltGuard.ticket.rowExecutions + rebuiltGuard.ticket.columnarExecutions
          )
          assertEquals(
            capability.expectedConfiguration,
            rebuiltGuard.ticket.compilationConfiguration
          )
          if (adaptive.contains(false)) assertSame(spark, rebuiltGuard.ticket.compilationSession)
          else assertNotSame(spark, rebuiltGuard.ticket.compilationSession)
          assertSame(spark.sharedState, rebuiltGuard.ticket.compilationSession.sharedState)
          assertFalse(rebuiltGuard.ticket.invocation.live)
          assertEquals("DRAFT", rebuiltGuard.ticket.state)
          assertEquals(0, rebuiltGuard.ticket.lazySeals)
          probe.record("replacement-cold-before-lazy-seal", rebuilt)
          assertThrows(classOf[IllegalStateException], () => firstGuard.executeCollect())
          assertEquals("RETIRED", firstGuard.ticket.state)
          assertThrows(classOf[IllegalStateException], () => standalone.executeCollect())
          probe.assertOutputMappings(rebuilt.cachedRepresentation)

          // No fixture seal call: the native producer gateway observes and seals its own winner.
          // The compilation invocation and recache call have both ended before materialization.
          probe.assertRows(spark.read.format("delta").load(location), Seq((1L, 11L), (2L, 22L)))
          probe.assertRows(spark.read.format("delta").load(location), Seq((1L, 11L), (2L, 22L)))
          assertTrue(rebuilt.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          assertEquals("INSTALLED", rebuiltGuard.ticket.state)
          assertEquals(1, rebuiltGuard.ticket.lazySeals)
          assertEquals(Some("row"), rebuiltGuard.ticket.lazySealGateway)
          assertTrue(rebuiltGuard.ticket.rowExecutions > 0)
          assertEquals(0, rebuiltGuard.ticket.columnarExecutions)
          assertNotEquals(
            oldBuffers.id,
            rebuilt.cachedRepresentation.cacheBuilder.cachedColumnBuffers.id
          )
          probe.record("replacement-loaded", rebuilt)
          if (vectorized) {
            probe.assertDirectColumnarRows(rebuiltGuard, Seq((1L, 11L), (2L, 22L)))
            assertEquals(1, rebuiltGuard.ticket.columnarExecutions)
          } else {
            assertThrows(classOf[IllegalStateException], () => rebuiltGuard.executeColumnar())
          }
          assertEquals(1, rebuiltGuard.ticket.lazySeals, "Repeated observation is idempotent")

          // A copied physical guard/tag cannot substitute for the installed root by equality.
          val copiedGuard = RawGenerationGuard(rebuiltGuard.child)(rebuiltGuard.ticket)
          copiedGuard.copyTagsFrom(rebuiltGuard)
          assertNotSame(rebuiltGuard, copiedGuard)
          assertThrows(classOf[IllegalStateException], () => copiedGuard.executeCollect())
          capability.close()
          assertEquals("RETIRED", rebuiltGuard.ticket.state)
          assertEquals("RETIRED", standaloneGuard.ticket.state)
          assertThrows(classOf[IllegalStateException], () => rebuiltGuard.executeCollect())
        }
      }
    }
  }

  private def withFixture(adaptive: Option[Boolean], vectorized: Boolean)(
      body: (SparkSession, String, GenerationBindingProbe) => Unit
  ): Unit = {
    val name = s"aqe-${adaptive.map(_.toString).getOrElse("native-default")}-columnar-$vectorized"
    val root = Files.createTempDirectory("native-cache-generation-" + name)
    val evidence = sys.env
      .get("GRAVITINO_DELTA_CACHE_GENERATION_EVIDENCE_DIR")
      .map(Paths.get(_))
      .getOrElse(root.resolve("evidence"))
    Files.createDirectories(evidence)
    val probe = new GenerationBindingProbe(
      evidence.resolve(name + "-" + System.nanoTime() + ".log")
    )
    require(
      SparkSession.getActiveSession.isEmpty && SparkSession.getDefaultSession.isEmpty,
      "Probe requires an exclusive fresh Spark fixture"
    )
    val builder = SparkSession
      .builder()
      .master("local[2]")
      .appName(name)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.databricks.delta.snapshotPartitions", "2")
      .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
      .config(SQLConf.AUTO_BUCKETED_SCAN_ENABLED.key, "false")
      .config(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key, "false")
      .config("spark.sql.parquet.enableVectorizedReader", vectorized.toString)
      .config("spark.sql.codegen.wholeStage", "true")
      .config(
        StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[DefaultCachedBatchSerializer].getName
      )
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.catalog.update.enabled", "false")
      .withExtensions(probe.install)
    adaptive.foreach(value => builder.config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, value))
    val spark = builder.getOrCreate()
    try {
      spark.sparkContext.setLogLevel("WARN")
      probe.note("fixture", root.toString)
      probe.note(
        "runtime",
        s"spark=${spark.version} scala=${Properties.versionNumberString}" +
          s" java=${System.getProperty("java.version")}" +
          s" sparkOrigin=${classOf[SparkSession].getProtectionDomain.getCodeSource.getLocation}" +
          s" deltaOrigin=${classOf[DeltaTableV2].getProtectionDomain.getCodeSource.getLocation}"
      )
      if (adaptive.isEmpty) {
        assertFalse(
          spark.sessionState.conf.getAllConfs.contains(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key)
        )
        assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled, "Pinned native AQE default")
      }
      val location = root.resolve("target").toString
      spark
        .sql("SELECT 1L id, 11L payload UNION ALL SELECT 2L id, 22L payload")
        .write
        .format("delta")
        .save(location)
      body(spark, location, probe)
    } catch {
      case failure: Throwable =>
        probe.note("failure", failure.toString)
        throw failure
    } finally {
      probe.close()
      spark.sharedState.cacheManager.clearCache()
      DeltaLog.clearCache()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      // Preserve native fixture/evidence directories, including failed runs.
    }
  }
}

/** Private test machinery; neither serialized tags nor table names mint authority. */
private[sql] object GenerationBindingProbe {
  val EnrollmentTag = TreeNodeTag[RawMaintenanceCapability]("test.raw-maintenance-enrollment")

  final class Identity(val value: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case identity: Identity => value eq identity.value
      case _                  => false
    }
    override def hashCode(): Int = System.identityHashCode(value)
  }

  def attribute(attribute: Attribute): Vector[Any] = Vector(
    attribute.getClass,
    attribute.name,
    attribute.exprId,
    attribute.qualifier,
    attribute.dataType.json,
    attribute.nullable,
    attribute.metadata.json
  )

  // Ignore only pinned transparent preparation layers; retain all executable operator fields.
  def physical(plan: SparkPlan): Any = plan match {
    case node: WholeStageCodegenExec => physical(node.child)
    case node: InputAdapter          => physical(node.child)
    case node: ColumnarToRowExec     => physical(node.child)
    case node: RowToColumnarExec     => physical(node.child)
    case node =>
      Vector(
        node.getClass,
        node.productIterator.map(value).toVector,
        node.output.map(attribute).toVector,
        node.children.map(physical).toVector
      )
  }

  private def value(item: Any): Any = item match {
    case null            => null
    case node: SparkPlan => physical(node)
    case attr: Attribute => attribute(attr)
    case fs: HadoopFsRelation =>
      Vector(
        new Identity(fs),
        new Identity(fs.location),
        new Identity(fs.fileFormat),
        fs.dataSchema.json,
        fs.partitionSchema.json,
        fs.options
      )
    case expression: Expression =>
      Vector(expression.getClass, expression.productIterator.map(value).toVector)
    case seq: Seq[_]      => seq.map(value).toVector
    case map: Map[_, _]   => map.map { case (key, valueItem) => value(key) -> value(valueItem) }
    case array: Array[_]  => array.map(value).toVector
    case product: Product => Vector(product.getClass, product.productIterator.map(value).toVector)
    case string: String   => string
    case number: java.lang.Number   => number
    case boolean: java.lang.Boolean => boolean
    case reference: AnyRef          => new Identity(reference)
    case scalar                     => scalar
  }
}

/** Synthetic capability minted only by explicit test enrollment; it is not a SELECT receipt. */
private[sql] final class RawMaintenanceCapability private[sql] (
    val probe: GenerationBindingProbe,
    val ownerSession: SparkSession,
    val expectedNative: LogicalRelation,
    val key: LogicalRelation,
    val expectedConfiguration: Map[String, String],
    val expectedSerializer: RawSerializerObservation
) {
  val tickets: mutable.ArrayBuffer[RawGenerationTicket] = mutable.ArrayBuffer.empty
  var installed: Option[RawGenerationTicket] = None
  private var live = true

  def admit(session: SparkSession, candidate: LogicalPlan): RawGenerationTicket = {
    requireLive()
    // This is before qe.optimizedPlan returns to either native InMemoryRelation factory.
    val serializer = probe.observeSerializer(session, "normalization-before-draft")
    expectedSerializer.requireSame(serializer)
    require(candidate eq key, "Copied or unknown stored recipe identity")
    probe.checkExpectedRecipe(expectedNative, candidate)
    require(session.sharedState eq ownerSession.sharedState, "Foreign shared cache domain")
    require(
      session.sessionState.conf.getAllConfs == expectedConfiguration,
      "Compilation configuration differs from exact qualified profile"
    )
    val ticket = new RawGenerationTicket(
      this,
      session,
      expectedConfiguration,
      serializer,
      probe.currentInvocation()
    )
    tickets += ticket
    ticket
  }

  def requireLive(): Unit = {
    if (!live) throw new IllegalStateException("Synthetic maintenance owner closed")
  }

  def close(): Unit = synchronized {
    live = false
    tickets.foreach(_.retire())
  }
}

/** Exact read-only observation; the object identities, not names, bind the actual serializer. */
private[sql] final case class RawSerializerObservation(
    module: AnyRef,
    serializer: DefaultCachedBatchSerializer
) {
  def requireSame(other: RawSerializerObservation): Unit = {
    if (!(module eq other.module) || !(serializer eq other.serializer))
      throw new IllegalStateException("Actual native serializer singleton changed")
  }
}

/** Synthetic finite caller envelope; closing it never seals any producer or grants row authority.
  */
private[sql] final class RawCompilationInvocation(val name: String) {
  var live = true
  var succeeded = false
}

/** Lazy exact native membership; no fixture/refresh-return seal call authorizes execution. */
private[sql] final class RawGenerationTicket(
    val capability: RawMaintenanceCapability,
    val compilationSession: SparkSession,
    val compilationConfiguration: Map[String, String],
    val serializerObservation: RawSerializerObservation,
    val invocation: RawCompilationInvocation
) {
  private var status = "DRAFT"
  private var expectedPhysical: Option[Any] = None
  private var expectedOrdering: Option[Seq[SortOrder]] = None
  private var preparedRoot: Option[RawGenerationGuard] = None
  private var boundBuilder: Option[CachedRDDBuilder] = None
  var rowExecutions = 0
  var columnarExecutions = 0
  var lazySeals = 0
  var lazySealGateway: Option[String] = None

  def state: String = status

  def hasPreparedRoot(root: RawGenerationGuard): Boolean = preparedRoot.exists(_ eq root)

  def freezePostColumnar(guard: RawGenerationGuard): Unit = {
    require(status == "DRAFT" && (guard.ticket eq this), "Wrong physical draft")
    serializerObservation.requireSame(
      capability.probe.observeSerializer(compilationSession, "post-columnar-before-installation")
    )
    val binding = GenerationBindingProbe.physical(guard.child)
    expectedPhysical match {
      case Some(expected) => require(expected == binding, "Physical draft changed before sealing")
      case None           => expectedPhysical = Some(binding)
    }
    preparedRoot = Some(guard)
    expectedOrdering = Some(guard.outputOrdering.toVector)
    capability.probe.note("post-columnar-draft", guard.treeString)
  }

  /** Accept only the exact stock preparation's structural copy of the frozen child. */
  def preparationCopy(previous: RawGenerationGuard, next: RawGenerationGuard): Unit = {
    if (preparedRoot.exists(_ eq previous)) {
      if (
        status != "DRAFT" || !capability.probe.isCurrentInvocation(invocation) ||
        !expectedPhysical.contains(GenerationBindingProbe.physical(next.child))
      )
        throw new IllegalStateException("Producer copy is outside its exact active compilation")
      preparedRoot = Some(next)
    }
  }

  def requireInstalled(root: RawGenerationGuard, gateway: String): Unit = {
    capability.requireLive()
    if (
      status == "RETIRED" || invocation.live || !invocation.succeeded ||
      !preparedRoot.exists(_ eq root) ||
      !expectedPhysical.contains(GenerationBindingProbe.physical(root.child)) ||
      !expectedOrdering.contains(root.outputOrdering)
    )
      throw new IllegalStateException("Incomplete, active, copied, changed or retired raw draft")
    // Do not read cachedColumnBuffers: even cold observation would construct its lazy RDD.
    def currentEntry(): CachedData = {
      val current = capability.ownerSession.sharedState.cacheManager
        .lookupCachedData(capability.key)
        .getOrElse {
          if (status == "INSTALLED") retire()
          throw new IllegalStateException("No installed native generation")
        }
      if (
        !(current.plan eq capability.key) ||
        !(current.cachedRepresentation.cacheBuilder.cachedPlan eq root)
      ) {
        if (status == "INSTALLED") retire()
        throw new IllegalStateException("Exact installed K/P differs from this producer")
      }
      current
    }
    val entry = currentEntry()
    val builder = entry.cachedRepresentation.cacheBuilder
    serializerObservation.requireSame(
      capability.probe.observeSerializer(compilationSession, "producer-before-child-execution")
    )
    if (
      !(builder.serializer eq serializerObservation.serializer) ||
      builder.storageLevel != StorageLevel.MEMORY_AND_DISK ||
      !builder.tableName.contains("raw-generation-probe") ||
      root.output.map(GenerationBindingProbe.attribute) !=
        capability.key.output.map(GenerationBindingProbe.attribute) ||
        !expectedOrdering.contains(entry.cachedRepresentation.outputOrdering)
    )
      throw new IllegalStateException("Installed serializer/builder/full output changed")
    val repeated = currentEntry()
    if (!(repeated.cachedRepresentation.cacheBuilder eq builder))
      throw new IllegalStateException("Native builder changed during lazy observation")
    // No native/cache/module monitor is held while the connector state transitions.
    capability.synchronized {
      capability.requireLive()
      if (status == "DRAFT") {
        boundBuilder = Some(builder)
        status = "INSTALLED"
        lazySeals += 1
        lazySealGateway = Some(gateway)
        capability.installed.filterNot(_ eq this).foreach(_.retire())
        capability.installed = Some(this)
      } else if (status != "INSTALLED" || !boundBuilder.exists(_ eq builder)) {
        throw new IllegalStateException("Draft cannot attach to another generation")
      }
    }
    capability.probe.note(
      "lazy-membership",
      s"state=$status seals=$lazySeals" +
        s" gateway=$gateway compilerClosed=${!invocation.live} source=${invocation.name}"
    )
  }

  def retire(): Unit = status = "RETIRED"
}

/** Normalization-only carrier; the stored native CacheManager key remains unchanged. */
private[sql] case class RawCompilationCarrier(child: LogicalPlan)(
    @transient val ticket: RawGenerationTicket
) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(ticket)
  override protected def withNewChildInternal(newChild: LogicalPlan): RawCompilationCarrier =
    RawCompilationCarrier(newChild)(ticket)
}

/** Non-codegen visible producer guard that delegates both native row and columnar paths. */
private[sql] case class RawGenerationGuard(child: SparkPlan)(
    @transient val ticket: RawGenerationTicket
) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  // DefaultCachedBatchSerializer consumes rows. Remain the non-transition outer row root.
  // A separate explicitly invoked columnar gateway below tests the qualified native counterpart.
  override def supportsColumnar: Boolean = false
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(ticket)
  override def executeCollect(): Array[InternalRow] = {
    context().requireInstalled(this, "collect")
    super.executeCollect()
  }
  override protected def doExecute(): RDD[InternalRow] = {
    context().requireInstalled(this, "row")
    ticket.rowExecutions += 1
    child.execute()
  }
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    context().requireInstalled(this, "columnar")
    val columnar = InMemoryRelation.convertToColumnarIfPossible(child)
    if (
      !columnar.supportsColumnar || columnar.output != output ||
      GenerationBindingProbe.physical(columnar) != GenerationBindingProbe.physical(child)
    )
      throw new IllegalStateException("No qualified native columnar counterpart")
    ticket.columnarExecutions += 1
    columnar.executeColumnar()
  }
  override protected def withNewChildInternal(newChild: SparkPlan): RawGenerationGuard = {
    val copied = RawGenerationGuard(newChild)(ticket)
    if (ticket != null) ticket.preparationCopy(this, copied)
    copied
  }

  /** Deterministically exercise the protected copy seam without TreeNode equal-child elision. */
  def copyPreparedChildForTest(newChild: SparkPlan): RawGenerationGuard =
    withNewChildInternal(newChild)

  private def context(): RawGenerationTicket = {
    if (ticket == null || isCanonicalizedPlan)
      throw new IllegalStateException("Inert raw generation evidence")
    ticket
  }
}

/** Captures exact driver references and append-only observations for this bounded fixture. */
private[sql] final class GenerationBindingProbe(evidence: Path) {
  private val log = LoggerFactory.getLogger(classOf[GenerationBindingProbe])
  private val capabilities = new IdentityHashMap[LogicalPlan, RawMaintenanceCapability]()
  var phase = "outside"
  private var invocation: Option[RawCompilationInvocation] = None

  def compiling[A](name: String)(body: => A): A = {
    require(invocation.isEmpty, "No reentrant synthetic compilation envelope")
    val scope = new RawCompilationInvocation(name)
    invocation = Some(scope)
    phase = name
    try {
      val result = body
      scope.succeeded = true
      result
    } finally {
      scope.live = false
      invocation = None
      phase = "outside"
      note("compiler-invocation-closed", name)
    }
  }

  def currentInvocation(): RawCompilationInvocation = invocation
    .filter(_.live)
    .getOrElse(throw new IllegalStateException("No synthetic compilation invocation"))

  def isCurrentInvocation(expected: RawCompilationInvocation): Boolean =
    expected.live && invocation.exists(_ eq expected)

  def observeSerializer(session: SparkSession, label: String): RawSerializerObservation = {
    val stock = classOf[InMemoryRelation]
    val loader = stock.getClassLoader
    val origin = Option(stock.getProtectionDomain.getCodeSource)
      .getOrElse(throw new IllegalStateException("Missing pinned Spark SQL code source"))
      .getLocation
    val moduleClass =
      Class.forName("org.apache.spark.sql.execution.columnar.InMemoryRelation$", false, loader)
    if (
      !(moduleClass.getClassLoader eq loader) ||
      moduleClass.getProtectionDomain.getCodeSource == null ||
      moduleClass.getProtectionDomain.getCodeSource.getLocation != origin ||
      session.sharedState.cacheManager.getClass != classOf[CacheManager] ||
      !(classOf[CacheManager].getClassLoader eq loader) ||
      classOf[CacheManager].getProtectionDomain.getCodeSource == null ||
      classOf[CacheManager].getProtectionDomain.getCodeSource.getLocation != origin ||
      !(classOf[DefaultCachedBatchSerializer].getClassLoader eq loader) ||
      classOf[DefaultCachedBatchSerializer].getProtectionDomain.getCodeSource == null ||
      classOf[
        DefaultCachedBatchSerializer
      ].getProtectionDomain.getCodeSource.getLocation != origin ||
      session.sessionState.conf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER) !=
        classOf[DefaultCachedBatchSerializer].getName
    )
      throw new IllegalStateException("Unqualified actual native serializer runtime/configuration")
    val module = moduleClass.getField("MODULE$").get(null).asInstanceOf[AnyRef]
    if (module == null || module.getClass != moduleClass || !(module eq InMemoryRelation))
      throw new IllegalStateException("Unexpected native module instance")
    val field = moduleClass.getDeclaredField("ser")
    if (
      field.getType != classOf[Option[_]] || Modifier.isStatic(field.getModifiers) ||
      !Modifier.isPrivate(field.getModifiers)
    )
      throw new IllegalStateException("Unexpected native singleton field shape")
    field.setAccessible(
      true
    ) // Pinned private READ only: never write/reset/initialize a serializer.
    val observation = module.synchronized {
      field.get(module) match {
        case some: Some[_] if some.getClass == classOf[Some[_]] =>
          val actual = some.value.asInstanceOf[AnyRef]
          if (actual == null || actual.getClass != classOf[DefaultCachedBatchSerializer])
            throw new IllegalStateException(
              "Actual native singleton is not the exact stock default"
            )
          RawSerializerObservation(module, actual.asInstanceOf[DefaultCachedBatchSerializer])
        case _ => throw new IllegalStateException("Absent or malformed actual native serializer")
      }
    }
    note(
      label,
      s"M=${identity(observation.module)} S=${identity(observation.serializer)} origin=$origin"
    )
    observation
  }

  def install(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlanNormalizationRule { spark =>
      new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = {
          val registered = Option(capabilities.get(plan))
          val carried = plan.getTagValue(GenerationBindingProbe.EnrollmentTag)
          if (registered.isEmpty && carried.isEmpty) plan
          else {
            val capability = registered.orElse(carried).get
            if (!registered.exists(_ eq capability))
              throw new IllegalStateException("Copied enrollment tag is not registration")
            val ticket = capability.admit(spark, plan)
            note(
              "admitted-compilation",
              s"session=${identity(spark)} key=${identity(plan)}" +
                s" ticket=${identity(ticket)} conf=${ticket.compilationConfiguration.toSeq.sorted}"
            )
            RawCompilationCarrier(plan)(ticket)
          }
        }
      }
    }
    extensions.injectPlannerStrategy { _ =>
      new SparkStrategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
          case carrier: RawCompilationCarrier =>
            if (carrier.ticket == null) throw new IllegalStateException("Inert compilation carrier")
            Seq(RawGenerationGuard(planLater(carrier.child))(carrier.ticket))
          case _ => Nil
        }
      }
    }
    extensions.injectColumnar { _ =>
      new ColumnarRule {
        override def postColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
          override def apply(plan: SparkPlan): SparkPlan = {
            plan.collect { case guard: RawGenerationGuard => guard }.foreach { guard =>
              guard.ticket.freezePostColumnar(guard)
            }
            plan
          }
        }
      }
    }
  }

  def enroll(
      spark: SparkSession,
      expected: LogicalRelation,
      key: LogicalRelation
  ): RawMaintenanceCapability = {
    checkExpectedRecipe(expected, key)
    require(!capabilities.containsKey(key), "Already enrolled")
    val original = spark.sessionState.conf.getAllConfs
    val configuration =
      if (spark.sessionState.conf.adaptiveExecutionEnabled)
        original.updated(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      else original
    val capability = new RawMaintenanceCapability(
      this,
      spark,
      expected,
      key,
      configuration,
      observeSerializer(spark, "synthetic-enrollment")
    )
    capabilities.put(key, capability)
    key.setTagValue(GenerationBindingProbe.EnrollmentTag, capability)
    capability
  }

  def checkExpectedRecipe(expected: LogicalRelation, candidate: LogicalPlan): Unit =
    candidate match {
      case actual: LogicalRelation if actual.getClass == classOf[LogicalRelation] =>
        if (
          !(actual.relation eq expected.relation) || actual.catalogTable != expected.catalogTable ||
          actual.isStreaming != expected.isStreaming || actual.children.nonEmpty ||
          actual.output.map(GenerationBindingProbe.attribute) !=
            expected.output.map(GenerationBindingProbe.attribute)
        )
          throw new IllegalStateException("Native recipe/source/full output changed")
      case _ =>
        throw new IllegalStateException("Only the exact enrolled native Delta leaf is qualified")
    }

  def onlyGuard(plan: SparkPlan): RawGenerationGuard = {
    val guards = plan.collect { case guard: RawGenerationGuard => guard }
    assertEquals(1, guards.size, "Exactly one generation guard must survive physical preparation")
    guards.head
  }

  def installedGuard(entry: CachedData): RawGenerationGuard = {
    val root = entry.cachedRepresentation.cacheBuilder.cachedPlan
    assertTrue(
      root.isInstanceOf[RawGenerationGuard],
      "Native columnar conversion must retain the guard as exact builder root"
    )
    assertSame(root, onlyGuard(root))
    root.asInstanceOf[RawGenerationGuard]
  }

  def assertOutputMappings(source: InMemoryRelation): Unit = {
    val remapped = source.output.map(_.newInstance())
    val copied = source.withOutput(remapped)
    val fresh = source.newInstance()
    validateOutput(source, copied, remapped)
    validateOutput(source, fresh, fresh.output)
    assertNotEquals(source.output.map(_.exprId), fresh.output.map(_.exprId))
    assertThrows(
      classOf[IllegalStateException],
      () => validateOutput(source, source.withOutput(remapped.reverse), remapped)
    )
    assertThrows(
      classOf[IllegalStateException],
      () => validateOutput(source, source.withOutput(remapped.reverse), remapped.reverse)
    )
  }

  def assertRows(frame: DataFrame, expected: Seq[(Long, Long)]): Unit = {
    val actual = frame.collect().map(row => (row.getLong(0), row.getLong(1))).sorted.toSeq
    note(
      "literal-readback",
      s"actual=$actual expected=$expected physical=${frame.queryExecution.executedPlan.treeString}"
    )
    assertEquals(expected.sorted, actual)
  }

  def assertDirectColumnarRows(root: RawGenerationGuard, expected: Seq[(Long, Long)]): Unit = {
    // Explicit producer API coverage, not DefaultCachedBatchSerializer columnar conversion.
    val actual = root
      .executeColumnar()
      .mapPartitions { batches =>
        batches.flatMap { batch =>
          (0 until batch.numRows()).iterator.map { ordinal =>
            (batch.column(0).getLong(ordinal), batch.column(1).getLong(ordinal))
          }
        }
      }
      .collect()
      .sorted
      .toSeq
    note("explicit-columnar-gateway-readback", s"actual=$actual expected=$expected")
    assertEquals(expected.sorted, actual)
  }

  def record(label: String, entry: CachedData): Unit = {
    val builder = entry.cachedRepresentation.cacheBuilder
    val loaded = builder.isCachedColumnBuffersLoaded
    val buffer = if (loaded) s"rdd=${builder.cachedColumnBuffers.id}" else "rdd=unobserved-cold"
    note(
      label,
      s"K=${identity(entry.plan)} B=${identity(builder)} P=${identity(builder.cachedPlan)}" +
        s" ticket=${identity(installedGuard(entry).ticket)} loaded=$loaded $buffer" +
        s" serializer=${builder.serializer.getClass.getName} storage=${builder.storageLevel}" +
        s" physical=${builder.cachedPlan.treeString}"
    )
  }

  def close(): Unit = {
    val iterator = capabilities.values().iterator()
    while (iterator.hasNext) iterator.next().close()
  }

  def note(label: String, value: String): Unit = synchronized {
    val line = s"$phase\t$label\t${value.replace("\n", "\\n")}\n"
    Files.write(
      evidence,
      line.getBytes(UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND
    )
    log.info(s"Native generation probe evidence $evidence: ${line.trim}")
  }

  private def identity(value: AnyRef): String = value.getClass.getName + "@" +
    Integer.toHexString(System.identityHashCode(value))

  private def validateOutput(
      source: InMemoryRelation,
      candidate: InMemoryRelation,
      expected: Seq[Attribute]
  ): Unit = {
    def ordinal(attribute: Attribute): Vector[Any] = Vector(
      attribute.getClass,
      attribute.name,
      attribute.qualifier,
      attribute.dataType.json,
      attribute.nullable,
      attribute.metadata.json
    )
    if (
      !(source.cacheBuilder eq candidate.cacheBuilder) || candidate.output != expected ||
      source.output.map(ordinal) != expected.map(ordinal)
    )
      throw new IllegalStateException("Builder or exact ordinal output mapping changed")
  }
}
