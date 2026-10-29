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

import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

/** Exercises native cache insertion and refresh without enrollment or a compilation envelope. */
class TestDeltaRangerRawMaintenanceIntegration {
  import RawNativeObservationFixture._

  @Test def localTargetFirstAppendDuplicateAndCopies(): Unit = scenario(dependentFirst = false)
  @Test def localDependentFirstAppendDuplicate(): Unit = scenario(dependentFirst = true)

  @Test def ordinaryProtocolAndActualSettingsAreRequired(): Unit = withEnabled(Seq("a", "q")) {
    (spark, native, _) =>
      active(native) { assertEquals(1, native.range(1).cache().collect().length) }
      Seq("a", "q").foreach(catalog => NativeSelectFixture.wrapper(spark, catalog))
      val mapped = active(native) {
        native.read.format("delta").load(NativeSelectFixture.locations("q"))
      }
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerRawMaintenance.normalize(mapped.queryExecution.analyzed, spark)
      )
      val raw = active(native) {
        native.read.format("delta").load(NativeSelectFixture.locations("a"))
      }
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerRawMaintenance.normalize(raw.queryExecution.analyzed, spark)
      )
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      spark.conf.set("spark.databricks.delta.asyncUpdate.stalenessTimeLimit", "1")
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerRawMaintenance.normalize(raw.queryExecution.analyzed, spark)
      )
  }

  @Test def rawMetadataReconstructionFromNativeHistory(): Unit = withEnabled() {
    (spark, native, _) =>
      active(native) { assertEquals(1, native.range(1).cache().collect().length) }
      val raw = active(native) {
        native.read.format("delta").load(NativeSelectFixture.locations("a"))
      }
      NativeSelectFixture.wrapper(spark, "a")
      val root = new QueryExecution(spark, raw.queryExecution.analyzed).executedPlan
        .asInstanceOf[DeltaRangerRawProducer]
      def field(target: AnyRef, name: String): AnyRef = {
        val member = target.getClass.getDeclaredField(name)
        member.setAccessible(true)
        member.get(target)
      }
      val owner = field(root.owner, "native").asInstanceOf[DeltaRangerRawNativeScans]
      val executions = field(owner.metadata, "executions")
        .asInstanceOf[java.util.IdentityHashMap[QueryExecution, _]]
      import scala.collection.JavaConverters._
      val recipes = executions.keySet().asScala.toVector.map(_.analyzed)
      val metadataGuard = executions
        .keySet()
        .asScala
        .toVector
        .flatMap(
          _.executedPlan.collect {
            case guard
                if guard.getClass.getSimpleName == "DeltaRangerCollectGuardExec" ||
                  guard.getClass.getSimpleName == "DeltaRangerRawMetadataGuardExec" =>
              guard
          }
        )
        .head
      val contextField = metadataGuard.getClass.getDeclaredField("driverContext")
      contextField.setAccessible(true)
      val context = contextField.get(metadataGuard)
      val constructor = metadataGuard.getClass.getDeclaredConstructors.head
      constructor.setAccessible(true)
      Seq(false, true).foreach { direct =>
        val callbacks = new AtomicInteger()
        val hostile = RawHostilePreparation(metadataGuard.output, callbacks)
        assertThrows(
          classOf[SecurityException],
          () => {
            val changed = if (direct) {
              try
                constructor
                  .newInstance(hostile, context)
                  .asInstanceOf[org.apache.spark.sql.execution.SparkPlan]
              catch {
                case failure: java.lang.reflect.InvocationTargetException => throw failure.getCause
              }
            } else metadataGuard.withNewChildren(Seq(hostile))
            changed.execute()
            ()
          }
        )
        assertEquals(0, callbacks.get(), "Raw metadata child prepared before rejection")
      }
      assertTrue(
        recipes.exists(
          _.collectLeaves()
            .exists(_.isInstanceOf[org.apache.spark.sql.execution.datasources.LogicalRelation])
        ),
        "Expected actual raw-owned log-segment replay before StateCache qe.toRdd"
      )
      val returns = field(owner.metadata, "returned").asInstanceOf[
        scala.collection.mutable.ArrayBuffer[
          (org.apache.spark.sql.execution.LogicalRDD, Vector[Any], Boolean, Vector[AnyRef])
        ]
      ]
      val original = returns.find(!_._3).get._1
      assertTrue(owner.metadata.contains(original, false))
      val changedOutput = original.copy(output =
        original.output.map(
          _.withExprId(org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId)
        )
      )(spark, None, None)
      assertFalse(owner.metadata.contains(changedOutput, false))
      val unowned = org.apache.spark.sql.execution.LogicalRDD(
        original.output,
        spark.sparkContext.emptyRDD[InternalRow]
      )(spark)
      assertFalse(owner.metadata.contains(unowned, false))
      val changedCopy = org.apache.spark.sql.execution.LogicalRDD(
        original.output,
        original.rdd.dependencies.head.rdd
          .asInstanceOf[RDD[InternalRow]]
          .map(row => row.copy())
      )(spark)
      assertFalse(owner.metadata.contains(changedCopy, false))
      val parentField = original.rdd.getClass.getDeclaredField("prev")
      parentField.setAccessible(true)
      val parent = parentField.get(original.rdd)
      parentField.set(original.rdd, spark.sparkContext.emptyRDD[InternalRow])
      try assertThrows(classOf[SecurityException], () => owner.metadata.contains(original, false))
      finally parentField.set(original.rdd, parent)
      assertTrue(owner.metadata.contains(original, false))
      val reconstruction = recipes
        .find(
          _.collectLeaves().exists(
            _.isInstanceOf[org.apache.spark.sql.execution.datasources.LogicalRelation]
          )
        )
        .get
      val changedOrder = reconstruction.transformDown {
        case sort: org.apache.spark.sql.catalyst.plans.logical.Sort =>
          sort.copy(order =
            sort.order.map(
              _.copy(
                direction = org.apache.spark.sql.catalyst.expressions.Descending
              )
            )
          )
      }
      assertThrows(
        classOf[SecurityException],
        () => owner.metadata.verifyReconstruction(changedOrder)
      )
      evidence(
        "semantics.txt",
        "nativeHistoryRawStateReplay=true foreignRDDAndCopyParentAndOutputAndReplayOrderDenied=true metadataQEs=" + recipes.size + "\n"
      )
      assertThrows(classOf[SecurityException], () => root.executeCollect())
  }

  @Test def classicCheckpointAndJsonTailReplayFromNativeHistory(): Unit = {
    val path = Files.createTempFile("raw-checkpoint-deployment", ".properties")
    Files.write(
      path,
      ("version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=1\n" +
        "table.0.metalake=lake\ntable.0.catalog=a\ntable.0.database=db\ntable.0.table=t\n")
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
    )
    Files.setPosixFilePermissions(
      path,
      java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")
    )
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          origins()
          val native = nativeSession(spark)
          active(native) { native.range(1).cache().collect() }
          val raw = active(native) {
            native.read.format("delta").load(NativeSelectFixture.locations("a"))
          }
          NativeSelectFixture.wrapper(spark, "a")
          val root = new QueryExecution(spark, raw.queryExecution.analyzed).executedPlan
            .asInstanceOf[DeltaRangerRawProducer]
          val head = root.owner.observation.selectedHead
          assertEquals(3L, head.version)
          assertEquals(2L, head.logSegment.checkpointProvider.version)
          assertEquals(
            Vector("00000000000000000003.json"),
            head.logSegment.deltas.map(_.getPath.getName).toVector
          )
          assertTrue(head.logSegment.checkpointProvider.topLevelFiles.nonEmpty)
          val ownerField = root.owner.getClass.getDeclaredField("native")
          ownerField.setAccessible(true)
          val owner = ownerField.get(root.owner).asInstanceOf[DeltaRangerRawNativeScans]
          val executionField = owner.metadata.getClass.getDeclaredField("executions")
          executionField.setAccessible(true)
          val executions = executionField
            .get(owner.metadata)
            .asInstanceOf[java.util.IdentityHashMap[QueryExecution, _]]
          import scala.collection.JavaConverters._
          val replay = executions
            .keySet()
            .asScala
            .toVector
            .find(qe =>
              qe.analyzed.collectLeaves().size == 2 && qe.analyzed
                .collectLeaves()
                .forall(_.isInstanceOf[org.apache.spark.sql.execution.datasources.LogicalRelation])
            )
          assertTrue(
            replay.nonEmpty,
            "Expected actual raw-owned classic checkpoint plus JSON replay"
          )
          val inputs = replay.get.analyzed
            .collectLeaves()
            .map(
              _.asInstanceOf[org.apache.spark.sql.execution.datasources.LogicalRelation].relation
                .asInstanceOf[org.apache.spark.sql.execution.datasources.HadoopFsRelation]
                .location
                .asInstanceOf[org.apache.spark.sql.delta.DeltaLogFileIndex]
                .files
                .map(_.getPath.getName)
                .toVector
            )
          assertEquals(
            Set(
              head.logSegment.deltas.map(_.getPath.getName).toVector,
              head.logSegment.checkpointProvider.topLevelFiles.map(_.getPath.getName).toVector
            ),
            inputs.toSet
          )
          val operation = DeltaRangerNativeOperation.rawPrepare(owner)
          try {
            val original = replay.get.analyzed
            val prefix = DeltaRangerDecisionReceipt.nativePrefix(original.clone(), spark)
            assertTrue(owner.metadata.executable(prefix))
            val originalIds = original.flatMap(_.output.map(_.exprId)).toSet
            val fresh = prefix
              .collect { case project: org.apache.spark.sql.catalyst.plans.logical.Project =>
                project.projectList.collect {
                  case alias: org.apache.spark.sql.catalyst.expressions.Alias
                      if alias.name == "add_path_canonical" && !originalIds(alias.exprId) =>
                    alias
                }
              }
              .flatten
              .head
            val alteredAlias = prefix.transformAllExpressions {
              case alias: org.apache.spark.sql.catalyst.expressions.Alias
                  if alias.exprId == fresh.exprId =>
                org.apache.spark.sql.catalyst.expressions.Alias(
                  org.apache.spark.sql.catalyst.expressions.Literal("foreign"),
                  alias.name
                )(
                  exprId = alias.exprId,
                  qualifier = alias.qualifier,
                  explicitMetadata = alias.explicitMetadata,
                  nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
                )
            }
            assertFalse(owner.metadata.executable(alteredAlias))
            val alteredSource =
              prefix.transformDown {
                case union: org.apache.spark.sql.catalyst.plans.logical.Union =>
                  union.copy(children = union.children.reverse)
              }
            assertFalse(owner.metadata.executable(alteredSource))
            val alteredMapping = prefix.transformAllExpressions {
              case alias: org.apache.spark.sql.catalyst.expressions.Alias
                  if alias.exprId == fresh.exprId =>
                org.apache.spark.sql.catalyst.expressions.Alias(alias.child, alias.name)(
                  exprId = original.output.head.exprId,
                  qualifier = alias.qualifier,
                  explicitMetadata = alias.explicitMetadata,
                  nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
                )
            }
            assertNotEquals(
              DeltaRangerNativeProvenance.binding(prefix),
              DeltaRangerNativeProvenance.binding(alteredMapping)
            )
            assertFalse(owner.metadata.executable(alteredMapping))
          } finally operation.close()
          assertThrows(classOf[SecurityException], () => root.executeCollect())
          evidence(
            "semantics.txt",
            "nativeHistoryRawCheckpointReplay=true checkpointVersion=2 headVersion=3 exactCheckpointAndJsonInputs=true alteredPrefixAliasSourceAndIdsDenied=true coldUserRowsProduced=0 metadataQEs=" + executions
              .size() + "\n"
          )
        },
        Some(NativeSelectFixture.extension(policy)),
        Map("spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> path.toString),
        beforeExtension = bootstrap => {
          val location = NativeSelectFixture.locations("a")
          bootstrap
            .sql(
              "ALTER TABLE delta.`" + location +
                "` SET TBLPROPERTIES ('delta.checkpointInterval'='2')"
            )
            .collect()
          (1 to 2).foreach(number =>
            bootstrap
              .sql(s"SELECT ${number + 10}L id, 'checkpoint-seed' secret, 'keep' region")
              .write
              .format("delta")
              .mode("append")
              .save(location)
          )
        }
      )
    finally { policy.engine.close(); Files.deleteIfExists(path) }
  }

  @Test def lateCallbacksAndUnrelatedOwnersFailClosed(): Unit = withEnabled() {
    (spark, native, _) =>
      active(native) { native.range(1).cache().collect() }
      NativeSelectFixture.wrapper(spark, "a")
      val raw = active(native) {
        native.read.format("delta").load(NativeSelectFixture.locations("a"))
      }
      val root = new QueryExecution(spark, raw.queryExecution.analyzed).executedPlan
        .asInstanceOf[DeltaRangerRawProducer]
      val scan = root.child.collectFirst {
        case scan: org.apache.spark.sql.execution.FileSourceScanExec => scan
      }.get
      val index =
        scan.relation.location.asInstanceOf[org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex]
      val unrelated = new DeltaRangerRawNativeScans(
        DeltaRangerRawNativeObservation.observe(spark, rawIndex(raw)),
        spark
      )
      val operation = DeltaRangerNativeOperation.rawPrepare(unrelated)
      try {
        assertThrows(classOf[SecurityException], () => index.inputFiles)
        assertThrows(classOf[SecurityException], () => index.sizeInBytes)
        assertThrows(
          classOf[SecurityException],
          () => root.owner.observation.requireCurrent(spark, unrelated)
        )
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerRawNativeObservation.observe(spark, rawIndex(raw))
        )
      } finally operation.close()
      active(native) {
        assertTrue(index.inputFiles.nonEmpty)
        assertTrue(index.sizeInBytes > 0)
        assertSame(native, SparkSession.active)
      }
      assertThrows(
        classOf[SecurityException],
        () => index.matchingFiles(Nil, Seq(org.apache.spark.sql.catalyst.expressions.Literal(true)))
      )
      assertThrows(classOf[SecurityException], () => root.executeCollect())
  }

  @Test def originalAuthorityAndSelectedInstalledIdentityCannotChange(): Unit = withEnabled() {
    (spark, native, _) =>
      active(native) { native.range(1).cache().collect() }
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      def raw = active(native) {
        native.read.format("delta").load(NativeSelectFixture.locations("a"))
      }
      val first = raw
      val token = "delta.registration.receipt.token"
      val previous = fixture.properties.get(token)
      try {
        assertThrows(
          classOf[SecurityException],
          () =>
            withSelectionCallback(_ => fixture.properties.put(token, "changed")) {
              new QueryExecution(spark, first.queryExecution.analyzed).executedPlan
            }
        )
      } finally fixture.properties.put(token, previous)
      val second = raw
      val before = rawIndex(second).deltaLog.unsafeVolatileSnapshot.version
      val once = new java.util.concurrent.atomic.AtomicBoolean()
      val selectionCallbacks = new AtomicInteger()
      assertThrows(
        classOf[SecurityException],
        () =>
          withSelectionCallback(selected => {
            selectionCallbacks.incrementAndGet()
            if (once.compareAndSet(false, true)) active(native) {
              native
                .sql("SELECT 9L id, 'native-new' secret, 'keep' region")
                .write
                .format("delta")
                .mode("append")
                .save(NativeSelectFixture.locations("a"))
            }
          }) { new QueryExecution(spark, second.queryExecution.analyzed).executedPlan }
      )
      assertTrue(once.get())
      assertEquals(1, selectionCallbacks.get())
      assertEquals(before + 1, rawIndex(second).deltaLog.unsafeVolatileSnapshot.version)
      assertNull(DeltaRangerNativeOperation.current())
      evidence(
        "semantics.txt",
        "selectionChangedAuthorityDenied=true selectedInstalledMismatchDenied=true nativeVersionAdvanced=true selectionCallbacks=1\n"
      )
  }

  @Test def constructorMetadataQueryHasNoSelectionAuthority(): Unit =
    withEnabled(storage = Map("spark.databricks.delta.writeChecksumFile.enabled" -> "false")) {
      (spark, native, _) =>
        active(native) { native.range(1).cache().collect() }
        val location = NativeSelectFixture.locations("a")
        val raw = active(native) { native.read.format("delta").load(location) }
        NativeSelectFixture.wrapper(spark, "a")
        active(native) {
          native
            .sql("SELECT 9L id, 'new' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
        }
        val counter = executionCounter("org.apache.spark.sql.execution.SQLExecution$")
        var executions = -1L
        val failure = assertThrows(
          classOf[SecurityException],
          () =>
            withSelectionCallback(_ => {
              val before = counter.get()
              try rawIndex(raw).deltaLog.getSnapshotAt(0)
              finally executions = counter.get() - before
            }) { new QueryExecution(spark, raw.queryExecution.analyzed).executedPlan }
        )
        assertEquals(0L, executions)
        assertTrue(
          failure.getStackTrace.exists(
            _.getMethodName.contains("protocolMetadataAndICTReconstruction")
          )
        )
        assertNull(DeltaRangerNativeOperation.current())
        evidence(
          "semantics.txt",
          "selectionConstructorMetadataDenied=true constructorSQLExecutions=0\n"
        )
    }

  private def executionCounter(name: String): java.util.concurrent.atomic.AtomicLong = {
    val companion = Class.forName(name)
    val field = companion.getDeclaredField("_nextExecutionId")
    field.setAccessible(true)
    field
      .get(companion.getField("MODULE$").get(null))
      .asInstanceOf[java.util.concurrent.atomic.AtomicLong]
  }

  private def withSelectionCallback[A](
      callback: org.apache.spark.sql.delta.stats.DeltaScanGenerator => Unit
  )(body: => A): A = {
    val module = org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
    val method = module.getClass.getDeclaredMethods
      .find(_.getName == "withCallbackOnGetDeltaScanGenerator")
      .get
    method.setAccessible(true)
    try method.invoke(module, callback, (() => body)).asInstanceOf[A]
    catch { case failure: java.lang.reflect.InvocationTargetException => throw failure.getCause }
  }

  @Test def excludedGrammarAndSecondReplacementCannotExecute(): Unit = withEnabled() {
    (spark, native, _) =>
      active(native) { native.range(1).cache().collect() }
      NativeSelectFixture.wrapper(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val raw = active(native) { native.read.format("delta").load(location) }
      val excluded = active(native) {
        Seq(
          raw.filter("id = 1"),
          raw.selectExpr("id as renamed"),
          raw.selectExpr("id + 1 as id"),
          raw.limit(0),
          raw.groupBy("region").count(),
          raw.union(raw)
        )
      }
      excluded.foreach(frame =>
        assertThrows(
          classOf[SecurityException],
          () => new QueryExecution(spark, frame.queryExecution.analyzed).executedPlan
        )
      )
      val root = new QueryExecution(spark, raw.queryExecution.analyzed).executedPlan
        .asInstanceOf[DeltaRangerRawProducer]
      val first = root.owner.observation.selectedHead
      active(native) {
        native
          .sql("SELECT 9L id, 'native-new' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .save(location)
      }
      val current = rawIndex(raw).deltaLog.unsafeVolatileSnapshot
      assertNotSame(first, current)
      assertEquals(first.version + 1, current.version)
      // Direct private-boundary negative: genuine native heads, no installed-state mutation.
      val constructor =
        classOf[DeltaRangerRawNativeScans.SelectionResult].getDeclaredConstructors.head
      constructor.setAccessible(true)
      val result = constructor
        .newInstance(root.owner.observation, current)
        .asInstanceOf[DeltaRangerRawNativeScans.SelectionResult]
      assertSame(current, result.selected)
      assertThrows(
        classOf[SecurityException],
        () => root.owner.observation.afterSelection(spark, result)
      )
      val failure = assertThrows(classOf[SecurityException], () => root.executeCollect())
      assertTrue(failure.getMessage.contains("Raw native observation rejected"))
      evidence(
        "semantics.txt",
        "excludedGrammarCases=6 directVersionContinuityDenied=true selectedIsInstalled=true secondFinalHeadReplacementDenied=true coldRowsProduced=0\n"
      )
  }

  @Test
  @EnabledIfSystemProperty(named = "raw.integration.foreignSerializer", matches = "true")
  def foreignActualSerializerRejectsAutomaticRefreshBeforeCallback(): Unit = {
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    val foreign = Map(
      "spark.sql.cache.serializer" -> classOf[RawCacheSerializerProbe.CountingSerializer].getName
    )
    val configuredDefault = java.lang.Boolean.getBoolean("raw.integration.serializerDefaultConfig")
    if (configuredDefault)
      NativeSelectFixture.withSpark(
        first => { first.range(1).cache().collect(); () },
        startupOptions = foreign
      )
    try
      withEnabled(storage = if (configuredDefault) Map.empty else foreign) {
        (spark, native, policy) =>
          NativeSelectFixture.catalogPolicies = Map("a" -> policy, "b" -> source)
          spark.conf.set(
            "spark.databricks.delta.write.txnAppId",
            "foreign-raw-" + UUID.randomUUID()
          )
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
          val location = NativeSelectFixture.locations("a")
          val raw = active(native) {
            val frame = native.read.format("delta").load(location).cache();
            assertEquals(3, frame.collect().length); frame
          }
          val entry = spark.sharedState.cacheManager.lookupCachedData(raw).get
          assertEquals(
            classOf[RawCacheSerializerProbe.CountingSerializer],
            entry.cachedRepresentation.cacheBuilder.serializer.getClass
          )
          assertEquals(
            if (configuredDefault)
              "org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer"
            else classOf[RawCacheSerializerProbe.CountingSerializer].getName,
            spark.conf.get("spark.sql.cache.serializer")
          )
          val field = classOf[RawCacheSerializerProbe].getDeclaredField("CALLBACKS")
          field.setAccessible(true)
          val callbacks = field.get(null).asInstanceOf[AtomicInteger]
          val beforeCallbacks = callbacks.get()
          val index = rawIndex(raw)
          val version = index.deltaLog.unsafeVolatileSnapshot.version
          val failure = assertThrows(
            classOf[SecurityException],
            () =>
              spark
                .sql("SELECT id, secret, region FROM b.db.t")
                .write
                .mode("append")
                .insertInto("a.db.t")
          )
          assertTrue(failure.getMessage.contains("serializer admission rejected"), failure.toString)
          assertEquals(version + 1, index.deltaLog.unsafeVolatileSnapshot.version)
          assertEquals(beforeCallbacks, callbacks.get())
          assertFalse(entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          assertTrue(spark.sharedState.cacheManager.lookupCachedData(raw).isEmpty)
          assertNull(DeltaRangerNativeOperation.current())
          evidence(
            "semantics.txt",
            "foreignActualSerializer=true configuredDefault=" + java.lang.Boolean
              .getBoolean("raw.integration.serializerDefaultConfig") +
              " automaticRefreshDenied=true additionalSupportsCallbacks=0 published=true oldBuilderUnloaded=true targetEntryAbsent=true\n"
          )
      }
    finally source.engine.close()
  }

  @Test
  @Tag("gravitino-docker-test")
  @EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
  def realS3AppendDuplicateBothOrders(): Unit = {
    val properties = new Properties()
    val input =
      Files.newInputStream(Paths.get(System.getProperty("gravitino.delta.test.s3.credentialsFile")))
    try properties.load(input)
    finally input.close()
    val storage = Map(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "fs.s3a.endpoint" -> System.getProperty("gravitino.delta.test.s3.endpoint"),
      "fs.s3a.path.style.access" -> "true",
      "fs.s3a.connection.ssl.enabled" -> "false",
      "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "fs.s3a.access.key" -> properties.getProperty("MINIO_ROOT_USER"),
      "fs.s3a.secret.key" -> properties.getProperty("MINIO_ROOT_PASSWORD")
    )
    assertTrue(storage.values.forall(value => value != null && value.nonEmpty))
    Seq(false, true).foreach(order => scenario(order, storage))
  }

  private[ranger] def scenario(
      dependentFirst: Boolean,
      storage: Map[String, String] = Map.empty,
      adaptive: Boolean = false
  ): Unit = {
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    val trace = InstalledAppendBoundaryProbe.fresh(
      s"raw-maintenance-aqe-$adaptive-project-first-$dependentFirst"
    )
    try
      withEnabled(
        Seq("a"),
        Map("spark.sql.adaptive.enabled" -> adaptive.toString) ++ storage.map { case (key, value) =>
          ("spark.hadoop." + key) -> value
        }
      ) { (spark, native, policy) =>
        trace.activate(spark)
        NativeSelectFixture.catalogPolicies = Map("a" -> policy, "b" -> source)
        val s3Root =
          if (storage.nonEmpty)
            Some(
              new Path(
                "s3a://" +
                  System.getProperty(
                    "gravitino.delta.test.s3.bucket"
                  ) + "/raw-maintenance-integration/" + UUID.randomUUID()
              )
            )
          else None
        try {
          s3Root.foreach { root =>
            active(native) {
              Seq("a", "b").foreach { catalog =>
                val location = new Path(root, catalog).toString
                native
                  .sql(
                    s"SELECT 1L id, '$catalog-one' secret, 'keep' region UNION ALL " +
                      s"SELECT 2L, '$catalog-two', 'drop' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'keep'"
                  )
                  .write
                  .format("delta")
                  .save(location)
                NativeSelectFixture.locations += catalog -> location
              }
            }
            Seq(
              "org.apache.hadoop.fs.s3a.S3AFileSystem",
              "com.amazonaws.auth.BasicAWSCredentials"
            )
              .foreach { name =>
                val cls = Class.forName(name, false, classOf[DeltaLog].getClassLoader)
                evidence(
                  "loaded-origins.txt",
                  name + "=" + cls.getProtectionDomain.getCodeSource.getLocation + "\n"
                )
              }
          }
          spark.conf.set(
            "spark.databricks.delta.write.txnAppId",
            "raw-maintenance-" + UUID.randomUUID()
          )
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
          val location = NativeSelectFixture.locations("a")
          val (target, dependent, unrelated) = active(native) {
            val target = native.read.format("delta").load(location)
            val dependent = target.select("id", "region")
            val ordered = if (dependentFirst) Seq(dependent, target) else Seq(target, dependent)
            ordered.foreach(frame => { frame.cache(); assertEquals(3, frame.collect().length) })
            val unrelated = native.range(4).toDF().cache()
            assertEquals(4, unrelated.collect().length)
            (target, dependent, unrelated)
          }
          val frames = Vector(target, dependent)
          def entries =
            frames.map(frame => spark.sharedState.cacheManager.lookupCachedData(frame).get)
          val old = entries
          val unrelatedEntry = spark.sharedState.cacheManager.lookupCachedData(unrelated).get
          old.foreach(entry =>
            assertTrue(entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          )
          val originalIndex = rawIndex(target)
          val oldHead = originalIndex.deltaLog.unsafeVolatileSnapshot
          def append(): Unit = spark
            .sql("SELECT id, secret, region FROM b.db.t")
            .write
            .mode("append")
            .insertInto("a.db.t")
          append()
          assertNull(DeltaRangerNativeOperation.current())
          assertTrue(DeltaRangerAppendCommit.current.isEmpty)
          assertTrue(OptimisticTransaction.getActive.isEmpty)
          val head = originalIndex.deltaLog.unsafeVolatileSnapshot
          assertEquals(oldHead.version + 1, head.version)
          val fresh = entries
          def replaced(previous: Vector[org.apache.spark.sql.execution.CachedData]): Unit = {
            entries.zip(previous).foreach { case (current, before) =>
              assertSame(before.plan, current.plan)
              assertNotSame(
                before.cachedRepresentation.cacheBuilder,
                current.cachedRepresentation.cacheBuilder
              )
              assertNotSame(
                before.cachedRepresentation.cacheBuilder.cachedPlan,
                current.cachedRepresentation.cacheBuilder.cachedPlan
              )
              assertFalse(before.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertFalse(current.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              TrustedAqeMaintenanceObservation
                .producer(current.cachedRepresentation.cacheBuilder.cachedPlan)
            }
            assertSame(
              unrelatedEntry.cachedRepresentation.cacheBuilder,
              spark.sharedState.cacheManager
                .lookupCachedData(unrelated)
                .get
                .cachedRepresentation
                .cacheBuilder
            )
            assertTrue(
              unrelatedEntry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded
            )
          }
          replaced(old)
          val root = fresh.head.cachedRepresentation.cacheBuilder.cachedPlan
            .asInstanceOf[DeltaRangerRawProducer]
          val preMembershipCopy = root.clone().asInstanceOf[DeltaRangerRawProducer]
          assertThrows(classOf[SecurityException], () => preMembershipCopy.executeCollect())
          root.owner.requireInstalled(root)
          def strip(
              node: org.apache.spark.sql.execution.SparkPlan
          ): org.apache.spark.sql.execution.SparkPlan = node match {
            case stage: org.apache.spark.sql.execution.WholeStageCodegenExec => strip(stage.child)
            case adapter: org.apache.spark.sql.execution.InputAdapter        => strip(adapter.child)
            case rows: org.apache.spark.sql.execution.ColumnarToRowExec      => strip(rows.child)
            case other                                                       => other
          }
          val prefixOnly = root
            .withNewChildren(Seq(strip(root.child)))
            .asInstanceOf[DeltaRangerRawProducer]
          assertTrue(
            assertThrows(classOf[SecurityException], () => prefixOnly.executeCollect()).getMessage
              .contains("incomplete raw physical tail")
          )
          val changedIds = root.child.transformDown {
            case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
              scan.copy(output =
                scan.output.map(
                  _.withExprId(
                    org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId
                  )
                )
              )
          }
          assertThrows(classOf[SecurityException], () => root.withNewChildren(Seq(changedIds)))
          val copy = root.clone().asInstanceOf[DeltaRangerRawProducer]
          assertNotSame(root, copy)
          assertThrows(classOf[SecurityException], () => copy.executeCollect())
          root.owner.requireInstalled(root)
          val callbacks = new AtomicInteger()
          val hostile = RawHostilePreparation(root.output, callbacks)
          assertThrows(classOf[SecurityException], () => root.withNewChildren(Seq(hostile)))
          assertEquals(0, callbacks.get())
          if (adaptive) {
            assertThrows(
              classOf[SecurityException],
              () => new QueryExecution(spark, fresh.head.plan).executedPlan.executeCollect()
            )
          } else {
            val standalone = new QueryExecution(spark, fresh.head.plan).executedPlan
            assertThrows(classOf[SecurityException], () => standalone.executeCollect())
          }
          val expected = Vector(
            Row(1L, "a-one", "keep"),
            Row(2L, "a-two", "drop"),
            Row(3L, null, "keep"),
            Row(1L, "b-one-masked", "keep"),
            Row(3L, null, "keep")
          )
          def counted(rows: Vector[Row]) =
            rows.groupBy(identity).map { case (row, values) => row -> values.size }
          def readback(): Unit = active(native) {
            def read = native.read.format("delta").load(location)
            assertEquals(counted(expected), counted(read.collect().toVector))
            assertSame(native, SparkSession.active)
            assertEquals(
              counted(expected.map(row => Row(row.getLong(0), row.getString(2)))),
              counted(read.select("id", "region").collect().toVector)
            )
            assertEquals(counted(expected), counted(read.collect().toVector))
          }
          readback()
          append()
          assertEquals(head.version, originalIndex.deltaLog.unsafeVolatileSnapshot.version)
          replaced(fresh)
          assertThrows(classOf[SecurityException], () => root.owner.requireInstalled(root))
          assertThrows(classOf[SecurityException], () => copy.executeCollect())
          entries.foreach { entry =>
            val next = TrustedAqeMaintenanceObservation.producer(
              entry.cachedRepresentation.cacheBuilder.cachedPlan
            )
            assertNotSame(root.owner, next.owner)
            assertSame(
              originalIndex.deltaLog.unsafeVolatileSnapshot,
              next.owner.observation.selectedHead
            )
          }
          readback()
          evidence(
            "semantics.txt",
            s"backend=${if (storage.isEmpty) "local" else "s3a-minio"} " +
              s"dependentFirst=$dependentFirst versionBefore=${oldHead.version} versionAfter=${head.version} " +
              "targetRows=5 dependentRows=5 duplicateVersionUnchanged=true exactKeys=true " +
              "buildersReplacedTwice=true unrelatedPreserved=true lateNativeReadback=true\n"
          )
        } finally
          s3Root.foreach(root =>
            root.getFileSystem(native.sessionState.newHadoopConf()).delete(root, true)
          )
      }
    finally { trace.close(); source.engine.close() }
  }
}

private[ranger] final case class RawHostilePreparation(
    override val output: Seq[Attribute],
    callbacks: AtomicInteger
) extends LeafExecNode {

  /** Counts any unsafe child preparation reached by the test. */
  override protected def doPrepare(): Unit = { callbacks.incrementAndGet(); () }

  /** The rejected child must never produce rows. */
  override protected def doExecute(): RDD[InternalRow] = throw new AssertionError(
    "foreign child executed"
  )
}
