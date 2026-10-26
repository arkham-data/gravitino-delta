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

import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec,
  ShuffleQueryStageExec,
  AQEShuffleReadExec
}
import org.apache.spark.sql.execution.exchange.{
  ShuffleExchangeExec,
  ReusedExchangeExec,
  BroadcastExchangeExec,
  Exchange
}
import org.apache.spark.sql.execution.{ProjectExec, InputAdapter, ColumnarToRowExec}
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.CoalescedPartitionSpec
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{
  SparkPlan,
  SparkStrategy,
  FilterExec,
  FileSourceScanExec,
  WholeStageCodegenExec
}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import org.apache.spark.sql.catalyst.expressions.{CaseWhen, Not, IsNotNull, ScalaUDF}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.expressions.{Literal, EqualTo}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerPhysicalAdmission {
  @Test
  def unknownColumnarRulesRejectBeforeInvocation(): Unit = {
    val calls = new AtomicInteger()
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val qualification = assertThrows(
            classOf[SecurityException],
            () => DeltaRangerPhysicalPlanning.qualify(spark)
          )
          assertTrue(
            qualification.getMessage.contains("unqualified columnar rules"),
            qualification.getMessage
          )
          assertThrows(
            classOf[DeltaRangerNativeOperation.OptimizerRejected],
            () => spark.sql("SELECT id FROM b.db.t").collect()
          )
          assertEquals(0, calls.get())
        },
        Some(extensions => {
          NativeSelectFixture.extension(policy)(extensions)
          extensions.injectColumnar(_ =>
            new org.apache.spark.sql.execution.ColumnarRule {
              override def preColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
                override def apply(plan: SparkPlan): SparkPlan = { calls.incrementAndGet(); plan }
              }
            }
          )
        })
      )
    finally policy.engine.close()
  }

  @Test
  def ordinaryParquetPhysicalSourceCannotBorrowPreparedLinks(): Unit = {
    var ordinary: FileSourceScanExec = null
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val root = spark
            .sql("SELECT id, region FROM b.db.t")
            .queryExecution
            .executedPlan
            .asInstanceOf[DeltaRangerCollectGuardExec]
          val scan = root.collectFirst { case value: FileSourceScanExec => value }.get
          assertFalse(
            ordinary.relation.location.isInstanceOf[org.apache.spark.sql.delta.files.TahoeFileIndex]
          )
          assertEquals(scan.relation.location.rootPaths, ordinary.relation.location.rootPaths)
          val changed = scan.copy(relation = ordinary.relation)
          changed.copyTagsFrom(scan)
          changed.unsetTagValue(
            TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
              "gravitino.delta.ranger.physical-evidence"
            )
          )
          assertEquals(scan.output, changed.output)
          assertTrue(changed.logicalLink.nonEmpty)
          assertThrows(
            classOf[SecurityException],
            () => {
              DeltaRangerPhysicalProvenance.capture(changed, spark); ()
            }
          )
          val foreignChild = physicalCopy(root.child, { case _: FileSourceScanExec => changed })
          val rejected =
            root.withNewChildren(Seq(foreignChild)).asInstanceOf[DeltaRangerCollectGuardExec]
          assertThrows(classOf[SecurityException], () => rejected.executeCollect())
          rejected.cleanupResources()
          assertEquals(Set(1L, 2L, 3L), root.executeCollect().map(_.getLong(0)).toSet)
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark => {
          val frame = spark.read.parquet(NativeSelectFixture.locations("b")).select("id", "region")
          assertEquals(3, frame.collect().length)
          ordinary = frame.queryExecution.executedPlan.collectFirst {
            case scan: FileSourceScanExec => scan
          }.get
        }
      )
    finally policy.engine.close()
  }

  @Test
  def delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      import org.apache.spark.sql.execution.{LogicalRDD, RDDScanExec}
      import org.apache.spark.sql.catalyst.InternalRow
      val frames = NativeSelectFixture.withNativeOperation(spark) { index =>
        val snapshot = index.getSnapshot
        DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
        Seq(snapshot.stateDF, snapshot.withStats)
      }
      assertNull(DeltaRangerNativeOperation.current())
      policy.events.clear()
      frames.foreach { frame =>
        val logical = frame.queryExecution.analyzed.asInstanceOf[LogicalRDD]
        assertFalse(logical.rdd.getStorageLevel == org.apache.spark.storage.StorageLevel.NONE)
        val root = frame.queryExecution.executedPlan
        assertFalse(root.exists(_.isInstanceOf[DeltaRangerCollectGuardExec]))
        assertTrue(frame.collect().nonEmpty)
        val scan = root.collectFirst { case value: RDDScanExec => value }.get
        val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
          "gravitino.delta.ranger.physical-evidence"
        )
        val ticket = scan.getTagValue(tag).get.get
        ticket.verify(scan.clone())
        val wrongRdd = scan.copy(rdd = spark.sparkContext.emptyRDD[InternalRow])
        val wrongOutput = scan.copy(output = scan.output.map(_.withName("changed")))
        Seq(wrongRdd, wrongOutput).foreach { candidate =>
          candidate.copyTagsFrom(scan)
          assertThrows(
            classOf[SecurityException],
            () => {
              DeltaRangerPhysicalProvenance.capture(candidate, spark); ()
            }
          )
        }
        assertTrue(frame.collect().nonEmpty)
      }
      assertTrue(
        policy.events.isEmpty,
        "Native cached execution must not recurse through user authorization"
      )
    }

  @Test
  def installedGuardCloneAndLifecycleKeepAuthorityOffSerializedReferences(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      import java.io.{
        ByteArrayInputStream,
        ByteArrayOutputStream,
        ObjectInputStream,
        ObjectOutputStream
      }
      val frame = spark.sql("SELECT id FROM b.db.t")
      val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val copy = root.clone().asInstanceOf[DeltaRangerCollectGuardExec]
      assertEquals(Set(1L, 2L, 3L), copy.executeCollect().map(_.getLong(0)).toSet)
      assertEquals(Seq(root.child), root.children)
      assertEquals(1, root.productArity)
      assertFalse(root.toJSON.contains("driverContext"))
      assertFalse(root.toJSON.contains("physical-evidence"))
      assertFalse(root.toJSON.contains("decision-receipt"))
      assertThrows(classOf[IllegalStateException], () => root.canonicalized.executeCollect())
      val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
        "gravitino.delta.ranger.physical-evidence"
      )
      val reference = DeltaRangerPhysicalProvenance
        .nodes(root.child)
        .flatMap(_.getTagValue(tag))
        .head
      val bytes = new ByteArrayOutputStream()
      val writer = new ObjectOutputStream(bytes)
      try writer.writeObject(reference)
      finally writer.close()
      val reader = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val detached =
        try reader.readObject().asInstanceOf[DeltaRangerPhysicalProvenance.Reference]
        finally reader.close()
      assertThrows(classOf[SecurityException], () => { detached.get; () })
      copy.cleanupResources()
      policy.denied.add("b:id")
      val failure = assertThrows(classOf[SecurityException], () => copy.executeCollect())
      assertFalse(
        failure.getStackTrace.exists(e =>
          e.getClassName.contains("DeltaRangerCollectGuardExec") && e.getMethodName == "doExecute"
        )
      )
      copy.cleanupResources()
      root.cleanupResources()
    }

  @Test
  def installedGuardCleanupAfterActualDataFileFailure(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val frame = spark.sql("SELECT id FROM b.db.t")
      val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val scan = root.collectFirst { case value: FileSourceScanExec => value }.get
      val file = scan.relation.location.inputFiles.head
      val path = new org.apache.hadoop.fs.Path(file)
      assertTrue(path.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(path, false))
      val failure = assertThrows(classOf[Exception], () => frame.collect())
      def causes(value: Throwable): Vector[Throwable] =
        if (value == null) Vector.empty else value +: causes(value.getCause)
      assertTrue(causes(failure).exists(_.getMessage.contains("File")), failure.toString)
      assertFalse(causes(failure).exists(_.isInstanceOf[SecurityException]))
      root.cleanupResources()
    }

  @Test
  def populatedUserCacheSubstitutionRejectsLogicallyAndPhysically(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
      import org.apache.spark.sql.catalyst.plans.logical.Project
      import org.apache.spark.storage.StorageLevel
      val frame = spark.sql("SELECT id, region FROM b.db.t")
      assertEquals(Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep")), frame.collect().toSet)
      val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val original = DeltaRangerNativeProvenance.binding(frame.queryExecution.analyzed)
      val cached = InMemoryRelation(StorageLevel.MEMORY_AND_DISK, frame.queryExecution, None)
      try {
        assertTrue(cached.cacheBuilder.cachedColumnBuffers.count() > 0L)
        assertTrue(cached.cacheBuilder.isCachedColumnBuffersLoaded)
        assertEquals(3L, cached.cacheBuilder.rowCountStats.value)
        val nested = Project(cached.output, cached)
        Seq(cached, nested).foreach { candidate =>
          candidate.copyTagsFrom(frame.queryExecution.analyzed)
          val failure = assertThrows(
            classOf[SecurityException],
            () => DeltaRangerDecisionReceipt.check(candidate, spark, optimized = false)
          )
          assertTrue(failure.getMessage.contains("cache substitution"), failure.getMessage)
        }
        val scan = InMemoryTableScanExec(cached.output, Nil, cached)
        Seq(scan, ProjectExec(scan.output, scan)).foreach { candidate =>
          candidate.copyTagsFrom(root.child)
          val changed =
            root.withNewChildren(Seq(candidate)).asInstanceOf[DeltaRangerCollectGuardExec]
          val failure = assertThrows(classOf[SecurityException], () => changed.executeCollect())
          assertTrue(failure.getMessage.contains("physical cache substitution"), failure.getMessage)
          assertFalse(
            failure.getStackTrace.exists(e =>
              e.getClassName.contains(
                "DeltaRangerCollectGuardExec"
              ) && e.getMethodName == "doExecute"
            )
          )
          changed.cleanupResources()
        }
        assertEquals(original, DeltaRangerNativeProvenance.binding(frame.queryExecution.analyzed))
        assertEquals(Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep")), frame.collect().toSet)
      } finally {
        cached.cacheBuilder.clearCache(blocking = true)
        root.cleanupResources()
      }
    }

  @Test
  def fullLogicalRecipeDistinguishesFloatingPointBits(): Unit = {
    import org.apache.spark.sql.catalyst.expressions.Alias
    import org.apache.spark.sql.catalyst.plans.logical.{Project, LocalRelation}
    import org.apache.spark.sql.types.Decimal
    val alias = Alias(Literal(0.0d), "probe")()
    def recipe(value: Any): LogicalPlan =
      Project(Seq(Alias(Literal(value), alias.name)(exprId = alias.exprId)), LocalRelation())
    val pairs: Seq[(Any, Any)] = Seq(
      (0.0d, -0.0d),
      (0.0f, -0.0f),
      (
        java.lang.Double.longBitsToDouble(0x7ff8000000000001L),
        java.lang.Double.longBitsToDouble(0x7ff8000000000002L)
      ),
      (java.lang.Float.intBitsToFloat(0x7fc00001), java.lang.Float.intBitsToFloat(0x7fc00002))
    )
    pairs.foreach { case (before, after) =>
      assertNotEquals(
        DeltaRangerNativeProvenance.binding(recipe(before)),
        DeltaRangerNativeProvenance.binding(recipe(after)),
        "Raw floating point bits must bind"
      )
    }
  }

  @Test
  def fullLogicalRecipeSnapshotsMutableDecimals(): Unit = {
    import org.apache.spark.sql.catalyst.expressions.Alias
    import org.apache.spark.sql.catalyst.plans.logical.{Project, LocalRelation}
    import org.apache.spark.sql.types.Decimal
    val decimal = Decimal(1L)
    val plan = Project(Seq(Alias(Literal(decimal), "probe")()), LocalRelation())
    val frozen = DeltaRangerNativeProvenance.binding(plan)
    decimal.set(2L)
    assertNotEquals(frozen, DeltaRangerNativeProvenance.binding(plan))
  }

  @Test
  def actualPhysicalOnlyFloatingPointSubstitutionRejectsWithOriginalReceiptUnchanged(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      for (
        (sql, replacement) <- Seq(
          ("0.0D", Literal(-0.0d)),
          ("0.0F", Literal(-0.0f)),
          (
            "CAST('NaN' AS DOUBLE)",
            Literal(java.lang.Double.longBitsToDouble(0x7ff8000000000002L))
          ),
          ("CAST('NaN' AS FLOAT)", Literal(java.lang.Float.intBitsToFloat(0x7fc00002)))
        )
      ) {
        val frame = spark.sql(s"SELECT $sql AS probe, secret FROM a.db.t WHERE id=1")
        val original = DeltaRangerNativeProvenance.binding(frame.queryExecution.analyzed)
        val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
        val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
          "gravitino.delta.ranger.physical-evidence"
        )
        val ticket = DeltaRangerPhysicalProvenance
          .nodes(root.child)
          .flatMap(_.getTagValue(tag).map(_.get))
          .head
        ticket.verify(root.child)
        var changed = 0
        val altered = physicalCopy(
          root.child,
          { case project: ProjectExec =>
            val expressions = project.projectList.map {
              case alias: org.apache.spark.sql.catalyst.expressions.Alias
                  if alias.name == "probe" =>
                changed += 1
                org.apache.spark.sql.catalyst.expressions.Alias(replacement, alias.name)(
                  exprId = alias.exprId,
                  qualifier = alias.qualifier,
                  explicitMetadata = alias.explicitMetadata
                )
              case other => other
            }
            project.copy(projectList = expressions)
          }
        )
        assertEquals(1, changed)
        assertEquals(original, DeltaRangerNativeProvenance.binding(frame.queryExecution.analyzed))
        assertThrows(classOf[SecurityException], () => { ticket.verify(altered); () })
      }
    }

  @Test
  def actualFoldedPhysicalDecimalMutationLeavesReceiptUnchangedAndRejects(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val frame = spark.sql("SELECT (1.0BD + 1.0BD) AS probe, secret FROM a.db.t WHERE id=1")
      val analyzed = frame.queryExecution.analyzed
      val original = DeltaRangerNativeProvenance.binding(analyzed)
      val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
        "gravitino.delta.ranger.physical-evidence"
      )
      val ticket = DeltaRangerPhysicalProvenance
        .nodes(root.child)
        .flatMap(_.getTagValue(tag).map(_.get))
        .head
      ticket.verify(root.child)
      val literal = root.child
        .flatMap(_.expressions.flatMap(_.collect {
          case literal: Literal if literal.value.isInstanceOf[org.apache.spark.sql.types.Decimal] =>
            literal
        }))
        .head
      literal.value.asInstanceOf[org.apache.spark.sql.types.Decimal].set(3L)
      assertEquals(original, DeltaRangerNativeProvenance.binding(analyzed))
      assertThrows(classOf[SecurityException], () => { ticket.verify(root.child); () })
    }

  @Test
  def nativePhysicalTicketsAndLogicalRecoveryRejectAnotherActiveSession(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      var actual: SparkPlan = null
      NativeSelectFixture.withNativeOperation(spark) { index =>
        val snapshot = index.getSnapshot
        DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
        actual = snapshot.stateDF.queryExecution.executedPlan
      }
      assertFalse(actual.exists(_.isInstanceOf[DeltaRangerCollectGuardExec]))
      val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
        "gravitino.delta.ranger.physical-evidence"
      )
      assertTrue(DeltaRangerPhysicalProvenance.nodes(actual).exists(_.getTagValue(tag).nonEmpty))
      val other = spark.newSession()
      val rejected = Seq("inherited", "recovered", "post").map { route =>
        val copy = actual.clone()
        if (route == "recovered")
          DeltaRangerPhysicalProvenance.nodes(copy).foreach(_.unsetTagValue(tag))
        org.apache.spark.sql.SparkSession.setActiveSession(other)
        try {
          try {
            if (route == "post") DeltaRangerPhysicalProvenance.finish(copy, other)
            else DeltaRangerPhysicalProvenance.capture(copy, other)
            false
          } catch { case _: SecurityException => true }
        } finally org.apache.spark.sql.SparkSession.setActiveSession(spark)
      }
      assertEquals(
        Seq(true, true, true),
        rejected,
        "Inherited and recovered physical evidence must bind caller session"
      )
    }

  @Test
  def closedNativePreparationRejectsUnknownPrefixBeforeInvocation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      var original: LogicalPlan = null
      NativeSelectFixture.withNativeOperation(spark) { index =>
        val snapshot = index.getSnapshot
        DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
        original = snapshot.stateDF.queryExecution.analyzed
      }
      val candidate = DeltaRangerDecisionReceipt.nativePrefix(original.clone(), spark)
      val calls = new AtomicInteger()
      val oldRules = spark.experimental.extraOptimizations
      spark.experimental.extraOptimizations = Seq(new Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = { calls.incrementAndGet(); plan }
      })
      try {
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerReadRule.prepare(candidate, spark)
        )
        assertEquals(0, calls.get())
      } finally spark.experimental.extraOptimizations = oldRules
    }

  @Test
  def logicalRecipeSnapshotsNestedInternalTargetSchema(): Unit = {
    import org.apache.spark.sql.catalyst.expressions.{Alias, IsNotNull, JsonToStructs}
    import org.apache.spark.sql.catalyst.plans.logical.{Project, LocalRelation}
    import org.apache.spark.sql.types.{StructType, StructField, StringType}
    val nested = StructType(Seq(StructField("value", StringType)))
    val target = StructType(Seq(StructField("object", nested)))
    val parsed = JsonToStructs(target, Map.empty[String, String], Literal("{}"), Some("UTC"))
    val plan = Project(Seq(Alias(IsNotNull(parsed), "present")()), LocalRelation())
    val output = plan.output
    val frozen = DeltaRangerNativeProvenance.binding(plan)
    nested.fields(0) = nested.fields(0).copy(name = "changed")
    assertEquals(output, plan.output)
    assertNotEquals(frozen, DeltaRangerNativeProvenance.binding(plan))
  }

  @Test
  def installedCollectionRetainsOneVisibleGuardAndRevalidatesEveryEntry(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val dataset = spark.sql("SELECT secret FROM a.db.t WHERE id = 1")
      val root = dataset.queryExecution.executedPlan
      assertTrue(root.isInstanceOf[DeltaRangerCollectGuardExec], root.treeString)
      val expected = Vector(Row("a-one-masked"))
      assertTrue(root.exists(_.isInstanceOf[WholeStageCodegenExec]))
      assertTrue(root.collect { case scan: FileSourceScanExec => scan }.exists(_.supportsColumnar))
      policy.events.clear()
      assertEquals(expected, dataset.collect().toVector)
      assertTrue(policy.events.asScala.count(_ == "filter") >= 1)
      policy.events.clear()
      assertEquals(expected, dataset.collectAsList().asScala.toVector)
      assertTrue(policy.events.asScala.count(_ == "filter") >= 1)
      policy.events.clear()
      assertEquals(expected, dataset.collect().toVector)
      assertTrue(policy.events.asScala.count(_ == "filter") >= 1)
      assertSame(root, dataset.queryExecution.executedPlan)
      assertEquals(1, root.collect { case _: DeltaRangerCollectGuardExec => 1 }.size)
    }
  @Test
  def preparedEvidenceMustBindExecutableExpressionsBeforePhysicalCapture(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val dataset = spark.sql("SELECT secret FROM a.db.t WHERE id = 1")
      val original = dataset.queryExecution.optimizedPlan
      val prepared = DeltaRangerNativeScans.prepared(original)
      prepared.verifyExecutable(original, spark)
      val changed = original.transformUp { case filter: Filter =>
        filter.copy(condition = filter.condition.transform {
          case equality: EqualTo if equality.right == Literal(1L) =>
            equality.copy(right = Literal(2L))
        })
      }
      assertNotEquals(original, changed)
      assertEquals(original.output, changed.output)
      assertSame(prepared, DeltaRangerNativeScans.prepared(changed))
      val suffix = spark.sessionState.optimizer.batches
        .dropWhile(_.name != "Pre CBO Rules")
        .map(b => b.name + ": " + b.rules.map(_.getClass.getName).mkString(", "))
        .mkString("\n")
      val strategies = spark.sessionState.planner.strategies.map(_.getClass.getName).mkString("\n")
      assertThrows(
        classOf[SecurityException],
        () => { prepared.verifyExecutable(changed, spark); () },
        "Missing semantic preparation binding; runtime suffix:\n" + suffix +
          "\nRuntime strategies:\n" + strategies
      )
    }

  @Test
  def customPlannerIsRejectedBeforeInvocation(): Unit = {
    val calls = new AtomicInteger()
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.adaptive.enabled", "false")
          assertThrows(
            classOf[Exception],
            () => {
              spark.sql("SELECT secret FROM a.db.t").queryExecution.executedPlan
              ()
            }
          )
          assertEquals(0, calls.get(), "Unqualified planner must never execute")
        },
        Some(extensions => {
          NativeSelectFixture.extension(policy)(extensions)
          extensions.injectPlannerStrategy(_ =>
            new SparkStrategy {
              override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
                calls.incrementAndGet()
                Nil
              }
            }
          )
        })
      )
    finally policy.engine.close()
  }

  @Test
  def exactPhysicalLoweringRejectsChangedPredicateWithCopiedLinks(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val dataset = spark.sql("SELECT secret FROM a.db.t WHERE id = 1")
      val logical = dataset.queryExecution.optimizedPlan
      val root = dataset.queryExecution.executedPlan
      val prepared = DeltaRangerNativeScans.prepared(logical)
      val child = root.asInstanceOf[DeltaRangerCollectGuardExec].child
      val witness = DeltaRangerPhysicalProvenance.user(child, logical, prepared, spark)
      witness.verify(child.clone())
      val changed = child.transformUp { case filter: FilterExec =>
        val copy = filter.copy(condition = filter.condition.transform {
          case equality: EqualTo if equality.right == Literal(1L) =>
            equality.copy(right = Literal(2L))
        })
        copy.copyTagsFrom(filter)
        copy
      }
      assertNotEquals(root, changed)
      assertThrows(classOf[SecurityException], () => witness.verify(changed))
    }

  @Test
  def retainedAccessRevokeRejectsBeforeFirstAndRepeatedCollection(): Unit = {
    Seq(false, true).foreach { collected =>
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        val dataset = spark.sql("SELECT secret FROM a.db.t WHERE id = 1")
        val root = dataset.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
        if (collected) assertEquals(Vector(Row("a-one-masked")), dataset.collect().toVector)
        val scans = root.collect { case scan: FileSourceScanExec => scan }
        assertFalse(scans.isEmpty)
        // Denial at action entry precedes recursive metric reset and all child preparation.
        val priorRows = scans.map(_.metrics("numOutputRows").value)
        val jobs = spark.sparkContext.statusTracker.getJobIdsForGroup("physical-guard-stale").toSet
        spark.sparkContext.setJobGroup("physical-guard-stale", "retained collection denial")
        policy.denied.add("a:secret")
        policy.events.clear()
        val failure =
          assertThrows(classOf[SecurityException], () => { dataset.collectAsList(); () })
        assertTrue(failure.getMessage.contains("denied"), failure.getMessage)
        assertTrue(
          failure.getStackTrace.exists(_.getClassName.endsWith("DeltaRangerCollectGuardExec"))
        )
        assertEquals(priorRows, scans.map(_.metrics("numOutputRows").value))
        assertEquals(
          jobs,
          spark.sparkContext.statusTracker.getJobIdsForGroup("physical-guard-stale").toSet
        )
        spark.sparkContext.clearJobGroup()
        assertTrue(policy.events.asScala.forall(_.startsWith("access:")))
        assertSame(root, dataset.queryExecution.executedPlan)
      }
    }
  }

  @Test
  def finalMetadataMutationOfFoldedPhysicalLiteralRejectsAtCollectionEntry(): Unit =
    Seq(false, true).foreach { reused =>
      NativeSelectFixture.withBoundSpark { (spark, _) =>
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        val dataset =
          spark.sql("SELECT concat('phy', 'sical') AS probe, secret FROM a.db.t WHERE id = 1")
        val analyzed = dataset.queryExecution.analyzed
        val originalBinding = DeltaRangerNativeProvenance.binding(analyzed)
        val root = dataset.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
        val literal = root.child
          .flatMap(_.expressions.flatMap(_.collect {
            case literal: Literal
                if literal.value.isInstanceOf[UTF8String] &&
                  literal.value.toString == "physical" =>
              literal
          }))
          .head
        if (reused)
          assertEquals(Vector(Row("physical", "a-one-masked")), dataset.collect().toVector)
        val registration = NativeSelectFixture.bindingFixture(spark, "a")
        val finalRead = registration.reads.get() + 4
        registration.onRead = count =>
          if (count == finalRead) {
            literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]](0) =
              'z'.toByte
          }
        val failure = assertThrows(classOf[SecurityException], () => { dataset.collect(); () })
        assertEquals(finalRead, registration.reads.get())
        assertEquals(originalBinding, DeltaRangerNativeProvenance.binding(analyzed))
        assertTrue(
          failure.getMessage.contains("physical") && failure.getMessage.contains("changed"),
          failure.getMessage
        )
        assertFalse(
          failure.getStackTrace.exists(frame =>
            frame.getClassName.endsWith(
              "DeltaRangerCollectGuardExec"
            ) && frame.getMethodName == "doExecute"
          ),
          "Mutation must reject at collection entry, before Spark prepares the child"
        )
        assertSame(root, dataset.queryExecution.executedPlan)
        assertTrue(
          root
            .collect { case scan: FileSourceScanExec => scan }
            .forall(_.metrics("numOutputRows").value == 0L)
        )
      }
    }

  @Test
  def realThreeWaySelfJoinRetainsDuplicateObligationsAcrossExchangeReuse(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val dataset = spark.sql(
        "SELECT x.secret, y.secret, z.secret FROM a.db.t x JOIN a.db.t y ON x.id=y.id JOIN a.db.t z ON x.id=z.id"
      )
      val prepared = DeltaRangerNativeScans.prepared(dataset.queryExecution.optimizedPlan)
      assertEquals(3, prepared.owner.obligations.size)
      val root = dataset.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      assertFalse(
        root.collect { case reused: org.apache.spark.sql.execution.exchange.ReusedExchangeExec =>
          reused
        }.isEmpty,
        root.treeString
      )
      val rows = Set(Row("a-one-masked", "a-one-masked", "a-one-masked"), Row(null, null, null))
      policy.events.clear()
      assertEquals(rows, dataset.collect().toSet)
      assertTrue(policy.events.asScala.count(_ == "filter") >= 3)
      assertEquals(rows, dataset.collectAsList().asScala.toSet)
      assertSame(root, dataset.queryExecution.executedPlan)
      val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
        "gravitino.delta.ranger.physical-evidence"
      )
      val ticket =
        DeltaRangerPhysicalProvenance.nodes(root.child).flatMap(_.getTagValue(tag).map(_.get)).head
      ticket.verify(root.child)
      val changedOutput = physicalCopy(
        root.child,
        { case reused: ReusedExchangeExec =>
          reused.copy(output = reused.output.map(_.withName("foreign")))
        }
      )
      assertThrows(classOf[SecurityException], () => ticket.verify(changedOutput))
      var changedSources = 0
      val changedSource = physicalCopy(
        root.child,
        { case reused: ReusedExchangeExec =>
          val child = physicalCopy(
            reused.child,
            { case scan: FileSourceScanExec =>
              val index = scan.relation.location
                .asInstanceOf[org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex]
              val copied = index.copy()
              assertEquals(index, copied)
              assertNotSame(index, copied)
              changedSources += 1
              scan.copy(relation = scan.relation.copy(location = copied)(spark))
            }
          )
          reused.copy(child = child.asInstanceOf[Exchange])
        }
      )
      assertTrue(changedSources > 0)
      assertEquals(root.child.output, changedSource.output)
      assertThrows(classOf[SecurityException], () => ticket.verify(changedSource))
    }

  @Test
  def simpleProjectionRecoversCompleteRecipeAndRejectsCopiedSamePathIndex(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val dataset = spark.sql("SELECT id, region FROM b.db.t")
      val root = dataset.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val rows = Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep"))
      assertEquals(rows, dataset.collect().toSet)
      assertEquals(rows, dataset.collectAsList().asScala.toSet)
      assertSame(root, dataset.queryExecution.executedPlan)
      val scan = root.collectFirst { case scan: FileSourceScanExec => scan }.get
      assertEquals(scan.logicalLink.get.output.map(_.exprId), scan.output.map(_.exprId))
      assertNotEquals(
        scan.logicalLink.get.output,
        scan.output,
        "Stock pruning removes physical qualifiers while preserving complete logical evidence"
      )
      val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
        "gravitino.delta.ranger.physical-evidence"
      )
      val unchanged = scan.clone()
      unchanged.unsetTagValue(tag)
      assertSame(unchanged, DeltaRangerPhysicalProvenance.capture(unchanged, spark))
      val index =
        scan.relation.location.asInstanceOf[org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex]
      val copied = index.copy()
      assertEquals(index, copied)
      assertNotSame(index, copied)
      val changed = scan.copy(relation = scan.relation.copy(location = copied)(spark))
      changed.copyTagsFrom(scan)
      changed.unsetTagValue(tag)
      assertThrows(
        classOf[SecurityException],
        () => { DeltaRangerPhysicalProvenance.capture(changed, spark); () }
      )
      val changedOutput = scan.copy(output = scan.output.map(_.withName("foreign")))
      changedOutput.copyTagsFrom(scan)
      changedOutput.unsetTagValue(tag)
      assertThrows(
        classOf[SecurityException],
        () => { DeltaRangerPhysicalProvenance.capture(changedOutput, spark); () }
      )
    }

  @Test
  def installedRowScanReturnsExactRows(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val dataset = spark.sql("SELECT secret FROM a.db.t WHERE id = 1")
          val root = dataset.queryExecution.executedPlan
          assertTrue(root.isInstanceOf[DeltaRangerCollectGuardExec])
          val scans = root.collect { case scan: FileSourceScanExec => scan }
          assertFalse(scans.isEmpty)
          assertTrue(scans.forall(!_.supportsColumnar))
          assertEquals(Vector(Row("a-one-masked")), dataset.collect().toVector)
          assertEquals(Vector(Row("a-one-masked")), dataset.collectAsList().asScala.toVector)
          assertSame(root, dataset.queryExecution.executedPlan)
        },
        Some(NativeSelectFixture.extension(policy)),
        startupOptions = Map(
          "spark.sql.adaptive.enabled" -> "false",
          "spark.sql.parquet.enableVectorizedReader" -> "false"
        )
      )
    finally policy.engine.close()
  }

  @Test
  def realNativeCaseWhenPairsBindBothFieldsAfterOwnerCloses(): Unit = {
    val native = new ConcurrentLinkedQueue[LogicalPlan]()
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.adaptive.enabled", "false")
          assertEquals(
            Vector(Row("a-one-masked")),
            spark.sql("SELECT secret FROM a.db.t WHERE id = 1").collect().toVector
          )
          assertFalse(native.isEmpty)
          val original = native.asScala
            .find(_.expressions.exists(_.exists {
              case branch: CaseWhen =>
                branch.branches.exists { case (_: IsNotNull, _: ScalaUDF) => true; case _ => false }
              case _ => false
            }))
            .get
          val pair = original.expressions
            .flatMap(_.collect { case branch: CaseWhen => branch.branches })
            .flatten
            .find { case (_: IsNotNull, _: ScalaUDF) => true; case _ => false }
            .get
          assertEquals(classOf[Tuple2[_, _]], pair.getClass)
          assertEquals(classOf[IsNotNull], pair._1.getClass)
          assertEquals(classOf[ScalaUDF], pair._2.getClass)
          assertNotNull(DeltaRangerNativeProvenance.requireExecutable(original, spark))
          val changed = original.transformAllExpressions { case branch: CaseWhen =>
            branch.copy(branches = branch.branches.map { case (condition, value) =>
              (Not(condition), value)
            })
          }
          assertTrue(changed.resolved)
          assertEquals(original.output, changed.output)
          assertThrows(
            classOf[SecurityException],
            () => { DeltaRangerNativeProvenance.requireExecutable(changed, spark); () }
          )
        },
        Some(extensions => {
          NativeSelectFixture.extension(policy)(extensions)
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                if (
                  DeltaRangerNativeOperation.current() != null && plan.resolved &&
                  plan.expressions.exists(_.exists(_.isInstanceOf[CaseWhen]))
                ) {
                  DeltaRangerNativeProvenance.classify(plan) match {
                    case _: DeltaRangerNativeProvenance.SealedNative => native.add(plan.clone())
                    case _                                           =>
                  }
                }
                plan
              }
            }
          )
        })
      )
    finally policy.engine.close()
  }

  @Test
  def actualCompletedNativeStagesKeepExchangeRootsAndRejectMissingMixedOrPartialEvidence(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      val observed = new ConcurrentLinkedQueue[SparkPlan]()
      val completed = new CountDownLatch(1)
      val listener = new QueryExecutionListener {
        override def onSuccess(name: String, query: QueryExecution, duration: Long): Unit = {
          // onSuccess only: executedPlan is already evaluated, and Adaptive.executedPlan
          // is the public current-plan getter, not the execution-triggering finalPhysicalPlan.
          query.executedPlan match {
            case adaptive: AdaptiveSparkPlanExec if adaptive.isFinalPlan =>
              val actual = adaptive.executedPlan
              if (
                DeltaRangerPhysicalProvenance.nodes(actual).exists(_.isInstanceOf[QueryStageExec])
              ) {
                observed.add(actual)
                completed.countDown()
              }
            case _ =>
          }
        }
        override def onFailure(name: String, query: QueryExecution, error: Exception): Unit = ()
      }
      spark.listenerManager.register(listener)
      try {
        Seq("a", "b").foreach { catalog =>
          val optimized =
            spark.sql(s"SELECT secret FROM $catalog.db.t").queryExecution.optimizedPlan
          assertEquals(
            Set(catalog),
            DeltaRangerNativeScans.prepared(optimized).owner.obligations.map(_.scan.catalog).toSet
          )
        }
        assertTrue(
          completed.await(10, TimeUnit.SECONDS),
          "No successful native hidden-stage capture"
        )
        val plans = observed.asScala.toVector
        assertFalse(plans.isEmpty)
        val stages = plans.flatMap(DeltaRangerPhysicalProvenance.nodes).collect {
          case stage: ShuffleQueryStageExec => stage
        }
        assertFalse(stages.isEmpty)
        assertTrue(stages.forall(_.plan.getClass == classOf[ShuffleExchangeExec]))
        assertTrue(plans.forall(!_.exists(_.isInstanceOf[DeltaRangerCollectGuardExec])))
        val tag = TreeNodeTag[DeltaRangerPhysicalProvenance.Reference](
          "gravitino.delta.ranger.physical-evidence"
        )
        val stage = stages.head
        assertSame(stage, DeltaRangerPhysicalProvenance.capture(stage, spark))
        assertSame(stage, DeltaRangerPhysicalProvenance.finish(stage))
        val missing = stage.copy(plan = stage.plan.clone())
        DeltaRangerPhysicalProvenance.nodes(missing).foreach(_.unsetTagValue(tag))
        assertThrows(
          classOf[SecurityException],
          () => { DeltaRangerPhysicalProvenance.capture(missing, spark); () }
        )
        val references = plans
          .flatMap(DeltaRangerPhysicalProvenance.nodes)
          .flatMap(_.getTagValue(tag))
          .groupBy(_.get)
          .values
          .map(_.head)
          .toVector
        assertTrue(
          references.size >= 2,
          "Need two actual native authorities for mixed-stage control"
        )
        val own = stage.plan.getTagValue(tag).get.get
        val foreign = references.find(reference => !(reference.get eq own)).get
        val mixed = stage.copy(plan = stage.plan.clone())
        mixed.setTagValue(tag, foreign)
        assertThrows(
          classOf[SecurityException],
          () => { DeltaRangerPhysicalProvenance.capture(mixed, spark); () }
        )
        val complete = AQEShuffleReadExec(
          stage,
          Seq(CoalescedPartitionSpec(0, stage.plan.outputPartitioning.numPartitions))
        )
        assertSame(complete, DeltaRangerPhysicalProvenance.capture(complete, spark))
        val partial = complete.copy(partitionSpecs =
          Seq(CoalescedPartitionSpec(1, stage.plan.outputPartitioning.numPartitions))
        )
        assertThrows(
          classOf[SecurityException],
          () => { DeltaRangerPhysicalProvenance.capture(partial, spark); () }
        )
        assertEquals("true", spark.conf.get("spark.sql.adaptive.enabled"))
      } finally spark.listenerManager.unregister(listener)
    }

  @Test
  def adaptiveExtensionRejectsBeforeInvocation(): Unit = {
    val calls = new AtomicInteger
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val failure = assertThrows(
            classOf[SecurityException],
            () => { spark.sql("SELECT secret FROM a.db.t"); () }
          )
          assertNotNull(failure.getMessage)
          assertEquals(0, calls.get())
          assertEquals("true", spark.conf.get("spark.sql.adaptive.enabled"))
        },
        Some(extensions => {
          NativeSelectFixture.extension(policy)(extensions)
          extensions.injectQueryStagePrepRule(_ =>
            new Rule[SparkPlan] {
              override def apply(plan: SparkPlan): SparkPlan = { calls.incrementAndGet(); plan }
            }
          )
        }),
        startupOptions = Map("spark.sql.adaptive.enabled" -> "true")
      )
    finally policy.engine.close()
  }

  @Test
  def enabledAqeAdmitsFreshReadWithoutMutatingConfiguration(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      val frame = spark.sql("SELECT secret FROM a.db.t WHERE id=1")
      assertEquals(Vector(Row("a-one-masked")), frame.collect().toVector)
      assertTrue(frame.queryExecution.executedPlan.isInstanceOf[DeltaRangerCollectGuardExec])
      assertEquals("true", spark.conf.get("spark.sql.adaptive.enabled"))
    }

  // Explicit stock constructors preserve deliberate equal-value/different-backing mutations;
  // TreeNode.transform intentionally elides equal case-class children.
  private def physicalCopy(
      node: SparkPlan,
      change: PartialFunction[SparkPlan, SparkPlan]
  ): SparkPlan = {
    if (change.isDefinedAt(node)) return change(node)
    def next(child: SparkPlan): SparkPlan = physicalCopy(child, change)
    node match {
      case value: WholeStageCodegenExec =>
        value.copy(child = next(value.child))(value.codegenStageId)
      case value: ProjectExec       => value.copy(child = next(value.child))
      case value: FilterExec        => value.copy(child = next(value.child))
      case value: InputAdapter      => value.copy(child = next(value.child))
      case value: ColumnarToRowExec => value.copy(child = next(value.child))
      case value: BroadcastHashJoinExec =>
        value.copy(left = next(value.left), right = next(value.right))
      case value: BroadcastExchangeExec => value.copy(child = next(value.child))
      case value: ReusedExchangeExec => value.copy(child = next(value.child).asInstanceOf[Exchange])
      case value: FileSourceScanExec => value.clone()
      case other =>
        throw new AssertionError("Uncharacterized test copy node: " + other.getClass.getName)
    }
  }

}
