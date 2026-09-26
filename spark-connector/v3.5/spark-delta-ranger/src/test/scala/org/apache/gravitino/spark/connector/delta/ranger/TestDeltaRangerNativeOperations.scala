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

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerNativeOperations {
  @Test
  def nativeCacheRebindingBindsExactUnknownPartitioningValues(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      import org.apache.spark.sql.catalyst.plans.physical.{
        UnknownPartitioning,
        RoundRobinPartitioning
      }
      NativeSelectFixture.withNativeOperation(spark) { index =>
        val operation = DeltaRangerNativeOperation.current()
        val snapshot = index.getSnapshot
        operation.capture(index, snapshot)
        Seq(false -> snapshot.stateDF, true -> snapshot.withStats).foreach { case (stats, frame) =>
          val original = frame.queryExecution.analyzed.asInstanceOf[LogicalRDD]
          assertEquals(classOf[UnknownPartitioning], original.outputPartitioning.getClass)
          val count = original.outputPartitioning.numPartitions
          val changedCount =
            original.copy(outputPartitioning = UnknownPartitioning(count + 1))(spark, None, None)
          val changedType =
            original.copy(outputPartitioning = RoundRobinPartitioning(count))(spark, None, None)
          assertFalse(operation.contains(changedCount, stats))
          assertFalse(operation.contains(changedType, stats))
          val wrongRdd =
            original.copy(rdd = spark.sparkContext.emptyRDD[InternalRow])(spark, None, None)
          assertFalse(operation.contains(wrongRdd, stats))
          val fresh =
            original.copy(outputPartitioning = UnknownPartitioning(count))(spark, None, None)
          assertNotSame(original.outputPartitioning, fresh.outputPartitioning)
          assertSame(original.rdd, fresh.rdd)
          assertEquals(original.output, fresh.output)
          assertTrue(
            operation.contains(fresh, stats),
            "Fresh exact stock partitioning value must retain captured cache membership"
          )
        }
      }
    }

  @Test
  def uncachedRetainedStateKeepsExactPreparationAndDelayedNativeExecution(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val analyzed = spark.sql("SELECT id, region FROM b.db.t").queryExecution.analyzed
      val owner = DeltaRangerNativeScans.consume(analyzed, spark, optimized = false)
      val index = owner.obligations.head.binding.index()
      val snapshot = index.deltaLog.unsafeVolatileSnapshot
      val foreignOwner = DeltaRangerNativeScans.consume(
        spark.sql("SELECT id, region FROM a.db.t").queryExecution.analyzed,
        spark,
        optimized = false
      )
      val foreignSnapshot =
        foreignOwner.obligations.head.binding.index().deltaLog.unsafeVolatileSnapshot
      var preparation: DeltaRangerNativeOperation = null
      var oldState: LogicalRDD = null
      var oldStats: LogicalRDD = null
      owner.run { operation =>
        preparation = operation
        operation.capture(index, snapshot)
        oldState = snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD]
        oldStats = snapshot.withStats.queryExecution.analyzed.asInstanceOf[LogicalRDD]
        assertTrue(operation.contains(index, snapshot))
      }
      assertTrue(preparation.succeeded())
      snapshot.uncache()
      owner.invoke(index, Nil, Nil) { operation =>
        assertThrows(
          classOf[SecurityException],
          () => operation.captureRetained(index, snapshot, operation)
        )
        assertThrows(
          classOf[SecurityException],
          () => operation.captureRetained(index.copy(), snapshot, preparation)
        )
        assertThrows(
          classOf[SecurityException],
          () => operation.captureRetained(index, foreignSnapshot, preparation)
        )
      }
      foreignOwner.invoke(foreignOwner.obligations.head.binding.index(), Nil, Nil) { operation =>
        assertThrows(
          classOf[SecurityException],
          () => operation.captureRetained(index, snapshot, preparation)
        )
      }
      val frames = owner.invoke(index, Nil, Nil) { operation =>
        assertFalse(
          operation.contains(index, snapshot),
          "Inherited evidence cannot complete fresh capture"
        )
        operation.captureRetained(index, snapshot, preparation)
        assertTrue(operation.contains(index, snapshot))
        assertTrue(operation.contains(oldState.clone().asInstanceOf[LogicalRDD], false))
        assertFalse(operation.contains(oldStats, true), "Old stats cache must not transfer")
        val decoy = oldState.copy(rdd = spark.sparkContext.emptyRDD[InternalRow])(spark, None, None)
        assertFalse(operation.contains(decoy, false))
        val changedOutput =
          oldState.copy(output = oldState.output.map(_.withName("changed")))(spark, None, None)
        assertFalse(operation.contains(changedOutput, false))
        assertThrows(
          classOf[SecurityException],
          () => operation.requireEvidence(spark.newSession())
        )
        val state = snapshot.stateDF
        val stats = snapshot.withStats
        assertFalse(state.queryExecution.analyzed.isInstanceOf[LogicalRDD])
        assertFalse(stats.queryExecution.analyzed.isInstanceOf[LogicalRDD])
        assertThrows(
          classOf[SecurityException],
          () => foreignOwner.sealReconstruction(operation, state.queryExecution.analyzed, false)
        )
        Seq(state, stats)
      }
      assertNull(DeltaRangerNativeOperation.current())
      var generatedProofs = 0
      frames.foreach { frame =>
        if (assertNestedFieldCorrespondence(frame.queryExecution.analyzed, spark))
          generatedProofs += 1
        assertNotNull(
          DeltaRangerNativeProvenance.requireExecutable(frame.queryExecution.analyzed, spark)
        )
        assertFalse(
          frame.queryExecution.executedPlan.exists(_.isInstanceOf[DeltaRangerCollectGuardExec])
        )
        assertTrue(
          frame.collect().nonEmpty,
          "Owned uncached native route must execute after owner close"
        )
        val copied = frame.queryExecution.analyzed.clone()
        assertNotNull(DeltaRangerNativeProvenance.requireExecutable(copied, spark))
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerNativeProvenance.requireExecutable(copied, spark.newSession())
        )
      }
      assertTrue(generatedProofs > 0, "Must exercise actual generated native nested-field aliases")
      var failed: DeltaRangerNativeOperation = null
      val abort = assertThrows(
        classOf[IllegalStateException],
        () =>
          owner.invoke(index, Nil, Nil) { operation =>
            failed = operation
            operation.captureRetained(index, snapshot, preparation)
            assertTrue(operation.contains(oldState, false))
            throw new IllegalStateException("abort inherited native invocation")
          }
      )
      assertEquals("abort inherited native invocation", abort.getMessage)
      assertNull(DeltaRangerNativeOperation.current())
      assertThrows(classOf[SecurityException], () => failed.contains(oldState, false))
      owner.invoke(index, Nil, Nil) { operation =>
        assertThrows(
          classOf[SecurityException],
          () => operation.captureRetained(index, snapshot, failed)
        )
        operation.captureRetained(index, snapshot, preparation)
        assertTrue(operation.contains(oldState, false))
      }
    }

  private def assertNestedFieldCorrespondence(
      original: LogicalPlan,
      spark: SparkSession
  ): Boolean = {
    import org.apache.spark.sql.catalyst.expressions.{
      Alias,
      ExprId,
      GetStructField,
      NamedExpression
    }
    import org.apache.spark.sql.catalyst.plans.logical.{Project, MapPartitions}
    val expected = DeltaRangerDecisionReceipt.nativePrefix(original.clone(), spark)
    val candidate = DeltaRangerDecisionReceipt.nativePrefix(original.clone(), spark)
    assertTrue(DeltaRangerNativeProvenance.prefixCorresponds(original, expected, candidate))
    val originalIds = original
      .flatMap(node =>
        node.output.map(_.exprId) ++
          node.expressions.flatMap(_.collect { case named: NamedExpression => named.exprId })
      )
      .toSet
    def producer(plan: LogicalPlan): Option[Project] = plan.collectFirst {
      case project: Project if project.projectList.exists {
            case alias: Alias => alias.name.startsWith("_extract_") && !originalIds(alias.exprId)
            case _            => false
          } =>
        project
    }
    if (producer(expected).isEmpty) return false
    val left = producer(expected).get
    val right = producer(candidate).get
    val aliases = right.projectList.zipWithIndex.collect {
      case (alias: Alias, index) if !originalIds(alias.exprId) => (alias, index)
    }
    assertTrue(aliases.size >= 2)
    val (first, firstIndex) = aliases.head
    val (second, secondIndex) = aliases(1)
    def withId(alias: Alias, id: ExprId): Alias = Alias(alias.child, alias.name)(
      id,
      alias.qualifier,
      alias.explicitMetadata,
      alias.nonInheritableMetadataKeys
    )
    def changed(alias: Alias): Project =
      right.copy(projectList = right.projectList.updated(firstIndex, alias))
    def rejects(plan: LogicalPlan): Unit =
      assertFalse(DeltaRangerNativeProvenance.prefixCorresponds(original, left, plan))
    assertTrue(DeltaRangerNativeProvenance.prefixCorresponds(original, left, right))
    rejects(right.copy(projectList = right.projectList.patch(firstIndex, Nil, 1)))
    rejects(right.copy(projectList = right.projectList :+ withId(first, NamedExpression.newExprId)))
    rejects(changed(withId(first, second.exprId)))
    rejects(changed(withId(first, ExprId(first.exprId.id, java.util.UUID.randomUUID()))))
    rejects(changed(withId(first, originalIds.head)))
    val field = first.child.asInstanceOf[GetStructField]
    rejects(
      changed(Alias(org.apache.spark.sql.catalyst.expressions.Literal(1), first.name)(first.exprId))
    )
    rejects(changed(Alias(field, "_extract_changed")(first.exprId)))
    rejects(changed(Alias(field, first.name)(first.exprId, Seq("changed"))))
    rejects(
      changed(
        Alias(field, first.name)(
          first.exprId,
          first.qualifier,
          Some(new org.apache.spark.sql.types.MetadataBuilder().putString("changed", "yes").build())
        )
      )
    )
    rejects(
      changed(
        Alias(field.copy(ordinal = if (field.ordinal == 0) 1 else 0), first.name)(first.exprId)
      )
    )
    val consumerLeft = Project(left.output, left)
    val consumerRight = Project(right.output, right)
    assertTrue(DeltaRangerNativeProvenance.prefixCorresponds(original, consumerLeft, consumerRight))
    val remapped = consumerRight.copy(projectList =
      consumerRight.projectList
        .updated(firstIndex, right.output(firstIndex).withExprId(second.exprId))
    )
    assertFalse(DeltaRangerNativeProvenance.prefixCorresponds(original, consumerLeft, remapped))
    val swapped = consumerRight.copy(projectList =
      consumerRight.projectList
        .updated(firstIndex, right.output(firstIndex).withExprId(second.exprId))
        .updated(secondIndex, right.output(secondIndex).withExprId(first.exprId))
    )
    assertFalse(DeltaRangerNativeProvenance.prefixCorresponds(original, consumerLeft, swapped))
    val functionChanged = expected.transformUp { case node: MapPartitions =>
      node.copy(func = (rows: Iterator[Any]) => rows)
    }
    assertNotEquals(expected, functionChanged)
    assertThrows(
      classOf[SecurityException],
      () => DeltaRangerReadRule.prepare(functionChanged, spark)
    )
    true
  }

  @Test
  def retainedStateBindingMustSnapshotSharedMutableSchema(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val owner = DeltaRangerNativeScans.consume(
        spark.sql("SELECT id, region FROM b.db.t").queryExecution.analyzed,
        spark,
        optimized = false
      )
      val index = owner.obligations.head.binding.index()
      val snapshot = index.deltaLog.unsafeVolatileSnapshot
      var preparation: DeltaRangerNativeOperation = null
      var oldState: LogicalRDD = null
      owner.run { operation =>
        preparation = operation
        operation.capture(index, snapshot)
        oldState = snapshot.stateDF.queryExecution.analyzed.asInstanceOf[LogicalRDD]
      }
      snapshot.uncache()
      owner.invoke(index, Nil, Nil) { operation =>
        operation.captureRetained(index, snapshot, preparation)
        val schema = oldState.output
          .find(_.name == "add")
          .get
          .dataType
          .asInstanceOf[org.apache.spark.sql.types.StructType]
        val field = schema.fields(0)
        try {
          schema.fields(0) = field.copy(name = "changed")
          assertThrows(classOf[SecurityException], () => operation.contains(oldState, false))
        } finally schema.fields(0) = field
      }
    }

  @Test
  def pendingGetterCandidatesCannotExecuteAndOnlyReturnedLineageIsSealed(): Unit = {
    val candidates = new ConcurrentLinkedQueue[(LogicalRDD, LogicalRDD, Boolean)]()
    val checkingPhysical = new java.util.concurrent.atomic.AtomicBoolean()
    val physicalDenials = new java.util.concurrent.atomic.AtomicInteger()
    val f = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals(
            Set(Row("a-one-masked"), Row(null)),
            spark.sql("SELECT secret FROM a.db.t").collect().toSet
          )
          assertFalse(candidates.isEmpty)
          assertTrue(
            physicalDenials.get() > 0,
            "Actual pending getter must reach physical planning"
          )
          candidates.asScala.foreach { case (real, decoy, executableWhilePending) =>
            assertFalse(executableWhilePending, "Pre-CBO must reject pending cache candidates")
            assertNotNull(DeltaRangerNativeProvenance.requireExecutable(real.clone(), spark))
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerNativeProvenance.requireExecutable(decoy, spark)
            )
          }
        },
        Some(extensions => {
          extensions.injectResolutionRule(spark =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val operation = DeltaRangerNativeOperation.current()
                plan match {
                  case rdd: LogicalRDD
                      if !checkingPhysical.get() && operation != null &&
                        operation.capturingSnapshot() != null &&
                        !operation.contains(rdd, false) && !operation.contains(rdd, true) =>
                    val decoy =
                      rdd.copy(rdd = spark.sparkContext.emptyRDD[InternalRow])(spark, None, None)
                    assertEquals(
                      DeltaRangerNativeProvenance.AnalysisOnlyNative,
                      DeltaRangerNativeProvenance.classify(decoy)
                    )
                    val permitted =
                      try { DeltaRangerReadRule.prepare(rdd, spark); true }
                      catch {
                        case failure: SecurityException =>
                          assertTrue(
                            failure.getMessage
                              .contains("Pending native Delta analysis cannot execute")
                          )
                          false
                      }
                    checkingPhysical.set(true)
                    try {
                      val failure = assertThrows(
                        classOf[SecurityException],
                        () => {
                          new org.apache.spark.sql.execution.QueryExecution(spark, rdd).executedPlan
                          ()
                        }
                      )
                      assertTrue(
                        failure.getMessage.contains("Pending native Delta analysis cannot execute"),
                        failure.getMessage
                      )
                      physicalDenials.incrementAndGet()
                    } finally checkingPhysical.set(false)
                    candidates.add((rdd, decoy, permitted))
                  case _ =>
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        })
      )
    finally f.engine.close()
  }

  @Test
  def checkpointAndJsonTailNamedLoadCapturesActualNativeInputs(): Unit = {
    val inspected = new java.util.concurrent.atomic.AtomicBoolean()
    val transitions =
      new ConcurrentLinkedQueue[(Boolean, Boolean, Boolean, Boolean, Boolean, Boolean)]()
    val inputs = new ConcurrentLinkedQueue[LogicalPlan]()
    val kinds = new ConcurrentLinkedQueue[String]()
    val unions = new ConcurrentLinkedQueue[LogicalPlan]()
    val reconstructions = new ConcurrentLinkedQueue[LogicalPlan]()
    val f = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals(
            Set(Row("a-one-masked"), Row(null)),
            spark.sql("SELECT secret FROM a.db.t").collect().toSet
          )
          assertTrue(
            kinds.contains("checkpoint"),
            "Must observe an actual checkpoint input during owned named load"
          )
          assertTrue(inspected.get(), "Must verify the actual canonical projection transition")
          assertFalse(transitions.isEmpty)
          transitions.asScala.foreach {
            case (
                  positive,
                  duplicateResolved,
                  duplicateShape,
                  duplicateAdmitted,
                  replacementAdmitted,
                  orphanAdmitted
                ) =>
              assertFalse(
                orphanAdmitted,
                "A child cannot borrow the parent projection transition by itself"
              )
              assertTrue(positive, "Actual projection pushdown must retain native evidence")
              assertTrue(duplicateResolved)
              assertTrue(
                duplicateShape,
                "Duplicate negative must otherwise have a genuine native shape"
              )
              assertFalse(
                duplicateAdmitted,
                "An extra canonical application on one exact input/path role must reject"
              )
              assertFalse(
                replacementAdmitted,
                "A copied tag cannot authorize a replacement canonical function"
              )
          }
          assertTrue(kinds.contains("json"), "Must observe the actual JSON tail after checkpoint")
          assertFalse(unions.isEmpty, "Must exercise actual native log union reconstruction")
          assertNull(DeltaRangerNativeOperation.current())
          (inputs.asScala ++ unions.asScala ++ reconstructions.asScala).foreach(plan =>
            assertNotNull(DeltaRangerNativeProvenance.requireExecutable(plan.clone(), spark))
          )
          val delayed = (inputs.asScala ++ unions.asScala ++ reconstructions.asScala)
            .groupBy(_.getClass)
            .values
            .map(_.head)
            .toSeq
          assertTrue(delayed.size >= 3, "Must physically exercise input, union and reconstruction")
          delayed.foreach { plan =>
            val physical =
              new org.apache.spark.sql.execution.QueryExecution(spark, plan.clone()).executedPlan
            assertFalse(physical.exists(_.isInstanceOf[DeltaRangerCollectGuardExec]))
            assertTrue(physical.executeCollect().nonEmpty)
          }
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val operation = DeltaRangerNativeOperation.current()
                if (
                  operation != null && plan.resolved &&
                  plan.exists(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Union]) &&
                  plan.exists(
                    _.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.MapPartitions]
                  ) &&
                  inspected.compareAndSet(false, true)
                ) {
                  DeltaRangerPlanAdmission.internalMetadata(plan)
                  val session = SparkSession.active
                  val prefix = new org.apache.spark.sql.execution.SparkOptimizer(
                    session.sessionState.catalogManager,
                    session.sessionState.catalog,
                    session.experimental
                  ) {
                    override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
                      Seq(new org.apache.spark.sql.delta.optimizer.RangePartitionIdRewrite(session))
                    override def defaultBatches: Seq[Batch] =
                      super.defaultBatches.takeWhile(_.name != "Pre CBO Rules")
                  }
                  val originalUnion = plan.collectFirst {
                    case value: org.apache.spark.sql.catalyst.plans.logical.Union => value
                  }.get
                  assertTrue(DeltaRangerPlanAdmission.internalMetadata(originalUnion))
                  val optimized = prefix.execute(plan.clone())
                  assertTrue(DeltaRangerPlanAdmission.nativeShape(optimized))
                  val optimizedUnion = optimized.collectFirst {
                    case value: org.apache.spark.sql.catalyst.plans.logical.Union => value
                  }.get
                  optimizedUnion.copyTagsFrom(originalUnion)
                  val orphanAdmitted = DeltaRangerPlanAdmission.internalMetadata(optimizedUnion)
                  val positive = DeltaRangerPlanAdmission.internalMetadata(optimized)
                  reconstructions.add(optimized)
                  val project = optimized.collectFirst {
                    case value: org.apache.spark.sql.catalyst.plans.logical.Project
                        if value.expressions.exists(
                          _.exists(
                            _.isInstanceOf[org.apache.spark.sql.catalyst.expressions.ScalaUDF]
                          )
                        ) =>
                      value
                  }.get
                  val udf = project.expressions
                    .flatMap(_.collect {
                      case value: org.apache.spark.sql.catalyst.expressions.ScalaUDF => value
                    })
                    .head
                  val duplicate = optimized.transformDown {
                    case value: org.apache.spark.sql.catalyst.plans.logical.Project
                        if value eq project =>
                      value.copy(projectList = value.projectList.map(_.transformUp {
                        case expression: org.apache.spark.sql.catalyst.expressions.ScalaUDF
                            if expression eq udf =>
                          org.apache.spark.sql.catalyst.expressions
                            .Coalesce(Seq(expression, expression))
                      }.asInstanceOf[org.apache.spark.sql.catalyst.expressions.NamedExpression]))
                  }
                  val changed = optimized.transformAllExpressions {
                    case value: org.apache.spark.sql.catalyst.expressions.ScalaUDF
                        if value eq udf =>
                      value.copy(function = ((path: String) => path))
                  }
                  transitions.add(
                    (
                      positive,
                      duplicate.resolved,
                      DeltaRangerPlanAdmission.nativeShape(duplicate),
                      DeltaRangerPlanAdmission.internalMetadata(duplicate),
                      DeltaRangerPlanAdmission.internalMetadata(changed),
                      orphanAdmitted
                    )
                  )
                }
                if (
                  operation != null && operation
                    .callback() == DeltaRangerNativeOperation.Callback.LOAD && plan.resolved
                ) {
                  plan.collect { case relation: LogicalRelation => relation }.foreach { relation =>
                    relation.relation match {
                      case fs: HadoopFsRelation =>
                        fs.location match {
                          case index: org.apache.spark.sql.delta.DeltaLogFileIndex =>
                            assertTrue(
                              index.rootPaths.forall(path => operation.ownsLogRoot(path.getParent))
                            )
                            if (index.rootPaths.exists(_.getName.endsWith(".checkpoint.parquet"))) {
                              assertEquals(
                                classOf[
                                  org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
                                ],
                                fs.fileFormat.getClass
                              )
                              kinds.add("checkpoint")
                            }
                            if (index.rootPaths.exists(_.getName.endsWith(".json")))
                              kinds.add("json")
                            assertTrue(DeltaRangerPlanAdmission.internalMetadata(relation))
                            inputs.add(relation)
                          case _ =>
                        }
                      case _ =>
                    }
                  }
                  plan
                    .collect { case union: org.apache.spark.sql.catalyst.plans.logical.Union =>
                      union
                    }
                    .foreach { union =>
                      if (DeltaRangerPlanAdmission.internalMetadata(union)) {
                        unions.add(union)
                        val foreign = LogicalRDD(
                          union.children.head.output,
                          SparkSession.active.sparkContext.emptyRDD[InternalRow]
                        )(SparkSession.active)
                        assertTrue(foreign.resolved)
                        assertFalse(
                          DeltaRangerPlanAdmission.internalMetadata(
                            union.copy(children = union.children.updated(0, foreign))
                          )
                        )
                        assertFalse(
                          DeltaRangerPlanAdmission.internalMetadata(union.copy(byName = true))
                        )
                        assertFalse(
                          DeltaRangerPlanAdmission
                            .internalMetadata(union.copy(byName = true, allowMissingCol = true))
                        )
                      }
                    }
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        }),
        beforeExtension = spark => {
          val location = NativeSelectFixture.locations("a")
          val checkpointLog = org.apache.spark.sql.delta.DeltaLog.forTable(spark, location)
          checkpointLog.checkpoint(checkpointLog.unsafeVolatileSnapshot)
          spark
            .sql("SELECT 4L id, 'a-tail' secret, 'drop' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
        }
      )
    finally f.engine.close()
  }

  @Test
  def actualDifferingFiltersReenterOnBroadcastWorkerAfterPreparation(): Unit = {
    val callbacks = new ConcurrentLinkedQueue[(String, PreparedDeltaFileIndex)]()
    val f = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val frame = spark.sql(
            "SELECT x.secret AS first, y.secret AS second FROM a.db.t x JOIN a.db.t y ON x.id=y.id WHERE y.region='keep'"
          )
          assertEquals(
            Set(Row("a-one-masked", "a-one-masked"), Row(null, null)),
            frame.collect().toSet
          )
          assertTrue(
            callbacks.asScala.exists(_._1.contains("broadcast")),
            "Must observe genuine differing-filter native work on the broadcast worker"
          )
          val installed =
            frame.queryExecution.optimizedPlan.collect { case relation: LogicalRelation =>
              relation.relation.asInstanceOf[HadoopFsRelation].location
            }
          callbacks.asScala.foreach { case (_, index) =>
            assertTrue(installed.exists(_ eq index))
          }
          assertNull(DeltaRangerNativeOperation.current())
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val op = DeltaRangerNativeOperation.current()
                if (op != null) op.invokedIndex() match {
                  case index: PreparedDeltaFileIndex =>
                    val filters = org.apache.spark.sql.catalyst.expressions
                      .ExpressionSet(op.partitionFilters() ++ op.dataFilters())
                    if (
                      filters != index.preparedScan.allFilters &&
                      filters != index.preparedScan.filtersUsedForSkipping
                    )
                      callbacks.add(Thread.currentThread().getName -> index)
                  case _ =>
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        })
      )
    finally f.engine.close()
  }

  @Test
  def foreignObjectFunctionsCannotAcquireNativeReconstructionEvidence(): Unit = {
    val attempts = new ConcurrentLinkedQueue[Boolean]()
    val originals = new ConcurrentLinkedQueue[(LogicalPlan, AnyRef)]()
    val selects = new ConcurrentLinkedQueue[Boolean]()
    val activeChanges = new ConcurrentLinkedQueue[Boolean]()
    val duplicatedFunctions = new ConcurrentLinkedQueue[Boolean]()
    val f = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals(
            Set(Row("a-one-masked"), Row(null)),
            spark.sql("SELECT secret FROM a.db.t").collect().toSet
          )
          spark.sql("SELECT id, region FROM b.db.t").collect()
          val first = originals.asScala.head
          val other = originals.asScala.find(entry => !(entry._2 eq first._2)).get
          assertNotNull(DeltaRangerNativeProvenance.requireExecutable(first._1.clone(), spark))
          val replaced = first._1.transformUp {
            case node: org.apache.spark.sql.catalyst.plans.logical.MapPartitions =>
              node.copy(func = other._2.asInstanceOf[Iterator[Any] => Iterator[Any]])
          }
          replaced.copyTagsFrom(first._1)
          val foreign = assertThrows(
            classOf[SecurityException],
            () => DeltaRangerNativeProvenance.requireExecutable(replaced, spark)
          )
          assertTrue(foreign.getMessage.contains("no sealed executable evidence"))
          val physical =
            new org.apache.spark.sql.execution.QueryExecution(spark, first._1.clone()).executedPlan
          assertFalse(physical.exists(_.isInstanceOf[DeltaRangerCollectGuardExec]))
          assertTrue(physical.executeCollect().nonEmpty)
          var functions = 0
          val changedFunction =
            physical.transformUp { case node: org.apache.spark.sql.execution.MapPartitionsExec =>
              functions += 1
              node.copy(func = other._2.asInstanceOf[Iterator[Any] => Iterator[Any]])
            }
          assertTrue(functions > 0)
          assertThrows(
            classOf[SecurityException],
            () => {
              DeltaRangerPhysicalProvenance.capture(changedFunction, spark); ()
            }
          )
          var encoders = 0
          val changedEncoder = physical.transformAllExpressions {
            case expression: org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke =>
              encoders += 1
              org.apache.spark.sql.catalyst.expressions.Literal.create(null, expression.dataType)
          }
          assertTrue(encoders > 0)
          assertThrows(
            classOf[SecurityException],
            () => {
              DeltaRangerPhysicalProvenance.capture(changedEncoder, spark); ()
            }
          )
          assertFalse(attempts.isEmpty, "Must exercise actual native object reconstruction")
          assertTrue(attempts.asScala.forall(!_))
          assertFalse(selects.isEmpty, "Must exercise actual native typed AddFile select")
          assertTrue(selects.asScala.forall(!_))
          assertFalse(duplicatedFunctions.isEmpty)
          assertTrue(
            duplicatedFunctions.asScala.forall(!_),
            "Captured function occurrence count cannot grow"
          )
          assertFalse(
            activeChanges.isEmpty,
            "Must replace a captured native function while an owner is active"
          )
          assertTrue(
            activeChanges.asScala.forall(!_),
            "An active owner cannot replace a captured function"
          )
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                if (plan.resolved && DeltaRangerNativeOperation.current() != null) {
                  plan.collect {
                    case node: org.apache.spark.sql.catalyst.plans.logical.MapElements =>
                      if (DeltaRangerPlanAdmission.internalMetadata(node)) {
                        import org.apache.spark.sql.catalyst.plans.logical._
                        import org.apache.spark.sql.catalyst.expressions._
                        val binding = node.child.asInstanceOf[DeserializeToObject]
                        val project = binding.child.asInstanceOf[Project]
                        val replacements = Seq(
                          node.copy(func = ((value: Any) => value)),
                          node.copy(argumentSchema = org.apache.spark.sql.types.StructType(Nil)),
                          node.copy(child =
                            binding.copy(deserializer =
                              Literal.create(null, binding.deserializer.dataType)
                            )
                          ),
                          node.copy(child =
                            binding.copy(child =
                              project.copy(projectList =
                                Seq(
                                  Alias(Literal.create(null, project.output.head.dataType), "add")()
                                )
                              )
                            )
                          ),
                          node.copy(child = binding.transformUp { case rdd: LogicalRDD =>
                            rdd.copy(rdd = SparkSession.active.sparkContext.emptyRDD[InternalRow])(
                              SparkSession.active,
                              None,
                              None
                            )
                          })
                        )
                        replacements.foreach(changed =>
                          selects.add(DeltaRangerPlanAdmission.internalMetadata(changed))
                        )
                      }
                  }
                }

                if (
                  plan.resolved && DeltaRangerNativeOperation.current() != null &&
                  plan.exists(
                    _.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.MapPartitions]
                  )
                ) {
                  if (DeltaRangerPlanAdmission.internalMetadata(plan)) {
                    val function = plan.collectFirst {
                      case node: org.apache.spark.sql.catalyst.plans.logical.MapPartitions =>
                        node.func
                    }.get
                    originals.asScala.find(entry => !(entry._2 eq function)).foreach { prior =>
                      val changed = plan.transformUp {
                        case node: org.apache.spark.sql.catalyst.plans.logical.MapPartitions =>
                          node.copy(func = prior._2.asInstanceOf[Iterator[Any] => Iterator[Any]])
                      }
                      changed.copyTagsFrom(plan)
                      activeChanges.add(DeltaRangerPlanAdmission.internalMetadata(changed))
                    }
                    val duplicate = plan.transformUp {
                      case node: org.apache.spark.sql.catalyst.plans.logical.MapPartitions =>
                        node.copy(child = node)
                    }
                    duplicate.copyTagsFrom(plan)
                    duplicatedFunctions.add(DeltaRangerPlanAdmission.internalMetadata(duplicate))
                    originals.add(plan -> function)
                  }
                  val altered = plan.transformUp {
                    case node: org.apache.spark.sql.catalyst.plans.logical.MapPartitions =>
                      node.copy(func = ((input: Iterator[Any]) => input))
                  }
                  attempts.add(DeltaRangerPlanAdmission.internalMetadata(altered))
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        })
      )
    finally f.engine.close()
  }

  @Test
  def snapshotCaptureRejectsCopiedIndicesForeignSnapshotsAndSessions(): Unit = {
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val foreignPlan = spark.sql("SELECT id, region FROM a.db.t").queryExecution.analyzed
      val foreign = foreignPlan.collectFirst { case relation: LogicalRelation =>
        relation.relation
          .asInstanceOf[HadoopFsRelation]
          .location
          .asInstanceOf[org.apache.spark.sql.delta.files.TahoeLogFileIndex]
          .getSnapshot
      }.get
      NativeSelectFixture.withNativeOperation(spark) { index =>
        val op = DeltaRangerNativeOperation.current()
        val snapshot = index.getSnapshot
        DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
        assertTrue(op.contains(index, snapshot))
        val copied = index.copy()
        assertEquals(index, copied)
        val copyFailure =
          assertThrows(classOf[SecurityException], () => op.capture(copied, snapshot))
        assertTrue(copyFailure.getMessage.contains("operation ownership"))
        assertThrows(classOf[SecurityException], () => op.capture(index, foreign))
        assertThrows(classOf[SecurityException], () => op.requireEvidence(spark.newSession()))
        assertNotNull(
          DeltaRangerNativeProvenance.requireExecutable(
            snapshot.stateDF.queryExecution.analyzed,
            spark
          )
        )
      }
    }
  }

  @Test
  def failedOperationsInvalidateTheirSealedRddsAndAlwaysClearTheThread(): Unit = {
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      var failed: LogicalRDD = null
      val failure = assertThrows(
        classOf[IllegalStateException],
        () =>
          NativeSelectFixture.withNativeOperation(spark) { index =>
            val snapshot = index.getSnapshot
            DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
            failed = snapshot.withStats.queryExecution.analyzed.clone().asInstanceOf[LogicalRDD]
            assertNotNull(DeltaRangerNativeProvenance.requireExecutable(failed, spark))
            throw new IllegalStateException("abort native invocation")
          }
      )
      assertEquals("abort native invocation", failure.getMessage)
      assertNull(DeltaRangerNativeOperation.current())
      val rejected = assertThrows(
        classOf[SecurityException],
        () => DeltaRangerNativeProvenance.requireExecutable(failed, spark)
      )
      assertTrue(rejected.getMessage.contains("no sealed executable evidence"))
      // A later successful invocation explicitly rebinds the current snapshot's returned lineage.
      NativeSelectFixture.withNativeOperation(spark) { index =>
        val snapshot = index.getSnapshot
        DeltaRangerPlanAdmission.registerSnapshot(index, snapshot)
        assertNotNull(
          DeltaRangerNativeProvenance.requireExecutable(
            snapshot.withStats.queryExecution.analyzed,
            spark
          )
        )
      }
      assertNull(DeltaRangerNativeOperation.current())
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerNativeProvenance.requireExecutable(failed, spark)
      )
    }
  }

  @Test
  def simultaneousFallbacksHaveIndependentFiniteOperations(): Unit = {
    val operations = new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
    val entered = new java.util.concurrent.CountDownLatch(2)
    val seen = java.util.concurrent.ConcurrentHashMap.newKeySet[Long]()
    val f = new TestDeltaRangerNativeSelect.Fixture
    val executor = java.util.concurrent.Executors.newFixedThreadPool(2)
    try
      NativeSelectFixture.withSpark(
        spark => {
          val frame = spark.sql("SELECT secret FROM a.db.t WHERE region='keep'")
          val plan = frame.queryExecution.optimizedPlan
          val index = plan.collectFirst { case relation: LogicalRelation =>
            relation.relation
              .asInstanceOf[HadoopFsRelation]
              .location
              .asInstanceOf[PreparedDeltaFileIndex]
          }.get
          val control = index.matchingFiles(Nil, Nil).size
          val calls = (1 to 2).map { _ =>
            executor.submit(new java.util.concurrent.Callable[Int] {
              override def call(): Int = {
                SparkSession.setActiveSession(spark)
                try index.matchingFiles(Nil, Nil).size
                finally {
                  assertNull(DeltaRangerNativeOperation.current());
                  SparkSession.clearActiveSession()
                }
              }
            })
          }
          assertEquals(
            calls.head.get(45, java.util.concurrent.TimeUnit.SECONDS),
            calls.last.get(45, java.util.concurrent.TimeUnit.SECONDS)
          )
          assertEquals(control, calls.head.get().intValue())
          assertEquals(2, operations.size())
          val captured = operations.asScala.toVector
          assertNotSame(captured.head, captured.last)
          assertTrue(captured.forall(_.succeeded()))
          assertNull(DeltaRangerNativeOperation.current())
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val op = DeltaRangerNativeOperation.current()
                if (
                  op != null && op.invokedIndex() != null &&
                  Thread.currentThread().getName.startsWith("pool-") && seen
                    .add(Thread.currentThread().getId)
                ) {
                  operations.add(op)
                  entered.countDown()
                  assertTrue(entered.await(30, java.util.concurrent.TimeUnit.SECONDS))
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        })
      )
    finally { executor.shutdownNow(); f.engine.close() }
  }

  @Test
  def exactPreparedOccurrencesRejectIndexCopiesAndInertReferences(): Unit = {
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val frame = spark.sql(
        "SELECT x.secret AS first, y.secret AS second FROM a.db.t x JOIN a.db.t y ON x.id=y.id WHERE y.region='keep'"
      )
      val plan = frame.queryExecution.optimizedPlan
      val proof = DeltaRangerNativeScans.prepared(plan)
      proof.verify(plan.clone(), spark)
      assertEquals(2, proof.owner.obligations.size)
      assertEquals(2, proof.occurrences.size)
      assertTrue(proof.occurrences.map(_.obligation).distinct.size == 2)
      assertThrows(classOf[SecurityException], () => proof.verify(plan, spark.newSession()))
      val relation = proof.occurrences.head.relation
      val fs = relation.relation.asInstanceOf[HadoopFsRelation]
      val index = fs.location.asInstanceOf[PreparedDeltaFileIndex]
      assertTrue(
        java.io.ObjectStreamClass.lookup(index.getClass).getFields.isEmpty,
        "Owned prepared index must add no serialized authority fields"
      )
      val copied = index.copy()
      assertEquals(index, copied)
      assertNotSame(index, copied)
      val changedRelation = relation.copy(relation = fs.copy(location = copied)(spark))
      // A singleton relation check reaches the same exact backing binding without relying on
      // transformUp, which intentionally preserves equal case-class children.
      val fakeProof = new DeltaRangerNativeScans.Reference(proof)
      val bytes = new java.io.ByteArrayOutputStream()
      val output = new java.io.ObjectOutputStream(bytes)
      try output.writeObject(fakeProof)
      finally output.close()
      val input = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes.toByteArray))
      val inert =
        try input.readObject().asInstanceOf[DeltaRangerNativeScans.Reference]
        finally input.close()
      assertThrows(classOf[SecurityException], () => inert.get)
      assertFalse(
        new String(bytes.toByteArray, java.nio.charset.StandardCharsets.ISO_8859_1)
          .contains("private-wrapper-token")
      )
      val mutation = plan match {
        case project: org.apache.spark.sql.catalyst.plans.logical.Project =>
          def replace(node: LogicalPlan): LogicalPlan = node match {
            case leaf: LogicalRelation if leaf.relation eq relation.relation => changedRelation
            case p: org.apache.spark.sql.catalyst.plans.logical.Project =>
              p.copy(child = replace(p.child))
            case f: org.apache.spark.sql.catalyst.plans.logical.Filter =>
              f.copy(child = replace(f.child))
            case j: org.apache.spark.sql.catalyst.plans.logical.Join =>
              j.copy(left = replace(j.left), right = replace(j.right))
            case other => other
          }
          replace(project)
        case _ => throw new AssertionError("Expected an installed self-join projection")
      }
      val rejected = assertThrows(classOf[SecurityException], () => proof.verify(mutation, spark))
      assertTrue(rejected.getMessage.contains("backing identity"))
      assertEquals(Set(Row("a-one-masked", "a-one-masked"), Row(null, null)), frame.collect().toSet)
      val physical = frame.queryExecution.sparkPlan
      val physicalScans = physical.collect {
        case scan: org.apache.spark.sql.execution.FileSourceScanExec => scan
      }
      assertEquals(2, physicalScans.size)
      physicalScans.foreach { scan =>
        val cloned = scan.clone().asInstanceOf[org.apache.spark.sql.execution.FileSourceScanExec]
        assertTrue(cloned.relation.location eq scan.relation.location)
        assertTrue(proof.occurrences.exists(_.index eq cloned.relation.location))
      }
      NativeSelectFixture.bindingFixture(spark, "a").wrapper.close()
      assertThrows(classOf[SecurityException], () => index.matchingFiles(Nil, Nil))
      assertNull(DeltaRangerNativeOperation.current())
    }
  }

  @Test
  def installedNoSkippingQueriesKeepNativeSettingAndRows(): Unit = {
    val f = new TestDeltaRangerNativeSelect.Fixture
    val sizes = new ConcurrentLinkedQueue[DeltaRangerLogIndex]()
    val aggregates = new ConcurrentLinkedQueue[LogicalPlan]()
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals("false", spark.conf.get("spark.databricks.delta.stats.skipping"))
          val frame = spark.sql(
            "SELECT x.secret AS first, y.secret AS second FROM a.db.t x JOIN a.db.t y ON x.id=y.id WHERE y.region='keep'"
          )
          assertEquals(
            Set(Row("a-one-masked", "a-one-masked"), Row(null, null)),
            frame.collect().toSet
          )
          assertEquals(
            Set(Row(1L, "keep"), Row(2L, "drop"), Row(3L, "keep")),
            spark.sql("SELECT id, region FROM b.db.t").collect().toSet
          )
          val installed =
            frame.queryExecution.optimizedPlan.collect { case relation: LogicalRelation =>
              relation.relation.asInstanceOf[HadoopFsRelation].location
            }
          assertTrue(installed.forall(_.isInstanceOf[DeltaRangerLogIndex]))
          installed.foreach { value =>
            val index = value.asInstanceOf[DeltaRangerLogIndex]
            assertTrue(index.inputFiles.nonEmpty)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(java.io.ObjectStreamClass.lookup(index.getClass).getFields.isEmpty)
            assertNotSame(index, index.copy())
          }
          assertFalse(
            sizes.isEmpty,
            "Cold native size aggregation must run through the owned index"
          )
          sizes.asScala.foreach(index => assertTrue(installed.exists(_ eq index)))
          assertNull(DeltaRangerNativeOperation.current())
          assertFalse(aggregates.isEmpty, "Must capture actual owned size aggregate")
          aggregates.asScala.foreach { plan =>
            val physical =
              new org.apache.spark.sql.execution.QueryExecution(spark, plan.clone()).executedPlan
            assertFalse(physical.exists(_.isInstanceOf[DeltaRangerCollectGuardExec]))
            assertTrue(physical.executeCollect().nonEmpty)
            var changedOutputs = 0
            val changed = physical.transformAllExpressions {
              case alias: org.apache.spark.sql.catalyst.expressions.Alias =>
                changedOutputs += 1
                alias.withNewChildren(
                  Seq(
                    org.apache.spark.sql.catalyst.expressions.Literal
                      .create(null, alias.child.dataType)
                  )
                )
            }
            assertTrue(changedOutputs > 0)
            assertThrows(
              classOf[SecurityException],
              () => {
                DeltaRangerPhysicalProvenance.capture(changed, spark); ()
              }
            )
          }
          assertEquals("false", spark.conf.get("spark.databricks.delta.stats.skipping"))
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                val op = DeltaRangerNativeOperation.current()
                if (
                  op != null && op
                    .callback() == DeltaRangerNativeOperation.Callback.SIZE_IN_BYTES &&
                  plan.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Aggregate]
                ) {
                  sizes.add(op.invokedIndex().asInstanceOf[DeltaRangerLogIndex])
                  DeltaRangerNativeProvenance.classify(plan) match {
                    case _: DeltaRangerNativeProvenance.SealedNative => aggregates.add(plan.clone())
                    case _                                           =>
                  }
                }
                plan
              }
            }
          )
          NativeSelectFixture.extension(f)(extensions)
        }),
        Map("spark.databricks.delta.stats.skipping" -> "false")
      )
    finally f.engine.close()
  }
}
