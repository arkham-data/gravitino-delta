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

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.{Callable, ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{LongType, StringType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerNoSkipping {
  @Test
  def installedJoinsMappedColumnsAndPartitionFiltersKeepTheirOriginalOwners(): Unit = {
    val callbacks = new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
    withSpark(
      spark => {
        val queries = Seq(
          (
            "SELECT x.secret, y.secret FROM a.db.t x JOIN b.db.t y ON x.id=y.id",
            Set(Row("a-one-masked", null), Row(null, null)),
            Set("a", "b")
          ),
          (
            "SELECT `odd`` name` FROM q.db.t WHERE region='keep'",
            Set(Row("q-one"), Row(null)),
            Set("q")
          ),
          (
            "SELECT secret FROM a.db.t WHERE region='keep' AND id=1L",
            Set(Row("a-one-masked")),
            Set("a")
          )
        )
        queries.foreach { case (sql, rows, catalogs) =>
          val frame = spark.sql(sql)
          val plan = frame.queryExecution.optimizedPlan
          val proof = DeltaRangerNativeScans.prepared(plan)
          assertEquals(catalogs, proof.owner.obligations.map(_.scan.catalog).toSet)
          assertEquals(catalogs.size, proof.owner.obligations.size)
          assertEquals(rows, frame.collect().toSet)
          proof.occurrences.foreach { occurrence =>
            val index = occurrence.index.asInstanceOf[DeltaRangerLogIndex]
            val original = occurrence.obligation.binding.index()
            if (occurrence.obligation.scan.catalog == "q") {
              assertTrue(occurrence.obligation.columns.contains("odd` name"))
              assertTrue(
                occurrence.obligation.binding.nativeSchemaJson().contains("delta.columnMapping.id")
              )
            }
            assertNotSame(original, index)
            assertSame(original.deltaLog, index.deltaLog)
            assertSame(original.snapshotAtAnalysis, index.snapshotAtAnalysis)
            assertSame(original.partitionFilters, index.partitionFilters)
            assertSame(
              occurrence.obligation.binding.descriptor(),
              occurrence.relation.catalogTable.get
            )
            assertTrue(callbacks.asScala.exists(_.invokedIndex() eq index))
          }
        }
        val plan = spark.sql("SELECT id, region FROM a.db.t").queryExecution.optimizedPlan
        val proof = DeltaRangerNativeScans.prepared(plan)
        val index = proof.occurrences.head.index.asInstanceOf[DeltaRangerLogIndex]
        assertEquals(Seq("region"), index.metadata.partitionColumns)
        val partitions: Seq[Expression] =
          Seq(EqualTo(AttributeReference("region", StringType)(), Literal("keep")))
        val data: Seq[Expression] =
          Seq(GreaterThan(AttributeReference("id", LongType)(), Literal(0L)))
        callbacks.clear()
        val files = index.matchingFiles(partitions, data)
        assertFalse(files.isEmpty)
        assertTrue(files.forall(_.partitionValues("region") == "keep"))
        val observed = callbacks.asScala.filter(_.invokedIndex() eq index).toVector
        assertFalse(observed.isEmpty)
        assertTrue(
          observed.forall(op => (op.partitionFilters() eq partitions) && (op.dataFilters() eq data))
        )
        assertEquals(Nil, index.partitionFilters)
        assertEquals("false", spark.conf.get("spark.databricks.delta.stats.skipping"))
      },
      (_, op) =>
        if (op != null && op.invokedIndex().isInstanceOf[DeltaRangerLogIndex]) callbacks.add(op),
      spark => {
        val partitioned =
          Paths.get(NativeSelectFixture.locations("a")).resolveSibling("partitioned-a").toString
        spark.read
          .format("delta")
          .load(NativeSelectFixture.locations("a"))
          .select("id", "region", "secret")
          .write
          .format("delta")
          .partitionBy("region")
          .save(partitioned)
        NativeSelectFixture.locations += "a" -> partitioned
      }
    )
  }

  @Test
  def noSkippingProofRejectsEqualSubstitutionsAndDifferentSessionsButKeepsClones(): Unit =
    withSpark { spark =>
      val frame = spark.sql("SELECT secret FROM a.db.t WHERE id=1L")
      val plan = frame.queryExecution.optimizedPlan
      val proof = DeltaRangerNativeScans.prepared(plan)
      proof.verify(plan.clone(), spark)
      val wrongSession =
        assertThrows(classOf[SecurityException], () => proof.verify(plan, spark.newSession()))
      assertTrue(wrongSession.getMessage.contains("operation ownership"))
      val occurrence = proof.occurrences.head
      val relation = occurrence.relation
      val fs = relation.relation.asInstanceOf[HadoopFsRelation]
      val index = occurrence.index.asInstanceOf[DeltaRangerLogIndex]
      val copied = index.copy()
      assertEquals(index, copied)
      assertNotSame(index, copied)
      val replacement = relation.copy(relation = fs.copy(location = copied)(spark))
      def replace(node: LogicalPlan): LogicalPlan = node match {
        case value: LogicalRelation if value.relation eq relation.relation => replacement
        case value: Project => value.copy(child = replace(value.child))
        case value: Filter  => value.copy(child = replace(value.child))
        case other          => other
      }
      val failure =
        assertThrows(classOf[SecurityException], () => proof.verify(replace(plan), spark))
      assertTrue(failure.getMessage.contains("backing identity"))
      assertEquals(Set(Row("a-one-masked")), frame.collect().toSet)
      val scans = frame.queryExecution.sparkPlan.collect { case scan: FileSourceScanExec => scan }
      assertEquals(1, scans.size)
      scans.foreach(scan =>
        assertSame(index, scan.clone().asInstanceOf[FileSourceScanExec].relation.location)
      )
      assertSame(
        occurrence.obligation.binding.index(),
        occurrence.obligation.binding.relation().location
      )
    }

  @Test
  def actualNewHeadIsCapturedAndNativeSchemaAndMappingChecksStillReject(): Unit = {
    val commits = scala.collection.mutable.Map.empty[(String, Long), (Path, Array[Byte])]
    val captured = new ConcurrentLinkedQueue[(DeltaRangerNativeOperation, Snapshot)]()
    withSpark(
      spark => {
        val aPlan = spark.sql("SELECT id, region FROM a.db.t").queryExecution.optimizedPlan
        val aProof = DeltaRangerNativeScans.prepared(aPlan)
        val a = aProof.occurrences.head.index.asInstanceOf[DeltaRangerLogIndex]
        val qPlan = spark.sql("SELECT `odd`` name` FROM q.db.t").queryExecution.optimizedPlan
        val qProof = DeltaRangerNativeScans.prepared(qPlan)
        val q = qProof.occurrences.head.index.asInstanceOf[DeltaRangerLogIndex]
        val original = aProof.owner.obligations.head.binding.index()
        assertEquals(0L, a.getSnapshot.version)
        assertEquals(0L, q.getSnapshot.version)
        def publish(catalog: String, version: Long): Unit = {
          val (path, bytes) = commits((catalog, version))
          Files.write(path, bytes)
        }
        val oldFiles = a.matchingFiles(Nil, Nil).map(_.path).toSet
        publish("a", 1L)
        val selected = a.getSnapshot
        assertEquals(1L, selected.version)
        assertSame(selected, a.deltaLog.unsafeVolatileSnapshot)
        assertEquals(0L, a.snapshotAtAnalysis.version)
        assertSame(original.snapshotAtAnalysis, a.snapshotAtAnalysis)
        assertTrue(captured.asScala.exists { case (op, snapshot) =>
          (snapshot eq selected) && (op.invokedIndex() eq a) && op.contains(
            original,
            selected
          ) && op.succeeded()
        })
        assertEquals(oldFiles.size + 1, a.matchingFiles(Nil, Nil).size)
        aProof.verify(aPlan, spark)
        publish("a", 2L)
        val schemaFailure = assertThrows(classOf[Exception], () => a.getSnapshot)
        assertNativeCompatibility(schemaFailure)
        assertNull(DeltaRangerNativeOperation.current())
        val logicalBefore =
          q.snapshotAtAnalysis.schema.map(field => (field.name, field.dataType, field.nullable))
        (1L to 3L).foreach(version => publish("q", version))
        val mappingFailure = assertThrows(classOf[Exception], () => q.getSnapshot)
        assertNativeCompatibility(mappingFailure)
        val mappedHead = q.deltaLog.unsafeVolatileSnapshot
        assertEquals(3L, mappedHead.version)
        assertEquals(
          logicalBefore,
          mappedHead.schema.map(field => (field.name, field.dataType, field.nullable))
        )
        assertNotEquals(
          q.snapshotAtAnalysis.schema("odd` name").metadata,
          mappedHead.schema("odd` name").metadata
        )
        assertNull(DeltaRangerNativeOperation.current())
        assertSame(original, aProof.owner.obligations.head.binding.index())
      },
      (_, op) =>
        if (
          op != null && op.invokedIndex().isInstanceOf[DeltaRangerLogIndex] && op
            .capturingSnapshot() != null
        )
          captured.add((op, op.capturingSnapshot())),
      spark => {
        val a = NativeSelectFixture.locations("a")
        spark
          .sql("SELECT 4L id, 'a-tail' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .save(a)
        spark
          .sql("SELECT 'changed' id, 'a-new' secret, 'keep' region")
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(a)
        val q = NativeSelectFixture.locations("q")
        spark.sql(s"ALTER TABLE delta.`$q` RENAME COLUMN `odd`` name` TO renamed").collect()
        spark.sql(s"ALTER TABLE delta.`$q` DROP COLUMN renamed").collect()
        spark.sql(s"ALTER TABLE delta.`$q` ADD COLUMNS (`odd`` name` STRING)").collect()
        Seq(("a", 1L), ("a", 2L), ("q", 1L), ("q", 2L), ("q", 3L)).foreach {
          case key @ (catalog, version) =>
            val path =
              Paths.get(NativeSelectFixture.locations(catalog), "_delta_log", f"$version%020d.json")
            commits(key) = (path, Files.readAllBytes(path))
        }
        // Preserve native-generated bytes and data files. Only these temp fixture commits are
        // withheld until after protected analysis; publishing them restores the exact valid history.
        commits.values.foreach { case (path, _) => Files.delete(path) }
      }
    )
  }

  @Test
  def callbackFailureFailedPreparationAndClosedOwnerLeaveNoAuthorityOnThread(): Unit = {
    val abort = new AtomicBoolean()
    val failed = new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
    withSpark(
      spark => {
        val plan = spark.sql("SELECT id, region FROM a.db.t").queryExecution.optimizedPlan
        val index = DeltaRangerNativeScans
          .prepared(plan)
          .occurrences
          .head
          .index
          .asInstanceOf[DeltaRangerLogIndex]
        assertFalse(index.matchingFiles(Nil, Nil).isEmpty)
        abort.set(true)
        val thrown = assertThrows(classOf[Exception], () => index.matchingFiles(Nil, Nil))
        assertTrue(causes(thrown).exists(_.getMessage == "no-skipping callback abort"))
        assertNull(DeltaRangerNativeOperation.current())
        assertEquals(1, failed.size())
        assertFalse(failed.peek().succeeded())
        assertThrows(classOf[SecurityException], () => failed.peek().requireEvidence(spark))
        assertNotNull(index.getSnapshot)
        val analyzed = spark.sql("SELECT id, region FROM a.db.t").queryExecution.analyzed
        val owner = DeltaRangerNativeScans.consume(analyzed, spark, optimized = false)
        var abandoned: DeltaRangerLogIndex = null
        assertThrows(
          classOf[IllegalStateException],
          () =>
            owner.run { op =>
              abandoned =
                DeltaRangerLogIndex.create(owner, owner.obligations.head.binding.index(), op)
              throw new IllegalStateException("failed native preparation")
            }
        )
        assertThrows(classOf[SecurityException], () => abandoned.getSnapshot)
        assertNull(DeltaRangerNativeOperation.current())
        NativeSelectFixture.bindingFixture(spark, "a").wrapper.close()
        assertThrows(classOf[SecurityException], () => index.getSnapshot)
        assertNull(DeltaRangerNativeOperation.current())
      },
      (_, op) =>
        if (
          op != null && op.invokedIndex().isInstanceOf[DeltaRangerLogIndex] && abort
            .compareAndSet(true, false)
        ) {
          failed.add(op)
          throw new IllegalStateException("no-skipping callback abort")
        }
    )
  }

  @Test
  def overlappingNoSkippingCallbacksHaveIndependentOperationsAndCleanup(): Unit = {
    val operations = new ConcurrentLinkedQueue[DeltaRangerNativeOperation]()
    val entered = new CountDownLatch(2)
    val seen = java.util.concurrent.ConcurrentHashMap.newKeySet[Long]()
    val executor = Executors.newFixedThreadPool(2)
    try
      withSpark(
        spark => {
          val plan = spark.sql("SELECT id, region FROM a.db.t").queryExecution.optimizedPlan
          val index = DeltaRangerNativeScans
            .prepared(plan)
            .occurrences
            .head
            .index
            .asInstanceOf[DeltaRangerLogIndex]
          val control = index.matchingFiles(Nil, Nil).map(_.path).toSet
          val calls = (1 to 2).map { _ =>
            executor.submit(new Callable[Set[String]] {
              override def call(): Set[String] = {
                SparkSession.setActiveSession(spark)
                try index.matchingFiles(Nil, Nil).map(_.path).toSet
                finally {
                  assertNull(DeltaRangerNativeOperation.current());
                  SparkSession.clearActiveSession()
                }
              }
            })
          }
          calls.foreach(call => assertEquals(control, call.get(45, TimeUnit.SECONDS)))
          assertEquals(2, operations.size())
          assertNotSame(operations.asScala.head, operations.asScala.last)
          assertTrue(operations.asScala.forall(_.succeeded()))
          assertNull(DeltaRangerNativeOperation.current())
        },
        (_, op) =>
          if (
            op != null && op.invokedIndex().isInstanceOf[DeltaRangerLogIndex] &&
            Thread.currentThread().getName.startsWith("pool-") && seen
              .add(Thread.currentThread().getId)
          ) {
            operations.add(op); entered.countDown(); assertTrue(entered.await(30, TimeUnit.SECONDS))
          }
      )
    finally executor.shutdownNow()
  }

  private def causes(failure: Throwable): Vector[Throwable] =
    if (failure == null) Vector.empty else failure +: causes(failure.getCause)

  private def assertNativeCompatibility(failure: Throwable): Unit = {
    assertTrue(
      causes(failure).exists {
        case native: AnalysisException =>
          native.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"
        case _ => false
      },
      "Must retain Delta's precise schema/column-mapping compatibility error"
    )
    assertTrue(
      causes(failure).exists(
        _.getStackTrace.exists(frame =>
          frame.getClassName == "org.apache.spark.sql.delta.files.TahoeLogFileIndex" && frame.getMethodName == "getSnapshot"
        )
      ),
      "Must reach native snapshot compatibility checks"
    )
    assertFalse(
      causes(failure).exists(_.isInstanceOf[SecurityException]),
      "Native incompatibility must not be confused with Ranger admission denial"
    )
  }

  private def withSpark(
      body: SparkSession => Unit,
      observer: (LogicalPlan, DeltaRangerNativeOperation) => Unit = (_, _) => (),
      setup: SparkSession => Unit = _ => ()
  ): Unit = {
    val fixture = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals("false", spark.conf.get("spark.databricks.delta.stats.skipping"));
          body(spark)
          assertEquals("false", spark.conf.get("spark.databricks.delta.stats.skipping"))
        },
        Some(extensions => {
          extensions.injectResolutionRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = {
                observer(plan, DeltaRangerNativeOperation.current()); plan
              }
            }
          )
          NativeSelectFixture.extension(fixture)(extensions)
        }),
        Map("spark.databricks.delta.stats.skipping" -> "false"),
        setup
      )
    finally fixture.engine.close()
  }
}
