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

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream,
  NotSerializableException
}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.{Project, Filter}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Foreign plans and native reconstruction state cannot inherit another action's authority. */
class TestDeltaRangerCdfReadOwnership {
  @Test
  def ordinaryOperatorsDoNotWidenCdfSources(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      Seq[org.apache.spark.sql.DataFrame => org.apache.spark.sql.DataFrame](
        _.selectExpr("count(id)"),
        _.selectExpr("1 AS one"),
        _.distinct(),
        _.orderBy("id"),
        _.limit(1)
      ).foreach { operator =>
        assertThrows(classOf[SecurityException], () => operator(fixture.read(spark)).collect())
      }
    }
  }

  @Test
  def unrecognizedManifestOwnerCannotMintHelper(): Unit = {
    val fake = new DeltaRangerCdfReadScans.MatchOwner {
      override def requireHelper(candidate: DeltaRangerCdfReadScans.Match): Unit = ()
    }
    val rejected = assertThrows(
      classOf[SecurityException],
      () => new DeltaRangerCdfReadScans.Match(fake, null, Vector.empty, Nil, null)
    )
    assertTrue(rejected.getMessage.contains("unrecognized manifest owner"))
  }

  @Test
  def concurrentAnalyzedSiblingsIssueExactlyOneReceipt(): Unit = {
    import java.util.IdentityHashMap
    import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
    import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      val base = fixture.read(spark)
      val first = base.select("id")
      val second = base.select("_change_type")
      first.queryExecution.analyzed
      second.queryExecution.analyzed
      val source = first.queryExecution.analyzed.collectFirst { case relation: LogicalRelation =>
        relation.relation.asInstanceOf[DeltaRangerCdfRead.Source]
      }.get
      val lookup = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val secondLookup = new CountDownLatch(1)
      val calls = new AtomicInteger
      val field = source.getClass.getDeclaredFields.find(_.getName == "observations").get
      field.setAccessible(true)
      val original = field
        .get(source)
        .asInstanceOf[IdentityHashMap[LogicalPlan, DeltaRangerCdfReadReceipt.Observation]]
      val coordinated = new IdentityHashMap[LogicalPlan, DeltaRangerCdfReadReceipt.Observation] {
        override def get(key: AnyRef): DeltaRangerCdfReadReceipt.Observation = {
          if (calls.incrementAndGet() == 1) {
            lookup.countDown()
            assertTrue(release.await(20, TimeUnit.SECONDS), "release first lookup")
          } else secondLookup.countDown()
          super.get(key)
        }
      }
      coordinated.putAll(original)
      field.set(source, coordinated)
      val pool = Executors.newFixedThreadPool(2)
      val secondThread = new AtomicReference[Thread]
      def normalize(frame: org.apache.spark.sql.DataFrame, record: Boolean) =
        new Callable[Either[Throwable, LogicalPlan]] {
          override def call(): Either[Throwable, LogicalPlan] = {
            if (record) secondThread.set(Thread.currentThread())
            SparkSession.setActiveSession(spark)
            try {
              val normalized = frame.queryExecution.normalized
              assertTrue(normalized.isInstanceOf[DeltaRangerCdfRead.Carrier])
              assertSame(normalized, frame.queryExecution.normalized)
              Right(normalized)
            } catch { case failure: Exception => Left(failure) }
            finally SparkSession.clearActiveSession()
          }
        }
      try {
        val one = pool.submit(normalize(first, false))
        assertTrue(lookup.await(20, TimeUnit.SECONDS), "first query reaches observation lookup")
        val two = pool.submit(normalize(second, true))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        def blockedOnSource: Boolean = Option(secondThread.get()).exists { thread =>
          val info =
            java.lang.management.ManagementFactory.getThreadMXBean.getThreadInfo(thread.getId)
          info != null && info.getThreadState == Thread.State.BLOCKED && info.getLockInfo != null &&
          info.getLockInfo.getIdentityHashCode == System.identityHashCode(source)
        }
        while (secondLookup.getCount != 0 && !blockedOnSource && System.nanoTime() < deadline)
          Thread.sleep(1)
        assertTrue(
          secondLookup.getCount == 0 || blockedOnSource,
          "second reader reaches the coordinated boundary"
        )
        release.countDown()
        val results = Seq(one.get(30, TimeUnit.SECONDS), two.get(30, TimeUnit.SECONDS))
        assertEquals(1, results.count(_.isRight), results.toString)
        assertEquals(1, calls.get(), "only one observation can be consumed")
        assertTrue(
          results
            .collect { case Left(error) => error.getMessage }
            .exists(_.contains("reused CDF source"))
        )
      } finally {
        release.countDown(); pool.shutdownNow();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS))
      }
    }
  }

  @Test
  def foreignHistoryAndCachesCannotBorrowAuthority(): Unit = {
    val fixture = new DeltaRangerCdfReadFixture
    DeltaRangerCdfReadFixture.withGoverned(fixture.prepare) { spark =>
      import scala.collection.JavaConverters._
      val wrapper = NativeSelectFixture.wrapper(spark, "a")
      val native = wrapper
        .loadTable(org.apache.spark.sql.connector.catalog.Identifier.of(Array("db"), "t"))
        .asInstanceOf[org.apache.spark.sql.delta.catalog.DeltaTableV2]
      val foreignDescriptor = native.copy(catalogTable = Some(native.catalogTable.get.copy()))
      val foreignRelation = org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
        .create(foreignDescriptor, None, None)
        .copy(options =
          new org.apache.spark.sql.util.CaseInsensitiveStringMap(
            Map(
              "readChangeFeed" -> "true",
              "startingVersion" -> fixture.start.toString,
              "endingVersion" -> fixture.end.toString
            ).asJava
          )
        )
      assertThrows(
        classOf[Exception],
        () => TestDeltaRangerNativeSelect.rows(spark, foreignRelation)
      )
      val frame = fixture.read(spark)
      val original = frame.queryExecution.analyzed
      val source = original.collectFirst { case relation: LogicalRelation => relation }.get
      val copied = source.copy()
      assertThrows(classOf[Exception], () => TestDeltaRangerNativeSelect.rows(spark, copied))
      val rows = frame.collect().toVector
      assertThrows(classOf[Exception], () => { frame.select("id").collect(); () })
      val carrier = frame.queryExecution.optimizedPlan.asInstanceOf[DeltaRangerCdfRead.Carrier]
      def serialized(value: AnyRef): AnyRef = {
        val bytes = new ByteArrayOutputStream
        val out = new ObjectOutputStream(bytes)
        try out.writeObject(value)
        finally out.close()
        val in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
        try in.readObject()
        finally in.close()
      }
      val restored = serialized(carrier).asInstanceOf[DeltaRangerCdfRead.Carrier]
      assertThrows(classOf[SecurityException], () => restored.receipt)
      assertThrows(classOf[NotSerializableException], () => serialized(carrier.receipt))
      val changed = carrier.copy(output =
        carrier.output.head.withName("foreign") +: carrier.output.tail
      )(carrier.receipt)
      assertThrows(classOf[SecurityException], () => changed.receipt)
      assertThrows(
        classOf[Exception],
        () => TestDeltaRangerNativeSelect.rows(spark, Project(carrier.output, carrier))
      )
      assertThrows(
        classOf[Exception],
        () => TestDeltaRangerNativeSelect.rows(spark, Filter(Literal.TrueLiteral, carrier))
      )
      val other = DeltaRangerCdfReadFixture.read(spark, fixture.start + 1, fixture.end)
      other.collect()
      val foreign = other.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfProducer]
      val producer = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfProducer]
      assertThrows(
        classOf[SecurityException],
        () => producer.receipt.validateCollection(foreign.child)
      )
      assertThrows(classOf[SecurityException], () => producer.withNewChildren(Seq(foreign.child)))
      def field[T](value: AnyRef, name: String): T = {
        val member = value.getClass.getDeclaredFields.find(_.getName == name).get
        member.setAccessible(true); member.get(value).asInstanceOf[T]
      }
      val owner = field[DeltaRangerCdfReadScans](carrier.receipt, "scans")
      val otherOwner = field[DeltaRangerCdfReadScans](foreign.receipt, "scans")
      assertThrows(classOf[SecurityException], () => owner.requireInterval(otherOwner.interval))
      val index = DeltaRangerPhysicalProvenance
        .nodes(producer.child)
        .collectFirst { case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
          scan.relation.location.asInstanceOf[DeltaRangerCdfReadScans.OwnedIndex]
        }
        .get
      val nativeIndex =
        field[org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor](
          index,
          "original"
        )
      val wrongInterval = assertThrows(
        classOf[SecurityException],
        () =>
          owner.matchingFiles(nativeIndex, index, Seq.empty) {
            owner.requireInterval(otherOwner.interval)
            Seq.empty
          }
      )
      assertTrue(wrongInterval.getMessage.contains("foreign interval"))
      val injected = new IllegalStateException("injected native matching reconstruction failure")
      assertSame(
        injected,
        assertThrows(
          classOf[IllegalStateException],
          () => owner.matchingFiles(nativeIndex, index, Seq.empty) { throw injected }
        )
      )
      assertNull(DeltaRangerNativeOperation.current())
      assertThrows(
        classOf[Exception],
        () => { spark.sql("SELECT 99 AS foreign_raw").collect(); () }
      )

      assertEquals(fixture.multiset(rows), fixture.multiset(frame.collect().toVector))
      assertNull(DeltaRangerNativeOperation.current())
    }
  }

  @Test
  def nativeCachesInBothSourceOrdersAndFailureCleanup(): Unit = {
    for (cold <- Seq(true, false); order <- Seq(Seq("a", "b"), Seq("b", "a"))) {
      val fixture = new DeltaRangerCdfReadFixture
      val policy = new TestDeltaRangerNativeSelect.Fixture
      policy.filter = null; policy.maskColumn = "unused"
      try
        NativeSelectFixture.withSpark(
          spark => {
            // The second name deliberately targets the same native history through its own registration.
            NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
            def read(name: String) = spark.read
              .option("readChangeFeed", "true")
              .option("startingVersion", fixture.start)
              .option("endingVersion", fixture.end)
              .table(s"$name.db.t")
            val first = read(order.head)
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(first.collect().toVector)
            )
            val second = read(order.last)
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(second.collect().toVector)
            )
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(first.collect().toVector)
            )
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(read(order.head).collect().toVector)
            )
            val file = DeltaRangerCdfReadFixture.logFile(fixture.start)
            val bytes = java.nio.file.Files.readAllBytes(file)
            val modified = java.nio.file.Files.getLastModifiedTime(file)
            try {
              java.nio.file.Files
                .write(file, "corrupt-history\n".getBytes(java.nio.charset.StandardCharsets.UTF_8))
              assertThrows(classOf[Exception], () => { read(order.head).collect(); () })
              assertNull(DeltaRangerNativeOperation.current())
              assertThrows(
                classOf[Exception],
                () => { spark.sql("SELECT 99 AS foreign_raw").collect(); () }
              )
            } finally {
              java.nio.file.Files.write(file, bytes)
              java.nio.file.Files.setLastModifiedTime(file, modified)
            }
            assertEquals(
              fixture.multiset(fixture.expected),
              fixture.multiset(read(order.last).collect().toVector)
            )
            assertNull(DeltaRangerNativeOperation.current())
          },
          Some(NativeSelectFixture.extension(policy)),
          beforeExtension = spark => {
            fixture.prepare(spark)
            // Establish the cold case by evicting only this fixture's table once before admission.
            // Never clear global caches or evict between source orders/repeated actions.
            if (cold)
              org.apache.spark.sql.delta.DeltaLog.invalidateCache(
                spark,
                new org.apache.hadoop.fs.Path(DeltaRangerCdfReadFixture.location)
              )
          },
          clearNativeCacheBeforeExtension = false
        )
      finally policy.engine.close()
    }
  }
}
