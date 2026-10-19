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

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.nio.file.attribute.PosixFilePermissions
import java.util.{Collections, LinkedHashMap}
import java.util.concurrent.{
  Callable,
  CountDownLatch,
  Executors,
  ScheduledExecutorService,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.gravitino.{Catalog, NameIdentifier}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.rel.{Table, TableCatalog}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito._
import scala.collection.JavaConverters._

class TestDeltaRangerRawMaintenanceDeployment {
  private val setting = "spark.gravitino.deltaRanger.rawCacheMaintenance.config"
  private val prefix = "delta.registration.receipt."

  private def config(catalogs: String*): Path = {
    val path = Files.createTempFile("raw-deployment", ".properties")
    val records = catalogs.zipWithIndex.map { case (catalog, i) =>
      s"table.$i.metalake=lake\ntable.$i.catalog=$catalog\ntable.$i.database=db\ntable.$i.table=t\n"
    }.mkString
    Files.write(
      path,
      (s"version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=${catalogs.size}\n" + records)
        .getBytes(StandardCharsets.UTF_8)
    )
    Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))
    path
  }

  private def observe(spark: SparkSession) =
    DeltaRangerRawMaintenanceDeployment.observeRegistrations(spark)
  private def denied(body: => Any): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => { body; () })
    assertEquals("Raw maintenance registration observation is unavailable", failure.getMessage)
    assertNull(failure.getCause)
  }
  private def withEnabled(
      catalogs: Seq[String] = Seq("a")
  )(test: (SparkSession, TestDeltaRangerNativeSelect.Fixture, Path) => Unit): Unit = {
    val path = config(catalogs: _*)
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => test(spark, policy, path),
        Some(NativeSelectFixture.extension(policy)),
        Map(setting -> path.toString)
      )
    finally policy.engine.close()
  }

  @Test def enabledActualWrapperReadsStartupAnchorsWithoutAuthorization(): Unit = withEnabled() {
    (spark, policy, _) =>
      val jobs = spark.sparkContext.statusTracker.getJobIdsForGroup(null).toSet
      when(policy.plugin.getCurrentRangerAuthContext()).thenReturn(null)
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try {
        denied(observe(spark))
        policy.start()
        assertEquals(
          2,
          fixture.reads.get(),
          "Enabled wrapper must pin two real metadata publications"
        )
        val publications = observe(spark)
        assertEquals(1, publications.size())
        assertEquals("a", publications.get(0).publication().catalog())
        assertEquals(4, fixture.reads.get())
        publications.get(0).requireCurrent(spark)
        assertEquals(5, fixture.reads.get())
        assertEquals(1, fixture.opens.get())
        assertThrows(classOf[UnsupportedOperationException], () => publications.clear())
        assertTrue(policy.requests.isEmpty)
        assertNull(DeltaRangerNativeOperation.current())
        assertEquals(jobs, spark.sparkContext.statusTracker.getJobIdsForGroup(null).toSet)
        val foreign = classOf[SparkSession]
          .getConstructor(classOf[SparkContext])
          .newInstance(spark.sparkContext)
        assertNotSame(spark.sharedState, foreign.sharedState)
        denied(observe(foreign))
        denied(publications.get(0).requireCurrent(foreign))
        val unmatchedPolicy = new TestDeltaRangerNativeSelect.Fixture
        val unmatched = new NativeBoundCatalogFixture(spark, unmatchedPolicy, "q")
        try {
          assertEquals(0, unmatched.reads.get())
          assertEquals(0, unmatched.opens.get())
          assertEquals(1, observe(spark).size())
        } finally { unmatched.close(); unmatchedPolicy.engine.close() }
        publications.get(0).requireCurrent(spark)
        policy.engine.close()
        denied(publications.get(0).requireCurrent(spark))
        denied(observe(spark))

      } finally fixture.close()
      denied(observe(spark))
  }

  @Test def coldForeignSessionInitializationCannotBlockIncumbentRetirement(): Unit = withEnabled() {
    (spark, policy, _) =>
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      val original = observe(spark).get(0)
      // Do not touch sharedState before the observer reaches its real lazy initializer.
      val foreign = classOf[SparkSession]
        .getConstructor(classOf[SparkContext])
        .newInstance(spark.sparkContext)
      val observerThread = new AtomicReference[Thread]()
      val started = new CountDownLatch(1)
      val retired = new CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(2)
      try {
        val observing = foreign.synchronized {
          val result = executor.submit(new Callable[Boolean] {
            override def call(): Boolean = {
              observerThread.set(Thread.currentThread())
              started.countDown()
              denied(observe(foreign))
              true
            }
          })
          assertTrue(started.await(10, TimeUnit.SECONDS))
          val threads = ManagementFactory.getThreadMXBean
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
          var blockedOnColdInitializer = false
          while (!blockedOnColdInitializer && System.nanoTime() < deadline) {
            val info = threads.getThreadInfo(observerThread.get().getId, 32)
            blockedOnColdInitializer = info != null &&
              info.getThreadState == Thread.State.BLOCKED &&
              info.getLockOwnerId == Thread.currentThread().getId &&
              info.getLockInfo != null &&
              info.getLockInfo.getIdentityHashCode == System.identityHashCode(foreign) &&
              info.getStackTrace.exists(frame =>
                frame.getClassName == classOf[SparkSession].getName &&
                  frame.getMethodName == "sharedState$lzycompute"
              )
            if (!blockedOnColdInitializer) Thread.`yield`()
          }
          assertTrue(
            blockedOnColdInitializer,
            "Observer must reach the actual cold-session initializer"
          )
          executor.submit(new Runnable {
            override def run(): Unit = { fixture.close(); retired.countDown() }
          })
          assertTrue(
            retired.await(5, TimeUnit.SECONDS),
            "Cold session initialization must release the registry monitor before incumbent retirement"
          )
          result
        }
        assertTrue(observing.get(10, TimeUnit.SECONDS))
        denied(original.requireCurrent(spark))
        denied(observe(spark))
      } finally {
        executor.shutdown()
        try
          assertTrue(
            executor.awaitTermination(10, TimeUnit.SECONDS),
            "Owned initializer/retirement workers must terminate"
          )
        finally { executor.shutdownNow(); fixture.close() }
      }
  }

  @Test def sqlSettingsAndOptionsCannotEnableMissingStartupGrant(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val path = config("a")
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set(setting, path.toString)
          spark.conf.set("spark.sql.catalog.a.rawCacheMaintenance.config", path.toString)
          val fixture = new NativeBoundCatalogFixture(spark, policy)
          try { assertEquals(0, fixture.reads.get()); denied(observe(spark)) }
          finally fixture.close()
        },
        Some(NativeSelectFixture.extension(policy))
      )
    finally policy.engine.close()
  }

  @Test def immutableScopeDuplicateOwnersAndSessionBoundObservations(): Unit =
    withEnabled(Seq("a", "b")) { (spark, policy, path) =>
      val a = new NativeBoundCatalogFixture(spark, policy)
      val bPolicy = new TestDeltaRangerNativeSelect.Fixture
      val duplicatePolicy = new TestDeltaRangerNativeSelect.Fixture
      try {
        val original = observe(spark).get(0)
        val clone = spark.newSession()
        assertSame(spark.sharedState, clone.sharedState)
        denied(original.requireCurrent(clone))
        val cloned = observe(clone).get(0)
        cloned.requireCurrent(clone)
        denied(cloned.requireCurrent(spark))
        // A later owner must use the frozen snapshot, even when the deployment file becomes invalid.
        Files.write(path, "malformed=private-value".getBytes(StandardCharsets.UTF_8))
        val b = new NativeBoundCatalogFixture(spark, bPolicy, "b")
        try {
          denied(original.requireCurrent(spark))
          assertThrows(
            classOf[SecurityException],
            () => new NativeBoundCatalogFixture(spark, duplicatePolicy)
          )
          val all = observe(spark)
          assertEquals(Set("a", "b"), all.asScala.map(_.publication().catalog()).toSet)
          // Distinct canonical tuples may share storage; these remain two metadata candidates.
          b.properties.put(prefix + "location", a.properties.get(prefix + "location"))
          // A storage change cannot be adopted by the existing owner.
          denied(observe(spark))
          b.properties.put(prefix + "location", NativeSelectFixture.locations("b"))
          assertEquals(2, observe(spark).size())
          a.close()
          assertEquals("b", observe(spark).get(0).publication().catalog())
        } finally b.close()
        denied(observe(spark))
        // Last close releases the snapshot, so malformed startup configuration is now rejected.
        val freshPolicy = new TestDeltaRangerNativeSelect.Fixture
        try
          assertThrows(
            classOf[IllegalArgumentException],
            () => new NativeBoundCatalogFixture(spark, freshPolicy)
          )
        finally freshPolicy.engine.close()
      } finally { a.close(); bPolicy.engine.close(); duplicatePolicy.engine.close() }
    }

  @Test def metadataAndPolicyPublicationsMustStayCurrent(): Unit = withEnabled() {
    (spark, policy, _) =>
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      try {
        val original = observe(spark).get(0)
        Seq(
          "id" -> "2",
          "generation" -> "2",
          "token" -> "new-token",
          "nativeId" -> "new-id",
          "location" -> "file:/new-location",
          "runId" -> "new-run"
        ).foreach { case (field, value) =>
          val key = prefix + field
          val previous = fixture.properties.put(key, value)
          denied(original.requireCurrent(spark))
          if (field != "runId") denied(observe(spark))
          if (previous == null) fixture.properties.remove(key)
          else fixture.properties.put(key, previous)
        }
        val schema = fixture.columns
        fixture.columns = schema.reverse
        denied(original.requireCurrent(spark)); denied(observe(spark))
        fixture.columns = schema
        val nativeVersion = fixture.properties.get(prefix + "nativeVersion")
        fixture.properties.put(prefix + "nativeVersion", (nativeVersion.toLong + 1).toString)
        denied(original.requireCurrent(spark))
        val newer = observe(spark).get(0)
        newer.requireCurrent(spark)
        fixture.properties.put(prefix + "nativeVersion", nativeVersion)
        denied(newer.requireCurrent(spark))
        policy.start() // Real publication listener receives a new auth-context object at equal versions.
        denied(original.requireCurrent(spark))
        val ready = observe(spark).get(0)
        ready.requireCurrent(spark)
        val next = fixture.reads.get() + 2
        fixture.onRead =
          n => if (n == next) fixture.properties.put(prefix + "token", "mid-observation")
        denied(observe(spark))
        fixture.onRead = _ => ()
        fixture.properties.put(prefix + "token", "private-wrapper-token")
        val live = observe(spark).get(0)
        fixture.clock.set(TimeUnit.MINUTES.toNanos(6))
        denied(live.requireCurrent(spark)); denied(observe(spark))
      } finally fixture.close()
  }

  @Test def closeDuringReadDoesNotHoldRegistryMonitorOrReturnEvidence(): Unit = withEnabled() {
    (spark, policy, _) =>
      val fixture = new NativeBoundCatalogFixture(spark, policy)
      val entered = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(2)
      try {
        val original = observe(spark).get(0)
        fixture.onRead = _ => {
          entered.countDown(); assertTrue(release.await(10, TimeUnit.SECONDS))
        }
        val observing = executor.submit(new java.util.concurrent.Callable[Boolean] {
          override def call(): Boolean = { denied(observe(spark)); true }
        })
        assertTrue(entered.await(10, TimeUnit.SECONDS))
        val closing = executor.submit(new Runnable { override def run(): Unit = fixture.close() })
        closing.get(10, TimeUnit.SECONDS)
        release.countDown()
        assertTrue(observing.get(10, TimeUnit.SECONDS))
        denied(original.requireCurrent(spark)); denied(observe(spark))
      } finally { release.countDown(); fixture.close(); executor.shutdownNow() }
  }

  @Test def startupFailuresDoNotPublishPartialOwnersAndApplicationEndRetiresOwners(): Unit =
    withEnabled(Seq("a", "b")) { (spark, policy, _) =>
      val incumbent = new NativeBoundCatalogFixture(spark, policy, "b")
      try {
        val good = new LinkedHashMap[String, String](incumbent.properties)
        Seq("missing", "malformed", "unpublished", "changing").foreach { mode =>
          val ownPolicy = new TestDeltaRangerNativeSelect.Fixture
          val properties = new LinkedHashMap[String, String](good)
          if (mode == "missing") properties.clear()
          if (mode == "malformed") properties.put(prefix + "generation", "secret-invalid")
          if (mode == "unpublished") properties.put(prefix + "state", "RESERVED")
          try {
            assertThrows(
              classOf[SecurityException],
              () =>
                initializeFromPublication(
                  spark,
                  ownPolicy,
                  incumbent,
                  properties,
                  n => if (mode == "changing" && n == 2) properties.put(prefix + "token", "changed")
                )
            )
            assertEquals("b", observe(spark).get(0).publication().catalog())
          } finally ownPolicy.engine.close()
        }
        val ownPolicy = new TestDeltaRangerNativeSelect.Fixture
        val a = initializeFromPublication(spark, ownPolicy, incumbent, good, _ => ())
        try {
          val candidates = observe(spark)
          assertEquals(
            2,
            candidates.size(),
            "Aliased storage returns candidates, never a unique raw binding"
          )
          assertEquals(1, candidates.asScala.map(_.publication().location()).toSet.size)
          assertNull(DeltaRangerNativeOperation.current())
          // Invoke the actual installed callback without sending a synthetic end event to Spark.
          val shutdown = classOf[DeltaRangerCatalogSpark35].getDeclaredField("shutdown")
          shutdown.setAccessible(true)
          shutdown
            .get(a)
            .asInstanceOf[SparkListener]
            .onApplicationEnd(new SparkListenerApplicationEnd(System.currentTimeMillis()))
          incumbent.close()
          denied(observe(spark))
        } finally { a.close(); ownPolicy.engine.close() }
      } finally incumbent.close()
    }

  @Test def installationCannotReviveTheSnapshotOfARetiredScope(): Unit =
    withEnabled(Seq("a", "b")) { (spark, policy, _) =>
      val incumbent = new NativeBoundCatalogFixture(spark, policy, "b")
      val old = observe(spark).get(0)
      val ownPolicy = new TestDeltaRangerNativeSelect.Fixture
      var installed: DeltaRangerCatalogSpark35 = null
      try {
        val properties = new LinkedHashMap[String, String](incumbent.properties)
        assertThrows(
          classOf[SecurityException],
          () => {
            installed = initializeFromPublication(
              spark,
              ownPolicy,
              incumbent,
              properties,
              n => if (n == 2) incumbent.close()
            )
          }
        )
        denied(observe(spark))
        val replacementPolicy = new TestDeltaRangerNativeSelect.Fixture
        val replacement = new NativeBoundCatalogFixture(spark, replacementPolicy, "b")
        try {
          observe(spark).get(0).requireCurrent(spark)
          denied(old.requireCurrent(spark))
        } finally { replacement.close(); replacementPolicy.engine.close() }
      } finally {
        if (installed != null) installed.close()
        incumbent.close()
        ownPolicy.engine.close()
      }
    }

  // The normal shared fixture cannot mutate before initialize. This small transport fixture reuses
  // its native-seeded receipt/schema while retaining actual client/reader/wrapper/engine behavior.
  private def initializeFromPublication(
      spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      seed: NativeBoundCatalogFixture,
      properties: LinkedHashMap[String, String],
      hook: Int => Unit
  ): DeltaRangerCatalogSpark35 = {
    val client = mock(classOf[GravitinoClient])
    val catalog = mock(classOf[Catalog])
    val tables = mock(classOf[TableCatalog])
    val registration = mock(classOf[Table])
    val reads = new AtomicInteger()
    when(client.loadCatalog("a")).thenReturn(catalog)
    when(catalog.name()).thenReturn("a")
    when(catalog.provider()).thenReturn("lakehouse-delta")
    when(catalog.asTableCatalog()).thenReturn(tables)
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenAnswer(_ => {
      hook(reads.incrementAndGet()); registration
    })
    when(registration.name()).thenReturn("t")
    when(registration.properties()).thenAnswer(_ => properties)
    when(registration.columns()).thenAnswer(_ => seed.columns)
    val metadata =
      new DeltaRangerMetadataClient(() => 0L, mock(classOf[ScheduledExecutorService]), _ => client)
    val wrapper = new DeltaRangerCatalogSpark35(
      new DeltaRangerCatalogSpark35.State(
        metadata,
        () => throw new AssertionError("No actor read"),
        () => catalog,
        _ => policy.engine
      )
    )
    wrapper.initialize(
      "a",
      new CaseInsensitiveStringMap(
        Collections.singletonMap(
          "journal-directory",
          Files.createTempDirectory("raw-journal").toString
        )
      )
    )
    wrapper
  }
}
