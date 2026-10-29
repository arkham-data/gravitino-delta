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
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import io.delta.sql.DeltaSparkSessionExtension

class TestDeltaRangerTrustedAqeAppend {
  @Test
  def targetReadPoliciesDoNotConstrainAppend(): Unit =
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "targetReadPoliciesDoNotConstrainAppend",
      adaptive = true,
      restrictiveTargetReads = true,
      scenario = "aqe-target-read-policies",
      sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id"
    )

  @Test
  def broadcastPayloadUsesAppendContext(): Unit =
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "broadcastPayloadUsesAppendContext",
      adaptive = true,
      adaptiveBroadcast = true,
      scenario = "aqe-broadcast-conversion",
      sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id"
    )

  @Test
  def undeployedWarmTargetRejectsBeforeMutation(): Unit = withFixture("aqe-warm-preflight") {
    (spark, _, _, trace) =>
      val extensions = new SparkSessionExtensions
      new DeltaSparkSessionExtension().apply(extensions)
      val constructor =
        classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
      constructor.setAccessible(true)
      val native = constructor
        .newInstance(
          spark.sparkContext,
          Some(spark.sharedState),
          None,
          extensions,
          Map.empty[String, String]
        )
        .asInstanceOf[SparkSession]
      SparkSession.setActiveSession(native)
      val frame =
        try {
          val data = native.read.format("delta").load(NativeSelectFixture.locations("a")).cache()
          assertEquals(3, data.collect().length)
          data
        } finally SparkSession.setActiveSession(spark)
      val cached = spark.sharedState.cacheManager.lookupCachedData(frame).get
      val root = Paths.get(NativeSelectFixture.locations("a")).getParent
      val before = InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
      val failure = assertThrows(
        classOf[Exception],
        () =>
          spark
            .sql("SELECT id, secret, region FROM b.db.t")
            .write
            .mode("append")
            .insertInto("a.db.t")
      )
      trace.record(
        "negative-causes",
        Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).mkString(" | ")
      )
      InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("diagnostic-after"))
      assertTrue(
        Iterator
          .iterate[Throwable](failure)(_.getCause)
          .takeWhile(_ != null)
          .exists(error =>
            Option(error.getMessage).exists(message =>
              message.contains("Raw maintenance") || message
                .contains("Raw native observation rejected")
            )
          )
      )
      assertEquals(
        before,
        InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after"))
      )
      assertTrue(cached.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      assertSame(cached, spark.sharedState.cacheManager.lookupCachedData(frame).get)
  }

  @Test
  def coldCheckpointReopens(): Unit =
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "coldCheckpointReopens",
      adaptive = true,
      seedCommits = 9,
      partitionedTarget = true,
      scenario = "aqe-cold-checkpoint"
    )

  @Test
  def existingCdfEnabledAndDisabledTargetsRemainWritable(): Unit = {
    for (enabled <- Seq(true, false)) {
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "existingCdfEnabledAndDisabledTargetsRemainWritable",
        adaptive = true,
        cdfTarget = Some(enabled),
        frontend = if (enabled) "insertInto" else "saveAsTable",
        scenario = "aqe-cdf-target-" + enabled
      )
    }
  }

  @Test
  def capturedWriteThenPrecommitRevoke(): Unit =
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "capturedWriteThenPrecommitRevoke",
      adaptive = true,
      revokeAfterHelper = true,
      scenario = "aqe-precommit-revoke",
      sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id",
      failureCheck = Some(error => assertDenied(error))
    )

  @Test
  def emptyAndConstraintOutcomes(): Unit = {
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "emptyAndConstraintOutcomes",
      adaptive = true,
      emptySource = true,
      scenario = "aqe-empty"
    )
    new TestDeltaRangerNativeGovernedAppend().violatedAppend(adaptive = true)
  }

  @Test
  def failedHelperCannotAuthorizeNextWrite(): Unit = {
    var failed: DeltaRangerAppendHelper = null
    new TestDeltaRangerNativeGovernedAppend().violatedAppend(
      adaptive = true,
      check = (helper, _) => failed = helper,
      afterFailure = spark => {
        assertNotNull(failed)
        assertThrows(classOf[SecurityException], () => failed.requireLive())
        assertThrows(classOf[SecurityException], () => failed.write())
        spark
          .sql("SELECT id, CAST(NULL AS STRING) secret, region FROM b.db.t")
          .write
          .mode("append")
          .insertInto("a.db.t")
        assertThrows(classOf[SecurityException], () => failed.requireLive())
      }
    )
  }

  @Test
  def knownCommitDoesNotReplay(): Unit = {
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "knownCommitDoesNotReplay",
      adaptive = true,
      repeatCompletedResult = true,
      scenario = "aqe-known-commit"
    )
    val primary = new AssertionError("aqe-postpublication-observer-failure")
    var committed: DeltaRangerAppendCommit = null
    new TestDeltaRangerNativeGovernedAppend().successfulAppend(
      testMethod = "knownCommitDoesNotReplay",
      adaptive = true,
      scenario = "aqe-postpublication-failure",
      failureCheck = Some(error =>
        assertTrue(
          Iterator.iterate[Throwable](error)(_.getCause).takeWhile(_ != null).exists(_ eq primary)
        )
      ),
      inspect = (owner, _) => { committed = owner; throw primary }
    )
    assertNotNull(committed)
    assertThrows(classOf[SecurityException], () => committed.run())
  }

  @Test
  def duplicateSkipsSourceButRechecks(): Unit = withFixture("aqe-duplicate") {
    (spark, target, source, trace) =>
      spark.conf.set("spark.databricks.delta.write.txnAppId", "trusted-aqe-append")
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      var helperCalls = 0
      source.onRequest = _ => {
        if (
          Thread
            .currentThread()
            .getStackTrace
            .exists(_.getClassName.endsWith("DeltaRangerAppendHelper"))
        )
          helperCalls += 1
      }
      def write(): Unit =
        spark.sql("SELECT id, secret, region FROM b.db.t").write.mode("append").insertInto("a.db.t")
      write()
      assertTrue(helperCalls > 0)
      val firstCalls = helperCalls
      val root = Paths.get(NativeSelectFixture.locations("a")).getParent
      val before = InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("first"))
      val requests = source.requests.size()
      write()
      assertEquals(firstCalls, helperCalls)
      assertTrue(source.requests.size() > requests)
      assertEquals(
        before,
        InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("duplicate"))
      )
      for (access <- Seq("select", "update")) {
        val policy = if (access == "select") source else target
        policy.denied.add(if (access == "select") "b:secret" else "a:*")
        try assertDenied(assertThrows(classOf[Exception], () => write()))
        finally policy.denied.clear()
        assertEquals(firstCalls, helperCalls)
        assertEquals(
          before,
          InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("denied-" + access))
        )
      }
  }

  @Test
  def changedSourceTargetContextRejects(): Unit = withFixture("aqe-context") {
    (spark, target, source, trace) =>
      val root = Paths.get(NativeSelectFixture.locations("a")).getParent
      val before = InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
      for (change <- Seq("source", "target", "subject", "settings")) {
        val data = spark.sql("SELECT id, secret, region FROM b.db.t")
        val registration =
          NativeSelectFixture.bindingFixture(spark, if (change == "source") "b" else "a")
        val properties = new java.util.LinkedHashMap[String, String](registration.properties)
        val partitions = spark.conf.get("spark.sql.shuffle.partitions")
        val subject = spark.sparkContext.getLocalProperty("kyuubi.session.user")
        var fired = false
        source.onRequest = _ => {
          if (
            !fired && Thread
              .currentThread()
              .getStackTrace
              .exists(frame =>
                frame.getClassName.endsWith(
                  "DeltaRangerAppendV1Input$TerminalRelation"
                ) && frame.getMethodName == "insert"
              )
          ) {
            fired = true
            change match {
              case "source" | "target" =>
                registration.properties.put("delta.registration.receipt.generation", "2")
              case "subject" =>
                spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-subject")
              case "settings" => spark.conf.set("spark.sql.shuffle.partitions", "19")
            }
          }
        }
        try {
          assertThrows(classOf[Exception], () => data.write.mode("append").insertInto("a.db.t"))
          assertTrue(fired)
          assertEquals(
            before,
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("after-" + change))
          )
        } finally {
          source.onRequest = _ => ()
          registration.properties.clear()
          registration.properties.putAll(properties)
          spark.conf.set("spark.sql.shuffle.partitions", partitions)
          spark.sparkContext.setLocalProperty("kyuubi.session.user", subject)
        }
      }
  }

  @Test
  def sourceAndTargetDeniedBeforeWrite(): Unit = {
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val trace = InstalledAppendBoundaryProbe.fresh("aqe-entry-denials")
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          val before =
            InstalledAppendBoundaryProbe.manifest(root, trace.directory.resolve("before"))
          for (frontend <- Seq("insertInto", "saveAsTable"); access <- Seq("select", "update")) {
            val data = spark.sql("SELECT id, secret, region FROM b.db.t")
            val policy = if (access == "select") source else target
            policy.requests.clear()
            policy.denied.add(if (access == "select") "b:secret" else "a:*")
            try {
              val failure = assertThrows(
                classOf[Exception],
                () => {
                  if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
                  else data.write.mode("append").saveAsTable("a.db.t")
                }
              )
              assertTrue(
                Iterator
                  .iterate[Throwable](failure)(_.getCause)
                  .takeWhile(_ != null)
                  .exists(error =>
                    Option(error.getMessage).exists(_.toLowerCase.contains("denied"))
                  )
              )
              assertEquals(
                before,
                InstalledAppendBoundaryProbe.manifest(
                  root,
                  trace.directory.resolve(s"after-$frontend-$access")
                )
              )
            } finally policy.denied.clear()
          }
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally {
      trace.close()
      target.engine.close()
      source.engine.close()
    }
  }

  @Test
  def namedFrontendsUseAdaptivePayload(): Unit = {
    for (frontend <- Seq("insertInto", "saveAsTable"); partitioned <- Seq(false, true)) {
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "namedFrontendsUseAdaptivePayload",
        frontend = frontend,
        adaptive = true,
        partitionedTarget = partitioned,
        scenario = s"aqe-$frontend-partitioned-$partitioned",
        sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id"
      )
    }
  }

  @Test
  def defaultAndOriginalQueryBranches(): Unit = {
    for (frontend <- Seq("insertInto", "saveAsTable"); defaults <- Seq(false)) {
      new TestDeltaRangerNativeGovernedAppend().successfulAppend(
        testMethod = "defaultAndOriginalQueryBranches",
        frontend = frontend,
        nullDefault = defaults,
        adaptive = true,
        scenario = s"aqe-$frontend-defaults-$defaults",
        sourceSql = "SELECT l.id, l.secret, l.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id"
      )
    }
  }
  private def assertDenied(error: Throwable): Unit =
    assertTrue(
      Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .exists(cause => Option(cause.getMessage).exists(_.toLowerCase.contains("denied")))
    )

  private def withFixture(name: String)(
      body: (
          SparkSession,
          TestDeltaRangerNativeSelect.Fixture,
          TestDeltaRangerNativeSelect.Fixture,
          InstalledAppendBoundaryProbe.Trace
      ) => Unit
  ): Unit = {
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val trace = InstalledAppendBoundaryProbe.fresh(name)
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          body(spark, target, source, trace)
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.sql.adaptive.enabled" -> "true",
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        )
      )
    finally {
      trace.close()
      target.engine.close()
      source.engine.close()
    }
  }
}
