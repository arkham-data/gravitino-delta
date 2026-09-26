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

import java.security.PrivilegedExceptionAction
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.when

class TestDeltaRangerInstalledStaleness {
  @Test
  def firstAndRepeatedCollectionsRejectDecisionRoleAndPolicyChanges(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      Seq(false, true).foreach { reused =>
        Seq("mask", "filter", "role", "policy", "publication").foreach { change =>
          val (frame, root) = retained(spark, reused)
          val mask = policy.mask
          val filter = policy.filter
          val stamp = policy.engine.snapshotStamp()
          val expected = change match {
            case "mask"   => policy.mask = "upper({col})"; "action decision changed"
            case "filter" => policy.filter = "region = 'drop'"; "action decision changed"
            case "role" =>
              val context = policy.plugin.getCurrentRangerAuthContext
              val roles = policy.plugin.getRoles
              val version = policy.plugin.getRolesVersion + 1L
              roles.setRoleVersion(version)
              when(policy.plugin.getRolesVersion).thenReturn(version)
              context.setRoles(roles)
              policy.listener.contextChanged()
              "publication changed"
            case "policy" =>
              val version = policy.plugin.getPoliciesVersion + 1L
              when(policy.plugin.getPoliciesVersion).thenReturn(version)
              policy.start(); "publication changed"
            case "publication" => policy.start(); "publication changed"
          }
          try {
            rejected(frame, root, expected)
            if (Set("mask", "filter")(change)) policy.engine.requireSnapshot(stamp)
          } finally { policy.mask = mask; policy.filter = filter }
        }
      }
    }

  @Test
  def firstAndRepeatedCollectionsRejectEveryRegistrationReplacementAndMissingMetadata(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val properties = fixture.properties.asScala.toMap
      Seq(false, true).foreach { reused =>
        Seq(
          "id" -> "2",
          "generation" -> "2",
          "token" -> "replacement",
          "location" -> "/tmp/foreign-delta",
          "nativeId" -> "foreign-native",
          "state" -> "ADOPTING",
          "nativeVersion" -> "999"
        ).foreach { case (key, value) =>
          val (frame, root) = retained(spark, reused)
          fixture.properties.put("delta.registration.receipt." + key, value)
          try rejected(frame, root, "retained registration")
          finally { fixture.properties.clear(); fixture.properties.putAll(properties.asJava) }
        }
        val (missing, missingRoot) = retained(spark, reused)
        fixture.onRead = _ => throw new IllegalStateException("missing table")
        try rejected(missing, missingRoot, "retained registration")
        finally fixture.onRead = _ => ()
        val (schema, schemaRoot) = retained(spark, reused)
        val columns = fixture.columns
        fixture.columns = columns.dropRight(1)
        try rejected(schema, schemaRoot, "retained registration")
        finally fixture.columns = columns
      }
    }

  @Test
  def firstAndRepeatedCollectionsRejectUserGroupsConfigurationAndEnabledAqe(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      Seq(false, true).foreach { reused =>
        val (frame, root) = retained(spark, reused)
        val original = DeltaRangerQuerySubject.current(spark)
        val user = original.user()
        Seq(user, "different-user").foreach { name =>
          try
            UserGroupInformation
              .createUserForTesting(name, Array("installed-changed-group-" + reused))
              .doAs(new PrivilegedExceptionAction[Unit] {
                override def run(): Unit = {
                  val current = DeltaRangerQuerySubject.current(spark)
                  assertTrue(
                    current.user() != original.user() || current.groups() != original.groups(),
                    "Fixture must actually change subject"
                  )
                  rejected(frame, root, "subject changed"); ()
                }
              })
          finally UserGroupInformation.createUserForTesting(user, original.groups().asScala.toArray)
        }
        val zone = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
        try rejected(frame, root, "configuration changed")
        finally spark.conf.set("spark.sql.session.timeZone", zone)
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        try {
          rejected(frame, root, "unsupported user aqe")
          assertEquals("true", spark.conf.get("spark.sql.adaptive.enabled"))
        } finally spark.conf.set("spark.sql.adaptive.enabled", "false")
      }
    }

  @Test
  def catalogMetadataExpiryAndEngineClosureRejectBeforeOrDuringRetainedCollection(): Unit = {
    for (
      mode <- Seq("catalog", "metadata", "expiry", "engine"); during <- Seq(false, true);
      reused <- Seq(false, true)
    ) {
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        val (frame, root) = retained(spark, reused)
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        def close(): Unit = mode match {
          case "catalog"  => fixture.wrapper.close()
          case "metadata" => fixture.metadata.close()
          case "expiry"   => fixture.clock.set(TimeUnit.SECONDS.toNanos(301))
          case "engine"   => policy.engine.close()
        }
        val lastRead = fixture.reads.get() + 4
        if (during) {
          fixture.onRead = count => if (count == lastRead) close()
        } else close()
        rejected(
          frame,
          root,
          if (!during && Set("catalog", "engine")(mode)) "lease is closed"
          else "retained registration"
        )
        if (during) assertEquals(lastRead, fixture.reads.get())
        assertNull(DeltaRangerNativeOperation.current())
      }
    }
  }

  @Test
  def twoCatalogBroadcastRevalidatesEitherOriginalOwnerBeforeFirstAndRepeatedCollection(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, first) =>
      val second = new TestDeltaRangerNativeSelect.Fixture
      second.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("b" -> second)
      try {
        for (
          reused <- Seq(false, true); catalog <- Seq("a", "b");
          mode <- Seq("access", "mask", "publication", "binding")
        ) {
          val policy = if (catalog == "a") first else second
          val sql =
            "SELECT a.secret, b.secret FROM a.db.t a JOIN b.db.t b ON a.id=b.id WHERE a.id=1"
          val rows = Vector(Row("a-one-masked", "b-one-masked"))
          val (frame, root) = retained(spark, reused, sql, rows)
          val prepared = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan)
          assertEquals(Set("a", "b"), prepared.owner.obligations.map(_.scan.catalog).toSet)
          assertEquals(2, prepared.owner.obligations.size)
          assertTrue(root.exists(_.isInstanceOf[BroadcastHashJoinExec]), root.treeString)
          assertTrue(root.exists(_.isInstanceOf[BroadcastExchangeExec]), root.treeString)
          val fixture = NativeSelectFixture.bindingFixture(spark, catalog)
          val token = fixture.properties.get("delta.registration.receipt.token")
          val mask = policy.mask
          val expected = mode match {
            case "access"      => policy.denied.add(catalog + ":secret"); "access denied"
            case "mask"        => policy.mask = "upper({col})"; "action decision changed"
            case "publication" => policy.start(); "publication changed"
            case "binding" =>
              fixture.properties.put("delta.registration.receipt.token", "replacement");
              "retained registration"
          }
          first.events.clear(); second.events.clear()
          try {
            rejected(frame, root, expected)
            if (mode == "access")
              assertTrue(
                (first.events.asScala ++ second.events.asScala).forall(_.startsWith("access:"))
              )
          } finally {
            policy.denied.clear(); policy.mask = mask
            fixture.properties.put("delta.registration.receipt.token", token)
          }
        }
      } finally second.engine.close()
    }

  @Test
  def optimizedAwayScanRetainsOriginalAccessAndRegistrationBeforeFirstAndRepeatedCollection()
      : Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      for (reused <- Seq(false, true); mode <- Seq("access", "binding")) {
        val (frame, root) =
          retained(spark, reused, "SELECT secret FROM a.db.t WHERE false", Vector.empty)
        assertTrue(
          DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).occurrences.isEmpty
        )
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        val token = fixture.properties.get("delta.registration.receipt.token")
        if (mode == "access") policy.denied.add("a:secret")
        else fixture.properties.put("delta.registration.receipt.token", "replacement")
        try
          rejected(frame, root, if (mode == "access") "access denied" else "retained registration")
        finally {
          policy.denied.clear(); fixture.properties.put("delta.registration.receipt.token", token)
        }
      }
    }

  @Test
  def absentFilterAndNoneMaskRemainTimeDependentRetainedObligations(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      for (reused <- Seq(false, true); mode <- Seq("filter", "mask")) {
        policy.filter = null
        policy.maskColumn = "unused"
        val (frame, root) = retained(spark, reused, rows = Vector(Row("a-one")))
        val stamp = policy.engine.snapshotStamp()
        policy.onRequest = _ => {
          if (mode == "filter") policy.filter = "id=1L"
          else { policy.maskColumn = "secret"; policy.maskType = "MASK_NULL" }
        }
        policy.requests.clear()
        try {
          rejected(frame, root, "action decision changed")
          policy.engine.requireSnapshot(stamp)
          assertEquals(1, policy.requests.asScala.map(_.getAccessTime.toInstant).distinct.size)
        } finally policy.onRequest = _ => ()
      }
    }

  @Test
  def finalMetadataCallbacksCannotChangeEngineSubjectConfigurationOrOriginalRecipe(): Unit = {
    for (
      reused <- Seq(false, true); change <- Seq("engine", "subject", "configuration", "structure")
    ) {
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        val (frame, root) =
          retained(spark, reused, "SELECT secret FROM a.db.t WHERE region='keep' AND id=1")
        val analyzed = frame.queryExecution.analyzed
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        val lastRead = fixture.reads.get() + 4
        fixture.onRead = count =>
          if (count == lastRead) change match {
            case "engine" => policy.start()
            case "subject" =>
              spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
            case "configuration" => spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
            case "structure" =>
              val literal = analyzed
                .flatMap(_.expressions.flatMap(_.collect {
                  case literal: Literal
                      if literal.dataType == StringType && literal.value.toString == "keep" =>
                    literal
                }))
                .head
              literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]](0) =
                'z'.toByte
          }
        val expected = Map(
          "engine" -> "publication changed",
          "subject" -> "identity contradicts",
          "configuration" -> "configuration changed",
          "structure" -> "retained logical binding changed"
        )
        try rejected(frame, root, expected(change))
        finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
        assertEquals(lastRead, fixture.reads.get())
      }
    }
  }

  @Test
  def retainedSnapshotAllowsCompatibleAdvanceButRejectsSchemaAndObservedNativeDrift(): Unit = {
    for (reused <- Seq(false, true); mode <- Seq("compatible", "schema", "policy", "metadata")) {
      withHistory { (spark, policy, publish) =>
        val (frame, root) = retained(spark, reused)
        val prepared = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan)
        val bound = prepared.owner.obligations.head.binding
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        mode match {
          case "compatible" =>
            publish(1L)
            fixture.properties.put("delta.registration.receipt.nativeVersion", "1")
            assertEquals(Vector(Row("a-one-masked")), frame.collect().toVector)
            assertEquals(Vector(Row("a-one-masked")), frame.collectAsList().asScala.toVector)
            assertSame(root, frame.queryExecution.executedPlan)
            assertEquals(0L, bound.index().snapshotAtAnalysis.version)
            assertEquals(1L, bound.log().unsafeVolatileSnapshot.version)
          case "schema" =>
            publish(1L); publish(2L)
            rejected(frame, root, "retained registration")
          case "policy" =>
            policy.onRequest = _ => publish(1L)
            rejected(frame, root, "action observation changed")
          case "metadata" =>
            val afterRead = fixture.reads.get() + 2
            fixture.onRead = count => if (count == afterRead) publish(1L)
            rejected(frame, root, "retained registration")
        }
        assertNull(DeltaRangerNativeOperation.current())
      }
    }
  }

  private def withHistory(
      test: (SparkSession, TestDeltaRangerNativeSelect.Fixture, Long => Unit) => Unit
  ): Unit = {
    val commits = scala.collection.mutable.Map.empty[Long, (Path, Array[Byte])]
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark =>
          test(
            spark,
            policy,
            version => { val (path, bytes) = commits(version); Files.write(path, bytes); () }
          ),
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark => {
          val location = NativeSelectFixture.locations("a")
          spark
            .sql("SELECT 4L id, 'tail' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
          spark
            .sql("SELECT 'changed' id, 'new' secret, 'keep' region")
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(location)
          (1L to 2L).foreach { version =>
            val path = Paths.get(location, "_delta_log", f"$version%020d.json")
            commits(version) = (path, Files.readAllBytes(path)); Files.delete(path)
          }
        }
      )
    finally policy.engine.close()
  }

  private def retained(
      spark: SparkSession,
      reused: Boolean,
      sql: String = "SELECT secret FROM a.db.t WHERE id=1",
      rows: Vector[Row] = Vector(Row("a-one-masked"))
  ): (DataFrame, DeltaRangerCollectGuardExec) = {
    val frame = spark.sql(sql)
    val root = frame.queryExecution.executedPlan
    assertTrue(root.isInstanceOf[DeltaRangerCollectGuardExec], root.treeString)
    assertEquals(1, root.collect { case guard: DeltaRangerCollectGuardExec => guard }.size)
    if (reused) assertEquals(rows, frame.collect().toVector)
    (frame, root.asInstanceOf[DeltaRangerCollectGuardExec])
  }

  private def rejected(
      frame: DataFrame,
      root: DeltaRangerCollectGuardExec,
      expected: String
  ): RuntimeException = {
    val failure = assertThrows(
      classOf[RuntimeException],
      () => { frame.collectAsList(); () },
      "Expected installed denial: " + expected
    )
    assertTrue(
      failure.getMessage.toLowerCase(java.util.Locale.ROOT).contains(expected),
      failure.getMessage
    )
    assertTrue(
      failure.getStackTrace.exists(frame =>
        frame.getClassName.endsWith(
          "DeltaRangerCollectGuardExec"
        ) && frame.getMethodName == "executeCollect"
      ),
      "Failure must originate at installed collection entry"
    )
    assertFalse(
      failure.getStackTrace.exists(frame =>
        frame.getClassName.endsWith(
          "DeltaRangerCollectGuardExec"
        ) && frame.getMethodName == "doExecute"
      ),
      "Failure must precede defense-in-depth execution"
    )
    assertSame(root, frame.queryExecution.executedPlan)
    assertTrue(
      DeltaRangerPhysicalProvenance
        .nodes(root)
        .collect { case scan: FileSourceScanExec => scan }
        .forall(_.metrics("numOutputRows").value == 0L)
    )
    failure
  }
}
