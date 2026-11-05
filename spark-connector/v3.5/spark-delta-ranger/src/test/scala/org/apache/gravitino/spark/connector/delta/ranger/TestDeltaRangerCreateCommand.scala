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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Files
import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ranger.plugin.model.RangerPolicy
import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.apache.ranger.plugin.service.RangerBasePlugin
import org.apache.ranger.plugin.util.ServicePolicies
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.storage.HDFSLogStore
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, TableSpec}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{LongType, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock}

/** Real issued-command execution, with explicitly scripted registration and phase fault transport.
  */
class TestDeltaRangerCreateCommand {
  import InstalledSchemaCreateBoundaryProbe._
  private val fixture = new TestDeltaRangerInstalledSchemaCreateBoundary
  private val id = Identifier.of(Array("db"), "created")
  private def schema = new StructType().add("id", LongType, nullable = true)
  private def statement(columns: String = "id BIGINT", suffix: String = ""): String = {
    val properties = fixtureMap("tableProperties").toSeq.sorted
      .map { case (key, value) =>
        s"'$key'='$value'"
      }
      .mkString(",")
    s"CREATE TABLE a.db.created ($columns) USING delta TBLPROPERTIES ($properties) $suffix"
  }
  private def issued(spark: SparkSession): DeltaRangerCreateCommand.Carrier =
    spark.sessionState.analyzer
      .execute(spark.sessionState.sqlParser.parsePlan(statement()))
      .asInstanceOf[DeltaRangerCreateCommand.Carrier]

  @Test
  def rejectsUnissuedAndForeignCreate(): Unit =
    fixture.withCase("command-forgery", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val properties = fixtureMap("tableProperties").asJava
        Seq[() => Any](
          () => wrapper.createTable(id, schema, Array.empty[Transform], properties),
          () => wrapper.stageCreate(id, schema, Array.empty[Transform], properties),
          () => wrapper.stageReplace(id, schema, Array.empty[Transform], properties),
          () => wrapper.stageCreateOrReplace(id, schema, Array.empty[Transform], properties)
        ).foreach(call => assertThrows(classOf[SecurityException], () => { call(); () }))
        val carrier = issued(spark)
        val owner = carrier.owner
        assertThrows(classOf[SecurityException], () => owner.check(carrier.copy()(owner), spark))
        assertThrows(classOf[SecurityException], () => owner.check(carrier, spark.newSession()))
        val bytes = new ByteArrayOutputStream
        val output = new ObjectOutputStream(bytes)
        try output.writeObject(carrier)
        finally output.close()
        val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
        val restored =
          try input.readObject().asInstanceOf[DeltaRangerCreateCommand.Carrier]
          finally input.close()
        assertThrows(classOf[SecurityException], () => restored.owner)
        val physical = owner.plan(carrier, spark)
        assertThrows(classOf[SecurityException], () => physical.copy()(owner).run())
        spark.sparkContext.setLocalProperty("kyuubi.session.user", "foreign")
        try assertThrows(classOf[SecurityException], () => physical.run())
        finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
        spark.conf.set("spark.sql.shuffle.partitions", "99")
        try assertThrows(classOf[SecurityException], () => physical.run())
        finally spark.conf.set("spark.sql.shuffle.partitions", "2")
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def rejectsUnsupportedDeclarationBeforeReserve(): Unit =
    fixture.withCase("command-declarations", installed = true, observed = false) {
      (spark, transport, evidence) =>
        Seq(
          statement("id INT"),
          statement("id BIGINT NOT NULL"),
          statement("id BIGINT COMMENT 'x'"),
          statement(suffix = "PARTITIONED BY (id)"),
          statement(suffix = "LOCATION 'file:/tmp/foreign-create'"),
          statement(suffix = "COMMENT 'x'"),
          statement().replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"),
          "CREATE TABLE a.db.created USING delta AS SELECT 1L id"
        )
          .foreach(sql => assertThrows(classOf[Exception], () => { spark.sql(sql).collect(); () }))
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "false")
        try assertThrows(classOf[SecurityException], () => issued(spark))
        finally spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        val mutableSchema = schema
        val original = CreateTable(
          ResolvedIdentifier(spark.sessionState.catalogManager.catalog("a"), id),
          mutableSchema,
          Seq.empty,
          TableSpec(
            fixtureMap("tableProperties"),
            Some("delta"),
            Map.empty,
            None,
            None,
            None,
            false
          ),
          false
        )
        val carrier = DeltaRangerCreateCommand
          .resolve(original, spark, _ => wrapper)
          .asInstanceOf[DeltaRangerCreateCommand.Carrier]
        mutableSchema.fields(0) = mutableSchema.fields(0).copy(nullable = false)
        assertThrows(classOf[SecurityException], () => carrier.owner.plan(carrier, spark))
        assertTrue(transport.commands.isEmpty)
        assertEquals(0, transport.reads)
        assertTrue(evidence.files().isEmpty)
    }

  @Test
  def schemaGrantDoesNotBypassLiteralTableDeny(): Unit = {
    var real: RangerBasePlugin = null
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    try
      fixture.withCase(
        "command-literal-deny",
        installed = true,
        observed = false,
        policySetup = configured => {
          policy = configured
          real = realPolicies(configured, deny = true)
        }
      ) { (spark, transport, evidence) =>
        val subject = DeltaRangerQuerySubject.current(spark)
        policy.engine.authorize(
          subject,
          new DeltaRangerResourceKey("a", "db", "other", null, "create", null)
        )
        assertThrows(classOf[Exception], () => { spark.sql(statement()).collect(); () })
        assertTrue(
          policy.requests.asScala.exists(r => r.getResource.getValue("table") == "created")
        )
        assertTrue(transport.commands.isEmpty)
        assertTrue(evidence.files().isEmpty)
      }
    finally if (real != null) real.cleanup()
  }

  @Test
  def revocationAtEachBoundaryPreservesEffects(): Unit = (1 to 3).foreach { boundary =>
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    fixture.withCase(
      "command-revoked-" + boundary,
      installed = true,
      observed = false,
      policySetup = configured => policy = configured
    ) { (spark, transport, evidence) =>
      var deniedAtBoundary = false
      policy.onRequest = _ => {
        val phase =
          if (transport.commands.isEmpty) 1
          else if (DeltaRangerCreateMutation.current.isEmpty) {
            if (Files.exists(evidence.log0)) 3 else 2
          } else 0
        if (phase == boundary) {
          deniedAtBoundary = true
          policy.denied.add("a:*")
        }
      }
      assertThrows(classOf[Exception], () => { spark.sql(statement()).collect(); () })
      assertTrue(deniedAtBoundary)
      assertLiteralCreateRequests(policy)
      assertEquals(
        if (boundary == 1) 0 else 1,
        transport.commands.count(_.operation().name() == "RESERVE")
      )
      assertEquals(0, transport.commands.count(_.operation().name() == "PUBLISH"))
      assertEquals(boundary == 3, Files.exists(evidence.log0))
      assertTrue(DeltaRangerCreateMutation.current.isEmpty)
    }
  }

  @Test
  def publishedCreateDoesNotRequireSelect(): Unit = {
    var real: RangerBasePlugin = null
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    try
      fixture.withCase(
        "command-create-only",
        installed = true,
        observed = false,
        policySetup = configured => {
          policy = configured; real = realPolicies(configured, deny = false)
        }
      ) { (spark, transport, evidence) =>
        transport.publishSuccess = true
        spark.sql(statement()).collect()
        assertLiteralCreateRequests(policy)
        evidence.record("policy-request-count", policy.requests.size().toString)
        assertEquals("PUBLISHED", transport.receipt.state().name())
        assertEquals(0, loads)
        assertTrue(Files.exists(evidence.log0))
        assertTrue(Files.exists(evidence.log0.resolveSibling("00000000000000000000.crc")))
      }
    finally if (real != null) real.cleanup()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("changed-version", "same-version"))
  def completedPolicyRefreshKeepsLiteralCreateAuthorized(mode: String): Unit =
    policyRefreshAfterCreate0(mode)

  @ParameterizedTest
  @ValueSource(strings = Array("deny", "unavailable", "overlap"))
  def postCommitPolicyFailuresBlockPublishWithoutReplay(mode: String): Unit =
    policyRefreshAfterCreate0(mode)

  @Test
  def scopeAcquisitionDenyIsNotNativeInvocation(): Unit = {
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    fixture.withCase(
      "command-scope-denied",
      installed = true,
      observed = false,
      policySetup = configured => policy = configured
    ) { (spark, transport, evidence) =>
      var deniedAtScope = false
      policy.onRequest = _ => {
        if (
          DeltaRangerCreateMutation.current.nonEmpty && DeltaRangerNativeOperation.current() == null
        ) {
          deniedAtScope = true
          policy.denied.add("a:*")
        }
      }
      val carrier = issued(spark)
      val physical = carrier.owner.plan(carrier, spark)
      val failure = assertThrows(classOf[SecurityException], () => physical.run())
      evidence.failed("expected-scope-deny", failure)
      assertTrue(deniedAtScope)
      assertEquals("Delta Ranger access denied", failure.getMessage)
      assertEquals(Vector("RESERVE"), transport.commands.map(_.operation().name()).toVector)
      assertFalse(Files.exists(evidence.log0))
      assertTrue(DeltaRangerCreateMutation.current.isEmpty)
      assertNull(DeltaRangerNativeOperation.current())
      val commands = transport.commands.toVector
      assertThrows(classOf[SecurityException], () => physical.run())
      assertEquals(commands, transport.commands.toVector)
    }
  }

  @Test
  def postCommitFailureNeverReplays(): Unit =
    fixture.withCase("command-lost-publish", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val carrier = issued(spark)
        val physical = carrier.owner.plan(carrier, spark)
        val failure = assertThrows(classOf[Exception], () => physical.run())
        assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
        assertTrue(failure.getMessage.contains("publication=STATUS_NEEDED"), failure.toString)
        val commands = transport.commands.toVector
        val files = evidence.files()
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(commands, transport.commands.toVector)
        assertEquals(files, evidence.files())
        assertTrue(Files.exists(evidence.log0))
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
    }

  @Test
  def deniedMetadataAndOccupiedOrLostReserveNeverBecomeAbsence(): Unit =
    Seq("metadata", "occupied", "lost-reserve").foreach { mode =>
      fixture.withCase("command-" + mode, installed = true, observed = false) {
        (spark, transport, evidence) =>
          val failure = new IllegalStateException(mode)
          if (mode == "metadata") transport.metadataFailure = failure
          else transport.reserveFailure = failure
          assertThrows(classOf[Exception], () => { spark.sql(statement()).collect(); () })
          assertEquals(if (mode == "metadata") 0 else 1, transport.commands.size)
          assertTrue(evidence.files().isEmpty)
          assertTrue(DeltaRangerCreateMutation.current.isEmpty)
      }
    }

  @Test
  def confirmedPublicationSurvivesPostResponseJournalFailure(): Unit =
    fixture.withCase("command-published-journal-failure", installed = true, observed = false) {
      (spark, transport, evidence) =>
        transport.publishSuccess = true
        transport.afterPublishedResponse = () => {
          val paths = Files.list(evidence.directory.resolve("wrapper-journal"))
          try
            paths.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).foreach {
              path =>
                Files.delete(path)
                Files.createDirectory(path)
            }
          finally paths.close()
        }
        val carrier = issued(spark)
        val physical = carrier.owner.plan(carrier, spark)
        val failure = assertThrows(classOf[Exception], () => physical.run())
        evidence.failed("expected-post-response-journal", failure)
        assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
        assertTrue(failure.getMessage.contains("publication=PUBLISHED"), failure.toString)
        assertEquals("PUBLISHED", transport.receipt.state().name())
        val commands = transport.commands.toVector
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(commands, transport.commands.toVector)
    }

  @Test
  def scopeAcquisitionFailureIsNotNativeInvocation(): Unit = {
    var scopeCheck = false
    fixture.withCase(
      "command-scope-not-invoked",
      installed = true,
      observed = false,
      policySetup = policy => {
        doAnswer(_ => {
          if (
            DeltaRangerCreateMutation.current.nonEmpty && DeltaRangerNativeOperation
              .current() == null
          ) {
            scopeCheck = true
            java.lang.Long.valueOf(2L)
          } else java.lang.Long.valueOf(1L)
        }).when(policy.plugin).getPoliciesVersion
      }
    ) { (spark, transport, evidence) =>
      val failure =
        assertThrows(classOf[SecurityException], () => { spark.sql(statement()).collect(); () })
      evidence.failed("expected-scope-acquisition", failure)
      assertTrue(scopeCheck)
      assertEquals(Vector("RESERVE"), transport.commands.map(_.operation().name()).toVector)
      assertFalse(Files.exists(evidence.log0))
      assertTrue(DeltaRangerCreateMutation.current.isEmpty)
      assertNull(DeltaRangerNativeOperation.current())
    }
  }

  @Test
  def rejectsForeignActiveTransaction(): Unit =
    fixture.withCase("command-foreign-transaction", installed = true, observed = false) {
      (spark, transport, evidence) =>
        val carrier = issued(spark)
        val physical = carrier.owner.plan(carrier, spark)
        val foreign = mock(classOf[OptimisticTransaction])
        OptimisticTransaction.withActive(foreign) {
          assertThrows(classOf[SecurityException], () => physical.run())
        }
        assertTrue(transport.commands.isEmpty)
        assertFalse(Files.exists(evidence.log0))
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
    }

  @Test
  def unknownNativeFailureIsTerminal(): Unit =
    fixture.withCase("command-native-unknown", installed = true, observed = false) {
      (spark, transport, evidence) =>
        Files.write(evidence.target, Array[Byte](1))
        val carrier = issued(spark)
        val physical = carrier.owner.plan(carrier, spark)
        val failure = assertThrows(classOf[Exception], () => physical.run())
        assertTrue(failure.getMessage.contains("native=UNKNOWN"), failure.toString)
        val commands = transport.commands.toVector
        assertEquals(1, commands.size)
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(commands, transport.commands.toVector)
        assertFalse(Files.exists(evidence.log0))
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
    }

  private def realPolicies(
      fixture: TestDeltaRangerNativeSelect.Fixture,
      deny: Boolean,
      version: Long = 1L
  ): RangerBasePlugin = {
    val real = new RangerBasePlugin(fixture.plugin.getConfig)
    val definition = fixture.plugin.getServiceDef
    def policy(id: Long, table: String, denied: Boolean): RangerPolicy = {
      val value = new RangerPolicy
      value.setId(id); value.setName("create-" + id); value.setService("service")
      value.setPolicyType(0); value.setIsEnabled(true)
      value.setResources(
        Map("catalog" -> "a", "database" -> "db", "table" -> table, "column" -> "*").map {
          case (key, target) => key -> new RangerPolicy.RangerPolicyResource(target)
        }.asJava
      )
      val item = new RangerPolicy.RangerPolicyItem
      item.setUsers(Collections.singletonList(UserGroupInformation.getCurrentUser.getShortUserName))
      item.setAccesses(
        Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess("create", true))
      )
      if (denied) value.setDenyPolicyItems(Collections.singletonList(item))
      else value.setPolicyItems(Collections.singletonList(item))
      value
    }
    val policies = new ServicePolicies
    policies.setServiceName("service"); policies.setServiceDef(definition);
    policies.setPolicyVersion(version)
    policies.setServiceConfig(
      Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake")
    )
    policies.setPolicies(
      (Seq(policy(1, "*", denied = false)) ++
        (if (deny) Seq(policy(2, "created", denied = true)) else Seq.empty)).asJava
    )
    real.setPolicies(policies)
    real.setRoles(fixture.plugin.getRoles)
    doAnswer(call => {
      val request = call.getArgument[RangerAccessRequest](0)
      fixture.requests.add(request)
      fixture.onRequest.accept(request)
      real.isAccessAllowed(request)
    }).when(fixture.plugin).isAccessAllowed(any(classOf[RangerAccessRequest]))
    real
  }

  private def assertLiteralCreateRequests(policy: TestDeltaRangerNativeSelect.Fixture): Unit = {
    assertFalse(policy.requests.isEmpty)
    policy.requests.asScala.foreach { request =>
      assertEquals("create", request.getAccessType)
      assertEquals("create", request.getAction)
      assertEquals("a", request.getResource.getValue("catalog"))
      assertEquals("db", request.getResource.getValue("database"))
      assertEquals("created", request.getResource.getValue("table"))
      assertEquals("*", request.getResource.getValue("column"))
      assertNull(request.getResource.getOwnerUser)
      assertEquals(UserGroupInformation.getCurrentUser.getShortUserName, request.getUser)
    }
  }

  private def policyRefreshAfterCreate0(mode: String): Unit = {
    var policy: TestDeltaRangerNativeSelect.Fixture = null
    val plugins = ArrayBuffer.empty[RangerBasePlugin]
    try
      fixture.withCase(
        "command-postcommit-policy-" + mode,
        installed = true,
        observed = false,
        policySetup = configured => {
          policy = configured
          plugins += realPolicies(policy, deny = false)
        },
        logStoreClass = Some(classOf[CreatePolicyLogStore].getName)
      ) { (spark, transport, evidence) =>
        val allowed = mode == "changed-version" || mode == "same-version"
        var wroteLog0 = false
        var requestsAtWrite = -1
        var overlapped = false
        def publish(version: Long): Unit = {
          doAnswer(_ => java.lang.Long.valueOf(version)).when(policy.plugin).getPoliciesVersion
          policy.start()
        }
        CreatePolicyLogStore.afterWrite = path => {
          if (path.toUri.getPath == evidence.log0.toString) {
            assertFalse(wroteLog0)
            wroteLog0 = true
            assertTrue(Files.exists(evidence.log0))
            assertTrue(DeltaRangerCreateMutation.current.nonEmpty)
            assertNotNull(DeltaRangerNativeOperation.current())
            requestsAtWrite = policy.requests.size()
            mode match {
              case "changed-version" | "same-version" | "deny" =>
                val version = if (mode == "same-version") 1L else 2L
                plugins += realPolicies(policy, deny = mode == "deny", version = version)
                publish(version)
              case "unavailable" =>
                doAnswer(_ => java.lang.Long.valueOf(2L)).when(policy.plugin).getPoliciesVersion
              case "overlap" =>
                policy.onRequest = _ => {
                  if (!overlapped) {
                    overlapped = true
                    publish(2L)
                  }
                }
            }
            evidence.record(
              "completed-log0-policy-boundary",
              s"mode=$mode requests=$requestsAtWrite"
            )
          }
        }
        transport.publishSuccess = true
        val carrier = issued(spark)
        val physical = carrier.owner.plan(carrier, spark)
        val start = System.nanoTime()
        try {
          if (allowed) {
            physical.run()
            assertEquals("PUBLISHED", transport.receipt.state().name())
            assertTrue(Files.exists(evidence.log0.resolveSibling("00000000000000000000.crc")))
          } else {
            val failure = assertThrows(classOf[Exception], () => physical.run())
            evidence.failed("expected-postcommit-policy-" + mode, failure)
            assertTrue(failure.getMessage.contains("native=UNKNOWN"), failure.toString)
            assertTrue(failure.getMessage.contains("publication=NOT_ATTEMPTED"), failure.toString)
            val causes = Iterator
              .iterate[Throwable](failure)(_.getCause)
              .takeWhile(_ != null)
              .map(_.getMessage)
              .toVector
            val expected = mode match {
              case "deny"        => "Delta Ranger access denied"
              case "unavailable" => "Completed policy binding/version mismatch"
              case "overlap"     => "Delta Ranger publication changed; reanalyze the query"
            }
            assertTrue(causes.contains(expected), causes.mkString("; "))
          }
        } finally {
          evidence.record(
            "policy-reauthorization-cost",
            s"requests=${policy.requests.size()} requestsAtWrite=$requestsAtWrite elapsedNanos=${System.nanoTime() - start}"
          )
        }
        assertTrue(wroteLog0)
        assertTrue(Files.exists(evidence.log0))
        if (mode != "unavailable") assertTrue(policy.requests.size() > requestsAtWrite)
        assertEquals(mode == "overlap", overlapped)
        assertLiteralCreateRequests(policy)
        assertEquals(
          if (allowed) Vector("RESERVE", "PUBLISH") else Vector("RESERVE"),
          transport.commands.map(_.operation().name()).toVector
        )
        assertFalse(Files.exists(evidence.log0.resolveSibling("00000000000000000001.json")))
        assertEquals(0, loads)
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
        assertNull(DeltaRangerNativeOperation.current())
        assertTrue(OptimisticTransaction.getActive.isEmpty)
        val commands = transport.commands.toVector
        val files = evidence.files()
        assertFalse(files.keys.exists(_.endsWith(".parquet")))
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(commands, transport.commands.toVector)
        assertEquals(files, evidence.files())
      }
    finally {
      CreatePolicyLogStore.afterWrite = _ => ()
      plugins.foreach(_.cleanup())
    }
  }
}

/** Exposes only the return from the public native store write, preserving all original bytes. */
class CreatePolicyLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends HDFSLogStore(sparkConf, hadoopConf) {

  /** Completes the native write before notifying the deterministic policy-refresh fixture. */
  override def write(
      path: HadoopPath,
      actions: Iterator[String],
      overwrite: Boolean,
      conf: Configuration
  ): Unit = {
    super.write(path, actions, overwrite, conf)
    CreatePolicyLogStore.afterWrite(path)
  }
}

private[ranger] object CreatePolicyLogStore {
  var afterWrite: HadoopPath => Unit = (_: HadoopPath) => ()
}
