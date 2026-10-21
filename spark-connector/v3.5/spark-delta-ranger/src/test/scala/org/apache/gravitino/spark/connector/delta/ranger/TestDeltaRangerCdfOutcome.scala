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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.storage.HDFSLogStore
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.when

/** Classifies the real native publication after policy revocation at the log-write boundary. */
class TestDeltaRangerCdfOutcome {
  private val sql = "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"

  @Test
  def policyRevocationAfterNativeWriteReportsCommitAndAllowsObservationOnlyRecovery(): Unit =
    withWriteBoundary { (spark, policy, fixture, next) =>
      var owner: DeltaRangerCdfPropertyMutation = null
      var published = Map.empty[String, Vector[Byte]]
      CdfOutcomeLogStore.afterWrite = path =>
        if (Paths.get(path.toUri) == next && DeltaRangerCdfPropertyMutation.current.nonEmpty) {
          owner = DeltaRangerCdfPropertyMutation.current.get
          assertNotNull(DeltaRangerNativeOperation.current())
          val state = owner.getClass.getDeclaredFields.find(_.getName.endsWith("publication")).get
          state.setAccessible(true)
          assertEquals(None, state.get(owner), "Native write must precede the first observer")
          published = manifest(next.getParent.getParent)
          policy.denied.add("a:*")
          policy.denied.add("a:id")
          advance(policy)
        }
      val failure = alterFailure(spark)
      assertNotNull(owner, "The real native LogStore write must reach the boundary")
      assertOutcome(failure, "CDF property change committed;")
      assertTrue(causes(failure).exists(e => Option(e.getMessage).exists(_.contains("reanalyze"))))
      assertEquals(published, manifest(next.getParent.getParent))
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertClosed(owner)
      val requests = policy.requests.size()
      assertThrows(classOf[Exception], () => { spark.sql("SELECT id FROM a.db.t").collect(); () })
      assertTrue(
        policy.requests.asScala
          .drop(requests)
          .exists(request =>
            request.getAccessType == "select" && request.getResource.getValue("column") == "id"
          ),
        "The updated policy must evaluate and deny the later governed SELECT"
      )
      assertEquals(published, manifest(next.getParent.getParent))
      policy.denied.clear()
      advance(policy)
      DeltaLog.clearCache()
      fixture.wrapper.loadTable(Identifier.of(Array("db"), "t"))
      val recovered =
        DeltaLog.forTable(spark, NativeSelectFixture.locations("a")).unsafeVolatileSnapshot
      assertEquals(next.getFileName.toString.stripSuffix(".json").toLong, recovered.version)
      assertEquals(Some("true"), recovered.metadata.configuration.get("delta.enableChangeDataFeed"))
      assertEquals(published, manifest(next.getParent.getParent))
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertFalse(Files.exists(next.resolveSibling(f"${recovered.version + 1}%020d.json")))
    }

  @Test
  def changedPublicationAfterAnEarlierProofKeepsTheUncertainOutcome(): Unit =
    withWriteBoundary { (spark, policy, fixture, next) =>
      var changed = false
      var owner: DeltaRangerCdfPropertyMutation = null
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { current =>
          if (!changed && Files.exists(next)) {
            val state =
              current.getClass.getDeclaredFields.find(_.getName.endsWith("publication")).get
            state.setAccessible(true)
            if (state.get(current).asInstanceOf[Option[_]].nonEmpty) {
              owner = current
              changed = true
              replace(
                spark,
                next,
                Files.readAllBytes(next) ++ "{\"remove\":{}}\n".getBytes(StandardCharsets.UTF_8)
              )
              advance(policy)
            }
          }
        }
      val failure = alterFailure(spark)
      assertTrue(changed, "The live owner must have already proved its original publication")
      assertOutcome(failure, "CDF property change outcome uncertain;")
      assertTrue(causes(failure).exists(_.getSuppressed.nonEmpty))
      assertClosed(owner)
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      val version = next.getFileName.toString.stripSuffix(".json").toLong
      assertFalse(Files.exists(next.resolveSibling(f"${version + 1}%020d.json")))
    }

  @ParameterizedTest(name = "unprovable native publication reports uncertain: {0}")
  @ValueSource(strings = Array("foreign", "tampered", "unreadable", "missing"))
  def unavailableOrForeignPublicationNeverReportsRetryOrCommit(mode: String): Unit =
    withWriteBoundary { (spark, policy, _, next) =>
      var owner: DeltaRangerCdfPropertyMutation = null
      CdfOutcomeLogStore.afterWrite = path =>
        if (Paths.get(path.toUri) == next && DeltaRangerCdfPropertyMutation.current.nonEmpty) {
          owner = DeltaRangerCdfPropertyMutation.current.get
          val bytes = Files.readAllBytes(next)
          mode match {
            case "foreign" =>
              val mapper = new ObjectMapper
              val actions =
                new String(bytes, StandardCharsets.UTF_8).trim.split("\n").map(mapper.readTree)
              actions.head
                .get("commitInfo")
                .asInstanceOf[ObjectNode]
                .put("txnId", "foreign-transaction")
              replace(
                spark,
                next,
                (actions.map(_.toString).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
              )
            case "tampered" =>
              replace(spark, next, bytes ++ "{\"remove\":{}}\n".getBytes(StandardCharsets.UTF_8))
            case "unreadable" => Files.delete(next); Files.createDirectory(next)
            case "missing"    => Files.delete(next)
          }
          advance(policy)
        }
      val failure = alterFailure(spark)
      assertNotNull(owner)
      assertOutcome(failure, "CDF property change outcome uncertain;")
      val outcome = causes(failure)
        .find(e =>
          Option(e.getMessage).exists(_.startsWith("CDF property change outcome uncertain;"))
        )
        .get
      assertNotNull(outcome.getCause, "The original native or context failure must be retained")
      assertTrue(outcome.getSuppressed.nonEmpty, "The failed publication proof must be retained")
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertClosed(owner)
      val version = next.getFileName.toString.stripSuffix(".json").toLong
      assertFalse(Files.exists(next.resolveSibling(f"${version + 1}%020d.json")))
    }

  @Test
  def outcomeProofUsesTheCommitInfoProfileCapturedBeforeNativeInvocation(): Unit =
    withWriteBoundary { (spark, policy, _, next) =>
      var written = false
      CdfOutcomeLogStore.afterWrite = path =>
        if (Paths.get(path.toUri) == next && DeltaRangerCdfPropertyMutation.current.nonEmpty) {
          written = true
          spark.conf.set("spark.databricks.delta.commitInfo.userMetadata", "after-publication")
          spark.conf.set("spark.databricks.delta.history.metricsEnabled", "false")
          advance(policy)
        }
      val failure = alterFailure(spark)
      assertTrue(written)
      assertOutcome(failure, "CDF property change committed;")
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

  @Test
  def preNativePolicyDenialKeepsTheOriginalDenialAndAllBytes(): Unit =
    withWriteBoundary { (spark, policy, _, next) =>
      val before = manifest(next.getParent.getParent)
      var writes = 0
      CdfOutcomeLogStore.afterWrite = _ => writes += 1
      policy.denied.add("a:*")
      advance(policy)
      val failure = alterFailure(spark)
      assertFalse(
        causes(failure).exists(e =>
          Option(e.getMessage).exists(_.startsWith("CDF property change"))
        )
      )
      assertEquals(0, writes)
      assertEquals(0, CdfAlterForwardingCatalog.alters.get())
      assertEquals(before, manifest(next.getParent.getParent))
      assertNull(DeltaRangerNativeOperation.current())
    }

  private def withWriteBoundary(
      body: (
          SparkSession,
          TestDeltaRangerNativeSelect.Fixture,
          NativeBoundCatalogFixture,
          Path
      ) => Unit
  ): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          CdfAlterForwardingCatalog.alters.set(0)
          val fixture = NativeSelectFixture.bindingFixture(spark, "a")
          val location = NativeSelectFixture.locations("a")
          val version = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version + 1
          body(
            spark,
            policy,
            fixture,
            Paths.get(location).resolve("_delta_log").resolve(f"$version%020d.json")
          )
        },
        Some(NativeSelectFixture.extension(policy)),
        Map("spark.delta.logStore.class" -> classOf[CdfOutcomeLogStore].getName)
      )
    finally { CdfOutcomeLogStore.afterWrite = _ => (); policy.engine.close() }
  }

  private def advance(policy: TestDeltaRangerNativeSelect.Fixture): Unit = {
    val version = policy.plugin.getPoliciesVersion + 1L
    when(policy.plugin.getPoliciesVersion).thenReturn(version)
    policy.start()
  }

  private def alterFailure(spark: SparkSession): Exception =
    assertThrows(classOf[Exception], () => { spark.sql(sql).collect(); () })

  private def causes(failure: Throwable): Vector[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toVector

  private def assertOutcome(failure: Throwable, prefix: String): Unit = {
    val messages = causes(failure).flatMap(e => Option(e.getMessage))
    assertTrue(messages.exists(_.startsWith(prefix)), messages.mkString("\n"))
    assertTrue(messages.exists(_.contains("do not retry ALTER")), messages.mkString("\n"))
    assertTrue(messages.find(_.startsWith("CDF property change")).exists(_.startsWith(prefix)))
  }

  private def assertClosed(owner: DeltaRangerCdfPropertyMutation): Unit = {
    assertThrows(classOf[SecurityException], () => owner.requireLive())
    assertTrue(DeltaRangerCdfPropertyMutation.current.isEmpty)
    assertNull(DeltaRangerNativeOperation.current())
    assertTrue(OptimisticTransaction.getActive.isEmpty)
  }

  private def replace(spark: SparkSession, path: Path, bytes: Array[Byte]): Unit = {
    val hadoop = new HadoopPath(path.toUri)
    val output = hadoop.getFileSystem(spark.sessionState.newHadoopConf()).create(hadoop, true)
    try output.write(bytes)
    finally output.close()
  }

  private def manifest(root: Path): Map[String, Vector[Byte]] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(path => root.relativize(path).toString -> Files.readAllBytes(path).toVector)
        .toMap
    finally paths.close()
  }
}

/** Runs the test callback only after the native store has completely published its own bytes. */
class CdfOutcomeLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends HDFSLogStore(sparkConf, hadoopConf) {

  /** Preserves native publication and exposes its return boundary to the installed test. */
  override def write(
      path: HadoopPath,
      actions: Iterator[String],
      overwrite: Boolean,
      conf: Configuration
  ): Unit = {
    super.write(path, actions, overwrite, conf)
    CdfOutcomeLogStore.afterWrite(path)
  }
}

private[ranger] object CdfOutcomeLogStore {
  var afterWrite: HadoopPath => Unit = (_: HadoopPath) => ()
}
