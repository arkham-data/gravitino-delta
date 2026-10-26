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
import java.net.InetAddress
import java.security.PrivilegedExceptionAction
import org.apache.hadoop.security.UserGroupInformation
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import py4j.GatewayServer
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.SparkSession

class TestDeltaRangerReadActions {
  @Test
  def nativeArrowFreshAndRepeated(): Unit = runPython("arrow")

  @Test
  def mixedActionsReauthorizeBeforePreparation(): Unit = runPython("mixed")

  @Test
  def arrowCallerAndRegistrationDriftReject(): Unit = runPython("drift")

  @Test
  def inFlightArrowKeepsCapture(): Unit = runPython("inflight")

  @Test
  def failedArrowDoesNotAuthorizeNextAction(): Unit = runPython("failure")

  @Test
  def metricResetCannotMintAuthority(): Unit = {
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, policy, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      val root = frame.queryExecution.executedPlan
      val owner = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner
      assertThrows(classOf[SecurityException], () => root.resetMetrics())
      assertTrue(owner.readContext.isEmpty)
      spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, "-1")
      try assertThrows(classOf[SecurityException], () => root.resetMetrics())
      finally spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
      val other = spark.sql(DeltaRangerTrustedAqeFixture.query)
      SQLExecution.withNewExecutionId(other.queryExecution) {
        assertThrows(classOf[SecurityException], () => root.resetMetrics())
      }
      var execution = -1L
      SQLExecution.withNewExecutionId(frame.queryExecution) {
        execution = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY).toLong
        root.resetMetrics()
        val captured = owner.readContext.get
        policy.denied.add("a:secret")
        root.resetMetrics()
        root.children.foreach(_.resetMetrics())
        assertTrue(owner.readContext.contains(captured))
      }
      assertTrue(owner.readContext.isEmpty)
      spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, execution.toString)
      try assertThrows(classOf[SecurityException], () => root.resetMetrics())
      finally spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
      policy.denied.clear()
      assertEquals(3, root.executeCollect().length)
      assertTrue(owner.readContext.isEmpty)
    }
  }

  @Test
  def directCollectionOwnsItsScopeInsideAnotherExecution(): Unit = {
    DeltaRangerTrustedAqeFixture.withSpark() { (spark, policy, _) =>
      val frame = spark.sql(DeltaRangerTrustedAqeFixture.query)
      val root = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCollectGuardExec]
      val owner = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner
      val other = spark.sql(DeltaRangerTrustedAqeFixture.query)
      SQLExecution.withNewExecutionId(other.queryExecution) {
        assertEquals(3, root.executeCollect().length)
        assertTrue(owner.readContext.isEmpty)
        policy.denied.add("a:secret")
        assertThrows(classOf[SecurityException], () => root.executeCollect())
        assertTrue(owner.readContext.isEmpty)
        policy.denied.clear()
        val state = new CollectGuardChildState
        state.failExecution = true
        val failing =
          root.withNewChildren(Seq(CollectGuardTestProbeExec(root.child, new Object, state)))
        assertThrows(classOf[IllegalStateException], () => failing.executeCollect())
        assertTrue(owner.readContext.isEmpty)
      }
      assertEquals(3, frame.collect().length)
      assertTrue(owner.readContext.isEmpty)
    }
  }

  @Test
  def nativeArrowEmptyAndConversionFailure(): Unit = runPython("conversion")

  @Test
  def nonadaptiveReadsExpireCaptureAndRevalidate(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      assertFalse(spark.sessionState.conf.adaptiveExecutionEnabled)
      val frame = spark.sql("SELECT id, secret FROM a.db.t")
      frame.queryExecution.executedPlan
      val owner = DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner
      assertEquals(2, frame.collect().length)
      assertTrue(owner.readContext.isEmpty)
      policy.denied.add("a:secret")
      assertThrows(classOf[SecurityException], () => frame.collect())
      assertTrue(owner.readContext.isEmpty)
    }

  @Test
  def nonadaptiveArrowRejectsBeforeBroadcastPreparation(): Unit = runPython("legacy")

  @Test
  def nonadaptiveInFlightArrowKeepsCapture(): Unit = runPython("legacy-inflight")

  @Test
  def nativeArrowPreservesAuthenticatedCaller(): Unit = {
    val origin = UserGroupInformation.createUserForTesting("arrow-origin", Array("readers"))
    val other = UserGroupInformation.createUserForTesting("arrow-other", Array("other-readers"))
    origin.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = DeltaRangerTrustedAqeFixture.withSpark() { (spark, policy, _) =>
        val probe = new DeltaRangerReadActionProbe(spark, policy)
        runPythonSession("named", spark, policy, Some(probe))
        other.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = runPythonSession("wrong-caller", spark, policy, Some(probe))
        })
        runPythonSession("named-recovery", spark, policy, Some(probe))
      }
    })
  }

  private def runPython(mode: String): Unit = {
    if (mode.startsWith("legacy")) {
      val policy = new TestDeltaRangerNativeSelect.Fixture
      try
        NativeSelectFixture.withSpark(
          spark => runPythonSession(mode, spark, policy),
          Some(NativeSelectFixture.extension(policy)),
          startupOptions =
            Map("spark.plugins" -> classOf[DeltaRangerTrustedAqeBarrierPlugin].getName)
        )
      finally policy.engine.close()
    } else
      DeltaRangerTrustedAqeFixture.withSpark(
        Map("spark.plugins" -> classOf[DeltaRangerTrustedAqeBarrierPlugin].getName)
      ) { (spark, policy, _) => runPythonSession(mode, spark, policy) }
  }

  private def runPythonSession(
      mode: String,
      spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      providedProbe: Option[DeltaRangerReadActionProbe] = None
  ): Unit = {
    val root = Iterator
      .iterate(Paths.get("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .find(p => Files.exists(p.resolve("delta/acceptance")))
      .get
    val evidence = sys.env
      .get("READ_ACTION_EVIDENCE")
      .map(Paths.get(_))
      .getOrElse(Files.createTempDirectory("delta-read-actions-"))
    val token = java.util.UUID.randomUUID().toString
    val gateway = new GatewayServer.GatewayServerBuilder()
      .entryPoint(providedProbe.getOrElse(new DeltaRangerReadActionProbe(spark, policy)))
      .authToken(token)
      .javaPort(0)
      .javaAddress(InetAddress.getLoopbackAddress)
      .build()
    gateway.start()
    var process: Process = null
    try {
      val command = new ProcessBuilder(
        sys.env.getOrElse(
          "READ_ACTION_PYTHON",
          root
            .resolve(".superpowers/runtime-20260914/spark-" + spark.version + "/bin/python")
            .toString
        ),
        root.resolve("delta/acceptance/read_parity_actions.py").toString,
        gateway.getListeningPort.toString,
        mode
      )
      command.environment().put("PYSPARK_PIN_THREAD", "false")
      command.environment().put("READ_ACTION_TOKEN", token)
      command.redirectOutput(evidence.resolve(mode + ".stdout").toFile)
      command.redirectError(evidence.resolve(mode + ".stderr").toFile)
      Files.write(
        evidence.resolve(mode + ".argv.json"),
        new ObjectMapper().writeValueAsBytes(command.command())
      )
      process = command.start()
      assertTrue(process.waitFor(180, TimeUnit.SECONDS), "Python probe timed out")
      Files.write(
        evidence.resolve(mode + ".exit"),
        process.exitValue().toString.getBytes("UTF-8")
      )
      assertEquals(0, process.exitValue(), "See " + evidence.resolve(mode + ".stderr"))
    } finally {
      if (process != null && process.isAlive) {
        process.destroyForcibly()
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Python process did not stop")
      }
      gateway.shutdown()
    }
  }
}
