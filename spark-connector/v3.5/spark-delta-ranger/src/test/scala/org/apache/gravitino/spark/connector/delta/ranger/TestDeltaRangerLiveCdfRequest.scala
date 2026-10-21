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

import java.net.{URI, InetSocketAddress}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import com.sun.net.httpserver.{HttpServer, HttpExchange}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerLiveCdfRequest {
  @Test def everyNamedTableMetadataGetRequiresTheSeparateIdentity(): Unit = {
    val calls = new AtomicInteger()
    val upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    upstream.createContext(
      "/",
      (exchange: HttpExchange) => {
        calls.incrementAndGet(); exchange.sendResponseHeaders(200, -1L); exchange.close()
      }
    )
    upstream.start()
    val output = Files.createTempDirectory("cdf-metadata-identity")
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
      URI.create("http://127.0.0.1:" + upstream.getAddress.getPort),
      output
    )
    val prefix = "/api/metalakes/l/catalogs/c/schemas/d/tables/"
    gateway.observeTables(prefix, "metadata")
    def get(table: String, principal: String): Int = {
      val auth = Base64.getEncoder.encodeToString((principal + ":").getBytes("UTF-8"))
      HttpClient
        .newHttpClient()
        .send(
          HttpRequest
            .newBuilder(gateway.uri.resolve(prefix + table))
            .header("Authorization", "Basic " + auth)
            .GET()
            .build(),
          HttpResponse.BodyHandlers.discarding()
        )
        .statusCode()
    }
    try {
      assertEquals(200, get("target", "metadata"))
      assertEquals(200, get("source", "metadata"))
      gateway.requireMetadataObserved()
      val proof = new ObjectMapper().readTree(output.resolve("metadata-identity.json").toFile)
      assertEquals(2, proof.path("successfulNamedGets").size())
      assertEquals(503, get("diagnostic", "application"))
      assertEquals(2, calls.get())
    } finally {
      gateway.close(); upstream.stop(0)
      Files.deleteIfExists(output.resolve("metadata-identity.json")); Files.delete(output)
    }
  }

  @Test def literalReadbackCollectsTheIssuedFrameWithoutDerivingAPlan(): Unit = {
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val image = DeltaRangerLiveAppend.governedRowImage(spark.table("a.db.t"))
      assertEquals(
        new ObjectMapper().readTree("[[1,\"a-one-masked\",\"keep\"],[3,null,\"keep\"]]"),
        image
      )
    }
  }

  @Test def cdfAppendPinsNativeTransactionOptionsAndRestoresThem(): Unit = {
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val keys = Seq(
        "spark.databricks.delta.write.txnAppId",
        "spark.databricks.delta.write.txnVersion",
        "spark.databricks.delta.write.txnVersion.autoReset.enabled"
      )
      spark.conf.set(keys(0), "prior-app"); spark.conf.set(keys(1), "7");
      spark.conf.set(keys(2), "true")
      val before = keys.map(spark.conf.getOption)
      val marker = new IllegalStateException("owned-test-failure")
      for (fail <- Seq(false, true)) {
        val run = () =>
          DeltaRangerLiveAppend.withCdfAppendTransaction(spark) {
            assertTrue(SparkSession.getActiveSession.exists(_ eq spark))
            assertEquals("false", spark.conf.get(keys(2)))
            assertEquals("1", spark.conf.get(keys(1)))
            assertNotEquals("prior-app", spark.conf.get(keys(0)))
            assertNotNull(DeltaRangerNativeIdempotency.validateOptions(spark, Map.empty))
            if (fail) throw marker
            "completed"
          }
        if (fail) assertSame(marker, assertThrows(classOf[IllegalStateException], () => run()))
        else assertEquals("completed", run())
        assertEquals(before, keys.map(spark.conf.getOption))
      }
      keys.foreach(spark.conf.unset)
      val unset = keys.map(spark.conf.getOption)
      DeltaRangerLiveAppend.withCdfAppendTransaction(spark) {
        assertNotNull(DeltaRangerNativeIdempotency.validateOptions(spark, Map.empty))
      }
      assertEquals(unset, keys.map(spark.conf.getOption))
    }
  }

  private def input: ObjectNode = {
    val value = new ObjectMapper().createObjectNode()
    Seq(
      "sourceSecurityDirectory",
      "driverSecurityDirectory",
      "sparkConfigurationFile",
      "stateEvidenceFile",
      "packageManifest",
      "controlDirectory"
    ).foreach(k => value.put(k, "/tmp/" + k))
    Seq(
      "resourceService",
      "resourceDefinition",
      "metalake",
      "catalog",
      "database",
      "source",
      "target",
      "enabledDiagnostic",
      "updateDiagnostic",
      "deniedDiagnostic"
    ).foreach(k => value.put(k, k))
    value.put("subject", "application"); value.put("metadataPrincipal", "metadata")
    value.put("expectedSparkVersion", "3.5.4"); value.put("expectedDeltaVersion", "3.3.0")
    Seq("publicStateSha256", "rangerStateSha256").foreach(k => value.put(k, "a" * 64))
    value.put("schemaVersion", 1); value.put("minPolicy", 1); value.put("minRole", 1)
    value.put("beforeVersion", 0); value.put("deadlineSeconds", 300)
    value
  }

  @Test def acceptsOnlyBoundedCdfInputs(): Unit = {
    assertEquals(0, DeltaRangerLiveAppend.parseCdf(input.toString).path("beforeVersion").asInt())
    for (spark <- Seq("3.5.4", "3.5.6"); delta <- Seq("3.3.0", "3.3.2")) {
      val v = input.put("expectedSparkVersion", spark).put("expectedDeltaVersion", delta)
      assertEquals(
        delta,
        DeltaRangerLiveAppend.parseCdf(v.toString).path("expectedDeltaVersion").asText()
      )
    }
  }

  @Test def rejectsAmbiguousOrUnboundedRequests(): Unit = {
    for (
      (key, replacement) <- Seq(
        "deadlineSeconds" -> "0",
        "deadlineSeconds" -> "601",
        "beforeVersion" -> "-1",
        "minPolicy" -> "\"1\"",
        "subject" -> "\"metadata\"",
        "enabledDiagnostic" -> "\"target\"",
        "controlDirectory" -> "\"relative\"",
        "target" -> "\"t;drop\"",
        "expectedDeltaVersion" -> "\"3.3.1\"",
        "unknown" -> "true"
      )
    ) {
      val v = input; v.set(key, new ObjectMapper().readTree(replacement))
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveAppend.parseCdf(v.toString)
      )
    }
    val missing = input; missing.remove("minRole")
    Seq(
      missing.toString,
      input.toString + " {}",
      input.toString.dropRight(1) + ",\"schemaVersion\":1}"
    ).foreach(v =>
      assertThrows(classOf[IllegalArgumentException], () => DeltaRangerLiveAppend.parseCdf(v))
    )
  }
}
