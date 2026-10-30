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

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files
import java.util.Base64
import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Finite transport observations include rejected attempts without changing forwarding. */
class TestDeltaRangerLiveNamedRequests {

  /** A later request cannot mutate an earlier boundary snapshot. */
  @Test
  def snapshotsRetainRepeatedAllowedAndDeniedRequests(): Unit = {
    val upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    upstream.createContext(
      "/",
      exchange => {
        val status = if (exchange.getRequestURI.getPath.endsWith("denied")) 403 else 200
        exchange.sendResponseHeaders(status, -1); exchange.close()
      }
    )
    upstream.start()
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
      URI.create("http://127.0.0.1:" + upstream.getAddress.getPort),
      Files.createTempDirectory("named-request-evidence")
    )
    try {
      val prefix = "/api/metalakes/lake/catalogs/catalog/schemas/schema/tables/"
      gateway.observeTables(prefix, "metadata")
      val empty = gateway.namedRequests()
      val client = HttpClient.newHttpClient()
      def get(table: String): Int = client
        .send(
          HttpRequest
            .newBuilder(gateway.uri.resolve(prefix + table))
            .header(
              "Authorization",
              "Basic " + Base64.getEncoder.encodeToString("metadata:ignored".getBytes("UTF-8"))
            )
            .GET()
            .build(),
          HttpResponse.BodyHandlers.discarding()
        )
        .statusCode()
      assertEquals(200, get("source"))
      val first = gateway.namedRequests()
      assertEquals(403, get("denied"))
      assertEquals(200, get("source"))
      assertTrue(empty.isEmpty)
      assertEquals(1, first.size())
      assertEquals(200, first.get(0).get("status"))
      val last = gateway.namedRequests()
      assertEquals(3, last.size())
      assertEquals(403, last.get(1).get("status"))
      assertEquals(prefix + "denied", last.get(1).get("path"))
      assertEquals(3L, last.get(2).get("sequence"))
      assertEquals(
        Set("sequence", "method", "path", "status"), {
          import scala.collection.JavaConverters._
          last.get(0).keySet().asScala.toSet
        }
      )
    } finally { gateway.close(); upstream.stop(0) }
  }

  /** Authentication failures are attempted table requests too. */
  @Test
  def failedAuthenticationIsRetainedWithoutSecrets(): Unit = {
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
      URI.create("http://127.0.0.1:1"),
      Files.createTempDirectory("failed-named-request")
    )
    try {
      val prefix = "/api/metalakes/lake/catalogs/catalog/schemas/schema/tables/"
      gateway.observeTables(prefix, "metadata")
      val response = HttpClient
        .newHttpClient()
        .send(
          HttpRequest.newBuilder(gateway.uri.resolve(prefix + "source")).GET().build(),
          HttpResponse.BodyHandlers.discarding()
        )
      assertEquals(503, response.statusCode())
      val requests = gateway.namedRequests()
      assertEquals(1, requests.size())
      assertEquals(0, requests.get(0).get("status"))
      assertFalse(requests.toString.contains("Authorization"))
    } finally gateway.close()
  }
}
