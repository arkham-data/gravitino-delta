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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerCallbackGateway {
  @TempDir Path directory;

  @Test
  void forwardsActualNoContentStatusAndResponseWithoutInventingJson() throws Exception {
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/owned/user",
        exchange -> {
          assertArrayEquals(new byte[] {1, 2, 3}, exchange.getRequestBody().readAllBytes());
          exchange.getResponseHeaders().set("X-Observed", "actual");
          exchange.sendResponseHeaders(204, -1);
          exchange.close();
        });
    upstream.start();
    try (DeltaRangerCallbackGateway gateway =
        new DeltaRangerCallbackGateway(
            URI.create("http://127.0.0.1:" + upstream.getAddress().getPort()),
            directory.resolve("events.jsonl"))) {
      HttpResponse<byte[]> response =
          HttpClient.newHttpClient()
              .send(
                  HttpRequest.newBuilder(gateway.uri().resolve("/owned/user"))
                      .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1, 2, 3}))
                      .build(),
                  HttpResponse.BodyHandlers.ofByteArray());
      assertEquals(204, response.statusCode());
      assertArrayEquals(new byte[0], response.body());
      assertEquals("actual", response.headers().firstValue("X-Observed").orElseThrow());
      assertTrue(gateway.awaitDrained());
      assertEquals(1, gateway.completed());
      assertTrue(
          Files.readString(directory.resolve("events.jsonl")).contains("\"upstreamStatus\":204"));
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void exactOneFaultCannotConsumeAnotherMethodOrPathOrReachUpstream() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/owned",
        exchange -> {
          requests.incrementAndGet();
          byte[] body = new byte[] {4, 5};
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    upstream.start();
    try (DeltaRangerCallbackGateway gateway =
        new DeltaRangerCallbackGateway(
            URI.create("http://127.0.0.1:" + upstream.getAddress().getPort()),
            directory.resolve("events.jsonl"))) {
      gateway.failNext("PUT", "/owned/policy/1", 503);
      HttpClient client = HttpClient.newHttpClient();
      for (String[] attempt :
          new String[][] {
            {"GET", "/owned/policy/1", "200"},
            {"PUT", "/owned/policy/2", "200"},
            {"PUT", "/owned/policy/1", "503"},
            {"PUT", "/owned/policy/1", "200"}
          }) {
        HttpResponse<byte[]> response =
            client.send(
                HttpRequest.newBuilder(gateway.uri().resolve(attempt[1]))
                    .method(attempt[0], HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(Integer.parseInt(attempt[2]), response.statusCode());
      }
      assertEquals(3, requests.get());
      assertTrue(gateway.awaitDrained());
      assertEquals(4, gateway.completed());
    } finally {
      upstream.stop(0);
    }
  }
}
