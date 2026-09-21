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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerCallbackGateway {
  @TempDir Path directory;

  @Test
  void privatelyRetainsExactCompletedResponseBytesWhenExplicitlyRequested() throws Exception {
    byte[] actual = new byte[] {0, 1, 2, -1};
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/owned",
        exchange -> {
          exchange.sendResponseHeaders(200, actual.length);
          exchange.getResponseBody().write(actual);
          exchange.close();
        });
    upstream.start();
    try (DeltaRangerCallbackGateway gateway =
        new DeltaRangerCallbackGateway(
            URI.create("http://127.0.0.1:" + upstream.getAddress().getPort()),
            directory.resolve("events.jsonl"),
            0,
            true)) {
      HttpClient.newHttpClient()
          .send(
              HttpRequest.newBuilder(gateway.uri().resolve("/owned")).GET().build(),
              HttpResponse.BodyHandlers.ofByteArray());
      assertTrue(gateway.awaitDrained());
      JsonNode finished = new ObjectMapper().readTree(gateway.recordedEvents().get(1));
      assertTrue(finished.path("responseBodyFile").isTextual());
      Path body = Path.of(finished.path("responseBodyFile").asText());
      assertArrayEquals(actual, Files.readAllBytes(body));
      assertEquals(
          PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(body));
      assertTrue(finished.has("upstreamResponseCompleted"));
    } finally {
      upstream.stop(0);
    }
  }

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

  @Test
  void initialEvidenceFailureClosesExchangeDrainsAndStopsListener() throws Exception {
    assertEvidenceFailure(false);
  }

  @Test
  void finalEvidenceFailureClosesExchangeDrainsAndStopsListener() throws Exception {
    assertEvidenceFailure(true);
  }

  private void assertEvidenceFailure(boolean failFinal) throws Exception {
    Path evidence = directory.resolve("events.jsonl");
    AtomicInteger requests = new AtomicInteger();
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/owned",
        exchange -> {
          try {
            requests.incrementAndGet();
            Files.delete(evidence);
            Files.createDirectory(evidence);
            exchange.sendResponseHeaders(204, -1);
          } finally {
            exchange.close();
          }
        });
    upstream.start();
    DeltaRangerCallbackGateway gateway =
        new DeltaRangerCallbackGateway(
            URI.create("http://127.0.0.1:" + upstream.getAddress().getPort()), evidence);
    int port = gateway.uri().getPort();
    try {
      if (!failFinal) {
        Files.delete(evidence);
        Files.createDirectory(evidence);
      }
      // One socket means exactly one request, with no client transport retry after an empty reply.
      try (Socket socket = new Socket("127.0.0.1", port)) {
        socket.setSoTimeout(2000);
        socket
            .getOutputStream()
            .write(
                ("POST /owned HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx")
                    .getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        socket.getInputStream().readAllBytes();
      }
      assertTimeoutPreemptively(
          Duration.ofSeconds(2),
          () -> {
            assertTrue(gateway.awaitDrained(), "Evidence errors cannot leak active handlers");
            assertThrows(
                IOException.class,
                gateway::recordedEvents,
                "Lost evidence must prevent download snapshot acceptance");
            assertThrows(
                IOException.class, gateway::close, "Lost evidence must prevent acceptance");
          });
      assertEquals(failFinal ? 1 : 0, requests.get());
      assertEquals(1, gateway.completed());
      try (ServerSocket released = new ServerSocket()) {
        released.bind(new InetSocketAddress("127.0.0.1", port));
      }
    } finally {
      upstream.stop(0);
    }
  }
}
