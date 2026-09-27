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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.xml.sax.SAXParseException;

class TestDeltaRangerDownloadedSecurity {
  @TempDir Path directory;

  @Test
  void postCursorPolicyAndBothSelectExactAppliedResponseDigests() throws Exception {
    String policyBody = "{\"serviceName\":\"service\",\"policyVersion\":1}";
    String roleBody = "{\"serviceName\":\"service\",\"roleVersion\":1}";
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          byte[] body =
              (exchange.getRequestURI().getPath().contains("roles") ? roleBody : policyBody)
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    upstream.start();
    Path source = source("http://127.0.0.1:" + upstream.getAddress().getPort());
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture();
        DeltaRangerDownloadedSecurity security =
            new DeltaRangerDownloadedSecurity(
                source,
                Files.createDirectory(directory.resolve("postcursor")),
                directory,
                "service")) {
      DeltaRangerPolicyEngine.Versions applied = fixture.engine.versions();
      long cursor = security.completedCursor();
      String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
      String role = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "service";
      request(security.uri(), policy);
      assertTrue(security.awaitDrained());
      DeltaRangerDownloadedSecurity.DownloadEvidence policyOnly =
          security.completedAfter(
              cursor, DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY, applied);
      assertEquals(applied.policy(), policyOnly.policy().orElseThrow().version());
      assertTrue(policyOnly.role().isEmpty());
      assertEquals(
          HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(policyBody.getBytes(StandardCharsets.UTF_8))),
          policyOnly.policy().orElseThrow().responseSha256());
      cursor = policyOnly.cursor();
      request(security.uri(), policy);
      request(security.uri(), role);
      assertTrue(security.awaitDrained());
      DeltaRangerDownloadedSecurity.DownloadEvidence both =
          security.completedAfter(
              cursor, DeltaRangerDownloadedSecurity.DownloadRequirement.BOTH, applied);
      assertEquals(applied.policy(), both.policy().orElseThrow().version());
      assertEquals(applied.role(), both.role().orElseThrow().version());
      assertEquals(
          HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(roleBody.getBytes(StandardCharsets.UTF_8))),
          both.role().orElseThrow().responseSha256());
      assertTrue(both.cursor() > cursor);
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void latestCompletedRoleMustMatchAppliedVersionAndNoneAdvancesCursor() throws Exception {
    AtomicReference<String> response =
        new AtomicReference<>("{\"serviceName\":\"owned\",\"roleVersion\":1}");
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          byte[] body = response.get().getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    upstream.start();
    Path source = source("http://127.0.0.1:" + upstream.getAddress().getPort());
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture();
        DeltaRangerDownloadedSecurity security =
            new DeltaRangerDownloadedSecurity(
                source, Files.createDirectory(directory.resolve("scoped")), directory, "owned")) {
      DeltaRangerPolicyEngine.Versions applied = fixture.engine.versions();
      long initial = security.completedCursor();
      String role = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "owned";
      request(security.uri(), role);
      assertTrue(security.awaitDrained());
      DeltaRangerDownloadedSecurity.DownloadEvidence matching =
          security.completedAfter(
              initial, DeltaRangerDownloadedSecurity.DownloadRequirement.ROLE, applied);
      assertEquals(1L, matching.role().orElseThrow().version());
      assertTrue(matching.policy().isEmpty());
      assertEquals(64, matching.role().orElseThrow().responseSha256().length());
      long cursor = matching.cursor();
      response.set("{\"serviceName\":\"owned\",\"roleVersion\":2}");
      request(security.uri(), role);
      assertTrue(security.awaitDrained());
      assertThrows(
          IllegalStateException.class,
          () ->
              security.completedAfter(
                  initial, DeltaRangerDownloadedSecurity.DownloadRequirement.ROLE, applied));
      DeltaRangerDownloadedSecurity.DownloadEvidence none =
          security.completedAfter(
              cursor, DeltaRangerDownloadedSecurity.DownloadRequirement.NONE, applied);
      assertTrue(none.role().isEmpty());
      assertTrue(none.cursor() > cursor);
      for (String malformed :
          List.of(
              "{\"serviceName\":\"other\",\"roleVersion\":1}",
              "{\"serviceName\":\"owned\"}",
              "{\"serviceName\":\"owned\",\"roleVersion\":1.5}",
              "{\"serviceName\":\"owned\",\"roleVersion\":9223372036854775808}",
              "{\"serviceName\":\"owned\",\"roleVersion\":1,\"roleVersion\":1}",
              "not-json")) {
        response.set(malformed);
        long before = security.completedCursor();
        request(security.uri(), role);
        assertTrue(security.awaitDrained());
        assertThrows(
            IllegalStateException.class,
            () ->
                security.completedAfter(
                    before, DeltaRangerDownloadedSecurity.DownloadRequirement.ROLE, applied));
      }
      assertFalse(Files.readString(directory.resolve("downloads.jsonl")).contains("roleVersion"));
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void copiesOnlyTwoPropertiesAndRequiresBothCompletedFreshDownloads() throws Exception {
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          assertEquals(
              "Basic test-only-token", exchange.getRequestHeaders().getFirst("Authorization"));
          byte[] body =
              (exchange.getRequestURI().getPath().contains("roles")
                      ? "{\"serviceName\":\"owned\",\"roleVersion\":9}"
                      : "{\"serviceName\":\"owned\",\"policyVersion\":7}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    upstream.start();
    Path original = source("http://127.0.0.1:" + upstream.getAddress().getPort());
    byte[] originalBytes = Files.readAllBytes(original.resolve("ranger-spark-security.xml"));
    Path target = Files.createDirectory(directory.resolve("driver"));
    Path evidence = Files.createDirectory(directory.resolve("evidence"));
    try (DeltaRangerDownloadedSecurity security =
        new DeltaRangerDownloadedSecurity(original, target, evidence, "owned")) {
      Map<String, String> before =
          DeltaRangerDownloadedSecurity.properties(original.resolve("ranger-spark-security.xml"));
      Map<String, String> after =
          DeltaRangerDownloadedSecurity.properties(target.resolve("ranger-spark-security.xml"));
      assertEquals(before.keySet(), after.keySet());
      assertEquals(
          "test-only-secret", after.get("ranger.plugin.spark.policy.rest.client.password"));
      assertEquals(security.uri().toString(), after.get("ranger.plugin.spark.policy.rest.url"));
      Path cache = Path.of(after.get("ranger.plugin.spark.policy.cache.dir"));
      try (var files = Files.list(cache)) {
        assertEquals(0, files.count());
      }
      assertEquals(
          PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(cache));
      assertEquals(
          PosixFilePermissions.fromString("rw-------"),
          Files.getPosixFilePermissions(target.resolve("ranger-spark-security.xml")));
      assertArrayEquals(
          originalBytes, Files.readAllBytes(original.resolve("ranger-spark-security.xml")));
      assertEquals("safe logger", Files.readString(target.resolve("log4j2.xml")));
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(7, 9, 7, 9));
      request(
          security.uri(),
          RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "owned");
      assertTrue(security.awaitDrained());
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(7, 9, 7, 9));
      request(
          security.uri(), RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "owned");
      assertTrue(security.awaitDrained());
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(6, 9, 7, 9));
      security.recordInitialDownloads(7, 9, 7, 9);
      String manifest = Files.readString(evidence.resolve("security-manifest.json"));
      assertFalse(manifest.contains("test-only-secret"));
      JsonNode preparation = new ObjectMapper().readTree(manifest);
      assertTrue(preparation.path("freshEmptyCache").asBoolean());
      assertEquals("empty", preparation.path("mode").asText());
      String events = Files.readString(evidence.resolve("downloads.jsonl"));
      assertFalse(events.contains("test-only-token"));
      assertFalse(events.contains("policyVersion"));
      assertFalse(events.contains("roleVersion"));
      var accepted =
          new ObjectMapper().readTree(evidence.resolve("initial-downloads.json").toFile());
      assertEquals(7, accepted.path("policyVersion").asLong());
      assertEquals(9, accepted.path("roleVersion").asLong());
      assertEquals(1, accepted.path("policyAttempt").asLong());
      assertEquals(2, accepted.path("roleAttempt").asLong());
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void oldConstructorRetainsEmptyTargetAndPostCloseLogAccess() throws Exception {
    Path root = directory.toRealPath();
    Path original = source("http://127.0.0.1:1");
    Path target = Files.createDirectory(root.resolve("driver"));
    Path evidence = Files.createDirectory(root.resolve("evidence"));
    DeltaRangerDownloadedSecurity security =
        new DeltaRangerDownloadedSecurity(original, target, evidence, "owned");
    security.close();
    assertTrue(security.startupEvents().isEmpty());
    try (var entries = Files.list(target.resolve("cache"))) {
      assertEquals(0, entries.count());
    }
    Path seeded = Files.createDirectory(root.resolve("seeded"));
    Files.writeString(seeded.resolve("policy"), "old cache");
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerDownloadedSecurity(original, seeded, evidence, "owned"));
  }

  @Test
  void missingRecorderFileInvalidatesStartupEventAudit() throws Exception {
    Path root = directory.toRealPath();
    Path original = source("http://127.0.0.1:1");
    Path target = Files.createDirectory(root.resolve("driver"));
    Path evidence = Files.createDirectory(root.resolve("evidence"));
    try (DeltaRangerDownloadedSecurity security =
        new DeltaRangerDownloadedSecurity(original, target, evidence, "owned")) {
      Files.delete(evidence.resolve("downloads.jsonl"));
      assertThrows(IOException.class, security::startupEvents);
    }
  }

  @Test
  void rejectsStaleDirectoryAndNeverCountsNotModifiedOrAnotherService() throws Exception {
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          exchange.sendResponseHeaders(
              exchange.getRequestURI().getPath().endsWith("/other") ? 200 : 304, -1);
          exchange.close();
        });
    upstream.start();
    Path original = source("http://127.0.0.1:" + upstream.getAddress().getPort());
    Path target = Files.createDirectory(directory.resolve("stale"));
    Files.writeString(target.resolve("old-cache"), "stale");
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerDownloadedSecurity(original, target, directory, "owned"));
    Path fresh = Files.createDirectory(directory.resolve("fresh"));
    try (DeltaRangerDownloadedSecurity security =
        new DeltaRangerDownloadedSecurity(original, fresh, directory, "owned")) {
      request(
          security.uri(),
          RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "owned");
      request(
          security.uri(), RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "owned");
      request(
          security.uri(),
          RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "other");
      request(
          security.uri(), RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "other");
      assertTrue(security.awaitDrained());
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(7, 9, 7, 9));
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void requiresGetAndRecordedUpstreamCompletionForBothDownloads() throws Exception {
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          byte[] body =
              (exchange.getRequestURI().getPath().contains("roles")
                      ? "{\"serviceName\":\"owned\",\"roleVersion\":9}"
                      : "{\"serviceName\":\"owned\",\"policyVersion\":7}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    upstream.start();
    Path original = source("http://127.0.0.1:" + upstream.getAddress().getPort());
    Path target = Files.createDirectory(directory.resolve("driver"));
    try (DeltaRangerDownloadedSecurity security =
        new DeltaRangerDownloadedSecurity(original, target, directory, "owned")) {
      String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "owned";
      HttpClient.newHttpClient()
          .send(
              HttpRequest.newBuilder(security.uri().resolve(policy))
                  .PUT(HttpRequest.BodyPublishers.noBody())
                  .build(),
              HttpResponse.BodyHandlers.ofByteArray());
      request(
          security.uri(), RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "owned");
      assertTrue(security.awaitDrained());
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(7, 9, 7, 9));
      request(security.uri(), policy);
      assertTrue(security.awaitDrained());
      Path events = directory.resolve("downloads.jsonl");
      String complete = Files.readString(events);
      List<String> incomplete = new ArrayList<>();
      ObjectMapper json = new ObjectMapper();
      for (String line : complete.lines().toList()) {
        ObjectNode event = (ObjectNode) json.readTree(line);
        if (event.path("path").asText().equals(policy)) {
          event.remove("upstreamResponseCompleted");
        }
        incomplete.add(json.writeValueAsString(event));
      }
      Files.write(events, incomplete);
      assertThrows(IllegalStateException.class, () -> security.recordInitialDownloads(7, 9, 7, 9));
      Files.writeString(events, complete);
      security.recordInitialDownloads(7, 9, 7, 9);
    } finally {
      upstream.stop(0);
    }
  }

  @Test
  void rejectsMissingDuplicateAndExternalEntityConfigurationBeforeDownloads() throws Exception {
    Path original = source("http://127.0.0.1:1");
    Path xml = original.resolve("ranger-spark-security.xml");
    String valid = Files.readString(xml);
    Files.writeString(xml, "<configuration/>");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeltaRangerDownloadedSecurity(
                original, Files.createDirectory(directory.resolve("missing")), directory, "owned"));
    Files.writeString(
        xml,
        valid.replace(
            "</configuration>",
            "<property><name>ranger.plugin.spark.policy.cache.dir</name><value>duplicate</value></property></configuration>"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeltaRangerDownloadedSecurity(
                original,
                Files.createDirectory(directory.resolve("duplicate")),
                directory,
                "owned"));
    Files.writeString(
        xml, "<!DOCTYPE configuration SYSTEM 'file:///unread-test-sentinel'>" + valid);
    assertThrows(
        SAXParseException.class,
        () ->
            new DeltaRangerDownloadedSecurity(
                original, Files.createDirectory(directory.resolve("entity")), directory, "owned"));
  }

  private Path source(String url) throws Exception {
    Path source = Files.createDirectory(directory.resolve("source"));
    Files.writeString(source.resolve("log4j2.xml"), "safe logger");
    Files.writeString(
        source.resolve("ranger-spark-security.xml"),
        "<configuration>"
            + "<property><name>ranger.plugin.spark.policy.rest.url</name><value>"
            + url
            + "</value></property>"
            + "<property><name>ranger.plugin.spark.policy.cache.dir</name><value>/old/cache</value></property>"
            + "<property><name>ranger.plugin.spark.policy.rest.client.password</name><value>test-only-secret</value></property>"
            + "</configuration>");
    return source;
  }

  private static void request(URI uri, String path) throws Exception {
    HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(uri.resolve(path))
                .header("Authorization", "Basic test-only-token")
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofByteArray());
  }
}
