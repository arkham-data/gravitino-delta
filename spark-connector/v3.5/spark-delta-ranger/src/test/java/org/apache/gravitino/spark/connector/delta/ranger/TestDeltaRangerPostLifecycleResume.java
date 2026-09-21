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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRangerPostLifecycleResume {
  private static final ObjectMapper JSON = new ObjectMapper();

  private ObjectNode fixture;
  private ObjectNode checkpoint;
  private ObjectNode baseline;
  private ObjectNode preflight;
  @TempDir Path directory;

  @BeforeEach
  void inputs() throws Exception {
    try (InputStream input = getClass().getResourceAsStream("/post-lifecycle-resume.json")) {
      JsonNode data = JSON.readTree(input);
      fixture = data.path("fixture").deepCopy();
      checkpoint = data.path("checkpoint").deepCopy();
      preflight =
          JSON.createObjectNode()
              .put("schemaVersion", 1)
              .put("stage", "post-role-lifecycle-root-preflight")
              .put("metalake", fixture.path("metalake").asText())
              .put("expectedMetalakeId", "986854579816189983")
              .put("mode", "retained-process")
              .put("s3ObjectCount", 12)
              .put("storageBaseline", "restored-original-bytes-new-mtime")
              .putNull("coldH2Sha256");
      for (String key :
          List.of(
              "processIdentityVerified",
              "executableInputsVerified",
              "rangerPersistenceVerified",
              "callbackOwnershipReleased",
              "publicContinuityVerified",
              "s3ExactBytesVerified")) preflight.put(key, true);
      for (String key :
          List.of(
              "s3ManifestSha256",
              "transitionSha256",
              "processEvidenceSha256",
              "rangerPersistenceEvidenceSha256",
              "executableManifestSha256")) preflight.put(key, "a".repeat(64));
      baseline =
          JSON.createObjectNode()
              .put("schemaVersion", 1)
              .put("stage", "post-role-lifecycle-resume-baseline")
              .put("metalake", fixture.path("metalake").asText())
              .put("expectedMetalakeId", "986854579816189983")
              .put("checkpointSha256", DeltaRangerPostLifecycleResume.CHECKPOINT_SHA256)
              .put("preflightSha256", "b".repeat(64))
              .put("policyVersion", 44)
              .put("roleVersion", 63);
      baseline.set("observations", data.path("observations"));
    }
  }

  @Test
  void acceptsCurrentBoundaryAndItsExplicitDisabledPolicyProjection() {
    assertDoesNotThrow(this::validate);
    assertEquals(19, baseline.at("/observations/policies/body/policies").size());
    assertEquals(18, baseline.at("/observations/policyDownload/body/policies").size());
    preflight.put("mode", "cold-restart").put("coldH2Sha256", "c".repeat(64));
    assertDoesNotThrow(this::validate);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "stage",
        "metalake",
        "expectedMetalakeId",
        "policyVersion",
        "roleVersion",
        "preflightSha256",
        "checkpointSha256",
        "schemaVersion"
      })
  void rejectsEnvelopeDrift(String field) {
    baseline.put(field, "invalid");
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "stage",
        "mode",
        "processIdentityVerified",
        "executableInputsVerified",
        "rangerPersistenceVerified",
        "callbackOwnershipReleased",
        "publicContinuityVerified",
        "s3ExactBytesVerified",
        "s3ObjectCount",
        "s3ManifestSha256",
        "storageBaseline",
        "transitionSha256",
        "processEvidenceSha256",
        "rangerPersistenceEvidenceSha256",
        "executableManifestSha256",
        "coldH2Sha256"
      })
  void rejectsIncompleteRootPreflight(String field) {
    preflight.remove(field);
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "role:metadata_schema",
        "role:global_guard",
        "sidePolicy",
        "sideService",
        "definition",
        "tagService",
        "downloader",
        "resourceService",
        "roleDownload",
        "policyDownload"
      })
  void rejectsMissingRequiredObservation(String key) {
    ((ObjectNode) baseline.path("observations")).remove(key);
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "schemaContribution",
        "auditRole",
        "guardContribution",
        "sideReference",
        "foreignPolicy",
        "orphanIdentity",
        "protocolIdentity",
        "guardIdentity",
        "schemaIdentity",
        "membership",
        "missingDownload",
        "generation",
        "historicalGeneration",
        "localSchemaMissing",
        "unknownObservation"
      })
  void rejectsProtectedStateAndSemanticDrift(String boundary) {
    ArrayNode roles = (ArrayNode) baseline.at("/observations/roles/body/roles");
    ArrayNode policies = (ArrayNode) baseline.at("/observations/policies/body/policies");
    switch (boundary) {
      case "schemaContribution":
      case "guardContribution":
        ((ObjectNode) policies.get(0))
            .putArray("policyItems")
            .addObject()
            .putArray("roles")
            .add(
                "GRAVITINO_METALAKE_986854579816189983_ROLE_"
                    + fixture.path("rolesPrefix").asText()
                    + (boundary.equals("schemaContribution") ? "metadata_schema" : "global_guard"));
        break;
      case "auditRole":
        roles.addObject().put("id", 24).put("name", "audit");
        break;
      case "sideReference":
        ((ObjectNode) baseline.at("/observations/sidePolicy/body")).putArray("policyItems");
        break;
      case "foreignPolicy":
        ((ObjectNode) policies.get(9)).put("id", 99);
        break;
      case "orphanIdentity":
        changeRoleId(roles, 7);
        break;
      case "protocolIdentity":
        changeRoleId(roles, 13);
        break;
      case "guardIdentity":
        changeRoleId(roles, 23);
        break;
      case "schemaIdentity":
        changeRoleId(roles, 25);
        break;
      case "membership":
        for (JsonNode role : roles)
          if (role.path("id").asInt() == 23)
            ((ObjectNode) role)
                .putArray("users")
                .addObject()
                .put("name", "intruder")
                .put("isAdmin", false);
        break;
      case "missingDownload":
        ((ArrayNode) baseline.at("/observations/policyDownload/body/policies")).remove(0);
        break;
      case "generation":
        ((ObjectNode) baseline.at("/observations/roleDownload/body")).put("roleVersion", 64);
        break;
      case "historicalGeneration":
        ((ObjectNode) checkpoint.path("lastObservedDownloads")).put("policyVersion", 44);
        break;
      case "localSchemaMissing":
        ((ObjectNode) baseline.at("/observations/role:metadata_schema")).put("status", 404);
        break;
      case "unknownObservation":
        ((ObjectNode) baseline.path("observations")).putObject("evil");
        break;
      default:
        throw new AssertionError(boundary);
    }
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  @Test
  void freshTransportUsesOnlyCompletedGetResponsesForBothGateways() throws Exception {
    validate();
    Map<String, String> paths = DeltaRangerPostLifecycleResume.paths(fixture);
    List<String> methods = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger failures = new AtomicInteger();
    HttpServer upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    upstream.createContext(
        "/",
        exchange -> {
          methods.add(exchange.getRequestMethod());
          String key =
              paths.entrySet().stream()
                  .filter(e -> e.getValue().equals(exchange.getRequestURI().toString()))
                  .findFirst()
                  .orElseThrow()
                  .getKey();
          JsonNode observation = baseline.path("observations").path(key);
          byte[] bytes =
              JSON.writeValueAsBytes(
                  failures.get() == 0 ? observation.path("body") : JSON.createObjectNode());
          exchange.sendResponseHeaders(observation.path("status").asInt(), bytes.length);
          exchange.getResponseBody().write(bytes);
          exchange.close();
        });
    upstream.start();
    URI endpoint = URI.create("http://127.0.0.1:" + upstream.getAddress().getPort());
    try (DeltaRangerCallbackGateway publicGateway =
            new DeltaRangerCallbackGateway(endpoint, directory.resolve("public.jsonl"), 0, true);
        DeltaRangerCallbackGateway callbackGateway =
            new DeltaRangerCallbackGateway(
                endpoint, directory.resolve("callback.jsonl"), 0, true)) {
      DeltaRangerPostLifecycleResume.ReadOnlyGet reads =
          (key, path) ->
              DeltaRangerPostLifecycleResume.get(
                  path.startsWith("/api/") ? publicGateway.uri() : callbackGateway.uri(),
                  path,
                  "Basic offline-test");
      assertEquals(33, DeltaRangerPostLifecycleResume.validateCurrent(fixture, baseline, reads));
      assertEquals(33, methods.size());
      publicGateway.awaitDrained();
      callbackGateway.awaitDrained();
      DeltaRangerPostLifecycleResume.requireCompletedReads(publicGateway.recordedEvents());
      DeltaRangerPostLifecycleResume.requireCompletedReads(callbackGateway.recordedEvents());
      List<String> incomplete = new ArrayList<>(callbackGateway.recordedEvents());
      incomplete.remove(incomplete.size() - 1);
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerPostLifecycleResume.requireCompletedReads(incomplete));
      assertEquals(Set.of("GET"), Set.copyOf(methods));
      failures.set(1);
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerPostLifecycleResume.validateCurrent(fixture, baseline, reads));
      assertEquals(34, methods.size());
      assertEquals(Set.of("GET"), Set.copyOf(methods));
      assertEquals(0, methods.stream().filter(m -> !m.equals("GET")).count());
    } finally {
      upstream.stop(0);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "role:metadata_schema",
        "role:audit_cleanup",
        "role:global_guard",
        "role:protocol",
        "user:metadata",
        "user:visibilityOnly",
        "group:query",
        "roles",
        "policies",
        "sidePolicy",
        "sideService",
        "definition",
        "downloader",
        "policyDownload",
        "roleDownload"
      })
  void freshReadRejectsEvenAnAdditionalModelField(String key) {
    ObjectNode changed = baseline.path("observations").deepCopy();
    ((ObjectNode) changed.path(key).path("body")).put("unexpectedField", true);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerPostLifecycleResume.validateCurrent(
                fixture, baseline, (name, path) -> changed.path(name)));
  }

  @Test
  void rejectsNumericStringsAndMalformedJsonBeforeDispatch() throws Exception {
    baseline.put("policyVersion", "44");
    assertThrows(IllegalArgumentException.class, this::validate);
    for (String content :
        List.of(
            "{\"operation\":\"close\",\"operation\":\"grant-user\"}",
            "{\"operation\":\"close\"} {\"operation\":\"grant-user\"}")) {
      Path command = directory.resolve("malformed.json");
      Files.writeString(command, content);
      Files.setPosixFilePermissions(command, PosixFilePermissions.fromString("rw-------"));
      assertThrows(Exception.class, () -> DeltaRangerPostLifecycleResume.readCommand(command));
    }
  }

  @Test
  void onlyEnumeratedTelemetryAndCollectionOrderCanDiffer() throws Exception {
    JsonNode expected = baseline.path("observations").path("roles");
    ObjectNode actual = expected.deepCopy();
    ((ObjectNode) actual.path("body")).put("queryTimeMS", 99999);
    ArrayNode roles = (ArrayNode) actual.path("body").path("roles");
    JsonNode first = roles.remove(0);
    roles.add(first);
    assertDoesNotThrow(() -> DeltaRangerPostLifecycleResume.compare("roles", expected, actual));
    ((ObjectNode) roles.get(0)).put("updateTime", 99999);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.compare("roles", expected, actual));
    ObjectNode ordered =
        (ObjectNode)
            JSON.readTree(
                "{\"status\":200,\"body\":{\"columns\":[\"a\",\"b\"],\"values\":[\"x\",\"y\"]}}");
    ObjectNode reordered = ordered.deepCopy();
    ((ObjectNode) reordered.path("body")).putArray("columns").add("b").add("a");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.compare("table:delta_a", ordered, reordered));
    ObjectNode side = baseline.path("observations").path("sideService").deepCopy();
    ObjectNode config = (ObjectNode) side.path("body").path("configs");
    config.put(
        "ranger.plugin.audit.filters",
        config.path("ranger.plugin.audit.filters").asText().replace("'", "\""));
    assertDoesNotThrow(
        () ->
            DeltaRangerPostLifecycleResume.compare(
                "sideService", baseline.path("observations").path("sideService"), side));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "method",
        "upstreamAttempted",
        "upstreamResponseCompleted",
        "downstreamResponseCompleted",
        "upstreamStatus",
        "responseBodyFile",
        "transportFailure",
        "upstreamCompletionUnresolved"
      })
  void rejectsIncompleteReadEvidence(String field) throws Exception {
    Path response = directory.resolve("completed.body");
    Files.writeString(response, "{}");
    ObjectNode start =
        JSON.createObjectNode().put("attempt", 1).put("method", "GET").put("path", "/owned");
    ObjectNode complete =
        start
            .deepCopy()
            .put("finished", "now")
            .put("upstreamAttempted", true)
            .put("upstreamResponseCompleted", "now")
            .put("downstreamResponseCompleted", true)
            .put("upstreamStatus", 200)
            .put("responseBodyFile", response.toString());
    DeltaRangerPostLifecycleResume.requireCompletedReads(
        List.of(start.toString(), complete.toString()));
    if (Set.of("transportFailure", "upstreamCompletionUnresolved").contains(field)) {
      complete.put(field, true);
    } else {
      complete.remove(field);
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerPostLifecycleResume.requireCompletedReads(
                List.of(start.toString(), complete.toString())));
  }

  @Test
  void refusesReintroducedAuditFilterEvenInAnInternallyConsistentBaseline() {
    ObjectNode configs = (ObjectNode) baseline.at("/observations/sideService/body/configs");
    configs.put(
        "ranger.plugin.audit.filters",
        "[{\"accessResult\":\"DENIED\",\"roles\":[\"audit_cleanup\"],\"isAudited\":true}]");
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  @Test
  void fixedProtocolRoleCannotDisappearFromBothAdminAndDownloads() {
    for (String pointer :
        List.of("/observations/roles/body/roles", "/observations/roleDownload/body/rangerRoles")) {
      ArrayNode roles = (ArrayNode) baseline.at(pointer);
      for (int i = 0; i < roles.size(); i++)
        if (roles.get(i).path("id").asInt() == 14) {
          roles.remove(i);
          break;
        }
    }
    ((ObjectNode) baseline.at("/observations/roles/body"))
        .put("totalCount", 18)
        .put("resultSize", 18);
    assertThrows(IllegalArgumentException.class, this::validate);
  }

  private void validate() {
    DeltaRangerPostLifecycleResume.validateInputs(
        fixture,
        checkpoint,
        baseline,
        preflight,
        DeltaRangerPostLifecycleResume.CHECKPOINT_SHA256,
        "b".repeat(64));
  }

  private static void changeRoleId(ArrayNode roles, int id) {
    for (JsonNode role : roles)
      if (role.path("id").asInt() == id) ((ObjectNode) role).put("id", 99);
  }

  @Test
  void bindsPrivateInputBytesBeforeParsing() throws Exception {
    Path input = directory.resolve("input.json");
    byte[] bytes = "{\"schemaVersion\":1}".getBytes(StandardCharsets.UTF_8);
    Files.write(input, bytes);
    Files.setPosixFilePermissions(input, PosixFilePermissions.fromString("rw-------"));
    StringBuilder digest = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes))
      digest.append(String.format("%02x", b));
    assertEquals(
        1,
        DeltaRangerPostLifecycleResume.readBound(input, digest.toString())
            .path("schemaVersion")
            .asInt());
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.readBound(input, "0".repeat(64)));
    Files.setPosixFilePermissions(input, PosixFilePermissions.fromString("rw-r-----"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.readBound(input, digest.toString()));
  }

  @Test
  void exactResponseComparisonRejectsEveryModelFieldDrift() throws Exception {
    ObjectNode expected =
        (ObjectNode)
            JSON.readTree(
                "{\"status\":200,\"body\":{\"id\":53,\"updateTime\":1,\"policyItems\":[{\"roles\":[\"guard\"]}]}}");
    assertDoesNotThrow(
        () -> DeltaRangerPostLifecycleResume.compare("sidePolicy", expected, expected.deepCopy()));
    ObjectNode drift = expected.deepCopy();
    ((ObjectNode) drift.path("body")).put("updateTime", 2);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.compare("sidePolicy", expected, drift));
    ObjectNode missing = expected.deepCopy();
    ((ObjectNode) missing.path("body")).remove("policyItems");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.compare("sidePolicy", expected, missing));
  }

  @Test
  void protectedAndUnknownCommandsFailBeforeDispatch() {
    for (String operation : List.of("metadata-schema", "reconcile-owner", "bootstrap", "unknown")) {
      ObjectNode command = JSON.createObjectNode().put("operation", operation);
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerPostLifecycleResume.validateCommand(command, false));
    }
    for (String suffix :
        List.of(
            "global_guard",
            "audit_cleanup",
            "metadata_schema",
            "protocol",
            "owner",
            "_fixed_protocol")) {
      ObjectNode command =
          JSON.createObjectNode()
              .put("operation", "grant-user")
              .put("roleSuffix", suffix)
              .put("subject", "freshCallback");
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerPostLifecycleResume.validateCommand(command, false));
    }
  }

  @Test
  void malformedAndQuarantinedMutationsFailClosed() {
    for (ObjectNode command :
        List.of(
            JSON.createObjectNode(),
            JSON.createObjectNode().put("operation", "grant-user"),
            JSON.createObjectNode().put("operation", "close").put("roleSuffix", "ordinary"),
            JSON.createObjectNode().put("operation", "snapshot").put("id", "../escape"))) {
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerPostLifecycleResume.validateCommand(command, false));
    }
    ObjectNode command =
        JSON.createObjectNode()
            .put("operation", "grant-user")
            .put("roleSuffix", "ordinary")
            .put("subject", "freshCallback");
    assertDoesNotThrow(() -> DeltaRangerPostLifecycleResume.validateCommand(command, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerPostLifecycleResume.validateCommand(command, true));
    assertDoesNotThrow(
        () ->
            DeltaRangerPostLifecycleResume.validateCommand(
                JSON.createObjectNode().put("operation", "snapshot").put("id", "safe-snapshot"),
                true));
    assertDoesNotThrow(
        () ->
            DeltaRangerPostLifecycleResume.validateCommand(
                JSON.createObjectNode().put("operation", "close"), true));
  }
}
