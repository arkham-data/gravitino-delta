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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRangerStartupProbe {
  private static final ObjectMapper JSON = new ObjectMapper();
  @TempDir Path directory;

  @Test
  void absentProbeKeepsDefaultAndNullProbeRejects() throws Exception {
    assertTrue(DeltaRangerStartupProbe.parse(JSON.readTree("{}")).isEmpty());
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupProbe.parse(JSON.readTree("{\"startupProbe\":null}")));
  }

  @Test
  void probeBuildsOnlyOneNamedSelect() throws Exception {
    DeltaRangerStartupProbe.Request request = DeltaRangerStartupProbe.parse(input()).orElseThrow();
    assertEquals("SELECT `secret` FROM `a`.`db`.`t`", request.sql());
    assertTrue(request.manifestSha256().isEmpty());
    ((ArrayNode) request.expectedRows()).removeAll();
    assertEquals(2, request.expectedRows().size());
  }

  @Test
  void missingRolesNeverExecutesSqlAndRemainsStartupFailure() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      when(fixture.plugin.getRangerRoles()).thenReturn(null);
      SparkSession spark = mock(SparkSession.class);
      AtomicLong now = new AtomicLong();
      DeltaRangerStartupProbe.Observation result =
          DeltaRangerStartupProbe.observe(
              spark,
              fixture.engine,
              DeltaRangerStartupProbe.parse(input()).orElseThrow(),
              now::get,
              millis -> now.addAndGet(Duration.ofMillis(millis).toNanos()));
      assertFalse(result.ready());
      assertFalse(result.queryAttempted());
      assertEquals("not-ready", result.evidence().get("readiness"));
      assertNull(result.evidence().get("versions"));
      assertThrows(
          UnsupportedOperationException.class,
          () -> ((List<?>) result.evidence().get("expectedRows")).clear());
      verifyNoInteractions(spark);
      assertTrue(now.get() >= Duration.ofSeconds(60).toNanos());
    }
  }

  @Test
  void anyHttp200InvalidatesAnOutageEvenWithoutDownloadMetadata() {
    String start =
        "{\"run\":\"r\",\"attempt\":1,\"method\":\"GET\","
            + "\"path\":\"/unclassified\",\"started\":\"2026-09-27T00:00:00Z\"}";
    String end =
        start.substring(0, start.length() - 1)
            + ",\"upstreamAttempted\":true,\"upstreamStatus\":200,"
            + "\"transportFailure\":\"java.io.IOException\","
            + "\"finished\":\"2026-09-27T00:00:01Z\",\"activeAfter\":0}";
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(start, end), "service"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "null",
        "false",
        "true",
        "[]",
        "{\"mode\":\"other\",\"query\":{}}",
        "{\"mode\":\"empty\",\"manifestSha256\":\"a\",\"query\":{}}",
        "{\"mode\":\"snapshot\",\"query\":{}}",
        "{\"mode\":\"empty\",\"timeout\":60,\"query\":{}}",
        "{\"mode\":\"empty\",\"expectedReady\":true,\"query\":{}}"
      })
  void rejectsUnknownOrMalformedProbe(String probe) throws Exception {
    JsonNode input = JSON.readTree("{\"catalogs\":[\"a\"],\"startupProbe\":" + probe + "}");
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(input));
  }

  @ParameterizedTest
  @ValueSource(strings = {"a.b", "a/b", "a`b", "*", "", "1a"})
  void rejectsUnsafeIdentifier(String name) throws Exception {
    ObjectNode input = (ObjectNode) input();
    ((ObjectNode) input.path("startupProbe").path("query")).put("table", name);
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(input));
  }

  @ParameterizedTest
  @ValueSource(strings = {"sql", "path", "timeout", "expectedReady", "denied", "refresh"})
  void rejectsAdditionalProbeFlags(String key) throws Exception {
    ObjectNode value = (ObjectNode) input();
    ((ObjectNode) value.path("startupProbe")).put(key, true);
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(value));
  }

  @Test
  void rejectsInvisibleCatalogAndColumnOrRowBounds() throws Exception {
    ObjectNode invisible = (ObjectNode) input();
    ((ArrayNode) invisible.path("catalogs")).removeAll();
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(invisible));

    ObjectNode duplicate = (ObjectNode) input();
    ((ArrayNode) duplicate.path("startupProbe").path("query").path("columns")).add("secret");
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(duplicate));

    ObjectNode emptyColumns = (ObjectNode) input();
    ((ArrayNode) emptyColumns.path("startupProbe").path("query").path("columns")).removeAll();
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(emptyColumns));

    ObjectNode manyColumns = (ObjectNode) input();
    ArrayNode columns = (ArrayNode) manyColumns.path("startupProbe").path("query").path("columns");
    for (int i = 0; i < 16; i++) {
      columns.add("c" + i);
    }
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(manyColumns));

    ObjectNode manyRows = (ObjectNode) input();
    ArrayNode rows = (ArrayNode) manyRows.path("startupProbe").path("query").path("expectedRows");
    for (int i = rows.size(); i <= 256; i++) {
      rows.addArray().add("value");
    }
    assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(manyRows));

    for (String invalidRows : List.of("[[\"a\",\"b\"]]", "[[{}]]", "[[[]]]")) {
      ObjectNode value = (ObjectNode) input();
      ((ObjectNode) value.path("startupProbe").path("query"))
          .set("expectedRows", JSON.readTree(invalidRows));
      assertThrows(IllegalArgumentException.class, () -> DeltaRangerStartupProbe.parse(value));
    }
  }

  @Test
  void wrongPolicyAndRolePublicationsRemainNotReady() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      when(fixture.plugin.getPoliciesVersion()).thenReturn(-1L);
      AtomicLong now = new AtomicLong();
      SparkSession spark = mock(SparkSession.class);
      DeltaRangerStartupProbe.Observation result =
          DeltaRangerStartupProbe.observe(
              spark,
              fixture.engine,
              DeltaRangerStartupProbe.parse(input()).orElseThrow(),
              now::get,
              millis -> now.addAndGet(Duration.ofMillis(millis).toNanos()));
      assertFalse(result.ready());
      verifyNoInteractions(spark);
    }
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      RangerRoles roles = new RangerRoles();
      roles.setServiceName("wrong-service");
      roles.setRoleVersion(1L);
      when(fixture.plugin.getRangerRoles()).thenReturn(roles);
      AtomicLong now = new AtomicLong();
      SparkSession spark = mock(SparkSession.class);
      DeltaRangerStartupProbe.Observation result =
          DeltaRangerStartupProbe.observe(
              spark,
              fixture.engine,
              DeltaRangerStartupProbe.parse(input()).orElseThrow(),
              now::get,
              millis -> now.addAndGet(Duration.ofMillis(millis).toNanos()));
      assertFalse(result.ready());
      verifyNoInteractions(spark);
    }
  }

  @Test
  void interruptionAndClosedEnginePropagate() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      when(fixture.plugin.getRangerRoles()).thenReturn(null);
      SparkSession spark = mock(SparkSession.class);
      assertThrows(
          InterruptedException.class,
          () ->
              DeltaRangerStartupProbe.observe(
                  spark,
                  fixture.engine,
                  DeltaRangerStartupProbe.parse(input()).orElseThrow(),
                  () -> 0L,
                  millis -> {
                    throw new InterruptedException("interrupted");
                  }));
      verifyNoInteractions(spark);
      fixture.engine.close();
      assertThrows(
          IllegalStateException.class,
          () ->
              DeltaRangerStartupProbe.observe(
                  spark, fixture.engine, DeltaRangerStartupProbe.parse(input()).orElseThrow()));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void collectedRowsSurviveLaterVersionFailureWithoutPassing() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      SparkSession spark = mock(SparkSession.class);
      Dataset<Row> dataset = mock(Dataset.class);
      when(dataset.collectAsList()).thenReturn(List.of(RowFactory.create("a-one-masked")));
      when(spark.sql(anyString()))
          .thenAnswer(
              invocation -> {
                when(fixture.plugin.getRangerRoles()).thenReturn(null);
                return dataset;
              });
      DeltaRangerStartupProbe.Observation result =
          DeltaRangerStartupProbe.observe(
              spark, fixture.engine, DeltaRangerStartupProbe.parse(input()).orElseThrow());
      assertTrue(result.ready());
      assertTrue(result.queryAttempted());
      assertFalse(result.exactRows());
      assertTrue(result.failure().isPresent());
      assertNull(result.evidence().get("versionsAfter"));
      assertEquals(List.of(List.of("a-one-masked")), result.evidence().get("rows"));
    }
  }

  @Test
  void strictApplicationReaderRejectsDuplicateAndTrailingJsonBeforeDirectoryCreation()
      throws Exception {
    for (String payload : List.of("{\"startupProbe\":{},\"startupProbe\":{}}", "{} {}")) {
      Path request =
          Files.writeString(directory.resolve("request-" + payload.length() + ".json"), payload);
      Path output = directory.resolve("evidence-" + payload.length());
      assertThrows(
          Exception.class,
          () ->
              DeltaRangerDownloadedQuery.main(
                  new String[] {request.toString(), output.toString()}));
      assertFalse(Files.exists(output));
    }
  }

  @Test
  void failedPolicyAndRoleAttemptsAreTransportEvidenceButUpstreamCompletionIsUnknown() {
    String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
    String role = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "service";
    List<String> log = List.of(start(1, policy), end(1, policy), start(2, role), end(2, role));
    Map<String, Object> result = DeltaRangerStartupProbe.downloads(log, "service");
    assertEquals(2, result.get("attempts"));
    assertEquals(1, result.get("policyAttempts"));
    assertEquals(1, result.get("roleAttempts"));
    assertEquals(true, result.get("upstreamCompletionUnresolved"));
    assertEquals(0, result.get("upstreamHttp200"));
  }

  @Test
  void unmatchedOrDuplicatedGatewayEventsReject() {
    String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
    String first = start(1, policy);
    String last = end(1, policy);
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(first), "service"));
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(last), "service"));
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(first, last, last), "service"));
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(first, "invalid"), "service"));
    assertThrows(
        IllegalStateException.class,
        () ->
            DeltaRangerStartupProbe.downloads(
                List.of(first, last.replace("\"run\":\"r\"", "\"run\":\"other\"")), "service"));
    assertThrows(
        IllegalStateException.class,
        () ->
            DeltaRangerStartupProbe.downloads(
                List.of(first, last.replace("\"upstreamAttempted\":true,", "")), "service"));
  }

  @Test
  void notModifiedIsNotA200ButStillNeedsIndependentOutageProof() {
    String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
    String terminal =
        end(1, policy)
            .replace(
                "\"upstreamAttempted\":true,",
                "\"upstreamAttempted\":true,\"upstreamStatus\":304,");
    Map<String, Object> summary =
        DeltaRangerStartupProbe.downloads(List.of(start(1, policy), terminal), "service");
    assertEquals(0, summary.get("upstreamHttp200"));
    assertEquals(1, summary.get("policyAttempts"));
  }

  @Test
  void completedNotReadyRecordBindsDigestOnlyAfterCleanupAndAudit() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      when(fixture.plugin.getRangerRoles()).thenReturn(null);
      AtomicLong now = new AtomicLong();
      DeltaRangerStartupProbe.Observation observation =
          DeltaRangerStartupProbe.observe(
              mock(SparkSession.class),
              fixture.engine,
              DeltaRangerStartupProbe.parse(input()).orElseThrow(),
              now::get,
              millis -> now.addAndGet(Duration.ofMillis(millis).toNanos()));
      Path output = Files.createDirectory(directory.toRealPath().resolve("complete"));
      String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
      Map<String, Object> downloads =
          DeltaRangerStartupProbe.downloads(List.of(start(1, policy), end(1, policy)), "service");
      DeltaRangerStartupProbe.finish(output, observation, downloads, Map.of("mode", "empty"));
      byte[] bytes = Files.readAllBytes(output.resolve("startup-probe.json"));
      JsonNode marker = JSON.readTree(output.resolve("startup-probe-completed.json").toFile());
      assertEquals(
          DeltaRangerStartupProbe.sha256(bytes), marker.path("startupProbeSha256").asText());
      assertTrue(marker.path("observationCompleted").asBoolean());
      assertEquals("not-ready", JSON.readTree(bytes).path("readiness").asText());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaRangerStartupProbe.finish(
                  directory.toRealPath(),
                  observation,
                  downloads,
                  Map.of("mode", "empty", "readiness", "fake")));
    }
  }

  @Test
  void auditFailureDoesNotWriteCompletionMarker() throws Exception {
    Path output = Files.createDirectory(directory.toRealPath().resolve("failed-audit"));
    String policy = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
    Files.writeString(output.resolve("startup-observation.json"), "{\"readiness\":\"not-ready\"}");
    assertThrows(
        IllegalStateException.class,
        () -> DeltaRangerStartupProbe.downloads(List.of(start(1, policy)), "service"));
    assertTrue(Files.exists(output.resolve("startup-observation.json")));
    assertFalse(Files.exists(output.resolve("startup-probe-completed.json")));
  }

  private static String start(int attempt, String path) {
    return "{\"run\":\"r\",\"attempt\":"
        + attempt
        + ",\"method\":\"GET\",\"path\":\""
        + path
        + "\",\"started\":\"2026-09-27T00:00:00Z\"}";
  }

  private static String end(int attempt, String path) {
    String first = start(attempt, path);
    return first.substring(0, first.length() - 1)
        + ",\"upstreamAttempted\":true,\"transportFailure\":\"java.io.IOException\","
        + "\"upstreamCompletionUnresolved\":true,\"finished\":\"2026-09-27T00:00:01Z\","
        + "\"activeAfter\":0}";
  }

  private static JsonNode input() throws Exception {
    return JSON.readTree(
        "{\"catalogs\":[\"a\"],\"startupProbe\":{\"mode\":\"empty\",\"query\":{"
            + "\"catalog\":\"a\",\"schema\":\"db\",\"table\":\"t\","
            + "\"columns\":[\"secret\"],\"expectedRows\":[[\"a-one-masked\"],[null]]}}}");
  }
}
