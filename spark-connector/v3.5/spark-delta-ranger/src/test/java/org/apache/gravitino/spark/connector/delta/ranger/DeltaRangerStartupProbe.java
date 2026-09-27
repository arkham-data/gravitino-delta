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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Finite private startup observation for the downloaded-query test application. */
final class DeltaRangerStartupProbe {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final Set<String> PROBE_KEYS = Set.of("mode", "manifestSha256", "query");
  private static final Set<String> QUERY_KEYS =
      Set.of("catalog", "schema", "table", "columns", "expectedRows");
  private static final long DEADLINE = Duration.ofSeconds(60).toNanos();

  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  static final class Request {
    private final String mode;
    private final Optional<String> manifestSha256;
    private final String sql;
    private final JsonNode expectedRows;

    private Request(
        String mode, Optional<String> manifestSha256, String sql, JsonNode expectedRows) {
      this.mode = mode;
      this.manifestSha256 = manifestSha256;
      this.sql = sql;
      this.expectedRows = expectedRows.deepCopy();
    }

    String mode() {
      return mode;
    }

    Optional<String> manifestSha256() {
      return manifestSha256;
    }

    String sql() {
      return sql;
    }

    JsonNode expectedRows() {
      return expectedRows.deepCopy();
    }
  }

  static final class Observation {
    private final Map<String, Object> evidence;
    private final boolean exactRows;
    private final boolean ready;
    private final boolean queryAttempted;
    private final Optional<Throwable> failure;

    private Observation(
        Map<String, Object> evidence,
        boolean exactRows,
        boolean ready,
        boolean queryAttempted,
        @Nullable Throwable failure) {
      this.evidence = freezeMap(evidence);
      this.exactRows = exactRows;
      this.ready = ready;
      this.queryAttempted = queryAttempted;
      this.failure = Optional.ofNullable(failure);
    }

    Map<String, Object> evidence() {
      return evidence;
    }

    boolean exactRows() {
      return exactRows;
    }

    boolean ready() {
      return ready;
    }

    boolean queryAttempted() {
      return queryAttempted;
    }

    Optional<Throwable> failure() {
      return failure;
    }
  }

  private DeltaRangerStartupProbe() {}

  static Optional<Request> parse(JsonNode input) {
    if (input == null || !input.isObject()) {
      throw new IllegalArgumentException("Request must be an object");
    }
    if (!input.has("startupProbe")) {
      return Optional.empty();
    }
    JsonNode probe = input.get("startupProbe");
    exactKeys(probe, "startupProbe", Set.of("mode", "query"), PROBE_KEYS);
    String mode = text(probe, "mode");
    Optional<String> sha = Optional.empty();
    if ("snapshot".equals(mode)) {
      if (!probe.has("manifestSha256")) {
        throw new IllegalArgumentException("Snapshot requires manifest digest");
      }
      String value = text(probe, "manifestSha256");
      if (!value.matches("[0-9a-f]{64}")) {
        throw new IllegalArgumentException("Invalid manifest digest");
      }
      sha = Optional.of(value);
    } else if (!"empty".equals(mode) || probe.has("manifestSha256")) {
      throw new IllegalArgumentException("Invalid probe mode or digest");
    }
    JsonNode query = probe.path("query");
    exactKeys(query, "query", QUERY_KEYS, QUERY_KEYS);
    String catalog = identifier(query, "catalog");
    String schema = identifier(query, "schema");
    String table = identifier(query, "table");
    boolean visible = false;
    for (JsonNode node : input.path("catalogs")) {
      visible |= node.isTextual() && catalog.equals(node.textValue());
    }
    if (!visible) {
      throw new IllegalArgumentException("Catalog is not visible");
    }
    JsonNode columns = query.path("columns");
    if (!columns.isArray() || columns.size() < 1 || columns.size() > 16) {
      throw new IllegalArgumentException("Expected 1-16 columns");
    }
    List<String> names = new ArrayList<>();
    Set<String> distinct = new HashSet<>();
    for (JsonNode column : columns) {
      if (!column.isTextual() || !identifier(column.textValue())) {
        throw new IllegalArgumentException("Invalid column");
      }
      if (!distinct.add(column.textValue())) {
        throw new IllegalArgumentException("Duplicate column");
      }
      names.add("`" + column.textValue() + "`");
    }
    JsonNode expected = query.path("expectedRows");
    if (!expected.isArray() || expected.size() > 256) {
      throw new IllegalArgumentException("Invalid expected rows");
    }
    for (JsonNode row : expected) {
      if (!row.isArray() || row.size() != columns.size()) {
        throw new IllegalArgumentException("Invalid expected row width");
      }
      for (JsonNode cell : row) {
        if (cell.isContainerNode()) {
          throw new IllegalArgumentException("Expected scalar cell");
        }
      }
    }
    return Optional.of(
        new Request(
            mode,
            sha,
            "SELECT "
                + String.join(", ", names)
                + " FROM `"
                + catalog
                + "`.`"
                + schema
                + "`.`"
                + table
                + "`",
            expected));
  }

  static Observation observe(SparkSession spark, DeltaRangerPolicyEngine engine, Request request)
      throws Exception {
    return observe(spark, engine, request, System::nanoTime, Thread::sleep);
  }

  static Observation observe(
      SparkSession spark,
      DeltaRangerPolicyEngine engine,
      Request request,
      LongSupplier nanoTime,
      Sleeper sleeper)
      throws Exception {
    long started = nanoTime.getAsLong();
    SecurityException first = null;
    SecurityException last = null;
    DeltaRangerPolicyEngine.Versions readyVersions = null;
    int attempts = 0;
    do {
      attempts++;
      try {
        engine.requireReady();
        readyVersions = engine.versions();
        break;
      } catch (SecurityException incomplete) {
        if (first == null) {
          first = incomplete;
        }
        last = incomplete;
      }
      sleeper.sleep(100);
    } while (nanoTime.getAsLong() - started < DEADLINE);
    boolean ready = readyVersions != null;
    Map<String, Object> evidence = new LinkedHashMap<>();
    evidence.put("readiness", ready ? "ready" : "not-ready");
    evidence.put("readinessAttempts", attempts);
    evidence.put("elapsedNanos", nanoTime.getAsLong() - started);
    evidence.put("firstReadinessFailure", DeltaRangerDownloadedQuery.failureDetails(first));
    evidence.put("lastReadinessFailure", DeltaRangerDownloadedQuery.failureDetails(last));
    evidence.put("versions", ready ? versions(readyVersions) : null);
    evidence.put("sql", request.sql());
    evidence.put("expectedRows", request.expectedRows());
    evidence.put("queryAttempted", ready);
    evidence.put("versionsBefore", ready ? versions(readyVersions) : null);
    if (!ready) {
      evidence.put("rows", null);
      evidence.put("versionsAfter", null);
      evidence.put("queryFailure", null);
      evidence.put("exactRows", false);
      return new Observation(evidence, false, false, false, last);
    }
    List<List<Object>> rows = new ArrayList<>();
    boolean collected = false;
    Throwable queryFailure = null;
    Map<String, Object> after = null;
    try {
      for (Row row : spark.sql(request.sql()).collectAsList()) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
          values.add(row.isNullAt(i) ? null : row.get(i));
        }
        rows.add(Collections.unmodifiableList(values));
      }
      collected = true;
    } catch (Exception failure) {
      queryFailure = failure;
    }
    try {
      after = versions(engine.versions());
    } catch (Exception failure) {
      if (queryFailure == null) {
        queryFailure = failure;
      } else {
        queryFailure.addSuppressed(failure);
      }
    }
    boolean exact =
        queryFailure == null
            && multiset(JSON.valueToTree(rows)).equals(multiset(request.expectedRows()));
    evidence.put("rows", collected ? JSON.valueToTree(rows) : null);
    evidence.put("versionsAfter", after);
    evidence.put("queryFailure", DeltaRangerDownloadedQuery.failureDetails(queryFailure));
    evidence.put("exactRows", exact);
    return new Observation(evidence, exact, true, true, queryFailure);
  }

  static Map<String, Object> downloads(List<String> recordedEvents, String service) {
    Map<Long, JsonNode> starts = new LinkedHashMap<>();
    Set<Long> terminals = new HashSet<>();
    String run = null;
    int policy = 0;
    int role = 0;
    int other = 0;
    int attempted = 0;
    int unresolved = 0;
    int http200 = 0;
    int activeAtLastTerminal = -1;
    List<String> finished = new ArrayList<>();
    String policyPath = RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + service;
    String rolePath = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + service;
    try {
      for (String line : recordedEvents) {
        JsonNode event = JSON.readTree(line);
        if (event == null
            || !event.isObject()
            || !event.path("run").isTextual()
            || !event.path("attempt").isIntegralNumber()
            || event.path("attempt").asLong() <= 0
            || !event.path("method").isTextual()
            || !event.path("path").isTextual()
            || !event.path("started").isTextual()) {
          throw new IllegalStateException("Malformed gateway event");
        }
        String eventRun = event.path("run").asText();
        if (run == null) {
          run = eventRun;
        } else if (!run.equals(eventRun)) {
          throw new IllegalStateException("Mixed gateway runs");
        }
        long id = event.path("attempt").asLong();
        if (event.has("upstreamStatus") && event.path("upstreamStatus").asInt() == 200) {
          http200++;
        }
        if (event.has("finished")) {
          JsonNode start = starts.get(id);
          if (start == null
              || !terminals.add(id)
              || !event.path("finished").isTextual()
              || !event.path("activeAfter").isIntegralNumber()
              || event.path("activeAfter").asInt() < 0
              || !same(start, event, "run")
              || !same(start, event, "attempt")
              || !same(start, event, "method")
              || !same(start, event, "path")
              || !same(start, event, "started")) {
            throw new IllegalStateException("Unmatched or changed gateway terminal");
          }
          if (!event.path("upstreamAttempted").isBoolean()) {
            throw new IllegalStateException("Missing upstream attempt evidence");
          }
          if (event.path("upstreamAttempted").asBoolean()) {
            attempted++;
            if (policyPath.equals(event.path("path").asText())) {
              policy++;
            } else if (rolePath.equals(event.path("path").asText())) {
              role++;
            } else {
              other++;
            }
          }
          if (event.path("upstreamCompletionUnresolved").asBoolean(false)) {
            unresolved++;
          }
          activeAtLastTerminal = event.path("activeAfter").asInt();
          finished.add(event.path("finished").asText());
        } else if (starts.putIfAbsent(id, event) != null || terminals.contains(id)) {
          throw new IllegalStateException("Duplicate gateway start");
        }
      }
    } catch (Exception malformed) {
      if (malformed instanceof IllegalStateException) {
        throw (IllegalStateException) malformed;
      }
      throw new IllegalStateException("Malformed gateway log", malformed);
    }
    if (http200 != 0
        || starts.size() != terminals.size()
        || policy + role == 0
        || activeAtLastTerminal != 0) {
      throw new IllegalStateException("Incomplete or contaminated outage download evidence");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("run", run);
    result.put("attempts", starts.size());
    result.put("upstreamAttempts", attempted);
    result.put("policyAttempts", policy);
    result.put("roleAttempts", role);
    result.put("otherAttempts", other);
    result.put("upstreamHttp200", http200);
    result.put("upstreamCompletionUnresolved", unresolved != 0);
    result.put("finished", List.copyOf(finished));
    result.put(
        "rawLogSha256",
        sha256((String.join("\n", recordedEvents) + "\n").getBytes(StandardCharsets.UTF_8)));
    return Collections.unmodifiableMap(result);
  }

  static void finish(
      Path output,
      Observation observation,
      Map<String, Object> downloadSummary,
      Map<String, Object> context)
      throws Exception {
    if (observation.ready() && !observation.exactRows()) {
      throw new IllegalStateException(
          "Ready SELECT did not match expected rows", observation.failure().orElse(null));
    }
    Map<String, Object> record = new LinkedHashMap<>(context);
    putUnique(record, "schemaVersion", 1);
    for (Map.Entry<String, Object> entry : observation.evidence().entrySet()) {
      putUnique(record, entry.getKey(), entry.getValue());
    }
    if (!record.containsKey("mode")) {
      throw new IllegalArgumentException("Missing final evidence mode");
    }
    putUnique(record, "downloads", downloadSummary);
    putUnique(record, "cleanupComplete", true);
    byte[] bytes = JSON.writeValueAsBytes(record);
    Path result = output.resolve("startup-probe.json");
    Files.createFile(
        result, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    Files.write(result, bytes, StandardOpenOption.WRITE);
    Map<String, Object> marker = new LinkedHashMap<>();
    marker.put("schemaVersion", 1);
    marker.put("observationCompleted", true);
    marker.put("startupProbeSha256", sha256(bytes));
    Path completed = output.resolve("startup-probe-completed.json");
    Files.createFile(
        completed,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    Files.write(completed, JSON.writeValueAsBytes(marker), StandardOpenOption.WRITE);
  }

  static String sha256(byte[] bytes) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder hex = new StringBuilder();
      for (byte value : digest) {
        hex.append(String.format("%02x", value));
      }
      return hex.toString();
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static void exactKeys(
      JsonNode object, String what, Set<String> required, Set<String> allowed) {
    if (!object.isObject()) {
      throw new IllegalArgumentException(what + " must be an object");
    }
    Set<String> keys = new HashSet<>();
    object.fieldNames().forEachRemaining(keys::add);
    if (!keys.containsAll(required) || !allowed.containsAll(keys)) {
      throw new IllegalArgumentException("Invalid " + what + " keys");
    }
  }

  private static String text(JsonNode parent, String key) {
    JsonNode value = parent.path(key);
    if (!value.isTextual()) {
      throw new IllegalArgumentException("Expected text: " + key);
    }
    return value.textValue();
  }

  private static String identifier(JsonNode parent, String key) {
    String value = text(parent, key);
    if (!identifier(value)) {
      throw new IllegalArgumentException("Invalid identifier: " + key);
    }
    return value;
  }

  private static boolean identifier(String value) {
    return value.matches("[A-Za-z_][A-Za-z0-9_]{0,127}");
  }

  private static Map<String, Object> versions(DeltaRangerPolicyEngine.Versions value) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("service", value.service());
    record.put("definition", value.definition());
    record.put("policy", value.policy());
    record.put("role", value.role());
    return Collections.unmodifiableMap(record);
  }

  private static List<String> multiset(JsonNode rows) {
    List<String> values = new ArrayList<>();
    rows.forEach(row -> values.add(row.toString()));
    Collections.sort(values);
    return values;
  }

  private static boolean same(JsonNode first, JsonNode second, String key) {
    return first.path(key).equals(second.path(key));
  }

  private static void putUnique(Map<String, Object> record, String key, Object value) {
    if (record.containsKey(key)) {
      throw new IllegalArgumentException("Duplicate final evidence field: " + key);
    }
    record.put(key, value);
  }

  private static Map<String, Object> freezeMap(Map<String, Object> source) {
    Map<String, Object> copied = new LinkedHashMap<>();
    source.forEach((key, value) -> copied.put(key, freeze(value)));
    return Collections.unmodifiableMap(copied);
  }

  private static Object freeze(Object value) {
    if (value instanceof JsonNode) {
      return freeze(JSON.convertValue(value, Object.class));
    }
    if (value instanceof Map<?, ?>) {
      Map<String, Object> copied = new LinkedHashMap<>();
      ((Map<?, ?>) value).forEach((key, item) -> copied.put((String) key, freeze(item)));
      return Collections.unmodifiableMap(copied);
    }
    if (value instanceof List<?>) {
      List<Object> copied = new ArrayList<>();
      ((List<?>) value).forEach(item -> copied.add(freeze(item)));
      return Collections.unmodifiableList(copied);
    }
    return value;
  }
}
