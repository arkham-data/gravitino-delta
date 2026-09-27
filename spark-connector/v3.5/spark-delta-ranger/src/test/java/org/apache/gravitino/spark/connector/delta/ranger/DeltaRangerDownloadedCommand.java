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
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/** Strict, finite command grammar for the test-only downloaded-policy driver. */
final class DeltaRangerDownloadedCommand {
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9_-]{1,64}");

  enum Kind {
    FRESH,
    PREPARE,
    COLLECT,
    RELEASE,
    CLOSE
  }

  static final class Ledger {
    private final Set<String> basenames = new HashSet<>();

    void reserve(String id) {
      if (Set.of("ready", "failure", "completed").contains(id) || id.endsWith("-passed")) {
        throw new IllegalArgumentException("Reserved command ID");
      }
      if (basenames.contains(id) || basenames.contains(id + "-passed")) {
        throw new IllegalArgumentException("Command output filename collision");
      }
      basenames.add(id);
      basenames.add(id + "-passed");
    }
  }

  static final class Parsed {
    private final Kind kind;
    @Nullable private final String id;
    @Nullable private final String retentionId;
    @Nullable private final String sql;
    @Nullable private final String action;
    @Nullable private final JsonNode expectedRows;
    private final boolean denied;
    private final boolean hidden;
    @Nullable private final String catalog;
    @Nullable private final String expectedFailureContains;
    private final long minPolicy;
    private final long minRole;
    @Nullable private final DeltaRangerDownloadedSecurity.DownloadRequirement requiredDownloads;

    private Parsed(Kind kind, JsonNode node) {
      this.kind = kind;
      id = value(node, "id");
      retentionId = value(node, "retentionId");
      sql = value(node, "sql");
      action = value(node, "action");
      expectedRows = node.get("expectedRows");
      denied = node.path(kind == Kind.FRESH ? "denied" : "deniedAtEntry").asBoolean(false);
      hidden = node.path("hidden").asBoolean(false);
      catalog = value(node, "catalog");
      expectedFailureContains = value(node, "expectedFailureContains");
      minPolicy = node.path("minPolicy").asLong();
      minRole = node.path("minRole").asLong();
      requiredDownloads =
          kind == Kind.COLLECT
              ? DeltaRangerDownloadedSecurity.DownloadRequirement.valueOf(
                  node.get("requiredDownloads").textValue().toUpperCase(Locale.ROOT))
              : null;
    }

    Kind kind() {
      return kind;
    }

    @Nullable
    String id() {
      return id;
    }

    String retentionId() {
      require(Kind.PREPARE, Kind.COLLECT, Kind.RELEASE);
      return retentionId;
    }

    String sql() {
      require(Kind.FRESH, Kind.PREPARE);
      return sql;
    }

    String action() {
      require(Kind.COLLECT);
      return action;
    }

    Optional<JsonNode> expectedRows() {
      require(Kind.FRESH, Kind.COLLECT);
      return Optional.ofNullable(expectedRows);
    }

    boolean deniedAtEntry() {
      require(Kind.COLLECT);
      return denied;
    }

    boolean denied() {
      require(Kind.FRESH);
      return denied;
    }

    boolean hidden() {
      require(Kind.FRESH);
      return hidden;
    }

    String catalog() {
      require(Kind.FRESH);
      return catalog;
    }

    Optional<String> expectedFailureContains() {
      require(Kind.COLLECT);
      return Optional.ofNullable(expectedFailureContains);
    }

    DeltaRangerDownloadedSecurity.DownloadRequirement requiredDownloads() {
      require(Kind.COLLECT);
      return requiredDownloads;
    }

    long minPolicy() {
      require(Kind.FRESH, Kind.PREPARE, Kind.COLLECT);
      return minPolicy;
    }

    long minRole() {
      require(Kind.FRESH, Kind.PREPARE, Kind.COLLECT);
      return minRole;
    }

    private void require(Kind... allowed) {
      for (Kind candidate : allowed) {
        if (kind == candidate) {
          return;
        }
      }
      throw new IllegalStateException("Accessor does not apply to " + kind);
    }
  }

  static Parsed parse(String line, Ledger ledger) {
    final JsonNode node;
    try {
      node = JSON.readTree(line);
    } catch (IOException failure) {
      throw new IllegalArgumentException("Invalid command JSON", failure);
    }
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException("Command must be a JSON object");
    }
    if (node.has("close")) {
      if (node.size() != 1
          || !node.path("close").isBoolean()
          || !node.path("close").booleanValue()) {
        throw new IllegalArgumentException("Only exact close:true is a shutdown command");
      }
      return new Parsed(Kind.CLOSE, node);
    }
    Kind kind;
    if (!node.has("operation")) {
      kind = Kind.FRESH;
    } else if (!node.path("operation").isTextual()) {
      throw new IllegalArgumentException("Invalid operation");
    } else {
      switch (node.get("operation").textValue()) {
        case "prepare":
          kind = Kind.PREPARE;
          break;
        case "collect":
          kind = Kind.COLLECT;
          break;
        case "release":
          kind = Kind.RELEASE;
          break;
        default:
          throw new IllegalArgumentException("Unsupported operation");
      }
    }
    String id = string(node, "id");
    safe(id);
    if (kind != Kind.FRESH) {
      safe(string(node, "retentionId"));
    }
    if (kind == Kind.FRESH || kind == Kind.PREPARE || kind == Kind.COLLECT) {
      nonnegative(node, "minPolicy");
      nonnegative(node, "minRole");
    }
    switch (kind) {
      case FRESH:
        string(node, "sql");
        boolean freshDenied = truth(node, "denied");
        boolean hidden = truth(node, "hidden");
        boolean rows = node.path("expectedRows").isArray();
        if ((freshDenied ? 1 : 0) + (hidden ? 1 : 0) + (rows ? 1 : 0) != 1) {
          throw new IllegalArgumentException("One fresh outcome is required");
        }
        if (hidden) {
          string(node, "catalog");
        }
        keys(
            node,
            hidden
                ? Set.of("id", "sql", "minPolicy", "minRole", "hidden", "catalog")
                : freshDenied
                    ? Set.of("id", "sql", "minPolicy", "minRole", "denied")
                    : Set.of("id", "sql", "minPolicy", "minRole", "expectedRows"));
        break;
      case PREPARE:
        string(node, "sql");
        keys(node, Set.of("id", "operation", "retentionId", "sql", "minPolicy", "minRole"));
        break;
      case COLLECT:
        String action = string(node, "action");
        if (!action.equals("collect") && !action.equals("collectAsList")) {
          throw new IllegalArgumentException("Unsupported retained action");
        }
        String download = string(node, "requiredDownloads");
        if (!Set.of("none", "policy", "role", "both").contains(download)) {
          throw new IllegalArgumentException("Unsupported download requirement");
        }
        boolean entryDenied = truth(node, "deniedAtEntry");
        if (entryDenied) {
          if (string(node, "expectedFailureContains").isEmpty()) {
            throw new IllegalArgumentException("Expected failure fragment is empty");
          }
          keys(
              node,
              Set.of(
                  "id",
                  "operation",
                  "retentionId",
                  "action",
                  "deniedAtEntry",
                  "expectedFailureContains",
                  "minPolicy",
                  "minRole",
                  "requiredDownloads"));
        } else {
          if (!node.path("expectedRows").isArray()) {
            throw new IllegalArgumentException("Expected rows are required");
          }
          keys(
              node,
              Set.of(
                  "id",
                  "operation",
                  "retentionId",
                  "action",
                  "expectedRows",
                  "minPolicy",
                  "minRole",
                  "requiredDownloads"));
        }
        break;
      case RELEASE:
        keys(node, Set.of("id", "operation", "retentionId"));
        break;
      default:
        throw new IllegalStateException("Unexpected command kind");
    }
    ledger.reserve(id);
    return new Parsed(kind, node);
  }

  private static void keys(JsonNode node, Set<String> expected) {
    if (node.size() != expected.size()) {
      throw new IllegalArgumentException("Unexpected or missing command field");
    }
    Iterator<String> names = node.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (!expected.contains(name) || node.get(name).isNull()) {
        throw new IllegalArgumentException("Unexpected or null command field");
      }
    }
  }

  private static void safe(String value) {
    if (!SAFE.matcher(value).matches()) {
      throw new IllegalArgumentException("Unsafe command ID");
    }
  }

  private static String string(JsonNode node, String name) {
    JsonNode value = node.get(name);
    if (value == null || !value.isTextual()) {
      throw new IllegalArgumentException("String field required: " + name);
    }
    return value.textValue();
  }

  private static void nonnegative(JsonNode node, String name) {
    JsonNode value = node.get(name);
    if (value == null
        || !value.isIntegralNumber()
        || !value.canConvertToLong()
        || value.longValue() < 0) {
      throw new IllegalArgumentException("Nonnegative integral field required: " + name);
    }
  }

  private static boolean truth(JsonNode node, String name) {
    JsonNode value = node.get(name);
    if (value == null) {
      return false;
    }
    if (!value.isBoolean() || !value.booleanValue()) {
      throw new IllegalArgumentException("Only true is accepted for " + name);
    }
    return true;
  }

  @Nullable
  private static String value(JsonNode node, String name) {
    JsonNode value = node.get(name);
    return value == null ? null : value.textValue();
  }

  private DeltaRangerDownloadedCommand() {}
}
