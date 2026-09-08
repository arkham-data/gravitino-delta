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

package org.apache.gravitino.catalog.lakehouse.delta.registration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Immutable bounded registration command. Actor authority and authoritative native markers are
 * supplied by the server, never by this wire envelope. Tokens only correlate an operation.
 */
public final class DeltaRegistrationCommand {
  /** Property carrying a registration command in table POST/PUT requests. */
  public static final String COMMAND_PROPERTY = "delta.registration.command";

  private static final int MAX_COMMAND_BYTES = 512 * 1024;

  /** Registration command category. */
  public enum Operation {
    /** Reserve a native CREATE. */
    RESERVE,
    /** Adopt an existing native snapshot. */
    ADOPT,
    /** Reserve a native REPLACE. */
    BEGIN_REPLACE,
    /** Request native evidence verification and publication. */
    PUBLISH,
    /** Read status by expected identity. */
    STATUS,
    /** Read status by current operation token and recorded writer run. */
    STATUS_BY_TOKEN,
    /** Privileged administrative status read without a caller identity. */
    ADMINISTRATIVE_STATUS,
    /** Record a handoff for the stored operation run. */
    HANDOFF,
    /** Request recovery or abandonment. */
    ABORT
  }

  private final Operation operation;
  private final Optional<DeltaRegistrationIdentity> expected;
  private final Optional<String> token;
  private final Optional<String> replacementToken;
  private final Optional<String> runId;
  private final Optional<String> location;
  private final Optional<String> exactSchemaJson;
  private final Optional<DeltaSchemaContract> schemaContract;
  private final Optional<String> reference;
  private final String encoded;

  private DeltaRegistrationCommand(ObjectNode node) {
    if (DeltaJson.number(node, "version") != 1) {
      throw invalid();
    }
    operation = Operation.valueOf(DeltaJson.text(node, "operation"));
    Set<String> fields = new HashSet<>(Arrays.asList("version", "operation"));
    switch (operation) {
      case RESERVE:
        fields.addAll(Arrays.asList("token", "runId"));
        schemaField(node, fields);
        break;
      case ADOPT:
        fields.addAll(Arrays.asList("token", "location", "runId", "reference"));
        break;
      case BEGIN_REPLACE:
        fields.addAll(Arrays.asList("expected", "replacementToken", "runId"));
        schemaField(node, fields);
        break;
      case PUBLISH:
      case STATUS:
      case ABORT:
        fields.add("expected");
        break;
      case STATUS_BY_TOKEN:
        fields.addAll(Arrays.asList("token", "runId"));
        break;
      case HANDOFF:
        fields.addAll(Arrays.asList("expected", "runId", "reference"));
        break;
      case ADMINISTRATIVE_STATUS:
        break;
      default:
        throw invalid();
    }
    requireFields(node, fields);
    if (fields.contains("expected")) {
      JsonNode identity = node.get("expected");
      requireFields(identity, new HashSet<>(Arrays.asList("id", "generation", "token")));
      expected =
          Optional.of(
              DeltaRegistrationIdentity.of(
                  DeltaJson.number(identity, "id"),
                  DeltaJson.number(identity, "generation"),
                  DeltaJson.text(identity, "token")));
    } else {
      expected = Optional.empty();
    }
    token = text(node, "token", 1024);
    replacementToken = text(node, "replacementToken", 1024);
    runId = text(node, "runId", 4096);
    location = text(node, "location", DeltaJson.MAX_TEXT_BYTES);
    reference = text(node, "reference", 4096);
    if (fields.contains("schema")) {
      exactSchemaJson = Optional.of(DeltaJson.schema(node.get("schema").toString()));
      schemaContract = Optional.empty();
    } else if (fields.contains("schemaContract")) {
      schemaContract = Optional.of(DeltaSchemaContract.fromJson(node.get("schemaContract")));
      exactSchemaJson = Optional.empty();
    } else {
      exactSchemaJson = Optional.empty();
      schemaContract = Optional.empty();
    }
    ObjectNode wire = envelope(operation);
    expected.ifPresent(value -> wire.set("expected", identityJson(value)));
    token.ifPresent(value -> wire.put("token", value));
    replacementToken.ifPresent(value -> wire.put("replacementToken", value));
    // Keep operation-specific envelope order stable; schema object keys use the existing helpers.
    location.ifPresent(value -> wire.put("location", value));
    runId.ifPresent(value -> wire.put("runId", value));
    reference.ifPresent(value -> wire.put("reference", value));
    exactSchemaJson.ifPresent(
        value -> wire.set("schema", DeltaJson.object(value, DeltaJson.MAX_SCHEMA_BYTES)));
    schemaContract.ifPresent(value -> wire.set("schemaContract", value.toJson()));
    encoded = wire.toString();
    DeltaJson.boundedText(encoded, MAX_COMMAND_BYTES);
  }

  /**
   * Reserves native CREATE with the unchanged v1 exact-schema binding.
   *
   * @param token operation correlation token
   * @param runId writer run
   * @param exactSchemaJson bounded schema object
   * @return immutable reservation command
   */
  public static DeltaRegistrationCommand reserve(
      String token, String runId, String exactSchemaJson) {
    ObjectNode node = reserveNode(token, runId);
    node.set(
        "schema", DeltaJson.object(DeltaJson.schema(exactSchemaJson), DeltaJson.MAX_SCHEMA_BYTES));
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Reserves native CREATE with a closed v2 schema binding; command wire version remains 1.
   *
   * @param token operation correlation token
   * @param runId writer run
   * @param schemaContract closed schema binding
   * @return immutable reservation command
   */
  public static DeltaRegistrationCommand reserve(
      String token, String runId, DeltaSchemaContract schemaContract) {
    ObjectNode node = reserveNode(token, runId);
    node.set("schemaContract", contractJson(schemaContract));
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Requests adoption after the recorded writer has retired.
   *
   * @param token operation correlation token
   * @param location existing native location
   * @param retiredRunId retired writer run, carried as runId
   * @param cutoverReference cutover reference, carried as reference
   * @return immutable adoption command
   */
  public static DeltaRegistrationCommand adopt(
      String token, String location, String retiredRunId, String cutoverReference) {
    ObjectNode node = envelope(Operation.ADOPT);
    node.put("token", token);
    node.put("location", location);
    node.put("runId", retiredRunId);
    node.put("reference", cutoverReference);
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Reserves REPLACE with an exact schema; the server captures the native base.
   *
   * @param expected current expected registration identity
   * @param newToken replacement correlation token
   * @param runId writer run
   * @param exactSchemaJson bounded schema object
   * @return immutable replacement command
   */
  public static DeltaRegistrationCommand beginReplace(
      DeltaRegistrationIdentity expected, String newToken, String runId, String exactSchemaJson) {
    ObjectNode node = replaceNode(expected, newToken, runId);
    node.set(
        "schema", DeltaJson.object(DeltaJson.schema(exactSchemaJson), DeltaJson.MAX_SCHEMA_BYTES));
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Reserves REPLACE with a closed v2 schema binding.
   *
   * @param expected current expected registration identity
   * @param newToken replacement correlation token
   * @param runId writer run
   * @param schemaContract closed schema binding
   * @return immutable replacement command
   */
  public static DeltaRegistrationCommand beginReplace(
      DeltaRegistrationIdentity expected,
      String newToken,
      String runId,
      DeltaSchemaContract schemaContract) {
    ObjectNode node = replaceNode(expected, newToken, runId);
    node.set("schemaContract", contractJson(schemaContract));
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Requests publication subject to server verification of native evidence.
   *
   * @param expected expected operation identity
   * @return immutable publication command
   */
  public static DeltaRegistrationCommand publish(DeltaRegistrationIdentity expected) {
    return new DeltaRegistrationCommand(expectedNode(Operation.PUBLISH, expected));
  }

  /**
   * Reads operation status without mutation.
   *
   * @param expected expected operation identity
   * @return immutable status command
   */
  public static DeltaRegistrationCommand status(DeltaRegistrationIdentity expected) {
    return new DeltaRegistrationCommand(expectedNode(Operation.STATUS, expected));
  }

  /**
   * Reads status using the current token and recorded writer run; neither grants authority.
   *
   * @param token current operation token
   * @param writerRunId recorded writer run, or retired writer run for ADOPT
   * @return immutable read-only status command
   */
  public static DeltaRegistrationCommand statusByToken(String token, String writerRunId) {
    ObjectNode node = envelope(Operation.STATUS_BY_TOKEN);
    node.put("token", token);
    node.put("runId", writerRunId);
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Requests administrative status. The server must require privileged authorization.
   *
   * @return immutable status command with no expected identity or token
   */
  public static DeltaRegistrationCommand administrativeStatus() {
    return new DeltaRegistrationCommand(envelope(Operation.ADMINISTRATIVE_STATUS));
  }

  /**
   * Records a handoff for the existing stored operation run.
   *
   * @param expected expected operation identity
   * @param runId existing stored writer run
   * @param reference handoff reference
   * @return immutable handoff command
   */
  public static DeltaRegistrationCommand handoff(
      DeltaRegistrationIdentity expected, String runId, String reference) {
    ObjectNode node = expectedNode(Operation.HANDOFF, expected);
    node.put("runId", runId);
    node.put("reference", reference);
    return new DeltaRegistrationCommand(node);
  }

  /**
   * Requests server-controlled recovery or abandonment.
   *
   * @param expected expected operation identity
   * @return immutable abort command
   */
  public static DeltaRegistrationCommand abort(DeltaRegistrationIdentity expected) {
    return new DeltaRegistrationCommand(expectedNode(Operation.ABORT, expected));
  }

  /**
   * Decodes a strict operation-specific shape, accepting insignificant order and whitespace.
   *
   * @param encoded command JSON, at most 512 KiB UTF-8
   * @return immutable canonical command
   * @throws IllegalArgumentException for malformed, duplicate, unknown, oversized or invalid fields
   */
  public static DeltaRegistrationCommand decode(String encoded) {
    ObjectNode node = DeltaJson.object(encoded, MAX_COMMAND_BYTES);
    validateNumbers(encoded);
    return new DeltaRegistrationCommand(node);
  }

  /**
   * @return canonical command JSON
   */
  public String encode() {
    return encoded;
  }
  /**
   * @return requested operation
   */
  public Operation operation() {
    return operation;
  }
  /**
   * @return expected identity when applicable
   */
  public Optional<DeltaRegistrationIdentity> expected() {
    return expected;
  }
  /**
   * @return token for RESERVE, ADOPT or STATUS_BY_TOKEN only
   */
  public Optional<String> token() {
    return token;
  }
  /**
   * @return new token for BEGIN_REPLACE only
   */
  public Optional<String> replacementToken() {
    return replacementToken;
  }
  /**
   * @return writer run, including the retired writer run for ADOPT, when applicable
   */
  public Optional<String> runId() {
    return runId;
  }
  /**
   * @return location for ADOPT only
   */
  public Optional<String> location() {
    return location;
  }
  /**
   * @return canonical v1 exact schema, mutually exclusive with schemaContract
   */
  public Optional<String> exactSchemaJson() {
    return exactSchemaJson;
  }
  /**
   * @return closed v2 binding, mutually exclusive with exactSchemaJson
   */
  public Optional<DeltaSchemaContract> schemaContract() {
    return schemaContract;
  }
  /**
   * @return cutover or handoff reference when applicable
   */
  public Optional<String> reference() {
    return reference;
  }

  private static ObjectNode envelope(Operation operation) {
    ObjectNode node = DeltaJson.newObject();
    node.put("version", 1);
    node.put("operation", operation.name());
    return node;
  }

  private static ObjectNode reserveNode(String token, String runId) {
    ObjectNode node = envelope(Operation.RESERVE);
    node.put("token", token);
    node.put("runId", runId);
    return node;
  }

  private static ObjectNode replaceNode(
      DeltaRegistrationIdentity expected, String token, String runId) {
    ObjectNode node = expectedNode(Operation.BEGIN_REPLACE, expected);
    node.put("replacementToken", token);
    node.put("runId", runId);
    return node;
  }

  private static ObjectNode expectedNode(Operation operation, DeltaRegistrationIdentity expected) {
    ObjectNode node = envelope(operation);
    node.set("expected", identityJson(expected));
    return node;
  }

  private static ObjectNode identityJson(DeltaRegistrationIdentity identity) {
    if (identity == null) {
      throw invalid();
    }
    ObjectNode node = DeltaJson.newObject();
    node.put("id", identity.id());
    node.put("generation", identity.generation());
    node.put("token", identity.token());
    return node;
  }

  private static ObjectNode contractJson(DeltaSchemaContract contract) {
    if (contract == null) {
      throw invalid();
    }
    return contract.toJson();
  }

  private static Optional<String> text(ObjectNode node, String field, int limit) {
    if (!node.has(field)) {
      return Optional.empty();
    }
    String value = DeltaJson.text(node, field);
    DeltaRegistrationIdentity.requiredText(value, limit);
    return Optional.of(value);
  }

  private static void schemaField(ObjectNode node, Set<String> fields) {
    if (node.has("schema") == node.has("schemaContract")) {
      throw invalid();
    }
    fields.add(node.has("schema") ? "schema" : "schemaContract");
  }

  private static void requireFields(JsonNode node, Set<String> fields) {
    if (node == null || !node.isObject() || node.size() != fields.size()) {
      throw invalid();
    }
    for (String field : fields) {
      if (!node.hasNonNull(field)) {
        throw invalid();
      }
    }
  }
  // Tree conversion loses the spelling of negative zero. Inspect only envelope identity numerics,
  // preserving every existing schema numeric rule, including decimal metadata precision and scale.
  private static void validateNumbers(String encoded) {
    try (JsonParser parser = DeltaJson.FACTORY.createParser(encoded)) {
      while (parser.nextToken() != null) {
        JsonToken token = parser.currentToken();
        if (!token.isNumeric()) {
          continue;
        }
        JsonStreamContext context = parser.getParsingContext();
        JsonStreamContext parent = context.getParent();
        String name = parser.currentName();
        boolean version = parent != null && parent.inRoot() && "version".equals(name);
        boolean identity =
            parent != null
                && parent.getParent() != null
                && parent.getParent().inRoot()
                && "expected".equals(parent.getCurrentName())
                && ("id".equals(name) || "generation".equals(name));
        if ((version || identity)
            && (token != JsonToken.VALUE_NUMBER_INT
                || !parser.getText().matches("0|[1-9][0-9]*"))) {
          throw invalid();
        }
      }
    } catch (IOException e) {
      throw invalid();
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_REGISTRATION_COMMAND");
  }
}
