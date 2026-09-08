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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Immutable correlation envelope for one reserved native Delta CREATE or REPLACE.
 *
 * <p>This marker binds identity, generation, token, location, schema and replacement base. It is
 * not actor authority, a stale-writer fence, or a digest of query semantics, rows or all table
 * properties. Locations and replacement bases must originate from the enclosing trusted
 * registration workflow.
 */
public final class DeltaOperationMarker {
  /** Metadata configuration key reserved for registration correlation. */
  public static final String PROPERTY_KEY = "gravitino.registration.operation";

  /** Native operation category bound by this marker. */
  public enum Operation {
    /** A fresh table at native version zero. */
    CREATE,
    /** A replacement immediately after the trusted actual base version. */
    REPLACE
  }

  private final long id;
  private final long generation;
  private final String token;
  private final String normalizedLocation;
  private final String schemaJson;
  private final Operation operation;
  private final OptionalLong baseVersion;
  private final Optional<String> baseUuid;
  private final String encoded;

  private DeltaOperationMarker(
      long id,
      long generation,
      String token,
      String normalizedLocation,
      String schemaJson,
      Operation operation,
      OptionalLong baseVersion,
      Optional<String> baseUuid) {
    if (id < 0 || generation < 0) {
      throw new IllegalArgumentException("NEGATIVE_IDENTITY");
    }
    DeltaJson.requiredText(token);
    DeltaJson.requiredText(normalizedLocation);
    this.id = id;
    this.generation = generation;
    this.token = token;
    this.normalizedLocation = normalizedLocation;
    this.schemaJson = DeltaJson.schema(schemaJson);
    this.operation = operation;
    this.baseVersion = baseVersion;
    this.baseUuid = baseUuid;
    ObjectNode envelope = DeltaJson.newObject();
    envelope.put("version", 1);
    envelope.put("operation", operation.name());
    envelope.put("id", id);
    envelope.put("generation", generation);
    envelope.put("token", token);
    envelope.put("location", normalizedLocation);
    envelope.set("schema", DeltaJson.object(this.schemaJson, DeltaJson.MAX_SCHEMA_BYTES));
    if (operation == Operation.REPLACE) {
      envelope.put("baseVersion", baseVersion.getAsLong());
      envelope.put("baseUuid", baseUuid.get());
    }
    encoded = envelope.toString();
    DeltaJson.boundedText(encoded, DeltaJson.MAX_MARKER_BYTES);
  }

  /**
   * Creates a marker for native version zero.
   *
   * @param id nonnegative authoritative registration identity
   * @param generation nonnegative reserved generation
   * @param token nonblank reservation token
   * @param normalizedLocation durable normalized table location, supplied by the trusted caller
   * @param schemaJson schema JSON object; object key order alone is ignored
   * @return immutable CREATE marker
   * @throws IllegalArgumentException if fields or resource bounds are invalid
   */
  public static DeltaOperationMarker create(
      long id, long generation, String token, String normalizedLocation, String schemaJson) {
    return new DeltaOperationMarker(
        id,
        generation,
        token,
        normalizedLocation,
        schemaJson,
        Operation.CREATE,
        OptionalLong.empty(),
        Optional.empty());
  }

  /**
   * Creates a marker bound to the actual native base captured before BEGIN_REPLACE.
   *
   * @param id nonnegative authoritative registration identity
   * @param generation nonnegative reserved generation
   * @param token nonblank reservation token
   * @param normalizedLocation durable normalized table location
   * @param schemaJson replacement schema JSON object
   * @param baseVersion trusted actual nonnegative base version; must allow adding one without
   *     overflow
   * @param baseUuid trusted actual nonblank base UUID whose published lineage the caller checked
   * @return immutable REPLACE marker
   * @throws IllegalArgumentException if fields or resource bounds are invalid
   */
  public static DeltaOperationMarker replace(
      long id,
      long generation,
      String token,
      String normalizedLocation,
      String schemaJson,
      long baseVersion,
      String baseUuid) {
    if (baseVersion < 0 || baseVersion == Long.MAX_VALUE) {
      throw new IllegalArgumentException("INVALID_BASE_VERSION");
    }
    DeltaJson.requiredText(baseUuid);
    return new DeltaOperationMarker(
        id,
        generation,
        token,
        normalizedLocation,
        schemaJson,
        Operation.REPLACE,
        OptionalLong.of(baseVersion),
        Optional.of(baseUuid));
  }

  /**
   * Returns compact canonical JSON with fixed envelope field order and recursively sorted schema
   * keys.
   *
   * @return metadata configuration string value, bounded to 256 KiB UTF-8
   */
  public String encode() {
    return encoded;
  }

  /**
   * Decodes only this contract's exact canonical encoding, with no duplicate or additional fields.
   *
   * @param encoded canonical marker JSON string
   * @return immutable decoded marker
   * @throws IllegalArgumentException for malformed, noncanonical, unsupported or oversized input
   */
  public static DeltaOperationMarker decode(String encoded) {
    ObjectNode envelope = DeltaJson.object(encoded, DeltaJson.MAX_MARKER_BYTES);
    long version = DeltaJson.number(envelope, "version");
    String operation = DeltaJson.text(envelope, "operation");
    JsonNode schema = envelope.get("schema");
    if (version != 1 || schema == null || !schema.isObject()) {
      throw new IllegalArgumentException("INVALID_MARKER");
    }
    long id = DeltaJson.number(envelope, "id");
    long generation = DeltaJson.number(envelope, "generation");
    String token = DeltaJson.text(envelope, "token");
    String location = DeltaJson.text(envelope, "location");
    DeltaOperationMarker marker;
    if ("CREATE".equals(operation) && envelope.size() == 7) {
      marker = create(id, generation, token, location, schema.toString());
    } else if ("REPLACE".equals(operation) && envelope.size() == 9) {
      marker =
          replace(
              id,
              generation,
              token,
              location,
              schema.toString(),
              DeltaJson.number(envelope, "baseVersion"),
              DeltaJson.text(envelope, "baseUuid"));
    } else {
      throw new IllegalArgumentException("INVALID_MARKER_FIELDS");
    }
    if (!marker.encode().equals(encoded)) {
      throw new IllegalArgumentException("NONCANONICAL_MARKER");
    }
    return marker;
  }

  /**
   * Copies properties and injects the marker after reservation and before native stage creation.
   *
   * <p>Rejects case-insensitive owned-key collisions including repeated leading {@code option.}
   * prefixes. The adapter must independently guard writer options, ALTER and other write entry
   * points.
   *
   * @param properties nonnull properties with no null keys or values
   * @return new unmodifiable map preserving all unrelated entries exactly
   * @throws IllegalArgumentException for a null property or reserved-key collision
   */
  public Map<String, String> injectInto(Map<String, String> properties) {
    if (properties == null) {
      throw new IllegalArgumentException("NULL_PROPERTIES");
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new IllegalArgumentException("NULL_PROPERTY");
      }
      String key = entry.getKey().toLowerCase(Locale.ROOT);
      while (key.startsWith("option.")) {
        key = key.substring("option.".length());
      }
      if (PROPERTY_KEY.equals(key)) {
        throw new IllegalArgumentException("RESERVED_PROPERTY");
      }
      result.put(entry.getKey(), entry.getValue());
    }
    result.put(PROPERTY_KEY, encoded);
    return Collections.unmodifiableMap(result);
  }

  /**
   * @return nonnegative registration identity
   */
  public long id() {
    return id;
  }

  /**
   * @return nonnegative reserved generation
   */
  public long generation() {
    return generation;
  }

  /**
   * @return reservation correlation token; not actor authority
   */
  public String token() {
    return token;
  }

  /**
   * @return caller-supplied durable normalized location
   */
  public String normalizedLocation() {
    return normalizedLocation;
  }

  /**
   * @return schema JSON with recursively sorted object keys and preserved array order
   */
  public String schemaJson() {
    return schemaJson;
  }

  /**
   * @return bound CREATE or REPLACE category
   */
  public Operation operation() {
    return operation;
  }

  /**
   * @return trusted actual base version for REPLACE, empty for CREATE
   */
  public OptionalLong baseVersion() {
    return baseVersion;
  }

  /**
   * @return trusted actual base UUID for REPLACE, empty for CREATE
   */
  public Optional<String> baseUuid() {
    return baseUuid;
  }
}
