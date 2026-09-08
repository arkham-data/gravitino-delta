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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;

/**
 * Immutable sanitized registration receipt. Only PUBLISHED describes a readable table; evidence on
 * terminal states is historical and never grants visibility or recovery authority.
 */
public final class DeltaRegistrationReceipt {
  private static final String PREFIX = "delta.registration.receipt.";
  private static final Set<String> FIELDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "id",
                  "generation",
                  "token",
                  "operation",
                  "state",
                  "runId",
                  "evidence",
                  "reason",
                  "marker",
                  "location",
                  "nativeId",
                  "nativeVersion")));
  private static final Set<String> NATIVE_FAILURES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "NATIVE_EVIDENCE_AMBIGUOUS",
                  "NATIVE_READ_UNAVAILABLE",
                  "ADOPTION_BASELINE_CHANGED",
                  "NATIVE_ID_CHANGED",
                  "NATIVE_VERSION_REGRESSION",
                  "NATIVE_METADATA_INCONSISTENT",
                  "UNREPRESENTABLE_NATIVE_METADATA")));

  /** Exposed registration lifecycle state. */
  public enum State {
    /** Native CREATE reserved, not readable. */
    RESERVED,
    /** Existing snapshot adoption pending, not readable. */
    ADOPTING,
    /** Native REPLACE reserved, not readable. */
    REPLACING,
    /** Verified native snapshot exposed as readable. */
    PUBLISHED,
    /** Unresolved or invalidated registration, not readable. */
    QUARANTINED,
    /** Abandoned registration, not readable. */
    ABANDONED
  }

  /** Evidence category, independent of present table visibility. */
  public enum Evidence {
    /** No verified native evidence yet. */
    NONE,
    /** Exact native CREATE or REPLACE commit was verified. */
    EXACT_NATIVE_COMMIT,
    /** Existing native snapshot was verified for adoption. */
    EXISTING_NATIVE_SNAPSHOT,
    /** Native evidence is unresolved. */
    UNRESOLVED
  }

  private final DeltaRegistrationIdentity identity;
  private final Operation operation;
  private final State state;
  private final String runId;
  private final Evidence evidence;
  private final String reason;
  private final Optional<DeltaOperationMarker> marker;
  private final Optional<String> location;
  private final Optional<String> nativeId;
  private final OptionalLong nativeVersion;
  private final Map<String, String> properties;

  private DeltaRegistrationReceipt(
      DeltaRegistrationIdentity identity,
      Operation operation,
      State state,
      String runId,
      Evidence evidence,
      String reason,
      Optional<DeltaOperationMarker> marker,
      Optional<String> location,
      Optional<String> nativeId,
      OptionalLong nativeVersion) {
    if (identity == null
        || operation == null
        || state == null
        || evidence == null
        || reason == null
        || marker == null
        || location == null
        || nativeId == null
        || nativeVersion == null) {
      throw invalid();
    }
    if (operation != Operation.RESERVE
        && operation != Operation.ADOPT
        && operation != Operation.BEGIN_REPLACE) {
      throw invalid();
    }
    DeltaRegistrationIdentity.requiredText(runId, 4096);
    location.ifPresent(
        value -> DeltaRegistrationIdentity.requiredText(value, DeltaJson.MAX_TEXT_BYTES));
    nativeId.ifPresent(
        value -> DeltaRegistrationIdentity.requiredText(value, DeltaJson.MAX_TEXT_BYTES));
    if (nativeId.isPresent() != nativeVersion.isPresent()
        || nativeVersion.isPresent() && nativeVersion.getAsLong() < 0) {
      throw invalid();
    }
    this.identity = identity;
    this.operation = operation;
    this.state = state;
    this.runId = runId;
    this.evidence = evidence;
    this.reason = reason;
    this.marker = marker;
    this.location = location;
    this.nativeId = nativeId;
    this.nativeVersion = nativeVersion;
    validateState();
    marker.ifPresent(
        value -> {
          if (value.id() != identity.id()
              || value.generation() != identity.generation()
              || !value.token().equals(identity.token())
              || operation == Operation.ADOPT
              || value.operation()
                  != (operation == Operation.RESERVE
                      ? DeltaOperationMarker.Operation.CREATE
                      : DeltaOperationMarker.Operation.REPLACE)) {
            throw invalid();
          }
        });
    Map<String, String> result = new LinkedHashMap<>();
    result.put(PREFIX + "id", Long.toString(identity.id()));
    result.put(PREFIX + "generation", Long.toString(identity.generation()));
    result.put(PREFIX + "token", identity.token());
    result.put(PREFIX + "operation", operation.name());
    result.put(PREFIX + "state", state.name());
    result.put(PREFIX + "runId", runId);
    result.put(PREFIX + "evidence", evidence.name());
    result.put(PREFIX + "reason", reason);
    marker.ifPresent(value -> result.put(PREFIX + "marker", value.encode()));
    location.ifPresent(value -> result.put(PREFIX + "location", value));
    nativeId.ifPresent(value -> result.put(PREFIX + "nativeId", value));
    if (nativeVersion.isPresent()) {
      result.put(PREFIX + "nativeVersion", Long.toString(nativeVersion.getAsLong()));
    }
    properties = Collections.unmodifiableMap(result);
  }

  /**
   * Creates a sanitized receipt, validating the state, opening operation, evidence and marker.
   *
   * @param identity positive authoritative registration identity
   * @param operation opening RESERVE, ADOPT or BEGIN_REPLACE command, never a later status request
   * @param state current registration state
   * @param runId recorded writer run, or retired writer run for adoption
   * @param evidence sanitized native evidence category
   * @param reason closed server reason code, never upstream exception text
   * @param marker matching server CREATE/REPLACE marker for pending native operations
   * @param location verified location, present only for PUBLISHED
   * @param nativeId opaque verified native identity, present only for PUBLISHED
   * @param nativeVersion verified nonnegative native version, present only for PUBLISHED
   * @return immutable receipt
   * @throws IllegalArgumentException for invalid fields or state combinations
   */
  public static DeltaRegistrationReceipt of(
      DeltaRegistrationIdentity identity,
      Operation operation,
      State state,
      String runId,
      Evidence evidence,
      String reason,
      Optional<DeltaOperationMarker> marker,
      Optional<String> location,
      Optional<String> nativeId,
      OptionalLong nativeVersion) {
    return new DeltaRegistrationReceipt(
        identity,
        operation,
        state,
        runId,
        evidence,
        reason,
        marker,
        location,
        nativeId,
        nativeVersion);
  }

  /**
   * Reads reserved receipt properties and ignores unrelated table properties.
   *
   * @param properties property map containing all required receipt fields
   * @return immutable sanitized receipt
   * @throws IllegalArgumentException for unknown reserved suffixes, nulls or malformed receipt data
   */
  public static DeltaRegistrationReceipt fromProperties(Map<String, String> properties) {
    if (properties == null) {
      throw invalid();
    }
    Map<String, String> receipt = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey() == null) {
        throw invalid();
      }
      if (!entry.getKey().startsWith(PREFIX)) {
        continue;
      }
      String field = entry.getKey().substring(PREFIX.length());
      if (!FIELDS.contains(field) || entry.getValue() == null) {
        throw invalid();
      }
      receipt.put(field, entry.getValue());
    }
    String markerText = receipt.get("marker");
    String versionText = receipt.get("nativeVersion");
    return of(
        DeltaRegistrationIdentity.of(
            decimal(required(receipt, "id")),
            decimal(required(receipt, "generation")),
            required(receipt, "token")),
        Operation.valueOf(required(receipt, "operation")),
        State.valueOf(required(receipt, "state")),
        required(receipt, "runId"),
        Evidence.valueOf(required(receipt, "evidence")),
        required(receipt, "reason"),
        markerText == null
            ? Optional.empty()
            : Optional.of(DeltaOperationMarker.decode(markerText)),
        Optional.ofNullable(receipt.get("location")),
        Optional.ofNullable(receipt.get("nativeId")),
        versionText == null ? OptionalLong.empty() : OptionalLong.of(decimal(versionText)));
  }

  /**
   * @return immutable reserved receipt properties, with no unrelated native configuration
   */
  public Map<String, String> toProperties() {
    return properties;
  }
  /**
   * @return authoritative current registration identity
   */
  public DeltaRegistrationIdentity identity() {
    return identity;
  }
  /**
   * @return opening RESERVE, ADOPT or BEGIN_REPLACE operation
   */
  public Operation operation() {
    return operation;
  }
  /**
   * @return lifecycle state; only PUBLISHED is readable
   */
  public State state() {
    return state;
  }
  /**
   * @return recorded writer run, or retired writer run for ADOPT
   */
  public String runId() {
    return runId;
  }
  /**
   * @return evidence category, historical on terminal states
   */
  public Evidence evidence() {
    return evidence;
  }
  /**
   * @return closed sanitized server reason code
   */
  public String reason() {
    return reason;
  }
  /**
   * @return matching server marker for a pending native operation, never a foreign ADOPT marker
   */
  public Optional<DeltaOperationMarker> marker() {
    return marker;
  }
  /**
   * @return verified readable table location for PUBLISHED only
   */
  public Optional<String> location() {
    return location;
  }
  /**
   * @return opaque native ID for PUBLISHED only
   */
  public Optional<String> nativeId() {
    return nativeId;
  }
  /**
   * @return nonnegative native version for PUBLISHED only
   */
  public OptionalLong nativeVersion() {
    return nativeVersion;
  }

  private void validateState() {
    boolean nativeOperation = operation != Operation.ADOPT;
    Evidence observed =
        nativeOperation ? Evidence.EXACT_NATIVE_COMMIT : Evidence.EXISTING_NATIVE_SNAPSHOT;
    boolean noNative = !location.isPresent() && !nativeId.isPresent() && !nativeVersion.isPresent();
    boolean pendingMarker = marker.isPresent() == nativeOperation;
    boolean valid;
    switch (state) {
      case RESERVED:
      case ADOPTING:
      case REPLACING:
        State pendingState =
            operation == Operation.RESERVE
                ? State.RESERVED
                : operation == Operation.ADOPT ? State.ADOPTING : State.REPLACING;
        valid =
            state == pendingState
                && evidence == Evidence.NONE
                && pendingMarker
                && noNative
                && (reason.equals("NONE")
                    || state != State.REPLACING && reason.equals("OWNER_INITIALIZATION_REQUIRED"));
        break;
      case PUBLISHED:
        valid =
            evidence == observed
                && !marker.isPresent()
                && location.isPresent()
                && nativeId.isPresent()
                && nativeVersion.isPresent()
                && reason.equals("NONE");
        break;
      case QUARANTINED:
        valid =
            noNative
                && NATIVE_FAILURES.contains(reason)
                && (evidence == Evidence.UNRESOLVED && pendingMarker
                    || evidence == observed && !marker.isPresent());
        break;
      case ABANDONED:
        valid =
            noNative
                && !marker.isPresent()
                && (evidence == Evidence.UNRESOLVED || evidence == observed)
                && reason.equals("ABANDONED_BY_RECOVERY");
        break;
      default:
        valid = false;
    }
    if (!valid) {
      throw invalid();
    }
  }

  private static String required(Map<String, String> properties, String field) {
    String value = properties.get(field);
    if (value == null) {
      throw invalid();
    }
    return value;
  }

  private static long decimal(String value) {
    if (value.length() > 19 || !value.matches("0|[1-9][0-9]*")) {
      throw invalid();
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw invalid();
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_REGISTRATION_RECEIPT");
  }
}
