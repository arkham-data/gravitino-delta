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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.Evidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRegistrationReceipt {
  private static final String PREFIX = "delta.registration.receipt.";
  private static final DeltaRegistrationIdentity ID = DeltaRegistrationIdentity.of(7, 2, "op");
  private static final String SCHEMA = "{\"type\":\"struct\",\"fields\":[]}";
  private static final String[] FAILURES = {
    "NATIVE_EVIDENCE_AMBIGUOUS",
    "NATIVE_READ_UNAVAILABLE",
    "ADOPTION_BASELINE_CHANGED",
    "NATIVE_ID_CHANGED",
    "NATIVE_VERSION_REGRESSION",
    "NATIVE_METADATA_INCONSISTENT",
    "UNREPRESENTABLE_NATIVE_METADATA"
  };

  @ParameterizedTest
  @MethodSource("validReceipts")
  void roundtripAndSanitizedProjection(DeltaRegistrationReceipt receipt) {
    Map<String, String> properties = new HashMap<>(receipt.toProperties());
    properties.put("unrelated", "secret");
    DeltaRegistrationReceipt read = DeltaRegistrationReceipt.fromProperties(properties);
    assertEquals(receipt.toProperties(), read.toProperties());
    assertEquals(ID, read.identity());
    assertEquals(receipt.operation(), read.operation());
    assertEquals(receipt.state(), read.state());
    assertEquals("run", read.runId());
    assertEquals(receipt.evidence(), read.evidence());
    assertEquals(receipt.reason(), read.reason());
    assertEquals(receipt.location(), read.location());
    assertEquals(receipt.nativeId(), read.nativeId());
    assertEquals(receipt.nativeVersion(), read.nativeVersion());
    assertEquals(
        receipt.marker().map(DeltaOperationMarker::encode),
        read.marker().map(DeltaOperationMarker::encode));
    assertFalse(read.toProperties().containsKey("unrelated"));
    assertThrows(UnsupportedOperationException.class, () -> read.toProperties().put("x", "y"));
    properties.put(PREFIX + "state", "corrupt");
    assertEquals(receipt.toProperties(), read.toProperties());
    if (receipt.state() != State.PUBLISHED) {
      assertFalse(read.location().isPresent());
      assertFalse(read.nativeId().isPresent());
      assertFalse(read.nativeVersion().isPresent());
    }
  }

  @Test
  void exhaustiveOperationStateEvidenceMatrix() {
    int accepted = 0;
    for (Operation operation : Operation.values()) {
      for (State state : State.values()) {
        for (Evidence evidence : Evidence.values()) {
          for (boolean hasMarker : new boolean[] {false, true}) {
            for (boolean nativeFields : new boolean[] {false, true}) {
              boolean opening =
                  operation == Operation.RESERVE
                      || operation == Operation.BEGIN_REPLACE
                      || operation == Operation.ADOPT;
              boolean adopt = operation == Operation.ADOPT;
              boolean observed =
                  evidence
                      == (adopt ? Evidence.EXISTING_NATIVE_SNAPSHOT : Evidence.EXACT_NATIVE_COMMIT);
              boolean valid =
                  opening
                      && ((state == State.RESERVED && operation == Operation.RESERVE
                                  || state == State.REPLACING
                                      && operation == Operation.BEGIN_REPLACE
                                  || state == State.ADOPTING && adopt)
                              && evidence == Evidence.NONE
                              && hasMarker == !adopt
                              && !nativeFields
                          || state == State.PUBLISHED && observed && !hasMarker && nativeFields
                          || state == State.QUARANTINED
                              && !nativeFields
                              && (evidence == Evidence.UNRESOLVED && hasMarker == !adopt
                                  || observed && !hasMarker)
                          || state == State.ABANDONED
                              && !nativeFields
                              && !hasMarker
                              && (evidence == Evidence.UNRESOLVED || observed));
              String reason =
                  state == State.QUARANTINED
                      ? FAILURES[0]
                      : state == State.ABANDONED ? "ABANDONED_BY_RECOVERY" : "NONE";
              String label =
                  operation + "/" + state + "/" + evidence + "/" + hasMarker + "/" + nativeFields;
              if (valid) {
                assertDoesNotThrow(
                    () -> receipt(operation, state, evidence, reason, hasMarker, nativeFields),
                    label);
                accepted++;
              } else {
                assertThrows(
                    IllegalArgumentException.class,
                    () -> receipt(operation, state, evidence, reason, hasMarker, nativeFields),
                    label);
              }
            }
          }
        }
      }
    }
    assertEquals(18, accepted);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"id", "generation", "token", "operation", "state", "runId", "evidence", "reason"})
  void requiredReceiptFields(String field) {
    Map<String, String> p =
        new HashMap<>(
            receipt(Operation.RESERVE, State.RESERVED, Evidence.NONE, "NONE", true, false)
                .toProperties());
    p.remove(PREFIX + field);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
    p.put(PREFIX + field, null);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
    p.put(PREFIX + field, "");
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"+1", "01", "-0", "-1", "1.0", "1e0", "9223372036854775808", " 1", "1 ", "١"})
  void canonicalDecimalProperties(String value) {
    for (String field : new String[] {"id", "generation", "nativeVersion"}) {
      Map<String, String> p =
          new HashMap<>(
              receipt(
                      Operation.RESERVE,
                      State.PUBLISHED,
                      Evidence.EXACT_NATIVE_COMMIT,
                      "NONE",
                      false,
                      true)
                  .toProperties());
      p.put(PREFIX + field, value);
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
    }
  }

  @Test
  void reasonCodesAndMalformedProperties() {
    for (String reason : FAILURES) {
      assertDoesNotThrow(
          () ->
              receipt(
                  Operation.RESERVE, State.QUARANTINED, Evidence.UNRESOLVED, reason, true, false));
    }
    assertDoesNotThrow(
        () ->
            receipt(
                Operation.RESERVE,
                State.RESERVED,
                Evidence.NONE,
                "OWNER_INITIALIZATION_REQUIRED",
                true,
                false));
    assertDoesNotThrow(
        () ->
            receipt(
                Operation.ADOPT,
                State.ADOPTING,
                Evidence.NONE,
                "OWNER_INITIALIZATION_REQUIRED",
                false,
                false));
    for (String reason :
        new String[] {
          "NONE",
          "OWNER_INITIALIZATION_REQUIRED",
          "ABANDONED_BY_RECOVERY",
          "raw exception secret",
          "INVALID_ADOPTION_MARKER",
          "REGISTRATION_CHANGED"
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              receipt(
                  Operation.RESERVE, State.QUARANTINED, Evidence.UNRESOLVED, reason, true, false));
    }
    for (String reason :
        new String[] {"OWNER_INITIALIZATION_REQUIRED", FAILURES[0], "ABANDONED_BY_RECOVERY"}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              receipt(
                  Operation.RESERVE,
                  State.PUBLISHED,
                  Evidence.EXACT_NATIVE_COMMIT,
                  reason,
                  false,
                  true));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            receipt(
                Operation.BEGIN_REPLACE,
                State.REPLACING,
                Evidence.NONE,
                "OWNER_INITIALIZATION_REQUIRED",
                true,
                false));
    assertThrows(
        IllegalArgumentException.class,
        () -> receipt(Operation.ADOPT, State.ABANDONED, Evidence.UNRESOLVED, "NONE", false, false));
    Map<String, String> p =
        new HashMap<>(
            receipt(
                    Operation.RESERVE,
                    State.PUBLISHED,
                    Evidence.EXACT_NATIVE_COMMIT,
                    "NONE",
                    false,
                    true)
                .toProperties());
    p.put(PREFIX + "unknown", "secret");
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
    p.remove(PREFIX + "unknown");
    for (String field : new String[] {"location", "nativeId", "nativeVersion"}) {
      Map<String, String> copy = new HashMap<>(p);
      copy.remove(PREFIX + field);
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(copy));
      copy.put(PREFIX + field, null);
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(copy));
    }
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationReceipt.fromProperties(Collections.emptyMap()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRegistrationReceipt.of(
                null,
                Operation.ADOPT,
                State.ADOPTING,
                "run",
                Evidence.NONE,
                "NONE",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()));
    p.put(PREFIX + "nativeVersion", Long.toString(Long.MAX_VALUE));
    p.put(PREFIX + "generation", Long.toString(Long.MAX_VALUE));
    p.put(PREFIX + "id", Long.toString(Long.MAX_VALUE));
    assertDoesNotThrow(() -> DeltaRegistrationReceipt.fromProperties(p));
    p.put(PREFIX + "id", "0");
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
  }

  @Test
  void markerMustBindAllIdentityAndOperationFields() {
    for (DeltaOperationMarker marker :
        new DeltaOperationMarker[] {
          DeltaOperationMarker.create(8, 2, "op", "loc", SCHEMA),
          DeltaOperationMarker.create(7, 3, "op", "loc", SCHEMA),
          DeltaOperationMarker.create(7, 2, "other", "loc", SCHEMA),
          DeltaOperationMarker.replace(7, 2, "op", "loc", SCHEMA, 0, "uuid")
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaRegistrationReceipt.of(
                  ID,
                  Operation.RESERVE,
                  State.RESERVED,
                  "run",
                  Evidence.NONE,
                  "NONE",
                  Optional.of(marker),
                  Optional.empty(),
                  Optional.empty(),
                  OptionalLong.empty()));
    }
    Map<String, String> p =
        new HashMap<>(
            receipt(Operation.RESERVE, State.RESERVED, Evidence.NONE, "NONE", true, false)
                .toProperties());
    p.put(PREFIX + "marker", "x".repeat(256 * 1024 + 1));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(p));
  }

  @Test
  void receiptTextBoundsAndNullOptionals() {
    Map<String, String> p =
        new HashMap<>(
            receipt(
                    Operation.ADOPT,
                    State.PUBLISHED,
                    Evidence.EXISTING_NATIVE_SNAPSHOT,
                    "NONE",
                    false,
                    true)
                .toProperties());
    for (String field : new String[] {"token", "runId", "location", "nativeId"}) {
      int max = field.equals("token") ? 1024 : field.equals("runId") ? 4096 : 8192;
      Map<String, String> copy = new HashMap<>(p);
      copy.put(PREFIX + field, "é".repeat(max / 2));
      assertDoesNotThrow(() -> DeltaRegistrationReceipt.fromProperties(copy));
      copy.put(PREFIX + field, "é".repeat(max / 2 + 1));
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(copy));
      for (String bad : new String[] {"", " ", "\u2003", "\uD800", "\uDC00"}) {
        copy.put(PREFIX + field, bad);
        assertThrows(
            IllegalArgumentException.class, () -> DeltaRegistrationReceipt.fromProperties(copy));
      }
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRegistrationReceipt.of(
                ID,
                Operation.ADOPT,
                State.ADOPTING,
                "run",
                Evidence.NONE,
                "NONE",
                null,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty()));
  }

  private static DeltaRegistrationReceipt receipt(
      Operation op,
      State state,
      Evidence evidence,
      String reason,
      boolean marker,
      boolean nativeFields) {
    DeltaOperationMarker m =
        op == Operation.BEGIN_REPLACE
            ? DeltaOperationMarker.replace(7, 2, "op", "loc", SCHEMA, 1, "base")
            : DeltaOperationMarker.create(7, 2, "op", "loc", SCHEMA);
    return DeltaRegistrationReceipt.of(
        ID,
        op,
        state,
        "run",
        evidence,
        reason,
        marker ? Optional.of(m) : Optional.empty(),
        nativeFields ? Optional.of("loc") : Optional.empty(),
        nativeFields ? Optional.of("uuid") : Optional.empty(),
        nativeFields ? OptionalLong.of(0) : OptionalLong.empty());
  }

  private static Stream<DeltaRegistrationReceipt> validReceipts() {
    List<DeltaRegistrationReceipt> values = new ArrayList<>();
    for (Operation op :
        new Operation[] {Operation.RESERVE, Operation.ADOPT, Operation.BEGIN_REPLACE}) {
      boolean nativeOp = op != Operation.ADOPT;
      Evidence observed =
          nativeOp ? Evidence.EXACT_NATIVE_COMMIT : Evidence.EXISTING_NATIVE_SNAPSHOT;
      values.add(
          receipt(
              op,
              op == Operation.RESERVE
                  ? State.RESERVED
                  : op == Operation.ADOPT ? State.ADOPTING : State.REPLACING,
              Evidence.NONE,
              "NONE",
              nativeOp,
              false));
      values.add(receipt(op, State.PUBLISHED, observed, "NONE", false, true));
      values.add(receipt(op, State.QUARANTINED, Evidence.UNRESOLVED, FAILURES[0], nativeOp, false));
      values.add(receipt(op, State.QUARANTINED, observed, FAILURES[0], false, false));
      values.add(
          receipt(op, State.ABANDONED, Evidence.UNRESOLVED, "ABANDONED_BY_RECOVERY", false, false));
      values.add(receipt(op, State.ABANDONED, observed, "ABANDONED_BY_RECOVERY", false, false));
    }
    return values.stream();
  }
}
