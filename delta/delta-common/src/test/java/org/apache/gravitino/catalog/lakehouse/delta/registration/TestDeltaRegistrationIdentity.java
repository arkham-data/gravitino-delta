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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRegistrationIdentity {
  @Test
  void immutableValue() {
    DeltaRegistrationIdentity a = DeltaRegistrationIdentity.of(1, Long.MAX_VALUE, "op");
    DeltaRegistrationIdentity b = DeltaRegistrationIdentity.of(1, Long.MAX_VALUE, "op");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertEquals(1, a.id());
    assertEquals(Long.MAX_VALUE, a.generation());
    assertEquals("op", a.token());
    assertNotEquals(a, DeltaRegistrationIdentity.of(2, Long.MAX_VALUE, "op"));
    assertNotEquals(a, DeltaRegistrationIdentity.of(1, 0, "op"));
    assertNotEquals(a, DeltaRegistrationIdentity.of(1, Long.MAX_VALUE, "other"));
    assertNotEquals(a, null);
    assertNotEquals(a, "op");
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(0, 0, "op"));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(-1, 0, "op"));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(1, -1, "op"));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(1, 0, null));
    assertDoesNotThrow(() -> DeltaRegistrationIdentity.of(Long.MAX_VALUE, 0, "é".repeat(512)));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(1, 0, "é".repeat(513)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\t", "\u2003", "\uD800", "\uDC00"})
  void rejectsBadToken(String token) {
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationIdentity.of(1, 0, token));
  }
}
