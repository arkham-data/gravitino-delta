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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaOperationMarker {
  private static final String SCHEMA = "{\"type\":\"struct\",\"fields\":[]}";
  private static final String CANONICAL =
      "{\"version\":1,\"operation\":\"CREATE\",\"id\":0,\"generation\":0,\"token\":\"token\",\"location\":\"s3://bucket/table\",\"schema\":{\"fields\":[],\"type\":\"struct\"}}";

  @Test
  void canonicalEnvelopeBindsAllCreateFields() {
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(0, 0, "token", "s3://bucket/table", SCHEMA);
    Assertions.assertEquals(CANONICAL, marker.encode());
    Assertions.assertEquals(CANONICAL, DeltaOperationMarker.decode(CANONICAL).encode());
    Assertions.assertEquals(
        CANONICAL,
        DeltaOperationMarker.create(
                0, 0, "token", "s3://bucket/table", " {\"fields\":[],\"type\":\"struct\"} ")
            .encode());
  }

  @Test
  void replacementBindsActualBase() {
    String encoded =
        DeltaOperationMarker.replace(
                Long.MAX_VALUE, Long.MAX_VALUE, "token", "s3://bucket/table", SCHEMA, 4, "old-uuid")
            .encode();
    Assertions.assertTrue(encoded.endsWith(",\"baseVersion\":4,\"baseUuid\":\"old-uuid\"}"));
    Assertions.assertEquals(encoded, DeltaOperationMarker.decode(encoded).encode());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "gravitino.registration.operation",
        "Gravitino.Registration.Operation",
        "option.gravitino.registration.operation",
        "OpTiOn.GRAVITINO.REGISTRATION.OPERATION",
        "option.OPTION.option.gravitino.registration.operation"
      })
  void rejectsOwnedAliases(String key) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> marker().injectInto(Collections.singletonMap(key, "user")));
  }

  @Test
  void injectionPreservesInputAndReturnsIndependentImmutableMap() {
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("Option.custom", " value ");
    Map<String, String> injected = marker().injectInto(properties);
    Assertions.assertEquals(Collections.singletonMap("Option.custom", " value "), properties);
    Assertions.assertEquals(" value ", injected.get("Option.custom"));
    Assertions.assertEquals(CANONICAL, injected.get("gravitino.registration.operation"));
    properties.put("later", "change");
    Assertions.assertFalse(injected.containsKey("later"));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> injected.put("other", "value"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> marker().injectInto(Collections.singletonMap(null, "v")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> marker().injectInto(Collections.singletonMap("k", null)));
    Assertions.assertThrows(IllegalArgumentException.class, () -> marker().injectInto(null));
  }

  @Test
  void rejectsMalformedNoncanonicalAndUnboundEnvelopes() {
    String[] invalid = {
      "",
      "null",
      "[]",
      CANONICAL + "{}",
      CANONICAL + " ",
      CANONICAL.replace("\"version\":1", "\"version\":1,\"version\":1"),
      CANONICAL.replace("\"version\":1", "\"version\":2"),
      CANONICAL.replace("\"id\":0", "\"id\":9223372036854775808"),
      CANONICAL.replace("\"id\":0", "\"id\":-1"),
      CANONICAL.replace("\"id\":0", "\"id\":0.0"),
      CANONICAL.replace("\"id\":0", "\"id\":\"0\""),
      CANONICAL.replace("\"id\":0,", ""),
      CANONICAL.replace("\"id\":0", "\"id\":0,\"extra\":0"),
      CANONICAL.replace("\"token\":\"token\"", "\"token\":null"),
      CANONICAL.replace("\"token\":\"token\"", "\"token\":\" \""),
      CANONICAL.replace("CREATE", "REPLACE"),
      CANONICAL.replace("CREATE", "WRITE"),
      CANONICAL.replace("\"id\":0,\"generation\":0", "\"generation\":0,\"id\":0"),
      CANONICAL.replace("\"id\":0", "\"\\u0069d\":0"),
      CANONICAL.replace("\"fields\":[]", "\"fields\":[],\"fields\":[]"),
      CANONICAL.replace("\"schema\":{\"fields\":[],\"type\":\"struct\"}", "\"schema\":\"wrong\""),
      CANONICAL.substring(0, CANONICAL.length() - 1) + ",\"baseVersion\":0,\"baseUuid\":\"x\"}"
    };
    for (String value : invalid) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> DeltaOperationMarker.decode(value), value);
    }
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DeltaOperationMarker.decode(null));
  }

  @Test
  void rejectsFactoryMisuseAndResourceExhaustion() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DeltaOperationMarker.create(-1, 0, "t", "s3://b/t", SCHEMA));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DeltaOperationMarker.create(0, -1, "t", "s3://b/t", SCHEMA));
    for (String text : new String[] {null, "", " \t", repeat("x", 9000)}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> DeltaOperationMarker.create(0, 0, text, "s3://b/t", SCHEMA));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> DeltaOperationMarker.create(0, 0, "t", text, SCHEMA));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> DeltaOperationMarker.replace(0, 0, "t", "s3://b/t", SCHEMA, 0, text));
    }
    for (long base : new long[] {-1, Long.MAX_VALUE}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> DeltaOperationMarker.replace(0, 0, "t", "s3://b/t", SCHEMA, base, "u"));
    }
    for (String schema :
        new String[] {
          null,
          "",
          "null",
          "[]",
          "{}{}",
          "{\"a\":0,\"a\":1}",
          "{\"x\":" + repeat("[", 80) + "0" + repeat("]", 80) + "}",
          "{\"x\":\"" + repeat("x", 140000) + "\"}"
        }) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> DeltaOperationMarker.create(0, 0, "t", "s3://b/t", schema));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DeltaOperationMarker.decode(repeat("x", 270000)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DeltaOperationMarker.create(0, 0, repeat("é", 5000), "s3://b/t", SCHEMA));
  }

  @Test
  void factoryDepthLimitLeavesRoomForCanonicalEnvelope() {
    String boundary = "{\"x\":" + repeat("[", 62) + "0" + repeat("]", 62) + "}";
    String encoded = DeltaOperationMarker.create(0, 0, "t", "s3://b/t", boundary).encode();
    Assertions.assertEquals(encoded, DeltaOperationMarker.decode(encoded).encode());
    String excessive = "{\"x\":" + repeat("[", 63) + "0" + repeat("]", 63) + "}";
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DeltaOperationMarker.create(0, 0, "t", "s3://b/t", excessive));
  }

  private static DeltaOperationMarker marker() {
    return DeltaOperationMarker.create(0, 0, "token", "s3://bucket/table", SCHEMA);
  }

  private static String repeat(String text, int count) {
    return String.join("", Collections.nCopies(count, text));
  }
}
