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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRegistrationCommand {
  private static final String SCHEMA = "{\"type\":\"struct\",\"fields\":[]}";
  private static final DeltaRegistrationIdentity ID = DeltaRegistrationIdentity.of(7, 2, "op");
  private static final DeltaSchemaContract CONTRACT =
      DeltaSchemaContract.spark35Delta33NameMapping(
          DeltaSchemaContract.OperationMode.SCHEMA_ONLY, SCHEMA);

  @ParameterizedTest
  @MethodSource("commands")
  void roundtripAndExactShapes(DeltaRegistrationCommand command) {
    String encoded = command.encode();
    DeltaRegistrationCommand decoded = DeltaRegistrationCommand.decode(encoded);
    assertEquals(encoded, decoded.encode());
    assertEquals(command.operation(), decoded.operation());
    assertEquals(command.expected(), decoded.expected());
    assertEquals(command.token(), decoded.token());
    assertEquals(command.replacementToken(), decoded.replacementToken());
    assertEquals(command.runId(), decoded.runId());
    assertEquals(command.reference(), decoded.reference());
    assertEquals(command.location(), decoded.location());
    assertEquals(command.exactSchemaJson(), decoded.exactSchemaJson());
    assertEquals(command.schemaContract().isPresent(), decoded.schemaContract().isPresent());
    ObjectNode node = json(encoded);
    assertEquals(1, node.get("version").intValue());
    assertEquals(encoded, DeltaRegistrationCommand.decode(" \n" + encoded + " ").encode());
    node.put("actor", "admin");
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(node.toString()));
    ObjectNode original = json(encoded);
    original
        .fieldNames()
        .forEachRemaining(
            field -> {
              ObjectNode missing = original.deepCopy();
              missing.remove(field);
              assertThrows(
                  IllegalArgumentException.class,
                  () -> DeltaRegistrationCommand.decode(missing.toString()),
                  field);
              ObjectNode nulled = original.deepCopy();
              nulled.putNull(field);
              assertThrows(
                  IllegalArgumentException.class,
                  () -> DeltaRegistrationCommand.decode(nulled.toString()),
                  field);
            });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRegistrationCommand.decode(
                encoded.replace("\"version\":1", "\"version\":1,\"version\":1")));
  }

  @Test
  void gettersAndCanonicalOrder() {
    DeltaRegistrationCommand c = DeltaRegistrationCommand.reserve("op-1", "run-17", SCHEMA);
    assertEquals("delta.registration.command", DeltaRegistrationCommand.COMMAND_PROPERTY);
    assertEquals(
        "{\"version\":1,\"operation\":\"RESERVE\",\"token\":\"op-1\",\"runId\":\"run-17\",\"schema\":{\"fields\":[],\"type\":\"struct\"}}",
        c.encode());
    assertFalse(c.location().isPresent());
    assertFalse(c.expected().isPresent());
    assertFalse(c.replacementToken().isPresent());
    assertFalse(c.reference().isPresent());
    assertFalse(c.schemaContract().isPresent());
    assertTrue(
        DeltaRegistrationCommand.reserve("op", "run", CONTRACT).schemaContract().isPresent());
    assertFalse(
        DeltaRegistrationCommand.reserve("op", "run", CONTRACT).exactSchemaJson().isPresent());
    assertFalse(
        DeltaRegistrationCommand.beginReplace(ID, "new", "run", SCHEMA).token().isPresent());
    assertEquals(
        "retired",
        DeltaRegistrationCommand.adopt("op", "s3://bucket/table", "retired", "cutover")
            .runId()
            .get());
    assertEquals("writer", DeltaRegistrationCommand.statusByToken("op", "writer").runId().get());
    assertFalse(DeltaRegistrationCommand.administrativeStatus().expected().isPresent());
    assertFalse(DeltaRegistrationCommand.administrativeStatus().token().isPresent());
    assertEquals(
        c.encode(),
        DeltaRegistrationCommand.decode(
                "{\"schema\": {\"type\":\"struct\",\"fields\":[]},\"runId\":\"run-17\",\"operation\":\"RESERVE\",\"token\":\"op-1\",\"version\":1}")
            .encode());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "actor",
        "baseUuid",
        "baseVersion",
        "candidateVersion",
        "location",
        "expected",
        "replacementToken",
        "reference",
        "schemaContract"
      })
  void rejectsReserveAuthorityAndDuplicateBinding(String field) {
    ObjectNode node = json(DeltaRegistrationCommand.reserve("op", "run", SCHEMA).encode());
    node.put(field, "injected");
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(node.toString()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "1.0",
        "1e0",
        "9223372036854775808",
        "-1",
        "01",
        "+1",
        "\"1\"",
        "null",
        "true",
        "-0"
      })
  void rejectsNumericRepresentations(String number) {
    String status = DeltaRegistrationCommand.status(ID).encode();
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.decode(status.replace("\"id\":7", "\"id\":" + number)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRegistrationCommand.decode(
                status.replace("\"generation\":2", "\"generation\":" + number)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRegistrationCommand.decode(
                status.replace("\"version\":1", "\"version\":" + number)));
  }

  @Test
  void rejectsMalformedShapesAndSchemas() {
    for (String bad :
        new String[] {
          "[]",
          "{}",
          "null",
          "{} {}",
          "\uD800",
          " ".repeat(512 * 1024 + 1),
          "{\"version\":1,\"operation\":\"RESERVE\",\"actor\":\"admin\"}"
        }) {
      assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(bad));
    }
    for (String field : new String[] {"kind", "featurePolicy"}) {
      ObjectNode node = json(DeltaRegistrationCommand.reserve("op", "run", CONTRACT).encode());
      ((ObjectNode) node.get("schemaContract")).put(field, "UNKNOWN");
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(node.toString()));
    }
    ObjectNode both = json(DeltaRegistrationCommand.reserve("op", "run", CONTRACT).encode());
    both.set("schema", json(SCHEMA));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(both.toString()));
    ObjectNode expected = json(DeltaRegistrationCommand.status(ID).encode());
    ((ObjectNode) expected.get("expected")).put("actor", "admin");
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRegistrationCommand.decode(expected.toString()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.reserve("op", "run", "{\"a\":1,\"a\":2}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.reserve("op", "run", (String) null));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.reserve("op", "run", (DeltaSchemaContract) null));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationCommand.publish(null));
  }

  @Test
  void exactUtf8Bounds() {
    assertDoesNotThrow(
        () ->
            DeltaRegistrationCommand.adopt(
                "é".repeat(512), "é".repeat(4096), "é".repeat(2048), "é".repeat(2048)));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.adopt("é".repeat(513), "loc", "run", "ref"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.adopt("op", "é".repeat(4097), "run", "ref"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.adopt("op", "loc", "é".repeat(2049), "ref"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.adopt("op", "loc", "run", "é".repeat(2049)));
    for (String bad : new String[] {"", " ", "\u2003", "\uD800", "\uDC00", null}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRegistrationCommand.adopt("op", bad, "run", "ref"));
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationCommand.statusByToken("op", bad));
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationCommand.handoff(ID, "run", bad));
    }
    String schema = "{\"v\":\"" + "a".repeat(128 * 1024 - 8) + "\"}";
    assertDoesNotThrow(() -> DeltaRegistrationCommand.reserve("op", "run", schema));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.reserve("op", "run", schema.replace("aaaa", "aaaaa")));
    String cmd = DeltaRegistrationCommand.administrativeStatus().encode();
    assertDoesNotThrow(
        () -> DeltaRegistrationCommand.decode(cmd + " ".repeat(512 * 1024 - cmd.length())));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRegistrationCommand.decode(cmd + " ".repeat(512 * 1024 - cmd.length() + 1)));
  }

  private static ObjectNode json(String text) {
    return DeltaJson.object(text, 512 * 1024);
  }

  private static Stream<DeltaRegistrationCommand> commands() {
    return Stream.of(
        DeltaRegistrationCommand.reserve("op", "run", SCHEMA),
        DeltaRegistrationCommand.reserve("op", "run", CONTRACT),
        DeltaRegistrationCommand.adopt("op", "s3://bucket/table", "retired", "ref"),
        DeltaRegistrationCommand.beginReplace(ID, "new", "run", SCHEMA),
        DeltaRegistrationCommand.beginReplace(ID, "new", "run", CONTRACT),
        DeltaRegistrationCommand.publish(ID),
        DeltaRegistrationCommand.status(ID),
        DeltaRegistrationCommand.statusByToken("op", "run"),
        DeltaRegistrationCommand.administrativeStatus(),
        DeltaRegistrationCommand.handoff(ID, "run", "ref"),
        DeltaRegistrationCommand.abort(ID));
  }
}
