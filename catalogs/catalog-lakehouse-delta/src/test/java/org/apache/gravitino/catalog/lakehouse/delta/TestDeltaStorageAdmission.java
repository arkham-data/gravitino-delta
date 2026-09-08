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

package org.apache.gravitino.catalog.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestDeltaStorageAdmission {
  @Test
  void directColumnUtf16BoundariesAndInvalidText() {
    assertDoesNotThrow(
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("n".repeat(128), "c".repeat(4096), Types.StringType.get()))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("n".repeat(129), "ok", Types.StringType.get()))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("n", "c".repeat(4097), Types.StringType.get()))));
    assertDoesNotThrow(
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("😀".repeat(64), null, Types.StringType.get()))));
    for (String name : List.of("😀".repeat(65), "a\u0000b", "a\ud800", "\udc00")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaStorageAdmission.checkColumns(
                  List.of(column(name, null, Types.StringType.get()))));
    }
  }

  @Test
  void serializedTypeBoundariesCountNestedCommentsAndEscapes() throws Exception {
    int overhead = encoded(nested(""));
    Type fits = nested("x".repeat(65535 - overhead));
    assertEquals(65535, encoded(fits));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkColumns(List.of(column("c", null, fits))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("c", null, nested("x".repeat(65536 - overhead))))));
    assertDoesNotThrow(
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("c", null, nested("x".repeat(4097))))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("c", null, nested("界".repeat(22000))))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(column("c", null, nested("\n".repeat(33000))))));
    assertDoesNotThrow(
        () -> DeltaStorageAdmission.checkColumns(List.of(column("c", null, nested("\u0000")))));
  }

  @Test
  void defaultExpressionUsesCoreDtoSerializationAndUnsetIsNull() throws Exception {
    int overhead = encoded(DTOConverters.toFunctionArg(Literals.stringLiteral("")));
    Column fits =
        Column.of(
            "c",
            Types.StringType.get(),
            null,
            Literals.stringLiteral("x".repeat(65535 - overhead)));
    assertEquals(65535, encoded(DTOConverters.toFunctionArg(fits.defaultValue())));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkColumns(List.of(fits)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaStorageAdmission.checkColumns(
                List.of(
                    Column.of(
                        "c",
                        Types.StringType.get(),
                        null,
                        Literals.stringLiteral("x".repeat(65536 - overhead))))));
    assertDoesNotThrow(
        () ->
            DeltaStorageAdmission.checkColumns(List.of(column("c", null, Types.StringType.get()))));
  }

  @Test
  void countingWriterMatchesCoreAndHandlesSplitSurrogates() throws Exception {
    for (Object value :
        List.of("ascii", "\n\\\"\u0000", "界😀", Map.of("key", "😀\n"), nested("界😀"))) {
      int bytes = encoded(value);
      assertDoesNotThrow(() -> DeltaStorageAdmission.checkJson(value, bytes));
      assertThrows(
          IllegalArgumentException.class, () -> DeltaStorageAdmission.checkJson(value, bytes - 1));
    }
    try (DeltaStorageAdmission.Utf8LimitWriter writer =
        new DeltaStorageAdmission.Utf8LimitWriter(4)) {
      writer.write("\ud83d");
      writer.write("\ude00");
      writer.finish();
      assertEquals(4, writer.count());
    }
    assertThrows(
        IOException.class,
        () -> {
          try (DeltaStorageAdmission.Utf8LimitWriter writer =
              new DeltaStorageAdmission.Utf8LimitWriter(4)) {
            writer.write("\ud800");
            writer.finish();
          }
        });
  }

  @Test
  void desiredSchemaUsesAuthoritativeProjectionAndNativeReaderRemainsBroader() {
    String template =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"comment\":\"%s\"}}]}";
    assertDoesNotThrow(
        () -> DeltaStorageAdmission.checkDesiredSchema(String.format(template, "ok")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaStorageAdmission.checkDesiredSchema(String.format(template, "x".repeat(4097))));
    assertEquals(
        4097,
        DeltaMetadataReader.projectSchema(String.format(template, "x".repeat(4097)))
            .get(0)
            .comment()
            .length());
  }

  @Test
  void schemaAndFinalCandidateCheckContainingPropertiesAndAllDirectFields() throws Exception {
    SchemaEntity schema = mock(SchemaEntity.class);
    when(schema.name()).thenReturn("s".repeat(128));
    when(schema.comment()).thenReturn("c".repeat(256));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkSchemaCandidate(schema));
    when(schema.comment()).thenReturn("c".repeat(257));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkSchemaCandidate(schema));
    TableEntity table = mock(TableEntity.class);
    when(table.name()).thenReturn("t");
    when(table.columns()).thenReturn(List.of());
    String internal = "\"".repeat(8_388_600);
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkJson(internal, 16777215));
    when(table.properties()).thenReturn(Map.of("delta.registration.internal.state", internal));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkTableCandidate(table));
    when(table.properties())
        .thenReturn(
            Map.of("delta.registration.internal.reason", "UNREPRESENTABLE_NATIVE_METADATA"));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkTableCandidate(table));
    when(table.name()).thenReturn("t".repeat(129));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkTableCandidate(table));
  }

  private static Column column(String name, String comment, Type type) {
    return Column.of(name, type, comment, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static Type nested(String comment) {
    return Types.StructType.of(
        Types.StructType.Field.of("nested".repeat(30), Types.StringType.get(), true, comment));
  }

  private static int encoded(Object value) throws Exception {
    return JsonUtils.anyFieldMapper()
        .writeValueAsString(value)
        .getBytes(StandardCharsets.UTF_8)
        .length;
  }

  @Test
  void finalDirectNamesFormatCommentAndColumnBounds() {
    SchemaEntity schema = mock(SchemaEntity.class);
    when(schema.name()).thenReturn("s".repeat(129));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkSchemaCandidate(schema));
    TableEntity table = mock(TableEntity.class);
    when(table.name()).thenReturn("t".repeat(128));
    when(table.columns()).thenReturn(List.of());
    when(table.properties()).thenReturn(Map.of(Table.PROPERTY_TABLE_FORMAT, "f".repeat(64)));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkTableCandidate(table));
    when(table.properties()).thenReturn(Map.of(Table.PROPERTY_TABLE_FORMAT, "f".repeat(65)));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkTableCandidate(table));
    when(table.properties()).thenReturn(Map.of());
    for (String comment : List.of("\u0000", "\ud800", "界".repeat(5_592_406))) {
      when(table.comment()).thenReturn(comment);
      assertThrows(
          IllegalArgumentException.class, () -> DeltaStorageAdmission.checkTableCandidate(table));
    }
    when(table.comment()).thenReturn(null);
    ColumnEntity column = mock(ColumnEntity.class);
    when(column.name()).thenReturn("c");
    when(column.dataType()).thenReturn(Types.StringType.get());
    when(column.comment()).thenReturn("x".repeat(4097));
    when(table.columns()).thenReturn(List.of(column));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaStorageAdmission.checkTableCandidate(table));
    when(column.comment()).thenReturn("x".repeat(4096));
    assertDoesNotThrow(() -> DeltaStorageAdmission.checkTableCandidate(table));
  }
}
