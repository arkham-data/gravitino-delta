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

package org.apache.gravitino.catalog.lakehouse.delta.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestDeltaSchemaProjection {
  @Test
  void projectsEveryPrimitive() {
    DataType[] nativeTypes = {
      BooleanType.BOOLEAN,
      ByteType.BYTE,
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG,
      FloatType.FLOAT,
      DoubleType.DOUBLE,
      new DecimalType(38, 18),
      StringType.STRING,
      BinaryType.BINARY,
      DateType.DATE,
      TimestampType.TIMESTAMP,
      TimestampNTZType.TIMESTAMP_NTZ,
      VariantType.VARIANT
    };
    Type[] expected = {
      Types.BooleanType.get(),
      Types.ByteType.get(),
      Types.ShortType.get(),
      Types.IntegerType.get(),
      Types.LongType.get(),
      Types.FloatType.get(),
      Types.DoubleType.get(),
      Types.DecimalType.of(38, 18),
      Types.StringType.get(),
      Types.BinaryType.get(),
      Types.DateType.get(),
      Types.TimestampType.withTimeZone(6),
      Types.TimestampType.withoutTimeZone(6),
      Types.VariantType.get()
    };
    for (int i = 0; i < nativeTypes.length; i++)
      assertEquals(expected[i], DeltaSchemaProjection.type(nativeTypes[i]));
    assertEquals(Types.DecimalType.of(1, 0), DeltaSchemaProjection.type(new DecimalType(1, 0)));
  }

  @Test
  void retainsNestedNullabilityAndCommentsWithoutInventingDefaults() {
    FieldMetadata metadata =
        FieldMetadata.builder()
            .putString("comment", "nested note")
            .putString("delta.generationExpression", "id + 1")
            .putString("CURRENT_DEFAULT", "42")
            .putLong("delta.identity.start", 1)
            .build();
    StructType inner =
        new StructType().add(new StructField("leaf", IntegerType.INTEGER, false, metadata));
    StructType schema =
        new StructType()
            .add(
                new StructField(
                    "nested",
                    new ArrayType(new MapType(StringType.STRING, inner, false), false),
                    false,
                    metadata));
    Column c = DeltaSchemaProjection.columns(schema).get(0);
    assertFalse(c.nullable());
    assertEquals("nested note", c.comment());
    assertFalse(c.autoIncrement());
    assertEquals(Column.DEFAULT_VALUE_NOT_SET, c.defaultValue());
    Types.ListType list = (Types.ListType) c.dataType();
    assertFalse(list.elementNullable());
    Types.MapType map = (Types.MapType) list.elementType();
    assertFalse(map.valueNullable());
    Types.StructType.Field field = ((Types.StructType) map.valueType()).fields()[0];
    assertFalse(field.nullable());
    assertEquals("nested note", field.comment());
    assertEquals(Types.IntegerType.get(), field.type());
  }

  @Test
  void retainsNullableContainersAndCopiesNestedTypesOnInputAndOutput() {
    StructType inner = new StructType().add("leaf", IntegerType.INTEGER);
    Type projected =
        DeltaSchemaProjection.type(
            new ArrayType(new MapType(StringType.STRING, inner, true), true));
    Types.ListType list = (Types.ListType) projected;
    assertTrue(list.elementNullable());
    assertTrue(((Types.MapType) list.elementType()).valueNullable());
    assertTrue(
        ((Types.StructType) ((Types.MapType) list.elementType()).valueType())
            .fields()[0].nullable());
    Map<String, String> config = new HashMap<>();
    config.put("key", "value");
    List<String> partitions = new ArrayList<>(List.of("z", "a"));
    List<Column> columns =
        new ArrayList<>(DeltaSchemaProjection.columns(new StructType().add("nested", inner)));
    DeltaSnapshotMetadata dto =
        new DeltaSnapshotMetadata(
            "location",
            0,
            "opaque",
            "raw",
            columns,
            partitions,
            config,
            Optional.empty(),
            OptionalLong.empty(),
            1,
            2,
            Set.of(),
            Set.of());
    ((Types.StructType) columns.get(0).dataType()).fields()[0] = null;
    partitions.clear();
    config.clear();
    columns.clear();
    assertEquals(List.of("z", "a"), dto.partitionColumns());
    assertEquals(Map.of("key", "value"), dto.configuration());
    assertNotNull(((Types.StructType) dto.columns().get(0).dataType()).fields()[0]);
  }

  @Test
  void rejectsUnrepresentableTypesAndComments() {
    assertThrows(DeltaMetadataException.class, () -> DeltaSchemaProjection.type(new StructType()));
    assertThrows(
        DeltaMetadataException.class,
        () -> DeltaSchemaProjection.type(new StringType("SPARK.UTF8_LCASE")));
    assertThrows(
        DeltaMetadataException.class,
        () ->
            DeltaSchemaProjection.type(
                new DataType() {
                  public int hashCode() {
                    return 0;
                  }

                  public boolean equals(Object other) {
                    return this == other;
                  }

                  public String toString() {
                    return "unknown";
                  }
                }));
    for (FieldMetadata m :
        List.of(
            FieldMetadata.builder().putLong("comment", 3).build(),
            FieldMetadata.builder().putNull("comment").build()))
      assertThrows(
          DeltaMetadataException.class,
          () ->
              DeltaSchemaProjection.columns(
                  new StructType().add(new StructField("bad", IntegerType.INTEGER, true, m))));
    assertThrows(IllegalArgumentException.class, () -> new DecimalType(39, 0));
  }
}
