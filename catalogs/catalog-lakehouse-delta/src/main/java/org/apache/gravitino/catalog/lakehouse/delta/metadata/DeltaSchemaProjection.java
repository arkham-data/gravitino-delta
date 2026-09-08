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

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
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
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Projects the representable Kernel schema while raw metadata remains authoritative. */
final class DeltaSchemaProjection {
  private DeltaSchemaProjection() {}

  static List<Column> columns(StructType schema) {
    if (schema.fields().isEmpty()) {
      throw unsupported();
    }
    List<Column> result = new ArrayList<>();
    for (StructField field : schema.fields()) {
      result.add(
          Column.of(
              field.getName(),
              type(field.getDataType()),
              comment(field),
              field.isNullable(),
              false,
              Column.DEFAULT_VALUE_NOT_SET));
    }
    return Collections.unmodifiableList(result);
  }

  static Type type(DataType nativeType) {
    if (nativeType instanceof BooleanType) {
      return Types.BooleanType.get();
    }
    if (nativeType instanceof ByteType) {
      return Types.ByteType.get();
    }
    if (nativeType instanceof ShortType) {
      return Types.ShortType.get();
    }
    if (nativeType instanceof IntegerType) {
      return Types.IntegerType.get();
    }
    if (nativeType instanceof LongType) {
      return Types.LongType.get();
    }
    if (nativeType instanceof FloatType) {
      return Types.FloatType.get();
    }
    if (nativeType instanceof DoubleType) {
      return Types.DoubleType.get();
    }
    if (nativeType instanceof BinaryType) {
      return Types.BinaryType.get();
    }
    if (nativeType instanceof DateType) {
      return Types.DateType.get();
    }
    if (nativeType instanceof TimestampType) {
      return Types.TimestampType.withTimeZone(6);
    }
    if (nativeType instanceof TimestampNTZType) {
      return Types.TimestampType.withoutTimeZone(6);
    }
    if (nativeType instanceof VariantType) {
      return Types.VariantType.get();
    }
    if (nativeType instanceof DecimalType) {
      DecimalType decimal = (DecimalType) nativeType;
      return Types.DecimalType.of(decimal.getPrecision(), decimal.getScale());
    }
    if (nativeType instanceof StringType) {
      if (!((StringType) nativeType)
          .getCollationIdentifier()
          .equals(StringType.STRING.getCollationIdentifier())) {
        throw unsupported();
      }
      return Types.StringType.get();
    }
    if (nativeType instanceof ArrayType) {
      ArrayType array = (ArrayType) nativeType;
      return Types.ListType.of(type(array.getElementType()), array.containsNull());
    }
    if (nativeType instanceof MapType) {
      MapType map = (MapType) nativeType;
      return Types.MapType.of(
          type(map.getKeyType()), type(map.getValueType()), map.isValueContainsNull());
    }
    if (nativeType instanceof StructType) {
      List<StructField> fields = ((StructType) nativeType).fields();
      if (fields.isEmpty()) {
        throw unsupported();
      }
      Types.StructType.Field[] projected = new Types.StructType.Field[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        StructField field = fields.get(i);
        projected[i] =
            Types.StructType.Field.of(
                field.getName(), type(field.getDataType()), field.isNullable(), comment(field));
      }
      return Types.StructType.of(projected);
    }
    throw unsupported();
  }

  // Gravitino StructType.fields() exposes a mutable array. Copy recursively on both DTO input and
  // output.
  static List<Column> copyColumns(List<Column> columns) {
    List<Column> result = new ArrayList<>();
    for (Column column : columns) {
      result.add(
          Column.of(
              column.name(),
              copyType(column.dataType()),
              column.comment(),
              column.nullable(),
              false,
              Column.DEFAULT_VALUE_NOT_SET));
    }
    return Collections.unmodifiableList(result);
  }

  private static Type copyType(Type type) {
    if (type instanceof Types.StructType) {
      Types.StructType.Field[] fields = ((Types.StructType) type).fields();
      Types.StructType.Field[] copy = new Types.StructType.Field[fields.length];
      for (int i = 0; i < fields.length; i++) {
        Types.StructType.Field field = fields[i];
        copy[i] =
            Types.StructType.Field.of(
                field.name(), copyType(field.type()), field.nullable(), field.comment());
      }
      return Types.StructType.of(copy);
    }
    if (type instanceof Types.ListType) {
      Types.ListType list = (Types.ListType) type;
      return Types.ListType.of(copyType(list.elementType()), list.elementNullable());
    }
    if (type instanceof Types.MapType) {
      Types.MapType map = (Types.MapType) type;
      return Types.MapType.of(
          copyType(map.keyType()), copyType(map.valueType()), map.valueNullable());
    }
    return type;
  }

  @Nullable
  private static String comment(StructField field) {
    if (!field.getMetadata().contains("comment")) {
      return null;
    }
    Object comment = field.getMetadata().get("comment");
    if (!(comment instanceof String)) {
      throw unsupported();
    }
    return (String) comment;
  }

  private static DeltaMetadataException unsupported() {
    return new DeltaMetadataException("UNREPRESENTABLE_SCHEMA", "PROJECT_SCHEMA");
  }
}
