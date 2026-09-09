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

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** Portable admission for the exact JSON/direct-string representations stored by core. */
final class DeltaStorageAdmission {
  static final long TEXT_BYTES = 65535;
  static final long MEDIUM_TEXT_BYTES = 16777215;

  private DeltaStorageAdmission() {}

  static void checkDesiredSchema(String rawSchemaJson) {
    try {
      checkColumns(DeltaMetadataReader.projectSchema(rawSchemaJson));
    } catch (RuntimeException e) {
      throw invalid();
    }
  }

  static void checkObservation(DeltaNativeObservation observation) {
    checkColumns(observation.columns());
    checkDirect(observation.description().orElse(null), MEDIUM_TEXT_BYTES, MEDIUM_TEXT_BYTES);
  }

  static void checkColumns(List<Column> columns) {
    try {
      for (Column column : columns) {
        checkColumn(column.name(), column.comment(), column.dataType(), column.defaultValue());
      }
    } catch (RuntimeException e) {
      throw invalid();
    }
  }

  static void checkSchemaCandidate(SchemaEntity schema) {
    try {
      checkDirect(schema.name(), 128, MEDIUM_TEXT_BYTES);
      checkSchemaComment(schema.comment());
      checkJson(schema.properties(), MEDIUM_TEXT_BYTES);
      checkJson(schema.auditInfo(), MEDIUM_TEXT_BYTES);
    } catch (RuntimeException e) {
      throw invalid();
    }
  }

  static void checkSchemaComment(@Nullable String comment) {
    checkDirect(comment, 256, MEDIUM_TEXT_BYTES);
  }

  static void checkTableCandidate(TableEntity table) {
    try {
      checkDirect(table.name(), 128, MEDIUM_TEXT_BYTES);
      checkDirect(table.comment(), MEDIUM_TEXT_BYTES, MEDIUM_TEXT_BYTES);
      checkDirect(
          table.properties() == null ? null : table.properties().get(Table.PROPERTY_TABLE_FORMAT),
          64,
          MEDIUM_TEXT_BYTES);
      checkJson(table.properties(), MEDIUM_TEXT_BYTES);
      checkJson(table.auditInfo(), MEDIUM_TEXT_BYTES);
      if (table.partitioning() != null) {
        checkJson(DTOConverters.toDTOs(table.partitioning()), MEDIUM_TEXT_BYTES);
      }
      if (table.distribution() != null) {
        checkJson(DTOConverters.toDTO(table.distribution()), MEDIUM_TEXT_BYTES);
      }
      if (table.sortOrders() != null) {
        checkJson(DTOConverters.toDTOs(table.sortOrders()), MEDIUM_TEXT_BYTES);
      }
      if (table.indexes() != null) {
        checkJson(DTOConverters.toDTOs(table.indexes()), MEDIUM_TEXT_BYTES);
      }
      for (ColumnEntity column : table.columns()) {
        checkColumn(column.name(), column.comment(), column.dataType(), column.defaultValue());
        checkJson(column.auditInfo(), MEDIUM_TEXT_BYTES);
      }
    } catch (RuntimeException e) {
      throw invalid();
    }
  }

  static void checkJson(@Nullable Object value, long limit) {
    try (Utf8LimitWriter writer = new Utf8LimitWriter(limit)) {
      JsonUtils.anyFieldMapper().writeValue(writer, value);
      writer.finish();
    } catch (IOException | RuntimeException e) {
      throw invalid();
    }
  }

  private static void checkColumn(
      String name, @Nullable String comment, Type type, @Nullable Expression defaultValue) {
    checkDirect(name, 128, MEDIUM_TEXT_BYTES);
    checkDirect(comment, 4096, MEDIUM_TEXT_BYTES);
    checkJson(type, TEXT_BYTES);
    if (defaultValue != null && !defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET)) {
      checkJson(DTOConverters.toFunctionArg(defaultValue), TEXT_BYTES);
    }
  }

  private static void checkDirect(@Nullable String value, long units, long bytes) {
    if (value == null) {
      return;
    }
    if (value.length() > units) {
      throw invalid();
    }
    try (Utf8LimitWriter writer = new Utf8LimitWriter(bytes)) {
      writer.write(value);
      writer.finish();
    } catch (IOException e) {
      throw invalid();
    }
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("UNREPRESENTABLE_NATIVE_METADATA");
  }
  /** Bounded UTF-8 sink; high surrogates can span serializer write calls. */
  static final class Utf8LimitWriter extends Writer {
    private final long limit;
    private long count;
    private boolean high;

    Utf8LimitWriter(long limit) {
      if (limit < 0) {
        throw invalid();
      }
      this.limit = limit;
    }

    void finish() throws IOException {
      if (high) {
        throw new IOException("INVALID_TEXT");
      }
    }

    long count() {
      return count;
    }
    /** {@inheritDoc} */
    @Override
    public void write(char[] chars, int offset, int length) throws IOException {
      for (int i = offset; i < offset + length; i++) {
        char c = chars[i];
        if (high) {
          if (!Character.isLowSurrogate(c)) {
            throw new IOException("INVALID_TEXT");
          }
          high = false;
          add(4);
        } else if (Character.isHighSurrogate(c)) {
          high = true;
        } else if (Character.isLowSurrogate(c) || c == 0) {
          throw new IOException("INVALID_TEXT");
        } else {
          add(c < 0x80 ? 1 : c < 0x800 ? 2 : 3);
        }
      }
    }
    /** {@inheritDoc} */
    @Override
    public void write(String value, int offset, int length) throws IOException {
      // Writer's default String overload allocates a char array proportional to the entire input.
      char[] buffer = new char[Math.min(1024, length)];
      for (int current = offset; current < offset + length; ) {
        int size = Math.min(buffer.length, offset + length - current);
        value.getChars(current, current + size, buffer, 0);
        write(buffer, 0, size);
        current += size;
      }
    }
    /** {@inheritDoc} */
    @Override
    public void flush() {}
    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      finish();
    }

    private void add(int bytes) throws IOException {
      if (count > limit - bytes) {
        throw new IOException("STORAGE_LIMIT");
      }
      count += bytes;
    }
  }
}
