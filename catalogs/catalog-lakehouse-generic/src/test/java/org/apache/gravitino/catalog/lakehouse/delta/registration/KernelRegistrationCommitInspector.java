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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;

/** Test-only Delta Kernel evidence reader. */
final class KernelRegistrationCommitInspector implements RegistrationCommitVerifier {
  private final Configuration configuration;

  KernelRegistrationCommitInspector(Configuration configuration) {
    this.configuration = new Configuration(Objects.requireNonNull(configuration));
  }

  /** {@inheritDoc} */
  @Override
  public Result verify(TableEntity storedReservation, long candidateVersion) {
    String location;
    String expectedSchema;
    Long storedBase;
    try {
      Objects.requireNonNull(storedReservation, "Stored reservation is required");
      Map<String, String> properties = storedReservation.properties();
      location =
          RegistrationCommand.normalizeLocation(
              required(properties, RegistrationCommand.INTERNAL + "location"));
      expectedSchema = required(properties, RegistrationCommand.INTERNAL + "schemaFingerprint");
      required(properties, RegistrationCommand.INTERNAL + "token");
      storedBase = storedBase(properties);
      if (candidateVersion < -1) {
        throw new IllegalArgumentException("Candidate version must be -1 or nonnegative");
      }
    } catch (RuntimeException invalidRecord) {
      return Result.ambiguous(
          "Stored registration evidence is invalid; publication remains blocked: "
              + invalidRecord.getMessage(),
          null);
    }

    try {
      Engine engine = DefaultEngine.create(new Configuration(configuration));
      Snapshot snapshot = Table.forPath(engine, location).getLatestSnapshot(engine);
      long nativeVersion = snapshot.getVersion(engine);
      StructType nativeSchema = snapshot.getSchema(engine);
      String nativeSchemaFingerprint = nativeSchema.toJson();
      Observation observation =
          new Observation(nativeVersion, null, nativeSchemaFingerprint, toColumns(nativeSchema));

      if (!expectedSchema.equals(nativeSchemaFingerprint)) {
        return Result.ambiguous(
            "The observed Delta schema does not match the stored requested schema; publication "
                + "remains blocked",
            observation);
      }
      if (storedBase != null && nativeVersion <= storedBase) {
        return Result.ambiguous(
            "The valid Delta snapshot did not advance the stored native base and cannot correlate "
                + "a registration operation",
            observation);
      }
      if (candidateVersion >= 0 && candidateVersion != nativeVersion) {
        return Result.ambiguous(
            "The candidate version is only a search hint; the valid snapshot at the stored "
                + "location cannot correlate a registration operation",
            observation);
      }
      return Result.ambiguous(
          "A valid Delta snapshot exists, but public Kernel metadata cannot correlate it to the "
              + "stored registration token",
          observation);
    } catch (TableNotFoundException absent) {
      return Result.notCommitted(
          "No Delta transaction log was observed at the stored location; this observation alone "
              + "does not prove a stale writer cannot commit later");
    } catch (UnsupportedSchemaTypeException unsupportedType) {
      return Result.ambiguous(
          "The Delta snapshot uses an unsupported schema type; publication remains blocked: "
              + unsupportedType.getMessage(),
          null);
    } catch (RuntimeException unreadable) {
      return Result.ambiguous(
          "The Delta transaction log at the stored location could not be read; publication remains "
              + "blocked ("
              + unreadable.getClass().getSimpleName()
              + ")",
          null);
    }
  }

  private static Long storedBase(Map<String, String> properties) {
    String value = properties.get(RegistrationCommand.INTERNAL + "native.version");
    if (value == null) {
      return null;
    }
    long parsed = Long.parseLong(value);
    if (parsed < 0) {
      throw new IllegalArgumentException("Stored native version must be nonnegative");
    }
    return parsed;
  }

  private static String required(Map<String, String> properties, String key) {
    String value = properties.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing required stored field " + key);
    }
    return value;
  }

  private static List<Column> toColumns(StructType schema) {
    List<Column> columns = new ArrayList<>();
    for (StructField field : schema.fields()) {
      columns.add(
          Column.of(
              field.getName(),
              toGravitinoType(field.getDataType()),
              null,
              field.isNullable(),
              false,
              null));
    }
    return columns;
  }

  private static Type toGravitinoType(DataType nativeType) {
    if (nativeType instanceof IntegerType) {
      return Types.IntegerType.get();
    }
    if (nativeType instanceof StringType) {
      return Types.StringType.get();
    }
    throw new UnsupportedSchemaTypeException(nativeType.getClass().getName());
  }

  private static final class UnsupportedSchemaTypeException extends RuntimeException {
    private UnsupportedSchemaTypeException(String nativeType) {
      super(nativeType);
    }
  }
}
