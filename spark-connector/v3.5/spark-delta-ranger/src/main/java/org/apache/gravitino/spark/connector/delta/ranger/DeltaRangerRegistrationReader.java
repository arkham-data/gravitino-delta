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

package org.apache.gravitino.spark.connector.delta.ranger;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerMetadataClient.MetadataLeaseStamp;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Detaches published registration metadata within one existing metadata lease. */
final class DeltaRangerRegistrationReader {
  private final Object owner = new Object();
  private final DeltaRangerMetadataClient metadata;
  private final String metalake;
  private final String catalog;

  DeltaRangerRegistrationReader(
      DeltaRangerMetadataClient metadata, String metalake, String catalog) {
    this.metadata = Objects.requireNonNull(metadata, "Metadata client required");
    this.metalake = DeltaRangerResourceKey.literal(metalake);
    this.catalog = DeltaRangerResourceKey.literal(catalog);
  }

  PublishedObservation read(Identifier identifier) {
    try {
      Objects.requireNonNull(identifier);
      String[] namespace = identifier.namespace();
      if (namespace == null || namespace.length != 1) {
        throw denied();
      }
      String database = DeltaRangerResourceKey.literal(namespace[0]);
      String tableName = DeltaRangerResourceKey.literal(identifier.name());
      return metadata.read(
          loadedCatalog -> {
            MetadataLeaseStamp lease = metadata.leaseStamp();
            if (!catalog.equals(loadedCatalog.name())
                || !"lakehouse-delta".equals(loadedCatalog.provider())) {
              throw denied();
            }
            Table table =
                loadedCatalog.asTableCatalog().loadTable(NameIdentifier.of(database, tableName));
            if (table == null || !tableName.equals(table.name())) {
              throw denied();
            }
            DeltaRegistrationReceipt receipt =
                DeltaRegistrationReceipt.fromProperties(table.properties());
            if (receipt.state() != State.PUBLISHED
                || !receipt.location().isPresent()
                || !receipt.nativeId().isPresent()
                || !receipt.nativeVersion().isPresent()
                || receipt.nativeVersion().getAsLong() < 0) {
              throw denied();
            }
            return new PublishedObservation(
                owner,
                lease,
                metalake,
                catalog,
                database,
                tableName,
                receipt,
                schemaJson(table.columns()));
          });
    } catch (RuntimeException | LinkageError failure) {
      // Upstream errors can include registration tokens, credentials or physical locations.
      throw denied();
    }
  }

  void requireLive(@Nullable PublishedObservation observation) {
    try {
      if (observation == null || observation.owner != owner) {
        throw denied();
      }
      metadata.requireLive(observation.lease);
    } catch (RuntimeException | LinkageError failure) {
      throw denied();
    }
  }

  private static String schemaJson(Column[] columns) {
    if (columns == null || columns.length == 0) {
      throw denied();
    }
    SparkTypeConverter converter = new SparkTypeConverter();
    Set<String> names = new HashSet<>();
    StructField[] fields = new StructField[columns.length];
    for (int i = 0; i < columns.length; i++) {
      Column column = Objects.requireNonNull(columns[i]);
      String name = fieldName(column.name());
      if (!names.add(name)) {
        throw denied();
      }
      DataType type = converter.toSparkType(Objects.requireNonNull(column.dataType()));
      fields[i] = DataTypes.createStructField(name, withoutMetadata(type), column.nullable());
    }
    return DataTypes.createStructType(fields).json();
  }

  private static DataType withoutMetadata(DataType type) {
    Objects.requireNonNull(type);
    if (type instanceof StructType) {
      StructField[] fields = ((StructType) type).fields();
      StructField[] detached = new StructField[fields.length];
      Set<String> names = new HashSet<>();
      for (int i = 0; i < fields.length; i++) {
        StructField field = Objects.requireNonNull(fields[i]);
        String name = fieldName(field.name());
        if (!names.add(name)) {
          throw denied();
        }
        detached[i] =
            DataTypes.createStructField(name, withoutMetadata(field.dataType()), field.nullable());
      }
      return DataTypes.createStructType(detached);
    }
    if (type instanceof ArrayType) {
      ArrayType array = (ArrayType) type;
      return DataTypes.createArrayType(withoutMetadata(array.elementType()), array.containsNull());
    }
    if (type instanceof MapType) {
      MapType map = (MapType) type;
      return DataTypes.createMapType(
          withoutMetadata(map.keyType()),
          withoutMetadata(map.valueType()),
          map.valueContainsNull());
    }
    return type;
  }

  private static String fieldName(String name) {
    if (name == null || name.trim().isEmpty()) {
      throw denied();
    }
    return name;
  }

  private static SecurityException denied() {
    return new SecurityException("Published registration metadata is unavailable");
  }

  /** Immutable metadata value; liveness must be checked explicitly by its owning reader. */
  static final class PublishedObservation {
    private final Object owner;
    private final MetadataLeaseStamp lease;
    private final String metalake;
    private final String catalog;
    private final String database;
    private final String table;
    private final DeltaRegistrationReceipt receipt;
    private final String schemaJson;

    private PublishedObservation(
        Object owner,
        MetadataLeaseStamp lease,
        String metalake,
        String catalog,
        String database,
        String table,
        DeltaRegistrationReceipt receipt,
        String schemaJson) {
      this.owner = owner;
      this.lease = lease;
      this.metalake = metalake;
      this.catalog = catalog;
      this.database = database;
      this.table = table;
      this.receipt = receipt;
      this.schemaJson = schemaJson;
    }

    String metalake() {
      return metalake;
    }

    String catalog() {
      return catalog;
    }

    String database() {
      return database;
    }

    String table() {
      return table;
    }

    DeltaRegistrationReceipt receipt() {
      return receipt;
    }

    DeltaRegistrationIdentity identity() {
      return receipt.identity();
    }

    String location() {
      return receipt.location().get();
    }

    String nativeId() {
      return receipt.nativeId().get();
    }

    long nativeVersion() {
      return receipt.nativeVersion().getAsLong();
    }

    String schemaJson() {
      return schemaJson;
    }

    // This compares complete observed publication values, not action-version compatibility.
    boolean samePublication(@Nullable PublishedObservation other) {
      return other != null
          && owner == other.owner
          && metalake.equals(other.metalake)
          && catalog.equals(other.catalog)
          && database.equals(other.database)
          && table.equals(other.table)
          && receipt.toProperties().equals(other.receipt.toProperties())
          && schemaJson.equals(other.schemaJson);
    }

    /** Returns a constant description without registration metadata. */
    @Override
    public String toString() {
      return "Published registration observation";
    }
  }
}
