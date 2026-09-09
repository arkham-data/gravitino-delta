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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaProtocolDescriptor;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaSnapshot;
import org.apache.gravitino.rel.Column;

/** Detached internal provider observation; public projection exposes only safe configuration. */
final class DeltaNativeObservation {
  private static final List<String> SAFE_KEYS =
      List.of(
          "delta.columnMapping.mode",
          "delta.columnMapping.maxColumnId",
          "delta.appendOnly",
          "delta.enableChangeDataFeed",
          "delta.enableDeletionVectors");
  private final String location;
  private final long version;
  private final String uuid;
  private final String schemaJson;
  private final List<Column> columns;
  private final List<String> partitionColumns;
  private final Optional<String> description;
  private final OptionalLong createdTime;
  private final DeltaProtocolDescriptor protocol;
  private final String configurationDigest;
  private final Map<String, String> safeConfiguration;
  private final Map<String, String> rawConfiguration;
  private final Optional<String> markerDigest;
  private final String markerProvenance;

  private DeltaNativeObservation(
      DeltaSnapshotMetadata metadata,
      String digest,
      Map<String, String> safe,
      Optional<String> markerDigest,
      String provenance) {
    location = metadata.location();
    version = metadata.version();
    uuid = metadata.uuid();
    schemaJson = metadata.schemaJson();
    columns = DeltaMetadataReader.copyColumns(metadata.columns());
    partitionColumns = List.copyOf(metadata.partitionColumns());
    description = metadata.description();
    createdTime = metadata.createdTime();
    protocol =
        DeltaProtocolDescriptor.of(
            metadata.minReaderVersion(),
            metadata.minWriterVersion(),
            metadata.readerFeatures(),
            metadata.writerFeatures());
    configurationDigest = digest;
    safeConfiguration = Map.copyOf(safe);
    rawConfiguration = Map.copyOf(metadata.configuration());
    this.markerDigest = markerDigest;
    markerProvenance = provenance;
  }

  static DeltaNativeObservation from(DeltaSnapshotMetadata metadata) {
    try {
      if (metadata.uuid() == null
          || metadata.uuid().isBlank()
          || utf8(metadata.uuid()).remaining() > 8192
          || metadata.version() < 0) {
        throw invalid();
      }
      Map<String, String> raw = metadata.configuration();
      Map<String, String> safe = new LinkedHashMap<>();
      for (String key : SAFE_KEYS) {
        if (!raw.containsKey(key)) {
          continue;
        }
        String value = raw.get(key);
        if (key.equals("delta.columnMapping.mode")) {
          if (!List.of("none", "name", "id").contains(value)) {
            throw invalid();
          }
        } else if (key.equals("delta.columnMapping.maxColumnId")) {
          if (!value.matches("0|[1-9][0-9]*") || Long.parseLong(value) < 0) {
            throw invalid();
          }
        } else if (!value.equals("true") && !value.equals("false")) {
          throw invalid();
        }
        safe.put(key, value);
      }
      Optional<String> markerDigest = Optional.empty();
      String provenance = "ABSENT";
      if (raw.containsKey(DeltaOperationMarker.PROPERTY_KEY)) {
        String marker = raw.get(DeltaOperationMarker.PROPERTY_KEY);
        DeltaOperationMarker parsed = DeltaOperationMarker.decode(marker);
        provenance = parsed.schemaContract().isPresent() ? "VALID_V2" : "VALID_V1";
        markerDigest = Optional.of(digest(Map.of(DeltaOperationMarker.PROPERTY_KEY, marker)));
      }
      return new DeltaNativeObservation(metadata, digest(raw), safe, markerDigest, provenance);
    } catch (Exception e) {
      throw invalid();
    }
  }

  String location() {
    return location;
  }

  long version() {
    return version;
  }

  String uuid() {
    return uuid;
  }

  String schemaJson() {
    return schemaJson;
  }

  List<Column> columns() {
    return DeltaMetadataReader.copyColumns(columns);
  }

  List<String> partitionColumns() {
    return partitionColumns;
  }

  Optional<String> description() {
    return description;
  }

  OptionalLong createdTime() {
    return createdTime;
  }

  DeltaProtocolDescriptor protocol() {
    return protocol;
  }

  DeltaSchemaSnapshot schemaContext() {
    return DeltaSchemaSnapshot.of(version, uuid, schemaJson, rawConfiguration, protocol);
  }

  String configurationDigest() {
    return configurationDigest;
  }

  Map<String, String> safeConfiguration() {
    return safeConfiguration;
  }

  Optional<String> markerDigest() {
    return markerDigest;
  }

  String markerProvenance() {
    return markerProvenance;
  }
  /**
   * @return fixed nonsecret diagnostic label
   */
  @Override
  public String toString() {
    return "DeltaNativeObservation";
  }

  private static String digest(Map<String, String> configuration) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    digest.update("gravitino.delta.configuration.v1".getBytes(StandardCharsets.UTF_8));
    for (Map.Entry<String, String> entry : new TreeMap<>(configuration).entrySet()) {
      for (String value : List.of(entry.getKey(), entry.getValue())) {
        ByteBuffer bytes = utf8(value);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.remaining()).array());
        digest.update(bytes);
      }
    }
    StringBuilder result = new StringBuilder("sha256-v1:");
    for (byte value : digest.digest()) {
      result.append(Character.forDigit((value >>> 4) & 15, 16));
      result.append(Character.forDigit(value & 15, 16));
    }
    return result.toString();
  }

  private static ByteBuffer utf8(String value) throws Exception {
    return StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(value));
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_DELTA_NATIVE_OBSERVATION");
  }
}
