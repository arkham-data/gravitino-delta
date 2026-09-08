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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.gravitino.rel.Column;

/**
 * Detached authoritative native metadata and a deliberately partial Gravitino schema projection.
 */
public final class DeltaSnapshotMetadata {
  private final String location;
  private final long version;
  private final String uuid;
  private final String schemaJson;
  private final List<Column> columns;
  private final List<String> partitionColumns;
  private final Map<String, String> configuration;
  private final Optional<String> description;
  private final OptionalLong createdTime;
  private final int minReaderVersion;
  private final int minWriterVersion;
  private final Set<String> readerFeatures;
  private final Set<String> writerFeatures;

  DeltaSnapshotMetadata(
      String location,
      long version,
      String uuid,
      String schemaJson,
      List<Column> columns,
      List<String> partitionColumns,
      Map<String, String> configuration,
      Optional<String> description,
      OptionalLong createdTime,
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    if (uuid == null || uuid.trim().isEmpty()) {
      throw new DeltaMetadataException("INVALID_TABLE_ID", "COPY_METADATA");
    }
    this.location = location;
    this.version = version;
    this.uuid = uuid;
    this.schemaJson = schemaJson;
    this.columns = DeltaSchemaProjection.copyColumns(columns);
    this.partitionColumns = Collections.unmodifiableList(new ArrayList<>(partitionColumns));
    this.configuration = boundedConfiguration(configuration);
    this.description = description;
    this.createdTime = createdTime;
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;
    this.readerFeatures = Collections.unmodifiableSet(new LinkedHashSet<>(readerFeatures));
    this.writerFeatures = Collections.unmodifiableSet(new LinkedHashSet<>(writerFeatures));
  }

  /**
   * @return normalized native location
   */
  public String location() {
    return location;
  }

  /**
   * @return observed native version
   */
  public long version() {
    return version;
  }

  /**
   * @return opaque, nonblank native table ID; not restricted to UUID syntax
   */
  public String uuid() {
    return uuid;
  }

  /**
   * @return exact native schema JSON, including field metadata
   */
  public String schemaJson() {
    return schemaJson;
  }

  /**
   * @return defensively copied Gravitino schema projection
   */
  public List<Column> columns() {
    return DeltaSchemaProjection.copyColumns(columns);
  }

  /**
   * @return immutable partition names in native order
   */
  public List<String> partitionColumns() {
    return partitionColumns;
  }

  /**
   * @return bounded immutable provider-internal configuration; may contain sensitive values
   */
  public Map<String, String> configuration() {
    return configuration;
  }

  /**
   * @return optional native description
   */
  public Optional<String> description() {
    return description;
  }

  /**
   * @return optional native creation time in epoch milliseconds
   */
  public OptionalLong createdTime() {
    return createdTime;
  }

  /**
   * @return native minimum reader protocol version
   */
  public int minReaderVersion() {
    return minReaderVersion;
  }

  /**
   * @return native minimum writer protocol version
   */
  public int minWriterVersion() {
    return minWriterVersion;
  }

  /**
   * @return immutable native reader features
   */
  public Set<String> readerFeatures() {
    return readerFeatures;
  }

  /**
   * @return immutable native writer features
   */
  public Set<String> writerFeatures() {
    return writerFeatures;
  }

  private static Map<String, String> boundedConfiguration(Map<String, String> configuration) {
    if (configuration.size() > 4096) {
      throw limit();
    }
    long total = 0;
    for (Map.Entry<String, String> entry : configuration.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null || key.length() > 16384 || value.length() > 1048576) {
        throw limit();
      }
      int keyBytes = key.getBytes(StandardCharsets.UTF_8).length;
      int valueBytes = value.getBytes(StandardCharsets.UTF_8).length;
      total += (long) keyBytes + valueBytes;
      if (keyBytes > 16384 || valueBytes > 1048576 || total > 16777216) {
        throw limit();
      }
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(configuration));
  }

  private static DeltaMetadataException limit() {
    return new DeltaMetadataException("CONFIGURATION_LIMIT", "COPY_METADATA");
  }
}
