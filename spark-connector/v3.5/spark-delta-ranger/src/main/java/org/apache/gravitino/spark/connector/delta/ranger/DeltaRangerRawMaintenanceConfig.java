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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/** Immutable canonical startup enrollment, containing no native or query authority. */
final class DeltaRangerRawMaintenanceConfig {
  private static final int MAX_BYTES = 65536;
  private final List<TableKey> tables;

  private DeltaRangerRawMaintenanceConfig(List<TableKey> tables) {
    this.tables = Collections.unmodifiableList(new ArrayList<>(tables));
  }

  static DeltaRangerRawMaintenanceConfig read(Path path) {
    try {
      if (!path.isAbsolute() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
        throw invalid();
      }
      Set<PosixFilePermission> permissions =
          Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS);
      if (permissions.stream()
          .anyMatch(p -> p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_"))) {
        throw invalid();
      }
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (InputStream stream = Files.newInputStream(path)) {
        byte[] buffer = new byte[4096];
        int count;
        while ((count = stream.read(buffer)) != -1) {
          if (count > MAX_BYTES - bytes.size()) throw invalid();
          bytes.write(buffer, 0, count);
        }
      }
      Properties properties = new UniqueProperties();
      try (InputStreamReader reader =
          new InputStreamReader(
              new ByteArrayInputStream(bytes.toByteArray()),
              StandardCharsets.UTF_8
                  .newDecoder()
                  .onMalformedInput(CodingErrorAction.REPORT)
                  .onUnmappableCharacter(CodingErrorAction.REPORT))) {
        properties.load(reader);
      }
      if (!"1".equals(properties.getProperty("version"))
          || !"raw-current-head-filter-project-v1".equals(properties.getProperty("profile")))
        throw invalid();
      int count = Integer.parseInt(properties.getProperty("table.count"));
      if (count < 1 || count > 256) throw invalid();
      Set<String> fields = new HashSet<>(Arrays.asList("version", "profile", "table.count"));
      List<TableKey> tables = new ArrayList<>();
      Set<TableKey> distinct = new HashSet<>();
      for (int i = 0; i < count; i++) {
        String prefix = "table." + i + ".";
        for (String field : Arrays.asList("metalake", "catalog", "database", "table"))
          fields.add(prefix + field);
        TableKey table =
            new TableKey(
                properties.getProperty(prefix + "metalake"),
                properties.getProperty(prefix + "catalog"),
                properties.getProperty(prefix + "database"),
                properties.getProperty(prefix + "table"));
        if (!distinct.add(table)) throw invalid();
        tables.add(table);
      }
      if (!fields.equals(properties.stringPropertyNames())) throw invalid();
      return new DeltaRangerRawMaintenanceConfig(tables);
    } catch (IOException | RuntimeException failure) {
      throw invalid();
    }
  }

  List<TableKey> tables() {
    return tables;
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("Invalid raw maintenance configuration");
  }

  private static final class UniqueProperties extends Properties {
    /** Rejects duplicate decoded keys rather than overwriting an earlier field. */
    @Override
    public synchronized Object put(Object key, Object value) {
      if (containsKey(key)) throw invalid();
      return super.put(key, value);
    }
  }

  /** A tuple of literal canonical components; dots retain their component boundaries. */
  static final class TableKey {
    private final String metalake;
    private final String catalog;
    private final String database;
    private final String table;

    private TableKey(String metalake, String catalog, String database, String table) {
      this.metalake = DeltaRangerResourceKey.literal(metalake);
      this.catalog = DeltaRangerResourceKey.literal(catalog);
      this.database = DeltaRangerResourceKey.literal(database);
      this.table = DeltaRangerResourceKey.literal(table);
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

    /** Compares all canonical components without splitting or case folding. */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof TableKey)) return false;
      TableKey that = (TableKey) other;
      return metalake.equals(that.metalake)
          && catalog.equals(that.catalog)
          && database.equals(that.database)
          && table.equals(that.table);
    }
    /** Hashes the complete canonical tuple. */
    @Override
    public int hashCode() {
      return Objects.hash(metalake, catalog, database, table);
    }
  }
}
