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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaColumnIdentities {
  @TempDir Path temp;
  private final AtomicLong ids = new AtomicLong(100);

  @Test
  void mappedRenameAndReorderPreserveIdsOnlyWithinNativeIdentity() throws Exception {
    DeltaNativeObservation first = observation("first", "uuid", "a:1,b:2");
    DeltaRegistrationState prior = prior(first);
    List<ColumnEntity> renamed =
        DeltaColumnIdentities.project(
            prior,
            observation("second", "uuid", "b:2,renamed:1,new:3"),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    assertEquals(102L, renamed.get(0).id());
    assertEquals(101L, renamed.get(1).id());
    assertEquals(103L, renamed.get(2).id());
    assertEquals(0, renamed.get(0).position());
    List<ColumnEntity> replacement =
        DeltaColumnIdentities.project(
            prior,
            observation("third", "new-uuid", "a:1,b:2"),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    assertNotEquals(101L, replacement.get(0).id());
  }

  @Test
  void unmappedRenameAllocatesButSameNamePreservesId() throws Exception {
    DeltaRegistrationState prior = prior(observation("first", "uuid", "a:0,b:0"));
    List<ColumnEntity> changed =
        DeltaColumnIdentities.project(
            prior,
            observation("second", "uuid", "b:0,renamed:0"),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    assertEquals(102L, changed.get(0).id());
    assertEquals(103L, changed.get(1).id());
    assertEquals(2, changed.size());
  }

  @Test
  void duplicateOrInvalidMappingIdsFallBackToExactLogicalName() throws Exception {
    DeltaRegistrationState prior = prior(observation("first", "uuid", "a:1,b:1"));
    List<ColumnEntity> changed =
        DeltaColumnIdentities.project(
            prior,
            observation("second", "uuid", "renamed:1,b:1"),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    assertEquals(103L, changed.get(0).id());
    assertEquals(102L, changed.get(1).id());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "a:0,renamed:1", "renamed:1,a:0",
        "a,renamed:1", "renamed:1,a",
        "a:2,other:2,renamed:1", "renamed:1,a:2,other:2"
      })
  void mappedRenameOwnsPriorIdentityBeforeNameFallback(String fields) throws Exception {
    DeltaRegistrationState prior = prior(observation("first", "uuid", "a:1"));
    List<ColumnEntity> changed =
        DeltaColumnIdentities.project(
            prior,
            observation("second", "uuid", fields),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    String[] orderedFields = fields.split(",");
    for (int i = 0; i < changed.size(); i++) {
      ColumnEntity column = changed.get(i);
      assertEquals(orderedFields[i].split(":")[0], column.name());
      assertEquals(i, column.position());
      if (column.name().equals("renamed")) {
        assertEquals(101L, column.id());
        assertEquals(prior.entity().columns().get(0).auditInfo(), column.auditInfo());
      } else {
        assertNotEquals(101L, column.id());
      }
    }
    assertEquals(changed.size(), changed.stream().map(ColumnEntity::id).distinct().count());
  }

  @Test
  void differentValidMappingDoesNotInheritMappedIdentityByName() throws Exception {
    DeltaRegistrationState prior = prior(observation("first", "uuid", "a:1"));
    List<ColumnEntity> changed =
        DeltaColumnIdentities.project(
            prior,
            observation("second", "uuid", "a:2"),
            ids::incrementAndGet,
            TestDeltaRegistrationOperations.AUDIT);
    assertNotEquals(101L, changed.get(0).id());
  }

  private DeltaRegistrationState prior(DeltaNativeObservation observed) {
    DeltaRegistrationState initial =
        DeltaRegistrationState.initial(
            TestDeltaRegistrationOperations.IDENT,
            10,
            DeltaRegistrationCommand.adopt("t", observed.location(), "retired", "cutover"),
            "alice",
            observed.location(),
            TestDeltaRegistrationOperations.AUDIT,
            observed);
    TableEntity row = initial.entity();
    row.columns()
        .addAll(
            DeltaColumnIdentities.project(
                null, observed, ids::incrementAndGet, TestDeltaRegistrationOperations.AUDIT));
    return DeltaRegistrationState.from(row);
  }

  private DeltaNativeObservation observation(String directory, String uuid, String fields)
      throws Exception {
    Path table = temp.resolve(directory);
    Path log = Files.createDirectories(table.resolve("_delta_log"));
    StringBuilder schema = new StringBuilder("{\"type\":\"struct\",\"fields\":[");
    for (String field : fields.split(",")) {
      if (schema.charAt(schema.length() - 1) != '[') {
        schema.append(',');
      }
      String[] parts = field.split(":");
      schema
          .append("{\"name\":\"")
          .append(parts[0])
          .append("\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{");
      if (parts.length == 2) {
        schema.append("\"delta.columnMapping.id\":").append(parts[1]);
      }
      schema.append("}}");
    }
    schema.append("]}");
    Map<String, Object> metadata =
        Map.of(
            "id",
            uuid,
            "schemaString",
            schema.toString(),
            "partitionColumns",
            List.of(),
            "configuration",
            Map.of(),
            "format",
            Map.of("provider", "parquet", "options", Map.of()));
    Files.writeString(
        log.resolve("00000000000000000000.json"),
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + TestDeltaRegistrationOperations.JSON.writeValueAsString(Map.of("metaData", metadata))
            + "\n");
    Configuration configuration = new Configuration();
    configuration.set(
        "fs.s3a.impl", TestDeltaRegistrationOperations.FixtureFileSystem.class.getName());
    configuration.set("delta.fixture.root", temp.toString());
    try (DeltaMetadataReader reader = new DeltaMetadataReader(configuration)) {
      return DeltaNativeObservation.from(reader.latest("s3a://bucket/fixture/" + directory));
    }
  }
}
