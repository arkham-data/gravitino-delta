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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerRawMaintenanceConfig {
  @TempDir Path directory;
  private static final String VALID =
      "version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=1\ntable.0.metalake=lake\ntable.0.catalog=A.b\ntable.0.database=Db\ntable.0.table=T\n";

  @Test
  void validLiteralComponentsAndDuplicateDecodedKeys() throws Exception {
    DeltaRangerRawMaintenanceConfig parsed = read(file(VALID));
    assertNotNull(parsed);
    assertEquals("A.b", parsed.tables().get(0).catalog());
    assertEquals("Db", parsed.tables().get(0).database());
    assertEquals("T", parsed.tables().get(0).table());
    assertThrows(UnsupportedOperationException.class, () -> parsed.tables().clear());
    invalid(VALID + "table.0.ta\\ble=other\n");
    invalid(VALID + "table.0.\\u0074able=other\n");
  }

  @Test
  void exactSchemaAndBoundedRecords() throws Exception {
    for (String text :
        new String[] {
          VALID.replace("version=1", "version=2"),
          VALID.replace("profile=raw-current-head-filter-project-v1", "profile=other"),
          VALID.replace("table.count=1", "table.count=0"),
          VALID.replace("table.count=1", "table.count=257"),
          VALID.replace("table.count=1", "table.count=2"),
          VALID.replace("table.0.", "table.1."),
          VALID + "unknown=secret\n",
          VALID + "table.0.path=secret\n",
          VALID.replace("table.0.table=T\n", ""),
          VALID.replace("table.0.table=T", "table.0.table= "),
          VALID.replace("table.0.table=T", "table.0.table=*"),
          VALID.replace("table.count=1", "table.count=2")
              + "table.1.metalake=lake\ntable.1.catalog=A.b\ntable.1.database=Db\ntable.1.table=T\n"
        }) invalid(text);
  }

  @Test
  void rejectsUnsafeFilesAndOversizeWithoutLeakingValues() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> read(Paths.get("relative.properties")));
    assertThrows(IllegalArgumentException.class, () -> read(directory));
    Path safe = file(VALID);
    Path link = directory.resolve("link");
    Files.createSymbolicLink(link, safe);
    assertThrows(IllegalArgumentException.class, () -> read(link));
    Files.setPosixFilePermissions(safe, PosixFilePermissions.fromString("rw-r-----"));
    assertThrows(IllegalArgumentException.class, () -> read(safe));
    invalid(VALID + "#" + new String(new char[65536]).replace('\0', 'x'));
    invalid(VALID + "broken=\\uZZZZ");
  }

  private Path file(String text) throws Exception {
    Path p = Files.createTempFile(directory, "config", ".properties");
    Files.write(p, text.getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-------"));
    return p;
  }

  private void invalid(String text) throws Exception {
    Path p = file(text);
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> read(p));
    assertEquals("Invalid raw maintenance configuration", failure.getMessage());
    assertNull(failure.getCause());
  }

  private DeltaRangerRawMaintenanceConfig read(Path path) {
    return DeltaRangerRawMaintenanceConfig.read(path);
  }
}
