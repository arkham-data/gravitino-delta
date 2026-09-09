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

package org.apache.gravitino.spark.connector.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.UUID;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRegistrationJournal {
  @TempDir Path directory;

  @Test
  void durablyReplacesSanitizedIntentAndRetainsUnknownIdentity() throws Exception {
    Path own = directory.resolve("journal");
    DeltaRegistrationJournal journal = DeltaRegistrationJournal.durable(own);
    DeltaRegistrationJournal.Entry intent =
        new DeltaRegistrationJournal.Entry(
            UUID.randomUUID().toString(),
            "canonical",
            "db",
            "t",
            DeltaRegistrationCommand.reserve("token", "run", "{\"type\":\"struct\",\"fields\":[]}"),
            DeltaRegistrationJournal.Phase.INTENT,
            null);
    journal.write(intent);
    Path file = own.resolve(intent.key + ".json");
    assertTrue(Files.readString(file).contains("RESERVE"));
    assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file));
    assertEquals(PosixFilePermissions.fromString("rwx------"), Files.getPosixFilePermissions(own));
    journal.write(intent.phase(DeltaRegistrationJournal.Phase.STATUS_NEEDED, null));
    String saved = Files.readString(file);
    assertTrue(saved.contains("STATUS_NEEDED"));
    assertTrue(saved.contains("token"));
    assertFalse(saved.contains("nativeId"));
    try (var files = Files.list(own)) {
      assertEquals(1, files.count());
    }
  }

  @Test
  void refusesSymlinkDirectoryAndNonPrivateExistingDirectory() throws Exception {
    Path link = directory.resolve("link");
    Files.createSymbolicLink(link, directory);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.durable(link));
    Path shared = Files.createDirectory(directory.resolve("shared"));
    Files.setPosixFilePermissions(shared, PosixFilePermissions.fromString("rwxr-xr-x"));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.durable(shared));
  }
}
