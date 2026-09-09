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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;

final class FileDeltaRegistrationJournal implements DeltaRegistrationJournal {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
  private static final Set<PosixFilePermission> DIRECTORY_PERMISSIONS =
      PosixFilePermissions.fromString("rwx------");
  private static final int MAX_BYTES = 1024 * 1024;
  private final Path directory;

  FileDeltaRegistrationJournal(Path directory) {
    try {
      if (Files.isSymbolicLink(directory)) {
        throw new IllegalArgumentException("Journal directory must not be a symbolic link");
      }
      if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
        Files.createDirectory(
            directory, PosixFilePermissions.asFileAttribute(DIRECTORY_PERMISSIONS));
      }
      if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
          || !Files.getPosixFilePermissions(directory, LinkOption.NOFOLLOW_LINKS)
              .equals(DIRECTORY_PERMISSIONS)) {
        throw new IllegalArgumentException("Journal directory must be driver-owned with mode 0700");
      }
      this.directory = directory.toRealPath();
      if (!Files.getOwner(this.directory)
          .equals(
              this.directory
                  .getFileSystem()
                  .getUserPrincipalLookupService()
                  .lookupPrincipalByName(System.getProperty("user.name")))) {
        throw new IllegalArgumentException("Journal directory must be owned by the driver user");
      }
      try (FileChannel parent =
          FileChannel.open(this.directory.getParent(), StandardOpenOption.READ)) {
        parent.force(true);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot initialize private driver journal");
    }
  }

  static Entry read(Path file) {
    try {
      if (Files.isSymbolicLink(file) || Files.size(file) > MAX_BYTES) {
        throw new IllegalArgumentException("Invalid bounded journal entry");
      }
      byte[] bytes;
      try (InputStream input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS);
          ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[8192];
        int count;
        while ((count = input.read(buffer)) != -1) {
          if (output.size() + count > MAX_BYTES) {
            throw new IllegalArgumentException("Invalid bounded journal entry");
          }
          output.write(buffer, 0, count);
        }
        bytes = output.toByteArray();
      }
      JsonNode node;
      try (JsonParser parser = JSON.getFactory().createParser(bytes)) {
        node = JSON.readTree(parser);
        if (parser.nextToken() != null) {
          throw new IllegalArgumentException("Invalid trailing journal content");
        }
      }
      Set<String> fields =
          new HashSet<>(
              Arrays.asList("version", "key", "catalog", "namespace", "table", "command", "phase"));
      if (node != null && node.has("receipt")) {
        fields.add("receipt");
      }
      if (node == null
          || !node.isObject()
          || node.size() != fields.size()
          || !node.path("version").isIntegralNumber()
          || node.path("version").intValue() != 1) {
        throw new IllegalArgumentException("Invalid bounded journal entry");
      }
      for (String field : fields) {
        if (!node.hasNonNull(field)) {
          throw new IllegalArgumentException("Invalid bounded journal entry");
        }
      }
      String key = text(node, "key");
      if (!key.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
        throw new IllegalArgumentException("Invalid journal key");
      }
      DeltaRegistrationReceipt receipt =
          node.has("receipt")
              ? DeltaRegistrationReceipt.fromProperties(
                  JSON.convertValue(
                      node.get("receipt"), new TypeReference<Map<String, String>>() {}))
              : null;
      DeltaRegistrationCommand command =
          DeltaRegistrationCommand.decode(node.get("command").toString());
      if (receipt != null) {
        DeltaRegistrationClient.validateBinding(command, receipt);
      }
      return new Entry(
          key,
          text(node, "catalog"),
          text(node, "namespace"),
          text(node, "table"),
          command,
          Phase.valueOf(text(node, "phase")),
          receipt);
    } catch (IOException | RuntimeException error) {
      throw new IllegalArgumentException("Invalid bounded journal entry");
    }
  }

  private static String text(JsonNode node, String key) {
    if (!node.path(key).isTextual()
        || node.path(key).textValue().isEmpty()
        || node.path(key).textValue().length() > 4096) {
      throw new IllegalArgumentException("Invalid journal field");
    }
    return node.path(key).textValue();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void write(Entry entry) {
    if (!entry.key.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
      throw new IllegalArgumentException("Invalid journal key");
    }
    Path temporary = null;
    try {
      ObjectNode node = JSON.createObjectNode();
      node.put("version", 1);
      node.put("key", entry.key);
      node.put("catalog", entry.catalog);
      node.put("namespace", entry.namespace);
      node.put("table", entry.table);
      node.set("command", JSON.readTree(entry.command.encode()));
      node.put("phase", entry.phase.name());
      if (entry.receipt != null) {
        node.set("receipt", JSON.valueToTree(entry.receipt.toProperties()));
      }
      byte[] bytes = JSON.writeValueAsBytes(node);
      if (bytes.length > MAX_BYTES) {
        throw new IllegalArgumentException("Journal entry exceeds bounded encoding");
      }
      temporary =
          Files.createTempFile(
              directory,
              ".intent-",
              ".tmp",
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      try (FileChannel file =
          FileChannel.open(temporary, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          file.write(buffer);
        }
        file.force(true);
      }
      Files.move(
          temporary,
          directory.resolve(entry.key + ".json"),
          StandardCopyOption.ATOMIC_MOVE,
          StandardCopyOption.REPLACE_EXISTING);
      temporary = null;
      try (FileChannel folder = FileChannel.open(directory, StandardOpenOption.READ)) {
        folder.force(true);
      }
    } catch (IOException e) {
      throw new IllegalStateException("DELTA_JOURNAL_WRITE_FAILED");
    } finally {
      if (temporary != null) {
        try {
          Files.deleteIfExists(temporary);
        } catch (IOException ignored) {
          /* Preserve evidence if temporary cleanup fails. */
        }
      }
    }
  }
}
