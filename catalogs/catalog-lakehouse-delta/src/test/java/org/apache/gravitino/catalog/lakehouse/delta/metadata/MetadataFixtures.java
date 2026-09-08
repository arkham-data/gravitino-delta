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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;

final class MetadataFixtures {
  static final ObjectMapper JSON = new ObjectMapper();
  static final StructType SCHEMA =
      new StructType()
          .add("id", IntegerType.INTEGER, false)
          .add("z", StringType.STRING)
          .add("a", StringType.STRING);
  static final List<String> PARTITIONS = Arrays.asList("z", "a");
  static final String ID = "opaque-native-id";
  static final String PROTOCOL = "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}";
  static final String V2_PROTOCOL =
      "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"v2Checkpoint\"],\"writerFeatures\":[\"v2Checkpoint\"]}}";

  static Configuration configuration() {
    return new Configuration();
  }

  static String location(Path table) {
    return table.toUri().toString();
  }

  static Path log(Path table) throws Exception {
    return Files.createDirectories(table.resolve("_delta_log"));
  }

  static Path commit(Path table, long version) throws Exception {
    return log(table).resolve(String.format(Locale.ROOT, "%020d.json", version));
  }

  static void actions(Path table, long version, String... actions) throws Exception {
    Files.writeString(commit(table, version), String.join("\n", actions) + "\n");
  }

  static String metadata(String id, String schema, Map<String, String> config) {
    ObjectNode m = JSON.createObjectNode();
    m.put("id", id);
    m.put("description", "native description");
    m.put("schemaString", schema);
    m.set("partitionColumns", JSON.valueToTree(PARTITIONS));
    m.put("createdTime", 123L);
    m.set("configuration", JSON.valueToTree(config));
    m.set(
        "format",
        JSON.createObjectNode().put("provider", "parquet").set("options", JSON.createObjectNode()));
    return JSON.createObjectNode().set("metaData", m).toString();
  }

  static void unitTable(Path table, String protocol, String schema, Map<String, String> config)
      throws Exception {
    actions(table, 0, protocol, metadata(ID, schema, config));
  }

  static void nativeTable(Path table, Map<String, String> config, int finalVersion)
      throws Exception {
    Engine engine = DefaultEngine.create(configuration());
    Table t = Table.forPath(engine, location(table));
    t.createTransactionBuilder(engine, "metadata-fixture", Operation.CREATE_TABLE)
        .withSchema(engine, SCHEMA)
        .withPartitionColumns(engine, PARTITIONS)
        .withTableProperties(engine, config)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());
    for (int v = 1; v <= finalVersion; v++) {
      t.createTransactionBuilder(engine, "metadata-fixture", Operation.WRITE)
          .build(engine)
          .commit(engine, CloseableIterable.emptyIterable());
    }
  }

  // Protocol-shaped unit control: Kernel 3.3 rejects custom properties at its writer API.
  // This deliberately edits only the owned fixture's CREATE action; it is not a native marker
  // write.
  static void markerBearingCreateControl(Path table, String marker) throws Exception {
    Path json = commit(table, 0);
    List<String> updated = new ArrayList<>();
    for (String action : Files.readAllLines(json)) {
      ObjectNode envelope = (ObjectNode) JSON.readTree(action);
      if (envelope.has("metaData")) {
        ((ObjectNode) envelope.get("metaData").get("configuration"))
            .put("gravitino.registration.operation", marker);
      }
      updated.add(envelope.toString());
    }
    Files.deleteIfExists(json.resolveSibling("." + json.getFileName() + ".crc"));
    Files.writeString(json, String.join("\n", updated) + "\n");
  }

  static void nativeCheckpoint(Path table, long version) throws Exception {
    Engine e = DefaultEngine.create(configuration());
    Table.forPath(e, location(table)).checkpoint(e, version);
  }

  static void pruneBefore(Path table, long version) throws Exception {
    for (long v = 0; v < version; v++) Files.deleteIfExists(commit(table, v));
  }

  static void parquet(Path path, List<String> actions) throws Exception {
    Files.createDirectories(path.getParent());
    Path input = Files.createTempFile(path.getParent(), "checkpoint-actions-", ".json");
    Files.writeString(input, String.join("\n", actions));
    StructType actionSchema =
        new StructType()
            .add("metaData", Metadata.FULL_SCHEMA)
            .add("protocol", Protocol.FULL_SCHEMA)
            .add("checkpointMetadata", new StructType().add("version", LongType.LONG))
            .add("sidecar", SidecarFile.READ_SCHEMA)
            .add(
                "remove",
                new StructType()
                    .add("path", StringType.STRING)
                    .add("deletionTimestamp", LongType.LONG)
                    .add("dataChange", BooleanType.BOOLEAN));
    Engine e = DefaultEngine.create(configuration());
    try (CloseableIterator<ColumnarBatch> batches =
        e.getJsonHandler()
            .readJsonFiles(
                Utils.singletonCloseableIterator(
                    FileStatus.of(input.toUri().toString(), Files.size(input), 0)),
                actionSchema,
                Optional.empty())) {
      List<FilteredColumnarBatch> data = new ArrayList<>();
      while (batches.hasNext())
        data.add(new FilteredColumnarBatch(batches.next(), Optional.empty()));
      try (CloseableIterator<FilteredColumnarBatch> iter =
          Utils.toCloseableIterator(data.iterator())) {
        e.getParquetHandler().writeParquetFileAtomically(path.toUri().toString(), iter);
      }
    } finally {
      Files.delete(input);
    }
  }

  static void checkpointFixture(Path table, String kind) throws Exception {
    String protocol = kind.startsWith("v2") ? V2_PROTOCOL : PROTOCOL;
    String metadata = metadata(ID, SCHEMA.toJson(), Map.of("fixture", "checkpoint"));
    unitTable(table, protocol, SCHEMA.toJson(), Map.of("fixture", "checkpoint"));
    for (int v = 1; v <= 40; v++) actions(table, v, "{\"commitInfo\":{\"timestamp\":" + v + "}}");
    Path log = log(table);
    String prefix = String.format(Locale.ROOT, "%020d.checkpoint", 40);
    if (kind.startsWith("multipart")) {
      parquet(log.resolve(prefix + ".0000000001.0000000002.parquet"), List.of(protocol));
      if (!kind.equals("multipart-incomplete"))
        parquet(log.resolve(prefix + ".0000000002.0000000002.parquet"), List.of(metadata));
    } else {
      List<String> content = new ArrayList<>(List.of(protocol, metadata));
      if (kind.startsWith("v2")) content.add("{\"checkpointMetadata\":{\"version\":40}}");
      if (kind.equals("v2-classic-sidecar")) {
        Path sidecar = log.resolve("_sidecars/" + UUID.randomUUID() + ".parquet");
        parquet(
            sidecar,
            List.of(
                "{\"remove\":{\"path\":\"former-data.parquet\",\"deletionTimestamp\":1,\"dataChange\":false}}"));
        content.add(
            "{\"sidecar\":{\"path\":\""
                + sidecar.getFileName()
                + "\",\"sizeInBytes\":"
                + Files.size(sidecar)
                + ",\"modificationTime\":1}}");
      }
      String suffix =
          kind.equals("v2-json")
              ? "." + UUID.randomUUID() + ".json"
              : kind.equals("v2-parquet") ? "." + UUID.randomUUID() + ".parquet" : ".parquet";
      if (kind.equals("v2-json"))
        Files.writeString(log.resolve(prefix + suffix), String.join("\n", content));
      else parquet(log.resolve(prefix + suffix), content);
    }
    pruneBefore(table, 40);
  }
}
