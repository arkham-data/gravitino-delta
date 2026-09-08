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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Test-only exact stored-location reader plus public Kernel snapshot validation. */
final class NativeRegistrationCommitVerifier
    implements RegistrationCommitVerifier, RegistrationReplacementBaseReader {
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final int MAX_LINEAGE_VERSIONS = 32;
  private static final int MAX_LINEAGE_JSON_BYTES = 16 * 1024 * 1024;
  private final Configuration configuration;

  NativeRegistrationCommitVerifier(Configuration configuration) {
    this.configuration = new Configuration(configuration);
  }

  /** {@inheritDoc} */
  @Override
  public Result verify(TableEntity row, long hint) {
    try {
      DeltaOperationMarker expected = marker(row);
      long candidate =
          expected.baseVersion().isPresent() ? expected.baseVersion().getAsLong() + 1 : 0;
      if (hint < -1 || (hint >= 0 && hint != candidate))
        return Result.ambiguous("CANDIDATE_HINT_MISMATCH", null);
      Engine engine = DefaultEngine.create(new Configuration(configuration));
      Table table = Table.forPath(engine, expected.normalizedLocation());
      long latest = table.getLatestSnapshot(engine).getVersion(engine);
      Snapshot snapshot = table.getSnapshotAsOfVersion(engine, candidate);
      StructType schema = snapshot.getSchema(engine);
      try (FileSystem fs =
          FileSystem.newInstance(new Path(expected.normalizedLocation()).toUri(), configuration)) {
        if (expected.baseVersion().isPresent()) {
          long base = expected.baseVersion().getAsLong();
          Snapshot baseSnapshot = table.getSnapshotAsOfVersion(engine, base);
          if (baseSnapshot.getVersion(engine) != base)
            throw new IllegalStateException("BASE_VERSION_MISMATCH");
          BaseObservation observed =
              lineage(fs, expected.normalizedLocation(), base, baseSnapshot.getSchema(engine));
          if (!observed.uuid.equals(expected.baseUuid().get())
              || !observed.schemaJson.equals(value(row, "replacement.base.schemaFingerprint")))
            throw new IllegalStateException("BASE_LINEAGE_MISMATCH");
        }
        try (InputStream input = fs.open(log(expected.normalizedLocation(), candidate))) {
          DeltaCommitEvidence evidence =
              new DeltaCommitActionReader()
                  .inspect(
                      expected,
                      candidate,
                      latest,
                      snapshot.getVersion(engine),
                      schema.toJson(),
                      input);
          if (evidence.kind() != DeltaCommitEvidence.Kind.MATCHED)
            return Result.ambiguous(evidence.reason(), null);
          return Result.matched(
              new Observation(
                  evidence.version().getAsLong(),
                  evidence.uuid().get(),
                  evidence.schemaJson().get(),
                  columns(schema)),
              "Exact stored JSON and public Kernel MATCHED");
        }
      }
    } catch (Exception unreadable) {
      // No exception message: SDK messages may contain storage configuration or credentials.
      return Result.ambiguous(
          "NATIVE_EVIDENCE_UNAVAILABLE:" + unreadable.getClass().getSimpleName(), null);
    }
  }

  /** {@inheritDoc} */
  @Override
  public BaseObservation read(TableEntity row) {
    try {
      String location = value(row, "location");
      Engine engine = DefaultEngine.create(new Configuration(configuration));
      Snapshot latest = Table.forPath(engine, location).getLatestSnapshot(engine);
      long version = latest.getVersion(engine);
      try (FileSystem fs = FileSystem.newInstance(new Path(location).toUri(), configuration)) {
        return lineage(fs, location, version, latest.getSchema(engine));
      }
    } catch (Exception unreadable) {
      throw new IllegalStateException(
          "Actual replacement base unavailable: " + unreadable.getClass().getSimpleName());
    }
  }

  static DeltaOperationMarker marker(TableEntity row) {
    String location = RegistrationCommand.normalizeLocation(value(row, "location"));
    long generation = Long.parseLong(value(row, "generation"));
    if ("REPLACE".equals(value(row, "operation"))) {
      return DeltaOperationMarker.replace(
          row.id(),
          generation,
          value(row, "token"),
          location,
          value(row, "schemaFingerprint"),
          Long.parseLong(value(row, "replacement.base.version")),
          value(row, "replacement.base.uuid"));
    }
    return DeltaOperationMarker.create(
        row.id(), generation, value(row, "token"), location, value(row, "schemaFingerprint"));
  }

  private static BaseObservation lineage(
      FileSystem fs, String location, long version, StructType snapshotSchema) throws Exception {
    columns(snapshotSchema); // This bounded adapter supports INT/STRING only.
    String validated = canonical(snapshotSchema.toJson());
    for (long current = version;
        current >= 0 && version - current < MAX_LINEAGE_VERSIONS;
        current--) {
      Path object = log(location, current);
      long size = fs.getFileStatus(object).getLen();
      if (size > MAX_LINEAGE_JSON_BYTES) throw new IllegalStateException("LINEAGE_JSON_TOO_LARGE");
      byte[] bytes = new byte[Math.toIntExact(size)];
      try (FSDataInputStream input = fs.open(object)) {
        input.readFully(bytes);
      }
      JsonNode metadata = null;
      for (String line : new String(bytes, StandardCharsets.UTF_8).split("\n")) {
        if (line.trim().isEmpty()) continue;
        JsonNode action = JSON.readTree(line);
        if (!action.isObject() || action.size() != 1)
          throw new IllegalStateException("INVALID_LINEAGE_ACTION");
        if (action.has("metaData")) {
          if (metadata != null) throw new IllegalStateException("DUPLICATE_LINEAGE_METADATA");
          metadata = action.get("metaData");
        }
      }
      if (metadata != null) {
        if (!metadata.path("id").isTextual()
            || !metadata.path("schemaString").isTextual()
            || !canonical(metadata.get("schemaString").textValue()).equals(validated))
          throw new IllegalStateException("LINEAGE_SCHEMA_MISMATCH");
        return new BaseObservation(version, metadata.get("id").textValue(), validated);
      }
    }
    throw new IllegalStateException("REQUIRED_LINEAGE_MISSING");
  }

  private static String canonical(String schema) {
    return DeltaOperationMarker.create(0, 0, "schema", "file:///schema", schema).schemaJson();
  }

  private static Path log(String location, long version) {
    return new Path(location, String.format(Locale.ROOT, "_delta_log/%020d.json", version));
  }

  private static String value(TableEntity row, String suffix) {
    String value = row.properties().get(RegistrationCommand.INTERNAL + suffix);
    if (value == null || value.isEmpty())
      throw new IllegalArgumentException("Missing stored " + suffix);
    return value;
  }

  private static List<Column> columns(StructType schema) {
    List<Column> result = new ArrayList<>();
    for (StructField field : schema.fields()) {
      Type type;
      if (field.getDataType() instanceof IntegerType) type = Types.IntegerType.get();
      else if (field.getDataType() instanceof StringType) type = Types.StringType.get();
      else throw new IllegalArgumentException("Unsupported schema conversion");
      result.add(Column.of(field.getName(), type, null, field.isNullable(), false, null));
    }
    return result;
  }
}
