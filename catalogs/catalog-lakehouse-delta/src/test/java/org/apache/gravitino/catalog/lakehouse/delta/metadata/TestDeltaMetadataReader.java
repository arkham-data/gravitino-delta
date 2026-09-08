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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructField;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaMetadataReader {
  @TempDir Path temp;

  @Test
  void readsNativeCheckpointBeyondThirtyTwoCommitsAndLaterMetadata() throws Exception {
    Path table = temp.resolve("native");
    MetadataFixtures.nativeTable(table, Map.of("delta.checkpointInterval", "50"), 36);
    DeltaSnapshotMetadata before;
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      before = reader.latest(MetadataFixtures.location(table));
    }
    MetadataFixtures.nativeCheckpoint(table, 36);
    MetadataFixtures.pruneBefore(table, 36);
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      DeltaSnapshotMetadata actual = reader.latest(MetadataFixtures.location(table));
      assertEquals(before.uuid(), actual.uuid());
      assertEquals(36, actual.version());
      assertEquals(before.schemaJson(), actual.schemaJson());
      assertEquals(before.configuration(), actual.configuration());
      assertEquals(before.partitionColumns(), actual.partitionColumns());
      assertEquals(before.minReaderVersion(), actual.minReaderVersion());
      assertEquals(before.minWriterVersion(), actual.minWriterVersion());
      assertEquals(36, reader.atVersion(MetadataFixtures.location(table), 36).version());
      assertThrows(
          DeltaMetadataException.class,
          () -> reader.atVersion(MetadataFixtures.location(table), 35));
      String evolved = MetadataFixtures.SCHEMA.add("later", IntegerType.INTEGER).toJson();
      MetadataFixtures.actions(
          table,
          37,
          MetadataFixtures.metadata(before.uuid(), evolved, Map.of("purpose", "evolved")));
      assertEquals(evolved, reader.latest(MetadataFixtures.location(table)).schemaJson());
      assertEquals(
          "evolved",
          reader.latest(MetadataFixtures.location(table)).configuration().get("purpose"));
      assertEquals(
          before.schemaJson(), reader.atVersion(MetadataFixtures.location(table), 36).schemaJson());
      MetadataFixtures.actions(table, 39, "{\"commitInfo\":{\"timestamp\":39}}");
      assertThrows(
          DeltaMetadataException.class, () -> reader.latest(MetadataFixtures.location(table)));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"multipart", "v2-json", "v2-parquet", "v2-classic-sidecar"})
  void readsCheckpointFamiliesAndLaterMetadata(String kind) throws Exception {
    Path table = temp.resolve(kind);
    MetadataFixtures.checkpointFixture(table, kind);
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertFixture(
          reader.latest(MetadataFixtures.location(table)),
          kind,
          40,
          MetadataFixtures.SCHEMA.toJson());
      String changed = MetadataFixtures.SCHEMA.add("newField", IntegerType.INTEGER).toJson();
      MetadataFixtures.actions(
          table,
          41,
          MetadataFixtures.metadata(MetadataFixtures.ID, changed, Map.of("fixture", "later")));
      assertFixture(reader.latest(MetadataFixtures.location(table)), kind, 41, changed);
      assertFixture(
          reader.atVersion(MetadataFixtures.location(table), 40),
          kind,
          40,
          MetadataFixtures.SCHEMA.toJson());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "stale", "corrupt"})
  void discoversCheckpointWithMissingStaleOrCorruptHint(String hint) throws Exception {
    Path table = temp.resolve(hint);
    MetadataFixtures.nativeTable(table, Map.of(), 3);
    MetadataFixtures.nativeCheckpoint(table, 3);
    String id;
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      id = r.latest(MetadataFixtures.location(table)).uuid();
    }
    MetadataFixtures.pruneBefore(table, 3);
    Path last = MetadataFixtures.log(table).resolve("_last_checkpoint");
    Files.deleteIfExists(MetadataFixtures.log(table).resolve("._last_checkpoint.crc"));
    if (hint.equals("missing")) Files.delete(last);
    else
      Files.writeString(last, hint.equals("corrupt") ? "{ broken" : "{\"version\":1,\"size\":2}");
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      DeltaSnapshotMetadata actual = r.latest(MetadataFixtures.location(table));
      assertEquals(id, actual.uuid());
      assertEquals(3, actual.version());
      assertEquals(MetadataFixtures.SCHEMA.toJson(), actual.schemaJson());
      assertEquals(MetadataFixtures.PARTITIONS, actual.partitionColumns());
      assertEquals(1, actual.minReaderVersion());
      assertEquals(2, actual.minWriterVersion());
    }
  }

  @Test
  void incompleteMultipartCannotReplaceRequiredHistory() throws Exception {
    Path table = temp.resolve("incomplete");
    MetadataFixtures.checkpointFixture(table, "multipart-incomplete");
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertThrows(DeltaMetadataException.class, () -> r.latest(MetadataFixtures.location(table)));
    }
  }

  @Test
  @SuppressWarnings("ObjectToString")
  void preservesRawSchemaFeaturesAndReturnsDeeplyDetachedCollections() throws Exception {
    Path table = temp.resolve("raw");
    String raw =
        "{ \"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"leaf\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"comment\":\"note\"}}]},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"physical\",\"delta.generationExpression\":\"2 + 3\",\"CURRENT_DEFAULT\":\"5\",\"EXISTS_DEFAULT\":\"5\",\"delta.identity.start\":1,\"delta.identity.step\":2,\"delta.typeChanges\":[{\"fromType\":\"byte\",\"toType\":\"integer\"}]}},{\"name\":\"z\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"a\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    MetadataFixtures.unitTable(
        table, MetadataFixtures.PROTOCOL, raw, Map.of("sensitive", "do-not-print"));
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      DeltaSnapshotMetadata actual = r.latest(MetadataFixtures.location(table));
      assertEquals(raw, actual.schemaJson());
      assertEquals(MetadataFixtures.ID, actual.uuid());
      assertEquals(Optional.of("native description"), actual.description());
      assertEquals(OptionalLong.of(123), actual.createdTime());
      assertThrows(UnsupportedOperationException.class, () -> actual.configuration().put("x", "y"));
      assertThrows(UnsupportedOperationException.class, () -> actual.partitionColumns().clear());
      assertThrows(UnsupportedOperationException.class, () -> actual.columns().clear());
      assertThrows(UnsupportedOperationException.class, () -> actual.readerFeatures().add("x"));
      assertThrows(UnsupportedOperationException.class, () -> actual.writerFeatures().add("x"));
      ((Types.StructType) actual.columns().get(0).dataType()).fields()[0] = null;
      assertNotNull(((Types.StructType) actual.columns().get(0).dataType()).fields()[0]);
      assertFalse(actual.toString().contains("do-not-print"));
    }
  }

  @Test
  void readsWriterUnsupportedButRejectsUnknownReaderFeaturesWithoutLeaking() throws Exception {
    Path table = temp.resolve("writer");
    MetadataFixtures.unitTable(
        table,
        "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[],\"writerFeatures\":[\"generatedColumns\",\"identityColumns\"]}}",
        MetadataFixtures.SCHEMA.toJson(),
        Map.of());
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(
          Set.of("generatedColumns", "identityColumns"),
          r.latest(MetadataFixtures.location(table)).writerFeatures());
      MetadataFixtures.actions(
          table,
          1,
          "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"secret-unsupported-feature\"],\"writerFeatures\":[]}}");
      DeltaMetadataException e =
          assertThrows(
              DeltaMetadataException.class, () -> r.latest(MetadataFixtures.location(table)));
      assertEquals("SNAPSHOT_UNAVAILABLE", e.reason());
      assertNull(e.getCause());
      assertEquals(0, e.getSuppressed().length);
      assertFalse(e.toString().contains("secret"));
    }
  }

  @Test
  void enforcesConfigurationUtf8BoundsBeforeKeepingObservation() throws Exception {
    for (String kind : List.of("count", "key", "value", "aggregate")) {
      Map<String, String> config = new HashMap<>();
      if (kind.equals("count")) for (int i = 0; i < 4097; i++) config.put("k" + i, "v");
      if (kind.equals("key")) config.put("é".repeat(8193), "v");
      if (kind.equals("value")) config.put("k", "é".repeat(524289));
      if (kind.equals("aggregate"))
        for (int i = 0; i < 17; i++) config.put("k" + i, "x".repeat(1024 * 1024));
      Path table = temp.resolve(kind);
      MetadataFixtures.unitTable(
          table, MetadataFixtures.PROTOCOL, MetadataFixtures.SCHEMA.toJson(), config);
      try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
        assertEquals(
            "CONFIGURATION_LIMIT",
            assertThrows(
                    DeltaMetadataException.class, () -> r.latest(MetadataFixtures.location(table)))
                .reason());
      }
    }
  }

  @Test
  void verifiesMarkerBearingCreateControlAndRejectsSupersededOrMissingExactCommit()
      throws Exception {
    Path table = temp.resolve("create");
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(
            1, 0, "token", MetadataFixtures.location(table), MetadataFixtures.SCHEMA.toJson());
    MetadataFixtures.nativeTable(table, Map.of(), 0);
    MetadataFixtures.markerBearingCreateControl(table, marker.encode());
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, r.verify(marker).kind());
      MetadataFixtures.actions(table, 1, "{\"commitInfo\":{\"timestamp\":1}}");
      MetadataFixtures.nativeCheckpoint(table, 1);
      Files.delete(MetadataFixtures.commit(table, 0));
      assertEquals(1, r.latest(MetadataFixtures.location(table)).version());
      assertEquals(
          "SNAPSHOT_UNAVAILABLE",
          assertThrows(DeltaMetadataException.class, () -> r.verify(marker)).reason());
    }
    Path later = temp.resolve("later");
    DeltaOperationMarker inherited =
        DeltaOperationMarker.create(
            2, 0, "token", MetadataFixtures.location(later), MetadataFixtures.SCHEMA.toJson());
    MetadataFixtures.nativeTable(later, Map.of(), 1);
    MetadataFixtures.markerBearingCreateControl(later, inherited.encode());
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, r.verify(inherited).kind());
    }
  }

  @Test
  void verifiesReplacementOnlyAgainstCorrectBaseAndEntireCandidateStream() throws Exception {
    Path table = temp.resolve("replacement");
    MetadataFixtures.unitTable(
        table, MetadataFixtures.PROTOCOL, MetadataFixtures.SCHEMA.toJson(), Map.of());
    DeltaOperationMarker marker =
        DeltaOperationMarker.replace(
            1,
            1,
            "replace",
            MetadataFixtures.location(table),
            MetadataFixtures.SCHEMA.toJson(),
            0,
            MetadataFixtures.ID);
    String commitInfo = "{\"commitInfo\":{\"operation\":\"REPLACE TABLE\",\"readVersion\":0}}";
    MetadataFixtures.actions(
        table,
        1,
        commitInfo,
        MetadataFixtures.metadata(
            "replacement-id", MetadataFixtures.SCHEMA.toJson(), marker.injectInto(Map.of())));
    try (DeltaMetadataReader r = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, r.verify(marker).kind());
      DeltaOperationMarker wrong =
          DeltaOperationMarker.replace(
              1,
              1,
              "replace",
              MetadataFixtures.location(table),
              MetadataFixtures.SCHEMA.toJson(),
              0,
              "wrong-id");
      assertEquals(
          "BASE_UUID_MISMATCH",
          assertThrows(DeltaMetadataException.class, () -> r.verify(wrong)).reason());
      Files.writeString(MetadataFixtures.commit(table, 1), "\n{ broken", StandardOpenOption.APPEND);
      try {
        assertNotEquals(DeltaCommitEvidence.Kind.MATCHED, r.verify(marker).kind());
      } catch (DeltaMetadataException expected) {
        assertNull(expected.getCause());
      }
    }
  }

  @Test
  void verifiesProtocolShapedMappedCreateAndReplacementWithFreshAssignments() throws Exception {
    Path table = temp.resolve("mapped-protocol-shaped");
    String required = MetadataFixtures.SCHEMA.toJson();
    DeltaSchemaContract contract =
        DeltaSchemaContract.spark35Delta33NameMapping(
            DeltaSchemaContract.OperationMode.SCHEMA_ONLY, required);
    DeltaOperationMarker create =
        DeltaOperationMarker.createWithSchemaContract(
            1, 0, "create", MetadataFixtures.location(table), contract);
    Map<String, String> config =
        Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "102");
    String protocol = "{\"protocol\":{\"minReaderVersion\":2,\"minWriterVersion\":5}}";
    // Protocol-shaped same-commit controls: Kernel cannot natively write the reserved marker.
    MetadataFixtures.actions(
        table,
        0,
        protocol,
        MetadataFixtures.metadata(
            MetadataFixtures.ID, mappedControlSchema(100), create.injectInto(config)),
        "{\"commitInfo\":{\"operation\":\"CREATE TABLE\"}}");
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, reader.verify(create).kind());
      DeltaOperationMarker replace =
          DeltaOperationMarker.replaceWithSchemaContract(
              1, 1, "replace", MetadataFixtures.location(table), contract, 0, MetadataFixtures.ID);
      Map<String, String> reset =
          Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "3");
      MetadataFixtures.actions(
          table,
          1,
          MetadataFixtures.metadata(
              "replacement", mappedControlSchema(1), replace.injectInto(reset)),
          "{\"commitInfo\":{\"operation\":\"REPLACE TABLE\",\"readVersion\":0}}");
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, reader.verify(replace).kind());
      assertEquals(
          "3",
          reader
              .latest(MetadataFixtures.location(table))
              .configuration()
              .get("delta.columnMapping.maxColumnId"));
    }
  }

  @Test
  void mappedReplacementRejectsUnmappedTrustedBase() throws Exception {
    Path table = temp.resolve("mapping-mode-change-control");
    MetadataFixtures.unitTable(
        table, MetadataFixtures.PROTOCOL, MetadataFixtures.SCHEMA.toJson(), Map.of());
    DeltaOperationMarker replace =
        DeltaOperationMarker.replaceWithSchemaContract(
            1,
            1,
            "replace",
            MetadataFixtures.location(table),
            DeltaSchemaContract.spark35Delta33NameMapping(
                DeltaSchemaContract.OperationMode.SCHEMA_ONLY, MetadataFixtures.SCHEMA.toJson()),
            0,
            MetadataFixtures.ID);
    MetadataFixtures.actions(
        table,
        1,
        "{\"protocol\":{\"minReaderVersion\":2,\"minWriterVersion\":5}}",
        MetadataFixtures.metadata(
            "replacement",
            mappedControlSchema(1),
            replace.injectInto(
                Map.of(
                    "delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "3"))),
        "{\"commitInfo\":{\"operation\":\"REPLACE TABLE\",\"readVersion\":0}}");
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, reader.verify(replace).kind());
    }
  }

  @Test
  void preservesRawSchemaLargerThanRegistrationMarkerEnvelope() throws Exception {
    String large =
        MetadataFixtures.SCHEMA
            .add(
                new StructField(
                    "large",
                    IntegerType.INTEGER,
                    true,
                    FieldMetadata.builder().putString("comment", "x".repeat(300000)).build()))
            .toJson();
    Path table = temp.resolve("large-schema");
    MetadataFixtures.unitTable(table, MetadataFixtures.PROTOCOL, large, Map.of());
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(large, reader.latest(MetadataFixtures.location(table)).schemaJson());
    }
  }

  @Test
  void rejectsUnavailableInvalidAndClosedReadsWithoutExposingLocation() {
    DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration());
    String location = temp.resolve("credential-secret").toUri().toString();
    DeltaMetadataException unavailable =
        assertThrows(DeltaMetadataException.class, () -> reader.latest(location));
    assertEquals("SNAPSHOT_UNAVAILABLE", unavailable.reason());
    assertFalse(unavailable.toString().contains("credential-secret"));
    assertNull(unavailable.getCause());
    assertEquals(
        "INVALID_VERSION",
        assertThrows(DeltaMetadataException.class, () -> reader.atVersion(location, -1)).reason());
    reader.close();
    reader.close();
    assertEquals(
        "READER_CLOSED",
        assertThrows(DeltaMetadataException.class, () -> reader.latest(location)).reason());
  }

  @Test
  void preservesMissingOptionalMetadataAndRejectsFutureReaderVersion() throws Exception {
    Path table = temp.resolve("optional");
    ObjectNode envelope =
        (ObjectNode)
            MetadataFixtures.JSON.readTree(
                MetadataFixtures.metadata(
                    MetadataFixtures.ID, MetadataFixtures.SCHEMA.toJson(), Map.of()));
    ((ObjectNode) envelope.get("metaData")).remove(List.of("description", "createdTime"));
    MetadataFixtures.actions(table, 0, MetadataFixtures.PROTOCOL, envelope.toString());
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      DeltaSnapshotMetadata actual = reader.latest(MetadataFixtures.location(table));
      assertEquals(Optional.empty(), actual.description());
      assertEquals(OptionalLong.empty(), actual.createdTime());
      MetadataFixtures.actions(
          table, 1, "{\"protocol\":{\"minReaderVersion\":4,\"minWriterVersion\":7}}");
      assertEquals(
          "SNAPSHOT_UNAVAILABLE",
          assertThrows(
                  DeltaMetadataException.class,
                  () -> reader.latest(MetadataFixtures.location(table)))
              .reason());
    }
  }

  @Test
  void rejectsCoordinatedCommitsInsteadOfReturningStaleBackfilledMetadata() throws Exception {
    Path table = temp.resolve("coordinated");
    String protocol =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":7,\"writerFeatures\":[\"coordinatedCommits-preview\"]}}";
    MetadataFixtures.unitTable(
        table,
        protocol,
        MetadataFixtures.SCHEMA.toJson(),
        Map.of("delta.coordinatedCommits.commitCoordinator-preview", "unit-secret-coordinator"));
    Path unbackfilled =
        Files.createDirectories(MetadataFixtures.log(table).resolve("_commits"))
            .resolve("00000000000000000001.11111111-1111-1111-1111-111111111111.json");
    Files.writeString(
        unbackfilled,
        MetadataFixtures.metadata(
            "later-unbackfilled-id",
            MetadataFixtures.SCHEMA.add("later", IntegerType.INTEGER).toJson(),
            Map.of()));
    Engine engine = DefaultEngine.create(MetadataFixtures.configuration());
    assertEquals(
        0,
        Table.forPath(engine, MetadataFixtures.location(table))
            .getLatestSnapshot(engine)
            .getVersion(engine));
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      DeltaMetadataException failure =
          assertThrows(
              DeltaMetadataException.class, () -> reader.latest(MetadataFixtures.location(table)));
      assertEquals("UNSUPPORTED_COORDINATED_COMMITS", failure.reason());
      assertNull(failure.getCause());
      assertFalse(failure.toString().contains("unit-secret"));
      assertEquals(
          "UNSUPPORTED_COORDINATED_COMMITS",
          assertThrows(
                  DeltaMetadataException.class,
                  () -> reader.atVersion(MetadataFixtures.location(table), 0))
              .reason());
      DeltaOperationMarker marker =
          DeltaOperationMarker.create(
              1, 0, "token", MetadataFixtures.location(table), MetadataFixtures.SCHEMA.toJson());
      assertEquals(
          "UNSUPPORTED_COORDINATED_COMMITS",
          assertThrows(DeltaMetadataException.class, () -> reader.verify(marker)).reason());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "delta.coordinatedCommits.commitCoordinator-preview",
        "delta.coordinatedCommits.commitCoordinatorConf-preview",
        "delta.coordinatedCommits.tableConf-preview"
      })
  void rejectsEmptyOrOrphanCoordinatedCommitConfiguration(String key) throws Exception {
    Path table = temp.resolve("orphan");
    MetadataFixtures.unitTable(
        table, MetadataFixtures.PROTOCOL, MetadataFixtures.SCHEMA.toJson(), Map.of(key, ""));
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertEquals(
          "UNSUPPORTED_COORDINATED_COMMITS",
          assertThrows(
                  DeltaMetadataException.class,
                  () -> reader.latest(MetadataFixtures.location(table)))
              .reason());
    }
  }

  @Test
  void rejectsCandidateIdDisagreementBetweenCheckpointAndExactJson() throws Exception {
    Path table = temp.resolve("candidate-id");
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(
            1, 0, "token", MetadataFixtures.location(table), MetadataFixtures.SCHEMA.toJson());
    MetadataFixtures.nativeTable(table, Map.of(), 0);
    MetadataFixtures.markerBearingCreateControl(table, marker.encode());
    MetadataFixtures.nativeCheckpoint(table, 0);
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      String checkpointId = reader.latest(MetadataFixtures.location(table)).uuid();
      Path json = MetadataFixtures.commit(table, 0);
      List<String> actions = new ArrayList<>();
      for (String line : Files.readAllLines(json)) {
        ObjectNode action = (ObjectNode) MetadataFixtures.JSON.readTree(line);
        if (action.has("metaData")) {
          ((ObjectNode) action.get("metaData")).put("id", "mutated-json-id");
        }
        actions.add(action.toString());
      }
      Files.deleteIfExists(json.resolveSibling("." + json.getFileName() + ".crc"));
      Files.writeString(json, String.join("\n", actions) + "\n");
      assertEquals(checkpointId, reader.latest(MetadataFixtures.location(table)).uuid());
      assertEquals(
          "CANDIDATE_ID_MISMATCH",
          assertThrows(DeltaMetadataException.class, () -> reader.verify(marker)).reason());
    }
  }

  @Test
  void rejectsCandidateMarkerDisagreementBetweenCheckpointAndExactJson() throws Exception {
    Path table = temp.resolve("candidate-marker");
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(
            1, 0, "token", MetadataFixtures.location(table), MetadataFixtures.SCHEMA.toJson());
    MetadataFixtures.nativeTable(table, Map.of(), 0);
    MetadataFixtures.nativeCheckpoint(table, 0);
    MetadataFixtures.markerBearingCreateControl(table, marker.encode());
    try (DeltaMetadataReader reader = new DeltaMetadataReader(MetadataFixtures.configuration())) {
      assertFalse(
          reader
              .latest(MetadataFixtures.location(table))
              .configuration()
              .containsKey(DeltaOperationMarker.PROPERTY_KEY));
      assertEquals(
          "CANDIDATE_MARKER_MISMATCH",
          assertThrows(DeltaMetadataException.class, () -> reader.verify(marker)).reason());
    }
  }

  private static String mappedControlSchema(int firstId) throws Exception {
    ObjectNode schema =
        (ObjectNode) MetadataFixtures.JSON.readTree(MetadataFixtures.SCHEMA.toJson());
    for (int index = 0; index < schema.get("fields").size(); index++) {
      ObjectNode metadata = (ObjectNode) schema.get("fields").get(index).get("metadata");
      metadata.put("delta.columnMapping.id", firstId + index);
      metadata.put(
          "delta.columnMapping.physicalName",
          String.format("col-12345678-1234-1234-1234-%012d", firstId + index));
    }
    return schema.toString();
  }

  private static void assertFixture(
      DeltaSnapshotMetadata actual, String kind, long version, String schema) {
    assertEquals(MetadataFixtures.ID, actual.uuid());
    assertEquals(version, actual.version());
    assertEquals(schema, actual.schemaJson());
    assertEquals(MetadataFixtures.PARTITIONS, actual.partitionColumns());
    assertEquals(kind.startsWith("v2") ? 3 : 1, actual.minReaderVersion());
    assertEquals(kind.startsWith("v2") ? 7 : 2, actual.minWriterVersion());
    assertEquals(
        kind.startsWith("v2") ? Set.of("v2Checkpoint") : Set.of(), actual.readerFeatures());
    assertEquals(
        kind.startsWith("v2") ? Set.of("v2Checkpoint") : Set.of(), actual.writerFeatures());
  }
}
