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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaNativeObservation {
  @Test
  @SuppressWarnings("ObjectToString")
  void reducesSecretsButIncludesThemInDigest() {
    DeltaNativeObservation a =
        DeltaNativeObservation.from(
            snapshot(Map.of("vendor.secret", "fixture-a", "delta.appendOnly", "true")));
    DeltaNativeObservation b =
        DeltaNativeObservation.from(
            snapshot(Map.of("vendor.secret", "fixture-b", "delta.appendOnly", "true")));
    assertFalse(a.safeConfiguration().containsKey("vendor.secret"));
    assertEquals(Map.of("delta.appendOnly", "true"), a.safeConfiguration());
    assertNotEquals(a.configurationDigest(), b.configurationDigest());
    assertFalse(a.toString().contains("fixture-a"));
  }

  @Test
  void malformedSafePropertiesAndForeignMarkersFailSanitized() {
    for (Map<String, String> properties :
        List.of(
            Map.of("delta.appendOnly", "TRUE"),
            Map.of("delta.columnMapping.mode", "unknown"),
            Map.of("delta.columnMapping.maxColumnId", "-1"),
            Map.of(DeltaOperationMarker.PROPERTY_KEY, "fixture-secret"))) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () -> DeltaNativeObservation.from(snapshot(properties)));
      assertFalse(error.toString().contains("fixture-secret"));
    }
  }

  static DeltaSnapshotMetadata snapshot(Map<String, String> configuration) {
    DeltaSnapshotMetadata result = mock(DeltaSnapshotMetadata.class);
    when(result.configuration()).thenReturn(configuration);
    when(result.uuid()).thenReturn("native-id");
    when(result.location()).thenReturn("s3a://bucket/t");
    when(result.schemaJson()).thenReturn("{\"type\":\"struct\",\"fields\":[]}");
    when(result.columns()).thenReturn(List.of());
    when(result.partitionColumns()).thenReturn(List.of());
    when(result.description()).thenReturn(Optional.empty());
    when(result.createdTime()).thenReturn(OptionalLong.of(-1));
    when(result.minReaderVersion()).thenReturn(1);
    when(result.minWriterVersion()).thenReturn(2);
    when(result.readerFeatures()).thenReturn(Set.of());
    when(result.writerFeatures()).thenReturn(Set.of());
    return result;
  }

  @TempDir Path temp;

  @Test
  void digestOrderLengthFramingAndOpaqueCreatedTime() {
    Map<String, String> first = new LinkedHashMap<>();
    first.put("\ud800\udc00", "astral");
    first.put("\ue000", "bmp");
    first.put("a", "ascii");
    Map<String, String> second = new LinkedHashMap<>();
    second.put("a", "ascii");
    second.put("\ue000", "bmp");
    second.put("\ud800\udc00", "astral");
    DeltaNativeObservation observation = DeltaNativeObservation.from(snapshot(first));
    assertEquals(
        "sha256-v1:49977a58120eeab6e6b1041786ae7253bdad53d7266194dae4be0eae437132b7",
        observation.configurationDigest());
    assertEquals(
        observation.configurationDigest(),
        DeltaNativeObservation.from(snapshot(second)).configurationDigest());
    assertNotEquals(
        DeltaNativeObservation.from(snapshot(Map.of("ab", "c"))).configurationDigest(),
        DeltaNativeObservation.from(snapshot(Map.of("a", "bc"))).configurationDigest());
    assertEquals(-1, observation.createdTime().getAsLong());
    assertEquals("ABSENT", observation.markerProvenance());
    assertFalse(observation.markerDigest().isPresent());
    assertThrows(
        UnsupportedOperationException.class, () -> observation.safeConfiguration().put("a", "b"));
  }

  @Test
  void boundsProtocolAndReceiptIdWithoutTruncation() {
    DeltaSnapshotMetadata metadata = snapshot(Map.of());
    when(metadata.uuid()).thenReturn("x".repeat(8192));
    assertDoesNotThrow(() -> DeltaNativeObservation.from(metadata));
    when(metadata.uuid()).thenReturn("x".repeat(8193));
    assertThrows(IllegalArgumentException.class, () -> DeltaNativeObservation.from(metadata));
    when(metadata.uuid()).thenReturn("id");
    Set<String> features = new HashSet<>();
    for (int i = 0; i < 4097; i++) {
      features.add("f" + i);
    }
    when(metadata.writerFeatures()).thenReturn(features);
    assertThrows(IllegalArgumentException.class, () -> DeltaNativeObservation.from(metadata));
    when(metadata.writerFeatures()).thenReturn(Set.of("f".repeat(8193)));
    assertThrows(IllegalArgumentException.class, () -> DeltaNativeObservation.from(metadata));
  }

  @Test
  void validForeignMarkerKeepsOnlyDigestAndClassification() {
    String schema =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(1, 0, "foreign-fixture-token", "s3a://bucket/t", schema);
    DeltaNativeObservation result =
        DeltaNativeObservation.from(
            snapshot(Map.of(DeltaOperationMarker.PROPERTY_KEY, marker.encode())));
    assertEquals("VALID_V1", result.markerProvenance());
    assertFalse(result.safeConfiguration().containsKey(DeltaOperationMarker.PROPERTY_KEY));
    assertFalse(result.markerDigest().get().contains("foreign-fixture-token"));
  }

  @Test
  void realReaderPreservesLargeCommentAndAdmissionRejectsIt() throws Exception {
    String schema =
        JsonUtils.anyFieldMapper()
            .writeValueAsString(
                Map.of(
                    "type",
                    "struct",
                    "fields",
                    List.of(
                        Map.of(
                            "name",
                            "c",
                            "type",
                            "string",
                            "nullable",
                            true,
                            "metadata",
                            Map.of("comment", "x".repeat(300000))))));
    Path log = Files.createDirectories(temp.resolve("native/_delta_log"));
    String metadata =
        JsonUtils.anyFieldMapper()
            .writeValueAsString(
                Map.of(
                    "metaData",
                    Map.of(
                        "id",
                        "native-fixture",
                        "format",
                        Map.of("provider", "parquet", "options", Map.of()),
                        "schemaString",
                        schema,
                        "partitionColumns",
                        List.of(),
                        "configuration",
                        Map.of("vendor.secret", "fixture-secret"),
                        "createdTime",
                        -1)));
    Files.writeString(
        log.resolve("00000000000000000000.json"),
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n" + metadata + "\n");
    try (DeltaMetadataReader reader = new DeltaMetadataReader(new Configuration(false))) {
      DeltaNativeObservation result =
          DeltaNativeObservation.from(reader.latest(temp.resolve("native").toUri().toString()));
      assertEquals(300000, result.columns().get(0).comment().length());
      assertThrows(
          IllegalArgumentException.class, () -> DeltaStorageAdmission.checkObservation(result));
      assertFalse(result.safeConfiguration().containsKey("vendor.secret"));
    }
  }

  @Test
  void observationAndFacadeDeepCopyNestedProjection() {
    Types.StructType type =
        Types.StructType.of(
            Types.StructType.Field.of("inner", Types.StringType.get(), true, "comment"));
    DeltaSnapshotMetadata metadata = snapshot(Map.of());
    when(metadata.columns())
        .thenReturn(List.of(Column.of("outer", type, null, Column.DEFAULT_VALUE_NOT_SET)));
    DeltaNativeObservation observation = DeltaNativeObservation.from(metadata);
    type.fields()[0] = Types.StructType.Field.of("changed", Types.IntegerType.get(), false, null);
    Types.StructType copy = (Types.StructType) observation.columns().get(0).dataType();
    assertEquals("inner", copy.fields()[0].name());
    copy.fields()[0] = type.fields()[0];
    assertEquals(
        "inner", ((Types.StructType) observation.columns().get(0).dataType()).fields()[0].name());
    assertThrows(UnsupportedOperationException.class, () -> observation.columns().clear());
  }

  @Test
  void finalizedV2MarkerAndExactSafeKeys() {
    String schema =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"c\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
    DeltaOperationMarker marker =
        DeltaOperationMarker.createWithSchemaContract(
            1,
            0,
            "foreign-fixture-token",
            "s3a://bucket/t",
            DeltaSchemaContract.spark35Delta33NameMapping(
                DeltaSchemaContract.OperationMode.SCHEMA_ONLY, schema));
    Map<String, String> safe =
        Map.of(
            "delta.columnMapping.mode",
            "name",
            "delta.columnMapping.maxColumnId",
            "9223372036854775807",
            "delta.appendOnly",
            "false",
            "delta.enableChangeDataFeed",
            "true",
            "delta.enableDeletionVectors",
            "false");
    Map<String, String> raw = new LinkedHashMap<>(safe);
    raw.put(DeltaOperationMarker.PROPERTY_KEY, marker.encode());
    raw.put("delta.columnMapping.vendorSecret", "fixture-secret");
    DeltaNativeObservation observation = DeltaNativeObservation.from(snapshot(raw));
    assertEquals("VALID_V2", observation.markerProvenance());
    assertEquals(safe, observation.safeConfiguration());
    assertEquals(1, observation.protocol().minReaderVersion());
    assertEquals(2, observation.protocol().minWriterVersion());
  }
}
