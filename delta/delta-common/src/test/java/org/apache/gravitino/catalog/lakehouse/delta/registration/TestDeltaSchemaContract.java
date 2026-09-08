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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaSchemaContract {
  @Test
  void decodesClosedV2RequiredLogicalSchema() {
    String schema =
        "{\"fields\":[{\"metadata\":{},\"name\":\"a\",\"nullable\":true,\"type\":\"long\"}],\"type\":\"struct\"}";
    String encoded =
        "{\"version\":2,\"operation\":\"CREATE\",\"id\":1,\"generation\":2,\"token\":\"token\",\"location\":\"file:/table\",\"schemaContract\":{\"kind\":\"SPARK_35_DELTA_33_NAME_MAPPING_V1\",\"operationMode\":\"SCHEMA_ONLY\",\"requiredSchema\":"
            + schema
            + ",\"columnMappingMode\":\"name\",\"featurePolicy\":\"BASIC_NAME_MAPPING_V1\"}}";
    DeltaOperationMarker marker = assertDoesNotThrow(() -> DeltaOperationMarker.decode(encoded));
    assertEquals(schema, marker.schemaJson());
    assertEquals(encoded, marker.encode());
  }

  private static final String SIMPLE =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"comment\":\"kept\",\"user\":1}}]}";
  private static final String PHYSICAL = "col-12345678-1234-1234-1234-123456789abc";
  private static final String PROTOCOL = "{\"minReaderVersion\":2,\"minWriterVersion\":5}";

  @Test
  void acceptsDifferentNativeAssignmentsButV1StillRequiresExactSchema() {
    DeltaOperationMarker marker = marker(SIMPLE);
    for (int id : new int[] {1, 89, Integer.MAX_VALUE}) {
      String actual =
          mapped(SIMPLE, id)
              .replace(PHYSICAL, String.format("col-12345678-1234-1234-1234-%012d", id));
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(marker, actual, props(id), protocol(), PROTOCOL).kind());
      DeltaOperationMarker v1 = DeltaOperationMarker.create(1, 2, "token", "file:/table", SIMPLE);
      assertEquals(
          DeltaCommitEvidence.Kind.AMBIGUOUS,
          new DeltaCommitActionReader()
              .inspect(
                  v1,
                  0,
                  0,
                  0,
                  actual,
                  stream(commit(v1, actual, props(id), PROTOCOL, "CREATE TABLE")))
              .kind());
    }
  }

  @Test
  void oldOverloadCannotDowngradeV2AndStillDrains() {
    DeltaOperationMarker marker = marker(SIMPLE);
    ByteArrayInputStream input =
        stream(commit(marker, mapped(SIMPLE, 1), props(1), PROTOCOL, "CREATE TABLE"));
    DeltaCommitEvidence result =
        new DeltaCommitActionReader().inspect(marker, 0, 0, 0, SIMPLE, input);
    assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, result.kind());
    assertEquals("MISSING_SCHEMA_CONTEXT", result.reason());
    assertEquals(0, input.available());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "byte",
        "short",
        "integer",
        "long",
        "float",
        "double",
        "boolean",
        "string",
        "binary",
        "date",
        "timestamp",
        "decimal(1,0)",
        "decimal(38,38)"
      })
  void acceptsClosedScalarTypes(String type) {
    String required = SIMPLE.replace("long", type);
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(required), mapped(required, 1), props(1), protocol(), PROTOCOL).kind());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "null",
        "void",
        "variant",
        "interval",
        "udt",
        "char(3)",
        "varchar(3)",
        "decimal(0,0)",
        "decimal(39,0)",
        "decimal(2,3)",
        "decimal(2,-1)",
        "LONG",
        "decimal(02,1)"
      })
  void rejectsExcludedAndUnknownTypes(String type) {
    assertThrows(IllegalArgumentException.class, () -> marker(SIMPLE.replace("long", type)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "delta.columnMapping.id",
        "delta.columnMapping.physicalName",
        "delta.columnMapping.nested.ids",
        "parquet.field.id",
        "parquet.field.nested.ids",
        "delta.generationExpression",
        "CURRENT_DEFAULT",
        "EXISTS_DEFAULT",
        "delta.identity.start",
        "delta.identity.step",
        "delta.identity.highWaterMark",
        "delta.identity.allowExplicitInsert",
        "__CHAR_VARCHAR_TYPE_STRING"
      })
  void rejectsExcludedRequiredMetadata(String key) {
    ObjectNode required = schema(SIMPLE);
    metadata(required, 0).put(key, "x");
    assertThrows(IllegalArgumentException.class, () -> marker(required.toString()));
  }

  @Test
  void rejectsEveryLogicalDifferenceAndUnknownGeneratedMetadata() {
    DeltaOperationMarker marker = marker(SIMPLE);
    for (String changed :
        new String[] {
          SIMPLE.replace("\"a\"", "\"b\""),
          SIMPLE.replace("long", "integer"),
          SIMPLE.replace("true", "false"),
          SIMPLE.replace("kept", "changed"),
          SIMPLE.replace("\"user\":1", "\"user\":2"),
          SIMPLE.replace(",\"user\":1", "")
        }) {
      ambiguous(inspect(marker, mapped(changed, 1), props(1), protocol(), PROTOCOL));
    }
    ObjectNode extra = schema(mapped(SIMPLE, 1));
    metadata(extra, 0).put("engine.unknown", true);
    ambiguous(inspect(marker, extra.toString(), props(1), protocol(), PROTOCOL));
    for (String key :
        new String[] {
          "delta.columnMapping.nested.ids", "parquet.field.id", "parquet.field.nested.ids"
        }) {
      ObjectNode nestedId = schema(mapped(SIMPLE, 1));
      metadata(nestedId, 0).put(key, 2);
      ambiguous(inspect(marker, nestedId.toString(), props(2), protocol(), PROTOCOL));
    }
  }

  @Test
  void validatesCollectionsRecursivelyAndPreservesOrderAndScale() {
    String nested =
        "{\"type\":\"map\",\"keyType\":"
            + SIMPLE
            + ",\"valueType\":{\"type\":\"array\",\"elementType\":"
            + SIMPLE
            + ",\"containsNull\":true},\"valueContainsNull\":true}";
    ObjectNode required = schema(SIMPLE);
    ((ObjectNode) required.path("fields").get(0)).set("type", schema(nested));
    String json = required.toString();
    assertDoesNotThrow(
        () ->
            DeltaSchemaContract.spark35Delta33NameMapping(
                DeltaSchemaContract.OperationMode.QUERY, json));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaSchemaContract.spark35Delta33NameMapping(
                DeltaSchemaContract.OperationMode.QUERY, json.replace("true", "false")));
    assertThrows(
        IllegalArgumentException.class,
        () -> marker(json.replace("\"nullable\":true", "\"nullable\":false")));
    assertDoesNotThrow(() -> marker(SIMPLE.replace("true", "false")));
    assertDoesNotThrow(
        () -> marker(json.replace("\"containsNull\":true", "\"containsNull\":false")));
    ambiguous(
        inspect(
            marker(json),
            mapped(json.replace("\"containsNull\":true", "\"containsNull\":false"), 1),
            props(3),
            protocol(),
            PROTOCOL));
    String decimal = SIMPLE.replace("long", "decimal(12,2)");
    ambiguous(
        inspect(
            marker(decimal),
            mapped(decimal.replace("12,2", "12,3"), 1),
            props(1),
            protocol(),
            PROTOCOL));
    ObjectNode two = schema(SIMPLE);
    ((ArrayNode) two.path("fields")).add(two.path("fields").get(0).deepCopy());
    ((ObjectNode) two.path("fields").get(1)).put("name", "b");
    String ordered = two.toString();
    ((ObjectNode) two.path("fields").get(0)).put("name", "b");
    ((ObjectNode) two.path("fields").get(1)).put("name", "a");
    ObjectNode reversed = schema(mapped(two.toString(), 1));
    metadata(reversed, 1)
        .put("delta.columnMapping.physicalName", "col-12345678-1234-1234-1234-123456789abd");
    ambiguous(inspect(marker(ordered), reversed.toString(), props(2), protocol(), PROTOCOL));
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "2147483648", "4294967297", "1.0", "\"1\"", "null"})
  void rejectsInvalidMappingIds(String id) {
    ObjectNode actual = schema(mapped(SIMPLE, 1));
    metadata(actual, 0).set("delta.columnMapping.id", schema("{\"id\":" + id + "}").get("id"));
    ambiguous(
        inspect(marker(SIMPLE), actual.toString(), props(Integer.MAX_VALUE), protocol(), PROTOCOL));
  }

  @Test
  void rejectsMissingPartialAndInvalidPhysicalAssignments() {
    ambiguous(inspect(marker(SIMPLE), SIMPLE, props(1), protocol(), PROTOCOL));
    for (String key : new String[] {"delta.columnMapping.id", "delta.columnMapping.physicalName"}) {
      ObjectNode actual = schema(mapped(SIMPLE, 1));
      metadata(actual, 0).remove(key);
      ambiguous(inspect(marker(SIMPLE), actual.toString(), props(1), protocol(), PROTOCOL));
    }
    for (String name :
        new String[] {
          "a",
          "col-not-a-uuid",
          "col-12345678-1234-1234-1234-123456789abg",
          "COL-12345678-1234-1234-1234-123456789abc"
        }) {
      ObjectNode actual = schema(mapped(SIMPLE, 1));
      metadata(actual, 0).put("delta.columnMapping.physicalName", name);
      ambiguous(inspect(marker(SIMPLE), actual.toString(), props(1), protocol(), PROTOCOL));
    }
  }

  @Test
  void rejectsDuplicateIdsAndFullPhysicalPathsButAllowsSameNameUnderDifferentParents() {
    ObjectNode required = schema(SIMPLE);
    ((ObjectNode) required.path("fields").get(0)).set("type", schema(SIMPLE));
    String actual = mapped(required.toString(), 1);
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(required.toString()), actual, props(2), protocol(), PROTOCOL).kind());
    ObjectNode duplicateId = schema(actual);
    metadata((ObjectNode) duplicateId.path("fields").get(0).get("type"), 0)
        .put("delta.columnMapping.id", 1);
    ambiguous(
        inspect(
            marker(required.toString()), duplicateId.toString(), props(2), protocol(), PROTOCOL));
    ObjectNode sibling = schema(SIMPLE);
    ((ArrayNode) sibling.path("fields")).add(sibling.path("fields").get(0).deepCopy());
    ((ObjectNode) sibling.path("fields").get(1)).put("name", "b");
    String duplicatePath = mapped(sibling.toString(), 1);
    ambiguous(inspect(marker(sibling.toString()), duplicatePath, props(2), protocol(), PROTOCOL));
  }

  @ParameterizedTest
  @ValueSource(strings = {"-1", "01", "+1", "1.0", " 1", "2147483648", "0", ""})
  void rejectsInvalidOrInsufficientMaximumId(String max) {
    Map<String, String> properties = props(1);
    properties.put("delta.columnMapping.maxColumnId", max);
    ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), properties, protocol(), PROTOCOL));
  }

  @Test
  void rejectsWrongMappingModeAndConfigurationContextMismatch() {
    for (String mode : new String[] {"none", "id", "NAME", ""}) {
      Map<String, String> properties = props(1);
      properties.put("delta.columnMapping.mode", mode);
      ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), properties, protocol(), PROTOCOL));
    }
    DeltaOperationMarker marker = marker(SIMPLE);
    String actual = mapped(SIMPLE, 1);
    DeltaSchemaSnapshot snapshot = DeltaSchemaSnapshot.of(0, "new", actual, props(2), protocol());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(
                marker,
                0,
                0,
                snapshot,
                Optional.empty(),
                stream(commit(marker, actual, props(1), PROTOCOL, "CREATE TABLE"))));
  }

  @Test
  void protocolAdmissionIsClosedAndFeatureFieldsAreNotLossy() {
    for (String bad :
        new String[] {
          "{\"minReaderVersion\":1,\"minWriterVersion\":5}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":4}",
          "{\"minReaderVersion\":3,\"minWriterVersion\":6}",
          "{\"minReaderVersion\":4,\"minWriterVersion\":8}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":5,\"readerFeatures\":[]}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":5,\"writerFeatures\":[]}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":7}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":7,\"writerFeatures\":[]}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":7,\"writerFeatures\":[\"columnMapping\",\"columnMapping\"]}",
          "{\"minReaderVersion\":2,\"minWriterVersion\":5,\"unknown\":1}",
          "{\"minReaderVersion\":2.0,\"minWriterVersion\":5}"
        }) {
      ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), protocol(), bad));
    }
    for (int writer : new int[] {5, 6}) {
      String wire = PROTOCOL.replace(":5", ":" + writer);
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(
                  marker(SIMPLE),
                  mapped(SIMPLE, 1),
                  props(1),
                  DeltaProtocolDescriptor.of(
                      2, writer, Collections.emptySet(), Collections.emptySet()),
                  wire)
              .kind());
    }
    for (int reader : new int[] {2, 3}) {
      DeltaProtocolDescriptor descriptor =
          DeltaProtocolDescriptor.of(
              reader,
              7,
              reader == 3 ? set("columnMapping") : Collections.emptySet(),
              set("columnMapping"));
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), descriptor, wire(descriptor))
              .kind());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "unknown",
        "coordinatedCommits-preview",
        "redirectReaderWriter-preview",
        "redirectWriterOnly-preview",
        "icebergCompatV1",
        "icebergCompatV2",
        "rowTracking",
        "clustering",
        "deletionVectors",
        "timestampNtz"
      })
  void rejectsUnknownExcludedOrUnpairedWriterFeatures(String feature) {
    DeltaProtocolDescriptor descriptor =
        DeltaProtocolDescriptor.of(2, 7, Collections.emptySet(), set("columnMapping", feature));
    ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), descriptor, wire(descriptor)));
  }

  @Test
  void admitsKnownCompatibilityFeaturesAndTheirDependencies() {
    Set<String> readers =
        set(
            "columnMapping",
            "deletionVectors",
            "timestampNtz",
            "typeWidening-preview",
            "typeWidening",
            "vacuumProtocolCheck",
            "variantType-preview",
            "v2Checkpoint");
    Set<String> writers = new HashSet<>(readers);
    writers.addAll(
        set(
            "appendOnly",
            "invariants",
            "checkConstraints",
            "changeDataFeed",
            "generatedColumns",
            "identityColumns",
            "rowTracking",
            "domainMetadata",
            "clustering",
            "allowColumnDefaults",
            "inCommitTimestamp",
            "checkpointProtection"));
    DeltaProtocolDescriptor descriptor = DeltaProtocolDescriptor.of(3, 7, readers, writers);
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), descriptor, wire(descriptor)).kind());
    DeltaProtocolDescriptor unpaired =
        DeltaProtocolDescriptor.of(3, 7, readers, set("columnMapping"));
    ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), unpaired, wire(unpaired)));
  }

  @Test
  void timestampNtzRequiresItsReaderAndWriterFeature() {
    String required = SIMPLE.replace("long", "timestamp_ntz");
    ambiguous(inspect(marker(required), mapped(required, 1), props(1), protocol(), PROTOCOL));
    DeltaProtocolDescriptor descriptor =
        DeltaProtocolDescriptor.of(
            3, 7, set("columnMapping", "timestampNtz"), set("columnMapping", "timestampNtz"));
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(required), mapped(required, 1), props(1), descriptor, wire(descriptor))
            .kind());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "delta.coordinatedCommits.commitCoordinator-preview",
        "delta.coordinatedCommits.commitCoordinatorConf-preview",
        "delta.coordinatedCommits.tableConf-preview",
        "delta.redirectReaderWriter-preview",
        "delta.redirectWriterOnly-preview"
      })
  void rejectsEvenOrphanOrEmptyExcludedConfiguration(String key) {
    Map<String, String> config = props(1);
    config.put(key, "");
    ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), config, protocol(), PROTOCOL));
  }

  @Test
  void rejectsActiveAndMalformedIcebergConfigurationButAdmitsExplicitInactiveValues() {
    for (String key : new String[] {"delta.enableIcebergCompatV1", "delta.enableIcebergCompatV2"}) {
      for (String value : new String[] {"true", "invalid", "", " false "}) {
        Map<String, String> config = props(1);
        config.put(key, value);
        ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), config, protocol(), PROTOCOL));
      }
      Map<String, String> config = props(1);
      config.put(key, "false");
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(marker(SIMPLE), mapped(SIMPLE, 1), config, protocol(), PROTOCOL).kind());
    }
    for (String value : new String[] {"iceberg", "hudi", ",", " "}) {
      Map<String, String> config = props(1);
      config.put("delta.universalFormat.enabledFormats", value);
      ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), config, protocol(), PROTOCOL));
    }
    Map<String, String> config = props(1);
    config.put("delta.universalFormat.enabledFormats", "");
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(SIMPLE), mapped(SIMPLE, 1), config, protocol(), PROTOCOL).kind());
  }

  @Test
  void replacementRequiresRealBaseButMayResetAssignmentsAndOmitUnchangedProtocol() {
    DeltaOperationMarker marker =
        DeltaOperationMarker.replaceWithSchemaContract(
            1, 2, "token", "file:/table", contract(SIMPLE), 4, "old");
    String actual = mapped(SIMPLE, 1);
    DeltaSchemaSnapshot candidate = DeltaSchemaSnapshot.of(5, "new", actual, props(1), protocol());
    DeltaSchemaSnapshot base =
        DeltaSchemaSnapshot.of(4, "old", mapped(SIMPLE, 99), props(99), protocol());
    String commit = commit(marker, actual, props(1), null, "REPLACE TABLE");
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        new DeltaCommitActionReader()
            .inspect(marker, 5, 5, candidate, Optional.of(base), stream(commit))
            .kind());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(marker, 5, 5, candidate, Optional.empty(), stream(commit)));
    for (DeltaSchemaSnapshot wrong :
        new DeltaSchemaSnapshot[] {
          DeltaSchemaSnapshot.of(3, "old", base.rawSchemaJson(), props(99), protocol()),
          DeltaSchemaSnapshot.of(4, "wrong", base.rawSchemaJson(), props(99), protocol()),
          DeltaSchemaSnapshot.of(
              4,
              "old",
              base.rawSchemaJson(),
              props(99),
              DeltaProtocolDescriptor.of(2, 6, Collections.emptySet(), Collections.emptySet()))
        }) {
      ambiguous(
          new DeltaCommitActionReader()
              .inspect(marker, 5, 5, candidate, Optional.of(wrong), stream(commit)));
    }
    Map<String, String> disabled = props(99);
    disabled.put("delta.columnMapping.mode", "none");
    DeltaSchemaSnapshot invalidBase =
        DeltaSchemaSnapshot.of(4, "old", base.rawSchemaJson(), disabled, protocol());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(marker, 5, 5, candidate, Optional.of(invalidBase), stream(commit)));
    Map<String, String> excluded = props(99);
    excluded.put("delta.redirectWriterOnly-preview", "");
    invalidBase = DeltaSchemaSnapshot.of(4, "old", base.rawSchemaJson(), excluded, protocol());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(marker, 5, 5, candidate, Optional.of(invalidBase), stream(commit)));
  }

  @Test
  void sameCommitMetadataProtocolMarkerAndExactSnapshotRemainMandatory() {
    DeltaOperationMarker marker = marker(SIMPLE);
    String actual = mapped(SIMPLE, 1);
    DeltaSchemaSnapshot candidate = DeltaSchemaSnapshot.of(0, "new", actual, props(1), protocol());
    ambiguous(inspect(marker, actual, props(1), protocol(), null));
    String actions = commit(marker, actual, props(1), PROTOCOL, "CREATE TABLE");
    for (String altered :
        new String[] {
          actions.replace("\"new\"", "\"wrong\""),
          actions + "{bad}",
          actions + "{\"protocol\":" + PROTOCOL + "}",
          "{\"commitInfo\":{\"operation\":\"CREATE TABLE\"}}",
          actions.replace("\\\"token\\\"", "\\\"different\\\""),
          actions.replace("CREATE TABLE", "CREATE TABLE AS SELECT")
        }) {
      ambiguous(
          new DeltaCommitActionReader()
              .inspect(marker, 0, 0, candidate, Optional.empty(), stream(altered)));
    }
    DeltaSchemaSnapshot different =
        DeltaSchemaSnapshot.of(0, "new", mapped(SIMPLE, 2), props(2), protocol());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(marker, 0, 0, different, Optional.empty(), stream(actions)));
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(marker, 0, 1, candidate, Optional.empty(), stream(actions)));
    DeltaOperationMarker query =
        DeltaOperationMarker.createWithSchemaContract(
            1,
            2,
            "token",
            "file:/table",
            DeltaSchemaContract.spark35Delta33NameMapping(
                DeltaSchemaContract.OperationMode.QUERY, SIMPLE));
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        new DeltaCommitActionReader()
            .inspect(
                query,
                0,
                0,
                candidate,
                Optional.empty(),
                stream(commit(query, actual, props(1), PROTOCOL, "CREATE TABLE AS SELECT")))
            .kind());
    ambiguous(
        new DeltaCommitActionReader()
            .inspect(
                query,
                0,
                0,
                candidate,
                Optional.empty(),
                stream(commit(query, actual, props(1), PROTOCOL, "CREATE TABLE"))));
  }

  @Test
  void codecRejectsUnknownPoliciesFieldsVersionsAndCrossOperationFields() {
    String encoded = marker(SIMPLE).encode();
    assertEquals(encoded, DeltaOperationMarker.decode(encoded).encode());
    for (String bad :
        new String[] {
          encoded.replace("BASIC_NAME_MAPPING_V1", "BASIC_NAME_MAPPING_V2"),
          encoded.replace("SPARK_35_DELTA_33_NAME_MAPPING_V1", "unknown"),
          encoded.replace("SCHEMA_ONLY", "UNKNOWN"),
          encoded.replace("\"version\":2", "\"version\":3"),
          encoded.replace("\"columnMappingMode\":\"name\"", "\"columnMappingMode\":\"id\""),
          encoded.replace("\"kind\":", "\"unknown\":1,\"kind\":"),
          encoded.replace("\"id\":1", "\"baseVersion\":4,\"id\":1")
        }) {
      assertThrows(IllegalArgumentException.class, () -> DeltaOperationMarker.decode(bad));
    }
  }

  @Test
  void contextIsDetachedImmutableAndStructurallyBoundedWithoutImposingV2OnV1() {
    Set<String> features = set("columnMapping");
    DeltaProtocolDescriptor descriptor = DeltaProtocolDescriptor.of(3, 7, features, features);
    features.clear();
    assertEquals(set("columnMapping"), descriptor.readerFeatures());
    assertThrows(UnsupportedOperationException.class, () -> descriptor.writerFeatures().clear());
    Map<String, String> config = props(1);
    DeltaSchemaSnapshot snapshot = DeltaSchemaSnapshot.of(0, "new", SIMPLE, config, descriptor);
    config.clear();
    assertEquals("name", snapshot.configuration().get("delta.columnMapping.mode"));
    assertThrows(UnsupportedOperationException.class, () -> snapshot.configuration().clear());
    assertDoesNotThrow(
        () -> DeltaProtocolDescriptor.of(1, 1, Collections.emptySet(), Collections.emptySet()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaProtocolDescriptor.of(2, 5, set("columnMapping"), Collections.emptySet()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaProtocolDescriptor.of(2, 5, Collections.emptySet(), set("columnMapping")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaProtocolDescriptor.of(0, 0, Collections.emptySet(), Collections.emptySet()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaProtocolDescriptor.of(2, 5, null, Collections.emptySet()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(-1, "new", SIMPLE, props(1), descriptor));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, " ", SIMPLE, props(1), descriptor));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "new", SIMPLE, null, descriptor));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "new", SIMPLE, props(1), null));
    Map<String, String> invalid = props(1);
    invalid.put("bad", null);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "new", SIMPLE, invalid, descriptor));
    invalid.put("bad", "\ud800");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "new", SIMPLE, invalid, descriptor));
  }

  @Test
  void preservesRequiredMetadataNumericKindsAndDecimalScale() {
    for (String number : new String[] {"1", "1.0", "1.00", "0.12345678901234567890123456789"}) {
      String required = SIMPLE.replace("\"user\":1", "\"user\":" + number);
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(marker(required), mapped(required, 1), props(1), protocol(), PROTOCOL).kind());
      String changed = required.replace("\"user\":" + number, "\"user\":2.0");
      ambiguous(inspect(marker(required), mapped(changed, 1), props(1), protocol(), PROTOCOL));
    }
    ambiguous(
        inspect(
            marker(SIMPLE),
            mapped(SIMPLE.replace("\"user\":1", "\"user\":1.0"), 1),
            props(1),
            protocol(),
            PROTOCOL));
    String decimal = SIMPLE.replace("\"user\":1", "\"user\":1.0");
    ambiguous(
        inspect(
            marker(decimal),
            mapped(decimal.replace("1.0", "1.00"), 1),
            props(1),
            protocol(),
            PROTOCOL));
  }

  @Test
  void malformedActualFieldTypeFailsClosedInsteadOfEscapingAsRuntimeError() {
    ObjectNode actual = schema(mapped(SIMPLE, 1));
    ((ObjectNode) actual.get("fields").get(0)).remove("type");
    ambiguous(inspect(marker(SIMPLE), actual.toString(), props(1), protocol(), PROTOCOL));
  }

  @Test
  void snapshotContextEnforcesIndependentUtf8AndAggregateConfigurationBounds() {
    Map<String, String> config = new HashMap<>();
    config.put(repeat('k', 16384), repeat('v', 1048576));
    assertDoesNotThrow(() -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.put(repeat('k', 16385), "v");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.clear();
    config.put("k", repeat('v', 1048577));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.clear();
    config.put("k", repeat('é', 524289));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.clear();
    for (int i = 0; i < 4096; i++) {
      config.put("k" + i, "");
    }
    assertDoesNotThrow(() -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.put("overflow", "");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.clear();
    String big = repeat('v', 1048576);
    for (int i = 0; i < 15; i++) {
      config.put("k" + i, big);
    }
    assertDoesNotThrow(() -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    config.put("k15", big);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, "id", SIMPLE, config, protocol()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaSchemaSnapshot.of(
                0, "id", "{\"large\":\"" + repeat('x', 131072) + "\"}", props(1), protocol()));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaSchemaSnapshot.of(0, repeat('x', 8193), SIMPLE, props(1), protocol()));
  }

  @Test
  void rejectsMalformedNormalizedShapesWithoutInventingMapKeyNullability() {
    for (String invalid :
        new String[] {
          SIMPLE.replace("\"nullable\":true", "\"nullable\":\"true\""),
          SIMPLE.replace("\"metadata\":{\"comment\":\"kept\",\"user\":1}", "\"metadata\":null"),
          SIMPLE.replace("\"long\"", "{\"type\":\"long\"}"),
          SIMPLE.replace("\"long\"", "{\"type\":\"array\",\"elementType\":\"long\"}"),
          SIMPLE.replace(
              "\"long\"",
              "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"long\",\"valueContainsNull\":true,\"keyContainsNull\":true}"),
          SIMPLE.replace("\"fields\":[", "\"unknown\":1,\"fields\":[")
        }) {
      assertThrows(IllegalArgumentException.class, () -> marker(invalid));
    }
  }

  @Test
  void newOverloadKeepsV1ProtocolAndIdentityRulesUnchanged() {
    DeltaOperationMarker v1 = DeltaOperationMarker.create(1, 2, "token", "file:/table", SIMPLE);
    DeltaSchemaSnapshot observation =
        DeltaSchemaSnapshot.of(
            0,
            "independent",
            SIMPLE,
            Collections.emptyMap(),
            DeltaProtocolDescriptor.of(1, 1, Collections.emptySet(), Collections.emptySet()));
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        new DeltaCommitActionReader()
            .inspect(
                v1,
                0,
                0,
                observation,
                Optional.empty(),
                stream(commit(v1, SIMPLE, Collections.emptyMap(), null, "CREATE TABLE AS SELECT")))
            .kind());
  }

  @Test
  void wireProtocolRequiresExactPresenceAndCandidateAgreement() {
    DeltaProtocolDescriptor descriptor =
        DeltaProtocolDescriptor.of(3, 7, set("columnMapping"), set("columnMapping"));
    String valid = wire(descriptor);
    for (String invalid :
        new String[] {
          valid.replace(",\"readerFeatures\":[\"columnMapping\"]", ""),
          valid.replace("\"readerFeatures\":[\"columnMapping\"]", "\"readerFeatures\":[]"),
          valid.replace("\"writerFeatures\":[\"columnMapping\"]", "\"writerFeatures\":[]"),
          valid.replace("\"readerFeatures\":[\"columnMapping\"]", "\"readerFeatures\":null"),
          valid.replace("\"writerFeatures\":[\"columnMapping\"]", "\"writerFeatures\":[1]"),
          PROTOCOL
        }) {
      ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), descriptor, invalid));
    }
    DeltaProtocolDescriptor unsupported =
        DeltaProtocolDescriptor.of(2, 4, Collections.emptySet(), Collections.emptySet());
    ambiguous(inspect(marker(SIMPLE), mapped(SIMPLE, 1), props(1), unsupported, PROTOCOL));
  }

  @Test
  void queryChecksEachCollectionFlagAndNestedKeyValueFieldIndependently() {
    ObjectNode map = DeltaJson.newObject().put("type", "map").put("valueContainsNull", true);
    map.set("keyType", schema(SIMPLE));
    ObjectNode array = map.putObject("valueType").put("type", "array").put("containsNull", true);
    array.set("elementType", schema(SIMPLE));
    ObjectNode required = schema(SIMPLE);
    ((ObjectNode) required.get("fields").get(0)).set("type", map);
    String json = required.toString();
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(json), mapped(json, 1), props(3), protocol(), PROTOCOL).kind());
    String nonnullElements = json.replace("\"containsNull\":true", "\"containsNull\":false");
    assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(marker(nonnullElements), mapped(nonnullElements, 1), props(3), protocol(), PROTOCOL)
            .kind());
    for (int mutation = 0; mutation < 5; mutation++) {
      ObjectNode changed = required.deepCopy();
      ObjectNode field = (ObjectNode) changed.get("fields").get(0);
      ObjectNode changedMap = (ObjectNode) field.get("type");
      ObjectNode changedArray = (ObjectNode) changedMap.get("valueType");
      switch (mutation) {
        case 0:
          field.put("nullable", false);
          break;
        case 1:
          changedMap.put("valueContainsNull", false);
          break;
        case 2:
          changedArray.put("containsNull", false);
          break;
        case 3:
          ((ObjectNode) changedMap.get("keyType").get("fields").get(0)).put("nullable", false);
          break;
        default:
          ((ObjectNode) changedArray.get("elementType").get("fields").get(0))
              .put("nullable", false);
      }
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaSchemaContract.spark35Delta33NameMapping(
                  DeltaSchemaContract.OperationMode.QUERY, changed.toString()));
    }
  }

  @Test
  void preservesTextualMappedLogicalNamesWithoutAddingAnIdentifierSyntaxPolicy() {
    for (String name : new String[] {"", " ", "a.b", "a b", "a=b"}) {
      String required = SIMPLE.replace("\"name\":\"a\"", "\"name\":\"" + name + "\"");
      assertEquals(
          DeltaCommitEvidence.Kind.MATCHED,
          inspect(marker(required), mapped(required, 1), props(1), protocol(), PROTOCOL).kind());
    }
  }

  private static String repeat(char ch, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, ch);
    return new String(chars);
  }

  private static DeltaSchemaContract contract(String required) {
    return DeltaSchemaContract.spark35Delta33NameMapping(
        DeltaSchemaContract.OperationMode.SCHEMA_ONLY, required);
  }

  private static DeltaOperationMarker marker(String required) {
    return DeltaOperationMarker.createWithSchemaContract(
        1, 2, "token", "file:/table", contract(required));
  }

  private static DeltaProtocolDescriptor protocol() {
    return DeltaProtocolDescriptor.of(2, 5, Collections.emptySet(), Collections.emptySet());
  }

  private static ObjectNode schema(String value) {
    return DeltaJson.object(value, DeltaJson.MAX_SCHEMA_BYTES);
  }

  private static ObjectNode metadata(ObjectNode value, int index) {
    return (ObjectNode) value.path("fields").get(index).get("metadata");
  }

  private static String mapped(String required, int id) {
    ObjectNode actual = schema(required);
    assign(actual, new long[] {id});
    return actual.toString();
  }

  private static void assign(JsonNode type, long[] next) {
    if (type.isObject()) {
      if (type.has("fields")) {
        for (JsonNode field : type.get("fields")) {
          ((ObjectNode) field.get("metadata"))
              .put("delta.columnMapping.id", next[0]++)
              .put("delta.columnMapping.physicalName", PHYSICAL);
          assign(field.get("type"), next);
        }
      }
      for (String child : new String[] {"elementType", "keyType", "valueType"}) {
        if (type.has(child)) {
          assign(type.get(child), next);
        }
      }
    }
  }

  private static Map<String, String> props(int maximum) {
    Map<String, String> result = new HashMap<>();
    result.put("delta.columnMapping.mode", "name");
    result.put("delta.columnMapping.maxColumnId", Integer.toString(maximum));
    return result;
  }

  private static String wire(DeltaProtocolDescriptor descriptor) {
    ObjectNode node =
        DeltaJson.newObject()
            .put("minReaderVersion", descriptor.minReaderVersion())
            .put("minWriterVersion", descriptor.minWriterVersion());
    if (descriptor.minReaderVersion() == 3) {
      descriptor.readerFeatures().forEach(node.putArray("readerFeatures")::add);
    }
    if (descriptor.minWriterVersion() == 7) {
      descriptor.writerFeatures().forEach(node.putArray("writerFeatures")::add);
    }
    return node.toString();
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  private static ByteArrayInputStream stream(String value) {
    return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String commit(
      DeltaOperationMarker marker,
      String actual,
      Map<String, String> config,
      String protocol,
      String operation) {
    ObjectNode metadata = DeltaJson.newObject().put("id", "new").put("schemaString", actual);
    ObjectNode properties = metadata.putObject("configuration");
    config.forEach(properties::put);
    properties.put(DeltaOperationMarker.PROPERTY_KEY, marker.encode());
    ObjectNode info = DeltaJson.newObject().put("operation", operation);
    if (marker.baseVersion().isPresent()) {
      info.put("readVersion", marker.baseVersion().getAsLong());
    }
    return (protocol == null ? "" : "{\"protocol\":" + protocol + "}\n")
        + "{\"metaData\":"
        + metadata
        + "}\n{\"commitInfo\":"
        + info
        + "}";
  }

  private static DeltaCommitEvidence inspect(
      DeltaOperationMarker marker,
      String actual,
      Map<String, String> config,
      DeltaProtocolDescriptor descriptor,
      String wire) {
    return new DeltaCommitActionReader()
        .inspect(
            marker,
            0,
            0,
            DeltaSchemaSnapshot.of(0, "new", actual, config, descriptor),
            Optional.empty(),
            stream(commit(marker, actual, config, wire, "CREATE TABLE")));
  }

  private static void ambiguous(DeltaCommitEvidence evidence) {
    assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, evidence.kind());
    assertTrue(!evidence.uuid().isPresent());
    assertTrue(!evidence.schemaJson().isPresent());
  }
}
