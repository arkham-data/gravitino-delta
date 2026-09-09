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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Closed Spark 3.5 / Delta 3.3 logical schema binding for basic name column mapping. Only mapping
 * IDs and generated physical names may be added by the native engine. This is semantic correlation,
 * not an allocation-order proof or a query/data digest.
 */
public final class DeltaSchemaContract {
  /** Native entrypoint normalization bound by this contract. */
  public enum OperationMode {
    /** Declared schema, preserving enforceable ordinary struct NOT NULL constraints. */
    SCHEMA_ONLY,
    /** Resolved query schema, recursively nullable including collection values. */
    QUERY
  }

  private static final String KIND = "SPARK_35_DELTA_33_NAME_MAPPING_V1";
  private static final String POLICY = "BASIC_NAME_MAPPING_V1";
  private static final String ID = "delta.columnMapping.id";
  private static final String PHYSICAL = "delta.columnMapping.physicalName";
  private static final String MODE = "delta.columnMapping.mode";
  private static final String MAX_ID = "delta.columnMapping.maxColumnId";
  private static final Set<String> LEAVES =
      set(
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
          "timestamp_ntz");
  private static final Pattern DECIMAL =
      Pattern.compile("decimal\\(([1-9][0-9]*),(0|[1-9][0-9]*)\\)");
  private static final Pattern PHYSICAL_NAME =
      Pattern.compile("col-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  private static final Set<String> EXCLUDED_METADATA =
      set(
          ID,
          PHYSICAL,
          "delta.columnMapping.nested.ids",
          "parquet.field.id",
          "parquet.field.nested.ids",
          "delta.generationExpression",
          "CURRENT_DEFAULT",
          "EXISTS_DEFAULT",
          "__CHAR_VARCHAR_TYPE_STRING");
  private static final Set<String> READER_FEATURES =
      set(
          "columnMapping",
          "deletionVectors",
          "timestampNtz",
          "typeWidening-preview",
          "typeWidening",
          "vacuumProtocolCheck",
          "variantType-preview",
          "v2Checkpoint");
  private static final Set<String> WRITER_ONLY_FEATURES =
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
          "checkpointProtection");
  private static final Set<String> EXCLUDED_PROPERTIES =
      set(
          "delta.coordinatedCommits.commitCoordinator-preview",
          "delta.coordinatedCommits.commitCoordinatorConf-preview",
          "delta.coordinatedCommits.tableConf-preview",
          "delta.redirectReaderWriter-preview",
          "delta.redirectWriterOnly-preview");
  private static final Set<String> ICEBERG_PROPERTIES =
      set("delta.enableIcebergCompatV1", "delta.enableIcebergCompatV2");
  private static final String FORMATS = "delta.universalFormat.enabledFormats";

  private final OperationMode operationMode;
  private final String requiredSchemaJson;

  private DeltaSchemaContract(OperationMode mode, String required) {
    if (mode == null) {
      throw invalid();
    }
    String canonical = DeltaJson.schema(required);
    ObjectNode schema = DeltaJson.object(canonical, DeltaJson.MAX_SCHEMA_BYTES);
    requireStruct(schema);
    validateType(schema, mode, false);
    operationMode = mode;
    requiredSchemaJson = canonical;
  }

  /**
   * Binds already-normalized Spark StructType JSON under the closed basic name-mapping policy.
   *
   * @param mode schema-only or recursively nullable query normalization
   * @param normalizedRequiredSchemaJson required logical schema without engine mapping metadata
   * @return immutable closed schema contract
   * @throws IllegalArgumentException for malformed, excluded or unnormalized schema
   */
  public static DeltaSchemaContract spark35Delta33NameMapping(
      OperationMode mode, String normalizedRequiredSchemaJson) {
    return new DeltaSchemaContract(mode, normalizedRequiredSchemaJson);
  }

  /**
   * @return bound native entrypoint normalization
   */
  public OperationMode operationMode() {
    return operationMode;
  }

  /**
   * @return canonical required logical schema JSON
   */
  public String requiredSchemaJson() {
    return requiredSchemaJson;
  }

  ObjectNode toJson() {
    ObjectNode value = DeltaJson.newObject();
    value.put("kind", KIND);
    value.put("operationMode", operationMode.name());
    value.set("requiredSchema", DeltaJson.object(requiredSchemaJson, DeltaJson.MAX_SCHEMA_BYTES));
    value.put("columnMappingMode", "name");
    value.put("featurePolicy", POLICY);
    return value;
  }

  static DeltaSchemaContract fromJson(JsonNode value) {
    fields(value, "kind", "operationMode", "requiredSchema", "columnMappingMode", "featurePolicy");
    if (!KIND.equals(DeltaJson.text(value, "kind"))
        || !POLICY.equals(DeltaJson.text(value, "featurePolicy"))
        || !"name".equals(DeltaJson.text(value, "columnMappingMode"))) {
      throw invalid();
    }
    return spark35Delta33NameMapping(
        OperationMode.valueOf(DeltaJson.text(value, "operationMode")),
        value.get("requiredSchema").toString());
  }

  void validateCandidate(DeltaSchemaSnapshot candidate) {
    String logical = validateMapped(candidate, operationMode);
    if (!requiredSchemaJson.equals(logical)) {
      throw invalid();
    }
  }

  /**
   * Admits a complete actual replacement base under this contract's closed feature policy. Callers
   * must obtain a fresh trusted native snapshot before reserving or starting native replacement;
   * this check does not establish snapshot freshness or replace postcommit verification.
   *
   * @param base complete native base schema, configuration and protocol, retained internally
   * @throws IllegalArgumentException if the base is excluded by the closed mapping policy
   */
  public void validateBase(DeltaSchemaSnapshot base) {
    validateMapped(base, OperationMode.SCHEMA_ONLY);
  }

  static void validateConfigurationAgreement(
      Map<String, String> action, Map<String, String> snapshot) {
    validateConfiguration(action);
    Set<String> keys = new HashSet<>(action.keySet());
    keys.addAll(snapshot.keySet());
    for (String key : keys) {
      if (key.startsWith("delta.columnMapping.")
          || EXCLUDED_PROPERTIES.contains(key)
          || ICEBERG_PROPERTIES.contains(key)
          || FORMATS.equals(key)) {
        if (!Objects.equals(action.get(key), snapshot.get(key))) {
          throw invalid();
        }
      }
    }
  }

  static DeltaProtocolDescriptor protocolAction(JsonNode value) {
    if (value == null || !value.isObject()) {
      throw invalid();
    }
    for (Iterator<String> names = value.fieldNames(); names.hasNext(); ) {
      String name = names.next();
      if (!set("minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures")
          .contains(name)) {
        throw invalid();
      }
    }
    long reader = DeltaJson.number(value, "minReaderVersion");
    long writer = DeltaJson.number(value, "minWriterVersion");
    if (reader < 1 || reader > Integer.MAX_VALUE || writer < 1 || writer > Integer.MAX_VALUE) {
      throw invalid();
    }
    if (value.has("readerFeatures") != (reader == 3)
        || value.has("writerFeatures") != (writer == 7)) {
      throw invalid();
    }
    DeltaProtocolDescriptor protocol =
        DeltaProtocolDescriptor.of(
            (int) reader,
            (int) writer,
            featureSet(value.get("readerFeatures")),
            featureSet(value.get("writerFeatures")));
    validateProtocol(protocol, false);
    return protocol;
  }

  private static String validateMapped(DeltaSchemaSnapshot snapshot, OperationMode mode) {
    long maximum = validateConfiguration(snapshot.configuration());
    ObjectNode logical = DeltaJson.object(snapshot.rawSchemaJson(), DeltaJson.MAX_SCHEMA_BYTES);
    requireStruct(logical);
    stripMapping(logical, "", maximum, new HashSet<>(), new HashSet<>());
    boolean timestampNtz = validateType(logical, mode, false);
    validateProtocol(snapshot.protocol(), timestampNtz);
    return DeltaJson.schema(logical.toString());
  }

  private static long validateConfiguration(Map<String, String> properties) {
    if (!"name".equals(properties.get(MODE))) {
      throw invalid();
    }
    String max = properties.get(MAX_ID);
    if (max == null || !max.matches("0|[1-9][0-9]{0,9}")) {
      throw invalid();
    }
    long maximum = Long.parseLong(max);
    if (maximum > Integer.MAX_VALUE) {
      throw invalid();
    }
    for (String key : EXCLUDED_PROPERTIES) {
      if (properties.containsKey(key)) {
        throw invalid();
      }
    }
    for (String key : ICEBERG_PROPERTIES) {
      String value = properties.get(key);
      if (value != null && !"false".equalsIgnoreCase(value)) {
        throw invalid();
      }
    }
    if (properties.containsKey(FORMATS) && !properties.get(FORMATS).isEmpty()) {
      throw invalid();
    }
    return maximum;
  }

  private static void validateProtocol(DeltaProtocolDescriptor protocol, boolean timestampNtz) {
    int reader = protocol.minReaderVersion();
    int writer = protocol.minWriterVersion();
    if (!((reader == 2 && (writer == 5 || writer == 6 || writer == 7))
        || (reader == 3 && writer == 7))) {
      throw invalid();
    }
    Set<String> readers = protocol.readerFeatures();
    Set<String> writers = protocol.writerFeatures();
    if (!READER_FEATURES.containsAll(readers) || !writers.containsAll(readers)) {
      throw invalid();
    }
    for (String feature : writers) {
      if (READER_FEATURES.contains(feature)) {
        if (!("columnMapping".equals(feature) && reader == 2) && !readers.contains(feature)) {
          throw invalid();
        }
      } else if (!WRITER_ONLY_FEATURES.contains(feature)) {
        throw invalid();
      }
    }
    if ((writer == 7 && !writers.contains("columnMapping"))
        || (reader == 3 && !readers.contains("columnMapping"))) {
      throw invalid();
    }
    if ((writers.contains("rowTracking") || writers.contains("clustering"))
        && !writers.contains("domainMetadata")) {
      throw invalid();
    }
    if (timestampNtz
        && (reader != 3
            || writer != 7
            || !readers.contains("timestampNtz")
            || !writers.contains("timestampNtz"))) {
      throw invalid();
    }
  }

  private static Set<String> featureSet(JsonNode value) {
    if (value == null) {
      return Collections.emptySet();
    }
    if (!value.isArray()) {
      throw invalid();
    }
    Set<String> features = new HashSet<>();
    for (JsonNode feature : value) {
      if (!feature.isTextual()) {
        throw invalid();
      }
      DeltaJson.requiredText(feature.textValue());
      if (!features.add(feature.textValue())) {
        throw invalid();
      }
    }
    return features;
  }

  private static void stripMapping(
      JsonNode type, String path, long max, Set<Long> ids, Set<String> paths) {
    if (type == null) {
      throw invalid();
    }
    if (!type.isObject()) {
      return;
    }
    if (type.has("fields")) {
      if (!type.get("fields").isArray()) {
        throw invalid();
      }
      for (JsonNode field : type.get("fields")) {
        JsonNode metadata = field.get("metadata");
        if (metadata == null || !metadata.isObject()) {
          throw invalid();
        }
        long id = DeltaJson.number(metadata, ID);
        String physical = DeltaJson.text(metadata, PHYSICAL);
        String fullPath = path + "." + physical;
        if (id < 1
            || id > Integer.MAX_VALUE
            || id > max
            || !ids.add(id)
            || !PHYSICAL_NAME.matcher(physical).matches()
            || !paths.add(fullPath)) {
          throw invalid();
        }
        ((ObjectNode) metadata).remove(Arrays.asList(ID, PHYSICAL));
        stripMapping(field.get("type"), fullPath, max, ids, paths);
      }
    }
    for (String child : new String[] {"elementType", "keyType", "valueType"}) {
      if (type.has(child)) {
        stripMapping(type.get(child), path + "." + child, max, ids, paths);
      }
    }
  }

  private static boolean validateType(JsonNode type, OperationMode mode, boolean inCollection) {
    if (type == null) {
      throw invalid();
    }
    if (type.isTextual()) {
      String name = type.textValue();
      if (LEAVES.contains(name)) {
        return "timestamp_ntz".equals(name);
      }
      Matcher decimal = DECIMAL.matcher(name);
      if (!decimal.matches() || decimal.group(1).length() > 2 || decimal.group(2).length() > 2) {
        throw invalid();
      }
      int precision = Integer.parseInt(decimal.group(1));
      int scale = Integer.parseInt(decimal.group(2));
      if (precision > 38 || scale > precision) {
        throw invalid();
      }
      return false;
    }
    String name = DeltaJson.text(type, "type");
    boolean ntz = false;
    switch (name) {
      case "struct":
        fields(type, "type", "fields");
        if (!type.get("fields").isArray()) {
          throw invalid();
        }
        Set<String> names = new HashSet<>();
        for (JsonNode field : type.get("fields")) {
          fields(field, "name", "type", "nullable", "metadata");
          String fieldName = DeltaJson.text(field, "name");
          DeltaJson.boundedText(fieldName, DeltaJson.MAX_SCHEMA_BYTES);
          if (!names.add(fieldName)) {
            throw invalid();
          }
          boolean nullable = bool(field, "nullable");
          if (!nullable && (mode == OperationMode.QUERY || inCollection)) {
            throw invalid();
          }
          JsonNode metadata = field.get("metadata");
          if (!metadata.isObject()) {
            throw invalid();
          }
          for (Iterator<String> keys = metadata.fieldNames(); keys.hasNext(); ) {
            String key = keys.next();
            if (EXCLUDED_METADATA.contains(key) || key.startsWith("delta.identity.")) {
              throw invalid();
            }
          }
          ntz |= validateType(field.get("type"), mode, inCollection);
        }
        return ntz;
      case "array":
        fields(type, "type", "elementType", "containsNull");
        if (!bool(type, "containsNull") && mode == OperationMode.QUERY) {
          throw invalid();
        }
        return validateType(type.get("elementType"), mode, true);
      case "map":
        fields(type, "type", "keyType", "valueType", "valueContainsNull");
        if (!bool(type, "valueContainsNull") && mode == OperationMode.QUERY) {
          throw invalid();
        }
        ntz = validateType(type.get("keyType"), mode, true);
        return validateType(type.get("valueType"), mode, true) || ntz;
      default:
        throw invalid();
    }
  }

  private static boolean bool(JsonNode value, String name) {
    JsonNode field = value.get(name);
    if (field == null || !field.isBoolean()) {
      throw invalid();
    }
    return field.booleanValue();
  }

  private static void requireStruct(JsonNode type) {
    if (!"struct".equals(DeltaJson.text(type, "type"))) {
      throw invalid();
    }
  }

  private static void fields(JsonNode value, String... names) {
    if (value == null || !value.isObject() || value.size() != names.length) {
      throw invalid();
    }
    for (String name : names) {
      if (!value.has(name)) {
        throw invalid();
      }
    }
  }

  private static Set<String> set(String... names) {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(names)));
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_SCHEMA_CONTRACT");
  }
}
