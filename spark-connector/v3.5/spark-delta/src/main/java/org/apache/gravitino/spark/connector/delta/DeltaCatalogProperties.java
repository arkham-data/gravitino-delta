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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

final class DeltaCatalogProperties {
  static final String UPDATE_ENABLED = "spark.databricks.delta.catalog.update.enabled";
  static final String MAPPING_DEFAULT =
      "spark.databricks.delta.properties.defaults.columnMapping.mode";
  static final String SESSION_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
  static final String EXTENSION = "io.delta.sql.DeltaSparkSessionExtension";

  static void validateSession(SparkSession session) {
    if (!SESSION_CATALOG.equals(session.conf().get("spark.sql.catalog.spark_catalog", ""))
        || !Arrays.asList(session.conf().get("spark.sql.extensions", "").split(","))
            .contains(EXTENSION)) {
      throw new IllegalArgumentException(
          "Configure spark.sql.catalog.spark_catalog="
              + SESSION_CATALOG
              + " and spark.sql.extensions="
              + EXTENSION
              + " before creating SparkSession");
    }
    if (!"false".equalsIgnoreCase(session.conf().get(UPDATE_ENABLED, "false"))) {
      throw new IllegalArgumentException(
          UPDATE_ENABLED
              + " must remain false throughout the session, including native commit hooks");
    }
  }

  static void validate(Map<String, String> values) {
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new IllegalArgumentException("Invalid Delta options");
      }
      String key = entry.getKey().toLowerCase(Locale.ROOT);
      if (!keys.add(key)) {
        throw new IllegalArgumentException("Conflicting case-insensitive Delta options");
      }
      if (key.startsWith("option.delta.")) {
        throw new IllegalArgumentException(
            "Write options cannot override bound Delta table metadata");
      }
      while (key.startsWith("option.")) {
        key = key.substring(7);
      }
      if (key.equals("location")
          || key.equals("path")
          || key.startsWith("gravitino.registration.")
          || key.startsWith("delta.registration.")
          || key.equals("delta.columnmapping.maxcolumnid")
          || key.startsWith("delta.coordinatedcommits.")
          || key.startsWith("delta.redirect")
          || key.startsWith("delta.universalformat.")
          || key.startsWith("delta.enableicebergcompat")
          || key.equals("replacewhere")
          || key.equals("mergeschema")
          || key.equals("overwriteschema")
          || key.startsWith("fs.")
          || key.startsWith("spark.")
          || key.startsWith("hadoop.")
          || key.startsWith("s3.")
          || key.startsWith("s3a.")
          || key.startsWith("aws.")
          || key.contains("credential")
          || key.contains("secret")
          || key.contains("password")
          || key.equals("endpoint")) {
        throw new IllegalArgumentException("Reserved or unsupported Delta option");
      }
      if (key.equals("provider") && !"delta".equalsIgnoreCase(entry.getValue())) {
        throw new IllegalArgumentException("The Delta adapter requires provider delta");
      }
    }
  }

  static void validateWrite(Map<String, String> values) {
    validate(values);
    for (String property : values.keySet()) {
      String key = property.toLowerCase(Locale.ROOT);
      if (key.startsWith("delta.") || key.equals("versionasof") || key.equals("timestampasof")) {
        throw new IllegalArgumentException(
            "Write options cannot override the bound Delta contract");
      }
    }
  }

  static void validateSchema(StructType schema) {
    try {
      validateSchemaNode(new ObjectMapper().readTree(schema.json()));
    } catch (IOException error) {
      throw new IllegalArgumentException("Invalid Delta schema");
    }
  }

  private static void validateSchemaNode(JsonNode node) {
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String key = field.getKey().toLowerCase(Locale.ROOT);
        if (key.startsWith("delta.columnmapping.")
            || key.startsWith("delta.identity.")
            || key.startsWith("parquet.field.")
            || key.equals("delta.generationexpression")
            || key.equals("current_default")
            || key.equals("exists_default")
            || key.equals("__char_varchar_type_string")) {
          throw new IllegalArgumentException(
              "Excluded engine schema metadata requires a separately qualified contract");
        }
        validateSchemaNode(field.getValue());
      }
    } else if (node.isArray()) {
      for (JsonNode child : node) {
        validateSchemaNode(child);
      }
    }
  }

  static String mapping(Map<String, String> values, String defaultMode) {
    String mode = defaultMode;
    for (Map.Entry<String, String> entry : values.entrySet()) {
      if ("delta.columnMapping.mode".equalsIgnoreCase(entry.getKey())) {
        mode = entry.getValue();
      }
    }
    if (!"none".equalsIgnoreCase(mode) && !"name".equalsIgnoreCase(mode)) {
      throw new IllegalArgumentException("Only none or name Delta column mapping is admitted");
    }
    return mode.toLowerCase(Locale.ROOT);
  }
}
