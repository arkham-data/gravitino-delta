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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Shared bounded, duplicate-rejecting JSON parsing and schema normalization. */
final class DeltaJson {
  static final int MAX_MARKER_BYTES = 256 * 1024;
  static final int MAX_SCHEMA_BYTES = 128 * 1024;
  static final int MAX_TEXT_BYTES = 8 * 1024;
  static final int MAX_ACTION_BYTES = 1024 * 1024;
  static final int MAX_ACTION_TOKENS = 65536;
  static final JsonFactory FACTORY =
      JsonFactory.builder()
          .streamReadConstraints(
              StreamReadConstraints.builder()
                  .maxNestingDepth(64)
                  .maxStringLength(MAX_MARKER_BYTES)
                  .maxNumberLength(1000)
                  .build())
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .disable(StreamReadFeature.AUTO_CLOSE_SOURCE)
          .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
          .build();
  private static final ObjectMapper MAPPER =
      JsonMapper.builder(FACTORY)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();

  private DeltaJson() {}

  static ObjectNode object(String json, int maxBytes) {
    boundedText(json, maxBytes);
    try (JsonParser parser = FACTORY.createParser(json)) {
      JsonNode value = MAPPER.readTree(parser);
      if (value == null || !value.isObject() || parser.nextToken() != null) {
        throw new IllegalArgumentException("INVALID_JSON_OBJECT");
      }
      return (ObjectNode) value;
    } catch (IOException e) {
      throw new IllegalArgumentException("INVALID_JSON_OBJECT");
    }
  }

  static ObjectNode newObject() {
    return MAPPER.createObjectNode();
  }

  static String schema(String json) {
    return canonical(object(json, MAX_SCHEMA_BYTES), 1).toString();
  }

  static String text(JsonNode object, String field) {
    JsonNode value = object.get(field);
    if (value == null || !value.isTextual()) {
      throw new IllegalArgumentException("INVALID_TEXT_FIELD");
    }
    return value.textValue();
  }

  static long number(JsonNode object, String field) {
    JsonNode value = object.get(field);
    if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
      throw new IllegalArgumentException("INVALID_INTEGER_FIELD");
    }
    return value.longValue();
  }

  static void requiredText(String value) {
    boundedText(value, MAX_TEXT_BYTES);
    if (value.trim().isEmpty()) {
      throw new IllegalArgumentException("EMPTY_TEXT_FIELD");
    }
  }

  // Count UTF-8 before allocating a byte array; reject invalid surrogate encodings as well.
  static void boundedText(String value, int maxBytes) {
    if (value == null || value.length() > maxBytes) {
      throw new IllegalArgumentException("TEXT_LIMIT");
    }
    int bytes = 0;
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (Character.isHighSurrogate(ch)) {
        if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) {
          throw new IllegalArgumentException("INVALID_UNICODE");
        }
        bytes += 4;
      } else if (Character.isLowSurrogate(ch)) {
        throw new IllegalArgumentException("INVALID_UNICODE");
      } else {
        bytes += ch < 0x80 ? 1 : ch < 0x800 ? 2 : 3;
      }
      if (bytes > maxBytes) {
        throw new IllegalArgumentException("TEXT_LIMIT");
      }
    }
  }

  private static JsonNode canonical(JsonNode value, int depth) {
    if (value.isContainerNode() && depth > 63) {
      throw new IllegalArgumentException("SCHEMA_DEPTH_LIMIT");
    }
    if (value.isObject()) {
      ObjectNode sorted = newObject();
      List<String> names = new ArrayList<>();
      value.fieldNames().forEachRemaining(names::add);
      Collections.sort(names);
      for (String name : names) {
        boundedText(name, MAX_SCHEMA_BYTES);
        sorted.set(name, canonical(value.get(name), depth + 1));
      }
      return sorted;
    }
    if (value.isArray()) {
      ArrayNode result = MAPPER.createArrayNode();
      for (JsonNode child : value) {
        result.add(canonical(child, depth + 1));
      }
      return result;
    }
    if (value.isTextual()) {
      boundedText(value.textValue(), MAX_SCHEMA_BYTES);
    }
    return value;
  }
}
