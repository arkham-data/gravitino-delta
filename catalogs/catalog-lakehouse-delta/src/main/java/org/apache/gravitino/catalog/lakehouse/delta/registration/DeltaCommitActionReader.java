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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Correlates an exact native commit action stream with one immutable registration marker.
 *
 * <p>No storage, Spark or Kernel calls occur here. The enclosing verifier must open the exact
 * candidate object below the durable stored location, obtain latest version and one successful
 * Kernel snapshot observation, check protocol validity and authorization, and establish the trusted
 * replacement base UUID lineage. Current commit actions cannot independently reconstruct that prior
 * UUID lineage.
 */
public final class DeltaCommitActionReader {
  private static final Set<String> CREATE_OPERATIONS =
      new HashSet<>(
          Arrays.asList(
              "CREATE TABLE", "CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"));
  private static final Set<String> REPLACE_OPERATIONS =
      new HashSet<>(
          Arrays.asList(
              "REPLACE TABLE", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"));

  /** Creates a stateless action reader; instances may be shared. */
  public DeltaCommitActionReader() {}

  /**
   * Consumes a commit to EOF without closing the caller-owned stream, then correlates its actions.
   *
   * <p>On malformed input the remaining bytes are drained without further interpretation. An I/O
   * failure prevents guaranteed EOF consumption and always yields AMBIGUOUS. Every ambiguous result
   * has empty observed metadata. VM errors are not swallowed.
   *
   * <p>Each action must be a single-key JSON object with an object value. Normal opaque actions,
   * including repeated add/remove actions, are consumed without interpreting Delta invariants.
   * Metadata, CommitInfo and protocol are singleton actions. Each action is bounded to 1 MiB UTF-8,
   * 65,536 tokens, 64 nesting levels, 256 KiB strings and 1,000-character numbers. No total commit
   * byte/action limit is imposed; resource-limit exhaustion yields AMBIGUOUS. Leading BOMs and
   * non-object action envelopes are unsupported.
   *
   * @param expected durable reserved marker, never an arbitrary caller's replacement binding
   * @param candidateVersion version of the exact commit object opened by the trusted caller
   * @param latestVersion independently observed latest native version at verification time
   * @param validatedSnapshotVersion actual version from the same successful Kernel snapshot as
   *     schema
   * @param validatedSchemaJson actual schema from that independently validated Kernel snapshot
   * @param commitJson caller-owned exact candidate stream, consumed but never closed
   * @return immutable MATCHED evidence or AMBIGUOUS with a stable diagnostic code
   * @throws IllegalArgumentException for null expected marker, snapshot schema or stream
   */
  public DeltaCommitEvidence inspect(
      DeltaOperationMarker expected,
      long candidateVersion,
      long latestVersion,
      long validatedSnapshotVersion,
      String validatedSchemaJson,
      InputStream commitJson) {
    if (expected == null || validatedSchemaJson == null || commitJson == null) {
      throw new IllegalArgumentException("NULL_INSPECTION_ARGUMENT");
    }
    // Do not close this wrapper: the stream belongs to the caller, including on failure.
    InputStream input = new BufferedInputStream(commitJson);
    ObjectNode metadata = DeltaJson.newObject();
    ObjectNode commitInfo = DeltaJson.newObject();
    Set<String> singletons = new HashSet<>();
    try {
      while (true) {
        byte[] action = nextAction(input);
        if (action.length == 0) {
          break;
        }
        String name = validateAction(action);
        if ("metaData".equals(name) || "commitInfo".equals(name) || "protocol".equals(name)) {
          if (!singletons.add(name)) {
            throw new IllegalArgumentException("DUPLICATE_ACTION");
          }
          if (!"protocol".equals(name)) {
            ObjectNode envelope =
                DeltaJson.object(
                    new String(action, StandardCharsets.UTF_8), DeltaJson.MAX_ACTION_BYTES);
            if ("metaData".equals(name)) {
              metadata = (ObjectNode) envelope.get(name);
            } else {
              commitInfo = (ObjectNode) envelope.get(name);
            }
          }
        }
      }
    } catch (JsonProcessingException e) {
      return drain(input, "INVALID_ACTIONS");
    } catch (IOException e) {
      return drain(input, "IO_ERROR");
    } catch (IllegalArgumentException e) {
      return drain(input, "INVALID_ACTIONS");
    }
    if (!singletons.contains("metaData") || !singletons.contains("commitInfo")) {
      return DeltaCommitEvidence.ambiguous("MISSING_ACTION");
    }
    if (candidateVersion < 0
        || candidateVersion != latestVersion
        || candidateVersion != validatedSnapshotVersion) {
      return DeltaCommitEvidence.ambiguous("VERSION_MISMATCH");
    }
    boolean replace = expected.operation() == DeltaOperationMarker.Operation.REPLACE;
    long requiredVersion = replace ? expected.baseVersion().getAsLong() + 1 : 0;
    if (candidateVersion != requiredVersion) {
      return DeltaCommitEvidence.ambiguous("OPERATION_VERSION_MISMATCH");
    }
    try {
      String operation = DeltaJson.text(commitInfo, "operation");
      if (!(replace ? REPLACE_OPERATIONS : CREATE_OPERATIONS).contains(operation)) {
        return DeltaCommitEvidence.ambiguous("OPERATION_MISMATCH");
      }
      if (replace) {
        if (DeltaJson.number(commitInfo, "readVersion") != expected.baseVersion().getAsLong()) {
          return DeltaCommitEvidence.ambiguous("READ_VERSION_MISMATCH");
        }
      } else if (commitInfo.has("readVersion")
          && DeltaJson.number(commitInfo, "readVersion") != -1) {
        return DeltaCommitEvidence.ambiguous("READ_VERSION_MISMATCH");
      }
      String uuid = DeltaJson.text(metadata, "id");
      DeltaJson.requiredText(uuid);
      if (replace && uuid.equals(expected.baseUuid().get())) {
        return DeltaCommitEvidence.ambiguous("UNCHANGED_UUID");
      }
      JsonNode configuration = metadata.get("configuration");
      if (configuration == null || !configuration.isObject()) {
        return DeltaCommitEvidence.ambiguous("MISSING_MARKER");
      }
      String marker = DeltaJson.text(configuration, DeltaOperationMarker.PROPERTY_KEY);
      if (!DeltaOperationMarker.decode(marker).encode().equals(expected.encode())) {
        return DeltaCommitEvidence.ambiguous("MARKER_MISMATCH");
      }
      String schema = DeltaJson.schema(DeltaJson.text(metadata, "schemaString"));
      if (!schema.equals(expected.schemaJson())
          || !schema.equals(DeltaJson.schema(validatedSchemaJson))) {
        return DeltaCommitEvidence.ambiguous("SCHEMA_MISMATCH");
      }
      return DeltaCommitEvidence.matched(candidateVersion, uuid, schema);
    } catch (IllegalArgumentException e) {
      return DeltaCommitEvidence.ambiguous("INVALID_EVIDENCE");
    }
  }

  // Frame one bounded object before parsing so even enormous JSON field names cannot allocate
  // unbounded parser buffers. Braces inside strings do not affect framing; Jackson validates
  // syntax.
  private static byte[] nextAction(InputStream input) throws IOException {
    int current;
    do {
      current = input.read();
    } while (current == ' ' || current == '\n' || current == '\r' || current == '\t');
    if (current == -1) {
      return new byte[0];
    }
    if (current != '{') {
      throw new IllegalArgumentException("INVALID_ACTION_ENVELOPE");
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    bytes.write(current);
    int depth = 1;
    boolean quoted = false;
    boolean escaped = false;
    while (depth != 0) {
      current = input.read();
      if (current == -1) {
        throw new IllegalArgumentException("TRUNCATED_ACTION");
      }
      if (bytes.size() == DeltaJson.MAX_ACTION_BYTES) {
        throw new IllegalArgumentException("ACTION_LIMIT");
      }
      bytes.write(current);
      if (quoted) {
        if (escaped) {
          escaped = false;
        } else if (current == '\\') {
          escaped = true;
        } else if (current == '"') {
          quoted = false;
        }
      } else if (current == '"') {
        quoted = true;
      } else if (current == '{' || current == '[') {
        if (++depth > 64) {
          throw new IllegalArgumentException("ACTION_DEPTH_LIMIT");
        }
      } else if (current == '}' || current == ']') {
        depth--;
      }
    }
    return bytes.toByteArray();
  }

  private static String validateAction(byte[] action) throws IOException {
    try (JsonParser parser = DeltaJson.FACTORY.createParser(action)) {
      if (parser.nextToken() != JsonToken.START_OBJECT
          || parser.nextToken() != JsonToken.FIELD_NAME) {
        throw new IllegalArgumentException("INVALID_ACTION_ENVELOPE");
      }
      String name = parser.currentName();
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("INVALID_ACTION_VALUE");
      }
      int depth = 1;
      int tokens = 3;
      while (depth != 0) {
        JsonToken token = parser.nextToken();
        if (token == null || ++tokens > DeltaJson.MAX_ACTION_TOKENS) {
          throw new IllegalArgumentException("ACTION_TOKEN_LIMIT");
        }
        if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
          depth++;
        } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
          depth--;
        } else if (token == JsonToken.VALUE_STRING) {
          // Force completion of lazy string parsing and its configured length check.
          parser.getText();
        }
      }
      if (parser.nextToken() != JsonToken.END_OBJECT || parser.nextToken() != null) {
        throw new IllegalArgumentException("INVALID_ACTION_ENVELOPE");
      }
      return name;
    }
  }

  private static DeltaCommitEvidence drain(InputStream input, String reason) {
    byte[] bytes = new byte[8192];
    try {
      while (input.read(bytes) != -1) {
        // Consume caller-owned input even when preceding evidence was malformed.
      }
    } catch (IOException e) {
      return DeltaCommitEvidence.ambiguous("IO_ERROR");
    }
    return DeltaCommitEvidence.ambiguous(reason);
  }
}
