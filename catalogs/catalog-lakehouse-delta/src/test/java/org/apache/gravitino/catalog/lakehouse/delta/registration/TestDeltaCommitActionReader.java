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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaCommitActionReader {
  private static final String SCHEMA =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"x\":1}}]}";
  private static final ObjectMapper JSON = new ObjectMapper();
  private final DeltaCommitActionReader reader = new DeltaCommitActionReader();

  @ParameterizedTest
  @ValueSource(
      strings = {"CREATE TABLE", "CREATE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"})
  void acceptsExactNativeCreateOperations(String operation) throws Exception {
    DeltaOperationMarker marker = create();
    TrackingStream stream = stream(metadata(marker, "new-uuid", SCHEMA) + info(operation, ""));
    DeltaCommitEvidence evidence = reader.inspect(marker, 0, 0, 0, SCHEMA, stream);
    Assertions.assertEquals(DeltaCommitEvidence.Kind.MATCHED, evidence.kind());
    Assertions.assertEquals(0, evidence.version().getAsLong());
    Assertions.assertEquals("new-uuid", evidence.uuid().get());
    Assertions.assertEquals(JSON.readTree(SCHEMA), JSON.readTree(evidence.schemaJson().get()));
    Assertions.assertFalse(stream.closed);
    Assertions.assertEquals(-1, stream.read());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"REPLACE TABLE", "REPLACE TABLE AS SELECT", "CREATE OR REPLACE TABLE AS SELECT"})
  void acceptsExactNativeReplaceOperations(String operation) throws Exception {
    DeltaOperationMarker marker = replace();
    DeltaCommitEvidence evidence =
        inspect(
            marker,
            5,
            5,
            5,
            SCHEMA,
            metadata(marker, "new-uuid", SCHEMA) + info(operation, ",\"readVersion\":4"));
    Assertions.assertEquals(DeltaCommitEvidence.Kind.MATCHED, evidence.kind());
    Assertions.assertEquals(5, evidence.version().getAsLong());
    Assertions.assertEquals("new-uuid", evidence.uuid().get());
  }

  @Test
  void independentSnapshotAndLatestAndOperationVersionsMustAgree() throws Exception {
    String commit = metadata(create(), "u", SCHEMA) + info("CREATE TABLE", "");
    for (long[] versions :
        new long[][] {{0, 0, 1}, {0, 1, 0}, {1, 1, 1}, {-1, 0, 0}, {0, -1, 0}, {0, 0, -1}}) {
      ambiguous(inspect(create(), versions[0], versions[1], versions[2], SCHEMA, commit));
    }
    String replacement =
        metadata(replace(), "u", SCHEMA) + info("REPLACE TABLE", ",\"readVersion\":4");
    ambiguous(inspect(replace(), 6, 6, 6, SCHEMA, replacement));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "WRITE",
        "SET TBLPROPERTIES",
        "ALTER TABLE",
        "CREATE OR REPLACE TABLE",
        "create table",
        "CREATE TABLE BAD",
        "REPLACE TABLE"
      })
  void rejectsInheritedOrUnsupportedCreateOperations(String operation) throws Exception {
    ambiguous(
        inspect(create(), 0, 0, 0, SCHEMA, metadata(create(), "u", SCHEMA) + info(operation, "")));
  }

  @Test
  void enforcesReadVersionAndReplacementUuid() throws Exception {
    for (String readVersion :
        new String[] {
          "",
          ",\"readVersion\":3",
          ",\"readVersion\":5",
          ",\"readVersion\":4.0",
          ",\"readVersion\":\"4\"",
          ",\"readVersion\":null",
          ",\"readVersion\":9223372036854775808"
        }) {
      ambiguous(
          inspect(
              replace(),
              5,
              5,
              5,
              SCHEMA,
              metadata(replace(), "new", SCHEMA) + info("REPLACE TABLE", readVersion)));
    }
    for (String readVersion :
        new String[] {
          ",\"readVersion\":0",
          ",\"readVersion\":2",
          ",\"readVersion\":-1.0",
          ",\"readVersion\":\"-1\"",
          ",\"readVersion\":null",
          ",\"readVersion\":-2"
        }) {
      ambiguous(
          inspect(
              create(),
              0,
              0,
              0,
              SCHEMA,
              metadata(create(), "u", SCHEMA) + info("CREATE TABLE", readVersion)));
    }
    Assertions.assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(
                create(),
                0,
                0,
                0,
                SCHEMA,
                metadata(create(), "u", SCHEMA) + info("CREATE TABLE", ",\"readVersion\":-1"))
            .kind());
    ambiguous(
        inspect(
            replace(),
            5,
            5,
            5,
            SCHEMA,
            metadata(replace(), "old-uuid", SCHEMA) + info("REPLACE TABLE", ",\"readVersion\":4")));
    ambiguous(
        inspect(
            replace(),
            5,
            5,
            5,
            SCHEMA,
            metadata(replace(), "new", SCHEMA) + info("CREATE TABLE", ",\"readVersion\":4")));
  }

  @Test
  void schemaComparisonIgnoresObjectOrderButPreservesContent() throws Exception {
    String reordered =
        "{\"fields\":[{\"metadata\":{\"x\":1},\"nullable\":true,\"type\":\"long\",\"name\":\"a\"}],\"type\":\"struct\"}";
    Assertions.assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(
                create(),
                0,
                0,
                0,
                reordered,
                metadata(create(), "u", reordered) + info("CREATE TABLE", ""))
            .kind());
    for (String schema :
        new String[] {
          SCHEMA.replace("true", "false"),
          SCHEMA.replace("\"x\":1", "\"x\":2"),
          SCHEMA.replace("long", "string"),
          SCHEMA.replace("\"x\":1", "\"x\":[1,2]"),
          "null",
          "{}{}"
        }) {
      ambiguous(
          inspect(
              create(),
              0,
              0,
              0,
              schema,
              metadata(create(), "u", SCHEMA) + info("CREATE TABLE", "")));
      ambiguous(
          inspect(
              create(),
              0,
              0,
              0,
              SCHEMA,
              metadata(create(), "u", schema) + info("CREATE TABLE", "")));
    }
    String arraySchema = SCHEMA.replace("\"x\":1", "\"x\":[1,2]");
    DeltaOperationMarker arrayMarker =
        DeltaOperationMarker.create(1, 2, "secret-token", "s3://bucket/table", arraySchema);
    ambiguous(
        inspect(
            arrayMarker,
            0,
            0,
            0,
            arraySchema,
            metadata(arrayMarker, "u", arraySchema.replace("[1,2]", "[2,1]"))
                + info("CREATE TABLE", "")));
  }

  @Test
  void requiresExactMarkerAndNonemptyMetadataUuid() throws Exception {
    DeltaOperationMarker[] others = {
      DeltaOperationMarker.create(2, 2, "secret-token", "s3://bucket/table", SCHEMA),
      DeltaOperationMarker.create(1, 3, "secret-token", "s3://bucket/table", SCHEMA),
      DeltaOperationMarker.create(1, 2, "another", "s3://bucket/table", SCHEMA),
      DeltaOperationMarker.create(1, 2, "secret-token", "s3://other/table", SCHEMA),
      replace()
    };
    for (DeltaOperationMarker marker : others) {
      ambiguous(
          inspect(
              create(), 0, 0, 0, SCHEMA, metadata(marker, "u", SCHEMA) + info("CREATE TABLE", "")));
    }
    DeltaOperationMarker wrongBase =
        DeltaOperationMarker.replace(
            1, 2, "secret-token", "s3://bucket/table", SCHEMA, 4, "different-base");
    ambiguous(
        inspect(
            replace(),
            5,
            5,
            5,
            SCHEMA,
            metadata(wrongBase, "u", SCHEMA) + info("REPLACE TABLE", ",\"readVersion\":4")));
    for (String uuid : new String[] {"", " "}) {
      ambiguous(
          inspect(
              create(),
              0,
              0,
              0,
              SCHEMA,
              metadata(create(), uuid, SCHEMA) + info("CREATE TABLE", "")));
    }
    String valid = metadata(create(), "u", SCHEMA) + info("CREATE TABLE", "");
    ambiguous(
        inspect(
            create(),
            0,
            0,
            0,
            SCHEMA,
            valid.replace("gravitino.registration.operation", "unowned")));
    ambiguous(
        inspect(
            create(), 0, 0, 0, SCHEMA, valid.replace("\\\"version\\\":1", "\\\"version\\\":2")));
  }

  @Test
  void rejectsMissingDuplicateWrongTypeAndMalformedTrailingActions() throws Exception {
    String metadata = metadata(create(), "u", SCHEMA);
    String info = info("CREATE TABLE", "");
    String[] bad = {
      "",
      metadata,
      info,
      metadata + metadata + info,
      metadata + info + info,
      metadata + info + "{",
      metadata + info + "null",
      metadata + info + "[]",
      metadata + info + "{}",
      metadata + info + "{\"add\":{},\"remove\":{}}",
      metadata + info + "{\"add\":{\"path\":\"a\",\"path\":\"b\"}}",
      metadata + info + "{\"protocol\":{}}{\"protocol\":{}}",
      metadata + info + "{\"add\":null}",
      metadata.replace("\"id\":\"u\"", "\"id\":\"u\",\"id\":\"v\"") + info,
      metadata + info("CREATE TABLE", ",\"operation\":\"WRITE\""),
      "{\"metaData\":null}" + info,
      metadata + "{\"commitInfo\":{\"operation\":3}}"
    };
    for (String commit : bad) {
      TrackingStream stream = stream(commit);
      ambiguous(reader.inspect(create(), 0, 0, 0, SCHEMA, stream));
      Assertions.assertFalse(stream.closed);
      Assertions.assertEquals(-1, stream.read());
    }
  }

  @Test
  void streamsRepeatedFileActionsAndConsumesAllBytes() throws Exception {
    StringBuilder commit = new StringBuilder(metadata(create(), "u", SCHEMA));
    for (int i = 0; i < 20000; i++) {
      commit.append("{\"add\":{\"path\":\"a\",\"stats\":\"{}\"}}\n{\"remove\":{\"path\":\"b\"}}\n");
    }
    commit.append(info("CREATE TABLE", ""));
    Assertions.assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(create(), 0, 0, 0, SCHEMA, commit.toString()).kind());
  }

  @Test
  void failsClosedOnPerActionResourceLimits() throws Exception {
    StringBuilder oversized =
        new StringBuilder(metadata(create(), "u", SCHEMA) + info("CREATE TABLE", ""));
    oversized.append("{\"add\":{\"huge\":[");
    for (int i = 0; i < 70000; i++) {
      oversized.append("0,");
    }
    oversized.append("0]}}");
    ambiguous(inspect(create(), 0, 0, 0, SCHEMA, oversized.toString()));
  }

  @Test
  void ioFailureNeverPublishesPartialMatchOrClosesStream() throws Exception {
    byte[] prefix =
        (metadata(create(), "u", SCHEMA) + info("CREATE TABLE", ""))
            .getBytes(StandardCharsets.UTF_8);
    boolean[] closed = {false};
    InputStream failing =
        new InputStream() {
          private int position;

          @Override
          public int read() throws IOException {
            if (position == prefix.length) {
              throw new IOException("secret-token must not escape");
            }
            return prefix[position++] & 0xff;
          }

          @Override
          public int read(byte[] bytes, int offset, int length) throws IOException {
            if (length == 0) {
              return 0;
            }
            if (position == prefix.length) {
              throw new IOException("secret-token must not escape");
            }
            int count = Math.min(length, prefix.length - position);
            System.arraycopy(prefix, position, bytes, offset, count);
            position += count;
            return count;
          }

          @Override
          public void close() {
            closed[0] = true;
          }
        };
    DeltaCommitEvidence evidence = reader.inspect(create(), 0, 0, 0, SCHEMA, failing);
    ambiguous(evidence);
    Assertions.assertEquals("IO_ERROR", evidence.reason());
    Assertions.assertFalse(closed[0]);
  }

  @Test
  void preservesDistinctHighPrecisionSchemaMetadataNumbers() throws Exception {
    String schema = SCHEMA.replace("\"x\":1", "\"x\":0.123456789012345678901");
    String different = SCHEMA.replace("\"x\":1", "\"x\":0.123456789012345678902");
    DeltaOperationMarker marker =
        DeltaOperationMarker.create(1, 2, "secret-token", "s3://bucket/table", schema);
    ambiguous(
        inspect(
            marker, 0, 0, 0, schema, metadata(marker, "u", different) + info("CREATE TABLE", "")));
  }

  @Test
  void malformedJsonHasDistinctDiagnosticFromTransportFailure() throws Exception {
    DeltaCommitEvidence evidence =
        inspect(
            create(),
            0,
            0,
            0,
            SCHEMA,
            metadata(create(), "u", SCHEMA) + info("CREATE TABLE", "") + "{\"add\":{\"x\":?}}");
    ambiguous(evidence);
    Assertions.assertEquals("INVALID_ACTIONS", evidence.reason());
  }

  @Test
  void framingHandlesQuotedBracesEscapesAndUnicode() throws Exception {
    String opaque = "{\"add\":{\"path\":\"é}b{\\\"c\",\"stats\":\"{\\\"x\\\":1}\"}}\n";
    Assertions.assertEquals(
        DeltaCommitEvidence.Kind.MATCHED,
        inspect(
                create(),
                0,
                0,
                0,
                SCHEMA,
                opaque + metadata(create(), "uuid-é", SCHEMA) + info("CREATE TABLE", ""))
            .kind());
  }

  @Test
  void boundsRawActionBytesAndDepthBeforePublishing() throws Exception {
    String valid = metadata(create(), "u", SCHEMA) + info("CREATE TABLE", "");
    String oversizedName = String.join("", Collections.nCopies(1100000, "x"));
    String deeplyNested =
        String.join("", Collections.nCopies(70, "["))
            + "0"
            + String.join("", Collections.nCopies(70, "]"));
    for (String action :
        new String[] {
          "{\"add\":{\"" + oversizedName + "\":0}}", "{\"add\":{\"deep\":" + deeplyNested + "}}"
        }) {
      TrackingStream stream = stream(valid + action);
      ambiguous(reader.inspect(create(), 0, 0, 0, SCHEMA, stream));
      Assertions.assertFalse(stream.closed);
      Assertions.assertEquals(-1, stream.read());
    }
  }

  private DeltaCommitEvidence inspect(
      DeltaOperationMarker marker,
      long candidate,
      long latest,
      long snapshot,
      String schema,
      String commit) {
    return reader.inspect(marker, candidate, latest, snapshot, schema, stream(commit));
  }

  private static void ambiguous(DeltaCommitEvidence evidence) {
    Assertions.assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, evidence.kind());
    Assertions.assertFalse(evidence.version().isPresent());
    Assertions.assertFalse(evidence.uuid().isPresent());
    Assertions.assertFalse(evidence.schemaJson().isPresent());
    Assertions.assertTrue(evidence.reason().matches("[A-Z_]{1,64}"));
    Assertions.assertFalse(evidence.reason().contains("secret-token"));
  }

  private static DeltaOperationMarker create() {
    return DeltaOperationMarker.create(1, 2, "secret-token", "s3://bucket/table", SCHEMA);
  }

  private static DeltaOperationMarker replace() {
    return DeltaOperationMarker.replace(
        1, 2, "secret-token", "s3://bucket/table", SCHEMA, 4, "old-uuid");
  }

  private static String metadata(DeltaOperationMarker marker, String uuid, String schema)
      throws JsonProcessingException {
    return "{\"metaData\":{\"id\":"
        + JSON.writeValueAsString(uuid)
        + ",\"schemaString\":"
        + JSON.writeValueAsString(schema)
        + ",\"configuration\":{\"gravitino.registration.operation\":"
        + JSON.writeValueAsString(marker.encode())
        + "}}}\n";
  }

  private static String info(String operation, String extra) throws JsonProcessingException {
    return "{\"commitInfo\":{\"operation\":" + JSON.writeValueAsString(operation) + extra + "}}\n";
  }

  private static TrackingStream stream(String text) {
    return new TrackingStream(text.getBytes(StandardCharsets.UTF_8));
  }

  private static final class TrackingStream extends ByteArrayInputStream {
    private boolean closed;

    private TrackingStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }
}
