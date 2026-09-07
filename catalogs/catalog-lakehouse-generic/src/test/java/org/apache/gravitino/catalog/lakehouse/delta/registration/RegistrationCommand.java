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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Immutable, strictly bounded version-one command transported through existing table properties.
 */
final class RegistrationCommand {
  static final String KEY = "delta.registration.command";
  static final String INTERNAL = "delta.registration.internal.";
  private static final int MAX_BYTES = 16384;

  enum Operation {
    RESERVE,
    PUBLISH,
    BEGIN_REPLACE,
    ABORT
  }

  final Operation operation;
  final long expectedId;
  final long generation;
  final String token;
  final long candidateVersion;
  @Nullable final String location;
  @Nullable final String schemaFingerprint;
  @Nullable final String replacementToken;

  private RegistrationCommand(
      Operation operation,
      long expectedId,
      long generation,
      String token,
      long candidateVersion,
      @Nullable String location,
      @Nullable String schemaFingerprint,
      @Nullable String replacementToken) {
    this.operation = operation;
    this.expectedId = expectedId;
    this.generation = generation;
    this.token = required(token);
    this.candidateVersion = candidateVersion;
    this.location = location;
    this.schemaFingerprint = schemaFingerprint;
    this.replacementToken = replacementToken;
    if (expectedId < 0 || generation < 0 || candidateVersion < -1) {
      throw new IllegalArgumentException(
          "Invalid expected identity, generation or candidate version");
    }
  }

  static RegistrationCommand reserve(String token, String location, String schemaFingerprint) {
    return new RegistrationCommand(
        Operation.RESERVE,
        0,
        0,
        token,
        -1,
        normalizeLocation(location),
        required(schemaFingerprint),
        null);
  }

  static RegistrationCommand publish(
      long expectedId, long generation, String token, long candidateVersion) {
    return new RegistrationCommand(
        Operation.PUBLISH, expectedId, generation, token, candidateVersion, null, null, null);
  }

  static RegistrationCommand beginReplace(
      long expectedId,
      long generation,
      String token,
      String replacementToken,
      String schemaFingerprint) {
    if (token.equals(replacementToken)) {
      throw new IllegalArgumentException("Replacement requires a new token");
    }
    return new RegistrationCommand(
        Operation.BEGIN_REPLACE,
        expectedId,
        generation,
        token,
        -1,
        null,
        required(schemaFingerprint),
        required(replacementToken));
  }

  static RegistrationCommand abort(long expectedId, long generation, String token) {
    return new RegistrationCommand(
        Operation.ABORT, expectedId, generation, token, -1, null, null, null);
  }

  String encode() {
    String envelope;
    switch (operation) {
      case RESERVE:
        envelope =
            "1|RESERVE|"
                + encodeText(token)
                + "|"
                + encodeText(location)
                + "|"
                + encodeText(schemaFingerprint);
        break;
      case PUBLISH:
        envelope = expectedEnvelope() + "|" + candidateVersion;
        break;
      case BEGIN_REPLACE:
        envelope =
            expectedEnvelope()
                + "|"
                + encodeText(replacementToken)
                + "|"
                + encodeText(schemaFingerprint);
        break;
      case ABORT:
        envelope = expectedEnvelope();
        break;
      default:
        throw new IllegalArgumentException("Unknown operation");
    }
    bounded(envelope);
    return envelope;
  }

  static RegistrationCommand decode(String envelope) {
    bounded(envelope);
    String[] fields = envelope.split("\\|", -1);
    if (fields.length < 2 || !"1".equals(fields[0])) {
      throw new IllegalArgumentException("Unsupported envelope version");
    }
    Operation operation = Operation.valueOf(fields[1]);
    int count =
        operation == Operation.RESERVE || operation == Operation.ABORT
            ? 5
            : operation == Operation.PUBLISH ? 6 : 7;
    if (fields.length != count) {
      throw new IllegalArgumentException("Missing or extraneous command fields");
    }
    if (operation == Operation.RESERVE) {
      return reserve(decodeText(fields[2]), decodeText(fields[3]), decodeText(fields[4]));
    }
    long id = Long.parseLong(fields[2]);
    long generation = Long.parseLong(fields[3]);
    String token = decodeText(fields[4]);
    switch (operation) {
      case PUBLISH:
        return publish(id, generation, token, Long.parseLong(fields[5]));
      case BEGIN_REPLACE:
        return beginReplace(id, generation, token, decodeText(fields[5]), decodeText(fields[6]));
      case ABORT:
        return abort(id, generation, token);
      default:
        throw new IllegalArgumentException("Invalid operation");
    }
  }

  // File URIs normalize filesystem dot segments. Remote URIs retain their exact object key;
  // ambiguous dot segments, escaped keys, userinfo, query, fragment and missing authority fail
  // closed.
  static String normalizeLocation(String input) {
    URI uri = URI.create(required(input));
    if (uri.getScheme() == null) {
      return normalizeFile(Paths.get(input));
    }
    if (uri.getQuery() != null || uri.getFragment() != null || uri.getUserInfo() != null) {
      throw new IllegalArgumentException("Ambiguous location");
    }
    if ("file".equals(uri.getScheme())) {
      return normalizeFile(Paths.get(uri));
    }
    if (uri.isOpaque()
        || uri.getHost() == null
        || uri.getPort() != -1
        || uri.getPath() == null
        || uri.getPath().isEmpty()
        || uri.getRawPath().contains("%")
        || !uri.normalize().equals(uri)) {
      throw new IllegalArgumentException("Unsupported or ambiguous remote location");
    }
    return uri.toString();
  }

  private static String normalizeFile(Path path) {
    // Path.toUri() consults directory existence and adds a trailing slash after native creation.
    // Build the URI lexically so retries are stable and validation inside callbacks performs no IO.
    try {
      return new URI("file", "", path.toAbsolutePath().normalize().toString(), null, null)
          .toASCIIString();
    } catch (URISyntaxException invalid) {
      throw new IllegalArgumentException("Invalid local location", invalid);
    }
  }

  private String expectedEnvelope() {
    return "1|" + operation + "|" + expectedId + "|" + generation + "|" + encodeText(token);
  }

  private static String required(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Required command value is empty");
    }
    return value;
  }

  private static String encodeText(@Nullable String text) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(Objects.requireNonNull(text).getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeText(String text) {
    String decoded = new String(Base64.getUrlDecoder().decode(text), StandardCharsets.UTF_8);
    if (!encodeText(decoded).equals(text)) {
      throw new IllegalArgumentException("Noncanonical text encoding");
    }
    return required(decoded);
  }

  private static void bounded(String text) {
    if (text == null || text.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
      throw new IllegalArgumentException("Command exceeds 16384 UTF-8 bytes");
    }
  }
}
