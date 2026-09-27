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

package org.apache.gravitino.spark.connector.delta.ranger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/** Private fresh-cache setup and transport evidence for the test-only Spark application. */
final class DeltaRangerDownloadedSecurity implements AutoCloseable {
  enum DownloadRequirement {
    NONE,
    POLICY,
    ROLE,
    BOTH
  }

  static final class CompletedDownload {
    private final long attempt;
    private final long version;
    private final String responseSha256;

    CompletedDownload(long attempt, long version, String responseSha256) {
      this.attempt = attempt;
      this.version = version;
      this.responseSha256 = responseSha256;
    }

    long attempt() {
      return attempt;
    }

    long version() {
      return version;
    }

    String responseSha256() {
      return responseSha256;
    }
  }

  static final class DownloadEvidence {
    private final Optional<CompletedDownload> policy;
    private final Optional<CompletedDownload> role;
    private final long cursor;

    DownloadEvidence(
        Optional<CompletedDownload> policy, Optional<CompletedDownload> role, long cursor) {
      this.policy = policy;
      this.role = role;
      this.cursor = cursor;
    }

    Optional<CompletedDownload> policy() {
      return policy;
    }

    Optional<CompletedDownload> role() {
      return role;
    }

    long cursor() {
      return cursor;
    }
  }

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String URL = "ranger.plugin.spark.policy.rest.url";
  private static final String CACHE = "ranger.plugin.spark.policy.cache.dir";
  private final DeltaRangerCallbackGateway gateway;
  private final Path evidence;
  private final String service;

  DeltaRangerDownloadedSecurity(Path source, Path target, Path evidence, String service)
      throws Exception {
    try (var files = Files.list(target)) {
      if (files.findAny().isPresent()) {
        throw new IllegalArgumentException("Driver security directory must start empty");
      }
    }
    Files.setPosixFilePermissions(target, PosixFilePermissions.fromString("rwx------"));
    this.evidence = evidence;
    this.service = service;
    Document document = document(source.resolve("ranger-spark-security.xml"));
    Map<String, String> original = properties(document);
    if (!original.containsKey(URL) || !original.containsKey(CACHE)) {
      throw new IllegalArgumentException("Both security URL and cache properties are required");
    }
    Path cache =
        Files.createDirectory(
            target.resolve("cache"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    gateway =
        new DeltaRangerCallbackGateway(
            URI.create(original.get(URL)), evidence.resolve("downloads.jsonl"), service);
    try {
      var nodes = document.getElementsByTagName("property");
      for (int i = 0; i < nodes.getLength(); i++) {
        Element property = (Element) nodes.item(i);
        String name = property.getElementsByTagName("name").item(0).getTextContent();
        if (name.equals(URL) || name.equals(CACHE)) {
          property
              .getElementsByTagName("value")
              .item(0)
              .setTextContent(
                  name.equals(URL) ? gateway.uri().toString() : cache.toAbsolutePath().toString());
        }
      }
      Path copied = privateFile(target.resolve("ranger-spark-security.xml"));
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
      var transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      try (var stream = Files.newOutputStream(copied)) {
        transformer.transform(new DOMSource(document), new StreamResult(stream));
      }
      Path logging = privateFile(target.resolve("log4j2.xml"));
      Files.write(logging, Files.readAllBytes(source.resolve("log4j2.xml")));
      Map<String, String> changed = properties(copied);
      for (String key : original.keySet()) {
        if (!key.equals(URL) && !key.equals(CACHE) && !original.get(key).equals(changed.get(key))) {
          throw new IllegalStateException("Unexpected security property change");
        }
      }
      Map<String, Object> manifest = new LinkedHashMap<>();
      manifest.put("originalSecuritySha256", digest(source.resolve("ranger-spark-security.xml")));
      manifest.put("copiedSecuritySha256", digest(copied));
      manifest.put("originalLoggingSha256", digest(source.resolve("log4j2.xml")));
      manifest.put("copiedLoggingSha256", digest(logging));
      manifest.put("changedPropertyNames", new String[] {URL, CACHE});
      manifest.put("freshEmptyCache", true);
      manifest.put("gateway", gateway.uri().toString());
      write(evidence.resolve("security-manifest.json"), manifest);
    } catch (Throwable failure) {
      try {
        gateway.close();
      } catch (Throwable cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  URI uri() {
    return gateway.uri();
  }

  boolean awaitDrained() throws InterruptedException {
    return gateway.awaitDrained();
  }

  long completedCursor() throws IOException, InterruptedException {
    if (!gateway.awaitDrained()) {
      throw new IllegalStateException("Download completion unresolved");
    }
    long cursor = 0;
    for (String line : gateway.recordedEvents()) {
      JsonNode event = JSON.readTree(line);
      if (event.has("finished")) {
        cursor = Math.max(cursor, event.path("attempt").asLong());
      }
    }
    return cursor;
  }

  DownloadEvidence completedAfter(
      long exclusiveCursor,
      DownloadRequirement requirement,
      DeltaRangerPolicyEngine.Versions applied)
      throws IOException, InterruptedException {
    return completedAfter(exclusiveCursor, requirement, applied.policy(), applied.role());
  }

  private DownloadEvidence completedAfter(
      long exclusiveCursor, DownloadRequirement requirement, long policyVersion, long roleVersion)
      throws IOException, InterruptedException {
    if (!gateway.awaitDrained()) {
      throw new IllegalStateException("Download completion unresolved");
    }
    JsonNode latestPolicy = null;
    JsonNode latestRole = null;
    long cursor = exclusiveCursor;
    for (String line : gateway.recordedEvents()) {
      JsonNode event = JSON.readTree(line);
      long attempt = event.path("attempt").asLong();
      if (attempt <= exclusiveCursor || !event.has("finished")) {
        continue;
      }
      cursor = Math.max(cursor, attempt);
      if (!"GET".equals(event.path("method").asText())
          || !event.path("upstreamAttempted").asBoolean()
          || !event.has("upstreamResponseCompleted")
          || !event.path("downstreamResponseCompleted").asBoolean()
          || event.path("upstreamStatus").asInt() != 200) {
        continue;
      }
      String path = event.path("path").asText();
      if (path.equals(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + service)
          && (latestPolicy == null || attempt > latestPolicy.path("attempt").asLong())) {
        latestPolicy = event;
      }
      if (path.equals(RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + service)
          && (latestRole == null || attempt > latestRole.path("attempt").asLong())) {
        latestRole = event;
      }
    }
    Optional<CompletedDownload> policy = Optional.empty();
    Optional<CompletedDownload> role = Optional.empty();
    if (requirement == DownloadRequirement.POLICY || requirement == DownloadRequirement.BOTH) {
      policy = Optional.of(require(latestPolicy, "policy", policyVersion));
    }
    if (requirement == DownloadRequirement.ROLE || requirement == DownloadRequirement.BOTH) {
      role = Optional.of(require(latestRole, "role", roleVersion));
    }
    return new DownloadEvidence(policy, role, cursor);
  }

  void recordInitialDownloads(long policy, long role, long minPolicy, long minRole)
      throws Exception {
    if (policy < minPolicy || role < minRole) {
      throw new IllegalStateException(
          "Fresh downloaded generations have not reached the fixture floor");
    }
    DownloadEvidence downloads = completedAfter(0, DownloadRequirement.BOTH, policy, role);
    long policyAttempt = downloads.policy().orElseThrow().attempt();
    long roleAttempt = downloads.role().orElseThrow().attempt();
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("policyAttempt", policyAttempt);
    record.put("roleAttempt", roleAttempt);
    record.put("policyVersion", policy);
    record.put("roleVersion", role);
    record.put("freshEmptyCacheAtPreparation", true);
    write(evidence.resolve("initial-downloads.json"), record);
  }

  private CompletedDownload require(JsonNode event, String kind, long version) {
    if (event == null
        || !kind.equals(event.path("downloadKind").asText())
        || !service.equals(event.path("downloadService").asText())
        || !event.path("downloadVersion").isIntegralNumber()
        || event.path("downloadVersion").asLong() != version
        || !event.path("responseSha256").isTextual()
        || !event.path("responseSha256").asText().matches("[0-9a-f]{64}")) {
      throw new IllegalStateException("Completed download does not match applied generation");
    }
    return new CompletedDownload(
        event.path("attempt").asLong(), version, event.path("responseSha256").asText());
  }

  static Map<String, String> properties(Path xml) throws Exception {
    return properties(document(xml));
  }

  /** Releases the recorder and rejects unresolved handlers or lost download evidence. */
  @Override
  public void close() throws Exception {
    gateway.close();
  }

  private static Map<String, String> properties(Document document) {
    Map<String, String> values = new LinkedHashMap<>();
    var nodes = document.getElementsByTagName("property");
    for (int i = 0; i < nodes.getLength(); i++) {
      Element property = (Element) nodes.item(i);
      String name = property.getElementsByTagName("name").item(0).getTextContent();
      String value = property.getElementsByTagName("value").item(0).getTextContent();
      if (values.put(name, value) != null) {
        throw new IllegalArgumentException("Duplicate security property");
      }
    }
    return values;
  }

  private static Document document(Path xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return factory.newDocumentBuilder().parse(xml.toFile());
  }

  private static Path privateFile(Path path) throws IOException {
    return Files.createFile(
        path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
  }

  private static void write(Path path, Object value) throws IOException {
    JSON.writeValue(privateFile(path).toFile(), value);
  }

  private static String digest(Path path) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path));
    StringBuilder result = new StringBuilder();
    for (byte value : digest) {
      result.append(String.format("%02x", value));
    }
    return result.toString();
  }
}
