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
            URI.create(original.get(URL)), evidence.resolve("downloads.jsonl"));
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

  void recordInitialDownloads(long policy, long role, long minPolicy, long minRole)
      throws Exception {
    if (policy < minPolicy || role < minRole) {
      throw new IllegalStateException(
          "Fresh downloaded generations have not reached the fixture floor");
    }
    long policyAttempt = 0;
    long roleAttempt = 0;
    for (String line : gateway.recordedEvents()) {
      JsonNode event = JSON.readTree(line);
      if (!"GET".equals(event.path("method").asText())
          || !event.has("upstreamResponseCompleted")
          || !event.has("finished")
          || !event.path("upstreamAttempted").asBoolean()
          || event.path("upstreamStatus").asInt() != 200
          || !event.path("downstreamResponseCompleted").asBoolean()) {
        continue;
      }
      if (event
          .path("path")
          .asText()
          .equals(RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + service)) {
        policyAttempt = event.path("attempt").asLong();
      }
      if (event
          .path("path")
          .asText()
          .equals(RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + service)) {
        roleAttempt = event.path("attempt").asLong();
      }
    }
    if (policyAttempt == 0 || roleAttempt == 0) {
      throw new IllegalStateException(
          "Both initial secure downloads require completed actual HTTP 200");
    }
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("policyAttempt", policyAttempt);
    record.put("roleAttempt", roleAttempt);
    record.put("policyVersion", policy);
    record.put("roleVersion", role);
    record.put("freshEmptyCacheAtPreparation", true);
    write(evidence.resolve("initial-downloads.json"), record);
  }

  static Map<String, String> properties(Path xml) throws Exception {
    return properties(document(xml));
  }

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
