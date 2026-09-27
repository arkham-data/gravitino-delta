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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ranger.plugin.util.RangerRESTUtils;

/** Test-only callback recorder; never installed in a provider or Spark plugin. */
final class DeltaRangerCallbackGateway implements AutoCloseable {
  private static final Set<String> HOP_HEADERS =
      Set.of(
          "connection",
          "content-length",
          "expect",
          "host",
          "http2-settings",
          "keep-alive",
          "proxy-authenticate",
          "proxy-authorization",
          "te",
          "trailer",
          "transfer-encoding",
          "upgrade");
  private final HttpServer server;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final HttpClient client =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(15))
          .followRedirects(HttpClient.Redirect.NEVER)
          .build();
  private final URI upstream;
  private final Path evidence;
  private final boolean retainResponseBodies;
  private final String downloadService;
  private final String run = UUID.randomUUID().toString();
  private final AtomicInteger active = new AtomicInteger();
  private final AtomicLong attempts = new AtomicLong();
  private final AtomicLong completed = new AtomicLong();
  private final AtomicReference<Fault> fault = new AtomicReference<>();
  private final AtomicReference<IOException> evidenceFailure = new AtomicReference<>();

  DeltaRangerCallbackGateway(URI upstream, Path evidence) throws IOException {
    this(upstream, evidence, 0);
  }

  DeltaRangerCallbackGateway(URI upstream, Path evidence, String downloadService)
      throws IOException {
    this(upstream, evidence, 0, false, downloadService);
  }

  DeltaRangerCallbackGateway(URI upstream, Path evidence, int port, boolean retainResponseBodies)
      throws IOException {
    this(upstream, evidence, port, retainResponseBodies, null);
  }

  private DeltaRangerCallbackGateway(
      URI upstream, Path evidence, int port, boolean retainResponseBodies, String downloadService)
      throws IOException {
    this.upstream = upstream;
    this.evidence = evidence;
    this.retainResponseBodies = retainResponseBodies;
    this.downloadService = downloadService;
    if (!Files.exists(evidence)) {
      Files.createFile(
          evidence,
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    }
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
    server.createContext("/", this::forward);
    server.setExecutor(executor);
    server.start();
  }

  DeltaRangerCallbackGateway(URI upstream, Path evidence, int port) throws IOException {
    this(upstream, evidence, port, false);
  }

  URI uri() {
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  void failNext(String method, String path, int status) {
    if (!path.startsWith("/") || path.contains("?") || status < 400 || status > 599) {
      throw new IllegalArgumentException(
          "An exact owned method/path and error status are required");
    }
    if (!fault.compareAndSet(null, new Fault(method, path, status))) {
      throw new IllegalStateException("An unconsumed callback fault is already armed");
    }
  }

  boolean awaitDrained() throws InterruptedException {
    long end = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (active.get() != 0 && System.nanoTime() < end) {
      Thread.sleep(10);
    }
    return active.get() == 0;
  }

  long completed() {
    return completed.get();
  }

  synchronized List<String> recordedEvents() throws IOException {
    if (evidenceFailure.get() != null) {
      throw new IOException(
          "Callback evidence was lost; acceptance is invalid", evidenceFailure.get());
    }
    return Files.readAllLines(evidence);
  }

  /**
   * Always releases the listener, reporting unresolved handlers or lost evidence as failures. Local
   * drain does not certify Gravitino completion.
   */
  @Override
  public void close() throws InterruptedException, IOException {
    try {
      if (!awaitDrained()) {
        throw new IllegalStateException("Callback completion is unresolved");
      }
      IOException failure = evidenceFailure.get();
      if (failure != null) {
        throw new IOException("Callback evidence was lost; acceptance is invalid", failure);
      }
    } finally {
      server.stop(0);
      executor.shutdown();
    }
  }

  private void forward(HttpExchange exchange) throws IOException {
    long attempt = attempts.incrementAndGet();
    active.incrementAndGet();
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("run", run);
    event.put("attempt", attempt);
    event.put("method", exchange.getRequestMethod());
    event.put("path", exchange.getRequestURI().getPath());
    if (retainResponseBodies) {
      event.put("requestTarget", exchange.getRequestURI().toString());
    }
    event.put("started", Instant.now().toString());
    event.put("activeAtStart", active.get());
    try {
      record(event);
      byte[] request = exchange.getRequestBody().readAllBytes();
      Fault candidate = fault.get();
      if (candidate != null
          && candidate.method.equals(exchange.getRequestMethod())
          && candidate.path.equals(exchange.getRequestURI().getPath())
          && fault.compareAndSet(candidate, null)) {
        event.put("faultStatus", candidate.status);
        event.put("upstreamAttempted", false);
        exchange.sendResponseHeaders(candidate.status, -1);
      } else {
        HttpRequest.Builder builder =
            HttpRequest.newBuilder(upstream.resolve(exchange.getRequestURI().toString()))
                .timeout(Duration.ofSeconds(90))
                .method(
                    exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(request));
        exchange
            .getRequestHeaders()
            .forEach(
                (key, values) -> {
                  if (!HOP_HEADERS.contains(key.toLowerCase(Locale.ROOT))) {
                    for (String value : values) {
                      builder.header(key, value);
                    }
                  }
                });
        event.put("upstreamAttempted", true);
        HttpResponse<byte[]> response =
            client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        event.put("upstreamStatus", response.statusCode());
        event.put("upstreamResponseCompleted", Instant.now().toString());
        recordDownloadMetadata(exchange, response, event);
        if (retainResponseBodies) {
          Path body =
              evidence.resolveSibling(evidence.getFileName() + "." + run + "." + attempt + ".body");
          try {
            Files.createFile(
                body,
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(body, response.body(), StandardOpenOption.WRITE);
            event.put("responseBodyFile", body.toAbsolutePath().toString());
          } catch (IOException failure) {
            evidenceFailure.compareAndSet(null, failure);
            throw failure;
          }
        }
        response
            .headers()
            .map()
            .forEach(
                (key, values) -> {
                  if (!HOP_HEADERS.contains(key.toLowerCase(Locale.ROOT))) {
                    exchange.getResponseHeaders().put(key, values);
                  }
                });
        boolean noBody =
            response.statusCode() == 204
                || response.statusCode() == 304
                || response.body().length == 0;
        exchange.sendResponseHeaders(response.statusCode(), noBody ? -1 : response.body().length);
        if (!noBody) {
          exchange.getResponseBody().write(response.body());
        }
      }
      event.put("downstreamResponseCompleted", true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      event.put("upstreamCompletionUnresolved", true);
      throw new IOException(
          "Callback gateway interrupted; independent completion evidence required");
    } catch (IOException e) {
      event.put("transportFailure", e.getClass().getName());
      event.put("upstreamCompletionUnresolved", !event.containsKey("upstreamResponseCompleted"));
      throw e;
    } finally {
      try {
        exchange.close();
        event.put("finished", Instant.now().toString());
        event.put("activeAfter", active.get() - 1);
        record(event);
      } finally {
        completed.incrementAndGet();
        active.decrementAndGet();
      }
    }
  }

  private synchronized void record(Map<String, Object> event) throws IOException {
    try {
      Files.writeString(
          evidence, new ObjectMapper().writeValueAsString(event) + "\n", StandardOpenOption.APPEND);
    } catch (IOException e) {
      evidenceFailure.compareAndSet(null, e);
      throw e;
    }
  }

  private void recordDownloadMetadata(
      HttpExchange exchange, HttpResponse<byte[]> response, Map<String, Object> event) {
    if (downloadService == null
        || !"GET".equals(exchange.getRequestMethod())
        || response.statusCode() != 200) {
      return;
    }
    String path = exchange.getRequestURI().getPath();
    String kind;
    String versionField;
    if (path.equals(
        RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + downloadService)) {
      kind = "policy";
      versionField = "policyVersion";
    } else if (path.equals(
        RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + downloadService)) {
      kind = "role";
      versionField = "roleVersion";
    } else {
      return;
    }
    event.put("downloadKind", kind);
    try {
      ObjectMapper strict =
          new ObjectMapper()
              .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
              .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
      JsonNode body = strict.readTree(response.body());
      if (body == null
          || !body.isObject()
          || !body.path("serviceName").isTextual()
          || !downloadService.equals(body.path("serviceName").textValue())
          || !body.path(versionField).isIntegralNumber()
          || !body.path(versionField).canConvertToLong()
          || body.path(versionField).longValue() < 0) {
        event.put("downloadMetadataInvalid", "service-or-version");
        return;
      }
      event.put("downloadService", downloadService);
      event.put("downloadVersion", body.path(versionField).longValue());
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(response.body());
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b));
      }
      event.put("responseSha256", hex.toString());
    } catch (IOException | NoSuchAlgorithmException invalid) {
      event.put("downloadMetadataInvalid", "malformed-response");
    }
  }

  private static final class Fault {
    private final String method;
    private final String path;
    private final int status;

    private Fault(String method, String path, int status) {
      this.method = method;
      this.path = path;
      this.status = status;
    }
  }
}
