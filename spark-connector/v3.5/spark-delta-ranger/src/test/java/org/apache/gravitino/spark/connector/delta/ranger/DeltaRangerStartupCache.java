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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.ranger.plugin.util.RangerRESTUtils;

/** Strict private cache bundle verifier and bounded copier for the test application. */
final class DeltaRangerStartupCache {
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final Set<String> EVIDENCE =
      Set.of(
          "source-provenance.json",
          "ready.json",
          "initial-downloads.json",
          "downloads.jsonl",
          "completed.json");
  private static final Set<String> MANIFEST_KEYS =
      Set.of(
          "schemaVersion",
          "service",
          "definition",
          "metalake",
          "spark",
          "delta",
          "ranger",
          "scala",
          "java",
          "sourceRunId",
          "sourcePid",
          "policyVersion",
          "roleVersion",
          "files");
  private static final Set<String> FILE_KEYS = Set.of("name", "size", "sha256");
  private static final Set<String> PROVENANCE_KEYS =
      Set.of(
          "schemaVersion",
          "sourceRunId",
          "sourcePid",
          "exitCode",
          "requestSha256",
          "profileSha256",
          "applicationSha256",
          "companionSha256",
          "fixtureSha256",
          "archiveManifestSha256",
          "sourceSecuritySha256",
          "captureStarted",
          "captureCompleted",
          "exitRecordSha256");
  private static final Set<String> READY_KEYS =
      Set.of(
          "spark",
          "user",
          "groups",
          "applicationUser",
          "origins",
          "helpers",
          "visibleCatalogs",
          "versions");
  private static final long MAX_FILE = 16L * 1024 * 1024;
  private static final long MAX_TOTAL = 64L * 1024 * 1024;

  static final class Snapshot {
    private final String manifestSha256;
    private final JsonNode manifest;
    private final JsonNode provenance;
    private final Map<String, byte[]> cacheFiles;

    private Snapshot(
        String manifestSha256,
        JsonNode manifest,
        JsonNode provenance,
        Map<String, byte[]> cacheFiles) {
      this.manifestSha256 = manifestSha256;
      this.manifest = manifest.deepCopy();
      this.provenance = provenance.deepCopy();
      this.cacheFiles = cloneFiles(cacheFiles);
    }

    String manifestSha256() {
      return manifestSha256;
    }

    JsonNode manifest() {
      return manifest.deepCopy();
    }

    JsonNode provenance() {
      return provenance.deepCopy();
    }

    Map<String, byte[]> cacheFiles() {
      return cloneFiles(cacheFiles);
    }
  }

  private DeltaRangerStartupCache() {}

  static Optional<Snapshot> load(
      Path requestFile, JsonNode input, DeltaRangerStartupProbe.Request request) throws Exception {
    if (!requestFile.isAbsolute()) {
      throw new IllegalArgumentException("Request path must be absolute");
    }
    Path run = requestFile.getParent();
    requireCanonical(run);
    long uid = effectiveUid();
    requireEffectiveOwner(run, uid);
    ownerOnlyDirectory(run, Files.getOwner(run, LinkOption.NOFOLLOW_LINKS).getName());
    privateOwnedFile(requestFile, uid);
    Path bundle = run.resolve("startup-cache-input");
    if ("empty".equals(request.mode())) {
      if (Files.exists(bundle, LinkOption.NOFOLLOW_LINKS)) {
        throw new IllegalArgumentException("Empty probe has a snapshot bundle");
      }
      return Optional.empty();
    }
    String owner = Files.getOwner(run, LinkOption.NOFOLLOW_LINKS).getName();
    requireEffectiveOwner(bundle, uid);
    ownerOnlyDirectory(bundle, owner);
    Path cache = bundle.resolve("cache");
    requireEffectiveOwner(cache, uid);
    ownerOnlyDirectory(cache, owner);
    requireEffectiveOwner(bundle.resolve("manifest.json"), uid);
    byte[] manifestBytes = read(bundle.resolve("manifest.json"), 64 * 1024, owner);
    String digest = DeltaRangerStartupProbe.sha256(manifestBytes);
    if (!digest.equals(request.manifestSha256().orElseThrow())) {
      throw new IllegalArgumentException("Snapshot manifest digest mismatch");
    }
    JsonNode manifest = JSON.readTree(manifestBytes);
    keys(manifest, MANIFEST_KEYS);
    requireInt(manifest, "schemaVersion", 1);
    requireTextEquals(manifest, "service", input.path("resourceService").asText());
    requireTextEquals(manifest, "definition", input.path("resourceDefinition").asText());
    requireTextEquals(manifest, "metalake", input.path("metalake").asText());
    requireAllowed(manifest, "spark", Set.of("3.5.4", "3.5.6"));
    requireAllowed(manifest, "delta", Set.of("3.3.0", "3.3.2"));
    requireTextEquals(manifest, "ranger", "2.8.0");
    requireTextEquals(manifest, "scala", "2.12");
    requireInt(manifest, "java", 17);
    if (!manifest.path("sourceRunId").isTextual()
        || manifest.path("sourceRunId").asText().isEmpty()
        || !manifest.path("sourcePid").isIntegralNumber()
        || manifest.path("sourcePid").asLong() <= 0
        || manifest.path("sourcePid").asLong() == ProcessHandle.current().pid()
        || !manifest.path("policyVersion").isIntegralNumber()
        || manifest.path("policyVersion").asLong() < 0
        || !manifest.path("roleVersion").isIntegralNumber()
        || manifest.path("roleVersion").asLong() < 0) {
      throw new IllegalArgumentException("Invalid source identity or versions");
    }
    JsonNode entries = manifest.path("files");
    if (!entries.isArray() || entries.size() < 6 || entries.size() > 21) {
      throw new IllegalArgumentException("Invalid snapshot file count");
    }
    Map<String, byte[]> files = new TreeMap<>();
    String previous = "";
    long totalCache = 0;
    for (JsonNode entry : entries) {
      keys(entry, FILE_KEYS);
      String name = text(entry, "name");
      if (name.compareTo(previous) <= 0
          || !(EVIDENCE.contains(name)
              || name.matches("cache/[A-Za-z0-9_-][A-Za-z0-9_.-]{0,191}"))) {
        throw new IllegalArgumentException("Invalid or unsorted snapshot filename");
      }
      previous = name;
      if (!entry.path("size").isIntegralNumber()
          || entry.path("size").asLong() < 0
          || entry.path("size").asLong() > MAX_FILE
          || !digest(text(entry, "sha256"))) {
        throw new IllegalArgumentException("Invalid snapshot inventory value");
      }
      Path path =
          name.startsWith("cache/") ? cache.resolve(name.substring(6)) : bundle.resolve(name);
      requireEffectiveOwner(path, uid);
      byte[] bytes = read(path, MAX_FILE, owner);
      if (bytes.length != entry.path("size").asLong()
          || !DeltaRangerStartupProbe.sha256(bytes).equals(text(entry, "sha256"))) {
        throw new IllegalArgumentException("Snapshot file differs from manifest");
      }
      files.put(name, bytes);
      if (name.startsWith("cache/")) {
        totalCache += bytes.length;
      }
    }
    if (!files.keySet().containsAll(EVIDENCE) || totalCache > MAX_TOTAL) {
      throw new IllegalArgumentException("Missing evidence or aggregate cache limit exceeded");
    }
    int cacheCount = files.size() - EVIDENCE.size();
    if (cacheCount < 1 || cacheCount > 16) {
      throw new IllegalArgumentException("Invalid cache leaf count");
    }
    Set<String> expectedBundle = new HashSet<>(EVIDENCE);
    expectedBundle.add("cache");
    expectedBundle.add("manifest.json");
    listed(bundle, expectedBundle);
    Set<String> expectedCache = new HashSet<>();
    files.keySet().stream()
        .filter(name -> name.startsWith("cache/"))
        .forEach(name -> expectedCache.add(name.substring(6)));
    listed(cache, expectedCache);
    JsonNode provenance = JSON.readTree(files.get("source-provenance.json"));
    keys(provenance, PROVENANCE_KEYS);
    requireInt(provenance, "schemaVersion", 1);
    requireInt(provenance, "exitCode", 0);
    requireTextEquals(provenance, "sourceRunId", text(manifest, "sourceRunId"));
    requireLong(provenance, "sourcePid", manifest.path("sourcePid").asLong());
    for (String hash :
        List.of(
            "requestSha256",
            "profileSha256",
            "applicationSha256",
            "companionSha256",
            "fixtureSha256",
            "archiveManifestSha256",
            "sourceSecuritySha256",
            "exitRecordSha256")) {
      if (!digest(text(provenance, hash))) {
        throw new IllegalArgumentException("Invalid source hash");
      }
    }
    Instant started = Instant.parse(text(provenance, "captureStarted"));
    Instant completed = Instant.parse(text(provenance, "captureCompleted"));
    if (completed.isBefore(started)) {
      throw new IllegalArgumentException("Invalid capture interval");
    }
    validatePrior(files, manifest);
    Map<String, byte[]> cacheFiles = new TreeMap<>();
    for (String name : expectedCache) {
      cacheFiles.put(name, files.get("cache/" + name));
    }
    return Optional.of(new Snapshot(digest, manifest, provenance, cacheFiles));
  }

  static Map<String, Object> copy(Snapshot snapshot, Path emptyCache) throws Exception {
    if (!emptyCache.isAbsolute()) {
      throw new IllegalArgumentException("Cache target must be absolute");
    }
    Path target = emptyCache;
    requireCanonical(target);
    requireEffectiveOwner(target, effectiveUid());
    ownerOnlyDirectory(target, Files.getOwner(target, LinkOption.NOFOLLOW_LINKS).getName());
    listed(target, Set.of());
    List<Map<String, Object>> copied = new ArrayList<>();
    for (Map.Entry<String, byte[]> entry : snapshot.cacheFiles.entrySet()) {
      String name = entry.getKey();
      if (!name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]{0,191}")) {
        throw new IllegalArgumentException("Invalid cache leaf");
      }
      Path destination = target.resolve(name);
      Files.createFile(
          destination,
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      try (OutputStream stream = Files.newOutputStream(destination, StandardOpenOption.WRITE)) {
        stream.write(entry.getValue());
      }
      byte[] copiedBytes =
          read(destination, MAX_FILE, Files.getOwner(target, LinkOption.NOFOLLOW_LINKS).getName());
      if (!Arrays.equals(copiedBytes, entry.getValue())) {
        throw new IllegalStateException("Copied cache bytes differ");
      }
      Map<String, Object> leaf = new LinkedHashMap<>();
      leaf.put("name", name);
      leaf.put("size", copiedBytes.length);
      leaf.put("sha256", DeltaRangerStartupProbe.sha256(copiedBytes));
      copied.add(Collections.unmodifiableMap(leaf));
    }
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("manifestSha256", snapshot.manifestSha256);
    result.put("files", List.copyOf(copied));
    return Collections.unmodifiableMap(result);
  }

  static void validateProbeTarget(Path requestFile, Path target) throws Exception {
    if (!requestFile.isAbsolute() || !target.isAbsolute()) {
      throw new IllegalArgumentException("Probe paths must be absolute");
    }
    requireCanonical(requestFile);
    Path run = requestFile.getParent();
    requireCanonical(run);
    if (!target.equals(run.resolve("driver-security"))) {
      throw new IllegalArgumentException("Probe target is outside its request run");
    }
    long uid = effectiveUid();
    requireEffectiveOwner(run, uid);
    ownerOnlyDirectory(run, Files.getOwner(run, LinkOption.NOFOLLOW_LINKS).getName());
    privateOwnedFile(requestFile, uid);
    requireEffectiveOwner(target, uid);
    validateTarget(target);
  }

  static List<Map<String, Object>> inventory(Path cache) throws Exception {
    if (!cache.isAbsolute()) {
      throw new IllegalArgumentException("Cache path must be absolute");
    }
    Path directory = cache;
    requireCanonical(directory);
    String owner = Files.getOwner(directory, LinkOption.NOFOLLOW_LINKS).getName();
    ownerOnlyDirectory(directory, owner);
    List<Map<String, Object>> result = new ArrayList<>();
    try (var listing = Files.list(directory)) {
      for (Path leaf : listing.sorted().toList()) {
        String name = leaf.getFileName().toString();
        if (!name.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]{0,191}")) {
          throw new IllegalArgumentException("Invalid cache leaf");
        }
        byte[] bytes = read(leaf, MAX_FILE, owner, false);
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("size", bytes.length);
        item.put("sha256", DeltaRangerStartupProbe.sha256(bytes));
        result.add(Collections.unmodifiableMap(item));
      }
    }
    return List.copyOf(result);
  }

  static void requireOwner(String actual, String expected) {
    if (!actual.equals(expected)) {
      throw new IllegalArgumentException("Snapshot owner differs from run owner");
    }
  }

  static long effectiveUid() throws Exception {
    Process process = new ProcessBuilder("/usr/bin/id", "-u").start();
    try (InputStream stdout = process.getInputStream();
        InputStream stderr = process.getErrorStream();
        OutputStream stdin = process.getOutputStream()) {
      stdin.close();
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Effective UID lookup timed out");
      }
      byte[] result = stdout.readNBytes(33);
      if (process.exitValue() != 0
          || result.length == 0
          || result.length > 32
          || stderr.read() != -1) {
        throw new IllegalStateException("Effective UID lookup failed");
      }
      String value = new String(result, StandardCharsets.US_ASCII);
      if (!value.matches("(0|[1-9][0-9]{0,19})\\n?")) {
        throw new IllegalStateException("Invalid effective UID output");
      }
      return Long.parseLong(value.strip());
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  static void requireEffectiveOwner(Path path, long uid) throws Exception {
    Object owner = Files.getAttribute(path, "unix:uid", LinkOption.NOFOLLOW_LINKS);
    if (uid < 0 || !(owner instanceof Number) || ((Number) owner).longValue() != uid) {
      throw new IllegalArgumentException("Snapshot owner differs from effective UID");
    }
  }

  private static void privateOwnedFile(Path path, long uid) throws Exception {
    requireCanonical(path);
    BasicFileAttributes basic =
        Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    PosixFileAttributes posix =
        Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!basic.isRegularFile()
        || !"rw-------".equals(PosixFilePermissions.toString(posix.permissions()))
        || !"1"
            .equals(Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS).toString())) {
      throw new IllegalArgumentException("Probe request file is not private");
    }
    requireEffectiveOwner(path, uid);
  }

  private static void validateTarget(Path target) throws Exception {
    if (!target.isAbsolute()) {
      throw new IllegalArgumentException("Target path must be absolute");
    }
    requireCanonical(target);
    String owner = Files.getOwner(target, LinkOption.NOFOLLOW_LINKS).getName();
    ownerOnlyDirectory(target, owner);
    listed(target, Set.of());
  }

  private static void validatePrior(Map<String, byte[]> files, JsonNode manifest) throws Exception {
    JsonNode ready = JSON.readTree(files.get("ready.json"));
    JsonNode initial = JSON.readTree(files.get("initial-downloads.json"));
    JsonNode completed = JSON.readTree(files.get("completed.json"));
    keys(ready, READY_KEYS);
    if (!ready.path("spark").isTextual()
        || !ready.path("spark").asText().equals(text(manifest, "spark"))
        || !ready.path("user").isTextual()
        || !ready.path("groups").isArray()
        || !ready.path("applicationUser").isTextual()
        || !ready.path("applicationUser").asText().equals(ready.path("user").asText())
        || !ready.path("origins").isObject()
        || !ready.path("helpers").asBoolean(false)
        || !ready.path("visibleCatalogs").isObject()
        || !initial.isObject()
        || !completed.isObject()
        || completed.size() != 1
        || !completed.path("completed").asBoolean(false)
        || initial.size() != 5
        || !initial.path("freshEmptyCacheAtPreparation").asBoolean(false)
        || !ready.path("versions").isObject()) {
      throw new IllegalArgumentException("Prior default completion evidence is missing");
    }
    requireLong(ready.path("versions"), "policy", manifest.path("policyVersion").asLong());
    requireLong(ready.path("versions"), "role", manifest.path("roleVersion").asLong());
    requireTextEquals(ready.path("versions"), "service", text(manifest, "service"));
    requireTextEquals(ready.path("versions"), "definition", text(manifest, "definition"));
    requireLong(initial, "policyVersion", manifest.path("policyVersion").asLong());
    requireLong(initial, "roleVersion", manifest.path("roleVersion").asLong());
    long policyAttempt = positive(initial, "policyAttempt");
    long roleAttempt = positive(initial, "roleAttempt");
    if (policyAttempt == roleAttempt) {
      throw new IllegalArgumentException("Prior policy and role attempts must differ");
    }
    Map<Long, JsonNode> starts = new LinkedHashMap<>();
    Map<Long, JsonNode> ends = new LinkedHashMap<>();
    String run = null;
    String[] lines = new String(files.get("downloads.jsonl"), StandardCharsets.UTF_8).split("\\R");
    for (String line : lines) {
      if (line.isEmpty()) {
        continue;
      }
      JsonNode event = JSON.readTree(line);
      String eventRun = text(event, "run");
      if (run == null) {
        run = eventRun;
      } else if (!run.equals(eventRun)) {
        throw new IllegalArgumentException("Mixed prior gateway runs");
      }
      long attempt = positive(event, "attempt");
      if (event.has("finished")) {
        if (ends.putIfAbsent(attempt, event) != null) {
          throw new IllegalArgumentException("Duplicate prior terminal");
        }
      } else if (starts.putIfAbsent(attempt, event) != null) {
        throw new IllegalArgumentException("Duplicate prior start");
      }
    }
    if (!starts.keySet().equals(ends.keySet())) {
      throw new IllegalArgumentException("Prior gateway handlers unresolved");
    }
    matchingDownload(
        starts,
        ends,
        policyAttempt,
        RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED
            + text(manifest, "service"),
        "policy",
        manifest.path("policyVersion").asLong(),
        text(manifest, "service"));
    matchingDownload(
        starts,
        ends,
        roleAttempt,
        RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + text(manifest, "service"),
        "role",
        manifest.path("roleVersion").asLong(),
        text(manifest, "service"));
  }

  private static void matchingDownload(
      Map<Long, JsonNode> starts,
      Map<Long, JsonNode> ends,
      long attempt,
      String path,
      String kind,
      long version,
      String service) {
    JsonNode first = starts.get(attempt);
    JsonNode last = ends.get(attempt);
    if (first == null
        || last == null
        || !first.path("path").asText().equals(path)
        || !"GET".equals(first.path("method").asText())
        || !first.path("started").equals(last.path("started"))
        || !first.path("path").equals(last.path("path"))
        || !last.path("upstreamAttempted").asBoolean(false)
        || last.path("upstreamStatus").asInt() != 200
        || !last.path("upstreamResponseCompleted").isTextual()
        || !last.path("downstreamResponseCompleted").asBoolean(false)
        || !kind.equals(last.path("downloadKind").asText())
        || !service.equals(last.path("downloadService").asText())
        || !last.path("downloadVersion").isIntegralNumber()
        || last.path("downloadVersion").asLong() != version
        || !digest(last.path("responseSha256").asText())) {
      throw new IllegalArgumentException("Prior matching completed download is missing");
    }
  }

  private static byte[] read(Path path, long limit, String owner) throws Exception {
    return read(path, limit, owner, true);
  }

  private static byte[] read(Path path, long limit, String owner, boolean privateMode)
      throws Exception {
    requireCanonical(path);
    BasicFileAttributes before =
        Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    PosixFileAttributes posix =
        Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!before.isRegularFile()
        || before.size() > limit
        || !"1".equals(Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS).toString())
        || !("rw-------".equals(PosixFilePermissions.toString(posix.permissions()))
            || (!privateMode
                && "rw-r--r--".equals(PosixFilePermissions.toString(posix.permissions()))))) {
      throw new IllegalArgumentException("Unsafe snapshot file");
    }
    requireOwner(posix.owner().getName(), owner);
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (SeekableByteChannel channel =
        Files.newByteChannel(path, Set.of(StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))) {
      ByteBuffer buffer = ByteBuffer.allocate(8192);
      while (channel.read(buffer) != -1) {
        buffer.flip();
        if (output.size() + buffer.remaining() > limit) {
          throw new IllegalArgumentException("Snapshot file exceeds limit");
        }
        output.write(buffer.array(), 0, buffer.remaining());
        buffer.clear();
      }
    }
    BasicFileAttributes after =
        Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    PosixFileAttributes afterPosix =
        Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!before.fileKey().equals(after.fileKey())
        || before.size() != after.size()
        || !before.lastModifiedTime().equals(after.lastModifiedTime())
        || !posix.owner().equals(afterPosix.owner())
        || !posix.permissions().equals(afterPosix.permissions())
        || !"1".equals(Files.getAttribute(path, "unix:nlink", LinkOption.NOFOLLOW_LINKS).toString())
        || output.size() != after.size()) {
      throw new IllegalArgumentException("Snapshot file changed while reading");
    }
    return output.toByteArray();
  }

  private static void ownerOnlyDirectory(Path path, String owner) throws Exception {
    requireCanonical(path);
    BasicFileAttributes basic =
        Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    PosixFileAttributes posix =
        Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    if (!basic.isDirectory()
        || !"rwx------".equals(PosixFilePermissions.toString(posix.permissions()))) {
      throw new IllegalArgumentException("Snapshot directory is not private");
    }
    requireOwner(posix.owner().getName(), owner);
  }

  private static void requireCanonical(Path path) throws Exception {
    if (!path.isAbsolute() || !path.normalize().equals(path)) {
      throw new IllegalArgumentException("Noncanonical snapshot path");
    }
    Path current = path.getRoot();
    for (Path name : path) {
      current = current.resolve(name);
      if (Files.isSymbolicLink(current)) {
        throw new IllegalArgumentException("Symlink in snapshot path");
      }
      if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)
          && !current.toRealPath(LinkOption.NOFOLLOW_LINKS).equals(current)) {
        throw new IllegalArgumentException("Noncanonical path component");
      }
    }
  }

  private static void listed(Path directory, Set<String> expected) throws IOException {
    Set<String> actual = new HashSet<>();
    try (var entries = Files.list(directory)) {
      entries.forEach(path -> actual.add(path.getFileName().toString()));
    }
    if (!actual.equals(expected)) {
      throw new IllegalArgumentException("Unexpected snapshot directory entries");
    }
  }

  private static void keys(JsonNode object, Set<String> expected) {
    if (!object.isObject()) {
      throw new IllegalArgumentException("Expected strict object");
    }
    Set<String> actual = new HashSet<>();
    object.fieldNames().forEachRemaining(actual::add);
    if (!actual.equals(expected)) {
      throw new IllegalArgumentException("Unexpected object fields");
    }
  }

  private static String text(JsonNode object, String key) {
    JsonNode value = object.path(key);
    if (!value.isTextual() || value.asText().isEmpty()) {
      throw new IllegalArgumentException("Missing text: " + key);
    }
    return value.textValue();
  }

  private static void requireTextEquals(JsonNode object, String key, String expected) {
    if (!text(object, key).equals(expected)) {
      throw new IllegalArgumentException("Snapshot binding mismatch: " + key);
    }
  }

  private static void requireAllowed(JsonNode object, String key, Set<String> allowed) {
    if (!allowed.contains(text(object, key))) {
      throw new IllegalArgumentException("Unsupported pin: " + key);
    }
  }

  private static void requireInt(JsonNode object, String key, int expected) {
    if (!object.path(key).isIntegralNumber() || object.path(key).asInt() != expected) {
      throw new IllegalArgumentException("Wrong integer: " + key);
    }
  }

  private static void requireLong(JsonNode object, String key, long expected) {
    if (!object.path(key).isIntegralNumber() || object.path(key).asLong() != expected) {
      throw new IllegalArgumentException("Wrong integer: " + key);
    }
  }

  private static long positive(JsonNode object, String key) {
    if (!object.path(key).isIntegralNumber() || object.path(key).asLong() <= 0) {
      throw new IllegalArgumentException("Invalid attempt: " + key);
    }
    return object.path(key).asLong();
  }

  private static boolean digest(String value) {
    return value.matches("[0-9a-f]{64}");
  }

  private static Map<String, byte[]> cloneFiles(Map<String, byte[]> files) {
    Map<String, byte[]> result = new TreeMap<>();
    files.forEach((name, bytes) -> result.put(name, bytes.clone()));
    return Collections.unmodifiableMap(result);
  }
}
