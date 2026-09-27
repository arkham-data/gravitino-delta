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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.ranger.plugin.util.RangerRESTUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRangerStartupCache {
  private static final ObjectMapper JSON = new ObjectMapper();
  @TempDir Path directory;

  @Test
  void emptyModeRequiresAbsentBundle() throws Exception {
    Path root = directory.toRealPath();
    Path request =
        Files.createFile(
            root.resolve("request.json"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    JsonNode input =
        new ObjectMapper()
            .readTree(
                "{\"catalogs\":[\"a\"],\"startupProbe\":{\"mode\":\"empty\",\"query\":{"
                    + "\"catalog\":\"a\",\"schema\":\"db\",\"table\":\"t\","
                    + "\"columns\":[\"secret\"],\"expectedRows\":[]}}}");
    assertTrue(
        DeltaRangerStartupCache.load(
                request, input, DeltaRangerStartupProbe.parse(input).orElseThrow())
            .isEmpty());
  }

  @Test
  void exactSnapshotCopiesBothLeavesWithoutRewritingTheirBytes() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    DeltaRangerStartupCache.Snapshot snapshot =
        DeltaRangerStartupCache.load(fixture.requestFile(), fixture.input(), fixture.request())
            .orElseThrow();
    Path target =
        Files.createDirectory(
            directory.toRealPath().resolve("target"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Map<String, Object> copied = DeltaRangerStartupCache.copy(snapshot, target);
    assertEquals(fixture.request().manifestSha256().orElseThrow(), copied.get("manifestSha256"));
    assertArrayEquals(
        Files.readAllBytes(fixture.cacheLeaf()), Files.readAllBytes(target.resolve("policy")));
    assertArrayEquals(
        Files.readAllBytes(fixture.rolesLeaf()), Files.readAllBytes(target.resolve("roles")));
    assertEquals(2, snapshot.cacheFiles().size());
  }

  @Test
  void alteredCacheBytesFailBeforeTargetPreparation() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.writeString(fixture.cacheLeaf(), "changed");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    assertFalse(Files.exists(directory.resolve("driver-security/cache")));
  }

  @Test
  void unlistedLeafAndSymlinkReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.createFile(fixture.cacheLeaf().getParent().resolve("extra"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    Files.delete(fixture.cacheLeaf().getParent().resolve("extra"));
    Files.delete(fixture.cacheLeaf());
    Files.createSymbolicLink(fixture.cacheLeaf(), fixture.rolesLeaf());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void hardLinkRejectsEvenWithExactBytes() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Path link = directory.toRealPath().resolve("same-bytes");
    Files.createLink(link, fixture.cacheLeaf());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void ownerComparisonRejectsDistinctPrincipal() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.requireOwner("other-principal", "run-principal"));
  }

  @Test
  void wrongManifestDigestRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.writeString(fixture.manifestFile(), "changed");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void wrongServiceBindingRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(manifest -> manifest.put("service", "other"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void sameProcessPidRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(manifest -> manifest.put("sourcePid", ProcessHandle.current().pid()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void unsafeInventoryNameRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(
        manifest -> ((ObjectNode) manifest.path("files").get(0)).put("name", "cache/../escape"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void sourceParentSymlinkRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Path real = directory.toRealPath().resolve("startup-cache-input");
    Path moved = directory.toRealPath().resolve("moved-bundle");
    Files.move(real, moved);
    Files.createSymbolicLink(real, moved);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void targetSymlinkRejectsBeforeCopy() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    DeltaRangerStartupCache.Snapshot snapshot =
        DeltaRangerStartupCache.load(fixture.requestFile(), fixture.input(), fixture.request())
            .orElseThrow();
    Path real =
        Files.createDirectory(
            directory.toRealPath().resolve("real-target"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Path alias = directory.toRealPath().resolve("alias-target");
    Files.createSymbolicLink(alias, real);
    assertThrows(
        IllegalArgumentException.class, () -> DeltaRangerStartupCache.copy(snapshot, alias));
  }

  @Test
  void probeTargetMustBeCanonicalDriverSecuritySibling() throws Exception {
    Path root = directory.toRealPath();
    Path run =
        Files.createDirectory(
            root.resolve("private-run"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Fixture fixture = new Fixture(run);
    Path target =
        Files.createDirectory(
            run.resolve("driver-security"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    DeltaRangerStartupCache.validateProbeTarget(fixture.requestFile(), target);
    Path outside =
        Files.createDirectory(
            root.resolve("outside"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.validateProbeTarget(fixture.requestFile(), outside));
    Path wrongSibling =
        Files.createDirectory(
            run.resolve("other"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.validateProbeTarget(fixture.requestFile(), wrongSibling));
  }

  @Test
  void probeOwnerAndPrivateRunModeBindToEffectiveUid() throws Exception {
    Path root = directory.toRealPath();
    Path run =
        Files.createDirectory(
            root.resolve("private-run"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Fixture fixture = new Fixture(run);
    Path target =
        Files.createDirectory(
            run.resolve("driver-security"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    long uid = DeltaRangerStartupCache.effectiveUid();
    DeltaRangerStartupCache.requireEffectiveOwner(target, uid);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.requireEffectiveOwner(target, uid + 1));
    Files.setPosixFilePermissions(run, PosixFilePermissions.fromString("rwxr-xr-x"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.validateProbeTarget(fixture.requestFile(), target));
  }

  @Test
  void oneDeclaredCacheLeafIsAcceptedWithoutInventingRoles() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.delete(fixture.rolesLeaf());
    fixture.rewriteManifest(
        manifest -> {
          ArrayNode files = (ArrayNode) manifest.path("files");
          for (int i = files.size() - 1; i >= 0; i--) {
            if ("cache/roles".equals(files.get(i).path("name").asText())) {
              files.remove(i);
            }
          }
        });
    DeltaRangerStartupCache.Snapshot snapshot =
        DeltaRangerStartupCache.load(fixture.requestFile(), fixture.input(), fixture.request())
            .orElseThrow();
    assertEquals(1, snapshot.cacheFiles().size());
    assertTrue(snapshot.cacheFiles().containsKey("policy"));
  }

  @Test
  void emptySnapshotAndOversizedManifestReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.delete(fixture.rolesLeaf());
    Files.delete(fixture.cacheLeaf());
    fixture.rewriteManifest(
        manifest -> {
          ArrayNode files = (ArrayNode) manifest.path("files");
          for (int i = files.size() - 1; i >= 0; i--) {
            if (files.get(i).path("name").asText().startsWith("cache/")) {
              files.remove(i);
            }
          }
        });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    Files.writeString(fixture.manifestFile(), "x".repeat(65 * 1024));
    fixture.bindManifestDigest();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void wrongPermissionsAndDirectoryInPlaceOfFileReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    Files.setPosixFilePermissions(
        fixture.cacheLeaf(), PosixFilePermissions.fromString("rw-r--r--"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    Files.delete(fixture.cacheLeaf());
    Files.createDirectory(fixture.cacheLeaf());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void unsupportedVersionAndNonzeroPriorExitReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(manifest -> manifest.put("spark", "3.5.5"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    fixture.rewriteManifest(manifest -> manifest.put("spark", "3.5.4"));
    fixture.rewriteEvidence("source-provenance.json", provenance -> provenance.put("exitCode", 1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "service",
        "definition",
        "metalake",
        "spark",
        "delta",
        "ranger",
        "scala",
        "java",
        "policyVersion",
        "roleVersion"
      })
  void unsupportedManifestBindingRejects(String key) throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(
        manifest -> {
          if ("java".equals(key)) {
            manifest.put(key, 18);
          } else if (key.endsWith("Version")) {
            manifest.put(key, -1);
          } else {
            manifest.put(key, "unsupported");
          }
        });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"/absolute", "cache/nested/name", "cache/..", "cache/policy"})
  void unsafeOrDuplicateInventoryNameRejects(String name) throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(
        manifest -> ((ObjectNode) manifest.path("files").get(1)).put("name", name));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"size", "sha256"})
  void wrongFileInventoryRejects(String field) throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(
        manifest -> {
          ObjectNode entry = (ObjectNode) manifest.path("files").get(0);
          if ("size".equals(field)) {
            entry.put(field, entry.path(field).asLong() + 1);
          } else {
            entry.put(field, "c".repeat(64));
          }
        });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void priorCompletionModeAndMissingMatchingDownloadReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteEvidence("completed.json", completed -> completed.put("mode", "probe"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    fixture.rewriteEvidence("completed.json", completed -> completed.remove("mode"));
    fixture.rewriteRawEvidence(
        "downloads.jsonl", text -> text.replace("\"downloadVersion\":1", "\"downloadVersion\":2"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void manifestAndTargetParentSymlinksReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    DeltaRangerStartupCache.Snapshot snapshot =
        DeltaRangerStartupCache.load(fixture.requestFile(), fixture.input(), fixture.request())
            .orElseThrow();
    Path realParent =
        Files.createDirectory(
            directory.toRealPath().resolve("real-parent"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Files.createDirectory(
        realParent.resolve("target"),
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Path aliasParent = directory.toRealPath().resolve("alias-parent");
    Files.createSymbolicLink(aliasParent, realParent);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerStartupCache.copy(snapshot, aliasParent.resolve("target")));
    Path moved = directory.toRealPath().resolve("moved-manifest");
    Files.move(fixture.manifestFile(), moved);
    Files.createSymbolicLink(fixture.manifestFile(), moved);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void declaredCountAndPerFileSizeLimitsReject() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteManifest(
        manifest ->
            ((ObjectNode) manifest.path("files").get(0)).put("size", 16L * 1024 * 1024 + 1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
    fixture.rewriteManifest(
        manifest -> {
          ArrayNode files = (ArrayNode) manifest.path("files");
          for (int i = 0; i < 15; i++) {
            ObjectNode leaf = files.addObject();
            leaf.put("name", "cache/extra" + i);
            leaf.put("size", 1);
            leaf.put("sha256", "d".repeat(64));
          }
        });
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void aggregateCacheLimitRejectsIndividuallyAllowedLeaves() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    byte[] content = new byte[13 * 1024 * 1024];
    Path cache = fixture.cacheLeaf().getParent();
    Map<String, Path> leaves = new TreeMap<>();
    for (String name : new String[] {"policy", "roles", "extra1", "extra2", "extra3"}) {
      Path leaf = cache.resolve(name);
      if (!Files.exists(leaf)) {
        Files.createFile(
            leaf,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      }
      Files.write(leaf, content);
      leaves.put(name, leaf);
    }
    fixture.replaceCacheInventory(leaves);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  @Test
  void priorVersionDisagreementRejects() throws Exception {
    Fixture fixture = new Fixture(directory.toRealPath());
    fixture.rewriteEvidence(
        "ready.json", ready -> ((ObjectNode) ready.path("versions")).put("role", 2));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaRangerStartupCache.load(
                fixture.requestFile(), fixture.input(), fixture.request()));
  }

  private static final class Fixture {
    private final Path requestFile;
    private final Path cacheLeaf;
    private final Path rolesLeaf;
    private final ObjectNode input;
    private final Path manifestFile;

    private Fixture(Path run) throws Exception {
      requestFile =
          Files.createFile(
              run.resolve("request.json"),
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      Path bundle =
          Files.createDirectory(
              run.resolve("startup-cache-input"),
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
      Path cache =
          Files.createDirectory(
              bundle.resolve("cache"),
              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
      cacheLeaf =
          privateBytes(cache.resolve("policy"), "policy bytes".getBytes(StandardCharsets.UTF_8));
      rolesLeaf =
          privateBytes(cache.resolve("roles"), "role bytes".getBytes(StandardCharsets.UTF_8));
      ObjectNode provenance = JSON.createObjectNode();
      provenance.put("schemaVersion", 1);
      provenance.put("sourceRunId", "previous-launch");
      provenance.put("sourcePid", ProcessHandle.current().pid() + 1);
      provenance.put("exitCode", 0);
      for (String key :
          new String[] {
            "requestSha256",
            "profileSha256",
            "applicationSha256",
            "companionSha256",
            "fixtureSha256",
            "archiveManifestSha256",
            "sourceSecuritySha256",
            "exitRecordSha256"
          }) {
        provenance.put(key, "a".repeat(64));
      }
      provenance.put("captureStarted", "2026-09-27T00:00:00Z");
      provenance.put("captureCompleted", "2026-09-27T00:00:01Z");
      privateBytes(bundle.resolve("source-provenance.json"), JSON.writeValueAsBytes(provenance));
      ObjectNode ready = JSON.createObjectNode();
      ready.put("spark", "3.5.4");
      ready.put("user", "query-user");
      ready.putArray("groups");
      ready.put("applicationUser", "query-user");
      ready.putObject("origins");
      ready.put("helpers", true);
      ready.putObject("visibleCatalogs").put("a", true);
      ObjectNode versions = ready.putObject("versions");
      versions.put("service", "service");
      versions.put("definition", "spark");
      versions.put("policy", 1);
      versions.put("role", 1);
      privateBytes(bundle.resolve("ready.json"), JSON.writeValueAsBytes(ready));
      ObjectNode initial = JSON.createObjectNode();
      initial.put("policyAttempt", 1);
      initial.put("roleAttempt", 2);
      initial.put("policyVersion", 1);
      initial.put("roleVersion", 1);
      initial.put("freshEmptyCacheAtPreparation", true);
      privateBytes(bundle.resolve("initial-downloads.json"), JSON.writeValueAsBytes(initial));
      String policyPath =
          RangerRESTUtils.REST_URL_POLICY_GET_FOR_SECURE_SERVICE_IF_UPDATED + "service";
      String rolePath = RangerRESTUtils.REST_URL_SERVICE_SERCURE_GET_USER_GROUP_ROLES + "service";
      String log =
          event(1, policyPath)
              + "\n"
              + terminal(1, policyPath, "policy")
              + "\n"
              + event(2, rolePath)
              + "\n"
              + terminal(2, rolePath, "role")
              + "\n";
      privateBytes(bundle.resolve("downloads.jsonl"), log.getBytes(StandardCharsets.UTF_8));
      privateBytes(
          bundle.resolve("completed.json"),
          "{\"completed\":true}".getBytes(StandardCharsets.UTF_8));
      ObjectNode manifest = JSON.createObjectNode();
      manifest.put("schemaVersion", 1);
      manifest.put("service", "service");
      manifest.put("definition", "spark");
      manifest.put("metalake", "lake");
      manifest.put("spark", "3.5.4");
      manifest.put("delta", "3.3.0");
      manifest.put("ranger", "2.8.0");
      manifest.put("scala", "2.12");
      manifest.put("java", 17);
      manifest.put("sourceRunId", "previous-launch");
      manifest.put("sourcePid", ProcessHandle.current().pid() + 1);
      manifest.put("policyVersion", 1);
      manifest.put("roleVersion", 1);
      ArrayNode files = manifest.putArray("files");
      Map<String, Path> paths = new TreeMap<>();
      for (String name :
          new String[] {
            "source-provenance.json",
            "ready.json",
            "initial-downloads.json",
            "downloads.jsonl",
            "completed.json"
          }) {
        paths.put(name, bundle.resolve(name));
      }
      paths.put("cache/policy", cacheLeaf);
      paths.put("cache/roles", rolesLeaf);
      paths.forEach(
          (name, path) -> {
            try {
              byte[] bytes = Files.readAllBytes(path);
              ObjectNode item = files.addObject();
              item.put("name", name);
              item.put("size", bytes.length);
              item.put("sha256", DeltaRangerStartupProbe.sha256(bytes));
            } catch (Exception failure) {
              throw new IllegalStateException(failure);
            }
          });
      byte[] manifestBytes = JSON.writeValueAsBytes(manifest);
      manifestFile = privateBytes(bundle.resolve("manifest.json"), manifestBytes);
      String digest = DeltaRangerStartupProbe.sha256(manifestBytes);
      input =
          (ObjectNode)
              JSON.readTree(
                  "{\"resourceService\":\"service\",\"resourceDefinition\":\"spark\","
                      + "\"metalake\":\"lake\",\"catalogs\":[\"a\"],\"startupProbe\":{\"mode\":\"snapshot\","
                      + "\"manifestSha256\":\""
                      + digest
                      + "\",\"query\":{\"catalog\":\"a\","
                      + "\"schema\":\"db\",\"table\":\"t\",\"columns\":[\"secret\"],"
                      + "\"expectedRows\":[]}}}");
    }

    Path requestFile() {
      return requestFile;
    }

    JsonNode input() {
      return input;
    }

    Path cacheLeaf() {
      return cacheLeaf;
    }

    Path rolesLeaf() {
      return rolesLeaf;
    }

    Path manifestFile() {
      return manifestFile;
    }

    DeltaRangerStartupProbe.Request request() {
      return DeltaRangerStartupProbe.parse(input).orElseThrow();
    }

    void rewriteManifest(Consumer<ObjectNode> change) throws Exception {
      ObjectNode manifest = (ObjectNode) JSON.readTree(manifestFile.toFile());
      change.accept(manifest);
      byte[] bytes = JSON.writeValueAsBytes(manifest);
      Files.write(manifestFile, bytes);
      bindManifestDigest();
    }

    void bindManifestDigest() throws Exception {
      ((ObjectNode) input.path("startupProbe"))
          .put("manifestSha256", DeltaRangerStartupProbe.sha256(Files.readAllBytes(manifestFile)));
    }

    void rewriteEvidence(String name, Consumer<ObjectNode> change) throws Exception {
      Path path = manifestFile.getParent().resolve(name);
      ObjectNode record = (ObjectNode) JSON.readTree(path.toFile());
      change.accept(record);
      byte[] bytes = JSON.writeValueAsBytes(record);
      Files.write(path, bytes);
      rewriteManifest(
          manifest -> {
            for (JsonNode entry : manifest.path("files")) {
              if (name.equals(entry.path("name").asText())) {
                ((ObjectNode) entry).put("size", bytes.length);
                ((ObjectNode) entry).put("sha256", DeltaRangerStartupProbe.sha256(bytes));
              }
            }
          });
    }

    void rewriteRawEvidence(String name, UnaryOperator<String> change) throws Exception {
      Path path = manifestFile.getParent().resolve(name);
      byte[] bytes = change.apply(Files.readString(path)).getBytes(StandardCharsets.UTF_8);
      Files.write(path, bytes);
      rewriteManifest(
          manifest -> {
            for (JsonNode entry : manifest.path("files")) {
              if (name.equals(entry.path("name").asText())) {
                ((ObjectNode) entry).put("size", bytes.length);
                ((ObjectNode) entry).put("sha256", DeltaRangerStartupProbe.sha256(bytes));
              }
            }
          });
    }

    void replaceCacheInventory(Map<String, Path> leaves) throws Exception {
      ObjectNode manifest = (ObjectNode) JSON.readTree(manifestFile.toFile());
      Map<String, ObjectNode> entries = new TreeMap<>();
      for (JsonNode entry : manifest.path("files")) {
        String name = entry.path("name").asText();
        if (!name.startsWith("cache/")) {
          entries.put(name, (ObjectNode) entry);
        }
      }
      for (Map.Entry<String, Path> leaf : leaves.entrySet()) {
        byte[] bytes = Files.readAllBytes(leaf.getValue());
        ObjectNode entry = JSON.createObjectNode();
        entry.put("name", "cache/" + leaf.getKey());
        entry.put("size", bytes.length);
        entry.put("sha256", DeltaRangerStartupProbe.sha256(bytes));
        entries.put("cache/" + leaf.getKey(), entry);
      }
      ArrayNode files = manifest.putArray("files");
      entries.values().forEach(files::add);
      Files.write(manifestFile, JSON.writeValueAsBytes(manifest));
      bindManifestDigest();
    }

    private static Path privateBytes(Path path, byte[] bytes) throws Exception {
      Files.createFile(
          path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      Files.write(path, bytes);
      return path;
    }

    private static String event(int attempt, String path) throws Exception {
      ObjectNode value = JSON.createObjectNode();
      value.put("run", "gateway-run");
      value.put("attempt", attempt);
      value.put("method", "GET");
      value.put("path", path);
      value.put("started", "2026-09-27T00:00:00Z");
      return JSON.writeValueAsString(value);
    }

    private static String terminal(int attempt, String path, String kind) throws Exception {
      ObjectNode value = (ObjectNode) JSON.readTree(event(attempt, path));
      value.put("upstreamAttempted", true);
      value.put("upstreamStatus", 200);
      value.put("upstreamResponseCompleted", "2026-09-27T00:00:01Z");
      value.put("downstreamResponseCompleted", true);
      value.put("downloadKind", kind);
      value.put("downloadService", "service");
      value.put("downloadVersion", 1);
      value.put("responseSha256", "b".repeat(64));
      value.put("finished", "2026-09-27T00:00:02Z");
      value.put("activeAfter", 0);
      return JSON.writeValueAsString(value);
    }
  }
}
