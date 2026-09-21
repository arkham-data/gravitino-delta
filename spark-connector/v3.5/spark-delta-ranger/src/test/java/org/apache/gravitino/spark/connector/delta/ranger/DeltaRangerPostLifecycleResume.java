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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Test-only validation of the separately approved post-lifecycle continuation boundary. */
final class DeltaRangerPostLifecycleResume {
  static final String CHECKPOINT_SHA256 =
      "90653533d333a59880459f9a88c6d4db236f38e2236abc13bffb444f6e088cda";
  private static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final List<String> ROLES =
      List.of(
          "visibility_metadata",
          "visibility_columnReader",
          "visibility_visibilityOnly",
          "visibility_groupReader",
          "visibility_freshCallback",
          "metadata",
          "metadata_schema",
          "ordinary",
          "group",
          "global_guard",
          "audit_cleanup",
          "protocol");
  private static final List<String> USERS =
      List.of("metadata", "freshCallback", "columnReader", "visibilityOnly", "groupReader");
  private static final Set<String> PREFLIGHT_FIELDS =
      Set.of(
          "schemaVersion",
          "stage",
          "metalake",
          "expectedMetalakeId",
          "mode",
          "processIdentityVerified",
          "executableInputsVerified",
          "rangerPersistenceVerified",
          "callbackOwnershipReleased",
          "publicContinuityVerified",
          "s3ExactBytesVerified",
          "s3ObjectCount",
          "s3ManifestSha256",
          "storageBaseline",
          "transitionSha256",
          "processEvidenceSha256",
          "rangerPersistenceEvidenceSha256",
          "executableManifestSha256",
          "coldH2Sha256");

  @FunctionalInterface
  interface ReadOnlyGet {
    JsonNode get(String key, String path) throws Exception;
  }

  static JsonNode readBound(Path path, String digest) throws Exception {
    require(
        path.isAbsolute() && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS),
        "Absolute nonsymlink regular private input required");
    require(
        Files.getPosixFilePermissions(path).stream()
            .noneMatch(p -> p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_")),
        "Owner-only input required");
    require(isDigest(digest), "Independent SHA-256 required");
    byte[] bytes = Files.readAllBytes(path);
    StringBuilder actual = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes))
      actual.append(String.format("%02x", b));
    require(digest.equals(actual.toString()), "Input digest mismatch");
    JsonNode parsed = JSON.readTree(bytes);
    require(parsed != null && parsed.isObject(), "JSON object required");
    return parsed;
  }

  static Map<String, String> paths(JsonNode fixture) {
    Map<String, String> result = new LinkedHashMap<>();
    String lake = "/api/metalakes/" + encode(text(fixture, "metalake"));
    result.put("metalake", lake);
    result.put("catalogs", lake + "/catalogs");
    require(
        fixture.path("catalogs").isArray() && fixture.path("catalogs").size() == 3,
        "Exactly three fixture catalogs required");
    for (JsonNode catalog : fixture.path("catalogs")) {
      String name = text(catalog, "name");
      require(
          result.put(
                  "table:" + name,
                  lake
                      + "/catalogs/"
                      + encode(name)
                      + "/schemas/"
                      + encode(text(catalog, "database"))
                      + "/tables/"
                      + encode(text(catalog, "table")))
              == null,
          "Duplicate catalog");
    }
    for (String role : ROLES)
      result.put("role:" + role, lake + "/roles/" + encode(text(fixture, "rolesPrefix") + role));
    for (String user : USERS)
      result.put("user:" + user, lake + "/users/" + encode(principal(fixture, user)));
    result.put("group:query", lake + "/groups/" + encode(text(fixture.path("groups"), "query")));
    result.put("definition", "/service/public/v2/api/servicedef/217");
    result.put("tagService", "/service/public/v2/api/service/15");
    result.put("resourceService", "/service/public/v2/api/service/16");
    result.put("sideService", "/service/public/v2/api/service/17");
    result.put("downloader", "/service/xusers/users/9");
    result.put("roles", "/service/roles/roles");
    result.put(
        "policies",
        "/service/plugins/policies?serviceName=" + encode(text(fixture, "resourceService")));
    result.put("sidePolicy", "/service/public/v2/api/policy/53");
    result.put(
        "policyDownload",
        "/service/plugins/secure/policies/download/"
            + encode(text(fixture, "resourceService"))
            + "?lastKnownVersion=-1&pluginId=delta-post-lifecycle-resume");
    result.put(
        "roleDownload",
        "/service/roles/secure/download/"
            + encode(text(fixture, "resourceService"))
            + "?lastKnownRoleVersion=-1&pluginId=delta-post-lifecycle-resume");
    return result;
  }

  static void validateInputs(
      JsonNode fixture,
      JsonNode checkpoint,
      JsonNode baseline,
      JsonNode preflight,
      String checkpointDigest,
      String preflightDigest) {
    require(CHECKPOINT_SHA256.equals(checkpointDigest), "Original checkpoint binding required");
    fields(
        baseline,
        Set.of(
            "schemaVersion",
            "stage",
            "metalake",
            "expectedMetalakeId",
            "checkpointSha256",
            "preflightSha256",
            "policyVersion",
            "roleVersion",
            "observations"));
    identity(fixture, checkpoint, "post-role-lifecycle-local-readback");
    identity(fixture, baseline, "post-role-lifecycle-resume-baseline");
    identity(fixture, preflight, "post-role-lifecycle-root-preflight");
    require(
        checkpoint.path("lastObservedDownloads").path("policyVersion").asInt(-1) == 40
            && checkpoint.path("lastObservedDownloads").path("roleVersion").asInt(-1) == 63,
        "Historical checkpoint generations changed");
    require(
        baseline.path("policyVersion").isIntegralNumber()
            && baseline.path("roleVersion").isIntegralNumber()
            && baseline.path("policyVersion").asInt(-1) == 44
            && baseline.path("roleVersion").asInt(-1) == 63,
        "Current transition must be 44/63");
    require(
        checkpointDigest.equals(text(baseline, "checkpointSha256"))
            && isDigest(preflightDigest)
            && preflightDigest.equals(text(baseline, "preflightSha256")),
        "Checkpoint/preflight envelope binding mismatch");
    fields(preflight, PREFLIGHT_FIELDS);
    for (String key :
        List.of(
            "processIdentityVerified",
            "executableInputsVerified",
            "rangerPersistenceVerified",
            "callbackOwnershipReleased",
            "publicContinuityVerified",
            "s3ExactBytesVerified"))
      require(
          preflight.path(key).isBoolean() && preflight.path(key).booleanValue(),
          "Failed root preflight: " + key);
    require(
        preflight.path("s3ObjectCount").isIntegralNumber()
            && preflight.path("s3ObjectCount").asInt(-1) == 12
            && "restored-original-bytes-new-mtime".equals(text(preflight, "storageBaseline")),
        "Explicit restored-byte baseline required");
    for (String key :
        List.of(
            "s3ManifestSha256",
            "transitionSha256",
            "processEvidenceSha256",
            "rangerPersistenceEvidenceSha256",
            "executableManifestSha256"))
      require(isDigest(text(preflight, key)), "Missing root evidence digest: " + key);
    String mode = text(preflight, "mode");
    require(
        ("retained-process".equals(mode) && preflight.path("coldH2Sha256").isNull())
            || ("cold-restart".equals(mode) && isDigest(text(preflight, "coldH2Sha256"))),
        "Invalid process/H2 preflight mode");
    fields(checkpoint.path("roles"), Set.copyOf(ROLES));
    fields(checkpoint.path("users"), Set.copyOf(USERS));
    fields(checkpoint.path("groups"), Set.of("query"));
    JsonNode divergence = checkpoint.path("divergence");
    fields(
        divergence,
        Set.of(
            "globalGuardRoleRetainedLocally",
            "globalGuardPrimaryRangerContributionAbsent",
            "externalGuardPolicyId",
            "doNotRepairOrReplay"));
    for (String key :
        List.of(
            "globalGuardRoleRetainedLocally",
            "globalGuardPrimaryRangerContributionAbsent",
            "doNotRepairOrReplay"))
      require(
          divergence.path(key).isBoolean() && divergence.path(key).booleanValue(),
          "Protected divergence changed");
    require(divergence.path("externalGuardPolicyId").asInt(-1) == 53, "Wrong side policy binding");
    JsonNode observations = baseline.path("observations");
    fields(observations, paths(fixture).keySet());
    for (String key : paths(fixture).keySet()) {
      JsonNode observation = observations.path(key);
      fields(observation, Set.of("status", "body"));
      require(
          observation.path("status").isIntegralNumber()
              && observation.path("status").asInt()
                  == ("role:audit_cleanup".equals(key) ? 404 : 200),
          "Wrong expected HTTP status: " + key);
      require(observation.path("body").isObject(), "Complete response object required: " + key);
    }
    for (String role : ROLES)
      compare(
          "role:" + role, checkpoint.path("roles").path(role), observations.path("role:" + role));
    for (String user : USERS)
      compare(
          "user:" + user,
          response(200, checkpoint.path("users").path(user)),
          observations.path("user:" + user));
    compare(
        "group:query",
        response(200, checkpoint.path("groups").path("query")),
        observations.path("group:query"));
    validateModels(fixture, observations);
  }

  static JsonNode readCommand(Path path) throws Exception {
    require(
        path.isAbsolute() && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS),
        "Regular private command required");
    require(
        Files.getPosixFilePermissions(path).stream()
            .noneMatch(p -> p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_")),
        "Owner-only command required");
    return JSON.readTree(Files.readAllBytes(path));
  }

  static void requireCompletedReads(List<String> events) throws Exception {
    Set<Long> started = new HashSet<>();
    Set<Long> finished = new HashSet<>();
    for (String line : events) {
      JsonNode event = JSON.readTree(line);
      require("GET".equals(event.path("method").asText()), "Validation transport mutated state");
      long attempt = event.path("attempt").asLong(-1);
      require(attempt > 0, "Missing read attempt identity");
      if (!event.has("finished")) {
        require(started.add(attempt), "Duplicate read start");
        continue;
      }
      require(
          finished.add(attempt) && started.contains(attempt),
          "Read completion without unique start");
      require(
          event.path("upstreamAttempted").asBoolean()
              && event.path("upstreamResponseCompleted").isTextual()
              && event.path("downstreamResponseCompleted").asBoolean()
              && !event.has("transportFailure")
              && !event.has("upstreamCompletionUnresolved")
              && !event.has("faultStatus"),
          "Incomplete upstream/downstream read");
      int status = event.path("upstreamStatus").asInt(-1);
      require(
          status == 200 || (status == 404 && event.path("path").asText().endsWith("audit_cleanup")),
          "Unexpected completed read status");
      require(
          Files.isRegularFile(Path.of(text(event, "responseBodyFile")), LinkOption.NOFOLLOW_LINKS),
          "Missing complete response evidence");
    }
    require(!started.isEmpty() && started.equals(finished), "Read attempts remain unresolved");
  }

  static JsonNode get(URI endpoint, String path, String authorization) throws Exception {
    HttpResponse<byte[]> result =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build()
            .send(
                HttpRequest.newBuilder(endpoint.resolve(path))
                    .timeout(Duration.ofSeconds(90))
                    .header("Authorization", authorization)
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray());
    // send returns only after the full body completes. A truncated body/timeout throws, never
    // validates.
    return response(result.statusCode(), JSON.readTree(result.body()));
  }

  static int validateCurrent(JsonNode fixture, JsonNode baseline, ReadOnlyGet transport)
      throws Exception {
    ObjectNode current = JSON.createObjectNode();
    for (Map.Entry<String, String> request : paths(fixture).entrySet()) {
      JsonNode actual = transport.get(request.getKey(), request.getValue());
      compare(request.getKey(), baseline.path("observations").path(request.getKey()), actual);
      current.set(request.getKey(), actual);
    }
    validateModels(fixture, current);
    return current.size();
  }

  static JsonNode response(int status, JsonNode body) {
    ObjectNode result = JSON.createObjectNode().put("status", status);
    result.set("body", body);
    return result;
  }

  static void compare(String key, JsonNode expected, JsonNode actual) {
    fields(actual, Set.of("status", "body"));
    require(expected.path("status").equals(actual.path("status")), "HTTP status changed: " + key);
    require(
        normalized(key, expected.path("body")).equals(normalized(key, actual.path("body"))),
        "Full response model changed: " + key);
  }

  static void validateCommand(JsonNode command, boolean quarantined) {
    String operation = text(command, "operation");
    if ("close".equals(operation)) {
      fields(command, Set.of("operation"));
      return;
    }
    if ("snapshot".equals(operation)) {
      fields(command, Set.of("operation", "id"));
      require(text(command, "id").matches("[A-Za-z0-9_-]{1,80}"), "Unsafe snapshot identifier");
      return;
    }
    require(!quarantined, "Mutation quarantined; snapshot or close only");
    boolean user = Set.of("grant-user", "revoke-user").contains(operation);
    require(
        user
            || Set.of(
                    "grant-group",
                    "revoke-group",
                    "grant-select",
                    "revoke-select",
                    "delete-role",
                    "fault-role-update")
                .contains(operation),
        "Unknown or protected operation");
    fields(
        command,
        user ? Set.of("operation", "roleSuffix", "subject") : Set.of("operation", "roleSuffix"));
    String role = text(command, "roleSuffix");
    require(
        Set.of("ordinary", "group", "visibility_visibilityOnly").contains(role), "Protected role");
    if ("visibility_visibilityOnly".equals(role))
      require(
          user && "visibilityOnly".equals(text(command, "subject")),
          "Visibility role permits only its owned user membership changes");
    if (user)
      require(
          Set.of("freshCallback", "columnReader", "visibilityOnly", "groupReader")
              .contains(text(command, "subject")),
          "Protected or unknown principal");
  }

  private static void validateModels(JsonNode fixture, JsonNode observations) {
    require(
        body(observations, "metalake").path("code").asInt(-1) == 0
            && text(fixture, "metalake")
                .equals(body(observations, "metalake").path("metalake").path("name").asText()),
        "Wrong public metalake");
    Set<String> catalogs = new HashSet<>();
    for (JsonNode catalog : fixture.path("catalogs")) catalogs.add(text(catalog, "name"));
    JsonNode listed = body(observations, "catalogs");
    require(
        listed.path("code").asInt(-1) == 0 && listed.path("identifiers").isArray(),
        "Incomplete catalog list");
    Set<String> actualCatalogs = new HashSet<>();
    for (JsonNode identifier : listed.path("identifiers"))
      require(actualCatalogs.add(text(identifier, "name")), "Duplicate catalog identifier");
    require(catalogs.equals(actualCatalogs), "Catalog set changed");
    for (JsonNode catalog : fixture.path("catalogs")) {
      JsonNode table = body(observations, "table:" + text(catalog, "name"));
      require(
          table.path("code").asInt(-1) == 0 && table.path("table").isObject(),
          "Incomplete registered table");
      JsonNode properties = table.path("table").path("properties");
      require(
          "PUBLISHED".equals(properties.path("delta.registration.receipt.state").asText())
              && !properties.path("delta.registration.receipt.nativeId").asText().isBlank()
              && "0".equals(properties.path("delta.registration.receipt.nativeVersion").asText())
              && !properties.path("delta.registration.receipt.location").asText().isBlank(),
          "Original published version-zero receipt required");
    }
    for (String role : ROLES) {
      JsonNode local = body(observations, "role:" + role);
      if ("audit_cleanup".equals(role)) {
        require(
            local.path("code").asInt(-1) == 1003
                && "NoSuchRoleException".equals(local.path("type").asText()),
            "Audit role must remain absent");
      } else {
        require(
            local.path("code").asInt(-1) == 0
                && (text(fixture, "rolesPrefix") + role)
                    .equals(local.path("role").path("name").asText())
                && local.path("role").path("securableObjects").isArray(),
            "Local role identity/grants missing: " + role);
      }
    }
    for (String user : USERS)
      require(
          body(observations, "user:" + user).path("code").asInt(-1) == 0
              && principal(fixture, user)
                  .equals(body(observations, "user:" + user).path("user").path("name").asText()),
          "Local user identity changed");
    require(
        body(observations, "group:query").path("code").asInt(-1) == 0
            && text(fixture.path("groups"), "query")
                .equals(body(observations, "group:query").path("group").path("name").asText()),
        "Local group identity changed");
    for (Map.Entry<String, Integer> entry :
        Map.of(
                "definition",
                217,
                "tagService",
                15,
                "resourceService",
                16,
                "sideService",
                17,
                "downloader",
                9,
                "sidePolicy",
                53)
            .entrySet())
      require(
          body(observations, entry.getKey()).path("id").asInt(-1) == entry.getValue(),
          "Ranger identity changed: " + entry.getKey());
    require(
        text(fixture, "resourceDefinition")
            .equals(body(observations, "definition").path("name").asText()),
        "Definition name changed");
    require(
        text(fixture, "tagService").equals(body(observations, "tagService").path("name").asText()),
        "Tag service name changed");
    require(
        text(fixture, "resourceService")
                .equals(body(observations, "resourceService").path("name").asText())
            && body(observations, "resourceService").path("policyVersion").asInt(-1) == 44,
        "Resource generation changed");
    JsonNode rolesResponse = body(observations, "roles");
    JsonNode roles = rolesResponse.path("roles");
    require(
        roles.isArray()
            && rolesResponse.path("totalCount").asInt(-1) == roles.size()
            && rolesResponse.path("resultSize").asInt(-1) == roles.size()
            && rolesResponse.path("startIndex").asInt(-1) == 0,
        "Incomplete Ranger role list");
    Map<Integer, JsonNode> roleIds = byId(roles);
    require(!roleIds.containsKey(24), "Audit Ranger role reappeared");
    for (int id : List.of(7, 13, 14, 23, 25))
      require(roleIds.containsKey(id), "Protected Ranger role missing");
    require(
        canonical(fixture, "_fixed_protocol").equals(roleIds.get(14).path("name").asText())
            && canonical(fixture, "protocol").equals(roleIds.get(13).path("name").asText())
            && canonical(fixture, "global_guard").equals(roleIds.get(23).path("name").asText())
            && canonical(fixture, "metadata_schema").equals(roleIds.get(25).path("name").asText()),
        "Protected Ranger role identity changed");
    require(
        "GRAVITINO_CATALOG_OWNER_ROLE_3654023421300958302"
            .equals(roleIds.get(7).path("name").asText()),
        "Original orphan owner identity changed");
    for (int id : List.of(7, 13, 23))
      for (String field : List.of("users", "groups", "roles"))
        require(
            roleIds.get(id).path(field).isMissingNode()
                || (roleIds.get(id).path(field).isArray() && roleIds.get(id).path(field).isEmpty()),
            "Protected role membership changed");
    JsonNode schema = roleIds.get(25);
    require(
        schema.path("users").isArray()
            && schema.path("users").size() == 1
            && principal(fixture, "metadata")
                .equals(schema.path("users").get(0).path("name").asText())
            && schema.path("users").get(0).path("isAdmin").isBoolean()
            && !schema.path("users").get(0).path("isAdmin").asBoolean(),
        "Schema membership changed");
    for (String field : List.of("groups", "roles"))
      require(
          schema.path(field).isMissingNode()
              || (schema.path(field).isArray() && schema.path(field).isEmpty()),
          "Schema role nesting changed");
    for (JsonNode role : roles)
      require(
          !canonical(fixture, "audit_cleanup").equals(role.path("name").asText()),
          "Audit role name reappeared");
    JsonNode policiesResponse = body(observations, "policies");
    JsonNode policies = policiesResponse.path("policies");
    require(
        policies.isArray()
            && policiesResponse.path("totalCount").asInt(-1) == policies.size()
            && policiesResponse.path("resultSize").asInt(-1) == policies.size()
            && policiesResponse.path("startIndex").asInt(-1) == 0,
        "Incomplete Ranger policy list");
    Set<Integer> requiredIds = new TreeSet<>();
    for (int id = 35; id <= 52; id++) requiredIds.add(id);
    requiredIds.add(54);
    require(requiredIds.equals(byId(policies).keySet()), "Managed/foreign policy registry changed");
    Set<String> protectedNames =
        Set.of(
            canonical(fixture, "global_guard"),
            canonical(fixture, "audit_cleanup"),
            canonical(fixture, "metadata_schema"));
    require(
        Collections.disjoint(roleReferences(policies), protectedNames),
        "Protected role contribution reappeared");
    for (JsonNode policy : policies)
      if (policy.path("id").asInt() <= 43)
        require(
            policy.path("policyLabels").isArray()
                && contains(
                    policy.path("policyLabels"),
                    "gravitino-metalake-id:" + text(fixture, "expectedMetalakeId")),
            "Persisted metalake label binding changed");
    JsonNode side = body(observations, "sidePolicy");
    require(
        side.path("isEnabled").isBoolean()
            && side.path("isEnabled").asBoolean()
            && roleReferences(side).equals(Set.of(canonical(fixture, "global_guard"))),
        "Side guard reference changed");
    JsonNode filters =
        normalized("sideService", body(observations, "sideService"))
            .path("configs")
            .path("ranger.plugin.audit.filters");
    JsonNode retainedFilters =
        JSON.valueToTree(
            List.of(
                Map.of("accessResult", "DENIED", "users", List.of("admin"), "isAudited", true),
                Map.of(
                    "accessResult",
                    "ALLOWED",
                    "users",
                    List.of(principal(fixture, "columnReader")),
                    "isAudited",
                    true)));
    require(retainedFilters.equals(filters), "Post-cleanup side audit filters changed");
    JsonNode policyDownload = body(observations, "policyDownload");
    JsonNode roleDownload = body(observations, "roleDownload");
    require(
        policyDownload.path("policyVersion").asInt(-1) == 44
            && policyDownload.path("serviceId").asInt(-1) == 16
            && roleDownload.path("roleVersion").asInt(-1) == 63,
        "Fresh download generations changed");
    for (JsonNode download : List.of(policyDownload, roleDownload))
      require(
          text(fixture, "resourceService").equals(download.path("serviceName").asText()),
          "Wrong downloaded service");
    require(
        !byId(policies).get(48).path("isEnabled").asBoolean(true), "Policy48 must remain disabled");
    ArrayNode downloadablePolicies = JSON.createArrayNode();
    for (JsonNode policy : policies) {
      if (policy.path("id").asInt() != 48) {
        require(
            policy.path("isEnabled").isBoolean() && policy.path("isEnabled").asBoolean(),
            "Unexpected disabled policy");
        downloadablePolicies.add(policy);
      }
    }
    Set<Integer> downloadIds = new TreeSet<>(requiredIds);
    downloadIds.remove(48);
    require(
        byId(policyDownload.path("policies")).keySet().equals(downloadIds),
        "Incomplete policy download");
    require(
        byId(roleDownload.path("rangerRoles")).keySet().equals(roleIds.keySet()),
        "Incomplete role download");
    require(
        canonicalArrays(downloadablePolicies, "policies")
            .equals(canonicalArrays(policyDownload.path("policies"), "policies")),
        "Downloaded policies differ from Admin models");
    require(
        canonicalArrays(roles, "roles")
            .equals(canonicalArrays(roleDownload.path("rangerRoles"), "roles")),
        "Downloaded roles differ from Admin models");
  }

  private static JsonNode normalized(String key, JsonNode body) {
    require(body.isObject(), "Complete JSON response required: " + key);
    ObjectNode result = body.deepCopy();
    if ("role:audit_cleanup".equals(key)) result.remove(List.of("message", "stack"));
    if (Set.of("roles", "policies").contains(key)) result.remove("queryTimeMS");
    if ("sideService".equals(key)) {
      JsonNode filters = result.path("configs").path("ranger.plugin.audit.filters");
      require(filters.isTextual(), "Side audit filters missing");
      try {
        ObjectMapper filterParser =
            new ObjectMapper()
                .enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        JsonNode parsed = filterParser.readTree(filters.asText());
        require(parsed.isArray(), "Audit filter array required");
        ((ObjectNode) result.path("configs")).set("ranger.plugin.audit.filters", parsed);
      } catch (IOException e) {
        throw new IllegalArgumentException("Malformed side audit filters", e);
      }
    }
    return canonicalArrays(result, "");
  }

  private static JsonNode canonicalArrays(JsonNode node, String field) {
    if (node.isObject()) {
      ObjectNode result = JSON.createObjectNode();
      node.fields()
          .forEachRemaining(
              entry ->
                  result.set(entry.getKey(), canonicalArrays(entry.getValue(), entry.getKey())));
      return result;
    }
    if (node.isArray()) {
      List<JsonNode> values = new ArrayList<>();
      node.forEach(value -> values.add(canonicalArrays(value, "")));
      if (Set.of(
              "roles",
              "users",
              "groups",
              "securableObjects",
              "privileges",
              "identifiers",
              "policies",
              "rangerRoles")
          .contains(field)) values.sort(Comparator.comparing(JsonNode::toString));
      ArrayNode result = JSON.createArrayNode();
      values.forEach(result::add);
      return result;
    }
    return node;
  }

  private static Map<Integer, JsonNode> byId(JsonNode models) {
    require(models.isArray(), "Complete model array required");
    Map<Integer, JsonNode> result = new LinkedHashMap<>();
    for (JsonNode model : models) {
      require(model.path("id").isIntegralNumber(), "Model ID required");
      require(result.put(model.path("id").intValue(), model) == null, "Duplicate model ID");
    }
    return result;
  }

  private static Set<String> roleReferences(JsonNode model) {
    Set<String> result = new HashSet<>();
    if (model.isObject())
      model
          .fields()
          .forEachRemaining(
              entry -> {
                if ("roles".equals(entry.getKey())) {
                  require(entry.getValue().isArray(), "Role reference array required");
                  entry
                      .getValue()
                      .forEach(
                          role -> {
                            require(role.isTextual(), "Role name reference required");
                            result.add(role.asText());
                          });
                } else result.addAll(roleReferences(entry.getValue()));
              });
    if (model.isArray()) model.forEach(value -> result.addAll(roleReferences(value)));
    return result;
  }

  private static void identity(JsonNode fixture, JsonNode input, String stage) {
    require(
        input.path("schemaVersion").isIntegralNumber()
            && input.path("schemaVersion").asInt() == 1
            && stage.equals(text(input, "stage")),
        "Wrong resume stage/schema");
    require(
        text(fixture, "metalake").equals(text(input, "metalake"))
            && "986854579816189983".equals(text(fixture, "expectedMetalakeId"))
            && text(fixture, "expectedMetalakeId").equals(text(input, "expectedMetalakeId")),
        "Wrong fixture identity");
  }

  private static boolean contains(JsonNode array, String value) {
    for (JsonNode element : array)
      if (element.isTextual() && value.equals(element.asText())) return true;
    return false;
  }

  private static JsonNode body(JsonNode observations, String key) {
    return observations.path(key).path("body");
  }

  private static String canonical(JsonNode fixture, String suffix) {
    return "GRAVITINO_METALAKE_"
        + text(fixture, "expectedMetalakeId")
        + "_ROLE_"
        + text(fixture, "rolesPrefix")
        + suffix;
  }

  private static String principal(JsonNode fixture, String key) {
    return "metadata".equals(key)
        ? text(fixture, "metadataPrincipal")
        : text(fixture.path("subjects"), key);
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static boolean isDigest(String value) {
    return value != null && value.matches("[0-9a-f]{64}");
  }

  private static String text(JsonNode node, String key) {
    require(
        node.path(key).isTextual() && !node.path(key).asText().isBlank(), "Missing string: " + key);
    return node.path(key).asText();
  }

  private static void fields(JsonNode node, Set<String> expected) {
    require(node.isObject(), "Object required");
    Set<String> actual = new HashSet<>();
    node.fieldNames().forEachRemaining(actual::add);
    require(actual.equals(expected), "Closed input fields mismatch");
  }

  private static void require(boolean valid, String message) {
    if (!valid) throw new IllegalArgumentException(message);
  }
}
