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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoAdminClient.AdminClientBuilder;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Public REST callback acceptance against explicitly provisioned external production services. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.production.enabled", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestDeltaRangerDownloadedPolicies {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String PREFIX = "gravitino.ranger.delta.production.";
  private JsonNode fixture;
  private Path evidence;
  private Properties adminCredentials;
  private URI adminUrl;
  private GravitinoAdminClient client;
  private GravitinoMetalake lake;
  private DeltaRangerCallbackGateway gateway;
  private DeltaRangerCallbackGateway publicGateway;
  private boolean postLifecycleResume;
  private boolean policyCallbackFailure;
  private DeltaRangerPolicyCallbackCase policyCase;
  private DeltaRangerPolicyCallbackCase.Selection policySelection;

  /**
   * Executes one explicit public-chain checkpoint from immutable test-application inputs.
   *
   * @param args one checkpoint: bootstrap, resume-catalog-bootstrap, fresh-principal, matrix, or
   *     matrix-resume, post-role-lifecycle-resume, or policy-callback-failure
   * @throws Exception if setup or an acceptance assertion fails
   */
  public static void main(String[] args) throws Exception {
    TestDeltaRangerDownloadedPolicies harness = new TestDeltaRangerDownloadedPolicies();
    try {
      harness.postLifecycleResume =
          args.length == 1 && "post-role-lifecycle-resume".equals(args[0]);
      harness.policyCallbackFailure = args.length == 1 && "policy-callback-failure".equals(args[0]);
      harness.start();
      if (args.length != 1) {
        throw new IllegalArgumentException("One checkpoint is required");
      }
      if ("bootstrap".equals(args[0])) {
        harness.bootstrapPublishedCatalogs();
      } else if ("resume-catalog-bootstrap".equals(args[0])) {
        harness.lake = harness.client.loadMetalake(harness.required("metalake"));
        assertEquals(
            0,
            harness.lake.listCatalogs().length,
            "Resume requires authoritative empty catalog state");
        harness.bootstrapCatalogEntries();
      } else if ("fresh-principal".equals(args[0])) {
        harness.freshPrincipalMembershipUsesActualExternalUserProtocol();
      } else if ("matrix".equals(args[0])) {
        harness.bootstrapMatrixIdentities();
        harness.matrixControlLoop();
      } else if ("matrix-resume".equals(args[0])) {
        harness.resumeMatrixIdentities();
        harness.matrixControlLoop();
      } else if ("policy-callback-failure".equals(args[0])) {
        harness.policyCase =
            DeltaRangerPolicyCallbackCase.parse(harness.fixture.path("policyCallbackCase"));
        harness.lake = harness.client.loadMetalake(harness.required("metalake"));
        harness.matrixControlLoop();
      } else if ("post-role-lifecycle-resume".equals(args[0])) {
        harness.resumePostRoleLifecycle();
        harness.postLifecycleControlLoop();
      } else {
        throw new IllegalArgumentException("Unknown public-chain checkpoint");
      }
    } finally {
      harness.close();
    }
  }

  @BeforeAll
  void start() throws Exception {
    if (!"true".equals(System.getProperty(PREFIX + "enabled"))) {
      throw new IllegalStateException("Explicit production endpoint opt-in is required");
    }
    Path fixtureFile = privateFile(requiredProperty("fixtureFile"));
    fixture = JSON.readTree(fixtureFile.toFile());
    assertEquals(1, fixture.path("schemaVersion").asInt(), "Unsupported private fixture schema");
    evidence = Path.of(required("evidenceDirectory"));
    Files.createDirectories(evidence);
    adminUrl = URI.create(requiredProperty("adminUrl"));
    adminCredentials = properties(privateFile(required("rangerAdminCredentialsFile")));
    Path gatewayAddress = evidence.resolve("callback-gateway-address.txt");
    int port =
        Files.exists(gatewayAddress) ? URI.create(Files.readString(gatewayAddress)).getPort() : 0;
    if (policyCallbackFailure) {
      if (!Files.exists(gatewayAddress) || port <= 0)
        throw new IllegalArgumentException("Verified fixed callback address required");
      URI address = URI.create(Files.readString(gatewayAddress));
      if (!"http".equals(address.getScheme())
          || !"127.0.0.1".equals(address.getHost())
          || address.getUserInfo() != null
          || address.getQuery() != null
          || address.getFragment() != null
          || !address.getPath().isEmpty())
        throw new IllegalArgumentException("Exact loopback callback address required");
    }
    gateway =
        new DeltaRangerCallbackGateway(
            adminUrl,
            evidence.resolve("callback-attempts.jsonl"),
            port,
            postLifecycleResume || policyCallbackFailure);
    Files.writeString(gatewayAddress, gateway.uri().toString());
    publicGateway =
        new DeltaRangerCallbackGateway(
            URI.create(requiredProperty("gravitinoUrl")),
            evidence.resolve("public-attempts.jsonl"),
            0,
            postLifecycleResume || policyCallbackFailure);
    AdminClientBuilder builder = GravitinoAdminClient.builder(publicGateway.uri().toString());
    builder.withSimpleAuth(required("gravitinoAdminPrincipal"));
    if (postLifecycleResume) {
      // Root binds the exact executable inputs; the closed resume read set excludes /api/version.
      builder.withVersionCheckDisabled();
    }
    client = builder.build();
  }

  @AfterAll
  void close() throws Exception {
    Exception failure = null;
    for (AutoCloseable resource : new AutoCloseable[] {client, publicGateway, gateway}) {
      if (resource != null) {
        try {
          resource.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Test
  @Order(1)
  void bootstrapPublishedCatalogs() throws Exception {
    // Creation is intentionally one-shot. Existing metadata is not a reason to replay callbacks.
    client.createMetalake(required("metalake"), "Isolated production Ranger acceptance", Map.of());
    lake = client.loadMetalake(required("metalake"));
    bootstrapCatalogEntries();
  }

  private void bootstrapCatalogEntries() throws Exception {
    for (JsonNode catalog : fixture.path("catalogs")) {
      JsonNode nativeInput =
          JSON.readTree(privateFile(catalog.path("nativeFixtureFile").asText()).toFile());
      JsonNode primary = nativeInput.path("primary");
      JsonNode tableInput = nativeInput.path("table");
      Properties storage = properties(privateFile(primary.path("credentialsFile").asText()));
      Map<String, String> conf = new HashMap<>();
      conf.put(Catalog.AUTHORIZATION_PROVIDER, "ranger-delta");
      conf.put("authorization.ranger.admin.url", gateway.uri().toString());
      conf.put("authorization.ranger.auth.type", "simple");
      conf.put("authorization.ranger.username", adminCredentials.getProperty("username"));
      conf.put("authorization.ranger.password", adminCredentials.getProperty("password"));
      conf.put("authorization.ranger.service.name", required("resourceService"));
      conf.put("authorization.ranger.service.type", required("resourceDefinition"));
      conf.put("authorization.ranger.service.create-if-absent", "false");
      conf.put("authorization.ranger.delta.metalake", required("metalake"));
      conf.put("authorization.ranger.delta.catalog", catalog.path("name").asText());
      conf.put("delta.warehouse", tableInput.path("location").asText());
      conf.put(
          "delta.adoption-roots",
          JSON.writeValueAsString(List.of(tableInput.path("location").asText())));
      conf.put("delta.storage-credential-mode", "static");
      conf.put("s3-endpoint", primary.path("endpoint").asText());
      conf.put("s3-region", primary.path("region").asText());
      conf.put("s3-path-style-access", "true");
      conf.put("s3-access-key-id", storage.getProperty("MINIO_ROOT_USER"));
      conf.put("s3-secret-access-key", storage.getProperty("MINIO_ROOT_PASSWORD"));
      Catalog created =
          lake.createCatalog(
              catalog.path("name").asText(),
              Catalog.Type.RELATIONAL,
              "lakehouse-delta",
              null,
              conf);
      String database = catalog.path("database").asText();
      created.asSchemas().createSchema(database, null, Map.of());
      NameIdentifier table = NameIdentifier.of(database, catalog.path("table").asText());
      DeltaRegistrationCommand adopt =
          DeltaRegistrationCommand.adopt(
              "production-" + catalog.path("name").asText(),
              tableInput.path("location").asText(),
              "root-native-writer-completed",
              "public-chain-observational-adoption");
      Table reserved =
          created
              .asTableCatalog()
              .createTable(
                  table,
                  new Column[0],
                  null,
                  Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, adopt.encode()));
      DeltaRegistrationReceipt receipt =
          DeltaRegistrationReceipt.fromProperties(reserved.properties());
      Table published =
          created
              .asTableCatalog()
              .alterTable(
                  table,
                  TableChange.setProperty(
                      DeltaRegistrationCommand.COMMAND_PROPERTY,
                      DeltaRegistrationCommand.publish(receipt.identity()).encode()));
      assertEquals(
          DeltaRegistrationReceipt.State.PUBLISHED,
          DeltaRegistrationReceipt.fromProperties(published.properties()).state());
      event("published-" + catalog.path("name").asText(), Map.of("publicResponseCompleted", true));
    }
    assertTrue(gateway.awaitDrained());
    event("bootstrap-completed", Map.of("callbackCompletions", gateway.completed()));
  }

  @Test
  @Order(2)
  void freshPrincipalMembershipUsesActualExternalUserProtocol() throws Exception {
    lake = client.loadMetalake(required("metalake"));
    String principal = fixture.path("subjects").path("freshCallback").asText();
    assertTrue(
        users(principal).isEmpty(), "Fresh callback principal must be absent in real Ranger");
    lake.addUser(principal); // Local user creation does not dispatch the Ranger principal callback.
    assertTrue(users(principal).isEmpty(), "Gravitino addUser unexpectedly created a Ranger user");
    JsonNode catalog = fixture.path("catalogs").get(0);
    String role = required("rolesPrefix") + "protocol";
    String table =
        catalog.path("name").asText()
            + "."
            + catalog.path("database").asText()
            + "."
            + catalog.path("table").asText();
    lake.createRole(
        role,
        Map.of(),
        List.of(
            SecurableObjects.parse(
                table, MetadataObject.Type.TABLE, List.of(Privileges.SelectTable.allow()))));
    long before = gateway.completed();
    String failureClass = null;
    try {
      lake.grantRolesToUser(List.of(role), principal);
    } catch (RuntimeException failure) {
      failureClass = failure.getClass().getName();
    }
    // No replay: authoritative reads and the original completed wire response decide the outcome.
    boolean drained = gateway.awaitDrained() && publicGateway.awaitDrained();
    JsonNode users = users(principal);
    JsonNode roles =
        adminGet("/service/roles/roles?serviceName=" + encoded(required("resourceService")));
    JsonNode policies =
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService")));
    Map<String, Object> observations = new LinkedHashMap<>();
    observations.put("failureClass", failureClass);
    observations.put("localRoleNames", lake.getUser(principal).roles());
    observations.put("rangerUsers", users);
    observations.put("rangerRoles", roles);
    observations.put("rangerPolicies", policies);
    observations.put("gatewayDrained", drained);
    observations.put("callbackCompletions", gateway.completed() - before);
    event("fresh-principal-protocol", observations);
    assertTrue(drained, "Gateway handlers remain unresolved; do not replay");
    assertEquals(
        1, users.size(), "Authoritative external-user readback must find exactly one user");
    assertNull(
        failureClass,
        "Fresh public membership failed after possible persistence; evidence retained and no replay attempted");
    assertEquals(principal, users.get(0).path("name").asText());
    assertRoleMembership(
        roles, "GRAVITINO_METALAKE_" + required("expectedMetalakeId") + "_ROLE_" + role, principal);
  }

  static void assertRoleMembership(JsonNode response, String roleName, String principal) {
    JsonNode roles = response.path("roles");
    assertTrue(roles.isArray(), "Ranger role response must contain a roles array");
    List<JsonNode> matches = new ArrayList<>();
    for (JsonNode role : roles) {
      if (roleName.equals(role.path("name").asText())) {
        matches.add(role);
      }
    }
    assertEquals(1, matches.size(), "Expected exactly the canonical Ranger role");
    JsonNode role = matches.get(0);
    JsonNode users = role.path("users");
    assertTrue(users.isArray(), "Expected explicit Ranger user membership");
    assertEquals(1, users.size(), "Unexpected additional or missing Ranger members");
    assertEquals(principal, users.get(0).path("name").asText());
    assertTrue(users.get(0).path("isAdmin").isBoolean());
    assertFalse(users.get(0).path("isAdmin").asBoolean());
    for (String field : List.of("groups", "roles")) {
      JsonNode members = role.path(field);
      assertTrue(members.isMissingNode() || members.isArray());
      assertEquals(0, members.size(), "Unexpected Ranger " + field + " membership");
    }
  }

  static void assertRoleAbsent(JsonNode response, String name) {
    assertTrue(response.path("roles").isArray(), "Ranger roles array required");
    for (JsonNode role : response.path("roles")) {
      assertFalse(name.equals(role.path("name").asText()), "New role already exists in Ranger");
    }
  }

  static void assertResumeRoleIds(JsonNode original, JsonNode current, List<String> names) {
    for (String name : names) {
      JsonNode before = exactRole(original, name);
      JsonNode after = exactRole(current, name);
      assertTrue(before.path("id").isIntegralNumber());
      assertEquals(before.path("id"), after.path("id"), "Ranger role identity changed");
      for (String field : List.of("users", "groups", "roles")) {
        assertEquals(
            before.path(field), after.path(field), "Ranger membership changed before resume");
      }
    }
  }

  static void assertExactGrants(List<SecurableObject> expected, List<SecurableObject> actual) {
    assertEquals(grantSignatures(expected), grantSignatures(actual), "Unexpected local grants");
  }

  private static List<String> grantSignatures(List<SecurableObject> objects) {
    return objects.stream()
        .map(
            object ->
                object.type()
                    + ":"
                    + object.fullName()
                    + ":"
                    + object.privileges().stream()
                        .map(privilege -> privilege.name() + ":" + privilege.condition())
                        .sorted()
                        .collect(Collectors.toList()))
        .sorted()
        .collect(Collectors.toList());
  }

  private static JsonNode exactRole(JsonNode response, String name) {
    assertTrue(response.path("roles").isArray(), "Ranger roles array required");
    List<JsonNode> matches = new ArrayList<>();
    response
        .path("roles")
        .forEach(
            role -> {
              if (name.equals(role.path("name").asText())) matches.add(role);
            });
    assertEquals(1, matches.size(), "Exactly one canonical role required");
    return matches.get(0);
  }

  private List<SecurableObject> visibilityObjects() {
    List<SecurableObject> result = new ArrayList<>();
    fixture
        .path("catalogs")
        .forEach(
            catalog ->
                result.add(
                    SecurableObjects.ofCatalog(
                        catalog.path("name").asText(), List.of(Privileges.UseCatalog.allow()))));
    return result;
  }

  private List<SecurableObject> metadataObjects(boolean schema) {
    List<SecurableObject> result = new ArrayList<>();
    fixture
        .path("catalogs")
        .forEach(
            catalog ->
                result.add(
                    schema
                        ? SecurableObjects.parse(
                            catalog.path("name").asText() + "." + catalog.path("database").asText(),
                            MetadataObject.Type.SCHEMA,
                            List.of(Privileges.UseSchema.allow()))
                        : tableObject(catalog)));
    return result;
  }

  private void resumePostRoleLifecycle() throws Exception {
    String checkpointDigest = requiredProperty("resumeCheckpointSha256");
    String preflightDigest = requiredProperty("resumePreflightSha256");
    String baselineDigest = requiredProperty("resumeBaselineSha256");
    JsonNode checkpoint =
        DeltaRangerPostLifecycleResume.readBound(
            Path.of(requiredProperty("resumeCheckpointFile")), checkpointDigest);
    JsonNode preflight =
        DeltaRangerPostLifecycleResume.readBound(
            Path.of(requiredProperty("resumePreflightFile")), preflightDigest);
    JsonNode baseline =
        DeltaRangerPostLifecycleResume.readBound(
            Path.of(requiredProperty("resumeBaselineFile")), baselineDigest);
    DeltaRangerPostLifecycleResume.validateInputs(
        fixture, checkpoint, baseline, preflight, checkpointDigest, preflightDigest);
    JsonNode downloader =
        JSON.readTree(privateFile(required("resumeDownloaderCredentialsFile")).toFile());
    String downloadUser = downloader.path("username").asText();
    String downloadPassword = downloader.path("password").asText();
    if (downloadUser.isBlank()
        || downloadPassword.isBlank()
        || !downloadUser.equals(
            baseline.path("observations").path("downloader").path("body").path("name").asText())) {
      throw new IllegalArgumentException("Owned downloader credentials required");
    }
    String publicAuth = basic(required("gravitinoAdminPrincipal"), "dummy");
    String adminAuth =
        basic(adminCredentials.getProperty("username"), adminCredentials.getProperty("password"));
    String downloadAuth = basic(downloadUser, downloadPassword);
    Map<String, Object> receipts = new LinkedHashMap<>();
    for (JsonNode catalog : fixture.path("catalogs")) {
      JsonNode nativeTable =
          JSON.readTree(privateFile(catalog.path("nativeFixtureFile").asText()).toFile())
              .path("table");
      JsonNode properties =
          baseline
              .path("observations")
              .path("table:" + catalog.path("name").asText())
              .path("body")
              .path("table")
              .path("properties");
      assertEquals(
          nativeTable.path("native").path("id").asText(),
          properties.path("delta.registration.receipt.nativeId").asText());
      assertEquals(
          nativeTable.path("version").asText(),
          properties.path("delta.registration.receipt.nativeVersion").asText());
      assertEquals(
          nativeTable.path("location").asText(),
          properties.path("delta.registration.receipt.location").asText());
      receipts.put(
          catalog.path("name").asText(),
          Map.of(
              "nativeId",
              nativeTable.path("native").path("id").asText(),
              "version",
              nativeTable.path("version").asLong(),
              "location",
              nativeTable.path("location").asText()));
    }
    DeltaRangerPostLifecycleResume.validateResumeBoundary(
        fixture,
        baseline,
        (key, path) ->
            DeltaRangerPostLifecycleResume.get(
                path.startsWith("/api/") ? publicGateway.uri() : gateway.uri(),
                path,
                path.startsWith("/api/")
                    ? publicAuth
                    : (key.endsWith("Download") ? downloadAuth : adminAuth)),
        () -> lake = client.loadMetalake(required("metalake")),
        publicGateway,
        gateway,
        reads -> {
          Map<String, Object> details = new LinkedHashMap<>();
          details.put("checkpointSha256", checkpointDigest);
          details.put("baselineSha256", baselineDigest);
          details.put("preflightSha256", preflightDigest);
          details.put("preflightMode", preflight.path("mode").asText());
          details.put("coldH2Sha256", preflight.path("coldH2Sha256"));
          details.put("s3ManifestSha256", preflight.path("s3ManifestSha256").asText());
          details.put("s3ExactBytesVerified", true);
          details.put("storageBaseline", preflight.path("storageBaseline").asText());
          details.put("transitionSha256", preflight.path("transitionSha256").asText());
          details.put("receipts", receipts);
          details.put(
              "serviceIds",
              Map.of("definition", 217, "tag", 15, "resource", 16, "side", 17, "downloader", 9));
          details.put(
              "protectedRoleIds",
              Map.of(
                  "orphanOwner",
                  7,
                  "protocol",
                  13,
                  "fixedProtocol",
                  14,
                  "globalGuard",
                  23,
                  "metadataSchema",
                  25));
          details.put(
              "policyIds",
              baseline
                  .path("observations")
                  .path("policies")
                  .path("body")
                  .path("policies")
                  .findValues("id"));
          details.put("policyVersion", 44);
          details.put("roleVersion", 63);
          details.put("semanticComparisonsPassed", reads);
          details.put("completedReadResponses", reads);
          details.put(
              "distinctObservationKeys", DeltaRangerPostLifecycleResume.paths(fixture).size());
          details.put("publicCompletions", publicGateway.completed());
          details.put("callbackCompletions", gateway.completed());
          details.put("divergence", checkpoint.path("divergence"));
          event("post-role-lifecycle-resume-validated", details);
          Path validated = evidence.resolve("post-role-lifecycle-resume-validated.json");
          Files.setPosixFilePermissions(validated, PosixFilePermissions.fromString("rw-------"));
          try (FileChannel channel = FileChannel.open(validated, StandardOpenOption.WRITE)) {
            channel.force(true);
          }
        });
  }

  private void postLifecycleControlLoop() throws Exception {
    Path commands = evidence.resolve("matrix-commands");
    Files.createDirectory(commands);
    event(
        "matrix-control-ready",
        Map.of(
            "commands",
            commands.toString(),
            "mode",
            "post-role-lifecycle-resume",
            "protectedState",
            true));
    boolean quarantined = false;
    for (int sequence = 1; ; sequence++) {
      String id = String.format("%06d", sequence);
      Path commandFile = commands.resolve(id + ".json");
      long deadline = System.nanoTime() + Duration.ofMinutes(30).toNanos();
      while (!Files.exists(commandFile)) {
        if (System.nanoTime() > deadline)
          throw new IllegalStateException("Post-lifecycle barrier expired");
        Thread.sleep(100);
      }
      JsonNode command;
      try {
        command = DeltaRangerPostLifecycleResume.readCommand(commandFile);
        DeltaRangerPostLifecycleResume.validateCommand(command, quarantined);
      } catch (Exception failure) {
        event(
            "matrix-command-" + id,
            Map.of(
                "rejectedBeforeDispatch",
                true,
                "failureClass",
                failure.getClass().getName(),
                "quarantined",
                quarantined));
        continue;
      }
      String operation = command.path("operation").asText();
      if ("close".equals(operation)) {
        event("matrix-closed", Map.of("quarantined", quarantined));
        return;
      }
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("operation", operation);
      result.put("started", Instant.now().toString());
      long publicBefore = publicGateway.completed();
      long callbackBefore = gateway.completed();
      try {
        matrixOperation(operation, command);
        result.put("completed", true);
      } catch (Exception | AssertionError failure) {
        quarantined = true;
        result.put("completed", false);
        result.put("failureClass", failure.getClass().getName());
      }
      boolean drained = gateway.awaitDrained() && publicGateway.awaitDrained();
      if (!drained) quarantined = true;
      // Surface recorder loss before another command can dispatch.
      gateway.recordedEvents();
      publicGateway.recordedEvents();
      result.put("gatewayDrained", drained);
      result.put("quarantined", quarantined);
      result.put("publicCompletions", publicGateway.completed() - publicBefore);
      result.put("callbackCompletions", gateway.completed() - callbackBefore);
      result.put("finished", Instant.now().toString());
      event("matrix-command-" + id, result);
    }
  }

  private static String basic(String username, String password) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private void resumeMatrixIdentities() throws Exception {
    JsonNode checkpoint = JSON.readTree(privateFile(required("resumeCheckpointFile")).toFile());
    assertEquals("matrix-identities-ready", checkpoint.path("stage").asText());
    assertEquals(
        canonicalRole("metadata"), checkpoint.path("details").path("metadataRole").asText());
    lake = client.loadMetalake(required("metalake"));
    List<String> catalogNames = new ArrayList<>();
    for (JsonNode catalog : fixture.path("catalogs")) {
      String name = catalog.path("name").asText();
      catalogNames.add(name);
      Table table =
          lake.loadCatalog(name)
              .asTableCatalog()
              .loadTable(
                  NameIdentifier.of(
                      catalog.path("database").asText(), catalog.path("table").asText()));
      DeltaRegistrationReceipt receipt =
          DeltaRegistrationReceipt.fromProperties(table.properties());
      JsonNode nativeTable =
          JSON.readTree(privateFile(catalog.path("nativeFixtureFile").asText()).toFile())
              .path("table");
      assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, receipt.state());
      assertEquals(
          nativeTable.path("native").path("id").asText(), receipt.nativeId().orElseThrow());
      assertEquals(nativeTable.path("version").asLong(), receipt.nativeVersion().orElseThrow());
      assertEquals(nativeTable.path("location").asText(), receipt.location().orElseThrow());
    }
    assertEquals(
        catalogNames.stream().sorted().collect(Collectors.toList()),
        Arrays.stream(lake.listCatalogs()).sorted().collect(Collectors.toList()));
    List<String> canonicalNames = new ArrayList<>();
    for (String key :
        List.of("metadata", "columnReader", "visibilityOnly", "groupReader", "freshCallback")) {
      String suffix = "visibility_" + key;
      assertExactGrants(
          visibilityObjects(), lake.getRole(required("rolesPrefix") + suffix).securableObjects());
      canonicalNames.add(canonicalRole(suffix));
      List<String> expected = new ArrayList<>();
      expected.add(required("rolesPrefix") + suffix);
      if ("metadata".equals(key)) expected.add(required("rolesPrefix") + "metadata");
      if ("freshCallback".equals(key)) expected.add(required("rolesPrefix") + "protocol");
      assertEquals(
          expected.stream().sorted().collect(Collectors.toList()),
          lake.getUser(matrixPrincipal(key)).roles().stream()
              .sorted()
              .collect(Collectors.toList()));
    }
    assertExactGrants(
        metadataObjects(false),
        lake.getRole(required("rolesPrefix") + "metadata").securableObjects());
    canonicalNames.add(canonicalRole("metadata"));
    for (String suffix : List.of("ordinary", "group", "global_guard", "audit_cleanup")) {
      assertExactGrants(
          List.of(tableObject(fixture.path("catalogs").get(0))),
          lake.getRole(required("rolesPrefix") + suffix).securableObjects());
      canonicalNames.add(canonicalRole(suffix));
    }
    assertEquals(
        List.of(required("rolesPrefix") + "group"),
        lake.getGroup(fixture.path("groups").path("query").asText()).roles());
    JsonNode roles = adminGet("/service/roles/roles");
    assertResumeRoleIds(checkpoint.path("details").path("roles"), roles, canonicalNames);
    assertTrue(gateway.awaitDrained() && publicGateway.awaitDrained());
    event(
        "matrix-resume-validated",
        Map.of(
            "roles",
            roles,
            "catalogs",
            catalogNames,
            "expectedMetalakeId",
            required("expectedMetalakeId"),
            "publicCompletions",
            publicGateway.completed(),
            "callbackCompletions",
            gateway.completed()));
  }

  private void grantMetadataSchemaVisibility() throws Exception {
    String name = required("rolesPrefix") + "metadata_schema";
    assertThrows(NoSuchRoleException.class, () -> lake.getRole(name));
    assertRoleAbsent(adminGet("/service/roles/roles"), canonicalRole("metadata_schema"));
    JsonNode before =
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService")))
            .path("policies");
    assertTrue(before.isArray());
    List<String> queryKeys = List.of("columnReader", "visibilityOnly", "groupReader");
    for (String key : queryKeys)
      assertEquals(
          List.of(required("rolesPrefix") + "visibility_" + key),
          lake.getUser(matrixPrincipal(key)).roles());
    lake.createRole(name, Map.of(), metadataObjects(true));
    lake.grantRolesToUser(List.of(name), required("metadataPrincipal"));
    assertExactGrants(metadataObjects(true), lake.getRole(name).securableObjects());
    assertRoleMembership(
        adminGet("/service/roles/roles"),
        canonicalRole("metadata_schema"),
        required("metadataPrincipal"));
    JsonNode after =
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService")))
            .path("policies");
    assertEquals(before, after, "Schema visibility changed Ranger policies");
    for (String key : queryKeys)
      assertEquals(
          List.of(required("rolesPrefix") + "visibility_" + key),
          lake.getUser(matrixPrincipal(key)).roles());
    assertEquals(
        Set.of(
            required("rolesPrefix") + "visibility_metadata",
            required("rolesPrefix") + "metadata",
            name),
        Set.copyOf(lake.getUser(required("metadataPrincipal")).roles()));
    verifyMetadataReadAdmission();
    event(
        "metadata-schema-granted",
        Map.of(
            "role",
            name,
            "policiesBefore",
            before,
            "policiesAfter",
            after,
            "metadataRoles",
            lake.getUser(required("metadataPrincipal")).roles()));
  }

  private void verifyMetadataReadAdmission() throws Exception {
    for (String key : List.of("metadata", "columnReader", "visibilityOnly", "groupReader")) {
      try (GravitinoAdminClient reader =
          GravitinoAdminClient.builder(publicGateway.uri().toString())
              .withSimpleAuth(matrixPrincipal(key))
              .build()) {
        GravitinoMetalake readerLake = reader.loadMetalake(required("metalake"));
        for (JsonNode catalog : fixture.path("catalogs")) {
          Catalog loaded = readerLake.loadCatalog(catalog.path("name").asText());
          NameIdentifier table =
              NameIdentifier.of(catalog.path("database").asText(), catalog.path("table").asText());
          if ("metadata".equals(key)) {
            loaded.asSchemas().loadSchema(catalog.path("database").asText());
            loaded.asTableCatalog().loadTable(table);
          } else {
            assertThrows(ForbiddenException.class, () -> loaded.asTableCatalog().loadTable(table));
          }
        }
      }
    }
    event("metadata-read-admission", Map.of("metadataTables", 3, "queryPrincipalTableDenials", 9));
  }

  private void bootstrapMatrixIdentities() throws Exception {
    lake = client.loadMetalake(required("metalake"));
    List<SecurableObject> visibility = new ArrayList<>();
    List<SecurableObject> metadata = new ArrayList<>();
    for (JsonNode catalog : fixture.path("catalogs")) {
      visibility.add(
          SecurableObjects.ofCatalog(
              catalog.path("name").asText(), List.of(Privileges.UseCatalog.allow())));
      metadata.add(tableObject(catalog));
    }
    for (String key :
        List.of("metadata", "columnReader", "visibilityOnly", "groupReader", "freshCallback")) {
      String principal = matrixPrincipal(key);
      if (!"freshCallback".equals(key)) {
        lake.addUser(principal);
      }
      String role = required("rolesPrefix") + "visibility_" + key;
      lake.createRole(role, Map.of(), visibility);
      lake.grantRolesToUser(List.of(role), principal);
    }
    String metadataRole = required("rolesPrefix") + "metadata";
    lake.createRole(metadataRole, Map.of(), metadata);
    lake.grantRolesToUser(List.of(metadataRole), required("metadataPrincipal"));
    lake.addGroup(fixture.path("groups").path("query").asText());
    SecurableObject table = tableObject(fixture.path("catalogs").get(0));
    for (String suffix : List.of("ordinary", "group", "global_guard", "audit_cleanup")) {
      lake.createRole(required("rolesPrefix") + suffix, Map.of(), List.of(table));
    }
    lake.grantRolesToGroup(
        List.of(required("rolesPrefix") + "group"), fixture.path("groups").path("query").asText());
    assertTrue(gateway.awaitDrained() && publicGateway.awaitDrained());
    JsonNode roles = adminGet("/service/roles/roles");
    assertRoleMembership(roles, canonicalRole("metadata"), required("metadataPrincipal"));
    Map<String, Object> local = new LinkedHashMap<>();
    for (String key : List.of("columnReader", "visibilityOnly", "groupReader")) {
      List<String> memberships = lake.getUser(matrixPrincipal(key)).roles();
      assertEquals(List.of(required("rolesPrefix") + "visibility_" + key), memberships);
      local.put(key, memberships);
    }
    Map<String, Object> ready = new LinkedHashMap<>();
    ready.put("roles", roles);
    ready.put(
        "policies",
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService"))));
    ready.put("localQueryMemberships", local);
    ready.put("metadataRole", canonicalRole("metadata"));
    ready.put("globalGuardRole", canonicalRole("global_guard"));
    ready.put("auditCleanupRole", canonicalRole("audit_cleanup"));
    ready.put("callbackCompletions", gateway.completed());
    ready.put("publicCompletions", publicGateway.completed());
    event("matrix-identities-ready", ready);
  }

  private void matrixControlLoop() throws Exception {
    Path commands = evidence.resolve("matrix-commands");
    Files.createDirectory(commands);
    event("matrix-control-ready", Map.of("commands", commands.toString()));
    boolean quarantined = false;
    for (int sequence = 1; ; sequence++) {
      String id = String.format("%06d", sequence);
      Path commandFile = commands.resolve(id + ".json");
      long deadline = System.nanoTime() + Duration.ofMinutes(30).toNanos();
      while (!Files.exists(commandFile)) {
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException("Matrix barrier expired without a command");
        }
        Thread.sleep(100);
      }
      JsonNode command = JSON.readTree(privateFile(commandFile.toString()).toFile());
      String operation = command.path("operation").asText();
      if (policyCallbackFailure) policyCase.begin(command);
      if ("close".equals(operation)) {
        if (policyCallbackFailure) policyCase.finish(true);
        event("matrix-closed", Map.of("quarantined", quarantined));
        return;
      }
      assertTrue(
          !quarantined || "snapshot".equals(operation),
          "Failed public mutation requires independent reconciliation; no replay");
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("operation", operation);
      result.put("started", Instant.now().toString());
      long publicBefore = publicGateway.completed();
      long callbackBefore = gateway.completed();
      int callbackOffset = policyCallbackFailure ? gateway.recordedEvents().size() : 0;
      int publicOffset = policyCallbackFailure ? publicGateway.recordedEvents().size() : 0;
      if (policyCallbackFailure) {
        result.put("command", command);
        event("policy-command-intent-" + id, Map.of("command", command));
      }
      try {
        matrixOperation(operation, command);
        result.put("completed", true);
      } catch (Exception | AssertionError failure) {
        if (!policyCallbackFailure && failure instanceof AssertionError)
          throw (AssertionError) failure;
        quarantined = true;
        result.put("completed", false);
        result.put("failureClass", failure.getClass().getName());
      }
      boolean drained = gateway.awaitDrained() && publicGateway.awaitDrained();
      result.put("gatewayDrained", drained);
      if (policyCallbackFailure) {
        List<JsonNode> callbacks = policyCaseEvents(gateway, callbackOffset);
        List<JsonNode> publicEvents = policyCaseEvents(publicGateway, publicOffset);
        result.put("callbackEvents", callbacks);
        result.put("publicEvents", publicEvents);
        if (operation.equals(policyCase.mutation())) {
          try {
            if (!drained || policySelection == null)
              throw new IllegalStateException("Mutation completion unresolved");
            DeltaRangerPolicyCallbackCase.verifyMutationEvidence(
                callbacks,
                publicEvents,
                policySelection.method,
                policySelection.path,
                "/api/metalakes/"
                    + required("metalake")
                    + "/permissions/roles/"
                    + policyCase.role
                    + "/table/"
                    + DeltaRangerPolicyCallbackCase.fullName(policyCase.target())
                    + ("grant-select".equals(operation) ? "/grant" : "/revoke"));
            result.put("mutationEvidenceQualified", true);
          } catch (Exception failure) {
            result.put("mutationEvidenceQualified", false);
            result.put("evidenceFailureClass", failure.getClass().getName());
          }
        }
        policyCase.finish(Boolean.TRUE.equals(result.get("completed")) && drained);
        quarantined = policyCase.quarantined();
        result.put("quarantined", quarantined);
      }
      result.put("publicCompletions", publicGateway.completed() - publicBefore);
      result.put("callbackCompletions", gateway.completed() - callbackBefore);
      result.put("finished", Instant.now().toString());
      event("matrix-command-" + id, result);
    }
  }

  private void matrixOperation(String operation, JsonNode command) throws Exception {
    if (policyCallbackFailure) {
      policyCaseOperation(operation, command);
      return;
    }
    if ("snapshot".equals(operation)) {
      Map<String, Object> snapshot = new LinkedHashMap<>();
      snapshot.put("roles", adminGet("/service/roles/roles"));
      snapshot.put(
          "policies",
          adminGet(
              "/service/plugins/policies?serviceName=" + encoded(required("resourceService"))));
      JsonNode service =
          adminGet("/service/public/v2/api/service/name/" + encoded(required("resourceService")));
      snapshot.put("serviceId", service.path("id"));
      snapshot.put("policyVersion", service.path("policyVersion"));
      snapshot.put("roleVersion", service.path("roleVersion"));
      Map<String, Object> local = new LinkedHashMap<>();
      for (String key : List.of("columnReader", "visibilityOnly", "groupReader", "freshCallback")) {
        local.put(key, lake.getUser(matrixPrincipal(key)).roles());
      }
      snapshot.put("localMemberships", local);
      event("matrix-snapshot-" + command.path("id").asText(), snapshot);
      return;
    }
    if ("metadata-schema".equals(operation)) {
      grantMetadataSchemaVisibility();
      return;
    }
    String suffix = command.path("roleSuffix").asText("ordinary");
    assertTrue(
        Set.of("ordinary", "group", "global_guard", "audit_cleanup", "visibility_visibilityOnly")
            .contains(suffix));
    String role = required("rolesPrefix") + suffix;
    String subject = command.path("subject").asText("freshCallback");
    switch (operation) {
      case "grant-user":
        lake.grantRolesToUser(List.of(role), matrixPrincipal(subject));
        break;
      case "revoke-user":
        lake.revokeRolesFromUser(List.of(role), matrixPrincipal(subject));
        break;
      case "grant-group":
        lake.grantRolesToGroup(List.of(role), fixture.path("groups").path("query").asText());
        break;
      case "revoke-group":
        lake.revokeRolesFromGroup(List.of(role), fixture.path("groups").path("query").asText());
        break;
      case "grant-select":
        lake.grantPrivilegesToRole(
            role,
            tableObject(fixture.path("catalogs").get(0)),
            Set.of(Privileges.SelectTable.allow()));
        break;
      case "revoke-select":
        lake.revokePrivilegesFromRole(
            role,
            tableObject(fixture.path("catalogs").get(0)),
            Set.of(Privileges.SelectTable.allow()));
        break;
      case "delete-role":
        assertTrue(lake.deleteRole(role));
        break;
      case "reconcile-owner":
        lake.setOwner(
            tableObject(fixture.path("catalogs").get(0)),
            required("gravitinoAdminPrincipal"),
            Owner.Type.USER);
        break;
      case "fault-role-update":
        JsonNode rangerRole =
            adminGet("/service/public/v2/api/roles/name/" + encoded(canonicalRole(suffix)));
        gateway.failNext(
            "PUT", "/service/public/v2/api/roles/" + rangerRole.path("id").asLong(), 503);
        break;
      default:
        throw new IllegalArgumentException("Unknown matrix operation");
    }
  }

  private static List<JsonNode> policyCaseEvents(DeltaRangerCallbackGateway recorder, int offset)
      throws Exception {
    List<String> lines = recorder.recordedEvents();
    if (offset > lines.size()) throw new IllegalStateException("Recorder history truncated");
    List<JsonNode> result = new ArrayList<>();
    for (String line : lines.subList(offset, lines.size())) result.add(JSON.readTree(line));
    return result;
  }

  private void policyCaseOperation(String operation, JsonNode command) throws Exception {
    switch (operation) {
      case "prepare-policy-case":
        preparePolicyCase();
        break;
      case "fault-policy-write":
        Map<String, Object> before = policyCaseSnapshot();
        policySelection = selectPolicyCase(before);
        event(
            "policy-case-arm",
            Map.of(
                "command",
                command,
                "method",
                policySelection.method,
                "path",
                policySelection.path,
                "snapshot",
                before,
                "simulatedPolicy",
                policySelection.after));
        gateway.failNext(policySelection.method, policySelection.path, 503);
        break;
      case "grant-select":
      case "revoke-select":
        if (policySelection == null)
          throw new IllegalStateException("Verified policy fault required");
        event(
            "policy-case-mutation-intent",
            Map.of(
                "command",
                command,
                "role",
                policyCase.role,
                "table",
                DeltaRangerPolicyCallbackCase.fullName(policyCase.target()),
                "privileges",
                List.of("SELECT_TABLE:ALLOW"),
                "callbackMethod",
                policySelection.method,
                "callbackPath",
                policySelection.path));
        if ("grant-select".equals(operation)) {
          lake.grantPrivilegesToRole(
              policyCase.role,
              tableObject(policyCase.target()),
              Set.of(Privileges.SelectTable.allow()));
        } else {
          lake.revokePrivilegesFromRole(
              policyCase.role,
              tableObject(policyCase.target()),
              Set.of(Privileges.SelectTable.allow()));
        }
        break;
      case "snapshot":
        event("matrix-snapshot-" + command.path("id").asText(), policyCaseSnapshot());
        break;
      default:
        throw new IllegalArgumentException("Unknown policy-case operation");
    }
  }

  private void preparePolicyCase() throws Exception {
    for (JsonNode table : List.of(policyCase.tableA, policyCase.tableB)) {
      long matches = 0;
      for (JsonNode catalog : fixture.path("catalogs")) {
        if (DeltaRangerPolicyCallbackCase.fullName(table)
            .equals(DeltaRangerPolicyCallbackCase.fullName(catalog))) matches++;
      }
      assertEquals(1, matches, "Exact registered fixture table required");
      lake.loadCatalog(table.path("name").asText())
          .asTableCatalog()
          .loadTable(
              NameIdentifier.of(table.path("database").asText(), table.path("table").asText()));
    }
    assertThrows(NoSuchRoleException.class, () -> lake.getRole(policyCase.role));
    assertThrows(NoSuchRoleException.class, () -> lake.getRole(policyCase.visibilityRole));
    assertThrows(NoSuchUserException.class, () -> lake.getUser(policyCase.subject));
    JsonNode rolesBefore = completeAdminRoles();
    assertRoleAbsent(rolesBefore, policyCaseCanonicalRole(policyCase.role));
    assertRoleAbsent(rolesBefore, policyCaseCanonicalRole(policyCase.visibilityRole));
    JsonNode policiesBefore =
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService")));
    event(
        "policy-case-before-setup",
        Map.of(
            "roles",
            rolesBefore,
            "policies",
            policiesBefore,
            "fixture",
            fixture.path("policyCallbackCase")));
    event("policy-case-add-user-intent", Map.of("subject", policyCase.subject));
    lake.addUser(policyCase.subject);
    List<SecurableObject> visibility = new ArrayList<>();
    Set<String> catalogs = new HashSet<>();
    Set<String> schemas = new HashSet<>();
    for (JsonNode table : List.of(policyCase.tableA, policyCase.tableB)) {
      String catalog = table.path("name").asText();
      String schema = catalog + "." + table.path("database").asText();
      if (catalogs.add(catalog))
        visibility.add(SecurableObjects.ofCatalog(catalog, List.of(Privileges.UseCatalog.allow())));
      if (schemas.add(schema))
        visibility.add(
            SecurableObjects.parse(
                schema, MetadataObject.Type.SCHEMA, List.of(Privileges.UseSchema.allow())));
    }
    event(
        "policy-case-create-visibility-intent",
        Map.of("role", policyCase.visibilityRole, "grants", grantSignatures(visibility)));
    lake.createRole(policyCase.visibilityRole, Map.of(), visibility);
    event(
        "policy-case-visibility-member-intent",
        Map.of("role", policyCase.visibilityRole, "subject", policyCase.subject));
    lake.grantRolesToUser(List.of(policyCase.visibilityRole), policyCase.subject);
    JsonNode afterVisibility =
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService")));
    assertEquals(
        policiesBefore.path("policies"),
        afterVisibility.path("policies"),
        "Local-only visibility changed Ranger policy items");
    List<SecurableObject> initial =
        policyCase.kind == DeltaRangerPolicyCallbackCase.Kind.FIRST_GRANT
            ? List.of()
            : List.of(tableObject(policyCase.tableA));
    event(
        "policy-case-create-role-intent",
        Map.of("role", policyCase.role, "grants", grantSignatures(initial)));
    lake.createRole(policyCase.role, Map.of(), initial);
    event(
        "policy-case-member-intent",
        Map.of("role", policyCase.role, "subject", policyCase.subject));
    lake.grantRolesToUser(List.of(policyCase.role), policyCase.subject);
    assertTrue(gateway.awaitDrained() && publicGateway.awaitDrained());
    Map<String, Object> snapshot = policyCaseSnapshot();
    DeltaRangerPolicyCallbackCase.Selection selection = selectPolicyCase(snapshot);
    event(
        "policy-case-prepared",
        Map.of(
            "snapshot",
            snapshot,
            "method",
            selection.method,
            "path",
            selection.path,
            "simulatedPolicy",
            selection.after));
  }

  private String policyCaseCanonicalRole(String name) {
    return "GRAVITINO_METALAKE_" + required("expectedMetalakeId") + "_ROLE_" + name;
  }

  private JsonNode completeAdminRoles() throws Exception {
    JsonNode roles = adminGet("/service/roles/roles");
    assertTrue(roles.path("roles").isArray(), "Complete Admin role response required");
    assertTrue(roles.path("totalCount").isIntegralNumber(), "Role total count required");
    assertEquals(
        roles.path("totalCount").longValue(), roles.path("roles").size(), "Truncated Admin roles");
    return roles;
  }

  private Map<String, Object> policyCaseSnapshot() throws Exception {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("roles", completeAdminRoles());
    snapshot.put(
        "policies",
        adminGet("/service/plugins/policies?serviceName=" + encoded(required("resourceService"))));
    snapshot.put(
        "service",
        adminGet("/service/public/v2/api/service/name/" + encoded(required("resourceService"))));
    snapshot.put("role", policyCase.role);
    snapshot.put("canonicalRole", policyCaseCanonicalRole(policyCase.role));
    snapshot.put("subject", policyCase.subject);
    try {
      snapshot.put(
          "localGrants", grantSignatures(lake.getRole(policyCase.role).securableObjects()));
    } catch (NoSuchRoleException absent) {
      snapshot.put("localRoleAbsent", true);
    }
    try {
      snapshot.put("localMemberships", lake.getUser(policyCase.subject).roles());
    } catch (NoSuchUserException absent) {
      snapshot.put("localUserAbsent", true);
    }
    snapshot.put("callbackCompletions", gateway.completed());
    snapshot.put("publicCompletions", publicGateway.completed());
    snapshot.put("quarantined", policyCase.quarantined());
    return snapshot;
  }

  private DeltaRangerPolicyCallbackCase.Selection selectPolicyCase(Map<String, Object> snapshot) {
    return policyCase.select(
        required("resourceService"),
        required("expectedMetalakeId"),
        policyCaseCanonicalRole(policyCase.role),
        lake.getRole(policyCase.role).securableObjects(),
        lake.getUser(policyCase.subject).roles(),
        ((JsonNode) snapshot.get("policies")).path("policies"),
        ((JsonNode) snapshot.get("roles")).path("roles"));
  }

  private String matrixPrincipal(String key) {
    if ("metadata".equals(key)) {
      return required("metadataPrincipal");
    }
    assertTrue(
        Set.of("columnReader", "visibilityOnly", "groupReader", "freshCallback").contains(key));
    String principal = fixture.path("subjects").path(key).asText();
    assertFalse(principal.isBlank());
    return principal;
  }

  private String canonicalRole(String suffix) {
    return "GRAVITINO_METALAKE_"
        + required("expectedMetalakeId")
        + "_ROLE_"
        + required("rolesPrefix")
        + suffix;
  }

  private SecurableObject tableObject(JsonNode catalog) {
    return SecurableObjects.parse(
        catalog.path("name").asText()
            + "."
            + catalog.path("database").asText()
            + "."
            + catalog.path("table").asText(),
        MetadataObject.Type.TABLE,
        List.of(Privileges.SelectTable.allow()));
  }

  private JsonNode users(String principal) throws Exception {
    return adminGet("/service/xusers/users?name=" + encoded(principal)).path("vXUsers");
  }

  private JsonNode adminGet(String path) throws Exception {
    String token =
        Base64.getEncoder()
            .encodeToString(
                (adminCredentials.getProperty("username")
                        + ":"
                        + adminCredentials.getProperty("password"))
                    .getBytes(StandardCharsets.UTF_8));
    HttpResponse<byte[]> response =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .build()
            .send(
                HttpRequest.newBuilder(adminUrl.resolve(path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Authorization", "Basic " + token)
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray());
    assertEquals(
        200, response.statusCode(), "Independent Admin readback failed during setup/observation");
    return JSON.readTree(response.body());
  }

  private void event(String stage, Map<String, ?> details) throws Exception {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("stage", stage);
    record.put("time", Instant.now().toString());
    record.put("details", details);
    if (postLifecycleResume) {
      record.put("mode", "post-role-lifecycle-resume");
      record.put("preservedDivergence", true);
      record.put(
          "protectedObjects",
          List.of(
              "global_guard",
              "audit_cleanup",
              "metadata_schema",
              "protocol",
              "originalCatalogOwner"));
    }
    Path output = evidence.resolve(stage + ".json");
    Files.writeString(output, JSON.writeValueAsString(record), StandardOpenOption.CREATE_NEW);
  }

  private String required(String field) {
    String value = fixture.path(field).asText();
    if (value.isBlank()) {
      throw new IllegalArgumentException("Missing private fixture field: " + field);
    }
    return value;
  }

  private static String requiredProperty(String field) {
    String value = System.getProperty(PREFIX + field);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing explicit production property: " + field);
    }
    return value;
  }

  private static Properties properties(Path path) throws Exception {
    Properties result = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      result.load(input);
    }
    return result;
  }

  private static Path privateFile(String value) throws Exception {
    Path path = Path.of(value);
    if (!path.isAbsolute() || !Files.isRegularFile(path)) {
      throw new IllegalArgumentException("An absolute regular private input file is required");
    }
    Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path);
    if (permissions.stream()
        .anyMatch(
            permission ->
                permission.name().startsWith("GROUP_")
                    || permission.name().startsWith("OTHERS_"))) {
      throw new IllegalArgumentException("Private input files require owner-only permissions");
    }
    return path;
  }

  private static String encoded(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
