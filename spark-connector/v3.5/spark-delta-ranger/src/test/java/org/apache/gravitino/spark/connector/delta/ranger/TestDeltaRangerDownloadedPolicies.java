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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
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

  /**
   * Executes one explicit public-chain checkpoint from immutable test-application inputs.
   *
   * @param args one checkpoint: bootstrap or fresh-principal
   * @throws Exception if setup or an acceptance assertion fails
   */
  public static void main(String[] args) throws Exception {
    TestDeltaRangerDownloadedPolicies harness = new TestDeltaRangerDownloadedPolicies();
    try {
      harness.start();
      if (args.length != 1) {
        throw new IllegalArgumentException("One checkpoint is required");
      }
      if ("bootstrap".equals(args[0])) {
        harness.bootstrapPublishedCatalogs();
      } else if ("fresh-principal".equals(args[0])) {
        harness.freshPrincipalMembershipUsesActualExternalUserProtocol();
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
    gateway =
        new DeltaRangerCallbackGateway(adminUrl, evidence.resolve("callback-attempts.jsonl"), port);
    Files.writeString(gatewayAddress, gateway.uri().toString());
    publicGateway =
        new DeltaRangerCallbackGateway(
            URI.create(requiredProperty("gravitinoUrl")),
            evidence.resolve("public-attempts.jsonl"));
    client =
        GravitinoAdminClient.builder(publicGateway.uri().toString())
            .withSimpleAuth(required("gravitinoAdminPrincipal"))
            .build();
  }

  @AfterAll
  void close() throws Exception {
    if (client != null) {
      client.close();
    }
    if (publicGateway != null) {
      publicGateway.close();
    }
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  @Order(1)
  void bootstrapPublishedCatalogs() throws Exception {
    // Creation is intentionally one-shot. Existing metadata is not a reason to replay callbacks.
    client.createMetalake(required("metalake"), "Isolated production Ranger acceptance", Map.of());
    lake = client.loadMetalake(required("metalake"));
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
    assertFalse(roles.isEmpty(), "Real Ranger role readback is required");
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
