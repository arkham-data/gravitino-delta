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
package org.apache.gravitino.authorization.ranger.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/** Real public REST dispatch and production Ranger client; only Admin is an HTTP fixture. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestRangerDeltaProductionCallbacks {
  private static final String LAKE = "lake";
  private static final String ADMIN = "delta-admin";
  private static final String CATALOG = "delta_a";
  @TempDir static Path directory;
  private MiniGravitino server;
  private AdminEndpoint endpoint;
  private GravitinoAdminClient client;
  private GravitinoMetalake lake;
  private JsonNode nativeFixture;
  private final Map<String, String> nativeProperties = new HashMap<>();
  private Object previousConfig;
  private Object previousMetrics;

  @BeforeAll
  void start() throws Exception {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousMetrics = FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
    endpoint = new AdminEndpoint();
    String nativeInput = System.getProperty("delta.ranger.callback.fixture");
    if (nativeInput != null) {
      nativeFixture = new ObjectMapper().readTree(Path.of(nativeInput).toFile());
      Properties credentials = new Properties();
      try (InputStream input =
          Files.newInputStream(
              Path.of(nativeFixture.get("primary").get("credentialsFile").asText()))) {
        credentials.load(input);
      }
      nativeProperties.put("delta.warehouse", nativeFixture.get("warehouse").asText());
      nativeProperties.put(
          "delta.adoption-roots",
          new ObjectMapper().writeValueAsString(List.of(nativeFixture.get("location").asText())));
      nativeProperties.put("s3-endpoint", nativeFixture.get("primary").get("endpoint").asText());
      nativeProperties.put("s3-region", nativeFixture.get("primary").get("region").asText());
      nativeProperties.put("s3-path-style-access", "true");
      nativeProperties.put("s3-access-key-id", credentials.getProperty("MINIO_ROOT_USER"));
      nativeProperties.put("s3-secret-access-key", credentials.getProperty("MINIO_ROOT_PASSWORD"));
    }
    Map<String, String> config = new HashMap<>();
    config.put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE);
    config.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
        directory.resolve("store").toString());
    config.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
        "jdbc:h2:file:"
            + directory.resolve("store")
            + ";DB_CLOSE_DELAY=0;MODE=MYSQL;AUTO_SERVER=FALSE");
    config.put(Configs.CACHE_ENABLED.getKey(), "false");
    config.put(Configs.CATALOG_LOAD_ISOLATED.getKey(), "false");
    config.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    config.put(Configs.AUTHENTICATORS.getKey(), "simple");
    config.put(Configs.CATALOG_CREDENTIAL_BACKFILL_TO_PROPERTIES.getKey(), "true");
    config.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    config.put("gravitino.auxService.names", "");
    config.put("SimpleAuthUserName", ADMIN);
    server = new MiniGravitino(new MiniGravitinoContext(config, true, true));
    server.start();
    JettyServerConfig jetty =
        JettyServerConfig.fromConfig(
            server.getServerConfig(), GravitinoServer.WEBSERVER_CONF_PREFIX);
    client =
        GravitinoAdminClient.builder("http://" + jetty.getHost() + ":" + jetty.getHttpPort())
            .withSimpleAuth(ADMIN)
            .build();
    client.createMetalake(LAKE, null, Map.of());
    lake = client.loadMetalake(LAKE);
    lake.addUser("alice");
    lake.addUser("bob");
    lake.addGroup("team");
    createCatalog(CATALOG);
    lake.loadCatalog(CATALOG).asSchemas().createSchema("sales", null, Map.of());
    if (nativeFixture == null) {
      reserve("orders");
    } else {
      adoptPublished("orders");
    }
  }

  @AfterEach
  void clearFailures() {
    endpoint.failPolicy = false;
    endpoint.rejectRoleDeletion = false;
  }

  @Test
  void catalogVisibilityHasNoRangerAccessContribution() {
    int before = endpoint.policies.size();
    lake.createRole(
        "visible",
        Map.of(),
        List.of(SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()))));
    lake.grantRolesToUser(List.of("visible"), "alice");
    assertEquals(before, endpoint.policies.size());
    assertFalse(endpoint.hasGrant(endpoint.roleEnding("visible").getName()));
    lake.grantRolesToGroup(List.of("visible"), "team");
    assertEquals(
        List.of("team"),
        endpoint.roleEnding("visible").getGroups().stream()
            .map(RangerRole.RoleMember::getName)
            .collect(Collectors.toList()));
    lake.revokeRolesFromUser(List.of("visible"), "alice");
    lake.revokeRolesFromGroup(List.of("visible"), "team");
    assertTrue(endpoint.roleEnding("visible").getUsers().isEmpty());
    assertTrue(endpoint.roleEnding("visible").getGroups().isEmpty());
    assertEquals(before, endpoint.policies.size());
    lake.deleteRole("visible");
    assertTrue(endpoint.roles.values().stream().noneMatch(r -> r.getName().endsWith("_visible")));
  }

  @Test
  void globalAdminRoleDeleteRejectionPreservesLocalRoleAndMembership() {
    lake.createRole(
        "delete_rejected",
        Map.of(),
        List.of(SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()))));
    lake.grantRolesToUser(List.of("delete_rejected"), "alice");
    RangerRole before = endpoint.roleEnding("delete_rejected");
    endpoint.rejectRoleDeletion = true;
    long attempts = endpoint.roleDeleteAttempts.get();
    assertThrows(RuntimeException.class, () -> lake.deleteRole("delete_rejected"));
    assertTrue(endpoint.roleDeleteAttempts.get() > attempts);
    assertEquals(
        "delete_rejected",
        lake.getRole("delete_rejected").name(),
        "RoleManager dispatches callbacks before deleting the local role");
    assertEquals(before, endpoint.roleEnding("delete_rejected"));
  }

  @AfterAll
  void stop() throws Exception {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
    if (endpoint != null) {
      endpoint.close();
    }
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "metricsSystem", previousMetrics, true);
  }

  @Test
  @Tag("gravitino-docker-test")
  @EnabledIfSystemProperty(named = "delta.ranger.callback.fixture", matches = ".+")
  void ordinaryPublicRoleMembershipGrantAndLastRevokeFailuresRemainDivergent() {
    MetadataObject table =
        MetadataObjects.parse(CATALOG + ".sales.orders", MetadataObject.Type.TABLE);
    lake.createRole(
        "reader",
        Map.of(),
        List.of(
            SecurableObjects.parse(
                table.fullName(), table.type(), List.of(Privileges.SelectTable.allow()))));
    lake.grantRolesToUser(List.of("reader"), "alice");
    lake.grantRolesToGroup(List.of("reader"), "team");
    RangerRole role = endpoint.roleEnding("reader");
    assertEquals(
        List.of("alice"),
        role.getUsers().stream().map(RangerRole.RoleMember::getName).collect(Collectors.toList()));
    assertEquals(
        List.of("team"),
        role.getGroups().stream().map(RangerRole.RoleMember::getName).collect(Collectors.toList()));
    endpoint.failPolicy = true;
    assertThrows(
        RuntimeException.class,
        () ->
            lake.revokePrivilegesFromRole("reader", table, Set.of(Privileges.SelectTable.allow())));
    assertTrue(lake.getRole("reader").securableObjects().isEmpty());
    long attempts = endpoint.policyWrites.get();
    lake.revokePrivilegesFromRole("reader", table, Set.of(Privileges.SelectTable.allow()));
    assertEquals(attempts, endpoint.policyWrites.get());
    assertTrue(endpoint.hasGrant(role.getName()));
    endpoint.failPolicy = false;
    lake.setOwner(table, ADMIN, Owner.Type.USER);
    assertTrue(
        endpoint.hasGrant(role.getName()),
        "same-owner repair must not pretend to repair ordinary divergence");

    lake.createRole("late", Map.of(), List.of());
    endpoint.failPolicy = true;
    assertThrows(
        RuntimeException.class,
        () -> lake.grantPrivilegesToRole("late", table, Set.of(Privileges.SelectTable.allow())));
    assertFalse(lake.getRole("late").securableObjects().isEmpty());
    attempts = endpoint.policyWrites.get();
    lake.grantPrivilegesToRole("late", table, Set.of(Privileges.SelectTable.allow()));
    assertEquals(attempts, endpoint.policyWrites.get());
    assertFalse(endpoint.hasGrant(endpoint.roleEnding("late").getName()));
    endpoint.failPolicy = false;
  }

  @Test
  @Tag("gravitino-docker-test")
  @EnabledIfSystemProperty(named = "delta.ranger.callback.fixture", matches = ".+")
  void emptyRoleMembershipRequiresQualifiedOrdinaryReplay() {
    MetadataObject table =
        MetadataObjects.parse(CATALOG + ".sales.orders", MetadataObject.Type.TABLE);
    lake.createRole("empty", Map.of(), List.of());
    lake.grantRolesToUser(List.of("empty"), "bob");
    assertTrue(endpoint.roles.values().stream().noneMatch(r -> r.getName().endsWith("empty")));
    lake.grantPrivilegesToRole("empty", table, Set.of(Privileges.SelectTable.allow()));
    RangerRole role = endpoint.roleEnding("empty");
    assertTrue(role.getUsers().isEmpty(), "first grant is not existing membership convergence");
    lake.grantRolesToUser(List.of("empty"), "bob");
    assertEquals(
        List.of("bob"),
        endpoint.roleEnding("empty").getUsers().stream()
            .map(RangerRole.RoleMember::getName)
            .collect(Collectors.toList()));
  }

  @Test
  void singleOwnerPutPersistsBeforeFailureAndSameOwnerRepairsStalePrincipal() {
    MetadataObject table = MetadataObjects.parse(CATALOG + ".sales", MetadataObject.Type.SCHEMA);
    lake.setOwner(table, "alice", Owner.Type.USER);
    endpoint.failPolicy = true;
    assertThrows(RuntimeException.class, () -> lake.setOwner(table, "bob", Owner.Type.USER));
    assertEquals("bob", lake.getOwner(table).orElseThrow().name());
    assertTrue(endpoint.ownerUsers(table).contains("alice"));
    assertThrows(RuntimeException.class, () -> lake.setOwner(table, "bob", Owner.Type.USER));
    endpoint.failPolicy = false;
    lake.setOwner(table, lake.getOwner(table).orElseThrow().name(), Owner.Type.USER);
    assertEquals(List.of("bob"), endpoint.ownerUsers(table));
  }

  @Test
  void schemaBatchFailureReturnsSuccessWhileTablePostRetainsPendingRegistration() throws Exception {
    endpoint.failPolicy = true;
    lake.loadCatalog(CATALOG).asSchemas().createSchema("batch", null, Map.of());
    MetadataObject schema = MetadataObjects.parse(CATALOG + ".batch", MetadataObject.Type.SCHEMA);
    assertEquals(ADMIN, lake.getOwner(schema).orElseThrow().name());
    assertTrue(endpoint.ownerUsers(schema).isEmpty());
    assertThrows(RuntimeException.class, () -> reserve("failed_owner"));
    MetadataObject table =
        MetadataObjects.parse(CATALOG + ".sales.failed_owner", MetadataObject.Type.TABLE);
    assertThrows(RuntimeException.class, () -> lake.getOwner(table));
    assertEquals(
        ADMIN,
        GravitinoEnv.getInstance()
            .internalOwnerDispatcher()
            .getOwner(LAKE, table)
            .orElseThrow()
            .name());
    TableEntity row =
        GravitinoEnv.getInstance()
            .entityStore()
            .get(
                NameIdentifier.of(LAKE, CATALOG, "sales", "failed_owner"),
                Entity.EntityType.TABLE,
                TableEntity.class);
    assertNotNull(row);
    assertTrue(row.properties().values().stream().anyMatch(v -> v.contains("RESERVED")));
    endpoint.failPolicy = false;
    lake.setOwner(schema, lake.getOwner(schema).orElseThrow().name(), Owner.Type.USER);
    reserve("failed_owner");
    assertEquals(List.of(ADMIN), endpoint.ownerUsers(schema));
    assertEquals(List.of(ADMIN), endpoint.ownerUsers(table));
  }

  @Test
  void catalogPostFailureLeavesIndependentRangerEffects() {
    endpoint.failPolicy = true;
    int roles = endpoint.roles.size();
    assertThrows(RuntimeException.class, () -> createCatalog("failed_catalog"));
    assertTrue(endpoint.roles.size() > roles, "catalog rollback cannot roll back Ranger roles");
    endpoint.failPolicy = false;
  }

  private void createCatalog(String name) {
    Map<String, String> conf = new HashMap<>(TestRangerDeltaAuthorization.config());
    conf.remove("authorization.metalake.id");
    conf.remove("authorization.catalog.id");
    conf.put(Catalog.AUTHORIZATION_PROVIDER, "ranger-delta");
    conf.put("authorization.ranger.admin.url", endpoint.url());
    conf.put("authorization.ranger.delta.catalog", name);
    conf.put("delta.warehouse", "s3a://reserved-only/task1");
    conf.put("delta.storage-credential-mode", "static");
    conf.put("s3-access-key-id", "test-access");
    conf.put("s3-secret-access-key", "private-test-value");
    conf.putAll(nativeProperties);
    lake.createCatalog(name, Catalog.Type.RELATIONAL, "lakehouse-delta", null, conf);
  }

  private void adoptPublished(String name) {
    DeltaRegistrationCommand command =
        DeltaRegistrationCommand.adopt(
            "adopt-" + name,
            nativeFixture.get("location").asText(),
            "root-native-writer-completed",
            "task1-read-only-callback-qualification");
    Table table =
        lake.loadCatalog(CATALOG)
            .asTableCatalog()
            .createTable(
                NameIdentifier.of("sales", name),
                new Column[0],
                null,
                Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()));
    DeltaRegistrationReceipt receipt = DeltaRegistrationReceipt.fromProperties(table.properties());
    Table published =
        lake.loadCatalog(CATALOG)
            .asTableCatalog()
            .alterTable(
                NameIdentifier.of("sales", name),
                TableChange.setProperty(
                    DeltaRegistrationCommand.COMMAND_PROPERTY,
                    DeltaRegistrationCommand.publish(receipt.identity()).encode()));
    assertEquals(
        DeltaRegistrationReceipt.State.PUBLISHED,
        DeltaRegistrationReceipt.fromProperties(published.properties()).state());
  }

  @Test
  @Tag("gravitino-docker-test")
  @EnabledIfSystemProperty(named = "delta.ranger.callback.fixture", matches = ".+")
  void publishedTableOwnerPutRepairsAfterFailureAndAdoptPostCanReplay() {
    MetadataObject table =
        MetadataObjects.parse(CATALOG + ".sales.orders", MetadataObject.Type.TABLE);
    lake.setOwner(table, "alice", Owner.Type.USER);
    endpoint.failPolicy = true;
    assertThrows(RuntimeException.class, () -> lake.setOwner(table, "bob", Owner.Type.USER));
    assertEquals("bob", lake.getOwner(table).orElseThrow().name());
    assertTrue(endpoint.ownerUsers(table).contains("alice"));
    endpoint.failPolicy = false;
    lake.setOwner(table, lake.getOwner(table).orElseThrow().name(), Owner.Type.USER);
    assertEquals(List.of("bob"), endpoint.ownerUsers(table));
    endpoint.failPolicy = true;
    assertThrows(RuntimeException.class, () -> adoptPublished("adopt_failed"));
    endpoint.failPolicy = false;
    adoptPublished("adopt_failed");
    assertEquals(
        List.of(ADMIN),
        endpoint.ownerUsers(
            MetadataObjects.parse(CATALOG + ".sales.adopt_failed", MetadataObject.Type.TABLE)));
  }

  private void reserve(String name) {
    DeltaRegistrationCommand command =
        DeltaRegistrationCommand.reserve(
            "token-" + name,
            "scheduler/" + name,
            "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}");
    lake.loadCatalog(CATALOG)
        .asTableCatalog()
        .createTable(
            NameIdentifier.of("sales", name),
            new Column[0],
            null,
            Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()));
  }

  static final class AdminEndpoint implements AutoCloseable {
    final ObjectMapper mapper = new ObjectMapper();
    final HttpServer http;
    final Map<Long, RangerPolicy> policies = new LinkedHashMap<>();
    final Map<String, RangerRole> roles = new LinkedHashMap<>();
    final Set<String> users = new HashSet<>(Set.of(ADMIN));
    final Set<String> groups = new HashSet<>();
    final AtomicLong policyWrites = new AtomicLong();
    final AtomicLong roleDeleteAttempts = new AtomicLong();
    volatile boolean failPolicy;
    volatile boolean rejectRoleDeletion;
    long id = 100;
    final RangerServiceDef definition;

    AdminEndpoint() throws Exception {
      definition =
          mapper.readValue(
              Path.of("src/main/resources/ranger-delta-servicedef.json").toFile(),
              RangerServiceDef.class);
      http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      http.createContext("/service", this::handle);
      http.start();
    }

    String url() {
      return "http://127.0.0.1:" + http.getAddress().getPort();
    }

    synchronized void handle(HttpExchange exchange) throws IOException {
      String path = exchange.getRequestURI().getPath();
      String method = exchange.getRequestMethod();
      int status = 200;
      Object response;
      try {
        if (path.contains("/servicedef/name/")) {
          response = definition;
        } else if (path.contains("/service/name/")) {
          RangerService service = new RangerService();
          service.setId(1L);
          service.setName("delta_local");
          service.setType("spark");
          service.setConfigs(Map.of("gravitino.metalake", LAKE));
          response = service;
        } else if (path.contains("/xusers/users")) {
          if (method.equals("POST")) {
            Map<?, ?> request = mapper.readValue(exchange.getRequestBody(), Map.class);
            String name = (String) request.get("name");
            users.add(name);
            response = Map.of("id", 1, "name", name);
          } else if (method.equals("DELETE")) {
            status = 204;
            response = null;
          } else {
            String name = query(exchange, "name");
            response =
                Map.of(
                    "vXUsers",
                    users.contains(name) ? List.of(Map.of("id", 1, "name", name)) : List.of(),
                    "resultSize",
                    users.contains(name) ? 1 : 0,
                    "totalCount",
                    users.contains(name) ? 1 : 0);
          }
        } else if (path.contains("/xusers/groups")) {
          if (method.equals("POST")) {
            Map<?, ?> request = mapper.readValue(exchange.getRequestBody(), Map.class);
            String name = (String) request.get("name");
            groups.add(name);
            response = Map.of("id", 1, "name", name);
          } else if (method.equals("DELETE")) {
            status = 204;
            response = null;
          } else {
            String name = query(exchange, "name");
            response =
                Map.of(
                    "vXGroups",
                    groups.contains(name) ? List.of(Map.of("id", 1, "name", name)) : List.of(),
                    "resultSize",
                    groups.contains(name) ? 1 : 0,
                    "totalCount",
                    groups.contains(name) ? 1 : 0);
          }
        } else if (path.contains("/roles/grant/") || path.contains("/roles/revoke/")) {
          Map<?, ?> request = mapper.readValue(exchange.getRequestBody(), Map.class);
          for (Object name : (List<?>) request.get("targetRoles")) {
            RangerRole role = roles.get(name);
            for (String kind : List.of("users", "groups")) {
              List<RangerRole.RoleMember> members =
                  kind.equals("users") ? role.getUsers() : role.getGroups();
              if (request.get(kind) == null) {
                continue;
              }
              for (Object principal : (List<?>) request.get(kind)) {
                members.removeIf(m -> m.getName().equals(principal));
                if (path.contains("/grant/")) {
                  members.add(new RangerRole.RoleMember(principal.toString(), false));
                }
              }
            }
          }
          response = Map.of("statusCode", 0);
        } else if (path.contains("/roles/name/")) {
          String name = path.substring(path.lastIndexOf('/') + 1);
          response = roles.get(name);
          if (method.equals("DELETE")) {
            roleDeleteAttempts.incrementAndGet();
            if (rejectRoleDeletion) {
              status = 400;
              response = Map.of("msgDesc", "Role is referenced in one or more policies");
            } else {
              roles.remove(name);
              status = 204;
            }
          } else if (response == null) {
            status = 404;
            response = Map.of("msgDesc", "Role missing");
          }
        } else if (path.endsWith("/roles") || path.matches(".*/roles/[0-9]+")) {
          RangerRole role = mapper.readValue(exchange.getRequestBody(), RangerRole.class);
          if (role.getId() == null) {
            role.setId(++id);
            status = 200;
          }
          roles.put(role.getName(), role);
          response = role;
        } else if (path.contains("/policy")) {
          if (method.equals("GET")) {
            response = new ArrayList<>(policies.values());
          } else {
            policyWrites.incrementAndGet();
            if (failPolicy) {
              status = 500;
              response = Map.of("msgDesc", "Injected policy failure");
            } else if (method.equals("DELETE")) {
              policies.remove(Long.valueOf(path.substring(path.lastIndexOf('/') + 1)));
              status = 204;
              response = null;
            } else {
              RangerPolicy policy = mapper.readValue(exchange.getRequestBody(), RangerPolicy.class);
              if (policy.getId() == null) {
                policy.setId(++id);
              }
              policies.put(policy.getId(), policy);
              response = policy;
            }
          }
        } else {
          status = 404;
          response = Map.of("msgDesc", "Unsupported fixture endpoint " + path);
        }
      } catch (Exception e) {
        status = 500;
        response = Map.of("msgDesc", e.toString());
      }
      byte[] bytes = mapper.writeValueAsBytes(response);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(status, status == 204 ? -1 : bytes.length);
      if (status != 204) {
        exchange.getResponseBody().write(bytes);
      }
      exchange.close();
    }

    String query(HttpExchange exchange, String key) {
      String query = exchange.getRequestURI().getRawQuery();
      if (query == null) {
        return null;
      }
      for (String pair : query.split("&")) {
        String[] split = pair.split("=", 2);
        if (split[0].equals(key)) {
          return URLDecoder.decode(split[1], StandardCharsets.UTF_8);
        }
      }
      return null;
    }

    synchronized RangerRole roleEnding(String name) {
      return roles.values().stream()
          .filter(r -> r.getName().endsWith("_" + name))
          .findFirst()
          .orElseThrow();
    }

    synchronized boolean hasGrant(String role) {
      return policies.values().stream()
          .flatMap(p -> p.getPolicyItems().stream())
          .anyMatch(
              i ->
                  i.getRoles().contains(role)
                      && i.getAccesses().stream().anyMatch(a -> a.getType().equals("select")));
    }

    synchronized List<String> ownerUsers(MetadataObject object) {
      String[] names = object.fullName().split("\\.");
      return policies.values().stream()
          .filter(
              p ->
                  p.getResources().get("catalog").getValues().equals(List.of(names[0]))
                      && p.getResources().get("database").getValues().equals(List.of(names[1]))
                      && p.getResources()
                          .get("table")
                          .getValues()
                          .equals(List.of(names.length == 3 ? names[2] : "*")))
          .flatMap(p -> p.getPolicyItems().stream())
          .filter(i -> i.getRoles().contains("GRAVITINO_OWNER_ROLE"))
          .flatMap(i -> i.getUsers().stream())
          .collect(Collectors.toList());
    }

    @Override
    public void close() {
      http.stop(0);
    }
  }
}
