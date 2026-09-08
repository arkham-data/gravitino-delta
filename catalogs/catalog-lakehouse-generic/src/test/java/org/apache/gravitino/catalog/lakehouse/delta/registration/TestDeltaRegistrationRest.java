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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real HTTP/JCasbin boundary experiment; simple named principals are not trusted identity proof.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDeltaRegistrationRest {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeltaRegistrationRest.class);
  private static final String METALAKE = "registration_rest";
  private static final String CATALOG = "registrations";
  private static final String ADMIN = "registration-admin";
  private static final String ACTOR = "registration-actor";
  private static final String OUTSIDER = "registration-outsider";
  private static final String FOREIGN = "registration-foreign";
  private static final String RECOVERY = "registration-recovery";
  @TempDir static Path directory;
  private final List<GravitinoAdminClient> clients = new ArrayList<>();
  private final Properties evidence = new Properties();
  private MiniGravitino server;
  private Object previousConfig;
  private GravitinoMetalake admin;
  private TableCatalog actor;
  private String uri;
  private String schema;
  private int sequence;

  @BeforeAll
  void start() throws Exception {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    Map<String, String> config = new HashMap<>();
    config.put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE);
    config.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
        directory.resolve("store").toString());
    config.put(Configs.CACHE_ENABLED.getKey(), "false");
    config.put(Configs.CATALOG_LOAD_ISOLATED.getKey(), "false");
    config.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    config.put(Configs.AUTHENTICATORS.getKey(), "simple");
    config.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    config.put("gravitino.auxService.names", "");
    config.put("SimpleAuthUserName", ADMIN);
    server = new MiniGravitino(new MiniGravitinoContext(config, true, true));
    server.start();
    JettyServerConfig jetty =
        JettyServerConfig.fromConfig(
            server.getServerConfig(), GravitinoServer.WEBSERVER_CONF_PREFIX);
    uri = "http://" + jetty.getHost() + ":" + jetty.getHttpPort();
    assertTrue(server.getServerConfig().get(Configs.ENABLE_AUTHORIZATION));
    assertFalse(server.getServerConfig().get(Configs.CACHE_ENABLED));
    assertFalse(server.getServerConfig().get(Configs.CATALOG_LOAD_ISOLATED));
    assertEquals(
        "org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer",
        server.getServerConfig().get(Configs.AUTHORIZATION_IMPL));
    assertTrue(Files.exists(directory.resolve("store.mv.db")));
    evidence.putAll(config);
    evidence.setProperty(
        "actual.jdbc.url",
        GravitinoEnv.getInstance().config().get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL));
    evidence.setProperty(
        "actual.authorizer", server.getServerConfig().get(Configs.AUTHORIZATION_IMPL));
    evidence.setProperty("actual.http.uri", uri);
    evidence.setProperty("actual.java.version", System.getProperty("java.version"));
    evidence.setProperty("actual.entityStore", store().getClass().getName());
    saveEvidence();
    GravitinoAdminClient adminClient = client(ADMIN);
    adminClient.createMetalake(METALAKE, null, Collections.emptyMap());
    admin = adminClient.loadMetalake(METALAKE);
    for (String user : Arrays.asList(ACTOR, OUTSIDER, FOREIGN, RECOVERY)) {
      admin.addUser(user);
    }
    admin.createCatalog(
        CATALOG, Catalog.Type.RELATIONAL, "delta-registration-test", null, Collections.emptyMap());
  }

  @BeforeEach
  void prepare() {
    RegistrationTestCatalog.reset();
    schema = "schema_" + sequence++;
    admin.loadCatalog(CATALOG).asSchemas().createSchema(schema, null, Collections.emptyMap());
    grant(ACTOR, Privileges.CreateTable.allow());
    grant(OUTSIDER);
    grant(FOREIGN, Privileges.ModifyTable.allow());
    grant(RECOVERY, Privileges.ModifyTable.allow());
    actor = tables(ACTOR);
  }

  @AfterEach
  void resetControls() {
    RegistrationTestCatalog.reset();
  }

  @AfterAll
  void stop() throws Exception {
    try {
      for (GravitinoAdminClient client : clients) {
        client.close();
      }
    } finally {
      try {
        if (server != null) {
          server.stop();
        }
      } finally {
        RegistrationTestCatalog.reset();
        FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
      }
    }
  }

  @Test
  void testDispatcherIdentityAndExactCreateRetry() throws Exception {
    Table first = reserve("retry");
    TableEntity before = raw("retry");
    assertEquals(1, RegistrationTestCatalog.CANDIDATES.size());
    assertEquals(before.id(), RegistrationTestCatalog.CANDIDATES.get(0).longValue());
    assertEquals(
        Long.toString(before.id()), first.properties().get("delta.registration.receipt.id"));
    Table retry = reserve("retry");
    assertEquals(2, RegistrationTestCatalog.CANDIDATES.size());
    assertNotEquals(
        RegistrationTestCatalog.CANDIDATES.get(0), RegistrationTestCatalog.CANDIDATES.get(1));
    assertEquals(before, raw("retry"));
    assertEquals(first.properties(), retry.properties());
    assertEquals(
        1,
        store()
            .list(
                Namespace.of(METALAKE, CATALOG, schema), TableEntity.class, Entity.EntityType.TABLE)
            .size());
    assertFalse(before.properties().containsKey(RegistrationCommand.KEY));
    assertEquals(0, RegistrationTestCatalog.VERIFICATIONS.get());
  }

  @Test
  void testPendingVisibilityAndPropertyValidation() throws Exception {
    Table receipt = reserve("pending");
    TableEntity before = raw("pending");
    assertEquals(4, receipt.properties().size());
    assertTrue(
        receipt.properties().keySet().stream()
            .allMatch(key -> key.startsWith("delta.registration.receipt.")));
    assertEquals(0, actor.listTables(Namespace.of(schema)).length);
    assertThrows(NoSuchTableException.class, () -> actor.loadTable(local("pending")));
    assertFalse(actor.tableExists(local("pending")));
    assertFalse(GravitinoEnv.getInstance().tableDispatcher().tableExists(full("pending")));
    assertEquals(before, raw("pending"));
    for (String key :
        Arrays.asList(
            StringIdentifier.ID_KEY,
            RegistrationCommand.INTERNAL + "state",
            "delta.registration.receipt.id")) {
      Map<String, String> properties = new HashMap<>();
      properties.put(RegistrationCommand.KEY, reservation("illegal").encode());
      properties.put(key, "forged");
      int creates = RegistrationTestCatalog.CANDIDATES.size();
      assertThrows(
          IllegalArgumentException.class,
          () -> actor.createTable(local("illegal"), new Column[0], null, properties));
      assertEquals(creates, RegistrationTestCatalog.CANDIDATES.size());
      int alters = RegistrationTestCatalog.ALTERS.get();
      assertThrows(
          IllegalArgumentException.class,
          () -> actor.alterTable(local("pending"), TableChange.setProperty(key, "forged")));
      assertEquals(alters, RegistrationTestCatalog.ALTERS.get());
    }
    int alters = RegistrationTestCatalog.ALTERS.get();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            actor.alterTable(
                local("pending"),
                TableChange.removeProperty(RegistrationCommand.INTERNAL + "state")));
    assertEquals(alters, RegistrationTestCatalog.ALTERS.get());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            actor.alterTable(
                local("pending"), TableChange.setProperty(RegistrationCommand.KEY, "2|PUBLISH")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            actor.alterTable(
                local("pending"),
                TableChange.setProperty(RegistrationCommand.KEY, publish("pending").encode()),
                TableChange.setProperty("ordinary", "value")));
    assertEquals(before, raw("pending"));
    assertEquals(0, RegistrationTestCatalog.VERIFICATIONS.get());
  }

  @Test
  void testPendingOwnershipAndAuthorization() throws Exception {
    reserve("owned");
    TableEntity before = raw("owned");
    assertEquals(ACTOR, before.auditInfo().creator());
    assertEquals(ACTOR, before.properties().get(RegistrationCommand.INTERNAL + "actor"));
    assertEquals(ACTOR, rawOwner("owned").get().name());
    assertEquals(
        ADMIN,
        admin
            .getOwner(
                MetadataObjects.of(Arrays.asList(CATALOG, schema), MetadataObject.Type.SCHEMA))
            .get()
            .name());
    RegistrationCommand publish = publish("owned");
    int calls = RegistrationTestCatalog.ALTERS.get();
    assertThrows(ForbiddenException.class, () -> transition(tables(OUTSIDER), "owned", publish));
    assertEquals(calls, RegistrationTestCatalog.ALTERS.get());
    RuntimeException foreignFailure =
        assertThrows(RuntimeException.class, () -> transition(tables(FOREIGN), "owned", publish));
    LOG.info(
        "Foreign actor HTTP mapping: {}: {}",
        foreignFailure.getClass().getName(),
        foreignFailure.getMessage());
    assertEquals(RuntimeException.class, foreignFailure.getClass());
    assertTrue(foreignFailure.getMessage().contains("SecurityException"));
    evidence.setProperty("foreign.clientException", foreignFailure.getClass().getName());
    evidence.setProperty("foreign.errorResponse", foreignFailure.getMessage());
    saveEvidence();
    assertEquals(calls + 1, RegistrationTestCatalog.ALTERS.get());
    assertEquals(before, raw("owned"));
    assertEquals(0, RegistrationTestCatalog.VERIFICATIONS.get());
    matched();
    transition(actor, "owned", publish);
    assertEquals(before.id(), raw("owned").id());
    assertEquals("PUBLISHED", state("owned"));
    assertEquals(1, actor.listTables(Namespace.of(schema)).length);
    assertTrue(actor.tableExists(local("owned")));
    assertEquals(
        "PUBLISHED",
        actor.loadTable(local("owned")).properties().get("delta.registration.receipt.state"));
  }

  @Test
  void testOwnerRestVisibilityBoundary() throws Exception {
    reserve("governance");
    assertEquals(ACTOR, rawOwner("governance").get().name());
    assertThrows(NoSuchMetadataObjectException.class, () -> admin.getOwner(metadata("governance")));
    assertThrows(
        NoSuchMetadataObjectException.class,
        () -> admin.setOwner(metadata("governance"), FOREIGN, Owner.Type.USER));
    assertEquals(ACTOR, rawOwner("governance").get().name());
    matched();
    transition(actor, "governance", publish("governance"));
    assertEquals(ACTOR, admin.getOwner(metadata("governance")).get().name());
    admin.setOwner(metadata("governance"), FOREIGN, Owner.Type.USER);
    assertEquals(FOREIGN, admin.getOwner(metadata("governance")).get().name());
  }

  @Test
  void testOwnerHookFailureAndCreateRetry() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    OwnerDispatcher original = env.internalOwnerDispatcher();
    AtomicBoolean failOnce = new AtomicBoolean(true);
    OwnerDispatcher injected =
        new OwnerDispatcher() {
          /** {@inheritDoc} */
          @Override
          public void setOwner(
              String metalake, MetadataObject object, String owner, Owner.Type type) {
            if (METALAKE.equals(metalake)
                && object.equals(metadata("owner_failure"))
                && failOnce.compareAndSet(true, false)) {
              throw new IllegalStateException(
                  "Synthetic owner assignment failure before relation write");
            }
            original.setOwner(metalake, object, owner, type);
          }

          /** {@inheritDoc} */
          @Override
          public Optional<Owner> getOwner(String metalake, MetadataObject object) {
            return original.getOwner(metalake, object);
          }
        };
    TableEntity before;
    try {
      FieldUtils.writeField(env, "internalOwnerDispatcher", injected, true);
      RuntimeException failure =
          assertThrows(RuntimeException.class, () -> reserve("owner_failure"));
      LOG.info(
          "Injected owner failure HTTP mapping: {}: {}",
          failure.getClass().getName(),
          failure.getMessage());
      assertEquals(RuntimeException.class, failure.getClass());
      evidence.setProperty("ownerFailure.clientException", failure.getClass().getName());
      evidence.setProperty("ownerFailure.errorResponse", failure.getMessage());
      saveEvidence();
      assertFalse(failOnce.get());
      before = raw("owner_failure");
      assertEquals("RESERVED", state("owner_failure"));
      assertFalse(rawOwner("owner_failure").isPresent());
      RegistrationCommand publish = publish("owner_failure");
      assertThrows(ForbiddenException.class, () -> transition(actor, "owner_failure", publish));
      assertEquals(0, RegistrationTestCatalog.ALTERS.get());
      assertEquals(before, raw("owner_failure"));
    } finally {
      FieldUtils.writeField(env, "internalOwnerDispatcher", original, true);
    }
    reserve("owner_failure");
    assertEquals(before, raw("owner_failure"));
    assertEquals(2, RegistrationTestCatalog.CANDIDATES.size());
    assertNotEquals(
        RegistrationTestCatalog.CANDIDATES.get(0), RegistrationTestCatalog.CANDIDATES.get(1));
    assertEquals(ACTOR, rawOwner("owner_failure").get().name());
    assertEquals(0, RegistrationTestCatalog.VERIFICATIONS.get());
  }

  @Test
  void testRecoveryRequiresExistingPermissionAndEvidence() throws Exception {
    reserve("recover");
    Path data = sentinel("recover");
    TableEntity before = raw("recover");
    RegistrationCommand abort = RegistrationCommand.abort(before.id(), 0, "token-recover");
    int calls = RegistrationTestCatalog.ALTERS.get();
    assertThrows(ForbiddenException.class, () -> transition(tables(OUTSIDER), "recover", abort));
    assertEquals(calls, RegistrationTestCatalog.ALTERS.get());
    TableCatalog recovery = tables(RECOVERY);
    assertThrows(RuntimeException.class, () -> transition(recovery, "recover", abort));
    assertEquals(calls + 1, RegistrationTestCatalog.ALTERS.get());
    assertEquals(before, raw("recover"));
    assertEquals(0, RegistrationTestCatalog.VERIFICATIONS.get());
    RegistrationTestCatalog.handoff = true;
    RegistrationTestCatalog.outcome =
        RegistrationCommitVerifier.Result.ambiguous("synthetic unavailable evidence", null);
    transition(recovery, "recover", abort);
    assertEquals("QUARANTINED", state("recover"));
    assertFalse(actor.tableExists(local("recover")));
    assertEquals(1, RegistrationTestCatalog.VERIFICATIONS.get());
    RegistrationTestCatalog.outcome =
        RegistrationCommitVerifier.Result.notCommitted("synthetic verified absence after handoff");
    transition(recovery, "recover", abort);
    assertEquals("ABORTED", state("recover"));
    assertEquals(before.id(), raw("recover").id());
    assertArrayEquals(new byte[] {3, 1, 4}, Files.readAllBytes(data));
  }

  @ParameterizedTest
  @ValueSource(strings = {"RESERVED", "REPLACING", "PUBLISHED", "ABORTED", "QUARANTINED"})
  void testSchemaGuardForAllRegistrationStates(String state) throws Exception {
    reserve("guarded");
    Path data = sentinel("guarded");
    if (state.equals("PUBLISHED") || state.equals("REPLACING")) {
      matched();
      transition(actor, "guarded", publish("guarded"));
      if (state.equals("REPLACING")) {
        transition(
            actor,
            "guarded",
            RegistrationCommand.beginReplace(
                raw("guarded").id(), 0, "token-guarded", "replacement", "schema-v2"));
      }
    } else if (state.equals("ABORTED") || state.equals("QUARANTINED")) {
      RegistrationTestCatalog.handoff = true;
      if (state.equals("QUARANTINED")) {
        RegistrationTestCatalog.outcome =
            RegistrationCommitVerifier.Result.ambiguous("synthetic uncertainty", null);
      }
      transition(
          actor, "guarded", RegistrationCommand.abort(raw("guarded").id(), 0, "token-guarded"));
    }
    assertEquals(state, state("guarded"));
    TableEntity before = raw("guarded");
    for (boolean cascade : new boolean[] {false, true}) {
      assertThrows(
          NonEmptySchemaException.class,
          () -> admin.loadCatalog(CATALOG).asSchemas().dropSchema(schema, cascade));
      assertEquals(before, raw("guarded"));
      assertArrayEquals(new byte[] {3, 1, 4}, Files.readAllBytes(data));
    }
    assertEquals(0, RegistrationTestCatalog.DROPS.get());
    String empty = schema + "_empty";
    admin.loadCatalog(CATALOG).asSchemas().createSchema(empty, null, Collections.emptyMap());
    assertTrue(admin.loadCatalog(CATALOG).asSchemas().dropSchema(empty, true));
  }

  @Test
  void testUnguardedManagedCascadeBoundary() throws Exception {
    reserve("unguarded");
    Path data = sentinel("unguarded");
    assertFalse(actor.tableExists(local("unguarded")));
    ManagedSchemaOperations unguarded =
        new ManagedSchemaOperations() {
          /** {@inheritDoc} */
          @Override
          protected EntityStore store() {
            return TestDeltaRegistrationRest.this.store();
          }
        };
    assertTrue(unguarded.dropSchema(NameIdentifier.of(METALAKE, CATALOG, schema), true));
    assertFalse(store().exists(full("unguarded"), Entity.EntityType.TABLE));
    assertEquals(0, RegistrationTestCatalog.DROPS.get());
    assertArrayEquals(new byte[] {3, 1, 4}, Files.readAllBytes(data));
  }

  private Path sentinel(String name) throws Exception {
    Path path = directory.resolve(schema).resolve(name).resolve("native-sentinel");
    Files.createDirectories(path.getParent());
    Files.write(path, new byte[] {3, 1, 4});
    return path;
  }

  private NameIdentifier local(String name) {
    return NameIdentifier.of(schema, name);
  }

  private NameIdentifier full(String name) {
    return NameIdentifier.of(METALAKE, CATALOG, schema, name);
  }

  private MetadataObject metadata(String name) {
    return MetadataObjects.of(Arrays.asList(CATALOG, schema, name), MetadataObject.Type.TABLE);
  }

  private Optional<Owner> rawOwner(String name) {
    return GravitinoEnv.getInstance().internalOwnerDispatcher().getOwner(METALAKE, metadata(name));
  }

  private String state(String name) throws Exception {
    return raw(name).properties().get(RegistrationCommand.INTERNAL + "state");
  }

  private RegistrationCommand publish(String name) throws Exception {
    return RegistrationCommand.publish(raw(name).id(), 0, "token-" + name, 0);
  }

  private void transition(TableCatalog catalog, String name, RegistrationCommand command) {
    catalog.alterTable(
        local(name), TableChange.setProperty(RegistrationCommand.KEY, command.encode()));
  }

  private void matched() {
    RegistrationTestCatalog.outcome =
        RegistrationCommitVerifier.Result.matched(
            new RegistrationCommitVerifier.Observation(
                0,
                "synthetic-native-uuid",
                "schema-v1",
                Collections.singletonList(Column.of("id", Types.LongType.get()))),
            "synthetic trusted correlation; no native writer");
  }

  private void saveEvidence() throws Exception {
    Path path = Paths.get(System.getenv("IT_PROJECT_DIR"), "delta-managed-rest-runtime.properties");
    try (OutputStream output = Files.newOutputStream(path)) {
      evidence.store(
          output,
          "Real embedded HTTP configuration and client error responses; synthetic native evidence");
    }
  }

  private GravitinoAdminClient client(String principal) {
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(uri).withSimpleAuth(principal).build();
    clients.add(client);
    return client;
  }

  private TableCatalog tables(String principal) {
    return client(principal).loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
  }

  private void grant(String user, Privilege... extra) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            CATALOG, Collections.singletonList(Privileges.UseCatalog.allow()));
    List<Privilege> privileges = new ArrayList<>();
    privileges.add(Privileges.UseSchema.allow());
    privileges.addAll(Arrays.asList(extra));
    String role = user + "_" + schema;
    admin.createRole(
        role,
        Collections.emptyMap(),
        Arrays.asList(catalog, SecurableObjects.ofSchema(catalog, schema, privileges)));
    admin.grantRolesToUser(Collections.singletonList(role), user);
  }

  private RegistrationCommand reservation(String name) {
    return RegistrationCommand.reserve(
        "token-" + name, directory.resolve(schema).resolve(name).toString(), "schema-v1");
  }

  private Table reserve(String name) {
    return actor.createTable(
        NameIdentifier.of(schema, name),
        new Column[0],
        null,
        Collections.singletonMap(RegistrationCommand.KEY, reservation(name).encode()));
  }

  private TableEntity raw(String name) throws Exception {
    return store()
        .get(
            NameIdentifier.of(METALAKE, CATALOG, schema, name),
            Entity.EntityType.TABLE,
            TableEntity.class);
  }

  private EntityStore store() {
    return GravitinoEnv.getInstance().entityStore();
  }
}
