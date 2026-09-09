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

package org.apache.gravitino.catalog.lakehouse.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretReference;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

/**
 * Real HTTP and durable H2 with JCasbin; simple test identities are not production authentication.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestDeltaCatalogRest {
  private static final String METALAKE = "delta_rest";
  private static final String CATALOG = "delta";
  private static final String ADMIN = "delta-admin";
  private static final String ACTOR = "delta-actor";
  private static final String FOREIGN = "delta-foreign";
  private static final String SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
  @TempDir static Path directory;
  private final List<GravitinoAdminClient> clients = new ArrayList<>();
  private MiniGravitino server;
  private Object previousConfig;
  private Object previousMetrics;
  private GravitinoMetalake admin;
  private TableCatalog actor;
  private String uri;
  private String schema;
  private int sequence;

  @BeforeAll
  void start() throws Exception {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousMetrics = FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
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
    config.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    config.put(Configs.CATALOG_CREDENTIAL_BACKFILL_TO_PROPERTIES.getKey(), "true");
    config.put("gravitino.auxService.names", "");
    config.put("SimpleAuthUserName", ADMIN);
    config.put("gravitino.secret.providers", "memory,reference");
    config.put(
        "gravitino.secret.provider.reference.className",
        NativeFreeSecretReferences.class.getName());
    config.put(
        "gravitino.secret.provider.memory.className",
        "org.apache.gravitino.secret.memory.InMemorySecretsProvider");
    for (String type :
        List.of(
            "org.slf4j.LoggerFactory",
            "org.apache.log4j.Logger",
            "jersey.repackaged.org.objectweb.asm.ClassVisitor",
            "org.glassfish.jersey.server.ResourceConfig")) {
      Class<?> actual = Class.forName(type);
      LoggerFactory.getLogger(TestDeltaCatalogRest.class)
          .info(
              "Runtime origin {} = {}",
              type,
              actual.getProtectionDomain().getCodeSource().getLocation());
    }
    server = new MiniGravitino(new MiniGravitinoContext(config, true, true));
    server.start();
    JettyServerConfig jetty =
        JettyServerConfig.fromConfig(
            server.getServerConfig(), GravitinoServer.WEBSERVER_CONF_PREFIX);
    uri = "http://" + jetty.getHost() + ":" + jetty.getHttpPort();
    assertTrue(server.getServerConfig().get(Configs.ENABLE_AUTHORIZATION));
    assertFalse(server.getServerConfig().get(Configs.CATALOG_LOAD_ISOLATED));
    assertEquals(
        "org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer",
        GravitinoEnv.getInstance().gravitinoAuthorizer().getClass().getName());
    assertTrue(Files.exists(directory.resolve("store.mv.db")));
    GravitinoAdminClient ac = client(ADMIN);
    ac.createMetalake(METALAKE, null, Map.of());
    admin = ac.loadMetalake(METALAKE);
    admin.addUser(ACTOR);
    admin.addUser(FOREIGN);
    admin.createCatalog(
        CATALOG,
        Catalog.Type.RELATIONAL,
        "lakehouse-delta",
        null,
        Map.of(
            "delta.warehouse",
            "s3a://reserved-only/task4",
            "delta.storage-credential-mode",
            "static",
            "s3-access-key-id",
            "test-access",
            "s3-secret-access-key",
            "private-test-value"));
  }

  @BeforeEach
  void prepare() {
    schema = "schema_" + sequence++;
    Schema created =
        admin.loadCatalog(CATALOG).asSchemas().createSchema(schema, "managed", Map.of());
    assertEquals(Map.of(), created.properties());
    grant(ACTOR, Privileges.CreateTable.allow(), Privileges.ModifyTable.allow());
    grant(FOREIGN, Privileges.CreateTable.allow(), Privileges.ModifyTable.allow());
    actor = tables(ACTOR);
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
        FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
        FieldUtils.writeField(GravitinoEnv.getInstance(), "metricsSystem", previousMetrics, true);
      }
    }
  }

  @Test
  void pendingIsHiddenAndLostReplyRecoversThroughTypedIntent() throws Exception {
    DeltaRegistrationCommand intent = reservation("lost");
    Files.writeString(directory.resolve("lost-intent.json"), intent.encode());
    reserve("lost"); // Intentionally discard the POST receipt and its numeric ID.
    DeltaRegistrationCommand saved =
        DeltaRegistrationCommand.decode(Files.readString(directory.resolve("lost-intent.json")));
    DeltaRegistrationReceipt recovered =
        command(
            actor,
            "lost",
            DeltaRegistrationCommand.statusByToken(
                saved.token().orElseThrow(), saved.runId().orElseThrow()));
    assertEquals(raw("lost").id(), recovered.identity().id());
    assertEquals(DeltaRegistrationReceipt.State.RESERVED, recovered.state());
    assertFalse(actor.tableExists(local("lost")));
    assertEquals(0, actor.listTables(Namespace.of(schema)).length);
    assertEquals(
        recovered.identity(),
        command(actor, "lost", DeltaRegistrationCommand.status(recovered.identity())).identity());
    assertEquals(
        recovered.identity(),
        command(tables(ADMIN), "lost", DeltaRegistrationCommand.administrativeStatus()).identity());
    TableEntity before = raw("lost");
    assertThrows(
        RuntimeException.class,
        () ->
            command(
                actor,
                "lost",
                DeltaRegistrationCommand.statusByToken("wrong", saved.runId().orElseThrow())));
    assertThrows(
        RuntimeException.class,
        () ->
            command(
                actor,
                "lost",
                DeltaRegistrationCommand.statusByToken(saved.token().orElseThrow(), "wrong")));
    assertThrows(
        RuntimeException.class,
        () ->
            command(
                tables(FOREIGN),
                "lost",
                DeltaRegistrationCommand.statusByToken(
                    saved.token().orElseThrow(), saved.runId().orElseThrow())));
    assertEquals(before, raw("lost"));
    assertFalse(before.properties().toString().contains("private-test-value"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void completedOwnerHookFailureRetainsRowAndExactReplayRepairs(boolean afterWrite)
      throws Exception {
    OwnerDispatcher original = GravitinoEnv.getInstance().internalOwnerDispatcher();
    AtomicBoolean fail = new AtomicBoolean(true);
    OwnerDispatcher injected =
        new OwnerDispatcher() {
          @Override
          public void setOwner(
              String metalake, MetadataObject object, String owner, Owner.Type type) {
            if (METALAKE.equals(metalake)
                && object.equals(metadata("owner"))
                && fail.compareAndSet(true, false)) {
              if (afterWrite) {
                original.setOwner(metalake, object, owner, type);
              }
              throw new IllegalStateException("Task4 injected completed owner hook failure");
            }
            original.setOwner(metalake, object, owner, type);
          }

          @Override
          public Optional<Owner> getOwner(String metalake, MetadataObject object) {
            return original.getOwner(metalake, object);
          }
        };
    TableEntity before;
    try {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "internalOwnerDispatcher", injected, true);
      assertThrows(RuntimeException.class, () -> reserve("owner"));
      assertFalse(fail.get());
      before = raw("owner");
      assertEquals(afterWrite, original.getOwner(METALAKE, metadata("owner")).isPresent());
      DeltaRegistrationReceipt receipt =
          command(tables(ADMIN), "owner", DeltaRegistrationCommand.administrativeStatus());
      assertEquals(DeltaRegistrationReceipt.State.RESERVED, receipt.state());
      if (!afterWrite) {
        RuntimeException denied =
            assertThrows(
                RuntimeException.class,
                () ->
                    command(
                        tables(ADMIN),
                        "owner",
                        DeltaRegistrationCommand.publish(receipt.identity())));
        assertTrue(denied.getMessage().contains("DELTA_AUTHORIZATION_REQUIRED"));
        assertEquals(before, raw("owner"));
      }
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "internalOwnerDispatcher", original, true);
    }
    reserve("owner");
    assertEquals(before, raw("owner"));
    assertEquals(ACTOR, original.getOwner(METALAKE, metadata("owner")).orElseThrow().name());
    original.setOwner(METALAKE, metadata("owner"), FOREIGN, Owner.Type.USER);
    assertThrows(RuntimeException.class, () -> reserve("owner"));
    assertEquals(FOREIGN, original.getOwner(METALAKE, metadata("owner")).orElseThrow().name());
    assertEquals(before, raw("owner"));
  }

  @Test
  void retainedRowsBlockLifecycleAndInternalPropertyWrites() throws Exception {
    reserve("retained");
    TableEntity before = raw("retained");
    assertThrows(RuntimeException.class, () -> actor.dropTable(local("retained")));
    assertThrows(RuntimeException.class, () -> actor.purgeTable(local("retained")));
    assertThrows(
        RuntimeException.class,
        () -> actor.alterTable(local("retained"), TableChange.rename("renamed")));
    assertThrows(
        RuntimeException.class,
        () ->
            actor.alterTable(
                local("retained"),
                TableChange.setProperty("delta.registration.internal.state", "{}")));
    assertThrows(
        RuntimeException.class,
        () -> admin.loadCatalog(CATALOG).asSchemas().dropSchema(schema, true));
    assertThrows(RuntimeException.class, () -> admin.dropCatalog(CATALOG, true));
    assertThrows(RuntimeException.class, () -> client(ADMIN).dropMetalake(METALAKE, true));
    assertEquals(before, raw("retained"));
  }

  @Test
  void schemaAndCatalogCannotAcceptUntrustedStorageSettings() {
    var schemas = admin.loadCatalog(CATALOG).asSchemas();
    assertThrows(
        RuntimeException.class,
        () -> schemas.createSchema("bad", null, Map.of("location", "s3a://elsewhere/root")));
    assertThrows(
        RuntimeException.class,
        () -> schemas.createSchema("bad_comment", "x".repeat(257), Map.of()));
    assertThrows(
        RuntimeException.class,
        () ->
            schemas.alterSchema(
                schema, SchemaChange.setProperty("location", "s3a://elsewhere/root")));
    assertThrows(
        RuntimeException.class,
        () ->
            admin.alterCatalog(
                CATALOG, CatalogChange.setProperty("delta.warehouse", "s3a://elsewhere/root")));
    assertFalse(admin.loadCatalog(CATALOG).properties().toString().contains("private-test-value"));
    assertEquals(Map.of(), schemas.loadSchema(schema).properties());
    assertTrue(schemas.dropSchema(schema, true));
  }

  @Test
  void commentTransportUpdatesScalarAndRejectsWholeInvalidBatch() throws Exception {
    var schemas = admin.loadCatalog(CATALOG).asSchemas();
    Schema changed = schemas.alterSchema(schema, SchemaChange.setProperty("comment", "updated"));
    assertEquals("updated", changed.comment());
    assertEquals(Map.of(), changed.properties());
    assertEquals("updated", schemas.loadSchema(schema).comment());
    var store = GravitinoEnv.getInstance().entityStore();
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, schema);
    SchemaEntity before = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
    assertThrows(
        RuntimeException.class,
        () ->
            schemas.alterSchema(
                schema,
                SchemaChange.setProperty("comment", "must-not-stick"),
                SchemaChange.setProperty("location", "s3a://elsewhere/root")));
    RuntimeException bindingRejected =
        assertThrows(
            RuntimeException.class,
            () ->
                schemas.alterSchema(
                    schema,
                    SchemaChange.setSecretBinding(
                        "comment", new SecretBinding("memory", "secret-comment"))));
    assertTrue(bindingRejected.getMessage().contains("must not be a secret URN"));
    assertEquals(before, store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class));
    RuntimeException referenceRejected =
        assertThrows(
            RuntimeException.class,
            () ->
                schemas.alterSchema(
                    schema,
                    SchemaChange.setSecretReference(
                        "comment",
                        new SecretReference("reference", Map.of("key", "secret-comment")))));
    assertTrue(referenceRejected.getMessage().contains("must not be a secret URN"));
    assertEquals(before, store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class));
    assertThrows(
        RuntimeException.class,
        () -> schemas.alterSchema(schema, SchemaChange.setProperty("comment", "x".repeat(257))));
    assertEquals(before, store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class));
    assertEquals(
        null, schemas.alterSchema(schema, SchemaChange.removeProperty("comment")).comment());
    assertEquals(Map.of(), schemas.loadSchema(schema).properties());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void invalidEarlierCommentCannotBeHiddenByLaterChange(boolean clear) throws Exception {
    var store = GravitinoEnv.getInstance().entityStore();
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, schema);
    SchemaEntity before = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
    for (String invalid : List.of("x".repeat(257), "bad\u0000comment")) {
      assertThrows(
          RuntimeException.class,
          () ->
              admin
                  .loadCatalog(CATALOG)
                  .asSchemas()
                  .alterSchema(
                      schema,
                      SchemaChange.setProperty("comment", invalid),
                      clear
                          ? SchemaChange.removeProperty("comment")
                          : SchemaChange.setProperty("comment", "valid")));
      assertEquals(before, store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class));
    }
  }

  @Test
  void emptySchemaChangePreservesCompleteStoredRow() throws Exception {
    var store = GravitinoEnv.getInstance().entityStore();
    NameIdentifier ident = NameIdentifier.of(METALAKE, CATALOG, schema);
    SchemaEntity before = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
    Schema unchanged = admin.loadCatalog(CATALOG).asSchemas().alterSchema(schema);
    assertEquals(before.comment(), unchanged.comment());
    assertEquals(Map.of(), unchanged.properties());
    assertEquals(before, store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class));
  }

  @Test
  void concurrentRegistrationAfterEmptyPrecheckSurvivesCascadeRequest() throws Exception {
    EntityStore real = GravitinoEnv.getInstance().entityStore();
    EntityStore intercepted = mock(EntityStore.class, delegatesTo(real));
    CountDownLatch observedEmpty = new CountDownLatch(1);
    CountDownLatch inserted = new CountDownLatch(1);
    Namespace namespace = Namespace.of(METALAKE, CATALOG, schema);
    doAnswer(
            call -> {
              var empty = real.list(namespace, TableEntity.class, Entity.EntityType.TABLE);
              assertTrue(empty.isEmpty());
              observedEmpty.countDown();
              assertTrue(inserted.await(20, TimeUnit.SECONDS));
              return empty;
            })
        .when(intercepted)
        .list(eq(namespace), eq(TableEntity.class), eq(Entity.EntityType.TABLE));
    DeltaCatalogOperations operations =
        new DeltaCatalogOperations(
            intercepted, GravitinoEnv.getInstance().idGenerator(), DeltaMetadataReader::new);
    var executor = Executors.newSingleThreadExecutor();
    try {
      var result =
          executor.submit(
              () ->
                  assertThrows(
                      NonEmptySchemaException.class,
                      () ->
                          operations.dropSchema(
                              NameIdentifier.of(METALAKE, CATALOG, schema), true)));
      assertTrue(observedEmpty.await(20, TimeUnit.SECONDS));
      reserve("racing");
      TableEntity before = raw("racing");
      inserted.countDown();
      result.get(20, TimeUnit.SECONDS);
      assertEquals(before, raw("racing"));
      assertEquals("managed", admin.loadCatalog(CATALOG).asSchemas().loadSchema(schema).comment());
    } finally {
      inserted.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(20, TimeUnit.SECONDS));
      operations.close();
    }
  }

  private GravitinoAdminClient client(String principal) {
    GravitinoAdminClient result =
        GravitinoAdminClient.builder(uri).withSimpleAuth(principal).build();
    clients.add(result);
    return result;
  }

  private TableCatalog tables(String principal) {
    return client(principal).loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
  }

  private void grant(String user, Privilege... extra) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(CATALOG, List.of(Privileges.UseCatalog.allow()));
    List<Privilege> privileges = new ArrayList<>(List.of(Privileges.UseSchema.allow()));
    privileges.addAll(Arrays.asList(extra));
    String role = user + "_" + schema;
    admin.createRole(
        role, Map.of(), List.of(catalog, SecurableObjects.ofSchema(catalog, schema, privileges)));
    admin.grantRolesToUser(List.of(role), user);
  }

  private NameIdentifier local(String name) {
    return NameIdentifier.of(schema, name);
  }

  private MetadataObject metadata(String name) {
    return MetadataObjects.of(List.of(CATALOG, schema, name), MetadataObject.Type.TABLE);
  }

  private DeltaRegistrationCommand reservation(String name) {
    return DeltaRegistrationCommand.reserve("token-" + name, "scheduler/" + name, SCHEMA_JSON);
  }

  private Table reserve(String name) {
    return actor.createTable(
        local(name),
        new Column[0],
        null,
        Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, reservation(name).encode()));
  }

  private DeltaRegistrationReceipt command(
      TableCatalog tables, String name, DeltaRegistrationCommand command) {
    return DeltaRegistrationReceipt.fromProperties(
        tables
            .alterTable(
                local(name),
                TableChange.setProperty(
                    DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()))
            .properties());
  }

  private TableEntity raw(String name) throws Exception {
    return GravitinoEnv.getInstance()
        .entityStore()
        .get(
            NameIdentifier.of(METALAKE, CATALOG, schema, name),
            Entity.EntityType.TABLE,
            TableEntity.class);
  }
  /** Test-only external locator provider, ensuring REST preprocessing reaches comment admission. */
  public static final class NativeFreeSecretReferences extends InMemorySecretsProvider {
    /** Creates the test-only provider loaded by the embedded server. */
    public NativeFreeSecretReferences() {}

    /** {@inheritDoc} */
    @Override
    public SecretUrn buildReferenceUrn(String propertyKey, Map<String, String> attributes) {
      return SecretUrn.parse(
          SecretConstants.URN_PREFIX + "reference:" + attributes.get("key") + ":" + propertyKey);
    }
  }
}
