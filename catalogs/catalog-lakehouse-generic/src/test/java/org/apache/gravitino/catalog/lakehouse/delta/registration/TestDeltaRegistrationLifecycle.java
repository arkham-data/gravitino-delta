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

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS;
import static org.apache.gravitino.Configs.ENTITY_CHANGE_LOG_RETENTION_SECS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.STORE_TRANSACTION_MAX_SKEW_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/** Exercises the test protocol against a real file-backed relational store. */
class TestDeltaRegistrationLifecycle {
  private static final String METALAKE_NAME = "registration_metalake";
  private static final String CATALOG_NAME = "registration_catalog";
  private static final NameIdentifier IDENT =
      NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "schema", "table");
  @TempDir Path directory;
  private EntityStore store;
  private IdGenerator idGenerator;
  private ManagedSchemaOperations schemas;
  private RegistrationLifecycleFixture ops;
  private Config config;
  private Object previousConfig;
  private String actor = "owner";
  private boolean handoff = true;
  private final AtomicInteger verifications = new AtomicInteger();
  private RegistrationCommitVerifier.Result outcome =
      RegistrationCommitVerifier.Result.notCommitted("synthetic absent");

  @BeforeEach
  void setUp() throws Exception {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    String storePath = directory.resolve("store").toString();
    config = Mockito.mock(Config.class);
    when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(storePath);

    // The following properties are used to create the JDBC connection; they are just for test, in
    // the real world, they will be set automatically by the configuration file if you set
    // ENTITY_RELATIONAL_STORE as EMBEDDED_ENTITY_RELATIONAL_STORE.
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=0;MODE=MYSQL", storePath));
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
    when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

    when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    when(config.get(STORE_TRANSACTION_MAX_SKEW_TIME)).thenReturn(1000L);
    when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    when(config.get(ENTITY_CHANGE_LOG_POLL_INTERVAL_SECS)).thenReturn(3L);
    when(config.get(ENTITY_CHANGE_LOG_RETENTION_SECS)).thenReturn(24 * 60 * 60L);
    when(config.get(ENTITY_CHANGE_LOG_CLEANUP_INTERVAL_SECS)).thenReturn(60 * 60L);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(false);

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    idGenerator = RandomIdGenerator.INSTANCE;

    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    AuditInfo audit =
        AuditInfo.builder().withCreator("owner").withCreateTime(Instant.now()).build();
    store.put(
        BaseMetalake.builder()
            .withId(idGenerator.nextId())
            .withName(METALAKE_NAME)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(audit)
            .build(),
        false);
    store.put(
        CatalogEntity.builder()
            .withId(idGenerator.nextId())
            .withName(CATALOG_NAME)
            .withNamespace(Namespace.of(METALAKE_NAME))
            .withProvider("test-only")
            .withType(Catalog.Type.RELATIONAL)
            .withAuditInfo(audit)
            .build(),
        false);
    openOperations();
    schemas.createSchema(
        NameIdentifier.of(IDENT.namespace().levels()),
        null,
        StringIdentifier.newPropertiesWithId(StringIdentifier.fromId(idGenerator.nextId()), null));
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (store != null) store.close();
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
    }
  }

  @Test
  void testReserveSurvivesReopenAndIsHidden() throws Exception {
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(1001L),
            Collections.singletonMap(
                "delta.registration.command",
                "1|RESERVE|dG9rZW4|ZmlsZTovLy90bXAvZGVsdGE|c2NoZW1h"));
    Table receipt = ops.createTable(IDENT, new Column[0], null, properties);
    Assertions.assertEquals(
        0,
        ops.listTables(IDENT.namespace()).length,
        "Reservation must not be an ordinary visible table");
    Assertions.assertThrows(NoSuchTableException.class, () -> ops.loadTable(IDENT));
    Assertions.assertFalse(ops.tableExists(IDENT));
    Assertions.assertEquals("1001", receipt.properties().get("delta.registration.receipt.id"));
    Assertions.assertEquals(1001L, raw().id());
    Assertions.assertEquals(
        "token", raw().properties().get(RegistrationCommand.INTERNAL + "token"));
    Assertions.assertEquals(
        "file:///tmp/delta", raw().properties().get(RegistrationCommand.INTERNAL + "location"));
    Assertions.assertEquals(
        "owner", raw().properties().get(RegistrationCommand.INTERNAL + "actor"));
    TableEntity reservation = raw();
    store.close();
    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    openOperations();
    Assertions.assertEquals(1001L, raw().id());
    Assertions.assertFalse(ops.tableExists(IDENT));
    Assertions.assertTrue(Files.exists(directory.resolve("store.mv.db")));
    Assertions.assertEquals(reservation, raw());
    Assertions.assertEquals(0, ops.listTables(IDENT.namespace()).length);
    Assertions.assertThrows(NoSuchTableException.class, () -> ops.loadTable(IDENT));
  }

  @Test
  void testExactRetryRetainsFirstDispatcherIdentity() throws Exception {
    reserve(1001L, "token", "schema");
    TableEntity first = raw();
    Files.createDirectories(
        directory.resolve("delta")); // Native creation must not change URI identity.
    Table retry = reserve(2002L, "token", "schema");
    Assertions.assertEquals("1001", retry.properties().get("delta.registration.receipt.id"));
    Assertions.assertEquals(first, raw());
    Assertions.assertThrows(IllegalStateException.class, () -> reserve(2002L, "other", "schema"));
    Assertions.assertThrows(
        IllegalStateException.class, () -> reserve(2002L, "token", "other-schema"));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> create(2002L, RegistrationCommand.reserve("token", "file:///tmp/other", "schema")));
    actor = "intruder";
    Assertions.assertThrows(ForbiddenException.class, () -> reserve(2002L, "token", "schema"));
    Assertions.assertEquals(first, raw());
  }

  @Test
  void testFinalizeRequiresTrustedEvidence() throws Exception {
    reserve(1001L, "token", "schema");
    RegistrationCommand publish = RegistrationCommand.publish(1001L, 0L, "token", -1L);
    transition(publish);
    Assertions.assertFalse(ops.tableExists(IDENT));
    Assertions.assertEquals("RESERVED", state());
    outcome = RegistrationCommitVerifier.Result.ambiguous("synthetic uncertainty", null);
    transition(publish);
    Assertions.assertEquals("QUARANTINED", state());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                IDENT, TableChange.setProperty("delta.registration.internal.state", "PUBLISHED")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterTable(
                IDENT,
                TableChange.setProperty(RegistrationCommand.KEY, publish.encode()),
                TableChange.setProperty("committed", "true")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ops.alterTable(IDENT, TableChange.setProperty("committed", "true")));
    Map<String, String> properties =
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(5005L),
            Collections.singletonMap(
                RegistrationCommand.KEY,
                RegistrationCommand.reserve("token", location(), "schema").encode()));
    Map<String, String> hostileProperties = new HashMap<>(properties);
    hostileProperties.put("delta.registration.internal.state", "PUBLISHED");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ops.createTable(IDENT, new Column[0], null, hostileProperties));
    outcome = syntheticMatched(7L, "native-original", "actual");
    reopen(); // Simulated commit evidence available, publication not yet performed.
    Assertions.assertFalse(ops.tableExists(IDENT));
    transition(publish);
    Assertions.assertEquals("actual", ops.loadTable(IDENT).columns()[0].name());
    Assertions.assertEquals(1001L, raw().id());
    Assertions.assertEquals(1, ops.listTables(IDENT.namespace()).length);
    TableEntity published = raw();
    int before = verifications.get();
    reopen(); // Publication succeeded, response lost.
    transition(publish);
    Assertions.assertEquals(before, verifications.get());
    Assertions.assertEquals(published, raw());
  }

  @Test
  void testReplacementAndStaleGeneration() throws Exception {
    reserve(1001L, "token", "schema");
    outcome = syntheticMatched(0L, "first-uuid", "first");
    transition(RegistrationCommand.publish(1001L, 0L, "token", 0L));
    TableEntity first = raw();
    transition(RegistrationCommand.beginReplace(1001L, 0L, "token", "replacement", "new-schema"));
    Assertions.assertFalse(ops.tableExists(IDENT));
    Assertions.assertEquals("REPLACING", state());
    reopen();
    Assertions.assertEquals(first.columns(), raw().columns());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.publish(1001L, 0L, "token", 1L)));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.publish(9999L, 1L, "replacement", 1L)));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.publish(1001L, 1L, "other", 1L)));
    outcome = syntheticMatched(1L, "new-uuid", "replacement_column");
    transition(RegistrationCommand.publish(1001L, 1L, "replacement", 1L));
    Assertions.assertEquals(first.id(), raw().id());
    Assertions.assertEquals(first.auditInfo(), raw().auditInfo());
    Assertions.assertEquals(
        first.properties().get(RegistrationCommand.INTERNAL + "location"),
        raw().properties().get(RegistrationCommand.INTERNAL + "location"));
    Assertions.assertEquals(
        "new-uuid", raw().properties().get(RegistrationCommand.INTERNAL + "native.uuid"));
    Assertions.assertEquals("replacement_column", ops.loadTable(IDENT).columns()[0].name());
    TableEntity replacement = raw();
    transition(
        RegistrationCommand.beginReplace(
            1001L, 1L, "replacement", "failed-replace", "failed-schema"));
    outcome = RegistrationCommitVerifier.Result.notCommitted("synthetic verified absence");
    transition(RegistrationCommand.abort(1001L, 2L, "failed-replace"));
    Assertions.assertEquals("PUBLISHED", state());
    Assertions.assertEquals(replacement.columns(), raw().columns());
    Assertions.assertEquals(
        "new-uuid", raw().properties().get(RegistrationCommand.INTERNAL + "native.uuid"));
    Assertions.assertEquals(
        "2", raw().properties().get(RegistrationCommand.INTERNAL + "generation"));
    TableEntity restored = raw();
    int callsBeforeReplay = verifications.get();
    reopen();
    transition(RegistrationCommand.abort(1001L, 2L, "failed-replace"));
    Assertions.assertEquals(restored, raw());
    Assertions.assertEquals(callsBeforeReplay, verifications.get());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.publish(1001L, 1L, "replacement", 2L)));
  }

  @Test
  void testAbortAndAmbiguityNeverDelete() throws Exception {
    Path data = directory.resolve("native-data");
    Files.write(data, new byte[] {1, 2, 3});
    reserve(1001L, "token", "schema");
    actor = "intruder";
    Assertions.assertThrows(
        ForbiddenException.class, () -> transition(RegistrationCommand.abort(1001L, 0L, "token")));
    actor = "recovery";
    handoff = false;
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.abort(1001L, 0L, "token")));
    handoff = true;
    outcome = RegistrationCommitVerifier.Result.ambiguous("synthetic uncertain writer", null);
    transition(RegistrationCommand.abort(1001L, 0L, "token"));
    Assertions.assertEquals("QUARANTINED", state());
    reopen();
    Assertions.assertFalse(ops.tableExists(IDENT));
    outcome =
        RegistrationCommitVerifier.Result.notCommitted(
            "synthetic absent with explicit writer handoff");
    transition(RegistrationCommand.abort(1001L, 0L, "token"));
    Assertions.assertEquals("ABORTED", state());
    reopen();
    Assertions.assertEquals(1001L, raw().id());
    Assertions.assertEquals("ABORTED", state());
    Assertions.assertFalse(ops.tableExists(IDENT));
    Assertions.assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(data));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> ops.dropTable(IDENT));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> ops.purgeTable(IDENT));
    Assertions.assertEquals(
        1, store.list(IDENT.namespace(), TableEntity.class, Entity.EntityType.TABLE).size());
  }

  @Test
  void testOptimisticConflictDoesNotLeakCandidate() throws Exception {
    reserve(1001L, "token", "schema");
    CountDownLatch bothRead = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      List<Future<TableEntity>> futures =
          Arrays.asList(
              workers.submit(() -> raceUpdate("alpha", 3001L, bothRead, release)),
              workers.submit(() -> raceUpdate("beta", 3002L, bothRead, release)));
      Assertions.assertTrue(
          bothRead.await(20, TimeUnit.SECONDS),
          "Both actual backend callbacks must read the same base before either writes");
      release.countDown();
      int winners = 0;
      int losers = 0;
      String winner = null;
      for (Future<TableEntity> future : futures) {
        try {
          winner = future.get(20, TimeUnit.SECONDS).properties().get("winner");
          winners++;
        } catch (ExecutionException failure) {
          Assertions.assertInstanceOf(OptimisticLockException.class, failure.getCause());
          losers++;
        }
      }
      Assertions.assertEquals(1, winners);
      Assertions.assertEquals(1, losers);
      Assertions.assertEquals(winner, raw().properties().get("winner"));
      Assertions.assertEquals(1, raw().columns().size());
      Assertions.assertEquals(winner, raw().columns().get(0).name());
      reopen();
      Assertions.assertEquals(winner, raw().columns().get(0).name());
    } finally {
      release.countDown();
      workers.shutdownNow();
      Assertions.assertTrue(workers.awaitTermination(20, TimeUnit.SECONDS));
    }
  }

  @Test
  void testConditionalDeleteAndTransactionLimits() {
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> store.executeInTransaction(() -> null));
  }

  @Test
  void testCommandValidationAndVerificationRace() throws Exception {
    for (String invalid :
        Arrays.asList(
            "2|ABORT|1|0|dA",
            "1|UNKNOWN",
            "1|RESERVE|a|b",
            "1|ABORT|1|0|dA|extra",
            "1|PUBLISH|1|0|dA|-2")) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> RegistrationCommand.decode(invalid));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RegistrationCommand.reserve("", location(), "schema"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RegistrationCommand.reserve("token", "s3://bucket/a/../b", "schema"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RegistrationCommand.reserve("token", "s3://bucket/a?version=x", "schema"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RegistrationCommand.reserve(
                    String.join("", Collections.nCopies(17000, "x")), location(), "schema")
                .encode());
    reserve(1001L, "token", "schema");
    ops =
        new RegistrationLifecycleFixture(
            store,
            schemas,
            idGenerator,
            () -> actor,
            "recovery",
            () -> handoff,
            (inspected, candidate) -> {
              try {
                store.update(
                    IDENT,
                    TableEntity.class,
                    Entity.EntityType.TABLE,
                    current -> {
                      Map<String, String> properties = new HashMap<>(current.properties());
                      properties.put(RegistrationCommand.INTERNAL + "generation", "1");
                      return RegistrationLifecycleFixture.copy(
                          current, properties, current.columns());
                    });
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return syntheticMatched(1L, "synthetic-racing-uuid", "must_not_publish");
            });
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> transition(RegistrationCommand.publish(1001L, 0L, "token", 1L)));
    Assertions.assertEquals("RESERVED", state());
    Assertions.assertTrue(raw().columns().isEmpty());
  }

  @Test
  void testReserveRetryAfterPublication() throws Exception {
    reserve(1001L, "token", "schema");
    outcome = syntheticMatched(0L, "synthetic-uuid", "actual");
    transition(RegistrationCommand.publish(1001L, 0L, "token", 0L));
    TableEntity published = raw();
    reopen();
    Assertions.assertThrows(IllegalStateException.class, () -> reserve(9999L, "token", "schema"));
    Assertions.assertEquals(published, raw());
  }

  @Test
  void testStoredRecordCodecRejectsMalformed() throws Exception {
    reserve(1001L, "token", "schema");
    outcome = syntheticMatched(0L, "synthetic-uuid", "actual");
    transition(RegistrationCommand.publish(1001L, 0L, "token", 0L));
    TableEntity published = raw();
    for (String field :
        Arrays.asList(
            "location", "token", "actor", "schemaFingerprint", "native.uuid", "native.version")) {
      store.update(
          IDENT,
          TableEntity.class,
          Entity.EntityType.TABLE,
          current -> {
            Map<String, String> properties = new HashMap<>(published.properties());
            properties.remove(RegistrationCommand.INTERNAL + field);
            return RegistrationLifecycleFixture.copy(current, properties, current.columns());
          });
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> ops.loadTable(IDENT),
          "Missing stored " + field + " must fail closed");
    }
    for (String[] invalid :
        Arrays.asList(
            new String[] {"version", "2"},
            new String[] {"state", "UNKNOWN"},
            new String[] {"generation", "-1"})) {
      store.update(
          IDENT,
          TableEntity.class,
          Entity.EntityType.TABLE,
          current -> {
            Map<String, String> properties = new HashMap<>(published.properties());
            properties.put(RegistrationCommand.INTERNAL + invalid[0], invalid[1]);
            return RegistrationLifecycleFixture.copy(current, properties, current.columns());
          });
      Assertions.assertThrows(IllegalArgumentException.class, () -> ops.loadTable(IDENT));
    }
  }

  @Test
  void testObservationDoesNotGrantCorrelation() {
    List<Column> columns =
        new ArrayList<>(Collections.singletonList(Column.of("observed", Types.StringType.get())));
    RegistrationCommitVerifier.Observation observation =
        new RegistrationCommitVerifier.Observation(4L, null, "observed-schema", columns);
    columns.clear();
    Assertions.assertEquals("observed", observation.columns().get(0).name());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> observation.columns().clear());
    RegistrationCommitVerifier.Result result =
        RegistrationCommitVerifier.Result.ambiguous(
            "Public snapshot has no operation correlation", observation);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, result.kind());
    Assertions.assertEquals(4L, result.observation().nativeVersion());
    Assertions.assertNull(result.observation().nativeUuid());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> RegistrationCommitVerifier.Result.matched(observation, "untrusted lack of identity"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "state",
        "version",
        "generation",
        "token",
        "actor",
        "location",
        "schemaFingerprint",
        "request",
        "operation",
        "native.version",
        "native.uuid",
        "native.schemaFingerprint",
        "completedCommand",
        "reason"
      })
  void testMissingPriorSnapshotCannotMutateReplacement(String missingField) throws Exception {
    assertMalformedPriorRejected(missingField, null);
  }

  @ParameterizedTest
  @CsvSource({
    "state, RESERVED",
    "version, 2",
    "generation, 5",
    "token, replacement",
    "actor, foreign",
    "location, file:///tmp/foreign",
    "native.version, 7",
    "native.uuid, foreign",
    "native.schemaFingerprint, foreign",
    "request, 1|RESERVE|b3RoZXI|ZmlsZTovLy90bXAvZGVsdGE|c2NoZW1h",
    "operation, UNKNOWN",
    "completedCommand, invalid",
    "prior.state, PUBLISHED"
  })
  void testCorruptPriorSnapshotCannotMutateReplacement(String field, String corruptValue)
      throws Exception {
    assertMalformedPriorRejected(field, corruptValue);
  }

  @Test
  void testInvalidTransitionCandidateIsRejectedBeforePersistence() throws Exception {
    reserve(1001L, "token", "schema");
    TableEntity reservation = raw();
    outcome =
        RegistrationCommitVerifier.Result.matched(
            new RegistrationCommitVerifier.Observation(
                0L,
                "synthetic-uuid",
                "",
                Collections.singletonList(Column.of("must_not_publish", Types.StringType.get()))),
            "synthetic malformed native schema fingerprint");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> transition(RegistrationCommand.publish(1001L, 0L, "token", 0L)));
    Assertions.assertEquals(
        reservation, raw(), "Invalid candidate must not change the stored row before failing");
  }

  private void assertMalformedPriorRejected(String field, @Nullable String corruptValue)
      throws Exception {
    reserve(1001L, "token", "schema");
    outcome = syntheticMatched(0L, "original-uuid", "original_column");
    transition(RegistrationCommand.publish(1001L, 0L, "token", 0L));
    transition(RegistrationCommand.beginReplace(1001L, 0L, "token", "replacement", "new-schema"));
    TableEntity validReplacement = raw();
    outcome = RegistrationCommitVerifier.Result.notCommitted("synthetic verified absence");
    for (String pendingState : Arrays.asList("REPLACING", "QUARANTINED")) {
      store.update(
          IDENT,
          TableEntity.class,
          Entity.EntityType.TABLE,
          current -> {
            Map<String, String> properties = new HashMap<>(validReplacement.properties());
            properties.put(RegistrationCommand.INTERNAL + "state", pendingState);
            String key = RegistrationCommand.INTERNAL + "prior." + field;
            if (corruptValue == null) {
              properties.remove(key);
            } else {
              properties.put(key, corruptValue);
            }
            return RegistrationLifecycleFixture.copy(
                current, properties, validReplacement.columns());
          });
      TableEntity malformed = raw();
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> transition(RegistrationCommand.abort(1001L, 1L, "replacement")),
          "Malformed " + field + " in " + pendingState + " must reject abort");
      Assertions.assertEquals(
          malformed, raw(), "Rejected abort must retain all malformed record fields for recovery");
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> transition(RegistrationCommand.publish(1001L, 1L, "replacement", 1L)),
          "Malformed snapshot must also reject publication");
      Assertions.assertEquals(malformed, raw());
      reopen();
      Assertions.assertEquals(
          malformed, raw(), "Neither rejected request may persist a changed row");
    }
  }

  private TableEntity raceUpdate(
      String label, long columnId, CountDownLatch bothRead, CountDownLatch release)
      throws IOException {
    return store.update(
        IDENT,
        TableEntity.class,
        Entity.EntityType.TABLE,
        current -> {
          Assertions.assertNull(current.properties().get("winner"));
          bothRead.countDown();
          try {
            Assertions.assertTrue(release.await(20, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          Map<String, String> properties = new HashMap<>(current.properties());
          properties.put("winner", label);
          ColumnEntity column =
              ColumnEntity.toColumnEntity(
                  Column.of(label, Types.StringType.get()), 0, columnId, current.auditInfo());
          return RegistrationLifecycleFixture.copy(
              current, properties, Collections.singletonList(column));
        });
  }

  private RegistrationCommitVerifier.Result syntheticMatched(
      long version, String uuid, String column) {
    return RegistrationCommitVerifier.Result.matched(
        new RegistrationCommitVerifier.Observation(
            version,
            uuid,
            "synthetic-schema",
            Collections.singletonList(Column.of(column, Types.StringType.get()))),
        "SYNTHETIC operation correlation; no native IO");
  }

  private Table reserve(long id, String token, String schema) {
    return create(id, RegistrationCommand.reserve(token, location(), schema));
  }

  private Table create(long id, RegistrationCommand command) {
    return ops.createTable(
        IDENT,
        new Column[0],
        null,
        StringIdentifier.newPropertiesWithId(
            StringIdentifier.fromId(id),
            Collections.singletonMap(RegistrationCommand.KEY, command.encode())));
  }

  private Table transition(RegistrationCommand command) {
    return ops.alterTable(
        IDENT, TableChange.setProperty(RegistrationCommand.KEY, command.encode()));
  }

  private String location() {
    return directory.resolve("delta").toUri().toString();
  }

  private String state() throws IOException {
    return raw().properties().get(RegistrationCommand.INTERNAL + "state");
  }

  private void reopen() throws Exception {
    store.close();
    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    openOperations();
    Assertions.assertEquals(
        "NoOpsCache", FieldUtils.readField(store, "cache", true).getClass().getSimpleName());
  }

  private TableEntity raw() throws IOException {
    return store.get(IDENT, Entity.EntityType.TABLE, TableEntity.class);
  }

  private void openOperations() {
    schemas =
        new ManagedSchemaOperations() {
          /** {@inheritDoc} */
          @Override
          protected EntityStore store() {
            return store;
          }
        };
    ops =
        new RegistrationLifecycleFixture(
            store,
            schemas,
            idGenerator,
            () -> actor,
            "recovery",
            () -> handoff,
            (entity, version) -> {
              verifications.incrementAndGet();
              return outcome;
            });
  }
}
