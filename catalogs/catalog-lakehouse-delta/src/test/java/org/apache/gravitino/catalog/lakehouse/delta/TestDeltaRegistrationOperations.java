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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataException;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Real relational lifecycle and actual-reader protocol-shaped fixture tests. */
public class TestDeltaRegistrationOperations {
  static final ObjectMapper JSON = new ObjectMapper();
  static final NameIdentifier IDENT = NameIdentifier.of("m", "c", "s", "t");
  static final String SCHEMA =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
  static final AuditInfo AUDIT =
      AuditInfo.builder()
          .withCreator("alice")
          .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
          .build();
  @TempDir Path temp;
  RelationalEntityStore store;
  ManagedSchemaOperations schemas;
  DeltaMetadataReader reader;
  DeltaRegistrationOperations operations;
  DeltaCatalogConfig config;
  DeltaAuthorization authorization;
  GravitinoAuthorizer authorizer;
  OwnerDispatcher owners;
  Config authConfig;
  final AtomicReference<String> actor = new AtomicReference<>("alice");
  final AtomicLong ids = new AtomicLong(1000);
  CountDownLatch entered;
  CountDownLatch resumed;
  ExecutorService executor;

  @BeforeEach
  void setup() throws Exception {
    Config storage = new Config(false) {};
    storage.set(Configs.ENTITY_STORE, Configs.RELATIONAL_ENTITY_STORE);
    storage.set(Configs.ENTITY_RELATIONAL_STORE, Configs.DEFAULT_ENTITY_RELATIONAL_STORE);
    storage.set(Configs.CACHE_ENABLED, false);
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH, temp.resolve("store").toString());
    storage.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        "jdbc:h2:file:" + temp.resolve("store") + ";DB_CLOSE_DELAY=0;MODE=MYSQL;AUTO_SERVER=FALSE");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "sa");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "");
    store = new RelationalEntityStore();
    store.initialize(storage);
    store.put(
        BaseMetalake.builder()
            .withId(1L)
            .withName("m")
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        CatalogEntity.builder()
            .withId(2L)
            .withName("c")
            .withNamespace(Namespace.of("m"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("lakehouse-delta")
            .withProperties(Map.of())
            .withAuditInfo(AUDIT)
            .build(),
        false);
    store.put(
        SchemaEntity.builder()
            .withId(3L)
            .withName("s")
            .withNamespace(Namespace.of("m", "c"))
            .withProperties(Map.of())
            .withAuditInfo(AUDIT)
            .build(),
        false);
    schemas =
        new ManagedSchemaOperations() {
          @Override
          protected EntityStore store() {
            return store;
          }
        };
    authConfig = new Config(false) {};
    authConfig.set(Configs.ENABLE_AUTHORIZATION, true);
    GravitinoEnv env = mock(GravitinoEnv.class);
    authorizer = mock(GravitinoAuthorizer.class);
    owners = mock(OwnerDispatcher.class);
    when(env.config()).thenReturn(authConfig);
    when(env.gravitinoAuthorizer()).thenReturn(authorizer);
    when(env.internalOwnerDispatcher()).thenReturn(owners);
    when(env.internalAccessControlDispatcher()).thenReturn(mock(AccessControlDispatcher.class));
    when(owners.getOwner(anyString(), any()))
        .thenAnswer(
            call -> {
              List<UserEntity> persisted =
                  store
                      .relationOperations()
                      .listEntitiesByRelation(
                          SupportsRelationOperations.Type.OWNER_REL,
                          IDENT,
                          Entity.EntityType.TABLE);
              if (persisted.isEmpty()) return Optional.empty();
              Owner value = mock(Owner.class);
              when(value.name()).thenReturn(persisted.get(0).name());
              when(value.type()).thenReturn(Owner.Type.USER);
              return Optional.of(value);
            });
    authorization = new DeltaAuthorization(() -> env, () -> () -> actor.get());
    config =
        DeltaCatalogConfig.from(
            Map.of(
                "delta.warehouse",
                "s3://bucket/fixture",
                "delta.storage-credential-mode",
                "instance",
                "delta.recovery-principals",
                "[\"recovery\"]",
                "delta.adoption-roots",
                "[\"s3://bucket/fixture\"]"));
    Configuration hadoop = new Configuration();
    hadoop.set("fs.s3a.impl", FixtureFileSystem.class.getName());
    hadoop.set("delta.fixture.root", temp.resolve("native").toString());
    reader = new DeltaMetadataReader(hadoop);
    operations = operations(reader, store);
  }

  @AfterEach
  void cleanup() throws Exception {
    if (resumed != null) resumed.countDown();
    if (executor != null) {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
    if (reader != null) reader.close();
    if (store != null) store.close();
  }

  @Test
  void reserveReplayAndLostReplyStatusAreDurableWithoutNativeIo() throws Exception {
    DeltaRegistrationCommand command = DeltaRegistrationCommand.reserve("token", "run-a", SCHEMA);
    DeltaRegistrationReceipt first = receipt(create(command, 10));
    assertEquals(10L, first.identity().id());
    assertEquals("RESERVED", first.state().name());
    assertEquals("s3a://bucket/fixture/s/10", first.marker().orElseThrow().normalizedLocation());
    assertEquals(10L, receipt(create(command, 11)).identity().id());
    assertEquals(
        10L,
        receipt(alter(DeltaRegistrationCommand.statusByToken("token", "run-a"))).identity().id());
    assertThrows(IllegalStateException.class, () -> operations.loadTable(IDENT));
    assertEquals(0, operations.listTables(IDENT.namespace()).length);
    assertEquals(1000L, ids.get());
    owner("governance-owner");
    assertThrows(SecurityException.class, () -> create(command, 14));
    assertThrows(
        RuntimeException.class,
        () -> create(DeltaRegistrationCommand.reserve("other", "run-a", SCHEMA), 12));
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.statusByToken("token", "wrong")));
    actor.set("bob");
    assertThrows(SecurityException.class, () -> create(command, 13));
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.statusByToken("token", "run-a")));
    when(authorizer.isServiceAdmin()).thenReturn(true);
    assertEquals(
        10L, receipt(alter(DeltaRegistrationCommand.administrativeStatus())).identity().id());
  }

  @Test
  void actualReaderCreateRefreshCompletedReplayAndListHydration() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    writeCommit(
        reserved.marker().orElseThrow().normalizedLocation(),
        0,
        "native-a",
        SCHEMA,
        Map.of(
            DeltaOperationMarker.PROPERTY_KEY,
            reserved.marker().orElseThrow().encode(),
            "vendor.secret",
            "never-persist"));
    TableEntity beforeOwner = rawTable(IDENT);
    assertEquals(
        "OWNER_INITIALIZATION_REQUIRED",
        assertThrows(
                IllegalStateException.class,
                () -> alter(DeltaRegistrationCommand.publish(reserved.identity())))
            .getMessage());
    assertEquals(beforeOwner, rawTable(IDENT));
    owner("alice");
    DeltaRegistrationReceipt published =
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity())));
    assertEquals("EXACT_NATIVE_COMMIT", published.evidence().name());
    long columnId = rawTable(IDENT).columns().get(0).id();
    String expanded =
        SCHEMA.replace(
            "}]}", "},{\"name\":\"added\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}");
    writeCommit(
        published.location().orElseThrow(),
        1,
        "native-a",
        expanded,
        Map.of("vendor.secret", "later-secret"));
    assertEquals(2, operations.loadTable(IDENT).columns().length);
    assertEquals(columnId, rawTable(IDENT).columns().get(0).id());
    assertEquals("alice", rawTable(IDENT).columns().get(0).auditInfo().creator());
    assertEquals(1, operations.listTables(IDENT.namespace()).length);
    TableEntity refreshed = rawTable(IDENT);
    assertEquals(
        1L,
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity())))
            .nativeVersion()
            .orElseThrow());
    assertEquals(refreshed, rawTable(IDENT));
    assertFalse(rawTable(IDENT).properties().toString().contains("secret"));
    assertFalse(operations.loadTable(IDENT).properties().toString().contains("secret"));
    assertThrows(
        RuntimeException.class,
        () -> create(DeltaRegistrationCommand.reserve("token", "run-a", SCHEMA), 11));
    assertThrows(UnsupportedOperationException.class, () -> operations.dropTable(IDENT));
    assertThrows(UnsupportedOperationException.class, () -> operations.purgeTable(IDENT));
  }

  @Test
  void replacementActorTurnoverAndExactNativeReplacement() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    owner("governance-owner");
    actor.set("bob");
    DeltaRegistrationReceipt replacing =
        receipt(
            alter(
                DeltaRegistrationCommand.beginReplace(
                    published.identity(), "next", "run-b", SCHEMA)));
    assertEquals(1L, replacing.identity().generation());
    assertEquals("bob", DeltaRegistrationState.from(rawTable(IDENT)).actor());
    actor.set("alice");
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.publish(replacing.identity())));
    actor.set("bob");
    DeltaOperationMarker marker = replacing.marker().orElseThrow();
    writeCommit(
        marker.normalizedLocation(),
        1,
        "native-b",
        SCHEMA,
        Map.of(DeltaOperationMarker.PROPERTY_KEY, marker.encode()));
    DeltaRegistrationReceipt result =
        receipt(alter(DeltaRegistrationCommand.publish(replacing.identity())));
    assertEquals("native-b", result.nativeId().orElseThrow());
    assertEquals(10L, result.identity().id());
    assertEquals("governance-owner", persistedOwner());
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.publish(published.identity())));
    verify(owners, never()).setOwner(any(), any(), any(), any());
  }

  @Test
  void refreshCannotOverwriteConcurrentReplacement() throws Exception {
    publishCreate();
    TableEntity before = rawTable(IDENT);
    CompletableFuture<Table> loading = loadWithPausedNativeRead(IDENT);
    actor.set("bob");
    alter(DeltaRegistrationCommand.beginReplace(identity(before), "next", "run-b", SCHEMA));
    resumeNativeRead();
    assertThrows(CompletionException.class, loading::join);
    DeltaRegistrationState current = DeltaRegistrationState.from(rawTable(IDENT));
    assertEquals("REPLACING", current.receipt().state().name());
    assertEquals("next", current.receipt().identity().token());
    assertEquals("bob", current.actor());
  }

  @Test
  void publishedIdentityDriftCannotEnterPendingRecovery() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    writeCommit(published.location().orElseThrow(), 1, "unexpected", SCHEMA, Map.of());
    assertThrows(RuntimeException.class, () -> operations.loadTable(IDENT));
    assertEquals(
        "QUARANTINED",
        receipt(alter(DeltaRegistrationCommand.status(published.identity()))).state().name());
    when(authorizer.isServiceAdmin()).thenReturn(true);
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.handoff(published.identity(), "run-a", "drained")));
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.publish(published.identity())));
    assertThrows(
        RuntimeException.class, () -> alter(DeltaRegistrationCommand.abort(published.identity())));
    assertEquals(0, operations.listTables(IDENT.namespace()).length);
  }

  @Test
  void handoffRequiresCurrentAuthorizationAndAbortRetainsName() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    actor.set("recovery");
    authConfig.set(Configs.ENABLE_AUTHORIZATION, false);
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.handoff(reserved.identity(), "run-a", "drained")));
    authConfig.set(Configs.ENABLE_AUTHORIZATION, true);
    assertThrows(
        RuntimeException.class, () -> alter(DeltaRegistrationCommand.abort(reserved.identity())));
    alter(DeltaRegistrationCommand.handoff(reserved.identity(), "run-a", "drained"));
    actor.set("alice");
    owner("alice");
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.publish(reserved.identity())));
    actor.set("recovery");
    DeltaRegistrationReceipt abandoned =
        receipt(alter(DeltaRegistrationCommand.abort(reserved.identity())));
    assertEquals("ABANDONED", abandoned.state().name());
    TableEntity saved = rawTable(IDENT);
    alter(DeltaRegistrationCommand.abort(reserved.identity()));
    assertEquals(saved, rawTable(IDENT));
    actor.set("alice");
    assertThrows(
        RuntimeException.class,
        () -> create(DeltaRegistrationCommand.reserve("new", "new-run", SCHEMA), 20));
  }

  @Test
  void adoptionPinsCompleteBaselineAndRequiresCurrentPrivilege() throws Exception {
    String location = "s3a://bucket/fixture/adopt";
    writeCommit(location, 0, "adopt-id", SCHEMA, Map.of("vendor.secret", "private"));
    when(authorizer.isServiceAdmin()).thenReturn(true);
    DeltaRegistrationReceipt adopting =
        receipt(
            create(DeltaRegistrationCommand.adopt("adopt", location, "retired", "cutover"), 10));
    assertTrue(adopting.marker().isEmpty());
    owner("alice");
    when(authorizer.isServiceAdmin()).thenReturn(false);
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.publish(adopting.identity())));
    when(authorizer.isServiceAdmin()).thenReturn(true);
    DeltaRegistrationReceipt adopted =
        receipt(alter(DeltaRegistrationCommand.publish(adopting.identity())));
    assertEquals("EXISTING_NATIVE_SNAPSHOT", adopted.evidence().name(), adopted.reason());
    assertFalse(rawTable(IDENT).properties().toString().contains("private"));
  }

  @Test
  void adoptionDigestDriftQuarantinesAndMalformedMarkerIsRejectedBeforeInsert() throws Exception {
    String location = "s3a://bucket/fixture/adopt";
    when(authorizer.isServiceAdmin()).thenReturn(true);
    writeCommit(
        location, 0, "adopt-id", SCHEMA, Map.of(DeltaOperationMarker.PROPERTY_KEY, "malformed"));
    assertThrows(
        RuntimeException.class,
        () -> create(DeltaRegistrationCommand.adopt("adopt", location, "retired", "cutover"), 10));
    writeCommit(location, 0, "adopt-id", SCHEMA, Map.of("vendor.secret", "before"));
    DeltaRegistrationReceipt adopting =
        receipt(
            create(DeltaRegistrationCommand.adopt("adopt", location, "retired", "cutover"), 10));
    owner("alice");
    writeCommit(location, 0, "adopt-id", SCHEMA, Map.of("vendor.secret", "after"));
    assertEquals(
        "ADOPTION_BASELINE_CHANGED",
        receipt(alter(DeltaRegistrationCommand.publish(adopting.identity()))).reason());
  }

  @Test
  void transientListReadFailsInsteadOfHidingPublishedEntry() throws Exception {
    publishCreate();
    DeltaMetadataReader failed = mock(DeltaMetadataReader.class);
    when(failed.latest(anyString())).thenThrow(mock(DeltaMetadataException.class));
    assertThrows(
        RuntimeException.class, () -> operations(failed, store).listTables(IDENT.namespace()));
    assertEquals(
        "PUBLISHED", DeltaRegistrationState.from(rawTable(IDENT)).receipt().state().name());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void inFlightPublicationCannotEraseHandoffOrAbandonment(boolean abort) throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    DeltaOperationMarker marker = reserved.marker().orElseThrow();
    writeCommit(
        marker.normalizedLocation(),
        0,
        "native-a",
        SCHEMA,
        Map.of(DeltaOperationMarker.PROPERTY_KEY, marker.encode()));
    owner("alice");
    entered = new CountDownLatch(1);
    resumed = new CountDownLatch(1);
    DeltaMetadataReader paused = mock(DeltaMetadataReader.class);
    when(paused.verify(any()))
        .thenAnswer(
            call -> {
              DeltaCommitEvidence evidence = reader.verify(call.getArgument(0));
              entered.countDown();
              if (!resumed.await(20, TimeUnit.SECONDS)) throw new AssertionError("resume timeout");
              return evidence;
            });
    when(paused.atVersion(anyString(), anyLong()))
        .thenAnswer(call -> reader.atVersion(call.getArgument(0), call.getArgument(1)));
    executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Table> publishing =
        CompletableFuture.supplyAsync(
            () ->
                operations(paused, store)
                    .alterTable(
                        IDENT,
                        TableChange.setProperty(
                            DeltaRegistrationCommand.COMMAND_PROPERTY,
                            DeltaRegistrationCommand.publish(reserved.identity()).encode())),
            executor);
    assertTrue(entered.await(20, TimeUnit.SECONDS));
    actor.set("recovery");
    alter(DeltaRegistrationCommand.handoff(reserved.identity(), "run-a", "controller-drained"));
    if (abort) alter(DeltaRegistrationCommand.abort(reserved.identity()));
    TableEntity retained = rawTable(IDENT);
    resumed.countDown();
    assertThrows(CompletionException.class, publishing::join);
    assertEquals(retained, rawTable(IDENT));
    assertEquals(
        abort ? "ABANDONED" : "RESERVED",
        DeltaRegistrationState.from(retained).receipt().state().name());
  }

  @Test
  void recoveryPublicationUsesExactEvidenceAndPreservesOriginalActor() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    owner("alice");
    assertEquals(
        "NATIVE_READ_UNAVAILABLE",
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity()))).reason());
    assertThrows(
        SecurityException.class,
        () -> alter(DeltaRegistrationCommand.publish(reserved.identity())));
    actor.set("recovery");
    alter(DeltaRegistrationCommand.handoff(reserved.identity(), "run-a", "drained"));
    DeltaOperationMarker marker = reserved.marker().orElseThrow();
    writeCommit(
        marker.normalizedLocation(),
        0,
        "native-a",
        SCHEMA,
        Map.of(DeltaOperationMarker.PROPERTY_KEY, marker.encode()));
    assertEquals(
        "PUBLISHED",
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity()))).state().name());
    DeltaRegistrationState state = DeltaRegistrationState.from(rawTable(IDENT));
    assertEquals("alice", state.actor());
    assertEquals("recovery", state.data().get("recoveryActor").asText());
    assertEquals("alice", persistedOwner());
  }

  @Test
  void staleIdentityFieldsAndHandoffRunNeverAuthorizeMutation() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    for (DeltaRegistrationIdentity stale :
        List.of(
            DeltaRegistrationIdentity.of(11, 0, "token"),
            DeltaRegistrationIdentity.of(10, 1, "token"),
            DeltaRegistrationIdentity.of(10, 0, "other"))) {
      assertThrows(RuntimeException.class, () -> alter(DeltaRegistrationCommand.publish(stale)));
      assertThrows(RuntimeException.class, () -> alter(DeltaRegistrationCommand.status(stale)));
    }
    actor.set("recovery");
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.handoff(reserved.identity(), "wrong", "drained")));
    assertEquals("RESERVED", DeltaRegistrationState.from(rawTable(IDENT)).receipt().state().name());
  }

  @Test
  void replacementAbortWithUnavailableLogNeverRestoresOldPublication() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    DeltaRegistrationReceipt replacing =
        receipt(
            alter(
                DeltaRegistrationCommand.beginReplace(
                    published.identity(), "next", "run-b", SCHEMA)));
    actor.set("recovery");
    alter(DeltaRegistrationCommand.handoff(replacing.identity(), "run-b", "drained"));
    assertEquals(
        "ABANDONED",
        receipt(alter(DeltaRegistrationCommand.abort(replacing.identity()))).state().name());
    assertThrows(RuntimeException.class, () -> operations.loadTable(IDENT));
    assertThrows(
        RuntimeException.class,
        () -> alter(DeltaRegistrationCommand.publish(published.identity())));
    assertEquals("alice", persistedOwner());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sameVersionPrivateConfigurationDriftCannotRefreshOrBecomeReplacementBase(boolean replace)
      throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    DeltaRegistrationState state = DeltaRegistrationState.from(rawTable(IDENT));
    writeCommit(
        published.location().orElseThrow(),
        0,
        "native-a",
        SCHEMA,
        Map.of(
            DeltaOperationMarker.PROPERTY_KEY,
            state.marker().encode(),
            "vendor.secret",
            "new-secret"));
    if (replace) {
      assertEquals(
          "NATIVE_METADATA_INCONSISTENT",
          receipt(
                  alter(
                      DeltaRegistrationCommand.beginReplace(
                          published.identity(), "next", "run-b", SCHEMA)))
              .reason());
    } else {
      assertThrows(RuntimeException.class, () -> operations.loadTable(IDENT));
    }
    assertEquals(
        "QUARANTINED", DeltaRegistrationState.from(rawTable(IDENT)).receipt().state().name());
    assertFalse(rawTable(IDENT).properties().toString().contains("new-secret"));
  }

  @Test
  void refreshStopsAfterThreeReloadsOnCompleteRowConflict() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    writeCommit(published.location().orElseThrow(), 1, "native-a", SCHEMA, Map.of());
    EntityStore conflicting = mock(EntityStore.class, delegatesTo(store));
    AtomicInteger conflicts = new AtomicInteger();
    doAnswer(
            call -> {
              int conflict = conflicts.incrementAndGet();
              store.update(
                  IDENT,
                  TableEntity.class,
                  Entity.EntityType.TABLE,
                  current -> {
                    TableEntity detached = DeltaRegistrationState.detach(current);
                    return TableEntity.builder()
                        .withId(detached.id())
                        .withName(detached.name())
                        .withNamespace(detached.namespace())
                        .withColumns(detached.columns())
                        .withProperties(detached.properties())
                        .withComment(detached.comment())
                        .withAuditInfo(
                            AuditInfo.builder()
                                .withCreator("alice")
                                .withCreateTime(detached.auditInfo().createTime())
                                .withLastModifier("conflict-" + conflict)
                                .build())
                        .build();
                  });
              Function<TableEntity, TableEntity> updater = call.getArgument(3);
              return store.update(IDENT, TableEntity.class, Entity.EntityType.TABLE, updater);
            })
        .when(conflicting)
        .update(eq(IDENT), eq(TableEntity.class), eq(Entity.EntityType.TABLE), any());
    assertThrows(
        IllegalStateException.class, () -> operations(reader, conflicting).loadTable(IDENT));
    assertEquals(4, conflicts.get());
    assertEquals(
        0L, DeltaRegistrationState.from(rawTable(IDENT)).receipt().nativeVersion().orElseThrow());
    assertEquals("conflict-4", rawTable(IDENT).auditInfo().lastModifier());
  }

  @Test
  void durableStoreReopensWithTheSameReservation() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    TableEntity original = rawTable(IDENT);
    store.close();
    store = new RelationalEntityStore();
    Config storage = new Config(false) {};
    storage.set(Configs.CACHE_ENABLED, false);
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH, temp.resolve("store").toString());
    storage.set(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        "jdbc:h2:file:" + temp.resolve("store") + ";DB_CLOSE_DELAY=0;MODE=MYSQL;AUTO_SERVER=FALSE");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, "sa");
    storage.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, "");
    store.initialize(storage);
    operations = operations(reader, store);
    assertEquals(original, rawTable(IDENT));
    assertEquals(
        reserved.identity(),
        receipt(alter(DeltaRegistrationCommand.statusByToken("token", "run-a"))).identity());
  }

  @ParameterizedTest
  @ValueSource(strings = {"description", "createdTime", "partitions", "protocol", "schema", "uuid"})
  void completeSameVersionTupleDriftQuarantines(String field) throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    mutateCommit(
        published.location().orElseThrow(),
        0,
        envelope -> {
          ObjectNode metadata = (ObjectNode) envelope.get("metaData");
          if (metadata != null)
            switch (field) {
              case "description":
                metadata.put("description", "");
                break;
              case "createdTime":
                metadata.remove("createdTime");
                break;
              case "partitions":
                metadata.putArray("partitionColumns").add("id");
                break;
              case "schema":
                metadata.put(
                    "schemaString",
                    SCHEMA.replace("\"metadata\":{}", "\"metadata\":{\"extra\":\"value\"}"));
                break;
              case "uuid":
                metadata.put("id", "unexpected");
                break;
              default:
                break;
            }
          if (field.equals("protocol") && envelope.has("protocol"))
            ((ObjectNode) envelope.get("protocol")).put("minWriterVersion", 3);
        });
    assertThrows(RuntimeException.class, () -> operations.loadTable(IDENT));
    assertEquals(
        field.equals("uuid") ? "NATIVE_ID_CHANGED" : "NATIVE_METADATA_INCONSISTENT",
        DeltaRegistrationState.from(rawTable(IDENT)).receipt().reason());
  }

  @Test
  void beginReplaceCapturesValidatedHigherNativeBase() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    writeCommit(published.location().orElseThrow(), 1, "native-a", SCHEMA, Map.of());
    DeltaRegistrationReceipt replacing =
        receipt(
            alter(
                DeltaRegistrationCommand.beginReplace(
                    published.identity(), "next", "run-b", SCHEMA)));
    DeltaOperationMarker marker = replacing.marker().orElseThrow();
    assertEquals(1L, marker.baseVersion().orElseThrow());
    assertEquals("native-a", marker.baseUuid().orElseThrow());
    writeCommit(marker.normalizedLocation(), 2, "native-b", SCHEMA, marker.injectInto(Map.of()));
    assertEquals(
        2L,
        receipt(alter(DeltaRegistrationCommand.publish(replacing.identity())))
            .nativeVersion()
            .orElseThrow());
  }

  @Test
  void v2MappedCreateAndReplacementUseActualReaderAndFreshColumnIdentity() throws Exception {
    DeltaSchemaContract contract =
        DeltaSchemaContract.spark35Delta33NameMapping(
            DeltaSchemaContract.OperationMode.SCHEMA_ONLY, SCHEMA);
    DeltaRegistrationReceipt reserved =
        receipt(create(DeltaRegistrationCommand.reserve("mapped", "run-a", contract), 10));
    DeltaOperationMarker marker = reserved.marker().orElseThrow();
    assertTrue(marker.schemaContract().isPresent());
    writeMappedCommit(marker, "mapped-a");
    owner("alice");
    DeltaRegistrationReceipt published =
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity())));
    assertEquals("PUBLISHED", published.state().name(), published.reason());
    long priorColumn = rawTable(IDENT).columns().get(0).id();
    DeltaRegistrationReceipt replacing =
        receipt(
            alter(
                DeltaRegistrationCommand.beginReplace(
                    published.identity(), "mapped-next", "run-b", contract)));
    assertTrue(replacing.marker().orElseThrow().schemaContract().isPresent());
    writeMappedCommit(replacing.marker().orElseThrow(), "mapped-b");
    assertEquals(
        "PUBLISHED",
        receipt(alter(DeltaRegistrationCommand.publish(replacing.identity()))).state().name());
    assertNotEquals(priorColumn, rawTable(IDENT).columns().get(0).id());
  }

  @Test
  void generationOverflowCannotOpenReplacementOrMutateRow() throws Exception {
    publishCreate();
    DeltaRegistrationState original = DeltaRegistrationState.from(rawTable(IDENT));
    DeltaRegistrationIdentity predecessor =
        DeltaRegistrationIdentity.of(10, Long.MAX_VALUE - 1, "penultimate");
    DeltaRegistrationCommand opening =
        DeltaRegistrationCommand.beginReplace(predecessor, "last", "run-last", SCHEMA);
    DeltaOperationMarker marker =
        DeltaOperationMarker.replace(
            10, Long.MAX_VALUE, "last", original.location(), SCHEMA, 0, "old-native-id");
    ObjectNode data =
        original
            .data()
            .put("kind", "BEGIN_REPLACE")
            .put("generation", Long.MAX_VALUE)
            .put("token", "last")
            .put("run", "run-last")
            .put("opening", opening.encode())
            .put("marker", marker.encode());
    writeCommit(original.location(), 1, "native-a", SCHEMA, Map.of());
    data.set(
        "observation",
        DeltaRegistrationState.observation(
            DeltaNativeObservation.from(reader.latest(original.location()))));
    DeltaRegistrationState maximum = DeltaRegistrationState.with(original.entity(), data);
    new DeltaRegistrationStore(store).replace(original, maximum.entity());
    TableEntity before = rawTable(IDENT);
    assertThrows(
        ArithmeticException.class,
        () ->
            alter(
                DeltaRegistrationCommand.beginReplace(
                    maximum.receipt().identity(), "overflow", "run-overflow", SCHEMA)));
    assertEquals(before, rawTable(IDENT));
  }

  @Test
  void invalidPersistedSafeValueNeverBecomesPublicMetadata() throws Exception {
    publishCreate();
    TableEntity row = rawTable(IDENT);
    ObjectNode data = DeltaRegistrationState.from(row).data();
    ((ObjectNode) data.get("observation").get("safe")).put("delta.appendOnly", "not-a-boolean");
    row.properties().put(DeltaRegistrationState.PROPERTY, data.toString());
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row));
  }

  @Test
  void adoptionUuidDriftCannotPublishOrBecomeAutomaticRecoveryRebinding() throws Exception {
    String location = "s3a://bucket/fixture/adopt";
    when(authorizer.isServiceAdmin()).thenReturn(true);
    writeCommit(location, 0, "adopt-a", SCHEMA, Map.of());
    DeltaRegistrationReceipt adopting =
        receipt(
            create(DeltaRegistrationCommand.adopt("adopt", location, "retired", "cutover"), 10));
    owner("alice");
    writeCommit(location, 1, "adopt-b", SCHEMA, Map.of());
    assertEquals(
        "ADOPTION_BASELINE_CHANGED",
        receipt(alter(DeltaRegistrationCommand.publish(adopting.identity()))).reason());
    alter(DeltaRegistrationCommand.handoff(adopting.identity(), "retired", "drained"));
    assertEquals(
        "ADOPTION_BASELINE_CHANGED",
        receipt(alter(DeltaRegistrationCommand.publish(adopting.identity()))).reason());
    assertEquals(
        "QUARANTINED", DeltaRegistrationState.from(rawTable(IDENT)).receipt().state().name());
  }

  private void writeMappedCommit(DeltaOperationMarker marker, String uuid) throws Exception {
    String physical =
        marker.operation() == DeltaOperationMarker.Operation.CREATE
            ? "col-12345678-1234-1234-1234-123456789abc"
            : "col-12345678-1234-1234-1234-123456789abd";
    String mapped =
        SCHEMA.replace(
            "\"metadata\":{}",
            "\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\""
                + physical
                + "\"}");
    long version = marker.baseVersion().isPresent() ? marker.baseVersion().getAsLong() + 1 : 0;
    writeCommit(
        marker.normalizedLocation(),
        version,
        uuid,
        mapped,
        marker.injectInto(
            Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "1")));
    mutateCommit(
        marker.normalizedLocation(),
        version,
        envelope -> {
          if (envelope.has("protocol"))
            ((ObjectNode) envelope.get("protocol"))
                .put("minReaderVersion", 2)
                .put("minWriterVersion", 5);
        });
  }

  private void mutateCommit(String location, long version, Consumer<ObjectNode> mutation)
      throws Exception {
    Path commit =
        temp.resolve("native")
            .resolve(URI.create(location).getPath().substring("/fixture/".length()))
            .resolve("_delta_log")
            .resolve(String.format(Locale.ROOT, "%020d.json", version));
    List<String> actions = new ArrayList<>();
    for (String line : Files.readAllLines(commit)) {
      ObjectNode envelope = (ObjectNode) JSON.readTree(line);
      mutation.accept(envelope);
      actions.add(envelope.toString());
    }
    Files.writeString(commit, String.join("\n", actions) + "\n");
  }

  @Test
  void corruptedReplacementBaseAndInventedPublicationAreRejected() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    alter(DeltaRegistrationCommand.beginReplace(published.identity(), "next", "run-b", SCHEMA));
    DeltaRegistrationState pending = DeltaRegistrationState.from(rawTable(IDENT));
    for (String field : List.of("uuid", "version", "publication")) {
      TableEntity row = pending.entity();
      ObjectNode data = pending.data();
      if (field.equals("publication"))
        data.put("state", "PUBLISHED").put("evidence", "EXACT_NATIVE_COMMIT");
      else if (field.equals("uuid"))
        ((ObjectNode) data.get("observation")).put("uuid", "not-the-base");
      else ((ObjectNode) data.get("observation")).put("version", 1L);
      row.properties().put(DeltaRegistrationState.PROPERTY, data.toString());
      assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row), field);
    }
  }

  @Test
  void listKeepsPublishedTableAndOmitsCoexistingPendingRegistration() throws Exception {
    publishCreate();
    NameIdentifier pending = NameIdentifier.of("m", "c", "s", "pending");
    operations.createTable(
        pending,
        new Column[0],
        null,
        Map.of(
            DeltaRegistrationCommand.COMMAND_PROPERTY,
            DeltaRegistrationCommand.reserve("pending", "run-pending", SCHEMA).encode(),
            StringIdentifier.ID_KEY,
            StringIdentifier.fromId(20).toString()),
        new Transform[0],
        null,
        null,
        null);
    NameIdentifier[] listed = operations.listTables(IDENT.namespace());
    assertEquals(1, listed.length);
    assertEquals(IDENT, listed[0]);
    assertEquals(
        "RESERVED", DeltaRegistrationState.from(rawTable(pending)).receipt().state().name());
  }

  @Test
  void regressingNativeVersionQuarantinesWithoutReanchoring() throws Exception {
    DeltaRegistrationReceipt published = publishCreate();
    writeCommit(published.location().orElseThrow(), 1, "native-a", SCHEMA, Map.of());
    operations.loadTable(IDENT);
    Path latest = temp.resolve("native/s/10/_delta_log/00000000000000000001.json");
    Files.delete(latest);
    assertThrows(RuntimeException.class, () -> operations.loadTable(IDENT));
    assertEquals(
        "NATIVE_VERSION_REGRESSION",
        DeltaRegistrationState.from(rawTable(IDENT)).receipt().reason());
  }

  @Test
  void testTransportRejectsUnownedReadsAndEveryWriteEntryPoint() throws Exception {
    Configuration owned = new Configuration();
    owned.set("delta.fixture.root", temp.resolve("native").toString());
    try (FixtureFileSystem fs = new FixtureFileSystem()) {
      fs.initialize(URI.create("s3a://bucket"), owned);
      for (String location :
          List.of(
              "s3a://other/fixture/a",
              "s3a://bucket/outside/a",
              "s3a://bucket/fixture/../../outside")) {
        assertThrows(
            IOException.class, () -> fs.getFileStatus(new org.apache.hadoop.fs.Path(location)));
      }
      assertThrows(
          IOException.class,
          () -> fs.delete(new org.apache.hadoop.fs.Path("/fixture/owned"), true));
      assertThrows(
          IOException.class,
          () ->
              fs.mkdirs(
                  new org.apache.hadoop.fs.Path("/fixture/owned"), FsPermission.getDirDefault()));
      assertThrows(
          IOException.class, () -> fs.create(new org.apache.hadoop.fs.Path("/fixture/owned")));
    }
  }

  DeltaRegistrationOperations operations(
      DeltaMetadataReader nativeReader, EntityStore entityStore) {
    return new DeltaRegistrationOperations(
        entityStore, schemas, ids::incrementAndGet, nativeReader, config, authorization);
  }

  DeltaRegistrationReceipt reserve() {
    return receipt(create(DeltaRegistrationCommand.reserve("token", "run-a", SCHEMA), 10));
  }

  DeltaRegistrationReceipt publishCreate() throws Exception {
    DeltaRegistrationReceipt reserved = reserve();
    DeltaOperationMarker marker = reserved.marker().orElseThrow();
    writeCommit(
        marker.normalizedLocation(),
        0,
        "native-a",
        SCHEMA,
        Map.of(DeltaOperationMarker.PROPERTY_KEY, marker.encode()));
    owner("alice");
    DeltaRegistrationReceipt result =
        receipt(alter(DeltaRegistrationCommand.publish(reserved.identity())));
    assertEquals("PUBLISHED", result.state().name(), result.reason());
    return result;
  }

  Table create(DeltaRegistrationCommand command, long id) {
    return operations.createTable(
        IDENT,
        new Column[0],
        null,
        Map.of(
            DeltaRegistrationCommand.COMMAND_PROPERTY,
            command.encode(),
            StringIdentifier.ID_KEY,
            StringIdentifier.fromId(id).toString()),
        new Transform[0],
        null,
        null,
        null);
  }

  Table alter(DeltaRegistrationCommand command) {
    return operations.alterTable(
        IDENT,
        TableChange.setProperty(DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()));
  }

  static DeltaRegistrationReceipt receipt(Table table) {
    return DeltaRegistrationReceipt.fromProperties(table.properties());
  }

  TableEntity rawTable(NameIdentifier ident) throws Exception {
    return store.get(ident, Entity.EntityType.TABLE, TableEntity.class);
  }

  static DeltaRegistrationIdentity identity(TableEntity row) {
    return DeltaRegistrationState.from(row).receipt().identity();
  }

  void owner(String name) throws Exception {
    NameIdentifier user = AuthorizationUtils.ofUser("m", name);
    store.put(
        UserEntity.builder()
            .withId(2000L + Math.abs(name.hashCode()))
            .withName(name)
            .withNamespace(user.namespace())
            .withAuditInfo(AUDIT)
            .build(),
        true);
    store
        .relationOperations()
        .insertRelation(
            SupportsRelationOperations.Type.OWNER_REL,
            IDENT,
            Entity.EntityType.TABLE,
            user,
            Entity.EntityType.USER,
            true);
  }

  String persistedOwner() throws Exception {
    List<UserEntity> users =
        store
            .relationOperations()
            .listEntitiesByRelation(
                SupportsRelationOperations.Type.OWNER_REL, IDENT, Entity.EntityType.TABLE);
    return users.get(0).name();
  }

  CompletableFuture<Table> loadWithPausedNativeRead(NameIdentifier ident) throws Exception {
    entered = new CountDownLatch(1);
    resumed = new CountDownLatch(1);
    DeltaMetadataReader paused = mock(DeltaMetadataReader.class);
    when(paused.latest(anyString()))
        .thenAnswer(
            call -> {
              DeltaSnapshotMetadata observed = reader.latest(call.getArgument(0));
              entered.countDown();
              if (!resumed.await(10, TimeUnit.SECONDS)) throw new AssertionError("resume timeout");
              return observed;
            });
    executor = Executors.newSingleThreadExecutor();
    CompletableFuture<Table> loading =
        CompletableFuture.supplyAsync(() -> operations(paused, store).loadTable(ident), executor);
    assertTrue(entered.await(10, TimeUnit.SECONDS));
    return loading;
  }

  void resumeNativeRead() {
    resumed.countDown();
  }

  // Only protocol-shaped test inputs: these commits were not emitted by Spark or a native writer.
  void writeCommit(
      String location,
      long version,
      String nativeId,
      String schema,
      Map<String, String> configuration)
      throws Exception {
    Path log =
        temp.resolve("native")
            .resolve(URI.create(location).getPath().substring("/fixture/".length()))
            .resolve("_delta_log");
    Files.createDirectories(log);
    ObjectNode metadata =
        JSON.createObjectNode()
            .put("id", nativeId)
            .put("schemaString", schema)
            .put("createdTime", 123);
    metadata.set("partitionColumns", JSON.createArrayNode());
    metadata.set("configuration", JSON.valueToTree(configuration));
    metadata.set(
        "format",
        JSON.createObjectNode().put("provider", "parquet").set("options", JSON.createObjectNode()));
    Files.writeString(
        log.resolve(String.format(Locale.ROOT, "%020d.json", version)),
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + JSON.createObjectNode().set("metaData", metadata)
            + "\n"
            + JSON.createObjectNode()
                .set(
                    "commitInfo",
                    JSON.createObjectNode()
                        .put("operation", version == 0 ? "CREATE TABLE" : "REPLACE TABLE")
                        .put("readVersion", version - 1))
            + "\n");
  }

  /** Read-only test transport confined to one test-owned local directory. */
  public static final class FixtureFileSystem extends FilterFileSystem {
    private Path root;
    /** Creates the Hadoop-reflected fixture transport. */
    public FixtureFileSystem() {
      super(new RawLocalFileSystem());
    }
    /** {@inheritDoc} */
    @Override
    public void initialize(URI uri, Configuration configuration) throws IOException {
      if (!"bucket".equals(uri.getAuthority())) throw new IOException("unowned authority");
      root = Path.of(configuration.get("delta.fixture.root")).toAbsolutePath().normalize();
      fs.initialize(URI.create("file:///"), configuration);
      setConf(configuration);
    }
    /** {@inheritDoc} */
    @Override
    public URI getUri() {
      return URI.create("s3a://bucket");
    }
    /** {@inheritDoc} */
    @Override
    public org.apache.hadoop.fs.Path makeQualified(org.apache.hadoop.fs.Path path) {
      return path.makeQualified(getUri(), new org.apache.hadoop.fs.Path("/fixture"));
    }

    /** {@inheritDoc} */
    @Override
    public FSDataOutputStream create(
        org.apache.hadoop.fs.Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      throw readOnly();
    }
    /** {@inheritDoc} */
    @Override
    public FSDataOutputStream append(
        org.apache.hadoop.fs.Path path, int bufferSize, Progressable progress) throws IOException {
      throw readOnly();
    }
    /** {@inheritDoc} */
    @Override
    public boolean delete(org.apache.hadoop.fs.Path path, boolean recursive) throws IOException {
      throw readOnly();
    }
    /** {@inheritDoc} */
    @Override
    public boolean rename(org.apache.hadoop.fs.Path source, org.apache.hadoop.fs.Path target)
        throws IOException {
      throw readOnly();
    }
    /** {@inheritDoc} */
    @Override
    public boolean mkdirs(org.apache.hadoop.fs.Path path, FsPermission permission)
        throws IOException {
      throw readOnly();
    }

    private static IOException readOnly() {
      return new IOException("fixture transport is read only");
    }

    private org.apache.hadoop.fs.Path local(org.apache.hadoop.fs.Path path) throws IOException {
      URI uri = path.toUri();
      if (uri.getAuthority() != null && !"bucket".equals(uri.getAuthority())
          || !uri.getPath().startsWith("/fixture/")) throw new IOException("unowned path");
      Path target = root.resolve(uri.getPath().substring("/fixture/".length())).normalize();
      if (!target.startsWith(root)) throw new IOException("unowned path");
      return new org.apache.hadoop.fs.Path(target.toUri());
    }

    private FileStatus external(FileStatus status) {
      String relative = root.relativize(Path.of(status.getPath().toUri())).toString();
      status.setPath(new org.apache.hadoop.fs.Path("s3a://bucket/fixture/" + relative));
      return status;
    }
    /** {@inheritDoc} */
    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path path, int size) throws IOException {
      return fs.open(local(path), size);
    }
    /** {@inheritDoc} */
    @Override
    public FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
      return external(fs.getFileStatus(local(path)));
    }
    /** {@inheritDoc} */
    @Override
    public FileStatus[] listStatus(org.apache.hadoop.fs.Path path) throws IOException {
      FileStatus[] result = fs.listStatus(local(path));
      for (FileStatus status : result) external(status);
      return result;
    }
  }
}
