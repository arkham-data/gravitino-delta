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

package org.apache.gravitino.catalog.lakehouse.delta.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Opted-in real S3, HTTP, packaged provider and separate native Spark process acceptance. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class TestDeltaCatalogNativeRestIT {
  private DeltaRestNativeFixtures f;

  @BeforeAll
  void start() throws Exception {
    f = new DeltaRestNativeFixtures();
    f.start();
    f.catalog("native", Map.of());
    f.origin("native");
  }

  @AfterAll
  void stop() throws Exception {
    if (f != null) {
      f.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void realServerReceiptDrivesNativeSchemaOnlyCreateAndReplace(boolean mapped) throws Exception {
    String name = mapped ? "mapped" : "ordinary";
    TableCatalog tables = tables("native");
    DeltaRegistrationCommand intent =
        reserve("create-" + name, "run-" + name, DeltaRestNativeFixtures.SCHEMA_JSON, mapped);
    Path journal = f.intent(name + "-create", intent);
    DeltaRegistrationReceipt reserved = post(tables, name, intent);
    assertEquals(DeltaRegistrationReceipt.State.RESERVED, reserved.state());
    assertFalse(tables.tableExists(local(name)));
    JsonNode child = f.publishNative(name + "-create", journal, reserved);
    DeltaOperationMarker marker = reserved.marker().orElseThrow();
    Map<String, String> before = f.manifest(marker.normalizedLocation());
    GravitinoEnv.getInstance()
        .catalogManager()
        .doWithCatalog(
            NameIdentifier.of(DeltaRestNativeFixtures.METALAKE, "native"),
            c -> {
              Class<?> readerType =
                  Class.forName(
                      "org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader",
                      true,
                      c.getClass().getClassLoader());
              try (AutoCloseable reader =
                  (AutoCloseable)
                      readerType.getConstructor(Configuration.class).newInstance(f.storage)) {
                Object snapshot =
                    readerType
                        .getMethod("latest", String.class)
                        .invoke(reader, marker.normalizedLocation());
                Map<?, ?> config =
                    (Map<?, ?>) snapshot.getClass().getMethod("configuration").invoke(snapshot);
                assertEquals(marker.encode(), config.get(DeltaOperationMarker.PROPERTY_KEY));
                Object evidence =
                    readerType
                        .getMethod("verify", DeltaOperationMarker.class)
                        .invoke(reader, marker);
                assertEquals(
                    DeltaCommitEvidence.Kind.MATCHED, ((DeltaCommitEvidence) evidence).kind());
              }
              return null;
            });
    DeltaRegistrationReceipt published =
        command(tables, name, DeltaRegistrationCommand.publish(reserved.identity()));
    assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, published.state());
    assertEquals(DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT, published.evidence());
    assertEquals(child.get("nativeId").asText(), published.nativeId().orElseThrow());
    assertEquals(0, published.nativeVersion().orElseThrow());
    assertEquals(before, f.manifest(marker.normalizedLocation()));
    assertEquals("id", tables.loadTable(local(name)).columns()[0].name());
    assertFalse(
        tables
            .loadTable(local(name))
            .properties()
            .toString()
            .contains(f.properties.get("s3-secret-access-key")));
    String changed =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    DeltaRegistrationCommand replace =
        mapped
            ? DeltaRegistrationCommand.beginReplace(
                published.identity(), "replace-" + name, "replace-run-" + name, contract(changed))
            : DeltaRegistrationCommand.beginReplace(
                published.identity(), "replace-" + name, "replace-run-" + name, changed);
    Path replaceIntent = f.intent(name + "-replace", replace);
    DeltaRegistrationReceipt replacing = command(tables, name, replace);
    assertEquals(published.identity().id(), replacing.identity().id());
    assertEquals(1, replacing.identity().generation());
    assertEquals(published.nativeId(), replacing.marker().orElseThrow().baseUuid());
    f.publishNative(name + "-replace", replaceIntent, replacing);
    Map<String, String> replacedManifest = f.manifest(marker.normalizedLocation());
    DeltaRegistrationReceipt replaced =
        command(tables, name, DeltaRegistrationCommand.publish(replacing.identity()));
    assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, replaced.state());
    assertEquals(DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT, replaced.evidence());
    assertEquals(1, replaced.nativeVersion().orElseThrow());
    assertNotEquals(published.nativeId(), replaced.nativeId());
    assertEquals("value", tables.loadTable(local(name)).columns()[0].name());
    assertEquals(replacedManifest, f.manifest(marker.normalizedLocation()));
    assertEquals(
        DeltaRestNativeFixtures.ADMIN,
        GravitinoEnv.getInstance()
            .internalOwnerDispatcher()
            .getOwner(DeltaRestNativeFixtures.METALAKE, metadata("native", name))
            .orElseThrow()
            .name());
    // A valid foreign marker is immutable native provenance, never new adoption authority.
    String alias = name + "_adopted";
    DeltaRegistrationReceipt adoption =
        post(
            tables,
            alias,
            DeltaRegistrationCommand.adopt(
                "adopt-" + name,
                marker.normalizedLocation(),
                "retired-" + name,
                "cutover-" + name));
    assertEquals(DeltaRegistrationReceipt.State.ADOPTING, adoption.state());
    assertFalse(adoption.marker().isPresent());
    assertEquals(
        DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT,
        command(tables, alias, DeltaRegistrationCommand.publish(adoption.identity())).evidence());
    assertEquals(replacedManifest, f.manifest(marker.normalizedLocation()));
  }

  @Test
  void kernelAdoptionLostReplyAndWrongIntentDoNotMutateNativeOrRows() throws Exception {
    String location = f.location("kernel-adopt");
    f.nativeTable(location, Map.of());
    TableCatalog tables = tables("native");
    DeltaRegistrationCommand intent =
        DeltaRegistrationCommand.adopt(
            "adoption-token", location, "retired-native-run", "operator-cutover");
    Path journal = f.intent("adoption-lost", intent);
    Map<String, String> nativeBefore = f.manifest(location);
    post(
        tables,
        "adoption_lost",
        intent); // Lost HTTP reply: recover using the pre-POST intent only.
    DeltaRegistrationCommand saved = DeltaRegistrationCommand.decode(Files.readString(journal));
    DeltaRegistrationReceipt status =
        command(
            tables,
            "adoption_lost",
            DeltaRegistrationCommand.statusByToken(
                saved.token().orElseThrow(), saved.runId().orElseThrow()));
    assertEquals(raw("adoption_lost").id(), status.identity().id());
    assertEquals(DeltaRegistrationReceipt.State.ADOPTING, status.state());
    assertFalse(tables.tableExists(local("adoption_lost")));
    TableEntity row = raw("adoption_lost");
    assertThrows(
        RuntimeException.class,
        () ->
            command(
                tables,
                "adoption_lost",
                DeltaRegistrationCommand.statusByToken("stale", saved.runId().orElseThrow())));
    assertThrows(
        RuntimeException.class,
        () ->
            command(
                tables,
                "adoption_lost",
                DeltaRegistrationCommand.statusByToken(saved.token().orElseThrow(), "wrong-run")));
    assertEquals(row, raw("adoption_lost"));
    DeltaRegistrationReceipt published =
        command(tables, "adoption_lost", DeltaRegistrationCommand.publish(status.identity()));
    assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, published.state());
    assertEquals(DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT, published.evidence());
    assertEquals(nativeBefore, f.manifest(location));
    assertFalse(
        raw("adoption_lost")
            .properties()
            .toString()
            .contains(f.properties.get("s3-secret-access-key")));
  }

  @Test
  void nativeMalformedMarkerIsRejectedBeforeAdoptionRowCreation() throws Exception {
    String location = f.location("malformed-marker");
    f.nativeTable(location, Map.of());
    JsonNode child = f.malformedNativeMarker(location);
    assertEquals("MALFORMED_ADOPTION_MARKER", child.get("fixture").asText());
    assertEquals(1, child.get("version").asLong());
    Map<String, String> before = f.manifest(location);
    RuntimeException rejected =
        assertThrows(
            RuntimeException.class,
            () ->
                post(
                    tables("native"),
                    "malformed_marker",
                    DeltaRegistrationCommand.adopt(
                        "malformed", location, "retired-malformed", "cutover")));
    assertTrue(rejected.getMessage().contains("INVALID_ADOPTION_MARKER"));
    assertThrows(NoSuchEntityException.class, () -> raw("malformed_marker"));
    assertEquals(before, f.manifest(location));
  }

  @ParameterizedTest
  @ValueSource(strings = {"version", "configuration", "identity"})
  void adoptionBaselineDriftQuarantines(String drift) throws Exception {
    String location = f.location("drift-" + drift);
    f.nativeTable(location, Map.of());
    TableCatalog tables = tables("native");
    String name = "drift_" + drift;
    DeltaRegistrationReceipt receipt =
        post(
            tables,
            name,
            DeltaRegistrationCommand.adopt(
                "drift-" + drift, location, "retired-" + drift, "cutover"));
    if (drift.equals("identity")) {
      assertTrue(f.filesystem.delete(new org.apache.hadoop.fs.Path(location), true));
      f.nativeTable(location, Map.of());
    } else {
      f.nativeChange(
          location,
          drift.equals("configuration") ? Map.of("delta.checkpointInterval", "20") : Map.of());
    }
    Map<String, String> before = f.manifest(location);
    DeltaRegistrationReceipt result =
        command(tables, name, DeltaRegistrationCommand.publish(receipt.identity()));
    assertEquals(DeltaRegistrationReceipt.State.QUARANTINED, result.state());
    assertEquals("ADOPTION_BASELINE_CHANGED", result.reason());
    assertFalse(tables.tableExists(local(name)));
    assertEquals(before, f.manifest(location));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void adoptionOwnerPosthookFailureIsDurableAndCompletedReplayRepairs(boolean afterWrite)
      throws Exception {
    String name = "adoption_owner_" + afterWrite;
    String location = f.location(name);
    f.nativeTable(location, Map.of());
    TableCatalog tables = tables("native");
    DeltaRegistrationCommand intent =
        DeltaRegistrationCommand.adopt(name, location, "retired-" + name, "cutover");
    OwnerDispatcher original = GravitinoEnv.getInstance().internalOwnerDispatcher();
    AtomicBoolean once = new AtomicBoolean(true);
    OwnerDispatcher injected =
        new OwnerDispatcher() {
          @Override
          public void setOwner(
              String metalake, MetadataObject object, String owner, Owner.Type type) {
            if (object.equals(metadata("native", name)) && once.compareAndSet(true, false)) {
              if (afterWrite) {
                original.setOwner(metalake, object, owner, type);
              }
              throw new IllegalStateException("Task4 completed native adoption owner failure");
            }
            original.setOwner(metalake, object, owner, type);
          }

          @Override
          public Optional<Owner> getOwner(String metalake, MetadataObject object) {
            return original.getOwner(metalake, object);
          }
        };
    TableEntity row;
    try {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "internalOwnerDispatcher", injected, true);
      assertThrows(RuntimeException.class, () -> post(tables, name, intent));
      assertFalse(once.get());
      row = raw(name);
      assertEquals(
          afterWrite,
          original
              .getOwner(DeltaRestNativeFixtures.METALAKE, metadata("native", name))
              .isPresent());
      DeltaRegistrationReceipt receipt =
          command(tables, name, DeltaRegistrationCommand.administrativeStatus());
      if (!afterWrite) {
        assertThrows(
            RuntimeException.class,
            () -> command(tables, name, DeltaRegistrationCommand.publish(receipt.identity())));
        assertEquals(row, raw(name));
      }
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "internalOwnerDispatcher", original, true);
    }
    DeltaRegistrationReceipt replay = post(tables, name, intent);
    assertEquals(row, raw(name));
    assertEquals(
        DeltaRestNativeFixtures.ADMIN,
        original
            .getOwner(DeltaRestNativeFixtures.METALAKE, metadata("native", name))
            .orElseThrow()
            .name());
    assertEquals(
        DeltaRegistrationReceipt.State.PUBLISHED,
        command(tables, name, DeltaRegistrationCommand.publish(replay.identity())).state());
  }

  @Test
  void pooledIsolatedCatalogsKeepCredentialsAndReaderClosureSeparate() throws Exception {
    String location = f.location("contexts");
    f.nativeTable(location, Map.of());
    f.catalog("valid_one", Map.of());
    f.catalog("valid_two", Map.of());
    f.catalog(
        "invalid",
        Map.of(
            "s3-access-key-id",
            "invalid-task4-key",
            "s3-secret-access-key",
            "invalid-task4-secret"));
    Object loader = f.origin("valid_one");
    assertSame(loader, f.origin("valid_two"));
    assertSame(loader, f.origin("invalid"));
    TableCatalog first = tables("valid_one");
    TableCatalog second = tables("valid_two");
    TableCatalog invalid = tables("invalid");
    DeltaRegistrationReceipt r1 =
        post(
            first,
            "shared",
            DeltaRegistrationCommand.adopt("valid-one", location, "retired-one", "cutover"));
    command(first, "shared", DeltaRegistrationCommand.publish(r1.identity()));
    assertThrows(
        RuntimeException.class,
        () ->
            post(
                invalid,
                "shared",
                DeltaRegistrationCommand.adopt("invalid", location, "retired-invalid", "cutover")));
    DeltaRegistrationReceipt r2 =
        post(
            second,
            "shared",
            DeltaRegistrationCommand.adopt("valid-two", location, "retired-two", "cutover"));
    command(second, "shared", DeltaRegistrationCommand.publish(r2.identity()));
    f.admin.disableCatalog("valid_one");
    assertEquals("id", second.loadTable(local("shared")).columns()[0].name());
  }

  @Test
  void restartRetainsRegistrationHandoffAndAbandonment() throws Exception {
    String name = "restart";
    TableCatalog tables = tables("native");
    DeltaRegistrationReceipt pending =
        post(
            tables,
            name,
            reserve("restart-token", "restart-run", DeltaRestNativeFixtures.SCHEMA_JSON, false));
    TableEntity original = raw(name);
    f.restart();
    tables = tables("native");
    assertEquals(
        pending.identity(),
        command(tables, name, DeltaRegistrationCommand.status(pending.identity())).identity());
    assertEquals(original, raw(name));
    command(
        tables,
        name,
        DeltaRegistrationCommand.handoff(
            pending.identity(), pending.runId(), "drained-native-controller-attestation"));
    TableEntity handedOff = raw(name);
    f.restart();
    tables = tables("native");
    assertEquals(handedOff, raw(name));
    DeltaRegistrationReceipt abandoned =
        command(tables, name, DeltaRegistrationCommand.abort(pending.identity()));
    assertEquals(DeltaRegistrationReceipt.State.ABANDONED, abandoned.state());
    f.restart();
    tables = tables("native");
    assertEquals(
        DeltaRegistrationReceipt.State.ABANDONED,
        command(tables, name, DeltaRegistrationCommand.status(pending.identity())).state());
    assertFalse(tables.tableExists(local(name)));
    f.origin("native");
  }

  private DeltaSchemaContract contract(String schema) {
    return DeltaSchemaContract.spark35Delta33NameMapping(
        DeltaSchemaContract.OperationMode.SCHEMA_ONLY, schema);
  }

  private DeltaRegistrationCommand reserve(
      String token, String run, String schema, boolean mapped) {
    return mapped
        ? DeltaRegistrationCommand.reserve(token, run, contract(schema))
        : DeltaRegistrationCommand.reserve(token, run, schema);
  }

  private NameIdentifier local(String name) {
    return NameIdentifier.of(DeltaRestNativeFixtures.SCHEMA, name);
  }

  private TableCatalog tables(String catalog) {
    return f.admin.loadCatalog(catalog).asTableCatalog();
  }

  private DeltaRegistrationReceipt post(
      TableCatalog tables, String name, DeltaRegistrationCommand command) throws Exception {
    f.openingIntent(command);
    DeltaRegistrationReceipt receipt =
        DeltaRegistrationReceipt.fromProperties(
            tables
                .createTable(
                    local(name),
                    new Column[0],
                    null,
                    Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()))
                .properties());
    f.receipt("POST", name, command, receipt);
    return receipt;
  }

  private DeltaRegistrationReceipt command(
      TableCatalog tables, String name, DeltaRegistrationCommand command) throws Exception {
    DeltaRegistrationReceipt receipt =
        DeltaRegistrationReceipt.fromProperties(
            tables
                .alterTable(
                    local(name),
                    TableChange.setProperty(
                        DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()))
                .properties());
    f.receipt("PUT", name, command, receipt);
    return receipt;
  }

  private MetadataObject metadata(String catalog, String name) {
    return MetadataObjects.of(
        List.of(catalog, DeltaRestNativeFixtures.SCHEMA, name), MetadataObject.Type.TABLE);
  }

  private TableEntity raw(String name) throws Exception {
    return GravitinoEnv.getInstance()
        .entityStore()
        .get(
            NameIdentifier.of(
                DeltaRestNativeFixtures.METALAKE, "native", DeltaRestNativeFixtures.SCHEMA, name),
            Entity.EntityType.TABLE,
            TableEntity.class);
  }
}
