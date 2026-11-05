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

package org.apache.gravitino.spark.connector.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.Seq;

@SuppressWarnings("deprecation")
class TestDeltaCreateLifecycle {
  private final TestGravitinoDeltaCatalogSpark35 fixture = new TestGravitinoDeltaCatalogSpark35();
  private final Recording lifecycle = new Recording();

  @BeforeEach
  void setup() throws Exception {
    fixture.setup();
    fixture.lifecycle = lifecycle;
  }

  @Test
  void descriptorHasNoLifecycleSideEffects() {
    StagedTable stage = stage();
    stage.schema();
    stage.partitioning();
    stage.properties();
    stage.capabilities();
    assertEquals(1, fixture.lifecycleFactories);
    assertTrue(fixture.effects.isEmpty());
    stage.abortStagedChanges();
    stage.abortStagedChanges();
    assertEquals(List.of("close"), fixture.effects);
  }

  @Test
  void intentAndMaterializationInputsAreImmutable() {
    String[] namespace = {"db"};
    StructType schema = new StructType().add("id", DataTypes.LongType, false);
    String original = schema.json();
    Map<String, String> properties = new HashMap<>(Map.of("comment", "original"));
    StagedTable stage =
        fixture.adapter.stageCreate(
            Identifier.of(namespace, "t"), schema, new Transform[0], properties);
    properties.put("comment", "changed");
    namespace[0] = "changed";
    schema.fields()[0] = DataTypes.createStructField("changed", DataTypes.StringType, true);
    assertEquals("alias", fixture.intent.catalog());
    assertEquals(List.of("db"), fixture.intent.namespace());
    assertEquals("t", fixture.intent.table());
    assertEquals(original, fixture.intent.schemaJson());
    assertEquals(0, fixture.intent.partitionCount());
    assertEquals(Map.of("comment", "original"), fixture.intent.properties());
    assertThrows(
        UnsupportedOperationException.class, () -> fixture.intent.namespace().set(0, "other"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> fixture.intent.properties().put("comment", "other"));
    Map<String, String> options = new HashMap<>(Map.of("compression", "snappy"));
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.schema()).thenReturn(new StructType().add("different", DataTypes.LongType));
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(options));
    lifecycle.requireDeclaration = true;
    assertSame(
        lifecycle.failure,
        assertThrows(
            IllegalStateException.class, () -> ((SupportsWrite) stage).newWriteBuilder(info)));
    assertEquals(List.of("beforeReserve", "close"), fixture.effects);
    verify(fixture.tables, never()).loadTable(any());
    options.put("compression", "gzip");
    assertEquals(OperationMode.QUERY, lifecycle.mode);
    assertNotEquals(original, lifecycle.schemaJson);
    assertEquals(Map.of("compression", "snappy"), lifecycle.options);
    assertThrows(
        UnsupportedOperationException.class, () -> lifecycle.options.put("compression", "gzip"));
    stage.abortStagedChanges();
    assertEquals(1, Collections.frequency(fixture.effects, "close"));
  }

  @Test
  void exactIdentityEvidenceIsFrozenAndUnknownTransformsAreNotInspected() {
    Transform[] partitions = {Expressions.identity("region"), Expressions.identity("id")};
    DeltaCreateLifecycle.Intent intent =
        new DeltaCreateLifecycle.Intent(
            "a", List.of("db"), "t", fixture.schema.json(), partitions, Map.of());
    partitions[0] = Expressions.identity("changed");
    assertEquals(List.of("region", "id"), intent.identityPartitionColumns().orElseThrow());
    assertThrows(
        UnsupportedOperationException.class,
        () -> intent.identityPartitionColumns().orElseThrow().set(0, "changed"));
    Transform unknown = mock(Transform.class);
    NamedReference unknownReference = mock(NamedReference.class);
    @SuppressWarnings("unchecked")
    Seq<String> unknownParts = mock(Seq.class);
    for (Transform transform :
        new Transform[] {
          unknown,
          new IdentityTransform(unknownReference),
          new IdentityTransform(new FieldReference(unknownParts)),
          Expressions.identity("nested.field"),
          Expressions.years("id")
        }) {
      DeltaCreateLifecycle.Intent unsupported =
          new DeltaCreateLifecycle.Intent(
              "a",
              List.of("db"),
              "t",
              fixture.schema.json(),
              new Transform[] {transform},
              Map.of());
      assertFalse(unsupported.identityPartitionColumns().isPresent());
      assertEquals(1, unsupported.partitionCount());
    }
    verifyNoInteractions(unknown, unknownReference, unknownParts);
    assertFalse(
        new DeltaCreateLifecycle.Intent("a", List.of("db"), "t", fixture.schema.json(), 0, Map.of())
            .identityPartitionColumns()
            .isPresent());
    assertEquals(
        List.of(),
        new DeltaCreateLifecycle.Intent(
                "a", List.of("db"), "t", fixture.schema.json(), new Transform[0], Map.of())
            .identityPartitionColumns()
            .orElseThrow());
  }

  @Test
  void reservationReceiptReachesExactLifecycle() {
    stage().commitStagedChanges();
    assertEquals(
        List.of(
            "beforeReserve",
            "INTENT",
            "POST",
            "RESERVED",
            "reserved",
            "NATIVE_STARTED",
            "native-stage",
            "beforeNative",
            "native-commit",
            "NATIVE_COMMITTED",
            "scope-close",
            "beforePublish",
            "INTENT",
            "PUBLISH",
            "PUBLISHED",
            "PUBLISHED",
            "published",
            "close"),
        fixture.effects);
    DeltaRegistrationReceipt reserved = fixture.entries.get(1).receipt;
    assertSame(reserved, lifecycle.reserved);
    assertSame(reserved, lifecycle.nativeReceipt);
    assertSame(reserved, lifecycle.publishReceipt);
    assertEquals(fixture.entries.get(0).command.token().orElseThrow(), reserved.identity().token());
    assertEquals(fixture.entries.get(0).command.runId().orElseThrow(), reserved.runId());
    assertEquals("file:/tmp/owned", reserved.marker().orElseThrow().normalizedLocation());
    assertSame(fixture.entries.get(fixture.entries.size() - 1).receipt, lifecycle.published);
  }

  @Test
  void deniedBeforeReserveHasNoTransport() {
    lifecycle.denied = "beforeReserve";
    IllegalStateException error =
        assertThrows(IllegalStateException.class, stage()::commitStagedChanges);
    assertSame(lifecycle.failure, error);
    assertEquals(List.of("beforeReserve", "close"), fixture.effects);
    verify(fixture.tables, never()).loadTable(any());
    assertTrue(fixture.entries.isEmpty());
  }

  @Test
  void deniedMetadataAndHiddenReservationConflictAreNeverAbsence() {
    ForbiddenException denied = new ForbiddenException("denied");
    doThrow(denied).when(fixture.tables).loadTable(any());
    assertSame(denied, assertThrows(ForbiddenException.class, stage()::commitStagedChanges));
    assertEquals(List.of("beforeReserve", "close"), fixture.effects);
    fixture.effects.clear();
    doThrow(new NoSuchTableException("hidden")).when(fixture.tables).loadTable(any());
    doThrow(new TableAlreadyExistsException("reserved"))
        .when(fixture.tables)
        .createTable(any(), any(), any(), any());
    assertThrows(IllegalStateException.class, stage()::commitStagedChanges);
    assertEquals(List.of("beforeReserve", "INTENT", "STATUS_NEEDED", "close"), fixture.effects);
    verify(fixture.nativeCatalog, never()).stageCreate(any(), any(StructType.class), any(), any());
  }

  @Test
  void deniedBeforeNativeNeverInvokesNative() {
    lifecycle.denied = "beforeNative";
    StagedTable stage = stage();
    assertSame(
        lifecycle.failure, assertThrows(IllegalStateException.class, stage::commitStagedChanges));
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    stage.abortStagedChanges();
    assertEquals(0, Collections.frequency(fixture.effects, "native-commit"));
    assertFalse(fixture.effects.contains("PUBLISH"));
    assertEquals(DeltaRegistrationJournal.Phase.NATIVE_STARTED, lastPhase());
    assertEquals(1, Collections.frequency(fixture.effects, "close"));
  }

  @Test
  void deniedBeforePublishRetainsNativeCommit() {
    lifecycle.denied = "beforePublish";
    StagedTable stage = stage();
    assertSame(
        lifecycle.failure, assertThrows(IllegalStateException.class, stage::commitStagedChanges));
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertEquals(DeltaRegistrationJournal.Phase.NATIVE_COMMITTED, lastPhase());
    assertEquals(1, Collections.frequency(fixture.effects, "native-commit"));
    assertEquals(1, Collections.frequency(fixture.effects, "beforePublish"));
    assertFalse(fixture.effects.contains("PUBLISH"));
    assertEquals(1, Collections.frequency(fixture.effects, "close"));
  }

  @Test
  void nativeThrowIsNeverReplayed() {
    IllegalStateException nativeFailure = new IllegalStateException("native uncertainty");
    doAnswer(
            call -> {
              fixture.effects.add("native-commit");
              throw nativeFailure;
            })
        .when(fixture.nativeStage)
        .commitStagedChanges();
    lifecycle.failScopeClose = true;
    lifecycle.failClose = true;
    StagedTable stage = stage();
    assertSame(
        nativeFailure, assertThrows(IllegalStateException.class, stage::commitStagedChanges));
    assertEquals(
        List.of(lifecycle.scopeFailure, lifecycle.closeFailure),
        List.of(nativeFailure.getSuppressed()));
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertEquals(1, Collections.frequency(fixture.effects, "native-commit"));
    assertEquals(DeltaRegistrationJournal.Phase.NATIVE_STARTED, lastPhase());
    assertFalse(fixture.effects.contains("PUBLISH"));
  }

  @Test
  void cleanupFailurePreservesCommittedOutcome() {
    lifecycle.failScopeClose = true;
    StagedTable stage = stage();
    assertSame(
        lifecycle.scopeFailure,
        assertThrows(IllegalStateException.class, stage::commitStagedChanges));
    IllegalStateException retry =
        assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertTrue(retry.getMessage().contains("PUBLICATION"));
    assertEquals(DeltaRegistrationJournal.Phase.NATIVE_COMMITTED, lastPhase());
    assertEquals(1, Collections.frequency(fixture.effects, "native-commit"));
    assertFalse(fixture.effects.contains("PUBLISH"));
    assertEquals(1, Collections.frequency(fixture.effects, "close"));
  }

  @Test
  void publishedObserverAndCloseFailuresPreservePublishedOutcome() {
    lifecycle.denied = "published";
    lifecycle.failClose = true;
    StagedTable stage = stage();
    assertSame(
        lifecycle.failure, assertThrows(IllegalStateException.class, stage::commitStagedChanges));
    stage.commitStagedChanges();
    assertEquals(DeltaRegistrationJournal.Phase.PUBLISHED, lastPhase());
    assertEquals(1, Collections.frequency(fixture.effects, "PUBLISH"));
    assertEquals(List.of(lifecycle.closeFailure), List.of(lifecycle.failure.getSuppressed()));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void publishedReceiptSurvivesEitherPostResponseJournalFailure(int occurrence) {
    fixture.failPhase = DeltaRegistrationJournal.Phase.PUBLISHED;
    fixture.failPhaseOccurrence = occurrence;
    lifecycle.denied = "published";
    lifecycle.failClose = true;
    StagedTable stage = stage();
    IllegalStateException error =
        assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertEquals("disk unavailable", error.getMessage());
    assertEquals(
        List.of(lifecycle.failure, lifecycle.closeFailure), List.of(error.getSuppressed()));
    assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, lifecycle.published.state());
    assertEquals(fixture.receipt.get().identity(), lifecycle.published.identity());
    assertEquals(1, Collections.frequency(fixture.effects, "published"));
    stage.commitStagedChanges();
    stage.abortStagedChanges();
    assertEquals(DeltaRegistrationReceipt.State.PUBLISHED, fixture.receipt.get().state());
    assertEquals(1, Collections.frequency(fixture.effects, "native-commit"));
    assertEquals(1, Collections.frequency(fixture.effects, "PUBLISH"));
    assertEquals(1, Collections.frequency(fixture.effects, "close"));
  }

  @Test
  void noOpPreservesExistingBaseModes() {
    fixture.lifecycle = DeltaCreateLifecycle.noop();
    stage().commitStagedChanges();
    assertEquals(1, Collections.frequency(fixture.effects, "native-commit"));
    assertEquals(DeltaRegistrationJournal.Phase.PUBLISHED, lastPhase());
    int factories = fixture.lifecycleFactories;
    fixture.receipt.set(null);
    StagedTable replace =
        fixture.adapter.stageCreateOrReplace(
            fixture.id, fixture.schema, new Transform[0], Map.of());
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.schema()).thenReturn(fixture.schema);
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Map.of()));
    ((SupportsWrite) replace).newWriteBuilder(info);
    replace.commitStagedChanges();
    assertEquals(factories, fixture.lifecycleFactories);
    assertTrue(fixture.effects.contains("native-cor"));
  }

  private StagedTable stage() {
    return fixture.adapter.stageCreate(fixture.id, fixture.schema, new Transform[0], Map.of());
  }

  private DeltaRegistrationJournal.Phase lastPhase() {
    return fixture.entries.get(fixture.entries.size() - 1).phase;
  }

  private final class Recording implements DeltaCreateLifecycle {
    private final IllegalStateException failure = new IllegalStateException("guard denied");
    private final IllegalStateException scopeFailure = new IllegalStateException("scope cleanup");
    private final IllegalStateException closeFailure =
        new IllegalStateException("lifecycle cleanup");
    private String denied;
    private boolean requireDeclaration;
    private boolean failScopeClose;
    private boolean failClose;
    private OperationMode mode;
    private String schemaJson;
    private Map<String, String> options;
    private DeltaRegistrationReceipt reserved;
    private DeltaRegistrationReceipt nativeReceipt;
    private DeltaRegistrationReceipt publishReceipt;
    private DeltaRegistrationReceipt published;

    @Override
    public void beforeReserve(OperationMode value, String schema, Map<String, String> inputs) {
      mode = value;
      schemaJson = schema;
      options = inputs;
      event("beforeReserve");
      if (requireDeclaration
          && (mode != OperationMode.SCHEMA_ONLY
              || !schemaJson.equals(fixture.intent.schemaJson())
              || !options.isEmpty())) {
        throw failure;
      }
    }

    @Override
    public void reserved(DeltaRegistrationReceipt receipt) {
      reserved = receipt;
      event("reserved");
    }

    @Override
    public Scope beforeNative(DeltaRegistrationReceipt receipt) {
      nativeReceipt = receipt;
      event("beforeNative");
      return () -> {
        event("scope-close");
        if (failScopeClose) throw scopeFailure;
      };
    }

    @Override
    public void beforePublish(DeltaRegistrationReceipt receipt) {
      publishReceipt = receipt;
      event("beforePublish");
    }

    @Override
    public void published(DeltaRegistrationReceipt receipt) {
      published = receipt;
      event("published");
    }

    @Override
    public void close() {
      event("close");
      if (failClose) throw closeFailure;
    }

    private void event(String name) {
      fixture.effects.add(name);
      if (name.equals(denied)) throw failure;
    }
  }
}
