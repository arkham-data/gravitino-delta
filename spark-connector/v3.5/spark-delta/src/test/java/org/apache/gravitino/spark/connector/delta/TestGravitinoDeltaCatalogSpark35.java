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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
class TestGravitinoDeltaCatalogSpark35 {
  private final Identifier id = Identifier.of(new String[] {"db"}, "t");
  private final StructType schema = new StructType();
  private final List<String> effects = new ArrayList<>();
  private final List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
  private final Catalog catalog = mock(Catalog.class);
  private final TableCatalog tables = mock(TableCatalog.class);
  private final DeltaCatalog nativeCatalog = mock(DeltaCatalog.class);
  private final StagedTable nativeStage =
      mock(StagedTable.class, withSettings().extraInterfaces(SupportsWrite.class));
  private final SparkSession session = mock(SparkSession.class);
  private final AtomicReference<DeltaRegistrationReceipt> receipt = new AtomicReference<>();
  private DeltaRegistrationJournal.Phase failPhase;
  private boolean uncertain;
  private GravitinoDeltaCatalogSpark35 adapter;

  @BeforeEach
  void setup() throws Exception {
    when(catalog.name()).thenReturn("canonical");
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(tables.loadTable(any()))
        .thenAnswer(
            call -> {
              if (receipt.get() == null) {
                throw new org.apache.gravitino.exceptions.NoSuchTableException("absent");
              }
              return table(receipt.get());
            });
    when(tables.createTable(any(), any(), any(), any()))
        .thenAnswer(
            call -> {
              effects.add("POST");
              Map<String, String> properties = call.getArgument(3);
              assertEquals(Set.of(DeltaRegistrationCommand.COMMAND_PROPERTY), properties.keySet());
              DeltaRegistrationCommand command =
                  DeltaRegistrationCommand.decode(
                      properties.get(DeltaRegistrationCommand.COMMAND_PROPERTY));
              receipt.set(pending(command));
              if (uncertain) {
                throw new IllegalStateException("secret backend endpoint");
              }
              return table(receipt.get());
            });
    when(tables.alterTable(any(), any()))
        .thenAnswer(
            call -> {
              TableChange.SetProperty property = call.getArgument(1);
              DeltaRegistrationCommand command =
                  DeltaRegistrationCommand.decode(property.getValue());
              effects.add(command.operation().name());
              if (command.operation() == DeltaRegistrationCommand.Operation.PUBLISH) {
                receipt.set(published(receipt.get()));
              }
              return table(receipt.get());
            });
    when(nativeCatalog.stageCreate(any(), any(StructType.class), any(), any()))
        .thenAnswer(
            call -> {
              effects.add("native-stage");
              return nativeStage;
            });
    when(nativeCatalog.stageCreateOrReplace(any(), any(StructType.class), any(), any()))
        .thenAnswer(
            call -> {
              effects.add("native-cor");
              return nativeStage;
            });
    doAnswer(
            call -> {
              effects.add("native-commit");
              return null;
            })
        .when(nativeStage)
        .commitStagedChanges();
    when(((SupportsWrite) nativeStage).newWriteBuilder(any())).thenReturn(mock(WriteBuilder.class));
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(session.conf()).thenReturn(conf);
    when(conf.get(anyString(), anyString())).thenAnswer(call -> call.getArgument(1));
    when(conf.get("spark.sql.catalog.spark_catalog", ""))
        .thenReturn("org.apache.spark.sql.delta.catalog.DeltaCatalog");
    when(conf.get("spark.sql.extensions", ""))
        .thenReturn("io.delta.sql.DeltaSparkSessionExtension");
    adapter =
        new GravitinoDeltaCatalogSpark35(
            nativeCatalog,
            new DeltaRegistrationClient(() -> catalog, () -> catalog),
            entry -> {
              effects.add(entry.phase.name());
              if (entry.phase == failPhase) {
                throw new IllegalStateException("disk unavailable");
              }
              entries.add(entry);
            },
            () -> session,
            (location, current) -> effects.add("head"));
    adapter.initialize("alias", new CaseInsensitiveStringMap(Map.of()));
  }

  @Test
  void durableIntentThenReceiptPrecedeNativeStageAndPublishFollowsCommit() {
    StagedTable stage = adapter.stageCreate(id, schema, new Transform[0], Map.of());
    stage.schema();
    stage.properties();
    stage.capabilities();
    assertTrue(effects.isEmpty());
    stage.commitStagedChanges();
    assertTrue(effects.indexOf("INTENT") < effects.indexOf("POST"));
    assertTrue(effects.indexOf("RESERVED") < effects.indexOf("native-stage"));
    assertTrue(effects.indexOf("native-commit") < effects.indexOf("PUBLISH"));
    assertEquals(DeltaRegistrationJournal.Phase.PUBLISHED, entries.get(entries.size() - 1).phase);
  }

  @Test
  void intentJournalFailurePreventsPost() {
    failPhase = DeltaRegistrationJournal.Phase.INTENT;
    assertThrows(
        IllegalStateException.class,
        () -> adapter.stageCreate(id, schema, new Transform[0], Map.of()).commitStagedChanges());
    assertFalse(effects.contains("POST"));
    assertFalse(effects.contains("native-stage"));
  }

  @Test
  void receiptJournalFailurePreventsNativeStageConstruction() {
    failPhase = DeltaRegistrationJournal.Phase.RESERVED;
    assertThrows(
        IllegalStateException.class,
        () -> adapter.stageCreate(id, schema, new Transform[0], Map.of()).commitStagedChanges());
    assertTrue(effects.contains("POST"));
    assertFalse(effects.contains("native-stage"));
  }

  @Test
  void uncertainPostRetainsOriginalIntentAndNeverRetries() {
    uncertain = true;
    StagedTable stage = adapter.stageCreate(id, schema, new Transform[0], Map.of());
    RuntimeException error = assertThrows(RuntimeException.class, stage::commitStagedChanges);
    assertFalse(error.toString().contains("secret"));
    assertThrows(RuntimeException.class, stage::commitStagedChanges);
    assertEquals(1, Collections.frequency(effects, "POST"));
    assertFalse(effects.contains("native-stage"));
    DeltaRegistrationJournal.Entry saved = entries.get(entries.size() - 1);
    assertEquals(DeltaRegistrationJournal.Phase.STATUS_NEEDED, saved.phase);
    assertNull(saved.receipt);
    assertEquals(entries.get(0).command.encode(), saved.command.encode());
  }

  @Test
  void missingReplaceAndExistingCreateFailBeforeReservation() {
    assertThrows(
        org.apache.spark.sql.catalyst.analysis.NoSuchTableException.class,
        () -> adapter.stageReplace(id, schema, new Transform[0], Map.of()).commitStagedChanges());
    assertTrue(effects.isEmpty());
    receipt.set(published(TestDeltaRegistrationClient.pending("token", "run")));
    assertThrows(
        org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException.class,
        () -> adapter.stageCreate(id, schema, new Transform[0], Map.of()).commitStagedChanges());
    assertTrue(effects.isEmpty());
  }

  @Test
  void absentQueryCorRetainsNativeCreateOrReplace() {
    StagedTable stage = adapter.stageCreateOrReplace(id, schema, new Transform[0], Map.of());
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.schema()).thenReturn(schema);
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(Map.of()));
    ((SupportsWrite) stage).newWriteBuilder(info);
    assertTrue(effects.contains("native-cor"));
    assertFalse(effects.contains("native-stage"));
    assertEquals(DeltaRegistrationCommand.Operation.RESERVE, entries.get(0).command.operation());
  }

  @Test
  void excludedEngineSchemaMetadataFailsBeforeReservation() {
    StructType internal =
        new StructType()
            .add(
                "id",
                org.apache.spark.sql.types.DataTypes.IntegerType,
                true,
                new org.apache.spark.sql.types.MetadataBuilder()
                    .putLong("delta.columnMapping.id", 7)
                    .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> adapter.stageCreate(id, internal, new Transform[0], Map.of()).commitStagedChanges());
    assertTrue(effects.isEmpty());
  }

  @Test
  void staleReplacementReceiptCannotConstructNativeStage() {
    receipt.set(published(TestDeltaRegistrationClient.pending("token", "run")));
    assertThrows(
        IllegalStateException.class,
        () -> adapter.stageReplace(id, schema, new Transform[0], Map.of()).commitStagedChanges());
    assertTrue(effects.contains("BEGIN_REPLACE"));
    assertFalse(effects.contains("native-stage"));
    assertFalse(effects.contains("native-cor"));
  }

  @Test
  void abortAfterLostPostKeepsCompletionUncertaintyExplicit() {
    uncertain = true;
    StagedTable stage = adapter.stageCreate(id, schema, new Transform[0], Map.of());
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    stage.abortStagedChanges();
    assertEquals(
        DeltaRegistrationJournal.Phase.STATUS_NEEDED, entries.get(entries.size() - 1).phase);
  }

  @Test
  void publicationHasItsOwnTypedDurableIntent() {
    adapter.stageCreate(id, schema, new Transform[0], Map.of()).commitStagedChanges();
    assertTrue(
        entries.stream()
            .anyMatch(
                entry ->
                    entry.phase == DeltaRegistrationJournal.Phase.INTENT
                        && entry.command.operation()
                            == DeltaRegistrationCommand.Operation.PUBLISH));
  }

  private static Table table(DeltaRegistrationReceipt receipt) {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(receipt.toProperties());
    return table;
  }

  private static DeltaRegistrationReceipt pending(DeltaRegistrationCommand command) {
    DeltaOperationMarker marker =
        command.schemaContract().isPresent()
            ? DeltaOperationMarker.createWithSchemaContract(
                11,
                0,
                command.token().orElseThrow(),
                "file:/tmp/owned",
                command.schemaContract().orElseThrow())
            : DeltaOperationMarker.create(
                11,
                0,
                command.token().orElseThrow(),
                "file:/tmp/owned",
                command.exactSchemaJson().orElseThrow());
    return DeltaRegistrationReceipt.of(
        DeltaRegistrationIdentity.of(11, 0, command.token().orElseThrow()),
        DeltaRegistrationCommand.Operation.RESERVE,
        DeltaRegistrationReceipt.State.RESERVED,
        command.runId().orElseThrow(),
        DeltaRegistrationReceipt.Evidence.NONE,
        "NONE",
        Optional.of(marker),
        Optional.empty(),
        Optional.empty(),
        OptionalLong.empty());
  }

  private static DeltaRegistrationReceipt published(DeltaRegistrationReceipt previous) {
    return DeltaRegistrationReceipt.of(
        previous.identity(),
        previous.operation(),
        DeltaRegistrationReceipt.State.PUBLISHED,
        previous.runId(),
        DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
        "NONE",
        Optional.empty(),
        Optional.of("file:/tmp/owned"),
        Optional.of("uuid"),
        OptionalLong.of(0));
  }
}
