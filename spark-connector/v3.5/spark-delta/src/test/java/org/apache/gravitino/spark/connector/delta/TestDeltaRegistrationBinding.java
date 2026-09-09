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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestDeltaRegistrationBinding {
  private static final String SCHEMA = TestDeltaRegistrationClient.SCHEMA;
  private static final String OTHER_SCHEMA =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\","
          + "\"nullable\":true,\"metadata\":{}}]}";
  private static final String LOCATION = "file:/tmp/owned";
  @TempDir Path directory;

  @ParameterizedTest(name = "{0}")
  @MethodSource("mismatches")
  void statusBindsObservedReceiptBeforeJournaling(
      String description, DeltaRegistrationCommand opening, DeltaRegistrationReceipt observed) {
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery = recovery(observed, entries::add);
    assertThrows(IllegalStateException.class, () -> recovery.status(entry(opening, null)));
    assertTrue(entries.isEmpty());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mismatches")
  void journalDecodeRejectsIndividuallyValidButMismatchedValues(
      String description, DeltaRegistrationCommand opening, DeltaRegistrationReceipt observed)
      throws Exception {
    DeltaRegistrationJournal journal = DeltaRegistrationJournal.durable(directory);
    DeltaRegistrationJournal.Entry valid =
        entry(DeltaRegistrationCommand.status(observed.identity()), observed);
    journal.write(valid);
    Path file = directory.resolve(valid.key + ".json");
    ObjectMapper json = new ObjectMapper();
    ObjectNode envelope = (ObjectNode) json.readTree(Files.readString(file));
    envelope.set("command", json.readTree(opening.encode()));
    Files.writeString(file, envelope.toString());
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.read(file));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mismatches")
  void constructorCreatedEntryCannotAuthorizeOperatorMutation(
      String description, DeltaRegistrationCommand opening, DeltaRegistrationReceipt observed) {
    Catalog catalog = mock(Catalog.class);
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery =
        new DeltaRegistrationRecovery(
            new DeltaRegistrationClient(() -> catalog, () -> catalog), entries::add);
    assertThrows(
        IllegalStateException.class,
        () ->
            recovery.execute(
                entry(opening, observed),
                DeltaRegistrationCommand.publish(observed.identity()),
                true));
    verifyNoInteractions(catalog);
    assertTrue(entries.isEmpty());
  }

  @Test
  void statusRejectsMismatchedExistingPairBeforeRequest() {
    Catalog catalog = mock(Catalog.class);
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery =
        new DeltaRegistrationRecovery(
            new DeltaRegistrationClient(() -> catalog, () -> catalog), entries::add);
    assertThrows(
        IllegalStateException.class,
        () -> recovery.status(entry(reserve(), pending(reserveOtherSchema()))));
    verifyNoInteractions(catalog);
    assertTrue(entries.isEmpty());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("openings")
  void matchingPublishedProgressionNeedsNoPendingMarker(DeltaRegistrationCommand opening)
      throws Exception {
    DeltaRegistrationReceipt published = published(pending(opening));
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery = recovery(published, entries::add);
    assertEquals(published.toProperties(), recovery.status(entry(opening, null)).toProperties());
    DeltaRegistrationJournal journal = DeltaRegistrationJournal.durable(directory);
    journal.write(entries.get(0));
    DeltaRegistrationJournal.Entry decoded =
        DeltaRegistrationJournal.read(directory.resolve(entries.get(0).key + ".json"));
    assertEquals(published.toProperties(), decoded.receipt.toProperties());
    assertEquals(published.toProperties(), recovery.status(decoded).toProperties());
    assertEquals(
        published.toProperties(),
        recovery
            .execute(decoded, DeltaRegistrationCommand.publish(published.identity()), true)
            .toProperties());
    assertEquals(DeltaRegistrationJournal.Phase.INTENT, entries.get(2).phase);
    assertEquals(DeltaRegistrationJournal.Phase.OPERATOR_COMPLETED, entries.get(3).phase);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("operators")
  void typedOperatorIntentsAndReceiptsRemainRecoverable(
      DeltaRegistrationCommand operator, DeltaRegistrationReceipt observed) throws Exception {
    DeltaRegistrationJournal journal = DeltaRegistrationJournal.durable(directory);
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery = recovery(observed, entries::add);
    DeltaRegistrationJournal.Entry intent = entry(operator, null);
    journal.write(intent);
    DeltaRegistrationJournal.Entry decoded =
        DeltaRegistrationJournal.read(directory.resolve(intent.key + ".json"));
    assertEquals(observed.toProperties(), recovery.status(decoded).toProperties());
    journal.write(entries.get(0));
    decoded = DeltaRegistrationJournal.read(directory.resolve(intent.key + ".json"));
    assertEquals(observed.toProperties(), recovery.status(decoded).toProperties());
  }

  private static Stream<Arguments> mismatches() {
    DeltaRegistrationCommand replacement = replace();
    return Stream.of(
        Arguments.of("exact schema", reserve(), pending(reserveOtherSchema())),
        Arguments.of(
            "v2 mode",
            contract(DeltaSchemaContract.OperationMode.SCHEMA_ONLY, SCHEMA),
            pending(contract(DeltaSchemaContract.OperationMode.QUERY, SCHEMA))),
        Arguments.of(
            "v2 schema",
            contract(DeltaSchemaContract.OperationMode.QUERY, SCHEMA),
            pending(contract(DeltaSchemaContract.OperationMode.QUERY, OTHER_SCHEMA))),
        Arguments.of(
            "exact versus v2",
            reserve(),
            pending(contract(DeltaSchemaContract.OperationMode.SCHEMA_ONLY, SCHEMA))),
        Arguments.of("opening operation", reserve(), pending(adopt())),
        Arguments.of(
            "create generation",
            reserve(),
            pending(reserve(), DeltaRegistrationIdentity.of(11, 1, "token"))),
        Arguments.of(
            "replacement id",
            replacement,
            pending(replacement, DeltaRegistrationIdentity.of(12, 5, "token"))),
        Arguments.of(
            "replacement generation",
            replacement,
            pending(replacement, DeltaRegistrationIdentity.of(11, 6, "token"))),
        Arguments.of(
            "replacement token",
            replacement,
            pending(replacement, DeltaRegistrationIdentity.of(11, 5, "other"))),
        Arguments.of(
            "replacement run",
            replacement,
            pending(
                DeltaRegistrationCommand.beginReplace(
                    DeltaRegistrationIdentity.of(11, 4, "previous"), "token", "other", SCHEMA))),
        Arguments.of(
            "replacement opening operation",
            replacement,
            pending(reserve(), DeltaRegistrationIdentity.of(11, 5, "token"))));
  }

  private static Stream<DeltaRegistrationCommand> openings() {
    return Stream.of(
        reserve(), replace(), adopt(), contract(DeltaSchemaContract.OperationMode.QUERY, SCHEMA));
  }

  private static Stream<Arguments> operators() {
    DeltaRegistrationReceipt pending = pending(reserve());
    DeltaRegistrationIdentity identity = pending.identity();
    DeltaRegistrationReceipt abandoned =
        DeltaRegistrationReceipt.of(
            identity,
            pending.operation(),
            DeltaRegistrationReceipt.State.ABANDONED,
            "run",
            DeltaRegistrationReceipt.Evidence.UNRESOLVED,
            "ABANDONED_BY_RECOVERY",
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty());
    return Stream.of(
        Arguments.of(DeltaRegistrationCommand.publish(identity), published(pending)),
        Arguments.of(
            DeltaRegistrationCommand.handoff(identity, "run", "operator-reference"), pending),
        Arguments.of(DeltaRegistrationCommand.abort(identity), abandoned));
  }

  private static DeltaRegistrationCommand reserve() {
    return DeltaRegistrationCommand.reserve("token", "run", SCHEMA);
  }

  private static DeltaRegistrationCommand reserveOtherSchema() {
    return DeltaRegistrationCommand.reserve("token", "run", OTHER_SCHEMA);
  }

  private static DeltaRegistrationCommand replace() {
    return DeltaRegistrationCommand.beginReplace(
        DeltaRegistrationIdentity.of(11, 4, "previous"), "token", "run", SCHEMA);
  }

  private static DeltaRegistrationCommand adopt() {
    return DeltaRegistrationCommand.adopt("token", LOCATION, "run", "cutover-reference");
  }

  private static DeltaRegistrationCommand contract(
      DeltaSchemaContract.OperationMode mode, String schema) {
    return DeltaRegistrationCommand.reserve(
        "token", "run", DeltaSchemaContract.spark35Delta33NameMapping(mode, schema));
  }

  private static DeltaRegistrationReceipt pending(DeltaRegistrationCommand opening) {
    boolean replacement = opening.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE;
    return pending(opening, DeltaRegistrationIdentity.of(11, replacement ? 5 : 0, "token"));
  }

  private static DeltaRegistrationReceipt pending(
      DeltaRegistrationCommand opening, DeltaRegistrationIdentity identity) {
    DeltaOperationMarker marker = null;
    DeltaRegistrationReceipt.State state;
    if (opening.operation() == DeltaRegistrationCommand.Operation.ADOPT) {
      state = DeltaRegistrationReceipt.State.ADOPTING;
    } else if (opening.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE) {
      state = DeltaRegistrationReceipt.State.REPLACING;
      marker =
          DeltaOperationMarker.replace(
              identity.id(),
              identity.generation(),
              identity.token(),
              LOCATION,
              opening.exactSchemaJson().orElseThrow(),
              2,
              "native-id");
    } else {
      state = DeltaRegistrationReceipt.State.RESERVED;
      marker =
          opening.schemaContract().isPresent()
              ? DeltaOperationMarker.createWithSchemaContract(
                  identity.id(),
                  identity.generation(),
                  identity.token(),
                  LOCATION,
                  opening.schemaContract().orElseThrow())
              : DeltaOperationMarker.create(
                  identity.id(),
                  identity.generation(),
                  identity.token(),
                  LOCATION,
                  opening.exactSchemaJson().orElseThrow());
    }
    return DeltaRegistrationReceipt.of(
        identity,
        opening.operation(),
        state,
        opening.runId().orElseThrow(),
        DeltaRegistrationReceipt.Evidence.NONE,
        "NONE",
        Optional.ofNullable(marker),
        Optional.empty(),
        Optional.empty(),
        OptionalLong.empty());
  }

  private static DeltaRegistrationReceipt published(DeltaRegistrationReceipt pending) {
    return DeltaRegistrationReceipt.of(
        pending.identity(),
        pending.operation(),
        DeltaRegistrationReceipt.State.PUBLISHED,
        pending.runId(),
        pending.operation() == DeltaRegistrationCommand.Operation.ADOPT
            ? DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT
            : DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
        "NONE",
        Optional.empty(),
        Optional.of(LOCATION),
        Optional.of("native-id"),
        OptionalLong.of(3));
  }

  private static DeltaRegistrationRecovery recovery(
      DeltaRegistrationReceipt observed, DeltaRegistrationJournal journal) {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    Table table = mock(Table.class);
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(tables.alterTable(any(), any())).thenReturn(table);
    when(table.properties()).thenReturn(observed.toProperties());
    return new DeltaRegistrationRecovery(
        new DeltaRegistrationClient(() -> catalog, () -> catalog), journal);
  }

  private static DeltaRegistrationJournal.Entry entry(
      DeltaRegistrationCommand command, DeltaRegistrationReceipt receipt) {
    return new DeltaRegistrationJournal.Entry(
        UUID.randomUUID().toString(),
        "canonical",
        "db",
        "t",
        command,
        DeltaRegistrationJournal.Phase.STATUS_NEEDED,
        receipt);
  }
}
