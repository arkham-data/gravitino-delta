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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRegistrationRecovery {
  @TempDir Path directory;

  @Test
  void boundedJournalRoundTripRejectsOversizedAndUnknownFields() throws Exception {
    DeltaRegistrationJournal.Entry entry =
        entry(
            DeltaRegistrationCommand.reserve("token", "run", TestDeltaRegistrationClient.SCHEMA),
            null);
    DeltaRegistrationJournal journal =
        DeltaRegistrationJournal.durable(directory.resolve("journal"));
    journal.write(entry);
    Path file = directory.resolve("journal").resolve(entry.key + ".json");
    DeltaRegistrationJournal.Entry decoded = DeltaRegistrationJournal.read(file);
    assertEquals(entry.command.encode(), decoded.command.encode());
    assertEquals(entry.phase, decoded.phase);
    assertNull(decoded.receipt);
    String valid = Files.readString(file);
    Files.writeString(
        file, valid.replace("\"version\":1", "\"version\":1,\"arbitrary\":\"secret\""));
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.read(file));
    Files.write(file, new byte[1024 * 1024 + 1]);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.read(file));
  }

  @Test
  void lostCreateUsesTokenRunStatusAndDoesNotSubmitReserveAgain() {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    when(catalog.asTableCatalog()).thenReturn(tables);
    DeltaRegistrationReceipt receipt = TestDeltaRegistrationClient.pending("token", "run");
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(receipt.toProperties());
    List<DeltaRegistrationCommand> commands = new ArrayList<>();
    when(tables.alterTable(any(), any()))
        .thenAnswer(
            call -> {
              TableChange.SetProperty change = call.getArgument(1);
              commands.add(DeltaRegistrationCommand.decode(change.getValue()));
              return table;
            });
    List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
    DeltaRegistrationRecovery recovery =
        new DeltaRegistrationRecovery(
            new DeltaRegistrationClient(() -> catalog, () -> catalog), entries::add);
    DeltaRegistrationJournal.Entry original =
        entry(
            DeltaRegistrationCommand.reserve("token", "run", TestDeltaRegistrationClient.SCHEMA),
            null);
    assertEquals(receipt.identity(), recovery.status(original).identity());
    assertEquals(DeltaRegistrationCommand.Operation.STATUS_BY_TOKEN, commands.get(0).operation());
    assertEquals("run", commands.get(0).runId().orElseThrow());
    assertEquals(DeltaRegistrationJournal.Phase.STATUS_NEEDED, entries.get(0).phase);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            recovery.execute(
                entries.get(0), DeltaRegistrationCommand.publish(receipt.identity()), false));
    assertEquals(1, commands.size());
  }

  @Test
  void recoveryMutationRequiresDurableIntentAndMatchingExpectedIdentity() {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    when(catalog.asTableCatalog()).thenReturn(tables);
    DeltaRegistrationReceipt receipt = TestDeltaRegistrationClient.pending("token", "run");
    DeltaRegistrationJournal.Entry original =
        entry(
            DeltaRegistrationCommand.reserve("token", "run", TestDeltaRegistrationClient.SCHEMA),
            receipt);
    DeltaRegistrationRecovery recovery =
        new DeltaRegistrationRecovery(
            new DeltaRegistrationClient(() -> catalog, () -> catalog),
            ignored -> {
              throw new IllegalStateException("journal failed");
            });
    assertThrows(
        IllegalStateException.class,
        () ->
            recovery.execute(original, DeltaRegistrationCommand.publish(receipt.identity()), true));
    verifyNoInteractions(tables);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            recovery.execute(
                original,
                DeltaRegistrationCommand.abort(DeltaRegistrationIdentity.of(99, 0, "other")),
                true));
  }

  @Test
  void journalRejectsTrailingJsonAndSymbolicLinkFiles() throws Exception {
    DeltaRegistrationJournal.Entry entry =
        entry(
            DeltaRegistrationCommand.reserve("token", "run", TestDeltaRegistrationClient.SCHEMA),
            null);
    DeltaRegistrationJournal journal =
        DeltaRegistrationJournal.durable(directory.resolve("journal"));
    journal.write(entry);
    Path file = directory.resolve("journal").resolve(entry.key + ".json");
    String saved = Files.readString(file);
    Files.writeString(file, saved + saved);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.read(file));
    Files.writeString(file, saved);
    Path link = directory.resolve("linked.json");
    Files.createSymbolicLink(link, file);
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationJournal.read(link));
  }

  @Test
  void lostBeginReplaceStatusUsesNextGenerationAndNewToken() {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    when(catalog.asTableCatalog()).thenReturn(tables);
    List<DeltaRegistrationCommand> commands = new ArrayList<>();
    when(tables.alterTable(any(), any()))
        .thenAnswer(
            call -> {
              TableChange.SetProperty change = call.getArgument(1);
              commands.add(DeltaRegistrationCommand.decode(change.getValue()));
              throw new IllegalStateException("captured");
            });
    DeltaRegistrationIdentity expected = DeltaRegistrationIdentity.of(11, 4, "previous");
    DeltaRegistrationRecovery recovery =
        new DeltaRegistrationRecovery(
            new DeltaRegistrationClient(() -> catalog, () -> catalog), ignored -> {});
    assertThrows(
        IllegalStateException.class,
        () ->
            recovery.status(
                entry(
                    DeltaRegistrationCommand.beginReplace(
                        expected, "new-token", "new-run", TestDeltaRegistrationClient.SCHEMA),
                    null)));
    assertEquals(DeltaRegistrationCommand.Operation.STATUS, commands.get(0).operation());
    assertEquals(
        DeltaRegistrationIdentity.of(11, 5, "new-token"), commands.get(0).expected().orElseThrow());
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
