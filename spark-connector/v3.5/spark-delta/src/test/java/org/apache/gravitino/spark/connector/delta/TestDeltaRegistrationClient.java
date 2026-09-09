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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

class TestDeltaRegistrationClient {
  static final String SCHEMA = "{\"type\":\"struct\",\"fields\":[]}";

  @Test
  void resolvesCurrentActorForEveryCommandAndUsesTypedPropertiesOnly() {
    Catalog first = mock(Catalog.class),
        second = mock(Catalog.class),
        metadata = mock(Catalog.class);
    TableCatalog one = mock(TableCatalog.class), two = mock(TableCatalog.class);
    when(first.asTableCatalog()).thenReturn(one);
    when(second.asTableCatalog()).thenReturn(two);
    AtomicReference<Catalog> current = new AtomicReference<>(first);
    DeltaRegistrationClient client = new DeltaRegistrationClient(() -> metadata, current::get);
    DeltaRegistrationReceipt receipt = pending("token", "run");
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(receipt.toProperties());
    when(one.alterTable(any(), any())).thenReturn(table);
    when(two.alterTable(any(), any())).thenReturn(table);
    Identifier id = Identifier.of(new String[] {"db"}, "t");
    DeltaRegistrationCommand command = DeltaRegistrationCommand.status(receipt.identity());
    assertEquals(receipt.identity(), client.submit(id, command).identity());
    current.set(second);
    assertEquals(receipt.identity(), client.submit(id, command).identity());
    verify(one).alterTable(any(), any());
    verify(two).alterTable(any(), any());
    verifyNoInteractions(metadata);
  }

  @Test
  void rejectsReceiptBoundToDifferentRunOrToken() {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    when(catalog.asTableCatalog()).thenReturn(tables);
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(pending("foreign", "other").toProperties());
    when(tables.createTable(any(), any(), any(), any())).thenReturn(table);
    DeltaRegistrationClient client = new DeltaRegistrationClient(() -> catalog, () -> catalog);
    assertThrows(
        IllegalStateException.class,
        () ->
            client.submit(
                Identifier.of(new String[] {"db"}, "t"),
                DeltaRegistrationCommand.reserve("token", "run", SCHEMA)));
  }

  static DeltaRegistrationReceipt pending(String token, String run) {
    return DeltaRegistrationReceipt.of(
        DeltaRegistrationIdentity.of(11, 0, token),
        DeltaRegistrationCommand.Operation.RESERVE,
        DeltaRegistrationReceipt.State.RESERVED,
        run,
        DeltaRegistrationReceipt.Evidence.NONE,
        "NONE",
        Optional.of(DeltaOperationMarker.create(11, 0, token, "file:/tmp/owned", SCHEMA)),
        Optional.empty(),
        Optional.empty(),
        OptionalLong.empty());
  }
}
