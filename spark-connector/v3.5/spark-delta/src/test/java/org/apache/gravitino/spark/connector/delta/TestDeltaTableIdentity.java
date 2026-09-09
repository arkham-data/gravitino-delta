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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.OptionalLong;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.Test;

class TestDeltaTableIdentity {
  @Test
  void replacementRequiresExactCurrentNativeBaseIdAndVersion() {
    DeltaTableV2 table = mock(DeltaTableV2.class);
    Snapshot snapshot = mock(Snapshot.class);
    Metadata metadata = mock(Metadata.class);
    when(table.update()).thenReturn(snapshot);
    when(snapshot.metadata()).thenReturn(metadata);
    when(metadata.id()).thenReturn("base");
    when(snapshot.version()).thenReturn(7L);
    DeltaRegistrationReceipt pending =
        DeltaRegistrationReceipt.of(
            DeltaRegistrationIdentity.of(11, 1, "replace"),
            DeltaRegistrationCommand.Operation.BEGIN_REPLACE,
            DeltaRegistrationReceipt.State.REPLACING,
            "run",
            DeltaRegistrationReceipt.Evidence.NONE,
            "NONE",
            Optional.of(
                DeltaOperationMarker.replace(
                    11,
                    1,
                    "replace",
                    "file:/owned",
                    TestDeltaRegistrationClient.SCHEMA,
                    7,
                    "base")),
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty());
    assertDoesNotThrow(() -> DeltaTableIdentity.validate(table, pending));
    when(snapshot.version()).thenReturn(8L);
    assertThrows(IllegalStateException.class, () -> DeltaTableIdentity.validate(table, pending));
    when(snapshot.version()).thenReturn(7L);
    when(metadata.id()).thenReturn("wrong");
    assertThrows(IllegalStateException.class, () -> DeltaTableIdentity.validate(table, pending));
  }

  @Test
  void publishedReadAllowsHeadAdvanceButRejectsVersionRegression() {
    DeltaTableV2 table = mock(DeltaTableV2.class);
    Snapshot snapshot = mock(Snapshot.class);
    Metadata metadata = mock(Metadata.class);
    when(table.update()).thenReturn(snapshot);
    when(snapshot.metadata()).thenReturn(metadata);
    when(metadata.id()).thenReturn("current");
    when(snapshot.version()).thenReturn(8L);
    DeltaRegistrationReceipt published =
        DeltaRegistrationReceipt.of(
            DeltaRegistrationIdentity.of(11, 0, "create"),
            DeltaRegistrationCommand.Operation.RESERVE,
            DeltaRegistrationReceipt.State.PUBLISHED,
            "run",
            DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
            "NONE",
            Optional.empty(),
            Optional.of("file:/owned"),
            Optional.of("current"),
            OptionalLong.of(7));
    assertDoesNotThrow(() -> DeltaTableIdentity.validate(table, published));
    when(snapshot.version()).thenReturn(6L);
    assertThrows(IllegalStateException.class, () -> DeltaTableIdentity.validate(table, published));
    verify(table, never()).initialSnapshot();
  }
}
