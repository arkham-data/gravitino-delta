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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

class TestDeltaPublicationStage {
  private final StructType schema = new StructType().add("id", DataTypes.IntegerType, false);
  private final AtomicInteger materializations = new AtomicInteger();
  private final AtomicInteger publications = new AtomicInteger();
  private final List<OperationMode> modes = new ArrayList<>();
  private final List<StructType> schemas = new ArrayList<>();
  private final StagedTable nativeStage =
      mock(StagedTable.class, withSettings().extraInterfaces(SupportsWrite.class));

  private DeltaPublicationStage stage(DeltaPublicationStage.Requested requested, Runnable check) {
    return new DeltaPublicationStage(
        "canonical.db.t",
        schema,
        new Transform[0],
        Map.of("comment", "stable"),
        requested,
        check,
        (mode, bound, options) -> {
          materializations.incrementAndGet();
          modes.add(mode);
          schemas.add(bound);
          return nativeStage;
        },
        publications::incrementAndGet,
        () -> {});
  }

  @Test
  void gettersAndAbortArePureBeforeMaterialization() {
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.CREATE, () -> {});
    assertEquals("canonical.db.t", stage.name());
    assertEquals(schema, stage.schema());
    assertEquals("stable", stage.properties().get("comment"));
    assertEquals(0, stage.partitioning().length);
    assertEquals(Set.of(TableCapability.V1_BATCH_WRITE), stage.capabilities());
    stage.abortStagedChanges();
    assertEquals(0, materializations.get());
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    verifyNoInteractions(nativeStage);
  }

  @Test
  void untouchedCommitMaterializesSchemaOnlyExactlyOnce() {
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.REPLACE, () -> {});
    stage.commitStagedChanges();
    stage.commitStagedChanges();
    assertEquals(List.of(OperationMode.SCHEMA_ONLY), modes);
    assertEquals(List.of(schema), schemas);
    assertEquals(1, materializations.get());
    assertEquals(1, publications.get());
    assertEquals(
        Set.of(TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE), stage.capabilities());
    verify(nativeStage, times(1)).commitStagedChanges();
  }

  @Test
  void queryNormalizesAndRejectsConflictingRepeatedBuilders() {
    when(((SupportsWrite) nativeStage).newWriteBuilder(any())).thenReturn(mock(WriteBuilder.class));
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.CREATE, () -> {});
    LogicalWriteInfo info = info(schema, Map.of("compression", "snappy"));
    assertSame(stage.newWriteBuilder(info), stage.newWriteBuilder(info));
    assertEquals(List.of(OperationMode.QUERY), modes);
    assertEquals(schema.asNullable(), schemas.get(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> stage.newWriteBuilder(info(schema, Map.of("compression", "gzip"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            stage.newWriteBuilder(
                info(
                    new StructType().add("other", DataTypes.LongType),
                    Map.of("compression", "snappy"))));
    assertEquals(1, materializations.get());
  }

  @Test
  void rejectsReservedWriteOptionsBeforeMaterialization() {
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.CREATE, () -> {});
    assertThrows(
        IllegalArgumentException.class,
        () -> stage.newWriteBuilder(info(schema, Map.of("option.path", "secret"))));
    assertEquals(0, materializations.get());
  }

  @Test
  void rechecksSessionBeforeNativeCommit() {
    AtomicInteger checks = new AtomicInteger();
    DeltaPublicationStage stage =
        stage(
            DeltaPublicationStage.Requested.CREATE,
            () -> {
              if (checks.incrementAndGet() > 1)
                throw new IllegalArgumentException("disabled required");
            });
    when(((SupportsWrite) nativeStage).newWriteBuilder(any())).thenReturn(mock(WriteBuilder.class));
    stage.newWriteBuilder(info(schema, Map.of()));
    assertThrows(IllegalArgumentException.class, stage::commitStagedChanges);
    verify(nativeStage, never()).commitStagedChanges();
  }

  @Test
  void failedPublicationNeverRepeatsNativeCommit() {
    DeltaPublicationStage stage =
        new DeltaPublicationStage(
            "t",
            schema,
            new Transform[0],
            Map.of(),
            DeltaPublicationStage.Requested.CREATE,
            () -> {},
            (mode, bound, options) -> nativeStage,
            () -> {
              throw new IllegalStateException("publication unavailable");
            },
            () -> {});
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    verify(nativeStage, times(1)).commitStagedChanges();
  }

  @Test
  void uncertainNativeCommitIsNeverAutomaticallyPublishedOrReplayed() {
    doThrow(new IllegalStateException("ambiguous native commit"))
        .when(nativeStage)
        .commitStagedChanges();
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.CREATE, () -> {});
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertThrows(IllegalStateException.class, stage::commitStagedChanges);
    assertEquals(0, publications.get());
    verify(nativeStage, times(1)).commitStagedChanges();
  }

  @Test
  void writeOptionsCannotOverrideTheBoundMappingContract() {
    DeltaPublicationStage stage = stage(DeltaPublicationStage.Requested.CREATE, () -> {});
    assertThrows(
        IllegalArgumentException.class,
        () -> stage.newWriteBuilder(info(schema, Map.of("delta.columnMapping.mode", "name"))));
    assertEquals(0, materializations.get());
  }

  private LogicalWriteInfo info(StructType value, Map<String, String> options) {
    LogicalWriteInfo info = mock(LogicalWriteInfo.class);
    when(info.schema()).thenReturn(value);
    when(info.options()).thenReturn(new CaseInsensitiveStringMap(options));
    return info;
  }
}
