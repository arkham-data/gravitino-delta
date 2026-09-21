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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.Evidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Product;

class TestDeltaRangerRegistrationReader {
  private static final Identifier NAME = Identifier.of(new String[] {"sales"}, "orders");
  private static final String PREFIX = "delta.registration.receipt.";

  @Test
  void publishedReadReturnsDetachedCanonicalValuesAndRepeatedCompleteObservation() {
    try (Fixture f = new Fixture()) {
      PublishedObservation first = f.reader.read(NAME);
      assertNotNull(first);
      assertEquals("lake", first.metalake());
      assertEquals("delta_a", first.catalog());
      assertEquals("sales", first.database());
      assertEquals("orders", first.table());
      assertEquals(DeltaRegistrationIdentity.of(10, 2, "private-token"), first.identity());
      assertEquals(State.PUBLISHED, first.receipt().state());
      assertEquals("s3a://Bucket/exact/Path/", first.location());
      assertEquals("opaque-native:id/not-a-uuid", first.nativeId());
      assertEquals(7, first.nativeVersion());
      PublishedObservation second = f.reader.read(NAME);
      assertTrue(first.samePublication(second));
      assertFalse(first.samePublication(null));
      f.reader.requireLive(first);
      f.reader.requireLive(second);
      assertEquals(1, f.opens.get());
      assertEquals(2, f.tableReads.get());
      verify(f.client, times(2)).loadCatalog("delta_a");
      verifyNoMoreInteractions(f.client);
    }
  }

  @Test
  void nestedSchemaPreservesOrderTypesAndEveryNullabilityBoundary() {
    try (Fixture f = new Fixture()) {
      f.columns = nestedColumns("private-comment", false, false);
      StructType leaf =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("leaf", DataTypes.IntegerType, false)
              });
      StructType mapped =
          DataTypes.createStructType(
              new StructField[] {DataTypes.createStructField("value", DataTypes.StringType, true)});
      StructType expected =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField(
                    "payload",
                    DataTypes.createStructType(
                        new StructField[] {
                          DataTypes.createStructField(
                              "items", DataTypes.createArrayType(leaf, false), true),
                          DataTypes.createStructField(
                              "index",
                              DataTypes.createMapType(DataTypes.StringType, mapped, false),
                              false)
                        }),
                    true)
              });
      assertEquals(expected.json(), f.reader.read(NAME).schemaJson());
    }
  }

  @Test
  void scalarProjectionUsesExistingUnsignedAndUuidConverterSemantics() {
    try (Fixture f = new Fixture()) {
      f.columns =
          new Column[] {
            Column.of("unsigned", Types.LongType.unsigned()),
            Column.of("uuid", Types.UUIDType.get())
          };
      StructType expected =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("unsigned", DataTypes.createDecimalType(20, 0), true),
                DataTypes.createStructField("uuid", DataTypes.StringType, true)
              });
      assertEquals(expected.json(), f.reader.read(NAME).schemaJson());
    }
  }

  @Test
  void canonicalDatabaseAndTableDistinguishOtherwiseIdenticalObservations() {
    try (Fixture f = new Fixture()) {
      PublishedObservation original = f.reader.read(NAME);
      when(f.tables.loadTable(any(NameIdentifier.class))).thenReturn(f.table);
      PublishedObservation differentDatabase =
          f.reader.read(Identifier.of(new String[] {"other_sales"}, "orders"));
      assertEquals(original.identity(), differentDatabase.identity());
      assertFalse(original.samePublication(differentDatabase));
      when(f.table.name()).thenReturn("other_orders");
      PublishedObservation differentTable =
          f.reader.read(Identifier.of(new String[] {"sales"}, "other_orders"));
      assertEquals(original.identity(), differentTable.identity());
      assertFalse(original.samePublication(differentTable));
    }
  }

  @Test
  void commentsAreExcludedConsistentlyAtTopLevelAndInsideArraysAndMaps() {
    try (Fixture f = new Fixture()) {
      f.columns = nestedColumns("first-private-comment", false, false);
      PublishedObservation first = f.reader.read(NAME);
      f.columns = nestedColumns("second-private-comment", false, false);
      PublishedObservation second = f.reader.read(NAME);
      assertTrue(first.samePublication(second));
      assertFalse(first.schemaJson().contains("private-comment"));
      assertFalse(second.schemaJson().contains("private-comment"));
    }
  }

  @Test
  void originalPropertyMapsColumnArraysAndNestedTypeArraysCannotMutateObservation() {
    try (Fixture f = new Fixture()) {
      Types.StructType.Field[] children = {
        Types.StructType.Field.of("original", Types.IntegerType.get(), true, "comment")
      };
      Column mutable = mock(Column.class);
      when(mutable.name()).thenReturn("value");
      when(mutable.dataType()).thenReturn(Types.StructType.of(children));
      when(mutable.nullable()).thenReturn(false);
      f.columns = new Column[] {mutable};
      PublishedObservation first = f.reader.read(NAME);
      PublishedObservation same = f.reader.read(NAME);
      String schema = first.schemaJson();
      Map<String, String> receipt = new LinkedHashMap<>(first.receipt().toProperties());
      children[0] = Types.StructType.Field.of("changed", Types.StringType.get(), false, null);
      when(mutable.name()).thenReturn("changed");
      when(mutable.nullable()).thenReturn(true);
      f.columns[0] = Column.of("replacement", Types.BooleanType.get());
      f.properties.clear();
      assertEquals(schema, first.schemaJson());
      assertEquals(receipt, first.receipt().toProperties());
      assertTrue(first.samePublication(same));
      assertThrows(
          UnsupportedOperationException.class,
          () -> first.receipt().toProperties().put(PREFIX + "token", "replacement"));
    }
  }

  @Test
  void eachRegistrationNativeFieldAndFullReceiptChangeIsObservable() {
    try (Fixture f = new Fixture()) {
      Map<String, String> baseline = new LinkedHashMap<>(f.properties);
      PublishedObservation first = f.reader.read(NAME);
      Map<String, String> changes = new LinkedHashMap<>();
      changes.put("id", "11");
      changes.put("generation", "3");
      changes.put("token", "replacement-token");
      changes.put("location", "s3a://Bucket/other/Path/");
      changes.put("nativeId", "another-opaque-native-id");
      changes.put("nativeVersion", "8");
      changes.put("runId", "another-run");
      for (Map.Entry<String, String> change : changes.entrySet()) {
        f.properties = new LinkedHashMap<>(baseline);
        f.properties.put(PREFIX + change.getKey(), change.getValue());
        PublishedObservation changed = f.reader.read(NAME);
        assertFalse(first.samePublication(changed), change.getKey());
        if (Arrays.asList("id", "generation", "token").contains(change.getKey())) {
          assertNotEquals(first.identity(), changed.identity());
        } else {
          assertEquals(first.identity(), changed.identity());
        }
        assertEquals(
            change.getValue(), changed.receipt().toProperties().get(PREFIX + change.getKey()));
      }
      f.properties = new LinkedHashMap<>(baseline);
      f.properties.put(PREFIX + "operation", "RESERVE");
      f.properties.put(PREFIX + "evidence", "EXACT_NATIVE_COMMIT");
      assertFalse(first.samePublication(f.reader.read(NAME)));
    }
  }

  @Test
  void schemaChangeCannotHideBehindUnchangedNameAndIncarnation() {
    try (Fixture f = new Fixture()) {
      f.columns = nestedColumns(null, false, false);
      PublishedObservation original = f.reader.read(NAME);
      List<Column[]> variants = new ArrayList<>();
      Column[] reordered = nestedColumns(null, false, false);
      variants.add(new Column[] {reordered[1], reordered[0]});
      Column[] renamed = nestedColumns(null, false, false);
      renamed[0] = column("other-id", Types.LongType.get(), false, null);
      variants.add(renamed);
      Column[] nullable = nestedColumns(null, false, false);
      nullable[0] = column("id", Types.LongType.get(), true, null);
      variants.add(nullable);
      Column[] retyped = nestedColumns(null, false, false);
      retyped[0] = column("id", Types.StringType.get(), false, null);
      variants.add(retyped);
      variants.add(nestedColumns(null, true, false));
      variants.add(nestedColumns(null, false, true));
      Column[] nestedChanged = nestedColumns(null, false, false);
      nestedChanged[1] =
          column(
              "payload",
              Types.StructType.of(
                  Types.StructType.Field.of("different", Types.IntegerType.get(), false, null)),
              true,
              null);
      variants.add(nestedChanged);
      for (Column[] variant : variants) {
        f.columns = variant;
        PublishedObservation changed = f.reader.read(NAME);
        assertEquals(original.identity(), changed.identity());
        assertNotEquals(original.schemaJson(), changed.schemaJson());
        assertFalse(original.samePublication(changed));
      }
    }
  }

  @Test
  void wrongCatalogProviderOrReturnedTableRejects() {
    try (Fixture f = new Fixture()) {
      when(f.catalog.name()).thenReturn("other-catalog");
      assertThrows(SecurityException.class, () -> f.reader.read(NAME));
      when(f.catalog.name()).thenReturn("delta_a");
      when(f.catalog.provider()).thenReturn("lakehouse-iceberg");
      assertThrows(SecurityException.class, () -> f.reader.read(NAME));
      assertEquals(0, f.tableReads.get());
      when(f.catalog.provider()).thenReturn("lakehouse-delta");
      when(f.table.name()).thenReturn("other-table");
      assertThrows(SecurityException.class, () -> f.reader.read(NAME));
    }
  }

  @Test
  void invalidNamespaceAndLiteralNamesRejectBeforeAnyIo() {
    try (Fixture f = new Fixture()) {
      List<Identifier> invalid = new ArrayList<>();
      invalid.add(null);
      invalid.add(identifier(null, "orders"));
      invalid.add(identifier(new String[0], "orders"));
      invalid.add(identifier(new String[] {"a", "b"}, "orders"));
      for (String name : Arrays.asList(null, "", " ", "*", "a?")) {
        invalid.add(identifier(new String[] {name}, "orders"));
        invalid.add(identifier(new String[] {"sales"}, name));
      }
      for (Identifier id : invalid) {
        assertThrows(SecurityException.class, () -> f.reader.read(id));
      }
      assertEquals(0, f.opens.get());
      verifyNoInteractions(f.client, f.catalog, f.tables, f.table);
    }
  }

  @Test
  void missingMalformedAndPendingReceiptsReject() {
    try (Fixture f = new Fixture()) {
      Map<String, String> valid = new LinkedHashMap<>(f.properties);
      List<Map<String, String>> invalid = new ArrayList<>();
      invalid.add(null);
      invalid.add(Collections.emptyMap());
      for (String key :
          Arrays.asList("id", "generation", "token", "location", "nativeId", "nativeVersion")) {
        Map<String, String> missing = new LinkedHashMap<>(valid);
        missing.remove(PREFIX + key);
        invalid.add(missing);
      }
      for (String[] bad :
          new String[][] {
            {"id", "-1"}, {"generation", "bad"}, {"token", " "},
            {"nativeVersion", "-1"}, {"nativeId", " "}, {"unknown", "private-token"}
          }) {
        Map<String, String> properties = new LinkedHashMap<>(valid);
        properties.put(PREFIX + bad[0], bad[1]);
        invalid.add(properties);
      }
      invalid.add(
          DeltaRegistrationReceipt.of(
                  DeltaRegistrationIdentity.of(10, 2, "private-token"),
                  Operation.ADOPT,
                  State.ADOPTING,
                  "run",
                  Evidence.NONE,
                  "NONE",
                  Optional.empty(),
                  Optional.empty(),
                  Optional.empty(),
                  OptionalLong.empty())
              .toProperties());
      for (Map<String, String> properties : invalid) {
        f.properties = properties;
        assertSanitized(assertThrows(SecurityException.class, () -> f.reader.read(NAME)));
      }
    }
  }

  @Test
  void missingMalformedDuplicateAndUnsupportedColumnsReject() {
    try (Fixture f = new Fixture()) {
      Column unnamed = mock(Column.class);
      when(unnamed.dataType()).thenReturn(Types.IntegerType.get());
      Column untyped = mock(Column.class);
      when(untyped.name()).thenReturn("value");
      List<Column[]> invalid =
          Arrays.asList(
              null,
              new Column[0],
              new Column[] {null},
              new Column[] {unnamed},
              new Column[] {untyped},
              new Column[] {Column.of(" ", Types.IntegerType.get())},
              new Column[] {
                Column.of("id", Types.IntegerType.get()), Column.of("id", Types.StringType.get())
              },
              new Column[] {Column.of("value", Types.UnparsedType.of("private-token"))},
              new Column[] {
                Column.of("value", Types.StructType.of(new Types.StructType.Field[] {null}))
              },
              new Column[] {
                Column.of(
                    "value",
                    Types.StructType.of(
                        Types.StructType.Field.of(" ", Types.IntegerType.get(), true, null)))
              });
      for (Column[] columns : invalid) {
        f.columns = columns;
        assertSanitized(assertThrows(SecurityException.class, () -> f.reader.read(NAME)));
      }
    }
  }

  @Test
  void readerIdentityCannotBeReplacedByAnotherReaderOnSameOrDifferentClient() {
    try (Fixture f = new Fixture();
        Fixture other = new Fixture()) {
      PublishedObservation observed = f.reader.read(NAME);
      DeltaRangerRegistrationReader sameClient =
          new DeltaRangerRegistrationReader(f.metadata, "lake", "delta_a");
      PublishedObservation replacement = sameClient.read(NAME);
      assertFalse(observed.samePublication(replacement));
      assertFalse(observed.samePublication(other.reader.read(NAME)));
      assertThrows(SecurityException.class, () -> sameClient.requireLive(observed));
      assertThrows(SecurityException.class, () -> other.reader.requireLive(observed));
      assertThrows(SecurityException.class, () -> f.reader.requireLive(null));
      f.reader.requireLive(observed);
    }
  }

  @Test
  void closedAndExpiredObservationsCannotRenewMetadataLease() {
    try (Fixture f = new Fixture();
        Fixture expired = new Fixture()) {
      PublishedObservation observed = f.reader.read(NAME);
      PublishedObservation old = expired.reader.read(NAME);
      f.metadata.close();
      assertThrows(SecurityException.class, () -> f.reader.requireLive(observed));
      assertThrows(SecurityException.class, () -> f.reader.read(NAME));
      expired.expire();
      assertThrows(SecurityException.class, () -> expired.reader.requireLive(old));
      assertThrows(SecurityException.class, () -> expired.reader.read(NAME));
      assertEquals(1, f.opens.get());
      assertEquals(1, expired.opens.get());
    }
  }

  @Test
  void expirationAndClosureDuringTableIoRejectLatePublishedObservation() {
    try (Fixture expired = new Fixture();
        Fixture closed = new Fixture()) {
      expired.loadHook = expired::expire;
      closed.loadHook = closed.metadata::close;
      assertSanitized(assertThrows(SecurityException.class, () -> expired.reader.read(NAME)));
      assertSanitized(assertThrows(SecurityException.class, () -> closed.reader.read(NAME)));
      assertEquals(1, expired.tableReads.get());
      assertEquals(1, closed.tableReads.get());
    }
  }

  @Test
  void expirationAndClosureDuringSchemaDecodingRejectLateObservation() {
    try (Fixture expired = new Fixture();
        Fixture closed = new Fixture()) {
      for (Fixture f : Arrays.asList(expired, closed)) {
        Column delayed = mock(Column.class);
        when(delayed.name()).thenReturn("id");
        when(delayed.dataType())
            .thenAnswer(
                call -> {
                  if (f == expired) {
                    f.expire();
                  } else {
                    f.metadata.close();
                  }
                  return Types.LongType.get();
                });
        f.columns = new Column[] {delayed};
        assertSanitized(assertThrows(SecurityException.class, () -> f.reader.read(NAME)));
      }
    }
  }

  @Test
  void upstreamFailuresAndConversionFailuresNeverEscapeWithSensitiveCauses() {
    try (Fixture f = new Fixture()) {
      when(f.tables.loadTable(any(NameIdentifier.class)))
          .thenThrow(
              new IllegalArgumentException("private-token s3a://private-location credential"));
      assertSanitized(assertThrows(SecurityException.class, () -> f.reader.read(NAME)));
    }
  }

  @Test
  void observationHasConstantRedactedRenderingAndNoSerializationOrScalaProduct() {
    try (Fixture f = new Fixture();
        Fixture other = new Fixture()) {
      PublishedObservation observed = f.reader.read(NAME);
      assertEquals(observed.toString(), other.reader.read(NAME).toString());
      assertFalse(observed.toString().contains("private-token"));
      assertFalse(observed.toString().contains("Bucket"));
      assertFalse(Serializable.class.isAssignableFrom(observed.getClass()));
      assertFalse(Product.class.isAssignableFrom(observed.getClass()));
    }
  }

  @Test
  void scopedMetadataReadLeavesQueryIdentityUnchangedAndOnlyUsesOwnedReadClient() throws Exception {
    try (Fixture f = new Fixture()) {
      UserGroupInformation query =
          UserGroupInformation.createUserForTesting("query-reader", new String[] {"finance"});
      when(f.tables.loadTable(any(NameIdentifier.class)))
          .thenAnswer(
              call -> {
                assertEquals(
                    "query-reader", UserGroupInformation.getCurrentUser().getShortUserName());
                return f.table;
              });
      query.doAs(
          (PrivilegedExceptionAction<Void>)
              () -> {
                f.reader.read(NAME);
                assertEquals(
                    "query-reader", UserGroupInformation.getCurrentUser().getShortUserName());
                return null;
              });
      verify(f.client).loadCatalog("delta_a");
      verifyNoMoreInteractions(f.client);
      assertEquals(1, f.opens.get());
    }
  }

  private static Identifier identifier(String[] namespace, String name) {
    Identifier identifier = mock(Identifier.class);
    when(identifier.namespace()).thenReturn(namespace);
    when(identifier.name()).thenReturn(name);
    return identifier;
  }

  private static void assertSanitized(SecurityException failure) {
    assertNull(failure.getCause());
    assertEquals(0, failure.getSuppressed().length);
    assertFalse(failure.getMessage().contains("private-token"));
    assertFalse(failure.getMessage().contains("private-location"));
    assertFalse(failure.getMessage().contains("credential"));
  }

  private static Column column(String name, Type type, boolean nullable, String comment) {
    return Column.of(name, type, comment, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  private static Column[] nestedColumns(
      String comment, boolean elementNullable, boolean valueNullable) {
    return new Column[] {
      column("id", Types.LongType.get(), false, comment),
      column(
          "payload",
          Types.StructType.of(
              Types.StructType.Field.of(
                  "items",
                  Types.ListType.of(
                      Types.StructType.of(
                          Types.StructType.Field.of(
                              "leaf", Types.IntegerType.get(), false, comment)),
                      elementNullable),
                  true,
                  comment),
              Types.StructType.Field.of(
                  "index",
                  Types.MapType.of(
                      Types.StringType.get(),
                      Types.StructType.of(
                          Types.StructType.Field.of(
                              "value", Types.StringType.get(), true, comment)),
                      valueNullable),
                  false,
                  comment)),
          true,
          comment)
    };
  }

  private static final class Fixture implements AutoCloseable {
    final AtomicLong time = new AtomicLong();
    final AtomicInteger opens = new AtomicInteger();
    final AtomicInteger tableReads = new AtomicInteger();
    final GravitinoClient client = mock(GravitinoClient.class);
    final Catalog catalog = mock(Catalog.class);
    final TableCatalog tables = mock(TableCatalog.class);
    final Table table = mock(Table.class);
    final DeltaRangerMetadataClient metadata =
        new DeltaRangerMetadataClient(time::get, mock(ScheduledExecutorService.class));
    final DeltaRangerRegistrationReader reader =
        new DeltaRangerRegistrationReader(metadata, "lake", "delta_a");
    Map<String, String> properties =
        new LinkedHashMap<>(
            DeltaRegistrationReceipt.of(
                    DeltaRegistrationIdentity.of(10, 2, "private-token"),
                    Operation.ADOPT,
                    State.PUBLISHED,
                    "run",
                    Evidence.EXISTING_NATIVE_SNAPSHOT,
                    "NONE",
                    Optional.empty(),
                    Optional.of("s3a://Bucket/exact/Path/"),
                    Optional.of("opaque-native:id/not-a-uuid"),
                    OptionalLong.of(7))
                .toProperties());
    Column[] columns = {column("id", Types.LongType.get(), false, null)};
    Runnable loadHook;

    Fixture() {
      metadata.configure(
          "delta_a",
          Duration.ofSeconds(1),
          () -> {
            opens.incrementAndGet();
            return client;
          });
      when(client.loadCatalog("delta_a")).thenReturn(catalog);
      when(catalog.name()).thenReturn("delta_a");
      when(catalog.provider()).thenReturn("lakehouse-delta");
      when(catalog.asTableCatalog()).thenReturn(tables);
      when(tables.loadTable(NameIdentifier.of("sales", "orders")))
          .thenAnswer(
              call -> {
                tableReads.incrementAndGet();
                if (loadHook != null) {
                  loadHook.run();
                }
                return table;
              });
      when(table.name()).thenReturn("orders");
      when(table.properties()).thenAnswer(call -> properties);
      when(table.columns()).thenAnswer(call -> columns);
    }

    void expire() {
      time.set(TimeUnit.SECONDS.toNanos(1));
    }

    @Override
    public void close() {
      metadata.close();
    }
  }
}
