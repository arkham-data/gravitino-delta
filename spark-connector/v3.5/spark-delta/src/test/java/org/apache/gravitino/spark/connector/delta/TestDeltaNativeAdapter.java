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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.hooks.UpdateCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/** Local native transactions with injected catalog APIs; no server/provider/Kernel fixtures. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("deprecation")
class TestDeltaNativeAdapter {
  @TempDir static Path directory;
  private SparkSession spark;
  private GravitinoDeltaCatalogSpark35 adapter;
  private final Map<String, DeltaRegistrationReceipt> receipts = new HashMap<>();
  private final Map<String, String> locations = new HashMap<>();
  private final AtomicLong ids = new AtomicLong(50);
  private final List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
  private String wrongNativeId;

  @BeforeAll
  void start() {
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    when(catalog.name()).thenReturn("canonical");
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(catalog.asSchemas()).thenReturn(schemas);
    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(Map.of());
    when(schemas.loadSchema("db")).thenReturn(schema);
    when(schemas.listSchemas()).thenReturn(new String[] {"db"});
    when(tables.loadTable(any()))
        .thenAnswer(
            call -> {
              NameIdentifier name = call.getArgument(0);
              String key = name.name();
              DeltaRegistrationReceipt receipt = receipts.get(key);
              if (receipt == null || receipt.state() != DeltaRegistrationReceipt.State.PUBLISHED) {
                throw new org.apache.gravitino.exceptions.NoSuchTableException("absent");
              }
              receipt = published(key, receipt);
              return table(receipt);
            });
    when(tables.createTable(any(), any(), any(), any()))
        .thenAnswer(
            call -> {
              NameIdentifier name = call.getArgument(0);
              Map<String, String> properties = call.getArgument(3);
              return table(
                  open(
                      name.name(),
                      DeltaRegistrationCommand.decode(
                          properties.get(DeltaRegistrationCommand.COMMAND_PROPERTY))));
            });
    when(tables.alterTable(any(), any()))
        .thenAnswer(
            call -> {
              NameIdentifier name = call.getArgument(0);
              TableChange.SetProperty change = call.getArgument(1);
              DeltaRegistrationCommand command = DeltaRegistrationCommand.decode(change.getValue());
              if (command.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE) {
                return table(open(name.name(), command));
              }
              DeltaRegistrationReceipt receipt = receipts.get(name.name());
              if (command.operation() == DeltaRegistrationCommand.Operation.PUBLISH) {
                Snapshot snapshot = nativeTable(name.name()).update();
                assertEquals(
                    receipt.marker().orElseThrow().encode(),
                    snapshot.metadata().configuration().apply(DeltaOperationMarker.PROPERTY_KEY));
                receipt = published(name.name(), receipt);
                receipts.put(name.name(), receipt);
              }
              return table(receipt);
            });
    NativeTestCatalog.client = new DeltaRegistrationClient(() -> catalog, () -> catalog);
    NativeTestCatalog.journal = entries::add;
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-adapter-native-test")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.registry", NativeTestCatalog.class.getName())
            .config(DeltaCatalogProperties.UPDATE_ENABLED, "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    adapter =
        (GravitinoDeltaCatalogSpark35) spark.sessionState().catalogManager().catalog("registry");
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db").collect();
  }

  @AfterAll
  void stop() {
    if (spark != null) {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
    NativeTestCatalog.client = null;
    NativeTestCatalog.journal = null;
  }

  @Test
  void nativeDescriptorDmlAndCompletedHooksPreserveCollidingSessionTable() throws Exception {
    spark
        .sql(
            "CREATE TABLE spark_catalog.db.sentinel (sentinel STRING) USING parquet TBLPROPERTIES ('sentinel'='keep')")
        .collect();
    spark.sql("INSERT INTO spark_catalog.db.sentinel VALUES ('untouched')").collect();
    CatalogTable before =
        spark
            .sessionState()
            .catalog()
            .getTableMetadata(new TableIdentifier("sentinel", Option.apply("db")));
    spark.sql("CREATE TABLE registry.db.sentinel (id INT, value STRING) USING delta").collect();
    DeltaTableV2 table = (DeltaTableV2) adapter.loadTable(id("sentinel"));
    assertEquals("canonical", table.catalogTable().get().identifier().catalog().get());
    assertEquals(
        "canonical", table.toLogicalRelation().catalogTable().get().identifier().catalog().get());
    assertEquals(
        "db", table.toLogicalRelation().catalogTable().get().identifier().database().get());
    assertEquals("sentinel", table.toLogicalRelation().catalogTable().get().identifier().table());
    spark.sql("INSERT INTO registry.db.sentinel VALUES (1,'a'),(2,'b')").collect();
    spark.sql("UPDATE registry.db.sentinel SET value='updated' WHERE id=1").collect();
    spark.sql("DELETE FROM registry.db.sentinel WHERE id=2").collect();
    spark
        .sql(
            "MERGE INTO registry.db.sentinel t USING (SELECT 3 AS id, 'new' AS value) s ON t.id=s.id WHEN NOT MATCHED THEN INSERT *")
        .collect();
    assertEquals(2, spark.sql("SELECT * FROM registry.db.sentinel").count());
    assertTrue(UpdateCatalog.awaitCompletion(10000));
    CatalogTable after =
        spark
            .sessionState()
            .catalog()
            .getTableMetadata(new TableIdentifier("sentinel", Option.apply("db")));
    assertEquals(before, after);
    assertEquals(
        "untouched",
        spark.sql("SELECT sentinel FROM spark_catalog.db.sentinel").first().getString(0));
    assertFalse(
        spark
            .sessionState()
            .catalog()
            .tableExists(new TableIdentifier("native_only", Option.apply("db"))));
  }

  @Test
  void nativeCreateReplaceCorAndAlterUsePathAndRefreshMetadata() {
    spark.sql("CREATE TABLE registry.db.native_only USING delta AS SELECT 1 AS id").collect();
    spark
        .sql("CREATE OR REPLACE TABLE registry.db.native_only USING delta AS SELECT 2 AS id")
        .collect();
    assertEquals(2, spark.sql("SELECT id FROM registry.db.native_only").first().getInt(0));
    spark
        .sql("CREATE OR REPLACE TABLE registry.db.absent_cor USING delta AS SELECT 7 AS id")
        .collect();
    assertEquals(7, spark.sql("SELECT id FROM registry.db.absent_cor").first().getInt(0));
    spark.sql("ALTER TABLE registry.db.native_only ADD COLUMNS (extra STRING)").collect();
    assertTrue(
        Arrays.asList(adapter.loadTable(id("native_only")).schema().fieldNames())
            .contains("extra"));
    spark.sql("RESTORE TABLE registry.db.native_only TO VERSION AS OF 1").collect();
    assertFalse(
        Arrays.asList(adapter.loadTable(id("native_only")).schema().fieldNames())
            .contains("extra"));
    assertTrue(UpdateCatalog.awaitCompletion(10000));
    assertFalse(
        spark
            .sessionState()
            .catalog()
            .tableExists(new TableIdentifier("native_only", Option.apply("db"))));
  }

  @Test
  void currentHeadIsCheckedSeparatelyFromHistoricalSelection() {
    spark.sql("CREATE TABLE registry.db.history USING delta AS SELECT 1 AS id").collect();
    String old = nativeTable("history").update().metadata().id();
    long historicalMicros = nativeTable("history").update().timestamp() * 1000;
    spark
        .sql("CREATE OR REPLACE TABLE registry.db.history USING delta AS SELECT 2 AS id")
        .collect();
    DeltaTableV2 historical = (DeltaTableV2) adapter.loadTable(id("history"), "0");
    assertEquals(0, historical.initialSnapshot().version());
    assertEquals(old, historical.initialSnapshot().metadata().id());
    assertEquals(
        0,
        ((DeltaTableV2) adapter.loadTable(id("history"), historicalMicros))
            .initialSnapshot()
            .version());
    wrongNativeId = "wrong-backend-id";
    try {
      assertThrows(IllegalStateException.class, () -> adapter.loadTable(id("history")));
      assertThrows(IllegalStateException.class, () -> adapter.loadTable(id("history"), "0"));
      int before = entries.size();
      assertThrows(
          IllegalStateException.class,
          () ->
              adapter
                  .stageReplace(
                      id("history"),
                      new StructType().add("id", DataTypes.IntegerType),
                      new org.apache.spark.sql.connector.expressions.Transform[0],
                      Map.of())
                  .commitStagedChanges());
      assertEquals(before, entries.size());
    } finally {
      wrongNativeId = null;
    }
  }

  @Test
  void wholeAlterListIsAdmittedBeforeNativeMutationAndEntryChecksRemainEnabled() {
    spark.sql("CREATE TABLE registry.db.admission (id INT) USING delta").collect();
    long version = nativeTable("admission").update().version();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            adapter.alterTable(
                id("admission"),
                org.apache.spark.sql.connector.catalog.TableChange.setProperty("admitted", "first"),
                org.apache.spark.sql.connector.catalog.TableChange.setProperty(
                    "location", "file:/wrong")));
    assertEquals(version, nativeTable("admission").update().version());
    spark.conf().set(DeltaCatalogProperties.UPDATE_ENABLED, "true");
    try {
      assertThrows(IllegalArgumentException.class, () -> adapter.loadTable(id("admission")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              adapter.stageCreate(
                  id("rejected"),
                  new StructType(),
                  new org.apache.spark.sql.connector.expressions.Transform[0],
                  Map.of()));
    } finally {
      spark.conf().set(DeltaCatalogProperties.UPDATE_ENABLED, "false");
    }
  }

  @Test
  void mappedSchemaAndQueryUseClosedContractAndExplicitNoneOverridesDefault() {
    spark
        .sql(
            "CREATE TABLE registry.db.mapped_declared (id INT NOT NULL) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='name')")
        .collect();
    spark
        .sql(
            "CREATE TABLE registry.db.mapped_query USING delta TBLPROPERTIES ('delta.columnMapping.mode'='name') AS SELECT 1 AS id")
        .collect();
    DeltaRegistrationCommand declared = reservation("mapped_declared"),
        query = reservation("mapped_query");
    assertEquals(
        org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode
            .SCHEMA_ONLY,
        declared.schemaContract().orElseThrow().operationMode());
    assertEquals(
        org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode
            .QUERY,
        query.schemaContract().orElseThrow().operationMode());
    assertFalse(nativeTable("mapped_declared").update().schema().fields()[0].nullable());
    spark.conf().set(DeltaCatalogProperties.MAPPING_DEFAULT, "name");
    try {
      spark
          .sql(
              "CREATE TABLE registry.db.explicit_none (id INT) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='none')")
          .collect();
      assertTrue(reservation("explicit_none").exactSchemaJson().isPresent());
      assertEquals(
          "none",
          nativeTable("explicit_none")
              .update()
              .metadata()
              .configuration()
              .apply("delta.columnMapping.mode"));
    } finally {
      spark.conf().unset(DeltaCatalogProperties.MAPPING_DEFAULT);
    }
  }

  @Test
  void schemaOnlyCorSelectsAuthoritativeCreateThenReplace() {
    spark.sql("CREATE OR REPLACE TABLE registry.db.schema_cor (id INT) USING delta").collect();
    assertEquals(0, nativeTable("schema_cor").update().version());
    spark.sql("CREATE OR REPLACE TABLE registry.db.schema_cor (id BIGINT) USING delta").collect();
    assertEquals(1, nativeTable("schema_cor").update().version());
    assertEquals(
        DataTypes.LongType, adapter.loadTable(id("schema_cor")).schema().fields()[0].dataType());
  }

  private DeltaRegistrationCommand reservation(String name) {
    return entries.stream()
        .filter(
            entry ->
                entry.table.equals(name)
                    && entry.command.operation() == DeltaRegistrationCommand.Operation.RESERVE)
        .findFirst()
        .orElseThrow()
        .command;
  }

  private DeltaRegistrationReceipt open(String name, DeltaRegistrationCommand command) {
    boolean replace = command.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE;
    long identity = replace ? command.expected().orElseThrow().id() : ids.incrementAndGet();
    long generation = replace ? command.expected().orElseThrow().generation() + 1 : 0;
    String token =
        replace ? command.replacementToken().orElseThrow() : command.token().orElseThrow();
    String location =
        locations.computeIfAbsent(name, key -> directory.resolve("data-" + key).toUri().toString());
    Snapshot base = replace ? nativeTable(name).update() : null;
    DeltaOperationMarker marker;
    if (command.schemaContract().isPresent()) {
      marker =
          replace
              ? DeltaOperationMarker.replaceWithSchemaContract(
                  identity,
                  generation,
                  token,
                  location,
                  command.schemaContract().orElseThrow(),
                  base.version(),
                  base.metadata().id())
              : DeltaOperationMarker.createWithSchemaContract(
                  identity, generation, token, location, command.schemaContract().orElseThrow());
    } else {
      marker =
          replace
              ? DeltaOperationMarker.replace(
                  identity,
                  generation,
                  token,
                  location,
                  command.exactSchemaJson().orElseThrow(),
                  base.version(),
                  base.metadata().id())
              : DeltaOperationMarker.create(
                  identity, generation, token, location, command.exactSchemaJson().orElseThrow());
    }
    DeltaRegistrationReceipt receipt =
        DeltaRegistrationReceipt.of(
            DeltaRegistrationIdentity.of(identity, generation, token),
            command.operation(),
            replace
                ? DeltaRegistrationReceipt.State.REPLACING
                : DeltaRegistrationReceipt.State.RESERVED,
            command.runId().orElseThrow(),
            DeltaRegistrationReceipt.Evidence.NONE,
            "NONE",
            Optional.of(marker),
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty());
    receipts.put(name, receipt);
    return receipt;
  }

  private DeltaRegistrationReceipt published(String name, DeltaRegistrationReceipt receipt) {
    Snapshot snapshot = nativeTable(name).update();
    return DeltaRegistrationReceipt.of(
        receipt.identity(),
        receipt.operation(),
        DeltaRegistrationReceipt.State.PUBLISHED,
        receipt.runId(),
        DeltaRegistrationReceipt.Evidence.EXACT_NATIVE_COMMIT,
        "NONE",
        Optional.empty(),
        Optional.of(locations.get(name)),
        Optional.of(wrongNativeId == null ? snapshot.metadata().id() : wrongNativeId),
        OptionalLong.of(snapshot.version()));
  }

  private DeltaTableV2 nativeTable(String name) {
    return DeltaTableV2.apply(
        spark,
        new org.apache.hadoop.fs.Path(locations.get(name)),
        Option.empty(),
        Option.empty(),
        Option.empty(),
        DeltaTableV2.apply$default$6());
  }

  private static Identifier id(String name) {
    return Identifier.of(new String[] {"db"}, name);
  }

  private static Table table(DeltaRegistrationReceipt receipt) {
    Table table = mock(Table.class);
    when(table.properties()).thenReturn(receipt.toProperties());
    return table;
  }
}
