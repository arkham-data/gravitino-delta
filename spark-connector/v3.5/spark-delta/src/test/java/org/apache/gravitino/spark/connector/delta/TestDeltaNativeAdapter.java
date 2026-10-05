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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
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
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.DataFrameWriterV2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.hooks.UpdateCatalog;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;
import scala.collection.Iterator;

/** Local native transactions with injected catalog APIs; no server/provider/Kernel fixtures. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("deprecation")
class TestDeltaNativeAdapter {
  private static final ObjectMapper JSON = new ObjectMapper();
  static AppendProbeFactory appendProbeFactory;
  @TempDir static Path directory;
  /** Factory installed by the test-only native append extension. */
  public interface AppendProbeFactory {
    /** Begins one append observation for the named target. */
    AppendProbeCapture begin(SparkSession spark, String targetTable, String mode, Path ownedTarget);
  }

  /** An active, thread-bound append observation. */
  public interface AppendProbeCapture extends AutoCloseable {
    /** Returns observations made while this capture was active. */
    AppendProbeReport report();

    /** Records an expected failure, including failures before physical planning. */
    void failure(Throwable failure);

    /** Records an assertion established by the owning fixture. */
    void verified(String assertion);

    /** Clears this capture. */
    @Override
    void close();
  }

  /** Bounded observations of a native append execution. */
  public interface AppendProbeReport {
    /** Ordered execution events. */
    List<String> events();

    /** Resolved and physical plan snapshots. */
    List<String> plans();

    /** Native helper action JSON before commit. */
    List<String> actions();

    /** Pending metadata JSON after the helper, or empty if no helper returned. */
    String pendingMetadata();

    /** Pending protocol JSON after the helper, or empty if no helper returned. */
    String pendingProtocol();

    /** Whether exact target, fallback input, schema and expression IDs were bound. */
    boolean inputBound();

    /** Exact logical write options bound to the physical writer. */
    Map<String, String> options();

    /** Number of intercepted insertion calls. */
    int entryCalls();

    /** Number of native file-generation helper calls. */
    int helperCalls();

    /** Number of native commit calls. */
    int commitCalls();

    /** Number of native idempotency skips. */
    int idempotentSkips();

    /** Whether a native transaction remained active after execution. */
    boolean activeTransactionAfter();
  }

  @RegisterExtension
  final TestWatcher appendResults =
      new TestWatcher() {
        @Override
        public void testSuccessful(ExtensionContext context) {
          preserveTestResult(context, null);
        }

        @Override
        public void testFailed(ExtensionContext context, Throwable cause) {
          preserveTestResult(context, cause);
        }
      };

  private SparkSession spark;
  private GravitinoDeltaCatalogSpark35 adapter;
  private final Map<String, DeltaRegistrationReceipt> receipts = new HashMap<>();
  private final Map<String, String> locations = new HashMap<>();
  private final AtomicLong ids = new AtomicLong(50);
  private final List<DeltaRegistrationJournal.Entry> entries = new ArrayList<>();
  private String wrongNativeId;
  private String appendTestName;
  private final Map<String, Option<String>> savedAppendConfiguration = new HashMap<>();

  @BeforeEach
  void saveAppendConfiguration(TestInfo info) {
    appendTestName = info.getTestMethod().orElseThrow().getName();
    for (String key :
        Arrays.asList(
            "spark.databricks.delta.write.txnAppId",
            "spark.databricks.delta.write.txnVersion",
            "spark.databricks.delta.write.txnVersion.autoReset.enabled",
            "spark.databricks.delta.schema.autoMerge.enabled",
            SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES().key())) {
      savedAppendConfiguration.put(key, spark.conf().getOption(key));
    }
  }

  @AfterEach
  void restoreAppendConfiguration() {
    savedAppendConfiguration.forEach(
        (key, value) -> {
          if (value.isDefined()) {
            spark.conf().set(key, value.get());
          } else {
            spark.conf().unset(key);
          }
        });
    savedAppendConfiguration.clear();
  }

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
            .config(
                "spark.sql.extensions",
                "org.apache.gravitino.spark.connector.delta.NativeAppendProbeExtension,"
                    + "io.delta.sql.DeltaSparkSessionExtension")
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
    appendProbeFactory = null;
  }

  @Test
  void appendFrontendsUseNativeAnalyzedQueryWithoutCompanion() throws Exception {
    assertTrue(appendProbeFactory != null, "native append extension must install a factory");
    spark
        .sql("CREATE TABLE registry.db.probe_source (value STRING, id STRING) USING delta")
        .collect();
    spark.sql("INSERT INTO registry.db.probe_source VALUES ('a','1'),('b','2')").collect();
    Dataset<Row> source = spark.table("registry.db.probe_source");
    assertNativeFrontend(
        "probe_native_values",
        () ->
            executeEagerInsert(
                "probe_native_values",
                "INSERT INTO registry.db.probe_native_values VALUES ('1','a'),('2','b')"),
        Arrays.asList(RowFactory.create("1", "a"), RowFactory.create("2", "b")));
    assertNativeFrontend(
        "probe_native_select",
        () ->
            executeEagerInsert(
                "probe_native_select",
                "INSERT INTO registry.db.probe_native_select SELECT id,value FROM registry.db.probe_source WHERE id='2'"),
        List.of(RowFactory.create("2", "b")));
    assertNativeFrontend(
        "probe_native_insert_into",
        () -> source.write().mode("append").insertInto("registry.db.probe_native_insert_into"),
        Arrays.asList(RowFactory.create("a", "1"), RowFactory.create("b", "2")));
    assertNativeFrontend(
        "probe_native_save_as",
        () -> source.write().mode("append").saveAsTable("registry.db.probe_native_save_as"),
        Arrays.asList(RowFactory.create("1", "a"), RowFactory.create("2", "b")));
    assertNativeFrontend(
        "probe_native_write_to",
        () -> source.writeTo("registry.db.probe_native_write_to").append(),
        Arrays.asList(RowFactory.create("1", "a"), RowFactory.create("2", "b")));
    long beforeForeign = nativeTable("probe_native_values").update().version();
    try (AppendProbeCapture capture = beginProbe("probe_native_values", "NATIVE")) {
      SparkSession foreign = spark.newSession();
      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  foreign.sql(
                      "INSERT INTO registry.db.probe_native_values VALUES ('foreign','session')"));
      capture.failure(failure);
      assertTrue(rootCause(failure).getMessage().contains("another session"));
      assertEquals(0, capture.report().entryCalls());
    }
    assertEquals(beforeForeign, nativeTable("probe_native_values").update().version());
  }

  private interface AppendAction {
    void run() throws Exception;
  }

  @Test
  void appendSplitMatchesNativeRowsActionsAndMetadata() throws Exception {
    String nativeName = "probe_pair_native_values";
    String splitName = "probe_pair_split_values";
    spark
        .sql("CREATE TABLE registry.db." + nativeName + " (id INT, value STRING) USING delta")
        .collect();
    spark
        .sql("CREATE TABLE registry.db." + splitName + " (id INT, value STRING) USING delta")
        .collect();
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = mode.equals("NATIVE") ? nativeName : splitName;
      long before = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        executeEagerInsert(name, "INSERT INTO registry.db." + name + " VALUES (1,'a'),(2,'b')");
        assertEquals(1, capture.report().entryCalls(), capture.report().plans().toString());
        assertTrue(capture.report().inputBound(), capture.report().plans().toString());
        if (mode.equals("SPLIT")) {
          assertEquals(1, capture.report().helperCalls(), capture.report().events().toString());
          assertEquals(1, capture.report().commitCalls(), capture.report().events().toString());
        }
        assertFalse(capture.report().activeTransactionAfter());
      }
      assertEquals(before + 1, nativeTable(name).update().version(), mode);
      assertEquals(
          Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")),
          spark.sql("SELECT id,value FROM registry.db." + name + " ORDER BY id").collectAsList());
    }
    assertPairedCommittedState(nativeName, splitName);

    spark
        .sql("CREATE TABLE registry.db.probe_pair_source (value STRING, id STRING) USING delta")
        .collect();
    spark.sql("INSERT INTO registry.db.probe_pair_source VALUES ('a','1'),('b','2')").collect();
    Dataset<Row> source = spark.table("registry.db.probe_pair_source");
    assertPairedFrontend(
        "select",
        name ->
            executeEagerInsert(
                name,
                "INSERT INTO registry.db."
                    + name
                    + " SELECT id,value FROM registry.db.probe_pair_source WHERE id='2'"),
        List.of(RowFactory.create("2", "b")));
    assertPairedFrontend(
        "insert_into",
        name -> source.write().mode("append").insertInto("registry.db." + name),
        Arrays.asList(RowFactory.create("a", "1"), RowFactory.create("b", "2")));
    assertPairedFrontend(
        "save_as",
        name -> source.write().mode("append").saveAsTable("registry.db." + name),
        Arrays.asList(RowFactory.create("1", "a"), RowFactory.create("2", "b")));
    assertPairedFrontend(
        "write_to",
        name -> source.writeTo("registry.db." + name).append(),
        Arrays.asList(RowFactory.create("1", "a"), RowFactory.create("2", "b")));

    String selfNative = "probe_self_native";
    String selfSplit = "probe_self_split";
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = mode.equals("NATIVE") ? selfNative : selfSplit;
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      spark.sql("INSERT INTO registry.db." + name + " VALUES (1,'a'),(2,'b')").collect();
      long before = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        executeEagerInsert(
            name, "INSERT INTO registry.db." + name + " SELECT id,value FROM registry.db." + name);
        assertEquals(1, capture.report().entryCalls());
        assertTrue(capture.report().inputBound());
      }
      assertEquals(before + 1, nativeTable(name).update().version());
      assertEquals(
          Arrays.asList(
              RowFactory.create(1, "a"),
              RowFactory.create(1, "a"),
              RowFactory.create(2, "b"),
              RowFactory.create(2, "b")),
          spark
              .sql("SELECT id,value FROM registry.db." + name + " ORDER BY id,value")
              .collectAsList());
    }
    assertPairedCommittedState(selfNative, selfSplit);
  }

  private interface NamedAppendAction {
    void run(String name) throws Exception;
  }

  @Test
  void appendEntryDenialLeavesOwnedStorageUnchanged() throws Exception {
    spark
        .sql("CREATE TABLE registry.db.probe_deny_source (id INT, value STRING) USING delta")
        .collect();
    spark.sql("INSERT INTO registry.db.probe_deny_source VALUES (7,'source')").collect();
    String[][] cases = {
      {"probe_deny_entry_values", "VALUES (1,'literal'),(2,'literal')"},
      {"probe_deny_entry_select", "SELECT id,value FROM registry.db.probe_deny_source"}
    };
    for (String[] testCase : cases) {
      String name = testCase[0];
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      long version = nativeTable(name).update().version();
      Map<String, String> before = ownedManifest(name);
      try (AppendProbeCapture capture = beginProbe(name, "DENY_ENTRY")) {
        Exception failure =
            assertThrows(
                Exception.class,
                () -> spark.sql("INSERT INTO registry.db." + name + " " + testCase[1]));
        capture.failure(failure);
        assertProbeDenial(failure, "NATIVE_APPEND_DENY_ENTRY");
        assertEquals(1, capture.report().entryCalls());
        assertTrue(capture.report().inputBound());
        assertEquals(0, capture.report().helperCalls());
        assertEquals(0, capture.report().commitCalls());
        assertFalse(capture.report().activeTransactionAfter());
      }
      assertEquals(version, nativeTable(name).update().version());
      assertEquals(before, ownedManifest(name), name);
      assertEquals(0, spark.sql("SELECT * FROM registry.db." + name).count());
    }
  }

  @Test
  void appendPrecommitDenialRetainsUncommittedFilesWithoutDeltaCommit() throws Exception {
    String name = "probe_deny_precommit";
    spark.sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta").collect();
    long version = nativeTable(name).update().version();
    Map<String, String> before = ownedManifest(name);
    Map<String, String> after;
    try (AppendProbeCapture capture = beginProbe(name, "DENY_PRECOMMIT")) {
      Exception failure =
          assertThrows(
              Exception.class,
              () -> spark.sql("INSERT INTO registry.db." + name + " VALUES (3,'orphan')"));
      capture.failure(failure);
      assertProbeDenial(failure, "NATIVE_APPEND_DENY_PRECOMMIT");
      assertEquals(1, capture.report().entryCalls());
      assertTrue(capture.report().inputBound());
      assertEquals(1, capture.report().helperCalls());
      assertEquals(0, capture.report().commitCalls());
      assertFalse(capture.report().activeTransactionAfter());
      assertTrue(
          capture.report().events().stream().anyMatch(event -> event.contains("helper actions=")),
          capture.report().events().toString());
      after = ownedManifest(name);
      assertHelperArtifacts(name, capture.report(), false);
      capture.verified(
          "helper AddFiles exist with exact sizes; unchanged persisted metadata/protocol");
    }
    assertEquals(version, nativeTable(name).update().version());
    assertEquals(0, spark.sql("SELECT * FROM registry.db." + name).count());
    before.forEach((key, value) -> assertEquals(value, after.get(key), key));
    assertTrue(
        after.keySet().stream()
            .anyMatch(key -> !before.containsKey(key) && key.endsWith(".parquet")),
        after.toString());
    assertEquals(
        before.keySet().stream().filter(key -> key.endsWith(".json")).count(),
        after.keySet().stream().filter(key -> key.endsWith(".json")).count());
  }

  @Test
  void appendTransactionIdentifiersPreserveNativeRetryAndResetSemantics() throws Exception {
    Dataset<Row> source = spark.sql("SELECT 9 AS id, 'txn' AS value");
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = "probe_txn_" + mode.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      long initial = nativeTable(name).update().version();
      appendWithTxnOptions(source, name, mode, null, null);
      appendWithTxnOptions(source, name, mode, null, null);
      assertEquals(initial + 2, nativeTable(name).update().version(), mode);
      AppendProbeReport first = appendWithTxnOptions(source, name, mode, mode + "-app", "1");
      long once = nativeTable(name).update().version();
      AppendProbeReport duplicate = appendWithTxnOptions(source, name, mode, mode + "-app", "1");
      assertEquals(once, nativeTable(name).update().version(), mode);
      if (mode.equals("SPLIT")) {
        assertEquals(1, first.helperCalls());
        assertEquals(1, duplicate.idempotentSkips());
        assertEquals(0, duplicate.helperCalls());
        assertEquals(0, duplicate.commitCalls());
      }
      appendWithTxnOptions(source, name, mode, mode + "-app", "2");
      assertEquals(once + 1, nativeTable(name).update().version(), mode);
      assertEquals(4, spark.sql("SELECT * FROM registry.db." + name).count(), mode);
      try (AppendProbeCapture capture = beginProbe(name, "DENY_ENTRY")) {
        Exception denied =
            assertThrows(
                Exception.class,
                () ->
                    source
                        .writeTo("registry.db." + name)
                        .option("txnAppId", mode + "-app")
                        .option("txnVersion", "2")
                        .append());
        capture.failure(denied);
        assertProbeDenial(denied, "NATIVE_APPEND_DENY_ENTRY");
        assertEquals(0, capture.report().idempotentSkips());
      }
      assertEquals(once + 1, nativeTable(name).update().version());
    }
    String appKey = "spark.databricks.delta.write.txnAppId";
    String versionKey = "spark.databricks.delta.write.txnVersion";
    String resetKey = "spark.databricks.delta.write.txnVersion.autoReset.enabled";
    try {
      for (boolean autoReset : new boolean[] {false, true}) {
        long[] deltas = new long[2];
        int index = 0;
        for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
          String name = "probe_session_txn_" + mode.toLowerCase() + "_" + autoReset;
          spark
              .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
              .collect();
          long before = nativeTable(name).update().version();
          spark.conf().set(appKey, "session-" + mode + "-" + autoReset);
          spark.conf().set(versionKey, "1");
          spark.conf().set(resetKey, Boolean.toString(autoReset));
          appendWithTxnOptions(source, name, mode, null, null);
          assertEquals(autoReset, spark.conf().getOption(versionKey).isEmpty());
          if (autoReset) {
            spark.conf().set(versionKey, "1");
          }
          AppendProbeReport duplicate = appendWithTxnOptions(source, name, mode, null, null);
          if (mode.equals("SPLIT")) {
            assertEquals(1, duplicate.idempotentSkips());
            assertEquals(0, duplicate.helperCalls());
          }
          assertEquals(
              autoReset, spark.conf().getOption(versionKey).isEmpty(), "duplicate native reset");
          assertEquals("session-" + mode + "-" + autoReset, spark.conf().get(appKey));
          deltas[index++] = nativeTable(name).update().version() - before;
          assertEquals(1, nativeTable(name).update().version() - before);
          assertEquals(
              List.of(RowFactory.create(9, "txn")),
              spark.sql("SELECT id,value FROM registry.db." + name).collectAsList());
          spark.conf().unset(appKey);
          spark.conf().unset(versionKey);
          spark.conf().unset(resetKey);
        }
        assertEquals(deltas[0], deltas[1], "session auto-reset=" + autoReset);
      }
    } finally {
      spark.conf().unset(appKey);
      spark.conf().unset(versionKey);
      spark.conf().unset(resetKey);
    }
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = "probe_txn_override_" + mode.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      spark.conf().set(appKey, "session-app-" + mode);
      spark.conf().set(versionKey, "7");
      try {
        appendWithTxnOptions(source, name, mode, "writer-app-" + mode, "1");
        long first = nativeTable(name).update().version();
        AppendProbeReport duplicate =
            appendWithTxnOptions(source, name, mode, "writer-app-" + mode, "1");
        assertEquals(first, nativeTable(name).update().version());
        if (mode.equals("SPLIT")) {
          assertEquals(1, duplicate.idempotentSkips());
        }
        appendWithTxnOptions(source, name, mode, null, null);
        assertEquals(first + 1, nativeTable(name).update().version());
        assertEquals("7", spark.conf().get(versionKey));
        assertEquals(
            Arrays.asList(RowFactory.create(9, "txn"), RowFactory.create(9, "txn")),
            spark.sql("SELECT id,value FROM registry.db." + name).collectAsList());
      } finally {
        spark.conf().unset(appKey);
        spark.conf().unset(versionKey);
      }
    }
    for (String missing : Arrays.asList("txnAppId", "txnVersion")) {
      List<String> rootClasses = new ArrayList<>();
      for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
        String name = "probe_txn_missing_" + missing.toLowerCase() + "_" + mode.toLowerCase();
        spark
            .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
            .collect();
        long version = nativeTable(name).update().version();
        try (AppendProbeCapture capture = beginProbe(name, mode)) {
          Exception failure =
              assertThrows(
                  Exception.class,
                  () ->
                      source
                          .writeTo("registry.db." + name)
                          .option(missing, missing.equals("txnVersion") ? "1" : "missing-app")
                          .append());
          capture.failure(failure);
          rootClasses.add(failureIdentity(failure));
          assertEquals(0, capture.report().commitCalls());
        }
        assertEquals(version, nativeTable(name).update().version());
      }
      assertEquals(rootClasses.get(0), rootClasses.get(1), missing);
    }
  }

  @Test
  void appendEmptyAndConstraintInputsPreserveNativeOutcomes() throws Exception {
    spark
        .sql("CREATE TABLE registry.db.probe_empty_source (id INT, value STRING) USING delta")
        .collect();
    spark.sql("INSERT INTO registry.db.probe_empty_source VALUES (1,'a')").collect();
    Dataset<Row> emptyFrame = spark.table("registry.db.probe_empty_source").filter("id < 0");
    for (boolean frame : new boolean[] {false, true}) {
      for (boolean transaction : new boolean[] {false, true}) {
        String suffix = (frame ? "frame" : "select") + (transaction ? "_txn" : "_plain");
        List<Map<String, Long>> emptyArtifacts = new ArrayList<>();
        long[] deltas = new long[2];
        int index = 0;
        for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
          String name = "probe_empty_" + suffix + "_" + mode.toLowerCase();
          spark
              .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
              .collect();
          long before = nativeTable(name).update().version();
          Map<String, String> beforeFiles = ownedManifest(name);
          try (AppendProbeCapture capture = beginProbe(name, mode)) {
            if (frame) {
              Dataset<Row> input =
                  frame
                      ? emptyFrame
                      : spark.sql(
                          "SELECT id,value FROM registry.db.probe_empty_source WHERE id < 0");
              DataFrameWriterV2<Row> writer = input.writeTo("registry.db." + name);
              if (transaction) {
                writer =
                    writer
                        .option("txnAppId", "empty-" + suffix + "-" + mode)
                        .option("txnVersion", "1");
              }
              writer.append();
            } else {
              String appKey = "spark.databricks.delta.write.txnAppId";
              String versionKey = "spark.databricks.delta.write.txnVersion";
              Option<String> previousApp = spark.conf().getOption(appKey);
              Option<String> previousVersion = spark.conf().getOption(versionKey);
              try {
                if (transaction) {
                  spark.conf().set(appKey, "empty-" + suffix + "-" + mode);
                  spark.conf().set(versionKey, "1");
                }
                executeEagerInsert(
                    name,
                    "INSERT INTO registry.db."
                        + name
                        + " SELECT id,value FROM registry.db.probe_empty_source WHERE id < 0");
              } finally {
                restoreConfiguration(appKey, previousApp);
                restoreConfiguration(versionKey, previousVersion);
              }
            }
            assertTransactionAction(
                name,
                transaction ? "empty-" + suffix + "-" + mode : null,
                transaction ? "1" : null);
            assertEquals(1, capture.report().entryCalls());
            assertTrue(capture.report().inputBound());
          }
          deltas[index++] = nativeTable(name).update().version() - before;
          Map<String, String> afterFiles = ownedManifest(name);
          beforeFiles.forEach((key, value) -> assertEquals(value, afterFiles.get(key), key));
          emptyArtifacts.add(newArtifactCounts(beforeFiles, afterFiles));
          assertEquals(0, spark.sql("SELECT * FROM registry.db." + name).count());
        }
        assertEquals(deltas[0], deltas[1], suffix);
        assertEquals(emptyArtifacts.get(0), emptyArtifacts.get(1), suffix);
        if (deltas[0] > 0) {
          assertPairedCommittedState(
              "probe_empty_" + suffix + "_native", "probe_empty_" + suffix + "_split");
        } else {
          assertEquals(0L, emptyArtifacts.get(0).getOrDefault("commit", 0L));
        }
      }
    }
    for (String violation : Arrays.asList("check", "not_null")) {
      List<String> rootClasses = new ArrayList<>();
      List<Map<String, Long>> artifacts = new ArrayList<>();
      for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
        String name = "probe_constraint_" + violation + "_" + mode.toLowerCase();
        spark
            .sql(
                "CREATE TABLE registry.db."
                    + name
                    + " (id INT NOT NULL, value STRING) USING delta "
                    + "TBLPROPERTIES ('delta.constraints.positive'='id > 0')")
            .collect();
        long version = nativeTable(name).update().version();
        Map<String, String> beforeFiles = ownedManifest(name);
        String invalid = violation.equals("check") ? "-1" : "NULL";
        try (AppendProbeCapture capture = beginProbe(name, mode)) {
          Exception failure =
              assertThrows(
                  Exception.class,
                  () ->
                      spark.sql(
                          "INSERT INTO registry.db."
                              + name
                              + " VALUES ("
                              + invalid
                              + ",'invalid')"));
          capture.failure(failure);
          Throwable cause = rootCause(failure);
          rootClasses.add(failureIdentity(failure));
          assertTrue(
              cause.getMessage().contains(violation.equals("check") ? "positive" : "id"),
              cause.toString());
          assertFalse(capture.report().activeTransactionAfter());
          Map<String, String> afterFiles = ownedManifest(name);
          beforeFiles.forEach((key, value) -> assertEquals(value, afterFiles.get(key), key));
          artifacts.add(newArtifactCounts(beforeFiles, afterFiles));
          capture.verified(
              "constraint identity and original bytes preserved; new artifacts="
                  + artifacts.get(artifacts.size() - 1));
          assertEquals(1, capture.report().entryCalls());
          assertTrue(capture.report().inputBound());
          assertEquals(0, capture.report().commitCalls());
        }
        assertEquals(version, nativeTable(name).update().version());
        assertEquals(0, spark.sql("SELECT * FROM registry.db." + name).count());
      }
      assertEquals(rootClasses.get(0), rootClasses.get(1), violation);
      assertEquals(artifacts.get(0), artifacts.get(1), violation);
    }
  }

  @Test
  void appendOptionsAndPendingMetadataMatchNative() throws Exception {
    Dataset<Row> compatible = spark.sql("SELECT 1 AS id, 'a' AS value");
    Dataset<Row> extra = spark.sql("SELECT 2 AS id, 'b' AS value, 'new' AS extra");
    assertOptionPair("compatible", compatible, Map.of(), false);
    assertOptionPair("user_metadata", compatible, Map.of("userMetadata", "append-probe"), false);
    assertOptionPair(
        "compression",
        spark
            .sql("SELECT 1 AS id, 'a' AS value UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'")
            .coalesce(1),
        Map.of("compression", "gzip", "maxRecordsPerFile", "1"),
        false);
    assertOptionPair("merge_schema", extra, Map.of("mergeSchema", "true"), true);
    String autoMerge = "spark.databricks.delta.schema.autoMerge.enabled";
    spark.conf().set(autoMerge, "true");
    try {
      assertOptionPair("session_merge", extra, Map.of(), true);
    } finally {
      spark.conf().unset(autoMerge);
    }
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = "probe_option_incompatible_" + mode.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      long before = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        capture.failure(
            assertThrows(Exception.class, () -> extra.writeTo("registry.db." + name).append()));
      }
      assertEquals(before, nativeTable(name).update().version());
      assertFalse(
          Arrays.asList(nativeTable(name).update().schema().fieldNames()).contains("extra"));
    }
    String deniedName = "probe_option_pending_metadata";
    spark
        .sql("CREATE TABLE registry.db." + deniedName + " (id INT, value STRING) USING delta")
        .collect();
    long before = nativeTable(deniedName).update().version();
    try (AppendProbeCapture capture = beginProbe(deniedName, "DENY_PRECOMMIT")) {
      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  extra
                      .writeTo("registry.db." + deniedName)
                      .option("mergeSchema", "true")
                      .append());
      capture.failure(failure);
      assertProbeDenial(failure, "NATIVE_APPEND_DENY_PRECOMMIT");
      assertEquals(1, capture.report().helperCalls());
      assertEquals(0, capture.report().commitCalls());
      JsonNode pending = JSON.readTree(capture.report().pendingMetadata()).get("metaData");
      StructType pendingSchema =
          (StructType) DataType.fromJson(pending.get("schemaString").asText());
      assertTrue(Arrays.asList(pendingSchema.fieldNames()).contains("extra"));
      assertFalse(
          Arrays.asList(nativeTable(deniedName).update().schema().fieldNames()).contains("extra"));
      assertHelperArtifacts(deniedName, capture.report(), true);
      capture.verified(
          "pending metadata alone evolves extra; generated action files remain uncommitted");
    }
    assertEquals(before, nativeTable(deniedName).update().version());
    assertFalse(
        Arrays.asList(nativeTable(deniedName).update().schema().fieldNames()).contains("extra"));
    for (String frontend : Arrays.asList("insert_into", "save_as", "write_to")) {
      String[] metadata = new String[2];
      int index = 0;
      for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
        String name = "probe_route_" + frontend + "_" + mode.toLowerCase();
        spark
            .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
            .collect();
        try (AppendProbeCapture capture = beginProbe(name, mode)) {
          switch (frontend) {
            case "insert_into":
              compatible
                  .write()
                  .option("userMetadata", "routed")
                  .mode("append")
                  .insertInto("registry.db." + name);
              break;
            case "save_as":
              compatible
                  .write()
                  .option("userMetadata", "routed")
                  .mode("append")
                  .saveAsTable("registry.db." + name);
              break;
            default:
              compatible.writeTo("registry.db." + name).option("userMetadata", "routed").append();
          }
          assertEquals(1, capture.report().entryCalls());
          assertTrue(capture.report().inputBound());
          assertEquals(Map.of("userMetadata", "routed"), capture.report().options());
          assertTrue(
              capture.report().plans().stream().anyMatch(plan -> plan.contains("options=")),
              capture.report().plans().toString());
        }
        metadata[index++] =
            spark.sql("DESCRIBE HISTORY registry.db." + name).first().getAs("userMetadata");
      }
      assertEquals(metadata[0], metadata[1], frontend);
      assertEquals("routed", metadata[0], frontend);
    }
    String nullDefaultKey = SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES().key();
    spark.conf().set(nullDefaultKey, "true");
    try {
      Dataset<Row> castInput = spark.sql("SELECT CAST('3' AS INT) AS id, 'cast' AS value");
      for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
        String name = "probe_null_default_" + mode.toLowerCase();
        spark
            .sql(
                "CREATE TABLE registry.db."
                    + name
                    + " (id INT, value STRING, extra STRING) USING delta")
            .collect();
        try (AppendProbeCapture capture = beginProbe(name, mode)) {
          castInput.writeTo("registry.db." + name).append();
          assertEquals(1, capture.report().entryCalls());
          assertTrue(capture.report().inputBound());
          assertTrue(
              capture.report().plans().stream()
                  .anyMatch(plan -> plan.contains("nullAsDefault=Some(true)")),
              capture.report().plans().toString());
        }
        assertEquals(
            List.of(RowFactory.create(3, "cast", null)),
            spark.sql("SELECT id,value,extra FROM registry.db." + name).collectAsList());
      }
      assertPairedCommittedState("probe_null_default_native", "probe_null_default_split");
    } finally {
      spark.conf().unset(nullDefaultKey);
    }
    List<String> castFailures = new ArrayList<>();
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String rejected = "probe_df_int_to_long_" + mode.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + rejected + " (id BIGINT, value STRING) USING delta")
          .collect();
      long version = nativeTable(rejected).update().version();
      Map<String, String> beforeFiles = ownedManifest(rejected);
      try (AppendProbeCapture capture = beginProbe(rejected, mode)) {
        Exception failure =
            assertThrows(
                Exception.class, () -> compatible.writeTo("registry.db." + rejected).append());
        capture.failure(failure);
        Throwable cause = rootCause(failure);
        castFailures.add(cause.getClass().getName() + ":" + cause.getMessage());
        assertTrue(
            cause.getMessage().contains("LongType") && cause.getMessage().contains("IntegerType"),
            cause.toString());
        assertEquals(0, capture.report().commitCalls());
      }
      assertEquals(version, nativeTable(rejected).update().version());
      assertEquals(beforeFiles, ownedManifest(rejected));
      String accepted = "probe_sql_int_to_long_" + mode.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + accepted + " (id BIGINT, value STRING) USING delta")
          .collect();
      try (AppendProbeCapture capture = beginProbe(accepted, mode)) {
        executeEagerInsert(
            accepted, "INSERT INTO registry.db." + accepted + " VALUES (3,'sql-cast')");
        assertTrue(capture.report().inputBound());
        assertEquals(1, capture.report().entryCalls());
      }
      assertEquals(
          List.of(RowFactory.create(3L, "sql-cast")),
          spark.sql("SELECT id,value FROM registry.db." + accepted).collectAsList());
    }
    assertEquals(castFailures.get(0), castFailures.get(1));
    assertPairedCommittedState("probe_sql_int_to_long_native", "probe_sql_int_to_long_split");
    for (String option : Arrays.asList("replaceWhere", "dataChange")) {
      String name = "probe_excluded_" + option.toLowerCase();
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      Map<String, String> beforeFiles = ownedManifest(name);
      long version = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, "SPLIT")) {
        Exception failure =
            assertThrows(
                Exception.class,
                () ->
                    compatible
                        .writeTo("registry.db." + name)
                        .option(option, option.equals("dataChange") ? "false" : "id > 0")
                        .append());
        capture.failure(failure);
        assertTrue(rootCause(failure).getMessage().contains("unqualified append mutation options"));
        assertEquals(0, capture.report().helperCalls());
        assertEquals(0, capture.report().commitCalls());
      }
      assertEquals(version, nativeTable(name).update().version());
      assertEquals(beforeFiles, ownedManifest(name));
    }
    assertAll(
        "native auto-compaction exclusion",
        () ->
            assertAutoCompactBoundary(
                "probe_compact_session_override",
                "SPLIT",
                "'delta.autoOptimize.autoCompact'='false'",
                "true",
                true),
        () ->
            assertAutoCompactBoundary(
                "probe_compact_legacy", "SPLIT", "'delta.autoOptimize'='true'", null, true),
        () -> {
          for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
            assertAutoCompactBoundary(
                "probe_compact_disabled_" + mode.toLowerCase(),
                mode,
                "'delta.autoOptimize'='true','delta.autoOptimize.autoCompact'='true'",
                "false",
                false);
          }
          assertPairedCommittedState(
              "probe_compact_disabled_native", "probe_compact_disabled_split");
        });
  }

  @Test
  void appendMetricsAndPostcommitEffectsRemainNative() throws Exception {
    String nativeName = "probe_post_native";
    String splitName = "probe_post_split";
    List<List<String>> executions = new ArrayList<>();
    List<Boolean> checkpoints = new ArrayList<>();
    List<Set<String>> historyMetricKeys = new ArrayList<>();
    List<List<String>> nestedMetrics = new ArrayList<>();
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = mode.equals("NATIVE") ? nativeName : splitName;
      spark
          .sql(
              "CREATE TABLE registry.db."
                  + name
                  + " (id INT, value STRING) USING delta "
                  + "TBLPROPERTIES ('delta.checkpointInterval'='1')")
          .collect();
      Dataset<Row> cached = spark.sql("SELECT id,value FROM registry.db." + name);
      cached.cache();
      List<String> events = Collections.synchronizedList(new ArrayList<>());
      Map<Long, SparkListenerSQLExecutionStart> starts =
          Collections.synchronizedMap(new HashMap<>());
      Map<Long, SparkListenerSQLExecutionEnd> ends = Collections.synchronizedMap(new HashMap<>());
      SparkListener listener =
          new SparkListener() {
            @Override
            public void onOtherEvent(SparkListenerEvent event) {
              if (event instanceof SparkListenerSQLExecutionStart) {
                SparkListenerSQLExecutionStart start = (SparkListenerSQLExecutionStart) event;
                starts.put(start.executionId(), start);
                events.add("start:" + start.executionId() + ":" + start.toString());
              } else if (event instanceof SparkListenerSQLExecutionEnd) {
                SparkListenerSQLExecutionEnd end = (SparkListenerSQLExecutionEnd) event;
                ends.put(end.executionId(), end);
                events.add("end:" + end.executionId() + ":" + end.toString());
              } else if (event instanceof SparkListenerDriverAccumUpdates) {
                events.add("accumulators:" + event.toString());
              }
            }
          };
      try {
        assertEquals(0, cached.count());
        spark.sparkContext().addSparkListener(listener);
        try (AppendProbeCapture capture = beginProbe(name, mode)) {
          executeEagerInsert(name, "INSERT INTO registry.db." + name + " VALUES (1,'a'),(2,'b')");
          assertEquals(1, capture.report().entryCalls());
          assertTrue(capture.report().inputBound());
          assertEquals(2, cached.count(), "cached read was not refreshed for " + mode);
          assertTrue(UpdateCatalog.awaitCompletion(10000));
          spark.sparkContext().listenerBus().waitUntilEmpty(10000);
          spark.sparkContext().removeSparkListener(listener);
          assertFalse(starts.isEmpty());
          assertEquals(starts.keySet(), ends.keySet(), "SQL executions must each finish");
          capture.report().events().addAll(events);
          List<String> metrics = new ArrayList<>();
          for (Map.Entry<Long, SparkListenerSQLExecutionEnd> entry : ends.entrySet()) {
            if (entry.getValue().executionName().isDefined()
                && entry.getValue().executionName().get().equals("deltaTransactionalWrite")) {
              assertTrue(starts.containsKey(entry.getKey()));
              assertTrue(entry.getValue().time() >= starts.get(entry.getKey()).time());
              List<String> observed = executionMetrics(entry.getKey());
              capture.verified(
                  "nested execution="
                      + entry.getKey()
                      + " metrics="
                      + observed
                      + " elapsedMs="
                      + (entry.getValue().time() - starts.get(entry.getKey()).time()));
              metrics.addAll(observed);
            }
          }
          assertFalse(
              metrics.isEmpty(),
              "native deltaTransactionalWrite execution and metrics required: " + events);
          Collections.sort(metrics);
          nestedMetrics.add(metrics);
          capture.verified("paired completed SQL execution IDs; nested metrics=" + metrics);
        }
      } finally {
        spark.sparkContext().removeSparkListener(listener);
        cached.unpersist();
      }
      assertTrue(events.stream().anyMatch(event -> event.startsWith("start:")), events.toString());
      assertTrue(events.stream().anyMatch(event -> event.startsWith("end:")), events.toString());
      executions.add(new ArrayList<>(events));
      Row history = spark.sql("DESCRIBE HISTORY registry.db." + name).first();
      Map<String, String> metrics = history.getJavaMap(history.fieldIndex("operationMetrics"));
      historyMetricKeys.add(new HashSet<>(metrics.keySet()));
      assertTrue(metrics.containsKey("numOutputRows"), metrics.toString());
      assertTrue(metrics.containsKey("numFiles"), metrics.toString());
      assertEquals("2", metrics.get("numOutputRows"));
      long outputBytes =
          nativeTable(name).update().allFiles().collectAsList().stream()
              .mapToLong(AddFile::size)
              .sum();
      assertEquals(outputBytes, Long.parseLong(metrics.get("numOutputBytes")));
      assertEquals(
          Long.parseLong(metrics.get("numFiles")), nativeTable(name).update().allFiles().count());
      checkpoints.add(
          ownedManifest(name).keySet().stream().anyMatch(key -> key.contains(".checkpoint.")));
      assertTrue(
          checkpoints.get(checkpoints.size() - 1), "checkpointInterval=1 must produce checkpoint");
      long committedVersion = nativeTable(name).update().version();
      DeltaLog.clearCache();
      SparkSession fresh = spark.newSession();
      assertEquals(
          committedVersion,
          DeltaLog.forTable(fresh, locations.get(name))
              .update(false, Option.empty(), Option.empty())
              .version());
      assertEquals(
          Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")),
          fresh.read().format("delta").load(locations.get(name)).orderBy("id").collectAsList());
    }
    assertPairedCommittedState(nativeName, splitName);
    assertEquals(
        executions.get(0).stream().anyMatch(event -> event.contains("deltaTransactionalWrite")),
        executions.get(1).stream().anyMatch(event -> event.contains("deltaTransactionalWrite")));
    assertEquals(checkpoints.get(0), checkpoints.get(1));
    assertEquals(historyMetricKeys.get(0), historyMetricKeys.get(1));
    assertEquals(nestedMetrics.get(0), nestedMetrics.get(1));
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

  private AppendProbeCapture beginProbe(String name, String mode) {
    AppendProbeCapture capture =
        appendProbeFactory.begin(spark, name, mode, Paths.get(URI.create(locations.get(name))));
    capture.verified("JUnit method=" + appendTestName);
    return capture;
  }

  private void assertNativeFrontend(String name, AppendAction action, List<Row> expected)
      throws Exception {
    spark
        .sql("CREATE TABLE registry.db." + name + " (id STRING, value STRING) USING delta")
        .collect();
    long before = nativeTable(name).update().version();
    try (AppendProbeCapture capture = beginProbe(name, "NATIVE")) {
      action.run();
      assertEquals(Map.of(), capture.report().options());
      assertEquals(before + 1, nativeTable(name).update().version(), name);
      assertEquals(1, capture.report().entryCalls(), capture.report().plans().toString());
      assertTrue(capture.report().inputBound(), capture.report().plans().toString());
      assertTrue(
          capture.report().plans().stream().anyMatch(plan -> plan.contains("AppendDataExecV1")),
          capture.report().plans().toString());
      assertTrue(
          capture.report().plans().stream().anyMatch(plan -> plan.contains("analyzed=Some")),
          capture.report().plans().toString());
    }
    assertEquals(
        expected,
        spark
            .sql("SELECT id,value FROM registry.db." + name + " ORDER BY id,value")
            .collectAsList());
  }

  private void executeEagerInsert(String name, String sql) {
    Dataset<Row> command = spark.sql(sql);
    long afterSql = nativeTable(name).update().version();
    command.collect();
    command.collect();
    assertEquals(afterSql, nativeTable(name).update().version(), "command result replayed");
  }

  private void assertPairedFrontend(String suffix, NamedAppendAction action, List<Row> expected)
      throws Exception {
    String nativeName = "probe_pair_native_" + suffix;
    String splitName = "probe_pair_split_" + suffix;
    for (String name : Arrays.asList(nativeName, splitName)) {
      spark
          .sql("CREATE TABLE registry.db." + name + " (id STRING, value STRING) USING delta")
          .collect();
    }
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = mode.equals("NATIVE") ? nativeName : splitName;
      long before = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        action.run(name);
        assertEquals(1, capture.report().entryCalls(), capture.report().plans().toString());
        assertTrue(capture.report().inputBound(), capture.report().plans().toString());
        if (mode.equals("SPLIT")) {
          assertEquals(1, capture.report().helperCalls(), capture.report().events().toString());
          assertEquals(1, capture.report().commitCalls(), capture.report().events().toString());
        }
      }
      assertEquals(before + 1, nativeTable(name).update().version(), name);
      assertEquals(
          expected,
          spark
              .sql("SELECT id,value FROM registry.db." + name + " ORDER BY id,value")
              .collectAsList(),
          name);
    }
    assertPairedCommittedState(nativeName, splitName);
  }

  private void assertPairedCommittedState(String nativeName, String splitName) throws Exception {
    Snapshot nativeSnapshot = nativeTable(nativeName).update();
    Snapshot splitSnapshot = nativeTable(splitName).update();
    assertEquals(nativeSnapshot.version(), splitSnapshot.version());
    assertEquals(nativeSnapshot.schema(), splitSnapshot.schema());
    assertEquals(nativeSnapshot.protocol(), splitSnapshot.protocol());
    assertNotEquals(nativeSnapshot.metadata().id(), splitSnapshot.metadata().id());
    for (String name : Arrays.asList(nativeName, splitName)) {
      DeltaOperationMarker marker =
          DeltaOperationMarker.decode(
              nativeTable(name)
                  .update()
                  .metadata()
                  .configuration()
                  .apply(DeltaOperationMarker.PROPERTY_KEY));
      DeltaRegistrationIdentity identity = receipts.get(name).identity();
      assertEquals(identity.id(), marker.id());
      assertEquals(identity.generation(), marker.generation());
      assertEquals(identity.token(), marker.token());
      assertEquals(locations.get(name), marker.normalizedLocation());
    }
    assertEquals(committedActionTypes(nativeName), committedActionTypes(splitName));
    assertEquals(materialActions(nativeName), materialActions(splitName));
    ObjectNode nativeConfiguration =
        (ObjectNode)
            JSON.readTree(nativeSnapshot.metadata().json()).get("metaData").get("configuration");
    ObjectNode splitConfiguration =
        (ObjectNode)
            JSON.readTree(splitSnapshot.metadata().json()).get("metaData").get("configuration");
    nativeConfiguration.remove(DeltaOperationMarker.PROPERTY_KEY);
    splitConfiguration.remove(DeltaOperationMarker.PROPERTY_KEY);
    assertEquals(nativeConfiguration, splitConfiguration);
    verifyFiles(nativeName);
    verifyFiles(splitName);
    List<String> nativeFiles = new ArrayList<>();
    List<String> splitFiles = new ArrayList<>();
    for (AddFile file : nativeSnapshot.allFiles().collectAsList()) {
      nativeFiles.add(
          file.partitionValues()
              + "|"
              + file.size()
              + "|"
              + file.stats()
              + "|"
              + file.dataChange());
    }
    for (AddFile file : splitSnapshot.allFiles().collectAsList()) {
      splitFiles.add(
          file.partitionValues()
              + "|"
              + file.size()
              + "|"
              + file.stats()
              + "|"
              + file.dataChange());
    }
    Collections.sort(nativeFiles);
    Collections.sort(splitFiles);
    assertEquals(nativeFiles, splitFiles, nativeName + " vs " + splitName);
    Row nativeHistory = spark.sql("DESCRIBE HISTORY registry.db." + nativeName).first();
    Row splitHistory = spark.sql("DESCRIBE HISTORY registry.db." + splitName).first();
    for (String field :
        Arrays.asList(
            "operation", "operationParameters", "readVersion", "isBlindAppend", "userMetadata")) {
      Object expectedValue = nativeHistory.getAs(field);
      assertEquals(expectedValue, splitHistory.getAs(field), field);
    }
  }

  private List<String> committedActionTypes(String name) throws Exception {
    long version = nativeTable(name).update().version();
    Path log =
        Paths.get(URI.create(locations.get(name)))
            .resolve("_delta_log")
            .resolve(String.format("%020d.json", version));
    assertTrue(log.toRealPath().startsWith(directory.toRealPath()));
    List<String> types = new ArrayList<>();
    for (String line : Files.readAllLines(log)) {
      JsonNode action = JSON.readTree(line);
      action.fieldNames().forEachRemaining(types::add);
    }
    Collections.sort(types);
    return types;
  }

  private static String failureIdentity(Throwable thrown) {
    List<String> chain = new ArrayList<>();
    Throwable current = thrown;
    while (current != null) {
      chain.add(current.getClass().getName());
      current = current.getCause();
    }
    return chain + ":" + rootCause(thrown).getMessage();
  }

  private static Throwable rootCause(Throwable thrown) {
    Throwable current = thrown;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private AppendProbeReport appendWithTxnOptions(
      Dataset<Row> source, String name, String mode, String appId, String version)
      throws Exception {
    String expectedApp =
        appId != null
            ? appId
            : spark.conf().getOption("spark.databricks.delta.write.txnAppId").getOrElse(() -> null);
    String expectedVersion =
        version != null
            ? version
            : spark
                .conf()
                .getOption("spark.databricks.delta.write.txnVersion")
                .getOrElse(() -> null);
    try (AppendProbeCapture capture = beginProbe(name, mode)) {
      DataFrameWriterV2<Row> writer = source.writeTo("registry.db." + name);
      if (appId != null) {
        writer = writer.option("txnAppId", appId).option("txnVersion", version);
      }
      writer.append();
      assertEquals(1, capture.report().entryCalls(), capture.report().plans().toString());
      assertTrue(capture.report().inputBound(), capture.report().plans().toString());
      assertFalse(capture.report().activeTransactionAfter());
      assertTransactionAction(name, expectedApp, expectedVersion);
      capture.verified(
          "committed SetTransaction appId/version equals selected writer/session pair");
      return capture.report();
    }
  }

  private void assertOptionPair(
      String suffix, Dataset<Row> source, Map<String, String> options, boolean hasExtra)
      throws Exception {
    String nativeName = "probe_option_native_" + suffix;
    String splitName = "probe_option_split_" + suffix;
    for (String mode : Arrays.asList("NATIVE", "SPLIT")) {
      String name = mode.equals("NATIVE") ? nativeName : splitName;
      spark
          .sql("CREATE TABLE registry.db." + name + " (id INT, value STRING) USING delta")
          .collect();
      long before = nativeTable(name).update().version();
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        DataFrameWriterV2<Row> writer = source.writeTo("registry.db." + name);
        for (Map.Entry<String, String> option : options.entrySet()) {
          writer = writer.option(option.getKey(), option.getValue());
        }
        writer.append();
        assertEquals(options, capture.report().options());
        assertEquals(1, capture.report().entryCalls());
        assertTrue(capture.report().inputBound());
        if (mode.equals("SPLIT")) {
          JsonNode pending = JSON.readTree(capture.report().pendingMetadata()).get("metaData");
          JsonNode persisted =
              JSON.readTree(nativeTable(name).update().metadata().json()).get("metaData");
          assertEquals(pending, persisted);
          assertEquals(
              JSON.readTree(capture.report().pendingProtocol()),
              JSON.readTree(nativeTable(name).update().protocol().json()));
          DeltaTableV2 refreshed = (DeltaTableV2) adapter.loadTable(id(name));
          assertEquals(nativeTable(name).update().schema(), refreshed.schema());
          capture.verified(
              "pending and persisted metadata/protocol plus adapter-refreshed schema match");
        }
      }
      assertEquals(before + 1, nativeTable(name).update().version(), name);
      assertEquals(
          hasExtra,
          Arrays.asList(nativeTable(name).update().schema().fieldNames()).contains("extra"),
          name);
      List<Row> expected =
          suffix.equals("compression")
              ? Arrays.asList(
                  RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"))
              : hasExtra
                  ? List.of(RowFactory.create(2, "b", "new"))
                  : List.of(RowFactory.create(1, "a"));
      assertEquals(
          expected,
          spark.sql("SELECT * FROM registry.db." + name + " ORDER BY id").collectAsList());
      if (suffix.equals("compression")) {
        assertEquals(3, nativeTable(name).update().allFiles().count());
        for (AddFile file : nativeTable(name).update().allFiles().collectAsList()) {
          org.apache.hadoop.fs.Path path =
              new org.apache.hadoop.fs.Path(
                  Paths.get(URI.create(locations.get(name))).resolve(file.path()).toUri());
          try (ParquetFileReader reader =
              ParquetFileReader.open(
                  HadoopInputFile.fromPath(path, spark.sparkContext().hadoopConfiguration()))) {
            reader
                .getFooter()
                .getBlocks()
                .forEach(
                    block ->
                        block
                            .getColumns()
                            .forEach(column -> assertEquals("GZIP", column.getCodec().name())));
          }
          assertEquals(1, JSON.readTree(file.stats()).get("numRecords").asInt());
        }
      }
      if (options.containsKey("userMetadata")) {
        assertEquals(
            "append-probe",
            spark.sql("DESCRIBE HISTORY registry.db." + name).first().getAs("userMetadata"));
      }
    }
    assertPairedCommittedState(nativeName, splitName);
  }

  private List<String> executionMetrics(long executionId) {
    SQLExecutionUIData execution = spark.sharedState().statusStore().execution(executionId).get();
    assertTrue(execution.completionTime().isDefined());
    List<String> metrics = new ArrayList<>();
    Iterator<SQLPlanMetric> iterator = execution.metrics().iterator();
    boolean outputRows = false;
    boolean timing = false;
    while (iterator.hasNext()) {
      SQLPlanMetric metric = iterator.next();
      Option<String> value =
          spark
              .sharedState()
              .statusStore()
              .executionMetrics(executionId)
              .get(metric.accumulatorId());
      assertTrue(value.isDefined(), metric.toString());
      String metricValue = value.get();
      if (metric.metricType().equals("timing") || metric.metricType().equals("nsTiming")) {
        timing = true;
        assertFalse(metricValue.isEmpty());
        metricValue = "measured-duration";
      }
      if (metric.name().equals("number of output rows")) {
        outputRows = true;
        assertEquals("2", metricValue);
      }
      metrics.add(metric.metricType() + "|" + metric.name() + "|" + metricValue);
    }
    assertTrue(outputRows, metrics.toString());
    if (!timing) {
      metrics.add("availability|timing|absent from literal-input deltaTransactionalWrite plan");
    }
    return metrics;
  }

  private void restoreConfiguration(String key, Option<String> value) {
    if (value.isDefined()) {
      spark.conf().set(key, value.get());
    } else {
      spark.conf().unset(key);
    }
  }

  private void assertTransactionAction(String name, String app, String version) throws Exception {
    List<JsonNode> transactions = new ArrayList<>();
    for (JsonNode action : committedActions(name)) {
      if (action.has("txn")) {
        transactions.add(action.get("txn"));
      }
    }
    if (app == null || version == null) {
      assertTrue(transactions.isEmpty(), transactions.toString());
    } else {
      assertEquals(1, transactions.size(), transactions.toString());
      assertEquals(app, transactions.get(0).get("appId").asText());
      assertEquals(Long.parseLong(version), transactions.get(0).get("version").asLong());
    }
  }

  private Map<String, Long> newArtifactCounts(
      Map<String, String> before, Map<String, String> after) {
    Map<String, Long> counts = new TreeMap<>();
    after.forEach(
        (key, value) -> {
          if (!before.containsKey(key)) {
            String kind =
                key.endsWith(".parquet")
                    ? "parquet"
                    : key.endsWith(".crc")
                        ? "checksum"
                        : key.endsWith(".json") ? "commit" : "other";
            counts.merge(kind, 1L, Long::sum);
          }
        });
    return counts;
  }

  private List<JsonNode> committedActions(String name) throws Exception {
    long version = nativeTable(name).update().version();
    Path log =
        Paths.get(URI.create(locations.get(name)))
            .resolve("_delta_log")
            .resolve(String.format("%020d.json", version));
    List<JsonNode> actions = new ArrayList<>();
    for (String line : Files.readAllLines(log)) {
      actions.add(JSON.readTree(line));
    }
    return actions;
  }

  private List<String> materialActions(String name) throws Exception {
    List<String> result = new ArrayList<>();
    for (JsonNode action : committedActions(name)) {
      ObjectNode value = (ObjectNode) action.deepCopy();
      if (value.has("add")) {
        ((ObjectNode) value.get("add")).remove(Arrays.asList("path", "modificationTime"));
      }
      if (value.has("metaData")) {
        ObjectNode metadata = (ObjectNode) value.get("metaData");
        metadata.remove(Arrays.asList("id", "createdTime"));
        ((ObjectNode) metadata.get("configuration")).remove(DeltaOperationMarker.PROPERTY_KEY);
      }
      if (value.has("commitInfo")) {
        ObjectNode commit = (ObjectNode) value.get("commitInfo");
        commit.remove(Arrays.asList("timestamp", "txnId"));
      }
      if (value.has("txn")) {
        ObjectNode txn = (ObjectNode) value.get("txn");
        txn.remove("lastUpdated");
        // Each appId/version is independently checked against the selected writer/session input.
        txn.put("appId", "paired-app");
      }
      result.add(value.toString());
    }
    Collections.sort(result);
    return result;
  }

  private void verifyFiles(String name) throws Exception {
    Path root = Paths.get(URI.create(locations.get(name))).toRealPath();
    for (AddFile file : nativeTable(name).update().allFiles().collectAsList()) {
      Path path = root.resolve(URI.create(file.path()).getPath()).toRealPath();
      assertTrue(path.startsWith(root));
      assertEquals(file.size(), Files.size(path), file.path());
      long records = spark.read().parquet(path.toString()).count();
      assertEquals(records, JSON.readTree(file.stats()).get("numRecords").asLong(), file.path());
    }
  }

  private void assertHelperArtifacts(String name, AppendProbeReport report, boolean evolved)
      throws Exception {
    assertFalse(report.actions().isEmpty());
    int files = 0;
    Path root = Paths.get(URI.create(locations.get(name))).toRealPath();
    for (String json : report.actions()) {
      JsonNode action = JSON.readTree(json);
      if (action.has("add")) {
        JsonNode add = action.get("add");
        Path path = root.resolve(URI.create(add.get("path").asText()).getPath()).toRealPath();
        assertTrue(path.startsWith(root));
        assertEquals(add.get("size").asLong(), Files.size(path));
        files++;
      }
    }
    assertTrue(files > 0);
    assertEquals(
        JSON.readTree(nativeTable(name).update().protocol().json()),
        JSON.readTree(report.pendingProtocol()));
    JsonNode persisted =
        JSON.readTree(nativeTable(name).update().metadata().json()).get("metaData");
    JsonNode pending = JSON.readTree(report.pendingMetadata()).get("metaData");
    if (evolved) {
      assertNotEquals(persisted.get("schemaString"), pending.get("schemaString"));
      assertEquals(persisted.get("configuration"), pending.get("configuration"));
      assertEquals(persisted.get("id"), pending.get("id"));
    } else {
      assertEquals(persisted, pending);
    }
  }

  private static void preserveTestResult(ExtensionContext context, Throwable failure) {
    String setting = System.getenv("GRAVITINO_DELTA_APPEND_EVIDENCE_DIR");
    if (setting == null || !context.getRequiredTestMethod().getName().startsWith("append")) {
      return;
    }
    try {
      Path root = Paths.get(setting);
      assertTrue(root.isAbsolute() && Files.isDirectory(root) && !Files.isSymbolicLink(root));
      StringWriter details = new StringWriter();
      if (failure != null) {
        failure.printStackTrace(new PrintWriter(details));
      }
      String result =
          "test="
              + context.getRequiredTestMethod().getName()
              + "\nresult="
              + (failure == null ? "PASS: all method assertions completed" : "FAIL")
              + "\n"
              + details;
      Path output =
          root.resolve(
              "test-result-"
                  + context.getRequiredTestMethod().getName()
                  + "-"
                  + UUID.randomUUID()
                  + ".txt");
      Files.createFile(
          output,
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
      Files.writeString(output, result, StandardOpenOption.WRITE);
    } catch (Exception error) {
      throw new AssertionError("Cannot retain append test outcome", error);
    }
  }

  private static void assertProbeDenial(Throwable thrown, String marker) {
    Throwable current = thrown;
    while (current != null) {
      if (current instanceof SecurityException && marker.equals(current.getMessage())) {
        return;
      }
      current = current.getCause();
    }
    throw new AssertionError("Expected probe SecurityException " + marker, thrown);
  }

  private Map<String, String> ownedManifest(String name) throws Exception {
    Path root = Paths.get(URI.create(locations.get(name))).toRealPath();
    assertTrue(root.startsWith(directory.toRealPath()), root.toString());
    Map<String, String> manifest = new TreeMap<>();
    try (Stream<Path> paths = Files.walk(root)) {
      for (Path path : (Iterable<Path>) paths::iterator) {
        assertFalse(Files.isSymbolicLink(path), path.toString());
        if (Files.isRegularFile(path)) {
          byte[] bytes = Files.readAllBytes(path);
          manifest.put(
              root.relativize(path).toString(),
              bytes.length
                  + ":"
                  + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        }
      }
    }
    return manifest;
  }

  private void assertAutoCompactBoundary(
      String name, String mode, String properties, String sessionSetting, boolean denied)
      throws Exception {
    spark
        .sql(
            "CREATE TABLE registry.db."
                + name
                + " (id INT, value STRING) USING delta TBLPROPERTIES ("
                + properties
                + ")")
        .collect();
    String key = "spark.databricks.delta.autoCompact.enabled";
    Option<String> previous = spark.conf().getOption(key);
    Map<String, String> before = ownedManifest(name);
    long version = nativeTable(name).update().version();
    try {
      if (sessionSetting == null) {
        spark.conf().unset(key);
      } else {
        spark.conf().set(key, sessionSetting);
      }
      try (AppendProbeCapture capture = beginProbe(name, mode)) {
        if (denied) {
          Exception failure =
              assertThrows(
                  Exception.class,
                  () ->
                      spark.sql("INSERT INTO registry.db." + name + " VALUES (8,'compact-guard')"));
          capture.failure(failure);
          assertTrue(rootCause(failure).getMessage().contains("unqualified auto-compaction"));
          assertEquals(0, capture.report().helperCalls());
          assertEquals(0, capture.report().commitCalls());
          assertEquals(version, nativeTable(name).update().version());
          assertEquals(before, ownedManifest(name));
          capture.verified(
              "effective native auto-compaction rejected before helper; storage unchanged");
        } else {
          executeEagerInsert(
              name, "INSERT INTO registry.db." + name + " VALUES (8,'compact-guard')");
          assertEquals(1, capture.report().entryCalls());
          assertEquals(version + 1, nativeTable(name).update().version());
          assertEquals(
              List.of(RowFactory.create(8, "compact-guard")),
              spark.sql("SELECT id,value FROM registry.db." + name).collectAsList());
          capture.verified(
              "session=false disables table/legacy auto-compaction per native precedence");
        }
      }
    } finally {
      restoreConfiguration(key, previous);
    }
  }
}
