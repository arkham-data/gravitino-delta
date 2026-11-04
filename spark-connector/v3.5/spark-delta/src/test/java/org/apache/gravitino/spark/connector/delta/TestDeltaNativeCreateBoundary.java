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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.storage.LogStore;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitActionReader;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.OptimisticTransaction;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;
import scala.util.Properties;

/**
 * Local native creation mechanisms; publication callbacks qualify evidence, not server authority.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("deprecation")
class TestDeltaNativeCreateBoundary {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Transform[] PARTITIONS = new Transform[0];
  private static final String SESSION_METADATA = "spark.databricks.delta.commitInfo.userMetadata";

  @TempDir static Path temporary;
  private final Map<String, Option<String>> settingsBefore = new TreeMap<>();
  private SparkSession spark;
  private DeltaCatalog catalog;
  private Path root;
  private Path target;
  private JsonNode fixture;
  private StructType schema;
  private String testName;

  @BeforeAll
  void start() throws Exception {
    try (InputStream input =
        getClass().getResourceAsStream("/native-create/spark-hub-options.json")) {
      fixture = JSON.readTree(input);
    }
    String retained = System.getenv("DELTA_CREATE_EVIDENCE_DIRECTORY");
    root = retained == null ? temporary : Paths.get(retained);
    assertTrue(root.isAbsolute());
    Files.createDirectories(root);
    assertFalse(Files.isSymbolicLink(root));
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-native-create-boundary")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", DeltaCatalog.class.getName());
    stringMap(fixture.get("settings")).forEach(builder::config);
    spark = builder.getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    catalog = (DeltaCatalog) spark.sessionState().catalogManager().catalog("spark_catalog");
    schema = data().schema().asNullable();
    assertTrue(Set.of("3.5.4", "3.5.6").contains(spark.version()), spark.version());
    assertEquals("17", System.getProperty("java.specification.version"));
    assertTrue(spark.conf().getOption(SESSION_METADATA).isEmpty());
    Map<String, Object> identity = new TreeMap<>();
    identity.put("sparkVersion", spark.version());
    identity.put("javaVersion", System.getProperty("java.version"));
    identity.put("scalaVersion", Properties.versionNumberString());
    assertEquals("2.12.21", identity.get("scalaVersion"));
    List<Map<String, String>> classes = new ArrayList<>();
    for (Class<?> type :
        List.of(
            SparkSession.class,
            DeltaCatalog.class,
            OptimisticTransaction.class,
            LogStore.class,
            Option.class,
            DeltaReplacementStage.class,
            DeltaPublicationStage.class,
            DeltaCommitActionReader.class)) {
      Path origin = Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI());
      Map<String, String> entry = new TreeMap<>();
      entry.put("class", type.getName());
      entry.put("origin", origin.toString());
      if (Files.isRegularFile(origin)) {
        entry.put("sha256", hash(origin));
      }
      String resource = "/" + type.getName().replace('.', '/') + ".class";
      try (InputStream input = type.getResourceAsStream(resource)) {
        entry.put("classSha256", digest(input.readAllBytes()));
      }
      classes.add(entry);
    }
    identity.put("classes", classes);
    write(root.resolve("runtime.json"), identity);
    write(root.resolve("fixture.json"), fixture);
  }

  @BeforeEach
  void before(TestInfo info) throws Exception {
    testName = info.getTestMethod().orElseThrow().getName();
    Path caseRoot = Files.createDirectory(root.resolve(testName + "-" + UUID.randomUUID()));
    target = caseRoot.resolve("table");
    settingsBefore.clear();
    for (String key : stringMap(fixture.get("settings")).keySet()) {
      settingsBefore.put(key, spark.conf().getOption(key));
    }
    settingsBefore.put(SESSION_METADATA, spark.conf().getOption(SESSION_METADATA));
    assertTrue(OptimisticTransaction.getActive().isEmpty());
    observe("before");
  }

  @AfterEach
  void after() throws Exception {
    assertTrue(OptimisticTransaction.getActive().isEmpty(), "native transaction must be cleared");
    for (Map.Entry<String, Option<String>> entry : settingsBefore.entrySet()) {
      assertEquals(entry.getValue(), spark.conf().getOption(entry.getKey()), entry.getKey());
    }
    observe("after");
  }

  @AfterAll
  void stop() {
    if (spark != null) {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  @Test
  void replacementWrapperAcknowledgesTruncateButRejectsOriginalFalseOption() throws Exception {
    StagedTable nativeStage = nativeStage(true, properties());
    assertTrue(nativeStage.getClass().getName().contains("StagedDeltaTableV2"));
    DeltaReplacementStage stage = new DeltaReplacementStage(nativeStage);
    LogicalWriteInfo info = info(data(), options());
    Map<String, String> unchanged = new HashMap<>(info.options().asCaseSensitiveMap());
    WriteBuilder builder = stage.newWriteBuilder(info);
    assertSame(builder, assertInstanceOf(SupportsTruncate.class, builder).truncate());
    insert(builder, failingData());
    observe("input-retained-no-execution");
    assertEquals(-1, snapshot().version());
    Exception failure = assertThrows(Exception.class, stage::commitStagedChanges);
    recordFailure("native-rejection", failure);
    assertTrue(failure.getMessage().contains("DELTA_ILLEGAL_USAGE"), failure.toString());
    assertTrue(failure.getMessage().contains("overwriteSchema"));
    assertTrue(failure.getMessage().contains("replacing"));
    assertFalse(stack(failure).contains("native-create-source-failure"));
    assertEquals(unchanged, info.options().asCaseSensitiveMap());
    assertEquals(options(), unchanged);
    assertEquals(-1, snapshot().version());
    assertFalse(hasDataFiles());
    stage.abortStagedChanges();
    observe("rejected-not-committed");
  }

  @Test
  void nativeCreateWithOriginalOptionsCommitsRowsButOmitsWriterMetadata() throws Exception {
    StagedTable stage = nativeStage(false, properties());
    insert(((SupportsWrite) stage).newWriteBuilder(info(data(), options())), data());
    assertEquals(-1, snapshot().version());
    observe("input-retained");
    stage.commitStagedChanges();
    assertEquals(0, snapshot().version());
    assertEquals(1, rows().size());
    assertEquals("CREATE TABLE AS SELECT", commitInfo(0).get("operation").asText());
    assertFalse(commitInfo(0).hasNonNull("userMetadata"));
    assertProperties();
    observe("ctas-committed-metadata-absent");
  }

  @Test
  void schemaCreatePublishesEvidenceBeforeOriginalOptionWrite() throws Exception {
    DeltaOperationMarker marker = marker();
    CountingStage delegate = new CountingStage(nativeStage(false, markedProperties(marker)));
    AtomicInteger publications = new AtomicInteger();
    DeltaPublicationStage owner =
        owner(
            delegate,
            () -> {
              assertMatched(marker);
              publications.incrementAndGet();
            },
            new AtomicInteger());
    owner.commitStagedChanges();
    assertEquals(1, delegate.commits);
    assertEquals(1, publications.get());
    assertEquals(0, snapshot().version());
    assertTrue(rows().isEmpty());
    assertEquals("CREATE TABLE", commitInfo(0).get("operation").asText());
    assertFalse(commitInfo(0).hasNonNull("userMetadata"));
    String uuid = snapshot().metadata().id();
    observe("create0-evidence-matched-empty");
    writeData(data());
    assertWritten(uuid);
    observe("write1-original-metadata-committed");
    owner.commitStagedChanges();
    owner.abortStagedChanges();
    assertEquals(1, delegate.commits);
    assertEquals(0, delegate.aborts);
    assertEquals(1, publications.get());
    assertThrows(IllegalStateException.class, () -> owner.newWriteBuilder(info(data(), options())));
  }

  @Test
  void delayedCreatePublicationRejectsActualAdvancedHead() throws Exception {
    DeltaOperationMarker marker = marker();
    nativeStage(false, markedProperties(marker)).commitStagedChanges();
    observe("create0-unpublished");
    String uuid = snapshot().metadata().id();
    writeData(data());
    assertWritten(uuid);
    DeltaCommitEvidence evidence = inspect(marker);
    assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, evidence.kind());
    assertEquals("VERSION_MISMATCH", evidence.reason());
    assertTrue(evidence.uuid().isEmpty());
    write(
        target.getParent().resolve("rejected-evidence.json"),
        Map.of("kind", evidence.kind(), "reason", evidence.reason()));
    observe("write1-delayed-create-evidence-rejected");
  }

  @Test
  void directFirstWriteIsNotAcceptedAsRegistrationCreate() throws Exception {
    DeltaOperationMarker marker = marker();
    data()
        .write()
        .format("delta")
        .options(options())
        .options(stringMap(fixture.get("tableProperties")))
        .mode("append")
        .save(target.toString());
    assertEquals(0, snapshot().version());
    assertEquals(data().toJSON().collectAsList(), rows());
    assertEquals("WRITE", commitInfo(0).get("operation").asText());
    assertEquals(
        fixture.get("expectedUserMetadata").asText(), commitInfo(0).get("userMetadata").asText());
    assertProperties();
    DeltaCommitEvidence evidence = inspect(marker);
    assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, evidence.kind());
    assertEquals("OPERATION_MISMATCH", evidence.reason());
    assertTrue(evidence.uuid().isEmpty());
    write(
        target.getParent().resolve("rejected-evidence.json"),
        Map.of("kind", evidence.kind(), "reason", evidence.reason()));
    observe("write0-committed-create-evidence-rejected");
  }

  @Test
  void failedInitialWriteLeavesPublishedEmptyCreateWithoutReplayingIt() throws Exception {
    DeltaOperationMarker marker = marker();
    CountingStage delegate = new CountingStage(nativeStage(false, markedProperties(marker)));
    AtomicInteger publications = new AtomicInteger();
    DeltaPublicationStage owner =
        owner(
            delegate,
            () -> {
              assertMatched(marker);
              publications.incrementAndGet();
            },
            new AtomicInteger());
    owner.commitStagedChanges();
    String uuid = snapshot().metadata().id();
    observe("create0-evidence-matched");
    Exception failure = assertThrows(Exception.class, () -> writeData(failingData()));
    recordFailure("initial-write-failure", failure);
    assertTrue(stack(failure).contains("native-create-source-failure"));
    assertEquals(0, snapshot().version());
    assertEquals(uuid, snapshot().metadata().id());
    assertTrue(rows().isEmpty());
    assertFalse(Files.exists(commitPath(1)));
    assertProperties();
    owner.commitStagedChanges();
    owner.abortStagedChanges();
    assertEquals(1, delegate.commits);
    assertEquals(1, publications.get());
    observe("create-committed-data-not-committed");
  }

  @Test
  void publicationOnlyRetryDoesNotRepeatNativeCreate() throws Exception {
    DeltaOperationMarker marker = marker();
    CountingStage delegate = new CountingStage(nativeStage(false, markedProperties(marker)));
    AtomicInteger publications = new AtomicInteger();
    DeltaPublicationStage owner =
        owner(
            delegate,
            () -> {
              assertMatched(marker);
              if (publications.incrementAndGet() == 1) {
                throw new IllegalStateException("injected-publication-failure");
              }
            },
            new AtomicInteger());
    assertThrows(IllegalStateException.class, owner::commitStagedChanges);
    String uuid = snapshot().metadata().id();
    Map<String, String> beforeRetry = manifest();
    observe("create-committed-publication-failed");
    owner.commitStagedChanges();
    owner.commitStagedChanges();
    assertEquals(1, delegate.commits);
    assertEquals(2, publications.get());
    assertEquals(uuid, snapshot().metadata().id());
    assertEquals(0, snapshot().version());
    assertEquals(beforeRetry, manifest());
    observe("explicit-publication-only-retry");
  }

  @Test
  void exceptionAfterDurableNativeCommitRequiresStatusAndNeverReplays() throws Exception {
    DeltaOperationMarker marker = marker();
    CountingStage delegate = new CountingStage(nativeStage(false, markedProperties(marker)));
    delegate.failAfterCommit = true;
    AtomicInteger publications = new AtomicInteger();
    AtomicInteger aborts = new AtomicInteger();
    DeltaPublicationStage owner = owner(delegate, publications::incrementAndGet, aborts);
    Exception failure = assertThrows(IllegalStateException.class, owner::commitStagedChanges);
    recordFailure("after-durable-commit-failure", failure);
    assertEquals("injected-after-native-commit", failure.getMessage());
    assertMatched(marker);
    String uuid = snapshot().metadata().id();
    Map<String, String> beforeRetry = manifest();
    Exception retry = assertThrows(IllegalStateException.class, owner::commitStagedChanges);
    assertEquals("DELTA_NATIVE_COMMIT_STATUS_NEEDED", retry.getMessage());
    owner.abortStagedChanges();
    owner.abortStagedChanges();
    assertThrows(IllegalStateException.class, owner::commitStagedChanges);
    assertEquals(1, delegate.commits);
    assertEquals(0, delegate.aborts);
    assertEquals(0, publications.get());
    assertEquals(1, aborts.get());
    assertEquals(uuid, snapshot().metadata().id());
    assertEquals(beforeRetry, manifest());
    observe("native-committed-owner-status-needed-no-replay");
  }

  @Test
  void abortIsTerminalWhileNativeInputRemainsUnevaluated() throws Exception {
    CountingStage delegate = new CountingStage(nativeStage(false, properties()));
    AtomicInteger aborts = new AtomicInteger();
    DeltaPublicationStage owner = owner(delegate, () -> {}, aborts);
    // This lifecycle control uses admitted empty options; original-option admission is tested
    // below.
    insert(owner.newWriteBuilder(info(failingData(), Map.of())), failingData());
    assertEquals(-1, snapshot().version());
    owner.abortStagedChanges();
    owner.abortStagedChanges();
    assertEquals(1, delegate.aborts);
    assertEquals(1, aborts.get());
    assertEquals(0, delegate.commits);
    assertThrows(IllegalStateException.class, owner::commitStagedChanges);
    assertThrows(IllegalStateException.class, () -> owner.newWriteBuilder(info(data(), Map.of())));
    assertEquals(-1, snapshot().version());
    assertFalse(hasDataFiles());
    observe("owner-aborted-no-native-commit");
  }

  @Test
  void originalOptionsRemainRejectedByExistingOwnerAdmission() throws Exception {
    AtomicInteger materializations = new AtomicInteger();
    DeltaPublicationStage owner =
        new DeltaPublicationStage(
            "local-create-admission",
            schema,
            PARTITIONS,
            properties(),
            DeltaPublicationStage.Requested.CREATE,
            () -> {},
            (mode, bound, options) -> {
              materializations.incrementAndGet();
              return nativeStage(false, properties());
            },
            () -> {},
            () -> {});
    Exception failure =
        assertThrows(
            IllegalArgumentException.class, () -> owner.newWriteBuilder(info(data(), options())));
    assertEquals("Reserved or unsupported Delta option", failure.getMessage());
    assertEquals(0, materializations.get());
    assertEquals(-1, snapshot().version());
    owner.abortStagedChanges();
    recordFailure("existing-option-admission-gate", failure);
  }

  @Test
  void existingTargetGuardRefusesReplacementBeforeStageConstruction() throws Exception {
    nativeStage(false, properties()).commitStagedChanges();
    writeData(data());
    Snapshot before = snapshot();
    Map<String, String> objects = manifest();
    AtomicInteger stages = new AtomicInteger();
    Exception failure =
        assertThrows(
            IllegalStateException.class,
            () -> {
              // A local fixture precondition, not a production reservation or concurrency fence.
              if (snapshot().version() >= 0) {
                throw new IllegalStateException("fixture-requires-absent-target");
              }
              stages.incrementAndGet();
              StagedTable stage = new DeltaReplacementStage(nativeStage(true, properties()));
              insert(
                  ((SupportsWrite) stage).newWriteBuilder(info(failingData(), options())),
                  failingData());
              stage.commitStagedChanges();
            });
    assertEquals("fixture-requires-absent-target", failure.getMessage());
    assertEquals(0, stages.get());
    assertEquals(before.version(), snapshot().version());
    assertEquals(before.metadata().id(), snapshot().metadata().id());
    assertEquals(1, rows().size());
    assertEquals(objects, manifest());
    observe("fixture-existing-target-refused");
  }

  private DeltaPublicationStage owner(
      CountingStage delegate, Runnable publish, AtomicInteger aborts) {
    return new DeltaPublicationStage(
        "local-create-evidence",
        schema,
        PARTITIONS,
        properties(),
        DeltaPublicationStage.Requested.CREATE,
        () -> {},
        (mode, bound, options) -> {
          assertTrue(mode == OperationMode.SCHEMA_ONLY || mode == OperationMode.QUERY);
          assertEquals(schema, bound);
          assertTrue(options.isEmpty());
          return delegate;
        },
        publish,
        aborts::incrementAndGet);
  }

  private StagedTable nativeStage(boolean replace, Map<String, String> properties) {
    Identifier id = Identifier.of(new String[] {"delta"}, target.toString());
    StagedTable stage =
        replace
            ? catalog.stageCreateOrReplace(id, schema, PARTITIONS, properties)
            : catalog.stageCreate(id, schema, PARTITIONS, properties);
    assertTrue(stage.getClass().getName().contains("StagedDeltaTableV2"));
    return stage;
  }

  private Dataset<Row> data() {
    return spark.sql(
        "SELECT 2L AS id, 'new' AS region, TIMESTAMP '2026-10-30 14:10:54.543278' AS __arkham_loaded_at");
  }

  private Dataset<Row> failingData() {
    return spark.sql(
        "SELECT CAST(raise_error('native-create-source-failure') AS BIGINT) AS id, "
            + "'new' AS region, TIMESTAMP '2026-10-30 14:10:54.543278' AS __arkham_loaded_at");
  }

  private LogicalWriteInfo info(Dataset<Row> source, Map<String, String> options) {
    return new LogicalWriteInfo() {
      /** {@inheritDoc} */
      @Override
      public CaseInsensitiveStringMap options() {
        return new CaseInsensitiveStringMap(options);
      }

      /** {@inheritDoc} */
      @Override
      public String queryId() {
        return testName;
      }

      /** {@inheritDoc} */
      @Override
      public StructType schema() {
        return source.schema();
      }
    };
  }

  private void insert(WriteBuilder builder, Dataset<Row> source) {
    assertInstanceOf(V1Write.class, builder.build()).toInsertableRelation().insert(source, false);
  }

  private Map<String, String> options() {
    return stringMap(fixture.get("writerOptions"));
  }

  private Map<String, String> properties() {
    Map<String, String> properties = stringMap(fixture.get("tableProperties"));
    properties.put("provider", "delta");
    properties.put("location", target.toUri().toString());
    return properties;
  }

  private DeltaOperationMarker marker() {
    return DeltaOperationMarker.create(
        41, 1, "local-native-create", target.toUri().toString(), schema.json());
  }

  private Map<String, String> markedProperties(DeltaOperationMarker marker) {
    return marker.injectInto(properties());
  }

  private Snapshot snapshot() {
    return DeltaLog.forTable(spark, target.toString())
        .update(false, Option.empty(), Option.empty());
  }

  private Path commitPath(long version) {
    return target.resolve("_delta_log").resolve(String.format("%020d.json", version));
  }

  private DeltaCommitEvidence inspect(DeltaOperationMarker marker) throws Exception {
    Snapshot observed = snapshot();
    try (InputStream input = Files.newInputStream(commitPath(0))) {
      return new DeltaCommitActionReader()
          .inspect(
              marker,
              0,
              observed.version(),
              observed.version(),
              observed.metadata().schemaString(),
              input);
    }
  }

  private void assertMatched(DeltaOperationMarker marker) {
    try {
      DeltaCommitEvidence evidence = inspect(marker);
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, evidence.kind(), evidence.reason());
      assertEquals(0, evidence.version().orElseThrow());
      assertEquals(snapshot().metadata().id(), evidence.uuid().orElseThrow());
      write(
          target.getParent().resolve("matched-evidence.json"),
          Map.of(
              "kind",
              evidence.kind(),
              "reason",
              evidence.reason(),
              "version",
              evidence.version().orElseThrow(),
              "uuid",
              evidence.uuid().orElseThrow()));
    } catch (Exception e) {
      throw new AssertionError("Cannot inspect actual CREATE commit", e);
    }
  }

  private void writeData(Dataset<Row> source) {
    source.write().format("delta").options(options()).mode("append").save(target.toString());
  }

  private List<String> rows() {
    return spark.read().format("delta").load(target.toString()).toJSON().collectAsList();
  }

  private void assertWritten(String uuid) throws Exception {
    assertEquals(1, snapshot().version());
    assertEquals(uuid, snapshot().metadata().id());
    assertEquals(data().toJSON().collectAsList(), rows());
    assertEquals("WRITE", commitInfo(1).get("operation").asText());
    assertEquals(
        fixture.get("expectedUserMetadata").asText(), commitInfo(1).get("userMetadata").asText());
    assertProperties();
  }

  private void assertProperties() {
    stringMap(fixture.get("tableProperties"))
        .forEach(
            (key, value) ->
                assertEquals(value, snapshot().metadata().configuration().apply(key), key));
  }

  private JsonNode commitInfo(long version) throws Exception {
    for (String line : Files.readAllLines(commitPath(version))) {
      JsonNode action = JSON.readTree(line);
      if (action.has("commitInfo")) {
        return action.get("commitInfo");
      }
    }
    throw new AssertionError("Missing actual native commitInfo");
  }

  private boolean hasDataFiles() throws Exception {
    try (Stream<Path> paths = Files.exists(target) ? Files.walk(target) : Stream.empty()) {
      return paths.anyMatch(path -> path.toString().endsWith(".parquet"));
    }
  }

  private Map<String, String> manifest() throws Exception {
    Map<String, String> objects = new TreeMap<>();
    try (Stream<Path> paths = Files.exists(target) ? Files.walk(target) : Stream.empty()) {
      for (Path file : (Iterable<Path>) paths.filter(Files::isRegularFile)::iterator) {
        objects.put(target.relativize(file).toString(), Files.size(file) + ":" + hash(file));
      }
    }
    return objects;
  }

  private void observe(String phase) throws Exception {
    Snapshot observed = snapshot();
    Map<String, Object> result = new TreeMap<>();
    result.put("phase", phase);
    result.put("version", observed.version());
    result.put("activeTransaction", OptimisticTransaction.getActive().isDefined());
    result.put("objects", manifest());
    Map<String, String> settings = new TreeMap<>();
    for (String key : settingsBefore.keySet()) {
      settings.put(key, spark.conf().getOption(key).getOrElse(() -> "<unset>"));
    }
    result.put("settings", settings);
    if (observed.version() >= 0) {
      result.put("uuid", observed.metadata().id());
      result.put("metadata", JSON.readTree(observed.metadata().json()));
      result.put("protocol", JSON.readTree(observed.protocol().json()));
      result.put("rows", rows());
    }
    write(target.getParent().resolve(phase + ".json"), result);
  }

  private void recordFailure(String phase, Throwable failure) throws Exception {
    Files.writeString(target.getParent().resolve(phase + ".txt"), stack(failure));
  }

  private static String stack(Throwable failure) {
    StringWriter output = new StringWriter();
    failure.printStackTrace(new PrintWriter(output));
    return output.toString();
  }

  private static Map<String, String> stringMap(JsonNode node) {
    Map<String, String> result = new TreeMap<>();
    node.fields().forEachRemaining(entry -> result.put(entry.getKey(), entry.getValue().asText()));
    return result;
  }

  private static void write(Path file, Object value) throws Exception {
    Files.writeString(file, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value) + "\n");
  }

  private static String hash(Path file) throws Exception {
    return digest(Files.readAllBytes(file));
  }

  private static String digest(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  // Counts calls while retaining the real native stage and all of its unchanged behavior.
  private static final class CountingStage implements StagedTable, SupportsWrite {
    private final StagedTable delegate;
    private int commits;
    private int aborts;
    private boolean failAfterCommit;

    private CountingStage(StagedTable delegate) {
      this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override
    public void commitStagedChanges() {
      commits++;
      delegate.commitStagedChanges();
      if (failAfterCommit) {
        throw new IllegalStateException("injected-after-native-commit");
      }
    }

    /** {@inheritDoc} */
    @Override
    public void abortStagedChanges() {
      aborts++;
      delegate.abortStagedChanges();
    }

    /** {@inheritDoc} */
    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      return ((SupportsWrite) delegate).newWriteBuilder(info);
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
      return delegate.name();
    }

    /** {@inheritDoc} */
    @Override
    public StructType schema() {
      return delegate.schema();
    }

    /** {@inheritDoc} */
    @Override
    public Transform[] partitioning() {
      return delegate.partitioning();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }

    /** {@inheritDoc} */
    @Override
    public Set<TableCapability> capabilities() {
      return delegate.capabilities();
    }
  }
}
