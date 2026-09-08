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
package org.apache.gravitino.spark.connector.integration.test.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitActionReader;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.NativeRegistrationTestSupport;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Owned H2/HTTP/Spark/MinIO recovery acceptance; exception/restart, not process-kill evidence. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
class TestDeltaNativeRegistrationRecovery {
  private static final String INTERNAL = "delta.registration.internal.";
  private static final String ADMIN = "registration-admin";
  private static final String ACTOR = "registration-actor";
  private static final String RECOVERY = "registration-recovery";
  private static final String CATALOG = "native_reg";
  private static final String SCHEMA = "sales";
  private static final ObjectMapper JSON = new ObjectMapper();
  @TempDir Path directory;
  private final List<GravitinoAdminClient> clients = new ArrayList<>();
  private final Properties evidence = new Properties();
  private MiniGravitino server;
  private SparkSession spark;
  private FileSystem fs;
  private String uri;
  private String metalake;
  private String prefix;
  private Path artifacts;
  private Object previousConfig;
  private Object previousMetrics;
  private boolean artifactsPreserved;

  @BeforeEach
  void start() throws Exception {
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousMetrics = FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
    String run = UUID.randomUUID().toString();
    metalake = "native_" + run.replace("-", "");
    prefix =
        "s3a://"
            + System.getProperty("gravitino.delta.test.s3.bucket")
            + "/gravitino-delta-native-registration/"
            + run;
    artifacts =
        Paths.get(
            System.getProperty("gravitino.delta.test.evidence"),
            System.getProperty("gravitino.delta.test.sparkVersion")
                + "-"
                + System.getProperty("gravitino.delta.test.deltaVersion"),
            run);
    Files.createDirectories(artifacts);
    Files.createDirectories(directory.resolve("journal"));
    fs =
        FileSystem.newInstance(
            new org.apache.hadoop.fs.Path(prefix).toUri(),
            NativeRegistrationTestSupport.storageConfiguration());
    startServer();
    GravitinoAdminClient adminClient = client(ADMIN);
    GravitinoMetalake admin = adminClient.createMetalake(metalake, null, Collections.emptyMap());
    for (String user : Arrays.asList(ACTOR, RECOVERY, "registration-outsider")) admin.addUser(user);
    Catalog catalog =
        admin.createCatalog(
            CATALOG,
            Catalog.Type.RELATIONAL,
            "delta-native-registration-test",
            null,
            Collections.emptyMap());
    catalog
        .asSchemas()
        .createSchema(SCHEMA, null, Collections.singletonMap("location", prefix + "/" + SCHEMA));
    grant(admin, ACTOR, Privileges.CreateTable.allow(), Privileges.ModifyTable.allow());
    grant(admin, RECOVERY, Privileges.ModifyTable.allow());
    grant(admin, "registration-outsider");
    startSpark();
  }

  @AfterEach
  void stop() throws Exception {
    try {
      DeltaS3Cleanup.runAll(
          this::stopSpark,
          this::closeServer,
          this::preserveArtifacts,
          () -> {
            if (fs != null && prefix != null && artifactsPreserved) {
              assertTrue(
                  prefix.matches("s3a://[^/]+/gravitino-delta-native-registration/[a-f0-9-]{36}"));
              fs.delete(new org.apache.hadoop.fs.Path(prefix), true);
            }
          });
    } finally {
      if (fs != null) fs.close();
      NativeRegistrationTestSupport.writerStarted();
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
      FieldUtils.writeField(GravitinoEnv.getInstance(), "metricsSystem", previousMetrics, true);
    }
  }

  private void preserveArtifacts() throws Exception {
    if (artifacts == null) return;
    preserveObjects("final-objects");
    if (server != null) throw new IllegalStateException("Cannot archive an open store");
    if (Files.exists(directory.resolve("store.mv.db")))
      Files.copy(
          directory.resolve("store.mv.db"),
          artifacts.resolve("closed-store.mv.db"),
          StandardCopyOption.REPLACE_EXISTING);
    try (OutputStream output = Files.newOutputStream(artifacts.resolve("runtime.properties"))) {
      evidence.store(output, "Test-only recovery boundary; credential-free evidence");
    }
    try (Stream<Path> journals = Files.list(directory.resolve("journal"))) {
      for (Path journal : (Iterable<Path>) journals::iterator)
        Files.copy(
            journal, artifacts.resolve(journal.getFileName()), StandardCopyOption.REPLACE_EXISTING);
    }
    artifactsPreserved = true;
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema", "ctas", "rtas", "cor_absent", "cor_existing"})
  void recoversNativeOperationFromDurableReceiptAfterServerAndSparkRestart(String operation)
      throws Exception {
    String table = "orders";
    boolean replace = operation.equals("rtas") || operation.equals("cor_existing");
    TableEntity prior = null;
    if (replace) {
      spark.sql("CREATE TABLE native_reg.sales.orders USING delta AS SELECT 1 id, 'old' label");
      prior = raw(table);
      assertEquals("0", field(prior, "native.version"));
      spark.sql("INSERT INTO native_reg.sales.orders VALUES (2,'dml')");
      assertEquals(1, nativeVersion(table));
      assertEquals("0", field(raw(table), "native.version"));
    }
    bridge().failAfterNativeCommit();
    String sql =
        operation.equals("schema")
            ? "CREATE TABLE native_reg.sales.orders (id INT, label STRING) USING delta"
            : (operation.equals("rtas")
                    ? "REPLACE"
                    : operation.startsWith("cor_") ? "CREATE OR REPLACE" : "CREATE")
                + " TABLE native_reg.sales.orders USING delta AS SELECT 3 id, 'new' note";
    Exception fault = assertThrows(Exception.class, () -> spark.sql(sql));
    assertTrue(fault.toString().contains("Injected after native commit"), fault.toString());
    TableEntity pending = raw(table);
    assertEquals(replace ? "REPLACING" : "RESERVED", field(pending, "state"));
    assertHidden(table);
    assertEquals(ACTOR, owner(table));
    assertEquals(replace ? 2 : 0, nativeVersion(table));
    if (replace) {
      assertEquals("1", field(pending, "replacement.base.version"));
      assertEquals("0", field(pending, "native.version"));
      assertEquals(prior.id(), pending.id());
      assertEquals(prior.auditInfo(), pending.auditInfo());
      assertEquals(field(prior, "native.uuid"), field(pending, "replacement.base.uuid"));
    }
    JsonNode metadata = action(table, replace ? 2 : 0, "metaData");
    JsonNode commit = action(table, replace ? 2 : 0, "commitInfo");
    DeltaOperationMarker marker =
        DeltaOperationMarker.decode(
            metadata.get("configuration").get(DeltaOperationMarker.PROPERTY_KEY).asText());
    assertEquals(pending.id(), marker.id());
    assertEquals(field(pending, "token"), marker.token());
    assertEquals(field(pending, "location"), marker.normalizedLocation());
    assertEquals(
        replace ? DeltaOperationMarker.Operation.REPLACE : DeltaOperationMarker.Operation.CREATE,
        marker.operation());
    assertEquals(
        operation.equals("schema")
            ? "CREATE TABLE"
            : operation.equals("rtas")
                ? "REPLACE TABLE AS SELECT"
                : operation.startsWith("cor_")
                    ? "CREATE OR REPLACE TABLE AS SELECT"
                    : "CREATE TABLE AS SELECT",
        commit.get("operation").asText());
    if (replace) assertEquals(1, commit.get("readVersion").asLong());
    assertTrue(NativeRegistrationTestSupport.inspect(pending, -1).startsWith("MATCHED:"));
    // Pending data checks deliberately use native paths, not public names.
    assertEquals(
        operation.equals("schema") ? 0 : 1,
        spark.read().format("delta").load(location(table)).count());
    evidence.setProperty("operation", operation);
    evidence.setProperty("pending.id", Long.toString(pending.id()));
    evidence.setProperty("pending.state", field(pending, "state"));
    evidence.setProperty("actual.operation", commit.get("operation").asText());
    evidence.setProperty(
        "pending.base", replace ? field(pending, "replacement.base.version") : "none");
    evidence.setProperty(
        "pending.publication", replace ? field(pending, "native.version") : "none");
    preserveObjects("before-restart");
    Path journal = directory.resolve("journal/orders.properties");
    assertTrue(Files.exists(journal));
    assertThrows(
        Exception.class,
        () -> NativeRegistrationTestSupport.recover(tables("registration-outsider"), journal));
    assertThrows(
        Exception.class, () -> NativeRegistrationTestSupport.recover(tables(RECOVERY), journal));
    marker = null;
    metadata = null;
    commit = null; // No operation marker/verdict survives as recovery authority.
    stopSpark();
    closeServer();
    Files.copy(directory.resolve("store.mv.db"), artifacts.resolve("pending-closed-store.mv.db"));
    startServer();
    assertEquals(pending, raw(table));
    assertHidden(table);
    NativeRegistrationTestSupport.handoffAfterSparkStopped();
    TableCatalog recovery = tables(RECOVERY);
    NativeRegistrationTestSupport.recover(recovery, journal);
    TableEntity published = raw(table);
    assertEquals("PUBLISHED", field(published, "state"));
    assertEquals(pending.id(), published.id());
    assertEquals(pending.auditInfo(), published.auditInfo());
    assertEquals(field(pending, "location"), field(published, "location"));
    assertEquals(ACTOR, owner(table));
    assertEquals(replace ? "2" : "0", field(published, "native.version"));
    assertEquals(
        action(table, replace ? 2 : 0, "metaData").get("id").asText(),
        field(published, "native.uuid"));
    if (replace) assertNotEquals(field(prior, "native.uuid"), field(published, "native.uuid"));
    assertEquals(
        operation.equals("schema") ? "label" : "note",
        tables(ACTOR).loadTable(NameIdentifier.of(SCHEMA, table)).columns()[1].name());
    assertFalse(
        published.properties().keySet().stream()
            .anyMatch(
                k ->
                    k.startsWith(INTERNAL + "replacement.base.")
                        || k.startsWith(INTERNAL + "prior.")));
    long objects = objectCount();
    NativeRegistrationTestSupport.recover(recovery, journal);
    assertEquals(published, raw(table));
    assertEquals(objects, objectCount());
    startSpark();
    assertEquals(replace ? 2 : 0, nativeVersion(table));
    assertTrue(
        bridge().loadTable(Identifier.of(new String[] {SCHEMA}, table)) instanceof DeltaTableV2);
    assertEquals(
        operation.equals("schema") ? 0 : 1,
        spark.sql("SELECT * FROM native_reg.sales.orders").count());
    evidence.setProperty("published.uuid", field(published, "native.uuid"));
    evidence.setProperty("published.version", field(published, "native.version"));
    evidence.setProperty("replay.nativeAdvance", "false");
    evidence.setProperty("recovery.receipt", "reloaded durable operator journal");
  }

  @Test
  void nativeAndFixtureCorruptionNegativesFailClosedAndPreserveData() throws Exception {
    bridge().failAfterNativeCommit();
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "CREATE TABLE native_reg.sales.bad USING delta AS SELECT 7 id, 'sentinel' note"));
    TableEntity pending = raw("bad");
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 0).startsWith("MATCHED:"));
    for (String field : Arrays.asList("token", "generation", "location", "schemaFingerprint")) {
      Map<String, String> corrupt = new HashMap<>(pending.properties());
      corrupt.put(
          INTERNAL + field,
          field.equals("generation")
              ? "9"
              : field.equals("location")
                  ? prefix + "/missing"
                  : field.equals("schemaFingerprint")
                      ? "{\"type\":\"struct\",\"fields\":[]}"
                      : "wrong-token");
      assertTrue(
          NativeRegistrationTestSupport.inspect(copy(pending, pending.id(), corrupt), 0)
              .startsWith("AMBIGUOUS:"),
          field);
    }
    assertTrue(
        NativeRegistrationTestSupport.inspect(
                copy(pending, pending.id() + 1, pending.properties()), 0)
            .startsWith("AMBIGUOUS:"));
    byte[] original = readLog("bad", 0);
    String withoutCommit =
        String.join(
                "\n",
                Arrays.stream(new String(original, StandardCharsets.UTF_8).split("\n"))
                    .filter(line -> !line.contains("\"commitInfo\""))
                    .toArray(String[]::new))
            + "\n";
    // Explicit fixture corruption, not a claim about native writer behavior.
    writeLog("bad", 0, withoutCommit.getBytes(StandardCharsets.UTF_8));
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 0).startsWith("AMBIGUOUS:"));
    NativeRegistrationTestSupport.recover(
        tables(ACTOR), directory.resolve("journal/bad.properties"));
    assertEquals("QUARANTINED", field(raw("bad"), "state"));
    assertHidden("bad");
    preserveObjects("missing-commit-info-fixture");
    writeLog("bad", 0, original);
    fs.delete(log("bad", 0), false);
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 0).startsWith("AMBIGUOUS:"));
    writeLog("bad", 0, original);
    spark.sql(
        "ALTER TABLE delta.`" + location("bad") + "` SET TBLPROPERTIES ('ordinary'='retained')");
    assertEquals(1, nativeVersion("bad"));
    JsonNode inherited = action("bad", 1, "metaData");
    assertEquals(
        action("bad", 0, "metaData").get("configuration").get(DeltaOperationMarker.PROPERTY_KEY),
        inherited.get("configuration").get(DeltaOperationMarker.PROPERTY_KEY));
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 0).startsWith("AMBIGUOUS:"));
    DeltaOperationMarker marker =
        DeltaOperationMarker.decode(
            inherited.get("configuration").get(DeltaOperationMarker.PROPERTY_KEY).asText());
    try (InputStream input = fs.open(log("bad", 1))) {
      assertEquals(
          DeltaCommitEvidence.Kind.AMBIGUOUS,
          new DeltaCommitActionReader()
              .inspect(marker, 1, 1, 1, inherited.get("schemaString").asText(), input)
              .kind());
    }
    assertEquals(
        Collections.singletonList(RowFactory.create(7, "sentinel")),
        spark.read().format("delta").load(location("bad")).collectAsList());
    assertHidden("bad");
    evidence.setProperty(
        "negatives",
        "wrong id/generation/token/location/schema; fixture absent CommitInfo/candidate; native inherited marker ALTER and superseded candidate");
  }

  @Test
  void replacementMissingLineageAndChangedPriorUuidCannotPublish() throws Exception {
    spark.sql("CREATE TABLE native_reg.sales.lineage USING delta AS SELECT 1 id, 'first' label");
    spark.sql("INSERT INTO native_reg.sales.lineage VALUES (2,'dml')");
    TableEntity beforeRejectedBegin = raw("lineage");
    byte[] retainedZero = readLog("lineage", 0);
    fs.delete(log("lineage", 0), false);
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "REPLACE TABLE native_reg.sales.lineage USING delta AS SELECT 3 id, 'next' note"));
    assertEquals(beforeRejectedBegin, raw("lineage"));
    assertEquals("PUBLISHED", field(raw("lineage"), "state"));
    writeLog("lineage", 0, retainedZero);
    bridge().failAfterNativeCommit();
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "REPLACE TABLE native_reg.sales.lineage USING delta AS SELECT 3 id, 'next' note"));
    TableEntity pending = raw("lineage");
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 2).startsWith("MATCHED:"));
    byte[] base = readLog("lineage", 0);
    fs.delete(log("lineage", 0), false);
    assertTrue(NativeRegistrationTestSupport.inspect(pending, 2).startsWith("AMBIGUOUS:"));
    writeLog("lineage", 0, base);
    Map<String, String> corrupt = new HashMap<>(pending.properties());
    corrupt.put(INTERNAL + "replacement.base.uuid", "changed-prior-uuid");
    assertTrue(
        NativeRegistrationTestSupport.inspect(copy(pending, pending.id(), corrupt), 2)
            .startsWith("AMBIGUOUS:"));
    fs.delete(log("lineage", 1), false);
    NativeRegistrationTestSupport.recover(
        tables(ACTOR), directory.resolve("journal/lineage.properties"));
    assertEquals("QUARANTINED", field(raw("lineage"), "state"));
    assertHidden("lineage");
    assertTrue(objectCount() > 0);
    evidence.setProperty(
        "negative.lineage",
        "missing required retained JSON and changed prior UUID fail closed; data objects retained");
  }

  @Test
  void ownedAliasesAndFailedSourceCannotPublish() throws Exception {
    StructType schema = new StructType().add("id", "integer");
    for (String alias :
        Arrays.asList(
            DeltaOperationMarker.PROPERTY_KEY,
            "OPTION.GrAvItInO.ReGiStRaTiOn.OpErAtIoN",
            "option.option.gravitino.registration.operation",
            "delta.registration.receipt.marker")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              bridge()
                  .stageCreate(
                      Identifier.of(new String[] {SCHEMA}, "alias"),
                      schema,
                      new Transform[0],
                      Collections.singletonMap(alias, "forged")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              bridge()
                  .alterTable(
                      Identifier.of(new String[] {SCHEMA}, "alias"),
                      TableChange.setProperty(alias, "forged")));
      assertFalse(tables(ACTOR).tableExists(NameIdentifier.of(SCHEMA, "alias")));
    }
    StagedTable staged =
        bridge()
            .stageCreate(
                Identifier.of(new String[] {SCHEMA}, "options"),
                schema,
                new Transform[0],
                Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ((SupportsWrite) staged)
                .newWriteBuilder(
                    new LogicalWriteInfo() {
                      /** {@inheritDoc} */
                      @Override
                      public String queryId() {
                        return "negative";
                      }
                      /** {@inheritDoc} */
                      @Override
                      public StructType schema() {
                        return schema;
                      }
                      /** {@inheritDoc} */
                      @Override
                      public CaseInsensitiveStringMap options() {
                        return new CaseInsensitiveStringMap(
                            Collections.singletonMap(
                                "option." + DeltaOperationMarker.PROPERTY_KEY, "forged"));
                      }
                    }));
    staged.abortStagedChanges();
    assertHidden("options");
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "CREATE TABLE native_reg.sales.source_failure USING delta AS SELECT CAST(raise_error('intentional-source-failure') AS INT) id"));
    assertHidden("source_failure");
    assertEquals("RESERVED", field(raw("source_failure"), "state"));
    assertFalse(fs.exists(log("source_failure", 0)));
    NativeRegistrationTestSupport.recover(
        tables(ACTOR), directory.resolve("journal/source_failure.properties"));
    assertEquals("QUARANTINED", field(raw("source_failure"), "state"));
    assertThrows(
        Exception.class,
        () ->
            spark.sql(
                "CREATE OR REPLACE TABLE native_reg.sales.schema_replace (id INT) USING delta"));
    assertHidden("schema_replace");
    assertFalse(fs.exists(log("schema_replace", 0)));
    evidence.setProperty(
        "negative.source",
        "native query failure before commit remains hidden; absent log is ambiguous, not safe rollback");
  }

  private void startServer() throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE);
    config.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(),
        directory.resolve("store").toString());
    config.put(Configs.CACHE_ENABLED.getKey(), "false");
    config.put(Configs.CATALOG_LOAD_ISOLATED.getKey(), "false");
    config.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    config.put(Configs.AUTHENTICATORS.getKey(), "simple");
    config.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    config.put("gravitino.auxService.names", "");
    config.put("SimpleAuthUserName", ADMIN);
    server = new MiniGravitino(new MiniGravitinoContext(config, true, true));
    server.start();
    JettyServerConfig jetty =
        JettyServerConfig.fromConfig(
            server.getServerConfig(), GravitinoServer.WEBSERVER_CONF_PREFIX);
    uri = "http://" + jetty.getHost() + ":" + jetty.getHttpPort();
    assertTrue(Files.exists(directory.resolve("store.mv.db")));
    evidence.setProperty("store", GravitinoEnv.getInstance().entityStore().getClass().getName());
    evidence.setProperty("authorizer", server.getServerConfig().get(Configs.AUTHORIZATION_IMPL));
    evidence.setProperty(
        "jdbc",
        GravitinoEnv.getInstance().config().get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL));
    evidence.setProperty("ownedPrefix", prefix);
  }

  private void closeServer() throws Exception {
    for (GravitinoAdminClient client : clients) client.close();
    clients.clear();
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  private void startSpark() throws Exception {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("joined-native-recovery")
            .config("spark.ui.enabled", "false")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.runSQLOnFiles", "true")
            .config("spark.sql.warehouse.dir", directory.resolve("session").toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.native_reg", NativeRegistrationDeltaCatalog.class.getName())
            .config("spark.sql.catalog.native_reg.uri", uri)
            .config("spark.sql.catalog.native_reg.metalake", metalake)
            .config("spark.sql.catalog.native_reg.registry", CATALOG)
            .config(
                "spark.sql.catalog.native_reg.journal", directory.resolve("journal").toString());
    Configuration conf = NativeRegistrationTestSupport.storageConfiguration();
    for (Map.Entry<String, String> entry : conf)
      if (entry.getKey().startsWith("fs.s3a."))
        builder.config("spark.hadoop." + entry.getKey(), entry.getValue());
    spark = builder.getOrCreate();
    NativeRegistrationTestSupport.writerStarted();
    assertEquals(System.getProperty("gravitino.delta.test.sparkVersion"), spark.version());
    evidence.setProperty("spark.version", spark.version());
    String sparkSource =
        SparkSession.class.getProtectionDomain().getCodeSource().getLocation().toString();
    assertTrue(sparkSource.contains("-" + spark.version() + ".jar"), sparkSource);
    evidence.setProperty("spark.codeSource", sparkSource);
    evidence.setProperty("java.version", System.getProperty("java.version"));
    for (String name :
        Arrays.asList(
            DeltaTableV2.class.getName(),
            "io.delta.kernel.Table",
            "io.delta.kernel.defaults.engine.DefaultEngine",
            "io.delta.storage.LogStore")) {
      String source =
          Class.forName(name).getProtectionDomain().getCodeSource().getLocation().toString();
      assertTrue(
          source.contains("-" + System.getProperty("gravitino.delta.test.deltaVersion") + ".jar"),
          source);
      evidence.setProperty(name, source);
    }
    String ranger =
        Class.forName("org.apache.ranger.plugin.service.RangerBasePlugin")
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString();
    assertTrue(ranger.contains("-2.8.0.jar"));
    evidence.setProperty("ranger.artifact", ranger);
    for (String key :
        Arrays.asList(
            "fs.s3a.endpoint",
            "fs.s3a.path.style.access",
            "fs.s3a.connection.ssl.enabled",
            "fs.s3a.aws.credentials.provider")) evidence.setProperty(key, conf.get(key));
  }

  private void stopSpark() throws Exception {
    SparkSession old = spark;
    DeltaS3Cleanup.runAll(
        () -> {
          if (old != null) bridge().close();
        },
        () -> {
          if (old != null) old.stop();
          spark = null;
        },
        SparkSession::clearActiveSession,
        SparkSession::clearDefaultSession);
  }

  private NativeRegistrationDeltaCatalog bridge() {
    return (NativeRegistrationDeltaCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
  }

  private GravitinoAdminClient client(String user) {
    GravitinoAdminClient client = GravitinoAdminClient.builder(uri).withSimpleAuth(user).build();
    clients.add(client);
    return client;
  }

  private TableCatalog tables(String user) {
    return client(user).loadMetalake(metalake).loadCatalog(CATALOG).asTableCatalog();
  }

  private void grant(GravitinoMetalake admin, String user, Privilege... extras) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            CATALOG, Collections.singletonList(Privileges.UseCatalog.allow()));
    List<Privilege> privileges = new ArrayList<>();
    privileges.add(Privileges.UseSchema.allow());
    privileges.addAll(Arrays.asList(extras));
    String role = "role_" + user;
    admin.createRole(
        role,
        Collections.emptyMap(),
        Arrays.asList(catalog, SecurableObjects.ofSchema(catalog, SCHEMA, privileges)));
    admin.grantRolesToUser(Collections.singletonList(role), user);
  }

  private TableEntity raw(String name) throws Exception {
    return NativeRegistrationTestSupport.raw(NameIdentifier.of(metalake, CATALOG, SCHEMA, name));
  }

  private String owner(String name) {
    return GravitinoEnv.getInstance()
        .internalOwnerDispatcher()
        .getOwner(
            metalake,
            MetadataObjects.of(Arrays.asList(CATALOG, SCHEMA, name), MetadataObject.Type.TABLE))
        .get()
        .name();
  }

  private static String field(TableEntity row, String name) {
    return row.properties().get(INTERNAL + name);
  }

  private String location(String name) {
    return prefix + "/" + SCHEMA + "/" + name;
  }

  private org.apache.hadoop.fs.Path log(String table, long version) {
    return new org.apache.hadoop.fs.Path(
        location(table), String.format(Locale.ROOT, "_delta_log/%020d.json", version));
  }

  private long nativeVersion(String table) {
    return spark.sql("DESCRIBE HISTORY delta.`" + location(table) + "`").first().getLong(0);
  }

  private void assertHidden(String table) {
    TableCatalog actor = tables(ACTOR);
    assertFalse(actor.tableExists(NameIdentifier.of(SCHEMA, table)));
    assertThrows(
        NoSuchTableException.class, () -> actor.loadTable(NameIdentifier.of(SCHEMA, table)));
    assertFalse(
        Arrays.stream(actor.listTables(Namespace.of(SCHEMA)))
            .anyMatch(id -> id.name().equals(table)));
  }

  private byte[] readLog(String table, long version) throws Exception {
    byte[] bytes = new byte[Math.toIntExact(fs.getFileStatus(log(table, version)).getLen())];
    try (FSDataInputStream input = fs.open(log(table, version))) {
      input.readFully(bytes);
    }
    return bytes;
  }

  private void writeLog(String table, long version, byte[] bytes) throws Exception {
    try (OutputStream output = fs.create(log(table, version), true)) {
      output.write(bytes);
    }
  }

  private JsonNode action(String table, long version, String key) throws Exception {
    for (String line : new String(readLog(table, version), StandardCharsets.UTF_8).split("\n")) {
      JsonNode action = JSON.readTree(line);
      if (action.has(key)) return action.get(key);
    }
    throw new AssertionError("Missing " + key);
  }

  private long objectCount() throws Exception {
    long count = 0;
    RemoteIterator<LocatedFileStatus> iterator =
        fs.listFiles(new org.apache.hadoop.fs.Path(prefix), true);
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    return count;
  }

  private void preserveObjects(String label) throws Exception {
    if (fs == null || !fs.exists(new org.apache.hadoop.fs.Path(prefix))) return;
    RemoteIterator<LocatedFileStatus> iterator =
        fs.listFiles(new org.apache.hadoop.fs.Path(prefix), true);
    while (iterator.hasNext()) {
      LocatedFileStatus status = iterator.next();
      String relative =
          status
              .getPath()
              .toUri()
              .getPath()
              .substring(new org.apache.hadoop.fs.Path(prefix).toUri().getPath().length() + 1);
      Path target = artifacts.resolve(label).resolve(relative);
      Files.createDirectories(target.getParent());
      try (InputStream input = fs.open(status.getPath())) {
        Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private static TableEntity copy(TableEntity row, long id, Map<String, String> properties) {
    return TableEntity.builder()
        .withId(id)
        .withName(row.name())
        .withNamespace(row.namespace())
        .withAuditInfo(row.auditInfo())
        .withProperties(properties)
        .withColumns(row.columns())
        .build();
  }
}
