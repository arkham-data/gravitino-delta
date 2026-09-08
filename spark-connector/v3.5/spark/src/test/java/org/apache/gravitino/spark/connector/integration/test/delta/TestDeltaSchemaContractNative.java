/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information.
 * Licensed under the Apache License, Version 2.0.
 */
package org.apache.gravitino.spark.connector.integration.test.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.tables.DeltaTable;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.VersionInfo;
import org.apache.spark.SparkThrowable;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/** Native Spark/Delta qualification for the closed schema contract. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestDeltaSchemaContractNative {
  private static final String CATALOG = "schema_probe";
  private static final String TABLE_PREFIX = CATALOG + ".db.";
  private static final String MAPPING = "TBLPROPERTIES ('delta.columnMapping.mode'='name')";
  private static final ObjectMapper JSON = new ObjectMapper();
  private final Properties observations = new Properties();
  private SparkSession spark;
  private Configuration storage;
  private FileSystem fs;
  private SchemaContractDeltaCatalog catalog;
  private String prefix;
  private Path artifacts;

  @BeforeAll
  void start() throws Exception {
    String run = UUID.randomUUID().toString();
    String pair = sparkVersion() + "-" + deltaVersion();
    prefix =
        "s3a://"
            + System.getProperty("gravitino.delta.test.s3.bucket")
            + "/gravitino-delta-schema-contract/"
            + pair
            + "/"
            + run;
    artifacts = Paths.get(System.getProperty("gravitino.delta.test.schemaEvidence"), pair, run);
    Files.createDirectories(artifacts);
    Properties credentials = new Properties();
    try (InputStream input =
        Files.newInputStream(
            Paths.get(System.getProperty("gravitino.delta.test.s3.credentialsFile")))) {
      credentials.load(input);
    }
    storage = new Configuration(false);
    storage.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    storage.set("fs.s3a.endpoint", System.getProperty("gravitino.delta.test.s3.endpoint"));
    storage.set("fs.s3a.path.style.access", "true");
    storage.set("fs.s3a.connection.ssl.enabled", "false");
    storage.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    storage.set("fs.s3a.access.key", required(credentials, "MINIO_ROOT_USER"));
    storage.set("fs.s3a.secret.key", required(credentials, "MINIO_ROOT_PASSWORD"));
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-schema-contract-native")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog." + CATALOG, SchemaContractDeltaCatalog.class.getName())
            .config("spark.sql.catalog." + CATALOG + ".base", prefix)
            .config("spark.sql.warehouse.dir", artifacts.resolve("warehouse").toString());
    for (Map.Entry<String, String> entry : storage) {
      builder.config("spark.hadoop." + entry.getKey(), entry.getValue());
    }
    spark = builder.getOrCreate();
    catalog = (SchemaContractDeltaCatalog) spark.sessionState().catalogManager().catalog(CATALOG);
    fs = FileSystem.newInstance(new org.apache.hadoop.fs.Path(prefix).toUri(), storage);
    assertRuntimeVersions();
  }

  @AfterAll
  void stop() throws Exception {
    Exception failure = null;
    try {
      preserve("final");
    } catch (Exception e) {
      failure = e;
    }
    try {
      if (spark != null) spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      if (fs != null) {
        assertTrue(
            prefix.matches(
                "s3a://[^/]+/gravitino-delta-schema-contract/3\\.5\\.[46]-3\\.3\\.[02]/[a-f0-9-]{36}"));
        fs.delete(new org.apache.hadoop.fs.Path(prefix), true);
        fs.close();
      }
    } catch (Exception e) {
      if (failure == null) failure = e;
      else failure.addSuppressed(e);
    }
    if (failure != null) throw failure;
  }

  @Test
  @Order(1)
  void schemaOnlyCreateReplaceAndCorUseRealMappedCommits() throws Exception {
    spark.sql(
        "CREATE TABLE "
            + TABLE_PREFIX
            + "declared (id BIGINT NOT NULL, payload STRUCT<label:STRING NOT NULL> COMMENT 'payload', amount DECIMAL(12,2)) USING delta "
            + MAPPING);
    verify("declared", "CREATE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 0);
    SchemaContractDeltaCatalog.OperationRecord created = record("declared");
    DeltaSnapshotMetadata mapped = snapshot(created.location());
    assertNotEquals(created.marker().schemaJson(), mapped.schemaJson());
    assertTrue(mapped.schemaJson().contains("delta.columnMapping.physicalName"));
    spark.sql(
        "CREATE TABLE "
            + TABLE_PREFIX
            + "v1_mapped (id BIGINT, note STRING) USING delta TBLPROPERTIES "
            + "('delta.columnMapping.mode'='name','gravitino.test.forceV1'='true')");
    SchemaContractDeltaCatalog.OperationRecord v1 = record("v1_mapped");
    try (DeltaMetadataReader reader = new DeltaMetadataReader(storage)) {
      DeltaCommitEvidence mismatch = reader.verify(v1.marker());
      assertEquals(DeltaCommitEvidence.Kind.AMBIGUOUS, mismatch.kind());
      assertEquals("SCHEMA_MISMATCH", mismatch.reason());
    }
    spark.sql(
        "INSERT INTO " + TABLE_PREFIX + "declared VALUES (1, named_struct('label','a'), 1.25)");
    assertNativeViolation(
        "declared.not-null.enforcement",
        "DELTA_NOT_NULL_CONSTRAINT_VIOLATED",
        "23502",
        "id",
        () ->
            spark.sql(
                "INSERT INTO "
                    + TABLE_PREFIX
                    + "declared VALUES (NULL, named_struct('label','bad'), 1.25)"));
    assertNativeViolation(
        "declared.nested-not-null.enforcement",
        "DELTA_NOT_NULL_CONSTRAINT_VIOLATED",
        "23502",
        "payload.label",
        () ->
            spark.sql(
                "INSERT INTO "
                    + TABLE_PREFIX
                    + "declared VALUES (2, named_struct('label',CAST(NULL AS STRING)), 1.25)"));
    spark.sql(
        "REPLACE TABLE "
            + TABLE_PREFIX
            + "declared (id BIGINT NOT NULL, note STRING COMMENT 'replacement') USING delta "
            + MAPPING);
    verify("declared", "REPLACE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 2);
    spark.sql(
        "CREATE OR REPLACE TABLE "
            + TABLE_PREFIX
            + "declared (id BIGINT, same_schema STRING) USING delta "
            + MAPPING);
    verify("declared", "REPLACE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 3);
    spark.sql(
        "CREATE OR REPLACE TABLE "
            + TABLE_PREFIX
            + "cor_absent (id BIGINT, attrs MAP<STRING,STRUCT<n:INT>>) USING delta "
            + MAPPING);
    verify("cor_absent", "CREATE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 0);
    spark.conf().set("spark.databricks.delta.properties.defaults.columnMapping.mode", "name");
    try {
      spark.sql("CREATE TABLE " + TABLE_PREFIX + "session_default (id BIGINT) USING delta");
      verify("session_default", "CREATE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 0);
      spark.sql(
          "CREATE TABLE "
              + TABLE_PREFIX
              + "explicit_none (id BIGINT) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='none')");
      verify("explicit_none", "CREATE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 0);
      assertFalse(record("explicit_none").marker().schemaContract().isPresent());
    } finally {
      spark.conf().unset("spark.databricks.delta.properties.defaults.columnMapping.mode");
    }
    spark.conf().set("spark.databricks.delta.properties.defaults.columnMapping.mode", "none");
    try {
      spark.sql(
          "CREATE TABLE " + TABLE_PREFIX + "explicit_name (id BIGINT) USING delta " + MAPPING);
      verify("explicit_name", "CREATE", DeltaSchemaContract.OperationMode.SCHEMA_ONLY, 0);
      assertTrue(record("explicit_name").marker().schemaContract().isPresent());
    } finally {
      spark.conf().unset("spark.databricks.delta.properties.defaults.columnMapping.mode");
    }
    preserve("schema-only");
  }

  @Test
  @Order(2)
  void collectionNestedNotNullIsCharacterizedAndRejectedBeforeNativeBinding() throws Exception {
    String conf = "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled";
    StructType excluded = excludedCollectionSchema();
    String defaultLocation = prefix + "/db/raw_nested_default";
    spark.conf().unset(conf);
    assertNativeViolation(
        "nested.default.native",
        "DELTA_NESTED_NOT_NULL_CONSTRAINT",
        "0AKDC",
        "items",
        () -> DeltaTable.create(spark).location(defaultLocation).addColumns(excluded).execute());
    assertEarlyExcluded("excluded_nested_default", excluded);

    String alternateLocation = prefix + "/db/raw_nested_alternate";
    spark.conf().set(conf, "true");
    try {
      DeltaTable.create(spark).location(alternateLocation).addColumns(excluded).execute();
      StructType normalized =
          (StructType) StructType.fromJson(snapshot(alternateLocation).schemaJson());
      StructType element =
          (StructType) ((ArrayType) normalized.apply("items").dataType()).elementType();
      assertTrue(element.apply("n").nullable());
      observations.setProperty("nested.alternate.native", "SUPPORTED_NORMALIZED_NULLABLE");
      assertEarlyExcluded("excluded_nested_alternate", excluded);
    } finally {
      spark.conf().unset(conf);
    }

    assertEarlyReservedProperty("excluded_max_id", "delta.columnMapping.maxColumnId", "7");
    assertEarlyReservedProperty("excluded_marker", DeltaOperationMarker.PROPERTY_KEY, "synthetic");
    Dataset<Row> frame =
        spark.createDataFrame(
            Arrays.asList(RowFactory.create(1L)),
            new StructType().add("id", DataTypes.LongType, false));
    for (String option : Arrays.asList("replaceWhere", "mergeSchema", "overwriteSchema")) {
      String table = "excluded_" + option.toLowerCase();
      assertThrows(
          IllegalArgumentException.class,
          () -> frame.writeTo(TABLE_PREFIX + table).using("delta").option(option, "true").create());
      assertFalse(catalog.records().containsKey("db." + table));
      assertFalse(fs.exists(new org.apache.hadoop.fs.Path(prefix + "/db/" + table)));
    }
    preserve("nested-exclusions");
  }

  @Test
  @Order(3)
  void queryAndDataFrameWriterRoutesBindResolvedSchemas() throws Exception {
    String query =
        "SELECT 1L id, named_struct('label','a') payload, "
            + "array(named_struct('n',1)) items, map('k',named_struct('n',2)) attrs, "
            + "CAST(1.25 AS DECIMAL(12,2)) amount";
    spark.sql("CREATE TABLE " + TABLE_PREFIX + "ctas USING delta " + MAPPING + " AS " + query);
    verify("ctas", "CREATE", DeltaSchemaContract.OperationMode.QUERY, 0);
    assertEquals(1, spark.table(TABLE_PREFIX + "ctas").count());
    spark.sql("INSERT INTO " + TABLE_PREFIX + "ctas " + query);
    spark.sql("REPLACE TABLE " + TABLE_PREFIX + "ctas USING delta " + MAPPING + " AS " + query);
    verify("ctas", "REPLACE", DeltaSchemaContract.OperationMode.QUERY, 2);
    spark.sql(
        "CREATE OR REPLACE TABLE " + TABLE_PREFIX + "ctas USING delta " + MAPPING + " AS " + query);
    verify("ctas", "REPLACE", DeltaSchemaContract.OperationMode.QUERY, 3);
    spark.sql(
        "CREATE OR REPLACE TABLE "
            + TABLE_PREFIX
            + "coras_absent USING delta "
            + MAPPING
            + " AS "
            + query);
    verify("coras_absent", "CREATE", DeltaSchemaContract.OperationMode.QUERY, 0);
    StructType nested =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.LongType, false, new MetadataBuilder().build()),
              new StructField(
                  "items",
                  new ArrayType(new StructType().add("n", DataTypes.IntegerType, true), true),
                  true,
                  new MetadataBuilder().putString("comment", "items").build()),
              new StructField(
                  "attrs",
                  new MapType(
                      DataTypes.StringType,
                      new StructType().add("n", DataTypes.IntegerType, true),
                      true),
                  true,
                  new MetadataBuilder().build()),
              new StructField("amount", new DecimalType(12, 2), true, new MetadataBuilder().build())
            });
    Dataset<Row> frame =
        spark.createDataFrame(
            Arrays.asList(
                RowFactory.create(
                    9L,
                    Arrays.asList(RowFactory.create(3)),
                    Collections.singletonMap("k", RowFactory.create(4)),
                    new java.math.BigDecimal("2.50"))),
            nested);
    frame
        .writeTo(TABLE_PREFIX + "df")
        .using("delta")
        .tableProperty("delta.columnMapping.mode", "name")
        .create();
    verify("df", "CREATE", DeltaSchemaContract.OperationMode.QUERY, 0);
    frame
        .writeTo(TABLE_PREFIX + "df")
        .using("delta")
        .tableProperty("delta.columnMapping.mode", "name")
        .replace();
    verify("df", "REPLACE", DeltaSchemaContract.OperationMode.QUERY, 1);
    frame.writeTo(TABLE_PREFIX + "df_cor_absent").using("delta").createOrReplace();
    verify("df_cor_absent", "CREATE", DeltaSchemaContract.OperationMode.QUERY, 0);
    frame
        .writeTo(TABLE_PREFIX + "df")
        .using("delta")
        .tableProperty("delta.columnMapping.mode", "name")
        .createOrReplace();
    verify("df", "REPLACE", DeltaSchemaContract.OperationMode.QUERY, 2);
    preserve("query-writes");
  }

  @Test
  @Order(4)
  void defaultsGeneratedAndIdentityRemainSeparateNativeProbes() throws Exception {
    String generated = pathTable("generated_sql");
    String defaults = pathTable("defaults_sql");
    String identity = pathTable("identity_sql");
    observations.setProperty(
        "generated.sql",
        probe(
            "CREATE TABLE " + generated + " (a INT, b INT GENERATED ALWAYS AS (a + 1)) USING delta",
            AnalysisException.class,
            "UNSUPPORTED_FEATURE.TABLE_OPERATION",
            "0A000"));
    observations.setProperty(
        "default.sql",
        probe(
            "CREATE TABLE "
                + defaults
                + " (a INT DEFAULT 7, note STRING) USING delta TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported')"));
    observations.setProperty(
        "identity.sql",
        probe(
            "CREATE TABLE "
                + identity
                + " (id BIGINT GENERATED ALWAYS AS IDENTITY, note STRING) USING delta",
            ParseException.class,
            "PARSE_SYNTAX_ERROR",
            "42601"));
    if ("SUPPORTED_NATIVE_SYNTAX".equals(observations.getProperty("generated.sql"))) {
      spark.sql("INSERT INTO " + generated + " (a) VALUES (4)");
      assertEquals(5, spark.sql("SELECT b FROM " + generated).head().getInt(0));
      assertNativeViolation(
          "generated.sql.enforcement",
          "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
          "23001",
          "Generated Column",
          () -> spark.sql("INSERT INTO " + generated + " VALUES (4,9)"));
    }
    if ("SUPPORTED_NATIVE_SYNTAX".equals(observations.getProperty("default.sql"))) {
      observations.setProperty(
          "default.omission", probe("INSERT INTO " + defaults + " (note) VALUES ('omitted')"));
      if ("SUPPORTED_NATIVE_SYNTAX".equals(observations.getProperty("default.omission"))) {
        assertEquals(
            7, spark.sql("SELECT a FROM " + defaults + " WHERE note='omitted'").head().getInt(0));
      }
      observations.setProperty(
          "default.keyword", probe("INSERT INTO " + defaults + " VALUES (DEFAULT,'keyword')"));
      if ("SUPPORTED_NATIVE_SYNTAX".equals(observations.getProperty("default.keyword"))) {
        assertEquals(
            7, spark.sql("SELECT a FROM " + defaults + " WHERE note='keyword'").head().getInt(0));
      }
      spark.sql("INSERT INTO " + defaults + " VALUES (NULL,'null')");
      assertEquals(
          1, spark.sql("SELECT a FROM " + defaults + " WHERE note='null' AND a IS NULL").count());
    }
    if ("SUPPORTED_NATIVE_SYNTAX".equals(observations.getProperty("identity.sql"))) {
      spark.sql("INSERT INTO " + identity + " (note) VALUES ('one'),('two')");
      assertEquals(2, spark.sql("SELECT id FROM " + identity).distinct().count());
      assertNativeViolation(
          "identity.sql.enforcement",
          "DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED",
          "42808",
          "id",
          () -> spark.sql("INSERT INTO " + identity + " VALUES (99,'x')"));
    }
    String generatedBuilderLocation = prefix + "/db/generated_builder";
    observations.setProperty(
        "generated.builder",
        builderProbe(
            () ->
                DeltaTable.create(spark)
                    .location(generatedBuilderLocation)
                    .addColumn("a", "INT")
                    .addColumn(
                        DeltaTable.columnBuilder(spark, "b")
                            .dataType("INT")
                            .generatedAlwaysAs("a + 1")
                            .build())
                    .execute()));
    if ("SUPPORTED_NATIVE_BUILDER".equals(observations.getProperty("generated.builder"))) {
      String table = "delta.`" + generatedBuilderLocation + "`";
      spark.sql("INSERT INTO " + table + " (a) VALUES (8)");
      assertEquals(9, spark.sql("SELECT b FROM " + table).head().getInt(0));
      assertNativeViolation(
          "generated.builder.enforcement",
          "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
          "23001",
          "Generated Column",
          () -> spark.sql("INSERT INTO " + table + " VALUES (8,99)"));
    }
    String identityBuilderLocation = prefix + "/db/identity_builder";
    observations.setProperty(
        "identity.builder",
        builderProbe(
            () ->
                DeltaTable.create(spark)
                    .location(identityBuilderLocation)
                    .addColumn(
                        DeltaTable.columnBuilder(spark, "id")
                            .dataType("BIGINT")
                            .generatedAlwaysAsIdentity(10, 3)
                            .build())
                    .addColumn("note", "STRING")
                    .execute()));
    if ("SUPPORTED_NATIVE_BUILDER".equals(observations.getProperty("identity.builder"))) {
      String table = "delta.`" + identityBuilderLocation + "`";
      spark.sql("INSERT INTO " + table + " (note) VALUES ('one'),('two')");
      List<Long> ids =
          spark.sql("SELECT id FROM " + table + " ORDER BY id").as(Encoders.LONG()).collectAsList();
      assertEquals(Arrays.asList(10L, 13L), ids);
      assertNativeViolation(
          "identity.builder.enforcement",
          "DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED",
          "42808",
          "id",
          () -> spark.sql("INSERT INTO " + table + " VALUES (99,'x')"));
      assertEquals(
          13L,
          metadataLong(
              snapshot(identityBuilderLocation).schemaJson(), "delta.identity.highWaterMark"));
      spark.sql("INSERT INTO " + table + " (note) VALUES ('three')");
      assertEquals(16L, spark.sql("SELECT max(id) FROM " + table).head().getLong(0));
      assertEquals(
          16L,
          metadataLong(
              snapshot(identityBuilderLocation).schemaJson(), "delta.identity.highWaterMark"));
    }
    assertFalse(observations.getProperty("generated.sql").isEmpty());
    assertFalse(observations.getProperty("default.sql").isEmpty());
    assertFalse(observations.getProperty("identity.sql").isEmpty());
  }

  private void assertEarlyExcluded(String table, StructType schema) throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog
                .stageCreate(
                    Identifier.of(new String[] {"db"}, table),
                    schema,
                    new Transform[0],
                    Collections.singletonMap("delta.columnMapping.mode", "name"))
                .commitStagedChanges());
    assertFalse(catalog.tableExists(Identifier.of(new String[] {"db"}, table)));
    assertFalse(catalog.records().containsKey("db." + table));
    assertFalse(fs.exists(new org.apache.hadoop.fs.Path(prefix + "/db/" + table)));
  }

  private void assertEarlyReservedProperty(String table, String property, String value)
      throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            catalog.stageCreate(
                Identifier.of(new String[] {"db"}, table),
                new StructType().add("id", DataTypes.LongType),
                new Transform[0],
                Collections.singletonMap(property, value)));
    assertFalse(catalog.records().containsKey("db." + table));
    assertFalse(fs.exists(new org.apache.hadoop.fs.Path(prefix + "/db/" + table)));
  }

  private static StructType excludedCollectionSchema() {
    return new StructType()
        .add(
            new StructField(
                "items",
                new ArrayType(new StructType().add("n", DataTypes.IntegerType, false), true),
                true,
                new MetadataBuilder().build()));
  }

  private String builderProbe(ThrowingRunnable action) throws Exception {
    action.run();
    return "SUPPORTED_NATIVE_BUILDER";
  }

  private String pathTable(String table) {
    return "delta.`" + prefix + "/db/" + table + "`";
  }

  private String probe(String sql) throws Exception {
    spark.sql(sql);
    return "SUPPORTED_NATIVE_SYNTAX";
  }

  private String probe(
      String sql,
      Class<? extends Exception> expectedType,
      String expectedErrorClass,
      String expectedSqlState)
      throws Exception {
    try {
      spark.sql(sql);
      return "SUPPORTED_NATIVE_SYNTAX";
    } catch (Exception failure) {
      return unsupported(failure, expectedType, expectedErrorClass, expectedSqlState);
    }
  }

  private String unsupported(
      Exception failure,
      Class<? extends Exception> expectedType,
      String expectedErrorClass,
      String expectedSqlState)
      throws Exception {
    Throwable current = failure;
    while (current.getCause() != null && current.getCause() != current)
      current = current.getCause();
    if (!expectedType.isInstance(current) || !(current instanceof SparkThrowable)) {
      throw failure;
    }
    SparkThrowable sparkFailure = (SparkThrowable) current;
    String errorClass = sparkFailure.getErrorClass();
    String sqlState = sparkFailure.getSqlState();
    if (!expectedErrorClass.equals(errorClass) || !expectedSqlState.equals(sqlState)) throw failure;
    return "UNSUPPORTED_NATIVE:"
        + current.getClass().getSimpleName()
        + ":"
        + errorClass
        + ":"
        + sqlState;
  }

  private void assertNativeViolation(
      String key,
      String expectedErrorClass,
      String expectedSqlState,
      String expectedMessage,
      ThrowingRunnable action) {
    Exception failure = assertThrows(Exception.class, action::run);
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SparkThrowable) {
        SparkThrowable sparkFailure = (SparkThrowable) current;
        String errorClass = sparkFailure.getErrorClass();
        if (expectedErrorClass.equals(errorClass)
            && expectedSqlState.equals(sparkFailure.getSqlState())
            && current.getMessage().contains(expectedMessage)) {
          observations.setProperty(
              key,
              current.getClass().getSimpleName()
                  + ":"
                  + errorClass
                  + ":"
                  + sparkFailure.getSqlState());
          return;
        }
      }
      current = current.getCause();
    }
    throw new AssertionError("Expected native " + expectedErrorClass + " violation", failure);
  }

  private void verify(
      String table, String selected, DeltaSchemaContract.OperationMode mode, long expectedVersion)
      throws Exception {
    SchemaContractDeltaCatalog.OperationRecord record = record(table);
    assertEquals(selected, record.selectedOperation());
    assertEquals(mode, record.mode());
    List<String> before = manifest();
    preserve("verify-before-" + table + "-" + expectedVersion);
    try (DeltaMetadataReader reader = new DeltaMetadataReader(storage)) {
      DeltaCommitEvidence evidence = reader.verify(record.marker());
      assertEquals(DeltaCommitEvidence.Kind.MATCHED, evidence.kind(), evidence.reason());
      assertEquals(expectedVersion, evidence.version().orElse(-1));
      assertEquals(expectedVersion, reader.latest(record.location()).version());
    }
    List<String> after = manifest();
    assertEquals(before, after, "Production verification must not mutate S3 objects");
    preserve("verify-after-" + table + "-" + expectedVersion);
  }

  private SchemaContractDeltaCatalog.OperationRecord record(String table) {
    return catalog.records().get("db." + table);
  }

  private DeltaSnapshotMetadata snapshot(String location) {
    try (DeltaMetadataReader reader = new DeltaMetadataReader(storage)) {
      return reader.latest(location);
    }
  }

  private void assertRuntimeVersions() throws Exception {
    assertEquals(sparkVersion(), spark.version());
    observations.setProperty("spark.version", spark.version());
    for (String name :
        Arrays.asList(
            DeltaTableV2.class.getName(),
            "io.delta.kernel.Table",
            "io.delta.kernel.defaults.engine.DefaultEngine",
            "io.delta.storage.LogStore")) {
      String source =
          Class.forName(name).getProtectionDomain().getCodeSource().getLocation().toString();
      assertTrue(source.contains("-" + deltaVersion() + ".jar"), source);
      observations.setProperty(name, source);
    }
    observations.setProperty("scala.version", scala.util.Properties.versionNumberString());
    observations.setProperty("hadoop.version", VersionInfo.getVersion());
  }

  private void preserve(String label) throws Exception {
    if (fs == null || artifacts == null) return;
    List<String> manifest = manifest();
    org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(prefix);
    if (fs.exists(root)) {
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
      while (files.hasNext()) {
        LocatedFileStatus status = files.next();
        String relative = root.toUri().relativize(status.getPath().toUri()).getPath();
        byte[] content;
        try (InputStream input = fs.open(status.getPath())) {
          content = readAll(input);
        }
        if (relative.contains("/_delta_log/") && relative.endsWith(".json")) {
          Path target = artifacts.resolve(label).resolve(relative);
          Files.createDirectories(target.getParent());
          Files.write(target, content);
        }
      }
    }
    Files.write(artifacts.resolve(label + "-manifest.tsv"), manifest, StandardCharsets.UTF_8);
    try (OutputStream output = Files.newOutputStream(artifacts.resolve("runtime.properties"))) {
      observations.store(output, "Credential-free native schema qualification");
    }
  }

  private List<String> manifest() throws Exception {
    List<String> manifest = new ArrayList<>();
    org.apache.hadoop.fs.Path root = new org.apache.hadoop.fs.Path(prefix);
    if (fs.exists(root)) {
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(root, true);
      while (files.hasNext()) {
        LocatedFileStatus status = files.next();
        String relative = root.toUri().relativize(status.getPath().toUri()).getPath();
        byte[] content;
        try (InputStream input = fs.open(status.getPath())) {
          content = readAll(input);
        }
        String hash = hex(MessageDigest.getInstance("SHA-256").digest(content));
        manifest.add(relative + "\t" + status.getLen() + "\t" + hash);
      }
    }
    manifest.sort(Comparator.naturalOrder());
    return manifest;
  }

  private static long metadataLong(String schemaJson, String key) throws Exception {
    Long value = findLong(JSON.readTree(schemaJson), key);
    if (value == null) throw new AssertionError("Missing numeric schema metadata " + key);
    return value;
  }

  private static Long findLong(JsonNode value, String key) {
    if (value.isObject()) {
      if (value.has(key) && value.get(key).isIntegralNumber()) return value.get(key).longValue();
      Iterator<JsonNode> children = value.elements();
      while (children.hasNext()) {
        Long found = findLong(children.next(), key);
        if (found != null) return found;
      }
    } else if (value.isArray()) {
      for (JsonNode child : value) {
        Long found = findLong(child, key);
        if (found != null) return found;
      }
    }
    return null;
  }

  private static byte[] readAll(InputStream input) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = input.read(buffer)) >= 0) output.write(buffer, 0, read);
    return output.toByteArray();
  }

  private static String hex(byte[] bytes) {
    return IntStream.range(0, bytes.length)
        .mapToObj(index -> String.format("%02x", bytes[index] & 0xff))
        .collect(Collectors.joining());
  }

  private static String required(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null || value.isEmpty()) throw new IllegalArgumentException("Missing " + key);
    return value;
  }

  private static String sparkVersion() {
    return System.getProperty("gravitino.delta.test.sparkVersion");
  }

  private static String deltaVersion() {
    return System.getProperty("gravitino.delta.test.deltaVersion");
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
