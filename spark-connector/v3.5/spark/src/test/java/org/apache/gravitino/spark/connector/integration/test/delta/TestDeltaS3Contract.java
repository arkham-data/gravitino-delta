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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.meta.TableEntity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Live contract for named Delta registration and data stored through S3A. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
public class TestDeltaS3Contract extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeltaS3Contract.class);
  private static final String CATALOG_NAME = "delta_a";
  private static final String SCHEMA_NAME = "sales";

  @TempDir java.nio.file.Path directory;
  private SparkSession spark;
  private GravitinoMetalake metalake;
  private Catalog registry;
  private FileSystem fileSystem;
  private String metalakeName;
  private String ownedPrefix;

  @AfterEach
  void cleanUp() throws Exception {
    DeltaS3Cleanup.runAll(this::stopSpark, this::deleteOwnedPrefix, this::dropMetalake);
  }

  @Test
  void preservesNamedDeltaLifecycleOnS3AcrossSessionRestart() throws Exception {
    String endpoint = requireProperty("gravitino.delta.test.s3.endpoint");
    String bucket = requireProperty("gravitino.delta.test.s3.bucket");
    Properties credentials = readCredentials();
    String accessKey = requireCredential(credentials, "MINIO_ROOT_USER");
    String secretKey = requireCredential(credentials, "MINIO_ROOT_PASSWORD");
    String runId = UUID.randomUUID().toString();
    metalakeName = "delta_s3_" + runId.replace("-", "");
    ownedPrefix = "s3a://" + bucket + "/gravitino-delta-contract/" + runId;
    String schemaLocation = ownedPrefix + "/" + CATALOG_NAME + "/" + SCHEMA_NAME;
    String ordersLocation = schemaLocation + "/orders/";
    String copyLocation = schemaLocation + "/copy/";

    metalake = client.createMetalake(metalakeName, "", Map.of());
    registry =
        metalake.createCatalog(
            CATALOG_NAME,
            Catalog.Type.RELATIONAL,
            "lakehouse-generic",
            "",
            Map.of("location", ownedPrefix + "/" + CATALOG_NAME));
    registry.asSchemas().createSchema(SCHEMA_NAME, "", Map.of("location", schemaLocation));
    startSpark(endpoint, accessKey, secretKey);
    fileSystem = new Path(ownedPrefix).getFileSystem(spark.sparkContext().hadoopConfiguration());

    spark.sql(
        "CREATE TABLE delta_a.sales.orders USING delta AS "
            + "SELECT * FROM VALUES (1L, 10L), (2L, 20L) AS orders(id, amount)");
    assertRows("delta_a.sales.orders", RowFactory.create(1L, 10L), RowFactory.create(2L, 20L));
    Assertions.assertEquals(ordersLocation, registeredLocation("orders"));
    Assertions.assertTrue(fileSystem.exists(new Path(ordersLocation, "_delta_log")));

    spark.sql("INSERT INTO delta_a.sales.orders VALUES (3L, 30L)");
    assertRows(
        "delta_a.sales.orders",
        RowFactory.create(1L, 10L),
        RowFactory.create(2L, 20L),
        RowFactory.create(3L, 30L));

    spark.sql(
        "MERGE INTO delta_a.sales.orders target "
            + "USING (SELECT * FROM VALUES (2L, 25L), (4L, 40L) AS updates(id, amount)) source "
            + "ON target.id = source.id "
            + "WHEN MATCHED THEN UPDATE SET amount = source.amount "
            + "WHEN NOT MATCHED THEN INSERT *");
    assertRows(
        "delta_a.sales.orders",
        RowFactory.create(1L, 10L),
        RowFactory.create(2L, 25L),
        RowFactory.create(3L, 30L),
        RowFactory.create(4L, 40L));

    spark.sql("CREATE TABLE delta_a.sales.copy USING delta AS SELECT * FROM delta_a.sales.orders");
    assertRows(
        "delta_a.sales.copy",
        RowFactory.create(1L, 10L),
        RowFactory.create(2L, 25L),
        RowFactory.create(3L, 30L),
        RowFactory.create(4L, 40L));
    long copyRegistrationId = registrationId("copy");
    Assertions.assertEquals(copyLocation, registeredLocation("copy"));
    String copyDeltaIdBeforeReplace = nativeSnapshot("copy").metadata().id();
    long copyHistoryBeforeReplace = historyCount("copy");
    Assertions.assertTrue(fileSystem.exists(new Path(copyLocation, "_delta_log")));

    spark.sql(
        "CREATE OR REPLACE TABLE delta_a.sales.copy USING delta AS "
            + "SELECT id, amount + 100L amount FROM delta_a.sales.orders WHERE id <= 2L");
    assertRows("delta_a.sales.copy", RowFactory.create(1L, 110L), RowFactory.create(2L, 125L));
    Assertions.assertEquals(copyRegistrationId, registrationId("copy"));
    Assertions.assertEquals(copyLocation, registeredLocation("copy"));
    String copyDeltaIdAfterReplace = nativeSnapshot("copy").metadata().id();
    Assertions.assertNotEquals(copyDeltaIdBeforeReplace, copyDeltaIdAfterReplace);
    long copyHistoryAfterReplace = historyCount("copy");
    Assertions.assertTrue(copyHistoryAfterReplace > copyHistoryBeforeReplace);
    LOG.info(
        "DELTA S3 COPY IDENTITIES registration={} beforeDelta={} afterDelta={}",
        copyRegistrationId,
        copyDeltaIdBeforeReplace,
        copyDeltaIdAfterReplace);

    stopSpark();
    startSpark(endpoint, accessKey, secretKey);
    assertRows(
        "delta_a.sales.orders",
        RowFactory.create(1L, 10L),
        RowFactory.create(2L, 25L),
        RowFactory.create(3L, 30L),
        RowFactory.create(4L, 40L));
    assertRows("delta_a.sales.copy", RowFactory.create(1L, 110L), RowFactory.create(2L, 125L));
    Assertions.assertEquals(copyRegistrationId, registrationId("copy"));
    Assertions.assertEquals(copyLocation, registeredLocation("copy"));
    Assertions.assertEquals(copyDeltaIdAfterReplace, nativeSnapshot("copy").metadata().id());
    Assertions.assertEquals(copyHistoryAfterReplace, historyCount("copy"));
    Assertions.assertTrue(fileSystem.exists(new Path(copyLocation, "_delta_log")));
  }

  private void startSpark(String endpoint, String accessKey, String secretKey) {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-s3-contract")
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
            .config("spark.sql.catalog.delta_a", NamedDeltaCatalog.class.getName())
            .config("spark.sql.catalog.delta_a.uri", serverUri)
            .config("spark.sql.catalog.delta_a.metalake", metalakeName)
            .config("spark.sql.catalog.delta_a.registry", CATALOG_NAME)
            .config("spark.sql.catalog.delta_a.direct", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", accessKey)
            .config("spark.hadoop.fs.s3a.secret.key", secretKey)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .getOrCreate();
    Assertions.assertEquals(
        System.getProperty("gravitino.delta.test.sparkVersion"), spark.version());
    String expectedDeltaVersion = requireProperty("gravitino.delta.test.deltaVersion");
    String deltaJar =
        DeltaTableV2.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    Assertions.assertTrue(deltaJar.contains("-" + expectedDeltaVersion + ".jar"), deltaJar);
    LOG.info("DELTA S3 RUNTIME spark={} delta={}", spark.version(), expectedDeltaVersion);
  }

  private void stopSpark() throws Exception {
    SparkSession currentSpark = spark;
    spark = null;
    DeltaS3Cleanup.runAll(
        () -> closeBridge(currentSpark),
        () -> {
          if (currentSpark != null) {
            currentSpark.stop();
          }
        },
        SparkSession::clearActiveSession,
        SparkSession::clearDefaultSession);
  }

  private void closeBridge(SparkSession currentSpark) throws Exception {
    if (currentSpark != null) {
      NamedDeltaCatalog bridge =
          (NamedDeltaCatalog) currentSpark.sessionState().catalogManager().catalog(CATALOG_NAME);
      bridge.close();
    }
  }

  private void deleteOwnedPrefix() throws Exception {
    FileSystem currentFileSystem = fileSystem;
    fileSystem = null;
    DeltaS3Cleanup.runAll(
        () -> {
          if (currentFileSystem != null && ownedPrefix != null) {
            currentFileSystem.delete(new Path(ownedPrefix), true);
          }
        },
        () -> {
          if (currentFileSystem != null) {
            currentFileSystem.close();
          }
        });
  }

  private void dropMetalake() throws Exception {
    if (metalakeName != null && client.metalakeExists(metalakeName)) {
      client.dropMetalake(metalakeName, true);
    }
  }

  private Properties readCredentials() throws IOException {
    Properties properties = new Properties();
    try (Reader reader =
        Files.newBufferedReader(
            Paths.get(requireProperty("gravitino.delta.test.s3.credentialsFile")), UTF_8)) {
      properties.load(reader);
    }
    return properties;
  }

  private void assertRows(String table, Row... expected) {
    List<Row> actual =
        spark.sql("SELECT id, amount FROM " + table + " ORDER BY id").collectAsList();
    Assertions.assertEquals(Arrays.asList(expected), actual);
  }

  private String registeredLocation(String table) {
    return registry
        .asTableCatalog()
        .loadTable(NameIdentifier.of(SCHEMA_NAME, table))
        .properties()
        .get("location");
  }

  private Snapshot nativeSnapshot(String table) throws Exception {
    DeltaTableV2 loaded =
        (DeltaTableV2) bridge().loadTable(Identifier.of(new String[] {SCHEMA_NAME}, table));
    return loaded.update();
  }

  private long historyCount(String table) {
    return spark.sql("DESCRIBE HISTORY delta_a.sales." + table).count();
  }

  // Embedded-store observation only: the REST table DTO does not expose this registration ID.
  private long registrationId(String table) {
    try {
      return GravitinoEnv.getInstance()
          .entityStore()
          .get(
              NameIdentifier.of(metalakeName, CATALOG_NAME, SCHEMA_NAME, table),
              Entity.EntityType.TABLE,
              TableEntity.class)
          .id();
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  private NamedDeltaCatalog bridge() throws Exception {
    return (NamedDeltaCatalog) spark.sessionState().catalogManager().catalog(CATALOG_NAME);
  }

  private String requireProperty(String name) {
    String value = System.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required system property " + name);
    }
    return value;
  }

  private String requireCredential(Properties credentials, String name) {
    String value = credentials.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("Missing required credential " + name);
    }
    return value;
  }
}
