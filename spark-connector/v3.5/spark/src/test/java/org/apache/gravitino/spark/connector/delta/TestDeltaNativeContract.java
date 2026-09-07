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

import java.nio.file.Path;
import java.util.Arrays;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Native Delta control for the Gravitino integration experiment. */
public class TestDeltaNativeContract {
  @TempDir Path temporaryDirectory;
  private SparkSession spark;

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void nativeLifecycleUsesRequestedRuntime() throws ClassNotFoundException {
    String expectedSpark = System.getProperty("gravitino.delta.test.sparkVersion");
    String expectedDelta = System.getProperty("gravitino.delta.test.deltaVersion");
    Assertions.assertNotNull(expectedSpark);
    Assertions.assertNotNull(expectedDelta);
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-native-contract")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", temporaryDirectory.toUri().toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    Assertions.assertEquals(expectedSpark, spark.version());
    String deltaJar =
        Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .getPath();
    Assertions.assertTrue(deltaJar.contains("-" + expectedDelta + ".jar"), deltaJar);

    spark.sql("CREATE DATABASE native_delta").collectAsList();
    spark
        .sql("CREATE TABLE native_delta.orders (id BIGINT, amount BIGINT) USING delta")
        .collectAsList();
    spark.sql("INSERT INTO native_delta.orders VALUES (1, 10), (2, 20)").collectAsList();
    spark
        .sql(
            "MERGE INTO native_delta.orders t "
                + "USING (SELECT 1L id, 15L amount UNION ALL SELECT 3L, 30L) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount "
                + "WHEN NOT MATCHED THEN INSERT *")
        .collectAsList();
    Assertions.assertEquals(
        65L, spark.sql("SELECT sum(amount) FROM native_delta.orders").head().getLong(0));
    spark.sql("UPDATE native_delta.orders SET amount = amount + 1 WHERE id = 1").collectAsList();
    spark.sql("DELETE FROM native_delta.orders WHERE id = 2").collectAsList();
    Assertions.assertEquals(
        46L, spark.sql("SELECT sum(amount) FROM native_delta.orders").head().getLong(0));
    spark
        .sql("CREATE TABLE native_delta.copied USING delta AS SELECT * FROM native_delta.orders")
        .collectAsList();
    Assertions.assertEquals(2L, spark.table("native_delta.copied").count());
    spark
        .sql(
            "CREATE OR REPLACE TABLE native_delta.copied USING delta "
                + "AS SELECT 9L id, 90L amount")
        .collectAsList();
    Assertions.assertEquals(90L, spark.table("native_delta.copied").head().getLong(1));
    spark.sql("ALTER TABLE native_delta.orders ADD COLUMNS (note STRING)").collectAsList();
    Assertions.assertTrue(
        Arrays.asList(spark.table("native_delta.orders").schema().fieldNames()).contains("note"));
    Assertions.assertTrue(spark.sql("DESCRIBE HISTORY native_delta.orders").count() >= 5);
  }
}
