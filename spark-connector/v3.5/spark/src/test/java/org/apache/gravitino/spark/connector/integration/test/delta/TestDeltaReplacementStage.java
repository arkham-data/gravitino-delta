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

import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Independent native staged-replacement compatibility diagnostic. */
public class TestDeltaReplacementStage {
  @TempDir Path directory;
  private SparkSession spark;

  @AfterEach
  void stop() {
    if (spark != null) spark.stop();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void replacesRowsSchemaAndHistoryOnce() throws Exception {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-replacement-diagnostic")
            .config("spark.ui.enabled", "false")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", directory.toUri().toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", ReplacementDeltaCatalog.class.getName())
            .getOrCreate();
    sql("CREATE TABLE copied USING delta AS SELECT 1L id, 10L amount");
    Assertions.assertEquals(1, spark.sql("DESCRIBE HISTORY copied").count());
    ReplacementDeltaCatalog catalog =
        (ReplacementDeltaCatalog) spark.sessionState().catalogManager().catalog("spark_catalog");
    Assertions.assertNull(catalog.lastStage, "CTAS is never wrapped");
    sql("CREATE OR REPLACE TABLE copied USING delta AS SELECT 2L id, 20L amount, 'new' note");
    Assertions.assertEquals(1, spark.table("copied").count());
    Assertions.assertEquals(20, spark.table("copied").head().getLong(1));
    Assertions.assertEquals(3, spark.table("copied").schema().size());
    Assertions.assertEquals(2, spark.sql("DESCRIBE HISTORY copied").count());
    Assertions.assertEquals(1, catalog.lastStage.builders);
    Assertions.assertEquals(1, catalog.lastStage.builds);
    Assertions.assertEquals(1, catalog.lastStage.commits);
    Assertions.assertEquals(spark.version().equals("3.5.6") ? 1 : 0, catalog.lastStage.truncates);
    Assertions.assertFalse(
        catalog.lastStage.capabilities().contains(TableCapability.OVERWRITE_BY_FILTER));
    sql("REPLACE TABLE copied USING delta AS SELECT 3L id, 30L amount");
    Assertions.assertEquals(30, spark.table("copied").head().getLong(1));
    Assertions.assertEquals(3, spark.sql("DESCRIBE HISTORY copied").count());
    sql("CREATE OR REPLACE TABLE created USING delta AS SELECT 4L id");
    Assertions.assertEquals(1, spark.sql("DESCRIBE HISTORY created").count());
    Assertions.assertThrows(
        Exception.class, () -> sql("REPLACE TABLE missing USING delta AS SELECT 1L id"));
    Assertions.assertFalse(spark.catalog().tableExists("missing"));
    Assertions.assertFalse(directory.resolve("missing").toFile().exists());
    Assertions.assertThrows(
        Exception.class,
        () ->
            sql(
                "REPLACE TABLE copied USING delta AS SELECT CAST(raise_error('source failed') AS BIGINT) id"));
    Assertions.assertEquals(30, spark.table("copied").head().getLong(1));
    Assertions.assertEquals(3, spark.sql("DESCRIBE HISTORY copied").count());
    Assertions.assertEquals(1, catalog.lastStage.aborts);
    StructType schema = spark.table("copied").schema();
    StagedTable stage =
        catalog.stageReplace(
            Identifier.of(new String[] {"default"}, "copied"),
            schema,
            new Transform[0],
            Map.of("provider", "delta"));
    WriteBuilder builder =
        ((SupportsWrite) stage)
            .newWriteBuilder(
                new LogicalWriteInfo() {
                  @Override
                  public String queryId() {
                    return "truncate-does-not-commit";
                  }

                  @Override
                  public StructType schema() {
                    return schema;
                  }

                  @Override
                  public CaseInsensitiveStringMap options() {
                    return new CaseInsensitiveStringMap(Map.of());
                  }
                });
    ((SupportsTruncate) builder).truncate();
    Assertions.assertEquals(0, catalog.lastStage.commits);
    Assertions.assertEquals(30, spark.table("copied").head().getLong(1));
    Assertions.assertEquals(3, spark.sql("DESCRIBE HISTORY copied").count());
    stage.abortStagedChanges();
    spark
        .sql("SELECT 5L id, 50L amount")
        .writeTo("copied")
        .using("delta")
        .option("userMetadata", "dfv2-replace")
        .replace();
    Assertions.assertEquals(50, spark.table("copied").head().getLong(1));
    // Native CreateDeltaTableCommand's Replace branch omits per-write userMetadata.
    Assertions.assertNull(spark.sql("DESCRIBE HISTORY copied").head().getAs("userMetadata"));
    spark
        .sql("SELECT 6L id, 60L amount")
        .writeTo("copied")
        .using("delta")
        .option("userMetadata", "dfv2-create-or-replace")
        .createOrReplace();
    Assertions.assertEquals(
        "dfv2-create-or-replace",
        spark.sql("DESCRIBE HISTORY copied").head().getAs("userMetadata"));
    Assertions.assertEquals(60, spark.table("copied").head().getLong(1));
    spark
        .sql("SELECT 7L id, 70L amount")
        .write()
        .format("delta")
        .mode(SaveMode.Overwrite)
        .saveAsTable("copied");
    Assertions.assertEquals(70, spark.table("copied").head().getLong(1));
    Assertions.assertEquals(1, spark.table("copied").count());
    Assertions.assertEquals(6, spark.sql("DESCRIBE HISTORY copied").count());
  }

  private void sql(String statement) {
    spark.sql(statement).collectAsList();
  }
}
