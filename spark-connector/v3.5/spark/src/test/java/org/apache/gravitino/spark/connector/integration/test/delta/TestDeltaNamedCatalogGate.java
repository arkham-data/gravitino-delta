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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.immutable.Map$;

/** Real Gravitino registration and native engine acceptance characterization. */
public class TestDeltaNamedCatalogGate extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(TestDeltaNamedCatalogGate.class);
  @TempDir Path directory;
  private SparkSession spark;
  private GravitinoMetalake metalake;

  @AfterAll
  void stopSpark() throws Exception {
    if (spark != null) {
      bridge("delta_a").close();
      bridge("delta_b").close();
      spark.stop();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void characterizeNamedLifecycleAndPublicationFailures() throws Exception {
    metalake = client.createMetalake("delta_gate", "", Map.of());
    for (String name : new String[] {"delta_a", "delta_b"}) {
      Catalog catalog =
          metalake.createCatalog(
              name,
              Catalog.Type.RELATIONAL,
              "lakehouse-generic",
              "",
              Map.of("location", directory.resolve(name).toString()));
      catalog
          .asSchemas()
          .createSchema(
              "sales", "", Map.of("location", directory.resolve(name).resolve("sales").toString()));
    }
    startSpark(true);
    for (String name : new String[] {"delta_a", "delta_b"}) {
      long amount = name.equals("delta_a") ? 11 : 111;
      String location = directory.resolve(name).resolve("sales/registered").toString();
      sql(
          "CREATE TABLE delta.`"
              + location
              + "` USING delta AS SELECT 1L id, "
              + amount
              + "L amount");
      registry(name)
          .asTableCatalog()
          .createTable(
              NameIdentifier.of("sales", "registered"),
              columns(),
              "",
              Map.of("format", "delta", "external", "true", "location", location));
      Assertions.assertEquals(
          amount, spark.sql("SELECT amount FROM " + name + ".sales.registered").head().getLong(0));
      Assertions.assertInstanceOf(
          DeltaTableV2.class,
          bridge(name).loadTable(Identifier.of(new String[] {"sales"}, "registered")));
    }
    String directSql = "CREATE TABLE delta_a.sales.orders (id BIGINT, amount BIGINT) USING delta";
    Exception directFailure = Assertions.assertThrows(Exception.class, () -> sql(directSql));
    Assertions.assertTrue(directFailure.getMessage().contains("SCHEMA_NOT_FOUND"));
    LOG.warn("FAILED ACCEPTANCE GATE: direct named delegation SQL=" + directSql, directFailure);
    Assertions.assertFalse(
        registry("delta_a").asTableCatalog().tableExists(NameIdentifier.of("sales", "orders")));
    Assertions.assertFalse(spark.catalog().databaseExists("sales"));
    restartSpark();
    for (String statement :
        new String[] {
          "CREATE TABLE delta_a.sales.orders (id BIGINT, amount BIGINT) USING delta",
          "CREATE TABLE delta_b.sales.orders (id BIGINT, amount BIGINT) USING delta",
          "INSERT INTO delta_a.sales.orders VALUES (1, 10)",
          "INSERT INTO delta_b.sales.orders VALUES (1, 100)"
        }) {
      sql(statement);
      snapshot();
    }
    Assertions.assertEquals(
        10, spark.sql("SELECT amount FROM delta_a.sales.orders").head().getLong(0));
    Assertions.assertEquals(
        100, spark.sql("SELECT amount FROM delta_b.sales.orders").head().getLong(0));
    sql("CREATE TABLE delta_a.sales.copy USING delta AS SELECT * FROM delta_a.sales.orders");
    snapshot();
    long copyIdentity = identity("delta_a", "copy");
    sql("CREATE OR REPLACE TABLE delta_a.sales.copy USING delta AS SELECT 2L id, 20L amount");
    Assertions.assertEquals(copyIdentity, identity("delta_a", "copy"));
    snapshot();
    Assertions.assertEquals("[2,20]", spark.table("delta_a.sales.copy").head().toString());
    sql(
        "MERGE INTO delta_a.sales.orders t USING (SELECT 1L id, 15L amount) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET amount = s.amount WHEN NOT MATCHED THEN INSERT *");
    snapshot();
    sql("ALTER TABLE delta_a.sales.orders ADD COLUMNS (note STRING)");
    snapshot();
    Assertions.assertEquals(
        15, spark.sql("SELECT amount FROM delta_a.sales.orders").head().getLong(0));
    Assertions.assertEquals(3, spark.table("delta_a.sales.orders").schema().size());
    Assertions.assertEquals(
        2,
        registry("delta_a")
            .asTableCatalog()
            .loadTable(NameIdentifier.of("sales", "orders"))
            .columns()
            .length,
        "Characterization: generic registration is stale");
    sql("DESCRIBE HISTORY delta_a.sales.orders");
    Assertions.assertEquals(
        100, spark.sql("SELECT amount FROM delta_b.sales.orders").head().getLong(0));
    LOG.warn(
        "FAILED ACCEPTANCE GATE: generic Gravitino schema remains stale after native ADD COLUMNS");
    Exception refresh =
        Assertions.assertThrows(
            Exception.class,
            () ->
                registry("delta_a")
                    .asTableCatalog()
                    .alterTable(
                        NameIdentifier.of("sales", "orders"),
                        TableChange.addColumn(new String[] {"note"}, Types.StringType.get())));
    Assertions.assertTrue(
        refresh.getMessage().contains("ALTER TABLE operations are not supported"));
    LOG.warn("FAILED ACCEPTANCE GATE: provider metadata finalization", refresh);
    LOG.info(
        "NAMED ANALYZED PLAN {}",
        spark.table("delta_a.sales.orders").queryExecution().analyzed().treeString());
    Assertions.assertTrue(
        hasAnonymousNativeRelation(
            spark.table("delta_a.sales.orders").queryExecution().analyzed()));
    LOG.warn(
        "FAILED ACCEPTANCE GATE: Delta rewrite removes catalog metadata from named logical relation; Ranger identity is unproven");
    DeltaTableV2 qualifiedString =
        new DeltaTableV2(
            spark,
            new org.apache.hadoop.fs.Path(directory.resolve("delta_a/sales/orders").toString()),
            Option.empty(),
            Option.apply("delta_a.sales.orders"),
            Option.empty(),
            Map$.MODULE$.empty());
    Exception identityFailure =
        Assertions.assertThrows(Exception.class, qualifiedString::getTableIdentifierIfExists);
    Assertions.assertTrue(identityFailure.getMessage().contains("IDENTIFIER_TOO_MANY_NAME_PARTS"));
    LOG.warn("FAILED ACCEPTANCE GATE: naive three-part tableIdentifier", identityFailure);
    characterizeSpecialCommands();
    Assertions.assertAll(
        () -> failureProbe("before-commit"),
        () -> failureProbe("after-commit"),
        () -> failureProbe("after-publication"));
    Map<String, Long> identities = new HashMap<>();
    for (String name : new String[] {"delta_a", "delta_b"}) {
      for (NameIdentifier id : registry(name).asTableCatalog().listTables(Namespace.of("sales"))) {
        identities.put(name + "." + id.name(), identity(name, id.name()));
      }
    }
    Assertions.assertFalse(spark.catalog().databaseExists("sales"));
    Assertions.assertFalse(spark.catalog().databaseExists("delta"));
    restartSpark();
    for (String key : identities.keySet()) {
      String[] parts = key.split("\\.");
      Assertions.assertEquals(identities.get(key), identity(parts[0], parts[1]));
    }
    Assertions.assertEquals(
        11, spark.sql("SELECT amount FROM delta_a.sales.registered").head().getLong(0));
    Assertions.assertEquals(
        111, spark.sql("SELECT amount FROM delta_b.sales.registered").head().getLong(0));
    Assertions.assertEquals(
        15, spark.sql("SELECT amount FROM delta_a.sales.orders").head().getLong(0));
    Assertions.assertEquals(
        100, spark.sql("SELECT amount FROM delta_b.sales.orders").head().getLong(0));
    Assertions.assertEquals(3, spark.table("delta_a.sales.orders").schema().size());
    snapshot();
  }

  private void startSpark(boolean direct) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-named-gate")
            .config("spark.ui.enabled", "false")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.runSQLOnFiles", "true")
            .config("spark.sql.warehouse.dir", directory.resolve("session").toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    for (String name : new String[] {"delta_a", "delta_b"}) {
      builder
          .config("spark.sql.catalog." + name, NamedDeltaCatalog.class.getName())
          .config("spark.sql.catalog." + name + ".uri", serverUri)
          .config("spark.sql.catalog." + name + ".metalake", "delta_gate")
          .config("spark.sql.catalog." + name + ".direct", Boolean.toString(direct));
    }
    spark = builder.getOrCreate();
    Assertions.assertEquals(
        "org.apache.spark.sql.catalyst.catalog.InMemoryCatalog",
        spark.sharedState().externalCatalog().unwrapped().getClass().getName());
    Assertions.assertEquals(
        System.getProperty("gravitino.delta.test.sparkVersion"), spark.version());
    LOG.info(
        "RUNTIME spark={} delta={} scala={}",
        spark.version(),
        DeltaTableV2.class.getProtectionDomain().getCodeSource().getLocation(),
        scala.util.Properties.versionNumberString());
  }

  private NamedDeltaCatalog bridge(String name) throws Exception {
    return (NamedDeltaCatalog) spark.sessionState().catalogManager().catalog(name);
  }

  private Catalog registry(String name) {
    return metalake.loadCatalog(name);
  }

  private void snapshot() {
    for (String name : new String[] {"delta_a", "delta_b"}) {
      for (NameIdentifier id : registry(name).asTableCatalog().listTables(Namespace.of("sales"))) {
        Table table = registry(name).asTableCatalog().loadTable(id);
        LOG.info(
            "REGISTRY {}.{} properties={} schema={} audit={}",
            name,
            id,
            table.properties(),
            Arrays.toString(Arrays.stream(table.columns()).map(Column::name).toArray()),
            table.auditInfo());
        LOG.info("IDENTITY {}.{} gravitino={}", name, id, identity(name, id.name()));
        try {
          DeltaTableV2 loaded =
              (DeltaTableV2)
                  bridge(name).loadTable(Identifier.of(new String[] {"sales"}, id.name()));
          Snapshot nativeSnapshot = loaded.update();
          LOG.info(
              "NATIVE {}.{} deltaId={} version={} protocol={} schema={}",
              name,
              id,
              nativeSnapshot.metadata().id(),
              nativeSnapshot.version(),
              nativeSnapshot.protocol(),
              nativeSnapshot.schema().json());
        } catch (Exception failure) {
          throw new IllegalStateException(failure);
        }
      }
    }
  }

  private void sql(String statement) {
    LOG.info("SQL {}", statement);
    Dataset<Row> result = spark.sql(statement);
    LogicalPlan plan = result.queryExecution().analyzed();
    if (statement.startsWith("MERGE")) {
      LOG.info("MERGE ANALYZED {}", plan.treeString());
      {
        MergeIntoCommand merge = Assertions.assertInstanceOf(MergeIntoCommand.class, plan);
        LOG.info(
            "MERGE SOURCE {} TARGET {}", merge.source().treeString(), merge.target().treeString());
        Assertions.assertTrue(hasAnonymousNativeRelation(merge.target()));
      }
    }
    LOG.info("SQL RESULT {}", result.collectAsList());
  }

  private boolean hasAnonymousNativeRelation(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      LogicalRelation relation = (LogicalRelation) plan;
      LOG.info(
          "V1 IDENTITY catalogTablePresent={} catalog={} identifier={}",
          relation.catalogTable().isDefined(),
          relation.catalogTable().isDefined()
              ? relation.catalogTable().get().identifier().catalog()
              : "None",
          relation.catalogTable().isDefined()
              ? relation.catalogTable().get().identifier()
              : "None");
      if (relation.catalogTable().isEmpty()) return true;
    }
    Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) if (hasAnonymousNativeRelation(children.next())) return true;
    return false;
  }

  private void restartSpark() throws Exception {
    bridge("delta_a").close();
    bridge("delta_b").close();
    spark.stop();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
    startSpark(false);
  }

  // Embedded-store observation only: the current REST table DTO and authorization callbacks do
  // not expose this ID. The bridge never relies on this inspection for its operations.
  private long identity(String catalog, String table) {
    try {
      return GravitinoEnv.getInstance()
          .entityStore()
          .get(
              NameIdentifier.of("delta_gate", catalog, "sales", table),
              Entity.EntityType.TABLE,
              TableEntity.class)
          .id();
    } catch (Exception failure) {
      throw new IllegalStateException(failure);
    }
  }

  private void characterizeSpecialCommands() {
    String[] statements = {
      "CREATE OR REPLACE TABLE delta_a.sales.copy (different STRING) USING delta",
      "ALTER TABLE delta_a.sales.orders SET LOCATION '"
          + directory.resolve("delta_b/sales/orders")
          + "'",
      "ALTER TABLE delta_a.sales.orders RENAME TO delta_a.sales.renamed",
      "REPLACE TABLE delta_a.sales.missing (id BIGINT) USING delta"
    };
    String[] expectedErrors = {
      "DELTA_CREATE_TABLE_SCHEME_MISMATCH",
      "DELTA_CANNOT_SET_LOCATION_ON_PATH_IDENTIFIER",
      "ALTER TABLE operations are not supported",
      "TABLE_OR_VIEW_NOT_FOUND"
    };
    for (int index = 0; index < statements.length; index++) {
      String statement = statements[index];
      Exception failure = Assertions.assertThrows(Exception.class, () -> sql(statement));
      Assertions.assertTrue(
          failure.getMessage().contains(expectedErrors[index]), failure.toString());
      LOG.warn("FAILED ACCEPTANCE / EXPECTED PRECONDITION SQL=" + statement, failure);
      snapshot();
    }
    Assertions.assertEquals("[2,20]", spark.table("delta_a.sales.copy").head().toString());
    Assertions.assertFalse(directory.resolve("delta_a/sales/missing").toFile().exists());
    sql("CREATE OR REPLACE TABLE delta_a.sales.schema_only (id BIGINT) USING delta");
    snapshot();
    Assertions.assertEquals(0, spark.table("delta_a.sales.schema_only").count());
    Assertions.assertEquals(1, spark.table("delta_a.sales.schema_only").schema().size());
    sql("REPLACE TABLE delta_a.sales.schema_only (different STRING) USING delta");
    snapshot();
    Assertions.assertEquals(
        "different", spark.table("delta_a.sales.schema_only").schema().fieldNames()[0]);
    Assertions.assertEquals(
        "id",
        registry("delta_a")
            .asTableCatalog()
            .loadTable(NameIdentifier.of("sales", "schema_only"))
            .columns()[0]
            .name());
    Assertions.assertEquals(2, spark.sql("DESCRIBE HISTORY delta_a.sales.schema_only").count());
    LOG.warn(
        "FAILED ACCEPTANCE GATE: schema-only REPLACE succeeds natively while Gravitino columns remain stale");
  }

  private void failureProbe(String point) throws Exception {
    String table = point.replace('-', '_');
    NameIdentifier id = NameIdentifier.of("sales", table);
    String statement = "CREATE TABLE delta_a.sales." + table + " USING delta AS SELECT 7L id";
    NamedDeltaCatalog catalog = bridge("delta_a");
    catalog.failurePoint = point;
    try {
      Exception failure = Assertions.assertThrows(Exception.class, () -> sql(statement));
      Assertions.assertTrue(failure.getMessage().contains("Injected " + point), failure.toString());
      LOG.warn("INJECTED PUBLICATION FAILURE " + point + " SQL=" + statement, failure);
    } finally {
      catalog.failurePoint = "none";
    }
    Path location = directory.resolve("delta_a/sales/" + table);
    boolean committed = !point.equals("before-commit");
    boolean published = point.equals("after-publication");
    Assertions.assertEquals(committed, Files.isDirectory(location.resolve("_delta_log")));
    Assertions.assertEquals(published, registry("delta_a").asTableCatalog().tableExists(id));
    if (committed) {
      long publishedId = published ? identity("delta_a", table) : -1;
      restartSpark();
      Assertions.assertEquals(published, registry("delta_a").asTableCatalog().tableExists(id));
      Assertions.assertEquals(1, spark.read().format("delta").load(location.toString()).count());
      Assertions.assertEquals(1, spark.sql("DESCRIBE HISTORY delta.`" + location + "`").count());
      Exception retry = Assertions.assertThrows(Exception.class, () -> sql(statement));
      Assertions.assertTrue(
          retry
              .getMessage()
              .contains(
                  published
                      ? "TABLE_OR_VIEW_ALREADY_EXISTS"
                      : "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION"),
          retry.toString());
      LOG.warn("RETRY RESULT " + point, retry);
      Assertions.assertEquals(1, spark.read().format("delta").load(location.toString()).count());
      Assertions.assertEquals(1, spark.sql("DESCRIBE HISTORY delta.`" + location + "`").count());
      if (published) Assertions.assertEquals(publishedId, identity("delta_a", table));
      if (!published) {
        LOG.warn(
            "FAILED ACCEPTANCE GATE: orphan committed Delta data; no durable generic reservation, automatic recovery unavailable");
        registry("delta_a")
            .asTableCatalog()
            .createTable(
                id,
                new Column[] {Column.of("id", Types.LongType.get())},
                "explicit recovery probe",
                Map.of("format", "delta", "external", "true", "location", location.toString()));
      }
    } else {
      sql(statement);
      Assertions.assertEquals(1, spark.sql("DESCRIBE HISTORY delta_a.sales." + table).count());
    }
    snapshot();
  }

  private Column[] columns() {
    return new Column[] {
      Column.of("id", Types.LongType.get()), Column.of("amount", Types.LongType.get())
    };
  }
}
