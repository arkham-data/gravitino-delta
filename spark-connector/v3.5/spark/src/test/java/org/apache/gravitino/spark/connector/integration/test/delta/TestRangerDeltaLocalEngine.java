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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.plugin.spark.authz.ObjectType$;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;
import org.apache.ranger.RangerClient;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Iterator;

/** Real local Delta plans and Ranger engine characterization, with no deployment claim. */
class TestRangerDeltaLocalEngine extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(TestRangerDeltaLocalEngine.class);
  @TempDir Path directory;
  private SparkSession spark;

  @Test
  void nativeIdentityAndRangerEnforcement() throws Exception {
    assertRangerArtifact(RangerClient.class, "ranger-intg");
    assertRangerArtifact(AuditProviderFactory.class, "ranger-audit-core");
    assertRangerArtifact(RangerCredentialProvider.class, "ranger-plugins-cred");
    assertRangerArtifact(RangerAuthzRequest.class, "ranger-authz-api");
    assertRangerArtifact(RangerPluginClassLoader.class, "ranger-plugin-classloader");
    assertRangerArtifact(RangerBasePlugin.class, "ranger-plugins-common");
    assertRangerArtifact(UgsyncCommonConstants.class, "ugsync-util");
    GravitinoMetalake metalake = client.createMetalake("ranger_delta_gate", "", ImmutableMap.of());
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("ranger-delta-gate")
            .config("spark.ui.enabled", "false")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog");
    for (String name : new String[] {"delta_a", "delta_b"}) {
      Catalog catalog =
          metalake.createCatalog(
              name,
              Catalog.Type.RELATIONAL,
              "lakehouse-generic",
              "",
              ImmutableMap.of("location", directory.resolve(name).toString()));
      catalog
          .asSchemas()
          .createSchema(
              "sales",
              "",
              ImmutableMap.of("location", directory.resolve(name).resolve("sales").toString()));
      builder
          .config(
              "spark.sql.catalog." + name,
              "org.apache.gravitino.spark.connector.integration.test.delta.IdentityDeltaCatalog")
          .config("spark.sql.catalog." + name + ".uri", serverUri)
          .config("spark.sql.catalog." + name + ".metalake", "ranger_delta_gate");
    }
    spark = builder.getOrCreate();
    try {
      for (String name : new String[] {"delta_a", "delta_b"}) {
        String location = directory.resolve(name).resolve("sales/orders").toString();
        spark
            .sql(
                "CREATE TABLE delta.`"
                    + location
                    + "` USING delta AS SELECT 1L id, 'visible' allowed, 'secret' denied UNION ALL SELECT 2L, 'second', 'hidden'")
            .collectAsList();
        metalake
            .loadCatalog(name)
            .asTableCatalog()
            .createTable(
                NameIdentifier.of("sales", "orders"),
                new Column[] {
                  Column.of("id", Types.LongType.get()),
                  Column.of("allowed", Types.StringType.get()),
                  Column.of("denied", Types.StringType.get())
                },
                "",
                ImmutableMap.of("format", "delta", "external", "true", "location", location));
        LogicalRelation scan =
            scan(spark.table(name + ".sales.orders").queryExecution().analyzed());
        LOG.info("NATIVE READ resource={} plan={}", name + ".sales.orders", scan.treeString());
        Assertions.assertTrue(
            scan.catalogTable().isDefined(), "Native read must retain canonical identity");
        Assertions.assertEquals(name, scan.catalogTable().get().identifier().catalog().get());
      }
      MergeIntoCommand merge =
          Assertions.assertInstanceOf(
              MergeIntoCommand.class,
              spark
                  .sessionState()
                  .analyzer()
                  .execute(
                      spark
                          .sessionState()
                          .sqlParser()
                          .parsePlan(
                              "MERGE INTO delta_a.sales.orders t USING delta_b.sales.orders s ON t.id = s.id WHEN MATCHED THEN UPDATE SET allowed = s.allowed")));
      Assertions.assertEquals(
          "delta_a", scan(merge.target()).catalogTable().get().identifier().catalog().get());
      Assertions.assertEquals(
          "delta_b", scan(merge.source()).catalogTable().get().identifier().catalog().get());
      LOG.info(
          "NATIVE MERGE source={} target={}",
          merge.source().treeString(),
          merge.target().treeString());
      Assertions.assertFalse(spark.catalog().databaseExists("sales"));
      for (String name : new String[] {"delta_a", "delta_b"})
        ((NamedDeltaCatalog) spark.sessionState().catalogManager().catalog(name)).close();
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark =
          builder
              .config(
                  "spark.sql.extensions",
                  "io.delta.sql.DeltaSparkSessionExtension,org.apache.gravitino.spark.connector.integration.test.delta.CatalogAwareRangerExtension")
              .getOrCreate();
      LocalRangerEngine.initialize();
      AccessResource stockA =
          AccessResource.apply(
              ObjectType$.MODULE$.COLUMN(),
              "sales",
              "orders",
              "allowed",
              Option.empty(),
              Option.apply("delta_a"));
      AccessResource stockB =
          AccessResource.apply(
              ObjectType$.MODULE$.COLUMN(),
              "sales",
              "orders",
              "allowed",
              Option.empty(),
              Option.apply("delta_b"));
      Assertions.assertEquals("delta_a", stockA.catalog().get());
      Assertions.assertNull(stockA.getValue("catalog"));
      Assertions.assertEquals(stockA.getAsMap(), stockB.getAsMap());
      LOG.warn(
          "STOCK KYUUBI catalog={} resource={} versus catalog={} resource={}",
          stockA.catalog(),
          stockA.getAsMap(),
          stockB.catalog(),
          stockB.getAsMap());
      String originalSubject = UserGroupInformation.getCurrentUser().getShortUserName();
      UserGroupInformation.createRemoteUser("reader")
          .doAs(
              (PrivilegedExceptionAction<Void>)
                  () -> {
                    Dataset<Row> allowedQuery =
                        spark.sql("SELECT allowed FROM delta_a.sales.orders");
                    LOG.info(
                        "SECURED ANALYZED {} OPTIMIZED {}",
                        allowedQuery.queryExecution().analyzed().treeString(),
                        allowedQuery.queryExecution().optimizedPlan().treeString());
                    List<String> rows =
                        allowedQuery.collectAsList().stream()
                            .map(Object::toString)
                            .collect(Collectors.toList());
                    LOG.info("EXECUTED ALLOWED ROWS {}", rows);
                    Assertions.assertEquals(
                        ImmutableList.of("[MASKED]"),
                        rows,
                        "Real Ranger mask and row filter must rewrite executed rows");
                    for (String query :
                        ImmutableList.of(
                            "SELECT denied FROM delta_a.sales.orders",
                            "SELECT * FROM delta_a.sales.orders",
                            "SELECT allowed FROM delta_b.sales.orders"))
                      assertDenied(query, "Ranger denied");
                    assertDenied(
                        "SELECT allowed FROM delta.`"
                            + directory.resolve("delta_a/sales/orders")
                            + "`",
                        "Missing canonical Delta identity");
                    LocalRangerPolicyClient.revokeColumnPolicy();
                    long start = System.nanoTime();
                    Assertions.assertEquals(2L, LocalRangerEngine.refresh());
                    LOG.info(
                        "LOCAL REFRESH version=2 elapsedMs={}",
                        (System.nanoTime() - start) / 1000000);
                    List<String> requestSnapshot = LocalRangerEngine.requests();
                    Assertions.assertThrows(
                        UnsupportedOperationException.class,
                        () -> requestSnapshot.add("snapshot must stay immutable"));
                    int requestCount = requestSnapshot.size();
                    assertDenied(
                        "SELECT allowed FROM delta_a.sales.orders",
                        "Ranger denied reader delta_a/sales/orders/allowed");
                    Assertions.assertEquals(
                        requestCount,
                        requestSnapshot.size(),
                        "Captured requests must remain a copied snapshot");
                    List<String> currentRequests = LocalRangerEngine.requests();
                    List<String> refreshedRequests =
                        currentRequests.subList(requestCount, currentRequests.size());
                    Assertions.assertTrue(
                        refreshedRequests.stream()
                            .anyMatch(
                                request ->
                                    request.startsWith("access user=reader")
                                        && request.contains("catalog=delta_a")
                                        && request.contains("database=sales")
                                        && request.contains("table=orders")
                                        && request.contains("column=allowed")
                                        && request.contains("policy=-1")
                                        && request.endsWith("allowed=false")),
                        () -> "Fresh canonical request was not denied: " + refreshedRequests);
                    return null;
                  });
      Assertions.assertEquals(
          originalSubject, UserGroupInformation.getCurrentUser().getShortUserName());
      Assertions.assertFalse(LocalRangerEngine.requests().isEmpty());
      Assertions.assertTrue(
          LocalRangerEngine.requests().stream()
              .allMatch(
                  request ->
                      request.contains("user=reader")
                          && request.contains("catalog=delta_")
                          && !request.contains("metadata_service")));
      Assertions.assertTrue(
          LocalRangerEngine.requests().stream()
              .anyMatch(request -> request.contains("catalog=delta_b")));
      for (String kind : ImmutableList.of("access", "mask", "filter"))
        Assertions.assertTrue(
            LocalRangerEngine.requests().stream().anyMatch(request -> request.startsWith(kind)));
    } finally {
      for (String name : new String[] {"delta_a", "delta_b"})
        ((NamedDeltaCatalog) spark.sessionState().catalogManager().catalog(name)).close();
      LocalRangerEngine.close();
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  @Nullable
  private LogicalRelation scan(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) return (LogicalRelation) plan;
    Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      LogicalRelation scan = scan(children.next());
      if (scan != null) return scan;
    }
    return null;
  }

  private void assertDenied(String query, String expectedReason) {
    Exception denied =
        Assertions.assertThrows(Exception.class, () -> spark.sql(query).collectAsList());
    LOG.info("DENIED SQL={} reason={}", query, denied.toString());
    Assertions.assertTrue(
        denied.toString().contains(expectedReason),
        () -> "Expected denial containing '" + expectedReason + "', but received " + denied);
  }

  private static void assertRangerArtifact(Class<?> type, String artifact) {
    Assertions.assertNotNull(type.getProtectionDomain().getCodeSource());
    String location = type.getProtectionDomain().getCodeSource().getLocation().toExternalForm();
    LOG.info("RANGER RUNTIME class={} location={}", type.getName(), location);
    Assertions.assertTrue(
        location.endsWith("/" + artifact + "-2.8.0.jar"),
        () -> type.getName() + " must load from Ranger 2.8.0, but loaded " + location);
  }
}
