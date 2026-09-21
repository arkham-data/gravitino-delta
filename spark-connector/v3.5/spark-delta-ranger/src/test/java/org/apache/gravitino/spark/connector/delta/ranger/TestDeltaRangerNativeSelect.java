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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

/** Java access to Spark's public JVM Dataset construction API for actual execution assertions. */
public final class TestDeltaRangerNativeSelect {
  private TestDeltaRangerNativeSelect() {}

  /** Executes the supplied native plan and returns external rows. */
  public static Row[] rows(SparkSession spark, LogicalPlan plan) {
    return (Row[]) Dataset.ofRows(spark, plan).collect();
  }

  /** Loads a real native descriptor through the wrapper with controlled startup/metadata seams. */
  public static void verifyWrapperDescriptor(SparkSession spark, String location, Snapshot snapshot)
      throws Exception {
    Path directory = Files.createTempDirectory("ranger-wrapper-native");
    Path config = directory.resolve("metadata.properties");
    Files.write(
        config,
        "auth-type=simple\nprincipal=separate-metadata-reader\nlifetime-seconds=300\n"
            .getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-------"));
    SparkConf startup = spark.sparkContext().conf();
    startup.set("spark.gravitino.enableDeltaSupport", "true");
    startup.set("spark.gravitino.enableDeltaRangerSupport", "true");
    startup.set("spark.sql.gravitino.uri", "http://127.0.0.1:1");
    startup.set("spark.sql.gravitino.metalake", "lake");
    startup.set("spark.gravitino.deltaRanger.metadata.config", config.toString());
    String extensions = spark.conf().get("spark.sql.extensions");
    // This test isolates wrapper loading; installed-extension execution is tested separately.
    spark
        .sessionState()
        .conf()
        .setConfString(
            "spark.sql.extensions", DeltaRangerCatalogSpark35.EXTENSION + "," + extensions);
    Catalog metadata = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    Table registration = mock(Table.class);
    when(metadata.name()).thenReturn("a");
    when(metadata.provider()).thenReturn("lakehouse-delta");
    when(metadata.asTableCatalog()).thenReturn(tables);
    DeltaRegistrationReceipt receipt =
        DeltaRegistrationReceipt.of(
            DeltaRegistrationIdentity.of(1, 1, "wrapper-token"),
            DeltaRegistrationCommand.Operation.ADOPT,
            DeltaRegistrationReceipt.State.PUBLISHED,
            "wrapper-run",
            DeltaRegistrationReceipt.Evidence.EXISTING_NATIVE_SNAPSHOT,
            "NONE",
            Optional.empty(),
            Optional.of(location),
            Optional.of(snapshot.metadata().id()),
            OptionalLong.of(snapshot.version()));
    when(registration.properties()).thenReturn(receipt.toProperties());
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenReturn(registration);
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.db").collect();
    spark
        .sql(
            "CREATE TABLE spark_catalog.db.t (sentinel STRING) USING parquet TBLPROPERTIES ('sentinel'='keep')")
        .collect();
    spark.sql("INSERT INTO spark_catalog.db.t VALUES ('untouched')").collect();
    CatalogTable before =
        spark
            .sessionState()
            .catalog()
            .getTableMetadata(new TableIdentifier("t", Option.apply("db")));
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    Fixture fixture = new Fixture();
    DeltaRangerCatalogSpark35.State state =
        new DeltaRangerCatalogSpark35.State(
            lease,
            () -> {
              throw new AssertionError("Read must not use the mutation actor");
            },
            () -> metadata,
            ignored -> fixture.engine);
    try (DeltaRangerCatalogSpark35 wrapper = new DeltaRangerCatalogSpark35(state, () -> metadata)) {
      wrapper.initialize(
          "a",
          new CaseInsensitiveStringMap(
              Collections.singletonMap(
                  "journal-directory", directory.resolve("journal").toString())));
      DeltaTableV2 nativeTable =
          (DeltaTableV2) wrapper.loadTable(Identifier.of(new String[] {"db"}, "t"));
      assertEquals(snapshot.metadata().id(), nativeTable.initialSnapshot().metadata().id());
      assertEquals("a", nativeTable.catalogTable().get().identifier().catalog().get());
      assertEquals(
          "a", nativeTable.toLogicalRelation().catalogTable().get().identifier().catalog().get());
      assertEquals(
          "db", nativeTable.toLogicalRelation().catalogTable().get().identifier().database().get());
      assertEquals("t", nativeTable.toLogicalRelation().catalogTable().get().identifier().table());
      assertEquals(
          before,
          spark
              .sessionState()
              .catalog()
              .getTableMetadata(new TableIdentifier("t", Option.apply("db"))));
      assertEquals(
          "untouched", spark.sql("SELECT sentinel FROM spark_catalog.db.t").first().getString(0));
      spark.conf().set("spark.databricks.delta.catalog.update.enabled", "true");
      assertThrows(
          IllegalArgumentException.class,
          () -> wrapper.loadTable(Identifier.of(new String[] {"db"}, "t")));
    } finally {
      spark.conf().set("spark.databricks.delta.catalog.update.enabled", "false");
      spark.sessionState().conf().setConfString("spark.sql.extensions", extensions);
      fixture.engine.close();
    }
    assertThrows(IllegalStateException.class, lease::get);
  }

  /** Scoped request-sensitive transport double; native Spark and production engine remain real. */
  public static final class Fixture implements DeltaRangerPolicyEngine.Backend, AutoCloseable {
    final RangerBasePlugin plugin = mock(RangerBasePlugin.class);
    final RangerPluginContext context = mock(RangerPluginContext.class);
    RangerAuthContextListener listener;
    /** Denied canonical catalog and column pairs. */
    public final Set<String> denied = new HashSet<>();
    /** Observed evaluation order for original authorization and rewrites. */
    public final List<String> events = new ArrayList<>();
    /** Canonical column selected by the mask fixture. */
    public String maskColumn = "secret";
    /** Catalog receiving the custom mask and filter. */
    public String customCatalog = "a";
    /** Custom mask expression returned by the fixture. */
    public String mask = "concat({col}, '-masked')";
    /** Mask kind returned for the custom catalog. */
    public String maskType = "CUSTOM";
    /** Row filter returned for the custom catalog. */
    public String filter = "region = 'keep'";
    /** Owned real production engine using the request-sensitive transport double. */
    public final DeltaRangerPolicyEngine engine;

    /** Initializes the tested engine boundary without a downloader or shared singleton. */
    public Fixture() throws Exception {
      RangerServiceDef definition;
      try (InputStreamReader reader =
          new InputStreamReader(
              getClass().getResourceAsStream("/ranger-delta-servicedef.json"),
              StandardCharsets.UTF_8)) {
        definition = new Gson().fromJson(reader, RangerServiceDef.class);
      }
      Configuration conf =
          TestDeltaRangerConfig.configuration(Files.createTempDirectory("ranger-select-cache"));
      Properties values = new Properties();
      conf.forEach(e -> values.setProperty(e.getKey(), e.getValue()));
      RangerPluginConfig config = new RangerPluginConfig("spark", "service", "sparkSql", values);
      when(plugin.getConfig()).thenReturn(config);
      when(plugin.getServiceName()).thenReturn("service");
      when(plugin.getServiceDef()).thenReturn(definition);
      when(plugin.getPoliciesVersion()).thenReturn(1L);
      when(plugin.getRolesVersion()).thenReturn(1L);
      when(plugin.getServiceConfigs())
          .thenReturn(Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake"));
      when(plugin.getPluginContext()).thenReturn(context);
      RangerRoles roles = new RangerRoles();
      roles.setServiceName("service");
      roles.setRoleVersion(1L);
      when(plugin.getRangerRoles()).thenReturn(roles);
      when(plugin.getRoles()).thenReturn(roles);
      when(plugin.getRolesFromUserAndGroups(any(), any())).thenReturn(Collections.emptySet());
      doAnswer(
              call -> {
                listener = call.getArgument(0);
                return null;
              })
          .when(plugin)
          .registerAuthContextEventListener(any());
      when(plugin.isAccessAllowed(any(RangerAccessRequest.class)))
          .thenAnswer(
              call -> {
                RangerAccessRequest request = call.getArgument(0);
                String key =
                    request.getResource().getValue("catalog")
                        + ":"
                        + request.getResource().getValue("column");
                events.add("access:" + key + ":" + request.getUser());
                RangerAccessResult result =
                    new RangerAccessResult(0, "service", definition, request);
                result.setIsAllowed(
                    !denied.contains(key) && !"denied-user".equals(request.getUser()));
                return result;
              });
      when(plugin.evalDataMaskPolicies(any(), any()))
          .thenAnswer(
              call -> {
                RangerAccessRequest request = call.getArgument(0);
                RangerAccessResult result =
                    new RangerAccessResult(1, "service", definition, request);
                events.add("mask");
                if (maskColumn.equals(request.getResource().getValue("column"))) {
                  boolean a = customCatalog.equals(request.getResource().getValue("catalog"));
                  result.setMaskType(a ? maskType : "MASK_NULL");
                  result.setMaskedValue(a ? mask : null);
                }
                return result;
              });
      when(plugin.evalRowFilterPolicies(any(), any()))
          .thenAnswer(
              call -> {
                RangerAccessRequest request = call.getArgument(0);
                RangerAccessResult result =
                    new RangerAccessResult(2, "service", definition, request);
                events.add("filter");
                if (customCatalog.equals(request.getResource().getValue("catalog")))
                  result.setFilterExpr(filter);
                return result;
              });
      engine =
          new DeltaRangerPolicyEngine.Owner(() -> this)
              .acquire(new DeltaRangerConfig("service", "spark", "lake", config));
    }

    /** {@inheritDoc} */
    @Override
    public RangerBasePlugin plugin() {
      return plugin;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
      RangerAuthContext published =
          new RangerAuthContext(Collections.emptyMap(), null, plugin.getRoles(), null);
      when(plugin.getCurrentRangerAuthContext()).thenReturn(published);
      when(context.getAuthContext()).thenReturn(published);
      listener.contextChanged();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {}
  }
}
