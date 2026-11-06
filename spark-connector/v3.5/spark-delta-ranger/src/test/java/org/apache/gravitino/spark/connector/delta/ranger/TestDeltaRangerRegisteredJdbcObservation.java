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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConverter;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import scala.Predef;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;

class TestDeltaRangerRegisteredJdbcObservation {
  private static final Identifier ID = Identifier.of(new String[] {"public"}, "source");

  @Test
  void observationRereadsOwnedMetadataAndUsesTypedCredentials() throws Exception {
    try (Fixture f = new Fixture()) {
      DeltaRangerJdbcSourceBindings.Observation first = f.bindings.read(ID);
      first.requireSame(f.bindings.read(ID));
      verify(f.client, times(2)).loadCatalog("pg");
      verify(f.tables, times(2)).loadTable(NameIdentifier.of("public", "source"));
      assertFalse(Serializable.class.isAssignableFrom(first.getClass()));
      assertFalse(first.toString().contains("private"));
      when(f.catalog.supportsCredentials().getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential("reader", "rotated-private")});
      assertThrows(SecurityException.class, () -> first.requireSame(f.bindings.read(ID)));
    }
  }

  @Test
  void everySourceIdentityAndConfigurationChangeInvalidatesObservation() throws Exception {
    for (String key :
        new String[] {
          "jdbc-url",
          "jdbc-database",
          "jdbc-driver",
          "authorization.ranger.service.name",
          "new-config"
        }) {
      try (Fixture f = new Fixture()) {
        DeltaRangerJdbcSourceBindings.Observation first = f.bindings.read(ID);
        f.properties.put(key, "changed-private");
        assertThrows(SecurityException.class, () -> first.requireSame(f.bindings.read(ID)));
      }
    }
    try (Fixture f = new Fixture()) {
      DeltaRangerJdbcSourceBindings.Observation first = f.bindings.read(ID);
      when(f.table.columns()).thenReturn(new Column[] {Column.of("other", Types.LongType.get())});
      assertThrows(SecurityException.class, () -> first.requireSame(f.bindings.read(ID)));
      when(f.catalog.provider()).thenReturn("jdbc-mysql");
      assertThrows(SecurityException.class, () -> f.bindings.read(ID));
    }
  }

  @Test
  void currentRemoteKindMustRemainPhysicalTable() throws Exception {
    try (Fixture f = new Fixture()) {
      DeltaRangerJdbcSourceBindings.Observation original = f.bindings.read(ID);
      for (String kind :
          new String[] {"VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "PARTITIONED TABLE"}) {
        f.tableKind.set(kind);
        assertThrows(SecurityException.class, () -> original.requireSame(f.bindings.read(ID)));
      }
    }
  }

  @Test
  void leaseCloseExpiryAndForeignObservationFailClosed() throws Exception {
    try (Fixture f = new Fixture();
        Fixture foreign = new Fixture()) {
      DeltaRangerJdbcSourceBindings.Observation first = f.bindings.read(ID);
      assertThrows(SecurityException.class, () -> first.requireSame(foreign.bindings.read(ID)));
      f.clock.set(Duration.ofSeconds(61).toNanos());
      assertThrows(SecurityException.class, () -> f.bindings.requireLive(first));
      assertThrows(SecurityException.class, () -> f.bindings.read(ID));
      foreign.bindings.close();
      assertThrows(SecurityException.class, () -> foreign.bindings.read(ID));
    }
  }

  @Test
  void upstreamErrorAndAuthorityBypassAreRedacted() throws Exception {
    try (Fixture f = new Fixture()) {
      f.properties.put("spark.bypass.sessionInitStatement", "private");
      assertThrows(SecurityException.class, () -> f.bindings.read(ID));
      f.properties.remove("spark.bypass.sessionInitStatement");
      when(f.tables.loadTable(NameIdentifier.of("public", "source")))
          .thenThrow(new IllegalArgumentException("private-password jdbc:postgresql://private"));
      SecurityException failure = assertThrows(SecurityException.class, () -> f.bindings.read(ID));
      assertFalse(failure.toString().contains("private"));
      assertNull(failure.getCause());
    }
  }

  @Test
  void metadataObservationNeverDemandsEndUserSelectAndDetachedConfigDoesNotMutate()
      throws Exception {
    try (Fixture f = new Fixture()) {
      DeltaRangerJdbcSourceBindings.Observation first = f.bindings.read(ID);
      assertDoesNotThrow(() -> first.requireSame(f.bindings.read(ID)));
      f.properties.put("comment", "new");
      assertThrows(SecurityException.class, () -> first.requireSame(f.bindings.read(ID)));
    }
  }

  @Test
  void onlyExactIssuedTableSessionAndMetadataRemainAuthoritative() throws Exception {
    try (Fixture f = new Fixture()) {
      SparkSession session = mock(SparkSession.class);
      SparkContext context = mock(SparkContext.class);
      RuntimeConfig runtime = mock(RuntimeConfig.class);
      when(session.sparkContext()).thenReturn(context);
      when(context.getConf()).thenReturn(new SparkConf(false));
      when(session.conf()).thenReturn(runtime);
      when(runtime.get(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
          .thenAnswer(call -> call.getArgument(1));
      when(runtime.getAll()).thenReturn(Map$.MODULE$.empty());
      DeltaRangerPolicyEngine engine = f.engine;
      DeltaRangerJdbcSourceBindings.Observation observation = f.bindings.read(ID);
      Map<String, String> options = new HashMap<>();
      options.put("url", f.properties.get("jdbc-url"));
      options.put("driver", "org.postgresql.Driver");
      options.put("user", "reader");
      options.put("password", "private-password");
      options.put("dbtable", "\"public\".\"source\"");
      JDBCOptions jdbcOptions =
          new JDBCOptions(
              JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(Predef.$conforms()));
      StructType schema =
          DataTypes.createStructType(
              new StructField[] {DataTypes.createStructField("id", DataTypes.LongType, true)});
      Supplier<SparkJdbcTable> tables =
          () ->
              new SparkJdbcTable(
                  ID,
                  f.table,
                  new JDBCTable(ID, schema, jdbcOptions),
                  new JDBCTableCatalog(),
                  JdbcPropertiesConverter.getNoTablePropertiesInstance(),
                  new SparkTransformConverter(false),
                  new SparkTypeConverter());
      SparkJdbcTable exact = tables.get();
      JDBCTable nativeTable = new JDBCTable(ID, schema, jdbcOptions);
      f.bindings.issue(exact, nativeTable, observation, session, engine);
      DeltaRangerJdbcSourceBindings.BoundJdbc bound = f.bindings.resolve(exact, ID, session);
      assertDoesNotThrow(() -> bound.validateCurrent(session));
      when(runtime.getAll())
          .thenReturn(
              JavaConverters.mapAsScalaMapConverter(
                      Collections.singletonMap("spark.sql.shuffle.partitions", "7"))
                  .asScala()
                  .toMap(Predef.$conforms()));
      assertThrows(SecurityException.class, () -> bound.validateCurrent(session));
      // Retained structure is not an action ticket: current admission must still fail.
      assertDoesNotThrow(() -> f.bindings.resolveRetained(exact, ID, session));
      when(runtime.getAll()).thenReturn(Map$.MODULE$.empty());

      assertThrows(SecurityException.class, () -> f.bindings.resolve(tables.get(), ID, session));
      assertThrows(
          SecurityException.class, () -> f.bindings.resolve(exact, ID, mock(SparkSession.class)));
      assertThrows(
          SecurityException.class,
          () ->
              f.bindings.resolve(exact, Identifier.of(new String[] {"public"}, "other"), session));
      assertFalse(bound.toString().contains("private-password"));
      UserGroupInformation user = UserGroupInformation.getCurrentUser();
      String[] groups = user.getGroupNames();
      UserGroupInformation.createUserForTesting(
          user.getShortUserName(), new String[] {"changed-group"});
      try {
        assertThrows(SecurityException.class, () -> bound.validateCurrent(session));
      } finally {
        UserGroupInformation.createUserForTesting(user.getShortUserName(), groups);
      }

      nativeTable.schema().fields()[0] =
          DataTypes.createStructField(
              "id",
              DataTypes.LongType,
              true,
              new MetadataBuilder().putBoolean("changed-authority", true).build());
      assertThrows(SecurityException.class, bound::requireRetained);
      nativeTable.schema().fields()[0] =
          DataTypes.createStructField("id", DataTypes.LongType, true);
      when(f.catalog.supportsCredentials().getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential("reader", "rotated")});
      assertThrows(SecurityException.class, () -> bound.validateCurrent(session));
    }
  }

  @Test
  void nativeMetadataAndNullableWideningAreBoundSeparatelyFromRegisteredSchema() throws Exception {
    try (Fixture f = new Fixture()) {
      when(f.table.columns())
          .thenReturn(
              new Column[] {
                Column.of(
                    "id", Types.LongType.get(), null, false, false, Column.DEFAULT_VALUE_NOT_SET)
              });
      DeltaRangerJdbcSourceBindings.Observation observation = f.bindings.read(ID);
      Map<String, String> options = new HashMap<>();
      options.put("url", f.properties.get("jdbc-url"));
      options.put("driver", "org.postgresql.Driver");
      options.put("user", "reader");
      options.put("password", "private-password");
      options.put("dbtable", "\"public\".\"source\"");
      JDBCOptions jdbc =
          new JDBCOptions(
              JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(Predef.$conforms()));
      StructType nativeSchema =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField(
                    "id",
                    DataTypes.LongType,
                    true,
                    new MetadataBuilder().putBoolean("isSigned", true).putLong("scale", 0).build())
              });
      assertDoesNotThrow(() -> observation.requireNative(new JDBCTable(ID, nativeSchema, jdbc)));
      when(f.table.columns())
          .thenReturn(
              new Column[] {
                Column.of("id", Types.LongType.get()), Column.of("other", Types.LongType.get())
              });
      DeltaRangerJdbcSourceBindings.Observation ordered = f.bindings.read(ID);
      StructType reversed =
          DataTypes.createStructType(
              new StructField[] {
                DataTypes.createStructField("other", DataTypes.LongType, true),
                DataTypes.createStructField("id", DataTypes.LongType, true)
              });
      assertThrows(
          SecurityException.class, () -> ordered.requireNative(new JDBCTable(ID, reversed, jdbc)));

      for (StructField field :
          new StructField[] {
            DataTypes.createStructField("other", DataTypes.LongType, true),
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("id", DataTypes.LongType, false)
          }) {
        StructType changed = DataTypes.createStructType(new StructField[] {field});
        assertThrows(
            SecurityException.class,
            () -> observation.requireNative(new JDBCTable(ID, changed, jdbc)));
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final DeltaRangerPolicyEngine engine = new TestDeltaRangerNativeSelect.Fixture().engine;
    private final AtomicLong clock = new AtomicLong();
    private final AtomicReference<String> tableKind = new AtomicReference<>("TABLE");
    private final GravitinoClient client = mock(GravitinoClient.class);
    private final Catalog catalog = mock(Catalog.class, RETURNS_DEEP_STUBS);
    private final TableCatalog tables = mock(TableCatalog.class);
    private final Table table = mock(Table.class);
    private final Map<String, String> properties = new HashMap<>();
    private final DeltaRangerMetadataClient metadata =
        new DeltaRangerMetadataClient(clock::get, mock(ScheduledExecutorService.class));
    private final DeltaRangerJdbcSourceBindings bindings;

    private Fixture() throws Exception {
      properties.put("jdbc-url", "jdbc:postgresql://localhost:5432/extraction");
      properties.put("jdbc-database", "extraction");
      properties.put("jdbc-driver", "org.postgresql.Driver");
      properties.put("authorization-provider", "ranger-delta");
      properties.put("authorization.ranger.delta.catalog", "pg");
      properties.put("authorization.ranger.delta.metalake", "lake");
      properties.put("authorization.ranger.service.name", "shared");
      properties.put("authorization.ranger.service.type", "spark");
      when(catalog.name()).thenReturn("pg");
      when(catalog.provider()).thenReturn("jdbc-postgresql");
      when(catalog.properties()).thenReturn(properties);
      when(catalog.supportsSecrets().getSecrets()).thenReturn(Collections.emptyMap());
      when(catalog.supportsCredentials().getCredentials())
          .thenReturn(new Credential[] {new JdbcCredential("reader", "private-password")});
      when(catalog.asTableCatalog()).thenReturn(tables);
      when(tables.loadTable(NameIdentifier.of("public", "source"))).thenReturn(table);
      when(table.name()).thenReturn("source");
      when(table.columns()).thenReturn(new Column[] {Column.of("id", Types.LongType.get())});
      when(table.properties()).thenReturn(Collections.emptyMap());
      when(client.loadCatalog("pg")).thenReturn(catalog);
      metadata.configure("pg", Duration.ofSeconds(60), () -> client);
      bindings =
          new DeltaRangerJdbcSourceBindings(
              metadata,
              "lake",
              "pg",
              "shared",
              "spark",
              (options, database, identifier) -> tableKind.get());
    }

    @Override
    public void close() {
      bindings.close();
      metadata.close();
      engine.close();
    }
  }
}
