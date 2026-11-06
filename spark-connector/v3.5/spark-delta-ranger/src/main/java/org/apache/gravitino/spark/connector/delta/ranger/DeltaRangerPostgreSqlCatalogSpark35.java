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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark35;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Optional read-only PostgreSQL catalog issuing exact driver-local governed source authority. */
public final class DeltaRangerPostgreSqlCatalogSpark35 extends GravitinoPostgreSqlCatalogSpark35
    implements AutoCloseable {
  private final State state;
  @Nullable private DeltaRangerPolicyEngine engine;
  @Nullable private DeltaRangerJdbcSourceBindings bindings;
  @Nullable private DeltaRangerJdbcSourceBindings.Observation loading;
  @Nullable private SparkSession session;
  @Nullable private SparkContext context;
  @Nullable private SparkListener shutdown;
  @Nullable private String canonicalName;
  private boolean initialized;
  private boolean closed;

  /** Creates the Spark-loadable wrapper without acquiring metadata or policy authority. */
  public DeltaRangerPostgreSqlCatalogSpark35() {
    this(new State());
  }

  DeltaRangerPostgreSqlCatalogSpark35(State state) {
    this.state = state;
  }

  /** Initializes the stock PostgreSQL catalog after validating trusted startup and visibility. */
  @Override
  public synchronized void initialize(String name, CaseInsensitiveStringMap options) {
    if (closed || initialized)
      throw new IllegalStateException("Registered JDBC catalog is closed or initialized");
    initialized = true;
    try {
      session = SparkSession.active();
      SparkConf conf = session.sparkContext().getConf();
      require(
          conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT, false)
              && conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_SUPPORT, false)
              && conf.getBoolean(
                  GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_JDBC_SUPPORT, false),
          "Registered JDBC requires all three startup opt-ins");
      require(
          "simple".equalsIgnoreCase(conf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "simple")),
          "Registered JDBC requires SIMPLE application authentication");
      require(
          Arrays.asList(session.conf().get("spark.sql.extensions", "").split(","))
              .contains(DeltaRangerCatalogSpark35.EXTENSION),
          "Install DeltaRangerSparkExtension before creating the session");
      require(options.isEmpty(), "Registered JDBC catalog options must come from owned metadata");
      canonicalName = DeltaRangerResourceKey.literal(name);
      requireVisible();
      DeltaRangerCatalogSpark35.MetadataIdentity identity =
          DeltaRangerCatalogSpark35.MetadataIdentity.read(
              Paths.get(required(conf, DeltaRangerCatalogSpark35.PREFIX + "metadata.config")));
      require(
          !identity.principal.equals(session.sparkContext().sparkUser()),
          "A distinct metadata identity is required");
      try {
        require(
            !identity.principal.equals(UserGroupInformation.getCurrentUser().getShortUserName()),
            "A distinct metadata identity is required");
      } catch (IOException failure) {
        throw new IllegalArgumentException("Unable to establish current actor");
      }
      String metalake = required(conf, GravitinoSparkConfig.GRAVITINO_METALAKE);
      state.metadata.configure(
          required(conf, GravitinoSparkConfig.GRAVITINO_URI),
          metalake,
          name,
          identity.lifetime,
          builder -> builder.withSimpleAuth(identity.principal));
      engine = state.engines.apply(conf);
      bindings =
          new DeltaRangerJdbcSourceBindings(
              state.metadata,
              metalake,
              name,
              engine.versions().service(),
              engine.versions().definition());
      // Reuse the existing converter and typed-credential initialization without replacing its
      // client.
      super.initialize(name, options);
      // Native Delta analyzer construction installs its Parquet field-id settings lazily.
      // Complete that stock construction before exact source/session settings are frozen.
      session.sessionState().analyzer();
      context = session.sparkContext();
      shutdown =
          new SparkListener() {
            /** {@inheritDoc} */
            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd event) {
              DeltaRangerPostgreSqlCatalogSpark35.this.close();
            }
          };
      context.addSparkListener(shutdown);
    } catch (RuntimeException | LinkageError failure) {
      close();
      if (failure instanceof StartupRejected) throw (StartupRejected) failure;
      throw new SecurityException("Registered JDBC catalog initialization rejected");
    }
  }

  /** Rejects unsupported driver and callback configuration before stock catalog initialization. */
  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    if (!options.isEmpty())
      throw new SecurityException("Registered JDBC catalog overrides rejected");
    DeltaRangerJdbcSourceBindings.validateCatalogProperties(properties);
    return super.createAndInitSparkCatalog(name, options, properties);
  }

  /** Loads metadata through the owned identity, retaining before/after native observations. */
  @Override
  public synchronized Table loadTable(Identifier id) {
    requireVisible();
    DeltaRangerJdbcSourceBindings authority = sourceBindings();
    if (loading != null) throw new SecurityException("Nested registered JDBC load");
    try {
      loading = authority.read(id);
      Table result = super.loadTable(id);
      loading.requireSame(authority.read(id));
      return result;
    } catch (Exception | LinkageError failure) {
      throw new SecurityException("Registered JDBC source load rejected");
    } finally {
      loading = null;
    }
  }

  /** Resolves schema metadata without requesting eager end-user SELECT on unused columns. */
  @Override
  protected synchronized org.apache.gravitino.rel.Table loadGravitinoTable(Identifier id) {
    requireVisible();
    if (loading == null) throw new SecurityException("Unbound registered JDBC metadata load");
    return loading.metadataTable();
  }

  /** Issues authority only for this stock table conversion and its current owned observation. */
  @Override
  protected synchronized Table createSparkTable(
      Identifier id,
      org.apache.gravitino.rel.Table metadata,
      Table nativeTable,
      TableCatalog nativeCatalog,
      PropertiesConverter converter,
      SparkTransformConverter transforms,
      SparkTypeConverter types) {
    if (loading == null
        || nativeTable.getClass() != JDBCTable.class
        || metadata != loading.metadataTable())
      throw new SecurityException("Unbound registered JDBC table conversion");
    loading.requireNative((JDBCTable) nativeTable);
    Table created =
        super.createSparkTable(
            id, metadata, nativeTable, nativeCatalog, converter, transforms, types);
    sourceBindings()
        .issue((SparkJdbcTable) created, (JDBCTable) nativeTable, loading, session, engine);
    return created;
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] transforms, Map<String, String> properties) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public boolean dropTable(Identifier id) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public void renameTable(Identifier oldId, Identifier newId) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw readOnly();
  }

  /** Rejects JDBC writes; this wrapper qualifies source extraction only. */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    throw readOnly();
  }

  /** Closes only this wrapper's source bindings, owned metadata client and policy lease. */
  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    if (bindings != null) bindings.close();
    try {
      state.metadata.close();
    } finally {
      try {
        if (engine != null) engine.close();
      } finally {
        if (context != null && shutdown != null) context.removeSparkListener(shutdown);
      }
    }
  }

  synchronized DeltaRangerJdbcSourceBindings sourceBindings() {
    if (closed || bindings == null)
      throw new SecurityException("Registered JDBC catalog is not live");
    return bindings;
  }

  static final class State {
    final DeltaRangerMetadataClient metadata;
    @Nullable final Supplier<Catalog> visibility;
    final Function<SparkConf, DeltaRangerPolicyEngine> engines;

    State() {
      this(
          new DeltaRangerMetadataClient(),
          null,
          conf ->
              DeltaRangerPolicyEngine.acquire(
                  DeltaRangerConfig.fromClasspath(
                      required(conf, DeltaRangerCatalogSpark35.PREFIX + "service"),
                      required(conf, DeltaRangerCatalogSpark35.PREFIX + "definition"),
                      required(conf, GravitinoSparkConfig.GRAVITINO_METALAKE))));
    }

    State(
        DeltaRangerMetadataClient metadata,
        @Nullable Supplier<Catalog> visibility,
        Function<SparkConf, DeltaRangerPolicyEngine> engines) {
      this.metadata = metadata;
      this.visibility = visibility;
      this.engines = engines;
    }
  }

  private void requireVisible() {
    if (closed || canonicalName == null)
      throw new SecurityException("Registered JDBC catalog is not live");
    Catalog visible =
        state.visibility == null
            ? GravitinoCatalogManager.get().getCatalogs().get(canonicalName)
            : state.visibility.get();
    require(
        visible != null
            && canonicalName.equals(visible.name())
            && "jdbc-postgresql".equals(visible.provider()),
        "PostgreSQL catalog is not application-visible under its canonical name");
  }

  private static String required(SparkConf conf, String key) {
    String value = conf.get(key, "");
    require(!value.trim().isEmpty(), "Missing trusted startup option " + key);
    return value;
  }

  private static void require(boolean condition, String message) {
    if (!condition) throw new StartupRejected(message);
  }

  private static final class StartupRejected extends IllegalArgumentException {
    private StartupRejected(String message) {
      super(message);
    }
  }

  private static SecurityException readOnly() {
    return new SecurityException("Registered JDBC supports source extraction only");
  }
}
