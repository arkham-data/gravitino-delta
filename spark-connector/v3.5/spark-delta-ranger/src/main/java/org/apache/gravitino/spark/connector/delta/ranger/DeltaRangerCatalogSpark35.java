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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.delta.DeltaCreateLifecycle;
import org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerMetadataClient.MetadataLeaseStamp;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.TableSpec;
import org.apache.spark.sql.connector.catalog.CatalogV2Util;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;

/** Optional native catalog with separate application, current actor, and metadata identities. */
public final class DeltaRangerCatalogSpark35 extends GravitinoDeltaCatalogSpark35 {
  static final String PREFIX = "spark.gravitino.deltaRanger.";
  static final String EXTENSION =
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension";
  private static final ThreadLocal<CreateInvocation> CREATE = new ThreadLocal<>();
  private static final ThreadLocal<CdfAlterInvocation> CDF_ALTER = new ThreadLocal<>();
  private final State state;
  @Nullable private DeltaRangerPolicyEngine engine;
  @Nullable private DeltaRangerRegistrationBindings bindings;
  @Nullable private Path mergeJournalDirectory;
  @Nullable private DeltaRangerRawMaintenanceDeployment rawMaintenance;
  @Nullable private SparkContext context;
  @Nullable private SparkListener shutdown;
  private boolean initialized;
  private boolean closed;

  /** Creates the Spark-loadable wrapper without reading metadata or initializing Ranger. */
  public DeltaRangerCatalogSpark35() {
    this(new State());
  }

  DeltaRangerCatalogSpark35(State state) {
    super(state.metadata, state::actor);
    this.state = state;
  }

  Path mergeJournalDirectory() {
    if (closed || mergeJournalDirectory == null)
      throw new IllegalStateException("Delta journal unavailable");
    return mergeJournalDirectory;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void initialize(String name, CaseInsensitiveStringMap options) {
    if (closed || initialized)
      throw new IllegalStateException("Delta Ranger catalog is closed or initialized");
    initialized = true;
    try {
      SparkSession session = SparkSession.active();
      SparkConf conf = session.sparkContext().getConf();
      require(
          conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT, false)
              && conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_SUPPORT, false),
          "Delta Ranger catalog requires both startup opt-ins");
      // Match the effective SIMPLE default and case handling of the common client factory.
      require(
          "simple".equalsIgnoreCase(conf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "simple")),
          "Delta Ranger requires SIMPLE Gravitino application authentication; other application auth modes are not qualified");
      Set<String> extensions =
          new HashSet<>(Arrays.asList(session.conf().get("spark.sql.extensions", "").split(",")));
      require(
          extensions.contains(EXTENSION),
          "Install DeltaRangerSparkExtension before creating the session");
      state.catalog = DeltaRangerResourceKey.literal(name);
      Catalog visible = state.visible();
      require(
          visible != null
              && name.equals(visible.name())
              && "lakehouse-delta".equals(visible.provider()),
          "Delta catalog is not application-visible under its canonical name; grant local USE_CATALOG visibility");
      MetadataIdentity metadata =
          MetadataIdentity.read(Paths.get(required(conf, PREFIX + "metadata.config")));
      require(
          !metadata.principal.equals(session.sparkContext().sparkUser()),
          "Configure a distinct metadata principal, not the application identity");
      try {
        require(
            !metadata.principal.equals(UserGroupInformation.getCurrentUser().getShortUserName()),
            "Configure a distinct metadata principal, not the current actor");
      } catch (IOException failure) {
        throw new IllegalArgumentException("Cannot establish the current actor");
      }
      String metalake =
          DeltaRangerResourceKey.literal(required(conf, GravitinoSparkConfig.GRAVITINO_METALAKE));
      state.metadata.configure(
          required(conf, GravitinoSparkConfig.GRAVITINO_URI),
          metalake,
          name,
          metadata.lifetime,
          builder -> builder.withSimpleAuth(metadata.principal));
      engine = state.engines.apply(conf);
      super.initialize(name, options);
      mergeJournalDirectory =
          Paths.get(options.get(JOURNAL_DIRECTORY)).toAbsolutePath().normalize();
      DeltaRangerRegistrationReader reader =
          new DeltaRangerRegistrationReader(state.metadata, metalake, name);
      bindings = new DeltaRangerRegistrationBindings(reader, engine);
      rawMaintenance =
          DeltaRangerRawMaintenanceDeployment.install(
              this, session, metalake, name, reader, engine);
      context = session.sparkContext();
      shutdown =
          new SparkListener() {
            /** {@inheritDoc} */
            @Override
            public void onApplicationEnd(SparkListenerApplicationEnd event) {
              DeltaRangerCatalogSpark35.this.close();
            }
          };
      context.addSparkListener(shutdown);
    } catch (RuntimeException | LinkageError failure) {
      close();
      throw failure;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id) {
    return registrationBindings().load(id, () -> super.loadTable(id));
  }

  // Only the normal application catalog can establish table visibility or typed absence.
  Optional<DeltaRegistrationReceipt> visibleForMetadata(Identifier id) {
    if (closed || id.namespace().length != 1) {
      throw new SecurityException("Invalid metadata catalog or namespace");
    }
    String database = DeltaRangerResourceKey.literal(id.namespace()[0]);
    String tableName = DeltaRangerResourceKey.literal(id.name());
    Catalog actor = state.actor();
    if (actor == null
        || !name().equals(actor.name())
        || !"lakehouse-delta".equals(actor.provider())) {
      throw new SecurityException("Invalid application metadata catalog");
    }
    TableCatalog tables = actor.asTableCatalog();
    org.apache.gravitino.rel.Table table;
    try {
      table = tables.loadTable(NameIdentifier.of(database, tableName));
    } catch (NoSuchTableException absent) {
      return Optional.empty();
    }
    if (table == null || !tableName.equals(table.name())) {
      throw new SecurityException("Invalid application table metadata");
    }
    return Optional.of(DeltaRegistrationReceipt.fromProperties(table.properties()));
  }

  MetadataLeaseStamp metadataLease() {
    if (closed) throw new SecurityException("Metadata catalog is closed");
    // Start/check the owned lease only after normal visibility, including authorized absence.
    return state.metadata.read(
        metadata -> {
          if (!name().equals(metadata.name()) || !"lakehouse-delta".equals(metadata.provider())) {
            throw new SecurityException("Invalid metadata catalog");
          }
          return state.metadata.leaseStamp();
        });
  }

  void requireMetadataLive(MetadataLeaseStamp lease) {
    if (closed) throw new SecurityException("Metadata catalog is closed");
    state.metadata.requireLive(lease);
  }

  Optional<DeltaRegistrationReceipt> publishedForCreate(Identifier id) {
    if (id.namespace().length != 1) throw new SecurityException("Invalid CREATE namespace");
    String database = DeltaRangerResourceKey.literal(id.namespace()[0]);
    String tableName = DeltaRangerResourceKey.literal(id.name());
    return state.metadata.read(
        metadata -> {
          if (closed
              || !name().equals(metadata.name())
              || !"lakehouse-delta".equals(metadata.provider())) {
            throw new SecurityException("Invalid CREATE catalog metadata");
          }
          TableCatalog tables = metadata.asTableCatalog();
          org.apache.gravitino.rel.Table table;
          try {
            table = tables.loadTable(NameIdentifier.of(database, tableName));
          } catch (NoSuchTableException absent) {
            return Optional.empty();
          }
          if (table == null || !tableName.equals(table.name())) {
            throw new SecurityException("Invalid CREATE table metadata");
          }
          DeltaRegistrationReceipt receipt =
              DeltaRegistrationReceipt.fromProperties(table.properties());
          if (receipt.state() != DeltaRegistrationReceipt.State.PUBLISHED
              || !receipt.location().isPresent()
              || !receipt.nativeId().isPresent()
              || !receipt.nativeVersion().isPresent()
              || receipt.nativeVersion().getAsLong() < 0) {
            throw new SecurityException("CREATE requires positively published metadata");
          }
          return Optional.of(receipt);
        });
  }

  void requireCreateMetadataLive() {
    // This catalog owns one nonrenewable lease; it cannot be rebound after the observation.
    if (closed) throw new SecurityException("CREATE catalog is closed");
    state.metadata.requireLive(state.metadata.leaseStamp());
  }

  interface CreateInvocation {
    /**
     * Returns the privately issued lifecycle after validating the immutable catalog declaration.
     *
     * @param catalog exact invoking wrapper
     * @param intent base-owned immutable declaration
     * @return the single issued lifecycle
     */
    DeltaCreateLifecycle lifecycle(
        DeltaRangerCatalogSpark35 catalog, DeltaCreateLifecycle.Intent intent);
  }

  static void withCreate(CreateInvocation invocation, Runnable body) {
    if (CREATE.get() != null) throw new SecurityException("Nested CREATE command");
    CREATE.set(invocation);
    try {
      body.run();
    } finally {
      CREATE.remove();
    }
  }

  static Map<String, String> convertCreateProperties(TableSpec specification) {
    return JavaConverters.mapAsJavaMapConverter(CatalogV2Util.convertTableProperties(specification))
        .asJava();
  }

  /** Accepts only the exact declaration carried by the privately issued CREATE invocation. */
  @Override
  protected DeltaCreateLifecycle newCreateLifecycle(DeltaCreateLifecycle.Intent intent) {
    CreateInvocation invocation = CREATE.get();
    if (invocation == null) throw new SecurityException("Unbound CREATE command");
    return invocation.lifecycle(this, intent);
  }

  /** Rejects replacement outside the qualified initial schema CREATE profile. */
  @Override
  public StagedTable stageReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    throw new SecurityException("Delta Ranger replacement is not qualified");
  }

  /** Rejects replacement outside the qualified initial schema CREATE profile. */
  @Override
  public StagedTable stageCreateOrReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    throw new SecurityException("Delta Ranger replacement is not qualified");
  }

  interface CdfAlterInvocation {
    void validate(DeltaRangerCatalogSpark35 catalog, Identifier id, TableChange[] changes);

    void execute(String location);

    boolean committed();
  }

  /** Reports an unprovable current outcome without discarding any earlier publication evidence. */
  static final class CdfAlterOutcomeUncertain extends IllegalStateException {
    CdfAlterOutcomeUncertain(Throwable cause) {
      super(
          "CDF property change outcome uncertain; do not retry ALTER; recover with a fresh named observation",
          cause);
    }
  }

  static Table withCdfAlter(CdfAlterInvocation invocation, Supplier<Table> body) {
    if (CDF_ALTER.get() != null) throw new SecurityException("Nested CDF property command");
    CDF_ALTER.set(invocation);
    try {
      return body.get();
    } finally {
      CDF_ALTER.remove();
    }
  }

  /** Requires an issued exact named CDF property command before native ALTER. */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    CdfAlterInvocation invocation = CDF_ALTER.get();
    if (invocation == null) throw new SecurityException("Unbound CDF property command");
    invocation.validate(this, id, changes);
    try {
      return super.alterTable(id, changes);
    } catch (RuntimeException failure) {
      if (failure instanceof CdfAlterOutcomeUncertain) throw failure;
      if (invocation.committed()) {
        throw new IllegalStateException(
            "CDF property change committed; named refresh is unconfirmed; do not retry ALTER",
            failure);
      }
      throw failure;
    }
  }

  /** Executes only the issued native CDF command at the validated registered location. */
  @Override
  protected void executeNativeAlter(Identifier id, String location, TableChange... changes) {
    CdfAlterInvocation invocation = CDF_ALTER.get();
    if (invocation == null) throw new SecurityException("Unbound CDF property command");
    invocation.validate(this, id, changes);
    invocation.execute(location);
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id, String version) {
    throw new SecurityException("Delta Ranger time travel is not qualified");
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id, long timestamp) {
    throw new SecurityException("Delta Ranger time travel is not qualified");
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    if (rawMaintenance != null) rawMaintenance.close();
    if (bindings != null) bindings.close();
    try {
      state.metadata.close();
    } finally {
      try {
        if (engine != null) engine.close();
      } finally {
        if (context != null && shutdown != null) context.removeSparkListener(shutdown);
        super.close();
      }
    }
  }

  synchronized DeltaRangerRegistrationBindings registrationBindings() {
    if (closed || bindings == null) throw new SecurityException("Delta Ranger catalog is not live");
    return bindings;
  }

  synchronized DeltaRangerPolicyEngine policyEngine() {
    if (closed || engine == null)
      throw new IllegalStateException("Delta Ranger catalog is not initialized");
    return engine;
  }

  static final class State {
    final DeltaRangerMetadataClient metadata;
    final Function<SparkConf, DeltaRangerPolicyEngine> engines;
    @Nullable final Supplier<Catalog> actors;
    @Nullable final Supplier<Catalog> visibility;
    @Nullable String catalog;

    State() {
      this(
          new DeltaRangerMetadataClient(),
          null,
          null,
          conf ->
              DeltaRangerPolicyEngine.acquire(
                  DeltaRangerConfig.fromClasspath(
                      required(conf, PREFIX + "service"),
                      required(conf, PREFIX + "definition"),
                      required(conf, GravitinoSparkConfig.GRAVITINO_METALAKE))));
    }

    State(
        DeltaRangerMetadataClient metadata,
        @Nullable Supplier<Catalog> actors,
        @Nullable Supplier<Catalog> visibility,
        Function<SparkConf, DeltaRangerPolicyEngine> engines) {
      this.metadata = metadata;
      this.actors = actors;
      this.visibility = visibility;
      this.engines = engines;
    }

    @Nullable
    Catalog visible() {
      if (catalog == null) throw new IllegalStateException("Canonical catalog is not bound");
      return visibility == null
          ? GravitinoCatalogManager.get().getCatalogs().get(catalog)
          : visibility.get();
    }

    Catalog actor() {
      if (catalog == null) throw new IllegalStateException("Canonical catalog is not bound");
      return actors == null
          ? GravitinoCatalogManager.get().getGravitinoCatalogInfo(catalog)
          : actors.get();
    }
  }

  static final class MetadataIdentity {
    final String principal;
    final Duration lifetime;

    private MetadataIdentity(String principal, Duration lifetime) {
      this.principal = principal;
      this.lifetime = lifetime;
    }

    static MetadataIdentity read(Path path) {
      try {
        require(
            path.isAbsolute() && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS),
            "Private metadata configuration must be an absolute regular file");
        Set<PosixFilePermission> permissions =
            Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS);
        require(
            permissions.stream()
                .noneMatch(p -> p.name().startsWith("GROUP_") || p.name().startsWith("OTHERS_")),
            "Metadata configuration must be owner-only");
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(path)) {
          properties.load(stream);
        }
        require(
            properties
                .stringPropertyNames()
                .equals(new HashSet<>(Arrays.asList("auth-type", "principal", "lifetime-seconds"))),
            "Metadata configuration requires only auth-type, principal and lifetime-seconds");
        require(
            "simple".equals(properties.getProperty("auth-type")),
            "Only explicit SIMPLE metadata authentication is qualified");
        String principal = DeltaRangerResourceKey.literal(properties.getProperty("principal"));
        long seconds = Long.parseLong(properties.getProperty("lifetime-seconds"));
        require(
            seconds > 0 && seconds <= 86400,
            "Metadata lifetime must be between 1 and 86400 seconds");
        return new MetadataIdentity(principal, Duration.ofSeconds(seconds));
      } catch (IOException | NumberFormatException failure) {
        throw new IllegalArgumentException("Invalid private metadata configuration");
      }
    }
  }

  private static String required(SparkConf conf, String key) {
    String value = conf.get(key, "");
    require(!value.trim().isEmpty(), "Missing driver startup option " + key);
    return value;
  }

  private static void require(boolean value, String message) {
    if (!value) throw new IllegalArgumentException(message);
  }
}
