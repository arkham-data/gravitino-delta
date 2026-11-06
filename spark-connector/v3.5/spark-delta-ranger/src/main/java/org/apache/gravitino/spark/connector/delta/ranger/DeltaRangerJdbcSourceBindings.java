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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConverter;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable;
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTypeConverter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.LiteralValue;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTable;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.jdbc.PostgresDialect$;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Predef;
import scala.collection.JavaConverters;

/** Private authority for exact PostgreSQL tables issued by the companion catalog. */
final class DeltaRangerJdbcSourceBindings {
  private static final Set<String> PROFILE =
      new HashSet<>(Arrays.asList("url", "driver", "user", "password", "dbtable"));

  private final DeltaRangerMetadataClient metadata;
  private final String metalake;
  private final String catalog;
  private final String service;
  private final String definition;
  private final TableKindObserver tableKinds;
  private final IdentityHashMap<SparkJdbcTable, BoundJdbc> issued = new IdentityHashMap<>();
  private volatile boolean closed;

  DeltaRangerJdbcSourceBindings(
      DeltaRangerMetadataClient metadata,
      String metalake,
      String catalog,
      String service,
      String definition) {
    this(
        metadata, metalake, catalog, service, definition, DeltaRangerJdbcSourceBindings::tableKind);
  }

  DeltaRangerJdbcSourceBindings(
      DeltaRangerMetadataClient metadata,
      String metalake,
      String catalog,
      String service,
      String definition,
      TableKindObserver tableKinds) {
    this.tableKinds = Objects.requireNonNull(tableKinds);
    this.metadata = Objects.requireNonNull(metadata);
    this.metalake = DeltaRangerResourceKey.literal(metalake);
    this.catalog = DeltaRangerResourceKey.literal(catalog);
    this.service = DeltaRangerResourceKey.literal(service);
    this.definition = DeltaRangerResourceKey.literal(definition);
  }

  Observation read(Identifier id) {
    try {
      require(!closed && id.namespace().length == 1);
      String schema = DeltaRangerResourceKey.literal(id.namespace()[0]);
      String table = DeltaRangerResourceKey.literal(id.name());
      return metadata.read(
          current -> {
            require(
                !closed
                    && catalog.equals(current.name())
                    && "jdbc-postgresql".equals(current.provider()));
            Map<String, String> properties = new HashMap<>(current.properties());
            properties.putAll(current.supportsSecrets().getSecrets());
            require(
                properties.keySet().stream()
                    .noneMatch(key -> key.toLowerCase(Locale.ROOT).startsWith("spark.bypass.")));
            require("ranger-delta".equals(properties.get("authorization-provider")));
            require(
                catalog.equals(properties.get("authorization.ranger.delta.catalog"))
                    && metalake.equals(properties.get("authorization.ranger.delta.metalake")));
            require(
                service.equals(properties.get("authorization.ranger.service.name"))
                    && definition.equals(properties.get("authorization.ranger.service.type")));
            Map<String, String> options =
                JdbcPropertiesConverter.getNoTablePropertiesInstance()
                    .toSparkCatalogProperties(properties);
            String credentialOwner = "catalog-properties";
            Credential[] credentials;
            try {
              credentials = current.supportsCredentials().getCredentials();
            } catch (UnsupportedOperationException unsupported) {
              credentials = new Credential[0];
            }
            for (Credential credential : credentials) {
              if (credential instanceof JdbcCredential) {
                require(credential.getClass() == JdbcCredential.class);
                JdbcCredential jdbc = (JdbcCredential) credential;
                credentialOwner = "catalog-typed-jdbc";
                options.put("user", jdbc.jdbcUser());
                options.put("password", jdbc.jdbcPassword());
                break;
              }
            }
            options.put("dbtable", "\"" + schema + "\".\"" + table + "\"");
            validateProfile(options, properties.get("jdbc-database"), id);
            String tableKind = tableKinds.observe(options, properties.get("jdbc-database"), id);
            require("TABLE".equals(tableKind));
            Table loaded = current.asTableCatalog().loadTable(NameIdentifier.of(schema, table));
            require(loaded != null && table.equals(loaded.name()));
            Column[] columns = loaded.columns();
            require(columns != null && columns.length > 0);
            StructField[] fields = new StructField[columns.length];
            SparkJdbcTypeConverter converter = new SparkJdbcTypeConverter();
            Set<String> names = new HashSet<>();
            for (int i = 0; i < columns.length; i++) {
              Column column = columns[i];
              String name = DeltaRangerResourceKey.literal(column.name());
              require(names.add(name));
              fields[i] =
                  DataTypes.createStructField(
                      name, converter.toSparkType(column.dataType()), column.nullable());
            }
            return new Observation(
                this,
                metadata.leaseStamp(),
                id,
                loaded,
                DataTypes.createStructType(fields),
                properties,
                options,
                credentialOwner,
                tableKind);
          });
    } catch (RuntimeException | LinkageError failure) {
      throw denied();
    }
  }

  void requireLive(Observation observation) {
    try {
      require(!closed && observation != null && observation.owner == this);
      metadata.requireLive(observation.lease);
    } catch (RuntimeException failure) {
      throw denied();
    }
  }

  synchronized void close() {
    closed = true;
    issued.clear();
  }

  synchronized void issue(
      SparkJdbcTable table,
      JDBCTable nativeTable,
      Observation before,
      SparkSession session,
      DeltaRangerPolicyEngine engine) {
    requireLive(before);
    require(
        table != null && table.getClass() == SparkJdbcTable.class && !issued.containsKey(table));
    before.requireSame(read(before.identifier));
    BoundJdbc bound = new BoundJdbc(this, table, nativeTable, before, session, engine);
    bound.requireRetained();
    issued.put(table, bound);
  }

  synchronized BoundJdbc resolveRetained(
      SparkJdbcTable table, Identifier id, SparkSession session) {
    require(table != null && table.getClass() == SparkJdbcTable.class);
    BoundJdbc binding = issued.get(table);
    require(binding != null && binding.observation.identifier.equals(id));
    binding.requireIdentity(session);
    return binding;
  }

  synchronized BoundJdbc resolve(SparkJdbcTable table, Identifier id, SparkSession session) {
    BoundJdbc binding = resolveRetained(table, id, session);
    binding.validateCurrent(session);
    return binding;
  }

  static final class BoundJdbc {
    private final DeltaRangerJdbcSourceBindings owner;
    private final SparkJdbcTable table;
    private final Observation observation;
    private final JDBCTable nativeTable;
    private final String nativeSchemaJson;
    private final String wrappedSchemaJson;
    private final SparkSession session;
    private final DeltaRangerPolicyEngine engine;
    private final DeltaRangerQuerySubject subject;
    private final scala.collection.immutable.Map<String, String> configuration;

    private BoundJdbc(
        DeltaRangerJdbcSourceBindings owner,
        SparkJdbcTable table,
        JDBCTable nativeTable,
        Observation observation,
        SparkSession session,
        DeltaRangerPolicyEngine engine) {
      this.owner = owner;
      this.table = table;
      require(nativeTable.getClass() == JDBCTable.class);
      observation.requireNative(nativeTable);
      this.nativeTable = nativeTable;
      this.nativeSchemaJson = nativeTable.schema().json();
      this.wrappedSchemaJson = table.schema().json();
      this.observation = observation;
      this.session = session;
      this.engine = engine;
      this.subject = DeltaRangerQuerySubject.current(session);
      this.configuration = session.conf().getAll();
    }

    void requireLive() {
      owner.requireLive(observation);
    }

    void requireRetained() {
      requireLive();
      require(table.getClass() == SparkJdbcTable.class);
      observation.requireOptions(table);
      require(
          nativeSchemaJson.equals(nativeTable.schema().json())
              && wrappedSchemaJson.equals(table.schema().json()));
      observation.requireNative(nativeTable);
      observation.requireWrapped(table.schema());
    }

    private void requireIdentity(SparkSession candidate) {
      require(candidate == session, "session changed");
      requireRetained();
      DeltaRangerQuerySubject current = DeltaRangerQuerySubject.current(session);
      require(
          current.user().equals(subject.user()) && current.groups().equals(subject.groups()),
          "subject changed");
    }

    Observation validateCurrent(SparkSession candidate) {
      require(configuration.equals(session.conf().getAll()), "session configuration changed");
      Observation current = validateMetadata(candidate);
      require(configuration.equals(session.conf().getAll()), "session configuration changed");
      return current;
    }

    // The postcommit cache owner brackets this metadata check with its own complete context
    // equality, including the exact native published-version transition. This supplies no
    // execution or Delta storage authority and never changes the source's captured settings.
    Observation validateMetadata(SparkSession candidate) {
      try {
        requireIdentity(candidate);
        Observation observed = owner.read(observation.identifier);
        observation.requireSame(observed);
        requireIdentity(candidate);
        return observed;
      } catch (RuntimeException | LinkageError failure) {
        if (failure instanceof Rejected) throw (Rejected) failure;
        throw denied();
      }
    }

    void requireOptions(JDBCOptions options) {
      requireRetained();
      require(options.getClass() == JDBCOptions.class);
      require(
          observation.options.equals(
              JavaConverters.mapAsJavaMapConverter(options.parameters().originalMap()).asJava()));
    }

    void recheckPublication() {
      validateCurrent(session);
    }

    DeltaRangerPolicyEngine engine() {
      requireLive();
      return engine;
    }

    String catalog() {
      return owner.catalog;
    }

    Identifier identifier() {
      return Identifier.of(
          observation.identifier.namespace().clone(), observation.identifier.name());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "Issued registered JDBC source";
    }
  }

  static final class Observation {
    private final DeltaRangerJdbcSourceBindings owner;
    private final DeltaRangerMetadataClient.MetadataLeaseStamp lease;
    private final Identifier identifier;
    private final Table metadataTable;
    private final StructType schema;
    private final Map<String, String> properties;
    private final Map<String, String> options;
    private final Map<String, String> tableProperties;
    private final String credentialOwner;
    private final String tableKind;

    private Observation(
        DeltaRangerJdbcSourceBindings owner,
        DeltaRangerMetadataClient.MetadataLeaseStamp lease,
        Identifier id,
        Table table,
        StructType schema,
        Map<String, String> properties,
        Map<String, String> options,
        String credentialOwner,
        String tableKind) {
      this.tableKind = tableKind;
      this.owner = owner;
      this.lease = lease;
      this.identifier = Identifier.of(id.namespace().clone(), id.name());
      this.metadataTable = table;
      this.schema = schema;
      this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
      this.options = Collections.unmodifiableMap(new HashMap<>(options));
      this.tableProperties = Collections.unmodifiableMap(new HashMap<>(table.properties()));
      this.credentialOwner = credentialOwner;
    }

    void requireNative(JDBCTable table) {
      requireOptions(table);
      StructField[] expected = schema.fields();
      StructField[] actual = table.schema().fields();
      require(expected.length == actual.length);
      // Stock JDBCRDD schema discovery calls JdbcUtils.getSchema(alwaysNullable=true).
      // Its PostgreSQL isSigned/scale metadata belongs to the separately retained native schema;
      // SparkJdbcTable exposes the Gravitino schema, including its original nullability.
      for (int i = 0; i < expected.length; i++) {
        require(
            expected[i].name().equals(actual[i].name())
                && expected[i].dataType().equals(actual[i].dataType())
                && actual[i].nullable());
      }
    }

    void requireWrapped(StructType actual) {
      StructField[] expected = schema.fields();
      StructField[] fields = actual.fields();
      require(expected.length == fields.length);
      for (int i = 0; i < expected.length; i++) {
        require(
            expected[i].name().equals(fields[i].name())
                && expected[i].dataType().equals(fields[i].dataType())
                && expected[i].nullable() == fields[i].nullable());
      }
    }

    void requireOptions(JDBCTable table) {
      require(
          options.equals(
              JavaConverters.mapAsJavaMapConverter(table.jdbcOptions().parameters().originalMap())
                  .asJava()));
    }

    Table metadataTable() {
      owner.requireLive(this);
      return metadataTable;
    }

    void requireSame(Observation other) {
      owner.requireLive(this);
      require(other != null && other.owner == owner);
      owner.requireLive(other);
      require(
          identifier.equals(other.identifier)
              && schema.equals(other.schema)
              && properties.equals(other.properties)
              && options.equals(other.options)
              && tableProperties.equals(other.tableProperties)
              && credentialOwner.equals(other.credentialOwner)
              && tableKind.equals(other.tableKind));
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "Registered JDBC metadata observation";
    }
  }

  // Java can name these public JVM classes despite their Scala package-private source visibility.
  static Class<?> nativeRelationClass() {
    return JDBCRelation.class;
  }

  static Class<?> fieldReferenceClass() {
    return FieldReference.class;
  }

  static Class<?> literalValueClass() {
    return LiteralValue.class;
  }

  interface TableKindObserver {
    String observe(Map<String, String> options, String database, Identifier identifier);
  }

  private static String tableKind(Map<String, String> options, String database, Identifier id) {
    try {
      // Use only the validated, packaged stock driver and the existing vended credential.
      // This exact-object metadata lookup does not construct or execute payload SQL.
      JDBCOptions jdbc =
          new JDBCOptions(
              JavaConverters.mapAsScalaMapConverter(options).asScala().toMap(Predef.$conforms()));
      JdbcDialect dialect = JdbcDialects.get(jdbc.url());
      require(dialect.getClass() == PostgresDialect$.class);
      // Spark's factory uses DriverRegistry and the Spark user/context classloader.
      try (Connection connection = dialect.createConnectionFactory(jdbc).apply(-1)) {
        require(connection != null);
        connection.setReadOnly(true);
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        require(escape != null && !escape.isEmpty());
        String schema = id.namespace()[0];
        String table = id.name();
        try (ResultSet rows =
            metadata.getTables(
                database, literalPattern(schema, escape), literalPattern(table, escape), null)) {
          int count = 0;
          String kind = null;
          while (rows.next()) {
            require(
                database.equals(rows.getString("TABLE_CAT"))
                    && schema.equals(rows.getString("TABLE_SCHEM"))
                    && table.equals(rows.getString("TABLE_NAME")));
            kind = rows.getString("TABLE_TYPE");
            require(++count == 1 && "TABLE".equals(kind));
          }
          require(count == 1);
          return kind;
        }
      }
    } catch (Exception | LinkageError failure) {
      throw denied();
    }
  }

  private static String literalPattern(String value, String escape) {
    return value
        .replace(escape, escape + escape)
        .replace("_", escape + "_")
        .replace("%", escape + "%");
  }

  static void validateCatalogProperties(Map<String, String> properties) {
    require(
        properties != null
            && properties.keySet().stream()
                .noneMatch(key -> key.toLowerCase(Locale.ROOT).startsWith("spark.bypass.")));
    Map<String, String> options =
        JdbcPropertiesConverter.getNoTablePropertiesInstance().toSparkCatalogProperties(properties);
    // Credentials are vended by the unchanged native initializer and compared at table issuance.
    // Validate the endpoint/driver/options before that initializer can load any driver callbacks.
    options.put("user", "profile-validation");
    options.put("password", "");
    options.put("dbtable", "\"public\".\"profile_validation\"");
    validateProfile(
        options,
        properties.get("jdbc-database"),
        Identifier.of(new String[] {"public"}, "profile_validation"));
  }

  static void validateProfile(Map<String, String> options, String database, Identifier id) {
    require(options != null && options.keySet().equals(PROFILE));
    require(id != null && id.namespace().length == 1);
    String schema = DeltaRangerResourceKey.literal(id.namespace()[0]);
    String table = DeltaRangerResourceKey.literal(id.name());
    require(("\"" + schema + "\".\"" + table + "\"").equals(options.get("dbtable")));
    require("org.postgresql.Driver".equals(options.get("driver")));
    require(options.get("user") != null && !options.get("user").isEmpty());
    require(options.get("password") != null);
    require(database != null && !database.isEmpty());
    String url = options.get("url");
    require(url != null && url.startsWith("jdbc:postgresql://"));
    try {
      URI endpoint = new URI(url.substring("jdbc:".length()));
      require(
          endpoint.getHost() != null
              && endpoint.getUserInfo() == null
              && endpoint.getQuery() == null
              && endpoint.getFragment() == null);
      require(("/" + database).equals(endpoint.getRawPath()));
      require(endpoint.getPort() == -1 || (endpoint.getPort() > 0 && endpoint.getPort() <= 65535));
    } catch (URISyntaxException invalid) {
      throw denied();
    }
  }

  private static void require(boolean condition) {
    if (!condition) throw denied();
  }

  private static void require(boolean condition, String reason) {
    if (!condition) throw new Rejected(reason);
  }

  private static final class Rejected extends SecurityException {
    private Rejected(String reason) {
      super("Registered JDBC source rejected: " + reason);
    }
  }

  private static SecurityException denied() {
    return new Rejected("unqualified extraction profile");
  }
}
