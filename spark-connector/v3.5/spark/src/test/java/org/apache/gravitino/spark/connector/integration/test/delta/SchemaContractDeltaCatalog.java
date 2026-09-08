/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information.
 * Licensed under the Apache License, Version 2.0.
 */
package org.apache.gravitino.spark.connector.integration.test.delta;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Serverless test-only catalog that binds schema markers before native Delta staging. */
@SuppressWarnings("deprecation")
public final class SchemaContractDeltaCatalog implements StagingTableCatalog, SupportsNamespaces {
  private static final String[] NAMESPACE = {"db"};
  private static final String MAPPING_MODE = "delta.columnMapping.mode";
  private static final String DEFAULT_MAPPING_MODE =
      "spark.databricks.delta.properties.defaults.columnMapping.mode";
  private static final String MAX_COLUMN_ID = "delta.columnMapping.maxColumnId";
  private static final String FORCE_V1 = "gravitino.test.forceV1";
  private static final Set<TableCapability> CREATE_CAPABILITIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(TableCapability.BATCH_WRITE, TableCapability.V1_BATCH_WRITE)));
  private static final Set<TableCapability> REPLACEMENT_CAPABILITIES =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  TableCapability.BATCH_WRITE,
                  TableCapability.V1_BATCH_WRITE,
                  TableCapability.TRUNCATE)));

  private final AtomicLong identities = new AtomicLong(1);
  private final Map<String, String> locations = new LinkedHashMap<>();
  private final Map<String, OperationRecord> records = new LinkedHashMap<>();
  private DeltaCatalog nativeCatalog;
  private String catalogName;
  private String baseLocation;

  /** Creates the Spark catalog plugin. */
  public SchemaContractDeltaCatalog() {}

  /** {@inheritDoc} */
  @Override
  public synchronized void initialize(String name, CaseInsensitiveStringMap options) {
    catalogName = name;
    baseLocation = options.get("base");
    if (baseLocation == null || baseLocation.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing serverless test base location");
    }
    nativeCatalog = new DeltaCatalog();
    PathOnlyDelegate pathDelegate = new PathOnlyDelegate();
    pathDelegate.initialize(name, options);
    nativeCatalog.setDelegateCatalog(pathDelegate);
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return catalogName;
  }

  /** Returns an immutable snapshot of marker provenance recorded before native execution. */
  public synchronized Map<String, OperationRecord> records() {
    return Collections.unmodifiableMap(new LinkedHashMap<>(records));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized Table loadTable(Identifier id) throws NoSuchTableException {
    String location = locations.get(key(id));
    if (location == null) throw new NoSuchTableException(id);
    return nativeCatalog.loadTable(pathIdentifier(location));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean tableExists(Identifier id) {
    return locations.containsKey(key(id));
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateTable(Identifier id) {}

  /** {@inheritDoc} */
  @Override
  public synchronized Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    checkNamespace(namespace);
    return locations.keySet().stream()
        .map(value -> Identifier.of(NAMESPACE, value.substring(value.indexOf('.') + 1)))
        .toArray(Identifier[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    StagedTable staged = stageCreate(id, schema, partitions, properties);
    staged.commitStagedChanges();
    return nativeCatalog.loadTable(pathIdentifier(locations.get(key(id))));
  }

  /** {@inheritDoc} */
  @Override
  public synchronized StagedTable stageCreate(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    checkNamespace(id.namespace());
    if (tableExists(id)) throw new TableAlreadyExistsException(id);
    validateProperties(properties);
    return new DeferredStage(id, schema, partitions, properties, RequestedOperation.CREATE, false);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized StagedTable stageReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    checkNamespace(id.namespace());
    if (!tableExists(id)) throw new NoSuchTableException(id);
    validateProperties(properties);
    return new DeferredStage(id, schema, partitions, properties, RequestedOperation.REPLACE, true);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized StagedTable stageCreateOrReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    checkNamespace(id.namespace());
    validateProperties(properties);
    return new DeferredStage(
        id, schema, partitions, properties, RequestedOperation.CREATE_OR_REPLACE, tableExists(id));
  }

  /** {@inheritDoc} */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    throw new UnsupportedOperationException("ALTER is outside the schema qualification fixture");
  }

  /** {@inheritDoc} */
  @Override
  public synchronized boolean dropTable(Identifier id) {
    records.remove(key(id));
    return locations.remove(key(id)) != null;
  }

  /** {@inheritDoc} */
  @Override
  public void renameTable(Identifier oldId, Identifier newId) {
    throw new UnsupportedOperationException("RENAME is outside the schema qualification fixture");
  }

  /** {@inheritDoc} */
  @Override
  public boolean namespaceExists(String[] namespace) {
    return Arrays.equals(NAMESPACE, namespace);
  }

  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces() {
    return new String[][] {NAMESPACE.clone()};
  }

  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    checkNamespace(namespace);
    return new String[0][];
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    checkNamespace(namespace);
    return Collections.singletonMap("location", new Path(baseLocation, "db").toString());
  }

  /** {@inheritDoc} */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException("The fixture owns exactly one namespace");
  }

  /** {@inheritDoc} */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw new UnsupportedOperationException("The fixture namespace is immutable");
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    throw new UnsupportedOperationException("The fixture namespace is immutable");
  }

  private void checkNamespace(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) throw new NoSuchNamespaceException(namespace);
  }

  private static String key(Identifier id) {
    return String.join(".", id.namespace()) + "." + id.name();
  }

  private static Identifier pathIdentifier(String location) {
    return Identifier.of(new String[] {"delta"}, location);
  }

  private static void validateProperties(Map<String, String> properties) {
    properties.keySet().stream()
        .filter(
            key ->
                MAX_COLUMN_ID.equalsIgnoreCase(key)
                    || DeltaOperationMarker.PROPERTY_KEY.equalsIgnoreCase(key))
        .findFirst()
        .ifPresent(
            key -> {
              throw new IllegalArgumentException("Reserved native property: " + key);
            });
  }

  private static void validateWriteOptions(CaseInsensitiveStringMap options) {
    if (options.containsKey("replaceWhere")
        || options.containsKey("mergeSchema")
        || options.containsKey("overwriteSchema")) {
      throw new IllegalArgumentException("Conflicting native schema write option");
    }
  }

  private final class DeferredStage implements StagedTable, SupportsWrite {
    private final Identifier id;
    private final StructType declaredSchema;
    private final Transform[] partitions;
    private final Map<String, String> properties;
    private final RequestedOperation requested;
    private final boolean replacing;
    private StagedTable nativeStage;
    private DeltaOperationMarker marker;

    private DeferredStage(
        Identifier id,
        StructType declaredSchema,
        Transform[] partitions,
        Map<String, String> properties,
        RequestedOperation requested,
        boolean replacing) {
      this.id = id;
      this.declaredSchema = declaredSchema;
      this.partitions = partitions.clone();
      this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
      this.requested = requested;
      this.replacing = replacing;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
      return id.toString();
    }

    /** {@inheritDoc} */
    @Override
    public StructType schema() {
      return nativeStage == null ? declaredSchema : nativeStage.schema();
    }

    /** {@inheritDoc} */
    @Override
    public Transform[] partitioning() {
      return nativeStage == null ? partitions.clone() : nativeStage.partitioning();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> properties() {
      return nativeStage == null ? properties : nativeStage.properties();
    }

    /** {@inheritDoc} */
    @Override
    public Set<TableCapability> capabilities() {
      return nativeStage == null
          ? requested == RequestedOperation.CREATE ? CREATE_CAPABILITIES : REPLACEMENT_CAPABILITIES
          : nativeStage.capabilities();
    }

    /** {@inheritDoc} */
    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      validateWriteOptions(info.options());
      bind(DeltaSchemaContract.OperationMode.QUERY, info.schema().asNullable());
      return ((SupportsWrite) nativeStage).newWriteBuilder(info);
    }

    /** {@inheritDoc} */
    @Override
    public void commitStagedChanges() {
      if (nativeStage == null) bind(DeltaSchemaContract.OperationMode.SCHEMA_ONLY, declaredSchema);
      nativeStage.commitStagedChanges();
      synchronized (SchemaContractDeltaCatalog.this) {
        locations.put(key(id), marker.normalizedLocation());
      }
    }

    /** {@inheritDoc} */
    @Override
    public void abortStagedChanges() {
      if (nativeStage != null) nativeStage.abortStagedChanges();
    }

    private void bind(DeltaSchemaContract.OperationMode mode, StructType requiredSchema) {
      if (nativeStage != null) return;
      String location;
      synchronized (SchemaContractDeltaCatalog.this) {
        location =
            replacing
                ? locations.get(key(id))
                : new Path(new Path(baseLocation, "db"), id.name()).toString();
      }
      DeltaSnapshotMetadata base = null;
      if (replacing) {
        try (DeltaMetadataReader reader =
            new DeltaMetadataReader(SparkSession.active().sparkContext().hadoopConfiguration())) {
          base = reader.latest(location);
        }
      }
      long identity = identities.getAndIncrement();
      String token = UUID.randomUUID().toString();
      boolean mapped =
          !"true".equals(properties.get(FORCE_V1))
              && ("name".equalsIgnoreCase(properties.get(MAPPING_MODE))
                  || "name"
                      .equalsIgnoreCase(
                          SparkSession.active().conf().get(DEFAULT_MAPPING_MODE, "none")));
      if (mapped) {
        DeltaSchemaContract contract =
            DeltaSchemaContract.spark35Delta33NameMapping(mode, requiredSchema.json());
        marker =
            replacing
                ? DeltaOperationMarker.replaceWithSchemaContract(
                    identity, identity, token, location, contract, base.version(), base.uuid())
                : DeltaOperationMarker.createWithSchemaContract(
                    identity, identity, token, location, contract);
      } else {
        marker =
            replacing
                ? DeltaOperationMarker.replace(
                    identity,
                    identity,
                    token,
                    location,
                    requiredSchema.json(),
                    base.version(),
                    base.uuid())
                : DeltaOperationMarker.create(
                    identity, identity, token, location, requiredSchema.json());
      }
      Map<String, String> nativeProperties =
          new HashMap<>(marker.injectInto(new HashMap<>(properties)));
      nativeProperties.remove(FORCE_V1);
      nativeProperties.put("provider", "delta");
      OperationRecord record =
          new OperationRecord(
              id.toString(), requested, replacing ? "REPLACE" : "CREATE", mode, marker, location);
      synchronized (SchemaContractDeltaCatalog.this) {
        records.put(key(id), record);
      }
      boolean queryCreateOrReplace =
          mode == DeltaSchemaContract.OperationMode.QUERY
              && requested == RequestedOperation.CREATE_OR_REPLACE;
      if (queryCreateOrReplace) {
        nativeStage =
            new ReplacementStage(
                nativeCatalog.stageCreateOrReplace(
                    pathIdentifier(location), requiredSchema, partitions, nativeProperties));
      } else if (replacing) {
        nativeStage =
            new ReplacementStage(
                nativeCatalog.stageReplace(
                    pathIdentifier(location), requiredSchema, partitions, nativeProperties));
      } else {
        nativeStage =
            nativeCatalog.stageCreate(
                pathIdentifier(location), requiredSchema, partitions, nativeProperties);
      }
    }
  }

  /** Immutable pre-native marker provenance exposed to the owning test. */
  public static final class OperationRecord {
    private final String identifier;
    private final RequestedOperation requested;
    private final String selected;
    private final DeltaSchemaContract.OperationMode mode;
    private final DeltaOperationMarker marker;
    private final String location;

    private OperationRecord(
        String identifier,
        RequestedOperation requested,
        String selected,
        DeltaSchemaContract.OperationMode mode,
        DeltaOperationMarker marker,
        String location) {
      this.identifier = identifier;
      this.requested = requested;
      this.selected = selected;
      this.mode = mode;
      this.marker = marker;
      this.location = location;
    }

    /** Returns the logical Spark identifier. */
    public String identifier() {
      return identifier;
    }

    /** Returns the original requested operation. */
    public String requestedOperation() {
      return requested.name();
    }

    /** Returns the selected native CREATE or REPLACE route. */
    public String selectedOperation() {
      return selected;
    }

    /** Returns the schema binding mode selected before native execution. */
    public DeltaSchemaContract.OperationMode mode() {
      return mode;
    }

    /** Returns the exact marker bound before native execution. */
    public DeltaOperationMarker marker() {
      return marker;
    }

    /** Returns the owned native location. */
    public String location() {
      return location;
    }
  }

  private enum RequestedOperation {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE
  }

  private final class PathOnlyDelegate extends DelegatingCatalogExtension {
    /** {@inheritDoc} */
    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
      // The delegate has no backing metastore; initialization only establishes the enclosing name.
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
      return catalogName;
    }

    /** {@inheritDoc} */
    @Override
    public Table loadTable(Identifier id) throws NoSuchTableException {
      throw new NoSuchTableException(id);
    }
  }
}
