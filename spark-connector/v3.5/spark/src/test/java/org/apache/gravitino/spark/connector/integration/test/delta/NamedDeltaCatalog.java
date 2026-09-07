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

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Experimental path-native engine facade; all durable name registration stays in Gravitino. */
@SuppressWarnings("deprecation")
public class NamedDeltaCatalog implements StagingTableCatalog, SupportsNamespaces, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(NamedDeltaCatalog.class);
  private GravitinoAdminClient client;
  private Catalog registry;
  private DeltaCatalog nativeCatalog;
  private String catalogName;
  private boolean direct;
  String failurePoint = "none";

  /**
   * {@inheritDoc}
   *
   * <p>Connects this fixture catalog to Gravitino and creates its native Delta delegate.
   */
  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    catalogName = name;
    client = createMetadataClient(options.get("uri"));
    registry =
        client
            .loadMetalake(options.get("metalake"))
            .loadCatalog(options.getOrDefault("registry", name));
    direct = Boolean.parseBoolean(options.getOrDefault("direct", "false"));
    nativeCatalog = new DeltaCatalog();
    nativeCatalog.setDelegateCatalog(new RegistryDelegate());
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return catalogName;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    client.close();
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id) throws NoSuchTableException {
    return loadRegistered(id);
  }

  /** {@inheritDoc} */
  @Override
  public boolean tableExists(Identifier id) {
    return registry.asTableCatalog().tableExists(gravitinoId(id));
  }

  /** {@inheritDoc} */
  @Override
  public void invalidateTable(Identifier id) {
    // No name or schema cache: every load resolves Gravitino and the native Delta snapshot.
  }

  /** {@inheritDoc} */
  @Override
  public Identifier[] listTables(String[] namespace) {
    return Arrays.stream(registry.asTableCatalog().listTables(Namespace.of(namespace)))
        .map(id -> Identifier.of(id.namespace().levels(), id.name()))
        .toArray(Identifier[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    precheck(id);
    String location = location(id);
    Table result =
        nativeCatalog.createTable(
            direct ? id : pathId(location), schema, partitions, nativeProperties(properties));
    publish(id, location);
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreate(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    precheck(id);
    String location = location(id);
    return new PublicationStage(
        nativeCatalog.stageCreate(
            direct ? id : pathId(location), schema, partitions, nativeProperties(properties)),
        id,
        location);
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    if (!tableExists(id)) throw new NoSuchTableException(id);
    String location = location(id);
    return new PublicationStage(
        new ReplacementStage(
            nativeCatalog.stageReplace(
                direct ? id : pathId(location), schema, partitions, nativeProperties(properties))),
        id,
        location);
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreateOrReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    checkNamespace(id.namespace());
    String location = location(id);
    return new PublicationStage(
        new ReplacementStage(
            nativeCatalog.stageCreateOrReplace(
                direct ? id : pathId(location), schema, partitions, nativeProperties(properties))),
        id,
        location);
  }

  /** {@inheritDoc} */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    // Native commands resolve through the Gravitino-backed delegate; generic metadata stays stale.
    return nativeCatalog.alterTable(id, changes);
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropTable(Identifier id) {
    return registry.asTableCatalog().dropTable(gravitinoId(id));
  }

  /** {@inheritDoc} */
  @Override
  public void renameTable(Identifier from, Identifier to) {
    registry
        .asTableCatalog()
        .alterTable(gravitinoId(from), org.apache.gravitino.rel.TableChange.rename(to.name()));
  }

  /** {@inheritDoc} */
  @Override
  public boolean namespaceExists(String[] namespace) {
    return namespace.length == 1 && registry.asSchemas().schemaExists(namespace[0]);
  }

  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces() {
    return Arrays.stream(registry.asSchemas().listSchemas())
        .map(n -> new String[] {n})
        .toArray(String[][]::new);
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
    return registry.asSchemas().loadSchema(namespace[0]).properties();
  }

  /** {@inheritDoc} */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties) {
    throw new UnsupportedOperationException("Fixture namespaces are created through Gravitino");
  }

  /** {@inheritDoc} */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    throw new UnsupportedOperationException("Not in the experimental contract");
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    throw new UnsupportedOperationException("Not in the experimental contract");
  }

  /**
   * Creates the fixture metadata client separately from the query UGI.
   *
   * @param uri the embedded Gravitino URI
   * @return the registration metadata client
   */
  protected GravitinoAdminClient createMetadataClient(String uri) {
    return GravitinoAdminClient.builder(uri).build();
  }

  private Table loadRegistered(Identifier id) throws NoSuchTableException {
    try {
      String location =
          registry.asTableCatalog().loadTable(gravitinoId(id)).properties().get("location");
      return nativeCatalog.loadTable(pathId(location));
    } catch (org.apache.gravitino.exceptions.NoSuchTableException missing) {
      throw new NoSuchTableException(id);
    }
  }

  private String location(Identifier id) {
    if (tableExists(id))
      return registry.asTableCatalog().loadTable(gravitinoId(id)).properties().get("location");
    return Paths.get(
            registry.asSchemas().loadSchema(id.namespace()[0]).properties().get("location"),
            id.name())
        .toString();
  }

  private void publish(Identifier id, String location) {
    if (registry.asTableCatalog().tableExists(gravitinoId(id))) {
      LOG.warn(
          "GENERIC LIMITATION: existing registration retained without native metadata refresh: {}.{}",
          catalogName,
          id);
      return;
    }
    StructType schema = nativeCatalog.loadTable(pathId(location)).schema();
    Column[] columns =
        Arrays.stream(schema.fields())
            .map(
                field ->
                    Column.of(
                        field.name(),
                        field.dataType().typeName().equals("long")
                            ? Types.LongType.get()
                            : Types.StringType.get()))
            .toArray(Column[]::new);
    registry
        .asTableCatalog()
        .createTable(
            gravitinoId(id),
            columns,
            "",
            Map.of("format", "delta", "external", "true", "location", location));
  }

  private void precheck(Identifier id)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    checkNamespace(id.namespace());
    if (tableExists(id)) throw new TableAlreadyExistsException(id);
  }

  private void checkNamespace(String[] namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) throw new NoSuchNamespaceException(namespace);
  }

  private NameIdentifier gravitinoId(Identifier id) {
    return NameIdentifier.of(Namespace.of(id.namespace()), id.name());
  }

  private Identifier pathId(String location) {
    return Identifier.of(new String[] {"delta"}, location);
  }

  private Map<String, String> nativeProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>(properties);
    result.put("provider", "delta");
    return result;
  }

  private void inject(String point) {
    if (failurePoint.equals(point)) throw new IllegalStateException("Injected " + point);
  }

  private final class RegistryDelegate extends DelegatingCatalogExtension {
    /** {@inheritDoc} */
    @Override
    public String name() {
      return catalogName;
    }

    /** {@inheritDoc} */
    @Override
    public Table loadTable(Identifier id) throws NoSuchTableException {
      if (Arrays.equals(id.namespace(), new String[] {"delta"})) throw new NoSuchTableException(id);
      return loadRegistered(id);
    }
  }

  private final class PublicationStage implements StagedTable, SupportsWrite {
    private final StagedTable nativeStage;
    private final Identifier id;
    private final String location;

    private PublicationStage(StagedTable nativeStage, Identifier id, String location) {
      this.nativeStage = nativeStage;
      this.id = id;
      this.location = location;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
      return nativeStage.name();
    }

    /** {@inheritDoc} */
    @Override
    public StructType schema() {
      return nativeStage.schema();
    }

    /** {@inheritDoc} */
    @Override
    public Transform[] partitioning() {
      return nativeStage.partitioning();
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> properties() {
      return nativeStage.properties();
    }

    /** {@inheritDoc} */
    @Override
    public Set<TableCapability> capabilities() {
      return nativeStage.capabilities();
    }

    /** {@inheritDoc} */
    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      return ((SupportsWrite) nativeStage).newWriteBuilder(info);
    }

    /** {@inheritDoc} */
    @Override
    public void commitStagedChanges() {
      inject("before-commit");
      nativeStage.commitStagedChanges();
      inject("after-commit");
      publish(id, location);
      inject("after-publication");
    }

    /** {@inheritDoc} */
    @Override
    public void abortStagedChanges() {
      nativeStage.abortStagedChanges();
    }
  }
}
