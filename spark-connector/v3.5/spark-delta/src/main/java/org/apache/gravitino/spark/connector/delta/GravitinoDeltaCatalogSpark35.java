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

package org.apache.gravitino.spark.connector.delta;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Optional Spark 3.5 catalog composing native Delta transactions with Gravitino registration.
 * Sessions must keep native catalog updates disabled through asynchronous hooks and align storage
 * routing with the provider. The identity descriptor supplies policy context, not authorization.
 */
@SuppressWarnings("deprecation")
public class GravitinoDeltaCatalogSpark35
    implements StagingTableCatalog, SupportsNamespaces, AutoCloseable {
  /** Explicit driver-owned directory for durable operation evidence. */
  public static final String JOURNAL_DIRECTORY = "journal-directory";

  private final DeltaCatalog nativeCatalog;
  private final Supplier<SparkSession> session;
  private final BiConsumer<String, DeltaRegistrationReceipt> headCheck;
  private DeltaRegistrationClient client;
  private DeltaRegistrationJournal journal;
  private String catalogName;
  private boolean closed;
  /** Creates a catalog initialized by Spark using current catalog-manager identities. */
  public GravitinoDeltaCatalogSpark35() {
    nativeCatalog = new DeltaCatalog();
    session = SparkSession::active;
    headCheck = this::validateHead;
  }

  /**
   * Creates an adapter with distinct caller-aware metadata and mutation suppliers.
   *
   * @param metadata current read-metadata catalog, which must not change the query subject
   * @param actor current operation actor catalog; never substitute the metadata credential here
   */
  public GravitinoDeltaCatalogSpark35(Supplier<Catalog> metadata, Supplier<Catalog> actor) {
    this();
    client = new DeltaRegistrationClient(metadata, actor);
  }

  GravitinoDeltaCatalogSpark35(
      DeltaCatalog nativeCatalog,
      DeltaRegistrationClient client,
      DeltaRegistrationJournal journal,
      Supplier<SparkSession> session,
      @Nullable BiConsumer<String, DeltaRegistrationReceipt> headCheck) {
    this.nativeCatalog = nativeCatalog;
    this.client = client;
    this.journal = journal;
    this.session = session;
    this.headCheck = headCheck == null ? this::validateHead : headCheck;
  }
  /** {@inheritDoc} */
  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    DeltaCatalogProperties.validateSession(session.get());
    catalogName = name;
    if (client == null) {
      client =
          new DeltaRegistrationClient(
              () -> GravitinoCatalogManager.get().getGravitinoCatalogInfo(catalogName),
              () -> GravitinoCatalogManager.get().getGravitinoCatalogInfo(catalogName));
    }
    if (journal == null) {
      String directory = options.get(JOURNAL_DIRECTORY);
      if (directory == null || directory.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Configure an explicit driver-owned " + JOURNAL_DIRECTORY + " for the Delta catalog");
      }
      journal = DeltaRegistrationJournal.durable(Paths.get(directory));
    }
    // Delta's path loader probes its delegate before applying native path fallback.
    nativeCatalog.setDelegateCatalog(new DeltaRegistryDelegate(this));
    nativeCatalog.initialize("delta", new CaseInsensitiveStringMap(Collections.emptyMap()));
  }
  /** {@inheritDoc} */
  @Override
  public String name() {
    return catalogName;
  }
  /** {@inheritDoc} */
  @Override
  public Identifier[] listTables(String[] namespace) {
    check();
    namespace(namespace);
    try {
      NameIdentifier[] names =
          client.metadataCatalog().asTableCatalog().listTables(Namespace.of(namespace));
      Identifier[] result = new Identifier[names.length];
      for (int i = 0; i < names.length; i++) {
        result[i] = Identifier.of(namespace, names[i].name());
      }
      return result;
    } catch (NoSuchSchemaException e) {
      return fail(new NoSuchNamespaceException(namespace));
    }
  }
  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id) {
    return load(id, null, null);
  }
  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id, String version) {
    return load(id, version, null);
  }
  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id, long timestamp) {
    return load(id, null, timestamp);
  }
  /** {@inheritDoc} */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    StagedTable stage = stageCreate(id, schema, partitions, properties);
    stage.commitStagedChanges();
    return loadTable(id);
  }
  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreate(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    return stage(id, schema, partitions, properties, DeltaPublicationStage.Requested.CREATE);
  }
  /** {@inheritDoc} */
  @Override
  public StagedTable stageReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    return stage(id, schema, partitions, properties, DeltaPublicationStage.Requested.REPLACE);
  }
  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreateOrReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    return stage(
        id, schema, partitions, properties, DeltaPublicationStage.Requested.CREATE_OR_REPLACE);
  }
  /** {@inheritDoc} */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    check();
    DeltaRegistrationClient.local(id);
    for (TableChange change : changes) {
      String key = null;
      if (change instanceof TableChange.SetProperty) {
        key = ((TableChange.SetProperty) change).property();
        DeltaCatalogProperties.validate(
            Collections.singletonMap(key, ((TableChange.SetProperty) change).value()));
      } else if (change instanceof TableChange.RemoveProperty) {
        key = ((TableChange.RemoveProperty) change).property();
        DeltaCatalogProperties.validate(Collections.singletonMap(key, "remove"));
      } else if (!(change instanceof TableChange.AddColumn
          || change instanceof TableChange.DeleteColumn
          || change instanceof TableChange.RenameColumn
          || change instanceof TableChange.UpdateColumnType
          || change instanceof TableChange.UpdateColumnComment
          || change instanceof TableChange.UpdateColumnNullability
          || change instanceof TableChange.UpdateColumnPosition)) {
        throw new UnsupportedOperationException("Unsupported Delta ALTER change");
      }
      if (key != null
          && (key.toLowerCase(Locale.ROOT).startsWith("delta.columnmapping.")
              || key.equalsIgnoreCase("provider"))) {
        throw new IllegalArgumentException("Reserved Delta ALTER property");
      }
      if (change instanceof TableChange.AddColumn) {
        DeltaCatalogProperties.validateSchema(((TableChange.AddColumn) change).dataType());
      } else if (change instanceof TableChange.UpdateColumnType) {
        DeltaCatalogProperties.validateSchema(
            ((TableChange.UpdateColumnType) change).newDataType());
      }
      if (change instanceof TableChange.AddColumn
          && ((TableChange.AddColumn) change).defaultValue() != null) {
        throw new UnsupportedOperationException(
            "Delta column defaults require a separately qualified schema contract");
      }
    }
    DeltaRegistrationClient.Observation observation = observation(id);
    String location = observation.receipt.location().get();
    headCheck.accept(location, observation.receipt);
    nativeCatalog.alterTable(path(location), changes);
    return loadTable(id);
  }
  /** {@inheritDoc} */
  @Override
  public void invalidateTable(Identifier id) {
    check();
    Optional<DeltaRegistrationClient.Observation> current = client.published(id);
    if (current.isPresent()) {
      nativeCatalog.invalidateTable(path(current.get().receipt.location().get()));
      DeltaTableIdentity.invalidate(current.get().receipt.location().get(), session.get());
    }
  }
  /** {@inheritDoc} */
  @Override
  public boolean dropTable(Identifier id) {
    throw new UnsupportedOperationException("Delta registration DROP is not supported");
  }
  /** {@inheritDoc} */
  @Override
  public boolean purgeTable(Identifier id) {
    throw new UnsupportedOperationException("Delta registration PURGE is not supported");
  }
  /** {@inheritDoc} */
  @Override
  public void renameTable(Identifier from, Identifier to) {
    throw new UnsupportedOperationException("Delta registration RENAME is not supported");
  }
  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces() {
    check();
    String[] names = client.metadataCatalog().asSchemas().listSchemas();
    String[][] result = new String[names.length][];
    for (int i = 0; i < names.length; i++) {
      result[i] = new String[] {names[i]};
    }
    return result;
  }
  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces(String[] parent) {
    if (parent.length == 0) {
      return listNamespaces();
    }
    loadNamespaceMetadata(parent);
    return new String[0][];
  }
  /** {@inheritDoc} */
  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) {
    check();
    namespace(namespace);
    try {
      Schema schema = client.metadataCatalog().asSchemas().loadSchema(namespace[0]);
      Map<String, String> result = new HashMap<>(schema.properties());
      if (schema.comment() != null) {
        result.put("comment", schema.comment());
      }
      return result;
    } catch (NoSuchSchemaException e) {
      return fail(new NoSuchNamespaceException(namespace));
    }
  }
  /** {@inheritDoc} */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    check();
    namespace(namespace);
    namespaceProperties(metadata);
    try {
      client
          .actorCatalog()
          .asSchemas()
          .createSchema(namespace[0], metadata.get("comment"), Collections.emptyMap());
    } catch (SchemaAlreadyExistsException e) {
      fail(new NamespaceAlreadyExistsException(namespace));
    }
  }
  /** {@inheritDoc} */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    check();
    namespace(namespace);
    List<SchemaChange> translated = new ArrayList<>();
    for (NamespaceChange change : changes) {
      if (change instanceof NamespaceChange.SetProperty) {
        NamespaceChange.SetProperty property = (NamespaceChange.SetProperty) change;
        namespaceProperties(Collections.singletonMap(property.property(), property.value()));
        translated.add(SchemaChange.setProperty("comment", property.value()));
      } else if (change instanceof NamespaceChange.RemoveProperty
          && "comment".equals(((NamespaceChange.RemoveProperty) change).property())) {
        translated.add(SchemaChange.removeProperty("comment"));
      } else {
        throw new IllegalArgumentException("Only namespace comment changes are supported");
      }
    }
    try {
      client
          .actorCatalog()
          .asSchemas()
          .alterSchema(namespace[0], translated.toArray(new SchemaChange[0]));
    } catch (NoSuchSchemaException e) {
      fail(new NoSuchNamespaceException(namespace));
    }
  }
  /** {@inheritDoc} */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    check();
    namespace(namespace);
    try {
      return client.actorCatalog().asSchemas().dropSchema(namespace[0], cascade);
    } catch (NonEmptySchemaException e) {
      return fail(new NonEmptyNamespaceException(namespace));
    }
  }
  /** {@inheritDoc} */
  @Override
  public void close() {
    closed = true;
  }

  private Table load(Identifier id, @Nullable String version, @Nullable Long timestamp) {
    check();
    DeltaRegistrationClient.Observation observation = observation(id);
    String location = observation.receipt.location().get();
    // Validate an ordinary current handle, then independently select the historical snapshot.
    headCheck.accept(location, observation.receipt);
    Table table =
        version != null
            ? nativeCatalog.loadTable(path(location), version)
            : timestamp != null
                ? nativeCatalog.loadTable(path(location), timestamp)
                : nativeCatalog.loadTable(path(location));
    if (!(table instanceof DeltaTableV2)) {
      throw DeltaRegistrationClient.bindingError();
    }
    return DeltaTableIdentity.attach((DeltaTableV2) table, observation.catalog, id);
  }

  private StagedTable stage(
      Identifier id,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      DeltaPublicationStage.Requested requested) {
    check();
    DeltaRegistrationClient.local(id);
    DeltaCatalogProperties.validate(properties);
    DeltaCatalogProperties.validateSchema(schema);
    DeltaCatalogProperties.mapping(
        properties, session.get().conf().get(DeltaCatalogProperties.MAPPING_DEFAULT, "none"));
    Pending pending =
        new Pending(
            id,
            partitions.clone(),
            Collections.unmodifiableMap(new HashMap<>(properties)),
            requested);
    return new DeltaPublicationStage(
        catalogName + "." + id,
        schema,
        partitions,
        properties,
        requested,
        this::check,
        pending::materialize,
        pending::publish,
        pending::abort);
  }

  private DeltaRegistrationClient.Observation observation(Identifier id) {
    DeltaRegistrationClient.local(id);
    Optional<DeltaRegistrationClient.Observation> current = client.published(id);
    if (!current.isPresent()) {
      return fail(new NoSuchTableException(id));
    }
    return current.get();
  }

  private void validateHead(String location, DeltaRegistrationReceipt receipt) {
    Table table = nativeCatalog.loadTable(path(location));
    if (!(table instanceof DeltaTableV2)) {
      throw DeltaRegistrationClient.bindingError();
    }
    DeltaTableIdentity.validate((DeltaTableV2) table, receipt);
  }

  private void check() {
    if (closed) {
      throw new IllegalStateException("Delta catalog is closed");
    }
    DeltaCatalogProperties.validateSession(session.get());
  }

  private static Identifier path(String location) {
    return Identifier.of(new String[] {"delta"}, location);
  }

  private static void namespace(String[] namespace) {
    if (namespace.length != 1) {
      fail(new NoSuchNamespaceException(namespace));
    }
  }

  private static void namespaceProperties(Map<String, String> properties) {
    for (Map.Entry<String, String> property : properties.entrySet()) {
      if (!"comment".equals(property.getKey())
          || property.getValue() == null
          || property.getValue().trim().isEmpty()
          || "******".equals(property.getValue())
          || property.getValue().startsWith(SecretConstants.URN_PREFIX)) {
        throw new IllegalArgumentException("Only a nonblank namespace comment is supported");
      }
    }
  }
  // Spark declares checked analysis exceptions on catalog APIs but not on StagedTable callbacks.
  @SuppressWarnings("unchecked")
  static <T, E extends Throwable> T fail(Throwable error) throws E {
    throw (E) error;
  }

  private final class Pending {
    private final Identifier id;
    private final Transform[] partitions;
    private final Map<String, String> properties;
    private final DeltaPublicationStage.Requested requested;
    private final String key = UUID.randomUUID().toString();
    private final String token = UUID.randomUUID().toString();
    private final String run = UUID.randomUUID().toString();
    @Nullable private DeltaRegistrationJournal.Entry entry;
    @Nullable private DeltaRegistrationReceipt receipt;
    private boolean publicationAttempted;

    private Pending(
        Identifier id,
        Transform[] partitions,
        Map<String, String> properties,
        DeltaPublicationStage.Requested requested) {
      this.id = id;
      this.partitions = partitions;
      this.properties = properties;
      this.requested = requested;
    }

    private StagedTable materialize(
        OperationMode mode, StructType schema, Map<String, String> options) {
      check();
      DeltaCatalogProperties.validateWrite(options);
      DeltaCatalogProperties.validateSchema(schema);
      String mapping =
          DeltaCatalogProperties.mapping(
              properties, session.get().conf().get(DeltaCatalogProperties.MAPPING_DEFAULT, "none"));
      DeltaSchemaContract contract =
          "name".equals(mapping)
              ? DeltaSchemaContract.spark35Delta33NameMapping(mode, schema.json())
              : null;
      Optional<DeltaRegistrationClient.Observation> existing = client.published(id);
      if (existing.isPresent() && requested == DeltaPublicationStage.Requested.CREATE) {
        return fail(new TableAlreadyExistsException(id));
      }
      if (!existing.isPresent() && requested == DeltaPublicationStage.Requested.REPLACE) {
        return fail(new NoSuchTableException(id));
      }
      if (existing.isPresent()) {
        headCheck.accept(existing.get().receipt.location().get(), existing.get().receipt);
      }
      DeltaRegistrationCommand command =
          existing.isPresent()
              ? (contract == null
                  ? DeltaRegistrationCommand.beginReplace(
                      existing.get().receipt.identity(), token, run, schema.json())
                  : DeltaRegistrationCommand.beginReplace(
                      existing.get().receipt.identity(), token, run, contract))
              : (contract == null
                  ? DeltaRegistrationCommand.reserve(token, run, schema.json())
                  : DeltaRegistrationCommand.reserve(token, run, contract));
      String canonical =
          existing.isPresent() ? existing.get().catalog : client.metadataCatalog().name();
      entry =
          new DeltaRegistrationJournal.Entry(
              key,
              canonical,
              id.namespace()[0],
              id.name(),
              command,
              DeltaRegistrationJournal.Phase.INTENT,
              null);
      journal.write(entry);
      try {
        receipt = client.submit(id, command);
      } catch (RuntimeException uncertain) {
        entry = entry.phase(DeltaRegistrationJournal.Phase.STATUS_NEEDED, null);
        journal.write(entry);
        throw new IllegalStateException(
            "DELTA_REGISTRATION_STATUS_NEEDED: retain journal; confirm request drain before operator recovery");
      }
      entry = entry.phase(DeltaRegistrationJournal.Phase.RESERVED, receipt);
      journal.write(entry);
      DeltaOperationMarker marker = receipt.marker().get();
      if (existing.isPresent()) {
        if (!existing.get().receipt.location().get().equals(marker.normalizedLocation())) {
          throw DeltaRegistrationClient.bindingError();
        }
        headCheck.accept(marker.normalizedLocation(), receipt);
      }
      Map<String, String> nativeProperties = new HashMap<>(marker.injectInto(properties));
      nativeProperties.put("provider", "delta");
      nativeProperties.put("delta.columnMapping.mode", mapping);
      entry = entry.phase(DeltaRegistrationJournal.Phase.NATIVE_STARTED, receipt);
      journal.write(entry);
      if (mode == OperationMode.QUERY
          && requested == DeltaPublicationStage.Requested.CREATE_OR_REPLACE) {
        return new DeltaReplacementStage(
            nativeCatalog.stageCreateOrReplace(
                path(marker.normalizedLocation()), schema, partitions, nativeProperties));
      }
      if (existing.isPresent()) {
        return new DeltaReplacementStage(
            nativeCatalog.stageReplace(
                path(marker.normalizedLocation()), schema, partitions, nativeProperties));
      }
      return nativeCatalog.stageCreate(
          path(marker.normalizedLocation()), schema, partitions, nativeProperties);
    }

    private void publish() {
      if (publicationAttempted) {
        throw new IllegalStateException(
            "DELTA_PUBLICATION_STATUS_NEEDED: automatic publication replay is disabled");
      }
      publicationAttempted = true;
      entry = entry.phase(DeltaRegistrationJournal.Phase.NATIVE_COMMITTED, receipt);
      journal.write(entry);
      DeltaRegistrationCommand command = DeltaRegistrationCommand.publish(receipt.identity());
      DeltaRegistrationJournal.Entry publication =
          new DeltaRegistrationJournal.Entry(
              UUID.randomUUID().toString(),
              entry.catalog,
              entry.namespace,
              entry.table,
              command,
              DeltaRegistrationJournal.Phase.INTENT,
              null);
      journal.write(publication);
      DeltaRegistrationReceipt published;
      try {
        published = client.submit(id, command);
      } catch (RuntimeException uncertain) {
        journal.write(publication.phase(DeltaRegistrationJournal.Phase.STATUS_NEEDED, null));
        throw new IllegalStateException(
            "DELTA_PUBLICATION_STATUS_NEEDED: native data and receipt retained");
      }
      if (published.state() != DeltaRegistrationReceipt.State.PUBLISHED) {
        throw DeltaRegistrationClient.bindingError();
      }
      journal.write(publication.phase(DeltaRegistrationJournal.Phase.PUBLISHED, published));
      receipt = published;
      entry = entry.phase(DeltaRegistrationJournal.Phase.PUBLISHED, published);
      journal.write(entry);
    }

    private void abort() {
      // Spark abort is local notification, not proof of native rollback or request drain.
      if (entry != null) {
        journal.write(entry);
      }
    }
  }
}
