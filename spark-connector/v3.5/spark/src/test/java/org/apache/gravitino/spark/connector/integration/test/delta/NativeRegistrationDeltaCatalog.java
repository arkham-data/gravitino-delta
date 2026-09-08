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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.NativeRegistrationTestSupport;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Strict test-only native writer: durable reserve, same-commit marker, then real HTTP publication.
 */
@SuppressWarnings("deprecation")
public final class NativeRegistrationDeltaCatalog extends NamedDeltaCatalog {
  private Path journalDirectory;
  private boolean failAfterCommit;

  /** Creates the Spark test catalog plugin. */
  public NativeRegistrationDeltaCatalog() {}

  /** {@inheritDoc} */
  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    super.initialize(name, options);
    journalDirectory = Paths.get(options.get("journal"));
  }

  /** Enables an exception strictly after the native commit and before HTTP publication. */
  public void failAfterNativeCommit() {
    failAfterCommit = true;
  }

  /** {@inheritDoc} */
  @Override
  protected GravitinoAdminClient createMetadataClient(String uri) {
    return GravitinoAdminClient.builder(uri).withSimpleAuth("registration-actor").build();
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    if (tableExists(id)) throw new TableAlreadyExistsException(id);
    DeltaOperationMarker receipt = begin(id, schema, properties, false);
    Table table =
        nativeDelegate()
            .createTable(path(receipt), schema, partitions, inject(receipt, properties));
    afterCommit(id, receipt);
    return table;
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreate(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, TableAlreadyExistsException {
    if (tableExists(id)) throw new TableAlreadyExistsException(id);
    DeltaOperationMarker receipt = begin(id, schema, properties, false);
    return new PublicationStage(
        nativeDelegate()
            .stageCreate(path(receipt), schema, partitions, inject(receipt, properties)),
        id,
        receipt,
        false);
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException, NoSuchTableException {
    if (!tableExists(id)) throw new NoSuchTableException(id);
    DeltaOperationMarker receipt = begin(id, schema, properties, true);
    return new PublicationStage(
        new ReplacementStage(
            nativeDelegate()
                .stageReplace(path(receipt), schema, partitions, inject(receipt, properties))),
        id,
        receipt,
        true);
  }

  /** {@inheritDoc} */
  @Override
  public StagedTable stageCreateOrReplace(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws NoSuchNamespaceException {
    DeltaOperationMarker receipt = begin(id, schema, properties, tableExists(id));
    return new PublicationStage(
        new ReplacementStage(
            nativeDelegate()
                .stageCreateOrReplace(
                    path(receipt), schema, partitions, inject(receipt, properties))),
        id,
        receipt,
        true);
  }

  /** {@inheritDoc} */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty)
        guardKey(((TableChange.SetProperty) change).property());
      else if (change instanceof TableChange.RemoveProperty)
        guardKey(((TableChange.RemoveProperty) change).property());
      else
        throw new UnsupportedOperationException(
            "Schema ALTER is outside this bounded native registration adapter");
    }
    return super.alterTable(id, changes);
  }

  private DeltaOperationMarker begin(
      Identifier id, StructType schema, Map<String, String> properties, boolean replacing)
      throws NoSuchNamespaceException {
    if (!namespaceExists(id.namespace())) throw new NoSuchNamespaceException(id.namespace());
    properties.keySet().forEach(NativeRegistrationDeltaCatalog::guardKey);
    DeltaOperationMarker receipt =
        NativeRegistrationTestSupport.begin(
            registrationCatalog().asTableCatalog(),
            local(id),
            registrationLocation(id),
            schema.asNullable().json(),
            replacing);
    try {
      NativeRegistrationTestSupport.journal(
          journalDirectory.resolve(id.name() + ".properties"), local(id), receipt);
    } catch (IOException failure) {
      throw new IllegalStateException("Operator receipt was not persisted", failure);
    }
    return receipt;
  }

  private void afterCommit(Identifier id, DeltaOperationMarker receipt) {
    if (failAfterCommit) {
      failAfterCommit = false;
      throw new IllegalStateException("Injected after native commit before publication");
    }
    Map<String, String> response =
        NativeRegistrationTestSupport.publish(
                registrationCatalog().asTableCatalog(), local(id), receipt)
            .properties();
    if (!"PUBLISHED".equals(response.get("delta.registration.receipt.state")))
      throw new IllegalStateException("Native evidence did not authorize publication");
  }

  private static Map<String, String> inject(
      DeltaOperationMarker marker, Map<String, String> properties) {
    Map<String, String> copy = new HashMap<>(properties);
    copy.put("provider", "delta");
    return marker.injectInto(copy);
  }

  private static Identifier path(DeltaOperationMarker receipt) {
    return Identifier.of(new String[] {"delta"}, receipt.normalizedLocation());
  }

  private static NameIdentifier local(Identifier id) {
    return NameIdentifier.of(Namespace.of(id.namespace()), id.name());
  }

  private static void guardKey(String key) {
    String normalized = key.toLowerCase(Locale.ROOT);
    while (normalized.startsWith("option.")) normalized = normalized.substring(7);
    if (normalized.equals(DeltaOperationMarker.PROPERTY_KEY)
        || normalized.startsWith("delta.registration.")
        || normalized.equals("gravitino.identifier"))
      throw new IllegalArgumentException("Owned registration property collision");
  }

  private final class PublicationStage implements StagedTable, SupportsWrite {
    private final StagedTable delegate;
    private final Identifier id;
    private final DeltaOperationMarker receipt;
    private final boolean requiresQuery;
    private boolean writerBuilt;

    private PublicationStage(
        StagedTable delegate, Identifier id, DeltaOperationMarker receipt, boolean requiresQuery) {
      this.delegate = delegate;
      this.id = id;
      this.receipt = receipt;
      this.requiresQuery = requiresQuery;
    }
    /** {@inheritDoc} */
    @Override
    public String name() {
      return delegate.name();
    }
    /** {@inheritDoc} */
    @Override
    public StructType schema() {
      return delegate.schema();
    }
    /** {@inheritDoc} */
    @Override
    public Transform[] partitioning() {
      return delegate.partitioning();
    }
    /** {@inheritDoc} */
    @Override
    public Map<String, String> properties() {
      return delegate.properties();
    }
    /** {@inheritDoc} */
    @Override
    public Set<TableCapability> capabilities() {
      return delegate.capabilities();
    }
    /** {@inheritDoc} */
    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      info.options()
          .asCaseSensitiveMap()
          .keySet()
          .forEach(NativeRegistrationDeltaCatalog::guardKey);
      writerBuilt = true;
      return ((SupportsWrite) delegate).newWriteBuilder(info);
    }
    /** {@inheritDoc} */
    @Override
    public void commitStagedChanges() {
      if (requiresQuery && !writerBuilt)
        throw new UnsupportedOperationException(
            "Schema-only CREATE OR REPLACE routing is outside this adapter");
      delegate.commitStagedChanges();
      afterCommit(id, receipt);
    }
    /** {@inheritDoc} */
    @Override
    public void abortStagedChanges() {
      delegate.abortStagedChanges();
    }
  }
}
