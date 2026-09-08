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
package org.apache.gravitino.catalog.lakehouse.delta.registration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.lakehouse.generic.GenericCatalogCapability;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.PrincipalUtils;

/** ServiceLoader provider used only by the real managed-registration HTTP experiment. */
public final class RegistrationTestCatalog extends BaseCatalog<RegistrationTestCatalog> {
  static final List<Long> CANDIDATES = Collections.synchronizedList(new ArrayList<>());
  static final AtomicInteger ALTERS = new AtomicInteger();
  static final AtomicInteger DROPS = new AtomicInteger();
  static final AtomicInteger VERIFICATIONS = new AtomicInteger();
  static volatile boolean handoff;
  static volatile RegistrationCommitVerifier.Result outcome =
      RegistrationCommitVerifier.Result.notCommitted("synthetic absence");

  /** Creates the test-only ServiceLoader provider. */
  public RegistrationTestCatalog() {}

  /** {@inheritDoc} */
  @Override
  public String shortName() {
    return "delta-registration-test";
  }

  /** {@inheritDoc} */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new Operations();
  }

  /** {@inheritDoc} */
  @Override
  public Capability newCapability() {
    return new GenericCatalogCapability();
  }

  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata catalogPropertiesMetadata() {
    return new BaseCatalogPropertiesMetadata() {
      /** {@inheritDoc} */
      @Override
      protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
        return Collections.emptyMap();
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata schemaPropertiesMetadata() {
    return new Metadata(false);
  }

  /** {@inheritDoc} */
  @Override
  public PropertiesMetadata tablePropertiesMetadata() {
    return new Metadata(true);
  }

  static void reset() {
    CANDIDATES.clear();
    ALTERS.set(0);
    DROPS.set(0);
    VERIFICATIONS.set(0);
    handoff = false;
    outcome = RegistrationCommitVerifier.Result.notCommitted("synthetic absence");
  }

  private static final class Metadata extends BasePropertiesMetadata {
    private final boolean table;

    private Metadata(boolean table) {
      this.table = table;
    }

    /** {@inheritDoc} */
    @Override
    protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
      if (!table) {
        return Collections.emptyMap();
      }
      Map<String, PropertyEntry<?>> entries = new HashMap<>();
      entries.put(
          RegistrationCommand.KEY,
          PropertyEntry.stringOptionalPropertyEntry(
              RegistrationCommand.KEY, "Registration command", false, null, false));
      entries.put(
          RegistrationCommand.INTERNAL,
          PropertyEntry.stringPropertyPrefixEntry(
              RegistrationCommand.INTERNAL,
              "Internal registration state",
              false,
              true,
              null,
              true,
              true));
      String receipt = "delta.registration.receipt.";
      entries.put(
          receipt,
          PropertyEntry.stringPropertyPrefixEntry(
              receipt, "Registration receipt", false, true, null, false, true));
      return Collections.unmodifiableMap(entries);
    }
  }

  private static final class Operations
      implements CatalogOperations, SupportsSchemas, TableCatalog {
    private EntityStore store;
    private ManagedSchemaOperations schemas;
    private RegistrationLifecycleFixture tables;

    /** {@inheritDoc} */
    @Override
    public void initialize(
        Map<String, String> config, CatalogInfo info, HasPropertyMetadata metadata) {
      store = GravitinoEnv.getInstance().entityStore();
      schemas =
          new ManagedSchemaOperations() {
            /** {@inheritDoc} */
            @Override
            protected EntityStore store() {
              return store;
            }
          };
      tables =
          new RegistrationLifecycleFixture(
              store,
              schemas,
              GravitinoEnv.getInstance().idGenerator(),
              PrincipalUtils::getCurrentUserName,
              "registration-recovery",
              () -> handoff,
              (row, version) -> {
                VERIFICATIONS.incrementAndGet();
                return outcome;
              });
    }

    /** {@inheritDoc} */
    @Override
    public void close() {}

    /** {@inheritDoc} */
    @Override
    public void testConnection(NameIdentifier identifier) {}

    /** {@inheritDoc} */
    @Override
    public NameIdentifier[] listSchemas(Namespace namespace) {
      return schemas.listSchemas(namespace);
    }

    /** {@inheritDoc} */
    @Override
    public Schema createSchema(
        NameIdentifier identifier, @Nullable String comment, Map<String, String> properties) {
      return schemas.createSchema(identifier, comment, properties);
    }

    /** {@inheritDoc} */
    @Override
    public Schema loadSchema(NameIdentifier identifier) {
      return schemas.loadSchema(identifier);
    }

    /** {@inheritDoc} */
    @Override
    public Schema alterSchema(NameIdentifier identifier, SchemaChange... changes) {
      return schemas.alterSchema(identifier, changes);
    }

    /** {@inheritDoc} */
    @Override
    public boolean dropSchema(NameIdentifier identifier, boolean cascade) {
      Namespace tableNamespace =
          Namespace.of(
              identifier.namespace().level(0), identifier.namespace().level(1), identifier.name());
      try {
        if (!store.list(tableNamespace, TableEntity.class, Entity.EntityType.TABLE).isEmpty()) {
          throw new NonEmptySchemaException(
              "Registration records still exist in schema %s", identifier);
        }
      } catch (IOException failure) {
        throw new RuntimeException("Cannot inspect registration rows", failure);
      }
      return schemas.dropSchema(identifier, cascade);
    }

    /** {@inheritDoc} */
    @Override
    public NameIdentifier[] listTables(Namespace namespace) {
      return tables.listTables(namespace);
    }

    /** {@inheritDoc} */
    @Override
    public Table loadTable(NameIdentifier identifier) {
      return tables.loadTable(identifier);
    }

    /** {@inheritDoc} */
    @Override
    public Table createTable(
        NameIdentifier identifier,
        Column[] columns,
        @Nullable String comment,
        Map<String, String> properties,
        Transform[] partitions,
        Distribution distribution,
        SortOrder[] sortOrders,
        Index[] indexes) {
      CANDIDATES.add(StringIdentifier.fromProperties(properties).id());
      return tables.createTable(
          identifier, columns, comment, properties, partitions, distribution, sortOrders, indexes);
    }

    /** {@inheritDoc} */
    @Override
    public Table alterTable(NameIdentifier identifier, TableChange... changes) {
      ALTERS.incrementAndGet();
      return tables.alterTable(identifier, changes);
    }

    /** {@inheritDoc} */
    @Override
    public boolean dropTable(NameIdentifier identifier) {
      DROPS.incrementAndGet();
      return tables.dropTable(identifier);
    }

    /** {@inheritDoc} */
    @Override
    public boolean purgeTable(NameIdentifier identifier) {
      return tables.purgeTable(identifier);
    }
  }
}
