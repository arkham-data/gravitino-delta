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

package org.apache.gravitino.catalog.lakehouse.delta;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
import org.apache.gravitino.catalog.ManagedSchemaOperations.ManagedSchema;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.secret.SecretPropertyUtils;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;

/**
 * Provider operations; owns one native reader and delegates the reviewed registration lifecycle.
 */
public final class DeltaCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog {
  private static final String COMMENT = "comment";
  private final EntityStore store;
  private final IdGenerator ids;
  private final Function<Configuration, DeltaMetadataReader> readers;
  private final ManagedSchemaOperations schemas;
  private DeltaMetadataReader reader;
  private DeltaRegistrationOperations tables;
  private boolean closed;

  /** Creates operations using the running server's managed storage and ID generator. */
  public DeltaCatalogOperations() {
    this(
        GravitinoEnv.getInstance().entityStore(),
        GravitinoEnv.getInstance().idGenerator(),
        DeltaMetadataReader::new);
  }

  DeltaCatalogOperations(
      EntityStore store, IdGenerator ids, Function<Configuration, DeltaMetadataReader> readers) {
    this.store = Objects.requireNonNull(store, "store");
    this.ids = Objects.requireNonNull(ids, "ids");
    this.readers = Objects.requireNonNull(readers, "readers");
    schemas =
        new ManagedSchemaOperations() {
          @Override
          protected EntityStore store() {
            return store;
          }
        };
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata metadata) {
    if (closed || reader != null) {
      throw new IllegalStateException("DELTA_OPERATIONS_ALREADY_INITIALIZED_OR_CLOSED");
    }
    try {
      DeltaCatalogConfig config = DeltaCatalogConfig.from(conf);
      reader = readers.apply(config.storageConfiguration());
      tables =
          new DeltaRegistrationOperations(
              store,
              this,
              ids,
              Objects.requireNonNull(reader, "reader"),
              config,
              new DeltaAuthorization());
    } catch (RuntimeException failure) {
      try {
        close();
      } catch (RuntimeException closing) {
        failure.addSuppressed(closing);
      }
      throw failure;
    }
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      if (reader != null) {
        reader.close();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void testConnection(NameIdentifier catalogIdent) {
    throw new UnsupportedOperationException(
        "Delta catalogs have no catalog-level connection probe; configuration is validated at initialization and native read access at operations");
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) {
    return schemas.listSchemas(namespace);
  }

  /** {@inheritDoc} */
  @Override
  public Schema createSchema(
      NameIdentifier ident, @Nullable String comment, Map<String, String> properties) {
    Map<String, String> users = new HashMap<>(Objects.requireNonNull(properties, "properties"));
    String injected = users.remove(StringIdentifier.ID_KEY);
    DeltaSchemaPropertiesMetadata.validateRequestProperties(users);
    StringIdentifier id;
    try {
      id = StringIdentifier.fromString(injected);
      if (id.id() <= 0) {
        throw new IllegalArgumentException();
      }
    } catch (RuntimeException invalid) {
      throw new IllegalArgumentException("INVALID_DELTA_SCHEMA_IDENTIFIER");
    }
    Map<String, String> internal = Map.of(StringIdentifier.ID_KEY, injected);
    SchemaEntity candidate =
        SchemaEntity.builder()
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withId(id.id())
            .withComment(comment)
            .withProperties(internal)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentUserName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    DeltaStorageAdmission.checkSchemaCandidate(candidate);
    return publicSchema(schemas.createSchema(ident, comment, internal));
  }

  /** {@inheritDoc} */
  @Override
  public Schema loadSchema(NameIdentifier ident) {
    return publicSchema(schemas.loadSchema(ident));
  }

  /** {@inheritDoc} */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes) {
    Objects.requireNonNull(changes, "changes");
    if (changes.length == 0) {
      return loadSchema(ident);
    }
    // "comment" is an existing property-shaped transport, never a stored user property.
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty
          && COMMENT.equals(((SchemaChange.SetProperty) change).getProperty())) {
        SecretPropertyUtils.validateAlterSetPropertyValue(
            COMMENT, ((SchemaChange.SetProperty) change).getValue());
        DeltaStorageAdmission.checkSchemaComment(((SchemaChange.SetProperty) change).getValue());
        continue;
      }
      if (change instanceof SchemaChange.RemoveProperty
          && COMMENT.equals(((SchemaChange.RemoveProperty) change).getProperty())) {
        continue;
      }
      throw new IllegalArgumentException("UNSUPPORTED_DELTA_SCHEMA_CHANGE");
    }
    try {
      SchemaEntity before = store.get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      String comment = before.comment();
      for (SchemaChange change : changes) {
        comment =
            change instanceof SchemaChange.SetProperty
                ? ((SchemaChange.SetProperty) change).getValue()
                : null;
      }
      SchemaEntity candidate =
          SchemaEntity.builder()
              .withId(before.id())
              .withName(before.name())
              .withNamespace(before.namespace())
              .withComment(comment)
              .withProperties(before.properties())
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(before.auditInfo().creator())
                      .withCreateTime(before.auditInfo().createTime())
                      .withLastModifier(PrincipalUtils.getCurrentUserName())
                      .withLastModifiedTime(Instant.now())
                      .build())
              .build();
      DeltaStorageAdmission.checkSchemaCandidate(candidate);
      SchemaEntity updated =
          store.update(
              ident,
              SchemaEntity.class,
              Entity.EntityType.SCHEMA,
              current -> {
                if (!current.equals(before)) {
                  throw new OptimisticLockException("Delta schema changed concurrently");
                }
                return candidate;
              });
      return ManagedSchema.builder()
          .withName(updated.name())
          .withComment(updated.comment())
          .withProperties(Map.of())
          .withAuditInfo(updated.auditInfo())
          .build();
    } catch (NoSuchEntityException absent) {
      throw new NoSuchSchemaException("Schema %s does not exist", ident);
    } catch (IOException failure) {
      throw new RuntimeException("Failed to alter Delta schema", failure);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) {
    try {
      if (!store
          .list(
              Namespace.of(ident.namespace().level(0), ident.namespace().level(1), ident.name()),
              TableEntity.class,
              Entity.EntityType.TABLE)
          .isEmpty()) {
        throw new NonEmptySchemaException("Schema %s retains Delta registrations", ident);
      }
      // The relational non-cascade transaction also checks children under its parent lock.
      return schemas.dropSchema(ident, false);
    } catch (NoSuchEntityException absent) {
      return false;
    } catch (IOException failure) {
      throw new RuntimeException("Failed to check Delta schema registrations", failure);
    }
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) {
    return tables.listTables(namespace);
  }
  /** {@inheritDoc} */
  @Override
  public Table loadTable(NameIdentifier ident) {
    try {
      return tables.loadTable(ident);
    } catch (IllegalStateException failure) {
      if ("DELTA_REGISTRATION_UNPUBLISHED".equals(failure.getMessage())) {
        throw new NoSuchTableException("Table %s is not published", ident);
      }
      throw failure;
    }
  }
  /** {@inheritDoc} */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      @Nullable String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    return tables.createTable(
        ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
  }
  /** {@inheritDoc} */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes) {
    return tables.alterTable(ident, changes);
  }
  /** {@inheritDoc} */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    return tables.dropTable(ident);
  }
  /** {@inheritDoc} */
  @Override
  public boolean purgeTable(NameIdentifier ident) {
    return tables.purgeTable(ident);
  }

  private static Schema publicSchema(Schema schema) {
    return ManagedSchema.builder()
        .withName(schema.name())
        .withComment(schema.comment())
        .withProperties(Map.of())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(schema.auditInfo().creator())
                .withCreateTime(schema.auditInfo().createTime())
                .withLastModifier(schema.auditInfo().lastModifier())
                .withLastModifiedTime(schema.auditInfo().lastModifiedTime())
                .build())
        .build();
  }
}
