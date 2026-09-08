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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.GenericTable;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;

/** Test-only single-record protocol. No native writer or destructive cleanup is available. */
final class RegistrationLifecycleFixture extends ManagedTableOperations {
  private static final String INTERNAL = RegistrationCommand.INTERNAL;
  private static final String RECEIPT = "delta.registration.receipt.";
  private static final String PRIOR = INTERNAL + "prior.";

  enum State {
    RESERVED,
    PUBLISHED,
    REPLACING,
    ABORTED,
    QUARANTINED
  }

  private final EntityStore entityStore;
  private final SupportsSchemas schemaOperations;
  private final IdGenerator ids;
  private final Supplier<String> principal;
  private final String recoveryPrincipal;
  private final BooleanSupplier writerHandoff;
  private final RegistrationCommitVerifier verifier;
  @Nullable private final RegistrationReplacementBaseReader baseReader;
  private final boolean nativeProjection;

  RegistrationLifecycleFixture(
      EntityStore store,
      SupportsSchemas schemas,
      IdGenerator ids,
      Supplier<String> principal,
      String recoveryPrincipal,
      BooleanSupplier writerHandoff,
      RegistrationCommitVerifier verifier) {
    this(store, schemas, ids, principal, recoveryPrincipal, writerHandoff, verifier, null, false);
  }

  RegistrationLifecycleFixture(
      EntityStore store,
      SupportsSchemas schemas,
      IdGenerator ids,
      Supplier<String> principal,
      String recoveryPrincipal,
      BooleanSupplier writerHandoff,
      RegistrationCommitVerifier verifier,
      @Nullable RegistrationReplacementBaseReader baseReader,
      boolean nativeProjection) {
    this.baseReader = baseReader;
    this.nativeProjection = nativeProjection;
    this.entityStore = store;
    this.schemaOperations = schemas;
    this.ids = ids;
    this.principal = principal;
    this.recoveryPrincipal = recoveryPrincipal;
    this.writerHandoff = writerHandoff;
    this.verifier = verifier;
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
    if (properties.size() != 2
        || !properties.containsKey(StringIdentifier.ID_KEY)
        || !properties.containsKey(RegistrationCommand.KEY)
        || columns.length != 0
        || comment != null
        || partitions.length != 0
        || sortOrders.length != 0
        || indexes.length != 0
        || !Distributions.NONE.equals(distribution)) {
      throw new IllegalArgumentException(
          "Reserve accepts only the command and dispatcher identity");
    }
    RegistrationCommand command =
        RegistrationCommand.decode(properties.get(RegistrationCommand.KEY));
    if (command.operation != RegistrationCommand.Operation.RESERVE) {
      throw new IllegalArgumentException("Create requires RESERVE");
    }
    String actor = Objects.requireNonNull(principal.get());
    long id = Objects.requireNonNull(StringIdentifier.fromProperties(properties)).id();
    Map<String, String> stored = new HashMap<>();
    stored.put(StringIdentifier.ID_KEY, properties.get(StringIdentifier.ID_KEY));
    stored.put(INTERNAL + "version", "1");
    stored.put(INTERNAL + "state", State.RESERVED.name());
    stored.put(INTERNAL + "generation", "0");
    stored.put(INTERNAL + "token", command.token);
    stored.put(INTERNAL + "actor", actor);
    stored.put(INTERNAL + "location", command.location);
    stored.put(INTERNAL + "schemaFingerprint", command.schemaFingerprint);
    // Canonical immutable request content is the fingerprint; the injected candidate ID is
    // excluded.
    stored.put(INTERNAL + "request", command.encode());
    stored.put(INTERNAL + "operation", "RESERVE");
    if (baseReader != null) stored.put(INTERNAL + "actualBaseRequired", "true");
    TableEntity candidate =
        TableEntity.builder()
            .withId(id)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withProperties(Collections.unmodifiableMap(stored))
            .withAuditInfo(
                AuditInfo.builder().withCreator(actor).withCreateTime(Instant.now()).build())
            .build();
    try {
      entityStore.put(candidate, false);
      return receipt(candidate);
    } catch (EntityAlreadyExistsException exists) {
      TableEntity current = raw(ident);
      if (!actor.equals(value(current, "actor"))) {
        throw new ForbiddenException("Reservation belongs to another principal");
      }
      if (!command.encode().equals(value(current, "request"))
          || Long.parseLong(value(current, "generation")) != 0
          || state(current) != State.RESERVED) {
        throw new IllegalStateException("Name is already reserved with different content or state");
      }
      return receipt(current);
    } catch (NoSuchEntityException absent) {
      throw new NoSuchSchemaException(absent, "Schema %s does not exist", ident.namespace());
    } catch (IOException failure) {
      throw new RuntimeException("Cannot reserve table", failure);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes) {
    if (changes.length != 1 || !(changes[0] instanceof TableChange.SetProperty)) {
      throw new IllegalArgumentException("Exactly one lifecycle command is required");
    }
    TableChange.SetProperty property = (TableChange.SetProperty) changes[0];
    if (!RegistrationCommand.KEY.equals(property.getProperty())) {
      throw new IllegalArgumentException(
          "Direct persisted-state and ordinary edits are prohibited");
    }
    RegistrationCommand command = RegistrationCommand.decode(property.getValue());
    if (command.operation == RegistrationCommand.Operation.RESERVE) {
      throw new IllegalArgumentException("RESERVE requires create");
    }
    String actor = Objects.requireNonNull(principal.get());
    boolean handedOff = writerHandoff.getAsBoolean();
    TableEntity inspected = raw(ident);
    validateExpectedState(inspected, command);
    authorize(inspected, actor);
    if (isCompletedReplay(inspected, command)) {
      return receipt(inspected);
    }
    if ((command.operation == RegistrationCommand.Operation.ABORT
            || actor.equals(recoveryPrincipal)
            || state(inspected) == State.QUARANTINED)
        && !handedOff) {
      throw new IllegalStateException("Explicit test writer handoff is required");
    }
    validateTransition(inspected, command);
    RegistrationReplacementBaseReader.BaseObservation actualBase =
        command.operation == RegistrationCommand.Operation.BEGIN_REPLACE && baseReader != null
            ? baseReader.read(inspected)
            : null;
    if (actualBase != null
        && (actualBase.version < Long.parseLong(value(inspected, "native.version"))
            || !actualBase.uuid.equals(value(inspected, "native.uuid"))
            || !actualBase.schemaJson.equals(value(inspected, "native.schemaFingerprint")))) {
      throw new IllegalStateException("Actual replacement base disagrees with publication lineage");
    }
    RegistrationCommitVerifier.Result evidence =
        command.operation == RegistrationCommand.Operation.BEGIN_REPLACE
            ? RegistrationCommitVerifier.Result.notCommitted("No inspection for begin replacement")
            : verifier.verify(inspected, command.candidateVersion);
    // Allocate column identities before the pure callback. The verifier cannot mutate stored maps.
    List<ColumnEntity> observedColumns = new ArrayList<>();
    if (evidence.kind() == RegistrationCommitVerifier.Kind.MATCHED) {
      RegistrationCommitVerifier.Observation observation =
          Objects.requireNonNull(evidence.observation());
      for (Column column : observation.columns()) {
        observedColumns.add(
            ColumnEntity.toColumnEntity(
                column, observedColumns.size(), ids.nextId(), inspected.auditInfo()));
      }
    }
    try {
      return receipt(
          entityStore.update(
              ident,
              TableEntity.class,
              Entity.EntityType.TABLE,
              current -> {
                validateExpectedState(current, command);
                authorize(current, actor);
                if (!current.equals(inspected)) {
                  throw new IllegalStateException("Reservation changed during evidence inspection");
                }
                TableEntity candidate =
                    applyVerifiedTransition(
                        current, command, evidence, observedColumns, actualBase);
                state(candidate);
                return candidate;
              }));
    } catch (IOException failure) {
      throw new RuntimeException("Cannot transition table", failure);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(NameIdentifier ident) {
    TableEntity entity = raw(ident);
    if (state(entity) != State.PUBLISHED) {
      throw new NoSuchTableException("Table %s is not published", ident);
    }
    return receipt(entity);
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) {
    try {
      return entityStore.list(namespace, TableEntity.class, Entity.EntityType.TABLE).stream()
          .filter(entity -> state(entity) == State.PUBLISHED)
          .map(entity -> NameIdentifier.of(namespace, entity.name()))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchEntityException absent) {
      throw new NoSuchSchemaException(absent, "Schema %s does not exist", namespace);
    } catch (IOException failure) {
      throw new RuntimeException("Cannot list registrations", failure);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException(
        "No expected-ID guarded TABLE delete; tombstones are retained");
  }

  /** {@inheritDoc} */
  @Override
  public boolean purgeTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("No data deletion in this experiment");
  }

  /** {@inheritDoc} */
  @Override
  protected EntityStore store() {
    return entityStore;
  }
  /** {@inheritDoc} */
  @Override
  protected SupportsSchemas schemas() {
    return schemaOperations;
  }
  /** {@inheritDoc} */
  @Override
  protected IdGenerator idGenerator() {
    return ids;
  }

  static TableEntity copy(
      TableEntity old, Map<String, String> properties, List<ColumnEntity> columns) {
    return TableEntity.builder()
        .withId(old.id())
        .withName(old.name())
        .withNamespace(old.namespace())
        .withAuditInfo(old.auditInfo())
        .withComment(old.comment())
        .withColumns(Collections.unmodifiableList(new ArrayList<>(columns)))
        .withProperties(Collections.unmodifiableMap(new HashMap<>(properties)))
        .withPartitioning(old.partitioning().clone())
        .withDistribution(old.distribution())
        .withSortOrders(old.sortOrders().clone())
        .withIndexes(old.indexes().clone())
        .build();
  }

  private TableEntity raw(NameIdentifier ident) {
    try {
      TableEntity entity = entityStore.get(ident, Entity.EntityType.TABLE, TableEntity.class);
      state(entity);
      return copy(entity, entity.properties(), entity.columns());
    } catch (NoSuchEntityException absent) {
      throw new NoSuchTableException(absent, "Table %s does not exist", ident);
    } catch (IOException failure) {
      throw new RuntimeException("Cannot read reservation", failure);
    }
  }

  private void validateExpectedState(TableEntity current, RegistrationCommand command) {
    state(current);
    if (current.id() != command.expectedId
        || Long.parseLong(value(current, "generation")) != command.generation
        || !command.token.equals(value(current, "token"))) {
      throw new IllegalStateException("Stale registration identity, token or generation");
    }
  }

  private void authorize(TableEntity current, String actor) {
    if (!actor.equals(value(current, "actor")) && !actor.equals(recoveryPrincipal)) {
      throw new ForbiddenException("Token is not recovery authority");
    }
  }

  private boolean isCompletedReplay(TableEntity current, RegistrationCommand command) {
    return (state(current) == State.PUBLISHED || state(current) == State.ABORTED)
        && command.encode().equals(current.properties().get(INTERNAL + "completedCommand"));
  }

  private void validateTransition(TableEntity current, RegistrationCommand command) {
    State state = state(current);
    if (command.operation == RegistrationCommand.Operation.BEGIN_REPLACE) {
      if (state != State.PUBLISHED) {
        throw new IllegalStateException("Replacement requires publication");
      }
      Math.addExact(command.generation, 1);
    } else if (state != State.RESERVED && state != State.REPLACING && state != State.QUARANTINED) {
      throw new IllegalStateException("Transition requires a pending operation");
    }
  }

  private TableEntity applyVerifiedTransition(
      TableEntity current,
      RegistrationCommand command,
      RegistrationCommitVerifier.Result evidence,
      List<ColumnEntity> observedColumns,
      @Nullable RegistrationReplacementBaseReader.BaseObservation actualBase) {
    Map<String, String> properties = new HashMap<>(current.properties());
    if (command.operation == RegistrationCommand.Operation.BEGIN_REPLACE) {
      current
          .properties()
          .forEach(
              (key, value) -> {
                if (key.startsWith(INTERNAL)) {
                  properties.put(PRIOR + key.substring(INTERNAL.length()), value);
                }
              });
      if (actualBase != null) {
        properties.put(INTERNAL + "replacement.base.version", Long.toString(actualBase.version));
        properties.put(INTERNAL + "replacement.base.uuid", actualBase.uuid);
        properties.put(INTERNAL + "replacement.base.schemaFingerprint", actualBase.schemaJson);
      }
      properties.put(INTERNAL + "state", State.REPLACING.name());
      properties.put(INTERNAL + "generation", Long.toString(Math.addExact(command.generation, 1)));
      properties.put(INTERNAL + "token", command.replacementToken);
      properties.put(INTERNAL + "schemaFingerprint", command.schemaFingerprint);
      properties.put(INTERNAL + "operation", "REPLACE");
      properties.remove(INTERNAL + "completedCommand");
      return copy(current, properties, current.columns());
    }
    properties.put(INTERNAL + "reason", evidence.reason());
    if (evidence.kind() == RegistrationCommitVerifier.Kind.MATCHED) {
      RegistrationCommitVerifier.Observation observation =
          Objects.requireNonNull(evidence.observation());
      String base = current.properties().get(INTERNAL + "native.version");
      if (base != null && observation.nativeVersion() <= Long.parseLong(base)) {
        throw new IllegalStateException("Verified replacement does not advance the stored base");
      }
      properties
          .keySet()
          .removeIf(key -> key.startsWith(PRIOR) || key.startsWith(INTERNAL + "replacement.base."));
      properties.put(INTERNAL + "state", State.PUBLISHED.name());
      properties.put(INTERNAL + "native.version", Long.toString(observation.nativeVersion()));
      properties.put(INTERNAL + "native.uuid", observation.nativeUuid());
      properties.put(INTERNAL + "native.schemaFingerprint", observation.schemaFingerprint());
      properties.put(INTERNAL + "completedCommand", command.encode());
      return copy(current, properties, observedColumns);
    }
    if (evidence.kind() == RegistrationCommitVerifier.Kind.AMBIGUOUS) {
      properties.put(INTERNAL + "state", State.QUARANTINED.name());
    } else if (command.operation == RegistrationCommand.Operation.ABORT) {
      if (properties.containsKey(PRIOR + "state")) {
        Map<String, String> restored = new HashMap<>();
        properties.forEach(
            (key, value) -> {
              if (key.startsWith(PRIOR)) {
                restored.put(INTERNAL + key.substring(PRIOR.length()), value);
              }
            });
        restored.put(StringIdentifier.ID_KEY, properties.get(StringIdentifier.ID_KEY));
        restored.put(INTERNAL + "generation", value(current, "generation"));
        restored.put(INTERNAL + "token", command.token);
        restored.put(INTERNAL + "reason", evidence.reason());
        properties.clear();
        properties.putAll(restored);
      } else {
        properties.put(INTERNAL + "state", State.ABORTED.name());
      }
      properties.put(INTERNAL + "completedCommand", command.encode());
    }
    return copy(current, properties, current.columns());
  }

  private static State state(TableEntity entity) {
    if (!"1".equals(value(entity, "version"))) {
      throw new IllegalArgumentException("Unsupported persisted record version");
    }
    long generation = Long.parseLong(value(entity, "generation"));
    if (generation < 0) {
      throw new IllegalArgumentException("Invalid stored generation");
    }
    State state = State.valueOf(value(entity, "state"));
    value(entity, "actor");
    value(entity, "token");
    value(entity, "schemaFingerprint");
    RegistrationCommand.normalizeLocation(value(entity, "location"));
    if (RegistrationCommand.decode(value(entity, "request")).operation
        != RegistrationCommand.Operation.RESERVE) {
      throw new IllegalArgumentException("Stored request is not a reservation");
    }
    String operation = value(entity, "operation");
    if (!"RESERVE".equals(operation) && !"REPLACE".equals(operation)) {
      throw new IllegalArgumentException("Unknown stored operation");
    }
    if (state == State.PUBLISHED || entity.properties().containsKey(INTERNAL + "native.version")) {
      if (Long.parseLong(value(entity, "native.version")) < 0) {
        throw new IllegalArgumentException("Invalid stored native version");
      }
      value(entity, "native.uuid");
      value(entity, "native.schemaFingerprint");
    }
    boolean pendingReplace =
        state == State.REPLACING || (state == State.QUARANTINED && "REPLACE".equals(operation));
    boolean anyBase =
        entity.properties().keySet().stream()
            .anyMatch(key -> key.startsWith(INTERNAL + "replacement.base."));
    if (anyBase
        || (pendingReplace
            && "true".equals(entity.properties().get(INTERNAL + "actualBaseRequired")))) {
      if (!pendingReplace
          || Long.parseLong(value(entity, "replacement.base.version"))
              < Long.parseLong(value(entity, "native.version"))
          || !value(entity, "replacement.base.uuid").equals(value(entity, "native.uuid"))
          || !value(entity, "replacement.base.schemaFingerprint")
              .equals(value(entity, "native.schemaFingerprint"))) {
        throw new IllegalArgumentException("Invalid actual replacement base");
      }
    }
    validatePriorPublication(entity, state, operation);
    return state;
  }

  private static void validatePriorPublication(TableEntity entity, State state, String operation) {
    boolean replacing =
        state == State.REPLACING || (state == State.QUARANTINED && "REPLACE".equals(operation));
    Map<String, String> priorProperties = new HashMap<>();
    entity
        .properties()
        .forEach(
            (key, value) -> {
              if (key.startsWith(PRIOR)) {
                String suffix = key.substring(PRIOR.length());
                if (suffix.startsWith("prior.")) {
                  throw new IllegalArgumentException("Nested prior publication is invalid");
                }
                priorProperties.put(INTERNAL + suffix, value);
              }
            });
    if (!replacing) {
      if (!priorProperties.isEmpty()) {
        throw new IllegalArgumentException(
            "Only pending replacements may retain prior publication");
      }
      return;
    }
    if (!"REPLACE".equals(operation)) {
      throw new IllegalArgumentException("Replacing state requires replacement operation");
    }
    TableEntity prior = copy(entity, priorProperties, entity.columns());
    if (state(prior) != State.PUBLISHED) {
      throw new IllegalArgumentException(
          "Replacement requires a complete published prior snapshot");
    }
    long generation = Long.parseLong(value(entity, "generation"));
    long priorGeneration = Long.parseLong(value(prior, "generation"));
    if (generation == 0
        || priorGeneration != generation - 1
        || value(entity, "token").equals(value(prior, "token"))) {
      throw new IllegalArgumentException("Prior publication has inconsistent generation or token");
    }
    for (String field :
        new String[] {
          "actor",
          "location",
          "request",
          "native.version",
          "native.uuid",
          "native.schemaFingerprint"
        }) {
      if (!value(entity, field).equals(value(prior, field))) {
        throw new IllegalArgumentException("Prior publication disagrees with retained " + field);
      }
    }
    value(prior, "reason");
    RegistrationCommand completed = RegistrationCommand.decode(value(prior, "completedCommand"));
    if ((completed.operation != RegistrationCommand.Operation.PUBLISH
            && completed.operation != RegistrationCommand.Operation.ABORT)
        || completed.expectedId != entity.id()
        || completed.generation != priorGeneration
        || !completed.token.equals(value(prior, "token"))) {
      throw new IllegalArgumentException("Prior publication has inconsistent completion receipt");
    }
  }

  private static String value(TableEntity entity, String suffix) {
    String value = entity.properties().get(INTERNAL + suffix);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Missing persisted field " + suffix);
    }
    return value;
  }

  private Table receipt(TableEntity entity) {
    Map<String, String> properties = new HashMap<>();
    properties.put(RECEIPT + "id", Long.toString(entity.id()));
    properties.put(RECEIPT + "generation", value(entity, "generation"));
    properties.put(RECEIPT + "token", value(entity, "token"));
    properties.put(RECEIPT + "state", state(entity).name());
    if (nativeProjection) {
      if (state(entity) == State.PUBLISHED) {
        properties.put("location", value(entity, "location"));
        properties.put(RECEIPT + "native.version", value(entity, "native.version"));
        properties.put(RECEIPT + "native.uuid", value(entity, "native.uuid"));
      } else {
        properties.put(
            RECEIPT + "marker", NativeRegistrationCommitVerifier.marker(entity).encode());
      }
    }
    return GenericTable.builder()
        .withName(entity.name())
        .withComment(entity.comment())
        .withProperties(Collections.unmodifiableMap(properties))
        .withAuditInfo(entity.auditInfo())
        .withColumns(
            entity.columns().stream()
                .sorted(Comparator.comparingInt(ColumnEntity::position))
                .map(
                    column ->
                        Column.of(
                            column.name(),
                            column.dataType(),
                            column.comment(),
                            column.nullable(),
                            column.autoIncrement(),
                            column.defaultValue()))
                .toArray(Column[]::new))
        .withPartitioning(entity.partitioning())
        .withDistribution(entity.distribution())
        .withSortOrders(entity.sortOrders())
        .withIndexes(entity.indexes())
        .build();
  }
}
