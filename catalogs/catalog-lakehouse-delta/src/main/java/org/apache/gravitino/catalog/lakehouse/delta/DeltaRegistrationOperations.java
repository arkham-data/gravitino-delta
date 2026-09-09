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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataException;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaSnapshotMetadata;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaCommitEvidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.apache.gravitino.connector.GenericColumn;
import org.apache.gravitino.connector.GenericTable;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
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
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;

/**
 * Managed registration lifecycle. Native storage is read-only and retained names are never deleted.
 */
final class DeltaRegistrationOperations extends ManagedTableOperations {
  private final EntityStore entityStore;
  private final SupportsSchemas schemas;
  private final IdGenerator ids;
  private final DeltaMetadataReader reader;
  private final DeltaCatalogConfig config;
  private final DeltaAuthorization authorization;
  private final DeltaRegistrationStore registrations;

  DeltaRegistrationOperations(
      EntityStore store,
      SupportsSchemas schemas,
      IdGenerator ids,
      DeltaMetadataReader reader,
      DeltaCatalogConfig config,
      DeltaAuthorization authorization) {
    this.entityStore = store;
    this.schemas = schemas;
    this.ids = ids;
    this.reader = reader;
    this.config = config;
    this.authorization = authorization;
    registrations = new DeltaRegistrationStore(store);
  }

  /** {@inheritDoc} */
  @Override
  protected EntityStore store() {
    return entityStore;
  }
  /** {@inheritDoc} */
  @Override
  protected SupportsSchemas schemas() {
    return schemas;
  }
  /** {@inheritDoc} */
  @Override
  protected IdGenerator idGenerator() {
    return ids;
  }

  /** {@inheritDoc} */
  @Override
  public Table createTable(
      NameIdentifier ident,
      @Nullable Column[] columns,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      @Nullable Transform[] partitions,
      @Nullable Distribution distribution,
      @Nullable SortOrder[] sortOrders,
      @Nullable Index[] indexes) {
    String actor = authorization.currentActor();
    boolean privileged = authorization.isPrivileged(ident, actor);
    if (columns != null && columns.length != 0
        || comment != null && !comment.isEmpty()
        || partitions != null && partitions.length != 0
        || distribution != null && !distribution.equals(Distributions.NONE)
        || sortOrders != null && sortOrders.length != 0
        || indexes != null && indexes.length != 0) {
      throw invalidCommand();
    }
    if (properties == null) {
      throw invalidCommand();
    }
    Map<String, String> request = new HashMap<>(properties);
    String injected = request.remove(StringIdentifier.ID_KEY);
    DeltaTablePropertiesMetadata.validateRequestProperties(request);
    long id;
    try {
      id = StringIdentifier.fromString(injected).id();
    } catch (RuntimeException e) {
      throw invalidCommand();
    }
    DeltaRegistrationCommand command =
        DeltaRegistrationCommand.decode(request.get(DeltaRegistrationCommand.COMMAND_PROPERTY));
    if (command.operation() != Operation.RESERVE && command.operation() != Operation.ADOPT) {
      throw invalidCommand();
    }
    if (command.operation() == Operation.ADOPT && !privileged) {
      throw denied();
    }
    try {
      return pendingReplay(ident, registrations.load(ident), command, actor);
    } catch (NoSuchEntityException absent) {
      /* Admission continues only for an absent name. */
    }
    schemas.loadSchema(NameIdentifier.of(ident.namespace().levels()));
    String location =
        command.operation() == Operation.ADOPT
            ? config.locations().admittedExistingLocation(command.location().orElseThrow())
            : config.locations().newTableLocation(ident, id);
    // Store the canonical admitted source in the opening command for deterministic replay.
    if (command.operation() == Operation.ADOPT) {
      command =
          DeltaRegistrationCommand.adopt(
              command.token().orElseThrow(),
              location,
              command.runId().orElseThrow(),
              command.reference().orElseThrow());
    }
    DeltaNativeObservation observed = null;
    if (command.operation() == Operation.ADOPT) {
      try {
        observed = observe(location);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("INVALID_ADOPTION_MARKER");
      }
    }
    DeltaRegistrationState initial =
        DeltaRegistrationState.initial(
            ident, id, command, actor, location, newAudit(actor), observed);
    if (observed != null) {
      initial = projected(initial, initial.data(), observed, actor);
    }
    try {
      return table(registrations.insert(initial.entity()), false);
    } catch (EntityAlreadyExistsException concurrent) {
      throw new TableAlreadyExistsException("REGISTRATION_ALREADY_EXISTS");
    }
  }

  /** {@inheritDoc} */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes) {
    String actor = authorization.currentActor();
    boolean privileged = authorization.isPrivileged(ident, actor);
    boolean recovery = privileged || config.recoveryPrincipals().contains(actor);
    if (changes == null
        || changes.length != 1
        || !(changes[0] instanceof TableChange.SetProperty)) {
      throw invalidCommand();
    }
    TableChange.SetProperty property = (TableChange.SetProperty) changes[0];
    if (!DeltaRegistrationCommand.COMMAND_PROPERTY.equals(property.getProperty())) {
      throw invalidCommand();
    }
    DeltaRegistrationCommand command = DeltaRegistrationCommand.decode(property.getValue());
    DeltaRegistrationState current = load(ident);
    if (command.operation() == Operation.ADMINISTRATIVE_STATUS) {
      if (!privileged) {
        throw denied();
      }
      return table(current, false);
    }
    if (command.operation() == Operation.STATUS_BY_TOKEN) {
      authorizeActor(current, actor, recovery);
      if (!current.receipt().identity().token().equals(command.token().orElseThrow())
          || !current.receipt().runId().equals(command.runId().orElseThrow())) {
        throw changed();
      }
      return table(current, false);
    }
    if (!command.expected().isPresent()) {
      throw invalidCommand();
    }
    if (!current.receipt().identity().equals(command.expected().orElseThrow())) {
      throw changed();
    }
    switch (command.operation()) {
      case STATUS:
        authorizeActor(current, actor, recovery);
        return table(current, false);
      case BEGIN_REPLACE:
        return table(beginReplace(current, command, actor), false);
      case HANDOFF:
        if (!recovery) {
          throw denied();
        }
        requirePending(current);
        if (!current.receipt().runId().equals(command.runId().orElseThrow())) {
          throw changed();
        }
        ObjectNode handoffData = current.data();
        if (handoffData.has("handoff")) {
          JsonNode old = handoffData.get("handoff");
          if (!actor.equals(old.get("actor").asText())
              || !command.reference().orElseThrow().equals(old.get("reference").asText())) {
            throw changed();
          }
          return table(current, false);
        }
        handoffData
            .putObject("handoff")
            .put("actor", actor)
            .put("time", Instant.now().toString())
            .put("reference", command.reference().orElseThrow())
            .put("run", current.receipt().runId());
        return table(replace(current, handoffData), false);
      case ABORT:
        if (!recovery || !current.data().has("handoff")) {
          throw denied();
        }
        if (current.receipt().state() == State.ABANDONED) {
          return table(current, false);
        }
        requirePending(current);
        ObjectNode aborted = current.data();
        aborted
            .put("state", "ABANDONED")
            .put("evidence", "UNRESOLVED")
            .put("reason", "ABANDONED_BY_RECOVERY")
            .put("recoveryActor", actor);
        aborted.remove("quarantineOrigin");
        return table(replace(current, aborted), false);
      case PUBLISH:
        return table(publish(ident, current, actor, privileged, recovery), false);
      default:
        throw invalidCommand();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Table loadTable(NameIdentifier ident) {
    String actor = authorization.currentActor();
    authorization.isPrivileged(ident, actor);
    DeltaRegistrationState state = refresh(ident, load(ident), actor);
    if (state.receipt().state() != State.PUBLISHED) {
      throw unpublished();
    }
    return table(state, true);
  }

  /** {@inheritDoc} */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) {
    String actor = authorization.currentActor();
    authorization.isPrivileged(NameIdentifier.of(namespace, "_list"), actor);
    List<NameIdentifier> result = new ArrayList<>();
    for (DeltaRegistrationState candidate : registrations.list(namespace)) {
      if (candidate.receipt().state() != State.PUBLISHED) {
        continue;
      }
      NameIdentifier ident = NameIdentifier.of(namespace, candidate.entity().name());
      if (refresh(ident, candidate, actor).receipt().state() == State.PUBLISHED) {
        result.add(ident);
      }
    }
    return result.toArray(new NameIdentifier[0]);
  }

  /** {@inheritDoc} */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("DELTA_REGISTRATION_RETAINED");
  }
  /** {@inheritDoc} */
  @Override
  public boolean purgeTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("DELTA_REGISTRATION_RETAINED");
  }

  private Table pendingReplay(
      NameIdentifier ident,
      DeltaRegistrationState current,
      DeltaRegistrationCommand command,
      String actor) {
    if (!actor.equals(current.actor())) {
      throw denied();
    }
    // The caller must serialize this retry after the original request has completed/drained.
    // Elapsed time and a lost response are not evidence that its owner hook has finished.
    if (current.receipt().state() != State.RESERVED
        && current.receipt().state() != State.ADOPTING) {
      throw changed();
    }
    if (command.operation() == Operation.ADOPT) {
      command =
          DeltaRegistrationCommand.adopt(
              command.token().orElseThrow(),
              config.locations().admittedExistingLocation(command.location().orElseThrow()),
              command.runId().orElseThrow(),
              command.reference().orElseThrow());
    }
    if (!command.encode().equals(current.data().get("opening").asText())
        || current.receipt().identity().generation() != 0
        || current.data().has("handoff")) {
      throw changed();
    }
    authorization.requirePendingReplayOwner(ident, actor);
    return table(current, false);
  }

  private DeltaRegistrationState beginReplace(
      DeltaRegistrationState current, DeltaRegistrationCommand command, String actor) {
    if (current.receipt().state() != State.PUBLISHED) {
      throw unpublished();
    }
    DeltaNativeObservation observed;
    try {
      observed = observe(current.location());
    } catch (IllegalArgumentException e) {
      return quarantine(current, "UNREPRESENTABLE_NATIVE_METADATA");
    }
    String inconsistency = inconsistency(current, observed);
    if (inconsistency != null) {
      return quarantine(current, inconsistency);
    }
    // A healthy base excluded by the requested contract is an operation rejection, not corruption.
    // Validate the complete fresh observation outside the store callback and quarantine catch.
    command.schemaContract().ifPresent(contract -> contract.validateBase(observed.schemaContext()));
    long generation = Math.addExact(current.receipt().identity().generation(), 1);
    String token = command.replacementToken().orElseThrow();
    if (token.equals(current.receipt().identity().token())) {
      throw changed();
    }
    DeltaRegistrationIdentity next =
        DeltaRegistrationIdentity.of(current.receipt().identity().id(), generation, token);
    DeltaOperationMarker marker =
        DeltaRegistrationState.marker(
            command, next, current.location(), observed.version(), observed.uuid());
    ObjectNode data = current.data();
    data.put("kind", "BEGIN_REPLACE")
        .put("state", "REPLACING")
        .put("generation", generation)
        .put("token", token)
        .put("run", command.runId().orElseThrow())
        .put("actor", actor)
        .put("opening", command.encode())
        .put("marker", marker.encode())
        .put("evidence", "NONE")
        .put("reason", "NONE");
    data.remove(List.of("handoff", "recoveryActor", "quarantineOrigin"));
    return registrations.replace(current, projected(current, data, observed, actor).entity());
  }

  private DeltaRegistrationState publish(
      NameIdentifier ident,
      DeltaRegistrationState current,
      String actor,
      boolean privileged,
      boolean recovery) {
    boolean handoff = current.data().has("handoff");
    if (handoff) {
      if (!recovery) {
        throw denied();
      }
    } else if (!actor.equals(current.actor())) {
      throw denied();
    }
    if (current.receipt().operation() == Operation.ADOPT && !privileged) {
      throw denied();
    }
    if (current.receipt().state() == State.PUBLISHED) {
      return current;
    }
    requirePending(current);
    if (current.receipt().state() == State.QUARANTINED && (!handoff || !recovery)) {
      throw denied();
    }
    if (current.receipt().reason().equals("NATIVE_ID_CHANGED")) {
      throw changed();
    }
    try {
      authorization.requireInitialOwner(ident);
    } catch (SecurityException e) {
      throw new IllegalStateException("OWNER_INITIALIZATION_REQUIRED");
    }
    DeltaNativeObservation observed;
    try {
      if (current.receipt().operation() == Operation.ADOPT) {
        observed = observe(current.location());
        if (!DeltaRegistrationState.observation(observed)
            .equals(current.data().get("observation"))) {
          return quarantine(current, "ADOPTION_BASELINE_CHANGED");
        }
      } else {
        DeltaOperationMarker marker = current.marker();
        DeltaCommitEvidence evidence = reader.verify(marker);
        if (evidence.kind() != DeltaCommitEvidence.Kind.MATCHED) {
          return quarantine(current, "NATIVE_EVIDENCE_AMBIGUOUS");
        }
        long version =
            marker.operation() == DeltaOperationMarker.Operation.CREATE
                ? 0
                : Math.addExact(marker.baseVersion().orElseThrow(), 1);
        observed = checked(reader.atVersion(current.location(), version), current.location());
        if (evidence.version().orElse(-1) != version
            || observed.version() != version
            || !observed.uuid().equals(evidence.uuid().orElse(null))
            || !DeltaRegistrationState.JSON
                .readTree(observed.schemaJson())
                .equals(
                    DeltaRegistrationState.JSON.readTree(evidence.schemaJson().orElseThrow()))) {
          return quarantine(current, "NATIVE_EVIDENCE_AMBIGUOUS");
        }
        if (marker.operation() == DeltaOperationMarker.Operation.REPLACE
            && marker.baseUuid().orElseThrow().equals(observed.uuid())) {
          return quarantine(current, "NATIVE_ID_CHANGED");
        }
      }
    } catch (DeltaMetadataException e) {
      return quarantine(current, "NATIVE_READ_UNAVAILABLE");
    } catch (IllegalArgumentException e) {
      return quarantine(current, "UNREPRESENTABLE_NATIVE_METADATA");
    } catch (IOException e) {
      return quarantine(current, "NATIVE_EVIDENCE_AMBIGUOUS");
    }
    ObjectNode data = current.data();
    data.put("state", "PUBLISHED")
        .put(
            "evidence",
            current.receipt().operation() == Operation.ADOPT
                ? "EXISTING_NATIVE_SNAPSHOT"
                : "EXACT_NATIVE_COMMIT")
        .put("reason", "NONE");
    data.remove("quarantineOrigin");
    if (handoff) {
      data.put("recoveryActor", actor);
    }
    DeltaRegistrationState candidate;
    try {
      candidate = projected(current, data, observed, actor);
    } catch (IllegalArgumentException e) {
      return quarantine(current, "UNREPRESENTABLE_NATIVE_METADATA");
    }
    return registrations.replace(current, candidate.entity());
  }

  private DeltaRegistrationState refresh(
      NameIdentifier ident, DeltaRegistrationState initial, String actor) {
    DeltaRegistrationState current = initial;
    for (int attempt = 0; attempt <= 3; attempt++) {
      if (current.receipt().state() != State.PUBLISHED) {
        return current;
      }
      try {
        DeltaNativeObservation observed;
        try {
          observed = observe(current.location());
        } catch (IllegalArgumentException e) {
          return quarantine(current, "UNREPRESENTABLE_NATIVE_METADATA");
        }
        String inconsistency = inconsistency(current, observed);
        if (inconsistency != null) {
          return quarantine(current, inconsistency);
        }
        if (DeltaRegistrationState.observation(observed)
            .equals(current.data().get("observation"))) {
          // An unchanged native head still needs a fresh complete row after native I/O.
          DeltaRegistrationState latest = registrations.load(ident);
          if (latest.entity().equals(current.entity())) {
            return latest;
          }
          throw changed();
        }
        DeltaRegistrationState candidate;
        try {
          candidate = projected(current, current.data(), observed, actor);
        } catch (IllegalArgumentException e) {
          return quarantine(current, "UNREPRESENTABLE_NATIVE_METADATA");
        }
        return registrations.replace(current, candidate.entity());
      } catch (OptimisticLockException e) {
        if (attempt == 3) {
          throw changed();
        }
      } catch (IllegalStateException e) {
        if (!"REGISTRATION_CHANGED".equals(e.getMessage()) || attempt == 3) {
          throw e;
        }
      }
      current = registrations.load(ident);
    }
    throw changed();
  }

  @Nullable
  private String inconsistency(DeltaRegistrationState current, DeltaNativeObservation observed) {
    JsonNode previous = current.data().get("observation");
    if (!previous.get("uuid").asText().equals(observed.uuid())) {
      return "NATIVE_ID_CHANGED";
    }
    long version = previous.get("version").longValue();
    if (observed.version() < version) {
      return "NATIVE_VERSION_REGRESSION";
    }
    if (observed.version() == version
        && !previous.equals(DeltaRegistrationState.observation(observed))) {
      return "NATIVE_METADATA_INCONSISTENT";
    }
    return null;
  }

  private DeltaRegistrationState projected(
      DeltaRegistrationState previous,
      ObjectNode data,
      DeltaNativeObservation observed,
      String actor) {
    DeltaStorageAdmission.checkObservation(observed);
    TableEntity old = previous.entity();
    AuditInfo observationAudit = newAudit(actor);
    TableEntity row =
        TableEntity.builder()
            .withId(old.id())
            .withName(old.name())
            .withNamespace(old.namespace())
            .withAuditInfo(old.auditInfo())
            .withComment(observed.description().orElse(null))
            .withColumns(DeltaColumnIdentities.project(previous, observed, ids, observationAudit))
            .withProperties(old.properties())
            .withPartitioning(
                observed.partitionColumns().stream()
                    .map(Transforms::identity)
                    .toArray(Transform[]::new))
            .withDistribution(old.distribution())
            .withSortOrders(old.sortOrders())
            .withIndexes(old.indexes())
            .build();
    data.set("observation", DeltaRegistrationState.observation(observed));
    return DeltaRegistrationState.with(row, data);
  }

  private DeltaRegistrationState quarantine(DeltaRegistrationState current, String reason) {
    ObjectNode data = current.data();
    boolean published = current.receipt().state() == State.PUBLISHED;
    data.put("state", "QUARANTINED")
        .put("reason", reason)
        .put("quarantineOrigin", published ? "PUBLISHED_REFRESH" : "PENDING_PUBLICATION");
    if (!published) {
      data.put("evidence", "UNRESOLVED");
    }
    return replace(current, data);
  }

  private DeltaRegistrationState replace(DeltaRegistrationState current, ObjectNode data) {
    return registrations.replace(
        current, DeltaRegistrationState.with(current.entity(), data).entity());
  }

  private DeltaNativeObservation observe(String location) {
    return checked(reader.latest(location), location);
  }

  private DeltaNativeObservation checked(DeltaSnapshotMetadata snapshot, String location) {
    DeltaNativeObservation observed = DeltaNativeObservation.from(snapshot);
    if (!location.equals(observed.location())) {
      throw new IllegalArgumentException("NATIVE_LOCATION_MISMATCH");
    }
    DeltaStorageAdmission.checkObservation(observed);
    return observed;
  }

  private DeltaRegistrationState load(NameIdentifier ident) {
    try {
      return registrations.load(ident);
    } catch (NoSuchEntityException e) {
      throw new NoSuchTableException("DELTA_REGISTRATION_NOT_FOUND");
    }
  }

  private Table table(DeltaRegistrationState state, boolean readable) {
    TableEntity row = state.entity();
    Map<String, String> properties = new HashMap<>(state.receipt().toProperties());
    if (readable) {
      state
          .data()
          .get("observation")
          .get("safe")
          .fields()
          .forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().asText()));
    }
    Column[] columns =
        readable
            ? row.columns().stream()
                .sorted(Comparator.comparingInt(ColumnEntity::position))
                .map(
                    column ->
                        GenericColumn.builder()
                            .withName(column.name())
                            .withType(column.dataType())
                            .withComment(column.comment())
                            .withNullable(column.nullable())
                            .withAutoIncrement(column.autoIncrement())
                            .withDefaultValue(column.defaultValue())
                            .build())
                .toArray(Column[]::new)
            : new Column[0];
    return GenericTable.builder()
        .withName(row.name())
        .withAuditInfo(row.auditInfo())
        .withComment(readable ? row.comment() : null)
        .withColumns(columns)
        .withProperties(properties)
        .withPartitioning(readable ? row.partitioning() : new Transform[0])
        .build();
  }

  private static AuditInfo newAudit(String actor) {
    return AuditInfo.builder().withCreator(actor).withCreateTime(Instant.now()).build();
  }

  private static void authorizeActor(
      DeltaRegistrationState current, String actor, boolean recovery) {
    if (!actor.equals(current.actor()) && !recovery) {
      throw denied();
    }
  }

  private static void requirePending(DeltaRegistrationState current) {
    if (!current.pendingRecovery()) {
      throw unpublished();
    }
  }

  private static SecurityException denied() {
    return new SecurityException("DELTA_AUTHORIZATION_REQUIRED");
  }

  private static IllegalStateException changed() {
    return new IllegalStateException("REGISTRATION_CHANGED");
  }

  private static IllegalStateException unpublished() {
    return new IllegalStateException("DELTA_REGISTRATION_UNPUBLISHED");
  }

  private static IllegalArgumentException invalidCommand() {
    return new IllegalArgumentException("INVALID_DELTA_REGISTRATION_COMMAND");
  }
}
