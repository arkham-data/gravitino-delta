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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.Evidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.apache.gravitino.dto.rel.DistributionDTO;
import org.apache.gravitino.dto.rel.SortOrderDTO;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** Validated, detached complete row; the internal codec is never a request property. */
final class DeltaRegistrationState {
  static final String PROPERTY = "delta.registration.internal.state";
  static final ObjectMapper JSON =
      new ObjectMapper()
          .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final Set<String> REQUIRED =
      Set.of(
          "version",
          "kind",
          "state",
          "id",
          "generation",
          "token",
          "run",
          "actor",
          "location",
          "opening",
          "evidence",
          "reason");
  private static final Set<String> OPTIONAL =
      Set.of("marker", "observation", "handoff", "recoveryActor", "quarantineOrigin");
  private final TableEntity row;
  private final ObjectNode data;
  private final DeltaRegistrationReceipt receipt;

  private DeltaRegistrationState(TableEntity source) {
    try {
      DeltaStorageAdmission.checkTableCandidate(source);
      if (source.properties().size() != 1 || !source.properties().containsKey(PROPERTY))
        throw invalid();
      JsonNode parsed = JSON.readTree(source.properties().get(PROPERTY));
      if (!(parsed instanceof ObjectNode)) throw invalid();
      data = (ObjectNode) parsed;
      fields(data, REQUIRED, OPTIONAL);
      if (number(data, "version") != 1 || number(data, "id") != source.id()) throw invalid();
      Operation kind = Operation.valueOf(text(data, "kind"));
      State state = State.valueOf(text(data, "state"));
      Evidence evidence = Evidence.valueOf(text(data, "evidence"));
      DeltaRegistrationIdentity identity =
          DeltaRegistrationIdentity.of(
              source.id(), number(data, "generation"), text(data, "token"));
      text(data, "actor");
      String location = text(data, "location");
      // Commands already enforce catalog roots. Retained rows must independently retain the
      // provider's canonical S3 transport shape before any later native read.
      if (!location.equals(
          new DeltaLocationPolicy(location, List.of(location)).admittedExistingLocation(location)))
        throw invalid();
      DeltaRegistrationCommand opening = DeltaRegistrationCommand.decode(text(data, "opening"));
      if (opening.operation() != kind || !opening.runId().orElseThrow().equals(text(data, "run")))
        throw invalid();
      if (kind == Operation.BEGIN_REPLACE) {
        DeltaRegistrationIdentity old = opening.expected().orElseThrow();
        if (old.id() != source.id()
            || Math.addExact(old.generation(), 1) != identity.generation()
            || !opening.replacementToken().orElseThrow().equals(identity.token())
            || old.token().equals(identity.token())) throw invalid();
      } else if (identity.generation() != 0
          || !opening.token().orElseThrow().equals(identity.token())) throw invalid();
      DeltaOperationMarker marker = null;
      if (kind == Operation.ADOPT) {
        if (data.has("marker") || !location.equals(opening.location().orElseThrow()))
          throw invalid();
      } else {
        marker = DeltaOperationMarker.decode(text(data, "marker"));
        if (marker.id() != source.id()
            || marker.generation() != identity.generation()
            || !marker.token().equals(identity.token())
            || !marker.normalizedLocation().equals(location)) throw invalid();
        DeltaOperationMarker expected =
            marker(
                opening,
                identity,
                location,
                marker.baseVersion().orElse(-1),
                marker.baseUuid().orElse(""));
        if (!expected.encode().equals(marker.encode())) throw invalid();
      }
      boolean pendingOrigin =
          state == State.QUARANTINED
              && "PENDING_PUBLICATION".equals(text(data, "quarantineOrigin"));
      if (state == State.QUARANTINED) {
        String origin = text(data, "quarantineOrigin");
        if (!origin.equals("PENDING_PUBLICATION") && !origin.equals("PUBLISHED_REFRESH"))
          throw invalid();
        if (pendingOrigin != (evidence == Evidence.UNRESOLVED)) throw invalid();
      } else if (data.has("quarantineOrigin")) throw invalid();
      if (data.has("handoff")) {
        JsonNode handoff = data.get("handoff");
        fields(handoff, Set.of("actor", "time", "reference", "run"), Set.of());
        text(handoff, "actor");
        text(handoff, "reference");
        Instant.parse(text(handoff, "time"));
        if (!text(handoff, "run").equals(text(data, "run"))) throw invalid();
      }
      if (data.has("recoveryActor")) {
        text(data, "recoveryActor");
        if (!data.has("handoff")) throw invalid();
      }
      if (state == State.ABANDONED && (!data.has("handoff") || !data.has("recoveryActor")))
        throw invalid();
      boolean published = state == State.PUBLISHED;
      JsonNode observation = data.get("observation");
      if (observation != null) validateObservation(observation, location);
      if ((kind == Operation.ADOPT
              || kind == Operation.BEGIN_REPLACE
              || published
              || state == State.QUARANTINED && !pendingOrigin)
          && observation == null) throw invalid();
      if (kind == Operation.BEGIN_REPLACE) {
        boolean completed = evidence == Evidence.EXACT_NATIVE_COMMIT;
        long nativeVersion = number(observation, "version");
        String nativeId = text(observation, "uuid");
        if (completed) {
          if (nativeVersion <= marker.baseVersion().orElseThrow()
              || nativeId.equals(marker.baseUuid().orElseThrow())) throw invalid();
        } else if (nativeVersion != marker.baseVersion().orElseThrow()
            || !nativeId.equals(marker.baseUuid().orElseThrow())) throw invalid();
      }
      receipt =
          DeltaRegistrationReceipt.of(
              identity,
              kind,
              state,
              text(data, "run"),
              evidence,
              text(data, "reason"),
              marker != null
                      && (state == State.RESERVED || state == State.REPLACING || pendingOrigin)
                  ? Optional.of(marker)
                  : Optional.empty(),
              published ? Optional.of(location) : Optional.empty(),
              published ? Optional.of(text(observation, "uuid")) : Optional.empty(),
              published ? OptionalLong.of(number(observation, "version")) : OptionalLong.empty());
      Set<Integer> positions = new HashSet<>();
      Set<Long> ids = new HashSet<>();
      Set<String> names = new HashSet<>();
      for (ColumnEntity column : source.columns()) {
        if (column.position() < 0
            || column.position() >= source.columns().size()
            || !positions.add(column.position())
            || !ids.add(column.id())
            || !names.add(column.name())) throw invalid();
      }
      row = detach(source);
    } catch (Exception e) {
      throw invalid();
    }
  }

  static DeltaRegistrationState from(TableEntity row) {
    return new DeltaRegistrationState(row);
  }

  TableEntity entity() {
    return detach(row);
  }

  DeltaRegistrationReceipt receipt() {
    return receipt;
  }

  ObjectNode data() {
    return data.deepCopy();
  }

  String actor() {
    return text(data, "actor");
  }

  String location() {
    return text(data, "location");
  }

  boolean pendingRecovery() {
    State state = receipt.state();
    return state == State.RESERVED
        || state == State.ADOPTING
        || state == State.REPLACING
        || state == State.QUARANTINED
            && "PENDING_PUBLICATION".equals(data.path("quarantineOrigin").asText());
  }

  DeltaOperationMarker marker() {
    return DeltaOperationMarker.decode(text(data, "marker"));
  }

  static DeltaRegistrationState initial(
      NameIdentifier ident,
      long id,
      DeltaRegistrationCommand command,
      String actor,
      String location,
      AuditInfo audit,
      @Nullable DeltaNativeObservation observed) {
    boolean adopt = command.operation() == Operation.ADOPT;
    ObjectNode data =
        JSON.createObjectNode()
            .put("version", 1)
            .put("kind", command.operation().name())
            .put("state", adopt ? "ADOPTING" : "RESERVED")
            .put("id", id)
            .put("generation", 0)
            .put("token", command.token().orElseThrow())
            .put("run", command.runId().orElseThrow())
            .put("actor", actor)
            .put("location", location)
            .put("opening", command.encode())
            .put("evidence", "NONE")
            .put("reason", "NONE");
    if (!adopt)
      data.put(
          "marker",
          marker(
                  command,
                  DeltaRegistrationIdentity.of(id, 0, command.token().orElseThrow()),
                  location,
                  -1,
                  "")
              .encode());
    if (observed != null) data.set("observation", observation(observed));
    TableEntity row =
        TableEntity.builder()
            .withId(id)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withAuditInfo(audit)
            .build();
    return with(row, data);
  }

  static DeltaRegistrationState with(TableEntity row, ObjectNode data) {
    DeltaStorageAdmission.checkJson(data, DeltaStorageAdmission.MEDIUM_TEXT_BYTES);
    TableEntity copy = detach(row);
    copy.properties().clear();
    copy.properties().put(PROPERTY, data.toString());
    DeltaStorageAdmission.checkTableCandidate(copy);
    return from(copy);
  }

  static ObjectNode observation(DeltaNativeObservation value) {
    ObjectNode result =
        JSON.createObjectNode()
            .put("location", value.location())
            .put("version", value.version())
            .put("uuid", value.uuid())
            .put("schema", value.schemaJson())
            .put("digest", value.configurationDigest())
            .put("markerProvenance", value.markerProvenance());
    result.set("partitions", JSON.valueToTree(value.partitionColumns()));
    result.set("safe", JSON.valueToTree(value.safeConfiguration()));
    ObjectNode protocol =
        result
            .putObject("protocol")
            .put("reader", value.protocol().minReaderVersion())
            .put("writer", value.protocol().minWriterVersion());
    protocol.set(
        "readerFeatures", JSON.valueToTree(new TreeSet<>(value.protocol().readerFeatures())));
    protocol.set(
        "writerFeatures", JSON.valueToTree(new TreeSet<>(value.protocol().writerFeatures())));
    value.description().ifPresent(v -> result.put("description", v));
    if (value.createdTime().isPresent()) result.put("createdTime", value.createdTime().getAsLong());
    value.markerDigest().ifPresent(v -> result.put("markerDigest", v));
    // Normalize numeric nodes exactly as the durable JSON decoder does. IntNode and LongNode
    // compare unequal even when they encode the same native version or creation timestamp.
    try {
      return (ObjectNode) JSON.readTree(result.toString());
    } catch (IOException e) {
      throw invalid();
    }
  }

  static DeltaOperationMarker marker(
      DeltaRegistrationCommand command,
      DeltaRegistrationIdentity identity,
      String location,
      long baseVersion,
      String baseUuid) {
    String schema =
        command.schemaContract().isPresent()
            ? command.schemaContract().orElseThrow().requiredSchemaJson()
            : command.exactSchemaJson().orElseThrow();
    DeltaStorageAdmission.checkDesiredSchema(schema);
    if (command.operation() == Operation.RESERVE)
      return command.schemaContract().isPresent()
          ? DeltaOperationMarker.createWithSchemaContract(
              identity.id(),
              identity.generation(),
              identity.token(),
              location,
              command.schemaContract().orElseThrow())
          : DeltaOperationMarker.create(
              identity.id(), identity.generation(), identity.token(), location, schema);
    return command.schemaContract().isPresent()
        ? DeltaOperationMarker.replaceWithSchemaContract(
            identity.id(),
            identity.generation(),
            identity.token(),
            location,
            command.schemaContract().orElseThrow(),
            baseVersion,
            baseUuid)
        : DeltaOperationMarker.replace(
            identity.id(),
            identity.generation(),
            identity.token(),
            location,
            schema,
            baseVersion,
            baseUuid);
  }

  static AuditInfo audit(Audit source) {
    return AuditInfo.builder()
        .withCreator(source.creator())
        .withCreateTime(source.createTime())
        .withLastModifier(source.lastModifier())
        .withLastModifiedTime(source.lastModifiedTime())
        .build();
  }

  static TableEntity detach(TableEntity source) {
    try {
      List<ColumnEntity> columns = new ArrayList<>();
      for (ColumnEntity column : source.columns()) {
        Expression expression = column.defaultValue();
        Expression copied =
            expression == null
                ? null
                : expression.equals(Column.DEFAULT_VALUE_NOT_SET)
                    ? Column.DEFAULT_VALUE_NOT_SET
                    : DTOConverters.fromFunctionArg(
                        (FunctionArg)
                            copy(DTOConverters.toFunctionArg(expression), Expression.class));
        columns.add(
            ColumnEntity.builder()
                .withId(column.id())
                .withName(column.name())
                .withPosition(column.position())
                .withDataType(copy(column.dataType(), Type.class))
                .withComment(column.comment())
                .withNullable(column.nullable())
                .withAutoIncrement(column.autoIncrement())
                .withDefaultValue(copied)
                .withAuditInfo(audit(column.auditInfo()))
                .build());
      }
      return TableEntity.builder()
          .withId(source.id())
          .withName(source.name())
          .withNamespace(source.namespace())
          .withAuditInfo(audit(source.auditInfo()))
          .withColumns(columns)
          .withProperties(new HashMap<>(source.properties()))
          .withComment(source.comment())
          .withPartitioning(
              DTOConverters.fromDTOs(
                  copy(DTOConverters.toDTOs(source.partitioning()), Partitioning[].class)))
          .withDistribution(
              DTOConverters.fromDTO(
                  copy(DTOConverters.toDTO(source.distribution()), DistributionDTO.class)))
          .withSortOrders(
              DTOConverters.fromDTOs(
                  copy(DTOConverters.toDTOs(source.sortOrders()), SortOrderDTO[].class)))
          .withIndexes(
              DTOConverters.fromDTOs(
                  copy(DTOConverters.toDTOs(source.indexes()), IndexDTO[].class)))
          .build();
    } catch (Exception e) {
      throw invalid();
    }
  }

  static String text(JsonNode node, String field) {
    JsonNode value = node == null ? null : node.get(field);
    if (value == null || !value.isTextual() || value.textValue().isBlank()) throw invalid();
    return value.textValue();
  }

  private static <T> T copy(Object source, Class<T> type) throws Exception {
    return JsonUtils.anyFieldMapper()
        .readValue(JsonUtils.anyFieldMapper().writeValueAsBytes(source), type);
  }

  private static long number(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null
        || !value.isIntegralNumber()
        || !value.canConvertToLong()
        || value.longValue() < 0) throw invalid();
    return value.longValue();
  }

  private static void fields(JsonNode node, Set<String> required, Set<String> optional) {
    if (node == null || !node.isObject()) throw invalid();
    for (String field : required) if (!node.hasNonNull(field)) throw invalid();
    node.fieldNames()
        .forEachRemaining(
            field -> {
              if (!required.contains(field) && !optional.contains(field)) throw invalid();
            });
  }

  private static void validateObservation(JsonNode node, String location) throws Exception {
    fields(
        node,
        Set.of(
            "location",
            "version",
            "uuid",
            "schema",
            "digest",
            "markerProvenance",
            "partitions",
            "safe",
            "protocol"),
        Set.of("description", "createdTime", "markerDigest"));
    if (!location.equals(text(node, "location"))
        || !text(node, "digest").matches("sha256-v1:[0-9a-f]{64}")) throw invalid();
    number(node, "version");
    text(node, "uuid");
    if (!JSON.readTree(text(node, "schema")).isObject()
        || !node.get("partitions").isArray()
        || !node.get("safe").isObject()) throw invalid();
    if (node.has("description") && !node.get("description").isTextual()
        || node.has("createdTime")
            && (!node.get("createdTime").isIntegralNumber()
                || !node.get("createdTime").canConvertToLong())) throw invalid();
    String provenance = text(node, "markerProvenance");
    if (!Set.of("ABSENT", "VALID_V1", "VALID_V2").contains(provenance)
        || provenance.equals("ABSENT") == node.has("markerDigest")) throw invalid();
    if (node.has("markerDigest") && !text(node, "markerDigest").matches("sha256-v1:[0-9a-f]{64}"))
      throw invalid();
    JsonNode protocol = node.get("protocol");
    fields(protocol, Set.of("reader", "writer", "readerFeatures", "writerFeatures"), Set.of());
    if (number(protocol, "reader") < 1
        || number(protocol, "writer") < 1
        || !protocol.get("readerFeatures").isArray()
        || !protocol.get("writerFeatures").isArray()) throw invalid();
    Set<String> safe =
        Set.of(
            "delta.columnMapping.mode",
            "delta.columnMapping.maxColumnId",
            "delta.appendOnly",
            "delta.enableChangeDataFeed",
            "delta.enableDeletionVectors");
    node.get("safe")
        .fields()
        .forEachRemaining(
            entry -> {
              if (!safe.contains(entry.getKey()) || !entry.getValue().isTextual()) throw invalid();
              String key = entry.getKey();
              String value = entry.getValue().textValue();
              if (key.equals("delta.columnMapping.mode")) {
                if (!Set.of("none", "name", "id").contains(value)) throw invalid();
              } else if (key.equals("delta.columnMapping.maxColumnId")) {
                if (!value.matches("0|[1-9][0-9]*") || Long.parseLong(value) < 0) throw invalid();
              } else if (!value.equals("true") && !value.equals("false")) throw invalid();
            });
  }

  private static IllegalArgumentException invalid() {
    return new IllegalArgumentException("INVALID_DELTA_REGISTRATION_STATE");
  }
}
