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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.storage.IdGenerator;

/**
 * Native-table-qualified logical projection identities; unmapped names imply no physical lineage.
 */
final class DeltaColumnIdentities {
  private DeltaColumnIdentities() {}

  static List<ColumnEntity> project(
      @Nullable DeltaRegistrationState prior,
      DeltaNativeObservation observed,
      IdGenerator ids,
      AuditInfo observationAudit) {
    Map<String, ColumnEntity> names = new HashMap<>();
    Map<Long, ColumnEntity> mapped = new HashMap<>();
    Map<String, Long> newMapping = mapping(observed.schemaJson());
    Map<String, Long> oldMapping = Map.of();
    if (prior != null
        && observed.uuid().equals(prior.data().path("observation").path("uuid").asText())) {
      oldMapping = mapping(prior.data().get("observation").get("schema").textValue());
      for (ColumnEntity column : prior.entity().columns()) {
        names.put(column.name(), column);
        if (oldMapping.containsKey(column.name())) {
          mapped.put(oldMapping.get(column.name()), column);
        }
      }
    }
    // Valid native identities have priority over name fallbacks, regardless of schema order.
    Set<Long> reserved = new HashSet<>();
    for (Long mappingId : newMapping.values()) {
      ColumnEntity previous = mapped.get(mappingId);
      if (previous != null) {
        reserved.add(previous.id());
      }
    }
    List<ColumnEntity> result = new ArrayList<>();
    Set<Long> used = new HashSet<>();
    for (Column column : observed.columns()) {
      Long mappingId = newMapping.get(column.name());
      ColumnEntity previous = mappingId == null ? names.get(column.name()) : mapped.get(mappingId);
      // A newly assigned/different valid mapping identity cannot reuse the old mapped identity.
      if (previous == null && mappingId != null && !oldMapping.containsKey(column.name())) {
        previous = names.get(column.name());
      }
      boolean mappedMatch = mappingId != null && mapped.containsKey(mappingId);
      if (previous != null && !mappedMatch && reserved.contains(previous.id())) {
        previous = null;
      }
      if (previous != null && !used.add(previous.id())) {
        previous = null;
      }
      result.add(
          ColumnEntity.toColumnEntity(
              column,
              result.size(),
              previous == null ? ids.nextId() : previous.id(),
              DeltaRegistrationState.audit(
                  previous == null ? observationAudit : previous.auditInfo())));
    }
    return result;
  }

  private static Map<String, Long> mapping(String schema) {
    try {
      Map<String, Long> result = new HashMap<>();
      Set<Long> seen = new HashSet<>();
      Set<Long> duplicates = new HashSet<>();
      for (JsonNode field : DeltaRegistrationState.JSON.readTree(schema).get("fields")) {
        JsonNode id = field.path("metadata").get("delta.columnMapping.id");
        if (id != null && id.isIntegralNumber() && id.canConvertToLong() && id.longValue() > 0) {
          if (!seen.add(id.longValue())) {
            duplicates.add(id.longValue());
          }
          result.put(field.get("name").textValue(), id.longValue());
        }
      }
      result.values().removeIf(duplicates::contains);
      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException("INVALID_DELTA_COLUMN_IDENTITY");
    }
  }
}
