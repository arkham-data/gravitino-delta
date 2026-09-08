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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

class TestDeltaRegistrationState {
  static DeltaRegistrationState reserved() {
    return DeltaRegistrationState.initial(
        TestDeltaRegistrationOperations.IDENT,
        10L,
        DeltaRegistrationCommand.reserve("token", "run-a", TestDeltaRegistrationOperations.SCHEMA),
        "alice",
        "s3a://bucket/fixture/s/10",
        TestDeltaRegistrationOperations.AUDIT,
        null);
  }

  @Test
  void rejectsLegacyAndCorruptStateBeforeUse() {
    TableEntity valid = reserved().entity();
    valid.properties().clear();
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(valid));
    for (String[] mutation :
        List.of(
            new String[] {"RESERVED", "PUBLISHED"},
            new String[] {"RESERVE", "ADOPT"},
            new String[] {"CREATE", "REPLACE"},
            new String[] {"token", "other"})) {
      TableEntity corrupt = reserved().entity();
      String key = DeltaRegistrationState.PROPERTY;
      corrupt
          .properties()
          .put(key, corrupt.properties().get(key).replaceFirst(mutation[0], mutation[1]));
      assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(corrupt));
    }
  }

  @Test
  void detachesNestedTypesPartitionsAuditAndRetainedBuilders() {
    TableEntity original = reserved().entity();
    Types.StructType nested =
        Types.StructType.of(Types.StructType.Field.nullableField("child", Types.IntegerType.get()));
    AuditInfo.Builder audit =
        AuditInfo.builder()
            .withCreator("creator")
            .withCreateTime(TestDeltaRegistrationOperations.AUDIT.createTime());
    ColumnEntity.Builder column =
        ColumnEntity.builder()
            .withId(101L)
            .withName("nested")
            .withPosition(0)
            .withDataType(nested)
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .withAuditInfo(audit.build());
    List<ColumnEntity> columns = new ArrayList<>(List.of(column.build()));
    Transform[] partitions = {Transforms.identity("nested")};
    TableEntity.Builder table =
        TableEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(original.namespace())
            .withColumns(columns)
            .withProperties(original.properties())
            .withPartitioning(partitions)
            .withAuditInfo(original.auditInfo());
    DeltaRegistrationState state = DeltaRegistrationState.from(table.build());
    nested.fields()[0] = Types.StructType.Field.nullableField("mutated", Types.StringType.get());
    audit.withCreator("mutated");
    column.withName("mutated");
    table.withComment("mutated");
    columns.clear();
    partitions[0] = Transforms.identity("mutated");
    TableEntity returned = state.entity();
    ((Types.StructType) returned.columns().get(0).dataType()).fields()[0] =
        Types.StructType.Field.nullableField("returned", Types.StringType.get());
    returned.partitioning()[0] = Transforms.identity("returned");
    TableEntity detached = state.entity();
    assertEquals("nested", detached.columns().get(0).name());
    assertEquals("creator", detached.columns().get(0).auditInfo().creator());
    assertEquals(
        "child", ((Types.StructType) detached.columns().get(0).dataType()).fields()[0].name());
    assertEquals("nested", detached.partitioning()[0].arguments()[0].toString());
  }

  @Test
  void rejectsNoncontiguousAndDuplicateColumnPositions() {
    for (int position : List.of(-1, 1, 2)) {
      TableEntity row = reserved().entity();
      row.columns()
          .add(
              ColumnEntity.builder()
                  .withId(101L)
                  .withName("id")
                  .withPosition(position)
                  .withDataType(Types.IntegerType.get())
                  .withAuditInfo(TestDeltaRegistrationOperations.AUDIT)
                  .build());
      assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row));
    }
  }

  @Test
  void duplicatePositionsNeverBecomeAnOrderedPublicSchema() {
    TableEntity row = reserved().entity();
    for (long id : List.of(101L, 102L))
      row.columns()
          .add(
              ColumnEntity.builder()
                  .withId(id)
                  .withName("c" + id)
                  .withPosition(0)
                  .withDataType(Types.IntegerType.get())
                  .withAuditInfo(TestDeltaRegistrationOperations.AUDIT)
                  .build());
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row));
  }

  @Test
  void rejectsTrailingJsonAndIdentityBoundMarkerCorruption() {
    TableEntity trailing = reserved().entity();
    trailing
        .properties()
        .put(
            DeltaRegistrationState.PROPERTY,
            trailing.properties().get(DeltaRegistrationState.PROPERTY) + " {}");
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(trailing));
    DeltaRegistrationState original = reserved();
    ObjectNode mismatched =
        original
            .data()
            .put(
                "marker",
                DeltaOperationMarker.create(
                        10, 0, "other", original.location(), TestDeltaRegistrationOperations.SCHEMA)
                    .encode());
    TableEntity corrupt = original.entity();
    corrupt.properties().put(DeltaRegistrationState.PROPERTY, mismatched.toString());
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(corrupt));
  }

  @Test
  void quarantineOriginCannotInventPublishedEvidenceOrRecoveryAuthority() {
    for (String origin : List.of("PUBLISHED_REFRESH", "arbitrary")) {
      TableEntity row = reserved().entity();
      ObjectNode data =
          reserved()
              .data()
              .put("state", "QUARANTINED")
              .put("quarantineOrigin", origin)
              .put("evidence", "UNRESOLVED")
              .put("reason", "NATIVE_READ_UNAVAILABLE");
      row.properties().put(DeltaRegistrationState.PROPERTY, data.toString());
      assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row));
    }
  }

  @Test
  void copiesEveryAdmittedExpressionDistributionSortIndexAndAuditField() {
    TableEntity original = reserved().entity();
    String[] field = {"nested", "leaf"};
    Expression[] arguments = {NamedReference.field(field)};
    Expression function = FunctionExpression.of("compute", arguments);
    ColumnEntity column =
        ColumnEntity.builder()
            .withId(101L)
            .withName("id")
            .withPosition(0)
            .withDataType(Types.IntegerType.get())
            .withNullable(false)
            .withAutoIncrement(true)
            .withDefaultValue(function)
            .withComment("")
            .withAuditInfo(TestDeltaRegistrationOperations.AUDIT)
            .build();
    SortOrder[] sorts = {SortOrders.ascending(NamedReference.field(field))};
    String[][] indexFields = {field};
    Map<String, String> indexProperties = new HashMap<>(Map.of("type", "btree"));
    Index[] indexes = {Indexes.primary("idx", indexFields, indexProperties)};
    TableEntity source =
        TableEntity.builder()
            .withId(original.id())
            .withName(original.name())
            .withNamespace(original.namespace())
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .withColumns(List.of(column))
            .withComment("")
            .withDistribution(Distributions.hash(4, arguments))
            .withSortOrders(sorts)
            .withIndexes(indexes)
            .build();
    DeltaRegistrationState state = DeltaRegistrationState.from(source);
    TableEntity expected = state.entity();
    field[0] = "mutated";
    arguments[0] = NamedReference.field("mutated");
    sorts[0] = SortOrders.descending(NamedReference.field("mutated"));
    indexProperties.put("type", "mutated");
    indexes[0] = Indexes.primary("mutated", new String[][] {{"mutated"}});
    assertEquals(expected, state.entity());
    assertEquals("", state.entity().comment());
    assertEquals("", state.entity().columns().get(0).comment());
    assertEquals(
        FunctionExpression.of("compute", NamedReference.field(new String[] {"nested", "leaf"})),
        state.entity().columns().get(0).defaultValue());
  }

  @Test
  void outerEscapedPropertiesAreBoundedAfterInternalEncoding() {
    TableEntity row = reserved().entity();
    ObjectNode data = reserved().data().put("actor", "\\".repeat(5 * 1024 * 1024));
    row.properties().put(DeltaRegistrationState.PROPERTY, data.toString());
    assertThrows(IllegalArgumentException.class, () -> DeltaRegistrationState.from(row));
  }

  @Test
  void rejectsNonCanonicalOrNonS3StoredLocations() {
    DeltaRegistrationState original = reserved();
    for (String location :
        List.of(
            "file:///tmp/not-a-provider-location",
            "s3://bucket/fixture/s/10",
            "s3a://bucket/fixture/../outside")) {
      TableEntity row = original.entity();
      ObjectNode data =
          original
              .data()
              .put("location", location)
              .put(
                  "marker",
                  DeltaOperationMarker.create(
                          10, 0, "token", location, TestDeltaRegistrationOperations.SCHEMA)
                      .encode());
      row.properties().put(DeltaRegistrationState.PROPERTY, data.toString());
      assertThrows(
          IllegalArgumentException.class, () -> DeltaRegistrationState.from(row), location);
    }
  }

  @Test
  void detachesSourceAndReturnedPropertiesAndBuilders() {
    TableEntity source = reserved().entity();
    DeltaRegistrationState state = DeltaRegistrationState.from(source);
    source.properties().clear();
    state.entity().properties().clear();
    assertEquals("token", state.receipt().identity().token());
    assertEquals(10L, state.entity().id());
    assertFalse(state.entity().properties().isEmpty());
  }
}
