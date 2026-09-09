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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.SupportsSchemas;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestDeltaNamespaces {
  private final Catalog metadata = mock(Catalog.class),
      first = mock(Catalog.class),
      second = mock(Catalog.class);
  private final SupportsSchemas reads = mock(SupportsSchemas.class),
      one = mock(SupportsSchemas.class),
      two = mock(SupportsSchemas.class);
  private final AtomicReference<Catalog> actor = new AtomicReference<>(first);
  private GravitinoDeltaCatalogSpark35 adapter;

  @BeforeEach
  void setup() {
    when(metadata.asSchemas()).thenReturn(reads);
    when(first.asSchemas()).thenReturn(one);
    when(second.asSchemas()).thenReturn(two);
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(session.conf()).thenReturn(conf);
    when(conf.get(anyString(), anyString())).thenAnswer(call -> call.getArgument(1));
    when(conf.get("spark.sql.catalog.spark_catalog", ""))
        .thenReturn(DeltaCatalogProperties.SESSION_CATALOG);
    when(conf.get("spark.sql.extensions", "")).thenReturn(DeltaCatalogProperties.EXTENSION);
    adapter =
        new GravitinoDeltaCatalogSpark35(
            mock(DeltaCatalog.class),
            new DeltaRegistrationClient(() -> metadata, actor::get),
            ignored -> {},
            () -> session,
            (location, receipt) -> {});
    adapter.initialize("alias", new CaseInsensitiveStringMap(Map.of()));
  }

  @Test
  void delegatesOneLevelReadsThroughMetadataSupplier() {
    when(reads.listSchemas()).thenReturn(new String[] {"db"});
    Schema schema = mock(Schema.class);
    when(schema.properties()).thenReturn(Map.of());
    when(schema.comment()).thenReturn("visible comment");
    when(reads.loadSchema("db")).thenReturn(schema);
    assertArrayEquals(new String[] {"db"}, adapter.listNamespaces()[0]);
    assertEquals(0, adapter.listNamespaces(new String[] {"db"}).length);
    assertEquals(
        Map.of("comment", "visible comment"), adapter.loadNamespaceMetadata(new String[] {"db"}));
    verifyNoInteractions(one, two);
    assertThrows(
        NoSuchNamespaceException.class,
        () -> adapter.loadNamespaceMetadata(new String[] {"nested", "db"}));
  }

  @Test
  void namespaceMutationResolvesCurrentActorAndPreservesCommentTransport() {
    List<SchemaChange> changes = new ArrayList<>();
    when(one.alterSchema(anyString(), any()))
        .thenAnswer(
            call -> {
              changes.add(call.getArgument(1));
              return mock(Schema.class);
            });
    when(two.alterSchema(anyString(), any()))
        .thenAnswer(
            call -> {
              changes.add(call.getArgument(1));
              return mock(Schema.class);
            });
    adapter.alterNamespace(new String[] {"db"}, NamespaceChange.setProperty("comment", "first"));
    actor.set(second);
    adapter.alterNamespace(new String[] {"db"}, NamespaceChange.removeProperty("comment"));
    assertEquals("first", ((SchemaChange.SetProperty) changes.get(0)).getValue());
    assertEquals("comment", ((SchemaChange.RemoveProperty) changes.get(1)).getProperty());
    verifyNoInteractions(reads);
  }

  @Test
  void completeNamespaceChangesAreValidatedBeforeMutation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            adapter.alterNamespace(
                new String[] {"db"},
                NamespaceChange.setProperty("comment", "valid"),
                NamespaceChange.setProperty("location", "file:/wrong")));
    verifyNoInteractions(one);
    for (String invalid : new String[] {" ", "******", "urn:gravitino-secret:untrusted"}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              adapter.alterNamespace(
                  new String[] {"db"}, NamespaceChange.setProperty("comment", invalid)));
    }
    verifyNoInteractions(one);
  }

  @Test
  void cascadeAndNonemptyPolicyStayWithProviderAndLifecycleRejectsEarly() {
    when(one.dropSchema("db", false))
        .thenThrow(new org.apache.gravitino.exceptions.NonEmptySchemaException("pending rows"));
    when(one.dropSchema("db", true)).thenReturn(true);
    assertThrows(
        NonEmptyNamespaceException.class, () -> adapter.dropNamespace(new String[] {"db"}, false));
    assertTrue(adapter.dropNamespace(new String[] {"db"}, true));
    Identifier id = Identifier.of(new String[] {"db"}, "t");
    assertThrows(UnsupportedOperationException.class, () -> adapter.dropTable(id));
    assertThrows(UnsupportedOperationException.class, () -> adapter.purgeTable(id));
    assertThrows(UnsupportedOperationException.class, () -> adapter.renameTable(id, id));
  }
}
