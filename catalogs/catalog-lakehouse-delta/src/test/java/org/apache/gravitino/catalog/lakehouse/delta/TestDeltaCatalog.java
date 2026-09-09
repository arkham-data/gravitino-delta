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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.metadata.DeltaMetadataReader;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.secret.SecretBinding;
import org.apache.gravitino.secret.SecretConstants;
import org.apache.gravitino.secret.SecretReference;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class TestDeltaCatalog {
  @Test
  void productionServiceIsDiscoverableAndManagesOnlyTablesAndSchemas() {
    CatalogProvider provider =
        ServiceLoader.load(CatalogProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> p.shortName().equals("lakehouse-delta"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Missing main lakehouse-delta SPI"));
    assertEquals(
        "org.apache.gravitino.catalog.lakehouse.delta.DeltaCatalog", provider.getClass().getName());
    assertInstanceOf(BaseCatalog.class, provider);
  }

  @Test
  void capabilitiesAndCredentialBackfillAreExplicit() {
    DeltaCatalog catalog = new DeltaCatalog();
    assertFalse(catalog.shouldBackfillCredential());
    for (Capability.Scope scope : Capability.Scope.values()) {
      assertEquals(
          scope == Capability.Scope.TABLE || scope == Capability.Scope.SCHEMA,
          catalog.newCapability().managedStorage(scope).supported());
    }
    assertThrows(NullPointerException.class, () -> catalog.newCapability().managedStorage(null));
  }

  @Test
  void readerAndGlobalStoreOwnershipAreSeparate() {
    EntityStore store = mock(EntityStore.class);
    DeltaMetadataReader reader = mock(DeltaMetadataReader.class);
    DeltaCatalogOperations ops =
        new DeltaCatalogOperations(store, mock(IdGenerator.class), conf -> reader);
    ops.initialize(Map.of("delta.warehouse", "s3a://bucket/owned"), null, new DeltaCatalog());
    assertThrows(
        IllegalStateException.class,
        () ->
            ops.initialize(
                Map.of("delta.warehouse", "s3a://bucket/owned"), null, new DeltaCatalog()));
    assertTrue(
        assertThrows(
                UnsupportedOperationException.class,
                () -> ops.testConnection(NameIdentifier.of("m", "c")))
            .getMessage()
            .contains("no catalog-level connection probe"));
    ops.close();
    ops.close();
    verify(reader, times(1)).close();
    verifyNoInteractions(store);
  }

  @Test
  void failedInitializationIsClosedAndInvalidConfigDoesNotConstructReader() {
    AtomicInteger calls = new AtomicInteger();
    DeltaCatalogOperations ops =
        new DeltaCatalogOperations(
            mock(EntityStore.class),
            mock(IdGenerator.class),
            conf -> {
              calls.incrementAndGet();
              return new DeltaMetadataReader(conf);
            });
    assertThrows(
        IllegalArgumentException.class, () -> ops.initialize(Map.of(), null, new DeltaCatalog()));
    assertEquals(0, calls.get());
    assertThrows(
        IllegalStateException.class,
        () ->
            ops.initialize(
                Map.of("delta.warehouse", "s3a://bucket/owned"), null, new DeltaCatalog()));
    ops.close();
  }

  @Test
  void schemaAdmissionRejectsMissingNegativeAndUserPropertiesBeforeStore() {
    EntityStore store = mock(EntityStore.class);
    DeltaCatalogOperations ops =
        new DeltaCatalogOperations(store, mock(IdGenerator.class), DeltaMetadataReader::new);
    NameIdentifier ident = NameIdentifier.of("m", "c", "s");
    assertThrows(IllegalArgumentException.class, () -> ops.createSchema(ident, null, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> ops.createSchema(ident, null, Map.of(StringIdentifier.ID_KEY, "-1")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.createSchema(
                ident,
                null,
                Map.of(
                    StringIdentifier.ID_KEY,
                    StringIdentifier.fromId(1).toString(),
                    "location",
                    "s3a://bucket/root")));
    verifyNoInteractions(store);
    ops.close();
  }

  @Test
  void schemaCommentTransportRejectsSecretAndInvalidValuesBeforeStorage() {
    EntityStore store = mock(EntityStore.class);
    DeltaCatalogOperations ops =
        new DeltaCatalogOperations(store, mock(IdGenerator.class), DeltaMetadataReader::new);
    NameIdentifier ident = NameIdentifier.of("m", "c", "s");
    for (String value : List.of("", " ", "******", SecretConstants.URN_PREFIX + "memory:comment")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> ops.alterSchema(ident, SchemaChange.setProperty("comment", value)));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterSchema(
                ident,
                SchemaChange.setSecretBinding("comment", new SecretBinding("memory", "secret"))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterSchema(
                ident,
                SchemaChange.setProperty("comment", "valid"),
                SchemaChange.removeProperty("location")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.alterSchema(
                ident,
                SchemaChange.setSecretReference(
                    "comment", new SecretReference("memory", Map.of("key", "secret")))));
    verifyNoInteractions(store);
    ops.close();
  }

  @Test
  void applicationProvidesSingleLoggingBindingAndBridge() throws Exception {
    Class<?> binding = LoggerFactory.getILoggerFactory().getClass();
    assertEquals("org.apache.logging.slf4j.Log4jLoggerFactory", binding.getName());
    Class<?> bridge = Class.forName("org.apache.log4j.Logger");
    assertTrue(
        bridge
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toString()
            .contains("log4j-1.2-api"));
    LoggerFactory.getLogger(TestDeltaCatalog.class)
        .info(
            "Logging binding {} from {}; Log4j1 bridge {}",
            binding.getName(),
            binding.getProtectionDomain().getCodeSource().getLocation(),
            bridge.getProtectionDomain().getCodeSource().getLocation());
  }
}
