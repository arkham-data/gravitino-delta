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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.PropertiesMetadataHelpers;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class TestDeltaCatalogConfig {
  @Test
  void credentialsAndFreshDefensiveConfiguration() {
    Map<String, String> values =
        new HashMap<>(
            Map.of(
                "delta.warehouse",
                "s3://bucket/root",
                "delta.storage-credential-mode",
                "session",
                "s3-access-key-id",
                "fixture-key",
                "s3-secret-access-key",
                "fixture-secret",
                "s3-session-token",
                "fixture-token",
                "s3-endpoint",
                "http://localhost:9000",
                "s3-path-style-access",
                "true"));
    DeltaCatalogConfig config = DeltaCatalogConfig.from(values);
    Configuration first = config.storageConfiguration();
    assertEquals(
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        first.get("fs.s3a.aws.credentials.provider"));
    assertEquals("fixture-token", first.get("fs.s3a.session.token"));
    assertEquals("false", first.get("fs.s3a.impl.disable.cache"));
    first.set("fs.s3a.access.key", "mutated");
    values.clear();
    assertEquals("fixture-key", config.storageConfiguration().get("fs.s3a.access.key"));
    assertNull(config.storageConfiguration().get("fs.defaultFS"));
  }

  @Test
  void failsClosedOnMissingPartialAndConflictingCredentials() {
    assertThrows(IllegalArgumentException.class, () -> DeltaCatalogConfig.from(Map.of()));
    for (Map<String, String> extra :
        List.of(
            Map.of("s3-access-key-id", "fixture"),
            Map.of("delta.storage-credential-mode", "static"),
            Map.of(
                "delta.storage-credential-mode",
                "session",
                "s3-access-key-id",
                "fixture",
                "s3-secret-access-key",
                "fixture"),
            Map.of("s3-session-token", "fixture"),
            Map.of("delta.storage-credential-mode", "arbitrary"),
            Map.of("fs.s3a.aws.credentials.provider", "arbitrary"),
            Map.of("s3-endpoint", "https://user@host"),
            Map.of("delta.adoption-roots", "[1]"),
            Map.of("delta.recovery-principals", "[] []"))) {
      Map<String, String> properties = new HashMap<>(extra);
      properties.put("delta.warehouse", "s3a://bucket/root");
      assertThrows(IllegalArgumentException.class, () -> DeltaCatalogConfig.from(properties));
    }
  }

  @Test
  void exactProviderModesAndExplicitTrustedEndpoints() {
    for (String endpoint :
        List.of("http://localhost:9000", "http://minio:9000", "https://s3.example.com")) {
      Configuration storage =
          DeltaCatalogConfig.from(
                  Map.of("delta.warehouse", "s3a://bucket/root", "s3-endpoint", endpoint))
              .storageConfiguration();
      assertEquals(endpoint, storage.get("fs.s3a.endpoint"));
      assertEquals(
          "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
          storage.get("fs.s3a.aws.credentials.provider"));
    }
    Configuration storage =
        DeltaCatalogConfig.from(
                Map.of(
                    "delta.warehouse",
                    "s3a://bucket/root",
                    "delta.storage-credential-mode",
                    "static",
                    "s3-access-key-id",
                    "fixture",
                    "s3-secret-access-key",
                    "fixture"))
            .storageConfiguration();
    assertEquals(
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        storage.get("fs.s3a.aws.credentials.provider"));
    assertNull(storage.get("fs.s3a.endpoint"));
    assertNull(storage.get("fs.s3a.session.token"));
    assertDoesNotThrow(
        () ->
            DeltaCatalogConfig.from(
                Map.of(
                    "delta.warehouse",
                    "s3a://bucket/root",
                    Catalog.AUTHORIZATION_PROVIDER,
                    "trusted-provider")));
  }

  @Test
  void strictArraysRecoveryAndRejectedSettingsAreSanitized() {
    DeltaCatalogConfig config =
        DeltaCatalogConfig.from(
            Map.of(
                "delta.warehouse",
                "s3a://bucket/root",
                "delta.adoption-roots",
                "[\"s3://bucket/adopt/\"]",
                "delta.recovery-principals",
                "[\"operator\"]"));
    assertEquals(Set.of("operator"), config.recoveryPrincipals());
    assertThrows(
        UnsupportedOperationException.class, () -> config.recoveryPrincipals().add("other"));
    assertEquals(
        "s3a://bucket/adopt/table",
        config.locations().admittedExistingLocation("s3a://bucket/adopt/table"));
    for (Map<String, String> extra :
        List.of(
            Map.of("s3-secret-access-key", "fixture-secret"),
            Map.of(
                "delta.storage-credential-mode",
                "static",
                "s3-access-key-id",
                " ",
                "s3-secret-access-key",
                "fixture-secret"),
            Map.of(
                "delta.storage-credential-mode",
                "static",
                "s3-access-key-id",
                "fixture",
                "s3-secret-access-key",
                "fixture-secret",
                "s3-session-token",
                "fixture"),
            Map.of("s3-endpoint", "https://host?fixture-secret"),
            Map.of("s3-endpoint", "https://host#fixture-secret"),
            Map.of("s3-endpoint", "file:/fixture-secret"),
            Map.of("s3-path-style-access", "TRUE"),
            Map.of("delta.adoption-roots", "null"),
            Map.of("delta.recovery-principals", "[\"\"]"),
            Map.of("s3-creds-provider", "fixture-secret"),
            Map.of("delta.logStore.class", "fixture-secret"))) {
      Map<String, String> properties = new HashMap<>(extra);
      properties.put("delta.warehouse", "s3a://bucket/root");
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> DeltaCatalogConfig.from(properties));
      assertFalse(error.toString().contains("fixture-secret"));
      assertNull(error.getCause());
    }
  }

  @Test
  void immutableStorageMetadataAndHiddenReservedPrefixes() {
    DeltaCatalogPropertiesMetadata catalog = new DeltaCatalogPropertiesMetadata();
    for (String key :
        List.of(
            "delta.warehouse",
            "delta.adoption-roots",
            "delta.recovery-principals",
            "delta.storage-credential-mode",
            "s3-endpoint",
            "s3-region",
            "s3-path-style-access",
            "s3-access-key-id",
            "s3-secret-access-key",
            "s3-session-token")) {
      assertTrue(catalog.getPropertyEntry(key).isImmutable());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PropertiesMetadataHelpers.validatePropertyForAlter(
                  catalog, Map.of(key, "fixture"), Map.of()));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              PropertiesMetadataHelpers.validatePropertyForAlter(
                  catalog, Map.of(), Map.of(key, "fixture")));
    }
    for (String key : List.of("s3-access-key-id", "s3-secret-access-key", "s3-session-token")) {
      assertTrue(catalog.getPropertyEntry(key).isHidden());
    }
    DeltaTablePropertiesMetadata table = new DeltaTablePropertiesMetadata();
    PropertyEntry<?> command = table.getPropertyEntry(DeltaRegistrationCommand.COMMAND_PROPERTY);
    assertFalse(command.isImmutable());
    assertFalse(command.isReserved());
    assertDoesNotThrow(
        () ->
            PropertiesMetadataHelpers.validatePropertyForAlter(
                table, Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, "request"), Map.of()));
    Map<String, String> visible =
        HiddenPropertyMaskUtils.maskHiddenProperties(
            Map.of(
                "delta.registration.internal.state",
                "fixture-secret",
                "delta.registration.receipt.state",
                "RESERVED"),
            table);
    assertFalse(visible.containsKey("delta.registration.internal.state"));
    assertEquals("RESERVED", visible.get("delta.registration.receipt.state"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PropertiesMetadataHelpers.validatePropertyForCreate(
                table, Map.of("delta.registration.receipt.state", "RESERVED")));
    for (String key : List.of("location", "s3-secret-access-key", "delta.warehouse", "ordinary")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaSchemaPropertiesMetadata.validateRequestProperties(Map.of(key, "fixture")));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              DeltaTablePropertiesMetadata.validateRequestProperties(
                  Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, "request", key, "fixture")));
    }
    assertDoesNotThrow(() -> DeltaSchemaPropertiesMetadata.validateRequestProperties(Map.of()));
    assertDoesNotThrow(
        () ->
            DeltaTablePropertiesMetadata.validateRequestProperties(
                Map.of(DeltaRegistrationCommand.COMMAND_PROPERTY, "request")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaTablePropertiesMetadata.validateRequestProperties(Map.of()));
  }
}
