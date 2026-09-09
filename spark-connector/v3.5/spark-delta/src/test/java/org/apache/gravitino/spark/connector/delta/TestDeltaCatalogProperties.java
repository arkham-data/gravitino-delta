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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class TestDeltaCatalogProperties {
  @Test
  void rejectsReservedPropertiesAndOptions() {
    for (String key :
        new String[] {
          "location",
          "path",
          "option.path",
          "GRAVITINO.REGISTRATION.OPERATION",
          "delta.registration.command",
          "delta.columnMapping.maxColumnId",
          "delta.coordinatedCommits.commitCoordinator-preview",
          "delta.universalFormat.enabledFormats"
        }) {
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaCatalogProperties.validate(Collections.singletonMap(key, "value")),
          key);
    }
  }

  @Test
  void explicitMappingOverridesSessionDefault() {
    assertEquals("name", DeltaCatalogProperties.mapping(Collections.emptyMap(), "name"));
    assertEquals(
        "none", DeltaCatalogProperties.mapping(Map.of("delta.columnMapping.mode", "none"), "name"));
    assertEquals(
        "name", DeltaCatalogProperties.mapping(Map.of("delta.columnMapping.mode", "name"), "none"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaCatalogProperties.mapping(Map.of("delta.columnMapping.mode", "id"), "none"));
  }

  @Test
  void validatesNativeSessionPrerequisiteAndUpdateSetting() {
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(session.conf()).thenReturn(conf);
    when(conf.get(anyString(), anyString())).thenAnswer(call -> call.getArgument(1));
    assertThrows(
        IllegalArgumentException.class, () -> DeltaCatalogProperties.validateSession(session));
    when(conf.get("spark.sql.catalog.spark_catalog", ""))
        .thenReturn("org.apache.spark.sql.delta.catalog.DeltaCatalog");
    when(conf.get("spark.sql.extensions", ""))
        .thenReturn("io.delta.sql.DeltaSparkSessionExtension");
    assertDoesNotThrow(() -> DeltaCatalogProperties.validateSession(session));
    when(conf.get(DeltaCatalogProperties.UPDATE_ENABLED, "false")).thenReturn("true");
    assertThrows(
        IllegalArgumentException.class, () -> DeltaCatalogProperties.validateSession(session));
  }

  @Test
  void optionWrappedMetadataCannotOverrideTableContract() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaCatalogProperties.validate(Map.of("option.delta.columnMapping.mode", "name")));
  }
}
