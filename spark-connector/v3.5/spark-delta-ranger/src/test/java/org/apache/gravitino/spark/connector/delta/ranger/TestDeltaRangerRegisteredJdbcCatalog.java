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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestDeltaRangerRegisteredJdbcCatalog {
  private GravitinoCatalogManager manager;

  @BeforeEach
  void createManager() {
    manager =
        GravitinoCatalogManager.create(
            new SparkConf(false), "test", identity -> mock(GravitinoClient.class));
  }

  @AfterEach
  void closeManager() {
    manager.close();
  }

  @Test
  void wrapperRequiresAllThreeTrustedStartupOptInsAndCannotBeReusedAfterFailure() {
    SparkSession session = mock(SparkSession.class);
    SparkContext context = mock(SparkContext.class);
    when(session.sparkContext()).thenReturn(context);
    SparkSession.setActiveSession(session);
    try {
      for (String missing :
          new String[] {
            "enableDeltaSupport", "enableDeltaRangerSupport", "enableDeltaRangerJdbcSupport"
          }) {
        SparkConf conf = new SparkConf(false);
        for (String flag :
            new String[] {
              "enableDeltaSupport", "enableDeltaRangerSupport", "enableDeltaRangerJdbcSupport"
            }) conf.set("spark.gravitino." + flag, Boolean.toString(!missing.equals(flag)));
        when(context.getConf()).thenReturn(conf);
        DeltaRangerPostgreSqlCatalogSpark35 catalog = new DeltaRangerPostgreSqlCatalogSpark35();
        assertThrows(
            IllegalArgumentException.class,
            () -> catalog.initialize("pg", new CaseInsensitiveStringMap(Collections.emptyMap())));
        assertThrows(
            IllegalStateException.class,
            () -> catalog.initialize("pg", new CaseInsensitiveStringMap(Collections.emptyMap())));
        catalog.close();
      }
    } finally {
      SparkSession.clearActiveSession();
    }
  }

  @Test
  void readOnlyWrapperRejectsCatalogMutationBeforeAnyNativeWork() {
    DeltaRangerPostgreSqlCatalogSpark35 catalog = new DeltaRangerPostgreSqlCatalogSpark35();
    Identifier id = Identifier.of(new String[] {"public"}, "source");
    assertThrows(
        SecurityException.class,
        () -> catalog.createTable(id, (StructType) null, null, Collections.emptyMap()));
    assertThrows(SecurityException.class, () -> catalog.alterTable(id));
    assertThrows(SecurityException.class, () -> catalog.dropTable(id));
    assertThrows(SecurityException.class, () -> catalog.renameTable(id, id));
    assertThrows(
        SecurityException.class,
        () -> catalog.createNamespace(new String[] {"public"}, Collections.emptyMap()));
    assertThrows(SecurityException.class, () -> catalog.alterNamespace(new String[] {"public"}));
    assertThrows(
        SecurityException.class, () -> catalog.dropNamespace(new String[] {"public"}, false));
    catalog.close();
  }
}
