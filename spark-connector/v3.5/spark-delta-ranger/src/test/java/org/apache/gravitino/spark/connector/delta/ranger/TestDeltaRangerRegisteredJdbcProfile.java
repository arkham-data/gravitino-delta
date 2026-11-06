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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

class TestDeltaRangerRegisteredJdbcProfile {
  private final Identifier id = Identifier.of(new String[] {"public"}, "source");

  private Map<String, String> options() {
    Map<String, String> result = new HashMap<>();
    result.put("url", "jdbc:postgresql://localhost:5432/extraction");
    result.put("driver", "org.postgresql.Driver");
    result.put("user", "reader");
    result.put("password", "private-fixture-material");
    result.put("dbtable", "\"public\".\"source\"");
    return result;
  }

  @Test
  void rejectDriverAndBypassBeforeNativeCatalogInitialization() {
    Map<String, String> properties = new HashMap<>();
    properties.put("jdbc-url", "jdbc:postgresql://localhost:5432/extraction");
    properties.put("jdbc-database", "extraction");
    properties.put("jdbc-driver", "org.postgresql.Driver");
    assertDoesNotThrow(() -> DeltaRangerJdbcSourceBindings.validateCatalogProperties(properties));
    properties.put("jdbc-driver", "untrusted.CustomDriver");
    assertThrows(
        SecurityException.class,
        () -> DeltaRangerJdbcSourceBindings.validateCatalogProperties(properties));
    properties.put("jdbc-driver", "org.postgresql.Driver");
    properties.put("spark.bypass.connectionProvider", "untrusted.Provider");
    assertThrows(
        SecurityException.class,
        () -> DeltaRangerJdbcSourceBindings.validateCatalogProperties(properties));
  }

  @Test
  void stockSinglePartitionNamedProfileIsAccepted() {
    assertDoesNotThrow(
        () -> DeltaRangerJdbcSourceBindings.validateProfile(options(), "extraction", id));
  }

  @Test
  void databaseMustBeExplicitAndAgreeWithRegistration() {
    for (String url :
        new String[] {
          "jdbc:postgresql://localhost",
          "jdbc:postgresql://localhost/",
          "jdbc:postgresql://localhost/other",
          "jdbc:mysql://localhost/extraction",
          "jdbc:postgresql://localhost/extraction?options=-c%20role=owner"
        }) {
      Map<String, String> changed = options();
      changed.put("url", url);
      assertThrows(
          SecurityException.class,
          () -> DeltaRangerJdbcSourceBindings.validateProfile(changed, "extraction", id));
    }
  }

  @Test
  void callbacksPartitioningQueriesAndOverridesAreRejectedWithoutSecrets() {
    for (String key :
        new String[] {
          "query",
          "prepareQuery",
          "sessionInitStatement",
          "connectionProvider",
          "partitionColumn",
          "numPartitions",
          "fetchsize",
          "spark.bypass.url"
        }) {
      Map<String, String> changed = options();
      changed.put(key, "private-fixture-material");
      SecurityException failure =
          assertThrows(
              SecurityException.class,
              () -> DeltaRangerJdbcSourceBindings.validateProfile(changed, "extraction", id));
      assertFalse(failure.toString().contains("private-fixture-material"));
      assertFalse(failure.toString().contains(changed.get("url")));
    }
  }

  @Test
  void onlyExactDriverAndQuotedTableAreAccepted() {
    for (String table :
        new String[] {"public.source", "\"other\".\"source\"", "(select * from public.source) t"}) {
      Map<String, String> changed = options();
      changed.put("dbtable", table);
      assertThrows(
          SecurityException.class,
          () -> DeltaRangerJdbcSourceBindings.validateProfile(changed, "extraction", id));
    }
    Map<String, String> changed = options();
    changed.put("driver", "untrusted.CustomDriver");
    assertThrows(
        SecurityException.class,
        () -> DeltaRangerJdbcSourceBindings.validateProfile(changed, "extraction", id));
  }
}
