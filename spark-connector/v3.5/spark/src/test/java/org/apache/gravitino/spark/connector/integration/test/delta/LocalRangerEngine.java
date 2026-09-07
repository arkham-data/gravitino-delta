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
package org.apache.gravitino.spark.connector.integration.test.delta;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin$;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Real Kyuubi 1.10.2 Ranger engine facade for the test-only catalog-aware rule. */
public final class LocalRangerEngine {
  private static final Logger LOG = LoggerFactory.getLogger(LocalRangerEngine.class);
  private static final List<String> REQUESTS = new CopyOnWriteArrayList<>();

  private LocalRangerEngine() {}

  /** Initializes the real Ranger plugin using the configured local policy-source client. */
  public static void initialize() {
    SparkRangerAdminPlugin$.MODULE$.initialize();
  }

  /** Evaluates one canonical column access request, failing closed on no decision. */
  public static void authorize(String catalog, String database, String table, String column) {
    RangerAccessRequestImpl request = request(catalog, database, table, column);
    RangerAccessResult result = SparkRangerAdminPlugin$.MODULE$.isAccessAllowed(request);
    record("access", request, result);
    if (result == null || !result.getIsAllowed())
      throw new SecurityException(
          "Ranger denied " + request.getUser() + " " + request.getResource().getAsString());
  }

  /** Returns the real Ranger CUSTOM masking expression or null when no mask applies. */
  @Nullable
  public static String mask(String catalog, String database, String table, String column) {
    RangerAccessRequestImpl request = request(catalog, database, table, column);
    RangerAccessResult result = SparkRangerAdminPlugin$.MODULE$.evalDataMaskPolicies(request, null);
    record("mask", request, result);
    LOG.info(
        "MASK EXPRESSION enabled={} type={} value={}",
        result != null && result.isMaskEnabled(),
        result == null ? null : result.getMaskType(),
        result == null ? null : result.getMaskedValue());
    if (result == null || !result.isMaskEnabled()) return null;
    if (!"CUSTOM".equals(result.getMaskType()))
      throw new IllegalStateException("Only CUSTOM masks characterized");
    return result.getMaskedValue();
  }

  /** Returns the real Ranger row-filter expression or null when no filter applies. */
  @Nullable
  public static String rowFilter(String catalog, String database, String table) {
    RangerAccessRequestImpl request = request(catalog, database, table, null);
    RangerAccessResult result =
        SparkRangerAdminPlugin$.MODULE$.evalRowFilterPolicies(request, null);
    record("filter", request, result);
    return result != null && result.isRowFilterEnabled() ? result.getFilterExpr() : null;
  }

  /** Captures immutable request strings for resource and subject assertions. */
  public static List<String> requests() {
    return List.copyOf(REQUESTS);
  }

  /**
   * Refreshes through Ranger's public source-polling entry point and reports the active version.
   */
  public static long refresh() {
    SparkRangerAdminPlugin$.MODULE$.refreshPoliciesAndTags();
    return SparkRangerAdminPlugin$.MODULE$.getPoliciesVersion();
  }

  /** Stops the real plugin's background workers after the fixture. */
  public static void close() {
    SparkRangerAdminPlugin$.MODULE$.cleanup();
  }

  private static RangerAccessRequestImpl request(
      String catalog, String database, String table, String column) {
    if (catalog == null || database == null || table == null)
      throw new SecurityException("Missing canonical resource identity");
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setValue("catalog", catalog);
    resource.setValue("database", database);
    resource.setValue("table", table);
    if (column != null) resource.setValue("column", column);
    try {
      String subject = UserGroupInformation.getCurrentUser().getShortUserName();
      if (subject.equals("metadata_service"))
        throw new SecurityException("Metadata identity reached query authorization");
      return new RangerAccessRequestImpl(
          resource, "select", subject, Collections.emptySet(), Collections.emptySet());
    } catch (IOException failure) {
      throw new IllegalStateException(failure);
    }
  }

  private static void record(
      String kind, RangerAccessRequestImpl request, RangerAccessResult result) {
    String value =
        kind
            + " user="
            + request.getUser()
            + " resource="
            + request.getResource().getAsMap()
            + " policy="
            + (result == null ? "null" : result.getPolicyId())
            + " allowed="
            + (result != null && result.getIsAllowed());
    REQUESTS.add(value);
    LOG.info("RANGER {}", value);
  }
}
