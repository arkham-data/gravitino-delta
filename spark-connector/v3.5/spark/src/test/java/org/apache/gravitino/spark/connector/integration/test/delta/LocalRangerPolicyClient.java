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

import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;

/** Ranger's supported policy-source hook, backed by a local snapshot rather than Ranger Admin. */
public class LocalRangerPolicyClient extends RangerAdminRESTClient {
  private static volatile ServicePolicies snapshot = readPolicies();

  /** {@inheritDoc} */
  @Override
  @Nullable
  public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long activationTime) {
    return snapshot.getPolicyVersion() > lastKnownVersion ? snapshot : null;
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public RangerRoles getRolesIfUpdated(long lastKnownVersion, long activationTime) {
    return null;
  }

  /** Publishes a new local snapshot without the direct Ranger-owned column access policy. */
  public static void revokeColumnPolicy() {
    ServicePolicies replacement = readPolicies();
    replacement.getPolicies().removeIf(policy -> policy.getId() == 1L);
    replacement.setPolicyVersion(2L);
    snapshot = replacement;
  }

  private static ServicePolicies readPolicies() {
    try (InputStream input =
            Objects.requireNonNull(
                LocalRangerPolicyClient.class.getResourceAsStream(
                    "/ranger-delta-local-policies.json"));
        InputStreamReader reader = new InputStreamReader(input, StandardCharsets.UTF_8)) {
      return new Gson().fromJson(reader, ServicePolicies.class);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
