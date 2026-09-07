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
package org.apache.gravitino.authorization.ranger.delta;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;

/** Test-only table-grant selector; lifecycle callbacks are outside this candidate's scope. */
class CandidateDeltaPolicyPlugin extends RangerAuthorizationHadoopSQLPlugin {
  CandidateDeltaPolicyPlugin(Map<String, String> config) {
    super("lake", config);
  }

  /** {@inheritDoc} */
  @Override
  public List<String> policyResourceDefinesRule() {
    return List.of("catalog", "database", "table", "column");
  }

  /** {@inheritDoc} */
  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject object) {
    if (object.type() != MetadataObject.Type.TABLE)
      throw new UnsupportedOperationException("Table characterization only");
    if (object.privileges().stream()
        .anyMatch(
            privilege ->
                privilege.name() != Privilege.Name.SELECT_TABLE
                    || privilege.condition() != Privilege.Condition.ALLOW))
      throw new UnsupportedOperationException("Only SELECT_TABLE allow is characterized");
    List<String> names = new ArrayList<>(List.of(object.fullName().split("\\.")));
    names.add("*");
    List<AuthorizationPrivilege> privileges =
        object.privileges().stream()
            .flatMap(privilege -> privilegesMappingRule().get(privilege.name()).stream())
            .collect(Collectors.toList());
    return List.of(new Resource(names, privileges));
  }

  /** {@inheritDoc} */
  @Override
  protected RangerPolicy createPolicyAddResources(AuthorizationMetadataObject object) {
    RangerPolicy policy = new RangerPolicy();
    policy.setName("gravitino-" + metalakeId + "-" + catalogId + "-table-" + object.fullName());
    policy.setService(rangerServiceName);
    policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
    policy.setPolicyLabels(labels());
    Map<String, RangerPolicy.RangerPolicyResource> resources = new LinkedHashMap<>();
    for (int i = 0; i < object.names().size(); i++)
      resources.put(
          policyResourceDefinesRule().get(i),
          new RangerPolicy.RangerPolicyResource(object.names().get(i)));
    policy.setResources(resources);
    return policy;
  }

  /** {@inheritDoc} */
  @Override
  protected List<RangerPolicy> wildcardSearchPolicies(AuthorizationMetadataObject object) {
    Map<String, String> filters = new LinkedHashMap<>();
    filters.put(SearchFilter.SERVICE_NAME, rangerServiceName);
    filters.put(SearchFilter.POLICY_TYPE, Integer.toString(RangerPolicy.POLICY_TYPE_ACCESS));
    filters.put(SearchFilter.POLICY_LABEL, "gravitino-catalog-id:" + catalogId);
    for (int i = 0; i < object.names().size(); i++)
      filters.put(
          SearchFilter.RESOURCE_PREFIX + policyResourceDefinesRule().get(i), object.names().get(i));
    try {
      return rangerClient.findPolicies(filters).stream()
          .filter(
              policy ->
                  Integer.valueOf(RangerPolicy.POLICY_TYPE_ACCESS).equals(policy.getPolicyType()))
          .filter(policy -> rangerServiceName.equals(policy.getService()))
          .filter(policy -> policy.getPolicyLabels().containsAll(labels()))
          .filter(policy -> policy.getResources().size() == object.names().size())
          .filter(
              policy ->
                  policy.getResources().values().stream()
                      .noneMatch(
                          resource ->
                              Boolean.TRUE.equals(resource.getIsExcludes())
                                  || Boolean.TRUE.equals(resource.getIsRecursive())))
          .collect(Collectors.toList());
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Candidate policy lookup failed");
    }
  }

  private List<String> labels() {
    return List.of(
        "gravitino-managed",
        "gravitino-metalake-id:" + metalakeId,
        "gravitino-catalog-id:" + catalogId);
  }

  private static final class Resource implements AuthorizationSecurableObject {
    private final List<String> names;
    private final List<AuthorizationPrivilege> privileges;

    private Resource(List<String> names, List<AuthorizationPrivilege> privileges) {
      this.names = names;
      this.privileges = privileges;
    }
    /** {@inheritDoc} */
    @Override
    public String name() {
      return names.get(names.size() - 1);
    }
    /** {@inheritDoc} */
    @Override
    public String parent() {
      return String.join(".", names.subList(0, names.size() - 1));
    }
    /** {@inheritDoc} */
    @Override
    public List<String> names() {
      return names;
    }
    /** {@inheritDoc} */
    @Override
    public Type type() {
      return () -> MetadataObject.Type.TABLE;
    }
    /** {@inheritDoc} */
    @Override
    public List<AuthorizationPrivilege> privileges() {
      return privileges;
    }
    /** {@inheritDoc} */
    @Override
    public void validateAuthorizationMetadataObject() {
      if (names.size() != 4) throw new IllegalArgumentException("Four resources required");
    }
  }
}
