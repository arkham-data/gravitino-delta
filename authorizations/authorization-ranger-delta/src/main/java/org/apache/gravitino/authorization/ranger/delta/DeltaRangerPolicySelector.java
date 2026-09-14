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
import javax.annotation.Nullable;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.plugin.model.RangerPolicy;

/** Exact mutation ownership, independent of Ranger's wildcard search behavior. */
final class DeltaRangerPolicySelector {
  private final String service;
  private final List<String> labels;

  DeltaRangerPolicySelector(String service, String metalakeId, String catalogId) {
    this.service = service;
    this.labels =
        List.of(
            "gravitino-managed",
            "gravitino-metalake-id:" + metalakeId,
            "gravitino-catalog-id:" + catalogId);
  }

  @Nullable
  RangerPolicy select(List<RangerPolicy> candidates, AuthorizationMetadataObject resource) {
    Map<String, RangerPolicy.RangerPolicyResource> expected = resources(resource);
    List<RangerPolicy> matches = new ArrayList<>();
    for (RangerPolicy p : candidates) {
      if (!Integer.valueOf(RangerPolicy.POLICY_TYPE_ACCESS).equals(p.getPolicyType())
          || !service.equals(p.getService())
          || !p.getPolicyLabels().containsAll(labels)
          || !p.getResources().keySet().equals(expected.keySet())) {
        continue;
      }
      if (expected.entrySet().stream()
          .allMatch(
              e -> {
                RangerPolicy.RangerPolicyResource r = p.getResources().get(e.getKey());
                return r != null
                    && e.getValue().getValues().equals(r.getValues())
                    && Boolean.FALSE.equals(r.getIsExcludes())
                    && Boolean.FALSE.equals(r.getIsRecursive());
              })) {
        matches.add(p);
      }
    }
    if (matches.size() > 1) {
      throw new AuthorizationPluginException("Duplicate owned Delta policies");
    }
    return matches.isEmpty() ? null : matches.get(0);
  }

  boolean isOtherCatalogGrant(RangerPolicy policy, String catalog) {
    List<String> catalogLabels = new ArrayList<>();
    for (String label : policy.getPolicyLabels()) {
      if (label.startsWith("gravitino-catalog-id:")) {
        catalogLabels.add(label);
      }
    }
    if (catalogLabels.size() != 1 || catalogLabels.get(0).equals(labels.get(2))) {
      return false;
    }
    String siblingId = catalogLabels.get(0).substring("gravitino-catalog-id:".length());
    if (siblingId.isBlank()) {
      return false;
    }
    List<String> names = new ArrayList<>();
    for (String key : DeltaRangerResource.KEYS) {
      RangerPolicy.RangerPolicyResource resource = policy.getResources().get(key);
      if (resource == null || resource.getValues().size() != 1) {
        return false;
      }
      String name = resource.getValues().get(0);
      if (name == null || name.isEmpty()) {
        return false;
      }
      if (names.size() < 3 && (name.contains("*") || name.contains("?"))) {
        return false;
      }
      names.add(name);
    }
    if (catalog.equals(names.get(0)) || !"*".equals(names.get(3))) {
      return false;
    }
    DeltaRangerPolicySelector sibling =
        new DeltaRangerPolicySelector(
            service, labels.get(1).substring("gravitino-metalake-id:".length()), siblingId);
    return sibling.select(List.of(policy), new DeltaRangerResource(names, List.of())) != null;
  }

  RangerPolicy create(AuthorizationMetadataObject object) {
    RangerPolicy policy = new RangerPolicy();
    policy.setName("gravitino-" + labels.get(1) + "-" + labels.get(2) + "-" + object.fullName());
    policy.setService(service);
    policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
    policy.setPolicyLabels(labels);
    policy.setResources(resources(object));
    return policy;
  }

  private Map<String, RangerPolicy.RangerPolicyResource> resources(
      AuthorizationMetadataObject object) {
    if (!(object instanceof DeltaRangerResource)) {
      throw new IllegalArgumentException("Typed Delta resource required");
    }
    Map<String, RangerPolicy.RangerPolicyResource> result = new LinkedHashMap<>();
    for (int i = 0; i < 4; i++) {
      result.put(
          DeltaRangerResource.KEYS.get(i),
          new RangerPolicy.RangerPolicyResource(List.of(object.names().get(i)), false, false));
    }
    return result;
  }
}
