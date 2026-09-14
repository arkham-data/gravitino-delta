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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;

/** Closed service and trusted catalog bindings; validation never mutates Admin state. */
final class DeltaRangerAuthorizationConfig {
  static final String METALAKE = "authorization.ranger.delta.metalake";
  static final String CATALOG = "authorization.ranger.delta.catalog";
  static final String SERVICE_METALAKE = "gravitino.metalake";
  private static final Set<String> ACTIONS =
      Set.of("select", "read", "write", "create", "drop", "alter", "update");
  final Map<String, String> properties;
  final String catalog;

  DeltaRangerAuthorizationConfig(String metalake, Map<String, String> properties) {
    this.properties = prepare(metalake, properties);
    this.catalog = properties.get(CATALOG);
  }

  static Map<String, String> prepare(String metalake, Map<String, String> properties) {
    require(
        Objects.equals(metalake, properties.get(METALAKE)), "Trusted metalake binding mismatch");
    literal(metalake);
    literal(properties.get(CATALOG));
    for (String key :
        List.of(
            BaseAuthorization.METALAKE_ID,
            BaseAuthorization.CATALOG_ID,
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE)) {
      require(
          properties.get(key) != null && !properties.get(key).isBlank(), "Missing binding: " + key);
    }
    require(
        !Boolean.parseBoolean(
            properties.get(RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT)),
        "Service creation is forbidden");
    Map<String, String> result = new HashMap<>(properties);
    result.put(RangerAuthorizationProperties.RANGER_SERVICE_CREATE_IF_ABSENT, "false");
    return Map.copyOf(result);
  }

  void validateService(RangerClientExtension client) {
    try {
      RangerService service =
          client.getService(properties.get(RangerAuthorizationProperties.RANGER_SERVICE_NAME));
      require(
          service != null
              && Objects.equals(
                  service.getName(),
                  properties.get(RangerAuthorizationProperties.RANGER_SERVICE_NAME)),
          "Provisioned service required");
      require(
          Objects.equals(
              service.getType(), properties.get(RangerAuthorizationProperties.RANGER_SERVICE_TYPE)),
          "Service type mismatch");
      require(
          Objects.equals(service.getConfigs().get(SERVICE_METALAKE), properties.get(METALAKE)),
          "Service metalake binding mismatch");
      RangerServiceDef definition = client.getServiceDef(service.getType());
      require(
          definition != null && service.getType().equals(definition.getName()),
          "Provisioned definition required");
      List<RangerServiceDef.RangerServiceConfigDef> metalakeConfigs =
          definition.getConfigs().stream()
              .filter(config -> SERVICE_METALAKE.equals(config.getName()))
              .collect(Collectors.toList());
      require(
          metalakeConfigs.size() == 1
              && "string".equals(metalakeConfigs.get(0).getType())
              && Boolean.TRUE.equals(metalakeConfigs.get(0).getMandatory()),
          "Required service metalake configuration definition missing or incompatible");
      resources(definition.getResources(), 4);
      resources(definition.getDataMaskDef().getResources(), 4);
      resources(definition.getRowFilterDef().getResources(), 3);
      Set<String> expected = new HashSet<>(ACTIONS);
      expected.add("all");
      require(
          definition.getAccessTypes().size() == expected.size()
              && definition.getAccessTypes().stream()
                  .map(RangerServiceDef.RangerAccessTypeDef::getName)
                  .collect(Collectors.toSet())
                  .equals(expected),
          "Unsupported owner access vocabulary");
      definition
          .getAccessTypes()
          .forEach(
              a ->
                  require(
                      new HashSet<>(a.getImpliedGrants())
                          .equals(a.getName().equals("all") ? ACTIONS : Set.of()),
                      "Unsupported implied grants"));
      require(
          definition.getDataMaskDef().getAccessTypes().size() == 1
              && definition.getDataMaskDef().getAccessTypes().get(0).getName().equals("select"),
          "Unsupported mask access");
      require(
          definition.getRowFilterDef().getAccessTypes().size() == 1
              && definition.getRowFilterDef().getAccessTypes().get(0).getName().equals("select"),
          "Unsupported filter access");
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Unable to validate provisioned Delta service");
    }
  }

  static void literal(String name) {
    require(
        name != null && !name.isEmpty() && name.indexOf('*') < 0 && name.indexOf('?') < 0,
        "Literal Delta name contains an unsupported wildcard or is empty");
  }

  static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  private static void resources(List<RangerServiceDef.RangerResourceDef> resources, int size) {
    require(resources != null && resources.size() == size, "Incompatible resource hierarchy");
    for (int i = 0; i < size; i++) {
      RangerServiceDef.RangerResourceDef r = resources.get(i);
      require(
          DeltaRangerResource.KEYS.get(i).equals(r.getName())
              && Objects.equals(r.getParent(), i == 0 ? null : DeltaRangerResource.KEYS.get(i - 1))
              && Objects.equals(r.getLevel(), (i + 1) * 10),
          "Incompatible resource identity");
      require(
          Boolean.TRUE.equals(r.getMandatory())
              && Boolean.valueOf(i == size - 1).equals(r.getIsValidLeaf())
              && Boolean.FALSE.equals(r.getRecursiveSupported())
              && Boolean.FALSE.equals(r.getExcludesSupported()),
          "Incompatible leaf semantics");
      require(
          "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher"
              .equals(r.getMatcher()),
          "Custom matcher is not qualified");
      require(
          Map.of(
                  "wildCard",
                  "true",
                  "ignoreCase",
                  "false",
                  "replaceTokens",
                  "false",
                  "replaceReqExpressions",
                  "false")
              .equals(r.getMatcherOptions()),
          "Incompatible matcher options");
    }
  }
}
