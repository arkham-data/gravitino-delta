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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.RangerClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Live Admin CRUD and independent plugin-download characterization of the test-only candidate. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.admin.enabled", matches = "true")
class TestRangerDeltaAdminContract {
  private static final Logger LOG = LoggerFactory.getLogger(TestRangerDeltaAdminContract.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void candidateLifecyclePreservesForeignPoliciesAcrossAdminAndPluginDownload() throws Throwable {
    assertArtifact(RangerClient.class, "ranger-intg");
    assertArtifact(RangerPolicy.class, "ranger-plugins-common");
    assertArtifact(RangerAdminRESTClient.class, "ranger-plugins-common");
    assertArtifact(ServicePolicies.class, "ranger-plugins-common");
    String url = required("gravitino.ranger.delta.admin.url");
    Properties credentials = new Properties();
    try (InputStream input =
        Files.newInputStream(Paths.get(required("gravitino.ranger.delta.admin.credentialsFile")))) {
      credentials.load(input);
    }
    String user = credential(credentials, "RANGER_ADMIN_USER");
    String password = credential(credentials, "RANGER_ADMIN_PASSWORD");
    RangerClient admin = new RangerClient(url, "simple", user, password, null);
    String suffix = UUID.randomUUID().toString().replace("-", "");
    String serviceName = "delta_contract_service_" + suffix;
    String tagServiceName = "delta_contract_tag_" + suffix;
    String definitionName = "delta_contract_" + suffix;
    String roleName = "delta_reader_" + suffix;
    String rangerRole = "GRAVITINO_" + roleName;
    String metalakeId = "lake_" + suffix;
    String catalogId = "catalog_" + suffix;
    List<String> labels =
        Arrays.asList(
            "gravitino-managed",
            "gravitino-metalake-id:" + metalakeId,
            "gravitino-catalog-id:" + catalogId);
    List<Cleanup> cleanup = new ArrayList<>();
    cleanup.add(
        () -> {
          List<RangerService> remaining = admin.findServices(Collections.emptyMap());
          Assertions.assertTrue(
              remaining.stream()
                  .noneMatch(
                      service ->
                          serviceName.equals(service.getName())
                              || tagServiceName.equals(service.getName())),
              "Both unique resource and tag services must be absent after cleanup");
        });
    Throwable failure = null;
    try {
      RangerServiceDef canonical;
      try (InputStream input = getClass().getResourceAsStream("/ranger-delta-servicedef.json")) {
        Assertions.assertNotNull(input);
        canonical = MAPPER.readValue(input, RangerServiceDef.class);
      }
      canonical.setName(definitionName);
      RangerServiceDef createdDef = admin.createServiceDef(canonical);
      cleanup.add(() -> admin.deleteServiceDef(createdDef.getId()));
      assertDefinition(canonical, admin.getServiceDef(definitionName));
      // Ranger 2.8 otherwise derives a shared tag-service name by stripping the final underscore
      // suffix. An explicit unique tag service prevents that untracked server-side side effect.
      RangerService tagService = new RangerService();
      tagService.setName(tagServiceName);
      tagService.setType("tag");
      // Tag-policy semantics are outside this contract. Suppress only this service's default
      // policies through RangerBaseService's supported per-service configuration.
      tagService.setConfigs(
          Collections.singletonMap("create.default.policy.per.hierarchy", "false"));
      RangerService createdTagService = admin.createService(tagService);
      cleanup.add(() -> admin.deleteService(createdTagService.getId()));
      cleanup.add(() -> cleanPolicies(admin, tagServiceName));
      Assertions.assertTrue(admin.getPoliciesInService(tagServiceName).isEmpty());
      RangerService service = new RangerService();
      service.setName(serviceName);
      service.setType(definitionName);
      service.setTagService(tagServiceName);
      service.setConfigs(Collections.emptyMap());
      RangerService createdService = admin.createService(service);
      cleanup.add(() -> admin.deleteService(createdService.getId()));
      // The service is unique to this invocation; collect its IDs even if a callback fails
      // mid-grant.
      cleanup.add(() -> cleanPolicies(admin, serviceName));
      RangerService persistedService = admin.getService(serviceName);
      Assertions.assertEquals(createdService.getId(), persistedService.getId());
      Assertions.assertEquals(tagServiceName, persistedService.getTagService());
      Assertions.assertEquals(createdTagService.getId(), admin.getService(tagServiceName).getId());
      LOG.info(
          "LIVE ADMIN linked unique tag service={} id={}",
          tagServiceName,
          createdTagService.getId());
      LOG.info(
          "LIVE ADMIN created service={} id={} definition={}",
          serviceName,
          createdService.getId(),
          definitionName);
      Map<String, String> config = new LinkedHashMap<>();
      config.put("authorization.ranger.admin.url", url);
      config.put("authorization.ranger.service.type", definitionName);
      config.put("authorization.ranger.service.name", serviceName);
      config.put("authorization.ranger.service.create-if-absent", "false");
      config.put("authorization.ranger.auth.type", "simple");
      config.put("authorization.ranger.username", user);
      config.put("authorization.ranger.password", password);
      config.put("authorization.metalake.id", metalakeId);
      config.put("authorization.catalog.id", catalogId);
      Map<Long, JsonNode> foreign = new LinkedHashMap<>();
      for (int type = 0; type <= 2; type++) {
        RangerPolicy policy = admin.createPolicy(foreignPolicy(serviceName, suffix, type));
        foreign.put(policy.getId(), json(admin.getPolicy(policy.getId())));
      }
      ServicePolicies foreignDownload = download(url, serviceName);
      assertDownload(
          foreignDownload, createdService, canonical, admin.getPoliciesInService(serviceName));
      LOG.info(
          "LIVE DOWNLOAD foreign service={} version={} policies={}",
          serviceName,
          foreignDownload.getPolicyVersion(),
          foreignDownload.getPolicies().size());
      CandidateDeltaPolicyPlugin plugin = new CandidateDeltaPolicyPlugin(config);
      Role role = Mockito.mock(Role.class);
      Mockito.when(role.name()).thenReturn(roleName);
      Mockito.when(role.securableObjects()).thenReturn(Collections.emptyList());
      cleanup.add(cleanup.size() - 1, () -> cleanRole(admin, rangerRole, user, serviceName));
      Assertions.assertTrue(plugin.onRoleCreated(role));
      Assertions.assertNotNull(admin.getRole(rangerRole, user, serviceName).getId());
      SecurableObject table =
          SecurableObjects.parse(
              "delta_a.sales.orders",
              MetadataObject.Type.TABLE,
              Collections.singletonList(Privileges.SelectTable.allow()));
      RoleChange grant = RoleChange.addSecurableObject(roleName, table);
      RoleChange revoke = RoleChange.removeSecurableObject(roleName, table);
      Assertions.assertTrue(plugin.onRoleUpdated(role, grant));
      RangerPolicy first = managed(admin.getPoliciesInService(serviceName), labels);
      assertForeign(admin, foreign);
      Assertions.assertTrue(plugin.onRoleUpdated(role, grant));
      List<RangerPolicy> granted = admin.getPoliciesInService(serviceName);
      Assertions.assertEquals(4, granted.size());
      RangerPolicy owned = managed(granted, labels);
      Assertions.assertEquals(first.getId(), owned.getId());
      Assertions.assertEquals(labels.size(), owned.getPolicyLabels().size());
      Assertions.assertEquals(json(resources(true, "*")), json(owned.getResources()));
      Assertions.assertTrue(owned.getDenyPolicyItems().isEmpty());
      Assertions.assertEquals(1, owned.getPolicyItems().size());
      RangerPolicy.RangerPolicyItem item = owned.getPolicyItems().get(0);
      Assertions.assertEquals(Collections.singletonList(rangerRole), item.getRoles());
      Assertions.assertEquals(1, item.getAccesses().size());
      Assertions.assertEquals("select", item.getAccesses().get(0).getType());
      Assertions.assertEquals(Boolean.TRUE, item.getAccesses().get(0).getIsAllowed());
      assertForeign(admin, foreign);
      ServicePolicies downloaded = download(url, serviceName);
      assertDownload(downloaded, createdService, canonical, granted);
      LOG.info(
          "LIVE DOWNLOAD grant service={} version={} policies={}",
          serviceName,
          downloaded.getPolicyVersion(),
          downloaded.getPolicies().size());
      Assertions.assertTrue(plugin.onRoleUpdated(role, revoke));
      assertForeign(admin, foreign);
      Assertions.assertTrue(plugin.onRoleUpdated(role, revoke));
      List<RangerPolicy> revoked = admin.getPoliciesInService(serviceName);
      Assertions.assertEquals(3, revoked.size());
      Assertions.assertTrue(
          revoked.stream().noneMatch(policy -> policy.getId().equals(owned.getId())));
      assertForeign(admin, foreign);
      ServicePolicies after = download(url, serviceName);
      assertDownload(after, createdService, canonical, revoked);
      Assertions.assertTrue(after.getPolicyVersion() > downloaded.getPolicyVersion());
      LOG.info(
          "LIVE DOWNLOAD revoke service={} version={} policies={}",
          serviceName,
          after.getPolicyVersion(),
          after.getPolicies().size());
    } catch (Throwable problem) {
      failure = problem;
    } finally {
      int cleanupFailures = 0;
      Collections.reverse(cleanup);
      for (Cleanup action : cleanup) {
        try {
          action.run();
        } catch (Throwable problem) {
          cleanupFailures++;
          if (failure == null) failure = problem;
          else failure.addSuppressed(problem);
        }
      }
      if (cleanupFailures == 0)
        LOG.info("LIVE CLEANUP completed all registered owned-resource cleanup steps: {}", suffix);
    }
    if (failure != null) throw failure;
  }

  private static void cleanRole(RangerClient admin, String role, String user, String service)
      throws Exception {
    if (admin.getAllRoleNames(user, service).contains(role)) {
      admin.deleteRole(admin.getRole(role, user, service).getId());
    }
  }

  private static void cleanPolicies(RangerClient admin, String service) throws Exception {
    Exception failure = null;
    for (RangerPolicy policy : admin.getPoliciesInService(service)) {
      try {
        admin.deletePolicy(policy.getId());
      } catch (Exception problem) {
        if (failure == null) failure = problem;
        else failure.addSuppressed(problem);
      }
    }
    if (failure != null) throw failure;
  }

  private static ServicePolicies download(String url, String service) throws Exception {
    Configuration conf = new Configuration(false);
    String prefix = "ranger.plugin.delta";
    conf.set(prefix + ".policy.rest.url", url);
    conf.setBoolean(prefix + ".supports.policy.deltas", false);
    conf.setBoolean(prefix + ".policy.rest.client.cookie.enabled", false);
    RangerAdminRESTClient downloader = new RangerAdminRESTClient();
    downloader.init(service, "delta-contract", prefix, conf);
    ServicePolicies result = downloader.getServicePoliciesIfUpdated(-1L, 0L);
    Assertions.assertNotNull(
        result,
        "Ranger plugin download must return a full snapshot; check deployment/download authentication");
    return result;
  }

  private static void assertDownload(
      ServicePolicies actual,
      RangerService service,
      RangerServiceDef canonical,
      List<RangerPolicy> expected)
      throws Exception {
    Assertions.assertEquals(service.getName(), actual.getServiceName());
    Assertions.assertEquals(service.getId(), actual.getServiceId());
    Assertions.assertNotNull(actual.getPolicyVersion());
    Assertions.assertTrue(actual.getPolicyVersion() > 0);
    assertDefinition(canonical, actual.getServiceDef());
    Map<Long, JsonNode> policies = new LinkedHashMap<>();
    for (RangerPolicy policy : actual.getPolicies()) policies.put(policy.getId(), json(policy));
    Assertions.assertEquals(expected.size(), actual.getPolicies().size());
    for (RangerPolicy policy : expected)
      Assertions.assertEquals(json(policy), policies.get(policy.getId()));
  }

  private static void assertDefinition(RangerServiceDef expected, RangerServiceDef actual)
      throws Exception {
    Assertions.assertEquals(expected.getName(), actual.getName());
    Assertions.assertEquals(json(expected.getResources()), json(actual.getResources()));
    Assertions.assertEquals(json(expected.getAccessTypes()), json(actual.getAccessTypes()));
    Assertions.assertEquals(json(expected.getDataMaskDef()), json(actual.getDataMaskDef()));
    Assertions.assertEquals(json(expected.getRowFilterDef()), json(actual.getRowFilterDef()));
    Assertions.assertEquals("false", actual.getOptions().get("lookupSupported"));
  }

  private static void assertForeign(RangerClient admin, Map<Long, JsonNode> expected)
      throws Exception {
    for (Map.Entry<Long, JsonNode> entry : expected.entrySet())
      Assertions.assertEquals(entry.getValue(), json(admin.getPolicy(entry.getKey())));
  }

  private static RangerPolicy managed(List<RangerPolicy> policies, List<String> labels) {
    List<RangerPolicy> owned =
        policies.stream()
            .filter(policy -> policy.getPolicyLabels().containsAll(labels))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, owned.size());
    Assertions.assertEquals(
        Integer.valueOf(RangerPolicy.POLICY_TYPE_ACCESS), owned.get(0).getPolicyType());
    return owned.get(0);
  }

  private static RangerPolicy foreignPolicy(String service, String suffix, int type) {
    RangerPolicy policy = new RangerPolicy();
    policy.setName("foreign_" + type + "_" + suffix);
    policy.setService(service);
    policy.setPolicyType(type);
    policy.setIsEnabled(true);
    policy.setResources(resources(type != RangerPolicy.POLICY_TYPE_ROWFILTER, "allowed"));
    RangerPolicy.RangerPolicyItem item;
    if (type == RangerPolicy.POLICY_TYPE_DATAMASK) {
      RangerPolicy.RangerDataMaskPolicyItem mask = new RangerPolicy.RangerDataMaskPolicyItem();
      mask.setDataMaskInfo(
          new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", null, "'MASKED'"));
      policy.setDataMaskPolicyItems(Collections.singletonList(mask));
      item = mask;
    } else if (type == RangerPolicy.POLICY_TYPE_ROWFILTER) {
      RangerPolicy.RangerRowFilterPolicyItem filter = new RangerPolicy.RangerRowFilterPolicyItem();
      filter.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id = 1"));
      policy.setRowFilterPolicyItems(Collections.singletonList(filter));
      item = filter;
    } else {
      item = new RangerPolicy.RangerPolicyItem();
      policy.setPolicyItems(Collections.singletonList(item));
    }
    item.setGroups(Collections.singletonList("public"));
    item.setAccesses(
        Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    return policy;
  }

  private static Map<String, RangerPolicy.RangerPolicyResource> resources(
      boolean column, String value) {
    Map<String, RangerPolicy.RangerPolicyResource> resources = new LinkedHashMap<>();
    resources.put("catalog", new RangerPolicy.RangerPolicyResource("delta_a"));
    resources.put("database", new RangerPolicy.RangerPolicyResource("sales"));
    resources.put("table", new RangerPolicy.RangerPolicyResource("orders"));
    if (column) resources.put("column", new RangerPolicy.RangerPolicyResource(value));
    return resources;
  }

  private static JsonNode json(Object value) throws Exception {
    return MAPPER.readTree(MAPPER.writeValueAsString(value));
  }

  private static String required(String name) {
    String value = System.getProperty(name);
    Assertions.assertNotNull(value, "Missing required system property " + name);
    Assertions.assertFalse(value.trim().isEmpty(), "Empty required system property " + name);
    return value;
  }

  private static String credential(Properties properties, String name) {
    String value = properties.getProperty(name);
    Assertions.assertNotNull(value, "Credential file must define " + name);
    Assertions.assertFalse(value.trim().isEmpty(), "Credential file must define " + name);
    return value;
  }

  private static void assertArtifact(Class<?> type, String artifact) {
    String location = type.getProtectionDomain().getCodeSource().getLocation().toExternalForm();
    LOG.info("RANGER RUNTIME class={} location={}", type.getName(), location);
    Assertions.assertTrue(
        location.endsWith("/" + artifact + "-2.8.0.jar"),
        "Ranger 2.8.0 artifact required: " + location);
  }

  @FunctionalInterface
  private interface Cleanup {
    void run() throws Exception;
  }
}
