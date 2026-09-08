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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.RangerHadoopSQLMetadataObject;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Characterizes stock policy selection separately from the test-only candidate. */
class TestRangerDeltaContract {
  private static final Logger LOG = LoggerFactory.getLogger(TestRangerDeltaContract.class);

  @BeforeAll
  static void requireRanger280Runtime() {
    assertRangerArtifact(RangerClient.class, "ranger-intg");
    assertRangerArtifact(RangerPolicy.class, "ranger-plugins-common");
  }

  @Test
  void columnBindingRemainsUnsupported() {
    Assertions.assertFalse(Privileges.SelectTable.allow().canBindTo(MetadataObject.Type.COLUMN));
  }

  @Test
  void stockCoexistenceRejectsMultiplePolicies() throws Exception {
    try (MockedConstruction<RangerClientExtension> clients =
        Mockito.mockConstruction(RangerClientExtension.class)) {
      RangerAuthorizationHadoopSQLPlugin plugin =
          new RangerAuthorizationHadoopSQLPlugin("lake", config());
      RangerPolicy owned = policy(1, 0);
      owned.setPolicyLabels(
          List.of("gravitino-managed", "gravitino-metalake-id:10", "gravitino-catalog-id:20"));
      Mockito.when(plugin.getRangerClient().findPolicies(Mockito.anyMap()))
          .thenReturn(List.of(owned, policy(2, 0), policy(3, 1), policy(4, 2)));
      Assertions.assertThrows(
          AuthorizationPluginException.class,
          () ->
              plugin.findManagedPolicy(
                  new RangerHadoopSQLMetadataObject(
                      "sales", "orders", RangerHadoopSQLMetadataObject.Type.TABLE)));
    }
  }

  @Test
  void stockRevokeIncorrectlyDeletesRangerOwnedMask() throws Exception {
    try (MockedConstruction<RangerClientExtension> clients =
        Mockito.mockConstruction(RangerClientExtension.class)) {
      RangerAuthorizationHadoopSQLPlugin plugin =
          new RangerAuthorizationHadoopSQLPlugin("lake", config());
      RangerPolicy mask = policy(3, 1);
      Mockito.when(plugin.getRangerClient().findPolicies(Mockito.anyMap()))
          .thenReturn(List.of(mask));
      Role role = Mockito.mock(Role.class);
      Mockito.when(role.name()).thenReturn("reader_role");
      SecurableObject object =
          SecurableObjects.parse(
              "delta_a.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      plugin.onRoleUpdated(role, RoleChange.removeSecurableObject(role.name(), object));
      Mockito.verify(plugin.getRangerClient(), Mockito.times(2)).deletePolicy(3L);
    }
  }

  @Test
  void candidateGrantRevokePreservesForeignPoliciesAndIsIdempotent() throws Exception {
    try (MockedConstruction<RangerClientExtension> clients =
        Mockito.mockConstruction(RangerClientExtension.class)) {
      RangerAuthorizationHadoopSQLPlugin plugin = candidate();
      List<RangerPolicy> policies =
          new ArrayList<>(List.of(policy(2, 0), policy(3, 1), policy(4, 2)));
      for (RangerPolicy policy : policies)
        policy.setResources(
            Map.of(
                "catalog",
                new RangerPolicy.RangerPolicyResource("delta_a"),
                "database",
                new RangerPolicy.RangerPolicyResource("sales"),
                "table",
                new RangerPolicy.RangerPolicyResource("orders"),
                "column",
                new RangerPolicy.RangerPolicyResource("*")));
      String before = policies.toString();
      RangerClientExtension admin = plugin.getRangerClient();
      Mockito.when(admin.findPolicies(Mockito.anyMap()))
          .thenAnswer(call -> new ArrayList<>(policies));
      AtomicLong ids = new AtomicLong(10);
      Mockito.when(admin.createPolicy(Mockito.any()))
          .thenAnswer(
              call -> {
                RangerPolicy policy = call.getArgument(0);
                policy.setId(ids.incrementAndGet());
                policies.add(policy);
                return policy;
              });
      Mockito.doAnswer(
              call -> {
                policies.removeIf(policy -> policy.getId().equals(call.getArgument(0)));
                return null;
              })
          .when(admin)
          .deletePolicy(Mockito.anyLong());
      Role role = Mockito.mock(Role.class);
      Mockito.when(role.name()).thenReturn("reader_role");
      SecurableObject object =
          SecurableObjects.parse(
              "delta_a.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      RoleChange grant = RoleChange.addSecurableObject(role.name(), object);
      RoleChange revoke = RoleChange.removeSecurableObject(role.name(), object);
      Assertions.assertTrue(plugin.onRoleUpdated(role, grant));
      Assertions.assertTrue(plugin.onRoleUpdated(role, grant));
      Assertions.assertEquals(4, policies.size());
      RangerPolicy managed = policies.get(3);
      Assertions.assertTrue(managed.getDenyPolicyItems().isEmpty());
      Assertions.assertEquals(1, managed.getPolicyItems().size());
      RangerPolicy.RangerPolicyItem item = managed.getPolicyItems().get(0);
      Assertions.assertEquals(List.of("GRAVITINO_reader_role"), item.getRoles());
      Assertions.assertEquals(1, item.getAccesses().size());
      Assertions.assertEquals("select", item.getAccesses().get(0).getType());
      Assertions.assertEquals(Boolean.TRUE, item.getAccesses().get(0).getIsAllowed());
      Assertions.assertEquals(
          List.of("delta_a"), managed.getResources().get("catalog").getValues());
      Assertions.assertTrue(managed.getPolicyLabels().contains("gravitino-catalog-id:20"));
      Assertions.assertTrue(plugin.onRoleUpdated(role, revoke));
      Assertions.assertTrue(plugin.onRoleUpdated(role, revoke));
      Assertions.assertEquals(before, policies.toString());
      Mockito.verify(admin, Mockito.times(1)).createPolicy(Mockito.any());
      Mockito.verify(admin, Mockito.times(1)).deletePolicy(managed.getId());
    }
  }

  @Test
  void candidateRejectsDuplicateOwnersAndIgnoresOtherOwnerTypeOrResource() throws Exception {
    try (MockedConstruction<RangerClientExtension> clients =
        Mockito.mockConstruction(RangerClientExtension.class)) {
      CandidateDeltaPolicyPlugin plugin = new CandidateDeltaPolicyPlugin(config());
      SecurableObject table =
          SecurableObjects.parse(
              "delta_a.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      AuthorizationSecurableObject resource = plugin.translatePrivilege(table).get(0);
      RangerPolicy owned = plugin.createPolicyAddResources(resource);
      RangerPolicy duplicate = plugin.createPolicyAddResources(resource);
      Mockito.when(plugin.getRangerClient().findPolicies(Mockito.anyMap()))
          .thenReturn(List.of(owned, duplicate));
      Assertions.assertThrows(
          AuthorizationPluginException.class, () -> plugin.findManagedPolicy(resource));
      duplicate.setPolicyLabels(
          List.of("gravitino-managed", "gravitino-metalake-id:10", "gravitino-catalog-id:21"));
      Assertions.assertSame(owned, plugin.findManagedPolicy(resource));
      duplicate.setPolicyLabels(owned.getPolicyLabels());
      duplicate.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
      Assertions.assertSame(owned, plugin.findManagedPolicy(resource));
      duplicate.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
      duplicate.getResources().remove("catalog");
      Assertions.assertSame(owned, plugin.findManagedPolicy(resource));
      duplicate.getResources().put("catalog", new RangerPolicy.RangerPolicyResource("delta_b"));
      Assertions.assertSame(owned, plugin.findManagedPolicy(resource));
      duplicate.getResources().put("catalog", new RangerPolicy.RangerPolicyResource("delta_a"));
      duplicate.getResources().get("table").setIsExcludes(true);
      Assertions.assertSame(owned, plugin.findManagedPolicy(resource));
    }
  }

  private static RangerAuthorizationHadoopSQLPlugin candidate() {
    return new CandidateDeltaPolicyPlugin(config());
  }

  private static RangerPolicy policy(long id, int type) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(id);
    policy.setName("policy-" + id);
    policy.setService("delta_local");
    policy.setPolicyType(type);
    if (type == RangerPolicy.POLICY_TYPE_ACCESS) {
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setUsers(List.of("ranger_reader"));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      policy.setPolicyItems(List.of(item));
    } else if (type == RangerPolicy.POLICY_TYPE_DATAMASK) {
      RangerPolicy.RangerDataMaskPolicyItem item = new RangerPolicy.RangerDataMaskPolicyItem();
      item.setUsers(List.of("ranger_reader"));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      item.setDataMaskInfo(
          new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", null, "'MASKED'"));
      policy.setDataMaskPolicyItems(List.of(item));
    } else {
      RangerPolicy.RangerRowFilterPolicyItem item = new RangerPolicy.RangerRowFilterPolicyItem();
      item.setUsers(List.of("ranger_reader"));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      item.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id = 1"));
      policy.setRowFilterPolicyItems(List.of(item));
    }
    policy.setResources(
        Map.of(
            "database",
            new RangerPolicy.RangerPolicyResource("sales"),
            "table",
            new RangerPolicy.RangerPolicyResource("orders")));
    return policy;
  }

  private static Map<String, String> config() {
    return Map.of(
        "authorization.ranger.admin.url",
        "http://127.0.0.1:1",
        "authorization.ranger.service.type",
        "hive",
        "authorization.ranger.service.name",
        "delta_local",
        "authorization.ranger.auth.type",
        "simple",
        "authorization.ranger.username",
        "admin",
        "authorization.ranger.password",
        "unused-local-fixture",
        "authorization.metalake.id",
        "10",
        "authorization.catalog.id",
        "20");
  }

  private static void assertRangerArtifact(Class<?> type, String artifact) {
    Assertions.assertNotNull(type.getProtectionDomain().getCodeSource());
    String location = type.getProtectionDomain().getCodeSource().getLocation().toExternalForm();
    LOG.info("RANGER RUNTIME class={} location={}", type.getName(), location);
    Assertions.assertTrue(
        location.endsWith("/" + artifact + "-2.8.0.jar"),
        () -> type.getName() + " must load from Ranger 2.8.0, but loaded " + location);
  }
}
