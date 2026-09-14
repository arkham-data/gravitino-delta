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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestDeltaRangerPolicySelector {
  @Test
  void foreignPolicyRoleReferencePreventsSilentDeletionSuccess() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
      plugin.onRoleCreated(
          TestRangerDeltaAuthorization.role("reader", TestRangerDeltaAuthorization.select()));
      RangerPolicy foreign = admin.clonePolicies().get(0);
      foreign.setId(900L);
      foreign.setPolicyLabels(List.of());
      admin.policies.put(900L, foreign);
      String before = admin.mapper.writeValueAsString(foreign);
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onRoleDeleted(
                  TestRangerDeltaAuthorization.role(
                      "reader", TestRangerDeltaAuthorization.select())));
      assertEquals(before, admin.mapper.writeValueAsString(admin.policies.get(900L)));
      assertEquals(1, admin.roles.size());
    }
  }

  @Test
  void foreignReferencesInEveryPolicyItemCollectionBlockRoleDeletion() throws Exception {
    for (int kind = 0; kind < 6; kind++) {
      try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
        RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
        plugin.onRoleCreated(
            TestRangerDeltaAuthorization.role("reader", TestRangerDeltaAuthorization.select()));
        RangerPolicy foreign = admin.clonePolicies().get(0);
        RangerPolicy.RangerPolicyItem item = foreign.getPolicyItems().get(0);
        foreign.setPolicyItems(List.of());
        foreign.setId(900L);
        foreign.setPolicyLabels(List.of());
        switch (kind) {
          case 0:
            foreign.setPolicyItems(List.of(item));
            break;
          case 1:
            foreign.setDenyPolicyItems(List.of(item));
            break;
          case 2:
            foreign.setAllowExceptions(List.of(item));
            break;
          case 3:
            foreign.setDenyExceptions(List.of(item));
            break;
          case 4:
            RangerPolicy.RangerDataMaskPolicyItem mask =
                new RangerPolicy.RangerDataMaskPolicyItem();
            mask.setRoles(item.getRoles());
            foreign.setDataMaskPolicyItems(List.of(mask));
            foreign.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
            break;
          default:
            RangerPolicy.RangerRowFilterPolicyItem filter =
                new RangerPolicy.RangerRowFilterPolicyItem();
            filter.setRoles(item.getRoles());
            foreign.setRowFilterPolicyItems(List.of(filter));
            foreign.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
        }
        admin.policies.put(900L, foreign);
        String before = admin.mapper.writeValueAsString(foreign);
        assertThrows(
            RuntimeException.class,
            () ->
                plugin.onRoleDeleted(
                    TestRangerDeltaAuthorization.role(
                        "reader", TestRangerDeltaAuthorization.select())),
            "item collection " + kind);
        assertEquals(before, admin.mapper.writeValueAsString(admin.policies.get(900L)));
        assertEquals(1, admin.roles.size());
      }
    }
  }

  @Test
  void malformedSiblingPolicyCannotJustifySuccessfulRoleDeletion() throws Exception {
    List<Consumer<RangerPolicy>> changes =
        List.of(
            p -> p.getResources().get("table").setValues(List.of("ord*")),
            p -> p.getResources().get("column").setValues(List.of("secret")),
            p -> p.getResources().get("database").setIsRecursive(true),
            p -> p.getPolicyLabels().remove("gravitino-metalake-id:10"),
            p -> p.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK),
            p -> {
              p.setDenyPolicyItems(p.getPolicyItems());
              p.setPolicyItems(List.of());
            });
    for (Consumer<RangerPolicy> change : changes) {
      try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
        RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
        plugin.onRoleCreated(
            TestRangerDeltaAuthorization.role("reader", TestRangerDeltaAuthorization.select()));
        RangerPolicy foreign = admin.clonePolicies().get(0);
        foreign.setId(900L);
        foreign.setPolicyLabels(
            new ArrayList<>(
                List.of(
                    "gravitino-managed", "gravitino-metalake-id:10", "gravitino-catalog-id:21")));
        foreign.getResources().get("catalog").setValues(List.of("delta_b"));
        change.accept(foreign);
        admin.policies.put(900L, foreign);
        String before = admin.mapper.writeValueAsString(foreign);
        assertThrows(
            RuntimeException.class,
            () ->
                plugin.onRoleDeleted(
                    TestRangerDeltaAuthorization.role(
                        "reader", TestRangerDeltaAuthorization.select())));
        assertEquals(before, admin.mapper.writeValueAsString(admin.policies.get(900L)));
        assertEquals(1, admin.roles.size());
      }
    }
  }

  @Test
  void sharedRoleIsDeletedAfterBothCatalogPolicyContributionsAreRemoved() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin first = TestRangerDeltaAuthorization.plugin();
      SecurableObject a = TestRangerDeltaAuthorization.select();
      SecurableObject b =
          SecurableObjects.parse(
              "delta_b.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      Map<String, String> config = new HashMap<>(TestRangerDeltaAuthorization.config());
      config.put("authorization.ranger.delta.catalog", "delta_b");
      config.put("authorization.catalog.id", "21");
      RangerAuthorizationHadoopSQLPlugin second =
          (RangerAuthorizationHadoopSQLPlugin)
              TestRangerDeltaAuthorization.provider().newPlugin("lake", "lakehouse-delta", config);
      first.onRoleCreated(TestRangerDeltaAuthorization.role("reader", a));
      second.onRoleCreated(TestRangerDeltaAuthorization.role("reader", b));
      first.onRoleDeleted(TestRangerDeltaAuthorization.role("reader", a));
      assertEquals(1, admin.roles.size(), "shared role must survive the first catalog removal");
      second.onRoleDeleted(TestRangerDeltaAuthorization.role("reader", b));
      assertTrue(admin.roles.isEmpty());
    }
  }

  @Test
  void foreignOrdinaryRoleIsRejectedBeforeMembershipMutation() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
      RangerRole foreign =
          new RangerRole("GRAVITINO_METALAKE_10_ROLE_reader", "foreign", null, null, null);
      foreign.setId(7L);
      admin.roles.put(foreign.getName(), foreign);
      User user = Mockito.mock(User.class);
      Mockito.when(user.name()).thenReturn("alice");
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onGrantedRolesToUser(
                  List.of(
                      TestRangerDeltaAuthorization.role(
                          "reader", TestRangerDeltaAuthorization.select())),
                  user));
    }
  }

  @Test
  void exactSelectorRefusesAdversarialCandidatesAndDuplicates() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
      plugin.onRoleCreated(
          TestRangerDeltaAuthorization.role("reader", TestRangerDeltaAuthorization.select()));
      RangerPolicy owned = admin.clonePolicies().get(0);
      AuthorizationSecurableObject resource =
          plugin.translatePrivilege(TestRangerDeltaAuthorization.select()).get(0);
      List<Consumer<RangerPolicy>> changes =
          List.of(
              p -> p.setPolicyType(1),
              p -> p.setPolicyType(2),
              p -> p.setService("foreign"),
              p -> p.getPolicyLabels().remove("gravitino-managed"),
              p -> p.getPolicyLabels().remove("gravitino-metalake-id:10"),
              p -> p.getPolicyLabels().remove("gravitino-catalog-id:20"),
              p -> p.getResources().remove("catalog"),
              p -> p.getResources().put("extra", new RangerPolicy.RangerPolicyResource("*")),
              p -> p.getResources().get("catalog").setValues(List.of("*")),
              p -> p.getResources().get("database").setValues(List.of("sales", "other")),
              p -> p.getResources().get("table").setValues(List.of("ord*")),
              p -> p.getResources().get("table").setIsExcludes(true),
              p -> p.getResources().get("table").setIsRecursive(true));
      for (Consumer<RangerPolicy> change : changes) {
        RangerPolicy candidate = admin.copy(owned, RangerPolicy.class);
        change.accept(candidate);
        admin.policies.clear();
        admin.policies.put(candidate.getId(), candidate);
        assertNull(plugin.findManagedPolicy(resource));
      }
      admin.policies.clear();
      admin.policies.put(owned.getId(), owned);
      RangerPolicy duplicate = admin.copy(owned, RangerPolicy.class);
      duplicate.setId(999L);
      admin.policies.put(999L, duplicate);
      assertThrows(RuntimeException.class, () -> plugin.findManagedPolicy(resource));
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onOwnerSet(
                  MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE),
                  null,
                  TestRangerDeltaAuthorization.owner("alice", Owner.Type.USER)));
    }
  }

  @Test
  void foreignAccessMasksAndFiltersSurviveGrantRevokeOwnerAndSecondCatalog() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
      plugin.onRoleCreated(
          TestRangerDeltaAuthorization.role("seed", TestRangerDeltaAuthorization.select()));
      RangerPolicy owned = admin.clonePolicies().get(0);
      List<RangerPolicy> foreign = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        RangerPolicy p = admin.copy(owned, RangerPolicy.class);
        p.setId(900L + i);
        p.setPolicyType(i);
        p.setPolicyLabels(List.of());
        p.setName("foreign" + i);
        foreign.add(p);
        admin.policies.put(p.getId(), p);
      }
      String before = admin.mapper.writeValueAsString(foreign);
      plugin.onRoleCreated(
          TestRangerDeltaAuthorization.role("reader", TestRangerDeltaAuthorization.select()));
      plugin.onRoleUpdated(
          TestRangerDeltaAuthorization.role("reader"),
          RoleChange.removeSecurableObject("reader", TestRangerDeltaAuthorization.select()));
      plugin.onOwnerSet(
          MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE),
          null,
          TestRangerDeltaAuthorization.owner("alice", Owner.Type.USER));
      Map<String, String> config = new HashMap<>(TestRangerDeltaAuthorization.config());
      config.put("authorization.ranger.delta.catalog", "delta_b");
      config.put("authorization.catalog.id", "21");
      RangerAuthorizationHadoopSQLPlugin second =
          (RangerAuthorizationHadoopSQLPlugin)
              TestRangerDeltaAuthorization.provider().newPlugin("lake", "lakehouse-delta", config);
      second.onRoleCreated(
          TestRangerDeltaAuthorization.role(
              "reader",
              SecurableObjects.parse(
                  "delta_b.sales.orders",
                  MetadataObject.Type.TABLE,
                  List.of(Privileges.SelectTable.allow()))));
      assertEquals(
          1, admin.roles.values().stream().filter(r -> r.getName().endsWith("_reader")).count());
      assertEquals(
          before,
          admin.mapper.writeValueAsString(
              List.of(
                  admin.policies.get(900L), admin.policies.get(901L), admin.policies.get(902L))));
    }
  }

  @Test
  void reservedRawRolesAndConflictingOwnerRolesFailBeforeMutations() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = TestRangerDeltaAuthorization.plugin();
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onRoleCreated(
                  TestRangerDeltaAuthorization.role(
                      "GRAVITINO_anything", TestRangerDeltaAuthorization.select())));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
      RangerRole foreign = new RangerRole("GRAVITINO_OWNER_ROLE", "foreign", null, null, null);
      foreign.setId(5L);
      admin.roles.put(foreign.getName(), foreign);
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onOwnerSet(
                  MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE),
                  null,
                  TestRangerDeltaAuthorization.owner("alice", Owner.Type.USER)));
      assertTrue(admin.policies.isEmpty());
      assertEquals("foreign", admin.roles.get(foreign.getName()).getDescription());
    }
  }
}
