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

import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.accessNames;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.foreignModels;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.owner;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.plugin;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.role;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.roleAccesses;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.select;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.visibility;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.junit.jupiter.api.Test;

class TestDeltaRangerCreateGrants {
  @Test
  void schemaCreateTranslationIsExactAndVisibilityStaysEmpty() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      assertTrue(plugin.allowPrivilegesRule().contains(Privilege.Name.CREATE_TABLE));
      assertEquals(
          Set.of(RangerPrivileges.RangerHadoopSQLPrivilege.CREATE),
          plugin.privilegesMappingRule().get(Privilege.Name.CREATE_TABLE));
      for (List<Privilege> privileges :
          List.of(
              List.<Privilege>of(Privileges.CreateTable.allow()),
              List.<Privilege>of(Privileges.CreateTable.allow(), Privileges.UseSchema.allow()),
              List.<Privilege>of(Privileges.UseSchema.allow(), Privileges.CreateTable.allow()),
              List.<Privilege>of(
                  Privileges.CreateTable.allow(),
                  Privileges.UseSchema.allow(),
                  Privileges.CreateTable.allow()))) {
        List<AuthorizationSecurableObject> translated =
            plugin.translatePrivilege(schema(privileges));
        assertEquals(1, translated.size());
        assertEquals(List.of("delta_a", "sales", "*", "*"), translated.get(0).names());
        assertEquals(List.of("create"), accessNames(translated.get(0)));
        assertEquals(Privilege.Condition.ALLOW, translated.get(0).privileges().get(0).condition());
      }
      assertTrue(plugin.translatePrivilege(schema(List.of())).isEmpty());
      assertTrue(
          plugin.translatePrivilege(schema(List.of(Privileges.UseSchema.allow()))).isEmpty());
      assertTrue(plugin.translatePrivilege(visibility()).isEmpty());
      assertEquals(Set.of(), plugin.privilegesMappingRule().get(Privilege.Name.USE_SCHEMA));
      assertEquals(Set.of(), plugin.privilegesMappingRule().get(Privilege.Name.USE_CATALOG));
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void invalidCreateGrantsRejectBeforeAdminCalls() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      List<SecurableObject> invalid = new ArrayList<>();
      for (String name :
          List.of(
              "delta_b.sales",
              "*.sales",
              "delta_a.*",
              "delta_a.sa?es",
              "delta_a",
              "delta_a.sales.orders",
              "",
              "delta_a.",
              ".sales",
              "delta_a..sales")) {
        invalid.add(raw(name, MetadataObject.Type.SCHEMA, List.of(Privileges.CreateTable.allow())));
      }
      for (MetadataObject.Type type :
          List.of(
              MetadataObject.Type.TABLE,
              MetadataObject.Type.COLUMN,
              MetadataObject.Type.CATALOG,
              MetadataObject.Type.METALAKE)) {
        String name =
            type == MetadataObject.Type.TABLE
                ? "delta_a.sales.orders"
                : type == MetadataObject.Type.COLUMN
                    ? "delta_a.sales.orders.id"
                    : type == MetadataObject.Type.CATALOG ? "delta_a" : "lake";
        invalid.add(raw(name, type, List.of(Privileges.CreateTable.allow())));
      }
      invalid.add(schema(List.of(Privileges.CreateTable.deny())));
      invalid.add(schema(List.of(Privileges.CreateTable.allow(), Privileges.UseSchema.deny())));
      invalid.add(schema(List.of(Privileges.CreateTable.allow(), Privileges.SelectTable.allow())));
      invalid.add(schema(List.of(Privileges.CreateTable.allow(), Privileges.ModifyTable.allow())));
      invalid.add(schema(List.of(Privileges.CreateTable.allow(), Privileges.UseCatalog.allow())));
      invalid.add(
          raw("delta_a.sales", MetadataObject.Type.SCHEMA, Collections.singletonList(null)));
      User user = mock(User.class);
      Group group = mock(Group.class);
      for (SecurableObject object : invalid) {
        Role invalidRole = role("invalid", object);
        clearInvocations(plugin.getRangerClient());
        assertThrows(IllegalArgumentException.class, () -> plugin.translatePrivilege(object));
        assertThrows(IllegalArgumentException.class, () -> plugin.onRoleCreated(invalidRole));
        assertThrows(IllegalArgumentException.class, () -> plugin.onRoleAcquired(invalidRole));
        assertThrows(IllegalArgumentException.class, () -> plugin.onRoleDeleted(invalidRole));
        assertThrows(
            IllegalArgumentException.class,
            () ->
                plugin.onRoleUpdated(
                    role("invalid"), RoleChange.addSecurableObject("invalid", object)));
        assertThrows(
            IllegalArgumentException.class,
            () ->
                plugin.onRoleUpdated(
                    role("invalid"), RoleChange.removeSecurableObject("invalid", object)));
        assertThrows(
            IllegalArgumentException.class,
            () ->
                plugin.onRoleUpdated(
                    role("invalid"),
                    RoleChange.updateSecurableObject("invalid", create(), object)));
        assertThrows(
            IllegalArgumentException.class,
            () ->
                plugin.onRoleUpdated(
                    role("invalid"),
                    RoleChange.updateSecurableObject("invalid", object, create())));
        assertThrows(
            IllegalArgumentException.class,
            () -> plugin.onGrantedRolesToUser(List.of(invalidRole), user));
        assertThrows(
            IllegalArgumentException.class,
            () -> plugin.onRevokedRolesFromUser(List.of(invalidRole), user));
        assertThrows(
            IllegalArgumentException.class,
            () -> plugin.onGrantedRolesToGroup(List.of(invalidRole), group));
        assertThrows(
            IllegalArgumentException.class,
            () -> plugin.onRevokedRolesFromGroup(List.of(invalidRole), group));
        verifyNoInteractions(plugin.getRangerClient());
        assertTrue(admin.roles.isEmpty());
        assertTrue(admin.policies.isEmpty());
      }
    }
  }

  @Test
  void schemaCreateRoleLifecyclePreservesOwnerAndForeignPolicies() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      plugin.onOwnerSet(create(), null, owner("alice", Owner.Type.USER));
      assertTrue(plugin.onRoleCreated(role("other", create())));
      RangerPolicy owned = plugin.findManagedPolicy(plugin.translateOwner(create()).get(0));
      long ownedId = owned.getId();
      String ownerAndOther = admin.mapper.writeValueAsString(owned);
      assertEquals(List.of("delta_a"), owned.getResources().get("catalog").getValues());
      assertEquals(List.of("sales"), owned.getResources().get("database").getValues());
      assertEquals(List.of("*"), owned.getResources().get("table").getValues());
      assertEquals(List.of("*"), owned.getResources().get("column").getValues());
      plugin.onRoleCreated(role("table_reader", select()));
      RangerPolicy table = plugin.findManagedPolicy(plugin.translatePrivilege(select()).get(0));
      String tableBefore = admin.mapper.writeValueAsString(table);
      installForeignPolicies(admin, owned);
      String foreignBefore = foreignModels(admin);
      SecurableObject mixed =
          schema(List.of(Privileges.CreateTable.allow(), Privileges.UseSchema.allow()));
      SecurableObject visible = schema(List.of(Privileges.UseSchema.allow()));
      Role creator = role("creator", mixed);
      assertTrue(plugin.onRoleCreated(creator));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertEquals(List.of("create"), roleAccesses(admin.policies.get(ownedId), "creator"));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertEquals(List.of("create"), roleAccesses(admin.policies.get(ownedId), "other"));
      assertEquals(
          1,
          admin.policies.get(ownedId).getPolicyItems().stream()
              .filter(item -> item.getUsers().equals(List.of("alice")))
              .count());
      String created = admin.mapper.writeValueAsString(admin.policies);
      String roles = admin.mapper.writeValueAsString(admin.roles);
      assertTrue(plugin.onRoleAcquired(creator));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      plugin = plugin();
      assertEquals(created, admin.mapper.writeValueAsString(admin.policies));
      assertEquals(roles, admin.mapper.writeValueAsString(admin.roles));
      assertTrue(
          plugin.onRoleUpdated(
              creator, RoleChange.updateSecurableObject("creator", mixed, visible)));
      assertEquals(ownerAndOther, admin.mapper.writeValueAsString(admin.policies.get(ownedId)));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertTrue(
          plugin.onRoleUpdated(
              role("creator", visible),
              RoleChange.updateSecurableObject("creator", visible, mixed)));
      assertEquals(List.of("create"), roleAccesses(admin.policies.get(ownedId), "creator"));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertTrue(plugin.onRoleUpdated(creator, RoleChange.removeSecurableObject("creator", mixed)));
      assertEquals(ownerAndOther, admin.mapper.writeValueAsString(admin.policies.get(ownedId)));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertTrue(
          plugin.onRoleUpdated(role("creator"), RoleChange.addSecurableObject("creator", mixed)));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertTrue(plugin.onRoleDeleted(creator));
      assertFalse(admin.roles.containsKey("GRAVITINO_METALAKE_10_ROLE_creator"));
      assertEquals(ownerAndOther, admin.mapper.writeValueAsString(admin.policies.get(ownedId)));
      assertUnrelatedPolicies(admin, table, tableBefore, foreignBefore);
      assertEquals(tableBefore, admin.mapper.writeValueAsString(admin.policies.get(table.getId())));
      assertEquals(foreignBefore, foreignModels(admin));
      assertEquals(5, admin.policies.size());
      RangerPolicy duplicate = admin.copy(admin.policies.get(ownedId), RangerPolicy.class);
      duplicate.setId(999L);
      admin.policies.put(999L, duplicate);
      String beforeDuplicate = admin.mapper.writeValueAsString(admin.policies);
      RangerAuthorizationHadoopSQLPlugin current = plugin;
      assertThrows(
          RuntimeException.class,
          () -> current.findManagedPolicy(current.translatePrivilege(create()).get(0)));
      assertThrows(
          RuntimeException.class,
          () ->
              current.onRoleUpdated(
                  role("other", create()), RoleChange.addSecurableObject("other", create())));
      assertEquals(beforeDuplicate, admin.mapper.writeValueAsString(admin.policies));
    }
  }

  @Test
  void schemaCreateMembershipAndRepeatedChangesStayBounded() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      doAnswer(
              call -> {
                updateMembership(admin, call.getArgument(1), true);
                return null;
              })
          .when(plugin.getRangerClient())
          .grantRole(anyString(), any());
      doAnswer(
              call -> {
                updateMembership(admin, call.getArgument(1), false);
                return null;
              })
          .when(plugin.getRangerClient())
          .revokeRole(anyString(), any());
      Role creator = role("creator", create());
      assertTrue(plugin.onRoleCreated(creator));
      User user = mock(User.class);
      when(user.name()).thenReturn("schema_creator");
      Group group = mock(Group.class);
      when(group.name()).thenReturn("schema_creators");
      String policies = admin.mapper.writeValueAsString(admin.policies);
      for (int iteration = 0; iteration < 2; iteration++) {
        assertTrue(plugin.onRoleCreated(creator));
        assertTrue(plugin.onRoleAcquired(creator));
        assertTrue(
            plugin.onRoleUpdated(creator, RoleChange.addSecurableObject("creator", create())));
        assertTrue(plugin.onGrantedRolesToUser(List.of(creator), user));
        assertTrue(plugin.onGrantedRolesToUser(List.of(creator), user));
        assertTrue(plugin.onGrantedRolesToGroup(List.of(creator), group));
        assertTrue(plugin.onGrantedRolesToGroup(List.of(creator), group));
        RangerRole rangerRole = admin.roles.get("GRAVITINO_METALAKE_10_ROLE_creator");
        assertEquals(List.of("schema_creator"), memberNames(rangerRole.getUsers()));
        assertEquals(List.of("schema_creators"), memberNames(rangerRole.getGroups()));
        assertTrue(rangerRole.getUsers().stream().noneMatch(RangerRole.RoleMember::getIsAdmin));
        assertTrue(rangerRole.getGroups().stream().noneMatch(RangerRole.RoleMember::getIsAdmin));
        assertEquals(policies, admin.mapper.writeValueAsString(admin.policies));
        assertTrue(plugin.onRevokedRolesFromUser(List.of(creator), user));
        assertTrue(plugin.onRevokedRolesFromUser(List.of(creator), user));
        assertTrue(rangerRole.getUsers().isEmpty());
        assertEquals(List.of("schema_creators"), memberNames(rangerRole.getGroups()));
        assertTrue(plugin.onRevokedRolesFromGroup(List.of(creator), group));
        assertTrue(plugin.onRevokedRolesFromGroup(List.of(creator), group));
        assertTrue(rangerRole.getGroups().isEmpty());
        assertEquals(policies, admin.mapper.writeValueAsString(admin.policies));
        assertEquals(1, admin.roles.size());
        assertEquals(List.of("create"), roleAccesses(admin.clonePolicies().get(0), "creator"));
        assertTrue(
            plugin.onRoleUpdated(creator, RoleChange.removeSecurableObject("creator", create())));
        assertTrue(
            plugin.onRoleUpdated(
                role("creator"), RoleChange.removeSecurableObject("creator", create())));
        assertTrue(admin.policies.isEmpty());
        assertTrue(
            plugin.onRoleUpdated(
                role("creator"), RoleChange.addSecurableObject("creator", create())));
        RangerPolicy replacement = admin.clonePolicies().get(0);
        assertEquals(List.of("*"), replacement.getResources().get("table").getValues());
        assertEquals(List.of("*"), replacement.getResources().get("column").getValues());
        assertEquals(List.of("create"), roleAccesses(replacement, "creator"));
        policies = admin.mapper.writeValueAsString(admin.policies);
      }
      assertTrue(plugin.onRoleDeleted(creator));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  private static SecurableObject create() {
    return schema(List.of(Privileges.CreateTable.allow()));
  }

  private static SecurableObject schema(List<Privilege> privileges) {
    return SecurableObjects.parse("delta_a.sales", MetadataObject.Type.SCHEMA, privileges);
  }

  private static SecurableObject raw(
      String name, MetadataObject.Type type, List<Privilege> privileges) {
    SecurableObject object = mock(SecurableObject.class);
    when(object.fullName()).thenReturn(name);
    when(object.type()).thenReturn(type);
    when(object.privileges()).thenReturn(privileges);
    return object;
  }

  private static void assertUnrelatedPolicies(
      TestRangerDeltaAuthorization.Admin admin,
      RangerPolicy table,
      String tableBefore,
      String foreignBefore)
      throws Exception {
    assertEquals(tableBefore, admin.mapper.writeValueAsString(admin.policies.get(table.getId())));
    assertEquals(foreignBefore, foreignModels(admin));
  }

  private static void installForeignPolicies(
      TestRangerDeltaAuthorization.Admin admin, RangerPolicy owned) throws Exception {
    for (int type = 0; type < 3; type++) {
      RangerPolicy foreign = admin.copy(owned, RangerPolicy.class);
      foreign.setId(900L + type);
      foreign.setName("foreign_" + type);
      foreign.setPolicyLabels(List.of());
      foreign.setPolicyType(type);
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setUsers(List.of("foreign_user"));
      item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      foreign.setPolicyItems(type == 0 ? List.of(item) : List.of());
      if (type == RangerPolicy.POLICY_TYPE_DATAMASK) {
        RangerPolicy.RangerDataMaskPolicyItem mask = new RangerPolicy.RangerDataMaskPolicyItem();
        mask.setUsers(item.getUsers());
        mask.setAccesses(item.getAccesses());
        mask.setDataMaskInfo(
            new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", null, "'MASKED'"));
        foreign.setDataMaskPolicyItems(List.of(mask));
      } else if (type == RangerPolicy.POLICY_TYPE_ROWFILTER) {
        RangerPolicy.RangerRowFilterPolicyItem filter =
            new RangerPolicy.RangerRowFilterPolicyItem();
        filter.setUsers(item.getUsers());
        filter.setAccesses(item.getAccesses());
        filter.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id = 1"));
        foreign.setRowFilterPolicyItems(List.of(filter));
      }
      admin.policies.put(foreign.getId(), foreign);
    }
  }

  private static void updateMembership(
      TestRangerDeltaAuthorization.Admin admin, GrantRevokeRoleRequest request, boolean grant) {
    assertEquals("admin", request.getGrantor());
    assertEquals(Set.of("GRAVITINO_METALAKE_10_ROLE_creator"), request.getTargetRoles());
    for (String name : request.getTargetRoles()) {
      RangerRole role = admin.roles.get(name);
      updateMembers(role.getUsers(), request.getUsers(), grant);
      updateMembers(role.getGroups(), request.getGroups(), grant);
    }
  }

  private static void updateMembers(
      List<RangerRole.RoleMember> members, Set<String> principals, boolean grant) {
    for (String principal : principals) {
      members.removeIf(member -> member.getName().equals(principal));
      if (grant) {
        members.add(new RangerRole.RoleMember(principal, false));
      }
    }
  }

  private static List<String> memberNames(List<RangerRole.RoleMember> members) {
    return members.stream().map(RangerRole.RoleMember::getName).collect(Collectors.toList());
  }
}
