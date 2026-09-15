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

import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.plugin;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.role;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.select;
import static org.apache.gravitino.authorization.ranger.delta.TestRangerDeltaAuthorization.visibility;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.junit.jupiter.api.Test;

class TestDeltaRangerSchemaVisibility {
  @Test
  void schemaOnlyLifecycleAndMembershipNeverCreateDataAccess() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      SecurableObject schema = schema("delta_a.sales", Privileges.UseSchema.allow());
      SecurableObject empty =
          SecurableObjects.parse("delta_a.sales", MetadataObject.Type.SCHEMA, List.of());
      assertTrue(plugin.allowPrivilegesRule().contains(Privilege.Name.USE_SCHEMA));
      assertTrue(plugin.allowMetadataObjectTypesRule().contains(MetadataObject.Type.SCHEMA));
      assertTrue(plugin.privilegesMappingRule().get(Privilege.Name.USE_SCHEMA).isEmpty());
      assertTrue(plugin.translatePrivilege(schema).isEmpty());
      Role role = role("schema_reader", schema);
      assertTrue(plugin.onRoleCreated(role));
      assertTrue(plugin.onRoleAcquired(role));
      assertEquals(1, admin.roles.size());
      assertTrue(admin.policies.isEmpty());
      User user = mock(User.class);
      when(user.name()).thenReturn("metadata_reader");
      Group group = mock(Group.class);
      when(group.name()).thenReturn("metadata_group");
      assertTrue(plugin.onGrantedRolesToUser(List.of(role), user));
      assertTrue(plugin.onRevokedRolesFromUser(List.of(role), user));
      assertTrue(plugin.onGrantedRolesToGroup(List.of(role), group));
      assertTrue(plugin.onRevokedRolesFromGroup(List.of(role), group));
      assertTrue(admin.policies.isEmpty());
      assertTrue(
          plugin.onRoleUpdated(
              role("schema_reader", empty),
              RoleChange.updateSecurableObject("schema_reader", schema, empty)));
      assertTrue(
          plugin.onRoleUpdated(
              role, RoleChange.updateSecurableObject("schema_reader", empty, schema)));
      assertTrue(
          plugin.onRoleUpdated(
              role("schema_reader"), RoleChange.removeSecurableObject("schema_reader", schema)));
      assertTrue(
          plugin.onRoleUpdated(role, RoleChange.addSecurableObject("schema_reader", schema)));
      assertTrue(plugin.onRoleDeleted(role));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void mixedSchemaVisibilityPreservesOnlyExactTableSelect() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      SecurableObject schema = schema("delta_a.sales", Privileges.UseSchema.allow());
      Role role = role("mixed", visibility(), schema, select());
      assertTrue(plugin.onRoleCreated(role));
      assertEquals(1, admin.policies.size());
      String policyBefore = admin.mapper.writeValueAsString(admin.policies);
      assertEquals(
          List.of("orders"),
          admin.policies.values().iterator().next().getResources().get("table").getValues());
      assertEquals(
          List.of("*"),
          admin.policies.values().iterator().next().getResources().get("column").getValues());
      assertTrue(
          plugin.onRoleUpdated(
              role("mixed", visibility(), select()),
              RoleChange.removeSecurableObject("mixed", schema)));
      assertEquals(policyBefore, admin.mapper.writeValueAsString(admin.policies));
      assertTrue(plugin.onRoleUpdated(role, RoleChange.addSecurableObject("mixed", schema)));
      assertEquals(policyBefore, admin.mapper.writeValueAsString(admin.policies));
      assertTrue(
          plugin.onRoleUpdated(
              role("mixed", visibility(), schema),
              RoleChange.removeSecurableObject("mixed", select())));
      assertTrue(admin.policies.isEmpty());
      assertTrue(plugin.onRoleDeleted(role("mixed", visibility(), schema)));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void invalidSchemaAndCrossTypePrivilegesRejectBeforeAnyClientCall() throws Exception {
    try (TestRangerDeltaAuthorization.Admin admin = new TestRangerDeltaAuthorization.Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      for (SecurableObject invalid :
          List.of(
              schema("delta_b.sales", Privileges.UseSchema.allow()),
              schema("delta_a.s*", Privileges.UseSchema.allow()),
              schema("delta_a.sales", Privileges.UseSchema.deny()),
              schema("delta_a.sales", Privileges.SelectTable.allow()),
              schema("delta_a.sales", Privileges.UseCatalog.allow()),
              schema("delta_a.sales", Privileges.CreateTable.allow()),
              SecurableObjects.parse(
                  "delta_a", MetadataObject.Type.CATALOG, List.of(Privileges.UseSchema.allow())),
              SecurableObjects.parse(
                  "delta_a.sales.orders",
                  MetadataObject.Type.TABLE,
                  List.of(Privileges.UseSchema.allow())))) {
        clearInvocations(plugin.getRangerClient());
        assertThrows(
            IllegalArgumentException.class, () -> plugin.onRoleCreated(role("invalid", invalid)));
        verifyNoInteractions(plugin.getRangerClient());
        assertTrue(admin.roles.isEmpty());
        assertTrue(admin.policies.isEmpty());
      }
    }
  }

  private static SecurableObject schema(String name, Privilege privilege) {
    return SecurableObjects.parse(name, MetadataObject.Type.SCHEMA, List.of(privilege));
  }
}
