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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
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
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.connector.authorization.AuthorizationProvider;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.util.JsonUtilsV2;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedConstruction;
import org.slf4j.LoggerFactory;

class TestRangerDeltaAuthorization {
  @Test
  void userRemovalRejectsGlobalDeletionWithoutAdminMutation() throws Exception {
    assertPrincipalRemovalRejected(Owner.Type.USER);
  }

  @Test
  void groupRemovalRejectsGlobalDeletionWithoutAdminMutation() throws Exception {
    assertPrincipalRemovalRejected(Owner.Type.GROUP);
  }

  @Test
  void missingPrincipalAndMembershipReadbackFailuresNeverReportOwnerSuccess() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      RangerClientExtension client = plugin.getRangerClient();
      when(client.searchUser(anyMap())).thenReturn(new VXUserList());
      when(client.createUser(any())).thenReturn(false);
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onOwnerSet(
                  MetadataObjects.parse("delta_a.sales", MetadataObject.Type.SCHEMA),
                  null,
                  owner("missing", Owner.Type.USER)));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      plugin.onOwnerSet(
          MetadataObjects.parse("delta_a", MetadataObject.Type.CATALOG),
          null,
          owner("alice", Owner.Type.USER));
      RangerClientExtension client = plugin.getRangerClient();
      doAnswer(c -> c.getArgument(1)).when(client).updateRole(anyLong(), any());
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onOwnerSet(
                  MetadataObjects.parse("delta_a", MetadataObject.Type.CATALOG),
                  owner("bob", Owner.Type.USER),
                  owner("bob", Owner.Type.USER)));
      assertEquals(
          "alice",
          admin
              .roles
              .get(RangerHelper.generateCatalogOwnerRoleName("20"))
              .getUsers()
              .get(0)
              .getName());
    }
  }

  @Test
  void observedDefinitionChangeRejectsMutationAndLiteralTokensRemainLiteral() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      SecurableObject literal =
          SecurableObjects.parse(
              "delta_a.sales.{USER}",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      assertEquals("{USER}", plugin.translatePrivilege(literal).get(0).names().get(2));
      admin
          .definition
          .getRowFilterDef()
          .getResources()
          .get(0)
          .getMatcherOptions()
          .put("replaceTokens", "true");
      assertThrows(RuntimeException.class, () -> plugin.onRoleCreated(role("reader", select())));
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void unpropagatedLegacyMetalakeBindingRejectsBeforeAdminMutation() throws Exception {
    try (Admin admin = new Admin()) {
      admin.service.setConfigs(Map.of("gravitino.metalake", "lake"));
      assertThrows(RuntimeException.class, TestRangerDeltaAuthorization::plugin);
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void ordinaryRolesAreGlobalButNamesAreMetalakeScoped() throws Exception {
    try (Admin admin = new Admin()) {
      plugin().onRoleCreated(role("reader", select()));
      Map<String, String> second = new HashMap<>(config());
      second.put("authorization.metalake.id", "11");
      second.put("authorization.ranger.delta.metalake", "lake_two");
      admin.service.setConfigs(Map.of("ranger.plugin.spark.gravitino.metalake", "lake_two"));
      ((RangerAuthorizationHadoopSQLPlugin)
              provider().newPlugin("lake_two", "lakehouse-delta", second))
          .onRoleCreated(role("reader", select()));
      assertTrue(admin.roles.containsKey("GRAVITINO_METALAKE_10_ROLE_reader"));
      assertTrue(admin.roles.containsKey("GRAVITINO_METALAKE_11_ROLE_reader"));
    }
  }

  @Test
  void roleUpdatesChangeOnlyTheirCatalogWhenOldRoleSpansCatalogs() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin pluginA = plugin();
      Map<String, String> configB = new HashMap<>(config());
      configB.put("authorization.ranger.delta.catalog", "delta_b");
      configB.put("authorization.catalog.id", "21");
      RangerAuthorizationHadoopSQLPlugin pluginB =
          (RangerAuthorizationHadoopSQLPlugin)
              provider().newPlugin("lake", "lakehouse-delta", configB);
      SecurableObject selectB =
          SecurableObjects.parse(
              "delta_b.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      pluginA.onOwnerSet(select(), null, owner("alice", Owner.Type.USER));
      pluginB.onOwnerSet(selectB, null, owner("bob", Owner.Type.USER));
      pluginA.onRoleCreated(role("reader", select()));
      pluginB.onRoleCreated(role("foreign_reader", selectB));
      String baseline = admin.mapper.writeValueAsString(admin.policies);
      RangerPolicy policyA =
          admin.clonePolicies().stream()
              .filter(p -> p.getResources().get("catalog").getValues().equals(List.of("delta_a")))
              .findFirst()
              .orElseThrow();
      RangerPolicy policyB =
          admin.clonePolicies().stream()
              .filter(p -> p.getResources().get("catalog").getValues().equals(List.of("delta_b")))
              .findFirst()
              .orElseThrow();
      String rolesBefore = admin.mapper.writeValueAsString(admin.roles);
      String reader = "GRAVITINO_METALAKE_10_ROLE_reader";
      pluginB.onRoleUpdated(
          role("reader", select()), RoleChange.addSecurableObject("reader", selectB));
      assertEquals(
          admin.mapper.writeValueAsString(policyA),
          admin.mapper.writeValueAsString(admin.policies.get(policyA.getId())));
      List<RangerPolicy.RangerPolicyItem> added =
          admin.policies.get(policyB.getId()).getPolicyItems();
      assertTrue(added.stream().anyMatch(i -> i.getRoles().contains(reader)));
      assertTrue(
          added.stream()
              .anyMatch(i -> i.getRoles().contains("GRAVITINO_METALAKE_10_ROLE_foreign_reader")));
      assertEquals(
          admin.mapper.writeValueAsString(
              policyB.getPolicyItems().stream()
                  .filter(i -> i.getUsers().contains("bob"))
                  .collect(Collectors.toList())),
          admin.mapper.writeValueAsString(
              added.stream()
                  .filter(i -> i.getUsers().contains("bob"))
                  .collect(Collectors.toList())));
      pluginB.onRoleUpdated(
          role("reader", select(), selectB), RoleChange.removeSecurableObject("reader", selectB));
      assertEquals(baseline, admin.mapper.writeValueAsString(admin.policies));
      pluginB.onRoleUpdated(
          role("reader", select()), RoleChange.addSecurableObject("reader", selectB));
      pluginB.onRoleUpdated(
          role("reader", select(), selectB),
          RoleChange.updateSecurableObject(
              "reader",
              selectB,
              SecurableObjects.parse(selectB.fullName(), MetadataObject.Type.TABLE, List.of())));
      assertEquals(baseline, admin.mapper.writeValueAsString(admin.policies));
      assertEquals(rolesBefore, admin.mapper.writeValueAsString(admin.roles));
    }
  }

  @Test
  void modifyRoleChangesKeepSameNameTableInOtherCatalogUntouched() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin pluginA = plugin();
      Map<String, String> configB = new HashMap<>(config());
      configB.put("authorization.ranger.delta.catalog", "delta_b");
      configB.put("authorization.catalog.id", "21");
      RangerAuthorizationHadoopSQLPlugin pluginB =
          (RangerAuthorizationHadoopSQLPlugin)
              provider().newPlugin("lake", "lakehouse-delta", configB);
      SecurableObject modifyB =
          table("delta_b.sales.orders", List.of(Privileges.ModifyTable.allow()));
      SecurableObject mixedB =
          table(
              "delta_b.sales.orders",
              List.of(Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()));
      SecurableObject selectB =
          table("delta_b.sales.orders", List.of(Privileges.SelectTable.allow()));
      pluginA.onOwnerSet(select(), null, owner("alice", Owner.Type.USER));
      pluginB.onOwnerSet(modifyB, null, owner("bob", Owner.Type.USER));
      pluginA.onRoleCreated(role("writer", modify()));
      RangerPolicy policyA =
          admin.clonePolicies().stream()
              .filter(p -> p.getResources().get("catalog").getValues().equals(List.of("delta_a")))
              .findFirst()
              .orElseThrow();
      RangerPolicy policyB =
          admin.clonePolicies().stream()
              .filter(p -> p.getResources().get("catalog").getValues().equals(List.of("delta_b")))
              .findFirst()
              .orElseThrow();
      String unchangedA = admin.mapper.writeValueAsString(policyA);
      String unchangedB = admin.mapper.writeValueAsString(policyB);
      pluginB.onRoleUpdated(
          role("writer", modify()), RoleChange.addSecurableObject("writer", modifyB));
      assertEquals(List.of("update"), roleAccesses(admin.policies.get(policyB.getId()), "writer"));
      assertEquals(
          unchangedA, admin.mapper.writeValueAsString(admin.policies.get(policyA.getId())));
      pluginB.onRoleUpdated(
          role("writer", modify(), modifyB),
          RoleChange.updateSecurableObject("writer", modifyB, mixedB));
      assertEquals(
          List.of("select", "update"), roleAccesses(admin.policies.get(policyB.getId()), "writer"));
      pluginB.onRoleUpdated(
          role("writer", modify(), mixedB),
          RoleChange.updateSecurableObject("writer", mixedB, selectB));
      assertEquals(List.of("select"), roleAccesses(admin.policies.get(policyB.getId()), "writer"));
      pluginB.onRoleUpdated(
          role("writer", modify(), selectB), RoleChange.removeSecurableObject("writer", selectB));
      assertEquals(
          unchangedB, admin.mapper.writeValueAsString(admin.policies.get(policyB.getId())));
      assertEquals(
          unchangedA, admin.mapper.writeValueAsString(admin.policies.get(policyA.getId())));
    }
  }

  @Test
  void roleUpdateDoesNotTranslateForeignUnsupportedContext() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      SecurableObject foreign =
          SecurableObjects.parse(
              "other_provider.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.CreateTable.allow()));
      plugin.onRoleUpdated(
          role("reader", foreign), RoleChange.addSecurableObject("reader", select()));
      assertEquals(1, admin.policies.size());
      assertTrue(
          admin.policies.values().iterator().next().getPolicyItems().stream()
              .anyMatch(i -> i.getRoles().contains("GRAVITINO_METALAKE_10_ROLE_reader")));
    }
  }

  @Test
  void explicitRoleChangesStillRejectBeforeAnyAdminInteraction() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      SecurableObject foreign =
          SecurableObjects.parse(
              "delta_b.sales.orders",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      SecurableObject unsupported =
          SecurableObjects.parse(
              select().fullName(),
              MetadataObject.Type.TABLE,
              List.of(Privileges.CreateTable.allow()));
      SecurableObject foreignModify =
          table("delta_b.sales.orders", List.of(Privileges.ModifyTable.allow()));
      SecurableObject deniedModify =
          table(select().fullName(), List.of(Privileges.ModifyTable.deny()));
      SecurableObject wrongType =
          table(select().fullName(), List.of(Privileges.UseCatalog.allow()));
      SecurableObject denied =
          SecurableObjects.parse(
              select().fullName(),
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.deny()));
      SecurableObject wildcard =
          SecurableObjects.parse(
              "delta_a.sales.o*",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      SecurableObject renamed =
          SecurableObjects.parse(
              "delta_a.sales.renamed",
              MetadataObject.Type.TABLE,
              List.of(Privileges.SelectTable.allow()));
      List<Executable> rejected = new ArrayList<>();
      for (SecurableObject object :
          List.of(foreign, foreignModify, unsupported, denied, deniedModify, wrongType, wildcard)) {
        for (SecurableObject valid : List.of(select(), modify())) {
          rejected.add(
              () ->
                  plugin.onRoleUpdated(
                      role("reader"),
                      RoleChange.addSecurableObject("reader", valid),
                      RoleChange.addSecurableObject("reader", object)));
        }
        rejected.add(
            () ->
                plugin.onRoleUpdated(
                    role("reader"), RoleChange.removeSecurableObject("reader", object)));
        rejected.add(
            () ->
                plugin.onRoleUpdated(
                    role("reader"), RoleChange.updateSecurableObject("reader", select(), object)));
      }
      rejected.add(
          () ->
              plugin.onRoleUpdated(
                  role("reader"), RoleChange.updateSecurableObject("reader", select(), renamed)));
      rejected.add(
          () ->
              plugin.onRoleUpdated(
                  role("GRAVITINO_reserved"),
                  RoleChange.addSecurableObject("GRAVITINO_reserved", select())));
      rejected.add(() -> plugin.onRoleCreated(role("reader", foreign)));
      rejected.add(() -> plugin.onRoleAcquired(role("reader", foreign)));
      rejected.add(() -> plugin.onRoleDeleted(role("reader", foreign)));
      for (Executable operation : rejected) {
        admin.construction.constructed().forEach(client -> clearInvocations(client));
        admin.baseConstruction.constructed().forEach(client -> clearInvocations(client));
        assertThrows(IllegalArgumentException.class, operation);
        admin.construction.constructed().forEach(client -> verifyNoInteractions(client));
        admin.baseConstruction.constructed().forEach(client -> verifyNoInteractions(client));
      }
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void ranger28DecoderCannotReadLegacyUserModel() throws Exception {
    String json = "{\"vXUsers\":[{\"id\":1,\"name\":\"alice\"}],\"resultSize\":1,\"totalCount\":1}";
    Exception failure =
        assertThrows(Exception.class, () -> JsonUtilsV2.jsonToObj(json, VXUserList.class));
    LoggerFactory.getLogger(TestRangerDeltaAuthorization.class)
        .info("Ranger 2.8 legacy decoding failure", failure);
  }

  @Test
  void productionProviderIsDiscoverable() {
    assertEquals(
        1,
        ServiceLoader.load(AuthorizationProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(provider -> provider.shortName().equals("ranger-delta"))
            .count());
  }

  @Test
  void bindingAndServiceAreValidatedBeforeWrites() throws Exception {
    try (Admin admin = new Admin()) {
      BaseAuthorization<?> spi = provider();
      assertThrows(IllegalArgumentException.class, () -> spi.newPlugin("lake", "hive", config()));
      assertThrows(
          IllegalArgumentException.class,
          () -> spi.newPlugin("wrong", "lakehouse-delta", config()));
      Map<String, String> invalid = new HashMap<>(config());
      invalid.put("authorization.ranger.delta.catalog", "delta_?");
      assertThrows(
          IllegalArgumentException.class, () -> spi.newPlugin("lake", "lakehouse-delta", invalid));
      admin.definition.getResources().get(0).getMatcherOptions().put("ignoreCase", "true");
      assertThrows(
          IllegalArgumentException.class, () -> spi.newPlugin("lake", "lakehouse-delta", config()));
      assertTrue(admin.policies.isEmpty());
      assertTrue(admin.roles.isEmpty());
    }
  }

  @Test
  void missingMetalakeDefinitionBindingRejectsBeforeWrites() throws Exception {
    try (Admin admin = new Admin()) {
      admin.definition.setConfigs(List.of());
      assertThrows(IllegalArgumentException.class, TestRangerDeltaAuthorization::plugin);
      assertTrue(admin.roles.isEmpty());
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void selectAndVisibilityAreDisjointAndUnsupportedCombinationsReject() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      AuthorizationSecurableObject translated = plugin.translatePrivilege(select()).get(0);
      assertEquals(List.of("delta_a", "sales", "orders", "*"), translated.names());
      assertEquals("select", translated.privileges().get(0).getName());
      assertEquals(Privilege.Condition.ALLOW, translated.privileges().get(0).condition());
      assertTrue(plugin.translatePrivilege(visibility()).isEmpty());
      for (SecurableObject unsupported :
          List.of(
              SecurableObjects.parse(
                  "delta_a.sales.orders",
                  MetadataObject.Type.TABLE,
                  List.of(Privileges.SelectTable.deny())),
              SecurableObjects.parse(
                  "delta_b.sales.orders",
                  MetadataObject.Type.TABLE,
                  List.of(Privileges.SelectTable.allow())),
              SecurableObjects.parse(
                  "delta_a.sales.o*",
                  MetadataObject.Type.TABLE,
                  List.of(Privileges.SelectTable.allow())),
              SecurableObjects.parse(
                  "delta_a.sales",
                  MetadataObject.Type.SCHEMA,
                  List.of(Privileges.CreateTable.allow())))) {
        assertThrows(RuntimeException.class, () -> plugin.translatePrivilege(unsupported));
      }
      plugin.onRoleCreated(role("visible", visibility()));
      assertTrue(admin.policies.isEmpty());
      plugin.onRoleCreated(role("reader", visibility(), select()));
      assertEquals(1, admin.policies.size());
      assertTrue(admin.roles.containsKey("GRAVITINO_METALAKE_10_ROLE_reader"));
      plugin.onRoleUpdated(
          role("reader", visibility()), RoleChange.removeSecurableObject("reader", select()));
      assertTrue(admin.policies.values().stream().allMatch(p -> p.getPolicyItems().isEmpty()));
    }
  }

  @Test
  void modifyTranslationIsOnlyUpdateAndMixedAccessesAreUnique() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      assertTrue(plugin.allowPrivilegesRule().contains(Privilege.Name.MODIFY_TABLE));
      assertEquals(
          Set.of(RangerPrivileges.RangerHadoopSQLPrivilege.UPDATE),
          plugin.privilegesMappingRule().get(Privilege.Name.MODIFY_TABLE));
      SecurableObject modify = modify();
      AuthorizationSecurableObject update = plugin.translatePrivilege(modify).get(0);
      assertEquals(List.of("delta_a", "sales", "orders", "*"), update.names());
      assertEquals(List.of("update"), accessNames(update));
      assertEquals(Privilege.Condition.ALLOW, update.privileges().get(0).condition());
      for (List<Privilege> privileges :
          List.of(
              List.<Privilege>of(Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()),
              List.<Privilege>of(Privileges.ModifyTable.allow(), Privileges.SelectTable.allow()),
              List.<Privilege>of(
                  Privileges.ModifyTable.allow(),
                  Privileges.SelectTable.allow(),
                  Privileges.ModifyTable.allow(),
                  Privileges.SelectTable.allow()))) {
        AuthorizationSecurableObject both =
            plugin.translatePrivilege(table("delta_a.sales.orders", privileges)).get(0);
        assertEquals(Set.of("select", "update"), Set.copyOf(accessNames(both)));
        assertEquals(2, both.privileges().size());
      }
      assertTrue(plugin.translatePrivilege(table("delta_a.sales.orders", List.of())).isEmpty());
      assertTrue(plugin.translatePrivilege(visibility()).isEmpty());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              plugin.translatePrivilege(
                  table(
                      "delta_a.sales.orders",
                      List.of(Privileges.ModifyTable.allow(), Privileges.SelectTable.deny()))));
      assertTrue(admin.policies.isEmpty());
    }
  }

  @Test
  void modifyCallbacksPreserveOwnersOtherRolesAndForeignModels() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      plugin.onOwnerSet(select(), null, owner("alice", Owner.Type.USER));
      plugin.onRoleCreated(role("other", select(), modify()));
      RangerPolicy original = admin.clonePolicies().get(0);
      RangerPolicy foreignAccess = admin.copy(original, RangerPolicy.class);
      foreignAccess.setId(900L);
      foreignAccess.setPolicyLabels(List.of());
      foreignAccess.getPolicyItems().get(0).setUsers(List.of("foreign_user"));
      admin.policies.put(900L, foreignAccess);
      RangerPolicy foreignMask = admin.copy(original, RangerPolicy.class);
      foreignMask.setId(901L);
      foreignMask.setPolicyType(RangerPolicy.POLICY_TYPE_DATAMASK);
      foreignMask.setPolicyLabels(List.of());
      foreignMask.setPolicyItems(List.of());
      RangerPolicy.RangerDataMaskPolicyItem maskItem = new RangerPolicy.RangerDataMaskPolicyItem();
      maskItem.setUsers(List.of("foreign_user"));
      maskItem.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      maskItem.setDataMaskInfo(
          new RangerPolicy.RangerPolicyItemDataMaskInfo("CUSTOM", null, "'MASKED'"));
      foreignMask.setDataMaskPolicyItems(List.of(maskItem));
      admin.policies.put(901L, foreignMask);
      RangerPolicy foreignFilter = admin.copy(original, RangerPolicy.class);
      foreignFilter.setId(902L);
      foreignFilter.setPolicyType(RangerPolicy.POLICY_TYPE_ROWFILTER);
      foreignFilter.setPolicyLabels(List.of());
      foreignFilter.setPolicyItems(List.of());
      RangerPolicy.RangerRowFilterPolicyItem filterItem =
          new RangerPolicy.RangerRowFilterPolicyItem();
      filterItem.setGroups(List.of("foreign_group"));
      filterItem.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      filterItem.setRowFilterInfo(new RangerPolicy.RangerPolicyItemRowFilterInfo("id = 1"));
      foreignFilter.setRowFilterPolicyItems(List.of(filterItem));
      admin.policies.put(902L, foreignFilter);
      String untouched =
          admin.mapper.writeValueAsString(
              List.of(
                  admin.policies.get(900L), admin.policies.get(901L), admin.policies.get(902L)));
      String ownerAndOther = admin.mapper.writeValueAsString(admin.policies.get(original.getId()));

      plugin.onRoleCreated(role("writer", modify()));
      assertEquals(List.of("update"), roleAccesses(admin.policies.get(original.getId()), "writer"));
      assertEquals(untouched, foreignModels(admin));
      plugin.onRoleUpdated(
          role("writer", modify()), RoleChange.updateSecurableObject("writer", modify(), mixed()));
      assertEquals(
          List.of("select", "update"),
          roleAccesses(admin.policies.get(original.getId()), "writer"));
      assertEquals(untouched, foreignModels(admin));
      plugin.onRoleUpdated(
          role("writer", mixed()), RoleChange.updateSecurableObject("writer", mixed(), select()));
      assertEquals(List.of("select"), roleAccesses(admin.policies.get(original.getId()), "writer"));
      assertEquals(untouched, foreignModels(admin));
      plugin.onRoleUpdated(
          role("writer", select()), RoleChange.updateSecurableObject("writer", select(), modify()));
      assertEquals(List.of("update"), roleAccesses(admin.policies.get(original.getId()), "writer"));
      assertEquals(untouched, foreignModels(admin));
      plugin.onRoleUpdated(
          role("writer", modify()), RoleChange.removeSecurableObject("writer", modify()));
      assertEquals(
          ownerAndOther, admin.mapper.writeValueAsString(admin.policies.get(original.getId())));
      assertEquals(untouched, foreignModels(admin));
    }
  }

  @Test
  void ownerReconciliationRepairsDesiredStateAndPreservesDistinctScopes() throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      plugin.onRoleCreated(role("reader", select()));
      for (MetadataObject object :
          List.of(
              MetadataObjects.of(null, "lake", MetadataObject.Type.METALAKE),
              MetadataObjects.parse("delta_a", MetadataObject.Type.CATALOG),
              MetadataObjects.parse("delta_a.sales", MetadataObject.Type.SCHEMA),
              MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE))) {
        plugin.onOwnerSet(object, null, owner("alice", Owner.Type.USER));
        plugin.onOwnerSet(object, owner("bob", Owner.Type.USER), owner("bob", Owner.Type.USER));
      }
      assertEquals(3, admin.policies.size());
      assertTrue(
          admin.policies.values().stream()
              .flatMap(p -> p.getPolicyItems().stream())
              .noneMatch(i -> i.getUsers().contains("alice")));
      assertEquals(
          List.of("bob"),
          admin.roles.get(RangerHelper.generateMetalakeOwnerRoleName("10")).getUsers().stream()
              .map(RangerRole.RoleMember::getName)
              .collect(Collectors.toList()));
      assertTrue(admin.roles.get(RangerHelper.GRAVITINO_OWNER_ROLE).getUsers().isEmpty());
      assertTrue(
          admin.policies.values().stream()
              .flatMap(p -> p.getPolicyItems().stream())
              .anyMatch(i -> i.getRoles().contains("GRAVITINO_METALAKE_10_ROLE_reader")));
      assertTrue(
          admin.policies.values().stream()
              .flatMap(p -> p.getPolicyItems().stream())
              .noneMatch(i -> Boolean.TRUE.equals(i.getDelegateAdmin())));
      admin.failWrites = true;
      assertThrows(
          RuntimeException.class,
          () ->
              plugin.onOwnerSet(
                  MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE),
                  owner("bob", Owner.Type.USER),
                  owner("team", Owner.Type.GROUP)));
      admin.failWrites = false;
      plugin.onOwnerSet(
          MetadataObjects.parse("delta_a.sales.orders", MetadataObject.Type.TABLE),
          owner("team", Owner.Type.GROUP),
          owner("team", Owner.Type.GROUP));
      assertTrue(
          admin.policies.values().stream()
              .flatMap(p -> p.getPolicyItems().stream())
              .anyMatch(i -> i.getGroups().equals(List.of("team"))));
    }
  }

  static BaseAuthorization<?> provider() {
    return (BaseAuthorization<?>)
        ServiceLoader.load(AuthorizationProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(p -> p.shortName().equals("ranger-delta"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Production SPI absent"));
  }

  static RangerAuthorizationHadoopSQLPlugin plugin() {
    return (RangerAuthorizationHadoopSQLPlugin)
        provider().newPlugin("lake", "lakehouse-delta", config());
  }

  static Map<String, String> config() {
    return Map.of(
        "authorization.ranger.admin.url",
        "http://127.0.0.1:1",
        "authorization.ranger.service.type",
        "spark",
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
        "20",
        "authorization.ranger.delta.metalake",
        "lake",
        "authorization.ranger.delta.catalog",
        "delta_a");
  }

  static SecurableObject select() {
    return SecurableObjects.parse(
        "delta_a.sales.orders", MetadataObject.Type.TABLE, List.of(Privileges.SelectTable.allow()));
  }

  static SecurableObject modify() {
    return table("delta_a.sales.orders", List.of(Privileges.ModifyTable.allow()));
  }

  static SecurableObject mixed() {
    return table(
        "delta_a.sales.orders",
        List.of(Privileges.SelectTable.allow(), Privileges.ModifyTable.allow()));
  }

  static SecurableObject table(String name, List<Privilege> privileges) {
    return SecurableObjects.parse(name, MetadataObject.Type.TABLE, privileges);
  }

  static List<String> accessNames(AuthorizationSecurableObject object) {
    return object.privileges().stream().map(p -> p.getName()).sorted().collect(Collectors.toList());
  }

  static List<String> roleAccesses(RangerPolicy policy, String role) {
    String rangerRole = "GRAVITINO_METALAKE_10_ROLE_" + role;
    return policy.getPolicyItems().stream()
        .filter(item -> item.getRoles().contains(rangerRole))
        .flatMap(item -> item.getAccesses().stream())
        .map(RangerPolicy.RangerPolicyItemAccess::getType)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
  }

  static String foreignModels(Admin admin) throws Exception {
    return admin.mapper.writeValueAsString(
        List.of(admin.policies.get(900L), admin.policies.get(901L), admin.policies.get(902L)));
  }

  static SecurableObject visibility() {
    return SecurableObjects.parse(
        "delta_a", MetadataObject.Type.CATALOG, List.of(Privileges.UseCatalog.allow()));
  }

  static Role role(String name, SecurableObject... objects) {
    Role role = mock(Role.class);
    when(role.name()).thenReturn(name);
    when(role.securableObjects()).thenReturn(List.of(objects));
    return role;
  }

  static Owner owner(String name, Owner.Type type) {
    Owner owner = mock(Owner.class);
    when(owner.name()).thenReturn(name);
    when(owner.type()).thenReturn(type);
    return owner;
  }

  private void assertPrincipalRemovalRejected(Owner.Type type) throws Exception {
    try (Admin admin = new Admin()) {
      RangerAuthorizationHadoopSQLPlugin plugin = plugin();
      RangerClientExtension client = plugin.getRangerClient();
      plugin.onRoleCreated(role("reader", select()));
      RangerPolicy foreign = admin.clonePolicies().get(0);
      foreign.setId(900L);
      foreign.setService("independent_service");
      foreign.setPolicyLabels(List.of());
      foreign.getPolicyItems().get(0).setUsers(List.of("shared_user"));
      foreign.getPolicyItems().get(0).setGroups(List.of("shared_group"));
      admin.policies.put(900L, foreign);
      VXUserList users = new VXUserList();
      users
          .getList()
          .add(admin.mapper.convertValue(Map.of("id", 71L, "name", "shared_user"), VXUser.class));
      VXGroupList groups = new VXGroupList();
      groups
          .getList()
          .add(admin.mapper.convertValue(Map.of("id", 72L, "name", "shared_group"), VXGroup.class));
      when(client.searchUser(anyMap())).thenReturn(users);
      when(client.searchGroup(anyMap())).thenReturn(groups);
      when(client.deleteUser(anyLong()))
          .thenAnswer(
              call -> {
                users.getList().clear();
                return true;
              });
      when(client.deleteGroup(anyLong()))
          .thenAnswer(
              call -> {
                groups.getList().clear();
                return true;
              });
      User user = mock(User.class);
      when(user.name()).thenReturn("shared_user");
      Group group = mock(Group.class);
      when(group.name()).thenReturn("shared_group");
      Map<String, Object> state =
          Map.of(
              "users", users, "groups", groups, "policies", admin.policies, "roles", admin.roles);
      String before = admin.mapper.writeValueAsString(state);
      Executable removal =
          type == Owner.Type.USER
              ? () -> plugin.onUserRemoved(user)
              : () -> plugin.onGroupRemoved(group);
      clearInvocations(client);
      assertAll(
          () -> assertThrows(UnsupportedOperationException.class, removal),
          () -> verifyNoInteractions(client),
          () -> assertEquals(before, admin.mapper.writeValueAsString(state)));
    }
  }

  static final class Admin implements AutoCloseable {
    final ObjectMapper mapper = new ObjectMapper();
    final Map<Long, RangerPolicy> policies = new LinkedHashMap<>();
    final Map<String, RangerRole> roles = new LinkedHashMap<>();
    final RangerServiceDef definition;
    final RangerService service = new RangerService();
    final MockedConstruction<DeltaRangerClientExtension> construction;
    final MockedConstruction<RangerClientExtension> baseConstruction;
    boolean failWrites;
    long id = 100;

    Admin() throws Exception {
      definition =
          mapper.readValue(
              Path.of("src/main/resources/ranger-delta-servicedef.json").toFile(),
              RangerServiceDef.class);
      service.setName("delta_local");
      service.setType("spark");
      service.setConfigs(Map.of("ranger.plugin.spark.gravitino.metalake", "lake"));
      baseConstruction = mockConstruction(RangerClientExtension.class);
      construction =
          mockConstruction(
              DeltaRangerClientExtension.class,
              (client, context) -> {
                when(client.getService(anyString())).thenReturn(service);
                when(client.getServiceDef(anyString())).thenAnswer(c -> definition);
                when(client.findPolicies(anyMap())).thenAnswer(c -> clonePolicies());
                when(client.createPolicy(any()))
                    .thenAnswer(
                        c -> {
                          checkWrite();
                          RangerPolicy p = c.getArgument(0);
                          p.setId(++id);
                          policies.put(p.getId(), copy(p, RangerPolicy.class));
                          return copy(p, RangerPolicy.class);
                        });
                when(client.updatePolicy(anyLong(), any()))
                    .thenAnswer(
                        c -> {
                          checkWrite();
                          RangerPolicy p = c.getArgument(1);
                          policies.put(p.getId(), copy(p, RangerPolicy.class));
                          return copy(p, RangerPolicy.class);
                        });
                when(client.getPolicy(anyLong()))
                    .thenAnswer(c -> copy(policies.get(c.getArgument(0)), RangerPolicy.class));
                doAnswer(
                        c -> {
                          checkWrite();
                          policies.remove(c.getArgument(0));
                          return null;
                        })
                    .when(client)
                    .deletePolicy(anyLong());
                when(client.getRole(anyString(), anyString(), anyString()))
                    .thenAnswer(c -> copy(roles.get(c.getArgument(0)), RangerRole.class));
                when(client.createRole(anyString(), any()))
                    .thenAnswer(
                        c -> {
                          checkWrite();
                          RangerRole r = c.getArgument(1);
                          r.setId(++id);
                          roles.put(r.getName(), copy(r, RangerRole.class));
                          return copy(r, RangerRole.class);
                        });
                when(client.updateRole(anyLong(), any()))
                    .thenAnswer(
                        c -> {
                          checkWrite();
                          RangerRole r = c.getArgument(1);
                          roles.put(r.getName(), copy(r, RangerRole.class));
                          return copy(r, RangerRole.class);
                        });
                doAnswer(
                        c -> {
                          checkWrite();
                          roles.remove(c.getArgument(0));
                          return null;
                        })
                    .when(client)
                    .deleteRole(anyString(), anyString(), anyString());
                when(client.searchUser(anyMap()))
                    .thenAnswer(
                        c -> {
                          VXUserList users = new VXUserList();
                          users
                              .getList()
                              .add(
                                  VXUser.builder()
                                      .withName(
                                          ((Map<String, String>) c.getArgument(0)).get("name"))
                                      .build());
                          return users;
                        });
                when(client.searchGroup(anyMap()))
                    .thenAnswer(
                        c -> {
                          VXGroupList groups = new VXGroupList();
                          groups
                              .getList()
                              .add(
                                  VXGroup.builder()
                                      .withName(
                                          ((Map<String, String>) c.getArgument(0)).get("name"))
                                      .build());
                          return groups;
                        });
              });
    }

    List<RangerPolicy> clonePolicies() throws Exception {
      List<RangerPolicy> result = new ArrayList<>();
      for (RangerPolicy p : policies.values()) {
        result.add(copy(p, RangerPolicy.class));
      }
      return result;
    }

    <T> T copy(T value, Class<T> type) throws Exception {
      return value == null ? null : mapper.readValue(mapper.writeValueAsBytes(value), type);
    }

    void checkWrite() {
      if (failWrites) {
        throw new IllegalStateException("Injected Admin failure");
      }
    }

    @Override
    public void close() {
      construction.close();
      baseConstruction.close();
    }
  }
}
