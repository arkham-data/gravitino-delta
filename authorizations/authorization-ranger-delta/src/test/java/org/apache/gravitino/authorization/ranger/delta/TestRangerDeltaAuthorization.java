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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.RangerHelper;
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
import org.mockito.MockedConstruction;
import org.slf4j.LoggerFactory;

class TestRangerDeltaAuthorization {
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
  void ordinaryRolesAreGlobalButNamesAreMetalakeScoped() throws Exception {
    try (Admin admin = new Admin()) {
      plugin().onRoleCreated(role("reader", select()));
      Map<String, String> second = new HashMap<>(config());
      second.put("authorization.metalake.id", "11");
      second.put("authorization.ranger.delta.metalake", "lake_two");
      admin.service.setConfigs(Map.of("gravitino.metalake", "lake_two"));
      ((RangerAuthorizationHadoopSQLPlugin)
              provider().newPlugin("lake_two", "lakehouse-delta", second))
          .onRoleCreated(role("reader", select()));
      assertTrue(admin.roles.containsKey("GRAVITINO_METALAKE_10_ROLE_reader"));
      assertTrue(admin.roles.containsKey("GRAVITINO_METALAKE_11_ROLE_reader"));
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
      service.setConfigs(Map.of("gravitino.metalake", "lake"));
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
