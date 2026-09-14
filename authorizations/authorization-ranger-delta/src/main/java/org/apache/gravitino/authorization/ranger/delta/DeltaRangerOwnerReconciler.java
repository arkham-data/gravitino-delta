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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXGroupList;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.authorization.ranger.reference.VXUserList;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.codehaus.jackson.map.ObjectMapper;

/** Desired-state reconciliation for exclusively owned roles and marked policy items. */
final class DeltaRangerOwnerReconciler {
  private final RangerDeltaAuthorizationPlugin plugin;
  private final RangerClientExtension client;
  private final String service;
  private final String admin;

  DeltaRangerOwnerReconciler(
      RangerDeltaAuthorizationPlugin plugin,
      RangerClientExtension client,
      String service,
      String admin) {
    this.plugin = plugin;
    this.client = client;
    this.service = service;
    this.admin = admin;
  }

  boolean roleExists(String name) {
    return readRole(name) != null;
  }

  void ensureOrdinaryRole(String name) {
    ensureRole(plugin.getRangerHelper().generateGravitinoRoleName(name));
  }

  void verifyOrdinaryRole(String name) {
    String qualified = plugin.getRangerHelper().generateGravitinoRoleName(name);
    validateRole(readRole(qualified), qualified);
  }

  void ensurePrincipal(String name, Owner.Type type) {
    DeltaRangerAuthorizationConfig.require(
        name != null && !name.isBlank(), "Owner principal required");
    if (!principalExists(name, type)) {
      if (type == Owner.Type.USER) {
        client.createUser(VXUser.builder().withName(name).withDescription(name).build());
      } else {
        client.createGroup(VXGroup.builder().withName(name).withDescription(name).build());
      }
    }
    if (!principalExists(name, type)) {
      throw new AuthorizationPluginException("Ranger principal readback failed");
    }
  }

  void reconcile(AuthorizationSecurableObject resource, String ownerRole, Owner desired) {
    RangerPolicy policy = plugin.findManagedPolicy(resource);
    boolean direct = ownerRole.equals(RangerHelper.GRAVITINO_OWNER_ROLE);
    if (policy != null) {
      validateItems(policy, ownerRole, direct);
    }
    // Validate reserved identities before principal or policy changes.
    RangerRole existing = readRole(ownerRole);
    if (existing != null) {
      validateRole(existing, ownerRole);
      if (direct && !empty(existing)) {
        throw new AuthorizationPluginException("Owner marker role must have no membership");
      }
    }
    ensurePrincipal(desired.name(), desired.type());
    RangerRole role = ensureRole(ownerRole);
    if (direct) {
      if (!empty(role)) {
        throw new AuthorizationPluginException("Owner marker role must have no membership");
      }
    } else {
      List<RangerRole.RoleMember> member =
          List.of(new RangerRole.RoleMember(desired.name(), false));
      role.setUsers(desired.type() == Owner.Type.USER ? member : List.of());
      role.setGroups(desired.type() == Owner.Type.GROUP ? member : List.of());
      role.setRoles(List.of());
      try {
        client.updateRole(role.getId(), role);
      } catch (RangerServiceException e) {
        throw new AuthorizationPluginException(e, "Owner role update failed");
      }
      RangerRole actual = readRole(ownerRole);
      validateRole(actual, ownerRole);
      if (!role.getUsers().equals(actual.getUsers())
          || !role.getGroups().equals(actual.getGroups())
          || !actual.getRoles().isEmpty()) {
        throw new AuthorizationPluginException("Owner role membership readback mismatch");
      }
    }
    if (policy == null) {
      policy = plugin.createPolicyAddResources(resource);
    }
    policy.getPolicyItems().removeIf(item -> item.getRoles().contains(ownerRole));
    RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
    item.setRoles(List.of(ownerRole));
    item.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("all", true)));
    item.setDelegateAdmin(false);
    if (direct) {
      item.setUsers(desired.type() == Owner.Type.USER ? List.of(desired.name()) : List.of());
      item.setGroups(desired.type() == Owner.Type.GROUP ? List.of(desired.name()) : List.of());
    }
    policy.getPolicyItems().add(item);
    try {
      if (policy.getId() == null) {
        client.createPolicy(policy);
      } else {
        client.updatePolicy(policy.getId(), policy);
      }
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Owner policy update failed");
    }
    RangerPolicy actual = plugin.findManagedPolicy(resource);
    if (actual == null) {
      throw new AuthorizationPluginException("Owner policy missing on readback");
    }
    validateItems(actual, ownerRole, direct);
    List<RangerPolicy.RangerPolicyItem> items =
        actual.getPolicyItems().stream()
            .filter(i -> i.getRoles().contains(ownerRole))
            .collect(Collectors.toList());
    if (items.size() != 1 || !items.get(0).equals(item)) {
      throw new AuthorizationPluginException("Owner policy readback mismatch");
    }
    RangerRole finalRole = readRole(ownerRole);
    validateRole(finalRole, ownerRole);
    if (!role.getUsers().equals(finalRole.getUsers())
        || !role.getGroups().equals(finalRole.getGroups())
        || !finalRole.getRoles().isEmpty()) {
      throw new AuthorizationPluginException("Owner role changed during reconciliation");
    }
  }

  private void validateItems(RangerPolicy policy, String role, boolean direct) {
    for (RangerPolicy.RangerPolicyItem item : policy.getPolicyItems()) {
      if (!item.getRoles().contains(role)) {
        continue;
      }
      if (!item.getRoles().equals(List.of(role))
          || item.getAccesses().size() != 1
          || !"all".equals(item.getAccesses().get(0).getType())
          || !Boolean.TRUE.equals(item.getAccesses().get(0).getIsAllowed())
          || Boolean.TRUE.equals(item.getDelegateAdmin())
          || !item.getConditions().isEmpty()
          || (!direct && (!item.getUsers().isEmpty() || !item.getGroups().isEmpty()))) {
        throw new AuthorizationPluginException("Conflicting reserved owner policy item");
      }
    }
    if (policy.getDenyPolicyItems().stream().anyMatch(i -> i.getRoles().contains(role))
        || policy.getAllowExceptions().stream().anyMatch(i -> i.getRoles().contains(role))
        || policy.getDenyExceptions().stream().anyMatch(i -> i.getRoles().contains(role))) {
      throw new AuthorizationPluginException("Conflicting reserved owner policy item");
    }
  }

  private RangerRole ensureRole(String name) {
    RangerRole role = readRole(name);
    if (role == null) {
      try {
        client.createRole(
            service, new RangerRole(name, RangerHelper.MANAGED_BY_GRAVITINO, null, null, null));
      } catch (RangerServiceException e) {
        throw new AuthorizationPluginException(e, "Ranger role creation failed");
      }
    }
    role = readRole(name);
    validateRole(role, name);
    return role;
  }

  @Nullable
  private RangerRole readRole(String name) {
    try {
      return client.getRole(name, admin, service);
    } catch (RangerServiceException e) {
      if (e.getStatus() != null && e.getStatus().getStatusCode() == 404) {
        return null;
      }
      // Ranger 2.8 reports missing roles with this permission-shaped error. Creation is
      // still followed by authoritative identity validation; other failures propagate.
      if (e.getMessage() != null
          && e.getMessage().contains("User doesn't have permissions to get details")) {
        return null;
      }
      throw new AuthorizationPluginException(e, "Ranger role readback failed");
    }
  }

  private void validateRole(@Nullable RangerRole role, String name) {
    if (role == null
        || role.getId() == null
        || !name.equals(role.getName())
        || !Objects.equals(RangerHelper.MANAGED_BY_GRAVITINO, role.getDescription())
        || !role.getOptions().isEmpty()) {
      throw new AuthorizationPluginException("Missing or conflicting managed Ranger role");
    }
  }

  private boolean empty(RangerRole role) {
    return role.getUsers().isEmpty() && role.getGroups().isEmpty() && role.getRoles().isEmpty();
  }

  private boolean principalExists(String name, Owner.Type type) {
    if (type == Owner.Type.USER) {
      VXUserList users = client.searchUser(Map.of("name", name));
      return users != null && users.getList().stream().anyMatch(u -> name.equals(u.getName()));
    }
    VXGroupList groups = client.searchGroup(Map.of("name", name));
    return groups != null
        && groups.getList().stream()
            .anyMatch(g -> name.equals(new ObjectMapper().convertValue(g, Map.class).get("name")));
  }
}
