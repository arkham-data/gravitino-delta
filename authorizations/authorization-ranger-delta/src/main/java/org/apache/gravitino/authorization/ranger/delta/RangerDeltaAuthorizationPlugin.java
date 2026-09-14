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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationMetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.authorization.ranger.RangerAuthorizationHadoopSQLPlugin;
import org.apache.gravitino.authorization.ranger.RangerHelper;
import org.apache.gravitino.authorization.ranger.RangerPrivileges;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;

/** Four-level Delta callback translation with exact mutation ownership. */
public final class RangerDeltaAuthorizationPlugin extends RangerAuthorizationHadoopSQLPlugin {
  private final DeltaRangerAuthorizationConfig binding;
  private final DeltaRangerPolicySelector selector;
  private final DeltaRangerOwnerReconciler owners;
  /**
   * Creates a plugin after validating provisioned service identity and definition.
   *
   * @param metalake canonical metalake
   * @param properties trusted authorization configuration
   */
  public RangerDeltaAuthorizationPlugin(String metalake, Map<String, String> properties) {
    super(metalake, DeltaRangerAuthorizationConfig.prepare(metalake, properties));
    rangerClient =
        new DeltaRangerClientExtension(
            properties.get(RangerAuthorizationProperties.RANGER_ADMIN_URL),
            properties.get(RangerAuthorizationProperties.RANGER_AUTH_TYPE),
            properties.get(RangerAuthorizationProperties.RANGER_USERNAME),
            properties.get(RangerAuthorizationProperties.RANGER_PASSWORD));
    binding = new DeltaRangerAuthorizationConfig(metalake, properties);
    selector = new DeltaRangerPolicySelector(rangerServiceName, metalakeId, catalogId);
    rangerHelper =
        new DeltaRangerHelper(
            rangerClient,
            rangerAdminName,
            rangerServiceName,
            ownerMappingRule(),
            policyResourceDefinesRule(),
            metalakeId);
    owners = new DeltaRangerOwnerReconciler(this, rangerClient, rangerServiceName, rangerAdminName);
    validateService();
  }
  /** {@inheritDoc} */
  @Override
  public List<String> policyResourceDefinesRule() {
    return DeltaRangerResource.KEYS;
  }
  /** {@inheritDoc} */
  @Override
  public Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule() {
    return Map.of(
        Privilege.Name.SELECT_TABLE,
        Set.of(RangerPrivileges.RangerHadoopSQLPrivilege.SELECT),
        Privilege.Name.USE_CATALOG,
        Set.of());
  }
  /** {@inheritDoc} */
  @Override
  public Set<Privilege.Name> allowPrivilegesRule() {
    return Set.of(Privilege.Name.SELECT_TABLE, Privilege.Name.USE_CATALOG);
  }
  /** {@inheritDoc} */
  @Override
  public Set<MetadataObject.Type> allowMetadataObjectTypesRule() {
    return Set.of(MetadataObject.Type.CATALOG, MetadataObject.Type.TABLE);
  }
  /** {@inheritDoc} */
  @Override
  public List<AuthorizationSecurableObject> translatePrivilege(SecurableObject object) {
    List<String> names = literalNames(object);
    if (object.type() != MetadataObject.Type.TABLE
        && object.type() != MetadataObject.Type.CATALOG) {
      throw new IllegalArgumentException("Unsupported Delta grant type");
    }
    for (Privilege privilege : object.privileges()) {
      if (privilege == null
          || privilege.condition() != Privilege.Condition.ALLOW
          || (object.type() == MetadataObject.Type.TABLE
              ? privilege.name() != Privilege.Name.SELECT_TABLE
              : privilege.name() != Privilege.Name.USE_CATALOG)) {
        throw new IllegalArgumentException("Unsupported Delta privilege or condition");
      }
    }
    if (object.privileges().isEmpty() || object.type() == MetadataObject.Type.CATALOG) {
      return List.of();
    }
    names.add("*");
    return List.of(
        new DeltaRangerResource(
            names,
            List.of(
                new RangerPrivileges.RangerHivePrivilegeImpl(
                    RangerPrivileges.RangerHadoopSQLPrivilege.SELECT, Privilege.Condition.ALLOW))));
  }
  /** {@inheritDoc} */
  @Override
  public List<AuthorizationMetadataObject> translateMetadataObject(MetadataObject object) {
    return new ArrayList<>(translateOwner(object));
  }
  /** {@inheritDoc} */
  @Override
  public List<AuthorizationSecurableObject> translateOwner(MetadataObject object) {
    List<String> names;
    if (object.type() == MetadataObject.Type.METALAKE) {
      DeltaRangerAuthorizationConfig.require(
          metalake.equals(object.name()), "Owner metalake mismatch");
      names = new ArrayList<>(List.of(binding.catalog));
    } else {
      names = literalNames(object);
    }
    while (names.size() < 4) {
      names.add("*");
    }
    return List.of(new DeltaRangerResource(names, new ArrayList<>(ownerMappingRule())));
  }
  /** {@inheritDoc} */
  @Override
  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      List<String> names,
      AuthorizationMetadataObject.Type type,
      Set<AuthorizationPrivilege> privileges) {
    throw new UnsupportedOperationException("Use validated Delta translation");
  }
  /** {@inheritDoc} */
  @Override
  public AuthorizationSecurableObject generateAuthorizationSecurableObject(
      AuthorizationMetadataObject object, Set<AuthorizationPrivilege> privileges) {
    if (!(object instanceof DeltaRangerResource)) {
      throw new IllegalArgumentException("Typed Delta resource required");
    }
    return new DeltaRangerResource(object.names(), new ArrayList<>(privileges));
  }
  /** {@inheritDoc} */
  @Override
  @Nullable
  public RangerPolicy findManagedPolicy(AuthorizationMetadataObject object) {
    try {
      return selector.select(
          rangerClient.findPolicies(Map.of(SearchFilter.SERVICE_NAME, rangerServiceName)), object);
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Delta policy lookup failed");
    }
  }
  /** {@inheritDoc} */
  @Override
  public boolean validAuthorizationOperation(List<SecurableObject> objects) {
    objects.forEach(this::translatePrivilege);
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onRoleCreated(Role role) {
    validateRole(role);
    validateService();
    owners.ensureOrdinaryRole(role.name());
    return super.onRoleCreated(role);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes) {
    validateRole(role);
    for (RoleChange change : changes) {
      if (change instanceof RoleChange.AddSecurableObject) {
        translatePrivilege(((RoleChange.AddSecurableObject) change).getSecurableObject());
      } else if (change instanceof RoleChange.RemoveSecurableObject) {
        translatePrivilege(((RoleChange.RemoveSecurableObject) change).getSecurableObject());
      } else if (change instanceof RoleChange.UpdateSecurableObject) {
        RoleChange.UpdateSecurableObject update = (RoleChange.UpdateSecurableObject) change;
        translatePrivilege(update.getSecurableObject());
        translatePrivilege(update.getNewSecurableObject());
        DeltaRangerAuthorizationConfig.require(
            update.getSecurableObject().fullName().equals(update.getNewSecurableObject().fullName())
                && update.getSecurableObject().type() == update.getNewSecurableObject().type(),
            "Grant resource cannot change");
      } else {
        throw new IllegalArgumentException("Unsupported Delta role change");
      }
    }
    validateService();
    owners.ensureOrdinaryRole(role.name());
    return super.onRoleUpdated(role, changes);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onRoleDeleted(Role role) {
    validateRole(role);
    validateService();
    onRoleUpdated(
        role,
        role.securableObjects().stream()
            .map(object -> RoleChange.removeSecurableObject(role.name(), object))
            .toArray(RoleChange[]::new));
    String name = rangerHelper.generateGravitinoRoleName(role.name());
    try {
      List<RangerPolicy> remaining =
          rangerClient.findPolicies(Map.of(SearchFilter.SERVICE_NAME, rangerServiceName));
      boolean siblingReference = false;
      for (RangerPolicy policy : remaining) {
        boolean referenced =
            Stream.of(
                    policy.getPolicyItems(),
                    policy.getDenyPolicyItems(),
                    policy.getAllowExceptions(),
                    policy.getDenyExceptions(),
                    policy.getDataMaskPolicyItems(),
                    policy.getRowFilterPolicyItems())
                .flatMap(List::stream)
                .anyMatch(item -> item.getRoles().contains(name));
        if (!referenced) {
          continue;
        }
        boolean unsupportedReference =
            Stream.of(
                    policy.getDenyPolicyItems(),
                    policy.getAllowExceptions(),
                    policy.getDenyExceptions(),
                    policy.getDataMaskPolicyItems(),
                    policy.getRowFilterPolicyItems())
                .flatMap(List::stream)
                .anyMatch(item -> item.getRoles().contains(name));
        if (unsupportedReference || !selector.isOtherCatalogGrant(policy, binding.catalog)) {
          throw new AuthorizationPluginException(
              "Role deletion blocked by foreign or unresolved policy references");
        }
        siblingReference = true;
      }
      // RoleManager dispatches once per catalog. Only another catalog's exact owned
      // table grant can defer shared-role deletion without hiding foreign divergence.
      if (siblingReference) {
        return true;
      }
      // Ranger 2.8 validates global policy/role/zone references; service lookup above
      // is not proof of global absence. Admin rejection must propagate to the caller.
      rangerClient.deleteRole(name, rangerAdminName, rangerServiceName);
      if (owners.roleExists(name)) {
        throw new AuthorizationPluginException("Deleted role remains on Ranger readback");
      }
      return true;
    } catch (RangerServiceException e) {
      throw new AuthorizationPluginException(e, "Delta role deletion failed");
    }
  }

  /** {@inheritDoc} */
  @Override
  public Boolean onRoleAcquired(Role role) {
    validateRole(role);
    validateService();
    owners.verifyOrdinaryRole(role.name());
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user) {
    roles.forEach(this::validateRole);
    validateService();
    roles.forEach(role -> owners.ensureOrdinaryRole(role.name()));
    return super.onGrantedRolesToUser(roles, user);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user) {
    roles.forEach(this::validateRole);
    validateService();
    roles.forEach(role -> owners.ensureOrdinaryRole(role.name()));
    return super.onRevokedRolesFromUser(roles, user);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group) {
    roles.forEach(this::validateRole);
    validateService();
    roles.forEach(role -> owners.ensureOrdinaryRole(role.name()));
    return super.onGrantedRolesToGroup(roles, group);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group) {
    roles.forEach(this::validateRole);
    validateService();
    roles.forEach(role -> owners.ensureOrdinaryRole(role.name()));
    return super.onRevokedRolesFromGroup(roles, group);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onUserAdded(User user) {
    validateService();
    owners.ensurePrincipal(user.name(), Owner.Type.USER);
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onGroupAdded(Group group) {
    validateService();
    owners.ensurePrincipal(group.name(), Owner.Type.GROUP);
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onUserRemoved(User user) {
    validateService();
    return super.onUserRemoved(user);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onGroupRemoved(Group group) {
    validateService();
    return super.onGroupRemoved(group);
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onOwnerSet(MetadataObject object, @Nullable Owner previous, Owner desired) {
    DeltaRangerAuthorizationConfig.require(desired != null, "Owner required");
    AuthorizationSecurableObject resource = translateOwner(object).get(0);
    validateService();
    String role =
        object.type() == MetadataObject.Type.METALAKE
            ? RangerHelper.generateMetalakeOwnerRoleName(metalakeId)
            : object.type() == MetadataObject.Type.CATALOG
                ? RangerHelper.generateCatalogOwnerRoleName(catalogId)
                : RangerHelper.GRAVITINO_OWNER_ROLE;
    owners.reconcile(resource, role, desired);
    return true;
  }
  /** {@inheritDoc} */
  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) {
    throw new UnsupportedOperationException("Delta Ranger rename/drop callbacks are not qualified");
  }
  /** {@inheritDoc} */
  @Override
  protected RangerPolicy createPolicyAddResources(AuthorizationMetadataObject object) {
    return selector.create(object);
  }
  /** {@inheritDoc} */
  @Override
  protected List<RangerPolicy> wildcardSearchPolicies(AuthorizationMetadataObject object) {
    RangerPolicy p = findManagedPolicy(object);
    return p == null ? List.of() : List.of(p);
  }
  /** {@inheritDoc} */
  @Override
  protected void renameMetadataObject(
      AuthorizationMetadataObject before, AuthorizationMetadataObject after) {
    throw new UnsupportedOperationException("Delta Ranger rename is not qualified");
  }
  /** {@inheritDoc} */
  @Override
  protected void removeMetadataObject(AuthorizationMetadataObject object) {
    throw new UnsupportedOperationException("Delta Ranger drop is not qualified");
  }
  /** {@inheritDoc} */
  @Override
  protected void updatePolicyByMetadataObject(
      MetadataObject.Type operation,
      AuthorizationMetadataObject before,
      AuthorizationMetadataObject after) {
    throw new UnsupportedOperationException(
        "Delta Ranger resource transformations are not qualified");
  }
  /** {@inheritDoc} */
  @Override
  protected void removePolicyByMetadataObject(AuthorizationMetadataObject object) {
    throw new UnsupportedOperationException("Delta Ranger resource removal is not qualified");
  }

  void validateService() {
    binding.validateService(rangerClient);
  }

  private void validateRole(Role role) {
    DeltaRangerAuthorizationConfig.require(
        !role.name().toUpperCase(Locale.ROOT).startsWith("GRAVITINO_"),
        "Raw reserved role prefix is not admitted");
    String name = rangerHelper.generateGravitinoRoleName(role.name());
    DeltaRangerAuthorizationConfig.require(
        !name.toUpperCase(Locale.ROOT).startsWith(RangerHelper.GRAVITINO_METALAKE_OWNER_ROLE)
            && !name.toUpperCase(Locale.ROOT).startsWith(RangerHelper.GRAVITINO_CATALOG_OWNER_ROLE)
            && !name.equalsIgnoreCase(RangerHelper.GRAVITINO_OWNER_ROLE),
        "Reserved owner role");
    validAuthorizationOperation(role.securableObjects());
  }

  private List<String> literalNames(MetadataObject object) {
    List<String> names =
        new ArrayList<>(SecurableObjects.DOT_SPLITTER.splitToList(object.fullName()));
    int expected =
        object.type() == MetadataObject.Type.CATALOG
            ? 1
            : object.type() == MetadataObject.Type.SCHEMA
                ? 2
                : object.type() == MetadataObject.Type.TABLE ? 3 : -1;
    DeltaRangerAuthorizationConfig.require(
        names.size() == expected && binding.catalog.equals(names.get(0)),
        "Canonical Delta catalog/resource mismatch");
    names.forEach(DeltaRangerAuthorizationConfig::literal);
    return names;
  }
}
