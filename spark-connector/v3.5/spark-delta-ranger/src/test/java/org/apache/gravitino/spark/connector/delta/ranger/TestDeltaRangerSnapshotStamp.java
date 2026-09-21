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

package org.apache.gravitino.spark.connector.delta.ranger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.gson.Gson;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerSnapshotStamp {
  @TempDir Path directory;

  @Test
  void sameVersionRoleMappingReplacementDuringEvaluationRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      lease.authorize(f.subject, f.key);
      f.plugin.afterEvaluation = () -> f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
    }
  }

  @Test
  void roleRefreshDuringReadinessCaptureRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.plugin.onDefinition = () -> f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, lease::versions);
    }
  }

  @Test
  void roleMappingAssignedWithoutCompletionNotificationRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.plugin.deliverNotifications = false;
      f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, lease::requireReady);
      f.plugin.deliverNotifications = true;
      f.plugin.getPluginContext().notifyAuthContextChanged();
      lease.requireReady();
    }
  }

  @Test
  void unchangedStampSurvivesOriginalAccessMaskAndFilterBatch() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      assertFalse(
          f.plugin.getConfig().getBoolean("ranger.plugin.spark.supports.policy.deltas", false));
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      lease.authorize(f.subject, f.key);
      assertEquals(
          DeltaRangerPolicyEngine.MaskDecision.Kind.NONE, lease.mask(f.subject, f.key).kind());
      assertFalse(
          lease
              .rowFilter(
                  f.subject,
                  new DeltaRangerResourceKey("delta_a", "sales", "orders", null, "select", null))
              .isPresent());
      lease.requireSnapshot(stamp);
      lease.requireSnapshot(lease.snapshotStamp());
    }
  }

  @Test
  void stampCannotBeUsedByAnotherCompatibleOrClosedLease() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine first = f.owner.acquire(f.config);
        DeltaRangerPolicyEngine second = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = first.snapshotStamp();
      assertThrows(SecurityException.class, () -> second.requireSnapshot(stamp));
      assertThrows(SecurityException.class, () -> second.requireSnapshot(null));
      first.close();
      assertThrows(IllegalStateException.class, () -> first.requireSnapshot(stamp));
      assertThrows(IllegalStateException.class, first::snapshotStamp);
      second.requireReady();
    }
  }

  @Test
  void completedSameVersionPolicyReplacementInvalidatesRetainedBatch() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      f.plugin.setPolicies(f.policies);
      assertEquals(7, lease.versions().policy());
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
      lease.requireSnapshot(lease.snapshotStamp());
    }
  }

  @Test
  void roleOnlyRefreshAndSameInputRepublishInvalidateRetainedBatch() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      RangerRoles roles = f.plugin.getRoles();
      Object originalMapping = f.plugin.getCurrentRangerAuthContext().getRangerRolesUtil();
      f.plugin.setRoles(roles);
      assertNotSame(originalMapping, f.plugin.getCurrentRangerAuthContext().getRangerRolesUtil());
      assertEquals(3, lease.versions().role());
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
      DeltaRangerPolicyEngine.SnapshotStamp second = lease.snapshotStamp();
      f.plugin.setRoles(f.roles(4, "replacement"));
      assertEquals(7, lease.versions().policy());
      assertEquals(4, lease.versions().role());
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(second));
      lease.requireSnapshot(lease.snapshotStamp());
    }
  }

  @Test
  void stampCaptureAndValidationBothRejectRoleRefreshAtEnd() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      f.plugin.onDefinition = () -> f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, lease::snapshotStamp);
      f.plugin.onDefinition = () -> f.plugin.setRoles(f.roles(3, "another"));
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
    }
  }

  @Test
  void observedSameContextNotificationInvalidatesStampWithoutRepairingPolicy() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      f.plugin.getPluginContext().notifyAuthContextChanged();
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
      f.policies.setServiceConfig(
          Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "wrong"));
      f.plugin.setPolicies(f.policies);
      f.plugin.getServiceConfigs().put("ranger.plugin.spark.gravitino.metalake", "lake");
      f.plugin.getPluginContext().notifyAuthContextChanged();
      assertThrows(SecurityException.class, lease::snapshotStamp);
    }
  }

  @Test
  void incompletePolicyAndWrongRoleServiceNeverMintReusableStamp() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.plugin.deliverNotifications = false;
      f.plugin.setPolicies(f.policies);
      assertThrows(SecurityException.class, lease::snapshotStamp);
      f.plugin.deliverNotifications = true;
      f.plugin.getPluginContext().notifyAuthContextChanged();
      lease.requireSnapshot(lease.snapshotStamp());
      RangerRoles wrong = f.roles(3, "wrong");
      wrong.setServiceName("other-service");
      f.plugin.setRoles(wrong);
      assertThrows(SecurityException.class, lease::snapshotStamp);
      wrong.setServiceName("service");
      f.plugin.getPluginContext().notifyAuthContextChanged();
      assertThrows(SecurityException.class, lease::snapshotStamp);
      f.plugin.setRoles(f.roles(3, "restored"));
      lease.requireSnapshot(lease.snapshotStamp());
    }
  }

  @Test
  void unrelatedNotificationCannotBlessPartiallyConstructedRoleMapping() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      RangerRoles pending =
          new RangerRoles() {
            boolean fired;

            @Override
            public Set<RangerRole> getRangerRoles() {
              if (!fired) {
                fired = true;
                f.plugin.getPluginContext().notifyAuthContextChanged();
                assertThrows(SecurityException.class, lease::snapshotStamp);
              }
              return super.getRangerRoles();
            }
          };
      pending.setServiceName("service");
      pending.setRoleVersion(3L);
      pending.setRangerRoles(f.roles(3, "replacement").getRangerRoles());
      f.plugin.setRoles(pending);
      lease.requireSnapshot(lease.snapshotStamp());
      assertEquals(
          Collections.singleton("replacement"),
          f.plugin.getRolesFromUserAndGroups("query-reader", Collections.singleton("finance")));
    }
  }

  @Test
  @SuppressWarnings("ObjectToString") // The regression intentionally detects identity rendering.
  void opaqueStampRenderingDoesNotRevealPublicationOrLeaseIdentity() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine first = f.owner.acquire(f.config);
        DeltaRangerPolicyEngine second = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp original = first.snapshotStamp();
      f.plugin.setRoles(f.roles(3, "replacement"));
      assertEquals(original.toString(), second.snapshotStamp().toString());
    }
  }

  @Test
  void completedRefreshDuringFinalBindingReadRejectsCaptureAndRetainedReuse() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      f.plugin.beforeFinalBindingReturn = () -> f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, lease::snapshotStamp);
      DeltaRangerPolicyEngine.SnapshotStamp next = lease.snapshotStamp();
      f.plugin.beforeFinalBindingReturn = () -> f.plugin.setPolicies(f.policies);
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(next));
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
    }
  }

  @Test
  void incompleteRolePublicationDuringFinalBindingReadRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.plugin.beforeFinalBindingReturn =
          () -> {
            f.plugin.deliverNotifications = false;
            f.plugin.setRoles(f.roles(3, "replacement"));
          };
      assertThrows(SecurityException.class, lease::snapshotStamp);
    }
  }

  @Test
  void incompletePolicyPublicationDuringFinalBindingReadRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.plugin.beforeFinalBindingReturn =
          () -> {
            f.plugin.deliverNotifications = false;
            f.plugin.setPolicies(f.policies);
          };
      assertThrows(SecurityException.class, lease::snapshotStamp);
    }
  }

  @Test
  void sameVersionRoleRevocationChangesActualAccessAndRejectsOldStamp() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.SnapshotStamp stamp = lease.snapshotStamp();
      lease.authorize(f.subject, f.key);
      f.plugin.setRoles(f.roles(3, "replacement"));
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
      assertThrows(SecurityException.class, () -> lease.requireSnapshot(stamp));
      f.plugin.setRoles(f.roles(3, "reader"));
      lease.authorize(f.subject, f.key);
    }
  }

  private static final class Fixture implements DeltaRangerPolicyEngine.Backend {
    final HookedPlugin plugin;
    final DeltaRangerConfig config;
    final DeltaRangerPolicyEngine.Owner owner = new DeltaRangerPolicyEngine.Owner(() -> this);
    final ServicePolicies policies = new ServicePolicies();
    final DeltaRangerQuerySubject subject;
    final DeltaRangerResourceKey key =
        new DeltaRangerResourceKey("delta_a", "sales", "orders", "secret", "select", null);

    Fixture(Path directory) throws Exception {
      Configuration conf = TestDeltaRangerConfig.configuration(directory);
      Properties properties = new Properties();
      conf.forEach(entry -> properties.setProperty(entry.getKey(), entry.getValue()));
      RangerPluginConfig rangerConfig =
          new RangerPluginConfig("spark", "service", "sparkSql", properties);
      config = new DeltaRangerConfig("service", "spark", "lake", rangerConfig);
      plugin = new HookedPlugin(rangerConfig);
      try (InputStreamReader reader =
          new InputStreamReader(
              getClass().getResourceAsStream("/ranger-delta-servicedef.json"),
              StandardCharsets.UTF_8)) {
        policies.setServiceDef(new Gson().fromJson(reader, RangerServiceDef.class));
      }
      policies.setServiceName("service");
      policies.setPolicyVersion(7L);
      policies.setServiceConfig(
          Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake"));
      RangerPolicy policy = new RangerPolicy();
      policy.setId(1L);
      policy.setName("allow-reader");
      policy.setService("service");
      policy.setPolicyType(0);
      policy.setIsEnabled(true);
      Map<String, RangerPolicy.RangerPolicyResource> resources = new LinkedHashMap<>();
      resources.put("catalog", new RangerPolicy.RangerPolicyResource("delta_a"));
      resources.put("database", new RangerPolicy.RangerPolicyResource("sales"));
      resources.put("table", new RangerPolicy.RangerPolicyResource("orders"));
      resources.put("column", new RangerPolicy.RangerPolicyResource("secret"));
      policy.setResources(resources);
      RangerPolicy.RangerPolicyItem item = new RangerPolicy.RangerPolicyItem();
      item.setRoles(Collections.singletonList("reader"));
      item.setAccesses(
          Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess("select", true)));
      policy.setPolicyItems(Collections.singletonList(item));
      policies.setPolicies(Collections.singletonList(policy));
      UserGroupInformation ugi =
          UserGroupInformation.createUserForTesting("query-reader", new String[] {"finance"});
      subject =
          ugi.doAs(
              (PrivilegedExceptionAction<DeltaRangerQuerySubject>)
                  () ->
                      DeltaRangerQuerySubject.current(
                          TestDeltaRangerQuerySubject.session(null, "simple")));
    }

    RangerRoles roles(long version, String name) {
      RangerRole role = new RangerRole();
      role.setName(name);
      role.setGroups(Collections.singletonList(new RangerRole.RoleMember("finance", false)));
      RangerRoles roles = new RangerRoles();
      roles.setServiceName("service");
      roles.setRoleVersion(version);
      roles.setRangerRoles(Collections.singleton(role));
      return roles;
    }

    @Override
    public RangerBasePlugin plugin() {
      return plugin;
    }

    @Override
    public void start() {
      plugin.setRoles(roles(3, "reader"));
      plugin.setPolicies(policies);
      assertEquals(
          Collections.singleton("reader"),
          plugin.getRolesFromUserAndGroups("query-reader", Collections.singleton("finance")));
    }

    @Override
    public void close() {
      plugin.cleanup();
    }
  }

  private static final class HookedPlugin extends RangerBasePlugin {
    Runnable onDefinition;
    Runnable afterEvaluation;
    Runnable beforeFinalBindingReturn;
    int bindingReads;
    boolean deliverNotifications = true;

    HookedPlugin(RangerPluginConfig config) {
      super(config);
    }

    @Override
    public RangerServiceDef getServiceDef() {
      Runnable hook = onDefinition;
      onDefinition = null;
      if (hook != null) {
        hook.run();
      }
      return super.getServiceDef();
    }

    @Override
    public Map<String, String> getServiceConfigs() {
      Map<String, String> result = super.getServiceConfigs();
      if (beforeFinalBindingReturn != null && ++bindingReads == 2) {
        Runnable hook = beforeFinalBindingReturn;
        beforeFinalBindingReturn = null;
        bindingReads = 0;
        hook.run();
      }
      return result;
    }

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
      RangerAccessResult result = super.isAccessAllowed(request);
      Runnable hook = afterEvaluation;
      afterEvaluation = null;
      if (hook != null) {
        hook.run();
      }
      return result;
    }

    @Override
    public void registerAuthContextEventListener(RangerAuthContextListener listener) {
      super.registerAuthContextEventListener(
          () -> {
            if (deliverNotifications) {
              listener.contextChanged();
            }
          });
    }
  }
}
