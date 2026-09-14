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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessRequest;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaRangerPolicyEngine {
  @Test
  void catalogsAndOwnersSurviveImmutableDeduplication() {
    DeltaRangerResourceKey a =
        new DeltaRangerResourceKey("a", "db", "table", "id", "select", "alice");
    DeltaRangerResourceKey same =
        new DeltaRangerResourceKey("a", "db", "table", "id", "select", "alice");
    DeltaRangerResourceKey b =
        new DeltaRangerResourceKey("b", "db", "table", "id", "select", "alice");
    DeltaRangerResourceKey owner =
        new DeltaRangerResourceKey("a", "db", "table", "id", "select", "bob");
    DeltaRangerResourceKey action =
        new DeltaRangerResourceKey("a", "db", "table", "id", "update", "alice");
    assertEquals(a, same);
    Set<DeltaRangerResourceKey> keys = new HashSet<>(Arrays.asList(a, same, b, owner, action));
    assertEquals(4, keys.size());
    AccessResource resource = a.resource();
    assertEquals("a", resource.getValue("catalog"));
    assertEquals("a", resource.catalog().get());
    assertEquals("alice", resource.getOwnerUser());
    resource.setValue("catalog", "changed");
    assertTrue(keys.contains(same));
    assertEquals("a", a.resource().getValue("catalog"));
  }

  @Test
  void preserveBothCatalogsAndOriginalColumnSetsIncludingNoColumnSelect() {
    Set<DeltaRangerResourceKey> keys = new HashSet<>();
    keys.addAll(
        DeltaRangerResourceKey.select(
            "a", "db", "table", Arrays.asList("id", "predicate", "id"), null));
    keys.addAll(
        DeltaRangerResourceKey.select("b", "db", "table", Arrays.asList("id", "join_key"), null));
    assertEquals(4, keys.size());
    Set<DeltaRangerResourceKey> count =
        DeltaRangerResourceKey.select("a", "db", "table", Collections.emptyList(), null);
    assertEquals(1, count.size());
    assertEquals("*", count.iterator().next().resource().getValue("column"));
  }

  @Test
  void rejectIncompleteAndWildcardLiteralIdentities() {
    for (String bad : Arrays.asList(null, "", "*", "a?")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new DeltaRangerResourceKey(bad, "db", "t", "c", "select", null));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              new DeltaRangerResourceKey("a", "db", "t", bad == null ? "*" : bad, "select", null));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeltaRangerResourceKey("a", "db", "t", "c", "use", null));
  }

  @TempDir Path directory;

  @Test
  void nullDenyAndEvaluationErrorsRejectWithoutEmptyEngineSuccess() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.allowed = null;
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
      f.allowed = false;
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
      f.failure = new IllegalStateException("boundary failure");
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
    }
  }

  @Test
  void missingDefinitionRolesOrMetalakeNeverBecomesReady() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      when(f.plugin.getServiceDef()).thenReturn(null);
      assertThrows(SecurityException.class, lease::requireReady);
      when(f.plugin.getServiceDef()).thenReturn(f.definition);
      when(f.plugin.getServiceConfigs()).thenReturn(Collections.emptyMap());
      assertThrows(SecurityException.class, lease::requireReady);
      f.bindMetalake();
      when(f.plugin.getRolesVersion()).thenReturn(-1L);
      assertThrows(SecurityException.class, lease::requireReady);
      when(f.plugin.getRolesVersion()).thenReturn(3L);
      when(f.plugin.getPoliciesVersion()).thenReturn(-1L);
      assertThrows(SecurityException.class, lease::requireReady);
    }
  }

  @Test
  void restoredConfigCannotAuthorizeOldWrongBoundEngineBeforeCompletedRefresh() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      when(f.plugin.getServiceConfigs())
          .thenReturn(Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "wrong"));
      when(f.plugin.getPoliciesVersion()).thenReturn(13L);
      f.completePolicy();
      assertThrows(SecurityException.class, lease::requireReady);
      // Ranger publishes the restoring config BEFORE replacing the old version-13 engine.
      f.bindMetalake();
      if (f.listener != null) {
        f.listener.contextChanged(); // A same-context notification cannot rebind that engine.
      }
      assertThrows(SecurityException.class, lease::requireReady);
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
      assertThrows(SecurityException.class, () -> lease.mask(f.subject, f.key));
      assertThrows(SecurityException.class, () -> lease.rowFilter(f.subject, f.table));
      when(f.plugin.getPoliciesVersion()).thenReturn(14L);
      assertThrows(SecurityException.class, lease::requireReady);
      f.completePolicy();
      lease.requireReady();
      lease.authorize(f.subject, f.key);
      lease.mask(f.subject, f.key);
      lease.rowFilter(f.subject, f.table);
    }
  }

  @Test
  void constructionAndSameVersionContextReplacementReject() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      when(f.pluginContext.getAuthContext()).thenReturn(mock(RangerAuthContext.class));
      assertThrows(SecurityException.class, lease::requireReady);
      f.completePolicy();
      lease.requireReady();
      when(f.plugin.isAccessAllowed(any(RangerAccessRequest.class)))
          .thenAnswer(
              call -> {
                f.completePolicy(); // Same version, different engine context during evaluation.
                RangerAccessResult result =
                    new RangerAccessResult(0, "service", f.definition, call.getArgument(0));
                result.setIsAllowed(true);
                return result;
              });
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
    }
  }

  @Test
  void pinnedListenerRunsSynchronouslyAfterEngineAndCurrentContextPublication() throws Exception {
    Fixture fixture = new Fixture(directory);
    RangerBasePlugin plugin = new RangerBasePlugin(fixture.plugin.getConfig());
    AtomicInteger callbacks = new AtomicInteger();
    List<RangerAuthContext> contexts = new ArrayList<>();
    Thread caller = Thread.currentThread();
    // Register before the startup cache can install policies. The production owner does likewise.
    plugin.registerAuthContextEventListener(
        () -> {
          assertSame(caller, Thread.currentThread());
          assertSame(
              plugin.getPluginContext().getAuthContext(), plugin.getCurrentRangerAuthContext());
          assertEquals(7, plugin.getPoliciesVersion());
          assertEquals(
              "lake", plugin.getServiceConfigs().get("ranger.plugin.spark.gravitino.metalake"));
          contexts.add(plugin.getCurrentRangerAuthContext());
          callbacks.incrementAndGet();
        });
    ServicePolicies policies = new ServicePolicies();
    policies.setServiceName("service");
    policies.setServiceDef(fixture.definition);
    policies.setPolicyVersion(7L);
    policies.setPolicies(Collections.emptyList());
    policies.setServiceConfig(
        Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake"));
    try {
      plugin.setPolicies(policies);
      assertEquals(1, callbacks.get());
      plugin.setPolicies(policies); // Even same downloaded version constructs a fresh context.
      assertEquals(2, callbacks.get());
      assertNotSame(contexts.get(0), contexts.get(1));
      plugin.setRoles(fixture.roles);
      assertEquals(3, callbacks.get());
      assertSame(contexts.get(1), contexts.get(2));
    } finally {
      plugin.cleanup();
    }
    // RangerAccessResult.policyVersion describes a matched individual policy revision; it is
    // deliberately not used as the downloaded engine version or as a completed-context identity.
  }

  @Test
  void sharedLeasesInitializeOnceAndCloseOnlyAfterLastOwner() throws Exception {
    Fixture f = new Fixture(directory);
    DeltaRangerPolicyEngine first = f.owner.acquire(f.config);
    DeltaRangerPolicyEngine second = f.owner.acquire(f.config);
    assertEquals(1, f.starts.get());
    first.close();
    assertEquals(0, f.closes.get());
    second.authorize(f.subject, f.key);
    assertEquals(1, f.requests.size());
    assertThrows(IllegalStateException.class, first::requireReady);
    second.close();
    second.close();
    assertEquals(1, f.closes.get());
  }

  @Test
  void incompatibleServiceMetalakeOrConfigurationLeaseRejects() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine ignored = f.owner.acquire(f.config)) {
      Configuration config = TestDeltaRangerConfig.configuration(directory);
      assertThrows(
          IllegalArgumentException.class,
          () -> f.owner.acquire(new DeltaRangerConfig("service", "spark", "other-lake", config)));
      config.set("ranger.plugin.spark.policy.rest.url", "http://localhost:5678");
      assertThrows(
          IllegalArgumentException.class,
          () -> f.owner.acquire(new DeltaRangerConfig("service", "spark", "lake", config)));
    }
  }

  @Test
  void requestContainsCanonicalCatalogAndActualGroupsAndDownloadedRoles() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      lease.authorize(f.subject, f.key);
      assertEquals(1, f.requests.size());
      RangerAccessRequest request = f.requests.get(0);
      assertEquals("delta_a", request.getResource().getValue("catalog"));
      assertEquals("query-reader", request.getUser());
      assertEquals(Collections.singleton("finance"), request.getUserGroups());
      assertEquals(Collections.singleton("downloaded-role"), request.getUserRoles());
      assertEquals("select", request.getAccessType());
    }
  }

  @Test
  void policyAndRoleVersionsRefreshIndependently() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      DeltaRangerPolicyEngine.Versions original = lease.versions();
      assertEquals(7, original.policy());
      assertEquals(3, original.role());
      when(f.plugin.getPoliciesVersion()).thenReturn(8L);
      f.completePolicy();
      assertEquals(8, lease.versions().policy());
      assertEquals(3, lease.versions().role());
      f.roles.setRoleVersion(4L);
      when(f.plugin.getRolesVersion()).thenReturn(4L);
      assertEquals(8, lease.versions().policy());
      assertEquals(4, lease.versions().role());
      assertEquals(3, original.role());
      assertEquals("service", original.service());
    }
  }

  @Test
  void customMasksQuoteCanonicalColumnsAndNullRemainsTyped() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.maskType = "CUSTOM";
      f.maskValue = "upper({col})";
      DeltaRangerResourceKey key =
          new DeltaRangerResourceKey("delta_a", "sales", "orders", "odd` column", "select", null);
      DeltaRangerPolicyEngine.MaskDecision mask = lease.mask(f.subject, key);
      assertEquals(DeltaRangerPolicyEngine.MaskDecision.Kind.EXPRESSION, mask.kind());
      assertEquals("upper(`odd`` column`)", mask.expression().get());
      f.maskType = "MASK_NULL";
      assertEquals(
          DeltaRangerPolicyEngine.MaskDecision.Kind.NULL, lease.mask(f.subject, key).kind());
      assertFalse(lease.mask(f.subject, key).expression().isPresent());
      f.maskType = "UNSUPPORTED";
      assertThrows(SecurityException.class, () -> lease.mask(f.subject, key));
    }
  }

  @Test
  void nullMaskFilterResultsAndMalformedExpressionsReject() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.nullMask = true;
      assertThrows(SecurityException.class, () -> lease.mask(f.subject, f.key));
      f.nullFilter = true;
      assertThrows(SecurityException.class, () -> lease.rowFilter(f.subject, f.table));
      f.nullFilter = false;
      f.filter = "region = 'allowed'";
      assertEquals("region = 'allowed'", lease.rowFilter(f.subject, f.table).get());
      f.filter = "region =";
      assertThrows(SecurityException.class, () -> lease.rowFilter(f.subject, f.table));
    }
  }

  @Test
  void refreshedDefinitionAndDownloadedRoleServiceCannotContradictBinding() throws Exception {
    Fixture f = new Fixture(directory);
    try (DeltaRangerPolicyEngine lease = f.owner.acquire(f.config)) {
      f.roles.setServiceName("other-service");
      assertThrows(SecurityException.class, lease::requireReady);
      f.roles.setServiceName("service");
      f.definition.getResources().get(0).getMatcherOptions().put("ignoreCase", "true");
      assertThrows(SecurityException.class, lease::requireReady);
      f.definition.getResources().get(0).getMatcherOptions().put("ignoreCase", "false");
      lease.requireReady();
      when(f.plugin.getRolesFromUserAndGroups(any(), any())).thenReturn(null);
      assertThrows(SecurityException.class, () -> lease.authorize(f.subject, f.key));
    }
  }

  @Test
  void canonicalDatabaseTableAndColumnRemainDistinct() {
    Set<DeltaRangerResourceKey> keys = new HashSet<>();
    keys.add(new DeltaRangerResourceKey("a", "db", "t", "c", "select", null));
    keys.add(new DeltaRangerResourceKey("a", "db2", "t", "c", "select", null));
    keys.add(new DeltaRangerResourceKey("a", "db", "t2", "c", "select", null));
    keys.add(new DeltaRangerResourceKey("a", "db", "t", "c2", "select", null));
    assertEquals(4, keys.size());
  }

  @Test
  void pinnedPublicMaskHelperEvaluatesCatalogsButDoesNotQuoteUnusualCanonicalColumns()
      throws Exception {
    Fixture fixture = new Fixture(directory);
    String full =
        "regexp_replace(regexp_replace(regexp_replace(regexp_replace(amount, '[A-Z]', 'X'), '[a-z]', 'x'), '[0-9]', 'n'), '[^A-Za-z0-9]', 'U')";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("MASK", full);
    expected.put(
        "MASK_SHOW_FIRST_4",
        "regexp_replace(regexp_replace(regexp_replace(regexp_replace(amount, '[A-Z]', 'X', 5), '[a-z]', 'x', 5), '[0-9]', 'n', 5), '[^A-Za-z0-9]', 'U', 5)");
    expected.put(
        "MASK_SHOW_LAST_4",
        "concat(regexp_replace(regexp_replace(regexp_replace(regexp_replace(left(amount, length(amount) - 4), '[A-Z]', 'X'), '[a-z]', 'x'), '[0-9]', 'n'), '[^A-Za-z0-9]', 'U'), right(amount, 4))");
    expected.put("MASK_HASH", "md5(cast(amount as string))");
    expected.put("MASK_DATE_SHOW_YEAR", "date_trunc('YEAR', amount)");
    expected.put("TRANSFORMER", "lower(amount)");
    expected.put("CUSTOM", "upper(amount)");
    expected.put("MASK_NULL", "NULL");
    long version = 1;
    try {
      for (Map.Entry<String, String> entry : expected.entrySet()) {
        RangerServiceDef.RangerDataMaskTypeDef type = new RangerServiceDef.RangerDataMaskTypeDef();
        type.setItemId(100L);
        type.setName(entry.getKey());
        type.setTransformer("lower({col})");
        List<RangerServiceDef.RangerDataMaskTypeDef> types = new ArrayList<>();
        types.add(type);
        if (!entry.getKey().equals("MASK_NULL")) {
          RangerServiceDef.RangerDataMaskTypeDef nullType =
              new RangerServiceDef.RangerDataMaskTypeDef();
          nullType.setItemId(101L);
          nullType.setName("MASK_NULL");
          nullType.setTransformer("NULL");
          types.add(nullType);
        }
        fixture.definition.getDataMaskDef().setMaskTypes(types);
        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("compatibility");
        policies.setServiceDef(fixture.definition);
        policies.setPolicyVersion(version++);
        policies.setPolicies(
            Arrays.asList(
                maskPolicy("delta_a", "amount", entry.getKey()),
                maskPolicy("delta_b", "amount", "MASK_NULL")));
        SparkRangerAdminPlugin.setPolicies(policies);
        assertEquals(
            entry.getValue(),
            SparkRangerAdminPlugin.getMaskingExpr(maskRequest("delta_a", "amount")).get());
        assertEquals(
            "NULL", SparkRangerAdminPlugin.getMaskingExpr(maskRequest("delta_b", "amount")).get());
      }
      RangerServiceDef.RangerDataMaskTypeDef custom = new RangerServiceDef.RangerDataMaskTypeDef();
      custom.setItemId(102L);
      custom.setName("CUSTOM");
      fixture.definition.getDataMaskDef().setMaskTypes(Collections.singletonList(custom));
      ServicePolicies unusual = new ServicePolicies();
      unusual.setServiceName("compatibility");
      unusual.setServiceDef(fixture.definition);
      unusual.setPolicyVersion(version);
      unusual.setPolicies(Collections.singletonList(maskPolicy("delta_a", "odd` name", "CUSTOM")));
      SparkRangerAdminPlugin.setPolicies(unusual);
      // The pinned API substitutes the raw resource string. Prequoting would change policy
      // identity.
      assertEquals(
          "upper(odd` name)",
          SparkRangerAdminPlugin.getMaskingExpr(maskRequest("delta_a", "odd` name")).get());
      assertFalse(
          SparkRangerAdminPlugin.getMaskingExpr(maskRequest("delta_a", "`odd`` name`"))
              .isDefined());
      RangerRole groupRole = new RangerRole();
      groupRole.setName("GRAVITINO_METALAKE_1_ROLE_reader");
      groupRole.setGroups(Collections.singletonList(new RangerRole.RoleMember("finance", false)));
      RangerRoles groups = new RangerRoles();
      groups.setServiceName("compatibility");
      groups.setRoleVersion(99L);
      groups.setRangerRoles(Collections.singleton(groupRole));
      long policyVersion = SparkRangerAdminPlugin.getPoliciesVersion();
      SparkRangerAdminPlugin.setRoles(groups);
      assertEquals(
          Collections.singleton("GRAVITINO_METALAKE_1_ROLE_reader"),
          SparkRangerAdminPlugin.getRolesFromUserAndGroups(
              fixture.subject.user(), fixture.subject.groups()));
      assertTrue(
          SparkRangerAdminPlugin.getRolesFromUserAndGroups(
                  "metadata-service", Collections.singleton("metadata-group"))
              .isEmpty());
      assertEquals(99, SparkRangerAdminPlugin.getRolesVersion());
      assertEquals(policyVersion, SparkRangerAdminPlugin.getPoliciesVersion());
      groups.setRoleVersion(100L);
      groups.setRangerRoles(Collections.emptySet());
      SparkRangerAdminPlugin.setRoles(groups);
      assertTrue(
          SparkRangerAdminPlugin.getRolesFromUserAndGroups(
                  fixture.subject.user(), fixture.subject.groups())
              .isEmpty());
      assertEquals(100, SparkRangerAdminPlugin.getRolesVersion());
      assertEquals(policyVersion, SparkRangerAdminPlugin.getPoliciesVersion());
      assertEquals(6, DeltaRangerPolicyEngine.runtimeOrigins().size());
    } finally {
      SparkRangerAdminPlugin.cleanup();
    }
  }

  private static AccessRequest maskRequest(String catalog, String column) {
    AccessRequest request = new AccessRequest(AccessType.SELECT());
    AccessResource resource =
        new DeltaRangerResourceKey(catalog, "sales", "orders", column, "select", null).resource();
    resource.setServiceDef(SparkRangerAdminPlugin.getServiceDef());
    request.setResource(resource);
    request.setUser("query-reader");
    request.setUserGroups(Collections.emptySet());
    request.setUserRoles(Collections.emptySet());
    request.setAccessType("select");
    return request;
  }

  private static RangerPolicy maskPolicy(String catalog, String column, String type) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(catalog.equals("delta_a") ? 1L : 2L);
    policy.setName(catalog + "-mask");
    policy.setService("compatibility");
    policy.setPolicyType(1);
    policy.setIsEnabled(true);
    Map<String, RangerPolicy.RangerPolicyResource> resources = new LinkedHashMap<>();
    resources.put("catalog", new RangerPolicy.RangerPolicyResource(catalog));
    resources.put("database", new RangerPolicy.RangerPolicyResource("sales"));
    resources.put("table", new RangerPolicy.RangerPolicyResource("orders"));
    resources.put("column", new RangerPolicy.RangerPolicyResource(column));
    policy.setResources(resources);
    RangerPolicy.RangerDataMaskPolicyItem item = new RangerPolicy.RangerDataMaskPolicyItem();
    item.setUsers(Collections.singletonList("query-reader"));
    item.setAccesses(
        Collections.singletonList(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    item.setDataMaskInfo(new RangerPolicy.RangerPolicyItemDataMaskInfo(type, null, "upper({col})"));
    policy.setDataMaskPolicyItems(Collections.singletonList(item));
    return policy;
  }

  private static final class Fixture implements DeltaRangerPolicyEngine.Backend {
    final RangerBasePlugin plugin = mock(RangerBasePlugin.class);
    final RangerServiceDef definition;
    final RangerRoles roles = new RangerRoles();
    final DeltaRangerConfig config;
    final DeltaRangerPolicyEngine.Owner owner = new DeltaRangerPolicyEngine.Owner(() -> this);
    final AtomicInteger starts = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();
    final List<RangerAccessRequest> requests = new ArrayList<>();
    final DeltaRangerQuerySubject subject;
    final DeltaRangerResourceKey key =
        new DeltaRangerResourceKey("delta_a", "sales", "orders", "secret", "select", null);
    final DeltaRangerResourceKey table =
        new DeltaRangerResourceKey("delta_a", "sales", "orders", null, "select", null);
    RangerAuthContextListener listener;
    final RangerPluginContext pluginContext = mock(RangerPluginContext.class);
    Boolean allowed = true;
    RuntimeException failure;
    String maskType;
    String maskValue;
    String filter;
    boolean nullMask;
    boolean nullFilter;

    Fixture(Path directory) throws Exception {
      try (InputStreamReader reader =
          new InputStreamReader(
              getClass().getResourceAsStream("/ranger-delta-servicedef.json"),
              StandardCharsets.UTF_8)) {
        definition = new Gson().fromJson(reader, RangerServiceDef.class);
      }
      Configuration conf = TestDeltaRangerConfig.configuration(directory);
      Properties properties = new Properties();
      conf.forEach(entry -> properties.setProperty(entry.getKey(), entry.getValue()));
      RangerPluginConfig pluginConf =
          new RangerPluginConfig("spark", "service", "sparkSql", properties);
      config = new DeltaRangerConfig("service", "spark", "lake", pluginConf);
      when(plugin.getConfig()).thenReturn(pluginConf);
      when(plugin.getPluginContext()).thenReturn(pluginContext);
      doAnswer(
              call -> {
                listener = call.getArgument(0);
                return null;
              })
          .when(plugin)
          .registerAuthContextEventListener(any());
      when(plugin.getServiceName()).thenReturn("service");
      when(plugin.getServiceDef()).thenReturn(definition);
      when(plugin.getPoliciesVersion()).thenReturn(7L);
      when(plugin.getRolesVersion()).thenReturn(3L);
      roles.setServiceName("service");
      roles.setRoleVersion(3L);
      when(plugin.getRangerRoles()).thenReturn(roles);
      bindMetalake();
      when(plugin.getRolesFromUserAndGroups(any(), any()))
          .thenReturn(Collections.singleton("downloaded-role"));
      when(plugin.isAccessAllowed(any(RangerAccessRequest.class)))
          .thenAnswer(
              call -> {
                if (failure != null) {
                  throw failure;
                }
                RangerAccessRequest request = call.getArgument(0);
                requests.add(request);
                if (allowed == null) {
                  return null;
                }
                RangerAccessResult result =
                    new RangerAccessResult(0, "service", definition, request);
                result.setIsAllowed(allowed);
                result.setPolicyVersion(plugin.getPoliciesVersion());
                return result;
              });
      when(plugin.evalDataMaskPolicies(any(), any()))
          .thenAnswer(
              call -> {
                if (nullMask) {
                  return null;
                }
                RangerAccessResult result =
                    new RangerAccessResult(1, "service", definition, call.getArgument(0));
                result.setPolicyVersion(plugin.getPoliciesVersion());
                result.setMaskType(maskType);
                result.setMaskedValue(maskValue);
                return result;
              });
      when(plugin.evalRowFilterPolicies(any(), any()))
          .thenAnswer(
              call -> {
                if (nullFilter) {
                  return null;
                }
                RangerAccessResult result =
                    new RangerAccessResult(2, "service", definition, call.getArgument(0));
                result.setPolicyVersion(plugin.getPoliciesVersion());
                result.setFilterExpr(filter);
                return result;
              });
      UserGroupInformation ugi =
          UserGroupInformation.createUserForTesting("query-reader", new String[] {"finance"});
      subject =
          ugi.doAs(
              (PrivilegedExceptionAction<DeltaRangerQuerySubject>)
                  () ->
                      DeltaRangerQuerySubject.current(
                          TestDeltaRangerQuerySubject.session(null, "simple")));
    }

    void completePolicy() {
      RangerAuthContext context = mock(RangerAuthContext.class);
      when(plugin.getCurrentRangerAuthContext()).thenReturn(context);
      when(pluginContext.getAuthContext()).thenReturn(context);
      if (listener != null) {
        listener.contextChanged();
      }
    }

    void bindMetalake() {
      when(plugin.getServiceConfigs())
          .thenReturn(Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "lake"));
    }

    @Override
    public RangerBasePlugin plugin() {
      return plugin;
    }

    @Override
    public void start() {
      starts.incrementAndGet();
      completePolicy();
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }
}
