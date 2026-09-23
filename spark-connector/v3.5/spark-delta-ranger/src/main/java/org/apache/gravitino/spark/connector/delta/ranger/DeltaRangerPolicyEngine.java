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

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessRequest;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin$;
import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAuditHandler;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerRolesUtil;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;

/**
 * An owned lease on the actual Kyuubi 1.10.2 singleton and Ranger 2.8 downloader.
 *
 * <p>No ready policy/role/service snapshot means rejection. Ranger may retain a last-known-good
 * snapshot during an Admin outage; versions remain observable and revocations take effect after
 * refresh. This adapter makes no instantaneous revocation or reused-plan guarantee.
 */
public final class DeltaRangerPolicyEngine implements AutoCloseable {
  private static final Owner OWNER = new Owner(KyuubiBackend::new);
  private static final String METALAKE = "ranger.plugin.spark.gravitino.metalake";
  private static final List<String> RESOURCES =
      Arrays.asList("catalog", "database", "table", "column");
  private static final Set<String> ACTIONS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList("select", "read", "write", "create", "drop", "alter", "update")));
  private final Owner owner;
  private boolean closed;

  private DeltaRangerPolicyEngine(Owner owner) {
    this.owner = owner;
  }

  /**
   * Acquires a shared engine lease with an immutable trusted deployment binding. The first
   * acquisition initializes the real downloader; readiness is required separately. Incompatible
   * bindings always reject. Once the final lease closes, this JVM's engine is terminal and a new
   * driver process is required, avoiding unsafe singleton/audit restarts.
   *
   * @param config trusted driver configuration, never query-controlled options
   * @return owned lease
   */
  public static DeltaRangerPolicyEngine acquire(DeltaRangerConfig config) {
    return OWNER.acquire(config);
  }

  /**
   * Rejects absent/default snapshots, mismatched service/metalake, and incompatible definitions.
   * Call before query execution; initialization alone does not mean readiness.
   *
   * @throws SecurityException if no valid downloaded or cached snapshot is available
   */
  public void requireReady() {
    versions();
  }

  /**
   * Evaluates one original canonical privilege; null, denial and engine errors reject.
   *
   * @param subject authenticated query subject
   * @param key original canonical resource, including explicit table-wide reads
   */
  public void authorize(DeltaRangerQuerySubject subject, DeltaRangerResourceKey key) {
    authorize(subject, key, Instant.now());
  }

  void authorize(DeltaRangerQuerySubject subject, DeltaRangerResourceKey key, Instant accessTime) {
    synchronized (owner) {
      RangerAccessResult result = evaluate(subject, key, 0, accessTime);
      if (!result.getIsAllowed()) {
        throw new SecurityException("Delta Ranger access denied");
      }
    }
  }

  /**
   * Returns a typed decision for the initially qualified CUSTOM and MASK_NULL mask kinds. The
   * caller must authorize the original columns first and bind expressions to fresh attributes.
   *
   * @param subject authenticated query subject
   * @param key canonical top-level column
   * @return NONE, a typed NULL instruction, or a syntactically validated SQL expression
   */
  public MaskDecision mask(DeltaRangerQuerySubject subject, DeltaRangerResourceKey key) {
    return mask(subject, key, Instant.now());
  }

  MaskDecision mask(
      DeltaRangerQuerySubject subject, DeltaRangerResourceKey key, Instant accessTime) {
    synchronized (owner) {
      if (key.column() == null || !"select".equals(key.accessType())) {
        throw new SecurityException("Mask requires a canonical SELECT column");
      }
      RangerAccessResult result = evaluate(subject, key, 1, accessTime);
      if (!result.isMaskEnabled()) {
        return new MaskDecision(MaskDecision.Kind.NONE, null);
      }
      if ("MASK_NULL".equals(result.getMaskType())) {
        return new MaskDecision(MaskDecision.Kind.NULL, null);
      }
      if (!"CUSTOM".equals(result.getMaskType())) {
        throw new SecurityException("Unsupported enabled Delta Ranger mask kind");
      }
      String value = result.getMaskedValue();
      if (value == null) {
        return new MaskDecision(MaskDecision.Kind.NULL, null);
      }
      String quoted = "`" + key.column().replace("`", "``") + "`";
      String expression = value.replace("{col}", quoted);
      validateExpression(expression);
      return new MaskDecision(MaskDecision.Kind.EXPRESSION, expression);
    }
  }

  /**
   * Evaluates a table filter; the caller must resolve scope and validate the admitted plan shape.
   *
   * @param subject authenticated query subject
   * @param key canonical table-wide SELECT resource
   * @return optional syntactically validated expression
   */
  public Optional<String> rowFilter(DeltaRangerQuerySubject subject, DeltaRangerResourceKey key) {
    return rowFilter(subject, key, Instant.now());
  }

  Optional<String> rowFilter(
      DeltaRangerQuerySubject subject, DeltaRangerResourceKey key, Instant accessTime) {
    synchronized (owner) {
      if (key.column() != null || !"select".equals(key.accessType())) {
        throw new SecurityException("Row filter requires a canonical SELECT table");
      }
      RangerAccessResult result = evaluate(subject, key, 2, accessTime);
      if (!result.isRowFilterEnabled()) {
        return Optional.empty();
      }
      validateExpression(result.getFilterExpr());
      return Optional.of(result.getFilterExpr());
    }
  }

  /**
   * @return immutable active service, definition, policy and role versions after readiness checks
   */
  public Versions versions() {
    synchronized (owner) {
      requireOpen();
      try {
        return snapshot().versions;
      } catch (RuntimeException e) {
        throw new SecurityException("Delta Ranger policy/role/service snapshot is not ready", e);
      }
    }
  }

  SnapshotStamp snapshotStamp() {
    synchronized (owner) {
      requireOpen();
      try {
        return snapshot();
      } catch (RuntimeException | LinkageError e) {
        throw new SecurityException("Delta Ranger publication snapshot is not ready", e);
      }
    }
  }

  void requireSnapshot(@Nullable SnapshotStamp expected) {
    synchronized (owner) {
      requireOpen();
      SnapshotStamp current = snapshotStamp();
      if (expected == null
          || expected.lease != this
          || !expected.binding.equals(owner.config)
          || expected.publication != current.publication) {
        throw new SecurityException("Delta Ranger publication changed; reanalyze the query");
      }
    }
  }

  // Driver-local proof only: no getters, serialization, value equality, or diagnostic rendering.
  static final class SnapshotStamp {
    private final DeltaRangerPolicyEngine lease;
    private final DeltaRangerConfig binding;
    private final Publication publication;
    private final Versions versions;

    private SnapshotStamp(
        DeltaRangerPolicyEngine lease,
        DeltaRangerConfig binding,
        Publication publication,
        Versions versions) {
      this.lease = lease;
      this.binding = binding;
      this.publication = publication;
      this.versions = versions;
    }

    /** Returns a constant description without lease, binding, or publication identities. */
    @Override
    public String toString() {
      return "Delta Ranger driver snapshot";
    }
  }

  /** Drains this lease; only the last owned user closes the downloader and audit resources. */
  @Override
  public void close() {
    synchronized (owner) {
      if (!closed) {
        closed = true;
        owner.release();
      }
    }
  }

  /** Typed rewrite instruction; a NULL decision must use the original Spark data type. */
  public static final class MaskDecision {
    /** Admitted mask outcomes. */
    public enum Kind {
      /** No enabled mask. */
      NONE,
      /** Replace with a null literal of the original attribute's type. */
      NULL,
      /** Resolve the validated expression against the original attribute. */
      EXPRESSION
    }

    private final Kind kind;
    @Nullable private final String expression;

    private MaskDecision(Kind kind, @Nullable String expression) {
      this.kind = kind;
      this.expression = expression;
    }
    /**
     * @return typed mask outcome
     */
    public Kind kind() {
      return kind;
    }
    /**
     * @return expression only for EXPRESSION; NULL never carries an untyped SQL string
     */
    public Optional<String> expression() {
      return Optional.ofNullable(expression);
    }
    /** Compares the typed outcome and optional SQL expression by value. */
    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof MaskDecision)) {
        return false;
      }
      MaskDecision that = (MaskDecision) other;
      return kind == that.kind && Objects.equals(expression, that.expression);
    }

    /** Returns a hash of the typed outcome and optional SQL expression. */
    @Override
    public int hashCode() {
      return Objects.hash(kind, expression);
    }
  }

  /** Immutable observed versions; policy and role refreshes are independent. */
  public static final class Versions {
    private final String service;
    private final String definition;
    private final long policy;
    private final long role;

    private Versions(String service, String definition, long policy, long role) {
      this.service = service;
      this.definition = definition;
      this.policy = policy;
      this.role = role;
    }
    /**
     * @return active policy version
     */
    public long policy() {
      return policy;
    }
    /**
     * @return active role version
     */
    public long role() {
      return role;
    }
    /**
     * @return bound service name
     */
    public String service() {
      return service;
    }
    /**
     * @return validated definition name
     */
    public String definition() {
      return definition;
    }
  }

  interface Backend extends AutoCloseable {
    RangerBasePlugin plugin();

    void start();

    @Override
    void close();
  }

  static final class Owner {
    private final Supplier<Backend> factory;
    @Nullable private Backend backend;
    @Nullable private DeltaRangerConfig config;
    private int users;
    private boolean terminal;
    private final Object refreshLock = new Object();
    @Nullable private volatile Publication publication;

    Owner(Supplier<Backend> factory) {
      this.factory = factory;
    }

    synchronized DeltaRangerPolicyEngine acquire(DeltaRangerConfig requested) {
      Objects.requireNonNull(requested, "Trusted Ranger configuration required");
      if (config != null && !config.equals(requested)) {
        throw new IllegalArgumentException(
            "Incompatible service, metalake, or configuration lease in one JVM");
      }
      if (terminal) {
        throw new IllegalStateException(
            "Delta Ranger singleton was closed; start a new driver process");
      }
      if (backend == null) {
        config = requested;
        Backend created = factory.get();
        try {
          requested.requireCompatible(created.plugin().getConfig());
          created
              .plugin()
              .registerAuthContextEventListener(() -> completedRefresh(created.plugin()));
          created.start();
          backend = created;
        } catch (RuntimeException | LinkageError failure) {
          terminal = true;
          try {
            created.close();
          } catch (RuntimeException cleanup) {
            failure.addSuppressed(cleanup);
          }
          throw failure;
        }
      }
      users++;
      return new DeltaRangerPolicyEngine(this);
    }

    private void completedRefresh(RangerBasePlugin plugin) {
      synchronized (refreshLock) {
        Publication previous = publication;
        CompletedPolicy policy = previous == null ? null : previous.policy;
        CompletedRoles roles = previous == null ? null : previous.roles;
        RangerAuthContext context = plugin.getCurrentRangerAuthContext();
        // Policy construction exposes plugin-context before engine/current-context assignment.
        // The untyped callback must never rebind the same rejected policy or role mapping.
        if (context != null && context == plugin.getPluginContext().getAuthContext()) {
          if (policy == null || context != policy.context) {
            policy =
                new CompletedPolicy(
                    context, plugin.getPoliciesVersion(), plugin.getServiceConfigs().get(METALAKE));
          }
          RangerRoles input = plugin.getRoles();
          RangerRolesUtil mapping = context.getRangerRolesUtil();
          // Ranger 2.8 builds all five final maps before assigning a NEW util. Final-field
          // initialization safety covers those maps even when Ranger's optional locks are off.
          // This callback observes publication, not a role-event kind or setRoles return.
          if (mapping != null
              && input != null
              && input == mapping.getRoles()
              && (roles == null || roles.context != context || roles.mapping != mapping)) {
            roles = new CompletedRoles(context, mapping, input);
          }
        }
        // Every observed notification invalidates retained stamps, including enricher events.
        publication = new Publication(policy, roles);
      }
    }

    private void release() {
      if (--users == 0) {
        terminal = true;
        backend.close();
        backend = null;
      }
    }
  }

  static Map<String, String> runtimeOrigins() {
    Map<String, String> origins = new LinkedHashMap<>();
    origin(
        origins,
        SparkRangerAdminPlugin$.class,
        "org.apache.kyuubi",
        "kyuubi-spark-authz_2.12",
        "1.10.2");
    origin(origins, RangerBasePlugin.class, "org.apache.ranger", "ranger-plugins-common", "2.8.0");
    origin(
        origins,
        RangerPolicyEngineImpl.class,
        "org.apache.ranger",
        "ranger-plugins-common",
        "2.8.0");
    origin(origins, RangerRoles.class, "org.apache.ranger", "ranger-plugins-common", "2.8.0");
    origin(
        origins,
        RangerPluginClassLoader.class,
        "org.apache.ranger",
        "ranger-plugin-classloader",
        "2.8.0");
    origin(origins, AuditProviderFactory.class, "org.apache.ranger", "ranger-audit-core", "2.8.0");
    return Collections.unmodifiableMap(origins);
  }

  private static final class CompletedPolicy {
    private final RangerAuthContext context;
    private final long version;
    @Nullable private final String metalake;

    private CompletedPolicy(RangerAuthContext context, long version, @Nullable String metalake) {
      this.context = context;
      this.version = version;
      this.metalake = metalake;
    }
  }

  private static final class CompletedRoles {
    private final RangerAuthContext context;
    private final RangerRolesUtil mapping;
    private final RangerRoles input;
    private final long version;
    @Nullable private final String service;

    private CompletedRoles(RangerAuthContext context, RangerRolesUtil mapping, RangerRoles input) {
      this.context = context;
      this.mapping = mapping;
      this.input = input;
      this.version = mapping.getRoleVersion();
      this.service = input.getServiceName();
    }
  }

  private static final class Publication {
    @Nullable private final CompletedPolicy policy;
    @Nullable private final CompletedRoles roles;

    private Publication(@Nullable CompletedPolicy policy, @Nullable CompletedRoles roles) {
      this.policy = policy;
      this.roles = roles;
    }
  }

  private static final class KyuubiBackend implements Backend {
    private final SparkRangerAdminPlugin$ plugin;

    private KyuubiBackend() {
      DeltaRangerConfig.requireSafeLogging();
      runtimeOrigins();
      plugin = SparkRangerAdminPlugin$.MODULE$;
      if (plugin.getPoliciesVersion() >= 0 || plugin.getResultProcessor() != null) {
        throw new IllegalStateException(
            "Kyuubi Ranger singleton already used outside the Delta lease owner");
      }
    }

    @Override
    public RangerBasePlugin plugin() {
      return plugin;
    }

    @Override
    public void start() {
      plugin.initialize();
      plugin.setResultProcessor(new SparkRangerAuditHandler());
    }

    @Override
    public void close() {
      plugin.cleanup();
      plugin.getAuditProviderFactory().shutdown();
    }
  }

  private void requireOpen() {
    if (closed || owner.backend == null) {
      throw new IllegalStateException("Delta Ranger lease is closed");
    }
  }

  private SnapshotStamp snapshot() {
    RangerBasePlugin plugin = owner.backend.plugin();
    DeltaRangerConfig config = owner.config;
    Publication publication = owner.publication;
    CompletedPolicy completed = publication == null ? null : publication.policy;
    CompletedRoles completedRoles = publication == null ? null : publication.roles;
    DeltaRangerConfig.require(
        completed != null
            && completed.context == plugin.getCurrentRangerAuthContext()
            && completed.context == plugin.getPluginContext().getAuthContext(),
        "Policy refresh has not completed publication");
    long policy = plugin.getPoliciesVersion();
    DeltaRangerConfig.require(
        policy == completed.version && config.metalake.equals(completed.metalake),
        "Completed policy binding/version mismatch");
    long role = plugin.getRolesVersion();
    DeltaRangerConfig.require(
        policy >= 0 && role >= 0, "Downloaded policy and role versions required");
    DeltaRangerConfig.require(
        config.service.equals(plugin.getServiceName()), "Downloaded service mismatch");
    DeltaRangerConfig.require(
        config.metalake.equals(plugin.getServiceConfigs().get(METALAKE)),
        "Downloaded metalake binding mismatch");
    validateDefinition(plugin.getServiceDef(), config.definition);
    DeltaRangerConfig.require(
        completedRoles != null
            && completedRoles.context == completed.context
            && completedRoles.mapping == completed.context.getRangerRolesUtil()
            && completedRoles.version == role
            && config.service.equals(completedRoles.service),
        "Role mapping refresh has not completed publication");
    RangerRoles roles = plugin.getRangerRoles();
    DeltaRangerConfig.require(
        roles != null
            && roles == completedRoles.input
            && roles == plugin.getRoles()
            && roles == completedRoles.mapping.getRoles()
            && config.service.equals(roles.getServiceName())
            && Objects.equals(roles.getRoleVersion(), role),
        "Downloaded roles missing or bound to another service/version");
    DeltaRangerConfig.require(
        roles == plugin.getRangerRoles()
            && roles == plugin.getRoles()
            && Objects.equals(roles.getRoleVersion(), role)
            && config.service.equals(roles.getServiceName())
            && config.service.equals(plugin.getServiceName())
            && config.metalake.equals(plugin.getServiceConfigs().get(METALAKE))
            && completed.context == plugin.getCurrentRangerAuthContext()
            && completed.context == plugin.getPluginContext().getAuthContext()
            && completedRoles.mapping == completed.context.getRangerRolesUtil()
            && policy == plugin.getPoliciesVersion()
            && role == plugin.getRolesVersion()
            && publication == owner.publication,
        "Policy or role publication changed during readiness validation");
    return new SnapshotStamp(
        this, config, publication, new Versions(config.service, config.definition, policy, role));
  }

  private RangerAccessResult evaluate(
      DeltaRangerQuerySubject subject,
      DeltaRangerResourceKey key,
      int policyType,
      Instant accessTime) {
    requireOpen();
    try {
      Objects.requireNonNull(subject, "Authenticated query subject required");
      Objects.requireNonNull(key, "Canonical resource required");
      Date requestTime = Date.from(Objects.requireNonNull(accessTime, "Access time required"));
      SnapshotStamp before = snapshot();
      RangerBasePlugin plugin = owner.backend.plugin();
      AccessResource resource = key.resource();
      resource.setServiceDef(plugin.getServiceDef());
      if (policyType == 2) {
        resource.setValue("column", null);
      }
      AccessRequest request =
          new AccessRequest(AccessType.withName(key.accessType().toUpperCase(Locale.ROOT)));
      request.setResource(resource);
      request.setUser(subject.user());
      request.setUserGroups(subject.groups());
      request.setUserRoles(
          Objects.requireNonNull(
              plugin.getRolesFromUserAndGroups(subject.user(), subject.groups()),
              "Downloaded role resolution returned null"));
      request.setAccessType(key.accessType());
      request.setAction(key.accessType());
      request.setAccessTime(requestTime);
      request.setClusterName(plugin.getClusterName());
      RangerAccessResult result;
      if (policyType == 0) {
        result = plugin.isAccessAllowed(request);
      } else if (policyType == 1) {
        result = plugin.evalDataMaskPolicies(request, plugin.getResultProcessor());
      } else {
        result = plugin.evalRowFilterPolicies(request, plugin.getResultProcessor());
      }
      DeltaRangerConfig.require(result != null, "Null Ranger evaluation result");
      DeltaRangerConfig.require(
          before.versions.service.equals(result.getServiceName()), "Evaluation service mismatch");
      validateDefinition(result.getServiceDef(), before.versions.definition);
      requireSnapshot(before);
      return result;
    } catch (RuntimeException | LinkageError e) {
      throw new SecurityException("Delta Ranger evaluation rejected", e);
    }
  }

  private static void validateExpression(String expression) {
    try {
      if (expression == null || expression.trim().isEmpty()) {
        throw new IllegalArgumentException("Empty policy expression");
      }
      CatalystSqlParser$.MODULE$.parseExpression(expression);
    } catch (Exception e) {
      throw new SecurityException("Invalid Delta Ranger policy expression", e);
    }
  }

  private static void validateDefinition(RangerServiceDef definition, String name) {
    DeltaRangerConfig.require(
        definition != null && name.equals(definition.getName()), "Provisioned definition required");
    List<RangerServiceDef.RangerServiceConfigDef> binding =
        definition.getConfigs().stream()
            .filter(config -> METALAKE.equals(config.getName()))
            .collect(Collectors.toList());
    DeltaRangerConfig.require(
        binding.size() == 1
            && "string".equals(binding.get(0).getType())
            && Boolean.TRUE.equals(binding.get(0).getMandatory()),
        "Required metalake configuration definition missing");
    resources(definition.getResources(), 4);
    resources(definition.getDataMaskDef().getResources(), 4);
    resources(definition.getRowFilterDef().getResources(), 3);
    Set<String> expected = new HashSet<>(ACTIONS);
    expected.add("all");
    DeltaRangerConfig.require(
        definition.getAccessTypes().size() == expected.size()
            && definition.getAccessTypes().stream()
                .map(RangerServiceDef.RangerAccessTypeDef::getName)
                .collect(Collectors.toSet())
                .equals(expected),
        "Incompatible access vocabulary");
    definition
        .getAccessTypes()
        .forEach(
            access ->
                DeltaRangerConfig.require(
                    new HashSet<>(access.getImpliedGrants())
                        .equals("all".equals(access.getName()) ? ACTIONS : Collections.emptySet()),
                    "Incompatible implied grants"));
    DeltaRangerConfig.require(
        definition.getDataMaskDef().getAccessTypes().size() == 1
            && "select".equals(definition.getDataMaskDef().getAccessTypes().get(0).getName()),
        "Unsupported mask access");
    DeltaRangerConfig.require(
        definition.getRowFilterDef().getAccessTypes().size() == 1
            && "select".equals(definition.getRowFilterDef().getAccessTypes().get(0).getName()),
        "Unsupported filter access");
  }

  private static void resources(List<RangerServiceDef.RangerResourceDef> resources, int size) {
    DeltaRangerConfig.require(
        resources != null && resources.size() == size, "Incompatible resource hierarchy");
    Map<String, String> options = new LinkedHashMap<>();
    options.put("wildCard", "true");
    options.put("ignoreCase", "false");
    options.put("replaceTokens", "false");
    options.put("replaceReqExpressions", "false");
    for (int i = 0; i < size; i++) {
      RangerServiceDef.RangerResourceDef resource = resources.get(i);
      DeltaRangerConfig.require(
          RESOURCES.get(i).equals(resource.getName())
              && Objects.equals(resource.getParent(), i == 0 ? null : RESOURCES.get(i - 1))
              && Objects.equals(resource.getLevel(), (i + 1) * 10),
          "Incompatible resource identity");
      DeltaRangerConfig.require(
          Boolean.TRUE.equals(resource.getMandatory())
              && Boolean.valueOf(i == size - 1).equals(resource.getIsValidLeaf())
              && Boolean.FALSE.equals(resource.getRecursiveSupported())
              && Boolean.FALSE.equals(resource.getExcludesSupported()),
          "Incompatible resource leaf semantics");
      DeltaRangerConfig.require(
          "org.apache.ranger.plugin.resourcematcher.RangerDefaultResourceMatcher"
                  .equals(resource.getMatcher())
              && options.equals(resource.getMatcherOptions()),
          "Unqualified resource matcher semantics");
    }
  }

  private static void origin(
      Map<String, String> origins, Class<?> type, String group, String artifact, String version) {
    try {
      URL location = type.getProtectionDomain().getCodeSource().getLocation();
      Enumeration<URL> definitions =
          type.getClassLoader().getResources(type.getName().replace('.', '/') + ".class");
      DeltaRangerConfig.require(
          Collections.list(definitions).size() == 1,
          "Duplicate pinned runtime class " + type.getName());
      try (JarFile jar = new JarFile(Paths.get(location.toURI()).toFile())) {
        JarEntry entry =
            jar.getJarEntry("META-INF/maven/" + group + "/" + artifact + "/pom.properties");
        DeltaRangerConfig.require(entry != null, "Missing runtime provenance for " + artifact);
        Properties properties = new Properties();
        try (InputStream input = jar.getInputStream(entry)) {
          properties.load(input);
        }
        DeltaRangerConfig.require(
            group.equals(properties.getProperty("groupId"))
                && artifact.equals(properties.getProperty("artifactId"))
                && version.equals(properties.getProperty("version")),
            "Incompatible runtime provenance for " + artifact);
      }
      origins.put(type.getName(), location.toExternalForm());
    } catch (Exception e) {
      throw new IllegalStateException("Cannot qualify pinned runtime class " + type.getName(), e);
    }
  }
}
