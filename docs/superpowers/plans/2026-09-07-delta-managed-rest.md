# Delta Managed Provider REST Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. The user has authorized proceeding with delegation; no repeated approval checkpoint is needed for this reversible test package.

**Goal:** Verify managed registration through real Gravitino HTTP, dispatcher, property validation, authorization and owner hooks.

**Architecture:** A test-only ServiceLoader catalog composes RegistrationLifecycleFixture and adds a raw-row schema guard. MiniGravitino runs the existing JCasbin authorizer against file-backed H2, with requests from distinct simple-auth principals.

**Tech Stack:** Java8-compatible sources, JDK17, Gradle8.2, JUnit, MiniGravitino, Java REST client, H2 and existing JCasbin. No Docker in this harness.

**Spec:** [Managed REST design](../specs/2026-09-07-delta-managed-rest-design.md).

## Global Constraints

- No Gravitino core/API/server/client/storage-schema source changes.
- New provider and harness stay in generic-module test sources/resources; no production provider registration or distribution.
- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 and Ranger 2.8.0 remain required eventual targets.
- Gravitino owns registrations; no Glue/Hive registration authority is introduced.
- Ranger owns column permissions, masks and filters; do not add a broad SELECT grant or change existing Gravitino privileges.
- Use real file-backed H2, disabled metadata cache, real HTTP clients, enabled authorization and the existing JCasbin authorizer.
- Simple test principals are client-selected names, not production OAuth/Kerberos identity assurance.
- Native MATCHED evidence and writer handoff remain explicit test controls; no native commit correlation or distributed fencing claim.
- Reuse the reviewed lifecycle protocol without changing its transitions or weakening record validation.
- Capture original module XML with nonzero counts, commands, exits and runtime configuration; setup failures are not behavioral RED.
- English code/docs, Java8-compatible sources on JDK17, normal imports, Nullable, SLF4J, and Javadoc on public/protected declarations and overrides.

---

## Task 1: Auth-enabled managed registration REST harness

**Files:**
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/RegistrationTestCatalog.java`.
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/TestDeltaRegistrationRest.java`.
- Create or append test provider entry in `catalogs/catalog-lakehouse-generic/src/test/resources/META-INF/services/org.apache.gravitino.CatalogProvider` (preserve other entries).
- Create `docs/delta-managed-rest-results.md` for this package's evidence and later deployment handoff.
- No shared build changes expected. A necessary test-only dependency or small support file requires a recorded controller ruling; no production edit.

**Interfaces:**
- `RegistrationTestCatalog extends BaseCatalog<RegistrationTestCatalog>` implements the existing provider contract, public no-arg constructor, short name `delta-registration-test`.
- `newOps(Map<String,String>)` returns an adapter implementing `CatalogOperations`, connector `SupportsSchemas`, and `TableCatalog`; `initialize(Map<String,String>, CatalogInfo, HasPropertyMetadata)` uses real `GravitinoEnv.entityStore()` and `idGenerator()`.
- Compose `RegistrationLifecycleFixture(EntityStore, SupportsSchemas, IdGenerator, Supplier<String>, String recoveryPrincipal, BooleanSupplier writerHandoff, RegistrationCommitVerifier)` in its existing package. Principal supplier is `PrincipalUtils::getCurrentUserName`, never a command field.
- Reuse `RegistrationCommand.KEY`, `INTERNAL`, `reserve`, `publish`, `beginReplace`, `abort`, `encode`; preserve current generation/token and immutable observation contracts.
- Provider test controls capture incoming dispatcher StringIdentifier/call counts and supply clearly synthetic verifier results. Expose only the narrow access needed by this test; reset controls after each test/server shutdown.
- Existing `GenericCatalogCapability` supplies TABLE/SCHEMA managed capability. Catalog/schema metadata can be empty; do not inherit mandatory generic format routing.
- Declare mutable nonreserved command input and reserved hidden internal prefix using actual property metadata factories:

```java
stringOptionalPropertyEntry(RegistrationCommand.KEY, "Registration command", false, null, false)
stringPropertyPrefixEntry(RegistrationCommand.INTERNAL, "Internal registration state", false, true, null, true, true)
```

Receipt prefix `delta.registration.receipt.` is reserved but visible. Base metadata
already declares StringIdentifier.ID_KEY reserved and hidden. Preserve source
factory parameter meaning; read PropertyEntry and BasePropertiesMetadata.

- [ ] **Step 1: Wire the smallest real HTTP fixture and first executable test.**

Use actual APIs from MiniGravitino and existing client authorization examples:

```java
MiniGravitino server = new MiniGravitino(new MiniGravitinoContext(config, true, true));
server.start();
GravitinoAdminClient client = GravitinoAdminClient.builder(uri).withSimpleAuth(principal).build();
```

Configure relational file-backed H2 via ENTITY_RELATIONAL_JDBC_BACKEND_PATH,
CACHE_ENABLED=false, CATALOG_LOAD_ISOLATED=false, ENABLE_AUTHORIZATION=true,
AUTHENTICATORS=simple, SERVICE_ADMINS=registration-admin, auxiliary services
disabled. Derive actual HTTP URI from JettyServerConfig and server.getServerConfig().
Do not extend broad BaseIT or start Docker. Restore singleton configuration and
injected fields, close clients and stop server. Keep test execution serial.

Use real admin client to create metalake/users/catalog/schema and role grants.
Actor gets CREATE_TABLE and use privileges, remains non-admin and not schema owner.
Outsider gets only use privileges. A separate foreign/recovery principal may get
existing MODIFY_TABLE/use to distinguish core permission from provider actor checks.
Do not grant SELECT_TABLE as a metadata/authorization shortcut.

First test should exercise a hidden reservation and schema cascade. Start with
ordinary managed schema delegation, create by HTTP with command only and empty
columns/null comment, and assert cascade refuses the pending row. Capture its
actual executed failure before adding the raw-row guard. If behavior is already
correct, record characterization honestly; do not force synthetic RED mutations.

- [ ] **Step 2: Run executable RED and retain evidence.**

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs --tests '*TestDeltaRegistrationRest'
```

Compiler, authentication bootstrap, classloader or server-start failures are setup,
not intended behavioral RED. Copy XML from module `build/test-results/test` before
another run, assert nonzero test count, retain exact command and exit.

- [ ] **Step 3: Implement adapter/guard and complete behavior tests.**

A raw schema guard must inspect authoritative TableEntity rows:

```java
if (!store.list(tableNamespace, TableEntity.class, Entity.EntityType.TABLE).isEmpty()) {
  throw new NonEmptySchemaException("Registration records still exist");
}
return managedSchemas.dropSchema(identifier, cascade);
```

Use actual exception constructor and namespace mapping from existing source.
Test these named behaviors (may use parameterization for parallel cases):

1. `testDispatcherIdentityAndExactCreateRetry`: real HTTP create carries no ID; capture distinct dispatcher candidates on repeat, same original receipt ID/raw row/audit/properties, exactly one durable row. Verify managed persistence avoids an extra store mutation via full-row stability or a narrow write observation. Do not inject IDs in requests.
2. `testPendingVisibilityAndPropertyValidation`: list/GET/exists hide pending; GET leaves row intact. Core rejects client reserved ID/internal/receipt writes and internal removals before adapter mutation. Malformed command version and mixed ordinary changes reject with unchanged row/verifier count; input command is not persisted.
3. `testPendingOwnershipAndAuthorization`: actual non-admin actor becomes raw owner and stored creator; owner-authorized pending alter publishes on synthetic MATCHED. Outsider rejected before provider alter invocation. Existing MODIFY_TABLE foreign subject reaches provider but actor check rejects; record actual HTTP mapping without forcing403.
4. `testOwnerRestVisibilityBoundary`: raw owner exists but admin owner REST GET/SET fails for pending table; after synthetic publication owner REST works. Label pending repair gap explicitly.
5. `testOwnerHookFailureAndCreateRetry`: temporarily replace only private GravitinoEnv.internalOwnerDispatcher with forwarding fault injection on target TABLE setOwner before delegate. HTTP create fails yet RESERVED row survives without owner; CREATE-only actor alter denied. Restore field in finally; exact create retry uses new candidate ID, same row, assigns owner, no native replay. Do not replace whole table dispatcher/auth filter.
6. `testRecoveryRequiresExistingPermissionAndEvidence`: fixture recovery principal with existing MODIFY_TABLE/use still needs handoff and verifier; token possession/ordinary outsider cannot change row. Preserve AMBIGUOUS unavailable state and no data deletion.
7. `testSchemaGuardForAllRegistrationStates`: refuse cascade/noncascade for RESERVED, REPLACING, PUBLISHED, ABORTED, QUARANTINED raw rows; sentinel local data and row remain. Empty schema drop succeeds.
8. `testUnguardedManagedCascadeBoundary`: disposable schema/raw hidden reservation, directly invoke the existing unguarded ManagedSchemaOperations cascade; row disappears without table drop while sentinel remains. Clearly separate baseline deficiency from guarded provider acceptance. No catalog/metalake/fileset/concurrent-drop claim.

- [ ] **Step 4: Run focused GREEN, lifecycle regression and formatting.**

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs --tests '*TestDeltaRegistrationRest' --tests '*TestDeltaRegistrationLifecycle' :catalogs:catalog-lakehouse-generic:spotlessCheck
```

Run module spotlessApply before final check. Preserve per-class nonzero XML,
commands/exits/configuration. No Kernel/Spark/Ranger matrix repetition unless
shared code changes create a concrete regression concern. If tests expose a
no-core interface limitation, preserve exact failure and characterize it; do not
silently mock authorization or widen end-user access to make the test pass.

- [ ] **Step 5: Results, self-review, scoped commit and independent review.**

Document actual identity/managed persistence/HTTP authorization and owner outcomes,
owner REST gap, schema guard versus unsafe baseline, source file references,
synthetic matching/handoff/simple identity assumptions, and remaining production
limits. Include MinIO pinned local test option and catalog-demo Ranger2.7 mismatch
as deployment handoff, without claiming those services were tested by this suite.
Only scoped provider/test/resource/results are committed. Controller reviews the
diff/evidence, resolves findings via the implementer, and retains archived reports.
