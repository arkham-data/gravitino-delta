# Delta production Ranger implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development task by task. Steps use checkbox syntax. Activate after continuous production-chain review is accepted; root owns service inputs and sequential source ownership.

**Goal:** Ship production Gravitino Ranger pushdown and Spark enforcement for fresh named Delta SELECT queries, including column access, Ranger masks/filters, two-catalog joins, and real grant/revoke propagation through Ranger 2.8 policy/role downloads.

**Architecture:** The production Delta runtime supplies trusted canonical catalog/schema/table descriptors and separate metadata-read/mutation transports. A dedicated authorization provider writes only labelled owned policies through existing Gravitino callbacks. An optional Spark extension reuses Kyuubi's pinned privilege extraction and masking-resolution helpers but constructs four-level requests before any deduplication; its genuine Ranger engine downloads policies and roles from the configured service.

**Tech stack:** Ranger 2.8.0, Kyuubi authz 1.10.2, Scala 2.12, Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, existing Java client/REST and authorization SPI. No core/API/server/client/storage-schema source changes.

**Inputs:** tracked `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`, `docs/superpowers/plans/2026-09-08-delta-production-provider.md`, `docs/superpowers/plans/2026-09-08-delta-spark-production-runtime.md`; read-only findings `/private/tmp/delta-production-ranger-enforcement-preflight-20260908.md` and `/private/tmp/delta-ranger-stock-bridge-preflight-20260908.md`. Reconcile transport/helper names against committed prerequisite interfaces before dispatch; do not reimplement them.

**Spec:** `docs/superpowers/specs/2026-09-08-delta-ranger-production-design.md`, extending `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`.

**September 14 recovery:** the referenced temporary preflight files and historical
runtime evidence disappeared during the interruption. Continue from the committed
foundation and persistent task ledger; requalify dependencies and affected behavior
with fresh artifacts. Historical reports remain historical. No Checkstyle task or
configuration exists in this baseline: use the existing Google Java Spotless check,
strict Javadoc and an explicit review of imports, member ordering and public/protected
documentation. The commands below reflect those available checks.

**September 21 continuation:** The user subsequently authorized starting the local
stack and continuing tests. Ranger persistence and Gravitino registrations were
verified. The original MinIO tmpfs had lost its data; all 12 archived Delta objects
were restored byte-for-byte into persistent MinIO, with an explicit new modification
time baseline. The native SnapshotState fix `e1347911c` passed both Delta adapter
suites (71 tests each) and independent review. Download recording and precise denial
assertions are implemented and qualified.

The four-pair column-policy matrices and fresh-driver lifecycle matrices now total
160 accepted query checks. Public user grant/revoke, group revocation and catalog
visibility removal were verified against Ranger 2.8.0, with independent identity,
download, model and result checks. Original memberships were restored; the known
global-guard divergence remains intentionally preserved. That lifecycle checkpoint recorded policy/
role generations 48/73. See the tracked `task-4-live-matrix-report.md` and
`task-4-lifecycle-report.md` under
`.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/` for scope and retained
failures. The test-only resume boundary in `c78df9948` was qualified at 44/63; future
resume work must explicitly bind the final checkpoint and its verified transition
chain, without replaying bootstrap. Recheck process and storage continuity before
using retained services.

A subsequent same-JVM fresh-query matrix passed 48 more checks across all four
version pairs, for 208 accepted live checks in total. Each pair retained one driver
through initial denial, public grant and public revoke. Final policy/role generations
are 56/81, with original memberships and all protected models preserved. See
`task-4-same-jvm-refresh-report.md` in the same ledger directory. This proves fresh
SQL analysis after combined policy/role updates; role-only refresh, outage behavior,
caches and reused-plan enforcement remain separate gates. A test-only unary
collection-boundary prototype passes on both Spark patches but installs no production
guard or complete security receipt. Task 4 remains open, as does the broader
full-Delta objective.

The next production foundation replaces numeric-only engine checks with opaque,
lease-bound policy and role publication stamps, including same-version role-map
replacement and observed refreshes during evaluation. A package-private metadata
read scope now checks the same finite lease around complete caller operations.
Both foundations passed independent review and 65 focused tests on Spark 3.5.4 /
Delta 3.3.2 with Ranger 2.8.0, plus formatting and Javadoc. See
`task-4-engine-stamp-report.md` and `task-4-metadata-scope-report.md` in the ledger.
The typed reader now detaches PUBLISHED registration receipts and logical schemas
inside that metadata scope. Its 19 tests and the 19 metadata lease regressions pass
on Spark 3.5.4 / Delta 3.3.2. Native public state/stats cache getters were also
characterized on Spark 3.5.4 / Delta 3.3.0 and Spark 3.5.6 / Delta 3.3.2 (two tests
each), including coexistence with the current production logical rules. Both
changes passed independent review (one report-only callback count corrected). These
tests establish capture order, not production native ticket enforcement.

Ordinary named wrapper loads now bracket native loading with registration reads
from the same metadata lease and bind the exact native descriptor, relation, index
and log. The initial four-pair matrix passed 119 test executions. A deterministic
collision-test correction then passed 15 focused tests on each pair (60 executions),
with Ranger 2.8.0 in every captured runtime. The new collection-guard component
passed four tests on each diagonal pair, including repeated collection, denial
before child preparation, Spark copies, rendering and serialization. It remains
uninstalled. See the binding and collection-guard reports in the ledger for exact
source versions and review status. Native operation ownership, physical provenance
and the complete original-decision receipt remain integration gates. The 208 live
checks and checkpoint 56/81 are unchanged by this offline work.

The engine now accepts an explicit immutable access time for each access, mask and
filter request. This prerequisite passed 36 focused tests and independent review;
the next logical receipt caller must sample one time for the whole batch. The
optimizer-prefix subclass compiles on both Spark patches; runtime replay and
complete receipt consumption remain unqualified.

## Global Constraints

- One Ranger resource service is provisioned for exactly one metalake. Server pushdown and Spark engine bind to the same service identity and metalake; canonical catalog is within that metalake. Policy labels are mutation ownership selectors, not an extra Ranger resource dimension. No global catalog-name uniqueness assumption.
- Validate a closed Ranger 2.8 service definition on startup and every observed definition refresh: every access/mask/filter resource uses RangerDefaultResourceMatcher with wildCard=true, ignoreCase=false, replaceTokens=false and replaceReqExpressions=false. Initial Ranger-integrated admission rejects literal `*` and `?` names before the first Admin mutation and before engine evaluation. Keep explicitly typed descendant wildcard selectors distinct from user literals. Braces, backslashes and token-looking text remain literal; do not invent SQL or backslash escaping. This scoped restriction does not change the base Delta provider and needs a later canonical-name/custom-matcher qualification before full name support.
- Preserve `catalog → database → table → column`. Access/mask requests include all four keys; filters end at table. Canonical names come from the registered Delta descriptor, never SQL parsing, current-catalog guesses, Spark aliases or path→name maps.
- Preserve native DeltaTableV2. Participating sessions maintain `spark.databricks.delta.catalog.update.enabled=false`; required runtime entry/commit checks and session-catalog sentinel qualification remain in force. This is a trusted session/deployment lifetime invariant, not protection from arbitrary later config mutation or cached-plan execution.
- Read-only metadata credentials may resolve registrations for users with Ranger-only column grants. They never become the registration mutation actor or authenticated Ranger query subject. Reuse the runtime's explicit read/mutation supplier seam, ordinary existing client authentication and lifecycle; no hard-coded metadata_service or new auth protocol.
- All code below is production `src/main`, with tests in `src/test`. No test classes/resources, LocalRangerPolicyClient, fake downloader, class shadowing, Kyuubi package overrides or reflection-based processor replacement in artifacts.
- Pinned Kyuubi privilege extraction reports top-level Attribute names and does not establish nested-field permissions. An allowed struct column covers that column; never broaden a leaf-only policy into a struct grant. A literal top-level column named `payload.secret` is not a nested path. Nested permissions and struct masking require their own qualification.
- First execution acceptance: fresh named SELECT with projection, predicates and two-catalog joins, CUSTOM/NULL masks and table row filters. Unsupported shapes/types must fail explicitly; they are not full Delta or full Ranger conformance. DML/streaming/maintenance/cached-plan and remaining masks are required follow-up gates before the user's larger goal is complete.
- Source work runs sequentially under root's ownership after the packaged Spark chain is accepted. Root owns deployment inputs/services/cleanup. This plan neither starts services nor authorizes changes to shared Ranger/demo state.

## Verified reuse and deliberate replacements

Cached Kyuubi 1.10.2 exposes these public APIs: `PrivilegesBuilder.build(LogicalPlan, SparkSession)` returning `(Iterable[PrivilegeObject], Iterable[PrivilegeObject], OperationType)`; `PrivilegeObject.catalog/dbname/objectName/columns/owner/actionType`; `AccessResource.apply(objectType, db, table, column, owner, catalog)`; `AccessRequest.apply(resource, UGI, operationType, accessType)`; `AuthZUtils.getAuthzUgi(SparkContext)`; `DataMaskingStage0Marker(child, scan)`, `RuleApplyDataMaskingStage1(spark)`, `RowFilterMarker(child)` and `RuleEliminateMarker`. SparkRangerAdminPlugin exposes the inherited Ranger 2.8 evaluation/refresh/version APIs and public `getMaskingExpr(AccessRequest)`/`getFilterExpr(AccessRequest)`.

Do **not** install stock `RuleAuthorization`, `RuleApplyDataMaskingStage0`, `RuleApplyRowFilter` or the whole stock RangerSparkExtension alongside this replacement: mask/filter constructors omit catalog, while authorization HashSet deduplication loses catalog before Ranger. Do not use `SparkRangerAdminPlugin.verify` as the fail-closed adapter because its null-result path can return without rejection. A ContextEnricher cannot restore already missing/deduplicated identity.

### Task 1: Ship Delta-aware Gravitino authorization SPI and real callback contracts

**Owned files:**

- `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/RangerDeltaAuthorization.java`
- Same package: `RangerDeltaAuthorizationPlugin.java`, `DeltaRangerResource.java`, `DeltaRangerPolicySelector.java`, `DeltaRangerAuthorizationConfig.java`, `DeltaRangerOwnerReconciler.java`.
- `authorizations/authorization-ranger-delta/src/main/resources/META-INF/services/org.apache.gravitino.connector.authorization.AuthorizationProvider` and `ranger-delta-servicedef.json`.
- Modify only module `build.gradle.kts`, add module README, and add distribution wiring needed to invoke its `copyLibAndConfig` task.
- Tests in matching module package: `TestRangerDeltaAuthorization`, `TestDeltaRangerPolicySelector`, `TestRangerDeltaProductionCallbacks`; keep original candidate tests as historical regression, not runtime implementation.


**Additional binding ownership:** narrow `DeltaCatalog` initialization/configuration and catalog-property metadata/tests validate immutable `authorization.ranger.delta.catalog`/`.metalake` against the actual CatalogEntity before `super.initAuthorizationPluginInstance` can mutate Admin state. No core or base-provider Ranger dependency. Task1 must explicitly support canonical CATALOG/USE_CATALOG/ALLOW as local-only visibility with zero Ranger resource/policy contribution, alongside TABLE/SELECT_TABLE/ALLOW; cover visibility-only and mixed role lifecycles and reject other combinations. Never inherit wildcard SELECT for visibility. Real REST tests initialize roles with SELECT before membership and separately characterize empty-role membership-before-first-grant. Ensure/verify the ordinary Ranger role on first relevant update; preexisting Gravitino membership requires qualified ordinary replay/readback, not inferred convergence. Test grant/revoke failure after local persistence, including last privilege removal: identical retries can skip callbacks, same-owner PUT repairs only owners, and unresolved grant divergence remains explicit. No blind replay or temporary access-broadening repair.

**Interfaces:** `RangerDeltaAuthorization extends BaseAuthorization<RangerDeltaAuthorization>`, `shortName() = "ranger-delta"`; `newPlugin(metalake, catalogProvider, properties)` accepts lakehouse-delta and returns the production plugin. `RangerDeltaAuthorizationPlugin extends RangerAuthorizationHadoopSQLPlugin` only where inherited role/user/group mechanics remain valid; explicitly override resource/owner translation and every policy-selection/update operation whose inherited HadoopSQL assumptions differ. Public methods/Javadoc follow project conventions.

Config uses existing `authorization.ranger.*` Admin URL/auth/service name and `BaseAuthorization` supplied metalake/catalog IDs, plus a required trusted `authorization.ranger.delta.metalake` binding checked against `metalake`. Service creation remains false: validate a provisioned compatible service/definition, do not auto-install/change a shared service definition. Distribution directory is `authorizations/ranger-delta/libs` with one pinned 2.8 closure; ordinary ranger default version stays unchanged.

Explicit ordinary grant contract starts with TABLE SELECT_TABLE/ALLOW → `select` on `{canonical catalog, schema, table, column=*}`. Reject unsupported grant types/conditions before policy writes; do not silently translate unknown privileges. Owner translation must be separately explicit for metalake/catalog/schema/table bootstrap and transfer: preserve Gravitino's existing owner semantics using correctly scoped four-level resources and the validated service's access vocabulary. Metalake-owner callbacks from a per-catalog plugin scope their policy resources to that plugin's catalog, avoiding another catalog's owned policy. Do not inherit the three-level owner translation or add success no-ops. Owner access is exactly `all`, whose impliedGrants are exactly select/read/write/create/drop/alter/update; it is not delegateAdmin or Ranger API administration. Complete owner leaf resources are C/*/*/* for metalake/catalog, C/S/*/* for schema and C/S/T/* for table. Distinct metalake/catalog owner items and ID-suffixed reserved roles must coexist without altering ordinary grants. Override the entire onOwnerSet callback: returning false is discarded upstream and inherited role failures are swallowed. Reconcile the complete desired owned role membership or precisely marked owner policy item, then authoritative readback; never use only the preOwner delta because a retry sees the newly persisted owner. Verify user/group existence because inherited false conflates already-existing with failed creation. Keep the marker role membership-free and reject foreign/conflicting reserved roles.

`DeltaRangerPolicySelector` accepts only policy type ACCESS, exact configured service, all three labels (`gravitino-managed`, metalake ID, catalog ID), exact expected resource keys/values and exclude/recursive=false; duplicate owned candidates fail. Gravitino grant/revoke/owner updates never mutate Ranger-owned column access, masks or filters. Reject unsupported rename/drop callback transformations rather than broad wildcard mutation; use the current provider lifecycle boundaries.

- [x] Add behavioral RED: ServiceLoader/newPlugin; rejected incompatible provider/service/metalake; allowed select with condition retained; exact selector/duplicates/adversarial wildcards; owner bootstrap/transfer; role create/update/membership/removal; two catalogs sharing one service; foreign access/mask/filter models unchanged.
- [x] Implement the production SPI/config/selector/translation, reusing existing Ranger client/helper APIs. Ensure selector coverage includes owner callbacks and every inherited mutating path. Reuse `RangerAuthorizationPlugin.onRoleCreated/onRoleUpdated/onGrantedRolesToUser/onGrantedRolesToGroup` only after these tests exercise actual resources. Do not promote CandidateDeltaPolicyPlugin unchanged.
- [x] Add real Gravitino REST/lifecycle tests through ordinary public client: create role/user, `grantRolesToUser`, `grantPrivilegesToRole`, `revokePrivilegesFromRole`; production SPI handles callbacks. Intercept only Admin boundary in unit/component tests, not the Gravitino dispatcher. Assert the actual callback matrix: explicit Owner PUT (including same-owner) and table POST report thrown failures after persisting ownership; table pending registration remains. Schema POST uses batch callbacks, logs their failures and may return success. Catalog POST attempts local deletion after failure without rolling back Ranger effects. After original request drain and current-owner read, explicit ordinary same-owner PUT is the supported serialized reconciliation retry; there is no expected-owner CAS or cross-system atomicity. Independently compare desired Ranger policy/role state and later downloaded versions. Owner relation presence is never Ranger convergence evidence. Test partial failures and stale prior-owner removal without broadening selectors. Real Ranger endpoint acceptance follows Task 4.
- [x] Build a dedicated 2.8 artifact using the existing ranger module's `runtimeJars`/`copyAuthorizationLibs` pattern, with no duplicate 2.4 classes or tests. Run focused module tests/Javadoc/format, inspect jars, report and commit task-owned files for review.

Commands at implementation time (not executed by this draft):

```bash
./gradlew :authorizations:authorization-ranger-delta:test --tests '*TestRangerDeltaAuthorization' --tests '*TestDeltaRangerPolicySelector' --tests '*TestRangerDeltaProductionCallbacks' -PskipITs
./gradlew :authorizations:authorization-ranger-delta:javadoc :authorizations:authorization-ranger-delta:spotlessCheck :authorizations:authorization-ranger-delta:copyLibAndConfig
```

### Task 2: Add the real policy-engine, subject and metadata-resolution adapters

**Verified protocol corrections (September 14):** use the single required Ranger
service/definition key `ranger.plugin.spark.gravitino.metalake`; secure downloads do
not propagate the earlier arbitrary `gravitino.metalake` key. Task 2 also owns the
narrow matching optional-provider config, definition, tests and README correction.
The Gravitino property `authorization.ranger.delta.metalake` is unchanged. Qualify
actual UGI non-token subjects; token query mode must reject while the existing
manager offers only unverified JWT cache identity. An authenticated token-to-UGI
bridge remains a full-goal gate. Separate metadata-client token authentication stays
supported through ordinary client builders.

**Owned files:** new module `spark-connector/v3.5/spark-delta-ranger/build.gradle.kts`, mapping `:spark-connector:spark-delta-ranger-3.5` in `settings.gradle.kts`; production package `org.apache.gravitino.spark.connector.delta.ranger` with `DeltaRangerConfig`, `DeltaRangerPolicyEngine`, `DeltaRangerResourceKey`, `DeltaRangerQuerySubject`, `DeltaRangerMetadataClient`. Matching tests `TestDeltaRangerConfig`, `TestDeltaRangerPolicyEngine`, `TestDeltaRangerQuerySubject`, `TestDeltaRangerMetadataClient`.


**Dependency and consumer contract:** use plain Kyuubi authz1.10.2 and explicitly select the complete Ranger 2.8.0 closure, including classloader/audit split artifacts. The base Delta runtime remains the sole supplier of its33 Kyuubi utilities; prove relevant nonpublic/member linkage and executed helper behavior before excluding companion duplicates. Preserve the private Guava/publicsuffix ABI. Metadata-client API exposes a bounded lazy read supplier/lease for Task3's no-arg catalog wrapper. Application discovery uses its own catalog-visibility grant; query/mutation identities remain separate.

**Interfaces:**

```text
DeltaRangerPolicyEngine.acquire(trustedConfig): AutoCloseable lease
  requireReady(): policy/role/service identity checked, no empty-engine success
  authorize(subject, canonicalResourceKey): void, null/deny/error rejects
  mask(subject, canonicalColumn): typed mask decision
  rowFilter(subject, canonicalTable): optional validated expression
  versions(): immutable service/policy/role version values
DeltaRangerQuerySubject.current(SparkSession): authenticated UGI snapshot
DeltaRangerMetadataClient: existing runtime read-metadata supplier interface
```

Use the actual pinned Kyuubi `SparkRangerAdminPlugin` singleton/Ranger downloader. A synchronized lease owner initializes once, rejects incompatible service/metalake/config leases in one JVM, drains/closes only after last owned user; do not one-catalog-close another's shared engine. Use ordinary `ranger-spark-security.xml`/credential mechanisms and a private cache directory; no Admin password passed to queries. Require exact runtime class provenance for Ranger 2.8 engine/roles/audit and Kyuubi 1.10.2. Record service binding, not secret-bearing config/maps. Require full-snapshot policy downloads: reject effective `ranger.plugin.spark.supports.policy.deltas=true` before singleton initialization; absent/false is the supported Ranger default. Both in-place and cloned incremental policy engines can reuse authorization context, so disabling only in-place updates is insufficient. This is independent of Delta Lake table support.

Ranger 2.8 AuditProviderFactory logs configuration properties at INFO, including Basic downloader passwords. Before accessing the Kyuubi singleton, the optional adapter must reject an INFO-enabled logger with that exact name. Deployment supplies WARN-or-stricter startup logging; test-only suppression is insufficient. Keep the diagnostic free of credentials and do not modify application logging globally. The first affected fixture credential was rotated and its old authentication rejected; retain that interrupted run separately from the safe live acceptance evidence.

Build AccessResource with owner/catalog fields **and** call `setValue("catalog", canonicalCatalog)` before any set/map insertion or evaluation. Deduplicate by immutable `DeltaRangerResourceKey(catalog, database, table, column, accessType, relevant owner)`; do not rely on inherited Ranger equals/hashCode. Preserve every table and every original column set from privilege extraction, including predicates and both join sides. Split columns only after retaining catalog; count/no-column reads require an explicit table-wide/select resource, never zero requests.

Capture the authenticated query UGI through the existing engine mechanism (`AuthZUtils.getAuthzUgi` only where its deployment identity behavior is verified); preserve actual groups and let Ranger resolve downloaded roles. Metadata service clients are separate read suppliers with bounded lifetime, sourced using existing client auth builders/private credential mechanism. Registration writes retain current actor transport. Identity/credential mismatch must fail before query execution, not silently use application metadata identity. Test non-token application mode and token request mode honestly.

For CUSTOM masks, retain `{col}` substitution semantics without turning column strings into unquoted SQL; for NULL return typed NULL. Fail explicitly for unsupported mask kinds. The public pinned `SparkRangerAdminPlugin.getMaskingExpr(AccessRequest)` already implements standard MASK, SHOW_FIRST_4, SHOW_LAST_4, HASH, DATE_SHOW_YEAR and service-definition transformer strings. Add a dedicated compatibility test of that API with catalog-bearing requests, quoted/unusual columns and real mask definitions before selecting it for additional types. Its null result alone cannot distinguish disabled mask from absent engine; combine with readiness/real evaluation semantics. Do not claim full mask support from CUSTOM/NULL acceptance or silently pass through an unknown enabled mask.

- [x] Add RED for same resource in two catalogs, immutable key coverage, missing/contradictory identity, actual query groups/roles, metadata-versus-query principal, null engine result, missing service definition, incompatible leases and closing one catalog.
- [x] Implement config/lease/request/subject adapters and real downloader configuration. No direct `setPolicies` in production. Tests may inject an evaluation boundary for deterministic errors, but deployment tests use Ranger's downloader.
- [x] Define explicit startup/outage contract: no ready policy snapshot means reject; last-known-good policy cache may continue during outage only as documented Ranger behavior, with active versions observable and revocation effective after refresh. No instantaneous revocation/no-cache claim. Test policy and role refresh independently.
- [x] Run focused tests, pinned dependency/JAR inspection and review. Keep engine dependencies optional and out of base Delta/default runtime.

```bash
./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerConfig' --tests '*TestDeltaRangerPolicyEngine' --tests '*TestDeltaRangerQuerySubject' --tests '*TestDeltaRangerMetadataClient' -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
./gradlew :spark-connector:spark-delta-ranger-3.5:dependencies --configuration runtimeClasspath
```

### Task 3: Implement bounded catalog-aware SELECT enforcement using pinned helpers

**Owned files:** same module production Scala package: `DeltaRangerSparkExtension.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerPlanAdmission.scala`, `DeltaRangerMaskRules.scala`. Tests `TestDeltaRangerReadRule.scala`, `TestDeltaRangerMaskRules.scala`, `TestDeltaRangerNativeSelect.java` in corresponding test packages. Modify the module build for Scala/JUnit and add README boundary/configuration.


**Additional binding ownership:** create production `DeltaRangerCatalogSpark35` in this optional module. Its no-arg constructor uses private initialization state/lazy metadata and fresh-current-actor suppliers; bind canonical config before delegate initialization and close only owned metadata resources, including failed initialization. Own narrow common driver/config tests for defaultfalse `spark.gravitino.enableDeltaRangerSupport` requiring DeltaSupport. String-only discovery selects this subclass and custom extension before session creation, rejecting stock authorization conflicts; no default static Ranger references. Test application-visible discovery with only local CATALOG/USE_CATALOG/ALLOW, a hidden-catalog negative and separate metadata resolution without table SELECT. Wrapper construction alone cannot fix omitted application catalogs.

`DeltaRangerSparkExtension extends (SparkSessionExtensions => Unit)` installs its admission/check and resolution/optimizer rules before the first session. Reject duplicate stock RangerSparkExtension configuration, since it would install incompatible rules and duplicate decisions. Preserve native Delta extension and unrelated explicitly compatible extensions. No global engine monkeypatch.

Admit fresh resolved SELECT plans over canonical native Delta TahoeLogFileIndex scans, covering projection, predicates, ordinary aliases and joins. Use an explicit finite plan-shape check; reject DML/DDL commands, views/UDFs, time-travel/streaming/maintenance or other unqualified routes in enforcement-enabled mode until their matrix cases exist. Internal native DeltaLogFileIndex reconstruction receives its narrowly typed metadata allowance; it must not become permission for arbitrary JSON/Parquet/path scans. Actual native caches also create LogicalRDD without that lineage. After authorizing the original finite canonical SELECT, an optional pre-CBO rule may delegate to public PrepareDeltaScan inside a private synchronous metadata scope for exactly qualified internal routes. Require finally cleanup/thread isolation, correct extension ordering before the native rule, and second-pass idempotence on both Delta patches. External LogicalRDD/raw/mixed sources still reject; no cache-disable, stack-based admission or broad schema/name heuristic. The separate readless SingleAction encoder exception remains narrow and tested. This limitation does not claim metadata confidentiality or arbitrary-job safety.

Before adding masks/filters, invoke `PrivilegesBuilder.build(originalResolvedPlan, spark)` and validate extracted tables/column sets against canonical native scan identities. Pinned native qualification shows CatalogTableTableExtractor drops catalog names. For required same-name joins, use a separate extraction-only copy with a unique opaque identifier per canonical scan and an exact local identifier-to-scan map, preserving all output ExprIds, expressions and owners; run the public whole-plan helper on that copy and execute the untouched original plan. Never reconstruct catalog identity after db/table grouping or infer it from names, paths or positions. Unknown, missing or ambiguous extraction results reject. Qualify asymmetric projection/predicate/join columns and aliases in both join orders before accepting this adaptation. Do not call stock RuleAuthorization. Build/deduplicate canonical requests as Task 2 specifies and authorize **original** columns, including denied predicate/join columns and SELECT *. Keep source and target resources separate even if names coincide. Masks do not grant access to denied original columns.

Obtain each table filter and each applicable column mask from Task 2, anchored to its scan identity. Install filters/mask aliases using the same marker topology as pinned Kyuubi's row-filter-before-mask order; reuse `RowFilterMarker`, `DataMaskingStage0Marker`, `RuleApplyDataMaskingStage1` and `RuleEliminateMarker` only after a small compatibility test proves resolution and fresh expression-ID remapping. Do not reuse stock request-generating stage-0/filter rules. Preserve original column authorization before helper marker elimination. Fixed-point application must not repeatedly filter/mask or reuse an earlier subject's decision. A per-analysis marker establishes only this fresh-analysis milestone, not revocation safety for cached/reused plans.

- [x] Add native RED: two catalogs with same schema/table/owners; distinct allow/mask/filter outcomes; join in both orders; denied projected, predicate and join columns; SELECT *; columnless/count behavior if admitted; raw Delta path rejection; malformed/unsupported enabled mask; internal metadata reconstruction; no repeated mask under analyzer fixed point.
- [x] Implement bounded admission and canonical authorization; verify PrivilegesBuilder preserves complete source/predicate resources before using its output. If a tested extraction shape loses required identity/columns, reject that shape and report it rather than guess. Reuse public pinned APIs only.
- [x] Implement/verify expression rewrites with actual executed rows, fresh alias IDs, quoting, nulls and joined identical column names. Add parameterized engine tests for standard mask helpers; qualify additional kinds explicitly or list exact remaining gates.
- [x] Verify subject changes/new sessions, class/module isolation, native descriptor/colliding session-catalog sentinel and disabled-update flag. Run focused tests on both Delta patches and review; defer live/pair qualification to Task 4.

```bash
./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerReadRule' --tests '*TestDeltaRangerMaskRules' --tests '*TestDeltaRangerNativeSelect' -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:javadoc
```

Native qualification additionally observed `Snapshot.allFilesViaStateReconstruction`
binding an `AddFile` encoder over a truly empty `LocalRelation`. Admit this exact
readless target only inside the synchronous `PrepareDeltaScan` scope, with the same
read/command/subquery/UDF exclusions and an outside-scope rejection test. This does
not expand the separate `SingleAction` exception to arbitrary encoder targets.
The native parsed-statistics cache also creates `LogicalRDD`. A narrow subclass of
public `PrepareDeltaScan` may override protected `getDeltaScanGenerator`, call
`super` once, require its actual selected `Snapshot`, and register that snapshot's
exact statistics-cache schema only in the current synchronous scope. Register only
for an already-authorized canonical index and return the generator unchanged.
`Snapshot.statsSchema` derives from metadata/configuration without preparing reads.
Do not infer the selected snapshot from a separate volatile read or cache schemas
across queries. Require wrong-schema/outside-scope negatives, both-patch linkage and
second-pass idempotence. The native private-in-Delta test callback is part of the
trusted-launcher boundary; do not replace or invoke it as an integration seam.
Final Spark `Dataset.collectFromPlan` also binds the exact `Row` object type over
empty `LocalRelation` after native preparation. Admit this readless binding with
the same guards, without requiring metadata scope or admitting generic object
encoders. Invalid `DeserializeToObject` roots must reject directly rather than
falling through to another metadata allowance. Preserve nonempty/read-backed/UDF/
subquery negatives and qualify actual result collection on both Delta patches.
Two native Scala UDF forms are required inside metadata preparation: Delta's
`CanonicalPathFunction` for add/remove paths and the nondeterministic
`DataSkippingReaderBase` size-collector function. Qualify each using the conjunction
of active authorized scope, exact native leaf/snapshot lineage, runtime class or
generated-lambda family, the same classloader and code origin as pinned `DeltaLog`,
function arity, input/output types, flags and argument lineage. Class name, origin
or signature alone is insufficient. Reject different same-signature user lambdas,
wrong arguments and either native form outside scope; original user plans and
readless encoder bindings still reject UDFs. Both patches require observed evidence.
This narrowly replaces the earlier blanket internal no-UDF shorthand and depends
on trusted runtime artifacts; no private API, reflective access or stack detection.
Restrict optional-module Spark/Ranger/Scala version overrides to its main/test
compile/runtime classpaths. Zinc and scalafmt require independent tool versions;
the earlier Zinc-only exemption was insufficient for the detached formatter
configuration. Preserve tool setup failures separately and verify actual Spark
classpaths retain Scala 2.12 and the required Ranger closure.
The existing trusted endpoint and metalake keys are `spark.sql.gravitino.uri` and
`spark.sql.gravitino.metalake`; use common configuration constants.

Task3 review correction: the initial wrapper requires SIMPLE for the effective
Gravitino application/current-actor client authentication mode, including the
existing default. A Basic username may equal the metadata principal even when
Spark/UGI differs, so the old comparison alone is insufficient. Reject Basic,
OAuth2, Kerberos, token and unknown modes before wrapper resource acquisition;
retain distinct-principal checks for admitted SIMPLE. Qualify the counterexample
and distinct SIMPLE positive in focused startup tests. Other application modes
remain later gates; Ranger downloader Basic and generic metadata builders remain
separate and unchanged.

### Task 4: Package and qualify the real Gravitino → Ranger → Spark chain

**Owned files:** `spark-connector/v3.5/spark-delta-ranger-runtime/build.gradle.kts`, settings mapping `:spark-connector:spark-delta-ranger-runtime-3.5`, deployment README; acceptance harness under `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerDownloadedPolicies.java`; result doc `docs/delta-ranger-production-results.md`. Harness starts separate production-only Spark/provider classpaths; test driver code is not installed in either plugin artifact.


**Additional acceptance gates:** actual full Ranger 2.8 dependency/class provenance and all four helper compatibility; real Gravitino-only visibility contributes zero Ranger SELECT and fresh-driver visibility revoke hides the catalog. Qualify Ranger-only column access without end-user Gravitino table grants, owner or wildcard policy workarounds. Initialize roles with admitted SELECT before membership, then revoke/regrant. Already-converged repetition is idempotence; callback failure after persistence is explicit divergence requiring independent Admin/download evidence, not repaired by identical grant/revoke replay or same-owner PUT. Preserve actual HTTP attempts/completion and test foreign policies throughout. Reconciliation and optional metadata-assisted discovery remain named subsequent gates.

Package `gravitino-spark-connector-delta-ranger-runtime-3.5_2.12` as a companion to the preceding Delta runtime. Include the enforcement module and exactly one reviewed Ranger/Kyuubi closure; keep Spark/Scala/Hadoop/Delta provided, and do not duplicate connector/client/protocol classes from the Delta runtime. Align audit classes with Ranger 2.8's split audit-core artifact; archive resolved classes/jar hashes. Server artifact remains independently installed under `authorizations/ranger-delta`.

- [ ] Add an opt-in external-endpoint test gate following existing Ranger/Delta conventions; missing explicit endpoint/private credential file/opt-in fails setup. Root supplies isolated runtime and owns cleanup. Use unique resource/tag service/definition with explicit linking; do not change shared defaults or deploy catalog-demo.
- [ ] Provision a compatible four-level service bound to one metalake and bootstrap production Gravitino catalog/auth provider. Through real public Gravitino client/REST create a role initially containing an admitted table SELECT securable object, then grant membership; verify exact owned Admin policy and fresh engine policy/role versions. Execute named SELECT, then revoke through Gravitino, await observed refresh and assert a **new** SELECT denied. Repeat already-converged requests idempotently and verify both systems; identical grant/revoke retries after callback failure are not reconciliation. No direct candidate plugin callback substitutes for REST.
- [ ] Independently create Ranger-owned column access/deny/CUSTOM or NULL masks/row filters. Assert foreign models preserved across Gravitino grant/revoke/owner operations and actual different masked/filtered rows from two catalogs and their join. Test projection/predicate/SELECT */path negatives, authenticated subject/groups/resources, metadata-only credential separation and engine startup/cache/outage behavior. Do not use table-wide grants to hide column-only metadata access problems.
- [ ] Repeat fresh JVM qualification across all four Spark/Delta pairs with real Ranger 2.8; retain nonzero original XML/process exits, class provenance, versions, artifact hashes and sanitized commands. Verify default/base Delta runtime unchanged. Commit owned files/results for review after checks; no push/merge/demo change.

Expected packaging command:

```bash
./gradlew :spark-connector:spark-delta-ranger-runtime-3.5:shadowJar -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
```

Finalize exact opt-in harness property names against root's supplied test runtime before implementing that harness. Their values/private file contents must never be placed in tracked reports. A deployment authentication failure is setup evidence, not an authorization behavioral failure or a reason to switch to local policies.

## Completion and remaining user-goal gates

This plan is complete only when production SPI grant/revoke and role membership propagate via actual Ranger downloads into executed canonical fresh SELECT results on all four pairs, foreign policies survive, and artifacts contain no fixtures. It does not complete the user's full Delta/Ranger goal by itself.

The following must be tracked in the subsequent conformance plan: all remaining Ranger standard/custom transformers and expression semantics; DML source/target and write actions, CTAS/RTAS/COR paths, ALTER/RESTORE/time travel, streaming/checkpoint restarts, maintenance/history, caches/reused plans/refresh revocation, metadata exposure and supported launcher boundary. Each route is explicitly rejected or deployment-restricted until qualified; unsupported masks are never silently ignored. Existing native runtime support without enforcement is not authorization evidence. Isolated catalog-demo integration follows production qualification and these agreed conformance gates.

The preflight stage used source/cached-bytecode inspection only. This plan does not claim production Ranger implementation, test execution or service qualification.

## Recorded design decisions and source evidence

Owner callback source/failure-propagation review is complete in `/private/tmp/delta-ranger-owner-callback-preflight-20260908.md`; its exact entry-path failure matrix and desired-state reconciliation contract are adopted above. No Gravitino core change is authorized.

Ruling: Task1 also owns narrow provider-side binding admission in the now-completed production DeltaCatalog and catalog property metadata/tests. Require immutable trusted `authorization.ranger.delta.catalog` and `.metalake` when authorization-provider is ranger-delta; reject mismatches against the actual CatalogEntity name/namespace before superclass initAuthorizationPluginInstance can invoke any Admin side effect. BaseCatalog only injects IDs and plugin.newPlugin does not receive the catalog name. Use ordinary public override seams: capture a defensive resolved configuration in withCatalogConf if needed, validate in initAuthorizationPluginInstance, then delegate to super without changing core or exposing secrets. Direct plugin construction must validate nonblank canonical names and injected IDs; the real provider hook establishes correspondence to the entity. No guessed first-callback binding, wildcard catalog, reentrant catalog load, reflection or unchecked property. Cost: a small provider-to-optional-authorization configuration contract and tests; no Ranger runtime dependency is required in the base provider. Stage these provider-local files as explicit Task1 ownership.

Ruling: schema CREATE success is not an owner-policy synchronization acknowledgment because upstream batch callbacks swallow failures. Qualify and document explicit same-owner reconciliation plus independent Admin/download readback rather than falsely promising atomic propagation or changing core. Cost: deployment/controller convergence verification remains necessary; partial callback/fanout effects and refresh delay remain visible limits.

Ranger Admin metadata lookup is separately unimplemented: the current fixture sets lookupSupported=false. `/private/tmp/delta-ranger-metadata-lookup-preflight-20260908.md` verifies the Ranger 2.8 RangerBaseService SPI and public Gravitino client traversal. An Admin-only module can offer catalog/database/table/top-level-column discovery using a scoped read client per call without another metadata store. Installed Admin2.8 loader/API preflight is now complete at `/private/tmp/delta-ranger-admin-loader-preflight-20260908.md`: ServiceMgr loads RangerServiceDef.implClass from immediate children of WEB-INF/classes/ranger-plugins/<serviceDef.name>, parent-first, and caches failed-load RangerDefaultService fallback by type. Use controlled owned lifecycle for later installation and verify actual class/JAR provenance; private directory alone does not isolate Gravitino client dependencies. POST /service/plugins/services/lookupResource/{serviceName} accepts ResourceLookupContext and returns string array; POST /service/plugins/services/validateConfig accepts RangerService and returns VXResponse, whose statusCode0 (not merely HTTP success) indicates validateConfig connectivityStatus=true. Lookup/validation timeout defaults1000/10000ms require appropriate bounded client timeout configuration. Installed credential handling specifically recognizes password; arbitrary custom secret key handling remains unqualified. No plugin is installed or enabled, and auth/credential/real HTTP behavior remain finite deployment tests before lookupSupported=true. Suggestions use ordinary published metadata and the service's read identity; they are not per-query-user visibility or grants. This optional policy-editor feature is distinct from actual grant/revoke/download/enforcement acceptance.

Ruling: the Ranger companion uses plain kyuubi-spark-authz_2.12:1.10.2, not its pre-shaded fat artifact, and explicitly resolves the complete Ranger 2.8.0 closure (the actual published authz POM defaults key modules to2.5.0). The accepted Delta runtime already supplies33org.apache.kyuubi.util classes from its KyuubiHive 1.11.0 input. Root javap found matching public member descriptors across all33; this is a feasibility check, not full helper execution. Keep one utility supplier from the base Delta runtime, exclude duplicate companion utility inputs, and prove protected/package-private/member linkage plus executed helper behavior in both real Spark distributions before acceptance. Distinguish Kyuubi authz1.10.2 and Hive 1.11.0 provenance; do not assert one namespace-wide version. Preserve the reviewed private Guava/publicsuffix ABI. Exact evidence and consequence if compatibility fails: /private/tmp/delta-ranger-runtime-overlap-preflight-20260908.md and its JSON/javap artifacts. No final artifact, Ranger download or runtime qualification has occurred here.

Ruling: Task3 also owns `DeltaRangerCatalogSpark35` in its optional Ranger module and narrow string-only common driver/config tests for `spark.gravitino.enableDeltaRangerSupport` (defaultfalse, requiresDeltaSupport). The accepted base adapter's constructor supplier seam is available, but default reflective discovery does not install a metadata read client and rejects a user-prebound catalog class. Use a no-arg subclass with constructor state/lazy suppliers, initialize canonical config before delegation, keep current-actor manager resolution per mutation, and close only owned metadata resources, including failed initialization. Enabled startup selects that subclass and custom extension before discovery/session creation, rejecting stock authorization extension conflicts; disabled/default dependency isolation persists. Full rationale and required tests: /private/tmp/delta-ranger-metadata-bootstrap-preflight-20260908.md. Task2's metadata-client component must expose the bounded lease/supplier needed by Task3; constructor seam tests alone do not prove production wiring. No base adapter/core/client source changes are needed for this selected wrapper approach.

Packaged Spark Task3 transport evidence qualifies the owner/recovery contract: the ordinary Gravitino HTTP client retries PUT requests; no application-level loop does not mean no wire-level retry. Future Ranger callback tests must retain actual attempt counts and distinguish a fully observed upstream response from a client/proxy timeout. Local listener/handler drain cannot prove a timed-out Gravitino request or its Ranger callback completed. Unknown forwarded work remains unresolved until independently confirmed or its actual owning server process has stopped. Same-owner reconciliation must not run across that unresolved boundary; do not solve it with sleeps or a changed test-only transport policy. Current no-core contract remains serialized, desired-state reconciliation with explicit completion evidence and separately observed downloaded policy versions.


## Independent pre-dispatch corrections

Source-only independent review: `/private/tmp/delta-ranger-plan-independent-preflight-20260908.md`; root confirmed the callback/discovery entry paths in committed code. These additions are binding task requirements, not claims of executed qualification.

- **Task1/4 role bootstrap:** initialize each ordinary test role with an admitted table SELECT securable object before assigning membership, so actual catalog callbacks run. Add real REST regression for legal empty role → membership → first grant: empty roles route no creation or membership callbacks, and a later onRoleUpdated is not inherited role/membership reconciliation. On a first relevant catalog grant, explicitly establish/verify the Ranger role and replay existing membership through the ordinary public membership API where qualified; never declare propagation from policy presence alone. If the implementation cannot qualify that replay without ambiguity, report divergence explicitly. Automatic reconciliation for arbitrary bootstrap order remains a concrete subsequent requirement, not an inherited guarantee.
- **Task2/3/4 discovery:** the application identity must have Gravitino catalog visibility before driver discovery; the optional wrapper is initialized only after application catalogs have been listed. Qualify minimal existing catalog-visibility grants without table SELECT and prove a Ranger-only column user resolves table metadata through the separate read supplier. Add a hidden-catalog setup negative and documented diagnostic. Do not globally substitute metadata credentials into the current-actor manager. Scoped metadata-assisted discovery without application visibility remains an additional feature requiring explicit ownership/tests if selected later.
- **Task1/4 grant divergence:** add real REST failure-after-persistence tests for first/additional grant and last-privilege revoke. Ordinary identical retries can skip callbacks once Gravitino state already matches; therefore success of replay cannot certify Ranger convergence. Same-owner PUT repairs only owner state. Require separate authoritative Admin readback and downloaded policy/role version observations; unresolved ordinary-grant divergence is explicit and cannot be cleared by a blind retry. Preserve actual attempt/completion evidence. A serialized desired-state ordinary-role/grant reconciliation path is required before the larger production rollout is complete; do not invent a temporary revoke/regrant that broadens access or claim cross-system atomicity.

Ruling: retain the bounded ordered bootstrap and catalog-visibility prerequisites for the first SELECT qualification, and expose failed ordinary-grant convergence as unresolved — these reflect existing public callback/discovery behavior while preserving no-core scope — cost is additional explicit role/grant reconciliation and optional discovery work before unrestricted production rollout. These limitations must appear in results and the next conformance plan, not disappear on completion of this stage.


### Explicit catalog visibility privilege

Task1 additionally admits exactly CATALOG / USE_CATALOG / ALLOW bound to this plugin's verified canonical catalog as a Gravitino-only visibility privilege. It produces zero Ranger access resources and zero policy items; never inherit HadoopSQL's USE_CATALOG → wildcard SELECT mapping. This is an enumerated supported local privilege, not an unknown-privilege fallback. TABLE / SELECT_TABLE / ALLOW remains the only ordinary grant translated to Ranger access. Reject other ordinary type/privilege/condition combinations, including METALAKE-scoped USE_CATALOG, unless separately qualified. Validate all securable objects and role changes before Admin mutations. Handle visibility-only and mixed visibility/SELECT roles consistently across create/update/delete/user/group callbacks. Mirrored role/membership records may exist but must carry no access from visibility alone.

Task3/4 startup tests grant application catalog visibility without any effective USE_CATALOG deny, Gravitino table SELECT or ownership. The separate scoped metadata identity resolves schemas/tables; the actual query subject remains Ranger's subject. A visibility-only application must discover the optional catalog and remain denied every SELECT until an independent Ranger access policy permits it. Real REST tests cover role/membership creation, visibility grant/revoke and mixed roles; assert no wildcard/access policy from visibility, foreign models unchanged, unknown privilege rejection, and visibility removal preventing fresh-driver discovery. Exact source proof is the narrow followup in the independent preflight report.

Ruling: explicitly support canonical catalog USE_CATALOG/ALLOW as local-only visibility — it is necessary before discovery and inherited HadoopSQL would broaden it into data SELECT — cost is deliberate filtering across all ordinary role lifecycle paths, backed by zero-access-policy and mixed-role tests. This does not alter owner access translation or authorize unknown privilege no-ops.


### Task 4 runtime and packaging qualification

Use immutable archived production server, optional plugins, coordinator application and actual Spark distribution classpaths. Qualify the companion on JDK17, excluding only Java21 multi-release entries unsupported by pinned Shadow ASM. Relocate only the three Ranger-owned ugsync helper families under the Hadoop namespace and verify direct/reflective references; preserve genuine Hadoop references. Kerberos remains outside this qualification. Resolve duplicate Hostname implementations to the pinned Kyuubi supplier with original bytecode provenance; use Spark-provided Servlet and JSR305 classes.

The separate test application may record normal loopback callbacks and inject exactly one fault scoped to an owned endpoint and method. Preserve actual response bytes/status and record completed public Gravitino responses separately from local callback-handler drain. Unknown upstream work cannot be retried or reconciled based on local drain. Root controls the production server lifecycle and direct foreign-policy fixtures; the implementer controls public coordinator mutations after an explicit handoff. Keep one coordinator/gateway lifecycle across the final four-pair matrix.

The deployed Ranger2.8 role update exposed incompatible client timestamp formatting. Keep the correction in the optional Delta client: detached complete request maps for role/policy writes, original createTime/updateTime expressed as epoch milliseconds, typed pinned responses, preserved resource validation and caller models. Do not mutate Ranger's shared serializer or Gravitino core. Qualify wire-level fields and actual deployed callbacks separately. The fresh-user204 and503 completion gates remain required after this earlier blocker is resolved.

Pinned Ranger2.8 write methods can treat HTTP503 as success after decoding its body. Enforce the declared HTTP status in the optional client for the nine affected callback writes (role/policy create/update overloads, policy deletion, named role deletion, and role grant/revoke). Keep the four callback reads inherited: actual GET503 already throws before the shared response handler returns. Preserve authentication, typed decoding, numeric timestamps, resource validation and missing-role classification; no global serializer/core or unused CRUD changes. Unit wire failures and successful200/204 controls complement, rather than replace, the real failure-after-persistence tests.

### Task4 live metadata admission correction (2026-09-15 UTC)

Actual public table GET returned403 for the separate metadata reader. Existing server
authorization requires USE_CATALOG, USE_SCHEMA and SELECT_TABLE; Task1's optional
contract omitted USE_SCHEMA. Extend only the optional provider to accept canonical
SCHEMA/USE_SCHEMA/ALLOW as local visibility with zero Ranger access resources/items.
Retain exact catalog/literal/type/ALLOW validation and fail closed for all other
unqualified combinations. Test schema-only and mixed SELECT role lifecycles,
membership, and absence of wildcard or access policy contributions. Normal mirrored
Ranger roles/principals are allowed. Give scoped schema visibility only to the
metadata reader; query identities remain catalog visibility only. No core changes,
ownership workaround, or use of local persistence after rejected callbacks.

Preserve the original first-query failure: the fixture configured only native Delta
in SparkSession while driver wiring installed the companion. A new private profile
explicitly orders companion before Delta; isolate that configuration correction in
a read-only query before qualifying metadata admission. Runtime/plugin archives
remain immutable; any provider fix requires focused RED/GREEN, review, archive,
completed coordinator close, and an owned server restart before new public grants.

### Post-Task4 full Delta conformance order

Independent source audit distinguishes the existing native four-pair tests from
Ranger qualification. Task4 actual public-client/downloaded-policy fresh SELECT
composition is still pending. Do not expand its accepted scope from packaging or
native test results. Subsequent implementation slices, each requiring actual
Ranger 2.8 downloads on all four pinned Spark/Delta pairs, are:

1. Enforce subject and policy-generation checks when an analyzed plan or cached
   Dataset is reused. Current identity tags and rewrite-time version checks only
   qualify restricted fresh queries. Establish an execution boundary before
   expanding authorization to writes.
2. Map DML source SELECT and target write/update actions, including MERGE, UPDATE
   and DELETE row-policy semantics; deny or safely implement unsupported cases.
3. Authorize registration/DDL before reserve, native commit and publish. Qualify
   CREATE/REPLACE/ALTER and implement missing native DROP/PURGE/RENAME with durable
   registration recovery, not an uncoordinated native mutation.
4. Gate metadata/history/refresh outputs and historical snapshots. Bind time travel
   to current registration UUID/generation and prevent reads across replacement
   incarnations.
5. Extend aggregate, columnless/count, join and nested-column policy semantics;
   retain field lineage and types and preserve row filters before aggregation.
6. Qualify streaming/checkpoint policy generations and maintenance commands
   (including OPTIMIZE/VACUUM/RESTORE) with command-specific authorization/recovery.

These are remaining requirements, not completed features or extra authorization
from the user. The audit preserves no-core source constraints and separate metadata
identity. Plan details and tests will be developed from each concrete execution
path; full Delta support cannot be claimed until the missing paths are implemented
and qualified. Isolated demo integration remains a subsequent deployment gate.

### Feature-bearing Delta tables remain separate conformance requirements

The independent audit at `8c383c940` found that protocol/configuration recognition
does not establish feature execution. The tested native lifecycle covers column
mapping none/name and ordinary add-column refresh; registered create/replace
currently rejects id mapping. Metadata-reader V2-checkpoint fixtures are not
evidence of a Spark-produced V2-checkpoint lifecycle. The following remain:

- Deletion-vector data lifecycle and correctness: enable, append, DELETE/UPDATE/
  MERGE, restart/checkpoint/refresh, historical read and Ranger filter/mask reads.
- Native V2-checkpoint production and reload, including sidecars and history after
  earlier JSON pruning.
- Column-mapping identity and preview/stable type-widening behavior: verify what
  the pinned native engines support, add corresponding registration paths, and
  retain physical field identity and mask types across schema changes.
- Change-data-feed production and reads, including before/after images, version
  ranges, schema changes, replacement boundaries and explicit authorization.
- Row tracking and clustering enablement/dependencies, hidden metadata, writes,
  maintenance, restart/refresh and combinations with mapping/deletion vectors.

Run native controls and relevant Ranger enforcement for each supported feature in
all four pinned Spark/Delta cells. Synthetic feature descriptors, preserved raw
properties and reader compatibility must remain labeled separately from qualified
feature behavior. This is required for the full Delta objective; Task4 fresh
SELECT composition does not close these gaps.
