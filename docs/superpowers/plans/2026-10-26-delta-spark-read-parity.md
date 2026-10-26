<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Delta Ordinary Spark Read Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already selected subagent-driven execution; preserve that choice.

**Goal:** Support the approved ordinary named SQL/classic-PySpark read operations with deferred column permissions and current authorization at each supported action.

**Architecture:** Preserve an unrewritten, session-bound ordinary-read origin during analysis/composition. Normalize the completed query before optimization to extract original demands, apply Ranger filters/masks once and issue the executable receipt. Extend the outer action boundary for native Arrow while retaining captured in-flight decisions and separate CDF/MERGE/append authority.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server and clients 2.8.0, MinIO S3, existing Scala/JUnit and classic PySpark harnesses.

**Spec:** [Approved design](../specs/2026-10-26-delta-spark-read-parity-design.md), approved after `255480dd9`. Read both documents.

**Status:** Approved by the user with “Proceed” after `eb8b1bf0d`. Tasks 1 and 2 are complete and independently reviewed; Task 3 is starting. Final four-pair/live qualification remains Task 5. Baseline product milestone: `683ba8976`. Existing worktree: `.worktrees/delta-spark-feasibility`, branch `codex/delta-spark-feasibility`.

## Global Constraints

- Qualify Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server and clients 2.8.0, and S3-compatible MinIO.
- No Gravitino core/API/server/client behavior changes, Spark/Delta fork, second metastore, path fallback, public MERGE API change or automatic session-setting changes. Build metadata repair is allowed in Task 5.
- Gravitino owns registration. Ranger owns column grants, filters and masks. The platform's single-writer constraint is unchanged; this milestone adds no writes.
- `spark.table("t").select("id").count()` requires id SELECT, not every schema column. Explicit inner projections, predicates, join/sort/count references survive pruning. A columnless occurrence uses existing table-wide SELECT (`column="*"`).
- Filters/masks precede user expressions. Every original occurrence keeps its registration/demand even if no scan survives. Raw statistics must not replace governed aggregate semantics.
- Planning may authorize; every supported action rechecks. An already-planned stale receipt rejects. Accepted in-flight work uses captured decisions; no per-stage/final-output or atomic revocation promise.
- Preserve the trusted stock execution model, extension order and protected-setting/foreign-rule checks. Do not revive exact-stage authentication.
- Do not broaden shared policy/CDF/MERGE scalar grammar, CDF reuse, MERGE sources or append/property AQE. Cache/persist/checkpoint, global/permanent/SQL-text views, general aggregates/windows, streaming/Connect and separate-worker deployment remain outside scope.
- Catalog-demo and append AQE stay in separate plans. Preserve private inputs, original evidence and worktrees; serialize heavy tests and never print credentials.
- Keep original failures. Reuse only unaffected/equal-input evidence. No push until the bounded milestone closes; then use the user's standing push authorization after checking branch and remote. No PR or deployment is requested.

## Review Focus

- A successful collect followed by revoked Arrow must not borrow the previous accepted context. Task 1: `mixedActionsReauthorizeBeforePreparation`.
- Schema discovery, explicit SELECT-star and alias/self-join remapping must not turn implicit schema into all-column demand or erase explicit demand. Task 2: `schemaAndExplicitProjectionHaveDifferentDemands` and `aliasedOccurrencesKeepOriginalDemands`.
- Optimizer null folding, zero-width scans and zero limits must preserve count results and obligations. Task 3: `emptyAndPrunedCountsRetainAuthority`.
- A replaced/dropped local view or composed previously actioned frame must keep original intent without replaying a stale mask. Tasks 2/4: `newCompositionUsesOriginalIntent` and `viewReplacementAndDropKeepBoundDefinitions`.
- Python conversion failure and asynchronous phase-observation delay must not create authority leaks or false qualification. Tasks 1/4/5: `failedArrowDoesNotAuthorizeNextAction`, `pandasPrimitiveNullsAndTimezone`, `phaseObservationsWaitForSubmittedJobs`.

## Files and cross-task interfaces

Expand these exact prefixes before editing:

- `M` = `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `T` = `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `A` = `delta/acceptance`
- `E` = `.superpowers/sdd/2026-10-26-delta-spark-read-parity`

| Unit | Responsibility |
| --- | --- |
| `M/DeltaRangerCollectGuardExec.scala`, `DeltaRangerPhysicalProvenance.scala`, `DeltaRangerNativeScans.scala` | Task 1: supported ordinary action acceptance and capture lifetime; preserve other ticket types. |
| New `M/DeltaRangerOrdinaryRead.scala` | Task 2: pending ordinary origins, composition validation and completed-query normalization. No executable authority in pending origins. |
| `M/DeltaRangerReadRule.scala`, `DeltaRangerRawMaintenance.scala`, `DeltaRangerSparkExtension.scala` | Task 2: defer ordinary rewrite; retain typed dispatch/check/preparation boundaries. RawNormalization currently lives in RawMaintenance. |
| New `M/DeltaRangerOrdinaryAdmission.scala`; `M/DeltaRangerDecisionReceipt.scala`, `DeltaRangerNativeProvenance.scala`, `DeltaRangerNativeScans.scala` | Tasks 2–3: distinct ordinary grammar, exact bindings, optimized/empty/zero-column scan ownership. Keep shared PlanAdmission.expression unchanged. |
| New `M/DeltaRangerOrdinaryViews.scala` | Task 4: finite DataFrame-backed local-view command and retained-definition binding. |
| New `T/DeltaRangerReadParityFixture.scala`, `TestDeltaRangerReadActions.scala`, `TestDeltaRangerReadComposition.scala`, `TestDeltaRangerReadOperators.scala`, `TestDeltaRangerReadViews.scala` | Installed tests using real extension routes; reuse NativeSelectFixture and trusted-AQE barriers. |
| New `A/read_parity_actions.py`, `T/DeltaRangerReadActionProbe.scala` | Task 1: actual Python action probe with controlled JVM fixture policy transitions. Test transport only, not a companion API or payload reconstruction. |
| New `A/read_parity_live.py`, `read_parity_pyspark.py`, `read_parity_checks.py`, `test_read_parity_checks.py`, `read_parity_live.gradle`; new `T/DeltaRangerLiveReadParity.scala` | Task 5: scoped Ranger/Gravitino/MinIO setup, native Python APIs, packaged evidence and independent checker. |
| `A/cdf_read_live_package.py` | Task 5: new `read-parity` thin-app profile with full immutable source/class/dependency evidence; existing read/merge profiles unchanged. |
| New `A/iceberg_read_parity.py`, `iceberg_read_parity_app.py` | Task 5: parameterized, tracked extension of the retained Iceberg experiment; do not overwrite historical harness/evidence. |
| `T/TestDeltaRangerTrustedAqeMerge.scala`, `TestDeltaRangerTrustedAqeVariants.scala`, `TestDeltaRangerTrustedAqeRead.scala`, `DeltaRangerTrustedAqeFixture.scala`, `DeltaRangerLiveCdfMerge.scala`; `A/cdf_merge_live_checks.py` | Task 5: bounded listener wait, truthful nonadaptive label and routine evidence logging; affected checker tests. |
| `common/build.gradle.kts`, `server/build.gradle.kts`; new `A/test_build_info_git.py` | Task 5: reliable commit metadata in linked/main/detached checkouts, without server behavior changes. |

Planned private interfaces (not a public API):

```scala
// Task 1: extend existing driver-only context; default no-op for other tickets.
def beginDatasetAction(root: SparkPlan, child: SparkPlan): Unit
// Existing validateCollection(child) / validateExecution(child) remain.
// The guard invokes this from its proven stock action hook, initially resetMetrics.

// Task 2: new DeltaRangerOrdinaryRead object.
def stage(plan: LogicalPlan, spark: SparkSession,
    catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35): LogicalPlan
def pending(plan: LogicalPlan): Boolean
def check(plan: LogicalPlan, spark: SparkSession): Unit
def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan]
// None means this is not an ordinary pending route, not permission to execute.

// Task 2: move existing complete auth/rewrite block to this ReadRule method.
private[ranger] def authorizeComplete(plan: LogicalPlan): LogicalPlan
// Existing constructor supplies session and canonical catalog resolver.

// Tasks 2–3: new DeltaRangerOrdinaryAdmission object.
def query(plan: LogicalPlan): Unit
// Starts with existing ordinary scalar grammar; Task 3 adds finite operators.

// Task 4: new DeltaRangerOrdinaryViews object.
def resolve(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan]
def check(plan: LogicalPlan, spark: SparkSession): Boolean
// Only finite local DataFrame definitions/commands; false means not this route.
```

Origins are private, driver-only and immutable; exact field layout and synchronization
are builder choices. They must retain canonical per-occurrence registration,
original authored tree/lineage and session/subject/settings identity. They must not
serialize engines/credentials or mint authority from copied tags. Normalization
uses the original resolver/bindings and existing receipt factory; do not invent a
second policy engine. Update these signatures in this plan if Task 1's measured
hook requires an equivalent private seam; record the evidence and preserve the
contract rather than enforcing an unproven method name.

## Task 1: Prove native action acceptance, including Arrow

**Complete:** `6b1d2b270`; SPEC PASS / QUALITY PASS, 0 Critical, 0 Important, 1 Minor (existing diagnostic noise, carried to Task 5). Each Spark patch with Delta 3.3.0 has 28 passing full-run tests plus 1 named-caller supplemental case and 11 successful actual Python subprocesses. Ranger client 2.8.0 is loaded; live server/MinIO qualification remains Task 5. Evidence: `E/task-1-report.md`, `E/task-1-review.md`, `E/task-1-controller-verification.json`, `E/task-1/action-boundary.json`.

**Files:** Action-boundary units, action suite and Python/JVM probe in the file map;
extend `T/TestDeltaRangerCollectGuardExec.scala` for unchanged helper behavior.

**Interfaces:** Consume `DeltaRangerDecisionReceipt.validateAction`,
`DeltaRangerNativeScans.beginRead(recipe)` and `DeltaRangerExecutionContext.forRead`.
Produce `beginDatasetAction` above and `E/task-1/action-boundary.json` recording
hook, QueryExecution/root identity, actual route, caller, capture and original
result/exit references. No action token becomes public or executor authority.

- [x] Add `nativeArrowFreshAndRepeated` and `mixedActionsReauthorizeBeforePreparation` using an already admitted SQL project/filter/join. Invoke actual Python `df.toPandas()` with `spark.sql.execution.arrow.pyspark.enabled=true` and fallback=false, then repeat and mix with collect. Record loaded Pandas/PyArrow versions, Arrow JVM route and original exits. With the trusted join fixture, assert the bag equals `{(1,"MASKED","x"), (1,"MASKED","y"), (3,"MASKED","v")}` on every allowed call. After SELECT revoke, assert rejection, zero payload jobs and no initialized static-broadcast future. An enabled Arrow flag alone fails the probe.
- [x] Run the new installed/probe lane RED on Spark 3.5.4 / Delta 3.3.0. Retain the current missing-accepted-action failure or stale-action defect; dependency/setup errors are not product RED. Characterize root resetMetrics before the action callback and child preparation on both Spark patches. Use the existing admitted grammar here; limit-zero/count action coverage is added in Task 3 after its grammar exists.
- [x] Add `metricResetCannotMintAuthority`, `arrowCallerAndRegistrationDriftReject`, `inFlightArrowKeepsCapture`, `failedArrowDoesNotAuthorizeNextAction`. Assert rejection for stale SQL execution IDs, another root/session, changed subject/settings/registration and changed mask/filter; no receipt-subject impersonation on the serving thread. Hold an actual accepted payload behind a bounded barrier, refresh policy, release it and require captured rows followed by next-action denial. Exercise cancellation, preparation/conversion failure and an unrelated successful fresh read afterward. No new shared-session concurrency claim.
- [x] Implement the proven synchronous entry before child preparation in the ordinary guard/ticket. Bind the exact active SQLExecution→QueryExecution/root/session and validate once per action; nested collect and adaptive callbacks consume the same capture. A repeated action requires a fresh current binding, not merely a nonempty readContext. Expire action bindings on all completion/error paths, with callback lifetime sufficient for accepted in-flight work. Keep separately owned direct collection and native helper routes. Post-job listeners cannot be the authorization hook. Do not add operators or defer authorization yet.
- [x] Run `TestDeltaRangerReadActions`, `TestDeltaRangerCollectGuardExec` and affected `TestDeltaRangerTrustedAqeRead` cases GREEN on both Spark patches / Delta 3.3.0, using the installed command below plus the actual Python probe. Ensure nonadaptive ordinary reads retain their existing checks and that append/CDF/native helpers do not acquire ordinary action authority. Record a concrete boundary map and original results. If no stock hook satisfies the contract, stop before Task 2 and report the native-Arrow blocker; no silent fallback or fork.
- [x] Format/check affected code, obtain fresh task SPEC/QUALITY review and commit `feat(delta): validate native read actions before execution`. Deliverable: native Arrow and collect action freshness on existing admitted query shapes. Later tasks must rerun these tests after lifecycle changes.

## Task 2: Defer ordinary permissions and preserve composed intent

**Complete:** `941d29e37`; SPEC PASS / QUALITY PASS, no findings. Spark 3.5.4 / Delta 3.3.0 has 119 qualified cases in aggregate (111 unaffected passes plus 8 corrected timing cases, with exact production-class equality); Spark 3.5.6 / Delta 3.3.0 has 48 passing cases. Each patch has 11 successful actual Python subprocesses. Original failed exits remain retained. Deferred origins, complete-query authorization, finite native USING/automatic self-joins, cache exclusion and existing append compatibility are qualified. Evidence: `E/task-2-report.md`, `E/task-2-review.md`, `E/task-2-controller-verification.json`. Count and other new operators remain Task 3.

**Files:** Ordinary origin/admission, ReadRule, RawNormalization and extension units;
new composition suite and shared parity fixture; affected receipt/read-rule tests.

**Interfaces:** Produce the four `DeltaRangerOrdinaryRead` methods and
`ReadRule.authorizeComplete`; consume Task 1's action boundary. New admission starts
with current project/filter/alias/conditioned-inner-join scalar grammar. Task 3
owns count and empty authored-column admission.

- [x] Add `schemaAndExplicitProjectionHaveDifferentDemands`: build a named schema handle while only id SELECT is allowed; schema discovery succeeds without payload grants, `select("id").collect()` succeeds, bare collect and explicit `select("*")` reject. Add `explicitPredicateCannotBePruned` using a denied secret reference below an outer projection/false predicate; assert denial even if optimizer removes its values/rows. Existing metadata/registration checks remain active. Test these through native Dataset construction, not a component rewriter.
- [x] Run composition tests RED with current eager checks. Add `newCompositionUsesOriginalIntent`: plan/action a frame, change a non-idempotent supported mask such as `concat(secret, '!')`, derive a fresh child and assert one application of the current mask; the old planned QueryExecution rejects on changed decisions. Add `aliasedOccurrencesKeepOriginalDemands` for self joins, repeated aliases, toDF rename and same database/table in different catalogs; assert canonical occurrence/column demand inventories.
- [x] Implement stage/check/normalize and extract authorizeComplete from current ReadRule without changing typed CDF/MERGE/append dispatch. Stage/analysis stores original named intent with no payload grant or mask rewrite. Normalization clones and authorizes the complete original tree before cache substitution/optimizer, rewrites once and issues a receipt. A check accepts only validated pending origins; pre-CBO/physical preparation still requires an executable receipt. Leave Kyuubi and structural demand agreement strict for currently supported nonempty cases.
- [x] Add `pendingOriginCannotAuthorizePayload`, `originDriftRejectsBeforeNormalization` and `cacheAttemptsLeaveNoReusableAuthority`. Assert foreign session/subject/settings/registration, raw paths, copied/serialized/canonicalized provenance, cached/persisted/checkpointed/RDD substitutions and consumed CDF sources cannot gain authority. Probe `explain` and eager planning: planning may authorize, but later action still rechecks. Denied cache attempts must not leave consumable payload entries. Keep unrelated legitimate caches intact.
- [x] Run composition and Task 1 suites GREEN on both Spark patches / Delta 3.3.0; affected `TestDeltaRangerReadRule`, `TestDeltaRangerDecisionReceipt`, `TestDeltaRangerInstalledStaleness`, CDF ownership, builder-source rejection and append input cases on the first pair. Old eager-timing assertions may be updated only with an explicit changed-contract explanation and equivalent payload-denial coverage. Assert pending analyzed tree remains unmasked while normalized executable tree has exactly one rewrite.
- [x] Format/check, fresh SPEC/QUALITY review and commit `feat(delta): defer ordinary read authorization until normalization`. Deliverable: finite native composition with column-only grants and unchanged executable/action guarantees. The approved projection-count example completes in Task 3, not via a fake count in this task.

## Task 3: Admit finite count, distinct, ordering and limits

**Files:** OrdinaryAdmission, ReadRule extraction/preparation, DecisionReceipt binding
and prefix replay, NativeScans and NativeProvenance; new operator suite and parity
fixture. Shared `DeltaRangerPlanAdmission.expression` remains unchanged.

**Interfaces:** Extend `DeltaRangerOrdinaryAdmission.query(plan)` and existing
`ReadRule.extract(plan, spark): Seq[(Scan, Set[String])]`. Zero-column demands are
allowed only for structurally proven ordinary occurrences; CDF receipt stays strict.
No new public methods or Ranger access types.

- [ ] Seed the design's five `(id,region,email,secret)` rows in the parity fixture. Add SQL and DataFrame tests `countUsesGovernedValues`, `distinctAndOrderingUseMasks`, `emptyAndPrunedCountsRetainAuthority`. Pin these assertions (variables are observed query values):

  ```scala
  assertEquals(4L, rowCount)
  assertEquals(3L, unmaskedEmailCount)
  assertEquals(4L, constantMaskedEmailCount)
  assertEquals(0L, nullMaskedEmailCount)
  assertEquals(Seq(Row("M")), constantDistinctRows)
  assertEquals(Seq(Row(null)), nullDistinctRows)
  assertEquals(Seq(1L, 2L, 3L, 5L), maskedEmailThenIdOrder)
  assertEquals(Seq(1L, 2L), maskedOrderedLimitTwo)
  assertEquals(Seq(Row(0L)), countAfterFalsePredicate)
  ```

  Test COUNT(*)/COUNT(1)/COUNT(top-level column), multiple aliased count outputs,
  SQL Distinct and full-output Deduplicate, ASC/DESC/NULLS FIRST/LAST, sort-only
  columns and literal limits including zero. Unordered limits assert only size
  and visible-row membership. Run RED before widening admission.
- [ ] Add `countRetainsOriginalDemand`: id-only principal succeeds at `spark.table(t).select("id").count()` and denies explicit-star then count; bare count/literal projection uses table-wide SELECT. Record actual Ranger wildcard/deny decisions rather than substituting all columns. Constant/NULL masked email still needs email SELECT; policy-only region does not invent authored region access, but an authored region predicate does. Joins/empty/eliminated occurrences keep their original registration and demand.
- [ ] Implement finite ordinary grammar and exact binding for global count with top-level column or literal-one inputs, full-output DISTINCT/Deduplicate, SortOrder fields and literal GlobalLimit/LocalLimit. Separate generated optimizer products from authored permissions. Extend recursive no-skipping preparation and prefix replay together. For zero-width or fully eliminated scans, retain a bound authorized derivation; COUNT(empty) is one zero row, not an empty result. Derive structural per-occurrence demand explicitly when Kyuubi omits a columnless input; retain checks for unknown/missing/nonmatching column-bearing inputs and every canonical registration.
- [ ] Add `unsupportedAggregateAndDedupNeighborsReject`: grouped/count-distinct/SUM/window/FILTER aggregates, arbitrary count argument expressions, subset or zero-column dedup, unsupported sort expressions and OFFSET remain rejected. Test nullable/nonnullable email before and after NULL masking, aliases, nested projections and distinct→count so null folding/pruning cannot use raw metadata. Re-run Task 1 action-denial/freshness cases with limit(0), empty/false query and count, including Arrow conversion of an admitted aggregate.
- [ ] Run operator, composition and action suites GREEN on Spark 3.5.4 with both Delta pins and Spark 3.5.6 / Delta 3.3.0; final Task 5 covers all four on the frozen candidate. Run affected no-skipping/native-snapshot/physical-admission cases. Verify ordinary grammar additions do not admit the same transforms into CDF/MERGE sources or raw metadata routes. Record actual aggregate/top-N/limit/scan plans; exchange-free counts do not count as AQE evidence.
- [ ] Format/check, fresh SPEC/QUALITY review and commit `feat(delta): support governed count distinct order and limit`. Deliverable: the approved native projection-count behavior and exact policy-before-operator semantics, with no general analytics allowlist.

## Task 4: Local DataFrame views, show and Pandas conversion

**Files:** New OrdinaryViews and view suite, ordinary dispatcher/admission/binding;
extend action/operator suites and `A/read_parity_actions.py`. No public Python facade
change is needed: exercise native DataFrame methods.

**Interfaces:** Produce `DeltaRangerOrdinaryViews.resolve/check`; consume ordinary
stage/check/normalize. View identity is a local definition/output binding, while
permissions always resolve to canonical base-table resources. Preserve Task 1
native Arrow boundary and no-double-authorization behavior.

- [ ] Add `localViewPreservesOriginalDemands` and `viewReplacementAndDropKeepBoundDefinitions`. Create a local DataFrame view from pending named intent with id-only grants, consume projected id and assert success; consume secret or an explicitly secret-projecting definition and assert denial. Native-characterize replacement/drop: fresh resolution sees replacement or missing view; already analyzed queries retain the old definition but recheck base registration/current policy. Assert no Ranger resource is fabricated from the view alias. Run RED before admitting view nodes/commands.
- [ ] Implement only local DataFrame-backed create/createOrReplace/drop and resolved view consumption, retaining pending original definition and output lineage. Verify original SQL text is absent for the accepted definition; do not admit generic commands or SQL CREATE VIEW. Enforce same session/subject/settings/registration and reject global/permanent/text-backed/foreign/cached/checkpointed/CDF definitions. Test view aliases/column renames and planned/actioned source frames without double masks.
- [ ] Add `showFormatsGovernedValues` using actual Python `show(2, truncate=False)` on ordered id/email with constant mask; assert rows `(1,M)` and `(2,M)`, continuation indication, no hidden/raw values and denial for a selected/predicate-only forbidden column. Implement only the bounded ToPrettyString display projection above the governed values, with exact output order/timezone/limit binding; keep policy/CDF/MERGE expression grammar unchanged.
- [ ] Add `pandasPrimitiveNullsAndTimezone`: a registered fixture with long, nullable string, boolean, double, decimal, date and timestamp values, nullable counterparts and UTC session timezone. Compare native Arrow-off and required-on outputs with governed collect after type-aware normalization; retain dtype and Pandas/PyArrow versions separately. Pin a non-null date `2026-10-26`, timestamp `2026-10-26 12:34:56.123456`, decimal `12.30` (precision 10, scale 2), boolean true, double 1.5 and their nulls. Test mask/filter effects, conversion failure, repeated/mixed actions and unchanged session settings. No conversion through locally reconstructed DataFrames.
- [ ] Run view/action/operator/composition tests and actual Python probes GREEN on both Spark patches / Delta 3.3.0. Probe forbidden local cache/global view/SQL-view neighbors and existing CDF/builder source rejection. Document exact primitive conversion outcomes rather than claiming all Pandas types. Type handling must stay stock Spark; fix only governed-plan integration failures.
- [ ] Format/check, fresh SPEC/QUALITY review and commit `feat(delta): support local views and native display conversions`. Deliverable: the full bounded native read surface; final packaged live qualification remains Task 5.

## Task 5: Reliable evidence and final Delta/Iceberg qualification

**Files:** Live/Python/checker/package/comparator and diagnostic/build-metadata units
in the file map; update `docs/delta-spark-parity.md`,
`docs/delta-spark-execution-assessment.md`, `docs/iceberg-spark-ranger-aqe-baseline.md`,
`A/README.md` and this plan. Preserve historical results as historical.

**Interfaces:**

- `read_parity_live.py --runtime <manifest> --inputs <private-inputs> --out <fresh-dir>` runs one pair. `read_parity_pyspark.py <request-path>` invokes actual native APIs.
- `read_parity_checks.verify_result(result: dict) -> None` rejects missing/incorrect semantics, policy or artifact evidence.
- Extend `cdf_read_live_package.app_profile(app)` and CLI with `read-parity`; its Gradle task is `readParityLiveThinJar`, output property `readParityLiveAppOut`, thin class `DeltaRangerLiveReadParity`. Keep read/merge behavior intact and retain supplemental dependencies for this profile.
- `iceberg_read_parity.py --spark <pin> --catalog native|gravitino --inputs <private-inputs> --out <fresh-dir>` runs one comparator cell using `iceberg_read_parity_app.py`. Reuse the retained experiment's setup/identity/resource approach, parameterize paths and capture new results separately.
- Result schema: `schemaVersion=1`, `spark`, `delta` (Delta cells), `engine`, `catalogRoute`, `runtimeManifest`, `rangerServerVersion`, `rangerClientVersion`, `cases`, `processExits`, and artifact path/hash references. Each case records `name`, `api`, `outcome` (allowed/denied), expected and observed type-normalized rows/order, original demands/Ranger requests where applicable, policy snapshot, analyzed/final-plan refs and actual action route. Pandas cases record enabled/fallback/observed Arrow plus versions/dtypes. Missing observations are failures or explicit unqualified comparator differences, never invented success.

- [ ] Fix diagnostic follow-ups with focused tests: bounded job-ID-tied listener draining in installed MERGE/variant tests; distinguish a nonadaptive exchange-bearing plan from an exchange-free one and retain backward compatibility with historical evidence; move routine plan/origin WARNs to structured/info output, preserving denial/fault stacks. `phaseObservationsWaitForSubmittedJobs` must cover delayed event delivery and a bounded timeout. Extend existing trusted-AQE checker tests for the diagnostic label.
- [ ] Repair commit metadata lookup in common/server build scripts with Git's repository-aware `rev-parse HEAD` (no secret-bearing shell interpolation), preserving a documented unavailable-Git fallback. `test_build_info_git.py` exercises real generated metadata in temporary main, linked-worktree and detached checkouts, including packed refs, against `git rev-parse HEAD`. Run focused build metadata tasks, not a server behavior rewrite. These changed build scripts invalidate shared-source equality in old package manifests: rebuild the shared assembly and create a truthful fresh base; never bypass `shared_sources` validation or rewrite old receipts.
- [ ] Write checker RED cases for Arrow flag without actual Arrow, fallback=true, wrong mask/count/distinct/null-order result, missing original demand, missing/failed child exit, wrong Spark/Delta/Ranger/provider, altered supplemental JAR, view alias used as Ranger resource and count metadata-only execution mislabeled as adaptive payload. Implement the thin app and supervisor using existing fixture transport/deployed-version/registration verification helpers.
  For all new Scala public classes/methods, follow the module's documentation conventions;
  all new Java public/protected members require Javadoc. Keep setup/application/metadata identities separate, provision only isolated fixture policies and confirm Ranger refresh. Add actual registration/restart evidence; label fixture-authored policies accurately instead of claiming grant propagation.
- [ ] Implement real Python operations and evidence collection for all approved cases, including id-only composition, explicit-star denial, table-wide count, policy-only versus authored region, email masks/denials, sorted limits, show, both Pandas routes, local views and retained-plan refresh/replacement. Include an exchange-bearing ordinary join/sort to demonstrate real final AQE per pair, independently of metadata-only count. Record actual Ranger wildcard/deny outcomes. Run checker unit tests and targeted request/package-profile regressions GREEN before building the full matrix.
- [ ] Extend the Iceberg comparator with the same finite operations/fixtures and retained-frame/view/Arrow probes. Use Iceberg 1.11.0/Kyuubi 1.10.2 and actual Ranger 2.8.0 on both Spark patches, native and Gravitino wrapper (four cells). Capture actual permission requests where its rewritten-plan extraction differs; label measured differences rather than changing Delta's original-demand rules. Preserve unique database scopes because the Hive service mapping lacks a catalog dimension. Native seeding/metadata-auth-off results do not prove Gravitino grant propagation.
- [ ] Freeze the final code candidate, inventory disk/services and run heavy builds serially. Run all new installed suites on all four Spark/Delta pairs plus the affected read/CDF/MERGE/append regression selectors in the coverage map. Reuse prior cases only where final source/class/dependency inputs match; do not rerun closed unrelated live MERGE phases merely for cleaner logs. Inspect every selected XML count and original exit; skips/zero-test/setup failures are not passes.
- [ ] Build four immutable `read-parity` packages and run four live Delta cells plus the four comparator cells. Verify server/client Ranger 2.8.0, exact loaded providers, original Python/native/supervisor exits, policy versions/download/application, canonical registration, native API routes and exact values. Retain all runtime and supplemental JARs/source inventories, including original failed attempts. Produce `E/final-evidence-index.json` and a support table separating Delta assertions, measured comparator differences and deployment exclusions.
- [ ] Update support/setup docs and this plan to actual observed results; independently check every final index reference/hash, source-to-class/package identity and documentation-only delta. Obtain fresh integrated SPEC/QUALITY review. Resolve findings with affected reruns only, commit the completed milestone, verify intended remote/branch and push under standing authorization. Report the actual push result and remaining append/catalog-demo work. Do not mark native Arrow or any other selected surface complete if its gate remains open.

## Commands and evidence discipline

Use fresh `E/task-N/<spark>-<delta>/attempt-N/` directories, with original argv,
source/runtime hashes, exits, stdout/stderr and XML. Memory tooling is unavailable;
retain findings locally. No runtime tests were run to write this plan.

Installed command template (replace suite, versions and fresh output directory):

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_merge_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerReadActions' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PcdfLiveXml="$PWD/.superpowers/sdd/2026-10-26-delta-spark-read-parity/task-1/3.5.4-3.3.0/attempt-1/xml" \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

GREEN requires exit 0, intended nonzero test count, zero failures/errors/skips and
verified requested providers. RED must be the specified behavior failure. Use one
4-GiB test JVM as configured by the init script, never concurrent heavy builds.
The Task 1 installed suite launches `read_parity_actions.py` using the matching
local pinned Python runtime and its test JVM bridge; record subprocess exit/output.
The test-only `DeltaRangerReadActionProbe` exposes `session(): SparkSession`,
`setSelectAllowed(allowed: Boolean): Unit`, `setMask(expression: String): Unit`,
`setFilter(expression: String): Unit` and `observations(): String` (JSON). It uses
the installed fixture engine; real downloaded-policy propagation is Task 5.
The suite owns and closes the loopback gateway/subprocess with bounded waits.
The bridge exposes session/policy controls and diagnostics, not returned payload rows. PySpark attaches to that session and calls native APIs itself.

With the same JDK/pins, run module `spotlessApply`, then `spotlessCheck` and
`git diff --check`; inspect formatting for unrelated edits. New public/protected
Java members need Javadoc and applicable checks. Markdown-only plan/status changes
do not justify runtime reruns.

```bash
python -m unittest discover -s delta/acceptance -p 'test_read_parity_checks.py' -v
python -m unittest discover -s delta/acceptance -p 'test_cdf_merge*checks.py' -v
python -m unittest discover -s delta/acceptance -p 'test_build_info_git.py' -v
```

Task 5 shared assembly after build-script changes uses the retained scoped assembler:

```bash
python .superpowers/sdd/2026-10-14-delta-native-governed-append/public-grants-live-harness/prepare.py \
  --inputs <verified-private-inputs> --out <fresh-shared-assembly> \
  --spark 3.5.4 --delta 3.3.0
python delta/acceptance/cdf_read_live_package.py --app read-parity \
  --base <fresh-verified-base-manifest> --out <fresh-package-dir> \
  --spark <pin> --delta <pin>
python delta/acceptance/read_parity_live.py --runtime <fresh-manifest> \
  --inputs <verified-private-inputs> --out <fresh-live-dir>
python delta/acceptance/iceberg_read_parity.py --spark <pin> \
  --catalog <native-or-gravitino> --inputs <verified-private-inputs> --out <fresh-comparator-dir>
```

Resolve placeholders from verified retained manifests; inspect assembler output
rather than assuming a filename. The new CLI commands become available in Task 5.
Keep current assembly source checks strict. MinIO/Ranger test services may be
restarted within existing authorization; do not use or modify catalog-demo.

## Self-review and handoff

Coverage: native action/captured policy/Arrow feasibility → Task 1; metadata-only
origins, exact authored demands, composition and cache rejection → Task 2; finite
operators, table-wide empty demand and optimizer products → Task 3; local-view
lifecycle, show and primitive Python conversion → Task 4; diagnostic repairs,
version/live/comparator matrices, artifact proof, docs/review/push → Task 5.
All five Review Focus cases have named tests. Private interfaces are defined once
and consumed consistently. Existing broader-read/cross-route contract tests are
retained or explicitly updated for the approved authorization timing.

One implementer task and fresh review at a time; no concurrent edits to shared
receipt/normalization code. The task commits are intermediate checkpoints, not
milestone closure. The user already chose subagent-driven execution. Review this
plan before implementation; after confirmation, proceed through task/review gates
without asking for the same execution preference or routine permission again.
