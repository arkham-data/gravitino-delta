# Governed Delta AQE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. The user already selected this method. Complete each task and its independent SPEC/QUALITY review before the next task. Steps use checkbox syntax for tracking.

**Goal:** Qualify genuine adaptive payload execution for the existing named Delta read, bounded CDF and SQL/JVM/classic-PySpark MERGE contracts.

**Architecture:** Add companion-private adaptive execution ownership around stock Spark stages, preserving existing logical authorization and native Delta algorithms. Admit only the approved coalescing-first profile, with checks before stage execution, consumption and publication. Start with a read feasibility gate; extend the proven boundary to CDF and MERGE only afterward.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21, classic PySpark and MinIO/S3.

**Spec:** [Approved governed AQE design](../specs/2026-10-25-delta-governed-aqe-design.md). User approved on 2026-10-25 with catalog-demo deferred while they verify missing operations. Planning baseline `05fb47eb9`; qualified product baseline `5a2e3676c`. Worktree `.worktrees/delta-spark-feasibility`, branch `codex/delta-spark-feasibility`.

**Status:** Approved for subagent-driven task-by-task execution on 2026-10-25. Task 1 is blocked at its feasibility gate; no governed AQE support has been implemented or qualified. Catalog-demo integration and additional logical operations are not tasks in this plan.

## Task 1 feasibility outcome — 2026-10-25

Independent review approved the bounded diagnostic evidence, not Task 1 completion.
Both Spark pins with Delta 3.3.0 ran the installed hook probe and existing rejection /
nonadaptive controls successfully. A second installed probe on each pin rejected a
test-only internal-guard candidate: a copied stage can imitate its monitor/caller
checks, and a cached copy can borrow the same map-stage future without another
child-guard check. That future yields MapOutputStatistics; no foreign payload-row
disclosure was demonstrated. Original failed hypotheses, named governed-read RED,
commands, source snapshots, XML and exits are retained under
`.superpowers/sdd/2026-10-25-delta-governed-aqe/task-1/`.

No examined stock extension callback exposes the new exact stage before materialize
method entry. Independent review clarified that an internal guard before substantive
payload work could still satisfy the design, but the tested guard cannot authenticate
the invoking stage. This is a concrete limitation of the examined ownership approach,
not proof that all governed AQE designs are impossible.

Production read/CDF/MERGE AQE rejection remains unchanged. Tasks 2–5 have not started.
Broadcast/cancellation/async policy lifecycle proof is deferred because exact invocation
identity failed first. The next step requires revisiting the ownership design; do not
remove the guard, claim support, start catalog-demo, or run a qualification matrix for
an unsupported implementation. Reports: `task-1-report.md`,
`task-1-feasibility-review.md`, `task-1/round-1/investigation.md`, and
`task-1-round1-review.md` in this plan's evidence workspace.

## Global Constraints

- Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21 and S3-compatible storage (MinIO locally).
- No Gravitino core/API/server/client changes, Spark/Delta fork, second metastore, path fallback, policy migration or source-payload collection/reconstruction.
- Preserve the platform's one-writing-job-per-dataset constraint and existing native conversion/schema/protocol/partition profiles.
- Preserve masks, row filters, original column/expression demands, hidden rows, omitted-column copy-forward and native ambiguity/constraint behavior. No reverse masking, deduplication, unmasked-key requirement or postimage write constraint.
- Public JVM/Python builder signatures and API version 1 remain unchanged. Only existing fresh logical grammar is admitted; count/distinct/sort/limit/window/reused-frame expansion is excluded.
- Real completed adaptive payload plans and owned stages are mandatory. AQE=true, metadata-only AQE, forceApply-only evidence or manual nonadaptive payload lowering is insufficient.
- Stage internals freeze at start. Cross-action/phase/attempt reuse, executable subqueries, foreign hooks/evaluators and unsupported adaptive rewrites reject.
- No silent configuration changes, global ownership transfer, executor permits, setup-principal impersonation or setting SparkSession.active to bypass checks.
- Naturally exchange-free CDF may execute without adaptive stages; report that accurately. New MERGE support does not broaden append/CDF-property/maintenance support.
- Local commits only; preserve original evidence and the worktree. Catalog-demo files, services, policies and deployment stay deferred until the user resumes integration.
- Use English, repository formatting, tests for new logic and documented public/protected APIs. No OpenAPI changes are planned.

### Exact opt-in profile

Validate effective values, then retain existing full settings capture/drift checks. Configure before creating sources/handles. AQE=false retains the existing route without these additional AQE-only restrictions.

| Setting | Required value |
| --- | --- |
| spark.sql.adaptive.enabled | true |
| spark.sql.adaptive.coalescePartitions.enabled | true |
| spark.sql.adaptive.autoBroadcastJoinThreshold | -1 |
| spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold | 0 |
| spark.sql.adaptive.skewJoin.enabled | false |
| spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled | false |
| spark.sql.adaptive.localShuffleReader.enabled | false |
| spark.sql.exchange.reuse | false |
| spark.sql.execution.reuseSubquery | false |
| spark.sql.optimizer.dynamicPartitionPruning.enabled | false |
| spark.sql.optimizer.runtime.bloomFilter.enabled | false |
| spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled | false |
| spark.sql.adaptive.optimizer.excludedRules | absent/empty |
| spark.sql.optimizer.excludedRules | absent/empty |
| spark.sql.adaptive.customCostEvaluatorClass | absent |

Do not require redundant explicit strings for upstream defaults. Preserve absent-versus-explicit differences in later settings drift checks. Static broadcast threshold, shuffle partition count and coalescing size are caller choices subject to actual plan admission.

## Review Focus

1. A cancellation races an asynchronous stage completion: the callback must not reopen closed authority or consume its output (Task 1 `cancelledOwnerRejectsLateCompletion`).
2. One live phase references a stage twice: idempotent validation must work, but a second action/phase must reject the same stage (Task 1 `sameOwnerReferenceIsNotCrossQueryReuse`).
3. An empty owned shuffle replaces a subtree: keep original demands and output identities; a fabricated empty relation must reject (Task 1 `ownedEmptyStagePreservesDemands`).
4. A native MERGE retry reuses a stage ID or equal plan from a retired attempt: identity must not transfer and no extra commit may occur (Task 4 `retiredAttemptCannotLendStage`).
5. A commit succeeds but cache/registration reconciliation fails: retain known-commit recovery and prevent blind replay/checkpoint advancement (Task 4 `adaptiveCommittedButUnreconciled`).

## File and interface map

All path abbreviations below are exact prefixes:

- `M` = `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `T` = `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `A` = `delta/acceptance`
- `E` = `.superpowers/sdd/2026-10-25-delta-governed-aqe`

Create `M/DeltaRangerAdaptiveProfile.scala` for configuration qualification, `M/DeltaRangerAdaptiveExecution.scala` for driver-local owner/stage lifecycle, and `M/DeltaRangerAdaptiveRules.scala` for exact stock-hook adapters. Keep physical transition comparison in the adaptive execution unit initially; split into a private companion file only if actual implementation size warrants it. Do not restructure unrelated classes.

Existing seams: `DeltaRangerReadRule.prepare` and `PhysicalProvenance.UserTicket` reject user AQE; `PhysicalPlanning.lower` filters InsertAdaptiveSparkPlan; `PhysicalPlanning.qualify` rejects every adaptive hook. `CdfRead.normalize` and the `MergeReceipt` constructor independently reject AQE. CDF receipts/scans and MERGE source/native/write owners seal fixed physical shapes and RDD recipes; several require the creating thread and active ThreadLocal. Preserve those nonadaptive paths.

Planned private contract, all inside package `org.apache.gravitino.spark.connector.delta.ranger`:

```scala
DeltaRangerAdaptiveProfile.requireSupported(spark: SparkSession): Unit
DeltaRangerAdaptiveExecution.forRead(
    spark: SparkSession, logical: LogicalPlan,
    prepared: DeltaRangerNativeScans.Prepared): DeltaRangerAdaptiveExecution.Owner
// Added by Task 2:
DeltaRangerAdaptiveExecution.forCdf(
    spark: SparkSession, receipt: DeltaRangerCdfReadReceipt): DeltaRangerAdaptiveExecution.Owner
// Added by Task 3:
DeltaRangerAdaptiveExecution.forSource(
    attempt: DeltaRangerMergeSource.Attempt): DeltaRangerAdaptiveExecution.Owner
DeltaRangerAdaptiveExecution.forNative(
    phase: DeltaRangerMergeNative): DeltaRangerAdaptiveExecution.Owner
DeltaRangerAdaptiveExecution.forWrite(
    phase: DeltaRangerMergeWrite): DeltaRangerAdaptiveExecution.Owner
// Owner has a private constructor; factories verify the exact live authority.
Owner.bind(root: SparkPlan): Unit
Owner.verifyCurrent(root: SparkPlan): Unit
Owner.verifyCompleted(root: SparkPlan): Unit
Owner.close(): Unit
DeltaRangerPhysicalPlanning.adaptive(
    spark: SparkSession, logical: LogicalPlan,
    owner: DeltaRangerAdaptiveExecution.Owner): SparkPlan
```

These are internal implementation names, not a new public API. Factories may not accept arbitrary validation lambdas, principal strings or caller-created permits. Binding a root is not stage authorization: permits are privately issued for exact exchanges/stage instances, with independently checked authorization at each boundary. `verifyCompleted` validates terminal native stage state and final output provenance before publication; it never forces a new action. `close` is idempotent and only closes this owner's resources. Naturally nonadaptive roots remain explicitly classified.

Read-only inspection of pinned Spark 3.5.6 bytecode found that exchange-child stage optimization and columnar/codegen hooks run before construction of the QueryStageExec; final materialize is called directly by private adaptive execution code. Matching hook signatures in 3.5.4 do not prove matching lifecycle coverage. Task 1 must prove exact invoking-stage authorization before substantive materialization or payload work, not equate a preparation hook with that boundary. Independent review clarified that entering the stock materialize method (which initially logs/delegates) is not itself payload execution. A test-only internal guard may be investigated, but it must distinguish copied stages sharing an exchange and cover early preparation/cancellation before any product gate changes.

Task 1 determines the precise stock hooks and stage guard arrangement from installed evidence; its reviewed hook map freezes that seam for Tasks 2–4. Do not treat a planning-time method name as proof that a Spark hook observes materialization. A required boundary unavailable without a fork/authority relaxation is a failed feasibility gate, not permission to weaken the design.

## Task 1: Prove governed adaptive ordinary reads

**Files:** Create the three `M/DeltaRangerAdaptive*.scala` units above and `T/TestDeltaRangerAdaptiveRead.scala`, `T/TestDeltaRangerAdaptiveOwnership.scala`, `T/DeltaRangerAdaptiveFixture.scala`. Modify `M/DeltaRangerSparkExtension.scala`, `DeltaRangerPhysicalPlanning.scala`, `DeltaRangerPhysicalProvenance.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerCollectGuardExec.scala`, `DeltaRangerDecisionReceipt.scala` and `DeltaRangerNativeScans.scala` only at the relevant authority/execution seams. Adapt existing AQE rejection assertions in `T/TestDeltaRangerPhysicalAdmission.scala` to distinguish unsupported profiles from the new bounded read profile.

**Interfaces:** Consume existing `Prepared.verifyExecutable`, receipt authorization and `UserTicket` collection checks. Produce the Task 1 private contract above and a reviewed `E/task-1/hook-map.md` covering both Spark pins' callback order, thread/session behavior and preexecution/final-validation boundaries. CDF/MERGE guards remain closed.

- [ ] Inspect pinned Spark 3.5.4/3.5.6 definitions for the profile keys and `SparkSessionExtensions` adaptive hooks, `AdaptiveSparkPlanExec`, `QueryStageExec.materialize`, exchange preparation and result consumption. Record exact source/artifact hashes and callback ordering in the hook map. Candidate seams are query-post-planner, query-stage preparation and query-stage optimizer rules plus connector-owned execution guards. QueryExecutionListener is observational, never a preexecution security gate.
- [ ] Add `freshNamedInnerJoinActuallyCoalesces` using fresh named tables, Ranger filter/mask decisions and the full opt-in profile before analysis. Choose enough input partitions to produce stock shuffle stages and genuine coalescing without forceApply. Capture a payload witness distinct from native metadata. Assert exact authorized row bag and:
  ```scala
  assertTrue(adaptive.isFinalPlan)
  assertTrue(completedPayloadShuffleStages.nonEmpty)
  assertTrue(coalescedPayloadReads.nonEmpty)
  assertEquals(settingsBeforeAction, settingsAfterAction)
  ```
  These variables are observations from `DeltaRangerAdaptiveFixture`, not tags supplied by the query. Run this test RED on Spark 3.5.4/Delta 3.3.0; preserve the existing AQE rejection and original exit.
- [ ] Implement `requireSupported(spark)` and table-driven tests for every profile value, absent/default equivalence and captured-setting drift. Add typed factories/Owner, exact input/initial/current/final transition checks, private stage permits and cleanup. Implement `PhysicalPlanning.adaptive` with stock adaptive insertion; keep existing `lower` for AQE=false/other qualified routes. Allowlist only exact companion-owned adaptive hook instances and retain foreign-hook rejection before invocation.
- [ ] Integrate ordinary receipt/scan checks with explicit caller versus owned-internal-callback validation. Reauthorize the captured subject/groups against current policy/registration state on the bound session without changing ambient session/subject. Keep current-context checks at caller boundaries. Seal exact scans, expressions, demands, output/partition identities and running-stage internals. Validate before materialization/consumption and after action completion before returning collected rows; close authority on success, failure or cancellation. Test actual async callbacks, not just synchronous calls to Owner methods.
- [ ] Add `staticBroadcastIsOwned`, `cancelledOwnerRejectsLateCompletion`, `sameOwnerReferenceIsNotCrossQueryReuse` and `ownedEmptyStagePreservesDemands`. Assert completed owned broadcast provenance; late callback rejection and zero new tasks; successful repeated reference within its live phase but rejection from a fresh query; exact empty output identity plus unchanged original SELECT demands. Reject fabricated empty results even with matching schema/statistics.
- [ ] Add `foreignOrCopiedStageRejects`, `revocationBeforeNextStagePreventsTasks` and `unsupportedTransitionRejects`. Exercise missing/mixed/copied tags, equal stage IDs, serialized/canonicalized references, wrong session/principal/settings, swapped scans/expressions/manifests, gapped/overlapping/partial coalescing, local/skew readers and adaptive join conversion. Assert rejection at the guarded boundary, no unauthorized next-stage tasks and no output publication. Verify a foreign callback with the same principal cannot acquire authority.
- [ ] Run Task 1 tests GREEN on Spark 3.5.4/Delta 3.3.0, then the same hook-sensitive tests on Spark 3.5.6/Delta 3.3.0. Run affected AQE=false native SELECT/physical binding/admission regressions. Require genuine payload stages, no skips, formatting and original provider/version evidence. An unavailable preexecution boundary stops this task with the exact failed trace and existing rejection retained; do not start Task 2.
- [ ] Obtain independent SPEC/QUALITY review of implementation and hook evidence, commit locally as `feat(delta): admit bounded governed adaptive reads`, and freeze the private contract/hook map. Record that CDF/MERGE AQE is still unsupported and full four-pair qualification remains pending.

## Task 2: Preserve standalone bounded CDF in the adaptive session

**Files:** Modify `M/DeltaRangerAdaptiveExecution.scala`, `DeltaRangerCdfRead.scala`, `DeltaRangerCdfReadReceipt.scala`, `DeltaRangerCdfReadScans.scala`. Create `T/TestDeltaRangerAdaptiveCdf.scala`; extend the Task 1 and existing CDF fixtures only as necessary.

**Interfaces:** Consume Task 1 Owner and reviewed hooks. Add the `forCdf` factory above. Keep the MERGE receipt AQE gate closed until Task 3. Native MERGE source materialization belongs to that task because the existing receipt gate prevents constructing its authority in an adaptive session; do not add a test bypass or temporarily toggle configuration.

- [ ] Add `freshBoundedCdfInAdaptiveSession`, using profile-before-construction and exact inclusive source versions. Assert exact filtered/masked CDF rows and original demands, unchanged bounds/settings and owned native file provenance. Preserve naturally exchange-free classification without synthetic shuffles. Run RED before changing the CDF gate.
- [ ] Implement CDF Owner binding in the receipt/scan lifecycle, retaining history/file checks, output occurrence, logical grammar and inert serialized carriers. Replace only the fixed nonadaptive physical-recipe checks when AQE actually applies. Qualify the profile before CDF admission; keep unsupported cache/derived/actioned sources rejected. Internal callbacks use exact stage permits, never a broadly active CDF or MERGE ThreadLocal.
- [ ] Add `cdfRevocationBeforeConsumption`, `actionedOrCachedCdfStillRejects` and `cdfCloseRetiresCallbacks`. Assert no unauthorized next work or output publication; inert late callbacks; no user-created LogicalRDD/cache acceptance. If the selected CDF route has no stages, enforce/test its action and consumption boundaries without pretending it exercises a stage callback. Keep the original nonadaptive one-start/shape/RDD checks and test them separately.
- [ ] Run Task 2 suites plus affected `TestDeltaRangerInstalledCdfRead`, `TestDeltaRangerCdfReadOwnership` and `TestDeltaRangerMergeSource` tests on Spark 3.5.4 with both Delta pins. Verify inherited Task 1 source/dependencies before reusing its results; rerun changed shared seams only. Review independently and commit locally as `feat(delta): admit bounded CDF in adaptive sessions`.

## Task 3: Execute native MERGE phases with adaptive ownership

**Files:** Modify `M/DeltaRangerAdaptiveExecution.scala`, `DeltaRangerMergeReceipt.scala`, `DeltaRangerMergeSource.scala`, `DeltaRangerMergeCommand.scala`, `DeltaRangerMergeNative.scala`, `DeltaRangerMergeWrite.scala`, `DeltaRangerMergeResult.scala`, `DeltaRangerMergeCommit.scala`, `DeltaRangerRawMaintenance.scala` and `DeltaRangerCdfCacheMaintenance.scala` only where required. Create `T/TestDeltaRangerAdaptiveMerge.scala` and `T/TestDeltaRangerAdaptiveSource.scala`; extend partitioned/native fixtures and physical dispatch only at the changed seam.

**Interfaces:** Consume Tasks 1–2 factories and hooks; add `forSource`/`forNative`/`forWrite`. Keep existing command, SourcePermit, native commit/result and public builder interfaces. Each touched-file/rewrite/metrics/output phase has its own exact owner and transaction/attempt binding; one phase's permit cannot authorize another.

- [ ] Add `namedMergeHasAdaptivePayloadPhases` using separate equivalent SQL and JVM API fixtures, fresh profile-configured handles and actual native source/touched-file/rewrite/output execution. Assert exact row/CDF/action bags, native commit version and original column demands. Require an actual final adaptive payload plan and completed exchange stages, separately labeling source and metadata phases. Run RED through the current public MERGE AQE gate.
- [ ] Add `nativeSourceCheckpointRetainsOwnedResult`, `foreignCheckpointCannotBorrowSource` and `sourceCloseRetiresCallbacks`. Implement `forSource(attempt)` and bind native materialization to the exact attempt. Preserve the existing proof of Dataset.localCheckpoint copying the issued source result, dependencies and output identities. Assert a foreign equal-schema RDD/checkpoint cannot substitute, source closure retires late callbacks and the bounded interval stays fixed. Run RED with the other Task 3 tests before enabling the complete adaptive MERGE route; test no source-only production bypass.
- [ ] Integrate native phase factories, exact current/final plan verification and scoped async checks. Preserve original action/column bindings, helper scan/file selection and masked-versus-raw assignment rules. Test pre-materialization hook coverage for each native phase before replacing the MERGE gate with profile validation; disable no guard merely to make the positive case run. Keep native materialization and row algorithms unchanged.
- [ ] Add `partitionedAdaptiveMergePreservesHiddenRows`, `sameRegistrationAdaptiveWriteback` and `nativeEmptyAmbiguousAndConstraintOutcomes`. Cover partition moves with escaped/null values, hidden rows in touched files, untouched files, omitted versus explicit masked assignments, masked merge keys, literal assignments without invented SELECT and ordered clauses. Compare exact native rows/CDF/actions and consumer checkpoint. Empty/false branches retain original permissions; ambiguity/constraint failure does not commit.
- [ ] Audit raw maintenance/cache invalidation before enabling public AQE MERGE. Add `unsupportedCacheRejectsBeforeMutation`; assert no new committed version, write tasks or checkpoint for a known unsupported condition. Permit only necessary separately proven metadata/publication routes; do not toggle AQE or admit cached application sources. Keep unrelated append/CDF-property AQE rejection intact.
- [ ] Add `revokeBeforeAdaptiveWriteAndCommit` and `phaseCannotBorrowForeignOutput`; assert source/target current-policy checks before native writes and before commit, original demands retained, no unauthorized commit/checkpoint, owned cleanup and unchanged session settings after stock initialization. Verify existing row-filter read semantics allow authorized postimages outside visibility.
- [ ] Run Task 3 suites and affected installed MERGE/partition/ownership/policy cases on Spark 3.5.4 with both Delta pins. Include targeted AQE=false regressions after shared-path edits. Review independently, commit locally as `feat(delta): execute governed adaptive MERGE phases`, and retain retry/recovery qualification as pending Task 4.

## Task 4: Qualify retries, revocation and publication recovery

**Files:** Create `T/TestDeltaRangerAdaptiveRecovery.scala`. Extend `T/TestDeltaRangerAdaptiveOwnership.scala`, `TestDeltaRangerAdaptiveMerge.scala` and existing native retry/journal fixtures. Production fixes stay within the owners/receipts/commit/journal paths implicated by a failing test; do not redesign the journal or add wrapper retries.

**Interfaces:** Consume Task 3 native phases and existing known-commit reconciliation. Produce `E/task-4/scenario-evidence.json`, mapping scenario, selector, runtime pair, candidate sources, original result and any explicitly inherited unchanged dependency to Task 5.

- [ ] Add `retiredAttemptCannotLendStage`, `adaptiveBlockLossRetry` and `adaptiveRetryExhaustion`. Trigger existing native block-loss hooks; assert fresh attempts/permits and exact source interval, no retired checkpoint/exchange/broadcast reuse, allowed stock task retry within the same live stage, one commit after successful retry and zero after exhaustion. Equal stage IDs or structurally equal plans do not transfer authority.
- [ ] Add `policyChangesBetweenAdaptiveAttempts`, `adaptiveAcknowledgementLossReconcilesOnce` and `adaptiveCommittedButUnreconciled`. Assert current source/target authority on retry; durable exact intent/commit identity; no second write on authorized known-commit reconciliation; no checkpoint advancement until synchronous success; no claim of rollback after a committed-but-unreconciled result.
- [ ] Add `revokedDuplicateChangedIntentAndUnknownOutcomeDoNotReplay`. Rebuild fresh sources/handles with txnAppId/txnVersion configured first, following the existing contract. Assert denied duplicate, changed intent and unknown outcome cannot submit another write; authorized duplicate uses exact committed proof and current policies. No implicit API replay.
- [ ] Exercise stage preparation/consumption/publication races with deterministic barriers and bounded timeouts. Assert late callbacks remain inert after every error path and separate owners do not interfere. Capture settings after native analyzer initialization and verify companion code never mutates them on either success or failure.
- [ ] Run new tests RED, implement only the demonstrated fixes, then GREEN on Spark 3.5.4 with both Delta pins plus affected AQE=false recovery cases. Preserve original failures and per-case task/commit/checkpoint observations. Independently review, commit as `test(delta): qualify adaptive MERGE lifecycle` (use a fix title for substantive product changes), and freeze the scenario map.

## Task 5: Package and qualify all four runtime pairs

**Files:** Modify `A/cdf_merge_live.py`, `A/cdf_merge_dataframe_pyspark.py`, `A/cdf_merge_live_checks.py`, `A/cdf_read_live_package.py` and `T/TestDeltaRangerLiveCdfMerge.scala`. Create `A/test_cdf_merge_adaptive_checks.py`. Extend `A/cdf_merge_live.gradle` only if the thin app needs additional test helpers. Update `docs/delta-spark-parity.md`, `docs/delta-cdf-merge.md`, `A/README.md` and this plan. No catalog-demo plan or deployment edits.

**Interfaces:** Add live CLI `--execution-profile nonadaptive|adaptive-coalescing` with default `nonadaptive`; preserve existing SQL/DataFrame frontends. The adaptive profile explicitly supplies the table above before session/source/handle construction. Package source/runtime/Python bindings remain authoritative. Adaptive live result records add `executionProfile`, effective/captured settings and per-phase payload evidence: root kind, initial/current/final identities, final-plan state, owned completed shuffle/broadcast stages, coalesced coverage, operation/attempt/phase linkage and original exits. Store identifiers/provenance, not credentials or payload in diagnostic plan records.

- [ ] Write checker RED tests for AQE=true without payload stages, metadata-only witness, forceApply-only proof, foreign/stale/missing stage or owner, unsupported profile, missing final plan, altered exact bags, wrong artifact/provider/import origin and missing original exit. A denied read must show denial before unauthorized payload work; do not require a completed stage for denial. Preserve legacy nonadaptive validation without relabeling historical results.
- [ ] Implement the explicit profile in session construction and structured live requests; retain actual Python source/Columns/builder execution. Add two phases to the existing five partitioned MERGE phases: ordinary named governed adaptive INNER join success and referenced-column denial. Use sufficient partitions/data for real payload coalescing. Fixture setup/observation can remain Scala; successful MERGE application execution must be the packaged classic Python facade.
- [ ] Run checker/facade tests, freeze source/class/package inputs and build one immutable package per Spark/Delta pair. Check disk capacity first and serialize heavy processes. Verify actual Ranger 2.8.0 and MinIO readiness, original provider origins and private fixture inputs without exposing secrets. Service recovery is allowed for these local tests; do not alter catalog-demo.
- [ ] Run all new adaptive installed suites on all four pairs, with directly affected nonadaptive regressions. Reuse Tasks 1–4 results only when final source/class/dependency equality is demonstrated. On each pair require genuine ordinary-read and MERGE adaptive payload execution, completed shuffle/static broadcast stages and at least one real coalesced payload read. Standalone naturally nonadaptive CDF is separately identified.
- [ ] Run seven fresh live phases per pair through the new adaptive profile: distinct-target MERGE, column UPDATE denial, source SELECT denial, target MODIFY denial, same-table writeback, ordinary governed adaptive join success and referenced-column denial. Require 28 original phase records, outer/Python/native exit 0 for successful harness verdicts, expected application denials, exact rows/CDF/actions/partitions/checkpoints, settings stability and persistent registration/restart evidence. Keep setup, metadata and application identity separation.
- [ ] Build a final evidence index with source/JAR/ZIP/member/provider hashes, XML/selectors, original commands/exits and checked per-phase witnesses. Keep failed attempts immutable and identify inherited unchanged checks explicitly. Rebuild/rerun only affected artifacts after fixes; documentation-only changes do not require repeating an unchanged matrix.
- [ ] Document supported profile, setup before handle construction, true adaptive versus naturally nonadaptive phases and policy-revocation limits. Keep ordinary grammar, cache/reuse, skew/local-reader/join-conversion, Connect/streaming/separate-executor and full-Delta compatibility limits explicit. Record catalog-demo as deferred pending user direction; no automatic next-operation selection.
- [ ] Obtain final independent integrated SPEC/QUALITY review against all five tasks and original evidence. Resolve concrete findings, commit locally as `feat(delta): qualify bounded governed AQE`, and report the closed scope plus outstanding operations separately. Do not push, publish, deploy or start another milestone from this plan.

## Commands and evidence

Use fresh `E/task-N/<spark>-<delta>/attempt-N/` directories. Retain original RED/GREEN commands, logs, exits, XML, source/class/provider hashes and review results. Memory connectors were unavailable during planning; use retained project reports and record new findings in this workspace.

Installed-test template (create the XML directory first; change selectors/pins/evidence path per task):

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_merge_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerAdaptiveRead' \
  --tests '*TestDeltaRangerAdaptiveOwnership' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PcdfLiveXml="$PWD/.superpowers/sdd/2026-10-25-delta-governed-aqe/task-1/3.5.4-3.3.0/attempt-1/xml" \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

Expected GREEN: original exit 0; nonzero intended tests; zero failures/errors/skips; loaded requested Spark/Delta and Ranger 2.8.0/Scala 2.12.21/Java 17 evidence. A dependency/setup failure is not a product RED. Record the actual discovered test count; do not invent it in advance. Task 4 RED tests precede any fixes.

Formatting: with the same JDK/pins, run `./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply` followed by `:spark-connector:spark-delta-ranger-3.5:spotlessCheck`; inspect the diff and run `git diff --check`. Check Java documentation if Java APIs change. Avoid a repository-wide formatting mutation unrelated to this module.

Task 5 Python checks use the pinned runtime interpreter:

```bash
python -m unittest discover -s delta/acceptance -p 'test_cdf_merge*checks.py' -v
python -m unittest discover -s spark-connector/v3.5/spark-delta-ranger/python/tests -v
```

Package with the existing `python delta/acceptance/cdf_read_live_package.py --app merge --base <verified-base-manifest> --out <fresh-package-directory> --spark <pin> --delta <pin>`. After Task 5 implements the option, run `python delta/acceptance/cdf_merge_live.py --execution-profile adaptive-coalescing --frontend dataframe --partitioned --runtime <fresh-manifest> --inputs <private-inputs> --out <fresh-live-directory>`.

Resolve placeholders from retained verified inputs; do not paste credential contents. Package and live runs need enough free disk and actual service readiness, not an assumption based on earlier sessions. Record each command's original exit even when a pipeline captures output.

## Self-review and handoff

Spec coverage: profile, actual stock insertion, async/current/final ownership and empty stages → Task 1; standalone CDF history/source and consumption → Task 2; native materialization/checkpoint identity, phases, policy semantics, maintenance and precommit authority → Task 3; retry/revocation/reconciliation and cleanup → Task 4; four pairs, actual Ranger/Python/MinIO and strict evidence/docs → Task 5. All five Review Focus conditions have named owning tests. Private factories and Owner methods are shared consistently; stage-hook details remain explicitly gated by installed Task 1 proof rather than assumed feasible.

No task broadens ordinary logical grammar or starts catalog-demo integration. Additional operations await the user's assessment and separate scope. Implementation method remains subagent-driven. User approved this plan. Start Task 1; subsequent task boundaries use independent review, not repeated direction questions.
