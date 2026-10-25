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

# Delta Trusted Spark AQE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already chose this execution method.

**Goal:** Execute the existing governed named Delta reads, bounded CDF and SQL/JVM/classic-PySpark MERGE with stock Spark AQE and the approved operation-boundary authorization contract.

**Architecture:** Retain logical policy rewriting, original permission demands and native Delta command orchestration. Capture accepted recipes at action/phase boundaries; stock Spark executes them without exact-stage identity authorization. Keep MERGE precommit and known-commit recovery checks.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21, S3/MinIO, existing Scala/JUnit and classic PySpark acceptance harnesses.

**Spec:** [Approved design](../specs/2026-10-25-delta-trusted-spark-aqe-design.md). Read it before this plan.

**Status:** Design approved by the user's “Proceed” after `f57759621`. Plan approved by the user’s “Yes”; Task 1 starting. This supersedes the [exact-stage plan](2026-10-25-delta-governed-aqe.md); its failed probes remain historical evidence.

## Global Constraints

- Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21 and S3-compatible storage with MinIO locally.
- No Gravitino core, API/server/client changes, second metastore, engine fork, path fallback, policy migration, source collection/reconstruction, new public builder API or automatic configuration toggles. Public builder API version remains 1.
- Gravitino owns registration; preserve catalog/schema/table/column resource mapping. Ranger owns column grants, filters and masks, including masks on merge keys.
- The platform continues to exclude concurrent writing jobs for one dataset.
- Retain original demands before pruning; filters/masks precede authored expressions. Preserve hidden/raw target copy-forward, explicit assignments, clause order, native ambiguity/constraints and row-filter read semantics. No reverse masking, deduplication, unmasked-key requirement or postimage constraint.
- Retain inclusive CDF bounds, history/files, native materialization, partitions, same-registration writeback, transaction intent, retries and known-commit reconciliation.
- Running reads may finish under captured decisions. Next supported action rechecks local Ranger state; no final-output or per-stage revocation promise. MERGE checks each source attempt, touched-file/write phase and precommit; no atomic Ranger/Delta revocation fence.
- Stock optimizations are allowed; foreign planner/columnar extensions, custom cost evaluators and disabled authorization remain rejected. Keep current extension order: companion then Delta, exactly once each; no stock RangerSparkExtension alongside the companion.
- No new ordinary logical grammar, source transformations, cross-action caches, policy subqueries, streaming/Connect or separate-worker deployment. Unrelated append and CDF-property AQE gates stay closed. Catalog-demo remains deferred.
- Preserve evidence and worktrees. Serialize heavy tests; reuse unchanged results only with source/class/dependency equality. No PR, deployment or milestone claim before integrated qualification. At actual milestone closure, honor the user's standing instruction to push the branch after checking the intended remote; no push during planning or intermediate tasks.

## Review Focus

- A plan prepared before a policy/registration change must reject before its first payload job, including static broadcasts prepared asynchronously. Task 1: `preparedReadRejectsBeforePayload`.
- Policy refresh during a read must not turn successful captured-policy execution into an incidental callback failure. Task 1: `inFlightReadUsesCapturedDecisions`.
- Two catalog names with the same database/table and empty/pruned scans must retain distinct original demands. Task 1: `catalogAndPrunedDemandsRemainDistinct`.
- An unrelated operation must not borrow a previous command's source checkpoint/context after native retry or failure. Tasks 2–3: `foreignCheckpointRejected`, `failedOperationDoesNotAffectNextCommand`.
- A commit that succeeded before acknowledgement/publication failure must not be replayed after revocation or a fresh API call. Task 3: `knownCommitRechecksWithoutReplay`.

## File map and interfaces

Paths below use these exact prefixes (expand them before editing):

- `M` = `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `J` = `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger`
- `T` = `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `A` = `delta/acceptance`
- `E` = `.superpowers/sdd/2026-10-25-delta-trusted-spark-aqe`

Responsibilities:

| Unit | Responsibility and planned change |
| --- | --- |
| New `M/DeltaRangerExecutionContext.scala` | Nonserializable driver context carrying a detached accepted logical recipe and immutable decisions; no stage registry, physical fingerprint or policy-engine callback. |
| `M/DeltaRangerDecisionReceipt.scala`, `DeltaRangerNativeScans.scala` | Keep named original obligations and action checks; separate callback recipe checks from live caller checks. |
| `M/DeltaRangerReadRule.scala`, `DeltaRangerPhysicalPlanning.scala`, `DeltaRangerPhysicalProvenance.scala`, `DeltaRangerColumnarRule.scala`, `DeltaRangerCollectGuardExec.scala`, `DeltaRangerSparkExtension.scala` | Admit supported reads, install a real action boundary before payload submission, and allow stock planning/preparation/AQE. Retain unrelated command routes. |
| `M/DeltaRangerCdfRead.scala`, `DeltaRangerCdfReadReceipt.scala`, `DeltaRangerCdfReadScans.scala`, `DeltaRangerCdfNativeReader.scala` | Bounded CDF logical admission, file/history proof, captured recipe and native source helpers. |
| `M/DeltaRangerMergeReceipt.scala`, `DeltaRangerMergeSource.scala`, `DeltaRangerMergeCommand.scala`, `DeltaRangerMergeNative.scala`, `DeltaRangerMergeWrite.scala` | Source-attempt and touched-file/write boundaries, original target semantics, checkpoint handoff and native algorithms. |
| `M/DeltaRangerMergeCommit.scala`, `DeltaRangerMergeResult.scala`, `DeltaRangerMergeJournal.scala`, `DeltaRangerRawMaintenance.scala`, `DeltaRangerCdfCacheMaintenance.scala`, `DeltaRangerCommitMetadata.scala` | Precommit/reconciliation and necessary metadata/publication paths; edit only implicated seams. |
| `J/DeltaRangerNativeOperation.java`, `M/DeltaRangerNativeProvenance.scala`, `DeltaRangerRawMetadata.scala` | Audit shared native callbacks; narrowly adapt only callbacks reached by the admitted operation. Preserve raw metadata/payload role separation. |
| New `T/DeltaRangerTrustedAqeFixture.scala` and focused `TestDeltaRangerTrustedAqe*` suites | New contract's installed execution/timing tests. Keep old AdaptiveOwnership/Invocation diagnostics and their restrictive fixture unchanged. Those tests describe the withdrawn exact-stage requirement; their passing results do not qualify the new route. |
| `A/cdf_merge_live.py`, `cdf_merge_dataframe_pyspark.py`, `cdf_merge_live_checks.py`, `cdf_read_live_package.py`, `cdf_merge_live.gradle`; `T/DeltaRangerLiveCdfMerge.scala` | Explicit stock-AQE live mode, packaged Python execution, final-plan diagnostics and complete artifact evidence. |

Private cross-task interface, introduced in Task 1 and extended in Task 2:

```scala
private[ranger] object DeltaRangerExecutionContext {
  sealed trait Context {
    def logicalPlan(): LogicalPlan // detached accepted recipe; never a reconstructed user payload
  }
  def forRead(receipt: DeltaRangerDecisionReceipt,
      spark: SparkSession, recipe: LogicalPlan): Context
  // Added by Task 2:
  def forCdf(receipt: DeltaRangerCdfReadReceipt,
      spark: SparkSession, recipe: LogicalPlan): Context
  def forMerge(receipt: DeltaRangerMergeReceipt,
      attempt: DeltaRangerMergeSource.Attempt, phase: MergePhase,
      recipe: LogicalPlan): Context
  sealed trait MergePhase
  case object Source extends MergePhase
  case object TouchedFiles extends MergePhase
  case object Write extends MergePhase
}
```

Use a private final nonserializable implementation of Context with immutable captured fields; its field layout is builder-owned. Factories are called only by the owning driver entry/phase after validating that the recipe belongs to that receipt/attempt. Each factory performs the current boundary authorization using existing `validateAction(session: SparkSession): Unit` or `validate(): Unit`, then captures immutable decisions/recipe. Context access does not authorize a new action or recheck ambient user, policy version or physical stage. Existing owners retain lifecycle/attempt state; Context does not replace command/source identity or expose a reusable public token. Keep executor closures free of driver receipts, engines and credentials. Do not introduce these interfaces as unused scaffolding: wire and test them in their owning task.

Before replacing any guard, record its existing symbol, semantic purpose, retained boundary or withdrawn physical guarantee, and owning test in `E/guard-map.md`. This is one maintained inventory, not a separate research task. Logical/source/transaction checks remain; reconstructed physical-tree/RDD-graph equality and every-stage checks are withdrawn only for the revised routes. AQE=false regressions must stay green.

## Task 1: Real governed Delta read with stock AQE

**Files:** Create `M/DeltaRangerExecutionContext.scala`, `T/DeltaRangerTrustedAqeFixture.scala`, `T/TestDeltaRangerTrustedAqeRead.scala`. Modify the ordinary-read and shared-callback units in the file map, only where the read route requires them. Extend existing `T/TestDeltaRangerInstalledStaleness.scala`, `TestDeltaRangerPhysicalAdmission.scala` or `TestDeltaRangerReadRule.scala` only for changed contract expectations.

**Interfaces:** Produce `Context.logicalPlan()` and `forRead` above. Consume existing `DeltaRangerDecisionReceipt.validateAction`, `DeltaRangerNativeScans.Prepared` and named logical obligations. Preserve `DeltaRangerCollectValidationContext.validateCollection(child: SparkPlan)` / `validateExecution(child: SparkPlan)` where needed by other commands; do not make every internal doExecute a new action check.

- [ ] Add `namedJoinUsesStockAqe`: use real named Delta tables via `NativeSelectFixture` in `T/TestDeltaRangerReadRule.scala` (installed extension route, never `componentRewriter`), a conditioned INNER join, projected masked column and row filter. Assert exact result multiset (including duplicate join matches), original permission columns, final `AdaptiveSparkPlanExec.isFinalPlan`, materialized payload shuffle stages and a real coalesced read. Configure fixture before constructing tables/handles; do not reuse the old sixteen-entry diagnostic profile as production restrictions. Add a stock-defaults case separately from the coalescing-tuned case.
  Test-owned seed: people `(1, "one", "east"), (2, "two", "west"), (3, null, "east")`; dimension `(1, "x", "east"), (1, "y", "east"), (2, "z", "east"), (3, "v", "east")`, using the existing `(id, secret, region)` schema for both. Apply `region = 'east'` and the constant string mask `'MASKED'` to the selected secret of catalog `a`, including null; catalog `b` has unmasked label values. Seed using the fixture setup principal before installing application authorization. The query is `SELECT p.id, p.secret, d.secret AS label FROM a.db.t p INNER JOIN b.db.t d ON p.id = d.id`; use the fixture's separate catalog policies and registration bindings. Pin these assertions in `namedJoinUsesStockAqe`:

  ```scala
  assertEquals(
    Map(Row(1L, "MASKED", "x") -> 1, Row(1L, "MASKED", "y") -> 1,
        Row(3L, "MASKED", "v") -> 1),
    actualRows.groupBy(identity).map { case (row, rows) => row -> rows.length })
  assertTrue(finalAdaptivePlan.isFinalPlan)
  assertTrue(materializedPayloadShuffleCount > 0)
  assertTrue(coalescedPayloadReadCount > 0)
  ```

  `actualRows` is the collected application result; the remaining values are observations of that action's real final plan, using the new test fixture. The default-AQE case asserts exact rows and actual adaptive execution without requiring coalescing if its data/stats choose broadcast.

- [ ] Run that test RED on Spark 3.5.4 / Delta 3.3.0 using the installed command below. Require the current user-AQE denial, not an infrastructure/import failure; retain original output and XML.
- [ ] Add `preparedReadRejectsBeforePayload`, `inFlightReadUsesCapturedDecisions`, `catalogAndPrunedDemandsRemainDistinct`, `foreignExtensionAndContextDriftReject`. Assertions: predicate-only SELECT and filter/mask changes before action deny with zero payload tasks; registration replacement, subject/session/config drift deny; policy change after action acceptance permits exact captured-policy rows; a fresh query observes refreshed policy; identical database/table names in distinct catalogs remain distinct; self-join, false predicate and empty scans retain all original demands. Use deterministic driver barriers with bounded timeouts, not sleeps or per-stage reauthorization.
- [ ] Implement read context capture and separate original logical validation from async callbacks. Remove the ordinary AQE rejection in `DeltaRangerReadRule.prepare` only with the installed action-boundary proof. Replace UserTicket's fixed-tree authorization for this route; normal stock physical preparation must create the adaptive plan. The action check must run before any payload submission, including broadcast preparation, and on every supported action; a planning-only check or post-job listener is insufficient. Preserve the logical mask/filter recipe and scan registration/role validation without rechecking mutable Ranger state from accepted in-flight callbacks.
- [ ] Implement startup/config qualification for stock AQE, retaining known extension order, protected authorization settings and foreign-rule/cost-evaluator rejection. No added stage hook, fabricated adaptive wrapper, fallback to AQE=false or automatic session mutation. Include necessary raw metadata integration for this read only. Keep append/property/CDF/MERGE gates unchanged for now. Record the concrete action boundary and callback flow in `E/task-1/boundary-map.md` with installed evidence on both Spark pins; do not claim an untested hook works.
- [ ] Run the new suite GREEN on Spark 3.5.4 and 3.5.6 / Delta 3.3.0. Run affected existing AQE=false read, staleness and physical-admission cases once per changed shared seam. Changed physical-tamper expectations must be explicitly classified in guard-map, never silently deleted to turn the suite green. Format/check the module and inspect the diff.
- [ ] Obtain fresh SPEC/QUALITY review against this task and original evidence. Commit locally as `feat(delta): authorize adaptive reads at action boundaries`. Deliverable: one actual named Delta adaptive read path, denials and approved timing on both Spark pins; CDF/MERGE AQE still pending.

## Task 2: Bounded CDF and native adaptive MERGE

**Files:** Extend the context with `forCdf` / `forMerge` and the three MergePhase values. Modify the CDF, MERGE and necessary maintenance/publication units in the file map. Create `T/TestDeltaRangerTrustedAqeCdf.scala` and `T/TestDeltaRangerTrustedAqeMerge.scala`; reuse existing CDF and partitioned MERGE fixtures.

**Interfaces:** Consume Task 1 Context and stock planning integration. Preserve `DeltaRangerMergeSource.begin(permit: DeltaRangerMergeCommand.SourcePermit): Attempt`, `Attempt.materialize(body: => Unit): Unit`, and the public builder signatures. The owning command calls `forMerge` for source, touched-file and write recipes; standalone CDF uses `forCdf`. Existing `DeltaRangerMergeCommit.run(): Option[Long]` remains the commit entry.

- [ ] Add `standaloneBoundedCdfUsesCapturedPolicies` and `namedSqlAndBuilderMergeUseAqe`. Assert inclusive versions, exact filtered/masked CDF rows and history/file rejection; compare SQL and JVM builder results on equivalent fresh targets, exact rows/CDF/action bags and one native commit. Require actual adaptive MERGE payload stages; label naturally exchange-free standalone CDF honestly. Run RED before opening either public gate. Reuse the installed MERGE fixture's inclusive `[0, 4]` source interval, with the existing version-4 non-preimage predicate; assert `afterVersion == beforeVersion + 1` for a nonempty successful mutation and `afterVersion == beforeVersion` plus an unchanged consumer checkpoint for a denied mutation. Compare full bags, not just counts.
- [ ] Add `foreignCheckpointRejected`, `actionedCachedAndDerivedSourcesReject`, `partitionedHiddenRowsAndMaskedKeys`, `sameRegistrationWriteback`, `emptyAndNativeErrorOutcomes`. Assert source occurrence/interval and command attempt remain bound through native localCheckpoint; user-supplied equal-schema RDD/checkpoint/cache cannot substitute through supported APIs; escaped/null partition moves, hidden target rows and untouched files survive; omitted fields retain raw values while authored expressions use masks. Literal assignments do not invent SELECT demands. Empty/false clauses keep permissions; ambiguity/constraint failures leave no new version/checkpoint. Authorized postimages outside a row filter remain allowed.
- [ ] Implement CDF context and native source handoff together with complete MERGE admission. The receipt constructor currently rejects AQE, so do not add a source-only test bypass. Capture the checkpoint through the owning native materializer/attempt; replace downstream fixed-RDD-graph proof with that semantic handoff, not a broader LogicalRDD allowlist. Keep native algorithms, file/history proof, source bounds, schema/protocol admission and API version 1.
- [ ] Wire driver checks before `prepareMergeSource`, `findTouchedFiles`, `writeAllChanges`/native write submission and immediately before `transaction.commitIfNeeded`. Split helper `requireLive`/physical verification from accepted context use without propagating ThreadLocals or switching active Spark sessions. Preserve transaction, action intent and target-head validation, including source-policy checks after source execution closes. No new final-output read revocation check. Keep existing known-commit recovery behavior.
- [ ] Add `revokedBetweenMergePhasesStopsNextWork` and `revokedBeforeCommitLeavesNoVersion`: change source grant, target UPDATE or mask/filter at deterministic boundaries; require next phase denial or precommit denial, zero new committed version/checkpoint, and no replay. In-flight task completion and uncommitted files are allowed. Include zero-row/optimized-away paths so boundary checks cannot disappear with payload work.
- [ ] Audit raw maintenance, native cache and recache/publication gates before admitting MERGE. Allow only the metadata implementation needed by this command under the captured/current boundary roles. Known unsupported application-cache conditions reject before mutation. Add `unsupportedCacheRejectsBeforeMutation`; assert unchanged target version/checkpoint and no write tasks. Preserve unrelated append/property AQE rejection and current settings handling after native initialization/postcommit metadata.
- [ ] Run focused suites GREEN on Spark 3.5.4 with both Delta pins, plus affected AQE=false installed CDF/MERGE/partition/source cases. Use Task 1 evidence only when its dependencies are unchanged. Format/check, independently review and commit as `feat(delta): execute bounded CDF and MERGE with stock AQE`. Deliverable: complete semantic CDF/MERGE route; recovery breadth and full live matrix remain pending.

## Task 3: Native recovery, isolation and stock optimization variants

**Files:** Create `T/TestDeltaRangerTrustedAqeRecovery.scala` and `T/TestDeltaRangerTrustedAqeVariants.scala`. Extend Task 1–2 fixture/tests and existing `T/TestDeltaRangerMergeRecovery.scala`, `TestGravitinoDeltaMergeLifecycle.scala`, `TestDeltaRangerPartitionedMergeLifecycle.scala` only for affected shared seams. Production edits belong only to the context/receipts/native helper/commit paths implicated by failing cases.

**Interfaces:** Consume Tasks 1–2 factories and existing journal/command APIs. Produce `E/task-3/scenario-evidence.json`: scenario, selector, version pair, observed branch, source/class/dependency hashes, original result paths and explicit inherited evidence. No wrapper retry or new checkpoint API.

- [ ] Add `nativeBlockLossRetry`, `nativeRetryExhaustion`, `policyChangedBeforeRetry` and `failedOperationDoesNotAffectNextCommand`. Use existing native block-loss hooks. Assert fresh attempt/context, same bounded interval and original intent, one commit after successful retry, zero after exhaustion/revocation; failed/closed command state cannot authorize a fresh command or another source. Test two independently authorized operations without cross-context interference. Do not require rejection of arbitrary JVM-created Spark stage objects.
- [ ] Add `knownCommitRechecksWithoutReplay`, `changedIntentAndUnknownOutcomeReject`, `publicationFailureRetainsRecoveryState`. Assert acknowledgement loss is reconciled using durable exact commit identity/current authority, authorized duplicate writes nothing, revoked duplicate/changed intent/unknown outcome cannot replay, and consumer checkpoint advances only after synchronous success. Preserve the already-committed state on reconciliation denial; never claim rollback. Run RED before any fixes; if unchanged paths already pass, retain GREEN as regression evidence rather than manufacture a failure.
- [ ] Add variants named `stockDefaults`, `coalescedShuffle`, `staticBroadcast`, `adaptiveBroadcastAndLocalReader`, `intraQueryExchangeReuse`, `skewedJoin` and `runtimeFiltering`. Use only admitted logical shapes, fixture statistics/data distributions and explicit settings captured before handle construction. Assert exact policy-governed bags plus the actual final operator/stage evidence for each claimed branch. Do not count an enabled setting as coverage or synthesize a stage. Test default behavior and actual coalescing on every pin in Task 4; targeted variants cover changed helper seams on both Spark pins. If a branch cannot arise within the current grammar/native helpers, record source-backed non-applicability and do not broaden grammar or claim it was tested.
- [ ] Exercise pre-action/static-broadcast, mid-read and between-phase policy timing under deterministic barriers, including cleanup on preparation/task/native failure. Verify current caller/config at boundaries, frozen in-flight decisions and unchanged settings. Check that Plan/Context objects and logs carry no admin credentials, and executors do not serialize mutable authorization engines.
- [ ] Fix only demonstrated integration failures, preserving stock algorithms and configuration. Run new recovery/variant suites on Spark 3.5.4 with both Delta pins and Spark 3.5.6 / Delta 3.3.0; include affected AQE=false lifecycle cases. Final Task 4 covers any remaining pair or invalidated candidate. Format/check, independently review, freeze scenario map and commit locally as `test(delta): qualify adaptive execution and recovery` (use a fix title if product changes dominate).

## Task 4: Packaged Python and live four-pair qualification

**Files:** Modify the acceptance harness units in the file map. Create `A/test_cdf_merge_trusted_aqe_checks.py`; extend `T/TestDeltaRangerLiveCdfMerge.scala` request tests if request shape changes. Update `docs/delta-spark-parity.md`, `docs/delta-cdf-merge.md`, `docs/delta-spark-execution-assessment.md`, `A/README.md` and this plan.

**Interfaces:** Add CLI `--execution-profile nonadaptive|adaptive-stock`, default `nonadaptive`, preserving `--frontend sql|dataframe` and `--partitioned`. Set the selected profile before session/source/handle construction. `adaptive-stock` enables AQE without the old disable-list; deterministic branch fixtures are separately recorded. Extend existing `verify_evidence(result)` / `verify_result(result)` in `cdf_merge_live_checks.py`; keep legacy nonadaptive records valid without relabeling historical evidence.

New records include `executionProfile`, requested and effective settings, original runtime/artifact identity, and per-phase `adaptiveEvidence` with `phase`, `isPayload`, `rootClass`, `isFinalPlan`, materialized shuffle/broadcast counts, coalesced/local/skew/reuse observations and original plan evidence path/hash. These are diagnostics, not authorization tokens. Expected denials and exchange-free CDF have explicit outcomes instead of fake successful-stage counts. Bind supplemental artifacts as well as the stock runtime.

- [ ] Write checker RED cases for AQE flag without actual payload execution, metadata-only/forceApply-only witness, absent final plan, wrong operator claim, wrong pair/provider/Python import, changed result bags, missing original exits and modified supplemental JAR. Add a request/profile test that asserts settings are supplied before session/handle construction. Keep facade/API version tests unchanged unless implementation changes their seam.
- [ ] Implement profile selection and capture actual final payload observations from the packaged native/Python application. Preserve the five existing MERGE phases: distinct target, column UPDATE denial, source SELECT denial, target MODIFY denial and same-table writeback. Add ordinary adaptive named join success and predicate-only SELECT denial. Successful DataFrame MERGE must execute through the packaged classic-PySpark facade, not be replaced by setup SQL or a driver-reconstructed DataFrame.
- [ ] Implement fresh-query revocation evidence and verified registration/resource mapping for the live Delta chain. Use actual Ranger 2.8.0 server/client and MinIO. Capture policy download/refresh evidence and original application identities; retain actual Gravitino grant propagation where claimed. Native-seeded Iceberg fixtures with metadata authorization disabled are not substitutes. A test can author a unique fixture policy only when labeled as that, never as proof of propagated grants.
- [ ] Run checker tests, affected facade/request tests and formatting. Freeze a final product candidate; check disk and service readiness before building four immutable packages. Serialize large processes. Reuse existing private local fixture inputs without printing credentials. Recover local test services if needed; leave catalog-demo untouched. Package provenance must cover source hashes, compiled classes, ZIP/JAR members, provider origins and supplemental artifacts.
- [ ] Run new installed suites on all four pairs, including final ordinary default/coalescing and MERGE payload evidence, plus affected AQE=false checks. Reuse earlier task results only for equal final source/class/dependency inputs. Record actual selected tests/counts and original exits. No zero-test, skipped-case or setup-failure pass.
- [ ] Run seven fresh live phases per pair using `adaptive-stock`, packaged DataFrame frontend and partitioned fixture: 28 original phase records plus explicit fresh-query revocation records. Require successful harness verdicts, expected application denials, actual final adaptive ordinary-read and MERGE payload stages, coalescing on each pair, exact rows/CDF/actions/partitions/checkpoints and stable settings. Preserve Gravitino registration/restart evidence and separate setup/metadata/application identities. Label naturally exchange-free phases honestly.
- [ ] Build a complete evidence index and run checkers against all frozen stock and supplemental artifacts, original exits, policy transitions and result bags. Retain failed attempts; mark unknown fields rather than infer them. After a fix, rebuild/rerun affected packages and scenarios only. Never repeat an unchanged matrix for documentation edits.
- [ ] Update support docs with the operation-boundary revocation contract, actual qualified variants, public API/setup instructions and remaining logical/deployment limitations. Identify withdrawn physical-hardening claims and superseded profile; retain historical diagnostics. Keep broader Spark operations and catalog-demo as pending user scope, not implied completed support.
- [ ] Obtain final independent integrated SPEC/QUALITY review of code and original evidence. Resolve concrete findings, commit the qualified scope, verify branch/remote and push under the user's standing milestone-close instruction. Report exact commit/push result, supported scope and remaining work; continue locally only within already authorized scope.

## Commands and evidence discipline

Use fresh `E/task-N/<spark>-<delta>/attempt-N/` directories; preserve original command, exit, logs, XML and candidate hashes. Memory connectors were unavailable during planning; use retained project evidence and save new findings locally. No runtime tests were run to produce this plan.

Installed-test template; create the output directory before running and substitute the owning task's suite/pins:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_merge_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerTrustedAqeRead' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PcdfLiveXml="$PWD/.superpowers/sdd/2026-10-25-delta-trusted-spark-aqe/task-1/3.5.4-3.3.0/attempt-1/xml" \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

Expected GREEN: original exit 0, intended nonzero test count, zero failures/errors/skips, requested loaded Spark/Delta and Ranger 2.8.0/Scala 2.12.21/Java 17. Preserve a genuine product RED before implementing new behavior; do not call dependency or fixture startup failure RED. Run suites individually if combined memory exceeds capacity. The existing init script uses one 4-GiB test JVM; do not run multiple heavy builds concurrently.

Formatting with the same JDK/pins: `./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply`, then `:spark-connector:spark-delta-ranger-3.5:spotlessCheck`, inspect diff and `git diff --check`. Run affected Java checkstyle when Java signatures change; document all new public/protected Java members. Avoid unrelated repository-wide edits.

Python checks use the pinned runtime interpreter:

```bash
python -m unittest discover -s delta/acceptance -p 'test_cdf_merge*checks.py' -v
python -m unittest discover -s spark-connector/v3.5/spark-delta-ranger/python/tests -v
```

Package with the existing command:
`python delta/acceptance/cdf_read_live_package.py --app merge --base <verified-base-manifest> --out <fresh-package-directory> --spark <pin> --delta <pin>`.
After implementing the CLI, run:
`python delta/acceptance/cdf_merge_live.py --execution-profile adaptive-stock --frontend dataframe --partitioned --runtime <fresh-manifest> --inputs <private-inputs> --out <fresh-live-directory>`.
Resolve placeholders from verified local manifests; never include private input contents in reports. Record original subprocess exits even when an outer harness reports success.

## Self-review and handoff

Coverage: ordinary read/startup/action timing and captured callbacks → Task 1; CDF/source checkpoint/native MERGE/partition/mask and precommit semantics → Task 2; native retries/reconciliation/isolation and representative stock variants → Task 3; all four pairs, real Ranger/MinIO/Python, grant/registration evidence, documentation and complete artifact checking → Task 4. All five Review Focus entries have named tests. Factories and existing entry signatures are consistent across tasks. Exact field layouts, locking and helper decomposition remain builder choices within the approved contract.

The real action boundary must be demonstrated by Task 1; no plan text assumes that a columnar hook alone runs before adaptive work. Failure to preserve a required semantic is a concrete blocker to report, not permission to silently weaken it. The old exact-stage requirement is withdrawn and must not be reopened as a prerequisite.

Implementation method remains subagent-driven, one task and fresh review at a time. The user approved this replacement plan. Proceed through these task gates without repeated direction questions. No additional operation or catalog-demo integration is authorized by this plan.
