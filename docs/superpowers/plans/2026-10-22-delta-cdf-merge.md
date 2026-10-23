# Governed CDF-to-MERGE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already selected subagent-driven execution.

**Goal:** Execute a named bounded CDF source through Spark directly into a named, Ranger-governed Delta MERGE, including same-table writeback, without driver collection of source rows.

**Architecture:** Capture original named operands and user expressions before native preprocessing, preserve Delta's full target relation, and apply Ranger policies to user match/action expressions. A companion command delegates row algorithms to native MERGE traits while owning source materialization, transaction authorization, commit evidence and registration publication. Prove native linkage and phase ownership before implementing the broader behavior or packaging fixtures.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21, Ranger 2.8.0, MinIO, Gradle, Python acceptance harness.

**Spec:** [Governed named Delta CDF-to-MERGE pipelines](../specs/2026-10-22-delta-cdf-merge-design.md).

**Status:** Approved for task-by-task execution on 2026-10-22, with former Task 6 deferred to a separate plan. Execute and review Task 1, report its result, then await the next task instruction. Target row-filter read semantics remain confirmed. Following two-pin frontend characterization, the user selected named SQL MERGE first and deferred a companion DataFrame builder to its own plan. Baseline `b321beace14a2313e54aee033d883df30d47f6c7`.

## Global Constraints

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21.
- Actual Ranger 2.8.0 and S3-compatible MinIO qualification.
- Gravitino owns named registration; Delta owns transaction and row reconstruction semantics.
- No Gravitino core/API/server/client changes, second metastore, or Spark/Delta fork.
- Existing Ranger access, row filters and masks govern source and destination user expressions.
- No unmasked/unique-key admission requirement, reverse-mask lookup, automatic deduplication or live policy migration.
- The platform prevents concurrent writer jobs per dataset; native task/source-materialization retries still require qualification.
- Preserve the existing worktree and accepted evidence; rerun earlier tests only for changed dependencies or concrete regressions.
- Retain separate setup, metadata and authenticated application identities; never transfer setup credentials into application execution.

## Review Focus

- Hidden and visible target rows share one file; an update must preserve hidden rows and unassigned raw fields, while explicit target self-assignment uses the mask (Task 2).
- MERGE's native localCheckpoint bypasses the original source producer; a later policy change, lineage substitution or source-loss retry must not bypass current authorization (Task 3).
- Masked keys collide or a hidden target has the same physical key; preserve the submitted governed SQL/native ambiguity or insert behavior without hidden-key matching/deduplication (Tasks 2 and 5).
- A native commit succeeds but registration publication or the caller acknowledgement fails; reconcile the exact commit, never blindly reexecute or advance a failed checkpoint (Task 4).
- Same-table CDF writeback changes the registration currently used by its source; finish source execution before the exact owned target advancement, expire old source authority and reject alternate aliases (Tasks 1, 3 and 4).

## Paths, interfaces and execution rules

All paths below are relative to the existing feasibility worktree. Abbreviations:

- `M = spark-connector/v3.5/spark-delta-ranger`
- `S = M/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `J = M/src/main/java/org/apache/gravitino/spark/connector/delta/ranger`
- `T = M/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `A = delta/acceptance`
- `E = .superpowers/sdd/2026-10-22-delta-cdf-merge/` (new implementation evidence; preserve planning discovery separately).

New units are companion-private, not Gravitino APIs: `DeltaRangerMergeCommand`
(capture and native trait adapter), `DeltaRangerMergeReceipt` (source/target demands
and policy expression rewriting), `DeltaRangerMergeSource` (attempt-owned native
materialization), `DeltaRangerMergeNative` (target/helper phase ownership),
`DeltaRangerMergeCommit` (Merge intent/publication), and `DeltaRangerMergeJournal`
(typed private recovery state). Keep responsibilities separate; do not add a broad
MERGE permission branch to append or raw query admission. Existing bindings may
extract shared validation utilities, but append-specific receipts remain typed.

Planned companion-private interfaces; Task 1 validates native accessibility before
these interfaces become implementation contracts:

- `DeltaRangerMergeCommand.capture(spark: SparkSession, plan: LogicalPlan): Option[Capture]` retains exact original named operands, user assignments and frontend identity.
- `DeltaRangerMergeReceipt.issue(spark: SparkSession, capture: Capture): DeltaRangerMergeReceipt`; `validate(): Unit`; `rewrite(): DeltaMergeInto`. Capture and receipt are unforgeable driver objects, not serializable authority.
- `DeltaRangerMergeCommand.bind(native: MergeIntoCommand, receipt: DeltaRangerMergeReceipt): DeltaRangerMergeCommand` accepts only the exact owned preprocessing successor. The command implements native `MergeIntoCommandBase` + `ClassicMergeExecutor` + `InsertOnlyMergeExecutor`; `protected def runMerge(spark: SparkSession): Seq[Row]` owns one transaction attempt.
- `DeltaRangerMergeSource.begin(receipt: DeltaRangerMergeReceipt, attempt: Int): Attempt`; `Attempt.validate(): Unit`; `Attempt.close(): Unit`. Native materialization callbacks register the actual issued LogicalRDD/RDD in that attempt, never arbitrary caller RDDs.
- `DeltaRangerMergeCommit.run(transaction: OptimisticTransaction, actions: Seq[Action], operation: DeltaOperations.Merge, receipt: DeltaRangerMergeReceipt): Option[Long]` freezes exact actions, validates before native commit, and records publication evidence.
- `DeltaRangerMergeJournal` stores an immutable typed `Intent` and transitions PREPARED → COMMITTED → RECONCILED, or UNKNOWN. Recovery requires matching native commit evidence and current authorization; state alone never grants access.

These are proposed interfaces, not claims that native methods with these names
already exist. Source discovery references actual protected native APIs in the spec.
If Task 1 disproves accessibility or lifecycle, stop that approach and revise the
small affected contract; do not expand privileges or start the final matrix.

For every task: run focused RED, implement, run covering GREEN/formatting, then
independent scoped review and local commit. Serialize implementation/build/live
ownership. Test only the changed behavior and affected seams before final matrix.
Retain exact commands/exits and concise evidence indexes; avoid repeated copies of
unchanged packages. No milestone-close claim until the five-task pipeline qualification and final review pass.
The user explicitly requested task-by-task progress; do not automatically start the
next task after reporting the current task and its independent review.

Base focused command (substitute task selectors; use JDK 17):

```bash
./gradlew -I delta/acceptance/cdf_read_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PskipITs -PskipDockerTests=false --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain \
  --tests '*TestDeltaRangerInstalledMerge'
```

Use the existing init's single fork/4 GiB test limit and retained native dependencies.
The accepted init uses `deltaSparkVersion` and `deltaVersion`; verify loaded native
origins in each result, not only the requested CLI labels.
Run module `spotlessApply` then `spotlessCheck` for changed Scala/Java, relevant
Javadoc/compilation, and Python unit tests for new checker logic. No checkstyle pass
claim for tasks absent from this module.

## Task 1: Prove the installed native MERGE integration

**Files:** Create `S/DeltaRangerMergeCommand.scala`, initial `S/DeltaRangerMergeReceipt.scala`, `S/DeltaRangerMergeSource.scala`, `S/DeltaRangerMergeNative.scala`, `S/DeltaRangerMergeCommit.scala` and `T/TestDeltaRangerInstalledMerge.scala`; modify `S/DeltaRangerReadRule.scala`, `S/DeltaRangerSparkExtension.scala` and exact owned dispatch seams in `J/DeltaRangerNativeOperation.java` / `J/DeltaRangerRegistrationBindings.java` only as demonstrated necessary. Native source is read-only.

**Interfaces:** Establish Capture → rewritten DeltaMergeInto → exact native preprocessing successor → owned command; inherit native protected source/selection/write methods and own runMerge transaction/commit. Initial source/native/commit units support the exact positive fixture and reject unknown branches; Tasks 3–4 complete their lifecycle/recovery contracts. Initial positive fixtures use actual allowed policies with no transforms; active transforms reject until Task 2, not silently pass through. This is an integration gate, not a separately advertised MERGE release.

- [x] `namedSqlReachesOwnedNativeMerge`: RED the SQL source-subquery frontend with named registrations, source versions [0,4], explicit update/delete/insert clauses; assert actual target rows/native Merge action type, no driver CDF payload collection/reparallelization.
- [x] Compile the minimal trait adapter against Delta 3.3.0 and 3.3.2. Supply the native abstract source/target/targetFileIndex/condition/clause/schema fields from the retained processed descriptor; keep schema evolution false. Delegate native row algorithms; document the small orchestration copied from pinned runMerge.
- [x] `nativePhasesRetainNamedOwnership`: prove actual AUTO localCheckpoint, repeated source reads, exact target transaction/index, helper actions, final precommit validation and recache ownership. Reject raw paths, unissued command copies and a foreign checkpoint RDD. Native metadata/metric collects remain allowed only within their owned phase.
- [x] `sameTableCdfWritebackHasFiniteSourceLifetime`: fixed pre-write interval on the same named CDF-enabled target; verify one native update and correct CDC output, source execution ends before commit and old receipt rejects afterward. Distinct-registration aliases to the same UUID/location reject. A distinct-table fixture must also pass.
- [x] Run focused installed tests GREEN and compile on both Delta pins. Save exact native method/phase observations and report whether every required hook exists. No broad four-pair package/live work at this gate. Commit `feat(delta): establish owned native merge command` after review.

## Task 2: Apply source and target policies without corrupting stored rows

**Files:** Complete `S/DeltaRangerMergeReceipt.scala`; extend `S/DeltaRangerMergeCommand.scala`; reuse `S/DeltaRangerMaskRules.scala` and `J/DeltaRangerResourceKey.java` without changing existing read/append semantics; create `T/TestDeltaRangerMergePolicies.scala`; extend `src/test/java/.../TestDeltaRangerPolicyEngine.java` for UPDATE-column evaluation.

**Interfaces:** issue/validate/rewrite produces policy-governed original user expressions and frozen SELECT/UPDATE demands. The complete native target and generated unassigned-field preservation remain raw internally. Identity binding uses resolved attributes/occurrences.

- [x] RED `hiddenRowsAndUnassignedMaskedFieldsSurviveRewrite`: visible and hidden rows in the same file, masked secret, update value only; hidden rows and every unassigned raw secret remain exact. Explicit `secret = t.secret` stores the permitted masked value with SELECT+UPDATE. Run the named SQL frontend; the companion builder is deferred.
- [x] RED `targetMatchUsesPoliciesAndNativeMultiplicity`: hidden matching key causes native unmatched insert; masked/null keys obey SQL/null-safe equality; one source matching multiple visible target rows updates them; multiple sources matching one target preserve native ambiguity error (and native unconditional-delete exception). No key precondition or deduplication.
- [x] Implement original-expression capture before assignment alignment; raw Ranger filter AND governed ON, governed user matched predicates/RHS, no postimage filter. Add update moving a row outside visibility and insert outside visibility positive cases. Reject NOT MATCHED BY SOURCE before execution.
- [x] RED/GREEN actual Ranger cases: wildcard UPDATE allow + named-column deny; column-only assigned UPDATE allow; denied-but-unassigned field survives copy; explicit RHS SELECT deny; UPDATE-star expansion; INSERT omitted fields and DELETE demand UPDATE on every physical field; unreachable clause and empty-source demands retained; scheduled/role changes invalidate.
- [x] Source metadata/key masks remain unchanged and are never bypassed for event routing. Reuse CDF per-image filter tests where the integration seam changed; qualify supported CUSTOM/NULL expressions and reject unsupported masks. Commit `feat(delta): govern merge expressions and column writes` after scoped GREEN/review.

## Task 3: Preserve CDF authorization through native materialization and retries

**Files:** Complete `S/DeltaRangerMergeSource.scala` and `S/DeltaRangerMergeNative.scala`; create `T/TestDeltaRangerMergeSource.scala` and `T/TestDeltaRangerMergeOwnership.scala`; modify `S/DeltaRangerCdfRead.scala`, `S/DeltaRangerCdfReadReceipt.scala`, `S/DeltaRangerCdfReadScans.scala`, `S/DeltaRangerPhysicalProvenance.scala`, `S/DeltaRangerPlanAdmission.scala`, `S/DeltaRangerRawMaintenance.scala` only at proven owned-consumer/native phase seams.

**Interfaces:** begin/validate/close issues one attempt-local source/materialization contract from original bounded evidence. Native helper ownership is distinct from user grammar. Exact issued materialized source remains tied to the original receipt even after localCheckpoint cuts lineage.

- [ ] RED `materializedSourceStillRevalidatesBeforeSecondPass`: apply source or target revoke/filter/mask change after first materialization and before later use/commit; fail without a committed target version although CDF producer need not execute again.
- [ ] RED `blockLossRetryCreatesFreshAttempt`: native source-block loss triggers native retry with fresh attempt and current authorization; success under unchanged policy, denial under changed policy, exhausted retry surfaces native failure. Cleanup removes only attempt-owned materialization; no global cache purge.
- [ ] RED `copiedMaterializationCannotBorrowMergeAuthority`: same-schema foreign RDD, copied command/receipt, source substitution, cross-session/subject, wrong source/target index, stale native UUID/schema and user cache/persist/checkpoint all reject. Correct executor serialization transfers data tasks, not driver authority.
- [ ] Implement exact native join/aggregate/UDF/index/write-helper admission under the MERGE phase owner. Preserve ordinary/CDF SELECT grammar. Same-table source closes before publication while target role accepts only its own proven advancement; compatible unrelated source advancement cannot extend the fixed interval.
- [ ] GREEN new source/ownership suites plus affected CDF ownership/SQL/physical tests and append command-boundary regressions. Commit `feat(delta): compose governed cdf with native merge execution` after review.

## Task 4: Validate native publication, idempotency and writeback recovery

**Files:** Complete `S/DeltaRangerMergeCommit.scala`; create `S/DeltaRangerMergeJournal.scala`, `T/TestDeltaRangerMergeCommit.scala` and `T/TestDeltaRangerMergeRecovery.scala`; complete command/native helper integration and target binding in `J/DeltaRangerRegistrationBindings.java`. Reuse existing journal path/config and metadata client primitives; do not change core/API/server/client or reinterpret append's Write/add-only intent.

**Interfaces:** run freezes exact Merge actions and checks authorization immediately before commit; journal binds command/source interval/target/txn identity and records exact native publication before registration reconciliation. Neither a journal row nor SetTransaction alone authorizes replay.

- [ ] RED `mergeIntentRejectsForeignActions`: actual AddFile/RemoveFile/AddCDCFile/SetTransaction plus Merge parameters accepted only from the owned native phase; changed actions, foreign paths/log/UUID or copied attempt reject. Compare native final row multisets and CDF-enabled target events.
- [ ] RED `postHelperRevokePreventsPublication`: apply completed newer source/target policy download after native files generated; commit denied, target committed version/rows unchanged, orphan files recorded separately. Repeat identity/config/schema drift at the same boundary.
- [ ] RED `duplicateRequiresAuthorizationAndIdenticalIntent`: native txnAppId/txnVersion with autoReset=false; authorized same intent creates no second publication, denied duplicate fails, changed interval/target/clauses under same identity fails. Missing idempotency pair cannot be advertised as safe automatic replay.
- [ ] RED `commitAcknowledgementFailureReconcilesWithoutReplay`: inject failures after native publication/before registration and after reconciliation/before caller success; inspect exact commit evidence and recover publication status without executing row changes twice. Unknown/mismatched evidence remains explicit failure; checkpoint is unchanged until success.
- [ ] Cover no-op/zero-visible-source success, native constraint/ambiguity/missing-history failure and same-table writeback. Verify registration/native version, hidden rows, unrelated sources and caches after close; closed authority cannot run another action. GREEN focused recovery/commit plus affected append publication/idempotency tests. Commit `feat(delta): validate merge publication and recovery` after review.

## Task 5: Qualify the full pipeline on four pinned runtimes

**Files:** Create `A/cdf_merge_live.gradle`, `A/cdf_merge_live.py`, `A/cdf_merge_live_checks.py`, `A/test_cdf_merge_live_checks.py`, `T/DeltaRangerLiveCdfMerge.scala`, `T/TestDeltaRangerLiveCdfMerge.scala`; extend existing CDF package helpers only to package/select the new thin app with explicit source/artifact binding. Keep accepted read/append assertions intact.

**Interfaces:** `cdf_merge_live.py --runtime <manifest> --inputs <private-inputs> --out <fresh-directory>` provisions isolated fixtures/identities and emits original JSON evidence. `cdf_merge_live_checks.verify_result(result: dict) -> None` raises on incomplete/incorrect evidence. The new Gradle init retains existing pins/memory settings and registers a thin JAR containing the MERGE app and explicitly required existing helpers; extend the packager with that app selection without changing old app contents. Checker validates original outcomes, native oracles and checkpoint states, never boolean summaries alone.

- [ ] RED checker rejects missing pair/provider pin, child-only success, absent policy download, altered target/source row bag, lost hidden row, extra commit, false checkpoint advancement and missing same-table result; permutations preserve multiset equality.
- [ ] First pair runs real Ranger 2.8.0/MinIO public SELECT/MODIFY grant/revoke/regrant plus positive key/metadata masks and target filters/masks. Prove named SQL MERGE, distinct destination, same-table writeback, INSERT/UPDATE/DELETE, native duplicate handling, empty interval, column deny, ambiguous merge and post-helper revoke. Inspect actual target log/actions, original source invariance where distinct, registration persistence/restart and application checkpoint behavior.
- [ ] Run all new/affected focused suites and the same fresh original pipeline phases sequentially for all four pairs. Retain exact pins, original outer/child exits, source/JAR/provider bindings and runtime observations. Reuse an already final first cell with proven identical inputs; do not rerun the closed whole append/CDF matrices.
- [ ] One complete package per changed runtime; retain unique source inputs/artifacts, share immutable proven-equal blobs rather than duplicate evidence trees. Scope failures precisely and rerun only affected checks before final qualification. Commit `test(delta): qualify governed cdf merge with Ranger 2.8` after review.

- [ ] Publish `docs/delta-cdf-merge.md` and `docs/delta-spark-parity.md`; update integration status, module/acceptance READMEs and this plan with exact local qualification and remaining limits. Obtain one integrated review of Tasks 1–5, resolve concrete findings with targeted checks, verify artifact bindings and all four original results, commit closure and push the existing Gravitino branch as already authorized. Verify remote SHA; retain evidence/worktree; no PR. Separate-worker deployment remains unqualified until the deferred demo plan completes.

## Deferred deployment plan

The former Task 6 is moved to [the separate catalog-demo plan](2026-10-22-delta-cdf-merge-demo.md)
at the user's request. It is not required to close this five-task local qualification
milestone. No separate-worker/cluster deployment claim may be made from this plan.

## Inline plan review

The five tasks map to native feasibility/SQL frontend, policy semantics, executor
composition, transaction/recovery and four-pair live qualification. Deployment
is explicitly deferred to its own plan.
Every review-focus item has named tests. The first proof explicitly covers the
highest-risk native APIs before expensive packaging; later tasks cannot treat
source inspection as execution evidence. Read semantics are confirmed; proposed
column-write demands and the bounded native orchestration are visible in the spec
for task-by-task execution. Implementation method remains subagent-driven.

## Deferred DataFrame builder plan

The user selected named SQL MERGE first after Task 1 proved that the stock
DeltaTable.forName route cannot resolve canonical Gravitino names and loses authored
assignment provenance before the available complete-command hook. A companion named
DataFrame builder is tracked in [its own plan](2026-10-22-delta-cdf-merge-builder.md).
It is not a gate for this SQL milestone. Native trait linkage passed on both Delta
pins; installed SQL remains unimplemented until Task 1's positive and ownership
tests pass. The characterization does not complete Task 1.
