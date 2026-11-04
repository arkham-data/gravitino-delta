# Governed CDF additive-history implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Consume bounded named CDF intervals across the finite additive schema changes already supported by governed writes.

**Architecture:** Keep Delta's stock CDF reconstruction and captured current read schema. Extend the existing history owner's admission with a finite pure schema check, preserving Ranger demand and receipt ownership.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger 2.8.0.

**Spec:** [Governed CDF additive-history design](../specs/2026-11-04-delta-cdf-additive-history-design.md).

## Global Constraints

The design's Global Constraints bind every task verbatim. Gravitino owns registration; native Delta owns reconstruction. No core/API/server/client/storage-schema change, engine fork, Glue compatibility, raw-path fallback, new reader or consumer-state project. No read ALTER obligation or writer-option dependency. Companion API remains version 1. Existing original SELECT/masks/filters, settings/ownership, completeness/profile checks and recovery-only payload refusal remain. Preserve evidence/worktree and distinguish installed Ranger client 2.8.0 qualification from the deferred consolidated server 2.8.0/MinIO gate.

## Review Focus

1. An addition after endingVersion changes the selected read schema but must not include later rows or inspect unrelated suffix log files — Task 1 stock and Task 2 bounded read controls.
2. A changed-then-restored schema looks compatible at head but contains a nonadditive transition within the interval — Task 1 pure and Task 2 native-history refusals.
3. Added null historical fields bypass SELECT/masking/filter demand or require ALTER unnecessarily — Task 2 policy cases and exact stock oracle comparisons.
4. A fresh supported historical read accidentally grants recovery-only receipts payload authority or accepts schema drift — Task 2 ownership/recovery regressions.
5. Native CDF output feeds a fresh MERGE whose selected new field and original authorization must survive projections/materialization/AQE — Task 2 SQL/companion controls and Task 3 pair matrix.

## File map and build

`main/` and `test/` below mean Scala package directories `spark-connector/v3.5/spark-delta-ranger/src/{main,test}/scala/org/apache/gravitino/spark/connector/delta/ranger/`.

Use Java 17 at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`; `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'` and Gradle `--max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g --console=plain`.
Focused command: `./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '<exact class or method>' -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.0 -PskipITs` plus the above flags. Use module-scoped Spotless and necessary static checks. Retain exact commands/exits/XML/source/runtime/native evidence in this plan's ignored SDD workspace. Native evidence roots must be fresh; never overwrite previous artifacts.

## Task 1: Native read controls and finite additive predicate

**Files:** Create `main/DeltaRangerCdfSchema.scala`, `test/TestDeltaRangerCdfSchema.scala`, `test/TestDeltaRangerNativeCdfSchema.scala`; reuse `test/NativeSelectFixture.scala` and `test/DeltaRangerCdfReadFixture.scala` with minimal shared fixture changes only if necessary.

**Interfaces:** `private[ranger] object DeltaRangerCdfSchema` exposes `requireCompatible(previous: StructType, next: StructType): Unit`, returning normally for identical schemas and qualified additive transitions, throwing a descriptive `SecurityException` for unqualified differences. It grants no authority and consumes no Spark/session/writer options. Exact identical schema retains existing admission rather than acquiring new evolution restrictions. The history owner will invoke it between observed versions and against captured read schema in Task 2. Native stock controls use no governed extension.

- [ ] Add pure RED tests for exact unchanged prefix, nullable scalar additions, multiple additions, unchanged nested old fields and ordinary comment metadata. Reject case duplicates, invalid/virtual names, nested additions, type/drop/rename/reorder/nullability/old metadata changes, nonnullable or engine-metadata additions; preserve matching-schema behavior. Confirm genuine missing predicate failure, then implement only the small finite check.
- [ ] Add stock controls named `additionInsideRangeUsesCapturedCurrentSchema`, `additionAfterEndDoesNotAddLaterRows`, `additionAtStartAndMetadataOnlyRange`, `multipleAdditionsPreserveNativeImages`, and `nonadditiveHistoryKeepsNativeCompatibilityChecks`. Use actual Delta native writes/ALTER in isolated temporary fixtures; assert full row multisets/schema, versions and null images. Record what native accepts versus the narrower governed contract; do not force a native rejection for a finite-profile restriction.
- [ ] Run pure suite and stock controls on Spark 3.5.6 with each Delta pin. Capture actual runtime origins, native JSON actions and exact row/schema observations. Preserve native disabled unsafe setting and no governed profile effect. No new services or four-pair pure-test repetition now.
- [ ] Self-review, module format/checks; controller obtains fresh task review and commits `feat(delta): define additive CDF schema compatibility`. No production admission change until Task 2.

## Task 2: Governed historical admission and incremental consumption

**Files:** Modify `main/DeltaRangerCdfHistory.scala`; create `test/TestDeltaRangerCdfSchemaEvolution.scala`; update concrete additive refusal expectations in existing `test/TestDeltaRangerCdfHistory.scala` only where behavior deliberately changes. Reuse existing CDF policy/admission/ownership and MERGE fixtures. Touch `DeltaRangerCdfReadScans.scala`, native reader or receipt/source only for a demonstrated native recipe mismatch, with controller ruling and focused refusal tests.

**Interfaces:** Consume Task 1's pure predicate. Preserve `capture(binding, request, spark): ValidatedInterval` and its ownership/API. Validate each encountered Metadata transition in order from start snapshot and compatibility to captured current schema, while retaining native profile and CDF checks. Keep action inventory limited to inclusive requested versions, no unrelated suffix scanning. Duplicate metadata in a malformed commit must not hide an incompatible intermediate state.

- [ ] Add actual installed RED controls for named CDF spanning an additive APPEND/MERGE-equivalent native history, old nulls/new values and exact oracle rows; include after-end, at-start, multiple and metadata-only ranges. Observe the existing historical schema equality refusal before modifying production admission.
- [ ] Replace only schema equality with finite compatibility checks at the appropriate history boundaries. Keep identity/partition/protocol, native compatibility, complete requested actions and all other profile/settings/file-action checks. Do not change unrelated writer/schema helpers.
- [ ] Add SELECT denial/new-column projection, mask/filter over null and populated values, read without ALTER, fresh revoke/schema drift, copied/foreign receipt and ordinary eager history cases. Include partitioned/AQE reads. Preserve end-stop/unrelated missing log behavior and test changed/drop/re-add/restored metadata inside the requested range fails.
- [ ] Prove fresh named SQL and companion MERGE with CDF input spanning the admitted addition. Include original added-field demand denial, correct masked value and no new authority from deferred recovery-only owners. Preserve the existing settings lifecycle and no automatic replay.
- [ ] Run the new installed cohort once on Spark 3.5.6/Delta 3.3.0 and focused affected CDF completeness/profile/ownership/recovery/source controls; freeze exact successful methods and retain real rows/log/schema/runtime evidence. No mutation from standalone reads. Inspect failures and run only justified follow-ups.
- [ ] Self-review and scoped format/static checks; controller obtains fresh task review and commits `feat(delta): read governed CDF across additive schemas`.

## Task 3: Four-pair qualification and bounded checkpoint

**Files:** Update `docs/delta-cdf-merge.md`, create `docs/superpowers/specs/2026-11-04-delta-cdf-additive-history-results.md`, update this plan and `docs/superpowers/plans/2026-10-29-spark-hub-adoption.md`. Test/fixture edits only for actual failures or necessary evidence.

**Interfaces:** Freeze exact Task 1/2 method identities and producing source/runtime hashes; reuse unchanged qualification with explicit provenance. Complete all Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 combinations sequentially. Stock-native, installed governance and historical reuse are distinct evidence categories.

- [ ] Execute missing exact method/pair cells with fresh evidence roots; inspect actual XML, runtime pins, rows/schema/native actions and original exit. Require no failures/errors/skips and exact identities. Do not rebuild services or repeat unchanged unrelated APPEND/MERGE matrices.
- [ ] Document current-schema/null semantics, bounded additive profile, current policies and context lifecycle, unqualified nested/type/consumer/streaming cases and installed-only evidence. Actual server 2.8.0/MinIO/PySpark/Spark Hub consolidated acceptance remains required. Update selected-workstream status without claiming full Delta support.
- [ ] Controller obtains fresh task review and commits qualification docs, then one whole-slice review from this plan's baseline. Resolve actionable findings with proportional tests.
- [ ] On clean review, push existing branch under standing authorization and verify exact remote SHA. Preserve evidence/worktree; continue next selected schema requirement or governed creation as justified by actual Spark Hub calls.
