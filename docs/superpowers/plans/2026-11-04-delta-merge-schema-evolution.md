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

# Automatic Additive MERGE Schema Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. The controller owns staging/commits; workers do not run Git mutations, indexing, services or subagents.

**Goal:** Atomic governed additive MERGE through named SQL and the classic DataFrame companion, with exact publication and duplicate reconciliation without replay.

**Architecture:** Native Delta resolves assignments/final schema and stages metadata inside the same native MERGE transaction. Extend the current finite receipt/helper/proof/publication path; retain versioned durable evolution evidence for fresh duplicate recovery. Gravitino's existing refresh/CAS publishes the resulting columns.

**Tech Stack:** Spark 3.5.4 / 3.5.6, Delta 3.3.0 / 3.3.2, Java 17, Scala 2.12.21, Ranger 2.8.0.

**Spec:** `docs/superpowers/specs/2026-11-04-delta-merge-schema-evolution-design.md`.

**Baseline:** `5bb3af7dd4d5cd54d4c1b45e705fc83abff72998`. Worktree `codex/spark-hub-adoption-plan`. Spark Hub worktree is read-only in this plan. Complete tasks sequentially; no repeated approval prompts. Checkpoint push is already authorized after bounded closure; no PR/merge/deploy.

## Global Constraints

- Glue is completely replaced; no Glue compatibility layer is required. Adapt spark_hub to existing Gravitino catalog and authorization contracts first, and keep custom connector development limited to necessary Delta gaps.

- Target Spark 3.5.4 and 3.5.6, Delta 3.3.0 and 3.3.2, Java 17, Scala 2.12.21, and actual Ranger server and client 2.8.0.
- Use S3-compatible storage; local acceptance uses MinIO. RustFS is an optional separately recorded storage profile, not evidence of AWS S3 or MinIO equivalence.
- Gravitino owns registration. Delta owns native files, log actions, transaction semantics and table features. Do not add a second authoritative metastore.
- Do not modify Gravitino core/API/server/client/storage schemas, fork Spark or Delta, or build a replacement transaction engine.
- Use the current trusted-stock-Spark execution model. Preserve original source authorization and operation-boundary checks; do not introduce exact per-stage authorization or custom adaptive execution.
- Preserve existing Ranger row filters, masks, column permissions and canonical resources. Masks may affect keys and CDF metadata. Do not require blanket unmasking or introduce raw-key/raw-row bypasses.
- Destination row filters retain read semantics, not a new insertion/update constraint. A filtered-out target row does not automatically suppress an authorized insert.
- One writing job per dataset is enforced by the platform. This does not imply that sources cannot advance while downstream jobs run.
- Preserve platform operations and options in the acceptance requirements. Do not silently drop schema options, disable auto-compaction, delete checkpoints or weaken policies to obtain a passing result.
- Classify every outcome as committed, not committed, or unknown using native evidence. Do not automatically replay an unknown or known-committed write.
- Keep the closed append-AQE milestone separate. Do not rerun unchanged successful matrices for documentation, planning or checker-only changes.


## Review Focus

- Disabled autoMerge with extra requested source fields: native assignments ignore extras, original source SELECT demands remain. Task 1 and Task 3 pin both.
- Same-schema autoMerge=true and native zero-row/metadata-only outcomes: actual schema change controls ALTER/Metadata action, not the flag or row count. Tasks 1 and 3 pin both.
- Retry after schema evolution, including same-registration CDF history crossing the change: retain original ALTER and exact intent, never materialize/replay source. Tasks 2 and 3 pin this.
- New-column UPDATE plus target row filtering/masking: no invented target SELECT on absent fields and no raw key/value bypass. Task 3 pins this.
- Exact native publication followed by registration/checkpoint/recache/journal failure: retain committed evidence through cleanup; unproved invocation is unknown. Task 3 pins this and Task 4 verifies retained native evidence.

## File responsibilities and verification convention

Scala production/test files below live under `spark-connector/v3.5/spark-delta-ranger/src/{main,test}/scala/org/apache/gravitino/spark/connector/delta/ranger/`. Java bindings/tests use the matching `src/{main,test}/java` package. Prefixes `main/`, `test/`, `java/` below refer to those exact directories. Public documentation paths are repository relative.

`DeltaRangerMergeSchema` is pure native-final-schema expectation, not authority. `DeltaRangerMergeEvolutionIntent` is immutable durable comparison data, not an execution owner. `DeltaRangerMergeJournal` owns private durable encoding/state. Existing receipt/command/source/write/commit owners continue to own execution. Keep unrelated APPEND/CDF/read semantics unchanged.

Use Java 17 at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`; `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'`; Gradle task `:spark-connector:spark-delta-ranger-3.5:test` with exact `--tests` selectors, `-PdeltaSparkVersion=<pin> -PdeltaVersion=<pin> -PskipITs --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g`. Retain logs, fresh JUnit XML, start/end/exit status, source hashes, runtime origins and primary native evidence beneath this plan's SDD workspace. Never report stale XML after compile failure as a passing run. Run scoped Spotless checks/apply for touched modules. No broad unchanged APPEND matrix.

## Task 1: Native MERGE final-schema expectation and controls

**Files:** Create `main/DeltaRangerMergeSchema.scala` and `test/TestDeltaRangerMergeSchema.scala`. Reuse `DeltaRangerAppendSchema.scala` unchanged. Reuse stock mode of existing `NativeSelectFixture.withSpark`; do not alter governed execution yet.

**Interfaces:** `DeltaRangerMergeSchema.derive(spark: SparkSession, base: Metadata, protocol: Protocol, finalSchema: Option[StructType], evolutionEnabled: Boolean): DeltaRangerAppendSchema.Intent`. Require native final schema, reject disabled actual schema change, and pass native final target schema to existing APPEND finite expectation with internal mergeSchema=true only after the independent effective MERGE check. No transaction ownership, user options or metadata staging in this adapter.

- [x] Add failing `TestDeltaRangerMergeSchema` cases with literal expectations: enabled updateAll/insertAll from source id/value/extra yields old fields plus nullable extra; disabled ignores extra and commits existing fields only; enabled matching schema is non-evolving. Resolve real native MERGE plans and compare the adapter expectation to actual independently executed stock MERGE metadata, protocol, one next-version log and complete row bags (matched, inserted, untouched old/null).
- [x] Add native final-schema controls for source order differing from target, explicit same-name addition excluding unused extras, existing NOT NULL and ordinary field metadata retention, nullable new literal, missing required source field when disabled, and unsupported nested/type changes refused by finite adapter. Native SQL and stock DeltaTable builder controls must be identified separately; no governed claim from these tests.
- [x] Run exact test before implementation and retain meaningful red evidence. Implement the small adapter using native resolved final schema and existing pure helper, never a second merger. Native controls may use fixture-authorized stock paths only before governed extension; product code has no path fallback.
- [x] Run the test on Spark 3.5.6 with Delta 3.3.0 and 3.3.2. Copy native log/rows/schema evidence to a fresh directory via a narrowly scoped test evidence option. Record actual runtime classes and source hashes. Verify new tests and scoped formatting; report exact signatures for Task 3.
- [x] Controller packages full staged diff, obtains fresh task review, commits `feat(delta): derive native merge schema intent`.

## Task 2: Versioned durable evolution intent without execution authority

**Files:** Create `main/DeltaRangerMergeEvolutionIntent.scala` and `test/TestDeltaRangerMergeEvolutionIntent.scala`; modify `main/DeltaRangerMergeJournal.scala`; add focused journal tests in `test/TestDeltaRangerMergeRecovery.scala` or a small dedicated `test/TestDeltaRangerMergeJournal.scala` where Spark execution is unnecessary. Do not enable evolving writes yet.

**Interfaces:** Add `DeltaRangerMergeJournal.peek(directory: Path, identity: Option[(String, Long)]): Option[Entry]` for strict parsed evidence only. Preserve existing constructor/format-1 fingerprint and state behavior; add an optional version-2 evolution envelope for evolving commands. `Entry` exposes `evolution: Option[DeltaRangerMergeEvolutionIntent]`. Existing prepare/committed/reconciled/unknown methods retain their contracts. Evolution intent stores authored command identity, canonical stable semantic binding, frozen original/expected Metadata and Protocol, and original ALTER/new-column UPDATE obligations. Provide `requireCurrent(candidate: DeltaRangerMergeEvolutionIntent): Unit` or an equivalently narrow exact comparison entry point; it validates data, never issues authority. Carry exact final private signature in the task report for Task 3.

- [x] Write failing tests for strict format-2 roundtrip, original/expected metadata detachment, unchanged old fields/protocol, exact authored/resolved/governed/source/subject/settings binding, same expected current schema after evolution, rejection of a different command/interval/registration/policy/source projection, and later schema drift. Retain and assert format-1 byte/fingerprint compatibility using existing legacy fixture.
- [x] Define the evolution envelope as versioned stable authored plus resolved/governed semantics, captured before native PreprocessTableMerge inserts defaults/copy-forward expressions. Use source output occurrence/target field paths and exact types in semantic identity, not generated ExprIds; preserve predicates/clause ordering/literals. Include full normalized expected Metadata schema and exact original transaction identity/base version; native analysis may mark new fields nonnullable before native metadata makes them nullable, so pin that original/fresh retry equivalence without relaxing source-output identity. Cross-check proof readVersion/committed version/native txn action against the retained transition during recovery; proof is not validated by envelope/hash alone. For same-registration source, normalize only the retained original-to-expected own schema transition in binding and leaf shape; explicit projected payload and referenced field types/metadata remain exact. Do not remove schema from general bindings or accept arbitrary caller-supplied hashes as authority.
- [x] Implement private journal parsing/encoding with strict versioned outer/native metadata/protocol/transition/proof fields and semantic root shapes, bounded size, duplicate/trailing JSON rejection, unchanged file permissions/atomic ownership and durable state machine. Nested semantic trees are opaque comparison-only data that must equal independently generated fresh admitted semantics; no duplicate recursive expression decoder or executable-plan reconstruction. `peek` must not create/claim/advance a journal file or authorize any operation. A malformed/unknown record never becomes a fresh attempt. Reject matching-schema candidates masquerading as a different evolving command.
- [x] Run focused tests once on Spark 3.5.6 / Delta 3.3.0; inspect retained format-1 compatibility and proof/command tampering assertions. No new services or full matrix for pure encoding.
- [x] Controller obtains fresh task review and commits `feat(delta): retain additive merge recovery intent`.

## Task 3: Wire governed atomic MERGE evolution, publication and recovery

**Files:** Modify `main/DeltaRangerMergeReceipt.scala`, `DeltaRangerMergeCommand.scala`, `DeltaRangerMergeCommit.scala`, `DeltaRangerMergeWrite.scala` / `DeltaRangerMergeNative.scala` only for proven changed helper recipes, `DeltaRangerCdfReadReceipt.scala` for retained-record recovery-only deferral, and `java/DeltaRangerRegistrationBindings.java`. Use Task 1/2 helpers. Add `test/TestDeltaRangerMergeSchemaEvolution.scala` and focused additions to existing ownership/policies/recovery/companion/partitioned/AQE suites. Use a small `test/MergeSchemaEvolutionFixture.scala` only if needed for shared native evidence/setup. Keep public companion version/signatures unchanged.

**Interfaces:** Receipt retains Task 1 schema intent plus optional Task 2 recovery candidate; native command preserves exact effective flag/migratedSchema. Capture authored `MergeIntoTable` before native resolution using an explicit internal handoff, not a thread-global mutable capture. Receipt owns distinct before/staged/closed/commit states and original ALTER obligations. Java adds a finite MERGE publication overload accepting expected Metadata, with old overload retaining old behavior; it validates only original or exact proved next-version registration with expected logical schema and all other receipt values unchanged. Shared `requireCompatiblePublication` remains unchanged. Commit takes expected metadata through existing receipt/permit, freezes optional native Metadata prefix, and passes expected state to `DeltaRangerCommitMetadata`. Any diagnostic outcome is private immutable evidence, not authority.

- [ ] Add failing actual installed SQL and companion updateAll/insertAll tests for wider explicitly projected bounded CDF payload, matched+new+untouched rows, source-derived new value, fresh named schema, exact schema/data single commit, disabled ignored extra with original SELECT demand, and enabled matching-schema operation without ALTER. Establish autoMerge and constraintPropagation=false before source/handle analysis and restore afterward. Retain context-drift rejection test; do not relax settings provenance.
- [ ] Wire native schema evolution in its stock position: authorize ALTER before duplicate work/mutation; retain exact base transaction before inherited native updateMetadata; require exact expected pending state afterward before source/touched/write/commit operations. Native same-schema true remains no Metadata change; native metadata-only evolution is not skipped. Preserve exact native source/target occurrences and existing retry attempt ownership.
- [ ] Add permission/semantics cases: added source SELECT and target UPDATE denial; ALTER deny/time-based revoke at existing boundaries; target mask reads and row-filter read semantics including hidden-target insert; omitted old raw fields; added alias from a masked source; false-mode unused extra still requires original SELECT. Assert no native commit on precommit failures and retained orphan files where native helpers generated them.
- [ ] Extend exact commit/publication checks and tests for missing/extra/reordered/foreign Metadata, Protocol drift, changed pending metadata, wrong owner/transaction/native UUID, expected registration schema mismatch, native constraint/duplicate-match failure, CDF pre/postimages, native version-10 checkpoint reconstruction and partitioned/AQE execution. Preserve matching-schema target-cache behavior; qualify any schema-changing target-cache admission before claiming it.
- [ ] Add version-2 recovery execution: inspect existing evidence before source receipt issuance; construct a source policy view with deferred native payload ONLY for a retained format-2 candidate and its exact owner. Validate fresh context/original demand/masks/filters/bindings and intent. Forbid source/openMergeExecution/materialization for that owner. Native duplicate marker plus exact original commit/proof/digest and current named registration is required; absent marker or unknown/malformed/mismatched record fails before source work. Fresh writes and ordinary CDF retain eager native history validation.
- [ ] Prove distinct-source and same-registration evolving retry, lost commit/registration/journal/recache acknowledgement, later compatible data-only advancement with a contiguous log suffix containing no subsequent Metadata/Protocol (even reverted schema), retained original ALTER/new-column UPDATE denial, changed interval/clause/projection/schema/UUID refusal and no new files/source materialization/log version on reconciliation. Preserve legacy fixed-schema recovery tests and ordinary CDF history rejection. Retain committed versus not-committed versus unknown evidence through cleanup and preserve original/suppressed failures; no auto replay.
- [ ] Run new integration cohort on Spark 3.5.6 / Delta 3.3.0 plus focused affected fixed-schema MERGE/CDF ownership/companion regressions, inspect fresh XML and primary native evidence. Scoped format/static checks and exact source/runtime hashes. Record any native unqualified branch explicitly without silently changing options.
- [ ] Controller packages full diff, obtains fresh review, commits `feat(delta): enable governed additive merge evolution`.

## Task 4: Four-pair qualification and bounded checkpoint

**Files:** Extend only fixtures/acceptance evidence needed by Task 3; update `docs/delta-cdf-merge.md`, create `docs/superpowers/specs/2026-11-04-delta-merge-schema-evolution-results.md`, update this plan and `docs/superpowers/plans/2026-10-29-spark-hub-adoption.md` with actual outcomes and outstanding scope.

**Interfaces:** Freeze the meaningful Task 1/2/3 cohort by exact JUnit method identities and source hashes. Reuse qualified source-identical method/pair evidence; run remaining changed cohort sequentially for all Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 combinations, with actual Java/Scala/Ranger origins. Reused evidence is labeled, not counted as a fresh run.

- [ ] Retain fresh logs/XML/runtime/source hash manifests and primary native row/schema/log/receipt/outcome evidence per pair; independently verify exact method cohort, failures/errors/skips and requested pins. No external services or fixtures need be rebuilt merely for this installed matrix. Resolve actual failures before claiming that cell.
- [ ] Record named SQL and companion support, disabled-extra semantics, permission/recovery guarantees and limitations in public docs. Explicitly state source remains bounded CDF, proper settings scope, no engine/core fork, API version 1, no new deployed Ranger/MinIO result, nested/type/full schema and broader sources remain unqualified. Do not claim Spark Hub integration from a connector-only test.
- [ ] Self-review against the design and selected four-workstream roadmap. Controller obtains fresh task review and commits documentation/evidence references; then obtains one whole-slice review from baseline, using retained evidence without redundant test reruns.
- [ ] On clean final review, verify branch/worktree and push existing branch under standing user authorization; verify exact remote SHA. Preserve evidence/worktree. Continue selected schema requirements or the next workstream as appropriate; this checkpoint does not declare all four complete.
