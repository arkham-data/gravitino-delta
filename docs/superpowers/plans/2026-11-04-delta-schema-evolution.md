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

# Automatic Additive APPEND Schema Evolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Execute one task at a time under the user's selected four-capability scope; no repeat permission gate for reversible connector work within that scope.

**Goal:** Enable atomic native additive APPEND with correct Gravitino schema observation and Ranger permissions.

**Architecture:** One finite native-derived schema intent carried through existing append ownership/helper/publication paths. Reuse provider load-time projection and existing metadata/checkpoint recipes; no core/engine or lifecycle protocol changes.

**Tech Stack:** Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, Java17, Scala2.12.21, Ranger2.8.0; existing installed and native fixtures.

**Spec:** [Automatic additive APPEND](../specs/2026-11-04-delta-schema-evolution-design.md).

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

- Schema option admission is mistaken for actual evolution: require one native WRITE containing exact Metadata plus data, old-row nulls and fresh named schema/readback.
- Pending arbitrary metadata borrows evolution authority: retain independently derived candidate; reject changes to old fields/configuration/protocol/identity and to exact published actions.
- UPDATE-only or duplicate execution bypasses ALTER: authorize prospective evolution before duplicate/helper entry using captured context, with revocation negatives.
- Old read/registration/cache bindings silently accept newer schema: permit only proved own publication, leave shared readers strict, reject unqualified evolving cache histories before mutation.
- A native commit is replayed after observation failure: exact committed/unconfirmed or uncertain classification, preserved cause/files and no automatic retry.

## Task 1: Native-derived finite schema intent and named controls

**Files:**
- Create `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendSchema.scala`.
- Create corresponding `src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerAppendSchema.scala`.
- Extend `spark-connector/v3.5/spark-delta/src/test/java/org/apache/gravitino/spark/connector/delta/TestDeltaNativeAdapter.java` only with focused named additive controls, reusing its existing registry/native capture fixture.

**Interfaces:** `DeltaRangerAppendSchema.derive(spark: SparkSession, base: Metadata, protocol: Protocol, source: StructType, options: Map[String,String]): Intent`. Private finite `Intent` has `baseMetadata`, `expectedMetadata`, `protocol`, `evolves`, and `requirePending(metadata: Metadata, protocol: Protocol): Unit`. Constructor private; no public user API or customizable callback. It is a schema expectation, not authorization or a transaction owner. Exact private signature refinements need to be recorded for the next task.

- [x] **Step1: Add focused red tests and native controls.** Cover identical-schema intent preserving existing profiles; top-level new-field native nullability; native mergeSchema/session precedence; existing-prefix name/order/type/directional-nullability refusal; null/collision/unsupported profile/metadata refusal; immutable exact pending metadata/protocol matching. Native named saveAsTable and insertInto controls write actual old/new rows and compare exact Metadata/Protocol/configuration and WRITE actions. Record genuine behavioral reds separately from missing-type/compile/setup failures; no artificial change to existing production behavior merely to manufacture a red.
- [x] **Step2: Implement finite native schema derivation.** For evolving input use public `SchemaUtils.typeAsNullable`, `DeltaColumnMapping.dropColumnMappingMetadata`, `SchemaMergingUtils.mergeSchemas` with implicit conversions/type widening disabled, then `SchemaUtils.removeUnenforceableNotNullConstraints`; validate unchanged old fields and allowed additions. Native expectation is `base.copy(schemaString = expectedSchema.json)`. Never mutate transaction or create a custom schema merger. Do not drop NullType before metadata merge; reject it in this first evolving profile. Matching-schema behavior must not acquire evolving-only restrictions.
- [x] **Step3: Verify and freeze.** Run focused new intent class and named native control on Spark3.5.6 with both Delta pins using existing Java17 module commands and bounded JVM settings. Preserve actual XML/logs/exits/source snapshots/pins before next run; module Spotless and diff checks. Report exact contract and remaining unwired state. Controller stages/reviews/commits; implementer does not mutate git/index or spawn agents.

## Task 2: Finite schema-change permission and registration continuation

**Files:** `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java`; extend existing `src/test/scala/.../TestDeltaRangerAppendTargetReceipt.scala` and `TestDeltaRangerAppendIdempotency.scala` as appropriate, without duplicating the fixture. Keep existing provider refresh regression as evidence; change provider code only for an actual discovered defect and report before widening.

**Interfaces:** Extend existing `BoundAppendTransaction` with package-private `authorizeSchemaEvolution()` and finite `beginCommit(AppendPublication publicationOwner, Metadata expectedMetadata)`, preserving the old overload by passing snapshot metadata. Existing `BoundAppendCommit` retains final expected metadata/logical schema; `AppendPublication.requirePublication` signature remains unchanged. The Java boundary checks its exact transaction/base, unchanged protocol/non-schema metadata, unchanged old fields and valid nullable scalar additions (mapping none); it does not duplicate Task1 profile/property/recursive feature-metadata qualification; passing a Metadata value alone grants no authority. Existing `logicalSchema(StructType)` conversion supplies expected named schema.

- [x] **Step1: Add failing ownership/permission tests.** Real native metadata candidate requires ALTER in addition to existing UPDATE; UPDATE-only refused; unrelated/stale/replaced target and changed subject/policy/settings refused. Own exact nativeVersion+schema advance accepted only after publication proof; missing proof, wrong schema/identity/generation/protocol, regression to old publication after advance and generic read reuse refused.
- [x] **Step2: Implement the narrow continuation.** Keep precommit anchor/old schema exact. Record schema authorization on the exact owner before accepting changed Metadata, and reauthorize ALTER with Instant.now at existing validation boundaries; policy-stamp equality alone is insufficient for time-sensitive decisions. After own commit proof, compare expected logical schema and exact receipt/coordinate identity with only version allowed to advance. Avoid calling the shared old-schema comparator on the proved candidate; keep that comparator unchanged for all other consumers. Retain reader leases, repeated observations and monotonic advancedPublication guard. Authorization is on captured subject/engine/policy stamp and canonical table resource.
- [x] **Step3: Run focused tests/format/review.** Use one supported pair first, include matching-schema/ordinary-read regression controls. Report method inventory and exact changes. Do not expose an arbitrary schema-update or transaction callback. Controller review/commit before wiring Task3.

## Task 3: Wire atomic automatic evolution through the existing APPEND path

**Files:** main Scala package `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`: `DeltaRangerAppendCommandBinding.scala`, `DeltaRangerAppendV1Input.scala`, `DeltaRangerAppendCoordinator.scala`, `DeltaRangerAppendHelper.scala`, `DeltaRangerAppendCommit.scala`; a narrow extraction/mode in `DeltaRangerCdfCacheMaintenance.scala` reuses existing target-dependency discovery for evolving writes even when AQE is disabled. Keep matching-schema maintenance behavior unchanged. Extend existing `TestDeltaRangerAppendV1Input`, `TestDeltaRangerNativeGovernedAppend`, `TestDeltaRangerAppendIdempotency`, and relevant commit/source fixtures; no new acceptance framework. Update `docs/delta-native-governed-append.md` with a distinct new bounded result only after tests.

**Interfaces:** Binding retains Task1 Intent from original native target snapshot, source schema, captured options/settings; original and successor revalidation preserves source identity/schema and candidate expectation. Both binding.hasBeenExecuted and coordinator authorize Task2 ALTER for an evolving intent before their respective scalar duplicate reads; coordinator also does so before helper work and passes the retained intent to helper/commit. Cover both paths explicitly: hasBeenExecuted can suppress execution before coordinator runs. Add optional intent parameters/default old behavior only where internal existing callers need compatibility; no public API. Retain a small private append outcome carrying exact existing publication witness plus native transaction/version identity from commit through coordinator to binding. It is diagnostic evidence only, grants no renewed authority, and survives owner cleanup; skipped/no-commit stays distinct. Classify invoked-but-unproved exceptions as uncertain, and errors after proved commit (including cleanup/maintenance/recache) as committed with observation unconfirmed. Preserve original causes and suppressed proof/cleanup failures. No durable journal or generic mutation framework. `DeltaRangerAppendCommit` passes expected metadata to Task2 beginCommit and existing `DeltaRangerCommitMetadata` callbacks.

- [x] **Step1: Add actual installed failing evolution tests.** Both named writers with a governed wider source, old target rows, exact writer metadata, masks/filters and source/target denial. Source remains existing admitted grammar. Include ordinary and existing qualified CDF-enabled targets, unchanged partitioning and a stock AQE exchange. Require one WRITE with exact Metadata and data, old rows null for additions, same native UUID/registration identity and fresh named schema/readback.
- [x] **Step2: Wire candidate intent and helper recipes.** Allow only matching-prefix additive input under native merge-option/session precedence. Preserve original-demand/source receipt, actual V1 input, option snapshot, attributes and fallback checks. Derive helper normalization/default/stats/invariant expectations from expected metadata without updating native transaction; let native WriteIntoDelta perform updateMetadata and data generation, then require exact pending state.
- [x] **Step3: Extend exact commit/publication proof.** Keep submitted helper actions add/txn only. Verify optional native-injected exact Metadata before unchanged submitted actions, unchanged Protocol and original CommitInfo intent. Reject foreign/extra/missing metadata/action/order/tag/transaction identity. Candidate metadata supplies native checkpoint reconstruction. Do not permit arbitrary metadata/config/partition/type/protocol changes.
- [x] **Step4: Complete failure/cache/permission regressions.** Authorize ALTER before duplicate in both binding.hasBeenExecuted and coordinator, and before helper for evolving intent; test UPDATE-only, explicit false versus session true, session-enabled evolution, added-column source denial, source/ALTER revoke after helper, arbitrary pending mutation, duplicates in each scalar/coordinator entry point and native invariant failure. On a CDF target, include a reserved CDC-column addition refused by native commit: no committed version/schema/data, generated orphan files retained and no replay. Native CDF validation is commit-time, so do not falsely assert it happens before file generation or duplicate native validation solely to move the boundary. Reject schema-changing target cache histories before mutation using existing discovery regardless of CDF/AQE settings; ordinary non-AQE prepare currently returns before enumeration, so testing only its Option result is insufficient. Test ordinary AQE-off, ordinary AQE-on and qualified CDF cache negatives; retain matching-schema cache support and current source-only cache admission. Fresh plans see evolved schema; old plans/receipts remain strict. Add committed-but-named-observation-failed and invoked-unconfirmed outcome tests with preserved cause/evidence and no replay, using existing fixture injection seams rather than a new production test callback. Check basic existing named CDF continued read on an evolved CDF-enabled target if that API is in the current qualified profile; any incompatibility is a real gap to report, not silently ignored.
- [x] **Step5: Verify/format/document/review.** Run exact focused installed methods on Spark3.5.6/Delta3.3.2, existing provider refresh column-identity regression and relevant matching-schema/AQE/checkpoint/cache controls. Preserve original evidence and frozen source inventory. No new deployed/server claim; scope limits explicit. Controller reviews and commits.

## Task 4: Cross-version installed qualification and checkpoint

**Files:** create `docs/superpowers/specs/2026-11-04-delta-schema-evolution-results.md`; update public append continuation and plan status. Evidence in this plan workspace; no product changes during qualification.

- [x] **Step1: Freeze required method inventory and producing source hashes.** Reuse source-identical first-pair evidence, not stale XML/cache output. Include native/runtime origins and exact intent/publication/permission cases; no passing-count-only gate.
- [x] **Step2: Execute remaining three pairs.** Existing Java17 Gradle commands, `:spark-connector:spark-delta-ranger-3.5:test` with exact selected methods, `-PdeltaSparkVersion=3.5.4|3.5.6 -PdeltaVersion=3.3.0|3.3.2 -PskipITs --max-workers=1`, bounded daemon heap. Fresh existing absolute evidence directory per run; original command/log/exit/XML copied before next run. Require actual test START and loaded requested JARs. No unchanged historical matrix replay.
- [x] **Step3: Record bounded installed outcome.** Independently exact-match inventory, failures/errors/skips, hashes, native schema/data versions, registration observations, postcommit failures and no replay. Ranger2.8 client fixtures do not prove new deployed Ranger/MinIO; retain consolidated live gate explicitly. Preserve first-profile exclusions and remaining actual MERGE evolution before proceeding to governed creation.
- [x] **Step4: Review/finalize/push checkpoint.** Fresh task and whole-slice reviews, proportional checks, commit/push existing branch under standing user authorization. Retain worktree/evidence; no PR/merge/deploy. User's other workstreams remain deferred.

## Self-review

Native schema and existing registration projection remain authoritative. Task1 produces a narrow expectation, Task2 authorizes/proves only its own transition, Task3 wires it into native execution, Task4 freezes actual compatibility. Every Review Focus has concrete tests. This does not implement a second schema store or replace native transaction semantics; ADD COLUMNS and full schema/feature support are not falsely claimed. App/MERGE follow-up remains explicit.

## Installed checkpoint closure

All four tasks passed independent review; the combined review approved the bounded installed checkpoint. The exact60-method cohort passed all four pinned pairs (169 new executions plus71 explicitly source-identical reused method-pairs). Qualification and limits are in [the results](../specs/2026-11-04-delta-schema-evolution-results.md). Remote checkpoint verification is retained in the plan ledger; worktree/evidence are preserved. Actual additive MERGE and the consolidated deployed Ranger2.8/MinIO gate remain open, so the schema track and M2 are not closed.
