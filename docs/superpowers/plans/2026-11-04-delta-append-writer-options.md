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

# Governed Append Writer Options Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. The user's continuation authorizes this connector implementation within the existing architecture; preserve the selected execution method.

**Goal:** Carry three original writer options through matching-schema governed named append with native semantics and precommit state protection.

**Architecture:** One private append-specific validator/snapshot, unchanged low-level session idempotency, existing native helper and exact publication verification. A finite precommit metadata/protocol check protects the unchanged-schema contract.

**Tech Stack:** Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, Java17, Scala2.12.21; existing companion test fixtures.

**Spec:** [Writer options design](../specs/2026-11-04-delta-append-writer-options-design.md).

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

- A duplicate shortcut bypasses malformed-option or source/target permission validation: pin both before suppression.
- A changed option map borrows tags/receipts from a valid command: bind immutable values through optimized V1 fallback.
- Native pending metadata/protocol is injected during prepareCommit: reject effective changes before native commit and verify unchanged head.
- True schema flags or session autoMerge bypass exact schema/source constraints: keep concrete mismatch negatives.
- Writer metadata is changed, ignored or loses precedence: inspect actual log string with differing session metadata through both named APIs.

## Task 1: Implement finite option-aware governed append

**Files:** all paths relative to `spark-connector/v3.5/spark-delta-ranger/` unless fully qualified.
- Create `src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendOptions.scala` for private finite validation/snapshot and empty transaction projection.
- Modify existing `DeltaRangerAppendCommandBinding.scala`, `DeltaRangerAppendV1Input.scala`, `DeltaRangerAppendCoordinator.scala`, `DeltaRangerAppendCommit.scala` in the same main package only at option/commit seams.
- Extend existing test classes `TestDeltaRangerAppendCommandBinding`, `TestDeltaRangerAppendV1Input`, `TestDeltaRangerNativeGovernedAppend`, `TestDeltaRangerAppendIdempotency` in `src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/`; a focused `TestDeltaRangerAppendOptions` may hold finite parser cases. Extend current fixtures with optional writer options; do not duplicate their native/governance harness.
- Add a provenance-preserving test resource under `src/test/resources/append/spark-hub-options.json` copied from the reviewed `spark-delta/src/test/resources/native-create/spark-hub-options.json`. Production must not depend on another module's test resource.
- Modify `docs/delta-native-governed-append.md` with a separately labeled installed-only continuation and remaining live/maintenance limits. Avoid rewriting historical results.

**Interfaces:** `DeltaRangerAppendOptions.validate(spark: SparkSession, options: Map[String,String]): Map[String,String]` returns an immutable preserved map after finite and eager native validation. `transactionOptions` returns the empty map for the validated append snapshot. Internal signatures may be tightened if necessary; no public API. Preserve existing `DeltaRangerNativeIdempotency` behavior unchanged. Use public `OptimisticTransaction.metadata`, `protocol`, `snapshot.metadata`, `snapshot.protocol` for the precommit fence in the existing finite commit owner, immediately before native invocation. No private reflection.

- [x] **Step1: Write and run meaningful red tests.** Consume the resource's exact string-valued writerOptions triple (never reserialize actualOptions or apply unqualified auto-compaction/manifest defaults) and assert admission on both frontends, real writer metadata, eager malformed-boolean refusal, immutable-map/optimized-option changes and precommit pending-state refusal. Run focused new tests on Spark3.5.6/Delta3.3.2 before production edits and retain original logs/XML/source snapshots. A compile error is not behavioral red evidence; establish at least the actual existing option rejection against runnable tests.
- [x] **Step2: Implement the finite validator and option ownership.** Replace only empty-option gates, make the detached immutable snapshot the execution authority and compare retained original/successors to it with locale-independent key admission, preserve all command/source/target guards, and route the full writer map into the native helper while idempotency receives the explicit empty transaction-option projection. Preserve session autoReset=false and native parser errors. Invoke both native boolean accessors before duplicate shortcuts.
- [x] **Step3: Add the finite precommit check.** Inside existing run cleanup, after operation entry and argument validation and before invoked/native commit, refuse changed effective metadata/protocol against snapshot state. Retain existing live owner checks, action allowlist, native commit result and exact published intent. Real pending metadata/protocol tests must assert update succeeds, effective getter differs from snapshot, specific new fence refusal and unchanged log head; earlier native errors or other ownership denials are not coverage.
- [x] **Step4: Complete regression coverage and run focused green tests.** Both named frontends carry actual metadata triple with masks/filters and differing session metadata. Exercise explicit true/false matching-schema cases, autoMerge=true mismatch refusal, both null-default branches and one AQE exchange source using existing fixtures. Assert malformed/unknown/null/case-collision/writer-txn refusal, optimized allowed-option mutation, authorized/denied duplicate, and source/target revoke ordering. Retain relevant schema mismatch guards, including admitted non-nullable-to-nullable and rejected nullable-to-non-nullable with flags enabled. Real pending-state tests may use existing low-level native fixture seams; do not add a production test callback.
- [x] **Step5: Format/document and review.** Run module Spotless apply/check and git diff --check. Report original exits, counts, exact selected pins/JAR origins, tested source/resource hashes, noisy diagnostics and limitations. Do not claim new live Ranger2.8/MinIO coverage. Controller stages/reviews/commits; implementer does not commit/push or mutate index.

## Task 2: Qualify the changed installed behavior on four pairs

**Files:** controller evidence and results in `docs/superpowers/specs/2026-11-04-delta-append-writer-options-results.md`; public doc continuation updated with exact result after success. No new product behavior in this task.

**Interfaces:** reuse Task1's final selected class/method inventory and original successful first-pair evidence if the final candidate source hashes match. No redefinition of acceptance cases from observed output.

- [ ] **Step1: Freeze reviewed source and fixture inventory.** Retain exact hashes and required nonzero methods, with source/resource origin and selected pin receipts. Reuse unchanged passed first-pair evidence; all remaining supported pairs run their actual selected classes, never a cache/up-to-date claim.
- [ ] **Step2: Execute remaining pairs.** With Java17 run `./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '<focused class>' ... -PdeltaSparkVersion=<3.5.4|3.5.6> -PdeltaVersion=<3.3.0|3.3.2> -PskipITs --max-workers=1`. Capture fresh original logs/exits/XML before next pair overwrites Gradle output. Use bounded existing JVM settings; no broad unchanged suite/package/live replay.
- [ ] **Step3: Record bounded result.** Independently parse all four original XML sets, verify required inventory/no failures/errors/skips and producing source hashes. Explain installed versus live qualification and carry remaining auto-compaction/full-sink gaps.
- [ ] **Step4: Review and checkpoint.** Fresh task review then final whole-slice review, focused checks, commit/push existing branch under standing authorization. Retain worktree and original evidence. Do not create PR/merge/deploy. Next work is focused packaged Ranger2.8/MinIO option profiles and remaining M2 contracts.

## Self-review

The two task gates separate production correctness from cross-version execution. One implementer at a time; one small private validator without new framework. Every Review Focus item maps to a specific Task1 assertion. Native create phase evidence is reused and not presented as governed append proof. Later deployed qualification remains explicit.
