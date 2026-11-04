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

# Delta Initial-Create Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development; one implementer, focused review, then checkpoint review. Existing user authorization covers this next native experiment.

**Goal:** Resolve the unchanged-option create decision using actual native stages and current registration evidence rules.

**Architecture:** Test the existing truncate wrapper and stage owner, then prove the two-phase CREATE/publication-evidence/WRITE sequence. Keep original Spark Hub option bytes and separate native phases. No new production facade, policy grant or transaction protocol.

**Tech Stack:** Spark 3.5.6, Delta 3.3.2 and 3.3.0, Java17, Scala2.12.21, JUnit5, isolated local filesystem.

**Spec:** [Create boundary](../specs/2026-11-04-delta-create-boundary-design.md).

## Global Constraints

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

- Wrapped CreateOrReplace gets past truncate but changes/drops explicit false — assert captured options unchanged and the actual next error before any commit.
- Empty CREATE is mistaken for successful data ingestion — retain separate versions/rows/metadata and injected initial-data failure.
- Delayed publication accepts CREATE0 after WRITE1 — exercise actual evidence reader with fresh latest version.
- Native commit is repeated after publication failure — use existing stage owner and verify unchanged version/UUID on publication-only retry.
- Abort/failure leaves usable transaction context or session metadata override — terminal-stage checks plus active-transaction/session-setting assertions.

## Task 1: Prove the native create sequence

**Files:**
- Create `spark-connector/v3.5/spark-delta/src/test/java/org/apache/gravitino/spark/connector/delta/TestDeltaNativeCreateBoundary.java`.
- Create `spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json`.
- Create `docs/superpowers/specs/2026-11-04-delta-create-boundary-findings.md`.
- Modify `docs/superpowers/specs/2026-10-30-spark-hub-write-native-findings.md` only to link the new bounded result.

**Interfaces:** No public production API added. Same-package tests access existing `DeltaReplacementStage(StagedTable)` and `DeltaPublicationStage` callbacks; native stage writer uses `LogicalWriteInfo`, `WriteBuilder.build()`, `V1Write.toInsertableRelation().insert(Dataset<Row>, boolean)` and `commitStagedChanges()`. Shared `DeltaCommitActionReader.inspect` consumes actual archived native commit JSON, latest/snapshot versions and metadata. Test fixture helper decomposition is builder discretion; avoid creating another acceptance framework.

- [x] **Step 1: Capture portable test input and write focused native assertions.** Extract original actual options, expectedUserMetadata and table properties from the immutable prior `native-cell-2/stage-create.json`; fixture records original result SHA256 `c6cd829c4b3daec3a5919a8ef88c4f9bcb638944b5067df87455644b32a03a97`, source revision `d01024b6f459ad2743b7b55099226b8c3dc742fd` and the relevant helper source hashes. No private paths/credentials. Require actual wrapper/stage classes, not copied implementations. Tests cover all five Review Focus items, successful original-option WRITE and settings/active transaction cleanup. These are characterization tests of existing code: no artificial failing product stub or production change is required. If a test exposes an actual defect needing a source fix, retain its original red result and report before widening scope.
- [x] **Step 2: Execute the first focused native suite.** Set Java17 and a fresh absolute `DELTA_CREATE_EVIDENCE_DIRECTORY` for the fixture to retain native files and phase observations (default test execution may use JUnit temp paths). Run `./gradlew :spark-connector:spark-delta-3.5:test --tests '*TestDeltaNativeCreateBoundary' -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs --max-workers=1`. Keep JVM memory bounded using existing repository conventions; inspect runtime class origins/pins. Preserve original failures and correct test expectations only from actual native evidence. Source errors are not native qualification.
- [x] **Step 3: Confirm the second Delta pin.** Run the same focused test command with `-PdeltaVersion=3.3.0` and a distinct fresh evidence directory. Before each follow-up run, copy completed original JUnit XML/logs to this plan's evidence directory. A cache/up-to-date result is not a new pin execution; require the test task to execute with the selected classes. No unchanged predecessor suite/matrix replay.
- [x] **Step 4: Format and inspect scope.** Run module Spotless apply/check with Java17 and `git diff --check`. Tests must have meaningful native assertions, no failures/errors/skips and finite nonzero case counts. Preserve the actual class/JAR identities and native log phases. Only rerun affected tests if source formatting/changes invalidate prior evidence.
- [x] **Step 5: Write the measured creation decision and review.** Findings identify tested pins/filesystem, exact native error after truncate acknowledgement, CREATE0 publication-evidence result, WRITE1 metadata/identity, failed write state, delayed-publication rejection, stage lifecycle cleanup and remaining authority boundaries. If two-phase is selected, state that initial empty publication matches current Spark Hub behavior and explain why publication precedes WRITE under the current protocol. Fresh reviewer checks code and evidence. No native single-commit or governed-support claim unless actually proven.
- [ ] **Step 6: Commit/push the reviewed checkpoint.** Commit only task files plus controller closure status, retain original evidence/worktree, push existing adoption branch under standing user authorization. No PR/merge or deployment. Full M2 remains open; next production task should implement the selected lifecycle's missing authorization/options contract rather than repeat this native experiment.

## Self-review

One task and no cross-task mutable interfaces. The five failure modes map directly to native assertions; previous real helper output is a portable fixture with provenance, while current Spark Hub execution is not claimed. Public unchanged production methods and exact logs supply evidence without private reflection or a new framework. Narrow local qualification does not substitute for later Ranger2.8/MinIO/all-pair integration.
