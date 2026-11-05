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

# Delta installed schema CREATE boundary probe

Status: selected test-only characterization under the existing execution authorization. This is the next bounded experiment under the [approved write foundation](2026-10-30-spark-hub-write-foundation-design.md), not production CREATE authority or M2 closure.

## Question and existing evidence

Find where schema-only CREATE first meets the installed Ranger extension and whether a read-only diagnostic observer naturally sees the exact active native transaction and absent-table snapshot before version0 appears. The [native create findings](2026-11-04-delta-create-boundary-findings.md) already establish CREATE0 -> publication at0 -> WRITE1; do not repeat that option/data matrix.

`spark-connector/v3.5/spark-delta/src/main/java/org/apache/gravitino/spark/connector/delta/GravitinoDeltaCatalogSpark35.java` implements untouched staged schema CREATE through RESERVE, real native stage/commit and PUBLISH. Its public constructor accepts separate metadata/actor suppliers. `DeltaPublicationStage` commits native work once and retains uncertainty after a thrown commit. The companion's `DeltaRangerCatalogSpark35.loadTable` uses published-only registration bindings; its reader sanitizes missing metadata into denial. Stock Spark schema CREATE uses tableExists before createTable. Existing `TestDeltaRangerInstalledAppendBoundaries` measures **CTAS** refusal, not schema-only SQL.

Native `CreateDeltaTableCommand` owns transaction creation behind private methods; the CDF ALTER implementation's overridable startTransaction seam is not established for CREATE. Record actual snapshot class/version, not a hypothesized InitialSnapshot class. These source facts motivate observation; they do not predict the installed stopping point.

## Global Constraints

- Initial execution uses Spark3.5.6/Delta3.3.2, Java17 and Scala2.12.21 on fresh isolated local filesystem targets.
- Repeat only a decision-relevant native seam on Spark3.5.6/Delta3.3.0; no four-pair matrix or unchanged predecessor reruns.
- Change tests and findings only. Preserve all production admission, tags, policy checks and native transaction behavior; no main-source/build/dependency/core/API/server/client/storage-schema edits.
- Use the real base catalog, DeltaPublicationStage and native Delta stage; no mock native stage, private reflection, engine instrumentation or replacement transaction logic.
- Consume tableProperties and settings unchanged from the existing native-create/spark-hub-options.json; no writer options, payload write, setting workaround or disabled maintenance feature.
- Never replay native mutation. Separate not committed, committed and unknown outcomes from publication and observation failures.
- Stub policy/transport and local native evidence do not prove real Ranger2.8/Gravitino/MinIO creation, recovery, revocation or ingestion support.

## Two core cases

**A — named schema-only refusal.** Execute `CREATE TABLE a.db.created (id BIGINT) USING delta` with the installed production extension and a dedicated metadata fixture returning the actual Gravitino NoSuchTableException for that fresh name. A test forwarding catalog delegates loadTable, tableExists and createTable unchanged to the real Ranger wrapper while recording entry counters. Record the first production failure, exact plan and zero reservation/native effects. If analysis rejects before tableExists, mark the latter NOT_REACHED; never translate denial to absence or allow CreateTable to advance the probe.

**B — base stage with installed native guards.** In the same installed topology, at a separately fresh target, construct `GravitinoDeltaCatalogSpark35(metadataSupplier, actorSupplier)`, initialize its normal journal, call public stageCreate with a declared schema, then commitStagedChanges exactly once. This diagnostic call bypasses the outer SQL frontend only; every native helper still encounters unchanged production admission. It is not a governed entry point. Script only the typed RESERVE response using the decoded token/run/schema, generation0 and owned local marker location. At PUBLISH arrival record the command and throw a unique fixture sentinel; never fabricate PUBLISHED or perform published load. Preserve the adapter's sanitized publication-status error separately from native refusal.

Both cases use ordinary mapping-none `StructType().add("id", LongType, true)` and no query builder. Case B consumes all fixture table properties; case A carries them as TBLPROPERTIES derived from the same map so its recorded declaration is complete. Both consume all fixture startup settings. The fixture is `spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json`; preserve its provenance and hash rather than copying values.

## Observation contract and stopping rule

Install a diagnostic resolution Rule before the companion rule, returning the **identical** plan and granting no permission. Record sequence/thread/session, node/class shape, object identities, current companion operation, and OptimisticTransaction.getActive. If naturally present, record transaction/log/snapshot identity, actual snapshot class/version, readVersion and catalogTable presence. Check expected local log0 existence at the callback to locate observation relative to native log0 appearance, not Gravitino registration publication. Do not read txn.metadata/protocol/state datasets, allFiles or execute Spark/native snapshot updates from the observer.

After unwinding, retain the original stack, counters, journal phases, native files/hashes and actual log0 bytes. Parse JSON outside Spark for CREATE operation, marker, UUID/schema/action evidence. Check active transaction/current operation cleanup and relevant settings; retain any native lastCommitVersionInSession change explicitly rather than restoring it silently. Diagnostic failures fail independently. A thrown call may already have committed; missing/unreadable evidence stays unknown. Absence of log0 after a drained local call is a bounded local observation, not remote rollback proof.

The measured conclusion is: an exact precommit transaction callback exists; only postcommit/no callback is exposed; or production admission stops earlier and deeper phases are NOT_REACHED. Do not promise a production capture/authorization seam from callback visibility alone.

If B stops before useful transaction observation, one optional identical base-stage run in an isolated **native-only** session may compare callback ordering. Keep schema/properties/settings equal and use a fresh target. Mark it explicitly native-only; it neither authorizes helpers nor repeats WRITE1. Stop after that differential. If no usable public-stage callback is demonstrated, report that result without permissive overrides, new owner design or manual CREATE actions around withNewTransaction. Different public creation frontends remain future hypotheses, not additional cases here.

## Implementation boundary and acceptance

Add `TestDeltaRangerInstalledSchemaCreateBoundary.scala` and `InstalledSchemaCreateBoundaryProbe.scala` under `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/`. Reuse the startup/extension ordering patterns from NativeSelectFixture and TestDeltaRangerNativeSelect.Fixture; adapt NativeBoundCatalogFixture wiring into a dedicated absent-name/actor fixture. A dedicated empty Spark fixture is allowed: NativeSelectFixture.withSpark precreates unrelated a/b/q tables, including column mapping, which is not part of this probe and may conflict with the required native settings. Preserve the real companion extension with an explicit resolver for the dedicated wrapper, rather than its published-table fixture resolver. Do not alter shared fixture behavior or disable required properties to accommodate setup. Its existing published-only row and rejecting actor supplier cannot be reused as create authority. Existing forwarding catalogs must not leave an unsupported create stub in the measured path.

Reuse InstalledAppendBoundaryProbe's evidence conventions, not its eager txn.metadata/protocol observation. Test helpers own only diagnostic tracing and scripted transport. No production interfaces are added. Accept the module's supported Spark3.5.4/3.5.6 setup pins while measuring only the explicitly executed pin; do not break ordinary module tests by hardcoding3.5.6 in setup.

Acceptance requires complete original evidence and measured assertions for both core cases, optional differential clearly accounted for, no instrumentation-induced progression, and a compact public findings document naming the earliest boundary and limits. Native success is not required. Missing evidence and arbitrary exceptions cannot pass as a completed characterization. No policy-revoke success, real publication, lifecycle fault matrix or data write is required.
