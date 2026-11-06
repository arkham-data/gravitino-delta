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

# Spark Hub shared JDBC writer implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement task-by-task, with independent task and final reviews.

**Goal:** Execute real JDBC snapshot jobs through job_wrapper and named write_dataset, including creation, APPEND and additive schema evolution.

**Architecture:** Reuse the existing native companion and Gravitino registration lifecycle. Add finite metadata observation and composed source witnesses, then a trusted Hub runtime/writer selected explicitly at the shared decorator. Preserve default legacy routing and report bounded profile limits.

**Tech Stack:** Spark 3.5.4/3.5.6; Delta 3.3.0/3.3.2; Java17; Scala2.12.21; classic PySpark; actual Ranger server/client2.8.0; PostgreSQL42.7.11; S3A/MinIO.

**Spec:** [Approved critical-path contract](../specs/2026-11-06-spark-hub-shared-writer-design.md).

## Global Constraints

- Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, actual Ranger server/client 2.8.0 and S3A/MinIO.
- Gravitino owns registration; Spark/Delta own execution and native transactions. No Gravitino core/API/server/client/storage-schema changes or engine forks.
- No Glue compatibility, raw-path destination fallback, arbitrary DataFrame trust or collected-row reconstruction. Named input policies and destination permissions remain unchanged.
- One writer per dataset. No automatic retry of failed, committed or unknown writes; no new durable retry store.
- Keep closed milestones/evidence intact. Only changed and affected tests run; native writes are never replayed to repair evidence. Reuse byte-identical artifacts and APFS clones, serialize heavy work and check capacity.
- Initial opt-in effective optimizeWrite/autoCompact/symlink defaults are explicitly false, AQE remains caller-selected within qualified profiles. No silent model/default changes. Preserve existing enabled CREATE profiles.
- Legacy routing stays unchanged without the trusted dependency. Governed failures never fall back.

## Workspaces and verification

G=/Users/jacobo.calderon/Code/Forks/gravitino-delta/.worktrees/spark-hub-adoption-plan; S=/Users/jacobo.calderon/Code/Arkham/spark_hub/.worktrees/gravitino-catalog-foundation. Keep both existing isolated branches/evidence. Base G1028f713d (upload design parked, no product work), S5feaa5d1. Prior qualification G ebfdfbbad remains closed.

P is this plan's ignored workspace. Preserve exact argv, exit, current source hashes and fresh XML; no success from partial logs or skipped native tests. Reuse pinned Python/dependency/artifact receipts from external JDBC; do not use Hub's unpinned main venv for native qualification. Java17 prefix: JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home, JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'; Gradle --max-workers=1 --console=plain -Dorg.gradle.jvmargs=-Xmx2g -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs -PskipDockerTests=false. PostgreSQL tests add -PrangerDeltaExternalJdbcEnabled=true. Use scoped Spotless and git diff --check.

For S edits, read parent AGENTS/CLAUDE, GitNexus impact before every edited symbol, report HIGH/CRITICAL findings, source-check missing graph callers, detect_changes before commit; preserve index PDG/embeddings settings on refresh. No memory tools are exposed: retain findings in P, do not claim memory saved.

## Review Focus

1. False absence triggers CREATE after denied/outage/pending metadata — Task1 exact error negatives and Task5 metadata visibility.
2. CREATE/CDF config advancement grants unrelated/expired source authority — Task2 owned-participant and failed-publication negatives.
3. Wrong defaults/modes/partition declarations silently change job semantics — Task3/4 preflight tests assert zero target/source side effects.
4. A successful native write is retried or reported rolled back after tracker/hook failure — Task3/4 phase outcome tests and Task5 postcommit evidence.
5. Schema additions reorder loaded-at fields or lose old rows/registration identity — Task3 by-name projection and Task5 real additive evolution/readback.

## Task 1: Metadata and typed absence for the named writer

**Files:** modify G companion GravitinoDeltaTable.scala, DeltaRangerCatalogSpark35.java, python/gravitino_delta/tables.py and __init__.py; create finite internal DeltaRangerTableMetadata.java or Scala equivalent as needed; add TestDeltaRangerTableMetadata and Python tests/test_metadata.py. Keep app and general catalog-load behavior unchanged.

**Interfaces:** JVM `GravitinoDeltaTable.metadataJson(SparkSession,String): String` nullable only for positively authorized typed table absence; Python `GravitinoDeltaTable.metadata(spark,name) -> Optional[GravitinoDeltaMetadata]`. Public detached metadata has `schema: StructType`, immutable `properties: Mapping[str,str]`, `partition_columns: tuple[str,...]`. JSON fields exactly schemaJson/properties/partitionColumns. Property allowlist: delta.enableChangeDataFeed, primary_key, delta.logRetentionDuration, delta.deletedFileRetentionDuration, delta.compatibility.symlinkFormatManifest.enabled (needed for early incompatible-hook rejection). No location/nativehandle/authority/credentials/stats.

- [ ] Step1: Add RED tests for positive MODIFY-only metadata without payload SELECT/CREATE; absent with authorized probe; denied existing/probe; NoSuchSchema/catalog; unavailable/expired lease; pending/malformed/replaced registration; wrong session/name; zero payload/mutations; copied result mutation cannot influence later observations; native schema/partition/property allowlist with no location leakage. Python missing API/Connect/malformed response/exception propagation tests.
- [ ] Step2: Run only new named JVM/Python selectors on lead pair, preserve meaningful RED separately from infrastructure failures.
- [ ] Step3: Implement normal application State.actor catalog lookup before separate-reader/native metadata. Catch only typed NoSuchTable at that exact boundary, never generic denial; validate context/lease on absent exit too. Positive observation uses existing published/native binding with before/after identity checks. Return detached data; every later operation obtains fresh authority. Normal metadata visibility, not a new Ranger permission, controls access.
- [ ] Step4: Run new tests plus exact affected catalog/reader/package controls, scoped format and diff checks. If a genuine seam cannot be verified in unit fixtures, include installed native positive/absence checks; report actual execution scope precisely.
- [ ] Step5: Commit product/tests and report to P/task-1-report.md for independent review. No milestone push yet.

## Task 2: Make native CREATE/CDF transitions compose with one JDBC scope

**Files:** G GravitinoDeltaTable.scala/python tables.py, DeltaRangerCreateDeclaration.scala/CreateCommand.scala/CreateMutation.scala, DeltaRangerCdfPropertyCommand.scala/Mutation.scala, DeltaRangerExternalJdbcSources.scala and the minimal shared owned-publication binding; tests TestDeltaRangerExternalJdbcLifecycle plus affected CREATE/CDF scope controls.

**Interfaces:** Python existing create gets optional keyword `source: Optional[DataFrame]=None`; source-bearing path delegates JVM `createForInput` with all existing create inputs plus original DataFrame. New Python `enableChangeDataFeed(spark,name,*,source=None)->None` delegates finite JVM method; no arbitrary property map/callback. Existing source-free create remains compatible. These methods capture exact participating external scopes and advance only a proved successful native metadata-operation setting transition.

- [ ] Step1: Write real native RED for already-issued JDBC source/schema -> schema CREATE0/publication0 -> same source APPEND1 with one immutable explicit false-maintenance profile; and CDF-off -> ALTER -> same source APPEND. Set previous lastCommitVersion to a distinct value before scope opening so no accidental equality passes. Preserve original failed target outcomes.
- [ ] Step2: Add tests for false optimizeWrite/autoCompact/symlink CREATE profile, preserving old accepted profiles and refusing mixed/unknown flags; safe ownership before native work; no new payload SELECT for metadata creation; exact source/session/subject/lifetime/config; wrong target/transaction/participants, unrelated setting, closed/expired source, failed/unknown publication cannot advance; create IF-NOT-EXISTS no-op cannot mint a mutation witness.
- [ ] Step3: Implement smallest typed successful CREATE/CDF witness using existing native/publication owners. No generic context-reset API, all-scope update or source reload. Permit the explicit false profile for schema-only creation; no runtime settings toggles. Enforce source relation ownership for source-bearing calls; no callback injection.
- [ ] Step4: Run targeted lead native scope/creation/CDF/read/append controls; ensure exact CREATE0 -> publication0 -> WRITE1 and CDF transition outcomes, rows and no replay. Other pins qualify in Task5; no closed matrix rerun.
- [ ] Step5: Commit/report for independent review; document any postcommit error as committed/unknown with original exception, not fresh retry eligibility.

## Task 3: Shared named write_dataset implementation

**Files:** S create src/shared/services/v3/gravitino_writer.py, tests/unit/shared/services/v3/test_gravitino_writer.py; reuse existing gravitino_catalog.py, models and source option contract. Minimal changes only where shared pure helpers genuinely avoid duplication.

**Interfaces:** `GravitinoDatasetWriter(spark: SparkClient, bindings: Sequence[DatasetBinding])`; `write_dataset(self,df,output_dataset,write_strategy,execution_properties)->GravitinoWriteResult`; existing argument names/types retained. Add named `external_table_exists(id)->tuple[bool, Optional[GravitinoDeltaMetadata]]` and inherited get_table_df_by_id without raw fallback. Result has independent create/CDF/data phase states; `GravitinoWriteError` preserves original cause, phase, data outcome and prior confirmed phase outcomes. States are not_attempted/confirmed/unknown; confirmed data means committed. No inferred rollback.

- [ ] Step1: RED unit tests for strict absence, existing UPDATE-only route, exact loaded-at legacy/v3 behavior, identity partitions/properties/comment/PK, actual model options including dictionary userMetadata and explicitfalse, existing field order + new fields projection; missing/invalid binding/session/schema/mode/refresh/generated/default profile rejects before target mutation. Reject MERGE/OVERWRITE/incremental; no schema flags dropped.
- [ ] Step2: Implement preflight, fresh metadata, timestamp projection, absent-only source-bound create, existing-only required CDF enablement, and native named format(delta).options(...).mode(append).saveAsTable. CDF alreadytrue avoids ALTER. No Glue notifications/symlink generation/raw reload; use fresh named metadata after successful schema change as needed. Expose phase results conservatively and never retry.
- [ ] Step3: Test partial failure at each create/CDF/append phase; normal data completion followed by metadata observation failure remains committed; exceptions during native mutation remain unknown without positive proof. Empty newly created table survives append denial. Test input/property maps do not mutate.
- [ ] Step4: Run focused writer/catalog/options/CDF unit controls in qualified dependency environment, lint affected files and GitNexus detect_changes. Shared-writer actual runtime proof follows Task4/5, no mock runtime claim.
- [ ] Step5: Commit/report independent review.

## Task 4: Actual shared runner with governed resources and tracker failures

**Files:** S create src/shared/services/v3/gravitino_job.py; modify src/shared/commands/job_runner.py and progress_tracker.py; add tests/unit/test_job_runner_gravitino.py and tracker controls; extend tests/integration/test_gravitino_external_jdbc.py or adjacent test_gravitino_shared_writer.py for real wrapper.

**Interfaces:** frozen `GravitinoJobRuntime(profile,bindings,jdbc_lifetime_seconds)`; `validate_payload(payload)->None`; `open(*,app_name,spark_config)->ContextManager[GravitinoJobClients]` with concrete spark/catalog. `job_wrapper(...,*,gravitino_runtime=None)` preserves positional old API. Fresh reader every invocation. ProgressTracker optional keyword `propagate_errors=False`, with `raise_if_failed()` and safe stop/join; legacy default unchanged.

- [ ] Step1: RED unit tests select actual governed branch only with trusted dependency, never payload flags; existing legacy tests unchanged. Preflight rejects unsupported request/defaults before source read or destination effects. Runtime opens reader after final Spark settings; foreign/closed failures propagate; no raw/Glue construction on governed path.
- [ ] Step2: Implement actual source -> tracker -> validation -> writer -> joined tracker check -> once-only hook; all exits stop/join, unpersist if frame exists and close reader last. Preserve primary error with secondary cleanup failure attached; propagate cleanup-only failure with phase outcome. Do not stop Spark. Use fresh reader for reused runtime config and no automatic operation retry.
- [ ] Step3: Test tracker failure before write and after committed write, writer exception, source exception, hook exception, stop/close exceptions, missing thread start, background context mismatch, repeated job invocation and no scope reuse. Native cause/data outcome survives existing wrapper error decoration.
- [ ] Step4: Run real pinned lead integration `job_wrapper(...,gravitino_runtime=...)(run_source_jdbc_v2)()` with actual SparkClient/reader/source/tracker/writer. Prove source count and tracker execute, count overlaps write using native fixture barriers, create/append/evolution options and actual native state, hooks once, reader remains live until joined and is closed afterward. No stubs for these production components; mock only external telemetry boundaries with explicit disclosure. Native metadata fixtures do not claim Ranger server acceptance.
- [ ] Step5: Run focused runner/source/tracker/catalog/runtime controls and integration; commit/report independent review.

## Task 5: Qualify the real shared route and publish

**Files:** G delta/acceptance/shared_writer_live.py, shared_writer_checks.py, test_shared_writer_checks.py; reuse immutable live/external-JDBC service helpers; coverage register and results doc; S docs/guides/gravitino-catalog.md. No fresh duplicate runtimes without source need.

**Inventory:** each of four pairs runs exactly shared_create_append, shared_existing_append, shared_additive_append, shared_merge_schema_false, shared_update_denied (20 cases). Lead3.5.6/3.3.2 additionally shared_create_denied, shared_alter_denied, shared_metadata_denied, shared_cdf_enable_append, shared_late_target_revoke, shared_postcommit_hook_failure, shared_tracker_failure (7). Every case invokes actual job_wrapper/source/writer; matching ordinary counterpart is not accepted. Freeze exact per-case source/read/write action identities and counts in checker before first run. Task4 installed faults cover unqualified scenarios not invented as service success.

- [ ] Step1: RED/GREEN checker tests reject missing/extra/duplicate cases, wrong versions/producer/process/caller identities, missing required late barriers, wrong denial cause, mismatched native outcomes, unknown-to-retry relabel, credential-derived records and false real-wrapper claims. Case-level phase outcome states independently verified.
- [ ] Step2: Run changed/affected installed tests on the four pinned pairs with immutable source/argv/XML/native receipts, reusing exact completed lead results when unchanged. Scope selectors to changed metadata/composition/writer/runner and affected authority boundaries.
- [ ] Step3: Run 27 fresh service cases serially against actual Gravitino/Ranger2.8/MinIO and read-only PostgreSQL source, using exact Hub package. Check original native UUID/log/schema/rows/properties and Gravitino column identity/version persistence. Schema addition keeps old rows with null newfield. Denied phases cannot commit data; CREATE success before write failure is separately retained. No source reissue/config reset/replay. Failures stop dependent cases until understood.
- [ ] Step4: Stop only owned fixtures and preserve original artifacts/data; summarize exact unsupported defaults/modes and phase outcomes. Update coverage for this shared JDBC route, keep full M2/overwrite/default-profile closure open.
- [ ] Step5: Independent task and whole-slice review, focused fixes only; close records and normal push both feature branches under standing authorization; verify remote heads, retain worktrees/evidence. No PR/merge/deployment or new upload work.
