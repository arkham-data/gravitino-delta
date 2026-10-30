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

# Spark Hub M2 write foundation — design and first task

Status: proposed on 2026-10-30. The approved architecture is retained. This document defines the M2 destination and a bounded first native-characterization task; it does not claim that M2 implementation seams or runtime support have been proven. No Spark Hub write routing changes are authorized by a passing native probe alone.

## Baselines and outcome

Append AQE is closed at `90a0e1c65db133ecdbff8a2758c71a4b749c58f1`; M0/M1 is reviewed and pushed at Gravitino `c7681d08a0f8a562375e20bc6517c8c6d6c6da45` and Spark Hub `d01024b6f459ad2743b7b55099226b8c3dc742fd`. Continue in the existing adoption worktrees. Preserve predecessor evidence unchanged.

M2 must make actual Spark Hub `job_wrapper -> write_dataset` initial ingestion, APPEND and full OVERWRITE work with Gravitino-owned registration and Ranger enforcement. It must preserve required options, table metadata, native identity and partial-failure behavior. A native-only experiment, owner-only create, fixed-schema append, or fabricated DataFrame does not close this milestone.

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


## Source findings that determine the work

All Gravitino paths below are relative to its pinned worktree; Spark Hub paths refer to its pinned worktree.

- Spark Hub `src/shared/services/v2/delta.py:529` owns a lifecycle, not just a writer call: target probe, loaded-at projection, native empty creation/CDF enablement, data write, Glue/Athena publication/refresh and native postcommit work. `src/shared/commands/job_runner.py:159` calls it and only then invokes post-write hooks.
- `ExecutionProperties.to_df_properties` in `src/shared/schemas/v2/base_jobs.py` passes a dictionary as `userMetadata`. Preserve and measure its actual PySpark string conversion. Refresh forces both schema options true but does not change the payload write mode; embedded metadata flags can differ from those effective options.
- The governed `DeltaRangerAppendCommandBinding.scala` currently rejects nonempty options and requires original governed source demands. `DeltaRangerReadRule.scala` does not define ordinary JDBC/file ingestion. These are connector gaps, not adapter configuration fixes.
- `catalogs/catalog-lakehouse-delta/.../DeltaRegistrationOperations.java` supports refreshed same-UUID schema observations. Its replacement path requires a new UUID. Native Delta `WriteIntoDelta` schema overwrite retains the UUID. Full refresh must use ordinary overwrite, not registration replacement.
- `spark-connector/v3.5/spark-delta/.../DeltaPublicationStage.java` reserves, stages and publishes native create. Ranger's existing published-target owner cannot authorize an unpublished reservation. Initial native staged CREATE may omit writer userMetadata; absent-only CREATE OR REPLACE is a candidate to investigate, never a retry license to replace an existing table.
- The Ranger Delta plugin maps MODIFY_TABLE to UPDATE + ALTER as already approved. Public non-owner schema CREATE_TABLE propagation is missing. USE_SCHEMA must stay visibility-only.

Detailed local discovery is retained under `.superpowers/sdd/2026-10-30-spark-hub-write-foundation/` in `sparkhub-sink-discovery.md` and `delta-native-discovery.md`. These are optional local evidence, not installation prerequisites. Native source references are the published Delta [3.3.0 sources](https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.0/delta-spark_2.12-3.3.0-sources.jar) and [3.3.2 sources](https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2-sources.jar); source similarity does not establish runtime equivalence.

## Approaches

The previously approved shared-service/native-engine approach remains binding. Within it, the recommended direction is to extend the existing native append integration for options and ordinary overwrite, and add a separate reservation-bound create lifecycle. This preserves the reviewed write ancestry without pretending published-target authority covers creation.

A named companion writer facade is an alternative if native command/staging boundaries cannot retain the necessary context. It must take the original same-session DataFrame and typed intent, never an arbitrary transaction callback, path or reconstructed governed rows. Choose the exact interface after characterization and native seam verification; do not promise a signature before it exists.

Using stageReplace for every refresh is excluded: it changes table identity and replacement semantics. Reimplementing Delta transactions or weakening source checks is excluded by the global constraints. This is an engineering recommendation, not a claim the user chose an unproven native seam.

## M2 behavior contract

| Area | Required result |
| --- | --- |
| Routing and names | Explicit runtime profile and UUID-to-catalog/schema/table binding. Spark Hub owns routing configuration; provider owns storage allocation/registration. Do not infer names from display names or S3 paths. Accept caller/operator-provided bindings for fixtures; production naming and provisioning remain an M6 prerequisite. |
| Create | Distinguish absence from denied/unreadable. Authorize canonical schema CREATE before reservation and at native/publication boundaries. Bind creator, source, reservation and native evidence. Publication of an empty native table and publication after data are distinct outcomes; no atomicity claim across those phases. Select the lifecycle after the first task. |
| Non-owner authority | Proposed plugin-only CREATE_TABLE -> Ranger CREATE on canonical schema resource with intentional table/column wildcards. Keep USE_SCHEMA visibility-only and existing-table SELECT/UPDATE/ALTER separate. Prove public grant/revoke, direct/group role lifecycle and other-schema denial on Ranger 2.8.0 before declaring creation supported. No core change or automatic live policy rewrite. |
| Source ingestion | Retain actual JDBC, file-derived and constructed batch plans and target authorization. Include the sink's loaded-at projection. A broad LogicalRDD allowance cannot prove original lineage; mixed governed leaves retain every original demand. Materialized governed input must not be relabeled external. Exact admission ownership is a later implementation gate. |
| Append | Preserve writer mergeSchema/overwriteSchema/userMetadata and effective session settings. Explicit false must remain false. No option collisions, paths or credentials smuggled into target metadata. |
| Full refresh | Use the explicit OVERWRITE strategy, preserve native UUID/history and existing metadata according to stock Delta semantics, refresh registration after own schema evolution. Reject dynamic partition overwrite for the full-refresh profile before mutation; it is not equivalent to full replacement of rows. Refresh+APPEND remains APPEND with existing option overrides, not a silently corrected payload. |
| Schema | Native merge/overwrite schema semantics, including empty refresh, new/dropped fields, nullability and ordinary partitions. Require UPDATE plus ALTER when metadata changes. Prove schema/protocol changes are this operation's own changes before accepting refreshed registration. |
| Defaults | Initial CDF=true, log retention 45 days and deleted-file retention 364 days; existing-table CDF enablement; effective optimizeWrite=true, autoCompact=true and default symlink hook are ordinary-route requirements. If unsupported, M2 remains incomplete. Never turn them off to pass. |
| Outcome and recovery | Record data mutation as not committed, committed or unknown, independently from creation, metadata ALTER, registration publication/refresh and maintenance. Known committed and unknown writes are not automatically replayed. Reconcile only proven phases under an authorized recovery actor. |
| Retry identity | Application supplies stable intent across attempts; native transaction suppression alone is not intent validation. Reject changed payload/target/options with reused identity; missing/expired native evidence remains unknown. Never hash UUIDs into supposedly monotonic txnVersion values. Exact durable identity/retention/store schema must be designed before retry implementation. |
| Legacy compatibility | Opt-in named service must not inherit AWS side effects. Legacy selection retains existing behavior; named selection has no Glue/path fallback. Business-event migration and actual backend cutover remain separately reviewed M6 work. |

## Dependency order and closure

1. **Native characterization (the exact first plan):** measure ordinary writer behavior and staged-create candidates with actual Spark Hub option inputs. Produce original native logs, UUIDs, rows, settings, failures and a compatibility decision. No product routing changes.
2. **Source and writer integration:** define retained external-input ownership and typed options; prove target denial and preservation of all governed source demands before write side effects.
3. **Option-aware append and recovery:** native append with stable intent and independently evidenced outcomes, including postcommit faults and duplicate retries.
4. **Create authorization and publication:** non-owner schema CREATE lifecycle, reservation-bound native execution, initial data/metadata, restart reconciliation and lost-response handling.
5. **Same-identity overwrite and schema evolution:** native remove/metadata/protocol actions, authorized registration refresh and full-refresh cases.
6. **Required ordinary postcommit work:** real auto-compaction and symlink behavior, separate maintenance outcomes, no replay of durable data. It may share implementation with earlier steps where the native lifecycle requires it; it cannot be deferred past M2 closure just because broader maintenance is M5.
7. **Actual shared routing:** opt-in job_wrapper and named write_dataset; named replacement for manual-upload schema protection; real JDBC/file/constructed callers; source/target denials, schema options and fault cases across all four Spark/Delta pairs with actual Ranger 2.8.0 and MinIO.

Steps 2–7 are dependency milestones, not invented exact implementation tasks. Their designs must settle native seam, external-input provenance, create publication sequencing and durable retry ownership using step 1 evidence before the associated production code is written. This avoids a large speculative implementation plan.

M3 retains general transformed DataFrames, checkpoints and snapshot MERGE; M4 retains CDF intervals/progress/reconciliation; M5 retains generated date partitions and broader maintenance/features; M6 retains deployed adoption/Glue retirement; M7 retains streaming. Reject unqualified scheduled/generated/transformed routes explicitly. Do not report those callers as supported by an ordinary-ingestion result. Catalog-demo stays outside this work.

## First-task boundary and acceptance

Build a small test-only native characterization runner in `delta/acceptance`, following the existing immutable-artifact and subprocess conventions. Use real Spark Hub model/helper imports from the pinned S worktree; do not replace their options with handwritten equivalents. Import helpers without constructing DeltaClient/AWS clients. The runner executes stock Spark/Delta in an independent fixture process. Its explicit raw paths are confined to the native oracle, never an application fallback or governed acceptance result.

Use an installed, hash-verified Spark 3.5.6 / Delta 3.3.2 / Java 17 / Scala 2.12.21 runtime first. Reuse validated artifacts without rerunning predecessor scenarios, but create fresh owned MinIO prefixes and result directories. No new four-package duplication. This first task does not require a Ranger write test because it cannot prove governance; actual Ranger 2.8.0 remains mandatory for later governed closure. Missing local services/artifacts are setup prerequisites, not a passing test or grounds for changing pins.

Required cases:

- Actual ordinary and FULL_REFRESH/STEP_REFRESH option construction, explicit false and refresh+APPEND mode preservation, including precise userMetadata bytes in native history.
- Legacy empty-create then data-write versus native V2 staged CREATE and absent-only CREATE OR REPLACE candidates. Record every native commit, UUID, schema, properties and userMetadata; do not pick a candidate merely because no exception occurred.
- Same-schema append, schema-merge append, explicit mergeSchema=false failure, full schema overwrite and empty overwrite; plain and ordinary partitioned targets; explicit overwriteSchema=false mismatch failure; actual loaded-at projection.
- Effective autoMerge option precedence and settings restoration after success/failure. Full-refresh dynamic partition mode refusal is recorded as an adapter requirement, not misreported as a native restriction.
- Native duplicate transaction suppression with the same app/version and a separate intentionally changed-intent observation. The oracle must show whether native suppresses different data too; suppression alone is not safe application retry.
- Enabled optimizeWrite/autoCompact and symlink hook with enough small files to trigger actual compaction; identify separate versions/actions. Enabled-but-idle is insufficient. If hooks hide a failure or cannot be observed through public seams, report the unresolved boundary instead of claiming a usable governed lifecycle.

A scenario error is an observation, not automatically a runner success. The checker validates required cases, exact pins/artifacts, fresh-prefix ownership, before/after native evidence and expected invariants. It distinguishes a completed characterization from a viable candidate and from governed support. Never treat missing evidence, settings-only observations or an error string as proof that no commit occurred. A separate observer may record native state when the writer exits abnormally; missing readback makes the relevant phase unknown.

Deliver a compact decision report with: chosen ordinary overwrite semantics; candidate creation sequence and userMetadata tradeoff; actual hook/compaction events; native boundaries still unavailable; and exact next source/options or create-lifecycle task. If no viable create boundary exists yet, say so and plan that targeted seam experiment; do not expand this first task into a new writer architecture.

## Builder discretion and remaining gates

Fixture row values, test helper decomposition and bounded compaction thresholds are engineering choices; record thresholds and prove a compaction commit. Production namespace policy, scheduler retry retention, event semantics and deployment storage are not needed to run this oracle and remain explicit gates before routing/cutover. Do not invent a durable retry API or production migration policy in the probe.

This design's approval covers the first characterization task and M2 direction. It does not approve a new live Ranger grant, tenant migration, deletion, production retry policy or all of steps 2–7 without their concrete implementation contracts.
