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

# Spark Hub adoption of governed Delta — design

Status: M0/M1 execution was approved on 2026-10-29 using the selected subagent-driven workflow. The workload register, bindings, optional startup profile and named-read service are implemented and independently reviewed. Task 4 code and Python source-archive preflight are reviewed/prepared; Scala/build and live qualification remain pending. Later milestone contracts require their own designs.

## Intent and baseline

Make the actual Spark Hub ingestion, transformation, incremental processing and maintenance paths operate through Gravitino-owned Delta registrations with Ranger policy enforcement. Spark Hub is the primary adoption acceptance repository; catalog-demo remains a separate, later integration. Passing a catalog-demo example does not qualify Spark Hub.

The source audit inspected Spark Hub commit `74b71155478d4a5f78360108002c95399310a998` and Gravitino candidate `6834b18f9349fab8c97decd1cec0de0140a69666`. The existing append-AQE milestone is still running and must close independently. Its frozen producing sources must not be modified by this planning work. Before JVM checks, native runtime packaging or live qualification, integrate its independently reviewed closure into the isolated adoption branch and reconcile any contract differences. Use a merge to preserve the audited commit references as reachable ancestors; code and pure Python checks may proceed independently beforehand.

Source evidence: Spark Hub's `src/shared/commands/job_runner.py` delegates non-preview output to `src/shared/services/v2/delta.py`; that sink uses path writes, schema options, native MERGE and Glue refresh. `src/shared/services/v3/cdf_reader.py` reads open-ended path CDF; `src/shared/utils/incremental/state_manager.py` overwrites state paths. Join v4 unions transformed sources and eagerly checkpoints them. These are production-source call paths, not proof of current deployment frequency.

Detailed local audit: `../../../../delta-spark-feasibility/.superpowers/sdd/2026-10-28-delta-append-aqe/sparkhub-compatibility-audit.md` from this planning worktree is only a local evidence pointer; source references above and the roadmap below are the portable basis. The detailed audit is not a prerequisite package to install.

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
- Keep the running append-AQE milestone separate. Do not rerun unchanged successful matrices for documentation, planning or checker-only changes.

## Approaches and recommendation

1. **Extend the connector and migrate shared Spark Hub services in stages — recommended.** Keep native Spark/Delta execution and put name resolution, runtime configuration and dataset lifecycle in shared services. Add connector support for actual required operations before routing dependent jobs. This requires work in both repositories but avoids copying governance logic into every job.
2. **Adapt every job to today's bounded connector contract.** Smaller initial connector changes, but requires removing or rewriting overwrite/schema evolution, generated partitions, joins/windows/checkpoints and streaming. It would change platform behavior and produce repeated adapters. It does not meet the stated full-support objective.
3. **Complete a general-purpose Delta catalog before integrating any real job.** A broad native delegation architecture is attractive, but implementing all Delta features first gives little early evidence that actual Spark Hub semantics work. Use the shared-service approach and workload-driven milestones instead; keep the remaining full-Delta feature inventory explicit.

M0/M1 is the approved first implementation slice. Later phases retain separate design gates; live Glue deletion and cutover are outside this slice.

## Boundaries and responsibilities

**Spark Hub** owns dataset UUID-to-canonical-name bindings, execution intent, business transforms, source intervals, durable consumer progress, replay decisions, and migration of its legacy Glue/Athena side effects. It must not become a second authorization engine.

**The Delta companion/provider** owns named operation integration, original source/target demands, policy-aware execution contexts, native identity/registration binding, native publication/reconciliation, and safe exposure of required metadata operations. Implement metadata access through an explicit authorized surface; do not fill missing history/head APIs with raw `forPath` access.

**Ranger** owns access, row filters and masking decisions. **Spark and Delta** own physical execution and native table semantics. External ingestion is a distinct source category requiring an authorized target; mixed plans must preserve any governed source demands. Application-created data is not automatically proof of governed lineage.

Extend operator families required by real workloads rather than adding individual physical-plan exceptions. Joined, windowed, UDF or checkpointed sources need an explicit logical-authority and materialization contract before implementation. A truncated logical plan or matching schema cannot stand in for source authorization. Policy changes must not allow retained materialized results to borrow a fresh authorization without the defined revalidation/rebuild behavior.

## Milestones and order

The [roadmap](../plans/2026-10-29-spark-hub-adoption.md) defines M0–M7. M0 establishes the workload coverage register. M1 delivers a deliberately read-only Spark Hub named-catalog slice over pre-existing PUBLISHED tables. M2 delivers ingestion/create/append/full-refresh lifecycle. M3 delivers actual transformed and snapshot MERGE. M4 completes CDF consumer/state semantics. M5 completes generated features and maintenance. M6 qualifies deployed operation and migration. M7 is the separate streaming plan. M3 and M4 are jointly required for incremental pipeline acceptance; M1 is not a substitute for write support.

The first implementation plan contains M0 and M1 only. Later milestones require their own reviewed design and exact implementation plan at the relevant dependency boundary. This avoids pretending unresolved native integration details are implementable signatures today.

## First-slice contract: M0/M1

Create an explicit optional Spark Hub runtime profile and a named read catalog service. It operates only on fixture- or operator-provisioned PUBLISHED Gravitino registrations. It does not create native tables, call Glue, change the default production backend, or masquerade as a complete `DeltaClient`.

The dataset binding is a configured mapping from platform UUID to three canonical identifier components. Catalog and schema are deployment inputs, not inferred from an S3 prefix. Reject duplicate UUIDs/names and malformed components. Native identity and registration checks remain in the companion's named resolution; the local mapping is a routing configuration, not authoritative metadata. Define production tenant/catalog naming and binding ownership in M2 before provisioning or migration.

An optional `GravitinoRuntimeProfile` is supplied at Spark session creation. It carries the exact reviewed runtime configuration and references to pre-provisioned artifacts/private files. Do not download packages or patch a reused session from the adapter. The profile validates the expected extensions/catalog/runtime configuration before work; the existing companion performs the security-critical runtime admission. Profile fields and safe errors must not expose secrets. Preserve the legacy SparkClient route when this optional profile is absent.

`GravitinoDatasetCatalog.get_table_df_by_id` resolves an explicit binding and calls the native `SparkSession.table` API. Missing names, permissions or bindings remain errors; none triggers path fallback or table creation. The new service is used by a dedicated read-only probe first. General job_wrapper production routing waits for M2/M3 write support; do not add a supposedly complete backend whose writes silently go to the old DeltaClient.

The probe invokes actual Spark Hub classes using classic PySpark, a PUBLISHED source and actual Ranger 2.8.0. It proves filtered/masked rows, demanded-column denial, targetless read behavior, missing-binding failure, fresh-action revocation and artifact/session identity. It must also prove that no Glue/Athena/native path lookup occurred. This qualifies the shared read boundary only, not arbitrary query jobs, background-thread tracking, external ingestion or writes.

## CDF and policy-sensitive consumer contract for M4

Capture a native source identity and explicit inclusive interval before reading. For multiple sources retain a vector of identities/intervals. Advance only the exact processed end after successful sink publication, including an empty authorized interval. Never checkpoint a separately queried later head or a maximum visible/masked commit version. Source replacement, incomplete history and policy changes require a recorded reconciliation decision.

Keep append-history, incremental upsert and full-replacement output contracts distinct. Existing INSERT+UPDATE_POSTIMAGE-only processing does not establish replica convergence. Tests must include deletes, rows entering/leaving visibility, policy-only changes, masked-key collisions and nonunique/changed keys. Use native ambiguity/error behavior where appropriate; do not invent an impossible universal exact-replication guarantee when policy-visible information is insufficient. The M4 design must explicitly select and test reconciliation behavior for each output mode before implementation; this draft does not authorize silently changing a pipeline's business semantics.

Checkpoint and sink updates are separate transactions. Specify recovery from sink committed/state failed and unknown sink outcomes; neither a local journal nor a successful state file write establishes exactly-once processing. Persist source identity and interpretation contract along with the processed interval; the exact durable schema and migration belong to M4.

## Acceptance and release discipline

Maintain a finite operation/case register linked to actual source callers. Record required API, source plan family, schema/features, configuration, policies, expected native state and process/artifact evidence. Initial unsupported cases are explicit gaps, never skipped tests counted as passing coverage. Do not infer that a method exists because an unused utility defines it.

For each milestone, run focused regression and negative tests, then affected actual Spark Hub/Gravitino/Ranger/MinIO cases across the required version pairs. Reuse unchanged verified evidence with explicit identity. Recheck failures and changed behavior; do not replay the entire program's tests at every small step. Keep one implementer per mutating task, independent task review, then a milestone review. Push reviewed milestone closures under the existing authorization; production migration is a separate concrete action.

Deployment qualification must include actual configured versions, distributed workers, user identity, Livy/session reuse if retained, the background read tracker, durable journal placement, source/sink/state restarts and storage access. Legacy Glue may remain the explicitly selected backend for unmigrated datasets; no governed operation may fall back to it. Each migrated dataset has one authoritative registration owner. A post-write rollback to Glue is not automatically safe; define recovery from native state first.

## Completion definitions

- M1 complete: the shared read-only named slice passes its exact acceptance cases. Spark Hub adoption is still incomplete.
- Batch adoption complete: all required tracked batch operations and their policy/state/recovery/deployment contracts pass, and selected datasets have a reviewed ownership migration path. Deferred streaming remains explicit.
- Full Spark Hub adoption complete: required streaming and remaining active caller paths also pass. A finite workload matrix is not a claim of every Delta feature being supported.
- Full Delta support remains the wider product objective. Features not exercised by Spark Hub need an explicit feature inventory and separate evidence; do not mark them supported by workload similarity.

Builder choices left open: idiomatic internal file organization, test fixture minimization and nonsemantic performance tuning. Product contracts, hidden option changes, policy meaning and irreversible migration are not builder discretion.
