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

# Delta Spark compatibility obligations

A bounded named CDF-to-MERGE milestone does not complete full Delta support.
Unsupported profiles remain implementation and qualification obligations. They
must not be admitted because their schema resembles a qualified relation.

| Area | Current boundary | Evidence required to close |
| --- | --- | --- |
| Named SQL CDF-to-MERGE | Locally qualified on all four pinned pairs | Retained original Ranger 2.8.0/MinIO, native-state, identity and artifact evidence; preserve the bounded profile across changes |
| Classic PySpark SQL | Actual Python `spark.sql` qualified on all four pairs | Original Python exit0, query-thread identity, provider/version, masks/filters, denial, target CDC and checkpoints retained |
| Materialization and native retries | Focused local owned-attempt tests | Preserve fresh authority, changed-policy rejection, block-loss retries, exhaustion and owned cleanup across runtime changes |
| Publication and recovery | Private durable driver-local journal and exact native proof | Current authorization/registration, exact intent, authorized duplicate, no replay after known commit; deployed placement still separate |
| Constraints and no-op paths | Native CHECK and empty/no-match paths tested locally | Continue distinguishing native constraint failure from adapter rejection; no committed changes on failure |
| Partitioned targets | Bounded named SQL profile locally qualified on all four pinned pairs | Five fresh Ranger 2.8.0/MinIO/PySpark scenarios per pair cover STRING/null/escaped partition values, governed moves, same-table writeback, denial, native CDC and persistence; broader type/settings/storage profiles remain separate |
| Insert-only optimization | Unsupported | Owned native branch with original column demands, masks/filters, output and denial evidence |
| Column mapping and defaults | Unsupported | Protocol/schema profiles, native features, authored-expression provenance and raw copy-forward |
| AQE | Bounded named ordinary reads, CDF and native MERGE use trusted stock adaptive execution; caller chooses the profile | Four-pair installed defaults/coalescing and representative optimization branches; seven fresh packaged Python/Ranger/MinIO phases per pair, exact original artifacts/exits and operation-boundary revocation. See [current contract](delta-cdf-merge.md#trusted-stock-adaptive-execution) |
| Schema evolution | Unsupported | Native schema evolution with registration/publication and policy demand correctness |
| Row tracking and deletion vectors | Unsupported write profiles | Exact native feature semantics and owned action/reconstruction evidence |
| Broader source grammar/clauses | One bounded CDF source with finite scalar grammar | Defined joins/windows/aggregates and NOT MATCHED BY SOURCE semantics; no admission by schema equivalence |
| Named DataFrame builder | Locally qualified on all four pinned pairs with actual packaged classic Python | Five final partitioned Ranger 2.8.0/MinIO phases per pair; installed unpartitioned/partitioned API suites (37+37 inherited with 61 unique methods per 3.5.4 pin, 51 fresh per 3.5.6 pin). Exact source/package/original-exit provenance and the bounded historical common-JAR exception are retained; final integrated SPEC/QUALITY review approved this bounded API milestone. See [supported API](delta-cdf-merge.md#named-dataframe-api); stock DeltaTable.forName remains unqualified |
| Spark Connect | Not qualified | Remote-session identity/authorization and supported frontend behavior |
| Separate workers/deployment | Separate plan; deferred pending DataFrame API qualification and assessment of broader Spark execution prerequisites | Qualify any execution capabilities required by the intended pipelines before integration; exercise the qualified PySpark DataFrame API with deployed authentication, executor boundaries, journal durability/placement, registration and artifact verification |
| Native strict source-count check | Known stock-equivalent constant-key full-outer nested-loop shape | Retain strict check and both stock/governed oracle; evaluate compatible upstream correction without suppressing validation |
| Ordinary projected SELECT with explicit BIGINT arithmetic key mask | Locally qualified on all four pinned pairs in a separate follow-up | `id + CAST(100 AS BIGINT)` returns masked values once, including admitted aliases/predicates alongside row filters; exact receipt binding and untyped-arithmetic rejection retained |

The [partitioned MERGE plan](superpowers/plans/2026-10-24-delta-partitioned-merge.md#task-2-native-conversion-limits-and-remaining-qualification)
records native binary conversion loss, a local-filesystem NUL failure and timestamp
shifts with mismatched session/JVM timezones. Matching stock Delta in these cases
does not establish lossless conversion or qualify other storage/settings profiles.
Installed scalar checks and live MinIO STRING-partition checks are distinct scopes.
The live qualification also verifies a Delta-provider correction for partitioned
registration publication after relational hydration, preserving complete conflict
checks without modifying Gravitino core.

The arithmetic-mask follow-up completes an already-resolved cast's session timezone
before receipt issuance. Focused installed-stack tests used Spark 3.5.4/3.5.6 ×
Delta 3.3.0/3.3.2 and Ranger 2.8.0 policy fixtures, retaining modified-mask,
original-column denial, receipt, ownership and native MERGE checks. The original
Task 5 diagnostic and historical live packages remain separate evidence; those
packages predate this repair.

The stock-equivalent source-count behavior was observed on both Delta pins. A
successful different join shape does not close it. Full compatibility also does
not follow from a Python harness launching a Scala class: Python itself must
submit the named SQL under its authenticated application identity.

See [CDF consumption](delta-cdf-consumption.md) for historical-schema and finite
read bounds, [governed MERGE](delta-cdf-merge.md) for policy/checkpoint semantics,
and the separate [builder](superpowers/plans/2026-10-22-delta-cdf-merge-builder.md)
and [deployment](superpowers/plans/2026-10-22-delta-cdf-merge-demo.md) plans.

## Revised adaptive execution boundary

The trusted-stock route supersedes the former exact physical-stage/RDD-graph
hardening requirement. Running reads may finish under captured decisions; the next
supported action checks locally applied Ranger state. MERGE also checks source
attempts, touched/write phases and precommit/reconciliation, without an atomic
Ranger/Delta fence. Historical restrictive-profile diagnostics remain historical.

All four requested Spark/Delta pairs have fresh installed evidence and seven fresh
packaged DataFrame live phases each. Ordinary final broadcast and native
touched/write coalescing are observed; exchange-free CDF and expected denials are
labeled explicitly. The full legacy baseline and focused per-pair coverage are
distinct in the [original execution table](delta-cdf-merge.md#original-task-4-execution-records).
Broader logical grammar, caches, Connect/streaming, separate workers and catalog-demo
remain pending scope; no API version or deployment claim follows from this change.
