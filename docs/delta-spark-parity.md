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

## Ordinary Spark read candidate — 2026-10-28

The bounded ordinary-read candidate covers Spark 3.5.4/3.5.6 × Delta
3.3.0/3.3.2 with Java 17, Scala 2.12.21, Ranger server/client 2.8.0,
and the manifested local Hadoop/S3A 3.3.6 MinIO profile. Final independent
review remains a separate gate; this does not qualify catalog-demo or production
deployment.

Native classic PySpark performs count, finite COUNT(*)/COUNT(1)/COUNT(column),
distinct, orderBy with deterministic limits, show, toPandas and local DataFrame
temporary-view operations. Original authored references survive pruning and
count optimization: an id projection needs id SELECT, explicit inner SELECT-star
retains every authored column, and a columnless occurrence uses Ranger column="*".
Policy-only filter/mask dependencies do not become user SELECT demands. Each
original self-join occurrence retains its own registration and column set.

Filters and masks precede user operations. The five-row fixture has four east
rows; email COUNT is 3 unmasked, 4 under constant 'M', and 0 under typed NULL.
Distinct produces one M or one NULL. Sorted/zero limits, false predicates and
metadata-only count retain authorization obligations even when execution removes
the scan. A separate exchange-bearing join proves final materialized AQE.

Every supported Delta action rechecks captured context and locally applied policy.
Already-planned stale decisions reject; accepted in-flight work may finish with
captured decisions. Local createTempView/createOrReplaceTempView/dropTempView
preserve original intent and bound definitions; replacing a name does not silently
rebind an existing frame. This is not atomic revocation or per-stage reauthorization.

Pandas is limited to classic Python 3.11, Pandas 2.2.3, PyArrow 17.0.0 and recorded
UTC Python/session timezones. Arrow off and required-on run through native APIs;
fallback is disabled and actual Arrow RecordBatch transport is observed. The
primitive profile covers long, nullable string/boolean/double, decimal, date and
timestamp. Dtypes are recorded separately: id=int64, nullable long/double=float64,
nullable string/boolean/decimal/date=object, timestamp=datetime64[ns] with Arrow
off and datetime64[us] with Arrow on. Other timezones, nested types, Connect,
streaming and separate workers remain unqualified.

Cache/persist/checkpoint, global/permanent/SQL-text views, general aggregates,
windows/UDFs and broader CDF/MERGE/append grammars remain outside this milestone.
No write surface or production authentication/grant-propagation claim is added.

Comparator observation uses the unchanged Kyuubi audit handler and actual SELECT
audit events, not one direct Ranger callback per original occurrence. Serial Spark
listener drain and bounded audit submitted-count/complete-file-line checks bracket
each case. Zero events are measured absence, not evidence of a fresh permission
grant. Hive mapping has no catalog dimension, so each cell uses a unique database.
Mask/filter evaluation requests and their policy IDs remain unobserved; actual
plans, values and downloaded policy snapshots establish the measured semantics.

Under id-only permissions Kyuubi denies composed collection and createTempView
because the rewritten plan references policy-only region. Six dependent view
probes are explicitly not executed/unqualified, linked to the captured actual
prerequisite denial. A denied retained-frame warmup means its later actual denial
after revoke does not qualify a revoke-on-reuse transition. Independent operations
continue. Counts can succeed with no SELECT audit even for no-grant or authored
references; this is a measured difference. Retained masked frames/views keep M
after refresh to NULL, including a fresh lookup of the previously defined view.
An explicit denied-email collect returns masked NULLs in the comparator fixture.
Delta keeps its original-demand and supported-action freshness contract.

The wildcard COUNT case records the actual Ranger policy outcome and exact
allowed value 4 or denial shape. It is not an independently predicted grant or proof
of Delta/Iceberg permission parity.

Frozen source: `f0c43aa3e5c84b0fcc9252e767ccbcff4b0bafc5`. The fresh shared common JAR reports that exact Git
revision. Each package binds the same source inventory to original build exits,
compiled classes, loaded providers and retained runtime/supplemental JARs. The
candidate has 102 passing acceptance tests. The 736 installed
methods executed at `0030902e16037bc143ec95357e65163f786b294c` and are reused for this candidate through
an explicit equality proof: only the semantic checker, shared live Python runner
and checker tests changed. Installed action-runner, module main/test, init/build
sources, unshaded compiled classes and captured pinned dependencies are unchanged.
Original execution receipts retain their original revision. The common JAR's
build-info resource intentionally changes to the new revision; whole-JAR equality
across revisions is not claimed.

| Spark | Delta | Installed tests | Native Python live cases | Result |
| --- | --- | ---: | ---: | --- |
| 3.5.4 | 3.3.0 | 184 | 42 | 0 failures/errors/skips; original exits 0 |
| 3.5.4 | 3.3.2 | 184 | 42 | 0 failures/errors/skips; original exits 0 |
| 3.5.6 | 3.3.0 | 184 | 42 | 0 failures/errors/skips; original exits 0 |
| 3.5.6 | 3.3.2 | 184 | 42 | 0 failures/errors/skips; original exits 0 |

The private evidence index is
`.superpowers/sdd/2026-10-26-delta-spark-read-parity/final-evidence-index.json`.
It retains all original failures separately. Final index verification rechecks
references/hashes, sources against the frozen Git revision, installed-to-package
class identity, original exits and the subsequent documentation-only delta.
Documentation updates do not relabel package metadata or claim that historical
packages were rebuilt.

### Permission-evidence validator supplement

Runtime qualification remains at `f0c43aa3e`; the original packages, executions
and final evidence index are unchanged. Validator fix `b2ed60551` adds a pure
permission-outcome gate: grant-denial cases require an attributed denied SELECT
result and the exact Delta authorization error. Allowed cases reject contradictory
SELECT denials. Stale-publication and dropped-view failures have separate checks.

The gate passed all 168 immutable original Delta records. This supplements the
original full validation; it is not a new Spark run or a new full-checker run.
The exact AST/source proof preserves all prior checker semantics and all producer,
observer, compiled-class and runtime inputs. Focused tests passed 40/40 and full
acceptance passed 104/104. The supplement index is
`.superpowers/sdd/2026-10-26-delta-spark-read-parity/task-5/fix-round-1/supplemental-index.json`.
Controller re-review remains pending.
