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

# Governed adaptive execution for named Delta reads and MERGE

Status: proposed design, awaiting user approval. On 2026-10-25 the user selected
the recommended direction—real governed AQE before additional demo read operations—
by replying Proceed to the completed [assessment](../../delta-spark-execution-assessment.md).
That selects the direction, not approval of the detailed profile below.
Baseline: `8bec415b9`; completed DataFrame product: `5a2e3676c`.

## Intent and completion boundary

Run the existing named, Ranger-governed Delta read and bounded CDF-to-MERGE
contracts with genuine Spark adaptive execution. Gravitino remains registration
owner; Ranger remains the authority for columns, row filters and masks; stock
Spark/Delta still choose and execute native algorithms within an explicitly
qualified physical profile.

Success requires final adaptive payload plans and owned completed stages, not only
`AQE=true`, metadata-only AQE, or correct rows from manually nonadaptive execution.
The initial profile is deliberately bounded. This milestone is followed by a
separate ordinary-read grammar milestone for the demo's count/distinct/order/display
operations, then separate catalog-demo integration. It does not close those tasks.

Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21
and S3-compatible storage (MinIO locally). No Gravitino core/API/server/client
changes, Spark/Delta fork, second metastore, path fallback, policy migration or
source-payload collection/reconstruction. Keep existing native conversion/profile
limits and the platform's one-writing-job-per-dataset constraint.

## Approaches and selected direction

- Explicit AQE-off integration first would reuse the closed bounded API sooner,
  but would not qualify adaptive execution or migrate all current notebooks.
- AQE-enabled sessions with deliberately nonadaptive governed execution are a
  smaller possible compatibility change, but cannot meet the selected outcome.
- **Selected direction: bounded real governed AQE, followed by observed read
  operations.** Its cost is substantive stage/lifecycle ownership work; its benefit
  is a genuine adaptive execution foundation without changing policy semantics.
- Expanding CDF source transformations first is deferred: no concrete production
  transformation request superseded the existing single bounded-source contract.

The initial physical subset and explicit settings below are the designer's
proposal for review. They are not previously agreed user requirements.

## Included and excluded behavior

Include fresh ordinary named SELECT using the existing Project/Filter/Alias,
conditioned INNER Join and finite scalar grammar; fresh named batch CDF reads;
and the complete existing SQL/JVM/classic-PySpark MERGE path. Include qualified
unpartitioned/partitioned targets, strings/Columns, original assignment demands,
ordered native clauses, same-registration writeback, retries and reconciliation.
Do not broaden reused/derived DataFrame or temp-view admission merely to reproduce
the demo's existing wrappers.

The first physical profile permits stock shuffle and broadcast exchange stages,
complete contiguous coalesced reducer reads, and a narrowly proven empty-stage
result substitution. The latter must originate in a completed owned empty stage,
preserve output identity/types and original demands, and match native no-op
semantics. Statistics estimates or a user-authored empty relation are not proof.
Static native join strategies remain those already qualified for the logical
operation; changing join strategy adaptively is outside this first profile.

Exclude adaptive join conversion, skew/partial mapper or reducer reads, local
shuffle readers, reused exchanges/stages across actions, dynamic pruning/runtime
filters, executable subqueries, custom cost evaluators, foreign optimizer rules,
and unknown physical rewrites. An internal stage referenced repeatedly by the same
owning phase is not a reusable cross-query cache: exact owner and graph evidence
must still match on every use.

Ordinary Sort/Limit/Distinct/Aggregate/Count/Window admission, CDF joins/aggregates/
windows/cache, arbitrary expressions, streaming/Connect, protocol/schema expansion,
new write APIs, policy-subquery support and separate workers remain outside scope.
Existing append, CDF-property and maintenance profiles retain their current scope;
AQE support for MERGE does not advertise new AQE support for those commands.

## Explicit session profile

The caller configures the profile before sources or handles are constructed.
No API, extension or fixture may silently toggle settings to obtain a passing
result. Validate effective values before admitting payload work and bind the
existing full settings context for later drift checks. AQE=false retains the
previous qualified route and is not subject to new AQE-only restrictions.

| Spark setting | Required initial AQE profile |
| --- | --- |
| `spark.sql.adaptive.enabled` | `true` |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | `-1` |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | `0` |
| `spark.sql.adaptive.skewJoin.enabled` | `false` |
| `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` | `false` |
| `spark.sql.adaptive.localShuffleReader.enabled` | `false` |
| `spark.sql.exchange.reuse` | `false` |
| `spark.sql.execution.reuseSubquery` | `false` |
| `spark.sql.optimizer.dynamicPartitionPruning.enabled` | `false` |
| `spark.sql.optimizer.runtime.bloomFilter.enabled` | `false` |
| `spark.sql.optimizer.runtimeFilter.semiJoinReduction.enabled` | `false` |
| `spark.sql.adaptive.optimizer.excludedRules` | absent/empty |
| `spark.sql.optimizer.excludedRules` | absent/empty |
| `spark.sql.adaptive.customCostEvaluatorClass` | absent |

No required value for static auto-broadcast threshold, shuffle partition count or
coalescing size is invented here; the chosen values are captured and their produced
plans must pass physical admission. Tests configure separate sessions for static
broadcast and shuffle cases. A required setting may equal its upstream default;
it need not be redundantly present as a string in SQLConf. Preserve existing
absent-versus-explicit handling in captured settings rather than normalizing drift
away. Test-only forceApply is not sufficient acceptance evidence.

These keys are grounded in [Spark 3.5.6 SQLConf](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala).
The implementation plan must verify both pinned versions' definitions. This is an
explicit opt-in profile, not a claim that unmodified Spark AQE defaults are fully
supported. Relevant current defaults can enable excluded optimizations.

## Ownership model

Extend connector-private evidence with an adaptive execution owner tied to the
existing read/CDF/MERGE authority. It must identify the exact SparkSession,
invoking subject and groups, captured settings, registration observations, policy
snapshot/decisions, original expression and column demands, logical occurrence,
operation, phase and (for MERGE) source attempt/transaction identity.

A driver-only stage permit binds one stock adaptive wrapper, one issued exchange,
its authorized input/output and immutable partitioning, the exact query-stage
instance, and the owning phase. Tags, structural/schema equivalence, a stage ID,
a thread name or serialized data alone cannot create this permit. Canonicalized,
cloned or deserialized authority is inert. The root operation remains the authority;
executor tasks receive only the already authorized native data recipe.

Track the adaptive input, initial plan, each newly executable current plan, and the
final plan. An initial-plan witness cannot stand in for later stages. A transition
must preserve every governed relation occurrence, expressions, policy placements,
scan/file bindings, original column demands and operation semantics. Changes are
limited to issued stage references, supported complete coalescing, and the owned
empty-stage case. Reject added/removed/foreign scans, swapped children, changed
expressions, unowned stage output and partial or overlapping reducer coverage.

Existing native metadata authority stays separate. Reuse its implementation ideas
for exact exchanges and reducer coverage, not its authority to read payload rows.
The classifier must identify native metadata versus governed data by existing
owned provenance, never a class name, schema or path convention alone.

## Lifecycle and authorization boundaries

The lifecycle is per operation/phase/attempt and per issued stage, rather than a
single mutable plan allowed to change arbitrarily after action start:

1. Issue authority while the normal caller's session/subject/settings and existing
   policy/registration checks are valid; bind the authorized logical recipe.
2. Prepare an adaptive wrapper and issue stage permits only for exact owned
   exchanges. Before making a stage executable, validate live authority and current
   policies/registrations; verify the next physical recipe.
3. Materialize only the issued stage. Stage internals freeze once execution starts.
   Completing one stage can permit a separately verified downstream plan revision;
   it never authorizes replacement of a running or retired stage.
4. Before consuming stage output, validate the same owner and current authority,
   completed result/partition map and approved current plan. Validate the final
   plan before exposing collected output or submitting native writes.
5. On success, denial, cancellation or failure, close stage and phase permits and
   release only owned resources. Late callbacks or copied outputs cannot reopen
   them. Idempotent callbacks for the same live stage may observe its recorded
   state; callbacks cannot create a new authorization decision implicitly.

Checks prevent newly authorized work after an observed revocation and prevent
publication/checkpoint advancement on failure. They do not create an atomic
Ranger/Delta revocation fence or promise to retract rows already delivered by an
in-flight authorized task. Document that existing practical limit explicitly.

Spark can prepare/materialize stages asynchronously. Do not make thread-local
ownership global, broadly propagate caller ThreadLocals, impersonate a setup
principal, or set an active session to bypass validation. At the caller boundary,
retain current-context checks. For an internal asynchronous callback, use only a
narrow nonserializable permit issued for that exact stage and still-live query;
revalidate the captured query principal through the existing policy authority and
its bound session/settings/registration context. An unrelated callback with the
same principal has no authority. The concrete Spark hook and safe synchronization
are builder decisions subject to the feasibility gate below; if the required
boundary cannot be observed, reject rather than inventing an unsafe fallback.

## CDF, MERGE, publication and recovery

Standalone bounded CDF may be naturally exchange-free. It must work honestly in
the supported AQE session without inventing shuffles or disabling insertion; label
its observed physical route accurately. Actual adaptive payload proof comes from
exchange-bearing ordinary reads and native MERGE phases, not metadata alone.

For MERGE, preserve the inclusive source interval and exact source occurrence.
Native prepareMergeSource/localCheckpoint remains the materializer. Replace only
the nonadaptive physical-recipe portion of its evidence with the qualified
adaptive stage graph; retain proof that the checkpoint copies the exact owned
source result. Do not accept an arbitrary LogicalRDD, cache or user checkpoint.
Close source authority before target publication, including same-table writeback.

Each native touched-file, matching/rewrite, metrics and partitioned-output phase
has its own owner. Before writes become executable and before commit submission,
recheck the current source/target authority and the exact write/action contract.
Preserve hidden raw rows and omitted-column copy-forward; masks apply to authored
expressions, not as reverse transforms. Row filters retain read semantics with no
new postimage constraint, deduplication or unmasked-key requirement.

Native block-loss retry creates fresh attempt/stage authority. Retired shuffle,
broadcast, source producer and checkpoint evidence cannot migrate into the new
attempt; stock task retry within the same live stage follows the same sealed data
recipe. Reauthorize between attempts. Known-commit reconciliation still proves the
exact native commit and current authority without another write; unknown outcome,
changed intent or revoked duplicate cannot replay. The consumer checkpoint advances
only after synchronous command success and reconciliation.

Raw maintenance/cache invalidation paths must be audited because some currently
reject AQE. Do not silently switch a session to AQE=false or broaden cached-source
admission. The initial profile excludes application cache/reuse; necessary existing
metadata/publication work may retain a separately proven native route. If a current
cache/maintenance condition cannot complete under this profile, reject it before
payload/native mutation and report the condition specifically. Any new transaction
commit followed by uncertain reconciliation must retain the existing recovery
semantics rather than claim rollback or retry automatically.

## Architecture and implementation freedom

Keep all new product code inside the Spark Delta Ranger companion. Likely seams:
`DeltaRangerPhysicalPlanning`, `DeltaRangerPhysicalProvenance`, `DeltaRangerReadRule`,
CDF receipt/scans, MERGE source/native/write owners and extension hook registration.
Use private typed evidence, not a public permit API. Keep the public JVM/Python
builder signatures and API version 1 unchanged unless a demonstrated necessity is
brought back as a design change.

Use stock SparkSessionExtensions adaptive hooks where they provide the required
pre-execution boundary. Whitelist only exact companion-owned hooks; retain rejection
of foreign rules/cost evaluators. Preserve native adaptive insertion where Spark
would normally select it. No monkeypatch, custom engine build or synthetic wrapper
that merely looks adaptive. Exact class names, file split, lock choices and test
helper organization are left to the builder.

The first implementation slice is an installed feasibility gate: an already
admitted named conditioned INNER read must complete with a real final adaptive
payload plan, owned shuffle stages and actual reducer coalescing, while a copied
or foreign stage and revocation-before-next-stage fail. Then prove the same hook
coverage for MERGE's native phases before removing any CDF/MERGE AQE rejection.
Instrument test fixtures if needed; do not claim broad support from this slice.
If stock hooks cannot enforce the boundary without a fork or authority relaxation,
retain the rejection and return that concrete limitation for a design revision.

## Qualification and acceptance

Use the existing task-by-task, independently reviewed workflow. No prior closed
matrix is a setup prerequisite. Start focused installed work on one pinned pair,
expand affected checks as seams change, then qualify all four pairs for the final
candidate. Retain original failures, commands/exits/XML, source/class/archive
hashes and loaded provider/version evidence. Do not relabel earlier AQE=false
results as AQE support; inherit only explicitly unchanged dependencies.

Required acceptance:

- Genuine adaptive ordinary-read and MERGE payload execution on every pair,
  including completed shuffle and static broadcast stages and at least one actual
  coalesced reducer read; distinguish metadata and naturally nonadaptive phases.
  Fixtures must cause the event, not just enable a setting. Record authorized
  initial/current/final plan and stage identities with no credentials/payload leak.
- Paired SQL and classic-PySpark outcomes using actual named sources/handles made
  after profile configuration. Exact row/CDF/action bags, original permission
  demands, masks/filters, hidden-row preservation and static native ambiguity /
  constraint / empty-input behavior remain equivalent to the closed contract.
- Positive standalone bounded CDF in the AQE session; distinct and same-registration
  MERGE, partition moves including escaped/null values, native retry/exhaustion,
  lost acknowledgement, authorized duplicate and unknown-outcome no replay.
- Negatives for invalid profile and settings drift, missing/mixed/copied stage
  ownership, session/subject changes, late/foreign callbacks, stale stage reuse,
  altered scan/manifest/RDD/expressions, unsupported rewrite/partition specifications,
  foreign hooks and revocation between stage preparation, consumption and publication.
  Verify rejection at the appropriate boundary, no unauthorized subsequent tasks,
  no committed mutation/checkpoint advancement on precommit denial, and owned cleanup.
- Fresh packaged Python live evidence with actual Ranger 2.8.0 and MinIO on all four
  pairs: the existing five MERGE phases (distinct, column UPDATE deny, source SELECT
  deny, target MODIFY deny, same-table writeback), plus an ordinary governed adaptive
  join success and referenced-column denial. Retain original outer/Python/native
  exits, identity separation, registration persistence/restart and exact artifact
  bindings. Live fixtures must include adaptive payload/coalescing witnesses, not
  only the small demo's row counts. Separate executors remain a later milestone.
- Targeted AQE=false regressions for changed shared seams. No companion-authored conf mutation
  on success or failure, measured after stock session/analyzer initialization. Unsupported logical grammar stays rejected. Checker tests
  must reject a false AQE label, metadata-only witness, stale/foreign/missing stage
  record, wrong configuration/origin, changed exact bags or missing original exit.

A design-phase review is not runtime evidence. No product support status changes
until this acceptance is complete and independently reviewed.

## Follow-on work and choices

After this milestone, design the smallest ordinary-read additions needed by the
selected demo consumer: count, distinct, sorting/display bounds and safe relation
reuse/temp-view semantics. This does not automatically include grouped analytics,
windowed deduplication or broad CDF transformations. Exact production ELT source
transformations remain a separate open requirement, not a blocker for this design.

Catalog-demo integration remains separate: actual Ranger 2.8.0, named engine-side
execution instead of path/Python wrappers, existing policy subqueries, bootstrap
ownership, shared-session identity, separate executors, storage access and durable
journal placement. No existing demo files/services/policies are changed here.

Reviewable designer choices: coalescing-first explicit opt-in profile; owned empty
stage handling; no default full-AQE claim; complete existing MERGE lifecycle before
closing AQE support; ordinary logical additions and deployment follow separately.
The next artifact after approval is an implementation plan, not product code.
