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

# Bounded ordinary Spark read parity for governed Delta

Status: approved by the user with “Approved” after the reviewed design at
`255480dd9`. Detailed implementation planning follows; no new runtime
qualification is implied. Baseline:
`683ba8976a8973f2027b217143782dccd9ccac1a` (previous milestone closed and pushed).
The user selected ordinary-read parity next and explicitly selected Spark-style
column permissions: `spark.table("t").select("id").count()` must work with access
to `id` without grants on the other columns. This document adds no runtime
qualification. Catalog-demo integration remains deferred.

## Intent and constraints

Make ordinary named SQL and classic PySpark/DataFrame reads useful for the next
integration: count, distinct, ordering, limits, display and Pandas conversion,
including composition and bounded local temporary views. Preserve Gravitino-owned
registration, Ranger policy semantics and the completed trusted-Spark AQE contract.
No Gravitino core/API/server/client behavior changes, Spark/Delta fork, second metastore,
path fallback, public MERGE API change or automatic session-setting changes.

Qualify Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21,
Ranger server and clients 2.8.0, and S3-compatible MinIO. The existing platform
single-writer constraint remains. This milestone adds no writes.

## Approaches

The selected direction is to extend the existing companion's ordinary-read route,
retaining original authored permissions and captured execution decisions. Source
audits identify a pre-optimizer normalization seam for deferred authorization and
a separate candidate action hook for native Arrow. Both need installed tests.

Keeping eager all-column parent authorization would preserve today's simpler
receipt lifecycle but reject the user's selected projection/count example.
Replacing the companion with stock Kyuubi would reuse a broader grammar, but its
rewritten-plan permission extraction and retained-plan lifetime are not the same
as our original-demand and next-action contracts. Neither alternative satisfies
the selected requirements. Merely adding operators to an allowlist would miss
composition, native scan preparation and Arrow action entry.

## Supported surface and explicit bounds

The following finite choices are proposed engineering scope decisions. They do
not claim that these cases already work.

| Surface | Included | Outside this milestone |
| --- | --- | --- |
| Count | DataFrame `count()`, SQL global `COUNT(*)`, `COUNT(1)` and `COUNT(top_level_column)`; one or more count outputs with aliases | Grouped aggregates, COUNT DISTINCT, aggregate FILTER, arbitrary aggregate arguments, SUM, windows |
| Distinct | SQL DISTINCT and DataFrame `distinct()` over the full child output | Subset `dropDuplicates`, zero-column deduplication |
| Ordering | Global orderBy/ORDER BY on top-level columns, explicit ascending/descending and null ordering; sort-only columns retain permissions | Arbitrary new sort expressions, sortWithinPartitions |
| Limits | Literal nonnegative integer LIMIT/limit, including zero and composed limits | OFFSET and tail |
| Display | Actual classic PySpark show, including its formatting projection and extra-row limit | General new formatting/UDF grammar |
| Pandas | Native toPandas with Arrow off and Arrow on with fallback disabled; primitive fixture types below | Nested/complex type conversion expansion, streaming, Spark Connect |
| Composition | Existing finite scalar project/filter/alias/conditioned inner-join operations composed with these new operators | General RDD/LogicalRDD/local payload substitutes, arbitrary transforms |
| Temporary views | Same-session local DataFrame-backed createTempView/createOrReplaceTempView, resolution and drop | SQL-text CREATE VIEW, global/permanent/definer-rights views, cache/persist/checkpoint |

Collect/collectAsList/Python collect, count, head/take underlying show and both
Pandas routes are the selected terminal actions. Iterator, foreach, RDD APIs,
isEmpty and write actions acquire no new support claim. Existing ordinary read
profiles remain; record actual adaptive or nonadaptive plans rather than changing
settings to make a probe pass.

Append AQE follows separately. CDF source grammar, MERGE source transforms,
policy subqueries, broader execution/deployment and catalog-demo are separate
work. Ordinary composition must not admit consumed CDF carriers or turn them into
reusable ordinary sources.

## Permissions and relational semantics

Treat the implicit schema of a named table as metadata, not an explicit payload
projection. Payload authorization occurs when the complete query is planned,
before optimization, and is checked again at each supported action. Planning or
explain may trigger it; this is not a promise of action-only initial checking.
Existing metadata/registration checks still apply to schema discovery.

Retain final output columns and every explicit authored projection, predicate,
join, sort and count reference per original named relation occurrence. An explicit
inner projection remains a demand when an outer count/limit removes its values.
Do not infer permission demands from the optimized or masked result.

| Authored operation | Required original SELECT demand |
| --- | --- |
| `spark.table(t).select("id").count()` | id |
| `spark.table(t).collect()` | All returned columns |
| Explicit `SELECT *` or `select("*")`, then count | Expanded explicit columns |
| `COUNT(email)` under a constant or NULL mask | email |
| Bare `COUNT(*)`, `COUNT(1)` or `SELECT 1 FROM t` | Existing table-wide SELECT resource, column `*` |
| Count after authored predicate/join | Predicate/join columns for each occurrence; table-wide fallback only for an occurrence with no authored column demand |
| Row policy references region but query only selects id | id; policy dependency is not an authored region demand |

Use the existing `DeltaRangerResourceKey.select(empty)` contract. This adds no
Ranger access type. A single-column grant must not be substituted for the empty
set's table-wide request; actual wildcard/deny precedence is Ranger 2.8's decision.
Preserve every original registration and demand even if optimization removes all
its scans. Zero-column input/output needs an explicit ordinary route, never a
general relaxation of Kyuubi-input-count or provenance checks.

Apply row filters and masks before user expressions. Count(*) counts visible rows;
count(email) counts non-null masked values. Distinct collapses masked values and
retains a single null according to Spark semantics. Sort sees masked values.
LIMIT 0, false predicates and empty inputs still authorize original demands;
COUNT over empty input returns one row containing zero. Raw Delta statistics must
not bypass these semantics. An unordered limit promises only Spark's native subset,
not stable row selection; ordered tests use a unique tie-breaker.

## Ordinary-read lifecycle

Keep an analyzed, unrewritten named origin for ordinary composition. Bind its
relation occurrences, registration provenance, session, subject, settings and
expression lineage. Schema inspection and view creation may carry this pending
origin, which grants no payload execution authority.

Use a distinct ordinary branch of the existing plan-normalization integration
(`DeltaRangerRawNormalization` / `DeltaRangerSparkExtension`) to clone the completed
original query, extract demands, authorize, capture filters/masks, rewrite once
and issue the existing executable decision receipt. Pinned QueryExecution puts
normalization before cache substitution and optimization. Verify that ordering on
both patches. Do not reuse CDF authority, mutate the origin, accept generic tags,
reconstruct SQL text or reload raw relations to recover lost provenance.

The check rule validates pending origins; pre-CBO preparation and physical execution
require an issued receipt. Extend the separate ordinary operator admission,
exact receipt binding, optimizer-prefix replay, no-file-skipping preparation and
scan ownership together. Bind sort direction/null ordering and count expression
fields explicitly. Do not open shared policy/CDF/MERGE scalar grammar or typed raw
metadata routes to these operators. Optimized-away/zero-column scans retain their
original obligations without pretending that a count result is an empty relation.

Lifecycle rules:

1. A schema-only frame can be composed without eager all-column grants. Changed
   identity/session/settings or registration invalidates its bound origin.
2. A newly planned query evaluates current policies against its complete original
   intent. Its masked executable plan has an immutable receipt.
3. Every supported action checks current locally applied Ranger decisions and
   existing registration, subject and settings bindings before payload preparation.
   A stale already-planned executable receipt rejects; it is not silently rewritten.
4. An accepted in-flight action finishes using captured decisions. The next action
   must recheck. There is no mid-stage or final-output revocation fence.
5. New composition from an intact original recipe creates a new planned query and
   can capture newer policies. It cannot derive authority from previously masked,
   cached, checkpointed or collected payload. Repeated actions on the same planned
   QueryExecution remain subject to step 3.

Preserve aliases/self-join occurrence identity despite ExprId remapping. Never
apply the same mask twice because a frame was already analyzed or used earlier.
Cache/persist/checkpoint routes remain rejected before materialization or payload
substitution; rejected attempts must not leave reusable cache authority.

Local DataFrame views store pending original intent and output lineage, not a
payload authorization or captured masked result. The consuming complete query
uses canonical base-table resources, not a Ranger resource named after the view.
A newly resolved query observes replacement; a previously analyzed query retains
its bound definition and undergoes current action checks. Drop prevents fresh
resolution; an already resolved query follows Spark's retained-definition behavior
with registration/policy checks. Verify this with native Spark characterization.
Cross-session use and unsupported view definitions reject. A view created from an
explicit projection retains that demand even if its consumer prunes it.

## Native action entry and Arrow feasibility gate

The current outer guard accepts through executeCollect. Native Arrow toPandas
instead reaches plan.execute from a serving thread. Its current accepted-read
flag is not sufficient for fresh and repeated Arrow actions. Asynchronous listeners
cannot supply synchronous authorization before preparation.

A source-backed candidate is the root resetMetrics call inside Dataset.withAction,
before the action callback. Bind it to the exact SQLExecution QueryExecution,
outer root and session, not a local property alone. Perform existing action
validation before any child preparation, then reuse that action's capture in
executeCollect/doExecute and adaptive callbacks without double authorization.
Preserve separately owned direct collection/helper routes.

This hook is unproven. The first implementation task must characterize it on both
Spark patches before broad operator implementation. Required proofs include:

- Fresh, repeated and mixed collect/Arrow actions, including empty/limit-zero;
  actual Python Arrow enabled with fallback disabled.
- Correct authenticated subject/local user/session on the Arrow serving thread;
  never install the receipt's old subject to make validation pass.
- Denied-first and deny-after-success, policy/filter/mask/registration/settings
  drift, with no payload jobs or prepared static broadcast before rejection.
- Captured in-flight completion, next-action rejection, exception/cancellation
  cleanup and successful subsequent unrelated query.
- No new authority from arbitrary metric resets, stale execution IDs, foreign
  roots or internal adaptive callbacks; no accepted-context reuse across actions.

The builder may choose a different proven stock synchronous hook within the same
contract. If no hook works without a fork, report the concrete blocker and request
a scope decision on native Arrow; do not silently disable Arrow, monkey-patch
PySpark or substitute a companion method while claiming native parity. Parallel
actions on the same retained frame and shared multiuser sessions remain outside
qualification; this does not permit new cross-query context leakage.

## Qualification and Iceberg comparison

Use stock Spark as the relational-result oracle and measure Iceberg/Kyuubi's
permissions separately. Existing Iceberg evidence covers governed joined reads,
real AQE and fresh-query denial, not these APIs. Its optimized/re-written demands
and retained checked plans are source-based reasons to measure differences, not
newly observed vulnerabilities or a mandate to weaken Delta's contract.

Use `(id BIGINT, region STRING, email STRING, secret STRING)` with rows
`(1,east,z,s1)`, `(2,east,a,s2)`, `(3,east,NULL,s3)`, `(4,west,hidden,s4)`,
`(5,east,b,s5)`. Filter region=east. Reuse supported constant `'M'` and typed-NULL
masks without adding CASE policy grammar. With no email mask, visible row count
is 4 and email count 3; constant mask gives email count 4 and one distinct value;
NULL mask gives email count 0 and one distinct null. Masked email then id ordering
is ids 1,2,3,5, which distinguishes it from raw null-first lexical ordering.

Probe table-wide, id-only, no-grant and denied-email identities; authored versus
policy-only region references; explicit inner projections; sort-only denied
columns; self-join occurrences; empty/false/zero-limit reads. Include schema-only
construction and explicit SELECT-star behavior. Test composition and local views
before and after planning/action, replacement/drop, registration changes and
confirmed policy refresh, including mask/filter changes with SELECT still allowed.

Installed coverage must test newly admitted logical shapes and optimizer products,
unsupported neighbors, zero-column/count ownership, action boundaries and unchanged
CDF/MERGE/append exclusions. Live packaged tests must execute actual classic PySpark
count/distinct/orderBy/limit/show/toPandas and local-view composition on all four
Spark/Delta pairs. Pandas scope includes long, nullable string, boolean, double,
decimal, date and timestamp (fixed recorded session timezone); record Pandas/PyArrow
versions and dtypes separately from normalized result values. Exercise Arrow off
and required-on; no JVM-only substitute for actual Python transport.

Extend the existing Iceberg 1.11.0/Kyuubi 1.10.2 comparator with only these new probes:
native Iceberg and actual Gravitino wrapper on each Spark patch (four comparator
cells). Retain actual Ranger requests/resources and outcomes, including retained
frames/views. Count can have metadata/local execution with no exchange; do not
label that an adaptive-payload proof. Use a separate exchange-bearing read for AQE.
Keep existing unique database scopes for Ranger's lack of a catalog resource in
that comparator. No grants-propagation or production-authentication claim follows.

Record source/artifact hashes, loaded engine/Ranger versions, exact inputs/results,
policy refresh/application, original exits and executed plans. Preserve failed
attempts. Reuse unaffected prior evidence rather than replaying the closed suite;
rerun affected ordinary-read and cross-route regression cases after lifecycle edits.

## Bounded harness follow-up

Alongside the relevant tests, synchronize installed native phase observations with
a bounded wait tied to submitted job IDs; distinguish nonadaptive plans from truly
exchange-free plans; move routine plan evidence out of WARN logs. Correct the
pre-existing Git worktree HEAD lookup in build-info generation, limited to build
metadata and with a focused main-checkout/worktree/detached-HEAD check. Do not
suppress denial/fault stacks or treat Gradle/dependency warnings as task failures.
None of these repairs requires replaying unaffected historical live evidence.

## Sequence and completion boundary

After design approval, prepare the detailed implementation plan in this order:

1. Characterize and qualify native action entry, especially Arrow; resolve the
   feasibility gate before broadening operators.
2. Implement deferred ordinary origins, complete-query authorization and finite
   composition; preserve all original demand and cross-route boundaries.
3. Add count/distinct/order/limit, display and local-view support with installed
   positive, negative and optimizer-elimination cases.
4. Run new packaged four-pair Delta qualification and bounded Iceberg comparator,
   integrate the harness repairs, review and update support documentation.

These are ordered work packages, not newly approved product implementation. The
builder chooses internal type names, test factoring and artifact layouts within
the contract. Stop widening scope once the acceptance matrix passes. Close and
push the branch only after the bounded milestone passes review, under the user's
standing milestone-push authorization. Append AQE and catalog-demo stay separate.

## Design evidence

Local source audits (read-only; no new runtime results):

- `.superpowers/sdd/2026-10-26-delta-spark-read-parity/admission-audit.md`
- `.superpowers/sdd/2026-10-26-delta-spark-read-parity/iceberg-audit.md`
- `.superpowers/sdd/2026-10-26-delta-spark-read-parity/builder-check.md`
  (fresh reviewer: nothing important left)

The Iceberg audit contains pinned primary-source references; the admission audit
records inspected local Spark bytecode and companion symbols. See also
[the completed AQE design](2026-10-25-delta-trusted-spark-aqe-design.md),
[execution assessment](../../delta-spark-execution-assessment.md) and
[Iceberg baseline](../../iceberg-spark-ranger-aqe-baseline.md).
Memory tooling was unavailable; findings are retained in these local artifacts.
