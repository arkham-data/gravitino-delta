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

# Native Delta append with Spark AQE: Iceberg-referenced compatibility

Status: execution scope recorded from the user's “Ok, proceed” and subsequent
explicit native target-cache decision. The user selected native append compatibility
with AQE using Iceberg as the reference, with additional guarantees identified
separately. The user also selected preservation of existing native target-cache
support under AQE. The independent builder check has no remaining material
questions. Implementation follows the standing subagent-driven authorization.
Baseline: `6a2bc1a44fbac8b001cceec3e85ff2c9ee03ab55`, the closed ordinary-read
milestone on `codex/delta-spark-feasibility`. Catalog-demo remains deferred.

## Intent

Make the existing governed named Delta append routes run in a stock AQE-enabled
Spark session. Use native Iceberg and Gravitino's Iceberg wrapper to characterize
what normal append integration actually does, including Ranger enforcement.
Spark supplies adaptive execution; the catalog companion must preserve logical
registration/policy semantics while allowing the stock engine to execute.

This is compatibility work, not a new adaptive engine or a stronger physical-stage
security contract. Preserve previously accepted append entry/precommit checks, but
identify those as Delta integration guarantees rather than inferred Iceberg parity.

## What is known and what remains unproven

- The closed Delta append milestone supports matching-schema, named
  `df.write.mode("append").insertInto(name)` and existing-table
  `df.write.mode("append").saveAsTable(name)`. Source names/types/nullability/order
  match the target, explicit writer options are empty, and the target is PUBLISHED.
  See [current append contract](../../delta-native-governed-append.md).
- Its real public Gravitino grants → Ranger 2.8.0 → MinIO qualification passed
  28 phases across four Spark/Delta pairs, with AQE disabled. That is historical
  evidence, not proof of the new AQE route.
- Current code explicitly rejects adaptive append in `DeltaRangerReadRule` and
  `DeltaRangerPhysicalProvenance.UserTicket`. The source/helper path also relies
  on driver-thread and active-owner checks. Removing the two flags alone would
  not establish correct asynchronous execution.
- Raw target/cache maintenance has a separate AQE restriction in
  `DeltaRangerRawMaintenance`. It must be assessed before enabling a public write,
  because a postcommit maintenance error cannot be described as a write rejection.
- Pinned Kyuubi 1.10.2 describes `AppendData` as an INSERT target and query source.
  Its INSERT maps to Ranger UPDATE; the source retains SELECT. This matches the
  current Delta target access type, but actual callbacks and outcomes must still
  be observed for each tested frontend.
- Existing live Iceberg/Kyuubi comparisons qualified reads. Their seed INSERT
  executes without the authorization extension and does not qualify governed
  append. Upstream Iceberg documents AQE write distribution; that is not a
  substitute for this pinned, deployed Ranger comparison.

## Approaches

1. **Extend the existing append coordinator to trusted stock execution.** Preserve
   Gravitino registration, original source authorization, native transaction/helper
   orchestration and commit checks. Replace only append's incompatible physical
   validation with an operation-bound accepted context, following the already
   approved read/CDF/MERGE architecture. This is the proposed implementation of
   the user's selected compatibility-first direction.
2. Replace the coordinator with direct planning-time delegation. Simpler physical
   integration, but loses existing precommit/current-authority and recovery
   behavior. That is a separate product-contract change, not implied by AQE.
3. Build a replacement general V2 writer or exact adaptive-stage permission system.
   Duplicates native semantics or revives the previously rejected stage-identity
   approach; neither is needed to establish stock Spark append compatibility.

## Scope and constraints

Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server
and clients 2.8.0, and S3-compatible storage with local MinIO. Iceberg comparator
uses the already pinned Iceberg 1.11.0/Kyuubi 1.10.2 artifacts. Inspect loaded
origins and actual deployed versions rather than trusting dependency declarations.

No Gravitino core/API/server/client changes, engine fork, second metastore,
raw-path fallback, replacement transaction implementation, new privilege mapping,
automatic setting changes or concurrent dataset writers. Retain canonical Delta
catalog/schema/table/column resources. Do not copy Iceberg's Hive-service resource
mapping into Delta. Existing MODIFY_TABLE mapping and policy ownership stay intact.

The controller proposes two deliberately separate qualification inventories:

- **Reference surface:** native Iceberg and its Gravitino wrapper, both Spark
  patches, for named insertInto, existing-table saveAsTable, writeTo(...).append()
  and SQL INSERT INTO ... SELECT. Record actual failures and permission semantics;
  do not hide a frontend failure behind a successful alternative.
- **Delta deliverable:** enable AQE for the two already qualified named DataFrame
  append frontends. Keep their existing source grammar, schema/option/transaction
  restrictions, duplicates, constraints, and target registration semantics. Record
  writeTo and SQL INSERT gaps explicitly; they are not completed by this milestone.

Partitioned and unpartitioned targets are part of qualification. Use supported
source shapes for the Delta positive cases; the recent broader ordinary-read
operators and views do not automatically become valid append inputs. A source
that can be collected is not therefore a qualified append source. No source
collection/reconstruction is permitted to bypass that boundary.

This design does not add schema evolution, CTAS/RTAS, overwrite, streaming,
Connect, application cache/checkpoint input, arbitrary writer options, default
transaction auto-reset, CDF-property AQE, separate workers or catalog-demo.
Previously qualified source/target feature behavior must not regress. Include
append to already-CDF-enabled targets and after disablement under the existing
accepted protocol profiles; changing the CDF property with AQE remains separate.
Unqualified Delta features are reported instead of being silently admitted through
AQE.

## Iceberg comparison before product changes

Reuse the existing runtime provisioning, private endpoint/credential loading,
policy-download recording, unique database scopes and native/wrapper setup.
Operate only in new owned fixture scopes. Preserve old result files and services.
The wrapper must actually load `GravitinoIcebergCatalogSpark35`; native-only
success is insufficient. Native-seeded tables and metadata-authorization-disabled
wrapper results remain separate from Gravitino grant propagation.

For each frontend, exercise a source whose visible rows include a masked column
and a hidden row, and a distinct existing target. Permit the source policy-only
column for the positive reference control if Kyuubi demands it, and separately
record the id-only policy-injected-column case as a measured difference. Do not
broaden Delta authored-column demands to match Kyuubi's rewritten extraction.

Capture at least: successful append with source filter/mask; denied source
SELECT; denied target UPDATE with source SELECT allowed; allowed target UPDATE
with source SELECT denied; and target SELECT-only filter/mask policies. Read target
rows and snapshots with a separate authorized verification identity to avoid
hiding committed rows behind target read policies. Include partitioned and
unpartitioned targets and exact duplicate-row multiplicities.

Use an exchange-bearing append with known distribution/input shape. Correlate
write callbacks, SQL executions and completed adaptive stages to the actual
append operation, excluding seed, setup and readback queries. Keep final plans,
materialized exchange/coalescing observations, task/commit results and before/after
snapshots. AQE=true alone, an adaptive source read done separately, or a later
adaptive readback does not pass the write proof. Some valid appends may have no
exchange; label them accurately and retain a separate adaptive positive control.

Keep audited permission events and policy downloads distinct from unobserved
filter/mask callbacks. A missing audit is not an implicit grant. Match actual
resource/access/subject and denied result to the expected failure. Transport,
setup or observer failures are not authorization denials. If an API cannot reach
a write because of a permission prerequisite, record the failure and mark the
adaptive route unqualified; do not invent execution evidence.

The first comparator task must produce a boundary map and measured support table
before Delta production edits. If the reference invalidates a central assumption,
update the design and explain the impact before expanding the implementation.

## Delta execution contract

The Spark process, configured stock extensions and native Delta code are trusted,
as in the approved trusted-AQE design. Supported APIs still validate the current
subject/session/settings, named source/target registrations, original demands,
command identity and transaction intent. Arbitrary code controlling the JVM or
storage credentials is not isolated by this companion.

| Boundary | Required behavior |
| --- | --- |
| Named command construction/entry | Bind the actual V1 fallback input and original source obligations; check target UPDATE and source SELECT/filter/mask decisions before payload work. |
| Authorized duplicate | Recheck current original source and target authority; retain native no-second-write and maintenance behavior. Do not execute source work for a duplicate. |
| Source preparation/native write start | Validate the current bound command/transaction and authorized recipe on the driver; capture the immutable execution context before stock asynchronous work can begin. |
| Within native payload work | Trust Spark's adaptive physical execution of the accepted recipe. Internal callbacks consume the captured context without impersonation, per-stage refresh or requiring the originating driver's ThreadLocal. |
| Immediately before native commit | Preserve current source/target, registration and transaction/action checks. Denial prevents a new committed version. Generated uncommitted files may remain. |
| Postcommit maintenance/recovery | Preserve known-commit/retry handling, native cache/checkpoint behavior and original errors. Do not present committed work as rolled back or automatically replay an uncertain write. |

Entry checks and precommit checks already exist in append; they are not new
Iceberg requirements. Captured execution changes only how supported internal
callbacks obtain the accepted context. Stock stage copies, code generation,
shuffle reuse and asynchronous scheduling do not mint a new operation. A public
query, stale command or foreign transaction cannot borrow the append context.

Classify guards before changing them: retain semantic/source/schema/registration/
transaction/lifecycle bindings; replace physical identity or ambient-thread checks
only where they conflict with the accepted trusted execution model. Keep ownership
cleanup on failure and prevent completed/failed owners from authorizing a new
write. Do not use a global AQE bypass or an ordinary-read ticket for append.

Source filters/masks apply before expressions and written values. Target SELECT
policies remain read semantics, not insertion constraints or write-time masking.
A target also used as a source has separate source duties. No atomic Ranger/Delta
revocation fence is promised: accepted tasks may finish after a refresh, while
an observed denial at the existing precommit boundary prevents publication.

## Native maintenance

The user explicitly selected preservation of existing native target-cache support.
Cold-only qualification does not close this milestone. Audit target snapshot reads,
native cache refresh and checkpoint/reopen paths before enabling append. Distinguish
necessary internal metadata from user payload and application-cached input.

The previously qualified raw target and direct attribute-Project cache profiles
must work with AQE enabled: target-first and Project-first insertion orders,
successful append, authorized duplicate, cold replacement of native cache builders,
and late/repeated native readback after the append owner expires, with unrelated
cache entries preserved. Include the already qualified continued-use maintenance
for CDF-enabled/disabled target profiles. Preserve the existing distinction between
packaged live observations and stronger in-process identity assertions; do not
invent a new cross-session security guarantee. Cached source DataFrames remain
unsupported.

Known unsupported cache dependencies must reject before mutation. No global cache
clearing, disabling AQE or rejecting every warm target is an acceptable shortcut.
Reuse existing trusted native-context mechanisms only where their owner and role
match append. Do not broaden raw-path queries, CDF property mutation or unrelated
maintenance authorization. If a required existing maintenance profile cannot work
under stock AQE, it is an explicit milestone blocker, not a successful cold-only
result or a predictably failing postcommit write.

## Verification and completion

Use behavioral RED/GREEN tests for new logic, then focused installed regressions.
Both named frontends must append exact governed rows to registered targets on all
four Spark/Delta pairs with real Ranger 2.8.0 and MinIO. Establish the public
Gravitino grant/revoke chain for the Delta route, using the existing accepted
append fixture rather than fixture-authored grants alone.

The test inventory must cover successful partitioned/unpartitioned writes,
source/target denials with unchanged committed state, original/pruned demands,
source masks/filters, target read-policy semantics, duplicate transaction identity,
changed policy/registration/subject/settings, both native default-expression/helper-owned and no-default/original-query branches
without altering caller settings, empty input, native constraint failure,
precommit revoke after file generation,
known-commit recovery and affected cache/checkpoint maintenance. Native write
observations must distinguish command completion from actual data/file/commit
work. Preserve existing AQE=false append and ordinary read/CDF/MERGE behavior.

Use deterministic barriers for refresh/failure timing where needed. Observe both
unchanged committed log state and any uncommitted files; do not claim that all
failures leave storage untouched. A refresh after the final observation remains
outside an atomic policy/commit guarantee.

Freeze production/test/harness inputs before the final matrix. Serialize heavy
runs, retain original exits and relevant artifacts, and run affected tests only
when a change or uncovered concern justifies them. Reuse closed evidence only
with explicit unchanged executed-input proof. New live append cases require
actual execution; historical nonadaptive append cannot qualify AQE. No repeated
unrelated read/MERGE matrices for cleaner logs.

Completion requires exact support/limitation tables, original native and governed
results, source/class/runtime provenance, independent task and integrated review,
and documentation of any Iceberg differences. The user authorized milestone
closure pushes; verify the intended branch/remote and preserve the local checkout
and evidence. No PR, deployment or catalog-demo mutation is authorized here.

## Builder choices and remaining observations

The builder chooses small class decomposition, accepted-context representation,
precise positive-control shuffle shape and bounded observation plumbing. The
controller's choices are the finite frontend split, both target partition forms,
retaining existing precommit guarantees, and reusing the established runtime
profile. These are proposals made explicit for review, not newly asserted user
requirements. Preserving existing native target-cache support was explicitly chosen
by the user after the builder check identified that scope decision.

Concrete runtime questions remain for the comparator and first Delta failing
probe: which native write execution owns the adaptive payload; which supported
source shape produces it without widening append grammar; which internal metadata
or cache callbacks need an accepted context; and whether known native policy
rewrite differences affect particular frontend positives. These are measured
implementation prerequisites, not permission to loosen authorization.

Discovery notes: `.superpowers/sdd/2026-10-28-delta-append-aqe/` contains
`delta-discovery.md` and `iceberg-discovery.md`. Memory search/save tools were
unavailable in this session; repo evidence and these notes retain the findings.
