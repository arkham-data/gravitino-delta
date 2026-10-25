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

# Delta AQE with trusted Spark execution and operation-boundary authorization

Status: proposed revision awaiting approval of the detailed contract. The user
agreed to reconsider Delta's physical-stage checks after the validated Iceberg
baseline. That selects this direction; the timing/profile decisions below are
explicit proposals. Baseline: `437ef5ace`. Catalog-demo remains deferred, and
additional logical operations await the user's missing-operation assessment.

This document proposes replacing the [earlier exact-stage design](2026-10-25-delta-governed-aqe-design.md)
and its blocked implementation plan. It does not change production behavior or
reinterpret the retained failed probes as successful implementations.

## Intent and evidence

Make the existing named Delta read, bounded CDF and SQL/JVM/classic-PySpark MERGE
contracts work with stock Spark AQE while preserving their policy and data semantics.
Gravitino owns registration. Ranger decides column access, row filters and masks.
The companion applies those decisions to Delta logical operations and native
command construction; trusted Spark and Delta execute the resulting plans.

The [Iceberg baseline](../../iceberg-spark-ranger-aqe-baseline.md) demonstrated
real adaptive shuffle/coalescing, filtered/masked results, predicate-column denial
and fresh-query revocation after policy refresh on Spark 3.5.4 and 3.5.6, through
both native Iceberg and Gravitino's wrapper. Its Ranger server and clients were
2.8.0 and storage was MinIO. It did not prove every AQE optimization, Delta CDF,
MERGE target-policy semantics or Gravitino-to-Ranger grant propagation.

The failed Delta probes show that the examined hooks/child guards cannot enforce
exact invoking-stage identity. That additional contract is explicitly withdrawn
in this proposal; it is not reimplemented as exchange identity, stack inspection,
monitor ownership or a renamed stage permit.

## Approaches and direction

- Retaining exact-stage permits leaves the demonstrated blocker and would require
  a new enforceable mechanism or an engine change. No such mechanism is established;
  an engine fork conflicts with the existing constraint.
- Replacing the entire Delta companion with stock Kyuubi would reuse ordinary
  planning authorization, but does not preserve the established Delta resource
  identity, bounded CDF and target MERGE policy contracts.
- **Proposed implementation of the agreed direction:** retain Delta semantic
  integration and its command lifecycle, and trust stock Spark physical execution
  after authorization at supported operation boundaries. This changes assurance
  and revocation timing; the changes are stated below rather than hidden behind
  an AQE flag.

Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17,
Scala 2.12.21 and S3-compatible storage with MinIO locally. No Gravitino core,
API/server/client changes, second metastore, engine fork, path fallback, policy
migration, source collection/reconstruction, new public builder API or automatic
configuration toggles. Public builder API version remains 1. The platform continues
to exclude concurrent writing jobs for one dataset.

## Binding semantics retained

Authorization uses the original named relation occurrences, column references and
authored assignments before pruning or optimization can erase demands. The resource
identity includes the existing canonical catalog/schema/table/column mapping and
Gravitino registration. Do not replace it with stock Kyuubi's database/table/column
mapping or rely on globally unique database names. Path inputs and unregistered
relations still reject at supported entry points.

Filters and masks apply before ordinary user expressions, predicates and joins.
Optimized-away scans, empty inputs and unreachable clauses do not erase original
permissions. Stock physical optimization may change how an authorized expression
executes, not which original permissions are required.

For MERGE, retain the complete native target for hidden-row and omitted-column
raw copy-forward. Apply target filters to visibility/matching and target masks to
authored expressions; keep explicit assignments distinct from native synthesized
copy-forward. Preserve original column UPDATE checks, clause order and native
ambiguity/constraint behavior. No reverse masking, deduplication, stable-unmasked-key
requirement or new postimage constraint. Row filters remain read semantics.

Keep bounded inclusive CDF intervals, history/file availability validation,
registration identity, exact logical source occurrence, native source materialization,
partition moves, same-registration writeback, transaction intent, native retry and
known-commit reconciliation. Standard DataFrame APIs must not be able to substitute
an arbitrary RDD, checkpoint or actioned/cached CDF source for the admitted source.

## Trust boundary and changes to assurance

The Spark process, stock engine implementation, installed companion and configured
extensions are trusted. Deployment controls identity, extension/classpath configuration
and direct storage credentials. This is consistent with the tested Iceberg/Kyuubi
boundary; the companion is not a sandbox against arbitrary JVM code that can mutate
Spark internals, invoke private helpers or directly use storage credentials.

Keep supported-API validation of subject/session/settings, canonical registration,
logical source/target and operation identity. Keep lifecycle state needed to prevent
accidental double execution, stale command reuse and unauthorized retries. Do not
accept user-provided tags, stage IDs, paths or arbitrary DataFrames as a substitute
for registration and logical authorization.

The revision removes the promise to authenticate every physical stage/callback
object, reconstruct an identical physical plan, prove every Spark RDD dependency
identity, or reauthorize each stage materialization/consumption. Stock exchange
reuse, stage copies, code generation, native internal caches and async scheduling
within an admitted engine operation are engine behavior. They do not create a new
policy decision. Cross-action application caches and previously unsupported source
reuse do not become supported through this change.

Exact logical/command identities may remain where they enforce the public operation
contract. An existing object-identity check must be classified before changing it:
semantic source/transaction/lifecycle checks stay; physical tamper proofs that conflict
with trusted stock execution are replaced. There is no blanket deletion of guards.

## Authorization timing

Use the existing local Ranger policy engine and refresh mechanism. A current check
means the engine's currently available policy snapshot; it is not an RPC to Ranger
Admin on every boundary and does not promise instantaneous propagation.

| Boundary | Required behavior |
| --- | --- |
| Analyze/construct named operation | Resolve registration and subject; capture original demands; authorize and apply filters/masks to the logical recipe. |
| Begin a supported ordinary-read or standalone-CDF action | Revalidate caller/session/settings, registration and current decisions before submitting its payload work. Changed decisions reject a stale recipe; a fresh query can capture the new policy. |
| Inside that Spark action | Execute its authorized immutable decision/recipe context with stock Spark. No per-stage policy refresh fence or exact-stage permission check. |
| Begin each native MERGE source attempt | Revalidate source and target authority and bounded source identity; bind a new attempt to the existing command. |
| Begin native touched-file and write phases | Revalidate on the command's driver boundary before that phase can submit new payload work; freeze the phase's authorized recipe. |
| Immediately before commit submission | Recheck current source/target authority, registrations, exact action/transaction intent and existing target-head constraints. Denial prevents commit and consumer-checkpoint advancement. |
| Reconcile a known commit / user retry | Preserve existing current-authority checks, durable exact-commit proof and no-second-write behavior. Unknown outcome or changed intent cannot replay. |

An ordinary read already authorized and in progress may finish and return its result
if policies change during that action. It does not gain a new final-output revocation
check. A subsequent supported action must revalidate; a stale retained plan may reject
instead of being transparently rewritten. This is a deliberate replacement of the
proposed stage/consumption revalidation promise. It does not expand retained DataFrame,
cache or temp-view admission beyond the existing contract.

A MERGE phase already running may finish its submitted tasks after policy change.
The next listed driver boundary must reject if authority changed. A denial after
file generation but before commit may leave uncommitted files; it must not publish a
new Delta version or advance the consumer checkpoint. The external policy store and
Delta commit do not form an atomic revocation fence. After a commit, uncertain
registration/cache reconciliation retains the existing recovery state; do not claim
rollback or start another write.

## Execution architecture

Retain the companion's logical rewriting and native command orchestration. The
current read receipt, CDF receipt and MERGE receipt remain the sources of original
demands, registration and operation intent. Refactor their current-context/policy
checks away from callbacks that stock Spark runs asynchronously.

At an authorized driver boundary, prepare a private immutable execution context
for the operation or native phase. It holds the already accepted decisions and
source/transaction recipe required by companion helpers. It is not a stage permit,
not a public API, and not a new path for constructing arbitrary user operations.
The exact representation and file decomposition are builder choices.

Internal callbacks use this captured context for deterministic recipe/resource
checks rather than borrowing ambient ThreadLocals, switching active sessions or
impersonating a setup principal. Current caller checks remain at public entry and
listed command boundaries. Executor closures contain the native data recipe, not
Ranger administration credentials or mutable authorization engines.

Keep raw Delta log/statistics maintenance and payload query roles distinct. Necessary
native metadata access is part of the admitted operation's implementation; it must
not become a public path or exempt arbitrary user scans. Native source checkpoint
capture must occur through the existing command's materializer and remain bound to
its source interval/attempt, without requiring every downstream Spark RDD to retain
a fixed object graph through AQE.

Use normal pinned Spark planning/preparation and adaptive insertion. Stop comparing
adaptive output to a manually rebuilt nonadaptive physical tree for authorization.
Record final plans/stages for qualification and diagnostics, not as the capability
that grants access. Do not introduce synthetic adaptive wrappers, engine subclasses
that change native algorithms, reflection-based production permits or a second
optimizer. Existing Delta helper/command subclasses may continue adapting authorized
expressions and native phases as they do today.

The currently qualified AQE=false route stays operational during the change. No
extra weak/strong authorization switch or public API version change is introduced.
Documentation for the completed revision will state its operation-boundary contract;
unmodified legacy checks may remain stricter but are not a portable promise that
all configurations provide stage-by-stage revocation.

## Stock Spark configuration and scope

Proposed configuration change: replace the earlier fifteen-setting restrictive
profile with AQE enabled under the pinned stock Spark/Delta rule set. No user must
disable normal coalescing, adaptive join changes, skew handling, local shuffle
readers or intra-query exchange reuse solely to satisfy physical identity checks.
These are trusted engine optimizations of admitted operations, not new logical APIs.
The builder must investigate any concrete incompatibility in Delta helper integration
and fix that integration or report a specific limitation; it cannot silently switch
AQE off or restore the blanket physical-stage restriction.

Preserve effective configuration capture and drift checks at the listed caller/
command boundaries, including existing handling of Delta's postcommit session
metadata. Keep restrictions on disabling authorization, foreign planning/columnar
extensions and custom cost evaluators. Do not apply the stock extension's SQL SET
checker as a claim to sandbox arbitrary programmatic configuration changes. Establish
the actual accepted extension ordering and protected keys through existing startup
qualification; exact known companion hooks may be allowed where implementation needs
them, without a broad exemption for foreign rules.

Initially retain the existing fresh ordinary-read logical grammar, bounded CDF
source grammar, supported scalar/types/schema/protocol profiles and bounded native
SQL/JVM/classic-PySpark MERGE surface. Ordinary count/distinct/order/window and
broader source transformations await separate scope. Appends, CDF property changes,
other write APIs, policy subqueries, streaming/Connect and separate-worker deployment
do not acquire AQE support merely because this work succeeds. Catalog-demo remains
deferred until the user resumes it.

## Implementation sequence and qualification

Implement and independently review one task at a time; this is a design sequence,
not the implementation plan. Preserve original results and avoid repeating unaffected
closed matrices.

1. Establish an ordinary named Delta read through the revised logical/action boundary
   with real stock AQE on one pinned pair, retaining original demands and current
   action checks. Demonstrate masked/filtering join results, column denial, policy
   change before action and new-query revocation. This is a Delta execution proof,
   not another stage-object feasibility probe.
2. Extend the context to standalone bounded CDF and the native MERGE source/phase
   lifecycle. Keep source materialization and complete MERGE gate changes together
   where the existing receipt constructor makes them inseparable. Preserve supported
   partitioned/unpartitioned and same-registration cases.
3. Verify native errors, retries, phase-boundary revocation, precommit rejection,
   known-commit recovery and no duplicate write. Retain original public API signatures
   and packaged classic Python behavior.
4. Qualify the final candidate on all four Spark/Delta pairs with actual Ranger 2.8.0
   and MinIO. Link original commands/exits, loaded artifacts, source/class hashes,
   exact rows/CDF/actions/checkpoints and observed adaptive plans to that candidate.

Acceptance uses semantic and operation-boundary evidence:

- Fresh SQL and classic PySpark named reads and existing MERGE API on all four pairs,
  with actual final adaptive payload stages/coalescing. Standalone exchange-free CDF
  is labeled honestly. Metadata-only AQE and forceApply-only evidence cannot qualify.
- Exercise representative stock physical variants, including default AQE behavior,
  coalescing, static broadcast, adaptive join conversion/local reader and intra-query
  reuse when the admitted query produces them. Use targeted low-threshold skew/runtime
  filter cases where changed helper seams can encounter those branches. Record which
  branches actually occurred; do not equate enabled settings with branch coverage or
  claim every combination was tested. Fixture tuning is explicit before construction.
- Keep exact original permission demands, filter/mask order, catalog disambiguation,
  escaped/null partition behavior, hidden/raw target preservation, column-only grants,
  explicit assignment versus copy-forward, and empty/optimized-away demand checks.
- Deterministic timing tests: revoke/change a mask before action and require denial;
  change policy after a read begins and allow completion under its captured decisions;
  require the next action/query to observe the updated engine state; revoke between
  MERGE phases or before commit and require no committed mutation/checkpoint advance.
  Do not require a running Spark stage to stop at an internal policy refresh.
- Reject unsupported public inputs, path/unregistered sources, changed caller context,
  stale source intervals, wrong logical source/checkpoint, changed transaction intent,
  disabled authorization and foreign extensions. Keep double-execution and retry
  isolation tests. Arbitrary direct construction/invocation of Spark stage objects
  is no longer an admission security test for the trusted-engine model.
- Native source block-loss retry, exhaustion, acknowledgement loss, authorized and
  denied duplicates, unknown outcome, cache/publication failures and current authority
  at reconciliation retain their existing behavior. Inspect new failure paths for
  cleanup of operation/phase context without implementing a global callback permit.
- Fresh live packaged-Python evidence keeps the five existing MERGE phases plus an
  ordinary adaptive read success and predicate-only column denial on each pair.
  Include fresh-query revocation and Gravitino registration/resource mapping evidence;
  the Iceberg fixture's disabled metadata authorization and native-seeded tables do
  not satisfy the Delta chain. Preserve and verify actual grant propagation where
  exercised rather than substituting locally authored policies for that claim.
- Run focused AQE=false regressions for changed seams. Evidence checkers must bind all
  stock and supplemental wrapper/provider artifacts, original process exits and final
  plans, exact bags and policy transitions. Preserve failed attempts and explicitly
  unknown fields. Do not inherit the Iceberg checker's supplemental-JAR coverage gap.

## Decisions to review and builder freedom

The proposed choices are operation/action authorization with captured in-flight read
decisions; retained MERGE phase/precommit/reconciliation checks; trusted stock physical
optimization instead of exact-stage permits; and stock AQE configuration instead of
the former coalescing-only restrictions. These choices need approval as a complete
contract before implementation planning.

The builder chooses context types, synchronization, file split, native helper adapter
refactoring and test organization. It must map each existing guard to a retained
semantic/boundary check or a deliberately withdrawn physical-hardening promise.
If stock native integration cannot retain a required data/policy semantic, report
that precise gap before relaxing it. This is not permission to substitute a new
policy meaning, engine fork or silent configuration fallback.
