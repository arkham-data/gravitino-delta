# Companion named DataFrame MERGE builder — deferred plan

Status: deferred by user choice on 2026-10-22. Do not execute during the current
named SQL MERGE milestone or count it as supported. This follow-up needs a reviewed
API design and implementation-ready task breakdown before code changes.

## Outcome and constraints

Provide a companion entry point for a bounded named CDF DataFrame to MERGE into an
exact Gravitino-owned named Delta registration. Reuse the qualified SQL command,
authorization and lifecycle contract. Preserve existing Ranger policies, native
Delta row algorithms, Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 and Ranger 2.8.0. No
Gravitino core/API/server/client changes, second metastore, engine fork or raw-path
fallback. Keep actual installed support distinct from native DeltaTable API parity.

## Proven reason for a companion API

The Task 1 probes on both Delta pins show that DeltaTable.forName rejects canonical
three-part names and then uses V1 metadata lookup. DeltaMergeBuilder resolves and
preprocesses directly before exposing the complete command to extension hooks.
An omitted column and an explicit target self-assignment become identical actions;
their authorization/masking semantics differ. The companion must retain authored
clauses before preprocessing; it cannot reconstruct that distinction afterward.

## Planned sequence

1. After the SQL integration contract is qualified, design the companion named
   factory/builder, supported languages, clause grammar and exact registration
   binding. Specify how original expressions and command ownership are retained.
2. Prove installed named creation and original-assignment capture before native
   preprocessing on both Delta pins. Bind only the exact native successor; reject
   copied/foreign commands and raw paths. No expression-helper ordering inference.
3. Reuse the qualified SQL command for CDF materialization, native phases, policies,
   publication and recovery. Test SQL/builder equivalence for explicit versus
   omitted masked fields, hidden target rows, column demands, native ambiguity,
   same-table writeback, retries and publication failure.
4. Qualify four pinned runtime pairs with actual Ranger 2.8.0/MinIO and independent
   review. Publish the companion API and limits without claiming stock
   DeltaTable.forName compatibility. Coordinate separate-worker deployment with
   the deferred demo plan.

## Evidence and dependencies

Current diagnostic report: `.superpowers/sdd/2026-10-22-delta-cdf-merge/task-1-report.md`.
The native trait probe passes but does not prove governed MERGE execution or this
builder design. Depends on [the SQL implementation plan](2026-10-22-delta-cdf-merge.md)
and [its policy/lifecycle design](../specs/2026-10-22-delta-cdf-merge-design.md).
