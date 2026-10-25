# Companion named DataFrame MERGE builder — critical prerequisite

Status: promoted by the user on 2026-10-25 to the next critical priority and a
prerequisite for catalog-demo integration. The named SQL and partitioned MERGE
milestones are complete. The user approved the API design and implementation plan
on 2026-10-25. JVM Task 1 is locally qualified on Spark 3.5.4 with both Delta pins. Python Task 2
is implemented and live-qualified on Spark 3.5.4/Delta 3.3.0 with Ranger 2.8.0 and
MinIO. Task 3 installed policy/lifecycle qualification passed 37 cases per Delta pin
on Spark 3.5.4; Task 4 final four-pair packaged qualification passed: five actual Python live phases per pair and fresh 51-case installed suites on both Spark 3.5.6 pins; unchanged Spark 3.5.4 evidence has explicit source-valid inheritance. Final independent SPEC/QUALITY review approved the milestone; the work is committed locally.

The approved [DataFrame API design](../specs/2026-10-25-delta-dataframe-merge-design.md)
defines strings and Columns, the shared SQL command path, Python packaging and
qualification. The [implementation plan](2026-10-25-delta-dataframe-merge.md)
supersedes the outline below for execution. All four tasks are complete and passed independent integrated review. Supported install/use and exact grammar are documented in [governed MERGE](../../delta-cdf-merge.md#named-dataframe-api).

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

1. Build on the qualified SQL integration contract to design the companion named
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
   DeltaTable.forName compatibility. Completing this qualification is a required
   entry gate for the [catalog-demo deployment plan](2026-10-22-delta-cdf-merge-demo.md),
   whose application must exercise the DataFrame API from PySpark. This plan ends
   with API qualification; catalog-demo integration is a separate plan.

## Dependency on catalog-demo

The demo must consume the qualified DataFrame API, including distinct-target and
same-table writeback to partitioned targets, with the existing Ranger policy and
checkpoint semantics. Named SQL remains the behavioral reference. A SQL-only demo
does not satisfy this integration milestone. Separate-worker qualification remains
part of the demo plan after the API's local four-pair qualification.

Before activating that separate integration plan, assess whether the intended
pipelines require broader Spark execution: AQE, joins, aggregations, windows,
caching or expressions beyond the qualified grammar. These are candidate
prerequisites, not supported features or an automatic expansion of this API
milestone. Any required expansion needs its own scoped plan and qualification.
Completing the DataFrame API alone does not establish readiness for integration.
The current catalog-demo session explicitly enables AQE, so that mismatch already
needs a separate scope decision before integration; this API does not change it.

## Evidence and dependencies

Current diagnostic report: `.superpowers/sdd/2026-10-22-delta-cdf-merge/task-1-report.md`.
The native trait probe passes but does not prove governed MERGE execution or this
builder design. Depends on [the SQL implementation plan](2026-10-22-delta-cdf-merge.md)
and [its policy/lifecycle design](../specs/2026-10-22-delta-cdf-merge-design.md).
