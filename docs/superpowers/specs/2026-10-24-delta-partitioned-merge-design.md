# Partitioned named Delta CDF-to-MERGE targets

Status: approved by the user's subsequent "Proceed" after design review; execute
task by task. Design baseline: `1e35d8d39022b4a608d1ae983d7d42bd96ef0e95`.

## Intent and inherited decisions

Extend the qualified named SQL CDF-to-MERGE path to partitioned Delta targets,
including partition-key updates and same-table writeback. ELT applications retain
the same SQL surface, including classic PySpark `spark.sql`. Gravitino owns table
registration; Delta owns file routing, row reconstruction and transaction actions.

The user selected this feature as the next priority. The engineering approach and
test breakdown below are approved, using the already approved
[MERGE policy and lifecycle contract](2026-10-22-delta-cdf-merge-design.md).
The companion DataFrame builder and separate-worker catalog-demo deployment stay
in their own plans.

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12.21, JDK 17.
- No Gravitino core/API/server/client changes, second metastore or engine fork.
- Keep fixed schema and partition metadata, the existing classic/CDF protocol
  profiles, AQE disabled and the bounded CDF source and SQL clause grammar.
- Existing Ranger SELECT/UPDATE demands, row filters and masks remain authoritative.
  No new write constraint, unmasked-key requirement or automatic policy migration.
- Keep native task/source retries, exact registration/policy/session checks and
  current publication/recovery semantics. The platform prevents concurrent writer
  jobs for one dataset; this does not remove task retry obligations.
- Preserve the worktree and original evidence. Keep follow-up commits local.

## Approach

Extend the existing owned native MERGE path. Let Delta perform partition routing,
repartitioning, touched-file selection, writes and CDF generation; recognize only
the precise additional plans produced inside the authorized write phase.

This is the viable approach within the existing engine-fork and policy constraints.
A custom partition writer would duplicate native storage semantics. Disabling
Delta's repartition setting or permitting only immutable partition keys would
avoid part of the problem while leaving this feature incomplete. Neither is the
proposed solution. No separate partition-level authorization model is introduced.

## Current implementation and required seams

`DeltaRangerMergeWrite.write` explicitly rejects nonempty partition columns. Its
owned writer recipe currently describes the unpartitioned output and optional CDC
projection. Native Delta's `MergeIntoCommandBase.writeFiles` can first repartition
the output by the table's partition columns, controlled by the existing native
setting and optimized-write branch. Therefore removing the guard alone is unsafe.

The native writer also normalizes empty string partition values to null before
CHECK evaluation. Its exact `Empty2Null` projection must be retained where the
installed native branch produces it; checks must see the same values as stock
Delta. This is internal write normalization, not a new admitted user expression.

The same method derives statistics plans from output attributes and table schema.
Partition fields must be excluded from native file statistics in both schema
selection branches, as the existing `DeltaRangerAppendHelper` already does; the
CDC routing marker is also excluded. Reuse that pattern without an unrelated
append or global admission refactor.

Registration bindings already compare partition metadata between the named
descriptor and native snapshots. CDF history requires unchanged partition
metadata across the requested interval, and partitioned CDF read tests already
exist. These contracts remain intact. Native source similarities between Delta
pins are design evidence, not runtime qualification.

The durable MERGE command fingerprint currently lacks an explicit ordered partition
stamp. A partitioned-target intent must bind both source and target partition
identity using the existing bound native partition stamp. Use a distinguished
partitioned-target encoding so the previous unpartitioned-target fingerprint and
retained journals remain compatible, including an already partitioned CDF source.
Never fall back to the legacy fingerprint for a partitioned target or rewrite old
journals automatically. Test the encoding and recovery boundary explicitly.

## Behavior and ownership

1. Bind partition columns, order and types to the exact authorized target snapshot
   and transaction. Resolve native partition references against the actual owned
   output; never authorize a user or copied plan by matching schema alone.
2. Recognize the exact native repartition successor when the supported native
   configuration produces it. Preserve the native no-repartition branch when its
   setting is false. Settings remain captured and revalidated, never silently
   changed. Optimized writes remain outside scope unless already admitted.
3. Retain exact expression/attribute/child ownership through the writer carrier,
   physical preparation and any partition sorting. Admission of these internal
   steps must not admit arbitrary user repartition, sort or cache plans.
4. Keep native partition path escaping, null handling and file/CDC action creation.
   Do not reconstruct partition values from masked keys or directory strings.
   The native CDC routing marker is not a user partition field or a persisted
   action partition key. Timestamp partition action values and directory names
   can have different native representations; compare through native decoding,
   not naive path-string/value equality.
5. Preserve full raw target rows for touched-file reconstruction. Target row
   filters constrain matching; user expressions see masks. Hidden rows and omitted
   fields retain stored values. Explicit assignment of a masked partition field
   stores that governed value and may move the row to another partition.
6. An authorized partition update may make a row invisible under the existing
   filter. No postimage filter constraint is added. Hidden rows cannot suppress an
   authorized INSERT, including when their raw keys collide with the source.
7. Preserve exact AddFile/RemoveFile/AddCDCFile/SetTransaction intent and commit
   proof. Partition values and paths in those actions must survive evidence checks
   and recovery unchanged. Failed authorization cannot advance committed state or
   the application checkpoint; uncommitted orphan files remain possible.
8. Same-table writeback fixes the CDF interval before mutation, closes source
   authority before publication and reconciles only the owned target advancement.
   Old source receipts and alternative registrations for the same dataset reject.

## Task sequence and proof

Work proceeds task by task, with implementation and independent review for each.

**Task 1 — Native partitioned writer integration.** Reproduce the current rejection
on installed Spark/Delta and actual Ranger policy fixtures. Implement the smallest
owned repartition/statistics correction. Prove native inserts, updates moving a
partition key, deletes and target CDF on the first pair, then native linkage and
that focused regression on the second Delta pin. Exercise repartition enabled and
disabled, both statistics schema settings, and CDF enabled/disabled targets.
Use a partition key independent of the visibility field so a hidden row and a
visible row demonstrably share a rewritten file; verify that membership before
the MERGE and preserve an untouched partition's original file actions.
Add rejection tests for changed partition expressions, foreign children, copied
writer authority and changed settings. Include empty-string normalization before
native CHECK evaluation and durable ordered partition identity with legacy
unpartitioned-target compatibility. Retain current unsupported feature guards.
This task establishes the native integration; it does not close live qualification.
Start the installed proof with string and ordered multiple partition keys. Retain
native partition-type validation and exact owned-plan admission; do not invent a
new permanent type allowlist. If a particular conversion introduces an unmodeled
native step, keep that case rejected until the step is explicitly owned and tested.

**Task 2 — Governed partition semantics and lifecycle.** Prove filtered/masked
partition fields, explicit versus omitted assignments, hidden and visible rows
sharing a touched file, untouched partitions, partition-key moves and same-table
writeback. Include null and escaped string values and ordered multi-column
partitions with representative non-string scalar values supported by the existing
table profile; include timestamps under UTC and a non-UTC session timezone.
Compare native rows and CDF event multisets plus file action maps;
do not assert randomly generated filenames. Include original column denies,
empty/unreachable demands, policy change after file generation, stale partition
metadata, native ambiguity/constraint failure and known-commit recovery. Partition
action mutation tests must change partition maps while keeping paths fixed, as
well as changing paths and the CDC marker, to verify complete intent binding.
Partition type support claims must follow the actual cases; unexercised profiles remain
explicit qualification obligations rather than silently acquiring support.

**Task 3 — Four-pair qualification and documentation.** Run the affected focused
tests and fresh partition-specific Ranger 2.8.0/MinIO acceptance on all four pairs,
with real PySpark SQL, independent native observers, identity/artifact provenance,
persisted registration and checkpoint outcomes. Reuse existing fixture machinery,
not historical runtime packages. Cover distinct destination and same-table
writeback, masks/filters and denial. Run one integrated review, update support
documents to the exact evidence and commit locally. Do not replay unrelated closed
append/CDF matrices or claim separate-worker deployment.

## Builder decisions and exit boundary

The implementer chooses private helper structure and fixture decomposition after
tracing the installed native plan, keeping changes concentrated in MERGE ownership
and tests. No new public API is required. If native execution exposes a new
authorization or publication mechanism rather than a bounded owned successor,
report that concrete seam and revise the design before widening admission.

The feature closes only when partition routing, governed row preservation, CDF,
same-table writeback and current authorization/publication checks have retained
four-pair evidence. Schema evolution, column mapping/defaults, deletion vectors,
row tracking, AQE, insert-only optimization, broader SQL grammar, builder and
deployment remain their existing separate obligations.
