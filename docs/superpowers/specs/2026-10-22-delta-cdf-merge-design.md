# Governed named Delta CDF-to-MERGE pipelines

Status: approved for task-by-task execution on 2026-10-22; product implementation
starts with Task 1. The user confirmed target row-filter read semantics and deferred
the catalog-demo/separate-worker deployment to its own plan. Baseline: `b321beace14a2313e54aee033d883df30d47f6c7`.

## Outcome

Execute a bounded named Delta CDF DataFrame through Spark transformations into a
separately authorized named Delta MERGE, without collecting source rows on the
driver or converting them back into a DataFrame. Include a distinct destination
and writeback to the same exact named registration. Preserve native Delta row
algorithms and existing Ranger policies. This is the next functional ELT milestone,
not completion of full Delta/Iceberg feature parity.

The existing CDF producer already implements executor RDD execution. The missing
work is admission and authorization across an owning MERGE, its repeated source
reads/materialization, target rewriting, commit and registration publication.
Native collection of file names or aggregate metrics is permitted; collection of
CDF payload rows as the bridge into MERGE is not.

## Global constraints

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21.
- Actual Ranger 2.8.0 and S3-compatible MinIO qualification.
- Gravitino owns named registration; Delta owns transaction and row reconstruction semantics.
- No Gravitino core/API/server/client changes, second metastore, or Spark/Delta fork.
- Existing Ranger access, row filters and masks govern source and destination user expressions.
- No unmasked/unique-key admission requirement, reverse-mask lookup, automatic deduplication or live policy migration.
- The platform prevents concurrent writer jobs per dataset; native task/source-materialization retries still require qualification.
- Preserve the existing worktree and accepted evidence; rerun earlier tests only for changed dependencies or concrete regressions.
- Retain separate setup, metadata and authenticated application identities; never transfer setup credentials into application execution.

## User surface and initial limits

Support both native frontends, independently tested:

```python
from delta.tables import DeltaTable

changes = (spark.read.option("readChangeFeed", "true")
    .option("startingVersion", "12").option("endingVersion", "15")
    .table("lake.db.source"))
prepared = changes.where("_change_type IN ('insert', 'update_postimage', 'delete')")
(DeltaTable.forName(spark, "lake.db.target").alias("t")
    .merge(prepared.alias("s"), "t.id = s.id")
    .whenMatchedDelete(condition="s._change_type = 'delete'")
    .whenMatchedUpdate(condition="s._change_type <> 'delete'", set={"value": "s.value"})
    .whenNotMatchedInsert(condition="s._change_type <> 'delete'",
                         values={"id": "s.id", "value": "s.value"})
    .execute())
```

Also support SQL MERGE with a source subquery over literal named `table_changes`.
Both routes produce one internal command contract. The example is an explicit
consumer program, not a generic replica algorithm: discarding preimages can leave
stale destination rows when source rows cross a policy boundary. Fixtures must
exercise that boundary rather than describing the example as complete replication.

Use one bounded CDF source, its existing deterministic scalar projection/filter/
alias grammar, and native MERGE ON/ordered matched UPDATE or DELETE/unmatched
INSERT clauses. Expand user stars with original-demand provenance. Keep explicit
integer start/end and the accepted fixed historical schema/profile. Support classic
reader-1 targets and the already accepted CDF-enabled profile, including native
AddCDCFile output, because same-table writeback necessarily writes a CDF table.
Use native schema constraints; disable/reject schema evolution for this milestone.

Do not admit WHEN NOT MATCHED BY SOURCE, standalone UPDATE/DELETE statements,
streaming, time travel, arbitrary path writes, unrelated source joins/windows/
aggregates/UDFs, arbitrary user cache/checkpoint, DV/column-mapping/row-tracking,
or generalized Spark action admission through this work. These are explicit parity
backlog items, not permanent catalog restrictions. Different named registrations
pointing at the same physical dataset reject initially; the same exact named
registration in the source and target roles is required to work.

## Existing policies and target semantics

Source behavior remains the closed CDF contract: original SELECT demands, native
row-image filtering, masks, then user transformations. Keys and virtual metadata
are ordinary policy-governed values. No raw event metadata is exposed to repair
consumer logic. Empty sources retain their original authorization obligations.

The user selected target read semantics, not a new postimage write constraint.
Evaluate the target Ranger row filter on raw target rows. User ON, clause conditions
and assignment RHS expressions see masked target values. An authorized INSERT or
UPDATE may create a row outside that filter and therefore invisible afterward.
Do not invent a WITH CHECK policy or change Ranger's service definition.

Keep the complete native target relation intact. Rewrite user matching as:

`rawTargetPolicyFilter AND userON(governedTargetValues, governedSourceValues)`.

Rewrite target references in original user clause predicates/RHS similarly, bound
by resolved attribute identity and source/target occurrence. Apply masks once;
never remask their internal raw reference, the policy predicate, or the already
governed CDF source. Hidden target rows do not match; a source row with a comparable
physical key may consequently INSERT another row. Do not consult hidden keys to
suppress that insert. Mask collisions retain submitted SQL/native MERGE semantics,
including multiple-target updates or native multiple-source ambiguity errors.

Delta rewrites whole touched files. Hidden/unchanged rows and unassigned columns
must retain their original stored values. Raw copy-forward is internal storage
preservation, not a raw SELECT surface. Capture authored assignments before native
preprocessing synthesizes default self-assignments: omitted `secret` remains raw;
explicit `SET secret = target.secret` reads its mask and stores that governed value.

### Authorization demands

Use existing Ranger SELECT and UPDATE access types; no new INSERT/DELETE type.
The following column contract is proposed for implementation and live qualification:

| Operation | Original demands |
| --- | --- |
| Source/target user expression | SELECT on every referenced column, including ON, clause predicates and RHS |
| Matched UPDATE | UPDATE on explicitly assigned target columns; user star expands to actual fields |
| INSERT | UPDATE on all physical target columns, including omitted native-default/NULL fields |
| DELETE | UPDATE on all physical target columns |
| Unassigned/unchanged native copy-forward | No additional user SELECT/UPDATE demand merely for copying |

Preserve original demands even when a clause is unreachable or optimization removes
an expression. A literal UPDATE can require UPDATE without SELECT on that assigned
column. Do not add a wildcard UPDATE prerequisite that blocks legitimate column-only
matched-update grants. Public MODIFY_TABLE continues to publish UPDATE + ALTER;
MERGE itself requests the required UPDATE privileges. Prove wildcard grants with
column denies, column-only grants, roles and scheduled changes using actual Ranger.

## Native integration and alternatives

Recommended: a companion command implements public native `MergeIntoCommandBase`
with `ClassicMergeExecutor` and `InsertOnlyMergeExecutor`. Own only bounded
runMerge transaction/orchestration and commit validation; delegate touched-file
selection, joins, clause evaluation, rewrites, inserts and CDC generation to native
protected methods. This duplicates a small amount of version-pinned orchestration;
it is not an untouched stock MergeIntoCommand invocation. Compilation and installed
execution are the first gate, before broad policy implementation or runtime packaging.

SQL and DeltaMergeBuilder reach preprocessing differently. Capture both original
named operands and authored demands before Delta PreprocessTableMerge aligns
assignments; associate only the resulting owned command. Never authorize by an
unknown command's matching name/path/schema. DeltaFullTable rejects a filtered
MERGE target, so filtering the target relation itself is not the design.

A wrapper around the stock command is insufficient: its private commit orchestration
does not expose the needed precommit boundary. Reimplementing all MERGE row
algorithms would duplicate Delta and is rejected. If the protected-trait approach
fails its first installed proof, report the exact inaccessible/lifecycle seam and
revise this design before widening admission or copying native row algorithms.

### Source materialization and attempts

Native AUTO identifies the opaque CDF source as non-Delta and uses localCheckpoint.
Qualify that native materialization, the exact issued materialized RDD, repeated
reads, pruning, cleanup and native block-loss retries. Do not turn this into generic
user persist/checkpoint support or infer authority from an equal RDD schema.

Retain original source policy/identity/interval evidence independently of any
materialized lineage. Revalidate before each native attempt and later execution
phase, and immediately before commit even when the original source producer is
bypassed. Each retry has a distinct attempt lifetime; copied/serialized/stale source
receipts cannot mint a new attempt. Keep current AQE=false prerequisite; do not
silently alter user settings. User grammar stays finite while native joins,
aggregations, metrics UDFs and write helpers get exact phase-owned admission.

### Transactions, writebacks and failures

Bind source and target roles separately to their named registrations and native
UUIDs. Same-table writeback fixes/materializes the CDF interval before mutation;
close source execution before publication, then accept only the target owner's
proven native advancement. Old source receipts remain invalid after that change.
Source aliases may not acquire authority by sharing a path.

MERGE needs its own commit intent for DeltaOperations.Merge and exact returned
AddFile/RemoveFile/AddCDCFile/SetTransaction actions. Existing append intent accepts
Write/add/txn only and must not be relabeled or broadened globally. Validate policy,
subject/config, registration/schema/native identity and source evidence after file
generation and before commit. Preserve native constraints and atomic commit behavior.

Retain native txnAppId/txnVersion duplicate handling when supplied, with autoReset
false. A duplicate still requires current authorization. Record a typed immutable
MERGE attempt intent and publication outcome in the companion's private journal
space; bind a reused transaction identity to the same command/source interval.
Do not treat the registration journal as already supporting MERGE replay. A known
committed attempt may reconcile registration from exact native commit evidence;
an unknown outcome must not automatically rerun MERGE. Native retries before a
commit and caller retries after an uncertain publication are different cases.

Only after confirmed native publication and successful registration reconciliation
may the caller advance its source checkpoint. The caller owns durable source
identity/end and sink progress. A success with zero visible events can advance;
denial/incomplete history/native ambiguity/constraint failure cannot. Policy-only
changes still require consumer reconciliation. There is no cross-system atomic
Ranger/Delta revocation fence, automatic replica repair or exactly-once sink claim.
Uncommitted orphan files after a failed write are possible; committed log/rows must
remain unchanged on precommit denial. Never delete uncertain evidence to retry.

## Qualification and exit criteria

1. First-pair installed proof: both named frontends, native trait linkage, source
   materialization/repeated reads, target preservation and actual precommit hook.
   Include same-table CDF writeback and CDF-enabled target actions as an early gate.
2. Focused behavior: positive target filters/masks, hidden rows sharing touched files,
   explicit versus synthesized assignments, wildcard/column denials, colliding keys,
   native ambiguity, empty source, missing history, stale/copy/foreign authority,
   block-loss retry, post-helper revocation and ambiguous publication recovery.
3. Four Spark/Delta pairs: affected tests plus fresh original Ranger 2.8.0/MinIO
   source-to-target pipeline outcomes, exact native final rows/actions, independent
   SELECT and MODIFY grants/revokes, checkpoint outcomes and registration readback.
4. Deferred deployment (separate plan, not this milestone exit gate): catalog-demo
   uses an isolated overlay with qualified packages, actual Ranger
   2.8.0 and MinIO; demonstrate both frontends and a separate executor process on
   Spark 3.5.4/Delta 3.3.0. Do not upgrade/rewrite its existing policies or services
   silently. Existing compose declares Ranger 2.7.0; an unused Dockerfile declares
   2.5.0. Neither satisfies this milestone. Additional pairs' cluster deployment
   is not implied by their local executor qualification.
5. One integrated review of Tasks 1–5 and their local qualification, clear
   support/parity matrix, closure commit and authorized
   branch push with remote equality. Preserve evidence and worktree; no PR.

## Remaining parity work

Track each item as supported, explicitly rejected or unqualified: general DataFrame
composition/actions, broader expressions and joins, schema evolution/history,
standalone UPDATE/DELETE, target-only MERGE clauses, streaming/checkpoint lifecycle,
Delta feature profiles, other distributed/runtime/auth/storage profiles. The bounded
CDF-to-MERGE pipeline must not be used to claim full Delta or Iceberg parity.

## Discovery references

Local detailed findings: `.superpowers/sdd/2026-10-22-delta-cdf-merge-planning/`
`execution-discovery.md` and `policy-discovery.md`. Existing public contracts:
[batch CDF](../../delta-cdf-consumption.md),
[native append](../../delta-native-governed-append.md).
Pinned source references:
[Delta 3.3.0 native MERGE](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/commands/MergeIntoCommand.scala),
[preprocessing](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/PreprocessTableMerge.scala),
[Kyuubi 1.10.2 command descriptors](https://github.com/apache/kyuubi/blob/v1.10.2/extensions/spark/kyuubi-spark-authz/src/main/resources/table_command_spec.json).
Six principal native source entries matched across the two local Delta source JARs;
that source comparison does not replace the first compile or four-pair execution.
