# Governed named batch Delta CDF reads

Status: approved by the user on 2026-10-20; implementation has not started.
Baseline: `0f94d951122dc58ff15ce46bda5d736f82b5cfb8`, the closed and pushed
[catalog CDF enablement milestone](../../delta-cdf-enablement.md).

## Purpose and confirmed requirements

The user needs CDF for incremental ELT pipelines, including merge/upsert processing
and writebacks. Existing Ranger policies must apply, including row filters and
masks on keys. The catalog must not invent keys, require unmasked access, remove
filters, migrate policies automatically, or decide the pipeline's business key.

This delivery provides the governed source-event read. Governed destination
MERGE/UPDATE/DELETE and writeback authorization remain subsequent implementation
work. Accepted matching-schema append does not provide those operations. A passing
CDF read is not evidence that the complete ELT pipeline is implemented.

Retain these constraints:

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21.
- Actual Ranger 2.8.0 and local S3 qualification with MinIO.
- Gravitino owns named registration; Delta owns the log and reconstruction.
- No Gravitino core/API/server/client changes, second metastore or Spark/Delta fork.
- Ranger owns access, column permissions, row filters and masks. The metadata
  identity remains separate from the authenticated application subject.
- The platform prevents concurrent writer jobs per dataset. Reads do not acquire
  permission to modify the source or its registration through a new writer.
- Preserve the existing worktree and accepted evidence. Reopen earlier milestones
  only for concrete changed dependencies or regressions.

## User-facing contract

Admit a named, nonstreaming read with explicit inclusive integer version bounds:

```python
changes = (spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", "12")
    .option("endingVersion", "15")
    .table("catalog_name.database_name.table_name"))
```

Also qualify the named SQL surface using the same underlying operation:

```sql
SELECT id, value, _change_type, _commit_version, _commit_timestamp
FROM table_changes('catalog_name.database_name.table_name', 12, 15);
```

The DataFrame route is implemented and characterized first; SQL normalization
follows within this milestone. The reader's format string is not proof of catalog
identity: Spark's named table method resolves the identifier and options.

Require `0 <= start <= end <= observed native head`. Require both bounds; omitted
or latest ends, timestamp bounds, conflicting options, ordinary time travel,
unknown options and path/Hadoop overrides reject in this first delivery. Direct
path reads and `table_changes_by_path` remain unqualified under the governed
extension. This bounded option set is not full native CDF API coverage.

Support one CDF source with the existing admitted projection/filter expression
family. Do not implicitly admit joins, aggregation, ordering, streaming, arbitrary
RDDs or SQL cache/persist through this route. Existing ordinary SELECT behavior
retains its own qualification. Prove the supported CDF query grammar using fresh
plans; a derived or copied plan does not acquire authority from an earlier read.

## Existing Ranger policies are authoritative

Authorize the current canonical catalog/database/table and demanded columns for
the application subject. Historical data does not restore historical grants.
Apply the current table row filter to each native row image, then apply column
masks, then evaluate user predicates against the governed values. Reuse existing
Ranger expression validation and reject unsupported expressions explicitly.

Keep all four native event types before policy projection: `insert`, `delete`,
`update_preimage`, `update_postimage`. Preserve duplicate rows and native metadata
values before Ranger transforms; never deduplicate, fabricate a primary key,
pair images by file order, or silently convert a preimage into a delete.

The three CDF fields are virtual output columns on the same table:
`_change_type` (string), `_commit_version` (long), `_commit_timestamp` (timestamp).
They participate in Ranger SELECT and masking under their literal column names.
Existing wildcard policies can cover them; explicit grants/denials remain
meaningful. Do not add them to Gravitino's stored physical table schema. Reject
payload-name collisions and incompatible native output types.

Filters and masks may reference the bound CDF output where supported by the
existing expression grammar. If Ranger masks event metadata, return the policy's
permitted value or reject an unsupported expression; never reveal the raw value
because an ELT consumer would find it convenient. Native event fidelity therefore
means no connector-invented event transformation, subject to Ranger's projection.
The implementation may use original metadata internally for its own proof, but
that is not authority to expose it.

Metadata-only projections still apply the row filter and require the demanded
metadata permissions. Optimizer simplification or zero visible rows cannot erase
original source/column authorization obligations. CDF admission does not require
a unique, present or unmasked business key and does not reject duplicate source
keys merely because some downstream MERGE strategy cannot use them.

## ELT interpretation and checkpoint boundaries

A filtered native feed is not automatically a complete replica of the currently
visible table. For a filter `region = 'US'`, an update US→EU can expose only the
preimage; EU→US only the postimage. A consumer that ignores all preimages can leave
stale rows behind. Include this behavior in examples and acceptance tests.

A consumer maintaining a masked projection can, under appropriate application
key and policy-stability assumptions, process commits in order, retract visible
preimages/deletes, then apply visible postimages/inserts. That is consumer logic,
not a transformation performed by this reader. Native CDF has no update-pair ID,
primary-key guarantee or deterministic row ordering within a commit. Colliding
masked keys and multiple changes per key require an application-defined strategy.

Policies may change the result of reading the same interval again. A policy-only
change creates no source Delta event for unchanged rows; this connector cannot
retroactively remove data already delivered or reconcile a destination by itself.
Do not promise policy-stable replay, complete view replication, exactly-once
application or automatic policy-change resnapshotting in this milestone.

The pipeline owns its checkpoint and sink transaction. It should retain source
native identity and the successfully processed requested end version, and advance
to end+1 only after successful sink application. A successful empty filtered batch
can still cover the requested interval; maximum visible `_commit_version` is not
a reliable cursor, especially when metadata is filtered or masked. A denied,
incomplete or failed batch must not be treated as empty success. Do not serialize
driver-local authorization receipts as durable checkpoint authority.

Writebacks require independent target authorization and a defined target-key
mapping. A masked source token does not automatically authorize or identify the
original unmasked target row. This design adds no reverse-mask lookup or write
permission.

## Architecture and alternatives

Recommended: retain the completed ordinary named load as an identity anchor, then
issue a separate CDF binding before Delta copies/lowers the native V2 relation.
Keep ordinary scan registration and its exact-object checks intact.

The binding captures the original named catalog/table objects, registration/native
UUID and location, interval, read-schema snapshot/version, protocol/partition
profile, subject, policy publication, configuration and exact output attributes.
Name/location equality or copied descriptor tags are insufficient authority.

Use an adapter-owned reader invoking native `CDCReaderImpl.changesToDF` and its
protected `scanIndex` extension point. Native code continues to select explicit
CDC versus inferred add/remove changes, suppress native no-op events and compute
commit timestamps. The companion owns only bounded admission and provenance.

An owned carrier/relation must explicitly call that reader: inherited
`getCDCRelation` alone still returns a relation calling Delta's singleton reader.
Capture the operation before nested analysis/physical planning begins. Spark
calls `CatalystScan.buildScan` during planning, so an executor-only guard is too
late. Native empty results, unions, log reconstruction, index file matching and
all scan branches need finite owner-specific handling.

Alternative: wrap the stock relation's callback. This reduces reconstruction glue
but gives no direct per-index issuance hook and makes nested planning provenance
harder. Alternative: construct all event scans ourselves. That gives direct
ownership but duplicates Delta's CDC/no-op/schema/timestamp semantics. Prefer the
native protected hook; establish its runtime feasibility before extending the
query surface or building the live matrix.

The first implementation gate must demonstrate one real named bounded read and
the exact internal plan/scan ownership on Spark 3.5.4 / Delta 3.3.0. If the protected
hook cannot preserve the contract without a fork or broad raw-plan admission,
stop that approach and report the concrete incompatibility; do not weaken the
contract to make the positive case pass.

## Historical data and completeness

Native batch CDF reconstruction uses `failOnDataLoss=false` for its change-log
iterator. Therefore native success alone does not prove a complete ELT interval.
Use bounded native public log access to prove every version from start through
end exists, including the terminal version, before consuming the same validated
actions for reconstruction. Do not accidentally validate unrelated versions
beyond end. Bind the action evidence and generated scan inventories so validation
and execution cannot silently select different versions/files.

Reject missing/corrupt history, unavailable historical data/CDC files, disabled-CDF
gaps and permissive ignore-missing/ignore-corrupt/out-of-range settings. Surface
failure without checkpoint advancement; do not silently repair, replay writes or
return an empty replacement. A source may advance compatibly after the bound end,
but the selected interval must remain fixed. Unexpected identity/schema/policy
changes invalidate the retained operation.

Three scan branches are required:

| Branch | Native index | Required behavior |
| --- | --- | --- |
| Inferred inserts | `CdcAddFileIndex` | Read the interval's historical adds; CDC files need not exist. |
| Explicit change files | `TahoeChangeFileIndex` | Preserve all native event types; do not double-count associated add/remove actions. |
| Inferred deletes | `TahoeRemoveFileIndex` | Read removed historical files, not only current snapshot files. |

Initially require payload schema, column identities, partitioning and supported
protocol to be compatible with the authoritative current registration throughout
the requested interval and the native read-schema snapshot. Reject schema changes,
column mapping, deletion vectors, row tracking and unsupported feature transitions
in this first profile. Historical actions must be checked as well as current
metadata. Broader historical schema/features remain explicit full-Delta work.

## Authorization lifetime and caches

Recheck subject, configuration, current registration/native identity and Ranger
publication at the existing qualified action boundaries. Reauthorize original
obligations and verify the effective masks/filter, including time-dependent
rules. Retained work rejects after an observed grant/revoke/role/mask/filter change
and requires a fresh read. This is not instantaneous Ranger Admin revocation or
cancellation of already running executor work.

Issue native helpers only for the captured operation, interval and exact native
objects. Never grant generic history access from a thread-local flag, class name,
equal schema, or CDF-looking relation. Account for multiple historical scan
branches per original source obligation. Bind returned RDD/physical child identity
and revalidate around the physical action boundary.

Keep governed SQL cache substitution rejected. Native reconstruction caches are
separate; validate their ownership for cold and warm execution without a global
cache purge. Copied, serialized, reused or foreign carriers cannot mint authority.

## Acceptance and sequencing

1. Prove named entry capture, native reconstruction and all three scan branches
   on the first runtime pair. Capture original failures and focused passing cases.
2. Implement policy projection and action/provenance checks for the bounded
   DataFrame route. Verify denied columns, existing filters, payload/key/metadata
   masks, metadata-only demand and copied/foreign ownership negatives.
3. Add the named SQL route using the same owned operation. Keep path and timestamp
   routes explicitly rejected. Verify interval completeness, historical files,
   empty/dataChange=false commits and fixed-end reads after compatible advancement.
4. Qualify native event parity: append inserts, explicit deletes and update pairs,
   inferred deletes, mixed and partitioned commits. Compare full row multisets and
   exact permitted metadata, including duplicate keys and repeated source rows.
5. Exercise all four filter transitions, masked-key collisions without reader
   deduplication, changed policies before retained actions, schema/feature rejection,
   native cache orders and ordinary SELECT/append/enablement regressions only where
   shared implementation changes require them.
6. Package the final changed source and qualify all four pinned version pairs with
   actual Ranger 2.8.0 and MinIO, production discovery and separate identities.
   Prove public SELECT grant/revoke downloads and Ranger-authored filters/masks.
   Use controlled native setup for source UPDATE/DELETE/MERGE events; that setup
   does not qualify those mutations through the governed catalog.
7. Record the supported source-read contract, original runtime/artifact bindings
   and remaining write/stream/schema work, review the integrated delta once, then
   close this milestone. Governed destination MERGE/writebacks are the next
   functional ELT step; streaming CDF requires a separate lifetime/checkpoint design.

The implementation sequence is recorded in the
[implementation plan](../plans/2026-10-20-delta-cdf-consumption.md).
Design approval does not claim runtime support.

## Source anchors

- [Current named-load binding](../../../spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java)
  and [read/authorization rule](../../../spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerReadRule.scala).
- [Delta 3.3.2 CDF relation and reconstruction](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala)
  and [named/path SQL functions](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/DeltaTableValueFunctions.scala).
- [Delta 3.3.2 version iteration](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala)
  and [Spark 3.5.6 scan planning](https://github.com/apache/spark/blob/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala).

Read-only discovery found the six relevant Delta reader/table/function/index
sources identical between 3.3.0 and 3.3.2; this is source evidence, not runtime
qualification. Local discovery reports are retained under
`.superpowers/sdd/2026-10-20-delta-cdf-consumption/`.
