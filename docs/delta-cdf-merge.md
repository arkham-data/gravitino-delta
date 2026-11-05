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

# Governed named Delta CDF-to-MERGE

The optional Spark 3.5 Ranger companion owns a bounded named SQL MERGE over
`table_changes`. Gravitino owns registration and authorization configuration;
Delta's native executors reconstruct rows and produce transaction actions.
Historical fixed-schema local qualification completed on Spark 3.5.4/3.5.6 ×
Delta 3.3.0/3.3.2 with actual Ranger 2.8.0, MinIO, JDK 17 and Scala 2.12.21.
The additive schema-and-data MERGE and additive-history CDF qualification
described below is installed fixture evidence; it adds no deployed Ranger server, provider, MinIO or Python
result. Separate-worker deployment remains unqualified.

## Trusted stock adaptive execution

The revised contract authorizes supported operations at their action boundaries
and trusts the pinned stock Spark optimizer, planner and AQE execution. A running
read may finish using its captured decisions. The next supported action rechecks
the locally applied Ranger state; server revocation must first be downloaded and
applied. MERGE also rechecks each source attempt, touched-file/write phase and
precommit or known-commit reconciliation. There is no final-output or every-stage
revocation guarantee, and no atomic Ranger/Delta revocation fence.

Set `spark.sql.adaptive.enabled=true` explicitly at application startup, before
constructing a session, source or table handle. The companion never toggles it.
Keep the companion extension followed by Delta's extension, each exactly once;
do not also install stock `RangerSparkExtension`. Foreign planner/columnar
extensions, a custom adaptive cost evaluator and disabled authorization remain
rejected. The public named builder API remains version 1, with the same packaged
`gravitino_delta` facade and existing source/target semantics.

Local qualification uses Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0,
Java 17, Scala 2.12.21 and MinIO. Installed tests exercise stock defaults,
coalescing, static and adaptive broadcast, local shuffle reads, skew splitting,
exchange reuse, runtime bloom filtering, native MERGE phases and recovery.
Deterministic branch fixtures record their startup settings; they are not
mandatory product configuration or hidden runtime toggles. The complete MERGE
write join is FullOuter; broadcast hash joining and runtime bloom pruning are not
claimed for that join. Native touched-file broadcast/bloom cases are qualified
separately by the installed suites.

Each pair's fresh live cell uses the packaged classic-Python DataFrame builder
for partitioned distinct-target and same-registration MERGE, plus column UPDATE,
source SELECT and target MODIFY denials. An ordinary named join succeeds and a
fresh predicate-only SELECT fails after a uniquely scoped test-authored column
denial. That denial is labeled as fixture policy, while table grant/revoke evidence
retains actual Gravitino-to-Ranger propagation. Final ordinary and MERGE payload
plans are bound to original files; coalescing is observed in native touched/write
phases. Bounded source CDF is naturally exchange-free and is recorded that way.
No metadata-only or forced adaptive wrapper counts as payload qualification.

The evidence index is
`.superpowers/sdd/2026-10-25-delta-trusted-spark-aqe/task-4/final-evidence-index.json`.
It retains original exits, exact row/CDF/action bags, partition maps and untouched
files, registration across restart, separate setup/metadata/application identities,
policy downloads, source/class inventories and stock/supplemental archive hashes.
Consumer checkpoint files advance only after synchronous success and remain
unchanged on denial. These are actual caller-managed file observations after the
application exits, not a new checkpoint API or a crash-durability guarantee.

The former exact physical-stage/RDD-graph hardening claim is withdrawn. Historical
restrictive-profile diagnostics and nonadaptive package records remain historical;
they do not qualify this execution route. Broader logical operations, cross-action
caches, policy subqueries, streaming, Spark Connect, feature-bearing write profiles,
separate workers and catalog-demo integration remain outside this qualification.
Concurrent writing jobs for one dataset remain excluded by the platform. Unrelated
append and CDF-property AQE gates are unchanged. Integrated review and milestone
closure are recorded separately from these local execution results.


Known diagnostic limitation: the new adaptive observation labels/counts are
qualified only for `adaptive-stock`. A newly run `nonadaptive` record can label
a plan `exchange-free` when it has no adaptive wrapper even if ordinary shuffle
operators exist. Inspect the retained operator plan; that label is not proof of
absence of nonadaptive exchanges. This does not change legacy operation support
or relabel historical evidence. The checker cannot use a nonadaptive record to
qualify adaptive execution.

### Original Task 4 execution records

| Spark | Delta | Original installed execution | Final live phases | Outer / Python / native exits |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 79 tests, 1 obsolete expectation failure, exit 1; corrected method 1/1 passed, exit 0 | 7 checked | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 49 tests passed, exit 0 | 7 checked | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 49 tests passed, exit 0 | 7 checked | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 49 tests passed, exit 0 | 7 checked | 0 / 0 / 0 |

The first baseline retains its original failed exit and 78 individually passing
cases. Its corrected method now constructs the builder before changing AQE and
checks settings drift; fresh AQE success is covered by the new suites. Every pair
has the same 49 required new/affected cases passing. The remaining pairs omit
unchanged blanket legacy suites. No run has skipped cases or setup-only passes.

## Named SQL and classic Python

Use a canonical three-part target and a literal canonical source with inclusive
integer bounds. The source supports the existing deterministic scalar
projection/filter/alias grammar. The destination is independently authorized.

```python
spark.sql("""
MERGE INTO lake.db.target AS t
USING (
  SELECT id, secret, region, _change_type
  FROM table_changes('lake.db.source', 12, 15)
  WHERE _change_type IN ('insert', 'update_postimage', 'delete')
) AS s
ON t.id = s.id
WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET secret = s.secret, region = s.region
WHEN NOT MATCHED AND s._change_type <> 'delete'
  THEN INSERT (id, secret, region) VALUES (s.id, s.secret, s.region)
""").collect()
```

Classic Python uses the same installed JVM session, catalog plugin and extensions.
No source-row collection or Python reconstruction feeds the MERGE. The returned
command metrics may be collected. Stock `DeltaTable.forName` and Spark Connect
remain separate compatibility work.

## Named DataFrame API

The companion `gravitino_delta.GravitinoDeltaTable` uses the installed classic
Spark session and its JVM Ranger runtime. Build the matching facade with
`./gradlew :spark-connector:spark-delta-ranger-3.5:deltaPythonZip`, then pass
`spark-connector/v3.5/spark-delta-ranger/build/distributions/gravitino-delta-python-2.0.0-SNAPSHOT.zip`
to `spark-submit --py-files`. The matching Ranger runtime bundle and configured
catalog/extensions must already be installed; the facade neither installs them
nor changes Spark settings. Keep zip/JAR source and API version bindings together.

```python
from gravitino_delta import GravitinoDeltaTable
from pyspark.sql import functions as F

# Configure transaction identity and the selected AQE profile before session/handle construction.
source = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 12).option("endingVersion", 15)
    .table("lake.db.source")
    .filter("_change_type <> 'update_preimage'").alias("s"))
target = GravitinoDeltaTable.forName(spark, "lake.db.target").alias("t")
(target.merge(source, F.col("t.id") == source["id"])
    .whenMatchedDelete(source["_change_type"] == F.lit("delete"))
    .whenMatchedUpdate(set={"secret": source["secret"], "region": "s.region"})
    .whenNotMatchedInsert("s._change_type <> 'delete'",
        values={"id": source["id"], "secret": "s.secret", "region": source["region"]})
    .execute())
# Only now durably advance this consumer's separately stored checkpoint to 15.
```

Predicates and assignment values accept SQL expression strings or classic Columns,
including mixed maps. `whenMatchedUpdate(condition=None, set=...)` and
`whenNotMatchedInsert(condition=None, values=...)` require nonempty mappings from
string keys to expressions. `whenMatchedDelete`, `whenMatchedUpdateAll` and
`whenNotMatchedInsertAll` accept an optional condition. `None` selects the
unconditional overload. Clauses retain authored order and native Delta ordering,
ambiguity and missing-column validation. String expressions use the owning
session's parser; quoted/backtick identifiers and literals keep Spark semantics.
A bound source Column must belong to this exact source occurrence; target Columns
use unresolved qualified references such as `F.col("t.secret")`.

`execute()` blocks through publication/reconciliation and returns `None`; it does
not return rows or metrics. Handles use one classic SparkSession and captured
subject/settings. Set transaction identity before constructing either handle or
source. Builders are single-use and not thread-safe; failed execution also seals
them. An alias change cannot alter an existing builder. The named target resolves
its current compatible registration at execute; the source retains its bounded
registration/interval observations. Recovery requires fresh handles and source,
current authorization and the existing known-commit protocol, never automatic
wrapper replay.

The source is exactly one fresh, bounded named Delta batch CDF read, with inclusive
nonnegative integer starting/ending versions, captured current read schema and
fixed partition metadata. Historical schemas may differ only within the finite
additive profile described below.
Only Project/Filter/SubqueryAlias composition is admitted. The deterministic scalar
expression whitelist is attributes, literals, aliases, casts, `=`, `<=>`, `>`, `>=`,
`<`, `<=`, AND/OR/NOT, IS NULL/IS NOT NULL, IN, upper, lower, concat, coalesce and
`+`, `-`, `*`, `/`. Native resolution and supported types/settings still apply;
this whitelist does not qualify every type/function combination. Reject consumed,
actioned, cached/checkpointed, foreign-session, streaming, unnamed and path sources.
Joins, windows, aggregation, cache, UDFs/subqueries and other expressions are outside
this MERGE source grammar. The caller selects the AQE profile before construction;
the facade never changes it.

The earlier nonadaptive API milestone retained the following evidence.
Installed API tests cover unpartitioned and qualified partitioned targets. All four
final packages passed five actual Python Ranger 2.8.0/MinIO phases each: distinct
destination, column UPDATE/source SELECT/target MODIFY denials and same-table
writeback. The successful phases use strings and bound Columns/mixed assignments.
Original outer/Python/native exits are zero; strict checks cover native bags/actions,
partition maps/files, checkpoint records and persisted registration after restart.
Unpartitioned API coverage is installed coverage, not a live claim.

| Spark | Delta | Installed methods | Final DataFrame live phases | Outer / Python / native exits |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 37 Task 1 + 37 Task 3 retained; 61 unique | 5 passed | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 37 Task 1 + 37 Task 3 retained; 61 unique | 5 passed | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 51 fresh | 5 passed | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 51 fresh | 5 passed | 0 / 0 / 0 |

The final evidence index is
`.superpowers/sdd/2026-10-25-delta-dataframe-merge/task-4/final-evidence-index.json`.
Source-valid Spark 3.5.4 inheritance records exact unchanged production/test inputs
and loaded dependencies, plus an unavailable historical common-JAR exception.
The preserved base/current common comparison differs only in generated build-date
metadata; full historical test-classpath byte equality is not claimed. Task 2 smoke,
SQL qualification and failed attempts remain separate. Final independent SPEC/QUALITY review approved the milestone, committed locally.
Later documentation status changes have separate hash/diff records and do not
relabel the original frozen packages.
Stock `DeltaTable.forName`, Spark Connect, streaming, NOT MATCHED BY SOURCE, schema
evolution, feature-bearing profiles and separate-worker deployment remain outside
this API milestone. Catalog-demo still needs its separate integration plan and qualification of its
broader operations and deployment model; API qualification does not establish full Delta
compatibility. See the [API design](superpowers/specs/2026-10-25-delta-dataframe-merge-design.md)
and [implementation status](superpowers/plans/2026-10-25-delta-dataframe-merge.md).

This example is an explicit consumer program. It is not a generic replica:
filtering preimages may leave stale destination rows when source images cross a
policy boundary. The application must define its desired reconciliation semantics.

## Bounded CDF across additive history

Fresh named DataFrame CDF reads and `table_changes` use the captured current
native schema. Delta reconstructs older images with nulls for subsequently added
fields, retaining native change types, commit versions/timestamps and duplicate
multiplicity. Inclusive numeric start/end versions remain mandatory. An addition
after the requested end can affect the captured read schema without including
later rows. A metadata-only interval can return that schema with no rows.

Only append-only nullable concrete top-level scalar additions are qualified:
Boolean, integral, floating, decimal, string, binary, date and timestamp. Every
old field retains its exact ordinal, name, type, nullability and metadata.
Existing nested fields may remain unchanged under the existing read profile, and
ordinary comment/application metadata may remain. Nested additions, timestamp-NTZ,
NullType, nonnullable additions, defaults, generated/identity/mapping metadata,
type changes, drops, renames and reordering are refused. Duplicate/case-colliding, invalid and CDF virtual names are refused.
Identical schemas retain the existing unchanged-schema admission profile.

The history owner validates the start snapshot and every observed Metadata
transition in the requested interval in native order, including duplicate actions,
and compares historical schemas with the captured read schema. A drop/re-add or
type-change/revert inside the interval cannot hide behind matching endpoints.
Transitions before the start snapshot and unrelated suffix history after the end
are not scanned; endpoint compatibility does not certify unrequested transitions.
Identity, partitioning and protocol remain fixed. Installed tests cover legacy
reader 1/writer 4 and reader 1/writer 7 with exactly `changeDataFeed`, `appendOnly`
and `invariants`; unchanged reads retain reader 1/writer <=4 admission. Missing or
corrupt requested history/files, disabled-CDF gaps, unsupported features, mapped
columns, deletion vectors, row tracking and permissive bypass settings still fail.

Current SELECT, masks and row filters apply to original requested source demands,
including added fields and predicate-only demand. An authorized projection may
read only old fields without SELECT on an unused added field. Historical nulls
remain subject to the same policies as populated values. Reading an addition
requires no ALTER. Captured subject/settings, registration and context ownership
remain binding; retained and fresh actions recheck policy under the existing
action-boundary contract. External schema advancement rejects retained and fresh
access while named registration remains stale; it does not refresh registration
automatically. Copied, foreign, expired and recovery-only owners gain no payload
authority. Standalone reads create no source files or commits.

Fresh SQL and JVM companion MERGE can consume qualified historical null/new-field
images through the existing source grammar and policies. The companion API remains
version 1. Evolving target writes retain their own ALTER/new-field UPDATE and
settings obligations. Recovery never opens old payload or automatically replays a
committed or unknown mutation.

The [additive-history results](superpowers/specs/2026-11-04-delta-cdf-additive-history-results.md)
distinguish pure, stock-native, installed and reused evidence on the four pinned
pairs. This is installed local Ranger client/plugin 2.8.0 qualification. The
consolidated Ranger server 2.8.0/MinIO/S3/PySpark/actual Spark Hub acceptance gate
remains required. Historical fixed-schema live evidence below retains its original
scope. Full schema evolution, broader source composition, consumer/checkpoint or
replica semantics, policy-transition reconciliation and streaming remain unqualified.

## Additive schema-and-data MERGE

The named SQL and JVM companion routes support one native atomic MERGE that adds
concrete nullable top-level scalar columns while updating/inserting rows. Native
Delta resolution determines the assigned additions and final target schema. Old
fields retain exact names, order, types, nullability and metadata; native table
identity, configuration, partitioning and protocol remain unchanged. There is no
preliminary catalog ALTER, engine fork, replacement transaction engine or new
metastore. Gravitino continues to own named registration. The companion API is
still version 1; Python signatures are unchanged, but this additive slice directly
qualifies SQL and the JVM companion, without a new PySpark deployment result.

Set `spark.databricks.delta.schema.autoMerge.enabled=true` and the qualified
`spark.sql.constraintPropagation.enabled=false` operation profile before analyzing
any governed source or constructing either handle. Keep that scope through
execution and restore previous settings in `finally`. Setting them only before
`execute()` is too late. An already analyzed upstream DataFrame is not made fresh
by constructing a new target handle. AQE remains selected before session setup;
the connector does not change native settings, checksum behavior or policies.

For evolving stars, explicitly project payload fields out of CDF after applying
the intended CDF filter. For example, use `SELECT id, value, extra FROM
table_changes('lake.db.source', 12, 15)` as the source of SQL `UPDATE SET *` and
`INSERT *`, or project those fields before companion `updateAll`/`insertAll`.
The connector does not strip authored CDF metadata columns for the caller. With
autoMerge disabled, native star expansion can ignore an unused extra source field
and commit existing-field changes; original SELECT obligations for the requested
payload and predicates remain. Enabled matching-schema operations require no ALTER
merely for the flag.
A real addition requires target ALTER and UPDATE on the added written fields at
the existing live operation boundaries, including known-commit reconciliation.
Masks and row filters continue to govern added values, keys and authored reads;
omitted old fields remain raw during native copy-forward. Destination filters
retain read/match semantics, so a hidden target row cannot suppress an authorized
insert.

The source remains one fresh bounded named CDF interval and the existing finite
projection/filter/alias grammar. Fresh ordinary CDF reads may span the qualified
additive history described above. Recovery-only source deferral proves an already
committed MERGE without opening its old payload; it does not authorize ordinary
historical reads. Exact retained
native proof/digest, transaction marker, fresh permissions, original ALTER and
added-column UPDATE demands, and current compatible named registration remain
mandatory. Later data-only advancement may reconcile; later Metadata/Protocol
changes, including a reverted schema, cannot borrow the original proof.

Outcome evidence remains not committed before native invocation, unknown after
an unproved invocation, or committed after exact native proof. Journal,
registration or recache acknowledgement failure after proof stays committed with
observation unconfirmed. Recovery acknowledgement failure also retains that
verified outcome before durable persistence. Never automatically replay unknown
or known-committed writes. Native source-loss cases retain proof/outcome with
named readback explicitly unconfirmed.

Installed controls cover partitioned AQE/CDF writes, metadata-only evolution,
version-10 native checkpoint reconstruction, masks/permissions, native duplicate
match and NOT NULL failure, and recovery without another source attempt or commit.
Evolving target cached plans reject before mutation. Matching-schema raw target
cache may commit and then fail stock recache; that is committed/unconfirmed,
not successful cache support. CHECK feature protocols, nested additions, type
changes, defaults/generated/identity fields, column mapping and broader table
features remain outside this additive profile. Broader snapshots, joins, windows,
aggregates, UDFs, arbitrary inputs and NOT MATCHED BY SOURCE remain unqualified.

See the [installed additive MERGE results](superpowers/specs/2026-11-04-delta-merge-schema-evolution-results.md)
for the exact four-pair cohort, fresh/reused evidence and runtime provenance.
A consolidated live Ranger server 2.8.0/MinIO gate and actual Spark Hub lifecycle
integration remain open; connector qualification does not close the schema track
or the selected creation, input/transformation and overwrite/full-refresh work.

## Policy semantics

Source SELECT obligations precede row-image filtering, masking and user predicates.
Keys and virtual CDF metadata are governed values. The application cannot recover
raw keys, versions or timestamps to repair matching. Masks may collide, and the
adapter neither deduplicates nor requires unique or unmasked keys.

On the destination, Ranger's row filter controls which raw target rows may match.
User ON conditions, clause predicates and assignment expressions see masked target
values. A hidden physical key cannot suppress a permitted INSERT; two stored rows
can therefore share a key. Hidden and unchanged rows in rewritten files retain
raw values. Omitted columns retain raw stored values; explicitly assigning
`secret = t.secret` writes the masked value.

These are read semantics, not a postimage constraint: an authorized INSERT or
UPDATE may produce a row outside the filter. Source and target policies both apply
when writing back to the same exact named registration. Another registration
aliasing the same physical table is rejected.

Every referenced source/target expression requires SELECT. UPDATE requires UPDATE
on authored assignment columns; INSERT and DELETE require UPDATE on all target
columns in the finite final write schema. Public `MODIFY_TABLE` supplies UPDATE
and ALTER; MERGE uses the
necessary UPDATE demands. Column denies remain effective under table grants.
Empty input and unreachable clauses retain original authorization obligations.

Custom mask expressions must preserve the original column type. For a BIGINT key,
`{col} + CAST(100 AS BIGINT)` provides explicitly typed arithmetic; `{col} + 100`
is rejected because the strict policy resolver does not add implicit coercions.
Individual admitted expression/profile combinations still require execution
qualification; see [the parity tracker](delta-spark-parity.md).

## Publication and application checkpoints

Only advance the consumer checkpoint after SQL or builder execution returns successfully,
which includes native commit proof and registration reconciliation. Retain the
source native UUID and requested inclusive end separately from masked row metadata.
A successful empty interval may advance to its requested end. Denial, missing CDF
history, native multiple-source ambiguity and constraint failure must preserve
checkpoint progress and committed target state. Uncommitted orphan files may
remain after a failed native write.

An optional native `txnAppId`/`txnVersion` pair, with auto-reset disabled, binds a
retry to the same intent and source interval. A known duplicate still requires
current authorization. The companion's private durable driver-local journal
records exact intent and publication evidence. Recovery checks the exact native
commit and current policy/registration before reconciling; journal state does not
grant access or authorize replay of an unknown outcome. Without a pair there is
no safe automatic replay claim. The application checkpoint is a separate durable
consumer responsibility, not the private journal or a CDF decision receipt.

Initial journal claims need atomic hard-link creation; later owned updates need
atomic replacement and fsync. Its configured location and referenced native JSON
must survive recovery. Cross-worker journal placement and global coordination are
not qualified by local testing. The platform prevents concurrent jobs for one
dataset; there is no atomic cross-system Ranger/Delta revocation fence or general
exactly-once sink guarantee.

## Bounded support and evidence

The target profile includes unpartitioned and partitioned reader-1/classic tables
and explicit nonadaptive or trusted-stock adaptive execution. The installed
additive extension above preserves fixed partition metadata and its finite schema
rules; the historical deployed results below used fixed schema. Native
constraints, INSERT/UPDATE/DELETE clauses, native source materialization/retries,
current authorization and known-commit reconciliation have focused local tests.
The live acceptance entry point is `delta/acceptance/cdf_merge_live.py`; its result
checker compares original native row/event multisets, log actions, checkpoints,
policy downloads, process exits and persisted registrations.

### Partitioned local qualification

Four fresh requested-pin packages passed five partition-specific live phases each:
distinct destination, column UPDATE denial, source SELECT revocation, target MODIFY
revocation and same-table writeback. Every MERGE uses actual classic Python
`spark.sql`; separate setup/native, metadata and application identities remain
bound to original process and provider evidence. The shared catalog metadata reader
uses Delta Kernel 3.3.0; Spark's native Delta versions are the requested pins below.

| Spark | Native Delta | Task 3 focused cases / earlier cases | New partition live phases | Outer / Python / native exits |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 38 / 0 | 5 passed | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 3 / 35 | 5 passed | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 38 / 0 | 5 passed | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 38 / 0 | 5 passed | 0 / 0 / 0 |

The live profile uses a fixed `part STRING` partition independent of the `region`
row filter. Visible and hidden rows share a touched file. A source partition mask
moves selected rows to escaped `moved/= %`; filtered rows retain their raw values
in rewritten files, while original untouched files, including a null partition,
retain their native AddFile records and object hashes. The checker compares exact
raw row/CDF multisets, add/remove/CDC partition maps and paths, complete active-file
changes, consumer source/end checkpoints and public/H2 registration after restart.
Commit data-change flags and normalized native snapshot flags are checked separately.

The provider's complete-row registration CAS normalizes relational partition DTOs
and also compares the complete serialized partition shape, preserving transform
kind, field, arguments and order. A real relational publication/refresh regression
and seven mutation cases pass with the existing lifecycle/state tests (59 total).
The 14 Python checker methods cover original evidence and pagination corruption.
Fixture role lookup follows every Ranger result page and rejects incomplete or
ambiguous results.

First/third Task 3 focused runs remain valid across checker/fixture-only corrections;
35 second-pin partition cases retain their reviewed Task 1/2 evidence. Older focused
common JAR archives are unavailable, so inheritance rests on unchanged production,
resource and build inputs, original Gradle outcomes and external dependency/origin
records. Package compile-classpath snapshots are distinguished from actual focused
JVM captures; byte-equivalence of missing archives is not claimed. Original first-two
live results remain unchanged and revalidate through identical-byte source archives.

The MinIO profile qualifies these STRING/null/escaped values. Selected local scalar
and settings cases retain native limits: invalid UTF8 binary partition conversion
is lossy, binary NUL fails on local filesystem, and mismatched session/JVM timestamp
zones can shift instants. See the [partitioned plan](superpowers/plans/2026-10-24-delta-partitioned-merge.md)
for exact cases. General scalar/timezone support, schema evolution, column mapping
or defaults, deletion vectors, row tracking, Spark Connect, separate workers
and deployment remain separate obligations.

The original evidence index is
`.superpowers/sdd/2026-10-24-delta-partitioned-merge/task-3/final-evidence-index.json`;
`task-3-report.md` records source freezes, fresh provider/package provenance, focused
inheritance, retained failed attempts and source relocation. Earlier failures keep
their original exits. The fixture needs preserved private local transport and
service inputs; it is not a clean-checkout bootstrap or credential-refresh test.

### Earlier unpartitioned qualification

All four corrected unpartitioned runtime packages passed the same 13 fresh live phases:
real classic Python `spark.sql` success and denial; distinct destination MERGE;
authorized transaction duplicate; empty interval; native ambiguity; incomplete
CDF history; column denial; post-helper revoke; independent SELECT and MODIFY
denials; regrant; and same-table writeback. Each retained original outer, Scala,
Python and native-observer exit is zero. Native row/event multisets, UUID/version
history, checkpoints, source invariance, public policy downloads, separate
identities and persisted registration after restart are checked from original
records. Query-thread identity and original Python frontend/provider records
are bound to exact source files, runtime JARs and requested-pin compile inputs.

| Spark | Delta | Focused cases | Live phases | Original processes |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 4 passed | 13 passed | All zero |
| 3.5.4 | 3.3.2 | 4 passed | 13 passed | All zero |
| 3.5.6 | 3.3.0 | 27 passed | 13 passed | All zero |
| 3.5.6 | 3.3.2 | 27 passed | 13 passed | All zero |

The Spark 3.5.4 focused counts combine two fresh live-helper cases with the two
original post-fix constructor-denial/installed-MERGE cases on identical sources
and pins. Spark 3.5.6 additionally runs publication, recovery and native block-loss
retry coverage. Earlier Task 4 evidence is retained under the documented source
mapping; no full historical append/CDF matrix was replayed. The affected Python
checker/package tests pass 29 cases, including corrupted-evidence rejection.

The retained evidence index is
`.superpowers/sdd/2026-10-22-delta-cdf-merge/task-5-final-evidence-index.json`;
`task-5-report.md` records exact commands, XML, original failed attempts, source
reuse and the corrected runtime freeze. Every final live cell uses the reviewed
constructor cleanup fix. One corrected Delta 3.3.2 fixture failed after a recorded
981-second host sleep and an S3 403; its exact storage error cause remains unknown.
Only that live fixture was retried using the identical package. These local cells
use static fixture storage credentials and do not qualify credential refresh. Earlier pre-fix successes and failed harness attempts
remain separate historical records. Final documentation changes are recorded as
a non-runtime delta; historical manifests and results are not rewritten.

The remaining feature obligations and known native source-count issue stay open
in [Delta Spark parity](delta-spark-parity.md). Deployment is tracked separately in
[the catalog-demo plan](superpowers/plans/2026-10-22-delta-cdf-merge-demo.md).
