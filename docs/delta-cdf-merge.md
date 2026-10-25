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
Local qualification completed on Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 with
actual Ranger 2.8.0, MinIO, JDK 17 and Scala 2.12.21. Separate-worker deployment
remains unqualified.

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

The companion `gravitino_delta.GravitinoDeltaTable` Python API is now implemented
and packaged as a zip for `spark-submit --py-files`. Its actual DataFrame frontend
passed five partitioned Ranger 2.8.0/MinIO phases on Spark 3.5.4/Delta 3.3.0,
including strings, bound Columns, permission denials and same-table writeback.
Installed API policy/lifecycle qualification also passed on Spark 3.5.4 with both
Delta pins: 37 cases per pin, including exact policy demands, partition moves,
revocation, native retries and commit reconciliation. The final packaged Python
four-pair matrix remains pending, so this is narrower than the SQL matrix below. See the [API design](superpowers/specs/2026-10-25-delta-dataframe-merge-design.md)
and [implementation status](superpowers/plans/2026-10-25-delta-dataframe-merge.md).

This example is an explicit consumer program. It is not a generic replica:
filtering preimages may leave stale destination rows when source images cross a
policy boundary. The application must define its desired reconciliation semantics.

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
on authored assignment columns; INSERT and DELETE require UPDATE on all physical
target columns. Public `MODIFY_TABLE` supplies UPDATE and ALTER; MERGE uses the
necessary UPDATE demands. Column denies remain effective under table grants.
Empty input and unreachable clauses retain original authorization obligations.

Custom mask expressions must preserve the original column type. For a BIGINT key,
`{col} + CAST(100 AS BIGINT)` provides explicitly typed arithmetic; `{col} + 100`
is rejected because the strict policy resolver does not add implicit coercions.
Individual admitted expression/profile combinations still require execution
qualification; see [the parity tracker](delta-spark-parity.md).

## Publication and application checkpoints

Only advance the consumer checkpoint after the SQL command returns successfully,
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

The supported target profile includes unpartitioned and partitioned reader-1/classic
tables, with fixed schema and partition metadata, AQE disabled and the explicitly
owned native execution path. Native
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
or defaults, deletion vectors, row tracking, AQE, builder/Connect, separate workers
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
