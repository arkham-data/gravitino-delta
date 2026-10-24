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
command metrics may be collected. Stock `DeltaTable.forName`, the DataFrame MERGE
builder and Spark Connect are separate compatibility work.

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

The supported target profile is unpartitioned, reader-1/classic CDF with fixed
schema, AQE disabled and the explicitly owned native execution path. Native
constraints, INSERT/UPDATE/DELETE clauses, native source materialization/retries,
current authorization and known-commit reconciliation have focused local tests.
The live acceptance entry point is `delta/acceptance/cdf_merge_live.py`; its result
checker compares original native row/event multisets, log actions, checkpoints,
policy downloads, process exits and persisted registrations.

All four corrected runtime packages passed the same 13 fresh live phases:
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
