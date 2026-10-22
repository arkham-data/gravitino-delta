<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Governed named batch Delta CDF consumption

The optional Delta Ranger companion qualifies bounded, named batch CDF reads on
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, JDK 17 and Scala 2.12.21. Each pair has
actual Ranger 2.8.0 / MinIO qualification. Gravitino owns the named registration;
Delta owns the transaction log and reconstructs native changes. Current Ranger
policies govern the authenticated application subject; the separate metadata
identity supplies registration metadata.

This delivers a governed source feed for incremental ELT. Governed destination
MERGE/UPDATE/DELETE and writebacks are the next functional ELT milestone. Streaming,
complete Delta compatibility, complete replica maintenance and end-to-end ELT
remain outside this result. Existing [SELECT](delta-ranger-production-results.md),
[append](delta-native-governed-append.md) and [CDF enablement](delta-cdf-enablement.md)
results retain their own limits.

## Named entry points

Use the [optional companion startup contract](../spark-connector/v3.5/spark-delta-ranger/README.md)
and an existing published registration with CDF enabled throughout the requested
interval. Both bounds are explicit inclusive native integer versions:

```python
changes = (spark.read
    .option("readChangeFeed", "true")
    .option("startingVersion", "12")
    .option("endingVersion", "15")
    .table("catalog_name.database_name.table_name"))
rows = changes.select("id", "value", "_change_type",
                      "_commit_version", "_commit_timestamp").collect()
```

```sql
SELECT id, value, _change_type, _commit_version, _commit_timestamp
FROM table_changes('catalog_name.database_name.table_name', 12, 15);
```

SQL `SELECT *` includes payload and all three virtual CDF fields. Fully qualified
named identifiers, including individually quoted identifier parts, are supported.
SQL requires a literal table name and integer literal bounds. The DataFrame option
keys are case insensitive; `readChangeFeed` has the exact value `true` and version
values are decimal digits fitting a signed 64-bit integer. Require
`0 <= start <= end <= observed native head`. Reconstruct one fresh CDF source with
the existing finite projection/filter expressions and aliases. Qualified actions
are driver `collect`/`collectAsList`; an admitted plan does not grant arbitrary
Spark job execution.

Missing/latest ends, timestamp bounds, conflicting/unknown options, path/Hadoop
overrides, ordinary time travel, direct paths and `table_changes_by_path` reject.
Joins, aggregation, ordering, streaming, arbitrary RDDs, derived/copied authority,
and governed SQL cache/persist substitution remain outside this route.

## Policies and event interpretation

Current table and demanded-column SELECT apply to the original query obligations,
including columns later removed by optimization and empty results. Each native
row image passes the current Ranger row filter, then column masks, then user
predicates on governed values. Unsupported policy expressions reject using the
existing finite expression grammar. Historical rows do not restore historical
grants.

`_change_type` (string), `_commit_version` (long) and `_commit_timestamp` (timestamp)
are virtual columns on the same table. Ranger grants, denials and masks use their
literal names, including wildcard grants where applicable. They are not added to
Gravitino's stored physical schema. Keys and event metadata remain policy owned:
a masked key or metadata value is returned as permitted, with no raw-value bypass,
key requirement, deduplication, automatic policy migration or reverse-mask lookup.

All four native event types survive before policy projection: `insert`, `delete`,
`update_preimage`, `update_postimage`. Duplicate rows and masked-key collisions
remain duplicated. Native CDF supplies neither an update-pair ID nor a business-key
or deterministic within-commit ordering guarantee. The reader does not pair images,
synthesize deletes from preimages, or impose downstream MERGE semantics.

For a row filter `region = 'US'`, update images are evaluated independently:

| Transition | Visible native images before masks |
| --- | --- |
| US → US | Preimage and postimage |
| US → EU | Preimage only |
| EU → US | Postimage only |
| EU → EU | Neither |

Ignoring all preimages can therefore leave stale destination rows. Consumers must
define key mappings and their treatment of preimages, deletes, masked collisions
and multiple changes per key. A policy-only change creates no Delta event for
unchanged source rows; consumers must reconcile or resnapshot their destinations
when policy changes require it. Re-reading an interval can yield different results
under different current policies.

## Completeness, profile and action lifetime

The reader proves every native log version in the inclusive requested interval
exists, including the terminal version, before passing those exact retained
actions to Delta reconstruction. It stops at the requested end. Native inferred
inserts, explicit CDC files and inferred historical deletes have separate owned
scan branches. Empty and `dataChange=false` commits do not fabricate events.
Missing/corrupt log or historical data/CDC files, disabled-CDF gaps and permissive
ignore-missing/ignore-corrupt/out-of-range settings fail; failure is never returned
as an empty successful interval.

Payload schema and column identities, native UUID, partitioning and protocol must
remain compatible with the authoritative registration and read snapshot throughout
the interval. Schema change-and-restore, payload names colliding with virtual CDF
columns, column mapping, deletion vectors, row tracking and unsupported historical
features reject. The initial protocol profile requires reader 1, no reader
features, and either writer ≤4 without writer features, or writer 7 with exactly
`changeDataFeed`, `appendOnly`, `invariants`. Broader historical schemas/features
remain subsequent work. Compatible source advancement beyond the fixed requested
end does not extend the selected interval. The platform serializes writer jobs per
dataset; reads acquire no writer authority.

The driver owns exact named bindings, source/output objects, interval/action
inventories, native helpers/indexes and physical/RDD identities. Copied, foreign or
serialized receipts cannot grant access. Cold/warm native reconstruction caches
retain bounded ownership; user governed-query caches remain rejected. At qualified
action boundaries the reader rechecks subject, configuration, registration/native
identity, current Ranger publication and effective decisions, including scheduled
changes. An observed change invalidates retained work and requires a fresh read.
This is optimistic revalidation, not instantaneous Ranger Admin revocation or
cancellation of executor work already running.

## Consumer checkpoints

Persist the native source identity and the requested end only after the entire
batch and sink operation succeed. Before resuming, verify that the stored native
identity still matches the registered source; a replacement source requires an
explicit consumer reconciliation decision. For example, for a checkpoint ending at 11,
request `[12,15]`; after successful sink application, durably checkpoint 15 and
request from 16 next. A successful zero-row filtered batch can still cover all of
`[12,15]` and advance to 16. A denied, incomplete or failed batch cannot advance.
The maximum visible `_commit_version` is unsuitable: metadata may be masked and
policies may hide all rows from the terminal commit.

The consumer owns its sink transaction, replay strategy and durable checkpoint.
Driver-local authorization receipts are not durable checkpoint authority. This
result promises neither exactly-once sink application nor policy-stable replay.
Writebacks require independent target authorization and an application-defined
mapping to target keys; a masked token grants no original-key lookup or permission.

## Qualification and retained evidence

Each local live fixture uses production Gravitino catalog/provider discovery,
distinct setup/metadata/application identities, the actual deployed Ranger 2.8.0,
and local S3 through MinIO. Native setup emits append, update, delete and merge
changes and records an independent 13-row oracle over `[0,4]`. Named DataFrame,
unfiltered SQL star and fresh-after-regrant each return the expected eight
six-column filtered/masked rows, preserving repeated rows and masked collisions.
The `[4,4]` interval succeeds empty. Out-of-range bounds reject. A dedicated
incomplete table first succeeds under application SELECT, then fails after setup
removes one owned CDF file; the primary source remains unchanged.

Each pair proves public SELECT grant/revoke/regrant and completed policy downloads
in one JVM, retained-operation rejection after revoke, fresh denial, and fresh
success after regrant. Source object bytes and registration documents compare
unchanged, persisted H2 identity/column IDs match, and a same-store Gravitino server
restart preserves readback. The reader remains SELECT-only.

| Spark | Delta | Final focused tests | Original deployed result | Evidence directory |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 162 passed | Ranger 2.8.0 / MinIO: PASS | `task-4-live-5` (immutable reuse) |
| 3.5.4 | 3.3.2 | 162 passed | Ranger 2.8.0 / MinIO: PASS | `task-5-matrix/3.5.4-3.3.2/live` (fresh) |
| 3.5.6 | 3.3.0 | 162 passed | Ranger 2.8.0 / MinIO: PASS | `task-5-matrix/3.5.6-3.3.0/live` (fresh) |
| 3.5.6 | 3.3.2 | 162 passed | Ranger 2.8.0 / MinIO: PASS | `task-5-matrix/3.5.6-3.3.2/live` (fresh) |

Evidence root:
`.superpowers/sdd/2026-10-20-delta-cdf-consumption/`. The first row retains the
original accepted `task-4-package-6` / `task-4-live-5` without modification. Task 5
strengthened its saved-result checker and explicitly rechecked complete unchanged
production/application sources, all runtime/shared artifact bytes and observed
class origins. The other rows each use a requested-pin Gradle package and an original fresh live
fixture. Gradle originally reported the shadow output UP-TO-DATE; a later bounded
shadow-only assembly under each requested pair proves equality of every archive
entry, including resources and duplicate-path ordering, against the immutable live
artifact. Original manifests retain their historical fresh-output wording; the
final index discloses the actual task outcomes. No live fixture is replayed. Complete-byte application reuse is recorded when
selected; companion reuse is allowed only for identical Spark **and** Delta pins
and production inputs. The final `task-5-report.md`, matrix index, per-pair manifests,
source/class/JAR captures, original argv/outer exits, child exits and test XML
provide the exact provenance. Documentation-only deltas after runtime capture are
recorded separately; old receipts are never rewritten to appear fresh.

Original failed Task 4 fixtures and diagnostics remain retained, including the
SQL-star failure and its focused regression correction. No unchanged append or
CDF enablement matrix was replayed. Built-in linked-worktree Git metadata remains
empty because of the existing Gradle path warning; explicit actual HEAD, complete
source inventory and class/JAR hashes supply provenance. Existing compiler,
Gradle and native fixture warnings remain disclosed in the task report.

These results qualify the stated local bounded profile. The harness depends on
retained immutable packages/helpers/private service inputs; a standalone clean
checkout reproduction kit, `catalog-demo`, distributed execution, cloud storage
and authentication hardening remain separate qualification work. See
[acceptance commands](../delta/acceptance/README.md#governed-batch-cdf-read-qualification).
