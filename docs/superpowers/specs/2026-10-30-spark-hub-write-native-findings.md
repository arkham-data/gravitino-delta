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

# Spark Hub native write findings — first-task characterization

Measured on 2026-10-30. The original Spark/Delta process completed all 15 fixed
cases with exit 0. Independent offline verification concludes
`characterizationComplete=true`, `nativeCandidateViable=false`,
`governedSupport=false`. Ordinary append, schema evolution, same-identity full
overwrite and bounded ordinary hooks worked in this native cell. Neither tested
single-phase create candidate preserves all required behavior with the original
writer options. No production create route is selected; M2 remains incomplete.

## Original evidence and identity

All local evidence is retained under
`.superpowers/sdd/2026-10-30-spark-hub-write-foundation/`. `native-cell-2/result.json`
(SHA-256 `c6cd829c4b3daec3a5919a8ef88c4f9bcb638944b5067df87455644b32a03a97`)
contains original command/exit/stdout/stderr references, every case and sequential
substep, native JSON logs and row readback, source snapshots and import inventories.
`task-1-independent-verification.json` is the final checker result. The original
`native-cell-2/verification.json` is preserved: its extra refresh metadata gap was
a checker comparison defect corrected offline against the recorded per-refresh
options, with a failing regression test. No writes were repeated for this correction.

Historical executed G source revision: `676e7a1ff2d9c409c076998cc5a3d961884620da`.
S source revision: `d01024b6f459ad2743b7b55099226b8c3dc742fd`.
The unchanged installed runtime comes from
`.superpowers/sdd/2026-10-29-spark-hub-catalog-foundation/task-4-runtime-successor-4/3.5.6-3.3.2/package/manifest.json`,
SHA-256 `0659132f5da3e9c49bdf449a3e5d33162e393161af28d22391d16fb4f7482da7`.
JAR bytes, runtime links and original immutable build receipts were rechecked;
no predecessor scenario was replayed and no runtime package was copied/rebuilt.
The child independently verified Spark/PySpark 3.5.6, Delta 3.3.2, Java 17,
Scala 2.12.21 and actual class origins:

| Actual provider | SHA-256 |
| --- | --- |
| spark-sql_2.12-3.5.6.jar | `62824ee15f3bee95ebcba4e26238d414cd71cc0f9cdc7d45f681b7366766ec19` |
| delta-spark_2.12-3.3.2.jar | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| delta-storage-3.3.2.jar | `c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36` |
| scala-library-2.12.21.jar | `e2f4040c95cc4dd1cfeaae38bdde596e4b36933632451efa7d8564c6a359510c` |

The new actual sink import inventory has 24 Spark Hub source files and 19 installed
distributions (3,822 file references). `base_jobs.py` hashes to
`3dfad83ff031a81b62ac9ab16ccc71cc682af5839b6467b44bebc92e01028a79`;
`services/v2/delta.py` hashes to
`35514f0008ed6d68a3044c05453cca0b82e7f3f861b7cbedc372b4096f58cbfc`.
This is separate from the old read-lane inventory. The executed app source is
archived at `native-cell-2/sources/spark_hub_write_native_app.py`, SHA-256
`4c640e95ead95f6025a7227330787fc2913a3f1b6b915e7eae87bb1ac4731529`.
The final checker has additional offline validation; the archived original checker
source and its initial verdict remain unchanged.

The delivered launcher also has a later preflight-only correction: it records the
actual current G HEAD rather than requiring the historical planning HEAD. Each
future run still archives actual consumed probe bytes and rejects changes during
the original process; immutable installed runtime bytes and their producer history
are verified independently. This stock oracle does not execute current G product
classes, so unrelated G product-source changes are not a native-runtime rejection
rule. The exact clean S revision requirement remains strict. The measured cell used
archived launcher SHA-256
`fb7efd7e8a779ec7d0439b9127f0c956a7606aacba174d07d2fdb634cdd921dc`;
its original G revision and archived sources were preserved. Temporary-repository
regressions validate the updated preflight without another native execution.

The first attempt, `native-cell-1`, exited 1 before any Spark/storage mutation:
normal DeltaClient import required missing boto3. Its original receipts and empty
object manifest remain intact. A task-owned Python 3.11 overlay then reused the
retained runtime environment, adding source-pinned boto3 1.28.20 and kafka-python
2.0.2 with their ordinary dependencies. The predecessor environment was not
modified. The actual import closure also requires a module-level Kafka producer;
a cached, task-owned single broker on loopback port 62912 satisfied that import.
No event was published, DeltaClient was not constructed, and no AWS client was
constructed by the probe. `dependency-install.log`, `kafka-container.json` and
`kafka-image.json` retain the setup and effective configuration. The Kafka image
is `confluentinc/cp-kafka:7.4.0`, ID
`sha256:187dac6627e7906c350f5f8c982f80ce735ff1a0e571a20de6000a309a12ce63`.
This eager Kafka connection is existing Spark Hub import coupling, not a new sink
runtime requirement imposed on production.

## Native observations and decision

Every independent target has a fresh exclusive MinIO location. All successful
targets retain CDF=true, log retention 45 days and deleted-file retention 364 days.
The legacy loaded-at projection is present in native schemas and row readback.
Actual helper dictionaries were passed directly through `.options(**actual)`;
userMetadata was not JSON-normalized. Native WRITE history retains Python's
string form of the dictionary, including `False`/`True` metadata flag spelling.

| Case | Native versions and decisive result |
| --- | --- |
| legacy-create | Substep `legacy-create-empty`: absent (-1) -> version 0 CREATE TABLE, empty rows. Main data step: 0 -> 1 WRITE, same UUID `8b892bcd-b110-4c28-a6d4-3da77331a022`. Exact writer userMetadata exists only on WRITE. Two durable phases remain separate. |
| stage-create | Absent -> version 0 CREATE TABLE AS SELECT, one data commit and native UUID. Native history omits writer userMetadata despite actual options. Rejected as a complete create lifecycle candidate. |
| absent-create-or-replace | Actual original options rejected with `_LEGACY_ERROR_TEMP_1113`: table does not support truncate in batch mode. Readback remained absent/version -1, with no native actions or rows. No options were removed to obtain success. |
| append / append-evolve | Version 0 -> 1; old row retained, new row appended. Evolution adds `extra` with NULL on the old row. Same UUID, properties and exact writer metadata. |
| append-explicit-false | Incompatible new field rejected with `_LEGACY_ERROR_TEMP_DELTA_0007`; version, rows and UUID unchanged at 0. |
| overwrite | Version 0 -> 1, old rows removed and replacement schema installed; UUID `cd8821a7-fb42-466d-80dd-1850ca68f33a` preserved. |
| overwrite-empty | Version 0 -> 1, explicit-schema empty input removes all rows; UUID `124b100b-3e53-4c27-8eb9-96fad5965669` and native properties preserved. |
| overwrite-partitioned | Two seeded partitions at version 1 -> version 2 full overwrite. Untouched `old-only` partition's row is removed; partition metadata and UUID `7082674a-c36a-4300-9524-9d6a99b0fbe7` preserved. |
| overwrite-explicit-false | Incompatible schema rejected without version/row change. |
| refresh-append | FULL_REFRESH (7) and STEP_REFRESH (8) force effective mergeSchema/overwriteSchema=true while WriteStrategy stays APPEND; both embedded original flags remain false in metadata. Two rows appended (versions 1,2), then auto-compaction at 3. |
| auto-merge-precedence | Session autoMerge=true does not override explicit writer mergeSchema=false. Incompatible append rejected at unchanged version 0. Session setting restored afterward. |
| duplicate-same / duplicate-changed | After the first txnAppId/version=9 write at version 1, repeating either exact intent or changed rows/userMetadata adds no version and changes no rows. Native suppression therefore does not validate intent. |
| ordinary-hooks | Six ordinary tiny writes with optimizeWrite=true, autoCompact=true and default symlink hook=true produced data versions 0,1,2,4,5,7 and separate OPTIMIZE versions 3,6, each with add/remove actions where dataChange=false. Native symlink manifest observed and archived. |

Observation granularity exception: the partitioned fixture's `overwrite-partitioned-seed`
step groups two seed writes. Original native logs for versions 0 and 1 are retained,
but there is no separate row readback between those writes. The decisive pre-overwrite
readback at version 1 and post-overwrite readback at version 2 are present. This limits
fixture-setup granularity without changing the observed full-overwrite result. Future
executions should retain one step per seed write; no new original readback is claimed.

The hook thresholds were minimum three small files, 1 MiB target files, 512 KiB
minimum small-file size, and 1 MiB optimize-write bin size. No manual OPTIMIZE or
disabling required options was used. Effective settings were observed before/after
successful and rejected operations and remained/restored equal. The aggregate
refresh record retains the initial ordinary helper output; `refreshOptions` records
the options passed to each actual refresh operation and governs the history check.

Ordinary full OVERWRITE is the measured same-identity refresh behavior. It supports
the design's exclusion of registration replacement for refresh. The legacy
empty-create/data-write sequence preserves metadata on the data commit but exposes
two durable phases. Staged CREATE loses that writer metadata; absent-only
createOrReplace rejects the unchanged options. No tested single-phase create route
is selected for production from this cell. `nativeCandidateViable=false` denotes
that complete required native compatibility has not been established; it does not
negate the successful ordinary operations.

Outcomes are independently derived from original native add/remove, metadata,
protocol and OPTIMIZE actions, with UUID/version and retained row readback. Native
metadata or maintenance phases that were not executed/independently resolved stay
`unknown`; they are not reported as proven uncommitted. Symlink existence and
observed compaction do not establish how to recover from opaque postcommit hook
failure. No automatic retry occurred, including after the setup failure.

## Exact next task and remaining gates

Next, run a smaller create-boundary experiment to establish whether native staged
creation can carry the original write metadata/options in a retained native context,
without deleting explicit false options, bypassing actual Spark Hub helpers,
replacing an existing table or implementing a transaction engine. Compare that
boundary with the measured two-phase empty-create/data-write lifecycle, retaining
separate native create/data outcomes and testing the selected context's cleanup.
Define a reviewable reservation-bound interface only after this seam is proven.
This is the remaining create decision; successful stock CREATE alone supplies no
Gravitino reservation or unpublished-creator authority.

The subsequent source/options integration must separately define retained ownership
for original JDBC/file/constructed inputs and typed writer options, prove target
and mixed-governed-source denials before mutation, and preserve option precedence.
This cell admits no new governed source plan and does not choose a broad LogicalRDD
allowance. Schema CREATE propagation, retained lifecycle authority, publication and
restart reconciliation, schema registration refresh, durable application intent and
retry retention, postcommit failure recovery and actual shared Spark Hub routing
remain open. Dynamic partition overwrite must be rejected by the future full-refresh
adapter. No new Ranger grant, AWS S3/RustFS equivalence, four-pair qualification or
production deployment is claimed. `governedSupport=false` throughout.
