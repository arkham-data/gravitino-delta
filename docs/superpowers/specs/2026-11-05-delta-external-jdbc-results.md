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

# Job-scoped external PostgreSQL input results

This milestone qualifies one bounded external PostgreSQL source with existing named
Delta APPEND destinations. Spark and Delta execute native reads and transactions;
Gravitino owns destination registration; supplied read-only PostgreSQL credentials
authorize the external source. Named Delta inputs retain Ranger SELECT, column, row filter and mask obligations. There is no fabricated Ranger resource for external SQL.

The matrix is Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, actual
Ranger server/client 2.8.0, PostgreSQL JDBC 42.7.11 and actual MinIO
RELEASE.2025-07-18T21-56-31Z. The same unmodified stock engines and reviewed companion
APIs serve every cell. The selected Hub archive contains commit
5feaa5d1a95df3a77d24aacb1a2baa138e54fb41 and SHA256
71530d87e7782b4f113401bef98136ad54ccd9b6e4e2f864df55c98f9c8f40dd.

Each pair runs external_insert_into, external_partitioned_save_as_table and
external_target_update_denied. Lead 3.5.6/3.3.2 also runs external_hub_tracker,
external_mixed_governed, external_named_source_denied,
external_scope_closed_before_publish, external_target_revoked_before_publish,
external_unissued_rejected and external_source_advances. Exactly 19 distinct pair/case
keys and 23 actions are required; the Hub case declares
remote_count/tracker_count/append and source advancement declares
count_before/append/count_after. All others declare one append.

The Hub proof executes actual run_source_jdbc_v2, SparkClient.query_jdbc,
GravitinoJdbcReader.read and ProgressTracker._run -> SparkClient.init_df_tracking. It
uses explicitly qualified auto_compact_enabled=false and delta_optimize_write=false
before binding. Its main and tracker connections have distinct Python/JVM threads with
the same session/UGI/groups. Actual-service actions are sequential; the separate
original Task 3 installed fixture proves count/append overlap and is not represented as
actual Ranger/MinIO evidence. Source advancement demonstrates that earlier count 3 can
precede an APPEND consuming 4 source rows and later count 4; no cross-action source
snapshot is promised.

The partitioned writer has two initial predicates and one same-PID/same-portal fetch
continuation with fetchsize 2. Its original three PostgreSQL payload execution events
are retained as three; schema discovery, remote count, payload and mutation counters are
separate. A fetch continuation is not a retry or a schema/count event. This is
functional qualification, with no throughput or latency claim.

The only additional production correction initializes the stock DeltaSQLConf registry
alongside the existing analyzer initialization before freezing scope configuration.
Native defaults are registered without setting values, prewarming a target or exempting
mutable keys. Cold-JVM RED/GREEN and later explicit configuration mutation denial are
retained.

Qualification may span immutable fixture segments. Each segment validates its original
native state, public transitions, policy downloads, actual server/client origins,
process exits, upstream read-only role, owned storage and producing artifacts.
Aggregation requires exactly four requested pairs, 19 unique keys, 23 exact actions and
the same product/Hub bytes per pair. Original failed checker/setup runs, committed
writes and unknown attempts are preserved; read-only correction records never relabel an
original producer or replay a target.

Original failures include PostgreSQL bind ownership before any target, lazy stock Delta
default registration before payload, checker native-partition field/registered-only
descriptor mismatches, a protocol-count assumption, missing retained Hub import
dependencies, an out-of-range test barrier deadline, an early-denial metadata cleanup
assertion, and a pre-case capacity guard. The early unissued rejection is qualified only
on a fresh target with actual executing-JVM identity captured before close and an exact
expected metadata-absence witness after verified source rejection, zero payload and
unchanged native state. Other close errors propagate. Their original process failures
and native outcomes remain in the evidence workspace; only specifically recovered
completed cases contribute to the final inventory. Named owned Docker volumes solve the
external fixture UID boundary without changing shared-service permissions; containers
stop and volumes/data remain retained. Shared Ranger/MinIO and historical targets remain
intact.

This does not qualify the full Hub job_wrapper/write_dataset sink, CREATE,
overwrite/refresh, schema evolution, arbitrary DataFrames, file/constructed origins,
broader transforms, other vendors or explicit TLS options/deployment. Options remain
restricted to url/driver/user/password/dbtable, optional nonnegative fetchsize and the
complete partition quartet. Unsupported options fail without fallback. Scope lifetime
1200 seconds is finite, without renewal; close/expiry does not atomically cancel an
already executing database statement. One writer owns each destination, and attempted
committed/uncertain targets are never replayed automatically.

The final read-only matrix uses allowlisted nonsecret fixture identities and opaque
original private-state paths, with no credential-state digest derivation or propagation.
It verifies the unchanged original producing artifacts, native outcomes and public
policy evidence. Self-review found that the inherited service helpers had emitted
credential-XML digests and full Docker inspection in private diagnostics. Those
historical originals are retained owner-only under the retention requirement and are not
represented as compliant with the no-credential-derived-evidence rule. The external-only
helper now omits credential-file digests while preserving in-memory property equality,
and collects selected Docker identity/state/port fields at source. Legacy defaults
remain unchanged. The focused security helper suite passed 11 tests with zero
failures/errors/skips, and the restricted real Ranger identity/JAR observation passed.
These prevention changes were tested after the original service runs; no new helper
bytes are relabeled as having produced those runs.

## Verification

| Spark | Delta | Installed tests | Service cases | Native commits | Denials |
| --- | --- | ---: | ---: | ---: | ---: |
| 3.5.6 | 3.3.2 | 26 | 10 | 5 | 5 |
| 3.5.4 | 3.3.0 | 26 | 3 | 2 | 1 |
| 3.5.4 | 3.3.2 | 26 | 3 | 2 | 1 |
| 3.5.6 | 3.3.0 | 26 | 3 | 2 | 1 |

All 104 installed tests passed with zero failures, errors or skips. Each pair ran
TestDeltaRangerExternalJdbcConstruction (6), TestDeltaRangerExternalJdbcNative (9),
TestDeltaRangerExternalJdbcScope (6) and TestDeltaRangerLiveAppendAqe (5).
The final external/registered checker controls passed 40 tests. The matrix
checker revalidated all original segments and returned four pairs, 19 cases and
23 actions. No registered service matrix or unrelated suite was replayed.

Commands used Java 17, `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'`, `./gradlew --max-workers=1 --console=plain
-Dorg.gradle.jvmargs=-Xmx2g -PdeltaSparkVersion=<spark> -PdeltaVersion=<delta>
-PskipITs -PskipDockerTests=false -PrangerDeltaExternalJdbcEnabled=true`, the
companion `spotlessApply`/`test` tasks and those four exact test selectors.
Checker command: `python3 -m unittest discover -v -s delta/acceptance
-p '*jdbc_checks.py'`. Exact commands, exits, original XML and producing manifests
are retained under `.superpowers/sdd/2026-11-05-delta-external-jdbc/`.

The retained `task-4-matrix.json`, `task-4-verification-summary.json` and
`task-4-command-inventory.json` contain the full 19-case/23-action inventory,
original references, byte hashes, all four XML inventories and process receipts.
`task-4-report.md` records every original failure and correction, ownership,
cleanup and self-review. Evidence is retained in the local worktree, not embedded
in the source distribution.

## Producing artifacts

| Spark / Delta | Full runtime product digest |
| --- | --- |
| 3.5.4 / 3.3.0 | `1c373816c2f7a4d0b871fea10a9a70cb8b06a3cf420d6635cd328466ba14c488` |
| 3.5.4 / 3.3.2 | `cf0694ad113a74e9f40541d8fe7d5723a8a4ad3f29c8238930666d31caa76806` |
| 3.5.6 / 3.3.0 | `ce3aae4d3472ec9763e6e36a1f4aba26433e006b7cf38f360dae81e7e70fa220` |
| 3.5.6 / 3.3.2 | `98125bb1bb45c377a1dac30d1ae2fc6f47803270794f423ddde762c2aa202131` |

The digest covers all pinned runtime JAR basenames/hashes and the companion Python
archive. Each immutable segment separately records its original source-input
digest, fresh build receipt, exact app/companion hashes, providers, Hub source
archive and cleanup receipts. Acceptance-only successors reuse product bytes
only after the existing exact source/byte checks; no original artifact is relabeled.

The conservative default capacity reserve remains 8 GiB. After one pre-case
capacity stop, a measured, explicitly approved 5 GiB reserve applied only to the
last two fresh pairs and their last build stages. The measurement/decision and
per-stage/per-case readiness are retained. No historical data was deleted.
