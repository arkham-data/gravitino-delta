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

# Installed Delta schema CREATE boundary findings

The installed SQL frontend rejects schema-only `CreateTable` before catalog entry. Calling the existing public base stage directly reaches a **durable native CREATE0**, then the unchanged companion rejects checksum reconstruction. The diagnostic resolution observer sees the active transaction only **after log0 exists**. The native-only differential has the same postcommit ordering on Delta 3.3.2 and 3.3.0. This probe establishes no usable precommit capture seam or production CREATE authority.

## Scope and evidence

The [probe design](2026-11-04-delta-create-installed-probe-design.md) was executed from checkpoint `b7549ed65`, changing two new test sources and this findings document only. Runtime records identify Spark 3.5.6, Java 17.0.16, Scala 2.12.21, and actual Ranger 2.8.0 client/model classes from `ranger-plugins-common-2.8.0.jar`. Policy and Gravitino transport are scripted fixtures; no live Ranger or Gravitino endpoint is contacted.

Evidence is retained under `.superpowers/sdd/2026-11-04-delta-create-installed-probe/`:

| Producing run | Exact test methods in `TestDeltaRangerInstalledSchemaCreateBoundary` | Original result |
| --- | --- | --- |
| `installed-3.3.2-attempt4/` | `namedSchemaCreateRetainsInstalledRefusal`; exploratory `baseSchemaCreateRecordsInstalledNativeBoundary` | A passes (0.312s); B intentionally fails its unpinned boundary assertion after retaining the actual native result. Task exit1; 2 tests, 1 failure. |
| `installed-native-3.3.2-attempt5/` | `baseSchemaCreateRecordsInstalledNativeBoundary`; `nativeOnlySchemaCreateDifferential` | Exit0; 2 tests, 0 failures/errors/skips; 6.729s. |
| `native-only-3.3.0-comparison/` | `nativeOnlySchemaCreateDifferential` only | Exit0; 1 test, 0 failures/errors/skips; 7.982s. |

Each named run retains `command.json`, `exit.json`, `gradle.log`, original XML, consumed Scala/resource copies, `source-manifest.json`, compiled-class hashes, `runtime-manifest.json`, and `evidence-manifest.json`. Case directories retain original stacks/events/settings, journals, native file hashes and actual native bytes. The runtime manifests identify loaded class resources and their actual JAR origins; directory-backed classes have individual hashes. A's passing result comes from attempt4; B's pinned assertions, the conditional C test and a shared fixture-hash assertion were added afterwards. `pinning-and-differential.diff` preserves that source distinction. A's body and startup topology were not rerun unchanged.

The common command environment was Java17 at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`, `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'`, a fresh absolute `GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR`, and the absolute existing `DELTA_CREATE_OPTIONS_FIXTURE`. The Gradle task was `:spark-connector:spark-delta-ranger-3.5:test`, with `-PdeltaSparkVersion=3.5.6 -PdeltaVersion=<pin> -PskipITs --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g --console=plain`. Attempt4 selected the class; subsequent runs used method-specific `--tests` selectors shown above. Every reported runtime test task executed; these are not cached test results.

## Measured boundaries

| Case | Native/observer outcome | Registration outcome |
| --- | --- | --- |
| A, installed named SQL | `SecurityException: Unqualified Delta Ranger route: CreateTable` from ordinary admission. No active transaction, reservation or target files. `loadTable`, `tableExists` and `createTable` are NOT_REACHED. | No command sent. This is an analyzer refusal, not a measured schema CREATE policy decision or table-existence result. |
| B, public base stage with installed guards | Descriptor construction has no transport/native effect. One outer commit reserves, writes CREATE0, then throws `SecurityException: Unqualified Delta Ranger route: LogicalRDD`. The first callback is in `Snapshot.emptyDF -> DummySnapshot.stateDS -> RecordChecksum.incrementallyComputeAddFiles`, inside `OptimisticTransaction.doCommit`, after log0. | One RESERVE, no PUBLISH. Journal remains `NATIVE_STARTED`; native commit return failed despite durable log0. No retry or abort/replay is attempted. |
| C, isolated native-only base stage, both Delta pins | Native commit returns. All 13 active-transaction resolution callbacks occur after log0. Native checksum file is present. | RESERVE then exact PUBLISH; the fixture records PUBLISH and throws `INSTALLED_SCHEMA_CREATE_PUBLISH_STOP`. Adapter separately throws `DELTA_PUBLICATION_STATUS_NEEDED: native data and receipt retained`. Final journal phases are `NATIVE_COMMITTED` and `STATUS_NEEDED`; no PUBLISHED response or published load. |

B's one active callback and all 13 C callbacks identify the actual `OptimisticTransaction`, its `DeltaLog`, and `org.apache.spark.sql.delta.DummySnapshot`, with snapshot version=-1, readVersion=-1 and no catalog table. Companion current operation is absent. These are positive observations of an active native transaction, but their timing is too late for precommit authorization. The observer returns the identical plan and verifies unchanged receipt references, current operation, active transaction and settings. It never reads transaction metadata/protocol, state datasets or allFiles, and never runs Spark/native updates.

After unwind, direct filesystem JSON parsing proves CREATE0 has exactly commitInfo/metadata/protocol actions, operation `CREATE TABLE`, a native UUID, nullable BIGINT `id`, no partitions, no data/AddFile action and no writer userMetadata. Its marker exactly matches the decoded reservation token/schema/generation0 and owned local location. All supplied table properties survive, with the existing adapter's marker and mapping-none additions. B retains log0 and the local filesystem CRC sidecar, but no native Delta checksum file; C also retains Delta's checksum. A thrown commit, journal `NATIVE_STARTED`, or an unchanged session commit counter therefore cannot be treated as evidence of no commit.

All cases finally clear `OptimisticTransaction.getActive` and current companion operation. B retains `lastCommitVersionInSession=None`; C changes it from absent to0 on both pins, and this outcome is recorded rather than silently restored. Full operation-baseline settings are unchanged except that allowed native outcome. The normal session catalog is initialized before the baseline; its native additions `spark.sql.parquet.fieldId.read.enabled=true` and `spark.sql.parquet.fieldId.write.enabled=true` are recorded separately, not set manually.

## Fixture and runtime identities

The unchanged [Spark Hub fixture](../../../spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json), SHA256 `789fbd8fa8f22f85daa6105c233c0f34bf239bcfc5b62d7044708ccea9030f18`, supplies every startup setting and table property. Writer options and payload WRITE are unused. Its original provenance is retained with the consumed bytes. Ordinary tests default to a temporary evidence directory and module-relative resource; explicit retained-run overrides are honored. Setup accepts supported Spark 3.5.4/3.5.6, but only 3.5.6 was executed here.

| Runtime JAR | SHA256 |
| --- | --- |
| spark-sql_2.12-3.5.6 | `37535c443d428a8728c21a1317faf5652179748dddc131da494f0377832daa08` |
| delta-spark_2.12-3.3.2 | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| delta-spark_2.12-3.3.0 | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| ranger-plugins-common-2.8.0 (RangerBasePlugin, RangerAdminRESTClient, RangerServiceDef) | `10ad212120866d79566a595ff1282e92a6e43e1460eab558ef0ad4ec1e27d59e` |

The manifests additionally retain Delta storage, Scala, connector runtime JAR and each observed class hash. Delta 3.3.0 was selected only for the decision-relevant native callback comparison; installed A/B were not rerun on that pin. C was necessary because B first exposed the transaction only after log0; C cannot qualify B's unreached checksum completion, native return, postcommit hooks or publication.

## Corrections and limits

Attempt1 stopped at the formatting prerequisite before compilation. Attempt2 retained A's actual refusal but failed the settings-baseline assertion; B stopped at a fixture assertion because the wire codec canonicalized JSON object-key order. Schema comparison now checks structural equality while preserving exact decoded-command/marker binding. Attempt3 failed compilation when setup used a nonpublic catalog accessor; it was corrected to public `catalog("spark_catalog")`. Its accidentally copied prior XML is explicitly labeled `STALE-PRIOR-RUN` with a provenance note and is not attributed to that attempt. The launcher now copies XML only when produced after command start. No production behavior was changed to address these fixture failures. Original attempts are preserved.

Scoped Spotless apply/check passed. Output retains existing Gradle Docker-environment diagnostics, deprecation/compiler warnings and expected native failure stacks; no Docker test or external service was run. The [prior native CREATE0/WRITE1 controls](2026-11-04-delta-create-boundary-findings.md) and installed read/append controls were referenced, not repeated.

Production integration still needs CREATE authorization bound to the reservation and immutable intent **before native invocation**, exact admission for postcommit helpers, and separate publication and outcome handling. This probe demonstrates no precommit transaction-capture callback through the observed public-stage resolution path. That is a valid characterization outcome: it does not establish that such a callback is required by the approved operation-boundary model or that public integration is impossible. No helper permission, absent-table owner, production registration publication, recovery, ingestion/lifecycle support, remote rollback, or full pin matrix is established.
