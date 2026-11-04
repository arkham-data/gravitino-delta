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

# Delta initial-create boundary findings

The compatible native sequence is **schema-only CREATE0 -> publication evidence at0 -> original-option WRITE1**. This preserves Spark Hub's measured empty-create/data-write behavior. Empty publication can precede data ingestion, so CREATE and WRITE must have separate outcomes. No production code changed.

## Executed scope and retained evidence

`TestDeltaNativeCreateBoundary` originally ran on Spark3.5.6, Java17.0.16 and Scala2.12.21 with each Delta pin on isolated local filesystem targets:

| Delta | Original evidence directory under `.superpowers/sdd/2026-11-04-delta-create-boundary/` | JUnit result |
| --- | --- | --- |
| 3.3.2 | `native-3.3.2-attempt3/` | 11 tests, 0 failures/errors/skips; 14.227s |
| 3.3.0 | `native-3.3.0-attempt1/` | 11 tests, 0 failures/errors/skips; 13.307s |

Review fix I1 changes only setup's hardcoded Spark3.5.6 assertion to accept the module's supported Spark3.5.4/3.5.6 pins. A fresh focused compatibility run on Spark3.5.4/Delta3.3.2, with Java17.0.16/Scala2.12.21 and the same eleven native cases, passed with exit0 and 11 tests/0 failures/errors/skips (18.316s). Its original command, executed task output, XML, runtime and native files are retained separately at `native-spark3.5.4-delta3.3.2-fix-round1/` under the same plan evidence directory. The command is the one below with `-PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2`; original Spark3.5.6 runs are preserved unchanged, not relabeled or replayed. This additional local compatibility check does not establish full four-pair qualification.

Both original test tasks executed, with exit0; neither result was an up-to-date/cache test result. Each directory retains `command.json`, `exit.json`, `gradle.log`, original JUnit XML, consumed test/resource source and SHA256, and `native/`. That directory contains actual Delta logs/data files, phase observations (head, UUID, metadata/protocol, rows, object hashes and settings), failure stacks and class/JAR origins/hashes. Native evidence remains in the plan workspace; it is not a portable server-publication receipt.

The exact command for each pin was:

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
DELTA_CREATE_EVIDENCE_DIRECTORY=<fresh absolute directory> \
./gradlew :spark-connector:spark-delta-3.5:test \
  --tests '*TestDeltaNativeCreateBoundary' \
  -PdeltaSparkVersion=3.5.6 -PdeltaVersion=<3.3.2 or 3.3.0> \
  -PskipITs --max-workers=1
```

The portable [option resource](../../../spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json) is extracted from the immutable prior `native-cell-2/stage-create.json`; its provenance records capture/result/source hashes and Spark Hub revision. It preserves the exact Python dictionary-string `userMetadata`, explicit `overwriteSchema=false`, `mergeSchema=true`, CDF=true, 45/364-day retention and symlink property. Original optimize-write/auto-compaction/default settings remain enabled and unchanged. No current Spark Hub helper import, Kafka, Docker service or predecessor native matrix was run.

## Measured behavior on both pins

| Case | Native/evidence observation and outcome |
| --- | --- |
| Actual `DeltaReplacementStage` around absent native staged COR | Truncate acknowledged; `V1Write.insert(df,false)` retains lazy input. Commit rejects `[DELTA_ILLEGAL_USAGE] The usage of overwriteSchema is not allowed when replacing a Delta table.` Original options remain unchanged. Head=-1, no data files or committed rows: not committed. |
| Native staged CREATE with original options | No commit at insert; commit produces CTAS0 and one row with required properties. Native commitInfo omits writer userMetadata: committed data, incompatible metadata. |
| Existing `DeltaPublicationStage` schema-only CREATE | CREATE0 has a native UUID, empty rows and no writer metadata. Actual log0 plus fresh native head/snapshot0 produce `MATCHED` in `DeltaCommitActionReader`. Only then is the local publication callback considered successful. |
| Original-option data WRITE after evidence0 | WRITE1 contains the exact captured metadata and expected row; native UUID and required properties are unchanged. CREATE and WRITE are separately committed. Repeated owner completion does not repeat either native CREATE or publication. |
| Publication delayed until after WRITE1 | Actual CREATE0 stream with fresh latest/snapshot1 yields `AMBIGUOUS/VERSION_MISMATCH`; no observed UUID is returned. Both native phases committed, first publication does not qualify. |
| First WRITE directly at0 | Native WRITE0 commits metadata/row/properties, but the actual reader yields `AMBIGUOUS/OPERATION_MISMATCH`. A successful native write cannot stand in for registration CREATE evidence. |
| Failed initial data execution | After evidence-qualified empty CREATE0, a lazy `raise_error` source fails during ordinary native WRITE. Head remains0, UUID/properties unchanged, rows empty and no commit1: CREATE committed, data not committed. No automatic CREATE/WRITE replay occurs. Native objects are retained; no storage rollback is inferred. |
| Publication callback failure and explicit retry | CREATE0 commits once; injected publication callback failure leaves its UUID/head intact. A caller-directed completion retry invokes publication only. Native call count remains1 and object hashes do not change. |
| Exception injected immediately after real durable native commit | Actual CREATE0 log still matches. Existing owner refuses another native call with `DELTA_NATIVE_COMMIT_STATUS_NEEDED`, does not publish, and becomes terminal on abort. Initial exception alone is unknown; retained log proves native committed, while publication remains incomplete. |
| Terminal abort | With admitted empty options as a separate lifecycle control, staged failing input stays unevaluated; owner delegates native abort once, prevents subsequent builder/commit use and retains head=-1 with no data files. This does not make native abort transactional. |
| Existing-target fixture precondition and connector option gate | Local existing-target check refuses before stage construction/evaluation; UUID/head/rows/files stay unchanged. Separately, original schema options are rejected by existing `DeltaPublicationStage.newWriteBuilder` before materialization (`Reserved or unsupported Delta option`). Neither check supplies production admission/absence authority. |

Every case observes no active native transaction before/after execution and unchanged session metadata/settings. No session metadata substitution is used. Existing native abort is a no-op; `DeltaPublicationStage` retains stage/builder/input references after terminal transitions. These tests establish terminal guards and native thread/session cleanup, not retained-reference release or distributed resource cleanup.

The CREATE evidence uses the existing exact-schema v1 marker for ordinary mapping-none tables. Its snapshot context comes from fresh native Spark snapshots, not a production pinned Kernel read. This qualifies the existing reader's correlation rules for these native files; it does not prove provider protocol admission, actor authorization, name-mapping policy, a real Gravitino publication or Ranger enforcement.

## Runtime identity and original failures

Runtime records identify the real existing wrapper/owner/common reader classes and the selected cached Maven JARs:

| JAR | SHA256 |
| --- | --- |
| spark-sql_2.12-3.5.4 (review-fix compatibility run) | `06baa5b5190831a1d075eeebb7aa02c872dee91975f97da6a2fb6c5deeff49ae` |
| spark-sql_2.12-3.5.6 | `37535c443d428a8728c21a1317faf5652179748dddc131da494f0377832daa08` |
| delta-spark_2.12-3.3.2 | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| delta-spark_2.12-3.3.0 | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| delta-storage-3.3.2 | `c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36` |
| delta-storage-3.3.0 | `951ce7e89468d1657aaf27067a5b9163a4bcccae3dd7d23d711f50bcd3365bb5` |
| scala-library-2.12.21 | `e2f4040c95cc4dd1cfeaae38bdde596e4b36933632451efa7d8564c6a359510c` |

The Maven Spark JAR differs from the prior PySpark-distribution JAR; the new runtime is explicitly measured rather than claiming the prior binary identity. `native-3.3.2-attempt1/` preserves an exit1 Java compilation failure: `DeltaLog.update` requires explicit Java arguments for Scala defaults. The fixture was corrected to call its public three-argument method. No native qualification is attributed to that failure. `attempt2/` retains the first successful ten-case source/result; the final suite added the direct WRITE0 control and normal-import style fixes. No failing production behavior was fixed or hidden.

The original eleven-case Spark3.5.6 source SHA256 is `f62fefb9273c9595d128b439de35c4648f04eaa9bfe430f31b736422e2adc152`; the setup-only review-fixed source is `425e4836b0a0fb5d412a5874b147adf9470ea06241741ddd86519aac225840d4`. Both original source copies remain in their original evidence directories. The resource is unchanged at SHA256 `789fbd8fa8f22f85daa6105c233c0f34bf239bcfc5b62d7044708ccea9030f18`. `fix-round1-source.diff` records the one-line setup change; no native assertion, option or evidence check was altered. Module Spotless apply/check passed after the fix.

Output note M1: retained successful output includes baseline NativeCodeLoader fallback and Spark plan-truncation warnings, Gradle Docker-probe diagnostics and deprecation notices. The failed-initial-write case intentionally emits Spark ERROR stacks for `native-create-source-failure`. These messages are distinct from JUnit failures; all original output is retained, without global suppression or a logging-only native rerun.

## Production integration decision

Keep the existing CREATE0 evidence/publication boundary before WRITE1. The current reader requires CREATE candidate=latest=snapshot=0, and excludes bare WRITE. Moving publication after WRITE or calling WRITE0 a CREATE would require a different reviewed protocol. Initial empty publication matches existing Spark Hub behavior and is not atomic ingestion.

The next bounded production task should add `userMetadata`, `mergeSchema` and `overwriteSchema` admission to the existing exact-schema governed append path, with immutable option binding, unchanged session-only idempotency, and a precommit fence over effective metadata/protocol. This supplies the initial-WRITE building block; these options are not implemented by this characterization. Creation/schema grants, reservation/absence ownership, retained external inputs and full lifecycle composition remain subsequent work. Preserve original source authorization and operation checks, Delta-owned transactions, explicit committed/not-committed/unknown outcomes and no automatic replay. The local precondition and evidence callback must not be promoted into production authority.

Full M2 remains open. This task adds no S3/MinIO/RustFS, Ranger2.8, live Gravitino publication, mixed governed/external-source admission, schema-evolution refresh, durable platform retry/restart recovery or full four-pair qualification. Existing successful append/AQE results remain separate.
