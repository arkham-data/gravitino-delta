<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Native Delta append characterization

## Qualification status

The test-only split append helper passed qualification against unmodified native
Delta on all four version pairs on October 5, 2026. Independent source review and
artifact verification passed. This checkpoint
precedes production implementation of the
[governed append design](superpowers/specs/2026-09-27-delta-governed-append-design.md);
it does not enable Ranger-governed writes.

| Spark | Delta | Complete adapter class | Evidence |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 14 passed; no failures, errors or skips | `resume-full-primary-09` |
| 3.5.4 | 3.3.2 | 14 passed; no failures, errors or skips | `matrix-354-332-10` |
| 3.5.6 | 3.3.0 | 14 passed; no failures, errors or skips | `matrix-356-330-11` |
| 3.5.6 | 3.3.2 | 14 passed; no failures, errors or skips | `matrix-356-332-12` |

Runs use JDK 17.0.16, Scala 2.12.21, local filesystem storage and the same frozen
test sources. Each complete class contains eight append test groups and six
existing adapter regressions: 56 passing test executions in total. Each cell
retained 96 invocation directories: 41 matching native/split pairs and 14 standalone
controls. Across the matrix, independent audits verified all 384 invocation
records, 4,184 copied table files and 164 normalized pairs. Scoped formatting
passed; this is focused module verification, not a repository-wide release check.

## Behavior measured

The probe decorates Spark's actual `AppendDataExecV1` V1 writer and binds its
received input to the resolved `AppendData.analyzedQuery`. Native controls invoke
the original writer. Split controls use native transaction/idempotency handling,
`WriteIntoDelta.writeAndReturnCommitData`, original actions/tags and
`commitIfNeeded`, preserving inner and outer cache refresh behavior.

- SQL VALUES, SQL INSERT SELECT, DataFrame `insertInto`, `saveAsTable` and
  `writeTo(...).append()` retain their native positional/name semantics. SQL
  insertion happens before result collection; collecting again does not replay it.
- Native and split controls compare literal rows, versions, schema/protocol,
  configuration, material actions, file sizes/statistics, history and transaction
  identifiers. Self-append, empty inputs, constraints, schema evolution, options,
  null defaults and native SQL assignment casts have explicit controls.
- Entry rejection leaves the owned table's complete file manifest unchanged.
  Rejection after the native helper preserves generated files but publishes no
  Delta commit or pending schema change. Neither path retries or cleans up those
  files as part of the write.
- Transaction duplicates and session transaction-version reset match native
  behavior. Relevant configuration is bound at entry; only the observed native
  reset transition is allowed. Option-provided identifiers take precedence over
  session identifiers.
- Actual nested SQL execution events, output metrics, cache refresh, native
  checkpoint artifacts and fresh-session readback are checked. Non-append native
  cache analysis can use a cloned SparkSession; foreign-session append attempts
  still reject.

The guard delegates effective auto-compaction settings to Delta's native resolver.
Three failing regression assertions demonstrated the earlier incorrect precedence
and missing legacy fallback. Enabled compaction rejects before the helper; a
session-level disable correctly overrides enabled table settings. This verifies
the exclusion boundary, not support for enabled auto-compaction.

## Evidence and build correction

Private artifacts remain under
`.superpowers/sdd/2026-09-27-delta-native-append-characterization/`. Each accepted
cell binds source hashes, archived compiled classes, actual loaded JAR origins
and hashes, JUnit XML, Gradle output and final method outcomes. Each invocation
retains raw plans/events/results, before/after file manifests, copied table bytes
and artifact classifications. Normalized comparisons are separate from raw
commits; a nine-field ledger explains every fixture/generated value normalized.
Material metadata, options, metrics, file sizes and statistics are not discarded.

Frozen source hashes:

| File | SHA-256 |
| --- | --- |
| `TestDeltaNativeAdapter.java` | `de3b379733beadd775224d143f8f517d42c06c6828a4a6ebf21c2692be2fd382` |
| `NativeAppendProbe.scala` | `9d77c87db41d94982c634aea146acc41d7e021cc72cee24eb5e03981d8dca20d` |
| `spark-delta/build.gradle.kts` | `f7c133d6a59b24325a0673c03dc19e6c7c2ac1d6b0605949803a862d900acc74` |

The build correction limits existing Spark/Scala dependency pins to the four
application compile/runtime configurations. Previously they also forced Scala
2.12 onto Zinc's Scala 2.13 compiler dependencies. Dependency versions and task
ordering are unchanged; normal Gradle compilation replaces no compiler stage.
The exact commands and assertion inventory are in the
[characterization plan](superpowers/plans/2026-09-27-delta-native-append-characterization.md).

## Remaining boundaries

No companion authorization is installed in these tests. They establish neither
Ranger UPDATE enforcement nor S3 behavior, atomic policy/registration fencing,
general user cache/AQE support, enabled auto-compaction or full Delta support.
The entry cases prove helper ordering and unchanged owned storage; no reliable
nonintrusive source-scan counter was available. Native literal-input write events
provide output-row metrics but no timing metric; that absence is recorded, while
execution start/end times are retained separately.

Production append still needs a target UPDATE receipt, retained source SELECT
obligations with Ranger masks/filters, owned native write scopes, and validation
both at entry and before commit. Native transaction-version reset and self-append
source planning must fit those ownership rules. DML/DDL, lifecycle, history/CDF,
streaming, maintenance/features, reconciliation and catalog-demo integration
remain in the [full implementation plan](superpowers/plans/2026-09-07-delta-spark-implementation.md).
The existing [finite SELECT milestone](delta-ranger-production-results.md) is
separate; no live fixture or production Gravitino core source changed here.
