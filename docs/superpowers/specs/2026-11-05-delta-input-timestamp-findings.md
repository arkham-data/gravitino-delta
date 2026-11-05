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

# Input timestamp qualification boundary

Actual-service qualification passed all four pinned pairs against Ranger
server/client 2.8.0, Gravitino registration and MinIO:

| Spark / Delta | Focused JVM | Classic PySpark cases |
| --- | --- | --- |
| 3.5.6 / 3.3.2 | Task 1 timestamp12/12 and two native controls | Both named writers, partitioned AQE exchange, target denial, late source SELECT revocation:4/4 |
| 3.5.4 / 3.3.0 |14/14 | Named timestamp append and target denial:2/2 |
| 3.5.4 / 3.3.2 |14/14 plus one revocation-witness test | Named timestamp append and target denial:2/2 |
| 3.5.6 / 3.3.0 |14/14 | Named timestamp append and target denial:2/2 |

All successful appends advance exactly one native version with the two masked,
filtered rows and two equal non-null timestamp outputs. Denials publish no native
version. Late source revocation retains one unpublished payload file and the
original publication-changed SecurityException, rather than claiming a rollback.

The first lead fixture stopped before writer launch because a historical
registration-checker import was absent. Its original package/fixture evidence is
retained. A fresh fixture then passed all four native outcomes; its outer checker
found a leftover old26-profile assertion. The corrected finite inventory was
verified by read-only revalidation of those original outcomes, with an exact
checker/test-only source delta. No write was replayed. Remaining pairs passed
with strict current-source checker packages. Final findings changes are separately
recorded as a docs-only source delta; executed packages are never relabeled.

The native input increment admits only a direct stock zero-argument
`CurrentTimestamp` under an alias in a Project over an already supported named
snapshot input. Plain PySpark `withColumn` chains and native named APPEND use the
existing session extension and receipt lifecycle. Both `insertInto` and existing
Delta `saveAsTable` use normal DataFrameWriter calls; no facade is added.

Stock Spark ComputeCurrentTime captures one value before authorization issues the
existing receipt. Every timestamp in that completed query retains that value
through optimizer, helper and native publication. Repeated actions on a retained
QueryExecution follow native cached lifecycle. A separate issued query captures
independently; distinct wall-clock values are not promised.

The bounded live inventory keeps source masks and row filters and the qualified
AQE/exchange settings. Timestamp targets are created initially with their intended
schema, including a region-partitioned target. Lead Spark 3.5.6 / Delta 3.3.2
qualification requires both named writers, target denial and actual late source
SELECT revocation. Each remaining 3.5.4/3.5.6 and 3.3.0/3.3.2 pair requires its
focused JVM suite and actual PySpark positive/denial evidence against Ranger
server/client 2.8.0, Gravitino registration and MinIO. The old 26-profile fixture
inventory is not rerun for this increment.

Authoritative commands, original outcomes, per-pair XML, current source/runtime
hashes, checker RED/GREEN results, readiness and cleanup are retained in
`.superpowers/sdd/2026-11-05-delta-input-timestamp/task-2-report.md` and its
`task-2-evidence` paths. That report distinguishes successful qualification from
failed or unconfirmed attempts; this scope document alone is not a pass record.

Task 1's focused run had an unchanged source-shape assertion-type mismatch and
must not be called broadly green. Schema-changing overwritten target history was
separately unqualified with literal timestamp input too. Native publication
followed by failed observation is committed/unconfirmed and must never be replayed.
A late precommit revocation may leave native payload files without a Delta log
publication; that is a denied no-publication outcome with retained evidence.

Registered Gravitino JDBC and externally authenticated JDBC remain separate pending
plans. Arbitrary DataFrames gain no external-source authority. Nested time
expressions, time predicates, other clocks, UDFs, broad transforms and overwrite
remain outside this increment. The policy/CDF/MERGE grammar is unchanged. Full Hub
optimizeWrite/autoCompact and Spark Hub M2 coverage are not implied.
