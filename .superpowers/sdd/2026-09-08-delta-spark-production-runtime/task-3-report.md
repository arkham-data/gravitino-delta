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

# Task 3 packaged runtime implementation report

Base: `f20389c73d505076fb9ca71d44beaeb82b7cc4bd`.
Implementation: `b1df301f6244312ed4021e4af3ff2826c127e70f` (harness) and
`aaf49a363b5526a70d979fc29e20f77709204b62` (current persisted column-version query regression).
Private evidence: `/private/tmp/delta-packaged-runtime-task3-evidence`.
Writer/recovery/authority matrix: `frozen-02` at `aaf49a363b5526a70d979fc29e20f77709204b62`, complete0.
Scoped close-evidence correction: `9930aa17566ef288732f70936516703b2b3d490c`;
`isolation-final-01` uses the same immutable runtime/packages, with its result below.
These are two explicitly identified source snapshots, not one all-code frozen run.

## Changes and source ownership

Added the documented standard-library Python CLI, narrow Gradle production package
preparation, fresh spark-submit applications, external HTTP fault proxy, owned S3
manifest/archive helper, and meaningful harness unit tests under `delta/acceptance`.
The optional runtime is built once and reused unchanged for both native dependency
packages and every driver. Existing production scripts, server runtimeClasspath,
provider package, logging template and H2 schema assets are copied into new owned
directories. Root compileDistribution and test-classpath fixture assembly are not
invoked. The only existing Java change clarifies application retries versus
underlying transport PUT retries in DeltaRegistrationClient Javadoc; behavior is
unchanged. No core/API/server/client/storage-schema source changes, container
mutations, PR/push/merge, or catalog-demo edits were made. Root's plan-only edit is
excluded from these commits.

## Commands and evidence

```bash
python3 -m unittest discover -s delta/acceptance -v
python3 -m py_compile delta/acceptance/*.py
/private/tmp/delta-packaged-runtime-task3-evidence/format-venv/bin/black --check delta/acceptance
git diff --check
python3 delta/acceptance/run.py \
  --inputs /private/tmp/delta-packaged-runtime-task3-inputs.json \
  --output /private/tmp/delta-packaged-runtime-task3-evidence/frozen-02
```

`scoped-java-checks.log` retains the explicit supplied-JDK17 Gradle
`:spark-connector:spark-delta-3.5:spotlessApply`, `spotlessCheck`, and strict
`javadoc` run, exit0. Accepted Task1/Task2/provider suites were not repeated.
`harness-column-version-green.log` records 12 passing harness cases. Python syntax and
Black24.10.0 checks pass. Commands with secrets use private property/config files;
tracked files are scanned against actual supplied credential values without
printing them. Process commands contain file paths, never credential literals.

The frozen run preserves its exact source files, per-file source SHA256s,
source HEAD, real package inventories, every distribution/added jar SHA256,
actual loaded versions/origins, original PID/exit JSON, SQL/assertion events,
server class-load logs, journal copies and exact S3 manifests/native bytes.
There are no invented JUnit XML results for these Python/production process runs.
The prior provider/adapter XML remains separate accepted fixture evidence.

## Original failed attempts and diagnostics

Every directory below retains its original failure/exit and private logs. Later
passes do not replace these artifacts.

| Attempt | Actual result and correction |
| --- | --- |
| `prepare-330.log`, `prepare-330-jdk17.log` | Default Java25 selection produced class major69 failure; supplied JDK17 corrected the environment. A separate narrow-copy preparation failed before the correct real logging-template directory was copied. These are setup failures, not behavioral REDs. |
| representative-01 | Real packaged server exit255: shipped H2 schema assets omitted. Narrow packaging now copies ordinary `scripts/h2`; no production source defect. |
| representative-02 | Real Spark exit1: missing private journal parent and fake Java user.name override conflicted with local journal ownership. Create owned parent and retain actual OS Java user identity; Spark/Hadoop actor configuration remains separate. |
| representative-03 | Real native SQL/checkpoint/read assertions passed until unquoted `DESCRIBE HISTORY ...shared.history` collided with the Delta keyword. Original exit1 retained. |
| diagnostic-01/02 | Actual installed DeltaSqlParser and extension recorded. Native path and ordinary identifiers parse; exact unquoted table `history` selects the wrong parse plan, while individually quoted components produce DescribeDeltaHistory. Harness SQL identifiers corrected; no parser/provider defect or unsupported three-part-command claim. |
| representative-04 | Actual main Spark exit0 including quoted HISTORY/checkpoint40/version41/42 rows. Outer evidence check incorrectly required a pending marker in a final PUBLISHED receipt. Correct evidence binds the retained original command and published identity to exact native commit actions; final receipts correctly omit pending markers. |
| representative-05 | Seed/main exit0 and outer complete0. Exact native operation/manifests, stopped-server column identity and provider origins pass. Representative scope only. |
| process-01 | Original reserve fault exit1; operator ABORT failed without required explicit HANDOFF. Recovery sequence corrected, with ABANDONED terminal state. |
| process-02 | A one-shot PUBLISH drop allowed the production PUT transport retry to publish. The original nonzero exit was then a harness expectation failure, not product publication uncertainty. HANDOFF on already PUBLISHED state correctly failed. Persistent before-forward blocking now covers all attempts; completed-publication loss separately accepts PUT retry plus STATUS-only recovery. |
| process-03 | Actual three recovery cases completed (original exits1/1/0, recovery0/0/0), then adoption expectation failed because ADOPT returns ADOPTING and needs explicit PUBLISH. These recovery results predate the final accepted-socket drain fix. |
| isolation-01 | Both native endpoint writers and fresh reads exit0; alternating provider reads and unchanged adoption manifests pass. Primary disable followed by secondary read did not prove asynchronous close completion. Later authority setup incorrectly attempted transfer while pending and received404. |
| authority-01 | Published actor transfer and revoked-grant403 checks pass. Published opening POST replay returns actual500 REGISTRATION_CHANGED, not guessed409; expectation corrected. |
| isolation-02 | Adoption/isolation and production-observable close completion pass; authority negatives pass. Quarantine GET returns actual404 NoSuchTableException ('is not published'), not guessed500. |
| recovery-authority-01 | Corrected accepted-connection/unknown-completion proxy sequence passes all actual recovery cases; original exits1/1/0 and recovery0/0/0. Quarantine404 repeated because the intended predicate edit had not applied before snapshot; original failure retained and exact predicate corrected. |
| frozen-01 | First pair disabled/seed/main all exit0; outer identity query rejected duplicate historical replacement-column rows. The earlier representative parser tolerated those rows; current inspection must select latest column version at/below current table version, exclude DELETE operations and soft-deleted rows. No product mutation/defect. |
| authority-02 | Complete0: actor grant revocation, owner-stable published POST rejection, original-object-preserving native identity fault, privileged QUARANTINED/NATIVE_ID_CHANGED status and fresh named Spark rejection all pass. |

Early independently prepared runtime330/runtime332 jars differed because of ZIP
metadata/build-info timestamps, with every other ZIP entry byte-identical. Those
preparation artifacts are retained honestly. The final harness supplies one common
runtime binary rather than relabeling different ZIP hashes as one artifact.

## Harness regressions

- Initial missing `harness` module RED (`harness-red.log`) is scaffolding, not an
  executed behavioral failure. Some early checks were added after implementation;
  blanket test-first coverage is not claimed.
- Real HTTP response-loss test confirms a completed upstream response and exactly
  one POST; request and response bodies are excluded from proxy event logs.
- Persistent-fault RED/GREEN (`harness-persistent-*.log`) covers every PUT attempt.
- Owned S3 guard RED/GREEN (`harness-s3-*.log`) refuses unowned/traversal/deletion
  requests before network access. Credentials are parsed as literal data.
- Accepted partial-body RED/GREEN (`harness-partial-body-*.log`) exposed counting
  only after body reads. Accepted sockets now count before parsing; a blocked body
  prevents drain/disarm. Closing the original socket permits successful drain.
- `harness-drain-proof-red.log` records a real failure allowing disarm without a
  successful drain. GREEN requires a joined accept loop, all accepted handlers
  complete, closed listener, and no unknown upstream completion. A blocked upstream
  request times out the local drain check; a subsequent upstream disconnect keeps
  completion unknown after its local handler exits, so recovery remains blocked.
- The proxy reopens its same owned listener only after successful drain. It never
  promotes unknown completion because time elapsed or a handler exited. Actual
  final fault cases use independently confirmed completion or proven non-forwarding.

The current-column query regression (`harness-column-version-red.log` then
`harness-column-version-green.log`) executes historical create/delete/replacement,
unchanged-column and future-version rows in an isolated SQLite database. The old query
returns retired and future identities; the corrected production-aligned selection
returns only current live columns. A separate actual read-only H2 query against the
stopped frozen01 server (`h2-current-columns-regression.log`) observes 12 current
columns and confirms both original fixture IDs plus a distinct new column. Full
frozen02 repeats the actual before/after process sequence with this corrected query.

## Acceptance scope and boundaries

The writer matrix targets Spark3.5.4/.6 by native Delta3.3.0/.2 against one server
Kernel3.3.2/Hadoop3.3.6 provider. Actual driver Hadoop3.3.4/Scala2.12.18/JDK17.0.16
comes from the supplied real distributions; hadoop-aws3.3.4 and its production-POM
AWS SDK1.12.262 are added. No Kernel dependency is admitted on the driver.

Native CREATE/CTAS/replacement variants/DFWriterV2 and basic DML are successful
subset assertions, not full lifecycle conformance. Mapped/basic nested schemas
and top-level NOT NULL cover admitted examples, not every feature combination.
The checkpoint gate is actual checkpoint40, direct provider refresh0→41 after
trusted native path appends, named42-row read and quoted HISTORY. Historical JSON
expiration/deletion and VACUUM are explicit follow-ups; no native objects deleted.

Publication ID and exact commit evidence, owner preservation, and existing column
identity are distinct assertions. Public ColumnDTO omits internal IDs; before/after
read-only `ACCESS_MODE_DATA=r` H2 queries occur only after owned server shutdown.
There are no direct-store writes or promotion.

Recovery distinguishes no added application retry, underlying HTTP PUT retry,
and no POST replay. Confirmed reserve-response loss uses STATUS/reread plus explicit
HANDOFF/ABORT; persistent unforwarded publication uses STATUS/reread/HANDOFF/PUBLISH;
completed publication loss can succeed through PUT retry and uses STATUS-only
recovery. HANDOFF is operator attestation, not fencing. Driver exits, accept-loop
join, accepted-handler drain and upstream-completion facts precede disarm and
server restart/operator actions. Unknown completion remains blocked.

Same-session Spark catalogs use different prefixes on one deployment endpoint.
Incompatible native Spark endpoint configurations use fresh drivers. Provider
isolation uses simultaneously loaded contexts at identical S3 authority and path
on independent endpoints/credentials. Existing CatalogManager named removal and
subsequent ClassLoaderPool release (positive remaining refcount) establish primary
cleanup completion before a secondary read. The matrix revision's automatic helper
checks ordering/provider/refcount, while a subsequent bounded correction requires
the same cleanup thread and exact configured loader key. Cross-thread and wrong-key
behavioral RED/GREEN is retained in `harness-close-correlation-*.log`. The corrected
helper receives its own fresh isolation run; it is unused by the writer matrix,
whose original frozen source and runtime remain separately identified. Earlier disable-only results do not
claim close completion.

Grant revocation after ownership transfer proves local retired actor authority,
not token expiry. Published original POST replay cannot replace successor ownership.
Native-ID quarantine is dedicated external-log fault injection: archive all original
bytes, append one metadata-only version with a new UUID after original writer exit,
reject REST/named Spark load, and assert ordinary administrative STATUS retains
id/generation/token with QUARANTINED/NATIVE_ID_CHANGED. Original published/native
UUID evidence and created-object manifests are retained. Observation afterward
must leave native objects unchanged.

Root owns both MinIO endpoints/containers. Harness cleanup is limited to owned
server/proxy/Spark children; native objects and private evidence remain. Simple
authentication is local test identity, not authentication hardening. Arbitrary-job
protection, full Delta lifecycle/features, exact full-row CAS barriers and internal
owner-hook failure injection are not established here. Accepted provider fixture
results remain separate. Packaged production Ranger2.8 policies/authorization
callback failures and isolated catalog-demo acceptance belong to following plans.

## Frozen artifact digests

Both production preparations exited0. The following exact artifacts were prepared;
per-driver inventories establish actual selection. Full closure hashes are in
`frozen-02/artifacts.json`; actual loaded origins are asserted separately.

| Artifact | SHA256 |
| --- | --- |
| `aws-java-sdk-bundle-1.12.262.jar` | `873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5` |
| `delta-kernel-api-3.3.2.jar` | `4174080102de50d455b772c0bfc96004dd9fe4e7b536b6c9998a8b9b2441bbc9` |
| `delta-kernel-defaults-3.3.2.jar` | `deb6c12432e3ccbedf0c57ba41ae82f00f3f406db787fd2109d766894c7c4a1f` |
| `delta-spark_2.12-3.3.0.jar` | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| `delta-spark_2.12-3.3.2.jar` | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| `delta-storage-3.3.0.jar` | `951ce7e89468d1657aaf27067a5b9163a4bcccae3dd7d23d711f50bcd3365bb5` |
| `delta-storage-3.3.2.jar` | `c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36` |
| `gravitino-catalog-lakehouse-delta-2.0.0-SNAPSHOT.jar` | `f8ff1b4e3d09473742c128557234f3b36b2c94217b6075300e07dcc85c942fe8` |
| `gravitino-spark-connector-delta-runtime-3.5_2.12-2.0.0-SNAPSHOT.jar` | `f7a4310e08451380dbd3d7fd7833f88763f97bded47b063d70aa23193d507049` |
| `hadoop-aws-3.3.4.jar` | `53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a` |

## Frozen outcome and self-review

`frozen-02/completed.json` records outer success and 25 original Spark exits:
23 zero exits and two expected original uncertainty failures (exit1). Both narrow
production package preparations exited0. All four pairs completed their disabled,
seed and main processes with exits0/0/0. Each main driver contains 31 predicate
checks, two parser/history observations, and 27 SQL events; the 60 records are not
60 tests. Actual DeltaSqlParser, checkpoint40, complete HISTORY versions0..41,
42-row named read, two stable fixture column IDs and a distinct added ID are
retained for each pair. Exact publication counts are12+1 across each pair's two
catalogs; unchanged manifests contain200 and4 objects respectively.

| Spark | Delta | Disabled / seed / main exits | Current identity / native evidence |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 0 / 0 / 0 | Passed |
| 3.5.4 | 3.3.2 | 0 / 0 / 0 | Passed |
| 3.5.6 | 3.3.0 | 0 / 0 / 0 | Passed |
| 3.5.6 | 3.3.2 | 0 / 0 / 0 | Passed |

| Recovery case | Original / fresh exits | Observed operation attempts |
| --- | --- | --- |
| Confirmed reserve response loss | 1 / 0 | One RESERVE POST; no replay |
| Commit before publication | 1 / 0 | Two PUBLISH attempts, both not forwarded |
| Completed publication response loss | 0 / 0 | Two PUBLISH PUT attempts; transport retry returned publication |

All three `drain-*.json` artifacts record the original exit, joined accept loop,
closed listener, zero accepted handlers, zero unknown upstream completions and
successful drain before operator recovery. POST counts are unchanged by recovery.
The six recovery processes are actual fresh Spark JVMs with persisted server
restart and real journal/APIs, not injected provider fixture seams.

The original full run's endpoint-native writers and fresh reads all exited0.
`primary-close-completion.log` independently correlates removal/release on
`ForkJoinPool.commonPool-worker-9`, exact key
`ClassLoaderKey{provider='lakehouse-delta', properties={}}`, and remaining refcount10,
with no competing removals or close failure before the subsequent secondary read.
This supplemental audit is stronger than the original helper's automatic check.
The later corrected helper asserts that same correlation automatically in the
separately scoped isolation run below.

Authority creation and fresh quarantine named rejection processes exited0.
`native-id-quarantine.json` binds retained id/generation/token to the prior published
receipt, records QUARANTINED/NATIVE_ID_CHANGED, and proves one created native log
object with every original object unchanged. The actual object was subsequently
read back into `quarantine-injected-native-log.jsonl`; its SHA256 matches the saved
manifest and its new UUID differs from the original published/native UUID. No
native object was replaced or deleted.

`frozen-02/independent-final-audit.json` and `frozen-02-audit.log` confirm the 85 source
hashes before the close fix, all original exits, actual pins/parser/history,
identity/native objects, common runtime and production cleanup correlation.
Root's independent `task-3-root-matrix-verification.json` separately verifies all
four writer pairs. Root also compared the archived Task2 runtime with frozen02:
both contain10412classes with no additions/removals. Only DeltaRegistrationClient
and its Observation nested class have different raw bytes; JDK17 `javap -p -c -s`
outputs match exactly, consistent with Javadoc/debug-line shifts. Original evidence
is `task-3-runtime-class-comparison.json` and adjacent task2/task3 javap outputs.
Whole-jar digests remain distinct and are not conflated by this comparison.

Self-review checked production-only origins, actual dependency separation,
phase-specific transport outcomes, request drain versus upstream completion,
owner/column/registration identity, exact immutable native manifests, source/artifact
binding and preservation of original failures. No product behavior change or
unresolved product defect was found in this scoped subset. Earlier setup/query/
expectation errors are preserved and characterized above. Full Delta, expired-token
and Ranger/arbitrary-job enforcement claims remain excluded as described above.

`isolation-final-01/completed.json` records the corrected scoped run at
`9930aa17566ef288732f70936516703b2b3d490c`: outer0, both native writers0 and both fresh
reads0. The stronger helper and independent audit correlate
`ForkJoinPool.commonPool-worker-6`, exact provider key, and remaining refcount2,
then the secondary read passes. Original before/after adoption manifests remain
unchanged. `isolation-final-01/independent-final-audit.json` binds its source and
actual common runtime. `final-scope-binding.json` proves that only
`delta/acceptance/harness.py` and `test_harness.py` changed between the two freezes;
83 other files (including writer and production sources) are unchanged. Per root's
scoped-retest ruling, unaffected writer/recovery/authority processes were not rerun.
This is combined explicitly scoped evidence, not one all-code frozen run.

Final local checks are recorded in `final-checks.json`: 12 harness cases pass,
Black24.10.0 and Python syntax pass, git diff whitespace check passes, and actual
supplied credential values are absent from every Task3 tracked source/doc/report.
The Java behavior was unchanged after the earlier scoped strict Javadoc/Spotless
pass. All owned server/Spark/proxy processes completed or were explicitly stopped;
root retains the service containers and all original evidence. No broad package
archive was made by this worker.

Final source commits are b1df301f6244312ed4021e4af3ff2826c127e70f,
aaf49a363b5526a70d979fc29e20f77709204b62, and
9930aa17566ef288732f70936516703b2b3d490c. The report/documentation commit contains this
file and `docs/delta-packaged-runtime-acceptance.md`; root's plan-only change remains
outside worker source commits. Ready for independent task and continuous-chain
review; no review approval is claimed by the implementer.
