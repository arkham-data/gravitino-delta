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

# Governed initial schema CREATE: actual-service findings

The initial **direct-user, schema-only CREATE** profile passed actual Gravitino, Ranger server/client 2.8.0 and MinIO qualification on Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2. The exact declaration is `(id BIGINT) USING delta` with the unchanged native-create fixture properties/settings. This does not close M2 or establish general CREATE support.

**Successful group-only CREATE is unsupported in the current SIMPLE-only application profile.** Actual Hadoop UGI group membership and the downloaded group role admitted the request in Ranger, but SIMPLE authentication supplies no groups to Gravitino. The original RESERVE returned HTTP 403 before provider/native effects. A later public group-role removal denied a fresh target in Ranger before any RESERVE. This is a group-removal control, not successful group publication. Authenticated identity/group propagation must be separately qualified before deploying group-only CREATE; OAuth/token application modes are explicitly rejected by the current companion.

## Source and actual runtime

The qualifying production checkpoint is `c610e4dc5fdcaa2510975129e37857c5ead7621a`. It incorporates the reviewed CREATE-only effective-policy refresh correction after the first real owner-policy propagation exposed a continuation failure. The source worktree was `.worktrees/spark-hub-adoption-plan`; Java 17 and Scala 2.12.21 were used.

One fresh assembly rebuilt the changed closure: base runtime, optional Ranger runtime, Gravitino server, Delta provider and Ranger Delta authorization plugin. It used the surviving assembly lane adapted only in task-owned scratch files. Build/source receipts, copied project dependencies, class/JAR hashes, unique provider origins, six shaded Ranger 2.8 Maven entries and the existing downloaded-query helper allowlist are retained. It did not put the entire test runtime classpath on Spark or introduce another JVM driver. The remaining pairs reuse those verified production bytes with their actual stock Spark and selected native Delta JARs. Historical source-match assertions were not weakened.

The fixture is `spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json`, SHA-256 `789fbd8fa8f22f85daa6105c233c0f34bf239bcfc5b62d7044708ccea9030f18`. CDF, 45-day log retention, 364-day deleted-file retention, optimizeWrite, autoCompact and symlink compatibility remain enabled. No writer options, payload write, seeded native target, ADOPT or direct PUBLISHED registration were used. Private metadata identity owns only its isolated fixture catalog; actual absent/read checks use that distinct identity.

## Executed cells

| Spark / Delta | Actual controls | Native/publication result | Granted-cell fresh JVM elapsed |
| --- | --- | --- | ---: |
| 3.5.6 / 3.3.2 | No grant, other-schema grant, shared-schema grant, privilege revoke, direct-role removal, group-role removal, literal Ranger deny, allowed wildcard sibling | Two direct-user CREATE0/checksum → PUBLISHED0; six denied targets have no reservation/native effects | 7.530 s for initial granted cell |
| 3.5.6 / 3.3.0 | No grant, shared-schema grant, downloaded privilege revoke | CREATE0/checksum → PUBLISHED0; both denied targets absent | 7.140 s |
| 3.5.4 / 3.3.2 | Same bounded three-cell subset | CREATE0/checksum → PUBLISHED0; both denied targets absent | 6.584 s |
| 3.5.4 / 3.3.0 | Same bounded three-cell subset | CREATE0/checksum → PUBLISHED0; both denied targets absent | 6.500 s |

These are **17 passing functional cells: five published CREATEs and twelve denied controls**, plus the separately retained unsupported group-granted attempt. The table does not label all historical attempts green. Each denial uses a fresh literal table name and actual policy/role downloads; it does not revoke a grant on a table whose new owner permissions could mask that removal. The full first-pair lifecycle was not duplicated on the other pairs.

A fresh JVM follows each completed public grant/revoke phase. Its initial completed policy/role attempt IDs join retained real response bytes/digests and the phase's actual public/Ranger models. A numeric generation floor alone is insufficient. The group control records actual UGI groups and absence of direct CREATE-role membership. The literal deny is a separately owned Ranger policy; schema wildcard permission still creates its fresh sibling. Existing managed owner items and foreign policy models are not rewritten to obtain admission.

All successful cells retain original public RESERVE and PUBLISH requests/responses and independent provider STATUS. Initial generation is zero; marker, ID, token, run, location and native UUID correlate. The native log has exactly commitInfo/metaData/protocol: CREATE TABLE, nullable `id: long`, no partitioning, Parquet format, exact fixture configuration plus the operation marker and column-mapping `none`. Protocol is reader 1/writer 7 with changeDataFeed, appendOnly and invariants. The native checksum matches log metadata/protocol/transaction ID and contains zero files/bytes and an empty AddFile list. There is no version 1, payload or CDF data file. Empty-table symlink behavior produces no data manifest; its enabling property is preserved. Native checksum work completes through the unchanged stock lifecycle.

A denied metadata lookup is not treated as proof of a free name. Denied controls also require no public RESERVE/PUBLISH and positive owned-object listings. The separate group transport limitation records one attempted RESERVE returning 403, no successful reservation receipt and no native effects.

## Original failures and correction history

The initial runner attempt retained a valid no-grant denial but stopped on an evidence filename mismatch: the existing driver writes `downloads.jsonl`, with separate start/completion records. Offline joining corrected the checker without replaying that target.

The first granted live attempt at `743dab36c` wrote log0, then normal RESERVE owner reconciliation downloaded policy 14 after starting at policy 13. The in-flight checksum continuation rejected the changed publication stamp. Its original driver reported native UNKNOWN/publication NOT_ATTEMPTED. Independent bytes prove native COMMITTED0 with incomplete checksum/symlink work; provider STATUS confirms RESERVED and no PUBLISH request exists. The original target, failed exit, download bodies and logs remain unchanged. The first observational STATUS failed because the stopped recording proxy prevented catalog initialization; restoring its exact endpoint allowed STATUS, with no CREATE/PUBLISH replay.

The separately reviewed production follow-up reauthorizes immutable literal CREATE through the existing engine at every existing CREATE continuation check. It preserves each evaluation's stable-publication guard and all structural/native proofs. Fresh qualification kept the original 500 ms polling interval. The fresh successful first-pair command observed policy 13 → 14 and completed publication; no slower polling workaround was applied.

After the five early passing cells, the group-granted REST 403 stopped the runner as an actual failure. Its original driver exit and wire bytes remain retained. A bounded continuation restored a byte-verified copy of the stopped fixture store with the same endpoints and entirely fresh evidence/log directories. It referenced the five original passes by hashes and ran only the unexecuted group-removal, literal-deny and wildcard-sibling cells. No attempted target was replayed.

## Evidence and verification

Local evidence root: `.superpowers/sdd/2026-11-04-delta-governed-schema-create/`.

- `task-3-final-evidence.json`: four package/source/class inventories, per-cell evidence hashes, exact process elapsed times and actual completed download counts.
- `task-3-package-c610-356-332/`: fresh shared assembly/source/build receipts. `task-3-package-c610-{356-330,354-332,354-330}/`: actual derived runtime manifests.
- `task-3-live-c610-356-332/` and `...-continuation/`: original five passes, unsupported group failure, fresh remaining three cells and hash-bound inherited evidence.
- `task-3-live-c610-{356-330,354-332,354-330}/`: bounded three-cell pair results.
- `task-3-live-356-332-attempt{1,2}/`: unchanged original failures, public/raw objects, retained STATUS and separate outcome classifications.
- `task-3-report.md`: exact commands, correction history, checker red/green logs and review handoff.

The final checker selection has 13 passing unit tests. It rejects missing/forged publication, version-1 substitution, incorrect marker/token/run/native ID, consistently forged nonzero generation, hidden reservations, missing object evidence, stale byte inventories, missing or unrefreshed policy models, missing checksum and metadata/serializer recipe drift. It preserves committed-unpublished and unsupported group outcomes separately. Raw original objects, runtime origins and completed models were rechecked after formatting; no live mutation was repeated for those checks.

## Operational and remaining scope

The elapsed times above include JVM startup, policy downloads, SQL and shutdown; they are not isolated CREATE latency. The focused Task 2 fixture measured 153 literal CREATE evaluations per successful operation. The existing live driver does not expose an evaluation/audit counter. Local Gravitino audit files are empty; actual download transport counts are retained separately and are not audit throughput. Production throughput and audit capacity remain unmeasured. A continuously overlapping refresh can still fail closed; the functional qualification does not remove that operational concern.

Deterministic before-native/before-publication revocation and other timed faults remain Task 2 focused tests. No live deterministic phase barrier, production override, atomic wall-clock authorization or automatic recovery is claimed.

The next M2 task is ordinary schemas, partitioning, comments, properties and `createIfNotExists`. Generated date partitions remain an M5 gap. Schema evolution, inputs and overwrite goals remain open. Only this initial direct-user schema profile is qualified; the broader Spark Hub/M2 surface is not closed.
