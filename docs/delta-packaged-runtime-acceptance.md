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

# Packaged Delta and Spark acceptance

The opt-in [acceptance harness](../delta/acceptance/README.md) runs an ordinary
packaged Gravitino server with an isolated Delta provider loader, persisted H2,
and fresh Spark drivers against two local S3 endpoints. It retains original
process exits, journals, object manifests, loaded class origins and jar hashes.

## Qualified deployment

The frozen process matrix result is recorded after execution below. Server and
writer dependencies are intentionally independent:

| Process | Components |
| --- | --- |
| Packaged provider | Delta Kernel 3.3.2, Hadoop 3.3.6 |
| Fresh Spark driver | Spark 3.5.4 or 3.5.6, Delta Spark/storage 3.3.0 or 3.3.2 |
| Distribution runtime | Hadoop 3.3.4, Scala 2.12.18, JDK 17.0.16 |
| Added S3 driver dependencies | Hadoop AWS 3.3.4, AWS SDK bundle 1.12.262 |

All four pairs use one optional Gravitino runtime binary. The driver has no Kernel
dependency; actual server class-loading records establish that Kernel and the
provider come from `catalogs/lakehouse-delta/libs`. Test classes and embedded server
fixtures are excluded from these production classpaths.

## Assertions

Each pair exercises enabled and disabled discovery, native `DeltaTableV2`, two
catalogs with the same schema/table names, CREATE/CTAS/REPLACE/COR/RTAS/CORAS,
DataFrameWriterV2 create/replace/createOrReplace/append, S3 append/overwrite,
UPDATE/DELETE/MERGE, version-zero reads, and native schema evolution followed by
provider refresh. Basic nested structs, arrays and maps run with column mapping
`none` and `name`; a top-level NOT NULL constraint rejects an invalid write.

Publication receipts bind to exact native metadata and commit actions. Creator
ownership and existing column identity survive refresh; before/after column IDs
come from read-only H2 queries while the owned server is stopped, because public
column DTOs omit internal IDs. These inspections never mutate or promote state.

The checkpoint case leaves the provider at version zero while a trusted native
path workload appends 41 commits. An actual checkpoint at version 40, provider
refresh directly to 41, named read of 42 rows, and quoted `DESCRIBE HISTORY` are
asserted. Historical JSON expiration/deletion and VACUUM are not exercised.

## Recovery and isolation boundaries

Reserve response loss occurs only after the proxy fully reads the actual upstream
response. Commit-before-publication blocks every PUBLISH attempt before forwarding
until the original driver exits and all accepted proxy connections drain. The
server then restarts with persisted state. Fresh operators use STATUS, immutable
journal rereads, and explicit HANDOFF plus ABORT or PUBLISH where appropriate.

The adapter adds no application retry. The production HTTP transport can retry
PUT after response loss, so a completed PUBLISH may return PUBLISHED to its original
driver through a transport retry. That case uses STATUS-only recovery. No recovery
POST is replayed. HANDOFF is an explicit operator attestation, not writer fencing.
A locally finished handler with unknown upstream completion cannot establish drain
or allow operator recovery; a timeout is never a completion fence.

Observational adoption compares exact before/after native object manifests.
Provider isolation uses identical S3 authority and path on two independent
endpoints with different credentials and simultaneous loaded catalog contexts.
Existing production logs establish primary catalog close completion and a positive
remaining shared-loader reference count before reading the secondary again.
Same-session Spark catalogs use separate prefixes on one deployment endpoint;
incompatible native Spark endpoint profiles run in separate fresh drivers.

Grant revocation tests that the old actor receives 403 for the original command,
STATUS and PUBLISH after ownership transfers. Published opening POST replay is
rejected before and after transfer without changing the successor owner. A separate
sacrificial table receives an appended metadata-only native identity fault after
its writer exits. Originals remain archived; privileged STATUS must report the
same registration identity with QUARANTINED/NATIVE_ID_CHANGED, and REST and a fresh
Spark named load must reject it. This is external-log identity fault injection,
not a successful native lifecycle operation or observational adoption.

## Limits

This qualifies the listed packaged process behavior, not full Delta feature or
lifecycle conformance. Type widening, deletion vectors, every protocol/mapping
variant, historical log expiration, VACUUM and broader concurrency remain follow-up
work. Native path writes are trusted test workload and do not demonstrate arbitrary
job protection. Local simple authentication plus grant revocation does not qualify
credential/token expiry or authentication hardening.

Accepted owner-hook injection and exact full-row CAS barriers remain separate
provider fixture evidence. Actual downstream authorization callback failure,
production Ranger 2.8 policies and the isolated catalog demo belong to subsequent
plans. No claim of Ranger enforcement follows from this matrix.

## Execution record

The writer matrix executed at `aaf49a363b5526a70d979fc29e20f77709204b62`.
Each pair's disabled-discovery, seed, and main Spark processes exited zero:

| Spark | Native Delta | Disabled / seed / main exits |
| --- | --- | --- |
| 3.5.4 | 3.3.0 | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 0 / 0 / 0 |

Each main driver records 31 predicate checks, two parser/history observations and
27 SQL events. SQL events are not counted as tests. Per pair, before/after evidence
confirms two existing column IDs remain stable, one added column has a distinct ID,
12 + 1 exact native publications, and unchanged manifests containing 200 and four
objects respectively.

All drivers select runtime SHA256
`f7a4310e08451380dbd3d7fd7833f88763f97bded47b063d70aa23193d507049`.
The three recovery original/fresh-driver exits are `1/0`, `1/0`, and `0/0`;
publication blocking and completed-publication loss each observed two PUT attempts.
The full run completed with 25 Spark processes: 23 exited zero and the two expected
uncertainty faults exited one. Authority revocation, published replay rejection and
native-ID quarantine passed.

The subsequent close-log correction at
`9930aa17566ef288732f70936516703b2b3d490c` received a fresh isolation-only run in
`/private/tmp/delta-packaged-runtime-task3-evidence/isolation-final-01`: all four
native writer/read processes exited zero. Its automatic check correlates the
primary removal and release on `ForkJoinPool.commonPool-worker-6`, the exact
`ClassLoaderKey{provider='lakehouse-delta', properties={}}`, and a remaining refcount
of two before the secondary read. Only the close helper and its tests changed;
83 other frozen files and the runtime SHA remained unchanged. The writer matrix
and corrected isolation qualification therefore have distinct recorded revisions.

Twelve harness unit cases pass, including cross-thread/wrong-key cleanup rejection,
accepted partial-body drain, unresolved upstream completion, persistent publication
blocking, current column-version selection and owned-object guards. Scoped Java
formatting/Javadoc and Python formatting/syntax checks passed.

Private original evidence is retained under
`/private/tmp/delta-packaged-runtime-task3-evidence/frozen-02`. The implementation
report records earlier failed attempts and diagnostic limits. Reproduction uses the
public CLI and private-input shape in the harness README.
