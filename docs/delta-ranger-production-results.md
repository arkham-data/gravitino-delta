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

# Delta Ranger production-chain qualification

Task 4 remains in progress. Qualification on September 26–27, 2026 (UTC), passed
the healthy SELECT, warm Admin outage, public Gravitino grant/revoke, and independent
role-publication gates on all four Spark/Delta pairs with Ranger 2.8.0. Cold/cache
startup, ordinary policy callback failures after persistence, and final branch
review remain open. The milestone checkpoint has not been pushed.

These results qualify the finite named SELECT surface with user AQE explicitly
disabled. They do not establish full Delta support, general user caching/AQE, or
deployment readiness. Historical checkpoints below remain evidence for their
original artifacts; the current checkpoint supersedes their pending-query status.

## Current packaged SELECT checkpoint

The accepted production guard is commit `55d8b746a`; the retained-query test
application is commit `82343db02`. Each of the four runtime manifests contains
259 verified archives with one base Delta runtime, one production Ranger companion,
and a separate test application. The companion SHA-256 is
`caed200d88440a0a0b45f6090d01513812bc5dcbdfdef7bb4a05d4233b36c856`;
the application SHA-256 is
`740907f388b06845203abeda10eda72c439799caeb6d5575ec049b61ad2bcb21`.
Test helpers are excluded from production artifacts. These bytes, rather than
unreviewed worktree changes, identify the following live results.

| Qualification | Spark 3.5.4 / Delta 3.3.0 | Spark 3.5.4 / Delta 3.3.2 | Spark 3.5.6 / Delta 3.3.0 | Spark 3.5.6 / Delta 3.3.2 |
| --- | --- | --- | --- | --- |
| Healthy fresh and retained SELECT | Passed | Passed | Passed | Passed |
| Warm Admin outage and reconnect | Passed | Passed | Passed | Passed |
| Public privilege and membership grant/revoke | Passed | Passed | Passed | Passed |
| Independent role-publication invalidation | Passed | Passed | Passed | Passed |
| New JVM, empty or genuine prior cache, Admin unavailable | Pending | Pending | Pending | Pending |

All accepted runs used JDK 17, Scala 2.12, real Ranger 2.8.0 downloads, native Delta
on the local S3-compatible fixture, and separate query and metadata subjects. The
healthy matrix executed 72 query actions: 40 fresh and 32 retained actions. It
covered Ranger column access, masks and row filters, cross-catalog queries, and
the declared negative cases. All four drivers exited naturally with zero status.
The production guard's preceding offline matrix passed 335 tests per pair; the
retained harness's focused matrix passed 23 per pair. These distinct test scopes
are not a claim that every Delta operation or Spark action has been qualified.

The warm-outage matrix qualified 20 query actions. Already loaded policies allowed
the expected masked/filtered rows while Admin was stopped and download attempts
failed. After the same Admin restarted, an observed policy publication change
invalidated each held Dataset at collection entry before any user scan; fresh
reanalysis returned the expected rows. One early first-pair action was dispatched
before stop verification and remains excluded from outage evidence. This result
does not promise immediate revocation while Admin is unavailable or prove a new
JVM can recover from cache.

Six public Gravitino membership/SELECT grant, revoke and repetition requests drove
44 query actions, including eight stale-plan entry denials. Independent local,
Admin and downloaded readbacks proved the intended policy and membership changes
while preserving other managed items and foreign policies. Fresh queries changed
between allowed and denied as expected. Repeating a converged SELECT grant produced
no callbacks; repeating an absent membership revoke advanced publications without
changing access semantics. That is observed idempotence, not recovery from a failed
callback. All four drivers exited naturally with zero status.

The independent role-publication gate executed 12 query actions. Creating one
empty, unreferenced test role advanced only the role generation. Each held Dataset
and physical root then failed at the collection guard with no jobs or named scan
rows; each fresh reanalysis returned the unchanged masked/filtered rows. All four
selected completed role-download body hashes exactly matched the independently
saved role response. This proves publication invalidation with unchanged access
semantics, not an effective role-only revoke. The owned role was deleted once after
all four drivers exited naturally. The original 19 roles and 33 authorization and
metadata models were restored; the final observed generations were policy 64,
tag 5 and role 86. Generations are observations, not constants for future runs.

Every accepted phase verified the original 12 S3 objects against their recorded
keys, bytes, hashes and explicit restored modification-time baseline. Service
recovery used a cloned database volume and preserved the original volume. Failed
setup attempts, the initial default-AQE rejection, the corrected RELEASE command
encoding failure, and the excluded outage action remain separate evidence. None
was converted into a successful query or replayed into its original evidence path.

The ignored private evidence directory named below contains the original controls,
HTTP records, query results, exit records, model readbacks and independent reviews.
The acceptance entry points are:

- `root-current-retained-healthy-verification-20260926.json`
- `root-warm-outage-matrix-acceptance-20260926.json`
- `root-effective-transitions-acceptance-20260927.json`
- `root-role-only-acceptance-20260927.json`

The next gates use fresh test applications and isolated runs. Startup observations
must distinguish a completed not-ready observation from successful cached startup.
Callback-failure cases must retain local/Admin/downloaded divergence and completed
HTTP evidence; identical grant/revoke replay and same-owner PUT are not ordinary
grant reconciliation. After these gates and final review close the milestone, the
accepted branch checkpoint will be pushed normally and its remote SHA verified;
subsequent full-Delta work continues locally.

## Initial verified packaging checkpoint

| Actual archived runtime | Packaging | Pinned helper probe | Public callbacks / native SELECT |
| --- | --- | --- | --- |
| Spark 3.5.4 / Delta 3.3.0 | Passed | Passed | Pending |
| Spark 3.5.4 / Delta 3.3.2 | Passed | Passed | Pending |
| Spark 3.5.6 / Delta 3.3.0 | Passed | Passed | Pending |
| Spark 3.5.6 / Delta 3.3.2 | Passed | Passed | Pending |

Each final runtime manifest contains the actual distribution JARs, matching native
Delta JARs, the previously qualified S3A dependency pair, archived base and Ranger
companion JARs, and a separate test application. The companion has no duplicate
class origins against the actual distribution/base runtime and contains no
Spark/Scala/Hadoop/Delta platform classes, Gravitino client/protocol classes,
Kyuubi utility copies, or project fixture applications. The base remains free of
Ranger/authz classes; its inherited distribution overlaps are recorded separately.

The companion resolves all six Ranger Maven artifacts to 2.8.0 and retains their
provenance metadata. It uses plain Kyuubi authz 1.10.2 while the base alone supplies
Kyuubi Hive 1.11.0 utilities and private Guava/publicsuffix classes. The executed
helper probes cover the accepted version/quoting, reflection, service-loader,
Ranger configuration, and private Guava/publicsuffix families on each archived
pair. They do not execute the Spark query driver or establish policy downloads.

Initial packaging attempts exposed Jackson Java 21 multi-release bytecode beyond
the pinned shading tool and duplicate Jersey/Hostname/Servlet/annotation inputs.
Original failing builds and pre-correction archives are retained. Final packaging
omits Java 21-only entries for the qualified Java 17 launcher, uses Kyuubi's
existing non-JNA Hostname implementation, and keeps distribution-provided APIs out
of the companion. Three original Ranger `ugsync-util` helpers and their JAAS name
strings move to a private package. This is not Kerberos qualification.

## Initial public-chain checkpoint (historical)

The production server bundle has 145 parent runtime JARs and separate archived
provider/authorization packages, with no project fixtures or parent Ranger
classes. Root provisioned a distinct Ranger service/definition/tag service and
launched the production Gravitino main from those archives. Readiness is setup
evidence, not callback acceptance. The worker has not issued public metadata or
Ranger model mutations at this checkpoint.

A separate archived public-client application is ready for one-shot bootstrap and
the truly fresh principal callback. It records both public Gravitino and downstream
Ranger HTTP attempt/completion events, forwards normal response status/body
unchanged, and reads Admin state independently without replay after failure.
Its gateway tests first failed both assertions, then passed both, including actual
HTTP 204 preservation and a single method/path-scoped HTTP 503 fault. The separate
Spark query application checks actual UGI/application identity, metadata separation,
original runtime provenance, downloaded versions, authorization-denial causes, and
executed rows; its live query mode remains unexecuted.

The known Ranger external-user HTTP 204/expected-200 incompatibility has not yet
been reproduced through this Java public callback. No optional provider correction
is claimed. Global role-delete/audit cleanup, empty-role bootstrap ordering,
ordinary failure-after-persistence divergence, catalog-only visibility and its
revoke, Ranger-only column metadata access, grants/membership refresh, foreign
policy preservation, masks/filters/joins, and actual downloader/cache/outage
composition all remain required before Task 4 acceptance.

Evidence is in the ignored persistent worktree directory
`.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/`.
Original commands/exits, focused XML, post-dependency execution inputs, source
snapshots, complete archived classpaths, dependency/class hashes, and helper
results remain distinct. One early formatting-only gateway attempt copied stale
Task 3 XML; it is explicitly labeled non-evidence and the runner was corrected.
An earlier source snapshot also included generated runtime build files; final
source binding must use the separately identified source paths. The initial artifact
verifier also assumed an input manifest existed for an up-to-date Shadow task;
the corrected verifier checks byte-identical companion reuse against its original
producer manifest. These limitations remain separate from the original gateway
RED/GREEN records and actual runtime manifests.

This milestone is not full Delta support or a security/demo integration completion.
Ordinary role/grant reconciliation, broader authentication, remaining masks and
expressions, DML/DDL, streaming, time travel, maintenance, cached/reused plans,
metadata confidentiality, supported launcher restrictions, and isolated catalog-demo
integration remain subsequent gates. No demo source, core/API/server/client source,
or native storage objects were changed by this checkpoint.

## Live callback serialization gate

The first public catalog request stopped before Ranger because the explicit-config
`GRAVITINO_TEST` layout also requires
`catalogs/catalog-lakehouse-delta/build/resources/main` to point to archived catalog
configuration. After correcting that private deployment path and independently
confirming catalog absence, a catalog-only resume made 13 completed Ranger calls.
Role creation succeeded, but the subsequent role update failed: Ranger 2.8's client
serialized fetched timestamps as `yyyyMMdd-HH:mm:ss.SSS-Z`, which its Admin rejected.
The public request completed with 500 and catalog creation rolled back; the empty
Ranger role remains an explicitly retained failed-attempt artifact.

The optional Delta client now sends detached role/policy request maps with the
original numeric epoch-millisecond timestamps. Five write overloads preserve all
model fields and typed responses without mutating shared serialization settings
or caller models. Focused wire tests observed the original rejection, then passed
with complete field/caller preservation; resource-empty policies are rejected
before sending a request. After a controlled provider restart, all three public catalog/schema/adopt/publish
sequences completed successfully. A genuine fresh-user membership then reproduced
the next protocol issue: the one external-user POST completed with 204, while the
public grant completed with 500 after local membership persisted. Independent
readback found the user but no matching Ranger membership. That failed role remains
untouched. The optional client now validates actual principal statuses, requires one
exact user readback after 204, and closes acquired responses. A distinct fresh
principal subsequently passed the public callback and exact Ranger membership checks.

Focused wire tests also reproduced false success on 503 for nine callback write
methods, including policy deletion and role membership. Those optional client paths
now validate each pinned API's expected status before decoding; the four model read
paths already fail in Ranger's GET transport and remain unchanged. Thirteen focused
status, principal, and timestamp tests passed before the controlled deployment.
These wire regressions do not replace the remaining live failure-after-persistence
and downloaded native-query gates.

The public matrix bootstrap created visibility-only roles and memberships for the
three query subjects. Such membership callbacks may mirror Ranger identities and
roles; independent policy inspection verified that the column reader's visibility
role had zero data-policy references. The separate metadata reader received its own
scoped table role. Ranger-owned column/mask/filter fixtures were then installed.

The first actual Spark 3.5.6 / Delta 3.3.2 driver verified its archived origins,
separate query identity, application-visible catalogs, and genuine Basic downloader
policy/role versions. Its first SELECT failed before table analysis because the
session extension configuration contained only the native Delta extension. The
original run and a diagnostic run are retained as setup/execution failures, not
authorization denials. Independent table-read checks also found that the metadata
reader needs schema-use permission in addition to catalog-use and table-select.
After admitting exact SCHEMA/USE_SCHEMA/ALLOW visibility with zero Ranger data
access, a separate public metadata-schema role made all three metadata reads
succeed while nine query-user metadata reads remained forbidden. Three actual
Spark 3.5.6 / Delta 3.3.2 queries then passed: masked/filtered projection, predicate,
and star on delta_a. The next unfiltered delta_b scan failed on an internal native
SnapshotState Aggregate; this was an admission failure, not a policy denial.

Commit `e1347911c` admits only the registered native SnapshotState lineage and its
exact aggregate, optimizer, limit and encoder-binding shapes. Complete adapter
suites on Spark 3.5.4 with each Delta version passed 71 tests and Javadoc, with no
failures or skips; independent review approved the change. Live reruns with the
new companion remain pending.

Offline September 21 verification also ran the two new native snapshot tests and
the existing installed filtered-scan regression on Spark 3.5.6 with each Delta
version: three tests per pair passed, no failures/errors/skips. Four new immutable
runtime manifests bind the approved native source and test harness to fresh
companion/application archives while reusing verified original Spark, native Delta,
base-runtime and S3A archives. No live readiness, downloads or query qualification
follows from these offline tests or artifact checks.

Historical independent role-lifecycle checks also passed audit cleanup after an
actual public role deletion. A separate deletion blocked by a foreign global role
reference completed with public 500 / Ranger 400: the local role and its original
grant remained, but its managed Ranger policy contribution had already been
removed. The foreign reference and all nine original foreign policies were
preserved. This divergence, the original failed-principal membership, and the
orphan catalog-owner role remain explicit retained state, never replayed as repair.

The offline query harness now requires a policy engine authorize-origin denial,
not merely any SecurityException, and retains private cause messages and stack
frames. Its per-driver recorder uses a new empty private cache and the pinned
Ranger secure policy and role download paths, requiring completed GET HTTP 200
before accepting the observed engine generations. Offline doubles qualify this
harness logic; they do not establish live downloads or complete the four-pair
matrix.
