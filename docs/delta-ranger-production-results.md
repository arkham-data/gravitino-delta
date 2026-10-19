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

Task 4's planned live qualification gates passed on September 26–27, 2026 (UTC):
the healthy SELECT, warm Admin outage, public Gravitino grant/revoke, independent
role-publication, and cold/cached startup gates on all four Spark/Delta pairs with
Ranger 2.8.0. First-grant, additional-grant and final-revoke callback failures are
also qualified on Spark 3.5.4/Delta 3.3.0. The additional-grant gate includes the
reviewed cross-catalog plugin fix, 50 passing offline module tests and verified
runtime replacement. Final independent branch review passed SPEC and QUALITY;
the finite SELECT milestone is closed and was pushed at `84172dd45`, with an
exact remote-SHA check. Subsequent work continues locally. Full Delta support
is a separate, unfinished objective.

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
Test helpers are excluded from production artifacts. These bytes identify the
healthy, warm-outage, public-transition and role-publication results. The startup
gate uses the separately identified test application below with the same companion.

| Qualification | Spark 3.5.4 / Delta 3.3.0 | Spark 3.5.4 / Delta 3.3.2 | Spark 3.5.6 / Delta 3.3.0 | Spark 3.5.6 / Delta 3.3.2 |
| --- | --- | --- | --- | --- |
| Healthy fresh and retained SELECT | Passed | Passed | Passed | Passed |
| Warm Admin outage and reconnect | Passed | Passed | Passed | Passed |
| Public privilege and membership grant/revoke | Passed | Passed | Passed | Passed |
| Independent role-publication invalidation | Passed | Passed | Passed | Passed |
| New JVM, empty or genuine prior cache, Admin unavailable | Passed | Passed | Passed | Passed |

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

The startup gate uses test-application commit `c5cd68a204` and SHA-256
`b480ebb0e6b1838ea1add21a10b614dfe448cff0891002807bd2b46d024dc648`.
Each manifest retains the other 258 archives. Four new healthy default controls
passed eight queries with matching policy/role HTTP 200 downloads and natural
exits, then supplied genuine exited-driver cache snapshots. With the same Ranger
Admin stopped, all four empty-cache JVMs remained not ready for the 60-second
observation and executed no query. All four cached JVMs became ready at policy
64/role 86 and returned the exact masked/filtered rows `[["ALPHA"],[null]]`.
All eight exited naturally after cleanup. The 2,236 local gateway attempts had
zero upstream HTTP responses or HTTP 200s; transport failures retain unresolved
upstream-completion evidence. Cache success does not establish policy freshness
or revocation propagation during an outage.

The exact Admin was restored with all ten configuration hashes unchanged.
Authorization contents, roles and registrations remained unchanged across 33
readbacks; restart publication advanced resource policy to 65 and tag to 6, while
role stayed 86. All 12 S3 objects remained unchanged. Independent review reconciled
the original process, request, result, download and cache-provenance records. The
final probe fix suite passed 106 tests at Spark 3.5.4/Delta 3.3.0, followed by final
compilation, one boundary regression and formatting after an order-only method
move. Earlier 71-per-pair offline results predate those fixes; the four live default
controls and eight probes use the final exported application. A direct injected
full-probe cleanup-failure test remains a documented harness coverage limit.

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
- `root-startup-cache-acceptance-20260927.json`
- `root-policy-callback-failures-acceptance-20260927.json`

The callback gate uses isolated roles and fresh query JVMs. Two cases passed on
Spark 3.5.4/Delta 3.3.0 with Ranger 2.8.0: an injected pre-forwarding policy PUT 503
produced a completed public Gravitino 500 after local persistence. First grant
persisted local SELECT while Ranger retained no matching policy grant and created
an empty remote role; fresh SELECT remained denied. Final revoke removed local
SELECT while Ranger retained the policy and membership; fresh SELECT remained
allowed. Complete Admin policy models, owner items and preexisting roles stayed
unchanged. All four query JVMs downloaded policies and roles over completed HTTP
200 responses, matched saved response hashes and cache models, and exited naturally.
The coordinators also exited naturally and the normal callback recorder was restored.

The first case used coordinator SHA-256
`58365e640920ae6d620468d6d115be0715a57b8026fa253cfadf2a8bae3cc214`;
final revoke used
`19ef4d4d726f317821a465e513e043a7f30d17b40f13ebbc1782b9b8f549721e`.
Both used the accepted startup query application and unchanged production runtime.
Harness fixes at `a7d4af244` align resource keys with actual Ranger policies and
compare deserialized privileges by name and condition. Their final focused suite
passed 23 tests with original failing regressions and formatting evidence preserved.
An earlier additional-grant setup rejected DTO equality before fault arming; it
remains quarantined separately.

The subsequent additional-grant request persisted local A+B SELECT but returned
HTTP 400 with `Canonical Delta catalog/resource mismatch`. Catalog B's plugin
received the old role containing A and incorrectly validated that context against
B. No Ranger callback occurred; the armed 503 was unused. Fresh queries still
allowed A and denied B at policy 69/role 100. This is a reproduced production defect,
not a passed callback-failure case. The reviewed fix validates role identity and
every explicit changed resource, without treating the old role's foreign grants
as this callback's scope. Full lifecycle and membership validation remains intact;
Gravitino core is unchanged. All 50 offline module tests passed, including a real
REST regression spanning two catalogs. Docker and native-fixture gates were excluded
from that suite. The failed identities remain unreconciled and were not replayed.

The corrected server plugin is source commit `2004c4899`, with qualified JAR SHA-256
`36a905d1044551d70e3c17776d15f1d661d7376598449d2917dfa34eeeb1f52f`.
The isolated export changes only the tested plugin class and preserves all 51 other
archives. A controlled restart retained the configuration, parent classpath and
H2 state; the loaded-class origin and all 33 pre/post models verified the replacement.
Ranger Admin and the Spark artifacts were unchanged.

A new additional-grant case then passed with coordinator
`19ef4d4d726f317821a465e513e043a7f30d17b40f13ebbc1782b9b8f549721e`.
After healthy A SELECT and mirrored membership, the B policy PUT received exactly
one injected 503 before forwarding. The public grant completed with HTTP 500 and
local A+B SELECT persisted. All 19 Admin policies and 29 roles stayed unchanged.
Fresh pre/post drivers allowed A and denied B at policy 73/role 110, with matching
download hashes and complete cache models. Both drivers and the coordinator exited
naturally, and the normal recorder was restored.

Across the three accepted cases, six query JVMs executed eight SELECT commands.
First grant and final revoke retain their original server artifact qualification;
the additional case qualifies the server-only fix. This is not a claim that every
case was rerun on one server binary or all four Spark/Delta pairs. Final readback
preserved all original owner/foreign policy contributions and 19 original roles,
the five quarantined case identities, and all 12 original S3 objects. Ten isolated
case roles and four healthy setup SELECT contributions remain deliberately present;
the final observed generations are policy 73, tag 6 and role 110.

Private independent reviews are `first-grant-live-review.md`,
`last-revoke-live-review.md` and `additional-grant-fixed-live-review.md` beneath the
callback task directory. The final aggregate review, `milestone-final-review.md`,
passed SPEC and QUALITY using these results and the accepted historical source
reviews. Ordinary grant reconciliation remains a separate rollout requirement.
Publication uses a normal push and exact remote-SHA verification; subsequent
full-Delta work continues locally.

## Historical local write-policy checkpoint

Current continuation (October 18, 2026): the [bounded named append milestone](delta-native-governed-append.md)
now qualifies the two named batch frontends and public MODIFY propagation on the
four pinned local packaged profiles. The checkpoint below retains its original
commit, tests and then-open gates; it is not the current append support statement.

After the SELECT milestone, local source commit `651e669fa` added the narrow
TABLE/MODIFY_TABLE/ALLOW → Ranger `update` mapping. Mixed SELECT/MODIFY grants
retain distinct accesses; tests cover selective changes, owners, foreign policies,
cross-catalog context and rejection before Admin activity. All 53 offline module
tests and scoped formatting passed; independent SPEC/QUALITY review passed.
Docker/native fixture and actual published-table MODIFY callbacks remain separate
gates. The tested plugin has not replaced the live SELECT runtime.

The [governed append design](superpowers/specs/2026-09-27-delta-governed-append-design.md)
preserves source SELECT masks/filters and target UPDATE access. The
[native append characterization](delta-native-append-results.md) passed all four
Spark/Delta pairs on October 5: 14 tests per pair, with independent review and
artifact verification. Command enforcement and real Ranger 2.8.0/S3 append
qualification remain subsequent work. This mapping does not
yet admit or qualify Spark writes. Full Delta and ordinary reconciliation remain
open.

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
