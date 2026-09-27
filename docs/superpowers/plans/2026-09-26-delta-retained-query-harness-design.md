# Retained downloaded-policy query harness design

## Purpose and boundary

Add a test-only command protocol to the archived `DeltaRangerDownloadedQuery`
application so one actual named `Dataset<Row>` and its actual `SparkPlan` root can
survive a public Gravitino policy/role transition and be collected under newly
downloaded Ranger state. This prepares a current-artifact live acceptance gate;
this design does not perform that gate. Existing fresh `spark.sql(...).collectAsList()`
commands, old evidence, public coordinator and production code remain unchanged.

The supported surface is named SELECT `collect` and `collectAsList`, including
masked/filtered rows, before-first and after-success stale denial, and unchanged
repeat. User cache, AQE, raw RDD, arbitrary actions and full Delta remain outside
this protocol. The driver must preserve the actual Java object references in its
own finite state; a numeric identity hash, plan rendering or serialized token is
diagnostic only, never proof of retained authority. No production global registry
or test hook is introduced.

## Source and evidence anchors

`spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerDownloadedQuery.java:135-186`
currently refreshes and waits, then constructs fresh SQL in every command and
writes `<id>.json` plus `<id>-passed.json`. Its `isSecurityFailure` at `:304`
recognizes only a `DeltaRangerPolicyEngine.authorize` denial. The current test
`TestDeltaRangerDownloadedQuery.java` checks this exact distinction and private
0600 output. `DeltaRangerDownloadedSecurity.java` already records actual completed
Ranger HTTP attempts and the initial policy/role pair, but does not bind a response
generation to the applied engine generation. The test-only
`DeltaRangerCallbackGateway.java:199-240` sees the upstream bytes before forwarding;
its ordinary event records status/completion only. Archived actual Ranger secure
policy/role JSON has `serviceName` and integral `policyVersion` or `roleVersion`.
The installed offline native
fixture at `TestDeltaRangerPhysicalAdmission.scala:491,598` proves same-Dataset
positive collection and stale entry denial, using `NativeSelectFixture.withBoundSpark`
from `TestDeltaRangerReadRule.scala:261`. The accepted September 21 Python
same-JVM runner/verifier freeze a three-phase, four-fresh-query shape and a
259-entry archive manifest; they are not edited or reused as new evidence.

## Protocol

The exact `{ "close": true }` line remains the shutdown command. Parse it with
duplicate-property and trailing-token checks, reject any extra or mistyped field,
stop without an action result, and clear all slots in `finally` before
Spark/recorder close. An absent `operation` on any other command retains the
existing fresh-query JSON contract and result semantics: `id`, `sql`,
`minPolicy`, `minRole` plus exactly one of `expectedRows`,
`denied:true`, or `hidden:true` with `catalog`. New commands require exact keys,
one JSON object per input line, and a
bounded safe `id` and `retentionId` matching `[A-Za-z0-9_-]{1,64}`. Reject reserved
legacy result basenames `ready`, `failure`, `completed`, names ending `-passed`,
duplicate command IDs, duplicate or reused retention IDs, unknown properties,
nulls, missing fields and unsupported action/outcome values before side effects.
The new protocol is:

```json
{"id":"p1","operation":"prepare","retentionId":"named-a","sql":"SELECT secret FROM a.db.t WHERE id = 1","minPolicy":56,"minRole":81}
{"id":"c1","operation":"collect","retentionId":"named-a","action":"collect","expectedRows":[["a-one-masked"]],"minPolicy":56,"minRole":81,"requiredDownloads":"none"}
{"id":"c2","operation":"collect","retentionId":"named-a","action":"collectAsList","deniedAtEntry":true,"expectedFailureContains":"denied","minPolicy":57,"minRole":82,"requiredDownloads":"both"}
{"id":"r1","operation":"release","retentionId":"named-a"}
```

`prepare` creates and stores exactly one Dataset and obtains its executed root,
without a user collection. A finite active limit of four retained IDs prevents
unbounded driver heap growth. `collect` uses that stored Dataset for either action;
it compares `dataset == prepared.dataset` and
`dataset.queryExecution().executedPlan() == prepared.root` before and after the
action, recording both booleans and failing if either becomes false. It never calls
`spark.sql` or replaces the root. It accepts exactly one outcome declaration:
`expectedRows` or `deniedAtEntry:true` with a nonempty expected failure fragment.
An entry denial must have `SecurityException` in the cause chain, an
`DeltaRangerCollectGuardExec.executeCollect` stack frame and no guard `doExecute`
frame; check the expected fragment against the failure while retaining the complete
private cause chain. Keep fresh-query `authorizationDenied` classification as-is.
The actual returned rows are compared with the supplied typed expected rows under
the existing row normalization, and the raw rows remain in private evidence.

The driver and real native fixture call the **same** package-private test-only
executor:
`DeltaRangerRetainedAction.execute(SparkSession spark,
DeltaRangerRetainedQueries.Slot slot,
DeltaRangerDownloadedCommand.Parsed command): Result`.
It accepts only `COLLECT`, invokes the held Dataset action, checks Java `==`
Dataset/root identity before and after, compares exact rows or precise
collection-entry denial, and records job IDs and named user-scan metrics. It never
builds SQL, refreshes Ranger, reads downloads or updates a slot cursor. `Result`
is immutable with `passed(): boolean`, `evidence(): Map<String,Object>` and
`failure(): @Nullable Throwable`. The evidence map has exactly `retentionId`,
`action`, `rows`, `failureDetails`, `actionEntryDenied`, `sameDatasetBefore`,
`sameRootBefore`, `sameDatasetAfter`, `sameRootAfter`, `jobGroup`, `jobIdsBefore`,
`jobIdsAfter`, `userScanOutputRows`, `passed`. `userScanOutputRows` is a nonempty
list of nonnegative metrics from named scans in the held physical root; a missing
named scan cannot pass this finite SELECT fixture. The list is not a claim about
native metadata jobs. Expected rows or expected entry denial with both reference
checks true yields `passed:true`; unexpected failures yield a complete private
diagnostic `Result` with `passed:false`. Driver writes that result before a pass
marker or throwing failure. Native fixture uses parsed collect commands to call
this executor for unchanged, before-first and after-success cases, including
role-publication drift; direct `frame.collect()` is insufficient fixture coverage.

Every prepare/collect command refreshes the pinned engine, awaits its requested
floors and records service/definition/policy/role before and after. Release only
clears local retained state. Construct
`new DeltaRangerRetainedQueries(SparkSession)` once per driver. Its
`prepare(String retentionId, String sql, long completedCursor,
DeltaRangerPolicyEngine.Versions applied): Slot` stores exact Dataset/root,
cursor and applied versions; `require(String): Slot`, `release(String): void` and
`close(): void` never recreate a released slot. `Slot` exposes `dataset():
Dataset<Row>`, `root(): SparkPlan`, `cursor(): long`, `versions(): Versions` and
`advance(long cursor, Versions applied): void`. For either an expected exact row
result or an expected collection-entry denial, write the immutable result and pass
marker, then advance before reading another command. An unexpected action or
evidence-write failure never advances; it ends the driver without a **complete**
pass marker and clears all slots in `finally`. `advance` is a bounded in-memory
assignment on already validated values and cannot perform I/O or throw.

Add a downloader-only constructor
`DeltaRangerCallbackGateway(URI upstream, Path evidence, String downloadService)`.
For an exact secure policy/role GET path on that bound service with upstream HTTP
200, inspect the forwarded response bytes in memory using duplicate-key and
trailing-token checks. Require a JSON object, exact string `serviceName` equal to
the bound service, and a nonnegative integral 64-bit `policyVersion` or
`roleVersion` field. In the completed event record only `downloadKind`,
`downloadService`, `downloadVersion` and `responseSha256`, never body, headers,
credentials or policy models. Forward original bytes and status unchanged.
Malformed, missing, fractional, out-of-range or wrong-service metadata is marked
invalid with a sanitized reason and cannot qualify. HTTP 304 and incomplete
upstream/downstream responses cannot qualify. Existing unscoped coordinator
constructors keep their behavior; the download gateway must not enable its
existing `retainResponseBodies` option.

`DeltaRangerDownloadedSecurity.completedCursor(): long` returns the highest
completed gateway attempt after drain; its existing `recordInitialDownloads` also
requires the two decoded initial response generations to equal the engine versions
passed to it, while preserving its current JSON result schema. `completedAfter(long exclusiveCursor,
DownloadRequirement requirement, DeltaRangerPolicyEngine.Versions applied):
DownloadEvidence` returns immutable `Optional<CompletedDownload> policy()`,
`Optional<CompletedDownload> role()` and `long cursor()`. A
`CompletedDownload` has `long attempt()`, `long version()` and
`String responseSha256()`. For each required kind, choose the latest completed
post-cursor HTTP 200 secure response; its exact path, decoded service and decoded
version must equal the requested **applied engine** generation. A later malformed
or wrong-generation 200 cannot be bypassed by cherry-picking an earlier matching
response. A wrong-generation HTTP 200 is insufficient even if engine numbers
separately advanced. Every collection declares one requirement:
`none`, `policy`, `role`, or `both`. `policy` requires an advanced policy generation
and a matching policy download; `role` requires an advanced role generation and
a matching role download; `both` requires both. Another generation may advance in
a `policy` or `role` phase; then its matching download is also required before
claiming that portion of the transition. The requirement names minimum evidence,
not an assumption that Admin increments only one number. `none` requires both
generations equal the slot baseline and reports **no refresh qualification**, even
if background requests occurred. For every successful COLLECT, `cursor()` is the
highest completed attempt observed after drain, including for `none`; advancing
the slot past incidental requests prevents a later action from reusing them as
fresh transition evidence. Same-number publication replacement remains
outside numeric download qualification. Independent live verification must match
chosen digest, attempt, version and time to its public phase attestation.

The executor records actual action/job group, job IDs before/after and named user
`FileSourceScanExec` `numOutputRows` metrics. On denied entry require zero
user-scan output metrics after Spark's action reset; job IDs are observations,
not standalone proof of user execution because native metadata can run. Preserve
complete private failure frames.

`release` removes the slot and drops both strong references; it rejects a missing
or already released ID. Driver shutdown clears all slots in `finally` even after
malformed input or failed collection, before Spark/recorder close. A completed
command produces one immutable 0600 JSON result and one 0600 pass marker under a
new 0700 `retained-events/` directory using a driver-incremented six-digit event
number, never an input-derived path. Reject a 4097th retained command before
writing; this keeps the private evidence set finite. Include command ID and retained ID inside
both records. The private top-level `failure.json` remains the terminal error
channel. A result file is written before the pass marker; a missing marker is a
failed/incomplete command, not acceptance. Preserve old fresh command names but
validate their ID-to-filename expansion for reserved names and collisions.

Each new result has `schemaVersion:1`, numeric `sequence` matching its six-digit filename, exact input
`id`, `operation` and `retentionId`. PREPARE adds `sql`, `versionsBefore`,
`versionsAfter`, `completedCursor` and `prepared:true`; RELEASE adds
`released:true`. COLLECT adds `versionsBefore`, `versionsAfter`, `downloads`,
`refreshQualified` and `actionResult` (the executor's exact evidence map).
`downloads` contains `requested`, `effective`, `cursorBefore`, `cursorAfter`,
`policy` and `role`, each download entry either null or exactly `attempt`,
`version`, `responseSha256`. The version maps use only `service`, `definition`,
`policy`, `role`. `refreshQualified` is false for `none`; for other values it is
true only after matched decoded downloads and applied generations. The pass
marker has exactly `schemaVersion`, `sequence`, `id`, `retentionId`, `passed:true`; it is written
only after a qualifying result. Preserve existing `ready.json`, `completed.json`,
`failure.json` and fresh `<id>` filenames. The later live verifier must hash the
original result and pass marker, reject missing/incomplete/mismatched fields,
and match response digests/versions/times to its public transition proof.

## Offline acceptance for this task

Unit tests exercise exact JSON/close parsing, path/collision safety, active and
released slot state, bounded capacity, reference identity, gateway response
service/version/digest parsing, wrong-generation and malformed download rejection,
entry-denial classification, failure cleanup and immutable evidence permissions.
A real local Spark/Delta fixture test uses installed extension and actual named
native table. It prepares one Dataset, calls the shared action executor for
unchanged `collect` and `collectAsList`, changes the request-sensitive test policy,
then calls that executor for before-first and after-success denial on the same
stored Dataset/root. This
fixture uses no external Ranger/Gravitino/MinIO endpoint and is labeled a native
offline test, never a downloaded-policy acceptance. Run the focused tests on all
four Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 pins with Ranger 2.8.0, Scala 2.12,
JDK17, format/Javadoc, and inspect the companion JAR to ensure test classes are
absent. Root owns archived artifacts, later live coordination and acceptance.
