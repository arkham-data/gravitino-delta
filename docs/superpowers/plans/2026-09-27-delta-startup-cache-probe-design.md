# Delta startup/cache probe design

This is a bounded elaboration of approved Task 4 in
`2026-09-08-delta-ranger-production-runtime.md`, informed by
`.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-startup-outage-seams.md`.
It adds private test application observations, not a new production startup or
cache contract. Root reviews the plan before the already authorized continuous
subagent execution. The author owns offline source/test proof; root owns Git,
artifact exports, credentials, service lifecycle, live inputs and acceptance.

Root's current checkpoint is
`rr/post-effective-transitions-baseline-20260927.json`: observed policy 64/tag 5/
role 84, four query drivers naturally closed, twelve S3 objects unchanged. These
are operational context, never hardcoded test generations or proof of the next
startup. Read-only cache inventory found a 33,438-byte policy leaf and 6,079-byte
roles leaf in an exited driver's private cache; root must still verify their
contents/bindings and provenance before qualification.

## Preserved boundaries

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12, JDK 17.
- Existing S3 native authority, Gravitino registration and separate query/metadata
  subjects remain binding. Explicit user AQE remains false for these cells.
- No production hooks, core/API/server/client/storage-schema changes, dependency
  changes, shared cache, mutation commands, synthetic downloader publications or
  expectation that a cached restart must succeed.
- Ordinary `DeltaRangerDownloadedQuery` startup still creates an empty cache,
  requires matching completed policy and role HTTP 200 responses, checks fixture
  floors, writes `initial-downloads.json` and `ready.json`, then reads commands.
- Existing healthy, warm-outage and effective-revocation evidence remains evidence
  for its original unchanged artifacts. A new test application requires a bounded
  healthy default control on every pin. Do not repeat the broad 1,340-test matrix
  or live mutation matrices without a source change or failure justifying it.
- The production companion excludes every test helper. It should remain identical
  in content; root independently checks its archive/hash before reuse.

## Why this seam exists

`DeltaRangerDownloadedSecurity` rejects nonempty target security directories and
creates `target/cache`. `DeltaRangerDownloadedQuery.main` then acquires the engine,
awaits version floors and calls `recordInitialDownloads` before command dispatch.
Those are correct default controls but cannot observe a cache-only restart.

`DeltaRangerPolicyEngine.requireReady()` calls `versions()`. The latter checks the
actual completed policy publication, matching role publication/context/mapping,
service, metalake, definition and active generations. Cached policy bytes alone
prove none of those facts. The final lease makes the production owner terminal;
each restart cell requires a genuinely new JVM. The probe does not reset that
owner or call a testing engine constructor in its deployed path.

## One explicit request branch

An absent `startupProbe` property selects the existing default path. A present
property must be a strict object; null, false, unknown keys, duplicate JSON keys,
invalid types and trailing JSON are errors. Strictly parse the input file before
any security preparation or Spark/plugin initialization.

The only accepted probe shapes are:

```json
{"mode":"empty","query":{"catalog":"a","schema":"db","table":"t","columns":["secret"],"expectedRows":[["a-one-masked"],[null]]}}
```

```json
{"mode":"snapshot","manifestSha256":"64 lowercase hexadecimal characters","query":{"catalog":"a","schema":"db","table":"t","columns":["secret"],"expectedRows":[["a-one-masked"],[null]]}}
```

The digest string above describes the field's required format; root supplies an
actual digest of the sealed manifest, never this explanatory value. There is no
expected-ready flag, expected-denial flag, alternate Admin URL, custom timeout,
cache path, refresh flag or stdin command stream in this mode. Read top-level
service/definition/metalake, subject, catalogs and security/profile paths from the
same trusted private launch request used by the ordinary application. Continue
recording actual subject/groups, origins, helper probes and application identity.

The query grammar is intentionally finite: each catalog/schema/table/column is a
nonempty `[A-Za-z_][A-Za-z0-9_]{0,127}` identifier; 1–16 distinct columns; 0–256
expected rows of exactly that width with JSON scalar/null cells. Construct the
quoted statement:

```sql
SELECT `secret` FROM `a`.`db`.`t`
```

Reject dots, slashes, wildcard columns, expressions and arbitrary
SQL. The selected catalog must occur in the trusted top-level catalogs list.
Compare rows as a JSON multiset, preserving duplicate counts and nulls; ordering
is not an acceptance condition. Root chooses a known positive masked/filtered
SELECT and derives expected rows independently before stopping Admin.

## Private cache provenance

Snapshot input has exactly one location: the `startup-cache-input` sibling of the
request file. No input field can name another source. The directory is a
per-consumer copy of a sealed, genuinely downloaded earlier driver's cache,
captured only after that driver exits naturally and its handlers are drained.
Root prepares it; the application never snapshots a live/shared cache.

Layout:

```text
private-run/
  request.json
  driver-security/                 # empty, owner-only before application start
  startup-cache-input/             # snapshot mode only, 0700
    manifest.json                  # 0600; hash supplied in request
    source-provenance.json          # 0600; hashes/IDs only, no credentials
    ready.json                     # exact prior successful default startup
    initial-downloads.json          # exact prior matching policy and role 200
    downloads.jsonl                # exact prior completed downloader events
    completed.json                 # exact prior application completion
    cache/                         # 0700; original leaf filenames, files 0600
  evidence/                        # newly created by application, 0700
```

`manifest.json` is a strict object with `schemaVersion:1`, `service`, `definition`,
`metalake`, `spark`, `delta`, `ranger:"2.8.0"`, `scala:"2.12"`, `java:17`,
`sourceRunId`, `sourcePid`, `policyVersion`, `roleVersion`, and `files`.
`files` is a list of strict `{name,size,sha256}` entries covering exactly the five
evidence files above and every cache leaf; it excludes the manifest itself.
`source-provenance.json` is one of those five files and has `schemaVersion:1`,
`sourceRunId`, `sourcePid`, `exitCode:0`, `requestSha256`, `profileSha256`,
`applicationSha256`, `companionSha256`, `fixtureSha256`, `archiveManifestSha256`,
`sourceSecuritySha256`, `captureStarted`, `captureCompleted`, and `exitRecordSha256`.
Root's durable capture record separately retains the exact source paths and all
referenced hash-bound originals, including the source exit record. A bare
self-authored manifest cannot establish a genuine prior download: root must
verify this chain against the archived source run before admitting the input.
The original Ranger writer can create 0644 leaves inside its 0700 private cache;
preserve and record that fact. Root copies unchanged bytes into this new 0700/
0600 input bundle instead of chmodding historical evidence or claiming its leaf
modes were already 0600. The bundle is logically sealed by hashes and exclusive
root ownership, not a claim that same-user writes are impossible.

Reject any missing/extra file, duplicate name, bad digest/size, negative version,
unsupported pin, binding mismatch, wrong fixed evidence shape, missing matching
completed policy/role download or disagreement with `ready.json` versions.
Evidence names are the exact five fixed basenames; cache names must match
`cache/[A-Za-z0-9_-][A-Za-z0-9_.-]{0,191}` with no `..` component or subdirectory.
One to sixteen cache files, each at most 16 MiB, aggregate at most 64 MiB; manifest
at most 64 KiB and each provenance/evidence file at most 16 MiB. These are test
fixture bounds, not production cache limits. The source gateway digest identifies
downloaded response bytes; it need not equal Ranger's serialized cache-file
digest. Record both and never equate them.
Application validation compares service/definition/metalake with the launch
request and checks the allowed pin values; root independently binds the manifest's
Spark/Delta pair to the exact launch distribution/JAR hashes. After Spark starts,
also compare its reported version with the snapshot manifest. Do not invent a
Delta-version method or trust a JAR filename as version proof.

Require canonical absolute paths with no symlink in any existing path component;
root uses `/private/...` rather than the `/tmp` alias. Require same effective
owner as the new run directory, exact owner-only modes under that run, regular
files with link count one, and no special files. Only inspect parents outside the
private run for symlinks/type; do not require system ancestors to be private.
Reject a nonempty or symlinked target rather than cleaning it. No recursive
traversal, tar extraction, hard links or arbitrary-directory copying.

Verify the manifest's exact bytes against the request digest, then verify bounded
source bytes with `NOFOLLOW_LINKS` before creating destination files with
`CREATE_NEW`, 0600. Copy only declared cache leaves into newly created 0700
`driver-security/cache`; rehash the bytes actually copied and recheck source
attributes before/after reading. Root owns the private namespace throughout;
this does not promise resistance to a malicious process of the same OS user.
Capture the validated raw manifest and provenance in private evidence plus the
sorted source/destination size/digest inventory. In empty mode the bundle must
be absent and the cache inventory must be empty. Do all this before Spark session
creation, which may itself acquire the plugin, and before explicit engine acquire.

`security-manifest.json` records `freshEmptyCache:false` for a seeded probe and
`true` for empty/default, plus mode and cache manifest/inventory when present.
The seeded path must not emit `initial-downloads.json` or claim a fresh download.
Input hashes and cache-before/cache-after inventories survive a failed startup.

## Observation and cleanup

1. Record the request/profile/security/artifact bindings, PID and start time before
   initialization. Root's launch record binds original bytes, command, environment
   allowlist, pin, archive manifest and hard process deadline. Do not print secrets.
2. Prepare private security/cache; build Spark using the same profile and identity
   checks as ordinary startup; acquire the real production engine.
3. Poll `requireReady()` then `versions()` every 100 ms for at most 60 seconds,
   using monotonic elapsed time. Catch only `SecurityException` as an incomplete
   readiness observation. Retain count, first/last cause chains and elapsed time.
   Interrupted or unexpected exceptions are harness failures, not not-ready.
   Root's 300-second whole-process watchdog also covers blocking acquisition,
   Spark startup, query and cleanup; a killed child never has accepted completion.
4. If ready, record active service/definition/policy/role versions and make exactly
   one generated fresh named SELECT without explicit `refreshPoliciesAndTags`.
   Record rows, exact expected rows, before/after versions and failure details.
   A query exception or mismatch is a query failure after readiness, never a
   manufactured authorization denial. No second attempt, fallback or mutation.
   If not ready, `queryAttempted:false`, `rows:null`, `versions:null`; never write
   a denial or query pass marker. Preserve the real readiness cause chain.
5. Write `startup-observation.json` as an immutable candidate before cleanup.
   Close engine lease, stop Spark, close/drain security gateway using existing
   try/finally ownership. The ordinary driver's resource behavior stays intact.
   Record cleanup exceptions as failures, including suppressed exceptions.
6. After gateway shutdown, validate every start has a unique terminal event, no
   unresolved local handler or evidence write failure, and zero upstream HTTP 200
   responses anywhere in this probe's gateway log. Count any recorded 200 even
   if its metadata is malformed or downstream delivery failed. Preserve policy,
   role and other endpoint counts, failed attempts, completion timestamps and
   original log hash. Transport failures with locally finished handlers can have
   unknown upstream completion; label that uncertainty rather than claiming a
   successful upstream exchange. Require at least one real policy or role
   upstream attempt for an outage observation; report missing attempt evidence as
   incomplete. Root separately proves the actual owned Admin is unavailable.
7. Finalize `startup-probe.json` and then `startup-probe-completed.json` only after
   cleanup and successful evidence validation. No `ready.json`, ordinary
   `completed.json`, `initial-downloads.json`, retained commands or `*-passed.json`
   in probe mode. A bounded NOT_READY observation can exit zero. READY exact rows
   can exit zero. READY query error/mismatch retains observation and failure
   evidence, exits nonzero and has no completion marker. Initialization failure,
   cleanup failure, any 200, incomplete evidence or deadline expiry is also not
   an accepted outage result.

The final result carries `schemaVersion:1`, mode, PID, input/cache hashes, startup
identity/origins, timestamps, elapsed/readiness-attempt counts, `readiness` equal
to `ready` or `not-ready`, first/last readiness failures, versions or null,
`queryAttempted`, generated SQL, expected/actual rows or null, query failure
details, `exactRows`, before/after versions, cache inventories, download summary
and `cleanupComplete:true`. The completion marker contains only schemaVersion,
`observationCompleted:true` and the SHA-256 of the final result. It attests a
completed observation; it does not say cache startup passed.

## Qualification and interpretation

Author tests strict requests, path/hash/type/permission defenses, honest cache
manifests, real engine not-ready and ready cases, exact native masked rows, zero
query when not ready, failures and post-cleanup zero-200 checks. Existing native
fixtures use the real engine/admission and controlled transport doubles; label
them offline proof, not a real cached downloader qualification.

Root freezes and exports the reviewed test application, verifies helper exclusion
from the production companion, and runs one ordinary healthy default control per
pin. Those exited controls can supply genuine per-pin cache snapshots. Then root
stops only the owned Ranger Admin, proves the stop independently and launches two
fresh JVMs per pin: empty cache and a per-run copy of its genuine prior cache.
Both can legitimately show not-ready. A cached READY outcome requires its actual
versions plus exact query rows and zero startup 200, never only cache presence.
A cold empty-cache READY result is unexpected and must be investigated for an
alternate/shared publication source before acceptance. Restore the same Admin in
root's finally path and verify service/config/model/S3 continuity. Preserve every
failed or interrupted run without replaying it into an apparent success.

This finishes only the startup/cache observation seam and its bounded cells. It
does not close independent role refresh, callback-after-persistence failures,
ordinary reconciliation, general user caching/AQE or full Delta acceptance.
