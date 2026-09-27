# Delta Startup/Cache Probe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already authorized continuous subagent execution; root reviews this elaboration before dispatch. Root owns Git and every live phase.

**Goal:** Observe real cold and genuine cached new-JVM Ranger startup with Admin unavailable, without weakening ordinary downloaded startup or promising cache readiness.

**Architecture:** Add one strict, finite branch to the private downloaded-query application. A test-only cache helper verifies/copies a root-sealed per-run snapshot before any plugin acquisition; a test-only probe helper polls actual engine readiness and executes one named SELECT only if ready. Final evidence is committed only after resource cleanup and a zero-HTTP-200 gateway audit.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12, JDK 17; existing Jackson, JUnit 5, Mockito, native Spark test fixtures and Gradle.

**Spec:** `docs/superpowers/plans/2026-09-27-delta-startup-cache-probe-design.md`; approved parent `docs/superpowers/plans/2026-09-08-delta-ranger-production-runtime.md`, Task 4.

## Global Constraints

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12, JDK 17.
- Existing S3 native authority, Gravitino registration and separate query/metadata subjects remain binding. Explicit user AQE remains false for these cells.
- No production hooks, core/API/server/client/storage-schema changes, dependency changes, shared cache, mutation commands, synthetic downloader publications or expectation that a cached restart must succeed.
- Ordinary startup remains empty-cache plus mandatory matching completed policy/role HTTP 200 and version floors before `ready.json`/commands.
- Author runs offline tests only. Root owns Git, immutable artifact export, private inputs, service lifecycle and all live acceptance.
- Preserve the original healthy/warm/effective-revocation evidence. Focused new helper/native tests plus one new default control per pin suffice unless a failure or broader source change justifies more; no automatic 1,340-test rerun.
- Follow existing Google Java style, imports, member ordering, resource ownership, nullable annotations and public/protected Javadoc. No new dependencies or test helpers in companion artifacts.

## Review Focus

- A valid policy file without matching role publication must remain a real engine NOT_READY observation; test a real engine fixture with missing roles and assert zero query calls.
- A terminal HTTP 200 with malformed metadata or failed downstream delivery still contaminates outage proof; test both and the duplicate start/terminal event representation.
- Cache source/destination parents, symlinks, hard links, special files, extra leaves and a wrong manifest must fail before engine acquisition; test the boundaries rather than only the happy copy.
- Readiness success followed by query failure or cleanup failure must retain its original observation without a success/completion marker; test native denial, wrong rows and failed finalization separately.
- Missing/null/malformed opt-in must never bypass ordinary startup, and a seeded cache must never be labelled fresh; test default gate preservation and both manifest shapes.

## File map and interface ownership

All Java paths below are under
`spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/`.

| File | Responsibility |
| --- | --- |
| Modify `DeltaRangerDownloadedQuery.java` | Strict input load, optional branch, shared unchanged Spark/identity setup and lifecycle integration. |
| Modify `DeltaRangerDownloadedSecurity.java` | Constructor overload for verified snapshot, truthful cache preparation manifest and post-close download evidence access. |
| Create `DeltaRangerStartupProbe.java` | Strict finite request, generated SELECT, bounded real readiness/action observation and final evidence validation. |
| Create `DeltaRangerStartupCache.java` | Fixed-location sealed manifest verification and private bounded cache copy; no live snapshot/export API. |
| Modify `TestDeltaRangerDownloadedQuery.java` | Default/probe dispatch and grammar regression tests. |
| Modify `TestDeltaRangerDownloadedSecurity.java` | Default empty-cache gate and seeded-constructor tests. |
| Create `TestDeltaRangerStartupProbe.java` | Readiness, evidence accounting, finalization and failure tests. |
| Create `TestDeltaRangerStartupCache.java` | Cache inventory/provenance/path/hash/permission adversarial tests. |
| Create `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerStartupProbeNative.scala` | New probe action against `NativeSelectFixture.withBoundSpark` and real native Delta rows. |

Read but do not edit `DeltaRangerCallbackGateway.java`,
`DeltaRangerDownloadedCommand.java`, `DeltaRangerPolicyEngine.java`,
`TestDeltaRangerNativeSelect.Fixture`, and the two module `build.gradle.kts` files.
Use the existing gateway: `recordedEvents()`, `awaitDrained()`, `close()`.
Do not introduce a general runner, callback fault facility or reusable framework.

### Task 1: Implement and prove the finite private startup probe

**Files:** Exactly the source/test files in the map above. Documentation edits are
limited to this plan's checkboxes and an author report supplied to root; root
decides durable report location. Do not edit runtime preparations or existing
qualification reports.

**Consumes:** `DeltaRangerPolicyEngine.acquire(DeltaRangerConfig)`,
`void requireReady()`, `Versions versions()`, `Versions.service()/definition()/policy()/role()`;
existing four-argument `DeltaRangerDownloadedSecurity` constructor;
`DeltaRangerDownloadedQuery.failureDetails(Throwable)`;
`NativeSelectFixture.withBoundSpark { (spark, policy) => ... }`;
`TestDeltaRangerNativeSelect.Fixture.engine` and mocked `plugin` for offline tests.

**Produces:** Package-private helpers, no new public production API:

```java
// DeltaRangerStartupProbe
static Optional<Request> parse(JsonNode input);
static Observation observe(SparkSession spark, DeltaRangerPolicyEngine engine, Request request)
    throws Exception;
static Observation observe(
    SparkSession spark, DeltaRangerPolicyEngine engine, Request request,
    LongSupplier nanoTime, Sleeper sleeper) throws Exception;
static Map<String, Object> downloads(List<String> recordedEvents, String service);
static void finish(Path output, Observation observation, Map<String, Object> downloadSummary,
    Map<String, Object> context) throws Exception;
interface Sleeper { void sleep(long millis) throws InterruptedException; }
// Request fields/accessors: mode(): String; manifestSha256(): Optional<String>;
// sql(): String; expectedRows(): JsonNode. Construct only through parse(input).
// Observation fields/accessors: evidence(): Map<String,Object>; exactRows(): boolean;
// ready(): boolean; queryAttempted(): boolean; failure(): Optional<Throwable>.
// Preserve detached immutable evidence; copy JsonNode input/output where necessary.

// DeltaRangerStartupCache
static Optional<Snapshot> load(Path requestFile, JsonNode input,
    DeltaRangerStartupProbe.Request request) throws Exception;
// Snapshot fields/accessors: manifestSha256(): String; manifest(): JsonNode;
// provenance(): JsonNode; cacheFiles(): Map<String,byte[]>.
// Keep bounded source bytes private; return defensive copies, do not expose mutable arrays.
// Map keys are validated leaf basenames with the manifest's "cache/" prefix removed.
static Map<String, Object> copy(Snapshot snapshot, Path emptyCache) throws Exception;
// Returns manifest digest and sorted copied file {name,size,sha256} inventory.

// DeltaRangerDownloadedSecurity: retain old constructor unchanged in meaning.
DeltaRangerDownloadedSecurity(Path source, Path target, Path evidence, String service,
    Optional<DeltaRangerStartupCache.Snapshot> snapshot) throws Exception;
List<String> startupEvents() throws IOException; // delegates to gateway.recordedEvents()

// DeltaRangerDownloadedQuery: extract original setup unchanged for both branches.
private static SparkSession openSpark(JsonNode input) throws Exception;
private static Map<String, Object> startup(SparkSession spark, JsonNode input) throws Exception;
private static void runStartupProbe(Path requestFile, JsonNode input, Path output,
    DeltaRangerStartupProbe.Request request) throws Exception;
```

`observe` with clock/sleeper exists only to drive a fixed 60-second deadline in
offline tests; no configurable duration in requests. `observe` normally delegates
with `System::nanoTime` and `Thread::sleep`. `finish` writes only after the caller
has completed engine/Spark/security cleanup. No supplier seam substitutes the
deployed engine or live SQL action.
`context` carries the launch/security/cache hashes, PID, startup identity/origins,
start/finish timestamps and copied/final cache inventories gathered by the owning
query application. `finish` builds a detached final record from context,
observation and download summary, rejects duplicate keys rather than allowing
context to overwrite observations, adds `cleanupComplete:true` only at that
post-cleanup boundary, and writes the result plus its digest-bound marker.

- [ ] **Step 1: Pin the grammar and default route with failing tests.**

Add these representative JUnit tests in `TestDeltaRangerStartupProbe`; imports use
normal JUnit/Jackson types. Complete the table directly below with parameterized
tests, retaining exact allowed key sets from the design.

```java
@Test
void absentProbeKeepsDefaultAndNullProbeRejects() throws Exception {
  ObjectMapper json = new ObjectMapper();
  assertTrue(DeltaRangerStartupProbe.parse(json.readTree("{}")).isEmpty());
  assertThrows(IllegalArgumentException.class,
      () -> DeltaRangerStartupProbe.parse(json.readTree("{\"startupProbe\":null}")));
}

@Test
void probeBuildsOnlyOneNamedSelect() throws Exception {
  JsonNode input = new ObjectMapper().readTree(
      "{\"catalogs\":[\"a\"],\"startupProbe\":{\"mode\":\"empty\",\"query\":{"
          + "\"catalog\":\"a\",\"schema\":\"db\",\"table\":\"t\","
          + "\"columns\":[\"secret\"],\"expectedRows\":[[\"a-one-masked\"],[null]]}}}");
  DeltaRangerStartupProbe.Request request = DeltaRangerStartupProbe.parse(input).orElseThrow();
  assertEquals("SELECT `secret` FROM `a`.`db`.`t`", request.sql());
  assertTrue(request.manifestSha256().isEmpty());
}
```

| Input | Required assertion |
| --- | --- |
| null/boolean/unknown mode; unknown key; empty-mode digest; snapshot missing/bad digest | Throws before any cache/Spark operation. |
| duplicate JSON key/trailing JSON in request bytes | Strict application reader throws, instead of silently keeping one branch value. |
| SQL/path/timeout/expectedReady/denied/refresh keys | Reject as unknown; no new flags. |
| missing catalog visibility; dot/slash/backtick/wildcard in identifier; duplicate/empty/>16 columns | Reject. |
| >256 expected rows; wrong row width; object/array cell | Reject. |
| two equal expected rows versus one actual row; null versus string "null" | Comparison fails; unordered equivalent multiset succeeds. |

Add a default-route regression using existing `TestDeltaRangerDownloadedSecurity`
loopback infrastructure: four-argument constructor yields empty target cache and
`freshEmptyCache:true`; `recordInitialDownloads(1,1,1,1)` throws before both real
matching local HTTP 200 responses. One response remains insufficient; after both
it succeeds. Seeded target passed to the old constructor continues to reject.
This is local transport evidence, not a live Ranger publication claim.

- [ ] **Step 2: Run RED and preserve the original exit/log.**

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerStartupProbe' \
  --tests '*TestDeltaRangerDownloadedSecurity' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 -PskipITs --offline --console=plain
```

Expected: new helper references fail compilation, or new assertions fail if the
minimal declaration exists. An unrelated build/environment error is not RED proof.

- [ ] **Step 3: Write cache-boundary tests before implementing the copier.**

Build private synthetic fixture files in `@TempDir` using the exact manifest and
evidence shapes from the design; fake JSON bytes are explicitly offline fixtures.
Use normal `Files.createDirectory`, `Files.createFile`, `PosixFilePermissions`,
`MessageDigest`, and Jackson. Each test starts from a new directory and records
the matching exact manifest digest in the request, avoiding mutation cross-talk.

```java
// In TestDeltaRangerStartupCache, fixture is a private nested helper whose
// constructor writes the complete layout from the design with policy=1, role=1,
// Spark=3.5.4, Delta=3.3.0 and service/definition/metalake=service/spark/lake.
// Methods: requestFile(): Path; input(): JsonNode; cacheLeaf(): Path;
// request(): DeltaRangerStartupProbe.Request. Use source PID distinct from current PID.
@Test
void alteredCacheBytesFailBeforeTargetPreparation() throws Exception {
  Fixture fixture = new Fixture(directory);
  Files.setPosixFilePermissions(fixture.cacheLeaf(),
      PosixFilePermissions.fromString("rw-------"));
  Files.writeString(fixture.cacheLeaf(), "changed");
  Files.setPosixFilePermissions(fixture.cacheLeaf(),
      PosixFilePermissions.fromString("rw-------"));
  assertThrows(IllegalArgumentException.class,
      () -> DeltaRangerStartupCache.load(
          fixture.requestFile(), fixture.input(), fixture.request()));
  assertFalse(Files.exists(directory.resolve("driver-security/cache")));
}
```

Additional independent tests: exact hash-preserving copy; both policy and roles
leaves copied without parsing/replacing their contents; one-file cache accepted
as an input inventory without inventing missing roles; empty snapshot rejected;
extra/unlisted leaf; size/hash/manifest mismatch; unsupported pin or wrong service/definition/
metalake/version; missing prior matching completed role/policy evidence; prior
PID equal to current PID; nonzero recorded source exit; wrong evidence mode;
symlink source parent, manifest, cache leaf, target and target parent; hard link;
directory in place of file; bad permissions/owner; `../escape`, absolute name,
nested name, duplicate name; each size/count limit. Use `Files.createSymbolicLink`
and `Files.createLink` on supported local POSIX storage. For owner tests, test the
owner-comparison function against a distinct principal without requiring chown.
Reject unsupported POSIX/link attributes instead of skipping validation.

- [ ] **Step 4: Implement strict manifest verification and the constructor overload.**

`load` resolves only `requestFile.getParent().resolve("startup-cache-input")`,
validates canonical parent chain and owned run paths, checks the supplied manifest
digest before parsing, checks the exact manifest/provenance schema, all declared
bindings and sorted inventory, then verifies the prior ready/initial-download/
completed/evidence consistency. Check source PID differs from current PID, but
do not mistake that single test for proof of a prior completed JVM. Root's source
exit record and independent chain verification provide that proof.

Use bounded reads with a channel opened using `LinkOption.NOFOLLOW_LINKS`, reject
nonregular/nlink!=1 files before opening, and compare attributes after reading.
Hash bytes retained for copying; do not reopen untrusted source paths later.
Read only expected fixed files and one flat cache listing. Compare listed and
manifest sets before copying; do not walk arbitrary descendants.

```java
// In copy: cache was exclusively created by DownloadedSecurity and is empty.
for (Map.Entry<String, byte[]> entry : snapshot.cacheFiles().entrySet()) {
  Path destination = emptyCache.resolve(entry.getKey()); // validated leaf only
  Files.createFile(destination,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
  try (OutputStream stream = Files.newOutputStream(destination, StandardOpenOption.WRITE)) {
    stream.write(entry.getValue());
  }
  // Inventory the destination's bounded bytes and assert size/digest equal the
  // already validated source entry before returning the copied inventory.
}
```

The old constructor delegates with `Optional.empty()`. The overload retains the
empty-target check and creates its private cache. Copy the verified bytes before
gateway creation and before any Spark/plugin access. Keep all other XML fields
identical; change only the same existing URL/cache properties. Set manifest
`freshEmptyCache` from snapshot presence, retain existing XML/logging hashes, and
write the validated input/cache provenance privately. A failure prevents engine
acquisition and preserves partial private evidence; it does not delete or reuse
the target. `startupEvents()` simply returns `gateway.recordedEvents()` and remains
usable after successful gateway close.

- [ ] **Step 5: Pin real readiness and post-cleanup evidence with failing tests.**

Use the real production engine inside `TestDeltaRangerNativeSelect.Fixture` to
exercise not-ready; use its existing transport mock only to remove a role
publication. Drive monotonic time forward without sleeping a minute:

```java
@Test
void missingRolesNeverExecutesSqlAndRemainsStartupFailure() throws Exception {
  try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
    when(fixture.plugin.getRangerRoles()).thenReturn(null);
    SparkSession spark = mock(SparkSession.class);
    AtomicLong now = new AtomicLong();
    JsonNode input = new ObjectMapper().readTree(
        "{\"catalogs\":[\"a\"],\"startupProbe\":{\"mode\":\"empty\",\"query\":{"
            + "\"catalog\":\"a\",\"schema\":\"db\",\"table\":\"t\","
            + "\"columns\":[\"secret\"],\"expectedRows\":[[\"a-one-masked\"],[null]]}}}");
    DeltaRangerStartupProbe.Request request = DeltaRangerStartupProbe.parse(input).orElseThrow();
    DeltaRangerStartupProbe.Observation result = DeltaRangerStartupProbe.observe(
        spark, fixture.engine, request,
        now::get, millis -> now.addAndGet(Duration.ofMillis(millis).toNanos()));
    assertFalse(result.ready());
    assertFalse(result.queryAttempted());
    assertEquals("not-ready", result.evidence().get("readiness"));
    assertNull(result.evidence().get("versions"));
    verifyNoInteractions(spark);
    assertTrue(now.get() >= Duration.ofSeconds(60).toNanos());
  }
}
```

Do not use a mocked `requireReady()` as the only readiness proof. Also test `getPoliciesVersion()=-1`,
wrong role service/context and an interrupted sleeper; only `SecurityException`
means incomplete readiness. A closed engine/unexpected error must propagate.

For download accounting use exact synthetic start/terminal JSON lines shaped like
the existing gateway. Deduplicate by `(run,attempt)`; require one run, strictly
positive attempt IDs, one start and one terminal per attempt, no terminal without
start, and identical immutable method/path/start fields. Preserve raw log hash.

```java
@Test
void anyHttp200InvalidatesAnOutageEvenWithoutDownloadMetadata() {
  String start = "{\"run\":\"r\",\"attempt\":1,\"method\":\"GET\","
      + "\"path\":\"/unclassified\",\"started\":\"2026-09-27T00:00:00Z\"}";
  String end = start.substring(0, start.length() - 1)
      + ",\"upstreamAttempted\":true,\"upstreamStatus\":200,"
      + "\"transportFailure\":\"java.io.IOException\","
      + "\"finished\":\"2026-09-27T00:00:01Z\",\"activeAfter\":0}";
  assertThrows(IllegalStateException.class,
      () -> DeltaRangerStartupProbe.downloads(List.of(start, end), "service"));
}
```

Test zero-200 failed policy/role attempts with locally finished handlers as valid
transport evidence while preserving `upstreamCompletionUnresolved:true`; test
an unmatched start, missing upstream attempt, duplicated terminal, malformed log,
mixed run, and lost-recorder exception as invalid. A 304 is not a 200, but it does
not prove Admin is unavailable; root's independent stop evidence remains required.
Test `finish` writes no completion marker on query error/mismatch or failed
download audit, and atomically binds the final result digest when all checks pass.
Simulate cleanup failure at the lifecycle boundary and assert finalization is
never called. Preserve `startup-observation.json` for each failure case.

- [ ] **Step 6: Implement observation and integrate one branch, preserving default behavior.**

Strictly read request bytes with the existing Jackson strict duplicate/trailing
options used by `DeltaRangerDownloadedCommand`. Parse the opt-in before setup;
after output directory creation, branch exactly once:

```java
Optional<DeltaRangerStartupProbe.Request> probe = DeltaRangerStartupProbe.parse(input);
if (probe.isPresent()) {
  runStartupProbe(Path.of(args[0]), input, output, probe.orElseThrow());
  return;
}
// Existing ordinary security constructor -> Spark -> acquire -> await ->
// recordInitialDownloads -> ready.json -> runCommands, unchanged in meaning.
```

Extract `openSpark` from existing properties/Hadoop/Spark builder code and
`startup` from existing identity/origins/helpers/catalog visibility code. Preserve
the exact construction and checks in both paths; no generalized settings object.
In the probe call `load` before constructing security, then construct security
before `openSpark`; initialize config with the same `fromClasspath` call. Keep a
reference to security so `startupEvents()` can read its durable log after close.

Readiness core (build the result from these actual observations):

```java
long started = nanoTime.getAsLong();
SecurityException first = null;
SecurityException last = null;
DeltaRangerPolicyEngine.Versions ready = null;
int attempts = 0;
do {
  attempts++;
  try {
    engine.requireReady();
    ready = engine.versions();
    break;
  } catch (SecurityException incomplete) {
    if (first == null) {
      first = incomplete;
    }
    last = incomplete;
  }
  sleeper.sleep(100);
} while (nanoTime.getAsLong() - started < Duration.ofSeconds(60).toNanos());
```

Never consult fixture floors to manufacture readiness and never call an explicit
refresh in the probe. A ready result executes `spark.sql(request.sql()).collectAsList()`
once, using the existing row-to-list conversion and JSON multiset comparison.
Retain actual before/after `Versions`; a later readiness failure during/after SQL
is a query-phase failure, not an initial NOT_READY rewrite. Do not call the
existing denial classifier or set `authorizationDenied` in a probe record.

`runStartupProbe` records the immutable candidate before cleanup, closes engine,
stops Spark in finally, then closes security. Only on successful cleanup call
`downloads(security.startupEvents(), input.path("resourceService").asText())`,
inventory the now-quiescent cache and call
`finish`. Catch failures outside the entire owned lifecycle, write ordinary private
`failure.json` with cause and suppressed cleanup chains, then rethrow. Include
input preparation hashes even when acquisition never succeeds. Keep an observation
variable optional until the engine is actually observed; do not fabricate one for
an acquisition exception. No `System.exit`, background action retry or JVM-owner
reset. Root's external watchdog handles uninterruptible initialization/query.

- [ ] **Step 7: Prove the action against native Delta on every pin.**

Add the new Scala native test using the established fixture. This verifies real
engine readiness and the new action path, not live cache loading:

```scala
@Test
def readyProbeCollectsExactNativeMaskedRows(): Unit =
  NativeSelectFixture.withBoundSpark { (spark, policy) =>
    val input = new ObjectMapper().readTree(
      """{"catalogs":["a"],"startupProbe":{"mode":"empty","query":{
        |"catalog":"a","schema":"db","table":"t","columns":["secret"],
        |"expectedRows":[["a-one-masked"],[null]]}}}""".stripMargin)
    val request = DeltaRangerStartupProbe.parse(input).orElseThrow()
    val result = DeltaRangerStartupProbe.observe(spark, policy.engine, request)
    assertTrue(result.ready())
    assertTrue(result.queryAttempted())
    assertTrue(result.exactRows())
    assertEquals("ready", result.evidence().get("readiness"))
    assertTrue(result.failure().isEmpty)
  }
```

Also add native tests that set `policy.denied.add("a:secret")` before observing
and that supply wrong expected rows. Both must retain `readiness:"ready"`, show
one attempted query and fail finalization without a completion marker. The denial
case retains the real query exception and empty/no partial rows; wrong rows retain
actual masked rows and `exactRows:false`. Add a not-ready native test by removing
roles, using the clock overload and asserting no new Spark job IDs. Do not alter
the shared fixture or production engine to accommodate a desired result.

- [ ] **Step 8: Run focused GREEN and formatting with durable per-pin outputs.**

Format only the affected module using its existing Spotless task. Use one author
build owner; archive original log, exit code and JUnit XML after each pin before
the next invocation overwrites `build/test-results`. Four sequential commands are
generated by this finite loop; root provides a fresh owner-only report directory
through `STARTUP_PROBE_PROOF`, not a service endpoint:

```bash
test -n "$STARTUP_PROBE_PROOF"
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
  :spark-connector:spark-delta-ranger-3.5:spotlessApply --offline --console=plain
for probe_spark in 3.5.4 3.5.6; do
  for probe_delta in 3.3.0 3.3.2; do
    probe_pair="$probe_spark-$probe_delta"
    mkdir "$STARTUP_PROBE_PROOF/$probe_pair"
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
      :spark-connector:spark-delta-ranger-3.5:test \
      --tests '*TestDeltaRangerStartupProbe' \
      --tests '*TestDeltaRangerStartupCache' \
      --tests '*TestDeltaRangerStartupProbeNative' \
      --tests '*TestDeltaRangerDownloadedQuery' \
      --tests '*TestDeltaRangerDownloadedSecurity' \
      --tests '*TestDeltaRangerCallbackGateway' \
      --tests '*TestDeltaRangerRetainedQuery' \
      -PdeltaSparkVersion="$probe_spark" -PdeltaVersion="$probe_delta" \
      -PskipITs --offline --console=plain \
      > "$STARTUP_PROBE_PROOF/$probe_pair/gradle.log" 2>&1
    probe_exit=$?
    printf '%s\n' "$probe_exit" > "$STARTUP_PROBE_PROOF/$probe_pair/exit.txt"
    cp -R spark-connector/v3.5/spark-delta-ranger/build/test-results/test \
      "$STARTUP_PROBE_PROOF/$probe_pair/junit"
    test "$probe_exit" -eq 0 || exit "$probe_exit"
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
      :spark-connector:spark-delta-ranger-3.5:spotlessCheck \
      -PdeltaSparkVersion="$probe_spark" -PdeltaVersion="$probe_delta" \
      --offline --console=plain \
      > "$STARTUP_PROBE_PROOF/$probe_pair/spotless.log" 2>&1
    probe_format_exit=$?
    printf '%s\n' "$probe_format_exit" > "$STARTUP_PROBE_PROOF/$probe_pair/spotless-exit.txt"
    test "$probe_format_exit" -eq 0 || exit "$probe_format_exit"
  done
done
```

Expected: all selected tests pass on all four pins; identify actual test counts
from original XML, not an expected fixed total. The retained-query suite is a
focused regression because startup extraction shares its application. No live
`TestDeltaRangerDownloadedPolicies`, endpoints or Docker opt-in. If Gradle reports
UP-TO-DATE across changed pins, inspect resolved classpaths and test task inputs;
force only this test task when needed, preserving the original skipped output.
Do not run repository-wide formatting or unrelated broad tests as a ritual.
Keep test and formatting invocations separate: Gradle otherwise associates the
trailing `--tests` options with `spotlessCheck` and rejects the command before any
test runs. Preserve a rejected invocation as setup evidence, not a test result.

- [ ] **Step 9: Deliver one reviewed implementation unit to root.**

Report changed files, exact signatures, RED/GREEN evidence, all four JUnit totals,
no production/dependency/build-script changes, and cache/path/evidence limitations.
Run `rg` over changed Java for inline fully qualified types and check new exposed
Javadoc/resource cleanup. Verify source diff scope and count new helpers. Root
performs independent review and decides the scoped commit; author does not commit,
touch Git state, package artifacts or launch services.

### Root qualification: freeze, export and observe the eight outage cells

This is a root-only execution checklist after Task 1 passes independent review,
not another implementation task or permission request. Preserve original artifact
and evidence records. New test-app evidence does not retroactively relabel prior
warm/revocation results.

- [ ] Freeze reviewed source IDs/hashes before packaging. Reuse the existing
  archive/export procedure and archived pin-specific dependency closure. Build
  `:spark-connector:spark-delta-ranger-3.5:testClasses` for each pair, with JDK17,
  `-PdeltaSparkVersion` and `-PdeltaVersion`; export test application separately.
  Include new `DeltaRangerStartupProbe$*` and `DeltaRangerStartupCache$*` application
  helpers in its explicit allowlist. Do not include `Test*` classes in deployed
  application. Record all class/JAR/source hashes and pin-specific classpaths.
- [ ] Independently inspect each production companion and base JAR with `jar tf`.
  Assert no class basenames matching `DeltaRanger(Downloaded|CallbackGateway|RetainedQueries|RetainedAction|StartupProbe|StartupCache)`
  and no `Test*` test classes; inspect the fully qualified private helper names to
  avoid mistaking production retained-query guards for test helpers. Verify the
  companion against the existing immutable archive manifest and hash. If rebuild
  ZIP metadata changes its outer digest, compare entry names and byte hashes and
  investigate any payload change before claiming unchanged runtime. A changed
  production payload requires a separate review and impact-based qualification.
- [ ] Prepare four new private healthy default launches using the new test app,
  existing trusted production artifacts, actual four Spark/Delta distributions,
  separate metadata principal and explicit user AQE false. Run one known positive
  named SELECT per pin; require ordinary empty-cache/security manifest, initial
  policy+role matching completed HTTP 200, exact expected rows, identities/origins,
  natural exit zero and root-verified fixture/S3 continuity. No mutation schedule.
- [ ] After each source JVM exits naturally, capture its entire flat cache into
  a sealed per-consumer bundle with byte/size/name hashes, exact prior ready,
  initial-download, gateway and completed records and root provenance. Record
  source exit evidence, source PID/run, exact source paths and manifest digest in
  root's private launch/capture ledger. Never select historical cache by filename
  alone or copy while its producer can refresh it. Derive each probe query's exact
  expected multiset from the independent current fixture/model/S3 baseline.
  Preserve original Ranger leaf modes (currently 0644 within private 0700 cache)
  in the capture record and copy byte-identical input leaves as 0600 in new 0700
  directories; do not chmod or rewrite the historical source. The current
  policy64/tag5/role84 checkpoint is context only, never an expected constant in
  source or a replacement for parsing/verifying the actual cache and live source.
- [ ] Prepare eight fresh launches: empty and snapshot for each pin, each with new
  request/output/security directories and separate JVM identity. Inputs contain
  the explicit probe object; no reusable cache path or custom timeout. Save exact
  input bytes/hashes, launch argv, environment allowlist, artifact manifest, known
  baseline model/download generations, root source-stop interval and expected
  results before launching. Default runtime settings and production XML retain
  the same content except the existing private gateway/cache rewrites.
- [ ] Snapshot root-owned Admin identity/config/model state, stop only that Admin
  and preserve the actual stop/unreachable evidence. Launch cells sequentially
  under the existing root process supervisor, each with a 300-second hard wall
  deadline from process spawn. Save stdout/stderr privately and exact exit/signal.
  On timeout, terminate the owned child and reap it; retain original partial
  evidence as incomplete, never fabricate completion or retry the same directory.
  Keep independent stop evidence around every cell. One shutdown interval may
  cover all cells if continuity remains independently demonstrable.
- [ ] Independently verify final result digest/marker, natural exit zero, unique
  PID versus producer, actual origins/pins, copied-cache inventory and recorded
  `requireReady`/versions outcome. Require every gateway attempt reconciled and
  zero HTTP 200 from process start through cleanup; verify the original log, not
  merely its summary. NOT_READY with no query is a valid startup observation.
  Cached READY requires exact versions and exact SELECT rows. Empty READY is an
  anomaly requiring investigation. A query error, cleanup error, missing attempt
  evidence, lost log or killed JVM remains unsuccessful qualification.
- [ ] Restore the exact owned Admin in a root finally path even after failed
  cells; preserve real restore output, service/config/model and S3 continuity,
  and any generation changes. Do not replay grants, restore an old DB snapshot,
  silently change expected results or rerun prior accepted mutation matrices.
  Retain the eight-cell table with outcome-specific wording and link the exact
  request/result/exit/cache/download evidence. If cached cells are NOT_READY,
  record cache-only availability as unproved rather than changing production.

## Plan self-review

The sole implementation task covers the strict opt-in, fixed private cache
provenance, real readiness/query observation, cleanup, evidence and focused tests.
Root qualification covers genuine prior downloads, fresh JVMs, Admin outage,
runtime hashes/pins and durable acceptance. Each Review Focus line has explicit
tests above. No production readiness change, startup success expectation, generic
cache path, speculative refresh option, live author task or broad matrix replay
is required by this plan.
