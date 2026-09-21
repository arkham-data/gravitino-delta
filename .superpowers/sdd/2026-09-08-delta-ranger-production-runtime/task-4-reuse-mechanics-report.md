# Reused collect boundary mechanics prototype

## Finite mechanics brief (written before test code)

Scope: a single new Scala test source, test-owned state/extension/operators, and this
report. No production hooks, security receipts, build files, services, Docker,
network, or commits. Base HEAD: `88478f00896c65d157f5f0d688bc0a59140bd543`.
The independent design review supersedes the opaque leaf proposal.

Use `SparkSession.Builder.withExtensions` to install a standalone `injectColumnar`
rule whose `postColumnarTransitions` adds a visible `UnaryExecNode` root without
`CodegenSupport`. The rule is local to test sessions. Override `executeCollect`
to call an injected driver validator before `super.executeCollect`; keep a
`doExecute` validation as defense in depth. Preserve output, ordering and
partitioning and visible child traversal. An instrumented child counts preparation,
execution and cleanup. Its optional preparation-time real Spark broadcast models
observable broadcast-like work before row execution; this is not qualification of
Spark broadcast-exchange joins.

Tests retain the same Dataset and physical plan across unchanged repeated
`collect`/`collectAsList`, then change the validator state and assert guard denial
with no additional child preparation or execution. A separate stale-first-action
case requires both counters to remain zero. Verify the wrapper remains root after
codegen passes, child codegen is still visible, and explicit root cleanup traverses
the child after success and failure. A failed child execution exercises cleanup
following an admitted action that fails downstream. Range scan is the bounded
minimum; file/vectorized scans may be added if runtime resources permit.

All supported test sessions explicitly configure `spark.sql.adaptive.enabled=false`;
unsupported AQE is rejected by the test extension if exercised. No production
configuration is changed. This is a characterization of the proposed boundary,
not proof of authorization, policy/role/registration publication consistency,
mask/filter decision receipts, native Delta provenance, cache rejection, AQE,
raw physical execute/prepare, RDDs, iterators, or already-running action revocation.
These remain a separate production vertical slice; task 4 stays open.

## Local API and build inspection

Existing Scala tests use JUnit Jupiter assertions and Spark `local[2]` sessions.
The module pins Scala 2.12.21, accepts `deltaSparkVersion=3.5.4|3.5.6` and
`deltaVersion=3.3.0|3.3.2`, and uses scalafmt 3.8.3. The Gradle task is
`:spark-connector:spark-delta-ranger-3.5:test`, with one test fork.
Local pinned Spark 3.5.4 JAR `javap` confirms non-final public `executeCollect`,
JVM-public `cleanupResources`, `ColumnarRule.postColumnarTransitions`, and public
`SparkSessionExtensions.injectColumnar`. Compilation/runtime will establish the
Scala override contract. No callable claude-mem/mcp-search tool was exposed;
prior local design and independent review were read instead.

## TDD and execution status

Initial RED source intentionally installs only the instrumented child, without an
action guard. The stale reused Dataset assertion must fail by returning rows where
a guard denial was expected. Preserve the failing report/log before adding the
minimal guard. Gradle execution was initially held until the root agent granted build ownership
after its current live Spark driver closed. At brief creation no RED/GREEN result was claimed; final outcomes appear below.

Planned command (run serially, with the root-approved existing Gradle environment):

```sh
./gradlew --offline --no-daemon --max-workers=1 \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerCollectBoundary \
  -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
```

Repeat the focused GREEN test with the second Spark pin when locally available;
record exact profile/results and limitations here. Apply module-scoped Scala
formatting after GREEN, then run its format check and focused test as needed.

## Preparation failures retained separately from RED

`collect-boundary-red`: Gradle cache lock denied by workspace sandbox; exit 1.
The automatic review approved the bounded offline escalation; no review rejection.
`collect-boundary-red-allowed`: normal Scala Spotless gate stopped before compile.
`collect-boundary-red-formatted`: pinned Scala compiler rejected direct
`SparkPlan.cleanupResources` access because it is Scala-protected despite JVM-public
bytecode. The fixture now exposes inherited cleanup through a test-only trait,
without reflection, package bridge, hidden child, or production changes.
These are setup failures, not the required behavioral RED. Module-scoped Scala
formatting changed only the new test source. Gradle's existing configuration emits
its usual read-only Docker environment probe; no Docker/service mutations or live
queries are performed by this task.

## Behavioral RED observed before guard implementation

`collect-boundary-red-compiled`: Spark 3.5.4 / Delta 3.3.0, exit 1, 3 tests,
3 assertion failures, 0 errors/skips. Both stale-first collection and collection
of the retained Dataset after earlier successful collect/collectAsList returned
rows instead of throwing `CollectBoundaryDenied`. The downstream failure case
also observed zero action validations where one was required. The source archive
contains only the instrumented child; no guard exists in this RED input. Original
JUnit XML, command, classpath/input archives and log are retained in its directory.
The minimal visible unary collection guard was added only after observing this RED.

`collect-boundary-green-354` also retained the unchanged RED source: a guarded
text replacement stopped before writing because formatter indentation differed,
but the following shell command still ran. Its name is not a GREEN claim; actual
exit is 1 with the same three expected RED assertions. Subsequent commands separate
source edits from Gradle launch to avoid this ambiguity.

## Final mechanics outcome

The visible test-only unary collection guard passes all three focused tests on both
pinned Spark versions. The public injection and collection override therefore
compile and execute on these two exact local test classpaths:

| Evidence directory | Spark / Delta | Tests | Failures / errors / skipped | Exit |
| --- | --- | --- | --- | --- |
| `collect-boundary-red-compiled` | 3.5.4 / 3.3.0 | 3 | 3 / 0 / 0 | 1 |
| `collect-boundary-green-354-guard` | 3.5.4 / 3.3.0 | 3 | 0 / 0 / 0 | 0 |
| `collect-boundary-green-356` | 3.5.6 / 3.3.2 | 3 | 0 / 0 / 0 | 0 |

All directories are beneath `task-4-evidence/` and retain command JSON, original
exit JSON/JUnit XML, logs, source snapshots, and actual `Test.doFirst` classpath
archives/hashes after dependency outputs were built. The fixture uses no credentials.
Runner limits: one Gradle worker, 768 MiB Gradle/Zinc maximum heaps and 1 GiB test
heap. No daemon is retained. Both runs include successful module Spotless checks;
the final run explicitly requests `spotlessScalaCheck`. No full-suite replay.

Observed assertions:

- The same retained Dataset and physical root return exactly rows `(1), (2), (3)`
  for unchanged collect and collectAsList. Their stale subsequent calls throw the
  test-owned guard exception without additional child preparation/execution.
  Restoring validator state permits the same Dataset again: five collection-entry
  validations across two initial successes, two denials and one restored success.
- The guard remains the actual root, while a WholeStageCodegenExec remains visible
  under its ordinary child. The guard itself has no CodegenSupport.
- A stale first collectAsList produces zero child prepare/execute/broadcast counts.
  An unchanged retry creates one real preparation-time broadcast. A later stale
  collect leaves child counters unchanged; explicit root cleanup destroys that
  broadcast through the visible child traversal.
- An admitted action that fails in child doExecute still has exactly one collection
  validation. Explicit root cleanup reaches the child and destroys its broadcast.

Cleanup coverage means an explicit inherited cleanup traversal via a test-only
Scala helper; it does not claim Dataset automatically calls cleanup on every action.
The broadcast fixture models preparation work using `SparkContext.broadcast`; it
is not a real BroadcastExchangeExec join qualification. The scan is a real Spark
Range scan; native Delta row/vectorized scans were deliberately not added to this
finite mechanics slice. Delta jars are profile inputs, not proof of Delta operations.
Only two diagonal version pairs ran; this is not the full four-pair acceptance matrix.

All supported tests explicitly use AQE=false. The extension has an AQE precondition,
but an AQE-negative test was not run and no AQE security/compatibility claim is made.
No engine/role/registration/original-decision receipt, user-cache rejection or typed
native-provenance implementation exists here. No production extension installs this
guard. Raw plan execution, prepare, RDDs and iterators remain outside the contract.
Production task 4, secure retained SELECT enforcement and broader Delta conformance
remain open. No production source, Gradle build file, core/API/client/server files,
commits, live endpoints or service state were changed by this subtask. The separately
modified `progress.md` belongs to the root agent.
