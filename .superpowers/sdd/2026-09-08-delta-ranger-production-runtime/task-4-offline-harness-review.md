# Task 4 offline harness intermediate review

Reviewed frozen commit `d252392ca649fc0d88578ced30f21b4d7dab8330` against parent `e1347911c906695a0eedfdfcf6e26d698ce33f6e` on September 21, 2026. Scope is the six changed test Java sources and the README/results changes. No builds, test executions, services, Docker operations, implementation edits, or external mutations were performed during this review.

**Verdict: approve the bounded test-harness correction; no P0–P2 findings.** One nonblocking documentation correction is noted below. This is an intermediate review, not Task 4 acceptance or four-pair live qualification.

## Minor documentation finding

- `spark-connector/v3.5/spark-delta-ranger-runtime/README.md:109–110` says qualification of the catalog-use/schema-use/table-select metadata permission composition is still pending. The same commit's `docs/delta-ranger-production-results.md:161–162` records the narrow historical check: all three metadata reads succeeded and nine query-user metadata reads remained forbidden. Distinguish that completed historical permission check from the broader pending four-pair/live qualification. Docker remains stopped, so this should not imply current service readiness.

## Implementation assessment

- Policy-denial classification is bound to the pinned engine's actual `authorize` throw: exact `SecurityException` class, exact denial message, and the top stack frame's engine class/method, including when wrapped in a causal chain. Native admission failures, readiness failures, evaluation failures, and similarly worded exceptions from another origin no longer count as policy denials. The new tests exercise real engine denial and real engine readiness/evaluation failure paths. This is a trusted test-harness classifier, not a security boundary against arbitrary code fabricating exception stacks.
- Failure diagnostics retain cause classes, messages, and stack frames, with output files created using mode 0600 under the driver's mode-0700 output directory. The copied security/cache directories are private. Existing security properties are preserved except the explicit URL/cache replacements; logging configuration bytes are preserved. Duplicate/missing properties and external-entity XML inputs are rejected.
- The helper requires an empty destination and creates a fresh empty private cache before engine initialization. It records source/copy hashes and the two changed property names, without copying credentials into the manifest. The forwarding recorder does not log authentication headers or response/request bodies.
- Initial download acceptance requires two distinct exact service-specific paths, method GET, upstream attempted, actual status 200, recorded upstream completion, downstream completion, and a finished event. The pinned Ranger 2.8.0 `RangerRESTUtils` constants were independently inspected: policy `/service/plugins/secure/policies/download/` and roles `/service/roles/secure/download/`. Generation floors are checked independently of transport evidence. Local gateway drain is not substituted for upstream completion.
- The recorder refuses to expose an acceptable event snapshot after evidence-write loss. Negative coverage includes PUT, HTTP 304, another service, missing upstream completion, insufficient generations, absent downloads, and evidence-write failure. Ordinary gateway forwarding and controlled-fault behavior remain covered by the retained gateway tests.
- No production source changes are present in this commit. The previously reviewed native SnapshotState admission remains outside this patch. No broader query, coordinator, or public-callback acceptance is inferred from these offline helpers.

## Evidence binding

Read original retained evidence under `task-4-evidence/offline-harness-frozen-green-20260921`, including `command.json`, `exit.json`, the three JUnit XML files, `source-hashes-before.json`, archived sources, and `frozen-source-verification.json`. Independently recomputed all six frozen Git source SHA-256 values; each matches the pre-run hash and archived source bytes:

| Source basename | SHA-256 |
| --- | --- |
| DeltaRangerDownloadedSecurity.java | `47a1dd5976569bf9e9aea043f4ddebbb8692f511bdf2aa615778294b897d66f0` |
| TestDeltaRangerDownloadedSecurity.java | `2465b6b13b613b9bc08e64ce479141b7e03c92c54e175950b53a1abb16614eba` |
| DeltaRangerDownloadedQuery.java | `14f26284e1ad471d707e5848876eb6527795b0dbc50bdef8d32624f79964f650` |
| TestDeltaRangerDownloadedQuery.java | `b52200463d02030f6896046ba4093b12743da1ef0f3bd97187ad877b5b91d2e2` |
| DeltaRangerCallbackGateway.java | `4a6e721ce2de38f86b8e84b1176ba0ecb81a21515f96b5d5e9ede09334f483fb` |
| TestDeltaRangerCallbackGateway.java | `81cfca992df52a22a2a2e9eaccbeac9f99dfb26b26ee4a6c897cbe120219c5fa` |

The original frozen run used Spark 3.5.4 / Delta 3.3.2, selected only these three test classes, and ran Gradle offline. XML totals are **11 tests, 0 failures, 0 errors, 0 skips** (gateway 4, security 4, query 3); the original process exit is **0**. This command does not provide a separate Javadoc qualification claim.

Also inspected retained regression evidence: `offline-denial-classifier-red-20260921` has three failing tests and exit 1; `offline-download-endpoint-red-20260921` has one failing test of three and exit 1. These establish the original classifier/diagnostics and endpoint failures before the frozen correction. No live Ranger, Gravitino, or Spark qualification was performed by this review; loopback test doubles in archived tests are offline evidence only.

## Remaining scope limits

Current services are stopped by user direction. Historical versions and earlier live query results do not establish current readiness. Launching the corrected driver with its copied security directory on the classpath, real completed initial downloads, and the remaining public callback/four-pair queries still require separate live evidence when authorized. The unrelated main-checkout GitNexus index was not treated as an exact-commit graph; this verdict is based on frozen source, pinned dependency inspection, and retained evidence.

## Scoped documentation closure — September 21, 2026

Reviewed frozen documentation/Javadoc commit `aefb7953e30814d0c3fd1601e190516c0821da54`, whose parent is documentation commit `e1147b556579b385cc1e073cd1c752ce425fa326`, following the approved `d252392ca` harness freeze.

**The minor README finding is closed.** The README now explicitly records the three successful historical metadata-reader table loads and nine rejected query-user table loads, while keeping the complete four-pair live matrix pending. It no longer describes that narrow permission check as unqualified.

The sole source change in `aefb7953e` is one Javadoc line on `DeltaRangerDownloadedSecurity.close()`: “Releases the recorder and rejects unresolved handlers or lost download evidence.” No executable logic changed. Independently recomputed the frozen source hashes recorded in `task-4-evidence/offline-harness-javadoc-only-binding.json`: the original `d252392ca` source remains `47a1dd5976569bf9e9aea043f4ddebbb8692f511bdf2aa615778294b897d66f0`; the Javadoc-only source is `b10c9f287118b98d3f271fdfccfdfd15516efe3e35bb96a337c791fcf17beda6`. Both match the binding record. The approved archives remain bound to their original `d252392ca` sources; this comment change does not relabel those archives as a new build.

No rebuild, tests, or service operations were performed or requested for this closure. The coordinator preparation script and broader archive/version validation are separately owned by the root reviewer and were not re-reviewed here. The bounded harness approval remains in force, with no open finding from this review; Task 4/live acceptance remains pending.
