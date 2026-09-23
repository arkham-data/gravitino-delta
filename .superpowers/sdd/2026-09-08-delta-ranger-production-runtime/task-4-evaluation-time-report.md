# One policy access time per decision batch: implementation evidence

Base commit: `52ef6ce5b9475fa644be0344616cf9b67f46c0ab`.

The Java engine now has package-private `authorize`, `mask`, and `rowFilter` overloads that accept an `Instant`. The existing public methods sample `Instant.now()` and delegate. `evaluate` converts the required time to a fresh `Date` for each Ranger request before invoking the evaluator; null and unrepresentable times reject through the existing fail-closed `SecurityException` path. Readiness, role resolution, publication identity, and result validation remain in the original evaluation path. No engine-global clock or batch state was added.

## Final source hashes

| Source | SHA-256 |
| --- | --- |
| `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerPolicyEngine.java` | `60a793ef23810955ff30cfa9b6b8fbf01bafddb474a0a2e9f3e8e76633a8be5d` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerPolicyEngine.java` | `2938ab911e8581014358cfbf2887986aec68b2568547ddc953b5e9d73fd06115` |

`git diff --check` passed. The only changed Java/Scala sources against the base commit are the two paths above. Concurrent task coordination changed `progress.md` and `task-4-analysis-receipt-implementation-brief.md`; this implementation did not edit them. The final run's 72 captured source hashes equal the current files after `spotlessApply`.

## RED and GREEN

All Gradle invocations used `task-4-evidence/run-gradle.py`, Spark 3.5.4, Delta 3.3.2, Ranger 2.8.0 as pinned by the module, `-PskipITs`, `--offline --console=plain --max-workers=2 --no-parallel`, and the single argument `-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2`. Each invocation included the task-3 classpath capture init script and a matching unique `-Ptask3FixEvidenceOutput` directory. The full argument vector and exit status are retained in each linked evidence directory's `command.json` and `exit.json`; the unabridged Gradle output is `gradle.log`.

| Run | Exact Gradle tasks and filters | Exit and finding |
| --- | --- | --- |
| [`evaluation-time-red-354-332-final`](task-4-evidence/evaluation-time-red-354-332-final/) | `:spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerPolicyEngine.explicitEvaluationTimeIsSharedAcrossTimeDependentDecisions'` | 1; XML records one assertion failure for the missing `authorize(subject, key, Instant)` overload. This establishes API absence, not the time semantics. |
| [`evaluation-time-semantic-red-354-332-captured`](task-4-evidence/evaluation-time-semantic-red-354-332-captured/) | Same focused test filter | 1; with an explicitly temporary wall-clock request assignment, the time-dependent access evaluator denied the request. XML records one `Delta Ranger access denied` failure. The production assignment was restored immediately afterward. |
| [`evaluation-time-final-354-332`](task-4-evidence/evaluation-time-final-354-332/) | `:spark-connector:spark-delta-ranger-3.5:spotlessApply :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerPolicyEngine' --tests '*TestDeltaRangerSnapshotStamp' :spark-connector:spark-delta-ranger-3.5:javadoc` | 0; XML: 20 engine tests and 16 snapshot stamp tests, no skipped/failures/errors. Spotless and Javadoc succeeded. |

Earlier unsuccessful attempts were setup failures, not behavioral RED: [`evaluation-time-red-354-332`](task-4-evidence/evaluation-time-red-354-332/) stopped at a sandbox-denied Gradle wrapper lock; [`evaluation-time-red-354-332-cache`](task-4-evidence/evaluation-time-red-354-332-cache/) stopped at test compilation due to ErrorProne warnings; [`evaluation-time-red-354-332-behavior`](task-4-evidence/evaluation-time-red-354-332-behavior/) stopped at Spotless; [`evaluation-time-red-354-332-assertion`](task-4-evidence/evaluation-time-red-354-332-assertion/) reached JUnit but failed during Mockito stub setup. Two semantic RED attempts also stopped at compile-time unused-variable warnings before the captured semantic RED. None are counted as passing evidence or a behavioral RED.

The final tests inspect the constructed Ranger access, mask, and filter requests and show the same explicit access time across all three, distinct mutable `Date` instances, time-dependent decisions, current-time behavior of public methods, and no evaluator invocation for null or unrepresentable explicit times. The existing fixture retains real `RangerAuthContext` and role mapping setup. Existing snapshot publication drift tests passed without changes.

The final evidence directory retains the original JUnit XML, source copies and hashes, artifact hashes, and `execution-classpath.json` captured by `Test.doFirst` immediately before the test JVM; it lists 203 inputs, with all 11 archived input paths present. This is the Java prerequisite only. The future Scala caller must sample one `Instant` for the whole access/filter/mask batch across engines, and native pair coverage belongs to that integrated change.
