# Task 4 evaluation-time review

Reviewed the frozen two-file review package against `task-4-evaluation-time-implementation-brief.md` and the exact current Java sources. The current source SHA-256 values match `task-4-evidence/evaluation-time-root-verification.json`: engine `60a793ef23810955ff30cfa9b6b8fbf01bafddb474a0a2e9f3e8e76633a8be5d`, test `2938ab911e8581014358cfbf2887986aec68b2568547ddc953b5e9d73fd06115`.

## SPEC verdict: PASS

No findings. The three public methods delegate with `Instant.now()` to package-private overloads (`DeltaRangerPolicyEngine.java:112-181`). Each overload retains the existing owner lock and calls the same private evaluator. That evaluator converts the required supplied `Instant` into a new `Date` for each call, before any Ranger evaluator invocation (`DeltaRangerPolicyEngine.java:609-645`). A null or out-of-range time is caught and rejected as `SecurityException` (`DeltaRangerPolicyEngine.java:618,653-655`). The existing snapshot validation before evaluation, downloaded-role resolution, result checks, and publication check after evaluation remain in the shared path (`DeltaRangerPolicyEngine.java:619-652`). No global clock or mutable batch time was added.

## QUALITY verdict: PASS

No findings. The new test makes access approval, custom masking, and row filtering depend on the exact supplied timestamp; it captures all three constructed Ranger requests and verifies equal times, distinct mutable `Date` objects, and isolation after mutating one (`TestDeltaRangerPolicyEngine.java:298-355`). Another test exercises current-time public calls and verifies that null and out-of-range explicit times reject without an evaluator call (`TestDeltaRangerPolicyEngine.java:358-384`). The existing fixture uses a real `RangerAuthContext` and downloaded-role setup, and unchanged snapshot-stamp tests exercise publication drift.

The archived semantic RED XML records one actual `Delta Ranger access denied` assertion failure. The frozen final XML records 20 engine tests and 16 snapshot-stamp tests, all passing with no skips; the archived command also includes module Spotless and Javadoc and exited 0. Root verification confirms 11 compiled archive inputs and the Spark 3.5.4, Delta 3.3.2, and Ranger 2.8.0 pins. This review inspected the archived evidence and sources; it did not rerun Gradle or validate an external GitNexus index.

This is the Java prerequisite only. The future Scala caller must sample one `Instant` for the full access/filter/mask batch across engines. Its receipt integration and four native-pair coverage are outside this diff and these verdicts.
