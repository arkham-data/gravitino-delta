# One policy access time per decision batch

Base: 52ef6ce5b9475fa644be0344616cf9b67f46c0ab. This is the small Java prerequisite
for the production logical receipt integration brief in this directory.

Own ONLY DeltaRangerPolicyEngine.java and TestDeltaRangerPolicyEngine.java (or one
new focused TestDeltaRangerEvaluationTime.java) in the optional spark-delta-ranger
module. Previous interrupted work is private at task-4-evidence/evaluation-time-staged;
inspect and reuse it if correct, but it is untested and has not been published.
No other implementation agent is active. You own the exclusive source/Gradle slot.
No git commit/staging, no services or Docker, no subagents. Root performs review
handoff and commits. No Gravitino core/API/server/client/base-catalog edits.

Add package-private overloads authorize(subject, key, Instant), mask(subject, key,
Instant), and rowFilter(subject, key, Instant). Existing public APIs retain their
behavior and delegate with Instant.now(). All existing readiness/publication checks
and fail-closed result semantics remain. Private evaluate uses the supplied required
immutable Instant and creates a distinct Date for each request. No engine-global
clock or mutable batch state. Invalid/null/unrepresentable Instant rejects before
calling the Ranger evaluator. A future Scala caller samples one Instant for the
whole access/filter/mask batch and across engines.

Tests must capture actual constructed Ranger requests and prove same explicit
access time for access/mask/filter, independently mutable Date objects, deterministic
time-dependent decisions, default public behavior, invalid time fail-closed without
evaluation, and unchanged publication drift checks. Use existing real
RangerAuthContext/roles fixture; do not weaken readiness checks or mock away evaluate.
Preserve meaningful RED (not a compilation/setup failure) and final GREEN.

Use existing task-4-evidence/run-gradle.py with unique labels, and always include:
-I .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix-classpath-capture.gradle
-Ptask3FixEvidenceOutput=.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/LABEL
: spark module is :spark-connector:spark-delta-ranger-3.5 (omit space after colon).
Final focused tests include TestDeltaRangerPolicyEngine, TestDeltaRangerSnapshotStamp
and the new evaluation tests, if separate. Run module spotlessApply/spotlessCheck
and javadoc with final first pair Spark3.5.4/Delta3.3.2/Ranger2.8.0, -PskipITs,
--offline --console=plain --max-workers=2 --no-parallel,
-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2 as ONE argument.
Do not rerun four native pairs for this Java-only prerequisite; the subsequent
integrated caller will cover all four. Archive exact commands, exits, original XML,
source hashes and Test.doFirst compiled inputs. Verify no other sources changed.
Write task-4-evaluation-time-report.md with source hashes, tests/commands/results,
RED vs setup distinction and limits. Return concise DONE/concerns, evidence paths,
and release the source/build slot. Independent spec+quality review follows.
