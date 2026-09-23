# Native-operation review fix round 1

Base remains993f5a9c9; source owner remains implement_native_operations. Review
`task-4-native-operation-review.md` R1/R2 and minor checkpoint warning. Root
verified the missing cases against the actual no-skipping test and distinct
DeltaRangerLogIndex continuation, and accepts both Important findings. Scope is
closing existing binding audit gates, not expanding the production feature.

Keep frozen original evidence intact: matrix-s354-d330271/0 and s354-d33254/0,
79-file source manifestSHA0b557168c2d9bc11a04b614fd33c3b74ecd621d5b7284ef8e29bc386b642d9dc.
Rows s356-d330/s356-d332 were never run on that freeze. Do not relabel old runs.

Add focused installed no-skipping positives for two-catalog joins, real quoted
name-mapped columns and partition/data filtering. Keep skipping false before and
after. Assert exact rows, original receipt obligations and actual owned log index
callback selection/filter sequences. Reuse real fixture helpers; do not manufacture
success by weakening native registration or settings.

Add distinct no-skipping controls from R2: compatible current-head advancement and
actual returned snapshot capture, native schema/column-mapping rejection, ordinary
logical/physical clones, equal bare-index/relation substitution and cross-session
Prepared.verify rejection. Exercise owned callbacks with closed/failed owners,
exception cleanup and overlapping independent invocations, preserving original
registration binding. Separate before-action compatibility from the later retained
action validator requirement. Use the correct provenance/callback boundary and
explicit positive controls, not generic unsupported errors. Do not claim complete
native-index serialization; accepted inert-ref/private transient-field limits stay.

For checkpoint warning, inspect an equivalent supported public entry point on both
pinned patches; use it if behavior equivalent, otherwise narrowly document/suppress
fixture-only intentional deprecated call. No production API workaround.

Start with tests against current production source; these missing coverage tests
may pass immediately, which is coverage evidence and not a fabricated RED. If any
reveals a production defect, preserve meaningful failure and diagnose before fixes,
notify root, then obtain fresh full regression. No root source edits.

Verification: format before new freeze. Preserve old manifest, write unique
native-operation-fix1-source-freeze.json after implementation/tests stabilized.
Capture final affected seven suites plus any new suite across all four pairs with
explicit pins, Ranger2.8/JDK17, cleanTest/spotlessCheck and existing evidence harness.
If ONLY tests change, original271 full regression remains a source-scoped baseline;
new affected-suite matrix on final test bytes closes coverage without repeating all
unchanged tests. Prove production source hashes identical between freezes and
report both source scopes. If ANY production source changes, rerun full optional
module on first final pair and affected suites on other three. No formatter during
final matrix. Final labels must be unique native-operation-fix1-matrix-sXXX-dXXX;
do not overwrite previous runs. Root verifies original XML/pins/hashes and dispatches
scoped independent re-review. Keep review/report revisions traceable and update
report with exact findings, new coverage, warning disposition and remaining limits.
No physical/action guard, services/live/Docker/demo/core changes, git or commits.
