# Scoped re-review: native operation fix round 1

Follow the SDD scoped re-review template. This is findings-only review plus new
breakage introduced by the fix; do not repeat the full original review or broaden
into physical/action enforcement. Worktree is delta-spark-feasibility. Read
`task-4-native-operation-fix-1-brief.md`, the fix section of
`task-4-native-operation-report.md`, then the fix diff once:
`task-4-evidence/native-operation-fix1-review-package.diff`.

Fix base is original frozen uncommitted source under git HEAD993f5a9c9,
manifestSHA2560b557168c2d9bc11a04b614fd33c3b74ecd621d5b7284ef8e29bc386b642d9dc;
head is the later native-operation-fix1-source-freeze.json. The package includes
before/after hashes and complete new files. Root must confirm production/build
source identical across freezes; a production change requires revised review scope
and full first-pair regression. Do not run git, Gradle, tests, services, source
edits or subagents. You may write only `task-4-native-operation-fix-1-review.md`.

The original full regression271invocations and second affected54 remain archived
with their exact original test source; never relabel them as final test bytes.
New final affected matrix is59invocations per pair across Spark3.5.4/3.5.6 x
Delta3.3.0/3.3.2, Ranger2.8.0/JDK17. Matrix may still run during review; root will
supply the proofs as ready. Verdict pending evidence precisely and let root close
the explicit final evidence gate. Existing focused XML covers new controls with
subsequent mapping-only refinement separately recorded. No warning-free claim for
historical runs; fixture checkpoint deprecation is a Minor finding below.

Global constraints: optional module only, no core/API/server/client/storage-schema
change; Gravitino-owned canonical registration, separate metadata/query identity,
actual native Delta objects and public/protected seams. No feature widening,
physical/action guard installation, live Ranger/Docker/demo changes or complete
native-index serialization claim. No no-skipping query may change its setting to
manufacture support. Real compatible head changes must preserve native selection;
native compatibility failures need exact native boundary evidence.

Findings copied verbatim from original review:

- **R1 — required no-skipping query shapes are unqualified.** `TestDeltaRangerNativeOperations.scala:711` runs an asymmetric self-join and an ordinary unpartitioned projection with skipping disabled, plus file-list/size observations. It never runs the audit-mandated two-catalog join, quoted name-mapped column, or partitioned selection through `DeltaRangerLogIndex` (`task-4-native-no-skipping-scope-audit.md:50`, `:53`). Existing positives under the prepared-index path cannot qualify this separate replacement/callback implementation. Add installed tests with skipping remaining false, real bound mapped/partitioned tables, exact expected rows and complete original obligations; observe the owned no-skipping index/callback and assert partition/data filter preservation. Include them in the affected four-pair qualification.
- **R2 — the no-skipping continuation's distinct selection and authority boundaries lack their required controls.** `TestDeltaRangerNativeOperations.scala:711` checks `inputFiles`, a cold size aggregate, empty added serialization fields and `assertNotSame(index, index.copy())`. It does not prove that a newer actual native head is selected and captured, that superclass schema/column-mapping incompatibility still rejects, or that explicit no-skipping index substitution/cross-session authority is rejected at its provenance boundary. It also does not exercise failure cleanup or overlapping independent invocations of this continuation. Those are explicit gates at `task-4-native-no-skipping-scope-audit.md:51`–`:54`; the prepared-index controls at `TestDeltaRangerNativeOperations.scala:571` and `:637` cover different continuation code. Add focused controls at the correct boundaries: use `Prepared.verify` (`DeltaRangerNativeScans.scala:145`) for a no-skipping plan with an equal bare copied index/relation and for a different session; prove ordinary logical/physical clones retain the owned backing index. Use the owned callbacks (`:304`, continuation at `:344`) for returned snapshot identity after a compatible head advance, native incompatibility rejection, unchanged filters, closed/failed-owner rejection, exception cleanup and independent callbacks. Retain the original registration binding throughout. These are missing required tests, not claims that the current implementation demonstrably violates those properties. Full native-index graph serialization remains expressly unclaimed; the transient-field and inert-reference evidence is accepted with that limit.

#### Minor

- `TestDeltaRangerNativeOperations.scala:297` uses the deprecated native `checkpoint()` fixture API and emits a new Scala deprecation warning. Prefer an equivalent supported checkpoint entry point on both pinned patches, or narrowly document/suppress the deliberate fixture-only use if no suitable public alternative exists. This is not a production Java `-Werror` failure: the verified full build exited zero. Baseline Gradle deprecation/JVM noise is separate and does not justify describing the run as warning-free.


Return per-finding ADDRESSED/NOT ADDRESSED with file:line evidence, new fix-induced
Critical/Important/Minor issues, non-blocking out-of-scope observations, and round
verdict. Inspect changed tests for genuine installed behavior, precise negative
boundaries and test assumptions; passing generic errors is insufficient.
