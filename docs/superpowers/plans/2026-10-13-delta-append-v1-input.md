# Actual Delta V1 writer input implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Root owns documentation, reviews, build scheduling and Git; implementers do not commit.

**Goal:** Bind and validate the actual Dataset delivered by Spark to its native V1 append execution, with an unconditional terminal denial before any write.

**Architecture:** Retain the original source SELECT obligations and registered target UPDATE association across native analysis and optimization. An explicit companion planner issuer validates the finite command shell, uses native-only planning to obtain the real V1 leaf, and replaces its opaque writer with an owned, nondelegating terminal writer. A physical owner brackets the entire native leaf execution, including Dataset construction, and revokes its transient capability on every exit.

**Tech Stack:** Spark 3.5.4 / 3.5.6, Delta 3.3.0 / 3.3.2, Ranger 2.8.0, Scala 2.12, JDK 17.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`, finite dependency of step 3. Append execution remains incomplete.

## Global constraints

- No Gravitino core/API/server/client/storage-schema changes; no Spark or Delta fork.
- Source SELECT and target UPDATE use the authenticated application subject. Preserve original source occurrences, columns, masks and filters, including empty, repeated and self-source plans.
- Platform excludes simultaneous dataset writer jobs. Validation is observational, not an atomic policy/registration fence or an arbitrary-code sandbox.
- Preserve accepted SELECT, native scalar ownership, registration, dependency and packaging boundaries.
- No native opaque writer delegation, transaction, idempotency helper, data execution, file generation, reset or commit in this phase. A checked actual input always ends in a specific SecurityException.
- No production test-mode flag, arbitrary continuation, foreign rule exemption, or authority derived solely from copied tags/logical links, schema, names or fastEquals.
- Keep evidence, worktrees and services. No push, PR, demo edits or cleanup. Root alone commits after review.

## Review focus

1. Spark stores analyzedQuery after post-hoc resolution; bind the actual final stored source rather than assuming the optimized child is the fallback input.
2. V2Writes creates an opaque writer after pre-CBO. A replay creates another writer and cannot prove original builder provenance. The finite shell has one inert write hole; never delegate through it or claim its origin authenticated.
3. Dataset.ofRows can fail between toInsertableRelation and insert. Lifetime brackets the entire native leaf run and closes on all failures, repeat calls and nested calls.
4. Foreign analyzer/planner/optimizer/columnar rules must not issue append authority. Do not claim the companion callback executes before an earlier foreign resolution callback.
5. Input validation preserves original source obligations and full subject/configuration/registration/publication state; observational equivalence cannot erase original obligations.

### Task 1: Own actual native fallback input, deny before writes

**Files:** All Scala paths below are under `spark-connector/v3.5/spark-delta-ranger/src/`.

- Create `main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendV1Input.scala` for finite phase ownership and terminal input validation; split focused helpers if justified.
- Modify `main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerSparkExtension.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerPhysicalPlanning.scala`, `DeltaRangerPhysicalProvenance.scala`, and `DeltaRangerColumnarRule.scala` only for exact phase dispatch and inventory qualification.
- Extend `DeltaRangerAppendCommandBinding.scala` only through a distinct stage interface; preserve existing early-object capture/validate/scalar identity contract.
- Create `test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerAppendV1Input.scala` and a focused probe if necessary.
- Update affected assumptions in `TestDeltaRangerAppendCommandBinding.scala`, `TestDeltaRangerAppendIdempotency.scala`, `TestDeltaRangerInstalledAppendBoundaries.scala` and `InstalledAppendBoundaryProbe.scala`. Keep cold scalar evidence genuinely cold.

**Interfaces and invariants:**

```scala
// Conceptual phase flow; implement private, finite typed owners, not a generic callback API.
// original resolved AppendData + original receipt -> final analyzedQuery witness
// -> qualified pre-CBO source witness -> qualified optimized command shell
// -> native-only AppendDataExecV1 -> owned terminal V1Write
// -> native SupportsV1Write creates actual Dataset -> validate -> unconditional denial
```

The planner must authenticate exact target/catalog/descriptor/options/stored source and qualified source transformation independently of the opaque write field. Capture the actual opaque writer identity once for later substitution checks, never invoke it. Obtain the native leaf using a qualified SparkPlanner that excludes the companion strategy to prevent recursion; preserve native plan/table/refresh identities. A later write coordinator must own native builder/helper construction and cannot upgrade the inert hole into authority.

The physical wrapper owns a single-use, creator-thread/session-bound scope around `nativeLeaf.run()` with finally cleanup. Its terminal writer checks scope, overwrite=false, actual Dataset session, analyzed source recipe, complete attributes/schema and original authority before throwing a unique terminal exception. No Dataset or executable authority escapes through diagnostics.

- [x] Add the real frontend test first; preserve behavioral RED showing the current derived/reused denial instead of the expected terminal input denial. Use a normally registered QueryExecutionListener to observe failed command analyzed/optimized/physical plans without altering rule inventories.
- [x] Admit only matching-schema named `insertInto` and existing-table `saveAsTable(..., append)` with an already receipted source and empty write options. Native casts/renames, literal SQL, SQL INSERT SELECT, writeTo, overwrite, CTAS/RTAS, streaming and raw paths remain specifically denied.
- [x] Implement explicit original/final-analysis/pre-CBO/planner/input ownership. Frozen full SQLConf, subject/groups, exact original receipt and target association, policy/role stamps and registration bindings must survive all phases. Qualify rule inventories before granting authority; account honestly for callback ordering limits.
- [x] Validate both real frontends reach the unique terminal insert failure through native SupportsV1Write/V1FallbackWriters. Inspect failed QueryExecution for actual stored source, writer and physical target/plan/refresh transitions. Terminal stack plus validation proves input entry; it is not direct capture of the local Dataset object.
- [x] Test denied UPDATE/SELECT, changed mask/filter, publication, registration, subject/groups/configuration during input revalidation; original empty/repeated/self-source obligations; exact query/stored-source/target/options/writer/refresh substitution; copied tags/links, cloned commands, missing/ambiguous witnesses; wrong session/thread, nested/repeated/expired/serialized ownership, and cleanup after Dataset construction failure.
- [x] Test relevant foreign inventory rejection and existing SELECT before/after failures. Record the exact rejection stage; never accept unrelated early failure as a mutation test pass.
- [x] Hash/archive all fixture storage files before/after each attempt, inventory directories if claiming all filesystem objects unchanged, assert no active transaction/native operation and unchanged full SQLConf. Structural call-path review complements measured storage and runtime evidence; unused manual-driver counters are not global native-call measurements.
- [x] Run scoped formatting and the new suite, then affected AppendCommandBinding, AppendIdempotency, InstalledAppendBoundaries, AppendTargetReceipt, PhysicalAdmission, ReadRule, DecisionReceipt and NativeIdempotency suites. Do not run the whole repository. Preserve commands, exit statuses, test XML and trace artifacts.
- [x] Independent task review of source, tests and evidence. Fix findings with covering tests before compatibility freeze.

Command shape (JDK 17, existing Gradle setup):

```sh
./gradlew :spark-connector:spark-delta-ranger-3.5:test -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --tests '*TestDeltaRangerAppendV1Input'
```

### Task 2: Freeze, qualify compatibility, document and checkpoint

**Consumes:** Reviewed Task 1 source and finite installed-input evidence. **Produces:** Four version-pair archives and an accurate local checkpoint record.

- [x] Freeze source hashes and run the new suite plus the affected regression selectors across all four exact Spark/Delta pairs. Serialize builds and archive each pair before the next.
- [x] Audit tests/failures/errors/skips, fresh runtime JDK/Scala/Ranger artifacts and origins, source hashes, full storage equality and phase evidence. Distinguish tests, fixtures and manifest entries. No live S3/Ranger claim from local transport fixtures.
- [x] Write `docs/delta-append-v1-input.md` with exact finite result, writer-origin and observation limits, remaining coordinator/native-helper/action/commit/reset/frontend work and CDF priority after Delta maturity.
- [x] Independent final review, then local commit only after all required checks pass. Preserve all evidence and services; do not push.

## Preflight and scope decisions

The existing architecture and latest Continue authorize this finite implementation without another approval round. A captured opaque writer is inert input, not proof of native builder origin. Direct observation of the Dataset local is not available through existing public listeners; entry stack plus production validation is the acceptance claim. Unknown earlier programmatic analyzer callbacks may run before companion inventory checks, but cannot receive append authority. These limits do not permit writes or weaken SELECT admission.
