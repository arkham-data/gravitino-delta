# Transaction-owned Delta append source implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Root owns documentation, evidence scheduling, review and Git; implementers do not commit.

**Goal:** Connect the actual retained V1 append input, native idempotency and source preparation inside one owned target transaction, then deny before the native write helper.

**Architecture:** Extend the accepted V1 input owner with a finite input lease and synchronous coordinator. The coordinator reuses the existing transaction binding and native scalar, then prepares the actual retained QueryExecution under an explicit source owner. Same-target preparation retains Delta's transaction generator and read tracking; source execution remains forbidden in this phase.

**Tech Stack:** Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Ranger 2.8.0; Scala 2.12; JDK 17.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`, finite dependency of step 3. Base `f8f74fd62130e301752544ebb576f435d3eebd09`; actual V1 input and separate native idempotency are already qualified.

## Global Constraints

- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- Gravitino owns registration; native Delta owns its log, transaction and read tracking. Do not implement a second transaction protocol.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal. Preserve original SELECT columns, occurrences, masks and filters and target UPDATE independently, including empty/repeated/self-source inputs.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Preserve accepted SELECT, registration, packaging and standalone scalar contracts. Keep strict idle-only entry validation for existing callers.
- This phase admits only matching-schema named insertInto and existing-table saveAsTable append with an already receipted source and empty explicit write options. Retain the reset=false restriction; session transaction app/version settings remain native scalar inputs.
- No opaque writer delegation, native write helper, source-data execution, file generation, commit, native reset or successful duplicate return. Both scalar outcomes end with a distinct bounded denial after the required revalidation.
- No production test-mode flag, observer exemption, unrestricted callback, ambient transaction-based permission, or authority based only on tags, logical links, tracker identity, schema, names or fastEquals.
- Keep evidence, worktrees and services. No push, PR, demo edits or cleanup. Root commits locally after review.

## Review Focus

1. QueryExecution optimization clones its source. Bind the actual retained Dataset/QE, exact planning tracker, original receipt recipe and actual optimized return; reject a cached result or foreign/reentrant callback that cannot supply the one owned preparation.
2. Replacing a same-log transaction generator with its Snapshot loses native read tracking. Keep the exact transaction, measure predicate/file reads and native whole-table reads with skipping disabled.
3. Prepared evidence can otherwise enter the ordinary SELECT physical path. Explicitly deny user-source physical execution for this preparation-only owner, including copied/shared-tracker plans, and expire all evidence when its transaction/input lifetime closes.
4. Idempotent duplicates still require source SELECT and target UPDATE. They must not prepare or execute the input or return apparent append success, even for empty or repeated sources.
5. Validation itself opens native scopes. Keep participant validation, scalar transaction-state scope and source PREPARE scope separate; every exceptional/nested/wrong-thread exit must preserve outer state and unwind both owners.

### Task 1: Compose actual input, one transaction and native source preparation

**Files:** Production/test paths are under `spark-connector/v3.5/spark-delta-ranger/src/`, package `org/apache/gravitino/spark/connector/delta/ranger/`.

- Create `main/scala/.../DeltaRangerAppendCoordinator.scala` for the finite transaction/source coordinator; a focused additional source-owner helper is permitted if it keeps ownership boundaries clear.
- Modify `main/scala/.../DeltaRangerAppendV1Input.scala` only to mint/retain the input lease from verified insert(data,false), expose narrow owned-phase validation and enter the coordinator. Preserve actual native leaf/run lifetime and all pre-input gates.
- Modify `main/scala/.../DeltaRangerAppendCommandBinding.scala` through a distinct retained-input transaction seam; preserve capture, candidate validation and standalone hasBeenExecuted behavior.
- Modify `main/scala/.../DeltaRangerNativeScans.scala`, `DeltaRangerReadRule.scala` and `DeltaRangerPhysicalProvenance.scala` for explicit source ownership, transaction generator qualification and preparation-only physical denial. Change other physical gates only if required by the concrete existing dispatch.
- Reuse `DeltaRangerDecisionReceipt.revalidate` for scope-safe original receipt context checks where sufficient; narrowly modify `DeltaRangerDecisionReceipt.scala` only if required to retain every source engine stamp without opening nested native operations.
- Reuse `main/scala/.../DeltaRangerNativeIdempotency.scala:readInTransaction` and `main/java/.../DeltaRangerRegistrationBindings.java:BoundAppendTransaction`. A narrowly necessary ownership validation addition is permitted; do not duplicate or weaken the native scalar/registration contract.
- Create `test/scala/.../TestDeltaRangerAppendTransactionSource.scala`; keep any read-only native-state inspection in test source. Update affected V1-input fixture settings/terminal expectations and affected existing tests without weakening exact earlier denials.

**Interfaces and invariants:** The names below describe the new ownership boundaries. Private implementation names may differ if the report maps them precisely.

```scala
// Input is minted only from the running V1 owner after actual Dataset validation.
// It retains that Dataset, its QueryExecution, its analyzed root, the original
// receipt and the exact original command association. No reconstructed Dataset.
DeltaRangerAppendCoordinator.run(input: RetainedV1Input): Nothing

// Existing native primitive: reuse within the single coordinator transaction.
DeltaRangerNativeIdempotency.readInTransaction(
  owner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
  transaction: OptimisticTransaction,
  options: Map[String, String]
): Boolean

// Finite sequence, not an API accepting arbitrary user work:
// actual input -> idle validation -> owned target transaction -> revalidation
// -> native scalar -> revalidation -> false: actual QE preparation -> revalidation
//                              \-> true: skip preparation -> revalidation
// -> specific unconditional denial -> close transaction/input owners
```

The new association seam must bind the original association to the currently live input lease, native transaction and its retained descriptor. Do not call standalone hasBeenExecuted and then open another transaction. Do not expose raw target authority or a general callback that bypasses input ownership. Preserve full configuration, authenticated subject/groups, policy/role publication stamps and registration identity across phases.

Spark's public `QueryExecution.tracker` and `QueryPlanningTracker.get` are available on both pinned Spark patches. Require the exact retained tracker at the actual source pre-CBO callback, together with a single outstanding preparation owner, original optimized-prefix recipe and exact active transaction. Tracker identity is necessary, not unforgeable: another QE can deliberately reuse it. Require exactly one newly issued Prepared object associated with this owner and verify it is the evidence on the retained QE's actual optimizedPlan return. Cached/no-callback, missing, repeated and foreign results reject. Source physical admission must reject this owner even while live; tracker/tag reuse must not grant data execution.

The source owner supplies original obligations to NativeScans and requires both input and BoundAppendTransaction lifetime for native preparation and later evidence checks. Participant/action revalidation happens between native operations; lightweight lifetime checks inside native preparation must not recursively open another operation. Preserve the Snapshot branch for cross-log sources and reject unowned/foreign transaction generators. For an owned same-log OptimisticTransaction, capture its snapshot lineage under the scan operation but return the original generator to native PrepareDeltaScan. The native no-skipping route must retain readWholeTable behavior; it does not call getDeltaScanGenerator. Distinct-object same-path log aliases remain rejected by existing exact-log lineage checks unless separately qualified. Native scan caching can reuse a DeltaScan across repeated occurrences: do not require one generator callback per join occurrence.

- [ ] Write a real installed frontend test first. It must fail at the accepted input-only denial rather than the proposed post-preparation denial. Use reset=false in the fixture before source receipts are created; preserve fresh RED XML/logs.
- [ ] Implement the finite input lease, one transaction coordinator and nonduplicate source path. Test both named frontends with the actual retained input/QE, one observed transaction, scalar false, one owned source preparation, exact terminal cause, no active transaction/native scope afterwards, unchanged full SQLConf and raw storage.
- [ ] Prove same-log selective self-source and repeated self-join preserve the exact native transaction generator. Read native predicate/file sets before/after preparation using a test-only read-only accessor, never by manually invoking native read methods. Verify expected native file identities and original occurrence count independently; a deduplicated file set is not a join occurrence count.
- [ ] Prove no-skipping same-log preparation records native readTheWholeTable false-to-true. Cross-log and mixed-log sources must preserve their respective Snapshot/transaction generator branches and complete original obligations. Optimized-empty sources still revalidate all original permissions.
- [ ] Add a seeded native duplicate case for each frontend. Require scalar true on the same transaction, source/UPDATE revalidation, zero actual-input preparation and generator/index issuance, a distinct duplicate-denial result and unchanged storage. Denied/changed source, target, mask/filter and publication cases must fail at their actual authority boundary on both scalar outcomes.
- [ ] Add distinct/borrowed-tracker QE, copied-plan/tag, cached/prepared-input, changed generator/transaction, expired source evidence and physical-execution negatives. Actual-source preparation must be linked to the retained QE return; preparation cannot authorize source data execution. Record the precise first denial and assert no later phase entry.
- [ ] Extend lifetime/context mutations at the new transaction and preparation boundaries: subject/groups/settings/session/thread, source/target registration and policy changes, nested/reentrant input, foreign/closed transactions, failed scalar/preparation/participant validation. Preserve primary exception identity and healthy SELECT controls after cleanup.
- [ ] Keep reset=true specifically rejected before scalar/preparation and preserve exact old scalar option rules. Update old V1 fixture terminal expectations and explicit reset=false startup configuration where needed; no production test branch, manufactured early command, broad alternative-error assertion or reopening of an expired source owner.
- [ ] Record identity and phase evidence from actual policy/metadata callbacks, ordinary listeners and read-only test inspection of retained owner state. Production validation plus entry stack is not direct external capture of a local Dataset variable; document that observation limit. Do not introduce production instrumentation solely to create test evidence.
- [ ] Archive every attempt's full regular-file manifest and object inventory before/after, full configuration and owner cleanup. Counters observe only their instrumented seam, not global I/O/native calls. Source/helper/commit nonentry also requires source-path review. Preserve all evidence, including failed attempts.
- [ ] Run scoped spotlessApply/check, the new suite and directly affected regressions. Start with AppendV1Input, AppendCommandBinding, AppendIdempotency, NativeIdempotency, ReadRule, NoSkipping and PhysicalAdmission; include DecisionReceipt or additional native-source tests when their changed seam requires it. Record exact selected methods/counts and justify the final compatibility selector set. Do not rerun unrelated suites or the whole repository.
- [ ] Release source/build lane with a complete report, then independent spec/quality review. Resolve findings with covering tests before compatibility freeze. Do not commit; root owns the reviewed local checkpoint.

Example focused invocation (use actual new method name for the initial RED):

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
-PskipITs -PskipDockerTests=true -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
--tests '*TestDeltaRangerAppendTransactionSource'
```

### Task 2: Qualify pinned pairs, document and checkpoint

**Consumes:** Reviewed Task 1 source, exact tests and completed development traces. **Produces:** Four preserved version-pair archives and a finite local result document.

- [ ] Freeze source/build/private tooling after Task 1 review. Adapt the previous runner and exact fixture auditor to the new released source; final counts and marker names must come from actual source and fresh development output, never the previous phase's inventory.
- [ ] Run the reviewed new/affected selector set serially across Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2. Force only test-task freshness and archive each cell before rebuilding. Record actual worker JDK, full ordered classpath/agents, all Ranger 2.8.0 artifacts and runtime origins, fresh XML and source equality.
- [ ] Independently audit native transaction identity/read-state deltas, original receipt/occurrence/UPDATE obligations, exact input/QE/preparation linkage, duplicate no-preparation, specific negative causes, scope cleanup and every storage manifest/object comparison. Missing evidence leaves a cell unqualified; unchanged files alone are insufficient.
- [ ] Write `docs/delta-append-transaction-source.md`, update the prior V1 checkpoint with a historical continuation link, and state exact finite results and observation limits. Successful append, helper-derived plans/actions, commit/reset/postcommit, broader frontends and deployed Ranger/S3 still remain. CDF stays the first feature after Delta maturity.
- [ ] Final independent review, then local commit with a clean tested tree. Keep private evidence/services/worktree; no push or demo changes.

## Scope and approval continuity

This executes the already selected append architecture under the user's continuous subagent-driven authorization and latest Continue. Prior native characterization and the accepted V1/scalar checkpoints are dependencies, not work to repeat. No new successful write authority is introduced. Review/refinement of this finite plan happens during authorized implementation; routine clarification does not require another approval round.
