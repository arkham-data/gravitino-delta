# Command-owned Delta idempotency implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Root owns documentation, reviews, build scheduling and Git; implementers do not commit.

**Goal:** Execute the native scalar lookup within one transaction owned by the retained append association, preserving original source and target authority through return.
**Architecture:** Add a finite Boolean operation to the existing association. Extract the scalar-only native operation from its standalone ownership wrapper; coordinate separate validation/state phases under the same exact bound transaction. No arbitrary continuation or transaction escapes.
**Tech Stack:** Spark 3.5.4 / 3.5.6, Delta 3.3.0 / 3.3.2, Ranger 2.8.0, Scala 2.12, JDK17.
**Spec:** docs/superpowers/specs/2026-09-27-delta-governed-append-design.md. This is a finite dependency of its command/source ownership step, not integrated append acceptance.

## Global constraints

- No Gravitino core/API/server/client/storage-schema changes; no Spark or Delta fork.
- Authenticated query subject owns source SELECT and target UPDATE. Metadata reader is never the write principal.
- Platform excludes simultaneous dataset writer jobs; observations are not an atomic policy/registration fence.
- Preserve installed append rejection, exact source occurrences/masks/filters, native state allowlists, packaging and dependency isolation.
- Current command stage remains empty write/options/analyzedQuery; reset must remain literal false. A Boolean true is not a completed or skipped append.
- Preserve existing evidence and services; no remote push, cleanup, demo modifications or broad test run. Tests use existing real-engine fixtures; transport is not live qualification.

## Review focus

1. A committed duplicate with denied/changed source authority must not return true.
2. Validation must run with the exact owned transaction active but outside transactionState native scope; retained validation opens its own finite scopes.
3. A transaction sharing a snapshot/log is not the same transaction; nested, foreign, closed and reactivated bindings must reject.
4. Source/target policy publication, registration, groups, configuration or command mutation during/after the scalar must prevent both Boolean outcomes.
5. Empty, repeated and self-source obligations survive lookup; they confer no source execution or helper-plan authority.

### Task 1: Compose the exact association and finite scalar phase

**Files:**
- Modify `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendCommandBinding.scala`.
- Modify `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerNativeIdempotency.scala`.
- Modify `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java` only for exact transaction argument validation.
- Create `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerAppendIdempotency.scala`.
- Extend existing test probes only as needed to observe actual frontend commands and real native callbacks; no production test hooks.

**Interfaces:**
```scala
// Association retains all authority; callers cannot supply substitute target/options/receipt.
def hasBeenExecuted(candidate: AppendData, session: SparkSession): Boolean
// Scalar owns only its transactionState operation; supplied binding remains live afterward.
def readInTransaction(
    owner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
    transaction: OptimisticTransaction,
    options: Map[String, String]
): Boolean
```
```java
// Package-scoped; compares candidate identity then runs the existing complete owner check.
void requireTransaction(OptimisticTransaction candidate) {
  if (candidate != transaction) throw denied();
  requireLive();
}
```

- [x] Write focused tests first. Preserve the initial compile failure for the missing API separately; establish behavioral RED with a fail-closed signature stub before implementing real behavior. A positive actual-frontend cold duplicate must require true and physical completion, so the stub cannot pass.
- [x] Reuse real native fixture seeding, installed companion and actual captured insertInto/saveAsTable AppendData roots. Do not fabricate an analyzedQuery or claim supplied command origin. Configure reset=false and session txn options before source capture. Verify duplicate=true, newApp=false, higherVersion=false, actual same transaction/snapshot, aggregate/encoder/physical completion, native state cold-to-computed and full storage equality.
- [x] Add tests for source SELECT and target UPDATE denial on duplicates before scalar execution; source mask/filter changes; original empty/repeated/self-source obligations; command/query/target copies, creator thread/session and nested scope rejection. Source execution stays closed.
- [x] Inject observable mutations through existing test observers during native scalar and during post-scalar validation. Cover source and target policy/role stamps, registration identity/publication, subject/groups, configuration and mutable command/source state. Assert each mutation fired and the precise expected denial; broad Exception is insufficient. Both true/false branches require revalidation.
- [x] Add finite scalar owner/transaction mismatch, closed/reactivated owner and exceptional cleanup checks. Check source/target validators run with the same active native transaction, never with the scalar operation still open. Preserve exception identity and fail on asynchronous listener errors.
- [x] Implement association operation: idle validation; retain exact command options; validate reset/options; open exactly one target.log.withNewTransaction with retained descriptor; create owner; private owned validation; native scalar; private owned validation; close owner in finally; after native scope closes, idle validation; return only Boolean. Private participant validation shares existing checks without weakening public idle validate. Context records creator thread and requires exact active Spark session.
- [x] Extract readInTransaction from existing helper. Derive session from owner, require exact transaction before/after, reject active native operation/reset true/invalid options, use existing Driver and finite state operation, close only that operation. Standalone hasBeenExecuted keeps its original entry, target receipt, options and cleanup behavior and delegates to this primitive.
- [x] Format scoped module, run new suite and relevant regression selectors. Store command/log/XML/source hashes and original storage bytes under this plan evidence directory. No full-module/repository run; targeted checks are the authorized verification scope.
- [x] Independent task review must pass SPEC and QUALITY; implementer fixes any findings with scoped tests before root qualifies matrix.

### Task 2: Compatibility, review and local checkpoint

- [x] Run new suite plus NativeIdempotency, AppendCommandBinding, AppendTargetReceipt, InstalledAppendBoundaries and ReadRule suites on all four exact version pairs. Add other selectors only if implementation touches their behavior or failures warrant it. Archive each before rebuilding the next pair.
- [x] Verify zero failures/errors/skips and no unexplained incomplete run, fresh runtime origins/pins, exact source/artifact hashes, storage manifests and phase traces. Document test counts versus fixture counts separately.
- [x] Independent final review of source, evidence and scope. Reuse retained live fixture for a fresh standalone-helper smoke only if refactor concerns warrant it; do not claim this new command phase is live-qualified from the prior helper result.
- [x] Record finite result and remaining source-preparation/helper/write/commit/reset/frontend tasks. Root creates local checkpoint after review; no push.

## Preflight

The existing association is inert and validates supplied early command objects; this change adds a finite scalar method, not command admission. Target/source validation uses retained native scopes, so its checks surround the scalar operation rather than nest inside it. The transaction anchor lives across those phases. Private helpers cannot return or capture an arbitrary executable continuation. Later actual writer input and native source preparation remain separate qualified phases under the approved design.

## Acceptance — October 11, 2026 UTC

The finite phase is accepted after independent source, runtime and documentation
review. All four exact version pairs passed 69 tests each with zero failures,
errors or skips; all 79 source files matched the reviewed freeze throughout.
The matrix retains 752 traced fixtures and 52,776 verified storage-manifest entries.
A fresh standalone-helper regression on Spark 3.5.6 / Delta 3.3.2 passed all four
live scenarios against Ranger 2.8.0, packaged Gravitino and MinIO, preserving all
16 original S3 objects. That live result does not qualify the new command-owned
phase as an integrated append.

See the [qualification record](../../delta-append-idempotency.md) for scope,
evidence and remaining work. Keep this checkpoint local and preserve the worktree
and evidence. The next phase must bind the actual Spark analyzedQuery and V1
writer input before source preparation, writing or native commit can be admitted.
