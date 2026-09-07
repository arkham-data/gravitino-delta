# Delta Registration Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. The user has authorized this approach and proceeding; do not ask again between tasks. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish executable, durable registration state transitions using existing Gravitino interfaces, and a separate fail-closed native log inspection contract.

**Architecture:** A test-only ManagedTableOperations adapter stores the complete operation record in one existing TableEntity. Pure EntityStore.update callbacks validate identity/token/generation against real relational optimistic concurrency. Native evidence inspection is separate from state mutation and does not replay commits.

**Tech Stack:** Java8-compatible sources on JDK17, JUnit, existing embedded H2 relational EntityStore, Delta Kernel3.3.0/3.3.2, Gradle8.2. Spark3.5.4/3.5.6 and Ranger2.8.0 remain downstream integration targets.

**Spec:** [Registration experiment design](../specs/2026-09-07-delta-registration-lifecycle-design.md).

## Global Constraints

- No Gravitino core/API/server/client/storage-schema changes and no second table registration authority.
- All lifecycle implementation in this package stays in the existing generic module's test sources; no production provider registration/distribution.
- Full Delta eventual acceptance covers Spark3.5.4/3.5.6 × Delta3.3.0/3.3.2; Ranger2.8.0 is required.
- Ranger owns column permissions/masks/filters; existing Gravitino privileges stay unchanged.
- S3 and platform single-writer exclusion remain deployment constraints; H2/local logs are not S3 evidence.
- Real file-backed H2 with cache disabled, actual close/reopen, no mock-store durability claims.
- No native IO/commit or other external side effects inside EntityStore.update callbacks.
- No fabricated CAS/guarded-delete API, no automatic timeout takeover, no read-check-delete cleanup.
- MATCHED evidence in state-machine tests is explicitly synthetic until native operation correlation is separately proved.
- Java public/protected declarations and overrides require Javadoc; use Nullable where applicable, standard imports/SLF4J, TestXxx JUnit naming.
- Preserve exact commands, exit codes, nonzero XML test counts and dependency selections. Historical Ranger2.5/2.4 evidence is not target acceptance.

---

## Task 1: Durable single-table lifecycle contract

**Files:**
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/TestDeltaRegistrationLifecycle.java`.
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/RegistrationLifecycleFixture.java` for test-only ManagedTableOperations adapter and pure state updates.
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/RegistrationCommand.java` for immutable validated command/expected-state values and versioned property encoding.
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/RegistrationCommitVerifier.java` for immutable evidence/result types and verifier interface.
- Create `docs/delta-registration-lifecycle-results.md` for observed results and limits.
- Use existing module dependencies; do not change shared production sources/build.

**Interfaces:**
- Consume actual `EntityStore.put(entity,false)`, `get(ident,TABLE,TableEntity.class)`, `list(namespace,TableEntity.class,TABLE)`, and `update(ident,TableEntity.class,TABLE,Function<TableEntity,TableEntity>)`.
- Fixture extends `ManagedTableOperations`; implement `store()`, `schemas()`, `idGenerator()` from injected values. Override existing create/alter/list/load as required for explicit test command transport; `tableExists` uses existing default behavior.
- Use test command key `delta.registration.command` and persisted prefix `delta.registration.internal.`. Version `1` only. Transport uses existing create properties and `TableChange.setProperty` for alter. Reject mixed commands/ordinary edits and direct persisted-state inputs. Use DTO/value classes compatible with Java8, not records.
- Define `RegistrationCommand.reserve(String token, String location, String schemaFingerprint)`; `publish(long expectedId, long generation, String token, long candidateVersion)`; `beginReplace(long expectedId, long generation, String token, String replacementToken, String schemaFingerprint)`; `abort(long expectedId, long generation, String token)`. Actor comes from injected principal supplier, never an untrusted command actor string. Values encode/decode a bounded versioned property envelope. Candidate-version `-1` means unspecified; reject other negative values.
- Define verifier `Result verify(TableEntity storedReservation, long candidateVersion)`; result kind MATCHED/NOT_COMMITTED/AMBIGUOUS, immutable native version/UUID/schema observation plus explicit reason. MATCHED requires the observation; AMBIGUOUS may retain a read-only observation for diagnostics without granting publication; NOT_COMMITTED may omit it. Keep observation separate from the correlation verdict so Task2 can assert parsed metadata without manufacturing MATCHED. MATCHED construction belongs to trusted verifier/test code, not input command parsing.
- Expose a test receipt through returned provider Table properties containing `delta.registration.receipt.id`, `.generation`, `.token`, `.state`. It is a reservation receipt, not completed Spark CREATE. Persist first StringIdentifier allocated/injected by the test simulating the dispatcher; exact retry ignores a new injected candidate ID.
- State enums RESERVED, PUBLISHED, REPLACING, ABORTED, QUARANTINED. New generation per begin-replace. Store full prior publication needed to restore verified precommit failure; preserve same entity ID/audit/location. Terminal states have no automatic delete/name reclaim in this task.

- [ ] **Step 1: Create executable RED tests with a minimal stub fixture.** Use embedded file-H2 setup from `TestGenericCatalogOperations`, including actual store bootstrap and cache-disabled settings. Reuse production ManagedSchemaOperations for namespace creation. Initialize only test-local global settings and restore any mutated singleton fields on teardown. Add named tests:
  - `testReserveSurvivesReopenAndIsHidden`: create receipt, assert provider list/load/exists excludes it, raw store contains expected ID/token/location/actor; close/reopen H2 and repeat.
  - `testExactRetryRetainsFirstDispatcherIdentity`: same logical command with a different injected StringIdentifier returns original ID and unchanged generation; changed actor/token/location/schema rejected without mutation.
  - `testFinalizeRequiresTrustedEvidence`: NOT_COMMITTED/AMBIGUOUS cannot publish, client persisted-state/committed flags rejected, synthetic MATCHED publishes actual schema and same identity, exact replay returns same result.
  - `testReplacementAndStaleGeneration`: blocks new loads while REPLACING, preserves registration ID/location/create audit, allows native UUID/schema change on synthetic MATCHED, rejects stale ID/token/generation; verified absence restores prior publication.
  - `testAbortAndAmbiguityNeverDelete`: terminal hidden state survives reopen, no EntityStore.delete call/data delete/native commit; foreign principal denied even with correct token, configured recovery principal still requires verifier.
  - `testOptimisticConflictDoesNotLeakCandidate`: two barrier-synchronized actual store.update callbacks read same base version, one commits and one loses; only winner properties/columns persist. Use latches/future timeouts for determinism, no sleeps as synchronization and no mocked store.
  - `testConditionalDeleteAndTransactionLimits`: executeInTransaction rejects on pinned relational store; describe absence of caller-expected-ID TABLE delete in evidence rather than implementing unsafe cleanup.

The essential update pattern is the existing API, with all expectations checked inside the callback:

```java
TableEntity changed = store.update(
    ident, TableEntity.class, Entity.EntityType.TABLE,
    current -> {
      validateExpectedState(current, command);
      return applyVerifiedTransition(current, command, verifiedEvidence);
    });
```

Define those two private helpers in RegistrationLifecycleFixture. `verifiedEvidence`
is read before the callback; validation rechecks the exact stored generation/base
that was inspected. It must reject if that state changed during verification.

- [ ] **Step 2: Run RED on JDK17.**

```bash
./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs --tests '*TestDeltaRegistrationLifecycle'
```

Capture actual executed failure. Compiler/dependency/startup failures are setup,
not intended behavior. Use `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` in this workspace. No concurrent Gradle with Ranger validation.

- [ ] **Step 3: Implement only the test fixture protocol.** Reserve uses unique insert and immutable fingerprint validation; update uses the callback above. Normalize/validate locations before storing and exclude injected identity from fingerprint. Version/state codecs reject malformed or unsupported input. List/load delegate only for PUBLISHED and never delete pending rows. Verify current actor or explicit recovery principal; token alone is insufficient. Tests inject synthetic commit verifier results and label them as such. A losing OCC call propagates a typed conflict; caller may retry after re-reading, never replay native work.

- [ ] **Step 4: Run GREEN and focused existing regressions.** Run the lifecycle class, `TestGenericCatalogOperations`, and `TestDeltaTableOperations`; format/check the module. Save XML before later tasks overwrite it. Include one actual close/reopen failure boundary before publish and another after publish/response loss. Record test counts and actual cache outcomes.

- [ ] **Step 5: Write observed results, self-review, commit.** State exact durability/OCC/visibility limits, no REST/owner-hook/native/S3 proof. Commit test fixture + results only. Independent task review follows before next implementer.

## Task 2: Actual Delta-log inspection that fails closed without operation correlation

**Files:**
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/TestDeltaRegistrationCommitEvidence.java`.
- Create `catalogs/catalog-lakehouse-generic/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/KernelRegistrationCommitInspector.java` as test-only read-only log inspector implementing Task1 verifier contract.
- Modify `catalogs/catalog-lakehouse-generic/build.gradle.kts` only for optional test Kernel patch selection.
- Update `docs/delta-registration-lifecycle-results.md`.

**Interfaces:**
- Consume Task1 `RegistrationCommitVerifier.Result` and TableEntity stored location/schema/base/token fields.
- Use actual `DefaultEngine.create(Configuration)`, `Table.forPath(engine, storedLocation)`, `getLatestSnapshot(engine)`, `Snapshot.getVersion(engine)`, `getSchema(engine)`.
- Public Kernel Snapshot3.3.0/3.3.2 exposes version/schema but no UUID/configuration accessor; leave unknown UUID/correlation explicit rather than casting internal classes. Separate observation (version/schema) from operation-correlation MATCHED evidence. Valid uncorrelated logs return AMBIGUOUS, never MATCHED. Missing log may yield NOT_COMMITTED only as an observation; this alone does not prove a stale writer cannot commit later. Corrupt/read-denied/mismatched logs fail closed with explicit diagnosis.
- Test-only Gradle property `deltaRegistrationKernelVersion` accepts only3.3.0/3.3.2; omitted preserves existing3.3.0. Align only io.delta Kernel artifacts on module testCompileClasspath/testRuntimeClasspath. Pass required expected version to test JVM and execute jar-source/version assertion. Do not upgrade global libs.versions or start Spark in this fixture.

- [ ] **Step 1: Add RED with actual local Kernel data.** Reuse `CatalogGenericCatalogDeltaIT` local native transaction pattern:

```java
Engine engine = DefaultEngine.create(configuration);
Table nativeTable = Table.forPath(engine, path);
nativeTable.createTransactionBuilder(engine, "registration-evidence-test", Operation.CREATE_TABLE)
    .withSchema(engine, schema)
    .withPartitionColumns(engine, Collections.emptyList())
    .build(engine)
    .commit(engine, emptyRowIterable());
```

Import `io.delta.kernel.Table`, `io.delta.kernel.data.Row`,
`io.delta.kernel.utils.CloseableIterable`, and `CloseableIterator` normally.
Implement the empty commit input exactly as the existing fixture does:

```java
private static CloseableIterable<Row> emptyRowIterable() {
  return new CloseableIterable<Row>() {
    /** {@inheritDoc} */
    @Override
    public CloseableIterator<Row> iterator() {
      return new CloseableIterator<Row>() {
        /** {@inheritDoc} */
        @Override
        public void close() {}
        /** {@inheritDoc} */
        @Override
        public boolean hasNext() { return false; }
        /** {@inheritDoc} */
        @Override
        public Row next() { throw new NoSuchElementException("Empty iterator"); }
      };
    }
    /** {@inheritDoc} */
    @Override
    public void close() {}
  };
}
```

Write `testValidSnapshotWithoutCorrelationIsAmbiguous`, `testMissingCorruptAndForeignLogsCannotPublish`, and `testCandidateVersionCannotOverrideStoredLocation`. Validate actual returned native schema/version and that no publication occurs. Record runtime Kernel jar versions. A fake JSON fixture alone is insufficient for valid-log proof; corrupted-copy fixture is appropriate for error handling.

- [ ] **Step 2: Execute RED, then implement read-only inspector.** Read stored path only, close native iterators/resources, inspect schema/base/candidate bounds, translate absent/invalid/uncorrelated evidence explicitly. Do not write an operation marker or native commit during verification. The valid fixture still returns AMBIGUOUS until a separately proven marker exists.

- [ ] **Step 3: Run both Kernel versions and existing lifecycle tests.**

```bash
./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs -PdeltaRegistrationKernelVersion=3.3.0 --tests '*TestDeltaRegistrationCommitEvidence' --tests '*TestDeltaRegistrationLifecycle'
./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs -PdeltaRegistrationKernelVersion=3.3.2 --tests '*TestDeltaRegistrationCommitEvidence' --tests '*TestDeltaRegistrationLifecycle'
```

Preserve per-version XML and testRuntimeClasspath dependency reports. Reject
unsupported property input and verify omitted property resolves baseline3.3.0.
Run module formatting/checks; rerun only if behavior changed.

- [ ] **Step 4: Commit and review.** Results must distinguish H2 lifecycle, actual
Kernel observation, synthetic matched evidence, and unproven native Spark marker
correlation. No production provider scaffold. Final review covers both tasks and
Ranger2.8.0 result linkage before claiming this work package complete.

## Next gate after this plan

Build a test-only managed-provider dispatcher/REST harness to prove actual ID
injection, ownership, command validation, pending authorization, owner-hook
failure, schema cascade and metadata visibility. Then prove native operation
correlation and administrative fencing before production provider packaging.
Failure of a no-core interface assumption changes the design, not the required
full support scope. No merge/PR/deployment is authorized by this plan.
