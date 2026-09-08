# Delta Native Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Execute task by task with scoped reviews and one Gradle owner.

**Goal:** Close registration ownership replay and prove native commit correlation/recovery as the prerequisite to a production Delta provider.

**Architecture:** Existing managed TABLE storage remains authoritative. Use an operation-local Delta metadata property and exact native log plus Kernel snapshot evidence; never infer correlation from latest token alone. A dedicated lakehouse-delta component owns reusable production protocol code; experimental adapters remain labelled until complete runtime acceptance.

**Tech Stack:** Java8-compatible source on JDK17; Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, H2/real HTTP/JCasbin, MinIO/S3A; Ranger2.8 remains the policy target.

**Spec:** docs/superpowers/specs/2026-09-07-delta-native-registration-design.md

## Global Constraints

- No core/API/server/client/storage-schema source edits; no generic external-registration behavior change.
- Spark3.5.4/3.5.6 x Delta3.3.0/3.3.2, local S3; Ranger2.8.0 remains required for integrated authorization.
- Gravitino owns registration. Token/marker is correlation, not actor authority or a stale-writer fence.
- No production synthetic MATCHED or boolean handoff wiring; no unsafe global SQLConf mutation or post-commit marker writes.
- Java8 APIs, normal imports, Javadoc on public/protected declarations, Nullable/resources/SLF4J/repository formatting.
- Preserve original XML, commands/exits, actual runtime versions, failure provenance and limitations. Keep private credentials out of logs/Git/evidence.
- Serialize Gradle. Commit scoped code only; coordination stays local and untracked. Review each task, then final branch. No PR/push/merge/shared deployment.

## Task 1: Ownership replay and parent lifecycle boundaries

**Files:** existing generic-module registration test fixture, lifecycle and REST tests; new parent-lifecycle test if isolation warrants it; docs/delta-registration-ownership-results.md. No production source changes in this prerequisite fix.

**Interfaces:** Preserve RegistrationCommand envelope and PUBLISH replay. Change create retry to accept only State.RESERVED. Use org.apache.gravitino.exceptions.ForbiddenException for provider actor/recovery denial. Preserve raw-row store/owner/data invariants.

- [ ] Add real REST delayed RESERVE replay after synthetic publication and owner transfer. The original actor retains schema CREATE_TABLE/use but loses table ownership/MODIFY. Capture the unsafe original result in original XML. Retain exact stored row/owner before and after.
- [ ] Apply the narrow retry condition and mapped exception type:

```java
if (!command.encode().equals(value(current, "request"))
    || Long.parseLong(value(current, "generation")) != 0
    || state(current) != State.RESERVED) {
  throw new IllegalStateException("Name is already reserved with different content or state");
}
// Provider actor/recovery denial:
throw new ForbiddenException("Token is not recovery authority");
```

- [ ] Update the existing published-reserve unit expectation to rejection; assert full row unchanged. Keep missing-owner RESERVED retry repair green. Verify authorized completed PUBLISH replay causes no verifier/native call, and old-actor replay after transfer is denied by existing permissions.
- [ ] Assert provider-reaching foreign actor now yields typed client ForbiddenException and actual HTTP403, while the use-only outsider remains denied before provider entry.
- [ ] Characterize parent guard routing with pending/published and malformed raw rows, catalog force/non-force, metalake force, and disabled-parent cases. Existing schema tests already cover every valid state; do not duplicate their full Cartesian product. Preserve full raw rows, owners and data; explicitly record any parent enabled-state or partial-cleanup side effects. Do not patch managers.
- [ ] Run focused new RED first, then TestDeltaRegistrationLifecycle, TestDeltaRegistrationRest and any new parent class with -PskipITs, scoped Spotless. Copy nonzero XML immediately into build/delta-native-registration-evidence/task-1, document results, commit and task review.

## Task 2: Exact native marker and durable recovery

**Files:** new protocol component under catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/registration, its module build/tests and settings include; generic registration test verifier/provider extension seams; Spark3.5 Delta integration tests/helper hooks and conditional test dependencies only as needed; docs/delta-native-registration-results.md.

**Interfaces and implementation order:** Use production `DeltaOperationMarker` (immutable CREATE/REPLACE request), `DeltaCommitEvidence` (MATCHED/AMBIGUOUS verdict and observed metadata), and `DeltaCommitActionReader` (streaming exact-commit action validation). `DeltaOperationMarker.create(id,generation,token,location,schemaJson)` has absent base; `.replace(id,generation,token,location,schemaJson,baseVersion,baseUuid)` requires a real base. Expose encode/decode and `injectInto(Map<String,String>)` with property key gravitino.registration.operation and strict owned-key/option-alias collision rejection. Bound and canonically validate the envelope; preserve immutable/null-rejecting values. `DeltaCommitActionReader.inspect(expected,candidateVersion,latestVersion,validatedSchemaJson,InputStream)` consumes a trusted stored marker and public-Kernel-validated snapshot schema/version; it does not itself make caller input authoritative. The enclosing verifier owns filesystem/Kernel reads and Gravitino actor/OCC checks. Correlation accepts authoritative expected marker/base plus actual commit actions and public Kernel snapshot validation, returns MATCHED only with observed UUID/schema/version; other verdicts remain fail closed. Test-only adapter binds it to existing RegistrationCommitVerifier and complete-row OCC publication. Replacement base is refreshed through trusted evidence before BEGIN_REPLACE persistence. No fabricated values or synthetic outcome fallback.

- [ ] Define marker/value/parser APIs and focused unit tests for exact bindings, malformed input, duplicate/inherited actions and aliases. Code assertions must verify concrete evidence:

```java
assertEquals(0L, observedVersion);
assertEquals(expectedEnvelope, metadataConfiguration.get("gravitino.registration.operation"));
assertEquals(1, metadataActionCount);
assertEquals(expectedSchema, observedSchema);
assertNotEquals(oldNativeUuid, replacementNativeUuid);
```

- [ ] Implement marker injection at native create/stage construction, after durable reserve. Keep all original properties/options except reject owned collisions; preserve native table types and replacement delegation. Do not mutate SQLConf or append marker-only commits.
- [ ] Read exact candidate JSON and validate the same Kernel snapshot; enforce CREATE0 or capturedbase+1, latest==candidate, native operation/schema/UUID/base bindings. Store actual replacement base durably, including an ordinary append before replacement case. Required missing/uncertain logs yield AMBIGUOUS.
- [ ] Join real reservation/native write/publication with injected after-commit failure, store/server+Spark reopen and authorized recovery from real evidence. Assert hidden durable state during failure, retained data, exact observed identity and no native version advance on replay.
- [ ] Run meaningful negative controls from the spec, then all four target pairs on MinIO. Capture exact code-source versions, JSON commit filenames/content, effective storage settings without credentials, commands/exits/original XML. Default builds remain isolated from opt-in live tests.
- [ ] Inspect production protocol artifact for test/synthetic classes, run covering formatting/unit/integration checks, document actual boundaries, commit and task review.

## Completion

Resolve review findings with covering checks. Run final whole-branch review and exact-HEAD clean-archive Rat; archive public evidence while retaining prior packages/worktree. A working production provider/runtime and Ranger-downloaded-policy demo remain subsequent deliverables, contingent on these qualified interfaces. Do not silently substitute a test-only scaffold for deployment acceptance.
