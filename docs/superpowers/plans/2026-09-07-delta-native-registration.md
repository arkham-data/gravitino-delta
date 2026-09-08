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

## Task 2: Production native marker and action reader

**Files:** new protocol module `catalogs/catalog-lakehouse-delta`, production package `org.apache.gravitino.catalog.lakehouse.delta.registration`, module build/settings registration and focused unit tests; `docs/delta-native-registration-results.md`. Do not alter provider or Spark fixtures in this task.

**Interfaces:** immutable `DeltaOperationMarker`, immutable `DeltaCommitEvidence` with MATCHED/AMBIGUOUS and read-only observed metadata, and `DeltaCommitActionReader`. No public synthetic MATCHED factory, NOT_COMMITTED result, Spark/Hadoop/Kernel dependency or storage calls in this component.

```java
DeltaOperationMarker.create(long id, long generation, String token,
    String normalizedLocation, String schemaJson)
DeltaOperationMarker.replace(long id, long generation, String token,
    String normalizedLocation, String schemaJson, long baseVersion, String baseUuid)
String DeltaOperationMarker.encode()
DeltaOperationMarker DeltaOperationMarker.decode(String encoded)
Map<String, String> DeltaOperationMarker.injectInto(Map<String, String> properties)
DeltaCommitEvidence DeltaCommitActionReader.inspect(
    DeltaOperationMarker expected, long candidateVersion, long latestVersion,
    long validatedSnapshotVersion, String validatedSchemaJson, InputStream commitJson)
```

- [ ] Write focused failing tests before implementation; preserve actual executed RED XML.
- [ ] Encode a bounded canonical envelope binding identity, generation/token, operation, location, schema and replacement base. Reject duplicate/unknown/missing/wrong-type fields, trailing documents, noncanonical encoding and overflow. Compare schema JSON ignoring object key order only; retain array order, nullability and metadata. This is not a full query/request digest.
- [ ] Inject `gravitino.registration.operation` into a new immutable map, preserving unrelated entries. Reject nulls and case-insensitive owned-key collisions, including repeated `option.` prefixes.
- [ ] Require actual snapshot version == candidate == latest; CREATE candidate0, REPLACE actual bound base+1 with overflow rejected. Require exactly one metadata action and one CommitInfo. CREATE accepts CREATE TABLE, CREATE TABLE AS SELECT, CREATE OR REPLACE TABLE AS SELECT; REPLACE accepts REPLACE TABLE, REPLACE TABLE AS SELECT, CREATE OR REPLACE TABLE AS SELECT. Match exact native operation names. REPLACE requires integral readVersion equal base; CREATE rejects inconsistent nonnegative readVersion. Require observed nonempty UUID (new UUID for replacement), exact marker and schemas.
- [ ] Consume the entire caller-owned stream without closing it. Stream file actions without collecting the commit; reject duplicate singleton actions/fields, malformed trailing actions and IO failure. Missing CommitInfo, inherited ALTER/WRITE, superseded candidates and mismatches yield AMBIGUOUS, with stable diagnostics and no partial publishable observation. Kernel remains responsible for snapshot/protocol validity; trusted verifier remains responsible for location I/O, base lineage and authorization.
- [ ] Cover independent snapshot-version mismatch, schema differences, marker bounds/types/canonical form, operation/readVersion/base errors, all aliases/map immutability, repeated file actions, duplicate required actions, trailing corruption, midstream IO failure and stream ownership.
- [ ] Run scoped tests/Spotless/Javadoc checks appropriate to this module, inspect production jar for absent test/synthetic classes, preserve commands/exits/XML and document this component's precise boundary. Commit and task review before native integration.

## Task 3: Joined native commit and durable recovery

**Files:** generic registration test verifier/provider extension seams; Spark3.5 Delta integration tests/helper hooks and conditional test dependencies only as needed; native results documentation. Reuse Task2 production component. No protected source edits.

- [ ] Bind a test-only verifier to trusted stored-location exact commit I/O plus public Kernel snapshot-as-of-version observation. Never convert AMBIGUOUS to synthetic MATCHED. Capture actual pre-write base and UUID lineage before BEGIN_REPLACE under complete-row OCC; ordinary DML may have advanced beyond published native.version.
- [ ] Inject marker after durable reservation and before native create/stage construction. Preserve unrelated properties/options, loaded native Delta types and replacement-stage delegation. Reject reserved aliases also in write options/ALTER entry points used by this adapter. No SQLConf mutation or marker-only commit.
- [ ] Prove native schema-only CREATE, CTAS, explicit RTAS and absent/existing CREATE OR REPLACE AS SELECT produce the expected same-commit marker, actual operation, UUID and schema. Do not silently rewrite schema-only create-or-replace routing.
- [ ] Join real H2/REST reservation, native write and publication. Inject failure after native commit before publication, verify pending hidden state/data, stop/reopen server/store and Spark, discard in-memory operation context and recover through authorized real evidence. Assert stable registration ID/location/owner/audit, observed new native UUID/schema/version, and no native version advance on exact publication replay.
- [ ] Include DML before replacement, inherited marker, wrong binding, absent CommitInfo, superseded candidate and missing lineage/log negative controls. Preserve data and fail closed; absence is not writer termination or safe rollback.
- [ ] Run all four Spark3.5.4/3.5.6 x Delta3.3.0/3.3.2 pairs on the owned local MinIO. Record exact loaded versions, JSON objects, credential-free storage settings, commands/exits and original XML. Default builds remain isolated from opt-in live endpoints.
- [ ] Run covering formatting/unit/integration checks, document actual recovery boundary and limitations, commit and task review. This is not yet a complete production CatalogProvider/runtime or deployed Ranger enforcement.

## Completion

Resolve review findings with covering checks. Run final whole-branch review and exact-HEAD clean-archive Rat; archive public evidence while retaining prior packages/worktree. A working production provider/runtime and Ranger-downloaded-policy demo remain subsequent deliverables, contingent on these qualified interfaces. Do not silently substitute a test-only scaffold for deployment acceptance.
