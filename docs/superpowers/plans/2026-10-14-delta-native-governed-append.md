# Native governed Delta append implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Root owns review, compatibility scheduling, documentation and commits. Preserve private evidence.

**Goal:** Enable successful governed named batch append through native Delta helper/action generation and commit, preserving source policies and target UPDATE authorization.

**Architecture:** Extend the retained V1 input and transaction coordinator at `9d907bf30`. Use Delta public builders to establish expected helper source projections and analysis-only statistics/constraint plans, retain exact transaction-owned scan evidence, invoke native `writeAndReturnCommitData`, revalidate, and pass its unchanged actions/tags to native `commitIfNeeded`. Preserve native inner and Spark outer cache refresh semantics.

**Tech Stack:** Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Ranger 2.8.0; Scala 2.12; JDK 17.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`. This continues its selected architecture under the user's instruction to proceed; previous preparation-only checkpoints are dependencies, not the final outcome.

## Global Constraints

- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- Gravitino owns registration; Delta owns log, read tracking, file actions and commit semantics. Never reconstruct actions or replace the transaction protocol.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal.
- Preserve original source SELECT columns, occurrences, masks, filters and publication/registration checks separately from target UPDATE. Empty and duplicate operations require authorization.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Retain currently qualified matching-schema, named insertInto and existing-table saveAsTable append, empty explicit options, reset=false and existing feature exclusions. Do not change native effective options or disable statistics/default behavior to obtain passing tests. Wider options, reset=true and frontends remain required subsequent work.
- No opaque writer delegation, ambient allow flag, stack-name authorization, production test bypass, or authority based only on tags, tracker identity, schema or names. Every helper execution must validate its complete expected plan and live owner.
- Native generated files may remain after precommit denial; distinguish them from committed data. Preserve native exceptions and never replay or delete possibly committed work after postcommit failure.
- Keep evidence, worktrees and services. No push, PR, demo edits or cleanup. Root commits locally after independent review and required verification.

## Review Focus

1. Full-schema inputs still receive a native default projection with Spark's default null-as-default setting; support that actual path, not a settings workaround.
2. Statistics and CHECK-expression analysis can use LocalRelation plans but must not grant arbitrary user LocalRelation or expression execution.
3. Same-table input must retain native OptimisticTransaction read tracking through helper execution; cross-table and repeated occurrences keep original policy obligations.
4. Revocation after file generation must prevent log publication, with generated objects retained and separately reported.
5. Successful commit changes native state: precommit validation must not be reused after commit to misclassify committed work as entry denial or trigger retry.

### Task 1: Execute native helper and commit under retained authority

**Files:** Under `spark-connector/v3.5/spark-delta-ranger/src/`, package `org/apache/gravitino/spark/connector/delta/ranger/`.

- Modify `main/scala/.../DeltaRangerAppendCoordinator.scala`, `DeltaRangerAppendV1Input.scala`, `DeltaRangerAppendCommandBinding.scala` to return normally on successful native commit or authorized native duplicate, retain Dataset access narrowly, and preserve owner lifetime through native helper and commit.
- Create `main/scala/.../DeltaRangerAppendHelper.scala` for helper plan reconstruction/admission/lifetime. A separate focused helper-plan witness file is permitted if needed; do not create a general write framework.
- Modify `DeltaRangerReadRule.scala`, `DeltaRangerNativeScans.scala`, `DeltaRangerPhysicalProvenance.scala` and existing physical/receipt gates only at the necessary helper-owned seams. Keep ordinary SELECT and preparation-only negative contracts outside this helper lifetime.
- Reuse `DeltaRangerNativeIdempotency`, `BoundAppendTransaction`, native `WriteIntoDelta`, public SchemaUtils/default-expression/statistics/invariant builders. Narrow binding additions are allowed only when necessary for this owned transaction; do not weaken standalone entry validation.
- Create `test/scala/.../TestDeltaRangerNativeGovernedAppend.scala`; update affected earlier tests to the new successful behavior while retaining their negative assertions. Reuse fixtures instead of copying entire suites.

**Consumes:** `RetainedV1Input`, its exact Dataset/QE/analyzed plan and original receipt; `BoundAppendTransaction`; original effective native options; original target descriptor and native table.

**Produces:** Actual native commit or authorized duplicate result from installed named frontend, with helper-only source execution authority revoked on every exit. Original actions/tags/operation semantics remain Delta-owned.

- [ ] Write an installed-frontend success test first. Use a fresh target and source with existing masks/filters; assert append succeeds, fresh native readback includes only authorized masked source rows, and one native version increment. Run and retain the failure at the existing preparation-only terminal before production edits.

```scala
val input = spark.sql("SELECT id, secret, region FROM b.db.t")
input.write.mode("append").insertInto("a.db.t")
// Read target with a separate native-only session: compare literal expected masked
// rows and exact version delta, not results computed by the implementation.
```

- [ ] Implement one owned helper lifetime inside the existing transaction. Bind exact retained input, source receipt, target, options, transaction and prepared source. Reconstruct expected normalization/default projections using the pinned public native builders. Compare complete source expressions and original scan identities; no unconstrained Project wrapper or receipt copied to an arbitrary root. Retain actual helper QE/prepared evidence through execution and revoke on every exit. For original-QE paths retain its existing preparation instead of manufacturing a new input.
- [ ] Admit helper statistics and invariant expression analysis only against expected plans built from the exact transaction schema/protocol and native builders. Their analysis evidence must never permit physical execution. Verify foreign source, changed projection, copied tag, borrowed tracker, stats/check-plan execution, reentry and expired-owner negatives with exact causes.
- [ ] Invoke native helper, validate original source/target authorization after action generation, then native commit with the unchanged result:

```scala
val tagged = native.writeAndReturnCommitData(transaction, spark)
validate()
val operation = DeltaOperations.Write(
  SaveMode.Append, Some(native.partitionColumns),
  native.options.replaceWhere, native.options.userMetadata)
transaction.commitIfNeeded(tagged.actions, operation, tags = tagged.stringTags)
```

The exact native constructor/options and inner recache must match pinned WriteIntoDeltaBuilder/WriteIntoDelta. Preserve outer AppendDataExecV1 invalidation by returning through the issued native leaf. Do not call native.run (which opens another transaction). Keep native auto-compaction excluded until separately qualified. Once commit is attempted, propagate native/postcommit failures without automatic retry or entry-denial translation.

- [ ] Qualify the actual post-publication snapshot continuation. The first implementation attempt reached native publication and failed in `createSnapshotAfterCommit` → `validateFileListAgainstCRC` → `stateReconstruction`; do not bypass CRC or preinstall a replacement snapshot. A distinct commit owner must bind the original native log-segment files plus the exact newly published version, immutable target identity, and native session last-commit-version transition. Retain actual reconstruction QueryExecution evidence and verify the precise native StateCache copy wrapper over its RDD, including native function implementation/captures, parent identity and analyzed output. Same-parent RDDs, schema alone, path prefixes and class names alone are insufficient. Native hooks clear the active transaction, so postcommit lifetime checks must be explicit and separate from precommit invariants. Cover forged log inputs, altered copy functions/parents, unexpected configuration transitions, checkpoint execution, cache refresh and failure after publication. No new authority may expose these internal plans as user data queries.

Component review identified admitted checkpoint variants that also require qualification: configured multipart checkpoints (`spark.databricks.delta.checkpoint.partSize`), `delta.checkpoint.writeStatsAsJson=false`, and partitioned targets with both `delta.checkpoint.writeStatsAsStruct` values. Reconstruct their exact native partition count and projections from independently bound settings, metadata and published snapshot state; retain complete plan/function/input checks. Tests must establish actual checkpoint parts/pointer/schema, fresh checkpoint-plus-tail readback and normal rows/version/cleanup. A default-only test does not exclude other admitted settings; do not replace native support with entry rejection or forced configuration resets.

Cache coverage in this phase verifies the native inner and outer invalidation behavior, including authorized duplicates, using a genuinely populated native/shared cache and native readback. It does not enable governed cached SELECT or append from an InMemoryTableScanExec; the spec retains broader query/cache support as subsequent full-Delta work. A no-cache invocation alone is insufficient evidence of warm-cache refresh. Preserve existing SQLConf and SELECT enforcement while testing the native cache effect.

Pinned-source investigation identified a prerequisite: native recache creates a new query execution under the installed session or its configuration-adjusted clone, and the rebuilt cache can materialize after the append owner expires. Existing SELECT receipts do not authorize that lifecycle. Define and review a stable cache-rebuild/execution provenance contract before implementing warm-cache success. Preserve the exact native inner call and characterize failures with genuine populated matching entries in the meantime; continue independent append coverage. Warm-cache acceptance remains open until the prerequisite and runtime tests pass. Do not substitute cache eviction, preclearing, rejection of warm writes, or a no-match refresh for the required native behavior.

- [ ] Test both named frontends with Spark's default null-as-default behavior and explicit false. Cover separate source, same-target source, repeated/mixed source occurrences, empty source, native CHECK success/failure, retained masks/filters, source column denial and target UPDATE denial. Assert fresh native rows, versions, AddFile actions/statistics, unchanged schema/protocol/registration, scope cleanup and SQLConf. Preserve native file/read tracking rather than substituting Snapshot.
- [ ] Test seeded txn app/version duplicate with reset=false: authorize source and target, skip source execution/helper and return success without a new log version. Denied duplicate still fails. Keep reset=true rejected before mutation until its separate native state-transition work.
- [ ] Inject policy/registration changes through existing real callbacks or test fixtures at entry and after helper file generation; assert exact denial and no new committed version. Archive full before/after file manifests and generated files separately. No production observer seam solely for tests.
- [ ] Test input/helper/scalar/constraint/precommit/postcommit failures, stale source/settings/session/subject, and reuse after exit. Exceptions retain primary identity and all native/thread-local owners unwind. A successful write result cannot replay on later result collection. Verify healthy SELECT after failures.
- [ ] Use scoped formatter and focused tests while iterating, then affected suites once. Start from transaction-source/V1/command-binding/idempotency/read-rule/no-skipping/physical-admission tests; select additional tests only for modified seams. Archive RED and GREEN output before another invocation overwrites XML.

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
  -PskipITs -PskipDockerTests=true -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  --tests '*TestDeltaRangerNativeGovernedAppend'
```

- [ ] Release the source/build lane with commands, exact outcomes, limitations and self-review in the task report. Root dispatches independent spec/quality review and routes corrections before matrix qualification. Do not commit.

### Task 2: Qualify native governed append and record the result

**Consumes:** Independently reviewed Task 1 source and frozen selectors. **Produces:** Four fresh pinned runtime archives, honest result documentation and reviewed local checkpoint.

- [ ] Freeze released source/build inputs; run affected selectors serially across all four Spark/Delta pairs with Ranger 2.8.0. Record actual worker JDK, classpath and artifact origins, fresh XML, source hashes and native data/log/object assertions. Reuse evidence tooling without changing previous archives.
- [ ] Verify each accepted test actually reaches the claimed helper/commit/denial phase. Audit masked/filtered native readback, original action/statistics semantics, duplicate behavior, constraints, precommit rejection with uncommitted files, and lifetime/cleanup negatives. Local fixtures do not establish deployed Ranger or S3 acceptance.
- [ ] Write `docs/delta-native-governed-append.md` and link the historical transaction-source checkpoint. State supported frontends/settings and remaining reset/options/SQL/live-S3 requirements explicitly. CDF remains the first feature priority after Delta maturity.
- [ ] Complete independent final review, fix any material findings with covering tests, and commit locally with a clean tested tree. Continue toward live public Gravitino grants → Ranger 2.8.0 → S3 acceptance; do not label this finite phase full Delta support.
