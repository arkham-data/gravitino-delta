# Native Delta append characterization implementation plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Root owns sequential source/build handoff, Git and runtime. This test-only plan continues the authorized full-Delta implementation; it does not enable production writes.

**Goal:** Demonstrate whether native append can be split at the file-generation/commit boundary without changing Delta behavior, before implementing production governed writes.

**Architecture:** Reuse the existing Java native adapter fixture. Add one test-only Scala extension/probe that first observes and delegates the real native V1 writer, then substitutes only its InsertableRelation execution with a native-helper coordinator. Keep all production adapter and companion code unchanged.

**Tech stack:** JDK17, Scala2.12, Spark3.5.4/3.5.6 × Delta3.3.0/3.3.2, existing local filesystem native harness. No services, Ranger endpoints, S3 or Docker in this task.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`. Discovery references: `.superpowers/sdd/2026-09-27-delta-governed-append/native-boundaries.md` and `registration-contract.md` in the same private directory. The operational requirements below are self-contained; those reports retain source/bytecode provenance. The approved design overrides earlier exploratory terminology: eventual target access is **UPDATE**, not WRITE; target SELECT masks/filters are not insertion constraints. This task implements neither access mapping nor authorization.

## Global constraints

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; no Spark or Delta fork.
- Test sources only; keep current production source, deployed services, existing SELECT evidence and quarantined cases unchanged.
- One source/build owner; root owns Git. No remote push or runtime packaging/replacement.

## Review focus

- The actual V1 fallback input is analyzedQuery, not merely the outer optimized query: prove the executed source and literal rows for every frontend.
- Native idempotency can change session transaction-version state: preserve expected native changes and reject external configuration drift.
- File generation precedes commit: separate entry no-mutation from late-denial orphan evidence.
- Eager command execution and cached command results: no replay on recollection and no guard deferred until after the write.
- Native metadata, metrics and postcommit/cache work: compare actual native controls, never normalize away material differences.

## Task 1: Native append frontends and split-transaction characterization

## Scope and file ownership

Exactly two test files are proposed:

1. Modify `spark-connector/v3.5/spark-delta/src/test/java/org/apache/gravitino/spark/connector/delta/TestDeltaNativeAdapter.java`.
2. Create `spark-connector/v3.5/spark-delta/src/test/scala/org/apache/gravitino/spark/connector/delta/NativeAppendProbe.scala`, containing the test-only extension, probe scope/report and split helper.

No production, build, dependency, core/API/server/client or runtime/archive changes. The implementing agent does not perform Git operations; root commits reviewed source/results. Do not modify `NativeTestCatalog.java`. Do not create a second mock registry implementation or a general fault framework. Private evidence/report are permitted under `.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/`, owner-only.

`TestDeltaNativeAdapter` already owns `spark`, `adapter`, `receipts`, `locations`, `entries`, `nativeTable(name)`, `id(name)`, the mocked public catalog APIs and registration callbacks. They are private, so put the new JUnit cases and small setup/comparison helpers **inside that class**, rather than promising another class can access them. Existing `NativeTestCatalog` exposes the injected transport and journal. No extraction is necessary. Use unique table names per test/control; do not change the original tests' tables or receipts.

In `start`, add the test-only extension before `io.delta.sql.DeltaSparkSessionExtension`. The extension must be inert when no probe scope is active. Keep Delta's actual session catalog, local[2], snapshot/shuffle settings and `catalog.update.enabled=false`. The companion/Ranger extension is deliberately absent: frontend and split-native behavior must be characterized before production security admission is changed.

## Minimal helper contract

Public/test-loadable declarations need Javadoc/Scaladoc consistent with repository checks. Use normal imports and the existing compiler/runtime; do not add a package under org.apache.spark or impersonate a native catalog.

**Java/Scala compilation constraint:** Java tests compile before the new Scala test helper; do not directly reference NativeAppendProbe types from Java or change build ordering. Define these small nested interfaces and a package-visible factory holder in TestDeltaNativeAdapter (the same existing Java file). The Scala extension installs an implementation during Spark extension application; Java config names the extension by string. Clear the holder in stop and require it was installed before each probe case. This reuses the repository's test-time injection pattern without extracting another fixture:

```java
interface AppendProbeFactory {
  AppendProbeCapture begin(SparkSession spark, String targetTable, String mode);
}
interface AppendProbeCapture extends AutoCloseable {
  AppendProbeReport report();
  @Override void close();
}
interface AppendProbeReport {
  List<String> events();
  List<String> plans();
  int entryCalls();
  int helperCalls();
  int commitCalls();
  int idempotentSkips();
  boolean activeTransactionAfter();
}
static AppendProbeFactory appendProbeFactory;
```

The Scala object below returns implementations of those Java-defined interfaces; its begin method is invoked by the installed factory, not a Java-to-Scala compile-time reference. Keep implementation/report classes private. Use public nested Java interfaces if required by compiler access, with Javadoc. No new production seam is introduced.

```scala
final class NativeAppendProbeExtension
    extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit
}

object NativeAppendProbe {
  // Closed modes: NATIVE, SPLIT, DENY_ENTRY, DENY_PRECOMMIT.
  def begin(spark: SparkSession, targetTable: String, mode: String)
      : TestDeltaNativeAdapter.AppendProbeCapture
}
```

Report fields are the minimum durable observations, not substitutes for state assertions. Add explicit typed fields for measured schema/options/action/metadata results when needed; avoid a general arbitrary command API. The Java test keeps ownership of the action and exceptions:

```java
try (AppendProbeCapture capture =
    appendProbeFactory.begin(spark, targetName, "DENY_PRECOMMIT")) {
  Exception failure = assertThrows(Exception.class, () -> spark.sql(insertSql));
  // Assert the actual probe SecurityException and exact marker in the cause chain.
  // Spark may wrap command failures; retain the full chain and boundary counters.
  // Snapshot files/log/rows plus capture.report(), including failures, before close/cleanup.
}
```

Use a non-inheritable ThreadLocal scope bound to the owning SparkSession and target name. Reject nested begin or another session, and close in finally. It is test instrumentation, not a future authorization token. Scope only matches a native DeltaTableV2 whose descriptor is the fixture's canonical catalog/database/table; record the descriptor and actual target table object. Never arm a raw path or another table merely because the source query references the target.

### Frontend probe seam

Use resolution/check rules only to record bounded plan snapshots, never to rewrite source data. Retain the latest resolved AppendData for the scoped target, including its exact DeltaTableV2 reference, query/analyzedQuery, writeOptions and effective null-default SQLConf. Match this observation to the physical AppendDataExecV1 target/input before substitution; reject missing/ambiguous matches rather than reading private fields from the opaque native writer. The writer's raw options are obtained from this resolved command, not invented from Spark defaults. Reject substitution if externally changed execution-relevant SQLConf differs between the observed build configuration and entry. After entry, distinguish external changes from native-owned txnVersion auto-reset in hasBeenExecuted/createSetTransaction: preserve that native reset, record its phase and before/after value, and assert it against the unmodified native control. Do not reject a valid native reset as an unexpected configuration change. Use a test-only `ColumnarRule.postColumnarTransitions` to find **AppendDataExecV1**, retain its native table/plan/refreshCache, and copy it with a decorated `V1Write`. Record the physical fallback `plan` and the original built writer's class. Its InsertableRelation wrapper records the actual received `Dataset.queryExecution.analyzed`, schema and overwrite argument, then:

- NATIVE: invokes the original InsertableRelation exactly once; preserves real builder normalization/options, native run, inner recache and outer V1FallbackWriters refresh.
- SPLIT/denial: invokes the split coordinator below, then the same native-equivalent inner recache on successful return; leaves original outer refresh callback on the copied exec.

The original V1Write wrapper must not invoke `insert` while building/planning. Retain a per-node marker/reference to avoid decorating its own wrapper on repeated preparation. Record observations of repeat planning separately from invocation counters. No broad command allowance is involved because Ranger is not installed.

**First compile/behavior gate:** prove the pinned Scala compiler permits the existing AppendDataExecV1 copy/V1Write seam and that the columnar rule visits the eagerly executed command before insertion. If it does not, preserve the failure and return to root for the bounded planner-strategy alternative; do not add reflection, monkey-patch Spark, widen packages, or silently switch architecture. Do not instantiate an inaccessible native strategy on speculation.

### Split helper seam

Keep this helper private to the test Scala file:

```scala
private def appendSplit(
    spark: SparkSession,
    table: DeltaTableV2,
    data: DataFrame,
    options: Map[String, String],
    denyEntry: Boolean,
    denyPrecommit: Boolean,
    record: String => Unit): Unit
```

Its owning helper mixes in the public DeltaCommand trait so it can call inherited **protected** hasBeenExecuted. Do not call the protected method on a separate writer through reflection. Native source sequence to mirror:

```scala
// entry event and DENY_ENTRY before transaction, input evaluation or native helper
val native = WriteIntoDelta(table.deltaLog, SaveMode.Append,
  new DeltaOptions(effectiveOptions, spark.sessionState.conf), Nil,
  table.deltaLog.unsafeVolatileSnapshot.metadata.configuration,
  data, table.catalogTable)
table.deltaLog.withNewTransaction(table.catalogTable) { txn =>
  // Require this characterization target to exist; record transaction snapshot.
  if (hasBeenExecuted(txn, spark, Some(native.options))) {
    // Record skip, no helper or commit; preserve native session auto-reset behavior.
  } else {
    val tagged = native.writeAndReturnCommitData(txn, spark)
    // Record actions, transaction metadata/protocol and storage BEFORE commit.
    // DENY_PRECOMMIT throws here, with no cleanup or retry.
    val operation = DeltaOperations.Write(SaveMode.Append, Some(Nil),
      native.options.replaceWhere, native.options.userMetadata)
    txn.commitIfNeeded(tagged.actions, operation, tags = tagged.stringTags)
  }
}
// Preserve the native builder's successful inner recacheByPlan behavior.
```

If `native.options` is not callable in Scala, retain the locally constructed `DeltaOptions` reference instead; do not introduce a getter to production. The helper must pass native action objects and stringTags unchanged, use the catalogTable overload, and preserve native transaction finally cleanup. No manually assembled AddFile/SetTransaction/Metadata, optimistic-commit algorithm, retry loop, or second invocation of native.run after writing.

Native WriteIntoDeltaBuilder captures SQLConf.useNullsForMissingDefaultColumnValues when built and injects `ColumnWithDefaultExprUtils.USE_NULL_AS_DEFAULT_DELTA_OPTION` on insertion. Record the effective SQLConf at the probe's planning/build observation; keep it fixed for each case and reproduce that **exact native option** in the split branch. Do not omit this behavior or use a hardcoded guessed key. Compilation/access to the constant is a gate; no reflection workaround. Later production receipt design must handle changes between planning/build/execution; this test-only probe does not solve that race.

## Behavioral cases and exact JUnit methods

Add the following methods to TestDeltaNativeAdapter. Each uses separate native-control and split target tables of the same schema, independently created through `registry.db` and the real adapter fixture. The same action builder drives both. Assert expected literal data independently; equality between two wrong results is insufficient.

### 1. `appendFrontendsUseNativeAnalyzedQueryWithoutCompanion`

Run **NATIVE only first**, before implementing split coordination. Cases:

- `INSERT INTO registry.db.<target> VALUES (1,'a'),(2,'b')`.
- `INSERT INTO registry.db.<target> SELECT id,value FROM registry.db.<source> WHERE id=2` over a genuine named source containing the two rows.
- `spark.table(source).write().mode("append").insertInto(target)` (position-based).
- `spark.table(source).write().mode("append").saveAsTable(target)` (name-based existing table).
- `spark.table(source).writeTo(target).append()` (name-based).

Use a column-order variant to demonstrate position versus name semantics with compatible types, rather than asserting only an isByName flag. Observe resolved AppendData, native target class/descriptor, fallback AppendDataExecV1, its logical plan, built V1Write and actual insert DataFrame. Pin which source trees the real engine executes and retain tree/ExprId/schema data. Show source filter effects through literal target row assertions. Do not insert a fabricated masked/analyzedQuery sentinel yet: that would test a rewrite not native control behavior.

For SQL, snapshot target version immediately after `spark.sql` returns and before calling collect; then collect the returned command Dataset twice and verify no additional commit. For DF APIs, return from append means the write was executed. Record nested QueryExecution and native plan classes through bounded observers. Assert probe captured one native insertion rather than assuming the extension reached it.

### 2. `appendSplitMatchesNativeRowsActionsAndMetadata`

Run all five frontends against native/split paired targets after the first gate. Assert exact rows and equal version deltas. Compare committed action types, partition values, per-file record statistics, schema/protocol/configuration, operation name/parameters, readVersion, blind-append status, userMetadata and transaction actions.

Keep per-table native ID/location/registration marker/token distinct and verify each remains unchanged from its own baseline. Normalize only these known fixture identities plus generated file basenames, commit timestamps/transaction IDs and measured duration fields; retain raw originals. Compare file sizes/statistics against their own actual file manifest, not a blanket field deletion. Schema ordering/nullability/types and AddFile.dataChange are material and remain exact.

Include self-append with a finite source snapshot and assert native/split row multiplication matches once, not a repeated or recursively growing source. Require active native transaction and probe scope cleared after success.

### 3. `appendEntryDenialLeavesOwnedStorageUnchanged`

For a nonempty literal input and named source, deny before transaction/helper. Assert the actual probe SecurityException and exact marker in the thrown exception/cause chain (retaining any Spark wrapper), entryCalls=1/helperCalls=0/commitCalls=0, same Delta version, same rows, and exact recursive file-key/length/SHA manifest. Also assert source scan work did not run using a bounded test-only source-execution observation, **not** a policy claim. If no nonintrusive reliable scan observer is available, report that gap and rely on the helper-entry boundary rather than adding a UDF with different execution semantics.

### 4. `appendPrecommitDenialRetainsUncommittedFilesWithoutDeltaCommit`

Nonempty input, real native helper, throw after helper returns. Assert the actual probe SecurityException and exact marker in the thrown exception/cause chain; a Spark wrapper alone is not a failure of this boundary evidence. Assert helperCalls=1/commitCalls=0, unchanged visible rows/version, no new committed JSON actions, and generated Parquet paths recorded in helper actions present under the owned target. Verify pre-existing object bytes unchanged. Preserve the generated files in evidence; do not vacuum/delete/replay them. Compare transaction metadata captured before throwing with persisted log metadata and assert active transaction/thread scope cleared. A separate fresh append on a separate target remains the control; do not turn this failure case into an automatic recovery test.

### 5. `appendTransactionIdentifiersPreserveNativeRetryAndResetSemantics`

Paired native/split cases: ordinary same append twice (two writes); explicit writer txnAppId/txnVersion first and duplicate (one write); larger version (new write); options override session transaction identifiers; session-only pair with auto-reset both true and false; missing counterpart rejects as native does. Capture session state after each invocation. SPLIT duplicate reports idempotentSkips=1 and no helper/commit calls. Authorize-equivalent DENY_ENTRY precedes the duplicate no-op and must still throw; label this probe ordering, not actual Ranger authorization. Do not infer dedup from DataFrame object identity or saved command results.

### 6. `appendEmptyAndConstraintInputsPreserveNativeOutcomes`

Paired empty named SELECT and empty DataFrame input, with and without a SetTransaction identifier. Compare actual native version delta/action sets and empty file handling; do not assume all empty inputs avoid commits. Add genuine NOT NULL/check-constraint violation through existing admitted table schema/property creation; compare exception class/root cause, log/version and physical artifacts. Constraint failures may leave physical files depending on native task completion: retain manifests and do not require zero artifacts without control evidence.

### 7. `appendOptionsAndPendingMetadataMatchNative`

Paired compatible schema, incompatible schema, explicit mergeSchema=true, effective session autoMerge=true, userMetadata, and explicitly selected compression/maxRecordsPerFile. Use extra-column data for schema evolution so the case actually changes metadata. Record old snapshot metadata, txn.metadata after helper, and persisted metadata after success/denial. Repeat schema evolution with DENY_PRECOMMIT and prove pending metadata was not published although files may exist. Preserve native schema/casts/null-default behavior and verify subsequent adapter metadata refresh.

Also characterize the actual options paths for each frontend: insertInto may ignore source-format options while saveAsTable/writeTo forward options. Expected outcomes come from native controls, not an assumed uniform option surface. Overwrite/replaceWhere/dataChange=false/auto-compaction are **not silently supported by this append-only helper**; record encountered effective settings and explicitly reject or return the unmodified native control where appropriate. Production admission decisions remain subsequent work. Do not fabricate a before-file authorization guarantee from a mismatch detected only after helper return.

### 8. `appendMetricsAndPostcommitEffectsRemainNative`

Enable ordinary native history metrics identically for controls and split. Assert operation metric key sets and values for numOutputRows/numFiles, and validate numOutputBytes against each target's actual committed AddFile sizes. Retain timing fields but compare their presence/validity rather than equality. Capture SQL execution IDs/events with a scoped QueryExecutionListener or SparkListener, wait for completion with a bounded latch/predicate, and retain original events; no sleeping to presume completion. Verify nested `deltaTransactionalWrite` execution and nonmissing write metrics where native control supplies them.

Use one named cached-read native control to verify post-append cache refresh returns the new rows; this is native characterization, not support for Ranger user caches. Both the Delta builder's inner recache and Spark V1FallbackWriters' outer refresh are part of the route. Preserve outer callback through exec copy; reproduce inner recache after successful split/no-op. Do not call either after a precommit exception. Preserve the existing colliding session-catalog sentinel and await existing UpdateCatalog completion with the established bounded API.

For a concrete native postcommit hook check, use independently created owned tables with `delta.checkpointInterval=1` and compare checkpoint artifacts/readback after successful append when the control produces them. Do not count registration of a synthetic hook as equivalent proof. If native configuration/admission makes this unavailable, report the measured boundary rather than altering provider production code. Source inspection shows WriteIntoDelta.run itself does not call sendDriverMetrics here; do not add that call speculatively. TransactionalWrite registers metrics on txn, commitIfNeeded writes history and runs native postcommit hooks. Differential evidence must catch anything lost by splitting. A recache/hook failure after native commit is a committed/uncertain outcome, never retroactively a denied write.

## Assertions, artifacts and cleanup

- Keep all real data under the existing @TempDir-owned warehouse/target directories. Verify each target is beneath that root before recursive inspection or cleanup; follow no external symlink.
- Manifest every regular file with relative key, size and SHA256; separately classify committed JSON, checkpoint/sidecar, data and filesystem auxiliary files. Retain originals; do not hide unexpected artifacts by broad `_` filename filters.
- To survive JUnit temp cleanup, copy each owned case's native log and files plus manifests/plans/results into a unique owner-only evidence directory **in finally**, including failures. Proposed optional environment variable `GRAVITINO_DELTA_APPEND_EVIDENCE_DIR` supplies an existing private root; validate it is absolute and create per-case children with CREATE_NEW/exclusive semantics. No build-file forwarding is required for an environment variable. If unset, tests still assert normally but must not claim durable evidence.
- Capture source/class hashes and effective runtime JAR versions/origins alongside results. Retain raw event JSON/text and normalized comparison separately; record every tolerated difference by field/reason.
- Close Capture, unregister listeners, unpersist the test's cached Dataset and restore every changed SQLConf in finally. Existing stop closes Spark and resets NativeTestCatalog static seams. Delete only JUnit-owned temporary storage through its normal lifecycle; copied orphan evidence remains retained.
- Tests are not Ranger tests. Do not alter current SELECT caches, live runtime fixtures, quarantined cases, policy services or user identities.

## Ordered implementation and gates

- [ ] Add frontend observation/delegation first and prove genuine native behavior. Missing instrumentation, compilation and setup failures are not behavioral RED; preserve and label them accurately. This is characterization of existing native behavior, so do not manufacture a failing assertion merely to claim TDD. Any actual seam/compiler incompatibility returns to root with evidence.
- [ ] Add native-vs-split behavioral tests before split coordination; preserve actual failures without labeling an unavailable helper as a native semantic failure. Implement the minimal test-only coordinator and prove data/action/idempotency equivalence before adding failure modes. Never count a compile failure as behavioral RED.
- [ ] Add entry/precommit fault modes and retain RED/GREEN assertions against native storage state. Add options/metadata and metrics/postcommit comparisons; classify every mismatch rather than normalizing it away.
- [ ] Format only spark-delta, run the complete existing TestDeltaNativeAdapter class at the first pair; inspect failures and existing baseline tests.
- [ ] Once the source is stable, run the same exact class on the other three pairs, archiving each pair before another build overwrites classes/XML. No production coordinator or widened companion admission follows automatically.
- [ ] Freeze two test sources, commands/exit codes, effective runtime manifests, compiled probe classes and four-pair XML/log/artifact results; report unresolved native semantics and evidence limits for root review.

One source/build owner only; the server MODIFY mapping task has completed and released the lane at local source commit651e669fa; its accepted closeout is9438e9e59.

## Four-pair offline commands

Run from the feasibility worktree. Create the four evidence roots owner-only before running. These are future execution instructions, not tests performed while authoring this brief.

```bash
GRAVITINO_DELTA_APPEND_EVIDENCE_DIR="$PWD/.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/354-330" JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-3.5:test --tests org.apache.gravitino.spark.connector.delta.TestDeltaNativeAdapter -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
GRAVITINO_DELTA_APPEND_EVIDENCE_DIR="$PWD/.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/354-332" JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-3.5:test --tests org.apache.gravitino.spark.connector.delta.TestDeltaNativeAdapter -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2 --offline --console=plain
GRAVITINO_DELTA_APPEND_EVIDENCE_DIR="$PWD/.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/356-330" JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-3.5:test --tests org.apache.gravitino.spark.connector.delta.TestDeltaNativeAdapter -PskipITs -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.0 --offline --console=plain
GRAVITINO_DELTA_APPEND_EVIDENCE_DIR="$PWD/.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/356-332" JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-3.5:test --tests org.apache.gravitino.spark.connector.delta.TestDeltaNativeAdapter -PskipITs -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 --offline --console=plain
```

Formatting: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-3.5:spotlessApply :spark-connector:spark-delta-3.5:spotlessCheck --offline --console=plain`. Do not rerun a pair solely to change evidence location; arrange capture before execution.

## Completion meaning

A passing report establishes native orchestration and available observation boundaries on the four pins. It does not establish Ranger UPDATE enforcement, atomic registration/policy fencing, S3 orphan cleanup, full Delta feature support, or safety of arbitrary jobs. Any native-control-vs-split mismatch is a design gate, not a reason to relax expected Delta behavior.
