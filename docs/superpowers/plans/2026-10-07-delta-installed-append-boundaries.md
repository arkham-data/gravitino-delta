# Installed-companion append boundary characterization implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Root reviewed and released this bounded task after target-authority acceptance at `24e7b3736` (500 passing component/regression tests across all four pairs). Existing continuous authorization supplies the execution method; no new user approval question is needed.

**Goal:** Produce one finite, reproducible installed-companion characterization of actual append inputs, source/target coexistence, native transaction reset and transactional/native-internal boundaries, without opening production write admission.

**Architecture:** Exercise actual SQL/DataFrame commands under the current companion and record their first rejection and captured inputs. Separately invoke the already characterized native transaction/helper seams from a test-only driver on owned disposable tables while the same companion remains installed; retained target/source checks stay real and no native scope surrounds the whole helper. Successful native-only observations, blocked installed operations and unreachable downstream phases remain distinct evidence.

**Tech Stack:** Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12.21; JDK 17; existing Ranger 2.8.0 engine and request-sensitive fixture; local filesystem only.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`. Also consume the subsequent integrated gate in `docs/superpowers/plans/2026-10-05-delta-append-target-authority.md`, `.superpowers/sdd/2026-09-27-delta-governed-append/production-integration-map.md`, and `docs/delta-native-append-results.md`.

## Global Constraints

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- S3-compatible native storage; local MinIO qualification is permitted. Preserve existing SELECT fixture and quarantined callback cases.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal.
- Keep accepted runtime packaging/dependency isolation. No remote push, demo change or service replacement in the initial local implementation task.
- This task modifies only new test sources plus a results document. No production rule, strategy, receipt, native scope, planner/prefix allowlist, build dependency or existing SELECT fixture/test is changed.
- Do not remove the companion, restore native-reset SQLConf, substitute a Snapshot for a transaction generator, attach fabricated tags, or introduce an ambient permission to get past a recorded blocker.
- Keep all failed-attempt, source, storage and diagnostic evidence under owner-only private directories; full SQLConf and traces are not published in tracked documentation. Root alone owns Git and acceptance. One agent owns all sources/builds serially; no parallel Spark/Gradle work.
- CDF enablement is the first feature priority after Delta maturity. This harness neither enables CDF nor changes that ordering.

## Review Focus

1. Additional test instrumentation must not itself reject a previously healthy named SELECT, change the plan, or grant native/command authority. Task 1 step 2 verifies this explicitly.
2. The V1 writer executes `AppendData.analyzedQuery`, not merely the ordinary child; actual command/source identity and load ordering can invalidate retained bindings. Steps 3–4 retain both shapes and both source/target validation outcomes.
3. Native transaction reset may invalidate every existing full-SQLConf receipt even on a duplicate skip. Step 5 captures native before/after state and tests old receipts without restoring it.
4. A transaction-active self-source and helper-created plans encounter different provenance gates; the earliest failure must not be relabeled as successful downstream coverage. Steps 6–7 retain active transaction/source/native-scope identity and explicit not-reached phases.
5. Entry failure, generated-but-uncommitted files and postcommit work have different durable outcomes. This harness never invokes commit under the companion; steps 3–7 assert exact manifests/no new log commit and report any helper-generated files separately.

---

## Why this is the next bounded step

The native four-pair probe already established native execution equivalence and exact native V1 writer inputs. The target component adds no command admission. A positive full SQL append under the unchanged companion is therefore not a valid expectation for this task. A test-only blanket allowlist would hide the exact integration gaps we need to resolve.

The smallest substantive output is one combined **boundary dossier**: actual command inputs observed before current rejection; independent target UPDATE and complete source obligations in the same session; real native reset/transaction/helper attempts under that installed companion; first blocking stack and phase; and a concrete dependency order for the later owned continuation/command implementation. A blocked phase is a measured integration gap, not a qualified write. If a phase cannot be reached without a bypass, record it as not reached and do not invent evidence for it.

## Source-verified seams and instrumentation constraints

Paths **S**, **J** and **T** mean respectively `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`, corresponding `src/main/java/.../ranger/`, and corresponding `src/test/scala/.../ranger/`.

| Existing source/interface | Exact use and restriction |
| --- | --- |
| T/`TestDeltaRangerReadRule.scala`, `NativeSelectFixture.withSpark(test: SparkSession => Unit, extension: Option[SparkSessionExtensions => Unit], startupOptions: Map[String,String], beforeExtension: SparkSession => Unit): Unit` | Creates genuine native fixture data before companion installation; use `beforeExtension` only for native initial histories/transaction markers. Installed callbacks then use actual named wrappers. Preserve its cleanup and capture evidence before it runs. |
| Same file, `NativeSelectFixture.extension(fixture): SparkSessionExtensions => Unit`, `wrapper(spark,catalog)`, `bindingFixture(spark,catalog)` | Install the real companion via existing injection. Current mock transport is not live Ranger delivery. Do not use `componentRewriter` as the installed route. |
| S/`DeltaRangerSparkExtension.scala:42–55` | Installs resolution, columnar, pre-CBO and final check callbacks. Keep every callback and native Delta order. |
| S/`DeltaRangerPhysicalPlanning.scala:137–159`; S/`DeltaRangerDecisionReceipt.scala:384–427` | Qualification checks exact columnar/planner/adaptive and optimizer boundaries; it does not enumerate analyzer resolution-rule classes. An added columnar observer from `NativeAppendProbeExtension` would fail qualification. Do not port that extension wholesale. |
| S/`DeltaRangerReadRule.scala:61–94`, `apply(plan)` | Resolved unsupported commands can reject during resolution before final check rules. `spark.sql` can fail before returning a Dataset. Use a strictly identity-returning resolution observer only for evidence unavailable from returned Dataset/call-site catches. |
| J/`DeltaRangerRegistrationBindings.java`, `resolveAppendTarget(DeltaTableV2): BoundAppendTarget`; S/`DeltaRangerAppendTargetReceipt.scala`, `capture(spark,target)` / `validate(spark)` | Exact table and target-only UPDATE under the unchanged context. No command or transaction authority is implied. |
| S/`DeltaRangerDecisionReceipt.scala`, `consumeOwned(plan,spark,optimized:Boolean)` / `validateAction(spark)` / `obligations` | Use the actual accepted named source analyzed root with `optimized=false`. Each obligation exposes `scan`, `columns`, `binding`, `decisions`. Keep every original occurrence, including empty-source obligations. |
| S/`DeltaRangerReadRule.scala:173–240`, `prepare(plan,spark)` | `PrepareDeltaScan.getDeltaScanGenerator(index)` rejects a non-Snapshot generator. Do not pass a raw analyzed plan into an API that consumes optimized evidence; use actual `data.queryExecution.optimizedPlan` to exercise installed pre-CBO ordering. |
| J/`DeltaRangerNativeOperation.java:122–160`, `current()` | Existing scopes are finite/non-reentrant. Record presence/object identity at test call boundaries; do not open a global helper scope or reflectively access private evidence. |
| Native `OptimisticTransaction.getActive`, `DeltaLog.withNewTransaction(table.catalogTable)`, `txn.readVersion/metadata/protocol` | Already used by accepted native probe. Record the actual active transaction, log and snapshot values under the installed session. |
| Native `DeltaCommand.hasBeenExecuted(txn,spark,Some(native.options))`, `WriteIntoDelta.writeAndReturnCommitData(txn,spark)` | Already compiled/run across four pairs. The proposed test driver extends DeltaCommand, using protected methods normally. No reflection and no native transaction reimplementation. |
| Native `AppendData.analyzedQuery/query/writeOptions/isByName`, `DataSourceV2Relation.table`, `AppendDataExecV1.plan` | Accepted native probe retains their relation. Installed command analysis may stop before physical V1 creation; retain absence rather than synthesizing a physical plan. |

The accepted immutable reference is `.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/corrected-source-final-20261005/NativeAppendProbe.scala`, SHA `9d77c87db41d94982c634aea146acc41d7e021cc72cee24eb5e03981d8dca20d`. It is a source reference, not an importable production authorization layer. Do not reuse its `fastEquals` matching or selected-config-key comparison as an authority contract.

## Task 1: Produce the installed-companion boundary dossier

**Files:**

- Create T/`InstalledAppendBoundaryProbe.scala`: test-only trace, non-mutating observer and direct native phase driver. It must contain no commit invocation or production override.
- Create T/`TestDeltaRangerInstalledAppendBoundaries.scala`: finite isolated fixture cases and assertions.
- Create `docs/delta-installed-append-boundaries.md`: reviewed results, actual first blockers, unresolved ownership contracts and explicit support limits.
- Private output root: `.superpowers/sdd/2026-10-07-delta-installed-append-boundaries/`. Root assigns unique attempt/cell directories; evidence serialization writes only beneath each assigned directory.

**Consumes:** only the source-verified interfaces above and the actual installed fixture. **Produces:** test-only data and eight JUnit test groups below. No production API is proposed. A compact helper interface is sufficient:

```scala
// New test-only declarations, not existing product APIs or transferable authority.
private[ranger] object InstalledAppendBoundaryProbe {
  final class Trace(val directory: java.nio.file.Path) extends AutoCloseable {
    def observe(spark: SparkSession, phase: String, plan: Option[LogicalPlan]): Unit
    def failed(phase: String, error: Throwable): Unit
    override def close(): Unit
  }
  def observer(trace: Trace): SparkSessionExtensions => Unit
  def inspectNativePhases(
      spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      targetReceipt: DeltaRangerAppendTargetReceipt,
      sourceReceipt: Option[DeltaRangerDecisionReceipt],
      data: DataFrame,
      options: Map[String, String],
      trace: Trace): Unit
}
```

The phase driver receives the already captured target and optional source receipts; it must validate those same objects before native handling and after any observed reset. It must not recapture authority to conceal load-order or configuration drift. `None` is allowed only for explicitly labelled literal-input driver probes, never for named sources. Use normal imports in implementation. Trace saves full SQLConf before/after native call boundaries, plan class/tree/schema/output ExprIds, exact-instance ordinal IDs assigned in a test-local IdentityHashMap, active transaction identity, current native operation identity, raw exception/cause stack, policy requests and timestamps. None of these records is authority. Trace must not call `queryExecution.optimizedPlan` while observing a plan or invoke any callback to obtain permission; that would recursively alter the execution being measured. For unresolved nodes, record only safe class/tree/resolution state and leave schema/output marked unavailable; do not access unresolved attributes merely to enrich a trace. Records describe what is already available at the call site. Instrumentation failure is a failed harness, never the accepted companion blocker.

### Step 1: Establish failure and storage accounting before adding observations

- [x] Add `installedCommandRemainsClosedWithValidTargetUpdate`. Use `NativeSelectFixture.withBoundSpark`, load exact `a.db.t`, resolve/capture a target receipt, then attempt `spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')")`. Capture its complete cause chain; do not assume collection is the first execution point. Assert a companion admission rejection is present, exact log version/storage bytes unchanged, target receipt never attached, and `DeltaRangerNativeOperation.current() == null` / `OptimisticTransaction.getActive.isEmpty` afterward.
- [x] Before helper implementation, write assertions requiring a trace event for command analysis and complete failure accounting. The initial failure must be missing trace evidence, not a deliberate removal of the production guard. Save this RED separately from compilation errors.
- [x] Implement a test-local file manifest that snapshots every regular file relative to the owned target and records byte hash/size; archive bytes before teardown. Do not normalize away metadata/actions/options, mutate files to “clean up”, or use the shared live SELECT warehouse.

### Step 2: Qualify the observer with ordinary SELECT controls

- [x] Add `resolutionObserverPreservesInstalledSelectAndRejectsNoAdditionalShape`. Run the same supported named query on isolated equivalent fixtures without and with the observer. Compare returned masked/filtered rows, canonical source request inventory and installed extension/optimizer/strategy/columnar class manifests. All must agree; the only additional class is the test resolution observer.
- [x] Implement the observer ahead of the existing companion resolution rule, without adding optimizer/columnar/planner rules:

```scala
val install = NativeSelectFixture.extension(policy)
val extension: SparkSessionExtensions => Unit = extensions => {
  extensions.injectResolutionRule { spark => new Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      trace.observe(spark, "resolution", Some(plan))
      plan // Exact same object; no copy, tags, rewrite or exception suppression.
    }
  }}
  install(extensions)
}
```

- [x] Restrict recording to the test-owned active trace and session; record a foreign/cloned session as a different identity, never treat sharedState as permission. Close and clear trace state in `finally`.
- [x] Recheck raw-path SELECT and unreceipted source rejection with the observer installed. If instrumentation prevents the healthy control, stop and fix instrumentation; do not change production qualification to admit it. Direct phase probes use call-site records without the observer unless nested plan visibility is necessary.

### Step 3: Record the actual command input boundary for all five frontends

- [x] Add `frontendsExposeCommandInputsBeforeCurrentAdmissionRejection`. Isolated cases use SQL VALUES; `INSERT INTO a.db.t SELECT id, secret, region FROM b.db.t`; `spark.sql("SELECT id, secret, region FROM b.db.t").write.mode("append").insertInto("a.db.t")`; the same named source with `.saveAsTable("a.db.t")`; and `.writeTo("a.db.t").append()`.
- [x] At each actually observed `AppendData`, save the target native object's identity, `query` and optional `analyzedQuery` separately, each schema/output name/type/nullability/ExprId, `isByName`, options, full SQLConf and presence of source receipts. Keep repeated resolution observations with ordered IDs; do not collapse by `fastEquals` or SQL text. If the current companion rejects before `analyzedQuery` exists, record the exact preceding shape and absent field.
- [x] All five operations must remain rejected with no commit/storage changes. No physical `AppendDataExecV1` is required if the guard stops earlier; report it as not reached. Do not synthesize it just to populate the evidence.
- [x] Add raw-path and create/replace controls to the same group: they remain rejected and cannot borrow the target receipt. A Dataset source that is rejected before append construction is a frontend/source-composition gap, not a measured V1 input.

### Step 4: Retain source and target together, including load-order interactions

- [x] Add `targetUpdateAndOriginalSourceObligationsCoexistWithoutCommandAuthority`. Construct a supported named source, obtain `data.queryExecution.analyzed`, consume its real source receipt with `optimized=false`, and independently capture target UPDATE. Revalidate both before proceeding. Repeat source-first/target-first loading for self-source; record exact descriptor/table/map replacement outcomes. Do not reload a source or recapture a different receipt merely to conceal an invalidated binding.
- [x] Cases: separate source b→target a; source a→target a; a finite empty named query; repeated source occurrences in a supported inner join; and same names/shared native location under separate catalogs. Record original occurrence/column/decision inventories even when later optimizer output is empty.
- [x] Assert UPDATE-only target policy does not imply source SELECT: request-sensitive test transport separately denies the source column/key. Assert target read masks/filters never enter target decisions, while source decisions retain them. Use the existing real Ranger Task 2 tests as policy-matching evidence; this fixture remains a transport double.
- [x] Collect a separate ordinary SELECT control to verify source masks/filters, but preserve an unexecuted independent source Dataset for transaction-active preparation. Collecting first would precompute/cache its physical plan and hide the native generator boundary.

### Step 5: Observe native idempotency reset against real retained receipts

- [x] Add `nativeIdempotencyRecordsInstalledResetReachability`. Preseed duplicate transaction state using a genuine native append in `beforeExtension`, with writer `.option("txnAppId", "probe-app").option("txnVersion", "1")`; keep all generated JSON/CRC artifacts. After installation set the session options, then capture target/source receipts under that exact map.
- [x] The new test-only driver extends `DeltaCommand`; it constructs the real `DeltaOptions` and `WriteIntoDelta` as in the immutable probe, then enters `target.log().withNewTransaction(target.table().catalogTable)`. Entry calls both existing receipts before native idempotency handling. Record full conf and actual transaction before/after `hasBeenExecuted` in a `finally` block.
- [x] Finite reset subcases: matching duplicate with reset enabled; matching duplicate with reset disabled; first app/version with reset enabled; explicit option app/version overriding differing session app/version. Record native returned skip result and exact map delta. On observed native removal, both old receipts must reject their original full configuration. Leave the removal intact. No new receipt is issued and no ambient map is substituted.
- [x] If `hasBeenExecuted` or transaction creation itself rejects under installed admission, capture that exact phase/cause and mark later reset/helper phases not reached. Manual `spark.conf.unset` is not acceptable evidence of a native-owned reset. The existing Task 2 removal test already covers external drift; do not repeat it as native proof.

### Step 6: Measure transactional source preparation and active ownership

- [x] Add `selfSourceRetainsNativeTransactionGeneratorAndCurrentPreparationRejects`. Within the same real transaction phase, retain `OptimisticTransaction.getActive` and inspect the native generator for the source obligation's exact original index through a test subclass:

```scala
final class GeneratorObservation(spark: SparkSession) extends PrepareDeltaScan(spark) {
  def inspect(index: TahoeLogFileIndex): DeltaScanGenerator = getDeltaScanGenerator(index)
}
```

- [x] Record generator class and exact equality to the active transaction when native returns that transaction; do not assert all cross-table generators are transactional. Exercise the unexecuted source's actual `queryExecution.optimizedPlan` under the installed pre-CBO callback. A transactional generator must still hit `Native transactional scan preparation is not qualified`; if an earlier receipt/load-order gate stops it, retain that as the earlier blocker rather than pretending this branch ran.
- [x] Repeat self-source, different-log source and source under an unrelated active target transaction. The observations determine which finite source occurrence/log/transaction lineage the future continuation must own. Do not substitute a Snapshot, alter the active transaction, bypass `prepare`, or surround the helper with a source PREPARE scope.
- [x] Every failure records no active native operation/transaction after the real transaction scope unwinds. Validate cleanup without trying to reuse failed native evidence.

### Step 7: Attempt real native helper phases; preserve first blockers and generated files

- [x] Add `nativeHelperKeepsCompanionChecksAndNeverCommits`. In a fresh fixture use the real named source Dataset and native options; literal-input helper attempts may use a finite test-created DataFrame, explicitly tagged in the report as a driver-level native probe, never as admitted source authority. Source SELECT checks are required for named inputs. Target-only UPDATE is not permission for arbitrary DataFrame execution.
- [x] The phase driver uses this measured call sequence, deliberately with no commit path:

```scala
val native = WriteIntoDelta(table.deltaLog, SaveMode.Append, deltaOptions, Nil,
  table.deltaLog.unsafeVolatileSnapshot.metadata.configuration, data, table.catalogTable)
table.deltaLog.withNewTransaction(table.catalogTable) { txn =>
  // Existing target/source checks run before this call, under their unchanged configuration.
  trace.observe(spark, "before-hasBeenExecuted", None)
  val skipped = try hasBeenExecuted(txn, spark, Some(native.options))
    finally trace.observe(spark, "after-hasBeenExecuted", None)
  if (!skipped) {
    trace.observe(spark, "before-helper", None)
    val tagged = try native.writeAndReturnCommitData(txn, spark)
      finally trace.observe(spark, "after-helper", None)
    // Archive original action JSON/stringTags and pending txn metadata/protocol.
    // Stop here: this characterization never invokes txn.commitIfNeeded or native.run.
  }
}
```

The real implementation records failures in a catch that rethrows; a returned phase does not swallow validation errors. Effective null-default/AutoCompact/options handling is copied narrowly from the accepted helper source; delegate `AutoCompact.getAutoCompactType` and reject enabled compaction. Do not introduce a second append algorithm.

- [x] Exercise named separate source, self-source, empty source, and one native CHECK-constraint fixture created before installation. Retain internal plan observations/cause stacks, exact active transaction/native-operation context and SQLConf on both successful return and failure. If the helper returns, archive its native actions but commit none. If it fails before SetTransaction generation, report reset-at-helper as not reached, not as reset absent by design.
- [x] Compare before/after full manifests, Delta log versions and native-only durable readback after the installed attempt. No new committed version/rows is allowed. Enumerate generated uncommitted data files without deleting them. Recheck original publication and source/target request inventories so “no commit” is not mistaken for complete enforcement.

### Step 8: Qualify checksum reconstruction and synthesize the dependency order

- [x] Add `checksumBearingAndAbsentHistoriesKeepDistinctInstalledOutcomes`. Use genuine native version-1 commits and matching JSON/CRC/sidecars from Task 2's accepted history technique. Compare two fresh owned histories: complete checksum-bearing history and the same valid commit without the optional version checksum/its Hadoop checksum sidecar. Never modify commit JSON while retaining an old native checksum. Preserve original bytes before any fixture operation.
- [x] Exercise actual `target.validateCurrent(spark)` / receipt validation under the installed companion. Record whether `DeserializeToObject` reconstruction is reached/rejected; archive the externally available causal stack, exact last observed reconstruction node and last observed native version. Existing target validation intentionally replaces some native exception details; if the inner cause is redacted, say so rather than claiming the outer stack proves a hidden exception. Use the non-mutating observer plus the existing retained diagnostic as separate evidence; do not reflectively open an authority anchor or add a production diagnostic hook. Lack of CRC is an availability case; no encoder/native-package allowlist may be introduced by this task.
- [x] Write `docs/delta-installed-append-boundaries.md` with one row per frontend/phase/control: exact input identity, source/target authority state, native transaction state, conf transition, last completed phase, first rejected phase/cause, durable outcome, and next required ownership contract. State not-reached commit/recache/cloned-session outcomes explicitly. The old native recache evidence remains a downstream design dependency, not newly integrated evidence.
- [x] Select the next implementation dependency from the measured earliest blockers: exact command/source ownership and allowed native-owned config continuation must be designed together with transactional/native-plan provenance before admitting AppendData. Prefer decorating the native V1 writer only after its columnar admission interactions are explicitly implemented/qualified; no strategy or decorator is selected merely because native-only tests used it. If results show a different first ownership dependency, state it with exact source/phase evidence.

## Verification and acceptance

One implementation agent owns this whole task, because fixtures, trace and native phases share mutable Spark/UGI state. Independent review is read-only. Root schedules builds and acceptance after the accepted target-authority component; the draft author ran none.

Initial primary selector:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerInstalledAppendBoundaries -PskipITs -PskipDockerTests=true -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

After source review/freeze, run the new class and unchanged `TestDeltaRangerReadRule`, `TestDeltaRangerActionValidation`, `TestDeltaRangerPhysicalAdmission`, `TestDeltaRangerNativeSnapshotAdmission`, `TestDeltaRangerRetainedNativeValidation`, `TestDeltaRangerRetainedQuery`, and `TestDeltaRangerAppendTargetReceipt` serially on all four pins. Archive each cell before the next; do not rerun the native-only characterization. Retain launcher/resolved JAR hashes including Ranger 2.8 and source hashes; distinguish that from in-JVM loaded origins. Do not read or overwrite the current component's moving build outputs while drafting or scheduling this task.

Acceptance requires all eight named groups, actual healthy instrumentation controls, complete explicit phase accounting, exact storage outcomes, unchanged production source and reviewed evidence across four pins. A test expecting a characterized rejection can pass only when it asserts the intended companion cause, phase and no downstream effects; generic `assertThrows(Exception)` alone is insufficient. Unexpected success is not automatically a qualification: inspect whether it bypassed actual installed callbacks or committed native work.

Root stages only the two new test files and reviewed result document after required checks. Root chooses the local commit message from the final measured scope; no push/PR/service action. If source findings require changes to production admission, stop that lane at the documented boundary and produce a separately reviewed concrete production plan rather than expanding this test-only task.

## Known unresolved questions, resolved ownership of decisions

- Actual command input may be unavailable after the first current rejection, and load order may replace source/target bindings. The observer and paired load-order cases resolve reachability; they do not invent accepted `analyzedQuery` or shared authority.
- First-write reset inside the helper may remain unreachable until native internal provenance is implemented. Duplicate reset can be measured independently through native idempotency. Report which was observed; do not count independent observations as one successful append lifetime.
- Helper internal plans, checksum-absent reconstruction and transactional source preparation can stop at different gates. The dossier must distinguish independent seam probes from a continuous end-to-end trace.
- Owner-dependent current Gravitino authorization, native recache in cloned sessions, command-result replay, actual commit gates and live S3/Ranger delivery remain in the subsequent integrated acceptance contract. None is qualified by this harness.
- No unavoidable user design choice is identified. Existing constraints already select native transaction delegation, independent source SELECT/target UPDATE, exact provenance, unchanged core and fail-closed admission. The question is which observed ownership seam must be implemented first, a technical result for the next plan.

## Plan self-review

This plan intentionally covers characterization of the coupled seams, not the entire append specification. Every new helper is explicitly test-only; every consumed production/native name was checked in source or the accepted immutable compiled probe. The added resolution observer has a mandatory healthy control and grants no authority. No generic optimizer/columnar bypass, copied receipt, manual native reset, missing-phase success claim, production hook, cleanup or service mutation is part of the task. Existing full append/live-service requirements remain in the referenced spec and later gate rather than being declared complete here.

## Root preflight and accepted prerequisites

The target-authority matrix exposed two fixture hazards to avoid here: Hadoop createUserForTesting changes shared same-user group mappings, so derive genuinely different membership and restore original mappings in finally; DeltaTableV2.initialSnapshot may reuse a cached head, so establish intended current fixture versions through explicit native refresh where required rather than timing assumptions. Preserve intentionally retained older snapshots in drift tests. These are test setup requirements, not production exemptions.

The target-authority component is accepted at `24e7b3736`; its final matrix report and committed source hashes were independently audited. Root has checked this plan against the approved append design and the current receipt/planner interfaces. This task implements only the characterization harness. No production admission, remote push or deployment is included.


## Execution scope and root responsibilities

The single implementer owns the two new test sources, the results document and
the new plan workspace. Existing referenced evidence is read-only input. Root owns
Git, runtime scheduling and independent acceptance. Preserve all private evidence;
no worker commits or workspace cleanup. No other build or source implementer runs
in parallel. Use process-scoped `caffeinate -i` for local Gradle verification to
avoid idle sleep; preserve actual wall times and never alter global sleep settings.

Run focused primary development first, then freeze the sources and obtain source
review before the serial four-pair combined regression. Archive each cell before
the next replaces build outputs. Existing warning cleanup is outside this scope.
The incomplete full append/live-service/CDF requirements remain explicitly tracked
in the approved spec and subsequent integrated gate.

## Accepted characterization checkpoint

All eight groups are implemented and reviewed. The final combined matrix passed 82 tests per pair (328 total, zero failures/errors/skips) on Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 with Ranger 2.8.0, Scala 2.12.21 and JDK 17.0.16. Scoped formatting checks passed. Root independently audited all 32 JUnit files, 124 fixture cases, archived storage/runtime artifacts and unchanged production sources. The results are recorded in `docs/delta-installed-append-boundaries.md`.

Checked steps denote completed characterization, including explicitly recorded first blockers and phases not reached. Native reset, actual V1 writer input, commit, recache and live S3/Ranger append are not qualified by this checkpoint. Installed append remains closed. Exact command/source association and finite native transaction/helper provenance are the next dependencies; CDF remains first after Delta maturity. Keep the branch and all private evidence locally.
