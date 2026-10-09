# Delta Native Idempotency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Execute the installed native `hasBeenExecuted` transaction-state read, including a cold computed-state Aggregate and actual physical callbacks, under exact target authority, returning only its Boolean before source preparation or writing.

**Architecture:** A private phase-specific entry point owns native `withNewTransaction` and its lifetime. A checked target-owned transaction binding authorizes only the selected snapshot's cached state backing, with a finite transaction-only logical route and existing logical/physical provenance checks. Entry, transaction-state and exit validation use sequential bounded native scopes; append command admission and the inert association remain unchanged.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12, JDK 17; JUnit 5 and the installed local native fixtures.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`.

## Global Constraints

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- S3-compatible native storage; local MinIO qualification is permitted. Preserve existing SELECT fixture and quarantined callback cases.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal.
- Keep accepted runtime packaging/dependency isolation. No remote push, demo change or service replacement in the initial local implementation task.
- This finite slice requires `spark.databricks.delta.write.txnVersion.autoReset.enabled=false` before capture. Reject reset-enabled input; never toggle/reset/restore configuration. Native reset continuation remains subsequent work.
- No command enablement, source issuance/preparation, helper invocation, `SetTransaction` generation, commit, recache, auto-compaction or live qualification. A true result is a native idempotency result, not a completed or successfully skipped governed append.
- Keep full original source/target decisions in surrounding tests. No prewarming `Snapshot.transactions`, transaction-map substitution, source-owner impersonation, raw-path production input or arbitrary execution callback.
- One source/build worker, serial Spark/Gradle runs, immutable per-run evidence. Root owns release gates, final acceptance and all Git mutations. No commits or other Git mutations by implementation/review workers.
- CDF remains the first feature priority after Delta maturity.

## Review Focus

- A completed successful native operation normally retains scan evidence: transaction evidence must expire when its phase closes, including escaped logical and physical tickets (Task 2).
- A same-location foreign catalog or same-log foreign descriptor is not the registered target; identity, publication and native snapshot compatibility all matter (Task 2).
- `stateDF` can be cached while computed state is cold, or reconstruction can be uncached; only the measured first case is a positive path (Tasks 1–2).
- A callback can change subject/groups/full SQLConf or policy/registration publication while the transaction remains active; no Boolean may escape failed revalidation (Task 2).
- Empty/malformed/session/explicit idempotency inputs and reset settings take distinct native branches; option precedence and no-op behavior must remain native, with denied UPDATE/source controls even for a duplicate (Task 2).

---

## Evidence and release boundary

Planning base: `aac535933ea0b5cc851ba468a63b338d20147d09`, worktree `/Users/jacobo.calderon/Code/Forks/gravitino-delta/.worktrees/delta-spark-feasibility`. Read these alongside the spec:

- `.superpowers/sdd/2026-10-09-delta-append-command-association/next-boundary-assessment.md`.
- `.superpowers/sdd/2026-10-07-delta-installed-append-boundaries/next-implementation-map.md`.
- `docs/delta-installed-append-boundaries.md` and the previous immutable matrix dossiers.

The earlier installed test proves a native transaction reaches `hasBeenExecuted → txnVersion → Snapshot.transactions` and rejects an Aggregate. It does **not** inventory every expression, encoder and physical transition under the new transaction owner. Task 1 must inventory actual inputs before production changes. If cached state plus cold computation cannot be obtained through the installed fixture without warming transaction state, or the exact recipe differs from the finite existing recipe, stop at an evidence gate and send the concrete getter/plan/callback blocker to root. Do not replace the intended positive with a warmed map or broaden admission speculatively. Only root releases Task 2 after that gate.

No memory search/save tool was available during planning; discovery returned no claude-mem capability. Source inspection was used, with no build, service, production/test edit or Git mutation.

## File map and verified interfaces

Abbreviations below expand to `spark-connector/v3.5/spark-delta-ranger/src/{main/java,main/scala,test/scala}/org/apache/gravitino/spark/connector/delta/ranger/`: **J**, **S**, **T**, respectively. These are path abbreviations, not new directories.

| File | Responsibility and planned change |
| --- | --- |
| Create T/`NativeIdempotencyProbe.scala` | Dedicated fixture, observations, fault injection and immutable evidence; no execution authority. Reuse existing fixture APIs without changing old tests. |
| Create T/`TestDeltaRangerNativeIdempotency.scala` | Inventory, installed positive/negative, lifetime, drift and storage tests. |
| Modify J/`DeltaRangerRegistrationBindings.java` | Add a private-anchor checked transaction binding and scoped liveness. No anchor getter. |
| Modify J/`DeltaRangerNativeOperation.java` | Concrete transaction owner/callback, state-only capture and nontransferable phase evidence. Existing load/retained/scan semantics remain intact. |
| Modify S/`DeltaRangerPlanAdmission.scala` | Early transaction-only finite branch; reuse existing Aggregate/encoder/first recipes, exclude generic native state-derived plans. |
| Create S/`DeltaRangerNativeIdempotency.scala` | Internal `DeltaCommand` mix-in, owned transaction, native options and scalar result. |
| Create `docs/delta-native-idempotency.md` | Final measured boundary and matrix, written only after qualification. |

No change to `DeltaRangerAppendCommandBinding`, `DeltaRangerNativeProvenance`, `DeltaRangerPhysicalPlanning`, `DeltaRangerPhysicalProvenance`, the SELECT fixture, old installed-boundary probe, Gradle dependencies or runtime rule manifests is planned. A concrete failure requiring one of those files is a root review gate, not implicit scope expansion.

Verified consumed interfaces:

```scala
// Native Delta 3.3 source, pinned locally under task-3-evidence/pinned/.
DeltaLog.withNewTransaction[T](catalogTableOpt: Option[CatalogTable],
    snapshotOpt: Option[Snapshot] = None)(thunk: OptimisticTransaction => T): T
OptimisticTransaction.getActive(): Option[OptimisticTransaction]
// txn.deltaLog, txn.catalogTable, txn.snapshot, txn.readVersion are native getters.
// txn.txnVersion(id: String): Long records native readTxn before snapshot.transactions.
protected def hasBeenExecuted(txn: OptimisticTransaction, sparkSession: SparkSession,
    options: Option[DeltaOptions] = None): Boolean // DeltaCommand

DeltaRangerAppendTargetReceipt.capture(spark: SparkSession,
    target: DeltaRangerRegistrationBindings.BoundAppendTarget): DeltaRangerAppendTargetReceipt
// receipt.validate(session: SparkSession): Unit opens bounded retained validation scopes.
// BoundAppendTarget.requireRetained(), requireLive(), validateCurrent(SparkSession)
// retain the exact table/descriptor/log and private registration anchor.
// DeltaRangerNativeOperation.requireEvidence(SparkSession) feeds logical witness validation,
// NativeWitness.requireLive/replay and NativeTicket.verify through existing callbacks.
```

`SnapshotState.scala` calls `stateDF.select(aggregations: _*).as[SnapshotState].first()`. Existing `snapshotStateAggregate` checks fourteen named expressions, exact owned RDD/output and permitted pruning; its DV metric slots are null for the qualified ordinary table. `snapshotStateBinding` checks the resolved stock `SnapshotState` encoder against recorded Aggregate outputs, normalizes binding/lambda IDs and rejects injected functions. Existing `GlobalLimit(1, LocalLimit(1, aggregate))` is the qualified native first recipe. Do not guess a new expression/encoder recipe or trust a Delta/Spark package name.

## Task 1: Inventory the real cold native input and pin the existing rejection

**Files:** Create T/`NativeIdempotencyProbe.scala`, T/`TestDeltaRangerNativeIdempotency.scala`; private evidence only under `.superpowers/sdd/2026-10-09-delta-native-idempotency/`.

**Consumes:** `NativeSelectFixture.withSpark`, `.extension`, `.locations`, `.wrapper`; `TestDeltaRangerNativeSelect.Fixture`; `InstalledAppendBoundaryProbe.Trace`, `fresh`, `manifest`. Existing `withCase`, `bound`, `source`, `ruleManifest` and `NativeDriver` are private to their old owners. Implement dedicated equivalents in the new probe, using their existing implementations as models; do not call or expose those private methods. The old `inspectNativePhases` lacks a pre-invocation hook inside its transaction, so the new probe needs its own private `DeltaCommand` mix-in.

**Produces:** A compiling test probe and root-reviewed `inventory.md`; no production permit. Use these test-only interfaces:

```scala
private[ranger] object NativeIdempotencyProbe {
  final case class Case(spark: SparkSession,
      policy: TestDeltaRangerNativeSelect.Fixture,
      trace: InstalledAppendBoundaryProbe.Trace)
  def withCase(name: String, preseed: Boolean = true)(body: Case => Unit): Unit
  def computed(snapshot: Snapshot): Boolean
  def inventory(spark: SparkSession, trace: InstalledAppendBoundaryProbe.Trace,
      snapshot: Snapshot): Unit
  def inspectCold(c: Case, target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      targetReceipt: DeltaRangerAppendTargetReceipt,
      sourceReceipt: DeltaRangerDecisionReceipt, options: Map[String, String]): Boolean
}
```

- [x] **Write a characterization test and its observations.** New JUnit method `inventoryFindsColdOwnedTargetAggregate` uses the dedicated wrapper, sets reset false and session `txnAppId=probe-app`, `txnVersion=1`, retains source and target receipts and invokes `inspectCold`. Expect the established Aggregate rejection; require observations of the exact active txn, selected snapshot, `_computedStateTriggered=false` immediately before native invocation, backing exposed by the actual native Aggregate child and actual Aggregate expressions. This test passes by proving a real existing rejection; do not manufacture a missing-instrumentation failure and call it production red. Task 2 supplies the runtime red for the new behavior.
- [x] **Implement read-only instrumentation.** The new wrapper uses `NativeSelectFixture.withSpark(..., Some(extension), beforeExtension=...)`. Its setup performs the genuine native preseed already used by the old test:

```scala
spark.sql("SELECT 4L id, 'prior' secret, 'keep' region")
  .write.format("delta").mode("append")
  .option("txnAppId", "probe-app").option("txnVersion", "1")
  .save(NativeSelectFixture.locations("a"))
```

  Setup finishes before the installed companion and before the storage baseline. Do not read state/transactions during setup. `computed` uses the same read-only public bytecode getter as `TestDeltaRangerNativeSnapshotAdmission`:

```scala
def computed(snapshot: Snapshot): Boolean =
  snapshot.getClass.getMethod("_computedStateTriggered").invoke(snapshot)
    .asInstanceOf[java.lang.Boolean].booleanValue()
```

  Implement `inspectCold` in a private `InventoryDriver extends DeltaCommand` and forward from the probe; its body is:

```scala
targetReceipt.validate(c.spark)
sourceReceipt.validateAction(c.spark)
target.log().withNewTransaction(target.table().catalogTable) { txn =>
  targetReceipt.validate(c.spark)
  sourceReceipt.validateAction(c.spark)
  assert(OptimisticTransaction.getActive.contains(txn))
  assert(!computed(txn.snapshot))
  inventory(c.spark, c.trace, txn.snapshot)
  assert(!computed(txn.snapshot))
  hasBeenExecuted(txn, c.spark, Some(new DeltaOptions(options, c.spark.sessionState.conf)))
}
```

  `inventory` records that exact snapshot identity, coldness and immutable metadata/context only. It must not independently invoke `stateDF.queryExecution.analyzed`: an unowned getter can itself trigger companion rejection and perturb the actual native path. Record the RDD backing and output from the native Aggregate observed during the subsequent real invocation. Task 2's owned state-only capture establishes the exact getter-return association. The surrounding JUnit assertion checks the expected SecurityException cause and storage/scope cleanup. This new private test driver installs no authority and cannot make the native Aggregate pass.

  Install an observation-only resolution rule before the companion, returning the identical `plan`. Only record a matching real active transaction's Aggregate/`DeserializeToObject`; retain classes, aliases/types/expression trees, output IDs, backing identities, snapshot version/native metadata, callback and thread. The observer must never set tags, invoke classification to manufacture evidence, replace a plan or invoke a native state getter. Inspect the already observed Aggregate child (including its Project child if present) for exact LogicalRDD fields and complete immutable output stamps. Copy the fixture's before/after byte manifests and scope-cleanup checks. Preserve actual loaded origins and full runtime versions. An earlier native constructor/getter failure is recorded as the actual reached phase; a separate shadow-getter failure is not evidence that production execution is impossible.
- [x] **Run the passing characterization and write the finite input table.** Include: cached-state/cold-computation duplicate; new app; same app higher version; explicit options differing from session; reset enabled; uncached `snapshot.uncache()` control; and checksum-absent retained-head history control based on the old history fixture's exact staged native bytes. CRC and uncache controls have their own explicit published/after baselines and expected rejection phase; do not silently classify them as supported. Source/target validation before native entry must be recorded separately from the Aggregate rejection. A repeat transaction lookup belongs to Task 2 after successful cold execution; it is not a substitute for cold evidence. Inventory both Delta patches serially on Spark 3.5.4 if their actual expressions differ; otherwise Task 3 supplies full execution coverage.
- [x] **Root gate before production edits.** Report exact matched recipe and coldness, any eager constructor/checksum work before the transaction owner, and actual first failures. A non-RDD getter or `_computedStateTriggered=true` prerequisite is a concrete blocker, not a passing positive. If the inventory cannot establish the existing finite ordinary-table recipe, stop with evidence and a prerequisite recommendation. Root alone decides revised scope and Git checkpoint.

Primary invocation (unique directory per run; replace only `inventory-01` with the recorded run name):

```bash
umask 077
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
export GRAVITINO_DELTA_INSTALLED_APPEND_EVIDENCE_DIR="$PWD/.superpowers/sdd/2026-10-09-delta-native-idempotency/inventory-01/trace"
mkdir -p "$GRAVITINO_DELTA_INSTALLED_APPEND_EVIDENCE_DIR"
caffeinate -i ./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerNativeIdempotency.inventoryFindsColdOwnedTargetAggregate -PskipITs -PskipDockerTests=true -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

Retain exact shell command, exit status, stdout/stderr and fresh XML before another build. Infrastructure failures do not count as red tests. Use existing authorized execution procedure for Gradle sandbox restrictions; no service changes or new network dependency are part of this task.

## Task 2: Own and execute the finite native idempotency phase

**Files:** All four production files in the map and the two new test files. The target/owner/native invocation form one security boundary and one integrated red/green task; do not release a disconnected validator as completion.

**Consumes:** Task 1's root-approved exact input inventory and all verified interfaces above.

**Produces:** These internal interfaces; no public API or general execute/callback method:

```java
// Nested in DeltaRangerRegistrationBindings; private constructor, non-serializable.
static final class BoundAppendTransaction implements AutoCloseable {
  SparkSession session();
  Snapshot snapshot();
  void requireLive();
  public void close();
}
// Added to BoundAppendTarget; checked bridge to the private anchor.
BoundAppendTransaction bindTransaction(SparkSession session, OptimisticTransaction transaction);

// Added to DeltaRangerNativeOperation, not a callback accepting caller code.
static DeltaRangerNativeOperation transactionState(BoundAppendTransaction owner);
void captureTransactionState();
boolean transactionState();
// Callback enum gains TRANSACTION_STATE.
```

```scala
private[ranger] object DeltaRangerNativeIdempotency {
  def hasBeenExecuted(spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      options: Map[String, String]): Boolean
}
```

- [x] **Write the positive red tests.** `coldDuplicateExecutesNativeAggregate` and `coldNewApplicationExecutesNativeAggregate` invoke the new scalar entry point, use separate fresh fixtures and require true/false respectively, with false→true `computed` transition on the *actual transaction snapshot*. Record/retain the active transaction from the observer; do not substitute a standalone transaction. Require real native Aggregate and encoder observations, actual native `.first()` query completion and physical plan evidence, and unchanged complete SQLConf/storage manifests. Add same-app/higher-version false. A compilation failure establishes only the missing interface; after adding signatures with a throwing implementation, record the semantic execution failure before implementing authority.
- [x] **Implement the checked target transaction binding.** Keep the anchor private. The binder and each `requireLive()` check active SparkSession identity, creator thread, exact active native txn, its exact selected snapshot, `txn.deltaLog == target.log`, `txn.catalogTable.get == target.descriptor`, `snapshot.deltaLog == target.log`, and retained/live original target. Read current publication through the anchor; reuse `requireCompatiblePublication` and `requireCompatibleHead` plus exact native-configuration equality. Bracket these observations with reader liveness/publication equality; do not invoke `validateCurrent` or open another operation from `requireLive`. Capture immutable full SQLConf, authenticated subject/groups and the engine snapshot stamp. At issuance authorize canonical `update` using C/D/T/* and descriptor owner, with `Instant.now()`, between repeated context/publication checks. Subsequent liveness checks recheck the frozen context/stamp, current publication and exact transaction/snapshot, so a stale policy/role engine or replaced registration fails. `close()` irreversibly invalidates the binding and is idempotent, even on exceptional scope exit. Redact `toString`/denials.

  The binding is checked association plus scoped authority, not a proof of native transaction construction history. Fresh transaction provenance comes from the production entry point owning `withNewTransaction`; callers cannot supply a transaction. Do not add a global used-transaction cache or reflection of native protected `committed` state.
- [x] **Implement state-only operation capture/lifetime.** Add a nullable concrete `BoundAppendTransaction` owner, separate state-only `Returned` storage and a new constructor selection; do not use `ScanOwner`. Constructor requires no current operation, a live transaction binding and its session. `captureTransactionState()` sets `capturing` only while reading the bound `snapshot.stateDF.queryExecution.analyzed`, with `statsCapture=false`, requires exact nonstreaming `LogicalRDD`, then freezes the exact returned RDD backing/output. Do not call `withStats`, `allFiles`, `transactions` or `txnVersion` here. Reject non-RDD reconstruction and invalidate on failure. `contains(rdd,false)` uses only this exact state; `contains(rdd,true)`, index/snapshot scan lineage and `ownsLogRoot` must not grant transaction owners generic reconstruction authority. Other factories, scan callbacks, `capture` and retained capture reject this new owner where inappropriate. Audit every enum consumer so `scan(..., TRANSACTION_STATE)` cannot create a scan-owned transaction callback.

  `requireEvidence` must additionally require the transaction operation to be active **and unclosed**, regardless of successful completion; then call binding liveness. `requireOwner` selects the new concrete binding. `succeed` permits this owner but only after native scalar return and a final liveness check. `close` clears CURRENT in finally and makes all transaction evidence unusable immediately; preserve existing successful closed scan evidence behavior. Neither a second owner nor a nested phase is allowed.

  Implement a private Java `TransactionCache` immutable stamp within `NativeOperation` rather than using `Returned` with `cacheBinding=null` or referring from Java to a newly compiled Scala class. `LogicalRDD`'s actual Spark 3.5.4 bytecode getters were checked with JDK17 `javap`: `output`, `rdd`, `outputPartitioning`, `outputOrdering`, `isStreaming`, and `otherCopyArgs` (session/origin stats/origin constraints); its case-class product comprises the first five fields. Verify the same inventory on 3.5.6. Freeze exact LogicalRDD class and backing RDD identity, each AttributeReference's exact class/name/ExprId/qualifier/dataType JSON/nullability/metadata JSON, exact stock UnknownPartitioning class and partition count, empty outputOrdering, nonstreaming flag and immutable copied `otherCopyArgs` sequence. Require the observed getter to have stock UnknownPartitioning and empty ordering; unknown input is a Task 1 gate. Compare both the original getter wrapper and every candidate to the immutable stamp; session in otherCopyArgs must retain object identity and origin option payloads retain their exact observed objects. Inventory origin options explicitly: empty options are the minimum finite recipe; nonempty origins need a concrete immutable stamp reviewed by root before production, never an assumed wildcard or unchecked mutable value. No caller supplies a Predicate or arbitrary freeze callback. This Java-only implementation can use normal imports for AttributeReference and UnknownPartitioning and Java 8 collections. Add metadata/partitioning/ordering/origin-copy substitutions to the negative tests.
- [x] **Restrict transaction admission before the generic branch.** After the existing exact Aggregate/encoder/first checks in `nativeShape`, return for the transaction case before any `SingleAction`/`Row`/`AddFile` helper or generic reconstruction checks:

```scala
if (operation != null && operation.transactionState()) {
  return plan match {
    case rdd: LogicalRDD if rdd.getClass == classOf[LogicalRDD] =>
      operation.contains(rdd, false)
    case _ => false
  }
}
```

  Those earlier checks retain their existing recipes; Task 1 must prove they describe the actual input. Pending cache evidence stays analysis-only until exact getter return is frozen. Reject generic Project/Filter over the state, allFiles/AddFile selection, withStats, raw log reconstruction, arbitrary Aggregate, encoder replacement, changed grouping/projection/limits, user receipts, functions and foreign RDD/output even while this phase is active. Do not admit an optimizer variant unless it is actually observed and root reviews a finite recipe change.
- [x] **Implement the native scalar entry point.** Use a private nested `Driver extends DeltaCommand` to access the protected native method without shadowing it. The public-to-package method rejects null inputs, nonactive session, any existing native transaction/operation and reset-enabled configuration before issuing authority. Copy the immutable options map and construct `new DeltaOptions(options, spark.sessionState.conf)`; accept only case-insensitive `txnAppId` and `txnVersion` option names in this finite API, reject duplicate case-insensitive keys, and delegate pair/number validation and session precedence to native DeltaOptions/hasBeenExecuted. No invented effective write options are needed because this phase never builds a writer. Capture its own target receipt from the supplied exact bound target; never accept a separate caller-provided receipt. Use the following sequence:

```scala
val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
val result = target.log().withNewTransaction(target.table().catalogTable) { txn =>
  receipt.validate(spark)
  val binding = target.bindTransaction(spark, txn)
  val result = try {
    val operation = DeltaRangerNativeOperation.transactionState(binding)
    try {
      operation.captureTransactionState()
      val value = Driver.read(txn, spark, nativeOptions)
      binding.requireLive()
      operation.succeed()
      value
    } finally operation.close()
  } finally binding.close()
  receipt.validate(spark)
  result
}
receipt.validate(spark)
result
```

  Define `Driver.read(txn: OptimisticTransaction, spark: SparkSession, options: DeltaOptions): Boolean = hasBeenExecuted(txn, spark, Some(options))` inside the private mix-in. `nativeOptions` is the already validated DeltaOptions instance. Entry context must remain the captured receipt's context through exit; never refresh the receipt to accommodate changes. Native finally clears active transaction; operation/binding finally blocks run on every exception. The final receipt validation occurs after the native transaction has unwound. Do not surround retained receipt validation with the state operation.
- [x] **Complete installed negative/lifetime tests before accepting green.** Use the observer's real operation/txn/plan references for adversarial checks; injected test plans are negative inputs only. Add JUnit methods with the following exact cases/assertions:

| Test method | Cases and mandatory assertion |
| --- | --- |
| `nativeOptionsPreserveSessionAndExplicitPrecedence` | Duplicate session pair true; explicit duplicate pair overriding different session pair true; explicit new pair overriding duplicate session pair false; absent pair native false; partial/malformed/unknown options reject; reset true rejects before txn/state; complete SQLConf equals entry on every path. |
| `targetAuthorityPrecedesDuplicateLookup` | Denied UPDATE with preseeded duplicate: no state Aggregate/native result. In the surrounding probe, source receipt denial before invoking scalar API also prevents lookup; repeat source validation afterward using the identical retained source receipt. This proves probe ordering only, not frontend enforcement. |
| `transactionBindingRejectsIdentitySubstitution` | Inactive txn, another log, same-log different canonical catalog/descriptor, foreign session and target registration/native ID/schema/protocol/partition/mapping/configuration drift reject at checked binding. Under a different active txn with an equal-content but distinct selected Snapshot, the original binding rejects; the production API has no snapshot argument. A newer compatible native snapshot selected by the real default transaction constructor is not itself a substitution. Reuse existing fixture mutation techniques; baseline deliberate fixture changes separately. |
| `transactionEvidenceExpiresOnEveryExit` | Capture real successful Aggregate/logical witness/native physical plan; after scope `requireExecutable`, native planning replay and physical ticket verification reject. Test failed scope, nested API invocation, closed binding, closed operation, active txn temporarily replaced and original restored, and native completed txn reactivated via test-only `OptimisticTransaction.withActive`: original evidence stays dead and scalar API rejects its nonidle entry. No production txn injection exists. |
| `statePhaseRejectsExtraNativeAuthority` | Substitute empty RDD, output ExprIds/nullability/metadata/qualifiers, aggregate expression/grouping, encoder constructor/function/field, first limits, stats RDD, allFiles/withStats and raw-log plans; each rejects while genuine owner is active. No copied tag may repair it. |
| `contextDriftAndExceptionsUnwindBeforeReturn` | Resolution-time mutation of unrelated SQLConf, subject/groups, engine policy/role stamp, registration publication, plus injected exception. No Boolean returned; no files changed; CURRENT and native active empty. Restore only test-owned mutations after the failed call and prove a separate fresh invocation works; do not restore inside production to conceal drift. |
| `uncachedStateAndMissingChecksumRemainClosed` | Fresh installed uncache and accepted checksum-absent history recipe fail at recorded boundaries; do not claim their support or warm state to bypass them. |

  Capture actual native completion with a test `QueryExecutionListener` registered through `spark.listenerManager.register`, recording the completed QueryExecution/physical plan and a bounded completion signal. Do not inject a columnar rule or replace the planner to observe physical callbacks. Native `.first()` completion under the unchanged exact columnar manifest, retained physical tickets and subsequent expiry checks establish actual physical execution; a resolution-only event does not. Listener callbacks must be observational and their asynchronous failures surfaced by the test. Do not call `executedPlan` prematurely outside its real query.
- [x] **Run green primary tests and affected regressions.** Require positive cold Aggregate execution and actual scalar result, no file/log/row/version changes, exact rule manifests, retained original receipts, and every negative's intended phase. Preserve old installed-boundary tests unchanged: their old driver lacks the new owner and must still reject Aggregate. Use these eleven exact class selectors: `TestDeltaRangerNativeIdempotency`, `TestDeltaRangerNativeOperations`, `TestDeltaRangerNativeSnapshotAdmission`, `TestDeltaRangerRetainedNativeValidation`, `TestDeltaRangerAppendTargetReceipt`, `TestDeltaRangerAppendCommandBinding`, `TestDeltaRangerInstalledAppendBoundaries`, `TestDeltaRangerNativeSelect`, `TestDeltaRangerPhysicalAdmission`, `TestDeltaRangerNativeCacheCapture`, `TestDeltaRangerReadRule`. The last two directly cover cache binding and the shared admission branch changed here. Prefix each with `--tests org.apache.gravitino.spark.connector.delta.ranger.` in the command. Format with scoped `spotlessApply`, then `spotlessCheck`; once primary is accepted also run `:spark-connector:spark-delta-ranger-3.5:javadoc` with the same version pins and JDK17. The module has Java 8 release compatibility and Javadoc `Xwerror`; no new Java dependency on the Scala executor/receipt is allowed. Do not assume checkstyle tasks exist. Missing Javadoc on new public `close`/`toString` methods is a failure.
- [x] **Freeze and request root review.** Archive changed sources/tests and hashes, diff and primary logs/XML/traces/runtime origins, assertion-to-case inventory and storage audit. Root requests independent review before the four-pair run. A missing physical transition or unknown reconstruction is a blocker to report with the actual tree; do not disable callbacks or weaken evidence for green. Root owns any checkpoint commit.

## Task 3: Qualify the focused boundary on all four pairs and document limits

**Files:** New `docs/delta-native-idempotency.md`; private evidence/scripts under `.superpowers/sdd/2026-10-09-delta-native-idempotency/`. No production changes while running the matrix.

**Consumes:** Root-reviewed frozen Task 2 source and exact primary test selectors; existing prior archive scripts as patterns, not mutable evidence to overwrite.

**Produces:** Immutable matrix and precise result document. A source fix returns to Task 2 primary/review before affected matrix reruns.

- [x] **Retain four explicit qualified run directories/scripts** for 3.5.4/3.3.0, 3.5.4/3.3.2, 3.5.6/3.3.0, 3.5.6/3.3.2, sequentially. Reuse the accepted eleven-selector primary as the first cell if its source matches the reviewed freeze and its immutable evidence meets this same contract; do not repeat that unchanged successful run solely for its directory label. Review findings requiring source changes invalidate that reuse until affected verification passes. A correction confined to the new test/probe may use a corrected full new-class overlay plus the original unchanged-production regression archive; record both exact source sets and count each qualified scenario once. Production changes require affected regression requalification. Use Task 1 command shape with full Task 2 focused selectors, `spotlessCheck` rather than `spotlessApply`, actual pins and unique trace directory. Preserve JDK17/Ranger2.8.0/Scala2.12 verification in every worker. No simultaneous builds or services.
- [x] **Run, then archive each cell before starting the next.** Retain original logs and exit/time records, fresh JUnit XML/binary/HTML, exact source copies/hashes and BASE comparison, actual Java worker command/argfile/classpath, all resolved artifact hashes, copies of moving project jars/class directories, runtime loaded origins, every trace and storage manifest plus original bytes. Follow the completeness model in the previous association `matrix-report.md`. Origin-only records or later moving-output reads are insufficient.
- [x] **Audit real execution, not just test totals.** Per cell, list each cold true/false fixture with exact native txn/snapshot, cold→computed transition, Aggregate/encoder/physical completion, SQLConf equality, scope teardown and unchanged bytes/rows/version. List each expected denial and its reached boundary separately. Confirm old append/raw-path/CTAS/RTAS controls and masked/filtered SELECT pass unchanged, and uncached/CRC-absent support is not claimed.
- [x] **Write the result document from archived evidence.** State the internal scalar phase now executes (only if all cells prove it), supported cached/cold recipe, reset-disabled contract, expiry/identity/context guarantees, exact test counts and evidence paths. Explicitly leave frontend/source authority, transaction-owned self-source preparation, reset continuation, helper/action generation, commit, postcommit and live S3/Ranger qualification open. Keep the inert association contract and current append rejection clear. No atomic policy/registration fence or full-append claim.
- [x] **Root final acceptance/Git.** Supply reviewed source freeze, primary/review evidence, matrix summary and immutable archives. Do not push or create a PR; repository instructions require a concrete preview and confirmation for PR creation, which is outside this task.

## Completion test

This plan completes only when the real installed native idempotency call, in its own exact-target transaction, executes formerly rejected cold computed-state work and its physical callbacks on all four pairs, returns true/false with full revalidation, and leaves data/log files unchanged. Another validator, synthetic transaction state, warmed transaction map, disabled callback or native-only execution session is not completion. If Task 1 exposes a prerequisite, the valid deliverable is the bounded evidence report plus root-reviewed rescope; no working execution claim is permitted.

## Execution status

Task 1 passed independent quality and plan-compliance review. Primary Spark 3.5.4 / Delta 3.3.0 inventory: three tests/eight fixtures, zero failures/errors/skips, with final 95.631-second JUnit run. Root independently audited both successful archives, all runtime/source hashes and 630/646 retained storage entries. Original source/target decisions, full SQLConf and scope cleanup are preserved. All 73 preexisting module source files remain unchanged at this gate.

Ordinary inputs reach the genuine cold fourteen-expression Aggregate over exact LogicalRDD/MapPartitionsRDD, stock UnknownPartitioning(0), empty ordering, nonstreaming and otherCopyArgs(same session,None,None). No independent unowned state getter perturbs the call. Uncached state reaches SerializeFromObject rejection inside hasBeenExecuted, while missing-checksum history fails retained target validation before transaction entry. No native Boolean or physical completion is claimed by this inventory.

Task 2 now passes independent source/spec review and primary runtime verification. The initial 106-test regression run exposed no runtime failure, but review found that three drift test branches were missing and a broad exception assertion accepted MatchError. The corrected assertion reproduced that failure; a test-only fix performs real group/policy/role mutations, checks their before/after values and requires the intended denial. The corrected full fourteen-test class passes, all four production sources remain unchanged, and scoped re-review and Javadoc pass.

Primary qualification uses the immutable original 92 unchanged regression tests plus the corrected fourteen-test class; both source layers are retained rather than counting the rerun as additional scenarios. Root independently verified runtime/source/storage bytes and all seven corrected drift cases. All three remaining pairs passed the same 106 tests with zero failures/errors/skips and unchanged source. Root independently audited all archives, actual Java workers, classpaths, source and compiled bytes, native positive/drift traces and storage baselines. The four-pair total is 424 qualified test cases and 464 traced fixtures, with the primary correction retained separately. Scoped formatting and Javadoc pass. Final independent source/integration and runtime/documentation review passes with no outstanding findings. This accepted checkpoint is kept on the existing local branch, with the worktree and private evidence preserved. Append commands remain rejected and no live-service qualification is claimed.
