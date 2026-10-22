# Governed named batch Delta CDF reads implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already selected this execution method.

**Goal:** Read a complete, explicitly bounded native Delta change feed through a Gravitino table name, applying existing Ranger permissions, filters and masks.

**Architecture:** Retain the completed ordinary named load as the registration anchor and issue a separate, driver-local CDF operation before native V2 lowering. Use an owned `CDCReaderImpl` and its `scanIndex` hook for native reconstruction, with exact history, scan and action ownership. DataFrame and SQL entry points share the same authorization and reconstruction path.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21, Ranger 2.8.0, Gravitino and MinIO.

**Spec:** [Approved CDF consumption design](../specs/2026-10-20-delta-cdf-consumption-design.md).

**Status:** Closed on 2026-10-22. All five tasks passed independent scoped review; the final integrated review is PASS / APPROVE with no blocking findings. All four focused and deployed Ranger 2.8.0 / MinIO qualification cells passed. The authorized branch push and remote commit equality are recorded in the retained closure receipt; the worktree and original evidence are preserved.

## Global Constraints

- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, JDK 17, Scala 2.12.21.
- Actual Ranger 2.8.0 and local S3 qualification with MinIO.
- Gravitino owns named registration; Delta owns the log and reconstruction.
- No Gravitino core/API/server/client changes, second metastore or Spark/Delta fork.
- Ranger owns access, column permissions, row filters and masks. The metadata identity remains separate from the authenticated application subject.
- The platform prevents concurrent writer jobs per dataset. Reads do not acquire permission to modify the source or its registration through a new writer.
- Preserve the existing worktree and accepted evidence. Reopen earlier milestones only for concrete changed dependencies or regressions.
- Require explicit integer bounds with `0 <= start <= end <= observed native head`; one batch source and the existing finite projection/filter expressions.
- Existing policies govern keys and CDF metadata too. No reader key requirement, deduplication, mask bypass, automatic policy migration or reverse-mask lookup.
- Governed destination MERGE/UPDATE/DELETE, writebacks, streaming and broader historical schema/features remain subsequent work.
- Serialize builds and live fixture ownership. Preserve original failures and private evidence; never replay uncertain mutations to obtain a passing result.

## Review Focus

1. Missing terminal commit, disabled-CDF gap or permissive file settings must fail instead of producing an apparently complete empty batch — Tasks 1–2.
2. Optimizer-empty and metadata-only reads must retain original permissions; an observed policy change invalidates a retained action — Tasks 1 and 3.
3. Updates crossing a filter boundary and colliding masked keys must preserve the permitted native images and duplicate multiplicity — Tasks 2–4.
4. A later append or a gap beyond the requested end must not expand or invalidate a complete fixed interval; inclusive iteration must not overflow — Tasks 1–2.
5. A copied scan/cache carrier or unsupported historical schema/protocol must not inherit authority from a valid current table — Tasks 1–3.

## Files and ownership

All paths below are relative to the existing worktree. These directory abbreviations apply to every task:

- `M`: `spark-connector/v3.5/spark-delta-ranger`
- `J`: `M/src/main/java/org/apache/gravitino/spark/connector/delta/ranger`
- `S`: `M/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`
- `T`: `M/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger`

New production responsibilities:

| File | Responsibility |
| --- | --- |
| `S/DeltaRangerCdfRead.scala` | Parse named requests, bind the original table, route DataFrame/SQL plans and enforce the CDF query grammar. |
| `S/DeltaRangerCdfHistory.scala` | Capture a complete inclusive interval and validate its historical schema/protocol/actions. |
| `S/DeltaRangerCdfNativeReader.scala` | Owned native reader and relation/carrier; issue each native index before nested planning. |
| `S/DeltaRangerCdfReadReceipt.scala` | Original column demands, current policy decisions, output identity and action revalidation. |
| `S/DeltaRangerCdfReadScans.scala` | Exact native helper/index/physical/RDD ownership for multiple branches of one source. |

Existing integration points: `J/DeltaRangerRegistrationBindings.java` retains the named anchor; `J/DeltaRangerNativeOperation.java` adds a distinct CDF read owner/callback. Dispatch through `S/DeltaRangerReadRule.scala`, `S/DeltaRangerSparkExtension.scala`, `S/DeltaRangerNativeProvenance.scala`, `S/DeltaRangerPhysicalPlanning.scala` and `S/DeltaRangerCollectGuardExec.scala` only where the owned path requires it. Keep ordinary scan/append receipts intact. Extract small policy-expression helpers from `S/DeltaRangerMaskRules.scala` only as needed to share its existing semantics.

Tests live beside existing installed-extension tests. Reuse `NativeSelectFixture`, `NativeBoundCatalogFixture` and the actual engine fixture. Add `T/DeltaRangerCdfReadFixture.scala` for native setup/oracles; it must not bypass installed admission for the operation being tested.

## Commands and evidence

Run from the worktree with JDK 17. Initial focused command, substituting the named test suite for each task:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerInstalledCdfRead' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

The existing init script gives the test JVM 4 GiB and one fork; the 1536 MiB setting above is for Gradle. Preserve exit status, XML, runtime pins, relevant source/class/JAR hashes and original diagnostics under `.superpowers/sdd/2026-10-20-delta-cdf-consumption/`. Do not retain redundant package copies when an identical verified artifact can be reused. Check available space before packaging. No broad reruns without a changed dependency or failure.

Each implementation task ends with focused validation, a scoped review using the selected subagent workflow, and a local commit. Stage only its named files. Push once at milestone closure as already authorized; do not create a PR.

## Task 1: Working owned DataFrame CDF read

**Files:** Create the five production files above, `T/DeltaRangerCdfReadFixture.scala`, `T/TestDeltaRangerInstalledCdfRead.scala` and `T/TestDeltaRangerCdfHistory.scala`; modify the listed integration points. This is one vertical feature: an internal ungoverned prototype is not its acceptance result.

**Interfaces:**

- `DeltaRangerCdfRead.Request(start: Long, end: Long)` is a pure value, never authority. `parse(options: Map[String, String], observedHead: Long): Request` rejects conflicts before case normalization and rejects every unsupported option.
- `DeltaRangerCdfRead.resolve(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan]` captures named CDF before native copying/lowering; `None` means unrelated work, not a recognized but unsupported CDF request. Unsupported requests throw.
- Add package-private `BoundCdfRead bindCdfRead(DeltaTableV2 original)` in `DeltaRangerRegistrationBindings`. Its new nested `BoundCdfRead` retains the exact ordinary anchor and exposes `CurrentObservation validateCurrent(SparkSession session)` and `BoundRegistrationScan anchor()`. Identity lookup, not descriptor equality, issues it.
- `DeltaRangerCdfHistory.capture(binding: BoundCdfRead, request: Request, spark: SparkSession): ValidatedInterval`. New nonserializable `ValidatedInterval` retains the exact read-schema snapshot and validated version/actions; only the owned reader can consume its action iterator.
- `DeltaRangerCdfReadReceipt` is a nonserializable, privately issued class with `validateAction(spark: SparkSession): Unit`; it retains the original binding, interval, subject, decisions, column obligations and output attributes. It is not an ordinary `DeltaRangerDecisionReceipt`.
- `DeltaRangerCdfNativeReader.read(interval: ValidatedInterval, owner: DeltaRangerCdfReadScans, spark: SparkSession): DataFrame` invokes inherited `changesToDF(...).fileChangeDf` with the same validated actions and supplied start snapshot. Its override is `protected def scanIndex(spark: SparkSession, index: TahoeFileIndexWithSnapshotDescriptor, isStreaming: Boolean = false): DataFrame`.
- `DeltaRangerCdfReadScans` exposes `requireLive(): Unit`, `issued(index: TahoeFileIndexWithSnapshotDescriptor): Unit` and `seal(plan: SparkPlan): Unit`. Its private identity inventories bind the original interval, every native branch and exact physical/RDD children. A distinct `CDF_READ` callback and owner in `DeltaRangerNativeOperation` dispatch to this authority, never to append/commit authority.

- [x] Write `namedBoundedReadUsesOwnedNativeBranches` in `TestDeltaRangerInstalledCdfRead`: native setup creates inferred inserts, explicit update/delete CDC, and an inferred delete. Through the installed governed extension and named DataFrame API, assert the full row multiset equals independent native CDF, all three index kinds were issued by the same operation, and the current registration schema contains no virtual CDF fields.
- [x] Add `deniedAndFilteredReadCannotEscape` to the same suite: deny a demanded payload column and assert failure; apply `region = 'US'` plus a constant key mask and assert only authorized images, masked keys, and unchanged duplicate counts. Include a predicate on the masked key and a metadata-only projection. These are required for the first working read.
- [x] Run the focused command for both new suites and retain the original RED: named CDF is rejected or required owned interfaces are absent. Do not replace installed execution with a component-only rewrite test.
- [x] Implement request parsing and exact named binding; reject path, streaming, unknown/conflicting options, time travel, missing bounds, timestamp bounds, invalid range and unsupported query shapes. Preserve exact original V2/descriptor identity before native copies. Never mint a named anchor from a path or matching URI.
- [x] Implement `capture`: use public `DeltaLog.getChanges(start, failOnDataLoss = true)` with explicit inclusive consumption. Check first, every intermediate and terminal version. Stop immediately after `version == end`, before another `hasNext`; do not use `takeWhile`, `end + 1`, an Int-sized interval count or inaccessible `private[sql]` overloads. Feed these same validated actions into reconstruction. Validate current and historical schema/profile, CDF enablement, required files and rejecting permissive session settings.
- [x] Implement the native reader/carrier and exact owner before enabling the positive path. Inherited `getCDCRelation` calls the singleton and cannot provide ownership: the owned carrier must call the owned reader. Bind nested planning, native empty RDD/union and index matching before they execute; retain driver-local authority at physical action boundaries. No class-name, thread-local-only or generic raw-plan allowance.
- [x] Implement policy receipt/rewrite: capture all original demands, filter original images, mask payload and virtual columns with the existing expression grammar, then apply user expressions. Revalidate subject/configuration/registration/native profile/current decisions at action boundaries. Ensure optimizer-empty plans still validate their original obligations. Keep helpers private and authority nonserializable.
- [x] Run both suites GREEN and affected existing registration/read/mask/physical tests. Record whether the protected native hook works on Spark 3.5.4 / Delta 3.3.0. If it requires a fork or broad admission, report the concrete failing boundary before building more surfaces. Commit as `feat(delta): add owned named batch CDF reads` after scoped review.

## Task 2: Complete historical intervals and native event fidelity

**Files:** Extend `S/DeltaRangerCdfHistory.scala`, `S/DeltaRangerCdfNativeReader.scala` and `S/DeltaRangerCdfReadScans.scala`; tests `T/TestDeltaRangerCdfHistory.scala`, new `T/TestDeltaRangerCdfNativeParity.scala`, and the shared CDF fixture.

**Interfaces:** Consume Task 1's `capture`, `ValidatedInterval` and `read` unchanged. Add fixture `rowMultiset(rows: Seq[Row]): Map[Row, Int]` for equality including repeated rows; never compare with `toSet`. History tests use a counted synthetic iterator for boundary arithmetic and actual Delta logs for runtime completeness.

- [x] Add `completeInclusiveIntervalStopsAtEnd`: assert start=end succeeds, missing start/middle/end fails, and an iterator throwing on access after end is never advanced. Exercise a terminal `Long.MaxValue` in the isolated counter test without creating that many commits. A later compatible append changes neither selected rows nor requested end.
- [x] Add `historicalFailuresAreNotEmptySuccess`: remove/corrupt requested log, CDC and removed-data files in separate owned fixtures; assert failure. Include disabled-CDF gaps, out-of-range and ignore-missing/ignore-corrupt settings. A missing unrelated version beyond a complete end must not be examined.
- [x] Add `nativeEventMultisetIsPreserved`: append inserts, native UPDATE/DELETE/MERGE fixture events, explicit-CDC priority over associated adds/removes, inferred full-partition deletes, mixed/partitioned commits, empty and dataChange=false commits. Assert exact native metadata types/values and all four event types without assuming row order.
- [x] Add `historicalProfileCannotHideBehindCurrentMetadata`: schema changes (including change-and-restore), partition/profile transitions, column mapping, deletion vectors, row tracking and unsupported protocol features fail. Native metadata-name collisions also fail. Assert fixed interval validity after compatible current-head advancement and failure after identity/profile drift.
- [x] Run the focused command with `--tests '*TestDeltaRangerCdfHistory' --tests '*TestDeltaRangerCdfNativeParity'`; retain RED diagnostics for missing behavior. Complete the bounded evidence/profile/index checks, then rerun to GREEN. Include the installed positive test if reconstruction changed. Commit as `fix(delta): enforce complete native CDF intervals` after scoped review.

## Task 3: Named SQL, current policies and retained-action ownership

**Files:** Extend `S/DeltaRangerCdfRead.scala`, `S/DeltaRangerCdfReadReceipt.scala`, `S/DeltaRangerCdfReadScans.scala` and their dispatch sites; create `T/TestDeltaRangerCdfSql.scala`, `T/TestDeltaRangerCdfReadPolicies.scala`, `T/TestDeltaRangerCdfReadOwnership.scala`.

**Interfaces:** Normalize named `table_changes(name, start, end)` through the same `Request`, `resolve`, binding and receipt as DataFrame reads. Do not introduce another authorization API or a path-to-name resolver. Existing `validateAction` remains the lifetime boundary for both surfaces.

- [x] Add `namedSqlMatchesDataFrame`: assert multiset equality for explicit integer bounds and finite projection/filter queries; reject `table_changes_by_path`, timestamp forms, missing bounds, joins, aggregates, ordering, streaming, cache/persist and raw RDD admission. Cover quoted multipart identifiers without accepting path-like first arguments.
- [x] Add `policiesApplyToEveryNativeImage`: test US→US, US→EU, EU→US and EU→EU for `region = 'US'`; assert the respective native images survive independently. Mask distinct keys to one constant and preserve both rows. Apply SELECT denial and masks to each of `_change_type`, `_commit_version`, `_commit_timestamp`; test wildcard grants, metadata-referencing row filters, metadata-only reads and unsupported policy expressions. Do not require any unique/unmasked key.
- [x] Add `retainedActionRevalidatesOriginalObligations`: after analysis or a successful first action, change each of grant/revoke, role, filter, mask, subject, configuration and time-dependent effective decision. Assert retained action failure and successful fresh read only when currently authorized. Include zero-row optimizer results and denied columns eliminated from optimized output.
- [x] Add `foreignHistoryAndCachesCannotBorrowAuthority`: copied/serialized carrier or receipt, foreign descriptor/index/output/RDD/physical child, derived plans and a mismatched interval fail. Exercise unchanged repeated actions and cold/warm native reconstruction caches in both source-first orders; do not clear global caches to make tests pass. Inject reconstruction failure and prove owner cleanup does not authorize the next unrelated query.
- [x] Run the focused command for the three new suites; retain RED. Implement the SQL normalization and any narrowly demonstrated policy/provenance fixes. Keep one source with multiple issued native branches; do not apply ordinary one-index-per-obligation counting to CDF.
- [x] Rerun these suites to GREEN and affected existing SELECT/append/CDF-property tests where shared helpers changed. Commit as `feat(delta): qualify governed CDF SQL and action lifetime` after scoped review.

## Task 4: One deployed Ranger 2.8.0 and MinIO acceptance cell

**Files:** Create `delta/acceptance/cdf_read_live.py`, `cdf_read_live_checks.py`, `test_cdf_read_live_checks.py`, `cdf_read_native.py`, `cdf_read_live.gradle` in that directory; create `T/DeltaRangerLiveCdfRead.scala`. Reuse `cdf_live.py` transport/provisioning and `cdf_live_package.py` artifact verification; modify packaging only where it must accept the new thin application. Keep the existing enablement application's assertions intact.

**Interfaces:** `cdf_read_live.py --runtime <runtime.json> --inputs <private-inputs.json> --out <fresh-directory>` consumes the existing runtime/private-input schema and writes `result.json` plus private traces. `run_cell(runtime_path: Path, inputs_path: Path, output: Path) -> None` is the harness entry. `cdf_read_live_checks.verify_result(result: dict) -> None` rejects incomplete/mismatched evidence. Native setup accepts one private JSON input file and emits an oracle result; governed `DeltaRangerLiveCdfRead.main(args: Array[String]): Unit` accepts one private request file and writes its observed result.

- [x] Write checker RED tests: missing requested terminal coverage, missing/altered multiset entries, duplicate-count loss, wrong runtime/Ranger version, unauthorized raw metadata, absent public grant download or absent revoke denial cannot pass. Run `python3 -m unittest discover -s delta/acceptance -p 'test_cdf_read_live_checks.py'`.
- [x] Implement the small CDF read app and harness. Provision fresh resources with separate setup/metadata/application identities and production provider discovery. Seed native UPDATE/DELETE/MERGE only through controlled setup. Capture native oracle before policy projection; assert the exact expected filtered/masked multiset in the governed result. Compare source log/registration state before/after reads to prove no reader writeback.
- [x] Prove public Gravitino SELECT grant/revoke reaches actual Ranger 2.8.0 plugin downloads, plus Ranger-authored row filters and key/metadata masks. A retained operation rejects after observed policy change; a fresh operation follows current policy. Include empty successful intervals and incomplete denied/failed intervals as distinct outcomes. Verify persisted registration identity without treating mocked transport as deployment proof.
- [x] Add `cdfReadLiveThinJar` to the new init script; retain the existing Hadoop pins/test memory behavior. Run the checker suite GREEN, package the final first-pair source, then run `python3 delta/acceptance/cdf_read_live.py --runtime "$cdf_runtime" --inputs "$cdf_inputs" --out "$cdf_output"` with fresh private paths recorded in the evidence ledger. Require original exit 0, checked result and exact artifact/runtime binding. Preserve a failing fixture; diagnose before another run.
- [x] Commit harness and any proven integration fix as `test(delta): qualify CDF reads with deployed Ranger 2.8` after scoped review. If production code changed here, rerun its focused tests before treating this cell as final.

## Task 5: Final four-pair qualification and milestone closure

**Files:** Update `M/README.md`, `delta/acceptance/README.md`, `docs/delta-integration-status.md`; create `docs/delta-cdf-consumption.md`. Update this plan's checkboxes and retained evidence index. Production changes are only justified by a concrete remaining failure and return to their focused tests.

**Interfaces:** Consume the final Task 4 harness and verified artifacts. Result documentation maps each runtime pair to its original invocation, artifact/source hashes, test XML, deployed Ranger version and accepted result; no fixture status is inferred from a different pair.

- [x] Run new/affected focused suites for (3.5.4,3.3.0), (3.5.4,3.3.2), (3.5.6,3.3.0), (3.5.6,3.3.2), substituting both properties in the documented command. Reuse the first cell if its source/artifact inputs are still final. Run scoped formatting with `:spark-connector:spark-delta-ranger-3.5:spotlessApply`, then `spotlessCheck`, relevant Java checkstyle/Javadoc tasks and Python checker tests. Resolve actual failures; report checks not runnable rather than implying they passed.
- [x] Package each changed final pair and run Task 4's deployed command sequentially against actual Ranger 2.8.0/MinIO. Require production discovery, public policy delivery, exact governed event multiset and failure cases for each. Maintain a four-row status table; one cell's success never substitutes for another's. Do not repeat unchanged closed append/enablement matrices.
- [x] Document both named APIs and exact bounded/profile restrictions. Explain row-image filter transitions, policy-owned key/metadata masks, duplicate preservation and absence of pairing/order guarantees. Show consumer checkpoint advancement from the successfully processed requested end after sink success, not maximum visible metadata; distinguish empty success from failure. Explain policy-only changes require consumer reconciliation and receipts are not durable checkpoints. Mark destination MERGE/writebacks as the next functional ELT milestone; no claim of complete Delta or end-to-end ELT support.
- [x] Obtain one integrated review of the final implementation/spec/qualification evidence. Fix concrete findings with targeted reruns. Verify clean intended diff, final artifact bindings and all four acceptance results, then commit as `docs(delta): close governed batch CDF read milestone` and push `codex/delta-spark-feasibility` to its existing remote. Verify the remote commit equals the closure commit and retain the worktree.

## Inline plan review

The spec's named API, policy precedence, ELT limitations, native ownership, completeness, historical profile, cache lifetime and qualification sections map to Tasks 1–5. All five review-focus conditions have explicit tests. Shared interfaces are defined in Task 1; SQL reuses them and the live harness consumes public behavior. The first executable gate includes authorization rather than postponing it to the live matrix. No new key policy, full-dataset replica promise, governed mutation support or earlier-milestone replay was added.


### Task 5 qualification evidence

All four final focused cells passed (162 tests each, zero failures/errors/skips),
with scoped SpotlessApply/Check; first-pair Javadoc passed. This module has no
checkstyleMain/checkstyleTest tasks; no checkstyle pass is claimed. The strengthened
checker suite passes 11/11. Accepted review findings now require newer regrant
SELECT/download evidence and identical Spark+Delta pins for companion reuse.

The original accepted first live cell remains immutable and passed strengthened
validation with complete production/app source, runtime artifact and observed
provider equality. Each remaining pair has its own requested-pin package, complete forced-shadow entry
equality proof, and
original Ranger 2.8.0 / MinIO fixture, outer0/child0, checked result and no failure
marker. Exact invocations, source/class/JAR hashes, XML, policy downloads, current
documentation-only deltas and original receipts are indexed in retained
`.superpowers/sdd/2026-10-20-delta-cdf-consumption/task-5-report.md` and
`task-5-matrix-index.json`. Public contract and limits:
[governed batch CDF consumption](../../delta-cdf-consumption.md).

No closed append/enablement live matrix was replayed. Task 5 scoped review and the
single integrated review both passed without new findings at implementation commit
`5d282987d385250d254f6016d5cf44ebe3d4c6c1`. Their full reports are retained as
`task-5-review.md` and `final-review.md` in the evidence directory above. Closure
changes only this plan; `closure-source-delta.json` binds that documentation delta
to the reviewed source inventory, and `closure-receipt.json` records the closure
commit, authorized push and verified remote equality. No production or application
source changed after qualification. Governed target MERGE/upserts and writebacks
remain the next functional ELT milestone; streaming and broader Delta profiles
remain separate work.
