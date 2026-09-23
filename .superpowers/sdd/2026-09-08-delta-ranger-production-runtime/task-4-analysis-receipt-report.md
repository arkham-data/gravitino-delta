# Task 4: production logical decision receipt

Base: `513e13ba1ee85dd8226f2785c636fd2b76071f79`. Scope is the amended
`task-4-analysis-receipt-implementation-brief.md` and optimizer reconciliation.
This implements logical analysis/check/pre-CBO authority. It does not install
physical guards, native-operation tickets, or retained-action admission.

## Production change

Every original named LogicalRelation resolves through the canonical actual
`DeltaRangerCatalogSpark35.registrationBindings().resolve`. Its bound scan supplies
the engine; the extension no longer accepts an independently selected engine.
The production resolver uses the session catalog manager and requires the actual
wrapper type. Test forwarding catalogs delegate real loads to actual wrappers;
the injected test resolver returns those same wrappers.

The driver receipt retains the authenticated subject/groups, each original scan
occurrence and canonical resource/owner/columns, exact bound scan, each owning
engine's opaque publication stamp, one batch Instant, and the captured optional
filter plus per-column MaskDecision (including NONE). All original access requests
finish before any filter/mask request. Rewrite uses captured decisions only and
keeps Kyuubi stage 0/stage 1/marker-removal ordering and existing expression checks.

Configuration is cloned before policy evaluation. Observed changes reject before
rewrite; rewrite and optimizer replay run under the private snapshot. Subsequent
consumption requires the same session and complete SQLConf map. Original metadata
publications are reread with the existing finite reader and checked with
samePublication and owner/lease liveness. After that work, frozen original and
candidate bindings are checked again, followed by engine stamps and current
subject/groups. This ordering catches a metadata callback that changes an
already-compared tree. No new current-native-head claim is made.

The plain driver-owned receipt is neither Product nor Serializable. A constant-
rendering Serializable reference stores its authority in a transient field.
Serialization produces an inert reference whose consumption fails closed.
Plan JSON/rendering and the serial reference do not expose the authority graph.

The closed tree binding checks exact supported node/expression classes, child
order, outputs and producers, ExprIds including UUID, names/qualifiers/types/
nullability/metadata, alias secondary fields, cast and arithmetic modes, and
literal values (including raw floating-point bits). Mutable literal bytes are
copied into the frozen binding. Retained-tree checks around replay and metadata
prevent shared payload changes from becoming authority. Surviving sources require
exact native relation, file index, file format, DeltaLog and descriptor identities,
plus schema/descriptor values. Ordinary Spark clones preserving these bindings
pass; copied references do not authorize changed sources, expressions, outputs,
children or reused occurrences. Removed scans remain in the original inventory.

A Ranger-package SparkOptimizer subclass replays only the qualified stock prefix
before Pre CBO Rules on the retained approved clone, under private SQLConf. The
actual ordered prefix's rule classes and strategies must match the stock replay
plus typed native RangePartitionIdRewrite, respecting Spark exclusions. Exact
stock pre-CBO rules precede the Ranger consumer, which must precede native/custom
pre-CBO work and cannot be excluded. Qualification happens during analysis and
consumption, before an unknown injected optimizer callback can run. The incoming
candidate is compared directly with the replay result; it is never optimized into
acceptance. No reflection or private Spark-package bridge is used.

Changed production files are RegistrationBindings, ReadRule, MaskRules,
SparkExtension and the new DecisionReceipt. Native admission, core/API/server/
client/base-catalog sources, engine-time APIs and physical guard installation are
unchanged. The README is root-authored. Frozen hashes cover all 47 module source,
README and build/format files, not just the 14 changed/new module files.

## Behavioral evidence

The 23 receipt tests cover original self-join inventories, both catalog owners,
all-access-before-decision ordering, capture once including NONE, one request time
across engines, clone admission, serialization, copied/changed literals, alias and
ExprId changes, removed mask Projects and Filters, copied descriptors, same-path foreign
sources, mixed/reused children,
publication/lease/close changes, same-number engine republication, subject/groups,
configuration drift during and after evaluation, and post-metadata tree mutation.
They also cover qualified optimizer replay through IN/InSet, folding, aliases,
filter pushdown/pruning and empty results; removed-source obligations; no replay
jobs or source preparation; unknown rule/evaluator non-invocation; stable and
special datetime casts; signed zero; and explicit feature boundaries below.

Existing installed native controls still return exact plain/filter/masked rows,
two-catalog rows in both orders, quoted mapped-column rows, and supported symmetric
self-join rows. Registration, native snapshot and cache-getter characterization
regressions remain covered. Pure extraction/instrumentation fixtures do not
silently install the companion merely because they install an observer extension.

Observed behavioral REDs (original XML/logs retained):

| Run | Observed failure before correction |
| --- | --- |
| `analysis-receipt-red-3` | Committed subject-tag admission accepted a changed literal: expected SecurityException, none thrown. |
| `analysis-receipt-hardening-red-2` | Unknown optimizer callback ran three times before rejection; expected zero. |
| `analysis-receipt-hardening-red-3` | Unsupported foldable cast child evaluated once before rejection; expected zero. |
| `analysis-receipt-limits-red-2` | Signed-zero change and special datetime keyword with timezone suffix were accepted. |
| `analysis-receipt-config-red-1` | Policy-time SQLConf mutation was adopted instead of rejected. |
| `analysis-receipt-late-tree-red-1` | Metadata callback changed an already-compared literal and consumption returned successfully. |

These are specific observed REDs, not a claim that every new assertion was run
against the untouched baseline. `analysis-receipt-frozen/run-history.json` retains
all intermediate runs, counts, original XML hashes and every failing test name.
Setup/test-construction failures are not counted as behavioral RED: initial
sandbox Gradle-cache lock, formatting gate, Scala Batch/Alias/helper compile
errors, static test configuration mutation, an instrumentation-only fixture's
missing policy, external-source Spotless restriction, and a copied cache plan
whose pre-existing tags prevented copyTagsFrom from installing the receipt.
Intermediate stock pre-CBO ordering, empty-result producer and new-session fixture
failures were corrected and remain archived. Expected-message mismatches remain
visible rather than being reported as authority failures of the desired kind.

## Native asymmetric self-join boundary

The following installed query still reaches a pre-existing native-operation gap:

```sql
SELECT x.secret AS first, y.secret AS second
FROM a.db.t x JOIN a.db.t y ON x.id = y.id
WHERE y.region = 'keep'
```

`analysis-receipt-baseline-native-control-2` compiled the committed base's Scala
sources via an explicit private source-set override. All 12 other Scala files
were byte-identical to the base; the thirteenth only added the installed control.
The original direct-rule component test passed; the identical SQL with the
committed production extension failed at PreparedDeltaFileIndex.matchingFiles ->
Snapshot.filesForScan on the broadcast worker with `Unqualified Delta Ranger route:
LogicalRDD`. The current installed query has the same failure after successful
logical admission. This is not attributed to receipt comparison.

The override, source hashes, original commands/XML and actual compiled inputs are
retained under `analysis-receipt-baseline-native-control-inputs` and
`analysis-receipt-baseline-native-control-2`; root independently verified them in
`analysis-receipt-baseline-root-verification.json`. The first baseline attempt was
only a Spotless setup failure on external archived sources. Its second diagnostic
excluded that module's Scala formatting tasks only; final production rows exclude
no formatting checks.

Per the amended brief, the original exact component rows remain in a separately
named component fixture using actual wrapper-bound scans, production capture/
rewrite and Kyuubi rules, without any fabricated receipt or alternate production
ReadRule. The installed action is a precise known-gap characterization retaining
its failure and stack boundary, plus an acceptance target for typed native
operations. Its passing characterization test does not claim full asymmetric
self-join action support. AQE, caching and data skipping are not disabled.

## Final matrix and frozen inputs

Original frozen full regression matrix (Ranger 2.8.0 in every row):

| Spark | Delta | Original XML executions | Fail / error / skipped | Evidence directory |
| --- | --- | ---: | --- | --- |
| 3.5.4 | 3.3.2 | 128 | 0 / 0 / 0 | `analysis-receipt-final-354-332` |
| 3.5.4 | 3.3.0 | 53 | 0 / 0 / 0 | `analysis-receipt-final-354-330` |
| 3.5.6 | 3.3.0 | 53 | 0 / 0 / 0 | `analysis-receipt-final-356-330` |
| 3.5.6 | 3.3.2 | 53 | 0 / 0 / 0 | `analysis-receipt-final-356-332` |

All four exited 0: 287 executions, 128 distinct tests. These rows retain the
original 47-source freeze in `analysis-receipt-frozen/source-hashes.json`.

After the full matrix, final proof inspection found that the existing
`changedOutputsRemovedMasksAndForeignSamePathSourcesCannotBorrowReceipt` test
removed filters and changed aliases/sources but did not directly remove the mask
Project. The final test-only supplement removes the actual Concat-bearing mask
Project, reconnects consumers to original unmasked attributes, asserts that the
candidate remains resolved, and requires `logical binding changed`. This avoids
passing merely because of dangling attributes or an unsupported plan shape.

All four supplements exited 0, each with one test and no failures/errors/skips:
`analysis-receipt-supplement-354-332`, `analysis-receipt-supplement-354-330`,
`analysis-receipt-supplement-356-330`, and `analysis-receipt-supplement-356-332`.
These are four additional executions of the changed existing test, reported
separately from the original 287 full-matrix executions. Each supplement ran
normal SpotlessCheck and captured fresh original XML and Test.doFirst inputs.

The supplement has a distinct revised 47-source freeze at
`analysis-receipt-supplement-frozen/source-hashes.json`; `delta-from-original.json`
proves only that one test source changed. All production and the other 46 files
remain byte-identical. Original full rows are not relabeled as containing final
test bytes. Root authorized this bounded single-test supplement instead of
repeating the broad matrix for an added negative assertion.

Every row uses the standard `task-4-evidence/run-gradle.py`, unique output property,
`task-3-evidence/fix-classpath-capture.gradle` Test.doFirst capture, `cleanTest`,
`-PskipITs --offline --console=plain --max-workers=2 --no-parallel`, and the single
argument `-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2`. JDK17, Scala2.12
and Ranger2.8.0 are retained. Exact argv/exit, full log, fresh original XML,
source snapshot, artifact hashes and Test.doFirst compiled-input archives are in
each immutable run directory. The first row includes SpotlessCheck/Javadoc and
policy-engine/stamp/registration-reader/metadata-client regressions. Remaining
rows run all 53 affected native/receipt tests plus SpotlessCheck. No services,
Docker, deployment, policy writes or git staging/commits occurred.

`analysis-receipt-frozen/matrix-audit.json` verifies all original rows against
all 47 frozen source hashes, their source archives and 44 Test.doFirst compiled
archives (including every archived entry hash), and checks actual Spark SQL,
Delta Spark and Ranger common dependency jar versions/hashes. Its original
current-worktree check was run before the final test-only edit. Root independently
verified the same matrix in `analysis-receipt-root-matrix-verification.json`.
`analysis-receipt-supplement-frozen/matrix-audit.json` separately verifies the
revised 47-source test freeze, all four supplement rows and another 44 compiled
archives with the same source, compiled-entry and dependency checks. Audit scripts are retained beside their outputs.
No source changes were made during either frozen matrix.

## Remaining limits

- Current native-head validation, owner-bearing native operations, physical guard
  installation and retained collection/cache enforcement belong to subsequent work.
- Asymmetric self-join native file-selection fallback above is not supported by
  the installed action path yet; native admission was not widened to admit it.
- Delta row tracking explicitly rejects before normalization can replace source
  identities. User cache substitution rejects; normal caching may also reject
  when Spark clones the receipt's session. Cache support is not claimed.
- Clock-dependent datetime keyword casts (including computed constants and zone
  suffixes) reject. Stable dated/numeric/string casts remain qualified.
- More than two join inputs with star-schema detection enabled and CBO disabled
  reject before statistics-producing replay; configurations remain unchanged.
- Other unsupported query shapes and expression classes remain outside the finite
  grammar. Replay runs ordinary Spark optimizer metrics/logging; it is not claimed
  to be side-effect-free, and drift checks cannot detect change-and-restore races.

No memory-search/save connector was callable in this subagent tool inventory;
findings and all failure evidence are retained in the local report/evidence tree.
Implementation and scoped qualification are complete. Source and Gradle slots are
released with no compiler/test process running. Independent review and commit
remain root-owned.
