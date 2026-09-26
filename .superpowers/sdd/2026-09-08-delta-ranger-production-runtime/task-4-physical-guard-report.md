# Physical provenance and installed collection guard

Status: DONE_WITH_CONCERNS — implementation and all four frozen 2 rows PASS;
independent SPEC/QUALITY approved. The implementation concerns are the explicit
finite support and composed observation limits below, not failing gates. Root owns
final independent aggregation and acceptance. Source remains frozen; implementer
releases Gradle ownership with no active builds. Base: `1dd224183fac4f47928ae1d9e88f838eecf826a9`.

## Bootstrap refinement

The accepted `Prepared.verify` checks exact issued source/index/occurrence/output
bindings; it explicitly does not establish executable expression authority. A
real named filter with the same exact sources/output and copied preparation tag
was accepted after changing its predicate. This is the missing physical contract,
not a regression claim against the accepted component.

Root approved the following bounded refinement after source/runtime inspection:
seal the complete actual prepared logical output; qualify the remaining ordered
optimizer rules and batch strategies before replay; compare the candidate directly
to the replay result; lower only the verified result with qualified strategies and
the pinned pre-columnar preparation prefix. Collection must consume the frozen
physical binding and original action validator without replaying user planning.
Native bootstrap must use sealed operation-owned evidence, without reclassifying
an arbitrary candidate into authority. Unknown rules/strategies require zero-
invocation negatives. Generated identities require explicit correspondence, not
schema/path/marker equality or blanket omission.

Pinned accessible hooks are `QueryExecution.createSparkPlan` and a subclass's
protected `preparations`. QueryExecution is documented by Spark as internal;
this is not a stable public API guarantee. The bootstrap does not read its lazy
optimizedPlan/executedPlan/toRdd or call prepare/execute/materialize. Tests read
actual Dataset query-execution artifacts to exercise the real consumer.

## Preserved attempts

All labels below are under `task-4-evidence/`. Exact commands, original XML,
pre-run source, logs, and Test.doFirst execution classpath/archive evidence are
retained by the existing harness. Iteration formatting may differ from pre-run
source snapshots; these are not frozen-matrix rows. Every behavioral run pins
Spark 3.5.4 / Delta 3.3.0, JDK17 and Ranger2.8, skips integration services and uses
offline/no-parallel/max-workers=2.

| Label | Result |
| --- | --- |
| physical-guard-installed-red | 1/1/0/0; real named masked/filter query returns WholeStageCodegenExec instead of the required outer guard. |
| physical-guard-bootstrap-characterization | 1/1/0/0; copied prepared evidence accepts changed filter with unchanged exact sources and output. Failure preserves the complete actual suffix/planner manifest. |
| physical-guard-semantic-green | Compile-only failure: stock Batch has no copy method. |
| physical-guard-semantic-green-2 | Test compile-only failure: JUnit Executable lambda returned LogicalPlan rather than Unit. |
| physical-guard-semantic-green-3 | 1/0/0/0; unchanged actual optimized recipe passes, changed predicate fails the new semantic consumer. |
| physical-guard-planner-red | 1/1/0/0; unknown injected planner accepted instead of rejected. |
| physical-guard-lowering-green | 2/0/0/0; unknown planner rejects with zero invocations; actual physical projection/filter/row-transition/codegen matches independent lowering, clone passes, changed predicate with copied links rejects. |
| physical-guard-installed-green | 1/1/0/0; installation reaches native named-load bootstrap but fails behind the unchanged registration sanitizer. |
| physical-guard-native-bootstrap-diagnostic | 1/1/0/0; temporary SLF4J capture identifies native StateCache reconstruction's executable logical binding mismatch. |
| physical-guard-native-binding-diagnostic | 1/1/0/0; mismatch is one deep retained identity, not a row/output/schema/lambda-ID difference. Temporary private fixture diagnostics retain both recipes and the binding path. |
| physical-guard-native-identity-diagnostic | 1/1/0/0; only exact scala.Tuple2 identity differs. Root approved recursive exact Tuple2 fields, retaining function/source identity. |
| physical-guard-native-pair-green | 1/1/0/0; logical reconstruction matches; next physical binding mismatch isolated. |
| physical-guard-native-lowering-diagnostic | 1/1/0/0; Seq and StructType matched Function1 before structural cases. Pattern ordering corrected without relaxing real functions. |
| physical-guard-native-containers-green | 1/0/0/0; first actual installed exact-row same-Dataset/root collect and collectAsList pass. |
| physical-guard-installed-controls | 8/3/0/0; semantic/physical/planner/row/native-pair controls pass. Fixture failures: expected one filter request despite entry+defense validation; previous metric values compared with Dataset-reset zero values. Native AQE rejected pending its binding. |
| physical-guard-installed-nonaqe-controls | Compile-only: partially replaced metric assertion referenced removed `before` variable. No behavioral XML. |
| physical-guard-native-aqe-binding | 1/0/0/0; enabled native loading survives independently bound inputPlan/initialPlan and exact stage subtree correspondence; user reaches specific unsupported-AQE denial with setting true. |
| physical-guard-final-physical-check-red | 1/1/0/0; original receipt unchanged and final callback reached; fixture expected only executable-message but shared prepared folded payload reports sealed-physical-message. |
| physical-guard-final-physical-check-red-2 | 1/1/0/0; exact missing boundary reproduced: denial stack contains guard doExecute after collection validation. |
| physical-guard-final-physical-check-green | 9/0/0/0; one pure final verify after action validation rejects physical-only final-callback mutation at collection entry; all current installed/semantic/planner/native pair/AQE controls pass. |
| physical-guard-native-stage-controls | 2/1/0/0; adaptive extension rejects with zero calls; actual successful native stages captured, exchange roots/unchanged/missing controls pass, fixture fails because ordinary stage.clone shares hidden plan and missing-tag edit removed original tag. |
| physical-guard-native-stage-controls-2 | 2/0/0/0; actual hidden native shuffle stage/exchange unchanged, missing/mixed evidence reject; complete coalescing positive/partial negative; adaptive extension calls zero. Explicit hidden-plan clones isolate destructive negatives. |
| physical-guard-native-regression | 22/5/0/0: guard component 4/0, no-skipping 5/0, NativeOperations9/2, NativeSnapshot2/1, NativeCache2/2. Four failures share valid simple-projection qualifier transformation; standalone cache characterization expects explicit AQE=true. |
| physical-guard-root-link-diagnostic | 1/1/0/0; full Project link is retained; physical FileSourceScan output has empty qualifiers while logical output has b/db/t qualifiers. ExprIds/names/nullability agree. This is not evidence of root-link disappearance. |
| physical-guard-root-recovery-green | 6/0/0/0; affected native/cache/no-skipping/unfiltered positives and exact copied same-path prepared-index/output negatives pass. |
| physical-guard-reuse-red | 1/1/0/0; actual three-way self-join has three original obligations and real ReusedExchangeExec; collection denies unqualified reuse. |
| physical-guard-reuse-green | 1/0/0/0; independently constructed stock transitions/codegen/reuse tail bound at bootstrap; same Dataset/root exact rows, three obligations retained and reevaluated. |
| physical-guard-installed-stale-matrix | 6/3/0/0; registration/schema/missing, two-catalog broadcast both owners, and eliminated obligations pass. Three fixture defects: nested Mockito stubbing, dangling callback else, reused Hadoop test group mapping. |
| physical-guard-installed-stale-matrix-2 | Running corrected fixtures plus exact reused-reference source/output negatives. |

The test suite is `TestDeltaRangerPhysicalAdmission`. Current exact test names:

- `installedCollectionRetainsOneVisibleGuardAndRevalidatesEveryEntry` — installed same-Dataset/root exact rows passed in native-containers-green; repeat count fixture corrected after installed-controls.
- `preparedEvidenceMustBindExecutableExpressionsBeforePhysicalCapture` — focused semantic component GREEN before physical installation.
- `customPlannerIsRejectedBeforeInvocation` — focused zero-invocation GREEN before physical installation.
- `exactPhysicalLoweringRejectsChangedPredicateWithCopiedLinks` — focused physical component GREEN before physical installation.

## Current limits and remaining gates

All nine integration gates remain pending. Focused installed collection and enabled native loading are GREEN; there is no complete stale first/repeated Dataset matrix, explicit hidden-stage negative matrix, cache
substitution matrix, exchange-reuse proof, complete prepared-source substitution
matrix, or installed lifecycle/task-closure qualification. No frozen first module
row or other-pair qualification has started. Earlier component passes do not
qualify the later installed source.

Current source is an incomplete fail-closed implementation: exact replay/binding,
pre/post callback installation and receipt action delegation are present, but
ordinary named loading now succeeds with row/vectorized scans. Native AQE has a focused enabled positive; mixed/missing stages and finite coalescing boundaries still need controls. User three-way exchange reuse now has a focused positive through independently bound stock tail. Native reused-stage variants remain unqualified unless exact issued subtree matching succeeds.
Temporary diagnostic logging/class rendering must be removed before freeze. No
settings are silently changed in production; the supported user test sets AQE
false explicitly, while production rejects user AQE before native preparation.

No services, Docker, live policies, deployments, demo changes, git mutations,
subagents, core/API/server/client/storage-schema edits or memory connector actions
were performed. No callable memory-search/save tool was exposed; accepted local
designs/reports and pinned source supplied context. Root owns integration,
independent reviews, git checkpoints and live qualification.

## Latest bounded native correspondence

The real native CaseWhen branch pair is exactly scala.Tuple2(IsNotNull, ScalaUDF).
The unchanged delayed owned recipe and a changed nested condition control pass;
function objects remain identity-bound. Physical binding explicitly handles schema
and Seq containers before Function1 because those stock Scala containers implement
Function1; genuine function values still use object identity.

Native adaptive bootstrap now independently compares inputPlan against qualified
raw lowering and initialPlan against qualified prepared lowering. A driver-local
non-Product ticket holds only sealed expected recipes and their exact subtree
bindings. Stage callbacks consume that same ticket, never issue new native authority
from the callback candidate. Exact shuffle/broadcast stages expand only their proper
exchange roots; exact AQEShuffleReadExec permits only complete ordered contiguous
CoalescedPartitionSpec reducer coverage. Unknown/partial/skew/local mappings deny.
Adaptive extension rules and custom cost evaluator configuration reject before
construction through the existing early qualifier. No execution/materialization or
finalPhysicalPlan/getFinalPhysicalPlan reads were added. Actual stage observation
and zero-invocation adaptive-extension controls are still pending.

Root identified an additional boundary: after action metadata callbacks the retained
physical child needs one final pure witness check. The focused RED is being captured
before adding that check. The final physical check is implemented and focused GREEN. The original RED specifically observed downstream doExecute rejection; GREEN requires its absence and exact final metadata read/original analyzed-binding controls.

Source is not frozen. Temporary SLF4J mismatch diagnostics remain and must be
removed; no final full-module or other-pair row has run.

## Prepare/execute observability refinement

Root approved composed ordering evidence under the public seams: unchanged guard
executeCollect validates before super/child work; its instrumented real-child
component suite proves zero prepare/execute on denial; installed same-Dataset tests
establish exact collection-entry validation stack before guard doExecute and actual
user scan metrics, with warm-cache job observations only where meaningful. These
are not direct stock-child prepare/execute counters. No reflection, bytecode
instrumentation, production observer hook or replacement-child test is used to
manufacture installed evidence. Final matrix must rerun the component suite and
report this composition for review. Native metadata jobs during validation are
legitimate and must not be confused with admitted user child work.

## Nine-gate progress map (not acceptance)

| Gate | Current named evidence and remaining work |
| --- | --- |
| 1 Installed unchanged | `installedCollectionRetainsOneVisibleGuardAndRevalidatesEveryEntry`, `installedRowScanReturnsExactRows`: 9-test focused row GREEN includes both scans, actual guard/codegen, same Dataset/root and repeated exact rows. Requalify frozen. |
| 2 Stale action | `retainedAccessRevokeRejectsBeforeFirstAndRepeatedCollection`, `finalMetadataMutationOfFoldedPhysicalLiteralRejectsAtCollectionEntry` GREEN. Complete policy/role/subject/config/registration/lifetime/drift and two-catalog installed matrix pending; direct accepted action tests are not substitutes. |
| 3 Exact source | `preparedEvidenceMustBindExecutableExpressionsBeforePhysicalCapture`, `exactPhysicalLoweringRejectsChangedPredicateWithCopiedLinks` GREEN. Existing original/issued-index/self-join suite running. Complete physical source substitution, root-link recovery and exchange-reuse qualification pending. |
| 4 Delayed native | Existing native operation/snapshot suites running. Real native reconstruction-pair and actual enabled stage positive passed. Full delayed physical routes/failed owner negatives pending. |
| 5 Reentrant cache | Existing cache/native suites running; accepted pending/returned logical controls remain supporting evidence. Installed physical pending/foreign/output/session matrix pending. |
| 6 Spoof resistance | Existing native function controls running; `realNativeCaseWhenPairsBindBothFieldsAfterOwnerCloses` GREEN. Full actual physical spoof controls pending. |
| 7 AQE | `enabledNativeAqeSurvivesBeforeSpecificUserAqeDenial`, `actualCompletedNativeStagesKeepExchangeRootsAndRejectMissingMixedOrPartialEvidence`, `adaptiveExtensionRejectsBeforeInvocation` GREEN. Additional retained setting-change denial and full regression/frozen qualification pending. |
| 8 Cache | Native persisted cache regression running. Full/nested copied-receipt logical/physical user cache substitution matrix pending. |
| 9 Lifecycle | Exact actual physical clone/predicate substitution passes; accepted guard mechanics suite rerunning. Full installed child substitution/serialization/JSON/task/cleanup/downstream controls and exchange reuse pending. |

Actual successful native `QueryExecutionListener.onSuccess` captures are required,
with a bounded latch and nonempty hidden-stage assertions; tests read the plain
Adaptive.executedPlan current-plan getter only after success. Production discovery
continues to use only inputPlan/initialPlan. Stage negative controls operate on
explicitly cloned hidden .plan, since ordinary QueryStage.clone preserves that
hidden reference. No production authority is inferred from listener observations.

## Correction: observed root-link rejection and full authority recovery

The observed failure was **not disappearance of the root logical link**. The
pre-columnar physical root is a FileSourceScanExec over exact issued prepared
backing with `(id, region)` and empty qualifiers. Its retained logical link is the
complete Project over LogicalRelation with the same ExprIds/names/nullability and
`b,db,t` qualifiers. Filtering candidate links by logical.output == physical.output
therefore rejected a valid stock lowering.

Root approved recovery from one complete Prepared authority: revalidate its sealed
actual completed recipe, including original occurrence/index ownership and config,
before and after qualified suffix replay; independently lower the complete recipe;
compare the entire actual physical tree. All recovered refs must agree on the same
parent. This removes arbitrary candidates.head/output selection; qualifier fields
remain bound. Original eliminated/duplicate obligations remain in the receipt.
The analogous native branch replays only the single already owned complete witness;
query stages missing their existing physical ticket cannot bootstrap new authority.

The shared user fixture explicitly sets AQE=false for the supported user surface.
The standalone cache characterization and installed native AQE controls explicitly
set true, preserving those controls. Production never changes either setting.

## Qualified user tail

The actual three-way self-join RED contains ReusedExchangeExec and three distinct
original obligations. Bootstrap applies exact stock ApplyColumnarRulesAndInsertTransitions
(with no recursive companion callback), CollapseCodegenStages, then
ReuseExchangeAndSubquery to the independently lowered sealed recipe. The expected
pre-tail and expected-tail trees each have frozen complete bindings. Actual
ReusedExchangeExec output and hidden referenced exchange are compared directly to
those bindings. No candidate canonicalization, rule replay at collection, source
schema/path equivalence or lost duplicate receipt obligation is introduced. The
focused unchanged three-way retained Dataset/root positive passed; altered hidden
source/output controls are running. This is a finite pinned tail qualification,
not a general native AQE reuse exemption.


## Installed stale matrix and outstanding compatible-advance regression

`physical-guard-installed-stale-matrix-2`: original XML **7 tests, 0 failures,
0 errors, 0 skipped**, exit 0. Six installed staleness methods exercise 72 retained
first/reused scenarios covering decision, role/policy publication, every registration
receipt field, missing/schema metadata, subjects/groups/config/AQE, owner lifetime,
final metadata lifetime mutation, either owner of an actual two-catalog broadcast,
and obligations eliminated by optimization. The seventh method is the actual
three-way exchange-reuse positive plus hidden source/output substitution negatives.
The preceding matrix attempt preserves fixture failures (UGI mapping reuse,
dangling test else, nested Mockito stubbing); it is not counted as production RED.

`physical-guard-installed-observation-controls`: original XML **4 tests, 1 failure**,
exit 1. Absent-filter/NONE-mask time dependence, final metadata mutation of
engine/subject/config/original structure, and physical-only folded literal mutation
before first/after successful collection pass. Compatible native advance fails:
action validation admits retained snapshot 0, advancing the log uncaches it, then
actual matchingFiles continuation calls NativeOperation.capture and stateDF returns
uncached reconstruction instead of the hard-required LogicalRDD. This is an actual
compatibility regression, not a stale-denial success. Root corroborated pinned
SnapshotManagement and StateCache sources. No final gate is closed by this failure.

`physical-guard-numeric-red-and-uncache-diagnostic` is compile-only (extra test
Ticket.get). Its `-2` retry has original XML **3 tests, 3 failures**: full logical
binding confirms signed-zero equivalence RED; physical substitution encountered a
fixture missing root tag and is being corrected to the existing subtree-ticket
lookup; native diagnostic preserves the full actual returned SerializeFromObject
-> Snapshot MapPartitions -> DeserializeToObject -> canonical Projects ->
Sort/Repartition -> JSON relation. Decimal was split into a separate method so the
signed-zero assertion cannot hide its outcome.

Root approved explicit owned cached-RDD versus reconstruction return variants,
with only cached variants eligible for contains(LogicalRDD). Both actual getter
returns must validate before index/snapshot capture is complete. Implementation
retains preparation/index/snapshot/session checks and consumes the existing full
native witness under the active operation; no arbitrary logical-plan fallback.
First lineage attempt was compile-only: Java is compiled before Scala, so direct
Java reference to the Scala witness is unavailable. The narrow ScanOwner callback
now returns its sealed pure validator; Java holds it outside Spark products. The
callback defaults to denial. The corrected focused run is in progress.

Current remaining gates: close compatible-advance regression and raw numeric
bindings; validate explicit uncached-lineage negatives and native regressions;
complete delayed physical native/cache spoof and installed lifecycle controls;
finish user cache rejection checks; remove diagnostics/update README/format; then
freeze and run all four matrix rows. No frozen row or completion claim exists.


## Raw numeric binding controls and old state-cache continuity

`physical-guard-numeric-red-and-reconstruction-lineage-2` and `-3` were
compile-only (strict Java unused state, Scala inner witness encapsulation).
`-4` has original XML **4 tests/4 failures**: physical-only signed-zero alteration
is now a meaningful RED (no exception despite changed executable literal and
unchanged analyzed receipt); logical signed zero and mutable Decimal are separate
REDs. The native failure has advanced to analysis of uncached withStats.

`physical-guard-numeric-green-and-stats-diagnostic` has original XML **5 tests,
1 failure**: all four numeric controls pass. Both new executable binders encode
Float/Double with raw bits and immutable Decimal value snapshots. Tests cover
Float/Double signed zero and distinct NaN payloads in full logical recipes,
mutable logical Decimal, and physical-only signed-zero/folded-Decimal alterations
while preserving the original analyzed receipt binding. The remaining failure
is native compatibility, with the full diagnostic retained in XML.

Correction to the preliminary reconstruction hypothesis: uncached withStats does
not reconstruct its state input afresh. Its cached original recipe is Project
(from_json stats) -> SerializeFromObject(AddFile) -> Dataset MapElements ->
DeserializeToObject(Tuple1) -> Project(add) -> Filter(IsNotNull(add)) -> the OLD
state LogicalRDD. The newly returned uncached stateDF is SerializeFromObject
reconstruction. These are distinct pieces of evidence. Root approved exact old
state-cache continuity from the successful original prepared-index operation for
the same index/snapshot/scan owner/session. The old stats cache is not transferred.

`captureRetained` keeps original state-cache membership separately from new
returned lineage, then still invokes both actual public getters. Failed/pending
source operations and owner/index/snapshot/session mismatch cannot adopt it. The
existing installed prepared-index continuation checks run before this method.
Only the explicitly retained old state RDD can satisfy its cache membership;
fresh reconstruction never satisfies contains(LogicalRDD). Focused compatible
first/reused and native fallback/ownership regression is running. New adoption
negative matrix and final affected Java/native/action regression remain pending.


## Continuity GREEN and delayed native planning refinement

`physical-guard-retained-state-continuity`: original XML **4/0/0/0**, exit 0.
Installed native-version test completes eight first/reused × compatible/schema/
policy/metadata scenarios. Asymmetric broadcast fallback and existing native copied
index/foreign snapshot/session/failed-operation controls also pass.

`physical-guard-adopted-state-controls` is fixture compile-only (SnapshotDescriptor
versus actual Snapshot). `-2` original XML **3 tests/2 failures**: actual physical
Float/Double signed-zero and NaN-payload negatives pass. Shared nested StructType
mutation is a new RED: old-state membership accepted a candidate sharing the
mutated captured output. Delayed native execution passes capture/owner-close and
analyzed native evidence checks, then fails PreCBO with missing receipt after
stock prefix changes its logical recipe. These are separate boundary failures.

`physical-guard-original-cache-binding-green`: original XML **2/0/0/0**, exit 0.
A pure exact scan-owner cache comparator freezes the actual getter's complete
binding before sharing, checks the frozen original before candidate membership,
and is mandatory for retained-state adoption. Shared schema mutation now rejects;
all eight installed native-version scenarios still pass. Default owner callback
rejects. Captures by Java load/retained-validation owners without ScanOwner retain
their previously accepted scope; these tests do not establish their complete
immutability. captureRetained is explicitly limited to MATCHING_FILES.

Root approved a bounded delayed native PreCBO continuation, since exact analyzed
witnesses alone cannot survive the already qualified optimizer prefix after owner
close. It retains the original owned complete recipe/config, rechecks source
before and after existing qualified Prefix replay, compares candidate directly,
and chains the continued witness to the same successful owner and original proof.
Recovery occurs only at PreCBO from an existing root reference. No analyzer-wide
recovery, missing-receipt authority, candidate optimization, collection replay or
new owner is introduced. Diagnostic run `physical-guard-delayed-native-prefix` is
in progress; generated-ID/function mismatches must be characterized rather than
omitted. Pending/failed owner regressions run alongside its actual delayed native
state/stats execution control.

## Current integration map (supersedes earlier WIP remaining-work lists)

No frozen matrix or acceptance claim yet. Existing failed attempts remain intact.
`physical-guard-native-prefix-and-session-green` has original XML 2/0/0;
`physical-guard-native-continuation-negatives` has 4/0/0. Delayed uncached
state/stats now physically execute after successful owner close, without a user
guard. Qualified NestedColumnAliasing correspondence preserves original IDs and
matches only fresh exact Alias(GetStructField) producers and all their uses.
Actual producer/consumer tests reject duplicate/missing/extra definitions, changed
field/name/qualifier/metadata/UUID, original-ID collision and merged/swapped uses;
changed MapPartitions function with copied tags rejects. Unknown prefix has zero
invocations. Native physical inherited/recovered/post-columnar entrypoints reject
a different supplied active session; their original acceptance is preserved in
`physical-guard-native-session-red-and-prefix-ids`.

The pure nested internal target-schema test in
`physical-guard-nested-target-schema-red` is behavioral RED (1/1): mutating a
JsonToStructs nested target schema hidden behind Boolean IsNotNull left the
binding equal because StructType matched Seq before DataType. Dispatch now
snapshots DataType/Metadata before Seq. Original GREEN XML in
`physical-guard-nested-target-schema-green` is 3/0/0, including prior raw-float and
mutable Decimal recipe controls.

| Gate | Current evidence | Remaining before frozen qualification |
| --- | --- | --- |
| 1 | Installed same Dataset/root first/repeated collect/list, exact masks/rows, row/vector scans, one guard, visible codegen; named PhysicalAdmission tests | Frozen full regression |
| 2 | InstalledStaleness nine methods cover first/reused stale matrices, two-catalog broadcast, eliminated obligations, final metadata mutations, compatible advance and drift; final physical-only mutation rejects at collection entry | Frozen full regression; ordering evidence remains the explicitly reported composed proof |
| 3 | Actual issued prepared backing, simple projection complete-parent recovery, equal-path copied index/output denial, 3-way exchange reuse and altered hidden source/output | Actual ordinary-Parquet physical spoof control |
| 4 | Actual uncached state/stats delayed physical execution and successful native AQE execution | Delayed log JSON/checkpoint/union/reconstruction and cached state/stats/aggregate coverage; current focused run extends log routes |
| 5 | Actual returned lineage, exact old-state continuity, pending/failed/wrong owner/index/snapshot/session/RDD/output and shared mutable original-schema controls | Actual pending getter physical attempt in current focused run |
| 6 | Native logical function/encoder/aggregate controls retained; physical session, hidden stage mixed/missing/partial, source/expression and copied tags denied | Direct native physical wrong cached RDD/output and relevant physical spoof controls |
| 7 | Native AQE enabled successful hidden stages/exchange roots and exact complete coalesced reads; user AQE denial and retained changed configuration; custom adaptive rules zero invocation | Frozen regression and explicit finite unsupported-route documentation |
| 8 | Existing native persisted caches execute during installed queries | Same-session populated full/nested user cache logical/physical substitution in current focused run; explicit delayed cached native positive |
| 9 | Existing instrumented guard component copy/cleanup/serialization proof; installed codegen/row/columnar/reuse positives | Installed clone/changed-child/canonicalized/reference serialization/explicit success-denial-downstream-failure cleanup controls |

Temporary diagnostics, Java member ordering/import cleanup, README supported
surface, exact physical Product rationale, formatting, all affected accepted
suites and the four pinned source/compiled matrix rows remain before freeze.

## Closed focused integration controls before freeze

Preserved `physical-guard-cache-and-delayed-controls` is compile-only: stock
SparkPlan.cleanupResources is package-scoped, while the guard's existing override
is public. The corrected guard-typed call and removal of an unsupported native
cleanup call produced `physical-guard-cache-and-delayed-controls-2`: original XML
**3/0/0/0**. No private bridge was added.

- `populatedUserCacheSubstitutionRejectsLogicallyAndPhysically` constructs Spark's
  actual InMemoryRelation through its public factory in the installed session,
  materializes its actual cacheBuilder buffers, checks positive batch count,
  loaded state and exact three-row accumulator. Full/nested cache plans carrying
  copied receipt/physical tags reject at the logical/physical cache boundary;
  original Dataset still returns exact rows afterward. This is real cache
  materialization, not a pre-restart cache flag or an empty synthetic cache leaf.
- `pendingGetterCandidatesCannotExecuteAndOnlyReturnedLineageIsSealed` now attempts
  actual QueryExecution physical planning from inside the pending real getter
  callback, with test-only recursion protection. It observes the exact pending
  native rejection and proves a nonzero number of attempts. Returned lineage
  succeeds after close; decoys remain rejected.
- `checkpointAndJsonTailNamedLoadCapturesActualNativeInputs` now physically lowers
  and executes nonempty representatives of the actual captured input, union and
  reconstruction classes after successful owner close, without a user guard.

`physical-guard-lifecycle-and-native-cache`: original XML **4/0/0/0**.

- `delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution` captures
  actual state/stats frames inside their real owner, then plans and collects each
  twice after close. Logical RDD storage level is persisted, no user guard appears,
  and no new Ranger user-policy events occur. Real copied physical RDD scans with
  another RDD or changed output reject while an unchanged clone verifies.
- `installedGuardCloneAndLifecycleKeepAuthorityOffSerializedReferences` collects
  exact rows through a real installed guard clone, checks visible child/product and
  sanitized JSON, rejects canonicalized execution and a serialized/deserialized
  actual physical reference, then rejects stale cloned collection at entry.
- `installedGuardCleanupAfterActualDataFileFailure` removes a real selected data
  file in the isolated fixture; admitted collection reaches a downstream Spark
  file failure with no SecurityException, and explicit guard cleanup completes.
- `installedNoSkippingQueriesKeepNativeSettingAndRows` now captures actual native
  size Aggregate recipes during the real owned callback, then physically plans
  and executes them after close, without a user guard or configuration changes.

Cleanup observations are composed evidence, not direct real-stock-child counters:
installed public guard cleanup is called after success/denial/downstream failure;
the unchanged public override delegates to Spark's inherited visible-child
traversal; accepted instrumented-child component tests measure traversal counts.
The same explicit observability limit applies to prepare/execute ordering.
No automatic Dataset cleanup claim is made.

`physical-guard-source-function-aggregate-spoofs`: original XML **3/0/0/0**.

- `ordinaryParquetPhysicalSourceCannotBorrowPreparedLinks` uses an actual ordinary
  Parquet scan at the same root path, preserving prepared output and copied logical
  links. Both recovered bootstrap and retained guard reject its foreign backing;
  original prepared collection remains valid.
- `foreignObjectFunctionsCannotAcquireNativeReconstructionEvidence` now physically
  plans/executes the unchanged owned reconstruction after close, then rejects an
  actual MapPartitionsExec function substitution and a native encoder StaticInvoke
  substitution with copied provenance.
- The delayed no-skipping aggregate control now also changes actual physical alias
  result expressions and rejects those at the physical provenance boundary.

`physical-guard-final-qualification-controls`: original XML **4/1/0/0**. Three
unchanged native-cache/AQE/reuse controls pass after production diagnostic removal.
The unknown-columnar test assertion expected the inner qualification message, but
installed native loading deliberately emits the existing sanitized
NativeOperation.OptimizerRejected message. This is a fixture expectation failure;
the corrected test will separately assert the direct exact qualification reason,
the installed exception class, and zero rule invocations.

Temporary test-only inventory inspection in that retained run found the generic
physical Product branch handling TableIdentifier (name/database/catalog values),
BuildRight and Inner singleton values, HashedRelationBroadcastMode (bound expression
keys and null-aware Boolean), UnknownPartitioning (partition count), and
REPARTITION_BY_NUM singleton. Native CaseWhen additionally has exact Tuple2
expression pairs. These are stock structural payloads compared by exact class and
all product fields to an independently lowered sealed recipe. This branch cannot
admit an arbitrary class by Product membership: candidate class/fields must match
that expected tree. Expressions recurse, schemas/metadata snapshot to JSON,
float values retain raw bits, mutable Decimal/UTF8/arrays snapshot, and relation,
index, RDD and actual function/source objects retain identity binding. Opaque
non-Product payloads use identity. Actual function/encoder/source substitutions
above reject. No claim is made about arbitrary injected Spark nodes/functions:
custom planner/rules and unknown logical/native routes reject before derivation.

## Exact final gate inventory

This is the intended frozen regression inventory, not a substitute for its final
XML. Earlier progress maps remain historical; the entries below supersede their
outstanding focused-test lists. `TestDeltaRangerPhysicalAdmission` is abbreviated
`Physical`; `TestDeltaRangerNativeOperations` is `NativeOperations`.

| Design gate | Exact named installed/native controls |
| --- | --- |
| 1 | Physical.installedCollectionRetainsOneVisibleGuardAndRevalidatesEveryEntry; installedRowScanReturnsExactRows |
| 2 | TestDeltaRangerInstalledStaleness.firstAndRepeatedCollectionsRejectDecisionRoleAndPolicyChanges; firstAndRepeatedCollectionsRejectEveryRegistrationReplacementAndMissingMetadata; firstAndRepeatedCollectionsRejectUserGroupsConfigurationAndEnabledAqe; catalogMetadataExpiryAndEngineClosureRejectBeforeOrDuringRetainedCollection; twoCatalogBroadcastRevalidatesEitherOriginalOwnerBeforeFirstAndRepeatedCollection; optimizedAwayScanRetainsOriginalAccessAndRegistrationBeforeFirstAndRepeatedCollection; absentFilterAndNoneMaskRemainTimeDependentRetainedObligations; finalMetadataCallbacksCannotChangeEngineSubjectConfigurationOrOriginalRecipe; retainedSnapshotAllowsCompatibleAdvanceButRejectsSchemaAndObservedNativeDrift. Physical.retainedAccessRevokeRejectsBeforeFirstAndRepeatedCollection; finalMetadataMutationOfFoldedPhysicalLiteralRejectsAtCollectionEntry. Guard component counters support the explicitly composed entry-order evidence. |
| 3 | Physical.preparedEvidenceMustBindExecutableExpressionsBeforePhysicalCapture; exactPhysicalLoweringRejectsChangedPredicateWithCopiedLinks; simpleProjectionRecoversCompleteRecipeAndRejectsCopiedSamePathIndex; ordinaryParquetPhysicalSourceCannotBorrowPreparedLinks; realThreeWaySelfJoinRetainsDuplicateObligationsAcrossExchangeReuse. Existing NativeOperations prepared-index occurrence/clone and no-skipping controls retained. |
| 4 | NativeOperations.checkpointAndJsonTailNamedLoadCapturesActualNativeInputs; uncachedRetainedStateKeepsExactPreparationAndDelayedNativeExecution; foreignObjectFunctionsCannotAcquireNativeReconstructionEvidence; installedNoSkippingQueriesKeepNativeSettingAndRows. Physical.delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution. Existing failed-operation and snapshot/native admission suites retained. |
| 5 | NativeOperations.pendingGetterCandidatesCannotExecuteAndOnlyReturnedLineageIsSealed; uncachedRetainedStateKeepsExactPreparationAndDelayedNativeExecution; retainedStateBindingMustSnapshotSharedMutableSchema; snapshotCaptureRejectsCopiedIndicesForeignSnapshotsAndSessions. Physical.nativePhysicalTicketsAndLogicalRecoveryRejectAnotherActiveSession; delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution. |
| 6 | NativeOperations.foreignObjectFunctionsCannotAcquireNativeReconstructionEvidence; installedNoSkippingQueriesKeepNativeSettingAndRows, now with direct changed physical encoder/function/aggregate results. Physical.delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution; ordinaryParquetPhysicalSourceCannotBorrowPreparedLinks; actualCompletedNativeStagesKeepExchangeRootsAndRejectMissingMixedOrPartialEvidence. Existing TestDeltaRangerNativeSnapshotAdmission aggregate/encoder grammar and foreign-log/index controls remain in full regression. |
| 7 | Physical.actualCompletedNativeStagesKeepExchangeRootsAndRejectMissingMixedOrPartialEvidence; enabledNativeAqeSurvivesBeforeSpecificUserAqeDenial; adaptiveExtensionRejectsBeforeInvocation. InstalledStaleness.firstAndRepeatedCollectionsRejectUserGroupsConfigurationAndEnabledAqe. |
| 8 | Physical.populatedUserCacheSubstitutionRejectsLogicallyAndPhysically; delayedNativePersistedCachesRejectPhysicalRddAndOutputSubstitution. Existing TestDeltaRangerNativeCacheCapture and logical DecisionReceipt cache controls retained. |
| 9 | Physical.installedGuardCloneAndLifecycleKeepAuthorityOffSerializedReferences; installedGuardCleanupAfterActualDataFileFailure; realThreeWaySelfJoinRetainsDuplicateObligationsAcrossExchangeReuse; installedRowScanReturnsExactRows. TestDeltaRangerCollectGuardExec supplies instrumented lifecycle, task/serialization and zero-child-work counts; actual installed cleanup observations have the limits stated above. |

Bootstrap/value controls additionally include Physical.customPlannerIsRejectedBeforeInvocation;
unknownColumnarRulesRejectBeforeInvocation; closedNativePreparationRejectsUnknownPrefixBeforeInvocation;
fullLogicalRecipeDistinguishesFloatingPointBits; fullLogicalRecipeSnapshotsMutableDecimals;
logicalRecipeSnapshotsNestedInternalTargetSchema;
actualPhysicalOnlyFloatingPointSubstitutionRejectsWithOriginalReceiptUnchanged;
actualFoldedPhysicalDecimalMutationLeavesReceiptUnchangedAndRejects;
realNativeCaseWhenPairsBindBothFieldsAfterOwnerCloses. The actual delayed native
continuation test exercises finite generated-alias producer/use correspondence
and its missing/extra/duplicate/swapped/UUID/field/function negative matrix.

Full-module qualification also retains the accepted action-validator, registration,
metadata lifetime, subject, engine publication, Java native ownership/cache,
no-skipping mapped/partitioned/two-catalog, asymmetric broadcast, empty result,
mask/filter and native-cache regressions. Service/download/live fixtures stay
under their existing disabled assumptions; no service or Docker scope is enabled.

## Frozen qualification

`physical-guard-final-format-and-columnar` preserves the unchanged wrong-message
fixture again: a scripted edit precondition failed before writing and the following
build had already started. It is not new behavioral evidence. Subsequent edit and
build operations were separate and exit-gated.
`physical-guard-final-format-and-columnar-2` is original XML **1/0/0/0**, exit 0:
exact direct unknown-columnar reason, installed sanitized OptimizerRejected class,
and zero custom rule invocations. Temporary inventory logging and all production
binding/tree diagnostics are removed. Java Lineage constructor order is corrected.
README now describes the installed finite collection boundary and explicit limits.

Module source/README/build configuration is frozen as
`task-4-evidence/physical-guard-frozen-source-1/source-hashes.json` (59 files), SHA256
`c8e67ff6d6bbea778513fd72ec38254bb962c10c29377846e21d2122435c6c4e`.
`changed-module-files.json` in the same directory lists the exact 15 changed module
paths relative to base `1dd224183fac4f47928ae1d9e88f838eecf826a9`.
No source formatter or README/source mutation is permitted during this matrix.

Root approved full-module consistency on all four pairs because the affected
native/action/physical/registration/engine suites now cover nearly the entire
non-service module. Each row uses the existing evidence wrapper and Test.doFirst
compiled-input capture, JDK17/Ranger2.8, explicit Spark/Delta properties, offline,
`-PskipITs`, two Gradle workers and no parallel Gradle projects. Task order is
`spotlessCheck`, `cleanTest`, full `test`, then `javadoc`. No service/Docker/live
switch is enabled. Exact commands are preserved in each row's `command.json`.

| Evidence row | Spark / Delta | Original XML / exit | Status |
| --- | --- | --- | --- |
| physical-guard-final-3.5.4-3.3.0 | 3.5.4 / 3.3.0 | 334/1/0/0; exit 1 | FAILED; preserved |
| physical-guard-final-3.5.4-3.3.2 | 3.5.4 / 3.3.2 | Not run | Superseded by freeze 2 |
| physical-guard-final-3.5.6-3.3.0 | 3.5.6 / 3.3.0 | Not run | Superseded by freeze 2 |
| physical-guard-final-3.5.6-3.3.2 | 3.5.6 / 3.3.2 | Not run | Superseded by freeze 2 |

Root additionally verified the composite qualification capsule
`task-4-evidence/physical-guard-source-freeze-1.json`: **87 files**, SHA256
`9934bddcbe6da746185f903eec8e64e07a471a6bd6ebb93cb1e92ad37df11759`.
It combines the 59 module files above with 28 unchanged accepted authorization,
common wiring, runtime and settings files. Root confirmed all 87 match current
files, first-row pre-run hashes and original archived source. The two manifests
have different explicit scopes; neither replaces or modifies the other.

### Freeze 1 qualification failure (preserved)

`physical-guard-final-3.5.4-3.3.0` completed with original XML **334 tests,
1 failure, 0 errors, 0 skipped**, exit 1; Gradle elapsed **16m16s**. Formatting
checks passed. Javadoc was not reached after test failure. The only failure is
`TestDeltaRangerMaskRules.installedExtensionExecutesFreshNamedScansAndInternalNativeReconstruction`,
line 208 in the archived source: an existing captured-native-UDF positive returns
false during a later explicitly rebound PREPARE operation. Earlier installed
query assertions in that test passed. This is a failed regression qualification,
not an accepted first row, and the other three matrix rows have not started.

Root authorized test-only diagnostics after preserving the complete original row.
The observer currently stores live plan references admitted across several queries,
deliberate denied queries and another Spark session, then asks the later owner to
classify each. Diagnostic assertions preserve the original positive requirement
and record only capture index, class shapes, function-inventory checks, exact
current-owner RDD membership, binding unchanged status and owner/session lifecycle
flags—no credentials or query data. Production authority remains unchanged.
Freeze 1 capsules/packages remain immutable historical evidence; any corrected
acceptance matrix will use a new freeze and unique row labels.

### Confirmed cache comparator regression and bounded correction

The initial historical-cache-absence hypothesis is **withdrawn**. Test-only
`physical-guard-mask-capture-diagnostic` is compile-only (private Reference.get was
not accessible; no private bridge was added). The next diagnostic uses the already
exposed classifier result/witness without an extra classify invocation.
`physical-guard-mask-capture-diagnostic-2`,
`physical-guard-mask-current-cache-diagnostic`, and
`physical-guard-mask-cache-fields` each retain original XML **1/1/0/0**.

The failing capture is an unchanged Filter over a LogicalRDD, from a successful
PREPARE owner in the same session. All eligible successful-original-owner
requireValid/requireExecutable positives passed. A freshly returned current stats
getter has the **same actual RDD object**, equal output attributes/ExprIds/schema,
but the new full freezeCache comparator denies membership. Exact product-field
inspection identifies field 2: two equal stock UnknownPartitioning objects have
different object identities. Other differences are an equal output Seq container
already structurally bound; RDD identity, Nil ordering and streaming flag agree.
NativeProvenance.value previously used its opaque identity fallback for this
immutable non-expression partitioning value. This is production overbinding,
not a reason to drop captured plans or weaken the positive fixture expectation.

Both official pinned sources define UnknownPartitioning with only immutable
numPartitions: [Spark 3.5.4 partitioning](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala),
[Spark 3.5.6 partitioning](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala).
Original source bytes and URL/SHA metadata are preserved under
`physical-guard-partitioning-sources` (definition at archived line 240).
Root approved exactly this class plus numPartitions snapshot, with changed
count/type negatives. Generic Product matching is not added to logical binding,
and source/RDD/output/session/schema/function evidence remains unchanged.
A focused fresh-wrapper/cache RED runs before applying this bounded correction.

`physical-guard-cache-partitioning-red` is original XML **1/1/0/0**: the new
`NativeOperations.nativeCacheRebindingBindsExactUnknownPartitioningValues` first
rejects changed count/type, then fails its fresh exact-value positive despite the
same RDD and output. The approved correction snapshots only the exact
UnknownPartitioning class and numPartitions; subclasses/other partitioning types
retain the existing opaque handling. Generic logical Product binding remains
absent. Test diagnostics are removed, all original MaskRules current-owner and
function/argument assertions are preserved, and concise successful-original-owner
delayed checks remain with nonzero coverage.

The source-backed trigger is the existing session-switch sequence: pinned
StateCache.CachedDS uses DatasetRefCache to recreate a LogicalRDD wrapper when the
active session changes. MaskRules executes a new session and returns to the
original session before fresh getter capture. The persisted RDD and output remain
exact while the wrapper's immutable UnknownPartitioning allocation changes. This
qualifies one exact stock value representation, not general partitioning
semantics, schema equivalence, source replacement, or historical cache adoption.

## Current review handoff: freeze 2

`physical-guard-cache-partitioning-green` has original XML **3/0/0/0**, exit 0:
new exact state/stats cache wrapper positives, changed partition count/type and
foreign-RDD negatives; original MaskRules installed/current-owner/function/argument
positive-negative contract; successful-original-owner delayed checks; and the
prior shared mutable-schema rejection all pass. The last run applied final module
formatting. Production changes since freeze 1 are only the exact stock
UnknownPartitioning value snapshot; tests add the new focused cache control and
concise original-owner observations. No diagnostic scaffolding remains.

Freeze 2 module capsule:
`task-4-evidence/physical-guard-frozen-source-2/source-hashes.json`, **59 files**,
SHA256 `1a347188cc3417fc677854d7010cc822ab986fe11afb45344fa4149ad8907274`.
Its `changed-module-files.json` gives the exact **16 changed module paths** from
base `1dd224183fac4f47928ae1d9e88f838eecf826a9`. Freeze 1 remains preserved.
The new cache test supplements gates 4/5/8; all other exact gate mappings above
remain the intended complete regression inventory.

Source/README ownership is released for independent read-only SPEC/QUALITY review;
the implementer retains exclusive sequential Gradle matrix ownership. Root permits
review to overlap these corrected rows based on freeze 1's 333 positive cases and
the diagnosed/corrected single regression. This changes timing only: **no acceptance
until every corrected frozen row and independent review passes**.

| Corrected frozen evidence row | Spark / Delta | Original XML / exit | Status |
| --- | --- | --- | --- |
| physical-guard-freeze2-final-3.5.4-3.3.0 | 3.5.4 / 3.3.0 | 335/0/0/0; exit 0 | PASS |
| physical-guard-freeze2-final-3.5.4-3.3.2 | 3.5.4 / 3.3.2 | 335/0/0/0; exit 0 | PASS |
| physical-guard-freeze2-final-3.5.6-3.3.0 | 3.5.6 / 3.3.0 | 335/0/0/0; exit 0 | PASS |
| physical-guard-freeze2-final-3.5.6-3.3.2 | 3.5.6 / 3.3.2 | 335/0/0/0; exit 0 | PASS |

Root's freeze 2 composite capsule is
`task-4-evidence/physical-guard-source-freeze-2.json`, **87 files**, SHA256
`8fd4acdeae6528b77581b65a7e8cef61889963cf38197c9243ae792cfe1db6c4`.
It combines this 59-file module capsule with the same 28 unchanged accepted
common/authorization/runtime/settings inputs. Root verified all 87 against current,
first corrected row pre-run hashes and archived source. The 16-file review diff
SHA256 is `873729782fd6cb9c6aeaeaa97fb7742f8363eb2f832eadc17c16be4d63d79c80`.
Fresh independent SPEC/QUALITY review used that frozen package; its completed
conditional result is recorded below. The implementer has now completed all four
passing rows; root retains final independent aggregation and acceptance.

Independent review completed in `task-4-physical-guard-review.md`: **SPEC compliant /
QUALITY Approved, conditional on all four corrected frozen rows**. No Critical or
Important findings. Minor output noise (unattributed Scala feature warning,
existing fruitless Product test, Gradle/Spark fixture warnings) is deferred by the
review/SDD decision; source freeze is not interrupted for warning cleanup.
The matrix condition is now satisfied by the original results below. Root retains
final independent composite source/compiled/isolation verification and acceptance.


## Final implementer handoff

All four corrected full-module rows completed sequentially against frozen 2.
There were no source/README/formatter changes after freezing, no reruns of a
passing row, and no services, Docker, live policy, demo, deployment or Git actions.
The final execution session is terminal and Gradle ownership is released.

| Spark / Delta | Original tests/failures/errors/skips | Exit | Gradle duration | Formatting / Javadoc |
| --- | --- | --- | --- | --- |
| 3.5.4 / 3.3.0 | 335/0/0/0 | 0 | 15m46s | PASS / PASS |
| 3.5.4 / 3.3.2 | 335/0/0/0 | 0 | 16m38s | PASS / PASS |
| 3.5.6 / 3.3.0 | 335/0/0/0 | 0 | 16m6s | PASS / PASS |
| 3.5.6 / 3.3.2 | 335/0/0/0 | 0 | 15m21s | PASS / PASS |

Each `physical-guard-freeze2-final-<Spark>-<Delta>` directory preserves its exact
`command.json`, original 27-suite XML, `exit.json`, `gradle.log`, source copies and
pre-run hashes, Test.doFirst `execution-classpath.json` with compiled archives,
and artifact hashes. Each command runs module `spotlessCheck`, `cleanTest`, full
`test`, then `javadoc`, using the same classpath-capture init script, explicit
Spark/Delta properties, `-PskipITs --offline --console=plain --max-workers=2
--no-parallel`, and `-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2`.
The wrapper pins JDK 17.0.16; actual execution dependency capture pins Ranger
2.8.0 and each stated Spark/Delta pair.

Original XML totals are **1,340 tests, 0 failures, 0 errors, 0 skipped**. All four
sorted `(classname, name)` multisets match exactly, preserved independently as
`test-case-multiset.json` in each row, SHA256
`865ee7e8a6d9e2ee57b8b095cec51252c921665aa45210aca63bc37d9463f89c`.
The implementer verified all 59 module source hashes against current and every
row's pre-run source snapshot. Root's 87-file composite and compiled archive
verification is separate, using the unchanged composite manifest stated above.
The nine-gate inventory and supplementary value/bootstrap controls above are
included in each complete module row; accepted action/native/engine/registration
component suites remain in the same full regression.

Independent SPEC/QUALITY review found no Critical or Important findings and was
conditional on these four corrected rows. Deferred minor build/fixture warnings
remain recorded, with no freeze-breaking cleanup. All original REDs, compile and
fixture attempts, failed freeze 1 qualification, diagnosis and bounded
UnknownPartitioning correction remain preserved.

The result is the scoped installed collection guard, not complete Delta or Spark
execution authorization. User collection requires the qualified finite grammar
and AQE disabled; native AQE support is limited to the exact observed sealed
routes/stages and complete ordered coalesced reducer coverage. Unknown/custom
optimizer/planner/columnar routes, user cache substitutions and unsupported AQE
mappings fail closed. Collection checks the retained actual physical child,
validates the original receipt, then checks that same physical child again; it
does not replan or execute user paths to discover authority.

Preparation/execute ordering and cleanup observations are explicitly composed:
production guard entry/traversal, instrumented-child component counters, and
actual installed collection-entry denial/scan/warm-job evidence. These are not
direct stock-child prepare or cleanup counters. Legitimate native metadata jobs
may run during validation. No raw iterator/RDD/general execute, cancellation,
atomic change-and-revert, automatic Dataset cleanup, full serialization, live
Ranger or arbitrary Spark-version support claim is added.

**Contract: DONE_WITH_CONCERNS.** No remaining implementation or matrix work.
Concerns are finite supported routes, the pinned accessible internal Spark seam,
composed observability limits, and deferred minor warnings. Root retains final
independent aggregation, checkpoint and any separately authorized publication.
