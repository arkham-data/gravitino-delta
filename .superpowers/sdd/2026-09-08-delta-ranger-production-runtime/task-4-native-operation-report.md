# Native operation implementation report

Status: DONE for the authorized native-operation increment. All four final affected-suite rows pass; this does not complete Task 4 or the full Delta objective.

## Implemented scope

The optional Ranger module now brackets ordinary named loads with their actual metadata reader and published observation, then seals a completed binding. Scan preparation consumes the complete existing decision receipt. Finite thread-local invocations carry that explicit owner; successful close retains immutable plan-owned evidence, while failed or incomplete operations invalidate it and clear the thread. Java remains compiled before Scala.

Actual stateDF and withStats getters expose analysis-only candidates during eager analysis. Only their returned RDD identities and full outputs acquire sealed evidence. Existing successful cached lineage must be explicitly captured under the current owner's actual snapshot. A different active owner cannot borrow an older sealed witness. Resolved native admission reuses the existing optimizer-prefix qualification before eager native optimization; unknown extension rules do not execute first.

The native classifier distinguishes user, analysis-only and sealed plans. Logical evidence binds exact backing objects, expressions, outputs and function instances. Native object reconstruction checks pinned origin and loader. The Dataset-owned Tuple1-to-AddFile helper is constrained to its exact typed encoder and `Project(add, Filter(IsNotNull(add), owned state RDD))` recipe. Captured object functions are retained by operator role, output identity and occurrence count across qualified removals; an active owner cannot replace a captured function or add a duplicate operator merely by copying tags. Malformed expressions that throw during Spark's resolved probe fail closed. Native positional unions admit only owned Delta log inputs. Canonical path projection pushdown is represented by exact filesystem relation/index identity, add/remove path role and exact function reference, with multiplicity per input/role. An expression above a native union binds each input; branch projections consume the same obligations. Prior child tags may reconcile only beneath a captured parent projection whose exact input inventory and function obligations survive; noncanonical/object functions remain strict.

An ordinary non-case PreparedDeltaFileIndex subclass retains the exact installed index, original index, DeltaScan, selected snapshot and complete duplicate occurrence mapping. Its delayed matchingFiles callback starts a fresh finite invocation on the calling thread and passes actual filters to the native implementation unchanged. The installed asymmetric self-join now returns its expected rows through the real differing-filter broadcast-worker fallback.

The data-skipping-disabled route calls the native protected preparation hook first, then installs an owned TahoeLogFileIndex subclass. Explicit finite reconstruction is necessary because inherited Tahoe equality makes generic tree transforms discard an equal replacement. Matching-files, getSnapshot, inputFiles and sizeInBytes callbacks preserve native snapshot selection and use their actual selected snapshot. No native settings are disabled.

Empty preparation retains the complete original receipt and records the finite projection/filter/alias shape over an empty LocalRelation. The explicit later collapse to an empty LocalRelation with exactly the recorded final output is accepted. Changed surviving empty expressions remain rejected.

## Verification

The initial review accepted the production direction but required R1/R2 no-skipping coverage. The original freeze below is retained unchanged. Two original rows completed; the Spark 3.5.6 rows were held and never run on that test source. Fix round 1 changed tests only. Its new affected-suite matrix and explicit production-hash equality are recorded separately below; old evidence is not relabeled.

Frozen source manifest: `task-4-evidence/native-operation-source-freeze.json` (all 79 captured source/configuration files, including current module README; exact count/hash is in the manifest). Final formatting and Javadoc passed in `native-operation-final-format`. No formatter mutation is permitted during the matrix. Every row explicitly pins Spark and Delta versions, uses JDK 17, cleanTest original XML and Test.doFirst compiled-input capture; historical failed attempts remain separate from final qualification.

| Evidence label | Spark | Delta | Selection | Result |
| --- | --- | --- | --- | --- |
| native-operation-matrix-s354-d330 | 3.5.4 | 3.3.0 | Full optional module, format check and Javadoc | 271/0/0/0; exit 0 |
| native-operation-matrix-s354-d332 | 3.5.4 | 3.3.2 | Seven affected suites | 54/0/0/0; exit 0 |
| native-operation-matrix-s356-d330 | 3.5.6 | 3.3.0 | Seven affected suites | Not run; review hold |
| native-operation-matrix-s356-d332 | 3.5.6 | 3.3.2 | Seven affected suites | Not run; review hold |

JDK selection is explicit in the shared harness (`JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`). `native-operation-jdk-runtime-probe.json` independently records that executable's version 17.0.16+0, hash and live Java executable paths during the original second row. No per-Test.doFirst JVM-version field or retrospective per-worker version capture is claimed.

Affected suites: TestDeltaRangerNativeOperations, TestDeltaRangerNativeSnapshotAdmission, TestDeltaRangerNativeCacheCapture, TestDeltaRangerDecisionReceipt, TestDeltaRangerMaskRules, TestDeltaRangerReadRule and TestDeltaRangerRegistrationBindingsNative (54 tests).

## Fix round 1 review response

The scoped `task-4-native-operation-fix-1-review.md` marks R1, R2 and the minor warning addressed, with no new Critical/Important breakage; all four final rows and their independent root verification now pass.

R1 adds installed skipping-disabled two-catalog join, real name-mapped quoted-column selection and middle-position partition-column/data filtering. Tests assert exact rows, complete original owner obligations, exact original/issued index/descriptor/filter-sequence identities and actual owned callbacks while the setting stays false.

R2 adds the distinct no-skipping boundaries: compatible native head advancement and actual returned snapshot lineage; native schema and mapping compatibility errors; ordinary logical/physical clones; equal bare index/relation substitution and different-session Prepared.verify rejection; callback exception, failed preparation and closed-owner denial; independent overlapping callback invocations and thread cleanup. Native commits are generated by Delta during plain fixture setup, their bytes and data files preserved, and only temp-fixture commit JSON files withheld until after protected analysis. Publishing the exact valid history then exercises Delta's real update/compatibility path. These controls do not install retained action-time validation.

The minor warning is addressed with the supported public `checkpoint(snapshot)` overload using the same unsafeVolatileSnapshot that the deprecated no-argument implementation selects. Both pinned JAR signatures and no-argument bytecode delegation are archived in `native-operation-fix1-checkpoint-public-api.json` and `native-operation-fix1-checkpoint-delegation.json`. The [pinned Checkpoints source](https://raw.githubusercontent.com/delta-io/delta/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/Checkpoints.scala) identifies this replacement. No production workaround or warning suppression was introduced.

`native-operation-fix1-noskip-controls` had six invocations and two test-assumption failures: expected b.secret overlooked the existing MASK_NULL fixture policy, and warmed getSnapshot did not trigger an analyzer fault hook. The correct policy expectation and matchingFiles fault-injection route passed in `native-operation-fix1-noskip-controls-2` (6/0/0/0). No production defect was exposed and no production source was changed. The mapping-only refinement passes in `native-operation-fix1-mapping-controls` (6/0/0/0), with exact native compatibility error, unchanged logical schema and different physical mapping metadata; Javadoc also passes. Final matrix passes on all four supported pairs.

Fix round 1 freezes 80 files in `task-4-evidence/native-operation-fix1-source-freeze.json`, SHA-256 `1d9c6b426660a00e015ff43652c963297eb7bfbd8ce307a75bbef5163f659bc9`. `native-operation-fix1-production-equality.json` proves all 36 previously captured non-test files unchanged (including production Java/Scala, builds and README); only the existing checkpoint test and new no-skipping suite differ. Thus the original full-row 271 invocations remain a production-source baseline; final test bytes are qualified by all eight affected suites (59 invocations per pair). This is not a claim that the whole module was rerun on the new test source.

| Final evidence label | Spark | Delta | Result |
| --- | --- | --- | --- |
| native-operation-fix1-matrix-s354-d330 | 3.5.4 | 3.3.0 | 59/0/0/0; exit 0 |
| native-operation-fix1-matrix-s354-d332 | 3.5.4 | 3.3.2 | 59/0/0/0; exit 0 |
| native-operation-fix1-matrix-s356-d330 | 3.5.6 | 3.3.0 | 59/0/0/0; exit 0 |
| native-operation-fix1-matrix-s356-d332 | 3.5.6 | 3.3.2 | 59/0/0/0; exit 0 |

The final rows contain the same 59-invocation testcase multiset, 236 invocations total with zero failures/errors/skips. All four source archives and current working source match the 80-file freeze. `native-operation-fix1-matrix-summary.json` records these checks; root's separate row proofs pass for all four rows, verifying exact Spark/Delta/Ranger 2.8 pins, 80 frozen files and 11 Test.doFirst compiled-input archives per row. Root's aggregate check also confirms the identical testcase multisets and the two-test-file-only difference from the original full-regression baseline. No additional source changes or tests are planned after this checkpoint.

## Native coverage inventory

- Named JSON log reconstruction: installed ordinary projections, masked/filter queries and asymmetric joins use actual owned DeltaLogFileIndex inputs and native Snapshot reconstruction. `actualDifferingFiltersReenterOnBroadcastWorkerAfterPreparation` observes the native fallback on a broadcast worker, exact installed index and actual filter sets.
- Checkpoint and JSON-tail input: `checkpointAndJsonTailNamedLoadCapturesActualNativeInputs` creates a real checkpoint and subsequent append before extension/registration setup, observes actual native Parquet and JSON indices under LOAD, and exercises native positional union reconstruction. The focused controls and final four-pair matrix pass.
- State and parsed statistics caches: `pendingGetterCandidatesCannotExecuteAndOnlyReturnedLineageIsSealed`, both `TestDeltaRangerNativeCacheCapture` tests, snapshot/index/session controls and failed-operation rebinding tests exercise actual public getters and exact returned lineage.
- Native aggregate and encoder: `TestDeltaRangerNativeSnapshotAdmission` retains the full altered aggregate/encoder matrix; no-skipping tests observe actual cold size aggregation through the owned size callback. Typed AddFile select mutations cover function, argument schema, encoder, projected field and backing RDD.
- Delayed and independent callbacks: no-skipping matchingFiles/getSnapshot/inputFiles/size paths, concurrent prepared fallbacks, post-owner-close denial and finally cleanup are covered. Duplicate obligations, inherited index copies, inert reference serialization and exact ordinary physical scan clones are covered separately.
- Existing positive surface: `TestDeltaRangerMaskRules`, `TestDeltaRangerReadRule` and `TestDeltaRangerRegistrationBindingsNative` retain plain/masked/filtered, two-catalog, quoted mapped-column, middle-position partition metadata, cold/warm load and native persistence controls.

These are current logical admission and native callback controls. They do not establish final physical checkpoint/log classification, AQE stage provenance, exchange reuse authorization, columnar guard coverage or action-time revalidation; those remain later Task 4 work.

## Evidence history and diagnosis

Complete completed-run inventory: `task-4-evidence/native-operation-run-inventory.json` records every native-operation evidence label, exit status, original module XML totals and file hashes, including setup/diagnostic attempts. The root's independent pre-freeze inventory and final row verifier remain separate checks.

- `native-operation-installed-red-2`: actual installed asymmetric query rejected LogicalRDD from PreparedDeltaFileIndex.matchingFiles on the broadcast worker. The component-only fallback was not used as acceptance.
- `native-operation-installed-green-2`: first bounded installed row positive.
- `native-operation-capture-noskip-red-2`: pending candidates could execute; native no-skipping size aggregation lacked an owned callback.
- `native-operation-noskip-green`: generic transformUp discarded the equal owned index replacement; explicit finite copies closed this transition defect in `native-operation-noskip-green-2`.
- `native-operation-regression-2`: native eager optimization invoked an unknown optimizer rule before qualification, and an active owner borrowed another owner's sealed aggregate. Both were production defects; all original aggregate/encoder negatives were retained.
- `native-operation-owner-green-3`: a foreign user object-function closure could acquire native evidence; the concurrent test also rendezvoused inside a shared native lazy-initialization monitor. The concurrency control now warms through the actual fallback before testing overlapping independent callback invocations; cold size behavior remains separately tested.
- `native-operation-owner-green-4` and function-origin diagnostics: an overly restrictive Delta-only function-origin check rejected Spark's genuine Dataset typed-select helper. Pinned source and actual runtime function/plan diagnostics identified the finite Tuple1/AddFile route; no reflection or closure execution was used to inspect it.
- `native-operation-owner-green-5`: 11 focused controls passed, before subsequent completeness refinements.
- `native-operation-regression-3`: 270 tests, two failures. The empty projection was rejected by an unnecessarily bare-local preparation condition. A serialization assertion had accidentally targeted the original native index rather than the owned subclass; the assertion was relocated without changing production serialization.
- `native-operation-function-empty-red`: active-owner copied-tag substitution with a different genuine Snapshot function reproduced, and the exact empty projection was captured.
- `native-operation-function-empty-green`: identified the later empty projection collapse and a malformed encoder whose resolved probe throws; the latter remains a negative and is handled fail closed.
- `native-operation-function-count-red`: reusing the same captured function in a duplicate MapPartitions occurrence reproduced, and final empty output was confirmed as a bare LocalRelation.

- `native-operation-function-count-green`: 12 focused controls passed.
- `native-operation-regression-4`: full optional module regression, 271 tests with no failures/errors/skips, plus Javadoc. This predates the combined checkpoint/tail refinement.
- `native-operation-checkpoint-tail-red`: actual checkpoint-plus-tail named-load rejection exposed the missing finite native union route.
- `native-operation-checkpoint-tail-diagnostic-3`: the standalone pinned prefix retained a valid native shape but duplicated the same canonical UDF instance from two syntactic occurrences to four when pushing projections through the two native inputs. Strict syntactic counts were insufficient; exact per-input/role obligations preserve the required multiplicity without admitting duplicates on one input.
- `native-operation-checkpoint-tail-diagnostic`: diagnostic setup-only failure from inserting an unqualified pre-CBO observer; removed. Subsequent diagnostics use a standalone pinned prefix, with no session optimizer injection.
- `native-operation-checkpoint-tail-binding-green` and `native-operation-checkpoint-tail-boundary-diagnostic[-2]`: the prefix-positive assertion passed, but the duplicate-negative fixture added a column to one Union branch and became unresolved. Spark wrapped that analyzer assertion in SparkException. Temporary stack-only instrumentation identified the exact test line, then was removed and the load sanitizer restored. The negative now duplicates a UDF within an existing expression, retaining branch outputs, and asserts collected outcomes after the real query succeeds. These are fixture/diagnostic failures, not an additional production admission cause.

- `native-operation-checkpoint-tail-binding-green-2`: 11 focused tests pass, including actual combined checkpoint/tail rows and output-preserving duplicate/foreign function controls.
- `native-operation-checkpoint-transition-red`: the additional child test assumed Spark universally preserves child tags. The optimized Union was instead fresh finite native construction under an active owner. This is a diagnostic assumption failure, not a production vulnerability or a universal untagged-plan denial requirement.
- `native-operation-checkpoint-transition-red-2`: despite its predicted RED label, actual exit is GREEN (one test). Explicitly copying the original Union witness proves that conflicting child evidence rejects alone, while the qualified parent transition and unchanged sealed reconstruction clone after successful close pass. Fresh untagged finite native construction under a valid active owner remains the existing issuance route.

Formatting/compilation-only attempts and diagnostic runs are preserved under their unique labels, with exit status and original artifacts. They are not behavioral RED claims or final source-compatibility claims.

## Remaining work and limits

This increment installs no physical provenance classifier, final row/columnar collection guard, AQE/cache transition verifier, retained-binding/action native-head validator, or full Delta read/write surface. Prepared evidence currently proves exact occurrence/backing identity; the later physical gate must additionally verify executable behavior and approved optimizer transitions. Physical clone tests only establish preservation of backing indices, not a physical authorization gate.

Per the explicit scope ruling, ValidateRetainedBinding is deferred until its actual metadata bracket and action-validator consumer exist. The current owners are LoadNamed and PrepareScans and their owned continuations. An unused authority-issuing factory was not added.

Index subclasses add only a private final transient continuation field (confirmed by JVM field inspection and serialization-descriptor tests). Driver evidence references round-trip inert; complete native index graph serialization is not claimed. Native inherited copy creates a distinct ordinary index and does not transfer owned continuation authority. Rendering of added authority objects is constant and omits owners, principal, receipts and engine authority.

No core, API, server, client, base storage schema, reviewed policy-engine/time code, Java/Scala compilation-order, deployment, Docker, live-policy or catalog-demo changes were made. Root retains publication, independent review and service ownership. The full goal remains open.
