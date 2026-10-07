# Installed Delta append boundary characterization

The installed companion still admits no append command. This test-only dossier measures the
boundaries preceding an owned append implementation; it does not qualify a write. The focused primary
Spark 3.5.4 / Delta 3.3.0 run exercises eight JUnit groups and 31 isolated fixtures. The serial
combined regression passes all four Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 pairs: 82 tests per pair,
328 total, with no failures, errors or skips. Each pair uses JDK 17.0.16, Scala 2.12.21 and Ranger
2.8.0. Independent source review and artifact audits cover this finite characterization.

The unchanged companion runs before the native Delta extension. A test resolution observer returns
its exact input object and adds no optimizer, columnar, planner, command, or native permission.
Equivalent installed SELECT controls with and without it return the same masked/filtered rows,
canonical request inventory, and extension/optimizer/strategy/columnar manifests; the only added
resolution class is the observer. Raw-path and unreceipted SELECT remain rejected. Policy transport
is a request-sensitive double around the real engine and registration wrapper, not live Ranger.
The accepted target-authority tests provide the separate real Ranger 2.8 policy-matching evidence.

## Frontends and exact command inputs

Each frontend starts with a valid independent target UPDATE receipt for the exact loaded table.
The observer retains every AppendData occurrence, native target object, `query`, optional
`analyzedQuery`, resolution state, schemas, output names/types/nullability/ExprIds, by-name flag,
options and receipt-tag presence. Ordinals in the private trace use identity equality; SQL text and
`fastEquals` do not merge instances. The command's subsequently loaded native table must not inherit
an independent receipt merely because catalog names and storage paths match.

All observed `analyzedQuery` values are absent. A physical AppendDataExecV1 and its actual V1 writer
input are therefore **not reached**. No command or source input is synthesized to fill that gap.
The DataFrame frontends retain and revalidate the same source receipt captured from their actual
accepted analyzed Dataset root; SQL VALUES and INSERT SELECT have no separately issued source
receipt for their observed command input.

| Attempt | Actual observed input | Last completed phase | First rejection | Native transaction / durable result | Required ownership |
| --- | --- | --- | --- | --- | --- |
| Valid UPDATE + SQL VALUES control | AppendData / LocalRelation, by-position | Target UPDATE capture, command observation | `Unqualified Delta Ranger route: AppendData` | No transaction; no file or log change | Exact command target and literal input |
| SQL VALUES | AppendData / LocalRelation, by-position | Command analysis observation | `Unqualified Delta Ranger route: AppendData` | No transaction; unchanged | Exact command target and literal input |
| SQL INSERT SELECT b→a | AppendData / Project, by-position | Command analysis observation | `Unqualified Delta Ranger route: AppendData` | No transaction; unchanged | Original complete source batch plus actual writer input |
| DataFrame `insertInto` | AppendData / receipted Project, by-position | Actual Dataset analyzed receipt captured | `Derived or reused Delta plans are not qualified` | No transaction; unchanged | Deliberate command/source composition |
| DataFrame `saveAsTable` append | AppendData / receipted Project, by-name | Actual Dataset analyzed receipt captured | `Derived or reused Delta plans are not qualified` | No transaction; unchanged | Deliberate command/source composition |
| DataFrame `writeTo.append` | Two ordered AppendData / Project observations, by-name | Command observations retain distinct query objects | `Unqualified Delta Ranger route: AppendData` | No transaction; unchanged | Preserve actual analyzedQuery/V1 lineage |
| Raw-path append | AppendData / LocalRelation | Command observation | `Unqualified Delta Ranger route: AppendData` | No transaction; unchanged | Raw paths gain no named authority |
| CREATE TABLE AS SELECT | CreateTableAsSelect; no AppendData | Resolved command observation | `Unqualified Delta Ranger route: CreateTableAsSelect` | No transaction; unchanged | Separate future lifecycle contract |
| REPLACE TABLE AS SELECT | ReplaceTableAsSelect; no AppendData | Resolved command observation | `Unqualified Delta Ranger route: ReplaceTableAsSelect` | No transaction; unchanged | Separate future lifecycle contract |

The primary identity inventory records the captured target independently from each observed command
and records retained target/source validation after frontend loading. Those checks convey no command
authority. All command attempts retain the installed callbacks and fail before committed mutation.

The focused `primary-04` run's exact instance ordinals are below. Ordinals are local to each fixture,
not reusable authority. Captured target receipts validate after each frontend; all three retained
DataFrame source receipts also validate. Every observed command target is a different object from
the independently captured target. All options are empty at this command observation and every
`analyzedQuery` is absent.

| Frontend | Captured DeltaTableV2 | AppendData / actual DeltaTableV2 | Actual query identity | Resolved / root receipt present | By-name / options |
| --- | --- | --- | --- | --- | --- |
| insert-select | 30 | 61 / 62 | Project@63 | true / false | false / `Map()` |
| insertInto | 30 | 69 / 70 | Project@63 | true / true | false / `Map()` |
| raw-path | 30 | 34 / 35 | LocalRelation@36 | true / false | false / `Map()` |
| saveAsTable | 30 | 69 / 70 | Project@63 | true / true | true / `Map()` |
| values | 30 | 34 / 35 | LocalRelation@36 | true / false | false / `Map()` |
| writeTo | 30 | 69 / 70 | Project@71 | true / false | true / `Map()` |
| writeTo | 30 | 72 / 70 | Project@73 | true / false | true / `Map()` |

## Source and target obligations

The source receipt is consumed from the accepted analyzed root with `optimized=false`. No optimized
evidence API receives a raw analyzed plan. Every original scan occurrence is preserved, including an
empty source and both occurrences of a self-join. Separate ordinary SELECT controls run before the
measured source captures, leaving the transaction preparation Dataset unexecuted.

| Case | Retained identity and authority result | Native state / configuration / durable result | Remaining contract |
| --- | --- | --- | --- |
| Separate b→a | Original b scan; independent a UPDATE; both validate | No active transaction; unchanged configuration/storage | Bind both decisions to exact command input |
| Self-source, source loaded first | Same DeltaLog; distinct source/target descriptors; both validate | No active transaction; unchanged | Own source occurrence and target together |
| Self-source, target loaded first | Same retained target receipt survives source load; both validate | No active transaction; unchanged | Do not assume every future load preserves retention |
| Empty named b source | One original obligation for id/secret/region retained | No command authority; unchanged | Keep obligations when optimization removes rows/scans |
| Repeated b inner-join source | Two relation occurrences; shared descriptor/index/log; columns id/secret and id/region | Both source obligations and target validate; unchanged | Preserve occurrence identity, not only table identity |
| Separate catalogs sharing location | b source and a target share DeltaLog, retain distinct canonical bindings | Both validate; unchanged | Shared location does not merge policy resources |
| UPDATE-only request control | a UPDATE validates while b secret SELECT is denied | Source action rejection; unchanged | UPDATE never implies SELECT |
| Target read policy control | UPDATE capture produces no mask/filter evaluations | Source a still has `region = 'keep'` and secret mask; b secret is null-masked | Source decisions must survive write preparation |
| SELECT without observer | Rows `(1, a-one-masked, keep)` and `(3, null, keep)` | Healthy ordinary collection; unchanged | Baseline |
| SELECT with observer | Same rows, request inventory and production rule manifests | Healthy ordinary collection; unchanged | Observer adds no rejection or permission |
| Raw-path SELECT with observer | No named source authority | `Raw Delta paths are not admitted`; unchanged | Keep path rejection |
| Unreceipted clone with observer | Existing receipt tags removed from test clone | `Delta decision receipt rejected: missing receipt`; unchanged | Keep exact receipt provenance |

## Real native phases under the installed companion

The test driver receives the already captured target and source receipts, validates both before native
handling and again inside the real `withNewTransaction` scope, and retains those same objects after
idempotency handling. It constructs native DeltaOptions/WriteIntoDelta, delegates native
`hasBeenExecuted` and `writeAndReturnCommitData`, and contains no commit invocation. Effective
options include native `__use_null_as_default=true`; native AutoCompact resolution returns None.
No native-owned configuration reset is restored, and no new receipt conceals a transition.

| Case / phase | Exact input or transaction relationship | Last completed phase | First rejected phase / cause | Configuration and durable outcome | Required ownership |
| --- | --- | --- | --- | --- | --- |
| Duplicate app/version, reset enabled | Retained b source + a target; native preseeded app/version 1 | Real transaction and both entry validations | `hasBeenExecuted`: unqualified Aggregate | Full map identical; skip/reset/helper not reached; version 1 unchanged | Owned native transaction-state reconstruction |
| Duplicate app/version, reset disabled | Same finite setup with reset false | Real transaction and entry validations | `hasBeenExecuted`: unqualified Aggregate | Full map identical; no returned skip; unchanged | Same reconstruction owner |
| First app/version, reset enabled | New app with session version 1 | Real transaction and entry validations | `hasBeenExecuted`: unqualified Aggregate | Full map identical; first-write reset not reached; unchanged | Same owner plus later native reset continuation |
| Explicit options override differing session app/version | Options probe-app/1, session session-app/99 | Native options constructed; transaction entered | `hasBeenExecuted`: unqualified Aggregate | Options retained; full map identical; no skip/reset; unchanged | Bind effective options and owned config transition |
| Self-source preparation | Original Tahoe index returns the **exact active OptimisticTransaction** for the same DeltaLog | Native generator returned; idempotency false without app/version | Actual `queryExecution.optimizedPlan`: `Native transactional scan preparation is not qualified` | Full map unchanged; no helper or data/log change | Transaction-owned source preparation, without Snapshot substitution |
| Different-log b source, target a | Original source generator is Snapshot, distinct from active target transaction | Actual optimized plan returns | None through the measured preparation boundary | Idempotency false; unchanged; helper not requested | Preserve cross-log lineage separately |
| b source under unrelated q target transaction | Source Snapshot remains distinct from active q transaction | Actual optimized plan returns | None through the measured preparation boundary | Idempotency false; unchanged across all fixture tables | Do not infer all generators are transactions |
| Native helper, separate b→a | Same original source/target receipts validated in active transaction | Idempotency returns false, both receipts revalidate | `writeAndReturnCommitData`: derived/reused plan | No generated files/actions or commit; native readback unchanged | Own helper-created source composition |
| Native helper, self-source a→a | Same-log source/target retained | Idempotency false, both receipts revalidate | Helper: derived/reused plan | No generated files/actions or commit; readback unchanged | Helper provenance and transactional preparation are separate gates |
| Native helper, empty named source | Original source obligation remains present | Idempotency false, both receipts revalidate | Helper: derived/reused plan | Empty source is not a bypass; unchanged | Keep original authorization before native no-op handling |
| Native helper, CHECK-bearing target | Genuine native positive-id constraint created before installation | Idempotency false, both receipts revalidate | Helper: derived/reused plan | CHECK evaluation not reached; unchanged native version 1/rows | Native constraint execution remains downstream |
| Literal SQL construction probe | Actual `SELECT -1L ...` attempt; no Dataset returned | Independent target capture | Construction: `Unqualified Delta Ranger route: OneRowRelation` | Transaction/idempotency/helper not reached; unchanged | Qualify literal input without granting arbitrary DataFrame execution |

The idempotency causal stacks include `DeltaCommand.hasBeenExecuted`,
`OptimisticTransaction.txnVersion`, and `Snapshot.transactions` before the Aggregate rejection.
This is evidence that native transaction-state reconstruction blocks these attempts. It is **not**
evidence of an observed reset or of a successfully skipped duplicate. The original receipts still
validate afterward because the native call leaves the full SQLConf map unchanged. The test retains
post-reset validation logic, but this installed route does not reach a native reset.

The helper failure occurs before action return, SetTransaction generation and any commit path.
Transaction-active self-source preparation was measured in a separate driver invocation; it is not a
continuous successful helper lifetime. Native-only durable readback runs in a separate fresh session
after the installed attempt has closed. It does not resume governed execution without the companion.

## Checksum reconstruction and storage evidence

| History | Last completed boundary | Actual outcome | Durable result / ownership dependency |
| --- | --- | --- | --- |
| Complete native version-1 JSON/CRC/Hadoop sidecars | `target.validateCurrent` and same receipt validation | Both return; observed native head advances from 0 to 1 | Exact native artifacts published from preserved setup bytes; no new commit; current-head validation succeeds |
| Same genuine history without optional version CRC and its Hadoop sidecar | Native reconstruction observed as DeserializeToObject | Both validations return `Native Delta registration binding is unavailable`; cached head remains 0 | Published JSON is unchanged; reconstruction owner remains an availability requirement |

The observer's DeserializeToObject observation and the redacted outer validation exception are
separate evidence. The outer stack does not establish the hidden inner exception. The earlier
accepted target-authority diagnostic independently recorded a DeserializeToObject rejection; this
harness neither reproduces its temporary reflection diagnostic nor opens a private authority anchor.
No commit JSON is edited while retaining an old native checksum. Original version-1 bytes are
archived before setup stages them, and published-history manifests equal post-validation manifests.

Every fixture archives every regular file under its owned directory, including JSON, CRC, Hadoop
sidecars, data files and metadata configuration, with byte length and SHA-256. Primary before/after
manifests show no new files, changed bytes or committed versions for command/native attempts.
History cases publish only their exact setup-generated version-1 artifacts. All active native
operations and transactions are absent after unwinding. No helper-generated uncommitted files were
observed; their possible existence after a later, deeper failure remains part of the append contract.
Full SQLConf, identity inventories, policy requests/timestamps, cause stacks and storage bytes remain
in owner-only private evidence, outside this document.

## Combined regression evidence

| Spark | Delta | Tests | Failures / errors / skips | Installed fixtures |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 82 | 0 / 0 / 0 | 31 |
| 3.5.4 | 3.3.2 | 82 | 0 / 0 / 0 | 31 |
| 3.5.6 | 3.3.0 | 82 | 0 / 0 / 0 | 31 |
| 3.5.6 | 3.3.2 | 82 | 0 / 0 / 0 | 31 |

Each cell combines this class (8 tests) with the unchanged ReadRule (5), ActionValidation (17),
PhysicalAdmission (26), NativeSnapshotAdmission (2), RetainedNativeValidation (4), RetainedQuery
(4), and AppendTargetReceipt (16) classes. Scoped formatting checks pass. All builds run offline
with integration and Docker tests skipped; no live service runs are included.

The 31 case failure-phase/message inventories and durable before/after version maps agree across
all four pairs. Independent archive checks verify 2,052 storage-manifest entries per pair, 8,208
entries across 124 fixture executions. Each cell retains all eight JUnit XML files, complete private
traces and storage bytes, loaded class origins, launcher and resolved artifact hashes, exact source
copies, and copies of the moving project JARs and compiled class directories before the next build.
These results preserve the first blockers above; they add no successful installed append, native
reset, constraint execution, action return or commit coverage.

## Next dependency and support limits

Design exact command-target/source ownership together with finite native-plan provenance before
admitting AppendData. The observed command table differs from the independently captured native
object; the future owner must bind the actual command target, original source obligations and the
V1 `analyzedQuery` that is currently unreachable. Its native lifetime must own transaction-state
reconstruction, self-source scan preparation and helper-created plan composition. The allowed
native-owned transaction-version reset must be an explicit continuation of the full configuration
contract; the present blocker does not remove that downstream requirement.

Only then select and qualify a native V1 writer integration, including its columnar admission
interactions. The native-only decorator precedent does not select a production strategy by itself.
Precommit permission/publication checks, commit, command-result replay, recache, postcommit hooks
and cloned-session behavior are not reached or newly qualified here. Prior native recache results
remain a downstream design dependency.

No live S3/Ranger delivery, current Gravitino owner-dependent append authorization, arbitrary-code
sandbox, cross-system atomicity, simultaneous-writer support, full Delta write coverage or CDF is
claimed. The approved governed-append design remains the integrated acceptance contract. CDF
remains the first feature priority after Delta maturity.
