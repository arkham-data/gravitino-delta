# Retained binding and complete action decision validation

Status: DONE for the authorized component increment. Independent review is SPEC PASS / QUALITY READY with no findings; all four final rows pass on the same frozen source. Root retains the final acceptance audit and integration. This report covers the authorized component increment against `a1f2ce4b5e9d22d36f5e712312576e0837a3d999`, not physical or retained-collection enforcement.

## Implementation

`DeltaRangerDecisionReceipt.validateAction(session)` consumes the actual original driver receipt. It checks session, captured configuration, authenticated short user/groups, complete engine/role stamps and retained original structure. Every original occurrence remains an obligation, including self joins and optimized-away scans. One fresh immutable Instant is shared by all action requests. All original access resources across every engine are authorized before any filter/mask request. Outcomes are compared by filter presence/expression and mask kind/expression, including absent filters and NONE. The validator never rewrites the retained plan, creates a replacement receipt or invokes a user physical child.

`BoundRegistrationScan.validateCurrent(session)` checks registry membership and exact retained descriptor/relation/index/log before reading current scoped metadata. It requires the same canonical name, registration incarnation, location, native ID and logical schema. Its new binding-owned `VALIDATE_RETAINED_BINDING` native operation reads the current head through that exact retained log and captures its actual state/statistics lineage using the accepted native protocol. Raw schema, ordered partitions, protocol and mapping configuration must exactly match retained stamps. Fresh metadata reads bracket native work; a second native-head observation after metadata I/O detects observed native version movement within that bracket. Operation close always clears its thread; failed operations cannot authorize later evidence.

Each successful pass returns an ordinary non-Product, non-Serializable `CurrentObservation`, bound to the exact scan object and Spark session. The action retains its initial vector only for that invocation, then compares every final observation's complete current publication and native version after policy evaluation. Thus independent compatible reads cannot hide a version change observed across the whole action. Final metadata work precedes the final original-structure, engine, subject, configuration and liveness checks.

Publication and actual admitted snapshot version have separate lower bounds. A publication may legitimately trail the loaded snapshot. Compatible data-only advance between actions is permitted, including a refreshed publication, while the retained plan may keep its original snapshot. No latest-rows guarantee or mutable cross-action high-water registry is introduced. Ordinary load and analysis `recheckPublication` retain their exact-publication contract; action validation does not replace structural optimizer replay.

Java remains compiled before Scala. No policy-engine, metadata-reader, build, core/API/server/client/base-connector or storage-schema changes were needed. Existing engine Instant overloads, semantic MaskDecision equality, complete stamps, finite metadata lease and native capture protocol are reused.

## TDD and attempt history

All attempts use the preserved `task-4-evidence/run-gradle.py` and unique `action-validation-*` labels. Behavioral runs capture original XML and Test.doFirst compiled inputs. Every command supplies Spark 3.5.4/Delta 3.3.0 during iteration, `-PskipITs --offline --console=plain --max-workers=2 --no-parallel`, and the existing classpath-capture init script. Ranger is pinned by the unchanged module to 2.8.0. The harness sets JDK17 explicitly. Each attempt's `command.json` is the exact command record.

| Evidence label | Result and interpretation |
| --- | --- |
| `action-validation-contract-red` | Exit 1 before tests: new Scala tests had formatting violations. Not behavioral RED. |
| `action-validation-red-format` | Exit 0: formats the initial tests. |
| `action-validation-contract-red-2` | 3 tests, 3 expected failures: no fresh filter calls, revoked access accepted, and a changed mask accepted by the old validator. The changed-filter assertion was not reached until GREEN. The initial test helper called existing analysis-only `revalidate`; this characterizes the missing action contract, not a bug in the existing analysis contract. |
| `action-validation-decision-green` | 3/0/0/0, exit 0. Tests now call the new actual `validateAction`. |
| `action-validation-native-red` | 7 tests, 4 expected failures: the old exact-publication check rejects compatible advancement, while changed native schema and two observed-drift cases are accepted. Demonstrates missing new retained-native behavior. |
| `action-validation-native-green` | 7/0/0/0, exit 0 with binding-owned native inspection and per-action observations. |
| `action-validation-controls` | 16 tests, 1 assertion failure. Catalog close also closes the policy engine; the test expected SecurityException but the existing entry stamp check correctly threw IllegalStateException, `Delta Ranger lease is closed`. No production fix was needed. |
| `action-validation-lifetime-control` | 1 test exercising eight before/during lifetime cases, zero failures, exit 0, after correcting that assertion to its actual boundary. |
| `action-validation-native-controls` | 4/0/0/0, exit 0: actual operation lineage, independent version lower bounds, inner/whole-action publication drift, protocol/partition/mapping changes. |
| `action-validation-final-format` | Exit 0: final formatting and strict Javadoc. |
| `action-validation-matrix-s354-d330` | Exit 1 before tests: Gradle scheduler could not progress when Javadoc preceded cleanTest/test. Retained as an unsuccessful build attempt; retry uses the accepted baseline task ordering, with no source change. |

Iteration commands containing SpotlessApply have pre-format source archives and the exact executed compiled inputs; they are not final frozen-source equality claims. Final matrix commands use SpotlessCheck only.

## Verification map

New test names below belong to `TestDeltaRangerActionValidation` unless the `RetainedNative` prefix names `TestDeltaRangerRetainedNativeValidation`.

| Contract | Covering component evidence |
| --- | --- |
| Real authority | Every new test captures a receipt from actual named Spark SQL through NativeBoundCatalogFixture; `RetainedNative.retainedOperationCapturesExactCurrentLogAndKeepsPublicationLowerBoundSeparate` observes the actual typed operation and exact retained index/current snapshot lineage. Existing registration identity/substitution suites remain in regression. |
| Current registration | `registrationReplacementPendingMissingAndLogicalSchemaChangesRejectAtRetainedBoundary` pairs unchanged controls with ID/generation/token/location/native ID/pending/missing/logical-schema and too-new metadata versions. |
| Exact native compatibility | `changedNativeSchemaRejectsWithoutReplacementTableLoad`; `RetainedNative.protocolOrderedPartitionsAndMappingChangesRejectWithLogicalSchemaUnchanged` restores real Delta-generated commit histories, proving changed protocol and reordered partition names with unchanged raw schema, and changed physical column mapping with unchanged logical schema. |
| Version semantics | `compatibleDataAdvanceBetweenActionsKeepsOriginalReceiptAndSnapshot`; the retained-operation/lower-bound test additionally permits publication 0 with admitted snapshot 1 and rejects a publication regression from its independently captured baseline 1. Inner native drift and whole-action native drift have distinct tests. `RetainedNative` inner/whole-action publication-drift tests keep native head stable while publication changes. Native head regression has a direct source guard; no artificially lower head is forced through the normal monotonic retained DeltaLog. |
| Finite lifetime | `finiteCatalogMetadataAndEngineLifetimesRejectBeforeOrDuringValidation`: catalog close, metadata close, deadline expiry, engine close before entry and in the final metadata read. No lease is renewed. Existing `MetadataClient.realClientBuilderUsesSeparateMetadataPrincipalAndPreservesQueryUgi`, `lazyReadSupplierOwnsOnlyItsClientAndDoesNotUseMutationActor` and `RegistrationReader.scopedMetadataReadLeavesQueryIdentityUnchangedAndOnlyUsesOwnedReadClient`, plus scoped decoding/exception tests, are reused unchanged. |
| Complete original access | `unchangedActionReevaluatesCompleteOriginalInventoryAtOneFreshTime` retains duplicate self-join accesses. `twoCatalogsAuthorizeEveryOriginalResourceBeforeAnyDecision` proves global ordering and second-engine deny before any decision. `eliminatedScanStillRequiresItsOriginalAccessAndRegistration` retains a receipt after actual empty optimization. |
| Canonical owner separation | `sharedNativeLocationKeepsCanonicalOwnersAndFinalCrossCatalogEngineChecks` proves distinct descriptors/catalog resources on the same native log and detects first-engine publication change during the other catalog's final metadata pass. |
| Captured decision equality | `unchangedPublicationCannotHideChangedMaskOrFilter`; `absentFilterAndNoneMaskRemainObligationsAndTimeDependentChangesReject` changes results based on fresh request time, with access allowed and the complete engine stamp still valid. |
| One fresh action time | The self-join and two-engine tests assert one request Instant across the entire action and a time newer than analysis. Existing engine tests preserve defensive Date copies per request. |
| Complete publications | `completedRoleOnlyAndPolicyPublicationsInvalidateRetainedAction`: role-only mapping publication with unchanged policy/context, policy refresh, and same-number replacement. Two-engine and final-metadata tests cover late publication changes. |
| Subject/configuration | `sessionUserGroupsAndConfigurationChangesRejectBeforeEvaluation`; `finalMetadataWorkPrecedesEngineSubjectConfigurationAndStructuralChecks` changes engine, subject evidence or configuration in the final metadata read and verifies the actual failure boundary. |
| Retained structure | The latter test mutates an original UTF8 literal during final metadata work and requires retained-logical-binding rejection. Existing analysis replay tests remain unchanged; compatible publication advancement explicitly still fails exact analysis revalidation. |
| Driver-only authority | `transientObservationIsBoundToExactOccurrenceAndCannotSerialize` rejects observation substitution between self-join occurrences, confirms no Product/serialization authority, and checks constant rendering. Existing receipt reference serialization/rendering tests remain unchanged. |
| Qualified scope | All new enforcement claims are direct component calls on actual receipts. No physical guard is installed and no retained collect/collectAsList revocation is claimed. |

Failure boundaries are deliberately distinct: Ranger access denial; changed action semantics; retained-registration validation (sanitized metadata/native/within-bracket/lifetime failure); whole-action observation change; stale engine publication; subject/configuration/original-structure rejection; and the existing closed-engine lease IllegalStateException before entry. A failed closed lease is unavailable runtime state, not a Ranger policy deny. The tests assert those boundaries rather than accepting arbitrary Spark/unsupported-shape failures.

## Frozen source and final matrix

Freeze: `task-4-evidence/action-validation-source-freeze.json`, 82 files, SHA-256 `917843641f3ea7ab95906c681614ca8361c0f1d7d069596130c23e871c2f9bf0`. Only six captured paths differ from the accepted native checkpoint: three production files listed above, module README, and the two new test suites. No source formatter or source mutation is permitted during final qualification.

First full row uses the accepted order `spotlessCheck cleanTest test javadoc`, the existing Test.doFirst init script, explicit `-PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0`, and the unchanged JDK17/Ranger2.8 harness. Actual original XML confirms 297 full-module tests (276 existing plus 21 new), with zero failures/errors/skips. The exact affected inventory is 149 tests: prior eight native suites 59, new action suites 21, RegistrationBindings 10, RegistrationReader 19, MetadataClient 20 and PolicyEngine 20.

| Evidence label | Spark | Delta | Selection | Actual result |
| --- | --- | --- | --- | --- |
| `action-validation-matrix-s354-d330-2` | 3.5.4 | 3.3.0 | Full optional module | 297/0/0/0; exit 0 |
| `action-validation-matrix-s354-d332` | 3.5.4 | 3.3.2 | Affected suites | 149/0/0/0; exit 0 |
| `action-validation-matrix-s356-d330` | 3.5.6 | 3.3.0 | Affected suites | 149/0/0/0; exit 0 |
| `action-validation-matrix-s356-d332` | 3.5.6 | 3.3.2 | Affected suites | 149/0/0/0; exit 0 |

Affected selection: NativeOperations, NativeSnapshotAdmission, NativeCacheCapture, DecisionReceipt, MaskRules, ReadRule, RegistrationBindingsNative, NoSkipping, ActionValidation, RetainedNativeValidation, RegistrationBindings, RegistrationReader, MetadataClient, and PolicyEngine (all with `TestDeltaRanger` prefix). This includes unchanged consumer foundations as well as every affected native route. The full-row original XML supplies the 149-test affected inventory. All three affected rows have the exact same testcase multiset, verified as a multiset subset of the full row. The final matrix totals 744 invocations, zero failures/errors/skips.

The first row's `action-validation-matrix-s354-d330-2-verification.json` verifies all 82 archived/current source hashes, 11 archived compiled inputs, exact Spark/Delta/Ranger pins, original XML and exit status. `action-validation-jdk-version.txt` records OpenJDK 17.0.16; the live `action-validation-jdk-process-probe.json` observes Java executable paths from that JDK while the full row ran, with no process arguments captured. This supplements the harness's explicit JAVA_HOME. Because process arguments were not captured, the probe establishes executable provenance only and does not identify worker roles; no per-Test.doFirst runtime-version field is claimed.

The final `action-validation-matrix-summary.json` records each original XML file hash, exact testcase inventory, dependency pins, the common source freeze, and compiled manifest identity. Per-row `*-verification.json` files verify all 82 archived/current source hashes and all 11 compiled-input archives, plus unchanged external JAR hashes. `action-validation-run-inventory.json` retains all 15 completed Gradle attempts, including every failing/pre-test attempt; no historical result was relabeled as final-source qualification.

| Final row | Test.doFirst execution-classpath manifest SHA-256 |
| --- | --- |
| `action-validation-matrix-s354-d330-2` | `55869ba17ba3b0c35770ecf4a14363090b6b2d003cc4f92a1a4e46e74afefe23` |
| `action-validation-matrix-s354-d332` | `0b1cfc44c6466ae0743c3b8866226dc1903f79c61b649c4b207609ab0aa8eb1c` |
| `action-validation-matrix-s356-d330` | `8b964d633c3e9f15fdc82a1362394ce9f32e126597e4bd96b121bab1fbe40a0e` |
| `action-validation-matrix-s356-d332` | `03cdec834aa840235da62ebd48d352309d1d10664610133335f7a3d3441f5e75` |

## Self-review and remaining limits

Reviewed new Java imports, member visibility, nullable handling, public override Javadoc and try-with-resources. Root Java compilation retains `-Werror`; strict Javadoc and module Spotless checks pass in the stated attempts. This baseline has no Checkstyle task. Gradle deprecation and existing environment/build warnings are not called pristine output. Independent review also found Spark plan-string truncation, ignored-path and empty-policy warnings in original XML, all present in the accepted native-operation baseline.

RegistrationBindings is an existing multi-responsibility Java file; new compatibility and observation logic remains with its actual owner rather than introducing a parallel registry. Native/current observations are optimistic: they cannot detect an unobserved change-and-revert or cancel an update after the final admission boundary. Native DeltaLog normally advances monotonically; the lower-native-head rejection is implemented but is not claimed as a forced actual-native negative test. No new reflection, private-package adapter or artificial production injection seam was added for that case.

Physical provenance classification, physical-child witness verification, guard installation, retained Dataset collection qualification, full Delta read/write coverage, live Ranger downloads, Docker/deployment, policy writes and catalog-demo changes remain outside this increment. No subagents, git operations, service changes or publication were performed. No callable memory-search/save tool was exposed; accepted local designs/reports supplied prior context. The independent `task-4-action-validation-review.md` reports SPEC PASS / QUALITY READY, no Critical/Important/Minor finding, conditional on root closing final matrix evidence. Root owns final evidence audit and integration.
