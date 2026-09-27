# SDD ledger — plan: docs/superpowers/plans/2026-09-26-delta-retained-query-harness.md

Parent: approved production-runtime Task4, finite retained SELECT live qualification.
Accepted BASE: 55d8b746a2f31c1b185684e42c6c43ef8b1372b7.
Spec: docs/superpowers/plans/2026-09-26-delta-retained-query-harness-design.md.

## Preflight

| Tasks | Shared file/interface or self-consistency | Finding |
| --- | --- | --- |
| Task1 self | Ten test sources; parser/holder/shared action/gateway/driver tests and contracts agree with spec; legacy fields verified against current driver | Consistent after planner revisions. Exact close, generation-bound response bytes, shared native executor and failure advancement present. |
| Task1 internal gateway/security | Completed HTTP200 metadata and digest consumed by latest post-cursor generation matcher | Consistent; later invalid response cannot be bypassed and NONE advances observed cursor without claiming refresh. |
| Task1 internal holder/action/driver | Actual Dataset/root refs, immutable result, persisted pass marker then cursor assignment | Consistent; expected denial is a completed command; unexpected action/write failure terminates and clears refs. |
| Task1 internal native test/action | Native fixture invokes shared executor used by driver | Consistent; exact rows/denial, refs and named scan observations tested without external services. |

Only one task; no cross-task shared pairs. Ruling: treat test harness as elaboration of already authorized production-runtime Task4 — bounded test-only changes enable required live evidence — cost if wrong is rework of a reversible test protocol.
Ruling: root retains Git mutations and fixture operations while implementer owns source/Gradle — preserves established credential/evidence checkpoint controls — cost if wrong is delayed checkpoint, not lost source.

Task1: pending dispatch. Final focused matrix and independent SPEC/QUALITY review required.
Parent full milestone and broader Delta goal remain open; user-authorized remote push occurs only at milestone closure.

Task1: dispatched /root/implement_retained_harness (gpt-6-sol); exclusive ten-source-file/Gradle owner, root retains Git/services. JDK17 path supplied; no live flags.

Task1 first freeze: all4focused profiles19/0/0/0 each,76total, testcase multisets identical; root verified exact10source hashes/original XML/exits and archived review package (131113bytes) under evidence/task-1-review-package.diff. Final compiled snapshots are reconstructed package evidence, explicitly separate from original tests, no claim they were archived at original test execution. Independent review not yet dispatched.
Author self-review found nested mutable collections beneath unmodifiable Result.evidence map. Root confirms contract requires defensive recursive collection snapshot/freeze, not Throwable internals. Author correcting within10files, adding nestedmutation regression, preserving firstfreeze unchanged and running newlylabelled freeze2 fourprofile qualification. Firstfreeze is superseded for acceptance once correction lands; do not overwrite evidence or reuse passingtests for changedcode.

Task1 self-review correction RED: nestedrowmutation changedexpected1to2, then GREEN after defensive recursivecollectionfreeze. Originalfreeze76pass evidence preserved. Authorreportsfreeze2sourcehashed; correctedfourprofile matrix now capturesoriginalXML andcompiledartifacts at eachterminalpinboundary. Awaitreport/review; no Gitcheckpoint or milestonepush.

Task1 independent review /root/review_retained_harness: SPEC issuesfound, QUALITY needsfixes (task-1-review.md). Important1 requiresactualdriver PREPARE/COLLECT/RELEASE serializedenvelopes/close/malformedcleanup/privatepath/cap behavior tests; currentnativecompleteCollectonefield surrogateinsufficient. Important2 requirespostcursorPOLICY/BOTH andeffectiveunion integratedactualrecorder/appliedgeneration tests. Minor3exactdigestassertion andMinor4LocaleFQNs recorded. OriginalREDomissions remainhonestprocessgaps, not recreated.
Task1 fixround1/5 dispatchedoriginalauthor /root/implement_retained_harness, source/Gradle ownership restored within10testfiles. Driver-used runCommands seam withinexistingtestapp permitted; production unchanged. Newtests useactualin-processgateway/engineversions. Ruling: normalLocaleimports followexplicituserAGENTS requirement duringaffectedfilefix — no userinstructionexception justified — cost ifwrong is trivial reversibleimportchange. Exactdigestassertions fitrequirednewwireselectiontests. Newfix1labelledfourprofile evidence, oldfreeze2preserved.
Rootfreeze2artifactverification: four366class archives/pinneddependencyreports hashes verified;119production entries and247testclass exclusions verified incompanioncaed200d88440a0a0b45f6090d01513812bc5dcbdfdef7bb4a05d4233b36c856, archived freeze2-runtime-package. Rawcompiledclassbytesare not identicaltoshadowedjar dueconstantpoolrewriting; oneNativeScansPreparedjavap diagnostic confirmsidenticalsignatures/instructionsafterindexalignmentnormalization, no wholejarbyte-equivalenceclaim. ExistingNOTICE/LICENSEduplicatesalsoinhistoricalacceptedcompanion; no duplicateclasses. Actualcurrentpackagedruntime livequalification stillrequired, not inferredfromunitclasspath.

Fix1 author update: driver-usedrunCommands nativeenvelope/close/malformedGREEN attempt1exit0. AddedPOLICY/BOTHrealrecorder/exactdigest/Locale/cap4097andnativeeffectiveuniontests; firstpinrunstarting, no reportedblocker. No finalfix1matrix/re-review/checkpoint yet.

Fix1 final matrix: root verified four23/0/0/0 original focused XML rows,92total, identical inventories, pinned dependencies, successful exits, exact10source and compiled archive hashes. Packagechecks exit0; companion caed200d... identical accepted freeze2 packaging, no testharness/duplicateclass entries. Author released source/Gradle, report appended. Scoped re-review dispatched original reviewer using fivefile fix-onlydiff ef22f87b... against reconstructed/hashverified freeze2 source, not broad HEADdiff. No checkpoint/push yet.

Task 1: fix round 1/5 (4 addressed, 0 open; uncommitted frozen fix1 reviewed against freeze2). Independent SPEC compliant / QUALITY approved, no new breakage. Nonblocking assertion opportunities: compare marker sequence/id/retentionId values to result; compare driver serialized selected digests to gateway records. Dedicated digest tests already cover exact values. Carry to finalwholebranch review; no extra fix loop. Task 1: complete (review clean, exact safe checkpoint pending). Offline harness only; live/entiremilestone remains open.
