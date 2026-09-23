# Independent frozen analysis-receipt review

**SPEC: PASS for the amended task-scoped logical receipt contract. QUALITY: READY.**
No actionable HIGH, MEDIUM or LOW findings were identified in this frozen change.
This verdict does not complete Task 4's subsequent native-operation work or the
larger Delta/Ranger implementation.

## Provenance and scope

Reviewed the 14 changed/new module files in
`task-4-evidence/analysis-receipt-review-package-2.diff` against base
`513e13ba1ee85dd8226f2785c636fd2b76071f79`, including README and the final explicit
mask-Project-removal assertion. This is a working-tree review, not a commit review.
Independently hashed all 14 current files against
`analysis-receipt-review-source-hashes-2.json`: all match. The implementation brief,
its amended baseline-proven fixture section, optimizer reconciliation and author
report supplied the contract; source and original XML supplied the behavioral
evidence. The initially pending supplements completed during review.

## SPEC assessment

Paths below are relative to `spark-connector/v3.5/spark-delta-ranger/`.

- `src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerReadRule.scala:75`
  resolves each extracted original scan through the actual wrapper's registry and
  takes its engine from that binding. It retains per-occurrence obligations,
  completes the deduplicated original access batch before any decision request,
  and uses one Instant across engines. `DeltaRangerMaskRules.scala:37` captures
  the optional table filter and every requested column decision, including NONE;
  rewriting consumes the captured values. Canonical descriptor identity and the
  four-level resource hierarchy are preserved.
- `src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerDecisionReceipt.scala:72`
  performs original publication/lease/owner checks before final engine publication,
  subject/group and configuration checks. The second frozen-tree/candidate
  comparison follows metadata callbacks. The authority object is a plain class;
  its reference at line 100 has transient authority and constant rendering, and
  deserialization fails closed.
- `DeltaRangerDecisionReceipt.scala:186` and `:256` use closed expression/node
  classes and structural bindings, including output ExprIds/UUIDs, alias fields,
  copied mutable literal payloads, floating-point bits, source object identities,
  descriptors, schemas and ordered children. Analyzer reentry and pre-CBO
  consumption use this authority rather than subject/tag equality.
- `DeltaRangerDecisionReceipt.scala:330` qualifies the ordered actual optimizer
  prefix and strategies, typed Delta rule and consumer position. Replay operates
  on the retained approved clone under captured SQLConf; the candidate is compared
  directly. Removed sources remain in the original inventory. Unknown callbacks,
  clock-dependent casts, cache substitution, row tracking and the identified
  statistics-producing join configuration reject explicitly.
- The diff introduces no native admission widening, physical guard, core/API/
  server/client/storage-schema change, or runtime setting that disables AQE,
  caching or native data skipping. Existing catalog-update checks remain.

## QUALITY and evidence

The tests exercise installed native SELECTs, CUSTOM/NULL masking, table filters,
both catalog orders, quoted mapped columns, separate engine owners, source/tag
spoofs, mutation/drift ordering, replay transformations and explicit limitations.
The mask-removal supplement reconnects original unmasked attributes and asserts
resolution before requiring the logical-binding failure, so it does not succeed
merely through dangling attributes.

Independently parsed and checked the original XML hashes against the root matrix
inventory: 128 + 53 + 53 + 53 = **287 executions**, zero failures/errors/skips.
Independently read each supplement's exit and XML: **four additional executions**,
one per supported Spark/Delta pair, all exit 0 with zero failures/errors/skips.
The original six cited behavioral RED groups retain the reported assertion
failures in their XML; unrelated intermediate failures remain visible.

Reviewed `analysis-receipt-root-matrix-verification.json`,
`analysis-receipt-root-supplement-verification.json` and the completed supplement
audit. Root independently verified 47 archived source files per row, 44 compiled
archives per four-row matrix, and actual Spark/Delta/Ranger 2.8.0 jar pins. This
review did not repeat that complete archive audit. The final freeze changes only
the receipt test source; the original full matrix is correctly distinguished
from the supplemental final test bytes.

Focused unchanged-code checks were limited to these named risks:

- **Foreign source/owner substitution:** registration `resolve` checks exact
  descriptor/relation/index/DeltaLog ownership and recorded native/schema values;
  canonical extraction uses the descriptor and verifies per-scan referenced
  columns. No path-to-name authority was introduced.
- **Fresh grammar widening:** existing `PlanAdmission.query/expression` remains
  finite; optimizer-only empty LocalRelation/InSet forms are reached through
  comparison with trusted replay, not fresh admission.

## Limits and remaining gates

The asymmetric self-join installed failure is a demonstrated baseline limitation.
Original baseline XML contains the passing direct-rule component control and the
failing installed query. Current
`src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerDecisionReceipt.scala:470`
asserts the precise native LogicalRDD/PreparedDeltaFileIndex failure; the separate
component fixture makes no receipt or installed-enforcement claim. `README.md:147`
states this boundary honestly. Full installed asymmetric self-join support remains
required follow-up work.

Current native-head checks, native-operation ownership/tickets, installed physical
guards, retained-action/cache enforcement, DML/streaming/maintenance, remaining
masks and packaged deployment remain outside this verdict. Observed drift checks
do not establish atomic protection against concurrent change-and-restore.

No Gradle/JVM/service/Docker execution, source/index/git mutation or additional
agent was used. Graph/taint analysis was not performed because this assignment
prohibited index work and required a frozen diff review; no graph-backed coverage
is claimed. No memory-search/save connector was available. No task-scoped test
evidence remains pending.
