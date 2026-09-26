# Physical guard independent review brief — frozen source 2

Current review base: 1dd224183fac4f47928ae1d9e88f838eecf826a9.
Head: frozen uncommitted worktree, exact 16 module changes in
`task-4-evidence/physical-guard-review-package-2.diff` (274224 bytes), SHA256
`873729782fd6cb9c6aeaeaa97fb7742f8363eb2f832eadc17c16be4d63d79c80`.
Composite source manifest `task-4-evidence/physical-guard-source-freeze-2.json`
has 87 files, SHA256 `8fd4acdeae6528b77581b65a7e8cef61889963cf38197c9243ae792cfe1db6c4`.
Root verified all current, first-row pre-run and archived source matches.
Implementer report: `task-4-physical-guard-report.md`; its final freeze2 handoff
supersedes earlier pending/failure notes. Read the historical failure diagnosis
below as context, not as a claim that current correction is unverified.

Root independently verified meaningful partitioning RED1/1 and corrected GREEN3/0
from original XML/exits, including original MaskRules and mutable-schema regression.
The first full freeze1 run was 334/1, all 333 other tests passed; its single failure
is diagnosed and corrected. Corrected four full-module matrix rows are running
sequentially under author ownership, each `physical-guard-freeze2-final-<Spark>-<Delta>`.
Root owns their verification after this review starts. Your verdict may be
conditional on those exact remaining evidence gates; do not infer they have passed.
No source/README mutation or concurrent Gradle is permitted during review.

Review only this optional-module increment. Read the physical-provenance design,
implementation brief including all root rulings, verification checklist, and
final report. Audit all nine gates against named original XML and actual test
assertions. Distinguish component evidence, real installed evidence and unsupported
routes. Root owns remaining profile evidence and any local checkpoint.

Required independent SPEC verdict, then QUALITY verdict with concrete findings:

- Complete executable logical recipe bootstrap and exact class/order/config
  qualification before invoking optimizer/planner/columnar/adaptive extensions.
  Unknown extension tests must demonstrate zero invocations. No candidate
  optimization into acceptance, execution-for-discovery, or collection-time replay.
- Exact physical source/function/expression/output/edge binding and mutable values.
  A copied logical link/tag or same-path source is not authority. Check complete
  authority recovery, qualifiers, eliminated and duplicate obligations, stock
  transition/codegen/reuse tail, and changed reused output/hidden reference.
- Retained collect/collectAsList validates full original receipt and actual child,
  including rechecking physical binding after final metadata callbacks. Verify
  before-first and after-success denials, two-catalog broadcast, lifetime and
  native-head compatibility/drift. Unchanged positive controls must really execute.
- Preparation-order proof is composed: unchanged guard ordering, instrumented
  child component tests, installed entry stack plus user scan/warm-job evidence.
  Assess whether that composition supports zero user work; direct real-stock-child
  prepare/execute counts were not measured and must not be claimed.
- Native tickets consume exact successful owner evidence. Exercise delayed
  execution after close, pending and failed owner denials, exact state/stats RDD,
  fresh rebound owners and mixed/forged/foreign sources. No receipt absence fallback.
- AQE hidden initialized plans/stages/reuse must retain exact evidence and exchange
  root categories; finite coalesced coverage only, no executing/materializing for
  discovery. User AQE rejection must preserve the user's setting. Record limits.
- Real full/nested user caches rejected at logical and physical boundaries, copied
  receipts included; real native persisted caches remain valid with caching enabled.
- Clone/canonicalization/serialization/rendering/task/cleanup/downstream failure
  tests: transient inert references, no authority leakage or global current-plan
  registry, no production test hooks/reflection/private bridge. Explicitly distinguish
  component-only mechanics from actual installed lifecycle coverage.
- Remove temporary detailed binding/identity diagnostic logs before qualification.
  README must accurately state supported entrypoints, finite pinned internals, and
  remaining full Delta/live integration work. No Gravitino core/API/server changes.

Do not modify production/tests, run concurrent Gradle, commit, or touch services.
If evidence is incomplete, identify the exact missing observation. Do not accept
nine passing tests as equivalent to nine fulfilled integration gates. Do not infer
full Delta support or live Ranger/S3 acceptance from this local module checkpoint.

Specific regression probes raised during implementation: compatible native-head
advance uncaches old snapshot, causing owned getter to return reconstruction, not
LogicalRDD. Review the bounded explicit lineage fix and original RED, not only
initial snapshot cases. Also verify exact raw-bit Float/Double equality in complete
executable logical and physical recipes (physical-only substitution must not be
hidden by original logical receipt validation), and explain any generic Product
structural correspondence beyond the approved exact Tuple2 native container.

## Binding global constraints (verbatim from production runtime spec)

- Gravitino owns table registration. Native Delta metadata and data remain authoritative in S3.
- Qualify Spark 3.5.4 and 3.5.6 against Delta 3.3.0 and 3.3.2, all four combinations, with Scala 2.12.
- Ranger 2.8.0 is the required authorization target. The four-level catalog/database/table/column Ranger resource service is provisioned for exactly one Gravitino metalake, shared consistently by pushdown and Spark enforcement; labels do not supply a missing metalake resource dimension. Downloaded-policy Spark enforcement and catalog-demo integration follow production artifact qualification; a dependency report alone does not prove enforcement.
- Do not modify core, API, server, client, or storage-schema source. Build wiring outside those source trees is allowed.
- The platform prevents concurrent writer jobs for one dataset. This does not independently fence an old writer after failure.
- Preserve existing generic catalog behavior and default connector dependency isolation.
- No shared deployment, remote push, merge, or PR creation is included. Preserve local evidence and unrelated work.

Subsequent user authorization on 2026-09-23 supersedes only the remote-push
exclusion: root is to push the branch at accepted milestone closure, then keep
subsequent work local. This does not authorize reviewer Git mutations, a PR, merge,
shared deployment, or publishing current unaccepted WIP. Preserve all evidence.

Review scope is this physical-guard increment, not full Delta conformance or live
Ranger/S3 acceptance. Root owns remaining frozen profile verification; report
cross-task or missing evidence explicitly instead of assuming completion.

## Frozen row 1 result (supersedes pending status above)

The first full row physical-guard-final-3.5.4-3.3.0 completed original XML
334 tests / 1 failure / 0 errors / 0 skipped, exit1. The sole failure is
TestDeltaRangerMaskRules.installedExtensionExecutesFreshNamedScansAndInternalNativeReconstruction
at a captured native-UDF rebind assertion. Formatting passed; Javadoc was not
reached. Freeze1 and review-package1 are preserved failed-row evidence, not an
acceptance candidate. Focused diagnosis and a new freeze are required before
this review is dispatched; root-failure-verification.json archives exact XML
hashes. No review verdict exists.

## Full-regression partitioning finding

The single full-row failure was investigated through preserved mask-capture,
mask-current-cache and mask-cache-fields diagnostic rows. Original-owner delayed
proof succeeded. The rejected captured cache uses the SAME current stats RDD,
with equal output/ExprIds/schema, ordering and streaming fields. Exact product
field2 is a separately allocated but value-equal stock UnknownPartitioning.
NativeProvenance.value used opaque Identity fallback, so fresh rebind rejected.
Root authorized only an exact UnknownPartitioning class/numPartitions immutable
snapshot, with unchanged fresh-wrapper positive and changed count/type negatives,
original MaskRules positive unchanged and mutable nested-schema regression.
Do assess this finite value binding and its tests; no generic Product expansion
or suppressed positive expectation is authorized. Both pinned source references
are preserved under physical-guard-partitioning-sources.

Review timing ruling: after focused correction green and freeze2, review may
run concurrently with the four full-module rows. Root owns final original XML,
source/compiled artifacts/dependency pins and identical-case matrix verification.
No increment acceptance until all four rows and both review verdicts pass.
