# Reused Delta SELECT execution boundary: source audit and proposed first slice

Audited HEAD `88478f00896c65d157f5f0d688bc0a59140bd543`. Read-only production audit;
this report is the only artifact written. No compilation, JVM, tests, services,
Docker, mutation, or commit was performed. Proposed behavior below is not qualified
implementation. Scope remains Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0,
trusted launchers, and the existing platform exclusion of concurrent dataset writers.

## Recommendation

Reject a previously authorized plan when its actual query subject/groups, engine
generation, or canonical registration incarnation differs at execution. Require a
new named SELECT to obtain new authorization, masks, and row filters. Do not attempt
to remove or replace policy expressions in an already optimized/physical plan.
Initially reject user SQL cache consumption unless a separate cache provenance
contract is implemented. This is a bounded SELECT boundary, not arbitrary Spark/RDD
job security or revocation of rows already returned.

The accepted production design explicitly limits its proof to fresh analysis. The
next-step requirement is in `docs/superpowers/plans/2026-09-08-delta-ranger-production-runtime.md:326`.

## Existing gaps, with source anchors

Paths below are relative to `spark-connector/v3.5/spark-delta-ranger/src/main/`.

* `scala/.../DeltaRangerReadRule.scala:48`: a root authorization tag matching only
  `(user, groups)` returns immediately. Policy/role versions are sampled around a
  fresh rewrite but discarded. `check:114` and `prepare:125` check the same identity
  marker; neither checks current policies or registration. A plan already analyzed
  and physically prepared can execute without any of these rules running again.
* `ReadRule.scala:151,205`: canonical extraction retains catalog/database/table and
  owner, plus native Delta scan identity. It does not retain the registration
  id/generation/token or native UUID. Native load validation in the base optional
  connector, `spark-delta/.../DeltaTableIdentity.java:77`, occurs at load; its
  descriptor attachment at `:36` does not preserve the receipt incarnation.
* `java/.../DeltaRangerPolicyEngine.java:178,234,328,428,465`: readiness validates
  current service/definition/metalake/policy/role state. Individual evaluations
  reject a changing completed policy context. The public version tuple lacks a
  local publication epoch; the read rule compares only numeric policy/role values
  across its multi-evaluation rewrite. A same-version context replacement is not
  represented by that tuple. Role publication stability deserves an independent
  before/after check, including role-only refreshes.
* `DeltaRangerMaskRules.scala` authorizes original extracted columns before rewriting,
  applies row filtering over original scan attributes, and masks with new ExprIds.
  Preserve that order and the extraction-only opaque identity map. Re-extracting
  privileges from the rewritten plan could lose original predicate/join columns.
* `DeltaRangerPlanAdmission.scala:59,66,107` has a separate typed native metadata
  route. Snapshot aggregation relies on thread-local scope and registered native
  RDD/output lineage. A new physical guard must preserve this route without turning
  a missing user authorization tag into permission to read arbitrary files.

## Spark seam and its limitations

In both pinned Spark versions, `SparkPlan.execute()` calls `doExecute()` on every
invocation. `prepare()` is final, recursively prepares children first, and runs
`doPrepare()` only once. Therefore a doPrepare-only check fails repeated actions;
a conventional unary guard's doExecute check can occur after child preparation
has started broadcast work. See pinned [3.5.4 SparkPlan](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala)
and [3.5.6 SparkPlan](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala),
methods `execute`, `executeQuery`, and `prepare`.

The most suitable public injection point to prototype is
`SparkSessionExtensions.injectColumnar`, using
`ColumnarRule.postColumnarTransitions` to install a physical execution envelope.
Analysis/check/pre-CBO hooks remain useful early checks; they are not per-action
hooks. AQE-specific post-planner/runtime rules alone do not cover AQE-disabled
execution. Public injection does not imply stable SparkPlan ABI; qualify both
versions. See [3.5.6 extensions](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/SparkSessionExtensions.scala)
and [ColumnarRule/application order](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/Columnar.scala).
The 3.5.4 extensions source fetch was unavailable; its API compatibility still
needs direct compile/runtime proof, rather than treating the 3.5.6 fetch as proof.

Prototype an opaque `LeafExecNode` envelope holding the already prepared delegate,
with no ordinary children for Spark to prepare before validation. Each execution
entry validates on the driver before delegating. Preserve output, partitioning,
ordering, row/columnar behavior and cleanup/metrics ownership; do not implement
codegen passthrough that removes the guard. Hiding the delegate from subsequent
codegen traversal can cost performance and requires explicit tests. This envelope
is an implementation proposal, not a proven drop-in operator.

AQE invokes columnar rules during stage construction and requires shuffle/broadcast
roots to remain exchange nodes. Blindly wrapping every callback root is invalid.
Use a positively identified outer query boundary and preserve stage roots, or
make AQE-enabled user SELECT explicitly unsupported in the initial slice. Do not
silently disable AQE and report its coverage. An outer guard must remain effective
after final-stage reuse. See [3.5.6 AdaptiveSparkPlanExec](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala),
`newQueryStage`, `executeCollect`, and `doExecute`.

`QueryExecution` retains analyzed, optimized and executed plans, and retains `toRdd`
as a lazy value. Its cache substitution precedes optimization. A physical guard
therefore covers repeated plan entry, not subsequent actions on an RDD already
returned from that plan. See [3.5.4 QueryExecution](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala)
and [3.5.6 QueryExecution](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala).
`Dataset.rdd` is itself lazy and creates a deserialized query; keep that shape
rejected for user SELECT. Direct `queryExecution.toRdd`, arbitrary cached RDDs,
retained iterators, and custom driver code require the trusted launcher boundary;
do not advertise their revocation enforcement. See [3.5.6 Dataset](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/core/src/main/scala/org/apache/spark/sql/Dataset.scala),
`rddQueryExecution`, `rdd`, and `javaToPython`.

## Minimal owned implementation

All changes can remain in the optional Ranger connector; no core/API/server/client
or storage-schema source changes are needed.

1. **Engine snapshot stamp — `DeltaRangerPolicyEngine.java`.** Add an immutable,
   driver-local stamp containing trusted service/definition/metalake binding,
   owner/lease identity, validated completed policy-context publication identity
   (or monotonic epoch), policy version, and independently validated role generation.
   No live Ranger object or credential should be serialized to tasks. Sample the
   full stamp before and after the complete original-access/mask/filter decision
   batch. Reject incomplete publication, lease closure, or any change. Do not assume
   an owner synchronized block stops Ranger downloader publication.
2. **Registration stamp — `DeltaRangerCatalogSpark35.java` plus a new small
   `DeltaRangerRegistrationBinding` helper.** Through the existing distinct metadata
   supplier and ordinary named `loadTable`, decode a PUBLISHED
   `DeltaRegistrationReceipt`. Bind catalog/database/table, registration identity,
   normalized registered location, native UUID, and admitted native snapshot/schema.
   Capture the receipt around native `super.loadTable(id)` and reject drift. Preserve
   native DeltaTableV2, associating the receipt with its actual descriptor/scan via
   an owned immutable binding; do not reconstruct authority from a path lookup.
   If descriptor propagation needs an opaque ticket, retain its full binding in the
   driver and validate provenance, never treat ticket possession as authorization.
   Do not expose operation tokens in plan display. At execution perform a fresh
   scoped metadata read and compare incarnation/state and native scan UUID. Receipt
   native-version advancement alone is not a new registration incarnation; schema
   drift against a retained plan must reject until explicitly qualified.
3. **Decision receipt — `DeltaRangerReadRule.scala`.** Retain exact original resource
   and column sets per scan, subject/groups, engine stamp(s), registration stamp(s),
   and resolved policy decision identity. The analyzer tag remains only fixed-point
   bookkeeping. Tag copies, logicalLink propagation, optimizer clones, and joins
   must carry enough owned provenance to validate all scans; missing/ambiguous
   provenance rejects. Revalidate before native scan preparation and at execution.
   Reauthorize original accesses at entry. If time-sensitive policy evaluation can
   change masks/filters without a generation change, compare current decisions to
   the retained ones or explicitly reject such policy features; equality of version
   numbers alone does not freeze policy time conditions.
4. **Physical guard — new `DeltaRangerExecutionRule.scala` and
   `DeltaRangerGuardExec.scala`, wired by `DeltaRangerSparkExtension.scala`.** Install
   the envelope once at a qualified boundary, validate before delegate preparation
   or execution, and never unwrap/rewrite stale plans. Check all join scans before
   any child starts. The guard should hold a driver-side validator, not captured
   metadata credentials. Sanitize stale-plan diagnostics.
5. **Cache/native admission — `DeltaRangerPlanAdmission.scala` and pre-CBO guard.**
   Explicitly reject user `InMemoryRelation`/`InMemoryTableScanExec`, including nested
   substitutions in fresh SELECT, before data access. Native Delta metadata caches
   are a distinct typed route, not a blanket exemption for cache nodes. Preserve
   exact log-file-index and snapshot lineage admission; test physical planning both
   inside and after metadata scopes. Any carried native capability must remain
   bound to its owning snapshot/operation and cannot authorize user data scans.

Spark SQL cache matching uses `sameResult` and substitutes an `InMemoryRelation`
with adjusted output. A subject tag is not a sufficient cache partition key. Fully
supporting cache reuse later requires producer provenance bound to subject, policy,
roles and incarnation, plus safe invalidation; merely authorizing a fresh consumer
does not establish that cached rows have its masks/filters. See [3.5.4 CacheManager](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/CacheManager.scala),
`lookupCachedData` and `useCachedData`.

## Meaningful RED cases and acceptance split

Add `TestDeltaRangerExecutionRule`/`TestDeltaRangerGuardExec`, extend engine/read-rule
and native snapshot admission tests, and add a separate retained-plan downloaded
query harness. Do not relabel the existing fresh-query harness as reuse evidence:
`DeltaRangerDownloadedQuery.java:140` refreshes and waits, then constructs a new
`spark.sql(...):149` for each command.

| Case | Required observation |
| --- | --- |
| Same JVM, fresh SELECT after grant/revoke | Real downloaded policy versions advance; new query changes allow/deny; same process/service/subject recorded. |
| Same JVM, role-only membership refresh | Role version advances independently; fresh query changes; policy-only waiting cannot satisfy the case. |
| Analyze, then revoke, then first collect | Original Dataset is retained; no new SQL parsing; execution rejects without reading user rows. |
| Force optimizedPlan/executedPlan, then revoke | Each retained stage rejects; analysis/pre-CBO checks alone are insufficient. |
| Collect successfully, revoke, collect same Dataset/physical plan again | Second action rejects; detects once-only doPrepare checks. |
| Change mask or row filter with SELECT still granted | Old analyzed/optimized/physical plans reject; fresh SELECT produces exact new masked/filtered rows. Use a predicate and a two-catalog join to retain original column requirements. |
| UGI A to B; same short name with changed actual groups | Old plan rejects before native/child execution; metadata identity never becomes subject. |
| Same numeric versions, changed policy context; role-only publication race | Old receipt rejects, and mixed decision batches cannot be stamped valid. |
| Pending replacement, published replacement with new generation/UUID, missing registration | Retained plan rejects even with unchanged Ranger versions; fresh named read follows current published incarnation only. |
| Parent join deny with broadcast-capable child | Instrument child doPrepare/doExecute; neither begins before guard validation. |
| Cache/persist, cached temp view, nested cache hit, fresh consumer with different UGI | User cache consumption rejects explicitly; no generic missing-tag or cache fallback. Qualify prevention even when outer root tag survives. |
| AQE off/on, row/columnar, repeated physical collect/take | Each supported mode checks every entry; unsupported AQE rejects clearly. No invalid wrapping of exchange roots. |
| Native snapshot reconstruction and stats, including cached native state | Existing typed native route still works; copied native marker, ordinary JSON/Parquet, and raw Delta path remain denied. |
| Metadata lease expiry, engine close, invalid refreshed service binding | Retained plan fails closed; no stale success based solely on old receipt. |

For every stale-plan test include an unchanged-state control showing repeated
execution works, and assert the denial originates at the intended guard rather
than an unrelated unsupported shape. No-op plan changes alone are not meaningful
RED coverage. Use deterministic publication barriers for races, not sleeps.

Local tests establish mechanics. Final acceptance separately requires real Ranger
2.8.0 policy/role downloads and exact row/denial results on all four pinned pairs.
Record before/after engine stamps and public Admin/callback readback separately.
Revocation takes effect for a new execution after the updated snapshot is observed;
this design does not cancel a query already running across refresh or provide a
registration read/write fence. Task 4 and broader Delta conformance remain open.
