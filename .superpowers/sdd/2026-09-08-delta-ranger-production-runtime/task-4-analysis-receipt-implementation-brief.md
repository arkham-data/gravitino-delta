# Task 4: consume exact bindings in production SELECT analysis

Execute the accepted logical-receipt protocol against the reviewed registration,
engine and metadata foundations. This is production analysis integration, not a
new mechanics prototype. It does not install physical admission or claim retained
collection enforcement. Native operation ownership and physical installation follow.

## Ownership and interfaces

The Scala integration owner owns ReadRule, MaskRules, SparkExtension, a new
package-local decision receipt / inert driver witness reference, necessary narrow
RegistrationBindings accessors/publication recheck, and focused existing/new
native analysis tests and their fixture migration. No core/base catalog changes.
The independent engine-time owner owns only DeltaRangerPolicyEngine.java and its
focused Java tests; coordinate using package-private authorize/mask/rowFilter
(subject, key, java.time.Instant) overloads. Existing public overloads remain.
No simultaneous source publication or Gradle runs: stage privately until root grants
an exclusive slot. Root owns documentation, commits, services and evidence review.

## Required production behavior

- Resolve EVERY original LogicalRelation through its canonical actual wrapper's
  registrationBindings().resolve. The resolver supplies the exact bound scan;
  obtain the owning engine from that binding, avoiding an independently supplied
  mismatched engine. No engine-only production/test fallback, path identity, or
  manufactured binding. The production constructor uses catalogManager normally.
- Retain the original authenticated subject/groups, every original scan occurrence
  and canonical resource/owner/columns, exact bound scan, opaque owning engine
  stamp, and exact optional filter / per-column MaskDecision including NONE.
  Preserve duplicate self-join obligations even if access checks are deduplicated.
- Sample one Instant for the authorization/decision batch across all engines. All
  original access authorization completes before ANY filter/mask request. Capture
  each decision once and rewrite using those captured values only. Keep existing
  expression scope/type validation and Kyuubi marker ordering.
- Revalidate original binding publication values after evaluations using the same
  finite metadata reader. A narrow bound-scan method may re-read canonical metadata
  and require original samePublication + lease/owner liveness. No native current-head
  claim: that future action validator needs an owner-bearing native operation.
  Metadata work precedes final engine stamp and current subject checks. Reject
  observed drift instead of emitting a rewritten plan.
- Replace the subject-only root tag with a plain driver-owned non-Product,
  non-Serializable receipt behind an inert serializable tag reference whose
  authority is transient and whose rendering is constant. Deserialized references
  have no authority and fail closed. Do not leak receipt/engine/principal through
  logical plan tags, JSON, explain or serialization.
- Normal analyzer/check/pre-CBO paths consume this receipt. A copied marker alone
  must not admit a different source, expression tree or mixed/reused child. Bind
  the supported rewritten logical tree to its actual source identities, expressions,
  output attributes and child structure; permit normal Spark clones preserving
  those exact bindings. Do not use plan text, canonicalized/sameResult equality,
  names or tag equality alone. Do not silently skip checks because a subject matches.
- Keep current native admission unchanged in this increment; install no columnar
  rule or guard until typed native operation/tickets are integrated. Do not disable
  AQE/caching configuration to manufacture positive results or widen finite SELECT
  shapes. Record any migration issue instead of adding a bypass.

## Tests and evidence

Use real native Delta named tables with real wrapper/reader/registry bindings and
finite metadata leases. Inject only metadata transport and the existing Ranger
backend fixtures below their real validation boundaries. Keep extraction-only
fixtures separate when they intentionally do not install production rules.

RED/GREEN must prove canonical registration owner consumption, same-path foreign
binding/copied descriptor rejection, all-original-access-before-rewrite ordering,
exact one filter/mask capture, same Instant across requests, opaque same-number
publication drift, subject/group changes, late metadata/lease/close drift, two
catalogs and self-join inventories, normal Spark clone controls and copied/changed
receipt negatives, constant rendering and inert serialization. Existing native
fresh SELECT/filter/mask/quoted mapping controls must continue returning exact rows.
Do not count generic unsupported-shape errors as intended authority failures.

Capture original RED and final GREEN with the standard run-gradle.py, Test.doFirst
init and unique output property, offline/plain/max-workers2/no-parallel/2GiB plus
ActiveProcessorCount2. First full focused regression pair 3.5.4/3.3.2 includes
Spotless/Javadoc; run affected real-native tests on all four Spark3.5.4/3.5.6 x
Delta3.3.0/3.3.2 pairs with Ranger2.8.0. Freeze exact source hashes and compiled
inputs, report honest limits, then independent cross-review. No live services,
Docker, policy writes or deployment in this slice.

## Optimizer boundary reconciliation (September 23)

Read task-4-analysis-optimizer-reconciliation.md for the concrete source-backed
route and proof obligations. Prefer an ordinary Ranger-package SparkOptimizer
subclass replaying only the qualified prefix before Pre CBO Rules on the retained
approved clone, under a private SQLConf snapshot, then direct exact comparison of
the incoming candidate. Never optimize the candidate into acceptance. Compile the
public/protected hook on both Spark patches before deeper integration; retain no
private-package bridge or reflection. The reconciliation is not runtime evidence.

Qualify the actual prefix and native Delta RangePartitionIdRewrite; unknown rules
must reject before invocation. Preserve original obligations across pruning and
empty-result rewrites. Handle clock-dependent datetime casts, row-tracking
normalization, user cache substitution and statistics-producing star-schema
reordering explicitly; no silent configuration changes or source-identity
relaxation. Temporary unsupported combinations remain gaps toward full Delta
conformance, not a redefinition of the final objective. Record proof and failure
boundaries precisely. The complete original source/binding and mask/filter
inventory remains authoritative even if optimizer removes a scan.
