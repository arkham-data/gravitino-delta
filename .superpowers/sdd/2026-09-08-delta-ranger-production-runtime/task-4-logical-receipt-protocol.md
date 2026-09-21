# Original decision receipt protocol for retained SELECT

Design input for the forthcoming production integration, based on f1bf2f193.
This is not installed or qualified. Complete engine stamp implementation and
registration/native provenance designs are independent prerequisites; exact APIs
will be reconciled before handing a single Scala integration owner the work.

The receipt must retain the authenticated short user and immutable actual groups;
every original scan occurrence with its canonical catalog/database/table/owner,
original column set, original access resources and exact registration binding;
one complete stamp for every owning engine lease; and the exact table filter and
per-original-column mask decisions used for rewriting. A join covers all scans,
including same-name tables and repeated aliases. Do not deduplicate registrations
or privileges using unqualified table names or paths. MaskDecision equality is
semantic kind plus optional expression, including NONE and absent filter.

Analysis protocol:

1. Preserve the existing finite shape and exact original-column extraction. Capture
   subject and all complete engine stamps, and resolve every scan through its
   catalog-owned registration binding. A copied descriptor/index must not become
   registration authority merely because fields compare equal.
2. Authorize the complete original resource inventory for all scans before any
   rewriting decision is used. Obtain row-filter and mask decisions once, then
   build the actual rewrite from those captured values. Refactor MaskRules to
   accept captured decisions; do not capture one decision and rewrite from a
   later independent evaluation. Keep existing type/expression/scope checks.
3. Revalidate registrations, subject and all stamps after the batch. Metadata I/O
   must precede the final engine/subject check. Attach an opaque driver-owned
   receipt to the admitted rewritten root. No operation token, mutable Ranger
   object or complete receipt payload belongs in plan text/JSON/task closures.
4. Before PrepareDeltaScan, require the receipt's current validity; user cache
   substitution or missing/mixed provenance rejects. Native metadata gets its
   independent typed admission, never permission by absence of a user receipt.

Repeated supported action protocol:

1. At the visible unary root's executeCollect entry, validate current subject,
   complete engine/role publication and every original registration binding.
   A changed generation or same-number publication replacement requires a newly
   analyzed SELECT; the retained plan is not silently rewritten at execution.
2. Reauthorize all original resources before reevaluating every retained mask and
   row-filter decision. Compare semantic outcomes, including absence. This detects
   changed time-dependent decisions even if generation values agree. Evaluation
   must not run the retained physical child or rewrite the retained plan.
3. Revalidate registrations and finish by checking all engine/role stamps and
   current subject after the metadata/evaluation work. Reject observed drift before
   delegating collection to the child. Updates after the admitted execution
   boundary do not cancel an already-started action. This is not a distributed
   metadata/policy transaction or instantaneous revocation guarantee.

The first action contract is Dataset.collect and collectAsList with explicitly
admitted ordinary SELECT shapes. Raw physical execute/prepare, retained RDDs and
iterators are trusted-launcher boundaries. AQE and user caches require explicit
rejection until their own implementation/qualification, without silently changing
session configuration. Keep native Delta metadata caches on the typed route.

Required vertical tests must retain a real named native Dataset, force analysis /
optimization / physical planning and execute successfully, then revoke or change
publication and reuse it. Stale action must fail at the guard before instrumented
child preparation/execution; unchanged control must still return exact rows.
Cover access revoke, role-only and same-version publication, subject/groups,
changed mask/filter while access stays allowed, registration replacement/pending/
missing/schema drift, metadata lease expiry, engine closure, two-catalog joins,
cache substitution, unsupported AQE and missing/copied/mixed native provenance.
Native row/vectorized scans and a real broadcast exchange need explicit positive
and stale negatives. Existing 48 fresh same-JVM checks and mechanics prototype are
separate evidence; they cannot satisfy these production tests.

Open reconciliation points: authoritative publication/happens-before proof for the
Ranger role stamp; exact weak-identity descriptor/base-relation propagation on both
Delta patches; native provisional analysis tickets and final physical-plan binding;
receipt cloning/serialization/plan-display behavior; whether batch evaluation needs
an explicit common access-time sample for time-sensitive policy semantics.

## Driver context copy, rendering and canonicalization follow-up

Root checked pinned official Spark 3.5.4 source: `TreeNode.jsonFields` uses both `productIterator` and `otherCopyArgs`; a second constructor argument list is therefore insufficient by itself to omit a guard context from JSON. Use an ordinary non-Product opaque context and explicitly sanitize/omit its JSON field, with actual explain/JSON tests. Clone support still needs `otherCopyArgs` and the transient driver field. Source: [TreeNode](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala).

`QueryPlan.isCanonicalizedPlan` is protected and accessible to the guard subclass. Check it at the beginning of overridden collection entry, before parent collection/child work. Spark's own execute-time check happens inside `executeQuery`, after preparation, so it cannot establish the required collection pre-prepare guarantee. Sources: [QueryPlan](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/QueryPlan.scala), [SparkPlan](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala). These are design constraints; no production guard or serialization/canonicalization test is installed by this note.
