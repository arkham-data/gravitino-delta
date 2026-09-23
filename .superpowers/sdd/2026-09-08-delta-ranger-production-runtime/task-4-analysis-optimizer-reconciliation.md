# Task 4: reconcile the analysis receipt with the optimizer prefix

Recommendation: retain an exact analyzed binding in the driver receipt and derive
one expected pre-CBO binding by replaying only Spark's qualified optimizer prefix
on a detached copy of that approved tree. Compare the incoming pre-CBO tree
directly with the expected result. Never optimize the incoming candidate to make
it match. This is a concrete implementation route, not runtime qualification;
no Scala compilation, tests, services, or production code changes were performed
for this note.

## Public/protected source hooks

`SparkOptimizer` is publicly subclassable. Its `defaultBatches` is public and
overridable. `Optimizer.batches` is public and final, applying configured rule
exclusions to `defaultBatches`. `RuleExecutor.Batch` is `protected[catalyst]`,
which also admits subclass access: declare the subclass inside the Ranger
package, keep all `Batch` manipulation inside it, and expose only an
`expectedPlan(LogicalPlan): LogicalPlan` method. No Spark-package bridge or
reflection is needed. This access path still needs a compile check on both
supported patches. See [SparkOptimizer 3.5.4](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala),
[RuleExecutor 3.5.4](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules/RuleExecutor.scala),
and [Optimizer 3.5.6](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala).

The subclass should obtain `super.defaultBatches`, require exactly one
`Pre CBO Rules` boundary, then return its prefix. Call inherited `execute`, so
the native fixed-point strategies and exclusion filtering remain in force.
Do not override `batches` or manually flatten rule execution. Construct with
the real session's catalog manager/catalog and experimental methods; the user
optimizer batch is after the boundary and must never execute in replay.

Spark's session builder appends injected optimizer rules to
`extendedOperatorOptimizationRules`; these run twice before pre-CBO. Therefore
plain `new SparkOptimizer` is not automatically the session's actual optimizer.
See [BaseSessionStateBuilder](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala).

## Next owner's implementation recipe

1. Capture the rewritten analyzed tree after Kyuubi stage 1 and marker removal,
   its immutable exact binding, every original scan occurrence and decision, and
   the active session identity. Analyzer reentry/check requires that exact
   analyzed binding. A matching subject or copied receipt reference alone never
   bypasses comparison. Keep decisions immutable and never request them again
   during replay.
2. Capture a private `SQLConf` clone and its full `getAllConfs` map once analyzer
   initialization has settled. Conservatively require identical configuration
   at subsequent receipt consumption; do not print/store that map outside the
   transient authority. Execute the detached prefix under public
   `SQLConf.withExistingConf(snapshot)`. Check live configuration before and
   after replay, including optimizer exclusions/iterations, constraint
   propagation, IN conversion, ANSI, time zone and cast settings. Full-map
   equality initially avoids an incomplete hand-maintained dependency list.
   These checks detect observed drift, not concurrent change-and-restore.
   [SQLConf source](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
   exposes both scoped configuration and cloning.
3. Qualify the actual prefix rather than trusting `spark.sql.extensions` text.
   Inside the subclass, inspect the actual optimizer's public batch/rule
   descriptions and require the supported stock prefix plus the exact native
   Delta `RangePartitionIdRewrite` class. Compare ordered class identities and
   strategies with the qualified replay prefix; names identify the boundary,
   not authority. Reject unknown pre-optimization batches or injected rules
   before invoking any of them. A bounded typed manifest is preferable to
   executing arbitrary extensions to discover whether they change this tree.
   Mirror only the qualified Delta rule into replay; its exact expression
   trigger is absent from the finite grammar. Require the Ranger pre-CBO
   consumer to precede native Delta preparation and other transforming custom
   pre-CBO rules. Reject optimizer exclusions that remove that consumer.
4. At pre-CBO, build the expected result from the retained approved clone under
   the captured configuration. Strip inert authorization references from the
   detached tree, retain the exact native source objects, and avoid any
   `QueryExecution`, cache substitution, native scan preparation, physical
   planning or collection. Compare the incoming tree directly against this
   expected binding. Preserve original authorization obligations even if a
   native optimization removes a scan or a projected column. Revalidate the
   original metadata publications and engine/subject state in the already
   specified order before proceeding to existing native preparation.
5. Use a closed structural comparator, not `sameResult`, canonicalization,
   plan strings, or bare case-class equality. Include exact node classes,
   child order, join kind/hints, expressions and ordered output attributes.
   Explicitly include ExprId (including JVM UUID), qualifiers, names, type,
   nullability, metadata, alias secondary fields, literal values, cast zone/
   evaluation mode and arithmetic mode. Resolve every attribute to its bound
   producer. For each surviving leaf require its original occurrence's exact
   `HadoopFsRelation`, file index, file format and native owner identities,
   descriptor values and complete output binding. Logical plan clones may
   differ by wrapper object identity; native source objects may not. Copy
   mutable literal payloads in the retained binding.

The current analyzed grammar is unchanged. The expected optimizer result can
contain an `InSet`, a conditionless inner join, or an empty `LocalRelation`
derived from a false filter. Support such results only through exact comparison
with the trusted replay result, never as independently authorized fresh inputs.
Keep all original receipt obligations for the empty-result case. Exact source
identity is required for every source that remains; removed sources remain in
the original obligation inventory.

## Limits requiring explicit handling

- **Normalization/cache:** Spark applies normalization and cache substitution
  before the optimizer. Pinned Delta `GenerateRowIDs` can replace the base
  relation/file format when row tracking is enabled, even without selecting a
  metadata field. Treat this as an explicit unqualified feature in the current
  finite surface; do not relax native identity to accommodate it. Cache
  substitution produces a different source and must fail the receipt comparison.
  Keep caching and AQE configuration unchanged. Full-feature conformance remains
  future required work. See [QueryExecution](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala)
  and pinned `task-3-evidence/pinned/org/apache/spark/sql/delta/GenerateRowIDs.scala`.
- **Clock-dependent casts:** Determinism flags alone are insufficient.
  `SpecialDatetimeValues` evaluates foldable string-to-date/timestamp casts;
  strings such as `now` or `today` can depend on replay time. Reject the
  context-dependent subset explicitly, including strings computed by allowed
  constant expressions. Preserve ordinary numeric/string casts, arithmetic,
  and non-special dated literals. Unsupported UDFs, subqueries, current-time
  expressions and range-partition expressions remain excluded. Use exact
  expression classes so an untrusted subclass cannot supply evaluation code.
  See [finishAnalysis](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/finishAnalysis.scala).
- **Statistics/side effects:** Pre-CBO does not mean universally statistics-free.
  `ReorderJoin` invokes star-schema detection for more than two join inputs
  when enabled and CBO is disabled. Leave configurations unchanged; reject
  that unqualified combination for affected plans until its statistics inputs
  and native metadata interactions are qualified. Normal two-way/self-joins
  retain their existing route. Replay also updates Spark optimizer metrics,
  invokes ordinary plan logging, and can initialize catalog context through
  finish-analysis rules. Do not claim it is side-effect-free.
  See [ReorderJoin](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala).
- **IDs and stability:** Existing admitted projections/filters/joins should
  preserve bound IDs through pruning, collapsing and alias removal, but that
  is a hypothesis to test on both patches. Never erase or renumber ExprIds to
  mask a discrepancy. Likewise, fail a configuration or prefix mismatch rather
  than retrying against another configuration or weakening equality.

## Required proof before acceptance

Compile the ordinary Ranger-package subclass on Spark 3.5.4 and 3.5.6 first.
Then preserve real-native plain SELECT, row filter, mask, quoted mapping,
two-catalog and self-join exact-row controls on all four supported Spark/Delta
pairs. Add clone positives and copied-receipt negatives for changed literals,
mask/filter removal, alias/ExprId/output changes, same-path foreign sources,
mixed children, and reused occurrences. Exercise IN-to-InSet, constant folding,
alias elimination, filter pushdown, pruning and empty-result rewrites. Verify
config drift, unknown extension rejection before invocation, special datetime
rejection, inactive RangePartitionIdRewrite, row-tracking limitation and cache
substitution rejection with their specific failures. Verify no native scan
preparation or jobs occur during replay. These are proof obligations, not tests
reported as run by this source-only review.

## Root compile check, September 23

A private Ranger-package subclass compiled successfully on Spark 3.5.4 and 3.5.6,
using the captured Delta 3.3.2 classpaths, Scala compiler 2.12.21 and JDK17. It
accesses the actual optimizer batch descriptions, overrides defaultBatches,
instantiates typed RangePartitionIdRewrite, clones SQLConf, and calls scoped
execute on a cloned plan. Evidence: task-4-evidence/optimizer-public-seam-compile-20260923.
Both compiler exits are zero and generated class files are preserved. This closes
the language/API visibility question only; the subclass was not executed and
none of the runtime proof obligations above are satisfied by compilation.
