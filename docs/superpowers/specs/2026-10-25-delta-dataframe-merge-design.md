# Named DataFrame and PySpark MERGE API

Status: approved by the user's “Proceed” on 2026-10-25. Implementation and exact
qualification scope are tracked in the linked implementation plan; design approval
does not establish full API support. Baseline: `09e092b2f937b1db17f992aedbeb22763162e527`. The user promoted this
work to critical priority before catalog-demo integration. Integration has its own
plan; broader Spark execution may be an additional prerequisite.

Independent builder check on 2026-10-25 found no important unanswered planning
questions. This is design review only, not installed implementation evidence.

## Outcome and constraints

Let ELT applications pass an actual bounded, governed CDF DataFrame into a named
Delta MERGE through classic PySpark and a companion JVM API. Preserve the qualified
SQL behavior, including partitioned targets and same-table writeback. Gravitino
continues to own registration, Ranger governs access/filters/masks, and native
Delta performs row reconstruction and transaction actions.

Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17 and Scala 2.12.21.
Do not modify Gravitino core/API/server/clients, introduce another metastore, fork
Spark/Delta, or fall back to paths. No source-payload collect, toPandas,
createDataFrame reconstruction or temporary-view SQL bridge. Native metadata and
command metrics collection remain governed by the existing contract.

## Approaches and decision history

The user already selected a companion DataFrame builder as a separate follow-up
after named SQL. The retained Task 1 probes show why stock DeltaTable.forName is
not a viable frontend on these pins: three-part names take a V1 parsing/catalog
route, and native builder preprocessing loses the distinction between omitted
assignments and explicit target self-assignments.

The proposed implementation constructs a complete Spark MergeIntoTable from the
actual source plan and authored clauses, then enters the existing analyzer and
owned MERGE command. It preserves expression identity and shares SQL execution.
Generating SQL through temporary views would introduce naming/lifetime state and
cannot preserve arbitrary resolved Column identity. Direct use of Delta's native
builder would repeat the proven assignment-loss problem. Neither is selected.

The public names, Column support and packaging below were engineering proposals
accepted with this design approval.

## Public surface

Provide `GravitinoDeltaTable.forName(spark, name)` in the Spark Ranger companion,
with a classic Python facade imported as `gravitino_delta.GravitinoDeltaTable`.
This is a companion API, not a replacement or monkey patch of `delta.tables`.
The initial API includes:

- `forName(spark, name)` with a canonical three-part Gravitino table identifier.
- `alias(name)` for the target occurrence.
- `merge(source, condition)` accepting an actual same-session batch DataFrame.
- Ordered `whenMatchedUpdate(condition=None, set=...)`,
  `whenMatchedDelete(condition=None)`, and
  `whenNotMatchedInsert(condition=None, values=...)` clauses.
- `whenMatchedUpdateAll(condition=None)` and
  `whenNotMatchedInsertAll(condition=None)` with existing SQL star semantics.
- `execute()`, synchronous, returning `None` in Python and `Unit` on the JVM only
  after native completion and registration reconciliation. No application
  checkpoint is advanced by the library.

Python predicates and assignment values accept expression strings or PySpark
Columns, including mixed values in an assignment mapping. JVM overloads accept
Spark Columns and strings; Java collection adapters keep the Python gateway
simple. Assignment keys are top-level target column identifiers, parsed using
Spark's identifier rules; nested assignments and duplicate keys under the active
resolver reject. Literal strings as values must use `lit` or a quoted expression,
matching the distinction between a SQL expression and a literal value.

Builders are mutable fluent objects for one linear construction and one execute
attempt, not thread-safe or reusable execution handles. Clause order is retained;
terminal execution seals the builder even on failure. Validate simple argument
types before mutation and defensively copy assignment maps when adding a clause.
A new attempt requires a fresh builder and fresh CDF source.
This does not disable native retries inside the owned command or authorized
known-commit reconciliation with the existing transaction identity.

Example of the approved interface (not executable support yet):

```python
from pyspark.sql import functions as F
from gravitino_delta import GravitinoDeltaTable

changes = (
    spark.read.option("readChangeFeed", "true")
    .option("startingVersion", 12)
    .option("endingVersion", 15)
    .table("lake.db.source")
    .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
    .select("id", "value", "part", "_change_type")
    .alias("s")
)

(
    GravitinoDeltaTable.forName(spark, "lake.db.target").alias("t")
    .merge(changes, F.col("t.id") == changes["id"])
    .whenMatchedDelete(condition=F.col("s._change_type") == F.lit("delete"))
    .whenMatchedUpdate(set={"value": F.col("s.value"), "part": "s.part"})
    .whenNotMatchedInsert(
        condition="s._change_type <> 'delete'",
        values={"id": "s.id", "value": "s.value", "part": "s.part"},
    )
    .execute()
)
# Advance the application's durable checkpoint only after execute succeeds.
```

This is an explicit consumer program. Discarding preimages does not implement
complete replication across row-filter visibility changes. Masked keys may
collide; the library neither recovers raw keys nor deduplicates the source.

## Logical-plan and expression boundary

Retain the source's exact analyzed plan and owning SparkSession. The existing
CDF reader already records pending observations for projections, filters and
aliases. Do not request its optimized/executed plan to assemble a MERGE, because
that can consume a CDF observation and issue read execution authority.

Build the target as an unresolved canonical named relation and optional alias
inside the complete MergeIntoTable. Do not first call spark.table on the target:
ordinary read analysis can issue a filtered/masked read, while MERGE must retain
the complete native target for hidden-row and unchanged-column preservation.

Parse string expressions with the owning session's SQL parser. Pass Column
expression trees through without stringifying and reparsing them. Bind unresolved
references against the two operands using native resolution. Accept resolved
source attributes only when bound to the retained source occurrence; reject
foreign or stale expression references, even where names and types match. Target
references use unresolved target-qualified expressions; exposing a target toDF
read handle is outside this milestone. Ambiguity, missing attributes, foreign
sessions, streaming frames and unsupported expressions reject before native writes.

Preserve authored assignment sets and ordered clauses before native preprocessing.
Construct native SQL-equivalent star actions without pre-filling omitted fields.
Enter DeltaRangerReadRule and DeltaRangerMergeCommand.resolve through normal Spark
analysis. The existing DeltaAnalysis/receipt issuance/PreprocessTableMerge path
must issue authority for exactly its own successor, as it does for SQL. A public
builder is not itself an execution permit; it cannot manufacture a receipt or
authorize a copied native command.

The first installed proof must demonstrate this path, including resolved source
Columns, both Delta pins and source observation lifetime. If normal analysis does
not preserve these identities, report the precise failed boundary and revise the
design before widening plan admission. Do not infer ownership from expression
text, matching schemas, native helper ordering or arbitrary logical-plan tags.

## Policies, lifecycle and supported scope

Reuse the current SQL receipt, native source materialization, retry ownership,
commit validation, journal and registration publication. Python sends its actual
JVM DataFrame and expressions using the same session; it does not carry an
independent policy decision. Apply current authenticated query identity at
execution and retain existing identity/policy revalidation across native phases.
Construction grants no permission; a revoked grant cannot survive in a builder.

The factory captures the exact session, authenticated subject and SQL settings;
execution rejects a changed subject/session/settings rather than transferring the
handle. Set transaction identity and other required configuration before creating
the factory handle or CDF source. Target registration resolves freshly inside the
command at execute; the name-only handle does not promise a frozen target snapshot.
The CDF source retains its existing analyzed registration/interval/policy
observations, which must still validate. A policy refresh cannot turn those
observations into permission to reuse changed masks or a stale source.

Source SELECT/filter/mask ordering and target read semantics remain unchanged.
Omitted columns retain raw stored values, whereas an explicit `secret = t.secret`
stores the governed value. Hidden target rows cannot match or suppress an insert;
hidden/unchanged file rows retain raw values during reconstruction. No postimage
write constraint, unmasked-key requirement or new Ranger access type is added.
Keep original column demands for unreachable clauses and empty inputs.

Qualify unpartitioned and already-supported partitioned classic targets, CDF output,
distinct destinations and exact same-registration writeback. Retain a fixed source
interval, fixed schema/partition metadata, the admitted scalar expression grammar,
and source Project/Filter/SubqueryAlias composition. Keep current native ambiguity,
constraint, retry, publication and known-commit behavior, including failures that
must not advance the application checkpoint. Builders do not add automatic replay.

Reject already consumed, actioned, cached/checkpointed or foreign CDF sources and
raw-path/unnamed inputs. Merely matching a source schema never confers authority.
Native internally owned source materialization remains permitted. No permission
relaxation is allowed to make a DataFrame frontend pass.

## Packaging and tests

Keep JVM implementation within spark-connector/v3.5/spark-delta-ranger and its
existing runtime bundle. Add the small Python facade under that module's python
directory, packaged as an importable zip for `spark-submit --py-files` (also usable
on PYTHONPATH). No change to the Gravitino Python REST client is required. A wheel
and public package publication are outside this milestone.

The Python module must fail clearly if the companion JVM entry point is absent or
its API version does not match. It must not install dependencies, alter Spark
configuration, choose another session or import Delta's stock builder as fallback.
The acceptance harness binds the Python zip and JVM runtime to the same candidate
source manifest; version compatibility alone is not provenance evidence.

Qualification must cover:

- JVM and real classic Python entry points with strings, Columns and mixed
  assignment values; actual Python execution identity and artifact origins.
- Exact SQL/API row, CDC, native action, registration and failure equivalence.
  Paired equivalent fixtures avoid mutating one target twice as an oracle.
- Explicit versus omitted masked assignments; stars and ordered clauses; string
  escaping/quoted identifiers; foreign session/Column/source and stale builder
  rejection; malformed calls without mutation; duplicate execute rejection.
- Column-only permissions, column deny under a table grant, source/target revoke,
  hidden targets, masked keys/collisions, empty inputs and native ambiguity.
- Partition moves, null/escaped partition values, hidden rows sharing rewritten
  files, exact same-table writeback and source bounds fixed before target mutation.
- Native retry and publication/recovery paths through the frontend, preserving
  existing authorization and checkpoint guarantees. Retain unchanged deep native
  tests where dependencies are unchanged; do not replay every historical matrix.
- Import from the built Python zip with a real spark-submit process. All four
  pinned runtime pairs must pass focused tests and live Ranger 2.8.0/MinIO API
  scenarios. SQL-only successes cannot close this milestone.

Record original process exits, source/artifact hashes, policy evidence and native
state results. Existing failed/historical evidence remains separate. Qualification
is local; separate workers and deployment are not inferred from these results.

## Broader Spark execution and separate integration

DataFrame syntax does not imply arbitrary Spark plan support. AQE, joins,
aggregations, windows, caching and expressions outside the admitted grammar remain
separate work. Streaming, Spark Connect, NOT MATCHED BY SOURCE, schema evolution,
mapping/defaults, deletion vectors and row tracking are not added by this frontend.

There is already a concrete integration mismatch: catalog-demo's
workspaces/spark-engine/src/spark_engine/session.py configures
spark.sql.adaptive.enabled=true, while the current CDF/MERGE code rejects AQE.
Before integration, a separate scope decision must either qualify governed AQE or
explicitly approve a demo profile with AQE disabled. This API must not silently
toggle that setting or present rejection as full Spark execution support.

Determine joins/aggregations/windows/cache requirements from the intended ELT
pipelines before activating integration. Do not label every broader capability a
mandatory prerequisite without that evidence. Required execution expansion gets
its own plan and qualification. No catalog-demo files or services change here.

## Implementation seams and remaining engineering choices

Primary seams: DeltaRangerMergeCommand.resolve, DeltaRangerMergeReceipt.issue,
DeltaRangerCdfRead.Source/pending, and the existing installed MERGE/policy/recovery
tests. Reuse the existing live harness's independent native observer and identity
separation, adding an actual DataFrame Python frontend and artifact binding.

The implementer chooses internal helper factoring, Java-friendly overload shape,
zip task naming and focused test organization. Those choices cannot change the
surface or admitted execution grammar above. An installed identity/lifecycle
failure is a design finding, not permission to bypass the existing receipts.

The [implementation plan](../plans/2026-10-25-delta-dataframe-merge.md) covers the
installed JVM seam, Python facade/package, policy/lifecycle coverage and four-pair
live qualification with independent review. Integration remains in its own plan.
