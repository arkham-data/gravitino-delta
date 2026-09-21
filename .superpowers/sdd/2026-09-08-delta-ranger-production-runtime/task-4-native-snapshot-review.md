# Task 4 intermediate native SnapshotState review

**No severity findings. READY within the frozen native SnapshotState admission scope. This is not Task 4 acceptance.**

Reviewed exact parent `8c383c940b6bbb08f11d009a80c04c8d901d399f` through frozen commit `e1347911c906695a0eedfdfcf6e26d698ce33f6e`. The entire two-file diff was inspected using `git show`/`git diff`: production `DeltaRangerPlanAdmission.scala` and new `TestDeltaRangerNativeSnapshotAdmission.scala`. The dirty test-only recorder, broader coordinator, and live services were excluded.

## Implementation and security checks

- **Registration and initialization** (`DeltaRangerPlanAdmission.scala:66`): registration still requires the active scope, the authorized file-index object, and matching DeltaLog identity. It obtains the actual snapshot's analyzed, nonstreaming SingleAction LogicalRDD before scan generation. Aggregate admission itself performs no snapshot initialization. The existing PrepareDeltaScan caller remains downstream of query authorization. Scope cleanup discards both lineage and aggregate-output records in `finally`.
- **Lineage and aggregate shape** (`:178`): accepted input requires the same underlying RDD object and equality of the complete registered output Attribute sequence. Same-schema replacement RDDs are insufficient. The optional pruning Project must contain exactly the six original attributes in their registered order, with no aliases or computed fields. The aggregate must be ungrouped and contain exactly fourteen uniquely named aliases with the pinned expression for each name. Only the two named histogram outputs additionally accept a null literal of their exact encoder field type.
- **Semantic comparison boundary**: inspected pinned Spark AggregateExpression bytecode. Canonicalization removes generated aggregate result IDs while retaining aggregation mode, distinctness, filter, and canonicalized function. The implementation compares against typed complete aggregate expressions, rather than SQL text or names alone. The generic native node allow-list is unchanged and still excludes Aggregate, so a failed specialized match does not fall through into schema-only aggregate admission.
- **Encoder binding** (`:237`): only an empty, nonstreaming LocalRelation with exactly previously recorded aggregate output attributes can enter the SnapshotState binding path. The deserializer must have the exact SnapshotState ObjectType and contain no Scala/Python UDF or subquery. The expected expression comes from public ExpressionEncoder/SimpleAnalyzer; the actual expression follows SimplifyCasts then BindReferences. Spark's public ReassignLambdaVariableID is applied independently over each whole comparison plan, and final equality is structural. This preserves declaration/use sharing and non-ID constructor, call, type, nullability, and collection details. Prior pinned inspection confirms MapObjects and the map-conversion binders expose their declarations as children; the upstream reassignment rule retains sharing and rejects zero/mixed-sign IDs.
- **Optimizer and user boundary** (`:107`, `:86`): the only added first-row wrapper is integer GlobalLimit(1) over integer LocalLimit(1) over an independently validated aggregate. Other wrappers remain rejected. User query admission remains unchanged, including Aggregate/LIMIT rejection. The existing native Function4 UDF guard remains exactly BooleanType/LongType/LongType/LongType with its prior provenance and argument checks; the earlier accidental five-type regression is absent from the frozen diff.

## Tests and original evidence

Read `task-4-native-snapshot-review-brief.md`, the full new test source, original commands/exits/XML and Javadoc success lines from:

| Evidence directory under `task-4-evidence/` | Actual configured pair | Original exit | XML total |
| --- | --- | --- | --- |
| `native-snapshot-full-330-corrected` | Spark 3.5.4 / Delta 3.3.0 | 0 | 71 tests; 0 failures/errors/skips |
| `native-snapshot-full-332` | Spark 3.5.4 / Delta 3.3.2 | 0 | 71 tests; 0 failures/errors/skips |

Both original commands run the full optional adapter test task plus its Javadoc task. Both logs record Javadoc execution and BUILD SUCCESSFUL. These are two Delta regressions on Spark 3.5.4, not four-pair deployed qualification.

Independently hashed both committed files and compared them against each run's original `source-hashes-before.json`; all four comparisons match exactly:

- Production: `7c8f78ba242be48a371c90eef4f415ad58a585983cf614eec3181792ed60a5e7`.
- New test: `eab6003ab96b0e759ca0ab7119bedfff5ba591f8c79581f2032d27ce09d10d13`.

The new tests check that acquiring native state lineage does not trigger computedState, execute a real unfiltered native projection with installed rules, and verify captured aggregate/encoder admission. Negative cases cover unregistered/expired/foreign/replacement lineage; changed full attributes; grouped/partial/extra/changed aggregates; altered pruning projections and histogram values/types; wrong limit values/types/order/wrappers; nonempty/streaming/read-backed or altered-output bindings; fake ObjectType/UDF/subquery bindings; constructor/static-call/ordinal/collection changes; and lambda ID merge, declaration-only/use-only changes, capture, and name/type/nullability changes. Consistent ID renaming passes. Real user COUNT, GROUP BY, and LIMIT requests remain rejected.

The earlier incomplete iterations described in the brief are not counted as success evidence. The frozen production and test files are bound to the two complete corrected GREEN runs above; no result is inferred from mutable build outputs or the dirty recorder.

## Residual scope

This remains deliberately finite admission for the pinned SingleAction cache and SnapshotState shapes. Nonnull DV-metric/histogram variants, uncached state represented by another logical shape, additional optimizer forms, asynchronous scope propagation, broader Delta operations, and arbitrary Spark jobs/extensions are not qualified by this patch. Unexpected forms fail closed. The exact templates have maintenance cost across upstream versions, but replacing them with broader provenance-based admission is separate work.

New immutable companion/application exports, all four actual Spark/Delta executions, initial authenticated policy/role downloads, live denial/refresh/outage tests, and complete Task 4 acceptance remain separate gates. No new deployed artifact or live outcome is claimed here.

No build, test execution, service action, public mutation, or source edit was performed. This report is the only reviewer output file. Review used frozen source, archived upstream source/bytecode, and retained test evidence; no exact-head GitNexus/PDG index was available or refreshed, so graph/taint coverage is not claimed.
