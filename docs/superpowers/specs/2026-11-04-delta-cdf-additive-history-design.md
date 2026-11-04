# Governed CDF across additive schema history

## Intent and scope

Make the already selected schema-evolution capability usable by incremental ELT: a fresh named bounded CDF read must consume a history spanning the nullable top-level scalar additions now supported by governed APPEND and MERGE. Delta owns the reconstruction and selected read schema; Ranger owns current authorization, row filters and masks. This follows the reviewed additive MERGE checkpoint `8420b96979c528319c33a7a8d71d7946e5d939f0`.

The user selected schema evolution, governed creation, input/transformation support and overwrite/full refresh, in that order, with minimum custom code and Gravitino-owned registration. This is a bounded schema dependency, not the deferred CDF consumer/state or replica-semantics project. No checkpoint-store, delete/preimage consumer, mask-key identity, bootstrap or rebootstrap redesign is included.

## Global constraints

- Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21; actual Ranger client/plugin 2.8.0 in installed tests. The consolidated live gate still requires Ranger server 2.8.0 and MinIO/S3.
- Gravitino owns named registration. Native Delta owns log, schema, files and transactions. No Glue compatibility, second metastore, engine fork, core/API/server/client/storage-schema modification or raw-path governance fallback.
- Current Ranger SELECT, masks and row filters apply to original source demands, including newly selected fields. Preserve Spark-style projection authorization and original operation ownership. No ALTER is required merely to read a historical addition.
- Read APIs and companion API version 1 remain unchanged. Explicit inclusive numeric start/end remain mandatory. Keep the existing bounded CDF source grammar and immutable read-context/settings capture.
- Ordinary CDF validates real native history eagerly. Retained MERGE recovery-only owners remain unable to open, plan or materialize payload. Do not reuse journal proof as historical read authority.
- Keep missing/corrupt history, disabled-CDF gaps, unsupported protocol/features, mapped columns, deletion vectors, row tracking, virtual-column collisions and permissive native bypass settings rejected. Unchanged-schema admission must retain its existing profile, including reader 1/writer <=4 and its existing writer-7 feature set.
- No new commit or source files from reads. No automatic replay of a committed or unknown mutation; preserve MERGE recovery and native outcome semantics.
- Preserve worktree and test evidence. Run sequential bounded tests, retain exact XML/native/runtime/source evidence, reuse unchanged qualified results explicitly and do not replay unrelated historical matrices.

## Approaches and selected direction

1. Keep identical historical schema equality: smallest code, but incremental reads fail after an authorized added column, so it cannot satisfy the selected schema work.
2. Use the existing stock `CDCReaderImpl.changesToDF` and its native compatibility check, with a small finite additive admission check in the existing history owner. This is the selected engineering approach under the user's native/minimal-custom direction. It changes admission, not data reconstruction or policy semantics.
3. Build a schema-normalizing CDF reader or cast/union historical versions ourselves: more custom execution and authority paths, with unnecessary divergence from Delta. Rejected.

Do not call `DeltaRangerAppendSchema.derive` from reads: it validates writer options and has narrower writer protocols. A small pure read predicate is justified; a general schema framework or broad shared writer refactor is not.

## Read contract

Retain the captured current native snapshot as the read schema, as the existing non-column-mapped reader does. Older images expose native nulls for added fields; commit version/timestamp/type and duplicate multiplicity remain native. A bounded interval ending before an addition can use that captured current schema without consuming later rows. Prove this with stock controls for both Delta pins before widening production admission.

For actual schema differences, qualify only forward append-only field sequences: every old StructField remains exactly equal at its ordinal (name, type, nullability and metadata); new top-level fields are nullable concrete scalar types already supported by additive writes (Boolean, integral, floating, decimal, string, binary, date, timestamp; not timestamp-NTZ, NullType, nested additions, defaults, generated or identity fields). Existing nested fields may remain unchanged if existing read admission supports them. Reject duplicate/case-colliding fields, invalid names and virtual collisions. Do not discard metadata to force equality. New scalar fields may preserve ordinary comments/application metadata but not engine-specific default/identity/mapping metadata.

Validate the start snapshot and every Metadata transition inside the requested inclusive interval in native order, and compatibility of these historical schemas with the captured read schema. A drop/re-add or type-change/revert inside the interval cannot masquerade as additive merely because the head schema matches. Identity, partition columns and protocol remain equal to the captured snapshot. Other historical property changes retain existing admission rules; this read change does not newly require all configuration equality.

The interval begins at the start snapshot's schema. No obligation to validate schema transitions preceding that snapshot is added. Do not scan the suffix after the requested end merely to reconstruct schema history: preserve the existing end-boundary and unrelated-missing-log test. The endpoint-to-captured-schema comparison qualifies their observed additive relationship, not an assertion that every unrequested historical transition was additive. Selected rows still stop at the requested end.

Native compatibility remains enabled; no unsafe setting or writer-option dependency. Source metadata/schema drift after receipt capture retains current rejection behavior. Data-only source advancement follows the existing contract. Do not relax raw native scan, copied-plan, cache, receipt or transaction guards to get a positive test through.

## Implementation boundaries

`DeltaRangerCdfHistory` retains completeness, native actions and profile ownership. Prefer a small package-private `DeltaRangerCdfSchema.requireCompatible(previous: StructType, next: StructType): Unit` for the finite predicate if separate tests warrant it; it derives no execution authority. `DeltaRangerCdfNativeReader` continues stock reconstruction. `DeltaRangerCdfReadScans` already checks captured snapshot metadata and native output schema; change it only if a measured native additive recipe requires a finite adjustment, with negative ownership controls.

Use existing registration and Ranger fixtures. Native raw-path operations are test setup/oracles before governed admission only. Product code cannot expose a fallback. A fresh named CDF source spanning the change must also work through the existing SQL and JVM companion MERGE source path, with original demand/policies intact. It is not permission to add general DataFrame source support in this task.

## Acceptance and evidence

- Stock controls: addition inside range, at start, after end; multiple additions; metadata-only/empty intervals; native pre/postimages, inserts/deletes and null older values. Compare exact row multisets/schema, inclusive versions and actual pinned runtime origins.
- Installed named DataFrame and existing SQL CDF frontend: same results as stock for admitted history, partitioned and AQE paths, current registration schema and no mutation. Added-column SELECT denial, projection-only permitted read, mask and row-filter behavior on old null/new values, fresh policy revoke and context drift remain enforced.
- Fresh SQL and companion MERGE consumption spanning an addition; no weakening of fixed/evolving write permissions, settings scope, recovery-only ownership or no-replay behavior.
- Refusals: nested/type/drop/rename/reorder/nullability/engine metadata and changed-then-restored schemas inside interval; CDF gap, missing history, protocol/identity/partition drift and foreign/copy/expired contexts. Unchanged-profile regressions remain valid.
- Freeze exact meaningful method identities; qualify four pinned Spark/Delta pairs. Retain fresh versus reused evidence separately. Public docs state installed-only scope and still-required consolidated Ranger-server 2.8.0/MinIO/PySpark/Spark Hub gate.

## Source basis and open engineering work

Pinned Delta `commands/cdc/CDCReader.scala` and `schema/SchemaUtils.scala` are byte-identical between 3.3.0 and 3.3.2. The native reader chooses a current schema for ordinary non-mapped reads and checks historical compatibility with `SchemaUtils.isReadCompatible`. This is source grounding, not new runtime evidence. Task 1 must confirm actual native behavior and preserve evidence before Task 2 changes admission. If a native control contradicts the finite contract, narrow the documented installed profile with a concrete finding; do not implement a replacement reader.

References: [Delta 3.3.0 CDCReader](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala), [Delta 3.3.2 CDCReader](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala).
