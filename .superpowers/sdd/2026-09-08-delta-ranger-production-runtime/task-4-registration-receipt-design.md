# Task 4: registration receipt to actual native scan binding

Source audit at `f1bf2f193048771fb7e1dce0611daa5c344e9d09`, 2026-09-21. This is a
design artifact, not implementation or qualification. Only this ledger file was
written; no source edits, Gradle, JVM, services, Docker, commits or subagents were
used. No callable claude-mem/mcp-search was available. The accepted boundary is the
review's visible unary guard for repeated `collect`/`collectAsList`, user AQE off.
The engine, logical receipt and physical/native provenance designs are separate.

## Decision and exact existing seam

Implement entirely in `spark-delta-ranger`. Override ordinary named
`DeltaRangerCatalogSpark35.loadTable(Identifier)`, bracket `super.loadTable(id)` with
fresh scoped registration reads, and bind the **actual returned native table's
descriptor and base relation**. Return that same `DeltaTableV2`; do not wrap or
copy it. No core/API/server/client/storage-schema or base `spark-delta` source
change is required. Add an explicit `compileOnly(project(":delta:delta-common"))`
to the optional module: its current delta-common dependency is test-only and its
base connector dependency is nontransitive.

The base connector's `load` first reads a receipt, validates a separate native head
handle, loads another native table, then calls package-private
`DeltaTableIdentity.attach`. `attach` returns a copy with a canonical `CatalogTable`
and schema; it preserves no receipt. Consequently the new wrapper must validate
the returned object's own initial snapshot and relation, not infer their binding
from the earlier head check. `DeltaRegistrationReceipt.fromProperties`, identity
equality and all required getters are public. The package-private base registry
helpers are unnecessary.

Delta 3.3.0's ordinary `DeltaRelation.fromV2Relation` uses
`d.withOptions(...).toBaseRelation` and `d.ttSafeCatalogTable`; ordinary options
return `d`, and non-time-travel `ttSafeCatalogTable` retains its descriptor object.
Thus forcing the public lazy base relation once on the returned table creates an
exact object seam through fallback. It does not require replacing native analysis.
See [DeltaAnalysis 3.3.0](https://raw.githubusercontent.com/delta-io/delta/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala)
and [DeltaTableV2 3.3.0](https://raw.githubusercontent.com/delta-io/delta/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaTableV2.scala).
`DeltaLog.createRelation` constructs a `TahoeLogFileIndex` and `HadoopFsRelation`;
it does not collect user rows. Its anonymous relation can retain catalog metadata,
which matters for weak-map ownership. See [DeltaLog 3.3.0](https://raw.githubusercontent.com/delta-io/delta/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala).

The 3.3.2 DeltaAnalysis/DeltaTableV2 source fetches failed. Their exact propagation,
plus Scala visibility and both Spark patches, remain explicit compilation/native
integration gates. A lost descriptor or changed lazy relation identity must deny;
do not introduce a path lookup, string ticket or structural-equality fallback to
make a failing pinned test pass.

## Optional-module types and ownership

Use ordinary non-case classes for secret-bearing receipts, with sanitized
`toString`, no serializable implementation and private fields. Signatures below
are proposed package-local Java/Scala interfaces, not claims about existing APIs.

```java
// DeltaRangerMetadataClient additions; no Catalog/Table handle escapes this method.
PublishedObservation readPublished(Identifier id);
MetadataLeaseStamp leaseStamp();
void requireLive(MetadataLeaseStamp expected);

// DeltaRangerCatalogSpark35 additions.
@Override public Table loadTable(Identifier id);
DeltaRangerRegistrationBindings registrationBindings();

// Immutable values, excluding DTO/client/native objects.
// PublishedObservation: lease stamp, canonical metalake/catalog/database/table,
// DeltaRegistrationIdentity, PUBLISHED state, receipt location/native ID/version,
// projected logical schema stamp. Native ID is opaque text, not UUID parsing.
```

```scala
private[ranger] final class DeltaRangerRegistrationBindings(/* catalog owner */) {
  def capture(id: Identifier, before: PublishedObservation,
      table: DeltaTableV2, after: PublishedObservation): RegistrationBinding
  def resolve(relation: LogicalRelation): BoundRegistrationScan
  def validateCurrent(scan: BoundRegistrationScan): RegistrationObservation
  def requireLive(): Unit
  def close(): Unit
}
```

The owner is the specific catalog wrapper instance plus its immutable metadata
lease identity and canonical metalake/catalog. Catalog name locates this owner in
Spark's catalog manager; it never proves scan provenance. `resolve` requires this
owner, exact descriptor registry membership, and matching actual native objects.
An identically named replacement wrapper owns a different registry/lease and must
reject old receipts.

The registry is a **weak identity map**, keyed by the exact returned
`CatalogTable` instance. Ordinary `WeakHashMap` is wrong: `CatalogTable` is a Scala
case class with structural equality. Implement weak-reference keys with stable
`System.identityHashCode`, live-referent `==`/`eq`, and a `ReferenceQueue`; cleared
keys compare equal only to themselves. Synchronize its bounded operations, drain
the queue on access/insertion, and clear on catalog close. Hash collisions must
still compare referents. Never evict an in-use binding merely for a short TTL.

`RegistrationBinding` contains immutable canonical/receipt/schema stamps and weak
references to the admitted `HadoopFsRelation`, `TahoeLogFileIndex`, and `DeltaLog`.
It must not strongly reference the key, native table, snapshot, native relation,
owner, or a closure that reaches them: the relation's implementation can capture
the descriptor, defeating weak keys. The owning registry is instance-local; there
is no global name/path/UUID authority map. A live returned table or analyzed plan
holds its descriptor and relation; registry lookup therefore survives normal GC.

After resolution, `BoundRegistrationScan` is strongly owned by the driver decision
receipt and retains the original descriptor, relation/index and immutable binding
for that scan occurrence. It can retain the catalog owner for validation. This is
not stored as the weak-map value. Keep it outside executor closures, plan products,
JSON/text rendering and broadcast values. A cloned plan receives provenance only
after validation against the actual corresponding scan; a copied marker is not a
new binding. Self joins can share a load binding but retain separate occurrence
IDs, attributes and original access sets in the decision receipt.

## Scoped metadata lease, including the currently uncovered table read

`DeltaRangerMetadataClient.get()` currently checks before and after
`GravitinoClient.loadCatalog`; its caller's later `asTableCatalog.loadTable` is
outside those checks. Calling `get()` once is insufficient at action time.

`readPublished` must validate a single literal namespace/name, establish its lease
stamp, call the existing `get()`, require returned catalog name/provider to match
the configured owner, call `loadTable(NameIdentifier.of(database, table))`, decode
the complete receipt, and build immutable schema/receipt values. Require expected
table name and PUBLISHED state. Recheck the same owned client/lease and monotonic
deadline **after table I/O and decoding**, including on exceptional exit; surface
a sanitized failure. Never hold the metadata monitor during network I/O. Missing,
unpublished, malformed and unavailable metadata all fail closed. The ordinary
read endpoint suffices; do not send STATUS commands using the operation actor.

`MetadataLeaseStamp` is a fresh opaque identity owned by the once-configured
client. `requireLive(expected)` compares identity and existing configuration,
closed state and monotonic expiry; it does not extend the deadline or create a
replacement client. Close during a read makes its eventual successful response
unusable. Preserve the current finite lifetime, asynchronous expiration and owned
client cleanup. A receipt cannot renew a closed lease.

The wrapper's no-argument production construction must use the same
`state.metadata` for base loads and receipt reads. The existing test-only
`(State, Supplier<Catalog>)` constructor must not create a split path where base
loads use a fake supplier but receipt checks quietly use an unrelated lease.
Prefer replacing that fixture seam with a package-private metadata-client factory
injection beneath the **real** get/readPublished/expiry implementation. For
example, inject a builder-to-client function, invoked only after the existing
authentication/metalake validation; production uses the real builder and tests
return a mock client. Keep that validation outside the injected function. Existing
unit `configure(catalog, lifetime, clientFactory)` can exercise isolated client
tests without adding a new public authentication bypass.

## Named-load protocol

1. Check catalog/engine owner liveness and reject path identifiers, missing/multiple
   namespace levels and already unsupported time travel. Capture `before` with
   `readPublished(id)` through the distinct metadata principal. This observation
   is the provisional owner of a catalog-load native operation scope.
2. Enter that positively owned native metadata scope before `super.loadTable(id)`.
   The base head check and snapshot reconstruction happen inside this scope.
   Capture the returned `DeltaTableV2`, its exact descriptor, its `initialSnapshot`,
   and its public lazy `toBaseRelation`, requiring a `HadoopFsRelation` with an
   ordinary `TahoeLogFileIndex`. Do not call user collection, filesForScan, prepare
   or a fresh path loader to create this proof.
3. Require exact canonical descriptor identifier/provider/location; no time travel,
   path partition filters or CDF. Validate the actual table/index log identity,
   analysis snapshot native ID, version lower bound, and schemas. Capture a fresh
   native head from this same admitted log under the typed operation when needed
   for the current schema/version check. Preserve distinct admitted-schema and
   native-schema stamps below. This native operation protocol belongs to the
   companion physical/native provenance design.
4. Read `after` using the scoped metadata client. Compare complete before/after
   registration observation, including nativeVersion and projected schema, to
   detect observed load-time drift. Compare both to the returned native evidence.
   Recheck metadata/catalog lease after all work; reject instead of retrying and
   adopting an observation that never bracketed this returned object.
5. Register the exact descriptor -> completed binding, then return the same table.
   Insertion and close must be coordinated: no successful insertion after close.
   No partial binding becomes visible on any failure.

Postfallback `resolve` requires descriptor identity membership, exact native
base-relation/index identity, native log identity and matching stamps. SQL aliases
affect qualifiers, not canonical binding. An equal descriptor copy, a fresh index
at the same location, another catalog's descriptor or a raw Delta path cannot
resolve. The optional rule's temporary privilege-extraction descriptor copies must
be made **after** resolving originals; they are never registry keys.

## Schema and action comparison

Retain separate immutable values:

* Native stamp: raw `initialSnapshot.metadata.schemaString`, ordered partition
  names, protocol value, and the column-mapping mode/configuration needed to bind
  physical column IDs. Compare full raw schema, including mapping metadata, rather
  than only public `table.schema()` which strips it. Conservatively reject any
  schema/protocol/mapping change until separately qualified.
* Admitted Spark stamp: descriptor schema plus actual relation data/partition
  schema and original output type/name/order/nullability. Preserve original
  attributes separately for occurrence identity; ExprIds are not schema evolution.
* Metadata projection stamp: ordered Gravitino column names, recursively converted
  data types and nullability via existing `SparkTypeConverter`, detached from the
  mutable DTO. Compare a logical projection of native schema to it. Comments can
  be excluded from this logical comparison, but a raw native schema change still
  conservatively denies. Conversion failures reject. Do not mistake a published
  receipt for a receipt carrying complete native schema: it has no such field.

Native `TahoeLogFileIndex.metadata` reflects its analysis snapshot, while its
ordinary `version` can consult a later volatile snapshot. Capture
`snapshotAtAnalysis.version` rather than mixing these. Its inherited native
compatibility test can permit some schema changes; the retained-plan contract here
is deliberately exact. See [TahoeFileIndex 3.3.2](https://raw.githubusercontent.com/delta-io/delta/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/files/TahoeFileIndex.scala).

At pre-CBO and collection entry, `validateCurrent` first validates the retained
native object provenance, then reads a fresh scoped `PublishedObservation` for
its canonical name. Require the same `(id, generation, token)` incarnation,
PUBLISHED state, registered location and opaque native ID. Compare fresh metadata
logical schema to the retained logical schema. Inspect the current native head
using the **retained exact index's log**, under an operation owned by this binding,
and require native ID/schema/protocol/mapping compatibility and a version at least
the metadata receipt's version. Do not call `super.loadTable` at action time and
accidentally authorize a replacement table while executing the old one.

Read registration again after native inspection and compare the two complete
observations. Reject observed batch drift, then perform final lease checks for all
owning catalogs. The outer decision validator must perform its final subject and
engine/role checks after this metadata I/O, not before it. For joins, collect all
checks before any user child preparation. Keep preparation's newly selected scan
snapshot bound to the occurrence through the companion pre-CBO/native proof.

Native-version advancement **between** admitted actions is not a new registration
incarnation. With unchanged schema/UUID/protocol, a retained physical plan may
continue its previously selected snapshot; this does not promise latest rows.
Reject regression or a fresh receipt claiming a version newer than the verified
native head. Version changes observed **within** a validation bracket deny as
drift. Replacement generation/token/native UUID, missing registration and pending
states deny even when Ranger versions are unchanged. The server read currently
refreshes native evidence and schema before returning PUBLISHED, but the optional
client still needs its own bracket and actual-scan checks.

Compare stored registered location as its canonical value and verify actual native
URIs against it. Production registration already canonicalizes admitted S3 paths
to `s3a` using `DeltaLocationPolicy`; do not invent filesystem symlink resolution,
bucket/identifier case folding or path aliases as authority. Local-file fixtures
need an explicit test location comparator; production path support stays within
the existing contract. Two canonical names at the same location retain separate
receipt/owner/resource bindings even if the native log cache returns one log.

These are optimistic observations, not a write fence. They reject observed drift;
an update after the final admission boundary does not cancel the action, and an
unobservable change-and-revert between reads cannot be detected without a server
fence/epoch API. Keep the existing exclusion of concurrent dataset writers.

## Deterministic implementation gates

1. `TestDeltaRangerRegistrationBindings`: equal-but-distinct CatalogTable keys deny;
   real identity resolves; simulated identity-hash collision remains separate;
   descriptor A + index B denies even for equal paths/UUID/schema; separate
   catalog wrappers and same-name reinitialization deny; queue draining and close
   remove entries. Test reference-queue cleanup by clearing/enqueuing test-owned
   reference keys, not by sleeps or hoping `System.gc()` runs. Inspect value
   reachability to prevent descriptor/base-relation closure cycles.
2. `TestDeltaRangerMetadataClient`: use existing fake clock/scheduler and a mock
   GravitinoClient/Catalog/TableCatalog. Block specifically in `loadTable` with
   latches; advance deadline or close, release its successful response, require
   denial. Repeat with malformed receipt/exception, close after decoding, wrong
   catalog/provider/table, and expired lease after a prior success. Verify no
   actor supplier call and unchanged UGI throughout. An unconfigured fake
   Supplier<Catalog> must not make production receipt validation succeed.
3. `TestDeltaRangerCatalogSpark35`: script before/base/after observations and returned
   native handles. Test published control; pending/missing; changing each identity
   field/location/native UUID/schema; base head handle valid but returned handle
   different; after-read closure; native-version regression and mid-bracket advance.
   Assert same native object returned and no binding installed on failure.
4. Native integration on all four pinned pairs: actual named load -> native V1
   fallback preserves descriptor, base relation and index reference identity;
   record those assertions explicitly. Include SQL aliases, self join, two catalogs
   sharing a native path, and copied descriptor/index negative controls. Exercise
   metadata cold start and cached snapshots so eager lazy-relation capture does
   not break typed native admission. No unsupported-shape denial counts as a
   successful provenance test.
5. Retain Dataset/analyzed/optimized/physical plans, mutate mocked registration
   observations deterministically, then invoke repeated collect/collectAsList.
   Every stale case needs an unchanged-state repeated-action control and a
   guard-specific denial with child prepare/execute counters unchanged. Cover
   pending/replacement/missing, native schema and column-mapping changes, and
   metadata closure/expiry after the last successful collection. A data-only
   version advance control proves it is not mistaken for a new incarnation.

These gates prove the seam and wiring when implemented. Real Ranger downloads and
complete four-pair production acceptance remain separate; this design does not
close task 4.
