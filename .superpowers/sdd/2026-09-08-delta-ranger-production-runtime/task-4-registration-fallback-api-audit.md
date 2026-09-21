# Task 4: Delta 3.3.2 registration fallback API audit

Audit date: 2026-09-21. This closes only the Delta 3.3.2 fallback API gap in
`task-4-registration-receipt-design.md`; it is not a design re-review or runtime
qualification. No source, build, Gradle, Spark, Docker, network, service or commit
operation was used.

## Verdict

**Compatible for the pinned ordinary named-table fallback, subject to the four-pair
runtime identity gate below.** The pinned Delta 3.3.2 bytecode exposes the required
methods and preserves the exact table, descriptor, returned base-relation and
file-index references along the ordinary non-time-travel, non-CDF path. No API
incompatibility was found.

The audited artifact is the exact Delta jar recorded by
`task-4-evidence/native-snapshot-full-332/execution-classpath.json`:

```
~/.gradle/caches/modules-2/files-2.1/io.delta/delta-spark_2.12/3.3.2/
  7fff11b74e7284eec27fe106f31c95b3acc8018/delta-spark_2.12-3.3.2.jar
sha256 4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85
```

That cache entry contains the binary jar and POM, but no sources jar. The findings
below therefore come from `javap -p -c` against that exact binary. This is stronger
evidence for the pinned executable artifact than a nearby tag, while still not
replacing runtime identity tests.

## Pinned bytecode proof

### Public callable seam

`DeltaTableV2` has JVM-public accessors for `initialSnapshot()`, `deltaLog()`,
`catalogTable()`, `toBaseRelation()`, `withOptions(scala.collection.immutable.Map)`
and `ttSafeCatalogTable()`. `DeltaRelation$.fromV2Relation(DeltaTableV2,
DataSourceV2Relation, CaseInsensitiveStringMap)` is also JVM-public. The lazy-value
compute methods and backing fields are private, as expected; the accessors needed
by Scala module code are public.

These are public bytecode members in Delta's internal package surface, not a
cross-version compatibility promise. Compile and native tests must remain pinned
gates. Java code would also need Scala collection interop and
`DeltaRelation$.MODULE$`; the planned Scala fallback avoids that friction.

### Exact identity flow on the ordinary path

1. **Table:** `DeltaTableV2.withOptions` parses time-travel and CDF options. Its
   ordinary branch ends at offsets 180-181 with `aload_0; areturn`, so an ordinary
   option map retains the exact `DeltaTableV2`. A new table is returned only by the
   time-travel branch (offsets 40-106) or CDF branch (107-179).

2. **Descriptor:** `ttSafeCatalogTable$lzycompute` loads `catalogTable` into local
   3. It creates `Some(catalog.copy(...))` only when `timeTravelSpec.isDefined`; the
   non-time-travel paths converge at offsets 248/251 and store `aload_3` directly.
   Thus both the original `Option` and its `CatalogTable` referent are retained on
   the admitted ordinary path.

3. **Base relation:** `toBaseRelation$lzycompute` stores one computed
   `BaseRelation` in its backing field at offset 149, sets the lazy bitmap, and its
   public accessor subsequently returns that field. The ordinary lambda
   `$anonfun$toBaseRelation$3` calls the same table's `deltaLog.createRelation`
   with `Some(initialSnapshot)`, `catalogTable`, and the time-travel flag. Repeated
   `toBaseRelation` calls therefore return the exact first relation reference.

4. **File index:** `DeltaLog.createRelation` constructs one
   `TahoeLogFileIndex(this DeltaLog, dataPath, selectedSnapshot, partitionFilters,
   isTimeTravelQuery)` and passes that exact local to
   `buildHadoopFsRelationWithFileIndex`. That method passes the exact index as the
   first `HadoopFsRelation` constructor argument. `DeltaLog$$anon$2`, the relation
   ultimately returned by `createRelation`, constructs its superclass with
   `underlying.location()`; this is the same `TahoeLogFileIndex` reference. The
   binding must retain this returned anonymous relation and its `location`, not
   the short-lived intermediate `HadoopFsRelation` object. One related caveat is
   intentional: the non-time-travel `TahoeLogFileIndex` overload wraps the input
   `Snapshot` in a new `ShallowSnapshotDescriptor`. Tests must compare its native
   ID/version/schema evidence, not require `snapshotAtAnalysis eq initialSnapshot`.

5. **Fallback logical relation:** ordinary
   `DeltaRelation$.$anonfun$fromV2Relation$1` calls
   `d.withOptions(...).toBaseRelation`, stores that exact result, and passes it
   together with `d.ttSafeCatalogTable` to
   `LogicalRelationShims.newInstance`. The shim invokes the four-argument
   `LogicalRelation` constructor without copying either argument. In both locally
   pinned Spark 3.5.4 and 3.5.6 jars, that constructor stores the exact relation
   and catalog `Option` arguments in fields, and their public accessors return the
   fields directly.

The resulting ordinary fallback therefore has the identity chain required by the
design:

```
returned DeltaTableV2
  -> exact lazy BaseRelation (DeltaLog$$anon$2)
     -> exact TahoeLogFileIndex
        -> exact DeltaLog; snapshot evidence derived from the selected Snapshot
  -> exact non-time-travel CatalogTable

DeltaRelation.fromV2Relation
  -> same BaseRelation
  -> same CatalogTable
```

No 3.3.0 comparison was necessary to close this gap: the exact 3.3.2 artifact
confirms the same ordinary-path contract that the design had already established
from 3.3.0 source.

## Required runtime identity gate

Bytecode proves availability and direct reference propagation in these class
bodies. It does not qualify analysis interaction, lazy evaluation, optimizer
rewrites, classpath selection or the actual named-load shape. Native tests must
still run for all four pinned pairs: Spark 3.5.4/Delta 3.3.0, Spark 3.5.4/Delta
3.3.2, Spark 3.5.6/Delta 3.3.0 and Spark 3.5.6/Delta 3.3.2.

For an ordinary named table, each pair must use reference identity (`eq`), not
case-class equality, to establish at least:

- ordinary `withOptions` returns the loaded `DeltaTableV2`;
- `ttSafeCatalogTable.get` is the loaded table's `catalogTable.get`;
- repeated `toBaseRelation` returns the forced admitted relation;
- `DeltaRelation.fromV2Relation(...).relation` is that admitted relation;
- the logical relation's `catalogTable.get` is the admitted descriptor; and
- the returned relation's `location` is the admitted `TahoeLogFileIndex`, whose
  `deltaLog` is the admitted log and whose analysis native ID/version/schema
  matches the captured initial snapshot.

Time travel, CDF, path loads and partition-filtered shapes remain negative cases.
The bytecode explicitly shows that time travel and CDF can copy the table and that
time travel copies the descriptor, so identity failure there must deny rather than
fall back to names, paths, tickets or structural equality.
