# Task 4: late prepared-index native fallback seam

Read-only feasibility audit, September 23, 2026. Only this document was written. No production/test/Gradle edits, JVM, build, services, Docker, agents or commits were run. No memory search tool was exposed; this audit used the existing design, evidence, pinned source and dependency class bytes. The logical-receipt implementation remains frozen.

**Recommendation: use the existing protected `PrepareDeltaScan.getPreparedIndex` factory to return an ordinary, non-case Ranger-package subclass of `PreparedDeltaFileIndex`; override public `matchingFiles` to enter a fresh, explicitly receipt/registration-owned native invocation around `super.matchingFiles(partitionFilters, dataFilters)`.** The source and binary seams are present on both Delta patches. This is a feasible integration route for the already-planned typed operation protocol, not a compiled or runtime-proven fix and not permission to derive an owner from an index or path.

## Established failure and scope

The baseline is `513e13ba1ee85dd8226f2785c636fd2b76071f79`. The installed query is:

```sql
SELECT x.secret AS first, y.secret AS second
FROM a.db.t x JOIN a.db.t y ON x.id=y.id
WHERE y.region='keep'
```

The original XML in `task-4-evidence/analysis-receipt-baseline-native-control-2/spark-connector-v3.5-spark-delta-ranger-xml/TEST-org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerMaskRules.xml` records two tests, one failure: `baselineInstalledAsymmetricSelfJoinNativeControl` fails with `Unqualified Delta Ranger route: LogicalRDD`; the original `quotedColumnsAndSelfJoinAliasesKeepOriginalIdentity` passes. The failure stack reaches `Snapshot.filesForScan` and `PreparedDeltaFileIndex.matchingFiles(PrepareDeltaScan.scala:386)` on broadcast preparation. The sibling `task-4-evidence/analysis-receipt-baseline-root-verification.json` records baseline source comparison, the sole added installed test, and archived compiled-input verification. Its XML SHA-256 is `7a477aa22a5a934508017b928a2ab189b223dad10ee28960a3ceba0e2f27beed`.

The current `DeltaRangerReadRule.prepare` scopes only synchronous `PrepareDeltaScan.apply` (lines 138–161 when inspected). `DeltaRangerPlanAdmission.withNativeScans` removes the ThreadLocal in `finally`. The late call is outside that lifetime. Reopening the old schema-based `withNativeScans` from a saved index would reproduce its weaker authority model; it is not this recommendation.

This audit supplements sections 4 and 8 of `task-4-physical-provenance-design.md` and the final section of `task-4-analysis-receipt-implementation-brief.md`. It does not replace their state/stats capture, sealing, native witness, registration or physical acceptance requirements.

## Exact Delta references and patch evidence

All local source references below are under `task-3-evidence/pinned/org/apache/spark/sql/delta/`:

| Source | Relevant behavior |
| --- | --- |
| `stats/PrepareDeltaScan.scala:76–104` | Protected scan-generator and prepared-index factories; the latter constructs the index from the actual input index and supplied `DeltaScan`, asserting empty extracted partition filters. |
| `stats/PrepareDeltaScan.scala:139–183` | `prepareDeltaScan` calls the factory for each matched scan; Delta may share a `DeltaScan` for canonicalized equivalent scans, then installs each returned index through `replaceFileIndex`. |
| `stats/PrepareDeltaScan.scala:341–393` | Public non-final case class and public non-final `matchingFiles`. It returns prepared files when either filter-set comparison matches; otherwise it calls the exact `preparedScan.scannedSnapshot.filesForScan(partitionFilters ++ dataFilters)`. |
| `stats/PrepareDeltaScan.scala:399–418` | Existing input-file, refresh, size, equality and hash behavior. Equality omits any Ranger authority. |
| `stats/DeltaScan.scala:75–98` | Public exact `scannedSnapshot` and filter fields in the second parameter list; snapshot/filter fields are deliberately excluded from generated case-class equality. |
| `files/TahoeFileIndex.scala:91–110` | Both `listPartitionsAsAddFiles` and ordinary file listing dispatch through `matchingFiles`. |
| `DeltaTable.scala:415–420` | Copies logical/filesystem relations while installing the supplied index object as `location`. |
| `stats/DataSkippingReader.scala:223–258, 1184–1301` | Stats cache and public final `withStats`; `filesForScan` can create native filtering queries over that cache. |
| `util/StateCache.scala:51–103` | Actual persisted RDD becomes a new `LogicalRDD` through `Dataset.ofRows`, which can reenter analysis; dataset wrappers can be recreated across sessions. |

The official [Delta 3.3.2 source](https://raw.githubusercontent.com/delta-io/delta/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/stats/PrepareDeltaScan.scala) corroborates the factory and fallback. The attempted official 3.3.0 fetch was unavailable; this audit does not claim a separately fetched 3.3.0 source archive. Instead, both actual cached dependency jars were opened with Python `zipfile`; their classfile constant pools, class flags and method tables were parsed directly, without loading classes or starting a JVM. The four relevant class entries are byte-identical, including their compiled code and Scala metadata, across the two patches. Thus the pinned source's seam is supported by exact binary identity for 3.3.0 as well as 3.3.2.

Jar locations relative to `/Users/jacobo.calderon/.gradle/caches/modules-2/files-2.1/io.delta/delta-spark_2.12/`:

| Patch / relative artifact | Whole jar SHA-256 |
| --- | --- |
| `3.3.0/f3ed9d3e1e34a4e2d45b810ea3dfa454e1d00d01/delta-spark_2.12-3.3.0.jar` | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| `3.3.2/7fff11b74e7284eec27fe106f31c95b3acc8018/delta-spark_2.12-3.3.2.jar` | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |

Class entry names have prefix `org/apache/spark/sql/delta/stats/` and suffix `.class`:

| Entry | Identical entry SHA-256 on both patches |
| --- | --- |
| `PreparedDeltaFileIndex` | `dbf92630831eaa370eac90a0dd684a409c9b7de13ee314973be407a963fbed03` |
| `PrepareDeltaScan` | `031294e5e4cc6e5c84c4d676bd895c80d6906860f0c8bb94b21ae34c98fd0614` |
| `PrepareDeltaScanBase` | `7e675ddda95b370040708555fe5dc94a35c05a05fee7df1377450fee6fa5a1b5` |
| `DeltaScan` | `7a1ded27faa43b12703b761be3a8894c74cbefeabedff4dedb1696697783a5b8` |

`PreparedDeltaFileIndex` and `PrepareDeltaScan` have class flags `0x0021` (public, superclass semantics; not final). `matchingFiles`, the prepared-index constructor, the getters `preparedScan`/`versionScanned`, and generated `copy` each have flags `0x0001` (public; not final). `getPreparedIndex(DeltaScan, TahoeLogFileIndex): PreparedDeltaFileIndex` appears public at bytecode level, with the protected Scala source contract preserved by the identical metadata. The subclass should honor that protected source contract. These observations establish an available seam; only a Scala compilation can close source-level inheritance/signature compatibility.

## Smallest integration route

1. Keep the existing owner-bearing `PrepareScans(UserReceipt, bindings)` design. Before creating an index, resolve its exact original input object through the complete receipt/registration bindings, and retain all matching occurrence obligations. A self-join can share native objects; object sharing must not collapse receipt occurrences. Keep the existing exact native `Snapshot` generator check. The factory receives the actual `DeltaScan` used by Delta, including when Delta reused it; verify that actual snapshot against the qualified transition, rather than substituting a newly obtained head snapshot.
2. Override only `getPreparedIndex` in the existing Ranger-owned `PrepareDeltaScan` instance. Construct the ordinary non-case subclass with the exact same Spark session, DeltaLog, Path, `DeltaScan` and version option as the base factory; preserve its empty-partition-filter assertion. Return and record that very subclass instance as the prepared output of the original-to-prepared binding. Do not call `super.getPreparedIndex`, record its temporary base index and return a different wrapper: the recorded identity must be the one installed in the relation. This is a narrow adjustment to section 3's original record-after-super suggestion.
3. Store a private driver-only invocation authority issued by the valid preparation operation and bound to that actual prepared index, original input index, receipt/registration owner and actual scan/snapshot. It is outside the inherited product surface, transient for serialization, and unusable when missing. Constructor visibility must prevent arbitrary public index construction from issuing authority. No new global strong registry is needed.
4. On `matchingFiles`, derive a fresh bounded invocation of the existing `PrepareScans` purpose from that sealed owner-bound authority, on the thread making the call. Bind the actual partition/data expression sequences supplied to this invocation; pass them unchanged to `super.matchingFiles`. Keep Delta's own filter comparison, distinct-files behavior, snapshot fallback, telemetry and errors. Do not use `ExpressionSet` equality as an authorization test or rewrite filters to force reuse. Bracketing the entire method avoids copying its fallback-selection implementation; its fast path simply performs no additional native query.
5. The worker invocation uses the section 4 native protocol: exact snapshot/state/stats capture or explicit exact cached-lineage rebind, analysis-only pending candidates, sealed finite native recipes, failed-operation invalidation, and exception-safe removal of the active scope. Successful closing freezes this invocation's issuance while retaining its sealed tickets for delayed native work. Do not reopen issuance on the original already-closed operation. A narrowly typed continuation can issue a new invocation from its sealed owner; it cannot mint a new owner. Concurrent invocations have distinct state and no cross-thread mutable scope sharing. No `InheritableThreadLocal`, broad thread-pool propagation or session-global current-plan slot is necessary.

The authority admits the native metadata operation only. Actual filter arguments are inputs to that operation, not permission for an arbitrary user subtree, data scan or modified receipt. The finite native witness verifier must still reject foreign functions, mixed leaves and same-schema RDDs. Do not recursively run the complete user action validator while it performs its own native metadata validation. A future physical boundary still owns final user scan/filter/occurrence verification.

The initial owner-bearing load operation, exact RDD capture, failure semantics and all finite native routes from section 4 remain mandatory. A callback wrapper plus the existing Boolean/schema native predicate is insufficient. No physical guard or columnar rule is proposed for installation by this audit.

## Copy and equality hazards

An ordinary subclass is required: Scala case-to-case inheritance is inappropriate. The inherited generated `copy` returns a base `PreparedDeltaFileIndex`, without the private invocation authority. An explicit index copy must be rejected by exact prepared-index provenance checks; do not transfer authority automatically through `copy`, equality or matching constructor fields. Inherited `equals`/`hashCode` should retain Delta's ordinary reuse semantics, but every Ranger binding must use reference identity. In particular `DeltaScan.equals` intentionally ignores snapshot/filter fields, so even equal scans cannot substitute for one another as authority.

Normal Spark plan cloning is a different operation. [Spark 3.5.4 LogicalRelation](https://raw.githubusercontent.com/apache/spark/v3.5.4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/LogicalRelation.scala) changes output attributes in `newInstance` while retaining the relation; canonicalization also retains it. [Spark 3.5.6 TreeNode](https://raw.githubusercontent.com/apache/spark/v3.5.6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees/TreeNode.scala) clones tree nodes, not arbitrary embedded file-index products. Together with Delta's exact `location` installation, this supports retaining the subclass object through ordinary logical/physical tree clones. It does not establish universal propagation across optimizer substitutions; verify actual references on both Spark patches. Attribute changes in `newInstance` still require the existing occurrence mapping rather than blanket output equivalence.

Because the base case class is serializable, the added owner payload must be transient and excluded from product/explain/JSON surfaces. Deserialized instances must fail closed before opening an operation, including on the prepared-files fast path. Retaining a session/DeltaLog in the inherited base surface does not justify serializing receipt/engine/principal authority. Ensure task closures do not accidentally capture that authority.

## Concrete gates for the next implementation

These are required checks, not results of this audit:

1. Compile the ordinary Ranger-package subclass, exact constructor, `matchingFiles` override calling `super`, and protected factory override against Delta 3.3.0 and 3.3.2. No Delta/Spark package bridge, shadow class or reflection. Compile/test both Spark 3.5.4 and 3.5.6 profiles before claiming the four-pair surface.
2. Preserve the original failing XML as RED. Run the exact installed asymmetric query with real wrapper/registration bindings and production rules; assert exact expected rows and both original self-join obligations. Instrument invocation entry sufficiently to prove the differing-filter fallback runs on the broadcast worker after the original preparation scope has closed. A direct-rule-only mask test, prepared-files fast path, generic rejection or synchronous synthetic call does not close this gate.
3. Assert by reference that installed relation and eventual `FileSourceScanExec.relation.location` carry the same issued subclass; its `preparedScan` and snapshot are the supplied actual objects. Exercise normal logical clone, physical clone/preparation and self-join alias/output changes on both Spark patches. Confirm equal-but-distinct prepared indices/scans cannot satisfy authority. Explicit inherited `copy`, same-path substitution and copied tags reject at the relevant provenance boundary.
4. On genuine fallback, capture/seal the actual state and stats RDD/output identities; a cloned native logical tree sharing those objects succeeds after successful close. Pending/failed-operation candidates, fabricated same-schema RDDs, changed snapshot/index/session and foreign object functions fail. Exercise exact cached-lineage rebind and simultaneous callbacks with distinct operations. Verify scope removal after successful return and exception, and no authority inheritance to an unrelated worker task.
5. Compare files/rows and unchanged filter inputs for both reused and fallback branches, including partition filters. Keep AQE, native caching and data skipping settings intact. Native delayed planning/AQE still needs section 8's full native witness gate; worker-local scope alone does not qualify later work on another worker. Retain installed two-catalog, quoted-column, filter and mask controls.
6. Verify inert serialization and rendering of the subclass's new payload, plus failure before native work when that payload is absent or owner binding failed. Repeated calls may create separately owned invocations; they cannot extend a closed operation's issuance or bypass owner lifecycle checks.

Public `Snapshot.filesForScan` alone is too late to install a callback into the actual existing snapshot; replacing/subclassing that snapshot would lose the exact object lineage. Overriding preparation-time `filesForScan` alone misses the later call. A generic FileIndex wrapper does not meet `getPreparedIndex`'s concrete return type and would require broader replacement logic. The prepared-index subclass is therefore the smallest source-backed seam found. Data-skipping-disabled plans retain a `TahoeLogFileIndex` through `prepareDeltaScanWithoutFileSkipping`; this audit does not solve that distinct route or authorize changing the setting.

Task 4 remains open. The audit establishes a bounded public/protected hook and patch identity, while compile compatibility, exact authority integration, delayed native propagation, positive installed rows and the later physical collection boundary remain unproved.
