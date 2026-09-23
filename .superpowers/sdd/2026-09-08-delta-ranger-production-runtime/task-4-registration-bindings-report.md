# Task 4 ordinary named-load native registration bindings

Status: bounded implementation and review correction round 1 matrix complete; scoped independent re-review pending. The original proof remains preserved separately from corrected-source evidence. No physical guard or retained-action completion is claimed.

Base: `0940d100b570e2b186eee4bf073a2aa7adf41a7a`. Root granted exclusive source/Gradle ownership after reader/cache review. Root approved a Java registry in place of the proposed Scala implementation: the optional module compiles Java before Scala, and changing joint compilation would alter Java/Error Prone/Javadoc behavior. Public JVM Delta/Spark APIs suffice; no build configuration change or production reflection/private Spark bridge was introduced.

## Owned files and minimal API

All source paths below are under `spark-connector/v3.5/spark-delta-ranger/`:

- New `src/main/java/.../DeltaRangerRegistrationBindings.java`: package-private registry and authority values.
- `src/main/java/.../DeltaRangerCatalogSpark35.java`: install one reader/registry, bracket real ordinary super load, expose package-local registry, close registry before metadata/engine cleanup; remove split `(State, Supplier<Catalog>)` constructor.
- `src/main/java/.../DeltaRangerMetadataClient.java`: narrow package-private builder-construction seam below real auth/metalake validation, with unchanged production default.
- `src/test/java/.../TestDeltaRangerMetadataClient.java`: explicit auth/metalake-before-factory coverage.
- `src/test/java/.../TestDeltaRangerNativeSelect.java`: existing wrapper test uses the actual finite metadata lease, with real schema/name fixture data and sanitized-denial expectation.
- New `src/test/scala/.../TestDeltaRangerRegistrationBindings.scala`: registry/native identity, weak reference and liveness controls.
- New `src/test/scala/.../TestDeltaRangerRegistrationBindingsNative.scala`: actual wrapper, fallback, installed extension, mapped/partitioned native controls.
- `src/test/scala/.../TestDeltaRangerReadRule.scala`: root-authorized optional `startupOptions` argument in existing NativeSelectFixture; defaults and existing test semantics unchanged. New wrapper fixtures supply trusted options to SparkSession.Builder before context construction.

`...` is `org/apache/gravitino/spark/connector/delta/ranger`. Reader/engine/cache/admission/extension/physical rules/base connector/catalog/core/dependencies remain untouched. No git or live/Docker operations were performed.

Registry constructor: `DeltaRangerRegistrationBindings(reader, engine)`. Package-local API: `Table load(Identifier, Supplier<Table>)`, `BoundRegistrationScan resolve(LogicalRelation)`, `requireLive()`, `close()`. The production supplier is fixed to `super.loadTable(id)` in wrapper `loadTable` at line 146; it is not an independent metadata authority. `registrationBindings()` is package-local at line 180. BoundRegistrationScan exposes only the exact descriptor/relation/index/log, immutable observation and native schema/protocol/partition/mapping/version stamps, plus explicit liveness validation. None of the binding authority classes implements Serializable or Scala Product; their rendering is constant.

## Load and resolve protocol

Registry `load` at line 74 checks owner/engine readiness, invokes the accepted reader before the real base load, rejects time-travel/options/partition-filtered shapes before forcing native lazy state, forces the returned exact DeltaTableV2's initialSnapshot and toBaseRelation, reads metadata again through the same owned finite lease, validates complete before/after publication equality and native evidence, and inserts only the completed binding. It returns the exact loaded object, not a copy or proxy.

Evidence checks include canonical descriptor catalog/database/table/provider/location; ordinary ttSafeCatalogTable reference identity; exact initial snapshot/table/index/log relationship; native ID and analysis version matching initial snapshot, with initial version at least the published receipt version; raw native schema/protocol, ordered partition names and mapping configuration; admitted descriptor/data/partition schemas; and reader's logical metadata projection. SnapshotAtAnalysis is a ShallowSnapshotDescriptor on this native path, so validation compares its values and log identity, not Snapshot reference equality.

The reader preserves trusted metalake/catalog and receipt location verbatim. Object-store location checks use exact canonical URI text. The only equivalence is a local absolute file path written with or without the `file` scheme, with absent authority/query/fragment and identical **raw** path; no URI normalization, path aliases, percent-decoding aliases or case folding is used.

Native raw `metadata.schemaString`, protocol JSON, ordered partition stamp and sorted column-mapping configuration stamp remain distinct from logical schema projection. The projection recursively strips metadata; it does not remove native authority fields from the raw stamp. Existing SparkTypeConverter is used by the accepted reader, not reimplemented here. Exact full native schema checks are intentionally conservative.

`resolve` at line 127 requires exact CatalogTable weak-map membership and exact admitted HadoopFsRelation, TahoeLogFileIndex and DeltaLog references, then revalidates native/admitted schema evidence, logical output name/type/order/nullability, owner/engine and the exact observation lease. Equal descriptor copies, a new equal-path index/relation, crossed relations, missing descriptors, streaming, another owner and expired/closed leases reject. Aliases and ordinary self-join relation instances keep the exact admitted native objects and resolve independently; occurrence ExprIds/privileges remain subsequent decision-receipt work. Resolution is exposed for future use before privilege-extraction copies; current read rules are not wired to it in this increment.

All upstream Exceptions, including Scala/Spark checked DeltaAnalysisException, and LinkageErrors become a constant SecurityException without upstream cause/suppressed exceptions. A temporary private denial subclass used solely to diagnose the schema rejection is absent from final production source; its intermediate evidence is preserved.

## Weak ownership and closure

`IdentityKey` at line 320 uses stable System.identityHashCode, live referent identity and self-only equality when cleared. ReferenceQueue draining at line 187 runs on lookup/insertion and close. It does not use WeakHashMap, structural CatalogTable equality, arbitrary TTL eviction or global path/name authority.

Map values (`RegistrationBinding`, line 347) contain immutable scalar/value stamps, the detached PublishedObservation, and exactly three WeakReferences to relation/index/log. No descriptor, native table, snapshot, owner or closure is held strongly. The reader observation itself contains only detached canonical/value metadata, a plain owner Object and the metadata lease stamp whose identity is another plain Object. Structural tests inspect only our own classes, never private Spark/Delta APIs. They verify those tokens do not hold a reader/client/native owner. BoundRegistrationScan (line 387) may retain strong native/owner references after successful resolution; it is never a map value.

Queue cleanup is tested by explicit clear/enqueue, without System.gc or sleeps. A separate test finds an actual identity-hash collision on the pinned JVM and verifies both live referents remain distinct in the map and collection removes only the enqueued key. It bounds the search at one million objects and fails if no collision is found; it never skips the gate.

Native/network work runs outside the registry monitor. Final insertion and close share it; metadata before/after stamps and owner/engine readiness are rechecked at insertion. Wrapper close invalidates the registry before closing the same metadata lease and engine. A latch-based actual-wrapper test stalls the final metadata read, closes concurrently without waiting for that callback, then rejects the late result. No partially validated entry is published. Liveness checks do not renew the finite lease, and a policy refresh does not invalidate registration solely because an engine publication changed; future decision receipts own that check.

## One metadata authority and fixture seam

The removed split supplier could let tests use one Catalog for base loading and another lease for authorization. Both base loads and reader brackets now use `state.metadata`. The package-private metadata constructor takes the existing clock/scheduler plus `Function<ClientBuilder, GravitinoClient>`; the function runs only inside MetadataBuilder.build after explicit auth-provider and expected-metalake validation (line 319). The no-arg production constructor still uses super.build unchanged. The construction function is transferred into the configured owned factory and cleared from the metadata field; close also clears it. This small seam avoids another HTTP fixture server and does not provide a public credential path.

Tests require missing authentication and changed metalake to fail before factory invocation; valid repeated reads create one owned client. Actual wrapper tests use only that client, never the actor supplier, and preserve caller UGI. Existing metadata monotonic deadline/cleanup/authentication tests remain GREEN. New wrapper test directories, including partitioned data, are deleted after fixtures and Spark close. The legacy existing Java fixture's inline startup configuration remains; the attempted extracted Java helper was removed, and no new proxy for Scala-private SparkContext.conf remains.

## Pinned native schema finding

The middle-position partition fixture exposed an incorrect initial expectation that HadoopFsRelation.dataSchema equaled Metadata.dataSchema. Exact Delta 3.3.2 bytecode shows buildHadoopFsRelationWithFileIndex passes the **full** metadata.schema, optionally drops NullType columns, strips writer/mapping metadata, and separately passes ordered partitionSchema. The archived bytecode is `task-4-evidence/registration-bindings-staged/delta-log-332-javap.txt`, around lines 2779–2824. Root independently confirmed the same full-schema contract in the pinned Delta 3.3.0 sources. Final capture checks the full snapshot schema and separate partition schema. The positive has declared order `id, region, secret` with `region` partitioned in the middle, and proves ordinary fallback order/identity, not an accepted denial. The mapped q fixture proves raw mapping IDs survive the native stamp while the logical projection excludes them.

NullType columns that Delta drops will conservatively fail exact full-schema comparison. These tests do not establish universal Delta schema support or qualify arbitrary writer/internal schema features.

## Verification and original evidence

All runs are offline, Java 17, Ranger 2.8.0, Scala 2.12, one test fork, max-workers=2/no-parallel, using the existing immutable-label run-gradle.py harness. Original command.json, source snapshots/hashes, full gradle.log, exit.json and fresh original XML remain under each evidence directory. Test.doFirst records execution-classpath.json and archives actual compiled directories/local jars after producer tasks, immediately before test JVM execution. Cached external dependency jars are hashed in that manifest.

Behavioral RED preceded implementation: `registration-bindings-red-1` (1/1/0/0) failed same-live-referent weak-key equality against the empty scaffold; `registration-bindings-red-native` (1/1/0/0) failed assertNotNull for binding resolution of an actual native fallback. These are feature failures, not compile failures.

Intermediate failures are retained separately: green-1 failed Error Prone on intentional reference comparisons (now narrowly documented/suppressed); green-2 failed Scala lambda syntax; green-3 failed Scala access to SparkContext.conf; green-4 ran 10/1/0/0 and exposed checked CDF failure before late options rejection; green-5 ran 14/1/0/0 and exposed the partition full-schema expectation; native-shape-diagnostic ran 1/1/0/0 and identified that exact comparison. green-6 ran 1/0/0/0 after the pinned-contract fix. The broader pre-freeze green-354-332 ran 72/0/0/0 plus Javadoc successfully. format-final succeeded, then final source was frozen before the matrix captures.

| Final evidence directory (`task-4-evidence/`) | Spark / Delta | Original XML tests/failures/errors/skips | Status |
| --- | --- | --- | --- |
| registration-bindings-final-354-332 | 3.5.4 / 3.3.2 | 74 / 0 / 0 / 0 | GREEN, Spotless and Javadoc |
| registration-bindings-final-354-330 | 3.5.4 / 3.3.0 | 15 / 0 / 0 / 0 | GREEN, Spotless |
| registration-bindings-final-356-330 | 3.5.6 / 3.3.0 | 15 / 0 / 0 / 0 | GREEN, Spotless |
| registration-bindings-final-356-332 | 3.5.6 / 3.3.2 | 15 / 0 / 0 / 0 | GREEN, Spotless |

Each final native pair runs the complete registry 10 plus actual-wrapper 5 tests, including exact DeltaRelation.fromV2Relation fallback identities, repeated lazy getters/withOptions/ttSafeCatalogTable, same-name replacement owners, distinct canonical names sharing one path, equal descriptor/equal-path index/crossed relation negatives, mapped and partitioned positives, weak values, drift/closure, and actual installed production logical extension cold/warm loads. The 74-test row also runs metadata 20, reader 19, catalog 13, existing read rule 5 and native snapshot admission 2. It does not count unsupported-shape denials as a successful native binding.

Exact final argv is in each `command.json`. Common invocation shape is:

```sh
python3 .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/run-gradle.py RUN \
  -I .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix-classpath-capture.gradle \
  -Ptask3FixEvidenceOutput=.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/RUN \
  :spark-connector:spark-delta-ranger-3.5:spotlessCheck \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerRegistrationBindings' --tests '*TestDeltaRangerRegistrationBindingsNative' \
  -PdeltaSparkVersion=SPARK -PdeltaVersion=DELTA -PskipITs --offline --console=plain \
  --max-workers=2 --no-parallel '-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2'
```

The 354-332 row additionally selects the five regression classes named above and appends the module javadoc task. Existing Gradle deprecation/worktree Git-ID warnings are retained in logs, not suppressed or reclassified as test failures.

## Frozen SHA256

The complete machine-readable manifest is `task-4-evidence/registration-bindings-staged/final-source-hashes.json`. At the original handoff, all eight source bytes matched all four original final pre-run source archives. Full first-RED versus final source manifests differ only in these eight owned files. The Test.doFirst classpath audit confirms the expected Spark SQL/Delta Spark pair and Ranger plugins-common 2.8.0 in every final execution. The final matrix contains 119 successful test executions (74 in the main regression row plus the same 15 binding tests repeated on each other pair), not 119 distinct tests.

| Owned source | SHA256 |
| --- | --- |
| `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java` | `53182110452911f80198a436dd1bed43d51e31b3514c72727137e1a98c373cda` |
| `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerCatalogSpark35.java` | `fc0fe7c1013f617a2390e3c08658eacebc623b3e030765a405c12ed73d4e9c27` |
| `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerMetadataClient.java` | `9f7a2a464879cc304b1fa8a7bb96d4e610d3472527792b07b9286bfa9533ef5e` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerMetadataClient.java` | `f5c99539e5204609933d88ffa6a187d6aea9bcb1574451883416c224f328eda4` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerNativeSelect.java` | `bb0a174a36739861864f8b2614d1c0b60831386b889f6e817a440727b589287a` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerRegistrationBindings.scala` | `24114e5570d6fde0e3629e896968f8802bd490470a533f2654969f083ee2ce94` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerRegistrationBindingsNative.scala` | `de9e010be10787bc2871fb591778d529492c362fc2353a21fd9dc1f948a689a6` |
| `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerReadRule.scala` | `f0e334dec6b3ef6349f2624ddea186e7cd777fbf3f0a6eef38fece0d5d325bbb` |

## Remaining integration limits

This binds ordinary production wrapper loads to exact driver-local native objects and exposes resolution. It installs no physical classifier/guard, changes no existing native admission, and wires no retained logical/physical/action decision receipt. The owner-specific native operation/ticket protocol must land before physical enforcement. The unchanged metadata admission surface is not thereby qualified as safe. Cold/warm installed-extension controls establish composition for these fixtures, not complete production SELECT enforcement.

No direct user-data collection/prepare/filesForScan operation is invoked to manufacture provenance; no fresh arbitrary path loader, schema-only admission, empty metadata scope, raw-plan marker, private Spark bridge or credentials fallback was added. Native version is evidence from the admitted snapshot, not a current-head freshness guarantee. Metadata/native/policy updates remain non-atomic; future action validation must re-read the registration and qualify the retained exact log under the designed native operation owner. Logical comments are excluded while raw native metadata remains strict. Broader profiles, schema forms and production S3 runtime behavior beyond the existing canonical-path contract are unqualified. Task 4 and full Delta security completion remain open.

## Review correction round 1: deterministic hash collision coverage

Independent review found one LOW test-quality issue and no implementation/spec correctness issue. The original test could allocate up to one million retained objects without a JVM-contract guarantee of finding an identity-hash collision. Root authorized the package-private explicit-hash constructor seam. Only DeltaRangerRegistrationBindings.java and TestDeltaRangerRegistrationBindings.scala changed: the production constructor still derives its hash from System.identityHashCode and delegates; the test assigns hash 37 to distinct live referents, checks same-referent equality, collision separation, stable hash after clearing, self-only cleared equality and the same queue/map cleanup. No public authority API was added.

This correction supersedes the earlier report's description of an allocation-search collision test. The original four-row matrix, original eight-file hashes and 119 original successful executions above remain historical evidence for the original source, not evidence relabeled for this correction. They are preserved unchanged. The current two-file delta is `task-4-evidence/registration-bindings-review-fix-1-staged/fix.diff`. Corrected-source hashes are in `review-fix-source-hashes.json` in that directory. The other six binding sources and both new guard sources were verified unchanged before publication, with separate unchanged manifests retained there.

Correction qualification is complete: focused registry 10 plus actual-wrapper 5 tests passed on all four pairs, with fresh source and Test.doFirst execution-input captures. SpotlessCheck passed on every pair; the first pair also passed Javadoc. This provides 60 successful corrected-source test executions of 15 distinct tests. The original 74-test regression row remains historical evidence for its original source. The new `review-fix-matrix-audit.json` records original XML hashes, expected Spark/Delta/Ranger dependency hashes, and all ten current/source-archive hash matches. The source is frozen for scoped re-review and the Gradle slot is released.

| Corrected evidence directory (`task-4-evidence/`) | Spark / Delta | Tests/failures/errors/skips | Status |
| --- | --- | --- | --- |
| registration-bindings-review-fix-1-354-332 | 3.5.4 / 3.3.2 | 15 / 0 / 0 / 0 | GREEN; Spotless/Javadoc |
| registration-bindings-review-fix-1-354-330 | 3.5.4 / 3.3.0 | 15 / 0 / 0 / 0 | GREEN; Spotless |
| registration-bindings-review-fix-1-356-330 | 3.5.6 / 3.3.0 | 15 / 0 / 0 / 0 | GREEN; Spotless |
| registration-bindings-review-fix-1-356-332 | 3.5.6 / 3.3.2 | 15 / 0 / 0 / 0 | GREEN; Spotless |

Corrected-source SHA256:

- `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java`: `9dcc968f15f98baa6e5c957d1058d2747a1a5d8064b979fc6c136f344ba53bb7`
- `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerRegistrationBindings.scala`: `c7cac7413608379c7ec9a185bd1c359608ad70f8b8d2b1f8fb81970cb296949b`
