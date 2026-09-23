# Ordinary named-load registration bindings review

Review scope: the eight frozen files in `task-4-evidence/registration-bindings-staged/final-source-hashes.json`, the implementation brief and fallback audit, author report SHA256 `0efcc529d81ef5198b800f049a9d9a2e9cf50f56727dcfd798a418e800a8da31`, review-package diff, supporting reader/metadata/native sources, and original four-profile evidence summarized by `registration-bindings-root-verification.json`. All eight current source hashes match the frozen manifest. No tests were rerun because the archived proof was complete and source inspection found no production defect requiring a new execution.

## Specification verdict: PASS

No specification finding was identified.

- `DeltaRangerCatalogSpark35.java:69-72,115-147` removes the split metadata supplier, installs one reader/registry over `state.metadata` and trusted metalake/catalog, and routes ordinary named loads through the real `super.loadTable(id)` supplier. Version/timestamp overloads remain denied.
- `DeltaRangerRegistrationBindings.java:74-123` brackets the exact real load with accepted reader observations, forces the returned `DeltaTableV2` initial snapshot and base relation, rejects time travel/CDF/options/partition filters, validates native evidence, and publishes only after final registry/engine/lease checks. Native/network/lazy work occurs outside the registry monitor; insertion and close share that monitor.
- `DeltaRangerRegistrationBindings.java:127-168` resolves only an exact registered descriptor and the exact admitted `HadoopFsRelation`, `TahoeLogFileIndex`, and `DeltaLog`. It rechecks descriptor, output, native schema, protocol, version, ordered partitions, mapping configuration, location, owner/engine, and lease evidence. Equal descriptors, equal-path replacement indices, crossed relations, streaming relations, other owners, and dead owners fail closed.
- `DeltaRangerRegistrationBindings.java:192-314` keeps the raw native schema/protocol/partition/mapping stamps distinct from the recursively metadata-stripped logical projection. The full snapshot schema and separate partition schema checks match the pinned Delta 3.3.0/3.3.2 relation construction established by source/bytecode and the middle-position partition test. Local path equivalence is narrowly limited to an absolute raw path with absent authority/query/fragment; non-local URIs require exact text.
- `DeltaRangerRegistrationBindings.java:320-459` implements weak exact-identity descriptor keys with stable identity hashes, self-only equality after clearing, queue removal, and values containing immutable stamps plus weak relation/index/log references. Values do not strongly retain descriptors, native tables, snapshots, catalog owners, or the registry. A successfully resolved `BoundRegistrationScan` may hold strong native/owner references and is not stored in the map.
- `DeltaRangerCatalogSpark35.java:164-183` closes the registry before the shared metadata lease and engine. `DeltaRangerRegistrationBindings.java:108-118,170-185` prevents insertion after a registry close and clears all completed bindings. The actual-wrapper latch test demonstrates that close does not wait for a stalled final metadata callback and that its late load is denied.
- `DeltaRangerMetadataClient.java:75-115,303-328` places the package-private construction function below the existing explicit-authentication and exact-metalake validation in `MetadataBuilder.build`; the public production constructor still calls `super.build`. Tests prove missing authentication and changed metalake reject before the injected function executes.

The native integration tests establish the ordinary fallback reference chain, repeated lazy getter identities, mapped metadata, partition ordering, same-path/different-owner isolation, negative copied identities, metadata drift, expiry/close, and cold/warm installed-extension composition. They do not treat an unsupported-shape denial as a positive binding result.

## Quality verdict: PASS WITH ONE LOW-SEVERITY FINDING

### LOW — Identity-hash collision coverage depends on an unguaranteed JVM allocation outcome

Location: `TestDeltaRangerRegistrationBindings.scala:84-94`.

The test retains up to one million objects and fails unless `System.identityHashCode` happens to collide within that bound. Java guarantees that the identity hash is stable for an object, but it does not guarantee a collision distribution or a collision within any allocation count. A conforming JVM can therefore fail this test even when `IdentityKey.equals`, map lookup, and queue cleanup are correct. The four pinned runs found a collision and passed, so this does not undermine the reviewed production behavior; it is a portability and test-determinism defect.

Remediation: let production `IdentityKey(value, queue)` continue to use `System.identityHashCode(value)`, and add a package-private test seam that constructs keys with an explicit hash. Use two distinct live referents with the same injected hash to exercise collision-safe lookup and queued-key removal deterministically. This also removes the million-object retention cost.

No other quality finding was identified. The APIs remain package-private and small, exception rendering is constant and sanitized, reflection-based reachability assertions are limited to owned classes, and the test matrix covers both supported Spark and Delta patch versions.

## Evidence checked

The original commands, exit records, XML suite headers/hashes, and Gradle logs agree with `registration-bindings-root-verification.json`:

| Spark / Delta | Executions | Failures / errors / skips | Result |
| --- | ---: | ---: | --- |
| 3.5.4 / 3.3.2 | 74 | 0 / 0 / 0 | exit 0; Spotless and Javadoc |
| 3.5.4 / 3.3.0 | 15 | 0 / 0 / 0 | exit 0; Spotless |
| 3.5.6 / 3.3.0 | 15 | 0 / 0 / 0 | exit 0; Spotless |
| 3.5.6 / 3.3.2 | 15 | 0 / 0 / 0 | exit 0; Spotless |

This is 119 successful executions across the matrix, with the expected Spark SQL, Delta Spark, and Ranger 2.8.0 jar hashes in each archived classpath.

## Review limits

- This verdict covers ordinary named-load binding and package-local resolution only. No existing logical rule consumes the registry yet, and there is no physical classifier/guard, retained-plan/action decision receipt, collection/preparation enforcement, or owner-bearing native operation/ticket in this increment.
- The installed-extension cold/warm tests establish fixture composition, not complete production SELECT enforcement. Existing native admission was intentionally unchanged and is not qualified as safe by this binding.
- Native version is evidence for the admitted analysis snapshot, not a current-head freshness guarantee. Future action validation must re-read registration metadata, apply the separate between-action version-advancement rule, and validate the retained exact log under its native operation owner.
- NullType columns dropped by Delta and schema/runtime/storage forms outside the four pinned profiles and existing canonical-path contract remain unqualified.
- The available GitNexus index predates the binding/catalog symbols, so structural conclusions come from the exact frozen sources, diff, fallback audit, and archived runtime evidence rather than a current graph index.
