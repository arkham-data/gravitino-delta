# Immutable published-registration reader

Status: DONE — bounded reader foundation complete; independent review pending.
Base supplied by root: `61a2a676f142f059a0e8f6f0657dd6260207c293`.
Build/source publication slot was granted after native characterization, then released immediately after the final GREEN sealed. No further Gradle work is pending.

## Owned change

Only these three source/build files were published:

- `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationReader.java` (new).
- `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerRegistrationReader.java` (new).
- `spark-connector/v3.5/spark-delta-ranger/build.gradle.kts:59`: one added `compileOnly(project(":delta:delta-common"))` line; existing test dependency remains.

Private staging preceded source publication: `task-4-evidence/registration-reader-staged/` contains tests, the intentionally empty API scaffold, and the one-line dependency patch. It is historical staging, not the final source. No metadata, engine, native fixture, Scala, catalog or core sources were edited. Comparing the first RED and final source manifests shows changes only to the owned new reader and test after publication (the dependency line was already present in the first capture). No live calls, Docker actions, commits or subagents were used.

## API and authority

Package-private final `DeltaRangerRegistrationReader` binds the existing exact metadata client and trusted canonical metalake/catalog. Its constructor validates the configured literal names (`:49`). The minimal package-local API is `PublishedObservation read(Identifier)` (`:56`) and `void requireLive(@Nullable PublishedObservation)` (`:102`). No new credential/client construction or unscoped `get()` call is introduced.

`read` validates exactly one literal namespace and a literal table name before any I/O. It then invokes the accepted `metadata.read` callback, captures the already-started exact lease stamp, checks returned catalog name/provider (`lakehouse-delta`), loads the table by `NameIdentifier.of(database, table)`, and checks returned table name. Receipt decoding, schema conversion and immutable observation construction all occur inside that scope; its final deadline/close check covers the entire callback. The trusted constructor metalake is retained exactly, never inferred from a SQL alias.

Receipt decoding uses existing `DeltaRegistrationReceipt.fromProperties` and requires PUBLISHED plus present location/native ID/nonnegative version. The immutable existing receipt/identity types are reused. Registered location is retained exactly as the provider-canonical receipt value: no alias normalization or path-derived identity. Native ID remains opaque and may be non-UUID.

The nested final `PublishedObservation` (`:175`) stores only final canonical strings, the immutable receipt, immutable schema JSON, a private plain owner identity object, and the exact private metadata lease stamp. It retains no reader/client, catalog/table/column DTO, mutable array/map, Spark relation or converter. Value getters are package-local. Its rendering is constant and it implements neither Serializable nor Scala Product.

`samePublication` (`:245`) compares exact reader identity, all canonical names, complete immutable receipt properties (including native version and operational/evidence fields), and schema JSON. It deliberately does not infer current liveness or define action-incarnation compatibility. `requireLive` separately rejects null, another reader (including one sharing the same client), another client, expiry or closure; it delegates the exact stamp check to the existing metadata owner without renewal.

Read/liveness failures from RuntimeException or LinkageError become a constant SecurityException with no upstream cause or suppressed exception. Upstream tokens, locations and credential text are not copied into the denial. Fatal VM errors outside LinkageError are not converted.

## Schema projection

`schemaJson` (`:113`) uses the existing public `SparkTypeConverter` for each Gravitino type, preserving existing scalar semantics, including unsigned long to decimal(20,0) and UUID to string. It validates non-null/nonempty columns, non-null column/type/name, nonblank names and exact duplicate names. Recursive rebuilding (`:132`) strips all Spark field metadata, including comments on nested structs inside arrays/maps; top-level comments are excluded consistently. Field order, names/types/nullability, array element nullability and map value nullability remain unchanged. Nested null/blank/duplicate fields reject. Only the final deterministic JSON String is retained; no mutable Spark StructType is stored or exposed.

The schema is a logical projection, not a storage schema or native Delta head proof. Unsupported converter types are sanitized denials rather than dropped fields or synthesized types. Literal wildcard restrictions apply to identifiers, not schema field names.

## Verification and original evidence

All commands ran offline with Java 17, Spark 3.5.4, Delta 3.3.2 and the existing Ranger 2.8.0 dependency profile. Every run used `task-4-evidence/run-gradle.py`, the existing `task-3-evidence/fix-classpath-capture.gradle`, `-PskipITs --offline --console=plain --max-workers=2 --no-parallel`, and `-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2`. Exact argv is preserved in each run's `command.json`; no logs or XML were rewritten.

| Evidence directory under `task-4-evidence/` | Result | Meaning |
| --- | --- | --- |
| `registration-reader-red-1` | exit 1; no test XML or Test.doFirst input capture | Setup failure: Error Prone rejected an always-false `Class.isInstance` expression on the final observation type in the non-serialization test. Changed the test to class assignability inspection. This is not behavioral RED. |
| `registration-reader-red-2` | exit 1; original XML 19 tests / 19 failures / 0 errors / 0 skipped | Behavioral RED against the minimal empty API scaffold. The positive published read failed `assertNotNull`; denial tests failed because nothing rejected; remaining value tests failed when dereferencing the absent observation. |
| `registration-reader-green-1` | exit 0; original XML 19 / 0 / 0 / 0 | Implemented reader passes all focused tests. Module spotlessApply and Javadoc executed successfully. Its pre-run source is before automatic formatting; actual compiled test inputs are captured. |
| `registration-reader-green-final` | exit 0; original XML total 38 / 0 / 0 / 0 | Exact already-formatted final sources: reader 19 / 0 / 0 / 0 plus existing metadata-client 19 / 0 / 0 / 0. Module spotlessCheck passed; Javadoc was up-to-date from the successful preceding run. |

The final command is equivalent to this exact wrapper invocation (argv is also retained in `registration-reader-green-final/command.json`):

```sh
python3 .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/run-gradle.py registration-reader-green-final \
  -I .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix-classpath-capture.gradle \
  -Ptask3FixEvidenceOutput=.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/registration-reader-green-final \
  :spark-connector:spark-delta-ranger-3.5:spotlessCheck \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerRegistrationReader' --tests '*TestDeltaRangerMetadataClient' \
  :spark-connector:spark-delta-ranger-3.5:javadoc \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2 -PskipITs \
  --offline --console=plain --max-workers=2 --no-parallel \
  '-Dorg.gradle.jvmargs=-Xmx2g -XX:ActiveProcessorCount=2'
```

Fresh original XML is under each run's `spark-connector-v3.5-spark-delta-ranger-xml/`. Source copies and SHA256 values are in `source/` and `source-hashes-before.json`; actual executed compiled directories and local jars are retained by `execution-inputs/` and `execution-classpath.json` captured in Test.doFirst after artifact-producing tasks; dependency jars outside the worktree are hashed there. `artifact-hashes.json`, `exit.json` and full `gradle.log` complete the record. Existing Gradle deprecation and worktree Git-ID warning output remains in private logs; neither prevented the checks.

The focused tests cover valid/repeated read, exact canonical names, nested ordering/nullability and scalar converter semantics, recursive comment exclusion, mutation of original maps/column arrays/nested type arrays after detachment, every incarnation/native identity/location/version change plus full receipt fields, logical schema changes, wrong provider/catalog/table, malformed/pending/missing receipt, malformed schema, invalid input before I/O, wrong reader/client, closed/expired lease without renewal, expiry/closure during table I/O and schema decoding, sanitized upstream/converter failures, redacted non-serialization and unchanged query UGI with only the existing owned read client.

## Frozen source SHA256

All three hashes below match both current source bytes and the final GREEN pre-run source archive:

| File | SHA256 |
| --- | --- |
| `DeltaRangerRegistrationReader.java` | `2a019ec590b9c19acb9d9d38658d316859bf2f5868aed671b911ddb35a0ff8a7` |
| `TestDeltaRangerRegistrationReader.java` | `58d167f9f0c3396865787765f6ab8046110c59206a368f13819dd1cb0f44e894` |
| `spark-delta-ranger/build.gradle.kts` | `dfab7f89357f4347b6620825bbb2aec26839318ba005016f787df3d9f7fefe81` |

## Integration limits

This is typed metadata groundwork only. There is no named native catalog binding, native scan provenance proof, fresh Delta head comparison, location policy, physical scan guard, action receipt integration, or production authorization completion. Liveness is an explicit instantaneous check; comparing immutable values does not revoke already-returned metadata or make metadata and policy updates distributed-atomic. This reader does not synchronize arbitrary external mutation of provider DTOs during a read, and does not add runtime-transitive dependencies. Native/cache characterization and reviewed foundations remain owned and frozen separately. No Task 4 or full Delta completion is claimed.
