# Task 4 production collection-boundary primitive

Status: **DONE** for this bounded, uninstalled component. This does not complete Task 4 or qualify production authorization.

## Change

Added only:

- `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerCollectGuardExec.scala`
- `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerCollectGuardExec.scala`

Final SHA-256 values:

- production: `8a53473a76759878312ab36eeb08ee5b8e607ff9fbddd05044f2b88d2d30c5d1`
- test: `af4b042bbcbd5dffdb32f16520904607154dc29506d162ad43064fa4dd3f45b8`

Both final evidence snapshots and the working files match these hashes. The module formatter left all eight frozen registration-binding files equal to their sealed hashes. No build file, shared fixture, extension, service, Docker resource or commit was changed.

## Minimal API and behavior

The new package-local `DeltaRangerCollectGuardExec` is an ordinary final `UnaryExecNode`. It preserves the current child's output, partitioning and ordering and adds no code-generation or columnar contract.

Its package-local `DeltaRangerCollectValidationContext` is an ordinary trait, not a case class, `Product` or declared `Serializable`. Its two methods receive the actual current child:

- `validateCollection(child)` is called at each supported `executeCollect` entry, before `super.executeCollect` can prepare or execute the child.
- `validateExecution(child)` is called in `doExecute` as a defense in depth before `child.execute`.

Both paths first reject a canonicalized guard and an absent/deserialized transient context with sanitized `IllegalStateException` messages. The constructor, Spark copy paths and rendering perform no validation or I/O.

Only the child is in the case-class Product surface. `otherCopyArgs` and `withNewChildInternal` retain the live driver context for normal Spark copies while requiring the replacement child to be validated. The explicit `jsonFields` filter removes `driverContext`; the transient field is omitted from Java serialization. The ordinary `cleanupResources` override delegates to Spark's inherited visible-child traversal.

## Focused coverage

The four tests exercise the production guard directly:

1. A retained real Dataset collects exact rows through the same executed guard root with a visible `WholeStageCodegenExec` descendant. Every collection entry is counted independently; stale entry attempts deny before additional defense, preparation or child execution, and restoring the unchanged context permits collection again. The test explicitly sets AQE false and confirms the guard does not change that setting.
2. A stale first collection denies before a real preparation-time `SparkContext.broadcast`. An allowed downstream failure creates the broadcast, after which inherited cleanup reaches the child and destroys it.
3. Normal clone and `withNewChildren` copies preserve context without validating during construction. The actual unchanged-source child succeeds, an unexpected replacement child fails before preparation, and a canonicalized guard fails before callback or child work.
4. Actual Java serialization succeeds for a guard with a serializable minimal child while omitting a deliberately non-serializable, sensitive context. The deserialized guard fails closed before preparation/execution. Product, string, tree, JSON and explain renderings omit the context, its marker and the `driverContext` field.

The repeated-collection assertions distinguish mandatory collection-entry validation from Spark's cached `executeRDD`/`doExecute` lifecycle: successful repeated actions need not rerun the defense or native child execution.

## TDD evidence

The meaningful unchecked baseline is archived at `task-4-evidence/collect-guard-red-354-330-6`. Its production source hash was `84a1619a84d0fe84e624d5c56cbe5225326c6338a66213b85c8036c95bf5cb46`; the test already had its final hash. Compilation and formatting passed, then the original XML recorded 4 tests, 4 failures, 0 errors and 0 skips:

- retained and copied plans recorded zero validation;
- stale first collection did not throw before downstream work;
- the deserialized guard produced a null dereference instead of the required sanitized fail-closed exception.

Earlier non-behavioral attempts remain separate and are not counted as RED:

- `collect-guard-red-354-330`: sandbox denied the Gradle wrapper lock before build execution;
- `collect-guard-red-354-330-2`, `-3` and `-5`: Spotless stopped the build before compilation/tests;
- `collect-guard-red-354-330-4`: test compilation rejected reference identity on a statically typed `Any`; no test ran.

## Final verification

| Evidence | Runtime | Result |
| --- | --- | --- |
| `task-4-evidence/collect-guard-final-354-330` | Spark 3.5.4 / Delta 3.3.0 / Ranger 2.8.0 | exit 0; 4 tests; 0 failures/errors/skips; module SpotlessCheck and Javadoc passed |
| `task-4-evidence/collect-guard-final-356-332` | Spark 3.5.6 / Delta 3.3.2 / Ranger 2.8.0 | exit 0; 4 tests; 0 failures/errors/skips; module SpotlessCheck passed |

Each final directory retains the exact command, exit, full Gradle log, original XML, artifacts, pre-run source/hash snapshot, `execution-classpath.json` and archived `execution-inputs/`. The captured test classpaths contain the stated Spark SQL and Delta Spark versions plus Ranger plugins-common 2.8.0. Both runs used the standard capture init script and output property, `--offline`, `--console=plain`, `--max-workers=2`, `--no-parallel`, a 2 GiB heap and `-XX:ActiveProcessorCount=2`. The new API is package-local Scala; it adds no public Java API requiring Javadoc.

`collect-guard-green-354-330-1` and `collect-guard-green-356-332-1` also passed the same final sources with original XML 4/0/0/0 and SpotlessCheck. They are retained as successful intermediate evidence only: their commands omitted the compiled-input capture and final bounded offline flags, so they are not the final qualification artifacts.

## Limits

The guard is deliberately not installed by an extension and its validation context has no production implementation in this change. These tests prove the collection-boundary mechanics, copy/serialization/rendering behavior and test-child pre-prepare ordering. They do not prove a complete typed decision receipt, real native Delta physical provenance, AQE enforcement, arbitrary raw Spark actions, task-closure safety, user cache revocation, production retained-plan enforcement or the Task 4 matrix.
