# Task 2 report: native Delta schema-contract qualification

Date: 2026-09-08

Baseline: `a8d351b66c6e6aea9e62b5e6104fcde422551664`

Implementation commit: `2af351dde` (`test(spark): qualify Delta schema contract natively`)

## Scope and result

Task 2 qualifies the Task 1 schema contract against real Spark and Delta commits on the four pinned patch pairs. It adds a serverless, test-only `SchemaContractDeltaCatalog`, a native S3 qualification test, and opt-in test wiring for the production Delta provider main artifact. It does not add a production Spark catalog, provider registration transport, or runtime packaging.

The fixture owns its local name and operation maps. Its staged descriptor binds `SCHEMA_ONLY` on an untouched commit and the resolved nullable writer schema for `QUERY`. It records the marker before invoking the native stage factory. `V1_BATCH_WRITE` remains visible on every deferred route; only REPLACE and CREATE_OR_REPLACE expose `TRUNCATE`. Query CREATE_OR_REPLACE always uses native `stageCreateOrReplace` plus the existing `ReplacementStage`, including the authoritative-absence case. Schema-only CREATE_OR_REPLACE selects native create or replace from local authoritative existence. No overwrite-by-filter capability was added.

Every positive verification reopens the live S3 table through production `DeltaMetadataReader.verify`. The test compares complete path/size/SHA-256 manifests before and after verification. It copies native Delta JSON commits, runtime provenance, and manifests under `build/delta-schema-contract-evidence/native/<pair>/<run>` before deleting only the exact UUID-owned S3 prefix.

## Native entrypoints and admitted behavior

The matrix covers schema-only SQL CREATE, REPLACE, existing and absent CREATE OR REPLACE; CTAS, RTAS, and existing and absent CORAS; and DataFrameWriterV2 create, replace, and both createOrReplace existence branches. The schemas cover structs, arrays, maps, decimals, comments, column nullability, explicit name mapping, and session-default name mapping.

Mapped commits use the closed v2 contract. A deliberately v1-bound mapped native commit produces production-reader `AMBIGUOUS/SCHEMA_MISMATCH`, proving that logical-schema equality cannot qualify assigned physical names. Ordinary schema-only NOT NULL remains exact and rejects a null insert with `DELTA_NOT_NULL_CONSTRAINT_VIOLATED/23502`. Query markers use the resolved writer schema and are verified before later DML changes the candidate.

The fixture rejects supplied `delta.columnMapping.maxColumnId`, the reserved operation-marker property, collection-nested NOT NULL metadata, and conflicting `replaceWhere`/schema-evolution write options before it records a marker or creates an owned path. Raw native path-builder characterization is kept separate: with the default configuration, collection-nested NOT NULL fails as `DELTA_NESTED_NOT_NULL_CONSTRAINT/0AKDC`; with `spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled=true`, native Delta admits the table and normalizes the nested field to nullable. The closed admission contract rejects the same requested schema under both settings.

The initialized path-only delegate and native `DeltaCatalog` produce the actual DeltaTableV2/V1 writer commits. The test registry is instance-owned and exposes immutable operation-record snapshots; it never supplies a verifier callback or a synthetic `MATCHED` result.

## Four-pair evidence

All retained successful XML files have nonzero execution counts and zero skips. Later focused XML files cover source amendments without replaying unrelated native cases.

| Spark / Delta | Retained successful XML and counts (`tests/failures/errors/skips`) | Credential-free artifact runs |
|---|---|---|
| 3.5.4 / 3.3.0 | `native-final-3.5.4-3.3.0.xml` `3/0/0/0`; `native-covering-final-3.5.4-3.3.0.xml` `2/0/0/0`; `native-nested-final-3.5.4-3.3.0.xml` `1/0/0/0` | `d0c0cbca-1637-4f45-b240-3335d11e3e10`, `e6db657b-c002-4e5a-b20f-7a444731cdea`, `2a86c193-0c0f-4b7d-a61b-b03c93ad111d` |
| 3.5.4 / 3.3.2 | `native-final-3.5.4-3.3.2.xml` `3/0/0/0`; `native-covering-final-3.5.4-3.3.2.xml` `3/0/0/0` | `7ba44895-aa7c-4775-8a35-94efa7de1442`, `67a62185-aba4-4ac5-80f6-fc1880612f3e` |
| 3.5.6 / 3.3.0 | `native-final-3.5.6-3.3.0.xml` `3/0/0/0`; `native-covering-final-3.5.6-3.3.0.xml` `2/0/0/0`; `native-nested-final-3.5.6-3.3.0.xml` `1/0/0/0` | `1756f2fc-022b-4f81-9d88-c5d312f3c6b0`, `5a8a3d72-198f-428f-9b38-49a328fc7b9e`, `6a8944d3-0a84-4ec7-8239-e4a84da5d9fd` |
| 3.5.6 / 3.3.2 | `native-final-3.5.6-3.3.2.xml` `4/0/0/0`; exact NOT NULL amendment `native-schema-final-3.5.6-3.3.2.xml` `1/0/0/0`; all reserved write-option branches `native-write-options-final-3.5.6-3.3.2.xml` `1/0/0/0` | `fa4704e3-4701-42cc-aa03-0bb870fae057`, `080b4e49-989d-4931-8214-4f384bbf7995` |

The original XML and Gradle logs are preserved in `/private/tmp/delta-schema-native-task2-evidence`. Artifact directories contain copied `_delta_log/*.json`, detached runtime provenance, and before/after hash manifests. Copied commits and prior `MATCHED` results are review provenance only; each run performed verification against live S3 before cleanup.

All four runs loaded Scala 2.12.21 and Hadoop 3.3.6. Spark runtime versions matched the selected 3.5.4 or 3.5.6 coordinate. `DeltaTableV2` and `LogStore` loaded from the selected `delta-spark_2.12` and `delta-storage` 3.3.0 or 3.3.2 jars. Kernel `Table` and `DefaultEngine` loaded from the matching `delta-kernel-api` and `delta-kernel-defaults` 3.3.0 or 3.3.2 jars. Exact code-source paths are in each listed run's `runtime.properties`.

## Generated, default, and identity observations

These observations are separate from basic schema correlation.

- `CREATE TABLE delta.\`owned-path\`` with a generated expression reaches the configured Delta path topology and returns `AnalysisException/UNSUPPORTED_FEATURE.TABLE_OPERATION/0A000` on all four pairs. This result is scoped to that path SQL entrypoint. The public `DeltaTable.columnBuilder(...).generatedAlwaysAs(...)` entrypoint succeeds, computes the omitted value, and rejects an inconsistent explicit value with `DeltaInvariantViolationException/DELTA_VIOLATE_CONSTRAINT_WITH_VALUES/23001` identifying the generated-column constraint.
- Path SQL with `a INT DEFAULT 7` and the column-default feature property succeeds on all four pairs. Omitting `a` yields 7, `VALUES(DEFAULT, ...)` yields 7, and an explicitly supplied NULL remains NULL. The earlier `INSERT ... DEFAULT VALUES` parser rejection is retained as a spelling boundary, not evidence that column defaults are unsupported.
- Path SQL `GENERATED ALWAYS AS IDENTITY` returns `ParseException/PARSE_SYNTAX_ERROR/42601` on all four pairs. The public identity column builder succeeds with start 10 and step 3. It produces 10 and 13, rejects explicit 99 with `DeltaAnalysisException/DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED/42808`, exposes numeric high-water mark 13, then produces 16 and advances the numeric high-water mark to 16.

The next semantic contract work is to define closed marker versions for generated-expression and identity metadata at their supported builder entrypoints, including replacement/reset rules. Defaults need a closed requested-schema contract that preserves omission, DEFAULT-keyword, and explicit-NULL semantics. None of those families was admitted by broad metadata stripping or inferred from post-commit schema alone.

## Failure provenance

- `first-red.log` is setup-only: the Gradle wrapper lock could not be opened in the sandbox. It is not a contract RED. The first missing-class compile failure was visible only in unretained tool output and is reported as unretained.
- `native-red-3.5.4-3.3.0.xml` is `3/2/0/0`; both failures were harness-local mutations of immutable property maps before any native commit. Defensive mutable copies fixed them.
- `native-full-red-3.5.4-3.3.0.xml` is `3/1/0/0`; `INSERT ... DEFAULT VALUES` failed with the actual parser boundary. The test now probes omission and `VALUES(DEFAULT, ...)` separately.
- `native-red-3.5.6-3.3.0.xml` is `3/1/0/0`; absent SQL CORAS at the then-compiled source line failed with `Table does not support overwrite by expression`. Preserving `V1_BATCH_WRITE`, exposing `TRUNCATE` only on replacement routes, and keeping query COR on native `stageCreateOrReplace` plus `ReplacementStage` fixed both SQL CORAS and DataFrame createOrReplace without adding arbitrary predicate overwrite.
- `native-feature-assertion-red-3.5.6-3.3.0.xml` is `1/1/0/0`; native Delta correctly raised `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES`, while the provisional assertion expected a generic generated token. The final assertion checks the exact class, error class, SQLState, and generated-column constraint message.
- `native-nested-first-attempt-3.5.6-3.3.0.log` is compile-only and executed no test: `StructType.fromJson` returns `DataType` in this API. The explicit checked StructType cast compiled, and the retained focused XML then passed.

No failure was converted into an unsupported-feature success by a catch-all exception handler. Unsupported SQL results are admitted only for the exact observed type, Spark error class, and SQLState; unexpected analysis, I/O, or harness errors are rethrown.

## Commands and build isolation

The full native command used the required opt-in properties and never passed `-PskipITs`:

```text
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-3.5:test \
  --tests 'org.apache.gravitino.spark.connector.integration.test.delta.TestDeltaSchemaContractNative' \
  -PdeltaTestSparkVersion=SPARK -PdeltaTestVersion=DELTA -PdeltaKernelVersion=DELTA \
  -PdeltaS3Enabled=true -PskipDockerTests=false \
  -PdeltaS3Endpoint=http://127.0.0.1:50234 -PdeltaS3Bucket=delta-fixtures \
  -PdeltaS3CredentialsFile=/private/tmp/gravitino-delta-local-runtime/private/minio.env
```

Focused amendment runs used the same command and properties with exact method filters for `collectionNestedNotNullIsCharacterizedAndRejectedBeforeNativeBinding`, `queryAndDataFrameWriterRoutesBindResolvedSchemas`, `defaultsGeneratedAndIdentityRemainSeparateNativeProbes`, and the final schema-only method. Original command output is in the correspondingly named `*-attempt-<pair>.log` files.

Final checks:

```text
./gradlew :spark-connector:spark-3.5:spotlessJavaCheck \
  -PdeltaTestSparkVersion=3.5.6 -PdeltaTestVersion=3.3.2 -PdeltaKernelVersion=3.3.2
./gradlew :spark-connector:spark-3.5:compileTestJava
./gradlew :spark-connector:spark-3.5:dependencies --configuration testRuntimeClasspath
git diff --check
```

The formatting check succeeded. Default compile succeeded with the Delta integration-test package excluded. The default `testRuntimeClasspath` report contains no `io.delta`, Delta Kernel, or `catalog-lakehouse-delta` entry, confirming that the provider main artifact and native dependencies remain confined to the existing Delta test opt-in.
