# Named Delta lifecycle on local S3

## Result

The opt-in `TestDeltaS3Contract` passes on all four required Spark and Delta runtime pairs. It
creates a Gravitino-owned `lakehouse-generic` registration named `delta_a.sales`, while
`NamedDeltaCatalog` delegates native Delta operations by the registered S3A path. The test uses a
unique metalake and `s3a://delta-fixtures/gravitino-delta-contract/<UUID>/` prefix for every run and
deletes only those objects and metadata during cleanup.

The lifecycle performs named CTAS, append, MERGE update plus insert, copy CTAS, RTAS, `DESCRIBE
HISTORY`, and a full Spark stop/restart. It checks the exact ordered rows after every mutation,
the embedded-store Gravitino registration ID, the exact normalized registered S3A URI, native
Delta metadata IDs, retained history, and the physical `_delta_log` through S3A.

RTAS keeps the Gravitino registration ID and S3 location and advances the same Delta log history.
Delta 3.3.0 and 3.3.2 intentionally create a new native metadata UUID for replacement; the test
asserts that the UUID changes at RTAS and that the new UUID survives Spark restart. This is distinct
from the stable Gravitino registration identity.

## URI compatibility change

The executable pre-fix Spark 3.5.4/Delta 3.3.0 CTAS ran one test and failed in S3A with `bucket is
null/empty`. `java.nio.file.Paths.get(schemaLocation, tableName)` had converted the S3A location to
host-filesystem form and lost its bucket authority. `NamedDeltaCatalog` now joins the schema URI
and table name with `org.apache.hadoop.fs.Path`, which preserves both local filesystem behavior and
the `s3a://` scheme and authority. The first post-fix CTAS completed its write and publication and
also characterized `lakehouse-generic` URI normalization: registered table locations end in `/`.

The repository's macOS Docker gate normally excludes every `gravitino-docker-test` when it cannot
detect the container-IP bridge. This live contract uses a supplied localhost endpoint and does not
need that bridge. A task-scoped `gravitinoExternalTestEndpoints` marker now prevents that single tag
exclusion only after `deltaS3Enabled=true`, `skipDockerTests=false`, and all required inputs pass
validation. The actual `dockerTest` environment value remains unchanged, and ordinary test tasks
retain the default exclusion.

## Runtime matrix

The selected Delta jar version is asserted from its loaded code-source path and Spark reports its
runtime version. Each cell ran both `TestDeltaS3Contract` and the existing local
`TestDeltaNamedCatalogGate` regression.

| Spark | Delta | S3 contract | Named local regression | Gradle test time | Exit |
| --- | --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 1 passed, 0 skipped (90.771 s) | 1 passed, 0 skipped (78.021 s) | 182.456 s | 0 |
| 3.5.4 | 3.3.2 | 1 passed, 0 skipped (24.807 s) | 1 passed, 0 skipped (59.391 s) | 98.464 s | 0 |
| 3.5.6 | 3.3.0 | 1 passed, 0 skipped (36.394 s) | 1 passed, 0 skipped (58.093 s) | 109.425 s | 0 |
| 3.5.6 | 3.3.2 | 1 passed, 0 skipped (43.165 s) | 1 passed, 0 skipped (70.898 s) | 130.348 s | 0 |

All eight final test executions have zero failures and errors. The Spark 3.5.6 RTAS results use the
existing `ReplacementStage` compatibility wrapper. They do not establish unmodified native Spark
3.5.6 RTAS parity.

The matrix command for each runtime pair was:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :spark-connector:spark-3.5:test \
  -PdeltaTestSparkVersion=<3.5.4|3.5.6> \
  -PdeltaTestVersion=<3.3.0|3.3.2> \
  -PdeltaS3Enabled=true \
  -PdeltaS3Endpoint=http://127.0.0.1:50234 \
  -PdeltaS3Bucket=delta-fixtures \
  -PdeltaS3CredentialsFile=/private/tmp/gravitino-delta-local-runtime/private/minio.env \
  -PskipDockerTests=false \
  --tests 'org.apache.gravitino.spark.connector.integration.test.delta.TestDeltaS3Contract' \
  --tests 'org.apache.gravitino.spark.connector.integration.test.delta.TestDeltaNamedCatalogGate'
```

The local object store was MinIO `RELEASE.2025-07-18T21-56-31Z`, pinned as
`minio/minio@sha256:da0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac`.
The endpoint and bucket are supplied properties. Credential values are read only from the private
properties file, are never passed as Gradle arguments, and are absent from the evidence files.

Raw final logs, preserved pre-fix RED XML, development characterizations, and final per-class XML
are under `build/delta-s3-ranger-live-evidence/task-1/`.

## Limits

This test proves sequential S3A I/O, Delta log/data persistence, Gravitino-owned named
registration, and recovery in a new Spark session against broad local MinIO credentials. It does
not join the managed reservation fixture to native commits, prove S3 IAM or TLS, establish
production identity propagation, or cover all Delta operations. It adds no Glue or Hive
registration authority. One writer job per dataset remains an assumption; there is no distributed
fencing or native correlation claim.
