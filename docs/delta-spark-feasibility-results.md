---
title: "Delta Spark feasibility results"
license: "This software is licensed under the Apache License version 2."
---

# Delta Spark feasibility results

These results were produced on September 7, 2026 by the opt-in native Delta
control in `TestDeltaNativeContract`. The control uses Spark's in-memory session
catalog and does not exercise the Gravitino catalog.

## Runtime matrix

All rows ran with Java 17.0.16, Gradle 8.2, Scala binary 2.12 (resolved Scala
library 2.12.21), Hadoop 3.3.6, AWS SDK 2.31.73, and Kyuubi 1.11.0. Each Gradle
dependency report completed successfully and selected one Spark patch version
throughout the Spark modules plus the requested `delta-spark_2.12` and
`delta-storage` versions.

| Spark | Delta Spark/storage | JUnit result | Native test time | Result |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 1 test, 0 failures, 0 skipped | 49.538 s | Pass |
| 3.5.4 | 3.3.2 | 1 test, 0 failures, 0 skipped | 40.986 s | Pass |
| 3.5.6 | 3.3.0 | 1 test, 1 failure, 0 skipped | 32.912 s | Fail at RTAS |
| 3.5.6 | 3.3.2 | 1 test, 1 failure, 0 skipped | 35.482 s | Fail at RTAS |

The Spark 3.5.4 rows passed create, insert, merge, update, delete, CTAS,
`CREATE OR REPLACE TABLE AS SELECT` (RTAS), schema evolution, and history. Both
Spark 3.5.6 rows passed through CTAS and then failed RTAS with:

```text
AnalysisException: Table copied does not support truncate in batch mode.
OverwriteByExpression ... spark_catalog.native_delta.copied ... true, false
```

The 3.5.6 dependency reports resolve every displayed Spark module to 3.5.6, so
the failure is not caused by mixed Spark patch versions. Spark 3.5.6 changed the
staged RTAS write from `AppendData` to `OverwriteByExpression(TRUE)`. Delta 3.3.0
and 3.3.2 advertise only `V1_BATCH_WRITE` on the staged table and do not provide
the truncate support required by that Spark plan. The same upstream failure is
reported in [delta-io/delta#4671](https://github.com/delta-io/delta/issues/4671).
The native control and its two failed rows remain unchanged; a compatibility
adapter must be tested separately.

## Execution evidence

The test-first run before Gradle wiring executed one test and failed at the first
`assertNotNull` because `gravitino.delta.test.sparkVersion` was absent. After
wiring, the initial Spark 3.5.4 / Delta 3.3.0 run exposed incorrect planned sums:
the merge produces `15 + 20 + 30 = 65`, and update/delete produces `16 + 30 =
46`. The controller approved correcting the planned `55`/`36` expectations to
`65`/`46`; the corrected row then passed.

Each matrix row used this command shape with the table values substituted:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :spark-connector:spark-3.5:test -PskipITs \
  -PdeltaTestSparkVersion=<spark> -PdeltaTestVersion=<delta> \
  --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'

JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :spark-connector:spark-3.5:dependencies \
  --configuration testRuntimeClasspath \
  -PdeltaTestSparkVersion=<spark> -PdeltaTestVersion=<delta>
```

The preserved local reports are under `build/task-0.1-evidence/<spark>-<delta>/`
with `html-report/` and `xml-results/` subdirectories. Build output is ignored by
Git and should be archived externally when durable evidence is required.

Supplying only `deltaTestSparkVersion` fails configuration with `Specify both
deltaTestSparkVersion and deltaTestVersion`. Supplying Spark 3.5.5 with Delta
3.3.0 also fails configuration. With neither property, a direct filter for the
native control reports no tests because the class is explicitly excluded; this
excluded result is not counted as a passing matrix row. The default-mode
`TestCatalogNameAdaptor` baseline passed before and after the wiring (one test,
zero failures).

This local-filesystem native control does not validate Gravitino registration,
Ranger authorization, S3 commit behavior, or concurrent writers.
