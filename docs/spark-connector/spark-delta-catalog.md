---
title: "Delta Lake catalog with Spark"
slug: "/spark-connector/spark-delta-catalog"
keyword: "spark connector delta lake catalog"
license: "This software is licensed under the Apache License version 2."
---

## Supported runtime

Delta catalog discovery is disabled by default and is supported only with Spark 3.5 and Scala
2.12. These are the targeted dependency pairs; packaged runtime qualification is completed separately:

| Spark | Delta Lake |
|-------|------------|
| 3.5.4 | 3.3.0 |
| 3.5.4 | 3.3.2 |
| 3.5.6 | 3.3.0 |
| 3.5.6 | 3.3.2 |

Install the following connector artifact in place of the ordinary Gravitino Spark runtime. Do not
put both Gravitino runtime artifacts on the classpath.

```text
org.apache.gravitino:gravitino-spark-connector-delta-runtime-3.5_2.12:<gravitino-version>
```

Supply the selected native Delta artifact separately:

```text
io.delta:delta-spark_2.12:<3.3.0-or-3.3.2>
```

Spark, Scala, Hadoop, and Delta Lake are provided by the Spark driver deployment. Delta Kernel is
a server-side provider dependency and must not be added to the Spark driver. Use the Scala 2.12
libraries from the Spark distribution. For S3A, use `org.apache.hadoop:hadoop-aws` at the
exact Hadoop version shipped by Spark, together with its matching transitive AWS SDK. The Spark
3.5.4 and 3.5.6 distributions qualified here ship Hadoop 3.3.4, so their concrete coordinate is:

```text
org.apache.hadoop:hadoop-aws:3.3.4
```

Optional Ranger 2.8.0 enforcement has separate [SELECT results](../delta-ranger-production-results.md)
and [bounded named append results](../delta-native-governed-append.md). Follow the
[companion runtime contract](../../spark-connector/v3.5/spark-delta-ranger-runtime/README.md)
for its additional startup and identity requirements. The October 18 append matrix
uses a separately manifested local Hadoop/S3A 3.3.6 profile; the stock 3.3.4
coordinate above remains the historical base-runtime profile, not a universal
coordinate for all distributions. The append result does not qualify YARN, IAM,
all Delta operations or general cached governed queries.

## Spark configuration

Set the following properties before the first `SparkSession` is created:

```properties
spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
spark.sql.gravitino.uri=http://gravitino.example:8090
spark.sql.gravitino.metalake=production
spark.gravitino.enableDeltaSupport=true
spark.databricks.delta.catalog.update.enabled=false
spark.sql.catalog.<catalog-name>.journal-directory=/var/lib/gravitino/delta-journal/<catalog-name>
```

When Delta support is enabled, the plugin validates the optional adapter and native Delta classes,
adds `io.delta.sql.DeltaSparkSessionExtension` to `spark.sql.extensions`, and configures
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` when that setting
is absent. It preserves other extensions and an exact existing native catalog value. A conflicting
session catalog fails driver startup. Disabling the flag leaves both native settings unchanged and
does not discover Delta catalogs.

Keep `spark.databricks.delta.catalog.update.enabled=false` for the complete session lifetime. The
adapter, native Delta provider, and object-store client must resolve the same physical location and
credentials. Run only one writer job for a dataset at a time.

The `journal-directory` is a durable, driver-owned local directory. Give each discovered catalog a
separate directory and preserve it across driver recovery. Every staged operation generates its own
local journal-key UUID, registration token, and writer-run UUID. The writer run is operation
correlation evidence; it is not a configured job ID, a fencing token, or proof that a writer has
stopped.

## Schema admission and recovery

The adapter admits Spark schemas recursively, including structs nested in arrays and maps. Native
Delta column mapping `none` uses the exact admitted schema; mapping `name` binds the required Delta
v2 schema contract. Caller-selected locations, registration markers, internal column-mapping IDs,
excluded engine schema metadata, per-request storage-routing overrides, and generated, default, or
identity metadata are rejected. Qualify those features separately before enabling them.

If a registration or publication result is uncertain:

1. Stop or drain the original writer and retain both the journal and native data.
2. Read the durable journal entry and query status with its original token and writer-run UUID.
3. Reread the entry after status records any observed receipt.
4. After independently confirming request drain and matching the recorded identity, explicitly
   choose `PUBLISH`, `HANDOFF`, or `ABORT` through the authenticated recovery API.

Recovery does not replay uncertain requests, fabricate stop proof, delete native data, or bypass
authorization. Ranger 2.8 engine enforcement has separately bounded SELECT/append
qualification linked above. Broader S3/provider deployment behavior still requires
its own qualification; including this artifact does not establish it.
