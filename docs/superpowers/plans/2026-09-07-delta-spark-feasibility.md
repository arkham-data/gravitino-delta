# Delta Spark Feasibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not dispatch agents unless separately authorized.

**Goal:** Establish an executable native baseline and determine the exact catalog and authorization interfaces needed for Gravitino-owned Delta integration.

**Architecture:** Start with unmodified native Delta on the target Spark runtime, then run the same contract against an experimental Gravitino-backed bridge. Keep experiments in test sources until the results select a production design. This package does not claim full Delta support.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java/JUnit, Gradle, existing Gravitino embedded test infrastructure, Ranger/Kyuubi.

**Spec:** [Design baseline](../specs/2026-09-07-delta-spark-design.md)

## Global Constraints

- Target all four combinations of Spark 3.5.4/3.5.6 and Delta 3.3.0/3.3.2.
- Gravitino owns registration. Native in-memory session registration is permitted only in the control test, never as the target architecture.
- Do not modify Gravitino core, public privilege definitions, storage schema, server REST contracts, or client contracts. Use existing extension interfaces.
- Ranger owns column permissions, masks, and row filters. Gravitino pushes only its existing supported grants and revocations.
- Preserve native Delta operations, named-catalog identity, and authorization semantics.
- S3 is the first-release storage; the platform excludes overlapping writer jobs per dataset.
- Local storage tests below establish engine behavior only. They do not prove S3 commit guarantees.
- Use the repository's Scala 2.12 baseline for this experiment; record it and confirm the deployed binary version before packaging.
- Follow repository Java/Javadoc/formatting rules and `TestXxx` test naming.
- All proposed commands and code below are instructions for execution; none have been run as part of planning.

---

## Task 0.1: Make the version matrix explicit and test native behavior

**Files:**

- Modify `spark-connector/v3.5/spark/build.gradle.kts`.
- Create `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/delta/TestDeltaNativeContract.java`.
- Create `docs/delta-spark-feasibility-results.md` to record actual execution evidence.

**Interfaces:**

- Consume new Gradle properties `deltaTestSparkVersion` and `deltaTestVersion`.
- Produce test JVM properties `gravitino.delta.test.sparkVersion` and `gravitino.delta.test.deltaVersion`.
- Produce resolved runtime dependency reports and JUnit results. Later tasks use the same runtime selection.

- [ ] Add the following native contract test, including the repository's ASF license header. It is intentionally a native session-catalog control, not a Gravitino acceptance test.

```java
package org.apache.gravitino.spark.connector.delta;

import java.nio.file.Path;
import java.util.Arrays;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Native Delta control for the Gravitino integration experiment. */
public class TestDeltaNativeContract {
  @TempDir Path temporaryDirectory;
  private SparkSession spark;

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void nativeLifecycleUsesRequestedRuntime() throws ClassNotFoundException {
    String expectedSpark = System.getProperty("gravitino.delta.test.sparkVersion");
    String expectedDelta = System.getProperty("gravitino.delta.test.deltaVersion");
    Assertions.assertNotNull(expectedSpark);
    Assertions.assertNotNull(expectedDelta);
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-native-contract")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.warehouse.dir", temporaryDirectory.toUri().toString())
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
    Assertions.assertEquals(expectedSpark, spark.version());
    String deltaJar =
        Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .getPath();
    Assertions.assertTrue(deltaJar.contains("-" + expectedDelta + ".jar"), deltaJar);

    spark.sql("CREATE DATABASE native_delta").collectAsList();
    spark.sql("CREATE TABLE native_delta.orders (id BIGINT, amount BIGINT) USING delta")
        .collectAsList();
    spark.sql("INSERT INTO native_delta.orders VALUES (1, 10), (2, 20)").collectAsList();
    spark.sql(
            "MERGE INTO native_delta.orders t "
                + "USING (SELECT 1L id, 15L amount UNION ALL SELECT 3L, 30L) s "
                + "ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET amount = s.amount "
                + "WHEN NOT MATCHED THEN INSERT *")
        .collectAsList();
    Assertions.assertEquals(
        65L, spark.sql("SELECT sum(amount) FROM native_delta.orders").head().getLong(0));
    spark.sql("UPDATE native_delta.orders SET amount = amount + 1 WHERE id = 1")
        .collectAsList();
    spark.sql("DELETE FROM native_delta.orders WHERE id = 2").collectAsList();
    Assertions.assertEquals(
        46L, spark.sql("SELECT sum(amount) FROM native_delta.orders").head().getLong(0));
    spark.sql("CREATE TABLE native_delta.copied USING delta AS SELECT * FROM native_delta.orders")
        .collectAsList();
    Assertions.assertEquals(2L, spark.table("native_delta.copied").count());
    spark.sql(
            "CREATE OR REPLACE TABLE native_delta.copied USING delta "
                + "AS SELECT 9L id, 90L amount")
        .collectAsList();
    Assertions.assertEquals(90L, spark.table("native_delta.copied").head().getLong(1));
    spark.sql("ALTER TABLE native_delta.orders ADD COLUMNS (note STRING)").collectAsList();
    Assertions.assertTrue(
        Arrays.asList(spark.table("native_delta.orders").schema().fieldNames()).contains("note"));
    Assertions.assertTrue(spark.sql("DESCRIBE HISTORY native_delta.orders").count() >= 5);
  }
}
```

- [ ] Run the test before build wiring. Expect failure from missing test properties or missing Delta runtime, not a passing or zero-test result:

```bash
./gradlew :spark-connector:spark-3.5:test -PskipITs --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'
```

- [ ] Replace the module's `sparkVersion` declaration with this opt-in experiment override, and add the Delta dependency only for experimental runs. Keep the existing default build behavior.

```kotlin
val deltaTestSparkVersion = providers.gradleProperty("deltaTestSparkVersion").orNull
val deltaTestVersion = providers.gradleProperty("deltaTestVersion").orNull
require((deltaTestSparkVersion == null) == (deltaTestVersion == null)) {
  "Specify both deltaTestSparkVersion and deltaTestVersion"
}
if (deltaTestSparkVersion != null) {
  require(deltaTestSparkVersion in setOf("3.5.4", "3.5.6"))
  require(deltaTestVersion in setOf("3.3.0", "3.3.2"))
}
val sparkVersion: String = deltaTestSparkVersion ?: libs.versions.spark35.get()

dependencies {
  if (deltaTestVersion != null) {
    testImplementation("io.delta:delta-spark_$scalaVersion:$deltaTestVersion")
  }
}

if (deltaTestSparkVersion != null) {
  configurations.matching {
    it.name == "testCompileClasspath" || it.name == "testRuntimeClasspath"
  }.configureEach {
    resolutionStrategy.eachDependency {
      if (requested.group == "org.apache.spark") {
        useVersion(deltaTestSparkVersion)
        because("The Delta experiment must run one Spark patch version")
      }
    }
  }
}

tasks.test {
  if (deltaTestSparkVersion == null) {
    exclude("**/connector/delta/TestDeltaNativeContract.class")
  } else {
    systemProperty("gravitino.delta.test.sparkVersion", deltaTestSparkVersion)
    systemProperty("gravitino.delta.test.deltaVersion", requireNotNull(deltaTestVersion))
  }
}
```

Insert declarations after `scalaVersion` and before `sparkMajorVersion`; merge
dependency and test-task blocks with the existing blocks. Do not add Delta to
`spark-common`, which compiles against Spark 3.3. The default exclusion makes this
opt-in experiment explicit; final release matrix tests must run it and may not
report excluded tests as passing evidence.

- [ ] Run all four pairs. Inspect each exit status and test count; preserve the report directory for each pair before the next run replaces it.

```bash
./gradlew :spark-connector:spark-3.5:test -PskipITs -PdeltaTestSparkVersion=3.5.4 -PdeltaTestVersion=3.3.0 --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'
./gradlew :spark-connector:spark-3.5:test -PskipITs -PdeltaTestSparkVersion=3.5.4 -PdeltaTestVersion=3.3.2 --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'
./gradlew :spark-connector:spark-3.5:test -PskipITs -PdeltaTestSparkVersion=3.5.6 -PdeltaTestVersion=3.3.0 --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'
./gradlew :spark-connector:spark-3.5:test -PskipITs -PdeltaTestSparkVersion=3.5.6 -PdeltaTestVersion=3.3.2 --tests 'org.apache.gravitino.spark.connector.delta.TestDeltaNativeContract'
```

- [ ] For each pair, inspect the resolved classpath. Replace the property values with that pair's values:

```bash
./gradlew :spark-connector:spark-3.5:dependencies --configuration testRuntimeClasspath -PdeltaTestSparkVersion=3.5.4 -PdeltaTestVersion=3.3.0
```

Expected: one selected Spark patch throughout Spark modules and the requested
Delta Spark/storage versions. Record Hadoop, AWS, Scala, Java, and Kyuubi versions
too. Resolve dependency conflicts before interpreting a failed SQL case as a
catalog defect.

- [ ] Format, run the targeted tests again if formatting changed code, and commit the native experimental harness separately from production code.

## Task 0.2: Test named catalog delegation without a second registry

**Files:**

- Create `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/delta/TestDeltaNamedCatalogGate.java`.
- Keep candidate bridge implementations under that test package.
- Update `docs/delta-spark-feasibility-results.md` with exact source changes, failed call paths, and native/integrated outcomes.

**Interfaces:** Consume Task 0.1's runtime selection. Use existing Gravitino
`TableCatalog` APIs for the first registration probe; produce a decision on
additional registration lifecycle interfaces only after observing the boundary.

- [ ] Start a disposable embedded Gravitino environment following the existing Spark integration tests. Create two generic catalogs, `delta_a` and `delta_b`, with separate temporary warehouse locations and a `sales` schema in each.
- [ ] Create native Delta data at both locations, register it through Gravitino with `format=delta` and `external=true`, and require different sentinel rows. This first probe establishes identity/loading only and cannot pass the full gate.
- [ ] Attempt the smallest unmodified-Delta delegation adapter against those registrations. Inspect whether native `DeltaTableV2` recognition survives resolution and whether creation consults Spark's session catalog. Capture failures with class, method, SQL, and stack trace.
- [ ] Exercise the following catalog contract for each candidate adapter. The fixture must define `delta_a` and `delta_b` as Gravitino-backed named catalogs before executing it:

```sql
CREATE TABLE delta_a.sales.orders (id BIGINT, amount BIGINT) USING delta;
CREATE TABLE delta_b.sales.orders (id BIGINT, amount BIGINT) USING delta;
INSERT INTO delta_a.sales.orders VALUES (1, 10);
INSERT INTO delta_b.sales.orders VALUES (1, 100);
SELECT amount FROM delta_a.sales.orders;
SELECT amount FROM delta_b.sales.orders;
CREATE TABLE delta_a.sales.copy USING delta AS SELECT * FROM delta_a.sales.orders;
CREATE OR REPLACE TABLE delta_a.sales.copy USING delta AS SELECT 2L id, 20L amount;
MERGE INTO delta_a.sales.orders t
USING (SELECT 1L id, 15L amount) s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT *;
ALTER TABLE delta_a.sales.orders ADD COLUMNS (note STRING);
DESCRIBE HISTORY delta_a.sales.orders;
SELECT amount FROM delta_b.sales.orders;
```

Expected amounts are `10`, `100`, and finally `100` for the explicit selects;
the target in `delta_a` ends at `15`, the copy at `(2,20)`, and its native schema
contains `note`. Query Gravitino after each mutation and restart Spark before
rechecking registrations. Verify no Glue/Hive connection and no durable secondary
registration store was used.

- [ ] Compare two implementation directions if direct delegation fails: a Spark compatibility facade whose registry is Gravitino, and a minimal native Delta delegate-callback patch. Document actual constraints for CTAS/replace, session isolation, and native command types. Do not implement a production patch or choose it implicitly.
- [ ] Inject failure before registration publication and after Delta commit. Record what is visible, what remains on storage, and whether a retry duplicates data or identity. Produce a reservation/finalization/recovery protocol using existing provider interfaces and APIs; reject candidates requiring core changes.
- [ ] Run the selected experimental candidate on the other three runtime pairs. Do not use `-PskipITs` for this package's integration-test directory:

```bash
./gradlew :spark-connector:spark-3.5:test -PdeltaTestSparkVersion=3.5.4 -PdeltaTestVersion=3.3.0 --tests 'org.apache.gravitino.spark.connector.integration.test.delta.TestDeltaNamedCatalogGate'
```

- [ ] Record the dedicated-versus-generic provider decision. Treat test-only bridge code as experimental until its interface and licensing/dependency implications are reviewed.

## Task 0.3: Establish the authorization contract before production work

**Files:**

- Create `authorizations/authorization-ranger-delta/src/test/java/org/apache/gravitino/authorization/ranger/delta/TestRangerDeltaContract.java` in the proposed extension module as characterization coverage.
- Extend the experimental Delta integration fixture for actual Spark/Ranger calls.
- Update `docs/delta-spark-feasibility-results.md` and the design baseline with the selected resource mapping and metadata/query identity contracts.

**Interfaces:** Use existing Gravitino grants, authorization extension contracts,
and Ranger policy models for characterization. Column grants use Ranger Admin
APIs directly. Produce resource and identity decisions without changing Gravitino
API or privilege contracts.

- [ ] Characterize the unchanged binding from the extension's test, using `MetadataObject` and `Privileges` imports:

```java
Assertions.assertFalse(Privileges.SelectTable.allow().canBindTo(MetadataObject.Type.COLUMN));
```

This assertion records the intended unchanged Gravitino contract. It is not a
column-enforcement acceptance test. The production package must prove column
authorization with policies authored directly in Ranger.

- [ ] Construct Ranger access, mask, and row-filter policies for identical resources. Exercise managed-policy lookup and revoke behavior through existing plugin methods and mocks. Record the multiple-policy failure and any incorrect selection/deletion path before proposing a fix.
- [ ] Run the candidate Spark bridge with Ranger and capture resource names for `delta_a.sales.orders` and `delta_b.sales.orders`. Determine whether the deployed Kyuubi service configuration supports catalog-aware resources or per-catalog service routing; neither is assumed.
- [ ] Verify a grant for `delta_a` cannot authorize `delta_b`. Record the exact mapping jointly required by pushdown, metadata lookup, and Spark authorization.
- [ ] Author a column-select policy, one row filter, and one mask directly in Ranger and execute native Delta reads through the candidate adapter. Verify allowed columns, denied columns, and policy revocation without broadening the query user's access to obtain Gravitino metadata. Capture extraction/rewrite and identity behavior as runtime results.
- [ ] Present the results and write the downstream production plans with exact interfaces, focused tests, and dependency changes. Keep the entire acceptance matrix in the delivery plan.

## Gate output and decision

The results document must contain actual values and evidence for:

| Field | Evidence |
| --- | --- |
| Runtime | Requested/resolved Spark, Delta, Scala, Java, Hadoop, AWS, and Kyuubi versions. |
| Native baseline | Command, exit status, nonzero test count, JUnit report path for each pair. |
| Catalog bridge | Native classes preserved, session-catalog calls avoided/adapted, exact patch if needed. |
| Registration | State transitions, visibility, failure injection results, retry/idempotency behavior. |
| Provider selection | Dedicated and generic comparison against identical lifecycle tests. |
| Authorization | Canonical resources, Ranger-owned column/mask/filter policies, existing metadata permissions, query identity, coexistence and runtime results. |
| Core boundary | No core/API/server/client contract or storage-schema modifications required by the candidate. |
| Production scope | Exact interfaces and file lists for downstream code-level plans. |

If the gate fails, report the failed native contract and the concrete change
needed. Do not mark the entire objective blocked solely because a first adapter
fails, and do not replace full support with external registration or reads only.
If a dependency patch or platform change is required, prepare it as a reviewable
proposal before asking to expand implementation scope.
