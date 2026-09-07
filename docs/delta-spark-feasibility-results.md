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


## Task 0.2: experimental named catalog bridge

The experiment uses an embedded Gravitino server, its existing `lakehouse-generic`
provider, and two registered named catalogs (`delta_a` and `delta_b`) with separate
local warehouses. Only test sources and opt-in module wiring were added. No native
jar, core, API, REST, client, storage schema, or authorization implementation was
changed. This is **failed full-support acceptance**, even when its characterization
tests pass.

### Candidates and observed boundaries

Direct named delegation can load separately registered native data, but the exact
`CREATE TABLE delta_a.sales.orders (id BIGINT, amount BIGINT) USING delta` fails
with `SCHEMA_NOT_FOUND` in `SessionCatalog.requireDbExists`: Gravitino's `sales`
schema is absent from Spark's session catalog. The failure stack is retained. A
Java anonymous delegate also exposed an earlier native limitation:
`DeltaCatalog.isUnityCatalog` dereferences a null canonical class name. The final
experiment uses an ordinary named Java delegate; it does not impersonate Unity
Catalog.

The path-native facade first resolves the name through Gravitino and gives the
composed, unmodified `DeltaCatalog` an internal `delta.<absolute path>` identifier.
Native loading returns actual `DeltaTableV2`, never a generic table wrapper.
CREATE and staged CTAS commit Delta before publishing registration through the
existing Gravitino `TableCatalog.createTable` API. Existing registration is retained
on replacement, with its metadata limitation explicitly recorded. No drop/recreate
is used to simulate replacement or rename identity.

The exact SQL contract produces amounts `10`, `100`, and finally `100`; `delta_a`
orders ends at `15`, copy at `(2,20)`, and the native orders schema includes `note`.
Both pre-existing registration sentinels (`11`, `111`) and contract sentinels remain
separate after Spark restart. Every mutation is followed by Gravitino lookup;
runtime logs include native schema, protocol, log version, Delta metadata UUID,
Gravitino entity ID, and registry properties. The copy's native Delta metadata UUID
changes between CTAS version 0 and replacement version 1 while its Gravitino entity
ID stays constant; these are different identity concepts. The Gravitino IDs are read solely by
the test through the embedded entity store. They are absent from the REST table
properties and are **not assumed available to a production Spark adapter or
current authorization callbacks**.

The named fixture uses an in-memory Spark session catalog, no Hive/Glue catalog
configuration or service, and no second durable registration store. Its ordinary
session catalog has no `sales` or `delta` namespace. Engine-side path resolution
uses the normal `spark.sql.runSQLOnFiles` behavior; no native test switches are
set. The native-session replacement diagnostic is a separate control only.

### Staged replacement compatibility

`ReplacementStage` wraps only native replacement stage objects and preserves the
native V1 writer. Its copied capabilities add `TRUNCATE`; its `SupportsTruncate`
builder acknowledges the already selected native replace mode and forwards native
`build()` unchanged. Truncate itself performs no write or commit. CTAS is not
wrapped, and arbitrary predicate overwrite is not advertised. No loaded
`DeltaTableV2` is wrapped.

The independent regression first reproduced the unmodified Spark 3.5.6 truncate
failure, then passed with the wrapper. Tests cover existing/missing replacement,
changed schema, one commit per successful replacement, preserved prior history,
failed source query and abort, builder/build/commit counts, truncate without commit,
DataFrameWriterV2 replace/createOrReplace, and saveAsTable overwrite. A native option
quirk was also characterized: the pinned `CreateDeltaTableCommand.getOperation`
`Replace` branch omits per-write `userMetadata`, while `CreateOrReplace` passes it.
The test records the former's null history field and verifies option forwarding
through the latter's actual history value. This is not an engine fix.

The original Task 0.1 native controls and their two failed Spark 3.5.6 rows above
remain unchanged.

### Failed acceptance gates

| Gate | Observed result / failed path |
| --- | --- |
| Generic native schema refresh | Native ADD COLUMNS yields three columns; Gravitino still returns two. Provider `DeltaTableOperations.alterTable:242` rejects ALTER. |
| Existing schema-only CREATE OR REPLACE | `DELTA_CREATE_TABLE_SCHEME_MISMATCH`, through native metadata verification: path lookup supplies no existing named catalog descriptor. |
| Schema-only REPLACE | Native replacement works, but Gravitino retains the previous column metadata. Missing REPLACE is rejected before storage allocation; missing CREATE OR REPLACE creates one empty table. |
| SET LOCATION | `DELTA_CANNOT_SET_LOCATION_ON_PATH_IDENTIFIER` from `DeltaCatalog.alterTable:834`. |
| RENAME | Existing generic Delta provider rejects ALTER; registration is not dropped/recreated. |
| Managed semantics | Native creation history reports `isManaged=false`; registration explicitly uses `external=true`. No managed deletion or full native managed lifecycle is proved. |
| Canonical authorization identity | Named reads and native MERGE targets become V1 `LogicalRelation` with `catalogTablePresent=false`, `catalog=None`, `identifier=None`. A naive three-part `DeltaTableV2.tableIdentifier` fails `IDENTIFIER_TOO_MANY_NAME_PARTS` in `CatalogV2Implicits.asTableIdentifier`. |
| Automatic registration recovery | Post-commit publication failure leaves a valid, committed Delta table with no registration or durable generic reservation. |

The MERGE plan is a native `MergeIntoCommand`; its source is the literal SELECT
from the requested contract. It does **not** prove named-source authorization.
Ranger was not implemented or tested here. A synthetic, non-persisted
`CatalogTable` identity projection remains a separate experiment.

### Failure and retry evidence

| Injection | Visible registration | Native storage after failure | Retry and recovery |
| --- | --- | --- | --- |
| Before native staged commit | Absent | No Delta log | Retry creates one table and one version. |
| After native commit, before publication | Absent | One row, version 0 | Spark restart preserves the orphan. Repeating CTAS raises `DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION`, with the original row and single commit unchanged. Existing create-registration API can explicitly publish it after inspection; this is manual recovery, not an automatic protocol. |
| After publication, before success response | Present | One row, version 0 | Spark restart retains registration and its entity ID. Retry is rejected by name existence; no second identity or commit appears. |

The retry error text claims the location is not a Delta table even though the
probe reads its valid Delta row and history. Record the native error faithfully;
it does not establish storage corruption. An abort after successful native commit
cannot roll that commit back.

### Provider and native lifecycle decision

Reject the current generic provider for full Delta support. Propose a dedicated
Delta provider using existing `BaseCatalog`, `CatalogOperations`, `SupportsSchemas`,
`TableCatalog`, `ManagedTableOperations`, and provider-owned entity-store updates.
This requires no core, REST, public privilege, or storage-schema change. It must
preserve table/column identity, reconcile native versions and metadata, support
rename, and implement managed ownership deliberately.

A dedicated provider alone does not repair path-native managed history,
schema-only CREATE OR REPLACE, SET LOCATION, or authorization identity. The next
integration decision must cover the native command lifecycle as well:

* A compatibility facade can keep Gravitino authoritative and reuse the proven
  staged wrapper, but must provide a validated named metadata projection and
  explicitly adapt session-bound operations. Native command recognition and
  post-commit catalog hooks require separate proof; path substitution is not full
  parity.
* A minimal native delegate-callback patch would need more than the guarded
  CREATE callback: existence lookup, location/managed ownership, staged CTAS and
  replacement finalization, update hooks, and SET LOCATION all require coherent
  catalog routing. A CREATE-only patch is insufficient. No dependency patch is
  implemented or selected by this experiment.

The proposed provider protocol is `ABSENT -> RESERVED -> ACTIVE`, with a durable
operation token, target location, observed base log version, and committed native
version stored in existing provider properties/entity records. Reservation uses
existing create; finalization uses existing alter, with provider-side atomic
validation in the entity update callback. The generic provider cannot implement
this because ALTER is forbidden and its create immediately exposes a registration.
The protocol must:

1. Validate namespace, existing grants, registration identity, ownership/location,
   and the platform's dataset writer exclusion before reserving an operation.
2. Hide an unfinished reservation from ordinary reads, and retain the same
   registration identity while replacing an existing active table.
3. Commit native data with a durable operation token that survives retries; verify
   the resulting log version and schema before atomically activating metadata.
4. On failed/unknown publication, inspect the log and stored token after restart;
   finalize the same operation exactly once, or report a conflict. Never blindly
   repeat CTAS, delete committed data, or manufacture a fresh identity.
5. On lost success responses, return the existing completed operation. Authorize
   each transition and verify native evidence; arbitrary end-user property values
   must not assert that a commit occurred.

This is a proposal, not a demonstrated distributed transaction or recovery
implementation. Existing APIs provide no cross-storage atomic transaction or
client CAS. S3 commit guarantees, overlapping writers, stable column policy IDs,
managed purge, broad native features, and authorization remain unproven.

### Task 0.2 execution evidence

| Spark | Delta | Named test | Wrapper test | JUnit total | Full support |
| --- | --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 49.655 s | 16.310 s | 2 passed, 0 skipped | **Failed** |
| 3.5.4 | 3.3.2 | 50.870 s | 15.232 s | 2 passed, 0 skipped | **Failed** |
| 3.5.6 | 3.3.0 | 46.560 s | 15.146 s | 2 passed, 0 skipped | **Failed** |
| 3.5.6 | 3.3.2 | 50.505 s | 15.457 s | 2 passed, 0 skipped | **Failed** |

All eight final matrix tests executed with zero failures, errors, or skips. The
full-support gate fails in every row for the characterized limitations above.
Final checks and raw Gradle output, runtime logs, XML, and HTML reports are under
`build/task-0.2-evidence/`. All runtime invocations omit `-PskipITs` and use:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :spark-connector:spark-3.5:spotlessApply \
  :spark-connector:spark-3.5:test \
  -PdeltaTestSparkVersion=<spark> -PdeltaTestVersion=<delta> \
  --tests 'org.apache.gravitino.spark.connector.integration.test.delta.TestDelta*'
```

`TestDeltaReplacementStage` and `TestDeltaNamedCatalogGate` are separate tests.
The first wrapper RED and GREEN ran independently before the named bridge used
it. The failure-injection RED run recorded three missing-exception failures before
any injection hooks were implemented. Default builds exclude these Java sources
from compilation as well as execution; excluding test execution alone would not
compile direct Delta imports. Opt-in builds additionally assemble the existing
generic provider jar for MiniGravitino's isolated provider loader. The default
Delta dependency remains absent. The final default `TestCatalogNameAdaptor` check
ran one test with zero failures or skips, and default Spotless passed. Its
`compileTestJava` task was restored `FROM-CACHE`; this was a successful default
baseline, not a fresh default compilation. All four
opt-in dependency reports resolved 13 Spark artifacts to the requested patch and
both Delta Spark/storage artifacts to the requested version. Full inventories
(including Scala, Hadoop, AWS, and Kyuubi) are saved in `dependency-inventory.json`.
The requested `checkstyleTest` task does not exist in this repository; that attempt
failed task selection, not source checking. Opt-in Spotless and `-Werror` compilation
passed during the final matrix, and no additional checking tool was installed.

The experiment stays on Scala binary 2.12; confirm the deployed binary before
packaging. Spark and Delta retain their unmodified Apache-licensed jars. Any
production bridge/native dependency package still needs interface, version,
license/NOTICE, and transitive dependency review. Local ignored build evidence
must be archived externally if it needs to outlive this workspace.
