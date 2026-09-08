# Delta demo production handoff

This is a source-backed handoff for eventual integration into
`/Users/jacobo.calderon/Code/Arkham/catalog-demo`. It does not claim a production
Delta catalog, a deployable Spark Ranger extension, complete authorization, or full
Delta support. The inspected Gravitino feasibility worktree is
`/private/tmp/gravitino-delta-feasibility`. No demo or worktree source was changed.

## Current boundary

The demo currently downloads only Delta/S3A libraries
(`docker/spark/download-jars.sh:20-32`), copies every file in `jars/` into the Spark
image (`docker/spark/Dockerfile:27-28`), and also adds those files to a local
PySpark session (`workspaces/spark-engine/src/spark_engine/session.py:135-150,226-231`).
That is the concrete classpath insertion seam for future production artifacts.

The current session enables only `DeltaSparkSessionExtension` and the session
`DeltaCatalog` (`session.py:199-214`). It does not set `spark.plugins`,
`spark.sql.gravitino.uri`, `spark.sql.gravitino.metalake`, or a Ranger extension.
The existing `AuthorizedSession.table()` obtains a location through Gravitino and
then performs a direct Delta path read (`workspaces/ranger-client/src/ranger_client/authorized_session.py:54-119`).
`AuthorizedSession.sql()` delegates directly to the raw session and `__getattr__`
exposes the raw session surface (`authorized_session.py:153-159`). The deprecated
`read_table()` is another explicit path-read bypass (`workspaces/spark-engine/src/spark_engine/catalog.py:12-36`).

The demo is pinned to Spark 3.5.4 and Delta 3.3.0 in both its image and Python
package (`docker/spark/Dockerfile:3,31-34`; `workspaces/spark-engine/pyproject.toml:6-10`).
Its JAR downloader hard-codes Delta 3.3.0 (`download-jars.sh:20-22`). Compose does
not pass Spark/Delta build arguments to any of the three Spark-image consumers
(`docker-compose.yml:116-170`). Ranger is 2.7.0 (`docker-compose.yml:86-98`).
Therefore the required Spark 3.5.4/3.5.6 x Delta 3.3.0/3.3.2 x Ranger 2.8.0 matrix
cannot be produced by the current demo build.

## Exact production insertion points and blocking contracts

| Layer | Exact insertion point | Contract that must be implemented before demo integration |
| --- | --- | --- |
| Demo dependency set | `docker/spark/download-jars.sh:4-32` and repository `jars/` | Produce a clean, immutable JAR set per Spark/Delta cell. It must include the production Gravitino Delta catalog runtime and production Spark Ranger extension plus their reviewed runtime dependencies. JARs from different cells must never accumulate. Copying feasibility test output or a `*-tests.jar` is forbidden. |
| Spark image | `docker/spark/Dockerfile:3-4,27-34` | Parameterize both Spark and Delta versions, install the matching Python packages, and copy the production artifacts to the classpath used by driver and workers. The same image definition must identify the actual Spark, Delta, Scala 2.12, Ranger 2.8.0, Hadoop, and AWS SDK artifacts at runtime. |
| Compose matrix | `docker-compose.yml:116-170` | Pass the selected Spark/Delta build inputs to `spark-master`, `spark-worker`, and `jupyter`; do not let independently built service images drift. Replace the runnable Ranger image at `docker-compose.yml:86-98` with the pinned, observed 2.8.0 image and retain an authenticated API/version probe, not only `/login.jsp`. |
| Spark boot | `workspaces/spark-engine/src/spark_engine/session.py:153-233` | Before the first `getOrCreate()`, configure the production catalog/plugin and extension. Existing Gravitino config names are `spark.plugins=org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin`, `spark.sql.gravitino.uri`, and `spark.sql.gravitino.metalake` (`docs/spark-connector/spark-connector.md:24-47`). Preserve `io.delta.sql.DeltaSparkSessionExtension` while adding the production Ranger extension. Prove the extension is active in both local and cluster entry points. |
| Demo catalog creation | `workspaces/setup-scripts/src/setup_scripts/02_create_catalogs.py:15-38` | Replace `provider="lakehouse-generic"` only after a production Delta provider name and artifact exist. Keep Gravitino as registration owner. If grant pushdown is required, add the production provider's `authorization-provider` and Ranger properties here; the existing contract is per-catalog (`docs/security/authorization-pushdown.md:13-19,27-39`). Do not change Gravitino core. |
| Existing external fixtures | `workspaces/setup-scripts/src/setup_scripts/04_load_data.py:130-142,145-205` | These path-written, REST-registered Delta tables may remain fixture input for read acceptance, but they do not prove a native managed create/commit protocol. Queries used as acceptance must resolve a registered name, not substitute its S3 location. |
| Ranger service/policies | `workspaces/setup-scripts/src/setup_scripts/05_setup_ranger_policies.py:67-82,87-233` | The current Hive service and database/table/column policies must not be reused as evidence for the catalog-aware contract. Install/read back a Ranger 2.8 service definition whose hierarchy is `catalog -> database -> table -> column`; access/mask requests carry all four keys and row filters stop at table. Keep Gravitino-owned access policies distinguishable from Ranger-owned access/mask/filter policies. |
| Gravitino Spark catalog discovery | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/version/CatalogNameAdaptor.java:30-64,89-105` and `.../plugin/GravitinoDriverPlugin.java:144-186` | Add the production Delta provider-to-Spark-catalog mapping and class in production `src/main`. Today `lakehouse-generic` has no mapping and is skipped. This is a connector change, not a core change. |
| Gravitino Spark packaging | `spark-connector/v3.5/spark/build.gradle.kts:76-86,247-265,300-309` and `spark-connector/v3.5/spark-runtime/build.gradle.kts:32-57` | Move the selected catalog/identity and enforcement code to production sources with production dependency scopes. The normal 3.5 runtime shadow JAR includes `spark-3.5`, but Delta, Kyuubi authorization, and Ranger 2.8 are currently test dependencies only. Decide and verify the dependency boundary of the separate Ranger extension artifact; do not infer that the existing runtime JAR contains it. |
| Gravitino catalog-provider packaging | `catalogs/catalog-lakehouse-generic/build.gradle.kts:97-139` and `catalogs/catalog-lakehouse-generic/src/main/resources/META-INF/services/org.apache.gravitino.CatalogProvider:19` | A dedicated production Delta provider can use the existing CatalogProvider SPI and distribution `libs`/`conf` pattern. It needs a production service registration and package copy task. The current generic provider remains external-registration-only: ALTER and purge are unsupported (`DeltaTableOperations.java:50-80,227-267`). |
| Gravitino Ranger pushdown packaging | `authorizations/authorization-ranger/build.gradle.kts:113-136` and `authorizations/authorization-ranger/src/main/resources/META-INF/services/org.apache.gravitino.connector.authorization.AuthorizationProvider:19` | If Gravitino grants are to create the catalog-aware policies, promote a production Delta-aware authorization provider through the same ServiceLoader/distribution pattern. It must select only exact, labelled, Gravitino-owned access policies and preserve foreign Ranger access, mask, and filter policies. |

Two version facts must be made deliberate rather than inherited. The Gravitino
default dependency catalog still says Spark 3.5.3 and Ranger 2.4.0
(`gradle/libs.versions.toml:76-80,131`); the feasibility build overrides Spark and
Ranger only on test classpaths (`spark-connector/v3.5/spark/build.gradle.kts:38-50,76-86,221-245`).
A production build must demonstrate its resolved runtime closure for both Spark
patches and Ranger 2.8.0.

## Minimum automated SQL acceptance for the current bypass

Use a test-unique registered Delta table in two catalogs, for example
`delta_a.sales.orders` and `delta_b.sales.orders`, with rows
`(1,'visible','secret')` and `(2,'second','hidden')`. Give subject `reader` access
only to `delta_a/sales/orders/allowed`, apply a CUSTOM mask returning `'MASKED'`,
apply row filter `id = 1`, and give no matching access in `delta_b`. This is the
smallest existing source-backed policy shape
(`spark-connector/v3.5/spark/src/test/resources/ranger-delta-local-policies.json:1-164`).

Run each query in a fresh, authenticated Spark application whose subject is
asserted by the server/plugin. Do not switch `HADOOP_USER_NAME` inside an already
created JVM and call that multi-user isolation.

1. Through the facade bypass: `authorized.sql("SELECT allowed FROM delta_a.sales.orders")`
   must return exactly one row, `MASKED`. This proves the currently delegating
   `AuthorizedSession.sql()` is protected by engine enforcement.
2. Through the retained raw session: `spark_raw.sql("SELECT allowed FROM delta_a.sales.orders")`
   must return the identical one-row masked result. This closes the raw-session SQL
   bypass for the controlled-SQL boundary.
3. `spark_raw.sql("SELECT denied FROM delta_a.sales.orders")` must fail before any
   row is returned.
4. `spark_raw.sql("SELECT * FROM delta_a.sales.orders")` must fail; projection
   pruning must not hide the denied column from authorization.
5. `spark_raw.sql("SELECT allowed FROM delta_b.sales.orders")` must fail and the
   recorded Ranger request must contain `catalog=delta_b`. This prevents
   same-schema/table collisions across catalogs.
6. `spark_raw.sql("SELECT allowed FROM delta.`<registered-location>`")` must fail
   with missing canonical identity. Name-to-path substitution must not bypass the
   registered policy.

For all six calls, assert Ranger sees the authenticated query subject and the
canonical catalog/database/table/column, never the metadata service identity. The
existing feasibility test contains these exact positive/negative shapes and
resource assertions (`TestRangerDeltaLocalEngine.java:192-265,287-293`), but its
local JSON policy source is not deployment evidence.

This query set closes only the explicitly controlled SQL surface. The current
facade still exposes arbitrary Spark APIs (`authorized_session.py:157-159`), and
the feasibility resolution rule expressly does not prove cached/reused plans,
arbitrary file APIs, DML, history/time travel, streaming, maintenance, or metadata
confidentiality (`CatalogAwareRangerExtension.scala:31-63`; `build/task-0.3-evidence/task-0.3-report.md:186-194`).
Production must either prohibit arbitrary user code/raw `DataFrameReader` access
at the launcher boundary or add and test enforcement for those entry points. A
passing controlled-SQL suite cannot be reported as arbitrary-job security or full
Delta support.

## Test-only artifacts that cannot be deployed

All current candidate implementations below are excluded from normal production
artifacts:

- `authorizations/authorization-ranger-delta/build.gradle.kts:19-38` declares an
  explicitly test-only module with only `testImplementation` dependencies. Its
  `CandidateDeltaPolicyPlugin.java:38-123` and contract tests are under `src/test`;
  there is no production provider, ServiceLoader resource, runtime copy task, or
  distribution artifact.
- `NamedDeltaCatalog.java:54-81`, `IdentityDeltaCatalog.scala:29-49`,
  `CatalogAwareRangerExtension.scala:31-95`, `LocalRangerEngine.java:34-99`,
  `LocalRangerPolicyClient.java:32-68`, `ReplacementStage.java:34-128`, and the
  associated catalog/replacement helpers all live under
  `spark-connector/v3.5/spark/src/test`. Default builds exclude the whole Delta
  integration test source/resource tree (`spark/build.gradle.kts:247-260`).
- The S3 fixture also directly names the test `NamedDeltaCatalog`
  (`TestDeltaS3Contract.java:110-139`) and reads credentials only for a test run;
  it is evidence of fixture behavior, not a shippable catalog.
- The registration state-machine/provider experiments are likewise test-only:
  `RegistrationTestCatalog.java:61-83`, `RegistrationLifecycleFixture.java:58-59`,
  and `KernelRegistrationCommitInspector.java:41-42`. Their test ServiceLoader
  resource is under `src/test/resources`, so none can be installed as the server's
  production catalog provider.
- The feasibility Ranger policy snapshot is a local file and the client overrides
  Ranger's downloader (`LocalRangerPolicyClient.java:32-64`). Production acceptance
  must download policies from the live Ranger 2.8 service, survive its normal
  cache/refresh behavior, and fail closed on backend/authentication failure.

The blocking deliverable is therefore two reviewable production artifacts (the
Delta catalog/runtime side and the Spark Ranger enforcement side), plus any
Delta-aware Gravitino authorization-provider package needed for grant pushdown.
Their classes, SPI registrations, resolved dependencies, configuration keys, and
checksums must be inspectable before the demo image consumes them. Only then can
the six controlled-SQL probes be promoted into the demo's automated acceptance
suite and repeated across all four Spark/Delta cells against Ranger 2.8.0.
