# Task 2 report: optional Spark Delta discovery and runtime

## Status and commits

**Complete.** Implementation commit: `e18dafc119b6a995f47d8ec2c849f5f4631e234b`. This report is committed separately after the implementation. Only Task 2 Spark connector sources/tests, the optional runtime build, connector deployment documentation, settings registration, and this report changed. No core/API/server/client/storage-schema source, service, Ranger, demo, Task 3 harness, PR, push, or merge work was performed.

## Behavior and files

- `GravitinoSparkConfig` adds documented `spark.gravitino.enableDeltaSupport`, defaulting to false at use sites.
- `CatalogNameAdaptor` maps `lakehouse-delta` only for Spark 3.5 to `org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35`; Spark 3.3 and 3.4 return no mapping.
- `GravitinoDriverPlugin` leaves Delta discovery and native settings untouched when disabled. When enabled, before catalog discovery it validates the adapter, `io.delta.sql.DeltaSparkSessionExtension`, and `org.apache.spark.sql.delta.catalog.DeltaCatalog` without initialization; preserves and deduplicates existing extensions; sets an absent `spark.sql.catalog.spark_catalog`; accepts its exact Delta value; and rejects a conflicting value. Delta registration failures remain startup failures outside the existing per-catalog best-effort catch. There is no Delta type reference or second registry in spark-common.
- `settings.gradle.kts` and `spark-connector/v3.5/spark-delta-runtime/build.gradle.kts` add the single replacement artifact `gravitino-spark-connector-delta-runtime-3.5_2.12`. It assembles owned Spark connector projects plus one existing shaded client artifact and never nests the ordinary fat runtime. Spark 3.5.4/3.5.6 and Delta 3.3.0/3.3.2 are guarded properties; Scala is restricted to binary 2.12.
- `spark-connector.md` documents the flag. `spark-delta-catalog.md` documents replacement/runtime/native/storage coordinates, the Spark Hadoop-version rule and concrete Hadoop AWS 3.3.4 coordinate, driver/server Kernel separation, startup ordering, `catalog.update.enabled=false`, durable `journal-directory`, operation-generated UUIDs, schema admission, and explicit recovery sequencing. It identifies the four targeted pairs without claiming the packaged matrix, S3 coupling, or Ranger enforcement completed here.

## TDD and verification

All evidence is under `/private/tmp/delta-spark-runtime-task2-evidence`.

The genuine behavioral RED was preserved immediately after execution:

- `red/bootstrap-behavior.log` and `red/bootstrap-behavior-xml/`: 14 executed plugin cases, four expected failures against the compile-only scaffold.
- `red/version-3.5-behavior.log` and `red/version-3.5-behavior-xml/`: one executed mapping case, one expected failure.

`red/common-compile.log`, `red/common-missing-api.log`, and the first `red/version-3.5.log` are compile/style scaffolding failures, not behavioral RED. Their pre-existing XML was moved to the explicitly named `stale-baseline-xml-after-*` directories and is not claimed. The initial shadow build stopped at formatting (`green/shadow-jar-pre-format.log`). The first artifact/smoke iterations were superseded after supplier inspection found duplicate inputs and a standalone union-test-classpath Log4j conflict; no partial result was relabeled. The completed process is `final/runtime-smoke.log`.

Frozen-source final gates used JDK 17 and Gradle 8.2:

| Command / check | Result | Evidence |
|---|---:|---|
| `:spark-common:test` for config and driver plugin with `--rerun-tasks` | 15 / 0 failures / 0 errors | `final/spark-common-tests.log`, `final/spark-common-tests-xml-v1/` |
| Spark 3.3, 3.4, 3.5 `TestCatalogNameAdaptor` with `--rerun-tasks` | 1 each / 0 failures / 0 errors | `final/version-adaptors.log`, each selected `TEST-...TestCatalogNameAdaptor.xml` in `final/version-*-xml-v1/` |
| `:spark-common:javadoc`, Spark 3.5 compile, dependency reports, optional `shadowJar` | successful | `final/build-runtime-artifact.log` |
| Default runtime dependency assertion | no Delta or Kernel | `final/default-runtime-dependencies.log` |
| Optional runtime dependency assertion | owned project closure, no native engine dependency | `final/optional-runtime-dependencies.log` |
| Alternate Spark 3.5.6 / Delta 3.3.2 adapter compile | successful | `final/alternate-pin-compile.log` |
| Unsupported Spark, Delta, and Scala properties | all rejected | `final/reject-*.log` |
| Strict jar inspection and executable linkage smoke | successful | `final/artifact-inspection.json`, `final/runtime-smoke.log` |

The Spark 3.3 and 3.4 copied result directories also contain unrelated skipped/stale XML from the rerun task closure (43 total with 42 skipped, and 46 total with 42 skipped). The count above uses only the selected `TestCatalogNameAdaptor` XML, which is one executed passing test in each module. Source manifests before the gates and after the documentation clarification are `final/source-freeze.sha256` and `final/source-freeze-after-doc-clarification.sha256`; all hashes verify.

## Artifact and library ownership

Final artifact: `gravitino-spark-connector-delta-runtime-3.5_2.12-2.0.0-SNAPSHOT.jar`, 16,643,458 bytes, SHA-256 `e5addaaf5c7270fec9ee351bc468976ed088144dbf1d35ee9316dcb8a48ea30f`. Inspection found 10,975 entries, 10,412 classes, zero duplicate class entries, zero forbidden classes, zero unexpected Spark classes, all required connector/client/registration/relocated-library classes, and preserved credential, shaded Jackson, and Hadoop delegation-token services. Only duplicate `META-INF/LICENSE` and `META-INF/NOTICE` resources remain.

The closure uses the existing client runtime as the owner of the Gravitino client/protocol and its established shaded Jackson, Commons, HTTP client, Caffeine, and Guava namespaces. Spark, Scala, Hadoop, Delta, Kernel, server, and test classes remain provided/excluded. Kyuubi 1.11.0 retains its private Guava ABI; only its three public-suffix companions are moved into that private namespace because replacing them with the client copy produces incompatible `ImmutableMap` descriptors and a concrete `NoSuchFieldError` path.

The exact narrow Spark-package allowlist is the six `HiveBridgeHelper` classes in `root-kyuubi-helper-origins.json`. They originate from Kyuubi Hive 1.11.0, source jar SHA-256 `35d8cc6bb63cc73f4cdce18fba0b682e9a3ee9bca7da70ddae07826ac3e84cf3`, and collide with no entry across either checked 252-jar Spark 3.5.4 or 3.5.6 distribution. No broad Spark-package exception or namespace relocation exists. The final isolated-classloader smoke executes both client-owned and Kyuubi-owned `InternetDomainName.topPrivateDomain()` paths, calls `HiveBridgeHelper.Utils()`, confirms owned classes originate from the runtime, and confirms Spark/Delta/Scala/Hadoop originate externally.

## Self-review, concerns, and limits

Self-review checked startup ordering, disabled-state preservation, exact conflict handling, catch boundaries, absence of static Delta references, public Javadoc, dependency ownership, relocation ABI, services, provided-class exclusions, and documentation terminology. The documentation-only post-gate clarification does not affect compiled output; its final manifest is separately preserved.

No known Task 2 source or packaging concern remains. This task proves focused discovery/bootstrap behavior, assembly closure, artifact inspection, and standalone linkage. It does not prove actual `spark-submit` processes, the four-pair packaged matrix, S3/provider coupling, full Delta conformance, Ranger 2.8 enforcement, server deployment, recovery authorization, or physical cleanup; those remain Task 3/deployment qualification boundaries. The standalone smoke intentionally avoids constructing an adapter without an active Spark session and uses Gradle's Scala 2.12.21 test closure; real Spark 3.5.4/3.5.6 distributions provide Scala 2.12.18 and must remain the production Scala owner.
