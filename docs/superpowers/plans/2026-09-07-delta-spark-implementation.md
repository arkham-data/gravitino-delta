# Delta Spark Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not dispatch agents unless separately authorized.

**Goal:** Deliver full native Delta support through Gravitino and Spark, with Gravitino-owned registration and Ranger authorization, on the confirmed S3 deployment.

**Architecture:** A dedicated Delta provider is the preferred server boundary, with a Spark 3.5 adapter preserving native Delta execution and a Ranger metadata lookup adapter. The first work package proves catalog delegation and fixes the interface decisions before production implementation. Gravitino registration and Delta commits have distinct ownership.

**Tech Stack:** Java, Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Gravitino entity store and REST APIs, Ranger, Kyuubi Spark authorization, S3, Gradle, JUnit.

**Spec:** [Design baseline](../specs/2026-09-07-delta-spark-design.md)

## Global Constraints

- Support Spark 3.5.4 and 3.5.6 with Delta 3.3.0 and 3.3.2. Validate all four combinations as the planning assumption.
- Gravitino owns registration and named-table resolution. No Glue or independently authoritative Hive metastore in the target.
- Gravitino authors its existing supported grants and revocations; Ranger owns column permissions, masks, and row filters using Gravitino metadata.
- Do not modify Gravitino core, public privilege definitions, storage schema, server REST contracts, or client contracts. Use existing APIs and catalog, Spark connector, and authorization extension interfaces.
- S3 is the first-release storage backend; registration must remain independent of AWS catalog services.
- The platform prevents concurrent writer jobs for the same dataset. All log-mutating paths must preserve that constraint.
- Preserve native Delta behavior and the complete acceptance scope. Registration-only support is not completion.
- Repository Scala baseline is 2.12; confirm deployed binary compatibility before packaging.
- English code/docs, normal imports, Google Java Style, Javadoc on new public/protected APIs, nullable annotations, SLF4J, and try-with-resources.
- New logic requires unit tests named `TestXxx`; Docker-dependent tests use `@Tag("gravitino-docker-test")`.
- Show issue/PR previews and obtain user confirmation before submission. This plan authorizes neither production cutover nor deletion of Glue resources.

---

## Plan status and decomposition

This is the complete delivery sequence and acceptance map. The linked feasibility
package is detailed enough to start. Later packages have concrete boundaries,
files, and exit criteria here; their code-level plans must be written against the
interfaces produced by the feasibility gate. Inventing a working native bridge
or transaction protocol in advance would make those instructions unreliable.

Execute packages in this order. No release or full-support claim occurs at an
intermediate milestone. Each production package gets its own focused plan and
test/review cycle before execution; this avoids one coupled change spanning
catalog storage, Spark planning, and Ranger service extensions.

## Package 0: Native compatibility and interface decisions

**Plan:** [Executable feasibility package](2026-09-07-delta-spark-feasibility.md).

**Produces:** Runtime dependency reports; native and Gravitino contract results;
selected provider; exact Spark bridge classes/interfaces; registration transaction
protocol; canonical Ranger resource mapping; authorization gap report.

- [ ] Execute the native baseline and version-matrix checks.
- [ ] Prove two-catalog isolation and native create/CTAS/replace/merge behavior.
- [ ] Exercise the Ranger resource identity and policy-coexistence boundary.
- [ ] Record the bridge decision and write the production package plans from it.

**Gate:** No provider scaffold is considered a successful outcome. If unmodified
Delta cannot satisfy the contract, present the exact required upstream change and
its maintenance cost before widening the dependency scope.

## Package 1: Gravitino-owned Delta registration and snapshot metadata

**Depends on:** Package 0 registration lifecycle and provider decision.

**Proposed production files for a dedicated provider:**

- Create `catalogs/catalog-lakehouse-delta/build.gradle.kts`.
- Create `catalogs/catalog-lakehouse-delta/src/main/resources/META-INF/services/org.apache.gravitino.CatalogProvider`.
- Create `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaCatalog.java` for provider identity and contracts.
- Create `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaCatalogOperations.java` for schema/table routing.
- Create `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaRegistrationOperations.java` for entity-store lifecycle and stable identity.
- Create `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaSnapshotReader.java` for native schema/protocol projection.
- Create `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaCatalogCapability.java` and `DeltaCatalogPropertiesMetadata.java` in that package.
- Modify `settings.gradle.kts` and catalog distribution wiring, following `catalogs/catalog-lakehouse-generic/build.gradle.kts`.
- Tests: `TestDeltaRegistrationOperations`, `TestDeltaSnapshotReader`, and `TestDeltaCatalogCapability` under the new module's matching test package.

**Interfaces:** Consume the exact reserve/finalize/abort/recover protocol selected
in Package 0, implemented using existing APIs and provider extension points.
Produce existing Gravitino `TableCatalog` behavior and a versioned snapshot
projection. No new core REST operations or entity-store schema changes are
allowed. Reject a lifecycle design that requires them and revise the adapter or
provider approach without reducing its consistency guarantees.

- [ ] Write entity-store tests for duplicate registration, namespace/location precedence, stable identity, and external versus managed ownership.
- [ ] Write snapshot tests using actual Delta logs for schema evolution, column mapping, feature metadata, and inconsistent/unreadable locations.
- [ ] Implement registration and metadata loading without starting a Spark engine in the server or silently mutating the Delta log.
- [ ] Add fault injection at the selected lifecycle boundaries and verify retry/idempotency, visibility, and recovery.
- [ ] Verify rename preserves location and policy identity; drop/recreate does not inherit stale grants.
- [ ] Run module tests, formatting, and checks; review and commit this package.

**Expected evidence:** Existing external data/history remains unchanged after
registration. Native schema changes appear through Gravitino without manual
drop/recreate. Failed native creation is not published as a healthy table.

## Package 2: Spark catalog integration and table lifecycle

**Depends on:** Packages 0 and 1.

**Files:**

- Create `spark-connector/v3.5/spark/src/main/java/org/apache/gravitino/spark/connector/delta/GravitinoDeltaCatalogSpark35.java` and the bridge classes fixed by Package 0.
- Modify `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/version/CatalogNameAdaptor.java`.
- Modify `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/plugin/GravitinoDriverPlugin.java`.
- Modify `spark-connector/v3.5/spark/build.gradle.kts` and `spark-connector/v3.5/spark-runtime/build.gradle.kts`.
- Test `TestGravitinoDeltaCatalogSpark35` in the matching versioned test package.
- Test `TestDeltaCatalogLifecycle` under `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/delta/`.

**Interfaces:** Consume the Gravitino registration contract and emit native Delta
tables/plans as demonstrated by Package 0. Keep Delta 3.3-dependent code in the
Spark 3.5 module; `spark-common` currently compiles against Spark 3.3.

- [ ] Write provider mapping and plugin tests for enablement, extension ordering, duplicate names, and coexistence with Iceberg.
- [ ] Add native-versus-integrated create, CTAS, replace, alter, rename, and drop tests using the same SQL and expected rows.
- [ ] Implement the proven catalog bridge and lifecycle protocol; route named resolution through Gravitino.
- [ ] Test session restart, two catalogs containing `sales.orders`, default catalog changes, and invalidation after schema/ownership changes.
- [ ] Test denied and aborted creation/replace before and after the commit boundary; inspect both registration and Delta history.
- [ ] Run all four runtime pairs and existing Iceberg connector regression tests; review and commit.

**Gate:** No hidden Glue/Hive registration, temporary session metadata masquerading
as durable Gravitino registration, or read-only fallback.

## Package 3: Full native Delta operations and table features

**Depends on:** Package 2.

**Files:**

- Add focused tests under `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/delta/`: `TestDeltaDml`, `TestDeltaHistory`, `TestDeltaStreaming`, `TestDeltaMaintenance`, and `TestDeltaTableFeatures`.
- Add fixtures under `spark-connector/v3.5/spark/src/test/resources/delta/`.
- Modify only the Delta bridge/planner classes selected in Package 0 when native cases expose gaps.
- Create `docs/delta-spark-support-matrix.md` as the versioned capability/evidence inventory.

**Interfaces:** Existing Spark SQL, DataFrame, and Delta APIs; preserve native
schema, rows, errors, history, and operation semantics.

- [ ] Enumerate features and commands from pinned 3.3.0/3.3.2 sources; record native support and defaults per version.
- [ ] Add positive and negative cases for append/overwrite, partition overwrite, merge/update/delete, and schema enforcement/evolution.
- [ ] Add time travel, history, restore, change data feed, and supported conversion/clone/maintenance operations.
- [ ] Add streaming source/sink, checkpoint restart, schema changes, and sequential writer handoff.
- [ ] Cover applicable protocol features, constraints, defaults, generated/identity columns, column mapping, deletion vectors, clustering, type widening, row tracking, feature changes, and UniForm.
- [ ] Preserve native errors for features absent from a specific runtime; show the native control proving that absence.
- [ ] Run the complete four-pair matrix; review and commit.

**Gate:** No skipped or ignored failing case can be counted as full support.
Planner incompatibilities must be fixed or brought back as explicit design
failures without removing the requirement.

## Package 4: Ranger-owned fine-grained policies and pushdown coexistence

**Depends on:** Package 0 resource mapping; integrates with Packages 1–3.

**Files:**

- Extend the test-only `authorizations/authorization-ranger-delta/build.gradle.kts` module with production provider packaging and its service registration resource after the feasibility and deployment gates pass.
- Create `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/DeltaRangerAuthorizationProvider.java` and `DeltaRangerAuthorizationPlugin.java` implementing the existing extension contracts.
- Add `TestRangerPolicyCoexistence` in the new module's matching test package and `TestRangerDeltaColumnAccess` in the Spark Delta integration-test package.
- Read existing Ranger client and translator implementations as reuse references. Do not modify core/API/server/client contracts or shared Ranger plugin behavior for this feature.
- Add only the new module's build, distribution, provider configuration, and documentation wiring.

**Interfaces:** Existing `AuthorizationProvider` and `AuthorizationPlugin`
callbacks consume Gravitino's supported grants. Ranger Admin APIs manage column
policies directly. Gravitino-managed and Ranger-owned policies share canonical
resource identity but have distinct ownership; access policies also need
ownership discrimination because both systems can create that policy type.

- [ ] Author a column-select policy directly in Ranger for a user without a broad table-read grant; assert allowed columns succeed and denied columns fail through Spark.
- [ ] Revoke the column policy in Ranger and verify denial after the measured policy-refresh boundary. Gravitino's API and stored privileges remain unchanged.
- [ ] Add same-resource Gravitino access and Ranger column/mask/filter fixtures. Change Gravitino grants for a separate subject while asserting Ranger-owned policies remain byte-equivalent except for documented resource rename behavior.
- [ ] Implement policy selection, revoke/delete, ownership updates, rename, and drop/recreate within the new authorization provider against those fixtures.
- [ ] Verify metadata loading under existing permissions. If a scoped metadata service identity is used, assert it is never used as the end-user Ranger query subject and cannot confer table-wide data access on that user.
- [ ] Test broad table grants and column-specific policies together so effective access follows Ranger's actual combining rules; do not assume a narrow allow overrides an inherited broad allow.
- [ ] Test failed Ranger writes, retries, and convergence through existing callbacks; keep recovery logic within extension interfaces, then review and commit.

**Gate:** Verify Gravitino grant → pushed policy → query and Ranger column
grant → query → Ranger revoke → denial as separate paths. No public privilege
extension, core authorization change, or broad end-user grant is permitted as a
shortcut for column-only access.

### Exact production follow-up from Task 0.3

- Add `META-INF/services/org.apache.gravitino.connector.authorization.AuthorizationProvider`
  naming the new provider. Implement `shortName()` as `ranger-delta` and
  `newPlugin(String metalake, String catalogProvider, Map<String,String> config)`.
  Distribution belongs under `authorizations/ranger-delta/libs`; no provider is
  installed by the characterization module.
- Implement four-level `AuthorizationMetadataObject` and
  `AuthorizationSecurableObject` classes alongside the provider. Override
  `translatePrivilege`, `translateMetadataObject`, `translateOwner`,
  `generateAuthorizationSecurableObject`, `policyResourceDefinesRule`,
  `createPolicyAddResources`, `wildcardSearchPolicies`, and `findManagedPolicy`.
  Reuse inherited grant/revoke only after its translation matrix is covered.
  The local candidate intentionally covers only SELECT_TABLE allow on tables.
- Override `onMetadataUpdated`, `updatePolicyByMetadataObject`,
  `renameMetadataObject`, and `removeMetadataObject`; do not inherit the
  HadoopSQL implementation's three-level catalog-removing lifecycle code.
  Apply the same ownership/type boundary to owner changes, rename, cleanup,
  retries, and catalog removal. Add `TestRangerPolicyLifecycle` covering
  duplicate ownership labels, null/malformed policy responses, denied grants,
  every existing supported privilege, failures/retries, and drop/recreate.
- Add `authorizations/ranger-gravitino-service/src/main/resources/ranger-servicedef-delta.json`
  in Package 5 with the exact canonical hierarchy and service lookup implementation.
  Its `GravitinoResourceLookup` uses existing authenticated Gravitino client
  load/list interfaces, bounded pagination, errors on failed authentication/backends,
  and direct Admin import/export/lookup tests. Review deployment of this service definition
  before installing it; local `ServicePolicies` is not Admin persistence.
- Pin the isolated production authz package to the required Ranger 2.8.0 target.
  Gravitino's existing Ranger provider uses 2.4.0, and Kyuubi authz 1.10.2 was
  compiled against 2.5.0; those Task 0.3 results are historical only. Task 0.4
  runs both bounded fixtures on 2.8.0 and converges every Ranger module in each
  relevant test classpath. Ranger 2.8's `ranger-plugins-audit` is a POM-only
  aggregator and executable audit APIs live in `ranger-audit-core`; select and
  validate the deployed audit destinations separately. Test shared-model and
  packaging boundaries explicitly. In the Spark fixture, exclude Ranger's raw
  Hadoop 3.3.4 in favor of Spark's Hadoop client 3.3.6, and exclude Jersey 1
  server/bundle plus
  `jsr311-api` while retaining Jersey client/core 1.19.4. These exclusions
  resolve observed duplicate-class failures with embedded Jersey 2.41.
  The final distribution must separately resolve/validate residual Hive/YARN
  2.7.2 versus Hadoop client/HDFS 3.3.6, and AWS v1 bundle 1.12.367 versus
  core/logs 1.12.765 before S3/YARN acceptance. Keep the existing S3 credential,
  LogStore, executor, refresh, and failure matrix; local engine success does not
  establish compatibility for these unexecuted dependency paths.

## Package 5: Ranger metadata discovery and Spark enforcement

**Depends on:** Packages 1, 2, and 4.

**Proposed files:**

- New module `authorizations/ranger-gravitino-service/` with a build file, service definition resources, and `RangerGravitinoService.java` under `src/main/java/org/apache/gravitino/authorization/ranger/service/`.
- Add `GravitinoResourceLookup.java` in that package for Gravitino API traversal; keep Ranger service wiring separate from HTTP/client behavior.
- Add `TestGravitinoResourceLookup` in that module and `TestDeltaRangerEnforcement` in the Spark Delta integration-test package.
- Extend the existing Ranger integration environment and service packaging following the selected Ranger deployment version.
- Add `DeltaRangerExtension.java`, `DeltaCanonicalResource.java`, and
  `DeltaRangerReadRule.java` under
  `spark-connector/v3.5/spark/src/main/java/org/apache/gravitino/spark/connector/delta/`.
  Use the public `SparkSessionExtensions` rule hooks and native `CatalogTable`
  descriptor from `GravitinoDeltaCatalogSpark35.loadTable(Identifier)`; preserve
  named source/target identities and fail closed on missing identities.
  The test helper is not a production implementation: replace its per-analysis
  tags with reviewed query/subject/policy-version handling before cached/reused
  plans can be accepted. Masks require fresh Catalyst expression IDs and explicit
  parent-reference remapping so optimization cannot erase them.
- Add a scoped metadata client factory inside the Spark Delta package using
  existing client builder authentication. Production subject resolution requires
  a platform-owned authenticated contract; local `UGI.doAs` and Kyuubi
  `private[authz]` helpers are not a supported public authentication interface.
  Test failed metadata credentials and missing/tampered query identity independently.
- Preserve the distinction between `DeltaLogFileIndex` metadata reconstruction
  and `TahoeLogFileIndex` data scans, and test internal log visibility, raw Delta
  paths, raw file APIs, and arbitrary-job restrictions against the selected
  deployment trust model before release.

**Interfaces:** Ranger's resource lookup contract consumes a prefix and parent
resource context; the adapter obtains names from Gravitino APIs. Its exact Ranger
service type and Spark service routing must match Package 0's identity decision.

- [ ] Add lookup tests for catalog/schema/table/column traversal, prefix filtering, pagination, denial, timeout, and schema freshness.
- [ ] Implement authenticated Gravitino lookup with bounded caching and explicit errors. Remove Glue/Hive lookup dependencies from this service path.
- [ ] Configure Spark Ranger enforcement and test exact resources/actions for named reads, merge source/target, maintenance, and path access.
- [ ] Author column permissions, masks, and filters directly in Ranger, then verify allowed/denied columns and returned rows/values through the Gravitino Delta adapter.
- [ ] Test cached plans/results, long-running streams, checkpoint restarts, identity changes, and policy refresh; define and verify the revocation boundary.
- [ ] Verify two catalogs with identical schema/table names cannot share grants accidentally; review and commit.

**Gate:** Metadata lookup, policy publication, and runtime enforcement must each
have independent evidence, followed by the combined end-to-end test.

## Package 6: S3 credentials, packaging, and platform integration

**Depends on:** Packages 2, 3, and 5.

**Files:**

- Add Delta credential adaptation in the versioned Delta connector package; reuse `bundles/aws/src/main/java/org/apache/gravitino/s3/credential/S3TokenProvider.java` and `S3TokenGenerator.java` where applicable.
- Inspect `common/src/main/java/org/apache/gravitino/credential/CredentialPropertyUtils.java`; keep Delta Hadoop configuration separate from Iceberg FileIO properties.
- Add `TestDeltaS3` and `TestDeltaCredentialRefresh` in the Delta integration-test package.
- Modify versioned Spark runtime and catalog distribution build files for the selected dependency contract.

**Interfaces:** Use the platform's actual workload identity or Gravitino-vended
credentials, not an invented platform API. Read S3 locations from Gravitino and
preserve the platform's writer exclusion for every mutating entry point.

- [ ] Confirm deployed Scala binary, job entry points, user identity, IAM mechanism, and writer exclusion integration from platform configuration.
- [ ] Resolve and report Spark/Hadoop/AWS/Delta dependency versions; reject mixed Spark patches and incompatible AWS connector dependencies.
- [ ] Implement credential delivery and, when applicable, refresh on driver/executors without logging secrets or leaking credentials across catalogs.
- [ ] Run real S3 reads/writes, sequential jobs, checkpoint restart, expired credentials, interrupted writes, and lifecycle recovery.
- [ ] Test the packaged artifact in a deployment with Glue/Hive catalog access removed; review and commit.

## Package 7: Acceptance matrix, Glue migration rehearsal, and release checks

**Depends on:** Packages 1–6.

**Files:**

- Create `.github/workflows/delta-spark-integration-test.yml`, following existing Spark workflows.
- Complete `docs/delta-spark-support-matrix.md` with test identifiers and actual results.
- Create `docs/spark-connector/spark-delta-catalog.md` for setup, supported versions, identity, and policy behavior.
- Create `docs/delta-glue-migration.md` for registration import/rehearsal and cutover checks.

- [ ] Run each full suite on all four Spark/Delta pairs; ensure reported test counts are nonzero and failures are not suppressed.
- [ ] Rehearse importing a representative existing Glue table at its current S3 location; validate schema, partitions, protocol, history, grants, and lookup through Gravitino.
- [ ] Disable Glue/Hive access in the rehearsal environment and run the same read/write/authorization contract.
- [ ] Document recovery, rollback of routing/registration, and why existing data is not deleted during a failed migration.
- [ ] Run formatting and required checks, inspect packaged dependency reports, and prepare a concise PR preview for user confirmation.

## Verification commands

Run focused tests at each package's red/green cycle. The following repository
checks supplement, rather than replace, the four-pair Delta runtime evidence:

```bash
./gradlew spotlessApply
./gradlew test -PskipITs -PskipDockerTests=false
./gradlew test -PskipTests -PskipDockerTests=false
./gradlew build -PskipDockerTests=false
```

If OpenAPI YAML changes:

```bash
./gradlew :docs:build
```

Use module-scoped tests during development and the repository checks for release.
Respect the shared integration environment serialization already configured in
`build.gradle.kts`. Actual S3 tests require the authorized test deployment; Docker
test results alone do not establish S3 behavior.

## Requirement coverage

| Requirement | Packages | Completion evidence |
| --- | --- | --- |
| R1 versions | 0, 3, 7 | Resolved dependencies and passing results for all four pairs. |
| R2 registration | 1, 2, 7 | Durable registrations and successful operation with Glue/Hive absent. |
| R3 catalog parity | 0, 2 | Named catalogs, two-catalog isolation, lifecycle, Iceberg coexistence. |
| R4 full Delta | 2, 3, 6, 7 | Native comparison inventory, all feature/command tests, S3 execution. |
| R5 existing Gravitino grants | 4, 5 | Existing grant/revoke/deny API, pushdown, and enforcement without privilege-model changes. |
| R6 Ranger columns/masks/filters | 4, 5 | Direct Ranger authoring/revocation, metadata lookup, coexistence, denied columns, filtered/masked results. |
| R7 S3/multi-cloud design | 1, 6 | S3 validation and storage-independent registration boundary. |
| R8 writer exclusion | 0, 1, 3, 6 | No uncoordinated server writer; retry/recovery/stream/maintenance handoff tests. |
| R9 unchanged Gravitino core | 0–7 | Diff review confirms no core/API/server/client contract or storage-schema changes; integration uses existing extension interfaces. |

## Review and execution boundary

Planning does not certify the native bridge, policy model, or deployment. Start
with Package 0, review its evidence, then expand the downstream code-level plans
without reducing the acceptance contract. Use inline execution unless the user
chooses delegation. Each production package ends with focused verification and a
reviewable commit; no issue or PR is submitted without the required preview.

## Current implementation checkpoint — October 5, 2026

The finite SELECT milestone is accepted and pushed at `84172dd45`. Its exact
runtime scope, evidence and remaining limitations are recorded in
[the production qualification results](../../delta-ranger-production-results.md).
This closes neither the packages above nor the full-support requirements.

Subsequent local work added the reviewed TABLE/MODIFY_TABLE/ALLOW to Ranger
`update` mapping. The
[native append characterization](2026-09-27-delta-native-append-characterization.md)
then passed all four Spark/Delta pairs on October 5, with 14 tests per pair and
independent source/artifact review; see the
[results](../../delta-native-append-results.md). The next increment is production
[exact target binding and UPDATE authority](2026-10-05-delta-append-target-authority.md),
followed by integrated command/source enforcement under the
[governed append design](../specs/2026-09-27-delta-governed-append-design.md).
Native-only append results do not establish Ranger enforcement or deployed S3 writes.

The remaining acceptance scope still includes the full command and table-feature
inventory, governed DML/DDL, history and change data feed, streaming and restart,
maintenance, recovery and policy reconciliation, and isolated catalog-demo and
migration qualification. Keep each later package tied to the requirement coverage
table above; an intermediate passing suite does not replace those requirements.

## Priority update — October 7, 2026

The user's highest-priority feature once the Delta integration is mature is
**Change Data Feed (CDF) enablement through the catalog**. Continue the current
registration and governed-write foundations; prioritize catalog-mediated CDF
next once those maturity gates are satisfied. This does not change the required
Spark/Delta versions, Ranger 2.8.0, S3 qualification or unchanged-core constraint.

CDF remains part of the full-support contract. Its later plan must distinguish
enabling the table feature through the named catalog from qualifying CDF reads
and policy enforcement. No default-on behavior or present CDF support is implied
by this priority update.
