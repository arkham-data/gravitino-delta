# Delta S3 and Ranger 2.8 Live Contract Plan

> Execute with superpowers:subagent-driven-development. The user authorized local MinIO/RustFS and sub-agent execution. Continue without another approval checkpoint; preserve the existing demo environment.

**Goal:** Extend the reviewed feasibility branch with real S3A Delta I/O and Ranger 2.8 Admin/plugin-download contracts. These independent component tests precede a production connector and catalog-demo integration.

**Architecture:** Reuse the named Spark test catalog against an isolated MinIO bucket. Reuse CandidateDeltaPolicyPlugin against a disposable custom four-level Ranger service on official Ranger 2.8.0 with PostgreSQL. Source map: `.superpowers/sdd/2026-09-07-delta-s3-ranger-live/source-research.md`.

## Global Constraints

- No Gravitino core/API/server/client/storage-schema production changes. All new behavior stays in opt-in tests and test fixtures.
- Test Spark 3.5.4 and 3.5.6 with Delta 3.3.0 and 3.3.2 in all four combinations. Load Ranger 2.8.0 artifacts and use deployed Ranger 2.8.0.
- Gravitino owns named table registration; do not introduce Glue/Hive registration authority.
- Ranger owns column permissions, masks and filters. Do not add broad SELECT privileges or substitute Python policy evaluation for Spark enforcement.
- One writer job per dataset remains a platform assumption. No distributed fencing or native correlation claim.
- Use unique local test objects and clean only those objects. Never alter catalog-demo source, data, policies or volumes in this package.
- Endpoints and credential-file paths are configurable. Never put credential values in code, logs, command arguments, evidence, Git or archives.
- Live tests use @Tag("gravitino-docker-test") and explicit opt-in. Selected live tests must execute with nonzero XML counts; missing required inputs fail rather than silently passing. Ordinary builds must not require services or gain production dependencies.
- Java8-compatible source on JDK17; normal imports, public/protected Javadoc, resource cleanup and repository formatting.
- Preserve original RED/GREEN XML, exact commands/exits, loaded versions, endpoint/image evidence and honest limitations. Setup failures are not behavioral RED.

## Task 1: Named Delta lifecycle on MinIO

**Files:**
- Modify `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/delta/NamedDeltaCatalog.java`.
- Add `TestDeltaS3Contract.java` in that package.
- Modify `spark-connector/v3.5/spark/build.gradle.kts` only for opt-in input forwarding/validation.
- Add `docs/delta-s3-contract-results.md`.

**Interfaces:** Extend the existing BaseIT pattern in TestDeltaNamedCatalogGate. Use registry alias delta_a, direct=false, lakehouse-generic registration and unique metalake/schema/prefix. Forward deltaS3Enabled, deltaS3Endpoint, deltaS3Bucket, deltaS3CredentialsFile to gravitino.delta.test.s3.enabled/endpoint/bucket/credentialsFile. When enabled, require existing Delta runtime opt-in and nonblank required inputs. Read MINIO_ROOT_USER and MINIO_ROOT_PASSWORD from the private properties file. Configure S3A endpoint, path style, SSL false and SimpleAWSCredentialsProvider. No new S3 dependency expected.

1. Write the actual live CTAS test first against a schema location under s3a://<bucket>/gravitino-delta-contract/<UUID>/. Run Spark3.5.4/Delta3.3.0 before fixing NamedDeltaCatalog.location. Preserve an executed failure attributable to host-filesystem URI joining, or record real characterization if it already works. Never manufacture RED by changing reviewed code to be wrong.
2. Replace local-only Paths.get location joining with Hadoop Path where the live failure supports it. Keep local behavior and unaltered native negative controls.
3. Complete one sequential lifecycle test: named CTAS, append, MERGE update+insert, copy CTAS, RTAS, DESCRIBE HISTORY and Spark stop/restart. Assert exact ordered rows after mutations; Gravitino registration ID, exact registered s3a URI, native Delta metadata ID and advancing/retained history across replacement and session restart. Verify actual _delta_log via S3A. Explicitly close named-catalog clients and Spark sessions; delete only the UUID-owned S3 prefix and test metadata.
4. Run all four runtime combinations selecting TestDeltaS3Contract, with -PskipDockerTests=false and no -PskipITs (would exclude the package). Run existing local TestDeltaNamedCatalogGate across the four combinations because shared NamedDeltaCatalog changes. Combine selectors per matrix run when practical. Record actual loaded versions and nonzero test counts; no stale XML. Run scoped spotlessApply/Check.
5. Document exact commands, counts, URI fix, pinned local MinIO and limitations. Commit only owned files and report for independent review. This test does not join the managed reservation fixture to native commits, prove S3 IAM/TLS, production identity or full Delta operation support.

**Runtime:** Controller-owned `/private/tmp/gravitino-delta-local-runtime/state.json` contains current endpoints/image pins/bucket (no secrets). MinIO credentials file is `/private/tmp/gravitino-delta-local-runtime/private/minio.env`. Root manages service lifecycle; worker does not restart Docker. Dynamic ports must be rediscovered if a container restarts.

**Command shape:**

```text
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-3.5:test -PdeltaTestSparkVersion=<3.5.4|3.5.6> -PdeltaTestVersion=<3.3.0|3.3.2> -PdeltaS3Enabled=true -PdeltaS3Endpoint=<endpoint> -PdeltaS3Bucket=delta-fixtures -PdeltaS3CredentialsFile=<private-file> -PskipDockerTests=false --tests '*TestDeltaS3Contract' --tests '*TestDeltaNamedCatalogGate'
```

## Task 2: Live Ranger Admin and policy download

**Files:**
- Add `authorizations/authorization-ranger-delta/src/test/java/org/apache/gravitino/authorization/ranger/delta/TestRangerDeltaAdminContract.java`.
- Add a test resource service-definition JSON copied from the existing canonical four-level fixture; preserve license and source attribution.
- Modify `authorizations/authorization-ranger-delta/build.gradle.kts` for opt-in input forwarding and necessary test-only runtime dependencies exposed by actual execution.
- Add `docs/delta-ranger-live-results.md`.

**Interfaces:** Forward rangerDeltaAdminEnabled/Url/CredentialsFile to gravitino.ranger.delta.admin.enabled/url/credentialsFile. Explicit enabled selection requires inputs. Read RANGER_ADMIN_USER and RANGER_ADMIN_PASSWORD from `/private/tmp/gravitino-delta-local-runtime/private/ranger-admin.env`. Use actual RangerClient(url, "simple", user, password, null). Unique custom serviceDef has catalog/database/table/column, CUSTOM masking and table-level row filtering, lookupSupported=false. Precreate unique service with matching type before CandidateDeltaPolicyPlugin; set authorization.ranger.service.create-if-absent=false. Config uses existing source keys and unique metalake/catalog IDs. Do not change production Ranger modules.

1. Add the executable live contract using actual Ranger 2.8 RangerClient create/read APIs, canonical serviceDef and disposable service. Pin/verify loaded RangerClient, RangerPolicy, RangerAdminRESTClient and ServicePolicies 2.8.0 JARs. Preserve first actual result; dependency/bootstrap failures are setup, and an already-green characterization need not be artificially broken.
2. Drive CandidateDeltaPolicyPlugin.onRoleAdded(empty securables), then table SELECT grant twice via onRoleUpdated. Through independent RangerClient assert exactly one owned policy, expected three ownership labels, exact resources including column=*, expected prefixed role/access, no duplicate grant. Create independent foreign column access, CUSTOM mask and row-filter policies; read-back IDs/content must stay unchanged through candidate grants and revokes.
3. Invoke RangerAdminRESTClient.init(serviceName, "delta-contract", "ranger.plugin.delta", Configuration) with ranger.plugin.delta.policy.rest.url and full-download settings; getServicePoliciesIfUpdated(-1L,0L). Assert service name/id, version, exact canonical hierarchy and managed plus foreign policies. This is separate from Admin CRUD. If rejected, expose and resolve actual local deployment configuration/authentication if supported; never replace download with Admin list or suppress the assertion.
4. Revoke twice, assert owned policy absent and all foreign policies unchanged in Admin and fresh plugin download. Track and delete only owned policy IDs/role/service/serviceDef in finally, preserving initial test failure if cleanup also fails. No shared default service changes.
5. Run live and existing TestRangerDeltaContract together with -PskipDockerTests=false, plus format. Assert nonzero per-class XML and actual Ranger2.8 classpaths. Document separate live Admin/download versus existing local Spark enforcement proofs; no end-to-end downloaded policy enforcement or audit delivery claim. Commit scoped files/report and independent review.

**Command shape:**

```text
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :authorizations:authorization-ranger-delta:test -PrangerDeltaAdminEnabled=true -PrangerDeltaAdminUrl=<current-endpoint> -PrangerDeltaAdminCredentialsFile=<private-file> -PskipDockerTests=false --tests '*TestRangerDeltaAdminContract' --tests '*TestRangerDeltaContract'
```

## Completion

Both tasks need scoped spec/quality review and covering fix checks, then a final whole-branch review. Archive public evidence without private credential files. Retain the worktree and reports. The next implementation decision is a production provider/native lifecycle contract; catalog-demo integration must replace its current raw Spark/Python authorization bypass before it can establish a security boundary.
