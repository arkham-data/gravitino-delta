<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# External JDBC Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Admit owned external PostgreSQL query inputs to native named Delta APPEND and qualify the actual Spark Hub source/tracking path.

**Architecture:** An explicitly enabled, finite job scope constructs Spark's stock V1 JDBC relation and privately binds its exact identity before DataFrame analysis. Separate external execution provenance from registered Ranger obligations, preserving native planning, named writers, target cache and all governed-source policies. Spark Hub delegates only native JDBC construction through an optional owned reader.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, classic PySpark, PostgreSQL 16 with driver 42.7.11, actual Ranger server/client 2.8.0, Gravitino and S3A/MinIO.

**Spec:** [Approved external JDBC design](../specs/2026-11-05-delta-external-jdbc-design.md). User approved with “Go” after its builder-reviewed handoff. Standing execution authorization covers this implementation sequence; no repeated permission gate for routine plan details.

## Global Constraints

- Spark 3.5.4 and 3.5.6; Delta 3.3.0 and 3.3.2; Java 17; Scala 2.12.21.
- Actual Ranger server/client 2.8.0; Gravitino registration; S3A/MinIO.
- Gravitino owns destination registration; Spark and Delta own execution and transactions.
- No Gravitino core/API/server/client/storage-model changes, engine forks, Glue compatibility, manual Delta transactions or production raw-path destinations.
- Trusted application/runtime and credentials, not an arbitrary JVM/Python sandbox.
- Preserve registered source SELECT, column permissions, filters and masks, including masked keys; preserve destination authorization and existing publication/cache/recovery semantics.
- One writer per destination; concurrent readers and changing source data remain possible.
- No generic DataFrame admission, collected-row reconstruction, LogicalRDD trust or automatic replay of uncertain/committed writes.
- Catalog-demo, CDF consumer state, streaming and deployment remain separate.
- Required option keys: url, driver, user, password, dbtable. Optional fetchsize and the complete partitionColumn/lowerBound/upperBound/numPartitions quartet. Case-insensitive keys; ambiguous duplicates reject before connection. Stock org.postgresql.Driver, nonempty user, supplied password, single-host database-bearing URL without user-info/query/fragment. Fetch size nonnegative; partition count positive; native Spark validates partition types/bounds.
- Reject all other options, including explicit TLS properties, prepareQuery, sessionInitStatement, customSchema, custom connection providers and direct query/procedure APIs. Preserve native defaults; unsupported security options never fall back. No deployed TLS claim.
- Job scope lifetime is explicitly 1–86400 seconds, monotonic from creation, without renewal; qualification uses 1200 seconds. Expiry/close blocks subsequent action/publication but does not promise atomic cancellation of an in-flight database statement.
- All code/docs/comments in English; documented public/protected APIs; scoped Spotless, unit tests for new logic. Preserve original evidence and worktrees; push only the independently reviewed milestone.

## Workspaces and verification convention

G is the current linked worktree `gravitino-delta/.worktrees/spark-hub-adoption-plan` on `codex/spark-hub-adoption-plan`. S is `/Users/jacobo.calderon/Code/Arkham/spark_hub/.worktrees/gravitino-catalog-foundation`, initially `d01024b6f459ad2743b7b55099226b8c3dc742fd`. Do not edit either main checkout. This plan's evidence is `.superpowers/sdd/2026-11-05-delta-external-jdbc` under G. Read G/AGENTS.md and applicable S/main AGENTS.md; memory tools are unavailable. No registered-PostgreSQL matrix replay.

Path aliases below: R=`spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`, T=corresponding `src/test/scala/.../delta/ranger`, P=`spark-connector/v3.5/spark-delta-ranger/python`, C=`spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector`.

Lead pair is 3.5.6/3.3.2. Set JAVA_HOME to `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`; JAVA_TOOL_OPTIONS to `-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m`. Gradle invocation prefix: `./gradlew --max-workers=1 --console=plain -Dorg.gradle.jvmargs=-Xmx2g -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs -PskipDockerTests=false`. Run only the task's focused classes and affected controls; record exact command, exit and XML, including skips. Heavy builds, Docker mutations and qualification cells run serially.

## Review Focus

1. Case-variant options or caller map mutation alter an issued query/credential: Task 1 freezes canonical options and tests mutation/duplicate rejection before connection.
2. Native schema discovery occurs before a target decision: Tasks 1/4 distinguish schema/metadata traffic from payload, never assert no network contact at construction.
3. Expiry/close races with action or publication: Tasks 1/2 test retained bindings and the existing precommit boundary, preserving any original native outcome.
4. Count and APPEND overlap under different thread identities: Task 3 tests actual Hub tracker context, with rejection on mismatch and independent action contexts.
5. Optimizer pruning or mixed-source external classification drops named Ranger obligations: Task 2 tests hidden/join/pruned columns, masks/filters and target cache; Task 4 confirms real policy denial.

## Task 1: Own native external JDBC construction and scope lifetime

**Files:**
- Create `R/DeltaRangerExternalJdbcSources.scala`: private source issuer, scope/binding lifecycle and exact native relation registry.
- Create `R/DeltaRangerExternalJdbcOptions.scala`: pure canonical copied option validation, with safe diagnostics.
- Modify `C/GravitinoSparkConfig.java`: documented `GRAVITINO_ENABLE_DELTA_RANGER_EXTERNAL_JDBC_SUPPORT = "spark.gravitino.enableDeltaRangerExternalJdbcSupport"`.
- Create `T/TestDeltaRangerExternalJdbcScope.scala` and `T/TestDeltaRangerExternalJdbcConstruction.scala`.
- Reuse `src/test/java/.../RegisteredJdbcPostgresFixture.java` as an owned isolated fixture; change it only if a small additional test setup hook is necessary. Existing tests and behavior stay intact.
- Modify companion `build.gradle.kts` only to add explicit external-JDBC Docker test opt-in `rangerDeltaExternalJdbcEnabled` -> `gravitino.ranger.delta.externalJdbc.enabled`.

**Interfaces:**
- `private[ranger] object DeltaRangerExternalJdbcSources` provides `open(spark: SparkSession, lifetimeSeconds: Long): Scope` and `resolve(relation: JDBCRelation, spark: SparkSession): BoundInput`.
- `Scope extends AutoCloseable` provides `load(options: java.util.Map[String,String]): BoundInput`, `requireLive(): Unit`, and idempotent `close(): Unit`.
- `BoundInput` exposes package-private `relation: JDBCRelation`, `requireCurrent(spark: SparkSession): Unit`, and `requireSame(candidate: JDBCRelation): Unit`. These carry no Ranger resource or Delta storage authority.
- `DeltaRangerExternalJdbcOptions.copyAndValidate(options: java.util.Map[String,String]): Map[String,String]` returns a detached immutable canonical-key map, following Global Constraints exactly.
- Read startup external/Delta/Ranger opt-ins from SparkContext's original SparkConf, not a later session flag. Capture the owning active session, actual query subject/groups and complete effective session settings. Task 2 installs full public/runtime routing; Task 1 does not expose a public DataFrame API or weaken analyzer admission.

- [x] **Step 1: Write failing scope/options tests.** Assert 0 and 86401 seconds reject, 1 and 86400 validate; disabled/missing startup flags, foreign/no active session and subject mismatch reject. Assert duplicate URL/url and invalid/unknown options reject before any provider connection; caller map mutation cannot alter returned options; diagnostics omit sentinel password/query values. Assert negative fetchsize, zero partitions, incomplete quartet and invalid URL forms reject.
- [x] **Step 2: Run the new focused classes for meaningful RED.** Use the Gradle prefix plus `:spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerExternalJdbcScope*'`. Retain compilation/API failure for the missing contract; separate environment failures from RED.
- [x] **Step 3: Implement the private scope, exact option contract and native construction.** Delegate schema/partition construction to stock JdbcRelationProvider; issue only its exact returned JDBCRelation before Dataset analysis. Use identity-based registration, detached full native schema/options/partitions, monotonic expiry and cleanup on close/expiry/application end. Do not hold cleanup locks across JDBC network IO. A load racing with closure must not publish a live binding. A bounded package-private clock seam may support deterministic lifecycle tests; no public clock/provider callbacks or trust APIs.
- [x] **Step 4: Add real native construction and lifecycle tests.** Under the external Docker opt-in/tag, use the isolated read-only PostgreSQL fixture and a Hub-shaped subquery. Assert actual JDBCRelation schema/partition state, resolution of only the issued object, rejection of a reconstructed/copied relation with equal fields, current/closed/expired checks, and original options after caller mutation. Exercise native VARCHAR and both timestamp kinds; verify schema discovery occurred without claiming full payload execution. Test wrong credentials and redact native failure diagnostics without losing a safe failure category. Timer and application cleanup must not close shared Spark services.
- [x] **Step 5: Run GREEN, format and self-review.** Run both new focused classes on lead pair with `-PrangerDeltaExternalJdbcEnabled=true`; inspect XML for actual native test execution, no skips. Run scoped companion and spark-common Spotless/checks and `git diff --check`. Record exact native dependency/version evidence. This closes private source construction only; public reads/APPEND remain rejected until Task 2.
- [x] **Step 6: Commit and report for independent task review.** Record changes, RED/GREEN commands and all limitations in this plan's task report. No push or full-milestone claim.

## Task 2: Integrate external sources with native reads, APPEND and policy receipts

**Files:**
- Create `R/GravitinoJdbcInput.scala`: public job-scope API delegating Task 1's issuer.
- Create `R/DeltaRangerExternalJdbcScans.scala`: exact V1 stock-planning continuation and physical proof.
- Modify source-facing seams in `R/DeltaRangerInputSources.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerOrdinaryRead.scala`, `DeltaRangerOrdinaryAdmission.scala`, `DeltaRangerPlanAdmission.scala`, `DeltaRangerDecisionReceipt.scala`, `DeltaRangerNativeScans.scala`, `DeltaRangerMaskRules.scala`, `DeltaRangerPhysicalPlanning.scala`, `DeltaRangerPhysicalProvenance.scala`, `DeltaRangerNativeProvenance.scala`, `DeltaRangerAppendV1Input.scala`, `DeltaRangerAppendCommandBinding.scala`, `DeltaRangerCdfCacheMaintenance.scala`, `DeltaRangerSparkExtension.scala` only where the new finite source requires it.
- Modify `C/plugin/GravitinoDriverPlugin.java` and its config/driver tests to validate original startup dependencies; external opt-in must require existing Delta/Ranger opt-ins.
- Create `T/TestDeltaRangerExternalJdbcNative.scala`; extend affected source/receipt/cache tests narrowly.
- Update companion README for this installed-only capability.

**Interfaces:**
- Consume Task 1's Scope/load/resolve/BoundInput unchanged unless controller records a spec-preserving adjustment.
- Public `object GravitinoJdbcInput` provides `apiVersion(): Int = 1` and `open(spark: SparkSession, lifetimeSeconds: Long): GravitinoJdbcInput`; class extends AutoCloseable with `read(options: java.util.Map[String,String]): DataFrame` and idempotent `close(): Unit`. No DataFrame argument overload. The private native relation is issued before building Dataset.ofRows, through a small Spark-package bridge only if Spark visibility requires it.
- Finite source inventory distinguishes NamedDelta, RegisteredJdbc and ExternalJdbc; only the first two have Ranger resource obligations. Every kind has provenance/current/lifetime checks. No fake engine or synthetic catalog identity for external data.
- `DeltaRangerExternalJdbcScans` supplies logical binding, prepare, verify and native physical continuation to existing preparation/APPEND flows; signature details follow the existing registered JdbcScans module, with exact V1 ownership rather than V2 impersonation.

- [x] **Step 1: Add failing installed source read/count/APPEND tests.** Real native subquery -> select/filter/current_timestamp -> each named writer; query-result count and DataFrame.count are distinct cases. Inspect RowDataSourceScanExec/JDBCRDD identity, resulting rows/types and native commits. Add unissued DataFrame, cloned tags/relation, foreign scope/session, closed/expired scope, arbitrary LogicalRDD and unknown-source rejection.
- [x] **Step 2: Implement finite provenance/policy separation.** Capture every original occurrence before pruning. Registered sources keep all SELECT/mask/filter decisions and current metadata checks; external sources retain only exact execution-source obligations. Update source inventory, binding serialization/identity and cache participants together. No changes to CDF/MERGE grammar or target authorization/transaction semantics.
- [x] **Step 3: Implement owned stock V1 continuation and public reader.** Bind exact JdbcRelationProvider output through stock DataSourceStrategy to the native physical subtree/RDD; retain it per action/occurrence/recipe. Reuse the established lifecycle for count, APPEND, AQE and helper recipes. Do not authorize arbitrary equal RDDs or copied plan tags. Recheck every source at action and existing precommit boundaries.
- [x] **Step 4: Prove explicit native partitions and mixed policies.** Test complete numeric/date/timestamp bounds, NULL/out-of-bound rows and native fetchsize. Compare native partition predicates/rows; bounds are stride, not filters. Test external plus one named Delta INNER join with denied hidden/join fields, row filters, masked keys, source revoke/policy change, optimized-empty/pruned sources, and target UPDATE denial/revocation. Compare native version on every denied write. Capture original failure/commit evidence without replay.
- [x] **Step 5: Prove cache/configuration execution boundaries.** Cover AQE on/off, existing file-skipping profiles, unchanged native target-cache refresh, empty input and scope close/expiry at precommit. Repeated fresh actions while live must work; substituted configuration or expired retained source must fail. No source cache/checkpoint support is implied. Retain complete failure evidence if native commit preceded a later error.
- [x] **Step 6: Run focused tests, affected registered/Delta controls, format and commit.** Use Gradle prefix and external opt-in, selecting new external test classes plus source/receipt/cache controls touched by this task. Reuse historical evidence for unchanged suites; no broad registered matrix replay. Require independent review before Task 3.

## Task 3: Expose classic PySpark and the real Spark Hub source/tracker path

**Files:**
- Create `P/gravitino_delta/inputs.py`; modify `P/gravitino_delta/__init__.py`; create `P/tests/test_inputs.py`, extend `test_package.py`.
- S modify `src/shared/services/v2/spark.py` and, only for explicit thread-context propagation, `src/shared/commands/progress_tracker.py`.
- S create `src/shared/services/v3/gravitino_jdbc.py` and `tests/unit/shared/services/v3/test_gravitino_jdbc.py`; extend existing JDBC v2 tests for adapter routing.
- S add `tests/integration/test_gravitino_external_jdbc.py` for real source function and tracker acceptance, using explicit fixture/profile opt-in.
- Modify the existing Gravitino runtime profile only where required to carry the external startup flag and owned reader; never reroute all jobs or implement a pretend complete sink.

**Interfaces:**
- Python `GravitinoJdbcInput.open(spark: SparkSession, *, lifetime_seconds: int)` returns an owning context manager; `.read(options: Mapping[str,str]) -> DataFrame`, `.close() -> None`, `__enter__` returns self, `__exit__` closes. Validate classic PySpark, types and protocol version before invocation; JVM independently validates all authority. Copy mappings and retain native failures without retries.
- S `GravitinoJdbcReader` wraps that job scope and exposes `read(options: Mapping[str,str]) -> DataFrame` and close/context-manager lifecycle. SparkClient's optional keyword-only `external_jdbc_reader=None` supplies this dependency. In query_jdbc, retain existing construction and use its `.read(options)` only for selected stock JDBC; unsupported formats/options fail, never raw-reader fallback. No dependency preserves the legacy route.
- Real acceptance directly calls `run_source_jdbc_v2` with actual SparkClient/owned reader and nonincremental V2 payload; supplies existing named APPEND rather than claiming the legacy job_wrapper sink is migrated.

- [x] **Step 1: Add RED Python bridge and routing tests.** Missing/wrong JVM API, Spark Connect, nonstring/duplicate options, map mutation, close-on-exception, invocation errors without replay; external dependency receives the exact query/count options; legacy route stays selected only without it. Unsupported source formats and options reject without a raw fallback. Run existing project pytest entrypoints for these named files and preserve the first meaningful failure.
- [x] **Step 2: Implement thin bindings and shared source seam.** Follow Task 2 signatures exactly. Use Spark Hub GitNexus impact/context before symbol edits and detect_changes before commit per S guidance. Report high-risk shared caller impact before changes. No payload-controlled trusted dependency or register-existing-DataFrame API. No new secret or query logging.
- [x] **Step 3: Add actual threaded tracker acceptance.** Use the real JDBC v2 function including remote COUNT, background ProgressTracker -> SparkClient.init_df_tracking -> df.count, then current_timestamp and named APPEND. Require matching authenticated subject/session and separate concurrent read/write contexts. A wrong-subject/thread-context test must reject. If needed, propagate the captured trusted execution context explicitly; never convert mismatched identities silently or disable tracking. Compare native rows/commit and preserve count-vs-write source advancement differences.
- [x] **Step 4: Run focused Python tests and installed real source/tracker proof.** Use existing pinned pair Python environment; inspect real tests for execution rather than skips. Unit mocks do not qualify threading/native execution. Record remaining shared-sink/CREATE/overwrite/transform gaps in coverage documentation; keep external-ingestion-create open.
- [x] **Step 5: Review and commit both repositories independently.** Store source hashes and reports under G's plan workspace. No full Hub job, deployment, catalog-demo, vendor or TLS completion claim.

## Task 4: Qualify the new external-input milestone and close

**Files:**
- Create `delta/acceptance/external_jdbc_live.py`, `external_jdbc_checks.py`, `test_external_jdbc_checks.py`.
- Reuse existing owned PostgreSQL/Gravitino/Ranger/MinIO fixture, package and evidence machinery without replaying old cases. Extend only necessary case dispatch/receipt seams.
- Create `docs/superpowers/specs/2026-11-05-delta-external-jdbc-results.md`; update this plan, companion README, `docs/delta-spark-parity.md`, adoption roadmap and `delta/acceptance/spark_hub_coverage.json`.

**Interfaces and finite inventory:**
- Consume Tasks 1–3 APIs, runtime flag, exact installed binaries and Hub adapter at recorded commits.
- All four Spark/Delta pairs run `external_insert_into`, `external_partitioned_save_as_table`, `external_target_update_denied`: 12 cases. Lead additionally runs `external_hub_tracker`, `external_mixed_governed`, `external_named_source_denied`, `external_scope_closed_before_publish`, `external_target_revoked_before_publish`, `external_unissued_rejected`, `external_source_advances`: 7 cases, 19 total. A case may record separate explicitly inventoried actions; checker counts each and distinguishes native writes from legitimate counts/reads.
- Lead mixed case covers governed source mask/filter and native target cache; lead scope/revoke cases use existing prepublication barriers, never retry targets. Scope expiry, foreign identity, detailed types and date/timestamp partition shapes retain Task 2 installed evidence; cross-pair focused installed checks qualify the affected code on all four combinations.

- [x] **Step 1: Add checker RED tests.** Missing/extra cases or actions, wrong server/client or Spark/Delta version, stale bytes, missing actual Hub caller identity, false denial cause, secrets in summaries and mismatched native commit/publication must fail. Test committed/unknown outcomes are retained and cannot become retry eligibility.
- [x] **Step 2: Implement bounded fixture/checker orchestration.** Preserve actual process ownership, artifact receipts and native state conventions. Source role is read-only, fixtures isolated; keep private credential files owner-only and out of summaries. New records distinguish schema discovery, remote count, payload read and destination mutation. Check current disk/service readiness before each heavy stage; reuse unchanged packages exactly when justified.
- [x] **Step 3: Run affected installed and 19 actual-service cases serially.** Verify all four required pairs, actual Ranger server/client 2.8.0 and MinIO; no mocks or empty inventories qualify. Retain original failures/outcomes without replay. Both native writers, real source policy denial, target denial and real Hub tracker require original evidence.
- [x] **Step 4: Independently validate recorded evidence and close owned fixtures.** Check exact producing bytes, processes, exit state and native outcomes; don't relabel package contents after source changes. Shared services and historical evidence remain intact. Report source advancement and unsupported options/TLS/vendors/transforms explicitly; no performance claim from wall time alone.
- [ ] **Step 5: Task review, then whole-milestone review and closure.** Review both repository ranges and evidence once all tasks pass; fix material findings with focused requalification. Update completion records, push reviewed milestone branches under standing authorization and retain worktrees/evidence. No PR, merge, deployment or broader ingestion completion claim.

Implementation qualification: see [results](../specs/2026-11-05-delta-external-jdbc-results.md).
The exact 19 cases/23 actions may span immutable fixture segments with separately
verified original processes and identical qualified product/Hub bytes per pair.
Original failed outcomes and correction records remain retained; no target replay.
Step 5 remains the root agent's independent reviews and branch closure.
