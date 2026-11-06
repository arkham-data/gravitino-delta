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

# Registered PostgreSQL Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read a registered PostgreSQL table through native Spark and append its Ranger-governed output to an existing named Delta table.

**Architecture:** Reuse Gravitino's PostgreSQL catalog and credentials, extend the existing four-level Ranger authorization provider, and privately bind native JDBC scans into the current governed source/APPEND lifecycle. Keep external JDBC separate.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server/client 2.8.0, classic PySpark, PostgreSQL and S3A/MinIO.

**Spec:** [Registered JDBC design](../specs/2026-11-05-delta-registered-jdbc-design.md).

## Global Constraints

- Spark 3.5.4 and 3.5.6; Delta 3.3.0 and 3.3.2; Java 17; Scala 2.12.21.
- Actual Ranger server/client 2.8.0, Gravitino registration and S3A/MinIO for live qualification.
- No Gravitino core/API/server/client/storage-model changes, engine forks, manual Delta transactions or Glue compatibility.
- Preserve native classic PySpark DataFrame operations and both existing named APPEND writers: insertInto and existing-table Delta saveAsTable.
- Preserve caller AQE and the existing target policy, schema, publication, cache and recovery contracts.
- Ranger owns source column permissions, masks and row filters. Gravitino pushes catalog-qualified source SELECT grants through the existing authorization SPI.
- A trusted driver/runtime is assumed. Arbitrary JVM/Python code holding database credentials is not sandboxed by this adapter.
- One writer per destination; sources may advance. No remote database repeatable snapshot or immutable table incarnation is promised.
- No arbitrary DataFrame registration, LogicalRDD trust, raw-path destination or collected/reconstructed source authority.
- Retain original evidence; never automatically replay a known or uncertain committed write.

## Review Focus

1. Two PostgreSQL catalogs with the same schema/table collide in Ranger — Task 1 checks catalog-specific grant/revoke and preservation of foreign policies.
2. Eager metadata SELECT prevents a permitted id-only projection — Task 2 tests metadata resolution without broad payload permission and genuine denied-field failure.
3. Optimizer pruning removes a denied source or hidden key — Task 2 retains original occurrence/demand checks for mixed and empty plans.
4. Fresh stock JDBC replay allocates different RDDs or executes an untrusted callback — Task 2 tests owned native transition/reuse and rejects substituted scan/options before callbacks.
5. Configuration/revocation changes after source preparation permit publication or trigger replay — Tasks 2/3 test existing precommit boundaries and retain committed/unconfirmed outcomes.

## Task 1: Qualify PostgreSQL grant pushdown through the existing provider

**Files:**
- Modify: `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/RangerDeltaAuthorization.java`.
- Test: `authorizations/authorization-ranger-delta/src/test/java/org/apache/gravitino/authorization/ranger/delta/TestRangerDeltaAuthorization.java`.
- Test if the existing seam requires it: sibling `TestDeltaRangerPolicySelector.java`.
- Document: `docs/superpowers/specs/2026-11-05-delta-registered-jdbc-results.md` (task-scoped result, not runtime qualification).

**Interfaces:**
- Consumes unchanged `BaseAuthorization.newPlugin(String metalake, String catalogProvider, Map<String,String> properties)` and existing `RangerDeltaAuthorizationPlugin`.
- Produces exact factory acceptance of `lakehouse-delta` and `jdbc-postgresql`, returning the unchanged translator. `shortName()` remains `ranger-delta`; unrelated providers reject before admin mutation.

- [x] **Step 1: Add failing provider/grant lifecycle tests.** Exercise PostgreSQL factory construction with the existing mocked Admin fixture, public privilege translation SELECT_TABLE -> select on catalog/schema/table/*, real plugin role grant/revoke callbacks, and two PostgreSQL catalogs plus a Delta catalog sharing a service. Assert catalog isolation, preservation of unrelated Ranger-authored mask/filter policies and roles, unchanged USE visibility-only behavior, rejected unsupported providers/configuration and rejected rename/drop callbacks. Existing Delta tests remain unchanged.
- [x] **Step 2: Run the focused new tests and retain the original provider-rejection failure.** Use `./gradlew :authorizations:authorization-ranger-delta:test --tests '*TestRangerDeltaAuthorization*' -PskipITs --max-workers=1 --console=plain` with Java 17 and bounded Gradle memory. Record exact command/exit/XML; do not label configuration failures as meaningful RED.
- [x] **Step 3: Expand only the factory's exact provider set and its Javadoc/error contract.** Reuse the translator without a new SPI alias, namespace, module or policy schema. Fix any provider-specific lifecycle defect demonstrated by tests narrowly within the authorization module; no core changes.
- [x] **Step 4: Run the module's ordinary tests and scoped formatting/checks.** Exclude actual-service/production fixture methods unless their required opt-in is deliberately provisioned. Preserve the exact selected inventory and disclose exclusions. Verify existing Delta translation/isolation behavior, no new admin behavior outside the configured catalog, and `git diff --check`.
- [x] **Step 5: Record bounded results, review and commit.** This closes provider acceptance/policy administration tests only; no JDBC scan, live source grant or ingestion claim. Controller owns review and commit.

## Task 2: Bind native registered JDBC reads into governed APPEND

**Files:** `R` below is `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`; `J` is its corresponding `src/main/java/.../delta/ranger/` package.
- Create: `J/DeltaRangerPostgreSqlCatalogSpark35.java` (optional wrapper and issuer lifecycle).
- Create: `J/DeltaRangerJdbcSourceBindings.java` (private exact-table issuance/current metadata/credential comparison).
- Create: `R/DeltaRangerInputSources.scala` (two finite source variants and original obligations).
- Create: `R/DeltaRangerJdbcScans.scala` (finite scan binding and owned stock physical continuation).
- Modify: existing `DeltaRangerReadRule`, `DeltaRangerOrdinaryRead`, `DeltaRangerOrdinaryAdmission`, `DeltaRangerPlanAdmission`, `DeltaRangerDecisionReceipt`, `DeltaRangerMaskRules`, `DeltaRangerNativeScans`, `DeltaRangerNativeProvenance`, `DeltaRangerPhysicalPlanning`, `DeltaRangerPhysicalProvenance`, `DeltaRangerSparkExtension`, `DeltaRangerAppendV1Input`, `DeltaRangerAppendCommandBinding` and `DeltaRangerCdfCacheMaintenance` in R, only at the source-facing seams. Installed AQE/cache characterization identified the latter's Delta-only source participant view; target-cache semantics remain unchanged.
- Modify: `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/GravitinoSparkConfig.java` and `.../plugin/GravitinoDriverPlugin.java` for trusted startup selection; corresponding existing driver/config tests.
- Modify only if needed: companion Gradle dependency declarations for the existing PostgreSQL Spark wrapper and pinned test driver.
- Create focused `TestDeltaRangerRegisteredJdbc*` tests in the companion module's Java/Scala test packages; extend existing source/receipt/append test helpers narrowly.

**Interfaces:**
- Consumes Task 1's provider, existing `DeltaRangerMetadataClient`, resource keys and policy engine; existing stock PostgreSQL catalog/credential hooks; the existing pending-input/receipt/APPEND lifecycle.
- Produces public Spark-loadable `DeltaRangerPostgreSqlCatalogSpark35()` with documented overrides of `initialize`, `loadGravitinoTable`, `createSparkTable` and lifecycle cleanup. Startup boolean is `spark.gravitino.enableDeltaRangerJdbcSupport`, default false, requiring existing Delta/Ranger opt-ins.
- `DeltaRangerInputSources` has exactly NamedDelta and RegisteredJdbc variants; each validates its owner/current metadata and exposes original policy obligations. Delta-only views remain explicit; no fake Delta binding for JDBC.
- `DeltaRangerJdbcScans` owns logical scan bindings and one native physical subtree per action/occurrence/recipe. Consumers use the existing receipt/APPEND interfaces; no new public DataFrame registration method.

- [x] **Step 1: Write failing source-issuance and original-demand tests.** Cover exact table identity, session/subject/lease, current metadata/config/credentials, database-bearing URL, default single-partition profile, override/callback rejection, redacted diagnostics, id-only access, hidden predicate/join demand, mask/filter ordering and an optimized-away source. Use exact issued sources, not class-name or schema trust.
- [x] **Step 2: Implement the wrapper, metadata observer and finite inventory.** Reuse native catalog initialization/conversion and typed credentials. Resolve metadata through the owned client after application visibility checks; defer payload authorization until completed demands. Observe fresh configuration around native load and existing action/precommit checks. Keep the route rejected at execution until the following native continuation is complete.
- [x] **Step 3: Write failing real native JDBC read/append tests on the lead pair.** Use an isolated owned PostgreSQL fixture with a read-only extraction principal and trusted packaged driver. Assert the actual V2 relation -> V1ScanWrapper(JDBCScan) -> RowDataSourceScanExec/JDBCRDD chain, ordinary bounded collect, both writers, current_timestamp and one JDBC/Delta INNER join. Inspect exact rows and one native commit; no mock-only native success.
- [x] **Step 4: Extend logical policy/receipt/preparation together.** Capture per-occurrence authored columns before pushdown; use nonexecuting Kyuubi extraction tokens only. Bind original options and all stock scan/wrapper/predicate/schema/partition fields; reject aggregates/limits/samples before their callbacks. Separate JDBC-only preparation evidence from Delta native metadata authority, preserving original obligations even with no surviving scan.
- [x] **Step 5: Add the action-local stock planning continuation and APPEND revalidation.** Invoke exact stock DataSourceV2Strategy once for a verified PhysicalOperation recipe, retain its native subtree, anonymous relation and RDD, and reuse those through qualified planner replay/AQE. Never relax generic RDD equality. Extend existing `validateParticipants`/`validateRetainedOwned` and cleanup to every source kind; keep target transaction/publication unchanged.
- [x] **Step 6: Run focused lead tests and affected controls.** Include AQE on/off, both file-skipping profiles, empty/pruned input, late source revoke/config change, target UPDATE/ALTER denial before payload, foreign/copy-tagged source, unsupported raw/external/RDD sources and retained target-cache behavior. Preserve original failures. Do not rerun closed unrelated matrices.
- [x] **Step 7: Format, document supported scope, review and commit.** Add consumer instructions to the companion README and update the results file/coverage register. Scoped formatting and `git diff --check` must pass. This task's installed evidence does not close Task 3.

## Task 3: Qualify actual-service registered ingestion on all pinned pairs

**Files:**
- Create: `delta/acceptance/registered_jdbc_live.py` (owned source fixture/case supervisor).
- Create: `delta/acceptance/registered_jdbc_checks.py` and `test_registered_jdbc_checks.py` (exact inventory/provenance/outcomes).
- Extend existing package/artifact/caller support only where required; reuse its process ownership, Ranger/Gravitino/MinIO readiness and immutable evidence conventions.
- Modify: registered-JDBC results, plan and `docs/delta-spark-parity.md`.

**Interfaces:**
- Consumes Task 2's opt-in and native APIs plus Task 1's public grant pushdown. All package/class/helper/source receipts refer to original executed bytes.
- Produces per-pair classic Python records for real source grant, named source -> native APPEND, and real source/target denial; lead additionally covers both writers/mixed join/final-column composition/timestamp/source revoke/config change.

**Qualification inventory:** The lead pair is Spark 3.5.6 / Delta 3.3.2. All four pairs run `jdbc_insert_into`, `jdbc_source_select_denied` and `jdbc_target_update_denied`. Lead additionally runs `jdbc_mixed_save_as_table`, `jdbc_column_composition`, `jdbc_source_revoked_before_publish`, `jdbc_config_changed_before_publish` and `jdbc_view_rejected`: eight lead cases plus three per remaining pair, seventeen cases total. Positive insertion includes native direct timestamps. Mixed saveAsTable proves both source policies and existing native target-cache refresh with a fresh native query. Column composition records three distinct actions: allowed id-only read, denied hidden-column predicate, and denied pruned/empty original demand; it must not collapse them into one unexamined success. Late source/config cases use the existing prepublication barrier after payload and retain residue without replay. The same-shaped view has genuine metadata/SELECT grants and must reject for the physical-table guard with no payload/publication.

Provision a second owned PostgreSQL catalog with the same remote schema/table and distinct policies; source grant/revoke checks must preserve its policies, Delta target policies and unrelated masks/filters. A grant in the second catalog must not authorize the denied first catalog. Use a finite explicit inventory; never run the old default 26-case matrix. Retain Task 2's exact production evidence for both Delta 3.3.2 pairs; run the remaining two Delta 3.3.0 installed cells on the final production bytes and qualify all four actual-service/Python cells. Any required product correction invalidates affected reuse and gets focused requalification.

- [x] **Step 1: Add checker tests for missing cases, wrong source identity/version/artifacts, fake denial causes, extra/missing commits and source/target confusion.** Meaningful negative checker fixtures must fail before implementing checks.
- [x] **Step 2: Package an isolated PostgreSQL source and public Gravitino grant fixture.** Never reuse Ranger's DB or unrelated databases. Record trusted driver identity and exact versions, keep secrets private, and distinguish metadata discovery from payload fetch. Provision Ranger-authored masks/filters separately from Gravitino-pushed SELECT.
- [x] **Step 3: Run focused installed qualification on remaining version pairs, then actual classic PySpark cases on all four pairs.** Use original processes and current packages serially; no concurrent builds/services mutation. Each positive writes once with expected masked/filtered rows; real denials produce no published version. Verify source catalog isolation and exact original policy resources.
- [x] **Step 4: Independently validate artifacts/process exits/native outcomes and close owned fixtures.** Preserve shared services and original evidence. Retain failed attempts, uncommitted payload residue and uncertain postcommit outcomes without replay. A checker correction may revalidate originals only with an explicit source delta, never relabel package bytes.
- [x] **Step 5: Review the task and whole milestone, update limitations and push the reviewed milestone.** User already authorized milestone pushes; retain local worktrees/evidence. No PR, merge, deployment, full Spark Hub/M2 or external-JDBC completion claim.
