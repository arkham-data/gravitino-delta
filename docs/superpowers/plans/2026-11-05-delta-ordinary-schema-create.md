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

# Ordinary Governed Delta Schema CREATE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Create ordinary named Delta tables from SQL and classic PySpark StructType with exact Ranger authority and Gravitino publication.

**Architecture:** Extend the existing issued CREATE declaration and native proof; reuse the base stage/provider. Add positive-only IF NOT EXISTS metadata observation and one method on the existing Python/JVM bridge. Qualify the real entry points against existing services.

**Tech Stack:** Java17, Scala2.12.21, Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, Ranger2.8.0, Gravitino, MinIO, JUnit5, Python unittest/PySpark.

**Spec:** [Ordinary CREATE design](../specs/2026-11-05-delta-ordinary-schema-create-design.md).

## Global Constraints

- No Gravitino core/API/server/storage-model changes, engine fork, manual transaction, raw-path destination, Glue compatibility or automatic native/PUBLISH replay.
- Existing native Delta stage and Gravitino RESERVE/PUBLISH own effects. Preserve issued command/context/subject/settings/receipt/native/physical proof and current literal CREATE at each existing boundary.
- Only the design's ordinary schema-only profile; no CTAS/payload create, generated/date columns, mapping name or authentication expansion. Group-only SIMPLE CREATE remains unsupported.
- SQL and Python/JVM calls use the same Spark analysis and issued CREATE; no final SELECT/DeltaTable.forName requirement. Existing MERGE API remains compatible.
- Qualify all four pinned pairs with actual Ranger server/client2.8 and MinIO, preserving original evidence and source/runtime bindings. No full M2/Hub/Delta or throughput claim.
- Use the existing isolated worktree/branch at f45ab5221; controller owns commits/review/push. One implementer at a time; no worker subagents or parallel build/test/index/service mutations.

## Review Focus

1. Nullability/comment/default normalization changes a schema after reservation — Task1 tests compare original declaration, native metadata and published projection.
2. Equal partition counts conceal different names/order or mutable references — Task1 tests exact immutable identity declarations and pre-effect rejection.
3. Metadata403/unavailability/hidden reservation is treated as absent or already-created — Task2 tests each error and insists on positive published no-op only.
4. Python name/schema conversion or final observation causes duplicate mutation — Task2 real gateway tests three-part/quoted names, exact StructType and terminal failure.
5. Session defaults/AQE introduce unqualified native work or stale effective configuration — Task1 positive Hub profile and drift tests, Task3 actual-runtime checks; no work-disabling workaround.

## File map and verification conventions

B=`spark-connector/v3.5/spark-delta/src/main/java/org/apache/gravitino/spark/connector/delta/`.
S=`spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`.
J is the corresponding companion `src/main/java` package; T is its `src/test/scala` package.
Python=`spark-connector/v3.5/spark-delta-ranger/python/`.
Use fresh task evidence directories, exact command/exits/XML/hashes, Java17, one worker and resource limits already established. Initial Gradle flags: `-PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs --max-workers=1 --console=plain -Dorg.gradle.jvmargs=-Xmx2g`; JAVA_TOOL_OPTIONS processor2/metaspace768m. Scoped SpotlessApply/Check; new public/protected Java Javadoc. Do not change unrelated historical tests to manufacture a full-suite green result.

### Task 1: Ordinary declaration and exact native binding

**Files:** Modify B/DeltaCreateLifecycle.java, B/GravitinoDeltaCatalogSpark35.java, S/DeltaRangerCreateCommand.scala, S/DeltaRangerCreateMutation.scala and existing CREATE fixture/helper/tests. Add S/DeltaRangerCreateDeclaration.scala for immutable ordinary declaration validation/effective metadata, T/TestDeltaRangerOrdinaryCreate.scala, and `spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-ordinary-schema.json`. Change CREATE-specific native recipe dispatch only if a focused failing test proves a required shape; retain all other owners.

**Interfaces:** Keep the existing Intent constructor. Add immutable `Optional<List<String>> identityPartitionColumns()` evidence and an overloaded public Intent(String catalog, List<String> namespace, String table, String schemaJson, Transform[] partitions, Map<String,String> properties) constructor that captures known stock top-level identity transforms without retaining caller arrays/objects; unsupported forms yield no companion evidence while the no-op base path stays compatible. The legacy count-only constructor supplies no exact partition evidence. The overload supplies an immutable empty list for zero partitions. Public static `Intent.snapshotIdentityPartitionColumns(Transform[]): Optional<List<String>>` exposes the same pure snapshot operation for the companion; both constructor and declaration use it, avoiding a dummy Intent or duplicated extraction logic. `DeltaRangerCreateDeclaration` owns frozen StructType JSON, ordered canonical partition names, converted properties/comment and effective native configuration; command and mutation consume the same instance. No independent authority in this value.

- [ ] **Step1: Write and run RED** `TestDeltaRangerOrdinaryCreate.ordinarySqlCreatePublishesExactDeclaration`: multi-column schema with non-null long, string, timestamp, decimal, nullable scalar array/map, column/table comments, primary_key, nondefault retention, CDF and two ordered identity partitions. Use actual native stage and scripted registration, AQE/defaults from design; assert CREATE0/checksum and exact publication projection, no payload/version1 or final SELECT. Also pin old `(id BIGINT)` profile as regression.
- [ ] **Step2: Implement immutable declaration/partition capture** with defensive copying and stock validation. Existing Intent API and no-op base behavior remain compatible. Add tests for equal-count wrong partition names/order, mutated input arrays/maps/schema, unknown/nonidentity/nested transforms and invalid columns before RESERVE. Unsupported transform inspection must not execute arbitrary custom getters.
- [ ] **Step3: Extend the existing command/native proof** to the design's schema/property/comment/runtime profile. Freeze approved defaults and use native normalization helpers where available. Compare exact native config/description/ordered partitionBy and schema to the same issued declaration and receipt. Support both qualified old and Hub CREATE settings; add no generic metadata/AQE exemption.
- [ ] **Step4: Cover boundaries** using existing phase fault seams: mixed nullability, zero/one/two partitions, comments with quote/Unicode, primary_key absent/present, retention bounds, session-default override, invalid/unsupported metadata, changed settings/partitions, real literal deny and post-log0 failure/no replay. Preserve current policy-refresh allow/deny/overlap tests and outcome semantics.
- [ ] **Step5: Verify and review** focused ordinary+existing CREATE command/metadata suites, base lifecycle/catalog tests affected by Intent, and two current governed append success/revocation controls. Preserve RED/failed attempts, source/artifact/native evidence. Controller gets a fresh task review then commits `feat(delta): support ordinary governed schema declarations`.

### Task 2: Positive-only no-op and classic PySpark schema creation

**Files:** Modify J/DeltaRangerCatalogSpark35.java, S/DeltaRangerCreateCommand.scala, S/GravitinoDeltaTable.scala, Python/gravitino_delta/tables.py and existing Python tests. Add T/TestDeltaRangerCreateIfNotExists.scala and T/TestGravitinoDeltaCreate.scala. Reuse the current installed CREATE helper. Task3 owns the actual PySpark acceptance driver and service bootstrap, avoiding a partial test-helper dependency in this task.

**Interfaces:** Package-private `Optional<DeltaRegistrationReceipt> publishedForCreate(Identifier id)` uses existing metadata.read lease bracketing and validates literal target/catalog/provider/table name/state. Package-private `requireCreateMetadataLive()` checks the same nonrenewable metadata lease after final CREATE reauthorization/context validation; expiry during authorization remains an error. Python `GravitinoDeltaTable.create(spark, name, schema, *, partition_by=None, properties=None, comment=None, if_not_exists=False) -> None`; JVM `GravitinoDeltaTable.create(SparkSession, String, StructType, java.util.List[String], java.util.Map[String,String], String, Boolean): Unit`. Comment may be null; all collections copied. API version remains1 for additive compatibility; report a missing create entry clearly without disguising execution failures.

- [ ] **Step1: Write/run RED no-op tests**: supported differing declaration against PUBLISHED is a zero-effect no-op; plain CREATE conflicts; absent target creates; denied/unavailable/expired/malformed/pending metadata and literal CREATE denial never report success. Cover lease/context change and absence→RESERVE concurrent-winner conflict without retry.
- [ ] **Step2: Implement the companion-only no-op branch** after literal CREATE authorization and before any stage/journal creation. Catch only actual missing table load. Require positive PUBLISHED receipt and reauthorize before no-op return. Actual-create branch retains its independent mutation/publication success checks; do not fabricate lifecycle callbacks or published state.
- [ ] **Step3: Write/run RED frontend cases** for canonical named ordinary creation from a StructType decoded using Spark's JSON codec, Python gateway argument preservation, and a focused stock builder naming/final-observation negative. Installed JVM tests exercise the real command; actual PySpark-to-services success is Task3, not a mock-test claim. Add one additive bridge method using Spark schema JSON codec, multipart parser, standard CreateTable and normal issuance/command execution. No hand schema serializer, direct staging or returned table handle. Use native StructType input; no payload DataFrame execution required for declaration.
- [ ] **Step4: Test Python argument transport and installed JVM behavior.** Cover copying/types, quoted names, Unicode/comments, nullability/collection metadata and no-op. Use one bounded actual Py4J lookup check for missing method/companion transport semantics; unit doubles cover API-version/type/copy transport, while installed JVM cases reject unsupported schema, wrong session/config and copied/reused command. Preserve MERGE facade tests. Verify a publication followed by explicit read denial is not replayed or mislabeled rollback. Do not catch native failures as missing-API errors.
- [ ] **Step5: Verify and review** focused JVM CREATE/no-op/frontend suites and `PYTHONPATH=<Python> python -m unittest discover -s <Python>/tests`, with actual native execution in the installed JVM frontend test. Document that real PySpark/service qualification remains Task3, and record exact driver/module interfaces for that task. Document exact supported API. Fresh task review; controller commits `feat(delta): add governed schema creation to PySpark`.

### Task 3: Qualify ordinary SQL/PySpark creation on actual services

**Files:** Extend `delta/acceptance/create_live.py`, `create_live_checks.py`, `test_create_live_checks.py` and acceptance/companion READMEs. Add `delta/acceptance/create_pyspark.py`, following existing `cdf_merge_dataframe_pyspark.py`/`append_aqe_pyspark.py` provenance patterns. If required, extend the existing test-only J/DeltaRangerDownloadedQuery.java session bootstrap and its focused tests; do not clone an operation-specific JVM fixture framework. Add `docs/superpowers/specs/2026-11-05-delta-ordinary-schema-create-findings.md`. Reuse source-bound package lane and existing four stock runtimes/Python environments; old evidence stays historical.

**Interfaces:** Extend the existing finite CREATE runner with explicit ordinary fixture/frontend inputs and strict validation; retain original simple profile and unsupported-group classification. Actual SQL/Python invocations and package/module hashes must be distinguishable. Existing driver/transport remains; no direct seeding, ADOPT, PUBLISH fabrication or new auth framework. The current downloaded-query main exposes only a stdin SQL loop; its setup/security/readiness helpers can be reused through test-only `DeltaRangerDownloadedQuery.openPythonSession(String requestFile, String outputDirectory): PythonSession`, with `session(): SparkSession`, `complete(String id, Throwable failure): void` and `close(): void`. A null failure means normal API return. Keep this handle in the existing helper family, reuse original startup/download/result/cleanup logic and preserve primary exceptions; it is evidence plumbing, not command authority. Python must invoke the real public create API on the same classic pinned thread, not submit SQL on its behalf. Record exact module/archive/PySpark/Py4J/runtime origins. A SELECT-only literal deny can prove no-op succeeds without effective SELECT despite owner ALL; verify the separate read refusal without changing the no-op outcome.

- [ ] **Step1: Checker RED/green** rejects wrong ordinary schema/nullability/comment/partition order/config/defaults, forged no-op without positive PUBLISHED evidence, native/version mutation on no-op, wrong frontend/wheel/runtime/fixture and stale downloads. Keep positive absent-target proof and exact native/registration axes.
- [ ] **Step2: Fresh assembly/lead pair** runs actual SQL and PySpark ordinary CREATE, positive differing-declaration IF NOT EXISTS, plain duplicate conflict, grant/deny/revoke and field-boundary cases. Use original500ms polling, actual Ranger2.8/MinIO, fresh names and current source. Native profile includes AQE/defaults as specified. Retain native0/checksum/metadata/provider STATUS plus authoritative no-op zero mutation evidence.
- [ ] **Step3: Remaining pairs** run decision-relevant ordinary SQL/PySpark success/no-op/deny with actual Spark/native/Python artifacts. Reuse only byte-verified unchanged production assemblies; no full fault matrix repeats. No-op targets are intentionally reused only for read-only no-op verification, never mutation replay of an uncertain/committed attempt.
- [ ] **Step4: Findings/review/closure** records exact schema/API/runtime scope and all limitations/outcomes. Run relevant checker/formatting checks, fresh task review, then final integrated review. Controller closes and pushes only when required cells pass; preserve worktree/evidence. Inputs/transformations and overwrite follow, ordinary creation is not full Hub/M2 closure.
