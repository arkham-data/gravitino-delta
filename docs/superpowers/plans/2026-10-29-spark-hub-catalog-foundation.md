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

# Spark Hub Catalog Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Preserve the user's existing execution-method choice.

**Goal:** Produce an explicit workload coverage register and an actual read-only Spark Hub route through named Gravitino Delta tables, without misrepresenting batch-write adoption as complete.

**Architecture:** Add optional typed runtime configuration and an explicit dataset-name resolver in Spark Hub. A small independent catalog service uses Spark's native named read and the installed companion. A real classic-PySpark probe calls those production classes against isolated Gravitino/Ranger/MinIO fixtures; existing write services remain outside this first slice.

**Tech Stack:** Python 3.11, Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server/client 2.8.0 and MinIO.

**Spec:** [Proposed adoption design](../specs/2026-10-29-spark-hub-adoption-design.md), especially the M0/M1 contract. [Roadmap](2026-10-29-spark-hub-adoption.md).

Status: proposed and not executed. This task plan covers M0/M1 only. Its source-level interfaces are planning decisions, not existing APIs. Production deployment, write support and Glue retirement are not delivered by it.

## Global Constraints

- Target Spark 3.5.4 and 3.5.6, Delta 3.3.0 and 3.3.2, Java 17, Scala 2.12.21, and actual Ranger server and client 2.8.0.
- Use S3-compatible storage; local acceptance uses MinIO. RustFS is an optional separately recorded storage profile, not evidence of AWS S3 or MinIO equivalence.
- Gravitino owns registration. Delta owns native files, log actions, transaction semantics and table features. Do not add a second authoritative metastore.
- Do not modify Gravitino core/API/server/client/storage schemas, fork Spark or Delta, or build a replacement transaction engine.
- Use the current trusted-stock-Spark execution model. Preserve original source authorization and operation-boundary checks; do not introduce exact per-stage authorization or custom adaptive execution.
- Preserve existing Ranger row filters, masks, column permissions and canonical resources. Masks may affect keys and CDF metadata. Do not require blanket unmasking or introduce raw-key/raw-row bypasses.
- Destination row filters retain read semantics, not a new insertion/update constraint. A filtered-out target row does not automatically suppress an authorized insert.
- One writing job per dataset is enforced by the platform. This does not imply that sources cannot advance while downstream jobs run.
- Preserve platform operations and options in the acceptance requirements. Do not silently drop schema options, disable auto-compaction, delete checkpoints or weaken policies to obtain a passing result.
- Classify every outcome as committed, not committed, or unknown using native evidence. Do not automatically replay an unknown or known-committed write.
- Keep the running append-AQE milestone separate. Do not rerun unchanged successful matrices for documentation, planning or checker-only changes.

Additional M1 bound: read-only product service, pre-existing PUBLISHED registrations, no job_wrapper backend switch, no Spark Hub writes/DDL/streaming, and no changes to the default legacy route. Fixture provisioning/public grants are isolated test setup, not production migration.

## Review Focus

1. A valid-looking dataset name resolves to the wrong UUID mapping — Task 1 duplicate/cross-binding and Task 3 exact-routing tests.
2. Native Delta extensions or an existing session silently override the governed profile — Task 2 profile-conflict and Task 4 installed-origin checks.
3. Missing registration or denied SELECT falls through to raw paths/Glue — Task 3 poison-fallback tests and Task 4 native-state/network boundary observation.
4. Row masks/filters are bypassed through a convenience count/view/read — Task 4 real values and column-demand denials, within the existing ordinary-read grammar.
5. A missing case, failed process or substituted artifact is called a successful milestone — Task 4 checker mutation tests over the exact finite inventory and source/artifact identities.

## Repositories and file ownership

`S` means an execution-time isolated Spark Hub worktree based on the reviewed `74b7115` baseline or a deliberately re-audited successor. `G` means an isolated Gravitino worktree based on the reviewed append-AQE closure. Preserve existing untracked Spark Hub files. This planning checkout changes documentation only; implementation must not use the ongoing frozen test worktree.

| File | Responsibility |
| --- | --- |
| G `delta/acceptance/spark_hub_coverage.json` | Versioned finite operation and source-caller register; gaps remain explicit. |
| G `delta/acceptance/spark_hub_coverage.py` | Pure register validation and summary, no Spark/service launch. |
| S `src/shared/schemas/v3/gravitino.py` | Immutable dataset bindings and an optional runtime profile; no Spark/AWS imports. |
| S `src/shared/services/v3/gravitino_runtime.py` | Validate and apply the supplied reviewed startup profile; no downloads or policy decisions. |
| S `src/shared/services/v2/spark.py` | Optional profile integration; preserve the existing constructor calls and legacy path. |
| S `src/shared/services/v3/gravitino_catalog.py` | UUID/name routing and native named reads; no inheritance from the Glue-constructing ArkhamCatalog. |
| G `delta/acceptance/spark_hub_read_pyspark.py` | Actual caller of the packaged Spark Hub production classes. |
| G `delta/acceptance/spark_hub_read_live.py` | Isolated fixture/package/process orchestration and finite case selection. |
| G `delta/acceptance/spark_hub_read_checks.py` | Independent evidence checks. |

## Task 1: Pin workload requirements and dataset binding contract

**Files:** Create G `spark_hub_coverage.json`, `spark_hub_coverage.py`, `test_spark_hub_coverage.py` under `delta/acceptance`; create S `src/shared/schemas/v3/gravitino.py` and `tests/unit/test_gravitino_bindings.py`.

**Interfaces:** `validate_coverage(document: dict) -> None`; `coverage_summary(document: dict) -> dict`. Frozen Python dataclass `DatasetBinding(dataset_id: UUID, catalog: str, schema: str, table: str)` with `qualified_name() -> str`. `validate_bindings(bindings: Sequence[DatasetBinding]) -> dict[UUID, DatasetBinding]` rejects duplicate UUIDs and duplicate canonical names. These bindings are routing inputs, not a new metadata store.

- [ ] Write failing tests for malformed schema version, missing required fields, duplicate case IDs, unsupported cases relabeled passed without evidence, and unresolved predecessor IDs. Register fields: `schemaVersion=1`, repository revisions, case ID, production `path/symbol`, milestone, API, plan family, options/features/settings, policy profile, expected outcome, coverage status and evidence references. Status values: `supported-building-block`, `adaptation-required`, `connector-gap`, `unqualified`, `qualified`; the last requires original runtime evidence.
- [ ] Include cases for named read, raw-path/Glue migration, external ingestion/create, append options, overwrite/schema evolution, snapshot deletion, transformed/checkpointed merge, bounded CDF/bootstrap/state, generated partitions, auto-compaction, retention/VACUUM, manifest handling, streaming and deployed identity/session behavior. Mark optional unused optimize/delete utilities separately. Use the audited source symbols; do not claim runtime support from method existence.
- [ ] Write binding tests: exact UUID maps to exactly one three-part name; each identifier component is quoted with embedded backticks doubled; empty/NUL components reject; duplicate UUID/name reject; an S3 location is not accepted as an identifier component. Unknown UUID raises `KeyError`, never synthesized path fallback.
- [ ] Run G `python3 -m unittest discover -s delta/acceptance -p test_spark_hub_coverage.py` and S `python3 -m pytest tests/unit/test_gravitino_bindings.py -q`; confirm assertion-based RED, implement pure validation/types, then GREEN. Do not import production job modules or access services in these tests.
- [ ] Commit the bounded changes separately in each repository and report the exact source revisions and coverage limits for review.

## Task 2: Add an explicit optional Spark startup profile

**Files:** Extend S `src/shared/schemas/v3/gravitino.py`; create `src/shared/services/v3/gravitino_runtime.py`; modify `src/shared/services/v2/spark.py`; create `tests/unit/test_gravitino_runtime.py`.

**Interfaces:** Frozen `GravitinoRuntimeProfile(spark_version: str, delta_version: str, spark_conf: Mapping[str, str])`, with configuration omitted from repr. `validate_profile(profile) -> None`; `configure_builder(builder, profile) -> Any`; `validate_session(session, profile) -> None`. Extend `SparkClient.__init__(app_name, spark_config=None, *, gravitino_profile: Optional[GravitinoRuntimeProfile]=None)` without breaking existing positional callers. No new caller identity is inferred from group/project IDs.

- [ ] Run GitNexus upstream impact for `SparkClient` before editing and identify the actual callers that must retain legacy constructor behavior. Record the blast radius; do not refactor unrelated methods.
- [ ] Write failing tests for the legacy path, valid pinned profile, unsupported pair, missing enablement flag, reversed/duplicate extensions, conflicting stock RangerSparkExtension, startup-setting conflict, incompatible reused session and safe error/repr behavior. Both `spark.gravitino.enableDeltaSupport` and `spark.gravitino.enableDeltaRangerSupport` must equal `true`; companion extension precedes Delta's exactly once. Preserve explicit caller AQE=true. Full platform/artifact/authentication validation remains enforced by the current companion and later live checks.
- [ ] Implement profile application before session creation. When the optional profile is present, use preinstalled qualified artifacts and do not call `configure_spark_with_delta_pip` to resolve new packages. With no profile, retain the current legacy path. Refuse conflicting profile values instead of silently reconciling them. Validate the resulting session; do not repair an incompatible already-created session.
- [ ] Run S `python3 -m pytest tests/unit/test_gravitino_runtime.py tests/unit/test_gravitino_bindings.py -q` RED then GREEN; also run existing affected SparkClient-construction tests selected from the impact result. Test doubles establish Python configuration behavior only, not Ranger/runtime qualification.
- [ ] Document the optional profile and its read-only first use; run GitNexus change detection before the scoped Spark Hub commit. No default backend switch, implicit downloads or credentials in logs.

## Task 3: Implement shared named reads without Glue fallback

**Files:** Create S `src/shared/services/v3/gravitino_catalog.py` and `tests/unit/test_gravitino_catalog.py`.

**Interfaces:** `GravitinoDatasetCatalog(spark: SparkClient, bindings: Sequence[DatasetBinding])`; `resolve_dataset(dataset_id: UUID) -> DatasetBinding`; `get_table_df_by_id(id: UUID) -> DataFrame`. Consume Tasks 1/2 types. No `write_dataset` or native-table-management method is promised in M1.

- [ ] Write failing exact-routing tests using two dataset UUIDs and two explicit names. Verify `session.table(binding.qualified_name())` receives the correct identifier and returns the native frame unchanged. Missing/duplicate binding must fail before any Spark read.
- [ ] Poison `AWSGlue`, `DeltaTable.forPath`, `read.load`, native table creation and mutation entry points in unit tests. Confirm construction/lookup never invokes them, including when `session.table` throws missing-table or authorization errors. Preserve the original error; do not reinterpret denial as absence.
- [ ] Implement the small read service; avoid inheriting `ArkhamCatalog` because its constructor eagerly creates AWSGlue. Do not reroute `job_wrapper` until the later write contract exists.
- [ ] Run S `python3 -m pytest tests/unit/test_gravitino_catalog.py tests/unit/test_gravitino_runtime.py tests/unit/test_gravitino_bindings.py -q` RED then GREEN. Run change detection, commit and obtain task review.

## Task 4: Qualify the actual shared read path and publish bounded coverage

**Files:** Create G `delta/acceptance/spark_hub_read_pyspark.py`, `spark_hub_read_live.py`, `spark_hub_read_checks.py`, `test_spark_hub_read_checks.py`; update `spark_hub_coverage.json` and acceptance README. Package the exact S source files and dependencies used by the real Spark Hub imports; exclude settings values/credentials and unrelated untracked files.

**Interfaces:** `run_cell(runtime_path: Path, spark_hub_root: Path, inputs_path: Path, output: Path) -> dict`; CLI flags `--runtime --spark-hub-root --inputs --out`. `verify_result(result: dict) -> None`. Reuse existing manifested runtime checks, public fixture helpers and process capture from `cdf_live.py`, `cdf_merge_live.py`, `cdf_read_live_package.py` and `read_parity_live.py`; do not clone the entire harness or rerun its prior cases. Add a helper only for a missing boundary needed by this real caller.

- [ ] Define exactly six read-case IDs: `named-filter-mask`, `projected-count`, `column-denied`, `unknown-binding`, `unpublished-registration`, `fresh-action-revoke`. Each case records the actual Spark Hub method, source binding, current authority, expected outcome and original process/artifact references. No rejected/unsupported case is a successful read. Revoke uses a newly constructed supported action after confirmed applied policy refresh.
- [ ] Create behavioral checker RED tests: remove a case/runtime/original exit, replace a case with another successful case, substitute source hashes, supply wrong identity/policy download or wrong masked rows, hide a failed caller exit, or attach only metadata AQE. The full gate must reject; lightweight unit fixtures must be explicitly nonqualifying. Implement and run `python3 -m unittest discover -s delta/acceptance -p test_spark_hub_read_checks.py` to GREEN.
- [ ] Package the actual Task 2 SparkClient and Task 3 catalog service. The Python application must call those classes and their native DataFrame methods, not duplicate their behavior in a Scala bridge. Use a pre-existing PUBLISHED fixture with rows `(1, 'east', 'a')`, `(2, 'west', 'b')`, `(3, 'east', 'c')`, Ranger filter `region = 'east'` and email mask `'MASKED'`. For `named-filter-mask`, load the same dataset through two fresh service calls, alias them `l`/`r`, join on `l.id = r.id` and select `l.id`, `l.region`, `l.email`. The expected multiset is exactly `(1, 'east', 'MASKED')`, `(3, 'east', 'MASKED')`. Its fixture launcher explicitly sets both static/adaptive broadcast thresholds to `-1` and shuffle partitions to `4` before session creation, giving an exchange-bearing positive control without a product setting toggle. A separate id-only projected count is 2 under id-only SELECT. A column-denied case demands email and requires actual denied SELECT for that resource; unknown binding must make no table request. The unpublished case must reject rather than path-load data. Check native source identity/version/row multiset remains unchanged by application reads.
- [ ] Run the actual six-case cell serially for all four pinned Spark/Delta pairs with Ranger server/client 2.8.0 and MinIO. Verify the actual installed source/package identities, configuration and named Delta catalog, filtered/masked values, original outcomes and policy application. Record final stock AQE payload for an existing supported exchange-bearing read in the allowed case; do not count setup/readback AQE as that proof. Setup principals/public-grant work remain distinct from the application identity.
- [ ] Prove the product read path did not call Glue/Athena or a native path API using retained adapter call boundaries plus isolated service observations; ordinary S3 reads by Delta's named scan are expected. Do not confuse governed native storage access with forbidden user path fallback. Include the fixture source publication and exact name-to-UUID mapping.
- [ ] Update only M1 cases to qualified when the independent checker and review pass. Preserve every other M0 gap. Report that Spark Hub write/streaming/deployment adoption remains incomplete; no existing pipeline is switched automatically.
- [ ] Complete task and milestone reviews, commit, and push only reviewed milestone commits under standing authorization. Preserve both worktrees and evidence. Do not create a PR without the repository's preview/confirmation flow.

## Self-review and next dependency

The first slice deliberately does not claim history/head metadata, CDF cursor migration, general source plans or writes. All new product symbols have focused tests and later actual caller coverage; the source/UUID boundary, profile conflicts, fallback denial, policy behavior and incomplete-evidence cases have owning tests above. The final live runtime is built after product changes are frozen; documentation closure has separate provenance.

M2 is the next implementation design: actual `write_dataset` ingestion/create/append/overwrite, option preservation and safe native publication. Its implementation must not be replaced with more read-only examples. M3/M4 then close real incremental composition/state behavior before any production batch-adoption claim.
