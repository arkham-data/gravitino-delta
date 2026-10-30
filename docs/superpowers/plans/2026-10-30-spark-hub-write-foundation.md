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

# Spark Hub M2 Native Write Characterization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the native write/create behavior and remaining integration gates before implementing Spark Hub's governed sink.

**Architecture:** Add a test-only stock Spark/Delta oracle driven by actual pinned Spark Hub option construction. Capture original process and native-state evidence, then independently check it. No production connector, Spark Hub routing, Ranger policy or transaction behavior changes in this task.

**Tech Stack:** First probe Spark 3.5.6, Delta 3.3.2, Java 17, Scala 2.12.21, Python 3.11 and MinIO. Later governed M2 acceptance covers all required pairs and actual Ranger 2.8.0.

**Spec:** [M2 design and first-task boundary](../specs/2026-10-30-spark-hub-write-foundation-design.md).

Status: Task 1 is complete and independently reviewed. All 15 native cases completed on Spark 3.5.6 / Delta 3.3.2 / MinIO; 29 focused checker/preflight tests pass. Original evidence was rechecked offline after bounded checker/launcher fixes. See [measured findings](../specs/2026-10-30-spark-hub-write-native-findings.md). Characterization is complete; native candidate viability and governed support remain false. M2 steps 2–7 remain open.

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
- Keep the closed append-AQE milestone separate. Do not rerun unchanged successful matrices for documentation, planning or checker-only changes.


## Review Focus

- Native staged CREATE drops userMetadata despite accepting an option — Task 1 compares actual native history for all three creation sequences.
- Empty or partitioned full refresh accidentally preserves old rows or changes UUID — Task 1 checks row multisets, removals, native identity and schema.
- Enabled autoCompact does no work, or a hook commits before raising — Task 1 requires action/version evidence and separates write and maintenance phases.
- Native duplicate suppression silently drops changed intent — Task 1 records both identical and changed-input duplicates; it cannot qualify safe application retry.
- Refresh overwrites schema options but preserves APPEND mode and original embedded metadata flags — Task 1 imports the real helper and observes the resulting write.

## Files and responsibilities

All created files are in the Gravitino adoption worktree. The Spark Hub worktree remains read-only.

| File | Responsibility |
| --- | --- |
| Create `delta/acceptance/spark_hub_write_native.py` | Fresh subprocess launcher, validated inputs/artifacts, actual Spark Hub import inventory, immutable result references. |
| Create `delta/acceptance/spark_hub_write_native_app.py` | Stock native scenario execution, original model/helper imports, before/after log and row observations. |
| Create `delta/acceptance/spark_hub_write_native_checks.py` | Pure evidence validation and bounded conclusion; import has no Spark/network/process side effects. |
| Create `delta/acceptance/test_spark_hub_write_native_checks.py` | Checker and launcher input negative tests using synthetic artifacts; never native qualification. |
| Modify `delta/acceptance/README.md` | Exact command, input/output contract and native-only scope. |
| Create `docs/superpowers/specs/2026-10-30-spark-hub-write-native-findings.md` | Measured decision and precise next task after runtime execution, including unsupported seams. |

Read/reference: `append_aqe_native.py` for stock runtime/MinIO identity and native observations; `spark_hub_read_pyspark.py:import_inventory` for provenance conventions; `cdf_read_live_package.py:verify_runtime` for installed runtime integrity; `s3_objects.py:OwnedObjects` for fresh owned prefixes; pinned Spark Hub `ExecutionProperties.to_df_properties`, `DeltaClient._resolve_write_options`, `WriteStrategy` and `ComputeProperties`. Do not change frozen predecessor manifests or assume the old read import inventory covers newly imported sink helpers.

## Task 1: Measure native compatibility and choose the next seam

**Interfaces:**

- `spark_hub_write_native.py`: `run(runtime_path: Path, spark_hub_root: Path, inputs_path: Path, output: Path) -> Path` returns the new result JSON path; refuses an existing output directory or nonempty target prefix. CLI: `--runtime`, `--spark-hub-root`, `--inputs`, `--out`, all required.
- `spark_hub_write_native_app.py`: `main() -> None` consumes one private request JSON path via argv; executes the fixed scenario register, writes immutable per-case observations and exits nonzero on unexpected/incomplete execution. No public product API.
- `spark_hub_write_native_checks.py`: `validate_inputs(inputs: dict) -> None`, `verify_observations(result: dict) -> dict`, `verify_result(path: Path) -> dict`. Fail with descriptive validation exceptions. The first two are pure; `verify_result` rehashes referenced artifacts and original process receipts without launching processes. Return keys `characterizationComplete`, `nativeCandidateViable`, `governedSupport` (always false), `gaps` (list). Complete observations may document a rejected candidate; they must not imply viable governance.
- Input JSON version 1: `schemaVersion=1`, `python` (absolute existing Python 3.11 executable), `primary` (`endpoint`, `region`, `bucket`, `credentialsFile`), `prefix` (fresh exclusive MinIO prefix), `nativeIdentity` (fixture actor). Endpoint is the configured local MinIO service; require HTTP loopback/local Docker fixture host, reject AWS/production endpoints. Credential values stay in the existing private file. No arbitrary scenarios, paths, code callbacks or live-policy inputs.
- Runtime input is an existing verified native runtime manifest; derive sparkHome/JAR hashes from it. Verify exact pins in the child JVM and actual PySpark imports. New probe source hashes and the normal Spark Hub helper import closure are recorded separately. Do not duplicate old runtime packages or mutate their manifests.
- Result version 1: pinned G/S revisions, runtime/JAR/source/import references, effective settings before/after, original command/exit/stdout/stderr references, case observations and raw-native evidence references. Every case records target UUID/version before and after, schema/properties/protocol, add/remove/txn/commitInfo actions, row multiset and exception if any. Data, metadata/create and maintenance outcomes are separate enums `committed|not-committed|unknown`; absent/unexecuted phases are explicitly identified rather than asserted not committed.

- [x] **Step 1: Add failing contract tests.** In `test_spark_hub_write_native_checks.py`, add `test_rejects_existing_output_or_nonempty_prefix`, `test_rejects_wrong_pins_or_changed_artifact`, `test_requires_every_case_and_original_process`, `test_empty_refresh_requires_empty_rows_same_uuid`, `test_compaction_setting_without_commit_is_insufficient`, `test_exception_without_readback_is_unknown`, `test_duplicate_suppression_does_not_prove_intent`, `test_refresh_append_and_original_metadata_flags_preserved`, and `test_native_report_never_claims_governance`. Assert the exact interface/enum distinctions above; use independently altered synthetic reports for negative cases. Test imports must not load PySpark or start services.
- [x] **Step 2: Verify those tests fail for missing implementation.** Run `python3 -m unittest discover -s delta/acceptance -p 'test_spark_hub_write_native_checks.py' -v`. Retain the failure; it must be missing behavior rather than unrelated dependency/setup failure.
- [x] **Step 3: Implement the launcher, fixed scenario register and evidence checker.** Use the interfaces above. Import real Spark Hub helpers in the child with ordinary dependencies and without constructing `DeltaClient`, AWS clients or settings dumps. Record exact output from `to_df_properties` and `_resolve_write_options`; execute `.options(**actual_options)` without JSON-normalizing userMetadata. Instantiate WriteStrategy to preserve the actual mode. Run stock Delta without the Gravitino Ranger extension in this explicitly native-only process. Give every independent scenario a distinct owned location and each sequential operation a retained before/after observation.

  Required scenario IDs and observations:

  | IDs | Action and decisive observation |
  | --- | --- |
  | `legacy-create`, `stage-create`, `absent-create-or-replace` | Compare DeltaTable empty create followed by data write with stock DeltaCatalog V2 create/createOrReplace at separately fresh targets. CDF and 45/364 retention defaults; record native metadata/history/userMetadata and commit count. Absent-only candidate must refuse a pre-existing target in the probe launcher. Success alone does not select it for production. |
  | `append`, `append-evolve`, `append-explicit-false` | Original Spark Hub options, new field for evolution, explicit false rejects incompatible schema with no data commit; retained userMetadata string and loaded-at column. |
  | `overwrite`, `overwrite-empty`, `overwrite-partitioned`, `overwrite-explicit-false` | Full native overwrite preserves UUID, removes prior rows, applies schema behavior and preserves native properties. Empty input has explicit schema. Partitioned case includes an untouched old partition that full overwrite must remove. |
  | `refresh-append`, `auto-merge-precedence` | Actual FULL_REFRESH and STEP_REFRESH helper output, unchanged APPEND mode and embedded original flags; explicit writer false versus session autoMerge=true; compare settings before/after success and failure. |
  | `duplicate-same`, `duplicate-changed` | Same native txnAppId/version repeats exact intent, then changed rows/options; observe suppression/version/rows. Record native behavior without asserting it validates intent. |
  | `ordinary-hooks` | optimizeWrite=true, autoCompact=true, symlink hook default enabled. Seed sufficient tiny files under declared bounded test thresholds. Require actual compaction actions/version evidence and native manifest observation; settings-only is incomplete. Record opaque/hidden hook outcomes as gaps. |

  For initial staging candidates, use stock `DataFrameWriterV2` with native DeltaCatalog in the session catalog and a fixture-owned location; this compares native semantics only. Do not claim that invoking this route tests Gravitino reservation authority. Where a candidate rejects actual options, retain rejection and native readback rather than deleting the option or rewriting production code to pass.

- [x] **Step 4: Run focused checker/input tests and format the touched files.** Repeat the unittest command. Run `git diff --check`; apply repository formatters to touched sources and inspect the diff. No full unit suite or predecessor runtime replay for this isolated harness change. Tests must pass before native execution; a generated synthetic report is never qualification evidence.
- [x] **Step 5: Execute one native cell on fresh MinIO locations.** Inspect disk and retained runtime availability first; use an existing verified 3.5.6/3.3.2 package and Python environment with the actual S helper import dependencies. Resolve absolute paths into `M2_RUNTIME`, `M2_INPUTS` and `M2_OUT`; they are local shell variables, not system environment overrides. Start only required local fixture services if stopped under standing user authorization. Command:

  ```bash
  python3 delta/acceptance/spark_hub_write_native.py \
    --runtime "$M2_RUNTIME" \
    --spark-hub-root /Users/jacobo.calderon/Code/Arkham/spark_hub/.worktrees/gravitino-catalog-foundation \
    --inputs "$M2_INPUTS" --out "$M2_OUT"
  ```

  Expected: original execution and independent checker complete for every case, with explicit rejected candidates/gaps where observed. No automatic rerun of the same write prefix. If a runner defect requires another attempt, preserve the failed attempt and use a new prefix/output. Verify retained results separately with `verify_result(Path(...))`. Do not assert candidate viability from runner exit alone.

- [x] **Step 6: Record the decision and review it.** Write the findings document with actual package/source identities, selected/rejected create candidates, identity/schema/history results, default hook outcomes and remaining unavailable native seams. State `governedSupport=false`. Propose the exact next source/options task, or a smaller create/hook seam experiment if needed; leave production retry/publication/provenance designs explicit. Fresh independent review checks original evidence and the spec. No second whole matrix merely because the findings prose changes.
- [x] **Step 7: Commit the reviewed test-only slice.** Stage only the six files in the file map, run scope/change checks, and commit `test(delta): characterize Spark Hub native write lifecycle`. Push only if this reviewed slice is being closed as a milestone under standing authorization; never call it M2 complete. Do not edit Spark Hub source or create a PR.

## Self-review and handoff

This plan covers only the design's first-task acceptance. Real external-source admission, schema CREATE propagation, retained lifecycle authority, safe application retries, governed overwrite/default hooks and shared routing are still steps 2–7. Source evidence cannot close them. The five review-focus risks map to explicit cases/checker tests above; runtime identity, immutable evidence and fresh-prefix checks prevent synthetic success from being reported as native qualification.

Preserve the user's subagent-driven method: one implementer for this task, fresh review before the next task. The user approved this first-task design/plan on 2026-10-30. Do not reopen already-approved global architecture choices; follow the measured findings when preparing the next bounded task.
