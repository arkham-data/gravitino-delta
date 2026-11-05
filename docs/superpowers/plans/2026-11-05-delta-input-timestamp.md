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

# Governed Input Timestamp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let native named Delta reads and APPEND retain Spark Hub's current_timestamp loaded-at projection with complete Ranger governance.

**Architecture:** Admit the exact input projection separately from policy expressions. Freeze stock query time at pending-intent normalization and reuse the existing receipt and native append owner for every later phase.

**Tech Stack:** Java17, Scala2.12.21, Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, Ranger2.8.0, Gravitino, MinIO, JUnit5, classic PySpark.

**Spec:** [Input timestamp design](../specs/2026-11-05-delta-input-timestamp-design.md).

## Global Constraints

- No Gravitino core/API/server/storage-model changes, engine fork, manual transaction, raw-path destination, Glue fallback or automatic write replay.
- Only exact stock CurrentTimestamp directly aliased in input Project; policy/CDF/MERGE grammar stays unchanged. Original source column demand and all current registration, subject, settings, policy, native and physical proof remain binding.
- Capture once per issued completed query with stock Spark normalization; all receipt/native replay uses the same frozen literal tree. No session-global clock or invented repeated-action freshness guarantee.
- Both registered Gravitino JDBC and externally authenticated Hub JDBC remain planned; neither is admitted by this increment. No arbitrary DataFrame/LogicalRDD external authority.
- Qualify all four pinned pairs with actual Ranger server/client2.8.0 and MinIO. No full Hub/M2 claim or required-setting disabling workaround.
- Reuse the existing isolated spark-hub-adoption-plan worktree. Preserve prior evidence. One implementation worker at a time; no worker subagents or parallel build/test/index/service mutations. Controller owns commits, reviews and milestone push.

## Review Focus

1. Multiple timestamps or replay resample query time — Task1 proves one frozen value and stable receipt replay.
2. Constant folding hides denied original source demands — Task1 checks hidden predicate and columnless demand before normalization.
3. A structural expression case broadens policy/UDF authority — Task1 tests exact foreign/nested expressions and unchanged shared grammar.
4. Subject/settings/publication or policy changes after freezing — Task1 terminal negatives retain precommit denial; Task2 live revocation.
5. PySpark or another pinned engine reconstructs the native source differently — Task2 exercises real gateway/runtime provenance and all four pairs.

## File map and execution conventions

S=`spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`.
T is the corresponding `src/test/scala` package. A=`delta/acceptance/`.
Use Java17 at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`;
`JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'`.
Gradle flags: `-PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs --max-workers=1 --console=plain -Dorg.gradle.jvmargs=-Xmx2g`.
Use `:spark-connector:spark-delta-ranger-3.5:test` (settings.gradle.kts:119).
Retain commands/exits/XML and RED evidence under this plan's ignored workspace.
Run scoped SpotlessApply/Check; new public/protected Java needs Javadoc.

### Task 1: Native query-time projection through governed APPEND

**Files:** Add S/DeltaRangerInputProjection.scala and T/TestDeltaRangerInputTimestamp.scala. Modify S/DeltaRangerOrdinaryAdmission.scala, S/DeltaRangerOrdinaryRead.scala, S/DeltaRangerDecisionReceipt.scala, S/DeltaRangerAppendV1Input.scala and, if needed, S/DeltaRangerPlanAdmission.scala with a closed-default input-specific query entry. Extend existing installed append test fixture only with required source/target schema and observation callbacks; do not duplicate its native coordinator or authority setup.

**Interfaces:** Package-private `DeltaRangerInputProjection.expression(expr: Expression): Unit` admits the exact direct timestamp alias, delegating every other expression to existing grammar. `DeltaRangerInputProjection.freeze(plan: LogicalPlan): LogicalPlan` validates the selected projection and applies stock ComputeCurrentTime once, preserving output identity. If a separate query method is needed, use `DeltaRangerPlanAdmission.inputQuery(plan: LogicalPlan): Unit`; keep `query` and `expression` unchanged for existing consumers. OrdinaryRead.normalize keeps its signature and owns the authored-to-frozen transition; AppendV1Input keeps original demand extraction before it. The existing decision receipt binds the resulting literal tree; no new public API or independently renewable owner.

`ReadRule.extract` invokes `OrdinaryAdmission.query` on the authored tree before freezing. The input-specific Project validator must therefore be shared by ordinary pending and structural complete admission; changing pendingQuery alone is insufficient. It must not broaden other operators' expressions or shared policy admission.

- [ ] **Step1: Add and run RED installed tests** for named insertInto and existing-table saveAsTable with two direct timestamp aliases. Assert exact masked/filtered source rows, both timestamp outputs equal and non-null for all written rows, original schema/ExprIds retained and one native commit. Include one existing qualified AQE/exchange + partitioned target case. Record the initial admission failure, not an unrelated fixture failure.
- [ ] **Step2: Implement the input-only admission and normalization.** Structurally bind exact CurrentTimestamp without evaluating it. Validate original pending intent, clone/clear tags, apply stock rule once before authorizeComplete, and retain/check the relationship to its frozen executable receipt. Confirm stock normalization lifecycle from installed source/bytecode and tests; preserve current helpers and native owner.
- [ ] **Step3: Add adversarial and lifecycle tests** for unchanged policy/CDF/MERGE rejection, nested/time-predicate/custom/UDF expressions, columnless source, hidden denied predicate column, target UPDATE denial, subject/settings/publication change and source revocation at terminal input. Test timestamp-literal substitution before/after receipt, stable repeated replay, independent fresh query capture without timing sleeps, and original output/metadata preservation. Reuse existing failure seams; no production test-only clock/authority hook.
- [ ] **Step4: Verify the focused suite and current controls.** Run new timestamp suite, existing receipt/append source-shape/V1-input suites and `TestDeltaRangerNativeGovernedAppend.installedNamedAppendCommitsMaskedFilteredSource` plus `sourceRevocationAfterHelperPreservesUncommittedFiles`. Run scoped formatting. Record exact counts, source diff and limitations; do not claim known historical installed-boundary failures passed. Fresh task review; controller commits `feat(delta): preserve query timestamps in governed append`.

### Task 2: Pinned runtime and real PySpark qualification

**Files:** Extend A/append_aqe_pyspark.py and matching existing driver/checker only where they own reusable input/target fixtures. Prefer a small A/input_timestamp_pyspark.py driver if the existing fixed schema would otherwise need new unrelated modes; reuse downloaded-query/bootstrap/evidence helpers, no copied service framework. Add A/test_input_timestamp_checks.py for any new acceptance checker logic. Update companion/acceptance README and create `docs/superpowers/specs/2026-11-05-delta-input-timestamp-findings.md`.

**Interfaces:** Classic PySpark uses native `spark.table(name).withColumn(name, F.current_timestamp())` and native `.write.mode('append').insertInto(name)` / `.format('delta').mode('append').saveAsTable(name)`; no Python facade method. Task1 supplies input admission and freeze through existing native session extensions. Use fresh names/packaged current sources and original Ranger fixture polling; acceptance records contain versions, rows/timestamps, target versions, outcome and redacted runtime/source hashes.

- [ ] **Step1: Run focused Task1 coverage on the other three pinned pairs**, preserving per-pair XML/artifact origins before each rebuild. Add checker tests rejecting unequal/null timestamps, unexpected unfiltered/unmasked rows, wrong native version delta, swallowed denial or stale source/runtime evidence. Native engine characterization is part of these tests, not a standalone research phase.
- [ ] **Step2: Qualify lead Spark3.5.6/Delta3.3.2 PySpark on actual services.** Use actual Ranger2.8.0, Gravitino registration and MinIO. Exercise both existing-target writers with repeated timestamps, source mask/filter and the existing qualified AQE profile. Prove target denial and source revocation with exact outcome/native-log evidence. Preserve failed attempts; do not replay uncertain/committed writes.
- [ ] **Step3: Qualify remaining pairs with current source-bound packages.** Each must show named timestamp append and denial using the real PySpark gateway/module/runtime. Reuse byte-identical unchanged production artifacts only when manifests prove it. Do not print private fixture environments or credentials.
- [ ] **Step4: Review and close.** Record exact supported query-time lifecycle and settings, both JDBC routes still pending and broader input/transform/overwrite limits. Run checker/formatting checks, task review and final integrated review. Mark completed steps, commit findings and push the milestone branch only after all required checks pass. Keep local worktree/evidence for the next registered JDBC plan.
