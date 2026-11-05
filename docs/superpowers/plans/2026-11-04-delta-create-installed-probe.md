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

# Delta Installed Schema CREATE Probe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure the current schema-only SQL refusal and the real base schema CREATE's transaction/snapshot boundary under installed native guards.

**Architecture:** Two test-only cases use unchanged production admission and a no-op observer. Typed local RESERVE transport drives the actual base stage; PUBLISH deliberately stops at the fixture boundary. One native-only differential is conditional on missing installed observation.

**Tech Stack:** Spark3.5.6, Delta3.3.2 (conditional3.3.0 native comparison), Java17, Scala2.12.21, JUnit5, local filesystem.

**Spec:** [Installed schema CREATE probe](../specs/2026-11-04-delta-create-installed-probe-design.md). Also read the [native create findings](../specs/2026-11-04-delta-create-boundary-findings.md).

## Global Constraints

- Initial execution uses Spark3.5.6/Delta3.3.2, Java17 and Scala2.12.21 on fresh isolated local filesystem targets.
- Repeat only a decision-relevant native seam on Spark3.5.6/Delta3.3.0; no four-pair matrix or unchanged predecessor reruns.
- Change tests and findings only. Preserve all production admission, tags, policy checks and native transaction behavior; no main-source/build/dependency/core/API/server/client/storage-schema edits.
- Use the real base catalog, DeltaPublicationStage and native Delta stage; no mock native stage, private reflection, engine instrumentation or replacement transaction logic.
- Consume tableProperties and settings unchanged from the existing native-create/spark-hub-options.json; no writer options, payload write, setting workaround or disabled maintenance feature.
- Never replay native mutation. Separate not committed, committed and unknown outcomes from publication and observation failures.
- Stub policy/transport and local native evidence do not prove real Ranger2.8/Gravitino/MinIO creation, recovery, revocation or ingestion support.

## Review Focus

- The observer forces lazy metadata or grants helper authority: assert identical returned plan, unchanged context/settings and no diagnostic authorization/tag mutation.
- Missing metadata becomes permission or an unsupported forwarding stub: use actual typed absence, unchanged wrapper delegation and zero SQL reservation/native effects.
- A native error after log0 is labeled uncommitted: correlate retained CREATE0/marker and distinguish deliberate PUBLISH stop from native failure.
- Wrong fixture/settings or runtime makes a trace misleading: verify resource hash/provenance, effective property/settings maps and loaded class/JAR origins.
- A blocked native seam causes replay or permissive escalation: assert one outer commit invocation, honest NOT_REACHED phases and finally-cleaned transaction/operation state.

---

### Task 1: Characterize schema CREATE under installed guards

**Files:**
- Create `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerInstalledSchemaCreateBoundary.scala`.
- Create `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/InstalledSchemaCreateBoundaryProbe.scala`.
- Create `docs/superpowers/specs/2026-11-04-delta-create-installed-probe-findings.md`.
- Read unchanged `spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json`.

**Interfaces:** No production API. Helper object `InstalledSchemaCreateBoundaryProbe` supplies `observer(trace: Trace): SparkSessionExtensions => Unit`; its Rule.apply returns the identical LogicalPlan. Test-local `Trace.observe(spark: SparkSession, phase: String, plan: Option[LogicalPlan]): Unit` records only the spec's safe fields; `Trace` is AutoCloseable and writes original evidence. Keep transport/forwarding helpers private or test-loadable as needed; their decomposition is builder discretion. Consume the existing public `GravitinoDeltaCatalogSpark35(Supplier[Catalog], Supplier[Catalog])`, initialize, stageCreate(Identifier, StructType, Transform[], Map[String,String]) and StagedTable.commitStagedChanges. Do not predict native snapshot classes or add a runtime owner signature.

- [x] **Step 1: Implement the bounded fixtures and two core characterization tests.** Add `namedSchemaCreateRetainsInstalledRefusal()` and `baseSchemaCreateRecordsInstalledNativeBoundary()`. Reuse NativeSelectFixture startup patterns and TestDeltaRangerNativeSelect.Fixture; a dedicated empty Spark fixture is allowed to avoid precreating unrelated a/b/q tables (including mapped columns). Preserve the actual companion extension with an explicit dedicated-wrapper resolver, shared fixtures unchanged and required settings intact. Use the observer-before-companion ordering in TestDeltaRangerInstalledAppendBoundaries.withCase. Use NativeBoundCatalogFixture only as wiring guidance: create a genuine typed-absence fixture and actor transport; forward loadTable/tableExists/createTable to the real Ranger wrapper, not its existing unsupported test stub. Use nullable BIGINT id, no partitions/query/builder and the spec's fixture maps. Decode RESERVE and construct an exact token/run/schema-bound generation0 RESERVED receipt at a fresh local marker location; use TestGravitinoDeltaCatalogSpark35.pending as the existing recipe. PUBLISH records then throws a distinct sentinel; never return PUBLISHED. Supply temp evidence defaults and a module-relative resource default so ordinary tests do not require private artifacts or environment variables. Honor explicit evidence/resource overrides for retained runs. Setup accepts supported Spark3.5.4/3.5.6; runtime records prove the executed pin. This characterizes existing code: do not add an artificial failing product stub or a production fix.

- [x] **Step 2: Pin instrumentation and evidence invariants inside those tests.** In A, assert identical observer return and unchanged receipt presence/context; preserve the exact SQL failure and assert zero RESERVE/native files. Counters must distinguish NOT_REACHED tableExists/create from attempted failures. In B, assert descriptor construction has no transport/native side effect, one commit invocation, exact fixture/RESERVE bindings, unchanged property/settings inputs and no fabricated publication. In both, fail separately for trace errors, retain actual class/JAR hashes and original exception stacks, and assert finally-cleared OptimisticTransaction/current companion operation. Inspect log0 outside Spark after unwind; keep any lastCommitVersionInSession change as outcome evidence. These assertions cover all five Review Focus checks without manufacturing a successful CREATE or granting an owner.

- [x] **Step 3: Execute the initial focused characterization and retain its original outcome.** Use Java17, a fresh absolute evidence directory and the existing absolute fixture path with the command below. Capture exact command/exit/output/XML, sources/classes/JAR hashes, events, journal, files and settings. Distinguish compilation/instrumentation failures from native findings. From original evidence, pin the actual first rejected plan/failure or deliberate PUBLISH stop in the test; no “any exception passes” assertion. Rerun only when a real test edit/failure requires it. Tests pass when the specified evidence and measured assertions hold, even if native execution is refused.

```sh
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
GRAVITINO_DELTA_CREATE_PROBE_EVIDENCE_DIR=<fresh absolute directory> \
DELTA_CREATE_OPTIONS_FIXTURE=<absolute existing spark-hub-options.json> \
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerInstalledSchemaCreateBoundary' \
  -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2 -PskipITs --max-workers=1
```

- [x] **Step 4: Resolve only the conditional observation gap.** If B cannot expose the relevant active transaction before its installed refusal, add/run `nativeOnlySchemaCreateDifferential()` once using the identical real base stage, scripted transport, observer and fixture maps in a separately isolated native-only session/target. Record why it was necessary; otherwise record NOT_NEEDED and do not run it. No helper admission override, reflection or manual transaction/action construction. If the measured native seam determines a future implementation decision, repeat only B or this differential using method-specific `--tests` and `-PdeltaVersion=3.3.0`; otherwise record that comparison NOT_NEEDED. Mark deeper installed phases NOT_REACHED, not supported through the differential. Do not repeat A or the old CREATE0/WRITE1 matrix.

- [x] **Step 5: Record findings, format and review the one task.** Write the public findings with actual pin/artifact identities, first SQL/native boundary, transaction/snapshot/log0 ordering, exact fixture status, native/publication outcomes, optional comparisons and unresolved public seam. Separate positive callback observation from production authorization feasibility; include no lifecycle/ingestion support claim. Run `./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply :spark-connector:spark-delta-ranger-3.5:spotlessCheck`; verify only intended test formatting changed. A fresh review checks source and retained original evidence, especially each Review Focus item. If runtime behavior requires production changes, stop at the measured boundary and report it rather than widening this task. Controller obtains a task review before committing the two test files and findings with `test(delta): characterize installed schema create boundary`; one whole-slice review follows, then proceed to the source-grounded reservation-bound design. Preserve the worktree/evidence; no push or full creation closure follows from characterization alone.

## Build and controller boundary

Use `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'` and
`-Dorg.gradle.jvmargs=-Xmx2g --console=plain` with the above commands. Runtime JAR
origins must identify the actual Ranger 2.8.0 fixture client/model artifacts;
no Ranger server is contacted. Keep exact test/source/native evidence scope
explicit, with existing controls referenced rather than rerun. The implementer
reports actual behavior and engineering blockers; it does not alter production
admission to get a successful CREATE. Controller owns status docs, commits and
independent review. This follows the reviewed local schema-grant checkpoint
`9733e09142a959424379ae72727c082d7516396c`.

Task 1 and whole-slice review are complete (SPEC PASS, QUALITY APPROVED; no Critical/Important findings). The test-only checkpoint is closed locally at `c49f282fe`, with the exact historical/current run distinction retained in the findings. Two optional test observations remain: a recorded but unasserted zero-load counter and a deprecated schema accessor warning. Production work continues in the [reservation-bound creation plan](2026-11-04-delta-governed-schema-create.md); this characterization is not governed creation support.
