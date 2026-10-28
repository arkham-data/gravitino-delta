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

# Native Append AQE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user already chose subagent-driven execution and authorized continuation.

**Goal:** Run the existing named governed Delta append frontends with stock Spark AQE, preserving native target-cache support and comparing real Iceberg append behavior.

**Architecture:** Keep the Delta coordinator's registered source/target semantics and native transaction/helper/commit pipeline. Capture accepted append execution at driver boundaries and use trusted stock adaptive execution, following the established read/MERGE model. Prove actual Iceberg writes first; preserve native maintenance rather than declaring cold-only success.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, Ranger server/client 2.8.0, MinIO; Iceberg 1.11.0 and Kyuubi 1.10.2 reference.

**Spec:** `docs/superpowers/specs/2026-10-28-delta-append-aqe-design.md`.

**Baseline:** `6a2bc1a44fbac8b001cceec3e85ff2c9ee03ab55`. Evidence **E** is `.superpowers/sdd/2026-10-28-delta-append-aqe/`.

## Global Constraints

- No Gravitino core/API/server/client changes, engine fork, second metastore, raw-path fallback, replacement transaction implementation, new privilege mapping or automatic setting changes.
- Preserve Gravitino-owned PUBLISHED registration, canonical four-part Ranger resources, original source SELECT/filter/mask semantics and target UPDATE. Target SELECT policies are read semantics, not insertion constraints.
- Delta deliverable is existing matching-schema insertInto and existing-table saveAsTable with their existing writer-option, source-grammar and transaction restrictions. Reference characterizes those plus writeTo.append and SQL INSERT SELECT. Do not silently claim frontend expansion.
- Preserve existing entry/precommit checks, duplicates, native actions, failure/known-commit recovery. No new per-stage or atomic-revocation guarantee; captured callbacks cannot impersonate a principal or borrow ordinary-read authority.
- Existing native target/direct attribute-Project caches must work under AQE, including both insertion orders, duplicates and late readback. Cold-only success cannot close the milestone. Unsupported application-cached source and foreign cache histories remain rejected.
- Preserve previously qualified CDF-enabled/disabled target continued-use, partitioned targets and both default/no-default helper branches. CDF-property AQE and broader table features remain separate.
- Platform excludes concurrent writer jobs per dataset. Catalog-demo, streaming/Connect/separate workers, PR/deployment and general writer/source expansion remain outside this plan.
- Serialize heavy work. Keep original failures/exits/evidence; never rewrite old packages or downgrade source/runtime validation. No unchanged broad regression replay without a concrete affected-input reason.
- Use owned isolated local fixture services/policies only; keep credentials private. Preserve worktrees/evidence. Push only at independently reviewed milestone closure under standing authorization.

## Review Focus

- A write callback may expose metadata/readback AQE rather than payload: Task1 pins callback-to-write attribution and a negative mutant.
- Full-schema writes still enter native default projection: Task2 covers defaults=true and false without changing caller settings.
- Empty or duplicate writes can bypass expected work: Task2 distinguishes committed empty semantics, authorized duplicate no-source work and denied duplicate.
- Warm target metadata may fail after commit: Task3 requires supported native cache profiles and preflight rejection for known unsupported dependencies.
- A postcommit exception can be mistaken for no write: Tasks2/4 inspect native commit identity and reject automatic replay of uncertain work.

## File and interface map

Let **M**=`spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`, **T** the corresponding `src/test/scala/.../ranger`, and **A**=`delta/acceptance`.

- New `A/iceberg_append_aqe.py`, `A/iceberg_append_aqe_app.py`, `A/append_aqe_checks.py`, `A/test_append_aqe_checks.py`: reference orchestration, native writer app and independent result validation. Reuse existing runtime, service/download and audit helpers without altering the closed read oracle.
- `M/DeltaRangerAppendCoordinator.scala`, `DeltaRangerAppendHelper.scala`, `DeltaRangerExecutionContext.scala`, `DeltaRangerNativeScans.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerPhysicalProvenance.scala`: append acceptance and stock adaptive payload. Add a small append-specific carrier only if the measured lifecycle requires it.
- `M/DeltaRangerAppendCommandBinding.scala`, `DeltaRangerAppendCommit.scala`, `DeltaRangerRawMaintenance.scala`, `DeltaRangerCdfCacheMaintenance.scala` and implicated native-context helpers: retain transaction authority and bounded native metadata/cache behavior; edit only measured required seams.
- New `T/TestDeltaRangerTrustedAqeAppend.scala`, `T/TestDeltaRangerTrustedAqeAppendMaintenance.scala`; extend existing append/CDF/publication tests where their original fixtures are relevant. New logic receives focused tests.
- New `A/append_aqe_live.py`; extend `A/cdf_read_live_package.py` with a bounded append-AQE profile if that is the smallest compatible packaging route; reuse existing `T/DeltaRangerLiveAppend.scala` or add an explicitly named thin append-AQE app for observations.
- `docs/delta-native-governed-append.md`, `docs/delta-spark-parity.md`, `A/README.md` and this plan: truthful final support and evidence boundaries.

## Task 1: Measure governed Iceberg append with actual adaptive writes

**Complete:** `c17cab084` plus checker fix `36705e739`; SPEC PASS / QUALITY PASS after scoped re-review, no remaining findings. Four native/wrapper × Spark reference cells:96 primary cases (32 allowed adaptive writes,64 denials),4 supplemental denials,28 original process exits0. Existing incomplete attempt remains unqualified. Original109-test acceptance plus final8-test focused fix coverage; exact producer-equivalence and supplemental validation preserve original runtime identities. Root Spotless was interrupted while traversing evidence; scoped Python formatting/lint passed. This reference enables no Delta production behavior. Evidence: `E/task-1-report.md`, `E/task-1-fix-round-1-review.md`, `E/task-1/reference-matrix.json`, `E/append-boundary-map.md`.

**Files:** new four reference/checker files listed above; reuse `A/iceberg_read_parity.py`, `A/iceberg_read_parity_app.py`, `A/cdf_live.py` transport/runtime helpers; no Delta production changes.

**Interfaces:** `run(spark: str, route: str, inputs_path: Path, out: Path) -> None` creates immutable `result.json`; CLI `--spark`, `--catalog native|gravitino`, `--inputs`, `--out`. `verify_iceberg_append(result: dict) -> None` independently verifies observations and referenced artifacts. Output `E/task-1/reference-matrix.json` and `append-boundary-map.md` guide Task2; no runtime authorization token is exposed.

- [x] Define independent case IDs/oracles for all four APIs, both target partition forms and separate allowed/source-denied/target-denied identities. Fixture rows have duplicates, hidden rows, a mask, and two surviving target partition values. Fix exact expected row multisets in the checker before running; verification readback uses a separate identity.
- [x] Write checker behavioral RED mutations: flag-only AQE, readback-only adaptive plan, missing/failed original exit, wrong subject/resource/access, allowed-only denial evidence, wrong masked rows/duplicate count, unchanged snapshot on claimed successful nonempty write, altered referenced bytes and missing wrapper identity. Run focused unit RED, retaining actual assertion failures rather than setup errors.
- [x] Implement the native app and supervisor with fresh owned fixture scopes. Record every command/payload callback and actual execution identifiers; classify metadata/readback separately. Preserve original provider/source/runtime/policy/audit references and all process exits. Use typed exact value comparisons. Do not monkeypatch the read app/oracle.
- [x] Run focused checker tests GREEN. Execute native and Gravitino-wrapper reference cells on both Spark patches with actual Ranger2.8.0/MinIO. Characterize insertInto/saveAsTable/writeTo.append/SQL INSERT SELECT, source SELECT vs target UPDATE, source masks/filter and destination read-policy semantics. Record injected-column differences explicitly; do not adjust Delta original demands.
- [x] Require at least one successful, materialized exchange-bearing append payload per reference cell and actual coalesced readers where claimed. Use writer distribution or a qualified source join; do not claim a nonadaptive valid append is adaptive. If a frontend/policy prerequisite rejects, record it as unqualified, continue independent observations, and identify the effect on Task2 before production edits.
- [x] Write the actual command→payload→commit map and compare it to the existing Delta source/helper/maintenance gates from discovery. Specify which observations are source-level versus live; keep fixture-authored policies separate from public grant propagation. Obtain fresh SPEC/QUALITY review and commit the reference harness/evidence documentation. No Delta capability enabled by this task.

## Task 2: Native Delta append payload and commit under AQE

**Files:** adaptive payload files from map; new `T/TestDeltaRangerTrustedAqeAppend.scala`; affected existing append/helper/publication suites.

**Interfaces:** retain existing `DeltaRangerAppendCoordinator.run(input): Unit` and `inTransaction(...)`. Add `DeltaRangerExecutionContext.forAppend(source: DeltaRangerAppendCoordinator.Source, recipe: LogicalPlan): Context`, with source-owned driver validation before capture. The exact helper/default recipe validation must follow Task1's measured lifecycle; changing a planned signature requires a ledgered reason. Task3 consumes the same append operation identity, not an ordinary-read ticket.

- [ ] Add genuine RED cases `namedFrontendsUseAdaptivePayload`, `defaultAndOriginalQueryBranches`, `sourceAndTargetDeniedBeforeWrite`. Invoke actual named DataFrame writers with existing admitted source shapes under AQE, inspect native versions/rows/actions and original failure boundary on Spark3.5.4/Delta3.3.0. Cover partitioned/unpartitioned targets and masked/filtered source rows.
- [ ] Implement append-owned capture at the native helper boundary, retain driver transaction checks, and replace only append's incompatible physical-ticket gates. Keep exact original recipe/schema/source obligations and command/transaction identity. Stock internal callbacks use accepted immutable context; no driver-thread impersonation, per-stage refresh or automatic setting change.
- [ ] Qualify necessary cold native snapshot/metadata/commit continuations on the observed stock route. Audit the raw-cache preflight before enabling public execution; known unsupported cases must reject before file generation. Task3 adds required warm profiles before the milestone closes.
- [ ] Add `capturedWriteThenPrecommitRevoke`, `duplicateSkipsSourceButRechecks`, `failedHelperCannotAuthorizeNextWrite`, `changedSourceTargetContextRejects`, `emptyAndConstraintOutcomes`, and `knownCommitDoesNotReplay`. Assert observed precommit revoke prevents new committed version despite possible uncommitted files; preserve existing native commit/recovery behavior and no-source duplicate execution.
- [ ] Run new payload suite and affected command/helper/publication tests on both Spark patches/Delta3.3.0, plus the other Delta pin when a Delta-specific path changes. Run focused shared read/MERGE/CDF/append-source rejection controls; exact selector inventory and original counts are recorded. Final frozen four-pair qualification is Task4.
- [ ] Format/check affected module sources, obtain fresh SPEC/QUALITY review and commit. Record exact adaptive payload plans and remaining maintenance qualification, never describe Task2 alone as the completed append milestone.

## Task 3: Preserve native cache and checkpoint maintenance under AQE

**Files:** bounded maintenance/native-context files from map; new `T/TestDeltaRangerTrustedAqeAppendMaintenance.scala`; affected CDF continued-use/checkpoint tests.

**Interfaces:** use Task2's append operation capture and existing maintenance `prepare/complete` scope. Preserve public/native method contracts and known-commit observation. Any new maintenance context is internal, immutable and bound to the same actual append/target registration; it is not a new raw query entry point.

- [ ] Add RED cases `targetFirstAndProjectFirstRefresh`, `duplicateAndLateNativeReadback`, `cdfEnabledDisabledTargetMaintenance`, `partitionCheckpointReopen`, `unsupportedCacheRejectedBeforeMutation`. Exercise actual target/direct Project warm cache keys in both orders, retained native session readback after owner expiry, and unchanged unrelated entries. Observe real builder replacement and literal rows; no success based solely on append returning.
- [ ] Implement the necessary stock adaptive native-maintenance path without weakening raw-path admission, application-cache-source exclusion or driver commit ownership. Internal cloned-session/native-cache operations receive only their bounded accepted context; no global cache clearing or AQE toggle.
- [ ] Cover checkpoint parts/pointers and reopen with JSON tail on partitioned and unpartitioned targets, existing qualified CDF protocol profiles, defaults and duplicates. Known unsupported dependencies reject before mutation; required supported warm profiles cannot be replaced with blanket rejection. Retain possible-committed-work handling after a genuine postcommit failure.
- [ ] Run maintenance and affected existing cache/checkpoint/CDF continued-use tests on both Spark patches, focused AQE=false positives and shared trusted-AQE regressions. Record exact coverage, no setup/skip-as-pass. Format/check, obtain fresh SPEC/QUALITY review and commit; all required native profiles must be ready for final public qualification.

## Task 4: Frozen public-grant matrix, documentation and closure

**Files:** live runner/thin app/package profile and final support docs from map; extend `A/append_aqe_checks.py` and tests with actual Delta result verification.

**Interfaces:** `verify_delta_append(result: dict) -> None`; `append_aqe_live.py --runtime <manifest> --inputs <private-inputs> --out <fresh-dir>`; `E/final-evidence-index.json` joins frozen source, installed and real live results, original command exits and immutable runtime artifacts.

- [ ] Add checker RED mutants for missing source SELECT/target UPDATE decisions, false generic-error denials, missing public grant/download/application linkage, false duplicate success with second publication, wrong checkpoint/cache results, postcommit failure mislabeled rollback and metadata-only AQE. Run GREEN before the final matrix.
- [ ] Reuse the accepted public append fixture to propagate real Gravitino MODIFY/SELECT grants and revokes to Ranger2.8.0. New cases run actual native PySpark append without changing session AQE. Keep admin/downloader/metadata/application identities separate and retain canonical registration before/after persistence/restart where the existing fixture requires it.
- [ ] Freeze candidate source/tests/harness and inventory disk/services. Run new installed append/maintenance suites and affected regressions on all four Spark/Delta pairs with original nonzero method inventories and no failures/errors/skips. Reuse unaffected earlier checks only with exact executed-input proof.
- [ ] Build fresh immutable matching packages and execute four live cells serially. Each covers both named frontends, partitioned/unpartitioned committed rows, source masks/filter, denied source and target, authorized/denied duplicates, public revoke after file generation before commit, both warm-cache orders and late readback, required CDF continued-use and checkpoint outcomes. Observe adaptive write payload separately from metadata/readback; record actual Ranger server/client2.8.0 and loaded providers.
- [ ] Reuse Task1 comparator results only if its executed runtime/harness/source inputs remain equal. Re-run affected reference cells if those inputs changed; never relabel previous exits. Index all final references, hashes, source/class/package joins and independent check results. Retain all original failures.
- [ ] Update support docs with exact native APIs, source/schema/options limits, existing extra Delta revalidation guarantees and measured Iceberg differences. Obtain fresh task review then separate whole-branch review. Resolve findings with affected verification, close the milestone, push the intended branch under standing authorization and preserve the worktree/evidence. Catalog-demo stays deferred.

## Commands and evidence discipline

Use fresh `E/task-N/<spark>-<delta>/attempt-N/` paths; no source edits during final
frozen runs. Existing private input locator is in `E/iceberg-discovery.md`; never
print its contents. Reverify owned services before actual mutation.

```bash
python3 -m unittest discover -s delta/acceptance -p 'test_append_aqe_checks.py' -v
python3 delta/acceptance/iceberg_append_aqe.py --spark <pin> --catalog <native|gravitino> --inputs <private-file> --out <fresh-dir>
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_merge_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerTrustedAqeAppend' \
  -PdeltaSparkVersion=<pin> -PdeltaVersion=<pin> -PcdfLiveXml=<fresh-xml-dir> \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

Use matching pinned native Python runtimes for live PySpark. Check original
subprocess/supervisor exits, selected XML methods, source/runtime/provider records.
Run module-scoped spotlessApply/spotlessCheck and relevant Javadoc checks for code;
`git diff --check` for all changes. No full unrelated project suite for docs-only
updates or unchanged inputs. Shared builds/archives require truthful current Git
metadata and exact source equality, never manifest patching.

## Self-review and execution authorization

Spec coverage: reference and observation limits→Task1; source/target and accepted
payload/commit→Task2; required warm/native/CDF/checkpoint profiles→Task3;
public grants, all pins, provenance and final closure→Task4. Review Focus cases
are assigned to those tasks. Signatures retain existing coordinator interfaces;
only the append context and result-checker entry points are newly named.

The user's “Ok, proceed”, standing subagent-driven execution preference, and explicit
native-cache preservation answer authorize this work. This plan records that
scope rather than requiring repeat permission for ordinary implementation steps.
No product code or live append was executed while preparing the plan. The design
builder check found one scope question, now answered by the user; no material
question remains. Scope changes beyond this contract must be identified explicitly.
