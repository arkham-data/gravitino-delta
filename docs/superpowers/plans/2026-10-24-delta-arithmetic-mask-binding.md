# Arithmetic-mask SELECT binding repair

> **For agentic workers:** Use superpowers:subagent-driven-development. Preserve the completed CDF-to-MERGE milestone and its evidence.

**Goal:** Restore ordinary named SELECT with an already-admitted, explicitly typed arithmetic Ranger mask without relaxing plan ownership or current authorization.

**Architecture:** Reproduce and trace the exact analyzer mutation between decision-receipt issuance and consumption before choosing a correction. Repair that boundary using existing owned expression/plan contracts; do not accept arbitrary structurally similar or modified plans.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Scala 2.12.21, JDK 17.

**Spec:** Existing policy semantics in [governed CDF-to-MERGE](../../delta-cdf-merge.md), [its approved design](../specs/2026-10-22-delta-cdf-merge-design.md), and the exact known defect in [the parity tracker](../../delta-spark-parity.md). This is restoration of existing scalar-mask behavior, not a new API/profile design.

**Baseline:** `8643c1d20982e5840bbcee71df5a5774c149d375`; existing `codex/delta-spark-feasibility` worktree. User authorized continued implementation and subagent execution. Keep this follow-up local after review; the previous milestone is already pushed.

## Global Constraints

- No Gravitino core/API/server/client changes, second metastore, or Spark/Delta fork.
- Existing Ranger masks, row filters and original column demands remain authoritative.
- Preserve exact subject/session/configuration/registration/policy bindings and rejection of stale, copied, serialized or modified authority.
- The mask `id + CAST(100 AS BIGINT)` on a BIGINT column must return the masked value once. Do not add implicit coercion to make `id + 100` pass the strict resolver.
- No new source grammar, builder, AQE, partitioned-target or deployment support.
- Preserve prior evidence/worktrees; do not rerun closed live matrices merely as setup.

## Review Focus

- Identify the precise before/after analyzer expression change; no blanket exemption for expression equality or Catalyst plans.
- Preserve original SELECT demands even when projection/optimization hides a column.
- A forged replacement mask, literal, attribute, qualifier or relation must not acquire original authorization.
- Current policy/subject/settings/registration changes still reject execution; masks cannot be dropped or applied twice.
- Verify the affected native analysis/optimization behavior across all four pins, with original runtime origin evidence.

## Task 1: Diagnose, repair and qualify the existing binding defect

**Files:** Investigate `DeltaRangerReadRule.scala`, `DeltaRangerDecisionReceipt.scala`, and `DeltaRangerMaskRules.scala` under `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger`. Use their existing tests under the corresponding `src/test/scala` package; add `TestDeltaRangerArithmeticMaskBinding.scala` if a dedicated regression fixture is clearer. Update only the relevant known-bug entry in `docs/delta-spark-parity.md` and exact qualification notes if evidence supports closure.

**Interfaces:** Existing `DeltaRangerDecisionReceipt.consume(candidate, session, optimized)` and mask resolution/receipt issuance contracts; no new public entry point. The implementation location is determined by traced mutation, not by the exception site alone.

- [x] Reproduce ordinary projected SELECT with the explicit BIGINT arithmetic mask using installed native Spark/Delta and the existing Ranger policy fixture. Retain original RED output and exact before/after binding values or analyzer rule evidence. The historical Task 5 diagnostic is a starting point, not a replacement for current reproduction.
- [x] Identify the first mutation and distinguish legitimate engine completion from an unauthorized plan change. Record one evidence-backed hypothesis before editing production code; compare the accepted CDF-to-MERGE mask route and ordinary SELECT route.
- [x] Implement the smallest correction at that boundary. Add behavioral tests for exact masked results, alias/filter use where already admitted, a corresponding negative modified-mask/attribute/literal case, and unchanged authorization/receipt checks. Avoid production diagnostics or test-only hooks when existing fixtures suffice.
- [x] Run the new regression and affected receipt/mask/physical-binding tests on the first pair, and the new regression plus relevant ownership/denial and installed MERGE smoke on all four pairs. Choose existing selectors from actual impacted code and record them before running. Keep original commands/exits/XML/runtime origins; no blanket 85-case/live-matrix replay. Run module formatting/compile checks.
- [x] Obtain independent scoped and integrated review of this small follow-up, correct concrete findings with targeted checks, and update the parity entry only to the exact proven scope. Preserve historical live results as historical artifacts; do not imply those old packages contain this fix. Commit reviewed files locally; no PR or automatic push.

## Execution notes

Use JDK17 `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`. Existing `delta/acceptance/cdf_merge_live.gradle` provides the pinned test setup. Gradle uses `-PdeltaSparkVersion=<pin> -PdeltaVersion=<pin> -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon -Dorg.gradle.jvmargs=-Xmx1536m --console=plain`.

Prior exact diagnostic: `.superpowers/sdd/2026-10-22-delta-cdf-merge/task-5-typed-mask-failed.xml`; original failure is `Delta decision receipt rejected: logical binding changed` at `consume` during analyzer execution. The succeeding typed MERGE probe lives in `TestDeltaRangerLiveCdfMerge`. Read only those needed prior artifacts; do not modify their ledger or manifests.

This plan has one bounded implementation task. Review is independent; the final review scope is this follow-up diff against its baseline, not the already closed integration milestone. Keep a new evidence ledger. If the evidence requires a new authorization architecture rather than a bounded repair, report the concrete seam before expanding scope.
