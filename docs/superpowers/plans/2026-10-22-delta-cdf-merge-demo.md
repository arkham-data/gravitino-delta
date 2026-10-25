# Governed CDF-to-MERGE catalog-demo Deployment Plan

> **For agentic workers:** Use superpowers:subagent-driven-development when this deferred plan is activated. Do not execute it during the current implementation plan.

**Goal:** Demonstrate the qualified named DataFrame CDF-to-MERGE API from PySpark on a separate Spark executor in catalog-demo, using named SQL as the behavioral reference.

**Architecture:** Consume immutable artifacts from the completed SQL, partitioned MERGE and DataFrame API milestones in an isolated Docker Compose overlay. Preserve the existing demo stack and policies.

**Tech Stack:** Spark 3.5.4, Delta 3.3.0, JDK 17, Scala 2.12.21, Ranger 2.8.0, MinIO.

**Spec:** [CDF-to-MERGE design](../specs/2026-10-22-delta-cdf-merge-design.md).

**Status:** On 2026-10-25 the user made the DataFrame API a critical prerequisite for this integration. This plan remains deferred until that API is implemented and qualified. It preserves former Task 6; artifact paths and the concrete overlay must be refreshed from the completed prerequisite milestones before activation. This priority update does not initiate catalog-demo edits or deployment.

## Prerequisites and constraints

- Complete and review all five tasks in [the implementation plan](2026-10-22-delta-cdf-merge.md).
- Complete the [partitioned MERGE milestone](2026-10-24-delta-partitioned-merge.md) and the [DataFrame API qualification](2026-10-22-delta-cdf-merge-builder.md). The DataFrame API is a required entry and acceptance gate, not an optional follow-up.
- This integration has its own plan and is not a task in the DataFrame API milestone. Before activation, assess the intended pipelines' need for broader Spark execution (AQE, joins, aggregations, windows, caching and broader expressions). Record which capabilities are required and qualify those through separately scoped work before integration; do not infer readiness from API qualification alone.
- Use actual Ranger 2.8.0; the current Compose 2.7.0 and older Dockerfile 2.5.0 are not acceptance evidence.
- Follow `/Users/jacobo.calderon/Code/Arkham/catalog-demo/AGENTS.md`; preserve existing local edits, policies, containers and volumes. Use an isolated project and branch.
- Keep the approved source/target policy semantics and supported runtime profile unchanged.
- Do not infer multi-process support from local[2] qualification, or replay unchanged four-pair matrices for deployment-only changes.
- No product-core changes, policy migration, automatic cleanup, PR or catalog-demo remote push.

## Task 1: Package the isolated overlay and runnable example

**Files:** In catalog-demo create `docker-compose.delta-cdf-merge.yml`, `workspaces/notebooks/13_governed_cdf_merge.py` and `docs/delta-cdf-merge.md`; update README. The overlay consumes the closed milestone's exact artifacts and supplies matching driver/worker classpaths, private fixture inputs and independent storage/ports.

- [ ] Inspect the then-current demo configuration and dirty state; bind exact qualified artifacts and actual Ranger 2.8.0 installation.
- [ ] Write the PySpark DataFrame API example, source/target grants and application checkpoint handling in an isolated fixture, with named SQL as its behavioral reference. Preserve current policy semantics and existing demos.
- [ ] Verify rendered configuration and worker classpath before bringing up only the new project. Commit local changes after scoped review.

## Task 2: Prove separate-executor behavior and document deployment limits

- [ ] Run the qualified named DataFrame API through the actual PySpark frontend on Spark 3.5.4/Delta 3.3.0 with at least one separate worker process and actual Ranger 2.8.0/MinIO. Record actual driver/worker origins and source execution; no CDF payload collect/toPandas/createDataFrame bridge.
- [ ] Prove distinct destination and same-table writeback, including partitioned targets, masks/filters, hidden target preservation, denials and correct checkpoints. Compare actual native rows/actions, registration and original process exits against the qualified contract.
- [ ] Fix only demonstrated deployment issues with affected checks. Update demo documentation and Gravitino support/parity docs to the exact demonstrated scope, then obtain one integrated review.
- [ ] Retain original evidence and worktrees. Report local commits and deployment outcome; seek separate authorization before pushing catalog-demo.

The companion DataFrame API must be qualified before this plan starts. Its actual
use through PySpark on a separate executor is required to close the demo; named
SQL coverage alone cannot close it.
