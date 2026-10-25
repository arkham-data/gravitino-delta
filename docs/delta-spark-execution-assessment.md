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

# Broader Spark execution assessment

Assessment date: 2026-10-25. Product baseline: `5a2e3676c` (completed named
DataFrame CDF-to-MERGE milestone). This document records static findings and a
recommended next scope; it is not an approved implementation design or new runtime
qualification. Existing supported behavior and rejection boundaries remain intact.

Follow-on direction: the user replied Proceed to the recommended real-AQE-first
sequence. The [bounded AQE design](superpowers/specs/2026-10-25-delta-governed-aqe-design.md)
was approved on 2026-10-25 with catalog-demo explicitly deferred. The user will
verify which operations remain missing before follow-on scope is selected. The
assessment-time recommendations and open choices below are historical findings,
not authorization to begin integration or additional ordinary-read work.

## Subsequent AQE feasibility result — 2026-10-25

The approved [AQE plan](superpowers/plans/2026-10-25-delta-governed-aqe.md)
reached a concrete ownership limitation in Task 1. Installed probes on both Spark
pins show that the examined extension hooks do not expose a newly constructed
stage before materialization method entry. A test-only internal child guard also
failed exact invoking-stage authentication: copied stages can imitate its
monitor/caller checks and cached copies can reuse map-stage statistics without
another guard check. No foreign payload-row disclosure was demonstrated.

This rejects the examined candidate, not every possible AQE design. Independent
review approved the diagnostic evidence and confirmed that Task 1 remains
incomplete. Existing product AQE rejection is intact; CDF/MERGE extension and the
four-pair qualification have not begun. Revisiting ownership is necessary before
continuing this plan. Catalog-demo remains deferred pending user direction.

## Subsequent Iceberg baseline — 2026-10-25

The [live Iceberg/Ranger AQE baseline](iceberg-spark-ranger-aqe-baseline.md)
passed native Iceberg and the Gravitino Iceberg wrapper on both Spark 3.5.4 and
3.5.6 with actual Ranger 2.8.0 and MinIO. The runs verified final adaptive payload
stages/coalescing, exact masked/filtered join results, referenced-column denial
and fresh-query revocation after policy refresh. Independent review approved the
bounded evidence with no blocking findings; original results and version/artifact
records are retained.

The tested Iceberg/Kyuubi path uses normal query-planning authorization and trusted
Spark execution, without the Delta proposal's exact-stage permits. This supplies
the missing architectural comparator; it does not remove existing Delta guards or
qualify Delta CDF/MERGE target-policy semantics. The wrapper test uses native-seeded
tables, stock HadoopCatalog selected through the custom-backend option, and disabled
Gravitino metadata authorization. It is not grants-to-Ranger propagation proof.

The user agreed to reconsider the physical-stage contract after this baseline.
The [approved revised design](superpowers/specs/2026-10-25-delta-trusted-spark-aqe-design.md)
retains Delta semantic integration and defines authorization at action/command
boundaries with trusted stock physical execution. The detailed timing and profile
are approved; the [replacement implementation plan](superpowers/plans/2026-10-25-delta-trusted-spark-aqe.md)
is ready for review. Existing Delta support and rejection remain unchanged until
implementation and qualification succeed.

## Recommendation

Qualify **real governed AQE in bounded stages**, then the ordinary read operations
needed by the intended demo consumer, before activating catalog-demo integration.
Keep the existing named CDF MERGE source contract while doing that work. Expanding
ordinary reads must not implicitly admit joined, aggregated, windowed or cached
CDF MERGE sources.

The first design should specify an owned adaptive stage lifecycle and its initial
supported transitions. Stock shuffle/broadcast stage materialization and complete
coalesced reducer reads are a candidate starting point, not a proven implementation.
Do not describe a session that accepts `AQE=true` while retaining nonadaptive payload
execution as adaptive execution support.

The user's first production CDF transformation requirements and preferred next
scope remain open. The demo provides concrete ordinary-read examples, but no
incremental CDF consumer from which to infer those production requirements.

## What the demo actually uses

The inspected demo root is `/Users/jacobo.calderon/Code/Arkham/catalog-demo`.
Notebook references below use one-based positions in the complete `cells` array;
only source cells were inspected, not saved outputs.

| Evidence | Observation | Consequence |
| --- | --- | --- |
| `workspaces/spark-engine/src/spark_engine/session.py:223` | Explicit `spark.sql.adaptive.enabled=true` | Default demo sessions conflict with current governed read/CDF/MERGE AQE gates |
| `workspaces/notebooks/02_query_as_admin.ipynb`, cell 8 | Conditioned INNER joins of employees, salaries and departments, followed by ordering | Ordinary join and sort behavior needs end-to-end qualification, including temp-view handling |
| `03_query_as_manager.ipynb`, cell 8 | DISTINCT and ORDER BY | These are concrete read requirements; not evidence of general grouped analytics |
| Spark notebooks 02–04 and workers | `show`, `count`, `toPandas`, table reads and denials | Assess their actual resulting plans; collection for display does not authorize collecting/rebuilding a MERGE source |
| `workspaces/setup-scripts/src/setup_scripts/04_load_data.py:183-201` | Three-table HR derivation with CASE/date expressions and full overwrite | Privileged bootstrap batch work, not a demonstrated incremental CDF pipeline |
| `workspaces/setup-scripts/src/setup_scripts/05_setup_ranger_policies.py:198-199` | Manager salary filter includes an IN subquery over employees | Policy dependency resolution is a separate integration obligation; do not silently rewrite the policy |
| `docker-compose.yml:87` | Ranger image 2.7.0 | Later integration must use and verify actual Ranger 2.8.0 |
| Spark notebooks 11–12 and `_spark_worker.py:105-148` | Multiple processes, and shared-session per-user threads | Deployment and identity-isolation qualification remain separate from local AQE proof |

No non-test Spark windows, cache/persist, repartition, custom UDF, streaming,
CDF read or Delta MERGE usage was found in the inspected workspace sources and
notebook cells. This does not exclude future production needs. Trino SQL and
pandas `.merge()` examples do not establish Spark or Delta MERGE requirements.
Notebook 10 runs its wrappers sequentially despite its concurrent title.

The curated bootstrap summary has no employee ID (`04_load_data.py:248-252`).
Do not invent a name-based MERGE key or infer CDC update/delete semantics from it.
Existing Ranger masks and native matching/ambiguity semantics remain authoritative;
this assessment adds no unmasked-key or deduplication requirement.

## Current connector boundaries

All Scala references below resolve beneath
`spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`.

| Boundary | Current implementation |
| --- | --- |
| Ordinary read grammar | `DeltaRangerPlanAdmission.scala:66-85` admits Project/Filter/Alias/relations and conditioned INNER Join with finite scalar expressions. Sort, Limit, Distinct, Aggregate/Count and Window are absent from this fresh-user whitelist |
| Read reuse | `DeltaRangerReadRule.scala:122-129` rejects derived/reused plans; a logically admitted join does not prove the demo's reused DataFrame/temp-view route works |
| Ordinary read AQE | `DeltaRangerReadRule.scala:268-269` and `DeltaRangerPhysicalProvenance.scala:101-102` independently reject it |
| CDF and MERGE AQE | `DeltaRangerCdfRead.scala:246` and `DeltaRangerMergeReceipt.scala:45-53` reject it separately |
| Owned lowering | `DeltaRangerPhysicalPlanning.scala:164-221` deliberately excludes InsertAdaptiveSparkPlan from its qualified physical preparation |
| Adaptive hooks | `DeltaRangerPhysicalPlanning.scala:137-148` rejects foreign adaptive rules and custom cost evaluators; any new companion hook needs narrow ownership checks |
| Existing native metadata AQE | `DeltaRangerPhysicalProvenance.scala:137-160,210-243,338-370` has exact native stage/exchange and complete coalesced-partition evidence. This does not grant user/CDF/MERGE authority |
| Lifecycle constraints | CDF/source/native-writer implementations freeze producer, physical shape, RDD/dependency identities and driver-thread authority. Adaptive transitions must preserve current policy and registration checks, attempt closure and commit boundaries |

Removing the AQE session guards is insufficient: it may leave owned execution
nonadaptive, and cannot prove authority for newly executable stages. Checking only
an adaptive wrapper's initial plan is also insufficient for its later plans.
Spark describes AQE as runtime plan optimization, including partition coalescing
and join-strategy changes; a configuration flag alone is not that evidence.
See [Spark 3.5.6 AQE documentation](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html#adaptive-query-execution).

Pinned Spark 3.5.4/3.5.6 InsertAdaptiveSparkPlan bytecode was inspected locally.
Its insertion conditions include exchanges, required distributions, subqueries and
forced application. Small display queries can fail to exercise meaningful adaptive
behavior; future fixtures need explicit plan/stage observations.

## Scope choices presented

1. **Recommended: real AQE, then the observed ordinary-read operations.** This
   preserves the demo's intended AQE setting while introducing explicit stage
   ownership and qualification. It is substantive connector lifecycle work,
   followed by finite logical-operator admission where needed. Not every Spark
   adaptive optimization or query shape becomes supported at once.
2. **Explicit AQE-off integration profile first.** This reaches a bounded demo
   sooner using the already qualified contract. It requires an explicit profile
   decision and does not migrate all existing notebook operations or complete AQE.
3. **Broader CDF transformations first.** Appropriate if the first ELT consumer
   requires extra source composition before MERGE. Concrete transformations and
   semantics must be supplied; ordinary demo joins do not establish that need.

AQE-enabled but deliberately nonadaptive session compatibility is another technical
possibility, but is not recommended as a substitute for real AQE or as a solution
to exchange-bearing demo workloads. No option has been selected by this assessment.

## Evidence the next design must require

- Fresh AQE-enabled sessions, sources and handles, and actual finalized adaptive
  payload plans/stages. A stale pre-change handle can reject for changed settings
  without isolating AQE support. Preserve the existing settings-change negatives.
- Separate proof for ordinary reads, bounded CDF, MERGE materialization, target
  selection, rewrite/output and publication. Ordinary read support must not imply
  broader MERGE source grammar.
- Original column demands, row filters and masks at each relation occurrence;
  exact rows/CDF/action bags; hidden-row/raw copy-forward; distinct targets and
  exact-registration writeback; checkpoints and registration reconciliation.
- Revocation between stage preparation and execution; copied/mixed/stale stage
  authority; foreign subjects/sessions; unsupported partition readers and runtime
  rewrites; stage reuse across attempts; block-loss retries and unknown-outcome
  no-replay. Reject unsupported transitions without unauthorized work or commit.
- Scoped runtime evidence on Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 with Ranger
  2.8.0 and S3-compatible storage. Reuse unchanged prior evidence explicitly;
  do not replay unrelated historical matrices as preparation.

The initial real-AQE subset should explicitly account for join-strategy conversion,
skew splitting, local shuffle readers, exchange reuse and other adaptive rewrites:
either qualify them or keep them rejected. No implicit AQE toggle, global bypass,
new metastore, Gravitino-core change or Spark/Delta fork is proposed.

## Integration remains separate

The current Python `AuthorizedSession.table` resolves Gravitino metadata and then
loads Delta by path, applying Ranger behavior in Python
(`workspaces/ranger-client/src/ranger_client/authorized_session.py:54-149`). Its
`sql` and attribute delegation expose the underlying session. These wrappers are
not proof of the new catalog's engine-side governance. Later integration must
substitute named governed execution, preserve identity separation and existing
policies, and use actual Ranger 2.8.0.

Policy-subquery dependencies, bootstrap registration/write ownership, shared-JVM
identity, separate executors and durable recovery-journal placement remain explicit
integration gates. There is no authorization here to migrate policies, alter the
demo's dirty files, deploy it or weaken current denials.

## Assessment provenance

Two independent read-only audits are retained under
`.superpowers/sdd/2026-10-25-delta-broader-spark-assessment/` (`demo-audit.md` and
`aqe-audit.md`). GitNexus aided demo navigation; current source was the authority
where index coverage/line locations were incomplete. Memory-search tools were not
available. No tests, services, builds or demo-source edits were performed. The only
existing documentation correction removes the stale pending-review label from
the already approved DataFrame milestone in `delta-spark-parity.md`.

The completed [DataFrame API plan](superpowers/plans/2026-10-25-delta-dataframe-merge.md)
and the deferred [demo plan](superpowers/plans/2026-10-22-delta-cdf-merge-demo.md)
remain the baseline. This assessment does not reopen completed milestones or claim
new compatibility.
