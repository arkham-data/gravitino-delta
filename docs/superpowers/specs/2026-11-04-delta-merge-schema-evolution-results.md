# Installed additive MERGE schema evolution — November 4, 2026

The finite additive MERGE cohort is qualified across Spark 3.5.4/3.5.6 × Delta
3.3.0/3.3.2. The installed local fixture results below continue
the [additive MERGE design](2026-11-04-delta-merge-schema-evolution-design.md) and
[public MERGE contract](../../delta-cdf-merge.md). They establish no new deployed
Gravitino provider, Ranger server 2.8.0, MinIO or PySpark result. A consolidated
live gate remains required for the selected capabilities.

The independent task and whole-slice reviews are clean. The integrated review
covers the additive APPEND checkpoint `5bb3af7dd4d5cd54d4c1b45e705fc83abff72998`
through `9ba1da00234fbec0eba8515a866af467b3b98a73` and found no actionable
defect. The bounded installed checkpoint is closed; the selected schema track
and consolidated live gate remain open. The preserved review report and ledger
record the findings, evidence checks and exact remote publication.

## Exact cohort and provenance

Each pair requires the same **94 exact JUnit method identities**: 31 installed
evolution methods, 15 pure intent methods, 43 affected regressions and five
stock-native-control methods covering 11 native scenarios. Task 2's 15 pure cases
are already included. A test count alone is insufficient: exact identities,
zero failures/errors/skips, current source hashes, loaded runtime origins, original
logs/exits and primary native evidence are retained and independently checked.

| Spark | Delta | Fresh Task 4 methods | Reused methods | Status |
| --- | --- | ---: | --- | --- |
| 3.5.6 | 3.3.0 | 0 | 89 impact-qualified Task 3 + 5 source-identical stock controls | Passed with retained scope |
| 3.5.6 | 3.3.2 | 89 | 5 source-identical stock controls | Passed |
| 3.5.4 | 3.3.0 | 94 | None | Passed |
| 3.5.4 | 3.3.2 | 94 | None | Passed |

All four qualified cells have zero selected failures/errors/skips. The three new
invocations exited 0 with 277 method executions in total. The remaining 99
pair/method results are explicitly reused, giving 376 qualified pair/method
identities. Every new scenario records actual Java 17.0.16, Scala 2.12.21 and
Ranger client 2.8.0 origins, with the requested Spark and Delta JARs.

The retained first cell is a composition of distinct original runs, not a fresh
94-method run or whole-file final-source identity. Task 3's final reviewed index
contains 13 fresh amended-path results and 76 reviewed unchanged-path results
after the focused recovery-outcome ordering fix. Its original failed runs remain
intact. The original 88-method history includes 10 source-provenance exceptions
whose historical compiled-class hashes were never captured; no unavailable
class-byte equality or historical full-classpath byte equality is asserted. The
final 89 index and fix review retain those limits explicitly.

Task 1's five stock-control methods are reused on both Spark 3.5.6 Delta pins.
Their schema helper, APPEND expectation helper, test and stock fixture sources
match the retained original hashes. The transitive options helper is identical
to the committed Task 1 tree. Stock controls use the fixture without a governed
extension; later receipt/journal/source-path changes do not execute there.
Original loaded Spark/Delta/Scala/Java origins remain attached to those controls.
They are native schema controls, not governance or deployed-provider evidence.

The producing product revision is `a78f59f4d5b74197b78d305b4807996d762131db`.
Task 4 changes no product or test source. The retained workspace is
`.superpowers/sdd/2026-11-04-delta-merge-schema-evolution/`; `task-4-report.md`
records commands and outcomes. `task-4-evidence/cohort-94.tsv` freezes the exact
cohort before runs; `freeze.json` describes reuse and scope independence.
The input freeze retains 210 Java/Scala sources, three resources and 21 build/format
inputs. Per-cell logs, fresh XML, start/end/exit, source/class manifests, native
artifacts and runtime JAR origins/hashes remain separate. Both native-evidence options use
fresh existing roots, and scenario directories refuse reuse. No unchanged broad
APPEND matrix or external-service rebuild is part of this qualification.

## Observed contract

Both installed named SQL and the unchanged JVM companion API version 1 use native
Delta resolution, preprocessing and execution for one atomic schema-and-data
MERGE. Python signatures remain unchanged; this slice directly tests SQL and the
JVM companion and adds no Python deployment claim. Session autoMerge and the
qualified constraintPropagation=false profile precede source analysis and handle
construction, remain through execution and are restored afterward. Changing them
only before execute does not satisfy settings provenance.

Only concrete nullable top-level scalar additions are admitted. Existing field
order/name/type/nullability/metadata, native table UUID, configuration, partition
layout and protocol remain exact. Disabled evolution may ignore an unused extra
source field while committing existing-field updates; original source SELECT
obligations for the requested payload and predicates remain. Enabled
matching-schema writes need no ALTER solely for the flag. Real additions require ALTER and added-column UPDATE under
the existing captured subject, policy and live boundary checks. No raw-key/row
bypass or blanket unmasking is introduced. Destination filters retain read/match
semantics; hidden physical rows do not suppress an authorized insert, and omitted
old fields keep raw values during native file reconstruction.

| Installed scenario | Native evidence |
| --- | --- |
| SQL and JVM companion stars with explicitly projected wider payload | Version 0 → 1; one Metadata action with native data actions; complete matched/new/untouched row bag and fresh named schema |
| Empty source with actual addition | Metadata-only evolution commits; not a skipped no-op |
| Partitioned trusted AQE/CDF | Native partitioned actions, CDF pre/postimages and unchanged protocol |
| Version-ten evolution | Actual version 10 native checkpoint and expected-schema reconstruction |
| Duplicate match and NOT NULL failure | No new native schema/data publication |
| ALTER expiry after file generation | Not committed; old native version and retained orphan inventory |
| Registration/journal/recache acknowledgement loss | Exact native proof retains committed outcome and allows only authorized reconciliation without replay |
| Recovery acknowledgement persistence failure | Committed version/digest retained before failing journal persistence; durable record stays PREPARED; no source attempt, files or extra native version |
| Native invocation without exact proof | Unknown outcome and generated-file evidence; no automatic replay |
| Native source-loss fault before/after publication | Exact original failure/outcome retained; named readback explicitly unconfirmed |
| Evolving target cache / matching-schema raw cache | Evolving plan refuses before mutation; matching-schema write commits then fails stock recache, remaining committed/unconfirmed |

Recovery requires the original exact native commit/proof/digest and transaction
marker, fresh named registration and authorization, original ALTER and new-column
UPDATE obligations. A matching current schema does not downgrade a retained
format 2 operation to format 1. Compatible later data-only advancement may
reconcile; any later Metadata/Protocol change, including a reverted schema,
refuses. Missing, unknown, malformed or mismatched evidence never authorizes
replay. Recovery-only source views cannot open payload or native history.

## Remaining scope

The source remains a fresh bounded named CDF interval with the existing finite
projection/filter/alias grammar. Explicitly project payload fields for evolving
stars rather than accidentally adding authored CDF metadata columns. Ordinary
governed CDF intervals spanning additive schema boundaries still refuse and are
the next required schema-track task. Recovery-only deferral supplies no ordinary
historical read authority.

Nested/type-changing/full-schema evolution, defaults/generated/identity fields,
column mapping, broader feature protocols, CHECK constraint protocols, insert-only
MERGE and NOT MATCHED BY SOURCE remain outside this result. Native NOT NULL failure
is qualified. Target caches are limited as recorded above; successful APPEND
cache behavior is not inherited by MERGE. Native settings, checksum and policies
are preserved; no option is disabled to obtain a passing cell. Auto-compaction
is not newly qualified by these controls.

Gravitino owns registration; Delta owns native files/actions, table features and
transactions. No core/API/server/client/storage schema, Spark/Delta fork, second
metastore or replacement transaction engine is introduced. Registration readback
uses the installed named fixture, not a new actual-provider deployment. Policies
are test-authored installed Ranger fixtures, not new public grant propagation.
One writing job per dataset is a platform constraint; sources may still advance
while downstream jobs run.

The selected order remains schema evolution, governed creation, required
inputs/transformations, then overwrite/full refresh. This connector slice does
not close the schema track or qualify broader Spark Hub snapshots, joins, unions,
windows, aggregates, UDFs, materialization or its real settings lifecycle.
Consolidated actual Ranger server 2.8.0/MinIO acceptance and separate-worker
execution remain open. Checkpoint publication/review is tracked separately in
the [implementation plan](../plans/2026-11-04-delta-merge-schema-evolution.md) and
[adoption roadmap](../plans/2026-10-29-spark-hub-adoption.md).
