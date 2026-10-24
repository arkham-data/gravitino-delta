# Partitioned Delta MERGE Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. The user requires task-by-task execution; execute Task 1 first, review and report its result before later tasks.

**Goal:** Extend the governed named CDF-to-MERGE path to partitioned targets, preserving native storage semantics and existing Ranger policy behavior.

**Architecture:** Extend only the exact native writer successors owned by the current MERGE. Native Delta retains repartition, normalization, file routing, row reconstruction and CDF algorithms. Bind ordered partition identity in partitioned-target durable intents while preserving legacy unpartitioned-target fingerprints.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2; Ranger 2.8.0; Scala 2.12.21; JDK 17; MinIO in Task 3.

**Spec:** [Approved partitioned MERGE design](../specs/2026-10-24-delta-partitioned-merge-design.md). Implementation baseline `b5a260a37245ca2577bcdae03b7b2f35bbb8e66d` includes the completed arithmetic-mask fix. Existing worktree: `.worktrees/delta-spark-feasibility`, branch `codex/delta-spark-feasibility`.

**Task 1 verification:** Installed checks passed on Spark 3.5.4 with Delta 3.3.0 (59 cases) and Delta 3.3.2 (19 cases), using Ranger 2.8.0. Formatting/compilation passed. Independent review passed. Task 1 is complete and committed locally. This does not qualify general partitioned support or live deployment. Original evidence: `.superpowers/sdd/2026-10-24-delta-partitioned-merge/attempt-index.json`.

**Task 2 verification:** Installed checks passed on Spark 3.5.4 with each of Delta 3.3.0 and 3.3.2, using Ranger 2.8.0, Scala 2.12.21 and JDK 17.0.16. Each pin has 23 unique passing cases: 22 new policy/value/lifecycle tests and one existing named SQL smoke test. Module formatting and compilation passed. No production change was needed. Independent SPEC/QUALITY review passed. Task 2 is complete and committed locally. Original case-to-XML hashes and targeted replacements are recorded in `.superpowers/sdd/2026-10-24-delta-partitioned-merge/task-2/final-evidence-map.json`; superseded assertions are excluded from the final count.

**Task 3 verification:** All four Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 pairs passed five fresh classic PySpark live scenarios each against Ranger 2.8.0 and MinIO. All accepted original outer/Python/native-observer exits are zero. The scenarios cover distinct-target and same-table MERGE plus column, SELECT and MODIFY denial, with exact native rows/CDF/actions, untouched files and persisted registration/checkpoints. The catalog regression suites passed 59 cases and the acceptance checker suites passed 14 methods. Independent integrated SPEC/QUALITY review passed. Task 3 and the partitioned MERGE milestone are complete and committed locally. The retained index is `.superpowers/sdd/2026-10-24-delta-partitioned-merge/task-3/final-evidence-index.json`.

## Global Constraints

- No Gravitino core/API/server/client changes, second metastore or engine fork.
- Keep fixed schema and partition metadata, the existing classic/CDF protocol profiles, AQE disabled and the bounded CDF source and SQL clause grammar.
- Existing Ranger SELECT/UPDATE demands, row filters and masks remain authoritative. No new write constraint, unmasked-key requirement or automatic policy migration.
- Keep native task/source retries, exact registration/policy/session checks and current publication/recovery semantics. The platform prevents concurrent writer jobs for one dataset; this does not remove task retry obligations.
- Preserve the worktree and original evidence. Keep follow-up commits local.
- No blanket plan admission, structural-equivalence authorization for foreign plans, implicit settings changes or copied native row algorithms.
- No new permanent partition-type allowlist without a demonstrated native execution/ownership gap. Unknown branches continue to reject; qualify distinct type/conversion branches explicitly.
- No builder, Spark Connect or separate-worker/catalog-demo deployment through this plan.

## Review Focus

- A hidden row must actually share a touched file with a visible row; verify physical membership rather than only partition labels (Task 1).
- Empty strings normalize before native CHECK evaluation; distinguish native constraint errors from adapter rejection (Task 1).
- Reused txn identity must not accept changed partition order, source/target layout or action maps; preserve existing unpartitioned-target journals (Tasks 1–2).
- Masked partition predicates must not prune by raw partition values; explicit assignments and omitted copy-forward differ (Task 2).
- Timestamp/native path representation and CDC virtual routing are not ordinary user partition columns or simple directory-string comparisons (Tasks 2–3).

## Files and interfaces

Production Scala root: `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/` (S).
Tests root: corresponding `src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/` (T).

- `S/DeltaRangerMergeWrite.scala`: exact owned writer recipe, native statistics/constraint helper plans and physical ownership.
- `S/DeltaRangerMergeReceipt.scala`: original demands, snapshot/config/policy authority and durable command fingerprint.
- Existing `S/DeltaRangerMergeNative.scala`, `DeltaRangerMergeCommit.scala`, `DeltaRangerMergeJournal.scala`, `DeltaRangerCdfReadScans.scala`: inspect native selection/commit/helper contracts; edit only for a demonstrated new seam.
- `T/TestDeltaRangerPartitionedMerge.scala`: new native partitioned fixture and integration/ownership assertions.
- `T/TestDeltaRangerMergeRecovery.scala`, `TestDeltaRangerMergeCommit.scala`: extend existing recovery/intent fixtures or use dedicated partitioned tests when clearer; do not mirror implementation with hash-only tests.
- `delta/acceptance/cdf_merge_live.py`, its helper Scala/Python sources and tests: Task 3 live evidence only.

Reuse `DeltaRangerMergeWrite.write[T](frame: DataFrame)(body: => T): T`, its exact `normalize`/carrier checks, and `DeltaRangerMergeReceipt.journal`. `BoundRegistrationScan.nativePartitionStamp()` already provides length-framed ordered partition names; the binding also retains native schema types. No new public API or cross-task helper signature is required. Private helpers may be factored within these responsibilities.

## Task 1: Integrate the exact native partitioned writer

**Files:** Modify `S/DeltaRangerMergeWrite.scala` and `S/DeltaRangerMergeReceipt.scala`; create `T/TestDeltaRangerPartitionedMerge.scala`; narrowly reuse or extend installed MERGE/recovery fixtures. Do not modify production code outside these files without recording the exact encountered native seam. Root owns plan/spec status updates.

**Interfaces:** Consume the existing owned final output, active transaction metadata, captured SQLConf and bound source/target registration. Produce owned native repartition/CDC writer successors and partition-aware statistics while preserving existing carrier/receipt interfaces. Partitioned-target durable intent must include ordered source and target partition stamps; unpartitioned-target intent retains byte-identical legacy encoding even for a partitioned CDF source.

- [x] Add installed failing regression(s) using the existing `TestDeltaRangerInstalledMerge.withPolicies(... prepareTarget=...)` setup seam or a small dedicated fixture. Start with STRING partition keys. Use visible and hidden target rows in the same trusted-created data file, an unchanged row, a deletion candidate and an untouched partition. Assert their file membership before executing a named bounded CDF MERGE with UPDATE moving the visible row, DELETE and INSERT. Retain original RED output before production edits; expected initial failure is the unsupported native writer profile.
- [x] Trace the actual native successor before changing admission. Use the installed versions and retained pinned Delta source entries `MergeIntoCommandBase`, `TransactionalWrite`, `ClassicMergeExecutor`, `DelayedCommitProtocol`. Record repartition, CDC projection, empty-string normalization, constraint/statistics and physical-sort boundaries. Temporary diagnostics must be test-only and absent from final production. Do not disable native repartition to make the test pass.
- [x] Extend `write` and its exact owned recipe minimally: select native repartition-on/off using existing configuration; retain ordered partition attributes and the exact child; model applicable analyzer stages and CDC projection; exclude actual partition columns and CDC routing marker from the appropriate statistics input/table branches using the existing append-helper pattern. Keep native Empty2Null/constraint/sort execution. Preserve defaults/mapping/feature guards. No new user expression admission or general repartition authority.
- [x] Add a distinguished partitioned-target journal fingerprint containing both bound partition stamps. Keep the previous complete fingerprint encoding for unpartitioned targets, including partitioned sources. Prove actual known-commit duplicate/recovery behavior and changed-layout rejection; preserve old evidence, do not migrate files or fall back to legacy encoding for partitioned targets. If test fixtures need a historical fingerprint oracle, retain a frozen pre-change value/fixture plus a real recovery exercise, not only a duplicate implementation of the hash.
- [x] Prove exact raw final row bags, old/new partition action maps, hidden and unchanged raw values, untouched partition files and native CDF preimage/postimage/delete/insert bags. Include CDF-enabled and disabled targets, both repartition settings and both statistics schema settings, ordered multiple STRING partition columns, null/empty strings and native CHECK success/failure. Use a small deliberate scenario matrix covering both settings without redundant Cartesian runs. Check that only real partition keys appear in persisted actions and that partition columns are absent from native file statistics.
- [x] Add exact-ownership negatives: changed/reordered partition expressions, foreign child, copied/expired carrier and changed captured settings. Assert rejection before committed changes; use existing lifecycle test hooks, not production diagnostics. Run impacted existing installed-MERGE, receipt/ownership and recovery tests selected from the actual changes. Preserve unpartitioned behavior and strict existing source-count checks.
- [x] Run the new partition suite and selected affected tests on Spark 3.5.4/Delta 3.3.0, then the new suite and directly affected recovery/installed smoke on Spark 3.5.4/Delta 3.3.2. Record selectors before each final run. Keep commands, original exits/XML/logs, source hashes and loaded Spark/Delta/Ranger origins. Run module formatting and compilation. No Docker/live matrix or Spark 3.5.6 run is required for this first integration gate.
- [x] Obtain independent SPEC/QUALITY review against the full Task 1 baseline, fix concrete findings with targeted tests, and commit only reviewed files locally. Update the task status to the precise installed scope; keep partitioned support globally unqualified pending Tasks 2–3. Preserve all evidence; no push/PR.

## Task 2: Qualify policies, partition values and lifecycle

**Files:** Extend `T/TestDeltaRangerPartitionedMerge.scala` or cohesive new partition policy/lifecycle suites; existing MERGE policy/commit/recovery tests as needed. Production changes limited to demonstrated failures in the same owned path.

**Interfaces:** Consume Task 1's owned partition writer and partitioned durable fingerprint; preserve the original source/target policy demand and publication interfaces. No frontend or native row-algorithm change.

- [x] Prove masked partition predicates use governed values; omitted partition fields remain raw, explicit self-assignment masks once, and literal partition writes require UPDATE without spurious SELECT. Cover hidden-key collisions and updates moving outside a target filter without a postimage constraint.
- [x] Test original column denies, INSERT/DELETE all-column write demands, unreachable clauses and empty source. Assert exact committed state/checkpoint outcomes. Test ordered multiple keys, escaped/null/empty values, integer/date and materially distinct decimal/binary/timestamp branches admitted by the existing profile, using native-oracle observations. Exercise timestamp session UTC/non-UTC and account for native JVM/UTC-normalization settings.
- [x] Prove partitioned same-table CDF writeback fixes its interval and closes source authority before native publication. Reject alternate named aliases and stale source receipts. Test native ambiguity/constraints and relevant source retries without weakening strict native validation.
- [x] Extend post-helper revocation/subject/settings/registration drift cases after files exist; committed rows and version remain unchanged on denial. Mutate AddFile/RemoveFile/AddCDCFile partition maps while holding paths fixed, then paths/CDC marker, and prove exact intent/publication rejection. Test authorized known duplicate, denied duplicate, recovery and unknown-outcome no-replay behavior.
- [x] Run focused affected tests, retain original evidence, obtain independent review and commit locally. Record precise remaining type/settings qualification gaps for Task 3; do not advertise general partitioned support yet.

### Task 2 native conversion limits and remaining qualification

The installed tests compare the governed path with stock Delta, including exact row/event bags and partition action maps. Both Delta pins reproduce these native limitations on the local filesystem:

- Invalid UTF-8 binary partition bytes are lossy: authored hex `FF2F3D25` reads back as `EFBFBD2F3D25`. This proves native conversion parity, not lossless binary support.
- A binary partition containing NUL (`X'00ff2f3d25'`) fails in the native writer with `TASK_WRITE_FAILED` caused by `IOException: Mkdirs failed`. The governed failure leaves committed state and checkpoint unchanged. This local-filesystem result does not establish S3/MinIO behavior.
- With Spark session timezone `America/Los_Angeles`, JVM timezone `UTC`, and `spark.databricks.delta.write.utcTimestampPartitionValues=true`, authored `2024-03-11 08:02:03.654321Z` persists as `2024-03-11 01:02:03.654321Z`. Stock and governed paths both shift the instant. Matched UTC and matched Los Angeles cases were also exercised, including Los Angeles with native UTC normalization enabled and disabled. Arbitrary timezone combinations are not qualified.

The numeric coverage includes ordered `region(INT), secret(STRING)` keys, DATE and DECIMAL(12,3); it does not qualify every scalar type, precision or scale. No implicit settings changes or new conversion guards were added. Task 3 extends the installed cases to Spark 3.5.6 and separately qualifies a single STRING partition column with null/escaped values through real PySpark SQL, Ranger 2.8.0 and MinIO on all four pairs. Other scalar/settings/storage combinations and separate-worker deployment remain unqualified; the native limitations above still apply.

## Task 3: Qualify four runtime pairs with Ranger and S3

**Files:** Extend existing `delta/acceptance/cdf_merge_live.py` and its fixture/checker tests and helper sources. Update `docs/delta-cdf-merge.md`, `docs/delta-spark-parity.md`, relevant module/acceptance README and this plan after final evidence.

**Interfaces:** Reuse Tasks 1–2 exact named SQL, owned writer and durable intent. Reuse existing live fixture separation of setup/metadata/authenticated application identities, independent observer and checkpoint records.

**Live-discovered prerequisite:** The first partitioned source failed public
PUBLISH before MERGE because relational partition DTOs and detached domain
transforms compare unequal. Correct only the Delta provider's complete-row
comparison in `DeltaRegistrationStore.replace`, with real relational
publication/refresh and changed-state regression tests. Preserve the full
optimistic check and all Gravitino core code. Rebuild the provider in a fresh
immutable base; retain the failed fixture and use fresh corrected live cells.
See the [design addendum](../specs/2026-10-24-delta-partitioned-merge-design.md#registration-representation-correction-found-during-live-qualification).

**Focused evidence selection:** The final mapping distinguishes fresh Task 3
runs from inherited Tasks 1–2 cases:

| Spark | Delta | Fresh focused cases in Task 3 | Inherited earlier partition cases | Fresh live scenarios |
| --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 38 | 0 | 5 |
| 3.5.4 | 3.3.2 | 3 | 35 | 5 |
| 3.5.6 | 3.3.0 | 38 | 0 | 5 |
| 3.5.6 | 3.3.2 | 38 | 0 | 5 |

The first 38-case run predates the provider correction, which is absent from the
Spark test dependency graph. The third pair's 38-case run predates a fixture-only
Ranger pagination correction. Their unchanged Spark behavior is bound explicitly
to the final packages. For the first two pairs, original common-JAR copies are
unavailable: inheritance uses verified unchanged production/resource/build inputs,
dependency records and original Gradle/XML evidence, not a claim of archive-byte
or loaded-byte equality. The actual third/fourth common-JAR archives are retained;
their only archive-entry difference is generated `compile.date` metadata.

The failed registration fixture, failed snapshot-flag checker attempt and failed
first-page-only Ranger lookup remain failed evidence. Successful first-two-pair
results retain their original bytes; separate revalidation receipts bind identical
archived source bytes after the fixture helper changed. No failed run is counted
as a successful live cell. Final source/documentation deltas are recorded separately.

- [x] Add partition-specific live distinct-target and same-table scenarios with real classic PySpark SQL, Ranger 2.8.0 and MinIO. Validate original native row/event multisets, partition action maps, untouched files, registration and consumer checkpoint outcomes; include masks/filters and denial. Test checker corruption cases for newly required fields.
- [x] Build fresh immutable runtime packages and run all four Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 pairs. Run only the affected focused selectors and new partition live cases. Retain runtime/provider/identity/source hashes and original process exits; old package success is historical evidence only.
- [x] Review complete implementation against the approved design and original runtime evidence, resolve concrete issues, document exactly qualified profiles and remaining limitations, and commit locally. No automatic push, PR or catalog-demo deployment. All three tasks must be complete before this partitioned feature milestone closes.

## Execution commands and evidence

Use JDK17 `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.
Installed tests use `-I delta/acceptance/cdf_merge_live.gradle` and task
`:spark-connector:spark-delta-ranger-3.5:test`, with
`-PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon -Dorg.gradle.jvmargs=-Xmx1536m --console=plain`.
Select `--tests '*TestDeltaRangerPartitionedMerge'` and the documented affected existing selectors; switch only the requested pins for later runs. Preserve XML before the next invocation overwrites Gradle output. Use module `spotlessApply`/`spotlessCheck` and compile tasks, not a repository-wide build as setup.

Planning evidence: `.superpowers/sdd/2026-10-24-delta-partitioned-merge-planning/discovery.md` and `builder-check.md`. New execution evidence belongs to this plan's separate SDD workspace. Read earlier exact source/fixture artifacts only when needed; never rewrite historical ledgers/manifests. Keep runtime work sequential and any sleep inhibitor bounded and recorded.
