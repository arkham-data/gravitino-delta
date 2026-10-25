# Named DataFrame MERGE Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. The user already selected this method. Complete and independently review Task 1 before starting Task 2; use the same gate for subsequent tasks. Steps use checkbox syntax for tracking.

**Goal:** Ship and qualify a named DataFrame MERGE companion for classic PySpark and the JVM, preserving the existing governed SQL behavior.

**Architecture:** Build a complete Spark MergeIntoTable from the real source DataFrame's analyzed plan and authored clauses. Submit through normal analysis into the existing DeltaRangerMergeCommand/receipt/native execution path. The Python facade transports DataFrames and expressions; it does not execute SQL or implement policy decisions.

**Tech Stack:** Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17, Scala 2.12.21, classic PySpark and MinIO.

**Spec:** [Approved DataFrame API design](../specs/2026-10-25-delta-dataframe-merge-design.md). Design approved by the user's “Proceed” on 2026-10-25. Planning baseline: `6e185d38a`; product baseline: `09e092b2f937b1db17f992aedbeb22763162e527`. Worktree: `.worktrees/delta-spark-feasibility`; branch: `codex/delta-spark-feasibility`.

**Status:** Approved for task-by-task execution on 2026-10-25. Tasks 1–2 are complete and independently reviewed; Tasks 3–4 remain unstarted. This supersedes the implementation outline in [the earlier builder plan](2026-10-22-delta-cdf-merge-builder.md).

**Task 1 verification:** Spark 3.5.4 with Delta 3.3.0 and 3.3.2 each passed 37 installed cases: 14 new builder cases, 11 existing installed MERGE cases, 4 ownership cases and 8 source cases. Each original run exited 0 with no failures, errors or skips; loaded origins identify Ranger 2.8.0, Scala 2.12.21 and Java 17.0.16. Module formatting and diff checks passed. Independent SPEC/QUALITY review approved the frozen five-file product candidate. Evidence and the full report are retained in `.superpowers/sdd/2026-10-25-delta-dataframe-merge/`.

The only shared production correction omits an exact identity physical projection when issuing the owned source recipe, preserving existing transition/ownership checks. The new factory initializes the native analyzer before capturing settings, and the builder validates both clause-construction context and the exact governed command before execution. Python, Spark 3.5.6, the four-pair live matrix and integration remain unqualified by Task 1.

**Task 2 verification:** The classic Python facade and reproducible zip are implemented. Six Python facade/archive tests and 40 CDF package/checker tests passed. Actual spark-submit preflights rejected both missing and incompatible JVM APIs before the target factory. The packaged DataFrame frontend passed all five partitioned live phases on Spark 3.5.4/Delta 3.3.0 with Ranger 2.8.0 and MinIO: distinct destination, column UPDATE denial, source SELECT denial, target MODIFY denial and same-table writeback. Original outer/Python/native exits are all 0; rows, CDF/actions, checkpoints and persisted registration after restart passed strict checks. Independent SPEC/QUALITY review approved the candidate.

The final package source inventory adds a Python cache ignore and excludes four retained generated cache entries. Executable source inputs, all JVM/app/provider bytes and Python zip/member bytes are identical to the successful original live package; the original result remains unchanged and strictly validates. This non-runtime delta is recorded in `task-2/non-runtime-delta.json`; a redundant live replay was not needed. Missing/mismatched API preflights and the final zip import have their own original evidence. Broader frontend lifecycle coverage and the final four-pair matrix remain Tasks 3–4.

## Global Constraints

- Keep Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, Ranger 2.8.0, Java 17 and Scala 2.12.21.
- Do not modify Gravitino core/API/server/clients, introduce another metastore, fork Spark/Delta, or fall back to paths.
- No source-payload collect, toPandas, createDataFrame reconstruction or temporary-view SQL bridge.
- Preserve existing source SELECT/filter/mask ordering and target read semantics, original column demands, native row algorithms, retries, journal and registration reconciliation. No unmasked-key requirement, reverse masking, deduplication, postimage constraint or policy migration.
- Keep one fresh bounded named CDF source, its admitted Project/Filter/SubqueryAlias and scalar grammar, fixed schema/partition metadata, and the already qualified classic target profiles. AQE remains disabled and must never be toggled implicitly.
- Support strings and Columns, ordered matched UPDATE/DELETE and unmatched INSERT, update-all/insert-all, unpartitioned/partitioned targets, and exact same-registration writeback. Preserve native ambiguity and constraint behavior.
- Builders are single-use, not thread-safe; failed execute seals them. Native retries and authorized known-commit reconciliation remain available through the existing command with fresh caller handles/sources.
- Retain setup, metadata and application identity separation. No application receives setup credentials or a Python-authored execution permit.
- Keep follow-up commits local. Preserve worktrees and original evidence. No catalog-demo edits/deployment, remote push, PR, wheel publication, Spark Connect, streaming or broader Spark execution through this plan.

## Review Focus

- A resolved Column from a different or earlier DataFrame can have a matching name/type; reject it instead of rebinding by text (Task 1).
- Mutating a caller's assignment map after adding a clause must not change the command; invalid arguments must not append a partial clause (Tasks 1–2).
- A builder created before a subject/settings change must reject execution; construction must not preserve a revoked policy decision (Tasks 1 and 3).
- Strings containing quotes/backticks and case-equivalent assignment keys must retain parser semantics without SQL interpolation or duplicate writes (Tasks 1–2).
- A Python import from a source checkout or a mismatched zip/JAR can look successful; actual spark-submit must load the built zip and matching JVM API, with failures before writes (Tasks 2 and 4).

## Files and shared interfaces

The following roots are exact path abbreviations used below:

- **S:** `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`
- **T:** `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/`
- **P:** `spark-connector/v3.5/spark-delta-ranger/python/`
- **A:** `delta/acceptance/`

Production responsibilities:

- `S/GravitinoDeltaTable.scala`: public factory, session/subject/settings capture, target name and alias, API version handshake.
- `S/GravitinoDeltaMergeBuilder.scala`: public ordered clauses, defensive copies, single-use state, complete logical command and synchronous submission.
- `S/DeltaRangerMergeBuilderExpressions.scala`: private parsing, assignment keys, string/Column validation and exact source-attribute provenance. No policy authority.
- Existing `S/DeltaRangerMergeCommand.scala`, `DeltaRangerReadRule.scala`, `DeltaRangerCdfRead.scala`, `DeltaRangerMergeReceipt.scala`: reuse existing seams; change only if an installed failure identifies a necessary integration fix. Record the reason and affected regression selectors before editing these shared paths.
- `P/gravitino_delta/__init__.py` and `P/gravitino_delta/tables.py`: exports and classic Python facade.
- `spark-connector/v3.5/spark-delta-ranger/build.gradle.kts`: deterministic Python zip task; existing Ranger runtime bundle remains the JVM distribution.

Public JVM types live in `org.apache.gravitino.spark.connector.delta.ranger`.
Use the following signatures; all fluent methods return the stated builder/table:

```scala
object GravitinoDeltaTable {
  def apiVersion(): Int // 1
  def forName(spark: SparkSession, name: String): GravitinoDeltaTable
}
final class GravitinoDeltaTable {
  def alias(name: String): GravitinoDeltaTable
  def merge(source: DataFrame, condition: String): GravitinoDeltaMergeBuilder
  def merge(source: DataFrame, condition: Column): GravitinoDeltaMergeBuilder
}
final class GravitinoDeltaMergeBuilder {
  def whenMatchedUpdate(assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  def whenMatchedUpdate(condition: String, assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  def whenMatchedUpdate(condition: Column, assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  def whenNotMatchedInsert(assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  def whenNotMatchedInsert(condition: String, assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  def whenNotMatchedInsert(condition: Column, assignments: java.util.Map[String, _]): GravitinoDeltaMergeBuilder
  // Each has no-argument, String-condition and Column-condition overloads:
  def whenMatchedDelete(): GravitinoDeltaMergeBuilder
  def whenMatchedUpdateAll(): GravitinoDeltaMergeBuilder
  def whenNotMatchedInsertAll(): GravitinoDeltaMergeBuilder
  def execute(): Unit
}
```

Use normal imports in implementation; fully qualified Map above only distinguishes
Java gateway collections from Scala maps in this interface table. Map values must
be String or Column; wildcard generics allow mixed values without erased overload
collisions. Reject other values and nulls before clause mutation. A null predicate
is not the no-condition overload. Reject empty maps; UPDATE/INSERT defaults require
explicit supported actions rather than an ambiguous empty mapping. Factory and
table handles retain captured context; merge takes a snapshot of the target alias
so later alias calls cannot alter an existing builder. Private helper signatures
and constructor visibility are implementer choices; no public receipt constructors.

Python signatures use `Expression = Union[str, Column]`:

```python
GravitinoDeltaTable.forName(spark: SparkSession, name: str) -> GravitinoDeltaTable
table.alias(name: str) -> GravitinoDeltaTable
table.merge(source: DataFrame, condition: Expression) -> GravitinoDeltaMergeBuilder
builder.whenMatchedUpdate(condition=None, set=None) -> GravitinoDeltaMergeBuilder
builder.whenMatchedDelete(condition=None) -> GravitinoDeltaMergeBuilder
builder.whenNotMatchedInsert(condition=None, values=None) -> GravitinoDeltaMergeBuilder
builder.whenMatchedUpdateAll(condition=None) -> GravitinoDeltaMergeBuilder
builder.whenNotMatchedInsertAll(condition=None) -> GravitinoDeltaMergeBuilder
builder.execute() -> None
```

`set`/`values` are nonempty mappings from str to Expression. Python None selects the
no-condition JVM overload; it is not sent as an ambiguous null. Validation errors
are TypeError for wrong Python types and ValueError for missing/empty required
values; missing/mismatched JVM API raises a clear RuntimeError. Preserve Spark's
native/governance exception from execution, without replacing it with success or
retrying. JVM arguments must also be validated independently of Python.

## Task 1: Establish the installed JVM DataFrame MERGE path

**Files:** Create the three production Scala files above and `T/TestGravitinoDeltaMergeBuilder.scala`. Reuse `TestDeltaRangerInstalledMerge.withPolicies`, `PartitionedMergeFixture` and existing ownership tests. Shared production edits are conditional on demonstrated failures, not a planned refactor.

**Interfaces:** Consume the exact analyzed DataFrame plan and existing `DeltaRangerMergeCommand.resolve`/`DeltaRangerMergeReceipt.issue` path. Produce the JVM API above, including API version 1, for Python to invoke directly.

- [x] Add an installed RED `namedDataFrameUsesOwnedMerge` using a fresh source from `spark.read.option("readChangeFeed", "true").option("startingVersion", 0).option("endingVersion", 4).table("a.db.t")`, filter version 4 and exclude update_preimage, then alias s. Use the existing distinct SQL fixture and matching UPDATE/DELETE/INSERT clauses. Assert the final row bag is exactly `(1L,"changed","keep"), (3L,null,"keep"), (4L,"new","keep")` and native version advances once with operation MERGE. Missing API compilation is the initial RED; retain it, then require installed runtime success rather than counting compilation as the proof.
- [x] Add `stringsAndBoundColumnsPreserveAuthoredAssignments`: fresh equivalent fixtures for expression strings and `col("t.id") === source("id")`, mixed string/Column assignment values and admitted source projections/filters/aliases. Verify actual source reaches the owned command, SQL-equivalent rows and no unissued command/receipt. Test both omitted secret and explicit `col("t.secret")` under a target mask: stored `b-one` versus `b-one-masked`, respectively.
- [x] Implement factory/context capture and all public clause methods. Parse canonical three-part target identifiers and alias identifiers with the owning parser; build UnresolvedRelation inside the full command, never an ordinary target DataFrame read. Copy mappings and expressions at clause addition; retain clause order and original assignment inventory, including star actions. Capture the source's analyzed pending plan without requesting optimizedPlan/executedPlan.
- [x] Implement expression/key validation and normal Spark command submission. Preserve Catalyst trees for Columns and bind resolved source attributes to the actual supplied source occurrence; reject foreign/stale Dataset attribute metadata and missing references. Resolve target references only through the full command. Execute normal query execution, discard only authorized command metrics, and return Unit after reconciliation. No direct invocation of native command.run or wrapper-created receipt. Seal execution before submission, including failures.
- [x] Add `rejectsForeignColumnsAndContexts`, `assignmentMapsAreSnapshots`, `invalidClauseDoesNotMutateBuilder`, `identifiersAndDuplicateAssignmentsUseSessionParser` and `executeIsSingleUse`. Assert: same-name foreign source Column rejects; changed session/subject/settings rejects; caller map mutations do not alter output; wrong type/null/empty map does not append a clause; case-equivalent keys reject under case-insensitive mode; quoted valid identifiers/literal quotes remain correct; a second execute and fresh wrapper over a consumed source cannot produce another commit. Use exact before/after native versions and row bags for rejection assertions.
- [x] Add negative installed cases for paths, unresolved/ambiguous references, actioned/cached/checkpointed sources, streaming, unsupported UDF/subquery/nondeterministic expressions, joins/windows/aggregations and AQE. Unsupported construction may reject earlier than execute; assert no committed target mutation and no implicit setting changes. Do not relax existing guards to make argument types look supported.
- [x] Run Task 1 tests on Spark 3.5.4 with both Delta pins using the command template below, plus existing installed SQL and ownership suites. Require loaded Ranger 2.8.0 origins, both string and bound-Column success, module formatting/compilation and diff checks. If source/Column identities do not survive normal analysis, stop at the exact failed boundary for a design correction before adding authority paths.
- [x] Obtain independent SPEC and QUALITY review of the task diff and retained results; fix concrete findings with affected checks. Commit locally as `feat(delta): add governed named DataFrame merge builder`. Report Task 1's installed scope; do not claim Python or complete milestone support.

## Task 2: Ship the classic Python facade and packaged frontend

**Files:** Create `P/gravitino_delta/__init__.py`, `P/gravitino_delta/tables.py`, `P/tests/test_tables.py`, and `A/cdf_merge_dataframe_pyspark.py`. Modify the module `build.gradle.kts`, `A/cdf_read_live_package.py`, `A/cdf_merge_live.py`, `A/cdf_merge_live_checks.py`, and corresponding existing checker tests. Add `A/test_cdf_merge_dataframe_checks.py` for new frontend/provenance validation. Reuse `T/DeltaRangerLiveCdfMerge.scala` for fixture setup/before/after/observer plumbing, changing only the necessary structured request contract. The existing source inventory already includes non-ignored tracked/untracked Python files; preserve its historical helper unchanged and add explicit zip/input binding in the current package code.

**Interfaces:** Consume Task 1 JVM signatures. Produce the Python signatures above, Gradle task `deltaPythonZip`, and artifact `build/distributions/gravitino-delta-python-<projectVersion>.zip` containing `gravitino_delta/`. Extend the existing live CLI with `--frontend sql|dataframe` (default sql); the dataframe profile must run the new Python entry point. The first smoke profile is `--partitioned --frontend dataframe`.

- [x] Write failing Python tests for all overloads, mixed expression maps, None-condition dispatch, invalid arguments, map copying and None return. Verify Columns cross as their actual `_jc` and sources as their actual `_jdf`; string values remain strings for JVM parsing. Test missing API and API version other than 1. Mock gateway tests prove transport only, not policy or installed compatibility.
- [x] Implement the facade without creating sessions or changing configuration. Accept classic PySpark only, retain the original session, check `apiVersion()==1`, pass typed DataFrames/Columns through the gateway and let the JVM independently validate provenance. Reject Connect objects clearly; never use `_jc.toString()` as an expression, call Spark SQL, or collect payload rows.
- [x] Add a reproducible Gradle Zip task over production Python files, excluding tests, pycache and local environments. Wire it into the module's assemble lifecycle. Test exact archive members and import from the built zip with the source directory absent from PYTHONPATH. Test mismatched/missing JVM classes before target writes.
- [x] Extend package capture/verification to include the zip path/hash, member hashes, Python production source inputs and `GravitinoDeltaTable`/builder JVM provider origins. A Python-only change must invalidate its artifact evidence even if an unchanged JVM companion can be reused. Add checker negatives for changed zip bytes, wrong member/source binding, version mismatch and source-checkout import masquerading as packaged import. Keep legacy SQL manifests/profiles explicit rather than retroactively relabeling them.
- [x] Add structured DataFrame operations to the live request: canonical source/target, inclusive start/end, clause profile and expression mode (strings or Columns). Reuse fixture before/after controls, but Python constructs the DataFrame and invokes the public builder itself. Do not derive the operation by parsing SQL text or delegate execution to a Scala helper. Record frontend `classic-pyspark-dataframe`, module import origin, API version, requested operations, query-thread identity and original Python exit. For the new profile, the checker must reject a SQL frontend record even if target rows match.
- [x] Run Python unit/checker tests, build the zip and requested-pin runtime, and execute the existing five partitioned phases through actual packaged Python on Spark 3.5.4/Delta 3.3.0 with Ranger 2.8.0/MinIO. Use strings for one successful MERGE and Columns/mixed values for the other. Verify distinct-target and same-table results plus column/source/target denial, original process exits, CDF/actions and checkpoints. This is the Python smoke gate, not final four-pair qualification.
- [x] Review the Python/public packaging and harness diff independently and commit locally as `feat(delta): expose packaged PySpark merge API`. Preserve any failed attempts and exact package identities; broad policy/lifecycle testing follows in Task 3.

## Task 3: Qualify policy semantics and lifecycle through the API

**Files:** Create `T/TestGravitinoDeltaMergePolicies.scala` and `T/TestGravitinoDeltaMergeLifecycle.scala`. Extend `T/TestGravitinoDeltaMergeBuilder.scala` and existing policy/partition/recovery fixtures narrowly as needed. Production edits require a demonstrated failing case and regression selection; no policy or native algorithm redesign.

**Interfaces:** Consume Task 1 builder, Task 2 Python behavior and the existing SQL contract. Produce a scenario-to-evidence map that Task 4 uses to select new live cases and justify unchanged deep-native coverage.

- [ ] Add paired SQL/API policy cases for hidden rows sharing a touched file, omitted versus explicit masked assignments, source and target masked keys, literal assignment without spurious SELECT, column-only grants, denies under wildcard grants, and unreachable/empty-source demands. Use separate equivalent fixtures; assert exact native row/CDF bags and demanded resources, not only success. Include hidden-key INSERT and updates outside target visibility without adding a postimage constraint.
- [ ] Test ordered conditional UPDATE/DELETE/INSERT plus update-all/insert-all, quoted/literal values, missing source columns and ambiguous references. Verify native clause-order errors and multiple-source ambiguity leave target/checkpoint unchanged. A star expands to real original column demands; native synthesized self-assignments must not be mistaken for explicit user assignments.
- [ ] Exercise partition moves, escaped/null partition values, raw hidden/unchanged file preservation and exact same-registration writeback through the builder. Fix source bounds before mutation; reject alternate named aliases of the same dataset. Retain the previously documented scalar/timezone/native limits rather than claiming a larger partition profile.
- [ ] Add drift between construction and execute (subject, settings, source registration, source mask/filter or grants), plus post-helper revocation after data files exist. Assert current authority checks, unchanged committed version/state/checkpoint on denial, and no reuse of stale CDF observations. Target name resolves at execute as specified; do not impose a frozen target snapshot on a name-only handle.
- [ ] Drive existing native materialization retry and acknowledgement-failure hooks through the API: successful owned block-loss retry, policy change between attempts, known-commit reconciliation without a second write, denied duplicate, changed intent and unknown-outcome no replay. Configure txnAppId/txnVersion before factory/source creation and rebuild both handles for a user retry. Assert durable intent, source interval, native versions, reconciliation and consumer checkpoint behavior. No wrapper-level automatic retry.
- [ ] Run new policy/lifecycle suites and directly affected existing SQL/partition/recovery tests on Spark 3.5.4 with both Delta pins. Existing deep-native cases may be inherited only with recorded unchanged dependencies; new public-frontend cases need new results. Record selectors, original XML/exits, source inputs and loaded origins; finish module format checks.
- [ ] Obtain independent SPEC/QUALITY review and commit locally as `test(delta): qualify DataFrame merge policies and lifecycle` (use a fix title instead if production changes are substantive). Record exactly which frontend/runtime cases remain for Task 4.

## Task 4: Qualify the final API across all four runtime pairs

**Files:** Extend `A/cdf_merge_dataframe_pyspark.py`, live/package checker code and tests from Task 2, and live fixture helpers only where final scenarios need them. Update `docs/delta-cdf-merge.md`, `docs/delta-spark-parity.md`, `A/README.md`, the earlier builder outline and this plan. The catalog-demo plan gets only a dependency-status update, not deployment edits.

**Interfaces:** Consume final builder/Python artifacts and Task 3 scenario map. Produce a final evidence index binding each pinned pair to exact source/package inputs, focused test results, real Python live results, provider origins and original process exits.

- [ ] Resolve the nonblocking Task 2 archive-selection finding: replace first-glob selection with deterministic expected-archive selection or explicit ambiguity rejection, and test requested plus stale version archives. Current exact member/source validation rejects wrong bytes; this follow-up makes stale build outputs unambiguous.
- [ ] Complete new checker tests before runtime work: a false frontend label, wrong zip/JAR/API version, changed source request bounds, successful-looking denial with target advancement, altered partition/CDF bags or missing original Python/native exit must reject. Preserve working SQL checker coverage.
- [ ] Freeze the final candidate after focused checks; build one immutable requested-pin runtime package per pair, with the Python zip bound to each. Verify shared provider/base inputs and existing fixture credentials without exposing them. If shared inputs changed, rebuild the affected base; do not describe an old package as fresh. Keep processes serialized and check available disk before materializing artifacts.
- [ ] Run the new builder/policy/lifecycle suites on all four Spark/Delta combinations, with existing SQL smoke/ownership tests affected by actual shared changes. Run actual packaged Python live phases on Ranger 2.8.0/MinIO for all four pairs: distinct destination, source SELECT denial, target MODIFY denial, column UPDATE denial and same-table writeback using partitioned targets. Across the two successful phases exercise strings, bound Columns and mixed assignments. Focused installed suites also cover unpartitioned targets on all four pairs; do not claim live unpartitioned coverage unless separately run.
- [ ] Compare exact native rows/CDF/action bags, escaped/null partition maps, hidden-row preservation, untouched files, source interval/checkpoint records, public/persisted registration and restart identity. Bind setup/metadata/application identities and loaded Spark/Delta/Ranger/Scala/Java origins. A Scala fixture process may prepare/observe; actual Python must construct and execute the API under the application identity.
- [ ] Map original logs/XML/exits to the final candidate. Retain failed attempts as failed, and distinguish Task 2 smoke from final qualification. Reuse earlier focused checks only where source/dependency equality is demonstrated; rerun affected checks after corrections, not unrelated historical matrices. Never modify original results to make them match later source files.
- [ ] Document the supported API and install/use example, synchronous return and checkpoint semantics, same-session/single-use constraints and exact expression/source grammar. Explicitly retain stock DeltaTable.forName, AQE, arbitrary joins/windows/aggregation/cache, Connect/streaming and separate-worker limitations. Record that catalog-demo still needs its separate plan and an AQE scope decision.
- [ ] Obtain integrated independent SPEC/QUALITY review against all four tasks and original evidence. Resolve concrete findings, commit locally as `feat(delta): qualify named PySpark DataFrame merge`, and report the supported scope and remaining prerequisites. Do not close full Delta compatibility or start integration.

## Commands and evidence

Use a new evidence workspace `.superpowers/sdd/2026-10-25-delta-dataframe-merge/`
with per-task/per-attempt directories. Preserve original commands, exits, XML,
source/package hashes and reviewer outcomes. Do not rewrite earlier milestone
ledgers. Read private fixture paths from retained local evidence; never put secret
values in this plan or terminal output.

Installed test template, from the existing worktree (create the XML parent first):

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew -I delta/acceptance/cdf_merge_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestGravitinoDeltaMergeBuilder' \
  --tests '*TestDeltaRangerInstalledMerge' \
  --tests '*TestDeltaRangerMergeOwnership' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PcdfLiveXml="$PWD/.superpowers/sdd/2026-10-25-delta-dataframe-merge/task-1/3.5.4-3.3.0/attempt-1/xml" \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

Change only pins, selectors and fresh evidence directory for subsequent required
runs. Expected: exit 0, nonzero intended test count, no failures/errors/skips in the
selected qualification cases, and original loaded-version evidence. Offline cache
failure is not a product test result; resolve missing declared dependencies before
retrying. Do not mark expected RED runs as GREEN.

Module formatting: `./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply`
then `:spark-connector:spark-delta-ranger-3.5:spotlessCheck`, with the same JDK/pins.
Use `git diff --check`. Public API documentation and Java Javadoc checks apply to
any Java files introduced; use normal imports and the repository's style.

After Task 2 creates the commands, use the pinned runtime's Python interpreter for:

```bash
python -m unittest discover -s spark-connector/v3.5/spark-delta-ranger/python/tests -v
python -m unittest discover -s delta/acceptance -p 'test_cdf_merge*checks.py' -v
```

Build the zip with `:spark-connector:spark-delta-ranger-3.5:deltaPythonZip`.
Existing package command remains `python delta/acceptance/cdf_read_live_package.py
--app merge --base <verified-base-manifest> --out <fresh-package-directory>
--spark <pin> --delta <pin>`, extended to bind the zip. New live command is
`python delta/acceptance/cdf_merge_live.py --frontend dataframe --partitioned
--runtime <new-manifest> --inputs <private-inputs> --out <fresh-run-directory>`.
Resolve angle-bracket arguments from verified local inputs and record the exact
executed command privately. Expected: original outer/Python/native exits all zero,
strict checker passes, and five explicitly recorded live phases per final pair.

## Plan self-review and handoff

Spec mapping: public API/expression identity/context → Task 1; Python/package →
Task 2; original policy, partition and retry/recovery semantics → Task 3; all-pair
installed/live evidence and published limits → Task 4. Each Review Focus item has
a named owning test step. Java-map overloads avoid generic erasure conflicts;
Python None uses explicit no-condition overloads. Existing SQL and native helpers
remain the shared authority/behavior reference, not duplicated implementations.

Implementation method is already selected: subagent-driven, one task and fresh
review at a time. Tasks 1–2 are closed; Task 3 is the next implementation task. Broader Spark
execution and catalog-demo integration are not subsequent tasks in this plan.
