# Transaction-owned Delta append source

The four-pair compatibility matrix passed on 2026-10-14. This checkpoint enables
transaction-owned source preparation, not successful append.

The finite coordinator connects the actual V1 append input, native idempotency and
source preparation inside one target transaction. It still denies before source
execution, the native write helper, file generation or commit. This continues the
[actual V1 input checkpoint](delta-append-v1-input.md) and the [governed append
design](superpowers/specs/2026-09-27-delta-governed-append-design.md).

## Boundary

Matching-schema named insertInto and existing-table saveAsTable retain the actual
Dataset and QueryExecution received by Spark's V1 fallback. The original command
association retains all source SELECT obligations and target UPDATE authority. Source
occurrences remain distinct even for self-joins and optimized-empty inputs.

The association opens one native target transaction and reuses the existing native
idempotency scalar. A nonduplicate prepares the actual retained QueryExecution inside
that transaction. A duplicate skips preparation, revalidates original source and target
permissions, and also denies; this checkpoint does not report successful duplicate
append.

The preparation claim requires the retained planning tracker, original receipt recipe
and exact active transaction. The actual optimized return must carry the exact newly
issued Prepared evidence. A planning tracker is publicly shareable; its identity alone
is not permission. Prepared append-source evidence is explicitly non-executable and
expires with its input and transaction owners.

For same-log source scans, the coordinator preserves Delta's original transaction
generator so native predicate/file conflict tracking remains active. The no-skipping
path retains native whole-table tracking. Cross-log sources retain their native Snapshot
selection. Repeated authorization occurrences are checked independently of native
scan-cache reuse and deduplicated read-file sets. Distinct-object same-path aliases
retain the existing exact-log rejection boundary.

## Qualification

All four pairs passed **93 tests in nine suites per pair: 372 test executions**, with
zero failures, errors or skips. The new transaction-source suite contributes 14 tests
and 46 fixtures per pair.

| Spark | Delta | Tests | Fixture archives | Manifest entries |
| --- | --- | ---: | ---: | ---: |
| 3.5.4 | 3.3.0 | 93 | 157 | 12,238 |
| 3.5.4 | 3.3.2 | 93 | 157 | 12,238 |
| 3.5.6 | 3.3.0 | 93 | 157 | 12,238 |
| 3.5.6 | 3.3.2 | 93 | 157 | 12,238 |

The selectors cover the complete AppendTransactionSource, AppendV1Input,
AppendCommandBinding, ReadRule, NoSkipping and PhysicalAdmission suites, plus directly
affected methods in AppendIdempotency, NativeIdempotency and NativeCacheCapture. This is
scoped regression coverage, not a full repository test run. Each cell passed scoped
spotlessCheck and fresh tests with test-task cache and up-to-date reuse disabled.

All cells used the same 98-file source/build/tooling freeze. Actual workers used JDK
17.0.16, Scala 2.12.21 and 207 ordered classpath entries, including all six Ranger
**2.8.0** artifacts: plugins-common, audit-core, plugins-cred, plugin-classloader,
ugsync-util and authz-api. Worker commands, attached JaCoCo agent, classpaths,
source/classes, fresh XML and logs were archived before the next cell rebuilt.

Independent audits verified 628 fixture archives, 48,952 manifest entries and 764
storage comparisons across the matrix. All 184 new fixture executions also passed
directory-object comparisons and exact phase checks. These include native
predicate/file/whole-table read changes, original source and target obligations,
duplicate no-preparation, cached-live-QE rejection, actual retained Dataset.collect
denial, specific mutation failures and cleanup. Source hashes matched before and after
every cell.

Local policy-transport fixtures do not qualify deployed Ranger or S3 behavior. There is
no new live acceptance claim in this checkpoint.

Private evidence is retained under
`.superpowers/sdd/2026-10-14-delta-append-transaction-source/`: `frozen-task-2`, all
four sealed `matrix-2` cells and adjacent audits, and `root-verification.json` with
aggregate counts and archive-manifest digests. Local read-only sealing is not
tamper-proof storage. `matrix-1` was stopped and excluded after an audit script changed
following freeze; the corrected tooling received independent review before a new freeze
and complete restart.

## Evidence limits

Retained Dataset/QueryExecution state, callback stacks and production identity checks
establish the actual-input path. They are not an independent external capture of the
local Dataset parameter. Generator branch checks and native read-state changes establish
the bounded native behavior; the tests do not directly intercept the generator's local
return variable. Counters cover their instrumented callbacks, not every I/O or native
method globally.

The first development RED retains its exact assertion log, but its XML was inadvertently
overwritten before archival. It is not counted as a passing result. Later development
and all qualified matrix XML are preserved. Existing Gradle, worktree Git lookup, Scala
type-test and Hadoop Java-fallback warnings are recorded; the runs are not described as
warning-free.

The test accessors read native transaction state without invoking native read methods to
create evidence. Storage comparisons include regular-file manifests and object
inventories around attempts. These checks complement the source-path review that the
finite coordinator has no source-execution, helper, reset or commit continuation.

## Remaining integration

Successful append still needs the native helper's generated plans/actions, constraints
and files, precommit revalidation, commit/postcommit behavior, transaction-version
reset, broader frontend/options support and deployed public Gravitino MODIFY grant
propagation through Ranger 2.8.0 to S3. The opaque writer captured by the V1 boundary
remains inert and is not builder provenance.

The current finite contract retains matching names, types, nullability and order, empty
explicit writer options and reset=false. This checkpoint is not full Delta support. CDF
remains the first feature priority after Delta maturity. Changes stay in optional
integration modules; Gravitino core/API/server/client/storage schemas remain unchanged.
