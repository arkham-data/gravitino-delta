# Actual Delta V1 writer input

This boundary validates the actual Dataset that Spark constructs for a named
Delta append, then denies unconditionally before writing. It does not enable
successful append. The four-pair compatibility matrix passed on 2026-10-13.

The [implementation plan](superpowers/plans/2026-10-13-delta-append-v1-input.md)
continues the [command-owned idempotency checkpoint](delta-append-idempotency.md).
That earlier scalar phase remains separate; reaching a writer input does not
compose it into an append coordinator.

The [transaction-owned source continuation](delta-append-transaction-source.md)
connects these boundaries. The results below describe this historical input-only
checkpoint.

## Validated boundary

For a matching-schema, already authorized source, named `insertInto` and
existing-table `saveAsTable` append preserves original source SELECT
obligations and the exact Gravitino-registered target's UPDATE authority through
Spark's final analysis, optimization and V1 fallback. Spark executes the stored
`analyzedQuery`, which is checked independently of the optimized command
child.

The companion's planner validates the command and retains the actual native V1
leaf, replacing its opaque writer with a terminal writer that cannot delegate.
The execution owner covers the entire native leaf run, including failure
while Spark constructs the Dataset before calling `insert`. At input arrival,
validation checks the real Dataset and the retained source/target authority,
then denies unconditionally. No transaction, helper, source execution, file
generation, reset or commit is admitted by this boundary.

Capturing the optimizer's opaque writer does not authenticate its builder origin.
The writer remains inert. A future successful coordinator must own construction
of its native helper from approved inputs; it cannot treat the captured object
as permission to write.

## Qualification

The frozen matrix passed all four required version pairs: **134 tests in nine
suites per pair, 536 test executions total**, with zero failures, errors or skips.
The new input suite contributes 16 tests and 39 fixtures per pair.

| Spark | Delta | Tests | Fixtures | Verified storage entries |
| --- | --- | ---: | ---: | ---: |
| 3.5.4 | 3.3.0 | 134 | 227 | 17,720 |
| 3.5.4 | 3.3.2 | 134 | 227 | 17,720 |
| 3.5.6 | 3.3.0 | 134 | 227 | 17,720 |
| 3.5.6 | 3.3.2 | 134 | 227 | 17,720 |

Each cell ran scoped `spotlessCheck` and fresh tests, with test-task cache and
up-to-date reuse disabled. The selectors cover AppendV1Input, AppendCommandBinding,
AppendIdempotency, InstalledAppendBoundaries, AppendTargetReceipt,
PhysicalAdmission, ReadRule, DecisionReceipt and NativeIdempotency. This is bounded
regression coverage, not a full repository test run.

All cells used the same 95-file source/build/tooling freeze, JDK 17.0.16 and Scala
2.12.21. Each actual test worker's 207 ordered classpath entries were archived,
including six Ranger **2.8.0** artifacts: plugins-common, audit-core, plugins-cred,
plugin-classloader, ugsync-util and authz-api. The archive also retains the actual
worker command, attached agent, sources, classes, logs and fresh JUnit XML.

The storage audit checked 908 fixture archives and 70,880 manifest entries across
the matrix, including before/after and per-attempt comparisons. All 39 new input
fixtures per cell also passed directory-object inventory comparisons. Independent
phase checks cover original source occurrences and obligations, target UPDATE,
actual native input entry, specific mutation/frontier denials and owner cleanup.
Source hashes remained unchanged before and after every cell.

Both real named frontends reach
`Delta append input validated; writes remain disabled`. Evidence combines the
failed command's QueryExecution, native SupportsV1Write/V1FallbackWriters terminal
stack and production validation of the actual Dataset. It does not directly
capture the Dataset local variable through the external listener, or provide
global I/O/native-call interception.

Private local evidence is retained under
`.superpowers/sdd/2026-10-13-delta-append-v1-input/`: `frozen-task-1`, the four sealed
`matrix-1` cells and adjacent audit reports, and `root-verification.json` with
aggregate counts and archive-manifest digests. Exact archive inventories and
hashes are verified; local read-only sealing is not tamper-proof storage.

Local fixture policy transport does not qualify deployed Ranger or S3 behavior.
This phase adds no live acceptance claim to the separate earlier helper evidence.

## Current limits

The initial input contract requires matching source/target types, names, nullability
and column order. Native coercion, renaming and reordering remain rejected, as do repeated
append attempts using the same retained source owner. These are temporary limits
of this finite phase, not full Delta compatibility. The qualified-inventory
frontier also covers `writeTo`, SQL INSERT, raw paths, overwrite, CTAS/RTAS and
streaming. Qualified `writeTo` rejects with missing receipt; it does not reach
the terminal input boundary. The streaming test uses a real rate source and
rejects at source admission; it does not start a streaming writer. Each route
records its exact companion rejection. Older tests using an unsupported observer
extension establish only inventory rejection.

Rule inventories are pinned. Unknown rules cannot issue append authority. An
unsupported programmatic resolution rule ordered before the companion may already
have run before the companion detects it; the extension is not an arbitrary JVM
code sandbox.

Lifecycle tests explicitly inject an internal analyzed-flag fault into the actual
retained fallback plan to force a failure during native Dataset construction.
Spark normally reuses the already analyzed plan. The test verifies the injected exception identity, native construction stack,
owner expiry and cleanup. It is fault injection, not evidence of an ordinary
metadata-refresh failure.

Fixture storage snapshots begin after Spark warehouse initialization. All regular
files are archived and hashed; the new input cases also compare directory-object
inventories. These measurements do not claim that Spark creates no directories
during initial session setup.

## Remaining integration

The next coordinator step connects this actual V1 input, native idempotency and
source preparation under one owned target transaction. Same-target sources must
retain the native transaction generator and its read tracking. The captured
opaque writer remains inert throughout.

Successful append still needs transaction-owned source preparation, native helper
plans and action generation, precommit revalidation, native commit and postcommit
handling, transaction-version reset, broader frontend/options support and public
Gravitino MODIFY grant propagation through Ranger 2.8.0 to S3. These requirements
remain in the [governed append design](superpowers/specs/2026-09-27-delta-governed-append-design.md).

Full Delta support remains unfinished. CDF is the first feature priority after
Delta maturity. This work preserves Gravitino core/API/server/client/storage
schemas and remains local.
