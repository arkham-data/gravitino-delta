# Spark Delta production runtime — Task 1 implementation report

Implementation source commit: `98914afe5d64073ce3a11c19e020d000217b3f83`.

Base: `75619a222090cb45ed2e975676ddd4ec587e7f09`. Worktree: `/private/tmp/gravitino-delta-feasibility`, branch `codex/delta-spark-feasibility`. Provider prerequisite was independently accepted before dispatch (`b0fb125a0` plus `1a9c3184f`). This report records implementer verification; independent Task 1 review remains the root agent's responsibility.

Only `settings.gradle.kts`, the new `spark-connector/v3.5/spark-delta` module, and this report were committed. No core/API/server/client/storage-schema/provider sources, runtime discovery, runtime assembly, services, Ranger configuration, or demo files changed. No subagents, push, merge, PR, or live-service operations were used.

## Public API and deployment configuration

The optional project is `:spark-connector:spark-delta-3.5`. Its catalog implementation is `org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35`, implementing `StagingTableCatalog`, `SupportsNamespaces`, and `AutoCloseable`.

The adapter requires an active, correctly configured Spark session at initialization:

- `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`
- `spark.sql.extensions` includes `io.delta.sql.DeltaSparkSessionExtension`
- `spark.databricks.delta.catalog.update.enabled=false`
- Per-catalog option `journal-directory`, exposed by `GravitinoDeltaCatalogSpark35.JOURNAL_DIRECTORY`. In Spark configuration its spelling is `spark.sql.catalog.<catalog>.journal-directory`.

The journal directory must be an explicit driver-owned local directory on a filesystem supporting POSIX permissions, atomic replacement, and file/directory fsync. Its parent must already exist. A missing directory is created with mode `0700`; an existing nonprivate directory or directory symlink is rejected. Native required-session-configuration checks remain enabled. This module does not initialize the Gravitino catalog manager or provide runtime discovery; those belong to subsequent runtime integration.

The no-argument catalog constructor resolves both metadata and mutation catalogs through a supplier calling `GravitinoCatalogManager.get().getGravitinoCatalogInfo(name)` at operation time. It does not retain a first-user client or close manager-owned clients. The public two-supplier constructor, `GravitinoDeltaCatalogSpark35(Supplier<Catalog> metadata, Supplier<Catalog> actor)`, exposes the distinct metadata seam. `DeltaRegistrationClient` also accepts those two suppliers. A later read-only metadata credential must remain confined to the metadata supplier; it must not replace the mutation actor or change the query subject. Existing manager authentication semantics are preserved.

Production build properties are independent of fixture settings: `deltaSparkVersion` defaults to `3.5.4` and admits `3.5.4`/`3.5.6`; `deltaVersion` defaults to `3.3.0` and admits `3.3.0`/`3.3.2`. Other values fail configuration. The binary Scala selection must be `2.12`; resolved Scala library/reflect versions converge to `2.12.21`. All `org.apache.spark` dependencies in the module configurations converge to the selected production Spark patch. No `deltaTestSparkVersion` or `deltaTestVersion` setting is used.

## Registration, native staging, and recovery behavior

Staged getters expose defensive declared descriptor values and do not contact Gravitino, write journals, construct native stages, or mutate native data. CREATE exposes `V1_BATCH_WRITE`; REPLACE and CREATE_OR_REPLACE expose `V1_BATCH_WRITE` plus `TRUNCATE` both before and after materialization. They do not advertise general predicate overwrite.

Each staged operation generates three distinct UUID strings when its pending operation object is created: a local journal key, an operation token, and a writer run. The run is an adapter-generated operation correlation value. It is **not** Spark's `queryId`, an application/job ID, a caller-provided configuration option, a fencing token, or proof that a writer has stopped. The same original command/token/run survives request uncertainty; no registration ID is invented when the reservation response is lost. Deployment/operator orchestration must associate the operation with the real driver/job when confirming completion or drain.

The first write builder selects QUERY and binds `LogicalWriteInfo.schema().asNullable()` using Spark's recursive normalization. Untouched staged commit selects SCHEMA_ONLY and retains the declared schema. Repeated compatible builders reuse the existing builder; conflicting schema/options are rejected. Explicit `delta.columnMapping.mode` overrides the session default `spark.databricks.delta.properties.defaults.columnMapping.mode`. `none` selects the exact v1 command; `name` selects `DeltaSchemaContract.spark35Delta33NameMapping` with the selected operation mode. The effective mapping setting is installed on the native properties so a later default does not silently rebind it. There is no postcommit schema rebinding.

Before reservation, admission rejects caller-selected locations/paths, registration/marker properties, internal mapping IDs, excluded engine schema metadata, conflicting schema write options, option-wrapped metadata overrides, and per-request Hadoop/S3/storage-routing overrides. Generated/default/identity metadata requires separate conformance and is rejected by this adapter's admission path rather than stripped silently.

A fresh published provider lookup chooses authoritative CREATE versus replacement. Existing CREATE conflicts and missing REPLACE fails before reservation. Existing replacement also validates the current native head before requesting BEGIN_REPLACE. `DeltaRegistrationClient` sends only the typed production command through ordinary public `TableCatalog.createTable`/`alterTable` APIs. It validates receipt token/run/operation/schema bindings and expected registration identity/generation transitions. Pending native location and marker come from the validated server marker. The actual replacement base UUID/version is checked again before native construction.

The native `DeltaCatalog` receives a complete table/namespace delegate through `setDelegateCatalog` before native calls. Its path probe throws Spark's expected `NoSuchTableException`, allowing native path fallback. No secondary session-catalog table is registered. All CREATE/REPLACE/COR staging and outer ALTER calls use native path identifiers.

QUERY CREATE_OR_REPLACE always invokes native `stageCreateOrReplace`, including authoritative absence, with the deferred replacement/truncate wrapper. SCHEMA_ONLY COR selects native `stageCreate` for absence or `stageReplace` for an existing published table. Native write results and native transactions remain native. A thrown native commit is treated as potentially committed and cannot be automatically replayed or followed by automatic publication on a later commit callback.

Publication follows successful native commit. It has its own durable typed PUBLISH intent before the transport call, in addition to the original reservation evidence. Postcommit publication failure retains the native data and bound receipt and prevents automatic publication replay. Local Spark abort invokes the native stage abort only where appropriate, preserves the existing journal phase/evidence, and never issues privileged registration ABORT or physical deletion.

`DeltaRegistrationRecovery` exposes `status(Entry)`, `execute(Entry, DeltaRegistrationCommand, boolean requestDrained)`, and `administrativeStatus(Identifier)`. `DeltaRegistrationJournal.read(Path)` loads an entry; entries expose command, phase, optional receipt, canonical catalog, namespace, table, and local key. STATUS after lost CREATE uses `statusByToken(token, writerRunId)` when allocation is unknown. Lost BEGIN_REPLACE uses STATUS against the original expected ID with the next generation and replacement token. STATUS records the observed receipt while retaining `STATUS_NEEDED`; it does not prove original request completion. Reread the immutable journal entry after STATUS before using its newly learned receipt. Explicit HANDOFF/PUBLISH/ABORT requires a matching expected identity and independent caller confirmation of request drain. HANDOFF must identify the recorded writer run and uses an externally supplied reference. The helper does not fabricate stop proof or bypass existing REST authorization/recovery gates.

## Journal format and durability

Files are `<journal-key-uuid>.json`. The envelope contains version, key, canonical catalog, namespace, table, the exact typed command object, phase, and an optional sanitized typed receipt property map. It never stores the whole Spark table-property/write-option map or client credentials. The original RESERVE/BEGIN_REPLACE command is retained independently from a subsequent PUBLISH intent.

Writes encode at most 1 MiB, create a `0600` temporary file in the same private directory, write through a file channel, fsync the file, atomically replace the final file, and fsync the directory. Directory initialization also checks driver ownership and fsyncs its parent. Failures are fail-closed: intent-journal failure prevents the request; receipt-journal failure prevents native stage construction. Temporary cleanup does not delete pending final evidence.

Reads are bounded to 1 MiB, reject symlink files, unknown envelope fields, duplicate JSON fields, malformed typed commands/receipts, invalid keys, and trailing JSON content. `STATUS_NEEDED` explicitly denotes completion uncertainty. A pre-request INTENT found after a crash also requires reconciliation because the request may have been sent after that write. `NATIVE_STARTED` likewise allows an uncertain native outcome. `NATIVE_COMMITTED` records a successfully returned native commit; `PUBLISHED` requires a matching published provider receipt.

## Named identity, namespaces, and native metadata evolution

Each named lookup refreshes published metadata. Current native metadata ID and a nonregressing head version are checked with an ordinary unversioned native handle. Replacement requires exact equality with the server-supplied actual base UUID/version. Version/timestamp selection then uses the native historical overload independently of the current-head check.

Returned tables remain actual `DeltaTableV2` instances. A native copy attaches an external, nonpersisted `CatalogTable` descriptor carrying canonical Gravitino catalog/database/table identity, native location, schema, and partition columns while retaining native options and time-travel state. The descriptor reaches the native `LogicalRelation`. It does not change the query subject or install a session-catalog entry.

The descriptor activates native catalog-aware hook paths, so initialization, load, stage, and staged commit validate the false update setting. This remains a deployment invariant through native commit and asynchronous hooks; entry checks cannot make mutable SQLConf globally immutable or protect arbitrary later mutation/cached plans. Tests use a real colliding session-catalog sentinel and wait for `UpdateCatalog.awaitCompletion` before asserting the sentinel metadata/data remain unchanged.

One-level namespaces use the separate current metadata/mutation suppliers. Comment changes use the provider's actual `SchemaChange.setProperty("comment", value)` / remove-property transport. Blank, masked-placeholder, raw secret-URN, and unsupported property changes are rejected before mutation. Cascade/nonempty behavior is delegated to provider policy, including its retained hidden-row rules. Registration DROP/PURGE/RENAME fail explicitly. Cache invalidation calls native invalidation and invalidates the native DeltaLog cache.

Outer ALTER validates the entire admitted change list before the first native operation, resolves the current authorized location, invokes native path ALTER, then reloads through the metadata seam. Native ALTER can commit grouped changes separately; this implementation does not promise all-or-nothing atomicity for an admitted list. The native test exercises ordinary schema refresh and RESTORE within the current replacement lineage. It does not qualify restoration across an older replacement/native-ID lineage.

## Verification and original evidence

Evidence root: `/private/tmp/delta-spark-adapter-task1-evidence`. All commands ran in the task worktree with `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`. Final gates were sequential, with each pin's XML and jar copied before another test invocation. Source was frozen after the last comment/import cleanup.

| Gate | Executed tests | Failures/errors/skips | Evidence |
| --- | ---: | --- | --- |
| Spark 3.5.4 / Delta 3.3.0 final confirmation | 43 | 0 / 0 / 0 | `final-baseline-confirmation.log`, `final-baseline-confirmed-xml/` |
| Spark 3.5.6 / Delta 3.3.2 | 43 | 0 / 0 / 0 | `final-compatibility.log`, `final-compatibility-xml/` |
| Baseline strict Javadoc/format/jar/classpath | n/a | passed | `final-baseline-artifacts.log` |
| Compatibility strict Javadoc/format/jar/classpath | n/a | passed | `final-compatibility.log` |
| Jar/classpath/bytecode/XML inspection | n/a | passed | `inspect_artifacts.py`, `artifact-verification.json` |

Each pin executes nine suites: catalog orchestration 10, lazy publication stage 8, native adapter 6, recovery 5, configuration admission 4, namespaces 4, registration client 2, journal 2, and native identity 2. The six native cases exercise named INSERT/UPDATE/DELETE/MERGE and the colliding sentinel, CTAS/RTAS/query COR, schema-only COR, v2 mapping and explicit-none precedence, version/timestamp history with current-head rejection, and path ALTER/RESTORE admission/refresh. They use constructor-injected ordinary API mocks and task-owned local Delta paths. A test-only Spark class-instantiation bridge supplies those injected fixtures; production has no static server callbacks or provider/Kernel fixture dependencies.

Exact final commands (all prefixed with the JDK17 JAVA_HOME above):

```bash
./gradlew :spotlessKotlinGradleApply :spark-connector:spark-delta-3.5:spotlessApply :spark-connector:spark-delta-3.5:test :spark-connector:spark-delta-3.5:javadoc :spark-connector:spark-delta-3.5:jar -PskipITs
./gradlew :spark-connector:spark-delta-3.5:spotlessApply :spark-connector:spark-delta-3.5:spotlessCheck :spark-connector:spark-delta-3.5:javadoc :spark-connector:spark-delta-3.5:jar :spark-connector:spark-delta-3.5:task1Classpath -I /private/tmp/delta-spark-adapter-task1-evidence/classpath.init.gradle -PskipITs
./gradlew :spark-connector:spark-delta-3.5:test -PskipITs
./gradlew :spark-connector:spark-delta-3.5:test :spark-connector:spark-delta-3.5:javadoc :spark-connector:spark-delta-3.5:spotlessCheck :spark-connector:spark-delta-3.5:jar :spark-connector:spark-delta-3.5:task1Classpath -I /private/tmp/delta-spark-adapter-task1-evidence/classpath.init.gradle -PskipITs -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2
python3 /private/tmp/delta-spark-adapter-task1-evidence/inspect_artifacts.py
git diff --check
git diff --cached --check
```

The temporary init script only prints resolved artifact coordinates for compile/runtime/test runtime configurations; it is evidence tooling, not module production code. Both pins resolve 149 compile artifacts, 4 module runtime artifacts, and 165 test runtime artifacts. The module runtime closure is delta-common plus Jackson annotations/core/databind 2.15.2. Native Spark/Delta/Scala/Hadoop dependencies are provided and unrelocated. Compile/test classpaths contain only Gravitino api, delta-common, and spark-common project artifacts; no provider, server, core, Kernel, embedded server, or registration-test artifacts were present.

The thin `gravitino-spark-delta-3.5-2.0.0-SNAPSHOT.jar` contains 20 adapter classes, all class-file major version 52 (Java 8), with no native or test classes. Both tested pins produced the same jar SHA-256: `68e6c536533efdf8ea9a5a7c779e1112bdf606ae4f7b84b4641e16502b1a7407`. Preserved jars are in `final-baseline-confirmed-artifacts/` and `final-compatibility-artifacts/`. The Java helper uses native Scala 2.12 case-class APIs directly; no additional Scala helper source was necessary.

Behavioral RED history is retained separately from scaffolding/compiler failures:

| Behavioral RED | Tests/failures | Original XML |
| --- | --- | --- |
| `red-all-initial.log` | 13 / 13 | `red-initial-xml/` |
| `red-catalog.log` | 6 / 6 | `red-catalog-xml/` |
| `red-recovery.log` | 10 / 4 | unavailable; see note below |
| `red-hardening.log` | 26 / 4 | `red-hardening-xml/` |
| `red-publication-intent.log` | 1 / 1 | `red-publication-intent-xml/` |
| `red-namespace-final.log` | 6 / 1 | `red-namespace-xml/` |

The recovery RED log is genuine, but its XML copy was made after a local native test had overwritten Gradle's XML output. The mistakenly labeled directory was renamed `native-before-recovery-xml/`; it contains native green results and is **not** recovery RED evidence. No matching recovery RED XML was reconstructed or fabricated. The later stable-source final XML for both pins was preserved correctly. Earlier compile/style/Javadoc and native-fixture initialization failures remain in their logs and are not counted as behavioral RED. Already-correct follow-up cases were recorded as additional coverage without artificial failure injection.

Bounded self-review corrected ambiguous native-commit replay, option-wrapped mapping overrides, internal schema metadata admission, missing typed PUBLISH intent, namespace comment admission, strict public-phase Javadoc, and avoidable FQNs before the final source-freeze gates.

## Remaining qualification limits

This task provides a thin optional adapter and focused local evidence. It does not establish fullDelta, all four Spark/Delta patch combinations, packaged-runtime discovery/assembly, production S3/provider coupling, Ranger policy download/enforcement, or demo acceptance. The two cross patch pairs were not executed. Native mocked metadata transport is not a substitute for the accepted server/provider tests or later packaged acceptance.

Deployment must keep engine/provider storage routing aligned, keep catalog updates false through native hooks, and enforce one writer job per dataset. Native IDs detect wrong-table mismatches, not cloned backends with identical IDs or malicious storage configuration. Tokens, markers, receipts, and generated runs do not provide authorization, fencing, or a complete query/data digest. Generated/default/identity contracts and additional lifecycle operations remain separate qualification work. No physical cleanup or privileged recovery is automatic.

No callable claude-mem search/save tool was available in the dispatched agent's tool set; the root agent was notified. Findings are persisted in this report and the evidence directory rather than claiming an unavailable memory write.

## Fix round 1: R1 and R2

Base: `94037c4adb0421fdfbec8adbb62cff944f8df2c3`. Source fix commit: `b425df8ca8c288611810f39e0cd43118c4f3b9f9`. Both accepted Important findings are addressed; scoped independent rereview remains the root agent's next step.

R1 now admits `AddColumn.dataType()` and `UpdateColumnType.newDataType()` through the same recursive metadata policy used for staged schemas. The policy accepts any Spark `DataType` and inspects nested structs under arrays/maps. Validation remains inside the complete ALTER admission pass, before provider resolution and native dispatch. Regressions put a valid property change before each prohibited nested type and assert that neither provider lookup nor native ALTER occurs. Admitted native grouped-commit behavior is unchanged.

R2 introduces one shared semantic command/receipt binding check. Journal decoding applies it to the independently parsed typed pair. STATUS validates an existing pair before transport and binds the observed receipt to the original durable command before writing it. Operator execution validates constructor-created entries before journaling an intent or contacting the actor. The check enforces applicable token, writer run, expected identity, opening operation, initial generation or replacement generation/token, and pending exact schema or v2 required schema/operation mode. Immediate native reservations retain their existing pending-state requirement. Recovery accepts matching PUBLISHED progression without a pending marker and preserves typed PUBLISH/HANDOFF/ABORT entries and status recovery. No wire, provider, journal envelope, build dependency, or public API changes were made.

Evidence root for this round: `/private/tmp/delta-spark-adapter-task1-evidence/task1-fix1`.

| Gate | Tests | Failures / errors / skips | Evidence |
| --- | ---: | --- | --- |
| R1 genuine behavioral RED | 12 | 2 / 0 / 0 | `red-r1.log`, `red-r1-xml/` |
| R1 focused GREEN | 12 | 0 / 0 / 0 | `green-r1.log`, `green-r1-xml/` |
| R2 genuine behavioral RED | 41 | 30 / 0 / 0 | `red-r2.log`, `red-r2-xml/` |
| R2 focused GREEN with client/recovery regressions | 48 | 0 / 0 / 0 | `green-r2.log`, `green-r2-xml/` |
| Spark 3.5.4 / Delta 3.3.0 frozen-source full adapter suite | 86 | 0 / 0 / 0 | `final-baseline.log`, `final-baseline-xml/` |
| Spark 3.5.6 / Delta 3.3.2 frozen-source full adapter suite | 86 | 0 / 0 / 0 | `final-compatibility.log`, `final-compatibility-xml/` |

All RED XML was copied immediately after the failed invocation, before another test run. Eleven R2 cases were already correct or positive progression cases and passed during RED; no artificial failure was introduced for them. The 41 new binding cases exercise schema/mode/opening identity mismatches through STATUS, raw journal decode, and constructor-created operator entries, plus existing-pair preflight, four published opening/progression variants, and the three typed operator recovery variants. Each final run executed ten suites, including the same six native cases from Task1.

All commands used the JDK17 JAVA_HOME recorded above. After focused GREEN and final formatting, source stayed frozen through these sequential gates:

```bash
./gradlew :spark-connector:spark-delta-3.5:test :spark-connector:spark-delta-3.5:javadoc :spark-connector:spark-delta-3.5:spotlessCheck :spark-connector:spark-delta-3.5:jar -PskipITs
./gradlew :spark-connector:spark-delta-3.5:test :spark-connector:spark-delta-3.5:javadoc :spark-connector:spark-delta-3.5:spotlessCheck :spark-connector:spark-delta-3.5:jar -PskipITs -PdeltaSparkVersion=3.5.6 -PdeltaVersion=3.3.2
python3 /private/tmp/delta-spark-adapter-task1-evidence/task1-fix1/inspect_artifacts.py
git diff --check
git diff --cached --check
```

Strict Javadoc, formatting, and jar gates passed on both pins. Per-pin jars were copied to `final-baseline-artifacts/` and `final-compatibility-artifacts/` before another invocation. Each contains only 20 adapter classes, all major version 52, with no native or test classes. Both SHA-256 hashes are `2cc281758d658b68f074867a70b12ceea46fd791b0faea2b62986c66823f27d9`. `artifact-verification.json` records the source commit, original XML totals, class inspection, hashes, and verification of all 22 frozen source/build files. The initial manifest also captured transient Spotless scratch files; its original contents and correction are retained in `frozen-source-and-spotless-sha256.json` and `manifest-note.txt`. The final source manifest filters those original digests, without recomputing them after the gates.

Bounded self-review covered only the R1/R2 diff and its regression matrix. No new known concern remains within this fix scope. Earlier qualification limits remain in force: these adapter tests do not establish fullDelta, the two cross patch pairs, packaged production/runtime discovery, S3/provider coupling, Ranger, or demo acceptance. No live services, privileged recovery, or physical cleanup were performed.
