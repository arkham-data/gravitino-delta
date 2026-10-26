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

# Packaged Delta runtime acceptance

This opt-in harness starts an owned packaged Gravitino server and real `spark-submit`
processes against two existing, independently owned S3 endpoints. It does not start,
stop, or clean up containers. Every run needs a **new output directory** and unique
S3 prefix. Native objects and original failed process logs are retained.

```bash
python3 delta/acceptance/run.py --inputs /private/path/inputs.json --output /private/path/new-run
python3 -m unittest discover -s delta/acceptance -v
```

The private JSON input has this shape (placeholders are not usable credentials):

```json
{
  "javaHome": "/path/to/jdk17",
  "sparkEnvironmentsFile": "/private/path/spark-environments.json",
  "serverKernelVersion": "3.3.2",
  "prefix": "unique-owned-acceptance-prefix",
  "serviceAdmin": "owned-local-admin",
  "primary": {
    "endpoint": "http://127.0.0.1:9000",
    "region": "us-east-1",
    "bucket": "owned-bucket",
    "credentialsFile": "/private/path/primary.env"
  },
  "secondary": {
    "endpoint": "http://127.0.0.1:9001",
    "region": "us-east-1",
    "bucket": "owned-bucket",
    "credentialsFile": "/private/path/secondary.env"
  }
}
```

Credential files contain `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` assignments.
They are parsed as data, never sourced by a shell. Output directories are private;
Spark properties and H2 state contain local credentials. Do not commit them.
Local simple authentication identifies test actors; this is not authentication
hardening or Ranger enforcement.

The Spark environments file contains `environments`, an array of exactly two
objects with `spark` (`3.5.4` or `3.5.6`), `spark_home`, `spark_submit`, and `venv`
(the Python 3.11 environment containing that distribution). Both distributions
must provide Hadoop 3.3.4 and Scala 2.12.18. The selected added jars are native
Delta Spark/storage 3.3.0 or 3.3.2, Hadoop AWS 3.3.4, its production-POM AWS SDK
bundle 1.12.262, and one common optional Gravitino runtime binary. Kernel 3.3.2
is supplied only by the isolated server provider package; its Hadoop is 3.3.6.

The focused preparation task copies the ordinary server `runtimeClasspath`,
server jar, production provider package, real launch scripts, logging template,
and shipped H2 schema scripts. It never invokes root `compileDistribution`,
fixture compilation, or test classpaths:

```bash
JAVA_HOME=/path/to/jdk17 ./gradlew -I delta/acceptance/prepare.gradle \
  prepareDeltaAcceptance -PdeltaAcceptanceOutput=/private/path/new-package \
  -PdeltaKernelVersion=3.3.2 -PdeltaVersion=3.3.0 -PdeltaSparkVersion=3.5.4 -PskipITs
```

For the second native package, `-PdeltaAcceptanceRuntime=/path/to/first/runtime.jar`
reuses the identical production runtime. The runner supplies this automatically.
The Hadoop AWS SDK version is taken from Hadoop 3.3.4's published production POM;
no fixture Hadoop or Scala jars replace distribution dependencies.

Development-only input options are `representativeOnly` (first pair), `processOnly`
(skip the writer matrix), `processStages` (a subset of `recovery`, `isolation`,
`authority`), `diagnosticOnly` (parser diagnostics with `processOnly`), and `packages`
(a mapping from each Delta version to an already prepared immutable package).
The first package's runtime is selected for every driver. These options narrow
execution and must not be described as a full matrix result.

The harness snapshots its own submitted sources, hashes actual driver and server
jars, records each child PID/exit, retains class-loading logs, and saves exact
object manifests and native operation actions. Matrix seed and writer JVMs are
separate; stopped-server, read-only H2 snapshots verify internal column IDs that
public `ColumnDTO` intentionally omits. SQL identifiers are individually quoted,
including the table named `history`, which is a Delta parser keyword.

Recovery uses the real journal, ordinary authenticated API, and an external
HTTP proxy. The adapter adds no application retry, but the underlying HTTP client
can retry PUT after connection loss. The proxy records every attempt. Reserve
response loss retains an original nonzero exit; publication blocking rejects all
PUT attempts until the original driver and proxy handlers drain. Only then does
an operator STATUS/reread/HANDOFF/PUBLISH or ABORT run. Completed publication loss
may recover through a transport PUT retry and a later STATUS-only driver. No
recovery step replays POST. Drain stops and joins the accept loop and waits for
all accepted connections, including partially parsed requests. A forwarded request
with unknown upstream completion prevents recovery even after its local handler
exits. Waiting for a timeout never establishes request drain.

Two same-session named catalogs use different prefixes on the primary endpoint.
Incompatible native Spark endpoint profiles run in separate drivers. Provider
isolation uses the same authority **and path** on different endpoints, simultaneous
loaded contexts, existing production cleanup logs, and a secondary read after
primary close completion. It does not claim session-global Spark routing isolation.

Native-ID quarantine appends a metadata-only identity fault to one sacrificial
owned table after its original driver exits; original bytes are archived and no
objects are deleted. It is external-log fault injection, not a successful native
lifecycle operation or observational adoption. Full Delta conformance, downstream
owner-hook fault injection, exact CAS barriers, Ranger 2.8 enforcement, and the
catalog demo retain their separately documented evidence and follow-up scopes.


## Focused replacement base admission

The review regression uses the same prepared production packages and launches only
replacement-admission cases, with a fresh owned server, catalogs, journal and S3
prefix. It archives original native objects before each excluded replacement.

```bash
python3 delta/acceptance/admission_run.py \
  --inputs /private/path/inputs.json \
  --packages /private/path/prepared-packages \
  --output /private/path/new-admission-run
```

`--packages` contains `package-3.3.0` and `package-3.3.2`, prepared by the existing
narrow task. The first package's one runtime is used for all four Spark/Delta pairs.
Each pair covers mapping-none bases with schema-only/query replacement and
explicit/session-default name mapping, direct production `BEGIN_REPLACE` via REST,
a native generated-column table admitted by observational adoption, and a supported
name-to-name query replacement. Excluded requests must preserve native head, UUID,
active rows, public table metadata and exact object manifests. The direct command
is encoded by the production Java API. Full internal row equality is separate
provider unit evidence; this process test compares the complete public table DTO.

`--red` selects one pair for the retained pre-fix schema-only reproducer using an
older immutable package. It expects the original nonzero publication failure after
native replacement; original data objects remain archived. Source snapshots,
package/driver hashes, native state, process exits and server class origins remain
in the private output. Spark's sanitized status-needed diagnostic is preserved;
the server log and direct REST response carry the shared admission reason. The
harness does not retry the rejected operation or run operator recovery.

## Governed batch CDF read qualification

The [governed CDF consumption result](../../docs/delta-cdf-consumption.md) covers
named DataFrame and SQL reads with explicit inclusive integer bounds. It qualifies
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 with JDK 17, Scala 2.12.21, actual Ranger
2.8.0 and MinIO. This is source-event consumption; destination MERGE/writebacks,
streaming and broader historical schemas/features remain subsequent work.

The lane reuses the retained shared production package and transport helpers from
the earlier acceptance milestones. The immutable base manifest, private current
service inputs, pinned stock Spark distributions, Gradle dependency cache, actual
Ranger installation proof and owned MinIO/Ranger services must already exist.
This is not a clean-checkout bootstrap or a cloud/distributed qualification.
Never print or commit private inputs, logs, H2 state or credentials. Preserve every
original failed fixture and diagnose it before creating another attempt.

Run the focused checker tests:

```bash
python3 -m unittest discover -s delta/acceptance -p 'test_cdf_read_live_checks.py'
```

Use JDK 17 and run the new/affected focused suites for each pair, substituting both
version properties. `cdf_read_live.gradle` pins Hadoop test dependencies, one fork
and a 4 GiB test JVM; the 1536 MiB value below belongs to Gradle. The retained Task 5
commands include the affected registration/read/mask/physical/policy tests and
representative append/ALTER regressions; the minimal CDF selectors are:

```bash
JAVA_HOME=/path/to/jdk17 ./gradlew -I delta/acceptance/cdf_read_live.gradle \
  :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerInstalledCdfRead' \
  --tests '*TestDeltaRangerCdfHistory' \
  --tests '*TestDeltaRangerCdfNativeParity' \
  --tests '*TestDeltaRangerCdfSql' \
  --tests '*TestDeltaRangerCdfReadPolicies' \
  --tests '*TestDeltaRangerCdfReadOwnership' \
  --tests '*TestDeltaRangerLiveCdfRead' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PskipITs -PskipDockerTests=false --offline --max-workers=2 --no-daemon \
  -Dorg.gradle.jvmargs=-Xmx1536m --console=plain
```

Package and execute each new pair sequentially, using fresh output directories:

```bash
python3 delta/acceptance/cdf_read_live_package.py \
  --base /private/path/verified-base/manifest.json \
  --out /private/path/new-runtime --spark 3.5.4 --delta 3.3.0
python3 delta/acceptance/cdf_read_live.py \
  --runtime /private/path/new-runtime/manifest.json \
  --inputs /private/path/current-inputs.private.json \
  --out /private/path/new-live-cell
```

Capture the original outer subprocess exit separately from the Spark child
`exit.json`. Acceptance requires outer 0, child 0, a checked `result.json`, no
`failure.json`, exact manifest/source/class/JAR binding and actual per-cell Ranger
association. The checker validates exact governed event multisets, runtime pins,
terminal coverage, distinct empty/denied/failed outcomes, and SELECT/download
witnesses for grant, revoke and newer regrant. A cell cannot borrow another pair's
fixture result.

The packager captures complete sources before/after Gradle, compiled class hashes,
all runtime artifact bytes and unique observed providers. Shared components need
complete unchanged production source and immutable artifact equality. A companion
may be reused only when both requested runtime pins and production inputs match;
otherwise Gradle resolves/builds its full shadow under the requested pins. Retain
the actual task outcome: cached/up-to-date output is not a newly executed assembly
and requires complete input/archive-entry equivalence proof for qualification. Application reuse requires equality
of every archive entry, including resources, against a fresh thin build. Manifests
and receipts distinguish fresh and reused provenance. Do not rewrite old captures
when only checker/documentation sources change: retain the historical receipt and
an explicit non-runtime delta plus full unchanged-component proof.

Full per-pair invocations, original outer/child receipts, XML, artifact hashes,
source differences and service proof are indexed in the retained
`.superpowers/sdd/2026-10-20-delta-cdf-consumption/task-5-report.md`. The original
accepted first cell is reused under that explicit proof; the other three have
requested-pin packages and original fresh live fixtures. Gradle UP-TO-DATE shadow
outputs are disclosed separately from explicit base-artifact reuse. Each changed
pair also has a forced shadow-only assembly with complete archive-entry equality
to its immutable live artifact; dependencies keep their normal Gradle checks. No closed append or
CDF enablement matrix is repeated.


## Governed CDF-to-MERGE local lane

`cdf_merge_live.py --runtime <manifest> --inputs <private-inputs> --out <fresh-directory>`
executes an isolated local cell using actual Ranger 2.8.0 and MinIO. Select the
explicit thin application with `cdf_read_live_package.py --app merge`; the existing
read application remains the default. The MERGE init retains the pinned memory
limits and writes requested compile/test classpath hashes. Changed-pair shadow
assembly runs explicitly; normal dependency/compiler cache outcomes stay visible.

The Python application calls Python `spark.sql` on canonical names using the same
installed JVM session and authenticated application UGI. The Scala application
exercises additional transaction/no-op/error and same-table phases. A separate
setup-identity native observer records raw rows, versions, CDC and original log
actions. It never supplies collected payloads to application MERGE. Public
SELECT/MODIFY transitions and completed plugin downloads are retained, including
column denial and a post-helper revoke gate. Original process exits, application
checkpoints, provider/source/JAR bindings and persisted registration restart are
required by the result contract.

### Earlier unpartitioned qualification

Fixtures and failures are retained; only exact owned processes are stopped. This
lane currently depends on preserved private local packaging/transport artifacts;
it is not a standalone clean-checkout or separate-worker deployment kit. The
four-pair local qualification passed on Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2,
with 13 original phases per pair and original outer/Scala/Python/native exits of zero.
Focused counts are 4/4/27/27; affected Python checks pass 29. The exact original
commands, process receipts, focused XML and source/JAR/deployment bindings are
indexed by `.superpowers/sdd/2026-10-22-delta-cdf-merge/task-5-final-evidence-index.json`.
Historical failed attempts and pre-fix successes remain separate from these
corrected final packages. See [governed MERGE](../../docs/delta-cdf-merge.md)
and [remaining compatibility work](../../docs/delta-spark-parity.md).


### Partitioned qualification

`cdf_merge_live.py --partitioned --runtime <manifest> --inputs <private-inputs> --out <fresh-directory>`
selects a separate five-stage profile: distinct-target and same-table real classic
PySpark MERGE, plus column UPDATE, source SELECT and target MODIFY denial. All four
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 packages have fresh successful Ranger 2.8.0/MinIO
fixtures, totaling 20 partition phases with original outer/Python/native exits zero.
The shared catalog metadata reader is Delta Kernel 3.3.0.

A fixed `part STRING` partition is independent of the `region` row filter. Visible
and filtered rows physically share a touched file; source partition masking moves
selected rows to escaped `moved/= %`, raw filtered rows survive rewriting, and
untouched originals (including a null partition) retain their AddFiles/object
hashes. Original raw rows/CDF, partition action maps/paths, active files, checkpoint
source/end, runtime providers and registration/H2 restart state are required.
Snapshot replay clears dataChange; commit and snapshot flags have separate checks.
The fixture pages through the complete Ranger role list with stable totals and
unique IDs, then requires one owned application role.

Focused Task 3 runs contain 38/3/38/38 cases respectively, with 35 reviewed Task 1/2
partition cases retained for Spark 3.5.4/Delta 3.3.2. First/third focused runs remain
valid across checker/fixture-only changes. Missing older focused common JAR archives
are disclosed: source-valid inheritance uses exact unchanged source/resources/build
inputs, original Gradle outcomes and external dependency/origin records, rather than
claiming unavailable archive-byte equivalence. Later package and actual focused
project JAR archives are captured separately. Source relocation preserves the first
two successful live results and their original exits; failed attempts stay failed.
The provider lifecycle/state tests pass 59 cases, and checker tests pass 14 methods.

Evidence is indexed in
`.superpowers/sdd/2026-10-24-delta-partitioned-merge/task-3/final-evidence-index.json`.
The report retains per-cell freezes, fresh scoped provider assembly, exact shared
component provenance and original commands/receipts/XML. Local selected scalar
parity preserves native invalid-UTF8 binary loss, localFS binary-NUL failure and
mismatched timestamp-zone shifts. See the [tracked plan](../../docs/superpowers/plans/2026-10-24-delta-partitioned-merge.md)
and [bounded MERGE support](../../docs/delta-cdf-merge.md) for qualified cases and
remaining protocol, SQL, worker and deployment boundaries.


### Packaged DataFrame qualification

Build each requested Spark/Delta pair with `cdf_read_live_package.py --app merge
--base <verified-base-manifest> --out <fresh-package-directory> --spark <pin>
--delta <pin>`. It binds the built facade zip, companion JAR, thin fixture app,
source inventory and loaded providers. Ambiguous Python archives reject; clean
stale distribution outputs before retrying. Preserve the original failed attempt.

Run `cdf_merge_live.py --frontend dataframe --partitioned --runtime <new-manifest>
--inputs <private-inputs> --out <fresh-run-directory>` for actual Python public API
construction/execution under the application identity. Scala prepares/observes the
fixture; it does not replace the Python application. Five phases cover distinct
destination, column UPDATE/source SELECT/target MODIFY denials and same-table
writeback. The successful phases exercise strings and bound Columns/mixed maps.
Unpartitioned API coverage is installed-suite coverage, not a live claim.

The strict checker reopens original outer/Python/native exits, requests, imported
zip/module bytes, JAR/provider origins, native bags/actions, escaped/null partition
maps, hidden rows, untouched files, checkpoint source/end and persisted registration
across restart. SQL remains the default frontend and retains its own evidence.
All four final pairs passed five phases each with original outer/Python/native
exits zero. Spark 3.5.6 installed suites passed 51 cases per Delta pin; unchanged
Spark 3.5.4 suites use explicit source-valid inheritance with the historical
common-JAR metadata/archive exception. Original source/package/live evidence is
indexed by `.superpowers/sdd/2026-10-25-delta-dataframe-merge/task-4/final-evidence-index.json`;
final independent SPEC/QUALITY review approved the qualified scope and evidence.
See [supported API and example](../../docs/delta-cdf-merge.md#named-dataframe-api).

### Trusted-stock adaptive qualification

The execution profile is explicit and composes with `--frontend sql|dataframe`:
`--execution-profile nonadaptive|adaptive-stock`, default `nonadaptive`. The new
four-pair live matrix uses the packaged classic-Python DataFrame frontend:

```bash
python delta/acceptance/cdf_merge_live.py \
  --execution-profile adaptive-stock --frontend dataframe --partitioned \
  --runtime <fresh-manifest> --inputs <private-inputs> --out <fresh-directory>
```

Profile settings are written before Spark/session/source/handle construction.
`adaptive-stock` enables AQE and records an eight-partition deterministic fixture;
stock coalescing, local readers, skew, reuse and runtime-filter switches retain
their defaults. These fixture choices are not production toggles. SQL request
composition and installed native SQL execution have focused coverage; the new
four-pair live qualification is specifically the DataFrame path.

Seven phases retain distinct-target MERGE, column UPDATE denial, source SELECT
revocation, target MODIFY revocation and same-table writeback, plus an ordinary
named join and predicate-only SELECT denial. The last denial uses an explicitly
labeled unique fixture policy; public table grants/revokes retain the actual
Gravitino-to-Ranger callback/download chain. The checker validates final payload
plans, original requested/effective settings, native rows/CDF/actions/partitions,
checkpoint files, registration restart, original process exits and every bound
stock/supplemental artifact. Naturally exchange-free CDF and expected denials never
count as fabricated successful adaptive stages.

Original evidence is indexed in
`.superpowers/sdd/2026-10-25-delta-trusted-spark-aqe/task-4/final-evidence-index.json`.
The failed obsolete blanket-AQE expectation in the initial 79-case baseline retains
its original exit 1; the corrected retained-builder settings-drift case has its own
exit-zero run. Remaining pairs use all new trusted suites plus the request/profile
and six affected nonadaptive seams. Unchanged blanket legacy suites are not
represented as rerun on every pair. Every installed runtime JAR has an immutable
copy; original archive hashes and member differences are retained separately.

Support docs are a subsequent Markdown-only change to the recorded package
snapshots. Their separate diff/hash record does not relabel original manifests or
require repeating an unchanged execution matrix. See the [operation-boundary
contract and coverage table](../../docs/delta-cdf-merge.md#trusted-stock-adaptive-execution).

Known diagnostic limitation: the new adaptive observation labels/counts are
qualified only for `adaptive-stock`. A newly run `nonadaptive` record can label
a plan `exchange-free` when it has no adaptive wrapper even if ordinary shuffle
operators exist. Inspect the retained operator plan; that label is not proof of
absence of nonadaptive exchanges. This does not change legacy operation support
or relabel historical evidence. The checker cannot use a nonadaptive record to
qualify adaptive execution.
