# Production-chain review fix report

Review BASE: `a85e076532cb39bb09928152c56753524bea0e8a`.
Production fix and provider regressions: `b7211d88e16c6f923ad8645270da976fa6bf140c`.
Focused harness: `d1db670dc`, diagnostic correction `464d9a04e`, production command
encoding `51014c36f`, native generated-base builder `b3cdaa224`.
Private evidence: `/private/tmp/delta-production-chain-fix-evidence`.

## R1 implementation

`BEGIN_REPLACE` now applies the existing closed `DeltaSchemaContract.validateBase`
policy to the freshly observed complete native base, after native identity/
consistency checks and before generation allocation, marker creation or store
replacement. The shared method is public with strict Javadoc. No policy is copied
into the provider. The observation retains a defensive private configuration copy
and creates a bounded schema/protocol/configuration context only for contract
admission. Existing public safe projection and receipts are unchanged.

Requested-contract rejection is outside the observation-corruption quarantine
catch: a healthy excluded table keeps its PUBLISHED registration. Native I/O and
validation stay outside entity-store callbacks. Existing complete-row CAS and
postcommit base/candidate verification are unchanged. This adds early admission;
it does not claim atomicity with external native writers or fencing.

No core/API/server/client/storage-schema source, container, shared Ranger service,
or catalog-demo changes were made. Only new uniquely owned native prefixes,
server/H2/driver children and private evidence were used. Original data objects
and nonzero process/test results remain retained.

## Genuine pre-fix RED

`red-01` uses the accepted immutable `frozen-02` packages. Frozen package provenance
is `aaf49a363b5526a70d979fc29e20f77709204b62`, with its accepted independent
`source-freeze.json`; `red-source-equivalence.json` verifies frozen admission/
observation/verifier source hashes against the review BASE where present.
The original runtime SHA256 is
`f7a4310e08451380dbd3d7fd7833f88763f97bded47b063d70aa23193d507049`.
The reproducer's submitted Python sources are independently preserved in
`red-01/harness` and bound by `red-01/source.json`; they were not mislabeled as
committed production code.

The original Spark3.5.4/Delta3.3.0 driver exits **1** at native publication with
`DELTA_REGISTRATION_BINDING_MISMATCH`. Its ordinary mapping-none table initially
contains row `id=17`, head0. Schema-only REPLACE requesting name mapping commits
head1, changes the native UUID, and leaves the active table empty. The original
objects were archived before the orchestrator released replacement. Exact before/
after native state and object hashes are in `result.json` and `objects-*.json`.
The stopped server's read-only H2 query (`red-durable-query.log`,
`red-durable-table.csv`) independently confirms durable **QUARANTINED** state.
This is an executed native failure, not the earlier source-only review finding.

The new provider regression was also executed before production edits:
`unit-red-02.log` and `unit-red-xml` retain **6 tests, 6 failures**, each because
`BEGIN_REPLACE` returned instead of throwing. They cover schema-only/query
mapping-none bases and adopted bases with default, generated, identity or raw
redirect metadata. `unit-red.log` failed at formatting before test execution and
is explicitly not behavioral RED.

## Focused unit verification

JDK17: `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.
Final Kernel3.3.0 and3.3.2 focused coverage is **142 tests** per pin:
93 shared schema-contract,40 provider registration-operation,9 native-observation.
All have zero failures/errors/skips. Results are retained under `green-330-xml`
and `green-332-final-xml`; logs are `unit-green-330-02.log` and
`unit-green-332-final.log`. Strict shared/provider Javadoc passed on both pins.

Commands (with the above JAVA_HOME; K is3.3.0 then3.3.2):

```bash
./gradlew :delta:delta-common:spotlessApply :catalogs:catalog-lakehouse-delta:spotlessApply \
  :delta:delta-common:test --tests '*TestDeltaSchemaContract*' \
  :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaRegistrationOperations*' \
  --tests '*TestDeltaNativeObservation*' --tests '*TestDeltaRegistrationStore*' \
  :delta:delta-common:javadoc :catalogs:catalog-lakehouse-delta:javadoc \
  -PdeltaKernelVersion=$K -PskipITs --console=plain
```

There is no separate matched Store suite: the actual counts above are from XML.
An initial Kernel3.3.0 command attached `--tests` after the Javadoc task and failed
argument parsing (`unit-green-330.log`); the corrected original run is retained.
The final3.3.2 command reused already formatted source and omitted spotlessApply.
`green-332-initial-xml` preserves the earlier passing run before two additional
freshness/defensive-copy cases were added; final counts use final XML only.

Complete internal TableEntity equality and owner preservation are **provider unit
fixtures** using the real relational store and Kernel reader over protocol-shaped
local logs. They are distinct from public REST/process evidence. Additional
coverage confirms full raw configuration cannot leak through safe projection,
remains detached from mutable input, and a freshly mapped higher base is used
instead of the earlier stored mapping-none projection. The supported mapped
create/replacement and existing corruption/quarantine/postcommit tests pass.

## Packaged compatibility qualification

Updated packages are built once under `packages/` by the existing focused
`prepareDeltaAcceptance` task, with Kernel3.3.2 provider and externally supplied
native Delta3.3.0/.2 libraries. Both prepare commands exit0; exact commands and
logs are in `packages/prepare-*-command.json`, `prepare-*.log` and exit JSON.
The second preparation reuses the first runtime binary. No fixture jars are added
to provider/plugin classpaths. Independent source/package/driver hashes bind the
actual binaries; runtime ZIP comparison against the baseline differs only in
`DeltaSchemaContract.class` and inherited `gravitino-build-info.properties`.

`green-04` completes at harness source `b3cdaa224`, with outer exit0 and four
original driver exits0. `final-audit.json` independently verifies **24 excluded
requests**, **4 supported replacements**, **104 archived original objects**, and
**20 rejected Spark BEGIN_REPLACE journals without any returned reservation receipt**.
All 24 corresponding server failures originate in shared base admission.
The receipt-free journals corroborate rejection before native stage materialization;
unchanged native object manifests and rows supply process evidence of no mutation.

| Spark | Native Delta | Driver exit | Excluded requests preserved | Supported name-to-name |
| --- | --- | --- | --- | --- |
|3.5.4|3.3.0|0|6/6|pass|
|3.5.4|3.3.2|0|6/6|pass|
|3.5.6|3.3.0|0|6/6|pass|
|3.5.6|3.3.2|0|6/6|pass|

Each actual driver records Hadoop3.3.4 and Scala2.12.18 from its real distribution.
Provider origins in `green-04/provider-origins.json` identify the fixed provider
and shared jars, Kernel API/defaults3.3.2 and Hadoop3.3.6. Every driver jar was
rehash-checked; no Kernel jar is present. Copied server jars match the prepared
package. `source-artifact-binding.json` records the independent final hashes.

```bash
python3 delta/acceptance/admission_run.py \
  --inputs /private/tmp/delta-packaged-runtime-task3-inputs.json \
  --packages /private/tmp/delta-packaged-runtime-task3-evidence/frozen-02 \
  --output /private/tmp/delta-production-chain-fix-evidence/red-01 --red
python3 delta/acceptance/admission_run.py \
  --inputs /private/tmp/delta-packaged-runtime-task3-inputs.json \
  --packages /private/tmp/delta-production-chain-fix-evidence/packages \
  --output /private/tmp/delta-production-chain-fix-evidence/green-04
```

The first command was executed with the original archived pre-fix reproducer
snapshot. Later stronger harness assertions are not claimed to have existed then.
The final command uses its exact frozen `green-04/harness` source. Both use only
new owned prefixes and retain original native bytes.

| Artifact | SHA256 |
| --- | --- |
| Common runtime, identical across all four pairs | `56248fc0af262debc09ecfcde341022fd50db1ceefd903a818b369a67f92ae5e` |
| Provider | `6cd8f26be53e0cfcf262eda19f0bc7ac0286d93d127acae48e330673425ab0a4` |
| Shared Delta common | `68e6bb3ec9ad08dc2b31937a9e6a8b4695e68f0dbbc8231a35b2f6663b933b9f` |
| Kernel API3.3.2 | `4174080102de50d455b772c0bfc96004dd9fe4e7b536b6c9998a8b9b2441bbc9` |
| Kernel defaults3.3.2 | `deb6c12432e3ccbedf0c57ba41ae82f00f3f406db787fd2109d766894c7c4a1f` |
| Native Delta Spark3.3.0 | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| Native Delta Spark3.3.2 | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| Native Delta storage3.3.0 | `951ce7e89468d1657aaf27067a5b9163a4bcccae3dd7d23d711f50bcd3365bb5` |
| Native Delta storage3.3.2 | `c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36` |

Each pair covers six excluded requests: adopted native generated-column base,
schema-only and query replacement of mapping-none bases with explicit and session-
default name selection, plus direct production BEGIN_REPLACE over mapping-none.
The direct request is encoded by the real shared Java API and returns the shared
`INVALID_SCHEMA_CONTRACT` REST error. Spark retains its existing sanitized
`DELTA_REGISTRATION_STATUS_NEEDED` error; no transport change or automatic recovery
is introduced. All excluded operations compare native head/UUID/active rows,
complete public TableDTO, named reads and exact unchanged native object manifests.
A supported name-to-name query replacement retains registration ID, advances its
generation and returns row99.

The generated base is created through the pinned native DeltaTable/DeltaColumnBuilder
API, populated natively and observationally adopted before the excluded replacement.
Actual dependency signatures are retained in `native-builder-api.txt`.
Per-driver results record actual Spark/Delta/Hadoop/Scala class origins/versions;
server class-loading logs independently establish provider/Kernel origins.

Original corrected-attempt failures are retained without replacing evidence:

| Attempt | Original result |
| --- | --- |
| green-01 | First native rejection preserved state; harness incorrectly expected internal schema error text in sanitized Spark exception. |
| green-02 | Four Spark schema/query/default/explicit negatives passed; hand-encoded direct command used version2 instead of the actual version1 envelope and failed command parsing. |
| green-03 | All five unmapped Spark/direct negatives passed; native SQL CREATE rejected generated-column capability before adoption. Native builder API now seeds that fixture. |

These were harness expectation/setup corrections. Production commit and package
hashes remain unchanged across all corrected runs. Failed drivers retain original
exit1/logs. They are not counted as completed matrix passes.

## M1 and M2

M1: the harness SQLite query test now uses `contextlib.closing`, which closes the
connection explicitly rather than only ending a transaction. Python3.13 focused
resource-warning verification passes one test with ResourceWarning treated as error:

```bash
PYTHONPATH=delta/acceptance python3 -W error::ResourceWarning -m unittest \
  test_harness.TestHarness.test_column_snapshot_selects_current_live_version_after_replacement -v
```

Original output: `sqlite-resource-warning.log`; final output after matching the
existing Black24.10.0 formatting is `sqlite-resource-warning-pinned.log`.
`black-pinned-final.log`, Python syntax checking and `git diff --check` pass.
The initial newer Black formatter differed only in this SQLite test's multiline
string layout; the final formatting correction is test-only. Native submitted
harness files are unchanged. This cleanup did not trigger a repeated packaged suite.

M2: inherited worktree build-info lookup provenance from accepted Task3 remains a
separate build-cleanup item. This fix does not rewrite build-info or Gradle.
Current preparation/test logs retain Gradle deprecation warnings; provider Mockito
runs retain the existing JVM class-data-sharing warning. The older worktree lookup
warning is not claimed to have recurred when its task was up to date. Independent
source and artifact bindings remain authoritative.

## Limits and handoff

This report covers R1 admission, M1 resource cleanup and M2 warning provenance.
No full Delta lifecycle, Ranger enforcement, arbitrary-job fencing, new recovery
semantics or catalog-demo completion is claimed. Native unchanged-state checks
compare public registration DTOs; full internal row preservation is separate unit
evidence. No callable claude-mem search/save tool was available; findings remain
here for the root's one scoped rereview. All requested fix-wave validation is complete;
no open implementation finding is known. Independent rereview/approval remains
the root's next step, not a self-approval from this report.
