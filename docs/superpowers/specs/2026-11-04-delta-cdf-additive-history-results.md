# Installed additive-history CDF qualification — November 4, 2026

Fresh bounded named CDF reads across the finite additive profile are qualified on
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2, using Java 17.0.16, Scala 2.12.21 and actual
Ranger client/plugin 2.8.0. This continues the
[additive-history design](2026-11-04-delta-cdf-additive-history-design.md) and
[public CDF/MERGE contract](../../delta-cdf-merge.md). The result is local installed
fixture qualification. It establishes no new deployed Gravitino provider,
Ranger server 2.8.0, MinIO/S3, PySpark or actual Spark Hub acceptance. That
consolidated live gate and the wider selected schema track remain open.

The producing implementation is the reviewed Task 2 revision
`28dd2d85a7890523299a8bacf973641314c86d36`. Task 3 changes documentation only.
At this qualification handoff, independent Task 3/whole-slice review and branch
publication remain pending. Closure is tracked in the
[implementation plan](../plans/2026-11-04-delta-cdf-additive-history.md) and preserved
review ledger. Runtime qualification alone does not close those gates.

## Exact cohort and reuse

Each pair requires **84 exact JUnit invocation identities**, including expanded
parameter cases. Counts do not substitute for exact identities and producing
source/runtime provenance.

| Cohort | Exact invocations per pair |
| --- | ---: |
| `TestDeltaRangerCdfSchema` — full pure predicate class | 9 |
| `TestDeltaRangerNativeCdfSchema` — full stock-native class | 5 |
| `TestDeltaRangerCdfSchemaEvolution` — full installed additive-history class | 13 |
| `TestDeltaRangerCdfHistory` — full completeness/profile/history class | 43 |
| Selected ownership, policy, native parity, recovery, source and AQE controls below | 14 |
| **Total** | **84** |

The 14 selected controls are:

- `TestDeltaRangerCdfNativeParity.explicitCdcDoesNotRequireSupersededAddOrRemoveFiles`
- `TestDeltaRangerCdfReadOwnership.foreignHistoryAndCachesCannotBorrowAuthority`
- `TestDeltaRangerCdfReadOwnership.concurrentAnalyzedSiblingsIssueExactlyOneReceipt`
- `TestDeltaRangerCdfReadPolicies.policiesApplyToEveryNativeImage`
- `TestDeltaRangerMergeOwnership.copiedMaterializationCannotBorrowMergeAuthority`
- `TestDeltaRangerMergeOwnership.ordinaryCdfReceiptCannotUseNullMergeOwnerToReopenExecution`
- `TestDeltaRangerMergeRecovery.finalRecoveryReadbackRechecksSourcePolicy`
- `TestDeltaRangerMergeRecovery.missingUnknownAndMismatchedEvidenceNeverReplay`
- `TestDeltaRangerMergeRecovery.incompletePairAndAutoResetRejectBeforeFiles`
- `TestDeltaRangerMergeRecovery.commitAcknowledgementFailureReconcilesWithoutReplay`
- `TestDeltaRangerMergeSource.compatibleSourceAdvancementDoesNotExtendFixedInterval`
- `TestDeltaRangerMergeSource.blockLossRetryCreatesFreshAttempt`
- `TestDeltaRangerMergeSource.blockLossRetryRechecksChangedPolicy`
- `TestDeltaRangerTrustedAqeCdf.capturedNativeManifestCallbackUsesPhaseRecipe`

| Spark | Delta | Fresh Task 3 invocations | Reused invocations | Result |
| --- | --- | ---: | --- | --- |
| 3.5.6 | 3.3.0 | 0 | 70 source-identical installed/affected + 14 impact-qualified pure/native | Passed with retained scope |
| 3.5.6 | 3.3.2 | 70 installed/affected | 14 impact-qualified pure/native | Passed |
| 3.5.4 | 3.3.0 | 84 | None | Passed |
| 3.5.4 | 3.3.2 | 84 | None | Passed |

All selected cells have zero failures/errors/skips and original process exit 0.
There are **238 fresh + 98 reused = 336 qualified pair/invocation results**, with
no duplicate contribution from intermediate runs. The first pair is composed of
retained Task 1/2 runs; it is not represented as a fresh 84-invocation execution.
The 70-case cohort combines 13 installed additive invocations with 57 affected
controls; some history/parser/callback controls are pure unit tests. It does not
represent 70 separate installed policy-enforcement scenarios. Original failed
attempts remain preserved and are excluded from qualification.
No unrelated APPEND/MERGE matrix, external service or live deployment was rerun.

Task 2's two final runs have 169 Scala sources and 1,162 final compiled class
hashes, identical to one another. Their source inventories match Task 3BASE.
Task 1's two runs have 168 Scala sources and 1,155 classes. Compared with Task 2,
only the History implementation and its existing test differ among those sources
and classes; the new installed suite accounts for the additional source. The
pure helper, stock tests and shared fixtures retain their source hashes and
class bytes. Native controls install no governed extension, so later History
admission does not execute there. This is impact-qualified reuse, not equality
of all historical product files or a historical full-classpath/build/resource
attestation. Original runtime origins remain bound to each result.

## Native and installed observations

Delta retains the captured current read schema and reconstructs historical nulls
for added fields. Complete native row bags include real commit timestamps,
versions, change types and duplicate multiplicity. The pure predicate qualifies
only nullable concrete top-level scalar additions with an exact unchanged old
field prefix. It does not grant execution authority or consume writer options.
Stock controls also show native nested additions succeed outside the governed
finite profile, while an incompatible type history fails with native unsafe
compatibility bypass disabled.

| Installed named, legacy and partitioned/AQE interval | Exact rows | Observation |
| --- | ---: | --- |
| 1..8 | 8 | Old null insert, two duplicate populated inserts, two update pre/postimage pairs, populated delete |
| 1..2 | 1 | Current added fields are null; no later rows |
| 3..4 | 2 | Addition at the start and two duplicate populated inserts |
| 3..3 | 0 | Metadata-only interval retains current schema |

The history owner validates start-snapshot compatibility and every encountered
Metadata transition in native order within the inclusive requested interval.
Duplicate Metadata, drop/re-add and type-change/restoration cannot conceal an
incompatible in-range transition. Pre-start transitions and unrelated suffix log
files after the end are not newly scanned; endpoint-to-current compatibility
makes no claim about every unrequested transition. Identity, partitioning,
protocol, complete requested history and native file/profile checks remain.

Installed named history exercises reader 1/writer 7 with exactly `changeDataFeed`,
`appendOnly` and `invariants`. Separate legacy and partitioned/AQE histories
exercise reader 1/writer 4. Exact-schema reads retain the prior reader 1/writer
<=4 and writer-7 profile. These are measured profiles, not general feature support.

SELECT-only reads require no ALTER. Current real Ranger policies govern both null
and populated added values. Added-field denial, allowed old-field projection,
original predicate-only demand, coalesce/concat masks, row filters and retained/
fresh revoke are exercised. Native rows remain unchanged by standalone reads.
External schema advancement rejects retained and fresh reads while named
registration remains stale; automatic registration refresh is not established.
Copied plans/receipts/materialization, foreign ownership, eager missing history,
CDF gaps, unsupported features and permissive settings retain their refusals.
Captured subject/settings and the existing action-boundary policy contract remain
binding; this adds no final-output revocation guarantee.

Fresh SQL and JVM companion MERGE consume a named 0..6 interval across atomic
schema-plus-data changes. Native setup records WRITE/AddFile/Metadata for the SQL
source and MERGE/AddFile/CDC/RemoveFile/Metadata for the companion source. Original
added-field SELECT denial happens before target mutation; after grant, exact
four-row targets include `missing` for old nulls and `payload-masked` for the new
value. These fixed-schema target writes require no ALTER. The distinct real
evolving-target recovery case retains its own ALTER/new-field UPDATE and scoped
settings obligations. Its recovered owner cannot open, plan or materialize source
payload; source attempts stay zero, target version stays 1, and no source/target
files or extra commit are created. No automatic replay is added.

Each installed additive suite retains 24 scenario directories: 21 governed
observations and three explicitly labeled `merge-native-*` setup/oracle contexts.
Those three run before governed extension installation and show only
`io.delta.sql.DeltaSparkSessionExtension`; their fixture `rangerEnabled=true`
setting is not policy-enforcement evidence. Task 1 stock controls explicitly set
`rangerEnabled=false` and retain seven native scenarios per pin. The installed
observations retain both the governed extension and Ranger enabled.

## Reproduction and retained provenance

The preserved workspace is
`.superpowers/sdd/2026-11-04-delta-cdf-additive-history/`.
`task-3-evidence/cohort-84.json` freezes all exact XML identities;
`freeze.json`, `reuse.json` and `reuse-verified.json` record the producing sources,
reuse basis and independently checked original artifacts. Task 1/2 original
commands, exits, XML, runtime and native evidence remain in their original roots.
`task-3-report.md` records final fresh-cell verification and practical limits.

The three fresh cells run sequentially using Java 17 at
`/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`,
`JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'` and:

```sh
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
  --tests '<exact frozen selection>' \
  -PdeltaSparkVersion=<3.5.4 or 3.5.6> -PdeltaVersion=<3.3.0 or 3.3.2> \
  -PskipITs --max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g --console=plain
```

Repeat `--tests` for each full class or selected method in the cohort. The 70-case
cell omits the two already qualified pure/native classes. Exact expanded command
arrays and environments are retained per cell. Both evidence options use distinct
fresh roots: `GRAVITINO_DELTA_CDF_SCHEMA_EVIDENCE_DIR` for stock controls and
`GRAVITINO_DELTA_CDF_ADDITIVE_EVIDENCE_DIR` for installed observations.

Fresh runs retain before/after source/resource/build manifests, original logs/
exits, exact copied XML, compiled class inventories, actual/expected row and schema
artifacts, numbered native actions and loaded Spark/Delta/Scala/Ranger JAR paths
and hashes. Source inputs remain identical. The sole first-cell input change is
Gradle-generated `common/src/main/resources/gravitino-build-info.properties`,
which updates compile-date/commit metadata to the producing revision; both hashes
and the resulting bytes are retained. Later cells also retain its before/after
bytes. No historical pre-run bytes are invented. Per-cell `verified-summary.json`
and `native-manifest.json` record
checked counts and existing repository-relative artifact paths. The input freeze
covers 1,361 selected module/dependency/build files, including 928 Java and 170
Scala sources; 169 Scala sources belong to the target module. It is not an
entire-repository or full transitive runtime-closure attestation.

The first generated build-info SHA-256 changes from
`b712fabb469b660ae522dca61ec5df0c8969450b58b6fb6b90007a23bc83fc5a`
to `57ea22e8f6b0fed668099d385f8284bbdaaf71f761fdcdcad098fed969336b35`.
Its resulting properties record `2.0.0-SNAPSHOT`, compile date
`04/11/2026 17:36:47` and producer
`28dd2d85a7890523299a8bacf973641314c86d36`. The first pre-run bytes were not copied;
the original manifest is the preserved before-hash evidence. Concrete consumers
are `Version`'s info/DTO and the client version surface; HTTP requests use only
the unchanged project-version property for `X-Client-Version`. The changed CDF
admission, native reconstruction and policy paths do not read compile-date/commit
properties. This generated-metadata exception is not immutable-input equality.

Producing source SHA-256 values:

| Source | SHA-256 |
| --- | --- |
| `DeltaRangerCdfSchema.scala` | `82e0443e9e97631bd97deb2cb257a13fe705e886dbf363eb68be6d98b849eb15` |
| `DeltaRangerCdfHistory.scala` | `f453e1498c9e2a3c701801399c42d43c9ab7cfa1c92df8099c0bb408f1aa8604` |
| `DeltaRangerCdfNativeReader.scala` (unchanged) | `c57e4b27cae4474bb6b1435b027b182ed2d278eb2abeefc134ac90a9c0710aca` |
| `DeltaRangerCdfReadScans.scala` (unchanged) | `089bfbc7026ebb7f8bb27733e8f34d1252e97e455e1135ddb6d2d9a4bf7fb864` |
| `TestDeltaRangerCdfSchemaEvolution.scala` | `840b1455081cd4f34fe152496cfb59b4909acf4556be66fe73130ea11926d51b` |

Actual loaded artifact SHA-256 values (origin paths remain in each runtime record):

| Artifact | SHA-256 |
| --- | --- |
| `delta-spark_2.12-3.3.0.jar` | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| `delta-spark_2.12-3.3.2.jar` | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| `ranger-plugins-common-2.8.0.jar` | `10ad212120866d79566a595ff1282e92a6e43e1460eab558ef0ad4ec1e27d59e` |
| `scala-library-2.12.21.jar` | `e2f4040c95cc4dd1cfeaae38bdde596e4b36933632451efa7d8564c6a359510c` |
| `spark-sql_2.12-3.5.4.jar` | `06baa5b5190831a1d075eeebb7aa02c872dee91975f97da6a2fb6c5deeff49ae` |
| `spark-sql_2.12-3.5.6.jar` | `37535c443d428a8728c21a1317faf5652179748dddc131da494f0377832daa08` |

## Remaining scope

The named read and companion API version 1 remain unchanged. Gravitino owns
registration; stock Delta owns schema/log/file reconstruction and transactions.
No replacement reader, Glue compatibility, second metastore, raw-path governance
fallback, or core/API/server/client/storage-schema change is introduced.

Nested/type-changing/full-schema evolution, defaults/generated/identity fields,
column mapping, deletion vectors, row tracking and broader feature protocols
remain outside this finite profile. Broader source composition, consumer/state,
checkpoint/replica semantics, policy-transition reconciliation and streaming stay
deferred. Existing historical fixed-schema live records retain their own scope;
they do not close additive-history server/MinIO/Python/Spark Hub acceptance.
The selected order remains schema evolution, governed creation, inputs/
transformations, then overwrite/full refresh. No full Delta support is claimed.
