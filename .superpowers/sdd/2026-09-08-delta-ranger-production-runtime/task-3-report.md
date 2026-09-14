# Task 3 implementation report

Status: source committed and ready for independent root spec/quality review. This is bounded fresh SELECT qualification, not final production/runtime acceptance.

- BASE: `dc5e9cddefbcdcfd929c1f34f871ebb36a20e22e`
- Source HEAD: `d5e35f6e17862b0a86c7090f87e49571e622f0b4`
- Worktree: `/Users/jacobo.calderon/Code/Forks/gravitino-delta/.worktrees/delta-spark-feasibility`
- Branch: `codex/delta-spark-feasibility`
- Commit: `feat(spark): enforce bounded canonical Delta Ranger SELECT`
- Only the 15 owned optional-module/common source, test, build, formatter-config and README files were committed. Root-owned plan/spec edits remain unstaged. No provider/core/API/server/client/storage-schema source changes, demo edits, service mutations, push, merge or PR.

## Implemented interfaces and behavior

`org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerCatalogSpark35` is the optional no-arg native subclass. It retains the accepted base constructor's metadata/current-actor separation, lazy owned metadata client, current actor lookup per inherited mutation, native DeltaTableV2 loading and disabled catalog-update invariant. It requires canonical application-visible discovery before initializing its engine. Initialization failure, explicit close and application-end close only its owned metadata/engine lease; terminal lifecycle and hidden-catalog behavior are tested.

Common wiring is string-only, default false, and introduces `spark.gravitino.enableDeltaRangerSupport`. It requires `spark.gravitino.enableDeltaSupport`, selects the optional wrapper for discovered Delta catalogs, checks optional classes without initialization, and orders `DeltaRangerSparkExtension` before the native Delta extension. Stock RangerSparkExtension conflicts. Other explicitly compatible extension strings are retained; production extension startup requires exactly one companion before exactly one native extension.

Trusted startup keys are:

- `spark.gravitino.enableDeltaSupport=true`
- `spark.gravitino.enableDeltaRangerSupport=true`
- `spark.gravitino.deltaRanger.service`, `spark.gravitino.deltaRanger.definition`
- `spark.gravitino.deltaRanger.metadata.config`: absolute regular owner-only properties file
- Existing `spark.sql.gravitino.uri` and `spark.sql.gravitino.metalake`
- Existing `spark.sql.catalog.<canonical-catalog>.journal-directory`
- Existing native `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`, `spark.databricks.delta.catalog.update.enabled=false`

The private metadata file contains exactly `auth-type=simple`, an explicit distinct `principal`, and `lifetime-seconds` in 1..86400. Unknown keys/modes, symlinks, non-owner permissions and invalid lifetimes reject. Endpoint/metalake/catalog come from trusted startup/canonical binding, never the file. The wrapper's initial mode uses existing `withSimpleAuth`; no Basic/custom-token wrapper mode was invented.

The extension admits resolved canonical native scans with Project/Filter/SubqueryAlias and conditioned inner joins, finite scalar expressions and top-level columns. It invokes original `PrivilegesBuilder.build`, validates source/target/kind, and authorizes every original projected/predicate/join column before any row-filter/mask evaluation. Same-name scans, same owners, both join orders and self-join ExprIds remain distinct. Count, aggregates, columnless SELECT, outer joins, views, subqueries, user UDFs, raw sources, commands, time travel and other unqualified shapes reject.

Pinned Kyuubi V1 extraction drops catalog identity. The approved adaptation extracts an otherwise identical copy with a unique opaque descriptor table token for each exact scan. It maps results through the local token map, checks canonical db/owner/catalog/columns, and independently checks original leaf ExprId references. It never groups catalogless results to recover identity or changes executable descriptors/expressions/ExprIds. Public RowFilterMarker/DataMaskingStage0Marker, stage-1 remapping and marker elimination were qualified before reuse. No stock request-generating authorization/filter/mask rule is installed. CUSTOM and typed NULL preserve original types; row filters precede masks and user predicates see rewritten columns. Unsupported/cross-column expressions reject. Subject-tag fixed point avoids repeated rewriting and rejects another subject's marked plan.

Task2 interfaces remain unchanged: config/classpath audit guard and full-snapshot transport requirement, canonical resource keys, current authenticated UGI/groups, policy-engine leases/version/readiness/evaluation, and finite metadata-client lifetime. Production uses `DeltaRangerConfig.fromClasspath` and `DeltaRangerPolicyEngine.acquire`; no production test-policy injection. Binding remains `ranger.plugin.spark.gravitino.metalake`. Token query identity still rejects unverified identity.

## Qualified native metadata preparation

The companion pre-CBO rule checks fresh authorization, then synchronously delegates to a narrow public PrepareDeltaScan subclass before the native extension's own PrepareDeltaScan. Its protected getter calls super once, requires the actual selected Snapshot, registers only that snapshot's exact statistics-cache schema for the authorized native index, and returns the generator unchanged. Scope is private/non-inherited, clears on success/failure, and retains no cross-query snapshot/schema cache. Native caching is preserved; no cache disabling, private API invocation, processor replacement, reflection access or stack detection.

Admission combines finite observed native plan nodes with typed DeltaLogFileIndex leaves (bound to authorized log paths in scope), exact scoped SingleAction/state or selected-snapshot stats RDD schemas, and expression checks. Exact readless SingleAction/Row bindings require DeserializeToObject over an actually empty LocalRelation; AddFile additionally requires scope. Invalid/non-native/nonempty/read-backed/UDF/subquery bindings reject directly without leaf fallback.

Two observed native ScalaUDF forms are explicitly qualified: DeltaLog$CanonicalPathFunction and DataSkippingReaderBase's generated four-argument size-collector family. Both require active scope, pinned Delta classloader and CodeSource, class/family and Function1/Function4 signature, input/output/nullability/determinism, and exact qualified leaf argument lineage. Same-signature user functions, wrong native arguments, either form outside scope, and Python UDFs reject. The private native testing callback is neither invoked nor replaced; trusted-launcher/pinned-artifact integrity remains a boundary.

Public source evidence is retained under `task-3-evidence/pinned`: Kyuubi `serde/tableExtractors.scala`; Delta `Snapshot.scala`, `StateCache.scala`, `PrepareDeltaScan.scala`, `DataSkippingReader.scala`, `StatisticsCollection.scala`, `DeltaLog.scala`, and the source JARs. Relevant native paths are Snapshot state reconstruction -> cached SingleAction RDD; allFilesViaStateReconstruction -> AddFile binding; withStatsCache -> cached AddFile schema with parsed stats; final Dataset.collectFromPlan -> readless Row binding. Exact observations/stacks were archived before narrowing admission.

## Verification and original evidence

Evidence root: `.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence`.

| Final source run | Runtime | Result |
| --- | --- | --- |
| `accepted-source-delta330` | Spark 3.5.4 / Delta 3.3.0 / Scala 2.12.21 / Java 17 | exit 0; 52 optional + 17 common tests; 0 failures/errors/skips; strict Javadoc and jar passed |
| `final-source-delta332` | Spark 3.5.4 / Delta 3.3.2 / Scala 2.12.21 / Java 17 | exit 0; same 69 tests, 0 failures/errors/skips; strict Javadoc and jar passed |

The first directory name is an implementer evidence label, not root acceptance. `command.json`, `exit.json`, complete `gradle.log`, per-run copied JUnit XML, `source/`, `source-hashes-before.json`, `artifacts/`, `artifact-hashes.json` and `classpaths.json` retain each run. `final-verification.json` summarizes independently parsed suites/classpath/jar checks. Both test-run source manifests are identical. After verification, staged diff review removed trailing whitespace from one formatter license-comment line only; `post-verification-format.patch`/`.json` and `commit-source-binding.json` record that exact tested-to-committed delta. All executable/test source is unchanged from both runs. Scoped Spotless and staged diff checks pass; no suite replay was needed for a license-comment whitespace fix.

Final tests execute real native local Delta rows: masks/filters/NULL, both same-name catalog join orders, projected/predicate/join denials before rewrites, SELECT *, self-join/alias IDs, escaped quoted mapped column, malformed/unsupported masks, fixed point, changed-subject plan rejection, fresh actual Spark session, raw Delta/JSON/Parquet and mixed native/raw rejection, native reconstruction, rule ordering and native second-pass idempotence. Wrong-schema/snapshot/scope/thread/encoder/UDF controls pass on both patches. Common application-visible map/hidden-catalog and metadata/current-actor/lifecycle tests pass.

A concrete wrapper test loads the seeded native table through the actual wrapper/base implementation, validates native UUID and catalog/db/table descriptors, preserves a colliding spark_catalog.db.t descriptor/row, rejects enabled catalog updates and closes the owned lease. Its metadata supplier and startup settings are controlled package-only test seams; initialization and native load/head-validation/descriptor code are shared with no-arg production. The installed-extension execution tests use a request-sensitive RangerBasePlugin transport double behind the actual Task2 engine, and a native test catalog returning real DeltaTableV2 descriptors. These are not live Gravitino visibility, metadata REST, or downloaded-policy acceptance tests.

Original behavioral RED/GREEN evidence remains unmodified:

- `catalog-loss-red`, `token-extraction-red-marker-qualify`, `enforcement-red-token-green`, `enforcement-green-1`: catalog-loss/marker qualification, original-usage extraction and native authorization/rewrite progression. Leaf schema availability was corrected to non-leaf original expression usage; it is not column-use evidence.
- `installed-extension-red`: no-op installed extension returned original rows. Later SingleAction/StateCache/AddFile/stats/Row observations and failures remain in their named `installed-extension-*`, `scoped-native-*` and `scoped-stats-observer` runs.
- `scoped-native-row-3`: rows/join/native idempotence passed; JSON negative required a SecurityException cause-chain assertion because Spark wraps schema-inference rejection in AnalysisException. It also preserves the invalid read-backed binding negative exposed in `scoped-native-stats-2`.
- `native-udf-origin-red`/`native-udf-origin-green`: same-signature user-function admission RED, then origin/signature/lineage enforcement GREEN. Runtime function origins/flags and native source path observations are retained.
- `final-boundary-330`: direct wrapper descriptor/sentinel/update and new-session/mixed-source/changed-subject checks passed before final full runs.

Setup/compilation failures are distinct from behavioral RED: initial sandbox/cache access, Zinc Scala2.13 forced to2.12, test imports/API compile adjustments, programmatic/reused-builder extension-order setup, and formatter Scala mismatch. The final optional-module version override targets only main/test compile/runtime classpaths, leaving Zinc/scalafmt tool Scala independent. No global/default build changed. Older helper runs did not capture a complete source tree at each intermediate point, and per-run XML copies may include stale common XML when common was not selected; use final runs and command/log task timing for claims.

## Binary provenance limits

All six connector JARs per final run were copied and SHA256-verified after that run and before the next Gradle build. Base runtime has zero Ranger/authz/companion package entries; optional jar has no Spark/Delta/Scala platform classes and all its classes target major 52. Main/test dependency coordinates qualify Spark 3.5.4, Scala 2.12.21, Ranger 2.8.0 and each selected Delta patch; optional runtimeClasspath deliberately omits Spark/Scala/Delta. External immutable dependency hashes are distinct from project artifacts.

`classpaths.json` records resolution-time hashes, not uniformly post-build or execution-time hashes: its init-script task has no explicit dependency on all artifact-producing tasks. Delta330 log order: common:jar line166, base shadowJar232, classpaths238, optional test247; its base hash matches that run's post-build archive. Delta332: classpaths221, base shadowJar242, optional test250; its base hash is still the preceding330 artifact. The post-build `artifact-hashes.json`/archives are authoritative for connector binaries. Automatic rebuilds explain project-hash changes; they are not external dependency corruption.

The exact whole root common330 JAR (`45fe5e5de3d5a0aaf2ab0bb076caac1a0b18e7328b024e4b68f4bde8c22f05fc`) was not retained: the original archive helper covered `spark-connector/**/build/libs`, excluding root `common/build/libs`, and a search of 91 archived common-JAR candidates found no match. Do not claim complete byte-for-byte retention of every330 project dependency. Current common332 (`7a03a6c5252c0997f1c531b8cf780af2e707aeade027a861064417086933255a`) and all seven currently resolved project JARs were copied later, without another build, under `final-source-delta332/later-project-capture/` with a labeled manifest. This later capture does not retroactively repair the330 retention gap. Existing Gradle git-metadata path warnings also remain in logs; source identity is supplied by the explicit commit/source manifests, not embedded build git metadata.

## Remaining gates

Task4 owns the genuine public Gravitino -> Ranger2.8 provider/download -> packaged executed four-pair chain (including Spark3.5.6). It must create/grant the separate metadata principal, prove local CATALOG/USE_CATALOG/ALLOW application visibility with no table grant/owner workaround, fresh-driver revoke hiding the catalog, and metadata-reader resolution for a Ranger-only column user. No root S3/Ranger fixtures were mutated or needed by these focused local tests.

Count/aggregates/columnless SELECT, additional standard masks, unqualified checkpoint/table-feature reconstruction routes, full Delta operations, ordinary reconciliation, streaming/time travel/maintenance, cached/reused-plan revocation, authenticated token query identity, expanded wrapper metadata-auth modes, metadata confidentiality and arbitrary-job/deployment security remain explicit gates. A bounded fresh-analysis subject marker is not a cache/revocation guarantee. Root independent review and acceptance are pending.

## Fix round 1 — effective Gravitino application authentication

Review finding: “Validate the effective Gravitino application/actor identity, not only Spark/UGI names” (P2). Fix base `d5e35f6e17862b0a86c7090f87e49571e622f0b4`; committed source `f7343d46a6ee39fd1f614e648c5b555d031974e7` (`fix(spark): require qualified Gravitino application auth for Delta Ranger`). Exactly the optional wrapper, its wrapper test and README changed; root-owned plan/spec edits were excluded. The original Task3 evidence above is unchanged.

The common client factory reads `GravitinoSparkConfig.GRAVITINO_AUTH_TYPE` (`spark.sql.gravitino.authType`), defaults to SIMPLE, and applies SIMPLE case-insensitively. SIMPLE uses Spark's application user; Basic instead uses its independently configured username. The wrapper now requires that same effective SIMPLE mode before metadata configuration or engine acquisition. The existing distinct metadata-versus-Spark/current-UGI checks, no-arg path, current-actor transport, Task2 metadata builders and Ranger downloader authentication remain unchanged. The optional module uses the exported common key and verified literal SIMPLE default/case semantics; no new dependency or common source change was needed. Basic, Kerberos, OAuth2, token, unknown and empty configured application modes reject with a clear diagnostic. Other application-auth modes require later identity-separation qualification.

The regression runs inside a `worker` UGI subject with Spark user `worker` and Basic username equal to the separate configured `metadata-reader`. This previously reached engine acquisition. Seven configured modes (`basic`, `BASIC`, `kerberos`, `token`, `oauth2`, `unknown`, empty) now reject before engine/actor transport use. Default, explicit `simple` and uppercase `SIMPLE` preserve the distinct-principal visible-catalog/lifecycle positive. These are controlled wrapper/startup seams, not live authentication or REST acceptance.

| Fix run | Scoped wrapper result | Exit/output |
| --- | --- | --- |
| `fix1-auth-red` | 13 tests, 7 failures, 0 errors/skips; unsupported modes reached engine acquisition | 1; BUILD FAILED in 22s |
| `fix1-auth-format` | Scoped SpotlessApply | 0; BUILD SUCCESSFUL in 2s |
| `fix1-auth-green` | 13 tests, 0 failures/errors/skips; scoped SpotlessCheck, strict Javadoc and jar passed | 0; BUILD SUCCESSFUL in 22s |

Runtime: Java 17, Spark 3.5.4, Delta 3.3.2. Exact commands (relative to the isolated worktree):

```sh
./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerCatalogSpark35' -I .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix-classpath-capture.gradle -Ptask3FixEvidenceOutput=.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix1-auth-red -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2
./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2
./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerCatalogSpark35' :spark-connector:spark-delta-ranger-3.5:javadoc :spark-connector:spark-delta-ranger-3.5:jar -I .superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix-classpath-capture.gradle -Ptask3FixEvidenceOutput=.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-3-evidence/fix1-auth-green -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.2
```

Each named directory under `task-3-evidence` retains its command, exit, full Gradle output, source snapshots/hashes and copied XML/artifacts. Only the optional wrapper test was selected and executed in the RED/GREEN fix commands. The 17 common cases copied by the existing archive helper are prior-run XML, not additional fix validation. RED ran SpotlessApply before testing, so its pre-command test-source snapshot precedes formatting; its exact executed class files are separately retained. GREEN ran after formatting; all 25 captured source/config/build/README files match the committed tree byte for byte, as asserted in `fix1-source-binding.json`. No native logic changed; the previously retained 69-case runs on both Delta patches were not replayed for this scoped startup fix.

The private `fix-classpath-capture.gradle` attaches capture to `Test.doFirst`: artifact-producing dependencies finish first, then the actual test classpath is captured immediately before test execution. Each fix RED/GREEN execution manifest records 203 paths: 11 archived mutable inputs (directory contents zipped with per-entry hashes, plus project JARs), 191 immutable external JAR hashes, and one nonexistent main-resources path. All archive hashes, 68 directory-entry hashes and external hashes were verified for each run. Six connector JARs per run were separately copied and verified after the Gradle command finished, before the next build. `fix1-verification.json` records these checks and scoped XML counts. This new execution-time capture is independent of the older resolution-time manifests and does not retroactively fill the original Delta330 whole-common-JAR retention gap.

Self-review and `git diff --check` passed. M3 (post-acquisition cleanup test) and M4 (repeated traversal) remain deferred minor review points outside this fix round. Root narrow re-review is pending; no services, credentials, common/core sources or native admission code changed.
