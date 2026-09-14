# Task 2 implementation report

Status: implemented and committed, ready for root-owned independent review. This is Task 2 acceptance evidence, not completion of the full production-runtime goal.

- BASE: `f1d1ba8ef7a032a9de3a2f786ff1d6fe294c1980`.
- Source HEAD: `f03124b390b88ef2c5fc7fed18b82adfa7943ed2`.
- Scope: 18 task-owned source/build/documentation/test files; no core, API, server, client, storage-schema or base-runtime source changes. Root's spec/plan edits were not staged.
- Evidence root: `.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-2-evidence/` (called `E` below). Live fixture root: `.superpowers/runtime-20260914/ranger-download-fixture/` (`F`). These persistent local evidence directories include private runtime material; do not publish them wholesale.

## Implemented boundary and Task 3 contract

The optional Gradle project is `:spark-connector:spark-delta-ranger-3.5`. Its README documents startup and consumer requirements. `DeltaRangerConfig.fromClasspath(service, definition, metalake)` reads ordinary startup `ranger-spark-security.xml`, captures trusted configuration identity as a fingerprint, and requires an existing absolute owner-only POSIX 0700 cache directory. The actual public Kyuubi singleton initializes Ranger's real downloader. No production policy/role injection, reflection replacement or package/class shadow is used.

`DeltaRangerPolicyEngine.acquire(config)` returns a shared, synchronized AutoCloseable lease. Incompatible bindings/configurations reject; one catalog closing leaves another lease operational. Final owned close drains plugin/audit resources; the singleton is terminal thereafter and requires a fresh driver. `requireReady()` validates current downloaded service/config/closed definition, policy and role versions, and role-service identity. It reads current downloaded service configs, not a stale value merged into plugin configuration. `authorize`, `mask` and `rowFilter` recheck snapshots; null/error/deny and a refresh observed during evaluation reject. `versions()` returns immutable service/definition/policy/role observations.

Immutable `DeltaRangerResourceKey` includes canonical catalog/database/table/column/access/owner. Fresh AccessResource instances populate both catalog option and catalog resource-map value before evaluation. `select(..., originalColumns, ...)` preserves all original columns and yields a table-wide `*` request for an empty column set. Task 3 must call it for every original table, including predicates and both join sides, before rewrites.

`DeltaRangerQuerySubject.current(session)` captures actual current UGI short name/groups; a configured Kyuubi session user must agree. Downloaded Ranger roles resolve those subjects. Existing token cache identity parses unverified JWT claims and cannot attest token-to-UGI identity, so token query mode explicitly rejects, by root ruling. No metadata identity fallback is allowed. Live qualification used authenticated/current SIMPLE launcher UGI; it does not establish Kerberos or token qualification.

`DeltaRangerMetadataClient` is a lazy `Supplier<Catalog>` and AutoCloseable lease suitable for construction before the no-arg wrapper's super-constructor. Configure once with endpoint, canonical metalake/catalog, positive lifetime up to 24 hours, and explicit existing client-builder authentication callback. The separate owned client closes on expiration, including an in-flight read; expired results reject. Custom/token metadata HTTP support remains separate from query identity. Task 3 must keep the accepted current-actor supplier on existing transport and provide a distinct application catalog-visibility grant.

Mask results distinguish NONE, typed-NULL instruction, and EXPRESSION. CUSTOM substitutes a safely backtick-quoted canonical column. NULL consumers must construct the original Spark data type's null literal. Expressions are syntax checked; Task 3 still owns scope/admission/attribute/order/fresh-plan validation. Other enabled mask kinds explicitly reject. The real pinned public helper was characterized for standard masks, service transformers and unusual columns; its raw substitution is unsafe for unusual names and prequoting changes policy identity, so additional kinds were not selected.

## Protocol and deployment corrections

Ranger 2.8 ServiceDBStore propagates only `ranger.plugin.*` service properties. Actual HTTP and Java downloads proved that the original `gravitino.metalake` key was absent. Root authorized the narrow Task 1 provider/definition/test/README correction to the single authoritative `ranger.plugin.spark.gravitino.metalake` key. There is no old-key fallback; previously provisioned initial services must adopt this key before activation. Gravitino `authorization.ranger.delta.metalake` is unchanged. Source provenance and real snapshots are under `F/source/` and the fixture contract; binding regression is recorded below.

The first genuine downloader run exposed the fixture-only Basic downloader password through Ranger AuditProviderFactory's INFO property dump. That raw log is preserved privately at mode 0600; a tool tail also exposed the then-current fixture credential. No Admin credential was used by queries. Root immediately drained that JVM and rotated the exact downloader credential: old HTTP authentication 401, new 200 (`F/credential-rotation.json`). Subsequent fresh JVMs used startup Log4j2 configuration with AuditProviderFactory WARN. Production now checks that logger by string name before singleton initialization and rejects if INFO is enabled; it does not mutate global logging. A focused RED/GREEN regression covers the guard. The README requires the same deployment logger setting. Safe/live/outage logs were checked for the rotated credential with boolean-only output, all false.

## Verification and source binding

`E/final-source-manifest.json` freezes all 18 source SHA256s. `E/final-test-manifest.json` binds six archived XML suites to that manifest and `E/final-exits.json`. Final adapter results are in `E/final-adapter-xml/`: 29 tests; affected optional provider results in `E/final-provider-xml/`: 17 tests. All 46 have zero failures/errors/skips. Final source was frozen after the last focused run; final import cleanup is covered by that run.

The required focused test command was run with Java 17 and Spark 3.5.4/Delta 3.3.0:

```bash
./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests '*TestDeltaRangerConfig' --tests '*TestDeltaRangerPolicyEngine' --tests '*TestDeltaRangerQuerySubject' --tests '*TestDeltaRangerMetadataClient' -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
```

The final invocation also ran module Spotless application and strict Javadoc: `E/final-focused-verification.log`, session 98658, exit 0. Both changed projects' Spotless checks passed in `E/final-spotless-check.log`, session 55103, exit 0. The provider's two directly affected test classes (17 tests), strict Javadoc and dependency report passed in `E/final-dependencies-provider.log`, session 14628, exit 0. Runtime dependencies were inspected with `:spark-connector:spark-delta-ranger-3.5:dependencies --configuration runtimeClasspath`. `git diff --check` passed before commit. No broad native/provider suite was rerun.

Behavioral RED evidence retained in `E`: resource identity/count/invalid input (`red-resource-2.log`, 3 failures), config/subject (`red-config-subject-2.log`, 7 failures), metadata lease (`red-metadata.log`, 4 failures), engine boundaries (`red-engine-2.log`, 8 failures), binding propagation (`red-binding-propagation.log`, 8 failures), unsafe logging (`red-audit-logging.log`, 1 failure), and expiration during in-flight read (`red-inflight-and-compatibility.log`, 1 failure). Adjacent RED XML directories retain original results except the following archival defect: **`red-config-subject-xml/` is mislabeled and actually contains metadata RED XML**. Original config/subject XML was overwritten before archival; the seven original behavioral console failures remain. No reconstructed XML or later rerun is claimed as historical evidence. Root acknowledged this defect. Initial setup/compile failures are retained separately and are not behavioral RED.

## Genuine downloader, refresh and outage evidence

`F/fixture-contract.json` identifies the uniquely owned service `delta_engine_6960fcbba49b`, definition `delta_definition_6960fcbba49b`, metalake `delta_metalake_6960fcbba49b`, reader and role. The download account is a distinct ROLE_USER authorized for secure policy/role downloads, separate from Admin, query and metadata identities.

`E/live-downloader-safe/` records a fresh actual Kyuubi/Ranger JVM with Log4jLoggerFactory from log4j-slf4j2-impl 2.20.0 and audit INFO disabled. Its observed policy/role sequence was initial 8/12 allow, policy disabled 9/12 deny, policy restored 10/12 allow, role membership removed 11/13 deny, restored 12/14 allow, binding mismatch 13/14 reject, binding restored 14/14 allow. Closing one lease preserved allow; final close recorded policy version -1 and exit 0. These were real Admin mutations by root and genuine Java downloader refreshes. Role mutations also increment Ranger policy versions; the independent role-version-only behavior is separately exercised with test-injected real role models, not mislabeled as live-only refresh. Actual subject group-to-role behavior is tested with real Ranger mappings; the live fixture UGI had empty groups.

Live assertions cover identical names in delta_a/delta_b, preserved catalog authorization split, denied b.secret, a.secret CUSTOM upper mask, b.secret NULL mask decision without granting access, and the a row filter versus b no filter. Root independently verified all six mutations were restored except version/timestamp counters (`F/mutation-field-check.json`). No shared service shutdown occurred.

`E/live-downloader-safe/launched-class-artifact-hashes.json` records the exact loaded artifacts; they stayed unchanged until JVM 89844 drained. Later metadata TTL changes are covered by final unit tests, not retroactively attributed to that launch. Original unsafe JVM 39438 was also drained.

`F/outage-contract.json` supplies isolated unreachable endpoint/cache inputs. Each startup verified connection refusal. `E/outage-cached/` genuinely loaded canonical last-good policy/roles 14/14, passed initial assertions, and exited 0. `E/outage-empty/` rejected readiness at -1/-1 and exited 0. Both exit records report `credentialFoundInLog=false`. Last-good availability and post-refresh revocation are the contract; instantaneous or reused-plan revocation is not claimed.

## Dependency and helper qualification

Plain Kyuubi authz 1.10.2 and the complete Ranger 2.8.0 closure are pinned, including plugin-classloader/audit split artifacts. `E/selected-runtime-artifacts.json` records all 42 selected artifacts and SHA256s; `E/sources/sources.json` records pinned source provenance. Runtime origin checks inspect actual Maven artifact identity and reject duplicate class resources. Spark/Scala/Hadoop/Delta remain deployment provided.

The unchanged base Delta shadow JAR was rebuilt once for concrete helper qualification (`E/base-runtime-build.log`). SHA256: `286c255f95639935a7eaf183b36b833e5d331db128376a2e39348b3ac7af2439`. It contains no Ranger/authz classes and remains sole supplier of the 33 overlapping Kyuubi utilities. `E/linkage-inspection.json`, `E/javap/`, `E/utility-command-2.json` and `E/utility-executed.json` establish retained nonpublic member descriptors and executed semantic-version, reflection, service-loader, constructor, configuration and quoting helpers. Executed private Guava/publicsuffix ABI checks passed. Companion duplicate utility dependencies are excluded only on this evidence.

## Remaining gates

Task 3 wrapper/discovery and fresh-plan extraction/admission/rewrites are not implemented here. Task 4 companion packaging, deployed all-four Spark/Delta pairs, real Gravitino transport/application discovery/current-actor acceptance, and an authenticated token query bridge remain full-goal gates. Standard masks beyond CUSTOM/NULL remain unsupported. No DML/DDL, streaming, cached-plan, arbitrary-job or full Delta security claim is made. Existing provider unchecked-test-cast and repository Gradle/JVM warnings remain; no new production compile warning was introduced. Root owns independent review and all subsequent integration decisions.
