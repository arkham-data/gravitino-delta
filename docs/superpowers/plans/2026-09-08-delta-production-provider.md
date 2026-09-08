# Delta Production Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task after its reviewed prerequisites. Steps use checkbox (`- [ ]`) syntax for tracking. Root assigns one implementation task at a time and performs independent spec/code review between tasks; implementers do not spawn agents.

**Goal:** Ship a real `lakehouse-delta` ServiceLoader provider with durable registration, observational adoption, authenticated recovery, checkpoint-aware native metadata refresh, and existing REST transport.

**Architecture:** The Java-8 shared module carries immutable commands/receipts and existing v1/v2 correlation values. The server provider owns immutable catalog policy, existing managed TABLE/SCHEMA entities, per-operation actor authorization, isolated native reads, and full-row CAS transitions. Only published rows are discoverable; existing create ownership hooks remain outside provider CAS and require explicitly sequenced request completion before replay/publication.

**Tech Stack:** Java server connector, Java 8 shared protocol, Jackson, Gravitino managed relational EntityStore and REST client/test server, Kernel API/defaults/storage 3.3.0 or 3.3.2, Hadoop/S3A 3.3.6, JUnit 5, Mockito test-only.

**Spec:** `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`. Consume completed native-reader interfaces from `docs/superpowers/plans/2026-09-08-delta-native-metadata.md` and completed schema-contract Task 1 interfaces from `docs/superpowers/plans/2026-09-08-delta-schema-contract.md`. State research `/private/tmp/delta-production-state-contract-20260908.md` is background; its final section 9 overrides earlier alternatives. This plan adopts the explicit conservative foreign-marker ruling below.

## Global Constraints

- Do not modify core, API, server, client, or storage-schema source. Build wiring outside those source trees is allowed.
- Preserve existing generic catalog behavior and default connector dependency isolation.
- No shared deployment, remote push, merge, or PR creation is included. Preserve local evidence and unrelated work.
- Shared main code stays Java 8 compatible and depends only on Jackson; no Spark, Kernel, Hadoop, Gravitino core, test provider or fixture dependency.
- Server production runtime has no Spark dependencies. Spark adapter/runtime implementation and four-pair Spark qualification are separate root-owned work after this plan.
- Exact Kernel API/defaults/storage pins are 3.3.0 or 3.3.2 together, Hadoop 3.3.6. Consume reader guard for active/malformed coordinated-commit configuration; never claim coordinator-aware discovery.
- Ranger 2.8.0 remains the target; JCasbin REST tests and authorization-plugin loading do not establish downloaded-policy Spark enforcement. Do not claim that milestone here.
- Catalog administrators are trusted immutable storage/root/recovery configurators. Ordinary table/schema/command requests cannot override storage settings or roots. Disable credential backfill for this provider.
- No synthetic BooleanSupplier handoff or fake verifier in production; no test provider controls promoted to main. A durable controller attestation is not an independently verified writer fence.
- Capture Gravitino principal/authorization before native-reader Hadoop doAs; no authorization/entity-store/receipt work inside that Subject. Existing reader owns/isolates its resources; close only owned reader resources.
- No full native configuration map in durable table properties or public response. Store a SHA-256 digest of its bounded complete contents plus the exact safe allowlist below. Raw native schema/protocol and Gravitino projection remain preserved as specified.
- ADOPT accepts no marker or a strictly decodable existing v1/v2 marker as immutable provenance. A present malformed/unrecognized marker fails with sanitized `INVALID_ADOPTION_MARKER` **before** storing ADOPTING. Never repair/write/remove the native marker; its foreign identity is not adoption authority.
- No REPAIR_INITIAL_OWNER command, owner setter, DROP extension, purge, name reclamation, rename or cascade across retained rows. ABORT retains ABANDONED and never restores an old replacement publication on missing logs.
- One source implementer and one Gradle owner at a time. Root supplies/owns live MinIO configuration and fixture lifecycle; all production S3 paths remain read-only here. Local unit fixtures can write only their unique test directories. No live services started without root's task assignment.
- Javadoc for new public/protected members; normal Java imports; Google Java formatting; meaningful unit tests for new logic. Preserve RED/GREEN logs and original XML; report actual executed counts, versions, failures and limits.

## File ownership and dependencies

Task 1 changes `delta/delta-common` only and depends on finalized `DeltaSchemaContract`/v2 marker interfaces. Task 2 adds server policy helpers and build dependencies. Task 3 adds state/storage operations consuming Tasks 1–2 and the completed reader. Task 4 connects the provider SPI, packaging and real REST tests. Each task ends at an independently reviewable test gate; no broad unrelated refactor.

Existing reader consumed unchanged:

```java
public final class DeltaMetadataReader implements AutoCloseable {
  public DeltaMetadataReader(Configuration trustedConfiguration);
  public DeltaSnapshotMetadata latest(String normalizedLocation);
  public DeltaSnapshotMetadata atVersion(String normalizedLocation, long version);
  public DeltaCommitEvidence verify(DeltaOperationMarker expected);
  public void close();
}
```

`DeltaSnapshotMetadata` supplies `location()`, `version()`, `uuid()`, `schemaJson()`, `columns()`, `partitionColumns()`, `configuration()`, `description()`, `createdTime()`, protocol versions and feature sets. No reader internal/Kernel types escape. Schema-contract plan supplies `DeltaSchemaContract.spark35Delta33NameMapping(OperationMode, String)`, `DeltaOperationMarker.createWithSchemaContract(...)`, `replaceWithSchemaContract(...)`, and `marker.schemaContract()` in addition to unchanged v1 factories. Do not implement provider consumption against hypothetical different signatures; root resolves a prerequisite mismatch before its dependent task begins.

## Task 1: Shared production command and receipt contract

**Create:**
- `delta/delta-common/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/registration/DeltaRegistrationIdentity.java`
- `delta/delta-common/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/registration/DeltaRegistrationCommand.java`
- `delta/delta-common/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/registration/DeltaRegistrationReceipt.java`
- Corresponding `TestDeltaRegistrationIdentity.java`, `TestDeltaRegistrationCommand.java`, `TestDeltaRegistrationReceipt.java` in that module's `src/test/java/org/apache/gravitino/catalog/lakehouse/delta/registration/`.

**Modify:** shared `README.md`; `DeltaJson.java` only if a package-private bounded canonical JSON helper is required, with existing marker bytes/tests unchanged. No schema-contract semantics changed in this task. Resolve the prior deferred style minor in `TestDeltaSchemaContract.java` by moving its three static constants before the first test method; no test or contract behavior change.

**Interfaces:**

```java
DeltaRegistrationIdentity.of(long id, long generation, String token);
// Getters: id(), generation(), token(); immutable value equality/hashCode.

DeltaRegistrationCommand.reserve(String token, String runId, String exactSchemaJson);
DeltaRegistrationCommand.reserve(
    String token, String runId, DeltaSchemaContract schemaContract);
DeltaRegistrationCommand.adopt(
    String token, String location, String retiredRunId, String cutoverReference);
DeltaRegistrationCommand.beginReplace(
    DeltaRegistrationIdentity expected, String newToken, String runId, String exactSchemaJson);
DeltaRegistrationCommand.beginReplace(
    DeltaRegistrationIdentity expected, String newToken, String runId,
    DeltaSchemaContract schemaContract);
DeltaRegistrationCommand.publish(DeltaRegistrationIdentity expected);
DeltaRegistrationCommand.status(DeltaRegistrationIdentity expected);
DeltaRegistrationCommand.statusByToken(String token, String writerRunId);
DeltaRegistrationCommand.administrativeStatus();
DeltaRegistrationCommand.handoff(
    DeltaRegistrationIdentity expected, String runId, String reference);
DeltaRegistrationCommand.abort(DeltaRegistrationIdentity expected);
// Getters: operation(), expected(), token(), replacementToken(), runId(),
// location(), exactSchemaJson(), schemaContract(), reference(); optionals where absent.
// encode(), static decode(String), and public COMMAND_PROPERTY constant:
// "delta.registration.command".

DeltaRegistrationReceipt.fromProperties(Map<String, String> properties);
DeltaRegistrationReceipt.of(
    DeltaRegistrationIdentity identity, DeltaRegistrationCommand.Operation operation,
    DeltaRegistrationReceipt.State state, String runId,
    DeltaRegistrationReceipt.Evidence evidence, String reason,
    Optional<DeltaOperationMarker> marker, Optional<String> location,
    Optional<String> nativeId, OptionalLong nativeVersion);
// Getters for the above; toProperties() returns immutable sanitized receipt fields.
```

Command.Operation is exactly RESERVE, ADOPT, BEGIN_REPLACE, PUBLISH, STATUS, STATUS_BY_TOKEN, ADMINISTRATIVE_STATUS, HANDOFF, ABORT. Receipt.operation identifies the operation-opening command (RESERVE, ADOPT or BEGIN_REPLACE), not a later STATUS/PUBLISH request; marker operation remains its existing CREATE/REPLACE enum. States exactly RESERVED, ADOPTING, REPLACING, PUBLISHED, QUARANTINED, ABANDONED. Evidence exactly NONE, EXACT_NATIVE_COMMIT, EXISTING_NATIVE_SNAPSHOT, UNRESOLVED. Reason is a closed sanitized code produced by server logic, not arbitrary upstream exception text. No caller actor, base UUID/version, candidate version, or location field exists on RESERVE. Schema bindings are mutually exclusive: v1 exact schema JSON **or** v2 DeltaSchemaContract. Command wire version 1 is independent of contained marker/schema contract versions. The server—not the command—chooses the authoritative marker.

Emit canonical JSON with exact per-operation key sets; decode may canonicalize insignificant object key order/whitespace, while duplicate/unknown fields and invalid numeric representations fail. Define maximum command UTF-8 size 512 KiB, token/run/reference bounds 1024/4096/4096 bytes and location 8192 bytes (the existing marker location bound); schema limits remain existing marker/schema-contract limits. IDs must be positive for expected references, generation nonnegative, operation increment overflow checked server-side. Duplicate/unknown fields, duplicate schema bindings, malformed UTF-8, non-integral/overflowing identity numerics, blank values and extra caller authority fields fail. `statusByToken` carries exactly the current operation token and writerRunId (ADOPT uses recorded retiredWriterRunId), with no numeric ID/generation, and is read-only. `administrativeStatus` has no expected identity/token and must be privileged at the server.

Receipt prefix `delta.registration.receipt.` is reserved and visible. State-specific validation: pending native CREATE/REPLACE receipts contain the matching server marker; ADOPTING has no marker; PUBLISHED exposes identity/location/native ID/version and evidence; ABANDONED and QUARANTINED never masquerade as readable tables. No raw foreign marker or full native configuration in receipts. Correlation tokens are not bearer authority.

Command JSON uses exactly `version`, `operation`, and the following operation-specific fields. RESERVE: `token`, `runId`, exactly one of `schema`/`schemaContract`; ADOPT: `token`, `location`, `runId`, `reference`; BEGIN_REPLACE: `expected`, `replacementToken`, `runId`, one schema binding; PUBLISH/STATUS/ABORT: `expected`; STATUS_BY_TOKEN: `token`, `runId`; ADMINISTRATIVE_STATUS: none; HANDOFF: `expected`, `runId`, `reference`. `expected` is exactly `{id,generation,token}`. ADOPT parameter names retiredRunId/cutoverReference and statusByToken writerRunId use the generic runId/reference wire fields and getters. token() is populated only for RESERVE/ADOPT/STATUS_BY_TOKEN; replacementToken() only for BEGIN_REPLACE; all inapplicable getters are empty. HANDOFF names the existing stored operation run.

Receipt suffixes are exactly `id`, `generation`, `token`, `operation`, `state`, `runId`, `evidence`, `reason`, `marker`, `location`, `nativeId`, `nativeVersion`. Reject unknown reserved suffixes and malformed/null values; ignore unrelated table properties. Decimal strings are canonical (no signs, leading zeroes except zero, or overflow). Correlation bounds match commands; nativeId is nonblank valid Unicode up to8192 UTF-8 bytes and remains opaque. Marker decoding retains its256KiB bound. Reject partial native ID/version tuples. Validate these exact combinations:

| Receipt state | Opening operation / optional fields | Evidence |
| --- | --- | --- |
| RESERVED | RESERVE; CREATE marker required; location/native tuple absent | NONE |
| ADOPTING | ADOPT; marker/location/native tuple absent | NONE |
| REPLACING | BEGIN_REPLACE; REPLACE marker required; location/native tuple absent | NONE |
| PUBLISHED | RESERVE/BEGIN_REPLACE; marker absent; location/native ID/version required | EXACT_NATIVE_COMMIT |
| PUBLISHED | ADOPT; marker absent; location/native ID/version required | EXISTING_NATIVE_SNAPSHOT |
| QUARANTINED, pending origin | Opening operation retained; its CREATE/REPLACE marker retained or absent for ADOPT; location/native tuple absent | UNRESOLVED |
| QUARANTINED, published-refresh origin | Opening operation retained; marker/location/native tuple absent | Prior EXACT_NATIVE_COMMIT or EXISTING_NATIVE_SNAPSHOT, consistent with opening operation |
| ABANDONED | Opening operation retained; marker/location/native tuple absent | UNRESOLVED, or EXACT_NATIVE_COMMIT for native operation / EXISTING_NATIVE_SNAPSHOT for ADOPT only when actually observed |

Every exposed marker must match receipt ID/generation/token and opening-operation CREATE/REPLACE kind; current receipt ID remains positive. Native pending location is obtained from its marker. No foreign ADOPT marker is exposed. Only PUBLISHED receipts are readable; an exact evidence label on a terminal receipt is historical evidence, never current visibility or authority.

Keep String reason in the proposed API but accept only the finite set `NONE`, `OWNER_INITIALIZATION_REQUIRED`, `NATIVE_EVIDENCE_AMBIGUOUS`, `NATIVE_READ_UNAVAILABLE`, `ADOPTION_BASELINE_CHANGED`, `NATIVE_ID_CHANGED`, `NATIVE_VERSION_REGRESSION`, `NATIVE_METADATA_INCONSISTENT`, `UNREPRESENTABLE_NATIVE_METADATA`, `ABANDONED_BY_RECOVERY`. Ordinary pending/PUBLISHED uses NONE; RESERVED/ADOPTING may additionally report OWNER_INITIALIZATION_REQUIRED without changing the row. QUARANTINED requires one of the seven native-failure codes; ABANDONED requires ABANDONED_BY_RECOVERY. Provider normalizes reader diagnostics into these codes. INVALID_ADOPTION_MARKER and REGISTRATION_CHANGED remain request/CAS failures, not receipt reasons. Tests cover every valid matrix branch and neighboring invalid combinations, marker bindings, unknown fields, canonical numbers, Unicode and size boundaries.

- [x] **Step 1: Add adversarial tests and run RED.** Include the following concrete roundtrip and negative shape tests, parameterized across all operations and schema-binding overloads:

```java
DeltaRegistrationCommand request =
    DeltaRegistrationCommand.reserve("op-1", "run-17", "{\"type\":\"struct\",\"fields\":[]}");
assertEquals(request.encode(), DeltaRegistrationCommand.decode(request.encode()).encode());
assertFalse(request.location().isPresent());
assertFalse(request.expected().isPresent());
assertThrows(IllegalArgumentException.class,
    () -> DeltaRegistrationCommand.decode(
        "{\"version\":1,\"operation\":\"RESERVE\",\"actor\":\"admin\"}"));
```

Run `./gradlew :delta:delta-common:test --tests '*TestDeltaRegistration*'`; expected RED is missing new production API, not a Gradle environment failure. Capture log/XML after correcting any setup-only failure.
- [x] **Step 2: Implement immutable factory validation, codec and receipt projection.** Keep fields final and defensive; route exact schema canonicalization through existing v1 helper and v2 through DeltaSchemaContract. Do not expose a writable arbitrary JSON map. Demonstrate decode rejects both schema bindings, unknown v2 kind/policy and actor/version/base/location injection on RESERVE; fromProperties must reject malformed reserved receipt fields rather than silently defaulting state.
- [x] **Step 3: Run shared contract gate and inspect isolation.** Run `./gradlew :delta:delta-common:test :delta:delta-common:javadoc :delta:delta-common:spotlessApply :delta:delta-common:jar`; run `./gradlew :delta:delta-common:dependencies --configuration runtimeClasspath`. All existing v1 and finalized v2 tests remain executed. Runtime remains Jackson-only, main Java bytecode remains 8.
- [x] **Step 4: Document wire/receipt examples and review/commit the task.** README shows exact POST/PUT property usage, v1/v2 difference, token-not-authority and pending visibility. Run `git diff --check`; stage only the listed shared files and commit `feat(delta): add production registration commands and receipts`. Root performs independent spec/code review before Task 2.

Reviewed Task1 complete: implementation `342c5f0400a1f6ccf24478b742b348e4290202ce`, braces-only test fix `fce63b257b653034949cbdd21e7087aa66f2975e`; independent spec/code review and scoped fix review approved. Full shared gate209 tests, zero failures/errors/skips, strict Javadoc/format/JAR; eighteen Java8 classes and Jackson-only runtime. The style amendment passed scoped formatting/compilation without rerunning the test suite. Evidence archive `/private/tmp/gravitino-delta-provider-wire-evidence-fce63b257.tar.gz`, SHA-256 `2da86dcbb2948ea52afb826c382941bae37ca38a5ac2f2d61546e9d9776fb45f`; reports retained in this plan SDD workspace.

## Task 2: Trusted catalog policy, authorization and native observation reduction

**Create in `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/`:** `DeltaCatalogConfig.java`, `DeltaCatalogPropertiesMetadata.java`, `DeltaSchemaPropertiesMetadata.java`, `DeltaTablePropertiesMetadata.java`, `DeltaLocationPolicy.java`, `DeltaAuthorization.java`, `DeltaNativeObservation.java`, `DeltaStorageAdmission.java`. Test counterparts: `TestDeltaCatalogConfig`, `TestDeltaLocationPolicy`, `TestDeltaAuthorization`, `TestDeltaNativeObservation`, `TestDeltaStorageAdmission` in matching module test package. Narrowly extend `metadata/DeltaKernel33MetadataAdapter.java`, `DeltaMetadataReader.java` and covering tests for desired-schema projection as described below.

**Modify:** `catalogs/catalog-lakehouse-delta/build.gradle.kts` for `compileOnly` common/core/catalog-common as required, production `implementation(libs.hadoop3.aws)`, Jackson implementation for provider observation serialization if needed, test API/common/core/Mockito dependencies. Existing reader/shared dependencies and exact Kernel pins remain. Update provider README.

**Interfaces (provider-package private unless required by BaseCatalog overrides):**

```java
DeltaCatalogConfig.from(Map<String, String> trustedCatalogConfiguration);
Configuration DeltaCatalogConfig.storageConfiguration(); // defensive copy, never log
DeltaLocationPolicy DeltaCatalogConfig.locations();
Set<String> DeltaCatalogConfig.recoveryPrincipals();

String DeltaLocationPolicy.newTableLocation(NameIdentifier ident, long tableId);
String DeltaLocationPolicy.admittedExistingLocation(String suppliedLocation);

DeltaAuthorization(); // production GravitinoEnv-backed
String DeltaAuthorization.currentActor(); // reject anonymous/blank
void DeltaAuthorization.requirePrivileged(NameIdentifier ident);
boolean DeltaAuthorization.isPrivileged(NameIdentifier ident, String actor);
void DeltaAuthorization.requireInitialOwner(NameIdentifier ident);
void DeltaAuthorization.requirePendingReplayOwner(NameIdentifier ident, String creator);

DeltaNativeObservation.from(DeltaSnapshotMetadata metadata);
String DeltaNativeObservation.configurationDigest();
Map<String, String> DeltaNativeObservation.safeConfiguration();
// Other detached getters mirror needed snapshot values; raw configuration not retained.
```

Policy keys: required immutable `delta.warehouse`; optional immutable `delta.adoption-roots` and `delta.recovery-principals`, each a strict JSON string array (defaults `[]`); immutable `delta.storage-credential-mode` in `static`, `session`, `instance`, default `instance`; standard S3 `s3-endpoint`, `s3-region`, `s3-path-style-access`, `s3-access-key-id`, `s3-secret-access-key`, plus hidden `s3-session-token` for session mode. Require both static keys together; session mode additionally requires token; instance mode prohibits supplied static credentials. Use fixed provider classes SimpleAWSCredentialsProvider, TemporaryAWSCredentialsProvider or IAMInstanceCredentialsProvider for respective modes; no arbitrary class-name property from user requests. Read secrets from already-resolved initialize configuration, declare keys hidden, and never backfill them into catalog responses. Endpoint must be a well-formed http(s) URI without userinfo/query/fragment; plaintext HTTP only explicit trusted local endpoint setup, not synthesized as production default.

Canonical S3 policy: accept `s3` alias and `s3a`, normalize to `s3a` once; lowercase DNS bucket authority, reject port/userinfo/query/fragment/percent escapes/dot segments/empty segments/traversal and unsupported schemes. Trailing root slash normalized consistently. New path `${warehouse}/${schema}/${tableId}` uses positive injected ID and validates schema identifier segments; table/schema request `location` is not accepted. Adoption roots use exact bucket plus path-segment prefix boundaries; `s3a://bucket/allowed2` cannot satisfy `.../allowed`. No global uniqueness claim; administrator attestation/platform admission owns location exclusivity. A package-private policy test helper may exercise local paths for direct unit fixtures; do not expose a production `allow-file` flag or fake filesystem override.

Property metadata exposes exactly command transport; internal prefix `delta.registration.internal.` is reserved+hidden, receipt prefix `delta.registration.receipt.` is reserved+visible. Command transport remains mutable and nonreserved; it is accepted as an operation request and is never persisted as an ordinary editable table property. Keep all storage settings, including credentials, endpoint, region and path-style selection, immutable. Reuse standard S3 key constants but do not copy their generic mutable PropertyEntry definitions unchanged. Reject schema location/storage keys, table credential/root keys and arbitrary ordinary table alterations. BaseCatalog metadata still permits necessary shared catalog properties (authorization/package/secret metadata) under existing catalog-admin trust. `DeltaCatalogConfig.from` rejects conflicting credential settings and constructs an allowlisted fresh Configuration with S3A implementation/cache enabled; no arbitrary `fs.*`, logstore override or caller-derived Hadoop settings.

Native configuration reduction: compute SHA-256 over sorted UTF-8 keys/values encoded as unambiguous length-prefixed bytes, including **all** reader-bounded configuration entries. Persist only digest plus safe exact keys `delta.columnMapping.mode`, `delta.columnMapping.maxColumnId`, `delta.appendOnly`, `delta.enableChangeDataFeed`, `delta.enableDeletionVectors`. Validate expected mode/integer/boolean syntax; absent stays absent. No prefix-based copying. The digest is comparison data, not a signature. For ADOPT, inspect the transient configuration marker: absent allowed; present requires strict DeltaOperationMarker.decode (v1 or finalized v2), otherwise fail sanitized before row put. Retain only its digest and nonsecret provenance classification in durable properties; no foreign token in receipts. Raw schema/protocol/ordered partitions are preserved; configuration remains transient even in toString/error output.

Durable representation is narrower than native reader observation. Add provider-local DeltaStorageAdmission using the actual JsonUtils.anyFieldMapper serialization used by POConverters, without importing relational PO converters into provider logic or changing the shared mapper. Common backend limits: direct schema/table/top-level column names128 Java UTF-16 units; schema comments256; top-level column comments4096; table format64. Reject unpaired surrogates and actual NUL in directly stored strings. Nested names/comments are embedded in column_type JSON and governed by its encoded size, not separate VARCHAR limits. Serialized column types/default expressions allow65535 UTF-8 bytes each; table properties/comment/partitioning/distribution/sort/index/audit fields allow16777215 bytes each. Serialize types directly and default values via DTOConverters.toFunctionArg exactly as core does; default-not-set remains SQLNULL. Use a bounded counting Writer so escaping/multibyte characters and surrogate pairs split across writes are measured without allocating an unbounded string. Check final containing properties JSON as well as internal state encoding, because embedding raw schema/marker strings escapes them again. Sanitize errors and never truncate or substitute metadata.

Add `public static List<Column> DeltaMetadataReader.projectSchema(String rawSchemaJson)` as a provider-only, I/O-free desired-schema facade. Bound its command schema to the existing128KiB verification limit, validate exact runtime, and call `DataTypeJsonSerDe.deserializeStructType` only inside the existing sole-internal-import adapter, then reuse DeltaSchemaProjection. No new schema parser, native snapshot fabrication, public Kernel type or storage Subject callback. Return immutable defensive projection. RESERVE/BEGIN_REPLACE validate this authoritative command projection before initial put/CAS; ordinary DTO columns remain empty as prescribed. ADOPT validates full observed projection before ADOPTING. Reduce native protocol through DeltaProtocolDescriptor.of to apply its4096-feature/8192-byte-name bounds; enforce the receipt native-ID bound without truncation. Preserve optional createdTime as an opaque long, including negative values; no new timestamp semantic restriction. Every final table/schema candidate must pass stored-field bounds before put/update; actual PUBLISH/refresh observations are rechecked, since they can differ from requested schema. Oversized native metadata after a write yields a small full-row-CAS quarantine reason UNREPRESENTABLE_NATIVE_METADATA while retaining the last representable observation/correlation. If even diagnostic candidate fails admission, leave the old row and fail closed. This is explicit no-core durable admission, not a claim that native Delta cannot represent larger metadata.

Source/API and executable counting guidance: `/private/tmp/delta-provider-storage-limits-preflight-20260908.md`. Include boundary tests128/129,4096/4097,65535/65536 encoded bytes, multibyte/escaping amplification, nested versus top-level comments, NUL/malformed surrogates, before-reserve and before-ADOPT failure without row creation, and final candidate/quarantine bounds. Task2 tests admission helpers directly; Task3 adds the actual before-RESERVE/before-ADOPT/no-row and quarantine lifecycle assertions once registration operations exist. Native reader300K comment preservation must continue passing while provider admission rejects that unrepresentable top-level comment.

Authorization uses current authenticated Gravitino principal. `isPrivileged(ident, actor)` returns false when actor differs from the current principal name; otherwise use the original authenticated Principal with its groups, never a name-only reconstructed principal. The existing service-admin check reads the current Subject and must not be combined with ownership checks for a different actor. Existing REST applies CREATE/MODIFY privileges; do not replace that gate with token/row actor checks. Additional privileged gate uses real catalog/metalake ownership or service admin through existing GravitinoAuthorizer, or configured recovery principal for recovery-specific paths. A recovery-principal allowlist alone does not confer ADOPT/configuration rights. Reject disabled authorization/null ACL dispatchers on privileged branches: PassThroughAuthorizer returns true for owner/admin and is not proof. Owner presence/read uses internal owner dispatcher, never setOwner. Initial publication requires any valid persisted USER or GROUP owner; it does not require the governance owner to equal the operation actor. Pending replay requires currentActor==creator and an absent owner or USER owner with that exact creator name; a GROUP with the same name is insufficient. Missing dispatcher is a failure, not absent ownership. Authorization-disabled publication has no owner-presence proof and remains unsupported. Explicitly configured known PassThroughAuthorizer is also rejected even if the enable flag is true; compare its actual class/ancestry name if needed to avoid a new server-common production dependency. Custom administrator-selected authorizers remain part of the existing trusted server boundary.

- [ ] **Step 1: Add focused RED tests.** Include canonical S3 alias equivalence, prefix escape and malicious URI rejection; missing/partial credentials; immutable property update rejection through existing metadata helpers; anonymous/disabled-auth admin denial; group-owner API semantics via existing authorizer; and secret-safe observations:

```java
assertEquals(
    "s3a://warehouse/tables/sales/42",
    config.locations().newTableLocation(NameIdentifier.of("m", "c", "sales", "t"), 42));
assertThrows(IllegalArgumentException.class,
    () -> config.locations().admittedExistingLocation("s3a://warehouse/allowed2/t"));
assertFalse(observation.safeConfiguration().containsKey("vendor.secret"));
assertNotEquals(first.configurationDigest(), changedSecret.configurationDigest());
```

Test observation native metadata using existing reader on isolated local fixtures (or Mockito final reader DTO observation in unit-only controls), never production synthetic evidence factories. Run `./gradlew :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaCatalogConfig' --tests '*TestDeltaLocationPolicy' --tests '*TestDeltaAuthorization' --tests '*TestDeltaNativeObservation'` and capture meaningful RED.
- [ ] **Step 2: Implement policy/metadata/auth/reduction helpers.** The field/state restrictions above are the code contract. Use full digest for same-version/adoption-baseline comparison, retain no raw configuration map in helper fields, and make secret-bearing config toString unavailable/sanitized. Validate owner outside Hadoop scope and use actual root/catalog identity from NameIdentifier, never command metadata. Do not mutate global Hadoop settings or JVM credentials.
- [ ] **Step 3: Run helper and unchanged-reader regression checks.** Run `./gradlew :catalogs:catalog-lakehouse-delta:test :catalogs:catalog-lakehouse-delta:javadoc :catalogs:catalog-lakehouse-delta:spotlessApply -PdeltaKernelVersion=3.3.0`. Repeat affected tests with `-PdeltaKernelVersion=3.3.2` once after source stabilizes. Inspect runtime dependencies for matched S3A/common versions and no Spark.
- [ ] **Step 4: Document exact keys/trust boundary and review/commit.** Provider README contains an example using secret references, required root admission/cutover assumptions and safe-property table. Run `git diff --check`, stage only task files, commit `feat(delta): enforce trusted storage and registration policy` and obtain root's task review.

## Task 3: Durable managed registration and published metadata refresh

**Create in provider main package:** `DeltaRegistrationState.java`, `DeltaRegistrationStore.java`, `DeltaRegistrationOperations.java`, `DeltaColumnIdentities.java`.

**Create tests in matching provider test package:** `TestDeltaRegistrationState.java`, `TestDeltaRegistrationOperations.java`, `TestDeltaColumnIdentities.java`. Use existing native reader fixture patterns in provider test sources; do not import generic `registrationTestArtifacts` or test ServiceLoader providers.

**Interfaces:**

```java
// Package-private immutable validated row record:
DeltaRegistrationState.from(TableEntity row);
TableEntity DeltaRegistrationState.entity(); // defensive complete row
DeltaRegistrationReceipt DeltaRegistrationState.receipt();

DeltaRegistrationStore(EntityStore store);
DeltaRegistrationState DeltaRegistrationStore.load(NameIdentifier ident);
DeltaRegistrationState DeltaRegistrationStore.insert(TableEntity candidate); // overwrite=false
DeltaRegistrationState DeltaRegistrationStore.replace(
    DeltaRegistrationState inspected, TableEntity candidate); // complete-row compare only
List<DeltaRegistrationState> DeltaRegistrationStore.list(Namespace namespace);

DeltaRegistrationOperations(
    EntityStore store, SupportsSchemas schemas, IdGenerator ids,
    DeltaMetadataReader reader, DeltaCatalogConfig config, DeltaAuthorization authorization);
// Extends ManagedTableOperations, overrides all create/load/list/alter/drop/purge methods
// plus protected store()/schemas()/idGenerator(). Owns no globally shared client.

List<ColumnEntity> DeltaColumnIdentities.project(
    DeltaRegistrationState prior, DeltaNativeObservation observed,
    IdGenerator ids, AuditInfo observationAudit);
```

Recovery allowlist checks must not bypass enabled-auth/current-subject guards through Boolean short-circuiting: evaluate `authorization.isPrivileged(ident, currentActor)` first (which rejects unavailable/disabled authorization), then combine that result with the configured recovery-principal membership for recovery-specific operations. ADOPT continues to require actual privilege independently of that allowlist.

Store helper must not accept arbitrary I/O callbacks. Concrete update shape:

```java
return store.update(ident, TableEntity.class, Entity.EntityType.TABLE, current -> {
  if (!current.equals(inspected.entity())) {
    throw new IllegalStateException("REGISTRATION_CHANGED");
  }
  return candidate;
});
```

Detach mutable maps/arrays before I/O; row codec validates state/kind/required fields/receipt consistency and never accepts arbitrary legacy rows as published. Candidate construction, native reads, column-ID allocation and authorization happen before callback. Carry exact create/replacement marker in internal state, not an externally writable “verified” field. Persist a closed internal quarantineOrigin discriminator (PENDING_PUBLICATION or PUBLISHED_REFRESH only while QUARANTINED), and validate it against the opening kind/receipt evidence/marker. HANDOFF/recovery PUBLISH/ABORT accept only pending states or PENDING_PUBLICATION quarantine. PUBLISHED_REFRESH quarantine preserves completed historical evidence and cannot rebind or republish through the pending recovery path; its later remediation requires a separate qualified lifecycle operation. State name alone is insufficient recovery authority.

Durable transitions:

| Operation | Required source | Result / exact rules |
|---|---|---|
| RESERVE POST | absent | Read injected StringIdentifier, derive admitted path, actor=current request, generation0/run/token; build server v1 or v2 CREATE marker; insert RESERVED. Request has only command and injected ID, empty Gravitino columns/comment/partitions. |
| ADOPT POST | absent | Privileged actor, admitted source, cutover attestation; read latest, reduce configuration, reject malformed marker; insert ADOPTING with pinned native ID/version/schema/protocol/config digest and no operation marker. |
| Exact pending POST replay | RESERVED or ADOPTING only | Same actor/canonical command/run/token, same initial generation; existing owner absent or same creator. Return durable receipt/ID without native read or second logical ID. Reject all published/changed-state replays. Request completion/drain precondition is documented, not inferred from timeout. |
| BEGIN_REPLACE PUT | PUBLISHED | Existing REST MODIFY gate; **new caller** becomes operation actor independent of old actor/owner. Check expected ID/gen/token; read latest same native ID/nonregressing version; store actual base, new server v1/v2 marker, new token/run and gen+1 in full-row CAS; owner/table ID unchanged. |
| PUBLISH CREATE/REPLACE | RESERVED/REPLACING | Expected identity, current operation actor, initial owner exists, no post-failure handoff; exact reader.verify MATCHED and atVersion projection agree on candidate native ID/version/schema. REPLACE expected candidate base+1 and changed native ID; never ordinary refresh reanchor. Store PUBLISHED/EXACT_NATIVE_COMMIT. |
| PUBLISH ADOPT | ADOPTING | Stored adopter retains current privileged admission; initial owner relation exists; fresh latest equals pinned native ID/version/schema/protocol/config digest. Store PUBLISHED/EXISTING_NATIVE_SNAPSHOT. No marker or exact-commit verifier. Drift quarantines. |
| STATUS / statusByToken / admin STATUS | any retained row | No native read or mutation. STATUS validates full identity. statusByToken validates current token+run exactly, authorizes stored operation actor or privileged recovery/admin plus outer REST gate, and returns the missing numeric ID after a lost POST reply. ADOPT uses retiredRunId. Admin STATUS can omit all receipt identity for a known identifier. A missing initial owner may still force privileged administrativeStatus because outer REST can deny ordinary actor. Sanitized receipt only; mutating commands never drop their expected-ID checks. |
| HANDOFF | pending or pending-operation quarantine | Privileged recovery/admin + existing REST gate, exact ID/gen/token/stored run; persist authenticated attester/time/reference. Distinct from ADOPT's original cutover. No booleans or control-plane proof invented. |
| Recovery PUBLISH | pending-operation quarantine / pending | Matching durable handoff + privileged authority; exact native verification or ADOPT pinned-baseline reobservation, according to stored kind. Preserve operation actor; record recovery caller separately. Unexpected-native-ID quarantine cannot rebind automatically. |
| ABORT | pending/pending-origin quarantine | Matching handoff + privileged authority, retain ABANDONED. Native diagnostic may be exact match or unresolved; neither restores prior replacement nor proves noncommit. No physical writes/deletes/name reuse. |

Reader failure/ambiguous exact evidence blocks publication with sanitized quarantine reason; do not catch failure into success. Initial missing owner leaves RESERVED/ADOPTING unchanged with OWNER_INITIALIZATION_REQUIRED, enabling documented serialized pending POST repair. Normal actor cannot publish after post-failure HANDOFF; privileged recovery requires it. Exact completed PUBLISH/ABORT replay returns current sanitized receipt without mutation/native re-verification when same current operation identity/actor authority matches. Refresh retains completed correlation; newer generations reject old replays. Lost BEGIN_REPLACE reply uses STATUS with expected next generation/new token; no extra BEGIN_REPLACE replay exception in this initial implementation.

Refresh only starts from PUBLISHED, before native I/O. Latest same native ID refreshes schema/partitions/protocol/safe configuration+digest and observation version with complete-row CAS, preserving operation identity/actor/correlation and owner. Native ID drift or same-version metadata inconsistency quarantines; never auto-adopt a replacement. Same-version comparison covers exact raw schema, ordered partitions, complete protocol, description/createdTime including absence, and the versioned digest of all bounded native configuration. ADOPT pinning and publication use this same complete observation tuple plus normalized location/native ID/version, including ordered partitions and description/createdTime presence versus value. Absent description differs from present empty description. Projection/safe-property equality alone is insufficient. Persist the digest algorithm/domain version; equal complete observation is a no-op. On row conflict reload at most three times; if state is pending/terminal, return unpublished rather than applying prior native observation. LIST refreshes published candidates only; omit successfully quarantined IDs, fail the list on transient native-read errors rather than silently treating inaccessible entries as absent.

Column identity: trusted raw top-level native columnMapping.id when valid/unique, qualified by native table ID; otherwise exact unchanged logical name. Preserve unchanged logical column IDs across same-native-ID refresh, reorder and metadata evolution; allocate IDs only for additions. Mapped rename preserves ID only by mapping identity, unmapped rename is remove/add. Verified native replacement has changed native ID, so do not equate reassigned mapping IDs to old identities. Preserve creator/create time; observation-triggering reader is not native DML author. Removed columns do not persist in projection. No cross-observation physical-lineage claim for unmapped drop/readd.

- [ ] **Step 1: Write lifecycle RED tests over relational EntityStore and package-private constructor injection.** Use real relational store where available and mocked final DeltaMetadataReader only for isolated race/error unit controls; successful native evidence coverage uses the actual reader/native fixtures. A concrete race test must pause latest read, perform BEGIN_REPLACE, resume refresh and assert pending state/token/actor survive:

```java
TableEntity before = rawTable(ident);
CompletableFuture<Table> loading = loadWithPausedNativeRead(ident);
Table receipt = operations.alterTable(ident, TableChange.setProperty(
    DeltaRegistrationCommand.COMMAND_PROPERTY,
    DeltaRegistrationCommand.beginReplace(identity(before), "next", "run-b", schema).encode()));
resumeNativeRead();
assertThrows(CompletionException.class, loading::join);
assertEquals("REPLACING", DeltaRegistrationState.from(rawTable(ident)).receipt().state().name());
assertEquals("next", DeltaRegistrationState.from(rawTable(ident)).receipt().identity().token());
```

`rawTable`, `loadWithPausedNativeRead`, `resumeNativeRead`, and `identity` are test-class helpers explicitly implemented in `TestDeltaRegistrationOperations`; use CountDownLatch with bounded waits and executor cleanup. Run `./gradlew :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaRegistration*' --tests '*TestDeltaColumnIdentities'`; capture RED.
- [ ] **Step 2: Implement row codec, storage CAS and transitions.** Follow the table exactly; no raw configuration retained in row serialization. Apply DeltaStorageAdmission.checkTableCandidate to every constructed put/update/quarantine candidate before it enters the store; bound both internal row codec and final outer property serialization. Add negative tests for corrupted persisted kind/marker/state, stale IDs/generation/token/run, actor turnover, overflow, completed replay after refresh, adoption-source digest/UUID drift, initial owner absence and ABANDONED with missing replacement logs. Actor B beginning replacement must not call the fixture-style authorize(oldActor) gate before changing actor.
- [ ] **Step 3: Implement refresh/column preservation and run its race tests.** Add same-ID append/schema additions, mapping reorder/rename, native-ID change quarantine, list with pending rows, full-row conflict during HANDOFF/ABORT, and three-conflict exhaustion. Assert table ID/governance relation/operation correlation unchanged and no public/durable `vendor.secret` value. Create metadata fixtures only in test-owned paths; production provider location remains S3-only.
- [ ] **Step 4: Run full provider unit gates on each Kernel pin.** Run `./gradlew :catalogs:catalog-lakehouse-delta:test :catalogs:catalog-lakehouse-delta:javadoc :catalogs:catalog-lakehouse-delta:spotlessApply -PdeltaKernelVersion=3.3.0`; then same test/Javadoc command with `-PdeltaKernelVersion=3.3.2`. Capture counts/XML per pin before the second overwrites outputs. Native replacement fixtures written as protocol-shaped test input must be labeled accurately; Spark native replacement remains separate qualification.
- [ ] **Step 5: Review and commit.** `git diff --check`; verify no owner relation writes or physical deletes occur in production code, no generic-test imports, no missing public/protected Javadoc. Commit only task files with `feat(delta): persist guarded registration and native refresh`; root reviews every state transition and race assertion.

## Task 4: Operable provider SPI, packaging and real REST acceptance

**Create in provider main package:** `DeltaCatalog.java`, `DeltaCatalogCapability.java`, `DeltaCatalogOperations.java`.

**Create resources:**
- `catalogs/catalog-lakehouse-delta/src/main/resources/META-INF/services/org.apache.gravitino.CatalogProvider` containing `org.apache.gravitino.catalog.lakehouse.delta.DeltaCatalog`.
- `catalogs/catalog-lakehouse-delta/src/main/resources/lakehouse-delta.conf` with nonsecret commented examples only; no broad adoption root/recovery identity defaults.

**Create tests:**
- `catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/TestDeltaCatalog.java`
- `catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/TestDeltaCatalogRest.java`
- `catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/integration/test/TestDeltaCatalogNativeRestIT.java`
- `catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/integration/test/DeltaRestNativeFixtures.java`

**Modify:** provider build file/README; root `build.gradle.kts` distribution task list; only if required, module publication/build wiring. `settings.gradle.kts` already includes both modules. Do not edit source under protected modules or existing generic catalog production code.

**Interfaces:** `public final DeltaCatalog extends BaseCatalog<DeltaCatalog>` with public constructor, shortName `lakehouse-delta`, property metadata accessors, `newOps(Map<String,String>)`, capability, and protected `shouldBackfillCredential()` returning false. `DeltaCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog` owns config, reader, managed schema delegate and registration ops; initialize with existing GravitinoEnv store/id generator; close its reader exactly once. A failed initialize closes owned resources. `DeltaCatalogCapability.managedStorage` supports TABLE/SCHEMA only. `testConnection` throws UnsupportedOperationException with a clear message that this provider has no catalog-level connection probe; configuration is validated at initialization and native read access at operations. Do not add native writes or synthetic success for connection testing.

Schema delegate permits managed create/load/list with sanitized comment only (empty schema property map); ordinary schema alteration may update comment but rejects location/storage properties and rename. Drop schema is allowed only if raw TABLE list is empty, including all hidden/tombstoned registrations; cascade with any row throws NonEmptySchemaException. Table drop/purge and ordinary alter/rename remain explicitly unsupported. Catalog/metalake cascade behavior must be tested for protection afforded by existing parent dispatchers; a failing parent operation must not erase registrations or claim native deletion.

Build wiring: follow generic `runtimeJars`/`copyCatalogLibs`/`copyCatalogConfig`/`copyLibAndConfig` tasks, with destination `distribution/package/catalogs/lakehouse-delta/{libs,conf}`. Add `:catalogs:catalog-lakehouse-delta:copyLibAndConfig` to root distribution dependency list. Ship shared protocol jar, Kernel API/defaults/storage and S3A/common/AWS dependencies as resolved for the selected exact pin; apply repository exclusions for app-shared logging/Guava using generic package pattern. Resolve the metadata component review minor: duplicate SLF4J bindings and missing appenders appeared in both pinned unit matrices. Provider runtime/package must have one intended logging binding with test logging configured; inspect actual packaged jars and amended test stderr, not only declared exclusions. Keep API/common/core compileOnly. Test-only dependencies include client-java, server, server-common, integration-test-common testArtifacts, required HTTP/server test helpers, Mockito and JUnit. **Never** depend on generic `registrationTestArtifacts`; no fake ServiceLoader entry in production.

- [ ] **Step 1: Write provider/SPI and native-free real REST RED tests.** Start real MiniGravitino with durable temporary H2, JCasbin authorization, simple test principals, non-isolated load for this first transport suite; load the **main production** `lakehouse-delta` provider via its ServiceLoader entry. No static test outcome/handoff controls. Native-free REST scenarios reserve real S3 identifiers without performing native writes: pending hidden in GET/LIST, actor/admin STATUS, statusByToken after discarding the numeric ID in a lost POST response, wrong actor/run/token denial, immutable roots/secrets, direct internal property writes, blocked purge/rename/cascade. Use existing generic REST test setup as a reference, not a test-artifact dependency.

```java
Table reserved = actorTables.createTable(ident, new Column[0], null,
    Collections.singletonMap(DeltaRegistrationCommand.COMMAND_PROPERTY,
        DeltaRegistrationCommand.reserve("t1", "scheduler/run1", exactSchema).encode()));
DeltaRegistrationReceipt receipt = DeltaRegistrationReceipt.fromProperties(reserved.properties());
assertEquals(DeltaRegistrationReceipt.State.RESERVED, receipt.state());
assertFalse(actorTables.tableExists(ident));
Table status = actorTables.alterTable(ident, TableChange.setProperty(
    DeltaRegistrationCommand.COMMAND_PROPERTY,
    DeltaRegistrationCommand.status(receipt.identity()).encode()));
assertEquals(receipt.identity(), DeltaRegistrationReceipt.fromProperties(status.properties()).identity());
```

For lost POST reply, persist typed command intent in the test client before POST, intentionally discard the response identity, then call statusByToken(token,runId) through the known named-table PUT route and assert the durable numeric ID is returned. Repeat for ADOPT with retiredRunId in native REST tests; wrong/stale token or run reveals no receipt, performs no native read and changes no state. All mutating factory decoders still require full expected ID/generation/token. The downstream Spark runtime journals typed production command intent before POST and receipt afterward; this provider task documents that contract without implementing the runtime or blindly retrying timeouts.

Inject owner-dispatcher failure in test setup only (restore singleton state/clients in finally) to prove initial row survives and same-actor pending replay repairs owner after the failed request **completed**. Do not advertise simple test authentication as trusted production identity. Run `./gradlew :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaCatalog' --tests '*TestDeltaCatalogRest' -PskipITs` and capture RED.
- [ ] **Step 2: Implement SPI, operations delegation, safe packaging and native-free REST behavior.** Avoid connector-result escape of internal maps/classes; return GenericTable/native columns and sanitized properties. Validate receipt command bytes using Task1, preserve managed storage branches and owner hook sequencing. Ensure pending owner absence blocks initial PUBLISH even for admin before reader I/O.
- [ ] **Step 3: Add root-controlled live native REST tests.** Test class is under integration/test and excluded when `-PskipITs` is present. It consumes explicit system properties `delta.provider.it.endpoint`, `delta.provider.it.region`, `delta.provider.it.bucket`, `delta.provider.it.credentialsFile`, `delta.provider.it.prefix`; root provides an existing private credentials file and unique owned prefix. Forward only these nonsecret inputs from Gradle JVM properties into the test JVM with the build block below (credentialsFile contains a path, never secret bytes). Configuration is passed as structured Gradle properties/environment according to existing secret-safe harness; never echo file contents or credentials. The test helper reads credential file privately, starts no shared service, and writes only uniquely owned fixture paths through **test-only Kernel APIs**. First test invocation fails clearly if opted in without required settings; ordinary unit runs are not reported as native acceptance.

```kotlin
tasks.withType<Test>().configureEach {
  listOf("endpoint", "region", "bucket", "credentialsFile", "prefix").forEach { suffix ->
    val key = "delta.provider.it.$suffix"
    providers.systemProperty(key).orNull?.let { systemProperty(key, it) }
  }
  if (project.hasProperty("skipITs")) {
    exclude("**/integration/test/**")
  }
}
```

REST ADOPT baselines are real Kernel-generated tables. Kernel rejects the registration marker property: it cannot produce genuine marker-bound CREATE acceptance. Any locally edited JSON marker controls remain explicitly protocol-shaped unit controls. Genuine native CREATE/REPLACE publication uses retained/reproducible Spark fixture entrypoints from the preceding schema-contract characterization or the following packaged Spark acceptance; do not relabel handwritten marker injection as native acceptance. Mapped v2 and native Spark REPLACE evidence come from the separately finalized schema-contract/native qualification, not fabricated acceptance claims. Verify POST ADOPTING then PUT PUBLISH/EXISTING_NATIVE_SNAPSHOT; foreign valid marker preserved byte-for-byte; malformed marker rejected before row creation; object manifests unchanged by provider; configuration secret absent from durable properties/HTTP. Test owner-hook failure before and after relation insertion, admin owner-presence gate, completed replay after ownership transfer, and source-ID/version/config drift between ADOPT stages. Native CREATE publication/candidate evidence runs only on native fixture entrypoints actually supported and reported as such.

For isolated-package acceptance, run MiniGravitino with isolated catalog load and catalog `package` pointing to the built `distribution/package/catalogs/lakehouse-delta`; provider comes from packaged jars/main service file, not test-only service entry. Compare two catalog instances sharing pooled classloader/same bucket URI but differing trusted endpoint/credential configuration using one working and one deliberately invalid credential/endpoint configuration against the same URI. A falsely shared filesystem must not make the invalid catalog succeed. Two valid contexts then prove closing one leaves the other usable; root may supply a second owned endpoint if required for an additional endpoint-routing assertion. Cite tracking-filesystem tests only for mechanism, not live proof. Capture active Kernel versions and provider class origins; retain a live registration across server restart and verify STATUS/HANDOFF/ABANDONED receipts persist.
- [ ] **Step 4: Run final task gates with original evidence capture.** Sequential commands after source stabilizes:

```bash
./gradlew :delta:delta-common:test :catalogs:catalog-lakehouse-delta:test -PskipITs -PdeltaKernelVersion=3.3.0
./gradlew :catalogs:catalog-lakehouse-delta:test -PskipITs -PdeltaKernelVersion=3.3.2
./gradlew :delta:delta-common:javadoc :catalogs:catalog-lakehouse-delta:javadoc :delta:delta-common:spotlessApply :catalogs:catalog-lakehouse-delta:spotlessApply
./gradlew :catalogs:catalog-lakehouse-delta:jar :catalogs:catalog-lakehouse-delta:copyLibAndConfig -PdeltaKernelVersion=3.3.0
./gradlew :catalogs:catalog-lakehouse-delta:dependencies --configuration runtimeClasspath -PdeltaKernelVersion=3.3.0
./gradlew :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaCatalogNativeRestIT' -PdeltaKernelVersion=3.3.0
./gradlew :catalogs:catalog-lakehouse-delta:test --tests '*TestDeltaCatalogNativeRestIT' -PdeltaKernelVersion=3.3.2
```

The two live commands run only with root-supplied private inputs forwarded explicitly by the integration-test task; record those exact nonsecret inputs and actual exits. Ensure Kernel property participates in test inputs, so second pin reruns; copy first XML/logs before second run. Build/package again for 3.3.2 before its isolated live run; `runtimeJars` must remove stale alternate-version dependencies from its dedicated output so a previous pin cannot remain in the package. Never broad-delete shared distribution state. Inspect packaged service entry and dependency jars for exactly one matched Kernel pin, Hadoop3.3.6, shared protocol and no Spark/server test jars. `git diff --check`; use project formatting and broader tests only where changes justify them.
- [ ] **Step 5: Write acceptance report and commit.** Record production artifact paths, exact provider/config example, nonzero tests per pin, native-generated vs unit-shaped controls, ownership-posthook/request-drain limitation, platform writer/location admission assumptions, and unsupported lifecycle/protocol/feature boundaries. The report must not claim four-pair Spark runtime or Ranger enforcement. Commit task source/build/docs only with `feat(delta): package production catalog provider and REST lifecycle`; root performs independent final spec and code review before planning Spark runtime consumption.

## Preflight / handoff checks

- Every requested subsystem maps to one of four tasks; Spark runtime, REPAIR_INITIAL_OWNER, guarded DROP extension, physical purge and protected source changes are absent.
- Shared factories consume exactly one v1/v2 schema binding; authoritative actor, table ID, location, native candidate and replacement base are server-owned.
- ADOPTING and explicit owner presence handle deterministic create-hook failure; serialization/completion/drain limitation remains explicit, not upgraded to endpoint atomicity.
- Malformed present adoption marker is rejected, overriding the earlier research's opaque-marker alternative. Foreign valid marker is provenance only; its token never becomes current authority/public metadata.
- Durable observation stores raw schema/protocol plus complete configuration digest and safe allowlist, not full configuration.
- Full-row CAS and published-only refresh preserve actor correlation/column IDs without promoting pending operations; native ID changes quarantine.
- Evidence is truthful about prerequisite native-reader/schema-contract completion and root-controlled live test availability. This is a pending implementation plan; none of its proposed implementation/validation commands are execution evidence.
