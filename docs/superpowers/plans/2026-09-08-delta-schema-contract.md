# Delta Native Schema Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox syntax for tracking.

**Goal:** Correlate native name-mapped Delta creation and replacement without predicting engine-generated physical names or weakening v1.

**Architecture:** Add a closed v2 schema contract to the shared protocol and supply exact snapshot context from the production metadata reader. Validate required logical schema against the declared contract, while retaining exact native action versus Kernel schema equality. Characterize the resulting contract on all four native engine pairs before the production Spark adapter consumes it.

**Tech Stack:** Java 8 shared protocol, Jackson, server Kernel 3.3.0/3.3.2, Spark 3.5.4/3.5.6 with Delta 3.3.0/3.3.2 in tests only.

**Spec:** `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`

## Global Constraints

- Do not modify core, API, server, client, or storage-schema source.
- Existing v1 factories, wire encodings and verification behavior remain unchanged.
- Shared protocol depends on Jackson only; no Spark, Kernel, Hadoop, core or test fixtures.
- Marker data is correlation input, not a signature, bearer authority, writer fence, or complete query/data digest.
- Actual metadata-action schema must still exactly equal independently validated candidate snapshot schema after canonical JSON comparison.
- Only root owns live runtime creation/credentials/cleanup. Implementers never print credentials or mutate unrelated fixtures.
- One source implementer and Gradle owner at a time; no implementer subagents, shared deployment, remote push, merge or PR creation.

### Task 1: Add closed semantic schema binding and reader context

**Files:**
- Add `DeltaSchemaContract`, `DeltaProtocolDescriptor`, and `DeltaSchemaSnapshot` under the existing registration package in `delta/delta-common/src/main/java/`.
- Modify shared `DeltaOperationMarker`, `DeltaCommitActionReader` and focused tests; add `TestDeltaSchemaContract`.
- Modify production `metadata/DeltaMetadataReader.java` and its covering tests to supply trusted candidate/base context.
- Update shared/provider READMEs with exact admitted and excluded behavior.

**Interfaces:**

```java
DeltaSchemaContract.spark35Delta33NameMapping(
    DeltaSchemaContract.OperationMode mode, String normalizedRequiredSchemaJson);
DeltaOperationMarker.createWithSchemaContract(
    long id, long generation, String token, String location, DeltaSchemaContract contract);
DeltaOperationMarker.replaceWithSchemaContract(
    long id, long generation, String token, String location,
    DeltaSchemaContract contract, long baseVersion, String baseUuid);
DeltaProtocolDescriptor.of(
    int minReaderVersion, int minWriterVersion,
    Set<String> readerFeatures, Set<String> writerFeatures);
DeltaSchemaSnapshot.of(
    long version, String nativeId, String rawSchemaJson,
    Map<String, String> configuration, DeltaProtocolDescriptor protocol);
```

`OperationMode` is exactly `SCHEMA_ONLY` or `QUERY`. v2 marker envelope contains existing identity/location/operation/base fields and a schemaContract object with fixed kind `SPARK_35_DELTA_33_NAME_MAPPING_V1`, operationMode, canonical requiredSchema, columnMappingMode `name`, and fixed featurePolicy `BASIC_NAME_MAPPING_V1`. No user-supplied ignore list. `schemaJson()` continues to expose required schema for callers; an optional `schemaContract()` distinguishes v2 from v1.

Add the exact overload `inspect(DeltaOperationMarker expected, long candidateVersion, long latestVersion, DeltaSchemaSnapshot candidateSnapshot, Optional<DeltaSchemaSnapshot> trustedBase, InputStream commitJson)`. Snapshot version/schema/ID come from candidateSnapshot; no redundant validated version parameter. The old overload rejects a v2 marker with a stable missing-context result, never applies weaker v1 behavior. The new overload routes v1 through its unchanged rules. Context constructors reject nulls, invalid versions and malformed/bounded text; copy sets/maps eagerly and expose immutable values. Reuse the 128-KiB verification-schema bound and metadata reader configuration bounds (4096 entries, 16-KiB keys, 1-MiB values, 16-MiB aggregate), never log configuration. DeltaProtocolDescriptor validates structural effective state, including ordinary legacy reader1/writer1 protocols for unchanged v1 callers; closed pair/feature admission belongs to the v2 contract validator, not its generic descriptor factory. Context values are detached caller-supplied observations, not claims of cryptographic validation; production reader alone constructs them from its pinned Kernel reads.

- [ ] **Step 1: Capture meaningful RED.** Add adversarial tests where two native-shaped schemas have different valid mapping IDs/physical names but the same required logical schema; v1 continues to reject them and new v2 contract accepts only its admitted case. Add negative cases for changed field name/order/type/decimal scale/nullability/collection flags/comments/required metadata, absent or partial mapping assignments, duplicate IDs/full physical paths, invalid IDs/names/max ID, wrong mode/protocol, unknown contract fields and unknown generated additions. Preserve original RED logs/XML.
- [ ] **Step 2: Implement bounded immutable context and v2 encoding.** Reuse canonical JSON/resource checks without changing v1 bytes. Schema contract validates the normalized Spark StructType shape recursively. Admit leaf JSON type strings byte, short, integer, long, float, double, boolean, string, binary, date, timestamp and timestamp_ntz, decimal(p,s) with 1<=p<=38 and 0<=s<=p, and recursively struct/array/map. Exclude negative-scale decimal, interval, variant and unknown type strings/objects from this basic contract; exclude NullType, UDT, CHAR/VARCHAR markers, generated/default/identity semantics and UniForm/Iceberg nested-ID modes from this basic contract. Preserve required ordinary user metadata exactly; never strip arbitrary metadata keys. QUERY requires every StructField nullable=true, every ArrayType containsNull=true and every MapType valueContainsNull=true, recursively including types nested in map keys, values and array elements. MapType has no keyContainsNull field. Validate already-normalized input rather than silently changing it. SCHEMA_ONLY rejects nonnullable StructFields inside collection element/key/value types in this basic contract; preserve enforceable NOT NULL on ordinary struct paths and other admitted collection flags exactly. Factory/decoder reject unknown version, fields, feature policy and cross-operation fields.
- [ ] **Step 3: Validate mapped actual schema.** Permit exactly `delta.columnMapping.id` and `delta.columnMapping.physicalName` as engine-added field metadata. Require JSON integral IDs in `[1,2147483647]`, globally unique IDs, native generated `col-UUID` physical-name spelling and unique full physical paths (names may repeat under different parents). Reject `2147483648`, `4294967297` (aliases Int 1 in native conversion), floating nodes and partial assignments. Reject the other three mapping/parquet nested-ID additions under BASIC_NAME_MAPPING_V1. Require effective mode=name and canonical nonnegative decimal maxColumnId text no larger than 2147483647, covering every assignment. This is semantic validity, not proof of exact allocation order: arbitrary positive unique assignments satisfying these rules may pass. Candidate native metadata configuration must agree with the trusted candidate context for mapping-relevant properties. Native action raw schema and context raw schema remain canonically exact.

Admit explicit protocol pairs `(2,5)`, `(2,6)`, `(2,7)` and `(3,7)`, never a generic >= predicate. For writer versions below 7 both feature fields must be absent; for `(2,7)` readerFeatures is absent and writerFeatures is present with columnMapping; for `(3,7)` both are present and contain columnMapping. Parse candidate action field presence separately before reducing to sets, rejecting duplicates, invalid versions and unknown features. Require every reader feature in writerFeatures, and every reader-writer feature declared in writerFeatures to have its required reader support (columnMapping alone may use implicit reader2 support). Reader features admitted are the pinned Kernel set: columnMapping, deletionVectors, timestampNtz, typeWidening-preview, typeWidening, vacuumProtocolCheck, variantType-preview, v2Checkpoint. Writer features admitted are those reader features plus appendOnly, invariants, checkConstraints, changeDataFeed, generatedColumns, identityColumns, rowTracking, domainMetadata, clustering, allowColumnDefaults, inCommitTimestamp and checkpointProtection. Their presence is protocol compatibility, not a claim that all associated writes are qualified; excluded semantic field metadata/configuration still fails the basic contract. Reject coordinatedCommits-preview, redirectReaderWriter-preview, redirectWriterOnly-preview, icebergCompatV1 and icebergCompatV2 protocol features. Reject presence (including empty/orphan values) of delta.coordinatedCommits.commitCoordinator-preview, delta.coordinatedCommits.commitCoordinatorConf-preview, delta.coordinatedCommits.tableConf-preview, delta.redirectReaderWriter-preview and delta.redirectWriterOnly-preview configuration. Reject enabled delta.enableIcebergCompatV1/V2 and nonempty delta.universalFormat.enabledFormats; malformed boolean/format values fail, explicit false/empty is not activation. Apply these checks to candidate and trusted base effective metadata, independently of the general reader currentness guard. Any candidate protocol action must agree with the supplied exact snapshot descriptor. Add tests for timestampNtz requiring reader3/writer7, missing/empty feature-field presence, unsupported pairs and unknown features.

CREATE requires a protocol action. REPLACE may omit it only when candidateSnapshot.protocol equals trustedBase.protocol. Every candidate still requires exactly one metaData and commitInfo action; never synthesize same-commit metadata evidence from a snapshot. DeltaProtocolDescriptor represents effective validated state with empty sets for legacy absent feature fields; reject nonempty reader features for readerVersion2 or nonempty writer features below7. Wire parsing independently enforces absence/presence rules before reducing features to sets.

Validate the admitted feature dependencies from pinned TableFeature.scala: rowTracking and clustering each require domainMetadata. A schema containing timestamp_ntz requires reader3/writer7 and timestampNtz in both feature sets; merely recognizing the type is insufficient. Preserve absence-versus-empty protocol fields during action validation even though the detached Kernel descriptor exposes sets.
- [ ] **Step 4: Bind replacement to real base.** The production reader already obtains the exact base version and native ID. Supply its schema/config/protocol to v2 verification and reject mapping mode changes and excluded compatibility features. Fresh native REPLACE can reset IDs and reduce maxColumnId because it starts from new metadata: do not require old IDs/names to persist, disjoint IDs, or max >= base.max. Require the same semantic validity/generated-name rules as fresh CREATE; do not claim mathematical UUID noncollision across generations. Reject user/internal max-ID overrides at Spark admission. Retain changed native ID, candidate=base+1, latest=candidate, marker identity and whole-stream integrity checks. For v2 also require QUERY to match an admitted AS SELECT commitInfo operation, and SCHEMA_ONLY an admitted operation without AS SELECT; this is entrypoint correlation, not a query/data digest. Preserve v1 operation-name behavior. No synthetic matched factories or postcommit marker write.
- [ ] **Step 5: Run protocol/provider checks.** Run the shared tests and strict Javadoc/format/jar checks, plus amended provider tests on both Kernel versions. Capture original XML, exact commands/exits and dependency isolation. Confirm all original 36 v1 tests still execute unchanged. Commit only task source/tests/docs and report exact boundaries and concerns for independent review.

### Task 2: Characterize schema contracts against all four native engine pairs

**Files:**
- Add `spark-connector/v3.5/spark/src/test/java/org/apache/gravitino/spark/connector/integration/test/delta/TestDeltaSchemaContractNative.java` and focused fixture helpers.
- Modify only opt-in Spark test dependency/build wiring needed to consume the production reader and shared contract in tests.
- Add a result document identifying native entrypoints, exact versions, commands and remaining feature contracts.

**Interfaces:**
- Consume Task 1 shared contract and production metadata reader. Tests may exercise direct native Delta staging to produce evidence; label this accurately as schema-contract qualification, not production runtime packaging.
- Reuse existing explicit Delta/S3 opt-in gates and private credential-file transport; no static server test-support callback can manufacture matching evidence.

- [ ] **Step 1: Create native fixtures and capture first mismatch.** Run schema-only CREATE/REPLACE/existing and absent COR (route existing COR to native stageReplace and absent COR to stageCreate after authoritative branch selection), CTAS/RTAS/CORAS, and DataFrameWriterV2 under all four pairs. Use mapped and ordinary schemas with nested collections, decimals, comments and enforceable NOT NULL. Bind marker before native commit, then read the actual metadata action and production-reader snapshot. Start with a case proving v1 cannot bind assigned physical names and v2 does. Record original XML and raw native commit evidence for owned fixtures.
- [ ] **Step 2: Resolve normalization from observed native behavior.** Preserve schema-only ordinary struct NOT NULL and prove rejection of invalid inserts. Query schemas follow actual resolved native nullability. Test collection-nested NOT NULL under default and alternate relevant SQLConf, rejecting excluded cases before reservation/native mutation. Explicit and session-default mapping enablement must select a supported contract consistently. Native tests decide precise REPLACE physical-name/max-ID behavior; source-derived assumptions are amended only with recorded evidence and covering adversarial tests.
- [ ] **Step 3: Characterize remaining semantic metadata.** Execute separate generated-expression, default and identity cases using native Delta entrypoints to identify which are exact v1-compatible and which require a later closed contract. Verify actual inserted values/enforcement where the pinned engine supports the syntax. Unsupported syntax is an engine boundary, not a test success. Do not strip these metadata families broadly or claim their support from metadata preservation alone. Record the next concrete contract work needed for full Delta qualification.
- [ ] **Step 4: Finish the four-pair run.** Root supplies existing local MinIO endpoint/bucket/private credential path. Write only unique test prefixes, capture before/after manifests where verification must be observational, and clean only exact owned prefixes after evidence capture. Record Spark/Delta/Kernel loaded versions and original nonzero XML per pair. Keep default Spark compile/dependency isolation verified. Commit tests/results and report for independent review; production provider/runtime implementation follows continuously.
