# Delta Native Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox syntax for tracking.

**Goal:** Read authoritative checkpoint-backed Delta metadata for production registration without Spark in the server.

**Architecture:** An eager reader owns a separate Hadoop Subject per provider instance. A single package-private compatibility adapter copies Kernel 3.3 metadata into detached values; the public shared correlation component continues to verify exact native commits.

**Tech Stack:** Java, Kernel API/defaults/storage 3.3.0 and 3.3.2, Hadoop 3.3.6, JUnit 5, existing Gravitino column types.

**Spec:** `docs/superpowers/specs/2026-09-08-delta-production-runtime-design.md`

## Global Constraints

- Do not modify core, API, server, client, or storage-schema source.
- Server metadata code has no Spark or test-artifact runtime dependency. The shared protocol remains independent of Kernel/Hadoop/core.
- Exact supported Kernel API/defaults versions are 3.3.0 and 3.3.2, paired equally; reject missing, mismatched, or unknown runtime versions.
- Preserve raw native schema JSON, configuration and protocol details alongside Gravitino projection. Projection is not a lossless Delta schema round trip.
- No native writes or checkpoint creation in production readers. Tests may create and alter only their uniquely owned disposable fixtures.
- No global Hadoop UGI/configuration/filesystem mutation. Capture Gravitino authorization identity outside storage doAs.
- No source implementation or Gradle concurrency with another implementer. No subagents, shared deployment, remote push, merge, or PR creation by the implementer.

### Task 1: Implement checkpoint-aware metadata and isolated storage reads

**Files:**
- Create under `catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/metadata/`: `DeltaSnapshotMetadata.java`, `DeltaMetadataReader.java`, `DeltaKernel33MetadataAdapter.java`, `DeltaSchemaProjection.java`, `DeltaStorageContext.java`, and `DeltaMetadataException.java`.
- Create corresponding `TestDeltaMetadataReader`, `TestDeltaSchemaProjection`, `TestDeltaKernel33MetadataAdapter`, and `TestDeltaStorageContext` under the module test tree. Add narrowly scoped fixture helpers if needed.
- Modify `catalogs/catalog-lakehouse-delta/build.gradle.kts` for provider-only Kernel/Hadoop dependencies, Gravitino API compileOnly/testImplementation, strict Javadoc and an exact `deltaKernelVersion` build property defaulting to 3.3.0.
- Create `catalogs/catalog-lakehouse-delta/README.md` documenting the actual reader boundary and compatibility dependency. Distribution and ServiceLoader registration belong to the next provider task.

**Interfaces:**

```java
public final class DeltaMetadataReader implements AutoCloseable {
  public DeltaMetadataReader(Configuration trustedConfiguration);
  public DeltaSnapshotMetadata latest(String normalizedLocation);
  public DeltaSnapshotMetadata atVersion(String normalizedLocation, long version);
  public DeltaCommitEvidence verify(DeltaOperationMarker expected);
  public void close();
}
```

`DeltaSnapshotMetadata` exposes immutable values through `location()`, `version()`, `uuid()`, `schemaJson()` (raw `Metadata.getSchemaString()`), `columns()` (`List<Column>`), `partitionColumns()` (`List<String>`), `configuration()` (`Map<String,String>`), `description()` (`Optional<String>`), `createdTime()` (`OptionalLong`), `minReaderVersion()`, `minWriterVersion()`, `readerFeatures()`, and `writerFeatures()` (immutable sets). No Kernel internal types escape the adapter. Use defensive copies; public construction of synthetic native observations is unnecessary. The historically named `uuid()` returns Delta's opaque nonblank table ID, not a UUID-parser constraint. Configuration is provider-internal data: do not print it in toString, logs or errors, or blindly expose it in later receipts/table properties. Bound copied configuration to 4096 entries, 16 KiB per key, 1 MiB per value and 16 MiB aggregate UTF-8 bytes; reject excess before retaining a DTO. Preserve raw schema without using a registration marker's smaller envelope as a schema canonicalizer.

`DeltaMetadataException` carries a stable sanitized `reason()` and safe operation context, never SDK message/cause text that could contain credentials. It distinguishes unsupported runtime, unavailable snapshot/commit and reader closed; report unsupported protocol separately only when a typed native API makes that distinction reliable. A generic KernelException must not be mislabeled as a protocol failure or classified by exposing arbitrary message text. Production caller decides whether to quarantine durable state; this reader never mutates registration state.

`DeltaKernel33MetadataAdapter` is the sole production file importing `io.delta.kernel.internal.*`. It verifies actual Kernel API and defaults package Implementation-Version values before any read, requires exact SnapshotImpl compatibility, and copies Metadata/Protocol immediately. Use public Table snapshot construction to retain native protocol validation. Reject unknown reader features through Kernel's actual validation, while preserving writer-only features that do not prevent metadata reads.

Concrete currentness exception: Kernel 3.3's SnapshotImpl returns no commit-coordinator handler, so it cannot discover unbackfilled coordinated commits. Before returning any observation/evidence, reject presence (including empty/orphan values) of `delta.coordinatedCommits.commitCoordinator-preview`, `delta.coordinatedCommits.commitCoordinatorConf-preview`, or `delta.coordinatedCommits.tableConf-preview` with sanitized `UNSUPPORTED_COORDINATED_COMMITS`. Conforming activation is filesystem-visible: the conversion commit must be atomically backfilled before coordinator-only commits begin. This does not detect malicious/malformed logs that hide activation itself. Test a backfilled activation plus a later owned `_commits` control, empty/orphan keys, and an ordinary reader-supported/writer-unsupported control. This is a targeted storage-discovery guard, not a blanket writer-feature rejection.

`DeltaStorageContext` owns a fresh UGI Subject, defensive configuration and lifecycle lock. Its scoped I/O helper remains package-private and returns only eagerly materialized results. Each read creates its own engine inside `doAs`; snapshot access, action reads and all lazy iterator cleanup finish before returning. Cache remains enabled. `close()` drains admitted reads and closes only `FileSystem.closeAllForUGI(ownedUgi)`; repeated close is safe, new reads fail. Never expose an engine, snapshot, iterator, stream, UGI or filesystem to callers. Restore thread context classloader/Subject after exceptions as well as success. A package-private configuration seam may support tracking filesystems in tests; later provider construction supplies only allowlisted server storage settings.

Admit `s3a` for the S3 profile and `file` for owned local fixtures; a package-private seam may admit a tracking test scheme. Enable caching only for admitted schemes in the defensive configuration. Preserve unrelated Hadoop defaults such as disabled HAR caching, whose archive-specific semantics must not be overridden. Reject unsupported requested schemes instead of enabling all filesystem caches indiscriminately.

- [ ] **Step 1: Write focused failing tests and capture RED.** Cover immutable DTO collections; exact runtime pin acceptance/rejection; every supported primitive, decimal, nested struct/array/map, timestamp and variant projection; top-level and nested nullability/comments; raw metadata preservation for column mapping, generated/default/identity and type widening; unknown type failure. Build real Kernel tables with its transaction API for native read tests, and create checkpoints using `Table.checkpoint` in test-only setup. A minimal checkpoint acceptance assertion is:

```java
try (DeltaMetadataReader reader = new DeltaMetadataReader(configuration)) {
  DeltaSnapshotMetadata actual = reader.latest(location);
  assertEquals(expectedUuid, actual.uuid());
  assertEquals(checkpointVersion, actual.version());
  assertEquals(expectedRawSchema, actual.schemaJson());
  assertEquals(expectedConfiguration, actual.configuration());
}
```

Advance beyond 32 commits, create a valid checkpoint and remove only fixture JSON versions before that checkpoint to prove the old linear-history limit is gone. Test latest and retained as-of snapshots, checkpoint plus later metadata-changing JSON, missing required history, and unknown reader protocol failure. Handwritten protocol actions are acceptable unit controls, but report them separately from tables/checkpoints generated through native Kernel APIs.

Executed fixture discovery: Kernel 3.3.0 TransactionBuilder rejects unknown table properties, including the registration marker, through TableConfig validation. Use Kernel-generated tables/checkpoints for observation/reconstruction and separately labeled protocol-shaped marker-bearing JSON controls for verifier tests, modifying only uniquely owned temporary fixtures. Do not invent an internal writer bypass. Genuine Spark-written marker verification belongs to the following native schema-contract and packaged acceptance stages. Preserve initial fixture-failure RED output separately, then capture corrected behavioral RED after fixing setup.

Add explicit fixture coverage for complete/incomplete multipart V1 checkpoints, V2 UUID JSON and Parquet checkpoints, classic-named V2 checkpoints with sidecars, and missing/stale/corrupt `_last_checkpoint`. Use native checkpoint APIs where available and protocol-shaped unit fixtures otherwise; every accepted fixture asserts observed ID, raw schema, ordered partitions and protocol. The implementation delegates format discovery/replay to Kernel; tests must not add a separate production checkpoint parser. Compare adapter schema/partitions with public Snapshot observations before detaching results. Include a reader-supported/writer-unsupported table so observation never accidentally invokes the Kernel transaction writer gate.

- [ ] **Step 2: Implement full Kernel 3.3 metadata projection.** Preserve exact Metadata schemaString in the DTO. Map Kernel primitive types to matching Gravitino types; timestamp UTC and timestampNTZ use precision 6 and their correct zone semantics. Recurse through structs, lists and maps, retaining element/value nullability and comments. Use Gravitino VariantType for native variant and explicit failure for unrecognized types, malformed comments, unrepresentable empty structs and non-default string collation. Do not translate native generated/default expressions into Gravitino expressions without a proven semantic conversion; preserve their raw schema metadata instead. Required/default/comment details are documented explicitly. Full Kernel projection does not establish full Spark Delta feature support; record unrepresentable schemas for the later conformance milestone.
- [ ] **Step 3: Implement pinned metadata access and isolated lifecycle.** Guard actual package versions, construct snapshots through public Table APIs, then copy internal metadata/protocol in the compatibility class. Use per-instance UGI rather than FileSystem.newInstance around only manual reads or disabled caches. With a tracking filesystem test, show two contexts with the same URI and differing configuration resolve separately; closing one leaves the other usable; concurrent read/close drains correctly; lazy checkpoint reads run in scope; exceptions restore the caller Subject. No callback performing Gravitino auth/store operations runs inside storage scope.
- [ ] **Step 4: Compose exact commit verification.** For CREATE, candidate is 0; for REPLACE, candidate is checked base+1. Read latest and candidate under owned context; for replacement validate base snapshot UUID against marker.baseUuid through the checkpoint-aware adapter. Open only the exact candidate JSON at stored location and pass it to unchanged DeltaCommitActionReader with raw candidate schema/version. An inherited marker, missing exact JSON, wrong base UUID, superseded candidate, malformed trailing actions or read failure cannot produce MATCHED. After the shared inspector returns MATCHED, require its native table ID and version to equal the independently detached candidate snapshot, and require that snapshot configuration contains the exact expected encoded operation marker. A same-version checkpoint must not certify a conflicting JSON table ID or marker. Add focused checkpoint-versus-JSON disagreement controls. Catch no failure into synthetic success. Close all streams. Tests cover matching protocol-shaped CREATE and REPLACE evidence, wrong lineage and missing candidate after checkpoint; report which observation/checkpoint fixtures were native-generated.
- [ ] **Step 5: Run both pinned Kernel builds and inspect dependencies.** Execute module tests, strict Javadoc, jar and formatting for `-PdeltaKernelVersion=3.3.0` and `3.3.2`, ensuring tasks actually run under each selected runtime. Capture original XML, nonzero counts, command exits, actual loaded versions and resolved runtimeClasspath. Assert no Spark/server test jar in production runtime. Run shared protocol tests only if shared code changes (none expected).
- [ ] **Step 6: Self-review and commit.** Run `git diff --check`, document exact compatible features and remaining S3 live/checkpoint isolation acceptance, commit only task source/build/docs, and write full report with RED/GREEN evidence and concerns. Live MinIO qualification and production provider acceptance remain root-owned subsequent work; do not claim them from local tracking filesystem tests.
