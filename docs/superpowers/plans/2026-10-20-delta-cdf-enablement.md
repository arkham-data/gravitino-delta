# Governed CDF Enablement Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development, as the user requested. Preserve the existing worktree and retained evidence.

**Goal:** Enable and disable CDF through a registered Spark catalog name with Ranger authorization, refreshed Gravitino metadata, and continued governed SELECT/append.

**Architecture:** Gravitino remains registration owner. A dedicated command binding authorizes Ranger `alter`, delegates the mutation to native Delta through its protected transaction-start hook, validates its publication, and reloads the named table. Source SELECT and append authority remain independent.

**Tech Stack:** Spark 3.5.4/3.5.6; Delta 3.3.0/3.3.2; Scala 2.12; JDK 17; Ranger 2.8.0; S3/MinIO.

**Spec:** [Governed CDF property mutation](../specs/2026-10-20-delta-cdf-enablement-design.md).

Status: all four pinned runtime/live cells pass with Ranger 2.8.0 and MinIO. Tasks 1–3 and the final postpublication outcome fix
have passed their scoped reviews. The result page records final runtime/live
qualification separately from retained local tests. The outcome regression
covers policy change after publication and before first observation, explicit
committed/uncertain results, authority cleanup and recovery without replay.
The user approved the mapping and execution.
Completed prerequisite evidence must not be repeated merely to restart this plan.

## Global constraints

- No Gravitino core/API/server/client changes, separate authoritative metastore, or Spark/Delta fork.
- One writer job per dataset; retain native transaction/conflict behavior.
- Admit the named, single-property `delta.enableChangeDataFeed` boolean operation. Other DDL and governed CDF reads remain separate requirements, not implicitly authorized.
- The approved MODIFY_TABLE mapping is `{update, alter}`; SELECT stays separate. No automatic live policy migration.
- Native metadata authority must be operation-owned and must not authorize source data scans.
- Failed or uncertain writes are never automatically replayed or cleaned up.
- Use fresh fixture names. Preserve evidence. Serialize Gradle and live fixture ownership.
- Begin with one runtime pair; expand changed-path qualification after local success. Do not repeat the accepted append matrix without a regression or changed dependency.

## Review focus

- A policy revoked after analysis must deny the mutation before native publication.
- An old update-only policy must not acquire ALTER implicitly at startup.
- A successful native commit followed by reload failure must not be reported as a rollback.
- Disabling CDF must preserve native protocol history and existing enabled-period changes.
- CDF protocol state must not break ordinary named SELECT, append, duplicate handling or required native maintenance.

## Task 1: Approved privilege mapping and provider prerequisites

**Files:** `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/RangerDeltaAuthorizationPlugin.java`; its `TestRangerDeltaAuthorization.java` and README; `catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/TestDeltaRegistrationOperations.java`.

**Interfaces:** Produces canonical table-wide `alter` authorization through the existing plugin mapping, plus evidence for real provider metadata refresh.

- [x] Change both mapping/translation paths to UPDATE + ALTER after a failing regression.
- [x] Verify exact accesses, mixed SELECT, catalog isolation, legacy policy callback convergence/revocation, and no startup migration. Original RED: 4 failures; GREEN: 19 passing tests.
- [x] Verify real reader/operations/H2 refresh across manually authored CDF enable/disable metadata actions. One focused provider test passes; native Spark-to-provider composition is covered separately in Task 3.
- [x] Complete the scoped review: READY, no concrete defects. Commit these prerequisites with their documentation.

## Task 2: Working authorized property mutation

**Files:**

- Base adapter: `spark-connector/v3.5/spark-delta/src/main/java/org/apache/gravitino/spark/connector/delta/GravitinoDeltaCatalogSpark35.java`.
- Companion Java package: `DeltaRangerCatalogSpark35.java`, `DeltaRangerRegistrationBindings.java`, `DeltaRangerNativeOperation.java`.
- Companion Scala package: new `DeltaRangerCdfPropertyCommand.scala` and `DeltaRangerCdfPropertyMutation.scala`; relevant command admission, native metadata and physical planning integration in `DeltaRangerReadRule.scala`, `DeltaRangerSparkExtension.scala`, `DeltaRangerAppendInventory.scala`, `DeltaRangerNativeProvenance.scala`, `DeltaRangerPlanAdmission.scala` and `DeltaRangerPhysicalPlanning.scala`.
- Tests: evolve `TestDeltaRangerInstalledCdfAlter.scala`; add focused command/publication tests beside it. Preserve unrelated tests and admissions.

**Interfaces:**

- Base protected seam: `void executeNativeAlter(Identifier id, String location, TableChange... changes)`, defaulting to the existing native catalog call. It receives the already validated registered location and changes.
- Companion `alterTable` override consumes only an issued CDF command, validates its exact table/changes, and delegates through the base validation and reload.
- `BoundCdfPropertyMutation` binds registration, native table/log/snapshot and transaction independently of append authority.
- The owned native command subclass overrides `startTransaction()` to capture the returned native transaction; it inherits native `run`. Invoke `DeltaConfigs.validateConfigurations` as native catalog delegation does.

- [x] Write the installed positive test first: table-wide `alter` allows exact named CDF enablement, while update-only, select-only and denied policies leave all target bytes unchanged. Run it and retain the original admission failure.
- [x] Add exact command binding and the base native-ALTER seam. Unit-test wrong/mixed properties, path/versioned targets, direct unbound catalog calls, changed session/subject/catalog and copied command state.
- [x] Implement the native owner and publication proof in the same task. Bind actual transaction ID/read version and expected native metadata/protocol; admit no data actions or unrelated metadata changes. Reuse shared metadata recipe checks without borrowing append's unchanged-metadata proof.
- [x] Exercise authorization/registration revalidation before mutation, policy revocation after analysis, foreign publication and altered bytes. A witness for another transaction must fail.
- [x] Close mutation ownership before the ordinary named reload. Verify owner unwinding on failure; distinguish a committed operation with failed refresh from a precommit denial. Never retry automatically.
- [x] Make installed enable and disable tests pass, including fresh snapshot reconstruction and a classic checkpoint boundary. Confirm disabling retains the native upgraded protocol.
- [x] Run focused changed tests and affected module formatting/Javadoc. Obtain one integrated review of this working operation before continuing.

Initial command:

```bash
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerInstalledCdfAlter' \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 \
  -PskipITs -PskipDockerTests=true --offline --max-workers=2
```

The existing characterization passes by asserting rejection. Replace it with positive/negative operation expectations; do not count its old green result as implementation acceptance.

## Task 3: Continued use and real composition

**Files:** New `TestDeltaRangerCdfEnablement.scala` beside the installed tests; affected native maintenance source only where actual CDF protocol execution requires it; existing provider/live fixture entry points reused without private configuration in source.

**Interfaces:** Consumes the working Task 2 command and refreshed registration, then uses ordinary named SELECT and the accepted append frontends.

- [x] Enable CDF, execute masked/filtered SELECT and matching-schema `insertInto`/existing-table `saveAsTable`, and verify duplicate suppression. Disable CDF and repeat ordinary use.
- [x] Verify actual target-first and project-first native cache maintenance where retained caches are present, plus checkpoint reopening. Add an exact operation-owned CDF maintenance witness if required; do not globally permit raw CDF reads.
- [x] Compare change production with independent native bounded CDF reads: no pre-enable changes, literal enabled-period rows, retained historical interval after disable, and rejected disabled intervals. The oracle is not a user-facing path bypass.
- [x] Execute the same command with real Gravitino provider refresh and verify persisted registration/native identity, schema, owner, protocol, property and version. This must compose the Spark command and provider, not join two mock-based test claims.
- [x] Exercise failure after native publication but before successful provider reload, then recovery by observation without repeating the mutation.

## Task 4: Qualify and document the usable feature

**Files:** Focused runtime/live harness tests, companion/provider documentation, `docs/delta-integration-status.md`, and a CDF result page.

- [x] Run changed-path tests across all four Spark/Delta combinations.
- [x] Package and exercise authorized/denied/revoked CDF mutation, continued use and independent change readback against Ranger 2.8.0 and MinIO with distinct application/metadata identities.
- [x] Qualify new public MODIFY grants delivering both UPDATE and ALTER. Preserve legacy policies until an explicit operator migration; duplicate public grants are not a migration guarantee.
- [x] Record exact supported behavior and remaining CDF consumption/lifecycle/DML requirements. Complete the integrated review and focused addenda, commit, push the milestone branch as authorized, and preserve the local worktree/evidence.

Final evidence is summarized in [the CDF result](../../delta-cdf-enablement.md).
The first-pair outcome fix has 38 covering cases, including eight new outcome
cases. Valid earlier command, continued-use/cache and provider results remain
accepted; per-pin compiled-source checks and shared-artifact live results are
recorded separately. The final local harness relies on retained worktree helpers
and evidence, rather than providing a standalone clean-checkout reproduction kit.

This completes catalog CDF enablement only. Named governed CDF consumption, broader DML/DDL, streaming, table features and production/demo integration remain in the full-support scope register.
