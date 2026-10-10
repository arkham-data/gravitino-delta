# Live Delta idempotency harness implementation plan

**Goal:** Execute the existing native idempotency scalar against real Ranger 2.8.0 downloads and Gravitino-owned S3 registrations on all four supported Spark/Delta pairs.
**Authority:** docs/superpowers/specs/2026-09-27-delta-governed-append-design.md and the user-approved next step: add a live harness for the new idempotency helper. Continuous subagent-driven local implementation/testing authorization applies.
**Scope:** Test-only entry point; production scalar and installed append admission remain unchanged. No core/API/server/client/storage-schema changes, Spark/Delta forks, remote push, catalog-demo changes, or cleanup of prior evidence.

## Design

Add `DeltaRangerLiveIdempotency.scala` under the Ranger module test Scala package. Reuse `DeltaRangerDownloadedSecurity`, production discovery plugin/no-argument catalog wrapper and metadata reader; invoke `DeltaRangerNativeIdempotency.hasBeenExecuted` in the same package. Root provisions a fresh packaged Gravitino server, native Spark-created S3 tables, actual provider ADOPT/PUBLISH registrations, a unique real Ranger service and four-level definition. Metadata principal differs from real query UGI. All credentials are private file inputs. The startup classpath uses the current compiled production guard and base runtime with separate test classes; no historical query binaries stand in for current source. This is not packaged-launcher parity qualification.

Input is private JSON and a new output directory. Reuse existing downloaded-query fields: sourceSecurityDirectory, driverSecurityDirectory, sparkConfigurationFile, resourceService, resourceDefinition, metalake, subject, metadataPrincipal, minPolicy, minRole. Add catalog, database, `tables` with keys duplicate/newApplication/higherVersion/denied, `transactionAppId`, `transactionVersion`, expectedSparkVersion and expectedDeltaVersion. Each table is a separate native S3 location seeded by ordinary Delta write with the same committed app/version; duplicate/new/higher cases therefore start with independent cold snapshots. Version is a nonnegative long less than Long.MAX_VALUE. All inputs and actual runtime pins must be validated.

For each allowed table, resolve the real wrapper and BoundAppendTarget; observe native snapshot cold state before invocation, actual aggregate/physical query completion, scalar result, no active transaction/native-operation afterward and unchanged SQLConf. Denied table is resolvable by the same actor but has no Ranger UPDATE; require the precise Ranger access-denied cause even for the committed duplicate app/version. Compare complete object key/length/SHA256 manifests before/after every case and retain original bytes. Record production class origins, registration/native identities, real download events/versions and scalar outcomes. Native result expectations: duplicate=true, newApplication=false, higherVersion=false. Denial is not a duplicate success. No write/commit API is exercised by the helper. Observe cold provenance without adding production authorization hooks.

This phase does not qualify mid-read publication changes, reset=true continuation, governed append, production launcher packaging parity, or CDF. Existing offline tests own publication/configuration/subject invalidation coverage. Live policy publication/invalidation remains later live append work.

## Work items and ownership

- [x] Task 1 — Harness implementer: new Scala test entry point and focused unit coverage of request/runtime/result validation. Existing downloaded security helpers may be reused but not behaviorally broadened. Format, compile and focused tests; preserve original failure evidence and exact command/artifact hashes. Request build lane before running Gradle; no Git mutations. Root prepares fresh external fixture independently.
- [x] Task 2 — Root/fixture worker: provision isolated real Gravitino/Ranger/S3 fixture with native seeded tables, published provider receipts and test-only policies. Use fresh project-owned names/paths and retain all bytes; do not touch the historical demo or SELECT fixture. Build preparation serialized with task 1. Generate input and security files for current test app; fresh driver/cache/output per matrix cell.
- [x] Task 3 — Qualify/review: independent source/spec review, run the four version pairs, verify fresh runtime outcomes and S3 integrity, independently review results, document scope. Root owns local Git checkpoint after acceptance. No remote push.

## Required validation

Spark3.5.4/Delta3.3.0, Spark3.5.4/Delta3.3.2, Spark3.5.6/Delta3.3.0, Spark3.5.6/Delta3.3.2; JDK17, Scala2.12, actual Ranger2.8.0 artifacts and deployed2.8.0. Each pair must report all three exact Boolean outcomes and precise denied duplicate outcome. Require HTTP200 policy download evidence with applied versions, no stale cached-only substitute, real Gravitino PUBLISHED receipts and storage equality. Offline request tests cover malformed/missing input and unsupported version/unsafe output reuse; live driver must exit nonzero for failed assertions. Review looks for mocked authority, wrong/stale classpath, warm-snapshot false claims, overly broad exception assertions, secrets in public output and incomplete object inventories.

## Completed qualification

All four fresh runtime builds and JVMs exited zero. The sixteen standalone live
scenarios produced duplicate=true, newApplication=false, higherVersion=false and
precise denied duplicate on each pair. All sixteen raw S3 objects, including
markers, retained identical keys, lengths and bytes. Eight focused unit tests
passed; scoped Spotless passed. Independent source, fix and final live reviews
passed with no outstanding findings. Root and independent audits verified runtime
origins, all classpath hashes, identical current source manifests, HTTP200 policy
and role downloads, registration receipts and physical query attribution.

Results and scope are recorded in [the live qualification report](../../delta-live-idempotency.md).
Private evidence is retained under `.superpowers/sdd/2026-10-10-delta-live-idempotency/`.
The two initial startup/configuration failures and the interrupted unrelated suite
remain preserved and excluded from successful results. Production behavior remains
unchanged; this is a local checkpoint with no remote push.
