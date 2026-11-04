# Installed additive APPEND schema evolution — November 4, 2026

The finite additive APPEND implementation passed the bounded installed cohort
across Spark 3.5.4 and 3.5.6 with Delta 3.3.0 and 3.3.2. This continues the
[schema evolution design](2026-11-04-delta-schema-evolution-design.md) and the
[public append contract](../../delta-native-governed-append.md). It is installed
local fixture evidence. A new deployed Gravitino/Ranger-server 2.8.0/MinIO gate
remains required.

## Exact cohort and provenance

Every pair requires the same **60 method identities**: 41 integration and preserved
controls, 11 schema-intent methods and eight Java ownership/permission methods.
Exact method-set equality, actual test START/END, fresh XML, zero
failures/errors/skips, source/resource hashes, loaded runtime origins and native
artifacts are required; a passing count alone is insufficient.

| Spark | Delta | Task4 executed | Source-identical evidence reused | Status |
| --- | --- | ---: | --- | --- |
| 3.5.4 | 3.3.0 | 60 | None | Passed |
| 3.5.4 | 3.3.2 | 60 | None | Passed |
| 3.5.6 | 3.3.0 | 49 | 11 independent intent methods | Passed |
| 3.5.6 | 3.3.2 | 0 | 41 integrated + 11 intent + 8 ownership methods | Passed |

All four cells have zero failures/errors/skips. Loaded origins and worker records
establish Java 17.0.16, Scala 2.12.21 and Ranger client 2.8.0 alongside the
requested Spark/Delta versions. The three new test durations are 337,218 ms,
393,856 ms and 320,483 ms in the table's executed-cell order.

The reused first pair combines distinct original invocations; it is not a single
60-method run. Its intent and ownership sources and active dependencies are
unchanged by the integration work. Changed idempotency methods use the final
integrated evidence. The independent base-module native named-writer controls
remain their original Spark 3.5.6/two-Delta evidence, outside this cohort. The
actual-provider refresh/column-identity regression also passed separately.

Producing product revision: `3c01fbb0f3c8bedcedb7f8e2391bbc228b56c604`.
The retained evidence directory is
`.superpowers/sdd/2026-11-04-delta-schema-evolution/`; `task-4-report.md` records
commands and provenance, and `task-4-evidence/required-methods.json` freezes every
identity. New cells retain 210-entry source/resource/build manifests, original
commands/logs/exits/fresh XML, loaded JAR and compiled-class hashes, and native
before/after copies. Original Task1/2/3 evidence is preserved.

## Bounded observed behavior

The existing named `insertInto` and `saveAsTable` APPEND frontends use native
`WriteIntoDelta` for one atomic schema-and-data WRITE. Existing fields remain an
exact name/order/type prefix under the directional nullability rule; additions
are concrete nullable top-level scalar fields. Native option precedence and exact
writer `userMetadata` survive. Explicit false overrides session auto-merge;
explicit true or session auto-merge enables the qualified addition.

| Workload | Native observation |
| --- | --- |
| Masked/filtered wider source through named `insertInto` | Version 0 → 1; one WRITE containing Metadata and data |
| Partitioned CDF target through named `saveAsTable`, final stock AQE join exchange | Version 2 → 3; unchanged partition/protocol; continued fresh named CDF read |
| Evolving native checkpoint | Version 9 → 10; candidate schema in the write and native checkpoint artifacts |
| Reserved CDF `_commit_version` addition | Version 1 → 1; native refusal after file generation; retained orphan files; runtime outcome unknown; no replay |
| Three postcommit observation/outer Spark refresh failures | Version 0 → 1; exact publication survives cleanup; committed with observation unconfirmed; no replay |

Successful writes retain old rows with NULL additions and append only the
row-filtered source payload with original and added aliases masked. Copied native
transaction JSON, literal row multisets and writer metadata are checked
independently. Native UUID/configuration/protocol/partitioning remain unchanged;
the registration fixture keeps its identity while observing the exact own native
version and new columns. Fresh named plans read the evolved schema; old plans and
unrelated read receipts remain strict.

Source/target denials, UPDATE without ALTER, revocation after helper work,
duplicates at both entry points, arbitrary pending mutation, native invariant
failure and publication forgeries retain their expected refusal boundaries.
ALTER is required before either duplicate read and before helper work. Generated
uncommitted files are retained. Expired issued commands cannot create new jobs,
helper callbacks or files when reused after failure.

Evolving target-cache histories are refused before mutation for ordinary AQE-off,
ordinary AQE-on and qualified CDF targets. Existing matching-schema cache behavior
and non-substituting source-only cache presence retain their qualified behavior.

## Limits and next gate

These fixtures load Ranger **client** 2.8.0; they do not establish a new live
Ranger server or MinIO result. Registration column/version observations use the
existing installed fixture; actual-provider persistence/identity is separate
evidence. Historical packaged append matrices retain their original scope.

Nested or NullType additions, prefix/type/reorder/nullability changes, column
mapping, broader features/protocol transitions, special `_change_type` CDC helper
routing, source cache substitution, new operators, schema-changing target cache
histories, auto-compaction, creation and overwrite remain outside this result.
The reserved-name test does not qualify `_change_type` routing. Actual additive
MERGE evolution remains next before closing the schema track and proceeding to
governed creation. Input overwrite and other user priorities remain deferred.
There is no full-Delta-support claim and no automatic replay of unknown or
known-committed writes.
