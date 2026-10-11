# Live native Delta idempotency qualification

The test-only `DeltaRangerLiveIdempotency` entry point exercises the existing native
idempotency helper against deployed Ranger 2.8.0, real Gravitino registrations and
MinIO. It does not enable installed append or change production code.
The [command-owned follow-up](delta-append-idempotency.md) records the later
refactor and its fresh standalone-helper regression on Spark 3.5.6 / Delta 3.3.2.

## Fixture and authority

The isolated October 10 runtime uses the pinned images in the
[runtime handoff](delta-local-runtime-handoff.md). An ordinary packaged Gravitino
server owns four `lakehouse-delta` registrations through provider ADOPT/PUBLISH.
Each points to a separate S3 location created by native Spark with a committed
`SetTransaction` application/version pair. All four receipts are PUBLISHED at
native version 0. The query UGI differs from the metadata principal.

The application uses the production Gravitino discovery plugin, no-argument
Ranger catalog wrapper and metadata reader. Each supported pair gets a fresh JVM,
empty policy cache, journal and evidence directory. Real HTTP 200 resource-policy
and role downloads apply versions 4 and 3 respectively. Fixture resource policies
permit SELECT on all four tables and UPDATE on only the three allowed tables.
These policies are provisioned directly in Ranger; this phase does not qualify
public Gravitino MODIFY grant propagation. The tag endpoint returns 403, so this
phase makes no tag-policy claim.

## Verification

All four fresh JVMs exited zero on October 10, 2026 UTC:

| Spark | Delta | Live scenarios passed | Complete S3 inventory |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 4 / 4 | All 16 keys unchanged |
| 3.5.4 | 3.3.2 | 4 / 4 | All 16 keys unchanged |
| 3.5.6 | 3.3.0 | 4 / 4 | All 16 keys unchanged |
| 3.5.6 | 3.3.2 | 4 / 4 | All 16 keys unchanged |

These are 16 standalone live scenarios, separate from the eight focused unit tests.

The four scenarios are:

| Scenario | Required result | Computed transaction state |
| --- | --- | --- |
| Committed application/version | `true` | Cold before lookup; computed afterward |
| New application | `false` | Cold before lookup; computed afterward |
| Higher version of committed application | `false` | Cold before lookup; computed afterward |
| Committed duplicate without UPDATE | Exact Ranger access denial | Remains cold; no state Aggregate executes |

Successful state computation must belong to the helper's actual native transaction
snapshot. The observer correlates the fourteen Aggregate output ExprIds, including
their JVM UUID, with a completed physical query. It records the target-specific
Delta state path and physical plan. Regression tests reject an unrelated aggregate
with identical aliases, a callback from a prior case and an ExprId with a different
JVM UUID.

Every case checks unchanged SQLConf and closed transaction/operation scopes. The
helper requires transaction-version auto-reset to be false. Per-case Hadoop file
inventories retain original bytes and hashes. Independent raw S3 inventories cover
all 16 keys, including the four directory markers omitted by Hadoop file listing;
the complete before/after manifests and bytes must remain identical.

The focused harness suite has eight passing tests, including the three observer
regressions reproduced as failures before the correction. Runtime qualification
uses JDK 17.0.16 and Scala 2.12.21. All six resolved Apache Ranger artifacts, including `ugsync-util`, are 2.8.0.
Each frozen classpath contains 205 entries; source manifests cover the same 79
files in all four builds. The only dependency change is test-runtime Caffeine
2.9.3, using the existing repository pin, for unshaded discovery classes.

## Evidence and reproduction

Private evidence remains in
`.superpowers/sdd/2026-10-10-delta-live-idempotency/`. It contains the fixture's
native bytes and registration receipts, exact build and launch commands, frozen
project classes/JARs, dependency hashes, runtime origins, real download events,
case results and independent reviews. Credentials remain outside Git.

Accepted runs are `matrix-3.5.4-3.3.0-r1/run-02` and `run-01` under each other
matrix directory. `root-results-verification.json` records independent source,
runtime, result, download and storage checks; `final-live-review.md` records the
independent review. Earlier failed attempts remain separate and unmodified.

The retained `prepare-cell.py` and `run-cell.py` scripts build/freeze and execute
one supported pair. They require the private fixture described in `fixture/`, the
running isolated services and the prepared Hadoop S3 runtime. The committed Scala
entry point accepts a private request JSON file and a new evidence directory;
its required fields and runtime validation are defined in `parse`. A failed
assertion or incomplete run is not a successful qualification.

Two initial primary attempts are preserved as failures: missing unshaded Caffeine
in the test runtime, then an explicit fixture extension setting containing only
native Delta. The latter was corrected to the qualified Ranger-then-Delta extension
order. Both attempts left all S3 objects unchanged. An unrelated full-module test
run was stopped with exit 143 and is not counted as passing evidence.

## Remaining scope

This qualifies the finite scalar helper using current production classes on a
test classpath. It does not establish packaged-launcher parity, an accepted
AppendData command, integrated source authorization, action generation, commit,
postcommit handling, reset=true continuation or live publication invalidation.
Uncached/CRC-absent state reconstruction, broader Delta table features and
catalog-demo integration remain open. The earlier
[offline helper qualification](delta-native-idempotency.md) supplies separate
mutation and negative coverage.

Installed governed append remains disabled. The next implementation step is to
compose the bound append command, source obligations and native write phases in
the owned transaction, retaining revalidation through commit. No Gravitino core,
API, server, client or storage-schema changes are introduced. Full Delta support
is unfinished; CDF remains the first feature priority after Delta maturity.
