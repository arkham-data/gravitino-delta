# Native governed Delta batch append

On October 18, 2026, the bounded named append milestone passed the four-pair
packaged public Gravitino → Ranger 2.8.0 → MinIO matrix: **28 original phases,
seven per pair**, with independent specification and quality acceptance. This
continues the [October 14 transaction-source preparation checkpoint](delta-append-transaction-source.md).
The [accepted SELECT milestone](delta-ranger-production-results.md) remains a
separate result. This page records append qualification, not full Delta support.

## Qualified surface and policy

The supported frontends are matching-schema named DataFrame
`write.mode("append").insertInto("catalog.schema.table")` and
`write.mode("append").saveAsTable("catalog.schema.table")` against an existing
PUBLISHED table. Names, types, nullability and column order must match. Explicit
writer options are empty. The qualified session transaction settings are:

```properties
spark.databricks.delta.write.txnAppId=<application-owned transaction identity>
spark.databricks.delta.write.txnVersion=<transaction version>
spark.databricks.delta.write.txnVersion.autoReset.enabled=false
```

Both transaction keys are set together; the live cases exercised a first append
and its same-identity/version duplicate. The default auto-reset=true route and
writer-level transaction options are not qualified. An authorized duplicate
rechecks original source and target authority, skips source/helper execution,
adds no log version, and retains native cache refresh. A denied duplicate fails.
Do not infer support for explicit writer options from the lower-level native
idempotency validator accepting transaction keys.

Source SELECT checks retain original columns and occurrences, masks and row
filters before values reach the writer. Target authorization is Ranger `update`,
propagated from public Gravitino `TABLE/MODIFY_TABLE/ALLOW`; it does not grant
source SELECT. Target SELECT masks/filters are read policies, not insertion
constraints. A target used as a source retains its separate source SELECT duties.
Gravitino owns PUBLISHED registration; Delta owns native files, actions and
transactions. Successful publication may advance the target receipt's observed
native version; registration incarnation/schema and other receipt fields remain
protected. No Gravitino core/API/server/client/storage-schema change or Spark/Delta
fork is part of this milestone.

Use the [adapter startup contract](../spark-connector/v3.5/spark-delta-ranger/README.md)
and [companion packaging contract](../spark-connector/v3.5/spark-delta-ranger-runtime/README.md).
Both Delta enablement flags, exact extension order, immutable journal directory,
SIMPLE application/current actor, separate private SIMPLE metadata-reader file,
catalog visibility, full policy snapshots and safe preinitialization audit logging
remain required. Admin, downloader, metadata and application identities remain
separate. Credentials and deployment configuration stay private.

## Native cache and checkpoint scope

Automatic native refresh is qualified for existing raw target cache entries and
direct attribute-Project dependents, in both target-first and Project-first
insertion orders. Append and authorized duplicate retain native keys, unload old
builders and install cold replacements. Late native-only materialization and
repeated readback succeed after the append owner exits, preserving unrelated
cache entries. The live Project selects **id/region**; column positions must be
interpreted using each fixture's actual schema. These are native/shared-cache
maintenance results, separate from named governed user queries.

The exercised context uses the launcher's default/login UGI, a single target raw
cache deployment, and a distinct native-only SparkSession sharing the actual
SharedState. Late readback constructs a fresh DataFrame in that retained native
session after the append owner exits. The local tests also assert repeated
UGI-reference identity; that stronger witness is not claimed for packaged live runs. User governed cached SELECT and append from an
`InMemoryTableScanExec` remain rejected. Raw native cache access does not acquire
Ranger masks or make arbitrary raw paths governed query entry points.

This bounded path requires actual `spark.sql.adaptive.enabled=false`,
`spark.databricks.delta.asyncUpdate.stalenessTimeLimit=0`, the stock default cache
serializer in both configuration and the already initialized JVM singleton, and
ordinary uncached/native histories. Settings are checked, never silently toggled.
Raw normalization admits classic Parquet protocol/table shapes only: reader
version 1, writer version at most 2, no reader/writer features, no column mapping,
CDF, deletion vectors, row tracking or type widening. See
[raw admission](../spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRawMaintenance.scala)
and [serializer history checks](../spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRawCacheSerializer.java).

Separate four-pair checkpoint tests qualify classic checkpoint production and
reopening: configured multipart partSize, JSON statistics disabled, partitioned
JSON/struct statistics variants, and a fresh checkpoint-plus-JSON-tail reader.
They inspect actual parts, pointers, schemas, actions and literal rows. This is
not streaming checkpoint, V2/sidecar or all-feature coverage. A successful append
return alone does not prove a native checkpoint hook succeeded; native hooks can
swallow maintenance failures.

A genuine historical Snapshot constructor without CRC is rejected before metadata
SQLExecution. Positive raw JSON and checkpoint-plus-tail reconstruction are
separate observations. This neither makes CRC a mandatory Delta protocol feature
nor establishes support or rejection for every CRC-absent table/path.

## Executed qualification

Every packaged row used JDK 17, deployed Ranger **2.8.0**, real MinIO, and a
separately frozen local execution profile. Original package, actual launcher smoke,
cell and provision exits were zero. Each runtime has 261 JARs and 25 checked actual
class origins, including defining-loader/class/JAR joins. Spark and matching native
Delta Spark/storage origins match the row; Scala library/reflect/compiler are
**2.12.21**, Hadoop client API/runtime and S3A are **3.3.6**, AWS SDK bundle is
**1.12.367**, and ordinary Guava is **32.1.3-jre**.

| Spark | Delta | Original live phases | Frozen package manifest SHA-256 |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 7/7 passed | `116d2316b907524459ff80a5916dca5606321f9c5e4f523896b6d4052010937c` |
| 3.5.4 | 3.3.2 | 7/7 passed | `e02782eba2774eac6dc9ea1dd3ad387f8edc45561b76d083ad2b75ff21744f59` |
| 3.5.6 | 3.3.0 | 7/7 passed | `e9caf4727b5b4c695f88c4ccb0842423f103fb51c1d543b2b84b5663bb922b89` |
| 3.5.6 | 3.3.2 | 7/7 passed | `30f63b565c3eebf41fee7c2f78a48f30be64eff3156a6a65b494569cd347b089` |

This `local[2]` profile installs one base runtime plus one companion and the
matched native JARs into an isolated launcher. It omits the older YARN web-proxy
JAR and establishes no YARN/cluster support. It does not replace the historical
stock Hadoop 3.3.4 profile in the [base catalog guide](spark-connector/spark-delta-catalog.md).
Do not substitute a Gradle test classpath for a distribution or mix profiles.

The seven original phases in each pair establish:

1. Baseline target UPDATE denial with unchanged log, rows, registrations and raw storage.
2. Public MODIFY grant, named insertInto append and authorized duplicate.
3. Independent existing-table saveAsTable append and authorized duplicate.
4. Target-first native warm cache append, duplicate and late readback.
5. Project-first native warm cache append, duplicate and late readback.
6. Public revoke followed by entry denial with unchanged storage.
7. Same-JVM public revoke after helper file generation, applied newer downloaded
   policy, ordinary HTTP 200 metadata release, and precommit denial.

Admitted phases change the intended target from one row/version 0 to three
rows/version 1, with exactly two allowed masked/filtered source rows. Source and
unrelated registrations remain unchanged. First/duplicate/final native state,
complete registration observations and raw inventories agree after publication;
no second publication occurs. Both warm orders return three exact target rows,
three repeated target rows and three id/region projected rows. The final revoke
phase preserves committed version 0/one row and retains one generated uncommitted
Parquet object. These results join original public callbacks, distinct nonowner
application/metadata identities, real policy/role downloads, native actions,
SetTransaction/CRC, and full raw-byte inventories; an ACK alone is not acceptance.

Evidence layers remain distinct. In-process tests cover native helper/commit,
constraints, checkpoint settings, hostile provenance and cleanup. Four-pair real
MinIO tests cover append/duplicate, both cache orders and late native readback
with local policy fixtures. Packaged launcher smoke proves linkage/origins, not
policy or S3 behavior. The 28 live phases above establish the public-grant chain.
The first-pair local raw-suite invocation 37 failed its supplementary
checkpoint-prefix negative fixture (10 tests, one failure, one skip); its passing
cases plus corrected selector 40 (one pass) provide bounded evidence, not a
passing whole-suite rerun. Later local raw-suite rows each have 10 tests, no
failures/errors and one deliberate isolated-history skip. All four MinIO cases
and all eight separate foreign-serializer histories passed their expected checks.
Final module formatting/Javadoc checks exited zero on unchanged 118 module sources;
this is not a new full-module test-suite result.

Sanitized acceptance anchors are the four package digests above and the accepted
matrix record SHA-256
`cdd7af11b31f577b84d36ba729d08233c931631a5ec19765a908ef234423f79f`.
Retained owner-only evidence under
`.superpowers/sdd/2026-10-14-delta-native-governed-append/` includes
`phase-stack-firstpair-live-report.md`, its independent task review,
`phase-stack-pair{2,3,4}-report.md`, `phase-stack-remaining-matrix-report.md` and
`phase-stack-remaining-matrix-task-review.md`. The accepted first pair was composed
without replay. Those private archives are not required to follow the public
scope/results recorded here; they retain the original bytes for independent audit.

## Failure handling and historical outcomes

Entry denial leaves target log/storage unchanged. Observed after-helper revocation
can leave uncommitted output without committed rows/version advancement. Errors
after native publication, including cache/checkpoint maintenance errors, can leave
committed work. Preserve the original exception and evidence; independently inspect
native versions/actions/rows/objects, registration and public/Admin/downloaded state.
**Do not automatically replay an uncertain write or delete its output.**

The October 18 fixture11 warm append committed version 1 and the expected masked
rows, then its original client failed during native cache normalization; later
phases were not launched. Later fixes and accepted fresh fixtures14–17 do not
convert that failure, fixture10's earlier postcommit publication-guard failure,
or fixtures12–13's partial/failed outcomes into successes. Earlier local
checkpoint runs also returned normally while required checkpoint artifacts were
missing, before the separately qualified settings correction.

Foreign configured/actual serializers, and default configuration retaining an
actual foreign singleton from an ordinary earlier Spark application, demonstrated
committed publication followed by automatic-refresh denial, old builder unloading
and an absent target cache entry. These expected negative tests describe
postcommit maintenance failure, not denied-before-write evidence.

## Remaining work

SQL INSERT VALUES/SELECT, `writeTo.append`, raw-path writes, broader writer options,
reset=true, overwrite, UPDATE/DELETE/MERGE, CTAS/RTAS, streaming, auto-compaction,
CDF and broader Delta feature/table lifecycle support remain subsequent work.
The eventual SQL design is broader than these two qualified DataFrame frontends.
CDF remains the first feature priority after Delta maturity; it is not a condition
of this bounded append result.

Qualification assumes one writer job per dataset. Observed refresh/revalidation
is not instantaneous revocation or an atomic Ranger/registration/Delta fence.
This milestone does not establish an arbitrary-code security sandbox, production
IAM, general cached governed queries, arbitrary launcher/authentication modes,
a general callback reconciler, catalog-demo integration or full Delta support.
