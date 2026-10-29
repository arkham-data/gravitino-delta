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

## Caller-selected adaptive append

The bounded append continuation keeps `spark.sql.adaptive.enabled=true` throughout
the actual classic PySpark application. Its two existing native entry points are:

```python
source.write.mode("append").insertInto("catalog.schema.existing_table")
source.write.mode("append").saveAsTable("catalog.schema.existing_table")
```

The source remains an admitted registered relation or admitted conditioned join;
the exercised exchange-bearing source is a self-join on the registered source's
`id`. Payload rows are neither collected nor reconstructed by the bridge. Schema
names, types, nullability and order must match the existing PUBLISHED target.
Writer options remain empty. The existing session transaction pair and
`txnVersion.autoReset.enabled=false` retain their previous contract. Both
null/default-helper branches are exercised without toggling caller AQE. This
does not admit SQL INSERT, `writeTo.append`, raw-path writes, arbitrary source
grammar, schema evolution or additional writer options.

The append-owned callback records a final stock adaptive payload with materialized
shuffle stages. Metadata and independent readback callbacks are recorded
separately. A metadata adaptive wrapper does not qualify an adaptive write.
Original source columns and occurrences retain SELECT checks, masks and filters;
public target MODIFY_TABLE still supplies Ranger UPDATE through the existing
provider mapping. Target SELECT remains read authority, not an insertion
constraint. Admin, policy downloader, metadata reader and application identities
are distinct, and canonical four-part Ranger resources retain their metalake
service binding. Gravitino retains registration ownership and Delta retains native
files, actions and publication. Only the exact observed native-version receipt
advance may change the registration after a commit.

Existing entry/precommit checks, authorized/denied duplicate behavior and known
commit recovery remain in force. An authorized duplicate rechecks authority,
skips payload generation and creates no second publication. The public-revoke
case downloads and applies the new policy in the same JVM after native file
generation and before publication, then observes the existing precommit refusal.
This is a measured boundary, not a per-stage or atomic Ranger/Delta revocation
fence. Preserve exceptions and independently inspect native state before retrying
an uncertain write; postcommit observation failure is not rollback.

Native target and direct attribute-Project cache keys are maintained in both
insertion orders, including duplicate and late readback. Late readback constructs
fresh native DataFrames in the retained native session after the append owner
exits. Reusing an already executed Dataset/QueryExecution is a separate boundary:
a development attempt returned its old rows even though replacement cache owners
selected the new head. That observation is retained; refreshing such retained
QueryExecutions is not qualified here. Native CacheManager's existing compilation
clone/configuration behavior remains distinct from the unchanged caller AQE
setting. Governed cached SELECT and application-cached append sources remain
outside this scope.

CDF-enabled and CDF-disabled target continued use, partitioned targets and classic
multipart checkpoint plus JSON-tail reopening are separately exercised. CDF
enable/disable setup histories are native fixture setup before ADOPT; they do not
qualify changing CDF properties under caller AQE or broader Delta table features.
Checkpoint acceptance requires actual parts/pointer and exact independent
reopened rows/version, not just a successful append return.

The Iceberg 1.11/Kyuubi 1.10.2 reference covers native HadoopCatalog and Gravitino
wrapper on both Spark patches, with four append frontends and actual adaptive
payloads. Its target UPDATE resource and post-rewrite source permission audits
differ from Delta's canonical resource and original-demand checks: a constant
masked source column was absent from the Iceberg permission audits. This is a
measured observer result, not permission to remove Delta's original-column
authorization. The reference does not itself qualify Delta's duplicate,
registration, native cache, checkpoint or after-helper revalidation guarantees.
Its `writeTo.append` and SQL INSERT SELECT successes do not expand Delta's two
qualified frontends.

All execution remains bounded to the pinned Spark/Delta pairs, classic local
PySpark, Java 17, Scala 2.12.21, actual Ranger server/client 2.8.0 and the
manifested Hadoop/S3A 3.3.6 MinIO profile. One writer job per dataset is assumed.
Catalog-demo, Connect/streaming, separate workers, production authentication and
full Delta support remain separate work.

The development subset has 14 actual application attempts across seven profiles,
with original exits and independent public/runtime/payload/native checks passing.
This remains candidate evidence. Final qualification requires all four frozen
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 cells, each with 67 selected installed methods
and 26 live profiles / 42 application write attempts. The final index is retained
at `.superpowers/sdd/2026-10-28-delta-append-aqe/final-evidence-index.json` when that
matrix and independent reviews complete. No final four-pair result is claimed yet.

## Native cache and checkpoint scope

The following cache/protocol limits describe the historical October 18 AQE=false
profile. The adaptive continuation above has separate candidate evidence and
continues to exclude broader table/cache histories.


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

This list records the October 18 append boundary. Later CDF, MERGE and ordinary-read
results are tracked separately in the [current parity table](delta-spark-parity.md);
the adaptive append continuation above has its own qualification gate.

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
