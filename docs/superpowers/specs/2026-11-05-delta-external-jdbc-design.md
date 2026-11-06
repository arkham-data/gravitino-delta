<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# External PostgreSQL input for governed Delta

Status: approved by the user with “Go” after the builder-reviewed handoff.
Implementation follows the [task plan](../plans/2026-11-05-delta-external-jdbc.md);
no new runtime qualification is claimed by this design. The registered PostgreSQL milestone closed at
`e796a98180f37df5ac259053ba7b283260682353` and remains closed.

## Intent and constraints

Enable Spark Hub's externally authenticated PostgreSQL query output to feed
native named Delta APPEND. This is the next input capability in the
[adoption roadmap](../plans/2026-10-29-spark-hub-adoption.md), alongside the
already-qualified [registered source](2026-11-05-delta-registered-jdbc-results.md).
The user explicitly requires both source models. The approved
[adoption design](2026-10-29-spark-hub-adoption-design.md) distinguishes external
ingestion from registered Ranger-governed sources.

- Spark 3.5.4 and 3.5.6; Delta 3.3.0 and 3.3.2; Java 17; Scala 2.12.21.
- Actual Ranger server/client 2.8.0; Gravitino registration; S3A/MinIO.
- Gravitino owns destination registration; Spark and Delta own execution and transactions.
- No Gravitino core/API/server/client/storage-model changes, engine forks, Glue compatibility, manual Delta transactions or production raw-path destinations.
- Trusted application/runtime and credentials, not an arbitrary JVM/Python sandbox.
- Preserve registered source SELECT, column permissions, filters and masks, including masked keys; preserve destination authorization and existing publication/cache/recovery semantics.
- One writer per destination; concurrent readers and changing source data remain possible.
- No generic DataFrame admission, collected-row reconstruction, LogicalRDD trust or automatic replay of uncertain/committed writes.
- Catalog-demo, CDF consumer state, streaming and deployment remain separate.

## Caller evidence

Spark Hub worktree `gravitino-catalog-foundation`, commit
`d01024b6f459ad2743b7b55099226b8c3dc742fd`, contains these paths:

| Caller | Required behavior |
| --- | --- |
| `src/shared/services/v2/spark.py:198-249` | SQL query wrapped as `dbtable=(query) arkham_temp_table`; native JDBC reader; optional fetch size and explicit partition bounds. |
| `src/data_platform/sources/jdbc/v2/main.py:96-130` | Separate remote count query, then the actual source query; returns a native DataFrame. |
| `src/shared/commands/job_runner.py:125` and `src/shared/commands/progress_tracker.py:113-140` | Background `df.count()` may overlap with destination APPEND. |
| `src/shared/services/v2/delta.py:529-564` | Input timestamp, destination existence/creation and CDF setup. |
| `src/shared/services/v3/gravitino_catalog.py:1-31` | Existing UUID-to-name read adapter; shared write routing is still missing. |

This is source-level evidence, not deployment frequency or successful execution.
A physical-table-only reader would miss the normal query path. Automatic bounds
in v2 already raise NotImplementedError; stored procedures, prepareQuery and
non-JDBC Snowflake/BigQuery formats are distinct behaviors.

## Approaches

1. **Owned external reader using native Spark JDBC — recommended engineering choice.**
   A trusted job scope constructs and issues the exact native relation. It returns
   an ordinary classic DataFrame, preserving Spark composition and named writers.
   This adds source provenance, not a JDBC engine or authorization service.
2. **Require registration and map all queries to Ranger resources.** Reuses the
   registered route for physical tables, but changes the explicitly requested
   external source model. Arbitrary remote SQL would additionally need lineage
   and policy rewriting. Keep the registered route for those governed reads.
3. **Accept any JDBC-looking DataFrame or an external=true flag.** Smaller API,
   but copies/tags/schema do not prove how rows were obtained. Rejected by the
   existing authority contract.

These are proposals from the source audit; the user approved this concrete reader contract after the builder-reviewed
handoff.

## Source authorization boundary

External query access is governed by the upstream database credential and its
permissions. There is no fabricated catalog/table resource and no claim that
Ranger masks or filters are applied inside unregistered SQL. The trusted Hub
runtime chooses the route and supplies credentials; a job payload flag or a
DataFrame cannot switch a registered source to external authority.

Registered Gravitino inputs continue through their named catalog and Ranger
checks. An external reader must not accept an existing named DataFrame and
reclassify it. In a mixed external-plus-named plan, every named source retains
its original demanded-column obligations, including predicate/join-only fields
and optimized-away occurrences. Target grants never authorize a named source.

Deployment must keep external extraction credentials from providing a bypass
around sources whose access must be Ranger-governed. That is a trusted platform
credential/routing responsibility, not a promise that this adapter discovers
all remote aliases or prevents arbitrary driver code from using credentials.
No new permission engine, SQL-to-resource inference or automatic catalog
registration is introduced.

Use a read-only database extraction principal. The adapter does not prove SQL
read-only by checking a SELECT prefix: functions, views and schema discovery
can execute remote work. Native SQL validation and database privileges remain
responsible for query behavior. No source snapshot, immutable table incarnation
or atomic source-plus-destination transaction is promised.

## Owned reader and lifetime

Add a separate companion input entry point and thin classic PySpark binding.
A scope opens under explicit trusted startup enablement plus the existing
Delta/Ranger runtime profile. The scope binds the exact SparkSession, current
authenticated subject/groups, frozen configuration, issue identity and a finite
lifetime. It provides native JDBC reads and close/context-manager cleanup; it
never accepts an existing DataFrame or LogicalPlan.

Engineering choice: require an explicit lifetime of 1–86400 seconds, matching
existing metadata-lifetime bounds; use 1200 seconds in the qualification fixture.
Measure expiry monotonically from scope creation, with no implicit renewal.
Close or expiry invalidates every relation issued by the scope. New work requires
a new scope. Check before payload actions and before destination commit; do not
claim expiry cancels an already-running native JDBC statement atomically.

Each read copies the complete effective options and captures the exact native
JDBCRelation, schema, partition descriptors and session/subject. Issue authority
before the relation reaches ordinary analysis, using Spark's stock JDBC provider
construction. Caller-map mutation, copied tags, equal schemas, reconstructed
relations and shared RDDs cannot mint another issued source. The native reader
may contact PostgreSQL for schema discovery during construction; target denial
is not a claim that no source metadata traffic occurred.

Credentials remain private in native options and the owned scope. Never print
options, raw SQL, credential values or hashes of credential material into new
logs/evidence. Diagnostics use an opaque issue identifier and nonsecret case
labels. The JVM independently validates Python inputs and ownership.

Credentials/options are immutable for an issued read. Rotation requires a fresh
scope/read from the trusted credential owner; remote revocation applies through
normal database authentication/privileges. There is no invented live credential
refresh callback or guarantee that an already-open connection is immediately
revoked. Cleanup releases owned references, not shared Spark services.

## Native execution and qualified operations

Separate native input provenance from registered Ranger resource obligations in
the finite source inventory. External input has provenance and lifetime checks,
not an allow-all Ranger engine. Every inventory entry must resolve to exactly one
issued source kind; unknown sources fail closed. Preserve the current policy
capture/recheck path for named Delta and registered PostgreSQL sources.

Extend the native proof for Spark's V1 JDBCRelation -> RowDataSourceScanExec ->
JDBCRDD path. Reuse stock provider, partitioning, column pruning and filter
pushdown. Do not force external queries through a fake registered V2 table,
replace Spark planning, loosen generic RDD equality or change AQE settings.
Physical proof retains exact issued relation/options/partitions and occurrence
identity through optimizer pruning, empty plans and supported adaptive execution.

The milestone covers:

- PostgreSQL stock driver and database-bearing URL; explicit username/password.
- Hub-style `dbtable` subqueries, including remote aggregate/join/view queries
  allowed by the upstream principal. SQL remains opaque to Spark governance.
- Native projection/filter, existing bounded scalar expressions and direct
  current_timestamp; count actions required by Hub tracking.
- One external input alone, or one external input joined with one named Delta
  input using the already-supported bounded INNER-join contract.
- Both existing named APPEND APIs: insertInto and existing-table saveAsTable.
- Default one JDBC partition first, then fetch size and explicit complete
  partitionColumn/lowerBound/upperBound/numPartitions options before closure.
  Exercise integer/date/timestamp bounds and NULL/out-of-range rows. Reject
  incomplete or unsupported options; never silently drop the optimizer.
- Repeated fresh count/append actions over the issued lazy input while live;
  background read and write use distinct operation contexts. No source caching
  or cross-action snapshot is inferred.
- Existing native target-cache refresh support, with the external scope still
  valid, and native empty-input behavior.

The first profile accepts exactly the required `url`, `driver`, `user`, `password`
and `dbtable` options, optional `fetchsize`, and the complete optional quartet
`partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`. Normalize keys
case-insensitively and reject duplicate spellings before opening a connection.
Require the exact stock PostgreSQL driver, a nonempty user, a supplied password,
and a single-host database-bearing URL with no user-info, query or fragment;
reuse the existing registered endpoint restrictions. Native validation owns
partition type/bounds semantics; fetch size must be nonnegative and partition
count positive. Freeze all native effective defaults as well as explicit values.

Reject prepareQuery, procedures, sessionInitStatement, customSchema, driver or
provider callbacks, JDBC writes, other vendors/formats and all other properties.
This includes explicit TLS/connection properties in the first profile. Preserve
native driver defaults; do not force TLS off, strip required security options or
fall back when an option rejects. Actual TLS deployments remain unqualified and
need their own connection-profile expansion. Existing registered validation has
no TLS-option validator to reuse. This is a bounded local PostgreSQL profile,
not a claim of production external-connection coverage.

Explicit partition bounds define Spark partition stride, not a row filter.
Source: [Spark 3.5.6 JDBC documentation](https://spark.apache.org/docs/3.5.6/sql-data-sources-jdbc.html).
Remote count, DataFrame count and APPEND may see different committed source rows.
Telemetry counts are estimates, not proof of what a later write consumed.

## Spark Hub boundary

Use a narrow optional source-reader dependency in SparkClient.query_jdbc for the
selected governed runtime, retaining its existing connection/query construction.
The dependency receives options and constructs the source itself. Unsupported
profiles fail explicitly; there is no fallback into the legacy raw reader within
the governed route. Preserve the existing legacy route when that profile is absent.
Do not make the read-only GravitinoDatasetCatalog impersonate a complete sink.

Acceptance invokes the real JDBC v2 source function and tracking behavior through
this adapter, followed by the existing named APPEND APIs and timestamp expression.
Prove authenticated subject/active-session propagation for the background count;
a callback alone cannot authorize execution. If the current thread cannot retain
that context, add explicit trusted context propagation and test it before claiming
this integration passes; do not disable tracking silently.

The complete job_wrapper/create/shared-sink migration remains open. The next
integration work must combine existing governed schema CREATE/publication with
the admitted source and preserve primary-key metadata, table properties, CDF and
registration states. This milestone does not close the existing
external-ingestion-create coverage entry. Incremental cursor max, arbitrary Spark
transforms, key dedup, unions/windows/UDF/checkpoints, external MERGE, generated
partitions and overwrite/full refresh remain separately tracked requirements.

## Task sequence and acceptance

After design approval, write a source-level implementation plan with exact APIs,
files and tests, then execute with one mutating implementer and independent review:

1. Owned native external reader and scope, plus installed negative provenance,
   lifetime, subject, options and schema tests.
2. Complete count/APPEND integration, explicit native partitioning, mixed-source
   policy preservation, AQE and target-cache behavior; focused installed tests.
3. Thin Hub source routing and actual classic PySpark source/tracker acceptance;
   four version pairs with Ranger 2.8.0 and MinIO; independent closure review.

Required negative cases include unissued native JDBC/LogicalRDD; cloned relation
or tags; another session/subject; closed/expired scope; modified options; upstream
credential denial; unsupported options; named-source SELECT/hidden-column denial;
mask/filter changes; target UPDATE denial/revocation before commit; threaded
context mismatch; and unknown/failed native outcomes. Denials must leave native
Delta versions unchanged where no commit occurred. Preserve any original committed
or unknown outcome and never rerun its target automatically.

Qualify source types with VARCHAR, DECIMAL, DATE, TIMESTAMP and TIMESTAMPTZ using
native conversion, including timestamp timezone behavior. Show exact rows and
native commit/publication state, not merely successful process exit. Include
source advancement between count and append without promising count equality.

Use a lead pair for the complete new behavior and bounded cross-pair checks for
both writers, partitioning and denial. Do not replay the registered PostgreSQL
matrix. Run only affected named-source regressions after the source-inventory
change. Capture original artifact/process identities and payload-versus-metadata
query counts with retained source data where needed; no performance qualification
is implied. Use the existing evidence format and retain raw failures.

Closure means this finite external PostgreSQL input works, not full JDBC coverage,
full Spark Hub ingestion, full schema-evolution acceptance or all Delta features.
Update the coverage register, record limitations and push the reviewed milestone
under standing authorization. No merge, PR, deployment or catalog-demo cutover.

## Builder discretion

Internal class/file decomposition, weak-reference cleanup, idiomatic language
bindings and fixture minimization are engineering choices. The exact installed
native continuation seam must be demonstrated before broadening any guard. A
failed native proof is a reason to report the gap, not to admit arbitrary plans.
Changes to source policy ownership, credential routing, hidden option defaults,
row semantics or deferred workload claims require an explicit design revision.

## Builder check

A fresh independent builder found no important unanswered question after the
connection option/TLS scope was made explicit. Those restrictions and the finite
lifetime are engineering proposals for this first profile. The user approved this
concrete design with “Go”; implementation and qualification are tracked separately.
