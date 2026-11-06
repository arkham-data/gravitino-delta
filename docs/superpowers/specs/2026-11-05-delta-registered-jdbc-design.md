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

# Registered PostgreSQL inputs for governed Delta

## Intent and scope

Enable a named Gravitino PostgreSQL table to feed the existing governed native
Delta APPEND APIs through classic Spark/PySpark. This is the next input capability
in the [adoption roadmap](../plans/2026-10-29-spark-hub-adoption.md), following the
closed direct input timestamp milestone. It must reuse Gravitino metadata and
credentials, native Spark JDBC execution, existing Ranger evaluation and native
Delta writes. It does not complete all Spark Hub JDBC ingestion.

The first supported source is provider `jdbc-postgresql`, addressed by
`catalog.schema.table`. PostgreSQL is the reference vendor because Spark Hub
already uses `org.postgresql.Driver` and database-bearing PostgreSQL URLs. This
is an implementation choice, not a claim that other vendors are unnecessary.
Both registered and externally authenticated JDBC are required; the latter gets
its own task after this route. Existing datasets stay registered in Gravitino.

## Constraints

- Spark 3.5.4 and 3.5.6; Delta 3.3.0 and 3.3.2; Java 17; Scala 2.12.21.
- Actual Ranger server/client 2.8.0, Gravitino registration and S3A/MinIO for live qualification.
- No Gravitino core/API/server/client/storage-model changes, engine forks, manual Delta transactions or Glue compatibility.
- Preserve native classic PySpark DataFrame operations and both existing named APPEND writers: insertInto and existing-table Delta saveAsTable.
- Preserve caller AQE and the existing target policy, schema, publication, cache and recovery contracts.
- Ranger owns source column permissions, masks and row filters. Gravitino pushes catalog-qualified source SELECT grants through the existing authorization SPI.
- A trusted driver/runtime is assumed. Arbitrary JVM/Python code holding database credentials is not sandboxed by this adapter.
- One writer per destination; sources may advance. No remote database repeatable snapshot or immutable table incarnation is promised.
- No arbitrary DataFrame registration, LogicalRDD trust, raw-path destination or collected/reconstructed source authority.
- Retain original evidence; never automatically replay a known or uncertain committed write.

## Approach

Reuse `GravitinoPostgreSqlCatalogSpark35`, `JDBCTableCatalog`, `SparkJdbcTable`
and existing typed credential vending. A narrow optional Spark catalog wrapper
issues private source authority for the exact table it loads. Extend the existing
governed source inventory to distinguish Delta and registered JDBC, then reuse
ordinary read policy rewriting and the APPEND owner. The existing policy provider
can translate PostgreSQL source grants into the same Spark Ranger namespace.

Alternatives considered: a generic external-DataFrame admission API loses source
identity and hidden dependencies; a new JDBC catalog/provider duplicates existing
registration and credential machinery; a second Ranger runtime service requires
singleton isolation and namespace adaptation. None is needed for this first
registered route. External JDBC will require a distinct trusted origin contract.

The existing provider name `ranger-delta` remains for compatibility. Its factory
will accept exactly `lakehouse-delta` and `jdbc-postgresql`; its catalog-qualified
translator already has no native Delta registration or transaction dependency.
No generic `ranger`/HadoopSQL policies are silently translated: that provider uses
three resource levels, whereas this Spark service uses four.

## Policy and identity contract

PostgreSQL and Delta use the same provisioned Spark Ranger service and metalake,
under their own canonical catalog names. Resource keys are
`catalog / PostgreSQL schema / table / column`; the remote database belongs to
the catalog configuration and is not the resource's schema level. The deployed
Ranger resource field is literally `database`, matching DeltaRangerResourceKey;
its value is the PostgreSQL schema. Public
Gravitino SELECT_TABLE grants/revocations must reach the source resource through
the existing authorization plugin. Fine-grained column policies, filters and
masks remain Ranger-authored. Existing CREATE/MODIFY and owner translation is unchanged; unsupported DROP
translation and rename/drop callbacks stay rejected. This milestone does not
enable governed JDBC writes.

Application catalog visibility, current query subject, the distinct finite-lived
metadata identity and the upstream database credential owner remain separate.
The wrapper uses the owned metadata client for table schema resolution, after
application/catalog visibility checks, so eager BaseCatalog SELECT_TABLE checks
do not defeat final-column authorization. Reuse protected catalog hooks; do not
change BaseCatalog or temporarily swap its shared client. Metadata access never
authorizes payload. The completed logical query authorizes original demanded
columns before rewriting; `select(id)` must work without SELECT on unused fields,
while denied predicate/join fields must still fail.

Native JDBC schema discovery may contact PostgreSQL during table resolution.
Target or source denial must prevent payload fetch and Delta mutation, not claim
that schema discovery is impossible. The upstream PostgreSQL role is read-only
for fixture/production extraction. Ranger subjects are not PostgreSQL session
impersonation; database privileges continue to apply independently.

## Issued source and current metadata

The wrapper is selected only with explicit trusted startup opt-in, alongside the
existing Delta Ranger support; all other JDBC catalogs keep their existing path.
A table's class, schema, properties or copied Spark tags are not authority. Bind
its exact issuing wrapper/table object, canonical identifier, session/subject,
metadata lease, source policy contract and detached current metadata/configuration.
The public SparkJdbcTable constructor cannot mint a binding.

Use existing Gravitino catalog/table APIs and the owned finite metadata client.
Compare current provider/name, schema, canonical endpoint/database, driver,
credential owner/material and effective extraction configuration before/after
native load and at existing action/precommit boundaries. Re-read through the
owned client, not only GravitinoCatalogManager's cache. A changed or expired
source invalidates its issued binding. Endpoint/credential changes or lease
expiry require a fresh initialized catalog/runtime and metadata lease; merely
calling `spark.table` again in the same wrapper does not reload its frozen JDBC
options or replace an expired client. Never mutate issued options in place.
Observation reuses the stock `SparkJdbcTypeConverter`, including VARCHAR to
StringType and both timestamp forms to TimestampType. Stock native schema
discovery uses always-nullable JDBC fields and PostgreSQL
`isSigned`/`scale` metadata; compare ordered canonical names/types and only that
native nullability widening against registration, then retain the full original
native schema and the exact wrapped Gravitino schema independently. Later
changes to either fail. This reconciliation is JDBC-specific.
Gravitino's existing table DTO does not retain PostgreSQL TABLE_TYPE. Therefore
current observations also make one exact escaped DatabaseMetaData.getTables
lookup through Spark's stock PostgreSQL JDBC connection factory and the already
vended credential, matching database/schema/name and requiring one TABLE result.
Connection and ResultSet close immediately. This is metadata IO only; views and
other remote kinds reject without extending the server model or qualifying view
dependencies. It still provides no atomic remote-incarnation fence.
All comparisons are private; credentials must not appear in errors, receipts,
plan strings, logs or evidence. Metadata equality is not an atomic configuration
version and cannot detect every remote same-schema drop/recreate.

Require a database-bearing `jdbc:postgresql:` URL consistent with metadata
`jdbc-database`, stock `org.postgresql.Driver` from the trusted packaged artifact,
and a canonical quoted table identity supplied by the stock catalog. The first
profile uses a single JDBC partition and default stock read options. Reject
caller endpoint/driver/credential/table overrides, `query`, `prepareQuery`,
`sessionInitStatement`, custom connection callbacks and authority-bearing
`spark.bypass.*` settings. Do not treat a subquery in dbtable as a registered table.

## Logical and native execution

Support bounded Project/Filter over an issued PostgreSQL table, direct existing
current_timestamp projections, and one existing-grammar INNER join with a named
Delta snapshot. Original per-occurrence column demands survive pruning, constant
false predicates and eliminated scans. JDBC-only input is an authenticated
nonempty source inventory, not an empty Delta query. JDBC bindings grant no
Delta-log metadata or native-snapshot authority.

Apply source row filters and masks before user projection/filter/join, including
masked join keys. Keep the existing policy expression grammar and deny unsupported
masks. Capture original demands and decisions before native scan pushdown; do
not disable pushdowns globally to avoid implementing this boundary.

Stock Spark 3.5 JDBC begins as a DataSourceV2Relation, becomes a
DataSourceV2ScanRelation/V1ScanWrapper(JDBCScan), and uses V1Scan.toV1TableScan
for physical execution. The installed stock optimizer puts `Pre CBO Rules`
before `Early Filter and Projection Push-Down`: retain the exact issued V2
relation at pre-CBO, then verify the native scan through the complete stock
suffix. Original authorization is complete before either phase. It is not a BatchScanExec-only path. Bind the finite stock scan/options,
pushed filters, projected schema and source occurrence in receipt replay. Retain
the exact issued native physical scan and JDBCRDD through the supported query
continuation; do not broaden generic RDD equality or admit arbitrary callbacks.
Retain native optimizer/AQE behavior. Characterize both Spark pins in the tests
that implement this path, rather than a separate probe milestone.

Reuse APPEND original-input ownership, target checks, source revalidation,
transaction/publication and postcommit observation. Native preparation must
handle JDBC-only and mixed sources without manufacturing Delta bindings or
removing Delta's nonempty authority guards. Source policy/config changes before
publication reject through existing boundaries; in-flight reads follow captured
decisions and remote JDBC consistency semantics. No atomic Ranger/database/Delta
revocation fence is claimed.

## Acceptance

Installed tests first cover private issuance, fresh metadata/configuration,
credential redaction, service/catalog isolation, policy rewrite, original demand,
V2-to-V1 native execution, ordinary read and both APPEND writers. Include an
actual stock JDBC driver/database fixture for native behavior; mock-only scan
success is not execution proof. Include a JDBC-only input, a mixed INNER join,
pruned/empty input, id-only access with other columns denied, predicate/join
column denial, masks/filters, copied/foreign-session source and stale policy.

Actual-service acceptance uses an isolated owned PostgreSQL fixture, never the
Ranger database or unrelated existing databases. Qualify all four Spark/Delta
pairs with actual Ranger 2.8.0, Gravitino, MinIO and classic Python APIs. Every
pair proves source grant pushdown, masked/filtered native APPEND and genuine
source/target denial. The lead pair additionally proves both writers, mixed
source join, final-column composition, current_timestamp, source revoke and
configuration-change controls. Retain original process/artifact/native-state
receipts and distinguish metadata IO from payload IO. Positives commit once;
precommit denials publish no native version; any payload residue or uncertain
postcommit observation is disclosed without replay.

## Explicit remaining work

Arbitrary SQL/subqueries, stored procedures, prepareQuery, source views, other
vendors/providers, partitioned JDBC reads, COUNT estimates and incremental MAX
control reads are not covered by this first profile. Neither are external JDBC,
file/constructed inputs, unions/windows/dedup/materialization or governed refresh.
The first Spark Hub adaptation can use named table references with Spark
projection/filter; it must not pretend an existing arbitrary query payload has
only one table's dependencies. Full shared-sink adaptation/acceptance remains
in the coverage register. These limits are open obligations, not full JDBC or
Delta support claims.
