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

# Registered PostgreSQL input results

## Task 1: provider acceptance and grant administration

Completed on 2026-11-05 against base `e1901c4e2`. This result covers only the
`authorization-ranger-delta` module. It does not qualify JDBC execution, source
database grants, native reads, ingestion or APPEND.

The existing `ranger-delta` factory accepts exactly `lakehouse-delta` and
`jdbc-postgresql` and returns the unchanged `RangerDeltaAuthorizationPlugin`.
Other providers, including case/whitespace variants, reject before constructing
an Admin client. Catalog/service binding validation remains unchanged.

PostgreSQL `SELECT_TABLE` uses the existing four-level Spark Ranger resource:
`catalog / database / table / column`. The Ranger field literally named
`database` contains the PostgreSQL **schema** name (`sales` in the tests), not
the remote database configured in the catalog. The granted access is `select`
on the exact table with descendant column wildcard `*`. `USE_CATALOG` and
`USE_SCHEMA` remain visibility-only and create no data access policy.

The new tests exercise real plugin callbacks against the existing mocked Admin
boundary. They cover grant, revoke, regrant, role acquisition/deletion and the
user/group membership request payloads; two PostgreSQL catalogs plus a Delta
catalog on the same service; shared role lifecycle; preservation of existing
owners, unrelated roles and Ranger-authored access/mask/filter policies; and
rejection of foreign/wildcard/denied grants and rename/drop callbacks. Existing
Delta tests remain unchanged. No translator, service definition or policy schema
change was necessary.

## Verification

All commands used Java 17, `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2
-XX:MaxMetaspaceSize=768m'`, `--max-workers=1`, `--console=plain` and
`-Dorg.gradle.jvmargs=-Xmx2g`.

| Run | Selection | Exit | Tests / failures / errors / skipped |
| --- | --- | --- | --- |
| RED | Module test with `--tests '*TestRangerDeltaAuthorization*' -PskipITs` | 1 | 24 / 4 / 0 / 0 |
| Focused GREEN | Same selection after scoped `spotlessApply` | 0 | 24 / 0 / 0 / 0 |
| Ordinary module | Module test with `-PskipITs -PskipDockerTests=true` | 0 | 66 / 0 / 0 / 0 |
| Checks | Module `spotlessCheck` and `javadoc` | 0 | Javadoc warnings treated as errors |
| Patch whitespace | `git diff --check` | 0 | Clean |

The four RED failures were the original factory rejection:
`IllegalArgumentException: ranger-delta requires lakehouse-delta`. The fifth
new test checks rejection paths and passed before the factory change. Initial
formatting and test compilation failures are retained separately and do not
count as meaningful RED. Two post-change formatting failures also occurred
before test execution and are retained separately.

The ordinary module inventory was:

| Test class | Tests |
| --- | ---: |
| TestDeltaRangerCallbackStatus | 4 |
| TestDeltaRangerClientExtension | 3 |
| TestDeltaRangerCreateGrants | 4 |
| TestDeltaRangerPolicySelector | 11 |
| TestDeltaRangerPrincipalProtocol | 6 |
| TestDeltaRangerSchemaVisibility | 3 |
| TestRangerDeltaAuthorization | 24 |
| TestRangerDeltaContract | 5 |
| TestRangerDeltaProductionCallbacks | 6 |

The existing Docker tag excluded these opt-in tests before discovery; they are
not counted as JUnit skips:

- `TestRangerDeltaAdminContract.candidateLifecyclePreservesForeignPoliciesAcrossAdminAndPluginDownload`
- `TestRangerDeltaProductionCallbacks.ordinaryPublicRoleMembershipGrantAndLastRevokeFailuresRemainDivergent`
- `TestRangerDeltaProductionCallbacks.emptyRoleMembershipRequiresQualifiedOrdinaryReplay`
- `TestRangerDeltaProductionCallbacks.publishedTableOwnerPutRepairsAfterFailureAndAdoptPostCanReplay`

Original commands, exits, logs, XML and exact selected method inventories are
retained under the plan workspace at
`.superpowers/sdd/2026-11-05-delta-registered-jdbc/task-1-evidence/`.

The suite uses in-memory Admin models and local HTTP/MiniGravitino fixtures;
no actual Ranger, PostgreSQL, MinIO or native Delta callback opt-in was supplied.
No historical qualification matrix was replayed. JDBC source issuance, scans,
policy enforcement during execution and ingestion remain pending later tasks.


## Task 2: installed registered-source execution

Implemented against Task 1 commit `96e6a9299`, independently reviewed for spec
and quality compliance, and committed as `ad2b22987`. This evidence uses real PostgreSQL
16, stock Spark JDBC scans, native Delta execution and native target files.
Gravitino metadata and Ranger transport remain test doubles. It does not close
Task 3's four-pair live Gravitino/Ranger/MinIO matrix or Spark Hub adoption.

The optional startup flag `spark.gravitino.enableDeltaRangerJdbcSupport` selects
the companion PostgreSQL wrapper only with existing Delta/Ranger support.
Issuance binds the exact wrapper/table, canonical identity, session/subject,
owned metadata lease, current metadata, credential owner/material and full
settings. The default single-partition stock profile rejects arbitrary SQL,
connection callbacks, option overrides and custom drivers. A fresh exact escaped
`DatabaseMetaData.getTables` lookup through Spark's stock connection factory
requires one physical TABLE result; a same-shaped view rejects before payload
and APPEND. This deliberately adds metadata connections at observation checks.
It does not provide an atomic remote-incarnation fence or repeatable snapshot.

The source inventory contains only named Delta snapshots and issued registered
JDBC inputs. Original column demands, including predicate/join columns and
optimized-away inputs, remain authoritative. The native path retains
`DataSourceV2Relation -> V1ScanWrapper(JDBCScan) -> RowDataSourceScanExec/JDBCRDD`.
The stock strategy runs once for each verified action/occurrence/recipe and its
returned native subtree/RDD is retained. JDBC-only preparation grants no Delta
storage authority; generic RDD equality and installed optimizer/planner order
remain unchanged.

Installed characterization established three details beyond the initial seam
inventory. First, Pre CBO Rules precede Early Filter and Projection Push-Down on
both Spark pins, so preparation binds the issued V2 relation and validates the
JDBCScan through the verified suffix. Second, stock JDBC schema discovery widens
nullability and adds PostgreSQL metadata; ordered names/types reconcile against
registration while full native and wrapped schemas remain independently bound.
Third, native APPEND cache maintenance needs the finite JDBC participant. Its
existing owned Scope brackets metadata checks with complete context, policy,
identity and lease validation and alone accepts its already-proven native
published-version setting transition. No settings ignore-list was added.

### Final verification

All commands use Java 17 and the resource limits recorded for Task 1. Actual
worker classpaths include the freshly compiled wrapper/companion and explicit
PostgreSQL driver `42.7.11`. The fixture uses the readiness-pinned local
PostgreSQL 16 image, a unique container/database, ephemeral loopback port,
private credential files and a SELECT-only extractor. It stops only its own
containers. Docker tests are explicitly tagged and enabled with
`-PskipDockerTests=false -PrangerDeltaRegisteredJdbcEnabled=true`.

| Run | Selection | Exit | Tests / failures / errors / skipped |
| --- | --- | --- | --- |
| Startup GREEN | `TestGravitinoDriverPlugin` | 0 | 18 / 0 / 0 / 0 |
| Lead final, Spark 3.5.6 / Delta 3.3.2 | All `TestDeltaRangerRegisteredJdbc*`, 9 existing read-rule controls, 2 cache controls, 1 transaction-source control | 0 | 35 / 0 / 0 / 0 |
| Second pin, Spark 3.5.4 / Delta 3.3.2 | All `TestDeltaRangerRegisteredJdbc*` | 0 | 23 / 0 / 0 / 0 |
| Scoped checks | Common/companion formatting; companion and common Javadoc | 0 | Common Javadoc reports existing warnings |
| Final checks | `git diff --check`, coverage schema validation, fixture cleanup | 0 | No owned fixture containers remain |

Each pin runs 8 actual PostgreSQL/native scenarios and 15 focused issuance/profile
unit tests. The native scenarios cover normal catalog-manager load and direct
wrapper load; id-only permission with hidden-column denial; masks, NULL behavior
and row filters; AQE on/off with both file-skipping profiles; bounded JDBC/Delta
INNER join; direct timestamp projection; pruning; source
SELECT and target UPDATE/ALTER checks; late revoke/configuration changes;
foreign/copied/changed/raw source rejection; masked join-key and copied-leaf
rejection; physical-table/view distinction; and owned native target-cache refresh.

Both native writers, `insertInto` and `saveAsTable` APPEND, read one JDBC payload
query and create exactly one new native WRITE commit. The tests inspect exact
masked/filtered Parquet rows and two equal direct timestamps within the action's
bounds. Masking two source keys to the same BIGINT intentionally yields two
matching join rows under native relational semantics; it makes no uniqueness or
upsert promise. An optimized-empty APPEND authorizes the original source demands
but performs no payload query and creates no new version, matching stock Delta.
Denial scenarios compare target file inventories and source payload-query counts.

### Original failures and evidence limits

Original RED/setup attempts are retained separately; final passes do not erase
them. `native-red-7` reached the intended unsupported V2 execution boundary after
fixture/schema setup failures were resolved. `native-append-red` reached the old
Delta-only source extraction guard. `startup-red` demonstrated the missing opt-in
validation. `view-red` admitted a same-shaped view before the exact-object guard.
The detailed report inventories compilation/setup errors, phase-order discovery,
locator and empty-input failures, and subsequent focused fixes.

`native-timestamp-append` committed its first fresh target, then failed the mixed
AQE second target before commit at the Delta-only cache participant seam.
`native-cache-red` reached a native commit and then failed observation because
structural source binding rechecked the changed native last-commit setting. Its
outcome is recorded as committed/observation-unconfirmed, not successful. Those
original targets were never replayed. A later cache attempt committed but its
test read a stale previously compiled Dataset; a fresh read established the
correct replacement-cache behavior. Final tests additionally exercise unrelated
settings, unproven version changes, foreign/copied participants and expired scopes.

Logs/XML preserve those original outcomes. Native JSON commits and file
inventories were added to later scenarios and are retained in `native-state/`;
the earliest failed targets had already been cleaned up before that evidence
hook existed, so their original native objects are not claimed as retained.

Exact final commands, XML, runtime classpaths, original logs and the full failure
ledger are in `.superpowers/sdd/2026-11-05-delta-registered-jdbc/task-2-evidence/`
and `task-2-report.md`. The coverage register's external-ingestion status remains
`connector-gap` pending live qualification; only its expected-outcome note changed.
No historical qualification matrix was replayed.


## Task 3: actual-service functional qualification

The exact seventeen-case inventory passed on all four pinned pairs on
2026-11-05, with unchanged production sources from `ad2b22987`. Task and whole
milestone review remain with the controller; this result is not a full JDBC,
external-JDBC, Spark Hub or M2 completion claim.

| Spark | Delta | Actual-service cases | Original supervisor exit |
| --- | --- | ---: | ---: |
| 3.5.6 | 3.3.2 | 8 lead cases | 0 |
| 3.5.4 | 3.3.2 | 3 | 0 |
| 3.5.6 | 3.3.0 | 3 | 0 |
| 3.5.4 | 3.3.0 | 3 | 0 |

Each pair uses actual Gravitino registration, Ranger server and client 2.8.0,
S3A/MinIO, classic PySpark 3.5.x with Py4J 0.10.9.7 and pinned-thread identity,
Java 17, Scala 2.12.21, and an owned PostgreSQL 16.14 database with stock JDBC
42.7.11. The PostgreSQL image is pinned by content ID; no Ranger or unrelated
database was used. A fresh shared server/base package includes the current
Task 1 authorization and Task 2 common/plugin changes, PostgreSQL provider
closure and explicit driver. Every pair has an independently bound overlay.
Original server class-loading records and Spark class origins join the archived
provider, driver, helper and dependency bytes. Python ZIP containers and members
are also rehashed. Exact shared-production source equality was not bypassed.

All pairs execute `jdbc_insert_into`, `jdbc_source_select_denied` and
`jdbc_target_update_denied`. Public Gravitino source SELECT revoke/grant reaches
the exact Ranger catalog/schema/table resource. Native `spark.table` input,
projection/filter, direct `current_timestamp` columns and native
`write.mode("append").insertInto` publish exactly one new Delta version with
rows `(1, MASKED, allowed)` and `(3, MASKED, allowed)`. Both timestamp columns
contain the same non-null query instant within the original writer lifetime.
Genuine source SELECT and target UPDATE denials fetch no JDBC payload and publish
no new Delta version; metadata discovery is observed separately.

The lead's additional cases are:

- `jdbc_mixed_save_as_table`: named JDBC/Delta INNER join and native existing-table
  Delta `saveAsTable` APPEND. Both sources' policies apply. Fresh native queries
  verify the updated target cache, repeated read and dependent projection.
- `jdbc_column_composition`: three separately retained actions. Id-only access
  succeeds while unused columns are not granted; a hidden-column predicate and
  an optimized-empty original secret-column demand each fail before payload.
- `jdbc_source_revoked_before_publish`: the original writer is held after payload,
  a public source revoke is downloaded and applied, and publication fails through
  the existing policy snapshot boundary. Uncommitted S3 payload remains retained.
- `jdbc_config_changed_before_publish`: public catalog properties rotate to a
  second valid owned read-only credential after payload. Current metadata equality
  rejects the previously issued source before publication, with retained residue.
- `jdbc_view_rejected`: a same-shaped view has genuine metadata/SELECT grants.
  A distinct same-credential named physical-table control succeeds first. The
  view's subsequent original stock table-kind SQL lookup, absence of database
  errors and guarded load rejection establish the physical-TABLE boundary, with
  no view payload or publication. A generic sanitized exception alone is not
  accepted as this proof.

Two owned PostgreSQL catalogs expose the same remote schema/table under distinct
mask/filter policies. Qualified public transitions preserve the other catalog,
Delta policies, metadata authority and unrelated masks/filters byte for byte.
The second catalog's SELECT grant cannot authorize the denied first catalog.
Ranger's `database` field contains `source_schema`, not the remote database name.
The upstream SELECT-only role, query subject, metadata identity and service admin
remain distinct. No collected rows are reconstructed as writer input.

### Additional checks and retained evidence

The remaining Spark 3.5.6/3.3.0 and 3.5.4/3.3.0 installed cells each passed all
23 `TestDeltaRangerRegisteredJdbc*` tests with Docker and the explicit opt-in,
zero failures/errors/skips. Task 2's reviewed Delta 3.3.2 evidence was reused;
no closed historical matrix was rerun. Nine registered checker tests, 32 existing
append checker tests and 24 focused observer/request tests pass. Initial checker
RED retained 33 meaningful negative-fixture failures before the checks existed.

Original commands/PIDs/exits, source and archive receipts, policy downloads,
public callbacks, PostgreSQL statement logs/data, native JSON and S3 payload bytes
are retained under
`.superpowers/sdd/2026-11-05-delta-registered-jdbc/task-3-evidence/`.
The final inventory is seventeen cases and twenty separately recorded actions;
the extra actions are column composition and the view's physical-table control.
Five APPEND cases publish once. Two late controls retain unpublished payload.
Original raw results and packages remain immutable. Later checker/docs-only
verification uses explicit finite source deltas and never relabels package bytes.
Owned PostgreSQL and Gravitino processes are stopped; shared Ranger/MinIO remain.

The first base build succeeded, but its new archive collector initially rejected
existing duplicate LICENSE/NOTICE entries in shaded jars. The corrected collector
retains every ZIP entry by index/digest and still rejects other duplicates. The
first live setup stopped before Gravitino or any writer: Unix-socket readiness
observed PostgreSQL's temporary initialization server. TCP readiness corrected
that fixture race on a fresh owned cluster. Both attempts and their terminal
receipts remain retained. No committed or uncertain write was replayed.

### Limits and operational observations

This is functional qualification of the bounded registered PostgreSQL profile,
not a performance qualification. In the instrumented lead insertion, the original
log suffix contains one payload SELECT and 1,334 metadata SELECT statements:
444 stock getTables/TABLE_TYPE, 445 column-metadata, 222 other table-metadata,
222 other metadata and one zero-row schema query. These are individual PostgreSQL
messages, not repeated cumulative snapshots. Its full writer process took
73.44 seconds. The mixed/cache case observed one payload and 3,140 metadata
statements and took 304.16 seconds for the full process. These measurements
include live harness/observer/helper verification and startup; they are neither
isolated production-only payload timings nor a causal attribution of every query
to production. They do not establish acceptable performance. No currentness or
policy guard was weakened to reduce the observed cost.

Installed tests retain JSON commits and file inventories, while their temporary
Parquet files were inspected before cleanup and are not retained. Actual-service
runs retain original S3 payload bytes, including late-control residue. The existing
server wrapper retains its original command, PID, terminal status, fixture
lifecycle bounds and JVM class-load timestamps, but not a separate exact
server-spawn timestamp; that precision is not fabricated.

Remote repeatable snapshots, an atomic revocation/configuration fence and every
same-schema drop/recreate remain outside the contract. Source views, arbitrary
SQL/subqueries, callbacks/options, partitioned JDBC extraction, other vendors,
external JDBC and full shared Spark Hub ingestion remain unqualified or unsupported
as described in the design. The external-ingestion coverage status stays
`connector-gap` until those required integration paths are completed.
