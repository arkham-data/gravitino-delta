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

Implemented against Task 1 commit `96e6a9299`, with source changes awaiting the
controller's independent review and commit. This evidence uses real PostgreSQL
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
