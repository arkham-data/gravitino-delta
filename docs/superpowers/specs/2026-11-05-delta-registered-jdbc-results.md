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
