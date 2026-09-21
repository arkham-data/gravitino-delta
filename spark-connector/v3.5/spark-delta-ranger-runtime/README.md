<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Optional Delta Ranger companion runtime

Build the companion and the base Delta runtime for the selected deployment pair:

```bash
./gradlew :spark-connector:spark-delta-runtime-3.5:shadowJar \
  :spark-connector:spark-delta-ranger-runtime-3.5:shadowJar \
  -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0
```

The supported qualification matrix is Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2,
Scala 2.12, and Java 17. See the [production results](../../../../docs/delta-ranger-production-results.md)
for executed evidence and outstanding gates. Building a JAR does not qualify a
query route or complete the full Delta authorization goal.

As of September 21, 2026, the qualification environment's Docker services are
stopped and ongoing verification is offline only. Historical policy/role versions
and readiness records are not current service attestations. Re-establish exact
service and fixture continuity before resuming the live matrix; do not replay
completed or failed public mutations to reconstruct it.

Install exactly one `gravitino-spark-connector-delta-runtime-3.5_2.12` and one
`gravitino-spark-connector-delta-ranger-runtime-3.5_2.12` in the trusted Spark
launcher. Supply Spark/Scala/Hadoop from the selected actual Spark distribution,
and supply the matching native `delta-spark_2.12` and `delta-storage` JARs.
Native S3 access additionally requires the distribution-compatible Hadoop S3A
implementation and its AWS SDK. Do not replace distribution JARs with the test
classpath. The companion contains no connector/client/protocol implementations,
Spark/Scala/Hadoop/Delta platform classes, or project test applications.

The companion uses plain Kyuubi authz 1.10.2 and pins every resolved Ranger
artifact to 2.8.0, including split `ranger-audit-core`. It retains original Maven
metadata for the engine's runtime provenance checks. The base Delta runtime alone
supplies the 33 reviewed Kyuubi utility classes from Kyuubi Hive 1.11.0, with its
existing private Guava/publicsuffix ABI. These are distinct Kyuubi artifact
versions. The companion excludes duplicate utility inputs and the standalone
`gethostname4j` dependency: authz 1.10.2 already supplies its selected non-JNA
Hostname implementation. The Jersey bundle is excluded in favor of the normal
client/core inputs. Other implementation dependencies are relocated privately.

Ranger's `ugsync-util` contains three helpers under Hadoop's namespace. Only
`KrbPasswordSaverLoginModule`, `SecureClientLogin`, and
`SecureClientLoginConfiguration` move to the companion's private namespace;
actual Hadoop references remain provided. JAAS class-name strings move with those
helpers. This packaging does not qualify Kerberos authentication. Jackson's Java
21-only multi-release entries are omitted for the Java 17 launcher; the remaining
multi-release entries and provenance metadata are retained.

## Separate production processes and identities

Install the separately built server provider under `catalogs/lakehouse-delta`
and authorization distribution under `authorizations/ranger-delta`. Run the
production `org.apache.gravitino.server.GravitinoServer` with its normal packaged
server dependencies and isolated plugin directories. Neither Spark artifacts nor
test fixture classes belong in the server parent classpath. Archive the exact
server and plugin files before launch; a running process must not reference
mutable Gradle outputs.

The initial acceptance harness uses the production main's explicit configuration
argument with `GRAVITINO_TEST` present. That environment switch also selects
`catalogs/catalog-lakehouse-delta/build/libs` and
`authorizations/authorization-ranger-delta/build/libs` below `GRAVITINO_HOME`.
It also loads provider configuration from
`catalogs/catalog-lakehouse-delta/build/resources/main`; point that path to the
immutable catalog package `conf` directory. Point the library paths only at immutable archived production package files; the switch
does not justify installing MiniGravitino or test fixtures. A normal deployed
server uses the ordinary package layout without that switch.

Provision a unique four-level definition, resource service, and linked tag service
in Ranger 2.8. The resource service binds one canonical metalake through
`ranger.plugin.spark.gravitino.metalake`. Preserve separate Ranger Admin,
Basic-authenticated plugin downloader, Gravitino service administrator,
Gravitino application/current actor, metadata reader, and query identities.
See the [adapter startup contract](../spark-delta-ranger/README.md) for the exact
Spark flags, SIMPLE metadata file, full-snapshot requirement, cache permissions,
pre-initialization audit logging guard, and query restrictions. Set the initial
Spark session configuration explicitly, before building the session:

```properties
spark.sql.extensions=org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension,io.delta.sql.DeltaSparkSessionExtension
```

A native-only value copied into session options can override the driver plugin's
SparkContext setting and will fail the companion's extension-order validation.

Application discovery requires local `CATALOG/USE_CATALOG/ALLOW` before catalog
wrapper initialization. That privilege grants no Ranger SELECT. A Ranger-only
column reader keeps only catalog visibility in Gravitino; the separate metadata
reader resolves registration/schema/table metadata. Gravitino table reads require
scoped catalog-use, schema-use, and table-select permissions; qualification of this
metadata permission composition is still pending. A hidden catalog is absent
from Spark registration even when metadata credentials could read it. Token query
attestation and broader application authentication remain unqualified.

Create ordinary roles with an admitted table SELECT object before assigning
membership. Creating an empty role and adding membership does not dispatch the
same Ranger initialization callbacks. `addUser` creates only the local Gravitino
user; the first relevant `grantRolesToUser` dispatches Ranger user creation.
Observed callback failure after local persistence requires independent Admin and
downloaded-version readback. Identical ordinary grant/revoke retries may skip
callbacks, and same-owner PUT repairs owner state only. Preserve actual request
attempts and completion evidence; gateway drain alone cannot settle unfinished
work in the owning Gravitino server.

## Explicit external acceptance gate

The `TestDeltaRangerDownloadedPolicies` test application uses public Gravitino
client calls, ordinary forwarding/recording gateways, and independent direct Admin
readback. Its current executable checkpoints are `bootstrap` and `fresh-principal`.
The separate `DeltaRangerDownloadedQuery` application receives fresh query commands
on stdin; final four-pair callback/query orchestration is still pending. The gateway
and query driver are packaged only as an independent test application, never in
either production plugin. Root-owned services and fixtures are provisioned and cleaned up separately.
Normal forwarding preserves the actual upstream status/body, including HTTP 204.
Controlled faults match exactly one explicitly owned method/path; request bodies
and authentication headers are never written to the attempt log.

The Gradle opt-in requires all of:

```text
-PrangerDeltaProductionEnabled=true
-PrangerDeltaProductionGravitinoUrl=<explicit production endpoint>
-PrangerDeltaProductionAdminUrl=<explicit direct Admin endpoint>
-PrangerDeltaProductionFixtureFile=<absolute owner-only JSON file>
-PskipDockerTests=false
```

Endpoints without explicit opt-in, or enabled execution missing either endpoint
or private file, fail setup. Properties map to JVM system properties
`gravitino.ranger.delta.production.{enabled,gravitinoUrl,adminUrl,fixtureFile}`.
Credentials never go in command arguments, tracked files, or reports.

The private JSON schema has `schemaVersion: 1`, `metalake`, `resourceService`,
`resourceDefinition`, `tagService`, `tagDefinition`, `gravitinoAdminPrincipal`,
`metadataPrincipal`, `subjects` (`freshCallback`, `columnReader`, `visibilityOnly`,
`groupReader`), `groups.query`, `rolesPrefix`, `rangerAdminCredentialsFile`,
`metadataConfigFile`, `sparkSecurityDirectory`, `sparkConfigurationFile`,
`sparkHomes` (keys `3.5.4`, `3.5.6`), and `evidenceDirectory`. Each `catalogs` entry
contains `name`, `database`, `table`, `nativeFixtureFile`, and `journalDirectory`.
Each native fixture file contains `{primary, table}`: `primary` has `endpoint`,
`region`, `bucket`, and `credentialsFile`; `table` preserves the root's original
native identity, version, schema JSON, location, expected rows, and object manifest.
The harness adopts the existing published native data; it does not reseed it.

The Admin credential properties file contains `username` and `password` only.
The metadata properties retain the adapter's exact `auth-type`, `principal`, and
`lifetime-seconds` schema. The Spark properties file contains immutable startup
configuration, including separately scoped S3A credentials, and remains private.
Spark homes identify real distribution homes containing `jars`, not virtualenv
roots. Evidence directories retain sanitized commands, original process exits,
actual source/artifact hashes, downloaded versions, rows, and attempt/completion
records. Raw logs and credential-bearing files remain owner-only.
