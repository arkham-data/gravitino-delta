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

# Optional Spark Delta Ranger adapters

This optional module supplies the native Delta catalog wrapper, separate metadata reader,
policy engine and bounded fresh SELECT extension. The base/default Delta runtime has no
Ranger dependency. Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2 and Scala 2.12 remain deployment inputs.
Packaged four-pair public Gravitino/Ranger acceptance is a separate qualification gate.

## Fresh SELECT startup and boundary

Configure the Gravitino driver plugin before creating the first Spark session. Both
`spark.gravitino.enableDeltaSupport=true` and
`spark.gravitino.enableDeltaRangerSupport=true` are required; Ranger support defaults to false.
The string-only common wiring selects `DeltaRangerCatalogSpark35` for application-visible
Delta catalogs and orders `DeltaRangerSparkExtension` before `io.delta.sql.DeltaSparkSessionExtension`.
Use the full package prefix `org.apache.gravitino.spark.connector.delta.ranger.` for the two
optional classes. Exactly one companion and one native extension are required in that order.
Stock `org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension` conflicts and rejects.
Keep `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` and
`spark.databricks.delta.catalog.update.enabled=false`. Each catalog still needs its explicit
`spark.sql.catalog.<catalog>.journal-directory` from the base adapter contract.

Bind `spark.gravitino.deltaRanger.service` and `spark.gravitino.deltaRanger.definition` to the
provisioned Ranger service/definition. Existing `spark.sql.gravitino.uri` and
`spark.sql.gravitino.metalake` remain the trusted Gravitino endpoint/metalake. The wrapper reads
these immutable SparkContext startup values, never table properties or query/session overrides.
The effective Gravitino application/current-actor authentication must be SIMPLE:
`spark.sql.gravitino.authType=simple`, which is also the common client's default when the key
is absent. Explicit SIMPLE is case-insensitive. Basic, Kerberos, OAuth2, token, unknown and empty
configured modes reject before metadata configuration or engine acquisition; their authenticated
identity separation remains a later qualification gate. This restriction applies to the Gravitino
application/current-actor client; Ranger downloader authentication retains the Task2 contract.

Set `spark.gravitino.deltaRanger.metadata.config` to an absolute regular owner-only properties
file (for example mode 0600), containing exactly these keys:

```properties
auth-type=simple
principal=separate-metadata-reader
lifetime-seconds=300
```

The principal must be explicit and distinct from application/current-actor identity; lifetime
is 1–86400 seconds from the first metadata read attempt, including construction. This wrapper
currently qualifies only existing SIMPLE metadata authentication. Basic/custom-token wrapper
configuration and authenticated token query identity remain separate gates. The lower-level
metadata adapter retains its existing explicit authentication-builder API. Endpoint, metalake
and catalog cannot be overridden by the metadata file. The wrapper remains lazy about metadata,
uses application discovery only for visibility, and looks up the current actor afresh for inherited
mutation transport. It closes its owned metadata client/engine lease on close, failed initialization
or Spark application end; it never closes the application/current-actor manager.

Application discovery must already expose each catalog using the local-only
`CATALOG/USE_CATALOG/ALLOW` visibility grant. A hidden catalog is absent from Spark registration;
metadata credentials do not discover it. Visibility grants no table SELECT. The separate reader
needs scoped registration/schema/table metadata reads, while query SELECT decisions use the
current authenticated UGI and Ranger policies. Actual public visibility/revocation/metadata-only
transport composition is part of the packaged acceptance gate.

Admitted fresh plans are resolved named native Delta scans with projections, predicates,
top-level columns, ordinary aliases and inner equijoins/predicate joins. Original projected,
predicate and join columns are authorized for all scans before any mask/filter request.
CUSTOM expressions and typed NULL masks preserve original column types; cross-column mask
expressions, nested references, UDFs and unsupported expressions reject. Row filters operate
on original rows before masks; user predicates operate on the rewritten columns. Table and
column identity remains case-sensitive. Count, aggregates and columnless SELECT are currently
rejected, along with outer joins, views, subqueries, raw paths, DML/DDL, streaming, time travel
and maintenance commands. Standard mask kinds beyond CUSTOM/NULL remain unqualified.

Pinned Kyuubi V1 privilege extraction loses catalog names. The companion first characterizes
and validates the original whole-plan extraction, then extracts an otherwise identical copy
whose per-scan descriptor carries a unique opaque table token. It maps each result through that
exact local token-to-canonical-scan map and independently validates original ExprId references.
It never groups catalogless results by database/table or changes execution descriptors/ExprIds.
Unknown, missing, duplicate or lossy helper resources reject. Public row-filter/mask markers,
stage-1 ExprId remapping and marker elimination are used after original authorization; stock
request-generating authorization/filter/mask rules are not installed.

The native optimizer delegate runs only after authorized fresh analysis, before Delta's own
`PrepareDeltaScan`. Its private synchronous metadata scope preserves native reconstruction and
caching while constraining observed internal metadata RDD/encoder routes. A narrow protected
`getDeltaScanGenerator` override calls the public native superclass once, registers only the actual
selected Snapshot's statistics schema for that authorized index, and returns it unchanged. No
snapshot/schema registration survives the delegation. Scope clears on exceptions and does not
propagate to other threads; user RDD/raw scan routes outside it reject. Native DeltaLogFileIndex
reconstruction and exact readless SingleAction/Row encoder bindings are separate exceptions;
AddFile binding requires the scope. All binding exceptions require an empty LocalRelation and
reject child reads, nonnative targets, UDFs and subqueries.

Native reconstruction needs two narrowly qualified ScalaUDF forms: Delta's canonical path
function and its nondeterministic four-argument size collector. Admission jointly requires the
active scope, native leaf/snapshot lineage, function class/family, pinned Delta classloader and
CodeSource, signature/nullability/determinism, and exact argument lineage. It rejects other
same-signature functions and unrelated arguments. Python UDFs, subqueries and commands reject.
The native private testing callback is neither called nor replaced by the companion; trusted
launcher code must not inject callbacks or replace pinned classes. This finite reconstruction
qualification covers the tested native fixture routes, not every checkpoint/table-feature shape.
These exceptions do not establish metadata confidentiality or arbitrary-job security.
A per-analysis subject marker prevents repeated masking and rejects a changed subject; it is not
revocation protection for cached/reused plans. Those deployment and full Delta-operation gates
remain open.

## Deployment

Use plain `kyuubi-spark-authz_2.12:1.10.2` and the complete Ranger 2.8.0 dependency closure.
The base Delta runtime alone supplies its 33 Kyuubi utility classes; this module excludes
`kyuubi-util` and `kyuubi-util-scala_2.12` after member-descriptor and executed helper checks.
Preserve the base runtime's private Guava and relocated publicsuffix classes. Companion
packaging and qualification on all four Spark/Delta pairs remain separate runtime gates.

Provision the optional provider's closed four-level service definition and one service for
exactly one canonical metalake. The mandatory service binding is
`ranger.plugin.spark.gravitino.metalake`. Ranger propagates this key in policy downloads;
the earlier `gravitino.metalake` key is not accepted. Existing initial fixtures/services must
adopt the propagated key before activation. Keep `authorization.ranger.delta.metalake` as
the Gravitino provider's trusted catalog property.

Place ordinary `ranger-spark-security.xml` on the driver startup classpath **before any
Kyuubi singleton access**. Set `ranger.plugin.spark.service.name`,
`ranger.plugin.spark.policy.rest.url`, and an existing absolute
`ranger.plugin.spark.policy.cache.dir` accessible only by its owner (POSIX mode 0700).
Use Ranger's normal deployment authentication and credential mechanisms. A Basic plugin
downloader uses a separate account authorized to download this service's policies and roles;
it must not be the Ranger Admin account or the query/metadata user. Keep credential-bearing
configuration private. Set bounded HTTP connection/read timeouts and a suitable policy polling
interval using ordinary Ranger configuration.

Ranger 2.8's `AuditProviderFactory` dumps configuration properties at INFO, including Basic
passwords. Before any plugin initialization, configure the logger named
`org.apache.ranger.audit.provider.AuditProviderFactory` at WARN or stricter in the deployment's
logging configuration. The production adapter checks the SLF4J logger by name before accessing
the Kyuubi singleton and rejects unsafe logging. It does not modify application logging globally.
For Log4j2, load a private startup configuration with `-Dlog4j.configurationFile=...` containing
`<Logger name="org.apache.ranger.audit.provider.AuditProviderFactory" level="warn"/>`.

## Consumer contract

`DeltaRangerConfig.fromClasspath(service, definition, metalake)` captures the trusted effective
configuration without retaining or rendering credential maps. Pass it to
`DeltaRangerPolicyEngine.acquire(config)`. Acquisition initializes the actual pinned Kyuubi
singleton/Ranger downloader once; call `requireReady()` before query execution. Same-binding
leases share the engine. Other service/metalake/configuration bindings reject. Each lease closes
only its ownership; the final close drains the downloader and shuts down its audit factory.
That singleton is terminal after final close, and reuse requires a fresh driver process.
Do not install or initialize another owner of the stock singleton in the same driver.

Full Ranger policy snapshots are required. Leave
`ranger.plugin.spark.supports.policy.deltas` absent or set it to `false`; effective `true`
rejects during trusted configuration validation before singleton initialization. Both copied
and in-place incremental Ranger policy updates can reuse auth contexts, so disabling only
`supports.in.place.policy.updates` is insufficient. This is a Ranger policy-download transport
constraint, independent of Delta Lake table features.

Readiness requires nonnegative policy and role versions, the bound service, a downloaded
metalake binding, a complete validated definition, and matching downloaded roles. The default
empty engine is never ready. Each evaluation rechecks the snapshot; null, error or denial rejects.
Readiness is tied to an immutable record captured by the public completed-auth-context listener,
registered before startup cache/downloader initialization. Both public context identities must
match that record; constructing/publishing a replacement rejects. Invalid downloaded contexts
retain their rejected binding even when later same-context role notifications occur. Context
replacement during evaluation rejects even if the downloaded version number is unchanged.
`RangerAccessResult.policyVersion` is an individual policy revision, not an engine snapshot ID.
A refresh observed during evaluation rejects so the caller can reanalyze. Ranger may continue
using a valid last-known-good cache during an Admin outage. `versions()` exposes immutable
service/definition/policy/role observations; policy and role refresh are independent. Revocation
is effective after the corresponding refresh, with no instantaneous or reused-plan guarantee.

`DeltaRangerQuerySubject.current(session)` captures the current engine UGI's short name and
actual groups. Any Kyuubi session-local user must agree with that UGI. Ranger resolves downloaded
roles at evaluation. The metadata identity never substitutes for the subject. Token query mode
currently rejects: the existing Gravitino token cache identity parses unverified claims and
cannot attest a token-to-UGI binding. An authenticated token query bridge is a remaining gate;
separate metadata clients may still use normal custom/token authentication.

Build an immutable `DeltaRangerResourceKey` from canonical registration and original privilege
extraction. Call `select(catalog, database, table, originalColumns, owner)` for **every** original
table, retaining predicate and join columns before rewrites. Empty columns yield one explicit
table-wide SELECT resource. Catalog, database, table, column, access and owner participate in
identity. Literal wildcard names reject; null column is the distinct table-wide selector.
Fresh `AccessResource` instances populate their catalog option and resource-map catalog before
any evaluation; their inherited equality is never used for deduplication.

Authorize original columns before calling `mask` or `rowFilter`. Mask outcomes are `NONE`,
`NULL` (the consumer must create a null literal with the original Spark data type), or
`EXPRESSION`. CUSTOM substitutes a safely quoted canonical column for `{col}`. Additional
mask kinds reject explicitly. The pinned public Kyuubi masking API is characterized with real
models for standard masks/transformers, but it substitutes raw column names; prequoting also
changes policy identity. It is therefore not selected for additional mask types here.
Expressions are syntax-checked; the plan consumer must still validate scope, finite expression
and plan admission, attribute identity, ordering, and fresh rewrite markers.

Construct `new DeltaRangerMetadataClient()` for the no-arg wrapper's super-constructor supplier.
Configure it once after trusted initialization with endpoint, canonical metalake/catalog,
a positive lifetime of at most 24 hours, and a trusted callback using the existing
`GravitinoClient.ClientBuilder` authentication methods. Authentication must be explicit and
cannot change the bound metalake. Client creation is lazy; the monotonic lifetime starts on the first read attempt and includes
client construction. Deadline checks reject reads and results independently of timer execution.
Construction and resource cleanup never hold the lease monitor; timer callbacks only terminate
state and dispatch cleanup to separate daemon workers. A late-created client is closed, and a
concurrent read while the initial client is being constructed rejects for retry. Explicit close
terminates state promptly before closing only the owned metadata client outside the monitor;
its resource cleanup may itself wait on the transport. Expiration does not wait on other leases'
cleanup. Provision that credential with only the
required metadata read permissions. Keep the accepted catalog constructor's actor supplier on
the existing current-actor transport. Application catalog discovery uses its separate minimal
Gravitino visibility grant, never table SELECT inferred from visibility.

These adapters do not establish DML/DDL, cached-plan, streaming, arbitrary-job or full Delta
security. Companion packaging, live Gravitino transport acceptance and the remaining runtime
matrix are subsequent gates.
