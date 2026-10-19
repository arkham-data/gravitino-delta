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
policy engine, bounded SELECT collection extension and matching-schema named batch
append through insertInto or existing-table saveAsTable. The base/default Delta runtime
has no Ranger dependency. Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2 and Scala 2.12 remain
deployment inputs. The [October 18 append result](../../../docs/delta-native-governed-append.md)
records accepted four-pair packaged public-grant evidence and its limits. Explicit
writer options are empty; session transaction identity/version require
`spark.databricks.delta.write.txnVersion.autoReset.enabled=false`.

## Shared startup and SELECT boundary

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

Ordinary named loads read the PUBLISHED registration before and after native Delta loading
through that same metadata lease. Each read includes table receipt and schema conversion;
expiry or closure before it finishes rejects the result. Registration changes during the load
also reject. A completed load binds the exact native descriptor, relation and file index to its
catalog owner, preserving the native Delta table. Equal descriptor copies or a replacement index
at the same path do not satisfy that binding.

The binding retains the complete native schema, protocol, partition ordering and column-mapping
evidence, and checks the native logical schema against Gravitino columns. Partitioned tables
and name-mapped columns have
native fallback tests on all four supported version pairs. Schema forms where Delta drops
`NullType` columns currently reject because the resulting schemas differ. The installed collection guard rechecks the retained registration and original authorization
obligations before each supported collection.

Application discovery must already expose each catalog using the local-only
`CATALOG/USE_CATALOG/ALLOW` visibility grant. A hidden catalog is absent from Spark registration;
metadata credentials do not discover it. Visibility grants no table SELECT. The separate reader
needs scoped registration/schema/table metadata reads, while query SELECT decisions use the
current authenticated UGI and Ranger policies. Actual public visibility/revocation/metadata-only
transport composition has separate [SELECT evidence](../../../docs/delta-ranger-production-results.md)
and [bounded append evidence](../../../docs/delta-native-governed-append.md).

The fresh-plan logical grammar accepts resolved named native Delta scans with projections,
predicates, top-level columns, ordinary aliases and inner equijoins/predicate joins.
Native execution remains limited to the qualified routes described below. Original projected,
predicate and join columns are authorized for all scans before any mask/filter request.
CUSTOM expressions and typed NULL masks preserve original column types; cross-column mask
expressions, nested references, UDFs and unsupported expressions reject. Row filters operate
on original rows before masks; user predicates operate on the rewritten columns. Table and
column identity remains case-sensitive. Count, aggregates and columnless SELECT are currently
rejected, along with outer joins, views, subqueries, raw paths, unsupported DML/DDL,
streaming, time travel and maintenance commands. The separately qualified write surface
is limited to the two named batch append frontends above. Standard mask kinds beyond
CUSTOM/NULL remain unqualified.

Pinned Kyuubi V1 privilege extraction loses catalog names. The companion first characterizes
and validates the original whole-plan extraction, then extracts an otherwise identical copy
whose per-scan descriptor carries a unique opaque table token. It maps each result through that
exact local token-to-canonical-scan map and independently validates original ExprId references.
It never groups catalogless results by database/table or changes execution descriptors/ExprIds.
Unknown, missing, duplicate or lossy helper resources reject. Public row-filter/mask markers,
stage-1 ExprId remapping and marker elimination are used after original authorization; stock
request-generating authorization/filter/mask rules are not installed.

Ordinary loading and native scan preparation use finite, owner-bearing operations. Loading
carries the actual PUBLISHED observation through native loading and seals against the completed
registration binding. Preparation consumes the complete original decision receipt. Actual state
and statistics getters seal their exact returned cached RDD identities/full outputs or an owned
uncached reconstruction recipe; reentrant candidates remain analysis-only until capture completes. A successful close freezes issuance and preserves
sealed evidence, while failure invalidates that operation's evidence. Later cache use requires
explicit rebinding through the current owner's actual snapshot getters. After Delta uncaches a
retained snapshot, a matching-files continuation may preserve the original successful preparation's
exact state-cache evidence while completing both fresh getter observations. Empty encoder helpers
remain analysis-only and cannot authorize execution.

Prepared indexes retain their original scan occurrences and selected snapshot. A delayed
`matchingFiles` callback, including one on a broadcast worker, enters a fresh owned operation and
passes the actual filters unchanged to Delta. The separate no-skipping index covers snapshot,
file-list and size callbacks while preserving Delta's snapshot selection. Neither route inherits
thread-local authority. Scope clears on every exit; index copies do not acquire authority, and
serialized references retain no driver authorization payload.

Native reconstruction includes qualified JSON/checkpoint reads and positional unions of log
reads from the same owned table log. It binds actual function objects and preserves captured
function occurrence counts during reclassification, accounting for canonical-path projection
pushdown by exact log leaf and path field. Delta's canonical-path and size-collector UDFs require
their pinned origin, signature and argument lineage. Spark's typed `AddFile` select
requires its exact encoder, field/filter structure and owned returned state lineage. Arbitrary same-schema
RDDs, substituted functions, mixed log roots, Python UDFs, subqueries and commands reject. The
native private testing callback is neither called nor replaced; trusted launcher code must not
inject callbacks or replace pinned classes. These finite routes do not cover every checkpoint or
table-feature shape and do not establish metadata confidentiality or arbitrary-job security.

Analysis resolves each original scan through its canonical wrapper's exact registration binding.
A driver-owned decision receipt retains every original occurrence, resource, subject/group set,
engine publication stamp and captured mask/filter decision, including absent filters and NONE
masks. The complete batch uses one policy evaluation time. Registration publication, engine,
subject and SQL configuration changes observed during analysis reject the plan. Decisions are
captured once and used for rewriting; analyzer reentry validates the receipt without remasking.

The installed driver-only collection guard calls the original receipt's action validator.
It reauthorizes every original occurrence before reevaluating any mask/filter, using one fresh
policy time for the complete action. Changed decisions, including NONE/absent decisions, reject
without rewriting the retained plan. Binding-owned native operations inspect each retained exact
log between fresh metadata reads. Compatible data-only advancement between actions is accepted;
the retained plan may still use its previously selected snapshot. Observed publication/native
version drift within the action, registration replacement, raw schema/protocol/partition/mapping
changes, expiry, and changed subject/configuration/engine publications reject. Final metadata
work precedes the final subject and engine checks. These are optimistic observations, not a write
fence or a latest-rows promise.

For the qualified grammar, both first and repeated `collect`/`collectAsList` on the same Dataset
enter one visible outer guard. It compares the actual physical child to its sealed binding before
validation and again after metadata callbacks, before delegating collection. A defense-in-depth
execution check also remains. The guard preserves Spark's visible child, stock cleanup traversal,
code generation, row/columnar transitions and qualified exchange reuse. Copied or deserialized
evidence cannot authorize changed executable sources, expressions or outputs. Canonicalized
guards and deserialized driver references are inert.

Before native preparation, the receipt compares the incoming plan with a replay of the approved
plan through the qualified Spark optimizer prefix. Actual source identities, expressions,
attributes and child structure must agree; a copied tag cannot authorize a changed plan. Unknown
optimizer rules reject before invocation. Tags carry an inert serializable reference with transient
authority, so deserialized references cannot authorize a query. Empty-result optimization retains
all original authorization obligations. User SQL caches, row-tracking normalization and other
unqualified transformations reject rather than acquire authority from a copied receipt.

The installed asymmetric self-join route uses the owned delayed callback, retaining both
original scan obligations even when the same table is loaded twice. Empty-result optimization
also retains every original obligation, including when Spark collapses a masked projection to
an empty relation. Physical bootstrap independently lowers the sealed complete prepared recipe through exact pinned
optimizer/planner/preparation manifests. Custom optimizer, planner, columnar or adaptive rules
reject before invocation. It compares the actual candidate directly; collection never replans or
runs optimizer rules. Delayed native pre-CBO planning uses only a sealed successful native owner,
with a finite structural correspondence for generated nested-field aliases.

User AQE must be disabled (`spark.sql.adaptive.enabled=false`); enabling it rejects without changing
the setting. Native AQE retains its existing enabled setting and independently binds input and
initialized plans, exact hidden exchange stages and complete ordered coalesced reducer coverage.
Partial/skew/local/unknown adaptive shuffle mappings are unqualified. User SQL cache substitution,
including populated or nested caches carrying copied receipts, rejects; owned native persisted
RDDs remain supported. These controls qualify Dataset collection, not arbitrary raw RDD/iterator
or execute APIs, running-action cancellation, full native-index serialization, or every Delta route.
The [append result](../../../docs/delta-native-governed-append.md) separately qualifies
bounded raw native recache in both warm orders; it does not enable governed cached
SELECT. Broader deployment and full Delta-operation acceptance remain separate gates.

## Deployment

Use plain `kyuubi-spark-authz_2.12:1.10.2` and the complete Ranger 2.8.0 dependency closure.
The base Delta runtime alone supplies its 33 Kyuubi utility classes; this module excludes
`kyuubi-util` and `kyuubi-util-scala_2.12` after member-descriptor and executed helper checks.
Preserve the base runtime's private Guava and relocated publicsuffix classes. Companion
packaging has separate [SELECT](../../../docs/delta-ranger-production-results.md) and
[append](../../../docs/delta-native-governed-append.md) evidence on all four pairs;
retain their exact runtime profiles and qualification boundaries.

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

The qualified write surface is the [bounded named append milestone](../../../docs/delta-native-governed-append.md),
including its packaged public-grant matrix. Broader DML/DDL, governed cached queries,
streaming, arbitrary-job security and full Delta support remain subsequent work.
