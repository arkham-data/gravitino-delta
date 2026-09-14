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

This module supplies the policy-engine, query-subject and metadata-read boundaries for the
optional Spark 3.5 Delta wrapper. It does not install stock Kyuubi rules or extensions, and it
is not a query-plan enforcement extension by itself. The base/default Delta runtime has no
Ranger dependency. Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2 and Scala 2.12 remain deployment inputs.

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
security. Fresh-query plan enforcement, companion packaging, live Gravitino transport acceptance,
and the remaining runtime matrix are subsequent gates.
