# Delta production Ranger pushdown and fresh-query enforcement

This design extends the accepted Delta production provider/Spark runtime design.
The user authorized continuous subagent-driven implementation. Activation follows
acceptance of the continuous production-chain review; this document does not claim
Ranger implementation or live qualification has already happened.

## Binding requirements

- Gravitino owns registration; S3 Delta logs/data remain native authoritative state.
- No Gravitino core/API/server/client/storage-schema source changes. Use existing
  authorization/provider/client/Spark extension seams and optional modules.
- Spark 3.5.4 and3.5.6 × Delta 3.3.0 and3.3.2, Scala 2.12. Ranger 2.8.0 is mandatory
  across actual server/engine/download/audit dependencies, not merely a version label.
- Platform exclusion of two writer jobs per dataset is not HTTP/callback drain,
  a fencing token, cross-system atomicity or arbitrary-job protection.
- One four-level Ranger service is provisioned for exactly one Gravitino metalake.
  Catalog/database/table/column identities are preserved before deduplication and
  policy evaluation. Owner labels constrain mutations, not resource matching.
  The required service/definition binding key is
  `ranger.plugin.spark.gravitino.metalake`, which Ranger propagates in policy
  downloads. The server adapter and engine compare this value with their trusted
  canonical metalake; `authorization.ranger.delta.metalake` remains the Gravitino
  configuration property. There is no fallback to the earlier unpropagated service
  key. Initial provisioned services must migrate before engine activation.
- Gravitino manages its explicitly owned access policies and roles. Ranger-owned
  column policies, masks and row filters are preserved. Foreign models must survive
  Gravitino grant/revoke/owner operations byte-for-byte where unchanged.

## Production components

1. The `ranger-delta` authorization provider translates admitted Gravitino callbacks
   through the ordinary Ranger Admin client. Validate provisioned service definition,
   metalake and canonical catalog binding before Admin mutations. Bind against the
   actual CatalogEntity through the existing Delta provider initialization override;
   immutable trusted properties do not replace that entity check.
2. An optional Spark Ranger module owns validated engine leases, actual policy/role
   download, query-subject capture and separate scoped metadata-read clients. It
   reuses public Kyuubi 1.10.2 extraction/masking helpers while constructing complete
   catalog-bearing requests itself. Stock rules that omit catalog are excluded.
3. An optional no-arg Delta catalog subclass initializes the accepted adapter's
   metadata/actor suppliers. Defaultfalse string-only driver wiring selects it and
   its extension; base/default runtimes remain free of Ranger dependencies. The
   current mutation actor and authenticated query subject never become the metadata
   read identity. Each component closes only its owned resources.
4. A companion runtime contains the enforcement module and one reviewed dependency
   closure. Plain Kyuubi authz1.10.2 is distinct from the base runtime's Hive 1.11.0
   supplier; duplicate utility classes are excluded only with ABI/helper execution
   proof. Preserve the accepted private Guava/publicsuffix bindings. Native Spark,
   Scala, Hadoop and Delta stay provided; server authorization is separately packaged.

## Role, grant and ownership contracts

Ordinary Ranger access translation initially supports TABLE/SELECT_TABLE/ALLOW.
Additionally, canonical CATALOG/USE_CATALOG/ALLOW and SCHEMA/USE_SCHEMA/ALLOW
are explicitly supported as Gravitino-only visibility and contribute zero Ranger
access resources/items. The scoped metadata reader needs both visibility privileges
and table SELECT to satisfy Gravitino's existing metadata REST authorization.
Application subjects in the Ranger-only column-access matrix retain catalog
visibility only. Isolated callback-failure subjects have catalog and schema
visibility in both catalogs; visibility still contributes no Ranger data access.
Never inherit HadoopSQL's wildcard SELECT translation for visibility. Validate
every scoped role object for create/delete/membership callbacks, including mixed
visibility/SELECT roles. Update callbacks receive an unfiltered old role: validate
its identity and every explicit changed resource before side effects, without
translating foreign catalog grants from that context. Reject unqualified explicit
changes and retain strict target-catalog matching.

Driver catalog discovery precedes wrapper construction and uses application
identity. Qualify minimal catalog visibility without Gravitino table SELECT or
ownership. A separate metadata identity then resolves registration/schema/table
metadata for a query subject holding Ranger-only column access. Visibility alone
must not grant any SELECT. Hidden catalogs need a clear setup diagnostic; scoped
metadata-assisted discovery without application visibility is a separate feature.

Create ordinary acceptance roles with an admitted SELECT resource before assigning
membership so callbacks actually dispatch. Empty-role creation/membership dispatch
nothing; a first grant is not inherited role/membership reconciliation. Add real
REST regressions for that order and require explicit established-role/membership
readback or qualified replay. Automatic arbitrary-order convergence is unfinished
until its own implementation and tests exist.

Owner callbacks reconcile complete desired owned role/item state, then verify
Admin readback. Do not rely on inherited boolean returns or preOwner deltas: some
upstream entry paths discard false or swallow failures. Exact owner scope is
C/*/*/*, C/S/*/* or C/S/T/*, with owner `all` implying only the validated data-action
vocabulary, never delegateAdmin. Preserve separate owner scopes and ordinary grants.

Callbacks can fail after Gravitino persists state. Explicit same-owner PUT can
repair owner state after original requests/callbacks finish, but identical ordinary
grant/revoke retries may skip callbacks and cannot repair divergence. Test last-
privilege revoke failure and preserve unresolved divergence with authoritative Admin
and downloaded-version evidence. A serialized desired-state role/grant reconciler
is required before unrestricted rollout. Never repair by an unqualified temporary
regrant or silently equate metadata ownership with Ranger convergence.

Underlying HTTP PUT retries are real. No application retry is not no wire retry.
Unknown forwarded work remains unresolved until independently completed or the
actual owning upstream process terminates. Timeouts and local handler exit do not
permit reconciliation across an unfinished callback.

## Fresh SELECT enforcement

The first bounded gate covers fresh named SELECT projection, predicates and two-
catalog joins, top-level column access, CUSTOM/NULL masks and table row filters.
Preserve native DeltaTableV2 identity and the deployment invariant disabling native
catalog-update hooks. Derive identity from canonical registered scans, never SQL
text, aliases, current-catalog guesses or path mappings. Authorize original columns
before rewrites, including predicate/join columns; masking never grants access.

Use finite plan admission with explicit failure for unqualified commands/shapes,
raw paths, unsupported masks and ambiguous identities. Narrow internal Delta log
reconstruction allowance must not authorize arbitrary JSON/Parquet reads. Validate
mask/filter expression rewriting and marker order with actual rows and fresh
expression IDs; fixed-point analysis must not repeatedly apply decisions.
Top-level struct permission does not establish nested-leaf grants or struct masking.

The initial subject adapter qualifies non-token mode against the actual current
Hadoop UGI and its groups. A Kyuubi session-user string cannot establish identity
when it contradicts that UGI. Existing Gravitino JWT parsing partitions a client
cache and does not attest a token-to-UGI binding; reject token query mode until an
authenticated deployment bridge is qualified. Metadata clients may independently
use existing token/custom authentication without becoming the query subject.

The initial no-arg Ranger catalog wrapper additionally requires SIMPLE Gravitino
application/current-actor authentication. The common SIMPLE client uses Spark's
application user; Basic, OAuth2 and Kerberos can use a different configured identity
that the original Spark/UGI comparison does not establish. Reject those and unknown
modes before acquiring wrapper resources, retaining the distinct metadata-principal
checks. Broader application modes require their own effective-identity qualification.
This does not restrict Ranger's separate Basic-authenticated downloader or remove
the generic metadata-client builder's supported authentication modes.

Require a valid policy/role/service snapshot before evaluation; null/error/deny
rejects. A documented valid cache may continue during outage, with observed versions
and revocation effective after refresh. Fresh-analysis qualification does not prove
cached/reused-plan revocation or arbitrary-job security.

Every access/mask/filter resource matcher is case-sensitive with wildcards enabled,
token/request-expression replacement disabled. Initially reject literal `*`/`?`
resource names before Admin mutation/evaluation; typed descendant selectors remain
separate. Do not invent wildcard escaping. Broader name support needs qualification.

## Acceptance and remaining goal

Use ordinary public Gravitino REST/client callbacks, provision a uniquely owned
Ranger 2.8 service, and verify actual Admin state plus genuine engine policy/role
downloads in fresh Spark processes on all four pairs. Test grant/revoke, groups/
roles, column-only metadata resolution, different catalog masks/filters and joins,
foreign-policy preservation, startup/cache/refresh and negative access paths.
Keep fixture callbacks and live deployed evidence distinct. Root owns local services
and isolated configuration; do not change shared Ranger/demo state or publish.

This stage does not complete full Delta/Ranger support. Subsequent gates include
remaining native lifecycle/features, all remaining mask expressions, DML/DDL/source-
target authorization, streaming/restarts, maintenance/history/time travel, caches,
role/grant reconciliation, authentication expiry and supported launcher boundaries.
Admin metadata lookup stays disabled until its separate production loader/client
acceptance. Isolated catalog-demo integration follows production qualification.


Ranger startup logging is part of the credential boundary: the optional adapter
checks that `org.apache.ranger.audit.provider.AuditProviderFactory` cannot log at
INFO before touching the singleton, because the pinned implementation dumps its
configuration there. Deployment must provide WARN-or-stricter startup logging.
No global logging mutation or secret-bearing diagnostic is permitted.

The initial engine contract uses Ranger full-snapshot policy downloads. Reject
`ranger.plugin.spark.supports.policy.deltas=true` at trusted startup: incremental
updates may reuse an authorization context and are not qualified by the completed
context binding contract. The default absent/false setting remains supported. This
Ranger transport restriction is independent of Delta Lake table feature support.

Privilege extraction must preserve canonical scan identity even though the pinned
CatalogTableTableExtractor omits catalog names. A private extraction-only copy may
use opaque per-scan identifiers with an exact local map while preserving output
ExprIds, expressions and owners. The original plan remains the executed plan. No
name/path/position heuristic may reconstruct catalogs after helper aggregation;
unknown, missing or ambiguous results reject. Both join orders and asymmetric
original column references require executed qualification.
