---
title: "Delta Spark integration investigation"
license: "This software is licensed under the Apache License version 2."
---

# Delta Spark integration investigation

Status: discovery record supporting implementation planning. No runtime
integration has been implemented or validated by this investigation.

Planning artifacts:

- [Design baseline](superpowers/specs/2026-09-07-delta-spark-design.md)
- [Complete delivery sequence](superpowers/plans/2026-09-07-delta-spark-implementation.md)
- [First executable work package](superpowers/plans/2026-09-07-delta-spark-feasibility.md)

Repository baseline: `3e7b550d6a7452a96ffd195069f8443f9b97263a`.
Sources inspected on September 7, 2026.

## Objective and unresolved scope

Integrate Delta Lake with Gravitino through Spark, providing the catalog experience
available for Iceberg and propagating policies from Gravitino into Apache Ranger.
Evaluate a dedicated Delta catalog against reuse of existing catalog mechanisms.
Full Delta support remains the objective; external-table registration alone is
insufficient.

The engineer confirmed Spark 3.5.4 and 3.5.6, and Delta 3.3.0 and 3.3.2.
For planning, cover all four combinations unless the engineer specifies paired
deployments:

| Spark | Delta | Integration validation |
| --- | --- | --- |
| 3.5.4 | 3.3.0 | Pending |
| 3.5.4 | 3.3.2 | Pending |
| 3.5.6 | 3.3.0 | Pending |
| 3.5.6 | 3.3.2 | Pending |

The [upstream compatibility matrix](https://docs.delta.io/releases/) lists
Delta 3.3.x with Spark 3.5.x. This establishes the supported version family;
the four Gravitino integration combinations still require execution tests.

The engineer confirmed the policy ownership boundary:

- Gravitino manages and propagates its existing supported grants and revocations.
- Ranger manages column permissions, row filters, and masking policies using
  Gravitino metadata. Column grants are not authored through Gravitino.
- Spark must enforce the resulting Ranger policies on Delta operations. Keeping
  filter and mask authoring in Ranger does not remove their enforcement or
  coexistence from the integration's acceptance criteria.

The engineer subsequently made no Gravitino core changes a firm constraint.
Keep public privilege definitions/bindings, core storage, server REST contracts,
and client contracts unchanged. Implement through catalog, Spark connector, and
authorization extensions plus their build/configuration wiring. Registration
lifecycle and policy coexistence must both respect this boundary.

The engineer confirmed that Glue Catalog is the current metastore and will be
removed for multi-cloud support. Gravitino must own table registration. The target
architecture must therefore operate without Glue or an independently authoritative
Hive metastore. A Spark compatibility facade may be needed internally, but it must
resolve registrations through Gravitino rather than introduce a second registry.

The engineer confirmed Amazon S3 for the first release. Multi-cloud support
remains the architectural direction: registration must not depend on an AWS
catalog service, and storage-specific credentials and commit handling must remain
separate. Additional storage backends are not first-release acceptance targets.
The platform prevents two jobs from writing to the same dataset concurrently.
Use that platform guarantee as the writer-topology constraint, not Delta's
conflict detection. Credential deployment and supported Spark entry points still
need agreement. Ranger metadata discovery must use Gravitino.

## Existing implementation paths

Paths below are relative to the repository root.

| Concern | Reference | Observed behavior and implication |
| --- | --- | --- |
| Spark catalog discovery | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/plugin/GravitinoDriverPlugin.java` | Loads relational catalogs and registers `spark.sql.catalog.<name>` using the provider-to-class mapping. Unsupported providers are skipped. |
| Spark version adaptation | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/version/CatalogNameAdaptor.java` | Contains mappings for Spark 3.3, 3.4, and 3.5. Neither generic lakehouse nor Delta has a mapping. |
| Shared Spark lifecycle | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/catalog/BaseCatalog.java` | Creates and alters through Gravitino, then loads the native Spark table. Loading obtains Gravitino metadata before the native table. Delta must satisfy this ordering or deliberately specialize it. |
| Iceberg Spark delegation | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/iceberg/GravitinoIcebergCatalog.java` | Initializes native Iceberg `SparkCatalog`, translates properties, and handles credentials and optional REST routing. Provides the main integration reference. |
| Iceberg planner adaptation | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/iceberg/extensions/GravitinoIcebergSparkSessionExtensions.java` | Installs a custom planner strategy. Catalog and table wrappers alone are not the entire Iceberg integration. |
| Server catalog contract | `catalogs/catalog-lakehouse-iceberg/src/main/java/org/apache/gravitino/catalog/lakehouse/iceberg/IcebergCatalog.java` | Separates provider identity, operations, capabilities, and property metadata. A dedicated Delta provider should follow these contracts. |
| Current Delta support | `catalogs/catalog-lakehouse-generic/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaTableOperations.java` | Stores user-provided schema and location for external tables. Does not read or mutate the Delta log. Alter and purge throw unsupported-operation errors. |
| Delta format delegation | `catalogs/catalog-lakehouse-generic/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaTableDelegator.java` | Selects Delta table operations within the generic catalog; reuse avoids a new provider but still requires substantial lifecycle work. |
| Role updates | `core/src/main/java/org/apache/gravitino/authorization/PermissionManager.java` | Updates Gravitino role state, then executes authorization-plugin callbacks. Failure and recovery across that boundary must be tested. |
| Ranger translation | `authorizations/authorization-ranger/src/main/java/org/apache/gravitino/authorization/ranger/RangerAuthorizationHadoopSQLPlugin.java` | Maps Gravitino privileges and metadata objects into Ranger policies. Supports schema/table/column resources. |
| Ranger enforcement test | `authorizations/authorization-ranger/src/test/java/org/apache/gravitino/authorization/ranger/integration/test/RangerIcebergE2EIT.java` | Configures native Iceberg SparkCatalog with a Hive backend and Kyuubi Ranger extension. It does not establish that a Gravitino Delta Spark wrapper works. |
| Gravitino Spark authorization | `spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/authorization/GravitinoAuthorizationSparkSessionExtensions.java` | Installs a Gravitino privilege-check rule and parser wrapper. Its interaction with Delta and Ranger extensions needs explicit testing. |

## Architecture candidates

These are hypotheses for comparison, not implemented alternatives.

| Candidate | Reuse | Main work and tradeoff | Evidence needed to select it |
| --- | --- | --- | --- |
| Extend generic lakehouse plus a Delta Spark adapter | Existing Delta registration, entity store, format delegation | Add native Delta lifecycle, authoritative metadata loading, and Spark format dispatch. Mixed formats make catalog-wide Delta delegation more complicated. | Named catalog operations, schema freshness after native writes, staged creation, and policy identity remain correct without breaking other formats. |
| Existing metastore-backed registration plus native Delta Spark | Existing Hive-style catalog metadata and native Delta execution | Rejected as the target: the engineer requires Gravitino-owned registration and removal of Glue. An independent authoritative metastore would not satisfy that requirement. | Retain only as a native reference for comparison or migration input. |
| Dedicated `lakehouse-delta` provider plus Delta Spark adapter | Gravitino provider contracts, shared connector utilities, Ranger plugin | Gives Delta-specific properties and capabilities a clear home, but introduces a module and still needs a catalog backend and native Spark delegation strategy. | A prototype demonstrates native Delta command coverage and reliable coordination between catalog state and Delta commits. |

The current evidence rules out a provider mapping alone as a complete solution.
With Gravitino-owned registration confirmed, the provisional recommendation is a
dedicated Delta provider backed by Gravitino's entity store, plus a Delta Spark
adapter. Extending the generic catalog remains technically plausible and should
be compared in the experiment. Native Delta delegation must work in either case;
the placement of server code cannot resolve a Spark compatibility failure.

The generic catalog is more than a registration placeholder: its operations
resolve catalog/schema/table locations and delegate format-specific operations.
`LanceTableOperations` provides an existing pattern for refreshing stored schema
from the underlying dataset. These mechanisms are useful references regardless
of the provider choice. The reason to prefer a dedicated provider is the scope
of Delta-specific lifecycle, capabilities, and Spark integration, not a claim
that the generic catalog cannot own registrations.

Delta's documented configuration installs `DeltaCatalog` as `spark_catalog` and
adds `DeltaSparkSessionExtension`. This differs from the native Iceberg
`SparkCatalog` initialization used here. Named catalogs, session catalog fallback,
table wrappers, and command recognition must therefore be demonstrated rather
than inferred from Iceberg support. See [Delta batch documentation](https://docs.delta.io/delta-batch/).

## Policy propagation and enforcement

The existing reference flow is:

```text
Gravitino role / grant / revoke
  -> catalog authorization plugin
  -> Ranger Admin policies and roles
  -> Kyuubi Ranger plugin in Spark
  -> authorization of Spark operations
  -> Delta execution and storage access
```

The last two steps are a target flow for Delta, not a verified integration.
Gravitino metadata authorization and Ranger query authorization are separate
checks. A successful Ranger REST update does not prove enforcement by Spark.

The HadoopSQL mapping currently translates `SELECT_TABLE` into `read` and
`select`. `MODIFY_TABLE` expands into `read`, `select`, `update`, `alter`, and
`write`. Reuse preserves that broad privilege model; independent insert/update/
delete grants would require a separate evaluation.

Gravitino does not need column-permission, row-filter expression, or masking-policy
authoring APIs under the confirmed scope. Kyuubi documents column permissions, row filtering,
and masking, but that does not establish coverage of every Delta command. The
plugin can run without a Kyuubi server; deployment must still establish identity
and control security configuration.
See [Kyuubi 1.10.2 authorization documentation](https://kyuubi.readthedocs.io/en/v1.10.2/security/authorization/spark/overview.html).

Policy resource naming needs a collision test: two Gravitino catalogs with the
same schema and table names must not accidentally share permissions. The Ranger
HadoopSQL resource model uses schema/table/column names and a configured service;
catalog identity must be accounted for in the chosen deployment and mapping.

Direct path access, cached tables, long-running streams, and storage credentials
must be included in enforcement tests. Table authorization alone does not define
what a principal can do with independent filesystem or object-store credentials.

## Proposed acceptance matrix

Every row needs a pinned runtime, an operation result, and applicable authorization
assertions. This matrix identifies work; it makes no support claims.

| Capability | Required evidence |
| --- | --- |
| Catalog experience | Discovery, named `catalog.schema.table` resolution, namespace operations, and coexistence with Iceberg. |
| Table lifecycle | Create, register, CTAS, replace, alter, rename, drop; distinguish external and managed data ownership. Validate abort and retry behavior. |
| Batch access | SQL, DataFrame reads/writes, append, overwrite, and partition overwrite retain native Delta results. |
| Row changes | `MERGE`, `UPDATE`, and `DELETE`, including source and target authorization and denied operations leaving no commit. |
| Metadata freshness | Schema/property changes through native Delta are reflected correctly; stale catalog metadata cannot grant access to the wrong object. |
| Historical access | Time travel, history, restore, and change data feed obey the agreed policy model, including historical columns. |
| Streaming | Read/write, checkpoint restart, schema changes, and revocation during a running stream. Define the revocation boundary explicitly. |
| Maintenance | Optimize, vacuum, conversion, and other commands supported by the selected Delta release retain semantics and check privileges. |
| Table features | Enumerate the selected release's protocol features, including applicable column mapping, deletion vectors, constraints, defaults, clustering, and type evolution. Verify native behavior and metadata representation. |
| Policy lifecycle | Grant, deny, revoke, users/groups/roles, ownership, rename/drop/recreate, policy refresh, Ranger outages, and recovery after partial updates. |
| Isolation | Same table names across catalogs, path access, cached data, session identity changes, and credential scope. |
| Fine-grained policies | Ranger-authored column permissions, masks, and row filters use matching Gravitino metadata and affect Spark results. Direct Ranger column revocation takes effect. Gravitino grants/revokes preserve Ranger-owned policies. |
| Ranger metadata integration | Establish lookup through Gravitino; verify table/column discovery, freshness after schema changes, and consistent resource names. |
| Gravitino-owned registration | All named-table resolution and mutations use Gravitino. Verify the deployment starts and operates without Glue or Hive metastore access. |
| Unchanged Gravitino core | Use existing APIs/extension interfaces without modifying core, public privilege definitions, storage schema, server REST contracts, or client contracts. |
| S3 storage | Run applicable reads, writes, lifecycle, and credential-refresh tests on S3; verify Delta commit concurrency for the agreed writer topology. Keep storage integration separate for future clouds. |
| Glue transition | Proposed validation: import existing registrations, validate against Delta logs, compare identifiers and permissions, and demonstrate operation after Glue access is removed. Confirm migration tooling scope before implementation. |

The repository pins Spark 3.3.4, 3.4.3, and 3.5.3, Delta Kernel 3.3.0, and
Kyuubi authorization 1.10.2 in `gradle/libs.versions.toml`. A Kernel dependency
is not a Delta Spark runtime selection. The confirmed target matrix above uses
different Spark patch releases from the repository's current build pin. The
implementation must provide a reproducible way to validate all target pairs;
tests against Spark 3.5.3 alone do not establish target-runtime support.

## Implementation planning transition

The confirmed requirements are sufficient to start planning. The linked delivery
plan preserves the complete scope and splits production work into testable
packages. Its first package contains the native baseline and catalog/authorization
experiments needed to choose exact interfaces. Runtime inspection has been done;
runtime execution is the next evidence boundary.

Deployment entry points, Scala binary version, and S3 credential delivery are
explicit production-packaging checks. They do not prevent the initial experiment
using the documented repository defaults. The dedicated provider remains the
recommended architecture, with the generic provider retained for comparison at
the compatibility gate.

Planning confirmed that `Privileges.SelectTable` uses `TABLE_SUPPORTED_TYPES`,
which excludes `COLUMN`, and the HadoopSQL grant translation has no explicit
column case. The engineer chose to preserve that Gravitino contract and author
column permissions directly in Ranger. The plan therefore tests query enforcement
and metadata access through existing permissions rather than changing API
binding, validation, or serialization.

Verification so far is source inspection only. No Spark, Delta, Ranger, Docker,
unit, or integration tests were run. Claude memory tools were unavailable in this
session, so these notes preserve the findings in the repository.

## Pinned upstream source findings

The following inspection uses the confirmed Delta 3.3.0 and 3.3.2 targets and
Kyuubi 1.10.2 to match the repository's authorization dependency. Delta 4.0.0 was
examined before runtime selection and is retained below as background only; it
is outside the confirmed target matrix.

### Delta catalog delegation

In [DeltaCatalog 3.3.0](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala):

- `DeltaCatalog` extends `DelegatingCatalogExtension` and implements
  `StagingTableCatalog` and `SupportsPathIdentifier`.
- `createDeltaTable` constructs a Spark V1 table identifier and consults the
  session catalog for existing metadata and default locations.
- The callback to create through the catalog delegate is enabled only when the
  delegate's class name begins with `io.unitycatalog.` and no source query is
  present. A normal Gravitino delegate does not meet that condition.
- `loadTable` converts a Delta-backed Spark `V1Table` into a native `DeltaTableV2`.
  `alterTable` explicitly recognizes that native Delta type.

The downloaded [DeltaCatalog 3.3.2](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala)
is byte-for-byte identical to the 3.3.0 file. The delegation findings therefore
apply to both target Delta versions for this class. This comparison does not
establish that the rest of Delta or its dependencies are identical.

The inspected [DeltaCatalog 4.0.0](https://github.com/delta-io/delta/blob/v4.0.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala)
retains the session-catalog field and the Unity-specific creation callback gate.
Moving to that release alone does not eliminate the identified obstacle.

Design implication: copying the Iceberg pattern of constructing a native catalog
and wrapping tables cannot be assumed to preserve Delta creation, CTAS/replace,
or native type recognition. A dedicated server provider alone would not address
this Spark boundary. Do not impersonate Unity Catalog by adopting its package name
or use Delta test-only switches as the production integration mechanism.

### Kyuubi command authorization

The [Kyuubi 1.10.2 command descriptors](https://github.com/apache/kyuubi/blob/v1.10.2/extensions/spark/kyuubi-spark-authz/src/main/resources/table_command_spec.json)
include Delta delete, update, merge, optimize, and vacuum. Delete/update/merge
targets use the `UPDATE` action, merge also extracts its source query, and the
descriptors include URI extraction for path access. Their reliance on native
command names and plan fields makes wrapper compatibility a test requirement.

The [Delta authorization test suite](https://github.com/apache/kyuubi/blob/v1.10.2/extensions/spark/kyuubi-spark-authz/src/test/scala/org/apache/kyuubi/plugin/spark/authz/ranger/DeltaCatalogRangerSparkExtensionSuite.scala)
contains denied-user and allowed-admin assertions for creation, replacement,
alteration, inserts, updates, deletes, merges, optimization, vacuum, and path
operations. These are upstream test definitions inspected here, not tests executed
against this repository. No Delta-specific masking or row-filter suite was found
in that release's source tree; this is a coverage gap to investigate, not proof
that the features are unsupported.

### Recommended next experiment

Use Spark 3.5.4 with Delta 3.3.0 as the initial experimental baseline, then repeat
on the other three target pairs. Compare the native session catalog baseline
with a Gravitino named-catalog integration on the same runtime and storage:

1. Create two catalogs containing the same schema/table names and distinct data.
   Resolve each through its full name and inspect the actual native table type.
2. Exercise create, CTAS, replace, merge, and schema alteration. Inspect both
   catalog metadata and Delta log results, including failed staged operations.
3. Apply a Gravitino grant, verify the Ranger resource and subject, and run the
   operation as that subject. Revoke it, wait for the defined refresh boundary,
   and assert denial before another Delta commit.
4. Repeat with a path identifier and both source and target tables in a merge.
   Verify that resource identity and required permissions remain correct.

Use these results to choose between the generic and dedicated server provider.
Prefer a dedicated provider if Delta needs its own lifecycle, capabilities, and
credential configuration that cannot fit the generic format contract cleanly.
Prefer extending the generic provider if native delegation works through that
contract without unrelated format-specific branching. The metastore-backed
alternative is rejected as the target because Gravitino must own registration.

The full acceptance matrix above remains required after this experiment. Passing
these probes would establish an architectural direction, not full Delta support.

## Coexistence with Ranger-owned filters and masks

Source inspection identifies a compatibility concern in the existing Ranger
pushdown implementation that applies directly to the confirmed policy boundary:

- `RangerAuthorizationPlugin.preciseFindPolicy` selects policies by matching
  resources and rejects more than one match. The inspected HadoopSQL lookup
  path does not filter by access-policy type or an explicit Gravitino ownership
  marker. An access policy and a Ranger-owned mask/filter policy on the same
  resources therefore need a coexistence regression test.
- `removeSecurableObject` deletes a selected policy when its allow and deny
  item lists are empty. This decision does not inspect mask/filter item lists.
  Policy selection must prevent a Ranger-owned policy from entering that path.
- Metadata rename and drop callbacks also need an explicit ownership contract:
  preserve Ranger-authored policy content during ordinary grant changes, and
  define how resource changes update or invalidate policies without silently
  retaining access to a different table or column.

These are source-level findings, not reproduced runtime failures. The design
should isolate Gravitino-managed access policies from Ranger-managed column
access, filters, and masks and test their lifecycle together. Implement this
ownership handling in a dedicated authorization extension using existing SPIs,
without modifying core or shared authorization behavior.

No Ranger Admin resource-lookup implementation backed by Gravitino was identified
in the inspected authorization module. Authorization pushdown publishes policies;
it must not be assumed to provide Ranger UI metadata discovery. Evaluate a Ranger
resource-lookup adapter using Gravitino's catalog/schema/table/column APIs.
Do not make Ranger discovery depend on Glue or a separately maintained Hive
metastore. Spark resource extraction must use the same names.

## Proposed ownership model after Glue removal

This is a design proposal for review, not implemented behavior.

| Component | Authority |
| --- | --- |
| Gravitino registration | Catalogs, namespaces, stable object identity, table-to-location mapping, ownership, and access grants. |
| Delta transaction log | Committed table schema, partitioning, Delta properties, protocol features, data-file state, and history. Gravitino exposes a version-aware projection where needed. |
| Delta Spark runtime | Native reads, writes, transactions, schema evolution, and maintenance. Its catalog resolution must use Gravitino. |
| Ranger | Access-policy evaluation plus Ranger-authored column permissions, masks, and filters. Resource lookup uses Gravitino metadata. |
| Cloud storage integration | Location-specific filesystem access and credentials, separate from catalog registration. |

Creation and replacement span registration and Delta commits, so the design must
specify how incomplete operations are hidden, retried, and recovered. A successful
registration alone cannot count as a successful native Delta table creation.
Likewise, a completed Delta write must not require manually dropping and
re-registering a table to refresh its schema or preserve its policies.

Existing Glue-registered tables should be evaluated for registration at their
existing locations, validating schema and protocol from their Delta logs. Removing
Glue does not itself require rewriting the table data. Any data relocation or
cross-cloud replication is a separate scope decision and is not inferred here.

## S3 first-release design constraints

The existing `S3TokenProvider` and `S3TokenGenerator` are reuse candidates for
Gravitino credential vending. `CredentialPropertyUtils` contains format-specific
translation for Iceberg and Paimon; those property mappings do not by themselves
configure Delta's Hadoop filesystem access. The Delta adapter must explicitly
handle credentials on the driver and executors and refresh them for long-running
jobs. Runtime IAM roles remain an alternative deployment mechanism to evaluate.

S3 writer coordination is separate from Gravitino table registration. Delta
3.3.2's [S3SingleDriverLogStore](https://github.com/delta-io/delta/blob/v3.3.2/storage/src/main/java/io/delta/storage/S3SingleDriverLogStore.java)
uses a process-local path lock. Registering a table in Gravitino does not turn that
lock into cross-driver commit coordination. The [Delta S3 documentation](https://docs.delta.io/delta-storage/)
describes a DynamoDB-backed LogStore for multiple writers, requiring consistent
configuration among writers.

The engineer clarified that the platform natively prevents two jobs from writing
to the same dataset concurrently. This establishes the deployment constraint;
the design does not rely on Delta rejecting overlapping transactions.

Use Delta's standard single-driver S3 LogStore as the proposed baseline. A
DynamoDB-backed multi-driver LogStore is not required by the confirmed workload
scope. Validate sequential handoff between jobs and platform serialization during
retries, maintenance operations, streaming, and failed-job recovery. Any operation
that mutates the Delta log, including server-triggered DDL if introduced, must
participate in the same writer exclusion. Metadata-only registration does not
require becoming a second Delta writer.

The architectural recommendation remains a dedicated Gravitino Delta provider
with a Spark adapter and Ranger metadata lookup. S3 is its first storage backend,
not its catalog authority. Prove named-catalog native Delta behavior, policy
coexistence, and the selected commit mechanism before committing to implementation.
