# Delta schema CREATE grants

This is the first production slice of the selected governed-creation workstream,
following the closed additive APPEND, MERGE and CDF checkpoints. It implements the
non-owner schema grant described in the approved
[write-foundation design](2026-10-30-spark-hub-write-foundation-design.md). It does
not by itself enable a Spark CREATE route or close governed creation.

## Intent and existing paths

Gravitino roles must be able to grant `CREATE_TABLE` on a specific Delta schema
and propagate only Ranger `create` to the canonical
`catalog / database / table=* / column=*` resource. This lets a non-owner receive
creation authority without acquiring table SELECT, UPDATE or ALTER authority.
`USE_CATALOG` and `USE_SCHEMA` remain metadata visibility with no Ranger access.

The Delta plugin overrides the Hadoop SQL mapping and translation, so superclass
CREATE support is not inherited. The existing Delta service definition already
contains `create`; its exact managed-policy selector, ordinary role naming and
membership callbacks supply the mutation lifecycle. Reuse those paths. The
existing provider still owns reservation, allocation, native evidence and
publication. No engine or registration changes belong to this slice.

## Approaches and engineering decision

Extend the existing plugin with the one missing schema mapping and its callback
tests. This is the smallest change consistent with the approved foundation.
Ranger-only manual grants would bypass requested Gravitino policy propagation;
changing the Hadoop SQL superclass would affect unrelated catalogs. Neither is
needed.

Limit the new translation to `SCHEMA / CREATE_TABLE / ALLOW`. Core Gravitino also
allows catalog and metalake CREATE grants, but this slice retains explicit refusal
of those forms. Parent-scope inheritance is not being silently approximated:
catalog and metalake grants can collapse to the same per-catalog resource, and
overlapping revoke semantics need their own evidence before wider admission.
This bounded scope is an implementation decision, not a claim that Gravitino
lacks those privileges.

## Contract

- Add `CREATE_TABLE -> {CREATE}` to `privilegesMappingRule()` and CREATE_TABLE to
  `allowPrivilegesRule()`. Keep metadata-object type admission unchanged.
- `translatePrivilege(SecurableObject)` accepts exact canonical schema CREATE,
  alone or with USE_SCHEMA, producing exactly one resource with exactly `create`.
  Repeated allowed CREATE entries, if supplied directly to the plugin, do not
  duplicate accesses. Empty and visibility-only inputs still produce no policy.
- Literal catalog/schema names pass existing validation before translator-owned
  wildcards are appended. Wrong catalog, wildcard input, empty/malformed names,
  DENY, table/column/catalog/metalake CREATE, and unrelated schema privileges
  reject before an Admin client operation. Preserve existing table translation.
- Existing role create/acquire/update/delete and direct-user/group grant/revoke
  callbacks maintain the schema grant. Removing CREATE from a mixed
  CREATE+USE_SCHEMA object removes only that role's `create`; visibility grants
  do not create data authority. Existing owner and other role policy items survive.
- Use the existing exact managed-policy selector. Ranger-authored access, mask and
  row-filter policies remain byte-for-byte unchanged across grant/update/revoke;
  an exact table policy cannot be mistaken for a schema wildcard policy. Duplicate
  owned candidates still reject rather than selecting an arbitrary policy.
- Startup does not rewrite existing live policies. An operator must explicitly
  grant/regrant through Gravitino after deploying the updated plugin. No automatic
  live Admin mutation, migration, API signature or service-definition change.

## Constraints and qualification

Gravitino owns registration; native Delta owns files/log/schema/transactions. No
core/API/server/client/storage-schema changes, engine fork, Glue compatibility,
raw-path fallback or replacement metastore. Keep source policies and existing
UPDATE+ALTER semantics unchanged. Target pins remain Spark 3.5.4/3.5.6, Delta
3.3.0/3.3.2, Java 17, Scala 2.12.21 and Ranger server/client 2.8.0.

This Java authorization change has no Spark/Delta version-specific path. Qualify
the complete authorization-ranger-delta unit suite once using actual Ranger 2.8.0
artifacts, focused new regression RED/GREEN, scoped Spotless/static checks, and
independent review. Retain original command/exit/XML and producing source/runtime
hashes; distinguish the mocked Admin callback fixture from a real Ranger server.
Do not repeat unchanged Spark matrices or start live services for this isolated
translation task. Real public grant/revoke propagation and non-owner Spark
creation on Ranger server 2.8.0/MinIO remain required for governed-creation closure.

## Following creation work

Trace and qualify operation-bound absence checks, schema-only native CREATE0,
reservation-bound authority and publication before initial WRITE1. Treat denied
and absent separately and keep CREATE/data/publication outcomes separate. Reuse
the existing publication lifecycle; no automatic replay of committed or unknown
operations. Ordinary inputs, transformations and full refresh follow the selected
sequence. Catalog-demo, consumer state and streaming remain deferred.
