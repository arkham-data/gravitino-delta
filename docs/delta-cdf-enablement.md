# Delta CDF enablement through the catalog

Status: all four pinned runtime/live cells pass with Ranger 2.8.0 and MinIO. This milestone covers named CDF enablement and
disablement with ordinary governed reads and append. Governed CDF consumption
and full Delta support remain separate work; see the
[capability status](delta-integration-status.md).

## Operation and authorization

The installed Delta Ranger extension accepts a registered table name and one
canonical property with a literal lowercase boolean value:

```sql
ALTER TABLE catalog_name.database_name.table_name
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

ALTER TABLE catalog_name.database_name.table_name
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false');
```

The command requests table-wide Ranger `alter` for the application subject.
SELECT and append remain separately authorized by `select` and `update`.
An ALTER-only policy can authorize this property operation without SELECT.
Mixed properties, alternative key spelling, invalid values, UNSET, path/versioned
targets, other DDL and direct unbound catalog calls are outside this command.

The Delta authorization plugin maps Gravitino `MODIFY_TABLE` to both `update`
and `alter`. Existing Ranger policies do not change automatically on upgrade.
Follow the explicit [migration contract](../authorizations/authorization-ranger-delta/README.md#existing-modify-policy-migration);
repeating an unchanged public grant may not invoke a policy callback. Fresh
public grants, revokes and regrants are exercised in the live qualification;
these owned fixtures do not constitute migration of existing deployments.

Native Delta performs the metadata transaction and protocol changes. Gravitino
owns registration and observes the resulting native metadata. No second
metastore, Gravitino core change or REST-side Delta writer is introduced.
The deployment contract remains one writer job per dataset.

## Commit and recovery behavior

The command binds its table, catalog plugin, session, subject, configuration and
policy state, then verifies its own native metadata/protocol publication. A
copied, serialized or consumed command cannot issue another mutation. Changed
catalog identity is checked before execution and across native callbacks.

A failure after a proven native commit reports committed with refresh unconfirmed.
If policy changes after native publication but before its first observation, a
bounded read-only check classifies the captured transaction's publication without
requiring new execution authority. If that proof cannot establish ownership and
stable bytes, the result is explicitly uncertain. Neither result means rollback
or authorizes retrying ALTER. Execution remains denied after revocation; outcome
classification does not install a snapshot, refresh registration or resume work.

Recovery observes the named table again when authorized. The implementation does
not automatically replay ALTER or delete uncertain data. Disabling CDF retains
the upgraded native protocol and historical changes from the enabled interval.

## Qualification

All four pinned runtime/live cells pass with Ranger 2.8.0 and MinIO.

| Spark | Delta | Focused local evidence | Final packaged live result |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 38 final-fix cases plus retained continued-use/cache/provider evidence | Pass |
| 3.5.4 | 3.3.2 | 28 retained cases plus 8 final outcome cases | Pass |
| 3.5.6 | 3.3.0 | 36 cases, including the 8 outcome cases | Pass |
| 3.5.6 | 3.3.2 | 36 cases, including the 8 outcome cases | Pass |

All cells use JDK 17 and Scala 2.12.21. The retained Task 3 continued-use/cache/
provider cases and the prior 28 cases on 3.5.4/3.3.2 precede the final outcome fix
and remain accepted for unchanged paths. The first pair’s 38 covering cases
execute the final outcome fix. New outcome cases run on every pair. The live
matrix executes the final shared artifact on every pair and does not warm native
Spark caches. The 3.5.6 local suites include both retained-cache orders and the
clone-required denial; their compiled classes are distinct evidence from the
shared shaded artifact.

The final live cells use production discovery, actual Ranger 2.8.0, MinIO,
packaged Gravitino and distinct application, metadata and native-oracle identities.
Each uses a fresh owned fixture and the same reviewed optional Ranger artifact,
with its actual stock Spark/Delta jars and loaded origins recorded. Per-pin local
compilation evidence is recorded separately from packaged live execution.

Each live scenario checks:

- Six initial named authorization denials and a revoked disable, with native data
  and registration unchanged at the denied boundaries.
- Public MODIFY grant, revoke and regrant producing exactly UPDATE plus ALTER,
  preserving independent SELECT, and applied through authenticated policy
  downloads in the same application JVM.
- Native versions 0 → 1 (enable) → 2 (enabled append) → 3 (disable) → 4 (disabled
  append). Rejected disable retains version 2.
- Ordinary masked/filtered reads and matching-schema append after both changes.
  The final native table contains seven rows with exact duplicate multiplicity.
- An independent native CDF oracle: interval [1,2] has three inserts at version 2,
  including two identical masked id-1 rows; that interval is unchanged after
  disable. Interval [3,4] rejects with `DELTA_MISSING_CHANGE_DATA`.
- Complete raw object inventories and hashes, stable registration/native identity,
  schema, owner and column IDs; persisted H2 versions 0–4 and the same final
  public receipt after restarting the same packaged server and store.

These native path reads are a separate test oracle, not governed named CDF reads.
Enablement upgrades the observed reader 1/writer 2 protocol to reader 1/writer 7
with `changeDataFeed`, `appendOnly` and `invariants`; disable retains it.

The retained first-pair continued-use tests separately cover both accepted append
frontends, duplicate suppression, target-first and project-first retained native
caches, and checkpoint reopening. That scenario enables at version 2, appends at
3 and 4, disables at 5 and appends at 6 and 7, ending with 11 rows. It uses local
policy fixtures and mocked registration transport; it is distinct from the live
seven-row scenario above.

Separate real-provider fault injection establishes committed/refresh-unconfirmed
recovery: a metadata GET fails after native commit 3 becomes visible, then ordinary
named observation recovers receipt 3 without changing native files or creating
commit 4. Stopped-H2 history and same-store restart pass. Its Spark test succeeded
before an outer verifier failed on a JSON envelope assumption; the corrected
read-only verifier passed against the retained store without repeating mutations.

The postpublication policy-change regression uses an actual native log write
before the owner's first observation. Eight outcome cases cover committed and
uncertain classification, captured CommitInfo settings, tampered/foreign/missing
publication, authority cleanup, later denial and observation-only recovery.
Affected module checks, Spotless and Javadoc pass. This is focused verification;
it does not claim the full repository suite or warning-free compiler output.

The local acceptance harness uses retained historical fixture helpers and package
manifests in the worktree evidence. It is not a standalone clean-checkout
reproduction kit. Distributed execution, cloud S3/IAM and `catalog-demo` integration
remain to be qualified.

## Cache profile and scope

Named SELECT overlapping retained raw native caches is rejected by the existing
cache-substitution guard. Ordinary read acceptance and native cache maintenance
are separate; broader governed caching remains open.

CDF maintenance of matching retained raw cache keys requires:

```properties
spark.sql.adaptive.enabled=false
spark.sql.sources.bucketing.autoBucketedScan.enabled=false
spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=false
```

Spark can otherwise clone the recache session, whose ancestry the current owner
binding does not authenticate. The unsupported clone-required configuration
rejects before publication with the complete native file manifest unchanged.
Ordinary CDF use without matching retained raw caches passes with the default
`autoBucketedScan=true`. This does not broaden the non-CDF append profile.

The maintenance witness binds the issued append, retained cache keys and exact
native result version. The qualified CDF metadata/protocol transition retains
existing installed-head and checkpoint/segment guards. Recovery admits only the
exact empty native metadata tuple encoder under a live LOAD owner; it grants no
execution authority.

Governed named CDF consumption, broader queries, lifecycle/DML, streaming and
table features remain in the [scope register](delta-integration-status.md).
The [implementation plan](superpowers/plans/2026-10-20-delta-cdf-enablement.md)
records this milestone; the accepted [append result](delta-native-governed-append.md)
remains independently scoped.
