# Delta CDF enablement through the catalog

Status: the property command and first-pair continued-use, cache-maintenance and
real-provider recovery work pass their tests and integrated reviews. Four-pair
runtime/live qualification remains. This page does not claim governed CDF
consumption or full Delta support. See the [capability status](delta-integration-status.md)
for the remaining scope.

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
An ALTER-only policy can authorize this property operation without a SELECT grant.
Mixed properties, alternative key spelling, invalid values, UNSET, path/versioned
targets, other DDL and direct unbound catalog calls are outside this command.

The Delta authorization plugin now maps Gravitino `MODIFY_TABLE` to both `update`
and `alter`. Existing Ranger policies do not change automatically when the plugin
is upgraded. Follow the explicit [migration contract](../authorizations/authorization-ranger-delta/README.md#existing-modify-policy-migration);
repeating an unchanged public grant may not invoke a policy callback. Live
qualification of the expanded mapping remains pending.

Native Delta performs the metadata transaction and protocol changes. Gravitino
remains the owner of registration and observes the resulting native metadata.
No second metastore, Gravitino core change or REST-side Delta writer is introduced.
The deployment contract remains one writer job per dataset.

## Commit and recovery behavior

The command binds its table, catalog plugin, session, subject, configuration and
policy state, then verifies its own native metadata/protocol publication. A
copied, serialized or consumed command cannot issue another mutation. Changed
catalog identity is checked before execution and across native callbacks.

A failure after an observed native commit is reported as committed with refresh
unconfirmed. It is not a rollback. Recovery observes the named table again;
the implementation does not automatically replay ALTER or delete uncertain data.
Disabling CDF retains the upgraded native protocol and historical changes from
the enabled interval.

## Current evidence

All results here use Spark 3.5.4, Delta 3.3.0 and JDK 17 unless stated otherwise.

| Area | Established locally | Still required |
| --- | --- | --- |
| Property command | 17 passing CDF cases cover enable/disable, authorization, exact command/context binding, publication integrity, classic checkpoints and refresh-failure recovery; integrated review and catalog-identity fix review passed | Remaining runtime pairs and deployed Ranger 2.8.0/MinIO qualification |
| Provider composition | Actual named Spark command with packaged Gravitino, real HTTP clients, S3/MinIO and H2; enable/disable, postpublication failure, observation recovery and same-store restart pass | Production discovery and deployed Ranger authorization; other runtime pairs |
| Ordinary continued use | Masked/filtered named reads, both accepted append frontends and duplicate suppression pass after enable and disable | Remaining runtime/live qualification |
| Change production | Independent native oracle checks expected enabled-period insert rows after disable, empty enablement-only interval, and rejected pre-enable/disabled intervals | This does not provide governed named CDF reading |
| Retained native caches | Target-first and project-first cache maintenance, actual retained identities/generations, issued-witness rejection checks and fresh-session checkpoint reopening pass | Remaining runtime pairs; broader caching is outside this profile |

The ordinary-use scenario enables at version 2, appends at versions 3 and 4,
disables at version 5, then appends at versions 6 and 7. Duplicate attempts create
no additional version. Native CDF returns only the expected source-filtered and
masked inserts from versions 3 and 4; the final native table contains 11 rows.
A separate fresh native session reopens version 7 from its checkpoint and verifies
the complete row multiset. These continued-use tests use local policy fixtures
and mocked registration transport.

The separate real-provider scenario uses the production final wrapper and actual
application/metadata clients with distinct identities. It enables at version 1,
disables at version 2, then injects metadata GET failures after a third native
commit becomes visible. The command reports committed/refresh-unconfirmed.
An ordinary load and SELECT recover version 3 with the complete native file
manifest unchanged and no additional ALTER call or commit. The failure is proven
to occur after publication; native return is not observed at that boundary.

Stopped-H2 inspection verifies persisted versions 0–3, registration/native
identity, schema, owner, stable column IDs, protocol and configuration. Restarting
the same packaged server/store returns the same recovered receipt. The original
Spark test passed, then its store verifier failed on a JSON envelope assumption;
the corrected read-only verifier passed against that retained store without
repeating the Spark mutations. Production class bytes match the package. Custom
wrapper State and local policy transport mean this is not production discovery
or deployed Ranger qualification.

Fourteen distinct selected cases pass across these continued-use, admission and
provider runs, alongside affected module checks and Javadoc. The maintenance
source is identical across its accepted runs; a later narrowly scoped empty
native-encoder admission fix has separate failing/passing tests and real-provider
recovery evidence. This is focused verification, not a full repository test run.
Existing compiler/deprecation warnings remain in the retained output.

## Cache profile and recovery boundary

Raw native caches and governed query results have different admission rules.
Named SELECT overlapping the retained raw-cache fixture is rejected by the
existing cache-substitution guard. Ordinary read acceptance and native cache
maintenance are separate; broader governed caching remains open.

CDF maintenance of matching retained raw cache keys requires these settings
before execution:

```properties
spark.sql.adaptive.enabled=false
spark.sql.sources.bucketing.autoBucketedScan.enabled=false
spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=false
```

Spark can otherwise clone the recache session, whose ancestry the current owner
binding does not authenticate. The unsupported clone-required configuration is
rejected before publication, with the complete native file manifest unchanged.
The additional auto-bucketed-scan constraint does not apply to ordinary CDF use
without matching retained raw caches; that scenario passes with its default true
value. This result does not broaden the existing non-CDF append profile.

The maintenance witness binds the issued append, retained cache keys and exact
native result version. It permits only the qualified CDF metadata/protocol
transition and retains existing installed-head and checkpoint/segment checks.
Recovery also permits the exact empty tuple encoder that native Delta binds while
loading metadata, under a live LOAD owner. That analysis-only helper grants no
execution authority; altered, nonempty and wrong-owner plans reject.

The [implementation plan](superpowers/plans/2026-10-20-delta-cdf-enablement.md)
tracks the remaining provider/continued-use and qualification tasks. The accepted
[append milestone](delta-native-governed-append.md) remains independently scoped.
