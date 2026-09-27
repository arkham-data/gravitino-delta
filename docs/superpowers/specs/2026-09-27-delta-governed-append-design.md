# Governed Delta batch append design

This continues the full Delta integration under the user's continuous subagent-driven authorization. The finite SELECT checkpoint is published at `84172dd45`; subsequent work remains local. This document selects the next write architecture and separates source-proven contracts from native behavior still requiring characterization. It does not claim governed writes already work.

## Outcome and constraints

Support SQL INSERT VALUES, INSERT SELECT, and named DataFrame append to existing Gravitino-registered Delta tables, with ordinary Gravitino grants propagated to Ranger 2.8.0 and enforced by Spark. Gravitino retains registration ownership; Delta retains transaction/log/data authority. Preserve source column access, masks and row filters. Native behavior without authorization is not completion.

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- S3-compatible native storage; local MinIO qualification is permitted. Preserve existing SELECT fixture and quarantined callback cases.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal.
- Keep accepted runtime packaging/dependency isolation. No remote push, demo change or service replacement in the initial local implementation task.

## Policy contract

The canonical target access for this append path is Ranger **update**, not write. The pinned Kyuubi 1.10.2 AppendData command describes an INSERT output; its table access mapping resolves to UPDATE, while URI writes resolve to WRITE. The existing four-level Delta service definition already includes update. No service migration or broad HadoopSQL MODIFY bundle is needed.

Translate exactly TABLE/MODIFY_TABLE/ALLOW to one update access on C/D/T/*, preserving TABLE/SELECT_TABLE/ALLOW as select on the same canonical resource. A mixed object carries both distinct accesses. Empty privilege lists produce no access; local catalog/schema visibility continues to produce no access. Reject DENY, unsupported type/privilege combinations, explicit foreign-catalog resources and wildcard names before Admin interaction. Validate the entire explicit change batch before effects. Old unfiltered role context is not an explicit foreign change.

Gravitino owns only its labelled access contributions. Selective removal of MODIFY_TABLE must preserve SELECT_TABLE, owners, other roles and foreign access/mask/filter policies; the converse also holds. Roles retain ordinary membership semantics and the existing failure-after-persistence limitation. This mapping alone does not authorize a Spark command until the command route is implemented and qualified.

For INSERT SELECT, authorize original source columns and apply source row filters/masks before data reaches the writer. Target update does not grant source SELECT. Target SELECT-only masks/filters are not insertion constraints and do not themselves deny append; this matches the pinned Kyuubi AppendData route. If the target also occurs as a source, that source occurrence receives ordinary SELECT obligations. Never silently reinterpret a Ranger SELECT row filter as a SQL CHECK or mutate incoming values using target read masks.

## Native execution architecture

Three options were evaluated:

| Option | Consequence | Decision |
| --- | --- | --- |
| Analyze-time UPDATE check, then native run | Leaves analyzed/reused commands and changes during file generation without a commit-boundary recheck | Insufficient |
| Owned append command delegates native transaction phases | Can check at entry and between native action generation and native commit while retaining Delta's action/commit machinery | Selected for characterization |
| Replacement general V2 writer | Would duplicate substantial Delta semantics and disturb native table/planner identity | Avoid |

The candidate coordinator retains the exact named target binding, authorized source plan and command options. Spark V1 fallback executes `AppendData.analyzedQuery`; the source authority must bind that actual input, not merely the optimized command child. Within native `withNewTransaction`, preserve native idempotency handling, delegate `writeAndReturnCommitData`, revalidate, then invoke native `commitIfNeeded` with the original actions, operation and tags. Authorize before the native idempotent no-op branch. A coordinator can mix in the native `DeltaCommand` trait to call its protected `hasBeenExecuted`; do not use reflection to bypass visibility. Preserve native transaction-version auto-reset behavior. Never reconstruct file actions or implement a second transaction protocol. The upstream [Delta 3.3.2 WriteIntoDelta source](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/commands/WriteIntoDelta.scala#L96-L109) exposes this sequence. Pinned bytecode inspection found this boundary unchanged between 3.3.0 and 3.3.2; execution equivalence still needs tests.

Do not implement the production coordinator until characterization proves native option/idempotency/no-op behavior and nested execution shapes on both patches. SQL and named DataFrame APIs must reach the same qualified authority path; catalog lifecycle and postcommit hooks do not intercept ordinary append early enough.

An append receipt must bind session, subject/groups, exact target native object/descriptor, canonical registration identity and location/native ID, source obligations, policy/role publication stamps, configuration and effective write options. A native transaction-version auto-reset is an explicit owned state transition to characterize, not permission to ignore arbitrary configuration changes. Existing registration identity is numeric ID + generation + correlation token; the token is not bearer authority. Validate current publication and compatible native transaction snapshot at entry and before commit. Rechecking observations does not make Ranger, registration and Delta atomic. A publication arriving after the final observation remains outside that observation guarantee; do not claim instantaneous revocation.

Nested native snapshot/write plans require owner-specific provenance and bounded lifetime. Do not broaden the SELECT allowlist to arbitrary commands or allow native operations through ambient flags. Current SELECT action guards are not proof of write safety. Spark eagerly executes commands and caches command results: authorize the actual run before mutation, not a later collection of its result; collecting a completed result must never replay the write. A dedicated target binding is required; target loading must not masquerade as source SELECT.

## Failure and retry semantics

- Entry denial occurs before user scan/file generation and leaves the target log and storage unchanged.
- An observed permission, subject, registration or source-policy change before commit prevents the Delta commit. If files were already generated, uncommitted files may remain; record them separately from committed rows. No unsafe automatic deletion or replay is introduced.
- Native recache and postcommit hooks happen after commit; a failure there must remain possible committed work, never an entry denial. Auto-compaction can perform additional writes and needs explicit qualification.
- Preserve native transaction app/version idempotency, empty-input behavior, operation metrics/tags and constraint failures. Distinguish a successfully skipped retry from a denial. Characterization must determine the exact ordering before production implementation.
- Append options can request schema/protocol or other behavior. Preserve native support through explicit authorization/qualification; reject unqualified behavior before writes while tracking its implementation as a remaining full-Delta requirement. Do not describe temporary rejection as full support.
- Ordinary failed-callback reconciliation remains a rollout requirement. Start new live append subjects from verified converged local/Admin/downloaded grants; never repair or reuse quarantined SELECT cases.

## Acceptance and sequence

1. Implement the independently testable server MODIFY_TABLE mapping and policy-preserving grant/revoke tests. Existing Spark command admission stays unchanged.
2. Characterize native SQL/named DataFrame append, idempotency, no-op, failure points and nested plans in isolated local tables on all four pairs. Prove the split-native transaction path preserves committed data/actions/metadata and exposes entry/precommit barriers.
3. Implement owned target/command authority and source enforcement from those observed shapes; test stale/replaced registrations, subject/configuration changes, retained commands, policy refresh, same-name cross-catalog targets, literal inputs and self-append.
4. Qualify public Gravitino grant/revoke → real Ranger 2.8 downloads → native S3 append across all four pairs. Read back data and Delta versions/actions, registration identity, complete Admin policy/role models and storage state for denials. Include source masks/filters and owner/foreign preservation.
5. Independent source/runtime review and results documentation. No full-Delta claim from this slice.

Subsequent requirements remain UPDATE/DELETE/MERGE, overwrite, CTAS/RTAS/registration DDL, native DROP/PURGE/RENAME recovery, time travel/history/CDF, streaming/checkpoints, maintenance, feature-bearing table lifecycles, broader query/action/AQE/cache support, additional policy transformers, authentication/launcher modes, metadata exposure and demo integration.
