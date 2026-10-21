# Governed CDF property mutation

Status: all four pinned runtime/live cells pass with Ranger 2.8.0 and MinIO. Bounded qualification is recorded in the
[CDF result](../../delta-cdf-enablement.md). October 20, 2026.
The user approved the MODIFY_TABLE → UPDATE + ALTER policy mapping on October 20;
that bounded mapping change is committed with focused tests and review.
Baseline: `dd6171a0b`. This document does not authorize live policy changes.

## Contract and decision

Support named SQL `ALTER TABLE catalog.database.table SET TBLPROPERTIES
('delta.enableChangeDataFeed'='true')` and the corresponding `false` value.
Admit exactly one canonical property with a literal lowercase boolean value.
Reject mixed properties, aliases of the key, invalid values, UNSET, path targets,
versioned targets, schema changes and all other DDL. CREATE with CDF remains outside
this delivery. Native Delta owns validation, metadata/protocol changes, optimistic
transactions and checkpoint behavior. No Gravitino core change or REST-side writer.

Authorize the canonical table-wide Ranger `alter` resource, with the application
subject and authoritative owner. Deliberately extend the Delta authorization
plugin's TABLE/MODIFY_TABLE mapping from `{update}` to `{update, alter}` in both
`privilegesMappingRule` and `translatePrivilege`. MODIFY_TABLE already expresses
table modification; the current finite implementation underapproximates it.
Keep append on `update`, reads on `select`, and USE privileges as prerequisites
without data grants. Do not silently map CDF ALTER to `update`, which would make
an explicit Ranger update-only policy authorize a differently named operation.
No CREATE, DROP, DELETE, SELECT or unrestricted access is added.

This is a visible privilege expansion, requiring release documentation and an
explicit operator migration of existing managed policies. Updating Java mapping
alone does not update already-published role policies. Until an approved
reconciliation or actual revoke/grant transition has materialized `alter`, those policies remain unable to
enable CDF. Preserve policy IDs, scope, ownership and unrelated accesses during
that migration; test revoke after both old and upgraded policy states. Do not
automatically mutate Ranger policies at connector startup.
Reissuing an unchanged public Gravitino grant may be a no-op with no callback;
plugin callback convergence is not proof that such a public request migrates
existing policies. A revoke/grant transition temporarily removes MODIFY access
and must be an explicit operator action.

## Baseline path and implementation design

Relevant production sources are under
`spark-connector/v3.5/spark-delta-ranger/src/main/{java,scala}/org/apache/gravitino/spark/connector/delta/ranger/`,
unless qualified otherwise.

1. At the baseline, `DeltaRangerReadRule` sends resolved `SetTableProperties` into finite
   SELECT admission and rejects it before authorization. Add a dedicated
   `DeltaRangerCdfPropertyCommand` binding/physical command for this exact logical
   shape. Capture the resolved named catalog object, identifier, native table
   binding, exact property map, session, subject, configuration and policy stamp.
   Carry private driver authority through resolution, check and pre-CBO; a copied
   tag, serialized carrier, substituted child, changed map or foreign catalog
   must not create authority. Integrate its strategy/check in
   `DeltaRangerSparkExtension` and update the finite inventory expectations in
   `DeltaRangerPhysicalPlanning`/`DeltaRangerAppendInventory`.
2. Execute the native catalog operation once under this binding. Add an
   `alterTable` override to `DeltaRangerCatalogSpark35`, requiring the live issued
   command and exact changes. Direct Java calls without an issued command fail
   closed. Validate current named registration and native UUID/location/head,
   then authorize `alter` before native mutation. Recheck subject, configuration,
   registration and policy stamp immediately before invocation.
3. Base `GravitinoDeltaCatalogSpark35.alterTable` already validates changes,
   observes registration, checks the head, delegates native ALTER and calls
   virtual `loadTable(id)`. Retain that contract. Introduce only a protected
   native-ALTER execution seam, defaulting to the existing
   `nativeCatalog.alterTable(path(location), changes)`. The companion uses the
   seam exclusively for the bound CDF command, retaining normal base validation.
   Give any new public/protected seam Javadoc.
4. Native execution needs an observable transaction, not an ambient permission.
   In Delta 3.3.0, `AlterTableSetPropertiesDeltaCommand.run` obtains its transaction
   through protected `startTransaction`, updates metadata and calls native
   `commit` with `SetTableProperties`. Use a narrowly owned subclass overriding
   only `startTransaction` to retain the exact returned transaction; run the
   inherited native body. Build it from the retained named Delta table and the
   validated map. This preserves Delta's mutation algorithm rather than copying
   it. The same non-final command and protected hook exist in
   [3.3.2](https://raw.githubusercontent.com/delta-io/delta/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/commands/alterDeltaTableCommands.scala).
   Run `DeltaConfigs.validateConfigurations` on the map before construction, as
   [native DeltaCatalog does](https://raw.githubusercontent.com/delta-io/delta/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/catalog/DeltaCatalog.scala).
   Use a regular subclass, not a case-class subclass. The hook calls
   `super.startTransaction()`, captures and validates its returned object, then
   returns that same object. Enclose inherited `run` in the owner's try/finally,
   closing authority on success and all exceptions. Never replace the native
   transaction or call the command through a second nested Spark SQL execution.
   [Pinned 3.3.0 command source](https://raw.githubusercontent.com/delta-io/delta/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/commands/alterDeltaTableCommands.scala).
5. Close mutation authority before the ordinary named reload. Let existing
   `DeltaRangerRegistrationBindings.load` own the new load and its native metadata
   reconstruction. It requires the provider's before/after observations to agree.
   Provider `DeltaRegistrationOperations.loadTable`/`refresh` projects the native
   head into registration metadata; it is not an arbitrary REST property ALTER.
   Verify actual provider observation, not a mocked receipt updated by the test.

## Native ownership and publication

Add a separate `BoundCdfPropertyMutation` in `DeltaRangerRegistrationBindings` and
`DeltaRangerCdfPropertyMutation` execution owner. Share small identity/context
validation helpers where appropriate, but do not instantiate `BoundAppendTarget`,
`BoundAppendTransaction` or `AppendPublication` as CDF authority. Their contracts
assume append metadata/configuration stability and append-specific action intent.

The owner has finite phases: bound/authorized → native transaction capture →
commit in progress → verified native publication → closed → fresh named load.
Before transaction capture, permit only owned old-head metadata reconstruction.
After capture bind the exact native transaction, log, snapshot, transaction ID,
registered UUID/location, starting version and accepted property intent. Extend
`DeltaRangerNativeOperation` with an explicit property-mutation owner/callback;
do not relax `withNativeMetadata`, raw selection or generic command admission.
No source data scan is required or authorized.

Native commit can reconstruct the postcommit snapshot before returning. An
old-head-only guard therefore cannot surround the whole ALTER. Reuse the
*mechanism* illustrated by `DeltaRangerAppendCommit`: inspect the expected newly
published log object, verify stable status and bytes, bind its digest, and require
its CommitInfo transaction ID/read version/operation/property parameters to
match the captured native transaction. Verify that actions contain only the
native metadata update and permitted native protocol transition, plus native
CommitInfo; reject AddFile, RemoveFile, CDC, transaction and unrelated metadata
changes. UUID, schema, partitioning, description and unrelated configuration
must remain unchanged. Derive expected protocol behavior from the pinned native
transaction, not from an assumed writer version 4.

Only that verified publication admits its postcommit snapshot and exact log
segment. Reuse the finite metadata recipe, cache sealing and physical guards in
`DeltaRangerPlanAdmission`, `DeltaRangerNativeProvenance` and
`DeltaRangerPhysicalPlanning`; add explicit CDF-owner dispatch where commit paths
currently name `DeltaRangerAppendCommit`. Extract common log/checkpoint recipe
validation if necessary, preserving each operation's independent witness.
Never treat existence of version N+1, a matching property value, a thread-local
flag, or a class name alone as proof that this command published it.

The initial contract retains the single-writer deployment assumption. Native
conflicts must remain native failures; a concurrent unexpected head/publication
must not be adopted. If native commit retries against a newer version, fail closed
unless the owner has captured and validated that exact native transition. Do not
disable native concurrency checks or claim arbitrary concurrent-writer support.

Classic checkpoints must work at a property-commit boundary, including enabled
and disabled CDF protocol states. `DeltaRangerRawMaintenance` currently rejects
CDF/writer>2: keep raw user admission rejected. If native named append recache
uses that route, issue only an exact mutation/append-owned maintenance witness
for the qualified protocol and recipe. Never make all raw CDF relations legal.

If commit succeeds but provider reload fails, report an explicit committed but
refresh-unconfirmed outcome; do not retry ALTER automatically or pretend rollback.
Recovery is a fresh named load/observation and comparison with the retained commit
identity. A policy change after publication cannot undo the commit; it must block
subsequent governed use and must not be misreported as “no mutation occurred.”

If execution fails after native invocation but before the first publication
observation, classify the outcome with the captured transaction/log/snapshot,
read version, identity and CommitInfo settings. The terminal classifier makes
at most two bounded reads of the exact expected log object, checks stable file
status and identical complete publication proofs, and preserves any prior digest
binding. It does not require new policy/metadata execution authority. A proven
own publication reports committed/refresh-unconfirmed; an unprovable publication
reports uncertain, preserving the original cause and proof failure. Current
uncertainty takes precedence over a historical proof whose bytes no longer match.
Pre-native denials preserve their original failure.

This classifier may only record the outward outcome. It cannot install snapshots,
seal plans/caches, refresh registration, mark execution successful or replay SQL.
Execution guards remain unchanged and authority closes before the error escapes.
The actual native-write regression changes policy before the first owner
observation and verifies later denial and observation-only recovery with no N+2.


## Required tests and implementation sequence

1. Policy unit tests in
   `authorizations/authorization-ranger-delta/.../TestRangerDeltaAuthorization`:
   exact MODIFY mapping and translated resources `{update, alter}`; no SELECT or
   other grants; scope isolation; grant/revoke preserving unrelated policy items;
   old update-only policy denies ALTER until explicit reconciliation. Update the
   module README's current update-only contract.
2. New command/owner unit tests: exact property admission; malformed/mixed/UNSET
   rejection; direct catalog-call denial; forged/copied/reused physical command;
   foreign target/session/subject; changed policy stamp and registration/head
   between analysis and execution. Denials precede native invocation.
3. Replace the unsupported-route characterization in
   `TestDeltaRangerInstalledCdfAlter` with installed-extension allowed/denied and
   revoked cases. Denied cases require zero native ALTER calls and unchanged
   complete target file hashes, version, metadata, protocol and registration.
   Update-only and select-only policies deny; alter-only authorizes the property
   operation without requiring a data SELECT grant.
4. Verify native publication attacks/failures: foreign N+1 commit, incorrect
   transaction ID, altered log bytes, unrelated metadata/action, escaped owner,
   nested operation, postpublication reload failure and no automatic replay.
   Exercise fresh snapshot reconstruction, warm caches and a checkpoint boundary.
5. Real provider acceptance: registered UUID/location/state remain stable;
   refreshed property/protocol/native version reflect enable and disable; native
   UUID, schema/field identities and prior data remain stable. Compare native log
   output with the existing `TestDeltaNativeAdapter` CDF oracle.
6. Continued governed use after enable and after disable: ordinary SELECT with
   deny/column permissions/mask/row filter; accepted matching-schema `insertInto`
   and existing-table `saveAsTable`; duplicate handling, native recache and
   checkpoint behavior. Both enable and disable retain writer-7 feature
   declarations in the measured baseline. A separate native reader verifies
   exact CDF rows and version intervals; it is a test oracle only.
7. Start Spark 3.5.4/Delta 3.3.0, then qualify changed paths on all four pinned
   Spark/Delta pairs and real Ranger 2.8.0/MinIO after local tests pass. Format and
   run affected module tests before broad build. Do not rerun unrelated append
   qualification solely to reproduce its already accepted milestone.

## Original implementation risks and resolution

The original stock-adapter transaction-capture gap is resolved by the protected
native command hook and a narrow base-adapter seam, with native configuration
validation retained. A separate CDF owner proves publication and checkpoint
transitions independently of append authority. Real-provider refresh, ordinary
use, bounded retained-cache maintenance and observation-only recovery have
passing focused evidence. All four pinned runtime/live cells pass with Ranger 2.8.0 and MinIO.

The final outcome fix covers policy change between publication and its first
observation, preserving execution denial while distinguishing committed and
uncertain results without replay. The [CDF result](../../delta-cdf-enablement.md)
records the runtime and evidence boundaries. Governed CDF reads remain rejected
and require their own relation, historical-schema and masking/filtering design.
