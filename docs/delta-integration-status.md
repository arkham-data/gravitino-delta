# Delta integration: capability status and next delivery

Source audit, focused local probes and CDF implementation: October 20, 2026.
This is the current scope register, not full runtime qualification. CDF work
follows the [implementation plan](superpowers/plans/2026-10-20-delta-cdf-enablement.md).
The original objective remains full Delta integration through Spark,
Gravitino-owned registration, and Ranger enforcement without changes to Gravitino
core. Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Ranger 2.8.0, S3, and one writer job
per dataset remain the target.

## What has shipped and what has not

“Qualified” below means the specific behavior in the linked result, not the
whole feature family. Base-catalog implementation does not establish support
with the Ranger extension installed.

| Capability | Current evidence | Remaining acceptance |
| --- | --- | --- |
| Dedicated Delta catalog and discovery | Production `lakehouse-delta` provider and Spark adapter exist; [startup contract](spark-connector/spark-delta-catalog.md) | Deployment/coexistence acceptance in the actual demo and distributed runtime |
| Gravitino-owned registration | Publication, native metadata and recovery machinery exist; [provider contract](../catalogs/catalog-lakehouse-delta/README.md) | Complete lifecycle, recovery and migration acceptance; no second authoritative Glue/Hive registry |
| Public grants/revokes → Ranger | [Qualified SELECT propagation](delta-ranger-production-results.md) and [MODIFY append propagation](delta-native-governed-append.md), Ranger 2.8.0; new UPDATE + ALTER mapping tested locally | Live qualification/migration for expanded MODIFY grants, remaining operations and general partial-failure reconciliation |
| Named governed SELECT | Qualified projections, filters and the admitted join/expression shapes, column permissions, masks and row filters | General Spark queries: aggregation, ordering, broader joins/expressions, AQE and caching are not covered by this result |
| Named batch append | Qualified matching-schema `insertInto` and existing-table `saveAsTable`; all four version pairs | SQL INSERT, `writeTo`, wider options/schema handling and transaction-setting modes |
| Create/replace/ALTER | Base adapter has staged creation/replacement and native ALTER delegation | Governed DDL authorization, native execution and registration continuity; base implementation is not governed acceptance |
| Drop/purge/rename | Explicitly rejected by the current Spark adapter; provider retains registrations | Implement lifecycle and data-ownership/recovery semantics |
| Overwrite, UPDATE, DELETE, MERGE | Not qualified by SELECT or append | Native outcomes plus independent source/target authorization and denied-write behavior |
| Catalog CDF enablement | Local installed-extension named enable/disable tests now pass on Spark 3.5.4 / Delta 3.3.0; separate real-provider refresh test passes | Combined command/provider flow, continued governed use, remaining pairs and live Ranger/MinIO acceptance |
| CDF consumption | Existing governed relation checks do not admit native CDF relations | Named batch and streaming CDF, policy application to historical rows/columns and metadata columns |
| History/time travel/restore | Governed versioned loads explicitly reject time travel | Historical identity/schema semantics and authorization; restore write semantics |
| Schema evolution and table features | Metadata/schema infrastructure exists; individual paths have narrower admission | Enumerate the native 3.3.0/3.3.2 feature baseline and qualify each supported combination through the catalog |
| Streaming | Rejected by current governed query admission | Reads/writes, checkpoint restart, CDF streams and explicit policy-refresh boundaries |
| Maintenance | Some classic checkpoint and native cache maintenance qualified with append | OPTIMIZE, VACUUM, conversion and other native commands; broader checkpoint/cache behavior |
| Production/demo operation | Packaged local Spark + MinIO qualification exists | `catalog-demo`, driver/executor deployment, S3 credentials/IAM, identity and operational recovery |

There is no defensible percentage-complete estimate from these rows: their sizes
and dependencies differ. Full support requires a version-specific native
capability inventory and passing catalog/enforcement cases for it, not simply
more tests of the already accepted append path.

## Concrete CDF findings

The earlier shorthand “CDF is not implemented” needs qualification. The
end-to-end governed feature is missing, but useful foundations are present:

- [GravitinoDeltaCatalogSpark35](../spark-connector/v3.5/spark-delta/src/main/java/org/apache/gravitino/spark/connector/delta/GravitinoDeltaCatalogSpark35.java)
  accepts supported property changes in `alterTable`, resolves the registered
  location, checks its native head, delegates to native Delta and reloads the
  named table.
- [DeltaNativeObservation](../catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaNativeObservation.java)
  and [DeltaRegistrationState](../catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaRegistrationState.java)
  retain and validate the boolean `delta.enableChangeDataFeed` property.
- Ordinary Gravitino REST property mutation is a different path:
  [DeltaRegistrationOperations](../catalogs/catalog-lakehouse-delta/src/main/java/org/apache/gravitino/catalog/lakehouse/delta/DeltaRegistrationOperations.java)
  accepts registration commands, not arbitrary native table-property changes.
  Spark SQL enablement can use the existing native transaction delegation;
  it does not require a second server-side Delta writer.
- [DeltaRangerRegistrationBindings](../spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRegistrationBindings.java)
  rejects nonempty native table options and expects `HadoopFsRelation` with
  `TahoeLogFileIndex`. Native CDF uses a different relation; removing the options
  check alone cannot make it work.
- [DeltaRangerRawMaintenance](../spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerRawMaintenance.scala)
  explicitly rejects CDF and writer protocols above 2. This is a raw cache
  maintenance restriction, not evidence that every ordinary named read of a
  CDF-enabled table is rejected.
- [DeltaRangerPlanAdmission](../spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerPlanAdmission.scala)
  admits a finite query shape and rejects streaming. Broader SELECT support is
  a real full-integration requirement, independently of CDF.

This explains a material difference from the existing Iceberg/Hive practice:
[BaseCatalog](../spark-connector/spark-common/src/main/java/org/apache/gravitino/spark/connector/catalog/BaseCatalog.java)
sends converted property changes through Gravitino. The Delta adapter instead
owns the native Spark transaction and reloads Gravitino's observation. Reusing
the generic property path unchanged would hit the command-only REST contract.
The existing CDF safe-projection unit test uses a mocked protocol, so it does not
prove a real CDF table can complete this flow.

Native Delta enables CDF through a table property and records subsequent
changes. Insert-only changes can be reconstructed from the transaction log;
the absence of a `_change_data` directory does not establish failure.
See the [Delta CDF documentation](https://docs.delta.io/delta-change-data-feed/).
The pinned [3.3.0 reader](https://github.com/delta-io/delta/blob/v3.3.0/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala)
and [3.3.2 reader](https://github.com/delta-io/delta/blob/v3.3.2/spark/src/main/scala/org/apache/spark/sql/delta/commands/cdc/CDCReader.scala)
define `DeltaCDFRelation` as a `BaseRelation`/`CatalystScan`, with
`_change_type`, `_commit_version` and `_commit_timestamp`.

## Focused runtime results

On Spark 3.5.4 / Delta 3.3.0 / JDK 17, two new
[native adapter tests](../spark-connector/v3.5/spark-delta/src/test/java/org/apache/gravitino/spark/connector/delta/TestDeltaNativeAdapter.java)
passed:

- Named CREATE with CDF enabled, then two named appends, yields the exact
  expected insert rows for individual and combined version intervals.
- Named ALTER enables and disables CDF while preserving the native UUID and
  mocked registration identity. Pre-enable and post-disable ranges reject;
  the enabled historical interval remains readable. Ordinary named reads and
  appends continue.
- The observed protocol is reader 1/writer 7 with `changeDataFeed`, `appendOnly`
  and `invariants`. ALTER starts at reader 1/writer 2; disabling CDF retains
  the upgraded protocol and feature declarations.

The first invocation failed because the test assumed legacy writer protocol 4.
After checking the actual native protocol, the corrected fresh invocation passed
both tests, with zero failures/errors/skips. Original failure logs/XML remain;
JUnit automatically removed that first invocation's temporary native files.
The passing invocation's native files were retained separately.

The original installed Ranger characterization passed one test containing two
policy-control cases. Direct Ranger-engine
`alter` checks allowed and denied the respective cases. In both cases the actual
named SQL failed with `Unqualified Delta Ranger route: SetTableProperties`,
before any command policy request or catalog ALTER call. Target file hashes,
registration properties, native version, metadata and protocol were unchanged.
This is a passing characterization of a missing capability, not successful
governed enablement or evidence that SQL ALTER permissions are enforced.

These tests use local native Delta with mocked catalog responses and local policy
fixtures. CDF consumption in the native test uses a path read as an independent
oracle. They do not prove real provider refresh, deployed Ranger, S3, governed
CDF reads or the other three version pairs. These probes changed no production code.

The command implementation now passes 17 focused CDF invocations on Spark
3.5.4 / Delta 3.3.0 / JDK 17. These cover named enable/disable with ALTER and no
SELECT requests, denied-byte invariance, command/context restrictions, revocation,
catalog replacement before execution and during native callbacks, publication
integrity, classic checkpoints, and committed-but-refresh-unconfirmed
recovery without replay. Earlier integer-overflow, callback-context and log-segment
validation failures were fixed; their original failing evidence is retained.

Before the final catalog-identity fix, nine append publication regressions and
three base-adapter cases passed. Three append maintenance tests initially failed because
their reflective fixtures referenced fields moved into the shared metadata helper;
only those fixture targets were corrected, with their assertions unchanged. All
three passed in the focused rerun, and affected module checks and Javadoc passed.
The integrated review identified the missing native catalog-identity check; its
targeted regression reproduced the defect, and the fix passed scoped re-review.
All 15 prior CDF cases and the two catalog-replacement cases passed with the fix.
These are local tests with mocked registration transport and policy fixtures,
not combined provider or live Ranger/MinIO acceptance. Retained compiler/worktree
metadata and native fixture warnings do not represent pristine test output.

The separate
[provider refresh test](../catalogs/catalog-lakehouse-delta/src/test/java/org/apache/gravitino/catalog/lakehouse/delta/TestDeltaRegistrationOperations.java)
also passed one focused case. It uses the real metadata reader, registration
operations and H2 persistence with manually authored Delta metadata/protocol
actions. Fresh reader/operations instances observe enable and disable at new
versions, persist the corresponding safe property and changed configuration
digest, retain reader 1/writer 7 and the CDF feature declaration, and preserve
registration identity, native UUID, owner and column entities. The source commit
bytes remain unchanged. This is an empty local table with an open H2 store;
it does not establish nonempty data preservation, a server restart, native
Spark-emitted commits or the combined Spark/provider flow. The first invocation
failed on a fixture's attempt to read a marker from a published receipt; the
corrected invocation passed without a production fix.

## Approved authorization change

The user approved expanding TABLE/MODIFY_TABLE from Ranger `update` to
`update` plus `alter`. Both the advertised mapping and translated resources now
implement that decision. SELECT remains separate; USE privileges grant no data
access. Four focused tests first failed for missing ALTER, then all 19 tests in
`TestRangerDeltaAuthorization` passed after the change, including legacy-policy
callback convergence/revocation, independent SELECT, catalog isolation and
preservation of unrelated roles/policies. Module formatting checks passed.

No live policy was changed. Existing policies require an explicit migration;
reissuing an unchanged public grant may skip callbacks. See the
[migration contract](../authorizations/authorization-ranger-delta/README.md#existing-modify-policy-migration).
This supplies authorization plumbing for CDF mutation. The installed SQL command
now passes focused local enable/disable tests and review; live qualification remains.

## Next delivery and architectural decision

Recommended next user-visible result: enable CDF on a registered table through
the named catalog, append through the governed writer, and read the resulting
changes by name with Ranger permissions, masks and row filters enforced.
Accept this in two explicit steps: first catalog enablement with ordinary SELECT
and append preserved, then governed CDF consumption. The first step addresses
the user's enablement priority and can ship independently; it must not be
reported as governed CDF-read support.

The enablement design and subsequent CDF-read work must preserve:

1. **Property mutation:** authorize the application actor before native ALTER;
   preserve registration identity and observe the resulting native property and
   protocol. Do not silently extend MODIFY grants to unrelated operations.
2. **CDF reads:** bind the named registration, selected version interval and
   native CDF relation; apply current Ranger decisions to returned historical
   data. Define historical schema changes and treatment of the three CDF
   metadata columns explicitly.
3. **Continued use:** prove ordinary named reads, append, duplicate behavior and
   native maintenance still work after enabling CDF. Do not disable caches or
   weaken admission just to make a positive test pass.
4. **Acceptance:** compare literal change rows and version boundaries with native
   Delta, exercise denied/revoked access and masked/filtered results, then qualify
   the changed paths across the four version pairs with Ranger 2.8.0 and MinIO.
   A subsequent demo step must use catalog names, not substituted S3 paths.

The first runtime checks established native property delegation and identified
the installed `SetTableProperties` admission blocker. The new command has passed
its focused local command/publication tests and implementation review. Next,
complete real provider composition and continued named reads/appends before
rebuilding packages or launching a matrix.
For the enablement step, an independent native CDF read can validate change
production; it is a test oracle, not an authorized user-facing bypass.

The selected [enablement design](superpowers/specs/2026-10-20-delta-cdf-enablement-design.md)
extends the existing adapter with a CDF-specific command binding and independently
verified native transaction. It preserves existing source masking/filtering and
revocation boundaries. General operation authorization and governed CDF reading
remain separate work.

Keep the accepted append milestone closed unless a concrete regression or CDF
dependency requires a change. Do not repeat its complete matrix without such a
change. Full Delta lifecycle, DML, streaming and table-feature work remains in the
scope register; it is not an undefined prerequisite called “Delta maturity.”
