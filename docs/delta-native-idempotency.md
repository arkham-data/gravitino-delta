# Native Delta idempotency execution

Subsequent [live qualification](delta-live-idempotency.md) covers the finite helper
against deployed Ranger 2.8.0, Gravitino and MinIO. The results below describe the
earlier offline checkpoint and its distinct scope.

The internal `DeltaRangerNativeIdempotency.hasBeenExecuted` phase now executes Delta's native
transaction-id lookup under the registered target's UPDATE authority. It owns the native transaction
and returns only a Boolean after revalidation and cleanup. This is a dependency within governed
append; the installed companion still rejects append commands.

## Qualified execution

The positive fixture uses an ordinary table with cached native snapshot state and cold computed
transaction state. The real `DeltaCommand.hasBeenExecuted` calls `txnVersion` and
`Snapshot.transactions`, executes its fourteen-expression Aggregate, stock SnapshotState encoder and
native `first()` physical query, and changes computed state from false to true. A preseeded duplicate
returns true; a new application and a higher transaction version return false. The phase writes no
files and commits no Delta transaction.

Authority belongs to the exact registered target, native catalog descriptor, DeltaLog and selected
Snapshot, active Spark session and creator thread. Permission uses the authenticated application
subject and canonical catalog identity. A foreign catalog sharing the same DeltaLog does not inherit
the target's authority. Source SELECT receipts remain separate: source-denial checks surrounding the
scalar probe demonstrate ordering, not integrated append frontend enforcement.

Only the actual state getter's cached LogicalRDD/backing and immutable output, partitioning,
ordering, session and origin fields supply native state authority. The existing finite
Aggregate/encoder/first recipes remain in force. Generic projections, filters, statistics/file scans
and raw-log reconstruction receive no new permission. Transaction logical and physical evidence
expires on every phase exit, including successful exits. Existing successful SELECT scan
continuations retain their prior lifecycle.

The phase requires `spark.databricks.delta.write.txnVersion.autoReset.enabled=false` before entry and
never resets or restores SQLConf. Native DeltaOptions and the command preserve session/explicit
option precedence. This finite API permits only `txnAppId` and `txnVersion`, rejects case-insensitive
duplicate names, and delegates pair/numeric validation to Delta. Full SQLConf, user/groups,
policy/role publication and registration remain subject to revalidation through return.

## Verification

| Spark | Delta | Qualified tests | Failures / errors / skips |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 106 | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 106 | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 106 | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 106 | 0 / 0 / 0 |

All 424 qualified test cases use Ranger 2.8.0, Scala 2.12.21 and JDK 17.0.16. Eleven requested
selectors produce ten executable suites; `TestDeltaRangerNativeSelect` is a shared fixture rather
than an eleventh suite. The exact selectors and commands are in the
[implementation plan](superpowers/plans/2026-10-09-delta-native-idempotency.md).

The primary pair combines 92 unchanged regression tests with the corrected fourteen-test
idempotency class. Independent review found missing group/policy/role mutation branches whose
broad exception assertion accepted MatchError. A stricter assertion reproduced that failure; the
test-only correction proves actual mutations and requires the intended denial. The corrected class
and scoped re-review pass. All production sources and 131 compiled production classes are identical
across the original and corrected primary archives. The fourteen reruns are additional verification
executions, not additional unique scenarios; both source layers remain archived.

Tests exercise installed native logical/physical callbacks with the companion enabled. The new phase
uses the existing fixture settings: user AQE disabled, two shuffle partitions and two Delta snapshot
partitions. This checkpoint adds no broader AQE execution-mode qualification or support for
additional Delta protocols/table features. Policy transport uses the existing request-sensitive
fixture around the real engine and registration wrapper; these runs do not contact deployed Ranger.

Negative coverage includes denied UPDATE before lookup; transaction/session/catalog/metadata
substitution; copied native/user tags; altered RDD output, metadata, partitioning and origin fields;
invalid aggregates, encoders and limits; and closed, nested or reactivated transaction evidence.
Real configuration, identity, group, policy, role and registration drift prevents the Boolean from
returning. Tests verify each mutation, the exact denial, scope cleanup, expired evidence and recovery
after test-owned restoration. The deliberately injected exception must retain object identity, and
asynchronous listener failures fail the tests.

Each qualified pair covers 116 traced fixtures and 7,866 storage-manifest entries: 464 fixtures and
31,464 entries across the four pairs, using the corrected primary layer. Original superseded evidence
is also preserved. Every ordinary fixture's complete before/after storage map is identical.
Checksum-history controls deliberately publish genuine setup-generated history and compare
published versus post-validation bytes. Uncached state reconstruction and missing-checksum history
still reject at their measured boundaries. Scoped formatting and Javadoc pass. Tests run offline
with integration and Docker tests skipped; no live S3/Ranger test is included.

Private evidence is retained under `.superpowers/sdd/2026-10-09-delta-native-idempotency/`:
`frozen-task-2/`, `execution-primary-01/`, `execution-r1-class-01/`,
`matrix-3.5.4-3.3.2/`, `matrix-3.5.6-3.3.0/`, `matrix-3.5.6-3.3.2/`,
`matrix-report.md`, and the independent reviews and root audits. Archives contain exact source and
compiled bytes, worker commands/argfiles, all resolved artifact hashes and copies of moving project
artifacts, loaded runtime origins, traces, manifests and original storage bytes.

## Remaining append work

This scalar is not connected to an accepted AppendData command, and true does not represent a
completed or successfully skipped governed append. The append orchestrator must compose idempotency,
source preparation and writing within its owned transaction. It must preserve the bound
command/source obligations, including same-target transactional sources and helper-created plans.
Native transaction-version reset continuation, action generation, precommit revalidation, commit and
postcommit handling remain open. Uncached/CRC-absent reconstruction and live integrated
append/demo qualification are also unqualified here.

No Gravitino core/API/server/client/storage-schema, dependency, Spark or Delta fork changes are
introduced. The platform's exclusion of concurrent dataset writers remains an assumption, not an
atomic policy/registration fence or arbitrary-code sandbox. Full Delta support is unfinished. CDF
remains the first feature priority after Delta maturity.
