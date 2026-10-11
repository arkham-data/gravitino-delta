# Command-owned Delta idempotency

The retained append association can execute the native idempotency lookup under
one owned target transaction while preserving its original source obligations.
This is an internal Boolean phase. Installed append commands remain rejected.

## Authority and lifecycle

`DeltaRangerAppendCommandBinding.hasBeenExecuted` accepts only the exact retained
command and Spark session. The target, original source receipt and command options
remain private. Creator thread, active session, authenticated subject/groups,
complete SQLConf, all participating Ranger publication stamps and registration
identity must still match. The currently qualified command stage requires empty
write options, `write=None` and `analyzedQuery=None`, with transaction auto-reset false.

Idle validation checks the complete original source SELECT batch and target UPDATE
before opening a native transaction. The association then validates both within
that exact transaction, executes the native scalar phase, closes its finite native
state scope, validates both participants again, closes the transaction binding,
and performs final idle validation before returning either true or false. Source
and target validation run outside the scalar native operation because they own
separate retained-validation scopes.

`readInTransaction` checks the supplied native transaction against the binding's
private transaction identity. Sharing a snapshot or DeltaLog is insufficient. It
owns only its state operation; it never closes the caller's transaction binding or
returns that binding, transaction, executable plan or continuation. The existing
standalone helper delegates to this finite primitive and retains its own target
receipt and transaction cleanup contract.

Target UPDATE and source SELECT remain distinct decisions. A duplicate application/version
cannot bypass source columns, masks, filters, empty input or repeated source
occurrences. Same-table source obligations are revalidated, but this phase does not
execute a transactional source scan or write transformed rows. Native transaction
read tracking remains a requirement of later source preparation.

## Qualification evidence

All four pinned pairs passed on October 11, 2026 UTC:

| Spark | Delta | Tests | Failures / errors / skips |
| --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 69 | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 69 | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 69 | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 69 | 0 / 0 / 0 |

These are 276 test executions across six suites per pair. The new focused suite contains
nine methods and 72 fixture cases: six cold frontend/outcome positives, 52 drift
cases, and 14 authority/lifetime/option controls. The full regression selection also
includes NativeIdempotency, AppendCommandBinding, AppendTargetReceipt,
InstalledAppendBoundaries and ReadRule.

The six positives use actual captured insertInto/saveAsTable commands with separate
cold snapshots for duplicate=true, new-application=false and higher-version=false.
They observe the same transaction during source and target validation before and
after computed state, native Aggregate/encoder execution, and physical completion
matched by complete Spark ExprIds including JVM UUID. Asynchronous listener errors
are checked at every fixture teardown.

Drift tests mutate source/target policies, roles and registration IDs/tokens,
subject/groups, configuration, source literal content or command namespace during
the scalar and after scalar completion, for both native outcomes. Each mutation
must actually fire and produce its intended rejection. Evaluation failures require
the precise existing wrapper and cause; injected exceptions retain identity at the
native boundary or as the policy-evaluation wrapper's cause. No production policy
engine behavior was changed to accommodate test expectations.

Every captured runtime contains six Apache Ranger artifacts, all 2.8.0, including
`ugsync-util`, with Scala 2.12.21 and JDK 17.0.16. Each pair has 188 traced fixtures
and 13,194 verified storage-manifest entries: 752 fixtures and 52,776 entries across
the matrix. Every ordinary fixture preserves its complete before/after storage
map; history controls compare genuine setup-published versus post-validation bytes.
All 79 source files match the reviewed freeze before and after each run. Root and
independent review verified all 14,855 archived files per pair and the actual worker
runtime.
Scoped formatting checks passed. These tests use fixture policy transport; they
do not contact deployed Ranger.

A fresh standalone-helper regression also passed on Spark 3.5.6 / Delta 3.3.2
against the retained [live fixture](delta-live-idempotency.md): deployed Ranger
2.8.0, ordinary packaged Gravitino and MinIO. The duplicate returns true; a new
application and higher version return false; denied UPDATE rejects before native
lookup. All three allowed cases require cold native state and physical completion.
The fresh JVM exits zero, downloads resource policies and roles over HTTP 200 with
an initially empty cache, and preserves all 16 raw S3 objects against both its own
before/after inventory and the original fixture baseline. All scopes close and
SQLConf stays unchanged. Current source hashes and all 205 classpath entries are
verified; moving project artifacts are archived. This is a regression of the
refactored standalone helper; the new
command-owned phase is qualified by the fixture-transport matrix above, not by this
live run. Neither run enables append or qualifies public MODIFY grant propagation.

The [implementation plan](superpowers/plans/2026-10-10-delta-append-idempotency-composition.md)
records the scope and selectors.

Private evidence lives under
`.superpowers/sdd/2026-10-10-delta-append-idempotency-composition/`. Failed development
runs remain preserved. One run compiled an older assertion than the then-current
working source and is explicitly excluded from accepted qualification. Final
matrix runs freeze source before launch and verify equality afterward, retain the
actual worker/classpath plus moving project artifacts, original XML, phase traces
and complete original storage bytes/manifests.

## Remaining append work

The association validates supplied early command objects; it is not proof of Spark
command origin or authorization to execute an AppendData command. The actual
populated analyzedQuery/V1 writer input, transaction-owned same-target source
preparation, native helper-created plans and action generation still require
qualification. Native commit, precommit revalidation, recache/postcommit handling,
reset=true continuation, uncached/CRC-absent state and live integrated append remain
open. Public Gravitino MODIFY propagation through Ranger to S3 append is not
established by the matrix or the directly provisioned live fixture policies.

No Gravitino core/API/server/client/storage-schema or dependency changes are added.
The platform's single-writer assumption remains; repeated observations do not make
policy, metadata and Delta atomic. Full Delta support is unfinished. CDF remains
the first feature priority after Delta maturity.
