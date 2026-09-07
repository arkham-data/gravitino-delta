---
title: "Delta registration lifecycle experiment results"
license: "This software is licensed under the Apache License version 2."
---

# Delta registration lifecycle experiment results

## Task 1: durable H2 registration fixture

On September 7, 2026, the test-only `registration` package in
`catalogs/catalog-lakehouse-generic` exercised existing `ManagedTableOperations`
and a real file-backed relational `EntityStore` on the feasibility branch based
on `8e919ff9a`. This task changes no production provider, core, API, REST,
client, storage schema, native Delta patch, or module dependency.

The initial focused run passed **21 tests, 0 failures, 0 errors, 0 skipped**:

| Class | Executed tests | Result |
| --- | --- | --- |
| `TestDeltaRegistrationLifecycle` | 11 | Pass |
| `TestGenericCatalogOperations` | 2 | Pass |
| `TestDeltaTableOperations` | 8 | Pass |

A scoped review follow-up passed **39 lifecycle tests, 0 failures, 0 errors,
0 skipped**, with module formatting checks. Fourteen missing prior-field cases
and thirteen corrupt/inconsistent prior-field cases each exercise REPLACING and
QUARANTINED replacement records. Rejected abort and publish requests leave the
complete malformed row unchanged, including after reopen. A malformed
publication candidate is rejected before persistence. The restored-abort test
also reopens H2, repeats the exact abort, and checks unchanged row and verifier
call count. Existing generic/Delta operation suites were not rerun in this
follow-up; their initial results above remain separate evidence.

The module's `spotlessApply` and `spotlessCheck` passed. Sources use Java 8
language constructs and ran on JDK 17.0.16 with Gradle 8.2. The recorded
`testRuntimeClasspath` selects H2 2.2.224 and Delta Kernel API/defaults/storage
3.3.0 through the existing module dependencies. The existing worktree
Git-commit resource warning, Gradle deprecation notice, and macOS Docker-context
notice appeared; no Docker tests were requested.

## Task 2: actual Delta Kernel log inspection

Task 2 adds a separate test-only, read-only `KernelRegistrationCommitInspector`.
The fixture creates real local Delta transaction logs through Kernel
`createTransactionBuilder` and `commit`, then opens only the normalized location
stored in the Task 1 record. It calls the public `Table.getLatestSnapshot`,
`Snapshot.getVersion`, and `Snapshot.getSchema` APIs. No Spark process, native
operation marker, production provider, or Delta-log write occurs during
verification.

The final patch matrices passed **42 tests, 0 failures, 0 errors, 0 skipped** on
each selected Kernel patch:

| Kernel selection | Inspector tests | Lifecycle tests | Loaded artifacts | Result |
| --- | ---: | ---: | --- | --- |
| 3.3.0 | 3 | 39 | API/defaults 3.3.0 | Pass |
| 3.3.2 | 3 | 39 | API/defaults 3.3.2 | Pass |

The executed tests assert both artifact manifest versions and JAR code sources.
The per-version runtime dependency reports additionally select matching
`delta-storage` patches. Omitting `deltaRegistrationKernelVersion` executed the
valid-log test against the unchanged 3.3.0 baseline. Setting it to 3.3.1 failed
during configuration with the expected allowlist message; the override is
limited to this module's test compile and runtime classpaths.

A valid version-zero snapshot returns an immutable observation containing its
actual canonical Delta schema, converted integer/string columns, null native
UUID, and version. It remains **AMBIGUOUS** because the pinned public Kernel
snapshot API exposes no operation token or other proven correlation marker.
Schema mismatch, an unchanged replacement base, a candidate hint that differs
from the latest stored-location version, corrupt JSON, invalid stored evidence,
unsupported schema types, and other read failures all fail closed as AMBIGUOUS.
Only `TableNotFoundException` yields NOT_COMMITTED, and its reason states that
absence does not prove a stale writer cannot commit later. A valid log elsewhere
at the candidate version cannot override the stored location.

## Observed behavior

| Boundary | Executed evidence |
| --- | --- |
| Reserve and durability | Unique insert stores the first dispatcher-simulated `StringIdentifier`, token, principal, location, canonical request, and generation. A close/reopen of the same H2 file preserves the complete record. |
| Visibility | Reserved, replacing, aborted, and quarantined records remain in raw TABLE storage and are hidden from the fixture's ordinary list/load/exists. A published record appears once. |
| Retry | A new injected candidate ID does not replace the first ID for identical reservation content. Changed principal/token/schema/location fails without changing the row. Delayed reserve retry after publication and finalization after response loss preserve identity. |
| Evidence | NOT_COMMITTED cannot publish; AMBIGUOUS quarantines. Only explicitly synthetic MATCHED evidence publishes observed columns/version/UUID. Candidate version is merely passed to the trusted verifier. Client state flags and mixed property commands fail. |
| Replace | The same Gravitino ID, location, creator, and creation time survive a replacement with a new synthetic native UUID/schema. Pending replacement retains prior publication properties and columns. Verified absence on abort restores those values while keeping the newest generation/token; exact abort replay across reopen performs no new verification. |
| Recovery | Correct token alone does not authorize a foreign principal. Configured recovery authority still needs evidence and an explicit simulated writer handoff. QUARANTINED allows subsequent evidence-verified recovery with handoff; it never enables automatic timeout takeover. |
| Stale inspection | A real store mutation during the injected verifier invalidates its inspected base. The subsequent update rejects the stale ID/token/generation/base, leaving its candidate columns unpublished. |
| Backend OCC | Two actual update callbacks wait at a latch after reading the same unmodified base. Exactly one commits; the other throws `OptimisticLockException`. Only the winning properties and column persist, including after reopen. Timed latches/futures replace sleep-based synchronization. |
| Abort and deletion | Verified absence leaves a durable hidden ABORTED row. The sentinel data file is unchanged; raw TABLE count remains one. Drop/purge are explicitly unsupported in the adapter. |
| SPI limits | Calling the pinned relational store's `executeInTransaction` throws `UnsupportedOperationException`. TABLE deletion has no caller-expected-ID predicate; no check-then-delete workaround is implemented. |

H2 uses `DB_CLOSE_DELAY=0` and the fixture closes/recreates the actual store,
not a mock wrapper. Cache configuration is false, and reopen assertions inspect
an actual `NoOpsCache`. Only `GravitinoEnv.config` is assigned directly for
namespace setup, and its previous value is restored in teardown. These tests
run serially because the relational store uses process singleton infrastructure.

The callback implementation performs pure state validation and immutable
candidate construction. Evidence inspection and column ID allocation occur
before `store.update`. The callback checks the complete inspected entity again.
Pending replacements require a complete PUBLISHED prior snapshot with
consistent generation, token, principal, location, native base, and completion
receipt. The resulting candidate is validated inside the callback before it
returns to the store; it performs no native IO, native commit, data deletion,
or external ownership
operation. The synchronized race is a backend control-request conflict test,
not evidence of concurrent native writer support.

## Test protocol and inspection seam

The consumed input property is `delta.registration.command`. Version 1 uses a
fixed-field envelope, canonical URL-safe Base64 text fields, and a maximum of
16,384 UTF-8 bytes. Missing, unknown, extra, unsupported-version, and malformed
fields are rejected. Persisted properties use `delta.registration.internal.`;
returned reservation receipts expose only ID, generation, token, and state under
`delta.registration.receipt.`. The fixture accepts the injected identity as a
simulation of dispatcher behavior, not proof that it is authenticated REST input.

Local locations normalize lexically to absolute file URIs, avoiding the
filesystem-dependent trailing slash that `Path.toUri()` adds when a native
operation creates the directory. The retry regression creates that directory
between calls and verifies unchanged reservation identity. Remote URI keys retain exact
comparison semantics, rejecting ambiguous dot segments, escaped paths, userinfo,
query, fragment, and missing authority. This deliberately limited remote URI
validation is not a finalized S3 or multicloud location contract.

`RegistrationCommitVerifier.verify(TableEntity, long)` returns an immutable
`Result` with a distinct `Kind` and optional `Observation`. Observation exposes
native version, nullable native UUID, schema fingerprint, and copied columns.
An AMBIGUOUS observation can carry parsed schema/version without a UUID;
MATCHED requires both an observation and UUID. All MATCHED outcomes in the H2
lifecycle fixture are labeled **synthetic operation correlation**, with no
Delta-log read or write. The actual Kernel inspector consumes the same interface
and never manufactures MATCHED.

## Reproduction and retained evidence

Run from the feasibility checkout with:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
./gradlew :catalogs:catalog-lakehouse-generic:spotlessApply \
  :catalogs:catalog-lakehouse-generic:test -PskipITs \
  --tests '*TestDeltaRegistrationLifecycle' \
  --tests '*TestGenericCatalogOperations' \
  --tests '*TestDeltaTableOperations' \
  :catalogs:catalog-lakehouse-generic:spotlessCheck
```

Local evidence is retained under
`build/delta-registration-lifecycle-evidence/task-1/`. The scoped review follow-up
has `review-fix-1/commands.txt`, original `red-xml/` (28 executed, 28 failed), and
`green-xml/` (39 executed, all passed), with corresponding logs and exit files.
The original `commands.txt` records
commands and exit codes; `final-green-xml/` preserves nonzero per-class JUnit
counts before another task overwrites module reports. The actual module reports
originate in `catalogs/catalog-lakehouse-generic/build/test-results/test/`.

The original behavioral RED inherited unfiltered managed-table behavior and
failed the hidden-list assertion (1 executed, 1 failed, exit 1). Expanded RED
executed 8 tests with 6 failures; additional retry/record-codec RED executed 11
with 2 failures. Their logs and exit files survive, but their original XML was
not retained because the first copy script searched the wrong build directory.
A separately labeled temporary visibility-filter mutation later reproduced the
same assertion (1 executed, 1 failed, exit 1), with XML retained under
`visibility-mutation-red-xml/`. The exact passing source was restored and the
focused verification passed. A later concrete directory-creation retry regression
also failed (1 executed, 1 failed, exit 1), with original XML retained under
`location-red-xml/`; lexical URI construction fixed it.
`location-final-green.log` and `final-green-xml/` are the final 21-test verification.
The supplemental visibility-mutation XML is not represented as
historical XML from the original RED run.

Task 2 can be reproduced with:

```bash
./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs \
  -PdeltaRegistrationKernelVersion=3.3.0 \
  --tests '*TestDeltaRegistrationCommitEvidence' \
  --tests '*TestDeltaRegistrationLifecycle'
./gradlew :catalogs:catalog-lakehouse-generic:test -PskipITs \
  -PdeltaRegistrationKernelVersion=3.3.2 \
  --tests '*TestDeltaRegistrationCommitEvidence' \
  --tests '*TestDeltaRegistrationLifecycle'
```

Its local evidence is retained under
`build/delta-registration-lifecycle-evidence/task-2/`. `red-xml/` preserves the
executable 3.3.0 RED (3 executed, 2 failed, 0 errors). The final per-patch XML is
under `kernel-3.3.0-xml/` and `kernel-3.3.2-xml/`; dependency reports, exact logs,
and exit files are adjacent. `default-xml/`, `default.log`, and
`unsupported-property.log` retain the property-selection checks.

## Remaining acceptance boundaries

These results combine H2 lifecycle evidence, explicitly synthetic MATCHED
evidence, and actual Kernel snapshot observation. They prove neither end-to-end
Delta recovery nor actual Spark token correlation, S3 atomicity, REST
authentication/authorization, owner-hook atomicity, schema-cascade safety,
cross-server cache convergence, distributed writer fencing, or Ranger policy
isolation. The narrow schema conversion exercised here is not a claim of complete
Delta protocol/type support. New reads are blocked while REPLACING: native read
availability/parity during replacement remains unproved. Aborted names are not
automatically reclaimed.

Full eventual acceptance still requires Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2
and Ranger 2.8.0. Existing separate Spark/Ranger evidence is recorded in
[the feasibility results](delta-spark-feasibility-results.md); these tests add no
new Ranger or Spark acceptance. Dispatcher/REST ownership and native durable
operation correlation/fencing are later gates in the
[approved design](superpowers/specs/2026-09-07-delta-registration-lifecycle-design.md).
