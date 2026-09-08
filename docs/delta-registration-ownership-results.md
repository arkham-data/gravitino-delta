# Delta registration ownership and parent lifecycle evidence

This prerequisite changes only the test registration provider and its tests. It
supersedes the published RESERVE-retry and provider actor-denial behavior recorded
in `delta-managed-rest-results.md`; it does not promote a production Delta
provider or change generic external registration.

## Contract and observed boundaries

Exact CREATE/RESERVE replay succeeds only while the original row is RESERVED.
After publication it fails before the existing create hook can assign ownership.
Completed publication response loss is recovered with exact PUBLISH through alter,
subject to existing MODIFY permission and the stored actor/recovery check. A token
is correlation, not authority. The missing-owner RESERVED retry repair still uses
the create hook and retains the original row and dispatcher identity.

The original real REST regression created and synthetically published a table,
transferred its owner to `registration-foreign`, then replayed CREATE as the
original `registration-actor`, which retained CREATE_TABLE/use but had no table
MODIFY permission. The replay returned successfully and reset ownership to the
original actor while the full TABLE row stayed identical. Original failing XML
and before/after row and owner snapshots were saved before the fixture changed.

| Boundary | Result |
| --- | --- |
| Delayed published CREATE | Rejected; full raw row, transferred owner name/type and verifier count are preserved. The old actor's PUBLISH replay is denied before provider entry. |
| Authorized completed PUBLISH | Original actor retrieves the same receipt before transfer and, after an explicit MODIFY grant, after transfer. No create hook or verifier runs again; the transferred owner stays intact. |
| Provider-reaching foreign actor | Existing Gravitino `ForbiddenException` produces a typed Java client exception and directly measured HTTP 403. The use-only outsider also gets 403, before provider entry. Rows are unchanged and no verifier runs. |
| Catalog non-force | Enabled catalog is rejected as in-use; disabled nonempty catalog is rejected as nonempty, both before provider schema drop. |
| Catalog force | Enabled and disabled catalogs reach the raw schema guard for RESERVED, PUBLISHED and malformed rows. Catalog enabled state, full raw registration, catalog/schema/table owners and sentinel bytes survive. |
| Metalake force | Enabled and disabled metalakes reach the same guard for those three row shapes. Registration, parent metadata, owners and sentinel bytes survive. A disabled metalake is re-enabled before rejection; its catalog's `metalake-in-use` becomes true. This side effect is not rolled back. |
| Partial catalog cleanup | An empty schema enumerated before a guarded schema is deleted before the guard rejects. The guarded registration, parents, owners and sentinel survive. Failed parent deletion is not atomic. |
| Empty parents | Empty schema/catalog and metalake force deletion succeed. Existing schema tests retain all five valid registration states in both cascade modes. |

Malformed rows contain an unknown state and a missing actor. Guard tests inspect
raw storage instead of ordinary visibility. Owner equality compares name and type,
since the raw owner implementation has no value equality.

## Reproduction and evidence

Baseline: `8eb20098561e595c9077f76e32bf136442114830`. Runtime is JDK 17.0.16;
new code uses Java 8 APIs. Real MiniGravitino HTTP, Java clients, simple named
principals, JCasbin, file-backed H2, disabled cache and disabled catalog isolation
are retained. REST teardown now restores the prior metrics object as well as
config. This is a source fix for the observed REST-first lifecycle reopen failure;
the successful combined run executed lifecycle before REST, and explicit
REST-first re-verification was not performed.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :catalogs:catalog-lakehouse-generic:spotlessApply \
  :catalogs:catalog-lakehouse-generic:test -PskipITs \
  --tests '*TestDeltaRegistrationLifecycle' \
  --tests '*TestDeltaRegistrationRest' \
  :catalogs:catalog-lakehouse-generic:spotlessCheck
```

The covering run passed **39 lifecycle + 25 REST tests (64 total), zero failures,
errors or skips**, and scoped Spotless passed. Original final XML is in
`run-8-focused-green-xml/`; the command exited 0. After review, the tightened
replay assertion and scoped Spotless also passed: one test, zero failures, errors
or skips, exit 0 (`run-9-review-replay-assertion-green-xml/`).

Local evidence is under `build/delta-native-registration-evidence/task-1/`.
Each run preserves exact command arguments, JDK path, timestamps, exit code, log,
original XML and runtime properties. No evidence directory or coordination file
is force-added to Git.

The authoritative original ownership RED is
`run-6-original-ownership-snapshots-red-xml/` (one test, one failure), with full
before/after snapshots in its corresponding runtime properties. Run 3 also
records the precise owner-name reset. Run 5 is the actor-denial RED (one test, one
failure: generic RuntimeException instead of ForbiddenException). Run 2 initially
compared owner objects by identity, so its rejection assertion is relevant but its
owner-equality assertion is not the precise proof. Run 1 stopped at formatting
and run 4 stopped at compilation; their copied XML/runtime files predate those
runs and are not execution evidence. Run 7 executed 64 tests and failed 37:
three absent/default-true property assertions, one helper schema mismatch and
33 lifecycle reopen failures from the unrestored metrics singleton. Those
failures are preserved rather than relabeled as protocol RED.

## Limits

Publication and writer handoff remain explicitly synthetic test controls. These
tests establish neither actual native commit correlation nor a production writer,
S3 behavior, packaged isolation, distributed fencing, cache-enabled behavior or
Ranger 2.8 authorization. No Spark/Delta runtime matrix was run in this task.
Pending owner REST repair remains unavailable; a future public pending-owner
transfer facility would require reconsidering RESERVED create replay. Fileset
cleanup before schema guards and concurrent/multi-server parent mutation remain
outside this evidence. No protected core, API, server, client or storage-schema
source is changed, and parent managers are intentionally unpatched.
