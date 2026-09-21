# Scoped metadata lease foundation

Base `f1bf2f193048771fb7e1dce0611daa5c344e9d09`, 2026-09-21. This bounded
implementation changes only `DeltaRangerMetadataClient`, its focused test, this
report and private `metadata-scope-*` evidence. It does not decode registration
receipts, add a Delta dependency, alter the catalog wrapper, or qualify retained
native scans.

## Protocol

The package-private API is `<T> T read(Function<Catalog, T> operation)`,
`MetadataLeaseStamp leaseStamp()` and
`void requireLive(MetadataLeaseStamp expected)`. The stamp is an opaque,
non-serializable value with constant rendering. It carries only the identity of
the once-configured lease, not the client or catalog handle.

`read` obtains the catalog through the existing `get()` path, captures the exact
started lease, checks it immediately before invoking the callback, and checks it
again after either normal or exceptional completion. The callback runs outside
the client monitor. A live callback exception is preserved. Closure or monotonic
expiry during the callback supersedes a late value or callback failure with a
sanitized lease denial. The checks neither renew the deadline nor initialize or
replace a client.

The existing finite lifetime still begins before client construction. Scheduled
expiry and monotonic deadline checks still terminate the lease, and cleanup still
closes only its owned client. Public `get()` remains available to the base catalog
supplier.

## Deterministic coverage

The focused tests use controlled clocks, mocked schedulers and latches around
`TableCatalog.loadTable`. They cover:

* repeated successful reads through the same catalog and live stamp;
* unconfigured and unstarted stamp capture, foreign stamps, closure and expiry;
* a successful table response released after monotonic expiry;
* a successful table response released after concurrent nonblocking close;
* a live callback failure preserved by identity and an expired callback failure
  rejected by the lease;
* constant stamp rendering and absence of Java serialization;
* the existing separate metadata authentication with the query UGI unchanged
  inside and after the scoped callback.

## Evidence

`task-4-evidence/metadata-scope-red-2/` is the behavioral RED. It retained the
compiling stamp API but temporarily reduced `read` to the unchecked
`operation.apply(get())` baseline. The two late-value tests both failed because no
`IllegalStateException` was thrown: 2 tests, 2 failures, Gradle exit 1. The archive
contains the exact source variant and hashes, command, original XML, runtime inputs,
Gradle output and exit status. `metadata-scope-red-1/` is separately preserved as
an infrastructure-only attempt that could not open the sandboxed Gradle wrapper
lock; it is not behavioral evidence.

`task-4-evidence/metadata-scope-green-1/` restored the complete implementation and
ran `spotlessApply`, all `TestDeltaRangerMetadataClient` methods, Javadoc and
`spotlessCheck` with pinned Spark 3.5.4 / Delta 3.3.2, offline dependencies, two
workers and two active processors. Gradle exited 0; the original XML records 19
tests, 0 failures, 0 errors and 0 skipped. The archived source files are byte-equal
to the frozen working files:

* `DeltaRangerMetadataClient.java` SHA-256
  `ac5fbc0f05d8e0626b7928381429f1d34658f8becfdd7d8eda6bd2c60c557987`;
* `TestDeltaRangerMetadataClient.java` SHA-256
  `5a10ed99185ccfff76bd76e35949ea05921ed8911ca7562fdc0dbab3715a415d`.

No callable claude-mem/mcp-search tool was available. The existing GitNexus index
predates this optional module and could not resolve `DeltaRangerMetadataClient`, so
the consumer boundary was checked directly with repository search.
