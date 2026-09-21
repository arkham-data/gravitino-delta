# Scoped metadata lease foundation

Base f1bf2f193. Implement only optional-module DeltaRangerMetadataClient.java and
TestDeltaRangerMetadataClient.java, plus task-4-metadata-scope-report.md and new
metadata-scope-* private evidence. No other Java/Scala/build files, services, live
HTTP/Docker, commits or subagents. Engine worker owns Gradle until root transfers
build ownership; write tests/code but do not run Gradle before that message.
Read AGENTS and relevant TDD skills. No renewed user approval is needed.

Confirmed gap: get() protects GravitinoClient.loadCatalog, but the caller's later
table read/receipt decoding occurs outside the lease checks. Implement the small
package-private scoped-read primitive needed by forthcoming readPublished. Suggested
API: <T> T read(Function<Catalog,T> operation), MetadataLeaseStamp leaseStamp(),
void requireLive(MetadataLeaseStamp expected). Exact names can be improved, but send
root the API before code. No Delta receipt dependency/build change is needed here;
typed registration decoding will use this primitive in the next integration.

The opaque nonserializable stamp binds exactly this once-configured owned client
lease. It cannot be used on another client, renew a deadline, initialize a replacement
or survive close/expiry. Capture a stamp only for a started/live lease after get()
has established the owned client. Recheck client/stamp/closed state and monotonic
deadline after operation including table I/O and decoding, on success and exception.
Never hold the metadata monitor during the callback or network I/O. Late success
must reject if close or expiry happened during the operation. Preserve existing
lazy creation, bounded lifetime including client construction, asynchronous expiry,
nonblocking close during pending initialization and exactly owned cleanup.

Existing public get() behavior remains available for the base supplier. The new
operation is package-private and is not a new public metadata/mutation actor API.
It must receive the same catalog handle from the same get/client; no second credential
path or fallback. Do not log DTOs, credentials or operation errors. Root will use
scoped reads to return immutable registration values, not escaping handles.

Meaningful TDD with real lease implementation and controlled fake client/clock/
scheduler: get succeeds then downstream table operation expires or closes before
return; late value rejects. Callback exception with expiry still verifies lease;
unchanged exception/live behavior is intentional and documented. A concurrent close
must not block on a stalled operation. Same live lease succeeds/repeatedly checks;
wrong lease, unstarted/unconfigured, closed and expired stamps reject, including
scheduler-not-running monotonic checks. Preserve existing metadata-client tests,
separate principal and query identity checks. No sleep-only race test or fake
production readiness bypass. Preserve original RED/GREEN XML/source/actual test
runtime inputs using existing run-gradle.py conventions and resource caps once
root grants build ownership. Format/Javadoc/import checks for changed files.
This is lease foundation, not registration or physical guard qualification.
