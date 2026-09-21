# Independent metadata scope review

## Findings

No concrete correctness, security, compatibility, or material coverage findings
in the frozen two-file scope. No requested fixes.

## Spec-compliance verdict: READY

The implementation satisfies the bounded metadata lease foundation brief.

- `DeltaRangerMetadataClient.java:166` obtains exactly the catalog returned by
  the existing owned-client `get()` and invokes the callback without holding the
  metadata monitor. The stamp is captured only after `get()` has established a
  live owned client (`:169`, `:183`). There is no new credential path, fallback,
  actor selection, or public callback API.
- The immediate precheck (`:171`) and normal/exceptional postchecks (`:175-179`)
  cover callback table I/O and decoding. A still-live failure is rethrown by
  identity; closed/expired leases replace the late value/failure with the existing
  sanitized lease error. No DTO, credentials, or callback errors are logged.
- `leaseStamp` and `requireLive` (`:183-196`) are synchronized and bind the private
  identity created for this object (`:54`). They require a live, started, non-null
  owned client. A stamp cannot initialize a replacement or renew the deadline.
  The existing one-time configure check (`:210-223`) and terminal close state
  make identity reuse across replacement impossible through this API.
- Fixed monotonic expiry remains `nanoTime - deadline >= 0` (`:226-236`), with
  the deadline established before client construction (`:117-122`). Every new
  stamp check calls `requireUsable`, so delayed/non-running scheduled expiry does
  not permit a late result. The lifetime and lazy initialization paths are not
  changed by the diff.
- Close still detaches the owned client under the monitor and performs cleanup
  outside it (`:198-207`). The callback holds no monitor while stalled. Existing
  asynchronous expiration/late-construction cleanup (`:239-260`) is preserved.
- The stamp class is package-private, final, non-serializable, and contains only a
  private final identity; rendering is constant (`:277-289`). Public `get()` and
  the base catalog supplier path remain unchanged.

The direct caller check included `DeltaRangerCatalogSpark35.java:68-74` and
`:179-207`: the wrapper still passes its existing metadata supplier to the base
catalog. There is deliberately no typed registration-read integration yet.

## Code-quality verdict: READY

The change is small and uses existing lifecycle checks rather than duplicating
expiry, construction, credentials, or cleanup state. The new monitor boundaries
are clear and do not surround callback/network work. The local opaque identity
is sufficient because the client cannot be replaced or rebound after start/close.
Public API and authentication behavior are preserved.

Tests exercise the actual lease implementation with controlled clients/clocks and
latches, rather than a fabricated lease-valid boolean. In particular:

- `TestDeltaRangerMetadataClient.java:324` stalls actual callback table-load
  dispatch and advances the monotonic clock with a scheduler that never runs.
- `:363` proves close completes while the scoped operation remains stalled, then
  proves its late value is rejected. Both tests failed against the unchecked
  callback baseline before the implementation was restored.
- `:402` preserves a live callback failure by object identity; `:423` exercises
  expiry during an exceptional callback.
- `:240-320` covers same-catalog successful reads, repeated live validation,
  unconfigured/unstarted capture, wrong-client stamps, closure, expiry, constant
  rendering, and lack of Java serialization.
- Existing initialization-time expiry, asynchronous cleanup, separate metadata
  principal, and current query UGI assertions remain in the executed suite.

## Provenance and verification

Review target: the supplied frozen local two-file diff against
`f1bf2f193048771fb7e1dce0611daa5c344e9d09`. Unrelated engine/native-cache changes
in the shared worktree were excluded. I did not author this metadata change.

Independently recomputed working-file SHA-256 values:

- main: `ac5fbc0f05d8e0626b7928381429f1d34658f8becfdd7d8eda6bd2c60c557987`
- test: `5a10ed99185ccfff76bd76e35949ea05921ed8911ca7562fdc0dbab3715a415d`

Both match the assignment, GREEN source-hash manifest, and archived source bytes.
I inspected original `metadata-scope-red-2` XML: 2 tests, 2 expected assertion
failures (no late-value rejection), zero errors/skips, Gradle exit 1. Original
`metadata-scope-green-1` XML records 19 tests, zero failures/errors/skips and exit
0. The original command includes offline focused tests, Spotless and Javadoc.
The two actual compiled class entries in the Test.doFirst archives match their
recorded hashes. No new Gradle run was performed during this read-only review.

GitNexus `context` could not resolve `DeltaRangerMetadataClient` in the existing
index. I used direct full-source/diff/caller inspection; no index rebuild or
graph-backed taint/PDG completeness is claimed. No live action, source edit,
subagent, commit, or external message was performed.

## Remaining scope limits

This API is a trusted package-private operation scope. It does not prevent a
trusted callback from returning an escaping catalog/table handle; the planned
registration reader must return immutable decoded values as the brief requires.
It denies results after an observed close/expiry during the callback; it does not
cancel arbitrary transport work or prevent a later close after the final check.
Explicit physical client cleanup retains its existing behavior. Typed receipt
validation, catalog wiring, and retained-plan/physical guards remain separate
integration work. These are stated scope limits, not defects in this foundation.
