# Delta registration lifecycle experiment design

This continues the approved Delta integration direction. It establishes the
registration/provider contract before production packaging. The user's required
Ranger target is **2.8.0**; earlier Ranger2.5.0 engine/2.4.0 mocked Admin evidence
is historical. Ranger validation runs separately from this storage experiment.

## Scope and authority

Use existing `TableEntity` properties and `EntityStore` interfaces, with no
Gravitino core/API/server/client/storage-schema modifications. Gravitino remains
the sole table registration authority. All new lifecycle code stays in test
sources of the existing generic catalog module; no production provider is
registered or distributed by this package. S3 is the deployment target, but
local H2/Kernel tests prove only their stated boundaries.

The experiment has two independently reviewable outputs: a durable single-record
state machine against real file-backed H2, and a separate actual Delta Kernel
log-inspection fixture. Test evidence must distinguish synthetic matched commit
evidence from actual log inspection and from unproved Spark operation correlation.

## Existing extension contract

`ManagedTableOperations` implements existing `TableCatalog` and requires
`store()`, `schemas()`, and `idGenerator()`. Its standard create path consumes the
dispatcher-injected `StringIdentifier`, then writes `TableEntity` with
`EntityStore.put(entity, false)`. TABLE managed-storage capability bypasses the
dispatcher's extra persistence. A provider can filter ordinary list/load while
retaining a pending TableEntity. This does not hide the entity from governance,
owner, schema-cascade, or generic metadata APIs.

`EntityStore.update(identifier, TableEntity.class, TABLE, updater)` is the actual
conditional transition interface. The pinned relational backend checks table ID
and the old row version, invokes the callback once, and fails losing updates.
Callbacks must be pure. Verify native evidence outside the update, then recheck
identity/token/generation/base/location inside it. Neither a callback nor a JVM
TreeLock may span Spark execution or storage IO. Cache is disabled in this first
fixture; cross-server cache convergence remains a production gate.

`RelationalEntityStore.executeInTransaction` is unsupported. TABLE deletion has
no caller-supplied expected-ID predicate; read-check-delete cannot safely replace
one. The prototype therefore retains terminal abort/quarantine records and does
not reclaim names or delete Delta data automatically.

## Record, commands, and state

One TableEntity holds the registration ID, location, actor, operation kind,
operation token, request fingerprint, generation, base native observation,
state, and verified commit outcome. Use versioned provider properties; no new
entity or database schema. Tokens are idempotency keys, not credentials.

The fixture uses existing create/alter property transport for its explicitly
named test protocol. A mutable nonreserved input command property is consumed;
persisted state properties are provider-controlled and reserved/hidden. Reject
unknown versions, mixed command plus ordinary edits, client attempts to set
stored state, and malformed fields. The first create consumes the dispatcher
candidate ID. Exact retry returns the stored ID even if a new candidate ID was
injected; exclude that candidate ID from the request fingerprint.

States and visibility:

- ABSENT -> RESERVED through unique insertion. RESERVED is absent from ordinary
  provider list/load/exists, but durable in the store.
- RESERVED -> PUBLISHED only on matching verified evidence. Repeated identical
  finalization returns the same outcome without repeating native work.
- RESERVED -> ABORTED when no commit is verified and writer handoff is asserted
  by the test fixture; ambiguous evidence -> QUARANTINED. Both remain durable and
  hidden. Elapsed time never authorizes takeover or destructive cleanup.
- PUBLISHED -> REPLACING with the same registration ID and a new token/generation.
  The prototype blocks fresh named reads until resolution; it does not promise
  old snapshots remain available after native replacement.
- REPLACING -> PUBLISHED on verified replacement, preserving location, creator,
  creation time, and registration identity. Verified absence may restore the
  prior publication; ambiguous evidence remains unavailable. Native Delta UUID
  may change and is not the Gravitino registration identity.

For each transition, compare expected registration ID, generation, token,
principal/recovery authority, normalized stored location, and base observation.
A retry/conflict must never replay the native commit. Only the original actor or
explicit fixture recovery principal may request recovery, and neither bypasses
evidence verification. This local principal check does not prove REST auth.

## Verification boundary

A provider-local verifier returns MATCHED immutable evidence, NOT_COMMITTED, or
AMBIGUOUS. It opens the stored location; a client's candidate version is only a
search hint. Publication requires correlation with this operation, not merely a
valid/latest Delta table, schema, UUID, or client `committed=true` value.

The actual Kernel reader can prove snapshot existence/version/schema and reject
missing, corrupt, or mismatching logs. It must return AMBIGUOUS when operation
correlation is absent. State-machine tests may inject explicitly synthetic
MATCHED evidence; documentation must not label those end-to-end recovery tests.
Native Replace omits per-write userMetadata in the pinned Delta paths, so a
universal token marker is not assumed. Spark CREATE/CTAS/replace/RTAS token
correlation on all four pairs is a subsequent gate.

## Required evidence and deferred gates

Real H2 close/reopen demonstrates durability. Barrier-synchronized database
updates demonstrate one OCC winner and one loser without leaked changes; no
sleep-based race proof. Tests cover replay with newly allocated candidate ID,
conflicting token/payload/actor, hidden states, schema publication, pure callback
behavior, stale ID/generation, replacement, and terminal quarantine/abort.

Actual Kernel tests run on3.3.0 and3.3.2 without Spark in the server fixture.
Separate tests cover valid snapshot inspection and fail-closed missing/corrupt/
foreign/uncorrelated observations. Kernel availability is not full Delta protocol
feature support.

Before production provider delivery, separately prove dispatcher/REST managed
capability, ownership assignment after create, owner-hook failure recovery,
reserved-property validation, schema cascade, native durable commit correlation,
authenticated recovery authority, cache convergence, destructive operation
fencing, Ranger2.8.0 lifecycle reconciliation, and actual S3 execution. Existing
single-writer platform policy must cover recovery and administrative operations
before it can justify reclaim. No new broad SELECT or core privilege is allowed.
