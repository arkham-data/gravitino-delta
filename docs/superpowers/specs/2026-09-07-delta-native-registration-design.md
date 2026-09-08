# Delta native registration contract

Continue from c4a8c20e017617224c587d532a1bcbada2fd10f0 under the user's authorization to develop the production registration/commit contract. Gravitino owns registration; Spark3.5.4/3.5.6, Delta3.3.0/3.3.2, Ranger2.8.0 and S3 remain the targets. No core/API/server/client/storage-schema source changes are permitted. The platform prevents concurrent writer jobs for one dataset; this does not itself provide a stale-writer fence.

## Architecture and alternatives

Choose a dedicated future `lakehouse-delta` provider/module `catalogs/catalog-lakehouse-delta`, using existing managed SCHEMA/TABLE storage and ServiceLoader interfaces. Preserve generic external-registration behavior. Do not ship a provider that merely wraps synthetic native evidence. This package first closes the ownership retry defect and implements an independently testable native commit contract; production provider/Spark runtime packaging follows successful real reserve/commit/publication evidence.

Use adapter-owned ordinary Delta table property `gravitino.registration.operation`. Exact Delta3.3.0/3.3.2 sources preserve it in the atomic metadata action for schema-only CREATE, CTAS and query-bearing replacements. Native userMetadata/write transaction options do not cover all these paths; shared SQLConf mutation and post-commit marker writes are rejected. Reject caller collisions, case variants and option-prefixed aliases for the owned key. Unrelated user properties/options remain intact.

The bounded versioned marker binds the first authoritative Gravitino ID, generation, unique token, operation, normalized stored location, expected schema JSON, and actual pre-write base version/UUID for replacement. It contains no storage credentials and is not an authorization credential or signature. Expected values originate in durable server registration state and trusted native reads, not a publication request's assertions. These fields do not bind arbitrary CTAS query semantics, row contents, partitions or every user property; schema JSON is not a complete request digest.

## Ownership and HTTP rules

Exact RESERVE replay is accepted only while RESERVED, to retain failed owner-hook repair. Once PUBLISHED, delayed create must fail before the unconditional create owner hook, including after ownership transfer. Lost publication responses use authorized exact PUBLISH replay or authorized GET; they must not regain ownership through CREATE_TABLE permission. Provider actor/recovery denials use the existing Gravitino ForbiddenException mapping (HTTP403), retaining all existing authorization checks.

Parent deletion reaches the raw schema guard through existing catalog and metalake managers. Exercise those routes and document their actual partial side effects, especially force deletion of a disabled metalake. Preserve raw rows, ownership and data. Do not claim cross-server fencing, rollback of prior parent mutations, or fixes to protected managers.

## Native evidence rules

A protocol-action reader inspects the exact JSON commit beneath the stored location and composes with public Kernel snapshot-as-of-version validation. No Spark dependency or Kernel-internal downcast belongs in a server reader. A native UUID/schema must come from observed metadata, never from a submitted receipt. A marker merely inherited by a later snapshot is insufficient.

For the initial strict gate, require validatedSnapshotVersion==latest==candidate, fresh CREATE at version0, or replacement at the trusted actual pre-write base+1. Require exactly one matching metadata action and one CommitInfo with an explicitly allowed native CREATE/REPLACE operation. Validate schema with object-key-order-independent JSON comparison while preserving arrays, nullability and metadata. The reader checks observed UUID and bound base; the enclosing verifier owns actual prior UUID lineage validation. Consume the entire caller-owned stream without closing it; malformed trailing actions and IO failure invalidate an earlier apparent match. Repeated add/remove actions remain valid. Replacement adopts the new Delta UUID while retaining the Gravitino ID/location. Ordinary DML can advance Delta beyond the last publication: capture the actual base immediately before replacement and persist it under the existing complete-row optimistic check.

Wrong identity/token/generation/location/schema, option aliases, missing or altered marker, unrelated ALTER/WRITE with an inherited marker, superseded candidates, missing base evidence, malformed/duplicate actions, absent required logs and IO/permission errors cannot authorize publication. Missing replacement logs do not prove NOT_COMMITTED or justify restoring an old publication. No timeout proves writer termination. Do not modify/delete native logs except explicitly owned disposable negative-test fixtures, and never turn a test callback into synthetic MATCHED evidence in this contract.

## Observable acceptance

1. Preserve original ownership-replay RED XML, then prove delayed create cannot reset transferred ownership, authorized PUBLISH replay remains idempotent, and actor denial maps to403. Cover existing lifecycle/REST regressions and parent guard paths.
2. Implement a reusable strict marker/action-correlation component in production source only when it has meaningful unit coverage; keep integration adapters explicitly test-only until a complete production provider can use them. Its production artifact must contain no synthetic verifier or test handoff control.
3. Drive native schema-only CREATE, CTAS, explicit RTAS and create-or-replace-as-select with the marker in the same commit as metadata/data. Preserve the existing replacement-stage workaround and actual loaded native Delta table type. Schema-only create-or-replace routing is not silently rewritten.
4. Inject failure after native commit and before publication. Preserve pending registration and native data. Stop/reopen the file-backed store/server and Spark, discard in-memory operation context, explicitly hand off the stopped test writer, and recover using real native evidence. Prove stable registration identity/ownership/audit, observed UUID/schema/version, and no second native write on publication replay. Exception injection plus restart is not a process-kill claim.
5. Include ordinary DML before replacement to prove actual base refresh. Negative controls must leave rows hidden/quarantined and data intact. Reuse existing real HTTP and durable lifecycle seams where possible; any narrower intermediate boundary must be reported precisely and cannot replace the final joined acceptance.
6. Execute all four Spark/Delta pairs on the isolated local MinIO with nonzero original XML, exact commands/exits, loaded versions and runtime metadata. Keep default build/live gates intact. Ranger2.8 validation from the previous package remains evidence of separate Admin/download behavior; downloaded-policy Spark enforcement is a following milestone.

## Limits and completion

Do not describe this contract as full Delta conformance, production writer fencing, arbitrary-job security or deployed demo acceptance. The next complete runtime needs published location/native-schema projection, production isolated packaging, authenticated subject/handoff integration, and the remaining operation matrix. Retain source research, decisions, RED/GREEN evidence and independent task/final reviews. No PR, push, merge or catalog-demo deployment is included.
