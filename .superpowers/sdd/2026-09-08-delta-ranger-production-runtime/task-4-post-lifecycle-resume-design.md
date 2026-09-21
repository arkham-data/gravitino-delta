# Task 4 post-role-lifecycle cold resume design

## Scope and conclusion

This design is based on committed harness source
`e1347911c906695a0eedfdfcf6e26d698ce33f6e` and the retained private evidence under
`.superpowers/runtime-20260914/`. The Docker services were intentionally stopped on 2026-09-21.
The historical Gravitino PID 23663 was subsequently observed to exist but was not contacted or
changed. PID existence, its old session, prior readiness, endpoint, downloader cache, and generation
observations are historical evidence only.

The existing `matrix-resume` route is not valid for this state. It accepts only a
`matrix-identities-ready` event, requires `audit_cleanup` to exist, does not know
`metadata_schema`, and compares Ranger roles with the earlier membership snapshot. Reusing it would
either fail before the coordinator starts or invite an invalid repair toward obsolete state.

Add a separate, one-purpose `post-role-lifecycle-resume` route later. It performs a cold-store
preflight followed by read-only public and Ranger readback. It emits a validated-resume event before
ordinary grant/revoke/fault work may begin. It never creates, updates, deletes, replays, or repairs a
fixture.

## State that the resume must preserve

The accepted state is deliberately asymmetric:

| Object | Required state |
| --- | --- |
| `metadata_schema` | Local role exists with exactly `USE_SCHEMA/ALLOW` on the three fixture schemas. The metadata principal has exactly `visibility_metadata`, `metadata`, and `metadata_schema`. Canonical Ranger role ID 25 exists with only that principal, `isAdmin=false`, and no groups or nested roles. It contributes no Ranger access policy. |
| `audit_cleanup` | Local GET is 404 with `NoSuchRoleException`; canonical Ranger role ID 24/name is absent. Its sole-role audit filter is absent. The mixed filter retains `admin`, and the unrelated column-reader filter is unchanged. |
| `global_guard` | Local role remains with its original one-table `SELECT_TABLE/ALLOW` grant. Canonical Ranger role ID 23 remains, with unchanged empty membership. Its managed contribution is absent from the primary resource-service policies. Side policy ID 53 retains the exact normalized model from the baseline and still references role 23. This divergence is expected and must not be repaired. |
| Original protocol role | Canonical role ID 13 remains the same empty Ranger role. Its local role and the fresh-callback user's local membership remain unchanged. The distinct fixed protocol role ID 14 remains separate. |
| Original failed catalog owner | Orphan canonical catalog-owner role ID 7 remains present with the same name and empty users/groups/roles. It must not be deleted, reused, or populated by resume. |
| Other fixture identities | Existing role IDs, memberships, services, definitions, policies, catalogs, registrations, users, and group membership remain exactly as established by the retained post-lifecycle evidence, subject only to the explicit state above. |

## Accepted private resume input

The new route consumes the existing owner-only file
`ranger-production-fixture/post-role-lifecycle-local-checkpoint.json`. The invocation must also
supply its expected SHA-256 independently of the JSON. The accepted current digest is
`90653533d333a59880459f9a88c6d4db236f38e2236abc13bffb444f6e088cda`.

The minimal accepted schema is closed for security-relevant fields:

```text
schemaVersion = 1
stage = "post-role-lifecycle-local-readback"
metalake = fixture.metalake
expectedMetalakeId = fixture.expectedMetalakeId

roles = exactly {
  visibility_metadata, visibility_columnReader, visibility_visibilityOnly,
  visibility_groupReader, visibility_freshCallback, metadata, metadata_schema,
  ordinary, group, global_guard, audit_cleanup, protocol
}

users = exactly {metadata, freshCallback, columnReader, visibilityOnly, groupReader}
groups = exactly {query}

divergence = {
  globalGuardRoleRetainedLocally: true,
  globalGuardPrimaryRangerContributionAbsent: true,
  externalGuardPolicyId: 53,
  doNotRepairOrReplay: true
}

lastObservedDownloads = {policyVersion: 40, roleVersion: 63}
```

Every successful local role entry must have HTTP status 200, response code 0, the canonical local
name derived from the fixture, and the exact normalized grant signature. `audit_cleanup` alone must
have status 404, code 1003, and type `NoSuchRoleException`; its message and stack are diagnostic and
must not be compared. User and group response code must be zero and their role-name sets must match
exactly. `time` and the 18 saved request records are evidence metadata; if present, the requests must
be exactly 17 status-200 reads and the one `audit_cleanup` status-404 read, but neither timestamps nor
request counts establish current state.

The saved `40/63` values are required checkpoint bindings, not proof that a retained or restarted
Ranger service or downloader currently has those generations. Current service versions and
downloaded models must be observed afresh during resume.

## Root-owned process and store preflight

The process owner performs these checks before launching the coordinator. Process retention,
shutdown, service startup, persisted-volume inspection, H2 hashing, and raw S3 continuity are
root-owned orchestration. The Java test harness consumes a digested preflight result; it does not
become a general process, Docker-volume, database-file, or object-store orchestrator.

1. **Reject stale process evidence.** Do not use `active-server.json`, PID 23663, session 52237, old
   health responses, gateway ports, or old cache directories as proof that anything is ready or
   current. Before retaining or restarting a Gravitino process, independently bind its actual
   executable, complete classpath/archive hashes, configuration file, authorization archive,
   working directory, endpoint ownership, and start identity to the retained deployment. A PID
   match alone fails. Allocate fresh run/evidence directories and verify no previous coordinator or
   query JVM owns the callback endpoints.
2. **Retain or restart Gravitino deliberately.** Docker shutdown does not require a Gravitino
   restart. The independently verified existing process may remain if its executable, classpath,
   configuration, authorization archive, working directory, endpoint ownership, and public
   entity/receipt state all match. If any process identity check fails or a restart is otherwise
   chosen, stop only the exact verified owner and restart from the archived inputs and existing H2
   file. Neither path may run schema creation or catalog bootstrap.
3. **Bind H2 bytes only for a cold restart.** When Gravitino is retained, do not hash or claim byte
   equality for its live H2 file; public entity and receipt readback establishes continuity. When a
   restart is chosen, stop the exact verified owner, wait for process exit, and establish that no
   process holds the database. Then hash
   `.superpowers/runtime-20260914/private/ranger-production-2ffb7db265b1/gravitino-db.mv.db` and bind
   the restart to that exact file and pre-open digest. The audit observed size 315392 and SHA-256
   `509a4692d95d863ca5fa738958c2e807b4a33f2bbbc066d9f75fded874bf88e9` while PID 23663 might still
   own H2, so those values are a candidate continuity observation, not an accepted cold baseline.
   Once a restarted H2 opens, byte equality is no longer meaningful; public entity/receipt readback
   is authoritative.
4. **Bind Ranger persistence correctly.** Require reuse of the retained Ranger database volume
   identity/mount. There is no retained portable database-file byte manifest, so do not invent a raw
   byte-equality claim. Prove persistence after Ranger starts through exact IDs and normalized full
   model readback described below.
5. **Bind S3 fixture bytes.** Against the original manifest, require exactly the four recorded
   objects under each of `catalog-a`, `catalog-b`, and `quoted`, with no missing or extra object in
   those owned prefixes and exact recorded relative key, byte length, SHA-256, and native version-0
   content. `post-registration-continuity.json` must still resolve to three entries, four objects
   each, all unchanged. This is read-only object inspection; do not rewrite metadata or materialize a
   replacement table.
6. **Bind executable inputs.** Recheck the archived Gravitino parent/catalog/provider artifacts,
   server configuration, resource definition, and optional authorization archive against their
   retained manifests. A rebuild or newly resolved dependency set is outside this resume.

The process owner records these results in a new owner-only preflight file and supplies its expected
digest independently. The harness validates that binding and records it in the resume event. It does
not perform the process/store operations itself.

Failure of any root-owned preflight check stops the attempt. The operator may preserve new
observations and design a new reviewed checkpoint; the harness must not make the old state true.

## Read-only validation against the retained or restarted services

All checks below complete before the coordinator accepts a mutation command. GET responses must be
recorded as completed upstream responses; gateway drain alone is insufficient.

### Gravitino and native registrations

- Load the exact metalake name and bind its persisted ID to `986854579816189983`.
- Require exactly the three fixture catalogs and load each named table through its catalog.
- Require a PUBLISHED receipt whose native ID, version, and location equal the corresponding
  `delta_a-native.json`, `delta_b-native.json`, or `delta_quoted-native.json` record. This complements
  the raw S3 byte check and proves the H2 registration still refers to the original table.
- Compare every successful local role to the checkpoint's exact grant signature. Require
  `metadata_schema` and `global_guard` exactly as described above and require `audit_cleanup` to be
  absent.
- Compare the five users and query group to the checkpoint's exact role-name sets. No additional
  fixture-prefixed membership is allowed.

### Ranger identity and model

- Read back the exact resource definition ID 217, tag service ID 15, resource service ID 16, and
   downloader user ID 9/name from the retained full models. Read side service ID 17 separately.
  Compare complete models with their retained baselines. A field may differ only when an identified
  retained before/after record proves that exact server transformation; there is no generic timestamp
  or server-managed-field ignore rule.
- For every owned canonical role from the original matrix snapshot, require the same ID, name, and
  normalized users/groups/nested-roles. Apply the accepted delta: role 24 is absent and role 25 is
  present. Require IDs 7, 13, and 23 explicitly even if the broader loop already checked them.
- Require all primary managed policy IDs 35-43 and all nine foreign policy IDs 44-52. Compare each
  foreign policy's full model with `foreign-policy-state.json`. Equality remains exact except for an
  individually enumerated transformation already proven by retained evidence; name-only, count-only,
  version-only, or blanket timestamp-normalized comparison is insufficient.
- Compare the current primary policy array with the retained post-lifecycle snapshot. No primary
  policy item may reference role 23 or role 24. No policy item may reference `metadata_schema`.
  Other managed and foreign policy items remain exact.
- Require side policy ID 53 and compare its full model with `guard-audit-state.json`. It
  must be enabled and its sole role reference must be canonical role ID/name 23. As with the nine
  primary foreign policies, only an explicitly evidenced and enumerated server transformation may be
  normalized. This side reference explains why Ranger rejected deletion while the primary
  contribution had already been removed.
- Require side service ID 17 to have the retained post-cleanup audit filters: the audit-only role
  filter is absent, the denied/admin filter remains without role 24, and the unrelated allowed
  column-reader filter remains. Compare parsed filter objects so whitespace and quote spelling are
  irrelevant.
- Require the primary service's current policy and role versions to equal the checkpoint boundary
  `40/63` before any new mutation. Then obtain fresh policy and role downloads or the separately
  approved recorder evidence and compare their complete semantic models with current Admin
  readback. A version number, cache file, or readiness flag alone cannot pass.

### Validation output and continuation boundary

Write `post-role-lifecycle-resume-validated.json` with the input checkpoint digest, root-owned
preflight digest/mode (`retained-process` or `cold-restart`), conditional cold H2 digest, S3 manifest
digest/result, catalog receipt identities, service/definition IDs, named role/policy IDs, current
versions, semantic comparison results, public/callback completion counts, and the explicit four-part
divergence object. Do not write credentials, authorization headers, storage secrets, policy
expressions beyond their retained evidence references, or raw private response bodies.

Only after this event is durably created may a new coordinator accept ordinary independent
grant/revoke/fault commands. The continuation starts with `global_guard`, `audit_cleanup`,
`metadata_schema`, and the original protocol/owner artifacts in a protected set. Commands that
target, recreate, delete, grant through, revoke through, or otherwise reconcile those protected
objects are rejected before an API call. The accepted divergence is not a clean matrix state and
must not be fed into the old loop with `quarantined=false`.

## Difference from the current `matrix-resume`

| Current committed behavior | Required post-lifecycle behavior |
| --- | --- |
| Accepts stage `matrix-identities-ready` under `details`. | Accepts only the digested owner-only `post-role-lifecycle-local-readback` schema. |
| Requires `audit_cleanup` as an existing local and Ranger role. | Requires local and Ranger absence and verifies audit-filter cleanup. |
| Does not know `metadata_schema`. | Requires the exact local schema grants, metadata membership, Ranger role ID 25, and zero policy contribution. |
| Requires the old `global_guard` role/membership identity but does not model partial delete. | Requires local role/SELECT retention, Ranger role ID 23 retention through side policy 53, and absence from every primary policy. |
| Compares selected role IDs/memberships from one old Admin snapshot. | Composes the original identity baseline, metadata-schema snapshot, post-delete snapshot, full foreign baselines, and fresh retained-or-restarted service readback. |
| Checks catalog receipts and local grants, then starts the ordinary matrix loop. | Consumes a root-owned retained-process or cold-restart preflight, then adds exact Ranger service/definition/role/policy checks, fresh download comparison, and protected-object continuation. |
| Has no checkpoint digest requirement. | Requires an independently supplied SHA-256 and owner-only input permissions. |
| Does not distinguish a restarted service from a still-running process. | Rejects stale PID/health/cache/generation assumptions and validates the retained or cold-restarted state. |

## Minimal source and test changes for the later source owner

1. Add the `post-role-lifecycle-resume` main argument and a separate
   `resumePostRoleLifecycle()` method. Do not weaken or silently reinterpret `matrix-resume`.
2. Add a bounded parser/validator for the schema above and verify the independently supplied digest
   before parsing. Reuse `privateFile` for absolute regular owner-only input validation.
3. Factor pure helpers for normalized local grant signatures, canonical role lookup, role
   identity/membership comparison, policy comparison, audit-filter comparison, and validation of the
   root-owned preflight attestation. The harness does not inspect processes, Docker volumes, H2 bytes,
   or raw S3 itself. Every allowed model transformation must be enumerated with its retained evidence;
   there is no generic field ignore list.
4. Add a continuation loop that starts with the protected-object set and refuses those operations
   before dispatch. It may reuse command-file mechanics only after the validated event exists.
5. Ensure pre-validation transport is read-only. A focused test must prove no POST, PUT, PATCH, or
   DELETE reaches either gateway when validation succeeds or fails.
6. Add fixture tests for the exact accepted checkpoint plus one mutation per boundary: wrong digest,
   stage/metalake/ID, missing or mismatched root preflight attestation, schema role missing or
   contributing a policy, audit role present, guard local role absent, guard restored to the primary
   policy, side policy 53 missing or
   changed, foreign policy 44-52 drift, role IDs 7/13/23/25 changed, membership drift, service or
   definition ID drift, version drift, incomplete download, and an attempted protected-object
   command. Each case must fail before mutation.

No test or implementation should contain live credentials, replay bootstrap, create a replacement
fixture, or treat repair as resume.

## Retained evidence pointers

- `ranger-production-fixture/post-role-lifecycle-local-checkpoint.json` — owner-only checkpoint,
  SHA-256 `90653533d333a59880459f9a88c6d4db236f38e2236abc13bffb444f6e088cda`.
- `ranger-production-fixture/matrix-identities-ready-private.json` — original identity baseline,
  SHA-256 `c11f2fc44a3f604727438bb0fd504a158ed703602793127e6ca1a755d61d33f2`.
- `ranger-production-fixture/evidence-schema-visibility-private/matrix-resume-validated.json` —
  original no-replay resume and role IDs.
- `ranger-production-fixture/evidence-schema-visibility-private/matrix-snapshot-after-metadata-schema.json`
  — role 25 and unchanged policy evidence.
- `ranger-production-fixture/evidence-schema-visibility-private/matrix-snapshot-after-audit-global-guard.json`
  — final primary model snapshot after both delete operations.
- `ranger-production-fixture/root-metadata-schema-readback.json` — independent metadata 200/query
  403 readback.
- `ranger-production-fixture/root-global-guard-audit-readback.json` and
  `root-global-guard-divergence.json` — local audit absence, retained guard role, and the explicit
  primary/side-policy divergence.
- `ranger-production-fixture/foreign-policy-state.json` and
  `foreign-verify-20260915T011459Z-b36302/` — full nine-policy baseline and last independent
  continuity readback.
- `ranger-production-fixture/guard-audit-state.json` and
  `guard-audit-verify-{audit,guard}-*/observed-service.json` — side service 17, policy 53, and
  post-cleanup audit-filter state.
- `ranger-production-fixture/delta_{a,b,quoted}-native.json` and
  `ranger-select-fixture/post-registration-continuity.json` — original native IDs, locations,
  version zero, and twelve exact object-byte records.
- `.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-report.md:197-211` and
  `progress.md:254-288` — chronology and scope limits; these reports do not replace raw evidence.
