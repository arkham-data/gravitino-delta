# Task 4 intermediate schema visibility review

**No findings. READY within the bounded schema-usage admission scope. This is not Task 4 acceptance.**

Reviewed exact parent `7b9705a02b39bbfbf0e168720dda27bab4a17028` through frozen commit `8100a6cc7ea81ede655d4767a959c4f4bc3b32b9`. Only the optional `RangerDeltaAuthorizationPlugin` admission change and new `TestDeltaRangerSchemaVisibility` are included; separately changing plans, export artifacts, coordinator, and live grants are outside this review.

## Contract and inherited paths

- `USE_SCHEMA` maps to an empty authorization-privilege set and appears in `allowPrivilegesRule`; `SCHEMA` appears in `allowMetadataObjectTypesRule`. These declarations agree with the strict translation instead of relying on inherited HadoopSQL schema privilege mappings.
- Translation admits schema objects only with `USE_SCHEMA/ALLOW`. Existing literal validation still requires exactly two name segments, the bound canonical catalog, and nonempty names without `*` or `?`. Cross-type privilege combinations and DENY remain rejected before callback side effects.
- Schema usage returns an empty resource list before the table wildcard append. It creates no Ranger schema/table/wildcard resource and no policy item. Table SELECT translation is unchanged.
- All optional role create/update/acquire/delete and user/group grant/revoke paths retain centralized role validation. Inherited add/remove/update logic iterates the translated resource lists, so zero-list schema usage contributes no policy mutations. Normal role/principal/membership mirroring remains allowed; the new admission does not bypass those callbacks or their failure handling. Core catalog dispatch/filtering already handles canonical schema objects without a core change.
- This supplies the missing local schema prerequisite for the separately scoped metadata reader. It does not grant query subjects schema usage, substitute their credentials, add owner privileges, or claim that metadata transport credentials have no other preexisting grants. The metadata-reader GET success and query-subject GET denial still require live confirmation.

## Test evidence

Read the original XML and exits under `task-4-evidence/schema-visibility-red/` and `schema-visibility-green/`:

- RED: original exit 1, three new tests with two failures. Schema lifecycle fails its admission assertion and mixed-role construction rejects the previously unsupported schema type; invalid-input rejection already passes.
- GREEN: original exit 0, all three new tests plus all twelve existing `TestRangerDeltaAuthorization` tests pass, with zero failures/errors/skips. The run also records successful strict Javadoc.
- The schema-only test covers create/acquire, user and group membership grant/revoke, updating usage to/from an empty privilege set, removing/adding the schema securable object, and deleting the role. It verifies zero policies throughout and final role removal, plus consistency of both allow-lists and the empty mapping.
- The mixed-role test creates catalog usage, schema usage, and table SELECT together; adding/removing schema usage preserves the serialized policy state exactly. Removing SELECT leaves no policies, and subsequent role deletion succeeds without introducing access.
- Invalid schema grants test foreign catalogs, wildcard schemas, DENY, unsupported privileges, and schema usage bound to catalog/table objects. They assert rejection before any client interaction.

The new test source matches the GREEN pre-run source hash. The archived GREEN plugin source differs from the frozen commit only by Spotless wrapping the existing `Set.of(SELECT_TABLE, USE_CATALOG, USE_SCHEMA)` expression onto two lines. Consequently the pre-run plugin hash is not an exact committed-source hash; direct snapshot comparison establishes no behavioral difference. No rebuilt or newly exported artifact is claimed in this review while the worker's export is pending.

## Limits

No builds, tests, services, live grants, or implementation edits were performed. The only reviewer output is this report. This is a source and retained-test-evidence review; the previously inspected GitNexus index is not aligned to the frozen branch and was not refreshed. Live metadata HTTP 200, continued query-user HTTP 403, absence of schema/wildcard Ranger access, and final four-pair qualification remain separately owned gates.
