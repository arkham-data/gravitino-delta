# Delta schema CREATE grants implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Propagate non-owner schema CREATE_TABLE grants through the Delta Ranger plugin.

**Architecture:** Extend existing privilege translation and reuse exact managed-policy/role callbacks. Keep native creation and provider publication unchanged.

**Tech Stack:** Java 17, Ranger 2.8.0; overall target Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Scala 2.12.21.

**Spec:** [Delta schema CREATE grants](../specs/2026-11-04-delta-create-schema-grants-design.md).

## Global Constraints

Gravitino owns registration; native Delta owns files/log/schema/transactions. No core/API/server/client/storage-schema changes, engine fork, Glue compatibility, raw-path fallback or replacement metastore. Keep source policies and existing UPDATE+ALTER semantics unchanged. Target pins remain Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21 and Ranger server/client 2.8.0. This task admits only SCHEMA/CREATE_TABLE/ALLOW; broader scopes remain explicit refusals. No live policy migration or Spark CREATE success claim. Preserve existing evidence/worktree.

## Review Focus

1. CREATE+USE_SCHEMA transitions must remove only CREATE and retain visibility without a Ranger data grant.
2. Generated schema wildcards must not select/mutate exact table policies or Ranger-owned masks/filters.
3. Existing schema owner and other role accesses must survive create grant/revoke.
4. Unsupported scope, DENY and mixed invalid privileges must fail before any Admin call.
5. Repeated grants and user/group membership changes must preserve exact role identity and bounded access.

## Task 1: Schema grant translation and callback lifecycle

**Files:** Modify `authorizations/authorization-ranger-delta/src/main/java/org/apache/gravitino/authorization/ranger/delta/RangerDeltaAuthorizationPlugin.java`; create sibling test `TestDeltaRangerCreateGrants.java`. Update only obsolete schema-CREATE refusal cases in `TestRangerDeltaAuthorization.java` and `TestDeltaRangerSchemaVisibility.java`. Reuse their `Admin`, `plugin`, role and membership fixtures. Add a concise current-support section to `docs/delta-ranger-production-results.md` after tests, preserving historical claims.

**Interfaces:** Preserve `Map<Privilege.Name, Set<AuthorizationPrivilege>> privilegesMappingRule()`, `Set<Privilege.Name> allowPrivilegesRule()` and `List<AuthorizationSecurableObject> translatePrivilege(SecurableObject object)`. Add the mapping and schema branch; append translator-owned wildcards to four components only for nonempty accesses. Existing SELECT_TABLE/MODIFY_TABLE table behavior and all public callback signatures remain unchanged.

- [x] Add focused RED tests in `TestDeltaRangerCreateGrants`: `schemaCreateTranslationIsExactAndVisibilityStaysEmpty`, `invalidCreateGrantsRejectBeforeAdminCalls`, `schemaCreateRoleLifecyclePreservesOwnerAndForeignPolicies`, and `schemaCreateMembershipAndRepeatedChangesStayBounded`. Assert literal `delta_a.sales` -> `[delta_a,sales,*,*]`, one ALLOW `create`, alone/mixed/duplicate cases; explicit refusal families from the spec. Assert actual policy/role contents and unrelated policy snapshots, not only callback booleans. Cover full role creation/acquisition/update/removal/deletion and user/group membership paths across these methods.
- [x] Run the focused class and retain the real missing-schema-CREATE failure. Implement the smallest plugin change. Replace the two old schema-ALLOW refusal examples with still-invalid cases; retain all other existing assertions. Do not weaken the selector or broaden parent scope handling to make tests pass.
- [x] Run the full `:authorizations:authorization-ranger-delta:test` unit task once with `-PskipITs`, actual pinned Ranger 2.8.0 artifacts and no Docker/live endpoints. Require exact selected XML identities, no failure/error, and report deliberately excluded/tagged tests separately. Use Java 17, `JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2 -XX:MaxMetaspaceSize=768m'`, `--max-workers=1 -Dorg.gradle.jvmargs=-Xmx2g --console=plain`. Retain command/exit/log/XML and source/runtime manifests in this plan's workspace. Run scoped Spotless and relevant static checks; no unrelated Spark matrix.
- [x] Document supported scope, explicit regrant requirement, unchanged visibility/data permissions and still-open native/live creation boundaries. Self-review and report; controller obtains task review before commit `feat(delta): propagate schema create grants to Ranger`.
- [ ] Controller obtains whole-slice review from the recorded baseline, with proportionate fixes only. This dependency checkpoint is not whole governed-creation closure. Continue reservation-bound CREATE work; publish the bounded checkpoint only under the standing milestone policy, with exact remote verification if pushed.

## Controller preflight

Single task, no producer/consumer signature dependency. Every review-focus item has a corresponding lifecycle/refusal assertion above. No new public API, schema inheritance reconciliation, creation adapter or source-ingestion authority is implicit in the grant mapping.
