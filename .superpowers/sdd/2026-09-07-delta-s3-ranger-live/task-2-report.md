# Task 2 report: live Ranger Admin and policy download

Base: `db0713916ee12945f4ebeb33461c8ccbc37ab5f1`.
Worktree: `/private/tmp/gravitino-delta-feasibility`.
Run date: 2026-09-07 America/Mexico_City (XML timestamps 2026-09-08 UTC).

## Outcome

Final live execution: 1 live + 5 existing contract tests, all passed, no skips.
Default adapter checks: 1 new constructor + 4 existing helper tests, passed.
Default live-module selection: 5 local contracts, no live execution.
Three missing-input gates failed configuration as required. Scoped format passed.
Independent review is delegated to the controller after this scoped commit.

## Requirement mapping

- `TestRangerDeltaAdminContract`: actual independent Admin `RangerClient`, actual
  candidate constructor/callbacks, separate `RangerAdminRESTClient` full downloads;
  no network mocks, constructor interception, bytecode shims, or Admin-list download substitutes.
  Only the local Role interface is mocked with name and empty securables.
- Unique four-level custom serviceDef copied from the canonical local engine fixture;
  ID omitted, unique name assigned, source/Apache-2.0 attribution retained in description.
  CUSTOM mask and table-level row-filter hierarchies are compared to Admin/download values.
  `lookupSupported=false`; service precreated with matching type; create-if-absent=false.
- Callback correction: actual source API is `onRoleCreated`, not initial research's
  `onRoleAdded`. Real role created, fetched independently, then granted/revoked twice.
- Exactly one owned policy and exact ownership labels/resources/prefixed role/select ALLOW;
  no deny items. Each grant and revoke preserves all three foreign policy IDs/content.
- Independent downloads before grant, after duplicate grant, after duplicate revoke;
  exact service ID/name/hierarchy/models and positive increasing version asserted.
- Finally cleanup runs policies, role, service, serviceDef; only this run's service
  is enumerated, only its policy IDs deleted. Unique-role cleanup registered before callback,
  name existence read before ID deletion. Later cleanup failures preserve the original cause.
- Build forwards enabled/url/credentialsFile only in explicit enabled mode and requires
  skipDockerTests=false plus endpoint and file path. Existing root task marker reused unchanged.
  Test tag retained. Runtime credentials read from private file, never placed in command args.
- Sole authorized production exception: remove unused GenericType reflection lookup from
  authorization-ranger/RangerClientExtension; retained Class/no-response lookups unchanged.
  No core/API/server/client/storage-schema changes and no production dependency upgrade.
- Test-only candidate fixes null condition/extra READ translation; existing local grant/revoke
  assertions strengthened without changing its five-test count. Empty privilege lists stay empty.
- New test-only Hadoop client API compile dependency; actual JNA bootstrap failure justified
  JNA 5.13.0 testRuntimeOnly. No additional runtime dependency was speculatively added.

## Exact commands and preserved results

Every run directory below contains `command.txt`, `exit.txt`, and `gradle.log`.
Commands use JDK17 at `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`.
The private credentials-file path is configured; credentials are absent from commands.

| Evidence directory | Exit | Fresh test counts | Classification |
| --- | ---: | --- | --- |
| first-run | 1 | none | Initial formatter setup failure; no behavioral RED |
| run-2 | 1 | live 1 failed; local 5 passed | Original real Ranger2.8 adapter constructor incompatibility RED |
| run-3 | 1 | live 1 failed; local 5 passed | Missing JNA plugin-client bootstrap dependency; setup |
| run-4 | 1 | live 1 failed; local 5 passed | JSON field-order assertion defect in test; not product RED |
| run-5 | 1 | live 1 failed; local 5 passed | Actual candidate deny-vs-allow behavioral RED |
| run-6 | 0 | live 1 passed; local 5 passed | Initial full live GREEN |
| run-7-compile | 1 | none | Cleanup helper referenced runtime-only Jersey type; compile setup failure |
| final-live | 0 | live 1 passed; local 5 passed | Final full live GREEN, improved cleanup, explicit gate diagnostic |
| default-client | 0 | constructor 1 passed; helper 4 passed | Default Ranger2.4 regression |
| default-gate | 0 | local 5 passed; live excluded | Default marker=false, Docker tag excluded |
| missing-docker | 1 | none | Explicit opt-in requires skipDockerTests=false |
| missing-url | 1 | none | Explicit opt-in requires URL |
| missing-credentials | 1 | none | Explicit opt-in requires credential-file path |
| format-check | 0 | n/a | Both changed modules spotlessCheck |

The final-live command adds scoped spotlessApply and a read-only init script
printing the test task marker/tag exclusions after execution. The script is copied
into evidence. It reports external=true, excluded=[doris-multi-version]. Default
reports external=false, excluded=[gravitino-docker-test, doris-multi-version].
No XML from a previous execution is attributed to the compile-only failure.
`xml-summary.json` contains parsed per-class counts; all success counts are nonzero.

## Runtime and observations

Final service `delta_contract_service_a3e69554bdcb478984c4678da5550c48`, ID9;
unique definition suffix matches. Downloads: versions5/7/8 with policycounts3/4/3.
All registered cleanup steps reported successful. Controller can independently
confirm no resources with these test-owned names remain.

Actual loaded live classes in XML:

- RangerClient: ranger-intg-2.8.0.jar.
- RangerPolicy, RangerAdminRESTClient, ServicePolicies: ranger-plugins-common-2.8.0.jar.
- Default constructor test separately logs ranger-intg-2.4.0.jar.

Deployment endpoint http://127.0.0.1:50915; isolated project delta-probe-d3542daa.
Image apache/ranger@sha256:367328c2677ad64f4ebff0fa9db9e2607205b5bfc318c85cb4a55e5cc973c071.
No runtime restart, other service alteration, demo env read, or shared-policy mutation.

Expected/known log noise: native-Hadoop fallback, Mockito CDS warning, role-not-found
warning before its normal create-if-absent flow, Gradle8 deprecations, and existing
worktree Git-ID build warning. No test result was converted from fail to skip.
Evidence password scan found zero occurrences; no credential values emitted by scan.

## Limits and handoff

Admin CRUD and Java plugin download are live. Existing local Spark enforcement
proofs remain separate; no claim of enforcing downloaded policies in Spark or audit
delivery. No UI metadata lookup, production Delta provider, TLS/Kerberos/HA, or
all user/group APIs on2.8 verification. No other Spark/Delta matrix, Kernel, or REST
reruns were performed in this task.

No claude-mem/mcp-search tool was callable in this agent's tools; this report records
findings instead. Controller owns independent review and final integration.
