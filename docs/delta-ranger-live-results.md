<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Live Ranger 2.8 Admin and policy download

On 2026-09-07 (America/Mexico_City), the opt-in live contract passed
against the isolated Apache Ranger 2.8 deployment. One live test and
all five existing local Ranger Delta contracts executed with zero failures
or skips. The test loads and verifies `ranger-intg-2.8.0.jar` for
`RangerClient`, and `ranger-plugins-common-2.8.0.jar` for `RangerPolicy`,
`RangerAdminRESTClient`, and `ServicePolicies`.

## Boundaries exercised

`TestRangerDeltaAdminContract` creates a unique custom service definition
and service through a real `RangerClient`. Its four-level
catalog/database/table/column definition comes from the existing
`spark-connector/v3.5/spark/src/test/resources/ranger-delta-local-policies.json`
fixture. CUSTOM masks and table-level row filters retain their canonical
hierarchies; `lookupSupported=false` makes the absence of a metadata lookup
implementation explicit.

The candidate uses the precreated service, with service creation disabled.
Its inherited `onRoleCreated` callback creates a unique prefixed role from
an empty securable-object list. (`onRoleAdded` in the initial research was
an API naming error.) Two table SELECT grants produce exactly one owned
access policy with the three Gravitino ownership labels, exact four-level
resources including `column=*`, and one allowed `select` for the prefixed
role. The policy has no deny items. Two revokes remove it idempotently.

An independent Admin client verifies that foreign column access, CUSTOM
mask, and row-filter policies retain their IDs and full JSON content after
each grant and revoke. JSON comparison ignores object-field ordering but
preserves values and array contents.

A separate `RangerAdminRESTClient`, configured with the supplied endpoint
and full downloads enabled, calls `getServicePoliciesIfUpdated(-1L, 0L)`.
It verifies the service ID/name, policy version, complete canonical
service definition, and all Admin-read policy models. The final execution
observed policy snapshots of 3, 4, then 3 policies at versions 4, 6, and 7.
This is actual Java plugin download traffic, independent of Admin CRUD.
It uses the isolated deployment's plugin-download authentication behavior;
Admin credentials are used only for Admin CRUD.

Each invocation explicitly creates its own `tag` service and associates it
with the resource service before creation. It sets
`create.default.policy.per.hierarchy=false` only on that disposable tag service
and asserts its policy list is empty; tag-policy semantics are outside this
contract. The test verifies the persisted association and unique tag-service ID.

Cleanup removes resource policy IDs, the unique role, the resource service,
tag policy IDs, the tag service, and the custom service definition in dependency
order. Every registered step runs after a failure, with cleanup errors suppressed
onto the original failure. A final Admin list assertion verifies that both unique
services are absent. An independent post-run query also found no test services or
definitions; the readiness service's stable identity and all three original
readiness policy models were unchanged.

## Defects exposed and changes

The first actual live execution failed in `RangerClientExtension` after
successful Admin create/read calls. Its constructor reflected an unused
private `RangerClient.callAPI(..., GenericType)` overload. Ranger 2.8 uses
`TypeReference` for that overload. Removing the unused import, field, and
lookup restores construction without changing the two reflection paths
actually used by user/group methods. This is the sole production change,
inside the Ranger authorization adapter. The default Ranger dependency
remains 2.4.0; its new constructor regression and four existing helper tests
passed with the actual 2.4.0 JAR loaded.

A later live assertion exposed a test-only candidate defect: it passed raw
Ranger enum privileges with a null condition, so the helper created deny
items. The candidate now translates its already-supported SELECT_TABLE
ALLOW case to SELECT with the original ALLOW condition, without adding
READ. Its existing local grant/revoke contract now checks allowed select,
role/access uniqueness, and an empty deny list.

Independent review exposed a gap in the first cleanup implementation: Ranger
had automatically created `delta_contract_service_tag` (ID4). Ranger 2.8 schedules
linked-tag creation after service creation and derives its name by replacing the
last underscore suffix, so the initial UUID-last service names shared that tag
service. The initial passing XML verified only registered cleanup steps; it did
not establish zero side effects. The explicitly owned tag service now prevents
this behavior. The known leftover's ID, name, type, and creation time were verified
before deleting only its policies and service. [Ranger 2.8 service creation](https://github.com/apache/ranger/blob/release-ranger-2.8.0/security-admin/src/main/java/org/apache/ranger/rest/ServiceREST.java#L4425)

The first explicit tag-service attempt encountered a server
`NonUniqueResultException` while creating default policies. The final test uses
Ranger's supported per-service `create.default.policy.per.hierarchy=false` option
to avoid those unnecessary policies. No shared definition or database repair was
performed. The failed attempt remains preserved as deployment/bootstrap evidence;
tag default-policy generation is not validated here. [Ranger default-policy configuration](https://github.com/apache/ranger/blob/release-ranger-2.8.0/agents-common/src/main/java/org/apache/ranger/plugin/service/RangerBaseService.java#L463)

The download client's actual execution additionally required test-only JNA
5.13.0 for `RangerRESTUtils`/`gethostname4j`. Hadoop client API is a test-only
compile dependency for the download configuration. Production dependencies
and the default test exclusion remain unchanged.

## Reproduce

Create a private Java-properties-compatible credential file defining
`RANGER_ADMIN_USER` and `RANGER_ADMIN_PASSWORD`; do not put values in command
arguments or checked-in files. Supply the endpoint and credential-file path:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
./gradlew :authorizations:authorization-ranger-delta:test \
  -PrangerDeltaAdminEnabled=true \
  -PrangerDeltaAdminUrl=http://127.0.0.1:50915 \
  -PrangerDeltaAdminCredentialsFile=/private/tmp/gravitino-delta-local-runtime/private/ranger-admin.env \
  -PskipDockerTests=false \
  --tests '*TestRangerDeltaAdminContract' --tests '*TestRangerDeltaContract'
```

The recorded endpoint belongs to this local run; configure it for another
runtime. Missing endpoint, credential-file path, or explicit
`-PskipDockerTests=false` fails configuration. Missing/unreadable credential
files or missing keys fail the selected live test. Ordinary runs require no
external service and exclude `gravitino-docker-test`. Explicit live mode
reuses the validated external-endpoint test marker; it does not change the
root Docker environment detection.

Evidence is under `build/delta-s3-ranger-live-evidence/task-2/`: exact commands,
exit codes, original failure XML, final success XML, loaded JAR locations,
image/endpoint metadata, and default/input/format gate results. The final cleanup
fix is in `linked-tag-green/`; `linked-tag-fix/` preserves the earlier explicit-tag
failure, source files with SHA256 provenance, and the owned-leftover deletion proof. The runtime
image was `apache/ranger@sha256:367328c2677ad64f4ebff0fa9db9e2607205b5bfc318c85cb4a55e5cc973c071`.
The original formatter failure, missing-JNA bootstrap failure, a JSON-order
assertion defect, and a subsequent cleanup-helper compile failure are
identified separately; they are not behavioral RED evidence.

## Limits

This test proves live Admin lifecycle and Java plugin download compatibility
for this bounded candidate. Existing local Spark/Ranger engine tests prove
column permissions, masks, and filters separately from fixture policies;
this task did not rerun that matrix. It does not prove Spark enforcement
from downloaded policies, audit delivery, Ranger UI resource lookup,
production Delta-provider readiness, or all user/group APIs on Ranger 2.8.
The local deployment's plugin-download behavior is not a claim about TLS,
Kerberos, or production authentication. No Gravitino core, public API,
server, client, or storage-schema behavior changed.
