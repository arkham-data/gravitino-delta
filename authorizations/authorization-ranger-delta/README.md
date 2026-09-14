<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Optional Delta Ranger authorization

`ranger-delta` implements the Gravitino authorization SPI for `lakehouse-delta`.
It pushes owned access policies to a provisioned Ranger 2.8 service. Query execution
and policy/role downloads belong to the separate Spark enforcement stage.

Provision the bundled `ranger-delta-servicedef.json` under the chosen service type,
then provision one service for exactly one metalake. Its required, nonsecret
`ranger.plugin.spark.gravitino.metalake` service configuration must equal the canonical metalake name.
The plugin validates the service and definition at startup and before callback
mutations. It never creates a service or installs/updates a definition.

Catalog configuration requires:

```properties
authorization-provider=ranger-delta
authorization.ranger.admin.url=https://ranger.example
authorization.ranger.auth.type=simple
authorization.ranger.username=deployment-admin
authorization.ranger.password=<use the deployment secret mechanism>
authorization.ranger.service.name=delta_for_one_metalake
authorization.ranger.service.type=spark
authorization.ranger.service.create-if-absent=false
authorization.ranger.delta.metalake=canonical_metalake
authorization.ranger.delta.catalog=canonical_catalog
```

The provider checks the last two immutable properties against the actual
CatalogEntity before authorization initialization. Metalake/catalog IDs come from
BaseAuthorization, not user-supplied identity guesses. Base Delta remains free of
Ranger dependencies.

## Admitted callbacks

* TABLE / SELECT_TABLE / ALLOW becomes `select` on exactly
  `catalog / database / table / column=*`.
* Canonical CATALOG / USE_CATALOG / ALLOW supplies Gravitino catalog visibility
  and contributes **zero** Ranger resources or access items. Visibility-only and
  mixed visibility/SELECT roles are supported.
* Other privilege/type/condition combinations fail before Admin mutations.
* Literal resource names containing `*` or `?` are not admitted. Internally generated
  descendant selectors remain distinct; braces, backslashes and token-looking
  names retain literal meaning under the closed matcher options.
* Rename/drop callback transformations are unqualified and explicitly rejected.
* Global user/group removal callbacks are unqualified and throw
  `UnsupportedOperationException` before any Ranger Admin interaction. A catalog's
  service binding does not authorize deletion of a shared Ranger principal.
  Supporting removal requires a separate global identity ownership/reference and
  reconciliation contract. This callable SPI boundary does not imply that current
  public Gravitino `removeUser`/`removeGroup` requests dispatch these callbacks.

Ranger's role namespace is global. Ordinary roles use
`GRAVITINO_METALAKE_<metalakeId>_ROLE_<roleName>` and are shared across that
metalake's catalogs. Raw Gravitino role names beginning `GRAVITINO_` (ignoring
case) are not admitted. The final catalog callback removes the shared role after
its policy references are removed; unresolved or foreign references cannot be
silently treated as deletion convergence. The configured-service scan is not a
global absence check: final deletion relies on Ranger 2.8's Admin guard against
policy, role, and security-zone references across services, and propagates its
rejection. Role deletion callbacks run before Gravitino deletes its local role;
a thrown callback therefore retains the local role, although earlier owned policy
removals can already have persisted. Upstream successful role deletion also removes
that role from service audit-filter configuration. This upstream side effect and
global reference handling still require live Ranger qualification.

Owners receive exactly `all`, implying select/read/write/create/drop/alter/update,
with `delegateAdmin=false`. Complete owner resources are C/*/*/* for metalake and
catalog, C/S/*/* for schema, and C/S/T/* for table. Metalake and catalog owner
items coexist through distinct ID-suffixed reserved roles. Schema/table owner
items use the membership-free reserved marker role. Reconciliation replaces the
complete desired owner membership/item and verifies Admin readback; it does not
trust the previous-owner hint. Foreign/conflicting reserved roles are rejected.

Policy mutations require ACCESS type, exact service, all managed/metalake-ID/
catalog-ID labels, exact four-level resources and nonrecursive/nonexcluded
selectors. Duplicate owned candidates fail. Ranger-owned column policies, masks
and row filters are preserved.

## Persistence and retry limits

Gravitino and Ranger are not one transaction. Owner PUT and table POST can report
a callback failure **after** ownership/registration persistence. Schema POST uses
batch owner callbacks and can return success while logging callback failure.
Catalog POST attempts local deletion on callback failure without rolling back
Ranger effects; local cleanup can itself fail.

After the original request and callbacks are known complete, and current ownership
has been read, an explicitly serialized same-owner PUT repairs schema/catalog/
metalake or **published-table** owner state. There is no expected-owner CAS.
Pending registrations remain hidden from public Owner GET/PUT. A completed pending
POST failure uses the existing exact same-creator/command pending POST replay,
subject to its existing ownership and serialization gates. Ownership presence is
never evidence of Ranger convergence.

Create a SELECT-bearing role before assigning membership. Empty-role creation and
membership do not dispatch catalog callbacks. A first relevant privilege update
establishes/verifies its Ranger role but does not infer prior user/group membership.
The tested ordinary membership replay plus readback is required for that order.

Grant/revoke can fail after local privilege persistence, including removal of the
last privilege. Identical retries can skip callbacks. Same-owner PUT repairs owners
only and does not repair ordinary grant divergence. A general serialized desired-
state role/grant reconciler remains required before unrestricted rollout; do not
use a temporary regrant as a repair protocol. Empty roles can likewise cease to
provide a catalog callback route. Admin state and later genuine downloaded policy/
role versions must be checked independently before claiming enforcement convergence.

## Packaging and qualification

```bash
./gradlew :authorizations:authorization-ranger-delta:copyLibAndConfig
./gradlew :authorizations:authorization-ranger-delta:test -PskipITs
./gradlew :authorizations:authorization-ranger-delta:spotlessCheck \
  :authorizations:authorization-ranger-delta:javadoc
```

The distribution is `authorizations/ranger-delta/libs`, with one pinned Ranger
2.8 closure. The ordinary Ranger module's default version is unchanged. Gravitino
API/core and shared logging/Guava are supplied by the server. The optional client
adapts legacy user/group JSON through Map requests/responses on the existing
RangerClient private `callAPI` extension seam. This is a pinned 2.8 compatibility
dependency; authentication and HTTP behavior remain Ranger's. No global JSON mapper
changes, project test fixtures, fake downloaders or processor replacements are packaged.
Upstream Ranger classes, including audit-core's `audit/test/TestEvents`, remain intact.

The local REST tests use real Gravitino dispatch, its public Java client, and the
production Ranger HTTP client against an Admin boundary fixture. Tagged native
ADOPT/PUBLISH callback tests additionally require a private root-owned fixture:

```bash
./gradlew :authorizations:authorization-ranger-delta:test \
  --tests '*TestRangerDeltaProductionCallbacks' -PskipITs -PskipDockerTests=false \
  -PrangerDeltaCallbackFixture=/private/path/fixture-inputs.json
```

The input names a native location, warehouse, endpoint/region, and a credential-file
reference. Tests only read native data. This verifies callback contracts against a
real native Delta table; it does not qualify live Ranger acceptance or Spark
query enforcement. Historical candidate tests remain test-only regression evidence.
