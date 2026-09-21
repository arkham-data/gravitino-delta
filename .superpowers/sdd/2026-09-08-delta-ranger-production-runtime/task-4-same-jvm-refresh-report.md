# Same-JVM fresh-query refresh qualification

Ranger 2.8.0 grant and revoke updates were observed by fresh SQL queries in one
retained Spark driver per version pair. All 48 query checks passed independent
verification: four exact positive results and 44 precise authorization denials.
This adds to the previous 160 live checks; the combined total is 208.

| Spark | Delta | Driver PID | Initial policy/role | Granted | Revoked | Checks |
| --- | --- | --- | --- | --- | --- | --- |
| 3.5.4 | 3.3.0 | 80634 | 48/73 | 49/74 | 50/75 | 12 |
| 3.5.4 | 3.3.2 | 95854 | 50/75 | 51/76 | 52/77 | 12 |
| 3.5.6 | 3.3.0 | 4703 | 52/77 | 53/78 | 54/79 | 12 |
| 3.5.6 | 3.3.2 | 9363 | 54/79 | 55/80 | 56/81 | 12 |

Each driver ran the same four SQL statements before grant, after grant and after
revoke. The ordinary user had no table access initially. A public Gravitino user
role grant enabled only the complete three-column read of the registered table in
catalog A; reads from catalog B, the quoted-column catalog and their cross-catalog
join stayed denied. Public revoke removed the table access again. All catalogs
remained discoverable, actual application/UGI identity matched the intended user,
and actual groups were empty. Metadata credentials remained separate.

All four drivers retained their PID across the three phases, sent exactly 12
query commands and one close command, and exited naturally with code zero. Each
initial phase required fresh empty policy caches and completed policy and role
HTTP 200 downloads. Each later phase required new completed service-bound policy
and role HTTP 200 downloads after the pre-transition cursor, plus exact policy and
role versions before and after every query. There are 24 required completed
downloads across the four drivers. Background downloads during the root readback
wait are included; the cursor must not move past those valid updates just before
query release. Queries call the existing plugin refresh API and construct a new
`spark.sql` plan for every command.

Each public grant/revoke uses the existing Gravitino REST API. Root waits for its
complete response, collects 33 public/Admin/download models and verifies the exact
intended membership change, permitted audit/generation changes and preservation of
all other models before releasing the dependent SQL phase. The 10 foreign Ranger
policy models also pass full Admin readback after every transition. No direct
candidate callback, bootstrap replay or Gravitino core change is used.

The final readback is policy 56 / role 81. Original memberships are restored;
protected global-guard divergence, the removed audit role, metadata schema roles
and original/fixed protocol roles retain their previous state. Final GET-only
comparison matches all 33 models from the last verified revoke. The retained
Gravitino process, 361 deployment files, 145 parent JARs and two persistent volumes
were checked again. Final S3 readback verifies all 12 original object keys, sizes,
bytes and the explicit restored modification-time baseline. This is continuity
from the earlier byte-for-byte restoration, not a claim of uninterrupted storage.

The independent control-chain audit additionally recomputed all four cell
acceptances and matched exactly eight public HTTP 200 mutations to eight completed
Ranger role HTTP 200 callbacks, with no extra mutating callbacks in the matrix
window. It verified all eight complete-model transitions and all 24 required
initial/refreshed driver downloads against their startup or mutation response time.
Its six offline tests passed. The root reran this audit independently. Read-only
GET requests can overlap a callback; that does not imply concurrent mutations.

## Reproducible evidence and corrections

Private evidence is under `task-4-evidence/query-preparations/same-jvm-fresh-*` and
`same-jvm-root-inputs-20260921/`. Each cell has an independently produced
`root-same-jvm-verification.json` bound to original results, command/response hashes,
phase proofs and all 259 runtime artifacts. Known credential scans found no matches
in query logs, results, control messages or phase envelopes.

The runner passed 16 offline tests and independent source review. The public
mutator passed 19 offline contract tests and independent review. The independent
verifier passed 16 tests after correcting one archive-permission assumption:
nonsecret hash-bound artifact manifests permit their original 0644 permissions;
private query evidence still requires owner-only permissions. The original frozen
verifier and failed audit are retained. No query or public mutation was repeated.
Initial mutator review failures and the corrected background-refresh cursor tests
are retained as offline evidence.

Frozen runner SHA256:
`9b296234a55f2f1fe98aa0a4cb26e9cff81b977be1c2b42e3d14a8e3866e7b30`.
Independent verifier v2 SHA256:
`449cc9b3ee121e4498b2ee99d34302052321f1f5ea14986e381ede00bae360a4`.
Public mutator SHA256:
`ec109aa023156fd725bab75498c1aad84c67a03297c4f8dd8f213890407f7855`.
The immutable production companion remains
`5d91d3aa6db812d4d5a6247c205d52c204e097d72acb3996517f17669bc19be7`.

## Remaining scope

This qualifies fresh SQL analysis in a retained JVM for the stated user membership
transitions. Both policy and role generations advanced together; independent
role-only publication and outage/last-known-policy behavior remain unqualified.
It does not qualify reusing an analyzed/optimized/physical plan, user caches,
already-running actions, retained RDDs or iterators, AQE, or arbitrary driver code.

The separate test-only collection-boundary prototype demonstrates public Spark
hook mechanics on two diagonal version pairs. It installs no production guard and
contains no complete security receipt. The next production slice must retain and
validate original access/mask/filter decisions, full engine and role publication
identity, registration incarnation and typed native provenance before supported
collection actions. Native scans, broadcast joins and explicit cache/AQE handling
need their own qualification. Task 4 and the broader full-Delta goal remain open;
DML/DDL, streaming, maintenance, remaining masks and catalog-demo integration are
not closed by these SELECT tests.
