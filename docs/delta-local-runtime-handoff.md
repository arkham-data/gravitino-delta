# Local Delta and Ranger runtime handoff

This handoff records isolated local MinIO, PostgreSQL and official Ranger 2.8.0 deployments. The latest runtime is described first; the original September setup is retained below as historical evidence. Neither setup modifies the existing catalog-demo stack. Credentials are excluded from public evidence.

## Runtime refresh: October 10, 2026 (Asia/Tokyo)

The old temporary Compose directory and test containers were absent. A fresh isolated
project, `delta-live-20261010`, now runs the same pinned Ranger 2.8.0, PostgreSQL 16,
and MinIO images listed below. Installed Ranger libraries and authenticated Admin
HTTP 200 were verified; MinIO readiness and creation of `delta-fixtures` succeeded.
This deployment has its own persistent database and object-store volumes. Image
example-service seeding is disabled; the contracts provision their own unique resources.

The current-source contracts ran at `4aff7d91e` (October 9 UTC):

| Contract | Scope | Result |
| --- | --- | --- |
| `TestRangerDeltaAdminContract` | Real Ranger 2.8.0 Admin CRUD, foreign-policy preservation and Java plugin downloads | 1 passed, no failures/errors/skips; 7.517 s |
| `TestDeltaS3Contract` | Spark 3.5.4 / Delta 3.3.0 named native lifecycle and Spark-session restart on MinIO | 1 passed, no failures/errors/skips; 26.214 s |

Independent Admin queries found no test services, definitions or roles remaining.
The S3 table objects were removed; only the empty shared parent directory marker
`gravitino-delta-contract/` remained. Both Gradle processes exited zero, and the
archived XML was checked for freshness and integrity.

These are separate component contracts: the Admin contract uses its test-only
candidate plugin, and the S3 contract uses the named-catalog feasibility fixture.
They do not requalify the current production SELECT chain or the new native
idempotency helper. The subsequent [dedicated live helper qualification](delta-live-idempotency.md)
uses real downloaded policies and S3-backed registered targets across all four
supported pairs. Installed governed append remains disabled.

The restored runtime and private evidence are under
`.superpowers/runtime-20261010-live/` in this worktree. `compose.json` references
private generated credential files; `endpoints.json` records the current loopback
ports. Keep this directory private and outside Git. Rediscover ports after restarts:

```bash
docker compose -f .superpowers/runtime-20261010-live/compose.json port ranger-admin 6080
docker compose -f .superpowers/runtime-20261010-live/compose.json port minio 9000
```

`ranger-admin-01/` and `s3-lifecycle-01/` retain exact commands, logs, original XML,
and selected runtime artifact hashes. `root-results-verification.json` records the
independent result checks. Earlier runtime and SELECT qualification evidence remains
preserved. No catalog-demo services were changed.

## Original September runtime (historical)

The following configuration and commands describe the original deployment. Its temporary
Compose directory and containers are no longer present. Public evidence for that run is
under `build/delta-s3-ranger-live-evidence/runtime`. The pinned images are also used by
the October deployment above.

| Component | Pinned image / observed version |
| --- | --- |
| Ranger Admin | `apache/ranger@sha256:367328c2677ad64f4ebff0fa9db9e2607205b5bfc318c85cb4a55e5cc973c071`, ARM64, installed 2.8.0 |
| PostgreSQL | `postgres@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20`, 16 ARM64 |
| MinIO | `minio/minio@sha256:da0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac`, RELEASE.2025-07-18T21-56-31Z |
| MinIO client | `minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` |

Compose project `delta-probe-d3542daa` owns its own default network and PostgreSQL volume. Only loopback host ports are published; PostgreSQL is internal. MinIO uses a disposable 512MiB tmpfs and bucket `delta-fixtures`. Spark restart tests therefore check persisted object-store data across Spark sessions, not MinIO server restart durability.

Local runtime files live at `/private/tmp/gravitino-delta-local-runtime`. `compose.yaml` and `state.json` contain public configuration. The private directory is mode700 and its generated environment files are mode600. Reproduction needs fresh generated credentials; do not copy credentials into reports or Git. The image expects the same generated database password in PostgreSQL setup and Ranger database/admin setup. Dedicated test credentials files supply only the keys documented by each live-test plan.

Docker assigned a new host port after Ranger container restart. Always rediscover current mappings with `docker compose -p delta-probe-d3542daa -f /private/tmp/gravitino-delta-local-runtime/compose.yaml port ranger-admin 6080` and the equivalent `minio 9000`; update test endpoint inputs. A connection failure at the old port is not a policy-persistence failure.

## Original controller HTTP probes (historical)

These are separately recorded deployment probes, not JUnit or complete connector acceptance:

- MinIO readiness and bucket creation succeeded.
- Ranger authenticated Admin API accepted a unique service definition with catalog/database/table/column resources and a disposable service. Access, CUSTOM masking and row-filter policies were created and read back through HTTP.
- After restarting only the isolated Ranger Admin, the same service and all three policy IDs and relevant models were retained in PostgreSQL.
- The local unauthenticated plugin-download endpoint returned HTTP200, policy version6, all three policy types and the four-level definition. The Java downloader contract is tested separately.

The image's first-boot helper attempted to seed unrelated example services using `ranger-admin.rangernw`, which is absent from this isolated network. Those helper requests failed; the unique test service was created explicitly through the Admin API. No Solr is deployed here, so audit delivery is not verified. This local authentication setup does not establish production plugin identity, TLS or access isolation.

## catalog-demo integration target

Source review used `/Users/jacobo.calderon/Code/Arkham/catalog-demo` at `3363d9ebf91d0bb894bda92a989fc3dd5bc763ac`. Existing source changes were preserved.

- Runnable `docker-compose.yml` uses Ranger2.7.0. The unused Ranger Dockerfile mentions2.5.0. Future demo integration must actually run2.8.0 and verify the deployed image, rather than updating the unused Dockerfile alone.
- The demo uses Spark3.5.4/Delta3.3.0 and S3A/MinIO, writes native Delta, then registers lakehouse-generic metadata through Gravitino REST. The other required runtime pairs need explicit coverage.
- Its `create_spark_session` configures native DeltaCatalog without the Gravitino Spark connector artifact. `AuthorizedSession` applies policies in Python around direct path reads; `.sql()` and delegated raw Spark access remain bypasses. That wrapper cannot establish engine enforcement.
- Existing Hive-type policies have database/table/column resources, without the catalog dimension. The candidate Delta contract uses a separate custom definition with catalog/database/table/column and an explicit ownership selector for Gravitino-managed access policies. Foreign column policies, masks and filters belong to Ranger.
- The original Compose hardcodes network `catalog`; a different project name alone does not isolate it. Its `make down` and `make clean` delete volumes. Use a reviewed override or dedicated test topology before later demo work.

The subsequent finite SELECT qualification is recorded in [the production-chain results](delta-ranger-production-results.md). Demo integration and full Delta support remain separate work; the component contracts above do not establish either.
