# Local Delta and Ranger runtime handoff

This package uses an isolated local MinIO, PostgreSQL and official Ranger 2.8.0. It does not start or modify the existing catalog-demo stack. Public runtime evidence is under `build/delta-s3-ranger-live-evidence/runtime`; credentials are deliberately excluded.

## Observed runtime

| Component | Pinned image / observed version |
| --- | --- |
| Ranger Admin | `apache/ranger@sha256:367328c2677ad64f4ebff0fa9db9e2607205b5bfc318c85cb4a55e5cc973c071`, ARM64, installed 2.8.0 |
| PostgreSQL | `postgres@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20`, 16 ARM64 |
| MinIO | `minio/minio@sha256:da0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac`, RELEASE.2025-07-18T21-56-31Z |
| MinIO client | `minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` |

Compose project `delta-probe-d3542daa` owns its own default network and PostgreSQL volume. Only loopback host ports are published; PostgreSQL is internal. MinIO uses a disposable 512MiB tmpfs and bucket `delta-fixtures`. Spark restart tests therefore check persisted object-store data across Spark sessions, not MinIO server restart durability.

Local runtime files live at `/private/tmp/gravitino-delta-local-runtime`. `compose.yaml` and `state.json` contain public configuration. The private directory is mode700 and its generated environment files are mode600. Reproduction needs fresh generated credentials; do not copy credentials into reports or Git. The image expects the same generated database password in PostgreSQL setup and Ranger database/admin setup. Dedicated test credentials files supply only the keys documented by each live-test plan.

Docker assigned a new host port after Ranger container restart. Always rediscover current mappings with `docker compose -p delta-probe-d3542daa -f /private/tmp/gravitino-delta-local-runtime/compose.yaml port ranger-admin 6080` and the equivalent `minio 9000`; update test endpoint inputs. A connection failure at the old port is not a policy-persistence failure.

## Controller HTTP probes

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

The next deployment milestone requires a production connector artifact and a supported native registration/commit contract, then actual demo queries enforced from downloaded Ranger policies. Current component proofs must not be presented as complete Delta support or production authorization.
