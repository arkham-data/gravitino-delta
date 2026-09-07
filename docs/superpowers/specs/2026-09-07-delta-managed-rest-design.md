# Delta managed-provider REST experiment

This continues the approved integration direction after commit `0b7660607`.
The user authorized proceeding and nominated MinIO or RustFS for local S3-compatible
validation and `/Users/jacobo.calderon/Code/Arkham/catalog-demo` as an eventual
integration target. This package proves the real Gravitino boundary first;
object-store and deployed Ranger experiments remain separate executable stages.

## Design and alternatives

Compose the reviewed registration lifecycle fixture inside a test-only catalog
provider loaded by the existing ServiceLoader. Drive an embedded MiniGravitino
through actual Java REST clients with authorization enabled and the existing
JCasbin authorizer. This exercises the unchanged dispatcher, property validation,
owner hooks, and HTTP behavior while retaining the lifecycle's single TableEntity.

Mocked REST dispatch would not prove identity injection or authorization ordering.
A production provider scaffold would prematurely expose unresolved recovery and
fencing behavior. The composed test provider gives direct evidence of existing
extension interfaces before distribution, with no new registration authority.

## Binding constraints

- No Gravitino core/API/server/client/storage-schema source changes.
- New provider and harness stay in generic-module test sources/resources; no production provider registration or distribution.
- Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 and Ranger 2.8.0 remain required eventual targets.
- Gravitino owns registrations; no Glue/Hive registration authority is introduced.
- Ranger owns column permissions, masks and filters; do not add a broad SELECT grant or change existing Gravitino privileges.
- Use real file-backed H2, disabled metadata cache, real HTTP clients, enabled authorization and the existing JCasbin authorizer.
- Simple test principals are client-selected names, not production OAuth/Kerberos identity assurance.
- Native MATCHED evidence and writer handoff remain explicit test controls; no native commit correlation or distributed fencing claim.
- Reuse the reviewed lifecycle protocol without changing its transitions or weakening record validation.
- Capture original module XML with nonzero counts, commands, exits and runtime configuration; setup failures are not behavioral RED.
- English code/docs, Java8-compatible sources on JDK17, normal imports, Nullable, SLF4J, and Javadoc on public/protected declarations and overrides.

## Observable contract

The test provider has a unique short name and managed TABLE/SCHEMA capability.
Capture candidate IDs at provider create entry, after actual dispatcher injection.
An exact HTTP create retry receives a new candidate but retains the original row
identity and full stored state; there must be no second authoritative write.
Internal command/state properties stay out of responses, and existing core
metadata validation rejects client attempts to set or remove reserved state.

Pending rows remain durable but absent from ordinary table list/load/exists.
Existing managed alter dispatch can carry commands without a preliminary provider
load. A non-admin creator with schema CREATE_TABLE/use privileges becomes owner
through the real owner hook, and existing ownership authorizes a pending command.
An outsider is denied before reaching the adapter. A separately privileged foreign
principal reaches the adapter and must still fail its actor check.

Owner REST first tests table existence, so it cannot inspect or repair pending
ownership even when the raw owner relation exists. Execute and document that gap.
A narrowly injected owner-hook failure before relation assignment must leave the
reservation durable while the CREATE-only actor cannot alter it. Exact create
retry can rerun the owner hook without creating a second row or replaying native
work. Restore every injected singleton field in finally; never mock the table
request dispatcher or authorization filter to obtain a passing result.

A test-provider schema guard checks raw rows and refuses both cascade modes while
any table record exists. It protects hidden reservations within this local harness.
A separate unguarded baseline in a disposable schema demonstrates that existing
managed schema cascade can remove a hidden row without table-drop dispatch.
Neither test claims distributed create/drop fencing, catalog/metalake cascade
safety, or atomic rollback of fileset work preceding the provider call.

## Evidence and next stages

The package ends with reviewed HTTP/owner/authorization evidence and explicit
remaining limitations. MinIO tests will exercise actual S3A paths and the existing
Spark named bridge on all four version pairs. A separately pinned Ranger 2.8.0
Admin deployment will then replace mocked Admin evidence where appropriate.
Neither local S3 compatibility nor local container success certifies AWS IAM,
production authentication, deployed audit delivery, or full Delta semantics.
