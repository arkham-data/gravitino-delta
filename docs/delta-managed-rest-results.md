# Delta managed-provider REST experiment

This test-only package exercises the existing Gravitino HTTP, managed persistence,
and JCasbin authorization boundary. It does not distribute a production Delta
provider. The reviewed `RegistrationLifecycleFixture`, `RegistrationCommand`, and
`RegistrationCommitVerifier` are composed unchanged.

## Executed boundary evidence

The focused REST run passed **12 tests, zero failures and zero skips** on JDK 17,
using Java 8-compatible new sources. The first executable guard test failed before
the guard was added: HTTP schema cascade returned successfully while a hidden
reservation existed. Its original XML records one executed test and one assertion
failure. Earlier formatting, compilation and ServiceLoader setup failures are not
behavioral RED.

| Boundary | Observed result |
| --- | --- |
| Dispatcher identity and managed retry | Requests contain only the command. The real dispatcher supplies distinct candidate IDs on exact create retry; the receipt and sole durable row retain the first ID. Full `TableEntity` equality preserves audit, columns and internal properties, detecting dispatcher replacement of the authoritative row. No verifier/native work runs on reserve or retry. |
| Pending visibility | Actor list, GET and exists omit a durable pending row. A direct real dispatcher exists check also returns false; reads leave the full raw row intact. |
| Property validation | Core rejects client-supplied reserved ID, internal and receipt properties on create/alter, and rejects internal removal before provider invocation. Malformed version and mixed ordinary changes fail without changing the row or calling the verifier. The command is not persisted; receipts contain only four receipt fields. |
| Real authorization | A non-admin creator with CREATE_TABLE/use permissions, who is not schema owner, becomes raw owner and stored actor/creator. Existing ownership permits pending publication with synthetic MATCHED evidence. An outsider with only use privileges is denied before provider alter. No SELECT_TABLE grants are used. |
| Provider actor restriction | A foreign principal with existing MODIFY_TABLE/use reaches the provider but its actor check rejects the operation. The row remains unchanged. The Java client receives a generic `RuntimeException` containing the provider `SecurityException`; existing server/client handlers map this internal-error response to HTTP 500, not a dedicated actor-denial 403. |
| Owner REST visibility gap | The raw owner relation exists while authorized admin owner GET and SET return `NoSuchMetadataObjectException` (the missing-metadata/404 path). Both owner REST operations work after synthetic publication. Pending owner inspection and repair remain unresolved. |
| Owner hook failure | A forwarding fault injector replaces only `GravitinoEnv.internalOwnerDispatcher` and fails target TABLE assignment before the relation write. Real HTTP create fails, but the RESERVED row survives without an owner. The CREATE-only actor cannot alter it. Restoring the field in `finally` and exact create retry allocates a new candidate, preserves the full row and assigns its owner without native replay. |
| Recovery boundary | A recovery principal with existing MODIFY_TABLE/use still requires explicit writer handoff and verifier evidence. Token possession by the use-only outsider is insufficient. AMBIGUOUS evidence produces an unavailable QUARANTINED row; verified absence after handoff produces a retained ABORTED row. The local sentinel remains unchanged. |
| Guarded schema drop | Both cascade modes refuse raw RESERVED, REPLACING, PUBLISHED, ABORTED and QUARANTINED rows. Rows and data sentinels survive; no table drop is invoked. Empty schema cascade succeeds. |
| Unguarded baseline | Direct existing `ManagedSchemaOperations` cascade in a disposable schema deletes a hidden raw reservation without provider table drop. The data sentinel survives. This is an unsafe registration-retention boundary, separate from the test-provider guard. |

Receipt-only output originates in the reviewed fixture; these tests separately
prove core reserved-property input validation. They do not claim core masking
alone generates that receipt projection. Full-row stability establishes the
managed persistence outcome, not a count of attempted database writes.

## Harness and reproduction

`RegistrationTestCatalog` and `TestDeltaRegistrationRest` live only under
`catalogs/catalog-lakehouse-generic/src/test`. The actual ServiceLoader contract is
`org.apache.gravitino.CatalogProvider`, so the test resource is
`src/test/resources/META-INF/services/org.apache.gravitino.CatalogProvider`.
The early research incorrectly included `.connector` in this interface name;
the executable bootstrap exposed and corrected that test-resource mismatch.

The suite starts `MiniGravitino` with a unique file-backed H2 path, disabled cache,
disabled catalog isolation, enabled authorization, the existing JCasbin authorizer,
simple named principals and no auxiliary services. It uses real
`GravitinoAdminClient` requests for bootstrap, role grants and table/owner/schema
operations. It verifies the H2 database file and runtime settings. Execution is
serial; clients and server are closed, controls reset after each test, and the
previous singleton configuration and injected owner field are restored.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :catalogs:catalog-lakehouse-generic:spotlessApply \
  :catalogs:catalog-lakehouse-generic:test -PskipITs \
  --tests '*TestDeltaRegistrationRest' \
  --tests '*TestDeltaRegistrationLifecycle' \
  :catalogs:catalog-lakehouse-generic:spotlessCheck
```

Raw local artifacts are retained under
`build/delta-managed-rest-evidence/task-1/`: exact commands/exits in `commands.json`,
per-run logs, original preexisting module XML (5 commit-evidence tests), setup XML,
behavioral RED XML and focused GREEN XML. The test also writes actual runtime
configuration and client error responses to the module build directory as
`delta-managed-rest-runtime.properties`, copied into the evidence archive.
The combined regression passed **39 lifecycle + 12 REST tests**, with zero failures
or skips, and module formatting passed. Original XML is retained in
`run-9-final-xml/`. The final focused run also passed 12 REST tests and formatting;
its original XML is in `run-10-final-rest-xml/` and its runtime evidence contains the
initialized `jdbc:h2:file:` URL.

Relevant unchanged interfaces are
`core/.../catalog/TableOperationDispatcher.java` (identity injection, managed
persistence and command alter), `core/.../hook/TableHookDispatcher.java` (owner
assignment after create), `core/.../authorization/OwnerManager.java` (raw relation),
`server/.../web/rest/OwnerOperations.java` (metadata existence check),
`core/.../catalog/ManagedSchemaOperations.java` (unguarded cascade), and
`server/.../web/rest/ExceptionHandlers.java` plus
`clients/client-java/.../ErrorHandlers.java` (error mapping).

## Production limits and deployment handoff

Simple auth uses client-selected test names; it is not production OAuth/Kerberos
identity assurance. MATCHED/native identity/columns and writer handoff are explicit
synthetic controls. The suite does not prove native commit correlation, distributed
fencing, cache-enabled operation, or S3 semantics. The raw guard is local to the
test provider and is not distributed expected-ID fencing. Catalog/metalake cascade,
filesets and concurrent cross-server create/drop are outside this claim; fileset
work can precede the provider guard. Native deletion is unavailable. Pending owner
REST repair and provider actor-denial HTTP mapping remain open gaps.

The next local S3 option is the separately inspected pinned image
`minio/minio@sha256:da0fe26595711d0fb93dd28e24552520f68897195f4a23f5a17ffa9924ec3fac`
(RELEASE.2025-07-18T21-56-31Z, ARM64). Actual S3A/Delta validation must retain the
Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 matrix. Container readiness is not such evidence.

The nominated `/Users/jacobo.calderon/Code/Arkham/catalog-demo` at
`3363d9ebf91d0bb894bda92a989fc3dd5bc763ac` runs `apache/ranger:2.7.0` in
`docker-compose.yml:86-98`; its unused `docker/ranger/Dockerfile:1-6` specifies 2.5.0.
Both differ from the required Ranger 2.8.0 target. A separate pinned Ranger 2.8.0
Admin can support the next live-policy experiment. No MinIO, Ranger Admin or
catalog-demo deployment acceptance is claimed by this REST suite. Gravitino remains
the registration authority; Ranger remains responsible for column permissions,
masks and filters.
