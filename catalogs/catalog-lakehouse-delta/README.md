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

# Delta metadata reader and trusted provider policy

`DeltaMetadataReader` eagerly reads authoritative Delta metadata through Kernel's
public snapshot APIs. It exposes latest and retained as-of observations and exact
CREATE/REPLACE commit correlation. It performs no native writes, authorization,
registration-state changes, or publication. Provider registration, distribution,
and ServiceLoader wiring are separate work.

The compatibility boundary is intentionally narrow: `DeltaKernel33MetadataAdapter`
is the only production class using Kernel internals. It accepts matching API and
defaults manifest versions **3.3.0 or 3.3.2**, requires the exact `SnapshotImpl`
class, and copies metadata/protocol after public snapshot construction validates
read support. Upgrades require reviewing this adapter and rerunning both patch
matrices. Read support does not imply Kernel write support. Untyped Kernel failures,
including unsupported reader protocols, produce `SNAPSHOT_UNAVAILABLE`; messages,
locations, SDK causes and suppressed exceptions are not copied into diagnostics.

The presence of any `delta.coordinatedCommits.commitCoordinator-preview`,
`delta.coordinatedCommits.commitCoordinatorConf-preview`, or
`delta.coordinatedCommits.tableConf-preview` configuration key (including empty or
orphan values) yields `UNSUPPORTED_COORDINATED_COMMITS`. These Kernel patches have
no coordinator handler and can otherwise report a stale backfilled snapshot.
Conforming activation is backfilled and visible to this guard; malformed logs
that hide activation itself are outside this guarantee. This guard does not
blanket-reject ordinary writer-only features.

## Metadata and schema boundary

Observations preserve the exact native schema JSON, opaque nonblank table ID,
ordered partition columns, configuration, optional description/creation time, and
reader/writer protocol features. Collections are immutable, including defensive
copies of Gravitino's mutable nested struct arrays. Configuration stays internal:
never log it or copy it wholesale to receipts or public table properties. Its
limits are 4,096 entries, 16 KiB per key, 1 MiB per value, and 16 MiB total UTF-8
key/value bytes.

The Gravitino schema projection covers Kernel's boolean, byte, short, integer,
long, float, double, decimal, binary, date, default-collation string, timestamp,
timestamp-NTZ, variant, struct, array and map types. Both timestamp forms use
precision 6, with and without timezone respectively. Struct-field comments and
nullability, array element nullability, and map value nullability are retained.
Unknown types, empty structs, malformed comments and non-default string collations
fail projection. Raw schema JSON remains authoritative and is not normalized
through the registration marker's smaller JSON envelope.

Column mapping IDs/names, generated/default expressions, identity settings,
type-widening history and arbitrary native field metadata remain in raw JSON.
They are not translated into Gravitino expression semantics: projected defaults
are unset and auto-increment is false. This projection is not a lossless Delta
schema round trip or a claim of complete Spark Delta feature support.

## Checkpoints and exact commit evidence

Discovery and replay are delegated entirely to Kernel. Tests cover native classic
single-file checkpoints after 36 commits with earlier JSON removed, complete and
incomplete multipart V1, V2 UUID JSON/Parquet, classic-named V2 with a sidecar,
later metadata JSON, retained as-of snapshots, and missing/stale/corrupt
`_last_checkpoint`. Kernel requires the JSON delta file for the requested
snapshot's own version, even when that version has a checkpoint. Missing required
history fails closed. Metadata reads do not scan data files or validate sidecar
data-file contents.

Verification reads latest, candidate, and (for REPLACE) the trusted base snapshot
under the storage Subject. It checks base ID lineage, opens only candidate zero
or checked base-plus-one JSON at the durable marker location, and calls the
`:delta:delta-common` action reader. Before returning MATCHED, it also
requires that evidence ID/version and the candidate snapshot marker agree with
the detached native snapshot and expected marker. Missing exact evidence, inconsistent
lineage, superseded candidates, inherited markers or malformed trailing actions
cannot produce MATCHED. Evidence is an observation, not an atomic fence against
external writers or path replacement.

Native table creation, empty commits and classic checkpoints use Kernel's public
transaction/checkpoint APIs in disposable test directories. Multipart/V2 formats,
raw feature metadata and replacement evidence use protocol-shaped unit fixtures.
Kernel 3.3's writer rejects the custom registration-marker property; marker-bearing
CREATE controls explicitly edit only their owned fixture JSON after native table
creation. They are not claimed as native Kernel marker writes or new Spark
qualification.

## Storage lifecycle and validation

Each reader owns a fresh Hadoop UGI Subject and a defensive configuration. Every
read creates an engine inside `doAs`; snapshot replay, exact-action streams and
lazy iterator cleanup complete before results return. Only explicit `file:` and `s3a:` locations are admitted. Their filesystem caches
are enabled in the private configuration; unrelated cache policies (including
HAR) remain unchanged. Closing drains admitted reads and invokes
only `FileSystem.closeAllForUGI(ownedUgi)`. It never globally closes filesystems or
changes global UGI/configuration. Caller Subject and context classloader are
restored on success and failure. Later provider construction must supply only
allowlisted server storage settings and capture authorization outside this scope.

Tracking-filesystem tests establish cache separation, close isolation, concurrent
read draining and scoped lazy checkpoint reads locally. They do not establish
S3/MinIO endpoint/credential isolation, packaged provider acceptance, or Spark
feature conformance; those remain separate acceptance stages. The provider now bundles aligned `hadoop-aws`/S3A and constructs its configuration
from the trusted settings below. Distribution wiring and endpoint-isolation acceptance
remain later integration stages.

Run the module matrix with JDK 17:

```sh
./gradlew :catalogs:catalog-lakehouse-delta:spotlessApply \
  :catalogs:catalog-lakehouse-delta:test \
  :catalogs:catalog-lakehouse-delta:javadoc \
  :catalogs:catalog-lakehouse-delta:jar \
  -PskipITs -PdeltaKernelVersion=3.3.0
```

Repeat with `-PdeltaKernelVersion=3.3.2`. The exact property defaults to 3.3.0 and
rejects other values. Tests assert actual loaded manifest versions; changing the
property changes test inputs and dependency resolution. Hadoop artifacts are
aligned to 3.3.6, including the client API/runtime jars pulled by Kernel defaults. Kernel/Hadoop belong only
to this provider module, Gravitino API is compile-only in production, and the
shared protocol has no Kernel/Hadoop dependency. No Spark/server test jar belongs
on the production runtime classpath.


For v2 name-mapping markers, verification supplies the shared action reader with detached exact
candidate and trusted-base schema/configuration/protocol observations from those same Kernel reads.
The shared [closed schema contract](../../delta/delta-common/README.md#closed-schema-correlation)
validates generated assignments and feature admission while preserving exact action-to-snapshot
schema equality. The per-reader UGI lifecycle and final candidate ID/version/marker checks remain.
V1 verification keeps its original overload and semantics; latest/as-of metadata observations
remain broader than the bounded registration schema contract. The new mapped CREATE/REPLACE tests
are protocol-shaped controls, including fresh replacement IDs and a reduced maximum; they are
not native Kernel marker writes or Spark entrypoint qualification.

## Trusted catalog configuration

These provider-local helpers are the admission boundary consumed by the following
registration and catalog-SPI tasks. They do not yet install a provider or mutate
registration rows. Catalog administrators choose storage policy; table and schema
requests cannot override it. Configuration passed to `DeltaCatalogConfig.from`
must already have secret references resolved by Gravitino's existing secret manager.
Its Hadoop configuration is fresh, allowlisted, defensively copied, and must never
be logged. Unknown configuration keys, arbitrary `fs.*` properties, log-store
settings and caller-selected credential classes are rejected. Shared base catalog
metadata such as authorization provider and package settings remains available.

| Catalog key | Required/default | Meaning |
| --- | --- | --- |
| `delta.warehouse` | Required | Canonical S3 root for new tables |
| `delta.adoption-roots` | `[]` | Strict JSON string array of admitted existing roots |
| `delta.recovery-principals` | `[]` | Strict JSON string array for recovery-specific callers |
| `delta.storage-credential-mode` | `instance` | Exactly `static`, `session`, or `instance` |
| `s3-endpoint` | Unset | Explicit administrator-selected HTTP(S) endpoint |
| `s3-region` | Unset | S3 endpoint region |
| `s3-path-style-access` | `false` | Exactly `true` or `false` |
| `s3-access-key-id` | Mode dependent; hidden | Resolved access key |
| `s3-secret-access-key` | Mode dependent; hidden | Resolved secret key |
| `s3-session-token` | Session mode; hidden | Resolved session token |

All listed settings are immutable, including endpoint, region, path style and
credentials. Static mode requires both keys and uses S3A's
`SimpleAWSCredentialsProvider`. Session mode additionally requires a token and uses
`TemporaryAWSCredentialsProvider`. Instance mode rejects all supplied static/session
credentials and uses `auth.IAMInstanceCredentialsProvider`. No plaintext endpoint
is synthesized. Explicit HTTP is for trusted local/test setups, including
`http://localhost:9000` or `http://minio:9000`; hostname spelling does not prove
network locality. Production administrators should select HTTPS. Endpoint userinfo,
query strings and fragments are forbidden.

An illustrative catalog-create payload fragment uses the existing typed secret
references (the registered provider and locator attributes depend on deployment):

```json
{
  "properties": {
    "delta.warehouse": "s3a://warehouse/tables",
    "delta.adoption-roots": "[\"s3a://warehouse/admitted\"]",
    "delta.recovery-principals": "[\"delta-recovery-operator\"]",
    "delta.storage-credential-mode": "static",
    "s3-endpoint": "https://s3.example.internal",
    "s3-region": "us-east-1",
    "s3-path-style-access": "true"
  },
  "secretReferences": {
    "s3-access-key-id": {
      "provider": "configured-secret-provider",
      "attributes": {"key": "delta-reader-access-key"}
    },
    "s3-secret-access-key": {
      "provider": "configured-secret-provider",
      "attributes": {"key": "delta-reader-secret-key"}
    }
  }
}
```

Do not send raw secret URNs in request properties. Hidden metadata masks credentials;
the following SPI integration must also disable `BaseCatalog.shouldBackfillCredential`
so legacy credential backfill cannot expose resolved values in catalog responses.

Both `s3` and `s3a` inputs normalize once to `s3a`, with lowercase DNS bucket names
and a consistently removed trailing root slash. Ports, userinfo, query/fragment,
percent escapes, empty/dot path segments, traversal and other schemes are rejected.
New locations are `${warehouse}/${schema}/${positiveTableId}`. The schema must
contribute exactly one admitted path segment; safe punctuation such as `sales.v2`,
`-sales`, and `sales~archive`, and well-formed Unicode are preserved as supplied.
Path separators, escapes, dot segments, query/fragment syntax and malformed text
are rejected. Durable schema-name bounds remain a separate storage admission check.
Admission of an existing path requires an exact bucket
and complete path-segment prefix boundary. A configured `/allowed` root cannot
admit `/allowed2`. Empty adoption roots admit nothing. No production local-filesystem
policy flag exists; direct native-reader unit fixtures still use local storage.
Administrators/platform admission must attest location exclusivity and arrange
cutover, serialized request completion and writer drain; root membership is not a
global uniqueness guarantee or a fence against outside writers.

## Authorization and request properties

Registration authorization captures Gravitino's authenticated principal, including
groups, before storage UGI entry. Anonymous or blank actors fail. Privileged paths
require enabled authorization, live internal ACL and owner dispatchers, and a real
service-admin, catalog-owner or metalake-owner decision. The known pass-through
authorizer and its subclasses fail even when the enable flag is true. Custom
administrator-selected authorizers remain inside the trusted server boundary.
Checking another stored actor cannot borrow the current Subject's admin authority.
Recovery-principal configuration alone grants neither ADOPT nor configuration rights.
Existing REST CREATE/MODIFY authorization remains required.

Initial publication requires any valid persisted USER or GROUP owner; ownership
need not equal the operation actor. Pending creator replay requires the exact
current creator and either no persisted owner or that exact USER owner. A GROUP
with the creator's name is insufficient. Missing dispatchers and owner lookup
failures are failures, never evidence of absent ownership. These helpers never set
an owner. Authorization-disabled publication is unsupported.

Schema property requests must be empty. Table property requests contain only
`delta.registration.command`, which is mutable and nonreserved operation transport;
registration operations must consume it without persisting it as an ordinary table
property. `delta.registration.internal.` is reserved and hidden;
`delta.registration.receipt.` is reserved and visible. Strict provider request
validation is necessary because generic metadata helpers ignore unknown properties.
Ordinary property, location, credential and root alterations are rejected.

## Native reduction and durable admission

`DeltaNativeObservation` retains raw schema, ordered partitions, native ID/version,
protocol, optional description and opaque optional creation time (including negative
values). It never retains `DeltaSnapshotMetadata` or its raw configuration. Native
protocol uses shared 4096-feature/8192-byte-name admission; native IDs obey the
receipt's 8192-byte bound without truncation. Native projected nested types are
copied defensively using the existing projection implementation.

| Persisted safe configuration key | Accepted syntax |
| --- | --- |
| `delta.columnMapping.mode` | `none`, `name`, `id` |
| `delta.columnMapping.maxColumnId` | Canonical nonnegative signed-long decimal |
| `delta.appendOnly` | `true`, `false` |
| `delta.enableChangeDataFeed` | `true`, `false` |
| `delta.enableDeletionVectors` | `true`, `false` |

Absent safe keys remain absent; no prefix copying occurs. The full configuration
digest includes every reader-bounded entry. Its value is `sha256-v1:` followed by
64 lowercase hex digits. SHA-256 input begins with UTF-8 bytes of the exact domain
`gravitino.delta.configuration.v1` (no terminator), then entries sorted using Java
`String.compareTo` (UTF-16 lexicographic order). Each key and value contributes an
unsigned nonnegative 32-bit big-endian UTF-8 byte length followed by those bytes.
Malformed UTF-16 is rejected. The versioned digest is durable comparison data, not
a signature. Tests freeze ordering across BMP/non-BMP keys and insertion orders.

A present transient native operation marker must strictly decode as v1 or finalized
v2; malformed markers fail with a sanitized error before any caller should store
ADOPTING. Only a digest of the singleton marker configuration and provenance
`ABSENT`, `VALID_V1`, or `VALID_V2` remain in the observation. Foreign marker tokens
must never be copied into receipts. The full digest still includes the marker.

`DeltaMetadataReader.projectSchema` validates the exact supported runtime and
projects desired command schema with the sole internal adapter/parser, without
I/O, under the existing 128 KiB UTF-8 command limit. Its `copyColumns` facade copies
native observational projections without that command limit; it is not a general
mutable-expression clone.

`DeltaStorageAdmission` measures core's actual `JsonUtils.anyFieldMapper` output,
including default expressions converted with `DTOConverters.toFunctionArg` (unset
stays SQL NULL). Its bounded UTF-8 Writer counts escaping, multibyte characters and
surrogate pairs across writes without constructing unbounded serialized strings.
Direct names allow 128 UTF-16 units, schema comments 256, top-level column comments
4096, and table format 64. Actual NUL and malformed surrogates in direct strings
fail. Nested names/comments are governed by serialized type size, not separate
VARCHAR limits. Serialized types/defaults allow 65535 bytes; table/schema JSON and
audit fields plus direct table comments allow 16777215 bytes per field. These are
portable schema ceilings, not guarantees about deployment packet/memory limits.

The following registration lifecycle must check desired projection before
RESERVE/BEGIN_REPLACE, observed projection before ADOPT, and every complete schema
or table candidate before put/update. Internal state encoding and the final
containing properties map both need checks because embedded schema/markers escape
again. A native 300 KB top-level comment remains observable but is rejected for
durable storage. PUBLISH/refresh must recheck actual observations; unrepresentable
metadata requires a representable full-row-CAS diagnostic with
`UNREPRESENTABLE_NATIVE_METADATA`, retaining the last representable observation and
correlation. If that diagnostic cannot fit, keep the old row and fail closed.
Current tests cover helper admission and final candidate bounds; actual no-row and
quarantine lifecycle assertions belong to the subsequent state-machine task.
