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

# Delta native metadata reader

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
feature conformance; those remain separate acceptance stages. This subcomponent
does not yet bundle `hadoop-aws`/S3A; that connector, credential integration and
distribution wiring belong to the following provider packaging task.

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
