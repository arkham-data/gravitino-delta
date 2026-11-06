<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Owned manual-upload input for governed Delta

Status: proposed next input capability; discovery complete and independent builder
check found no important unanswered planning questions. Awaiting design approval.
No implementation or new runtime qualification is claimed. External JDBC remains
closed at Gravitino `ebfdfbbad` and Spark Hub `5feaa5d1`.

## Intent

Enable the actual Spark Hub manual-upload CSV/XLS/XLSX source to produce a native
Spark file input usable by existing named Delta APPEND. Preserve current upload
parsing, column types, nulls, normalization, timestamps and schema-protection
errors. Reuse native Spark file execution and the existing Gravitino/Ranger
companion instead of admitting arbitrary constructed Spark DataFrames.

This implements the next bounded input capability in the accepted adoption
roadmap. It does not migrate the complete job wrapper or change default refresh
behavior. Existing JDBC, creation, CDF and append milestones remain closed.

## Binding constraints

- Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21.
- Actual Ranger server and client 2.8.0; Gravitino-owned destination registration;
  S3A/MinIO destination qualification on all four version pairs.
- No Gravitino core/API/server/storage-model changes, engine forks, Glue
  compatibility, manual Delta transactions or raw-path destinations.
- Trusted application/runtime and extraction credentials; no arbitrary-code
  sandbox claim. Every named source retains its original Ranger SELECT, column,
  filter and mask obligations. Destination authorization remains unchanged.
- No arbitrary DataFrame/RDD adoption, copied-tag trust, or collecting named
  governed rows and reconstructing them as external input.
- One writer per destination. Preserve native commit outcomes; never replay a
  committed or uncertain write automatically.
- Retain original evidence. Reuse exact unchanged runtime artifacts and APFS
  clones for immutable copies; serialize builds/live tests and check free space.

## Caller and engine evidence

The pinned Hub source is `src/data_platform/sources/manual_uploads/v1/main.py`
at `5feaa5d1`: download at line 179; pandas CSV/XLS/XLSX parsing at 185–191;
header/type normalization at 36–100; explicit Spark schema/null conversion at
102–136; column normalization and current_timestamp at 225–233; existing-target
schema guard at 255–328; count at 335; input download cleanup at 342.
`tests/unit/sources/manual_uploads/v1/test_upload_regressions.py:90–113` requires
session-timezone interpretation of naive timestamps, independent of driver TZ.

Generic SFTP also needs multi-file alignment/Union and is a follow-on. Native
Spark CSV is currently used by legacy Petco, not by this generic upload path.
The shared `job_runner.py` still constructs the legacy catalog and writes through
its raw-path sink; this input milestone cannot close that migration.

Existing `DeltaRangerNativeProvenance` and `DeltaRangerPhysicalProvenance` bind
HadoopFsRelation, FileIndex, FileFormat, schema/options and FileSourceScanExec.
Stock FileSourceStrategy and captured ordinary/APPEND/AQE execution are reusable.
JDBC's special RowDataSourceScanExec continuation must not become a generic
file/RDD admission mechanism. Pinned Spark FileSourceScanExec builds its reader
using Hadoop configuration lazily; SQLConf freezing alone is insufficient.
Detailed source references are retained in this milestone's discovery reports.

## Approaches

1. **Owned typed Parquet staging — recommended engineering choice.** Keep the
   current pandas readers/normalizers, serialize their result with explicit types
   to a private stage, and let Spark read it natively. Reuses existing file-plan
   validation. Costs an explicit PyArrow dependency, staging I/O and lifecycle.
2. **Owned constructed-row issuer.** Avoids staging I/O but introduces a new
   LocalRelation/LogicalRDD/Arrow provenance boundary and mutable-row handling.
   This is substantially more custom connector work and is deferred.
3. **Replace parsing with Spark CSV or introduce a Hive table.** Spark CSV changes
   current upload semantics and cannot parse Excel; Hive introduces registrations
   and services for transient extraction files. Neither fits this bounded task.

Fileset/GVFS remains available for future managed file storage, but does not by
itself create a relational Ranger row/column-policy receipt. It is not required
for the first private local stage. These choices are proposals, not assertions
that the user already selected a new staging architecture.

## Source boundary and stage ownership

Use a separately enabled, job-owned Parquet scope with a thin classic PySpark
binding. The trusted application constructs it after Spark setup; job payloads
cannot select a trusted implementation, grant source authority or supply a
preexisting Spark DataFrame. The first profile uses private local staging under
local Spark; source download and Delta destination may be S3/MinIO. A local
stage is not a distributed-executor deployment contract.

The scope creates its own private directory and opaque stage identity. The
concrete upload adapter may write one ordinary, nonpartitioned Parquet file for
that stage, then seal it. Sealing validates its exact expected schema and regular
file identity and constructs the stock Spark relation before ordinary analysis.
Do not provide a public adopt-existing-DataFrame or arbitrary-existing-path API.
Reject symlinks, path escape, globbing, recursive lookup, custom formats/filesystems,
partition discovery, Parquet schema merging and streaming metadata indices.
The caller cannot change an issued schema/options/definition through a mutable map.

External upload access is authorized by its upstream S3 extraction identity and
private stage ownership, not an invented Ranger table. Registered source data
must continue through named access. Deployment credentials/routing must prevent
using external extraction to bypass a governed dataset; this adapter does not
infer source-policy lineage from arbitrary file contents.

The source retains exact native relation/index/format identity, complete options,
schema and sealed file membership. Keep a private in-memory content fingerprint
of the sealed file and validate it with metadata at action and precommit
boundaries; do not emit data or credential fingerprints into diagnostics. This
adds bounded I/O, to be measured and documented without a performance claim.
Changes, removal, replacement or refreshed membership fail closed. Checks do
not promise atomic cancellation or protection against arbitrary hostile driver
code modifying/restoring files between checks. The owned adapter never rewrites
a sealed stage; no cross-storage transaction or general snapshot service is added.

Freeze exact session/subject/groups, effective SQL configuration and effective
Hadoop configuration/credential context used by stock file readers. Compare
sensitive configuration privately; never log or hash credentials into evidence.
The first profile uses the existing static credential/runtime setup. Mutation
requires a fresh scope; do not invent a broad credential-refresh callback.
Use existing native initialization before capture and only the already-proved
owned lastCommitVersionInSession progression after successful named publication.

Lifetime is explicit, monotonic and finite (1–86400 seconds; 1200 in fixtures),
without renewal. Check close/expiry before reads and existing precommit checks.
The job owns the scope through source count, background tracker and write.
The downloaded CSV/Excel can be removed after staging; the Parquet stage must
survive source return. Close invalidates new work immediately. Delete only owned
stages after active native actions release them; TTL makes abandoned scopes
invalid but must not delete files still used by a live action. Abrupt process
termination may leave a private directory; report this limit without a global
cleanup daemon or broad temporary-directory deletion.

## Preserve the upload contract

Reuse current pandas CSV/XLS/XLSX reader options and header/dtype normalization.
Move shared pure conversion helpers only when needed to avoid divergence. Do not
replace pandas inference with Spark CSV, silently skip malformed rows or change
fallback behavior for the legacy route.

The finite upload schema is the existing nullable BooleanType, LongType,
DoubleType, StringType and TimestampType mapping, with original field order and
names. Preserve text identifiers, mixed textual columns, SQL nulls versus NaN,
all-null string columns, Unicode and microsecond precision. Do not serialize the
pandas index as a field. Unsupported/nested/decimal/binary profiles are not added
incidentally; use the existing upload conversion outcome as the differential
oracle, and explicitly document any rejected case rather than coercing it silently.

Naive Excel/pandas timestamps must retain the existing Spark session-zone epoch
semantics in UTC and America/Mexico_City, with driver TZ varied. Explicitly type
the Parquet timestamp and Spark reader schema; do not infer TimestampNTZ as an
accidental replacement. Use stock conversion utilities where they preserve the
pinned behavior. Ambiguous/nonexistent local times and out-of-range precision
need differential tests and equal failure behavior; no guessed timezone rule.

Declare PyArrow explicitly for the opt-in upload adapter; 18.1.0 is available in
the retained pinned runtime and is the initial qualification candidate with pandas
2.2.3. The normal Hub virtualenv has different Spark/pandas pins and no Arrow, so
it is not qualification evidence. Missing dependencies fail before source issuance;
no fallback to unissued createDataFrame is allowed in governed mode.

Keep actual column normalization, duplicate-header handling and current_timestamp
as existing Spark operations. For overwrite_schema=false, resolve the bound
existing destination by the named Gravitino catalog to obtain schema; never call
DeltaTable.forPath in governed mode. Missing binding or authorization failure is
not a first-upload signal. Preserve column-set checks and lossless cast validation
with Filter/Limit/count. No implicit schema replacement or CREATE is authorized.

The source function takes an explicit trusted adapter dependency and retains its
legacy path when absent, following the JDBC integration pattern. API/class names
and minimal helper placement are implementation choices. The owned stage writer
accepts data only from the concrete upload parser lifecycle, never arbitrary Spark
DataFrames or a user-selected callback.

## Native operations and policy checks

First cover one owned upload source with Project/Alias/Filter/literal Limit,
bounded count, supported casts and direct current_timestamp. Qualify both existing
named APPEND writers. Preserve native Parquet column pruning/filter pushdown,
vectorization and caller AQE within the existing qualified runtime profile;
unsupported settings reject explicitly, without silently disabling them.

Retain exact source obligations after pruning or an optimized-empty plan. Mixed
one-upload plus one named Delta INNER join is a security control: all original
named SELECT demands/masks/filters remain, including hidden predicate/join fields.
Target UPDATE denial, source-scope close/expiry and target revocation at the
existing precommit boundary must prevent a commit. Preserve native target-cache
refresh and the current distinction between precommit failure and postcommit
observation failure. No change to destination registration/transaction semantics.

## Acceptance and implementation sequence

1. Prove pandas-to-Parquet-to-stock-Spark equivalence against the current upload
   constructor on the pinned lead pair, using actual CSV/XLS/XLSX input. Cover
   the finite types, parsing failures, nulls, header cases and timezone/failure
   cases. A mismatch blocks routing, not an excuse to weaken the contract.
2. Implement the private owned stage/file issuer and native source inventory,
   logical/physical binding and scope checks. Add meaningful ownership and
   mutation negatives, including Hadoop configuration, cloned relations/tags,
   unissued ordinary Parquet and arbitrary RDD/DataFrame sources.
3. Wire the actual Hub upload parser, named schema guard and actual tracker to
   the owned reader and existing named APPEND. Prove stage survival after source
   return, overlapping count/write, close/failure cleanup and no raw fallback.
4. Qualify affected installed tests on all four pins and bounded fresh actual
   Gravitino/Ranger-2.8.0/MinIO source/destination cases, including both writers,
   policy denials, late lifecycle/target revocation and native outcomes. Freeze
   the finite case inventory in the implementation plan before executing it.
   Review each task and the complete slice independently before milestone push.

Tests extend existing upload regressions, external-scope ownership patterns,
ordinary file provenance negatives and affected read/APPEND/cache controls.
Do not rerun unchanged registered/external JDBC service matrices. Runtime evidence
must identify actual producing artifacts and all original failed/committed outcomes.
Use selected Docker inspection and safe identity projections already implemented;
no full environment dumps or credential-derived evidence. Preserve original records.

## Explicit exclusions and remaining work

No full job_wrapper/write_dataset cutover, payload CREATE/CTAS, overwrite/full
refresh, general schema evolution closure, SFTP multi-file Union, generic API-row
sources, general Union/window/dedup/UDF/checkpoint support, streaming, distributed
stage storage, dynamic credentials, deployment or catalog-demo integration.
The full shared sink remains open in the coverage register. This milestone's
claim is the real upload source and its schema-protection/count/tracker path feeding
an existing named APPEND destination under its documented local stage profile.
