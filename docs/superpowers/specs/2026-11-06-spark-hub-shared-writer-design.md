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

# Spark Hub shared JDBC ingestion and named write integration

Status: user approved this critical path with “Proceed” after explicitly parking
manual uploads. The approved deliverable is actual JDBC ingestion through
`job_wrapper -> write_dataset`, creating or appending to Gravitino-registered
Delta with additive schema evolution and Ranger enforcement. The concrete task
plan implements that direction; it does not reopen an approval cycle for the
same scope. Full overwrite/refresh follows this slice.

## Goal and boundaries

Use the actual shared runner, actual JDBC v2 snapshot source, real tracker and a
named writer service. Reuse existing native CREATE, CDF property mutation and
APPEND instead of implementing another storage/catalog engine. A direct-source
fixture bypassing job_wrapper cannot close this deliverable.

- Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Java 17, Scala 2.12.21, actual Ranger server/client 2.8.0 and S3A/MinIO.
- Gravitino owns registration; Spark/Delta own execution and native transactions. No Gravitino core/API/server/client/storage-schema changes or engine forks.
- No Glue compatibility, raw-path destination fallback, arbitrary DataFrame trust or collected-row reconstruction. Named input policies and destination permissions remain unchanged.
- One writer per dataset. No automatic retry of failed, committed or unknown writes; no new durable retry store.
- Keep closed milestones/evidence intact. Only changed and affected tests run; native writes are never replayed to repair evidence. Reuse byte-identical artifacts and APFS clones, serialize heavy work and check capacity.
- The initial opt-in profile explicitly selects already-qualified APPEND settings (optimizeWrite=false, autoCompact=false); never silently change user/model defaults to pass. Unsupported effective settings/modes fail before target side effects. Full default-profile M2 remains open.
- Existing legacy routing stays unchanged when the trusted runtime dependency is absent. Governed routing never falls back to it on failure.

## Concrete gaps found

1. Shared job_wrapper only constructs legacy SparkClient/DeltaClient and lacks a
   governed resource lifetime. Tracker callback failures currently escape only on
   its daemon thread; cleanup must join before closing the JDBC scope.
2. Named table load turns missing registration and operational/authorization
   failures into denial. The app needs a finite, visibility-checked metadata API;
   `except Exception: return False` and requiring CREATE on every existing write
   are unacceptable substitutes.
3. CREATE currently requires enabled maintenance settings, whereas external JDBC
   APPEND is qualified with both disabled. Permit the explicit existing APPEND
   profile for schema-only CREATE, without toggling session configuration.
4. Owned CREATE/CDF ALTER may advance lastCommitVersionInSession; only owned
   APPEND currently updates a participating external scope. Prove the mismatch
   with a real retained source and add narrow successful-operation witnesses.
5. Writer options/additive evolution are implemented but have no actual shared
   Hub/Ranger-server/MinIO acceptance yet. This slice supplies it.

## Public metadata contract

Add `GravitinoDeltaTable.metadata(spark, name)` to the classic companion. It
returns None only for typed table absence positively observed through the normal
application Gravitino catalog. Return detached schema, identity partition names
and only these present native properties: delta.enableChangeDataFeed, primary_key,
delta.logRetentionDuration, delta.deletedFileRetentionDuration, and
delta.compatibility.symlinkFormatManifest.enabled (for compatibility preflight). No raw location,
credentials, min/max statistics, payload rows, native handles or reusable authority
are exposed. Missing catalog/schema, denied visibility, unavailable metadata,
unpublished/replaced registration and invalid native identity all raise.

Require canonical three-part name, exact active session/current subject and
normal Gravitino metadata visibility first. Separate privileged metadata readers
must not disclose otherwise invisible tables. Existing published-registration /
native validation then supplies the schema/property snapshot, with unchanged
observation before/after. Do not require payload SELECT just to inspect metadata;
normal create/modify/owner visibility semantics remain intact. Metadata result
mutation grants no authority; every later operation obtains fresh checks.

## Compose source, CREATE and CDF enablement

Retain schema-only CREATE0 -> registration publication0 -> data WRITE1. A failed
write may leave a valid empty created table; never delete or recreate it as cleanup.
Only create after positive absence, using the exact stamped input schema, identity
partitions, description, primary_key and existing retention/CDF defaults. CREATE
requires schema CREATE; existing APPEND must not additionally require CREATE.

Extend existing create facade with optional `source` for composition; add a finite
`enableChangeDataFeed(spark,name,source=None)` facade using the existing owned
canonical property mutation. Source is the original same-session admitted
DataFrame; it never authorizes arbitrary RDDs or supplies a transaction callback.
Capture exact participating external scopes before invoking the owned metadata
operation. Advance only those scopes' exact configuration snapshot after verified
native commit and registration publication, same session/subject/target/operation.
No recapture, session reset, lifetime renewal, policy bypass, all-scopes broadcast
or acceptance after failed/unknown publication. Existing source-free APIs retain
behavior. A positive metadata-only no-op does not mint a mutation witness.

CDF enablement remains mandatory: new tables start true; existing false/absent
property uses existing ALTER authorization. Existing true requires no new ALTER.
The data write still requires UPDATE, and additive schema changes require ALTER.
The explicit new create/append profile also sets symlinkFormatManifest.enabled=false;
it is a new qualified profile, not an implicit override of enabled legacy defaults.
Preserve all existing enabled CREATE profiles; do not alter an existing table's
manifest property to force compatibility. Unsupported enabled existing hooks reject.
No broader ALTER-property or maintenance service is added.

## Shared writer semantics

A new `GravitinoDatasetWriter` reuses UUID-to-name bindings and exposes the same
write_dataset argument signature as the shared caller. It does not inherit the
AWS/Glue DeltaClient. Preserve loaded-at behavior exactly: add __arkham_loaded_at
when both timestamp columns are absent, or when the target retains that legacy
column and the source only has _arkham_loaded_at. Use native current_timestamp.

Use existing native named `df.write.format("delta").options(**options).mode("append").saveAsTable(name)`;
confirm on the real runtime, do not invent writeTo support or replace by positional
insertInto. Forward actual ExecutionProperties.to_df_properties output so native
PySpark preserves existing userMetadata string conversion and explicit false.
For an existing target, project source columns into the existing target field order
followed by new source fields, preserving every name and value. This explicit
by-name application projection satisfies the connector prefix contract; it does
not broaden connector reorder admission or fill missing columns silently.
Cover nullable scalar additions with mergeSchema=true, explicit false refusal,
UPDATE-without-ALTER refusal, old rows null-filled and refreshed registration.
No nested/type-changing/reordering evolution is claimed by additive success.

Validate output binding, same session, explicit APPEND/SNAPSHOT and supported
non-preview/non-scheduled execution profile before target effects. Reject default
OVERWRITE, refresh, MERGE, incremental cursor reads, generated date partitions,
unsupported source format/options and hash-dedup requests explicitly. Do not
silently drop requested options or turn an unsupported mode into APPEND.
Ordinary schema-only creation defaults are retained (CDF true, log retention45d,
deleted-file retention364d); no symlink/Glue export is added to the new route.
Any unqualified native property/hook combination must reject before mutation.

## Runner and resource lifetime

Add a keyword-only concrete GravitinoJobRuntime dependency to job_wrapper. The
trusted application supplies profile/bindings/JDBC lifetime, never job payload
extras or generic construction callbacks. Each invocation creates fresh reader
and writer after Spark initialization. The runtime config may be reused; the
closed reader may not. Do not stop shared Spark sessions during cleanup.

Actual source -> tracker -> validation -> writer -> tracker join/check ->
post_write_hook remains the governed flow. Capture tracker failures for the job
thread and check before writing when already available. A later tracker failure
cannot undo a committed write. Stop/join and unpersist on all paths, then close
reader. Preserve the primary failure if cleanup also fails; do not silently
suppress cleanup-only failures. Hook executes once only after data write and
successful tracker completion. No data-write retry on hook/tracker/cleanup failure.
Legacy routing/exception behavior remains unchanged except opt-in tracker error
propagation exercised by the governed route.

Track phase outcomes separately: creation/CDF alteration/data write. Before a
phase is invoked it is not attempted; normal completion is confirmed; an exception
during a mutating phase is unknown unless an existing owned result positively
classifies it. Returned data-write success followed by hook/tracker failure stays
committed in the propagated governed error. Never claim rollback from an exception
or infer ownership from a later table version alone. Preserve original cause.

## Acceptance and completion

First implement metadata and native source/metadata-operation composition, then
shared writer and runner. Independent task review follows each. Real installed
wrapper evidence must show exact production call path and concurrent tracker
identity; mocks only support unit tests. Actual service cases run all four pinned
pairs, with new-table create+append, existing append, schema-additive append,
explicit-false rejection, create/write/ALTER denial and provider registration
readback; lead pair adds CDF-off transition, denied metadata visibility, lifecycle
and postcommit fault cases. Retain native UUID/schema/version/row/property outcomes
and no-replay evidence. The plan freezes exact cases before the live run.

This closes the explicitly qualified shared JDBC create/append/evolution route,
not full M2. Next is governed overwrite/full refresh, required transformations
for selected real pipelines and full schema/default-profile acceptance. Uploads,
SFTP, general constructed inputs, streaming, deployment and catalog-demo remain
parked or separately planned. Do not turn those into prerequisites for this path.
