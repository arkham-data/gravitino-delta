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

# Shared Spark Hub JDBC named-writer qualification

The bounded shared JDBC route passed 27 original service cases and 31 actual
wrapper jobs across Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2. The jobs used the
actual Hub JDBC v2 source, `job_wrapper(..., gravitino_runtime=...)`,
`SparkClient`, progress tracker and named `write_dataset`. Java 17,
Scala 2.12.21, actual Ranger server/client 2.8.0, Gravitino, MinIO and a
stationary read-only PostgreSQL source were independently checked.

This qualifies the explicit profile and finite cases below. Full M2, complete
schema acceptance, legacy raw-path equivalence, overwrite/full refresh,
default-enabled maintenance, arbitrary sources/transforms, uploads,
distributed execution and production deployment remain open.

## Original current evidence

Current production is Gravitino `a9b98e16df0c1b0a73e0bdb256fd0adcc3d0cc1e`
and immutable Hub `8af43fa12c01394c32f1fef6196a3d384bd604e3` (259 production
files). All four packages captured the same 9,254 source files with digest
`677238b217cdc2b36ba47397c990b02f767356a6e13af59c0f8acb186d22aa06`.
Source files/digests match before and after builds; the before snapshot alone
also records Git HEAD. Every case binds its actual child producer, runtime,
caller/process identities and helper source to its original manifest.

| Spark / Delta | Cases / jobs | New publications | Confirmed data | Original checked cell (SHA-256) |
| --- | ---: | ---: | ---: | --- |
| 3.5.4/3.3.0 | 5 / 6 | 5 | 4 | [task-5-ready-live-3.5.4-3.3.0](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-ready-live-3.5.4-3.3.0/checked-result.json) (`627ea9d1ba468511fe29b1582fdd851174a735f9f5cec31abb84fe6f9d1b1d06`) |
| 3.5.4/3.3.2 | 5 / 6 | 5 | 4 | [task-5-ready-live-3.5.4-3.3.2](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-ready-live-3.5.4-3.3.2/checked-result.json) (`9158ab7c979450df7bc3767b636928b2a5460bc4b4fda18079bd526afab9bfce`) |
| 3.5.6/3.3.0 | 5 / 6 | 5 | 4 | [task-5-ready-live-3.5.6-3.3.0](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-ready-live-3.5.6-3.3.0/checked-result.json) (`73eb0240a17f3cca6621a3927ce5bffc8bccc8086b200178192d02fe771cd512`) |
| 3.5.6/3.3.2 | 12 / 13 | 10 | 7 | [task-5-ready-live-3.5.6-3.3.2](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-ready-live-3.5.6-3.3.2/checked-result.json) (`7a99339a64089202c9262e9d69e0c35be7a41355082ad5b7c780e999234253d9`) |

The retained aggregate is
[task-5-ready-matrix.json](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-ready-matrix.json).
It verifies exactly 27 cases, 31 jobs, 25 new native publications and 19
confirmed data outcomes. Fixture seed commits are excluded from publication
counts. All four original supervisors and owned fixtures exited 0 without
timeout. PostgreSQL containers stopped with their volumes and evidence
preserved. No original target was replayed to repair evidence.

## Frozen inventory and observations

Every pair ran `shared_create_append`, `shared_existing_append`,
`shared_additive_append`, `shared_merge_schema_false` and
`shared_update_denied`. The additive case has two distinct jobs: schema
addition and an ordinary matching-schema next append. Lead 3.5.6/3.3.2 also
ran `shared_create_denied`, `shared_alter_denied`, `shared_metadata_denied`,
`shared_cdf_enable_append`, `shared_late_target_revoke`,
`shared_postcommit_hook_failure` and `shared_tracker_failure`.

The checker freezes each job's SQL, call counts, phase states and expected
native publication before execution. Each job invokes the source once, issues
two source plans, performs one remote count and one real tracker count, and
calls the writer once. Original PostgreSQL traffic has exactly three schema
queries (two empty-schema probes plus one driver catalog query), one remote
COUNT, one tracker payload query and, only for admitted data actions, one
writer payload query. There are no source mutations. Native data-writer entry
and actual publication are checked separately.

All native JSON and CRC versions, reopened rows/schema/properties, registration
and persistent Gravitino column identities are checked. Schema addition keeps
the native UUID and old column IDs; the seed row receives null for the new
scalar field. Row counts advance 1 → 4 → 7 across the two distinct append jobs,
and the next ordinary append proves checksum continuation after evolution.
Denied phases produce no next JSON or CRC. Successful CREATE remains visible
as native version 0 when a subsequent data publication is denied. Hook and
tracker failures after writing preserve confirmed data; exceptions never
imply rollback or authorize automatic retry.

The actual tracker and writer remain intact. A passive observer waits for the
real tracker count before writer entry to make the frozen counts reproducible.
Both threads share the original session/UGI/groups; each job has a fresh owned
reader, retained through writing and closed after the tracker joins. Only the
external telemetry boundary is fault-injected after confirmed writer return.
This does not establish arbitrary scheduling or an atomic source snapshot
across remote COUNT, tracker COUNT and write.

## Qualified profile and authority boundary

Settings are established before Spark exists. The profile explicitly disables
optimizeWrite, autoCompact, autoMerge and symlink manifests while preserving
ordinary Hub AQE=true settings. It uses no transaction identity, snapshot
partition override, session repair, post-start reset or source renewal. One
writer owns each dataset. See the Hub guide `docs/guides/gravitino-catalog.md`
for trusted runtime wiring and the complete explicit profile.

The original query declares PostgreSQL VARCHAR normalization:
`SELECT id, CAST(secret AS TEXT) AS secret, region FROM source_schema.source`.
The additive query declares the same CAST and an integer scalar addition.
Spark's reserved CHAR/VARCHAR descriptor is intentionally rejected by both
writer and native guards. Native CHAR/VARCHAR and full schema support remain
unqualified; no guard is weakened or issued plan rewritten.

Database credentials authorize the external source; it is not represented as
a Ranger-governed table. Destination access uses real Ranger. Application-owned
CREATE produces the actual Gravitino-managed owner policy. CREATE_TABLE alone
does not supply UPDATE for the following append: deployments need suitable
ownership or preconfigured authority and effective policy publication. The
separate metadata principal does not replace application visibility or access.
The test gateway transparently forwards exact actor table lookups without
counting them as the separately authenticated metadata witness.

Denials use exact-resource Ranger-authored priority-1 override policies, with
exact actor role/access and preserved managed owner allows, role membership
and unrelated policies. They prove Ranger destination enforcement, not
Gravitino public-revoke pushdown. The late denial is armed at real saveAsTable
after CREATE, then waits for the acknowledged actual policy download/version
before releasing data publication. Staged files and successful CREATE are
retained; absence of a data commit is not rollback.

## Prerequisites and retained failures

The original CDF/AQE attempt published metadata JSON with CDF=true but failed
completion before CRC and before data writing. The reviewed prerequisite
`ac23182af453bd6cb81c54c602bd71ae38c2d330` preserves AQE exchanges only for
the exact active CDF native owner, retaining validation and the outer guard.
Its original lead controls and six focused remaining-pair invocations are
retained; the current service CDF case passed on a fresh target.

A later fresh CREATE exposed a self-triggered managed-owner policy publication
gap before native admission. Its original target remained absent, with empty
JSON/CRC and ordinary registration 404; CREATE was conservatively unknown.
The reviewed readiness implementation and fix are included in current
`a9b98e16`. They observe one pending local publication handoff for at most one
second, bind its exact completed publication, then perform normal literal
CREATE authorization. They grant no authority, request no extra download,
retry no denial and replay no native work. The budget does not hard-preempt
preexisting monitor acquisition, backend cleanup or arbitrary callbacks.
Later policy/role transitions can still fail closed with conservative unknown
outcomes; this is not general refresh atomicity or a retry guarantee. The
experimental test-only RESERVE-response barrier was archived and removed;
no service run used it. All current CREATE positives were autonomous.

Two new readiness selectors ran on each remaining pair: the retained JDBC
CREATE gap positive and completed-publication denial, six invocations total.
All six passed with fresh XML and original native proof. Earlier 88 affected
installed passes plus two bridge controls remain bound to their original
sources; they were not rerun or relabeled current. Original Task4 native5
remains producer40c, not current8af proof. The known legacy
`coldDuplicateExecutesNativeAggregate` failure before the changed entry point
is unresolved and not baseline-proven; no full-suite claim is made.

Other original failures remain retained: stale ports, duplicate schema
securable, startup precedence, reserved VARCHAR metadata, test gateway actor
lookup, owner DTO casing, normal-priority duplicate policy rejection and
Ranger canonical deny JSON. The VARCHAR failure's original frame schema was
not captured; its diagnosis is explicitly static, not restored observation.
Earlier eight successful cases and the partial CDF attempt remain historical
and are excluded from this current 27-case aggregate.

The CDF prerequisite's test helper does not apply remove actions while checking
old rows. Independent original native logs contain no removes; this service
verifier additionally checks actual reopened state and JSON actions. That
helper limitation is retained rather than silently presented as broader proof.

Checker negative controls cover missing/extra/duplicate cases, producer/runtime
and process identities, forged caller claims, wrong denial cause, missing or
forged late witnesses, phase/publication mismatch, checksum residue, mixed
production and retry relabeling. Exact RED/GREEN commands, original failures,
XML, source/artifact hashes, service version binding and scope warnings are in
[task-5-report.md](../../../.superpowers/sdd/2026-11-06-spark-hub-shared-writer/task-5-report.md).
Private connection files are identified by path/mode only, never by
credential-derived hashes or whole environment/container dumps.
