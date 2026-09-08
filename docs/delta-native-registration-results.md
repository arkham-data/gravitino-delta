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

# Delta native registration correlation component

## Delivered boundary

`catalogs/catalog-lakehouse-delta` is a reusable production Java correlation component.
It contains `DeltaOperationMarker`, `DeltaCommitActionReader`, immutable
`DeltaCommitEvidence`, and a package-private JSON helper. It does not advertise a
functioning catalog provider or add a provider SPI. Its runtime dependencies are
Jackson databind/core/annotations 2.15.2. Production classes target Java 8 through
`--release 8`; tests and compilation use the repository's Java 17 toolchain.

The component makes no storage, Spark, Kernel, Hadoop, authorization, publication,
or writer-handoff calls. A MATCHED result is conditional on trusted verifier inputs;
it is not a native recovery acceptance result or an authorization decision.

## Marker contract

The configuration string key is `gravitino.registration.operation`. Canonical compact
JSON fields have this exact order: `version`, `operation`, `id`, `generation`, `token`,
`location`, `schema`, then `baseVersion`, `baseUuid` for REPLACE only. Version is the
integer 1. Identity and generation are nonnegative signed longs. CREATE has no base;
REPLACE has a nonnegative base strictly below `Long.MAX_VALUE` and a nonblank base UUID.

The schema is a JSON object whose keys are recursively sorted. Array/column order,
nullability, metadata, JSON value types and exact decimal precision remain significant.
Formatting whitespace is ignored; string contents are preserved. Decimal parsing uses
BigDecimal without stripping trailing zeros. This binding does not cover arbitrary CTAS
query semantics, row contents, partition configuration or every user property.

Decode requires the exact canonical re-encoding. It rejects duplicate, additional,
missing or mistyped fields, unsupported operations/versions, overflow and trailing
documents. Required token/location/base-UUID text is nonblank and at most 8 KiB UTF-8;
schema input and normalized schema are bounded to 128 KiB; the envelope is bounded to
256 KiB. UTF-8 bounds are counted before allocating encoded byte arrays. Invalid
surrogate strings are rejected. Schema containers have at most 63 nesting levels,
leaving room within the 64-level envelope bound. JSON numbers have at most 1,000
characters. Locations are supplied already normalized by the trusted caller.

`injectInto` makes a new unmodifiable property map, preserves unrelated entries, and
rejects nulls or owned-key collisions, case-insensitively, after stripping every leading
`option.` prefix. Call it after reservation and before native stage creation. The
adapter must also protect `LogicalWriteInfo.options`, ALTER and every other write entry
point; this method cannot police those separate inputs.

## Action correlation

`inspect(expected, candidateVersion, latestVersion, validatedSnapshotVersion,
validatedSchemaJson, commitJson)` consumes caller-owned input without closing it.
It requires candidate == independently observed snapshot version == latest version.
CREATE requires candidate zero. REPLACE requires the actual trusted base plus one,
an integral CommitInfo readVersion equal to that base, and a new candidate UUID.
CREATE allows omitted readVersion or the native sentinel -1; every other value,
including null, strings and fractional numbers, is ambiguous.

Exactly one metadata action and one CommitInfo action must be present. Metadata must
contain a nonblank actual UUID, the exact canonical marker, and a schema equal to both
the expected schema and the independently validated snapshot schema. Exact native
operation allowlists are:

| Marker | Accepted CommitInfo operation |
| --- | --- |
| CREATE | CREATE TABLE; CREATE TABLE AS SELECT; CREATE OR REPLACE TABLE AS SELECT |
| REPLACE | REPLACE TABLE; REPLACE TABLE AS SELECT; CREATE OR REPLACE TABLE AS SELECT |

`CREATE OR REPLACE TABLE` without AS SELECT is deliberately unsupported. Missing
CommitInfo, inherited ALTER/WRITE, superseded candidates and every mismatch remain
AMBIGUOUS. Marker properties in operationParameters are never a fallback.

The reader buffers at most one action, bounded to 1 MiB UTF-8 before parsing, so even
large JSON field names cannot allocate unbounded parser buffers. Actions have at most
a 65,536-token parsing budget, 64 nesting levels, 256 KiB strings and 1,000-character numbers. Repeated
file actions are streamed without collecting the commit; there is no total commit-byte
or action-count limit. Resource-limit exhaustion is AMBIGUOUS.

Every action envelope must have exactly one key with an object value. Metadata,
CommitInfo and protocol are singleton actions; repeated add/remove and other opaque
object-valued actions are consumed. Duplicate fields anywhere, trailing corruption,
non-object envelopes and leading BOMs are rejected. This is not Delta conformance
validation. Kernel remains responsible for protocol, snapshot, file and data validity.
Malformed input drains remaining bytes without interpretation. I/O failure always
returns AMBIGUOUS and can prevent EOF consumption; the caller still owns the stream.

Evidence construction is package-private. Public read-only accessors expose Kind and a
stable bounded diagnostic code. Version/UUID/schema Optionals are present only for a
fully MATCHED result; no partial observation is publishable. There is no NOT_COMMITTED
outcome, public synthetic MATCHED factory or production handoff boolean.

## Trusted integration still required

The enclosing verifier must open the exact candidate JSON exclusively beneath the
durable stored location and obtain version/schema from one successful public Kernel
snapshot observation. Replacement base version/UUID must come from the actual trusted
pre-write observation persisted with BEGIN_REPLACE, including ordinary DML newer than
the last publication. The provider must check that base UUID against published lineage.
Current commit actions cannot establish prior UUID lineage independently; recovery
must use retained base metadata or accepted durable trusted evidence. Missing required
history is ambiguous. Actor authorization, Ranger 2.8.0, reservation/OCC, native writer
handoff, REST publication and quarantine/abort decisions remain outside this module.

The required Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 local-S3 native recovery matrix has
not been exercised by these parser unit tests. All commit fixtures in this module are
synthetic unit inputs, not claims of real native writes or production recovery.

## Executed component evidence

Local evidence is retained under
`build/delta-native-registration-evidence/task-2/` (not committed). Every Gradle attempt
has exact arguments, working directory, JAVA_HOME, timestamps, exit status and log.
The runner removes current module test XML before a test invocation and copies only
newly generated matching XML; setup failures have no copied XML.

The original executed RED is `red-executed-v2`: 31 tests, 31 failures, zero errors or
skips against API-only unsupported stubs. Original XML and the original source snapshot
are preserved. Earlier attempts failed during sandbox access, formatting or compilation;
they are setup failures, not behavioral RED. `green-v1` passed those same 31 tests.
The additional `precision-depth-red` run executed 34 tests with three failures for
schema decimal precision, envelope depth and parse-versus-I/O diagnostics;
`precision-depth-green` passed all 34.

The final covering run `final-green` passed 36 tests (11 marker, 25 reader), with zero
failures, errors or skips, on Homebrew OpenJDK 17.0.16+0. It also covers quoted braces,
escapes, Unicode, raw action-byte limits and action-depth limits. Artifact metadata is
recorded alongside the original RED.
Scoped Spotless, compiler/Error Prone checks, strict Javadoc (`-Xwerror`) and jar
inspection are included. The module exposes no Checkstyle task; none is claimed.
Jar inspection records entries, SHA-256 and Java class major versions and confirms
absence of test, fixture, synthetic, Spark/Kernel/Hadoop or service-provider classes.
No full-repository test run, deployment, push, merge or PR was performed for this task.
