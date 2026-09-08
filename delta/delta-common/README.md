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

# Delta Common

`delta-common` owns the stable
`org.apache.gravitino.catalog.lakehouse.delta.registration` package. It is a small runtime artifact
for the qualified Delta registration protocol and can be shared by server and Spark components
without bringing either runtime into the other.

The artifact contains typed registration commands and receipts, the operation marker wire format,
Delta commit action reader, immutable commit evidence, and their JSON support. Its production dependency is Jackson Databind.
It does not contain a catalog provider, Spark, Delta Kernel, Hadoop, Gravitino core, or test fixtures.


## Closed schema correlation

The original v1 factories, canonical bytes and exact-schema verification remain unchanged.
The v2 `createWithSchemaContract` / `replaceWithSchemaContract` factories accept only
`SPARK_35_DELTA_33_NAME_MAPPING_V1`, mode `SCHEMA_ONLY` or `QUERY`, mapping mode `name`,
and feature policy `BASIC_NAME_MAPPING_V1`. There is no configurable metadata ignore list.
`schemaJson()` returns the canonical required logical schema; `schemaContract()` distinguishes v2.

The contract admits Spark JSON byte, short, integer, long, float, double, boolean, string,
binary, date, timestamp, timestamp_ntz and decimal(p,s), where 1 <= p <= 38 and
0 <= s <= p, recursively within struct/array/map. QUERY input must already be recursively
nullable (fields, array elements and map values, including nested map-key types).
SCHEMA_ONLY preserves ordinary struct NOT NULL and collection flags, but excludes nonnullable
StructFields inside collection element/key/value types. Field names/order/types, decimal scale,
nullability, comments and all ordinary user metadata remain exact, including numeric metadata
precision and scale. Object-key order alone is ignored.

Only integral `delta.columnMapping.id` and native `col-UUID` physical names may be added.
IDs are globally unique integers in [1,2147483647]; full physical paths are unique, although
physical names may repeat under different parents. The canonical nonnegative decimal
`delta.columnMapping.maxColumnId` must cover all IDs and fit the same integer range.
Replacement may reset IDs/names and reduce the maximum. This validates semantic assignments,
not native allocation order or mathematical UUID noncollision across generations.

The exact candidate snapshot schema must still canonically equal the actual same-commit metadata
schema. The context overload takes immutable, bounded `DeltaSchemaSnapshot` and
`DeltaProtocolDescriptor` observations; they do not themselves prove a trusted native read.
The original overload drains v2 input and returns `MISSING_SCHEMA_CONTEXT` rather than downgrading.
CREATE needs same-commit protocol, metadata and commitInfo. REPLACE needs metadata and commitInfo,
a matching actual base version/ID and a changed candidate ID; protocol may be omitted only when
candidate and base effective protocols agree. Operation mode must correlate with an admitted
AS SELECT or schema-only commitInfo operation. Latest/candidate/base-plus-one checks remain.

Only protocol pairs (2,5), (2,6), (2,7) and (3,7) are admitted for v2. Legacy pairs have no feature
fields; writer7 requires writerFeatures with columnMapping, and reader3 requires readerFeatures
with columnMapping. Wire absence, empty fields, duplicates and unknown features are checked
before conversion to sets. Reader features are columnMapping, deletionVectors, timestampNtz,
typeWidening-preview, typeWidening, vacuumProtocolCheck, variantType-preview and v2Checkpoint.
Writer features additionally admit appendOnly, invariants, checkConstraints, changeDataFeed,
generatedColumns, identityColumns, rowTracking, domainMetadata, clustering, allowColumnDefaults,
inCommitTimestamp and checkpointProtection. Reader/writer support must agree; only columnMapping
has implicit reader2 support. rowTracking/clustering require domainMetadata. timestamp_ntz schema
requires reader3/writer7 and timestampNtz in both sets. These are protocol compatibility rules,
not write qualification for all those features. Generic protocol descriptors also represent
legacy reader1/writer1 observations; v2 admission is a separate check.

NullType, UDT, CHAR/VARCHAR markers, intervals, variants, unknown types, negative-scale decimals,
generated/default/identity field semantics and mapping/parquet nested IDs are excluded.
Candidate and trusted base reject coordinatedCommits-preview, redirectReaderWriter-preview,
redirectWriterOnly-preview, icebergCompatV1/V2 features. The three
`delta.coordinatedCommits.*-preview` configuration properties and both redirect properties are
rejected even when empty/orphaned. Iceberg compatibility properties admit only explicit false;
`delta.universalFormat.enabledFormats` admits only empty text when present. Mapping-relevant
action properties must agree with the candidate observation. Unrelated properties are not a
complete property digest and are never logged.

Snapshot schemas retain the 128-KiB UTF-8 verification bound. Configuration is copied eagerly
with at most 4096 entries, 16-KiB keys, 1-MiB values and 16-MiB aggregate UTF-8 data. Protocol
feature sets are copied, nonblank and bounded. Shared production bytecode remains Java 8 and
Jackson-only. Actual Spark native marker qualification is a separate acceptance stage.

## Registration commands and receipts

`DeltaRegistrationCommand` carries one typed operation in the reserved
`delta.registration.command` table property. Its wire version is always `1`; the contained
schema binding is either v1 `schema` (unchanged exact schema JSON) or v2 `schemaContract`
(the closed contract above), never both. Factories return immutable values and `decode` rejects
unknown/duplicate fields, malformed Unicode and noncanonical identity numbers. Object key order
and insignificant whitespace may be normalized. Commands are limited to 512 KiB UTF-8;
tokens to 1024 bytes, run IDs/references to 4096 bytes, and ADOPT locations to 8192 bytes.
Existing schema and marker bounds still apply.

The following shows the protocol's table-create POST body, at
`/api/metalakes/example/catalogs/delta/schemas/default/tables`. The property value is a JSON
**string**, not a nested object. These are wire examples; this shared artifact does not install a
provider or expose an HTTP endpoint.

```json
{
  "name": "events",
  "columns": [],
  "properties": {
    "delta.registration.command": "{\"version\":1,\"operation\":\"RESERVE\",\"token\":\"op-1\",\"runId\":\"run-17\",\"schema\":{\"fields\":[],\"type\":\"struct\"}}"
  }
}
```

After receiving the authoritative identity, a table-alter PUT to the same path plus `/events`
uses one `setProperty` update. For example, publication verification is requested with:

```json
{
  "updates": [
    {
      "@type": "setProperty",
      "property": "delta.registration.command",
      "value": "{\"version\":1,\"operation\":\"PUBLISH\",\"expected\":{\"id\":7,\"generation\":0,\"token\":\"op-1\"}}"
    }
  ]
}
```

A v2 reservation replaces `schema` with this complete binding inside the command string:

```json
{
  "schemaContract": {
    "kind": "SPARK_35_DELTA_33_NAME_MAPPING_V1",
    "operationMode": "SCHEMA_ONLY",
    "requiredSchema": {"fields": [], "type": "struct"},
    "columnMappingMode": "name",
    "featurePolicy": "BASIC_NAME_MAPPING_V1"
  }
}
```

RESERVE carries only token, run ID and schema binding. The server chooses its location,
registration ID/generation and CREATE marker. BEGIN_REPLACE carries the expected identity,
replacement token, run ID and schema binding; only the server captures the native base.
ADOPT carries token, location, retired writer run (`runId`) and cutover reference (`reference`),
without exposing a foreign native marker. PUBLISH, STATUS and ABORT carry only `expected`.
HANDOFF also carries the stored run ID and reference. Read-only STATUS_BY_TOKEN carries exactly
the current token and recorded writer run (the retired writer run for ADOPT), without numeric
identity. ADMINISTRATIVE_STATUS carries neither expected identity nor token and requires
privileged server authorization. Correlation tokens are **not bearer authority**; these value
objects do not authenticate, authorize, implement CAS or prove a writer has retired.

`DeltaRegistrationReceipt.fromProperties` reads the visible reserved
`delta.registration.receipt.` prefix, ignores unrelated properties and rejects unknown suffixes
or malformed reserved values. `toProperties()` returns only immutable sanitized receipt fields.
The operation is the opening RESERVE, ADOPT or BEGIN_REPLACE, even after a PUBLISH or STATUS
request. For example, a verified CREATE projects:

```json
{
  "delta.registration.receipt.id": "7",
  "delta.registration.receipt.generation": "0",
  "delta.registration.receipt.token": "op-1",
  "delta.registration.receipt.operation": "RESERVE",
  "delta.registration.receipt.state": "PUBLISHED",
  "delta.registration.receipt.runId": "run-17",
  "delta.registration.receipt.evidence": "EXACT_NATIVE_COMMIT",
  "delta.registration.receipt.reason": "NONE",
  "delta.registration.receipt.location": "s3://warehouse/events",
  "delta.registration.receipt.nativeId": "opaque-native-id",
  "delta.registration.receipt.nativeVersion": "0"
}
```

Only PUBLISHED exposes location/native ID/version and represents a readable table. RESERVED
and REPLACING expose a matching server CREATE/REPLACE `marker` and evidence NONE; their pending
location comes from that marker. ADOPTING has no marker. Pending receipts use reason NONE;
RESERVED/ADOPTING may instead report OWNER_INITIALIZATION_REQUIRED. QUARANTINED with UNRESOLVED
evidence preserves its pending native marker (none for ADOPT); quarantine after a published
refresh retains the prior evidence category with no marker or native tuple. ABANDONED has no
marker or native tuple and uses UNRESOLVED or the actually observed evidence category. Exact
terminal evidence is historical and confers no readability or recovery authority.

ADOPT publication evidence is EXISTING_NATIVE_SNAPSHOT; native CREATE/REPLACE publication evidence
is EXACT_NATIVE_COMMIT. QUARANTINED accepts only NATIVE_EVIDENCE_AMBIGUOUS, NATIVE_READ_UNAVAILABLE,
ADOPTION_BASELINE_CHANGED, NATIVE_ID_CHANGED, NATIVE_VERSION_REGRESSION,
NATIVE_METADATA_INCONSISTENT or UNREPRESENTABLE_NATIVE_METADATA as reasons. ABANDONED requires
ABANDONED_BY_RECOVERY. These are closed server-generated codes; raw exceptions, foreign markers
and full native configuration must not be projected into receipts.
