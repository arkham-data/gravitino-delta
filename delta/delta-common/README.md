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

The artifact currently contains the operation marker wire format, Delta commit action reader,
immutable commit evidence, and their JSON support. Its production dependency is Jackson Databind.
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
