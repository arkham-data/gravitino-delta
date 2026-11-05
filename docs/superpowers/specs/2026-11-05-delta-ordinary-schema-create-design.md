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

# Ordinary governed Delta schema creation

## Outcome and scope

Extend the closed initial CREATE slice at f45ab5221 into the ordinary schema-only declaration used by Spark Hub. Accept a caller's classic PySpark StructType, ordered existing-column identity partitions, optional table/column comments, primary_key metadata and retention/CDF properties. SQL and one synchronous Python/JVM method converge on the existing issued CreateTable command. Native Delta creates the empty table at version0; Gravitino reserves and publishes it. Payload WRITE1 remains the subsequent input/transformation workstream.

Use Java17/Scala2.12.21, Spark3.5.4/3.5.6 x Delta3.3.0/3.3.2, actual Ranger server/client2.8.0 and S3/MinIO. Preserve Gravitino-owned registration, native transaction ownership, literal CREATE authority, one-shot execution, exact native evidence and no automatic replay. No Gravitino core/API/server/storage-model change, engine fork, Glue compatibility, raw-path destination, parallel transaction framework or authentication expansion. Group-only CREATE remains unsupported under SIMPLE authentication. Catalog-demo and production routing/cutover remain deferred.

## Source grounding and selected approach

Spark Hub v2 delta.py:202–231 builds an empty table from df.schema, optional comment/primary_key, retention and mandatory CDF, and ordered ordinary partitions; :529–568 separates creation from the payload write. Date/timestamp partition keys generate columns and remain M5. Base staging already passes the schema/transforms/properties to native Delta. DeltaKernel33MetadataAdapter and DeltaRegistrationOperations already project ordinary columns, partition order, configuration and description. The current companion deliberately restricts these to one nullable BIGINT, no partitions/comment and exact fixture properties.

Three approaches were considered. The stock DeltaTableBuilder accepts StructType but parses TableIdentifier before multipart names and finishes with forName/forPath; it cannot be assumed to preserve canonical naming or distinguish publication from its final read. A Python-generated SQL adapter risks duplicating Spark's type serialization and quoting. The selected approach adds one synchronous method to the existing GravitinoDeltaTable bridge, carrying native StructType and building ordinary Spark CreateTable through normal analysis. It supplies no authority and calls no staging method directly. A focused negative records the stock builder's naming/final-observation limitation inside implementation tests, not another standalone characterization milestone.

## Declaration and runtime contract

Support ordinary top-level boolean, byte, short, integer, long, float, double, string, binary, date, timestamp and valid decimal types; arrays of these scalar types and map<string,string> with nullable elements/values. Preserve top-level nullability and optional string column-comment metadata. Reject empty/duplicate/invalid column names, unsupported nested structs/collection nesting, timestamp_ntz, void/interval/UDT, generated/default/identity/mapping metadata and other field metadata before reservation. Use Spark/Delta's existing validation and resolver where available; no handwritten schema-to-SQL serializer or silent asNullable conversion. Native normalization must be established by a failing installed test before changing the reservation comparison.

Partitions are zero or more distinct existing top-level scalar columns, ordered and resolved unambiguously using Spark's resolver. Freeze their literal canonical names and reconstruct stock identity transforms from private immutable data. No bucket/truncate/time transform, generated column, nested reference or collection partition is admitted. The base lifecycle Intent gains immutable optional identity-column evidence without retaining caller-owned Transform objects; legacy/nonidentity declarations remain usable by the no-op base lifecycle but cannot prove companion authority. Preserve existing constructor compatibility and all lifecycle outcome behavior.

Allowed explicit table properties are the qualified CDF/retention/symlink keys, optional primary_key and explicit columnMapping.mode=none. CDF must be true; retention is an integer day interval1–364; symlink remains true. primary_key is opaque application metadata, not an enforced constraint. Reject unknown/reserved registration/location/path/credential/write controls. Table comments are optional strings, including quote/Unicode and empty string when supplied. Compare exact native description, ordered partitions and effective configuration to the frozen declaration, including approved session defaults and the operation marker. Parse commit partitionBy structurally instead of requiring the literal [] string.

Qualify the real CREATE-relevant Hub defaults: AQE=true with coalesce=true, advisory128m, initial partitions2000, minimum16m, parallelismFirst=false; dataSkippingNumIndexedCols=-1, optimized writes/autoCompact/symlink enabled, allowUnenforcedNotNull=true. Preserve the prior qualified profile as a regression control. Use native defaults for tuning values the Hub does not set, and record effective values. Freeze settings before issuance. Admit only proven native recipes under those settings; no silent AQE/work disabling or generic adaptive-plan exemption. This does not qualify the entire deployed Hub compute profile or payload optimization behavior.

## IF NOT EXISTS and identity

After literal-target CREATE authorization, inspect only a lease-bound metadata read through the existing DeltaRangerMetadataClient.read. Validate catalog/provider/name and a PUBLISHED DeltaRegistrationReceipt. Catch only genuine NoSuchTableException from table load as absence. Denied/unavailable/malformed/pending/replacing metadata remains an error. Reauthorize the same literal CREATE and context before returning a positive no-op. A no-op performs no stage, journal, reservation, native work or publication, and grants no SELECT/UPDATE/ALTER. It does not compare or alter the existing declaration: differing supported schema/comment/properties still no-op, matching Spark IF NOT EXISTS semantics.

For a missing target, execute the unchanged reservation/native/publication lifecycle. A concurrent winner between observation and RESERVE remains an honest conflict/uncertain outcome; do not turn an arbitrary transport failure into no-op or retry. The platform excludes concurrent dataset writers, but a fresh test still pins this race behavior. Unsupported proposed declarations are rejected during analysis even with IF NOT EXISTS; this intentional bounded-profile limit is documented.

The Python API is GravitinoDeltaTable.create(spark, name, schema, *, partition_by=None, properties=None, comment=None, if_not_exists=False) -> None. The JVM method accepts SparkSession, String, StructType, java.util.List[String], java.util.Map[String,String], nullable String and Boolean, returning Unit. Validate/copy arguments; use Spark's schema JSON codec, multipart parser and normal command analysis/execution. Require the issued CREATE carrier. Do not reuse MERGE-specific ownership as CREATE authority, return a native DeltaTable handle or catch postcommit errors as success. An explicit later read is separately authorized.

## Verification and completion

Task1 proves ordinary SQL declarations and exact native/registration binding, including immutable partitions and the Hub CREATE settings. Task2 proves positive-only IF NOT EXISTS and the real PySpark bridge. Task3 qualifies both entry points with actual Ranger2.8/Gravitino/MinIO on all four pairs, using one source-bound assembly and actual per-pair runtimes where byte reuse is valid. Test nullability, arrays/maps, comments, partition order, custom property/defaults, positive no-op, denial/unavailability/pending/race, forged/mutated declarations and committed/uncertain outcomes. Keep native log0/checksum/marker/nativeID, RESERVE/PUBLISH/STATUS and positive absence/no-op evidence.

Reuse predecessor public-grant, package and downloaded-policy infrastructure plus existing PySpark acceptance patterns. Full changed behavior/fault cases run on the lead pair; repeat decision-relevant SQL/PySpark success/no-op/deny on the others. Do not rerun unchanged historical matrices. Seven historical append characterization failures are known baseline observations; retain them and use the current meaningful append controls. Previous153 authorization evaluations and unmeasured production audit capacity remain visible. Publish only this ordinary declaration scope after task reviews and final integrated review, then push the checkpoint as previously authorized; retain evidence and worktrees.
