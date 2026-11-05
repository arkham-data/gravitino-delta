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

# Ordinary governed Delta CREATE qualification

Ordinary schema-only CREATE is qualified through SQL and the real classic PySpark
`GravitinoDeltaTable.create` API on all four pinned Spark/Delta pairs, using actual
Gravitino, Ranger server/client 2.8.0 and S3A/MinIO. This extends initial CREATE; it
is not payload-write, full Spark Hub, M2 completion or production cutover evidence.

## Actual-service scope

| Spark | Delta | SQL decisions | Classic PySpark decisions |
| --- | --- | ---: | ---: |
| 3.5.6 | 3.3.2 | 9 | 9 |
| 3.5.4 | 3.3.0 | 4 | 4 |
| 3.5.4 | 3.3.2 | 4 | 4 |
| 3.5.6 | 3.3.0 | 4 | 4 |

The 42 decisions comprise eight published CREATE0 operations, ten positive no-ops,
18 genuine CREATE authorization refusals, two plain duplicate conflicts, two
unsupported nested-schema refusals, and two separately authorized read refusals.
Five lead SQL decisions are retained from package v3; its four pending decisions
completed under v4 using the existing stopped-store copy procedure. The other
33 decisions ran under v4 or its byte-verified production reuse packages. The
v3/v4 source difference is only the fixture policy serializer and its tests;
production and downloaded helper bytes are unchanged. Every live Python call
uses the packaged API, a real StructType and the same pinned Java/Python connection
and thread; it does not submit CREATE SQL on behalf of the API.

The shared declarative fixture contains non-null BIGINT `id`, STRING `region`,
TIMESTAMP `event_time`, DECIMAL(18,4) `amount`, nullable ARRAY<STRING> `tags` and
nullable MAP<STRING,STRING> `attributes`. It preserves column comment `clé 'id'`,
table comment `table '雪'`, ordered identity partitions `(region,id)`, opaque
`primary_key=id,region`, CDF=true, symlink=true, and 7/30-day retention. Each success
joins the exact native schema, comments, partitions, configuration and checksum to
RESERVE/PUBLISH/STATUS identity, native ID and version0. No payload Parquet or
version1 was created. Public Gravitino columns/nullability/comments/partition order
are checked independently. Public properties retain the existing safe CDF/mapping
projection; full retention/primary_key/default configuration is proved in native
metadata, not falsely claimed as a public arbitrary-property projection.

AQE/coalesce remain enabled with advisory128m, initial partitions2000,
minimum16m and parallelismFirst=false; optimized writes/autoCompact remain enabled,
with dataSkippingNumIndexedCols=-1 and allowUnenforcedNotNull=true. The inherited
small-fixture snapshotPartitions=2 override was removed only for this ordinary
profile; the original simple profile is preserved. Actual readiness records the
Hub settings and null getOption observations for the other tuning keys. Separately,
bounded inspection of both pinned Delta JARs records declared autoCompact
minNumFiles50/maxFileSize134217728 and optimizeWrite binSize536870912b/
numShuffleBlocks50000000; optional snapshotPartitions/minFileSize are undefined.
These JAR declarations are not presented as runtime measurements. Task1's actual
lead native observations independently recorded parsed binSize512,
maxShufflePartitions2000 and the same defaults/optional values.

## Positive no-op and authorization

IF NOT EXISTS proposes a different supported schema, partition list, comment and
retention property. It requires its own positive PUBLISHED metadata GET and fresh
completed policy/role downloads. Before/after registration and full object byte
inventories remain equal, with no public mutation or journal entry. Plain duplicate
CREATE remains TableAlreadyExistsException. Nested structs remain refused before
reservation; broader declaration boundaries remain Task1/Task2 installed tests.

After CREATE, Gravitino's automatic owner-ALL policy means removing a schema grant
alone does not establish absence of SELECT. The lead controls add a SELECT-only
deny to the exact existing literal owner policy while preserving its grants. A
separate read genuinely fails through DeltaRangerPolicyEngine.authorize, then
IF NOT EXISTS succeeds without changing the target. Fresh unowned targets prove
no-grant, literal CREATE deny and revoked membership. Generic configuration or
unsupported-plan SecurityException does not qualify as an authorization denial.

## Preserved attempts and evidence

Local immutable evidence is under
`.superpowers/sdd/2026-11-05-delta-ordinary-schema-create/`, indexed by
`task-3-report.md` and `task-3-final-audit.json`.

- Lead SQL v1 stopped before reservation because inherited snapshotPartitions=2
  violated the ordinary native-default profile. Metadata404, zero objects and zero
  public mutations were retained; no native/PUBLISH attempt was replayed.
- Lead SQL v2 passed five decisions, then Ranger rejected a duplicate-resource
  SELECT-deny policy POST (error3010). Those five passes are supplemental, outside
  the 42-decision count; its published target remains retained.
- Lead SQL v3 passed five decisions and successfully updated the existing owner
  policy. The fixture then rejected Ranger's omission of empty groups/roles and
  insertion of delegateAdmin=false. The fix was checked against the retained
  successful PUT/GET bytes. Only pending decisions continued from a byte-verified
  copy of the stopped fixture store; successful policy/native/publication calls
  were never reissued, and original files remain unchanged.

Package receipts retain fresh source-before/after captures, current base/optional/
provider/server/auth-plugin artifacts, the eight downloaded helper families and
packaged Python bytes. Remaining pairs use actual distinct Spark/native Delta
artifacts with unchanged production bytes. Python evidence additionally binds
loaded module hashes to installed RECORD/METADATA and original installation
artifact reports. Actual Ranger admin installation/JAR bytes and endpoint association
were freshly rechecked. Final audit revalidated all 42 records and process exits.

Group-only SIMPLE remains unsupported. No live mid-operation fault matrix,
concurrent-writer support, audit throughput, generated columns/date partitions,
CTAS, mapping-name, schema evolution, raw-path/Glue fallback or new identity model
is claimed. Task1/Task2 installed unavailable/pending/race/uncertain-outcome controls
remain distinct from actual-service coverage. The earlier 153 CREATE authorization
evaluations and unmeasured production audit capacity remain limitations. Inputs,
transformations, overwrite and catalog-demo routing/cutover follow separately.

Post-live review tightened the acceptance-only nested-field negative: both the
checker and Python caller now require the original unsupported-column validation
cause, including its class, message and originating frame. Unrelated native or
Python-only failures cannot qualify that boundary. The stricter checker revalidated
all 42 unchanged original records; both original boundary negatives contain the
required cause. Original runtime/package/driver bytes remain retained. This is
read-only revalidation, not a new live execution or package qualification of the
revised Python caller.
