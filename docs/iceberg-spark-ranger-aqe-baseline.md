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

# Iceberg, Gravitino and Ranger AQE baseline

Date: 2026-10-25. Experiment baseline: `9a4f648dd`. Status: all four runtime
cases passed; independent evidence review approved the bounded result. No Delta product behavior changed.

## Result

Stock Iceberg 1.11.0 and Kyuubi Spark AuthZ 1.10.2 executed named, Ranger-governed
reads with real adaptive execution on Spark 3.5.4 and 3.5.6. Both direct native
catalog access and the existing Gravitino Iceberg wrapper passed against actual
Ranger 2.8.0 and MinIO. Loaded Ranger client libraries were also 2.8.0; the
upstream Kyuubi dependency default was not used as version evidence.

| Route | Spark 3.5.4 | Spark 3.5.6 |
| --- | --- | --- |
| Native Iceberg `SparkCatalog` | Pass: `native-354-attempt05` | Pass: `native-356-attempt01` |
| `GravitinoIcebergCatalogSpark35` | Pass: `wrapper-354-attempt02` | Pass: `wrapper-356-attempt01` |

Every case demonstrated:

- A final `AdaptiveSparkPlan` with two materialized payload shuffle stages and
  two coalesced readers, each covering all eight shuffle partitions.
- An exact four-row join multiset after Ranger's `region = 'east'` filter and
  custom `'MASKED'` email expression, including duplicate join matches.
- SELECT denial for a column referenced only in a WHERE predicate.
- Fresh-query denial after revoking SELECT and observing policy refresh in the
  same application process. Three actual plugin policy-download HTTP 200 responses
  and cache revisions 7, 8 and 9 were retained for each case.
- Original setup and application exits of zero, separate setup/application users,
  actual loaded provider origins, and retained initial/final plans.

The application uses ordinary named SQL through classic PySpark. It loads stock
Kyuubi and Iceberg extensions; no Delta companion, custom execution guard or
exact-stage permit implements this baseline. AQE and coalescing are enabled,
shuffle partitions are eight, and static/adaptive broadcast thresholds are -1.
This proves the observed shuffle route, not every adaptive optimization.

## Gravitino and deployment scope

Wrapper cases use a real local Gravitino server and real metalake/catalog APIs.
The discovered catalog is `GravitinoIcebergCatalogSpark35`; its table is
`SparkIcebergTable`, a Gravitino subclass of native Iceberg `SparkTable`. Named
Gravitino table GETs and API responses are retained. The catalog selects stock
`HadoopCatalog` via the existing custom-backend option; no custom catalog class
was written. Tables were seeded natively before wrapper lookup, so these cases
do not establish table creation initiated through Gravitino.

The fixture uses MinIO/S3A with one setup writer. It does not establish production
HadoopCatalog-on-object-storage concurrency guarantees. Gravitino metadata
authorization and REST auto-routing are disabled in this isolated wrapper server;
Ranger query enforcement remains enabled. Consequently this result does not
qualify Gravitino grants-to-Ranger propagation, metadata authorization, credential
vending, or Hive/JDBC/REST backend behavior.

Policies use Ranger Hive database/table/column resources with a unique database
per case. Catalog is not a Ranger resource dimension in this stock mapping;
cross-catalog isolation requires separate qualification. Identities use controlled
local Spark/Hadoop SIMPLE users, not production remote authentication. Resource
policies were tested; tag policies and audit delivery were not. Tag-enrichment
warnings and synthetic-user group lookup warnings are retained in the logs.

## Consequence for Delta

The [earlier AQE feasibility result](superpowers/plans/2026-10-25-delta-governed-aqe.md)
rejects the examined exact-invoking-stage ownership approach. This new baseline
shows that such permits are not necessary for the tested Iceberg/Kyuubi integration.
The native catalog delegates execution to Spark, and Kyuubi installs authorization,
row-filter and masking rules during query planning. See the pinned
[Kyuubi extension](https://github.com/apache/kyuubi/blob/v1.10.2/extensions/spark/kyuubi-spark-authz/src/main/scala/org/apache/kyuubi/plugin/spark/authz/ranger/RangerSparkExtension.scala)
and its [authentication/configuration model](https://kyuubi.readthedocs.io/en/v1.10.2/security/authorization/spark/overview.html).

A revised Delta design should explicitly separate user-visible semantics from
additional physical-stage assurance. Adopting planning-time authorization would
change the promised revalidation boundary: fresh-query revocation after policy
refresh is not revocation of an already planned query, running stage or cached
result. These cases were not tested here. Arbitrary code controlling the Spark
JVM is outside this baseline's trusted-engine model.

Retain Delta-specific requirements independently: canonical Gravitino registration
through Delta lowering, original column/assignment demands, bounded CDF intervals,
policy application to authored MERGE expressions, hidden-row/raw copy-forward
preservation, native retries and commit reconciliation. Stock Kyuubi's MERGE command
descriptor treats its target as UPDATE output and its source as a query child;
ordinary Iceberg read success does not prove those Delta target-policy semantics.
See the pinned [command descriptors](https://github.com/apache/kyuubi/blob/v1.10.2/extensions/spark/kyuubi-spark-authz/src/main/resources/table_command_spec.json).

No existing Delta guards have been removed. Delta governed AQE remains unqualified,
and catalog-demo remains deferred. Revising the ownership contract is a separate
implementation decision; this experiment supplies the missing baseline.

## Evidence and reproducibility

All artifacts are retained under
`.superpowers/sdd/2026-10-25-iceberg-aqe-baseline/`:

- `evidence/runtime-report.md`: full scenarios, commands, versions and limitations.
- `evidence/evidence-index.json`: 548 hashed evidence files; controller verified all.
- `evidence/verified-results.json`: checked aggregate; originals remain in each case.
- `harness/run_baseline.py`, `iceberg_app.py`, `verify_evidence.py`: exact experiment
  and checker; each attempt retains its original source snapshots.
- `source-comparison.md`: source-level boundary comparison and pinned references.
- `independent-review.md`: original-evidence and artifact review; no Critical or
  Important findings.

Two nonblocking evidence caveats remain explicit. Original command records contain
arguments, identity and start time, not full environment allowlists as the frozen
runtime report states; runtime records independently establish the actual UGI. The
automated checker does not validate the supplemental wrapper-JAR hashes; independent
review checked those originals, including the saved pre-build client JAR. The
original evidence and report remain immutable; see `review-errata.md` in the
evidence workspace.

Original failed provisioning/classpath attempts are preserved. Their failures were
not treated as AQE incompatibilities. Numeric child exits were not captured for
three failed startup attempts; original supervisor exit 1 and startup traces remain.
All passing cases have original child exit receipts. Earlier results were not
backfilled with the in-process policy-version field added in the final wrapper run.

A narrow offline build verified matching class entries in the used wrapper/common/
catalog/client artifacts; only client build-info metadata changed. The old client
JAR is retained for original container-hash verification. This does not claim a
fresh build of the prebuilt server distribution. Owned local servers/proxies were
stopped; shared Ranger/MinIO services and unique evidence/resources were retained.

## Ordinary-read comparison candidate — 2026-10-28

The later ordinary-read matrix uses Iceberg 1.11.0, Kyuubi 1.10.2, Java 17,
Scala 2.12.21 and actual Ranger server/client 2.8.0 on both Spark patches. All
original seed/application/supervisor/outer exits are 0. These measurements extend
the historical AQE baseline; they do not revise its original records.

| Spark | Catalog route | Executed | Blocked view probes | Observed action differences | Unqualified revoke transitions* |
| --- | --- | ---: | ---: | ---: | ---: |
| 3.5.4 | native | 36 | 6 | 11 | 1 |
| 3.5.4 | gravitino | 36 | 6 | 11 | 1 |
| 3.5.6 | native | 36 | 6 | 11 | 1 |
| 3.5.6 | gravitino | 36 | 6 | 11 | 1 |

*The unqualified transition is included among observed action differences: both
attempts ran, but an id-only warmup denied, so a later denial after SELECT revoke
does not prove revoke-on-reuse. Six blocked view probes are not counted as executed
actions or successful comparisons. Each points to the actual createTempView denial
on policy-injected region, with original error, analyzed plan and audit evidence.

Native count can return 4 without a SELECT audit for id-only bare, explicit-star,
authored-region or no-grant probes. Composed id collection denies on policy-only
region. Retained masked frames/views and a fresh lookup of the old view return M
after confirmed NULL-mask refresh. The denied-email fixture returns four masked
NULL values. These are measured outcomes under the retained policies. The wildcard
COUNT is separately labelled measured-policy-outcome, not an independently
predicted grant or proof of permission parity.

Each cell’s four successful native Pandas probes retain actual transport/fallback flags,
Pandas/PyArrow versions, primitive dtypes and UTC settings. The separate join proves
final materialized AQE with a positive SELECT-audit control. Kyuubi's unchanged
handler supplies audited permission events; listener drain and bounded audit
submitted/complete-line counters bind raw offsets to serial actions. Zero events
are measured absence. Mask/filter evaluation requests and their policy IDs are
unobserved; actual plans/values and downloaded policy snapshots provide semantic
evidence. Hive resources retain their lack of a catalog dimension.

Each cell uses an isolated database and fixture-authored policies. Native seeding,
local simple identities and metadata authorization disabled for the Gravitino
wrapper do not establish production authentication or grant propagation. Delta's
supported-action rechecks and original occurrence demands remain independent.
See the [candidate support boundary](delta-spark-parity.md#ordinary-spark-read-candidate--2026-10-28)
and private final evidence index for exact artifacts. Final integrated review and
production deployment remain separate gates.
