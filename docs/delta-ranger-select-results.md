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

# Delta Ranger fresh SELECT qualification

The optional Spark wrapper now authorizes original columns of registered native
Delta scans before applying Ranger CUSTOM/NULL masks and row filters. Projection,
predicates, ordinary aliases and conditioned inner joins are qualified. Same-name
tables in different catalogs retain separate identities and policy decisions.
The native Delta table descriptor and disabled catalog-update invariant remain.
No Gravitino core, API, server, client or storage-schema source changed.

Source `d5e35f6e17862b0a86c7090f87e49571e622f0b4` and startup fix
`f7343d46a6ee39fd1f614e648c5b555d031974e7` passed independent task review and scoped
re-review. Two nonblocking items remain for final branch review: post-acquisition
failed-init cleanup coverage and repeated expression-reference traversal.

## Verification

| Scope | Spark / Delta | Result |
| --- | --- | --- |
| Initial final source, common startup and optional module | 3.5.4 / 3.3.0 | 69 tests passed |
| Same tested source, second Delta patch | 3.5.4 / 3.3.2 | 69 tests passed |
| Subsequent application-authentication fix | 3.5.4 / 3.3.2 | 13 focused wrapper tests passed |

All listed passing runs had zero failures, errors or skips; strict Javadoc, scoped
formatting and JAR checks passed. The authentication regression first produced
seven failures across 13 wrapper cases. The fix did not change native query rules;
the earlier 69-case runs are not relabeled as full-suite runs of the fix.
Final test classpaths use Scala 2.12.21, Java 17 and Ranger 2.8.0.

Actual local native rows establish masks/filters, quoted mapped columns, joins,
original-column denials, fresh-session behavior, changed-subject rejection, raw and
mixed-source rejection, native metadata reconstruction and second-pass native rule
idempotence. Metadata transport, application discovery and policy-engine transport
use explicit test seams. These checks do not establish public Gravitino grants or
genuine downloaded-policy enforcement.

## Startup and native integration

`spark.gravitino.enableDeltaRangerSupport` defaults to false and requires Delta
support. Common startup selects the optional no-arg wrapper and orders its extension
before native Delta without a static Ranger dependency on the default path.
The initial wrapper requires SIMPLE Gravitino application/current-actor auth and
a separate explicit SIMPLE metadata-reader principal in an owner-only configuration
file. Other application modes reject because their effective principal can differ
from Spark/UGI. Ranger's separate Basic-authenticated downloader is unaffected.
See the [module README](../spark-connector/v3.5/spark-delta-ranger/README.md).

Pinned Kyuubi extraction loses V1 catalog names. An extraction-only plan copy uses
opaque per-scan tokens and an exact local mapping; execution retains the original
native descriptors and expression identities. All original query columns are
authorized before filters or masks. Masking does not grant column access.

Native Delta caching requires a synchronous metadata-preparation scope around the
public PrepareDeltaScan rule. Qualified routes include exact state/statistics RDD
schemas from the selected native snapshot, readless encoder bindings and two native
UDF forms with origin, signature and argument checks. Scope cleanup, thread
isolation and outside-scope rejection are tested. This depends on trusted launcher
and runtime artifacts; it does not establish arbitrary-job or metadata-confidentiality
security. The [implementation plan](superpowers/plans/2026-09-08-delta-ranger-production-runtime.md)
records each native exception and its qualification requirements.

## Artifact evidence and remaining gates

Both runs' six archived connector JARs match their hashes. Each set of 199 external
dependencies matches its manifest. Base Delta artifacts contain no Ranger/authz/
companion classes; optional artifacts contain no Spark/Hadoop/Scala/Delta platform
classes. The whole common JAR from the initial Delta 3.3.0 run was not retained;
later captures do not replace that missing historical binary. Resolution-time
project hashes and post-build archives have explicitly different timing.

The startup fix captures its actual test inputs after build dependencies complete:
11 archived mutable inputs, 191 external JARs, one absent resource directory and
68 directory-entry hashes verified for each RED/GREEN run. Its 25 source entries
match the committed fix. Older evidence limitations remain separate.

The next gate is packaged public Gravitino callbacks, real Ranger 2.8 downloads and
executed queries on all four Spark 3.5.4/3.5.6 × Delta 3.3.0/3.3.2 pairs. It must
prove catalog-only application visibility, Ranger-only column-user metadata access,
grant/revoke and membership propagation, and foreign-policy preservation.
Full Delta operations, aggregates, additional masks/authentication modes, ordinary
grant reconciliation, streaming/time travel/maintenance, cached-plan revocation and
catalog-demo integration remain subsequent work. This milestone is not full Delta
support under Ranger enforcement.
