<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Delta Ranger production-chain qualification

Task 4 is in progress. The optional companion packaging and test-application
checkpoint are implemented; no public Gravitino → Ranger → executed Spark query
chain has been qualified by this checkpoint. The accepted preceding source is
`9aafaa9cb433e2341a3d7c1384d504ba4d7bc772`.

## Verified packaging checkpoint

| Actual archived runtime | Packaging | Pinned helper probe | Public callbacks / native SELECT |
| --- | --- | --- | --- |
| Spark 3.5.4 / Delta 3.3.0 | Passed | Passed | Pending |
| Spark 3.5.4 / Delta 3.3.2 | Passed | Passed | Pending |
| Spark 3.5.6 / Delta 3.3.0 | Passed | Passed | Pending |
| Spark 3.5.6 / Delta 3.3.2 | Passed | Passed | Pending |

Each final runtime manifest contains the actual distribution JARs, matching native
Delta JARs, the previously qualified S3A dependency pair, archived base and Ranger
companion JARs, and a separate test application. The companion has no duplicate
class origins against the actual distribution/base runtime and contains no
Spark/Scala/Hadoop/Delta platform classes, Gravitino client/protocol classes,
Kyuubi utility copies, or project fixture applications. The base remains free of
Ranger/authz classes; its inherited distribution overlaps are recorded separately.

The companion resolves all six Ranger Maven artifacts to 2.8.0 and retains their
provenance metadata. It uses plain Kyuubi authz 1.10.2 while the base alone supplies
Kyuubi Hive 1.11.0 utilities and private Guava/publicsuffix classes. The executed
helper probes cover the accepted version/quoting, reflection, service-loader,
Ranger configuration, and private Guava/publicsuffix families on each archived
pair. They do not execute the Spark query driver or establish policy downloads.

Initial packaging attempts exposed Jackson Java 21 multi-release bytecode beyond
the pinned shading tool and duplicate Jersey/Hostname/Servlet/annotation inputs.
Original failing builds and pre-correction archives are retained. Final packaging
omits Java 21-only entries for the qualified Java 17 launcher, uses Kyuubi's
existing non-JNA Hostname implementation, and keeps distribution-provided APIs out
of the companion. Three original Ranger `ugsync-util` helpers and their JAAS name
strings move to a private package. This is not Kerberos qualification.

## Public-chain checkpoint still pending

The production server bundle has 145 parent runtime JARs and separate archived
provider/authorization packages, with no project fixtures or parent Ranger
classes. Root provisioned a distinct Ranger service/definition/tag service and
launched the production Gravitino main from those archives. Readiness is setup
evidence, not callback acceptance. The worker has not issued public metadata or
Ranger model mutations at this checkpoint.

A separate archived public-client application is ready for one-shot bootstrap and
the truly fresh principal callback. It records both public Gravitino and downstream
Ranger HTTP attempt/completion events, forwards normal response status/body
unchanged, and reads Admin state independently without replay after failure.
Its gateway tests first failed both assertions, then passed both, including actual
HTTP 204 preservation and a single method/path-scoped HTTP 503 fault. The separate
Spark query application checks actual UGI/application identity, metadata separation,
original runtime provenance, downloaded versions, authorization-denial causes, and
executed rows; its live query mode remains unexecuted.

The known Ranger external-user HTTP 204/expected-200 incompatibility has not yet
been reproduced through this Java public callback. No optional provider correction
is claimed. Global role-delete/audit cleanup, empty-role bootstrap ordering,
ordinary failure-after-persistence divergence, catalog-only visibility and its
revoke, Ranger-only column metadata access, grants/membership refresh, foreign
policy preservation, masks/filters/joins, and actual downloader/cache/outage
composition all remain required before Task 4 acceptance.

Evidence is in the ignored persistent worktree directory
`.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/task-4-evidence/`.
Original commands/exits, focused XML, post-dependency execution inputs, source
snapshots, complete archived classpaths, dependency/class hashes, and helper
results remain distinct. One early formatting-only gateway attempt copied stale
Task 3 XML; it is explicitly labeled non-evidence and the runner was corrected.
An earlier source snapshot also included generated runtime build files; final
source binding must use the separately identified source paths. The initial artifact
verifier also assumed an input manifest existed for an up-to-date Shadow task;
the corrected verifier checks byte-identical companion reuse against its original
producer manifest. These limitations remain separate from the original gateway
RED/GREEN records and actual runtime manifests.

This milestone is not full Delta support or a security/demo integration completion.
Ordinary role/grant reconciliation, broader authentication, remaining masks and
expressions, DML/DDL, streaming, time travel, maintenance, cached/reused plans,
metadata confidentiality, supported launcher restrictions, and isolated catalog-demo
integration remain subsequent gates. No demo source, core/API/server/client source,
or native storage objects were changed by this checkpoint.
