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

# Optional Spark policy-engine stage

The optional adapters passed independent task review and two scoped fix reviews on
September 14, 2026, at source `58ebf23796184c1e77689bf6eef2da2dff26310d`.
This completes Task 2 of the [production plan](superpowers/plans/2026-09-08-delta-ranger-production-runtime.md).

The optional Spark 3.5 module now supplies real Ranger 2.8.0 policy downloads,
catalog-aware immutable access requests, query subjects and a separate bounded
metadata reader. It does not yet install Spark query-plan enforcement. The no-arg
catalog wrapper, original-column authorization, masks/filters over executed queries,
and the packaged four-pair production chain are subsequent stages.

A service binds to exactly one metalake through the single propagated key
`ranger.plugin.spark.gravitino.metalake`. Ranger omitted the earlier arbitrary key
from downloads, so the optional server provider and service definition were updated
as well. The Gravitino catalog property remains `authorization.ranger.delta.metalake`.
Initial provisioned services must migrate the service key before activation.

Real downloader checks against the owned Ranger 2.8.0 instance observed access
revocation/restoration, role membership revocation/restoration, wrong-metalake
rejection/restoration, distinct catalog access and mask/filter decisions, and shared
engine lease ownership. Role changes also increased policy versions. A refused local
endpoint with a valid cached snapshot retained its decisions; an empty cache rejected
readiness. These are engine-level decisions, not executed Spark policy rewrites or
the public Gravitino-to-Ranger callback chain.

Initial frozen source passed 29 adapter and 17 affected provider tests, all without
failures, errors or skips. Independent review found two concurrency defects: partial
refresh could pair restored service config with an old wrong-bound engine; a blocked
metadata cleanup could postpone another lease's expiration, and stalled construction
held the lease monitor. Three deterministic regressions reproduced those failures.
The fixes passed 28 affected tests, strict Javadoc and Spotless, plus one genuine
cached-startup check at policy/role versions 14/14 on archived fix artifacts. The
initial 46-test suite is not relabeled as a full rerun of the later fix.

A subsequent configuration guard passed six focused tests after one regression
reproduced the accepted-mode gap. Ranger full-snapshot policy downloads are required;
`ranger.plugin.spark.supports.policy.deltas=true` rejects at trusted startup because
incremental updates can reuse contexts. Default absent/false remains supported.
This restriction concerns Ranger policy transport, not native Delta Lake features.

The engine fix records completed public authorization-context events and rejects
incomplete or incompatible context transitions. The metadata fix checks a monotonic
deadline independently of timer execution, moves blocking construction/cleanup out
of state locks, and disposes of clients that arrive after terminal close/expiry.

Production startup requires the AuditProviderFactory logger at WARN or stricter;
a guard runs before singleton initialization. Ranger's INFO property dump exposed a
fixture-only downloader credential in the first harness; that JVM was drained and
the credential rotated, with old authentication rejected. Safe subsequent logs were
checked without printing secrets. No Admin credential is a query credential. This
is a deployment requirement, not a global logging change.

Query identity comes from the authenticated current UGI and actual groups. A
contradictory session identity rejects. Token query mode remains explicitly
unsupported without authenticated token-to-UGI attestation; the existing JWT cache
identity parses unverified claims. Separate metadata authentication may use existing
custom/token client builders. Synthetic SIMPLE subjects do not qualify a production
identity bridge.

All 42 selected dependency hashes were verified. Historical helper, live and final
base runtime hashes differ because focused Gradle tests automatically rebuilt the
shadow JAR. Original early whole JAR bytes were not archived and are not claimed
identical. A current final JAR was archived and passed the exact helper probe with
all helper origins in that archive; fix-startup project binaries were also archived
before launch. No Ranger/authz classes were found in the current base runtime.
The original config/subject RED XML was overwritten before archival; its console
failures remain, while the mislabeled XML directory is explicitly identified as
later metadata test output. No evidence was fabricated or relabeled.

Reports, original XML, command exits, source/artifact manifests and reviewed diffs
remain in `.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/` inside the
persistent ignored worktree. Root checked committed source and original XML hashes,
final exits, 216 fix artifact hashes, and the cached-startup observations. Private
runtime credentials and raw logs must not be published with evidence.

The broader goal still requires full Delta operation and remaining mask support,
authenticated token query support, ordinary grant reconciliation, cached/reused-plan
revocation semantics and the supported deployment boundary. Current demo Compose
selects Ranger 2.7.0; its later isolated integration must use 2.8.0. Demo source and
user changes have been preserved.
