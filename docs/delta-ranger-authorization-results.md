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

# Delta Ranger authorization SPI results

The initial optional authorization provider passed its task review on September 14,
2026, at `7626acc8c841f81b0385b71cd22bd57f2874a3bc`. This is one stage of the
[production Ranger plan](superpowers/plans/2026-09-08-delta-ranger-production-runtime.md).
Spark enforcement and full Delta support remain unfinished.

The `ranger-delta` SPI uses a provisioned Ranger 2.8.0 service bound to one metalake
and preserves catalog/database/table/column resources. It translates admitted table
SELECT grants and reconciles owner state with readback. Catalog visibility contributes
no Ranger access. The Delta provider validates immutable canonical bindings before
authorization initialization. No core, API, server, client or storage-schema source
was changed.

Fresh verification of implementation commit `703002e46` executed 38 tests with zero
failures or errors. One separate live Admin test was skipped. All eight public REST
callback cases and eight affected provider checks executed. These tests use the real
Gravitino dispatcher and public client with an HTTP Admin fixture. Spotless, strict
Javadoc and packaging passed. The final review fix then passed all 11 affected tests;
its two new regressions first reproduced the inherited forced principal deletion.
The full/native suite was not rerun or relabeled as a full run of that narrow fix.

The review fix rejects `onUserRemoved` and `onGroupRemoved` before any Admin
interaction. A catalog binding cannot prove ownership of a global Ranger principal.
These callbacks need a separate ownership/reference/reconciliation contract before
enabling removal. No current core dispatcher invocation was found; this is a tested
SPI boundary, not a claim of observed public REST or deployed Ranger deletion.

The rebuilt server distribution contains 52 jars and 593 unique Ranger classes,
with zero duplicate Ranger class origins. All five Ranger Maven artifacts are
version 2.8.0. Only the Delta authorization jar changed for the review fix; all 51
dependency jars match the earlier package. Project test fixtures and test-only jars
are excluded. Ranger's own audit TestEvents class remains in its original upstream
jar. The default Ranger dependency remains unchanged.

A native Spark 3.5.4 / Delta 3.3.2 S3 seed supplied the observational registration
tests. After the callbacks finished, an independent raw S3 inventory matched all
four objects, including the directory marker, in key, length, SHA-256 and timestamp.
The archived original bytes also match. Native UUID is
`9db8bec3-938b-4be0-9077-e27196ba2b76`, version 0, with two rows. This single seed
is not the four-pair Spark/Ranger acceptance matrix.

The interrupted temporary worktree and its historical archives, XML, private
configuration and tmpfs MinIO data disappeared. The committed foundation survived.
Eighteen uncommitted files were reconstructed from successful recorded writes, with
provenance and hashes. The final historical failing regression was reproduced before
fixing it. Historical green results were not substituted for current evidence.
The original main checkout and user files were preserved.

Fresh evidence is retained in the persistent ignored worktree under
`.superpowers/sdd/2026-09-08-delta-ranger-production-runtime/`: implementation and
review reports, original RED/GREEN XML, command exits, source archives, jar hashes,
class origins, complete distribution archives and separate fix evidence. Root's
independent source/XML checks are `task-1-root-evidence-check.json` and
`task-1-root-fix-1-check.json`. Native manifests and archived bytes are under
`.superpowers/runtime-20260914/ranger-spi-fixture/`. Historical missing archives have
not been recreated or represented as original evidence.

The owned Ranger database and Admin containers were restarted after these tests.
The preserved setup marker makes the existing entrypoint skip setup and default
service bootstrap. Authenticated read-only readiness returned HTTP 200, and the
deployed Ranger library names are 2.8.0. No policy, role, service or definition was
changed by that restart preparation. Spark 3.5.4 and 3.5.6 environments are restored.
Readiness and environment setup do not establish downloaded-policy enforcement.

The review records two nonblocking improvements for the final review: unchecked
casts in a unit fixture, and representative mask/filter payloads in the local
foreign-model preservation test. Genuine live model preservation remains a later
gate. Inherited build/environment warnings are recorded separately.

Remaining work includes the Spark policy engine, authenticated query and separate
metadata identities, genuine Ranger policy and role downloads, and executed column
access, masks, filters and joins across all four required Spark/Delta pairs. General
ordinary grant reconciliation, broader Delta operations and catalog-demo integration
also remain. Same-owner repair covers owners only; identical ordinary grant/revoke
retries can skip callbacks after local persistence. Global role-delete guards and
Ranger's audit-filter cleanup side effect still require deployed qualification.
