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

# Governed schema-only Delta CREATE — design

Status: selected implementation direction under the existing execution authorization; builder review found no material unanswered questions; execution is task-by-task. This is the first successful reservation-bound creation profile, not M2 closure or complete Spark Hub creation support. Schema CREATE_TABLE grant translation is already closed at `9733e0914`; installed characterization is retained at `c49f282fe`.

## Outcome and scope

Execute installed named `CREATE TABLE a.db.created (id BIGINT) USING delta` with the required fixture table properties, through the real base RESERVE → native schema CREATE0 → verified PUBLISH lifecycle. The actor can start as a schema non-owner with schema CREATE_TABLE and no explicit SELECT/UPDATE grant. Authorization uses the literal destination table and occurs before reservation, native execution and publication. This command does not demand SELECT/UPDATE. Existing Gravitino ownership behavior remains: the public RESERVE createTable hook assigns the actor as table owner, and Ranger owner mapping can then add permissions independently of the schema grant.

The initial qualified declaration is the measured nullable BIGINT `id`, mapping none, no query, partition transform, explicit location, column metadata/default/generated expression, table comment or IF NOT EXISTS. Reject other declarations before reservation. This narrow profile is an implementation checkpoint. Ordinary schemas, ordinary partitions, comments, properties and createIfNotExists need an explicit next M2 creation-profile task. Preserve the actual Spark Hub generated-date-partition gap under M5, as specified by the [approved foundation](2026-10-30-spark-hub-write-foundation-design.md). Schema evolution, input/transformation support and overwrite/full refresh remain binding roadmap priorities; this slice does not implement them or retire their work.

## Source basis and selected approach

The [installed findings](2026-11-04-delta-create-installed-probe-findings.md) establish SQL refusal before tableExists and a real base CREATE0 followed by installed LogicalRDD refusal. Native callbacks were observed only after CREATE0; a private precommit transaction hook is not a prerequisite. The [native-create findings](2026-11-04-delta-create-boundary-findings.md) separate CREATE0/publication0 from a later WRITE1. Preserve these original findings rather than rewriting historical outcomes.

`GravitinoDeltaCatalogSpark35.Pending.materialize/publish` and `DeltaPublicationStage.commitStagedChanges` already own the required base ordering, immutable schema copy, receipt validation, journal and no-replay behavior. Add typed lifecycle boundaries there; do not copy Pending. Follow `DeltaRangerCdfPropertyCommand` for an issued one-shot logical/physical command, but build creation authority from the intent and RESERVED receipt, never a published binding.

Choose a finite semantic empty-state recipe plus postcommit transaction capture. `DummySnapshot.stateDS` calls nonmemoized `Snapshot.emptyDF`; forcing it again during analysis recurses and produces another plan. Spark's native createDataFrame path maps a serializer over an empty RDD. Task 2 must measure the actual public RDD/dependency topology in its failing installed integration and prove zero executable input before admitting it. Neither an exact runtime topology nor complete compatibility of `DeltaRangerCommitMetadata` with the absent-table snapshot is already proven.

Rejected approaches: treating ordinary published-reader denial as absence; a wildcard table authorization request; admitting a class/path/current ThreadLocal alone; intercepting private native startTransaction; manually creating/replaying transactions; replacing the native stage; disabling checksum, symlink or other required work.

## Global Constraints

- Change the Spark Delta base connector and optional Ranger companion only; no Gravitino core/API/server/client/storage-schema change, engine fork, manual native transaction or mutation replay.
- Use Java 17 and Scala 2.12.21; implement first on Spark 3.5.6 / Delta 3.3.2, then qualify Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2 with actual Ranger server/client 2.8.0 and MinIO.
- Consume tableProperties and settings unchanged from spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json; retain its provenance/hash and effective values, with no writer options or payload write in this schema-only profile.
- Preserve native CDF=true, log retention 45 days, deleted-file retention 364 days, optimizeWrite=true, autoCompact=true and symlink behavior; never disable native work to obtain admission.
- Request CREATE on the literal canonical catalog/schema/table with column=* and owner=null before reserve, before native execution and before publication; USE remains visibility-only and existing-table permissions remain separate.
- Authority must be privately issued, one-shot, bound to immutable intent, exact validated reservation and live session/thread/subject/catalog/settings; no class-name, path-only, ThreadLocal-only or caller-supplied callback authority.
- Preserve separate native and publication outcomes, status-needed uncertainty, terminal cleanup and no automatic retry; never report a postcommit refusal as an uncommitted CREATE.
- Do not add private native interception or new reflective transaction capture; existing qualified metadata validators may be reused unchanged only where their actual inputs are proven compatible.
- This slice qualifies only the initial schema-only profile; ordinary metadata/partition/createIfNotExists profiles remain M2 work, generated date partitions remain M5, and full Spark Hub/M2 support remains open.

## Typed base lifecycle

Proposed new base file `DeltaCreateLifecycle.java` is the cross-module contract; all public/protected members have Javadoc. `GravitinoDeltaCatalogSpark35` adds `protected DeltaCreateLifecycle newCreateLifecycle(DeltaCreateLifecycle.Intent intent)`, returning the no-op implementation by default. Only Requested.CREATE uses it. The companion override accepts only its private live invocation. Base replacements/query writes remain unchanged; companion unissued create/stage and replacement routes remain refused.

`Intent` is immutable: catalog, copied namespace/name, schema JSON, partition count and copied unmodifiable properties. The initial companion requires partitionCount=0; a shallow Transform[] or transform description string is not partition authority. The base continues supporting existing transforms with its no-op guard. Actual materialization supplies `DeltaSchemaContract.OperationMode`, actual schema JSON and an immutable options snapshot, so QUERY or altered-schema materialization cannot reuse a schema-only intent.

The lifecycle exposes `beforeReserve(OperationMode mode, String schemaJson, Map<String,String> options)`, `reserved(DeltaRegistrationReceipt receipt)`, `Scope beforeNative(DeltaRegistrationReceipt receipt)`, `beforePublish(DeltaRegistrationReceipt receipt)`, `published(DeltaRegistrationReceipt receipt)`, and idempotent `close()`. `Scope extends AutoCloseable` with unchecked `void close()`. It exposes no native Runnable, DataFrame or transaction callback. The existing base remains the only native invoker. `reserved` retains the exact immutable receipt after the existing client validates it against the actual submitted command, including its base-generated token/run and generation. The companion checks RESERVED/generation0/CREATE and the applicable schema contract, then binds that validated receipt, token/run and normalized marker location for subsequent phases. Intent does not contain the base-generated token/run and cannot independently compare those fields. Reuse companion initialize's existing `name.equals(visible.name())` check for canonical catalog identity.

Ordering is part of the contract:

1. Descriptor construction is transport/native/policy side-effect free. Before materialization's metadata probe, journal or RESERVE, call beforeReserve; denied/unavailable metadata is never translated to absence. Provider RESERVE remains authoritative for hidden reservations and races.
2. After a validated reserve receipt and RESERVED journal, call reserved before native stage construction. Before entering native commit, obtain Scope; rejection must precede setting the native-invoked flag. Set that flag immediately before the existing native call, never retry it, and close Scope in finally.
3. On native return, set nativeCompleted immediately and record NATIVE_COMMITTED before any potentially failing post-return guard/cleanup or publication. Preserve the original error with cleanup errors suppressed. A thrown native call can already have CREATE0; retain NATIVE_STARTED/status-needed evidence rather than claiming rollback.
4. Call beforePublish after NATIVE_COMMITTED and before any PUBLISH intent/submission. Mark the publication attempt terminal even if the guard rejects. Keep the existing validated receipt/journal path, then call published. Close lifecycle once on terminal success, failure or abort; never erase a receipt, drain a run or retry native/PUBLISH automatically.

No-op regression coverage must preserve existing base schema/query/replace behavior and descriptor semantics. Lifecycle observers cannot cause a successful native return or validated PUBLISHED receipt to be forgotten merely by throwing afterward. A final loadTable SELECT denial after publication is an observation failure, not CREATE failure; the issued command uses the staged lifecycle and does not require the base createTable convenience method's final load.

## Issued command and policy

Recognize only resolved Spark `CreateTable` with `ResolvedIdentifier` for the exact configured `DeltaRangerCatalogSpark35`, no query and the initial profile. Freeze declaration and stock-compatible property conversion (`CatalogV2Util.convertTableProperties`), then replace it with a private issued carrier/physical V2 command. Reject unsupported TableSpec fields before conversion. Execute the exact base `stageCreate` and `commitStagedChanges` under the issued catalog invocation; do not use stock tableExists or an ordinary loadTable as an absence oracle. Check successful publication through the lifecycle receipt, not a SELECT privilege.

Add `DeltaRangerCreateCommand.scala` for carrier, execution and identity validation, and `DeltaRangerCreateMutation.scala` for reservation/native ownership. Scope transport follows the private invocation pattern in `DeltaRangerCatalogSpark35.withCdfAlter`; a scoped value carries a private owner and cannot independently grant authority. Pin exact catalog identity, identifier, schema/properties, Spark session/context, thread, subject/groups, settings and one-shot carrier/physical execution. Reject copied/serialized/stale/foreign carriers and nested native operations. Keep existing read admissions intact.

Use `DeltaRangerResourceKey(catalog, schema, table, null, "create", null)` and the existing policy engine at each operation boundary and existing native continuation check. Schema wildcard policy coverage must still match a literal target request so a table-specific Ranger deny applies. Each check reauthorizes the same immutable literal CREATE against a complete current publication; the engine requires that publication to remain stable throughout that evaluation. A completed refresh between checks may continue only if effective CREATE remains allowed. An overlapping refresh, effective deny or unavailable/invalid publication fails closed without automatic retry. This closed schema-only operation has no payload, source query or policy-derived filter/mask plan; its structural native proof remains bound independently of policy publication identity. Read, append, MERGE and CDF snapshot semantics remain unchanged.

Policy revocation before reserve has no side effects; before native leaves a reservation; before publication leaves CREATE0 unpublished. A failure during postcommit native work may leave CREATE0 and an UNKNOWN driver outcome; retain independent durable evidence and never replay it. No table owner is fabricated to obtain initial CREATE. The existing TableHookDispatcher.createTable hook assigns the current actor ownership after RESERVE, before PUBLISH; the Delta authorization plugin inherits ownerMappingRule=ALL. Removing only the schema grant after reservation can therefore leave effective CREATE allowed by the direct owner policy item. Boundary-denial tests must remove all effective authority or apply an explicit literal-table deny; fresh-name grant/revoke tests remain separate. Normal owner-policy propagation must not itself abort an otherwise authorized CREATE. Checks do not make policy publication atomic with native storage effects or guarantee progress during continuous refresh. Higher-scope CREATE grant mappings are not added here.

## Finite native continuation

The CREATE owner can open `DeltaRangerNativeOperation.commit(CommitOwner)` around the closed real native stage. Only its validated reservation can activate that scope. Capture the naturally active transaction at the first qualified postcommit callback and retain exact transaction/log/snapshot identities. Correlate its absent-table baseline, metadata/protocol and native transaction identity with the actual stable CREATE0 marker and reservation. This is continuation validation; pre-native authorization already occurred. Never force snapshot.stateDF/stateDS to discover ownership.

A dedicated `DeltaRangerCreateMetadata.scala` proves the initial `LogicalRDD`: expected Action.logSchema/output, non-streaming, exact Spark session/context, the measured stock conversion/dependency topology ending in an empty root, zero partitions throughout and no extra branch. Read public graph information without executing a job. Retain accepted plan/RDD/dependency identities and revalidate them at use. The proof is semantic no-input: the conversion closure cannot execute. A class-name check, arbitrary zero-output filter, user-supplied empty plan or unknown zero-partition parent is insufficient. The surrounding private closed schema-only invocation is essential.

Wire this owner into the existing admission, strategy and physical provenance dispatch. Reuse `DeltaRangerCommitMetadata`'s retainedState/log/checksum/reconstruction consumers only after focused evidence shows their exact compatibility; otherwise implement the smaller finite empty/log0 consumer in the CREATE metadata file. Bind log files, versions, native ID, metadata/protocol and exact query/physical execution; a directory prefix is not permission. Do not broaden existing append/CDF/merge recipes. Unknown plans or changed graphs fail closed with actual committed outcome retained.

## Acceptance and limits

The current companion qualifies only SIMPLE application authentication. Spark/Ranger can evaluate the actor's UGI groups, but Gravitino's SimpleAuthenticator constructs a REST principal without groups. A group-only CREATE grant can therefore pass Ranger and fail public RESERVE with HTTP 403 before provider/native execution. Retain this as an unsupported end-to-end identity profile, not successful group creation or evidence that Ranger denied it. Public group-role removal must still deny a fresh target before RESERVE. Successful creation qualification uses the supported direct-user grant route. OAuth/token-to-UGI binding or another group-capable authentication route requires separate identity qualification; do not bypass either authorization layer, change core authentication or infer group success from direct-user tests.

Task 2's first installed test supplies the remaining runtime topology and consumer evidence while failing closed, then becomes the successful production regression. If public no-input/commit correlation cannot be proved, stop that implementation with exact evidence; do not weaken authority or create another open-ended characterization milestone. An unproven private callback is not itself a blocker.

Retain three independent outcome axes: native NOT_INVOKED / CREATE0_CONFIRMED / UNKNOWN; publication NOT_ATTEMPTED / PUBLISHED / STATUS_NEEDED; final observation success/failure. Successful native return and positive CREATE0 evidence remain distinguishable when the native call throws in postcommit work. Actual provider verification, not scripted PUBLISHED metadata, establishes live publication support. Existing status/handoff/drain recovery rules remain unchanged.

The final qualification uses fresh actor/role/names/MinIO prefixes and downloaded real Ranger2.8 policies, with denied → schema grant → success → revoke and literal-table deny controls. Run the focused failure matrix and actual-service grant lifecycle on the initial pair, then the decision-relevant live create/deny/native-work/publication checks on the other three pairs using one verified artifact assembly strategy. Deterministic mid-operation faults remain focused lifecycle tests unless a harness-only live barrier is separately verified; do not claim that local timing tests exercised actual services. Preserve exact commands, inputs, class/JAR origins, log0/marker, registration receipts/journal and positive absence checks for denied cases. Existing native-only CREATE0/WRITE1 matrices and unchanged broad predecessor suites are references, not required reruns.
