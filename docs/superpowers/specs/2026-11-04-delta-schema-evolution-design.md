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

# Automatic additive Delta APPEND schema evolution

## Objective and selected scope

Deliver native atomic schema-plus-data APPEND through the existing named Gravitino/Ranger connector. This is the first implementation slice of the user-selected order: schema evolution, governed creation, inputs/transformations, overwrite/full refresh. Automatic additive MERGE remains required next in the schema track. Do not count this slice as all schema evolution or all four capabilities complete.

The initial profile supports both existing named DataFrame append entry points, `saveAsTable` and source-preserving `insertInto`: unchanged ordered existing names/types with existing directional nullability, followed by one or more new concrete nullable top-level columns. Native Delta makes new fields nullable even if the source field is non-nullable. Retain current admitted source/policy grammar. Require column mapping `none`, no existing-field/nested/type/partition/configuration/protocol change, and no new defaults/generated/identity/NullType/CHAR/VARCHAR or protocol-requiring types. Existing ordinary and CDF-enabled qualified target profiles may evolve without changing their protocol/features. Do not widen target-feature admission merely for this slice. Delta's special `_change_type` CDC-routing helper remains outside the admitted helper profile; other native reserved-column and invariant checks remain authoritative and can reject a candidate.

Use native mergeSchema/session-autoMerge precedence: explicit false overrides session true; absent option may use the captured session setting. Matching-schema append retains all previously admitted behavior. Schema-changing target-cache histories are not admitted in this first slice; reject them before native mutation/file generation using existing cache discovery regardless of CDF protocol or AQE setting (including ordinary targets with AQE off), while preserving matching-schema target-cache support. This is a stated initial evolution limit, not a cache-support regression for existing writes.

## Ownership and reuse

Published Gravitino columns already refresh from current Delta metadata in `DeltaRegistrationOperations.loadTable/refresh/projected`, with CAS and stable old-column IDs. Reuse this existing projection; no new metadata store, registration operation, marker, generation advance or refresh protocol. A fresh named load must expose the new schema. Old analyzed plans retain their original identities and must not acquire arbitrary newer-schema authority.

Keep the existing append association, retained source, native `WriteIntoDelta`, helper, commit owner and metadata/checkpoint reconstruction. Use public native schema APIs to derive the expected merged metadata. The native writer alone calls updateMetadata and writes/commits actions. Never pre-ALTER or split schema/data to emulate automatic evolution.

The finite schema intent stores original native Metadata/Protocol and one exact expected Metadata. Matching schema expects the original. Evolving metadata differs only in schemaString, with existing fields preserved and appended fields nullable. Do not accept arbitrary pending metadata merely because it is additive. Bind the candidate to original source schema, options, target snapshot and captured configuration; verify actual pending state equals it before commit.

The helper's expected projection/default/stats/invariant recipes must use expected metadata before the native writer updates its transaction. It must not normalize the wider source against the old schema or update the transaction early. Existing source output/attribute identity and receipt checks remain in force.

## Permissions and publication

Source SELECT, masks/filters and original column demands remain unchanged, including every added source field. Matching-schema writes retain target UPDATE. Prospective schema-changing APPEND additionally requires target ALTER using the same captured subject, policy snapshot and canonical resource, before duplicate suppression or native helper work. Both binding.hasBeenExecuted and coordinator must check it before their independent scalar duplicate reads; coordinator-only authorization is insufficient. Record the authorization on the exact transaction owner and reauthorize ALTER at existing operation boundaries with the captured subject/policy context and current decision time; this is not an atomic Ranger/Delta revocation protocol.

Extend the existing finite append publication handoff with expected native Metadata. Before publication, require the original registered observation and native snapshot exactly. After exact own-commit proof, accept only the expected logical schema at native version base+1 with identical registration coordinates, ID/generation/token/location/native UUID and all receipt fields except nativeVersion. Leave shared ordinary-read/old-schema guards unchanged.

Native helper actions remain add/txn-only and unmodified. Freeze expected pending Metadata separately. Verify the committed sequence as native CommitInfo, the exact expected Metadata when evolved, then exactly the original helper actions. No protocol action or extra/missing/reordered metadata is admitted. Preserve exact operation, transaction ID, version, tags, metrics and userMetadata checks. Existing `DeltaRangerCommitMetadata` expected-state callbacks receive the candidate metadata and unchanged protocol.

A native commit followed by named observation/recache failure is committed with observation unconfirmed, not rolled back. Retain a small private diagnostic outcome containing the exact publication witness and native transaction/version identity across commit-owner cleanup, coordinator and outer binding; it grants no renewed execution or read authority. Preserve original causes and suppressed proof/cleanup failures, including failures in maintenance completion and outer recache; classify an invoked native write without conclusive evidence as uncertain. Do not add replay, durable application journals or CREATE/REPLACE lifecycle machinery. Failures before native commit invocation are not committed. Once invoked, a failure without conclusive own-publication proof remains unknown, even when later test inspection finds no committed version. Retain any generated files in either case.

## Approaches considered

1. **Selected: extend current automatic APPEND with native-derived exact schema intent.** Directly serves actual Spark Hub default writes and preserves one native transaction. It requires coordinated updates to current helper/publication assumptions but no replacement engine.
2. Governed native ADD COLUMNS first. Smaller metadata-only feature using existing ALTER/CDF machinery, but not a prerequisite for the proven automatic APPEND/MERGE use cases. Keep it separately listed; do not substitute two commits for one.
3. New registration/evolution workflow. Rejected: existing provider refresh already owns projection and identity, so another store/protocol would add unnecessary machinery.

## Verification and boundaries

Native controls first establish both named frontends and exact public schema expectation against actual Delta commits, using existing base-module fixtures. Component tests establish the finite schema intent and publication authority. Installed connector tests must then prove actual masked/filtered evolved rows, old-row nulls, fresh named schema/readback, old column identity preservation through the existing provider, permissions/duplicates/revocation, immutable intent, exact publication, CDF-enabled targets, partition preservation, AQE and checkpoint reconstruction, and committed/unconfirmed failures.

Execute the changed installed behavior on all four pinned pairs, preserving original source/hash/runtime/log/XML evidence. Use Ranger 2.8.0 libraries. Existing deployed Ranger 2.8.0/MinIO remains the required live integration environment; a consolidated live gate for the selected write capabilities remains explicit, rather than opening the deferred broad deployment project or claiming installed tests prove server/S3 integration. Do not rerun unchanged historical matrices. Broader CDF consumer state, maintenance and streaming wait; immediate operation correctness and failure classification do not.

## Global Constraints

- Glue is completely replaced; no Glue compatibility layer is required. Adapt spark_hub to existing Gravitino catalog and authorization contracts first, and keep custom connector development limited to necessary Delta gaps.

- Target Spark 3.5.4 and 3.5.6, Delta 3.3.0 and 3.3.2, Java 17, Scala 2.12.21, and actual Ranger server and client 2.8.0.
- Use S3-compatible storage; local acceptance uses MinIO. RustFS is an optional separately recorded storage profile, not evidence of AWS S3 or MinIO equivalence.
- Gravitino owns registration. Delta owns native files, log actions, transaction semantics and table features. Do not add a second authoritative metastore.
- Do not modify Gravitino core/API/server/client/storage schemas, fork Spark or Delta, or build a replacement transaction engine.
- Use the current trusted-stock-Spark execution model. Preserve original source authorization and operation-boundary checks; do not introduce exact per-stage authorization or custom adaptive execution.
- Preserve existing Ranger row filters, masks, column permissions and canonical resources. Masks may affect keys and CDF metadata. Do not require blanket unmasking or introduce raw-key/raw-row bypasses.
- Destination row filters retain read semantics, not a new insertion/update constraint. A filtered-out target row does not automatically suppress an authorized insert.
- One writing job per dataset is enforced by the platform. This does not imply that sources cannot advance while downstream jobs run.
- Preserve platform operations and options in the acceptance requirements. Do not silently drop schema options, disable auto-compaction, delete checkpoints or weaken policies to obtain a passing result.
- Classify every outcome as committed, not committed, or unknown using native evidence. Do not automatically replay an unknown or known-committed write.
- Keep the closed append-AQE milestone separate. Do not rerun unchanged successful matrices for documentation, planning or checker-only changes.
