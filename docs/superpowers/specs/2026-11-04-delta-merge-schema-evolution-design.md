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

# Automatic additive Delta MERGE schema evolution

## Objective and selected scope

Extend the existing named SQL and classic DataFrame companion MERGE with one native atomic additive schema-and-data commit. This follows the reviewed APPEND checkpoint `5bb3af7dd4d5cd54d4c1b45e705fc83abff72998`. The user-selected sequence remains schema evolution, governed creation, inputs/transformations, overwrite/full refresh. This document covers additive MERGE; it does not close all schema evolution or the four workstreams. Before the schema track closes for incremental ELT, separately qualify ordinary governed CDF ranges spanning the admitted additive schema changes: current DeltaRangerCdfHistory requires historical schema equality. Recovery-only deferral below does not satisfy that read requirement. This remains schema correctness work, while broader CDF consumer/state redesign stays deferred.

Support the currently admitted MERGE clause/source profile on ordinary and CDF-enabled qualified targets, including partitioned targets and stock AQE. Add only concrete nullable top-level scalar columns. Preserve existing field names, order, types, nullability and metadata; preserve table identity, configuration, partition layout and protocol. Retain existing exclusions for nested assignments/additions, type widening, column mapping, generated/default/identity fields, row tracking, deletion-vector rewrite branches, unsupported target features, insert-only MERGE and NOT MATCHED BY SOURCE. A same-schema operation with autoMerge enabled retains matching-schema behavior and does not require ALTER solely because the flag is true.

The active Spark Hub caller uses updateAll/insertAll, not an ALTER followed by MERGE. Enabled evolution must use Delta's resolved assigned paths and native final schema. Disabled evolution must retain native target-column star expansion: an extra source payload field can be ignored while existing-field changes commit. Original source SELECT demands still include requested fields and predicates; native pruning does not remove those demands. Explicit assigned additions follow native resolution; do not invent a blanket direct-attribute-only RHS restriction. Preserve native type/cast/constraint checks within the admitted expression and additive profile.

## Native path and ownership

Use the existing DeltaAnalysis -> DeltaMergeInto -> PreprocessTableMerge -> exact governed MergeIntoCommand successor for both frontends. Capture the original authored command shape before native star expansion for recovery intent; it grants no authority. Use the native effective evolution flag and finalSchema/migratedSchema. A finite MERGE schema adapter may reuse DeltaRangerAppendSchema with the native final target schema and an internal enabled option, after independently verifying the effective MERGE setting. Do not pass raw MERGE source schema to APPEND's prefix validator; unused source fields are not necessarily target additions. Keep APPEND execution/authority separate and unchanged.

The expectation retains immutable base Metadata, exact expected Metadata and unchanged Protocol. Same-schema expectation is the base. Native final schema must preserve the original prefix; only additions are made nullable by native metadata normalization. No early catalog ALTER, hand-authored metadata action, replacement merger or new transaction engine is allowed.

After the existing native duplicate check and before source preparation, invoke the inherited native ImplicitMetadataOperation.updateMetadata exactly as stock Delta MERGE does when canMergeSchema is true. Before that call, the transaction must equal the retained base. Afterward it must equal the exact expected pending metadata/protocol. Bind the transition to one receipt, exact transaction/log/descriptor/readVersion and thread/session/subject/settings context; arbitrary base/expected toggling is not permitted. Retry attempts may each stage the same finite intent in their own exact native transaction under the existing attempt owner; do not reuse a prior attempt's authority.

Preserve native source materialization, touched-file selection, writeAllChanges, constraints, stats, CDF generation and file actions. Adapt only finite helper expectations required for native null aliases/post-evolution target columns. New fields in untouched or raw copy-forward rows are null; omitted old fields remain raw. Do not rebuild/collect a source with the same schema as a substitute for its original occurrence.

## Governance and named publication

Keep canonical source SELECT and target SELECT/UPDATE decisions before native defaults/copy-forward synthesis. Include every assigned added target column in UPDATE demand; INSERT/DELETE all-column inventory uses the appropriate finite final write schema. Newly absent target fields do not acquire invented old-row SELECT demands. Source masks and row filters apply to added values and aliases. Target masks still govern authored reads; target filters restrict matching, not authorized postimages. A hidden target key can yield an authorized insert, and no raw-key repair is introduced.

Actual schema change additionally requires canonical target ALTER before duplicate suppression or native metadata mutation and at existing operation boundaries, with the same captured subject/policy snapshot and current decision time. Evolving recovery must retain the original ALTER obligation even when fresh analysis sees no new schema change. Source/new-column UPDATE/ALTER denials, including time-based decisions without a stamp change, must prevent precommit mutation or reconciliation success as appropriate.

Use existing Gravitino loadTable/refresh/CAS to publish native metadata. Add only a MERGE-owned acceptance of the exact original observation or the proved own next-version observation with expected logical schema and otherwise identical registration coordinates, ID/generation/token/location/native UUID and receipt fields. Keep shared ordinary-read guards strict. Same-registration CDF writeback may accept the exact own transition only after source execution is closed, without reopening a CDF read.

Native supplied actions remain add/remove/cdc/txn only. Freeze expected metadata separately; verify CommitInfo, exactly one expected Metadata prefix when evolving, then exactly the native supplied actions, with no Protocol or extra/reordered metadata. Retain operation/transaction/version/tags/metrics/userMetadata checks and expected postcommit schema in existing checkpoint reconstruction. A metadata-only evolved commit is committed even if no payload rows change; it is not a skipped no-op.

## Durable recovery

Preserve existing format-1 fixed-schema journal fingerprints and duplicate behavior. Evolution needs a versioned intent because current fingerprints include old schema and resolved star expressions. Retain authored command shape, original and expected metadata, stable named source/target identities, source interval/projection/demands, resolved/governed semantics, subject/settings/policy expressions and exact native commit proof. Semantic final-schema identity uses the finite intent's normalized expected Metadata schema: a nonnullable new field in native analysis becomes nullable in native metadata, and a fresh retry must recognize that exact result. Independently validate each native final schema against its finite expectation; do not normalize away other changes. Original source projection types/nullability remain exact. The codec strictly validates the versioned outer record, native metadata/protocol/transition/proof fields and semantic root shapes. Nested authored/resolved/policy trees are detached comparison data; never decode them into executable plans. Require exact equality to independently generated fresh semantics from the admitted grammar before recovery, rather than maintaining a second recursive expression decoder. This evidence is private durable data, not execution authority. Fresh retries undergo ordinary named resolution and current authorization, and must match the retained intent and its exact expected schema; a changed source interval, clause, literal, projection, policy, registration, protocol, configuration or later schema change is not the same operation.

A verified native duplicate may reconcile a committed or prepared-with-exact-commit record without source materialization, files or another commit. Current named registration must be reconciled; later data-only advancement may be compatible, but cannot overwrite newer rows or excuse schema drift. UNKNOWN, absent/malformed evidence or changed intent never authorizes replay. The exact retained native commit and digest remain mandatory even if Delta's transaction marker reports a duplicate. Original ALTER and added-column UPDATE obligations survive fresh matching-schema analysis.

Same-registration evolution creates a schema boundary inside its original CDF history. Recovery must not execute that history or weaken ordinary DeltaRangerCdfHistory checks. Only a retained version-2 journal candidate may use a tightly owner-bound MERGE source policy/observation view without constructing native CDF payload. It must be unable to materialize/execute a carrier. If the native transaction marker does not establish a duplicate, reject that candidate before source work; never fall through into execution. Fresh writes and ordinary CDF reads retain eager full history/native-read qualification. Recovery may establish an already committed operation without requiring its old CDF payload files to remain readable; it grants no historical read permission. No generic recovery framework is intended.

## Outcomes and caller adaptation

Before native commit invocation, a failed operation is not committed even if orphan files exist. After invocation, an exact own log proof establishes committed; otherwise the result is unknown. Registration, policy, journal acknowledgement, native checkpoint or recache failure after exact proof is committed with observation/reconciliation unconfirmed. Preserve immutable diagnostic evidence across cleanup and original causes/suppressed failures. Do not replay either unknown or known-committed writes. No weakening of outcomes to make a test pass.

The existing companion API version 1 and Python facade can retain their signatures. Session autoMerge is the common frontend option. Spark Hub currently sets autoMerge and constraintPropagation=false after constructing its native builder; the governed adapter requires that per-operation configuration scope before handle creation and source analysis. Restore configuration in finally. An already analyzed governed source cannot be made fresh merely by moving handle construction: the application may need to establish the execution settings before the upstream read/transformation pipeline is analyzed, then keep them through the sink. Qualify that actual lifecycle during input/transformation integration. Do not loosen global settings provenance. This connector slice documents and tests the proper scope; actual Spark Hub broad-input integration remains in the selected input/transformation workstream because its active source is an ordinary pipeline DataFrame, while current MERGE accepts bounded named CDF only.

For evolving stars, filter CDF as needed and explicitly project payload columns first. Do not silently strip authored CDC columns inside the connector. No raw CDF star positive control should accidentally add _change_type/_commit_version/_commit_timestamp to the target. Native reserved-column rejection remains authoritative. Snapshot NOT MATCHED BY SOURCE deletion and broader pipeline operators remain separately tracked, not secretly dropped.

## Verification and alternatives

First prove native resolved-schema expectations against independently executed stock Delta MERGE for both pinned Delta versions. Then prove actual governed SQL/companion behavior, masks/filters/permissions, exact pending/action/publication guards, same-registration and distinct-source recovery without replay, partitioned/CDF/AQE/checkpoint operation, native constraint failures and postcommit failures. Execute affected installed behavior on all four pinned engine pairs with actual Ranger client 2.8.0, preserved runtime origins, source hashes, XML method identities and copied native commits/rows. Native controls do not prove policies; fixture readback does not prove a deployed provider. Existing provider refresh coverage can be reused unless its code changes.

A consolidated live Ranger server 2.8.0/MinIO gate remains required for the selected capabilities. This slice does not claim a new deployed result or open the deferred broad rollout plan. Do not repeat the unchanged APPEND matrix. Run focused shared-read regressions only when implementation changes shared CDF behavior.

Selected approach: extend the existing finite native MERGE owner and its existing journal/publication boundaries. A preliminary ADD COLUMNS then MERGE is rejected because it changes atomicity and caller behavior. A replacement metastore or generic mutation framework is rejected because Delta and Gravitino already supply transaction and publication mechanisms. Narrow pure expectation reuse from APPEND minimizes duplication while keeping operation authority separate.

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
