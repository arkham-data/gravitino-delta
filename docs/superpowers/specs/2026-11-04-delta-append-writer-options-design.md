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

# Governed append writer options design

Continue Spark Hub M2 with a production building block: admit the original `userMetadata`, `mergeSchema` and `overwriteSchema` strings through existing named, matching-schema governed DataFrame appends. Initial creation still needs CREATE0 -> publication0 -> option-aware WRITE1. This change supplies the last phase on existing tables; it does not compose the full create flow.

## Chosen approach and alternatives

Use an append-specific finite validator and immutable snapshot, preserve the original native writer map, and retain session-only idempotency through an explicitly empty transaction-option projection. This is the controller's implementation choice within the approved continuation. Widening the shared native idempotency utility would mix distinct contracts; stripping schema flags would violate Spark Hub semantics. Neither is selected.

## Contract

- Existing `insertInto` and existing-table `saveAsTable` append routes accept only the three case-insensitive writer keys above, individually or together, plus empty options. Preserve exact supplied key/value strings through binding and the native helper; no JSON or boolean-value normalization. Reject null entries, unknown keys, case-fold collisions and writer txn overrides.
- Native `DeltaOptions.canMergeSchema` and `canOverwriteSchema` perform boolean validation before any duplicate suppression. Explicit false stays false; matching-schema true does not grant schema evolution or replacement.
- Freeze a detached immutable option snapshot with command identity, use it as the authoritative writer input, and require both retained original and successors to match through analyzed/optimized fallback ownership. Case folding is locale-independent and for admission only. Parse/validate at capture and before execution; never borrow changed command tags or mutable option maps as authority.
- The complete writer map reaches native WRITE, with only the pre-existing internal null-default option added. Writer metadata overrides differing session metadata according to Delta. Do not change session metadata to carry writer intent.
- Low-level native idempotency retains its existing transaction-option utility contract. This append path supplies no writer txn overrides and uses the existing session txnAppId/txnVersion pair with autoReset=false. Recheck source SELECT and target UPDATE before duplicate suppression; malformed flags fail even for duplicates. Same native transaction identity is suppression, not changed-intent retry validation.
- Preserve exact names/order/types and directional nullability (`!from.nullable || to.nullable`) plus registered source admission; non-nullable source into nullable target stays admitted. Flags and session autoMerge do not expand them. Preserve masks, filters, source demand and target policy checks, stock AQE and cache maintenance.
- Inside the existing commit cleanup region, after operation entry and argument validation and immediately before setting invoked/calling the native append commit, compare effective public transaction metadata and protocol against the bound native snapshot, rejecting changes before invocation. Keep existing explicit-action rejection and exact published intent verification. This equality fence does not detect redundant identical pending actions and is not a generalized Delta transaction firewall.
- No creation, schema evolution, external-source admission, raw-path write, SQL INSERT, writeTo.append, overwrite, automatic compaction/manifest maintenance or durable-retry expansion. Enabled auto-compaction remains explicitly rejected. The real Spark Hub defaults must not be silently rewritten and called compatible.

## Verification and release claim

Focused red/green tests use existing companion fixtures and real native transactions on Spark3.5.6/Delta3.3.2 first; then the changed installed behavior is executed on all four supported pairs. Add finite tests for option parsing/immutability, both named frontends with filtered/masked data and exact metadata, pending metadata/protocol refusal before commit, source schema mismatch despite flags/autoMerge, and duplicate/revocation ordering. Reuse the captured exact Spark Hub option fixture with explicit provenance, avoiding a production module dependency on test data.

Installed tests use the existing local policy/native fixtures; they do not establish new deployed Ranger2.8/MinIO qualification. A subsequent focused packaged live profile is required before expanding public live support claims. Keep historical qualification intact and clearly label new installed coverage. Do not rerun unchanged predecessor matrices.

## Global constraints

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
