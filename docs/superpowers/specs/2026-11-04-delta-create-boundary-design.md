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

# Delta initial-create boundary

User authorized continued development on 2026-11-04 after the M2 characterization checkpoint `c5ab7cddc`. This is the next narrow native mechanism experiment specified in [the measured findings](2026-10-30-spark-hub-write-native-findings.md), within the existing architecture. It selects a compatible creation sequence before designing unpublished-creator authority.

## Global Constraints

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



## Decision to prove

The stock PySpark experiment omitted the adapter's existing `DeltaReplacementStage` truncate acknowledgement. Test that actual wrapper first. Exact Delta source then predicts a second rejection: `CreateDeltaTableCommand.replaceMetadataIfNecessary` rejects explicit `overwriteSchema=false` for CreateOrReplace even when absent. Do not remove that option or use session userMetadata as a workaround.

The compatible sequence to qualify at the native/evidence level is **schema-only native CREATE at version 0 -> publication evidence verified at version 0 -> initial native WRITE at version 1**. This matches Spark Hub's existing empty-create/data-write semantics. Writer options belong to the data phase; its userMetadata remains unchanged. An empty table may exist/publish before the data write, and failure of that write must remain a separate outcome.

Current registration evidence requires CREATE candidate=latest=snapshot=0. First publication after an initial WRITE advances the head to 1 cannot satisfy that contract. A first WRITE directly at 0 is also not an accepted CREATE operation. We will prove these facts using actual native logs and `DeltaCommitActionReader`, without a new registration protocol.

Alternatives: single staged CREATE drops writer metadata; wrapped CreateOrReplace still needs the unchanged-option native experiment. A new transaction engine, private-field manipulation, temporary session-option substitution, replacement of an existing target and option weakening are excluded. If measurements contradict the predicted behavior, preserve them and choose from the measured compatible paths.

## Small test scope

Add a focused native JUnit fixture in the existing Spark Delta module and a portable JSON input copied from the original validated Spark Hub option capture. Preserve its exact dictionary-string userMetadata, explicit overwriteSchema=false, mergeSchema=true, CDF=true, 45/364-day retention and symlink property. Record original capture/source hashes. This reuses captured real helper output; it is not a new Spark Hub helper or end-to-end caller run.

Run Spark 3.5.6 with both Delta 3.3.2 and 3.3.0 on Java 17/Scala 2.12.21, using isolated local filesystem targets. This characterizes the stage/protocol mechanism only. Previous MinIO results remain evidence for their original cases; this task does not extend S3, Ranger or four-pair qualification. No Docker services are required for these focused tests.

Use the real existing DeltaReplacementStage, DeltaPublicationStage, native DeltaCatalog stages and native DataFrameWriter. Publication callbacks use the actual shared commit-evidence reader against the native log; a successful callback is evidence-contract qualification, not a real Gravitino server/Ranger publication claim. No product source changes are planned.

Required observations: wrapped native rejection and no commit; empty CREATE metadata/identity and its MATCHED publication evidence; exact WRITE userMetadata/rows/properties/identity; failure of delayed CREATE verification after WRITE; a failed initial data write retaining the created empty table; and native commit/publication failure, explicit publication-only retry and terminal abort/consumption behavior using the existing stage owner. Confirm no leaked active native transaction or altered session metadata/settings. Record native state before and after each phase; no automatic operation replay.

## Completion

Focused tests pass on both selected pins with original command/exit/XML and retained native logs; independent review confirms the findings and existing API boundaries. Publish a concise measured decision and identify the next production integration task. Full governed creation, schema CREATE grants, external-source admission, safe platform retries and Spark Hub routing remain M2 work. Keep the evidence/worktree and push the reviewed checkpoint under standing authorization.
