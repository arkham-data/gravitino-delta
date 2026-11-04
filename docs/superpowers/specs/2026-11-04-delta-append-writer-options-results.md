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

# Governed append writer options: installed results

On November 4, 2026, the finite governed append writer-option continuation passed the focused installed selection on all four Spark/Delta pairs: **33 required methods in five classes per pair, 132 recorded executions, zero failures, errors or skips**. Task 2 executed the remaining three pairs sequentially (99 methods) and reused the source-identical Task 1 Spark 3.5.6 / Delta 3.3.2 evidence (33 methods).

This qualifies the [finite writer-option design](2026-11-04-delta-append-writer-options-design.md) against existing local policy fixtures and native Delta transactions. It does not establish new packaged, live Ranger server or MinIO qualification, full Spark Hub defaults, or full M2 completion. Historical empty-option and append-AQE results retain their original scope and were not rerun.

## Frozen source and inventory

The producing source is reviewed commit `a0e4937ba976d346e72f5e5622baff13bc6a1142`. All **159 inventoried source/resource entries** byte-match the reused final Task 1 run (`green-final-attempt4`), each remaining run's pre-invocation snapshot, the current workspace and that commit. Protected source/build/dependency paths have no changes in Task 2. The source manifest SHA-256 is `b9e1d3c97d82d1b1a31930bfd4de14bace44a49a897c3f335e1fb0238e70e167`.

The exact 33-method inventory was fixed before the remaining runs in `expected-methods.json`, SHA-256 `98af92b72a01593a834caa8a6a30dc82c94d7c7bed9b10cc4368d8626e96469a`. Independent parsing compared the complete `(class, method)` set, rejected missing/additional/duplicate cases, checked every testcase outcome and checked suite failures/errors/skips. Acceptance was not reconstructed from new output.

The [append option resource](../../../spark-connector/v3.5/spark-delta-ranger/src/test/resources/append/spark-hub-options.json) is byte-identical to its reviewed [native-create source resource](../../../spark-connector/v3.5/spark-delta/src/test/resources/native-create/spark-hub-options.json), SHA-256 `789fbd8fa8f22f85daa6105c233c0f34bf239bcfc5b62d7044708ccea9030f18`. It retains Spark Hub revision `d01024b6f459ad2743b7b55099226b8c3dc742fd` and original source/capture/result hashes. Tests consume its string-valued `writerOptions`; they do not serialize `actualOptions` or apply the captured maintenance defaults. Earlier native creation evidence supplies provenance, not governed append proof.

## Four-pair result

Every original Gradle command exited 0. Every cell has an actual `[TEST-TIMING] START`/`END` for `:spark-connector:spark-delta-ranger-3.5:test`, completed original XML, and observed runtime pins matching its requested pair. Each XML file's modification time and suite timestamp belong to its producing command; cached or stale XML is not used as execution evidence.

| Spark | Delta | Required methods | Failures / errors / skips | Test task seconds | JUnit seconds | Evidence |
| --- | --- | ---: | --- | ---: | ---: | --- |
| 3.5.4 | 3.3.0 | 33 | 0 / 0 / 0 | 156.971 | 149.005 | Fresh original Task 2 |
| 3.5.4 | 3.3.2 | 33 | 0 / 0 / 0 | 163.076 | 155.685 | Fresh original Task 2 |
| 3.5.6 | 3.3.0 | 33 | 0 / 0 / 0 | 195.354 | 187.848 | Fresh original Task 2 |
| 3.5.6 | 3.3.2 | 33 | 0 / 0 / 0 | 203.120 | 194.673 | Reused original Task 1 |

| Class in `org.apache.gravitino.spark.connector.delta.ranger` | Methods in every pair |
| --- | ---: |
| `TestDeltaRangerAppendCommandBinding` | 8 |
| `TestDeltaRangerAppendIdempotency` | 8 |
| `TestDeltaRangerAppendOptions` | 3 |
| `TestDeltaRangerAppendV1Input` | 4 |
| `TestDeltaRangerNativeGovernedAppend` | 10 |

All methods listed below passed individually in **each** of the four cells; the per-pair receipts retain each original testcase name, outcome and time.

### TestDeltaRangerAppendCommandBinding

- `actualNamedFrontendsBindTheirOwnTargetsAndReceiptedRoots()`
- `actualTargetCannotBorrowOtherWrapperOrIndependentLoad()`
- `captureRejectsQualifiedSourceAtUnqualifiedCommandStages()`
- `finiteWriterOptionsBindBothNamedFrontends()`
- `malformedNativeFlagsFailBeforeOpeningDuplicateTransaction()`
- `missingRootReceiptCannotBeReplacedByChildTags()`
- `retainedAssociationRejectsCopiesAndDifferentInputs()`
- `retainedOriginalOptionsCannotMutateTheirCapturedSnapshot()`

### TestDeltaRangerAppendIdempotency

- `actualColdFrontendsReturnOnlyNativeScalar()`
- `associationRejectsUnqualifiedResetBeforeOpeningTransaction()`
- `changedPendingMetadataRefusesBeforeNativeCommit()`
- `changedPendingProtocolRefusesBeforeNativeCommit()`
- `duplicateCannotHideMalformedWriterBoolean()`
- `duplicateRequiresOriginalSourceAndTargetAuthority()`
- `emptyRepeatedAndSelfSourceKeepTheirOriginalObligations()`
- `finiteScalarRequiresExactLiveOwnerAndLeavesItLive()`

### TestDeltaRangerAppendOptions

- `bothNativeBooleanAccessorsValidateEagerly()`
- `finiteOptionsPreserveEverySuppliedStringAndDetachRetainedMap()`
- `unknownNullCollidingAndWriterTransactionOptionsAreRefused()`

### TestDeltaRangerAppendV1Input

- `changedOptimizedFieldsCannotBorrowCapturedAuthority()`
- `deniedInitialSourceAndTargetNeverReachInput()`
- `nativeSourceNormalizationRemainsExplicitlyDenied()`
- `schemaFlagsAndAutoMergeCannotAdmitNullableIntoNativeNonNullableTarget()`

### TestDeltaRangerNativeGovernedAppend

- `explicitFalseSchemaFlagsPreserveMatchingSchema()`
- `explicitTrueSchemaFlagsPreserveMatchingSchemaWithAdaptiveExchange()`
- `installedNamedAppendCommitsMaskedFilteredSource()`
- `nativeCheckViolationRetainsItsExceptionBeforeCommit()`
- `nativeCheckpointHookWritesThresholdArtifacts()`
- `nonNullableSourceStillAppendsIntoNullableTargetWithFlags()`
- `sourceRevocationAfterHelperPreservesUncommittedFiles()`
- `sparkHubWriterOptionsInsertIntoPreservesNativeMetadata()`
- `sparkHubWriterOptionsSaveAsTablePreservesNativeMetadata()`
- `targetRevocationAfterHelperPreservesUncommittedFilesWithOptions()`

## Behavioral evidence and native outcomes

The selected cases verify both actual named frontends with the original metadata string, differing session metadata, source masks/filters and the two null-default branches; explicit false/true schema flags; real adaptive exchange input; directional nullability; name/order/type refusal despite flags; immutable original/successor option ownership; malformed/unknown/null/colliding/transaction options; original source/target authority before duplicate suppression; source and target revoke ordering; both real pending-state fences; and existing checkpoint, native invariant, empty-option and receipt boundaries.

Independent inspection of the eight original native commits from the two Spark Hub metadata cases across four pairs confirmed `WRITE`/`Append`, the exact Python dictionary string taking precedence over `different session metadata`, the requested Spark/Delta engine identity, one new target log version and no published metadata/protocol action. Original native logs, files, events and before/after manifests remain retained.

For every pair, all 43 retained native trace contexts have an explicit before/after target-log classification: **eight committed, 35 not committed, zero unknown**. These are trace-context observations, not application attempt counts: a context can contain negative probes and a positive control. Seven advancing contexts are the successful named-append cases; the eighth is the admitted positive control in `changedOptimizedFieldsCannotBorrowCapturedAuthority`. Existing setup versions are excluded. Refused attempts and duplicate/scalar-only probes do not add target log versions. Source/target revocation may leave uncommitted helper files; unchanged native log heads distinguish that from a commit. No automatic replay occurred.

Both pending metadata/protocol cases record successful native pending updates with changed effective state before the new refusal. Their before/after storage manifests are byte-identical, the target head does not advance, and the owner remains valid until normal cleanup. They are **not committed**. This fence compares changed effective public metadata/protocol to the retained snapshot; it does not detect redundant identical pending actions or arbitrary mutation during native commit preparation, and it is not a general transaction firewall.

## Runtime and original invocation

Loaded test workers report Java **17.0.16**, Scala **2.12.21**, and the requested Spark/Delta versions. `RangerBasePlugin` is loaded from the **2.8.0 client JAR**. No live Ranger server is implied. The actual Maven JAR origins, including their full absolute paths, are retained per pair; the distinct artifact hashes are:

| Loaded artifact | SHA-256 |
| --- | --- |
| `spark-sql_2.12-3.5.4.jar` | `06baa5b5190831a1d075eeebb7aa02c872dee91975f97da6a2fb6c5deeff49ae` |
| `spark-sql_2.12-3.5.6.jar` | `37535c443d428a8728c21a1317faf5652179748dddc131da494f0377832daa08` |
| `delta-spark_2.12-3.3.0.jar` | `c6e93686b24e067d9645aa5f04b06fdfa938f18336f713b7f138994f9890ec9e` |
| `delta-spark_2.12-3.3.2.jar` | `4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85` |
| `ranger-plugins-common-2.8.0.jar` | `10ad212120866d79566a595ff1282e92a6e43e1460eab558ef0ad4ec1e27d59e` |
| `scala-library-2.12.21.jar` | `e2f4040c95cc4dd1cfeaae38bdde596e4b36933632451efa7d8564c6a359510c` |

Task 2 reused the exact Task 1 selector argv below, changing only `deltaSparkVersion` and `deltaVersion` for each remaining pair. Java 17, a 2 GiB Gradle daemon, one Gradle worker and the existing 4 GiB test JVM convention remained unchanged. A fresh absolute evidence directory was created before each command and assigned to `GRAVITINO_DELTA_INSTALLED_APPEND_EVIDENCE_DIR`. Tests were sequential, with original evidence copied before the next command could replace Gradle outputs.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
GRAVITINO_DELTA_INSTALLED_APPEND_EVIDENCE_DIR=/absolute/fresh/existing/native \
./gradlew :spark-connector:spark-delta-ranger-3.5:test \
  --tests '*TestDeltaRangerAppendOptions' \
  --tests '*TestDeltaRangerAppendCommandBinding.finiteWriterOptions*' \
  --tests '*TestDeltaRangerAppendCommandBinding.malformedNativeFlags*' \
  --tests '*TestDeltaRangerAppendCommandBinding.retainedOriginalOptions*' \
  --tests '*TestDeltaRangerAppendV1Input.schemaFlagsAndAutoMergeCannotAdmitNullableIntoNativeNonNullableTarget' \
  --tests '*TestDeltaRangerAppendCommandBinding.actualNamedFrontendsBindTheirOwnTargetsAndReceiptedRoots' \
  --tests '*TestDeltaRangerAppendCommandBinding.captureRejectsQualifiedSourceAtUnqualifiedCommandStages' \
  --tests '*TestDeltaRangerAppendCommandBinding.retainedAssociationRejectsCopiesAndDifferentInputs' \
  --tests '*TestDeltaRangerAppendIdempotency.changedPending*' \
  --tests '*TestDeltaRangerAppendIdempotency.actualColdFrontendsReturnOnlyNativeScalar' \
  --tests '*TestDeltaRangerAppendIdempotency.duplicateRequiresOriginalSourceAndTargetAuthority' \
  --tests '*TestDeltaRangerAppendIdempotency.duplicateCannotHideMalformedWriterBoolean' \
  --tests '*TestDeltaRangerAppendIdempotency.associationRejectsUnqualifiedResetBeforeOpeningTransaction' \
  --tests '*TestDeltaRangerAppendIdempotency.finiteScalarRequiresExactLiveOwnerAndLeavesItLive' \
  --tests '*TestDeltaRangerAppendV1Input.changedOptimizedFields*' \
  --tests '*TestDeltaRangerAppendV1Input.nativeSourceNormalizationRemainsExplicitlyDenied' \
  --tests '*TestDeltaRangerAppendV1Input.deniedInitialSourceAndTargetNeverReachInput' \
  --tests '*TestDeltaRangerNativeGovernedAppend.sparkHubWriterOptions*' \
  --tests '*TestDeltaRangerNativeGovernedAppend.explicit*SchemaFlags*' \
  --tests '*TestDeltaRangerNativeGovernedAppend.nonNullableSourceStillAppendsIntoNullableTargetWithFlags' \
  --tests '*TestDeltaRangerNativeGovernedAppend.sourceRevocationAfterHelperPreservesUncommittedFiles' \
  --tests '*TestDeltaRangerNativeGovernedAppend.targetRevocationAfterHelperPreservesUncommittedFilesWithOptions' \
  --tests '*TestDeltaRangerNativeGovernedAppend.installedNamedAppendCommitsMaskedFilteredSource' \
  --tests '*TestDeltaRangerNativeGovernedAppend.nativeCheckpointHookWritesThresholdArtifacts' \
  --tests '*TestDeltaRangerNativeGovernedAppend.nativeCheckViolationRetainsItsExceptionBeforeCommit' \
  --tests '*TestDeltaRangerAppendCommandBinding.actualTargetCannotBorrowOtherWrapperOrIndependentLoad' \
  --tests '*TestDeltaRangerAppendCommandBinding.missingRootReceiptCannotBeReplacedByChildTags' \
  --tests '*TestDeltaRangerAppendIdempotency.emptyRepeatedAndSelfSourceKeepTheirOriginalObligations' \
  -PdeltaSparkVersion=3.5.6 \
  -PdeltaVersion=3.3.2 \
  -PskipITs \
  --max-workers=1 \
  -Dorg.gradle.jvmargs=-Xmx2g
```

## Retained receipts and diagnostics

The original evidence root is the retained worktree's ignored `.superpowers/sdd/2026-11-04-delta-append-writer-options/`. `task2-evidence-index.json` links all four pair receipts, the final source equality receipt, independent writer-metadata checks and native log classifications. Each pair receipt retains SHA-256 values for its original command/log/exit, five XML files and all 3,729 native evidence files, plus loaded JAR origins and individual testcase outcomes. It is an evidence index for this fixed selection, not a new acceptance framework.

| Pair | Original directory under the evidence root | Pair receipt SHA-256 |
| --- | --- | --- |
| 3.5.4 / 3.3.0 | `matrix-spark-3.5.4-delta-3.3.0-attempt1` | `8a72954057c5d45526a2c9eb97dd9bb6e847d3cbfc275c82a8b6c2ae2a1bc870` |
| 3.5.4 / 3.3.2 | `matrix-spark-3.5.4-delta-3.3.2-attempt1` | `5e6c9c39c7763af3399b0a7e3993e8f10d4e571dc1203ae4ea1511b4e528b427` |
| 3.5.6 / 3.3.0 | `matrix-spark-3.5.6-delta-3.3.0-attempt1` | `871d6155426152f0fee852f382f6e3b63f42931a4d752f5c4a19fdb20f56db9c` |
| 3.5.6 / 3.3.2 | `green-final-attempt4` | `c446f6314a5b4a55d29dbd0c41e0de6dc5274df6167cc4228731a8d33d6571ad` |

Pair receipt filenames are `task2-spark-<spark>-delta-<delta>-receipt.json`. Original Task 1 attempts and first-pair evidence were not overwritten. Early Task 1 pre-test failures are not used; their known stale XML remains classified in the Task 1 report.

Original diagnostics remain visible: existing unchecked Java compilation notes, fruitless-type-test and feature warnings, Delta 3.3.0's deprecated `DeltaLog.startTransaction` fixture warning, NativeCodeLoader fallback, plan truncation, anonymous fixture group mapping and Gradle deprecation notices. Each cell's native CHECK-constraint negative case emits five Spark ERROR log lines and the expected `DeltaInvariantViolationException`; the corresponding JUnit case passes and native evidence shows no commit. These expected diagnostics were not suppressed or counted as test failures.

## Remaining qualification and Spark Hub direction

Glue is completely replaced. Adapt Spark Hub to existing Gravitino named catalogs, publication ordering and authorization contracts, keeping custom connector development limited to necessary Delta gaps. Gravitino remains the registration owner and Delta owns native files and transactions; this result adds no second metastore or lifecycle owner.

The next bounded task is a **focused packaged Ranger 2.8.0 / MinIO qualification of these option-aware existing-table append profiles**. The captured enabled auto-compaction, optimized-write and manifest defaults need an explicit maintenance decision and separate qualification; they were not silently disabled or presented as compatible by these writer-only tests. Enabled auto-compaction remains explicitly rejected by the current boundary.

Full [M2 ingestion and refresh](../plans/2026-10-29-spark-hub-adoption.md) remains open: initial `CREATE0 -> publication0 -> option-aware WRITE1` composition, creation/schema grants and reservation/absence authority, retained external JDBC/file/constructed and mixed-source admission, overwrite/full refresh with schema evolution and registration consistency, effective platform maintenance settings, and durable sink retry/restart recovery. Prior native characterization does not close these governed integration contracts. Wider transformed MERGE, CDF/state, feature/maintenance and streaming milestones keep their separate gates. This installed slice establishes no S3/MinIO/RustFS or new live Gravitino/Ranger equivalence.
