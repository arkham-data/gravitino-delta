# Task 4: production collection-boundary primitive

Implement only the visible production root primitive required by the accepted physical-provenance design. This does not install authorization or qualify a security receipt. Read task-4-physical-provenance-design.md section6 and task-4-logical-receipt-protocol.md rendering/canonicalization follow-up. Existing TestDeltaRangerCollectBoundary is historical mechanics evidence; leave it unchanged.

## Ownership

Own new Scala DeltaRangerCollectGuardExec.scala (with small driver validation interface in same file) and new Scala TestDeltaRangerCollectGuardExec.scala only. No extension, native/logical admission, reader/registry/catalog/engine, shared fixtures, build/dependencies, live/services/git/subagents. Private staging under task-4-evidence/collect-guard-staged until root grants source/build slot. Another worker owns current Gradle and catalog bindings. Ask for slot after staging; never publish/format while it runs.

## Contract

- Package-local final ordinary visible UnaryExecNode, no CodegenSupport. Preserve child output, ordering, partitioning; row-based root above normal transitions. Do not add columnar/action coverage.
- Minimal package-local ordinary non-Product/non-Serializable driver validation interface/context: verifies actual current child provenance and complete original decision receipt. Its production implementation is subsequent integration. A test context may instrument validation, but tests cannot count as policy/registration enforcement. Do not add engine/reader dependencies or a general plugin API.
- Override executeCollect: reject absent/deserialized driver context and canonicalized node FIRST, invoke context validation of actual final child before ANY child preparation/execution, then super.executeCollect. Keep doExecute validation defense in depth then child.execute. Raw execute/prepare/RDD/iterators remain outside supported boundary; no false pre-prepare promise there.
- Context is transient, absent from Product/expression surfaces and omitted from plan string/JSON (TreeNode.jsonFields uses productIterator AND otherCopyArgs). A second constructor list alone is insufficient. Use public/protected Spark extension points, no reflection/private package bridge. Explicit otherCopyArgs/withNewChildInternal support correct Spark copies; copied child is revalidated, marker/context presence alone is not authority.
- No global registries. No context/receipt enters task closures or serialized guard authority. Canonicalized copy and deserialized copy must fail closed before child preparation, even if Spark's own execute rejection would occur later. Constructor/copy/rendering must not validate or perform I/O: validation only action paths.
- Inherited visible-child cleanup/reset traversal remains available. Do not call arbitrary child's protected cleanup via reflection; test public override or parent inherited traversal where available. No public test hook in production.

## TDD and verification

Preserve actual RED for missing behavior then implement. Focused tests must exercise production class, not a parallel test guard:
1. Real retained Dataset collect/collectAsList returns exact rows twice with same executed root and visible WholeStageCodegen child; stale context rejects before further child prepare/execute; restored unchanged context can collect again. Verify repeated collection entry separately from defense-in-depth evaluations.
2. Stale first collection rejects before a real test preparation-time SparkContext.broadcast, with zero child counters; actual downstream failure still leaves inherited cleanup traversal working. This is not qualification of real BroadcastExchange or native Delta.
3. Normal Spark clone and withNewChildren preserve context but revalidate the new child; valid clone unchanged source succeeds, unexpected replacement fails before preparation. Canonicalized guard rejects before child work.
4. Actual Java serialization roundtrip of guard (with serializable minimal test child) succeeds without serializing a deliberately non-Serializable/sensitive driver context; deserialized guard rejects before child work. Plan string/simpleString/explain/JSON outputs omit sensitive marker/context contents and no authority is exposed through Product. Be explicit about what serialization test covers; no unproved task-closure qualification.
5. No setting silently changed by production class. Test harness may explicitly configure AQE=false for this mechanics boundary, document that no AQE/native/user cache enforcement is installed.

Run bounded offline tests on both Spark patches (3.5.4/Delta3.3.0 and3.5.6/Delta3.3.2) after slot granted, then appropriate format/Javadoc. Use existing run-gradle.py/compiled-input capture/unique labels/no-parallel/max-workers2. No need broad suite absent concern.

Write task-4-collect-guard-report.md exact source hashes/minimal API/original failures and successful artifacts/limits. Independent review later. Do not claim production retained-plan enforcement, native security or Task4 complete; class stays uninstalled until full typed receipt/native physical classifier is ready.
