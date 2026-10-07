# Governed Delta append target authority implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user has already chosen continuous local subagent-driven execution; root owns release after the native gate and independent review. Do not ask for another execution-method approval.

**Goal:** Add a tested, driver-local binding and UPDATE decision receipt for the exact named append target, as the next production authority increment toward governed batch append.

**Architecture:** Extend the existing catalog-owned registration binding to retain the exact returned DeltaTableV2, and issue a separate target binding without constructing a SELECT relation or receipt. A new target receipt brackets current registration/native observations and an application-subject UPDATE decision with session, subject, full configuration and policy/role-publication checks. This increment does not admit or execute writes; source/command authority and the split-native coordinator remain a subsequent integrated gate described below.

**Tech Stack:** Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12 (existing runtime dependency 2.12.21); JDK 17 qualification; Ranger 2.8.0; Kyuubi 1.10.2; existing optional Delta authorization/Spark modules.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`. Before execution also read `.superpowers/sdd/2026-09-27-delta-governed-append/registration-contract.md`, `.superpowers/sdd/2026-09-27-delta-governed-append/policy-contract.md`, `.superpowers/sdd/2026-09-27-delta-governed-append/native-boundaries.md`, and `.superpowers/sdd/2026-09-27-delta-governed-append/production-integration-map.md`. These private reports remain at those workspace-relative paths if this plan is promoted into `docs/superpowers/plans/`.

## Global Constraints

- Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.
- No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.
- S3-compatible native storage; local MinIO qualification is permitted. Preserve existing SELECT fixture and quarantined callback cases.
- Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.
- Source and target use authenticated application subjects, never the separate metadata reader as a write principal.
- Keep accepted runtime packaging/dependency isolation. No remote push, demo change or service replacement in the initial local implementation task.
- Gravitino retains registration ownership; Delta retains transaction/log/data authority. Registration identity remains positive numeric ID + generation + correlation token, distinct from native metadata ID; the token is not bearer authority.
- Target append access is table-wide `update` on canonical catalog/database/table/column=`*`. Source SELECT and its original column/mask/filter obligations remain independent. Target SELECT masks/filters are not insertion constraints.
- All new logic has meaningful unit/component tests. Java uses normal imports, Google style, nullable annotations and Javadoc for new public/protected API. No production test switches or ambient native permissions.
- Native four-pair acceptance is a prerequisite for production edits, including Tasks 1–2. The four-pair gate is now accepted at local commit `1c0e44256`. The existing server MODIFY_TABLE → UPDATE mapping is already present; do not repeat it or change its service definition.

## Review Focus

1. Equal-path/equal-descriptor target copies and a second catalog pointing at the same native location must not inherit the first catalog's target binding (Task 1).
2. Registration or native metadata changing during policy evaluation must invalidate the result even if the initial UPDATE evaluation allowed (Task 2).
3. UPDATE-only target access must succeed without SELECT or mask/filter evaluation; SELECT-only/WRITE-only policies must not authorize UPDATE (Task 2, real-engine extension below).
4. Subject, group, arbitrary SQLConf, lease, exact native descriptor/owner binding or completed role-publication drift must reject retained target authority before another access evaluation (Tasks 1–2); fresh Gravitino ownership is not observed by the existing registration reader.
5. A valid target receipt must not make INSERT, raw-path writes, copied SELECT plans, native-package spoofing or a completed command result executable (Task 2 and subsequent integrated gate).

---

## Status and scope of this plan

Prepared read-only against the stable production sources at root-reported HEAD `447fa83cb`. The pushed finite SELECT milestone is `84172dd45`; subsequent work stays local. Native immutable input is `.superpowers/sdd/2026-09-27-delta-native-append-characterization/evidence/corrected-source-final-20261005/`, particularly `NativeAppendProbe.scala` (SHA256 `9d77c87db41d94982c634aea146acc41d7e021cc72cee24eb5e03981d8dca20d`). Read `.superpowers/sdd/2026-09-27-delta-native-append-characterization/final-delta-review.md` and `.superpowers/sdd/2026-09-27-delta-native-append-characterization/coverage-final-review-addendum.md`. Their primary evidence is 14 passing tests, 96 invocations, 41 compared native/split pairs; the source review is PASS. Root accepted all four cells at local commit `1c0e44256`, with 56/56 tests and independent source/class/runtime/file audits; see [native results](../../delta-native-append-results.md). This planning lane did not rerun the matrix. Do not read or operate on moving build outputs.

This plan intentionally stops its executable task list at a substantive target-authority boundary. It is independently testable without changing active command admission. Calling it “working append,” “append authorization complete,” or “production write support” would be incorrect. The unresolved source/configuration/native-continuation seams affect the whole command path and must be measured together rather than hidden in placeholder APIs. The later section gives exact integration acceptance obligations and the verified native call sequence, not fictional ready-made production interfaces.

The memory-search tools requested by AGENTS.md were not available in this lane's tool catalog. The existing local discovery documents were used; no network or stale GitNexus index conclusion was needed.

## Verified existing interfaces and proposed file ownership

Path aliases below are exact directory prefixes:

- **J** = `spark-connector/v3.5/spark-delta-ranger/src/main/java/org/apache/gravitino/spark/connector/delta/ranger/`
- **S** = `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/`
- **TJ** = `spark-connector/v3.5/spark-delta-ranger/src/test/java/org/apache/gravitino/spark/connector/delta/ranger/`
- **TS** = `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/`

| Existing source/interface, verified in this draft | Consequence |
| --- | --- |
| J/`DeltaRangerRegistrationBindings.java`: `Table load(Identifier, Supplier<Table>)`; `BoundRegistrationScan resolve(LogicalRelation)` | The load already has exact table/descriptor/relation/index/log/snapshot and complete published observations. `resolve` is source-only; add a separate target resolver. |
| Same file: `BoundRegistrationScan.validateCurrent(SparkSession): CurrentObservation`; `requireRetained()`, `recheckPublication()`, `requireLive()`; `CurrentObservation.requireSame(CurrentObservation)` | Reuse its retained-registration checks and bounded native validation as private metadata machinery. Neither a BoundRegistrationScan nor CurrentObservation by itself authorizes SELECT. Do not manufacture a logical source to call `resolve`. |
| J/`DeltaRangerRegistrationReader.java`: `PublishedObservation` has `identity`, `catalog`, `database`, `table`, `location`, `nativeId`, `nativeVersion`, `schemaJson`, `samePublication` | Reuse existing registration identity and publication compatibility. The private `owner` field here is reader ownership, not a table owner or Ranger principal. Table-owner policy context comes from the verified native catalog descriptor, as current SELECT extraction does. |
| J/`DeltaRangerPolicyEngine.java`: `snapshotStamp(): SnapshotStamp`; `requireSnapshot(SnapshotStamp): void`; `authorize(DeltaRangerQuerySubject, DeltaRangerResourceKey, Instant): void` | UPDATE does not need a new policy-engine API. Completed role-only publications already invalidate snapshot stamps. |
| J/`DeltaRangerResourceKey.java`: constructor `(String catalog, String database, String table, @Nullable String column, String accessType, @Nullable String owner)` | Build the target key from the retained binding with null column and `"update"`; its existing resource renderer produces column `*`. |
| S/`DeltaRangerReadRule.scala`: `canonical` uses `Option(descriptor.owner).filter(_.nonEmpty)` | Match that owner context; do not infer metadata-reader or native filesystem ownership. |
| S/`DeltaRangerDecisionReceipt.scala`: `consumeOwned(LogicalPlan, SparkSession, Boolean): DeltaRangerDecisionReceipt`; `validateAction(SparkSession): Unit` | These own source SELECT only and compare complete SQLConf. No target receipt may be attached with this source tag. |
| J/`DeltaRangerNativeOperation.java`: `validateRetained(SparkSession, BoundRegistrationScan)` and `CURRENT` reject nesting | Tasks 1–2 call current validation outside an active NativeOperation; they do not open a command-wide scope. |

**Modified production file:** J/`DeltaRangerRegistrationBindings.java`, responsible for exact catalog-returned target identity and current registration/native compatibility.

**New production file:** S/`DeltaRangerAppendTargetReceipt.scala`, responsible only for retained target UPDATE decisions and their observation bracket. No Spark Rule, Strategy, V1Write, command, plan tag or global registry in this file.

**Tests:** extend TS/`TestDeltaRangerRegistrationBindings.scala` and TS/`TestDeltaRangerRegistrationBindingsNative.scala`; create TS/`TestDeltaRangerAppendTargetReceipt.scala`. Reuse the existing `NativeSelectFixture.withBoundSpark`, `wrapper`, `bindingFixture` and `TestDeltaRangerNativeSelect.Fixture` transport double without changing their SELECT behavior. Use test-local Mockito restubbing for access-type-sensitive target cases. Extend TJ/`TestDeltaRangerPolicyEngine.java` with real Ranger-engine UPDATE cases: its ordinary private Fixture mocks the plugin, but `pinnedListenerRunsSynchronouslyAfterEngineAndCurrentContextPublication` already constructs an actual RangerBasePlugin, installs ServicePolicies/roles and cleans it up. Use that real-plugin pattern for policy matching; do not label the mocked fixture as a real Ranger policy evaluator.

**Untouched by Tasks 1–2:** extension/rules/planners, SELECT receipts and physical/native admission, coordinator/native helper, server policy mapping, build/dependencies and public APIs. Existing target load still returns the concrete native DeltaTableV2.

## Task 1: Bind the exact catalog-returned append target

**Depends on:** root acceptance of all four immutable native characterization cells. No dependency on a source receipt or command implementation.

**Files:** modify J/`DeltaRangerRegistrationBindings.java`; extend TS/`TestDeltaRangerRegistrationBindings.scala` and TS/`TestDeltaRangerRegistrationBindingsNative.scala`.

**Consumes:** the verified `load`, registration map, `BoundRegistrationScan` retained checks and `CurrentObservation` interfaces listed above.

**Produces:** these new package-private Java interfaces, nested in `DeltaRangerRegistrationBindings`:

```java
BoundAppendTarget resolveAppendTarget(DeltaTableV2 table);

static final class BoundAppendTarget {
  DeltaTableV2 table();
  CatalogTable descriptor();
  DeltaLog log();
  PublishedObservation observation();
  DeltaRangerPolicyEngine engine();
  void requireRetained();
  void requireLive();
  CurrentObservation validateCurrent(SparkSession session);
}
```

All constructors remain private. The class is neither Serializable nor a Scala Product. The returned `CurrentObservation` is a transient metadata observation tied to this target's private retained metadata anchor, not transferable authorization. `requireRetained` includes exact table identity and all frozen target metadata checks; it is stronger than merely comparing the path/name. No public catalog method needs changing.

- [x] **Write failing exact-target tests in the existing binding test class.** Its existing private `table`, `Fixture`, `fallback`, and `name` helpers are available there. Include this positive/foreign-copy core:

```scala
@Test
def appendTargetRequiresTheExactCompletedNamedLoad(): Unit =
  NativeSelectFixture.withSpark { spark =>
    val native = table(spark, "a")
    val f = new Fixture(native)
    try {
      assertThrows(classOf[SecurityException],
        () => f.bindings.resolveAppendTarget(native))
      f.bindings.load(name, () => native)
      val target = f.bindings.resolveAppendTarget(native)
      assertSame(native, target.table())
      assertSame(native.catalogTable.get, target.descriptor())
      assertSame(native.deltaLog, target.log())
      target.validateCurrent(spark)
      val equalPath = table(spark, "a")
      assertEquals(native.path, equalPath.path)
      assertThrows(classOf[SecurityException],
        () => f.bindings.resolveAppendTarget(equalPath))
      assertNotNull(f.bindings.resolve(fallback(native)))
    } finally f.close()
  }
```

- [x] **Add the full target-identity negative matrix before implementation.** Test unloaded/raw-path DeltaTableV2; equal descriptor copy on a distinct table; separate catalog binding at the same native location; historical or partition-filtered/option-bearing copies; closed owner/expired metadata lease; replacement numeric ID/generation/token/native ID/location/schema; protocol/partition/mapping drift; native version regression; concurrent close during publication read. Extend existing wrapper tests so a real `wrapper.loadTable` result resolves while the other wrapper rejects it. Keep original SELECT assertions unchanged.
- [x] **Run RED:** use the Task 1 command below. New method absence is the initial RED; after introducing signatures, preserve a behavioral RED for an equal-path copy or changed publication. Do not count only compilation failure as security evidence.
- [x] **Retain exact native identity during the existing successful load.** Add a weak `DeltaTableV2` reference to the existing private `RegistrationBinding`, supplied by `load`. Keep the map weak by descriptor identity and existing close/drain behavior. Do not store a second strong catalog-lifetime table registry. The target receipt later retains its exact table strongly only for its own lifetime.
- [x] **Implement the target resolver using existing membership checks.** Lookup by exact descriptor identity, require the stored weak table referent is the supplied object, enforce the same ordinary-load table conditions, and create a `BoundAppendTarget` with a private metadata anchor from that completed binding. This anchor can reuse `BoundRegistrationScan` for metadata checks; do not create a `LogicalRelation`, call the source resolver, evaluate SELECT, or expose the anchor as a source receipt. Example invariant inside the resolver:

```java
if (binding == null || binding.table.get() != table) {
  throw denied();
}
```

- [x] **Implement retained target metadata checks using the existing final native observation.** Freeze native descriptor owner and a defensive immutable copy of all initial native metadata configuration entries, alongside the existing schema/protocol/partition/mapping stamps. `requireRetained` rechecks original table/descriptor/log identities, ordinary-load restrictions and these frozen values. A target's write behavior can change when constraints or maintenance properties change, even with unchanged schema/protocol.

  Add a private final `Map<String, String> nativeConfiguration` field to `CurrentObservation`. Extend its private constructor to receive the defensive copy captured from the existing local `finalHead.metadata()` inside `BoundRegistrationScan.validateCurrent`, while the existing `validateRetained` operation is still active and after the existing final-head compatibility/version checks. Retain `nativeVersion = finalHead.version()` from that same object. No additional native I/O is needed. The existing `CurrentObservation.requireSame` semantics for SELECT stay unchanged; SELECT does not start comparing this extra field. Use the same private helper to freeze the initial target configuration and the final observation:

```java
private static Map<String, String> configurationSnapshot(Metadata metadata) {
  Map<String, String> values = new HashMap<>();
  Iterator<Tuple2<String, String>> iterator = metadata.configuration().iterator();
  while (iterator.hasNext()) {
    Tuple2<String, String> entry = iterator.next();
    values.put(entry._1(), entry._2());
  }
  return Collections.unmodifiableMap(values);
}
```

  `HashMap`, `Map`, `Metadata`, Scala `Iterator` and `Tuple2` already have imports in this Java file; add the normal `Collections` import. In `BoundAppendTarget.validateCurrent`, obtain `current = anchor.validateCurrent(session)`, compare `current.nativeConfiguration` to the target's frozen configuration, then recheck retained target and lease. Java sibling nested classes in this enclosing class can access this private field; no configuration/version getter is required. The existing anchor validates the final head's log and current registration and brackets native-version stability. Do not fetch `unsafeVolatileSnapshot` afterward, call a fresh unowned `log.update`, or infer the final head by reading a mutable log cache after the scope closes. This remains an observation guarantee, not protection from changes after the final observation. Compatible data-only advancement before a validation is allowed; incompatible target configuration and changes observed within the existing bracket reject.

  Descriptor-owner checks bind `descriptor.owner` on the exact retained native object and reject replacement descriptors/table copies (including a copy with a changed owner). `PublishedObservation.owner` is only reader identity. Its registration reader does not obtain a fresh Gravitino table owner, so this task neither claims nor tests detection of an ownership change solely in external Gravitino metadata. Current-owner authority, if required by the integrated append route, is an explicit downstream metadata requirement.

- [x] **Add happy and drift tests for current target observation.** Use the local Delta history fixture pattern in `TestDeltaRangerActionValidation.withHistory`: a compatible data-only advance before the first native head is accepted (including during the initial publication read), an advance between the first and final native heads rejects (including during the second publication read), registration incompatibility at either publication read rejects, a property-only native metadata change rejects, and two observations from different target bindings cannot satisfy `requireSame`. Prove no SELECT access/mask/filter requests were issued merely by binding or observing the target. Assert `DeltaRangerNativeOperation.current() == null` after success and all failures.
- [x] **Run GREEN, formatting and regression command below.** Independently review the target map's weak-reference lifecycle, authority identity comparisons, full failure cleanup and unchanged source resolution. After review, root may make a local commit with only these three files; no push.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRegistrationBindings --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRegistrationBindingsNative -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

**Task completion means:** production can recognize/observe an exact named target without granting a source read. It cannot yet authorize or execute an append.

## Task 2: Retain target UPDATE decisions with a complete observation bracket

**Depends on:** reviewed Task 1. This task changes no command admission.

**Files:** create S/`DeltaRangerAppendTargetReceipt.scala` and TS/`TestDeltaRangerAppendTargetReceipt.scala`; extend TJ/`TestDeltaRangerPolicyEngine.java` for real-engine UPDATE resource assertions.

**Consumes:** `BoundAppendTarget` from Task 1; existing subject `current(SparkSession)`, engine `snapshotStamp/requireSnapshot/authorize`, `DeltaRangerResourceKey`, and complete `spark.sessionState.conf.getAllConfs`.

**Produces:** these new package-scoped Scala interfaces:

```scala
private[ranger] final class DeltaRangerAppendTargetReceipt private (
    spark: SparkSession,
    target: DeltaRangerRegistrationBindings.BoundAppendTarget,
    subject: DeltaRangerQuerySubject,
    stamp: DeltaRangerPolicyEngine.SnapshotStamp,
    settings: Map[String, String]
) {
  def validate(session: SparkSession): Unit
}

private[ranger] object DeltaRangerAppendTargetReceipt {
  def capture(
      spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget
  ): DeltaRangerAppendTargetReceipt
}
```

The private constructor is accessible from its companion; neither object issues a plan tag, serialization reference, execution lease or generic callback. Freeze the target key privately from its canonical observation and descriptor owner. No caller-supplied resource/subject/stamp is accepted. `capture` captures current subject, engine stamp and full immutable settings, constructs the receipt and immediately calls `validate`; issuance cannot bypass validation.

- [x] **Write a failing positive test with the production engine and real named target fixture.** This tests more than a receipt-shaped mock:

```scala
@Test
def targetReceiptRequestsOnlyCanonicalUpdateAtEveryValidation(): Unit =
  NativeSelectFixture.withBoundSpark { (spark, policy) =>
    val wrapper = NativeSelectFixture.wrapper(spark, "a")
    val table = wrapper.loadTable(Identifier.of(Array("db"), "t"))
      .asInstanceOf[DeltaTableV2]
    val target = wrapper.registrationBindings().resolveAppendTarget(table)
    policy.requests.clear()
    policy.events.clear()
    val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
    receipt.validate(spark)
    receipt.validate(spark)
    assertEquals(3, policy.requests.size())
    assertTrue(policy.requests.asScala.forall(_.getAccessType == "update"))
    assertTrue(policy.requests.asScala.forall(_.getResource.getValue("catalog") == "a"))
    assertTrue(policy.requests.asScala.forall(_.getResource.getValue("database") == "db"))
    assertTrue(policy.requests.asScala.forall(_.getResource.getValue("table") == "t"))
    assertTrue(policy.requests.asScala.forall(_.getResource.getValue("column") == "*"))
    assertFalse(policy.events.asScala.exists(e => e == "filter" || e == "mask"))
    assertNull(DeltaRangerNativeOperation.current())
  }
```

Imports are the existing fixture's `Identifier`, `DeltaTableV2`, JUnit, Scala JavaConverters and SparkSession types. This does not claim downloaded Ranger policies: the transport is a request-sensitive double around the real engine.

- [x] **Add target policy and retention negatives.** Configure the test-local access answer to allow only SELECT, then only WRITE, then only UPDATE; only the third may issue the receipt. Use `policy.denied.add("a:*")` after capture to prove each later validation reevaluates UPDATE even without a changed stamp. Preserve target mask/filter fixture values and prove they are never requested or applied. Test same names under catalog `b` with separate engine and shared location; each key/engine stays canonical. In a real Ranger 2.8 plugin using the existing `setPolicies`/`setRoles` pattern, test UPDATE-only, SELECT-only, WRITE-only, wrong-catalog, target-scope deny and role/group membership with the canonical four-level key. Do not claim concrete-column deny semantics for a wildcard target request without an explicit engine result.
- [x] **Run RED:** run the Task 2 command below. Once compilable, retain an access-type-sensitive denial test that fails if the implementation mistakenly checks SELECT or never reevaluates UPDATE.
- [x] **Implement `capture` and `validate` with this exact ordering.** Private helpers may factor the repeated invariant checks; they do not add new external interfaces:

```text
capture:
  require target live/retained
  capture current application subject + groups, target engine stamp, full SQLConf map
  construct receipt and call validate(spark); return only on success

validate(session):
  require exact session object; captured full SQLConf; current user/groups; captured engine stamp
  require exact retained target, frozen native descriptor owner and target metadata configuration
  before = target.validateCurrent(session)
  authorize captured subject on canonical target UPDATE at one fresh Instant.now()
  after = target.validateCurrent(session)
  before.requireSame(after)
  repeat exact target/live, engine stamp, current user/groups, full SQLConf checks
```

The authorization call itself is the existing API:

```scala
val key = new DeltaRangerResourceKey(
  target.observation().catalog(), target.observation().database(),
  target.observation().table(), null, "update",
  Option(target.descriptor().owner).filter(_.nonEmpty).orNull)
target.engine().authorize(subject, key, Instant.now())
```

The real implementation freezes `key` once after target retention checks; the snippet illustrates its verified constructor and resource source. Never call SELECT mask/filter helpers with this key. Use fixed, non-sensitive `toString`/error descriptions and no serializable/Product receipt fields.

- [x] **Test every boundary with deterministic existing hooks.** Before validation: another session; changed user/groups via existing UGI `doAs` patterns; arbitrary time-zone change; txnVersion removal with auto-reset enabled; metadata/engine close or expiry; completed policy or role-only publication; registration replacement. All reject before access. During `policy.onRequest` or `bindingFixture.onRead`: registration generation/location/native-ID/schema change, native data/version advance, property-only metadata drift, policy/role publication, subject/configuration drift, close/expiry. All reject after the initial allow and leave native scopes cleared. Test descriptor replacement and changed-descriptor-owner copies through the target resolver, not a fictitious registration-reader owner callback. Borrow fixture callbacks from existing action-validation tests; do not add production hooks. Restoring a setting does not erase an observed failure, and target authority has no special reset exemption in this task.
- [x] **Test authority containment.** Assert the new receipt/binding classes cannot serialize and expose no plan tags or mutable principal/configuration getters. Obtain a valid target receipt, then execute actual `spark.sql("INSERT INTO a.db.t VALUES (9, 'keep', 'new')")` through the installed existing extension and assert rejection with no new Delta version or data files. Keep this fixture's isolated warehouse, inspect rows through its native test control, and snapshot filenames/bytes before/after. Raw paths and copied/unreceipted SELECT plans remain rejected. Do not attach the new receipt to a command to make this test pass.
- [x] **Run GREEN and unchanged SELECT regression suite.** Run Task 2 tests and the combined regression command below. Tests must prove current checks, not just constructor fields. Independent review precedes any local commit. Commit only the new receipt/tests and targeted real-engine test if needed; no push.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerAppendTargetReceipt --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerPolicyEngine -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

**Task completion means:** a target-only UPDATE decision has tested current observations under unchanged complete configuration. There is no command input binding, native transaction authorization, permission to call a helper, or before-commit guarantee yet.

## Verification and local integration of Tasks 1–2

Run formatting for the touched module (the repository instruction is `spotlessApply`; scope it to avoid unrelated changes), then the combined regressions. Root owns scheduling; no parallel Gradle/Spark writers in this worktree.

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessApply --offline --console=plain
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ./gradlew :spark-connector:spark-delta-ranger-3.5:spotlessCheck :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerAppendTargetReceipt --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRegistrationBindings --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRegistrationBindingsNative --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerReadRule --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerActionValidation --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerPhysicalAdmission --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerNativeSnapshotAdmission --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRetainedNativeValidation --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerRetainedQuery -PskipITs -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

After primary success, run this same finite suite serially with `(deltaSparkVersion,deltaVersion)` equal to `(3.5.4,3.3.2)`, `(3.5.6,3.3.0)`, `(3.5.6,3.3.2)`. Retain XML counts and actual worker JDK/loaded JAR bindings. Do not rerun native-only characterization or unrelated live SELECT transitions solely because this target component changed. Require unchanged-source SELECT regression outcomes, inspect the final diff, and record that command admission remains closed. Full repository tests are not a substitute for these specific authority negatives.

## Subsequent integrated append gate: required before opening command admission

This section is the dependency contract for the next implementation plan, not authorization to claim Tasks 1–2 completed it. Use the frozen native evidence to build one installed-companion local integration harness, exercising the actual extension ordering and real native coordinator. No production rule may allow AppendData until all of the following fit together. A standalone stub strategy/receipt/helper is not a working increment.

### 1. Bind the actual command input and source authority

The inspected immutable probe matches logical `AppendData` to `AppendDataExecV1` using exact table identity and its actual `analyzedQuery`. The latter is the executable V1 fallback input; `AppendData.query` or ordinary physical children alone are insufficient. Production binding must retain normalized schema/ExprIds, by-name/by-position mode, exact target, options, and the rewritten analyzed source. Reject a changed/missing `analyzedQuery`, mismatched physical `plan`, swapped options/table or an unreceipted copied root. `fastEquals` in the native test probe is characterization evidence, not an adequate production authority proof; use the existing detached exact-recipe discipline and explicit approved transformations.

For SELECT input, preserve every original occurrence/column/predicate/join authorization, then its masks/filters, including eliminated scans and self-append. A self-source occurrence of the target separately needs SELECT. Literal SQL/finite local DataFrame inputs need a distinct frozen source contract; current SELECT deliberately rejects arbitrary LocalRelation/LogicalRDD/RDD/UDF inputs. Do not loosen `DeltaRangerPlanAdmission.query` globally. Named DataFrame API support is qualified for its source shapes, not every Dataset implementation. Test SQL VALUES, SQL INSERT SELECT, `insertInto`, existing-table append `saveAsTable`, and `writeTo.append`; reject create/replace branches and raw paths.

### 2. Model native transaction-version reset without weakening SELECT

Before execution, source and command receipts must reject all external SQLConf drift. Existing `DeltaRangerDecisionReceipt`, `PhysicalPlanning.LogicalWitness` and `NativeProvenance.Witness` compare full maps. Native `hasBeenExecuted` may remove the session txnVersion on duplicate; `writeAndReturnCommitData` may remove it during SetTransaction creation. Capture before/after at those exact native-owned phases; permit only the evidenced removal with effective reset enabled and the matching selected session/option transaction identity. Unknown changes reject. Preserve the resulting session state on a later failure; do not restore an unset txnVersion or retry the write.

The integrated design must use an owner-bound continuation/reissuance for command and source validation under that exact transition. It cannot simply reuse the Task 2 receipt's `validate`, ignore txnVersion globally, temporarily reinstall old SQLConf to pass checks, or recapture all settings from the ambient session. Reset options-vs-session precedence, first/duplicate no-op and no-reset controls are compulsory. Unchanged standalone SELECT receipts retain their current strict contract.

### 3. Measure transactional self-source and narrow native operations

`DeltaRangerReadRule.prepare` currently rejects a transactional DeltaScanGenerator; self-append plans inside `withNewTransaction` can encounter it. Preserve the exact native transaction read tracking/OCC generator. Do not replace it with a Snapshot merely to satisfy the old matcher. An admitted transactional generator must be the exact transaction of the bound append target, tied to that source occurrence/log/snapshot and current registration. Test self-append under source masks/filters, denial of source SELECT despite target UPDATE, and unrelated/foreign active transactions.

`DeltaRangerNativeOperation` is non-reentrant. Source prepare/scan and target validation each open their own bounded operations; surrounding the whole helper with `prepare(sourceOwner)` would break nesting and grants no target authority for literals. The integrated owner must separate command lifetime from finite native invocation evidence and prove each helper-created invariant/statistics/snapshot plan against exact inputs/transaction lineage. Exercise cleanup after entry denial, source preparation failure, file-write failure and precommit denial. Do not allow arbitrary operations by package/class name, ThreadLocal presence, copied native tags or a callback invoked from user code. Preserve all existing native snapshot spoof negatives.

Task 2's primary installed-companion diagnostic on Spark 3.5.4 / Delta 3.3.0 found that removing the native version checksum forces a snapshot reconstruction containing `DeserializeToObject` in `getNumPartitionColumns`/`update`, which current admission rejects. Retain that failure as a separate availability boundary. The target component uses genuine native commits with their matching checksum artifacts; this does not qualify checksum-absent histories. The integrated gate must cover both checksum-bearing and checksum-absent native reconstruction without a generic encoder or native-package allowance.

Native recache can analyze non-append work in a cloned SparkSession, as the frozen probe records. That observation does not authorize arbitrary clone sessions or shared-state caches. Qualify an exact recache continuation, tied to the committed target and actual native-created plan/session, or keep the unsupported case as an explicit integration failure. No broad exemption for `sharedState` equality, all cloned sessions, native-looking plans or user cache substitution. A recache failure after commit is possible committed work and must remain so in the reported outcome.

### 4. Execute eagerly once, check entry and before native commit

The verified production candidate is a narrowly owned native V1 append substitution, preserving Spark's existing command-result caching. The immutable probe uses `AppendDataExecV1.copy(table, plan, refreshCache, wrappedV1Write)` in a columnar observer; an injected strategy is another available seam. Select one after installed extension-order tests, and update the exact planner/prefix qualification if a strategy is added. Never permit all extra strategies. A logical-only AppendData allowlist is insufficient.

The actual split call sequence below compiles in the immutable native probe. Names in it are existing native APIs, not a proposed production receipt API:

```scala
// Inside an owner that extends DeltaCommand; real entry validation comes first.
val native = WriteIntoDelta(table.deltaLog, SaveMode.Append, deltaOptions, Nil,
  table.deltaLog.unsafeVolatileSnapshot.metadata.configuration, data, table.catalogTable)
table.deltaLog.withNewTransaction(table.catalogTable) { txn =>
  // Validate exact target/current publication and this transaction snapshot here.
  if (!hasBeenExecuted(txn, spark, Some(native.options))) {
    val tagged = native.writeAndReturnCommitData(txn, spark)
    // Revalidate complete target + source authority and approved configuration transition here.
    val operation = DeltaOperations.Write(SaveMode.Append, Some(Nil),
      native.options.replaceWhere, native.options.userMetadata)
    txn.commitIfNeeded(tagged.actions, operation, tags = tagged.stringTags)
  }
}
```

Entry validation must precede native idempotency handling and file generation, and validate all source obligations even when native execution would skip. Validate the real target transaction's log/native ID/readVersion/schema/protocol/partition/mapping/configuration against current publication; a target-only current-head observation is insufficient. Before commit, revalidate subject/groups, all source and target engines/publications, source decisions/registrations and the exact effective options/config transition. Retain source validation after an empty optimized input. Allow only qualified metadata/actions; never reconstruct actions, reimplement native transaction IDs, call native `run` after the helper, or wrap a failed commit in an automatic replay. Use native cleanup and report postcommit uncertainty accurately.

The target key currently inherits the exact native descriptor's owner context, matching the existing SELECT boundary. The registration reader has no fresh table-owner field. Before claiming current-owner policy behavior for integrated append, measure where that descriptor owner originates and whether it is authoritative/current. If owner-dependent UPDATE requires refreshed Gravitino owner metadata, implement and test that observation within the allowed optional-module metadata path, or explicitly leave owner-dependent target authorization unsupported; do not infer it from reader identity, registration identity or an unchanged descriptor.

Retain exact effective options, including native null-as-default injection. Delegate `AutoCompact.getAutoCompactType(SQLConf, Metadata)` and reject enabled auto-compaction until separately qualified; native session/table/legacy precedence matters. Reject schema evolution, replaceWhere, dataChange=false, overwrite modes and unsupported inherited features/options before file writes for the initial finite production contract. The native test's successful mergeSchema control proves native behavior, not registration-safe governed schema evolution. Preserve measured ordinary userMetadata, codec/file-limit and txn options only after companion integration tests prove them; record unqualified options as remaining support gaps rather than silently drop them.

Spark SQL commands may commit before `spark.sql` returns. Test actual execution entry, not later result collection. Collecting/recollecting a completed result must never invoke insertion or reauthorize by replaying it. A retained command that has not executed must reject stale authority at execution. A new separately constructed ordinary append is another write; explicit app/version retries follow native semantics.

### 5. Integrated local and real-service acceptance

Before qualifying any frontend, installed-companion tests must measure exact committed rows, Delta versions/new actions/schema/protocol/metrics/tags and registration identity, plus full object names/bytes before/after. Required cases are target UPDATE-only success; target SELECT/WRITE-only denial; source-only denial; source filter/mask persisted in target; target read-policy independence; source=target; two catalogs with the same names/location; literal/empty input; txn first/duplicate/reset/option precedence; constraint and cast/default behavior; retained/derived source/command rejection; every entry/precommit drift listed above; eager execution/result recollection; internal cached/uncached snapshot work and cloned-session recache. Compare against isolated native controls on all four pins. Native-only success does not waive companion failures.

Entry denials require unchanged log and full storage manifest. Precommit denials after the helper require no new commit/rows but may leave generated uncommitted files; enumerate these separately and retain session reset effects. Never auto-delete or replay them. Postcommit/recache failures require durable readback and must not be described as entry denial. There is no claim of atomic Ranger/registration/Delta fencing or instantaneous revocation after the final observation.

The eventual live test must use a new writable S3/MinIO fixture, real Ranger 2.8 policy/role downloads and public Gravitino MODIFY_TABLE grants/revokes. Start subjects only from independently verified converged local/Admin/downloaded state. Preserve owner/foreign access/mask/filter policies and other-role contributions with whole-model readback. Do not modify or repair the protected SELECT/quarantined callback fixtures. Repeat success/target-denial/source-denial/source-mask-filter/target-read-policy independence on all four pins with actual runtime bindings and object evidence.

The existing module exposes guarded production endpoint configuration via `-PrangerDeltaProductionEnabled=true`, `-PskipDockerTests=false`, `-PrangerDeltaProductionGravitinoUrl`, `-PrangerDeltaProductionAdminUrl`, and `-PrangerDeltaProductionFixtureFile`. Those flags exist, but there is no governed append production test class today. Add a concrete new live harness and its explicit command only after the integrated local route is reviewed; do not run an imagined test class or reuse SELECT acceptance output as append evidence. Credentials and concrete fixture endpoints remain private. Ordinary failed-callback reconciliation is a rollout requirement; these tests do not solve it.

## Self-review and handoff

- Tasks 1–2 have concrete signatures, existing call sites, behavioral tests, commands, review boundaries and independent deliverables. No source/command admission is broadened by them.
- Every executable-task interface used later in this plan is defined above. Subsequent integration concerns intentionally have no invented production signatures until their lifecycle is measured.
- Spec coverage is partial by design: target binding/UPDATE observations are delivered here; complete source/command receipt, modeled txn reset, native continuation, commit boundary, all frontend support and real Ranger/S3 append remain explicitly unimplemented requirements of the next integrated gate.
- Existing MODIFY mapping is not repeated. SELECT regression source and strict provenance remain unchanged. Target read masks/filters remain independent from source obligations.
- No tests/builds, native operations, services, Git mutations, commits or pushes were performed while preparing this draft. Root self-reviewed the plan against the approved design and existing binding source, corrected final-head configuration capture and owner-observation scope, and promoted it after the native four-pair gate. Tasks 1–2 are now implemented and independently source-reviewed; combined verification and local acceptance are recorded below. Execution uses the user's existing continuous authorization.

## Task 1 local acceptance — October 7, 2026

Exact named-target binding passed independent spec/quality review and 45 focused
primary tests on Spark 3.5.4 / Delta 3.3.0: 21 binding, 7 native binding and 17
unchanged action-validation tests, with no failures, errors or skips. Scoped
formatting passed. Meaningful copied-target/session and native-configuration
regressions were observed before their checks passed. A stale-checksum error in
the disposable history fixture was corrected and retained separately from
production findings.

Root verified the frozen/current source hashes, exact patch, all 76 archived
artifact hashes, XML counts and 10 resolved classpath JAR hashes. The recorded
launcher is JDK 17.0.16; classpath versions are Spark 3.5.4, Delta 3.3.0 and Scala
2.12.21. This is launcher/classpath evidence, not an in-JVM loaded-class audit.
Private report/review/evidence live under
`.superpowers/sdd/2026-10-05-delta-append-target-authority/`.

At this Task 1 checkpoint, Task 2 and the combined four-pair regression were
pending; their final acceptance follows below. Target binding alone does
not authorize UPDATE, admit commands, refresh external Gravitino ownership, or
establish a policy/registration fence. Existing worktree revision-detection,
unchanged test type-check and Gradle deprecation warnings remain recorded;
no unrelated build cleanup is included.


## Task 2 and combined local acceptance — October 7, 2026

The target UPDATE receipt and its integration with the exact named-target binding
passed independent specification and quality reviews. Capture validates before
issuing a receipt; subsequent validation checks the exact session, current
application user/groups, full SQLConf, policy/role publication and retained target,
and brackets one canonical table-wide UPDATE decision with current registration
and native observations. Target SELECT, masks and filters are not evaluated.
The receipt grants no command or native execution authority.

The final unchanged source snapshot passed the following combined suite, with
zero failures, errors or skips in every cell:

| Spark | Delta | Tests | Build time |
| --- | --- | ---: | --- |
| 3.5.4 | 3.3.0 | 125 | 5m 36s |
| 3.5.4 | 3.3.2 | 125 | 5m 33s |
| 3.5.6 | 3.3.0 | 125 | 5m 53s |
| 3.5.6 | 3.3.2 | 125 | 6m 06s |

These 500 test executions cover the new receipt, real Ranger policy evaluation,
registration binding and existing SELECT/action/physical/native/retained-query
regressions across ten classes. Module-scoped formatting checks passed. Each cell
records JDK 17.0.16, Scala 2.12.21 and the requested Spark/Delta versions, including
five resolved Ranger 2.8.0 artifacts. Root independently verified all suite XML,
the six final source hashes and 15 recorded dependency JAR hashes per cell. This
is Gradle Test launcher/resolved-artifact provenance, not an in-JVM loaded-class
audit or a new live Ranger/S3 qualification.

Two fixture corrections were required and independently reviewed. The receipt's
group-change test now derives and asserts genuinely different membership and
restores the original mapping, avoiding contamination from an existing test's
shared Hadoop mapping. One binding history case explicitly refreshes the native
head before constructing its required version-1 target, because `initialSnapshot`
can reuse cached state. Original regression assertions remain intact; neither fix
changes production authorization or shared SELECT fixtures. Failed attempts,
diagnostics and original snapshots remain archived. An earlier long focused run
included 582 seconds of recorded machine sleep; final matrix commands prevented
idle sleep for their duration without changing global system settings.

Private reports, reviews and evidence remain under
`.superpowers/sdd/2026-10-05-delta-append-target-authority/`; final matrix evidence
is `evidence/matrix/attempt-3/`, including `root-artifact-audit.json`. The final
four-file revision snapshot is `evidence/task-1/history-cache-fix/`. Existing
worktree revision-detection, unchanged test type-check and Gradle deprecation
warnings remain deferred.

This closes the bounded target-authority plan locally. It does not enable append
execution, refresh external Gravitino ownership, model native transaction reset,
resolve checksum-absent reconstruction or establish an atomic authorization fence.
Source/command authority, transaction/helper/precommit integration and live
Ranger 2.8/S3 append remain the subsequent gate above. Catalog-based CDF enablement
remains the first feature priority after Delta maturity. No core changes, service
changes, deployment or remote push are part of this increment.
