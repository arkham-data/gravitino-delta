# Inert Delta AppendData Association Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking. Root released this bounded component after acceptance at `d5d0c516c`; the user has authorized continuous subagent-driven implementation. The implementer first verifies the real command inventory before constructing the association.

**Goal:** Add an independently testable driver-local association between one supplied, resolved `AppendData`, its exact registered native target, and its already-receipted analyzed source root, while every installed append remains rejected.

**Architecture:** A new package-private Scala component resolves the command's actual `DeltaTableV2` through the supplied registration-owning Ranger wrapper, issues target UPDATE authority internally, and retains the existing source SELECT receipt. It freezes command/catalog/input identity and context, and exposes only checked revalidation of that same association. It registers no Spark rule, attaches no tag, prepares no source, opens no transaction, invokes no writer and grants no execution permission.

**Tech Stack:** Spark 3.5.4/3.5.6, Delta 3.3.0/3.3.2, Scala 2.12, JDK 17 qualification, existing Ranger 2.8 engine and JUnit 5 fixtures.

**Spec:** `docs/superpowers/specs/2026-09-27-delta-governed-append-design.md`; source dependency memo `.superpowers/sdd/2026-10-07-delta-installed-append-boundaries/next-implementation-map.md`; observed boundary contract `docs/delta-installed-append-boundaries.md`.

## Global Constraints

- “Spark 3.5.4 and 3.5.6 × Delta 3.3.0 and 3.3.2; Scala 2.12; JDK 17 qualification.”
- “No Gravitino core/API/server/client/storage-schema changes; optional provider, authorization and Spark modules only. No Spark or Delta fork.”
- “Platform excludes simultaneous dataset writer jobs. This is not a policy or registration fence, arbitrary-code sandbox, or cross-system transaction.”
- “Source and target use authenticated application subjects, never the separate metadata reader as a write principal.”
- “Keep accepted runtime packaging/dependency isolation. No remote push, demo change or service replacement in the initial local implementation task.”
- Target access remains canonical UPDATE; original source SELECT columns/masks/filters remain independent. Target SELECT masks/filters are not insertion constraints.
- Create only the two files listed below. Preserve existing production callbacks, fixtures and all private evidence. Root owns Git and acceptance; one implementer owns source/build work serially.
- CDF remains the first feature priority after Delta maturity; it is outside this component.

## Review Focus

1. A relation can hold a forwarding Spark catalog while its exact native target belongs to a different, real Ranger wrapper object: test both identities and exact registry ownership, not name/path equality.
2. Policy or metadata callbacks for one participant can change another participant's context: test separate engines and final composite engine/subject/full-SQLConf checks, without claiming an atomic cross-system fence.
3. Existing source receipt validation binds a recipe rather than proving the outer command's origin: positive tests use actual observer objects; copies reject when checked against a retained association, not because the factory magically recognizes forged construction history.
4. A populated `analyzedQuery`, built `write`, or nonempty option map introduces an unqualified stage: reject at capture, even if it happens to point at the currently accepted source.
5. An empty or repeated-source plan still has complete original obligations, while a mutable literal can change underneath an unchanged root reference: retain/revalidate the source receipt and exercise those cases before calling the component complete.

---

## Scope and exact boundary

This implements only the inert association portion of acceptance item 3 in the approved append spec. It does not implement that whole acceptance item or any execution guarantee. The returned object's name is **DeltaRangerAppendCommandBinding**, and its class comment and `toString` say **association**, not permit/command receipt/authorized append. Source-continuation, SQL source issuance, literal input support, transaction-state reconstruction, reset continuation, V1 integration, columnar command tickets, entry/precommit enforcement and live S3/Ranger acceptance remain outside this plan.

Initial supported input is deliberately finite:

- Exact `AppendData` and `DataSourceV2Relation` classes; resolved, nonstreaming command/source; named native `DeltaTableV2` target retained by the supplied wrapper.
- Command `writeOptions` empty, relation options empty, `write = None`, `analyzedQuery = None`. Both by-name and by-position are accepted and frozen. **Absent analyzedQuery is an incomplete execution stage**, not a replacement instruction or source of writer authority.
- `query` has an existing root `DeltaRangerDecisionReceipt` that `consumeOwned(query, spark, optimized = false)` accepts, with at least one retained original scan obligation. Capture does not call `ReadRule.apply`, attach/copy tags or authorize a new source recipe. Later validation requires this exact query root reference and verifies its retained recipe.
- Call occurs outside any active `DeltaRangerNativeOperation` and outside an active native `OptimisticTransaction`. This component has no transaction-context contract.

The factory verifies a supplied object and underlying authority. Without a production issuer it cannot prove Spark originally constructed the outer `AppendData`; a freshly constructed identical command can receive its own inert association if every underlying check passes. The association for command A must reject command B, including an identical copy. No future consumer may use this factory result as an execution permit. Actual issuer/lineage proof is a distinct later design requirement.

The dossier proves actual `insertInto` and existing-table `saveAsTable` observations with receipted `Project` query roots. It has not recorded `write`, relation catalog/identifier/options details needed by the stricter shape checks below. The first implementation test must record these fields from the actual observed command. If either intended positive frontend falls outside this proposed finite shape, stop that positive case and report the mismatch; do not substitute a fabricated relation or broaden admission. This is an explicit observation gate, not an assertion of an already passing route.

## File map

Create only:

1. `spark-connector/v3.5/spark-delta-ranger/src/main/scala/org/apache/gravitino/spark/connector/delta/ranger/DeltaRangerAppendCommandBinding.scala`: one driver-local component and its private shape/context helpers. No Spark callback references it.
2. `spark-connector/v3.5/spark-delta-ranger/src/test/scala/org/apache/gravitino/spark/connector/delta/ranger/TestDeltaRangerAppendCommandBinding.scala`: actual observed-command fixture, association tests, context/identity rejection tests and unchanged-admission controls.

Do not modify the extension, read rule, plan admission, receipts, registration bindings, native operation, planner, columnar rules, native probe or existing characterization tests. If an existing API is insufficient, report that concrete gap instead of adding a new receipt/native hook during this task. No new dependency or build configuration is required.

## Consumed interfaces, verified from source

All application classes below share package `org.apache.gravitino.spark.connector.delta.ranger` unless stated otherwise.

| Existing API | Use |
| --- | --- |
| Spark `AppendData.table: NamedRelation`, `query: LogicalPlan`, `writeOptions: Map[String,String]`, `isByName: Boolean`, `write: Option[Write]`, `analyzedQuery: Option[LogicalPlan]` | Freeze the actual supplied command, reject unqualified stages, never synthesize analyzedQuery. Field signatures were also verified with read-only `javap` on the cached Spark 3.5.4 catalyst JAR. |
| Spark `DataSourceV2Relation.table: Table`, `output: Seq[AttributeReference]`, `catalog: Option[CatalogPlugin]`, `identifier: Option[Identifier]`, `options: CaseInsensitiveStringMap` | Require a real named target relation; freeze identities and immutable field snapshots. These signatures were verified with the same cached JAR. |
| `spark.sessionState.catalogManager.catalog(name): CatalogPlugin`; `CatalogPlugin.name(): String`; `Identifier.namespace(): Array[String]`, `name(): String` | Bind the relation's exact Spark plugin to this session's catalog manager, and compare literal catalog/database/table names with target descriptor/publication. Copy namespace contents into a Vector. |
| `DeltaRangerCatalogSpark35.registrationBindings(): DeltaRangerRegistrationBindings`; `resolveAppendTarget(table: DeltaTableV2): BoundAppendTarget` | Supplied catalog must own the exact command table in its binding registry. Never load a replacement target as part of capture. |
| `BoundAppendTarget.table()/descriptor()/observation()/engine()`, `requireRetained()/requireLive()`, `validateCurrent(SparkSession)` | Use retained target identity and canonical registration publication; current metadata/native validation remains inside the existing target receipt. |
| `DeltaRangerAppendTargetReceipt.capture(spark, target)`; `.validate(spark): Unit` | Issue this receipt internally from the resolved actual target. No caller-supplied target/receipt pair. |
| `DeltaRangerDecisionReceipt.consumeOwned(plan, spark, optimized): DeltaRangerDecisionReceipt`; `.consume(candidate, session, optimized): Unit`; `.validateAction(session): Unit`; `.revalidate(checkCandidate: () => Unit): Unit`; `.obligations` | Consume only actual analyzed source, retain complete original obligations, recheck recipe and current source policies/publications without rebuilding its authority. |
| `Obligation.binding.engine()/requireRetained()/requireLive()`; `DeltaRangerPolicyEngine.snapshotStamp()/requireSnapshot(stamp)` | Capture/check every participating engine and retained source binding around composite validation. |
| `DeltaRangerQuerySubject.current(spark)`, `.user()`, `.groups()`; `spark.sessionState.conf.getAllConfs` | Freeze full subject/groups and full SQLConf before callback-bearing work. No ignored config keys or reset exemption. |
| `DeltaRangerNativeOperation.current()`; `OptimisticTransaction.getActive` | Reject an already-active scope; assert this component does not leave a scope behind. |

The existing installed fixture uses `NativeBoundForwardingCatalog.loadTable(id)` to call `NativeSelectFixture.wrapper(SparkSession.active, name()).loadTable(id)`. Therefore the relation's `catalog` need not be the Ranger wrapper object in this fixture. Bind the exact manager plugin and the exact supplied wrapper independently. Equality of their names is only a consistency condition; authority comes from `resolveAppendTarget` accepting this exact table. No test-specific production branch or injected resolver is needed.

## Task 1: Implement and qualify the inert association

**Files:** the two new files in the file map. This is one independently reviewable task: the fixture, shape checks, internally issued target receipt and retained-source/context validation are inseparable parts of the same association contract.

**Produces:** these new package-private interfaces only:

```scala
private[ranger] final class DeltaRangerAppendCommandBinding private (
    spark: SparkSession,
    command: AppendData,
    catalog: DeltaRangerCatalogSpark35,
    shape: DeltaRangerAppendCommandBinding.Shape,
    target: DeltaRangerRegistrationBindings.BoundAppendTarget,
    targetReceipt: DeltaRangerAppendTargetReceipt,
    sourceReceipt: DeltaRangerDecisionReceipt,
    context: DeltaRangerAppendCommandBinding.Context
) {
  def validate(candidate: AppendData, session: SparkSession): Unit
  override def toString: String = "Checked Delta append association"
}

private[ranger] object DeltaRangerAppendCommandBinding {
  def capture(
      spark: SparkSession,
      command: AppendData,
      catalog: DeltaRangerCatalogSpark35
  ): DeltaRangerAppendCommandBinding
}
```

The constructor and helper types are private to the class/companion pair. The class is neither a case class nor `Product`/`Serializable`; it has no authority getters, `Reference`, TreeNodeTag, `run`, `execute`, `commit` or continuation method. The factory's catalog argument is an existing owning wrapper, not a resolver callback or a preissued receipt.

### Step 1: Add an actual-observation fixture and capture the stricter field inventory

- [x] Create the test file with ASF header, ordinary imports and a local `Observed` test case container holding `spark`, `targetPolicy`, `sourcePolicy`, `command`, `sourceRoot`, `catalog` and `trace`. Define `withObserved(frontend: String, sourceSql: String)(body: Observed => Unit): Unit`. The two frontend strings are `insertInto` and `saveAsTable`; other strings throw an assertion error in this helper.
- [x] Use the following exact existing fixture APIs: `new TestDeltaRangerNativeSelect.Fixture`, `NativeSelectFixture.catalogPolicies`, `NativeSelectFixture.extension`, `NativeSelectFixture.withSpark`, `NativeSelectFixture.wrapper`, and `InstalledAppendBoundaryProbe.fresh/observer/manifest`. Allocate separate target and source policy fixtures so cross-engine tests cannot pass merely because both participants share an engine. Before source construction set `sourcePolicy.customCatalog = "b"` and `NativeSelectFixture.catalogPolicies = Map("a" -> targetPolicy, "b" -> sourcePolicy)`; this makes changes to that source fixture's mask/filter fields effective for `b`, instead of accidentally testing its unchanged fallback NULL mask. Retain/restore the prior catalog-policy map.
- [x] The helper installs the identity-returning observer before the real companion, creates an ordinary accepted Dataset, consumes its analyzed source receipt, attempts the real frontend and retains an actual resolved command whose query is that same analyzed root. The operative code is:

```scala
val data = spark.sql(sourceSql)
val sourceRoot = data.queryExecution.analyzed
val originalReceipt = DeltaRangerDecisionReceipt.consumeOwned(
  sourceRoot, spark, optimized = false)
assertTrue(originalReceipt.obligations.nonEmpty)
val failure = assertThrows(classOf[Exception], () => {
  frontend match {
    case "insertInto" => data.write.mode("append").insertInto("a.db.t")
    case "saveAsTable" => data.write.mode("append").saveAsTable("a.db.t")
    case _ => throw new AssertionError("Unknown association fixture frontend")
  }
})
assertTrue(causes(failure).exists(_.getMessage ==
  "Derived or reused Delta plans are not qualified"))
val observed = trace.observedAppends.filter(a => a.resolved && (a.query eq sourceRoot))
assertTrue(observed.nonEmpty, "No actual receipted command/source association observed")
val command = observed.last
val relation = command.table.asInstanceOf[DataSourceV2Relation]
trace.record("association-input", s"command=${trace.id(command)} " +
  s"query=${trace.id(command.query)} table=${trace.id(relation.table)} " +
  s"catalog=${relation.catalog.map(trace.id)} identifier=${relation.identifier} " +
  s"relationOptions=${relation.options.asCaseSensitiveMap()} " +
  s"write=${command.write.map(trace.id)} analyzedQuery=${command.analyzedQuery.map(trace.id)}")
assertTrue(command.write.isEmpty)
assertTrue(command.analyzedQuery.isEmpty)
assertTrue(command.writeOptions.isEmpty)
assertTrue(relation.catalog.nonEmpty && relation.identifier.nonEmpty)
assertTrue(relation.options.isEmpty)
val catalog = NativeSelectFixture.wrapper(spark, "a")
assertSame(spark.sessionState.catalogManager.catalog("a"), relation.catalog.get)
catalog.registrationBindings().resolveAppendTarget(relation.table.asInstanceOf[DeltaTableV2])
```

- [x] Define `causes(error: Throwable): Vector[Throwable]` locally using `Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector`, matching the existing cause-inspection style. Do not catch a different first rejection and count it as this positive case.
- [x] Before frontend/association work, archive a full owned fixture-root manifest with `manifest(Paths.get(NativeSelectFixture.locations("a")).getParent, trace.directory.resolve("before"))`; archive `after` in `finally` and assert equality. Assert no active native operation or native transaction. Preserve trace/storage evidence before the existing fixture cleanup. Restore `catalogPolicies` and callback fields, close both policy engines and trace in `finally`; do not change global Spark/UGI state without restoring it.
- [x] Run only this new inventory test on the primary cell when root schedules implementation. Expected: actual supported inventories or a specifically documented observation mismatch. This first substep has no production association implementation and may pass; it is a prerequisite fixture check, not a claimed red production test.

### Step 2: Write positive association tests, then obtain the initial red result

- [x] Add `actualNamedFrontendsBindTheirOwnTargetsAndReceiptedRoots` using both helper frontends and `sourceSql = "SELECT id, secret, region FROM b.db.t"`:

```scala
Seq("insertInto", "saveAsTable").foreach { frontend =>
  withObserved(frontend, "SELECT id, secret, region FROM b.db.t") { observed =>
    val association = DeltaRangerAppendCommandBinding.capture(
      observed.spark, observed.command, observed.catalog)
    association.validate(observed.command, observed.spark)
    association.validate(observed.command, observed.spark)
    assertSame(observed.sourceRoot, observed.command.query)
    assertTrue(observed.command.analyzedQuery.isEmpty)
    assertFalse(classOf[Product].isAssignableFrom(association.getClass))
    assertFalse(classOf[java.io.Serializable].isAssignableFrom(association.getClass))
    assertEquals("Checked Delta append association", association.toString)
    assertTrue(observed.targetPolicy.requests.asScala.exists(_.getAccessType == "update"))
    assertFalse(observed.targetPolicy.events.asScala.exists(e => e == "mask" || e == "filter"))
    assertTrue(observed.sourcePolicy.requests.asScala.exists(_.getAccessType == "select"))
  }
}
```

Clear policy request/event lists immediately before factory invocation, after the Dataset/frontend observations, so target read-policy assertions measure this component. Only the separate-source fixture makes the “no target mask/filter” assertion; self-source control must expect ordinary source masks/filters.
- [x] Run the new class. A missing-class compilation failure only establishes that the API is absent. Add a compiling minimal skeleton with the stated capture/validate signatures whose capture throws UnsupportedOperationException("Append association not implemented"), then obtain and preserve a behavioral RED from the positive actual-command test. Retain source, logs and JUnit XML before the next run; root owns scheduling of the sole build lane.

### Step 3: Implement exact shape and catalog association checks

- [x] Add the production file with ASF header and class documentation stating inert scope. Implement private `Shape` as a non-authority holder of exact original command/relation/table/query/plugin/identifier references plus immutable snapshots. Define `private def denied(reason: String): Nothing = throw new SecurityException("Delta append association rejected: " + reason)` and use it for null/unsupported inputs; never render retained objects in errors.
- [x] Give `Shape` these private methods: `requireSame(candidate: AppendData, session: SparkSession): Unit` and companion factory `shapeOf(command: AppendData, spark: SparkSession, catalog: DeltaRangerCatalogSpark35): Shape`. The finite checks and canonical mapping are:

```scala
if (command.getClass != classOf[AppendData] || !command.resolved ||
    command.isStreaming || command.query.isStreaming ||
    command.write.nonEmpty || command.analyzedQuery.nonEmpty ||
    command.writeOptions.nonEmpty) denied("unqualified command stage")
val relation = command.table match {
  case r: DataSourceV2Relation if r.getClass == classOf[DataSourceV2Relation] => r
  case _ => denied("unqualified target relation")
}
if (relation.catalog.isEmpty || relation.identifier.isEmpty || !relation.options.isEmpty)
  denied("unqualified named target")
val table = relation.table match {
  case t: DeltaTableV2 => t
  case _ => denied("unqualified native target")
}
if (!(table.spark eq spark)) denied("target session changed")
val descriptor = table.catalogTable.getOrElse(denied("unnamed target"))
val descriptorId = descriptor.identifier
val catalogName = DeltaRangerResourceKey.literal(
  descriptorId.catalog.getOrElse(denied("missing target catalog")))
val database = DeltaRangerResourceKey.literal(
  descriptorId.database.getOrElse(denied("missing target database")))
val tableName = DeltaRangerResourceKey.literal(descriptorId.table)
val plugin = relation.catalog.get
val identifier = relation.identifier.get
if (catalog.name() != catalogName || plugin.name() != catalogName ||
    !(spark.sessionState.catalogManager.catalog(catalogName) eq plugin) ||
    identifier.namespace().toVector != Vector(database) || identifier.name() != tableName)
  denied("target catalog association changed")
```

- [x] Freeze `command.isByName`, full command/relation option maps, resolved/streaming state, `command.origin` value, each command/query/target-relation schema JSON, and complete output attributes. Define `attributes(output: Seq[Attribute]): Vector[Vector[Any]]` as below; reject a nonexact AttributeReference rather than accepting arbitrary expression subclasses:

```scala
output.map {
  case a: AttributeReference if a.getClass == classOf[AttributeReference] =>
    Vector(a.getClass, a.name, a.exprId.id, a.exprId.jvmId, a.qualifier.toVector,
      a.dataType.json, a.nullable, a.metadata.json)
  case _ => denied("unqualified output attribute")
}.toVector
```

- [x] `requireSame` first requires `candidate eq originalCommand`, `candidate.table eq originalRelation`, `candidate.query eq originalQuery`, `originalRelation.table eq originalTable`, and retained catalog/identifier references. Then repeat the finite/canonical checks and compare all immutable snapshots, including identifier namespace/name and catalog manager plugin identity. Source contents are verified by the retained source receipt in Step 4; do not duplicate its expression fingerprint or use `fastEquals`, `sameResult`, canonicalized plans or plan text.
- [x] Resolve `target = catalog.registrationBindings().resolveAppendTarget(shape.table)` without calling `loadTable`. Compare `target.table() eq shape.table`, `target.descriptor() eq shape.descriptor`, and `target.observation().catalog()/database()/table()` with the frozen canonical triple. Every revalidation calls `target.requireRetained()/requireLive()`, preserving the original wrapper/registry association. Do not mistake the fixture's forwarding plugin for the registry owner.

### Step 4: Implement composite context and retained decision revalidation

- [x] Implement private `Context` with the exact captured SparkSession reference, `DeltaRangerQuerySubject`, full immutable `getAllConfs` map and `Vector[(DeltaRangerPolicyEngine, DeltaRangerPolicyEngine.SnapshotStamp)]`. Define `requireCurrent(session: SparkSession): Unit` to compare exact session, user/groups, complete SQLConf and every engine stamp. Capture subject/configuration before callback-bearing receipt work; repeat those comparisons after constructing the engine vector. Engines are distinct target plus every original source-obligation engine.
- [x] Define `requireIdle(): Unit` with explicit rejection when `DeltaRangerNativeOperation.current() != null` or `OptimisticTransaction.getActive.nonEmpty`. It never closes or changes a caller's scope. Include this check at entry and before successful return.
- [x] Implement `capture` in this sequence: require idle/session/shape; freeze subject/full configuration; resolve the exact target; consume `command.query` with `optimized = false`; require nonempty original obligations; freeze all engine stamps; verify the frozen context and unchanged shape; issue `DeltaRangerAppendTargetReceipt.capture(spark, target)` internally; construct the association retaining all these objects; call its `validate(command, spark)`; return only if validation completes. Constructor/factory code must not accept an externally supplied target receipt. Failure returns no association.
- [x] Implement `validate` with this concrete order (the names are defined in the produced interface and Steps 3–4):

```scala
requireIdle()
context.requireCurrent(session)
shape.requireSame(candidate, session)
target.requireRetained()
target.requireLive()
sourceReceipt.consume(command.query, session, optimized = false)
targetReceipt.validate(session)
sourceReceipt.validateAction(session)
targetReceipt.validate(session)
sourceReceipt.revalidate(() => shape.requireSame(candidate, session))
target.requireRetained()
target.requireLive()
sourceReceipt.obligations.foreach { obligation =>
  obligation.binding.requireRetained()
  obligation.binding.requireLive()
}
shape.requireSame(candidate, session)
context.requireCurrent(session)
requireIdle()
```

The second target validation observes source-callback changes to the target; final source publication revalidation observes target-callback changes to original sources. Final composite checks catch completed policy/role publications in any participating engine and subject/fullconf drift during either participant's work. Neither receipt gains an exemption. Do not catch failures to retry, refresh a stamp, recapture a receipt or restore a native reset.

This order does **not** create a common atomic metadata/native/policy observation. External publication/native changes after a participant's last observation remain outside the guarantee. Nor does a final stamp check re-evaluate an unpublished, transport-double-only mutation of a decision after its last evaluation. Tests assert detected, observed changes and engine-published changes at specified phases; they must not claim every interleaving is fenced. Native/transaction snapshot compatibility and execution entry/precommit coordination remain future work.

### Step 5: Add exact-association, stage and ownership rejection tests

- [x] Add `retainedAssociationRejectsCopiesAndDifferentInputs`. Capture a real command, then check each copy against that association:

```scala
val command = observed.command
val association = DeltaRangerAppendCommandBinding.capture(
  observed.spark, command, observed.catalog)
val changed = Seq(
  command.copy(),
  command.copy(query = command.query.clone()),
  command.copy(isByName = !command.isByName),
  command.copy(writeOptions = Map("mergeSchema" -> "true")),
  command.copy(analyzedQuery = Some(command.query)))
changed.foreach { candidate =>
  assertThrows(classOf[SecurityException],
    () => association.validate(candidate, observed.spark))
}
association.validate(command, observed.spark)
```

These are retained-association negatives. Do not assert the factory can reject an otherwise identical new command purely because it was manually constructed.
- [x] Add `captureRejectsQualifiedSourceAtUnqualifiedCommandStages`, calling the factory on copies with `analyzedQuery = Some(command.query)`, nonempty `writeOptions`, and `write = Some(mock(classOf[Write]))`; all must fail for `unqualified command stage` before target UPDATE evaluation. Add relation-copy negatives with missing catalog/identifier, nonempty relation options, changed identifier namespace/name, and a foreign manager plugin. These input copies are negative shape tests, never positive origin evidence.
- [x] Add `actualTargetCannotBorrowOtherWrapperOrIndependentLoad`: capture with `NativeSelectFixture.wrapper(spark, "b")` must fail before target access. Also exercise a genuinely initialized, live, distinct wrapper named `a` with an independent registry: capture must reject the original actual target as not owned by that registry. This distinguishes registry identity from the wrong-name rejection; do not count an uninitialized/closed wrapper failure as that proof. Separately load the same name with `observed.catalog.loadTable(Identifier.of(Array("db"), "t"))`; assert it is a distinct native object. A copy of the original command targeting that table must reject against the retained association. Do not require the original binding to survive the extra load unless it actually does; an invalidated original is a valid rejection outcome, not a reason to mint replacement evidence.
- [x] Add `targetIdentityAndPublicationChangesRejectRetainedAssociation`. In separate fixtures mutate target registration numeric ID, generation, correlation token, native ID and columns using the existing `NativeBoundCatalogFixture.properties/columns`. The property names are `delta.registration.receipt.id`, `.generation`, `.token`, and `.nativeId`, as serialized by `DeltaRegistrationReceipt.toProperties`; the existing target receipt tests already use the same prefix. Use this concrete mutation dispatch, where `fixture = NativeSelectFixture.bindingFixture(spark, "a")` and `change` is the per-fixture case:

```scala
def mutateRegistration(): Unit = change match {
  case "id" | "generation" => fixture.properties.put("delta.registration.receipt." + change, "2")
  case "token" => fixture.properties.put("delta.registration.receipt.token", "changed-token")
  case "nativeId" => fixture.properties.put("delta.registration.receipt.nativeId", "changed-native-id")
  case "schema" => fixture.columns = fixture.columns.dropRight(1)
}
```

Perform each mutation both before validation and through `targetPolicy.onRequest` at its first UPDATE. Assert rejection, unchanged storage, no native transaction/operation, and no returned new association. Existing target receipt tests retain responsibility for exhaustive native-head/schema/property variants; this test pins their use by the composite component.

### Step 6: Add session, publication and full-configuration tests, including cross-engine callbacks

- [x] Add `sessionSubjectGroupsAndAnyConfigurationChangeRejectBeforeAccess`. After capture clear both fixtures' requests; check `association.validate(command, spark.newSession())`, Kyuubi session-user drift, genuinely changed UGI groups, and an arbitrary SQLConf key each reject with zero policy requests. Restore original UGI group mappings in `finally` using the technique in `TestDeltaRangerAppendTargetReceipt.sessionUserGroupsAndArbitraryConfigurationRejectBeforeAccess`; do not assume a chosen group differs from the original set. A subsequent unchanged validation must succeed.
- [x] Test removal of `spark.databricks.delta.write.txnVersion` as ordinary configuration drift. Configure it **before** constructing the source Dataset and capture, forwarding the helper startupOptions into NativeSelectFixture.withSpark; remove it afterward and require rejection. This component never executes idempotency and must not call this a native reset test.
- [x] Add `finalCompositeChecksRejectCrossParticipantPublications`. Define local `publish(policy, rolesOnly)` using the exact existing fixture mechanism:

```scala
private def publish(policy: TestDeltaRangerNativeSelect.Fixture, rolesOnly: Boolean): Unit = {
  if (rolesOnly) {
    val roles = policy.plugin.getRoles
    val next = policy.plugin.getRolesVersion + 1L
    roles.setRoleVersion(next)
    when(policy.plugin.getRolesVersion).thenReturn(next)
    policy.plugin.getCurrentRangerAuthContext.setRoles(roles)
    policy.listener.contextChanged()
  } else {
    val next = policy.plugin.getPoliciesVersion + 1L
    when(policy.plugin.getPoliciesVersion).thenReturn(next)
    policy.start()
  }
}
```

Use one-shot callbacks guarded by a local boolean. During a source SELECT callback publish a policy/role update in the **target** engine; during the second target UPDATE validation publish in the **source** engine. For the latter arm the callback only after capture and count UPDATE requests during `validate`, so the change occurs after the source action check. Require rejection even though the currently evaluating engine itself is unchanged. Repeat the latter phase with subject and arbitrary fullconf mutation. Reset callbacks in `finally`; each permanent publication variant gets a new fixture/association.
- [x] Add `lastSourcePublicationReadCannotStaleCompositeContextSilently`. Arm `NativeSelectFixture.bindingFixture(spark,"b").onRead` only after the second target UPDATE has completed, then publish target policy/role or mutate subject/fullconf on the next source metadata read. Require rejection by the final composite checks. Use callback phases/counters, not a guessed total read count. Do not claim a changed target registration after its final metadata observation is necessarily detected by engine checks.
- [x] Add `closedOrExpiredParticipantRejects`. Close either actual wrapper, metadata client or engine, or advance its fixture clock to `TimeUnit.SECONDS.toNanos(301)`, after capture. Require the same security/closed-lease failure classes accepted by existing target/source tests, no access when rejected at entry, unchanged storage and idle scopes. Do not swallow these failures into a successful association.

### Step 7: Pin complete source obligations and source content

- [x] Add `emptyAndRepeatedSourcesRetainTheirOriginalDecisionInventory`. Reuse `withObserved` with `SELECT id, secret, region FROM b.db.t WHERE id < 0` and `SELECT l.id, l.secret, r.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id`. Before capture retain the actual original source receipt with `consumeOwned(..., false)` and assert respectively one/two original obligations. After capture deny `b:secret` in the source fixture; `validate` must reject while target UPDATE remains allowed. Restore denial only to verify the retained source root still represents the original receipt, not a replacement. No source execution/optimization is needed.
- [x] Add `sourcePolicyAndMutableLiteralChangesRejectUnchangedCommandIdentity`. Change source mask/filter response before validation while target UPDATE stays allowed; require the existing source receipt's `action decision changed` rejection. In a separate predicate fixture, mutate the retained UTF8String literal bytes in the actual `command.query` without changing its root object:

```scala
val literal = observed.command.query.collect { case node =>
  node.expressions.flatMap(_.collect {
    case value: Literal if value.dataType == StringType => value
  })
}.flatten.find(_.value.toString == "keep").get
val bytes = literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]]
bytes(0) = 'z'.toByte
assertThrows(classOf[SecurityException], () =>
  association.validate(observed.command, observed.spark))
```

Use `SELECT id, secret, region FROM b.db.t WHERE region = 'keep'`; this is a negative mutation of a genuine accepted source, not a forged positive plan. Assert the cause concerns changed logical binding. No restoration/new receipt conceals the mutation.
- [x] Add `missingRootReceiptCannotBeReplacedByChildTags`. Remove the decision-receipt tag from a clone of the actual source root, preserve whatever tags its descendants actually contain, and call factory on a command copy referencing it; require missing root receipt rejection. Do not assume descendants carry receipts. In a separate negative forgery case explicitly place the borrowed root reference on a child and prove it cannot replace missing root authority. A copied root tag on a structurally changed projection must fail existing recipe checks. An identical cloned recipe carrying a valid existing reference is not falsely classified as a proven forged authority; the retained-association identity test already rejects using it in the old association.
- [x] Add a self-source observational case using `SELECT id, secret, region FROM a.db.t`, actual same-target frontend and its original source receipt. If both retained bindings survive the actual load order, association validation must preserve SELECT mask/filter obligations and UPDATE separately. If the real load order invalidates source retention, assert that exact existing rejection and record the case as unsupported by this inert subset; do not replace either descriptor/receipt. The previously measured independent self-source/target pairing is not proof this frontend pairing survives.

### Step 8: Prove the component never opens execution or gains ambient authority

- [x] Add `successfulAssociationLeavesInstalledAppendAndRawRoutesClosed`. After successful capture/validation, repeat actual `insertInto`/`saveAsTable` and assert their installed derived/reused rejection, observe SQL VALUES/INSERT SELECT/writeTo attempts and require existing installed AppendData rejection. Attempt raw-path append and raw-path SELECT with their respective existing rejection causes. There is no fallback allowing a command because an association object exists. Preserve before/after manifests and assert no active transaction/native operation.
- [x] For every actual SQL VALUES/INSERT SELECT/writeTo command observation from these failed attempts, record its complete input inventory and call the factory without a prefilter. Require the exact missing-root receipt rejection and independently assert root-receipt absence. A different shape/stage must fail qualification for investigation rather than silently changing the expected rejection. Do not issue source authority on their behalf. Populated literals and unreceipted sources remain unsupported.
- [x] Add `associationCannotEnterAnActiveNativeScopeOrTransaction`. Within an already valid existing source owner's `run` callback, call factory and require `active native scope` rejection; ensure the caller's owner remains responsible for its scope. Obtain that owner with `DeltaRangerNativeScans.consume(observed.sourceRoot, observed.spark, optimized = false)`. Separately use the actual command table in the following test, with no writer/commit call:

```scala
val table = observed.command.table.asInstanceOf[DataSourceV2Relation]
  .table.asInstanceOf[DeltaTableV2]
table.deltaLog.withNewTransaction(table.catalogTable) { _ =>
  val failure = assertThrows(classOf[SecurityException], () =>
    DeltaRangerAppendCommandBinding.capture(observed.spark, observed.command, observed.catalog))
  assertTrue(failure.getMessage.contains("active native transaction"))
}
assertTrue(OptimisticTransaction.getActive.isEmpty)
```

The test owns/clears that outer transaction. The component itself neither opens nor clears either scope.
- [x] Run a healthy installed SELECT control before and after association use. Expect the same existing masked/filtered rows and original request resources, proving no source rewrite or rule registration occurred. Do not collect the append source merely to create positive association evidence.

### Step 9: Focused verification, source review and finite compatibility acceptance

- [x] Root schedules primary development after preflight. The assigned sole implementer runs the focused red/green cycle; four-pair regression waits for root release after source review. Initial red/green selector:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home caffeinate -i ./gradlew :spark-connector:spark-delta-ranger-3.5:test --tests org.apache.gravitino.spark.connector.delta.ranger.TestDeltaRangerAppendCommandBinding -PskipITs -PskipDockerTests=true -PdeltaSparkVersion=3.5.4 -PdeltaVersion=3.3.0 --offline --console=plain
```

- [x] After meaningful new test failures are resolved, the sole build worker runs scoped `spotlessApply`, module `spotlessCheck`, and the new class plus unchanged `TestDeltaRangerAppendTargetReceipt`, `TestDeltaRangerDecisionReceipt`, `TestDeltaRangerActionValidation`, and `TestDeltaRangerInstalledAppendBoundaries`. This verifies all directly reused authorities and the admission boundary. No production rule changed, so do not repeat every historical physical/native suite without a new failure or specific unresolved concern.
- [x] Freeze new sources, obtain independent review, then root schedules that focused set serially on all four Spark/Delta pairs. Archive source hashes, commands, resolved/loaded dependency evidence, JUnit outcomes and private fixture manifests before the next cell. A test that expects rejection must assert the intended phase/cause and unchanged storage; a generic exception is insufficient. No live service or S3 qualification is claimed.
- [x] Confirm only the two intended new source files would be integrated, no existing main/test fixture changed, and no production caller/tag/planner/extension path references the new component. Root owns staging/commit decisions separately; the implementer performs no Git mutation, remote push, service action or cleanup.

## Plan self-review

**Spec coverage:** This task covers exact target/source association, retained-command checks, original source obligations, application identity, policy/role publication and full configuration sensitivity under the approved architecture. It deliberately leaves actual command admission, source transformation/SQL/literal issuance, transaction snapshot authority, native idempotency/config resets, source preparation, helper/precommit/commit, options/features, physical tickets, result replay and live qualification to the explicitly excluded dependencies. These are scope gaps in the overall append project, not hidden claims of completion.

**Interface consistency:** Only `capture(spark, command, catalog)` and `validate(candidate, session)` are produced. All authority is obtained internally from exact registry/source APIs; no unavailable target-receipt getter or target/source native continuation API is assumed. Private `Shape` and `Context` hold immutable snapshots and original references. Existing receipts/native lifecycle classes remain unchanged.

**Review focus coverage:** Step 1/3/5 cover forwarding catalog versus registry ownership; Step 6 covers cross-engine and final-context callback changes; Step 5/7 separate recipe validity from original-object association; Step 5 rejects populated analyzedQuery/write/options; Step 7 retains empty/repeated obligations and detects mutable source content. Step 8 pins inertness and unchanged admission.

**Executable choices versus observation gates:** The exact API fields and called companion methods exist. Actual absence of a built write, presence/canonical form of relation identifier/catalog/options, actual target/source retention for each frontend and self-source pairing still need the first tests. Those outcomes cannot be invented from the current dossier. The implementation stops at a mismatching positive observation rather than adding a new normalization or issuer hook. No user design decision is required to draft or test this finite component; any production issuer and execution continuation remains separate technical design work.

**No runtime claim:** This draft was written from source, the approved spec, the dependency memo and read-only cached-class signature inspection. No builds, tests, Git operations, services, production edits, delegation or cleanup were performed by the draft author. That historical draft was unimplemented. Root has now released the bounded implementation under the existing approved spec and continuous user authorization; actual observation gates remain required.

## Execution and acceptance ownership

BASE is `d5d0c516c`. The prior 328-test installed-boundary matrix is accepted; do not rerun it wholesale. One fresh implementer owns this task, preserving actual runtime observations and every failure. Use JDK 17 and process-scoped caffeinate for builds. Freeze the primary implementation and obtain independent spec/quality review before the focused four-pair matrix in Step 9. Retain per-cell XML, sources, JAR/class bytes, loaded origins and storage evidence before another build replaces outputs. No production callback may reference this association. Root handles tracked plan/results acceptance and local commits; preserve worktree/private evidence. CDF priority remains unchanged.

## Execution status

Steps 1–8 are implemented and independently reviewed. Actual insertInto/saveAsTable and self-source inputs satisfied the observation gate; no supported shape was broadened. A compiling behavioral RED preceded implementation. The full primary association class passed 17 tests; two subsequent test-only assertion refinements passed their focused checks and independent scoped review. The final reviewed production/test hashes are `aa459948221c6870cb1ebebaf359f5edb5723cc39a5c85e338c9734c607c421e` and `65cf3dfdb195cbb8020665729a068620b9d4702f36a80caba356d679d4490370`.

Independent task and final source integration reviews approved this bounded component with no outstanding findings. Both test-precision findings were fixed before qualification. The serial five-class matrix passed on the exact reviewed sources; root independently verified every cell below. Private failures, source snapshots and runtime evidence remain preserved under `.superpowers/sdd/2026-10-09-delta-append-command-association/`.

## Compatibility results — 2026-10-09

| Spark | Delta | Tests | Failures / errors / skipped |
| --- | --- | ---: | --- |
| 3.5.4 | 3.3.0 | 81 | 0 / 0 / 0 |
| 3.5.4 | 3.3.2 | 81 | 0 / 0 / 0 |
| 3.5.6 | 3.3.0 | 81 | 0 / 0 / 0 |
| 3.5.6 | 3.3.2 | 81 | 0 / 0 / 0 |

**324 tests passed**, covering the new 17-test association class and unchanged target receipt (16), source decision receipt (23), action validation (17), and installed append boundaries (8) per pair. Module Spotless checks passed in every run. Qualification used JDK 17.0.16, Scala 2.12.21 and all six resolved Ranger 2.8.0 artifacts; actual loaded Spark, Delta, Ranger and Scala origins match the selected runtime. No live Ranger service or S3 qualification was performed by this task.

The serial command used module `spotlessCheck` and `test`, five fully qualified `--tests` selectors for the classes in Step 9, and `-PskipITs -PskipDockerTests=true -PdeltaSparkVersion=<spark> -PdeltaVersion=<delta> --offline --console=plain`. Each private cell retains the literal command and environment, original logs, exit code and fresh timestamped XML, actual test-worker process and its full classpath argument file, 201 resolved artifact hashes, copied moving project JAR/class/resource bytes, and exact source copies. All 74 preexisting tracked module files remain identical to BASE. The two new sources retain the hashes above throughout all four cells.

Root independently verified all 20 XML documents, 804 resolved artifact records, archived runtime/source hashes and 19,616 storage entries across 308 fixtures. The 184 association fixtures preserve complete before/after storage equality. The 124 installed-boundary fixtures preserve their existing contract, including explicit publication/validation equality in the deliberate history fixtures; these fixture publications are not successful installed appends. The actual SQL observations agree across all pairs: one VALUES command, one INSERT SELECT command and two writeTo commands, each rejected exactly for its missing source-root receipt, with no silently excluded command shapes. Healthy SELECT rows and complete request inventories remain unchanged.

The implementation creates only the association and its test class. A production-source reference check finds no caller outside the new component itself. No existing admission rule, receipt, native-operation callback, dependency or build configuration changed. Association capture/validation grants no execution permission, proves no outer-command construction history, and provides no atomic revocation fence. Full governed append remains incomplete.

## Next dependency

A read-only assessment identifies native transaction-idempotency state execution as the next bounded candidate: bind the exact registered target and active native transaction, execute the genuine cold-state Aggregate/physical callbacks with configuration reset disabled, and return before source preparation, helper or commit. This is a recommendation for a subsequent plan, not an implemented or released execution path. Source issuance and normalization, actual analyzedQuery/V1 input continuity, transaction-owned self-source preparation, reset continuation, precommit enforcement, and live Ranger/S3 append qualification remain required. CDF remains the first feature priority after Delta maturity.
