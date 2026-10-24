/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.delta.ranger

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.hadoop.security.UserGroupInformation
import org.mockito.Mockito.when
import scala.collection.JavaConverters._
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Literal,
  AttributeReference,
  SortOrder,
  Ascending
}
import org.apache.spark.sql.catalyst.plans.logical.{
  Aggregate,
  Project,
  Filter,
  GlobalLimit,
  LocalLimit,
  LogicalPlan
}
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.types.{MetadataBuilder, StructType, LongType}
import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.catalyst.plans.logical.{DeserializeToObject, Statistics}
import org.apache.spark.sql.catalyst.expressions.objects.{NewInstance, StaticInvoke}
import org.apache.spark.sql.delta.SnapshotState
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerNativeIdempotency {
  import NativeIdempotencyProbe._
  import InstalledAppendBoundaryProbe.manifest

  @Test
  def coldDuplicateExecutesNativeAggregate(): Unit =
    executeCold("duplicate", "probe-app", "1", true)

  @Test
  def coldNewApplicationExecutesNativeAggregate(): Unit = executeCold("new", "new-app", "1", false)

  @Test
  def coldHigherVersionExecutesNativeAggregate(): Unit =
    executeCold("higher", "probe-app", "2", false)

  private def executeCold(name: String, app: String, version: String, expected: Boolean): Unit =
    withCase("execute-" + name) { c =>
      c.spark.conf.set(TxnApp, app)
      c.spark.conf.set(TxnVersion, version)
      c.spark.conf.set(Reset, "false")
      val sourceReceipt = source(c)
      val target = bound(c)
      val receipt = DeltaRangerAppendTargetReceipt.capture(c.spark, target)
      val before = c.spark.sessionState.conf.getAllConfs
      sourceReceipt.validateAction(c.spark)
      val result = DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty)
      assertEquals(expected, result)
      assertTrue(c.transaction.nonEmpty)
      assertTrue(computed(c.transaction.get.snapshot))
      assertTrue(c.aggregates.nonEmpty)
      assertTrue(c.bindings.nonEmpty)
      assertTrue(c.completion.await(30, TimeUnit.SECONDS), "Native first query did not complete")
      assertTrue(c.listenerFailures.isEmpty, c.listenerFailures.toString)
      assertFalse(c.completions.isEmpty)
      assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
      assertThrows(
        classOf[SecurityException],
        () => {
          DeltaRangerNativeProvenance.requireExecutable(c.aggregates.head, c.spark); ()
        }
      )
      assertThrows(
        classOf[SecurityException],
        () => {
          DeltaRangerPhysicalProvenance.finish(c.completions.peek().executedPlan, c.spark); ()
        }
      )
      assertEquals(before, c.spark.sessionState.conf.getAllConfs)
      receipt.validate(c.spark)
      sourceReceipt.validateAction(c.spark)
      c.trace.record("execution-result", s"result=$result computed=true confEqual=true")
    }

  @Test
  def nativeOptionsPreserveSessionAndExplicitPrecedence(): Unit = {
    val cases = Seq(
      ("session", Some("probe-app"), Some("1"), Map.empty[String, String], Some(true)),
      (
        "explicit-duplicate",
        Some("different"),
        Some("99"),
        Map("txnAppId" -> "probe-app", "txnVersion" -> "1"),
        Some(true)
      ),
      (
        "explicit-new",
        Some("probe-app"),
        Some("1"),
        Map("TXNAPPID" -> "new-app", "TXNVERSION" -> "1"),
        Some(false)
      ),
      ("absent", None, None, Map.empty[String, String], Some(false)),
      ("partial-app", None, None, Map("txnAppId" -> "probe-app"), None),
      ("partial-version", None, None, Map("txnVersion" -> "1"), None),
      ("malformed", None, None, Map("txnAppId" -> "probe-app", "txnVersion" -> "bad"), None),
      ("unknown", None, None, Map("mergeSchema" -> "true"), None),
      (
        "duplicate-case",
        None,
        None,
        Map("txnAppId" -> "a", "TXNAPPID" -> "b", "txnVersion" -> "1"),
        None
      ),
      ("session-partial", Some("probe-app"), None, Map.empty[String, String], None),
      ("reset", Some("probe-app"), Some("1"), Map.empty[String, String], None)
    )
    cases.foreach { case (name, app, version, options, expected) =>
      withCase("options-" + name) { c =>
        c.spark.conf.set(Reset, (name == "reset").toString)
        app.foreach(c.spark.conf.set(TxnApp, _))
        version.foreach(c.spark.conf.set(TxnVersion, _))
        val target = bound(c)
        val before = c.spark.sessionState.conf.getAllConfs
        expected match {
          case Some(value) =>
            assertEquals(
              value,
              DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, options)
            )
          case None =>
            assertThrows(
              classOf[Exception],
              () => {
                DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, options); ()
              }
            )
        }
        if (name == "reset") { assertTrue(c.transaction.isEmpty); assertTrue(c.aggregates.isEmpty) }
        assertEquals(before, c.spark.sessionState.conf.getAllConfs)
        c.trace.record("options-outcome", s"case=$name expected=$expected confEqual=true")
      }
    }
  }

  @Test
  def targetAuthorityPrecedesDuplicateLookup(): Unit = {
    Seq("update", "source").foreach { denial =>
      withCase("authority-" + denial) { c =>
        configure(c)
        val retainedSource = source(c)
        val target = bound(c)
        val before = c.spark.sessionState.conf.getAllConfs
        c.policy.denied.add(if (denial == "update") "a:*" else "b:secret")
        assertThrows(
          classOf[SecurityException],
          () => {
            retainedSource.validateAction(c.spark)
            DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
          }
        )
        assertTrue(c.transaction.isEmpty)
        assertTrue(c.aggregates.isEmpty)
        c.policy.denied.clear()
        retainedSource.validateAction(c.spark)
        assertEquals(before, c.spark.sessionState.conf.getAllConfs)
      }
    }
  }

  @Test
  def statePhaseRejectsExtraNativeAuthority(): Unit = withCase("state-negatives") { c =>
    configure(c)
    val sourcePlan = c.spark.sql("SELECT id, secret, region FROM b.db.t").queryExecution.analyzed
    val retainedSource =
      DeltaRangerDecisionReceipt.consumeOwned(sourcePlan, c.spark, optimized = false)
    assertTrue(DeltaRangerDecisionReceipt.present(sourcePlan))
    val target = bound(c)
    var checked = false
    var encoderChecked = false
    c.observe = {
      case aggregate: Aggregate if !checked && aggregate.resolved =>
        checked = true
        val operation = DeltaRangerNativeOperation.current()
        val rdd = aggregate.child.asInstanceOf[LogicalRDD]
        assertTrue(operation.contains(rdd, false))
        assertFalse(operation.contains(rdd, true))
        val first = rdd.output.head.asInstanceOf[AttributeReference]
        val outputs = Seq(
          first.newInstance(),
          first.withNullability(!first.nullable),
          first.withQualifier(Seq("foreign")),
          first.withMetadata(new MetadataBuilder().putString("foreign", "value").build())
        )
        outputs.foreach { replacement =>
          val changed = rdd.copy(output = replacement +: rdd.output.tail)(c.spark, None, None)
          assertFalse(operation.contains(changed, false))
          changed.copyTagsFrom(rdd)
          assertFalse(DeltaRangerPlanAdmission.nativeShape(changed))
        }
        val changes = Seq(
          rdd.copy(rdd = c.spark.sparkContext.emptyRDD[InternalRow])(c.spark, None, None),
          rdd.copy(outputPartitioning = UnknownPartitioning(1))(c.spark, None, None),
          rdd.copy(outputPartitioning = SinglePartition)(c.spark, None, None),
          rdd.copy(outputOrdering = Seq(SortOrder(first, Ascending)))(c.spark, None, None),
          rdd.copy(isStreaming = true)(c.spark, None, None),
          rdd.copy()(c.spark.newSession(), None, None),
          rdd.copy()(c.spark, Some(Statistics(1)), None),
          rdd.copy()(c.spark, None, Some(ExpressionSet(Seq(Literal(true)))))
        )
        changes.foreach(candidate => assertFalse(operation.contains(candidate, false)))
        val plans: Seq[LogicalPlan] = Seq(
          aggregate.copy(groupingExpressions = Seq(first)),
          aggregate.copy(aggregateExpressions =
            Alias(Literal(1L), "numOfFiles")() +: aggregate.aggregateExpressions.tail
          ),
          Project(rdd.output, rdd),
          Filter(Literal(true), rdd),
          GlobalLimit(Literal(2), LocalLimit(Literal(2), aggregate))
        )
        plans.foreach { plan =>
          plan.copyTagsFrom(aggregate)
          assertFalse(DeltaRangerPlanAdmission.nativeShape(plan), plan.treeString)
        }
        assertFalse(operation.ownsLogRoot(target.log().logPath))
        assertThrows(
          classOf[SecurityException],
          () => operation.capture(null, c.transaction.get.snapshot)
        )
      case binding: DeserializeToObject if binding.resolved && !encoderChecked =>
        encoderChecked = true
        val admitted = c.aggregates.head
        val nativeTag = TreeNodeTag[DeltaRangerNativeProvenance.Reference](
          "gravitino.delta.ranger.native-evidence"
        )
        assertTrue(
          admitted.getTagValue(nativeTag).nonEmpty,
          "Actual companion did not seal Aggregate before encoder resolution"
        )
        val spoof = admitted.copy(groupingExpressions = Seq(admitted.child.output.head))
        spoof.copyTagsFrom(admitted)
        assertThrows(
          classOf[SecurityException],
          () => {
            DeltaRangerNativeProvenance.requireExecutable(spoof, c.spark); ()
          }
        )
        val userTagged = admitted.clone()
        val userTag = TreeNodeTag[DeltaRangerDecisionReceipt.Reference](
          "gravitino.delta.ranger.decision-receipt"
        )
        userTagged.setTagValue(userTag, sourcePlan.getTagValue(userTag).get)
        assertTrue(DeltaRangerDecisionReceipt.present(userTagged))
        assertThrows(
          classOf[SecurityException],
          () => {
            DeltaRangerNativeProvenance.requireExecutable(userTagged, c.spark); ()
          }
        )
        DeltaRangerNativeOperation.current().requireEvidence(c.spark)
        c.trace.record(
          "live-negative-evidence",
          "nativeTagObserved=true groupingSpoofDenied=true userReceiptDenied=true ownerStillLive=true"
        )
        val variants = Seq(
          binding.deserializer.transformUp {
            case constructor: NewInstance if constructor.cls == classOf[SnapshotState] =>
              constructor.copy(arguments = constructor.arguments.updated(0, Literal(9L)))
          },
          binding.deserializer.transformUp {
            case attribute: AttributeReference if attribute.name == "sizeInBytes" =>
              StaticInvoke(classOf[java.lang.Long], LongType, "sum", Seq(attribute, Literal(1L)))
          },
          binding.deserializer.transformUp {
            case attribute: AttributeReference if attribute.name == "sizeInBytes" =>
              attribute.newInstance()
          },
          Literal.create(null, ObjectType(classOf[SnapshotState]))
        )
        variants.foreach { expression =>
          val changed = binding.copy(deserializer = expression)
          changed.copyTagsFrom(binding)
          assertFalse(DeltaRangerPlanAdmission.nativeShape(changed), expression.treeString)
        }
      case _ => ()
    }
    assertTrue(DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty))
    assertTrue(checked)
    assertTrue(encoderChecked)
    retainedSource.validateAction(c.spark)
  }

  @Test
  def contextDriftAndExceptionsUnwindBeforeReturn(): Unit = {
    Seq("configuration", "subject", "groups", "policy", "roles", "registration", "exception")
      .foreach { mode =>
        withCase("drift-" + mode) { c =>
          configure(c)
          val target = bound(c)
          val fixture = NativeSelectFixture.bindingFixture(c.spark, "a")
          val properties = fixture.properties.asScala.toMap
          val originalUser = UserGroupInformation.getCurrentUser
          val originalGroups = originalUser.getGroupNames
          val before = c.spark.sessionState.conf.getAllConfs
          val publication = c.policy.engine.snapshotStamp()
          val injected = new IllegalStateException("injected native observation failure")
          var changed = false
          var mutationVerified = false
          c.observe = {
            case _: Aggregate if !changed =>
              val operation = DeltaRangerNativeOperation.current()
              assertSame(c.operation.get, operation)
              assertTrue(operation.transactionState())
              operation.requireEvidence(c.spark)
              assertSame(c.transaction.get, OptimisticTransaction.getActive.get)
              assertFalse(computed(c.transaction.get.snapshot))
              c.trace.record("drift-live-aggregate", s"mode=$mode ownerLive=true computed=false")
              changed = true
              mode match {
                case "configuration" =>
                  c.spark.conf.set("native-idempotency.drift", "changed")
                  assertEquals("changed", c.spark.conf.get("native-idempotency.drift"))
                  assertNotEquals(before, c.spark.sessionState.conf.getAllConfs)
                case "subject" =>
                  assertNotEquals("changed-user", originalUser.getShortUserName)
                  c.spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
                  assertEquals(
                    "changed-user",
                    c.spark.sparkContext.getLocalProperty("kyuubi.session.user")
                  )
                case "groups" =>
                  val groups =
                    if (originalGroups.isEmpty) Array("native-idempotency-changed-group")
                    else Array.empty[String]
                  UserGroupInformation.createUserForTesting(originalUser.getShortUserName, groups)
                  val current = DeltaRangerQuerySubject.current(c.spark)
                  assertEquals(originalUser.getShortUserName, current.user())
                  assertEquals(
                    groups.toSet,
                    UserGroupInformation.getCurrentUser.getGroupNames.toSet
                  )
                  assertEquals(groups.toSet, current.groups().asScala.toSet)
                  assertNotEquals(originalGroups.toSet, current.groups().asScala.toSet)
                  c.trace.record(
                    "drift-groups",
                    s"before=${originalGroups.toVector} after=${current.groups()} sameUser=true"
                  )
                case "policy" | "roles" =>
                  val previous =
                    if (mode == "policy") c.policy.plugin.getPoliciesVersion
                    else c.policy.plugin.getRolesVersion
                  val next = previous + 1L
                  if (mode == "policy") {
                    when(c.policy.plugin.getPoliciesVersion).thenReturn(next)
                    c.policy.start()
                    assertEquals(next, c.policy.plugin.getPoliciesVersion)
                  } else {
                    val roles = c.policy.plugin.getRoles
                    roles.setRoleVersion(next)
                    when(c.policy.plugin.getRolesVersion).thenReturn(next)
                    c.policy.plugin.getCurrentRangerAuthContext.setRoles(roles)
                    c.policy.listener.contextChanged()
                    assertEquals(next, c.policy.plugin.getRolesVersion)
                    assertEquals(next, c.policy.plugin.getRoles.getRoleVersion.longValue())
                  }
                  assertNotEquals(previous, next)
                  val rejection = assertThrows(
                    classOf[SecurityException],
                    () => c.policy.engine.requireSnapshot(publication)
                  )
                  assertEquals(
                    "Delta Ranger publication changed; reanalyze the query",
                    rejection.getMessage
                  )
                  val current = c.policy.engine.snapshotStamp()
                  c.policy.engine.requireSnapshot(current)
                  c.expectedPublication = Some(current)
                  c.trace.record(
                    "drift-publication",
                    s"mode=$mode beforeVersion=$previous afterVersion=$next oldStampRejected=true newStampValid=true"
                  )
                case "registration" =>
                  assertNotEquals(
                    "2",
                    fixture.properties.get("delta.registration.receipt.generation")
                  )
                  fixture.properties.put("delta.registration.receipt.generation", "2")
                  assertEquals("2", fixture.properties.get("delta.registration.receipt.generation"))
                case "exception" =>
                  mutationVerified = true
                  throw injected
              }
              mutationVerified = true
              c.trace.record("drift-mutation-verified", s"mode=$mode")
            case _ => ()
          }
          try {
            if (mode == "exception") {
              val failure = assertThrows(
                classOf[IllegalStateException],
                () => {
                  DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
                }
              )
              assertSame(injected, failure)
              c.trace.record("drift-rejection", s"mode=$mode injectedIdentity=true")
            } else {
              val failure = assertThrows(
                classOf[SecurityException],
                () => {
                  DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
                }
              )
              val expected = mode match {
                case "subject" => "Kyuubi session identity contradicts authenticated query UGI"
                case "policy" | "roles" =>
                  "Delta Ranger publication changed; reanalyze the query"
                case _ => "Native Delta registration binding is unavailable"
              }
              assertEquals(expected, failure.getMessage)
              c.trace.record(
                "drift-rejection",
                s"mode=$mode exception=${failure.getClass.getName} message=${failure.getMessage}"
              )
            }
            assertTrue(changed)
            assertTrue(mutationVerified)
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
          } finally {
            c.observe = (_: LogicalPlan) => ()
            c.spark.conf.unset("native-idempotency.drift")
            c.spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
            UserGroupInformation.createUserForTesting(originalUser.getShortUserName, originalGroups)
            fixture.properties.clear(); fixture.properties.putAll(properties.asJava)
          }
          assertEquals(before, c.spark.sessionState.conf.getAllConfs)
          c.transaction = None
          assertTrue(DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty))
        }
      }
  }

  @Test
  def transactionBindingRejectsIdentitySubstitution(): Unit = withCase("binding-identity") { c =>
    configure(c)
    val target = bound(c)
    val snapshot = target.table().initialSnapshot
    val inactive = new OptimisticTransaction(target.log(), target.table().catalogTable, snapshot)
    assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, inactive))
    val other = NativeSelectFixture
      .wrapper(c.spark, "b")
      .loadTable(Identifier.of(Array("db"), "t"))
      .asInstanceOf[DeltaTableV2]
    other.deltaLog.withNewTransaction(other.catalogTable) { foreign =>
      assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, foreign))
    }
    val originalLocations = NativeSelectFixture.locations
    try {
      NativeSelectFixture.locations += "q" -> NativeSelectFixture.locations("a")
      val wrapper = NativeSelectFixture.wrapper(c.spark, "q")
      val foreignTable =
        wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
      val foreignTarget = wrapper.registrationBindings().resolveAppendTarget(foreignTable)
      assertSame(target.log(), foreignTarget.log())
      assertNotSame(target.descriptor(), foreignTarget.descriptor())
      assertEquals("q", foreignTarget.observation().catalog())
      foreignTarget.log().withNewTransaction(foreignTable.catalogTable) { txn =>
        val legitimate = foreignTarget.bindTransaction(c.spark, txn)
        try legitimate.requireLive()
        finally legitimate.close()
        assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, txn))
      }
      c.trace.record(
        "foreign-catalog-denial",
        "registered=q sameLog=true foreignBindingValid=true targetABindingDenied=true"
      )
    } finally NativeSelectFixture.locations = originalLocations
    val descriptor = target
      .descriptor()
      .copy(identifier = target.descriptor().identifier.copy(catalog = Some("foreign")))
    target.log().withNewTransaction(Some(descriptor)) { foreign =>
      assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, foreign))
    }
    target.log().withNewTransaction(target.table().catalogTable) { txn =>
      assertThrows(
        classOf[SecurityException],
        () => target.bindTransaction(c.spark.newSession(), txn)
      )
      val binding = target.bindTransaction(c.spark, txn)
      binding.requireLive()
      val threadFailure = new AtomicReference[Throwable]()
      val thread = new Thread(() => {
        try binding.requireLive()
        catch { case failure: Throwable => threadFailure.set(failure) }
      })
      thread.start(); thread.join(10000)
      assertFalse(thread.isAlive)
      assertTrue(threadFailure.get().isInstanceOf[SecurityException])
      val operation = DeltaRangerNativeOperation.transactionState(binding)
      try {
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerNativeOperation.transactionState(binding)
        )
      } finally operation.close()
      assertThrows(classOf[SecurityException], () => operation.requireEvidence(c.spark))
      val replacement = new Snapshot(
        target.log().logPath,
        txn.snapshot.version,
        txn.snapshot.logSegment,
        target.log(),
        txn.snapshot.checksumOpt
      )
      assertNotSame(txn.snapshot, replacement)
      assertEquals(txn.snapshot.metadata, replacement.metadata)
      val different =
        new OptimisticTransaction(target.log(), target.table().catalogTable, replacement)
      OptimisticTransaction.withActive(different, overrideExistingTransaction = true) {
        assertThrows(classOf[SecurityException], () => binding.requireLive())
      }
      binding.requireLive()
      binding.close()
      assertThrows(classOf[SecurityException], () => binding.requireLive())
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerNativeOperation.transactionState(binding)
      )
    }
    val fixture = NativeSelectFixture.bindingFixture(c.spark, "a")
    val original = fixture.properties.asScala.toMap
    Seq("generation", "nativeId", "schema").foreach { change =>
      val columns = fixture.columns
      try {
        if (change == "schema") fixture.columns = columns.dropRight(1)
        else
          fixture.properties.put(
            "delta.registration.receipt." + change,
            if (change == "generation") "2" else "foreign"
          )
        target.log().withNewTransaction(target.table().catalogTable) { txn =>
          assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, txn))
        }
      } finally {
        fixture.properties.clear(); fixture.properties.putAll(original.asJava);
        fixture.columns = columns
      }
    }
    val metadata = snapshot.metadata
    val variants = Seq(
      "id" -> metadata.copy(id = "foreign"),
      "schema" -> metadata.copy(schemaString = StructType(snapshot.schema.dropRight(1)).json),
      "partition" -> metadata.copy(partitionColumns = Seq("region")),
      "mapping" -> metadata.copy(configuration =
        metadata.configuration + ("delta.columnMapping.mode" -> "name")
      ),
      "configuration" -> metadata.copy(configuration =
        metadata.configuration + ("delta.appendOnly" -> "true")
      )
    )
    variants.foreach { case (name, changed) =>
      val checksum = snapshot.checksumOpt.get.copy(metadata = changed)
      val substituted = new Snapshot(
        target.log().logPath,
        snapshot.version,
        snapshot.logSegment,
        target.log(),
        Some(checksum)
      )
      val txn = new OptimisticTransaction(target.log(), target.table().catalogTable, substituted)
      OptimisticTransaction.withActive(txn) {
        assertThrows(
          classOf[SecurityException],
          () => { target.bindTransaction(c.spark, txn); () },
          name
        )
      }
    }
    val changedProtocol =
      snapshot.checksumOpt.get.copy(protocol = snapshot.protocol.copy(minWriterVersion = 3))
    val substituted = new Snapshot(
      target.log().logPath,
      snapshot.version,
      snapshot.logSegment,
      target.log(),
      Some(changedProtocol)
    )
    val txn = new OptimisticTransaction(target.log(), target.table().catalogTable, substituted)
    OptimisticTransaction.withActive(txn) {
      assertThrows(classOf[SecurityException], () => target.bindTransaction(c.spark, txn))
    }
  }

  @Test
  def transactionEvidenceExpiresOnEveryExit(): Unit = withCase("evidence-lifetime") { c =>
    configure(c)
    val target = bound(c)
    var observed = false
    c.observe = {
      case aggregate: Aggregate if aggregate.resolved && !observed =>
        observed = true
        assertThrows(
          classOf[SecurityException],
          () => {
            DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
          }
        )
        val operation = DeltaRangerNativeOperation.current()
        val txn = OptimisticTransaction.getActive.get
        val replacement =
          new OptimisticTransaction(target.log(), target.table().catalogTable, txn.snapshot)
        OptimisticTransaction.withActive(replacement, overrideExistingTransaction = true) {
          assertThrows(classOf[SecurityException], () => operation.requireEvidence(c.spark))
        }
        operation.requireEvidence(c.spark)
      case _ => ()
    }
    assertTrue(DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty))
    assertTrue(observed)
    assertTrue(c.completion.await(30, TimeUnit.SECONDS))
    val completed = c.completions.peek()
    val planning = DeltaRangerPhysicalPlanning.native(completed.optimizedPlan)
    assertTrue(planning.nonEmpty)
    assertThrows(classOf[SecurityException], () => { planning.get.replay(); () })
    val expiredPhysical = assertThrows(
      classOf[SecurityException],
      () => { DeltaRangerPhysicalProvenance.finish(completed.executedPlan, c.spark); () }
    )
    assertSecurity(expiredPhysical, "Native Delta operation ownership is unavailable")
    OptimisticTransaction.withActive(c.transaction.get) {
      assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
      assertThrows(
        classOf[SecurityException],
        () => { DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); () }
      )
    }
    c.operation.get.close()
    assertThrows(classOf[SecurityException], () => c.operation.get.captureTransactionState())
    val originalTransaction = c.transaction.get
    val aggregateCount = c.aggregates.size
    var repeated: Option[OptimisticTransaction] = None
    c.policy.onRequest = _ => {
      OptimisticTransaction.getActive.foreach(txn => repeated = Some(txn))
    }
    val before = c.spark.sessionState.conf.getAllConfs
    assertTrue(DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty))
    c.policy.onRequest = _ => ()
    assertTrue(repeated.nonEmpty)
    assertNotSame(originalTransaction, repeated.get)
    assertSame(originalTransaction.snapshot, repeated.get.snapshot)
    assertTrue(computed(repeated.get.snapshot))
    assertEquals(aggregateCount, c.aggregates.size)
    assertEquals(before, c.spark.sessionState.conf.getAllConfs)
    assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
    c.trace.record(
      "repeat-result",
      "result=true freshTransaction=true existingComputedState=true confEqual=true"
    )
  }

  @Test
  def uncachedStateRejectsAndOwnedMissingChecksumReadsCurrentTransaction(): Unit = {
    withCase("owned-uncached") { c =>
      configure(c)
      val target = bound(c)
      val before = c.spark.sessionState.conf.getAllConfs
      var requests = 0
      var selected: Option[Snapshot] = None
      c.policy.onRequest = request => {
        if (request.getAccessType == "update" && OptimisticTransaction.getActive.nonEmpty) {
          requests += 1
          if (requests == 2) {
            val snapshot = OptimisticTransaction.getActive.get.snapshot
            assertFalse(computed(snapshot))
            selected = Some(snapshot)
            snapshot.uncache()
          }
        }
      }
      val error = assertThrows(
        classOf[Exception],
        () => {
          DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
        }
      )
      c.policy.onRequest = _ => ()
      assertTrue(selected.nonEmpty)
      assertSecurity(error, "Unqualified Delta Ranger route: SerializeFromObject")
      assertTrue(
        Iterator
          .iterate[Throwable](error)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.getStackTrace.exists(_.getMethodName == "captureTransactionState"))
      )
      assertTrue(c.aggregates.isEmpty)
      assertFalse(computed(selected.get))
      assertEquals(before, c.spark.sessionState.conf.getAllConfs)
      c.trace.record(
        "owned-uncached-failure",
        error.toString + "\n" + error.getStackTrace.mkString("\n")
      )
    }
    var staged = Map.empty[Path, Array[Byte]]
    withHistoryCase(
      "owned-checksum-absent",
      (spark, trace) => {
        val location = NativeSelectFixture.locations("a")
        spark
          .sql("SELECT 4L id, 'prior' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .option("txnAppId", "probe-app")
          .option("txnVersion", "1")
          .save(location)
        val paths = Files.list(Paths.get(location, "_delta_log"))
        try
          staged = paths
            .iterator()
            .asScala
            .filter(_.getFileName.toString.contains("00000000000000000001."))
            .map(p => p -> Files.readAllBytes(p))
            .toMap
        finally paths.close()
        assertTrue(staged.keys.exists(_.getFileName.toString == "00000000000000000001.crc"))
        val original = trace.directory.resolve("original-version-1")
        Files.createDirectory(original)
        staged.foreach { case (path, bytes) =>
          Files.write(original.resolve(path.getFileName), bytes)
        }
        staged.keys.foreach(Files.delete)
        DeltaLog.clearCache()
      }
    ) { c =>
      configure(c)
      val target = bound(c)
      staged.foreach { case (path, bytes) =>
        val name = path.getFileName.toString
        if (name != "00000000000000000001.crc" && name != ".00000000000000000001.crc.crc")
          Files.write(path, bytes)
      }
      val location = Paths.get(NativeSelectFixture.locations("a"))
      val published = manifest(location, c.trace.directory.resolve("published"))
      val before = c.spark.sessionState.conf.getAllConfs
      assertTrue(DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty))
      assertEquals(1L, c.transaction.get.snapshot.version)
      assertEquals(1L, target.log().unsafeVolatileSnapshot.version)
      assertFalse(c.aggregates.isEmpty)
      assertTrue(c.appends.isEmpty)
      assertEquals(before, c.spark.sessionState.conf.getAllConfs)
      assertEquals(published, manifest(location, c.trace.directory.resolve("validated")))
      c.trace.record(
        "owned-checksum-outcome",
        "native-idempotency=true version=1 publishedBytesEqual=true confEqual=true"
      )
    }
  }

  @Test
  def statePhaseRejectsNativeFileAndReconstructionExecution(): Unit = {
    Seq("allFiles", "withStats", "raw-log").foreach { mode =>
      withCase("extra-getter-" + mode) { c =>
        configure(c)
        val target = bound(c)
        var attempted = false
        c.observe = {
          case _: Aggregate if !attempted =>
            attempted = true
            val snapshot = OptimisticTransaction.getActive.get.snapshot
            val dataset = mode match {
              case "allFiles"  => snapshot.allFiles
              case "withStats" => snapshot.withStats
              case "raw-log" =>
                c.spark.read.json(target.log().logPath.toString + "/00000000000000000001.json")
            }
            val actual = dataset.queryExecution.analyzed
            c.trace.record("extra-native-return", s"mode=$mode tree=${actual.treeString}")
            assertThrows(
              classOf[SecurityException],
              () => {
                DeltaRangerNativeProvenance.requireExecutable(actual, c.spark); ()
              }
            )
            dataset.collect()
          case _ => ()
        }
        val error = assertThrows(
          classOf[Exception],
          () => {
            DeltaRangerNativeIdempotency.hasBeenExecuted(c.spark, target, Map.empty); ()
          }
        )
        assertTrue(attempted)
        assertTrue(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(_.isInstanceOf[SecurityException])
        )
        assertNull(DeltaRangerNativeOperation.current())
        assertTrue(OptimisticTransaction.getActive.isEmpty)
      }
    }
  }

  private def configure(c: Case): Unit = {
    c.spark.conf.set(TxnApp, "probe-app")
    c.spark.conf.set(TxnVersion, "1")
    c.spark.conf.set(Reset, "false")
  }

  @Test
  def inventoryFindsColdOwnedTargetAggregate(): Unit = {
    Seq(
      "duplicate-session",
      "new-app",
      "higher-version",
      "explicit-duplicate",
      "explicit-new",
      "reset-enabled"
    ).foreach { mode =>
      withCase(mode) { c =>
        c.spark.conf.set(
          TxnApp,
          if (mode == "new-app") "new-app"
          else if (mode == "explicit-duplicate") "session-app"
          else "probe-app"
        )
        c.spark.conf.set(
          TxnVersion,
          if (mode == "higher-version") "2" else if (mode == "explicit-duplicate") "99" else "1"
        )
        c.spark.conf.set(Reset, (mode == "reset-enabled").toString)
        val sourceReceipt = source(c)
        val target = bound(c)
        val receipt = DeltaRangerAppendTargetReceipt.capture(c.spark, target)
        val options = mode match {
          case "explicit-duplicate" => Map("txnAppId" -> "probe-app", "txnVersion" -> "1")
          case "explicit-new"       => Map("txnAppId" -> "new-app", "txnVersion" -> "1")
          case _                    => Map.empty[String, String]
        }
        val before = c.spark.sessionState.conf.getAllConfs
        val error = assertThrows(
          classOf[Exception],
          () => { inspectCold(c, target, receipt, sourceReceipt, options); () }
        )
        assertSecurity(error, "Unqualified Delta Ranger route: Aggregate")
        assertEquals(Some("hasBeenExecuted"), c.trace.firstFailure.map(_._1))
        assertTrue(c.trace.skip.isEmpty)
        assertTrue(c.transaction.nonEmpty)
        assertTrue(c.aggregates.nonEmpty, "No actual native Aggregate observed")
        assertTrue(c.bindings.isEmpty, "Aggregate rejection should precede native encoder analysis")
        c.aggregates.foreach { aggregate =>
          assertTrue(aggregate.groupingExpressions.isEmpty)
          assertEquals(14, aggregate.aggregateExpressions.size)
          assertEquals(ExpectedAliases, aggregate.aggregateExpressions.map(_.name).toSet)
          assertEquals(classOf[LogicalRDD], aggregate.child.getClass)
          val rdd = aggregate.child.asInstanceOf[LogicalRDD]
          assertEquals(classOf[UnknownPartitioning], rdd.outputPartitioning.getClass)
          assertTrue(rdd.outputOrdering.isEmpty)
          assertFalse(rdd.isStreaming)
          val origins = otherCopyArgs(rdd)
          assertEquals(3, origins.size)
          assertSame(c.spark, origins.head)
          assertEquals(None, origins(1))
          assertEquals(None, origins(2))
        }
        assertEquals(before, c.spark.sessionState.conf.getAllConfs)
        assertTrue(c.trace.completed.contains("entry-target"))
        assertTrue(c.trace.completed.contains("entry-source"))
        assertTrue(c.trace.completed.contains("transaction-entry-target"))
        assertTrue(c.trace.completed.contains("transaction-entry-source"))
        c.trace.phase(c.spark, "after-target-same-receipt") { receipt.validate(c.spark) }
        c.trace.phase(c.spark, "after-source-same-receipt") {
          sourceReceipt.validateAction(c.spark)
        }
        assertEquals(before, c.spark.sessionState.conf.getAllConfs)
        c.trace.record(
          "inventory-outcome",
          s"mode=$mode actualAggregateCount=${c.aggregates.size} encoderCount=${c.bindings.size} boolean=NOT_RETURNED reset=NOT_REACHED receiptsRetained=true confEqual=true"
        )
      }
    }
  }

  @Test
  def uncachedStateRecordsActualClosedBoundary(): Unit = withCase("uncached") { c =>
    c.spark.conf.set(TxnApp, "probe-app")
    c.spark.conf.set(TxnVersion, "1")
    c.spark.conf.set(Reset, "false")
    val sourceReceipt = source(c)
    val target = bound(c)
    val receipt = DeltaRangerAppendTargetReceipt.capture(c.spark, target)
    val before = c.spark.sessionState.conf.getAllConfs
    val error = assertThrows(
      classOf[Exception],
      () => { inspectUncached(c, target, receipt, sourceReceipt); () }
    )
    assertSecurity(error, "Unqualified Delta Ranger route: SerializeFromObject")
    assertEquals(Some("hasBeenExecuted"), c.trace.firstFailure.map(_._1))
    assertTrue(c.aggregates.isEmpty)
    assertEquals(1, c.bindings.size)
    assertEquals(ObjectType(classOf[SingleAction]), c.bindings.head.deserializer.dataType)
    assertEquals(
      c.uncachedStorage,
      manifest(
        Paths.get(NativeSelectFixture.locations("a")),
        c.trace.directory.resolve("uncached-validated")
      )
    )
    assertTrue(c.transaction.nonEmpty)
    assertTrue(c.trace.skip.isEmpty)
    assertEquals(before, c.spark.sessionState.conf.getAllConfs)
    c.trace.record(
      "uncached-outcome",
      s"firstFailure=${c.trace.firstFailure.map(_._1)} aggregates=${c.aggregates.size} encoders=${c.bindings.size} boolean=NOT_RETURNED failure=${error.toString}"
    )
  }

  @Test
  def checksumAbsentUnownedHistoryRejectsAggregate(): Unit = {
    var staged = Map.empty[Path, Array[Byte]]
    withHistoryCase(
      "checksum-absent",
      (spark, trace) => {
        val location = NativeSelectFixture.locations("a")
        spark
          .sql("SELECT 4L id, 'prior' secret, 'keep' region")
          .write
          .format("delta")
          .mode("append")
          .option("txnAppId", "probe-app")
          .option("txnVersion", "1")
          .save(location)
        assertEquals(1L, DeltaLog.forTable(spark, location).update().version)
        val paths = Files.list(Paths.get(location, "_delta_log"))
        try
          staged = paths
            .iterator()
            .asScala
            .filter(_.getFileName.toString.contains("00000000000000000001."))
            .map(p => p -> Files.readAllBytes(p))
            .toMap
        finally paths.close()
        assertTrue(staged.keys.exists(_.getFileName.toString == "00000000000000000001.crc"))
        val original = trace.directory.resolve("original-version-1")
        Files.createDirectory(original)
        staged.foreach { case (path, bytes) =>
          Files.write(original.resolve(path.getFileName), bytes)
        }
        staged.keys.foreach(Files.delete)
        DeltaLog.clearCache()
      }
    ) { c =>
      c.spark.conf.set(TxnApp, "probe-app")
      c.spark.conf.set(TxnVersion, "1")
      c.spark.conf.set(Reset, "false")
      val sourceReceipt = source(c)
      val target = bound(c)
      val receipt = DeltaRangerAppendTargetReceipt.capture(c.spark, target)
      staged.foreach { case (path, bytes) =>
        val name = path.getFileName.toString
        if (name != "00000000000000000001.crc" && name != ".00000000000000000001.crc.crc")
          Files.write(path, bytes)
      }
      val location = Paths.get(NativeSelectFixture.locations("a"))
      val published = manifest(location, c.trace.directory.resolve("published"))
      val before = c.spark.sessionState.conf.getAllConfs
      val error = assertThrows(
        classOf[Exception],
        () => { inspectCold(c, target, receipt, sourceReceipt, Map.empty); () }
      )
      assertSecurity(error, "Unqualified Delta Ranger route: Aggregate")
      assertEquals(Some("hasBeenExecuted"), c.trace.firstFailure.map(_._1))
      assertEquals(1L, c.transaction.get.snapshot.version)
      assertFalse(computed(c.transaction.get.snapshot))
      assertEquals(1, c.aggregates.size)
      assertTrue(c.completions.isEmpty)
      assertTrue(c.appends.isEmpty)
      assertTrue(c.trace.skip.isEmpty)
      assertEquals(1L, target.log().unsafeVolatileSnapshot.version)
      assertEquals(before, c.spark.sessionState.conf.getAllConfs)
      assertEquals(published, manifest(location, c.trace.directory.resolve("validated")))
      c.trace.record(
        "checksum-outcome",
        "hasBeenExecuted=DENIED_AGGREGATE computed=false boolean=NOT_RETURNED publishedBytesEqual=true"
      )
    }
  }
}
