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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.mockito.Mockito.when
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, AppendData}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendIdempotency {
  import NativeIdempotencyProbe._

  @Test
  def actualColdFrontendsReturnOnlyNativeScalar(): Unit = {
    for (frontend <- Seq("insertInto", "saveAsTable"); mode <- Seq("duplicate", "new", "higher")) {
      withCase("append-" + frontend + "-" + mode) { c =>
        configure(c, mode)
        val command = captured(c, frontend)
        val association = DeltaRangerAppendCommandBinding.capture(
          c.spark,
          command,
          NativeSelectFixture.wrapper(c.spark, "a")
        )
        val target =
          command.table.asInstanceOf[DataSourceV2Relation].table.asInstanceOf[DeltaTableV2]
        val snapshot = target.deltaLog.unsafeVolatileSnapshot
        assertFalse(computed(snapshot))
        val before = c.spark.sessionState.conf.getAllConfs
        var ownedTransaction: Option[OptimisticTransaction] = None
        val validations = scala.collection.mutable.Set.empty[(String, Boolean)]
        c.policy.onRequest = request => {
          if (request.getAccessType == "select" || request.getAccessType == "update") {
            assertNull(DeltaRangerNativeOperation.current())
            OptimisticTransaction.getActive.foreach { txn =>
              ownedTransaction match {
                case Some(original) => assertSame(original, txn)
                case None           => ownedTransaction = Some(txn)
              }
              validations += ((request.getAccessType, computed(txn.snapshot)))
            }
          }
        }
        assertEquals(mode == "duplicate", association.hasBeenExecuted(command, c.spark))
        c.policy.onRequest = _ => ()
        assertSame(ownedTransaction.get, c.transaction.get)
        assertEquals(
          Set(("select", false), ("select", true), ("update", false), ("update", true)),
          validations.toSet
        )
        assertSame(target.deltaLog, c.transaction.get.deltaLog)
        assertSame(target.catalogTable.get, c.transaction.get.catalogTable.get)
        assertSame(snapshot, c.transaction.get.snapshot)
        assertTrue(computed(snapshot))
        assertTrue(c.aggregates.nonEmpty)
        assertTrue(c.bindings.nonEmpty)
        assertTrue(
          c.completion.await(30, TimeUnit.SECONDS),
          "Native physical query did not complete"
        )
        assertTrue(c.listenerFailures.isEmpty, c.listenerFailures.toString)
        assertFalse(c.completions.isEmpty)
        assertEquals(before, c.spark.sessionState.conf.getAllConfs)
        assertNull(DeltaRangerNativeOperation.current())
        assertTrue(OptimisticTransaction.getActive.isEmpty)
        association.validate(command, c.spark)
        c.trace.record(
          "association-scalar-result",
          s"frontend=$frontend mode=$mode computed=true result=${mode == "duplicate"}"
        )
      }
    }
  }

  @Test
  def duplicateRequiresOriginalSourceAndTargetAuthority(): Unit = {
    for (change <- Seq("select", "update", "mask", "filter")) {
      withCase("append-denied-" + change) { c =>
        configure(c)
        c.policy.customCatalog = "b"
        val command = captured(c)
        val association = capture(c, command)
        change match {
          case "select" => c.policy.denied.add("b:secret")
          case "update" => c.policy.denied.add("a:*")
          case "mask"   => c.policy.mask = "upper({col})"
          case "filter" => c.policy.filter = "id > 1"
        }
        val error = assertThrows(
          classOf[SecurityException],
          () => {
            association.hasBeenExecuted(command, c.spark); ()
          }
        )
        assertTrue(
          error.getMessage.contains(
            if (change == "mask" || change == "filter")
              "action decision changed"
            else "denied"
          ),
          error.toString
        )
        assertTrue(c.transaction.isEmpty)
        assertTrue(c.aggregates.isEmpty)
      }
    }
  }

  @Test
  def emptyRepeatedAndSelfSourceKeepTheirOriginalObligations(): Unit = {
    Seq(
      ("empty", "SELECT id, secret, region FROM b.db.t WHERE id < 0", 1, "b:secret"),
      (
        "repeated",
        "SELECT l.id, l.secret, r.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id",
        2,
        "b:secret"
      ),
      ("self", "SELECT id, secret, region FROM a.db.t", 1, "a:secret")
    ).foreach { case (name, sql, count, denied) =>
      withCase("append-source-" + name) { c =>
        configure(c)
        val command = captured(c, sql = sql)
        val original =
          DeltaRangerDecisionReceipt.consumeOwned(command.query, c.spark, optimized = false)
        assertEquals(count, original.obligations.size)
        val association = capture(c, command)
        assertTrue(association.hasBeenExecuted(command, c.spark))
        assertSame(
          original,
          DeltaRangerDecisionReceipt.consumeOwned(command.query, c.spark, optimized = false)
        )
        assertEquals(count, original.obligations.size)
        c.policy.denied.add(denied)
        val error = assertThrows(
          classOf[SecurityException],
          () => {
            association.hasBeenExecuted(command, c.spark); ()
          }
        )
        assertTrue(error.getMessage.contains("denied"), error.toString)
        c.policy.denied.clear()
        val execution = assertThrows(
          classOf[SecurityException],
          () => {
            DeltaRangerNativeProvenance.requireExecutable(command.query, c.spark); ()
          }
        )
        assertNotNull(execution.getMessage)
      }
    }
  }

  @Test
  def commandSessionThreadAndNestedScopesCannotSubstituteAuthority(): Unit =
    withCase("append-identity") { c =>
      configure(c)
      val command = captured(c)
      val association = capture(c, command)
      Seq(
        command.copy(),
        command.copy(query = command.query.clone()),
        command.copy(table = command.table.asInstanceOf[DataSourceV2Relation].copy())
      ).foreach { copy =>
        rejected("command identity changed") { association.hasBeenExecuted(copy, c.spark) }
      }
      val other = c.spark.newSession()
      try {
        rejected("session changed") { association.hasBeenExecuted(command, other) }
        SparkSession.setActiveSession(other)
        rejected("session changed") { association.hasBeenExecuted(command, c.spark) }
        SparkSession.clearActiveSession()
        rejected("session changed") { association.hasBeenExecuted(command, c.spark) }
      } finally SparkSession.setActiveSession(c.spark)
      val failure = new AtomicReference[Throwable]()
      val worker = new Thread(() => {
        SparkSession.setActiveSession(c.spark)
        try association.hasBeenExecuted(command, c.spark)
        catch { case error: Throwable => failure.set(error) }
        finally SparkSession.clearActiveSession()
      })
      worker.start(); worker.join(30000)
      assertFalse(worker.isAlive)
      assertTrue(failure.get().isInstanceOf[SecurityException])
      assertEquals(
        "Delta append association rejected: creator thread changed",
        failure.get().getMessage
      )
      val target = targetOf(c, command)
      target.log().withNewTransaction(Some(target.descriptor())) { txn =>
        rejected("active native transaction") { association.hasBeenExecuted(command, c.spark) }
        val owner = target.bindTransaction(c.spark, txn)
        try {
          val op = DeltaRangerNativeOperation.transactionState(owner)
          try rejected("active native scope") { association.hasBeenExecuted(command, c.spark) }
          finally op.close()
        } finally owner.close()
      }
      assertTrue(c.transaction.isEmpty)
    }

  @Test
  def finiteScalarRequiresExactLiveOwnerAndLeavesItLive(): Unit = withCase("append-scalar-owner") {
    c =>
      configure(c)
      val command = captured(c)
      val target = targetOf(c, command)
      var retained: DeltaRangerRegistrationBindings.BoundAppendTransaction = null
      var original: OptimisticTransaction = null
      target.log().withNewTransaction(Some(target.descriptor())) { txn =>
        original = txn
        val owner = target.bindTransaction(c.spark, txn)
        retained = owner
        val other = new OptimisticTransaction(target.log(), Some(target.descriptor()), txn.snapshot)
        try {
          rejected("Native Delta registration binding is unavailable") {
            DeltaRangerNativeIdempotency.readInTransaction(owner, other, Map.empty)
          }
          OptimisticTransaction.withActive(other, overrideExistingTransaction = true) {
            rejected("Native Delta registration binding is unavailable") {
              DeltaRangerNativeIdempotency.readInTransaction(owner, txn, Map.empty)
            }
          }
          assertThrows(
            classOf[IllegalArgumentException],
            () => {
              DeltaRangerNativeIdempotency
                .readInTransaction(owner, txn, Map("unqualified" -> "option")); ()
            }
          )
          val op = DeltaRangerNativeOperation.transactionState(owner)
          try
            rejected("Native idempotency entry rejected") {
              DeltaRangerNativeIdempotency.readInTransaction(owner, txn, Map.empty)
            }
          finally op.close()
          assertTrue(DeltaRangerNativeIdempotency.readInTransaction(owner, txn, Map.empty))
          owner.requireTransaction(txn)
          assertSame(txn, OptimisticTransaction.getActive.get)
          assertNull(DeltaRangerNativeOperation.current())
        } finally owner.close()
        rejected("Native Delta registration binding is unavailable") {
          DeltaRangerNativeIdempotency.readInTransaction(owner, txn, Map.empty)
        }
      }
      OptimisticTransaction.withActive(original) {
        rejected("Native Delta registration binding is unavailable") {
          DeltaRangerNativeIdempotency.readInTransaction(retained, original, Map.empty)
        }
      }
  }

  @Test
  def scalarAndValidationExceptionsKeepTheirIdentityAndUnwind(): Unit = {
    Seq("scalar", "after").foreach { phase =>
      withCase("append-exception-" + phase) { c =>
        configure(c)
        val command = captured(c)
        val association = capture(c, command)
        val injected = new IllegalStateException("append observation " + phase)
        var fired = false
        if (phase == "scalar") c.observe = {
          case _: Aggregate if !fired => fired = true; throw injected
          case _                      => ()
        }
        else
          c.policy.onRequest = request => {
            if (
              !fired && request.getAccessType == "select" && c.transaction
                .exists(t => computed(t.snapshot))
            ) {
              assertSame(c.transaction.get, OptimisticTransaction.getActive.get)
              assertNull(DeltaRangerNativeOperation.current())
              fired = true
              throw injected
            }
          }
        if (phase == "scalar") {
          val error = assertThrows(
            classOf[IllegalStateException],
            () => { association.hasBeenExecuted(command, c.spark); () }
          )
          assertSame(injected, error)
        } else {
          val error = assertThrows(
            classOf[SecurityException],
            () => { association.hasBeenExecuted(command, c.spark); () }
          )
          assertEquals("Delta Ranger evaluation rejected", error.getMessage)
          assertSame(injected, error.getCause)
        }
        assertTrue(fired)
        assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
        assertNull(DeltaRangerNativeOperation.current())
        assertTrue(OptimisticTransaction.getActive.isEmpty)
        c.policy.onRequest = _ => ()
      }
    }
  }

  @Test
  def scalarAndPostScalarMutationsRejectBothBooleanBranches(): Unit = {
    for (
      phase <- Seq("scalar", "after");
      mode <- Seq("duplicate", "new");
      change <- Seq(
        "source-policy",
        "target-policy",
        "source-roles",
        "target-roles",
        "source-id",
        "target-id",
        "source-token",
        "target-token",
        "subject",
        "groups",
        "configuration",
        "source-literal",
        "command-namespace"
      )
    ) {
      val sourcePolicy = new TestDeltaRangerNativeSelect.Fixture
      sourcePolicy.customCatalog = "b"
      try
        withCase(s"append-mutation-$phase-$mode-$change") { c =>
          NativeSelectFixture.catalogPolicies = Map("b" -> sourcePolicy)
          configure(c, mode)
          val command =
            captured(c, sql = "SELECT id, secret, region FROM b.db.t WHERE region = 'keep'")
          val association = capture(c, command)
          val registration = NativeSelectFixture.bindingFixture(
            c.spark,
            if (change.startsWith("source")) "b" else "a"
          )
          val properties = registration.properties.asScala.toMap
          val user = UserGroupInformation.getCurrentUser
          val groups = user.getGroupNames
          val namespace =
            command.table.asInstanceOf[DataSourceV2Relation].identifier.get.namespace()
          val originalNamespace = namespace(0)
          val literal = command.query
            .collect { case plan =>
              plan.expressions.flatMap(_.collect {
                case value: Literal
                    if value.dataType == StringType && value.value.toString == "keep" =>
                  value
              })
            }
            .flatten
            .head
          val bytes = literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]]
          val firstByte = bytes(0)
          var fired = false
          def mutate(): Unit = {
            assertFalse(fired)
            fired = true
            assertSame(c.transaction.get, OptimisticTransaction.getActive.get)
            if (phase == "after") assertNull(DeltaRangerNativeOperation.current())
            else assertTrue(DeltaRangerNativeOperation.current().transactionState())
            change match {
              case "source-policy" | "target-policy" | "source-roles" | "target-roles" =>
                val policy = if (change.startsWith("source")) sourcePolicy else c.policy
                val stamp = policy.engine.snapshotStamp()
                if (change.endsWith("policy")) {
                  val version = policy.plugin.getPoliciesVersion + 1L
                  when(policy.plugin.getPoliciesVersion).thenReturn(version)
                  policy.start()
                } else {
                  val roles = policy.plugin.getRoles
                  val version = policy.plugin.getRolesVersion + 1L
                  roles.setRoleVersion(version)
                  when(policy.plugin.getRolesVersion).thenReturn(version)
                  policy.plugin.getCurrentRangerAuthContext.setRoles(roles)
                  policy.listener.contextChanged()
                }
                rejected("Delta Ranger publication changed; reanalyze the query") {
                  policy.engine.requireSnapshot(stamp)
                }
                if (policy eq c.policy) c.expectedPublication = Some(policy.engine.snapshotStamp())
              case "source-id" | "target-id" =>
                registration.properties.put("delta.registration.receipt.id", "2")
                assertEquals("2", registration.properties.get("delta.registration.receipt.id"))
              case "source-token" | "target-token" =>
                registration.properties.put(
                  "delta.registration.receipt.token",
                  "changed-publication"
                )
                assertEquals(
                  "changed-publication",
                  registration.properties.get("delta.registration.receipt.token")
                )
              case "subject" =>
                c.spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
                assertEquals(
                  "changed-user",
                  c.spark.sparkContext.getLocalProperty("kyuubi.session.user")
                )
              case "groups" =>
                val changed = if (groups.isEmpty) Array("changed-group") else Array.empty[String]
                UserGroupInformation.createUserForTesting(user.getShortUserName, changed)
                assertNotEquals(
                  groups.toSet,
                  DeltaRangerQuerySubject.current(c.spark).groups().asScala.toSet
                )
              case "configuration" =>
                c.spark.conf.set("append-idempotency.changed", "true")
                assertEquals("true", c.spark.conf.get("append-idempotency.changed"))
              case "source-literal" => bytes(0) = 'z'.toByte; assertNotEquals(firstByte, bytes(0))
              case "command-namespace" =>
                namespace(0) = "changed"; assertNotEquals(originalNamespace, namespace(0))
            }
            c.trace.record(
              "mutation-fired",
              s"phase=$phase mode=$mode change=$change sameTransaction=true"
            )
          }
          if (phase == "scalar") c.observe = {
            case _: Aggregate if !fired => mutate()
            case _                      => ()
          }
          else
            sourcePolicy.onRequest = request => {
              if (
                !fired && request.getAccessType == "select" && c.transaction
                  .exists(t => computed(t.snapshot))
              ) mutate()
            }
          try {
            val failure = assertThrows(
              classOf[SecurityException],
              () => { association.hasBeenExecuted(command, c.spark); () }
            )
            assertTrue(fired, s"Mutation was not reached: $phase/$mode/$change")
            val expected = change match {
              case "source-policy" | "target-policy" | "source-roles" | "target-roles" =>
                "Delta Ranger publication changed; reanalyze the query"
              case "subject"        => "Kyuubi session identity contradicts authenticated query UGI"
              case "source-literal" => "logical binding changed"
              case "command-namespace" => "target catalog association changed"
              case "source-id" | "source-token" if phase == "after" =>
                "Delta retained registration validation rejected"
              case "groups" if phase == "after"        => "subject changed"
              case "configuration" if phase == "after" => "configuration changed"
              case _ => "Native Delta registration binding is unavailable"
            }
            val rejection =
              if (
                phase == "after" &&
                (change == "source-policy" || change == "source-roles")
              ) {
                assertEquals("Delta Ranger evaluation rejected", failure.getMessage)
                assertEquals(classOf[SecurityException], failure.getCause.getClass)
                failure.getCause
              } else failure
            assertTrue(
              rejection.getMessage.contains(expected),
              s"$phase/$mode/$change: $failure expected $expected"
            )
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
            c.trace.record("mutation-rejected", failure.toString)
          } finally {
            c.observe = _ => ()
            sourcePolicy.onRequest = _ => ()
            c.spark.conf.unset("append-idempotency.changed")
            c.spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
            UserGroupInformation.createUserForTesting(user.getShortUserName, groups)
            registration.properties.clear(); registration.properties.putAll(properties.asJava)
            bytes(0) = firstByte
            namespace(0) = originalNamespace
          }
        }
      finally sourcePolicy.engine.close()
    }
  }

  @Test
  def associationRejectsUnqualifiedResetBeforeOpeningTransaction(): Unit = {
    Seq("true", "FALSE").foreach { reset =>
      withCase("append-reset-" + reset) { c =>
        configure(c)
        c.spark.conf.set(Reset, reset)
        val command = captured(c)
        val association = capture(c, command)
        rejected("Native idempotency entry rejected") {
          association.hasBeenExecuted(command, c.spark)
        }
        assertTrue(c.transaction.isEmpty)
        assertTrue(c.aggregates.isEmpty)
      }
    }
  }

  @Test
  def scalarExceptionClosesOnlyItsOperation(): Unit = withCase("append-scalar-exception") { c =>
    configure(c)
    val command = captured(c)
    val target = targetOf(c, command)
    val injected = new IllegalStateException("finite scalar callback")
    var fired = false
    c.observe = {
      case _: Aggregate if !fired => fired = true; throw injected
      case _                      => ()
    }
    target.log().withNewTransaction(Some(target.descriptor())) { txn =>
      val owner = target.bindTransaction(c.spark, txn)
      try {
        val failure = assertThrows(
          classOf[IllegalStateException],
          () => {
            DeltaRangerNativeIdempotency.readInTransaction(owner, txn, Map.empty); ()
          }
        )
        assertSame(injected, failure)
        assertTrue(fired)
        assertSame(txn, OptimisticTransaction.getActive.get)
        assertNull(DeltaRangerNativeOperation.current())
        owner.requireTransaction(txn)
        assertThrows(classOf[SecurityException], () => c.operation.get.requireEvidence(c.spark))
      } finally owner.close()
    }
  }

  private def capture(c: Case, command: AppendData): DeltaRangerAppendCommandBinding =
    DeltaRangerAppendCommandBinding.capture(
      c.spark,
      command,
      NativeSelectFixture.wrapper(c.spark, "a")
    )

  private def targetOf(
      c: Case,
      command: AppendData
  ): DeltaRangerRegistrationBindings.BoundAppendTarget =
    NativeSelectFixture
      .wrapper(c.spark, "a")
      .registrationBindings()
      .resolveAppendTarget(
        command.table.asInstanceOf[DataSourceV2Relation].table.asInstanceOf[DeltaTableV2]
      )

  private def rejected(message: String)(body: => Any): Unit = {
    val error = assertThrows(classOf[SecurityException], () => { body; () })
    assertTrue(error.getMessage.contains(message), error.toString)
  }

  private def configure(c: Case, mode: String = "duplicate"): Unit = {
    c.spark.conf.set(TxnApp, if (mode == "new") "new-app" else "probe-app")
    c.spark.conf.set(TxnVersion, if (mode == "higher") "2" else "1")
    c.spark.conf.set(Reset, "false")
  }

  private def captured(
      c: Case,
      frontend: String = "insertInto",
      sql: String = "SELECT id, secret, region FROM b.db.t"
  ): AppendData = {
    val data = c.spark.sql(sql)
    val source = data.queryExecution.analyzed
    val receipt = DeltaRangerDecisionReceipt.consumeOwned(source, c.spark, optimized = false)
    assertTrue(receipt.obligations.nonEmpty)
    val failure = assertThrows(
      classOf[SecurityException],
      () => {
        if (frontend == "insertInto") data.write.mode("append").insertInto("a.db.t")
        else data.write.mode("append").saveAsTable("a.db.t")
      }
    )
    assertEquals("Derived or reused Delta plans are not qualified", failure.getMessage)
    val command = c.appends.filter(a => a.resolved && (a.query eq source)).last
    assertTrue(command.write.isEmpty)
    assertTrue(command.analyzedQuery.isEmpty)
    assertTrue(command.writeOptions.isEmpty)
    c.trace.record(
      "actual-command",
      s"frontend=$frontend command=${c.trace.id(command)} source=${c.trace.id(source)}"
    )
    command
  }
}
