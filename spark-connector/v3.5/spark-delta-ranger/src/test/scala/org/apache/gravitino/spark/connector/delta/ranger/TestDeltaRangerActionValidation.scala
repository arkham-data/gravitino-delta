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

import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}
import java.nio.file.{Files, Path, Paths}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.mockito.Mockito._

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerActionValidation {
  private val tag =
    TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")

  @Test
  def unchangedActionReevaluatesCompleteOriginalInventoryAtOneFreshTime(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt =
        captured(spark, "SELECT x.secret,y.secret FROM a.db.t x JOIN a.db.t y ON x.id=y.id")
      policy.requests.clear()
      policy.events.clear()
      validate(receipt, spark)
      assertEquals(2, receipt.obligations.size)
      assertEquals(2, policy.events.asScala.count(_ == "filter"))
      assertEquals(4, policy.events.asScala.count(_ == "mask"))
      assertEquals(4, policy.events.asScala.count(_.startsWith("access:")))
      assertEquals(1, policy.requests.asScala.map(_.getAccessTime.toInstant).distinct.size)
      assertTrue(policy.requests.asScala.forall(_.getAccessTime.toInstant.isAfter(receipt.instant)))
      val events = policy.events.asScala.toVector
      assertEquals(4, events.takeWhile(_.startsWith("access:")).size)
    }

  @Test
  def accessRevokeRejectsBeforeAnyDecisionEvaluation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      policy.events.clear()
      policy.denied.add("a:secret")
      rejected("denied", () => validate(receipt, spark))
      assertTrue(policy.events.asScala.forall(_.startsWith("access:")))
    }

  @Test
  def unchangedPublicationCannotHideChangedMaskOrFilter(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      policy.mask = "upper({col})"
      rejected("action decision changed", () => validate(receipt, spark))
      policy.mask = "concat({col}, '-masked')"
      policy.filter = "region = 'drop'"
      rejected("action decision changed", () => validate(receipt, spark))
    }

  @Test
  def compatibleDataAdvanceBetweenActionsKeepsOriginalReceiptAndSnapshot(): Unit =
    withHistory { (spark, policy, publish) =>
      val receipt = captured(spark)
      val bound = receipt.obligations.head.binding
      validate(receipt, spark)
      publish(1L)
      NativeSelectFixture
        .bindingFixture(spark, "a")
        .properties
        .put("delta.registration.receipt.nativeVersion", "1")
      validate(receipt, spark)
      validate(receipt, spark)
      assertEquals(0L, bound.index().snapshotAtAnalysis.version)
      assertEquals(1L, bound.log().unsafeVolatileSnapshot.version)
      assertSame(bound, receipt.obligations.head.binding)
      // Ordinary analysis replay still requires its exact captured publication.
      rejected("registration binding", () => receipt.revalidate())
    }

  @Test
  def nativeAdvanceDuringWholeActionIsRejectedEvenWhenPublicationStaysUnchanged(): Unit =
    withHistory { (spark, policy, publish) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      policy.onRequest = _ => publish(1L)
      rejected("action observation changed", () => validate(receipt, spark))
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def nativeAdvanceInsideRegistrationBracketIsRejected(): Unit =
    withHistory { (spark, policy, publish) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val afterRead = registration.reads.get() + 2
      registration.onRead = count => if (count == afterRead) publish(1L)
      rejected("retained registration", () => validate(receipt, spark))
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def changedNativeSchemaRejectsWithoutReplacementTableLoad(): Unit =
    withHistory { (spark, policy, publish) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      publish(1L)
      publish(2L)
      rejected("retained registration", () => validate(receipt, spark))
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def absentFilterAndNoneMaskRemainObligationsAndTimeDependentChangesReject(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      policy.filter = null
      policy.maskColumn = "unused"
      val receipt = captured(spark)
      validate(receipt, spark)
      val stamp = policy.engine.snapshotStamp()
      policy.onRequest = request =>
        if (request.getAccessTime.toInstant.isAfter(receipt.instant)) policy.filter = "id = 1L"
      rejected("action decision changed", () => validate(receipt, spark))
      policy.engine.requireSnapshot(stamp)
      policy.onRequest = request => {
        policy.filter = null
        if (request.getAccessTime.toInstant.isAfter(receipt.instant)) {
          policy.maskColumn = "secret"
          policy.maskType = "MASK_NULL"
        }
      }
      rejected("action decision changed", () => validate(receipt, spark))
      policy.engine.requireSnapshot(stamp)
      assertTrue(policy.events.asScala.exists(_.startsWith("access:")))
    }

  @Test
  def twoCatalogsAuthorizeEveryOriginalResourceBeforeAnyDecision(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, first) =>
      val second = new TestDeltaRangerNativeSelect.Fixture
      second.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("b" -> second)
      try {
        val receipt =
          captured(spark, "SELECT a.secret,b.secret FROM a.db.t a JOIN b.db.t b ON a.id=b.id")
        first.events.clear(); second.events.clear()
        first.requests.clear(); second.requests.clear()
        val order = scala.collection.mutable.ArrayBuffer.empty[String]
        first.onRequest = request => order += "a"
        second.onRequest = request => order += "b"
        validate(receipt, spark)
        assertEquals(Vector("a", "a", "b", "b"), order.take(4).toVector)
        assertEquals(
          1,
          (first.requests.asScala ++ second.requests.asScala)
            .map(_.getAccessTime.toInstant)
            .distinct
            .size
        )
        // Denial on the second engine cannot allow even the first engine's rewrite requests.
        first.events.clear(); second.events.clear()
        second.denied.add("b:secret")
        rejected("access denied", () => validate(receipt, spark))
        assertTrue((first.events.asScala ++ second.events.asScala).forall(_.startsWith("access:")))
        second.denied.clear()
        second.start()
        rejected("publication changed", () => validate(receipt, spark))
      } finally second.engine.close()
    }

  @Test
  def sharedNativeLocationKeepsCanonicalOwnersAndFinalCrossCatalogEngineChecks(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, first) =>
      NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
      val second = new TestDeltaRangerNativeSelect.Fixture
      second.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("b" -> second)
      try {
        val receipt =
          captured(spark, "SELECT a.secret,b.secret FROM a.db.t a JOIN b.db.t b ON a.id=b.id")
        val a = receipt.obligations.find(_.scan.catalog == "a").get.binding
        val b = receipt.obligations.find(_.scan.catalog == "b").get.binding
        assertSame(a.log(), b.log())
        assertNotSame(a.descriptor(), b.descriptor())
        first.requests.clear(); second.requests.clear()
        validate(receipt, spark)
        assertEquals(Set("a"), first.requests.asScala.map(_.getResource.getValue("catalog")).toSet)
        assertEquals(Set("b"), second.requests.asScala.map(_.getResource.getValue("catalog")).toSet)
        val fixture = NativeSelectFixture.bindingFixture(spark, "b")
        val lastRead = fixture.reads.get() + 4
        fixture.onRead = count => if (count == lastRead) first.start()
        rejected("publication changed", () => validate(receipt, spark))
        assertEquals(lastRead, fixture.reads.get())
      } finally second.engine.close()
    }

  @Test
  def eliminatedScanStillRequiresItsOriginalAccessAndRegistration(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val frame = spark.sql("SELECT secret FROM a.db.t WHERE false")
      val receipt = frame.queryExecution.analyzed.getTagValue(tag).get.receipt
      assertTrue(
        DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).occurrences.isEmpty
      )
      validate(receipt, spark)
      policy.denied.add("a:secret")
      rejected("access denied", () => validate(receipt, spark))
      policy.denied.clear()
      NativeSelectFixture
        .bindingFixture(spark, "a")
        .properties
        .put("delta.registration.receipt.token", "replacement")
      rejected("retained registration", () => validate(receipt, spark))
    }

  @Test
  def registrationReplacementPendingMissingAndLogicalSchemaChangesRejectAtRetainedBoundary(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt = captured(spark)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      validate(receipt, spark)
      val properties = fixture.properties.asScala.toMap
      Seq(
        "id" -> "2",
        "generation" -> "2",
        "token" -> "replacement",
        "location" -> "/tmp/foreign-delta",
        "nativeId" -> "foreign-native",
        "state" -> "ADOPTING",
        "nativeVersion" -> "999"
      ).foreach { case (key, value) =>
        fixture.properties.put("delta.registration.receipt." + key, value)
        rejected("retained registration", () => validate(receipt, spark))
        fixture.properties.clear(); fixture.properties.putAll(properties.asJava)
        validate(receipt, spark)
      }
      fixture.onRead = _ => throw new IllegalStateException("missing table")
      rejected("retained registration", () => validate(receipt, spark))
      fixture.onRead = _ => ()
      val columns = fixture.columns
      fixture.columns = columns.dropRight(1)
      rejected("retained registration", () => validate(receipt, spark))
      fixture.columns = columns
      validate(receipt, spark)
    }

  @Test
  def sessionUserGroupsAndConfigurationChangesRejectBeforeEvaluation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt = captured(spark)
      validate(receipt, spark)
      policy.events.clear()
      rejected("session changed", () => validate(receipt, spark.newSession()))
      val user = UserGroupInformation.getCurrentUser.getShortUserName
      Seq((user, Array("changed-group")), ("different-user", Array("changed-group"))).foreach {
        case (name, groups) =>
          UserGroupInformation
            .createUserForTesting(name, groups)
            .doAs(new PrivilegedExceptionAction[Unit] {
              override def run(): Unit = rejected("subject changed", () => validate(receipt, spark))
            })
      }
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      rejected("configuration changed", () => validate(receipt, spark))
      assertTrue(policy.events.isEmpty)
    }

  @Test
  def completedRoleOnlyAndPolicyPublicationsInvalidateRetainedAction(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val first = captured(spark)
      validate(first, spark)
      val context = policy.plugin.getCurrentRangerAuthContext
      val roles = policy.plugin.getRoles
      roles.setRoleVersion(2L)
      when(policy.plugin.getRolesVersion).thenReturn(2L)
      context.setRoles(roles)
      policy.listener.contextChanged()
      assertSame(context, policy.plugin.getCurrentRangerAuthContext)
      assertEquals(1L, policy.engine.versions().policy())
      assertEquals(2L, policy.engine.versions().role())
      rejected("publication changed", () => validate(first, spark))
      val second = captured(spark)
      validate(second, spark)
      when(policy.plugin.getPoliciesVersion).thenReturn(2L)
      policy.start()
      rejected("publication changed", () => validate(second, spark))
      val third = captured(spark)
      validate(third, spark)
      policy.start()
      rejected("publication changed", () => validate(third, spark))
    }

  @Test
  def finalMetadataWorkPrecedesEngineSubjectConfigurationAndStructuralChecks(): Unit = {
    Seq("engine", "subject", "configuration", "structure").foreach { change =>
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        val plan =
          spark.sql("SELECT secret FROM a.db.t WHERE region='keep'").queryExecution.analyzed
        val receipt = plan.getTagValue(tag).get.receipt
        validate(receipt, spark)
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        val lastRead = fixture.reads.get() + 4
        fixture.onRead = count =>
          if (count == lastRead) change match {
            case "engine" => policy.start()
            case "subject" =>
              spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
            case "configuration" => spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
            case "structure" =>
              val literal = plan
                .collect { case node =>
                  node.expressions.flatMap(_.collect {
                    case l: Literal if l.dataType == StringType && l.value.toString == "keep" => l
                  })
                }
                .flatten
                .head
              literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]](0) =
                'z'.toByte
          }
        val expected = Map(
          "engine" -> "publication changed",
          "subject" -> "identity contradicts",
          "configuration" -> "configuration changed",
          "structure" -> "retained logical binding changed"
        )
        try rejected(expected(change), () => validate(receipt, spark))
        finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
        assertEquals(lastRead, fixture.reads.get())
      }
    }
  }

  @Test
  def finiteCatalogMetadataAndEngineLifetimesRejectBeforeOrDuringValidation(): Unit = {
    Seq("catalog", "metadata", "expiry", "engine").foreach { mode =>
      Seq(false, true).foreach { during =>
        NativeSelectFixture.withBoundSpark { (spark, policy) =>
          val receipt = captured(spark)
          validate(receipt, spark)
          val fixture = NativeSelectFixture.bindingFixture(spark, "a")
          def close(): Unit = mode match {
            case "catalog"  => fixture.wrapper.close()
            case "metadata" => fixture.metadata.close()
            case "expiry"   => fixture.clock.set(TimeUnit.SECONDS.toNanos(301))
            case "engine"   => policy.engine.close()
          }
          if (during) {
            val lastRead = fixture.reads.get() + 4
            fixture.onRead = count => if (count == lastRead) close()
          } else close()
          if (!during && Set("catalog", "engine")(mode)) {
            val failure =
              assertThrows(classOf[IllegalStateException], () => validate(receipt, spark))
            assertEquals("Delta Ranger lease is closed", failure.getMessage)
          } else rejected("retained registration", () => validate(receipt, spark))
          assertNull(DeltaRangerNativeOperation.current())
        }
      }
    }
  }

  @Test
  def transientObservationIsBoundToExactOccurrenceAndCannotSerialize(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val receipt =
        captured(spark, "SELECT x.secret,y.secret FROM a.db.t x JOIN a.db.t y ON x.id=y.id")
      val first = receipt.obligations.head.binding.validateCurrent(spark)
      first.requireSame(receipt.obligations.head.binding.validateCurrent(spark))
      rejected(
        "action observation changed",
        () => first.requireSame(receipt.obligations(1).binding.validateCurrent(spark))
      )
      assertFalse(first.isInstanceOf[Product])
      val bytes = new ByteArrayOutputStream
      val output = new ObjectOutputStream(bytes)
      try assertThrows(classOf[NotSerializableException], () => output.writeObject(first))
      finally output.close()
      assertEquals("Current Delta action observation", first.toString)
    }

  private def withHistory(
      test: (SparkSession, TestDeltaRangerNativeSelect.Fixture, Long => Unit) => Unit
  ): Unit = {
    val commits = scala.collection.mutable.Map.empty[Long, (Path, Array[Byte])]
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark =>
          test(
            spark,
            policy,
            version => {
              val (path, bytes) = commits(version)
              Files.write(path, bytes)
              ()
            }
          ),
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark => {
          val location = NativeSelectFixture.locations("a")
          spark
            .sql("SELECT 4L id, 'tail' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(location)
          spark
            .sql("SELECT 'changed' id, 'new' secret, 'keep' region")
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(location)
          (1L to 2L).foreach { version =>
            val path = Paths.get(location, "_delta_log", f"$version%020d.json")
            commits(version) = (path, Files.readAllBytes(path))
            Files.delete(path)
          }
        }
      )
    finally policy.engine.close()
  }

  private def captured(
      spark: SparkSession,
      sql: String = "SELECT secret FROM a.db.t"
  ): DeltaRangerDecisionReceipt =
    spark.sql(sql).queryExecution.analyzed.getTagValue(tag).get.receipt

  private def validate(receipt: DeltaRangerDecisionReceipt, spark: SparkSession): Unit =
    receipt.validateAction(spark)

  private def rejected(part: String, action: () => Unit): Unit = {
    val failure = assertThrows(classOf[SecurityException], () => action())
    assertTrue(
      failure.getMessage.toLowerCase(java.util.Locale.ROOT).contains(part),
      failure.getMessage
    )
  }
}
