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

import java.time.Instant
import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}
import java.lang.reflect.Modifier
import java.nio.file.{Files, Path, Paths}
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.mockito.Mockito._
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendTargetReceipt {
  @Test
  def targetReceiptRequestsOnlyCanonicalUpdateAtEveryValidation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      policy.requests.clear()
      policy.events.clear()
      val before = Instant.now().minusMillis(1)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val firstValidation = Instant.now().minusMillis(1)
      receipt.validate(spark)
      assertTrue(policy.requests.get(1).getAccessTime.toInstant.isAfter(firstValidation))
      val secondValidation = Instant.now().minusMillis(1)
      receipt.validate(spark)
      assertTrue(policy.requests.get(2).getAccessTime.toInstant.isAfter(secondValidation))
      assertEquals(3, policy.requests.size())
      assertTrue(policy.requests.asScala.forall(_.getAccessType == "update"))
      assertTrue(policy.requests.asScala.forall(_.getResource.getValue("catalog") == "a"))
      assertTrue(policy.requests.asScala.forall(_.getResource.getValue("database") == "db"))
      assertTrue(policy.requests.asScala.forall(_.getResource.getValue("table") == "t"))
      assertTrue(policy.requests.asScala.forall(_.getResource.getValue("column") == "*"))
      val subject = DeltaRangerQuerySubject.current(spark)
      assertNotEquals("separate-metadata-reader", subject.user())
      assertTrue(policy.requests.asScala.forall(_.getUser == subject.user()))
      assertTrue(policy.requests.asScala.forall(_.getUserGroups == subject.groups()))
      assertTrue(policy.requests.asScala.forall(_.getAccessTime.toInstant.isAfter(before)))
      assertEquals("", target.descriptor().owner)
      assertTrue(policy.requests.asScala.forall(_.getResource.getOwnerUser == null))
      assertFalse(policy.events.asScala.exists(e => e == "filter" || e == "mask"))
      assertEquals("region = 'keep'", policy.filter)
      assertEquals("concat({col}, '-masked')", policy.mask)
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def selectOnlyAndWriteOnlyDenyButUpdateOnlyIssuesAndReevaluates(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      Seq("select", "write", "update").foreach { allowed =>
        policy.onRequest = request => {
          if (request.getAccessType == allowed) policy.denied.remove("a:*")
          else policy.denied.add("a:*")
        }
        if (allowed == "update") DeltaRangerAppendTargetReceipt.capture(spark, target)
        else
          assertThrows(
            classOf[SecurityException],
            () => DeltaRangerAppendTargetReceipt.capture(spark, target)
          )
      }
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      policy.onRequest = _ => ()
      val stamp = policy.engine.snapshotStamp()
      policy.denied.add("a:*")
      val count = policy.requests.size()
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      assertEquals(count + 1, policy.requests.size())
      policy.engine.requireSnapshot(stamp)
      assertFalse(policy.events.asScala.exists(e => e == "filter" || e == "mask"))
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def sameLocationCatalogsRetainSeparateCanonicalEngines(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, first) =>
      NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
      val second = new TestDeltaRangerNativeSelect.Fixture
      second.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("b" -> second)
      try {
        val a = bound(spark)
        val b = bound(spark, "b")
        assertSame(a.log(), b.log())
        assertNotSame(a.descriptor(), b.descriptor())
        first.requests.clear(); second.requests.clear()
        val receiptA = DeltaRangerAppendTargetReceipt.capture(spark, a)
        val receiptB = DeltaRangerAppendTargetReceipt.capture(spark, b)
        assertEquals(Set("a"), first.requests.asScala.map(_.getResource.getValue("catalog")).toSet)
        assertEquals(Set("b"), second.requests.asScala.map(_.getResource.getValue("catalog")).toSet)
        second.denied.add("b:*")
        receiptA.validate(spark)
        assertThrows(classOf[SecurityException], () => receiptB.validate(spark))
        assertFalse(
          (first.events.asScala ++ second.events.asScala).exists(e => e == "filter" || e == "mask")
        )
      } finally second.engine.close()
    }

  @Test
  def sessionUserGroupsAndArbitraryConfigurationRejectBeforeAccess(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, bound(spark))
      policy.requests.clear()
      assertThrows(classOf[SecurityException], () => receipt.validate(spark.newSession()))
      val original = UserGroupInformation.getCurrentUser
      val user = original.getShortUserName
      val groups = original.getGroupNames
      val changedGroups =
        if (groups.isEmpty) Array("append-receipt-changed-group") else Array.empty[String]
      assertNotEquals(groups.toSet, changedGroups.toSet)
      try
        Seq(user, "different-user").foreach { name =>
          UserGroupInformation
            .createUserForTesting(name, changedGroups)
            .doAs(new PrivilegedExceptionAction[Unit] {
              override def run(): Unit = {
                val actualGroups = UserGroupInformation.getCurrentUser.getGroupNames.toSet
                assertEquals(changedGroups.toSet, actualGroups)
                assertNotEquals(groups.toSet, actualGroups)
                assertThrows(classOf[SecurityException], () => receipt.validate(spark))
                ()
              }
            })
        }
      finally UserGroupInformation.createUserForTesting(user, groups)
      val timezone = spark.sessionState.conf.getAllConfs.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      restore(spark, "spark.sql.session.timeZone", timezone)
      spark.conf.set("task2.arbitrary.configuration", "changed")
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      spark.conf.unset("task2.arbitrary.configuration")
      assertTrue(policy.requests.isEmpty)
      receipt.validate(spark)
      assertEquals(1, policy.requests.size())
    }

  @Test
  def transactionVersionRemovalHasNoResetExemption(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val version = "spark.databricks.delta.write.txnVersion"
      spark.conf.set("spark.databricks.delta.write.txnAppId", "task2-app")
      spark.conf.set(version, "7")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "true")
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, bound(spark))
      policy.requests.clear()
      spark.conf.unset(version)
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      assertTrue(policy.requests.isEmpty)
      assertFalse(spark.conf.getOption(version).isDefined)
      spark.conf.set(version, "7")
      receipt.validate(spark)
      policy.onRequest = _ => spark.conf.unset(version)
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      assertFalse(spark.conf.getOption(version).isDefined)
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def completedPolicyAndRoleOnlyPublicationsRejectBeforeAccess(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      Seq("policy", "roles", "same-version").foreach { change =>
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        policy.requests.clear()
        publication(policy, change)
        assertThrows(classOf[SecurityException], () => receipt.validate(spark))
        assertTrue(policy.requests.isEmpty)
      }
    }

  @Test
  def registrationReplacementRejectsBeforeAccessAndDuringEitherObservation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val original = fixture.properties.asScala.toMap
      val columns = fixture.columns
      for (
        phase <- Seq(0, 1, 2, 3, 4); change <- Seq("generation", "location", "nativeId", "schema")
      ) {
        def mutate(): Unit = if (change == "schema") fixture.columns = columns.dropRight(1)
        else
          fixture.properties.put(
            "delta.registration.receipt." + change,
            if (change == "generation") "2"
            else if (change == "location") "/tmp/replaced-target"
            else "replaced-native"
          )
        policy.requests.clear()
        val at = fixture.reads.get() + phase
        if (phase == 0) mutate()
        else fixture.onRead = count => if (count == at) mutate()
        assertThrows(classOf[SecurityException], () => receipt.validate(spark))
        assertEquals(if (phase <= 2) 0 else 1, policy.requests.size())
        assertNull(DeltaRangerNativeOperation.current())
        fixture.onRead = _ => ()
        fixture.properties.clear(); fixture.properties.putAll(original.asJava);
        fixture.columns = columns
        receipt.validate(spark)
      }
    }

  @Test
  def generationChangeDuringAllowRejectsIssuanceAndRetainedValidation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      policy.onRequest = _ => fixture.properties.put("delta.registration.receipt.generation", "2")
      policy.requests.clear()
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      assertEquals(1, policy.requests.size())
      fixture.properties.put("delta.registration.receipt.generation", "1")
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerAppendTargetReceipt.capture(spark, target)
      )
      assertEquals(2, policy.requests.size())
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def finalPublicationWorkPrecedesFreshEngineSubjectAndFullConfigurationChecks(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      for (
        phase <- Seq("access", "last-read");
        change <- Seq("policy", "roles", "subject", "configuration")
      ) {
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        def mutate(): Unit = change match {
          case "policy" | "roles" => publication(policy, change)
          case "subject" =>
            spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
          case "configuration" => spark.conf.set("task2.arbitrary.configuration", "changed")
        }
        policy.requests.clear()
        val at = fixture.reads.get() + 4
        if (phase == "access") policy.onRequest = _ => mutate()
        else fixture.onRead = count => if (count == at) mutate()
        try {
          assertThrows(classOf[SecurityException], () => receipt.validate(spark))
          assertEquals(1, policy.requests.size())
          if (phase == "last-read") assertEquals(at, fixture.reads.get())
          assertNull(DeltaRangerNativeOperation.current())
        } finally {
          policy.onRequest = _ => ()
          fixture.onRead = _ => ()
          spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
          spark.conf.unset("task2.arbitrary.configuration")
        }
      }
    }

  @Test
  def closedOrExpiredAuthorityRejectsBeforeAccessAndAfterInitialAllow(): Unit = {
    for (
      mode <- Seq("catalog", "metadata", "engine", "expiry");
      phase <- Seq("before", "access", "last-read")
    ) {
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, bound(spark))
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        def close(): Unit = mode match {
          case "catalog"  => fixture.wrapper.close()
          case "metadata" => fixture.metadata.close()
          case "engine"   => policy.engine.close()
          case "expiry"   => fixture.clock.set(TimeUnit.SECONDS.toNanos(301))
        }
        policy.requests.clear()
        val at = fixture.reads.get() + 4
        if (phase == "before") close()
        else if (phase == "access") policy.onRequest = _ => close()
        else fixture.onRead = count => if (count == at) close()
        // Closed engine leases use IllegalStateException; metadata checks use SecurityException.
        val failure = assertThrows(classOf[RuntimeException], () => receipt.validate(spark))
        assertTrue(
          failure.isInstanceOf[SecurityException] || failure.isInstanceOf[IllegalStateException]
        )
        assertEquals(if (phase == "before") 0 else 1, policy.requests.size())
        assertNull(DeltaRangerNativeOperation.current())
      }
    }
  }

  @Test
  def compatibleDataAdvanceBeforeFirstHeadRemainsValid(): Unit =
    withHistory() { (spark, policy, target, publish) =>
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val at = fixture.reads.get() + 1
      fixture.onRead = count => if (count == at) publish("data")
      receipt.validate(spark)
      fixture.onRead = _ => ()
      receipt.validate(spark)
      assertEquals(1L, target.log().unsafeVolatileSnapshot.version)
      assertEquals(0L, target.table().initialSnapshot.version)
      assertNull(DeltaRangerNativeOperation.current())
    }

  @Test
  def nativeAdvanceAcrossAllowOrInsideEitherObservationRejects(): Unit = {
    Seq("access", "before-final-head", "after-first-head", "after-final-head").foreach { phase =>
      withHistory() { (spark, policy, target, publish) =>
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        val fixture = NativeSelectFixture.bindingFixture(spark, "a")
        policy.requests.clear()
        if (phase == "access") policy.onRequest = _ => publish("data")
        else {
          val offset =
            Map("before-final-head" -> 2, "after-first-head" -> 3, "after-final-head" -> 4)(phase)
          val at = fixture.reads.get() + offset
          fixture.onRead = count => if (count == at) publish("data")
        }
        assertThrows(classOf[SecurityException], () => receipt.validate(spark))
        assertEquals(if (phase == "before-final-head") 0 else 1, policy.requests.size())
        assertEquals(1L, target.log().unsafeVolatileSnapshot.version)
        assertNull(DeltaRangerNativeOperation.current())
      }
    }
  }

  @Test
  def nativePropertySchemaAndIdChangesDuringAllowReject(): Unit = {
    Seq("property", "schema", "nativeId").foreach { change =>
      withHistory(change) { (spark, policy, target, publish) =>
        val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
        policy.requests.clear()
        policy.onRequest = _ => publish(change)
        assertThrows(classOf[SecurityException], () => receipt.validate(spark))
        assertEquals(1, policy.requests.size())
        val head = target.log().unsafeVolatileSnapshot
        assertEquals(1L, head.version)
        change match {
          case "property" => assertEquals("true", head.metadata.configuration("delta.appendOnly"))
          case "schema" =>
            assertEquals(Seq("id", "secret", "region", "extra"), head.schema.fieldNames.toSeq)
          case "nativeId" => assertEquals("replacement-native-id", head.metadata.id)
        }
        assertNull(DeltaRangerNativeOperation.current())
      }
    }
  }

  @Test
  def copiedDescriptorsAndOwnersCannotBorrowTargetReceiptAuthority(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val native = target.table()
      val bindings = NativeSelectFixture.wrapper(spark, "a").registrationBindings()
      Seq(
        native.copy(),
        native.copy(catalogTable = Some(target.descriptor().copy())),
        native.copy(catalogTable = Some(target.descriptor().copy(owner = "different-owner")))
      ).foreach { copy =>
        assertThrows(classOf[SecurityException], () => bindings.resolveAppendTarget(copy))
      }
      receipt.validate(spark)
      bindings.load(Identifier.of(Array("db"), "t"), () => native)
      policy.requests.clear()
      assertThrows(classOf[SecurityException], () => receipt.validate(spark))
      assertTrue(policy.requests.isEmpty)
    }

  @Test
  def receiptAndTargetAreDriverLocalWithoutTransferablePrincipalOrPlanAuthority(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      Seq(target, receipt).foreach { value =>
        assertFalse(classOf[Serializable].isAssignableFrom(value.getClass))
        assertFalse(classOf[Product].isAssignableFrom(value.getClass))
        val output = new ObjectOutputStream(new ByteArrayOutputStream)
        try assertThrows(classOf[NotSerializableException], () => output.writeObject(value))
        finally output.close()
        if (value eq target)
          assertTrue(
            value.getClass.getDeclaredConstructors
              .filterNot(_.isSynthetic)
              .forall(c => Modifier.isPrivate(c.getModifiers))
          )
        assertFalse(
          value.getClass.getDeclaredMethods.exists(m =>
            !Modifier.isPrivate(m.getModifiers) &&
              (m.getReturnType == classOf[DeltaRangerQuerySubject] ||
                classOf[TreeNodeTag[_]].isAssignableFrom(m.getReturnType) ||
                classOf[scala.collection.Map[_, _]].isAssignableFrom(m.getReturnType))
          )
        )
        assertFalse(value.toString.contains(NativeSelectFixture.locations("a")))
        assertFalse(value.toString.contains("private-wrapper-token"))
      }
      assertEquals("Delta append target receipt", receipt.toString)
    }

  @Test
  def validUpdateReceiptDoesNotOpenInsertRawPathsOrUnreceiptedSelect(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val target = bound(spark)
      val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
      val location = NativeSelectFixture.locations("a")
      val before = manifest(Paths.get(location))
      val version = target.log().unsafeVolatileSnapshot.version
      assertThrows(
        classOf[Exception],
        () => spark.sql("INSERT INTO a.db.t VALUES (9, 'keep', 'new')").collect()
      )
      assertThrows(
        classOf[Exception],
        () => spark.sql(s"INSERT INTO delta.`$location` VALUES (9, 'keep', 'new')").collect()
      )
      assertThrows(
        classOf[Exception],
        () => spark.sql(s"SELECT * FROM delta.`$location`").collect()
      )
      assertEquals(version, target.log().unsafeVolatileSnapshot.version)
      assertEquals(before, manifest(Paths.get(location)))
      assertNull(DeltaRangerNativeOperation.current())
      // A named SELECT may replace the loaded target binding; retain the UPDATE validation first.
      receipt.validate(spark)
      val plan = spark.sql("SELECT secret FROM a.db.t").queryExecution.analyzed.clone()
      val tag =
        TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")
      plan.foreach(_.unsetTagValue(tag))
      assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(plan, spark))
      assertEquals(before, manifest(Paths.get(location)))
      // Inspect durable rows through a fresh native-only test control after all admission checks.
      val conf = spark.sparkContext.getConf
      spark.stop()
      SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
      DeltaLog.clearCache()
      val native = SparkSession
        .builder()
        .config(conf)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate()
      try {
        val rows = native.read.format("delta").load(location).collect().map(_.toSeq).toSet
        assertEquals(
          Set(Seq(1L, "a-one", "keep"), Seq(2L, "a-two", "drop"), Seq(3L, null, "keep")),
          rows
        )
        assertEquals(before, manifest(Paths.get(location)))
      } finally {
        native.stop()
        SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
      }
    }

  private def restore(spark: SparkSession, key: String, value: Option[String]): Unit =
    value match {
      case Some(setting) => spark.conf.set(key, setting)
      case None          => spark.conf.unset(key)
    }

  private def publication(policy: TestDeltaRangerNativeSelect.Fixture, change: String): Unit = {
    if (change == "roles") {
      val roles = policy.plugin.getRoles
      val version = policy.plugin.getRolesVersion + 1L
      roles.setRoleVersion(version)
      when(policy.plugin.getRolesVersion).thenReturn(version)
      policy.plugin.getCurrentRangerAuthContext.setRoles(roles)
      policy.listener.contextChanged()
    } else {
      if (change == "policy") {
        val version = policy.plugin.getPoliciesVersion + 1L
        when(policy.plugin.getPoliciesVersion).thenReturn(version)
      }
      policy.start()
    }
  }

  private def manifest(root: Path): Map[String, Vector[Byte]] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(p => root.relativize(p).toString -> Files.readAllBytes(p).toVector)
        .toMap
    finally paths.close()
  }

  private def withHistory(change: String = "data")(
      test: (
          SparkSession,
          TestDeltaRangerNativeSelect.Fixture,
          DeltaRangerRegistrationBindings.BoundAppendTarget,
          String => Unit
      ) => Unit
  ): Unit = {
    var staged = Map.empty[Path, Array[Byte]]
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val target = bound(spark)
          test(
            spark,
            policy,
            target,
            requested => {
              assertEquals(change, requested)
              staged.foreach { case (path, bytes) => Files.write(path, bytes) }
            }
          )
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = spark => {
          val location = NativeSelectFixture.locations("a")
          change match {
            case "data" =>
              spark
                .sql("SELECT 4L id, 'tail' secret, 'keep' region")
                .write
                .format("delta")
                .mode("append")
                .save(location)
            case "property" =>
              spark.sql(
                s"ALTER TABLE delta.`$location` SET TBLPROPERTIES ('delta.appendOnly' = 'true')"
              )
            case "schema" => spark.sql(s"ALTER TABLE delta.`$location` ADD COLUMNS (extra STRING)")
            case "nativeId" =>
              val transaction = DeltaLog.forTable(spark, location).startTransaction(None)
              transaction.commit(
                Seq(transaction.metadata.copy(id = "replacement-native-id")),
                DeltaOperations.ManualUpdate
              )
          }
          val head = DeltaLog.forTable(spark, location).update()
          assertEquals(1L, head.version)
          change match {
            case "property" => assertEquals("true", head.metadata.configuration("delta.appendOnly"))
            case "schema" =>
              assertEquals(Seq("id", "secret", "region", "extra"), head.schema.fieldNames.toSeq)
            case "nativeId" => assertEquals("replacement-native-id", head.metadata.id)
            case "data"     => ()
          }
          // Retain each genuine native JSON/CRC pair and its Hadoop checksum sidecars together.
          // Missing CRC reconstruction has a separately unqualified installed-admission route.
          val log = Paths.get(location, "_delta_log")
          val paths = Files.list(log)
          try
            staged = paths
              .iterator()
              .asScala
              .filter(_.getFileName.toString.contains("00000000000000000001."))
              .map(path => path -> Files.readAllBytes(path))
              .toMap
          finally paths.close()
          assertTrue(staged.keys.exists(_.getFileName.toString == "00000000000000000001.crc"))
          staged.keys.foreach(Files.delete)
          DeltaLog.clearCache()
        }
      )
    finally policy.engine.close()
  }

  private def bound(
      spark: SparkSession,
      catalog: String = "a"
  ): DeltaRangerRegistrationBindings.BoundAppendTarget = {
    val wrapper = NativeSelectFixture.wrapper(spark, catalog)
    val table = wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
    wrapper.registrationBindings().resolveAppendTarget(table)
  }
}
