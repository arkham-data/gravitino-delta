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

import java.nio.file.Paths
import java.security.PrivilegedExceptionAction
import java.util.concurrent.TimeUnit
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.mockito.Mockito.{mock, when}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerAppendCommandBinding {
  import InstalledAppendBoundaryProbe._

  @Test
  def actualFrontendInventory(): Unit =
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      withObserved(frontend, "SELECT id, secret, region FROM b.db.t") { observed =>
        assertSame(observed.sourceRoot, observed.command.query)
      }
    }

  @Test
  def actualNamedFrontendsBindTheirOwnTargetsAndReceiptedRoots(): Unit =
    Seq("insertInto", "saveAsTable").foreach { frontend =>
      withObserved(frontend, "SELECT id, secret, region FROM b.db.t") { observed =>
        observed.targetPolicy.requests.clear()
        observed.targetPolicy.events.clear()
        observed.sourcePolicy.requests.clear()
        observed.sourcePolicy.events.clear()
        val association = DeltaRangerAppendCommandBinding.capture(
          observed.spark,
          observed.command,
          observed.catalog
        )
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

  @Test
  def retainedAssociationRejectsCopiesAndDifferentInputs(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val command = observed.command
      val association = capture(observed)
      Seq(
        command.copy(),
        command.copy(query = command.query.clone()),
        command.copy(isByName = !command.isByName),
        command.copy(writeOptions = Map("mergeSchema" -> "true")),
        command.copy(analyzedQuery = Some(command.query))
      ).foreach { candidate =>
        rejects("command identity changed") { association.validate(candidate, observed.spark) }
      }
      association.validate(command, observed.spark)
    }

  @Test
  def captureRejectsQualifiedSourceAtUnqualifiedCommandStages(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val command = observed.command
      observed.targetPolicy.requests.clear()
      Seq(
        command.copy(analyzedQuery = Some(command.query)),
        command.copy(writeOptions = Map("mergeSchema" -> "true")),
        command.copy(write = Some(mock(classOf[Write])))
      ).foreach { candidate =>
        rejects("unqualified command stage") {
          DeltaRangerAppendCommandBinding.capture(observed.spark, candidate, observed.catalog)
        }
      }
      val relation = command.table.asInstanceOf[DataSourceV2Relation]
      val foreign = mock(classOf[CatalogPlugin])
      when(foreign.name()).thenReturn("a")
      val invalid = Seq(
        relation.copy(catalog = None) -> "unqualified named target",
        relation.copy(identifier = None) -> "unqualified named target",
        relation.copy(options =
          new CaseInsensitiveStringMap(Map("x" -> "y").asJava)
        ) -> "unqualified named target",
        relation.copy(identifier =
          Some(Identifier.of(Array("other"), "t"))
        ) -> "target catalog association changed",
        relation.copy(identifier =
          Some(Identifier.of(Array("db"), "other"))
        ) -> "target catalog association changed",
        relation.copy(catalog = Some(foreign)) -> "target catalog association changed"
      )
      invalid.foreach { case (target, reason) =>
        rejects(reason) {
          DeltaRangerAppendCommandBinding.capture(
            observed.spark,
            command.copy(table = target),
            observed.catalog
          )
        }
      }
      rejects("missing input") {
        DeltaRangerAppendCommandBinding.capture(null, command, observed.catalog)
      }
      rejects("missing input") {
        DeltaRangerAppendCommandBinding.capture(observed.spark, null, observed.catalog)
      }
      rejects("missing input") {
        DeltaRangerAppendCommandBinding.capture(observed.spark, command, null)
      }
      assertTrue(observed.targetPolicy.requests.isEmpty)
    }

  @Test
  def actualTargetCannotBorrowOtherWrapperOrIndependentLoad(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val association = capture(observed)
      observed.targetPolicy.requests.clear()
      observed.sourcePolicy.requests.clear()
      rejects("target catalog association changed") {
        DeltaRangerAppendCommandBinding.capture(
          observed.spark,
          observed.command,
          NativeSelectFixture.wrapper(observed.spark, "b")
        )
      }
      assertTrue(observed.targetPolicy.requests.isEmpty)
      assertTrue(observed.sourcePolicy.requests.isEmpty)
      val relation = observed.command.table.asInstanceOf[DataSourceV2Relation]
      val independent = observed.catalog.loadTable(Identifier.of(Array("db"), "t"))
      assertNotSame(relation.table, independent)
      rejects("command identity changed") {
        association.validate(
          observed.command.copy(table = relation.copy(table = independent)),
          observed.spark
        )
      }
      val foreignPolicy = new TestDeltaRangerNativeSelect.Fixture
      val foreign = new NativeBoundCatalogFixture(observed.spark, foreignPolicy, "a")
      try {
        val registry = foreign.wrapper.registrationBindings()
        val foreignTable =
          foreign.wrapper.loadTable(Identifier.of(Array("db"), "t")).asInstanceOf[DeltaTableV2]
        val ownTarget = registry.resolveAppendTarget(foreignTable)
        ownTarget.requireRetained()
        ownTarget.requireLive()
        registry.requireLive()
        assertNotSame(observed.catalog, foreign.wrapper)
        assertNotSame(observed.catalog.registrationBindings(), registry)
        assertNotSame(relation.table, foreignTable)
        assertEquals(observed.catalog.name(), foreign.wrapper.name())
        observed.targetPolicy.requests.clear()
        foreignPolicy.requests.clear()
        observed.trace.record(
          "same-name-registry",
          s"owningWrapper=${observed.trace.id(observed.catalog)} foreignWrapper=${observed.trace.id(foreign.wrapper)} owningRegistry=${observed.trace.id(observed.catalog.registrationBindings())} foreignRegistry=${observed.trace.id(registry)} originalTarget=${observed.trace.id(relation.table)} foreignTarget=${observed.trace.id(foreignTable)} catalog=${foreign.wrapper.name()} foreignOwnTargetRetained=true"
        )
        rejects("Native Delta registration binding is unavailable") {
          DeltaRangerAppendCommandBinding.capture(observed.spark, observed.command, foreign.wrapper)
        }
        assertTrue(observed.targetPolicy.requests.isEmpty)
        assertTrue(foreignPolicy.requests.isEmpty)
        ownTarget.requireRetained()
        ownTarget.requireLive()
        observed.trace.record(
          "same-name-registry-rejected",
          "phase=exact-target-registry-resolution updateRequests=0 foreignOwnTargetStillRetained=true"
        )
      } finally {
        foreign.close()
        foreignPolicy.engine.close()
      }
    }

  @Test
  def targetIdentityAndPublicationChangesRejectRetainedAssociation(): Unit = {
    for (
      change <- Seq("id", "generation", "token", "nativeId", "schema");
      phase <- Seq("before", "capture-update")
    ) {
      withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
        val association = capture(observed)
        val fixture = NativeSelectFixture.bindingFixture(observed.spark, "a")
        def mutate(): Unit = change match {
          case "id" | "generation" =>
            fixture.properties.put("delta.registration.receipt." + change, "2")
          case "token" =>
            fixture.properties.put("delta.registration.receipt.token", "changed-token")
          case "nativeId" =>
            fixture.properties.put("delta.registration.receipt.nativeId", "changed-native-id")
          case "schema" => fixture.columns = fixture.columns.dropRight(1)
        }
        observed.targetPolicy.requests.clear()
        if (phase == "before") {
          mutate()
          rejects("Native Delta registration binding is unavailable") {
            association.validate(observed.command, observed.spark)
          }
        } else {
          var fired = false
          observed.targetPolicy.onRequest = request =>
            if (!fired && request.getAccessType == "update") {
              fired = true
              mutate()
            }
          rejects("Native Delta registration binding is unavailable") { capture(observed) }
          assertTrue(fired)
        }
      }
    }
  }

  @Test
  def sessionSubjectGroupsAndAnyConfigurationChangeRejectBeforeAccess(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val association = capture(observed)
      val spark = observed.spark
      observed.targetPolicy.requests.clear()
      observed.sourcePolicy.requests.clear()
      rejects("session changed") { association.validate(observed.command, spark.newSession()) }
      val priorUser = spark.sparkContext.getLocalProperty("kyuubi.session.user")
      try {
        spark.sparkContext.setLocalProperty("kyuubi.session.user", "association-different-user")
        rejects("Kyuubi session identity contradicts") {
          association.validate(observed.command, spark)
        }
      } finally spark.sparkContext.setLocalProperty("kyuubi.session.user", priorUser)
      val original = UserGroupInformation.getCurrentUser
      val user = original.getShortUserName
      val groups = original.getGroupNames
      val changedGroups =
        if (groups.isEmpty) Array("association-changed-group") else Array.empty[String]
      assertNotEquals(groups.toSet, changedGroups.toSet)
      try
        UserGroupInformation
          .createUserForTesting(user, changedGroups)
          .doAs(new PrivilegedExceptionAction[Unit] {
            override def run(): Unit = {
              assertEquals(
                changedGroups.toSet,
                UserGroupInformation.getCurrentUser.getGroupNames.toSet
              )
              rejects("subject changed") { association.validate(observed.command, spark) }
            }
          })
      finally UserGroupInformation.createUserForTesting(user, groups)
      val key = "association.arbitrary.configuration"
      val prior = spark.conf.getOption(key)
      try {
        spark.conf.set(key, "changed")
        rejects("configuration changed") { association.validate(observed.command, spark) }
      } finally restore(spark, key, prior)
      assertTrue(observed.targetPolicy.requests.isEmpty)
      assertTrue(observed.sourcePolicy.requests.isEmpty)
      association.validate(observed.command, spark)
    }

  @Test
  def transactionVersionRemovalHasNoResetExemption(): Unit =
    withObserved(
      "insertInto",
      "SELECT id, secret, region FROM b.db.t",
      Map(
        "spark.databricks.delta.write.txnAppId" -> "association-app",
        "spark.databricks.delta.write.txnVersion" -> "7"
      )
    ) { observed =>
      val association = capture(observed)
      val key = "spark.databricks.delta.write.txnVersion"
      observed.targetPolicy.requests.clear()
      observed.sourcePolicy.requests.clear()
      observed.spark.conf.unset(key)
      rejects("configuration changed") { association.validate(observed.command, observed.spark) }
      assertTrue(observed.spark.conf.getOption(key).isEmpty)
      assertTrue(observed.targetPolicy.requests.isEmpty)
      assertTrue(observed.sourcePolicy.requests.isEmpty)
    }

  @Test
  def finalCompositeChecksRejectCrossParticipantPublications(): Unit = {
    for (
      phase <- Seq("source-select", "second-target-update");
      change <- Seq("policy", "roles", "subject", "configuration")
      if phase != "source-select" || Set("policy", "roles")(change)
    ) {
      withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
        val association = capture(observed)
        var fired = false
        var updates = 0
        val spark = observed.spark
        val priorUser = spark.sparkContext.getLocalProperty("kyuubi.session.user")
        val key = "association.callback.configuration"
        val priorConf = spark.conf.getOption(key)
        def mutate(): Unit = {
          fired = true
          change match {
            case "policy" | "roles" =>
              publish(
                if (phase == "source-select") observed.targetPolicy else observed.sourcePolicy,
                change == "roles"
              )
            case "subject" =>
              spark.sparkContext.setLocalProperty(
                "kyuubi.session.user",
                "association-different-user"
              )
            case "configuration" => spark.conf.set(key, "changed")
          }
        }
        if (phase == "source-select") observed.sourcePolicy.onRequest = request => {
          if (!fired && request.getAccessType == "select") mutate()
        }
        else
          observed.targetPolicy.onRequest = request => {
            if (request.getAccessType == "update") {
              updates += 1
              if (!fired && updates == 2) mutate()
            }
          }
        try {
          val error = assertThrows(
            classOf[SecurityException],
            () => association.validate(observed.command, spark)
          )
          assertTrue(fired)
          assertContextCause(error, change)
        } finally {
          observed.targetPolicy.onRequest = _ => ()
          observed.sourcePolicy.onRequest = _ => ()
          spark.sparkContext.setLocalProperty("kyuubi.session.user", priorUser)
          restore(spark, key, priorConf)
        }
      }
    }
  }

  @Test
  def lastSourcePublicationReadCannotStaleCompositeContextSilently(): Unit = {
    Seq("policy", "roles", "subject", "configuration").foreach { change =>
      withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
        val association = capture(observed)
        val source = NativeSelectFixture.bindingFixture(observed.spark, "b")
        val spark = observed.spark
        val priorUser = spark.sparkContext.getLocalProperty("kyuubi.session.user")
        val key = "association.last-read.configuration"
        val priorConf = spark.conf.getOption(key)
        var updates = 0
        var fired = false
        observed.targetPolicy.onRequest = request =>
          if (request.getAccessType == "update") {
            updates += 1
            if (updates == 2)
              source.onRead = _ =>
                if (!fired) {
                  fired = true
                  change match {
                    case "policy" | "roles" => publish(observed.targetPolicy, change == "roles")
                    case "subject" =>
                      spark.sparkContext.setLocalProperty(
                        "kyuubi.session.user",
                        "association-different-user"
                      )
                    case "configuration" => spark.conf.set(key, "changed")
                  }
                }
          }
        try {
          val error = assertThrows(
            classOf[SecurityException],
            () => association.validate(observed.command, spark)
          )
          assertTrue(fired)
          assertEquals(2, updates)
          assertContextCause(error, change)
        } finally {
          source.onRead = _ => ()
          observed.targetPolicy.onRequest = _ => ()
          spark.sparkContext.setLocalProperty("kyuubi.session.user", priorUser)
          restore(spark, key, priorConf)
        }
      }
    }
  }

  @Test
  def closedOrExpiredParticipantRejects(): Unit = {
    for (catalog <- Seq("a", "b"); mode <- Seq("catalog", "metadata", "engine", "expiry")) {
      withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
        val association = capture(observed)
        val fixture = NativeSelectFixture.bindingFixture(observed.spark, catalog)
        val policy = if (catalog == "a") observed.targetPolicy else observed.sourcePolicy
        observed.targetPolicy.requests.clear()
        observed.sourcePolicy.requests.clear()
        mode match {
          case "catalog"  => fixture.wrapper.close()
          case "metadata" => fixture.metadata.close()
          case "engine"   => policy.engine.close()
          case "expiry"   => fixture.clock.set(TimeUnit.SECONDS.toNanos(301))
        }
        val error = assertThrows(
          classOf[RuntimeException],
          () => association.validate(observed.command, observed.spark)
        )
        assertTrue(
          error.isInstanceOf[SecurityException] || error.isInstanceOf[IllegalStateException]
        )
        assertTrue(observed.targetPolicy.requests.isEmpty)
        assertTrue(observed.sourcePolicy.requests.isEmpty)
      }
    }
  }

  @Test
  def emptyAndRepeatedSourcesRetainTheirOriginalDecisionInventory(): Unit = {
    Seq(
      "SELECT id, secret, region FROM b.db.t WHERE id < 0" -> 1,
      "SELECT l.id, l.secret, r.region FROM b.db.t l JOIN b.db.t r ON l.id = r.id" -> 2
    ).foreach { case (sql, expected) =>
      withObserved("insertInto", sql) { observed =>
        val original = DeltaRangerDecisionReceipt.consumeOwned(
          observed.sourceRoot,
          observed.spark,
          optimized = false
        )
        assertEquals(expected, original.obligations.size)
        val association = capture(observed)
        observed.sourcePolicy.denied.add("b:secret")
        val error = assertThrows(
          classOf[SecurityException],
          () => association.validate(observed.command, observed.spark)
        )
        assertTrue(error.getMessage.contains("denied"), error.getMessage)
        assertTrue(observed.targetPolicy.requests.asScala.exists(_.getAccessType == "update"))
        observed.sourcePolicy.denied.clear()
        assertSame(
          original,
          DeltaRangerDecisionReceipt.consumeOwned(
            observed.sourceRoot,
            observed.spark,
            optimized = false
          )
        )
        assertEquals(expected, original.obligations.size)
      }
    }
  }

  @Test
  def sourcePolicyAndMutableLiteralChangesRejectUnchangedCommandIdentity(): Unit = {
    Seq("mask", "filter").foreach { change =>
      withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
        val association = capture(observed)
        if (change == "mask") observed.sourcePolicy.mask = "upper({col})"
        else observed.sourcePolicy.filter = "id > 1"
        rejects("action decision changed") {
          association.validate(observed.command, observed.spark)
        }
      }
    }
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t WHERE region = 'keep'") {
      observed =>
        val association = capture(observed)
        val literal = observed.command.query
          .collect { case node =>
            node.expressions.flatMap(_.collect {
              case value: Literal if value.dataType == StringType => value
            })
          }
          .flatten
          .find(_.value.toString == "keep")
          .get
        val bytes = literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]]
        bytes(0) = 'z'.toByte
        assertSame(observed.sourceRoot, observed.command.query)
        rejects("logical binding changed") {
          association.validate(observed.command, observed.spark)
        }
    }
  }

  @Test
  def missingRootReceiptCannotBeReplacedByChildTags(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val tag =
        TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")
      val source = observed.sourceRoot.clone()
      source.unsetTagValue(tag)
      val descendants = observed.sourceRoot.children.flatMap(_.collect { case node =>
        node.getTagValue(tag)
      })
      assertEquals(
        descendants,
        source.children.flatMap(_.collect { case node => node.getTagValue(tag) })
      )
      observed.targetPolicy.requests.clear()
      rejects("missing receipt") {
        DeltaRangerAppendCommandBinding.capture(
          observed.spark,
          observed.command.copy(query = source),
          observed.catalog
        )
      }
      assertTrue(observed.targetPolicy.requests.isEmpty)
      observed.trace.record(
        "missing-root-rejected",
        "input=natural-descendants phase=source-root-receipt updateRequests=0"
      )
      // A borrowed child reference is a separate negative input, never new authority for the root.
      source.children.head.setTagValue(tag, observed.sourceRoot.getTagValue(tag).get)
      rejects("missing receipt") {
        DeltaRangerAppendCommandBinding.capture(
          observed.spark,
          observed.command.copy(query = source),
          observed.catalog
        )
      }
      assertTrue(observed.targetPolicy.requests.isEmpty)
      observed.trace.record(
        "missing-root-rejected",
        "input=borrowed-child-reference phase=source-root-receipt updateRequests=0"
      )
      val project = observed.sourceRoot.asInstanceOf[Project]
      val changed = project.copy(projectList = project.projectList.reverse)
      changed.setTagValue(tag, observed.sourceRoot.getTagValue(tag).get)
      rejects("logical binding changed") {
        DeltaRangerAppendCommandBinding.capture(
          observed.spark,
          observed.command.copy(query = changed),
          observed.catalog
        )
      }
      assertTrue(observed.targetPolicy.requests.isEmpty)
    }

  @Test
  def actualSelfSourcePreservesBothSelectAndUpdate(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM a.db.t") { observed =>
      observed.targetPolicy.requests.clear()
      observed.targetPolicy.events.clear()
      val association = capture(observed)
      association.validate(observed.command, observed.spark)
      assertTrue(observed.targetPolicy.requests.asScala.exists(_.getAccessType == "select"))
      assertTrue(observed.targetPolicy.requests.asScala.exists(_.getAccessType == "update"))
      assertTrue(observed.targetPolicy.events.asScala.contains("mask"))
      assertTrue(observed.targetPolicy.events.asScala.contains("filter"))
    }

  @Test
  def successfulAssociationLeavesInstalledAppendAndRawRoutesClosed(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val spark = observed.spark
      def control(): Vector[String] = {
        observed.targetPolicy.requests.clear()
        val rows = spark.sql("SELECT id, secret, region FROM a.db.t").collect().map(_.toSeq).toSet
        assertEquals(Set(Seq(1L, "a-one-masked", "keep"), Seq(3L, null, "keep")), rows)
        val requests = observed.targetPolicy.requests.asScala
          .map(r => s"${r.getAccessType}:${r.getResource.getAsMap}:${r.getUser}:${r.getUserGroups}")
          .toVector
          .sorted
        assertTrue(requests.nonEmpty)
        assertTrue(
          observed.targetPolicy.requests.asScala.forall(r =>
            r.getAccessType == "select" && r.getResource.getValue("catalog") == "a" && r.getResource
              .getValue("database") == "db" && r.getResource.getValue("table") == "t"
          )
        )
        observed.trace.record("healthy-select-control", s"rows=$rows requests=$requests")
        requests
      }
      val beforeRequests = control()
      val association = capture(observed)
      association.validate(observed.command, spark)
      Seq(
        "insertInto",
        "saveAsTable",
        "values",
        "insert-select",
        "writeTo",
        "raw-append",
        "raw-select"
      ).foreach { route =>
        val offset = observed.trace.observedAppends.size
        val error = assertThrows(
          classOf[Exception],
          () => {
            route match {
              case "insertInto" =>
                spark
                  .sql("SELECT id, secret, region FROM b.db.t")
                  .write
                  .mode("append")
                  .insertInto("a.db.t")
              case "saveAsTable" =>
                spark
                  .sql("SELECT id, secret, region FROM b.db.t")
                  .write
                  .mode("append")
                  .saveAsTable("a.db.t")
              case "values" => spark.sql("INSERT INTO a.db.t VALUES (9L, 'new', 'keep')").collect()
              case "insert-select" =>
                spark.sql("INSERT INTO a.db.t SELECT id, secret, region FROM b.db.t").collect()
              case "writeTo" =>
                spark.sql("SELECT id, secret, region FROM b.db.t").writeTo("a.db.t").append()
              case "raw-append" =>
                spark
                  .sql(
                    s"INSERT INTO delta.`${NativeSelectFixture.locations("a")}` VALUES (9L, 'new', 'keep')"
                  )
                  .collect()
              case "raw-select" =>
                spark.sql(s"SELECT * FROM delta.`${NativeSelectFixture.locations("a")}`").collect()
            }
          }
        )
        val reason = route match {
          case "insertInto" | "saveAsTable" => "Delta append inventory rejected: resolution rules"
          case "raw-select"                 => "Raw Delta paths are not admitted"
          case _                            => "Delta append inventory rejected: resolution rules"
        }
        assertTrue(
          causes(error).exists(e => Option(e.getMessage).exists(_.contains(reason))),
          error.toString
        )
        if (Set("values", "insert-select", "writeTo")(route)) {
          val commands = observed.trace.observedAppends.drop(offset)
          assertTrue(commands.nonEmpty)
          commands.zipWithIndex.foreach { case (command, index) =>
            val relation = command.table.asInstanceOf[DataSourceV2Relation]
            observed.trace.record(
              "frontend-association-input",
              s"route=$route observation=$index command=${observed.trace.id(command)} resolved=${command.resolved} streaming=${command.isStreaming} query=${observed.trace.id(command.query)} queryResolved=${command.query.resolved} queryStreaming=${command.query.isStreaming} rootReceipt=${DeltaRangerDecisionReceipt.present(command.query)} target=${observed.trace.id(relation.table)} catalog=${relation.catalog.map(observed.trace.id)} identifier=${relation.identifier} relationOptions=${relation.options.asCaseSensitiveMap()} writeOptions=${command.writeOptions} write=${command.write.map(observed.trace.id)} analyzedQuery=${command.analyzedQuery.map(observed.trace.id)}"
            )
            val rejected = assertThrows(
              classOf[SecurityException],
              () => DeltaRangerAppendCommandBinding.capture(spark, command, observed.catalog)
            )
            observed.trace.record(
              "frontend-association-rejected",
              s"route=$route observation=$index cause=${rejected.getMessage}"
            )
            assertFalse(DeltaRangerDecisionReceipt.present(command.query))
            assertEquals("Delta decision receipt rejected: missing receipt", rejected.getMessage)
          }
          observed.trace.record(
            "frontend-association-categories",
            s"route=$route observed=${commands.size} finiteShapeExcluded=0 missingRootReceipt=${commands.size}"
          )
        }
      }
      assertEquals(beforeRequests, control(), "Healthy SELECT request resources changed")
    }

  @Test
  def associationCannotEnterAnActiveNativeScopeOrTransaction(): Unit =
    withObserved("insertInto", "SELECT id, secret, region FROM b.db.t") { observed =>
      val association = capture(observed)
      val owner =
        DeltaRangerNativeScans.consume(observed.sourceRoot, observed.spark, optimized = false)
      owner.run { _ =>
        val active = DeltaRangerNativeOperation.current()
        assertNotNull(active)
        rejects("active native scope") { capture(observed) }
        rejects("active native scope") { association.validate(observed.command, observed.spark) }
        assertSame(active, DeltaRangerNativeOperation.current())
      }
      val table =
        observed.command.table.asInstanceOf[DataSourceV2Relation].table.asInstanceOf[DeltaTableV2]
      table.deltaLog.withNewTransaction(table.catalogTable) { transaction =>
        rejects("active native transaction") { capture(observed) }
        rejects("active native transaction") {
          association.validate(observed.command, observed.spark)
        }
        assertSame(transaction, OptimisticTransaction.getActive.get)
      }
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

  private def capture(observed: Observed): DeltaRangerAppendCommandBinding =
    DeltaRangerAppendCommandBinding.capture(observed.spark, observed.command, observed.catalog)

  private def rejects(reason: String)(body: => Unit): Unit = {
    val error = assertThrows(classOf[SecurityException], () => body)
    assertTrue(
      causes(error).exists(e => Option(e.getMessage).exists(_.contains(reason))),
      error.toString
    )
  }

  private def assertContextCause(error: Throwable, change: String): Unit = {
    val expected = change match {
      case "policy" | "roles" => "publication changed"
      case "subject"          => "identity contradicts"
      case "configuration"    => "configuration changed"
    }
    assertTrue(
      causes(error).exists(e => Option(e.getMessage).exists(_.toLowerCase.contains(expected))),
      error.toString
    )
  }

  private def restore(spark: SparkSession, key: String, value: Option[String]): Unit = value match {
    case Some(setting) => spark.conf.set(key, setting)
    case None          => spark.conf.unset(key)
  }

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

  private case class Observed(
      spark: SparkSession,
      targetPolicy: TestDeltaRangerNativeSelect.Fixture,
      sourcePolicy: TestDeltaRangerNativeSelect.Fixture,
      command: AppendData,
      sourceRoot: LogicalPlan,
      catalog: DeltaRangerCatalogSpark35,
      trace: Trace
  )

  private def withObserved(
      frontend: String,
      sourceSql: String,
      startupOptions: Map[String, String] = Map.empty
  )(body: Observed => Unit): Unit = {
    val trace = fresh("association-" + frontend)
    val targetPolicy = new TestDeltaRangerNativeSelect.Fixture
    val sourcePolicy = new TestDeltaRangerNativeSelect.Fixture
    val previousPolicy = NativeSelectFixture.policy
    val previousPolicies = NativeSelectFixture.catalogPolicies
    sourcePolicy.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> targetPolicy, "b" -> sourcePolicy)
    val install = NativeSelectFixture.extension(targetPolicy)
    val extension: SparkSessionExtensions => Unit = extensions => {
      observer(trace)(extensions)
      install(extensions)
    }
    try
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          trace.record(
            "jvm-runtime",
            s"javaVersion=${System.getProperty("java.version")} javaHome=${System.getProperty("java.home")} spark=${spark.version} scala=${scala.util.Properties.versionNumberString}"
          )
          Seq(
            classOf[DeltaRangerAppendCommandBinding],
            classOf[TestDeltaRangerAppendCommandBinding],
            classOf[DeltaRangerDecisionReceipt]
          ).foreach { clazz =>
            trace.record(
              "runtime-origin",
              s"${clazz.getName} ${clazz.getProtectionDomain.getCodeSource.getLocation}"
            )
          }
          val root = Paths.get(NativeSelectFixture.locations("a")).getParent
          val before = manifest(root, trace.directory.resolve("before"))
          try {
            val data = spark.sql(sourceSql)
            val sourceRoot = data.queryExecution.analyzed
            val originalReceipt =
              DeltaRangerDecisionReceipt.consumeOwned(sourceRoot, spark, optimized = false)
            assertTrue(originalReceipt.obligations.nonEmpty)
            val failure = assertThrows(
              classOf[Exception],
              () => {
                frontend match {
                  case "insertInto"  => data.write.mode("append").insertInto("a.db.t")
                  case "saveAsTable" => data.write.mode("append").saveAsTable("a.db.t")
                  case _ => throw new AssertionError("Unknown association fixture frontend")
                }
              }
            )
            assertTrue(
              causes(failure).exists(
                _.getMessage == "Delta append inventory rejected: resolution rules"
              )
            )
            // This preceding foreign observer is deliberately unsupported. It records the real early
            // command, then the inventory gate rejects before native writer construction.
            val observed = trace.observedAppends.filter(a =>
              a.resolved && (a.query eq sourceRoot) && a.write.isEmpty && a.analyzedQuery.isEmpty
            )
            assertTrue(observed.nonEmpty, "No actual receipted command/source association observed")
            val command = observed.last
            val relation = command.table.asInstanceOf[DataSourceV2Relation]
            trace.record(
              "association-input",
              s"command=${trace.id(command)} query=${trace.id(command.query)} table=${trace.id(relation.table)} catalog=${relation.catalog.map(trace.id)} identifier=${relation.identifier} relationOptions=${relation.options.asCaseSensitiveMap()} write=${command.write.map(trace.id)} analyzedQuery=${command.analyzedQuery.map(trace.id)} byName=${command.isByName} commandOptions=${command.writeOptions}"
            )
            assertTrue(command.write.isEmpty)
            assertTrue(command.analyzedQuery.isEmpty)
            assertTrue(command.writeOptions.isEmpty)
            assertTrue(relation.catalog.nonEmpty && relation.identifier.nonEmpty)
            assertTrue(relation.options.isEmpty)
            val catalog = NativeSelectFixture.wrapper(spark, "a")
            assertSame(spark.sessionState.catalogManager.catalog("a"), relation.catalog.get)
            val target = catalog
              .registrationBindings()
              .resolveAppendTarget(relation.table.asInstanceOf[DeltaTableV2])
            target.requireRetained()
            originalReceipt.consume(sourceRoot, spark, optimized = false)
            trace.record(
              "association-retention",
              s"target=${trace.id(target)} sourceReceipt=${trace.id(originalReceipt)} retained=true"
            )
            body(Observed(spark, targetPolicy, sourcePolicy, command, sourceRoot, catalog, trace))
          } finally {
            trace.requests(targetPolicy, "target-final")
            trace.requests(sourcePolicy, "source-final")
            assertEquals(
              before,
              manifest(root, trace.directory.resolve("after")),
              "Owned fixture storage changed"
            )
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
          }
        },
        Some(extension),
        startupOptions = startupOptions
      )
    finally {
      NativeSelectFixture.policy = previousPolicy
      NativeSelectFixture.catalogPolicies = previousPolicies
      targetPolicy.onRequest = _ => ()
      sourcePolicy.onRequest = _ => ()
      targetPolicy.engine.close()
      sourcePolicy.engine.close()
      trace.close()
    }
  }

  private def causes(error: Throwable): Vector[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
}
