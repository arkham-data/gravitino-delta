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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.security.PrivilegedExceptionAction
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.plans.logical.{Join, Project}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerReadComposition {
  @Test
  def schemaAndExplicitProjectionHaveDifferentDemands(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      policy.denied.add("a:secret")
      policy.denied.add("a:region")
      val table = spark.table("a.db.t")
      assertTrue(table.schema.fieldNames.contains("secret"))
      assertEquals(2, table.select("id").collect().length)
      assertThrows(classOf[SecurityException], () => table.collect())
      assertThrows(classOf[SecurityException], () => table.select("*").collect())
      assertThrows(classOf[SecurityException], () => table.select("*").select("id").collect())
    }

  @Test
  def explicitPredicateCannotBePruned(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      policy.denied.add("a:secret")
      val frame = spark.table("a.db.t").filter("secret = 'x'").select("id").filter("false")
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def newCompositionUsesOriginalIntent(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      policy.mask = "concat({col}, '!')"
      val frame = spark.table("a.db.t").select("secret")
      val before = frame.collect().map(_.getString(0)).toSet
      assertEquals(Set("a-one!", null), before)
      policy.mask = "concat({col}, '?')"
      val child = frame.select("secret")
      assertEquals(Set("a-one?", null), child.collect().map(_.getString(0)).toSet)
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def aliasedOccurrencesKeepOriginalDemands(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val base = spark.table("a.db.t")
      val left = base.select("id", "secret").alias("left")
      val right = base.select("id", "region").alias("right")
      val joined = left.join(right, left("id") === right("id")).select(left("secret"))
      assertEquals(2, DeltaRangerReadParityFixture.demands(joined).size)
      assertEquals(
        Set(("a.db.t", Set("id", "secret")), ("a.db.t", Set("id", "region"))),
        DeltaRangerReadParityFixture.demands(joined).toSet
      )
      assertEquals(Set("a-one-masked", null), joined.collect().map(_.getString(0)).toSet)
      val repeated = base.alias("same").alias("same").select("id")
      assertEquals(Vector(("a.db.t", Set("id"))), DeltaRangerReadParityFixture.demands(repeated))
      val renamed = base.toDF("number", "payload", "area").select("number")
      assertEquals(
        Vector(("a.db.t", Set("id", "secret", "region"))),
        DeltaRangerReadParityFixture.demands(renamed)
      )
      val other = spark.table("b.db.t").alias("other")
      val cross = base
        .alias("base")
        .join(other, base("id") === other("id"))
        .select(base("secret"), other("region"))
      assertEquals(
        Set(("a.db.t", Set("id", "secret")), ("b.db.t", Set("id", "region"))),
        DeltaRangerReadParityFixture.demands(cross).toSet
      )
    }

  @Test
  def usingInnerJoinRequiresOnlyAuthoredColumns(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val joined =
        spark.table("a.db.t").join(spark.table("b.db.t"), Seq("id"), "inner").select("id")
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "using-inner-join.txt"),
          joined.queryExecution.analyzed.treeString.getBytes(StandardCharsets.UTF_8)
        )
      }
      policy.denied.add("a:secret")
      policy.denied.add("a:region")
      policy.denied.add("b:secret")
      policy.denied.add("b:region")
      assertEquals(2, joined.collect().length)
      assertEquals(
        Set(("a.db.t", Set("id")), ("b.db.t", Set("id"))),
        DeltaRangerReadParityFixture.demands(joined).toSet
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          spark
            .table("a.db.t")
            .select("*")
            .join(spark.table("b.db.t"), Seq("id"), "inner")
            .select("id")
            .collect()
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          spark
            .table("a.db.t")
            .join(spark.table("b.db.t"), Seq("id"), "inner")
            .select("*")
            .select("id")
            .collect()
      )
    }

  @Test
  def generatedProjectionMetadataCannotEraseAuthoredDemand(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val left = spark.table("a.db.t")
      val right = spark.table("b.db.t")
      val generated = left
        .join(right, Seq("id"), "inner")
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
      val authored = left
        .join(right, left("id") === right("id"))
        .select(left("id"), left("secret"), left("region"), right("secret"), right("region"))
      val original = authored.queryExecution.analyzed
      original.setTagValue(
        Project.hiddenOutputTag,
        generated.getTagValue(Project.hiddenOutputTag).get
      )
      policy.denied.add("a:secret")
      assertThrows(classOf[SecurityException], () => authored.select("id").collect())
      val stripped = generated.clone()
      stripped.unsetTagValue(Project.hiddenOutputTag)
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(stripped, spark)
      )
    }

  @Test
  def conditionlessInnerJoinCannotAuthorizeExecution(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val frame = spark.table("a.db.t").join(spark.table("b.db.t"))
      assertTrue(frame.schema.fieldNames.contains("secret"))
      assertTrue(DeltaRangerOrdinaryRead.pending(frame.queryExecution.analyzed))
      assertThrows(classOf[SecurityException], () => frame.collect())
      assertFalse(DeltaRangerDecisionReceipt.present(frame.queryExecution.analyzed))
    }

  @Test
  def nativeAutoResolvedSelfJoinKeepsIdDemand(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val base = spark.table("a.db.t")
      val full = base.join(base, base("id") === base("id"))
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        val analyzed = full.queryExecution.analyzed
        val normalized = full.queryExecution.normalized
        Files.write(
          Paths.get(directory, "auto-self-join-full.txt"),
          (analyzed.treeString + "\n" + analyzed
            .map(node =>
              s"${node.nodeName}: pending=${DeltaRangerOrdinaryRead.pending(node)} receipt=${DeltaRangerDecisionReceipt.present(node)}"
            )
            .mkString("\n") +
            "\nnormalized:\n" + normalized.treeString +
            s"\nroot pending=${DeltaRangerOrdinaryRead.pending(normalized)} receipt=${DeltaRangerDecisionReceipt.present(normalized)}")
            .getBytes(StandardCharsets.UTF_8)
        )
      }
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        val optimized = full.queryExecution.optimizedPlan
        val planned = full.queryExecution.sparkPlan
        Files.write(
          Paths.get(directory, "auto-self-join-prepared.txt"),
          (optimized.treeString + "\n" + optimized
            .map(node =>
              s"${node.nodeName}: prepared=${DeltaRangerNativeScans.evidence(node).nonEmpty}"
            )
            .mkString("\n") +
            "\nphysical:\n" + planned.treeString + "\n" + planned
              .map(node =>
                s"${node.nodeName}: link=${node.logicalLink.map(link => link.nodeName + ":" + DeltaRangerNativeScans.evidence(link).nonEmpty)}"
              )
              .mkString("\n"))
            .getBytes(StandardCharsets.UTF_8)
        )
      }
      assertEquals(2, full.collect().length)
      policy.denied.add("a:id")
      assertThrows(classOf[SecurityException], () => full.collect())
      policy.denied.clear()
      val frame = base.select("id")
      val joined = frame.join(frame, frame("id") === frame("id"))
      sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
        Files.write(
          Paths.get(directory, "auto-self-join.txt"),
          joined.queryExecution.analyzed.treeString.getBytes(StandardCharsets.UTF_8)
        )
      }
      policy.denied.add("a:secret")
      policy.denied.add("a:region")
      assertEquals(2, joined.collect().length)
    }

  @Test
  def joinReceiptLocatorCannotAuthorizeChangedOrDetachedPlans(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val base = spark.table("a.db.t")
      val issued = base.join(base, base("id") === base("id")).queryExecution.normalized
      val locator = TreeNodeTag[AnyRef]("gravitino.delta.ranger.ordinary-join-receipt")
      val candidate = DeltaRangerDecisionReceipt.nativePrefix(issued.clone(), spark)
      val scans = candidate.collect { case relation: LogicalRelation => relation }
      assertEquals(2, scans.size)
      assertTrue(scans.forall(_.getTagValue(locator).nonEmpty))
      assertFalse(DeltaRangerDecisionReceipt.present(candidate))
      val receipt = DeltaRangerDecisionReceipt.consumeOwned(candidate, spark, optimized = true)
      assertNotNull(receipt)
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(candidate, spark, optimized = false)
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerDecisionReceipt.consumeOwned(candidate.children.head, spark, optimized = true)
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            candidate,
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
      val missing = candidate.clone()
      missing.foreach(_.unsetTagValue(locator))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(missing, spark, optimized = true)
      )
      val changed = candidate.asInstanceOf[Join].copy(condition = Some(Literal.TrueLiteral))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(changed, spark, optimized = true)
      )
      val other = base.join(base, base("id") === base("id")).queryExecution.normalized
      val mixed = candidate.clone()
      mixed
        .collect { case relation: LogicalRelation => relation }
        .head
        .setTagValue(
          locator,
          other.collect { case relation: LogicalRelation => relation }.head.getTagValue(locator).get
        )
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(mixed, spark, optimized = true)
      )
      val bytes = new ByteArrayOutputStream
      val output = new ObjectOutputStream(bytes)
      try output.writeObject(scans.head.getTagValue(locator).get)
      finally output.close()
      val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val inert =
        try input.readObject()
        finally input.close()
      val serialized = candidate.clone()
      serialized
        .collect { case relation: LogicalRelation => relation }
        .foreach(_.setTagValue(locator, inert))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerDecisionReceipt.consumeOwned(serialized, spark, optimized = true)
      )
      val zone = spark.conf.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerDecisionReceipt.consumeOwned(candidate, spark, optimized = true)
        )
      finally spark.conf.set("spark.sql.session.timeZone", zone)
      // Same-publication grant changes are reevaluated at the action boundary, as for root receipts.
      policy.denied.add("a:id")
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerDecisionReceipt
            .consumeOwned(candidate, spark, optimized = true)
            .validateAction(spark)
      )
    }

  @Test
  def joinPreparationLocatorRequiresCompleteExecutableRoot(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val base = spark.table("a.db.t")
      val optimized = base.join(base, base("id") === base("id")).queryExecution.optimizedPlan
      val locator = TreeNodeTag[AnyRef]("gravitino.delta.ranger.ordinary-join-preparation")
      val scans = optimized.collect { case relation: LogicalRelation => relation }
      assertTrue(scans.forall(_.getTagValue(locator).nonEmpty))
      assertTrue(DeltaRangerNativeScans.evidence(optimized).nonEmpty)
      assertTrue(scans.forall(DeltaRangerNativeScans.evidence(_).isEmpty))
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerNativeScans.prepared(optimized.children.head)
      )
      val missing = optimized.clone()
      missing.foreach(_.unsetTagValue(locator))
      assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(missing))
      val partial = optimized.clone()
      partial.collect { case relation: LogicalRelation => relation }.head.unsetTagValue(locator)
      assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(partial))
      val changed = optimized.asInstanceOf[Join].copy(condition = Some(Literal.TrueLiteral))
      assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(changed))
      val other = base.join(base, base("id") === base("id")).queryExecution.optimizedPlan
      val mixed = optimized.clone()
      mixed
        .collect { case relation: LogicalRelation => relation }
        .head
        .setTagValue(
          locator,
          other.collect { case relation: LogicalRelation => relation }.head.getTagValue(locator).get
        )
      assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(mixed))
      val bytes = new ByteArrayOutputStream
      val output = new ObjectOutputStream(bytes)
      try output.writeObject(scans.head.getTagValue(locator).get)
      finally output.close()
      val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val inert =
        try input.readObject()
        finally input.close()
      val serialized = optimized.clone()
      serialized
        .collect { case relation: LogicalRelation => relation }
        .foreach(_.setTagValue(locator, inert))
      assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(serialized))
      val staged = optimized.clone()
      staged.foreach(
        _.unsetTagValue(TreeNodeTag[AnyRef]("gravitino.delta.ranger.ordinary-join-receipt"))
      )
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            staged,
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
      val zone = spark.conf.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try assertThrows(classOf[SecurityException], () => DeltaRangerNativeScans.prepared(optimized))
      finally spark.conf.set("spark.sql.session.timeZone", zone)
    }

  @Test
  def pendingOriginCannotAuthorizePayload(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val frame = spark.table("a.db.t").select("secret")
      val pending = frame.queryExecution.analyzed
      assertTrue(DeltaRangerOrdinaryRead.pending(pending))
      assertFalse(DeltaRangerDecisionReceipt.present(pending))
      assertFalse(pending.treeString.contains("-masked"))
      assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.prepare(pending, spark))
      val normalized = frame.queryExecution.normalized
      assertTrue(DeltaRangerDecisionReceipt.present(normalized))
      assertFalse(DeltaRangerOrdinaryRead.pending(normalized))
      assertFalse(pending.treeString.contains("-masked"))
      assertEquals(1, "-masked".r.findAllIn(normalized.treeString).length)
      assertEquals(Set("a-one-masked", null), frame.collect().map(_.getString(0)).toSet)
      policy.denied.add("a:secret")
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def originDriftRejectsBeforeNormalization(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val frame = spark.table("a.db.t").select("id")
      val pending = frame.queryExecution.analyzed
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(pending, spark.newSession())
      )
      val zone = spark.conf.get("spark.sql.session.timeZone")
      spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try assertThrows(classOf[SecurityException], () => frame.collect())
      finally spark.conf.set("spark.sql.session.timeZone", zone)
      UserGroupInformation
        .createUserForTesting("other-origin", Array("other-group"))
        .doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerOrdinaryRead.normalize(pending, spark)
            )
          }
        })
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val token = registration.properties.get("delta.registration.receipt.token")
      registration.properties.put("delta.registration.receipt.token", "changed")
      try
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerOrdinaryRead.normalize(pending, spark)
        )
      finally registration.properties.put("delta.registration.receipt.token", token)
    }

  @Test
  def copiedSerializedAndCanonicalizedOriginsRemainInert(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val pending = spark.table("a.db.t").select("id", "secret").queryExecution.analyzed
      val tag = TreeNodeTag[AnyRef]("gravitino.delta.ranger.ordinary-origin")
      val forged = Project(pending.output.take(1), pending.children.head)
      forged.copyTagsFrom(pending)
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(forged, spark)
      )
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(pending.canonicalized, spark)
      )
      val bytes = new ByteArrayOutputStream
      val output = new ObjectOutputStream(bytes)
      try output.writeObject(pending.getTagValue(tag).get)
      finally output.close()
      val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val restored =
        try input.readObject()
        finally input.close()
      val copied = pending.clone()
      copied.setTagValue(tag, restored)
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerOrdinaryRead.normalize(copied, spark)
      )
      assertThrows(
        classOf[SecurityException],
        () => spark.read.format("delta").load(NativeSelectFixture.locations("a")).collect()
      )
    }

  @Test
  def cacheAttemptsLeaveNoReusableAuthority(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val frame = spark.table("a.db.t").select("id")
      val manager = spark.sharedState.cacheManager
      assertThrows(classOf[SecurityException], () => frame.cache())
      assertTrue(manager.lookupCachedData(frame.queryExecution.analyzed).isEmpty)
      assertThrows(classOf[SecurityException], () => frame.persist())
      assertTrue(manager.lookupCachedData(frame.queryExecution.analyzed).isEmpty)
      assertThrows(classOf[SecurityException], () => frame.localCheckpoint(true))
      assertThrows(classOf[SecurityException], () => frame.rdd.collect())
      assertEquals(2, frame.collect().length)
    }

  @Test
  def explainAuthorizesButActionRechecks(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, policy) =>
      val frame = spark.table("a.db.t").select("id")
      frame.explain()
      policy.denied.add("a:id")
      assertThrows(classOf[SecurityException], () => frame.collect())
    }

  @Test
  def originMetadataCallbacksCannotChangeSettings(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val frame = spark.table("a.db.t").select("id")
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      val zone = spark.conf.get("spark.sql.session.timeZone")
      registration.onRead = _ => spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      try assertThrows(classOf[SecurityException], () => frame.queryExecution.normalized)
      finally {
        registration.onRead = _ => ()
        spark.conf.set("spark.sql.session.timeZone", zone)
      }
    }

  @Test
  def cacheWithoutSessionCloneStillRejects(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "false")
      spark.conf.set("spark.sql.adaptive.applyFinalStageShuffleOptimizations", "false")
      spark.conf.set("spark.sql.optimizer.canChangeCachedPlanOutputPartitioning", "false")
      val frame = spark.table("a.db.t").select("id")
      DeltaRangerReadParityFixture.withUnrelatedCache(spark) {
        val failure = assertThrows(classOf[SecurityException], () => frame.cache())
        assertFalse(failure.getMessage.contains("session changed"), failure.getMessage)
        sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
          Files.write(
            Paths.get(directory, "cache-matching-settings-denial.txt"),
            (failure.toString + "\n" + failure.getStackTrace.mkString("\n"))
              .getBytes(StandardCharsets.UTF_8)
          )
        }
        assertTrue(
          spark.sharedState.cacheManager.lookupCachedData(frame.queryExecution.normalized).isEmpty
        )
        val planned = spark.table("a.db.t").select("secret")
        planned.queryExecution.executedPlan
        assertThrows(classOf[SecurityException], () => planned.cache())
        assertTrue(
          spark.sharedState.cacheManager.lookupCachedData(planned.queryExecution.normalized).isEmpty
        )
        assertEquals(2, frame.collect().length)
        assertEquals(2, planned.collect().length)
      }
    }

  @Test
  def ordinaryRemappingCannotChangeColumnLineageOrUserMetadata(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val pending = spark
        .table("a.db.t")
        .select("secret")
        .queryExecution
        .analyzed
        .asInstanceOf[Project]
      val region = pending.child.output.find(_.name == "region").get
      val secret = pending.projectList.head.asInstanceOf[AttributeReference]
      val rewired = pending.copy(projectList = Seq(secret.withExprId(region.exprId)))
      rewired.copyTagsFrom(pending)
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            rewired,
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
      val metadata = new MetadataBuilder().putString("user-key", "changed").build()
      val changed = pending.copy(projectList = Seq(secret.withMetadata(metadata)))
      changed.copyTagsFrom(pending)
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            changed,
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
    }

  @Test
  def executableReceiptCannotBeRestagedAsOriginalIntent(): Unit =
    DeltaRangerReadParityFixture.withSpark { (spark, _) =>
      val normalized = spark.table("a.db.t").select("secret").queryExecution.normalized
      assertThrows(
        classOf[SecurityException],
        () =>
          DeltaRangerOrdinaryRead.stage(
            normalized.clone(),
            spark,
            scan => NativeSelectFixture.wrapper(spark, scan.catalog)
          )
      )
    }
}
