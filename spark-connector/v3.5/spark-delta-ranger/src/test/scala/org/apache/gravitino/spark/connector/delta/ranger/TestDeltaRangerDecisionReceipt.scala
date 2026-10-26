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

import java.io._
import java.security.PrivilegedExceptionAction
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

// Executable receipts are issued by normalization. These tests deliberately inspect that
// boundary; pending analysis and its independent payload-denial checks live in ReadComposition.
class TestDeltaRangerDecisionReceipt {
  private val tag =
    TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")

  @Test
  def originalInventoryAndSingleDecisionBatchSurviveSelfJoinAndClones(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val plan = spark
        .sql(
          "SELECT x.secret, y.secret FROM a.db.t x JOIN a.db.t y ON x.id = y.id WHERE y.region = 'keep'"
        )
        .queryExecution
        .normalized
      val receipt = plan.getTagValue(tag).get.receipt
      assertEquals(2, receipt.obligations.size)
      assertEquals(
        Set(Set("id", "secret"), Set("id", "secret", "region")),
        receipt.obligations.map(_.columns).toSet
      )
      assertEquals(2, policy.events.asScala.count(_ == "filter"))
      assertEquals(5, policy.events.asScala.count(_ == "mask"))
      val events = policy.events.asScala.toVector
      assertTrue(
        events.takeWhile(_.startsWith("access:")).size == events.count(_.startsWith("access:"))
      )
      assertEquals(1, policy.requests.asScala.map(_.getAccessTime.toInstant).distinct.size)
      assertTrue(receipt.obligations.forall(o => o.binding.engine() eq policy.engine))
      DeltaRangerReadRule.check(plan.clone(), spark)
      assertEquals(events, policy.events.asScala.toVector)
      assertFalse(classOf[Product].isAssignableFrom(receipt.getClass))
      assertFalse(classOf[Serializable].isAssignableFrom(receipt.getClass))
    }

  @Test
  def referencesRenderConstantlyAndDeserializeWithoutAuthority(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
      val reference = plan.getTagValue(tag).get
      assertEquals("Delta authorization reference", reference.toString)
      val bytes = new ByteArrayOutputStream
      val output = new ObjectOutputStream(bytes)
      try output.writeObject(reference)
      finally output.close()
      val rendered = new String(bytes.toByteArray, java.nio.charset.StandardCharsets.ISO_8859_1)
      assertFalse(rendered.contains("same-owner"))
      assertFalse(rendered.contains("private-wrapper-token"))
      val input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray))
      val restored =
        try input.readObject().asInstanceOf[DeltaRangerDecisionReceipt.Reference]
        finally input.close()
      val clone = plan.clone()
      clone.setTagValue(tag, restored)
      rejected("inert receipt", () => DeltaRangerReadRule.check(clone, spark))
      assertFalse(plan.toJSON.contains("private-wrapper-token"))
    }

  @Test
  def changedOutputsRemovedMasksAndForeignSamePathSourcesCannotBorrowReceipt(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark.sql("SELECT secret FROM a.db.t WHERE id = 1").queryExecution.normalized
      val top = plan.asInstanceOf[Project]
      val changedAlias = top.copy(projectList = Seq(Alias(top.projectList.head, "borrowed")()))
      changedAlias.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(changedAlias, spark))
      val removed = plan.transformUp { case filter: Filter => filter.child }
      removed.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(removed, spark))
      val maskProject = plan.collectFirst {
        case project: Project if project.projectList.exists(_.exists(_.isInstanceOf[Concat])) =>
          project
      }.get
      val unmaskedOutputs = maskProject.output
        .zip(maskProject.child.output)
        .map { case (masked, original) =>
          masked.exprId -> original
        }
        .toMap
      val withoutMask = plan
        .transformUp { case project: Project if project eq maskProject => project.child }
        .transformAllExpressions {
          case attribute: AttributeReference if unmaskedOutputs.contains(attribute.exprId) =>
            unmaskedOutputs(attribute.exprId)
        }
      assertTrue(withoutMask.resolved)
      withoutMask.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(withoutMask, spark))
      val relation = plan.collectFirst { case r: LogicalRelation => r }.get
      val copiedDescriptor = relation.copy(catalogTable = relation.catalogTable.map(_.copy()))
      val changed = replaceLeaf(plan, copiedDescriptor)
      changed.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(changed, spark))
      NativeSelectFixture.locations += "b" -> NativeSelectFixture.locations("a")
      val foreign = spark
        .sql("SELECT secret FROM b.db.t")
        .queryExecution
        .normalized
        .collectFirst { case r: LogicalRelation => r }
        .get
      val replaced = replaceLeaf(plan, foreign.copy(output = relation.output))
      replaced.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(replaced, spark))
      assertThrows(
        classOf[SecurityException],
        () => NativeSelectFixture.wrapper(spark, "b").registrationBindings().resolve(relation)
      )
    }

  @Test
  def staleMetadataSameNumberPolicyAndGroupsRejectOriginalReceipt(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val plan = spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      registration.properties.put("delta.registration.receipt.token", "replacement-token")
      rejected("registration binding", () => DeltaRangerReadRule.check(plan, spark))
      registration.properties.put("delta.registration.receipt.token", "private-wrapper-token")
      val user = UserGroupInformation.getCurrentUser.getShortUserName
      UserGroupInformation
        .createUserForTesting(user, Array("different-group"))
        .doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit =
            rejected("subject changed", () => DeltaRangerReadRule.check(plan, spark))
        })
      policy.start()
      rejected("publication changed", () => DeltaRangerReadRule.check(plan, spark))
    }

  @Test
  def lateMetadataLeaseAndOwnerCloseCannotEmitAuthorizedTree(): Unit = {
    Seq("publication", "lease", "close").foreach { mode =>
      NativeSelectFixture.withBoundSpark { (spark, policy) =>
        val registration = NativeSelectFixture.bindingFixture(spark, "a")
        policy.onRequest = _ =>
          mode match {
            case "publication" =>
              registration.properties.put("delta.registration.receipt.token", "changed")
            case "lease" => registration.clock.set(TimeUnit.SECONDS.toNanos(301))
            case "close" => registration.wrapper.close()
          }
        assertThrows(
          classOf[SecurityException],
          () => spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
        )
      }
    }
  }

  @Test
  def configurationChangeRejectsBeforeOptimizerConsumption(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
      spark.conf.set("spark.sql.optimizer.inSetConversionThreshold", "2")
      rejected("configuration changed", () => DeltaRangerReadRule.check(plan, spark))
    }

  @Test
  def unknownOptimizerRuleIsRejectedBeforeInvocation(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val calls = new AtomicInteger
    try
      NativeSelectFixture.withSpark(
        spark => {
          rejected("optimizer prefix", () => spark.sql("SELECT secret FROM a.db.t").collect())
          assertEquals(0, calls.get())
        },
        Some(extensions => {
          NativeSelectFixture.extension(policy)(extensions)
          extensions.injectOptimizerRule(_ =>
            new Rule[LogicalPlan] {
              override def apply(plan: LogicalPlan): LogicalPlan = { calls.incrementAndGet(); plan }
            }
          )
        })
      )
    finally policy.engine.close()
  }

  @Test
  def trustedReplayPreservesNativeRowsThroughPruningFoldingInsetsAndEmptyResults(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.optimizer.inSetConversionThreshold", "2")
      Seq(
        "SELECT secret FROM a.db.t WHERE id IN (1L, 2L, 3L)" -> Set(Row("a-one-masked"), Row(null)),
        "SELECT secret AS renamed FROM a.db.t WHERE id = 1L + 0L" -> Set(Row("a-one-masked")),
        "SELECT secret FROM a.db.t WHERE false AND id = 1L" -> Set.empty[Row],
        "SELECT x.secret FROM a.db.t x JOIN a.db.t y ON x.id = y.id WHERE y.id = 1L" -> Set(
          Row("a-one-masked")
        )
      ).foreach { case (sql, expected) =>
        val frame = spark.sql(sql)
        assertEquals(expected, frame.collect().toSet)
        if (expected.isEmpty) {
          val prepared = frame.queryExecution.optimizedPlan
          val proof = DeltaRangerNativeScans.prepared(prepared)
          assertEquals(1, proof.owner.obligations.size)
          assertTrue(proof.occurrences.isEmpty)
          proof.verify(prepared.clone(), spark)
          val beforeCollapse =
            new TestPrefix(spark).execute(frame.queryExecution.normalized.clone())
          proof.verify(beforeCollapse.clone(), spark)
          val changed = beforeCollapse match {
            case project: org.apache.spark.sql.catalyst.plans.logical.Project =>
              project.copy(projectList = project.projectList.map { expression =>
                expression
                  .withNewChildren(Seq(Literal.create(null, expression.dataType)))
                  .asInstanceOf[NamedExpression]
              })
            case _ =>
              throw new AssertionError(
                "Expected the actual masked projection over an empty local relation"
              )
          }
          assertEquals(beforeCollapse.output, changed.output)
          val rejected =
            assertThrows(classOf[SecurityException], () => proof.verify(changed, spark))
          assertTrue(rejected.getMessage.contains("executable logical binding changed"))
        }
      }
    }

  @Test
  def mutableLiteralCannotChangeRetainedReplayAuthority(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan =
        spark.sql("SELECT secret FROM a.db.t WHERE region = 'keep'").queryExecution.normalized
      val candidate = new TestPrefix(spark).execute(plan.clone())
      candidate.copyTagsFrom(plan)
      DeltaRangerDecisionReceipt.check(candidate, spark, optimized = true)
      val literal = candidate
        .collect { case node =>
          node.expressions.flatMap(_.collect { case l: Literal if l.dataType == StringType => l })
        }
        .flatten
        .find(_.value.toString == "keep")
        .get
      val bytes = literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]]
      bytes(0) = 'z'.toByte
      rejected(
        "logical binding changed",
        () => DeltaRangerDecisionReceipt.check(candidate, spark, optimized = true)
      )

    }

  @Test
  def unknownFoldableCastChildIsRejectedWithoutEvaluation(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan =
        spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized.asInstanceOf[Project]
      val calls = new AtomicInteger
      val changed =
        plan.copy(projectList = Seq(Alias(Cast(CountingFoldable(calls), DateType), "clock")()))
      changed.copyTagsFrom(plan)
      rejected("expression class", () => DeltaRangerReadRule.check(changed, spark))
      assertEquals(0, calls.get())
    }

  private case class CountingFoldable(calls: AtomicInteger)
      extends LeafExpression
      with org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback {
    override def nullable: Boolean = false
    override def dataType: DataType = StringType
    override def foldable: Boolean = true
    override def eval(input: org.apache.spark.sql.catalyst.InternalRow): Any = {
      calls.incrementAndGet()
      UTF8String.fromString("2000-01-01")
    }
  }

  private class TestPrefix(spark: SparkSession)
      extends org.apache.spark.sql.execution.SparkOptimizer(
        spark.sessionState.catalogManager,
        spark.sessionState.catalog,
        spark.experimental
      ) {
    override def extendedOperatorOptimizationRules: Seq[Rule[LogicalPlan]] =
      Seq(new org.apache.spark.sql.delta.optimizer.RangePartitionIdRewrite(spark))
    override def defaultBatches: Seq[Batch] =
      super.defaultBatches.takeWhile(_.name != "Pre CBO Rules")
  }

  @Test
  def twoCanonicalOwnersUseOneTimeAndRetainBothOriginalBindings(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val second = new TestDeltaRangerNativeSelect.Fixture
      second.customCatalog = "b"
      NativeSelectFixture.catalogPolicies = Map("b" -> second)
      try {
        val query = spark.sql("SELECT a.secret,b.secret FROM a.db.t a JOIN b.db.t b ON a.id=b.id")
        val receipt = query.queryExecution.normalized.getTagValue(tag).get.receipt
        val engines = receipt.obligations.map(o => o.scan.catalog -> o.binding.engine()).toMap
        assertTrue(engines("a") eq policy.engine)
        assertTrue(engines("b") eq second.engine)
        val requests = policy.requests.asScala ++ second.requests.asScala
        assertEquals(1, requests.map(_.getAccessTime.toInstant).distinct.size)
        assertEquals(
          Set(Row("a-one-masked", "b-one-masked"), Row(null, null)),
          query.collect().toSet
        )
      } finally second.engine.close()
    }

  @Test
  def emptyOptimizationStillRevalidatesRemovedOriginalRegistration(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark.sql("SELECT secret FROM a.db.t WHERE false").queryExecution.normalized
      val candidate = new TestPrefix(spark).execute(plan.clone())
      candidate.copyTagsFrom(plan)
      assertTrue(candidate.collect { case r: LogicalRelation => r }.isEmpty)
      DeltaRangerDecisionReceipt.check(candidate, spark, optimized = true)
      NativeSelectFixture
        .bindingFixture(spark, "a")
        .properties
        .put("delta.registration.receipt.token", "changed")
      rejected(
        "registration binding",
        () => DeltaRangerDecisionReceipt.check(candidate, spark, optimized = true)
      )
    }

  @Test
  def replayDoesNotSubmitJobsOrPrepareNativeScans(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan =
        spark.sql("SELECT secret FROM a.db.t WHERE id IN (1L,2L,3L)").queryExecution.normalized
      val candidate = new TestPrefix(spark).execute(plan.clone())
      candidate.copyTagsFrom(plan)
      val before = candidate.collect { case r: LogicalRelation => r.relation }
      spark.sparkContext.setJobGroup("receipt-replay-only", "receipt replay")
      try {
        DeltaRangerDecisionReceipt.check(candidate, spark, optimized = true)
        assertEquals(
          0,
          spark.sparkContext.statusTracker.getJobIdsForGroup("receipt-replay-only").length
        )
        assertTrue(candidate.collect { case r: LogicalRelation => r.relation }.zip(before).forall {
          case (a, b) => a eq b
        })
      } finally spark.sparkContext.clearJobGroup()
    }

  @Test
  def specialDatetimeConstantsRejectWhileStableDateAndNumericCastsCollect(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      Seq("'today'", "concat('to','day')", "'now UTC'").foreach { value =>
        rejected(
          "context-dependent datetime",
          () =>
            spark.sql(
              s"SELECT secret FROM a.db.t WHERE CAST($value AS TIMESTAMP) = TIMESTAMP '2000-01-01 00:00:00'"
            )
        )
      }
      assertEquals(
        Set(Row("a-one-masked")),
        spark
          .sql(
            "SELECT secret FROM a.db.t WHERE CAST(id AS STRING)='1' AND CAST('2000-01-01' AS DATE)=DATE '2000-01-01'"
          )
          .collect()
          .toSet
      )
    }

  @Test
  def changedSignedZeroCannotBorrowExpressionBinding(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark.sql("SELECT CAST(id AS DOUBLE) * 0.0D FROM a.db.t").queryExecution.normalized
      val changed = plan.transformAllExpressions {
        case literal: Literal if literal.dataType == DoubleType && literal.value == 0.0d =>
          Literal(-0.0d)
      }
      // Spark's transform helpers compare doubles by value and may preserve the old literal.
      val top = plan.asInstanceOf[Project]
      val alias = top.projectList.head.asInstanceOf[Alias]
      val multiplication = alias.child.asInstanceOf[Multiply]
      val replacement = top.copy(projectList =
        Seq(
          alias.copy(child = multiplication.copy(right = Literal(-0.0d)))(
            alias.exprId,
            alias.qualifier,
            alias.explicitMetadata,
            alias.nonInheritableMetadataKeys
          )
        )
      )
      replacement.copyTagsFrom(plan)
      rejected("logical binding changed", () => DeltaRangerReadRule.check(replacement, spark))
    }

  @Test
  def userCacheSubstitutionCannotBorrowFreshReceipt(): Unit = {
    var cachedPlan: LogicalPlan = null
    NativeSelectFixture.withSpark { spark =>
      val cached = spark.sql("SELECT secret FROM a.db.t").cache()
      cachedPlan = cached.queryExecution.withCachedData
      assertTrue(cachedPlan.isInstanceOf[org.apache.spark.sql.execution.columnar.InMemoryRelation])
    }
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val query = spark.sql("SELECT secret FROM a.db.t")
      val approved = query.queryExecution.normalized
      cachedPlan.setTagValue(tag, approved.getTagValue(tag).get)
      rejected(
        "cache substitution",
        () => DeltaRangerDecisionReceipt.check(cachedPlan, spark, optimized = true)
      )
      // Default cache planning clones the session; its exact executable receipt fails there.
      // ReadComposition separately pins identical settings to exercise cache-operation denial.
      rejected("session changed", () => query.cache())
    }
  }

  @Test
  def configurationMutationDuringPolicyEvaluationCannotEmitReceipt(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      policy.onRequest = _ => spark.conf.set("spark.sql.session.timeZone", "Pacific/Honolulu")
      rejected(
        "configuration changed",
        () => spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
      )
    }

  @Test
  def finalMetadataWorkPrecedesEnginePublicationAndSubjectChecks(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val registration = NativeSelectFixture.bindingFixture(spark, "a")
      // Arm the final metadata callback after payload authorization begins, since ordinary
      // staging now performs separate registration-only observations before that boundary.
      policy.onRequest = _ => registration.onRead = _ => policy.start()
      rejected(
        "publication changed",
        () => spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
      )
      registration.onRead = _ => ()
      policy.onRequest =
        _ => spark.sparkContext.setLocalProperty("kyuubi.session.user", "changed-user")
      try
        rejected(
          "identity contradicts",
          () => spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized
        )
      finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
    }

  @Test
  def rowTrackingIsExplicitlyUnqualifiedWithoutChangingNativeIdentity(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          rejected("row tracking", () => spark.sql("SELECT secret FROM a.db.t").collect())
        },
        Some(NativeSelectFixture.extension(policy)),
        Map("spark.test.delta.rowTracking" -> "true")
      )
    finally policy.engine.close()
  }

  @Test
  def starSchemaStatisticsAndExcludedConsumerAreRejectedDuringAnalysis(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.cbo.starSchemaDetection", "true")
      rejected(
        "star-schema",
        () =>
          spark.sql(
            "SELECT a.secret FROM a.db.t a JOIN b.db.t b ON a.id=b.id JOIN q.db.t q ON a.id=q.id"
          )
      )
      spark.conf.set("spark.sql.cbo.starSchemaDetection", "false")
      spark.conf.set("spark.sql.optimizer.excludedRules", classOf[DeltaRangerPreCboRule].getName)
      rejected("consumer", () => spark.sql("SELECT secret FROM a.db.t").queryExecution.normalized)
    }

  @Test
  def installedAsymmetricSelfJoinPreservesReceiptAndReturnsNativeRows(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val query = spark.sql(
        "SELECT x.secret AS first, y.secret AS second FROM a.db.t x JOIN a.db.t y ON x.id = y.id WHERE y.region = 'keep'"
      )
      val receipt = query.queryExecution.normalized.getTagValue(tag).get.receipt
      assertEquals(2, receipt.obligations.size)
      DeltaRangerReadRule.check(query.queryExecution.normalized.clone(), spark)
      assertEquals(Set(Row("a-one-masked", "a-one-masked"), Row(null, null)), query.collect().toSet)

    }

  @Test
  def metadataCallbackCannotMutateAlreadyComparedCandidate(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan =
        spark.sql("SELECT secret FROM a.db.t WHERE region='keep'").queryExecution.normalized
      val literal = plan
        .collect { case node =>
          node.expressions.flatMap(_.collect { case l: Literal if l.dataType == StringType => l })
        }
        .flatten
        .find(_.value.toString == "keep")
        .get
      val bytes = literal.value.asInstanceOf[UTF8String].getBaseObject.asInstanceOf[Array[Byte]]
      NativeSelectFixture.bindingFixture(spark, "a").onRead = _ => bytes(0) = 'z'.toByte
      rejected("logical binding changed", () => DeltaRangerReadRule.check(plan, spark))
    }

  @Test
  def changedExprIdsAndMixedOrReusedChildrenCannotBorrowReceipt(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      val plan = spark
        .sql("SELECT x.secret,y.secret FROM a.db.t x JOIN b.db.t y ON x.id=y.id")
        .queryExecution
        .normalized
        .asInstanceOf[Project]
      val output = plan.projectList.head.asInstanceOf[AttributeReference]
      val changedOutput = plan.copy(projectList =
        output.withExprId(NamedExpression.newExprId) +: plan.projectList.tail
      )
      changedOutput.setTagValue(tag, plan.getTagValue(tag).get)
      rejected("attribute producer changed", () => DeltaRangerReadRule.check(changedOutput, spark))
      val join = plan.child.asInstanceOf[Join]
      val reused = plan.copy(child = join.copy(right = join.left))
      reused.setTagValue(tag, plan.getTagValue(tag).get)
      val failure =
        assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(reused, spark))
      assertTrue(
        failure.getMessage.contains("attribute producer changed") || failure.getMessage.contains(
          "logical binding changed"
        )
      )
      val other = spark.sql("SELECT secret,id FROM b.db.t").queryExecution.normalized
      val mixed = plan.copy(child = join.copy(right = other))
      mixed.setTagValue(tag, plan.getTagValue(tag).get)
      val mixedFailure =
        assertThrows(classOf[SecurityException], () => DeltaRangerReadRule.check(mixed, spark))
      assertTrue(
        mixedFailure.getMessage.contains("attribute producer changed") || mixedFailure.getMessage
          .contains("logical binding changed")
      )
    }

  private def replaceLeaf(plan: LogicalPlan, leaf: LogicalRelation): LogicalPlan = plan match {
    case p: Project         => p.copy(child = replaceLeaf(p.child, leaf))
    case f: Filter          => f.copy(child = replaceLeaf(f.child, leaf))
    case a: SubqueryAlias   => a.copy(child = replaceLeaf(a.child, leaf))
    case _: LogicalRelation => leaf
  }

  private def rejected(part: String, action: () => Any): Unit = {
    val failure = assertThrows(classOf[Exception], () => { action(); () })
    var cause: Throwable = failure
    while (cause != null && !cause.isInstanceOf[SecurityException]) cause = cause.getCause
    assertNotNull(cause, "Expected authority failure, not an unrelated Spark error")
    assertTrue(cause.getMessage.contains(part), cause.getMessage)
  }
}
