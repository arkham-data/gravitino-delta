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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.MetadataBuilder

/** Pending named intent carries registration provenance, never payload permission. */
private[ranger] object DeltaRangerOrdinaryRead {
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.ordinary-origin")

  private final class Reference(@transient private val value: Origin) extends Serializable {
    def origin: Origin = {
      if (value == null) denied("inert origin reference")
      value
    }
    override def toString: String = "Pending Delta origin reference"
  }

  private final class Origin(
      plan: LogicalPlan,
      spark: SparkSession,
      val catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ) {
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val settings = spark.sessionState.conf.getAllConfs
    private val original = {
      val copy = plan.clone()
      copy.foreach(_.unsetTagValue(tag))
      copy
    }
    private val intent = binding(original)
    private val cleanedIntent = binding(cleanDatasetReferences(original))
    private val registrations = plan.collect { case relation: LogicalRelation =>
      val scan = DeltaRangerReadRule.canonical(relation)
      catalogs(scan).registrationBindings().resolve(relation)
    }
    if (registrations.isEmpty) denied("missing named occurrence")

    def validate(candidate: LogicalPlan, session: SparkSession, remap: Boolean = false): Unit = {
      def unchanged(): Unit = {
        if (!(session eq spark)) denied("session changed")
        val current = DeltaRangerQuerySubject.current(session)
        if (current.user() != subject.user() || current.groups() != subject.groups())
          denied("subject changed")
        if (settings != session.sessionState.conf.getAllConfs) denied("configuration changed")
        if (binding(original) != intent)
          denied("retained intent changed")
        val actual = binding(candidate)
        if (
          actual != intent && actual != cleanedIntent &&
          (!remap || !sameAfterIdRemap(candidate))
        ) denied("original intent changed")
      }
      unchanged()
      registrations.foreach(_.recheckPublication())
      unchanged()
    }

    override def toString: String = "Pending Delta origin"

    // DeduplicateRelations gives the right-hand occurrence fresh ExprIds. Only a bijective
    // producer-position rename is accepted; the full finite binding still checks every field,
    // reference, qualifier and exact native registration identity after that rename.
    private def sameAfterIdRemap(candidate: LogicalPlan): Boolean = {
      val before = original.flatMap(_.output.map(_.exprId))
      val after = candidate.flatMap(_.output.map(_.exprId))
      if (before.size != after.size) return false
      val pairs = before.zip(after).distinct
      if (
        pairs.map(_._1).distinct.size != pairs.size ||
        pairs.map(_._2).distinct.size != pairs.size
      ) return false
      val ids: Map[ExprId, ExprId] = pairs.toMap
      val renamed = original.transformAllExpressions {
        case attr: AttributeReference => attr.withExprId(ids.getOrElse(attr.exprId, attr.exprId))
        case alias: Alias =>
          alias.copy(child = alias.child, name = alias.name)(
            exprId = ids.getOrElse(alias.exprId, alias.exprId),
            qualifier = alias.qualifier,
            explicitMetadata = alias.explicitMetadata,
            nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
          )
      }
      val actual = binding(candidate)
      binding(renamed) == actual ||
      binding(cleanDatasetReferences(renamed)) == actual
    }

  }

  def stage(
      plan: LogicalPlan,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): LogicalPlan = {
    admit(plan)
    DeltaRangerDecisionReceipt.qualifyNative(plan, spark)
    plan.foreach { node =>
      node.getTagValue(tag).foreach(_.origin.validate(node, spark, remap = true))
    }
    val origin = new Origin(plan, spark, catalogs)
    origin.validate(plan, spark)
    plan.setTagValue(tag, new Reference(origin))
    plan
  }

  def pending(plan: LogicalPlan): Boolean = plan.getTagValue(tag).nonEmpty

  def check(plan: LogicalPlan, spark: SparkSession): Unit = {
    admit(plan)
    plan.getTagValue(tag).getOrElse(denied("missing origin")).origin.validate(plan, spark)
  }

  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] = {
    if (pending(plan) || DeltaRangerDecisionReceipt.present(plan)) rejectCacheConstruction()
    if (!pending(plan)) return None
    check(plan, spark)
    val origin = plan.getTagValue(tag).get.origin
    val copy = plan.clone()
    copy.foreach(_.unsetTagValue(tag))
    val authorized = new DeltaRangerReadRule(spark, origin.catalogs).authorizeComplete(copy)
    origin.validate(plan, spark)
    DeltaRangerDecisionReceipt.check(authorized, spark, optimized = false)
    Some(authorized)
  }

  // Stock CacheManager first normalizes the Dataset, then plans its normalized result in a
  // new QueryExecution before installing an entry. With all forced-off settings already off,
  // that compilation uses the same session. Reject this unsupported operation on both pinned
  // Spark patches. This is a denial-only check: no caller frame ever grants execution authority,
  // and native metadata/helper normalization without an ordinary origin/receipt stays separate.
  private def rejectCacheConstruction(): Unit = {
    if (
      Thread
        .currentThread()
        .getStackTrace
        .exists(frame =>
          frame.getClassName == "org.apache.spark.sql.execution.CacheManager" &&
            frame.getMethodName == "cacheQuery"
        )
    )
      denied("ordinary cache construction is not qualified")
  }

  private def admit(plan: LogicalPlan): Unit = {
    if (
      plan.exists(node =>
        DeltaRangerDecisionReceipt.executableReference(node) ||
          DeltaRangerNativeScans.joinLocatorPresent(node)
      )
    )
      denied("executable receipt cannot become an origin")
    DeltaRangerOrdinaryAdmission.pendingQuery(plan)
  }

  // DetectAmbiguousSelfJoin removes exactly these Dataset reference bookkeeping keys after
  // resolution. No user metadata, qualifiers, expression IDs or expression fields are erased.
  private def cleanDatasetReferences(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressions {
      case attr: AttributeReference
          if attr.metadata.contains("__dataset_id") && attr.metadata.contains("__col_position") =>
        attr.withMetadata(
          new MetadataBuilder()
            .withMetadata(attr.metadata)
            .remove("__dataset_id")
            .remove("__col_position")
            .build()
        )
    }

  private def binding(plan: LogicalPlan): (Vector[Any], Vector[Any]) =
    (
      DeltaRangerDecisionReceipt.binding(plan),
      DeltaRangerDecisionReceipt.binding(DeltaRangerOrdinaryAdmission.demandPlan(plan))
    )

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta ordinary origin rejected: " + reason)
}
