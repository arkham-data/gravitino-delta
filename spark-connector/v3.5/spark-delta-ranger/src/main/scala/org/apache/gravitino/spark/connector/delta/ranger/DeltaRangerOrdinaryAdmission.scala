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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, LongType}

/** Ordinary reads have their own admission boundary; shared policy grammar stays finite. */
private[ranger] object DeltaRangerOrdinaryAdmission {
  def query(plan: LogicalPlan): Unit = admit(plan, pending = false)

  // Dataset.join(usingColumns) temporarily analyzes a conditionless Inner.
  def pendingQuery(plan: LogicalPlan): Unit = admit(plan, pending = true)

  private def admit(plan: LogicalPlan, pending: Boolean): Unit = {
    if (!plan.resolved || plan.isStreaming) denied("unresolved or streaming plan")
    plan.foreach {
      case _: LogicalRelation =>
      case view: View         => DeltaRangerOrdinaryViews.validateDefinition(view)
      case project: Project if displayProjection(project) =>
      case node @ (_: Project | _: Filter | _: SubqueryAlias) =>
        node.expressions.foreach(DeltaRangerPlanAdmission.expression)
      case join: Join if join.joinType == Inner && (pending || join.condition.nonEmpty) =>
        join.expressions.foreach(DeltaRangerPlanAdmission.expression)
      case aggregate: Aggregate
          if aggregate.groupingExpressions.isEmpty &&
            aggregate.aggregateExpressions.nonEmpty =>
        aggregate.aggregateExpressions.foreach {
          case alias: Alias =>
            alias.child match {
              case expression: AggregateExpression
                  if expression.mode == Complete &&
                    !expression.isDistinct && expression.filter.isEmpty =>
                expression.aggregateFunction match {
                  case count: Count if count.children.size == 1 =>
                    count.children.head match {
                      case _: AttributeReference                           =>
                      case Literal(1, IntegerType) | Literal(1L, LongType) =>
                      case _ => denied("count argument")
                    }
                  case _ => denied("aggregate function")
                }
              case _ => denied("aggregate expression")
            }
          case _ => denied("aggregate output")
        }
      case distinct: Distinct if distinct.child.output.nonEmpty =>
      case dedup: Deduplicate
          if dedup.keys.nonEmpty &&
            dedup.keys.map(_.exprId).distinct.size == dedup.keys.size &&
            dedup.keys.map(_.exprId).toSet == dedup.child.output.map(_.exprId).toSet =>
      case sort: Sort if sort.global && sort.order.nonEmpty =>
        sort.order.foreach { order =>
          if (
            !order.child.isInstanceOf[AttributeReference] || order.sameOrderExpressions.nonEmpty ||
            !Set[SortDirection](Ascending, Descending)(order.direction) ||
            !Set[NullOrdering](NullsFirst, NullsLast)(order.nullOrdering)
          ) denied("sort expression")
        }
      case limit: GlobalLimit => literalLimit(limit.limitExpr)
      case limit: LocalLimit  => literalLimit(limit.limitExpr)
      case other              => denied(other.nodeName)
    }
  }

  // Dataset.getRows formats each selected value once, preserving output order. This
  // private display projection does not expand shared policy/CDF/MERGE expressions.
  private def displayProjection(project: Project): Boolean =
    project.projectList.nonEmpty && project.projectList.size == project.child.output.size &&
      project.projectList.zip(project.child.output).forall {
        case (alias: Alias, input) =>
          alias.child match {
            case pretty: ToPrettyString =>
              pretty.child == input &&
              pretty.timeZoneId
                .forall(_ == SQLConf.get.sessionLocalTimeZone)
            case _ => false
          }
        case _ => false
      }

  private def literalLimit(expression: Expression): Unit = expression match {
    case Literal(value: Int, IntegerType) if value >= 0 =>
    case _                                              => denied("limit expression")
  }

  // Only the measured ResolveNaturalAndUsingJoin passthrough is absent from authored demand.
  // Keep the executable tree intact, and give both Kyuubi and structural extraction this same
  // representation. An explicit outer select("*") remains a separate authored Project.
  def authoredDemandPlan(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case project: Project if generatedUsingProjection(project) => project.child
  }

  // Ordinary-only extraction carries hidden semantic demand across authored projections.
  // Append validates the original representation above before this transformation.
  def demandPlan(plan: LogicalPlan): LogicalPlan = {
    val authored = authoredDemandPlan(plan)
    val originalIds = semanticIds(authored)
    // Derived outputs keep traversal alive across nested authored projections/counts.
    // They are carriers only: all original base-column demand is checked again below.
    val visible = originalIds ++ authored.collect { case node @ (_: Project | _: Aggregate) =>
      node.output.map(_.exprId)
    }.flatten
    // Native display returns strings only at execution. Demand extraction keeps its
    // already-selected input values, before adding hidden original-demand carriers.
    val extraction = authored.transformDown {
      case project: Project if displayProjection(project) => project.child
    }
    val represented = extraction.transformUp {
      case view: View => view.child
      case project: Project =>
        val existing = project.projectList.map(_.exprId).toSet
        val carried = project.child.output.filter(a => visible(a.exprId) && !existing(a.exprId))
        project.copy(projectList = project.projectList ++ carried)
      case deduplicate: Deduplicate => Distinct(deduplicate.child)
      case aggregate: Aggregate if aggregate.groupingExpressions.isEmpty =>
        aggregate.copy(child =
          Project(aggregate.child.output.filter(a => visible(a.exprId)), aggregate.child)
        )
    }
    val representedIds = semanticIds(represented)
    val originalScans = authored.collect { case relation: LogicalRelation => relation }
    val representedScans = represented.collect { case relation: LogicalRelation => relation }
    if (
      originalScans.size != representedScans.size || !originalScans.zip(representedScans).forall {
        case (original, next) =>
          (original eq next) &&
          original.output.filter(a => originalIds(a.exprId)).map(_.name).toSet ==
            next.output.filter(a => representedIds(a.exprId)).map(_.name).toSet
      }
    ) denied("demand representation changed original occurrences")
    represented
  }

  private def semanticIds(plan: LogicalPlan): Set[ExprId] =
    plan.output.map(_.exprId).toSet ++ plan.collect {
      case distinct: Distinct => distinct.child.output.map(_.exprId)
      case node if !node.isInstanceOf[LogicalRelation] =>
        node.expressions.flatMap(_.references.map(_.exprId))
    }.flatten

  private def generatedUsingProjection(project: Project): Boolean = project.child match {
    case join: Join if join.joinType == Inner && join.condition.nonEmpty =>
      def conjuncts(expression: Expression): Seq[Expression] = expression match {
        case And(left, right) => conjuncts(left) ++ conjuncts(right)
        case other            => Seq(other)
      }
      val conditions = conjuncts(join.condition.get)
      val keys = conditions.collect {
        case EqualTo(left: AttributeReference, right: AttributeReference)
            if join.left.output.exists(_.exprId == left.exprId) &&
              join.right.output.exists(_.exprId == right.exprId) =>
          left -> right
      }
      val hidden = project.getTagValue(Project.hiddenOutputTag).getOrElse(Seq.empty[Attribute])
      val leftKeys = keys.map(_._1.exprId)
      val rightKeys = keys.map(_._2.exprId)
      val expected = leftKeys ++ join.left.output.map(_.exprId).filterNot(leftKeys.contains) ++
        join.right.output.map(_.exprId).filterNot(rightKeys.contains)
      keys.nonEmpty && keys.size == conditions.size &&
      leftKeys.distinct.size == keys.size && rightKeys.distinct.size == keys.size &&
      hidden.map(_.exprId) == rightKeys &&
      project.projectList.forall(_.isInstanceOf[AttributeReference]) &&
      project.projectList.map(_.exprId) == expected
    case _ => false
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Unqualified Delta Ranger route: " + reason)
}
