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

import org.apache.spark.sql.catalyst.expressions.{
  And,
  Attribute,
  AttributeReference,
  EqualTo,
  Expression
}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{
  Filter,
  Join,
  LogicalPlan,
  Project,
  SubqueryAlias
}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Ordinary reads have their own admission boundary; shared policy grammar stays finite. */
private[ranger] object DeltaRangerOrdinaryAdmission {
  def query(plan: LogicalPlan): Unit = DeltaRangerPlanAdmission.query(plan)

  // Dataset.join(usingColumns) analyzes a conditionless Inner before creating its UsingJoin.
  // This is analysis-only intent; query() still requires a condition before authorization.
  def pendingQuery(plan: LogicalPlan): Unit = {
    if (!plan.resolved || plan.isStreaming) denied("unresolved or streaming plan")
    plan.foreach {
      case _: Project | _: Filter | _: SubqueryAlias | _: LogicalRelation =>
      case join: Join if join.joinType == Inner                           =>
      case other                                                          => denied(other.nodeName)
    }
    plan.foreach {
      case _: LogicalRelation =>
      case node               => node.expressions.foreach(DeltaRangerPlanAdmission.expression)
    }
  }

  // Only the measured ResolveNaturalAndUsingJoin passthrough is absent from authored demand.
  // Keep the executable tree intact, and give both Kyuubi and structural extraction this same
  // representation. An explicit outer select("*") remains a separate authored Project.
  def demandPlan(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case project: Project if generatedUsingProjection(project) => project.child
  }

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
