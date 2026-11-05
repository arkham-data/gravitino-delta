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

import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentTimestamp, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.types.TimestampType

/** Input-only query-time projection; this never expands the shared policy grammar. */
private[ranger] object DeltaRangerInputProjection {
  def expression(expr: Expression): Unit = expr match {
    case alias: Alias
        if alias.getClass == classOf[Alias] &&
          alias.child.getClass == classOf[CurrentTimestamp] && alias.child.children.isEmpty =>
    case other => DeltaRangerPlanAdmission.expression(other)
  }

  def freeze(plan: LogicalPlan): LogicalPlan = {
    DeltaRangerOrdinaryAdmission.query(plan)
    val original = DeltaRangerDecisionReceipt.binding(plan)
    val timestamps = plan.collect { case project: Project =>
      project.projectList.collect {
        case alias: Alias if alias.child.getClass == classOf[CurrentTimestamp] => alias
      }
    }.flatten
    if (timestamps.isEmpty) return plan
    // Spark captures one Instant and shares its timestamp literal across the entire query.
    // Do this before the existing receipt, so optimizer/native replay cannot sample again.
    val frozen = ComputeCurrentTime(plan)
    val literal = frozen
      .collect { case project: Project =>
        project.projectList.collect {
          case alias: Alias if alias.exprId == timestamps.head.exprId => alias.child
        }
      }
      .flatten
      .headOption
      .getOrElse(denied())
    literal match {
      case value: Literal
          if value.getClass == classOf[Literal] &&
            value.dataType == TimestampType && value.value != null =>
      case _ => denied()
    }
    // Check the complete authored-to-frozen relationship, including non-time expressions,
    // producer identities, alias metadata and every occurrence. No second clock invocation.
    val expected = plan.transformAllExpressions {
      case value: CurrentTimestamp if value.getClass == classOf[CurrentTimestamp] => literal
    }
    if (
      DeltaRangerDecisionReceipt.binding(plan) != original ||
      DeltaRangerDecisionReceipt.binding(frozen) != DeltaRangerDecisionReceipt.binding(expected) ||
      frozen.output != plan.output || frozen.schema != plan.schema
    ) denied()
    frozen
  }

  private def denied(): Nothing =
    throw new SecurityException("Delta input timestamp normalization changed authored intent")
}
