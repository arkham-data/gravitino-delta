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

import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.DataMaskingStage0Marker
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.RowFilterMarker
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

private[ranger] object DeltaRangerMaskRules {
  final class Decisions(
      val filter: java.util.Optional[String],
      val masks: Map[String, DeltaRangerPolicyEngine.MaskDecision]
  )

  def capture(
      scan: DeltaRangerReadRule.Scan,
      columns: Set[String],
      engine: DeltaRangerPolicyEngine,
      subject: DeltaRangerQuerySubject,
      instant: java.time.Instant
  ): Decisions = {
    def key(column: String): DeltaRangerResourceKey =
      new DeltaRangerResourceKey(
        scan.catalog,
        scan.database,
        scan.table,
        column,
        "select",
        scan.owner.orNull
      )
    new Decisions(
      engine.rowFilter(subject, key(null), instant),
      columns.toVector.sorted
        .map(column => column -> engine.mask(subject, key(column), instant))
        .toMap
    )
  }

  def rewrite(
      scan: DeltaRangerReadRule.Scan,
      columns: Set[String],
      decisions: Decisions,
      spark: SparkSession
  ): LogicalPlan = {
    val filter = decisions.filter
    val filtered = if (filter.isPresent) {
      val condition = resolve(filter.get(), scan.relation.output, spark)
      if (condition.dataType != BooleanType)
        throw new SecurityException("Ranger row filter must be boolean")
      Filter(condition, RowFilterMarker(scan.relation))
    } else scan.relation
    val output = scan.relation.output.map { attribute =>
      if (!columns(attribute.name)) attribute
      else {
        val decision = decisions.masks(attribute.name)
        val expression = decision.kind() match {
          case DeltaRangerPolicyEngine.MaskDecision.Kind.NONE => None
          case DeltaRangerPolicyEngine.MaskDecision.Kind.NULL =>
            Some(Literal.create(null, attribute.dataType))
          case DeltaRangerPolicyEngine.MaskDecision.Kind.EXPRESSION =>
            val expr = resolve(decision.expression().get(), Seq(attribute), spark)
            if (expr.dataType != attribute.dataType)
              throw new SecurityException("Ranger mask must preserve its column type")
            Some(expr)
        }
        expression
          .map(expr => Alias(expr, attribute.name)(qualifier = attribute.qualifier))
          .getOrElse(attribute)
      }
    }
    if (output == scan.relation.output) filtered
    else DataMaskingStage0Marker(Project(output, filtered), scan.relation)
  }

  private def resolve(sql: String, attributes: Seq[Attribute], spark: SparkSession): Expression = {
    try {
      val parsed = spark.sessionState.sqlParser.parseExpression(sql)
      val resolved = parsed.transformUp {
        case unresolved: UnresolvedAttribute =>
          if (unresolved.nameParts.size != 1)
            throw new SecurityException("Ranger expression scope is not a top-level column")
          attributes.filter(_.name == unresolved.nameParts.head) match {
            case Seq(attribute) => attribute
            case _ =>
              throw new SecurityException("Ranger expression references an unqualified column")
          }
        case function: UnresolvedFunction =>
          if (
            function.nameParts.size != 1 || function.isDistinct || function.filter.nonEmpty || function.ignoreNulls
          ) {
            throw new SecurityException("Unsupported Ranger function expression")
          }
          spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier(function.nameParts.head), function.arguments)
      }
      DeltaRangerPlanAdmission.expression(resolved)
      if (!resolved.resolved || !resolved.checkInputDataTypes().isSuccess)
        throw new SecurityException("Invalid Ranger expression type")
      // ResolveTimeZone fills even already-resolved numeric casts on the next analyzer pass.
      // Complete that field before receipt issuance, retaining the strict type checks above.
      resolved.transformUp {
        case cast: Cast if cast.timeZoneId.isEmpty =>
          cast.withTimeZone(SQLConf.get.sessionLocalTimeZone)
      }
    } catch {
      case _: Exception =>
        throw new SecurityException("Unsupported Ranger mask or row filter expression")
    }
  }
}
