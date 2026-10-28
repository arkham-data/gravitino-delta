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
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.analysis.LocalTempView
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.execution.command.{CreateViewCommand, ExecutedCommandExec}
import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}

/** Only native DataFrame local definitions can carry pending ordinary intent. */
private[ranger] object DeltaRangerOrdinaryViews {
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.local-view-command")
  private final class Reference(@transient val value: Issued) extends Serializable {
    def issued: Issued = if (value == null) denied("inert command") else value
    override def toString: String = "Local view definition command"
  }
  private final class Issued(original: CreateViewCommand, session: SparkSession) {
    private val fields = signature(original)
    private val definition = DeltaRangerDecisionReceipt.binding(original.plan)
    def validate(candidate: CreateViewCommand, spark: SparkSession): Unit = {
      if (
        !(spark eq session) || signature(candidate) != fields || signature(original) != fields ||
        DeltaRangerDecisionReceipt.binding(candidate.plan) != definition ||
        DeltaRangerDecisionReceipt.binding(original.plan) != definition
      )
        denied("command or definition changed")
      DeltaRangerOrdinaryRead.check(candidate.plan, spark)
    }
  }

  def resolve(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] = {
    plan match {
      case command: CreateViewCommand =>
        validateShape(command)
        if (command.getTagValue(tag).isEmpty) {
          DeltaRangerOrdinaryRead.check(command.plan, spark)
          command.setTagValue(tag, new Reference(new Issued(command, spark)))
        }
      case _ =>
    }
    if (check(plan, spark)) return Some(plan)
    if (!plan.resolved || !plan.exists(_.isInstanceOf[View])) return None
    plan.foreach {
      case view: View =>
        validateDefinition(view)
        DeltaRangerOrdinaryRead.checkResolvedViewDefinition(view.child, spark)
      case _ =>
    }
    Some(plan)
  }

  // EliminateSubqueryAliases returns the child without the root receipt. Remove only
  // leading aliases, after original authorization and before issuing that receipt.
  def executableRoot(plan: LogicalPlan): LogicalPlan = plan match {
    case alias: SubqueryAlias =>
      if (
        alias.output != alias.child.output.map(
          _.withQualifier(alias.identifier.qualifier :+ alias.alias)
        )
      )
        denied("alias output changed")
      executableRoot(alias.child)
    case _ => plan
  }

  def validateDefinition(view: View): Unit = {
    if (
      view.getClass != classOf[View] || !view.isTempView ||
      !view.isTempViewStoringAnalyzedPlan || view.desc.viewText.nonEmpty ||
      view.desc.identifier.database.nonEmpty || view.desc.identifier.catalog.nonEmpty ||
      view.output != view.child.output
    ) denied("unsupported definition")
  }

  def check(plan: LogicalPlan, spark: SparkSession): Boolean = plan match {
    case command: CreateViewCommand =>
      command.getTagValue(tag).getOrElse(denied("unissued command")).issued.validate(command, spark)
      true
    case result: CommandResult =>
      (result.commandLogicalPlan, result.commandPhysicalPlan) match {
        case (logical: CreateViewCommand, physical: ExecutedCommandExec)
            if result.output.isEmpty && result.rows.isEmpty &&
              logical.getTagValue(tag).nonEmpty &&
              logical.getTagValue(tag) == physical.cmd.getTagValue(tag) =>
          check(logical, spark) && checkExecution(physical, spark)
        case _ => false
      }
    case _ => false
  }

  private def signature(command: CreateViewCommand): Vector[Any] = {
    validateShape(command)
    Vector(
      command.name,
      command.userSpecifiedColumns,
      command.comment,
      command.properties,
      command.originalText,
      command.allowExisting,
      command.replace,
      command.viewType,
      command.isAnalyzed,
      command.referredTempFunctions
    )
  }

  private def validateShape(command: CreateViewCommand): Unit = {
    if (
      command.getClass != classOf[CreateViewCommand] ||
      command.viewType != LocalTempView || !command.isAnalyzed ||
      command.originalText.nonEmpty || command.userSpecifiedColumns.nonEmpty ||
      command.comment.nonEmpty || command.properties.nonEmpty ||
      command.referredTempFunctions.nonEmpty || command.allowExisting ||
      command.name.database.nonEmpty || command.name.catalog.nonEmpty
    )
      denied("only DataFrame local create/replace is qualified")
  }

  // These stock command executions return no payload and only mutate this session's
  // local definitions. Recheck the retained pending definition at both transitions.
  def checkExecution(plan: SparkPlan, spark: SparkSession): Boolean = plan match {
    case command: ExecutedCommandExec if command.getClass == classOf[ExecutedCommandExec] =>
      check(command.cmd, spark)
    case result: CommandResultExec
        if result.getClass == classOf[CommandResultExec] &&
          result.output.isEmpty && result.rows.isEmpty =>
      checkExecution(result.commandPhysicalPlan, spark)
    case _ => false
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta local view rejected: " + reason)
}
