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

import java.util.{Map => JMap}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._

/** Ordered clauses for one synchronous governed named MERGE attempt. Not thread-safe. */
final class GravitinoDeltaMergeBuilder private[ranger] (
    private val context: GravitinoDeltaTable.Context,
    private val target: LogicalPlan,
    private val source: LogicalPlan,
    private val expressions: DeltaRangerMergeBuilderExpressions,
    private val condition: Expression
) {
  private var matched = Vector.empty[MergeAction]
  private var unmatched = Vector.empty[MergeAction]
  private var sealedExecution = false

  /** Add an unconditional matched UPDATE with a snapshot of authored assignments. */
  def whenMatchedUpdate(assignments: JMap[String, _]): GravitinoDeltaMergeBuilder =
    addMatched(UpdateAction(None, copyAssignments(assignments)))

  /** Add a matched UPDATE using an expression-string predicate. */
  def whenMatchedUpdate(
      condition: String,
      assignments: JMap[String, _]
  ): GravitinoDeltaMergeBuilder =
    addMatched(
      UpdateAction(Some(predicate(condition)), copyAssignments(assignments))
    )

  /** Add a matched UPDATE retaining a Column predicate. */
  def whenMatchedUpdate(
      condition: Column,
      assignments: JMap[String, _]
  ): GravitinoDeltaMergeBuilder =
    addMatched(
      UpdateAction(Some(predicate(condition)), copyAssignments(assignments))
    )

  /** Add an unconditional unmatched INSERT with a snapshot of authored assignments. */
  def whenNotMatchedInsert(assignments: JMap[String, _]): GravitinoDeltaMergeBuilder =
    addUnmatched(InsertAction(None, copyAssignments(assignments)))

  /** Add an unmatched INSERT using an expression-string predicate. */
  def whenNotMatchedInsert(
      condition: String,
      assignments: JMap[String, _]
  ): GravitinoDeltaMergeBuilder =
    addUnmatched(
      InsertAction(Some(predicate(condition)), copyAssignments(assignments))
    )

  /** Add an unmatched INSERT retaining a Column predicate. */
  def whenNotMatchedInsert(
      condition: Column,
      assignments: JMap[String, _]
  ): GravitinoDeltaMergeBuilder =
    addUnmatched(
      InsertAction(Some(predicate(condition)), copyAssignments(assignments))
    )

  /** Add an unconditional matched DELETE. */
  def whenMatchedDelete(): GravitinoDeltaMergeBuilder = addMatched(DeleteAction(None))

  /** Add a matched DELETE using an expression-string predicate. */
  def whenMatchedDelete(condition: String): GravitinoDeltaMergeBuilder =
    addMatched(DeleteAction(Some(predicate(condition))))

  /** Add a matched DELETE retaining a Column predicate. */
  def whenMatchedDelete(condition: Column): GravitinoDeltaMergeBuilder =
    addMatched(DeleteAction(Some(predicate(condition))))

  /** Add an unconditional native UPDATE star action. */
  def whenMatchedUpdateAll(): GravitinoDeltaMergeBuilder = addMatched(UpdateStarAction(None))

  /** Add a native UPDATE star action using an expression-string predicate. */
  def whenMatchedUpdateAll(condition: String): GravitinoDeltaMergeBuilder =
    addMatched(UpdateStarAction(Some(predicate(condition))))

  /** Add a native UPDATE star action retaining a Column predicate. */
  def whenMatchedUpdateAll(condition: Column): GravitinoDeltaMergeBuilder =
    addMatched(UpdateStarAction(Some(predicate(condition))))

  /** Add an unconditional native INSERT star action. */
  def whenNotMatchedInsertAll(): GravitinoDeltaMergeBuilder = addUnmatched(InsertStarAction(None))

  /** Add a native INSERT star action using an expression-string predicate. */
  def whenNotMatchedInsertAll(condition: String): GravitinoDeltaMergeBuilder =
    addUnmatched(InsertStarAction(Some(predicate(condition))))

  /** Add a native INSERT star action retaining a Column predicate. */
  def whenNotMatchedInsertAll(condition: Column): GravitinoDeltaMergeBuilder =
    addUnmatched(InsertStarAction(Some(predicate(condition))))

  /** Seal this attempt and submit the full command through normal Spark query execution. */
  def execute(): Unit = {
    requireOpen()
    sealedExecution = true
    context.validate()
    val command = MergeIntoTable(target, source, condition, matched, unmatched, Nil)
    val execution = context.spark.sessionState.executePlan(command)
    execution.assertAnalyzed()
    execution.analyzed match {
      case owned: DeltaRangerMergeCommand => owned.receipt
      case _ => throw new SecurityException("Delta MERGE requires an issued owned command")
    }
    // commandExecuted synchronously executes only the authorized command and its metric result.
    execution.commandExecuted
    ()
  }

  private def requireOpen(): Unit =
    if (sealedExecution) throw new IllegalStateException("Delta MERGE builder is single-use")

  private def predicate(value: Any): Expression = {
    requireOpen()
    context.validate()
    expressions.expression(value)
  }

  private def copyAssignments(values: JMap[String, _]): Vector[Assignment] = {
    requireOpen()
    context.validate()
    expressions.assignments(values)
  }

  private def addMatched(action: MergeAction): GravitinoDeltaMergeBuilder = {
    requireOpen()
    context.validate()
    matched :+= action
    this
  }

  private def addUnmatched(action: MergeAction): GravitinoDeltaMergeBuilder = {
    requireOpen()
    context.validate()
    unmatched :+= action
    this
  }
}
