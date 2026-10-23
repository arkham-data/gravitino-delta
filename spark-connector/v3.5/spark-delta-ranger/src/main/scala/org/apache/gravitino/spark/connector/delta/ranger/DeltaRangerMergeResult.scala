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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{
  CommandResult,
  LeafNode,
  LogicalPlan,
  Statistics
}
import org.apache.spark.sql.execution.{CommandResultExec, LeafExecNode, SparkPlan}

/** Completed native metrics only; this continuation cannot execute the command or read payload. */
private[ranger] final class DeltaRangerMergeResult(
    receipt: DeltaRangerMergeReceipt,
    original: CommandResult
) {
  val carrier = DeltaRangerMergeResult.Carrier(original.output)(this)
  private val rows = original.rows.map(_.copy()).toVector
  private val delegate = CommandResultExec(original.output, original.commandPhysicalPlan, rows)
  private var root: DeltaRangerMergeResult.Exec = null
  def check(candidate: DeltaRangerMergeResult.Carrier): Unit = {
    receipt.requireResult(this)
    if (candidate ne carrier) DeltaRangerMergeCommand.denied("copied result carrier")
  }
  def plan(candidate: DeltaRangerMergeResult.Carrier): SparkPlan = {
    check(candidate)
    if (root == null) root = DeltaRangerMergeResult.Exec()(this)
    root
  }
  def verify(candidate: DeltaRangerMergeResult.Exec): Unit = {
    check(carrier)
    if (candidate ne root) DeltaRangerMergeCommand.denied("copied result execution")
  }
  def collect(candidate: DeltaRangerMergeResult.Exec): Array[InternalRow] = {
    verify(candidate)
    delegate.executeCollect()
  }
}
private[ranger] object DeltaRangerMergeResult {
  final case class Carrier(override val output: Seq[Attribute])(
      @transient private val authority: DeltaRangerMergeResult
  ) extends LeafNode {
    def owner: DeltaRangerMergeResult = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert result carrier")
      authority.check(this); authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): LogicalPlan = this
    override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(32))
  }
  final case class Exec()(@transient private val authority: DeltaRangerMergeResult)
      extends LeafExecNode {
    def verify(): Unit = {
      if (authority == null || isCanonicalizedPlan)
        DeltaRangerMergeCommand.denied("inert result execution")
      authority.verify(this)
    }
    override def output = authority.carrier.output
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): SparkPlan = this
    override def executeCollect(): Array[InternalRow] = authority.collect(this)
    override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] =
      DeltaRangerMergeCommand.denied("command result is metrics-only")
  }
}
