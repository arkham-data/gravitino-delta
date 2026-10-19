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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

/** Raw metadata boundary that rejects changed children before Spark can prepare them. */
private[ranger] final case class DeltaRangerRawMetadataGuardExec(child: SparkPlan)(
    @transient private val driverContext: DeltaRangerPhysicalProvenance.RawMetadataContext
) extends UnaryExecNode {
  requiredDriverContext().validateCollection(child)

  override def output: Seq[Attribute] = child.output
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(driverContext)

  override def jsonFields = super.jsonFields.filterNot(_._1 == "driverContext")

  override def executeCollect(): Array[InternalRow] = {
    requiredDriverContext().validateCollection(child)
    super.executeCollect()
  }

  override protected def doExecute(): RDD[InternalRow] = {
    requiredDriverContext().validateExecution(child)
    child.execute()
  }

  /** Preserve Spark's inherited visible-child cleanup traversal. */
  override def cleanupResources(): Unit = super.cleanupResources()

  override protected def withNewChildInternal(
      newChild: SparkPlan
  ): DeltaRangerRawMetadataGuardExec = DeltaRangerRawMetadataGuardExec(newChild)(driverContext)

  private def requiredDriverContext(): DeltaRangerPhysicalProvenance.RawMetadataContext = {
    if (isCanonicalizedPlan) {
      throw new IllegalStateException("Canonicalized Delta Ranger collection guard cannot execute")
    }
    if (driverContext == null) {
      throw new IllegalStateException("Delta Ranger collection guard has no validation context")
    }
    driverContext
  }
}
