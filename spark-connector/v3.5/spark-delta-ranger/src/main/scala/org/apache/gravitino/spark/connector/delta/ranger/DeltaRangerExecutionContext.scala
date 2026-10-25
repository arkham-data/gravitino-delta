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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Driver-only accepted recipe; internal execution never refreshes its policy decisions. */
private[ranger] object DeltaRangerExecutionContext {
  sealed trait Context {
    def logicalPlan(): LogicalPlan
  }

  def forRead(
      receipt: DeltaRangerDecisionReceipt,
      spark: SparkSession,
      recipe: LogicalPlan
  ): Context = {
    val prepared = DeltaRangerNativeScans.prepared(recipe)
    if (!prepared.owner.owns(receipt))
      throw new SecurityException("Read recipe belongs to another receipt")
    prepared.verifyReadRecipe(recipe, spark)
    receipt.validateAction(spark)
    new ReadContext(recipe.clone())
  }

  private final class ReadContext(private val recipe: LogicalPlan) extends Context {
    override def logicalPlan(): LogicalPlan = recipe.clone()
  }
}
