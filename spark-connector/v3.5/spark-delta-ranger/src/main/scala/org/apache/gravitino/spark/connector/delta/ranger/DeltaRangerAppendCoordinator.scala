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
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.OptimisticTransaction

/** One denial-only transaction joining the actual native input, scalar and source preparation. */
private[ranger] object DeltaRangerAppendCoordinator {
  private val preparing = new ThreadLocal[Source]()
  val preparedMessage = "Delta append source prepared; writes remain disabled"
  val duplicateMessage = "Delta append duplicate validated; writes remain disabled"

  def run(input: DeltaRangerAppendV1Input.RetainedV1Input): Nothing =
    try input.association.coordinate(input)
    finally input.close()

  def inTransaction(
      input: DeltaRangerAppendV1Input.RetainedV1Input,
      owner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
      transaction: OptimisticTransaction,
      options: Map[String, String]
  ): Nothing = {
    def validate(): Unit = input.association.validateRetainedOwned(input, owner, transaction)
    validate()
    val duplicate = DeltaRangerNativeIdempotency.readInTransaction(owner, transaction, options)
    validate()
    if (duplicate) {
      validate()
      throw new SecurityException(duplicateMessage)
    }
    val source = new Source(input, owner, transaction)
    try {
      source.prepare()
      validate()
      throw new SecurityException(preparedMessage)
    } finally source.close()
  }

  def source(plan: LogicalPlan, spark: SparkSession): Option[DeltaRangerNativeScans] =
    Option(preparing.get()).map(_.consume(plan, spark))

  /** Preparation-only dependency retained by every issued native index and evidence object. */
  final class Source private[DeltaRangerAppendCoordinator] (
      input: DeltaRangerAppendV1Input.RetainedV1Input,
      transactionOwner: DeltaRangerRegistrationBindings.BoundAppendTransaction,
      transaction: OptimisticTransaction
  ) {
    private var closed = false
    private var entered = false
    private var claimed = false
    private var nativeOwner: DeltaRangerNativeScans = _
    private var issued: DeltaRangerNativeScans.Prepared = _

    def requireLive(): Unit = {
      if (closed) denied("expired source owner")
      input.requireLive()
      transactionOwner.requireTransaction(transaction)
      input.requireLive()
    }

    def requireGenerator(candidate: OptimisticTransaction): Unit = {
      requireLive()
      if (!(candidate eq transaction)) denied("foreign source transaction generator")
    }

    def consume(plan: LogicalPlan, spark: SparkSession): DeltaRangerNativeScans = {
      requireLive()
      if (!entered || claimed || !(preparing.get() eq this))
        denied("source preparation claim changed")
      if (
        !(spark eq input.session) ||
        !QueryPlanningTracker.get.exists(_ eq input.queryExecution.tracker)
      )
        denied("source planning tracker changed")
      claimed = true
      input.receipt.consume(plan, spark, optimized = true)
      requireLive()
      nativeOwner = DeltaRangerNativeScans.forAppend(spark, input.receipt, this)
      nativeOwner
    }

    def complete(prepared: DeltaRangerNativeScans.Prepared): Unit = {
      requireLive()
      if (
        !claimed || issued != null || !(preparing.get() eq this) ||
        nativeOwner == null || !(prepared.owner eq nativeOwner)
      )
        denied("source preparation issuance changed")
      issued = prepared
    }

    def prepare(): Unit = {
      requireLive()
      if (entered || preparing.get() != null) denied("nested source preparation")
      entered = true
      preparing.set(this)
      try {
        val actual = input.queryExecution.optimizedPlan
        requireLive()
        if (
          !claimed || issued == null ||
          !DeltaRangerNativeScans.evidence(actual).exists(_ eq issued)
        )
          denied("actual input preparation evidence changed")
        issued.verifyExecutable(actual, input.session)
        requireLive()
      } finally preparing.remove()
    }

    def close(): Unit = { closed = true }
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta append source rejected: " + reason)
}
