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

import java.util.Locale
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.delta.commands.DeltaCommand

/** Executes only the native scalar idempotency read under an owned target transaction. */
private[ranger] object DeltaRangerNativeIdempotency {
  def hasBeenExecuted(
      spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      options: Map[String, String]
  ): Boolean = {
    if (
      spark == null || target == null || options == null ||
      !SparkSession.getActiveSession.exists(_ eq spark) ||
      OptimisticTransaction.getActive.nonEmpty || DeltaRangerNativeOperation.current() != null ||
      spark.sessionState.conf.getConfString(
        "spark.databricks.delta.write.txnVersion.autoReset.enabled",
        "true"
      ) != "false"
    )
      throw new SecurityException("Native idempotency entry rejected")
    val copied = options.iterator.toMap
    val names = copied.keysIterator.map { key =>
      if (key == null) throw new IllegalArgumentException("Invalid idempotency option")
      key.toLowerCase(Locale.ROOT)
    }.toVector
    if (
      names.distinct.size != names.size ||
      names.exists(name => name != "txnappid" && name != "txnversion") ||
      copied.values.exists(_ == null)
    )
      throw new IllegalArgumentException("Invalid idempotency options")
    val nativeOptions = new DeltaOptions(copied, spark.sessionState.conf)
    val receipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
    val result = target.log().withNewTransaction(target.table().catalogTable) { txn =>
      receipt.validate(spark)
      val binding = target.bindTransaction(spark, txn)
      val result =
        try {
          val operation = DeltaRangerNativeOperation.transactionState(binding)
          try {
            operation.captureTransactionState()
            val value = Driver.read(txn, spark, nativeOptions)
            binding.requireLive()
            operation.succeed()
            value
          } finally operation.close()
        } finally binding.close()
      receipt.validate(spark)
      result
    }
    receipt.validate(spark)
    result
  }

  private object Driver extends DeltaCommand {
    def read(txn: OptimisticTransaction, spark: SparkSession, options: DeltaOptions): Boolean =
      hasBeenExecuted(txn, spark, Some(options))
  }
}
