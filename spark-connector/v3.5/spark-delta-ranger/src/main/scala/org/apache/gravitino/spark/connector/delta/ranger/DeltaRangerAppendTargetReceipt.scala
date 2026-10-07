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

import java.time.Instant

import org.apache.spark.sql.SparkSession

/** Driver-local target UPDATE decision; grants no command or native execution authority. */
private[ranger] final class DeltaRangerAppendTargetReceipt private (
    spark: SparkSession,
    target: DeltaRangerRegistrationBindings.BoundAppendTarget,
    subject: DeltaRangerQuerySubject,
    stamp: DeltaRangerPolicyEngine.SnapshotStamp,
    settings: Map[String, String]
) {
  private val key = new DeltaRangerResourceKey(
    target.observation().catalog(),
    target.observation().database(),
    target.observation().table(),
    null,
    "update",
    Option(target.descriptor().owner).filter(_.nonEmpty).orNull
  )

  def validate(session: SparkSession): Unit = {
    if (!(session eq spark)) denied("session changed")
    requireConfiguration()
    requireSubject()
    target.engine().requireSnapshot(stamp)
    target.requireRetained()
    target.requireLive()
    val before = target.validateCurrent(session)
    target.engine().authorize(subject, key, Instant.now())
    val after = target.validateCurrent(session)
    before.requireSame(after)
    target.requireRetained()
    target.requireLive()
    target.engine().requireSnapshot(stamp)
    requireSubject()
    requireConfiguration()
  }

  override def toString: String = "Delta append target receipt"

  private def requireSubject(): Unit = {
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("subject changed")
  }

  private def requireConfiguration(): Unit =
    if (settings != spark.sessionState.conf.getAllConfs) denied("configuration changed")

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta append target receipt rejected: " + reason)
}

private[ranger] object DeltaRangerAppendTargetReceipt {
  def capture(
      spark: SparkSession,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget
  ): DeltaRangerAppendTargetReceipt = {
    target.requireRetained()
    target.requireLive()
    val subject = DeltaRangerQuerySubject.current(spark)
    val stamp = target.engine().snapshotStamp()
    val settings = spark.sessionState.conf.getAllConfs
    val receipt = new DeltaRangerAppendTargetReceipt(spark, target, subject, stamp, settings)
    receipt.validate(spark)
    receipt
  }
}
