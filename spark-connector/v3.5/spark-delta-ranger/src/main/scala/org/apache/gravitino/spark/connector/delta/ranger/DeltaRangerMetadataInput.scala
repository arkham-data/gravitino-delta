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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.DeltaRangerExternalJdbcNative.{Relation => JDBCRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Exact source participation in one finite schema CREATE or CDF metadata operation. */
private[ranger] final class DeltaRangerMetadataInput private (
    spark: SparkSession,
    source: DataFrame
) {
  private val thread = Thread.currentThread()
  private val subject = DeltaRangerQuerySubject.current(spark)
  private val plan = source.queryExecution.analyzed
  val beforeSettings: Map[String, String] = spark.sessionState.conf.getAllConfs
  DeltaRangerOrdinaryRead.check(plan, spark)
  private val occurrences = plan.collect {
    case relation: LogicalRelation if relation.relation.isInstanceOf[JDBCRelation] => relation
  }.toVector
  private val participants = occurrences.map { relation =>
    DeltaRangerExternalJdbcSources.resolve(relation.relation.asInstanceOf[JDBCRelation], spark)
  }.distinct
  if (participants.isEmpty) denied()
  private var used = false

  def requireLive(): Unit = {
    requireContext()
    if (used || spark.sessionState.conf.getAllConfs != beforeSettings) denied()
    DeltaRangerOrdinaryRead.check(plan, spark)
    participants.foreach(_.requireCurrent(spark))
  }

  def complete(mutation: DeltaRangerCreateMutation): Unit =
    complete(new Publication(Left(mutation), 0L))

  def complete(mutation: DeltaRangerCdfPropertyMutation): Unit =
    complete(new Publication(Right(mutation), mutation.publishedVersion))

  private def complete(witness: Publication): Unit = {
    if (used) denied()
    used = true
    witness.active = true
    try {
      DeltaRangerExternalJdbcSources.advanceMetadataPublication(witness, participants)
    } finally witness.active = false
  }

  /** Inert outside the exact owner's successful completion, with no caller-supplied callback. */
  final class Publication private[DeltaRangerMetadataInput] (
      mutation: Either[DeltaRangerCreateMutation, DeltaRangerCdfPropertyMutation],
      version: Long
  ) {
    private[DeltaRangerMetadataInput] var active = false
    def beforeSettings: Map[String, String] = DeltaRangerMetadataInput.this.beforeSettings
    def afterSettings: Map[String, String] =
      beforeSettings.updated("spark.databricks.delta.lastCommitVersionInSession", version.toString)
    def requireParticipant(candidate: DeltaRangerExternalJdbcSources.BoundInput): Unit = {
      requireContext()
      if (!active || !participants.exists(_ eq candidate)) denied()
      mutation match {
        case Left(create) => create.requireExternalPublication(DeltaRangerMetadataInput.this)
        case Right(cdf)   => cdf.requireExternalPublication(DeltaRangerMetadataInput.this)
      }
      if (spark.sessionState.conf.getAllConfs != afterSettings) denied()
    }
  }

  private def requireContext(): Unit = {
    if (
      (Thread.currentThread() ne thread) || !SparkSession.getActiveSession.exists(_ eq spark) ||
      (source.sparkSession ne spark) || (source.queryExecution.analyzed ne plan)
    ) denied()
    val actual = plan.collect {
      case relation: LogicalRelation if relation.relation.isInstanceOf[JDBCRelation] => relation
    }.toVector
    if (
      actual.size != occurrences.size || !actual.zip(occurrences).forall { case (a, b) => a eq b }
    )
      denied()
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups()) denied()
  }
  private def denied(): Nothing = throw new SecurityException(
    "Metadata operation JDBC input changed or unowned"
  )
}

private[ranger] object DeltaRangerMetadataInput {
  def capture(spark: SparkSession, source: DataFrame): DeltaRangerMetadataInput = {
    require(spark != null && source != null, "Session and source DataFrame are required")
    if (
      (source.sparkSession ne spark) || !SparkSession.getActiveSession.exists(
        _ eq spark
      ) || source.isStreaming
    )
      throw new SecurityException("Metadata operation requires a same-session batch JDBC source")
    val input = new DeltaRangerMetadataInput(spark, source)
    input.requireLive()
    input
  }
}
