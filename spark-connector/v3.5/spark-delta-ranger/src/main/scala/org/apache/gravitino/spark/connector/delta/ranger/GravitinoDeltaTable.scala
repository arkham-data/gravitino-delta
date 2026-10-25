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

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}

/** Named, governed Delta table handles for the classic Spark DataFrame MERGE API. */
object GravitinoDeltaTable {

  /** The companion API protocol version, for language bindings. */
  def apiVersion(): Int = 1

  /** Capture a canonical three-part target name and its owning query context. */
  def forName(spark: SparkSession, name: String): GravitinoDeltaTable = {
    require(spark != null, "SparkSession is required")
    require(name != null && name.trim.nonEmpty, "Target name is required")
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(name)
    require(
      parts.size == 3 && parts.forall(_.nonEmpty),
      "Canonical three-part target name required"
    )
    // Delta installs its native analyzer defaults lazily. Complete that initialization before
    // freezing the context, without resolving or reading the target.
    spark.sessionState.analyzer
    new GravitinoDeltaTable(new GravitinoDeltaTable.Context(spark), parts.toVector, None)
  }

  private[ranger] final class Context(val spark: SparkSession) {
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val settings = spark.sessionState.conf.getAllConfs
    def validate(): Unit = {
      if ((SparkSession.active ne spark) || spark.sessionState.conf.getAllConfs != settings)
        throw new SecurityException("Delta MERGE builder session/configuration changed")
      val current = DeltaRangerQuerySubject.current(spark)
      if (current.user() != subject.user() || current.groups() != subject.groups())
        throw new SecurityException("Delta MERGE builder subject changed")
    }
  }
}

/** A name-only target handle; no ordinary target read or write authority is captured. */
final class GravitinoDeltaTable private[ranger] (
    private val context: GravitinoDeltaTable.Context,
    private val name: Vector[String],
    private val targetAlias: Option[String]
) {

  /** Return a handle with an alias parsed by the owning session's identifier parser. */
  def alias(name: String): GravitinoDeltaTable = {
    context.validate()
    require(name != null && name.trim.nonEmpty, "Target alias is required")
    val parts = context.spark.sessionState.sqlParser.parseMultipartIdentifier(name)
    require(parts.size == 1 && parts.head.nonEmpty, "One target alias identifier required")
    new GravitinoDeltaTable(context, this.name, Some(parts.head))
  }

  /** Begin a MERGE with an expression string and the actual supplied source occurrence. */
  def merge(source: DataFrame, condition: String): GravitinoDeltaMergeBuilder =
    builder(source, condition)

  /** Begin a MERGE retaining the Column's Catalyst expression tree. */
  def merge(source: DataFrame, condition: Column): GravitinoDeltaMergeBuilder =
    builder(source, condition)

  private def builder(source: DataFrame, condition: Any): GravitinoDeltaMergeBuilder = {
    context.validate()
    require(source != null, "Source DataFrame is required")
    if ((source.sparkSession ne context.spark) || source.isStreaming)
      throw new SecurityException("Delta MERGE requires a same-session batch source")
    val analyzed = source.queryExecution.analyzed
    DeltaRangerCdfRead
      .pending(analyzed, context.spark)
      .getOrElse(
        throw new SecurityException("Delta MERGE requires one fresh named bounded CDF source")
      )
    val expressions = new DeltaRangerMergeBuilderExpressions(context.spark, source, analyzed)
    val target: LogicalPlan = UnresolvedRelation(name)
    new GravitinoDeltaMergeBuilder(
      context,
      targetAlias.map(SubqueryAlias(_, target)).getOrElse(target),
      analyzed,
      expressions,
      expressions.expression(condition)
    )
  }
}
