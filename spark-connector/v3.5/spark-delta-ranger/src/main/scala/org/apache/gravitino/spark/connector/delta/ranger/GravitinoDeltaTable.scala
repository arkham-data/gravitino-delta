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

import javax.annotation.Nullable
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateTable,
  LogicalPlan,
  SubqueryAlias,
  TableSpec
}
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.types.{DataType, StructType}

/** Named, governed Delta schema creation and classic Spark DataFrame MERGE handles. */
object GravitinoDeltaTable {

  /** The companion API protocol version, for language bindings. */
  def apiVersion(): Int = 1

  /** Return detached visible native metadata, or null only for authorized typed table absence.
    *
    * @param spark
    *   the exact active classic session
    * @param name
    *   canonical three-part registered table name
    * @return
    *   JSON containing schemaJson, properties and partitionColumns, or null for absence
    * @throws SecurityException
    *   if visibility, registration identity, session or lease is invalid
    */
  @Nullable
  def metadataJson(spark: SparkSession, name: String): String =
    DeltaRangerTableMetadata.read(spark, name)

  /** Create an empty named Delta table through normal analysis and the issued CREATE command.
    *
    * The schema and collections are copied before analysis. Partition entries are literal top-level
    * column names. Completion never performs a final table read or returns a handle.
    */
  def create(
      spark: SparkSession,
      name: String,
      schema: StructType,
      partitionBy: java.util.List[String],
      properties: java.util.Map[String, String],
      @Nullable comment: String,
      ifNotExists: Boolean
  ): Unit = {
    require(spark != null, "SparkSession is required")
    require(name != null && name.trim.nonEmpty, "Target name is required")
    require(schema != null, "StructType schema is required")
    require(partitionBy != null && properties != null, "CREATE collections are required")
    if (!SparkSession.getActiveSession.exists(_ eq spark))
      throw new SecurityException("Schema CREATE requires the active owning session")
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(name).toVector
    require(
      parts.size == 3 && parts.forall(_.nonEmpty),
      "Canonical three-part target name required"
    )
    val copiedSchema = DataType.fromJson(schema.json).asInstanceOf[StructType]
    val copiedPartitions = partitionBy.asScala.toVector.map { column =>
      require(column != null && column.trim.nonEmpty, "Partition column name is required")
      Expressions.identity(QuotingUtils.quoteIdentifier(column))
    }
    val copiedProperties = properties.asScala.toMap
    require(
      copiedProperties.forall { case (key, value) =>
        key != null && key.trim.nonEmpty && value != null
      },
      "String CREATE properties are required"
    )
    val command = CreateTable(
      UnresolvedIdentifier(parts),
      copiedSchema,
      copiedPartitions,
      TableSpec(copiedProperties, Some("delta"), Map.empty, None, Option(comment), None, false),
      ifNotExists
    )
    val execution = spark.sessionState.executePlan(command)
    execution.assertAnalyzed()
    execution.analyzed match {
      case owned: DeltaRangerCreateCommand.Carrier => owned.owner.check(owned, spark)
      case _ => throw new SecurityException("Schema CREATE requires an issued owned command")
    }
    // The issued CREATE carrier is a leaf, so commandExecuted alone is not an action.
    execution.executedPlan.executeCollect()
    ()
  }

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
