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

/** Bounded named CDF request values never carry execution authority. */
private[ranger] object DeltaRangerCdfRead {
  final case class Request(start: Long, end: Long)

  def parse(options: Map[String, String], observedHead: Long): Request = {
    val grouped = options.toVector.groupBy(_._1.toLowerCase(Locale.ROOT))
    if (grouped.exists(_._2.size != 1)) denied("conflicting options")
    val normalized = grouped.map { case (key, entries) => key -> entries.head._2 }
    if (
      normalized.keySet != Set("readchangefeed", "startingversion", "endingversion") ||
      normalized("readchangefeed") != "true"
    ) denied("unsupported or missing options")
    def version(key: String): Long = {
      val text = normalized(key)
      if (text == null || !text.matches("[0-9]+")) denied("integer bounds required")
      try text.toLong
      catch { case _: NumberFormatException => denied("version overflow") }
    }
    val request = Request(version("startingversion"), version("endingversion"))
    if (request.start > request.end || request.end > observedHead) denied("invalid interval")
    request
  }

  final class Source private[DeltaRangerCdfRead] (
      val binding: DeltaRangerRegistrationBindings.BoundCdfRead,
      val request: Request,
      val spark: org.apache.spark.sql.SparkSession,
      originalOutput: Seq[org.apache.spark.sql.catalyst.expressions.AttributeReference]
  ) extends org.apache.spark.sql.sources.BaseRelation {
    override def sqlContext: org.apache.spark.sql.SQLContext = spark.sqlContext
    override val schema: org.apache.spark.sql.types.StructType =
      org.apache.spark.sql.delta.commands.cdc.CDCReader
        .cdcReadSchema(binding.anchor().index().schema)
    private val relation = org.apache.spark.sql.execution.datasources.LogicalRelation(this)
    val logical = relation.copy(output = relation.output.map { attribute =>
      originalOutput.find(_.name == attribute.name).getOrElse(attribute)
    })
    private val observations = new java.util.IdentityHashMap[
      org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      DeltaRangerCdfReadReceipt.Observation
    ]()
    private var consumed = false
    def observe(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): Unit =
      synchronized {
        if (consumed) denied("derived or reused CDF source")
        if (!observations.containsKey(plan))
          observations.put(plan, new DeltaRangerCdfReadReceipt.Observation(this, plan, spark))
      }
    def observation(
        plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    ): DeltaRangerCdfReadReceipt.Observation = synchronized {
      if (consumed) denied("reused CDF source")
      val observation =
        Option(observations.get(plan)).getOrElse(denied("missing exact analyzed CDF observation"))
      consumed = true
      observation
    }
    def check(
        relation: org.apache.spark.sql.execution.datasources.LogicalRelation,
        session: org.apache.spark.sql.SparkSession
    ): Unit = {
      if ((relation ne logical) || (spark ne session) || relation.isStreaming)
        denied("copied CDF source")
      binding.anchor().requireRetained()
    }
  }

  def resolve(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: org.apache.spark.sql.SparkSession
  ): Option[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = resolve(
    plan,
    spark,
    scan =>
      spark.sessionState.catalogManager.catalog(scan.catalog) match {
        case catalog: DeltaRangerCatalogSpark35 => catalog
        case _ => denied("CDF source does not belong to a Ranger-enabled catalog")
      }
  )

  def resolve(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: org.apache.spark.sql.SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): Option[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan] = {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
    import org.apache.spark.sql.catalyst.expressions.Literal
    import org.apache.spark.sql.delta.{CDCNameBased, CDCPathBased}
    import org.apache.spark.sql.types.{ByteType, ShortType, IntegerType, LongType, StringType}
    import org.apache.spark.sql.util.CaseInsensitiveStringMap
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
    // Capture the named function before Delta lowers it to its stock CDF relation. The
    // unresolved named load then follows exactly the DataFrame binding and receipt route.
    val functions = plan.collect {
      case named: CDCNameBased => named
      case _: CDCPathBased     => denied("path CDF function")
    }
    if (functions.nonEmpty) {
      if (functions.size != 1 || plan.isStreaming) denied("unsupported CDF source shape")
      val function = functions.head
      if (function.functionArgs.size != 3) denied("explicit CDF bounds required")
      val name = function.functionArgs.head match {
        case Literal(value, StringType) if value != null => value.toString
        case _ => denied("literal named CDF source required")
      }
      val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(name)
      if (parts.size != 3 || parts.exists(p => p.exists(c => c == '/' || c == '\\' || c == ':')))
        denied("fully qualified named CDF source required")
      parts.foreach(DeltaRangerResourceKey.literal)
      val bounds = function.functionArgs.tail.map {
        case Literal(value, ByteType | ShortType | IntegerType | LongType) if value != null =>
          value.toString
        case _ => denied("integer literal CDF bounds required")
      }
      val options = Map(
        "readChangeFeed" -> "true",
        "startingVersion" -> bounds.head,
        "endingVersion" -> bounds.last
      )
      parse(options, Long.MaxValue)
      return Some(plan.transformDown {
        case named: CDCNameBased if named eq function =>
          UnresolvedRelation(
            parts,
            new CaseInsensitiveStringMap(options.asJava),
            isStreaming = false
          )
      })
    }
    import org.apache.spark.sql.delta.catalog.DeltaTableV2
    val sources = plan.collect {
      case relation: DataSourceV2Relation
          if relation.options
            .asCaseSensitiveMap()
            .keySet()
            .asScala
            .exists(_.equalsIgnoreCase("readChangeFeed")) =>
        relation
    }
    if (sources.isEmpty) return None
    if (sources.size != 1 || plan.isStreaming) denied("unsupported CDF source shape")
    val source = sources.head
    val native = source.table match {
      case value: DeltaTableV2 => value
      case _                   => denied("unsupported CDF table")
    }
    val id = native.catalogTable.getOrElse(denied("unnamed CDF source")).identifier
    val catalog = catalogs(
      DeltaRangerReadRule.Scan(
        null,
        id.catalog.getOrElse(denied("missing catalog")),
        id.database.getOrElse(denied("missing database")),
        id.table,
        None
      )
    )
    val bound = catalog.registrationBindings().bindCdfRead(native)
    val request =
      parse(source.options.asCaseSensitiveMap().asScala.toMap, native.initialSnapshot.version)
    val pending = new Source(bound, request, spark, source.output)
    Some(plan.transformDown {
      case relation: DataSourceV2Relation if relation eq source => pending.logical
    })
  }

  def pending(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: org.apache.spark.sql.SparkSession
  ): Option[Source] = {
    import org.apache.spark.sql.catalyst.plans.logical.{Project, Filter, SubqueryAlias}
    import org.apache.spark.sql.execution.datasources.LogicalRelation
    val sources = plan.collect {
      case relation: LogicalRelation if relation.relation.isInstanceOf[Source] =>
        val source = relation.relation.asInstanceOf[Source]
        source.check(relation, spark)
        source
    }
    if (sources.isEmpty) return None
    if (sources.size != 1 || plan.isStreaming) denied("multiple or streaming CDF sources")
    plan.foreach {
      case _: Project | _: Filter | _: SubqueryAlias                     =>
      case relation: LogicalRelation if relation eq sources.head.logical =>
      case _ => denied("unsupported CDF query shape")
    }
    if (plan.resolved) plan.foreach {
      case _: LogicalRelation =>
      case node               => node.expressions.foreach(DeltaRangerPlanAdmission.expression)
    }
    Some(sources.head)
  }

  def normalize(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
      spark: org.apache.spark.sql.SparkSession
  ): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = {
    DeltaRangerCdfReadScans.normalize(plan, spark).foreach(result => return result)
    pending(plan, spark)
      .map { source =>
        if (spark.sessionState.conf.adaptiveExecutionEnabled) denied("CDF AQE is not qualified")
        if (spark.sharedState.cacheManager.lookupCachedData(plan).nonEmpty)
          denied("CDF cache substitution")
        DeltaRangerPhysicalPlanning.qualify(spark)
        DeltaRangerCdfReadReceipt.issue(source, plan, spark).carrier
      }
      .getOrElse(plan)
  }

  final case class Carrier(
      override val output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute]
  )(
      @transient private val authority: DeltaRangerCdfReadReceipt
  ) extends org.apache.spark.sql.catalyst.plans.logical.LeafNode {
    def receipt: DeltaRangerCdfReadReceipt = {
      if (authority == null || isCanonicalizedPlan) denied("inert CDF carrier")
      authority.requireCarrier(this)
      authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan = this
  }

  private[ranger] def denied(reason: String): Nothing =
    throw new SecurityException("Delta CDF read rejected: " + reason)
}
