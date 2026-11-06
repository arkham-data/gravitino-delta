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

import java.util.{IdentityHashMap, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.gravitino.spark.connector.jdbc.SparkJdbcTable
import org.apache.kyuubi.plugin.spark.authz.{OperationType, PrivilegeObjectType, PrivilegesBuilder}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogStorageFormat}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation
}

/** Finite original source inventory; JDBC never supplies a Delta storage binding. */
private[ranger] object DeltaRangerInputSources {
  sealed trait Source {
    def relation: LogicalPlan
    def catalog: String
    def database: String
    def table: String
    def owner: Option[String]
    def engine: DeltaRangerPolicyEngine
    def requireLive(): Unit
    def requireRetained(): Unit
    def recheck(): Unit
    def recheckMaintenance(scope: DeltaRangerCdfCacheMaintenance.Scope): Unit
    def observe(session: SparkSession): () => Unit
  }

  final class NamedDelta(
      val scan: DeltaRangerReadRule.Scan,
      val binding: DeltaRangerRegistrationBindings.BoundRegistrationScan
  ) extends Source {
    override def relation: LogicalRelation = scan.relation
    override def catalog: String = scan.catalog
    override def database: String = scan.database
    override def table: String = scan.table
    override def owner: Option[String] = scan.owner
    override def engine: DeltaRangerPolicyEngine = binding.engine()
    override def requireLive(): Unit = binding.requireLive()
    override def requireRetained(): Unit = binding.requireRetained()
    override def recheck(): Unit = binding.recheckPublication()
    override def recheckMaintenance(scope: DeltaRangerCdfCacheMaintenance.Scope): Unit = {
      scope.requireParticipant(this)
      binding.recheckPublication()
      scope.requireParticipant(this)
    }
    override def observe(session: SparkSession): () => Unit = {
      val before = binding.validateCurrent(session)
      () => before.requireSame(binding.validateCurrent(session))
    }
  }

  final class RegisteredJdbc(
      val relation: DataSourceV2Relation,
      val binding: DeltaRangerJdbcSourceBindings.BoundJdbc
  ) extends Source {
    override def catalog: String = binding.catalog()
    override def database: String = binding.identifier().namespace()(0)
    override def table: String = binding.identifier().name()
    override def owner: Option[String] =
      Option(relation.table.properties().get("owner")).filter(_.nonEmpty)
    override def engine: DeltaRangerPolicyEngine = binding.engine()
    override def requireLive(): Unit = binding.requireLive()
    override def requireRetained(): Unit = binding.requireRetained()
    override def recheck(): Unit = binding.recheckPublication()
    override def recheckMaintenance(scope: DeltaRangerCdfCacheMaintenance.Scope): Unit = {
      scope.requireParticipant(this)
      binding.validateMetadata(scope.spark)
      scope.requireParticipant(this)
    }
    override def observe(session: SparkSession): () => Unit = {
      val before = binding.validateCurrent(session)
      () => before.requireSame(binding.validateCurrent(session))
    }
  }

  def jdbc(relation: DataSourceV2Relation, session: SparkSession): RegisteredJdbc =
    resolveJdbc(relation, session, current = true)

  def retainedJdbc(relation: DataSourceV2Relation, session: SparkSession): RegisteredJdbc =
    resolveJdbc(relation, session, current = false)

  private def resolveJdbc(
      relation: DataSourceV2Relation,
      session: SparkSession,
      current: Boolean
  ): RegisteredJdbc = {
    if (
      relation.getClass != classOf[
        DataSourceV2Relation
      ] || relation.isStreaming || !relation.options.isEmpty
    ) denied()
    val catalog = relation.catalog match {
      case Some(value: DeltaRangerPostgreSqlCatalogSpark35) => value
      case _                                                => denied()
    }
    val table = relation.table match {
      case value: SparkJdbcTable if value.getClass == classOf[SparkJdbcTable] => value
      case _                                                                  => denied()
    }
    val id = relation.identifier.getOrElse(denied())
    val bound =
      if (current) catalog.sourceBindings().resolve(table, id, session)
      else catalog.sourceBindings().resolveRetained(table, id, session)
    val fields = table.schema().fields
    if (
      relation.output.size != fields.length || !relation.output.zip(fields).forall {
        case (attribute, field) =>
          attribute.name == field.name && attribute.dataType == field.dataType && attribute.nullable == field.nullable && attribute.metadata == field.metadata
      }
    ) denied()
    new RegisteredJdbc(relation, bound)
  }

  def leaf(plan: LogicalPlan): Boolean =
    plan.isInstanceOf[LogicalRelation] || plan.isInstanceOf[DataSourceV2Relation]

  def leaves(plan: LogicalPlan): Vector[LogicalPlan] = plan.collect {
    case relation: LogicalRelation      => relation
    case relation: DataSourceV2Relation => relation
    case scan: DataSourceV2ScanRelation => scan.relation
  }.toVector

  def inventory(
      plan: LogicalPlan,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): Vector[Source] =
    plan.collect {
      case relation: LogicalRelation =>
        val scan = DeltaRangerReadRule.canonical(relation)
        new NamedDelta(scan, catalogs(scan).registrationBindings().resolve(relation))
      case relation: DataSourceV2Relation => jdbc(relation, spark)
    }.toVector

  def extract(
      plan: LogicalPlan,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): Vector[(Source, Set[String])] = {
    DeltaRangerOrdinaryAdmission.query(plan)
    if (!plan.exists(_.isInstanceOf[DataSourceV2Relation]))
      return DeltaRangerReadRule
        .extract(plan, spark)
        .map { case (scan, columns) =>
          new NamedDelta(
            scan,
            catalogs(scan).registrationBindings().resolve(scan.relation)
          ) -> columns
        }
        .toVector
    val sources = inventory(plan, spark, catalogs)
    if (sources.isEmpty) denied()
    val byRelation = new IdentityHashMap[LogicalPlan, Source]()
    sources.foreach(source => byRelation.put(source.relation, source))
    val tokens = mutable.Map.empty[String, Source]
    // These copies are passed only to Kyuubi demand extraction and cannot become executable authority.
    val extraction = plan.transformUp {
      case relation if leaf(relation) =>
        val source = byRelation.get(relation)
        if (source == null) denied()
        val token = "source_" + UUID.randomUUID().toString.replace("-", "")
        tokens.put(token, source)
        relation match {
          case delta: LogicalRelation =>
            val descriptor = delta.catalogTable.get
            delta.copy(catalogTable =
              Some(descriptor.copy(identifier = descriptor.identifier.copy(table = token)))
            )
          case jdbc: DataSourceV2Relation =>
            // Kyuubi obtains JDBC names from SparkJdbcTable rather than the V2 identifier.
            // An inert metadata-only relation makes the per-occurrence token authoritative only
            // for extraction; it has no read interface and never leaves this local copy.
            val columns = jdbc.schema
            val backing = new BaseRelation {
              override def sqlContext: SQLContext = spark.sqlContext
              override def schema: StructType = columns
            }
            val descriptor = CatalogTable(
              identifier = TableIdentifier(token, Some(source.database), Some(source.catalog)),
              tableType = CatalogTableType.EXTERNAL,
              storage = CatalogStorageFormat.empty,
              schema = columns,
              provider = Some("jdbc"),
              properties = source.owner.map(value => Map("owner" -> value)).getOrElse(Map.empty)
            )
            LogicalRelation(backing, jdbc.output, Some(descriptor), isStreaming = false)
        }
    }
    val (inputs, outputs, operation) = PrivilegesBuilder.build(extraction, spark)
    if (outputs.nonEmpty || operation != OperationType.QUERY || inputs.size > sources.size) denied()
    val used = plan.output.map(_.exprId).toSet ++ plan.collect {
      case distinct: Distinct  => distinct.child.output.map(_.exprId)
      case node if !leaf(node) => node.expressions.flatMap(_.references.map(_.exprId))
    }.flatten
    def demanded(source: Source): Set[String] =
      source.relation.output.filter(a => used(a.exprId)).map(_.name).toSet
    val seen = mutable.Set.empty[String]
    val found = inputs.toVector.map { input =>
      val source = tokens.getOrElse(input.objectName, denied())
      if (
        input.privilegeObjectType != PrivilegeObjectType.TABLE_OR_VIEW || !seen.add(
          input.objectName
        ) || input.dbname != source.database || input.owner != source.owner || input.catalog.exists(
          _ != source.catalog
        ) || input.columns.toSet != demanded(source)
      ) denied()
      source -> input.columns.toSet
    }
    found ++ tokens.toVector.filterNot(pair => seen(pair._1)).map { case (_, source) =>
      if (demanded(source).nonEmpty) denied()
      source -> Set.empty[String]
    }
  }

  private def denied(): Nothing = throw new SecurityException(
    "Unissued or unsupported registered JDBC source"
  )
}
