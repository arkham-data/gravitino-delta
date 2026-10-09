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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/** Driver-local inert association of a supplied command, registered target and receipted source.
  * This is neither proof of command origin nor execution authority. Validation makes separate
  * observations of each participant, not an atomic policy, registration or native-state fence.
  */
private[ranger] final class DeltaRangerAppendCommandBinding private (
    spark: SparkSession,
    command: AppendData,
    catalog: DeltaRangerCatalogSpark35,
    shape: DeltaRangerAppendCommandBinding.Shape,
    target: DeltaRangerRegistrationBindings.BoundAppendTarget,
    targetReceipt: DeltaRangerAppendTargetReceipt,
    sourceReceipt: DeltaRangerDecisionReceipt,
    context: DeltaRangerAppendCommandBinding.Context
) {
  import DeltaRangerAppendCommandBinding._

  def validate(candidate: AppendData, session: SparkSession): Unit = {
    requireIdle()
    context.requireCurrent(session)
    shape.requireSame(candidate, session)
    target.requireRetained()
    target.requireLive()
    sourceReceipt.consume(command.query, session, optimized = false)
    targetReceipt.validate(session)
    sourceReceipt.validateAction(session)
    targetReceipt.validate(session)
    sourceReceipt.revalidate(() => shape.requireSame(candidate, session))
    target.requireRetained()
    target.requireLive()
    sourceReceipt.obligations.foreach { obligation =>
      obligation.binding.requireRetained()
      obligation.binding.requireLive()
    }
    shape.requireSame(candidate, session)
    context.requireCurrent(session)
    requireIdle()
  }

  override def toString: String = "Checked Delta append association"
}

private[ranger] object DeltaRangerAppendCommandBinding {
  def capture(
      spark: SparkSession,
      command: AppendData,
      catalog: DeltaRangerCatalogSpark35
  ): DeltaRangerAppendCommandBinding = {
    requireIdle()
    if (spark == null || command == null || catalog == null) denied("missing input")
    val shape = shapeOf(command, spark, catalog)
    val subject = DeltaRangerQuerySubject.current(spark)
    val settings = spark.sessionState.conf.getAllConfs.toMap
    val target = catalog.registrationBindings().resolveAppendTarget(shape.table)
    if (
      !(target.table() eq shape.table) || !(target.descriptor() eq shape.descriptor) ||
      target.observation().catalog() != shape.catalogName ||
      target.observation().database() != shape.database ||
      target.observation().table() != shape.tableName
    ) denied("target association changed")
    val sourceReceipt =
      DeltaRangerDecisionReceipt.consumeOwned(command.query, spark, optimized = false)
    if (sourceReceipt.obligations.isEmpty) denied("missing original source obligations")
    val engines = (Vector(target.engine()) ++ sourceReceipt.obligations.map(_.binding.engine()))
      .foldLeft(Vector.empty[DeltaRangerPolicyEngine]) { (all, engine) =>
        if (all.exists(_ eq engine)) all else all :+ engine
      }
    val context = new Context(spark, subject, settings, engines.map(e => e -> e.snapshotStamp()))
    context.requireCurrent(spark)
    shape.requireSame(command, spark)
    val targetReceipt = DeltaRangerAppendTargetReceipt.capture(spark, target)
    val association = new DeltaRangerAppendCommandBinding(
      spark,
      command,
      catalog,
      shape,
      target,
      targetReceipt,
      sourceReceipt,
      context
    )
    association.validate(command, spark)
    association
  }

  private final class Context(
      spark: SparkSession,
      subject: DeltaRangerQuerySubject,
      settings: Map[String, String],
      engines: Vector[(DeltaRangerPolicyEngine, DeltaRangerPolicyEngine.SnapshotStamp)]
  ) {
    def requireCurrent(session: SparkSession): Unit = {
      if (!(session eq spark)) denied("session changed")
      val current = DeltaRangerQuerySubject.current(session)
      if (current.user() != subject.user() || current.groups() != subject.groups())
        denied("subject changed")
      if (session.sessionState.conf.getAllConfs != settings) denied("configuration changed")
      engines.foreach { case (engine, stamp) => engine.requireSnapshot(stamp) }
    }
  }

  private final class Shape(
      original: AppendData,
      relation: DataSourceV2Relation,
      val table: DeltaTableV2,
      query: LogicalPlan,
      plugin: CatalogPlugin,
      identifier: Identifier,
      val descriptor: CatalogTable,
      catalog: DeltaRangerCatalogSpark35,
      val catalogName: String,
      val database: String,
      val tableName: String,
      snapshot: Vector[Any]
  ) {
    def requireSame(candidate: AppendData, session: SparkSession): Unit = {
      if (!(candidate eq original)) denied("command identity changed")
      if (
        !(candidate.table eq relation) || !(candidate.query eq query) ||
        !(relation.table eq table) || !relation.catalog.exists(_ eq plugin) ||
        !relation.identifier.exists(_ eq identifier) || !table.catalogTable.exists(_ eq descriptor)
      )
        denied("input identity changed")
      val current = shapeOf(candidate, session, catalog)
      if (snapshot != current.snapshotValue) denied("command shape changed")
    }
    private def snapshotValue: Vector[Any] = snapshot
  }

  private def shapeOf(
      command: AppendData,
      spark: SparkSession,
      catalog: DeltaRangerCatalogSpark35
  ): Shape = {
    if (
      command.query == null || command.table == null || command.write == null ||
      command.analyzedQuery == null || command.writeOptions == null ||
      command.getClass != classOf[AppendData] || !command.resolved ||
      command.isStreaming || !command.query.resolved || command.query.isStreaming ||
      command.write.nonEmpty || command.analyzedQuery.nonEmpty || command.writeOptions.nonEmpty
    )
      denied("unqualified command stage")
    val relation = command.table match {
      case r: DataSourceV2Relation if r.getClass == classOf[DataSourceV2Relation] => r
      case _ => denied("unqualified target relation")
    }
    if (
      relation.catalog == null || relation.identifier == null || relation.options == null ||
      relation.catalog.isEmpty || relation.identifier.isEmpty || !relation.options.isEmpty
    )
      denied("unqualified named target")
    val table = relation.table match {
      case t: DeltaTableV2 => t
      case _               => denied("unqualified native target")
    }
    if (!(table.spark eq spark)) denied("target session changed")
    val descriptor = table.catalogTable.getOrElse(denied("unnamed target"))
    val descriptorId = descriptor.identifier
    val catalogName = DeltaRangerResourceKey.literal(
      descriptorId.catalog.getOrElse(denied("missing target catalog"))
    )
    val database = DeltaRangerResourceKey.literal(
      descriptorId.database.getOrElse(denied("missing target database"))
    )
    val tableName = DeltaRangerResourceKey.literal(descriptorId.table)
    val plugin = relation.catalog.get
    val identifier = relation.identifier.get
    if (
      plugin == null || identifier == null || identifier.namespace() == null ||
      catalog.name() != catalogName || plugin.name() != catalogName ||
      !(spark.sessionState.catalogManager.catalog(catalogName) eq plugin) ||
      identifier.namespace().toVector != Vector(database) || identifier.name() != tableName
    )
      denied("target catalog association changed")
    val plans = Vector(command, command.query, relation)
    val snapshot = Vector[Any](
      command.isByName,
      command.writeOptions.toMap,
      relation.options.asCaseSensitiveMap().asScala.toMap,
      command.origin,
      identifier.namespace().toVector,
      identifier.name(),
      catalogName,
      database,
      tableName,
      plans.map(p => Vector(p.resolved, p.isStreaming, p.schema.json, attributes(p.output)))
    )
    new Shape(
      command,
      relation,
      table,
      command.query,
      plugin,
      identifier,
      descriptor,
      catalog,
      catalogName,
      database,
      tableName,
      snapshot
    )
  }

  private def attributes(output: Seq[Attribute]): Vector[Vector[Any]] =
    output.map {
      case a: AttributeReference if a.getClass == classOf[AttributeReference] =>
        Vector(
          a.getClass,
          a.name,
          a.exprId.id,
          a.exprId.jvmId,
          a.qualifier.toVector,
          a.dataType.json,
          a.nullable,
          a.metadata.json
        )
      case _ => denied("unqualified output attribute")
    }.toVector

  private def requireIdle(): Unit = {
    if (DeltaRangerNativeOperation.current() != null) denied("active native scope")
    if (OptimisticTransaction.getActive.nonEmpty) denied("active native transaction")
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta append association rejected: " + reason)
}
