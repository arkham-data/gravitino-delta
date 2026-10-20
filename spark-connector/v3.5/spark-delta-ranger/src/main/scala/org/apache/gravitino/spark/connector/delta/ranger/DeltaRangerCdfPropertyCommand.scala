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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, SetTableProperties}
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, V2CommandExec}

/** One issued driver command for the exact named CDF property operation. */
private[ranger] object DeltaRangerCdfPropertyCommand {
  val property = "delta.enableChangeDataFeed"

  def validateProperties(properties: Map[String, String]): Unit =
    if (properties.keySet != Set(property) || !Set("true", "false")(properties(property)))
      denied("unsupported property change")

  def resolve(
      command: SetTableProperties,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): LogicalPlan = {
    validateProperties(command.properties)
    val resolved = command.table match {
      case table: ResolvedTable if table.getClass == classOf[ResolvedTable] => table
      case _ => denied("unnamed or versioned target")
    }
    val native = resolved.table match {
      case table: DeltaTableV2 => table
      case _                   => denied("unsupported target")
    }
    val id = native.catalogTable.getOrElse(denied("unnamed target")).identifier
    val catalogName =
      DeltaRangerResourceKey.literal(id.catalog.getOrElse(denied("missing catalog")))
    val database = DeltaRangerResourceKey.literal(id.database.getOrElse(denied("missing database")))
    val tableName = DeltaRangerResourceKey.literal(id.table)
    val catalog = catalogs(DeltaRangerReadRule.Scan(null, catalogName, database, tableName, None))
    new Owner(command, resolved, native, catalog, spark).logical
  }

  final class Owner private[DeltaRangerCdfPropertyCommand] (
      original: SetTableProperties,
      resolved: ResolvedTable,
      table: DeltaTableV2,
      catalog: DeltaRangerCatalogSpark35,
      spark: SparkSession
  ) extends DeltaRangerCatalogSpark35.CdfAlterInvocation {
    private val thread = Thread.currentThread()
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val settings = spark.sessionState.conf.getAllConfs.toMap
    private val properties = original.properties.toMap
    private val identifier = resolved.identifier
    private val namespace = identifier.namespace().toVector
    private val name = identifier.name()
    private val plugin = resolved.catalog
    private val catalogName = catalog.name()
    private val target = catalog.registrationBindings().resolveCdfPropertyMutation(table)
    private val engine = target.engine()
    private val stamp = engine.snapshotStamp()
    private val key = new DeltaRangerResourceKey(
      target.observation().catalog(),
      target.observation().database(),
      target.observation().table(),
      null,
      "alter",
      Option(target.descriptor().owner).filter(_.nonEmpty).orNull
    )
    private var used = false
    private var invoking = false
    private var published = false
    private var physical: DeltaRangerCdfPropertyExec = null
    val logical: Carrier = Carrier()(this)
    requireCurrent()
    target.validateCurrent(spark)
    engine.authorize(subject, key)
    requireCurrent()

    def requireCurrent(): Unit = {
      requireContext()
      target.requireRetained()
      target.requirePublicationContext(false, target.table().initialSnapshot.version + 1)
      requireContext()
    }

    def check(candidate: Carrier, session: SparkSession): Unit = {
      if ((candidate ne logical) || (session ne spark) || used)
        denied("foreign or consumed command")
      requireCurrent()
    }

    def plan(candidate: Carrier, session: SparkSession): DeltaRangerCdfPropertyExec = {
      check(candidate, session)
      DeltaRangerAppendInventory.qualify(spark)
      if (physical == null) physical = DeltaRangerCdfPropertyExec()(this)
      physical
    }

    def verify(candidate: DeltaRangerCdfPropertyExec, session: SparkSession): Unit = {
      if ((candidate ne physical) || (session ne spark) || used)
        denied("foreign or consumed physical command")
      requireCurrent()
    }

    def run(candidate: DeltaRangerCdfPropertyExec): Seq[InternalRow] = {
      verify(candidate, spark)
      if (DeltaRangerNativeOperation.current() != null || OptimisticTransaction.getActive.nonEmpty)
        denied("nested native operation")
      target.validateCurrent(spark)
      engine.authorize(subject, key)
      requireCurrent()
      used = true
      invoking = true
      val change = TableChange.setProperty(property, properties(property))
      try DeltaRangerCatalogSpark35.withCdfAlter(this, () => plugin.alterTable(identifier, change))
      finally invoking = false
      Nil
    }

    override def validate(
        candidate: DeltaRangerCatalogSpark35,
        id: Identifier,
        changes: Array[TableChange]
    ): Unit = {
      if (
        !invoking || (candidate ne catalog) || id.namespace().toVector != namespace ||
        id.name() != name || changes.length != 1
      ) denied("unbound catalog call")
      changes.head match {
        case p: TableChange.SetProperty
            if p.property() == property && p.value() == properties(property) =>
        case _ => denied("catalog changes differ")
      }
      requireCurrent()
    }

    override def execute(location: String): Unit = {
      if (!invoking || published || location != target.observation().location())
        denied("native target changed")
      target.validateCurrent(spark)
      engine.authorize(subject, key)
      requireCurrent()
      val mutation =
        new DeltaRangerCdfPropertyMutation(
          target,
          spark,
          properties,
          () => requireCatalog(),
          () => published = true
        )
      mutation.run()
    }

    override def committed(): Boolean = published
    override def toString: String = "Issued CDF property command"

    private def requireContext(): Unit = {
      requireCatalog()
      if (
        (Thread.currentThread() ne thread) || !SparkSession.getActiveSession.exists(_ eq spark) ||
        (table.spark ne spark) || namespace != Vector(target.observation().database()) ||
        name != target.observation().table() || identifier.namespace().toVector != namespace ||
        identifier.name() != name || (resolved.table ne table) || (original.table ne resolved) ||
        original.properties != properties || spark.sessionState.conf.getAllConfs.toMap != settings
      )
        denied("command context changed")
      val current = DeltaRangerQuerySubject.current(spark)
      if (current.user() != subject.user() || current.groups() != subject.groups())
        denied("subject changed")
      engine.requireSnapshot(stamp)
    }

    private def requireCatalog(): Unit =
      if (
        (spark.sessionState.catalogManager.catalog(catalogName) ne plugin) ||
        plugin.name() != catalogName || catalog.name() != catalogName
      ) denied("catalog identity changed")
  }

  final case class Carrier()(@transient private val issued: Owner) extends LeafNode {
    def owner: Owner = if (issued == null) denied("inert command") else issued
    override def output: Seq[Attribute] = Nil
    override def clone(): LogicalPlan = this
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(issued)
    override def jsonFields = super.jsonFields.filterNot(_._1 == "issued")
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("CDF property command rejected: " + reason)
}

private[ranger] final case class DeltaRangerCdfPropertyExec()(
    @transient private val issued: DeltaRangerCdfPropertyCommand.Owner
) extends V2CommandExec
    with LeafV2CommandExec {
  private def owner: DeltaRangerCdfPropertyCommand.Owner =
    if (issued == null || isCanonicalizedPlan)
      throw new SecurityException("Inert CDF property command")
    else issued
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(issued)
  override def jsonFields = super.jsonFields.filterNot(_._1 == "issued")
  override def output: Seq[Attribute] = Nil
  override def clone(): SparkPlan = this
  def verify(spark: SparkSession): Unit = owner.verify(this, spark)
  override def run(): Seq[InternalRow] = owner.run(this)
}
