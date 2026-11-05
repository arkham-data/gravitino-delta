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
import org.apache.gravitino.spark.connector.delta.DeltaCreateLifecycle
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, LeafNode, LogicalPlan, TableSpec}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, V2CommandExec}
import org.apache.spark.sql.types.{DataType, LongType, StructType}

/** One privately issued initial named schema CREATE, without a final SELECT observation. */
private[ranger] object DeltaRangerCreateCommand {
  private val properties = Map(
    "delta.compatibility.symlinkFormatManifest.enabled" -> "true",
    "delta.deletedFileRetentionDuration" -> "interval 364 days",
    "delta.enableChangeDataFeed" -> "true",
    "delta.logRetentionDuration" -> "interval 45 days"
  )
  private val settings = Map(
    "spark.databricks.delta.optimizeWrite.enabled" -> "true",
    "spark.databricks.delta.autoCompact.enabled" -> "true",
    "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "true",
    "spark.databricks.delta.snapshotPartitions" -> "2",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "false",
    "spark.sql.sources.partitionOverwriteMode" -> "static",
    "spark.databricks.delta.autoCompact.minNumFiles" -> "3",
    "spark.databricks.delta.autoCompact.maxFileSize" -> "1048576",
    "spark.databricks.delta.autoCompact.minFileSize" -> "524288",
    "spark.databricks.delta.optimizeWrite.binSize" -> "1",
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "2"
  )
  private val schema = new StructType().add("id", LongType, nullable = true)

  def resolve(
      command: CreateTable,
      spark: SparkSession,
      catalogs: DeltaRangerReadRule.Scan => DeltaRangerCatalogSpark35
  ): LogicalPlan = {
    val resolved = command.name match {
      case value: ResolvedIdentifier if value.getClass == classOf[ResolvedIdentifier] => value
      case _ => denied("unnamed target")
    }
    val namespace = resolved.identifier.namespace().toVector
    if (namespace.size != 1) denied("unsupported namespace")
    val catalogName = DeltaRangerResourceKey.literal(resolved.catalog.name())
    val database = DeltaRangerResourceKey.literal(namespace.head)
    val table = DeltaRangerResourceKey.literal(resolved.identifier.name())
    val catalog = catalogs(DeltaRangerReadRule.Scan(null, catalogName, database, table, None))
    if (catalog.name() != catalogName) denied("foreign catalog")
    val specification = command.tableSpec match {
      case value: TableSpec if value.getClass == classOf[TableSpec] => value
      case _ => denied("unsupported table specification")
    }
    if (
      command.ignoreIfExists || command.partitioning.nonEmpty ||
      command.tableSchema != schema || specification.properties != properties ||
      specification.provider != Some("delta") || specification.options.nonEmpty ||
      specification.location.nonEmpty || specification.comment.nonEmpty ||
      specification.serde.nonEmpty || specification.external
    )
      denied("unsupported declaration")
    if (settings.exists { case (key, value) => !spark.conf.getOption(key).contains(value) })
      denied("unsupported initial CREATE settings")
    new Owner(
      command,
      resolved,
      catalog,
      spark,
      DeltaRangerCatalogSpark35.convertCreateProperties(specification).asScala.toMap
    ).logical
  }

  final class Owner private[DeltaRangerCreateCommand] (
      original: CreateTable,
      resolved: ResolvedIdentifier,
      catalog: DeltaRangerCatalogSpark35,
      spark: SparkSession,
      frozenProperties: Map[String, String]
  ) extends DeltaRangerCatalogSpark35.CreateInvocation {
    private val thread = Thread.currentThread()
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val settings = spark.sessionState.conf.getAllConfs.toMap
    private val namespace = resolved.identifier.namespace().toVector
    private val name = resolved.identifier.name()
    private val plugin = resolved.catalog
    private val schemaJson = original.tableSchema.json
    private var used = false
    private var invoking = false
    private var mutation: DeltaRangerCreateMutation = null
    private var physical: DeltaRangerCreateExec = null
    val logical: Carrier = Carrier()(this)

    def context(): Unit = {
      if (
        (Thread.currentThread() ne thread) || !SparkSession.getActiveSession.exists(_ eq spark) ||
        (spark.sessionState.catalogManager.catalog(catalog.name()) ne plugin) ||
        plugin.name() != catalog.name() || original.name != resolved ||
        resolved.identifier.namespace().toVector != namespace || resolved.identifier
          .name() != name ||
        original.tableSchema.json != schemaJson
      )
        denied("command context changed")
      val now = DeltaRangerQuerySubject.current(spark)
      if (now.user() != subject.user() || now.groups() != subject.groups())
        denied("subject changed")
      val actual = spark.sessionState.conf.getAllConfs.toMap
      if (
        actual != settings && !(mutation != null && mutation.nativeConfirmed &&
          actual == settings.updated("spark.databricks.delta.lastCommitVersionInSession", "0"))
      )
        denied("settings changed")
    }
    def check(candidate: Carrier, session: SparkSession): Unit = {
      if ((candidate ne logical) || (session ne spark) || used)
        denied("foreign or consumed command")
      context()
    }
    def plan(candidate: Carrier, session: SparkSession): DeltaRangerCreateExec = {
      check(candidate, session)
      DeltaRangerAppendInventory.qualify(spark)
      if (physical == null) physical = DeltaRangerCreateExec()(this)
      physical
    }
    def verify(candidate: DeltaRangerCreateExec, session: SparkSession): Unit = {
      if ((candidate ne physical) || (session ne spark) || used)
        denied("foreign or consumed physical command")
      context()
    }
    def run(candidate: DeltaRangerCreateExec): Seq[InternalRow] = {
      verify(candidate, spark)
      if (DeltaRangerNativeOperation.current() != null || OptimisticTransaction.getActive.nonEmpty)
        denied("nested native operation")
      used = true
      invoking = true
      try {
        DeltaRangerCatalogSpark35.withCreate(
          this,
          () => {
            val stage = catalog.stageCreate(
              Identifier.of(namespace.toArray, name),
              DataType.fromJson(schemaJson).asInstanceOf[StructType],
              Array.empty[Transform],
              frozenProperties.asJava
            )
            stage.commitStagedChanges()
          }
        )
        if (mutation == null || !mutation.publishedCreate) denied("publication unconfirmed")
      } catch {
        case failure: Throwable =>
          if (mutation == null) throw failure
          throw mutation.failure(failure)
      } finally invoking = false
      Nil
    }
    override def lifecycle(
        candidate: DeltaRangerCatalogSpark35,
        intent: DeltaCreateLifecycle.Intent
    ): DeltaCreateLifecycle = {
      context()
      if (
        !invoking || !used || mutation != null || (candidate ne catalog) ||
        intent.catalog() != catalog.name() || intent.namespace().asScala.toVector != namespace ||
        intent.table() != name || intent.schemaJson() != schemaJson || intent
          .partitionCount() != 0 ||
        intent.properties().asScala.toMap != frozenProperties
      ) denied("unbound catalog declaration")
      mutation = new DeltaRangerCreateMutation(intent, catalog, spark, this)
      mutation
    }
    def requireMutation(candidate: DeltaRangerCreateMutation): Unit = {
      if (!invoking || !used || (mutation ne candidate)) denied("unissued mutation owner")
      context()
    }
    override def toString: String = "Issued schema CREATE command"
  }

  final case class Carrier()(@transient private val issued: Owner) extends LeafNode {
    def owner: Owner = if (issued == null) denied("inert command") else issued
    override def output: Seq[Attribute] = Nil
    override def clone(): LogicalPlan = this
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(issued)
    override def jsonFields = super.jsonFields.filterNot(_._1 == "issued")
  }
  private def denied(reason: String): Nothing = throw new SecurityException(
    "Schema CREATE rejected: " + reason
  )
}

private[ranger] final case class DeltaRangerCreateExec()(
    @transient private val issued: DeltaRangerCreateCommand.Owner
) extends V2CommandExec
    with LeafV2CommandExec {
  private def owner: DeltaRangerCreateCommand.Owner =
    if (issued == null || isCanonicalizedPlan)
      throw new SecurityException("Inert schema CREATE command")
    else issued
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(issued)
  override def jsonFields = super.jsonFields.filterNot(_._1 == "issued")
  override def output: Seq[Attribute] = Nil
  override def clone(): SparkPlan = this
  def verify(spark: SparkSession): Unit = owner.verify(this, spark)
  override def run(): Seq[InternalRow] = owner.run(this)
}
