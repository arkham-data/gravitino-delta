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
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{LeafV2CommandExec, V2CommandExec}
import org.apache.spark.sql.types.{DataType, StructType}

/** One privately issued initial named schema CREATE, without a final SELECT observation. */
private[ranger] object DeltaRangerCreateCommand {
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
    val declaration = DeltaRangerCreateDeclaration(command, specification, spark)
    new Owner(
      command,
      resolved,
      catalog,
      spark,
      declaration
    ).logical
  }

  final class Owner private[DeltaRangerCreateCommand] (
      original: CreateTable,
      resolved: ResolvedIdentifier,
      catalog: DeltaRangerCatalogSpark35,
      spark: SparkSession,
      declaration: DeltaRangerCreateDeclaration
  ) extends DeltaRangerCatalogSpark35.CreateInvocation {
    private val thread = Thread.currentThread()
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val engine = catalog.policyEngine()
    private val settings = spark.sessionState.conf.getAllConfs.toMap
    private val namespace = resolved.identifier.namespace().toVector
    private val name = resolved.identifier.name()
    private val plugin = resolved.catalog
    private val schemaJson = declaration.schemaJson
    private var input: Option[DeltaRangerMetadataInput] = None
    private var used = false
    private var invoking = false
    private var mutation: DeltaRangerCreateMutation = null
    private var physical: DeltaRangerCreateExec = null
    val logical: Carrier = Carrier()(this)

    def bindInput(candidate: Carrier, value: DeltaRangerMetadataInput): Unit = {
      check(candidate, spark)
      if (input.nonEmpty) denied("input already bound")
      value.requireLive()
      input = Some(value)
    }
    def externalInput: Option[DeltaRangerMetadataInput] = input

    def context(): Unit = {
      if (
        (Thread.currentThread() ne thread) || !SparkSession.getActiveSession.exists(_ eq spark) ||
        (spark.sessionState.catalogManager.catalog(catalog.name()) ne plugin) ||
        plugin.name() != catalog.name() || original.name != resolved ||
        resolved.identifier.namespace().toVector != namespace || resolved.identifier
          .name() != name ||
        !declaration.unchanged(original)
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
      input.foreach(_.requireLive())
      used = true
      invoking = true
      try {
        if (original.ignoreIfExists) {
          authorizeCreate()
          if (catalog.publishedForCreate(Identifier.of(namespace.toArray, name)).isPresent) {
            authorizeCreate()
            catalog.requireCreateMetadataLive()
            return Nil
          }
        }
        DeltaRangerCatalogSpark35.withCreate(
          this,
          () => {
            val stage = catalog.stageCreate(
              Identifier.of(namespace.toArray, name),
              DataType.fromJson(schemaJson).asInstanceOf[StructType],
              declaration.partitions,
              declaration.properties.asJava
            )
            stage.commitStagedChanges()
          }
        )
        if (mutation == null || !mutation.publishedCreate) denied("publication unconfirmed")
        input.foreach(_.complete(mutation))
      } catch {
        case failure: Throwable =>
          if (mutation == null) throw failure
          throw mutation.failure(failure)
      } finally invoking = false
      Nil
    }
    private def authorizeCreate(): Unit = {
      context()
      if (catalog.policyEngine() ne engine) denied("expired catalog")
      engine.authorize(
        subject,
        new DeltaRangerResourceKey(catalog.name(), namespace.head, name, null, "create", null)
      )
      context()
    }
    override def lifecycle(
        candidate: DeltaRangerCatalogSpark35,
        intent: DeltaCreateLifecycle.Intent
    ): DeltaCreateLifecycle = {
      context()
      if (
        !invoking || !used || mutation != null || (candidate ne catalog) ||
        intent.catalog() != catalog.name() || intent.namespace().asScala.toVector != namespace ||
        intent.table() != name || !declaration.matches(intent)
      ) denied("unbound catalog declaration")
      mutation = new DeltaRangerCreateMutation(intent, catalog, spark, this, declaration)
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
