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

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.write.{RequiresDistributionAndOrdering, V1Write}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.{
  AppendDataExecV1,
  DataSourceV2Relation,
  LeafV2CommandExec,
  V2CommandExec
}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.catalog.DeltaTableV2

/** Finite denial-only ownership of Spark's actual V1 fallback input. */
private[ranger] object DeltaRangerAppendV1Input {
  private val executing = new ThreadLocal[Owner]()
  val terminalMessage = "Delta append input validated; writes remain disabled"
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.append-input")
  final class Reference(@transient private val owner: Owner) extends Serializable {
    def get: Owner = if (owner == null) denied("inert phase reference") else owner
    override def toString: String = "Delta append phase reference"
  }

  def inspect(command: AppendData, spark: SparkSession): Unit = {
    DeltaRangerAppendInventory.qualify(spark)
    if (
      command.query.resolved && command.table.resolved && DeltaRangerDecisionReceipt.present(
        command.query
      )
    ) {
      def fields(output: Seq[Attribute]): Vector[Any] =
        output.map(a => (a.name, a.dataType, a.nullable)).toVector
      if (fields(command.query.output) != fields(command.table.output))
        denied("native source normalization is not qualified")
    }
  }

  def resolve(
      command: AppendData,
      spark: SparkSession,
      catalog: DeltaRangerCatalogSpark35
  ): LogicalPlan = {
    inspect(command, spark)
    if (executing.get() != null) denied("nested append execution")
    if (!command.resolved) return command
    command.getTagValue(tag) match {
      case Some(reference) => reference.get.resolution(command)
      case None =>
        val owner = new Owner(command, spark, catalog)
        command.setTagValue(tag, new Reference(owner))
        if (command.query.getTagValue(tag).nonEmpty) denied("source already has an append owner")
        command.query.setTagValue(tag, new Reference(owner))
    }
    command
  }
  def analyzed(command: AppendData, spark: SparkSession): Unit =
    authority(command).analyzed(command, spark)
  def prepare(command: AppendData, spark: SparkSession): LogicalPlan =
    authority(command).prepare(command, spark)
  def plan(command: AppendData, spark: SparkSession): SparkPlan =
    authority(command).plan(command, spark)
  private def authority(command: AppendData): Owner = {
    val owners = (Vector(command, command.query) ++ command.analyzedQuery.toVector)
      .flatMap(_.getTagValue(tag).map(_.get))
      .distinct
    if (owners.size != 1) denied("missing or ambiguous phase owner")
    owners.head
  }

  final class Owner private[DeltaRangerAppendV1Input] (
      original: AppendData,
      spark: SparkSession,
      catalog: DeltaRangerCatalogSpark35
  ) {
    private val thread = Thread.currentThread()
    private val association = DeltaRangerAppendCommandBinding.capture(spark, original, catalog)
    private val receipt =
      DeltaRangerDecisionReceipt.consumeOwned(original.query, spark, optimized = false)
    private val relation = original.table.asInstanceOf[DataSourceV2Relation]
    private val table = relation.table.asInstanceOf[DeltaTableV2]
    private val descriptor = table.catalogTable.get
    private val originalOutput = attributes(original.query.output)
    private val originalSchema = original.query.schema.json
    private val targetOutput = attributes(relation.output)
    private var finalSource: LogicalPlan = _
    private var fallbackSource: LogicalPlan = _
    private var prepared: DeltaRangerNativeScans.Prepared = _
    private var opaque: V1Write = _
    private var issued: AppendDataExecV1 = _
    private var running = false
    private var leafRunning = false
    private var writerIssued = false
    private var inputEntered = false
    private var expired = false
    private val writer = new TerminalWriter(this)

    private def current(session: SparkSession): Unit = {
      if (expired) denied("expired execution owner")
      if (!(session eq spark) || !SparkSession.getActiveSession.exists(_ eq spark))
        denied("session changed")
      if (Thread.currentThread() ne thread) denied("creator thread changed")
      if (DeltaRangerNativeOperation.current() != null || OptimisticTransaction.getActive.nonEmpty)
        denied("active native operation or transaction")
      DeltaRangerAppendInventory.qualify(session)
      if (executing.get() != null && !(executing.get() eq this)) denied("nested append execution")
      association.validateRetained(session)
    }
    private def shell(command: AppendData): Unit = {
      val actual = command.table match {
        case value: DataSourceV2Relation if value.getClass == classOf[DataSourceV2Relation] => value
        case _ => denied("target relation changed")
      }
      val checks = Vector(
        "class" -> (command.getClass == classOf[AppendData]),
        "resolved" -> command.resolved,
        "streaming" -> !command.isStreaming,
        "by-name" -> (command.isByName == original.isByName),
        "origin" -> (command.origin == original.origin),
        "options" -> (command.writeOptions == original.writeOptions && command.writeOptions.isEmpty),
        "table" -> (actual.table eq table),
        "descriptor" -> table.catalogTable.exists(_ eq descriptor),
        "catalog" -> actual.catalog.exists(_ eq relation.catalog.get),
        "identifier" -> actual.identifier.exists(_ eq relation.identifier.get),
        "relation options" -> actual.options.isEmpty,
        "target attributes" -> (attributes(actual.output) == targetOutput),
        "source schema" -> (command.query.schema.json == originalSchema)
      )
      checks
        .collectFirst { case (field, false) => field }
        .foreach(field => denied("command shell changed: " + field))
    }
    def resolution(command: AppendData): Unit = {
      current(spark)
      shell(command)
      if (command.write.nonEmpty || prepared != null) denied("resolution stage changed")
      receipt.consume(command.query, spark, optimized = false)
    }
    def analyzed(command: AppendData, session: SparkSession): Unit = {
      current(session)
      shell(command)
      if (command.write.nonEmpty || prepared != null) denied("final analysis stage changed")
      val stored = command.analyzedQuery.getOrElse(denied("missing actual analyzed source"))
      if (!(command.query eq original.query) || !(stored eq original.query))
        denied("native source normalization is not qualified")
      receipt.consume(stored, session, optimized = false)
      if (finalSource != null && !(finalSource eq stored)) denied("final source changed")
      finalSource = stored
    }
    def prepare(command: AppendData, session: SparkSession): LogicalPlan = {
      current(session)
      shell(command)
      if (prepared != null) denied("pre-CBO stage changed: repeated preparation")
      if (finalSource == null) denied("pre-CBO stage changed: missing final source")
      if (command.write.nonEmpty) denied("pre-CBO stage changed: premature writer")
      receipt.consume(
        command.analyzedQuery.getOrElse(denied("missing stored source")),
        session,
        optimized = false
      )
      receipt.consume(finalSource, session, optimized = false)
      receipt.consume(command.query, session, optimized = true)
      val query = DeltaRangerReadRule.prepare(command.query, session)
      prepared = DeltaRangerNativeScans.prepared(query)
      val next = command.copy(query = query)
      next.copyTagsFrom(command)
      next
    }
    def plan(command: AppendData, session: SparkSession): SparkPlan = {
      current(session)
      shell(command)
      if (prepared == null || command.analyzedQuery.isEmpty)
        denied("planner stage changed")
      receipt.consume(finalSource, session, optimized = false)
      val candidateSource = command.analyzedQuery.get
      receipt.consume(candidateSource, session, optimized = false)
      prepared.verifyExecutable(command.query, session)
      DeltaRangerPhysicalPlanning.qualify(session)
      if (opaque != null && !command.write.exists(_ eq opaque))
        denied("captured inert writer changed")
      if (issued != null) denied("planner stage changed")
      fallbackSource = candidateSource
      opaque = command.write match {
        case Some(value: V1Write)
            if !value.isInstanceOf[RequiresDistributionAndOrdering] &&
              value.getClass == Class.forName(
                "org.apache.spark.sql.delta.catalog.WriteIntoDeltaBuilder$$anon$1",
                false,
                classOf[DeltaTableV2].getClassLoader
              ) =>
          value
        case _ => denied("unqualified inert native writer")
      }
      val native = DeltaRangerPhysicalPlanning.appendRaw(session, command) match {
        case value: AppendDataExecV1 if value.getClass == classOf[AppendDataExecV1] => value
        case _ => denied("native planner product changed")
      }
      if (
        !(native.table eq table) || !(native.plan eq fallbackSource) || !(native.write eq opaque) ||
        native.children.nonEmpty || native.output.nonEmpty || !(native.session eq session)
      ) denied("native leaf changed")
      issued = native.copy(write = writer)
      val guard = DeltaRangerAppendInputExec()(issued, new Reference(this))
      guard
    }
    def verify(guard: DeltaRangerAppendInputExec, session: SparkSession): Unit = {
      current(session)
      if (
        issued == null || !(guard.delegate eq issued) || !(issued.write eq writer) ||
        !(issued.table eq table) || !(issued.plan eq fallbackSource) || issued.children.nonEmpty ||
        issued.output.nonEmpty || !(issued.session eq session)
      ) denied("issued physical leaf changed")
    }
    def run(guard: DeltaRangerAppendInputExec): Seq[InternalRow] = {
      if (Thread.currentThread() ne thread) denied("creator thread changed")
      if (!SparkSession.getActiveSession.exists(_ eq spark)) denied("session changed")
      if (expired) denied("expired execution owner")
      if (running || executing.get() != null) denied("nested execution")
      running = true
      executing.set(this)
      try {
        verify(guard, spark)
        leafRunning = true
        issued.run()
      } finally {
        leafRunning = false
        running = false
        expired = true
        executing.remove()
      }
    }
    def terminalRelation(): InsertableRelation = {
      if (expired) denied("expired execution owner")
      if (!running || !leafRunning || !(executing.get() eq this))
        denied("writer outside owned execution")
      if (writerIssued) denied("terminal writer already issued")
      writerIssued = true
      current(spark)
      new TerminalRelation(this)
    }
    def insert(data: Dataset[Row], overwrite: Boolean): Unit = {
      if (expired) denied("expired execution owner")
      if (inputEntered) denied("actual input already entered")
      inputEntered = true
      current(spark)
      if (!running || overwrite || data == null || !(data.sparkSession eq spark))
        denied("input execution context changed")
      val actual = data.queryExecution.analyzed
      if (attributes(actual.output) != originalOutput || actual.schema.json != originalSchema)
        denied("actual input output changed")
      receipt.consume(actual, spark, optimized = false)
      receipt.consume(finalSource, spark, optimized = false)
      current(spark)
      throw new SecurityException(terminalMessage)
    }
    override def toString: String = "Delta append input owner"
  }
  private final class TerminalWriter(owner: Owner) extends V1Write {
    override def toInsertableRelation(): InsertableRelation = owner.terminalRelation()
    override def toString: String = "Nondelegating Delta terminal writer"
  }
  private final class TerminalRelation(owner: Owner) extends InsertableRelation {
    override def insert(data: Dataset[Row], overwrite: Boolean): Unit =
      owner.insert(data, overwrite)
  }
  private def attributes(output: Seq[Attribute]): Vector[Vector[Any]] = output.map {
    case a: AttributeReference if a.getClass == classOf[AttributeReference] =>
      Vector(
        a.getClass,
        a.name,
        a.exprId,
        a.qualifier.toVector,
        a.dataType.json,
        a.nullable,
        a.metadata.json
      )
    case _ => denied("unqualified output attribute")
  }.toVector
  private def denied(reason: String): Nothing = throw new SecurityException(
    "Delta append input rejected: " + reason
  )
}

private[ranger] final class DeltaRangerAppendInputStrategy(spark: SparkSession)
    extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case append: AppendData => Seq(DeltaRangerAppendV1Input.plan(append, spark))
    case _                  => Nil
  }
}

/** Driver leaf: native run constructs the Dataset; finally revokes every exit, including analysis
  * failure.
  */
private[ranger] final case class DeltaRangerAppendInputExec()(
    @transient val delegate: AppendDataExecV1,
    @transient private val reference: DeltaRangerAppendV1Input.Reference
) extends V2CommandExec
    with LeafV2CommandExec {
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(delegate, reference)
  override def jsonFields =
    super.jsonFields.filterNot(field => Set("delegate", "reference")(field._1))
  // The checked native delegate is a leaf with no physical children; its source Dataset is
  // never executed. This guard creates no hidden RDD or execution resource to clean up.
  override def cleanupResources(): Unit = super.cleanupResources()
  private def owner: DeltaRangerAppendV1Input.Owner = {
    if (isCanonicalizedPlan)
      throw new SecurityException("Delta append input rejected: canonicalized execution")
    if (reference == null)
      throw new SecurityException("Delta append input rejected: inert execution owner")
    reference.get
  }
  def verify(spark: SparkSession): Unit = owner.verify(this, spark)
  override def output: Seq[Attribute] = Nil
  override def run(): Seq[InternalRow] = owner.run(this)
}
