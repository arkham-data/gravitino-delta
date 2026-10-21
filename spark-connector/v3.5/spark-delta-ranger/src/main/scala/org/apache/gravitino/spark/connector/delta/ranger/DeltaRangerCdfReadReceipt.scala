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

import java.time.Instant
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.kyuubi.plugin.spark.authz.rule.RuleEliminateMarker
import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.RuleApplyDataMaskingStage1

/** Nonserializable policy and original-demand authority for one complete analyzed action query. */
private[ranger] final class DeltaRangerCdfReadReceipt private (
    source: DeltaRangerCdfRead.Source,
    original: LogicalPlan,
    spark: SparkSession
) extends DeltaRangerCollectValidationContext {
  import DeltaRangerCdfRead.denied
  private val analyzed = source.observation(original)
  analyzed.validate()
  private val settings = spark.sessionState.conf.getAllConfs
  private val originalBinding = DeltaRangerNativeProvenance.binding(original)
  private val subject = DeltaRangerQuerySubject.current(spark)
  private val anchor = source.binding.anchor()
  private val engine = anchor.engine()
  private val stamp = engine.snapshotStamp()
  private val descriptor = anchor.descriptor()
  private val id = descriptor.identifier
  private val scan = DeltaRangerReadRule.Scan(
    source.logical,
    id.catalog.get,
    id.database.get,
    id.table,
    Option(descriptor.owner).filter(_.nonEmpty)
  )
  private val used = original.output.map(_.exprId).toSet ++ original.collect {
    case node if !node.isInstanceOf[LogicalRelation] =>
      node.expressions.flatMap(_.references.map(_.exprId))
  }.flatten
  private val columns = source.logical.output.filter(a => used(a.exprId)).map(_.name).toSet
  if (columns.isEmpty) denied("columnless CDF demand")
  private val keys = DeltaRangerResourceKey
    .select(scan.catalog, scan.database, scan.table, columns.asJava, scan.owner.orNull)
    .asScala
    .toVector
  private val instant = Instant.now()
  keys.foreach(engine.authorize(subject, _, instant))
  private val decisions = DeltaRangerMaskRules.capture(scan, columns, engine, subject, instant)
  analyzed.requireDecisions(decisions)
  private val interval = DeltaRangerCdfHistory.capture(source.binding, source.request, spark)
  private val scans = new DeltaRangerCdfReadScans(interval, spark)
  private val masked = DeltaRangerMaskRules.rewrite(scan, columns, decisions, spark)
  private val rewritten = RuleEliminateMarker(
    RuleApplyDataMaskingStage1(spark)(original.transformUp {
      case relation: LogicalRelation if relation eq source.logical => masked
    })
  )
  private val native =
    DeltaRangerCdfNativeReader.read(interval, scans, spark).queryExecution.analyzed
  private val payload = Project(
    source.logical.output.map { expected =>
      val actual =
        native.output.find(_.name == expected.name).getOrElse(denied("missing native CDF field"))
      if (actual.dataType != expected.dataType) denied("native CDF field type changed")
      Alias(actual, expected.name)(exprId = expected.exprId, qualifier = expected.qualifier)
    },
    native
  )
  private val governed = rewritten.transformUp {
    case relation: LogicalRelation if relation eq source.logical => payload
  }
  val carrier: DeltaRangerCdfRead.Carrier = DeltaRangerCdfRead.Carrier(governed.output)(this)
  private var physical: SparkPlan = null
  private var prepared = false
  private var installed = false
  private var actionStarted = false

  def requireCarrier(candidate: DeltaRangerCdfRead.Carrier): Unit = {
    if ((candidate ne carrier) || candidate.output != governed.output)
      denied("copied or changed CDF executable carrier")
    requireContext()
  }
  def validateAction(session: SparkSession): Unit = {
    if (session ne spark) denied("CDF session changed")
    requireContext()
    val before = source.binding.validateCurrent(spark)
    val now = Instant.now()
    keys.foreach(engine.authorize(subject, _, now))
    val current = DeltaRangerMaskRules.capture(scan, columns, engine, subject, now)
    if (current.filter != decisions.filter || current.masks != decisions.masks)
      denied("CDF policy changed")
    scans.validateFiles()
    before.requireSame(source.binding.validateCurrent(spark))
    requireContext()
  }
  def plan(candidate: DeltaRangerCdfRead.Carrier): SparkPlan = {
    requireCarrier(candidate)
    validateAction(spark)
    if (physical != null) denied("CDF physical compilation repeated")
    val owned = scans.prepare(governed)
    val child = DeltaRangerPhysicalPlanning.lower(spark, owned)
    scans.seal(child)
    scans.tail(DeltaRangerPhysicalPlanning.tail(spark, owned, child))
    physical = DeltaRangerCdfProducer(child)(this)
    physical
  }
  def beforeTransitions(candidate: DeltaRangerCdfProducer): Unit = {
    if ((candidate ne physical) || installed) denied("foreign CDF physical preparation")
    scans.verify(candidate.child)
    prepared = true
  }
  def afterTransitions(candidate: DeltaRangerCdfProducer): Unit = {
    if (!prepared || installed) denied("repeated CDF physical installation")
    if (candidate ne physical) denied("foreign CDF physical installation")
    requireContext()
    scans.verify(candidate.child)
    installed = true
  }
  def replaceProducer(
      previous: DeltaRangerCdfProducer,
      child: SparkPlan
  ): DeltaRangerCdfProducer = {
    if (previous ne physical) denied("foreign CDF physical transition")
    requireContext()
    if (child eq previous.child) return previous
    if (actionStarted) denied("CDF physical transitions closed")
    scans.verify(child)
    val next = DeltaRangerCdfProducer(child)(this)
    physical = next
    next
  }
  def requireProducer(candidate: DeltaRangerCdfProducer): Unit = {
    if (!installed || (candidate ne physical)) denied("copied CDF physical carrier")
    actionStarted = true
  }
  def execute(
      child: SparkPlan
  ): org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    validateExecution(child)
    scans.execute(child)
  }
  override def validateCollection(child: SparkPlan): Unit = {
    scans.verify(child); validateAction(spark); scans.verify(child)
  }
  override def validateExecution(child: SparkPlan): Unit = {
    scans.verify(child); validateAction(spark); scans.verify(child)
  }
  private def requireContext(): Unit = {
    if (
      (SparkSession.active ne spark) || settings != spark.sessionState.conf.getAllConfs || originalBinding != DeltaRangerNativeProvenance
        .binding(original)
    ) denied("CDF context changed")
    val current = DeltaRangerQuerySubject.current(spark)
    if (current.user() != subject.user() || current.groups() != subject.groups())
      denied("CDF subject changed")
    engine.requireSnapshot(stamp)
    DeltaRangerCdfHistory.requireSettings(spark)
    source.check(source.logical, spark)
  }
}

private[ranger] object DeltaRangerCdfReadReceipt {

  /** Analysis evidence is not execution authority and never authorizes eager unused output. */
  final class Observation(
      source: DeltaRangerCdfRead.Source,
      original: LogicalPlan,
      spark: SparkSession
  ) {
    import DeltaRangerCdfRead.denied
    private val settings = spark.sessionState.conf.getAllConfs
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val engine = source.binding.anchor().engine()
    private val stamp = engine.snapshotStamp()
    private val descriptor = source.binding.anchor().descriptor()
    private val id = descriptor.identifier
    private val scan = DeltaRangerReadRule.Scan(
      source.logical,
      id.catalog.get,
      id.database.get,
      id.table,
      Option(descriptor.owner).filter(_.nonEmpty)
    )
    private val used = original.output.map(_.exprId).toSet ++ original.collect {
      case node if !node.isInstanceOf[LogicalRelation] =>
        node.expressions.flatMap(_.references.map(_.exprId))
    }.flatten
    private val columns = source.logical.output.filter(a => used(a.exprId)).map(_.name).toSet
    private val decisions =
      DeltaRangerMaskRules.capture(scan, columns, engine, subject, Instant.now())
    validate()
    def validate(): Unit = {
      if ((SparkSession.active ne spark) || settings != spark.sessionState.conf.getAllConfs)
        denied("analyzed CDF context changed")
      val current = DeltaRangerQuerySubject.current(spark)
      if (current.user() != subject.user() || current.groups() != subject.groups())
        denied("analyzed CDF subject changed")
      engine.requireSnapshot(stamp)
      source.check(source.logical, spark)
    }
    def requireDecisions(current: DeltaRangerMaskRules.Decisions): Unit = {
      validate()
      if (current.filter != decisions.filter || current.masks != decisions.masks)
        denied("analyzed CDF policy changed")
    }
  }

  def issue(
      source: DeltaRangerCdfRead.Source,
      original: LogicalPlan,
      spark: SparkSession
  ): DeltaRangerCdfReadReceipt =
    new DeltaRangerCdfReadReceipt(source, original, spark)
}

/** Visible physical child remains subject to driver validation on collection and execution. */
private[ranger] final case class DeltaRangerCdfProducer(child: SparkPlan)(
    @transient private val authority: DeltaRangerCdfReadReceipt
) extends UnaryExecNode {
  def receipt: DeltaRangerCdfReadReceipt = {
    if (authority == null || isCanonicalizedPlan)
      DeltaRangerCdfRead.denied("inert CDF physical carrier")
    authority
  }
  override def output = child.output
  override def outputPartitioning = child.outputPartitioning
  override def outputOrdering = child.outputOrdering
  override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
  override def clone(): SparkPlan = this
  override def executeCollect(): Array[org.apache.spark.sql.catalyst.InternalRow] = {
    receipt.requireProducer(this)
    receipt.validateCollection(child)
    super.executeCollect()
  }
  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    receipt.requireProducer(this)
    receipt.execute(child)
  }
  override protected def withNewChildInternal(child: SparkPlan): DeltaRangerCdfProducer =
    receipt.replaceProducer(this, child)
}
