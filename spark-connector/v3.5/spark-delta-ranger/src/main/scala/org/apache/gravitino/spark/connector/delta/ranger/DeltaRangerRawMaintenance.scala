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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Admission for the deliberately bounded raw leaf and attribute projection recipe. */
private[ranger] object DeltaRangerRawMaintenance {
  def normalize(plan: LogicalPlan, spark: SparkSession): LogicalPlan = {
    DeltaRangerNativeOperation.requireNoRawSelection()
    if (
      DeltaRangerNativeOperation.current() != null ||
      plan.exists(DeltaRangerDecisionReceipt.present) || !raw(plan)
    ) return plan
    val serializer = DeltaRangerRawCacheSerializer.observe(spark)
    if (
      spark.sessionState.conf.adaptiveExecutionEnabled ||
      spark.conf.get("spark.databricks.delta.asyncUpdate.stalenessTimeLimit", "0").toLong != 0L
    )
      reject("unqualified compilation settings")
    DeltaRangerPhysicalPlanning.qualify(spark)
    val relation = leaf(plan)
    val fs = relation.relation.asInstanceOf[HadoopFsRelation]
    val index = fs.location.asInstanceOf[TahoeLogFileIndex]
    DeltaRangerCdfCacheMaintenance.authorize(plan, spark, index)
    val observation = DeltaRangerRawNativeObservation.observe(spark, index)
    val protocol = observation.selectedHead.protocol
    val metadata = observation.selectedHead.metadata
    if (
      (!observation.cdfMaintenance && (protocol.minReaderVersion != 1 || protocol.minWriterVersion > 2 ||
        protocol.readerFeatures.nonEmpty || protocol.writerFeatures.nonEmpty ||
        metadata.configuration.getOrElse("delta.enableChangeDataFeed", "false") != "false")) ||
      metadata.format.provider != "parquet" || metadata.format.options.nonEmpty ||
      metadata.configuration.getOrElse("delta.columnMapping.mode", "none") != "none" ||
      Seq(
        "delta.enableDeletionVectors",
        "delta.enableRowTracking",
        "delta.enableTypeWidening"
      ).exists(name => metadata.configuration.getOrElse(name, "false") != "false")
    )
      reject("unqualified raw protocol or table feature")
    val expected =
      index.deltaLog.buildHadoopFsRelationWithFileIndex(index.snapshotAtAnalysis, index, None)
    val nativeClass = Class.forName(
      "org.apache.spark.sql.delta.DeltaLog$$anon$2",
      false,
      classOf[DeltaLog].getClassLoader
    )
    if (
      (fs.getClass != classOf[HadoopFsRelation] && fs.getClass != nativeClass) ||
      fs.getClass.getProtectionDomain.getCodeSource !=
        (if (fs.getClass == classOf[HadoopFsRelation]) classOf[HadoopFsRelation]
         else classOf[DeltaLog]).getProtectionDomain.getCodeSource ||
        fs.dataSchema != expected.dataSchema || fs.partitionSchema != expected.partitionSchema ||
        fs.bucketSpec.nonEmpty || fs.options.nonEmpty || fs.options != expected.options ||
        fs.fileFormat.getClass != expected.fileFormat.getClass || fs.fileFormat != expected.fileFormat ||
        relation.output.map(a => (a.name, a.dataType, a.nullable, a.metadata)) !=
        LogicalRelation(expected).output.map(a => (a.name, a.dataType, a.nullable, a.metadata)) ||
        relation.output.map(_.exprId).distinct.size != relation.output.size
    )
      reject("raw relation changed")
    val input = new SelectionInput(spark, plan, serializer, observation)
    Carrier(copyRecipe(plan), input)
  }

  def raw(plan: LogicalPlan): Boolean = plan match {
    case p: Project if p.getClass == classOf[Project] =>
      raw(p.child) && p.projectList.forall(a =>
        a.getClass == classOf[AttributeReference] &&
          p.child.output.contains(a)
      )
    case r: LogicalRelation
        if r.getClass == classOf[LogicalRelation] &&
          r.catalogTable.isEmpty && !r.isStreaming =>
      r.relation match {
        case fs: HadoopFsRelation if nativeRelationClass(fs) =>
          fs.location.getClass == classOf[TahoeLogFileIndex] &&
          r.output.forall(_.getClass == classOf[AttributeReference])
        case _ => false
      }
    case _ => false
  }

  private def nativeRelationClass(fs: HadoopFsRelation): Boolean =
    fs.getClass == classOf[HadoopFsRelation] || fs.getClass == Class.forName(
      "org.apache.spark.sql.delta.DeltaLog$$anon$2",
      false,
      classOf[DeltaLog].getClassLoader
    )

  trait Compilation {
    def prepare(plan: LogicalPlan): LogicalPlan
    def plan(child: LogicalPlan): org.apache.spark.sql.execution.SparkPlan
  }

  final class SelectionInput(
      val session: SparkSession,
      val key: LogicalPlan,
      val serializer: DeltaRangerRawCacheSerializer.Observation,
      val observation: DeltaRangerRawNativeObservation.Observation
  ) extends Compilation {
    private val settings = session.sessionState.conf.getAllConfs
    private val original = DeltaRangerNativeProvenance.binding(key)
    private var prepared = false
    def unchanged(): Unit = {
      if (
        settings != session.sessionState.conf.getAllConfs ||
        DeltaRangerNativeProvenance.binding(key) != original
      )
        reject("raw selection input changed")
      serializer.requireCurrent(session)
    }
    override def prepare(plan: LogicalPlan): LogicalPlan = {
      if (prepared) reject("raw selection repeated")
      prepared = true
      unchanged()
      observation.requireCurrent(session)
      val dependency = DeltaRangerRawCacheGeneration.validateRecipe(key, plan, observation)
      val (selected, native) = DeltaRangerRawNativeScans.prepare(this, plan, dependency)
      unchanged()
      val generation = new DeltaRangerRawCacheGeneration(
        session,
        key,
        serializer,
        native.observation,
        native,
        dependency,
        selected
      )
      Carrier(selected, generation)
    }
    override def plan(child: LogicalPlan): org.apache.spark.sql.execution.SparkPlan =
      reject("unprepared raw selection input")
  }

  private def leaf(plan: LogicalPlan): LogicalRelation = plan match {
    case p: Project         => leaf(p.child)
    case r: LogicalRelation => r
    case _                  => reject("raw recipe changed")
  }

  private def copyRecipe(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project         => p.copy(child = copyRecipe(p.child))
    case r: LogicalRelation => r.copy()
    case _                  => reject("raw recipe changed")
  }

  final case class Carrier(
      child: LogicalPlan,
      @transient private val generation: Compilation
  ) extends UnaryNode {
    override def output = child.output
    def owner: Compilation = {
      if (
        generation == null || (generation.getClass != classOf[SelectionInput] &&
          generation.getClass != classOf[DeltaRangerRawCacheGeneration])
      ) reject("inert raw carrier")
      generation
    }
    override protected def withNewChildInternal(newChild: LogicalPlan): Carrier =
      copy(child = newChild)
  }

  private def reject(reason: String): Nothing =
    throw new SecurityException("Raw maintenance rejected: " + reason)
}

private[ranger] final class DeltaRangerRawNormalization(spark: SparkSession)
    extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    DeltaRangerRawMaintenance.normalize(DeltaRangerCdfRead.normalize(plan, spark), spark)
}
