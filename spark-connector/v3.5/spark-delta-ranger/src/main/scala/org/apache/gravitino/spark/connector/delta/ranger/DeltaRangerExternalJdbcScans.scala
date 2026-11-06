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

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{
  SparkPlan,
  SparkStrategy,
  ProjectExec,
  FilterExec,
  RowDataSourceScanExec
}
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}

/** Owns stock V1 planning per exact source occurrence and complete accepted recipe. */
private[ranger] object DeltaRangerExternalJdbcScans {
  final class Prepared private[DeltaRangerExternalJdbcScans] (
      owner: DeltaRangerNativeScans,
      plan: LogicalPlan
  ) {
    private val relations = plan.collect {
      case relation: LogicalRelation if DeltaRangerInputSources.isExternal(relation) => relation
    }.toVector
    private val frozen = relations.map(logicalBinding)
    val obligations = relations.map { relation =>
      val found = owner.obligations.filter(_.source match {
        case source: DeltaRangerInputSources.ExternalJdbc =>
          (source.binding.relation eq relation.relation) && relation.output.forall(attribute =>
            source.relation.output.exists(_.exprId == attribute.exprId)
          )
        case _ => false
      })
      if (found.size != 1) denied()
      found.head
    }
    private var replay: () => LogicalPlan = _
    private val continuations = relations.zip(frozen).map { case (relation, expected) =>
      val continuation = new Continuation(owner, this, expected)
      relation.setTagValue(tag, new Reference(continuation))
      continuation
    }
    def bindRecipe(value: () => LogicalPlan): Unit = {
      if (replay != null) denied()
      replay = value
    }
    def verify(plan: LogicalPlan): Unit = {
      val actual = plan.collect {
        case relation: LogicalRelation if DeltaRangerInputSources.isExternal(relation) =>
          logicalBinding(relation)
      }.toVector
      if (actual != frozen) denied()
    }
    private[DeltaRangerExternalJdbcScans] def requireRecipe(
        candidate: LogicalPlan,
        expected: Any
    ): Unit = {
      if (replay == null || !continuations.exists(_.expected == expected)) denied()
      val allowed = replay().collect {
        case operation @ PhysicalOperation(_, _, relation: LogicalRelation)
            if DeltaRangerInputSources
              .isExternal(relation) && logicalBinding(relation) == expected =>
          DeltaRangerDecisionReceipt.binding(operation)
      }
      if (!allowed.contains(DeltaRangerDecisionReceipt.binding(candidate))) denied()
    }
  }

  private final class Continuation(
      owner: DeltaRangerNativeScans,
      prepared: Prepared,
      val expected: Any
  ) {
    private val native = mutable.Map.empty[Vector[Any], SparkPlan]
    def physical(plan: LogicalPlan, relation: LogicalRelation, session: SparkSession): SparkPlan =
      synchronized {
        if ((session ne owner.session) || logicalBinding(relation) != expected) denied()
        owner.requireLive()
        prepared.requireRecipe(plan, expected)
        val key = DeltaRangerDecisionReceipt.binding(plan)
        val result = native.getOrElseUpdate(
          key, {
            val emitted = DataSourceStrategy(plan)
            if (emitted.size != 1) denied()
            val nodes = emitted.head.collect { case node => node }
            val scans = nodes.collect { case scan: RowDataSourceScanExec => scan }
            if (
              scans.size != 1 || (scans.head.relation ne relation.relation) ||
              nodes.exists(node =>
                !Set[Class[_]](
                  classOf[RowDataSourceScanExec],
                  classOf[ProjectExec],
                  classOf[FilterExec]
                )(node.getClass)
              )
            ) denied()
            emitted.head
          }
        )
        if (DeltaRangerDecisionReceipt.binding(plan) != key || logicalBinding(relation) != expected)
          denied()
        result
      }
  }
  private final class Reference(@transient private val value: Continuation) extends Serializable {
    def continuation: Continuation = { if (value == null) denied(); value }
    override def toString: String = "External JDBC continuation reference"
  }
  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.external-jdbc-scan")
  private final class Identity(private val value: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case that: Identity => value eq that.value
      case _              => false
    }
    override def hashCode(): Int = System.identityHashCode(value)
    override def toString: String = "Issued external JDBC identity"
  }
  def logicalBinding(relation: LogicalRelation): Any = {
    val source = DeltaRangerInputSources.external(relation, SparkSession.active)
    Vector(new Identity(source.binding), relation.schema.json, relation.isStreaming)
  }
  def prepare(plan: LogicalPlan, owner: DeltaRangerNativeScans): Prepared =
    new Prepared(owner, plan)
  def plan(logical: LogicalPlan, spark: SparkSession): Seq[SparkPlan] = logical match {
    case operation @ PhysicalOperation(_, _, relation: LogicalRelation)
        if DeltaRangerInputSources.isExternal(relation) =>
      Seq(
        relation
          .getTagValue(tag)
          .getOrElse(denied())
          .continuation
          .physical(operation, relation, spark)
      )
    case _ => Nil
  }
  final class Strategy(spark: SparkSession) extends SparkStrategy {
    override def apply(logical: LogicalPlan): Seq[SparkPlan] = plan(logical, spark)
  }
  private def denied(): Nothing = throw new SecurityException(
    "External JDBC scan does not match its owned stock continuation"
  )
}
