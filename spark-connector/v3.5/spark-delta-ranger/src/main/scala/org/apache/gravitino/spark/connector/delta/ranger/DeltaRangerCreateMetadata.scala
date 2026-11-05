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

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, IsNotNull}
import org.apache.spark.sql.catalyst.plans.logical.{
  LocalRelation,
  LogicalPlan,
  Filter,
  Project,
  SerializeFromObject,
  MapElements,
  DeserializeToObject
}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.delta.actions.{Action, AddFile, SingleAction}
import org.apache.spark.sql.execution.LogicalRDD

/** The measured public zero-partition conversion graph; this carries no operation authority. */
private[ranger] final class DeltaRangerCreateMetadata(spark: SparkSession, validate: () => Unit) {
  private val mapClass =
    Class.forName("org.apache.spark.rdd.MapPartitionsRDD", false, classOf[RDD[_]].getClassLoader)
  private val emptyClass =
    Class.forName("org.apache.spark.rdd.EmptyRDD", false, classOf[RDD[_]].getClassLoader)
  private val structure = new DeltaRangerMetadataStructure("CREATE metadata rejected: ")
  private val retained = ArrayBuffer.empty[(LogicalRDD, Vector[Any], Vector[AnyRef])]

  def contains(candidate: LogicalRDD): Boolean = {
    validate()
    retained.find(_._1.rdd eq candidate.rdd) match {
      case Some((original, attributes, graph)) =>
        if (
          stamp(original) != attributes || stamp(candidate) != attributes ||
          topology(original) != graph || topology(candidate) != graph
        )
          denied("captured empty graph changed")
        true
      case None =>
        if (!initial(candidate)) return false
        val graph = topology(candidate)
        retained += ((candidate, stamp(candidate), graph))
        true
    }
  }

  // Only the observed empty-state prefixes and stock AddFile serializer may reach native analysis.
  // The shared commit metadata validator independently verifies the complete execution recipe.
  def prefix(plan: LogicalPlan): Boolean = {
    validate()
    plan match {
      case rdd: LogicalRDD => contains(rdd)
      case Filter(IsNotNull(add: AttributeReference), rdd: LogicalRDD) =>
        contains(rdd) && rdd.output.find(_.name == "add").contains(add)
      case Project(columns, filtered @ Filter(IsNotNull(add: AttributeReference), _: LogicalRDD)) =>
        columns == Seq(add) && prefix(filtered)
      case decoded: DeserializeToObject =>
        decoded.child match {
          case local: LocalRelation if !local.isStreaming && local.data.isEmpty =>
            val encoder = decoded.deserializer.dataType match {
              case ObjectType(clazz) if clazz == classOf[SingleAction] =>
                Some(Encoders.product[SingleAction].asInstanceOf[ExpressionEncoder[_]])
              case ObjectType(clazz) if clazz == classOf[AddFile] =>
                Some(Encoders.product[AddFile].asInstanceOf[ExpressionEncoder[_]])
              case _ => None
            }
            encoder.exists(e =>
              local.output.map(_.name) == e.schema.fieldNames.toSeq &&
                local.output.map(_.dataType) == e.schema.fields.map(_.dataType).toSeq &&
                structure.encoderBinding(decoded, e, local.output)
            )
          case _ => false
        }
      case serialized: SerializeFromObject =>
        serialized.child match {
          case map: MapElements =>
            map.child match {
              case decoded: DeserializeToObject =>
                decoded.child match {
                  case selected: Project => prefix(selected)
                  case _                 => false
                }
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def initial(candidate: LogicalRDD): Boolean =
    candidate.getClass == classOf[LogicalRDD] && !candidate.isStreaming &&
      candidate.output.forall(_.getClass == classOf[AttributeReference]) &&
      candidate.schema == Action.logSchema && candidate.rdd.getClass == mapClass &&
      candidate.outputPartitioning.getClass == classOf[UnknownPartitioning] &&
      candidate.outputPartitioning.numPartitions == 0 && candidate.outputOrdering.isEmpty &&
      DeltaRangerNativeOperation.stockCacheWrapper(candidate, spark)

  private def topology(candidate: LogicalRDD): Vector[AnyRef] = {
    if (!initial(candidate)) denied("foreign empty plan")
    val map = candidate.rdd
    val edges = map.dependencies.toVector
    if (edges.size != 1 || edges.head.getClass != classOf[OneToOneDependency[_]])
      denied("unknown empty dependency")
    val root = edges.head.rdd
    // Check the whole exact-class graph before calling partition getters, which delegate upstream.
    if (root.getClass != emptyClass) denied("unknown empty root")
    if (
      root.dependencies.nonEmpty || (root.context ne spark.sparkContext) ||
      (map.context ne spark.sparkContext) || root.partitions.nonEmpty || map.partitions.nonEmpty
    )
      denied("nonempty or foreign graph")
    Vector(map, edges.head, root)
  }

  private def stamp(candidate: LogicalRDD): Vector[Any] = {
    if (!initial(candidate) || candidate.output.exists(_.getClass != classOf[AttributeReference]))
      denied("empty output changed")
    candidate.output.toVector.map(a =>
      Vector(a.name, a.exprId, a.qualifier.toVector, a.dataType.json, a.nullable, a.metadata.json)
    )
  }
  private def denied(reason: String): Nothing =
    throw new SecurityException("CREATE metadata rejected: " + reason)
}
