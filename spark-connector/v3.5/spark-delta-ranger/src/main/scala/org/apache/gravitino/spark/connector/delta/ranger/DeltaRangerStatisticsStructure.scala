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

import java.lang.reflect.Modifier
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AttributeReference,
  ExprId,
  Expression,
  ScalaUDF
}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.mutable
import scala.reflect.ClassTag

/** Structural comparison of native statistics helpers; this grants no query authority. */
private[ranger] object DeltaRangerStatisticsStructure {

  /** Fresh helper aliases/aggregate IDs form one bijection; original source IDs never change. */
  def matches(
      expected: LogicalPlan,
      actual: LogicalPlan,
      source: LogicalPlan
  ): Boolean = {
    if (!expected.resolved || !actual.resolved || expected.getClass != actual.getClass) return false
    def ids(value: Any): Set[ExprId] = value match {
      case id: ExprId  => Set(id)
      case seq: Seq[_] => seq.flatMap(ids).toSet
      case Some(v)     => ids(v)
      case _           => Set.empty
    }
    val retained = ids(DeltaRangerNativeProvenance.binding(source))
    val forward = mutable.Map.empty[ExprId, ExprId]
    val reverse = mutable.Map.empty[ExprId, ExprId]
    def equal(a: Any, b: Any): Boolean = (a, b) match {
      case (x: ExprId, y: ExprId) if retained(x) || retained(y) => x == y
      case (x: ExprId, y: ExprId) =>
        forward.getOrElseUpdate(x, y) == y && reverse.getOrElseUpdate(y, x) == x
      case (x: Seq[_], y: Seq[_]) =>
        x.size == y.size && x.zip(y).forall { case (l, r) => equal(l, r) }
      case (Some(x), Some(y)) => equal(x, y)
      case _                  => a == b
    }
    val functions: Set[Class[_]] = expected
      .flatMap(_.expressions.flatMap(_.collect { case udf: ScalaUDF =>
        udf.function.getClass
      }))
      .toSet
    try equal(helperBinding(expected, functions), helperBinding(actual, functions))
    catch { case _: SecurityException => false }
  }
  private def helperBinding(value: Any, functions: Set[Class[_]]): Any = value match {
    case relation: LogicalRelation => DeltaRangerNativeProvenance.binding(relation)
    case attribute: AttributeReference =>
      Vector(
        attribute.getClass,
        attribute.name,
        attribute.exprId,
        attribute.qualifier.toVector,
        attribute.dataType.json,
        attribute.nullable,
        attribute.metadata.json
      )
    case alias: Alias =>
      Vector(
        alias.getClass,
        helperBinding(alias.child, functions),
        alias.name,
        alias.exprId,
        alias.qualifier.toVector,
        alias.explicitMetadata.map(_.json),
        alias.nonInheritableMetadataKeys.toVector
      )
    case expression: Expression =>
      Vector(
        expression.getClass,
        expression.productIterator.map(v => helperBinding(v, functions)).toVector
      )
    case plan: LogicalPlan =>
      Vector(
        plan.getClass,
        plan.productIterator.map(v => helperBinding(v, functions)).toVector,
        plan.output.map(v => helperBinding(v, functions))
      )
    case encoder: ExpressionEncoder[_] =>
      Vector(
        encoder.getClass,
        helperBinding(encoder.objSerializer, functions),
        helperBinding(encoder.objDeserializer, functions),
        encoder.clsTag.runtimeClass
      )
    case tag: ClassTag[_]   => tag.runtimeClass
    case data: DataType     => data.json
    case metadata: Metadata => metadata.json
    case seq: Seq[_]        => seq.map(v => helperBinding(v, functions)).toVector
    case Some(v)            => Some(helperBinding(v, functions))
    case array: Array[_]    => array.map(v => helperBinding(v, functions)).toVector
    case text: UTF8String   => text.getBytes.toVector
    case map: scala.collection.Map[_, _] =>
      map.map { case (k, v) => helperBinding(k, functions) -> helperBinding(v, functions) }.toMap
    case function: Function1[_, _] =>
      if (!functions(function.getClass)) denied("foreign native statistics function")
      val fields =
        function.getClass.getDeclaredFields.filterNot(f => Modifier.isStatic(f.getModifiers))
      if (
        fields.length != 1 || fields.head.getName != "arg$1" ||
        fields.head.getType.getName != "org.apache.spark.sql.delta.stats.StatisticsCollection$$anonfun$3"
      )
        denied("native statistics closure shape changed")
      val field = fields.head
      field.setAccessible(true)
      val captured = field.get(function)
      if (captured == null || captured.getClass != field.getType)
        denied("native statistics capture changed")
      val captures =
        captured.getClass.getDeclaredFields.filterNot(f => Modifier.isStatic(f.getModifiers))
      if (
        captures.length != 1 || captures.head.getName != "stringPrefix$1" || captures.head.getType != java.lang.Integer.TYPE
      )
        denied("native statistics prefix shape changed")
      captures.head.setAccessible(true)
      Vector(
        function.getClass,
        field.getName,
        captured.getClass,
        captures.head.getName,
        captures.head.getInt(captured)
      )
    case other => other
  }

  private def denied(reason: String): Nothing = throw new SecurityException(
    "Delta native statistics recipe rejected: " + reason
  )
}
