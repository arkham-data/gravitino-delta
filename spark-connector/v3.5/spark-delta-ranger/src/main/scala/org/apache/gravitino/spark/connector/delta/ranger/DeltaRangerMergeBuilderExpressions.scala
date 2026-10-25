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

import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, LogicalPlan}

/** Parser and source-reference validation only; this helper issues no execution authority. */
private[ranger] final class DeltaRangerMergeBuilderExpressions(
    spark: SparkSession,
    source: DataFrame,
    analyzed: LogicalPlan
) {
  // Spark 3.5 Dataset.col attaches these metadata keys to resolved bound Columns. They are
  // validation evidence only: source authority still comes exclusively from the CDF receipt.
  private val datasetId = "__dataset_id"
  private val columnPosition = "__col_position"
  private lazy val boundOutput = analyzed.output.map { attribute =>
    val quoted = "`" + attribute.name.replace("`", "``") + "`"
    source.col(quoted).expr match {
      case bound: AttributeReference => bound
      case _ => throw new SecurityException("Delta MERGE source column is not an attribute")
    }
  }

  def expression(value: Any): Expression = value match {
    case text: String =>
      require(text.trim.nonEmpty, "Expression must not be empty")
      spark.sessionState.sqlParser.parseExpression(text)
    case column: Column =>
      column.expr.transformDown { case attribute: AttributeReference =>
        bind(attribute)
      }
    case _ => throw new IllegalArgumentException("Expression must be a non-null String or Column")
  }

  def assignments(values: JMap[String, _]): Vector[Assignment] = {
    require(values != null && !values.isEmpty, "Nonempty assignment map required")
    val names = scala.collection.mutable.ArrayBuffer.empty[String]
    values.asScala.toVector.map { case (key, value) =>
      require(key != null && key.trim.nonEmpty, "Assignment key is required")
      val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(key)
      require(parts.size == 1 && parts.head.nonEmpty, "Top-level assignment identifier required")
      require(
        !names.exists(spark.sessionState.conf.resolver(_, parts.head)),
        "Duplicate assignment key"
      )
      names += parts.head
      Assignment(UnresolvedAttribute(parts), expression(value))
    }
  }
  private def bind(attribute: AttributeReference): AttributeReference = {
    val position = analyzed.output.indexWhere(_.exprId == attribute.exprId)
    if (position < 0) throw new SecurityException("Delta MERGE foreign or missing source attribute")
    val actual = analyzed.output(position)
    val bound = boundOutput(position)
    if (
      attribute.name != actual.name || attribute.dataType != actual.dataType ||
      attribute.nullable != actual.nullable || attribute.qualifier != actual.qualifier ||
      !attribute.metadata.contains(datasetId) || !attribute.metadata.contains(columnPosition) ||
      !bound.metadata.contains(datasetId) || !bound.metadata.contains(columnPosition) ||
      attribute.metadata.getLong(datasetId) != bound.metadata.getLong(datasetId) ||
      attribute.metadata.getLong(columnPosition) != position.toLong ||
      bound.metadata.getLong(columnPosition) != position.toLong
    )
      throw new SecurityException("Delta MERGE foreign, stale or unbound Dataset column metadata")
    actual match {
      case result: AttributeReference => result
      case _ => throw new SecurityException("Delta MERGE source output is not an attribute")
    }
  }

}
