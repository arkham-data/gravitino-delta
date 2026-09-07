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
package org.apache.gravitino.spark.connector.integration.test.delta

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, ExprId}
import scala.collection.mutable
import org.apache.spark.sql.catalyst.plans.logical.{Command, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.delta.files.TahoeLogFileIndex

/** Read-only feasibility rule, not a production authorization boundary. */
class CatalogAwareRangerExtension extends (SparkSessionExtensions => Unit) {
  /** Installs the test rule through Spark's public extension interface. */
  override def apply(extensions: SparkSessionExtensions): Unit =
    extensions.injectResolutionRule(spark => new CatalogAwareReadRule(spark))
}

/** Evaluates the real engine before applying its expressions to native Delta scans. */
private class CatalogAwareReadRule(spark: SparkSession) extends Rule[LogicalPlan] {
  private val applied = TreeNodeTag[Boolean]("delta-feasibility-ranger-applied")

  /** Checks original column references and rewrites only a freshly analyzed read plan. */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.resolved) return plan
    if (plan.exists(_.isInstanceOf[Command])) {
      throw new SecurityException("Only SELECT is characterized by this Ranger candidate")
    }
    val referenced = plan.collect {
      case node if node.children.nonEmpty => node.references.toSeq
    }.flatten.map(_.exprId).toSet ++ plan.output.map(_.exprId)
    val replacements = mutable.Map.empty[ExprId, Attribute]
    plan.transformUp {
      // Delta reconstructs its transaction log with a separate metadata-only file index.
      // This is not an exemption for ordinary JSON/Parquet scans or arbitrary paths.
      case scan: LogicalRelation if scan.relation.isInstanceOf[HadoopFsRelation] &&
          scan.relation.asInstanceOf[HadoopFsRelation].location.isInstanceOf[DeltaLogFileIndex] => scan
      case scan: LogicalRelation if !scan.getTagValue(applied).contains(true) =>
        require(scan.relation.isInstanceOf[HadoopFsRelation] &&
          scan.relation.asInstanceOf[HadoopFsRelation].location.isInstanceOf[TahoeLogFileIndex],
          "Only native Delta data scans are characterized")
        val identity = scan.catalogTable.map(_.identifier).getOrElse {
          throw new SecurityException("Missing canonical Delta identity; path reads are rejected")
        }
        val catalog = identity.catalog.getOrElse {
          throw new SecurityException("Missing canonical catalog")
        }
        val database = identity.database.getOrElse {
          throw new SecurityException("Missing canonical database")
        }
        val table = identity.table
        val columns = scan.output.filter(attr => referenced.contains(attr.exprId))
        if (columns.isEmpty) LocalRangerEngine.authorize(catalog, database, table, "*")
        columns.foreach(attr => LocalRangerEngine.authorize(catalog, database, table, attr.name))
        val filter = Option(LocalRangerEngine.rowFilter(catalog, database, table))
        val filtered = filter.map(expr => Filter(spark.sessionState.sqlParser.parseExpression(expr), scan))
          .getOrElse(scan)
        val masked = scan.output.map { attr =>
          Option(LocalRangerEngine.mask(catalog, database, table, attr.name)).map { expr =>
            // A mask must receive a new expression ID: reusing the scan's ID allows
            // Spark's redundant-project optimization to discard the mask.
            val alias = Alias(spark.sessionState.sqlParser.parseExpression(expr), attr.name)(
              qualifier = attr.qualifier)
            replacements.put(attr.exprId, alias.toAttribute)
            alias
          }.getOrElse(attr)
        }
        scan.setTagValue(applied, true)
        Project(masked, filtered)
      case parent => parent.transformExpressions {
        case attr: AttributeReference if replacements.contains(attr.exprId) =>
          replacements(attr.exprId).withQualifier(attr.qualifier)
      }
    }
  }
}
