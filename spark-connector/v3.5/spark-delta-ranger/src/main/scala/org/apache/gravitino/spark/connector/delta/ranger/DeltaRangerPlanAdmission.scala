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

import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.optimizer.{ReassignLambdaVariableID, SimplifyCasts}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectSet, Count, Last, Sum}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex, Snapshot, SnapshotState}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.types.{
  BooleanType,
  IntegerType,
  LongType,
  ObjectType,
  StringType,
  StructType
}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Finite admission for fresh SELECT; native log reconstruction is a separate typed route. */
private[ranger] object DeltaRangerPlanAdmission {
  private class NativeScope(val indices: Seq[TahoeLogFileIndex]) {
    val statsSchemas = scala.collection.mutable.Set.empty[StructType]
    val stateRdds = scala.collection.mutable.ArrayBuffer.empty[LogicalRDD]
    val aggregateOutputs = scala.collection.mutable.ArrayBuffer.empty[Seq[Attribute]]
  }
  private val nativeScope = new ThreadLocal[NativeScope]()
  private lazy val actionSchema = Encoders.product[SingleAction].schema
  private lazy val snapshotStateEncoder =
    Encoders.product[SnapshotState].asInstanceOf[ExpressionEncoder[SnapshotState]]
  private lazy val snapshotStateSchema = snapshotStateEncoder.schema

  def withNativeMetadata[T](body: => T): T = withNativeScans(Seq.empty)(body)

  def withNativeScans[T](indices: Seq[TahoeLogFileIndex])(body: => T): T = {
    if (nativeScope.get() != null) throw new SecurityException("Nested native Delta metadata scope")
    nativeScope.set(new NativeScope(indices))
    try body
    finally nativeScope.remove()
  }

  def registerSnapshot(index: TahoeLogFileIndex, snapshot: Snapshot): Unit = {
    val scope = nativeScope.get()
    if (
      scope == null || !scope.indices.exists(_ eq index) || !(snapshot.deltaLog eq index.deltaLog)
    ) {
      throw new SecurityException("Native snapshot is outside authorized scan preparation")
    }
    // Capture native lineage before filesForScan can ask Snapshot for computedState. Reconstructing
    // stateDF may reenter the existing typed log route; aggregate admission never initializes it.
    snapshot.stateDF.queryExecution.analyzed match {
      case rdd: LogicalRDD if !rdd.isStreaming && rdd.schema == actionSchema =>
        scope.stateRdds += rdd
      case _ => throw new SecurityException("Native snapshot state lineage is not qualified")
    }
    scope.statsSchemas += StructType(Encoders.product[AddFile].schema.fields.map { field =>
      if (field.name == "stats") field.copy(dataType = snapshot.statsSchema, nullable = true)
      else field
    })
  }

  def query(plan: LogicalPlan): Unit = {
    if (!plan.resolved || plan.isStreaming) reject("unresolved or streaming plan")
    plan.foreach {
      case _: Project | _: Filter | _: SubqueryAlias | _: LogicalRelation  =>
      case join: Join if join.joinType == Inner && join.condition.nonEmpty =>
      case other                                                           => reject(other.nodeName)
    }
    plan.foreach {
      case _: LogicalRelation =>
      case node               => node.expressions.foreach(expression)
    }
  }

  def expression(expr: Expression): Unit = expr.foreach {
    case _: AttributeReference | _: Literal | _: Alias | _: Cast | _: EqualTo | _: EqualNullSafe |
        _: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual | _: And | _: Or |
        _: Not | _: IsNull | _: IsNotNull | _: In | _: Upper | _: Lower | _: Concat | _: Coalesce |
        _: Add | _: Subtract | _: Multiply | _: Divide =>
    case other => reject("expression " + other.prettyName)
  }

  def internalMetadata(plan: LogicalPlan): Boolean = {
    if (snapshotStateAggregate(plan) || snapshotStateBinding(plan)) return true
    plan match {
      case GlobalLimit(
            Literal(1, IntegerType),
            LocalLimit(Literal(1, IntegerType), aggregate: Aggregate)
          ) if snapshotStateAggregate(aggregate) =>
        return true
      case _ =>
    }
    plan match {
      case binding: DeserializeToObject
          if binding.deserializer.dataType == ObjectType(
            classOf[SingleAction]
          ) || binding.deserializer.dataType == ObjectType(classOf[Row]) ||
            (nativeScope
              .get() != null && binding.deserializer.dataType == ObjectType(classOf[AddFile])) =>
        binding.child match {
          case local: LocalRelation if local.data.isEmpty && !local.isStreaming =>
            return !binding.expressions.exists(_.exists {
              case _: ScalaUDF | _: PythonUDF | _: SubqueryExpression => true
              case _                                                  => false
            })
          case _ => return false
        }
      case _: DeserializeToObject => return false
      case _                      =>
    }
    val leaves = plan.collectLeaves()
    val nativeNodes = !plan.exists {
      case _: LogicalRelation | _: LogicalRDD | _: Project | _: Filter | _: Sort |
          _: RepartitionByExpression | _: SerializeFromObject | _: DeserializeToObject |
          _: MapPartitions | _: MapElements =>
        false
      case _ => true
    }
    leaves.nonEmpty && nativeNodes && !plan.exists(_.isInstanceOf[Command]) &&
    !plan.exists(_.expressions.exists(_.exists {
      case _: SubqueryExpression | _: PythonUDF => true
      case udf: ScalaUDF                        => !nativeUdf(udf, leaves)
      case _                                    => false
    })) &&
    leaves.forall {
      case rdd: LogicalRDD if nativeScope.get() != null =>
        !rdd.isStreaming && (rdd.schema == actionSchema || nativeScope
          .get()
          .statsSchemas(rdd.schema))
      case relation: LogicalRelation if !relation.isStreaming && relation.catalogTable.isEmpty =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex =>
                index.getClass == classOf[DeltaLogFileIndex] && fs.fileFormat == index.format &&
                index.rootPaths.nonEmpty && index.rootPaths.forall { path =>
                  path.getParent != null && path.getParent.getName == "_delta_log" &&
                  (nativeScope.get() == null || nativeScope
                    .get()
                    .indices
                    .exists(_.deltaLog.logPath == path.getParent)) &&
                  path.getName.matches(
                    "[0-9]{20}(\\.json|\\.checkpoint(\\.[0-9]{10}\\.[0-9]{10})?\\.parquet)"
                  )
                }
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def snapshotStateAggregate(plan: LogicalPlan): Boolean = plan match {
    case aggregate: Aggregate
        if nativeScope.get() != null && aggregate.groupingExpressions.isEmpty =>
      val child = aggregate.child match {
        case Project(columns, rdd: LogicalRDD)
            if columns == rdd.output.filter(attribute =>
              Set("txn", "add", "remove", "metaData", "protocol", "domainMetadata")
                .contains(attribute.name)
            ) =>
          rdd
        case other => other
      }
      child match {
        case rdd: LogicalRDD if !rdd.isStreaming && nativeScope.get().stateRdds.exists {
              registered =>
                (registered.rdd eq rdd.rdd) && registered.output == rdd.output
            } =>
          val attributes = rdd.output.map(attribute => attribute.name -> attribute).toMap
          val add = attributes("add")
          val size = GetStructField(add, add.dataType.asInstanceOf[StructType].fieldIndex("size"))
          val expected: Map[String, Expression] = Map(
            "sizeInBytes" -> Coalesce(Seq(Sum(size).toAggregateExpression(), Literal(0L))),
            "numOfSetTransactions" -> Count(Seq(attributes("txn"))).toAggregateExpression(),
            "numOfFiles" -> Count(Seq(add)).toAggregateExpression(),
            "numOfRemoves" -> Count(Seq(attributes("remove"))).toAggregateExpression(),
            "numOfMetadata" -> Count(Seq(attributes("metaData"))).toAggregateExpression(),
            "numOfProtocol" -> Count(Seq(attributes("protocol"))).toAggregateExpression(),
            "setTransactions" -> CollectSet(attributes("txn")).toAggregateExpression(),
            "domainMetadata" -> CollectSet(attributes("domainMetadata")).toAggregateExpression(),
            "metadata" -> Last(attributes("metaData"), ignoreNulls = true).toAggregateExpression(),
            "protocol" -> Last(attributes("protocol"), ignoreNulls = true).toAggregateExpression(),
            "numDeletedRecordsOpt" -> Literal(null),
            "numDeletionVectorsOpt" -> Literal(null),
            "fileSizeHistogram" -> Cast(
              Literal(null),
              snapshotStateSchema("fileSizeHistogram").dataType
            ),
            "deletedRecordCountsHistogramOpt" -> Cast(
              Literal(null),
              snapshotStateSchema("deletedRecordCountsHistogramOpt").dataType
            )
          )
          val aliases = aggregate.aggregateExpressions.collect { case alias: Alias => alias }
          val valid = aggregate.aggregateExpressions.size == expected.size &&
            aliases.size == expected.size && aliases.map(_.name).toSet == expected.keySet &&
            aliases.forall { alias =>
              alias.child.semanticEquals(expected(alias.name)) ||
              (Set("fileSizeHistogram", "deletedRecordCountsHistogramOpt").contains(alias.name) &&
                alias.child == Literal.create(null, snapshotStateSchema(alias.name).dataType))
            }
          if (valid && !nativeScope.get().aggregateOutputs.contains(aggregate.output)) {
            nativeScope.get().aggregateOutputs += aggregate.output
          }
          valid
        case _ => false
      }
    case _ => false
  }

  private def snapshotStateBinding(plan: LogicalPlan): Boolean = plan match {
    case binding: DeserializeToObject
        if nativeScope
          .get() != null && binding.deserializer.dataType == ObjectType(classOf[SnapshotState]) =>
      binding.child match {
        case local: LocalRelation
            if local.data.isEmpty && !local.isStreaming &&
              nativeScope.get().aggregateOutputs.contains(local.output) && !binding.deserializer
                .exists {
                  case _: ScalaUDF | _: PythonUDF | _: SubqueryExpression => true
                  case _                                                  => false
                } =>
          try {
            val expected =
              snapshotStateEncoder.resolveAndBind(local.output, SimpleAnalyzer).deserializer
            val simplified = SimplifyCasts(binding).asInstanceOf[DeserializeToObject].deserializer
            val actual = BindReferences.bindReference(simplified, AttributeSeq(local.output))
            val normalizedActual = ReassignLambdaVariableID(binding.copy(deserializer = actual))
              .asInstanceOf[DeserializeToObject]
              .deserializer
            val normalizedExpected = ReassignLambdaVariableID(binding.copy(deserializer = expected))
              .asInstanceOf[DeserializeToObject]
              .deserializer
            normalizedActual == normalizedExpected
          } catch {
            case scala.util.control.NonFatal(_) => false
          }
        case _ => false
      }
    case _ => false
  }

  private def nativeUdf(udf: ScalaUDF, leaves: Seq[LogicalPlan]): Boolean = {
    if (nativeScope.get() == null || !udf.nullable) return false
    val functionClass = udf.function.getClass
    val nativeClass = classOf[DeltaLog]
    val origin = functionClass.getProtectionDomain.getCodeSource
    if (
      origin == null || origin != nativeClass.getProtectionDomain.getCodeSource ||
      functionClass.getClassLoader != nativeClass.getClassLoader
    ) return false
    if (functionClass.getName == "org.apache.spark.sql.delta.DeltaLog$CanonicalPathFunction") {
      udf.function.isInstanceOf[Function1[_, _]] && udf.udfDeterministic &&
      udf.dataType == StringType && udf.inputPrimitives == Seq(false) &&
      udf.children.size == 1 && leaves.exists {
        case relation: LogicalRelation =>
          relation.relation match {
            case fs: HadoopFsRelation if fs.location.isInstanceOf[DeltaLogFileIndex] =>
              Seq("add", "remove").exists(field(udf.children.head, relation.output, _, "path"))
            case _ => false
          }
        case _ => false
      }
    } else if (
      functionClass.getName.startsWith(
        "org.apache.spark.sql.delta.stats.DataSkippingReaderBase$$Lambda$"
      )
    ) {
      udf.function.isInstanceOf[Function4[_, _, _, _, _]] && !udf.udfDeterministic &&
      udf.dataType == BooleanType && udf.inputPrimitives == Seq(true, false, false, false) &&
      udf.children.map(_.dataType) == Seq(BooleanType, LongType, LongType, LongType) &&
      leaves.exists {
        case rdd: LogicalRDD if nativeScope.get().statsSchemas(rdd.schema) =>
          val arguments = udf.children
          arguments.head.references.subsetOf(AttributeSet(rdd.output)) &&
          attribute(arguments(1), rdd.output, "size") &&
          logicalRows(arguments(2), rdd.output) &&
          field(arguments(3), rdd.output, "stats", "numRecords")
        case _ => false
      }
    } else false
  }

  private def attribute(expr: Expression, output: Seq[Attribute], name: String): Boolean =
    expr match {
      case attr: AttributeReference =>
        output.exists(a => a.name == name && a.exprId == attr.exprId && a.dataType == attr.dataType)
      case _ => false
    }

  private def field(
      expr: Expression,
      output: Seq[Attribute],
      parent: String,
      name: String
  ): Boolean = expr match {
    case value: GetStructField =>
      value.child.dataType match {
        case schema: StructType =>
          value.ordinal < schema.size &&
          schema(value.ordinal).name == name && attribute(value.child, output, parent)
        case _ => false
      }
    case _ => false
  }

  private def logicalRows(expr: Expression, output: Seq[Attribute]): Boolean = expr match {
    case alias: Alias => logicalRows(alias.child, output)
    case subtract: Subtract =>
      field(subtract.left, output, "stats", "numRecords") && (subtract.right match {
        case coalesce: Coalesce if coalesce.children.size == 2 =>
          field(coalesce.children.head, output, "deletionVector", "cardinality") &&
          coalesce.children(1) == Literal(0L)
        case _ => false
      })
    case _ => false
  }

  private def reject(shape: String): Nothing =
    throw new SecurityException("Unqualified Delta Ranger route: " + shape)
}
