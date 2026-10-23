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

import org.apache.spark.sql.{Dataset, Encoders, Row}
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
  private def operation = DeltaRangerNativeOperation.current()
  private lazy val actionSchema = Encoders.product[SingleAction].schema
  private lazy val snapshotStateEncoder =
    Encoders.product[SnapshotState].asInstanceOf[ExpressionEncoder[SnapshotState]]
  private lazy val snapshotStateSchema = snapshotStateEncoder.schema
  private lazy val protocolMetadataEncoder =
    org.apache.spark.sql.delta.implicits.pmtvEncoder.asInstanceOf[ExpressionEncoder[_]]

  def registerSnapshot(index: TahoeLogFileIndex, snapshot: Snapshot): Unit = {
    if (operation == null) throw new SecurityException("Native snapshot lacks an owner")
    operation.capture(index, snapshot)
  }

  // Removed ambient entry points retain fail-closed behavior for callers compiled against the
  // earlier experimental surface. Ownership can only come from a registration or full receipt.
  def withNativeMetadata[T](body: => T): T =
    throw new SecurityException("Native metadata requires an owner")
  def withNativeScans[T](indices: Seq[TahoeLogFileIndex])(body: => T): T =
    throw new SecurityException("Native scans require a complete receipt")

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

  def internalMetadata(plan: LogicalPlan): Boolean =
    DeltaRangerNativeProvenance.classify(plan) != DeltaRangerNativeProvenance.UserPlan

  private[ranger] def rawAggregate(plan: LogicalPlan): Boolean =
    snapshotStateAggregate(plan) || snapshotStateBinding(plan) || (plan match {
      case GlobalLimit(
            Literal(1, IntegerType),
            LocalLimit(Literal(1, IntegerType), aggregate: Aggregate)
          ) =>
        snapshotStateAggregate(aggregate)
      case _ => false
    })

  private[ranger] def nativeShape(plan: LogicalPlan): Boolean = {
    DeltaRangerNativeOperation.requireNoRawSelection()
    DeltaRangerRawMetadata.current.foreach(owner => return owner.executable(plan))
    if (
      operation != null && operation.committing() &&
      DeltaRangerAppendCommit.current
        .map(_.metadata)
        .orElse(DeltaRangerCdfPropertyMutation.current.map(_.metadata))
        .orElse(DeltaRangerMergeCommit.current.map(_.metadata))
        .exists(_.checkpointPlan(plan))
    ) return true
    if (snapshotStateAggregate(plan) || snapshotStateBinding(plan)) return true
    plan match {
      case GlobalLimit(
            Literal(1, IntegerType),
            LocalLimit(Literal(1, IntegerType), aggregate: Aggregate)
          ) if snapshotStateAggregate(aggregate) =>
        return true
      case _ =>
    }
    if (operation != null && operation.transactionState()) {
      return plan match {
        case rdd: LogicalRDD if rdd.getClass == classOf[LogicalRDD] =>
          operation.contains(rdd, false)
        case _ => false
      }
    }
    plan match {
      case binding: DeserializeToObject
          if binding.deserializer.dataType == ObjectType(
            classOf[SingleAction]
          ) || binding.deserializer.dataType == ObjectType(classOf[Row]) ||
            (operation != null && binding.deserializer.dataType == ObjectType(classOf[AddFile])) =>
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
      case union: Union => !nativeLogUnion(union)
      case _: LogicalRelation | _: LogicalRDD | _: Project | _: Filter | _: Sort |
          _: RepartitionByExpression | _: SerializeFromObject | _: DeserializeToObject |
          _: MapPartitions | _: MapElements =>
        false
      case _ => true
    }
    val nativeFunctions = !plan.exists {
      case node: MapPartitions => !nativeObjectFunction(node.func)
      case node: MapElements   => !nativeAddFileSelect(node)
      case _                   => false
    }
    leaves.nonEmpty && nativeNodes && nativeFunctions && !plan.exists(_.isInstanceOf[Command]) &&
    !plan.exists(_.expressions.exists(_.exists {
      case _: SubqueryExpression | _: PythonUDF => true
      case udf: ScalaUDF                        => !nativeUdf(udf, leaves)
      case _                                    => false
    })) &&
    leaves.forall {
      case rdd: LogicalRDD if operation != null =>
        operation.contains(rdd, false) || operation.contains(rdd, true)
      case relation: LogicalRelation if !relation.isStreaming && relation.catalogTable.isEmpty =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex =>
                index.getClass == classOf[DeltaLogFileIndex] && fs.fileFormat == index.format &&
                (if (operation != null && operation.committing())
                   DeltaRangerAppendCommit.current
                     .map(_.metadata)
                     .orElse(DeltaRangerCdfPropertyMutation.current.map(_.metadata))
                     .orElse(DeltaRangerMergeCommit.current.map(_.metadata))
                     .exists(_.ownsLog(fs, index))
                 else if (operation != null && operation.cdfReadOwner() != null)
                   index.rootPaths.nonEmpty && index.rootPaths.forall(
                     operation.cdfReadOwner().ownsHistoryFile
                   )
                 else
                   index.rootPaths.nonEmpty && index.rootPaths.forall { path =>
                     path.getParent != null && path.getParent.getName == "_delta_log" &&
                     operation != null && operation.ownsLogRoot(path.getParent) &&
                     path.getName.matches(
                       "[0-9]{20}(\\.json|\\.checkpoint(\\.[0-9]{10}\\.[0-9]{10})?\\.parquet)"
                     )
                   })
              case _ => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  // Snapshot.loadActions combines its checkpoint and JSON-tail indices positionally. This is
  // only a log reconstruction union: each leaf must still pass the owned native-log checks above.
  private def nativeLogUnion(union: Union): Boolean = {
    val leaves = union.collectLeaves()
    val indices = leaves.flatMap {
      case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex if index.getClass == classOf[DeltaLogFileIndex] =>
                Some(index)
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
    union.getClass == classOf[Union] && !union.byName && !union.allowMissingCol &&
    union.children.size >= 2 && indices.size == leaves.size &&
    indices.flatMap(_.rootPaths).map(_.getParent).distinct.size == 1
  }

  private[ranger] def pendingCache(plan: LogicalPlan): Boolean = plan match {
    case rdd: LogicalRDD
        if operation != null && operation.capturingSnapshot() != null &&
          !rdd.isStreaming && rdd.getClass == classOf[LogicalRDD] =>
      val schema = if (operation.capturingStats()) {
        StructType(Encoders.product[AddFile].schema.fields.map { field =>
          if (field.name == "stats")
            field.copy(dataType = operation.capturingSnapshot().statsSchema, nullable = true)
          else field
        })
      } else actionSchema
      rdd.schema == schema
    case _ => false
  }

  private[ranger] def encoderBinding(plan: LogicalPlan): Boolean = plan match {
    case binding: DeserializeToObject =>
      binding.child match {
        case local: LocalRelation if local.data.isEmpty =>
          protocolMetadataBinding(binding, local) || nativeShape(plan)
        case _ => false
      }
    case _ => false
  }

  // Snapshot.protocolMetadataAndICTReconstruction binds its pinned tuple encoder against an
  // empty local relation during Dataset.as. This is analysis only, never a reconstruction ticket.
  private def protocolMetadataBinding(
      binding: DeserializeToObject,
      local: LocalRelation
  ): Boolean = {
    val owner = operation
    if (
      owner == null || !owner.active() ||
      (owner.callback() != DeltaRangerNativeOperation.Callback.LOAD &&
        owner.callback() != DeltaRangerNativeOperation.Callback.CDF_READ &&
        owner.callback() != DeltaRangerNativeOperation.Callback.VALIDATE_RETAINED_BINDING) || local.isStreaming ||
      !local.data.isEmpty || binding.deserializer.dataType != ObjectType(
        classOf[Tuple4[_, _, _, _]]
      ) ||
      local.output.map(_.name) != Seq("protocol", "metaData", "inCommitTimestamp", "version") ||
      local.output.map(_.dataType) != protocolMetadataEncoder.schema.fields.map(_.dataType).toSeq
    ) return false
    try {
      owner.requireEvidence(org.apache.spark.sql.SparkSession.active)
      val expected =
        protocolMetadataEncoder.resolveAndBind(local.output, SimpleAnalyzer).deserializer
      val actual = BindReferences.bindReference(
        SimplifyCasts(binding).asInstanceOf[DeserializeToObject].deserializer,
        AttributeSeq(local.output)
      )
      def normalized(expression: Expression): Expression =
        ReassignLambdaVariableID(binding.copy(deserializer = expression))
          .asInstanceOf[DeserializeToObject]
          .deserializer
      normalized(actual) == normalized(expected)
    } catch {
      case scala.util.control.NonFatal(_) => false
    }
  }

  private def snapshotStateAggregate(plan: LogicalPlan): Boolean = plan match {
    case aggregate: Aggregate if operation != null && aggregate.groupingExpressions.isEmpty =>
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
        case rdd: LogicalRDD if operation.contains(rdd, false) =>
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
          if (valid) operation.recordAggregate(aggregate.output)
          valid
        case _ => false
      }
    case _ => false
  }

  private def snapshotStateBinding(plan: LogicalPlan): Boolean = plan match {
    case binding: DeserializeToObject
        if operation != null && binding.deserializer.dataType == ObjectType(
          classOf[SnapshotState]
        ) =>
      binding.child match {
        case local: LocalRelation
            if local.data.isEmpty && !local.isStreaming &&
              operation.hasAggregate(local.output) && !binding.deserializer
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

  // Snapshot.allFiles selects the AddFile struct from the exact sealed state. Spark's typed
  // Dataset.select flattens its Tuple1 with a Dataset-owned function, not a Delta-owned closure.
  // Origin is only one issuance prerequisite; the encoder, field, filter and backing RDD are exact.
  private def nativeAddFileSelect(node: MapElements): Boolean = {
    val actual = node.func.getClass
    val expected = classOf[Dataset[_]]
    val encoder = ExpressionEncoder.tuple(
      Encoders.product[AddFile].asInstanceOf[ExpressionEncoder[AddFile]]
    )
    if (
      operation == null || actual.getProtectionDomain.getCodeSource == null ||
      actual.getProtectionDomain.getCodeSource != expected.getProtectionDomain.getCodeSource ||
      actual.getClassLoader != expected.getClassLoader ||
      !actual.getName.startsWith("org.apache.spark.sql.Dataset$$Lambda$") ||
      node.argumentClass != classOf[Tuple1[_]] || node.argumentSchema != encoder.schema ||
      node.outputObjAttr.dataType != ObjectType(classOf[AddFile])
    ) return false
    node.child match {
      case binding: DeserializeToObject =>
        binding.child match {
          case project @ Project(
                Seq(add: AttributeReference),
                Filter(IsNotNull(filtered), rdd: LogicalRDD)
              )
              if operation.contains(rdd, false) && filtered == add &&
                rdd.output.find(_.name == "add").contains(add) =>
            try {
              val expectedDeserializer =
                encoder.resolveAndBind(project.output, SimpleAnalyzer).deserializer
              val actualDeserializer = BindReferences.bindReference(
                SimplifyCasts(binding).asInstanceOf[DeserializeToObject].deserializer,
                AttributeSeq(project.output)
              )
              def normalized(expression: Expression): Expression =
                ReassignLambdaVariableID(binding.copy(deserializer = expression))
                  .asInstanceOf[DeserializeToObject]
                  .deserializer
              normalized(actualDeserializer) == normalized(expectedDeserializer)
            } catch {
              case scala.util.control.NonFatal(_) => false
            }
          case _ => false
        }
      case _ => false
    }
  }

  private def nativeObjectFunction(function: AnyRef): Boolean = {
    val actual = function.getClass
    val expected = classOf[DeltaLog]
    val origin = actual.getProtectionDomain.getCodeSource
    operation != null && origin != null && origin == expected.getProtectionDomain.getCodeSource &&
    actual.getClassLoader == expected.getClassLoader &&
    actual.getName.startsWith("org.apache.spark.sql.delta.")
  }

  private def nativeUdf(udf: ScalaUDF, leaves: Seq[LogicalPlan]): Boolean = {
    if (operation == null || !udf.nullable) return false
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
        case rdd: LogicalRDD if operation.contains(rdd, true) =>
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
