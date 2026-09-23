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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String

/** Logical evidence only. Physical backing leaves and optimizer transitions need a later verifier.
  */
private[ranger] object DeltaRangerNativeProvenance {
  sealed trait Admission
  case object UserPlan extends Admission
  case object AnalysisOnlyNative extends Admission
  final class SealedNative private[DeltaRangerNativeProvenance] (val witness: Witness)
      extends Admission

  final class Reference(@transient private val authority: Witness) extends Serializable {
    private[DeltaRangerNativeProvenance] def get: Option[Witness] = Option(authority)
    override def toString: String = "Native Delta evidence reference"
  }

  final class Witness private[DeltaRangerNativeProvenance] (
      private val operation: DeltaRangerNativeOperation,
      private val recipe: Vector[Any],
      private val cache: Option[LogicalRDD],
      private val functions: Vector[Vector[Any]],
      private val inputs: Vector[Vector[Any]],
      private val projectionAcrossUnion: Boolean
  ) {
    def requireValid(plan: LogicalPlan, session: SparkSession): Unit = {
      operation.requireEvidence(session)
      if (
        recipe != binding(plan) || cache
          .exists(rdd => !operation.contains(rdd, false) && !operation.contains(rdd, true))
      ) {
        throw new SecurityException("Native Delta evidence does not bind this plan")
      }
    }
    def retainsFunctions(plan: LogicalPlan): Boolean =
      functionBindings(plan).diff(functions).isEmpty
    def permitsProjectionTransition(plan: LogicalPlan): Boolean =
      projectionAcrossUnion && logInputs(plan) == inputs && retainsFunctions(plan)
    def retainsNonCanonicalFunctions(plan: LogicalPlan): Boolean =
      functionBindings(plan)
        .filterNot(canonicalBinding)
        .diff(functions.filterNot(canonicalBinding))
        .isEmpty
    def coversInputs(plan: LogicalPlan): Boolean = {
      val actual = logInputs(plan)
      actual.nonEmpty && actual.diff(inputs).isEmpty
    }
    def invalidate(): Unit = operation.invalidate()
    override def toString: String = "Sealed native Delta evidence"
  }

  private val tag = TreeNodeTag[Reference]("gravitino.delta.ranger.native-evidence")

  def classify(plan: LogicalPlan): Admission = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (
      session.isEmpty || !safelyResolved(plan) || plan.isStreaming ||
      plan.exists(DeltaRangerDecisionReceipt.present)
    ) return UserPlan
    // Existing native evidence can survive qualified optimizer removals, but cannot mint a
    // replacement for a captured function occurrence, even during another owned invocation.
    // A copied child tag cannot authorize new function occurrences on its own. Only a captured
    // ancestor projection with the same exact native inputs and conserved function obligations
    // can reconcile canonical-path pushdown into that child; object functions remain strict.
    def retained(node: LogicalPlan, parentTransition: Option[Witness]): Boolean = {
      val prior = node.getTagValue(tag).flatMap(_.get)
      val transition = prior.filter(_.permitsProjectionTransition(node)).orElse(parentTransition)
      val own = prior.forall { witness =>
        witness.retainsFunctions(node) ||
        (witness.retainsNonCanonicalFunctions(node) &&
          parentTransition.exists(_.coversInputs(node)))
      }
      own && node.children.forall(child => retained(child, transition))
    }
    if (!retained(plan, None)) return UserPlan
    val operation = DeltaRangerNativeOperation.current()
    // Empty encoder resolution helpers are never executable tickets.
    if (DeltaRangerPlanAdmission.encoderBinding(plan)) return AnalysisOnlyNative
    if (operation != null) {
      operation.requireEvidence(session.get)
      if (DeltaRangerPlanAdmission.nativeShape(plan)) {
        val witness = new Witness(
          operation,
          binding(plan),
          None,
          functionBindings(plan),
          logInputs(plan),
          hasProjectionAcrossUnion(plan)
        )
        plan.setTagValue(tag, new Reference(witness))
        return new SealedNative(witness)
      }
      if (DeltaRangerPlanAdmission.pendingCache(plan)) {
        val witness = new Witness(
          operation,
          binding(plan),
          Some(plan.asInstanceOf[LogicalRDD]),
          functionBindings(plan),
          logInputs(plan),
          hasProjectionAcrossUnion(plan)
        )
        plan.setTagValue(tag, new Reference(witness))
        return AnalysisOnlyNative
      }
      return UserPlan
    }
    plan.getTagValue(tag).flatMap(_.get) match {
      case Some(witness) =>
        try {
          witness.requireValid(plan, session.get)
          new SealedNative(witness)
        } catch { case _: SecurityException => UserPlan }
      case _ => UserPlan
    }
  }

  def requireExecutable(plan: LogicalPlan, session: SparkSession): Witness =
    classify(plan) match {
      case sealedNative: SealedNative =>
        sealedNative.witness.requireValid(plan, session)
        sealedNative.witness
      case _ => throw new SecurityException("Native Delta plan has no sealed executable evidence")
    }

  private def safelyResolved(plan: LogicalPlan): Boolean =
    try plan.resolved
    catch { case scala.util.control.NonFatal(_) => false }

  private[ranger] def emptyPreparationBinding(plan: LogicalPlan): Option[Vector[Any]] = {
    def finiteEmpty(node: LogicalPlan): Boolean = node match {
      case project: Project     => finiteEmpty(project.child)
      case filter: Filter       => finiteEmpty(filter.child)
      case alias: SubqueryAlias => finiteEmpty(alias.child)
      case local: LocalRelation =>
        local.getClass == classOf[LocalRelation] && local.data.isEmpty && !local.isStreaming
      case _ => false
    }
    if (finiteEmpty(plan)) Some(binding(plan)) else None
  }

  private val canonicalMarker = "canonical path by exact native input and path role"

  private def canonicalBinding(binding: Vector[Any]): Boolean =
    binding.headOption.contains(canonicalMarker)

  private def canonicalRole(udf: ScalaUDF): Option[String] =
    if (
      udf.function.getClass.getName != "org.apache.spark.sql.delta.DeltaLog$CanonicalPathFunction"
    ) None
    else
      udf.children match {
        case Seq(field: GetStructField) =>
          field.child match {
            case attribute: AttributeReference
                if Set("add", "remove").contains(attribute.name) &&
                  field.child.dataType.isInstanceOf[org.apache.spark.sql.types.StructType] &&
                  field.ordinal >= 0 &&
                  field.ordinal < field.child.dataType
                    .asInstanceOf[org.apache.spark.sql.types.StructType]
                    .size &&
                  field.child.dataType
                    .asInstanceOf[org.apache.spark.sql.types.StructType](field.ordinal)
                    .name == "path" =>
              Some(attribute.name)
            case _ => None
          }
        case _ => None
      }

  private def logInputs(plan: LogicalPlan): Vector[Vector[Any]] =
    plan
      .collect {
        case relation: LogicalRelation if relation.relation.isInstanceOf[HadoopFsRelation] =>
          relation.relation.asInstanceOf[HadoopFsRelation]
      }
      .filter(_.location.isInstanceOf[DeltaLogFileIndex])
      .map { fs =>
        Vector(new Identity(fs), new Identity(fs.location))
      }
      .toVector

  private def hasProjectionAcrossUnion(plan: LogicalPlan): Boolean =
    plan.exists { node =>
      node.isInstanceOf[Project] && node.exists(_.isInstanceOf[Union]) &&
      node.expressions.exists(_.exists {
        case udf: ScalaUDF => canonicalRole(udf).nonEmpty
        case _             => false
      })
    }

  private def functionBindings(plan: LogicalPlan): Vector[Vector[Any]] =
    plan.flatMap { node =>
      val objects = node match {
        case value: MapPartitions =>
          Vector(
            Vector(value.getClass, value.output.map(_.exprId).toVector, new Identity(value.func))
          )
        case value: MapElements =>
          Vector(
            Vector(value.getClass, value.output.map(_.exprId).toVector, new Identity(value.func))
          )
        case _ => Vector.empty
      }
      objects ++ node.expressions.flatMap(_.collect { case udf: ScalaUDF => udf }).flatMap { udf =>
        canonicalRole(udf) match {
          case Some(role) =>
            // Before pushdown a projection covers both Union inputs; afterwards each branch
            // consumes its own obligation. Repeating a call on one input still adds an occurrence.
            logInputs(node).map { input =>
              Vector(canonicalMarker, new Identity(udf.function), input, role)
            }
          case _ => Vector(Vector(udf.getClass, new Identity(udf.function)))
        }
      }
    }.toVector

  private final class Identity(private val item: AnyRef) {
    override def equals(other: Any): Boolean = other match {
      case identity: Identity => item eq identity.item
      case _                  => false
    }
    override def hashCode(): Int = System.identityHashCode(item)
  }

  private def binding(plan: LogicalPlan): Vector[Any] = {
    val backing: Any = plan match {
      case rdd: LogicalRDD => new Identity(rdd.rdd)
      case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            Vector(
              new Identity(fs),
              new Identity(fs.location),
              new Identity(fs.fileFormat),
              fs.options,
              fs.dataSchema.json,
              fs.partitionSchema.json
            )
          case other => new Identity(other)
        }
      case _ => Vector.empty
    }
    Vector(
      plan.getClass,
      backing,
      plan.productIterator.map(value).toVector,
      plan.output.map(value).toVector
    )
  }

  private def value(item: Any): Any = item match {
    case null              => null
    case plan: LogicalPlan => binding(plan)
    case attr: AttributeReference =>
      Vector(
        attr.getClass,
        attr.name,
        attr.exprId,
        attr.qualifier,
        attr.dataType.json,
        attr.nullable,
        attr.metadata.json
      )
    case expr: Expression         => Vector(expr.getClass, expr.productIterator.map(value).toVector)
    case values: Seq[_]           => values.map(value).toVector
    case optional: Option[_]      => optional.map(value)
    case array: Array[_]          => array.map(value).toVector
    case text: UTF8String         => text.getBytes.toVector
    case dataType: DataType       => dataType.json
    case metadata: Metadata       => metadata.json
    case text: String             => text
    case number: java.lang.Number => number
    case flag: Boolean            => flag
    case id: ExprId               => id
    case other: AnyRef            => new Identity(other)
  }
}
