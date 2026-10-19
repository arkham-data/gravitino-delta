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

import java.lang.invoke.{MethodHandleInfo, SerializedLambda}
import java.lang.reflect.Modifier

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.sql.{Column, Encoders, SparkSession}
import org.apache.spark.sql.functions.{col, coalesce, struct, when}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.optimizer.ReassignLambdaVariableID
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  Expression,
  Alias,
  Ascending,
  ExprId,
  NamedExpression,
  ScalaUDF,
  SortOrder
}
import org.apache.spark.sql.catalyst.plans.logical.{
  LogicalPlan,
  MapPartitions,
  Project,
  RepartitionByExpression,
  Sort,
  Union
}
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex, Snapshot}
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Structural facts only; callers retain all source, operation, and execution authority. */
private[ranger] final class DeltaRangerMetadataStructure(rejectionPrefix: String) {
  def reconstruction(
      actual: LogicalPlan,
      spark: SparkSession,
      hasCheckpoint: Boolean,
      minRetention: Long,
      nowRetention: Long,
      minTxnRetention: Option[AnyRef],
      nowTxnRetention: Option[AnyRef],
      expectedHadoopConfiguration: Map[String, String],
      snapshotPartitions: Int,
      knownRecipes: Set[Vector[Any]]
  ): Vector[Any] = {
    val leaves = actual.collectLeaves()
    if (leaves.size != (if (!hasCheckpoint) 1 else 2))
      denied("reconstruction segment coverage changed")
    val formats = leaves.map(
      _.asInstanceOf[LogicalRelation].relation
        .asInstanceOf[HadoopFsRelation]
        .fileFormat
    )
    val expectedFormats =
      if (!hasCheckpoint) Seq(DeltaLogFileIndex.COMMIT_FILE_FORMAT)
      else
        Seq(DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET, DeltaLogFileIndex.COMMIT_FILE_FORMAT)
    if (formats != expectedFormats) denied("reconstruction segment order changed")
    val maps = actual.collect { case map: MapPartitions => map }
    if (maps.size != 1) denied("reconstruction function count changed")
    val replay = lambda(
      maps.head.func,
      classOf[Snapshot],
      "org/apache/spark/sql/delta/Snapshot",
      "scala/Function1",
      "$anonfun$stateReconstruction$2",
      "(Ljava/lang/Object;)Ljava/lang/Object;",
      "(JLscala/Option;Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      "(Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      2
    )
    replay.getCapturedArg(0) match {
      case time: java.lang.Long if time >= minRetention && time <= nowRetention =>
      case _ => denied("reconstruction file retention changed")
    }
    (minTxnRetention, replay.getCapturedArg(1), nowTxnRetention) match {
      case (None, None, None) =>
      case (Some(low: java.lang.Long), Some(time: java.lang.Long), Some(high: java.lang.Long))
          if low <= time && time <= high =>
      case _ => denied("reconstruction transaction retention changed")
    }
    val udfs = actual.flatMap(_.expressions.flatMap(_.collect { case udf: ScalaUDF => udf }))
    if (udfs.size != 2 || !(udfs.head.function eq udfs.last.function))
      denied("reconstruction canonical functions changed")
    val function = udfs.head.function
    val canonicalClass = Class.forName(
      "org.apache.spark.sql.delta.DeltaLog$CanonicalPathFunction",
      false,
      classOf[DeltaLog].getClassLoader
    )
    if (
      function.getClass != canonicalClass || field(function, "fs") != null ||
      field(function, "bitmap$trans$0") != java.lang.Boolean.FALSE
    )
      denied("reconstruction canonical function changed")
    val configuration = lambda(
      field(function, "getHadoopConf"),
      classOf[DeltaLog],
      "org/apache/spark/sql/delta/DeltaLog",
      "scala/Function0",
      "$anonfun$getCanonicalPathFunction$1",
      "()Ljava/lang/Object;",
      "(Lorg/apache/spark/broadcast/Broadcast;)Lorg/apache/hadoop/conf/Configuration;",
      "()Lorg/apache/hadoop/conf/Configuration;",
      1
    )
    val broadcast = configuration.getCapturedArg(0)
    val broadcastClass = Class.forName(
      "org.apache.spark.broadcast.TorrentBroadcast",
      false,
      classOf[Broadcast[_]].getClassLoader
    )
    if (broadcast.getClass != broadcastClass) denied("reconstruction configuration origin changed")
    val capturedConf = broadcast.asInstanceOf[Broadcast[SerializableConfiguration]].value
    if (
      capturedConf.getClass != classOf[SerializableConfiguration] ||
      capturedConf.value.iterator().asScala.map(e => e.getKey -> e.getValue).toMap !=
        expectedHadoopConfiguration
    )
      denied("reconstruction configuration changed")
    // Inputs, function descriptors/captures and configuration are rechecked above on every use.
    // An unchanged complete immutable recipe needs only one independent Analyzer construction.
    val recipe = DeltaRangerNativeProvenance.binding(actual)
    if (knownRecipes(recipe)) return recipe
    val canonical =
      DeltaRangerNativeOperation.canonicalPathFunction(function.asInstanceOf[String => String])
    val analyzer = new Analyzer(spark.sessionState.catalogManager)
    def analyzed(plan: LogicalPlan): LogicalPlan = analyzer.execute(plan)
    def withColumn(child: LogicalPlan, name: String, value: Column): LogicalPlan = {
      val replacement = value.as(name).expr.asInstanceOf[NamedExpression]
      val columns: Seq[NamedExpression] =
        if (child.output.exists(_.name == name))
          child.output.map(a => if (a.name == name) replacement else a)
        else child.output :+ replacement
      analyzed(Project(columns, child))
    }
    val base = if (leaves.size == 1) leaves.head else Union(leaves)
    val stats = withColumn(base, "add_stats_to_use", col("add.stats"))
    val addPath = withColumn(
      stats,
      "add_path_canonical",
      when(col("add.path").isNotNull, canonical(col("add.path")))
    )
    val removePath = withColumn(
      addPath,
      "remove_path_canonical",
      when(col("remove.path").isNotNull, canonical(col("remove.path")))
    )
    val partitioned = analyzed(
      RepartitionByExpression(
        Seq(coalesce(col("add_path_canonical"), col("remove_path_canonical")).expr),
        removePath,
        Some(snapshotPartitions)
      )
    )
    val sorted = analyzed(Sort(Seq(SortOrder(col("version").expr, Ascending)), false, partitioned))
    val added = withColumn(
      sorted,
      "add",
      when(
        col("add.path").isNotNull,
        struct(
          col("add_path_canonical").as("path"),
          col("add.partitionValues"),
          col("add.size"),
          col("add.modificationTime"),
          col("add.dataChange"),
          col("add_stats_to_use").as("stats"),
          col("add.tags"),
          col("add.deletionVector"),
          col("add.baseRowId"),
          col("add.defaultRowCommitVersion"),
          col("add.clusteringProvider")
        )
      )
    )
    val removed = withColumn(
      added,
      "remove",
      when(
        col("remove.path").isNotNull,
        col("remove").withField("path", col("remove_path_canonical"))
      )
    )
    val encoder = Encoders.product[SingleAction]
    val expected = analyzed(
      MapPartitions[SingleAction, SingleAction](
        maps.head.func.asInstanceOf[Iterator[SingleAction] => Iterator[SingleAction]],
        removed
      )(encoder, encoder)
    )
    if (!corresponds(expected, actual, base)) denied("complete reconstruction recipe changed")
    recipe
  }

  def corresponds(
      expected: LogicalPlan,
      actual: LogicalPlan,
      base: LogicalPlan
  ): Boolean = {
    def ids(value: Any): Set[ExprId] = value match {
      case id: ExprId  => Set(id)
      case seq: Seq[_] => seq.flatMap(ids).toSet
      case Some(v)     => ids(v)
      case _           => Set.empty
    }
    val retained = ids(DeltaRangerNativeProvenance.binding(base))
    val forward = mutable.Map.empty[ExprId, ExprId]
    val reverse = mutable.Map.empty[ExprId, ExprId]
    def same(left: Any, right: Any): Boolean = (left, right) match {
      case (a: ExprId, b: ExprId) if retained(a) || retained(b) => a == b
      case (a: ExprId, b: ExprId) =>
        forward.getOrElseUpdate(a, b) == b && reverse.getOrElseUpdate(b, a) == a
      case (a: Seq[_], b: Seq[_]) =>
        a.size == b.size && a.zip(b).forall { case (x, y) => same(x, y) }
      case (Some(a), Some(b)) => same(a, b)
      case _                  => left == right
    }
    same(
      recipeBinding(ReassignLambdaVariableID(expected)),
      recipeBinding(ReassignLambdaVariableID(actual))
    )
  }

  private def recipeBinding(value: Any): Any = value match {
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
        recipeBinding(alias.child),
        alias.name,
        alias.exprId,
        alias.qualifier.toVector,
        alias.explicitMetadata.map(_.json),
        alias.nonInheritableMetadataKeys.toVector
      )
    case expression: Expression =>
      Vector(expression.getClass, expression.productIterator.map(recipeBinding).toVector)
    case plan: LogicalPlan =>
      Vector(
        plan.getClass,
        plan.productIterator.map(recipeBinding).toVector,
        plan.output.map(recipeBinding).toVector
      )
    case encoder: ExpressionEncoder[_] =>
      Vector(
        encoder.getClass,
        recipeBinding(encoder.objSerializer),
        recipeBinding(encoder.objDeserializer),
        encoder.clsTag.runtimeClass
      )
    case pair: Tuple2[_, _] => Vector(pair.getClass, recipeBinding(pair._1), recipeBinding(pair._2))
    case tag: ClassTag[_]   => tag.runtimeClass
    case data: DataType     => data.json
    case metadata: Metadata => metadata.json
    case seq: Seq[_]        => seq.map(recipeBinding).toVector
    case Some(v)            => Some(recipeBinding(v))
    case array: Array[_]    => array.map(recipeBinding).toVector
    case text: UTF8String   => text.getBytes.toVector
    case other              => other
  }

  def fileStamp(file: FileStatus): Vector[Any] =
    Vector(file.getPath, file.getLen, file.getModificationTime, file.isDirectory)

  def field(value: AnyRef, name: String): AnyRef = {
    val member = value.getClass.getDeclaredField(name)
    member.setAccessible(true)
    member.get(value)
  }

  def lambda(
      value: AnyRef,
      origin: Class[_],
      implClass: String,
      interface: String,
      method: String,
      interfaceSignature: String,
      signature: String,
      instantiated: String,
      captures: Int
  ): SerializedLambda = {
    val actual = value.getClass
    val source = actual.getProtectionDomain.getCodeSource
    if (
      !actual.isSynthetic || !actual.isHidden || !Modifier.isFinal(actual.getModifiers) ||
      actual.getClassLoader != origin.getClassLoader || source == null ||
      source != origin.getProtectionDomain.getCodeSource ||
      !actual.getName.startsWith(implClass.replace('/', '.') + "$$Lambda$")
    )
      denied("nonnative cache function")
    val replacement = actual.getDeclaredMethod("writeReplace")
    replacement.setAccessible(true)
    val serialized = replacement.invoke(value) match {
      case result: SerializedLambda => result
      case _                        => denied("invalid cache function descriptor")
    }
    if (
      serialized.getCapturingClass != implClass || serialized.getImplClass != implClass ||
      serialized.getFunctionalInterfaceClass != interface ||
      serialized.getFunctionalInterfaceMethodName != "apply" ||
      serialized.getFunctionalInterfaceMethodSignature != interfaceSignature ||
      serialized.getImplMethodKind != MethodHandleInfo.REF_invokeStatic ||
      serialized.getImplMethodName != method || serialized.getImplMethodSignature != signature ||
      serialized.getInstantiatedMethodType != instantiated ||
      serialized.getCapturedArgCount != captures
    )
      denied("cache function descriptor changed")
    serialized
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException(rejectionPrefix + reason)
}
