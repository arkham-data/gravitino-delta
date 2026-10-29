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

import java.util.IdentityHashMap

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{Column, Dataset, Encoders}
import org.apache.spark.sql.functions.{col, struct, from_json, to_json}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import org.apache.spark.sql.catalyst.analysis.Analyzer

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, MapElements, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.delta.{DeltaLog, DeltaLogFileIndex}

import org.apache.spark.sql.delta.actions.{Action, AddFile, SingleAction}
import org.apache.spark.sql.execution.{LogicalRDD, QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Finite metadata recipes owned by one selected raw installed head. */
private[ranger] final class DeltaRangerRawMetadata(owner: DeltaRangerRawNativeScans) {
  import DeltaRangerRawMetadata._
  private val structure = new DeltaRangerMetadataStructure("Raw metadata rejected: ")
  import structure._
  private val spark = owner.session()
  private val head = owner.observation.selectedHead
  private val segment = owner.observation.selectedSegment
  private val deltas = segment.deltas.map(fileStamp).toVector
  private val checkpoints = segment.checkpointProvider.topLevelFiles.map(fileStamp).toVector
  private val minRetention = DeltaRangerNativeOperation.retentionTimestamp(head)
  private val minTxnRetention = DeltaRangerNativeOperation.transactionRetentionTimestamp(head)
  private val qualifiedReconstructionRecipes = mutable.Set.empty[Vector[Any]]
  private val returned = ArrayBuffer.empty[(LogicalRDD, Vector[Any], Boolean, Vector[AnyRef])]
  private var getter: Option[Boolean] = None
  private val recipes = ArrayBuffer.empty[(LogicalPlan, Vector[Any])]
  private val executions = new IdentityHashMap[QueryExecution, Vector[Any]]()

  private def requireLive(): Unit = owner.requireLive()

  def appendMaintenance: Boolean = DeltaRangerCdfCacheMaintenance.owns(owner)

  def capture(): Unit = {
    requireLive()
    def get(stats: Boolean)(body: => Dataset[_]): Unit = {
      if (getter.nonEmpty) denied("nested raw getter")
      getter = Some(stats)
      val priorExecutions = executions.keySet().asScala.toSet
      try {
        val dataset = body
        val actual = dataset.queryExecution.analyzed
        actual match {
          case rdd: LogicalRDD =>
            if (
              !(dataset.sparkSession eq spark) || !DeltaRangerNativeOperation.stockCacheWrapper(
                rdd,
                spark
              )
            )
              denied("foreign raw cached return")
            if (!pending(rdd)) denied("raw cached getter output changed")
            val graph = verifyCache(rdd)
            val newExecutions = executions.keySet().asScala.filterNot(priorExecutions).toVector
            if (
              newExecutions.nonEmpty && !newExecutions.exists(qe =>
                (qe.toRdd eq rdd.rdd.dependencies.head.rdd) &&
                  DeltaRangerNativeProvenance.binding(qe.analyzed) == executions.get(qe)
              )
            )
              denied("new raw cache has no exact admitted QueryExecution parent")
            returned += ((rdd, DeltaRangerNativeProvenance.binding(rdd), stats, graph))
          case _ => denied("uncached raw getter is not qualified")
        }
      } finally getter = None
    }
    get(false)(head.stateDF)
    get(true)(head.withStats)
  }

  def contains(candidate: LogicalRDD, stats: Boolean): Boolean = {
    requireLive()
    returned.exists { case (original, frozen, kind, graph) =>
      kind == stats && (original.rdd eq candidate.rdd) &&
      verifyCache(original).zip(graph).forall { case (current, expected) => current eq expected } &&
      DeltaRangerNativeProvenance.binding(original) == frozen &&
      DeltaRangerNativeProvenance.binding(candidate) == frozen
    }
  }

  def pending(plan: LogicalPlan): Boolean = getter.nonEmpty && (plan match {
    case rdd: LogicalRDD if rdd.getClass == classOf[LogicalRDD] =>
      val schema =
        if (getter.contains(false)) Encoders.product[SingleAction].schema
        else
          org.apache.spark.sql.types.StructType(Encoders.product[AddFile].schema.fields.map { f =>
            if (f.name == "stats") f.copy(dataType = head.statsSchema, nullable = true) else f
          })
      rdd.schema == schema && DeltaRangerNativeOperation.stockCacheWrapper(rdd, spark)
    case _ => false
  })

  def ownsLog(fs: HadoopFsRelation, index: DeltaLogFileIndex): Boolean = {
    requireLive()
    if (index.getClass != classOf[DeltaLogFileIndex] || fs.getClass != classOf[HadoopFsRelation])
      return false
    val expected = head.deltaLog.indexToRelation(index).relation.asInstanceOf[HadoopFsRelation]
    val files = index.files.map(fileStamp).toVector
    index.getClass == classOf[DeltaLogFileIndex] && fs.getClass == classOf[HadoopFsRelation] &&
    fs.partitionSchema == index.partitionSchema && fs.dataSchema == Action.logSchema &&
    fs.bucketSpec.isEmpty && fs.fileFormat == index.format && fs.options == expected.options &&
    (fs.sparkSession eq spark) && index.rootPaths.toVector == index.files.map(_.getPath).toVector &&
    (((index.format eq DeltaLogFileIndex.COMMIT_FILE_FORMAT) && files == deltas) ||
      ((index.format eq DeltaLogFileIndex.CHECKPOINT_FILE_FORMAT_PARQUET) &&
        checkpoints.nonEmpty && files == checkpoints))
  }

  def analysis(plan: LogicalPlan): Boolean = {
    if (pending(plan)) return true
    val leaves = plan.collectLeaves()
    leaves.nonEmpty && leaves.forall {
      case r: LogicalRelation =>
        r.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex => ownsLog(fs, index)
              case _                        => false
            }
          case _ => false
        }
      case rdd: LogicalRDD => contains(rdd, false) || contains(rdd, true)
      case local: org.apache.spark.sql.catalyst.plans.logical.LocalRelation =>
        local.data.isEmpty && !local.isStreaming
      case _ => false
    } && !plan.exists(_.expressions.exists(_.exists {
      case _: org.apache.spark.sql.catalyst.expressions.SubqueryExpression => true
      case _: org.apache.spark.sql.catalyst.expressions.PythonUDF          => true
      case _                                                               => false
    }))
  }

  def executable(plan: LogicalPlan): Boolean = {
    try {
      verifyComplete(plan)
      val frozen = DeltaRangerNativeProvenance.binding(plan)
      if (!recipes.exists(_._2 == frozen)) recipes += ((plan.clone(), frozen))
      true
    } catch {
      case _: SecurityException =>
        recipes.exists { case (original, frozen) =>
          verifyComplete(original)
          if (DeltaRangerNativeProvenance.binding(original) != frozen)
            denied("raw metadata source recipe changed")
          val expected = DeltaRangerDecisionReceipt.nativePrefix(original.clone(), spark)
          corresponds(expected, plan, original)
        }
    }
  }

  private def verifyComplete(plan: LogicalPlan): Unit = {
    if (plan.collectLeaves().exists(_.isInstanceOf[LogicalRelation])) verifyReconstruction(plan)
    else verifyConsumer(plan)
  }

  def recordExecution(plan: SparkPlan): Unit = {
    requireLive()
    val id = Option(spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
      .getOrElse(denied("missing raw SQL execution"))
    val qe = SQLExecution.getQueryExecution(id.toLong)
    if (
      qe == null || !(qe.sparkSession eq spark) ||
      !DeltaRangerPhysicalProvenance.nodes(qe.executedPlan).exists(_ eq plan)
    )
      denied("foreign raw SQL execution")
    if (!executable(qe.analyzed)) denied("incomplete raw metadata execution")
    val frozen = DeltaRangerNativeProvenance.binding(qe.analyzed)
    if (executions.containsKey(qe) && executions.get(qe) != frozen)
      denied("raw metadata execution changed")
    executions.put(qe, frozen)
  }

  private def verifyConsumer(actual: LogicalPlan): Unit = {
    if (DeltaRangerPlanAdmission.rawAggregate(actual)) return
    val leaves = actual.collectLeaves()
    if (leaves.size != 1 || leaves.head.getClass != classOf[LogicalRDD])
      denied("unqualified raw consumer leaf")
    val cache = leaves.head.asInstanceOf[LogicalRDD]
    val analyzer = new Analyzer(spark.sessionState.catalogManager)
    def analyze(plan: LogicalPlan): LogicalPlan = analyzer.execute(plan)
    def withColumn(plan: LogicalPlan, name: String, expression: Column): LogicalPlan =
      analyze(
        Project(
          plan.output.map(a =>
            if (a.name == name)
              expression.as(name).expr.asInstanceOf[NamedExpression]
            else a
          ),
          plan
        )
      )
    val alternatives = ArrayBuffer.empty[LogicalPlan]
    if (contains(cache, false)) {
      val maps = actual.collect { case map: MapElements => map }
      if (maps.size != 1) denied("raw allFiles function count changed")
      val function = maps.head.func
      lambda(
        function,
        classOf[Dataset[_]],
        "org/apache/spark/sql/Dataset",
        "scala/Function1",
        "$anonfun$select$5",
        "(Ljava/lang/Object;)Ljava/lang/Object;",
        "(Lscala/Tuple1;)Ljava/lang/Object;",
        "(Lscala/Tuple1;)Ljava/lang/Object;",
        0
      )
      val add = cache.output.find(_.name == "add").getOrElse(denied("missing raw add action"))
      val selected = Project(Seq(add), Filter(IsNotNull(add), cache))
      val encoder = Encoders.product[AddFile].asInstanceOf[ExpressionEncoder[AddFile]]
      val tuple = ExpressionEncoder.tuple(encoder)
      val allFiles = analyze(
        MapElements[Tuple1[AddFile], AddFile](function, selected)(tuple, encoder)
      )
      alternatives += allFiles
      alternatives += withColumn(allFiles, "stats", from_json(col("stats"), head.statsSchema))
      alternatives += withColumn(
        allFiles,
        "stats",
        new Column(
          org.apache.spark.sql.catalyst.expressions.Literal
            .create(null, org.apache.spark.sql.types.StringType)
        )
      )
    } else if (contains(cache, true)) {
      alternatives += withColumn(
        cache,
        "stats",
        to_json(struct(col("stats.numRecords").as("numRecords")))
      )
    } else denied("unowned raw consumer")
    if (!alternatives.exists(expected => corresponds(expected, actual, cache)))
      denied("complete raw metadata consumer changed")
  }

  private def verifyCache(candidate: LogicalRDD): Vector[AnyRef] = {
    val rdd = candidate.rdd
    if (
      candidate.getClass != classOf[LogicalRDD] ||
      !DeltaRangerNativeOperation.stockCacheWrapper(candidate, spark) ||
      (rdd.sparkContext ne spark.sparkContext) ||
      !candidate.output.forall(_.getClass == classOf[AttributeReference]) ||
      candidate.output.map(_.exprId).distinct.size != candidate.output.size ||
      candidate.isStreaming || candidate.outputPartitioning != UnknownPartitioning(0) ||
      candidate.outputOrdering.nonEmpty || rdd.dependencies.size != 1 ||
      rdd.dependencies.head.getClass != classOf[OneToOneDependency[_]] ||
      !(field(rdd, "prev") eq rdd.dependencies.head.rdd) || rdd.partitioner.nonEmpty ||
      field(rdd, "isFromBarrier") != java.lang.Boolean.FALSE ||
      field(rdd, "isOrderSensitive") != java.lang.Boolean.FALSE
    ) denied("raw cached copy changed")
    val wrapper = lambda(
      field(rdd, "f"),
      classOf[RDD[_]],
      "org/apache/spark/rdd/RDD",
      "scala/Function3",
      "$anonfun$map$2$adapted",
      "(Ljava/lang/Object;Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
      "(Lscala/Function1;Lorg/apache/spark/TaskContext;Ljava/lang/Object;Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      "(Lorg/apache/spark/TaskContext;Ljava/lang/Object;Lscala/collection/Iterator;)Lscala/collection/Iterator;",
      1
    )
    lambda(
      wrapper.getCapturedArg(0),
      classOf[DeltaLog],
      "org/apache/spark/sql/delta/util/StateCache$CachedDS",
      "scala/Function1",
      "$anonfun$cachedDs$3",
      "(Ljava/lang/Object;)Ljava/lang/Object;",
      "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
      "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
      0
    )
    Vector(
      rdd.dependencies.head,
      rdd.dependencies.head.rdd,
      field(rdd, "f"),
      wrapper.getCapturedArg(0)
    )
  }

  def verifyReconstruction(actual: LogicalPlan): Unit = {
    requireLive()
    val leaves = actual.collectLeaves()
    leaves.foreach {
      case relation: LogicalRelation =>
        relation.relation match {
          case fs: HadoopFsRelation =>
            fs.location match {
              case index: DeltaLogFileIndex if ownsLog(fs, index) =>
              case _ => denied("reconstruction log input changed")
            }
          case _ => denied("reconstruction relation changed")
        }
      case _ => denied("reconstruction leaf changed")
    }
    qualifiedReconstructionRecipes += reconstruction(
      actual,
      spark,
      checkpoints.nonEmpty,
      minRetention,
      DeltaRangerNativeOperation.retentionTimestamp(head),
      minTxnRetention,
      DeltaRangerNativeOperation.transactionRetentionTimestamp(head),
      head.deltaLog.newDeltaHadoopConf().iterator().asScala.map(e => e.getKey -> e.getValue).toMap,
      DeltaRangerNativeOperation.snapshotPartitions(head),
      qualifiedReconstructionRecipes.toSet
    )
  }

}

private[ranger] object DeltaRangerRawMetadata {
  def current: Option[DeltaRangerRawMetadata] = Option(DeltaRangerNativeOperation.current())
    .flatMap(operation => Option(operation.rawOwner()))
    .map {
      case owner: DeltaRangerRawNativeScans => owner.metadata
      case _                                => denied("foreign raw owner")
    }
  private def denied(reason: String): Nothing =
    throw new SecurityException("Raw metadata rejected: " + reason)
}
