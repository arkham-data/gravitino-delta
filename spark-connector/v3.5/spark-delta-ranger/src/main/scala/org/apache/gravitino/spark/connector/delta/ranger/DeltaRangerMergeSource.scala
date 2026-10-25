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

import org.apache.spark.OneToOneDependency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Project, Statistics}
import org.apache.spark.sql.execution.{LogicalRDD, ProjectExec, SparkPlan, UnaryExecNode}

/** Exact native pruning/checkpoint ownership for one command attempt. */
private[ranger] object DeltaRangerMergeSource {
  import DeltaRangerMergeCommand.denied
  private val active = new ThreadLocal[Attempt]()

  def begin(permit: DeltaRangerMergeCommand.SourcePermit): Attempt = {
    val receipt = permit.consume()
    val result = new Attempt(receipt)
    receipt.openSourceAttempt(result, permit.number)
    result
  }
  def analysis(plan: LogicalPlan, spark: SparkSession): Boolean =
    DeltaRangerNativeOperation.current() == null && Option(active.get())
      .exists(_.analysis(plan, spark))
  def normalize(plan: LogicalPlan, spark: SparkSession): Option[LogicalPlan] =
    if (DeltaRangerNativeOperation.current() != null) None
    else Option(active.get()).flatMap(_.normalize(plan, spark))

  final class Attempt private[DeltaRangerMergeSource] (receipt: DeltaRangerMergeReceipt) {
    val spark = receipt.spark
    private val thread = Thread.currentThread()
    private val structure = new DeltaRangerMetadataStructure("Delta MERGE checkpoint rejected: ")
    import structure.{field, lambda}
    private val expected = Project(receipt.materializedColumns, receipt.cdf.carrier)
    private var closed = false
    private var entered = false
    private var carrier: Carrier = null
    private var root: SourceExec = null
    private var physical: Vector[Any] = null
    private var sourceRdd: RDD[InternalRow] = null
    private var pending: LogicalRDD = null
    private var issued: RDD[InternalRow] = null
    private var nativePlan: LogicalPlan = null
    private var prepared = false
    private var installed = false
    private var started = false

    def validate(): Unit = {
      if (closed || (Thread.currentThread() ne thread) || (SparkSession.active ne spark))
        denied("expired source attempt")
      receipt.requireSourceAttempt(this)
      receipt.validate()
    }
    private def requireConstructing(): Unit = {
      validate()
      if (!entered || (active.get() ne this)) denied("inactive native materialization")
    }
    def materialize(body: => Unit): Unit = {
      validate()
      if (entered || active.get() != null) denied("repeated native materialization")
      entered = true
      active.set(this)
      try { body; validate() }
      finally active.remove()
    }
    private def projection(plan: LogicalPlan): Boolean = plan match {
      case Project(columns, child) =>
        (child eq receipt.cdf.carrier) && columns == expected.projectList
      case _ => false
    }
    def analysis(plan: LogicalPlan, session: SparkSession): Boolean = {
      requireConstructing()
      if (session ne spark) denied("source analysis session changed")
      if (projection(plan)) return true
      plan match {
        case own: Carrier => check(own); true
        case rdd: LogicalRDD =>
          if (pending == null) {
            // Dataset.localCheckpoint copies rows once from the exact issued projected RDD.
            // No matching schema, path or unrelated checkpoint can confer this ownership.
            if (
              sourceRdd == null || rdd.output != expected.output || rdd.isStreaming ||
              rdd.rdd.getClass != Class.forName("org.apache.spark.rdd.MapPartitionsRDD") ||
              rdd.rdd.dependencies.size != 1 || (rdd.rdd.dependencies.head.rdd ne sourceRdd)
            )
              denied("foreign native checkpoint RDD")
            qualifyCopy(rdd.rdd)
            pending = rdd
          }
          (rdd eq pending) && (rdd.rdd eq pending.rdd) && rdd.output == expected.output
        case _ => false
      }
    }
    private def qualifyCopy(rdd: RDD[InternalRow]): Unit = {
      if (
        rdd.dependencies.head.getClass != classOf[OneToOneDependency[_]] ||
        (field(rdd, "prev") ne sourceRdd) || rdd.partitioner.nonEmpty ||
        field(rdd, "isFromBarrier") != java.lang.Boolean.FALSE ||
        field(rdd, "isOrderSensitive") != java.lang.Boolean.FALSE
      )
        denied("foreign checkpoint copy parent")
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
        classOf[Dataset[_]],
        "org/apache/spark/sql/Dataset",
        "scala/Function1",
        "$anonfun$checkpoint$2",
        "(Ljava/lang/Object;)Ljava/lang/Object;",
        "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
        "(Lorg/apache/spark/sql/catalyst/InternalRow;)Lorg/apache/spark/sql/catalyst/InternalRow;",
        0
      )
    }
    def normalize(plan: LogicalPlan, session: SparkSession): Option[LogicalPlan] = {
      requireConstructing()
      if (!projection(plan)) return None
      if (session ne spark) denied("source normalization session changed")
      if (carrier == null) carrier = Carrier(expected.output)(this)
      Some(carrier)
    }
    def check(candidate: Carrier): Unit = {
      requireConstructing()
      if ((candidate ne carrier) || candidate.output != expected.output)
        denied("copied source projection")
    }
    def plan(candidate: Carrier): SparkPlan = {
      check(candidate)
      if (root != null) denied("source projection planned twice")
      val cdf = receipt.cdf.plan(receipt.cdf.carrier)
      // Issue the final recipe without an exact identity projection. Spark removes that
      // projection during physical preparation; freezing it would reject the native successor.
      val child =
        if (receipt.materializedColumns == cdf.output) cdf
        else ProjectExec(receipt.materializedColumns, cdf)
      physical = DeltaRangerPhysicalProvenance.binding(child)
      root = SourceExec(child)(this)
      root
    }
    private def producer(child: SparkPlan): DeltaRangerCdfProducer = {
      val producers = DeltaRangerPhysicalProvenance.nodes(child).collect {
        case cdf: DeltaRangerCdfProducer => cdf
      }
      if (producers.size != 1 || (producers.head.receipt ne receipt.cdf))
        denied("foreign source producer")
      producers.head
    }
    def verify(candidate: SourceExec): Unit = {
      requireConstructing()
      if (
        (candidate ne root) || physical == null ||
        DeltaRangerPhysicalProvenance.binding(candidate.child) != physical
      ) denied("source physical substitution")
    }
    def before(candidate: SourceExec): Unit = {
      verify(candidate)
      if (prepared || installed) denied("source preparation repeated")
      receipt.cdf.beforeTransitions(producer(candidate.child))
      prepared = true
    }
    def after(candidate: SourceExec): Unit = {
      verify(candidate)
      if (!prepared || installed) denied("source installation repeated")
      receipt.cdf.afterTransitions(producer(candidate.child))
      installed = true
    }
    def replace(previous: SourceExec, child: SparkPlan): SourceExec = {
      verify(previous)
      if (child eq previous.child) return previous
      if (started || DeltaRangerPhysicalProvenance.binding(child) != physical)
        denied("changed source transition")
      root = SourceExec(child)(this)
      root
    }
    def execute(candidate: SourceExec): RDD[InternalRow] = {
      verify(candidate)
      if (!installed) denied("source projection not installed")
      started = true
      if (sourceRdd == null) sourceRdd = candidate.child.execute()
      sourceRdd
    }
    def returned(plan: LogicalPlan, rdd: Option[RDD[InternalRow]]): Unit = {
      requireConstructing()
      if (
        issued != null || pending == null || rdd.isEmpty || (rdd.get ne pending.rdd) ||
        (plan ne pending) || !rdd.get.isCheckpointed
      ) denied("native checkpoint successor changed")
      issued = rdd.get
      nativePlan = plan
    }
    def logical: LogicalPlan = {
      validate()
      if (nativePlan == null) denied("missing native checkpoint")
      nativePlan
    }
    def ownsRetained(plan: LogicalPlan): Boolean = {
      if (closed || (Thread.currentThread() ne thread) || (SparkSession.active ne spark))
        denied("expired source attempt")
      issued != null && issued.isCheckpointed && (plan eq nativePlan) &&
      plan.asInstanceOf[LogicalRDD].rdd.eq(issued)
    }
    def owns(plan: LogicalPlan): Boolean = { validate(); ownsRetained(plan) }
    def requireExecution(): Unit = requireConstructing()
    def isClosed: Boolean = closed
    def close(): Unit = {
      if (!closed) {
        closed = true
        receipt.closeSourceAttempt(this)
      }
    }
  }

  final case class Carrier(override val output: Seq[Attribute])(
      @transient private val authority: Attempt
  ) extends LeafNode {
    def owner: Attempt = {
      if (authority == null || isCanonicalizedPlan) denied("inert source projection")
      authority.check(this); authority
    }
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): LogicalPlan = this
    override def computeStats(): Statistics = Statistics(sizeInBytes = BigInt(Long.MaxValue))
  }
  final case class SourceExec(child: SparkPlan)(@transient private val authority: Attempt)
      extends UnaryExecNode {
    def owner: Attempt = {
      if (authority == null || isCanonicalizedPlan) denied("inert source physical projection")
      authority.verify(this); authority
    }
    override def output: Seq[Attribute] = child.output
    override def outputPartitioning = child.outputPartitioning
    override def outputOrdering = child.outputOrdering
    override protected def otherCopyArgs: Seq[AnyRef] = Seq(authority)
    override def clone(): SparkPlan = this
    override def executeCollect(): Array[InternalRow] = denied(
      "CDF payload collection inside MERGE"
    )
    override protected def doExecute(): RDD[InternalRow] = owner.execute(this)
    override protected def withNewChildInternal(child: SparkPlan): SourceExec =
      owner.replace(this, child)
  }
}
