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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.CachedData
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/** Issues CDF compatibility only for retained raw keys rebuilt by one actual append. */
private[ranger] object DeltaRangerCdfCacheMaintenance {
  private val active = new ThreadLocal[Scope]()
  private val property = "delta.enableChangeDataFeed"
  private val features = Some(Set("changeDataFeed", "appendOnly", "invariants"))

  def prepare(
      input: DeltaRangerAppendV1Input.RetainedV1Input,
      binding: DeltaRangerAppendCommandBinding,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget
  ): Option[Scope] = {
    if (input.association ne binding) reject("foreign append input")
    val head = target.log().unsafeVolatileSnapshot
    if (!cdfProtocol(head.protocol)) return None
    if (
      active.get() != null || DeltaRangerNativeOperation.current() != null ||
      OptimisticTransaction.getActive.nonEmpty
    ) reject("nested maintenance")
    input.requireLive()
    target.requireRetained()
    if (
      head.metadata != target.table().initialSnapshot.metadata ||
      head.protocol != target.table().initialSnapshot.protocol
    )
      reject("append target metadata changed")
    val scope = new Scope(input, binding, target, head)
    if (scope.hasKeys) Some(scope) else None
  }

  def authorize(plan: LogicalPlan, spark: SparkSession, index: TahoeLogFileIndex): Unit =
    Option(active.get()).foreach(_.authorize(plan, spark, index))

  def observation(spark: SparkSession, index: TahoeLogFileIndex): Option[Compatibility] =
    Option(active.get()).flatMap(_.observation(spark, index))

  private def cdfProtocol(protocol: Protocol): Boolean =
    protocol.minReaderVersion == 1 && protocol.minWriterVersion == 7 &&
      protocol.readerFeatures.isEmpty && protocol.writerFeatures == features

  private def ordinaryProtocol(protocol: Protocol): Boolean =
    protocol.minReaderVersion == 1 && protocol.minWriterVersion == 2 &&
      protocol.readerFeatures.isEmpty && protocol.writerFeatures.isEmpty

  final class Scope private[DeltaRangerCdfCacheMaintenance] (
      input: DeltaRangerAppendV1Input.RetainedV1Input,
      binding: DeltaRangerAppendCommandBinding,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      head: Snapshot
  ) {
    val spark = input.session
    private val thread = Thread.currentThread()
    private val settings = spark.sessionState.conf.getAllConfs
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val engine = target.engine()
    private val stamp = engine.snapshotStamp()
    private val sources = input.receipt.obligations.map(_.binding)
    private val sourceStamps =
      sources.map(source => source.engine() -> source.engine().snapshotStamp())
    private val sourcePlan = DeltaRangerNativeProvenance.binding(input.analyzed)
    private var published: Snapshot = null
    private var used = false
    private var expectedVersion: Option[Long] = None
    private var advanced = false
    private val log = target.log()
    private val selected = new IdentityHashMap[TahoeLogFileIndex, Compatibility]()
    private val keys = new IdentityHashMap[LogicalPlan, Any]()
    private var closed = false
    private val manager = spark.sharedState.cacheManager
    private val field = manager.getClass.getDeclaredField("cachedData")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[Seq[CachedData]].foreach { entry =>
      if (DeltaRangerRawMaintenance.raw(entry.plan)) {
        val relation = entry.plan.collectLeaves().head.asInstanceOf[LogicalRelation]
        val index =
          relation.relation.asInstanceOf[HadoopFsRelation].location.asInstanceOf[TahoeLogFileIndex]
        if ((index.deltaLog eq log) && index.path == tablePath)
          keys.put(entry.plan, DeltaRangerNativeProvenance.binding(entry.plan))
      }
    }
    def hasKeys: Boolean = !keys.isEmpty
    if (
      hasKeys && (spark.sessionState.conf.adaptiveExecutionEnabled ||
        spark.conf.get("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true").toBoolean ||
        spark.conf
          .get("spark.sql.optimizer.canChangeCachedPlanOutputPartitioning", "false")
          .toBoolean)
    )
      reject("cache recache requires a cloned session")
    private def tablePath = log.dataPath

    def complete(transaction: OptimisticTransaction, committed: Option[Long]): Unit = {
      if (
        closed || used || expectedVersion.nonEmpty || active.get() != null ||
        !OptimisticTransaction.getActive.exists(_ eq transaction) ||
        (transaction.deltaLog ne log) || transaction.readVersion != head.version ||
        transaction.snapshot.metadata != head.metadata || transaction.snapshot.protocol != head.protocol ||
        !transaction.catalogTable.exists(_ eq target.descriptor()) ||
        committed.exists(_ != head.version + 1)
      ) reject("foreign append completion")
      input.requireMaintenanceLease()
      expectedVersion = Some(committed.getOrElse(transaction.readVersion))
      advanced = committed.nonEmpty
    }

    def recache(): Unit = {
      if (
        used || closed || active.get() != null || DeltaRangerNativeOperation.current() != null ||
        OptimisticTransaction.getActive.nonEmpty
      ) reject("expired maintenance")
      used = true
      // Native checkpoint publication may replace the commit-time log segment at this version.
      // Observe it before raw selection freezes its exact descriptor/head/log-segment witness.
      target.validateCurrent(spark)
      published = log.unsafeVolatileSnapshot
      if (!expectedVersion.contains(published.version)) reject("unrelated native progression")
      active.set(this)
      try {
        requireLive()
        spark.sharedState.cacheManager.recacheByPlan(spark, target.table().toLogicalRelation)
        requireLive()
      } finally { closed = true; active.remove() }
    }

    def requireLive(): Unit = {
      if (
        closed || (active.get() ne this) || (Thread.currentThread() ne thread) ||
        !SparkSession.getActiveSession.exists(_ eq spark) ||
        (input.association ne binding)
      ) reject("expired maintenance")
      def context(): Unit = {
        val actualSettings = spark.sessionState.conf.getAllConfs
        val advanced = settings.updated(
          "spark.databricks.delta.lastCommitVersionInSession",
          published.version.toString
        )
        if (actualSettings != settings && !(this.advanced && actualSettings == advanced))
          reject("configuration changed")
        input.requireMaintenanceLease()
        if (DeltaRangerNativeProvenance.binding(input.analyzed) != sourcePlan)
          reject("source changed")
        val actual = DeltaRangerQuerySubject.current(spark)
        if (actual.user() != subject.user() || actual.groups() != subject.groups())
          reject("subject changed")
        engine.requireSnapshot(stamp)
        sourceStamps.foreach { case (sourceEngine, sourceStamp) =>
          sourceEngine.requireSnapshot(sourceStamp)
        }
        val current = log.unsafeVolatileSnapshot
        if (
          current.version != published.version || current.metadata != head.metadata ||
          current.protocol != head.protocol || (current.deltaLog ne log)
        ) reject("head changed")
      }
      context()
      target.requireRetained()
      sources.foreach { source =>
        source.requireRetained(); source.recheckPublication(); source.requireLive()
      }
      context()
    }

    def authorize(plan: LogicalPlan, session: SparkSession, index: TahoeLogFileIndex): Unit = {
      requireLive()
      if (
        (session ne spark) || !keys.containsKey(plan) ||
        keys.get(plan) != DeltaRangerNativeProvenance.binding(plan) ||
        (index.deltaLog ne log) || index.path != tablePath
      ) reject("unowned cache key")
      val witness = new Compatibility(index, log, published.version, head.metadata, head.protocol)
      witness.requireCurrent(index, log.unsafeVolatileSnapshot)
      selected.put(index, witness)
    }

    def observation(session: SparkSession, index: TahoeLogFileIndex): Option[Compatibility] = {
      requireLive()
      if (session ne spark) reject("foreign session")
      Option(selected.get(index))
    }
  }

  /** Immutable compatibility, usable only inside the existing exact installed cache generation. */
  final class Compatibility private[DeltaRangerCdfCacheMaintenance] (
      index: TahoeLogFileIndex,
      log: DeltaLog,
      version: Long,
      metadata: Metadata,
      protocol: Protocol
  ) {
    private val descriptor = index.snapshotAtAnalysis
    private val beforeMetadata = descriptor.metadata
    private val beforeProtocol = descriptor.protocol
    def requireCurrent(actual: TahoeLogFileIndex, head: Snapshot): Unit = {
      if (
        (actual ne index) || (actual.snapshotAtAnalysis ne descriptor) ||
        (actual.deltaLog ne log) || (head.deltaLog ne log) || head.version != version ||
        head.metadata != metadata || head.protocol != protocol ||
        descriptor.version < 0 || descriptor.version > version ||
        descriptor.metadata != beforeMetadata || descriptor.protocol != beforeProtocol ||
        !cdfProtocol(protocol) || !(cdfProtocol(beforeProtocol) || ordinaryProtocol(
          beforeProtocol
        )) ||
        !Set("true", "false")(metadata.configuration.getOrElse(property, "false")) ||
        !Set("true", "false")(beforeMetadata.configuration.getOrElse(property, "false")) ||
        beforeMetadata.copy(configuration = beforeMetadata.configuration - property) !=
          metadata.copy(configuration = metadata.configuration - property)
      )
        reject("descriptor compatibility changed")
    }
  }

  private def reject(reason: String): Nothing =
    throw new SecurityException("CDF cache maintenance rejected: " + reason)
}
