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

import java.sql.SQLException
import java.util.{IdentityHashMap, Properties, UUID}
import scala.collection.JavaConverters._
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.collection.mutable
import scala.util.control.NonFatal
import org.apache.gravitino.spark.connector.GravitinoSparkConfig
import org.slf4j.LoggerFactory
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DeltaRangerExternalJdbcNative.{Relation => JDBCRelation}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JdbcRelationProvider}

/** Private issuance proves native source construction, never Ranger or Delta storage authority.
  *
  * Stock BasicConnectionProvider logging must stay above DEBUG/TRACE. The trusted runtime owns
  * logging configuration during native IO; validation cannot make concurrent reconfiguration
  * atomic.
  */
private[ranger] object DeltaRangerExternalJdbcSources {
  private val nativeProviderLog = LoggerFactory.getLogger(
    "org.apache.spark.sql.execution.datasources.jdbc.connection.BasicConnectionProvider"
  )
  private val lock = new Object
  private val issued = new IdentityHashMap[JDBCRelation, BoundInput]()
  private val cleanup = new ScheduledThreadPoolExecutor(
    1,
    new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "delta-external-jdbc-expiry")
        thread.setDaemon(true)
        thread
      }
    }
  )
  cleanup.setRemoveOnCancelPolicy(true)

  def open(spark: SparkSession, lifetimeSeconds: Long): Scope =
    openWithClock(spark, lifetimeSeconds, () => System.nanoTime())

  // Bounded monotonic clock seam; neither provider construction nor issuance is injectable.
  private[ranger] def openWithClock(
      spark: SparkSession,
      lifetimeSeconds: Long,
      clock: () => Long
  ): Scope = {
    if (lifetimeSeconds < 1 || lifetimeSeconds > 86400)
      throw new IllegalArgumentException("External JDBC lifetime must be 1–86400 seconds")
    requireActive(spark)
    val startup = spark.sparkContext.getConf
    Seq(
      GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT,
      GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_SUPPORT,
      GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_EXTERNAL_JDBC_SUPPORT
    ).foreach { key => if (startup.get(key, "false") != "true") denied() }
    val scope = new Scope(spark, lifetimeSeconds, clock)
    scope.start()
    scope
  }

  def resolve(relation: JDBCRelation, spark: SparkSession): BoundInput = {
    val bound = lock.synchronized { issued.get(relation) }
    if (bound == null) denied()
    bound.requireCurrent(spark)
    bound
  }

  final class Scope private[DeltaRangerExternalJdbcSources] (
      private val spark: SparkSession,
      lifetimeSeconds: Long,
      clock: () => Long
  ) extends AutoCloseable {
    private val started = clock()
    private val lifetime = TimeUnit.SECONDS.toNanos(lifetimeSeconds)
    private val subject = DeltaRangerQuerySubject.current(spark)
    private val configuration = spark.conf.getAll
    private val effective = spark.sessionState.conf.getAllDefinedConfs.map(v => v._1 -> v._2).toMap
    private val bindings = mutable.ArrayBuffer.empty[BoundInput]
    private var closed = false
    private var timer: ScheduledFuture[_] = _
    private val listener = new SparkListener {
      override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = close()
    }

    private[DeltaRangerExternalJdbcSources] def start(): Unit = lock.synchronized {
      spark.sparkContext.addSparkListener(listener)
      timer = cleanup.schedule(
        new Runnable {
          override def run(): Unit = close()
        },
        lifetimeSeconds,
        TimeUnit.SECONDS
      )
      if (spark.sparkContext.isStopped) close()
    }

    def load(options: java.util.Map[String, String]): BoundInput = {
      val copied = DeltaRangerExternalJdbcOptions.copyAndValidate(options)
      requireLive()
      val issue = UUID.randomUUID().toString
      // Schema discovery may perform network IO; no cleanup lock is held here.
      val relation =
        try {
          new JdbcRelationProvider()
            .createRelation(spark.sqlContext, copied)
            .asInstanceOf[JDBCRelation]
        } catch {
          case NonFatal(failure) =>
            val state = Iterator
              .iterate(failure)(_.getCause)
              .take(16)
              .takeWhile(_ != null)
              .collectFirst {
                case sql: SQLException
                    if Option(sql.getSQLState).exists(_.matches("[0-9A-Z]{5}")) =>
                  sql.getSQLState
              }
            throw new IllegalStateException(
              "External JDBC issue " + issue + " native construction failed" +
                state.map(value => " (SQLSTATE " + value + ")").getOrElse(" (native provider)")
            )
        }
      lock.synchronized {
        requireLive()
        val bound = new BoundInput(this, relation, issue)
        bindings += bound
        issued.put(relation, bound)
        bound
      }
    }

    def requireLive(): Unit = lock.synchronized {
      if (closed) denied()
      if (spark.sparkContext.isStopped || clock() - started >= lifetime) {
        close()
        denied()
      }
      requireActive(spark)
      requireSafeLogging()
      val current = DeltaRangerQuerySubject.current(spark)
      if (
        current.user() != subject.user() || current.groups() != subject.groups() ||
        configuration != spark.conf.getAll ||
        effective != spark.sessionState.conf.getAllDefinedConfs.map(v => v._1 -> v._2).toMap
      ) denied()
    }

    override def close(): Unit = lock.synchronized {
      if (!closed) {
        closed = true
        bindings.foreach(bound => issued.remove(bound.relation))
        bindings.clear()
        if (timer != null) timer.cancel(false)
        spark.sparkContext.removeSparkListener(listener)
      }
    }
  }

  final class BoundInput private[DeltaRangerExternalJdbcSources] (
      private val owner: Scope,
      val relation: JDBCRelation,
      private val issue: String
  ) {
    private val schema = relation.schema.json
    private val options = relation.jdbcOptions.parameters.originalMap.toMap
    private val properties = snapshotProperties(relation.jdbcOptions.asProperties)
    private val connectionProperties = snapshotProperties(
      relation.jdbcOptions.asConnectionProperties
    )
    private val partitions = relation.parts.map {
      case part: JDBCPartition => (part.idx, part.whereClause)
      case _                   => denied()
    }.toVector

    override def toString: String = "External JDBC input " + issue

    def requireCurrent(spark: SparkSession): Unit = {
      requireActive(spark)
      if (relation.sqlContext.sparkSession ne spark) denied()
      owner.requireLive()
      requireSame(relation)
    }

    def requireSame(candidate: JDBCRelation): Unit = {
      owner.requireLive()
      if (
        (candidate ne relation) || schema != relation.schema.json ||
        options != relation.jdbcOptions.parameters.originalMap ||
        properties != snapshotProperties(relation.jdbcOptions.asProperties) ||
        connectionProperties != snapshotProperties(relation.jdbcOptions.asConnectionProperties) ||
        partitions != relation.parts.map {
          case part: JDBCPartition => (part.idx, part.whereClause)
          case _                   => denied()
        }.toVector
      ) denied()
    }
  }

  private def snapshotProperties(properties: Properties): Map[AnyRef, AnyRef] =
    properties.synchronized {
      properties.entrySet().asScala.iterator.map(entry => entry.getKey -> entry.getValue).toMap
    }

  private def requireSafeLogging(): Unit = {
    if (nativeProviderLog.isDebugEnabled || nativeProviderLog.isTraceEnabled)
      throw new SecurityException(
        "External JDBC requires native connection-provider logging above DEBUG/TRACE"
      )
  }

  private def requireActive(spark: SparkSession): Unit = {
    if (
      spark == null || spark.sparkContext.isStopped ||
      !SparkSession.getActiveSession.exists(_ eq spark)
    ) denied()
  }

  private def denied(): Nothing = throw new SecurityException(
    "External JDBC source is unissued, changed, or outside its live owning scope"
  )
}
