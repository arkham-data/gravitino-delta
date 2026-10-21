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
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile, Metadata, Protocol}
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.BoundCdfRead

/** Complete native history retained on the driver for one bounded reconstruction. */
private[ranger] object DeltaRangerCdfHistory {
  import DeltaRangerCdfRead.denied

  final class ValidatedInterval private[DeltaRangerCdfHistory] (
      val binding: BoundCdfRead,
      val request: DeltaRangerCdfRead.Request,
      val readSnapshot: Snapshot,
      val startSnapshot: Snapshot,
      private val actions: Vector[(Long, Seq[Action])]
  ) {
    private var consumer: DeltaRangerCdfReadScans = null
    private[ranger] def versionOf(action: Action, owner: DeltaRangerCdfReadScans): Long = {
      owner.requireInterval(this)
      if (consumer ne owner) denied("foreign action inventory owner")
      val matches = actions.collect {
        case (version, entries) if entries.exists(_ eq action) => version
      }
      if (matches.size != 1) denied("action is not from the validated interval")
      matches.head
    }
    private[ranger] def consume(owner: DeltaRangerCdfReadScans): Iterator[(Long, Seq[Action])] = {
      owner.requireInterval(this)
      if (consumer != null) denied("history already consumed")
      consumer = owner
      actions.iterator
    }
  }

  private[ranger] def consume(
      changes: Iterator[(Long, Seq[Action])],
      start: Long,
      end: Long
  ): Vector[(Long, Seq[Action])] = {
    if (start < 0L || end < start) denied("invalid interval")
    val result = Vector.newBuilder[(Long, Seq[Action])]
    var expected = start
    var complete = false
    while (!complete) {
      if (!changes.hasNext) denied("missing terminal history")
      val entry = changes.next()
      if (entry._1 != expected) denied("noncontiguous history")
      result += entry._1 -> entry._2.toVector
      if (expected == end) complete = true else expected += 1L
    }
    result.result()
  }

  def capture(
      binding: BoundCdfRead,
      request: DeltaRangerCdfRead.Request,
      spark: SparkSession
  ): ValidatedInterval = {
    binding.validateCurrent(spark)
    requireSettings(spark)
    val anchor = binding.anchor()
    val log = anchor.log()
    val read = log.unsafeVolatileSnapshot
    if (request.start < 0L || request.end < request.start || request.end > read.version)
      denied("invalid interval")
    val actions =
      consume(log.getChanges(request.start, failOnDataLoss = true), request.start, request.end)
    val start = DeltaRangerCdfReadScans.history(binding, request.start, spark)
    def profile(metadata: Metadata, protocol: Protocol): Unit = {
      if (
        metadata.id != read.metadata.id || metadata.schemaString != read.metadata.schemaString ||
        metadata.partitionColumns != read.metadata.partitionColumns || protocol != read.protocol
      )
        denied("historical schema or protocol changed")
      if (
        metadata.configuration.get("delta.columnMapping.mode").exists(_ != "none") ||
        metadata.configuration.get("delta.enableDeletionVectors").contains("true") ||
        metadata.configuration.get("delta.enableRowTracking").contains("true") ||
        protocol.minReaderVersion != 1 || protocol.readerFeatures.nonEmpty ||
        !((protocol.minWriterVersion <= 4 && protocol.writerFeatures.isEmpty) ||
          (protocol.minWriterVersion == 7 && protocol.writerFeatures == Some(
            Set("changeDataFeed", "appendOnly", "invariants")
          )))
      )
        denied("unsupported historical profile")
      if (
        metadata.schema.fieldNames.exists(name =>
          Set("_change_type", "_commit_version", "_commit_timestamp")(
            name.toLowerCase(java.util.Locale.ROOT)
          )
        )
      )
        denied("virtual column collision")
    }
    profile(read.metadata, read.protocol)
    profile(start.metadata, start.protocol)
    var metadata = start.metadata
    var protocol = start.protocol
    actions.foreach { case (_, entries) =>
      entries.foreach {
        case value: Metadata => metadata = value
        case value: Protocol => protocol = value
        case _               =>
      }
      profile(metadata, protocol)
      if (!metadata.configuration.get("delta.enableChangeDataFeed").contains("true"))
        denied("CDF disabled within requested interval")
      entries.foreach {
        case add: AddFile
            if add.deletionVector != null || add.baseRowId.nonEmpty || add.defaultRowCommitVersion.nonEmpty =>
          denied("unsupported historical add")
        case remove: RemoveFile
            if remove.deletionVector != null || remove.baseRowId.nonEmpty || remove.defaultRowCommitVersion.nonEmpty =>
          denied("unsupported historical remove")
        case _ =>
      }

    }
    binding.validateCurrent(spark)
    new ValidatedInterval(binding, request, read, start, actions)
  }

  private[ranger] def requireSettings(spark: SparkSession): Unit = {
    Seq(
      "spark.databricks.delta.catalog.update.enabled",
      "spark.sql.files.ignoreMissingFiles",
      "spark.sql.files.ignoreCorruptFiles",
      "spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled",
      "spark.databricks.delta.changeDataFeed.unsafeBatchReadOnIncompatibleSchemaChanges.enabled"
    ).foreach { key =>
      if (!spark.conf.get(key, "false").equalsIgnoreCase("false"))
        denied("permissive session setting")
    }
  }
}
