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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.commands.cdc.CDCReaderImpl
import org.apache.spark.sql.delta.files.TahoeFileIndexWithSnapshotDescriptor

/** Native reconstruction is invoked on this owned instance, never on the singleton relation. */
private[ranger] object DeltaRangerCdfNativeReader {
  def read(
      interval: DeltaRangerCdfHistory.ValidatedInterval,
      owner: DeltaRangerCdfReadScans,
      spark: SparkSession
  ): DataFrame = owner.run {
    val reader = new CDCReaderImpl {
      override def getNonICTTimestampsByVersion(
          log: org.apache.spark.sql.delta.DeltaLog,
          start: Long,
          end: Long
      ): Map[Long, java.sql.Timestamp] = {
        val result = super.getNonICTTimestampsByVersion(log, start, end)
        owner.timestamps(result)
        result
      }
      override protected def scanIndex(
          spark: SparkSession,
          index: TahoeFileIndexWithSnapshotDescriptor,
          isStreaming: Boolean = false
      ): DataFrame = {
        owner.issued(index)
        val result = super.scanIndex(spark, index, isStreaming)
        owner.emitted(index, result)
        result
      }
    }
    val result = reader
      .changesToDF(
        interval.readSnapshot,
        interval.request.start,
        interval.request.end,
        interval.consume(owner),
        spark,
        isStreaming = false,
        useCoarseGrainedCDC = false,
        startVersionSnapshot = Some(interval.startSnapshot)
      )
      .fileChangeDf
    owner.returned(result.queryExecution.analyzed)
    result
  }
}
