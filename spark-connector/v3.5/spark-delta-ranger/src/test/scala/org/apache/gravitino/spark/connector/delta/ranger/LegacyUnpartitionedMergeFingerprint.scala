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

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.plans.logical.DeltaMergeInto

/** Frozen b5a260a37245ca2577bcdae03b7b2f35bbb8e66d journal encoding for legacy recovery. */
private[ranger] object LegacyUnpartitionedMergeFingerprint {
  def apply(receipt: DeltaRangerMergeReceipt): String = {
    import TestDeltaRangerMergeSource.field
    val sourceAnchor = receipt.source.binding.anchor()
    val target = receipt.target
    val source = receipt.source
    val original = field[DeltaMergeInto](receipt, "original")
    val rewritten = field[DeltaMergeInto](receipt, "rewritten")
    val sourceReceipt = receipt.cdf
    val subject = field[DeltaRangerQuerySubject](receipt, "subject")
    val settings = field[Map[String, String]](receipt, "settings")
    import DeltaRangerMergeJournal.{vector, hash}
    def binding(value: DeltaRangerRegistrationBindings.BoundRegistrationScan): String = {
      val observation = value.observation()
      vector(
        Vector(
          observation.metalake(),
          observation.catalog(),
          observation.database(),
          observation.table(),
          vector(
            observation
              .receipt()
              .toProperties
              .asScala
              .toVector
              .filterNot(_._1 == "delta.registration.receipt.nativeVersion")
              .sortBy(_._1)
              .map { case (key, v) => vector(Seq(key, v)) }
          ),
          value.nativeSchemaJson(),
          value.nativeProtocolJson(),
          vector(
            value.index().snapshotAtAnalysis.metadata.configuration.toVector.sortBy(_._1).map {
              case (k, v) => vector(Seq(k, v))
            }
          )
        )
      )
    }
    val fingerprint = hash(
      vector(
        Vector(
          binding(sourceAnchor),
          binding(target),
          source.request.start.toString,
          source.request.end.toString,
          DeltaRangerMergeJournal.plan(original.source),
          vector(original.expressions.map(_.sql)),
          sourceReceipt.mergeIntent,
          vector(rewritten.expressions.map(_.sql)),
          subject.user(),
          vector(subject.groups().asScala.toVector.sorted),
          vector(
            settings.toVector
              .filterNot(_._1 == "spark.databricks.delta.lastCommitVersionInSession")
              .sortBy(_._1)
              .map { case (k, v) => vector(Seq(k, v)) }
          )
        )
      )
    )
    fingerprint
  }
}
