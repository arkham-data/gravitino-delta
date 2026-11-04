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

import java.util.Locale
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaOptions

/** Finite writer intent for matching-schema append; transaction settings remain session-owned. */
private[ranger] object DeltaRangerAppendOptions {
  private val allowed = Set("usermetadata", "mergeschema", "overwriteschema")

  def validate(spark: SparkSession, options: Map[String, String]): Map[String, String] = {
    if (spark == null || options == null) invalid()
    // Traverse even an immutable Map: a retained implementation can still delegate to mutable state.
    val snapshot = options.iterator.toVector.toMap
    val names = snapshot.iterator.map { case (key, value) =>
      if (key == null || value == null) invalid()
      val name = key.toLowerCase(Locale.ROOT)
      if (!allowed(name)) invalid()
      name
    }.toVector
    if (names.distinct.size != names.size) invalid()
    val native = new DeltaOptions(snapshot, spark.sessionState.conf)
    // These are accessors, not constructor checks. Force both before a duplicate can return.
    native.canMergeSchema
    native.canOverwriteSchema
    snapshot
  }

  def transactionOptions: Map[String, String] = Map.empty

  private def invalid(): Nothing = throw new IllegalArgumentException(
    "Invalid append writer options"
  )
}
