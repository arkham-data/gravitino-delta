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
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.types.StructType

/** Finite expectation from native MERGE resolution; owns no transaction or execution authority. */
private[ranger] object DeltaRangerMergeSchema {
  def derive(
      spark: SparkSession,
      base: Metadata,
      protocol: Protocol,
      finalSchema: Option[StructType],
      evolutionEnabled: Boolean
  ): DeltaRangerAppendSchema.Intent = {
    if (
      spark == null || base == null || protocol == null || finalSchema == null ||
      finalSchema.isEmpty || finalSchema.get == null
    )
      invalid("missing native final schema input")
    val resolved = finalSchema.get
    val existing = base.schema
    if (resolved != existing && !evolutionEnabled)
      invalid("schema evolution is disabled")
    if (
      resolved.length < existing.length ||
      !resolved.fields.take(existing.length).sameElements(existing.fields)
    )
      invalid("native final schema changed existing fields")
    // The independent MERGE check above grants the internal finite helper option. The schema is
    // Delta's resolved final target schema, never the raw source or user-provided writer options.
    DeltaRangerAppendSchema.derive(spark, base, protocol, resolved, Map("mergeSchema" -> "true"))
  }

  private def invalid(reason: String): Nothing =
    throw new IllegalArgumentException("Delta merge schema rejected: " + reason)
}
