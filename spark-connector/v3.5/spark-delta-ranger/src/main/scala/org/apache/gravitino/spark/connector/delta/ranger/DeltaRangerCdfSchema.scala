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

import com.fasterxml.jackson.databind.ObjectMapper
import java.util.Locale
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/** Finite historical read compatibility only; this check grants no execution authority. */
private[ranger] object DeltaRangerCdfSchema {
  private val mapper = new ObjectMapper()
  private val virtual = Set("_change_type", "_commit_version", "_commit_timestamp")

  def requireCompatible(previous: StructType, next: StructType): Unit = {
    if (previous == null || next == null) denied("missing schema")
    // Identical schemas keep the existing read profile, including unchanged nested fields.
    if (previous == next) return
    if (
      next.length <= previous.length ||
      !next.fields.take(previous.length).sameElements(previous.fields)
    )
      denied("old fields must remain exactly equal at their original ordinals")
    validateNames(next)
    if (SchemaUtils.findInvalidColumnNamesInSchema(next).nonEmpty) denied("invalid field name")
    if (next.fieldNames.exists(name => virtual(name.toLowerCase(Locale.ROOT))))
      denied("virtual column collision")
    next.fields.drop(previous.length).foreach { field =>
      if (!field.nullable || !scalar(field.dataType))
        denied("only nullable concrete scalar top-level additions are qualified")
      if (field.metadata == null || specialMetadata(field))
        denied("engine field metadata is not qualified")
    }
  }

  private def validateNames(dataType: DataType): Unit = dataType match {
    case struct: StructType =>
      val names = struct.fields.map { field =>
        if (field == null || field.name == null || field.name.isEmpty) denied("invalid field name")
        validateNames(field.dataType)
        field.name.toLowerCase(Locale.ROOT)
      }
      if (names.distinct.length != names.length) denied("duplicate or case-colliding field name")
    case ArrayType(element, _)  => validateNames(element)
    case MapType(key, value, _) => validateNames(key); validateNames(value)
    case _                      =>
  }

  private def specialMetadata(field: StructField): Boolean =
    mapper.readTree(field.metadata.json).fieldNames().asScala.exists { key =>
      val name = key.toLowerCase(Locale.ROOT)
      name.startsWith("delta.columnmapping.") || name.startsWith("delta.identity.") ||
      name.startsWith("parquet.field.") || name == "delta.generationexpression" ||
      name == "current_default" || name == "exists_default" ||
      name == "__char_varchar_type_string" || name == "__file_source_generated_metadata_col"
    }

  private def scalar(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        StringType | BinaryType | DateType | TimestampType =>
      true
    case _: DecimalType => true
    case _              => false
  }

  private def denied(reason: String): Nothing =
    throw new SecurityException("Delta CDF schema rejected: " + reason)
}
