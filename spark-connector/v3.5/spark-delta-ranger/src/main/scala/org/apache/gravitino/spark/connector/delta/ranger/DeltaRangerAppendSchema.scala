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
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaOptions}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.types._

/** Finite schema expectation only; deriving an intent grants no authority and owns no transaction.
  */
private[ranger] object DeltaRangerAppendSchema {
  private val metadataMapper = new ObjectMapper()
  private val cdfFeatures = Some(Set("changeDataFeed", "appendOnly", "invariants"))
  private val unqualifiedProperties = Set(
    "delta.enablerowtracking",
    "delta.enabledeletionvectors",
    "delta.enabletypewidening",
    "delta.enableincommittimestamps",
    "delta.enableicebergcompatv1",
    "delta.enableicebergcompatv2"
  )

  def derive(
      spark: SparkSession,
      base: Metadata,
      protocol: Protocol,
      source: StructType,
      options: Map[String, String]
  ): Intent = {
    if (spark == null || base == null || protocol == null || source == null)
      invalid("missing schema input")
    val writerOptions = DeltaRangerAppendOptions.validate(spark, options)
    val retained = freeze(base)
    val retainedProtocol = protocol.copy(
      readerFeatures = protocol.readerFeatures.map(_.iterator.toVector.toSet),
      writerFeatures = protocol.writerFeatures.map(_.iterator.toVector.toSet)
    )
    val existing = retained.schema
    if (
      source.length < existing.length || !source.fields
        .take(existing.length)
        .zip(existing.fields)
        .forall { case (from, to) =>
          from != null && to != null && from.name == to.name &&
          from.dataType == to.dataType && (!from.nullable || to.nullable)
        }
    ) invalid("native source normalization is not qualified")
    // Existing matching-schema admission does not acquire evolution's feature/metadata restrictions.
    if (source.length == existing.length)
      return new Intent(retained, retained, retainedProtocol)
    if (!new DeltaOptions(writerOptions, spark.sessionState.conf).canMergeSchema)
      invalid("schema evolution is disabled")
    qualify(retained, retainedProtocol, source)
    val nullable = SchemaUtils.typeAsNullable(source).asInstanceOf[StructType]
    val input = DeltaColumnMapping.dropColumnMappingMetadata(nullable)
    val merged = SchemaMergingUtils.mergeSchemas(
      existing,
      input,
      allowImplicitConversions = false,
      keepExistingType = false,
      allowTypeWidening = false,
      caseSensitive = false
    )
    val expected =
      SchemaUtils.removeUnenforceableNotNullConstraints(merged, spark.sessionState.conf)
    if (
      expected.length != source.length ||
      !expected.fields.take(existing.length).sameElements(existing.fields) ||
      !expected.fields.drop(existing.length).zip(source.fields.drop(existing.length)).forall {
        case (field, from) =>
          field.name == from.name && field.dataType == from.dataType &&
          field.nullable && field.metadata == from.metadata
      }
    ) invalid("native schema transition is not qualified")
    new Intent(retained, retained.copy(schemaString = expected.json), retainedProtocol)
  }

  /** Frozen exact native pending state. Only derive can construct an expectation. */
  final class Intent private[DeltaRangerAppendSchema] (
      val baseMetadata: Metadata,
      val expectedMetadata: Metadata,
      val protocol: Protocol
  ) {
    val evolves: Boolean = expectedMetadata != baseMetadata
    def requirePending(metadata: Metadata, protocol: Protocol): Unit = {
      if (metadata != expectedMetadata || protocol != this.protocol)
        invalid("pending native metadata or protocol changed")
    }
  }

  private def freeze(metadata: Metadata): Metadata = {
    if (
      metadata.schemaString == null || metadata.format == null ||
      metadata.configuration == null || metadata.partitionColumns == null || metadata.format.options == null
    )
      invalid("missing native metadata")
    def snapshot(values: Map[String, String]): Map[String, String] = {
      val entries = values.iterator.toVector
      if (entries.exists { case (key, value) => key == null || value == null })
        invalid("null native metadata entry")
      entries.toMap
    }
    metadata.copy(
      configuration = snapshot(metadata.configuration),
      partitionColumns = metadata.partitionColumns.toVector,
      format = metadata.format.copy(options = snapshot(metadata.format.options))
    )
  }

  private def qualify(base: Metadata, protocol: Protocol, source: StructType): Unit = {
    val ordinary = protocol.minReaderVersion == 1 && protocol.minWriterVersion == 2 &&
      protocol.readerFeatures.isEmpty && protocol.writerFeatures.isEmpty
    val cdf = protocol.minReaderVersion == 1 && protocol.minWriterVersion == 7 &&
      protocol.readerFeatures.isEmpty && protocol.writerFeatures == cdfFeatures
    if (!ordinary && !cdf) invalid("unqualified evolving protocol")
    if (base.columnMappingMode.name != "none") invalid("unqualified evolving column mapping")
    if (
      base.configuration.exists { case (key, value) =>
        val name = key.toLowerCase(Locale.ROOT)
        name.startsWith("delta.feature.") || name.startsWith("delta.coordinatedcommits.") ||
        name.startsWith("delta.universalformat.") || name == "delta.minreaderversion" ||
        name == "delta.minwriterversion" || (unqualifiedProperties(name) && value != "false")
      }
    ) invalid("unqualified evolving configuration")
    if (base.configuration.get("delta.enableChangeDataFeed").contains("true") && !cdf)
      invalid("unqualified CDF protocol")
    validateFields(base.schema)
    validateFields(source)
    if (!source.fields.drop(base.schema.length).forall(field => scalar(field.dataType)))
      invalid("only concrete scalar top-level additions are qualified")
    if (SchemaUtils.findInvalidColumnNamesInSchema(source).nonEmpty)
      invalid("invalid evolving column name")
  }

  private def validateFields(dataType: DataType): Unit = dataType match {
    case struct: StructType =>
      struct.fields.foreach { field =>
        if (field == null || field.name == null || field.metadata == null || specialMetadata(field))
          invalid("unqualified evolving field metadata")
        validateFields(field.dataType)
      }
    case ArrayType(element, _)  => validateFields(element)
    case MapType(key, value, _) => validateFields(key); validateFields(value)
    case value if scalar(value) =>
    case _                      => invalid("unqualified evolving field type")
  }

  // Match the excluded engine metadata used by native catalog schema admission. Ordinary
  // comments and application metadata remain intact; this is a refusal check, not a stripper.
  private def specialMetadata(field: StructField): Boolean =
    metadataMapper.readTree(field.metadata.json).fieldNames().asScala.exists { key =>
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

  private def invalid(reason: String): Nothing =
    throw new IllegalArgumentException("Delta append schema rejected: " + reason)
}
