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
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.gravitino.spark.connector.delta.DeltaCreateLifecycle
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{CreateTable, TableSpec}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.types._

/** Immutable data only: authority remains with the privately issued command and reservation. */
private[ranger] final class DeltaRangerCreateDeclaration private (
    val schemaJson: String,
    val partitionColumns: Vector[String],
    val properties: Map[String, String],
    val description: Option[String],
    val configuration: Map[String, String],
    private val sourcePartitions: Vector[String]
) {
  def schema: StructType = DataType.fromJson(schemaJson).asInstanceOf[StructType]
  def partitions: Array[Transform] =
    partitionColumns.map(name => Expressions.identity(quoteIdentifier(name))).toArray
  def matches(intent: DeltaCreateLifecycle.Intent): Boolean =
    intent.schemaJson() == schemaJson && intent.partitionCount() == partitionColumns.size &&
      intent.identityPartitionColumns().isPresent &&
      intent.identityPartitionColumns().get().asScala.toVector == partitionColumns &&
      intent.properties().asScala.toMap == properties
  def unchanged(command: CreateTable): Boolean =
    command.tableSchema.json == schemaJson &&
      DeltaRangerCreateDeclaration
        .identities(command.partitioning.toArray)
        .contains(sourcePartitions)
}

private[ranger] object DeltaRangerCreateDeclaration {
  private val mapper = new ObjectMapper
  private val retention = Set("delta.deletedFileRetentionDuration", "delta.logRetentionDuration")
  private val allowed = retention ++ Set(
    "delta.compatibility.symlinkFormatManifest.enabled",
    "delta.enableChangeDataFeed",
    "delta.columnMapping.mode",
    "primary_key"
  )
  private val defaultsPrefix = "spark.databricks.delta.properties.defaults."
  private val legacySettings = Map(
    "spark.databricks.delta.optimizeWrite.enabled" -> "true",
    "spark.databricks.delta.autoCompact.enabled" -> "true",
    "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "true",
    "spark.databricks.delta.snapshotPartitions" -> "2",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "false",
    "spark.sql.sources.partitionOverwriteMode" -> "static",
    "spark.databricks.delta.autoCompact.minNumFiles" -> "3",
    "spark.databricks.delta.autoCompact.maxFileSize" -> "1048576",
    "spark.databricks.delta.autoCompact.minFileSize" -> "524288",
    "spark.databricks.delta.optimizeWrite.binSize" -> "1",
    "spark.databricks.delta.optimizeWrite.numShuffleBlocks" -> "2"
  )
  private val hubSettings = Map(
    "spark.databricks.delta.optimizeWrite.enabled" -> "true",
    "spark.databricks.delta.autoCompact.enabled" -> "true",
    "spark.databricks.delta.properties.defaults.compatibility.symlinkFormatManifest.enabled" -> "true",
    "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols" -> "-1",
    "spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled" -> "true",
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "128m",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> "2000",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "16m",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "false",
    "spark.sql.sources.partitionOverwriteMode" -> "static"
  )

  def apply(
      command: CreateTable,
      specification: TableSpec,
      spark: SparkSession
  ): DeltaRangerCreateDeclaration = {
    if (
      command.ignoreIfExists || specification.provider != Some("delta") ||
      specification.options.nonEmpty || specification.location.nonEmpty ||
      specification.serde.nonEmpty || specification.external
    ) denied("unsupported declaration")
    def matches(settings: Map[String, String]): Boolean = settings.forall { case (key, value) =>
      spark.conf.getOption(key).contains(value)
    }
    if (!matches(legacySettings) && !(matches(hubSettings) && nativeHubTuning(spark)))
      denied("unsupported initial CREATE settings")
    val schemaJson = command.tableSchema.json
    val schema = DataType.fromJson(schemaJson).asInstanceOf[StructType]
    if (schema.isEmpty) denied("empty schema")
    SchemaUtils.checkFieldNames(schema.fieldNames.toVector)
    SchemaMergingUtils.checkColumnNameDuplication(
      schema,
      "in CREATE declaration",
      caseSensitive = false
    )
    val resolver = spark.sessionState.conf.resolver
    schema.fields.zipWithIndex.foreach { case (field, index) =>
      if (
        field.name.trim.isEmpty || schema.fields
          .take(index)
          .exists(other => resolver(field.name, other.name))
      )
        denied("invalid or duplicate column")
      if (!ordinary(field.dataType)) denied("unsupported column type")
      val metadata = mapper.readTree(field.metadata.json)
      if ((metadata.fieldNames().asScala.toSet -- Set("comment")).nonEmpty)
        denied("unsupported column metadata")
      if (metadata.has("comment") && !metadata.get("comment").isTextual)
        denied("invalid column comment")
    }
    val source =
      identities(command.partitioning.toArray).getOrElse(denied("unsupported partition transform"))
    val partitions = source.map { name =>
      val fields = schema.fields.filter(field => resolver(field.name, name))
      if (fields.length != 1 || !scalar(fields.head.dataType)) denied("invalid partition column")
      fields.head.name
    }
    if (partitions.distinct.size != partitions.size || partitions.size == schema.length)
      denied("invalid partition columns")
    val explicit = specification.properties.toMap
    if (!explicit.keySet.subsetOf(allowed)) denied("unsupported table property")
    validate(explicit)
    val sessionDefaults = spark.sessionState.conf.getAllConfs.collect {
      case (key, value) if key.startsWith(defaultsPrefix) =>
        ("delta." + key.stripPrefix(defaultsPrefix)) -> value
    }.toMap
    if (
      !sessionDefaults.keySet.subsetOf(
        (allowed - "primary_key") + "delta.dataSkippingNumIndexedCols"
      )
    ) denied("unsupported session default")
    validate(sessionDefaults)
    if (sessionDefaults.get("delta.dataSkippingNumIndexedCols").exists(_ != "-1"))
      denied("unsupported data skipping default")
    val effective = DeltaConfigs
      .validateConfigurations(DeltaConfigs.mergeGlobalConfigs(spark.sessionState.conf, explicit))
      .updated("delta.columnMapping.mode", "none")
    if (
      !effective.get("delta.enableChangeDataFeed").contains("true") ||
      !effective.get("delta.compatibility.symlinkFormatManifest.enabled").contains("true")
    ) denied("required CREATE properties missing")
    new DeltaRangerCreateDeclaration(
      schemaJson,
      partitions,
      DeltaRangerCatalogSpark35.convertCreateProperties(specification).asScala.toMap,
      specification.comment,
      effective,
      source
    )
  }

  def identities(partitions: Array[Transform]): Option[Vector[String]] = {
    val columns = DeltaCreateLifecycle.Intent.snapshotIdentityPartitionColumns(partitions)
    if (columns.isPresent) Some(columns.get().asScala.toVector) else None
  }

  // Tuning not supplied by Hub must retain its native default, rather than inherit fixture tuning.
  private def nativeHubTuning(spark: SparkSession): Boolean =
    !spark.sessionState.conf.getAllConfs.keys.exists { key =>
      !hubSettings.contains(key) &&
      (key.startsWith("spark.databricks.delta.optimizeWrite.") ||
        key.startsWith("spark.databricks.delta.autoCompact.") ||
        key == "spark.databricks.delta.snapshotPartitions")
    }

  private def scalar(value: DataType): Boolean = value match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
        StringType | BinaryType | DateType | TimestampType =>
      true
    case value: DecimalType =>
      value.precision >= 1 && value.precision <= 38 && value.scale >= 0 && value.scale <= value.precision
    case _ => false
  }
  private def ordinary(value: DataType): Boolean = value match {
    case ArrayType(element, true)              => scalar(element)
    case MapType(StringType, StringType, true) => true
    case other                                 => scalar(other)
  }
  private def validate(properties: Map[String, String]): Unit = properties.foreach {
    case (key, value) =>
      if (value == null) denied("null property")
      if (
        retention(key) && !value.matches(
          "interval ([1-9]|[1-9][0-9]|[12][0-9]{2}|3[0-5][0-9]|36[0-4]) days"
        )
      ) denied("unsupported retention")
      if (
        (key == "delta.enableChangeDataFeed" || key == "delta.compatibility.symlinkFormatManifest.enabled") && value != "true"
      ) denied("required CREATE property disabled")
      if (key == "delta.columnMapping.mode" && value != "none") denied("unsupported column mapping")
  }
  private def denied(reason: String): Nothing = throw new SecurityException(
    "Schema CREATE rejected: " + reason
  )
}
