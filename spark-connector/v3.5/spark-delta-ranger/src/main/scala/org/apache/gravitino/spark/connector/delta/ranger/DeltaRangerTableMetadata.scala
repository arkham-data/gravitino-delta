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
import javax.annotation.Nullable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import scala.util.control.NonFatal

/** Finite, detached metadata observation; native objects never cross the public API. */
private[ranger] object DeltaRangerTableMetadata {
  private val properties = Set(
    "delta.enableChangeDataFeed",
    "primary_key",
    "delta.logRetentionDuration",
    "delta.deletedFileRetentionDuration",
    "delta.compatibility.symlinkFormatManifest.enabled"
  )

  @Nullable
  def read(spark: SparkSession, name: String): String = {
    try {
      if (spark == null || !SparkSession.getActiveSession.exists(_ eq spark)) throw denied()
      if (name == null || name.trim.isEmpty) throw denied()
      val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(name).toVector
      if (parts.size != 3) throw denied()
      parts.foreach(DeltaRangerResourceKey.literal)
      // Complete native analyzer initialization before freezing session settings.
      spark.sessionState.analyzer
      val context = new GravitinoDeltaTable.Context(spark)
      context.validate()
      val manager = spark.sessionState.catalogManager
      val catalog = manager.catalog(parts.head) match {
        case owned: DeltaRangerCatalogSpark35 if owned.name() == parts.head => owned
        case _                                                              => throw denied()
      }
      val id = Identifier.of(Array(parts(1)), parts(2))
      val visible = catalog.visibleForMetadata(id)
      context.validate()
      val lease = catalog.metadataLease()
      val result =
        if (!visible.isPresent) null
        else {
          val table = catalog.loadTable(id).asInstanceOf[DeltaTableV2]
          val target = catalog.registrationBindings().resolveAppendTarget(table)
          if (table.spark ne spark) throw denied()
          if (visible.get().toProperties != target.observation().receipt().toProperties)
            throw denied()
          // Existing retained validation brackets both publication and native head, including
          // configuration, schema, protocol, partitions, lease and exact native object identity.
          target.validateCurrent(spark)
          val native = table.initialSnapshot.metadata
          val mapper = new ObjectMapper
          val json = mapper.createObjectNode()
          json.put("schemaJson", native.schema.json)
          val detachedProperties = json.putObject("properties")
          native.configuration.foreach { case (key, value) =>
            if (properties.contains(key)) detachedProperties.put(key, value)
          }
          val partitions = json.putArray("partitionColumns")
          native.partitionColumns.foreach(partitions.add)
          target.requireRetained()
          json.toString
        }
      context.validate()
      if (manager.catalog(parts.head) ne catalog) throw denied()
      catalog.requireMetadataLive(lease)
      result
    } catch {
      // Catalog/native transport errors can contain private credentials or physical locations.
      case NonFatal(_)     => throw denied()
      case _: LinkageError => throw denied()
    }
  }

  private def denied(): SecurityException =
    new SecurityException("Delta metadata observation rejected")
}
