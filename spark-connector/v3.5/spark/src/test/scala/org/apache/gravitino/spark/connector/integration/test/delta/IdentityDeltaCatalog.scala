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
package org.apache.gravitino.spark.connector.integration.test.delta

import org.apache.gravitino.client.GravitinoAdminClient
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.slf4j.LoggerFactory

/** Non-persisted identity descriptor for the native Delta read characterization. */
class IdentityDeltaCatalog extends NamedDeltaCatalog {
  /** Creates a simple-auth fixture client; this does not prove deployment authentication. */
  override protected def createMetadataClient(uri: String): GravitinoAdminClient =
    GravitinoAdminClient.builder(uri).withSimpleAuth("metadata_service").build()

  /** Decorates the native result using the public load seam, without a session-catalog entry. */
  override def loadTable(id: Identifier): Table = {
    val subject = UserGroupInformation.getCurrentUser.getShortUserName
    LoggerFactory.getLogger(getClass).info(
      s"METADATA client=metadata_service queryUgi=$subject resource=${name()}.${id}")
    val native = super.loadTable(id).asInstanceOf[DeltaTableV2]
    require(UserGroupInformation.getCurrentUser.getShortUserName == subject)
    val descriptor = CatalogTable(
      identifier = TableIdentifier(id.name(), Some(id.namespace().head), Some(name())),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(locationUri = Some(native.path.toUri)),
      schema = native.schema(),
      provider = Some("delta"))
    native.copy(catalogTable = Some(descriptor), tableIdentifier = None)
  }
}
