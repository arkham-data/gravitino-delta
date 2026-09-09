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

package org.apache.gravitino.spark.connector.delta;

import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaTableV2;
import scala.Option;

final class DeltaTableIdentity {
  static DeltaTableV2 attach(DeltaTableV2 table, String catalog, Identifier id) {
    CatalogStorageFormat empty = CatalogStorageFormat.empty();
    CatalogStorageFormat storage =
        empty.copy(
            Option.apply(table.path().toUri()),
            empty.inputFormat(),
            empty.outputFormat(),
            empty.serde(),
            empty.compressed(),
            empty.properties());
    CatalogTable descriptor =
        CatalogTable.apply(
            new TableIdentifier(id.name(), Option.apply(id.namespace()[0]), Option.apply(catalog)),
            CatalogTableType.EXTERNAL(),
            storage,
            table.schema(),
            Option.apply("delta"),
            table.initialSnapshot().metadata().partitionColumns(),
            CatalogTable.apply$default$7(),
            CatalogTable.apply$default$8(),
            CatalogTable.apply$default$9(),
            CatalogTable.apply$default$10(),
            CatalogTable.apply$default$11(),
            CatalogTable.apply$default$12(),
            CatalogTable.apply$default$13(),
            CatalogTable.apply$default$14(),
            CatalogTable.apply$default$15(),
            CatalogTable.apply$default$16(),
            CatalogTable.apply$default$17(),
            CatalogTable.apply$default$18(),
            CatalogTable.apply$default$19(),
            CatalogTable.apply$default$20());
    return table.copy(
        table.spark(),
        table.path(),
        Option.apply(descriptor),
        Option.empty(),
        table.timeTravelOpt(),
        table.options());
  }

  static void validate(DeltaTableV2 table, DeltaRegistrationReceipt receipt) {
    Snapshot current = table.update();
    boolean replacing = receipt.marker().isPresent();
    String expected =
        replacing ? receipt.marker().get().baseUuid().get() : receipt.nativeId().get();
    long version =
        replacing
            ? receipt.marker().get().baseVersion().getAsLong()
            : receipt.nativeVersion().getAsLong();
    if (!expected.equals(current.metadata().id())
        || (replacing ? version != current.version() : current.version() < version)) {
      throw DeltaRegistrationClient.bindingError();
    }
  }

  static void invalidate(String location, SparkSession session) {
    DeltaLog.invalidateCache(session, new Path(location));
  }
}
