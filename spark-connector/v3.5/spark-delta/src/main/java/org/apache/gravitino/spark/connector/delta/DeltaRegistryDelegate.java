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

import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Complete delegate for Delta's initial named lookup and path fallback. */
@SuppressWarnings("deprecation")
final class DeltaRegistryDelegate implements TableCatalog, SupportsNamespaces {
  private final GravitinoDeltaCatalogSpark35 outer;

  DeltaRegistryDelegate(GravitinoDeltaCatalogSpark35 outer) {
    this.outer = outer;
  }
  /** {@inheritDoc} */
  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {}
  /** {@inheritDoc} */
  @Override
  public String name() {
    return outer.name();
  }
  /** {@inheritDoc} */
  @Override
  public Table loadTable(Identifier id) throws NoSuchTableException {
    if (id.namespace().length == 1
        && "delta".equals(id.namespace()[0])
        && new Path(id.name()).isAbsolute()) {
      throw new NoSuchTableException(id);
    }
    return outer.loadTable(id);
  }
  /** {@inheritDoc} */
  @Override
  public Identifier[] listTables(String[] namespace) {
    return outer.listTables(namespace);
  }
  /** {@inheritDoc} */
  @Override
  public Table createTable(
      Identifier id, StructType schema, Transform[] partitions, Map<String, String> properties) {
    throw new UnsupportedOperationException("Native delegate cannot register a second table");
  }
  /** {@inheritDoc} */
  @Override
  public Table alterTable(Identifier id, TableChange... changes) {
    throw new UnsupportedOperationException("Native delegate cannot mutate registry metadata");
  }
  /** {@inheritDoc} */
  @Override
  public boolean dropTable(Identifier id) {
    throw new UnsupportedOperationException("Native delegate cannot drop registry metadata");
  }
  /** {@inheritDoc} */
  @Override
  public void renameTable(Identifier from, Identifier to) {
    throw new UnsupportedOperationException("Native delegate cannot rename registry metadata");
  }
  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces() {
    return outer.listNamespaces();
  }
  /** {@inheritDoc} */
  @Override
  public String[][] listNamespaces(String[] parent) {
    return outer.listNamespaces(parent);
  }
  /** {@inheritDoc} */
  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) {
    return outer.loadNamespaceMetadata(namespace);
  }
  /** {@inheritDoc} */
  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) {
    outer.createNamespace(namespace, metadata);
  }
  /** {@inheritDoc} */
  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) {
    outer.alterNamespace(namespace, changes);
  }
  /** {@inheritDoc} */
  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) {
    return outer.dropNamespace(namespace, cascade);
  }
}
