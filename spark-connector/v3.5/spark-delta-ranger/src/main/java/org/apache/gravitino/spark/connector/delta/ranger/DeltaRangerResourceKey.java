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

package org.apache.gravitino.spark.connector.delta.ranger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.kyuubi.plugin.spark.authz.ObjectType;
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessResource;
import scala.Option;

/** Immutable, catalog-bearing identity; mutable Ranger resource equality is never used. */
public final class DeltaRangerResourceKey {
  private static final Set<String> ACTIONS =
      Collections.unmodifiableSet(
          new LinkedHashSet<>(
              Arrays.asList(
                  "select", "read", "write", "create", "drop", "alter", "update", "all")));
  private final String catalog;
  private final String database;
  private final String table;
  @Nullable private final String column;
  private final String accessType;
  @Nullable private final String owner;

  /**
   * Creates an identity from canonical registration and original privilege extraction.
   *
   * @param catalog canonical catalog
   * @param database canonical database
   * @param table canonical table
   * @param column original top-level column, or null for an explicit table-wide request
   * @param accessType admitted lowercase Ranger access
   * @param owner authoritative owner when relevant
   */
  public DeltaRangerResourceKey(
      String catalog,
      String database,
      String table,
      @Nullable String column,
      String accessType,
      @Nullable String owner) {
    this.catalog = literal(catalog);
    this.database = literal(database);
    this.table = literal(table);
    this.column = column == null ? null : literal(column);
    if (!ACTIONS.contains(accessType)) {
      throw new IllegalArgumentException("Unsupported Delta access");
    }
    this.accessType = accessType;
    this.owner = owner == null ? null : literal(owner);
  }

  /**
   * Retains a table's full original column set before splitting, including no-column reads. Call
   * for every original table, including both join sides and predicate columns.
   *
   * @param catalog canonical catalog retained before deduplication
   * @param database canonical database
   * @param table canonical table
   * @param columns original columns; empty requires table-wide SELECT
   * @param owner authoritative owner, if available
   * @return immutable distinct requests
   */
  public static Set<DeltaRangerResourceKey> select(
      String catalog,
      String database,
      String table,
      Collection<String> columns,
      @Nullable String owner) {
    Objects.requireNonNull(columns, "Original columns required");
    Set<DeltaRangerResourceKey> keys = new LinkedHashSet<>();
    if (columns.isEmpty()) {
      keys.add(new DeltaRangerResourceKey(catalog, database, table, null, "select", owner));
    }
    for (String column : columns) {
      keys.add(
          new DeltaRangerResourceKey(catalog, database, table, literal(column), "select", owner));
    }
    return Collections.unmodifiableSet(keys);
  }

  /**
   * @return a new fully populated Ranger resource, safe to mutate without changing this key
   */
  public AccessResource resource() {
    AccessResource resource = new AccessResource(ObjectType.COLUMN(), Option.apply(catalog));
    resource.setValue("catalog", catalog);
    resource.setValue("database", database);
    resource.setValue("table", table);
    resource.setValue("column", column == null ? "*" : column);
    resource.setOwnerUser(owner);
    return resource;
  }

  /**
   * @return admitted access type
   */
  public String accessType() {
    return accessType;
  }

  /**
   * @return canonical column, or null for a table-wide request
   */
  @Nullable
  public String column() {
    return column;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DeltaRangerResourceKey)) {
      return false;
    }
    DeltaRangerResourceKey that = (DeltaRangerResourceKey) other;
    return catalog.equals(that.catalog)
        && database.equals(that.database)
        && table.equals(that.table)
        && Objects.equals(column, that.column)
        && accessType.equals(that.accessType)
        && Objects.equals(owner, that.owner);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(catalog, database, table, column, accessType, owner);
  }

  static String literal(String value) {
    if (value == null
        || value.trim().isEmpty()
        || value.indexOf('*') >= 0
        || value.indexOf('?') >= 0) {
      throw new IllegalArgumentException(
          "Missing canonical Delta identity or unsupported literal wildcard");
    }
    return value;
  }
}
