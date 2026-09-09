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

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** Deferred truncate acknowledgement for native Replace and CreateOrReplace creation modes. */
@SuppressWarnings("deprecation")
final class DeltaReplacementStage implements StagedTable, SupportsWrite {
  private final StagedTable stage;

  DeltaReplacementStage(StagedTable stage) {
    this.stage = stage;
  }
  /** {@inheritDoc} */
  @Override
  public String name() {
    return stage.name();
  }
  /** {@inheritDoc} */
  @Override
  public StructType schema() {
    return stage.schema();
  }
  /** {@inheritDoc} */
  @Override
  public Transform[] partitioning() {
    return stage.partitioning();
  }
  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return stage.properties();
  }
  /** {@inheritDoc} */
  @Override
  public Set<TableCapability> capabilities() {
    return EnumSet.of(TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE);
  }
  /** {@inheritDoc} */
  @Override
  public void commitStagedChanges() {
    stage.commitStagedChanges();
  }
  /** {@inheritDoc} */
  @Override
  public void abortStagedChanges() {
    stage.abortStagedChanges();
  }
  /** {@inheritDoc} */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new Builder(((SupportsWrite) stage).newWriteBuilder(info));
  }

  private static final class Builder implements SupportsTruncate {
    private WriteBuilder builder;

    private Builder(WriteBuilder builder) {
      this.builder = builder;
    }
    /** {@inheritDoc} */
    @Override
    public WriteBuilder truncate() {
      if (builder instanceof SupportsTruncate) {
        builder = ((SupportsTruncate) builder).truncate();
      }
      return this;
    }
    /** {@inheritDoc} */
    @Override
    public Write build() {
      return builder.build();
    }
  }
}
