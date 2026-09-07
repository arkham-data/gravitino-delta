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
package org.apache.gravitino.spark.connector.integration.test.delta;

import java.util.HashSet;
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

/** Temporary replacement stage; loaded tables retain their actual native Delta type. */
@SuppressWarnings("deprecation")
final class ReplacementStage implements StagedTable, SupportsWrite {
  private final StagedTable nativeStage;
  int builders;
  int builds;
  int truncates;
  int commits;
  int aborts;

  ReplacementStage(StagedTable nativeStage) {
    this.nativeStage = nativeStage;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return nativeStage.name();
  }

  /** {@inheritDoc} */
  @Override
  public StructType schema() {
    return nativeStage.schema();
  }

  /** {@inheritDoc} */
  @Override
  public Transform[] partitioning() {
    return nativeStage.partitioning();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return nativeStage.properties();
  }

  /** {@inheritDoc} */
  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> result = new HashSet<>(nativeStage.capabilities());
    result.add(TableCapability.TRUNCATE);
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public void commitStagedChanges() {
    commits++;
    nativeStage.commitStagedChanges();
  }

  /** {@inheritDoc} */
  @Override
  public void abortStagedChanges() {
    aborts++;
    nativeStage.abortStagedChanges();
  }

  /** {@inheritDoc} */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    builders++;
    return new ReplacementBuilder(((SupportsWrite) nativeStage).newWriteBuilder(info));
  }

  private final class ReplacementBuilder implements SupportsTruncate {
    private WriteBuilder nativeBuilder;

    private ReplacementBuilder(WriteBuilder nativeBuilder) {
      this.nativeBuilder = nativeBuilder;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Acknowledges replacement mode without committing or removing data.
     */
    @Override
    public WriteBuilder truncate() {
      truncates++;
      // The selected native stage already carries Replace/CreateOrReplace. It commits later.
      if (nativeBuilder instanceof SupportsTruncate)
        nativeBuilder = ((SupportsTruncate) nativeBuilder).truncate();
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public Write build() {
      builds++;
      return nativeBuilder.build();
    }
  }
}
