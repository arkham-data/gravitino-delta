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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;

/** Driver-local finite holder for actual retained Dataset and physical-plan references. */
final class DeltaRangerRetainedQueries implements AutoCloseable {
  private final SparkSession spark;
  private final Map<String, Slot> active = new HashMap<>();
  private final Set<String> used = new HashSet<>();

  DeltaRangerRetainedQueries(SparkSession spark) {
    this.spark = spark;
  }

  Slot prepare(
      String retentionId, String sql, long cursor, DeltaRangerPolicyEngine.Versions applied) {
    if (active.size() >= 4 || used.contains(retentionId)) {
      throw new IllegalStateException("Retained slot capacity or used ID");
    }
    Dataset<Row> dataset = spark.sql(sql);
    SparkPlan root = dataset.queryExecution().executedPlan();
    Slot slot = new Slot(dataset, root, cursor, applied);
    active.put(retentionId, slot);
    used.add(retentionId);
    return slot;
  }

  Slot require(String retentionId) {
    Slot slot = active.get(retentionId);
    if (slot == null) {
      throw new IllegalStateException("Missing retained slot");
    }
    return slot;
  }

  void release(String retentionId) {
    if (active.remove(retentionId) == null) {
      throw new IllegalStateException("Missing retained slot");
    }
  }

  @Override
  public void close() {
    active.clear();
  }

  static final class Slot {
    private final Dataset<Row> dataset;
    private final SparkPlan root;
    private long cursor;
    private DeltaRangerPolicyEngine.Versions versions;

    private Slot(
        Dataset<Row> dataset,
        SparkPlan root,
        long cursor,
        DeltaRangerPolicyEngine.Versions versions) {
      this.dataset = dataset;
      this.root = root;
      this.cursor = cursor;
      this.versions = versions;
    }

    Dataset<Row> dataset() {
      return dataset;
    }

    SparkPlan root() {
      return root;
    }

    long cursor() {
      return cursor;
    }

    DeltaRangerPolicyEngine.Versions versions() {
      return versions;
    }

    void advance(long cursor, DeltaRangerPolicyEngine.Versions applied) {
      this.cursor = cursor;
      this.versions = applied;
    }
  }
}
