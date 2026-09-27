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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.apache.spark.sql.execution.SparkPlan;
import scala.collection.Iterator;

/** Executes the same retained named action in the driver and native offline fixture. */
final class DeltaRangerRetainedAction {
  private static final ObjectMapper JSON = new ObjectMapper();

  static final class Result {
    private final boolean passed;
    private final Map<String, Object> evidence;
    @Nullable private final Throwable failure;

    Result(boolean passed, Map<String, Object> evidence, @Nullable Throwable failure) {
      this.passed = passed;
      this.evidence = freezeEvidence(evidence);
      this.failure = failure;
    }

    boolean passed() {
      return passed;
    }

    Map<String, Object> evidence() {
      return evidence;
    }

    @Nullable
    Throwable failure() {
      return failure;
    }
  }

  private static Map<String, Object> freezeEvidence(Map<String, Object> source) {
    Map<String, Object> copy = new LinkedHashMap<>();
    source.forEach((key, value) -> copy.put(key, freeze(value)));
    return Collections.unmodifiableMap(copy);
  }

  private static Object freeze(Object value) {
    if (value instanceof Map<?, ?>) {
      Map<Object, Object> copy = new LinkedHashMap<>();
      ((Map<?, ?>) value).forEach((key, item) -> copy.put(key, freeze(item)));
      return Collections.unmodifiableMap(copy);
    }
    if (value instanceof List<?>) {
      List<Object> copy = new ArrayList<>();
      for (Object item : (List<?>) value) {
        copy.add(freeze(item));
      }
      return Collections.unmodifiableList(copy);
    }
    if (value != null && value.getClass().isArray()) {
      List<Object> copy = new ArrayList<>();
      for (int i = 0; i < Array.getLength(value); i++) {
        copy.add(freeze(Array.get(value, i)));
      }
      return Collections.unmodifiableList(copy);
    }
    return value;
  }

  static Result execute(
      SparkSession spark,
      DeltaRangerRetainedQueries.Slot slot,
      DeltaRangerDownloadedCommand.Parsed command) {
    if (command.kind() != DeltaRangerDownloadedCommand.Kind.COLLECT) {
      throw new IllegalArgumentException("Only collect commands execute a retained action");
    }
    Dataset<Row> dataset = slot.dataset();
    SparkPlan root = slot.root();
    boolean sameDatasetBefore = dataset == slot.dataset();
    boolean sameRootBefore = root == dataset.queryExecution().executedPlan();
    String jobGroup = "delta-retained-" + UUID.randomUUID();
    spark.sparkContext().setJobGroup(jobGroup, "retained named SELECT", false);
    int[] jobsBefore = spark.sparkContext().statusTracker().getJobIdsForGroup(jobGroup);
    List<List<Object>> rows = new ArrayList<>();
    Throwable failure = null;
    try {
      Row[] collected =
          command.action().equals("collect")
              ? (Row[]) dataset.collect()
              : dataset.collectAsList().toArray(new Row[0]);
      for (Row row : collected) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < row.size(); i++) {
          values.add(row.isNullAt(i) ? null : row.get(i));
        }
        rows.add(values);
      }
    } catch (Throwable actionFailure) {
      failure = actionFailure;
    } finally {
      spark.sparkContext().clearJobGroup();
    }
    int[] jobsAfter = spark.sparkContext().statusTracker().getJobIdsForGroup(jobGroup);
    boolean sameDatasetAfter = dataset == slot.dataset();
    boolean sameRootAfter = root == dataset.queryExecution().executedPlan();
    List<Long> scans = new ArrayList<>();
    Iterator<SparkPlan> nodes = DeltaRangerPhysicalProvenance$.MODULE$.nodes(root).iterator();
    while (nodes.hasNext()) {
      SparkPlan node = nodes.next();
      if (node instanceof FileSourceScanExec && node.metrics().contains("numOutputRows")) {
        scans.add(node.metrics().apply("numOutputRows").value());
      }
    }
    boolean entryDenied = entryDenied(failure, command.expectedFailureContains().orElse(""));
    boolean outcome =
        command.deniedAtEntry()
            ? entryDenied && scans.stream().allMatch(value -> value == 0)
            : failure == null
                && normalized(JSON.valueToTree(rows))
                    .equals(normalized(command.expectedRows().orElseThrow()));
    boolean passed =
        outcome
            && sameDatasetBefore
            && sameRootBefore
            && sameDatasetAfter
            && sameRootAfter
            && !scans.isEmpty();
    Map<String, Object> evidence = new LinkedHashMap<>();
    evidence.put("retentionId", command.retentionId());
    evidence.put("action", command.action());
    evidence.put("rows", rows);
    evidence.put("failureDetails", DeltaRangerDownloadedQuery.failureDetails(failure));
    evidence.put("actionEntryDenied", entryDenied);
    evidence.put("sameDatasetBefore", sameDatasetBefore);
    evidence.put("sameRootBefore", sameRootBefore);
    evidence.put("sameDatasetAfter", sameDatasetAfter);
    evidence.put("sameRootAfter", sameRootAfter);
    evidence.put("jobGroup", jobGroup);
    evidence.put("jobIdsBefore", Arrays.stream(jobsBefore).boxed().toArray(Integer[]::new));
    evidence.put("jobIdsAfter", Arrays.stream(jobsAfter).boxed().toArray(Integer[]::new));
    evidence.put("userScanOutputRows", scans);
    evidence.put("passed", passed);
    return new Result(passed, evidence, failure);
  }

  private static boolean entryDenied(Throwable failure, String fragment) {
    if (failure == null || fragment.isEmpty()) {
      return false;
    }
    boolean security = false;
    boolean entry = false;
    boolean execute = false;
    boolean message = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      security |= cause instanceof SecurityException;
      message |=
          cause.getMessage() != null
              && cause
                  .getMessage()
                  .toLowerCase(Locale.ROOT)
                  .contains(fragment.toLowerCase(Locale.ROOT));
      for (StackTraceElement frame : cause.getStackTrace()) {
        if (frame.getClassName().endsWith("DeltaRangerCollectGuardExec")) {
          entry |= frame.getMethodName().equals("executeCollect");
          execute |= frame.getMethodName().equals("doExecute");
        }
      }
    }
    return security && entry && !execute && message;
  }

  private static List<String> normalized(JsonNode rows) {
    List<String> result = new ArrayList<>();
    for (JsonNode row : rows) {
      result.add(row.toString());
    }
    Collections.sort(result);
    return result;
  }

  private DeltaRangerRetainedAction() {}
}
