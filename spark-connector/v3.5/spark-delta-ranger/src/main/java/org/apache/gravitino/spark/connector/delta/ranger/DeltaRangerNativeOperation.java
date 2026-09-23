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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.BoundRegistrationScan;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.files.TahoeFileIndex;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.LogicalRDD;
import scala.collection.Seq;

/** Driver-only, finite native invocation. Java owns the lifecycle of ordinary catalog loads. */
@SuppressWarnings("ReferenceEquality") // Native authority binds backing objects by identity.
final class DeltaRangerNativeOperation implements AutoCloseable {
  private static final ThreadLocal<DeltaRangerNativeOperation> CURRENT = new ThreadLocal<>();

  static final class OptimizerRejected extends SecurityException {
    private static final long serialVersionUID = 1L;

    OptimizerRejected(boolean consumer) {
      super(
          consumer
              ? "Native Delta optimizer consumer rejected"
              : "Native Delta optimizer prefix rejected");
    }
  }

  enum Callback {
    LOAD,
    PREPARE,
    VALIDATE_RETAINED_BINDING,
    MATCHING_FILES,
    GET_SNAPSHOT,
    INPUT_FILES,
    SIZE_IN_BYTES
  }

  interface ScanOwner {
    SparkSession session();

    List<BoundRegistrationScan> bindings();

    void requireLive();
  }

  private final SparkSession session;
  @Nullable private final DeltaRangerRegistrationBindings loadOwner;
  @Nullable private final DeltaRangerRegistrationReader reader;
  @Nullable private final PublishedObservation before;
  @Nullable private final ScanOwner scanOwner;
  @Nullable private final BoundRegistrationScan retainedOwner;
  private final List<Lineage> lineages = new ArrayList<>();
  private final List<Seq<Attribute>> aggregateOutputs = new ArrayList<>();
  @Nullable private BoundRegistrationScan completedLoad;
  @Nullable private Snapshot capturing;
  private boolean statsCapture;
  private Callback callback;
  @Nullable private TahoeFileIndex invokedIndex;
  @Nullable private Seq<Expression> partitionFilters;
  @Nullable private Seq<Expression> dataFilters;
  private volatile boolean successful;
  private volatile boolean invalid;
  private volatile boolean closed;

  private DeltaRangerNativeOperation(
      SparkSession session,
      @Nullable DeltaRangerRegistrationBindings loadOwner,
      @Nullable DeltaRangerRegistrationReader reader,
      @Nullable PublishedObservation before,
      @Nullable ScanOwner scanOwner,
      @Nullable BoundRegistrationScan retainedOwner) {
    this.session = Objects.requireNonNull(session);
    this.loadOwner = loadOwner;
    this.reader = reader;
    this.before = before;
    this.scanOwner = scanOwner;
    this.retainedOwner = retainedOwner;
    this.callback =
        retainedOwner != null
            ? Callback.VALIDATE_RETAINED_BINDING
            : scanOwner == null ? Callback.LOAD : Callback.PREPARE;
    if (CURRENT.get() != null) throw denied();
    requireOwner();
    CURRENT.set(this);
  }

  static DeltaRangerNativeOperation load(
      DeltaRangerRegistrationBindings owner,
      DeltaRangerRegistrationReader reader,
      PublishedObservation before) {
    return new DeltaRangerNativeOperation(SparkSession.active(), owner, reader, before, null, null);
  }

  static DeltaRangerNativeOperation prepare(ScanOwner owner) {
    if (owner.bindings().isEmpty()) throw denied();
    return new DeltaRangerNativeOperation(owner.session(), null, null, null, owner, null);
  }

  static DeltaRangerNativeOperation validateRetained(
      SparkSession session, BoundRegistrationScan binding) {
    binding.requireRetained();
    return new DeltaRangerNativeOperation(session, null, null, null, null, binding);
  }

  static DeltaRangerNativeOperation scan(
      ScanOwner owner,
      TahoeFileIndex index,
      Seq<Expression> partitions,
      Seq<Expression> data,
      Callback callback) {
    if (index == null
        || partitions == null
        || data == null
        || callback == null
        || callback == Callback.LOAD
        || callback == Callback.PREPARE
        || callback == Callback.VALIDATE_RETAINED_BINDING) throw denied();
    DeltaRangerNativeOperation operation = prepare(owner);
    operation.callback = callback;
    operation.invokedIndex = Objects.requireNonNull(index);
    operation.partitionFilters = Objects.requireNonNull(partitions);
    operation.dataFilters = Objects.requireNonNull(data);
    return operation;
  }

  Callback callback() {
    return callback;
  }

  @Nullable
  TahoeFileIndex invokedIndex() {
    return invokedIndex;
  }

  @Nullable
  Seq<Expression> partitionFilters() {
    return partitionFilters;
  }

  @Nullable
  Seq<Expression> dataFilters() {
    return dataFilters;
  }

  @Nullable
  static DeltaRangerNativeOperation current() {
    return CURRENT.get();
  }

  boolean ownedBy(ScanOwner owner) {
    return scanOwner == owner;
  }

  void invalidate() {
    invalid = true;
  }

  SparkSession session() {
    return session;
  }

  boolean active() {
    return !closed && CURRENT.get() == this;
  }

  boolean succeeded() {
    return closed && successful && !invalid;
  }

  void requireEvidence(SparkSession expected) {
    if (invalid || session != expected || (closed && !successful) || (!closed && !active()))
      throw denied();
    requireOwner();
  }

  boolean ownsLogRoot(Path root) {
    requireEvidence(session);
    if (retainedOwner != null) return retainedOwner.log().logPath().equals(root);
    if (scanOwner != null) {
      return scanOwner.bindings().stream().anyMatch(b -> b.log().logPath().equals(root));
    }
    return before != null
        && DeltaRangerRegistrationBindings.locationMatches(
            before.location(), root.getParent().toUri());
  }

  void completeLoad(BoundRegistrationScan binding) {
    requireActive();
    if (loadOwner == null || before == null || !before.samePublication(binding.observation()))
      throw denied();
    binding.requireLive();
    completedLoad = binding;
    successful = true;
  }

  void succeed() {
    requireActive();
    if (scanOwner == null && retainedOwner == null) throw denied();
    successful = true;
  }

  void capture(TahoeLogFileIndex index, Snapshot snapshot) {
    requireActive();
    if (capturing != null || snapshot.deltaLog() != index.deltaLog()) throw denied();
    if (retainedOwner != null) {
      if (retainedOwner.index() != index || retainedOwner.log() != snapshot.deltaLog())
        throw denied();
    } else if (scanOwner != null) {
      if (scanOwner.bindings().stream().noneMatch(b -> b.index() == index)) throw denied();
    } else if (!ownsLogRoot(index.deltaLog().logPath())) {
      throw denied();
    }
    if (lineages.stream().anyMatch(l -> l.index == index && l.snapshot == snapshot)) return;
    Lineage lineage = new Lineage(index, snapshot);
    try {
      capturing = snapshot;
      statsCapture = false;
      lineage.state = returned(() -> snapshot.stateDF().queryExecution().analyzed());
      // State must be sealed before withStats plans allFiles over that exact cached state.
      lineages.add(lineage);
      statsCapture = true;
      lineage.stats = returned(() -> snapshot.withStats().queryExecution().analyzed());
    } catch (RuntimeException | Error failure) {
      invalid = true;
      lineages.remove(lineage);
      throw failure;
    } finally {
      capturing = null;
    }
  }

  @Nullable
  Snapshot capturingSnapshot() {
    return active() ? capturing : null;
  }

  boolean capturingStats() {
    return statsCapture;
  }

  boolean contains(LogicalRDD rdd, boolean stats) {
    return lineages.stream().anyMatch(l -> same(stats ? l.stats : l.state, rdd));
  }

  boolean contains(TahoeLogFileIndex index, Snapshot snapshot) {
    return lineages.stream()
        .anyMatch(l -> l.index == index && l.snapshot == snapshot && l.stats != null);
  }

  void recordAggregate(Seq<Attribute> output) {
    requireActive();
    if (!aggregateOutputs.contains(output)) aggregateOutputs.add(output);
  }

  boolean hasAggregate(Seq<Attribute> output) {
    return aggregateOutputs.contains(output);
  }

  /** Freezes successful evidence; an incomplete or exceptional invocation is inert. */
  @Override
  public void close() {
    if (closed) return;
    if (CURRENT.get() != this) throw denied();
    try {
      requireOwner();
    } catch (RuntimeException | Error failure) {
      successful = false;
      throw failure;
    } finally {
      closed = true;
      CURRENT.remove();
    }
  }

  /** Omits owner, native location, subject and receipt authority. */
  @Override
  public String toString() {
    return "Native Delta invocation";
  }

  private void requireActive() {
    if (invalid || !active() || successful) throw denied();
    requireOwner();
  }

  private void requireOwner() {
    if (retainedOwner != null) {
      retainedOwner.requireRetained();
    } else if (scanOwner != null) {
      scanOwner.requireLive();
      scanOwner.bindings().forEach(BoundRegistrationScan::requireLive);
    } else {
      Objects.requireNonNull(loadOwner).requireLive();
      Objects.requireNonNull(reader).requireLive(Objects.requireNonNull(before));
      if (completedLoad != null) completedLoad.requireLive();
    }
  }

  private static LogicalRDD returned(Supplier<LogicalPlan> getter) {
    LogicalPlan result = getter.get();
    if (!(result instanceof LogicalRDD) || result.isStreaming()) throw denied();
    return (LogicalRDD) result;
  }

  private static boolean same(@Nullable LogicalRDD captured, LogicalRDD candidate) {
    return captured != null
        && captured.rdd() == candidate.rdd()
        && captured.output().equals(candidate.output())
        && !candidate.isStreaming();
  }

  private static SecurityException denied() {
    return new SecurityException("Native Delta operation ownership is unavailable");
  }

  private static final class Lineage {
    private final TahoeLogFileIndex index;
    private final Snapshot snapshot;
    @Nullable private LogicalRDD state;
    @Nullable private LogicalRDD stats;

    private Lineage(TahoeLogFileIndex index, Snapshot snapshot) {
      this.index = index;
      this.snapshot = snapshot;
    }
  }
}
