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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.BoundAppendTransaction;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings.BoundRegistrationScan;
import org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationReader.PublishedObservation;
import org.apache.hadoop.fs.Path;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning;
import org.apache.spark.sql.delta.DeltaUDF;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.files.TahoeFileIndex;
import org.apache.spark.sql.delta.files.TahoeLogFileIndex;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

/** Driver-only, finite native invocation. Java owns the lifecycle of ordinary catalog loads. */
@SuppressWarnings("ReferenceEquality") // Native authority binds backing objects by identity.
final class DeltaRangerNativeOperation implements AutoCloseable {
  private static final ThreadLocal<DeltaRangerNativeOperation> CURRENT = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> RAW_SELECTION = new ThreadLocal<>();

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
    TRANSACTION_STATE,
    COMMIT,
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

    /** Freezes the exact cached getter return for later scan continuation comparisons. */
    default Predicate<LogicalRDD> freezeCache(LogicalRDD actual) {
      throw denied();
    }

    /** Seals the exact uncached getter return under this scan owner, or fails closed. */
    default Runnable sealReconstruction(
        DeltaRangerNativeOperation operation, LogicalPlan actual, boolean stats) {
      throw denied();
    }
  }

  interface CommitOwner {
    SparkSession session();

    void requireLive();

    boolean contains(LogicalRDD candidate);
  }

  /** Exact selected-head owner; never a named scan or log-root permit. */
  interface RawOwner {
    /** Returns the captured compilation session. */
    SparkSession session();

    /** Revalidates the retained registration, profile, native head and cache association. */
    void requireLive();

    /** Matches an exact stock getter-returned cache and its frozen dependency graph. */
    boolean contains(LogicalRDD candidate, boolean stats);
  }

  @Nullable private RawOwner rawOwner;
  @Nullable private CommitOwner commitOwner;
  private final SparkSession session;
  @Nullable private final DeltaRangerRegistrationBindings loadOwner;
  @Nullable private final DeltaRangerRegistrationReader reader;
  @Nullable private final PublishedObservation before;
  @Nullable private final ScanOwner scanOwner;
  @Nullable private final BoundRegistrationScan retainedOwner;
  @Nullable private final BoundAppendTransaction transactionOwner;
  @Nullable private TransactionCache transactionCache;
  private final List<Lineage> lineages = new ArrayList<>();
  @Nullable private DeltaRangerNativeOperation statePreparation;
  @Nullable private Returned retainedState;
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
      @Nullable BoundRegistrationScan retainedOwner,
      @Nullable BoundAppendTransaction transactionOwner) {
    this(session, loadOwner, reader, before, scanOwner, retainedOwner, transactionOwner, null);
  }

  private DeltaRangerNativeOperation(
      SparkSession session,
      @Nullable DeltaRangerRegistrationBindings loadOwner,
      @Nullable DeltaRangerRegistrationReader reader,
      @Nullable PublishedObservation before,
      @Nullable ScanOwner scanOwner,
      @Nullable BoundRegistrationScan retainedOwner,
      @Nullable BoundAppendTransaction transactionOwner,
      @Nullable CommitOwner commitOwner) {
    this(
        session,
        loadOwner,
        reader,
        before,
        scanOwner,
        retainedOwner,
        transactionOwner,
        commitOwner,
        null);
  }

  private DeltaRangerNativeOperation(
      SparkSession session,
      @Nullable DeltaRangerRegistrationBindings loadOwner,
      @Nullable DeltaRangerRegistrationReader reader,
      @Nullable PublishedObservation before,
      @Nullable ScanOwner scanOwner,
      @Nullable BoundRegistrationScan retainedOwner,
      @Nullable BoundAppendTransaction transactionOwner,
      @Nullable CommitOwner commitOwner,
      @Nullable RawOwner rawOwner) {
    this.rawOwner = rawOwner;
    this.commitOwner = commitOwner;
    this.session = Objects.requireNonNull(session);
    this.loadOwner = loadOwner;
    this.reader = reader;
    this.before = before;
    this.scanOwner = scanOwner;
    this.retainedOwner = retainedOwner;
    this.transactionOwner = transactionOwner;
    this.callback =
        transactionOwner != null
            ? Callback.TRANSACTION_STATE
            : retainedOwner != null
                ? Callback.VALIDATE_RETAINED_BINDING
                : scanOwner == null ? Callback.LOAD : Callback.PREPARE;
    if (CURRENT.get() != null) throw denied();
    requireNoRawSelection();
    requireOwner();
    CURRENT.set(this);
  }

  static DeltaRangerNativeOperation load(
      DeltaRangerRegistrationBindings owner,
      DeltaRangerRegistrationReader reader,
      PublishedObservation before) {
    return new DeltaRangerNativeOperation(
        SparkSession.active(), owner, reader, before, null, null, null);
  }

  static DeltaRangerNativeOperation prepare(ScanOwner owner) {
    if (owner.bindings().isEmpty()) throw denied();
    return new DeltaRangerNativeOperation(owner.session(), null, null, null, owner, null, null);
  }

  static DeltaRangerNativeOperation validateRetained(
      SparkSession session, BoundRegistrationScan binding) {
    binding.requireRetained();
    return new DeltaRangerNativeOperation(session, null, null, null, null, binding, null);
  }

  static DeltaRangerNativeOperation transactionState(BoundAppendTransaction owner) {
    Objects.requireNonNull(owner).requireLive();
    return new DeltaRangerNativeOperation(owner.session(), null, null, null, null, null, owner);
  }

  static DeltaRangerNativeOperation commit(CommitOwner owner) {
    DeltaRangerNativeOperation operation =
        new DeltaRangerNativeOperation(owner.session(), null, null, null, null, null, null, owner);
    operation.callback = Callback.COMMIT;
    return operation;
  }

  static void requireNoRawSelection() {
    if (Boolean.TRUE.equals(RAW_SELECTION.get())) throw denied();
  }

  static <T> T selectRaw(Supplier<T> callback) {
    requireNoRawSelection();
    if (CURRENT.get() != null) throw denied();
    RAW_SELECTION.set(Boolean.TRUE);
    try {
      return callback.get();
    } finally {
      RAW_SELECTION.remove();
    }
  }

  static <T> T withRawSession(RawOwner owner, Supplier<T> callback) {
    return withRawSession(owner.session(), callback);
  }

  static <T> T withRawSession(SparkSession session, Supplier<T> callback) {
    return session.withActive(
        new AbstractFunction0<T>() {
          /** Runs the finite callback under its captured session. */
          @Override
          public T apply() {
            return callback.get();
          }
        });
  }

  static DeltaRangerNativeOperation rawPrepare(RawOwner owner) {
    DeltaRangerNativeOperation operation =
        new DeltaRangerNativeOperation(
            owner.session(), null, null, null, null, null, null, null, owner);
    operation.callback = Callback.PREPARE;
    return operation;
  }

  static DeltaRangerNativeOperation rawScan(
      RawOwner owner, TahoeFileIndex index, Callback callback) {
    if (callback != Callback.MATCHING_FILES
        && callback != Callback.GET_SNAPSHOT
        && callback != Callback.INPUT_FILES
        && callback != Callback.SIZE_IN_BYTES) throw denied();
    DeltaRangerNativeOperation operation = rawPrepare(owner);
    operation.invokedIndex = Objects.requireNonNull(index);
    operation.callback = callback;
    return operation;
  }

  @Nullable
  RawOwner rawOwner() {
    return rawOwner;
  }

  static long retentionTimestamp(Snapshot snapshot) {
    return snapshot.minFileRetentionTimestamp();
  }

  static Option<Object> transactionRetentionTimestamp(Snapshot snapshot) {
    return snapshot.minSetTransactionRetentionTimestamp();
  }

  static int snapshotPartitions(Snapshot snapshot) {
    return snapshot.getNumPartitions();
  }

  static UserDefinedFunction canonicalPathFunction(scala.Function1<String, String> function) {
    return DeltaUDF.stringFromString(function);
  }

  static boolean stockCacheWrapper(LogicalRDD candidate, SparkSession session) {
    Seq<Object> origins = candidate.otherCopyArgs();
    return candidate.rdd().getClass() == MapPartitionsRDD.class
        && origins.size() == 3
        && origins.apply(0) == session
        && origins.apply(1) instanceof Option
        && ((Option<?>) origins.apply(1)).isEmpty()
        && origins.apply(2) instanceof Option
        && ((Option<?>) origins.apply(2)).isEmpty();
  }

  boolean committing() {
    return commitOwner != null;
  }

  boolean transactionState() {
    return transactionOwner != null;
  }

  void captureTransactionState() {
    requireActive();
    if (transactionOwner == null || capturing != null || transactionCache != null) throw denied();
    try {
      capturing = transactionOwner.snapshot();
      statsCapture = false;
      LogicalPlan result = capturing.stateDF().queryExecution().analyzed();
      if (result.getClass() != LogicalRDD.class) throw denied();
      transactionCache = new TransactionCache((LogicalRDD) result, session);
      transactionOwner.requireLive();
    } catch (RuntimeException | Error failure) {
      invalid = true;
      throw failure;
    } finally {
      capturing = null;
    }
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
        || callback == Callback.COMMIT
        || callback == Callback.TRANSACTION_STATE
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
    requireNoRawSelection();
    if ((transactionOwner != null || commitOwner != null || rawOwner != null)
        && (closed || !active())) throw denied();
    if (invalid || session != expected || (closed && !successful) || (!closed && !active()))
      throw denied();
    requireOwner();
  }

  boolean ownsLogRoot(Path root) {
    requireEvidence(session);
    if (transactionOwner != null || commitOwner != null || rawOwner != null) return false;
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
    if (scanOwner == null
        && retainedOwner == null
        && transactionOwner == null
        && commitOwner == null
        && rawOwner == null) throw denied();
    successful = true;
  }

  void captureRetained(
      TahoeLogFileIndex index, Snapshot snapshot, DeltaRangerNativeOperation preparation) {
    requireActive();
    if (scanOwner == null
        || invokedIndex == null
        || callback != Callback.MATCHING_FILES
        || preparation == this
        || !preparation.succeeded()
        || preparation.callback != Callback.PREPARE
        || preparation.scanOwner != scanOwner
        || preparation.session != session
        || statePreparation != null) throw denied();
    preparation.requireEvidence(session);
    Lineage original =
        preparation.lineages.stream()
            .filter(l -> l.index == index && l.snapshot == snapshot && l.complete())
            .findFirst()
            .orElseThrow(DeltaRangerNativeOperation::denied);
    if (original.state == null
        || original.state.cache == null
        || original.state.cacheBinding == null) throw denied();
    statePreparation = preparation;
    retainedState = original.state;
    // The original stats recipe retains this old state RDD after Snapshot.uncache().
    // This evidence does not complete fresh capture or transfer the old stats cache.
    capture(index, snapshot);
  }

  void capture(TahoeLogFileIndex index, Snapshot snapshot) {
    requireActive();
    if (transactionOwner != null || capturing != null || snapshot.deltaLog() != index.deltaLog())
      throw denied();
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
    requireEvidence(session);
    if (rawOwner != null) return rawOwner.contains(rdd, stats);
    if (commitOwner != null) return !stats && commitOwner.contains(rdd);
    if (transactionOwner != null)
      return !stats && transactionCache != null && transactionCache.matches(rdd);
    if (!stats && statePreparation != null) {
      statePreparation.requireEvidence(session);
      if (same(retainedState, rdd)) return true;
    }
    return lineages.stream().anyMatch(l -> same(stats ? l.stats : l.state, rdd));
  }

  boolean contains(TahoeLogFileIndex index, Snapshot snapshot) {
    return lineages.stream()
        .anyMatch(l -> l.index == index && l.snapshot == snapshot && l.complete());
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
    if (CURRENT.get() != this) {
      if (commitOwner != null) {
        closed = true;
        CURRENT.remove();
      }
      throw denied();
    }
    try {
      if (commitOwner == null) requireOwner();
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
    if (rawOwner != null) {
      rawOwner.requireLive();
    } else if (commitOwner != null) {
      commitOwner.requireLive();
    } else if (transactionOwner != null) {
      transactionOwner.requireLive();
    } else if (retainedOwner != null) {
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

  private Returned returned(Supplier<LogicalPlan> getter) {
    LogicalPlan result = getter.get();
    if (result.isStreaming()) throw denied();
    if (result.getClass() == LogicalRDD.class) {
      LogicalRDD cache = (LogicalRDD) result;
      return new Returned(
          cache, scanOwner == null ? null : Objects.requireNonNull(scanOwner.freezeCache(cache)));
    }
    if (scanOwner == null) throw denied();
    return new Returned(scanOwner.sealReconstruction(this, result, statsCapture));
  }

  private static boolean same(@Nullable Returned captured, LogicalRDD candidate) {
    return captured != null
        && captured.cache != null
        && captured.cache.rdd() == candidate.rdd()
        && captured.cache.output().equals(candidate.output())
        && !candidate.isStreaming()
        && (captured.cacheBinding == null || captured.cacheBinding.test(candidate));
  }

  private static SecurityException denied() {
    return new SecurityException("Native Delta operation ownership is unavailable");
  }

  /** Immutable record of the measured cached state getter, with no caller-supplied predicate. */
  private static final class TransactionCache {
    private final LogicalRDD original;
    private final RDD<?> backing;
    private final SparkSession session;
    private final List<List<Object>> output;
    private final List<Object> origins;
    private final int partitions;

    private TransactionCache(LogicalRDD actual, SparkSession session) {
      this.original = actual;
      this.backing = actual.rdd();
      this.session = session;
      if (actual.getClass() != LogicalRDD.class
          || backing.getClass() != MapPartitionsRDD.class
          || actual.output().size() != 10
          || actual.outputPartitioning().getClass() != UnknownPartitioning.class
          || actual.outputPartitioning().numPartitions() != 0
          || !actual.outputOrdering().isEmpty()
          || actual.isStreaming()) throw denied();
      this.partitions = actual.outputPartitioning().numPartitions();
      this.output = attributes(actual);
      List<Object> copied = new ArrayList<>();
      Iterator<Object> iterator = actual.otherCopyArgs().iterator();
      while (iterator.hasNext()) copied.add(iterator.next());
      if (copied.size() != 3
          || copied.get(0) != session
          || !(copied.get(1) instanceof Option)
          || !((Option<?>) copied.get(1)).isEmpty()
          || !(copied.get(2) instanceof Option)
          || !((Option<?>) copied.get(2)).isEmpty()) throw denied();
      this.origins = Collections.unmodifiableList(copied);
    }

    private boolean matches(LogicalRDD candidate) {
      return matchesOne(original) && matchesOne(candidate);
    }

    private boolean matchesOne(LogicalRDD candidate) {
      if (candidate.getClass() != LogicalRDD.class
          || candidate.rdd() != backing
          || candidate.outputPartitioning().getClass() != UnknownPartitioning.class
          || candidate.outputPartitioning().numPartitions() != partitions
          || !candidate.outputOrdering().isEmpty()
          || candidate.isStreaming()
          || !attributes(candidate).equals(output)) return false;
      Seq<Object> copied = candidate.otherCopyArgs();
      return copied.size() == origins.size()
          && copied.apply(0) == session
          && copied.apply(1) == origins.get(1)
          && copied.apply(2) == origins.get(2);
    }

    private static List<List<Object>> attributes(LogicalRDD rdd) {
      List<List<Object>> values = new ArrayList<>();
      Iterator<Attribute> iterator = rdd.output().iterator();
      while (iterator.hasNext()) {
        Attribute attribute = iterator.next();
        if (attribute.getClass() != AttributeReference.class) throw denied();
        List<Object> stamp = new ArrayList<>();
        stamp.add(attribute.getClass());
        stamp.add(attribute.name());
        stamp.add(attribute.exprId().id());
        stamp.add(attribute.exprId().jvmId());
        List<String> qualifier = new ArrayList<>();
        Iterator<String> parts = attribute.qualifier().iterator();
        while (parts.hasNext()) qualifier.add(parts.next());
        stamp.add(Collections.unmodifiableList(qualifier));
        stamp.add(attribute.dataType().json());
        stamp.add(attribute.nullable());
        stamp.add(attribute.metadata().json());
        values.add(Collections.unmodifiableList(stamp));
      }
      return Collections.unmodifiableList(values);
    }
  }

  private static final class Returned {
    @Nullable private final LogicalRDD cache;
    @Nullable private final Runnable reconstruction;
    @Nullable private final Predicate<LogicalRDD> cacheBinding;

    private Returned(LogicalRDD cache, @Nullable Predicate<LogicalRDD> cacheBinding) {
      this.cache = cache;
      this.reconstruction = null;
      this.cacheBinding = cacheBinding;
    }

    private Returned(Runnable reconstruction) {
      this.cache = null;
      this.cacheBinding = null;
      this.reconstruction = Objects.requireNonNull(reconstruction);
    }

    private void requireValid() {
      if (cacheBinding != null && !cacheBinding.test(Objects.requireNonNull(cache))) throw denied();
      if (reconstruction != null) reconstruction.run();
    }
  }

  private static final class Lineage {
    private final TahoeLogFileIndex index;
    private final Snapshot snapshot;
    @Nullable private Returned state;
    @Nullable private Returned stats;

    private Lineage(TahoeLogFileIndex index, Snapshot snapshot) {
      this.index = index;
      this.snapshot = snapshot;
    }

    private boolean complete() {
      if (state == null || stats == null) return false;
      state.requireValid();
      stats.requireValid();
      return true;
    }
  }
}
