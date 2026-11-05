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

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** Pure declared descriptor until a writer or untouched commit materializes the native stage. */
@SuppressWarnings("deprecation")
final class DeltaPublicationStage implements StagedTable, SupportsWrite {
  enum Requested {
    CREATE,
    REPLACE,
    CREATE_OR_REPLACE
  }

  interface Materializer {
    StagedTable materialize(OperationMode mode, StructType schema, Map<String, String> options);
  }

  interface CommitLifecycle {
    default DeltaCreateLifecycle.Scope beforeNative() {
      return () -> {};
    }

    default void nativeCommitted() {}

    default boolean publicationCompleted() {
      return false;
    }

    default void published() {}

    default void close() {}
  }

  private final String name;
  private final StructType declaredSchema;
  private final Transform[] partitions;
  private final Map<String, String> properties;
  private final Set<TableCapability> capabilities;
  private final Runnable check;
  private final Materializer materializer;
  private final Runnable publish;
  private final Runnable abort;
  private final CommitLifecycle lifecycle;
  @Nullable private StagedTable nativeStage;
  @Nullable private WriteBuilder builder;
  @Nullable private StructType querySchema;
  @Nullable private Map<String, String> queryOptions;
  private boolean attempted;
  private boolean committed;
  private boolean nativeCompleted;
  private boolean published;
  private boolean aborted;
  private boolean failed;
  private boolean lifecycleClosed;

  DeltaPublicationStage(
      String name,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      Requested requested,
      Runnable check,
      Materializer materializer,
      Runnable publish,
      Runnable abort) {
    this(
        name,
        schema,
        partitions,
        properties,
        requested,
        check,
        materializer,
        publish,
        abort,
        new CommitLifecycle() {});
  }

  DeltaPublicationStage(
      String name,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties,
      Requested requested,
      Runnable check,
      Materializer materializer,
      Runnable publish,
      Runnable abort,
      CommitLifecycle lifecycle) {
    this.name = name;
    this.declaredSchema = (StructType) StructType.fromJson(schema.json());
    this.partitions = partitions.clone();
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.capabilities =
        Collections.unmodifiableSet(
            requested == Requested.CREATE
                ? EnumSet.of(TableCapability.V1_BATCH_WRITE)
                : EnumSet.of(TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE));
    this.check = check;
    this.materializer = materializer;
    this.publish = publish;
    this.abort = abort;
    this.lifecycle = lifecycle;
  }
  /** {@inheritDoc} */
  @Override
  public String name() {
    return name;
  }
  /** {@inheritDoc} */
  @Override
  public StructType schema() {
    return (StructType) StructType.fromJson(declaredSchema.json());
  }
  /** {@inheritDoc} */
  @Override
  public Transform[] partitioning() {
    return partitions.clone();
  }
  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return properties;
  }
  /** {@inheritDoc} */
  @Override
  public Set<TableCapability> capabilities() {
    return capabilities;
  }
  /** {@inheritDoc} */
  @Override
  public synchronized WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    ensureOpen();
    check.run();
    Map<String, String> options = new HashMap<>(info.options().asCaseSensitiveMap());
    DeltaCatalogProperties.validateWrite(options);
    StructType normalized = info.schema().asNullable();
    if (builder != null) {
      if (!normalized.equals(querySchema) || !options.equals(queryOptions)) {
        throw new IllegalArgumentException("Conflicting repeated Delta write builder");
      }
      return builder;
    }
    try {
      materialize(OperationMode.QUERY, normalized, options);
      querySchema = normalized;
      queryOptions = options;
      builder = ((SupportsWrite) nativeStage).newWriteBuilder(info);
      return builder;
    } catch (Throwable error) {
      failed = true;
      closeAfterFailure(error);
      return GravitinoDeltaCatalogSpark35.fail(error);
    }
  }
  /** {@inheritDoc} */
  @Override
  public synchronized void commitStagedChanges() {
    if (published) {
      return;
    }
    if (aborted) {
      throw new IllegalStateException("Delta stage is aborted");
    }
    if (failed) {
      throw new IllegalStateException(
          nativeCompleted
              ? "DELTA_PUBLICATION_STATUS_NEEDED: native commit retained"
              : committed
                  ? "DELTA_NATIVE_COMMIT_STATUS_NEEDED"
                  : "Delta materialization requires operator recovery; request is never replayed");
    }
    check.run();
    try (DeltaCreateLifecycle.Scope cleanup = this::closeLifecycle) {
      if (!committed) {
        if (nativeStage == null) {
          materialize(OperationMode.SCHEMA_ONLY, declaredSchema, Collections.emptyMap());
        }
        try (DeltaCreateLifecycle.Scope scope = Objects.requireNonNull(lifecycle.beforeNative())) {
          // A thrown native commit may already have committed. Never execute it a second time.
          committed = true;
          nativeStage.commitStagedChanges();
          nativeCompleted = true;
          lifecycle.nativeCommitted();
        }
      }
      if (!nativeCompleted) {
        throw new IllegalStateException("DELTA_NATIVE_COMMIT_STATUS_NEEDED");
      }
      try (DeltaCreateLifecycle.Scope observation = this::observePublication) {
        publish.run();
        published = true;
      }
    } catch (Throwable error) {
      failed = true;
      GravitinoDeltaCatalogSpark35.fail(error);
    }
  }
  /** {@inheritDoc} */
  @Override
  public synchronized void abortStagedChanges() {
    if (aborted || published) {
      return;
    }
    aborted = true;
    try (DeltaCreateLifecycle.Scope cleanup = this::closeLifecycle) {
      try {
        if (nativeStage != null && !committed) {
          nativeStage.abortStagedChanges();
        }
      } finally {
        if (attempted) {
          abort.run();
        }
      }
    }
  }

  private void materialize(OperationMode mode, StructType schema, Map<String, String> options) {
    if (attempted) {
      throw new IllegalStateException(
          "Delta materialization requires operator recovery; request is never replayed");
    }
    attempted = true;
    StructType boundSchema = (StructType) StructType.fromJson(schema.json());
    Map<String, String> boundOptions = Collections.unmodifiableMap(new HashMap<>(options));
    nativeStage = materializer.materialize(mode, boundSchema, boundOptions);
  }

  private void observePublication() {
    if (published || lifecycle.publicationCompleted()) {
      // A validated response is a known outcome even if recording or observing it fails.
      published = true;
      lifecycle.published();
    }
  }

  private void closeLifecycle() {
    if (!lifecycleClosed) {
      lifecycleClosed = true;
      lifecycle.close();
    }
  }

  private void closeAfterFailure(Throwable error) {
    try {
      closeLifecycle();
    } catch (Throwable cleanup) {
      if (cleanup != error) {
        error.addSuppressed(cleanup);
      }
    }
  }

  private void ensureOpen() {
    if (aborted || committed || failed) {
      throw new IllegalStateException("Delta stage is closed");
    }
  }
}
