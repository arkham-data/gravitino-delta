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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract.OperationMode;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import scala.collection.Seq;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.Nil$;
import scala.collection.immutable.Vector;
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.WrappedArray;

/** Boundaries for a single CREATE, with the base catalog retaining native execution ownership. */
public interface DeltaCreateLifecycle extends AutoCloseable {
  /** Immutable declaration captured before materialization or registration transport. */
  final class Intent {
    private final String catalog;
    private final List<String> namespace;
    private final String table;
    private final String schemaJson;
    private final int partitionCount;
    private final Optional<List<String>> identityPartitionColumns;
    private final Map<String, String> properties;

    /**
     * Captures a declaration without retaining caller-owned collections.
     *
     * @param catalog initialized catalog name
     * @param namespace literal namespace components
     * @param table literal table name
     * @param schemaJson declared schema JSON
     * @param partitionCount number of declared partition transforms
     * @param properties declared table properties
     */
    public Intent(
        String catalog,
        List<String> namespace,
        String table,
        String schemaJson,
        int partitionCount,
        Map<String, String> properties) {
      this(catalog, namespace, table, schemaJson, partitionCount, properties, Optional.empty());
    }

    /**
     * Captures exact stock top-level identity partitions without retaining caller-owned transforms.
     * Unsupported transforms leave the optional evidence absent and remain usable by base catalogs.
     *
     * @param catalog initialized catalog name
     * @param namespace literal namespace components
     * @param table literal table name
     * @param schemaJson declared schema JSON
     * @param partitions declared partition transforms
     * @param properties declared table properties
     */
    public Intent(
        String catalog,
        List<String> namespace,
        String table,
        String schemaJson,
        Transform[] partitions,
        Map<String, String> properties) {
      this(
          catalog,
          namespace,
          table,
          schemaJson,
          partitions.length,
          properties,
          snapshotIdentityPartitionColumns(partitions));
    }

    private Intent(
        String catalog,
        List<String> namespace,
        String table,
        String schemaJson,
        int partitionCount,
        Map<String, String> properties,
        Optional<List<String>> identityPartitionColumns) {
      this.identityPartitionColumns = identityPartitionColumns;
      this.catalog = catalog;
      this.namespace = Collections.unmodifiableList(new ArrayList<>(namespace));
      this.table = table;
      this.schemaJson = schemaJson;
      this.partitionCount = partitionCount;
      this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    }

    /**
     * @return initialized catalog name
     */
    public String catalog() {
      return catalog;
    }

    /**
     * @return immutable literal namespace components
     */
    public List<String> namespace() {
      return namespace;
    }

    /**
     * @return literal table name
     */
    public String table() {
      return table;
    }

    /**
     * @return declared schema JSON
     */
    public String schemaJson() {
      return schemaJson;
    }

    /**
     * @return number of declared partition transforms
     */
    public int partitionCount() {
      return partitionCount;
    }

    /**
     * @return immutable ordered identity column names, or absent when exact evidence is unavailable
     */
    public Optional<List<String>> identityPartitionColumns() {
      return identityPartitionColumns;
    }

    /**
     * @return immutable declared table properties
     */
    public Map<String, String> properties() {
      return properties;
    }

    /**
     * Snapshots literal names from known stock top-level identity transforms. No caller-owned
     * array, transform, reference or sequence is retained, and unknown implementations are not
     * inspected. This pure snapshot supplies declaration evidence, not mutation authority.
     *
     * @param partitions non-null declared partition transforms
     * @return immutable ordered names, including an empty list for no partitions, or absent when
     *     exact stock evidence is unavailable
     */
    public static Optional<List<String>> snapshotIdentityPartitionColumns(Transform[] partitions) {
      List<String> columns = new ArrayList<>();
      for (Transform transform : partitions) {
        if (transform == null || transform.getClass() != IdentityTransform.class) {
          return Optional.empty();
        }
        NamedReference reference = ((IdentityTransform) transform).ref();
        if (reference == null || reference.getClass() != FieldReference.class) {
          return Optional.empty();
        }
        Seq<String> parts = ((FieldReference) reference).parts();
        if (parts == null) {
          return Optional.empty();
        }
        // A stock reference may still wrap a custom sequence. Inspect only the concrete stock
        // containers produced by Spark's parser and expression factory, before invoking getters.
        if (parts.getClass() == $colon$colon.class) {
          if (parts.tail().getClass() != Nil$.class) {
            return Optional.empty();
          }
        } else if ((parts.getClass() != Vector.class
                && parts.getClass() != ArrayBuffer.class
                && parts.getClass() != WrappedArray.ofRef.class)
            || parts.length() != 1) {
          return Optional.empty();
        }
        Object column = parts.apply(0);
        if (!(column instanceof String) || ((String) column).isEmpty()) {
          return Optional.empty();
        }
        columns.add((String) column);
      }
      return Optional.of(Collections.unmodifiableList(columns));
    }
  }

  /** Scope surrounding exactly one native commit invocation. */
  interface Scope extends AutoCloseable {
    /** Releases the scope without a checked exception. */
    @Override
    void close();
  }

  /**
   * Checks actual materialization before metadata lookup, journal or reservation.
   *
   * @param mode actual operation mode
   * @param schemaJson actual materialization schema JSON
   * @param options immutable materialization options snapshot
   */
  void beforeReserve(OperationMode mode, String schemaJson, Map<String, String> options);

  /**
   * Binds the exact validated reservation after its journal entry is durable.
   *
   * @param receipt validated reservation receipt
   */
  void reserved(DeltaRegistrationReceipt receipt);

  /**
   * Enters the native scope before the base records invocation or calls native commit.
   *
   * @param receipt exact validated reservation receipt
   * @return scope closed after native execution and successful-return recording
   */
  Scope beforeNative(DeltaRegistrationReceipt receipt);

  /**
   * Checks publication after native completion and before publication intent or transport.
   *
   * @param receipt exact validated reservation receipt
   */
  void beforePublish(DeltaRegistrationReceipt receipt);

  /**
   * Observes an already recorded publication; rejection cannot undo that outcome.
   *
   * @param receipt validated published receipt
   */
  void published(DeltaRegistrationReceipt receipt);

  /**
   * Releases lifecycle state on terminal success, failure or abort; implementations are idempotent.
   */
  @Override
  void close();

  /**
   * Returns an inert lifecycle for ordinary base catalog behavior.
   *
   * @return no-op lifecycle
   */
  static DeltaCreateLifecycle noop() {
    return new DeltaCreateLifecycle() {
      @Override
      public void beforeReserve(
          OperationMode mode, String schemaJson, Map<String, String> options) {}

      @Override
      public void reserved(DeltaRegistrationReceipt receipt) {}

      @Override
      public Scope beforeNative(DeltaRegistrationReceipt receipt) {
        return () -> {};
      }

      @Override
      public void beforePublish(DeltaRegistrationReceipt receipt) {}

      @Override
      public void published(DeltaRegistrationReceipt receipt) {}

      @Override
      public void close() {}
    };
  }
}
