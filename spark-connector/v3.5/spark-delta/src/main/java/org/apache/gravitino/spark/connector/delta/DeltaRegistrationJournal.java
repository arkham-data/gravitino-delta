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

import java.nio.file.Path;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;

/** Driver-owned recovery evidence; implementations must persist before returning. */
public interface DeltaRegistrationJournal {
  /** Durable operation phase; uncertainty never implies the request has completed. */
  enum Phase {
    /** Typed command saved before submission; a crash can leave completion unknown. */
    INTENT,
    /** Request completion is uncertain; STATUS alone does not prove request drain. */
    STATUS_NEEDED,
    /** A bound pending receipt is durable; native construction has not begun. */
    RESERVED,
    /** Native construction or commit may have begun; outcome may require reconciliation. */
    NATIVE_STARTED,
    /** Native commit returned successfully; publication is not yet confirmed. */
    NATIVE_COMMITTED,
    /** A matching published provider receipt has been durably recorded. */
    PUBLISHED,
    /** An explicit operator request returned a validated receipt. */
    OPERATOR_COMPLETED,
    /** Local abort notification; never proves request completion or native rollback. */
    LOCAL_ABORT
  }
  /**
   * Saves sanitized evidence before advancing to the next effect.
   *
   * @param entry immutable evidence
   */
  void write(Entry entry);
  /**
   * Opens a driver-owned local journal.
   *
   * @param directory explicit private local directory
   * @return durable journal
   */
  static DeltaRegistrationJournal durable(Path directory) {
    return new FileDeltaRegistrationJournal(directory);
  }
  /**
   * Reads bounded sanitized evidence without following a symbolic link.
   *
   * @param file saved entry file
   * @return decoded entry
   */
  static Entry read(Path file) {
    return FileDeltaRegistrationJournal.read(file);
  }

  /** Immutable command and receipt evidence. */
  final class Entry {
    final String key;
    final String catalog;
    final String namespace;
    final String table;
    final DeltaRegistrationCommand command;
    final Phase phase;
    @Nullable final DeltaRegistrationReceipt receipt;

    Entry(
        String key,
        String catalog,
        String namespace,
        String table,
        DeltaRegistrationCommand command,
        Phase phase,
        @Nullable DeltaRegistrationReceipt receipt) {
      this.key = key;
      this.catalog = catalog;
      this.namespace = namespace;
      this.table = table;
      this.command = command;
      this.phase = phase;
      this.receipt = receipt;
    }

    /**
     * @return original typed command, without arbitrary Spark options
     */
    public DeltaRegistrationCommand command() {
      return command;
    }
    /**
     * @return current durable phase; STATUS does not prove request completion
     */
    public Phase phase() {
      return phase;
    }
    /**
     * @return bound receipt, absent when an allocated ID remains unknown
     */
    public Optional<DeltaRegistrationReceipt> receipt() {
      return Optional.ofNullable(receipt);
    }
    /**
     * @return canonical catalog name
     */
    public String catalog() {
      return catalog;
    }
    /**
     * @return one-level namespace
     */
    public String namespace() {
      return namespace;
    }
    /**
     * @return table name
     */
    public String table() {
      return table;
    }
    /**
     * @return local evidence file key
     */
    public String key() {
      return key;
    }

    Entry phase(Phase next, @Nullable DeltaRegistrationReceipt value) {
      return new Entry(key, catalog, namespace, table, command, next, value);
    }
  }
}
