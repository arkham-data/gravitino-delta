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

import java.util.UUID;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Operator-controlled recovery through the same authenticated production transport. A STATUS
 * observation does not prove the original request or writer hook has finished. Administrative
 * authorization and handoff evidence remain the server's responsibility.
 */
public final class DeltaRegistrationRecovery {
  private final DeltaRegistrationClient client;
  private final DeltaRegistrationJournal journal;
  /**
   * Creates a helper.
   *
   * @param client current actor transport
   * @param journal durable evidence sink
   */
  public DeltaRegistrationRecovery(
      DeltaRegistrationClient client, DeltaRegistrationJournal journal) {
    this.client = client;
    this.journal = journal;
  }
  /**
   * Reads status without proving the original owner/request is drained.
   *
   * @param entry original durable evidence
   * @return observed receipt
   */
  public DeltaRegistrationReceipt status(DeltaRegistrationJournal.Entry entry) {
    validateEntry(entry);
    DeltaRegistrationCommand command;
    if (entry.receipt != null) {
      command = DeltaRegistrationCommand.status(entry.receipt.identity());
    } else if (entry.command.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE) {
      DeltaRegistrationIdentity previous = entry.command.expected().get();
      command =
          DeltaRegistrationCommand.status(
              DeltaRegistrationIdentity.of(
                  previous.id(),
                  Math.addExact(previous.generation(), 1),
                  entry.command.replacementToken().get()));
    } else if (entry.command.token().isPresent() && entry.command.runId().isPresent()) {
      command =
          DeltaRegistrationCommand.statusByToken(
              entry.command.token().get(), entry.command.runId().get());
    } else if (entry.command.expected().isPresent()) {
      command = DeltaRegistrationCommand.status(entry.command.expected().get());
    } else {
      throw new IllegalArgumentException("Original evidence does not identify a recovery request");
    }
    DeltaRegistrationReceipt receipt = client.submit(id(entry), command);
    DeltaRegistrationClient.validateBinding(entry.command, receipt);
    // Keep completion uncertainty explicit even after learning the allocated identity.
    journal.write(entry.phase(DeltaRegistrationJournal.Phase.STATUS_NEEDED, receipt));
    return receipt;
  }
  /**
   * Runs an explicit operator command after independent confirmation of request completion/drain.
   *
   * @param entry bound durable evidence
   * @param command HANDOFF, PUBLISH or ABORT command with matching expected identity
   * @param requestDrained independent operator confirmation, never inferred from STATUS
   * @return observed receipt
   */
  public DeltaRegistrationReceipt execute(
      DeltaRegistrationJournal.Entry entry,
      DeltaRegistrationCommand command,
      boolean requestDrained) {
    if (!requestDrained
        || entry.receipt == null
        || !command.expected().isPresent()
        || !command.expected().get().equals(entry.receipt.identity())
        || !(command.operation() == DeltaRegistrationCommand.Operation.HANDOFF
            || command.operation() == DeltaRegistrationCommand.Operation.PUBLISH
            || command.operation() == DeltaRegistrationCommand.Operation.ABORT)) {
      throw new IllegalArgumentException(
          "Recovery requires matching receipt and independent request-drain confirmation");
    }
    validateEntry(entry);
    if (command.operation() == DeltaRegistrationCommand.Operation.HANDOFF
        && !command.runId().get().equals(entry.receipt.runId())) {
      throw new IllegalArgumentException("Handoff must identify the recorded writer run");
    }
    DeltaRegistrationJournal.Entry intent =
        new DeltaRegistrationJournal.Entry(
            UUID.randomUUID().toString(),
            entry.catalog,
            entry.namespace,
            entry.table,
            command,
            DeltaRegistrationJournal.Phase.INTENT,
            null);
    journal.write(intent);
    DeltaRegistrationReceipt receipt;
    try {
      receipt = client.submit(id(entry), command);
    } catch (RuntimeException uncertain) {
      journal.write(intent.phase(DeltaRegistrationJournal.Phase.STATUS_NEEDED, null));
      throw new IllegalStateException("DELTA_RECOVERY_STATUS_NEEDED");
    }
    journal.write(intent.phase(DeltaRegistrationJournal.Phase.OPERATOR_COMPLETED, receipt));
    return receipt;
  }
  /**
   * Reads privileged administrative status when initial ownership could not be established.
   *
   * @param id named table; the server enforces the existing administrative authorization gate
   * @return sanitized observed receipt, without implying request drain
   */
  public DeltaRegistrationReceipt administrativeStatus(Identifier id) {
    return client.submit(id, DeltaRegistrationCommand.administrativeStatus());
  }

  private static void validateEntry(DeltaRegistrationJournal.Entry entry) {
    if (entry.receipt != null) {
      DeltaRegistrationClient.validateBinding(entry.command, entry.receipt);
    }
  }

  private static Identifier id(DeltaRegistrationJournal.Entry entry) {
    return Identifier.of(new String[] {entry.namespace}, entry.table);
  }
}
