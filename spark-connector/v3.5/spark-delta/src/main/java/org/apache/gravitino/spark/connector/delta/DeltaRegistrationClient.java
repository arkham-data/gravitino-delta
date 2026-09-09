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
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.spark.sql.connector.catalog.Identifier;

/** Typed transport using current catalog clients; the manager retains client ownership. */
public final class DeltaRegistrationClient {
  private final Supplier<Catalog> metadata;
  private final Supplier<Catalog> actor;
  /**
   * Creates transport with distinct metadata and mutation identities.
   *
   * @param metadata current read-metadata catalog supplier
   * @param actor current operation actor catalog supplier
   */
  public DeltaRegistrationClient(Supplier<Catalog> metadata, Supplier<Catalog> actor) {
    this.metadata = metadata;
    this.actor = actor;
  }
  /**
   * Executes one command without retry. STATUS uses the operation actor and existing server gates.
   *
   * @param id named table
   * @param command production wire command
   * @return validated receipt
   */
  public DeltaRegistrationReceipt submit(Identifier id, DeltaRegistrationCommand command) {
    Catalog current = actor.get();
    Table result;
    if (command.operation() == DeltaRegistrationCommand.Operation.RESERVE
        || command.operation() == DeltaRegistrationCommand.Operation.ADOPT) {
      result =
          current
              .asTableCatalog()
              .createTable(
                  local(id),
                  new Column[0],
                  null,
                  Collections.singletonMap(
                      DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()));
    } else {
      result =
          current
              .asTableCatalog()
              .alterTable(
                  local(id),
                  TableChange.setProperty(
                      DeltaRegistrationCommand.COMMAND_PROPERTY, command.encode()));
    }
    DeltaRegistrationReceipt receipt = DeltaRegistrationReceipt.fromProperties(result.properties());
    validate(command, receipt);
    return receipt;
  }

  Catalog metadataCatalog() {
    return metadata.get();
  }

  Catalog actorCatalog() {
    return actor.get();
  }

  Optional<Observation> published(Identifier id) {
    Catalog catalog = metadata.get();
    try {
      DeltaRegistrationReceipt receipt =
          DeltaRegistrationReceipt.fromProperties(
              catalog.asTableCatalog().loadTable(local(id)).properties());
      if (receipt.state() != DeltaRegistrationReceipt.State.PUBLISHED) {
        throw bindingError();
      }
      return Optional.of(new Observation(catalog.name(), receipt));
    } catch (NoSuchTableException absent) {
      return Optional.empty();
    }
  }

  static NameIdentifier local(Identifier id) {
    if (id.namespace().length != 1) {
      throw new IllegalArgumentException("Delta requires exactly one namespace level");
    }
    return NameIdentifier.of(id.namespace()[0], id.name());
  }

  static void validate(DeltaRegistrationCommand command, DeltaRegistrationReceipt receipt) {
    if (command.runId().isPresent() && !command.runId().get().equals(receipt.runId())) {
      throw bindingError();
    }
    if (command.token().isPresent() && !command.token().get().equals(receipt.identity().token())) {
      throw bindingError();
    }
    if (command.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE) {
      DeltaRegistrationIdentity expected = command.expected().get();
      if (receipt.identity().id() != expected.id()
          || receipt.identity().generation() != Math.addExact(expected.generation(), 1)
          || !receipt.identity().token().equals(command.replacementToken().get())) {
        throw bindingError();
      }
    } else if (command.expected().isPresent()
        && !command.expected().get().equals(receipt.identity())) {
      throw bindingError();
    }
    if (command.operation() == DeltaRegistrationCommand.Operation.RESERVE
        || command.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE) {
      if (receipt.operation() != command.operation()
          || !receipt.marker().isPresent()
          || receipt.state()
              != (command.operation() == DeltaRegistrationCommand.Operation.RESERVE
                  ? DeltaRegistrationReceipt.State.RESERVED
                  : DeltaRegistrationReceipt.State.REPLACING)) {
        throw bindingError();
      }
      DeltaOperationMarker marker = receipt.marker().get();
      if (command.operation() == DeltaRegistrationCommand.Operation.RESERVE
          && receipt.identity().generation() != 0) {
        throw bindingError();
      }
      if (command.exactSchemaJson().isPresent()
          && (!command.exactSchemaJson().get().equals(marker.schemaJson())
              || marker.schemaContract().isPresent())) {
        throw bindingError();
      }
      if (command.schemaContract().isPresent()
          && (!marker.schemaContract().isPresent()
              || !command.schemaContract().get().equals(marker.schemaContract().get()))) {
        if (!marker.schemaContract().isPresent()
            || !command
                .schemaContract()
                .get()
                .requiredSchemaJson()
                .equals(marker.schemaContract().get().requiredSchemaJson())
            || command.schemaContract().get().operationMode()
                != marker.schemaContract().get().operationMode()) {
          throw bindingError();
        }
      }
    }
  }

  static IllegalStateException bindingError() {
    return new IllegalStateException("DELTA_REGISTRATION_BINDING_MISMATCH");
  }

  static final class Observation {
    final String catalog;
    final DeltaRegistrationReceipt receipt;

    Observation(String catalog, DeltaRegistrationReceipt receipt) {
      this.catalog = catalog;
      this.receipt = receipt;
    }
  }
}
