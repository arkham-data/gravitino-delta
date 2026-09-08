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

package org.apache.gravitino.catalog.lakehouse.delta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.TableEntity;

/** Durable insert and complete-row CAS, with no caller I/O inside store callbacks. */
final class DeltaRegistrationStore {
  private final EntityStore store;

  DeltaRegistrationStore(EntityStore store) {
    this.store = store;
  }

  DeltaRegistrationState load(NameIdentifier ident) {
    try {
      return DeltaRegistrationState.from(
          store.get(ident, Entity.EntityType.TABLE, TableEntity.class));
    } catch (IOException e) {
      throw unavailable();
    }
  }

  DeltaRegistrationState insert(TableEntity candidate) {
    DeltaRegistrationState state = DeltaRegistrationState.from(candidate);
    TableEntity detached = state.entity();
    DeltaStorageAdmission.checkTableCandidate(detached);
    try {
      store.put(detached, false);
      return state;
    } catch (IOException e) {
      throw unavailable();
    }
  }

  DeltaRegistrationState replace(DeltaRegistrationState inspected, TableEntity candidate) {
    TableEntity expected = inspected.entity();
    DeltaRegistrationState next = DeltaRegistrationState.from(candidate);
    TableEntity detached = next.entity();
    if (!expected.id().equals(detached.id())
        || !expected.name().equals(detached.name())
        || !expected.namespace().equals(detached.namespace()))
      throw new IllegalArgumentException("REGISTRATION_IDENTITY_CHANGED");
    DeltaStorageAdmission.checkTableCandidate(detached);
    NameIdentifier ident = NameIdentifier.of(expected.namespace(), expected.name());
    try {
      return DeltaRegistrationState.from(
          store.update(
              ident,
              TableEntity.class,
              Entity.EntityType.TABLE,
              current -> {
                if (!current.equals(expected))
                  throw new IllegalStateException("REGISTRATION_CHANGED");
                return detached;
              }));
    } catch (IOException e) {
      throw unavailable();
    }
  }

  List<DeltaRegistrationState> list(Namespace namespace) {
    try {
      List<DeltaRegistrationState> result = new ArrayList<>();
      for (TableEntity partial :
          store.list(namespace, TableEntity.class, Entity.EntityType.TABLE)) {
        try {
          result.add(load(NameIdentifier.of(namespace, partial.name())));
        } catch (NoSuchEntityException e) {
          /* A concurrently removed identifier is no longer a candidate. */
        }
      }
      return result;
    } catch (IOException e) {
      throw unavailable();
    }
  }

  private static IllegalStateException unavailable() {
    return new IllegalStateException("REGISTRATION_STORE_UNAVAILABLE");
  }
}
