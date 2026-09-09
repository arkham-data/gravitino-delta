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

import java.util.Objects;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;

/** Delta schemas and registrations are stored in the managed entity store. */
public final class DeltaCatalogCapability implements Capability {
  /** Creates the managed-storage capability declaration. */
  public DeltaCatalogCapability() {}
  /** {@inheritDoc} */
  @Override
  public CapabilityResult managedStorage(Scope scope) {
    Objects.requireNonNull(scope, "scope");
    return scope == Scope.TABLE || scope == Scope.SCHEMA
        ? CapabilityResult.SUPPORTED
        : CapabilityResult.unsupported("Delta catalog does not manage storage for " + scope);
  }
}
