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
package org.apache.gravitino.authorization.ranger.delta;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.AuthorizationSecurableObject;

/** Complete resource; descendant wildcards are introduced only by the translator. */
final class DeltaRangerResource implements AuthorizationSecurableObject {
  static final List<String> KEYS = List.of("catalog", "database", "table", "column");
  private final List<String> names;
  private final List<AuthorizationPrivilege> privileges;

  DeltaRangerResource(List<String> names, List<AuthorizationPrivilege> privileges) {
    this.names = List.copyOf(names);
    this.privileges = List.copyOf(privileges);
    validateAuthorizationMetadataObject();
  }
  /** {@inheritDoc} */
  @Override
  public String name() {
    return names.get(3);
  }
  /** {@inheritDoc} */
  @Override
  public String parent() {
    return String.join(".", names.subList(0, 3));
  }
  /** {@inheritDoc} */
  @Override
  public List<String> names() {
    return names;
  }
  /** {@inheritDoc} */
  @Override
  public Type type() {
    return () -> MetadataObject.Type.TABLE;
  }
  /** {@inheritDoc} */
  @Override
  public List<AuthorizationPrivilege> privileges() {
    return privileges;
  }
  /** {@inheritDoc} */
  @Override
  public void validateAuthorizationMetadataObject() {
    if (names.size() != 4 || names.stream().anyMatch(String::isEmpty)) {
      throw new IllegalArgumentException("Complete Delta resources required");
    }
  }
}
