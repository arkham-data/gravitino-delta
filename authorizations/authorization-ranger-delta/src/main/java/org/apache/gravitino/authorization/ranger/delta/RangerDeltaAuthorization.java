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

import java.util.Map;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;

/** Optional Delta authorization provider for a provisioned Ranger 2.8 service. */
public final class RangerDeltaAuthorization extends BaseAuthorization<RangerDeltaAuthorization> {
  /** Creates the ServiceLoader provider. */
  public RangerDeltaAuthorization() {}
  /** {@inheritDoc} */
  @Override
  public String shortName() {
    return "ranger-delta";
  }
  /** {@inheritDoc} */
  @Override
  public AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> properties) {
    if (!"lakehouse-delta".equals(catalogProvider)) {
      throw new IllegalArgumentException("ranger-delta requires lakehouse-delta");
    }
    return new RangerDeltaAuthorizationPlugin(metalake, properties);
  }
}
