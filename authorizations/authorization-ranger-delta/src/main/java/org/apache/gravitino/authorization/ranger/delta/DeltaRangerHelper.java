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
import java.util.Set;
import org.apache.gravitino.authorization.AuthorizationPrivilege;
import org.apache.gravitino.authorization.ranger.RangerClientExtension;
import org.apache.gravitino.authorization.ranger.RangerHelper;

/** Metalake-scoped ordinary role identities on Ranger's global role namespace. */
final class DeltaRangerHelper extends RangerHelper {
  private final String prefix;

  DeltaRangerHelper(
      RangerClientExtension client,
      String admin,
      String service,
      Set<AuthorizationPrivilege> ownerPrivileges,
      List<String> resources,
      String metalakeId) {
    super(client, admin, service, ownerPrivileges, resources);
    prefix = "GRAVITINO_METALAKE_" + metalakeId + "_ROLE_";
  }
  /** {@inheritDoc} */
  @Override
  public String generateGravitinoRoleName(String name) {
    if (name.startsWith(prefix)
        || name.equals(GRAVITINO_OWNER_ROLE)
        || name.startsWith(GRAVITINO_METALAKE_OWNER_ROLE + "_")
        || name.startsWith(GRAVITINO_CATALOG_OWNER_ROLE + "_")) {
      return name;
    }
    return prefix + name;
  }
}
