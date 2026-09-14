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

package org.apache.gravitino.spark.connector.delta.ranger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

/** Immutable authenticated query UGI snapshot, independent of metadata service credentials. */
public final class DeltaRangerQuerySubject {
  private final String user;
  private final Set<String> groups;

  private DeltaRangerQuerySubject(UserGroupInformation ugi) {
    user = DeltaRangerResourceKey.literal(ugi.getShortUserName());
    groups = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(ugi.getGroupNames())));
  }

  /**
   * Captures the current authenticated engine UGI and its actual groups.
   *
   * <p>Kyuubi's local user override is accepted only when it agrees with the current UGI. Token
   * cache keys do not attest a principal; token query mode therefore fails closed. Roles are
   * deliberately resolved by the current downloaded Ranger snapshot at evaluation.
   *
   * @param session active query session
   * @return immutable subject
   * @throws SecurityException if identity is missing, contradictory, or unqualified
   */
  public static DeltaRangerQuerySubject current(SparkSession session) {
    if (session == null) {
      throw new SecurityException("Authenticated query session required");
    }
    SparkContext context = session.sparkContext();
    String mode = context.getConf().get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, "simple");
    if ("token".equalsIgnoreCase(mode)
        || "token"
            .equalsIgnoreCase(session.conf().get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, mode))) {
      throw new SecurityException(
          "Token query mode requires an authenticated token-to-UGI binding; cache identity is not authentication");
    }
    try {
      UserGroupInformation current = UserGroupInformation.getCurrentUser();
      String local = context.getLocalProperty("kyuubi.session.user");
      if (local != null && !local.equals(current.getShortUserName())) {
        throw new SecurityException("Kyuubi session identity contradicts authenticated query UGI");
      }
      UserGroupInformation selected = AuthZUtils.getAuthzUgi(context);
      if (!selected.equals(current)) {
        throw new SecurityException("Query UGI changed during capture");
      }
      return new DeltaRangerQuerySubject(current);
    } catch (IOException e) {
      throw new SecurityException("Unable to capture authenticated query UGI", e);
    }
  }

  /**
   * @return authenticated short user name
   */
  public String user() {
    return user;
  }

  /**
   * @return immutable actual UGI groups, never metadata groups
   */
  public Set<String> groups() {
    return groups;
  }
}
