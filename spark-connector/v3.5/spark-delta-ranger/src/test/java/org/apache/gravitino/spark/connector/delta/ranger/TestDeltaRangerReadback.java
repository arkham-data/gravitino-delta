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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDeltaRangerReadback {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void wrapperOrUnrelatedRoleCannotProveMembership() throws Exception {
    for (String response :
        new String[] {
          "{\"totalCount\":0,\"roles\":[]}",
          "{\"roles\":[{\"name\":\"other\",\"users\":[{\"name\":\"reader\",\"isAdmin\":false}]}]}"
        }) {
      JsonNode readback = JSON.readTree(response);
      Assertions.assertThrows(
          AssertionError.class,
          () ->
              TestDeltaRangerDownloadedPolicies.assertRoleMembership(
                  readback, "canonical", "reader"));
    }
  }

  @Test
  void exactRoleNeedsOnlyTheExpectedNonAdminUser() throws Exception {
    for (String members :
        new String[] {
          "\"users\":[]",
          "\"users\":[{\"name\":\"other\",\"isAdmin\":false}]",
          "\"users\":[{\"name\":\"reader\",\"isAdmin\":true}]",
          "\"users\":[{\"name\":\"reader\",\"isAdmin\":false}],\"groups\":[{\"name\":\"group\",\"isAdmin\":false}]",
          "\"users\":[{\"name\":\"reader\",\"isAdmin\":false}],\"roles\":[{\"name\":\"nested\",\"isAdmin\":false}]"
        }) {
      JsonNode readback = JSON.readTree("{\"roles\":[{\"name\":\"canonical\"," + members + "}]}");
      Assertions.assertThrows(
          AssertionError.class,
          () ->
              TestDeltaRangerDownloadedPolicies.assertRoleMembership(
                  readback, "canonical", "reader"));
    }
    JsonNode valid =
        JSON.readTree(
            "{\"roles\":[{\"name\":\"other\"},{\"name\":\"canonical\",\"users\":[{\"name\":\"reader\",\"isAdmin\":false}]}]}");
    TestDeltaRangerDownloadedPolicies.assertRoleMembership(valid, "canonical", "reader");
  }
}
