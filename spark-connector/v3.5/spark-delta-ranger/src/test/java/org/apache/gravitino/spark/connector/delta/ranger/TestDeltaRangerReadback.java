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
import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDeltaRangerReadback {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void resumeRequiresOriginalRoleIdsAndExactPrivileges() throws Exception {
    JsonNode original = JSON.readTree("{\"roles\":[{\"id\":7,\"name\":\"canonical\"}]}");
    TestDeltaRangerDownloadedPolicies.assertResumeRoleIds(original, original, List.of("canonical"));
    for (String changed :
        List.of(
            "{\"roles\":[]}",
            "{\"roles\":[{\"id\":8,\"name\":\"canonical\"}]}",
            "{\"roles\":[{\"id\":7,\"name\":\"canonical\"},{\"id\":7,\"name\":\"canonical\"}]}")) {
      JsonNode current = JSON.readTree(changed);
      Assertions.assertThrows(
          AssertionError.class,
          () ->
              TestDeltaRangerDownloadedPolicies.assertResumeRoleIds(
                  original, current, List.of("canonical")));
    }
    var expected = List.of(SecurableObjects.ofCatalog("a", List.of(Privileges.UseCatalog.allow())));
    TestDeltaRangerDownloadedPolicies.assertExactGrants(expected, expected);
    for (var changed :
        List.of(
            List.of(SecurableObjects.ofCatalog("b", List.of(Privileges.UseCatalog.allow()))),
            List.of(SecurableObjects.ofCatalog("a", List.of(Privileges.UseCatalog.deny()))),
            List.of(
                SecurableObjects.parse(
                    "a.s.t", MetadataObject.Type.TABLE, List.of(Privileges.SelectTable.allow()))),
            List.of(expected.get(0), expected.get(0)))) {
      Assertions.assertThrows(
          AssertionError.class,
          () -> TestDeltaRangerDownloadedPolicies.assertExactGrants(expected, changed));
    }
  }

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
