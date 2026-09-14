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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.admin.client.datatype.RESTResponse;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.util.GrantRevokeRoleRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Every nonprincipal client route reached by admitted Delta callbacks rejects HTTP errors. */
class TestDeltaRangerCallbackStatus {
  @Test
  void timestampBearingWritesReject503WithSuccessShapedJson() {
    RangerRole role = new RangerRole();
    role.setName("fixture-role");
    RangerPolicy policy = new RangerPolicy();
    policy.setName("fixture-policy");
    policy.setResources(Map.of("column", new RangerPolicy.RangerPolicyResource("secret")));
    Assertions.assertAll(
        () -> rejects503("createRole", "{}", client -> client.createRole("fixture", role)),
        () -> rejects503("updateRole", "{}", client -> client.updateRole(7, role)),
        () -> rejects503("createPolicy", "{}", client -> client.createPolicy(policy)),
        () -> rejects503("updatePolicyId", "{}", client -> client.updatePolicy(9, policy)),
        () ->
            rejects503(
                "updatePolicyName",
                "{}",
                client -> client.updatePolicy("fixture", "policy", policy)));
  }

  @Test
  void policyDeletionAndRoleMembershipReject503() {
    GrantRevokeRoleRequest membership = new GrantRevokeRoleRequest();
    Assertions.assertAll(
        () ->
            rejects503(
                "deletePolicy",
                "{}",
                client -> {
                  client.deletePolicy(9);
                  return null;
                }),
        () ->
            rejects503(
                "deleteRole",
                "{}",
                client -> {
                  client.deleteRole("role", "admin", "fixture");
                  return null;
                }),
        () ->
            rejects503(
                "grantRole",
                "{\"statusCode\":0}",
                client -> client.grantRole("fixture", membership)),
        () ->
            rejects503(
                "revokeRole",
                "{\"statusCode\":0}",
                client -> client.revokeRole("fixture", membership)));
  }

  @Test
  void readsCannotInterpret503AsAValidModelOrAbsence() {
    Assertions.assertAll(
        () ->
            rejects503(
                "getService", "{\"name\":\"fixture\"}", client -> client.getService("fixture")),
        () ->
            rejects503(
                "getServiceDef",
                "{\"name\":\"fixture\"}",
                client -> client.getServiceDef("fixture")),
        () ->
            rejects503(
                "getRole",
                "{\"name\":\"fixture\"}",
                client -> client.getRole("fixture", "admin", "fixture")),
        () ->
            rejects503(
                "findPolicies",
                "[]",
                client -> client.findPolicies(Map.of("serviceName", "fixture"))));
  }

  @Test
  void successfulDeletesAndMembershipPreserveEndpointAndTypedPayload() throws Exception {
    ObjectMapper json = new ObjectMapper();
    List<String> requests = new ArrayList<>();
    List<JsonNode> bodies = new ArrayList<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            if ("DELETE".equals(exchange.getRequestMethod())) {
              exchange.sendResponseHeaders(204, -1);
            } else {
              bodies.add(json.readTree(exchange.getRequestBody()));
              byte[] response =
                  "{\"statusCode\":0,\"msgDesc\":\"accepted\"}".getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "application/json");
              exchange.sendResponseHeaders(200, response.length);
              exchange.getResponseBody().write(response);
            }
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      DeltaRangerClientExtension client =
          new DeltaRangerClientExtension(
              "http://127.0.0.1:" + server.getAddress().getPort(), "simple", "fixture", "fixture");
      client.deletePolicy(9);
      client.deleteRole("owned-role", "admin", "fixture");
      GrantRevokeRoleRequest membership = new GrantRevokeRoleRequest();
      membership.setGrantor("admin");
      membership.setTargetRoles(Set.of("owned-role"));
      membership.setUsers(Set.of("reader"));
      membership.setGroups(Set.of("group"));
      membership.setRoles(Set.of("nested"));
      membership.setGrantOption(false);
      JsonNode expected = json.readTree(json.writeValueAsBytes(membership));
      RESTResponse granted = client.grantRole("fixture", membership);
      RESTResponse revoked = client.revokeRole("fixture", membership);
      Assertions.assertEquals(0, json.valueToTree(granted).path("statusCode").asInt(-1));
      Assertions.assertEquals(0, json.valueToTree(revoked).path("statusCode").asInt(-1));
      Assertions.assertEquals(List.of(expected, expected), bodies);
      Assertions.assertEquals(expected, json.readTree(json.writeValueAsBytes(membership)));
      Assertions.assertEquals(4, requests.size());
      Assertions.assertEquals("DELETE /service/public/v2/api/policy/9", requests.get(0));
      Assertions.assertTrue(
          requests.get(1).startsWith("DELETE /service/public/v2/api/roles/name/owned-role?"));
      Assertions.assertTrue(requests.get(1).contains("execUser=admin"));
      Assertions.assertTrue(requests.get(1).contains("serviceName=fixture"));
      Assertions.assertEquals("PUT /service/public/v2/api/roles/grant/fixture", requests.get(2));
      Assertions.assertEquals("PUT /service/public/v2/api/roles/revoke/fixture", requests.get(3));
    } finally {
      server.stop(0);
    }
  }

  private void rejects503(String route, String body, Request request) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            attempts.incrementAndGet();
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(503, bytes.length);
            exchange.getResponseBody().write(bytes);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      DeltaRangerClientExtension client =
          new DeltaRangerClientExtension(
              "http://127.0.0.1:" + server.getAddress().getPort(), "simple", "fixture", "fixture");
      Class<? extends Exception> expected =
          route.startsWith("get") || route.equals("findPolicies")
              ? RangerServiceException.class
              : AuthorizationPluginException.class;
      Assertions.assertAll(
          () -> Assertions.assertThrows(expected, () -> request.apply(client), route),
          () -> Assertions.assertEquals(1, attempts.get(), route + " must not retry"));
    } finally {
      server.stop(0);
    }
  }

  private interface Request {
    Object apply(DeltaRangerClientExtension client) throws Exception;
  }
}
