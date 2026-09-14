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
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Exercises the pinned Ranger client at its actual HTTP serialization boundary. */
class TestDeltaRangerClientExtension {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final long CREATED = 1789424560243L;
  private static final long UPDATED = 1789424560244L;
  private static final String BASE =
      "\"id\":7,\"guid\":\"retained-guid\",\"version\":19,\"isEnabled\":true,"
          + "\"createdBy\":\"creator\",\"updatedBy\":\"updater\","
          + "\"createTime\":"
          + CREATED
          + ",\"updateTime\":"
          + UPDATED;

  @Test
  void roleWritesPreserveDatesMembershipAndOptions() throws Exception {
    RangerRole role =
        MAPPER.readValue(
            "{"
                + BASE
                + ",\"name\":\"fixture-role\",\"description\":\"retained\","
                + "\"createdByUser\":\"creator\",\"options\":{\"audit\":{\"enabled\":true}},"
                + "\"users\":[{\"name\":\"reader\",\"isAdmin\":false}],"
                + "\"groups\":[{\"name\":\"group\",\"isAdmin\":true}],"
                + "\"roles\":[{\"name\":\"nested\",\"isAdmin\":false}]}",
            RangerRole.class);
    Assertions.assertAll(
        () -> assertWire(role, client -> client.createRole("fixture-service", role)),
        () -> assertWire(role, client -> client.updateRole(role.getId(), role)));
  }

  @Test
  void policyWritesPreserveDatesAndAllNestedRules() throws Exception {
    RangerPolicy policy =
        MAPPER.readValue(
            "{"
                + BASE
                + ",\"service\":\"fixture-service\",\"name\":\"fixture-policy\","
                + "\"policyType\":0,\"policyPriority\":1,\"isAuditEnabled\":true,"
                + "\"options\":{\"auditFilter\":{\"roles\":[\"foreign-role\"]}},"
                + "\"resources\":{\"column\":{\"values\":[\"secret\"],\"isExcludes\":false,\"isRecursive\":false}},"
                + "\"policyItems\":[{\"users\":[\"reader\"],\"roles\":[\"role\"],\"accesses\":[{\"type\":\"select\",\"isAllowed\":true}]}],"
                + "\"denyPolicyItems\":[{\"groups\":[\"denied\"],\"accesses\":[{\"type\":\"select\",\"isAllowed\":true}]}],"
                + "\"allowExceptions\":[{\"users\":[\"except-allow\"]}],\"denyExceptions\":[{\"users\":[\"except-deny\"]}],"
                + "\"conditions\":[{\"type\":\"ip\",\"values\":[\"127.0.0.1\"]}],"
                + "\"dataMaskPolicyItems\":[{\"users\":[\"reader\"],\"dataMaskInfo\":{\"dataMaskType\":\"CUSTOM\",\"valueExpr\":\"upper({col})\"}}],"
                + "\"rowFilterPolicyItems\":[{\"users\":[\"reader\"],\"rowFilterInfo\":{\"filterExpr\":\"region = 'allowed'\"}}],"
                + "\"policyLabels\":[\"foreign\"],\"zoneName\":\"zone\",\"isDenyAllElse\":false}",
            RangerPolicy.class);
    Assertions.assertAll(
        () -> assertWire(policy, client -> client.createPolicy(policy)),
        () -> assertWire(policy, client -> client.updatePolicy(policy.getId(), policy)),
        () ->
            assertWire(
                policy,
                client -> client.updatePolicy(policy.getService(), policy.getName(), policy)));
  }

  @Test
  void emptyPolicyResourcesAreRejectedBeforeAnyRequest() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            requests.incrementAndGet();
            byte[] response = MAPPER.writeValueAsBytes(new RangerPolicy());
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      DeltaRangerClientExtension client =
          new DeltaRangerClientExtension(
              "http://127.0.0.1:" + server.getAddress().getPort(), "simple", "fixture", "fixture");
      RangerPolicy empty = new RangerPolicy();
      Assertions.assertAll(
          () ->
              Assertions.assertThrows(
                  IllegalArgumentException.class, () -> client.createPolicy(empty)),
          () ->
              Assertions.assertThrows(
                  IllegalArgumentException.class, () -> client.updatePolicy(7, empty)),
          () ->
              Assertions.assertThrows(
                  IllegalArgumentException.class,
                  () -> client.updatePolicy("service", "policy", empty)),
          () -> Assertions.assertEquals(0, requests.get()));
    } finally {
      server.stop(0);
    }
  }

  private void assertWire(RangerBaseModelObject original, Write write) throws Throwable {
    JsonNode before = MAPPER.readTree(MAPPER.writeValueAsBytes(original));
    Date originalCreated = original.getCreateTime();
    Date originalUpdated = original.getUpdateTime();
    AtomicReference<JsonNode> received = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try {
            JsonNode body = MAPPER.readTree(exchange.getRequestBody());
            received.set(body);
            // Actual Ranger 2.8 GET timestamps are epoch milliseconds. Reject its client's bad
            // format.
            boolean valid =
                body.path("createTime").isIntegralNumber()
                    && body.path("updateTime").isIntegralNumber();
            byte[] response =
                MAPPER.writeValueAsBytes(
                    valid
                        ? body
                        : MAPPER.createObjectNode().put("message", "Invalid Date wire value"));
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(valid ? 200 : 400, response.length);
            exchange.getResponseBody().write(response);
          } finally {
            exchange.close();
          }
        });
    server.start();
    try {
      DeltaRangerClientExtension client =
          new DeltaRangerClientExtension(
              "http://127.0.0.1:" + server.getAddress().getPort(), "simple", "fixture", "fixture");
      RangerBaseModelObject response = write.apply(client);
      Assertions.assertEquals(
          before, received.get(), "All request fields must survive the wire boundary");
      Assertions.assertEquals(CREATED, received.get().path("createTime").longValue());
      Assertions.assertEquals(UPDATED, received.get().path("updateTime").longValue());
      Assertions.assertEquals(
          before, MAPPER.readTree(MAPPER.writeValueAsBytes(response)), "Typed response decoding");
      Assertions.assertEquals(
          before,
          MAPPER.readTree(MAPPER.writeValueAsBytes(original)),
          "Caller model must be unchanged");
      Assertions.assertSame(originalCreated, original.getCreateTime());
      Assertions.assertSame(originalUpdated, original.getUpdateTime());
    } finally {
      server.stop(0);
    }
  }

  private interface Write {
    RangerBaseModelObject apply(DeltaRangerClientExtension client) throws Throwable;
  }
}
