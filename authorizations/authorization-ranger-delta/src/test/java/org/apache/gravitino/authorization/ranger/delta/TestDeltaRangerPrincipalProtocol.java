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

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.authorization.ranger.reference.VXGroup;
import org.apache.gravitino.authorization.ranger.reference.VXUser;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Exercises principal status and authoritative readback through the pinned HTTP client. */
class TestDeltaRangerPrincipalProtocol {
  @Test
  void bodyless204RequiresExactlyOneMatchingAuthoritativeUserWithoutRetry() throws Exception {
    try (Endpoint endpoint = new Endpoint(204, "{\"vXUsers\":[{\"name\":\"fresh\"}]}")) {
      Assertions.assertTrue(
          endpoint.client().createUser(VXUser.builder().withName("fresh").build()));
      Assertions.assertEquals(1, endpoint.posts.get());
      Assertions.assertEquals(1, endpoint.reads.get());
    }
  }

  @Test
  void noMissingWrongOrAmbiguousUserCanComplete204() throws Exception {
    for (String users :
        new String[] {
          "[]", "[{\"name\":\"other\"}]", "[{\"name\":\"fresh\"},{\"name\":\"fresh\"}]"
        }) {
      try (Endpoint endpoint = new Endpoint(204, "{\"vXUsers\":" + users + "}")) {
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> endpoint.client().createUser(VXUser.builder().withName("fresh").build()));
        Assertions.assertEquals(1, endpoint.posts.get());
        Assertions.assertEquals(
            1, endpoint.reads.get(), "Exactly one read, no poll or write replay");
      }
    }
  }

  @Test
  void actualGroup200JsonAndSearchModelsRemainSupported() throws Exception {
    try (Endpoint endpoint = new Endpoint(200, "{\"vXGroups\":[{\"name\":\"group\"}]}")) {
      Assertions.assertTrue(
          endpoint.client().createGroup(VXGroup.builder().withName("group").build()));
      Assertions.assertEquals(
          1, endpoint.client().searchGroup(Map.of("name", "group")).getListSize());
      Assertions.assertEquals(1, endpoint.posts.get());
      Assertions.assertEquals(1, endpoint.reads.get());
    }
  }

  @Test
  void everyPrincipalRouteRejects503EvenWhenItsBodyLooksSuccessful() throws Exception {
    Assertions.assertAll(
        () ->
            rejectsUnavailable(
                client -> client.createUser(VXUser.builder().withName("fresh").build())),
        () ->
            rejectsUnavailable(
                client -> client.createGroup(VXGroup.builder().withName("group").build())),
        () -> rejectsUnavailable(client -> client.searchUser(Map.of("name", "fresh"))),
        () -> rejectsUnavailable(client -> client.searchGroup(Map.of("name", "group"))));
  }

  @Test
  void nullGroupResponseCannotReportCreationSuccess() throws Exception {
    try (Endpoint endpoint = new Endpoint(200, "{}")) {
      endpoint.postBody = "null";
      Assertions.assertThrows(
          AuthorizationPluginException.class,
          () -> endpoint.client().createGroup(VXGroup.builder().withName("group").build()));
      Assertions.assertEquals(1, endpoint.posts.get());
    }
  }

  @Test
  void unavailableReadbackAfter204DoesNotRepeatCreation() throws Exception {
    try (Endpoint endpoint = new Endpoint(204, "{}")) {
      endpoint.readStatus = 503;
      Assertions.assertThrows(
          AuthorizationPluginException.class,
          () -> endpoint.client().createUser(VXUser.builder().withName("fresh").build()));
      Assertions.assertEquals(1, endpoint.posts.get());
      Assertions.assertEquals(1, endpoint.reads.get());
    }
  }

  private void rejectsUnavailable(Request request) throws Exception {
    try (Endpoint endpoint = new Endpoint(503, "{}")) {
      Assertions.assertThrows(
          AuthorizationPluginException.class, () -> request.apply(endpoint.client()));
      Assertions.assertEquals(
          1, endpoint.posts.get() + endpoint.reads.get(), "No retry/readback after 503");
    }
  }

  private interface Request {
    Object apply(DeltaRangerClientExtension client);
  }

  private static final class Endpoint implements AutoCloseable {
    private final HttpServer server;
    private final AtomicInteger posts = new AtomicInteger();
    private final AtomicInteger reads = new AtomicInteger();
    private volatile String postBody = "{}";
    private volatile int readStatus = 200;

    private Endpoint(int status, String readback) throws Exception {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/",
          exchange -> {
            try {
              boolean get = "GET".equals(exchange.getRequestMethod());
              if (get) {
                reads.incrementAndGet();
              } else {
                posts.incrementAndGet();
              }
              int responseStatus = get && status != 503 ? readStatus : status;
              byte[] body = (get ? readback : postBody).getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().add("Content-Type", "application/json");
              exchange.sendResponseHeaders(
                  responseStatus, responseStatus == 204 ? -1 : body.length);
              if (responseStatus != 204) {
                exchange.getResponseBody().write(body);
              }
            } finally {
              exchange.close();
            }
          });
      server.start();
    }

    private DeltaRangerClientExtension client() {
      return new DeltaRangerClientExtension(
          "http://127.0.0.1:" + server.getAddress().getPort(), "simple", "fixture", "fixture");
    }

    /** Stops the owned test endpoint. */
    @Override
    public void close() {
      server.stop(0);
    }
  }
}
