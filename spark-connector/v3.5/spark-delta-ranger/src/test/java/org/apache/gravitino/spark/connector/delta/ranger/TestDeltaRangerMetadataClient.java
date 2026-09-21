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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.CustomTokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

class TestDeltaRangerMetadataClient {
  @Test
  void lazyReadSupplierOwnsOnlyItsClientAndDoesNotUseMutationActor() {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog metadata = mock(Catalog.class);
    when(client.loadCatalog("delta_a")).thenReturn(metadata);
    AtomicInteger opens = new AtomicInteger();
    AtomicInteger closes = new AtomicInteger();
    doAnswer(
            call -> {
              closes.incrementAndGet();
              return null;
            })
        .when(client)
        .close();
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    lease.configure(
        "delta_a",
        Duration.ofMinutes(5),
        () -> {
          opens.incrementAndGet();
          return client;
        });
    assertEquals(0, opens.get());
    assertSame(metadata, lease.get());
    assertSame(metadata, lease.get());
    assertEquals(1, opens.get());
    lease.close();
    lease.close();
    assertEquals(1, closes.get());
    assertThrows(IllegalStateException.class, lease::get);
  }

  @Test
  void missingConfigurationAndRebindingRejectBeforeClientUse() {
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    assertThrows(IllegalStateException.class, lease::get);
    lease.configure("delta_a", Duration.ofMinutes(1), () -> mock(GravitinoClient.class));
    assertThrows(
        IllegalStateException.class,
        () -> lease.configure("delta_b", Duration.ofMinutes(1), () -> mock(GravitinoClient.class)));
    lease.close();
  }

  @Test
  void lifetimeExpiresAndClosesClientWithoutFurtherQuery() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog metadata = mock(Catalog.class);
    when(client.loadCatalog("delta_a")).thenReturn(metadata);
    CountDownLatch closed = new CountDownLatch(1);
    doAnswer(
            call -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient()) {
      lease.configure("delta_a", Duration.ofMillis(80), () -> client);
      assertSame(metadata, lease.get());
      assertTrue(closed.await(5, TimeUnit.SECONDS));
      assertThrows(IllegalStateException.class, lease::get);
    }
  }

  @Test
  void blockingCleanupCannotStarveAnotherLeaseExpiration() throws Exception {
    GravitinoClient a = mock(GravitinoClient.class);
    GravitinoClient b = mock(GravitinoClient.class);
    when(a.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    when(b.loadCatalog("delta_b")).thenReturn(mock(Catalog.class));
    CountDownLatch cleanupEntered = new CountDownLatch(1);
    CountDownLatch cleanupRelease = new CountDownLatch(1);
    CountDownLatch bClosed = new CountDownLatch(1);
    doAnswer(
            call -> {
              cleanupEntered.countDown();
              cleanupRelease.await(5, TimeUnit.SECONDS);
              return null;
            })
        .when(a)
        .close();
    doAnswer(
            call -> {
              bClosed.countDown();
              return null;
            })
        .when(b)
        .close();
    DeltaRangerMetadataClient first = new DeltaRangerMetadataClient();
    DeltaRangerMetadataClient second = new DeltaRangerMetadataClient();
    try {
      first.configure("delta_a", Duration.ofMillis(50), () -> a);
      first.get();
      assertTrue(cleanupEntered.await(2, TimeUnit.SECONDS));
      second.configure("delta_b", Duration.ofMillis(50), () -> b);
      second.get();
      assertTrue(bClosed.await(1, TimeUnit.SECONDS), "Another cleanup blocked expiration");
      assertThrows(IllegalStateException.class, second::get);
      verify(b, times(1)).loadCatalog("delta_b");
    } finally {
      cleanupRelease.countDown();
      first.close();
      second.close();
    }
  }

  @Test
  void closeDoesNotWaitForStalledInitialBuilderAndClosesLateClient() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    CountDownLatch builderEntered = new CountDownLatch(1);
    CountDownLatch builderRelease = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    doAnswer(
            call -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();
    ExecutorService threads = Executors.newFixedThreadPool(2);
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    lease.configure(
        "delta_a",
        Duration.ofMinutes(1),
        () -> {
          builderEntered.countDown();
          try {
            builderRelease.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
          return client;
        });
    try {
      Future<?> read = threads.submit(() -> assertThrows(IllegalStateException.class, lease::get));
      assertTrue(builderEntered.await(2, TimeUnit.SECONDS));
      Future<?> close = threads.submit(lease::close);
      close.get(1, TimeUnit.SECONDS);
      builderRelease.countDown();
      read.get(2, TimeUnit.SECONDS);
      assertTrue(closed.await(2, TimeUnit.SECONDS));
      verify(client, times(0)).loadCatalog("delta_a");
    } finally {
      builderRelease.countDown();
      threads.shutdownNow();
      lease.close();
    }
  }

  @Test
  void monotonicDeadlineRejectsReadsAndResultsWhenTimerNeverRuns() {
    AtomicLong time = new AtomicLong();
    ScheduledExecutorService delayedTimer = mock(ScheduledExecutorService.class);
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    when(client.loadCatalog("delta_a")).thenReturn(catalog);
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient(time::get, delayedTimer)) {
      lease.configure("delta_a", Duration.ofMillis(1), () -> client);
      assertSame(catalog, lease.get());
      time.set(TimeUnit.MILLISECONDS.toNanos(1));
      assertThrows(IllegalStateException.class, lease::get);
      verify(client, times(1)).loadCatalog("delta_a");
    }
    time.set(0);
    GravitinoClient slow = mock(GravitinoClient.class);
    when(slow.loadCatalog("delta_a"))
        .thenAnswer(
            call -> {
              time.set(TimeUnit.MILLISECONDS.toNanos(1));
              return catalog;
            });
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient(time::get, delayedTimer)) {
      lease.configure("delta_a", Duration.ofMillis(1), () -> slow);
      assertThrows(IllegalStateException.class, lease::get);
    }
  }

  @Test
  void scopedReadUsesCatalogFromOwnedClientAndKeepsLiveStampValid() {
    AtomicLong time = new AtomicLong();
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    Object first = new Object();
    Object second = new Object();
    when(client.loadCatalog("delta_a")).thenReturn(catalog);
    try (DeltaRangerMetadataClient lease =
        new DeltaRangerMetadataClient(time::get, mock(ScheduledExecutorService.class))) {
      lease.configure("delta_a", Duration.ofMinutes(1), () -> client);

      assertSame(first, lease.read(loaded -> assertCatalogAndReturn(catalog, loaded, first)));
      DeltaRangerMetadataClient.MetadataLeaseStamp stamp = lease.leaseStamp();
      assertEquals("MetadataLeaseStamp", stamp.toString());
      assertFalse(Serializable.class.isAssignableFrom(stamp.getClass()));
      assertSame(second, lease.read(loaded -> assertCatalogAndReturn(catalog, loaded, second)));
      lease.requireLive(stamp);

      verify(client, times(2)).loadCatalog("delta_a");
    }
  }

  @Test
  void leaseStampRequiresStartedOwnedClient() {
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    try (DeltaRangerMetadataClient unconfigured = new DeltaRangerMetadataClient();
        DeltaRangerMetadataClient configured = new DeltaRangerMetadataClient()) {
      assertThrows(IllegalStateException.class, unconfigured::leaseStamp);
      configured.configure("delta_a", Duration.ofMinutes(1), () -> client);
      assertThrows(IllegalStateException.class, configured::leaseStamp);

      configured.get();
      configured.requireLive(configured.leaseStamp());
    }
  }

  @Test
  void leaseStampCannotValidateAnotherClientLease() {
    GravitinoClient firstClient = mock(GravitinoClient.class);
    GravitinoClient secondClient = mock(GravitinoClient.class);
    when(firstClient.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    when(secondClient.loadCatalog("delta_b")).thenReturn(mock(Catalog.class));
    try (DeltaRangerMetadataClient first = new DeltaRangerMetadataClient();
        DeltaRangerMetadataClient second = new DeltaRangerMetadataClient()) {
      first.configure("delta_a", Duration.ofMinutes(1), () -> firstClient);
      second.configure("delta_b", Duration.ofMinutes(1), () -> secondClient);
      first.get();
      second.get();

      assertThrows(IllegalStateException.class, () -> second.requireLive(first.leaseStamp()));
    }
  }

  @Test
  void closedAndMonotonicallyExpiredLeasesRejectPriorStamp() {
    AtomicLong time = new AtomicLong();
    ScheduledExecutorService delayedTimer = mock(ScheduledExecutorService.class);
    GravitinoClient closedClient = mock(GravitinoClient.class);
    GravitinoClient expiredClient = mock(GravitinoClient.class);
    when(closedClient.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    when(expiredClient.loadCatalog("delta_b")).thenReturn(mock(Catalog.class));
    DeltaRangerMetadataClient closed = new DeltaRangerMetadataClient(time::get, delayedTimer);
    DeltaRangerMetadataClient expired = new DeltaRangerMetadataClient(time::get, delayedTimer);
    try {
      closed.configure("delta_a", Duration.ofMillis(1), () -> closedClient);
      expired.configure("delta_b", Duration.ofMillis(1), () -> expiredClient);
      closed.get();
      expired.get();
      DeltaRangerMetadataClient.MetadataLeaseStamp closedStamp = closed.leaseStamp();
      DeltaRangerMetadataClient.MetadataLeaseStamp expiredStamp = expired.leaseStamp();

      closed.close();
      assertThrows(IllegalStateException.class, () -> closed.requireLive(closedStamp));
      time.set(TimeUnit.MILLISECONDS.toNanos(1));
      assertThrows(IllegalStateException.class, () -> expired.requireLive(expiredStamp));
    } finally {
      closed.close();
      expired.close();
    }
  }

  @Test
  void scopedTableReadRejectsValueWhenDeadlinePassesDuringLoadTable() throws Exception {
    AtomicLong time = new AtomicLong();
    ScheduledExecutorService delayedTimer = mock(ScheduledExecutorService.class);
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    Table table = mock(Table.class);
    NameIdentifier name = NameIdentifier.of("db", "table");
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(client.loadCatalog("delta_a")).thenReturn(catalog);
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(tables.loadTable(name))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return table;
            });
    ExecutorService reads = Executors.newSingleThreadExecutor();
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient(time::get, delayedTimer)) {
      lease.configure("delta_a", Duration.ofMillis(1), () -> client);
      Future<?> read =
          reads.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () -> lease.read(loaded -> loaded.asTableCatalog().loadTable(name))));
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      time.set(TimeUnit.MILLISECONDS.toNanos(1));
      release.countDown();
      read.get(2, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      reads.shutdownNow();
    }
  }

  @Test
  void closeDoesNotWaitForStalledScopedTableReadAndRejectsLateValue() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    NameIdentifier name = NameIdentifier.of("db", "table");
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(client.loadCatalog("delta_a")).thenReturn(catalog);
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(tables.loadTable(name))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return mock(Table.class);
            });
    ExecutorService threads = Executors.newFixedThreadPool(2);
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    lease.configure("delta_a", Duration.ofMinutes(1), () -> client);
    try {
      Future<?> read =
          threads.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () -> lease.read(loaded -> loaded.asTableCatalog().loadTable(name))));
      assertTrue(entered.await(2, TimeUnit.SECONDS));
      Future<?> close = threads.submit(lease::close);
      close.get(1, TimeUnit.SECONDS);
      release.countDown();
      read.get(2, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      threads.shutdownNow();
      lease.close();
    }
  }

  @Test
  void scopedReadPreservesCallbackFailureWhileLeaseRemainsLive() {
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    IllegalArgumentException malformed = new IllegalArgumentException("malformed receipt");
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient()) {
      lease.configure("delta_a", Duration.ofMinutes(1), () -> client);

      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  lease.read(
                      catalog -> {
                        throw malformed;
                      }));

      assertSame(malformed, failure);
    }
  }

  @Test
  void scopedReadRejectsCallbackFailureWhenLeaseExpiresDuringCallback() {
    AtomicLong time = new AtomicLong();
    GravitinoClient client = mock(GravitinoClient.class);
    when(client.loadCatalog("delta_a")).thenReturn(mock(Catalog.class));
    try (DeltaRangerMetadataClient lease =
        new DeltaRangerMetadataClient(time::get, mock(ScheduledExecutorService.class))) {
      lease.configure("delta_a", Duration.ofMillis(1), () -> client);

      assertThrows(
          IllegalStateException.class,
          () ->
              lease.read(
                  catalog -> {
                    time.set(TimeUnit.MILLISECONDS.toNanos(1));
                    throw new IllegalArgumentException("malformed receipt");
                  }));
    }
  }

  @Test
  void initialConstructionTimeCountsAgainstDeadlineAndLateClientCloses() throws Exception {
    AtomicLong time = new AtomicLong();
    GravitinoClient client = mock(GravitinoClient.class);
    CountDownLatch closed = new CountDownLatch(1);
    doAnswer(
            call -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();
    try (DeltaRangerMetadataClient lease =
        new DeltaRangerMetadataClient(time::get, mock(ScheduledExecutorService.class))) {
      lease.configure(
          "delta_a",
          Duration.ofMillis(1),
          () -> {
            time.set(TimeUnit.MILLISECONDS.toNanos(1));
            return client;
          });
      assertThrows(IllegalStateException.class, lease::get);
      assertTrue(closed.await(2, TimeUnit.SECONDS));
      verify(client, times(0)).loadCatalog("delta_a");
    }
  }

  @Test
  void invalidLifetimeAndMissingAuthenticationReject() {
    DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient();
    assertThrows(
        IllegalArgumentException.class,
        () -> lease.configure("delta_a", Duration.ZERO, () -> mock(GravitinoClient.class)));
    assertThrows(
        IllegalArgumentException.class,
        () -> lease.configure("http://localhost", "lake", "delta_a", Duration.ofMinutes(1), null));
  }

  @Test
  void realClientBuilderUsesSeparateMetadataPrincipalAndPreservesQueryUgi() throws Exception {
    assertMetadataTransport(false);
    assertMetadataTransport(true);
  }

  @Test
  void authenticationCallbackCannotFallBackOrChangeMetalake() {
    try (DeltaRangerMetadataClient missing = new DeltaRangerMetadataClient();
        DeltaRangerMetadataClient changed = new DeltaRangerMetadataClient()) {
      missing.configure(
          "http://localhost:1", "lake", "delta_a", Duration.ofMinutes(1), builder -> {});
      assertThrows(IllegalArgumentException.class, missing::get);
      changed.configure(
          "http://localhost:1",
          "lake",
          "delta_a",
          Duration.ofMinutes(1),
          builder -> {
            builder.withSimpleAuth("metadata-service");
            builder.withMetalake("other-lake");
          });
      assertThrows(IllegalArgumentException.class, changed::get);
    }
  }

  @Test
  void expiryCanCloseAnInFlightMetadataRead() throws Exception {
    GravitinoClient client = mock(GravitinoClient.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    when(client.loadCatalog("delta_a"))
        .thenAnswer(
            call -> {
              entered.countDown();
              if (!closed.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Read did not drain");
              }
              return mock(Catalog.class);
            });
    doAnswer(
            call -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();
    ExecutorService reads = Executors.newSingleThreadExecutor();
    try (DeltaRangerMetadataClient lease = new DeltaRangerMetadataClient()) {
      lease.configure("delta_a", Duration.ofMillis(100), () -> client);
      Future<?> read = reads.submit(() -> assertThrows(IllegalStateException.class, lease::get));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(closed.await(1, TimeUnit.SECONDS));
      read.get(5, TimeUnit.SECONDS);
    } finally {
      closed.countDown();
      reads.shutdownNow();
    }
  }

  private static void assertMetadataTransport(boolean token) throws Exception {
    List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    List<String> paths = Collections.synchronizedList(new ArrayList<>());
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
          paths.add(exchange.getRequestURI().getPath());
          String audit =
              "\"audit\":{\"creator\":\"fixture\",\"createTime\":\"2026-09-14T00:00:00Z\"}";
          String body =
              exchange.getRequestURI().getPath().endsWith("/catalogs/delta_a")
                  ? "{\"code\":0,\"catalog\":{\"name\":\"delta_a\",\"type\":\"RELATIONAL\",\"provider\":\"lakehouse-delta\",\"properties\":{},"
                      + audit
                      + "}}"
                  : "{\"code\":0,\"metalake\":{\"name\":\"lake\",\"properties\":{}," + audit + "}}";
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, bytes.length);
          try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
          }
        });
    server.start();
    try (DeltaRangerMetadataClient metadata = new DeltaRangerMetadataClient()) {
      metadata.configure(
          "http://127.0.0.1:" + server.getAddress().getPort(),
          "lake",
          "delta_a",
          Duration.ofMinutes(1),
          builder -> {
            builder.withVersionCheckDisabled();
            if (token) {
              builder.withCustomTokenAuth(new MetadataToken());
            } else {
              builder.withSimpleAuth("metadata-service");
            }
          });
      assertTrue(paths.isEmpty());
      UserGroupInformation query =
          UserGroupInformation.createUserForTesting("query-reader", new String[] {"query-group"});
      query.doAs(
          (PrivilegedExceptionAction<Void>)
              () -> {
                assertEquals(
                    "delta_a",
                    metadata.read(
                        catalog -> {
                          assertEquals(
                              "query-reader",
                              DeltaRangerQuerySubject.current(
                                      TestDeltaRangerQuerySubject.session(null, "simple"))
                                  .user());
                          return catalog.name();
                        }));
                assertEquals(
                    "query-reader",
                    DeltaRangerQuerySubject.current(
                            TestDeltaRangerQuerySubject.session(null, "simple"))
                        .user());
                return null;
              });
      assertEquals(
          Arrays.asList("/api/metalakes/lake", "/api/metalakes/lake/catalogs/delta_a"), paths);
      String expected =
          token
              ? "Bearer metadata-token"
              : "Basic "
                  + Base64.getEncoder()
                      .encodeToString("metadata-service:dummy".getBytes(StandardCharsets.UTF_8));
      assertEquals(Arrays.asList(expected, expected), authorizations);
    } finally {
      server.stop(0);
    }
  }

  private static Object assertCatalogAndReturn(Catalog expected, Catalog actual, Object result) {
    assertSame(expected, actual);
    return result;
  }

  private static final class MetadataToken extends CustomTokenProvider {
    private MetadataToken() {
      schemeName = "Bearer";
    }

    @Override
    protected String getCustomTokenInfo() {
      return "metadata-token";
    }
  }
}
