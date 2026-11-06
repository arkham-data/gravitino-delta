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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerRoles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDeltaRangerCreateReadiness {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void onlyCompletedExactCandidateReadiesBothReachablePolicyGaps(boolean currentAssigned)
      throws Exception {
    try (Pending pending = new Pending(currentAssigned)) {
      Future<?> wait = pending.await(() -> {});
      pending.requireWaiting();
      assertFalse(wait.isDone());
      assertThrows(SecurityException.class, pending.fixture.engine::requireReady);
      assertTrue(pending.fixture.requests.isEmpty());
      pending.complete();
      wait.get(5, TimeUnit.SECONDS);
      pending.fixture.engine.requireReady();
      assertTrue(pending.fixture.requests.isEmpty(), "Readiness must not evaluate or authorize");
    }
  }

  @Test
  void neverCompletedCandidateTimesOutWithoutEvaluation() throws Exception {
    try (Pending pending = new Pending(false)) {
      long started = System.nanoTime();
      assertThrows(
          SecurityException.class, () -> pending.fixture.engine.awaitCreatePublication(() -> {}));
      assertTrue(System.nanoTime() - started >= TimeUnit.MILLISECONDS.toNanos(900));
      assertThrows(SecurityException.class, pending.fixture.engine::requireReady);
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"candidate", "roles", "binding", "definition"})
  void replacedOrMalformedPublicationCannotBecomeReady(String change) throws Exception {
    try (Pending pending = new Pending(false)) {
      Future<?> wait = pending.await(() -> {});
      pending.requireWaiting();
      switch (change) {
        case "candidate":
          pending.candidate.set(
              new RangerAuthContext(
                  Collections.emptyMap(), null, pending.fixture.plugin.getRoles(), null));
          break;
        case "roles":
          RangerRoles replacement = new RangerRoles();
          replacement.setServiceName("service");
          replacement.setRoleVersion(1L);
          pending.roles.set(replacement);
          break;
        case "binding":
          pending.binding.set(
              Collections.singletonMap("ranger.plugin.spark.gravitino.metalake", "foreign"));
          break;
        default:
          pending.definition.set(null);
      }
      pending.complete();
      assertInstanceOf(
          RuntimeException.class,
          assertThrows(ExecutionException.class, () -> wait.get(5, TimeUnit.SECONDS)).getCause());
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @Test
  void notificationWithoutCandidateCompletionCannotCertifyCreateReadiness() throws Exception {
    try (Pending pending = new Pending(false)) {
      Future<?> wait = pending.await(() -> {});
      pending.requireWaiting();
      // The untyped event observes the same old current context while the constructor candidate
      // is exposed. It must not promote that candidate or reuse the old completed policy.
      pending.fixture.listener.contextChanged();
      assertInstanceOf(
          SecurityException.class,
          assertThrows(ExecutionException.class, () -> wait.get(5, TimeUnit.SECONDS)).getCause());
      assertThrows(SecurityException.class, pending.fixture.engine::requireReady);
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @Test
  void interruptionPreservesFlagAndCauseWithoutPublication() throws Exception {
    try (Pending pending = new Pending(false)) {
      AtomicReference<Thread> worker = new AtomicReference<>();
      AtomicBoolean interrupted = new AtomicBoolean();
      Future<?> wait =
          pending.executor.submit(
              () -> {
                worker.set(Thread.currentThread());
                try {
                  pending.fixture.engine.awaitCreatePublication(pending.context(() -> {}));
                } finally {
                  interrupted.set(Thread.currentThread().isInterrupted());
                }
              });
      pending.requireWaiting();
      worker.get().interrupt();
      Throwable failure =
          assertThrows(ExecutionException.class, () -> wait.get(5, TimeUnit.SECONDS)).getCause();
      assertInstanceOf(SecurityException.class, failure);
      assertInstanceOf(InterruptedException.class, failure.getCause());
      assertTrue(interrupted.get());
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @Test
  void closedLeaseTerminatesReadinessWithoutPublication() throws Exception {
    try (Pending pending = new Pending(false)) {
      Future<?> wait = pending.await(() -> {});
      pending.requireWaiting();
      pending.fixture.engine.close();
      assertInstanceOf(
          IllegalStateException.class,
          assertThrows(ExecutionException.class, () -> wait.get(5, TimeUnit.SECONDS)).getCause());
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @Test
  void expiredOrClosedCreateContextIsRecheckedWhileWaiting() throws Exception {
    try (Pending pending = new Pending(false)) {
      AtomicBoolean live = new AtomicBoolean(true);
      Future<?> wait =
          pending.await(
              () -> {
                if (!live.get()) {
                  throw new SecurityException("CREATE participant expired");
                }
              });
      pending.requireWaiting();
      live.set(false);
      assertInstanceOf(
          SecurityException.class,
          assertThrows(ExecutionException.class, () -> wait.get(5, TimeUnit.SECONDS)).getCause());
      assertTrue(pending.fixture.requests.isEmpty());
    }
  }

  @Test
  void missingInitialPublicationDoesNotWaitForAFirstRefresh() throws Exception {
    try (TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture()) {
      DeltaRangerPolicyEngine.Backend backend =
          new DeltaRangerPolicyEngine.Backend() {
            /** {@inheritDoc} */
            @Override
            public RangerBasePlugin plugin() {
              return fixture.plugin;
            }

            /** {@inheritDoc} */
            @Override
            public void start() {}

            /** {@inheritDoc} */
            @Override
            public void close() {}
          };
      try (DeltaRangerPolicyEngine engine =
          new DeltaRangerPolicyEngine.Owner(() -> backend)
              .acquire(
                  new DeltaRangerConfig("service", "spark", "lake", fixture.plugin.getConfig()))) {
        AtomicInteger observations = new AtomicInteger();
        assertThrows(
            RuntimeException.class,
            () -> engine.awaitCreatePublication(observations::incrementAndGet));
        assertEquals(1, observations.get());
        assertTrue(fixture.requests.isEmpty());
      } finally {
        fixture.engine.close();
      }
    }
  }

  private static final class Pending implements AutoCloseable {
    private final TestDeltaRangerNativeSelect.Fixture fixture =
        new TestDeltaRangerNativeSelect.Fixture();
    private final AtomicReference<RangerAuthContext> candidate =
        new AtomicReference<>(
            new RangerAuthContext(Collections.emptyMap(), null, fixture.plugin.getRoles(), null));
    private final AtomicReference<RangerAuthContext> current = new AtomicReference<>();
    private final AtomicReference<RangerRoles> roles =
        new AtomicReference<>(fixture.plugin.getRoles());
    private final AtomicReference<Map<String, String>> binding =
        new AtomicReference<>(fixture.plugin.getServiceConfigs());
    private final AtomicReference<RangerServiceDef> definition =
        new AtomicReference<>(fixture.plugin.getServiceDef());
    private final CountDownLatch waiting = new CountDownLatch(1);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private Pending(boolean currentAssigned) throws Exception {
      current.set(currentAssigned ? candidate.get() : fixture.plugin.getCurrentRangerAuthContext());
      when(fixture.plugin.getRoles()).thenAnswer(call -> roles.get());
      when(fixture.plugin.getRangerRoles()).thenAnswer(call -> roles.get());
      when(fixture.plugin.getServiceConfigs()).thenAnswer(call -> binding.get());
      when(fixture.plugin.getServiceDef()).thenAnswer(call -> definition.get());
      when(fixture.context.getAuthContext()).thenAnswer(call -> candidate.get());
      when(fixture.plugin.getCurrentRangerAuthContext()).thenAnswer(call -> current.get());
    }

    private Runnable context(Runnable check) {
      AtomicInteger calls = new AtomicInteger();
      return () -> {
        check.run();
        if (calls.incrementAndGet() >= 2) {
          waiting.countDown();
        }
      };
    }

    private Future<?> await(Runnable check) {
      return executor.submit(() -> fixture.engine.awaitCreatePublication(context(check)));
    }

    private void requireWaiting() throws InterruptedException {
      assertTrue(waiting.await(5, TimeUnit.SECONDS), "Readiness never awaited completion");
    }

    private void complete() {
      current.set(candidate.get());
      fixture.listener.contextChanged();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws InterruptedException {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      fixture.engine.close();
    }
  }
}
