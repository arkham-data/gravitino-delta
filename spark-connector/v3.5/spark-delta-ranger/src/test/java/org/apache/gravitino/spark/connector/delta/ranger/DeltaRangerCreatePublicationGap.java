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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.ranger.plugin.service.RangerAuthContext;

/** Deterministic reservation-triggered publication gap at the real CREATE admission boundary. */
public final class DeltaRangerCreatePublicationGap implements AutoCloseable {
  private final TestDeltaRangerNativeSelect.Fixture policy;
  private final Runnable beforeCompletion;
  private final AtomicBoolean cancelled = new AtomicBoolean();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final CountDownLatch completed = new CountDownLatch(1);
  private Thread publisher;

  /** Captures the real engine fixture and an assertion that native effects have not started. */
  public DeltaRangerCreatePublicationGap(
      TestDeltaRangerNativeSelect.Fixture policy, Runnable beforeCompletion) {
    this.policy = policy;
    this.beforeCompletion = beforeCompletion;
  }

  /** Exposes the constructor context, then finishes publication only after admission is waiting. */
  public void begin() {
    Thread writer = Thread.currentThread();
    RangerAuthContext next =
        new RangerAuthContext(Collections.emptyMap(), null, policy.plugin.getRoles(), null);
    AtomicReference<RangerAuthContext> current =
        new AtomicReference<>(policy.plugin.getCurrentRangerAuthContext());
    CountDownLatch observed = new CountDownLatch(1);
    when(policy.plugin.getCurrentRangerAuthContext()).thenAnswer(call -> current.get());
    when(policy.context.getAuthContext())
        .thenAnswer(
            call -> {
              if (Thread.currentThread() == writer) {
                observed.countDown();
              }
              return next;
            });
    publisher =
        new Thread(
            () -> {
              try {
                assertTrue(observed.await(5, TimeUnit.SECONDS), "CREATE never observed the gap");
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!cancelled.get()) {
                  boolean waiting = writer.getState() == Thread.State.TIMED_WAITING;
                  boolean admission = false;
                  for (StackTraceElement frame : writer.getStackTrace()) {
                    admission |= frame.getMethodName().equals("awaitCreatePublication");
                  }
                  if (waiting && admission) {
                    beforeCompletion.run();
                    current.set(next);
                    policy.listener.contextChanged();
                    completed.countDown();
                    return;
                  }
                  assertTrue(System.nanoTime() < deadline, "CREATE did not await publication");
                  LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                }
              } catch (Throwable error) {
                failure.set(error);
              }
            },
            "create-publication-gap");
    publisher.setDaemon(true);
    publisher.start();
  }

  /** Requires genuine completed publication before native CREATE can return normally. */
  public void requireCompleted() {
    if (failure.get() != null) {
      throw new AssertionError("Publication coordinator failed", failure.get());
    }
    assertTrue(completed.getCount() == 0, "Native CREATE returned before completed publication");
  }

  /** Stops the test coordinator without completing or repairing a failed CREATE. */
  @Override
  public void close() throws InterruptedException {
    cancelled.set(true);
    if (publisher != null) {
      publisher.interrupt();
      publisher.join(5000);
      assertTrue(!publisher.isAlive(), "Publication coordinator did not terminate");
    }
  }
}
