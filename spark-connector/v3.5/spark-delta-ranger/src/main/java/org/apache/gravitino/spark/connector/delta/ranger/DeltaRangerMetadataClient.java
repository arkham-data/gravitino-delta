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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;

/**
 * Lazy metadata read supplier with one owned client and a finite lifetime. Construct before the
 * no-arg catalog wrapper initializes; configure once with trusted metadata credentials. Mutation
 * actor transport remains owned by the existing runtime.
 */
public final class DeltaRangerMetadataClient implements Supplier<Catalog>, AutoCloseable {
  private static final ScheduledThreadPoolExecutor EXPIRATIONS = expirations();
  private static final ExecutorService CLEANUPS =
      Executors.newCachedThreadPool(
          task -> {
            Thread thread = new Thread(task, "delta-ranger-metadata-cleanup");
            thread.setDaemon(true);
            return thread;
          });
  private final LongSupplier nanoTime;
  private final ScheduledExecutorService scheduler;
  private final Object leaseIdentity = new Object();
  @Nullable private Supplier<GravitinoClient> clients;
  @Nullable private Function<GravitinoClient.ClientBuilder, GravitinoClient> clientFactory;
  @Nullable private GravitinoClient client;
  @Nullable private ScheduledFuture<?> expiration;
  @Nullable private String catalog;
  private long lifetimeMillis;
  private boolean closed;
  private boolean initializing;
  private boolean started;
  private long deadline;

  /** Creates an unconfigured supplier for use in the catalog wrapper's super constructor. */
  public DeltaRangerMetadataClient() {
    this(System::nanoTime, EXPIRATIONS);
  }

  DeltaRangerMetadataClient(LongSupplier nanoTime, ScheduledExecutorService scheduler) {
    this(nanoTime, scheduler, null);
  }

  DeltaRangerMetadataClient(
      LongSupplier nanoTime,
      ScheduledExecutorService scheduler,
      @Nullable Function<GravitinoClient.ClientBuilder, GravitinoClient> clientFactory) {
    this.nanoTime = nanoTime;
    this.scheduler = scheduler;
    this.clientFactory = clientFactory;
  }

  /**
   * Configures a separate metadata identity using existing public authentication builders. The
   * callback must explicitly select authentication (including custom/token providers), sourcing
   * credentials from a private deployment mechanism. No default actor identity is used.
   *
   * @param uri Gravitino endpoint
   * @param metalake canonical metalake
   * @param catalog canonical catalog
   * @param lifetime maximum duration from first read attempt, including construction, at most 24
   *     hours
   * @param authentication trusted metadata authentication builder callback
   */
  public synchronized void configure(
      String uri,
      String metalake,
      String catalog,
      Duration lifetime,
      Consumer<GravitinoClient.ClientBuilder> authentication) {
    DeltaRangerResourceKey.literal(metalake);
    DeltaRangerConfig.require(uri != null && !uri.trim().isEmpty(), "Metadata endpoint required");
    DeltaRangerConfig.require(authentication != null, "Separate metadata authentication required");
    Function<GravitinoClient.ClientBuilder, GravitinoClient> construction = clientFactory;
    configure(
        catalog,
        lifetime,
        () -> {
          MetadataBuilder builder = new MetadataBuilder(uri, metalake, construction);
          authentication.accept(builder);
          return builder.build();
        });
    clientFactory = null;
  }

  /**
   * Resolves metadata using the owned service client, creating it only on first use.
   *
   * @return catalog read handle using the separate metadata transport
   * @throws IllegalStateException if unconfigured, closed, or expired
   */
  @Override
  public Catalog get() {
    GravitinoClient active;
    Supplier<GravitinoClient> factory = null;
    String boundCatalog;
    synchronized (this) {
      requireUsable();
      if (!started) {
        started = true;
        deadline = nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(lifetimeMillis);
        expiration = scheduler.schedule(this::expire, lifetimeMillis, TimeUnit.MILLISECONDS);
      }
      if (client == null) {
        if (initializing) {
          throw new IllegalStateException("Metadata client construction is in progress");
        }
        initializing = true;
        factory = clients;
      }
      active = client;
      boundCatalog = catalog;
    }
    if (factory != null) {
      GravitinoClient created;
      try {
        created = Objects.requireNonNull(factory.get(), "Metadata client factory returned null");
      } catch (RuntimeException | Error failure) {
        close();
        throw failure;
      }
      synchronized (this) {
        initializing = false;
        if (closed || expired()) {
          cleanup(created);
          requireUsable();
        }
        client = created;
        active = created;
      }
    }
    synchronized (this) {
      requireUsable();
    }
    Catalog result =
        Objects.requireNonNull(active.loadCatalog(boundCatalog), "Metadata catalog missing");
    synchronized (this) {
      requireUsable();
      if (client != active) {
        throw new IllegalStateException("Metadata lease closed during the read");
      }
      return result;
    }
  }

  <T> T read(Function<Catalog, T> operation) {
    Function<Catalog, T> activeOperation =
        Objects.requireNonNull(operation, "Metadata read operation required");
    Catalog metadata = get();
    MetadataLeaseStamp expected = leaseStamp();
    requireLive(expected);
    T result;
    try {
      result = activeOperation.apply(metadata);
    } catch (RuntimeException | Error failure) {
      requireLive(expected);
      throw failure;
    }
    requireLive(expected);
    return result;
  }

  synchronized MetadataLeaseStamp leaseStamp() {
    requireUsable();
    if (!started || client == null) {
      throw new IllegalStateException("Metadata lease has no owned client");
    }
    return new MetadataLeaseStamp(leaseIdentity);
  }

  synchronized void requireLive(@Nullable MetadataLeaseStamp expected) {
    requireUsable();
    if (!started || client == null || expected == null || expected.leaseIdentity != leaseIdentity) {
      throw new IllegalStateException("Metadata lease stamp is not live");
    }
  }

  /** Closes only this lease's owned client; repeated close is harmless. */
  @Override
  public void close() {
    GravitinoClient owned;
    synchronized (this) {
      owned = terminate();
    }
    if (owned != null) {
      owned.close();
    }
  }

  synchronized void configure(
      String catalog, Duration lifetime, Supplier<GravitinoClient> clients) {
    if (closed || this.clients != null) {
      throw new IllegalStateException("Metadata lease cannot be rebound");
    }
    this.catalog = DeltaRangerResourceKey.literal(catalog);
    DeltaRangerConfig.require(
        lifetime != null
            && !lifetime.isNegative()
            && lifetime.compareTo(Duration.ofMillis(1)) >= 0
            && lifetime.compareTo(Duration.ofHours(24)) <= 0,
        "Metadata lifetime must be between one millisecond and 24 hours");
    this.lifetimeMillis = lifetime.toMillis();
    this.clients = Objects.requireNonNull(clients, "Separate metadata client factory required");
  }

  private boolean expired() {
    return started && nanoTime.getAsLong() - deadline >= 0;
  }

  private void requireUsable() {
    if (expired()) {
      cleanup(terminate());
    }
    if (closed || clients == null) {
      throw new IllegalStateException("Metadata lease is unconfigured, closed, or expired");
    }
  }

  @Nullable
  private GravitinoClient terminate() {
    closed = true;
    clients = null;
    clientFactory = null;
    if (expiration != null) {
      expiration.cancel(false);
      expiration = null;
    }
    GravitinoClient owned = client;
    client = null;
    return owned;
  }

  private void expire() {
    synchronized (this) {
      cleanup(terminate());
    }
  }

  private static void cleanup(@Nullable GravitinoClient owned) {
    if (owned != null) {
      CLEANUPS.execute(owned::close);
    }
  }

  private static ScheduledThreadPoolExecutor expirations() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            task -> {
              Thread thread = new Thread(task, "delta-ranger-metadata-expiration");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  static final class MetadataLeaseStamp {
    private final Object leaseIdentity;

    private MetadataLeaseStamp(Object leaseIdentity) {
      this.leaseIdentity = leaseIdentity;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return "MetadataLeaseStamp";
    }
  }

  private static final class MetadataBuilder extends GravitinoClient.ClientBuilder {
    private final String expectedMetalake;
    @Nullable private final Function<GravitinoClient.ClientBuilder, GravitinoClient> construction;

    private MetadataBuilder(
        String uri,
        String metalake,
        @Nullable Function<GravitinoClient.ClientBuilder, GravitinoClient> construction) {
      super(uri);
      expectedMetalake = metalake;
      this.construction = construction;
      withMetalake(metalake);
    }

    /** {@inheritDoc} */
    @Override
    public GravitinoClient build() {
      DeltaRangerConfig.require(
          authDataProvider != null,
          "Metadata authentication callback must select an explicit provider");
      DeltaRangerConfig.require(
          expectedMetalake.equals(metalakeName),
          "Metadata authentication callback changed the metalake binding");
      // The package-local construction seam runs only below the real authentication checks.
      return construction == null ? super.build() : construction.apply(this);
    }
  }
}
