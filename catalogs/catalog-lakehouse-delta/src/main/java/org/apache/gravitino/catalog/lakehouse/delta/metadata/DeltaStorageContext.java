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

package org.apache.gravitino.catalog.lakehouse.delta.metadata;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;

/** Owns one storage Subject and drains scoped reads before releasing its cached filesystems. */
final class DeltaStorageContext implements AutoCloseable {
  private final Configuration configuration;
  private final UserGroupInformation ownedUgi;
  private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock(true);
  private boolean closed;

  DeltaStorageContext(Configuration trustedConfiguration) {
    configuration =
        new Configuration(Objects.requireNonNull(trustedConfiguration, "configuration"));
    // Only admitted schemes have cache ownership semantics characterized by this reader.
    // Leave unrelated defaults (notably HAR's intentionally disabled cache) unchanged.
    configuration.setBoolean("fs.file.impl.disable.cache", false);
    configuration.setBoolean("fs.s3a.impl.disable.cache", false);
    ownedUgi = UserGroupInformation.createRemoteUser("gravitino-delta-" + UUID.randomUUID());
  }

  @FunctionalInterface
  interface Read<T> {
    /**
     * Materializes a detached result and closes every lazy resource within storage scope.
     *
     * @param engine per-read scoped engine
     * @param configuration per-read private settings
     * @return eagerly materialized result, never a resource or native handle
     * @throws Exception if storage cannot be read
     */
    T apply(Engine engine, Configuration configuration) throws Exception;
  }

  <T> T read(String operation, String normalizedLocation, Read<T> read) {
    try {
      lifecycle.readLock().lockInterruptibly();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DeltaMetadataException("READ_INTERRUPTED", operation);
    }
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      if (closed) {
        throw new DeltaMetadataException("READER_CLOSED", operation);
      }
      requireSupportedLocation(normalizedLocation);
      Thread.currentThread().setContextClassLoader(DeltaStorageContext.class.getClassLoader());
      return ownedUgi.doAs(
          (PrivilegedExceptionAction<T>)
              () -> {
                DeltaKernel33MetadataAdapter.validateRuntime();
                Configuration readConfiguration = new Configuration(configuration);
                return read.apply(DefaultEngine.create(readConfiguration), readConfiguration);
              });
    } catch (DeltaMetadataException e) {
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DeltaMetadataException("READ_INTERRUPTED", operation);
    } catch (IOException | RuntimeException e) {
      throw new DeltaMetadataException("STORAGE_UNAVAILABLE", operation);
    } catch (LinkageError e) {
      throw new DeltaMetadataException("UNSUPPORTED_RUNTIME", operation);
    } finally {
      Thread.currentThread().setContextClassLoader(original);
      lifecycle.readLock().unlock();
    }
  }

  /** Drains reads and closes only filesystems associated with this context's Subject. */
  @Override
  public void close() {
    lifecycle.writeLock().lock();
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try {
      if (closed) {
        return;
      }
      closed = true;
      Thread.currentThread().setContextClassLoader(DeltaStorageContext.class.getClassLoader());
      FileSystem.closeAllForUGI(ownedUgi);
    } catch (IOException | RuntimeException e) {
      throw new DeltaMetadataException("STORAGE_CLOSE_FAILED", "CLOSE");
    } finally {
      Thread.currentThread().setContextClassLoader(original);
      lifecycle.writeLock().unlock();
    }
  }

  private static void requireSupportedLocation(String location) {
    try {
      String scheme = URI.create(location).getScheme();
      if ("file".equals(scheme) || "s3a".equals(scheme)) {
        return;
      }
    } catch (RuntimeException e) {
      // Invalid URI details can contain credentials. Do not retain them.
    }
    throw new DeltaMetadataException("UNSUPPORTED_LOCATION", "VALIDATE_LOCATION");
  }
}
