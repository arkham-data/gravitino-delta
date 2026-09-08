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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestDeltaStorageContext {
  @TempDir Path temp;

  @Test
  void preservesUnrelatedHarCachePolicy() {
    Configuration original = configuration("policy");
    original.setBoolean("fs.har.impl.disable.cache", true);
    try (DeltaStorageContext context = new DeltaStorageContext(original)) {
      assertTrue(
          context.<Boolean>read(
              "READ",
              "file:///",
              (engine, conf) -> conf.getBoolean("fs.har.impl.disable.cache", false)));
      assertFalse(
          context.<Boolean>read(
              "READ",
              "file:///",
              (engine, conf) -> conf.getBoolean("fs.s3a.impl.disable.cache", true)));
    }
  }

  @Test
  void rejectsUnadmittedLocationSchemesBeforeStorageIo() {
    try (DeltaMetadataReader reader = new DeltaMetadataReader(configuration("admission"))) {
      for (String location :
          List.of("har:///archive.har/table", "hdfs://untrusted/table", "relative/table")) {
        assertEquals(
            "UNSUPPORTED_LOCATION",
            assertThrows(DeltaMetadataException.class, () -> reader.latest(location)).reason());
      }
    }
  }

  @Test
  void enablesOwnedCachesWithoutMutatingCallerConfiguration() {
    Configuration configuration = configuration("enabled");
    configuration.setBoolean("fs.file.impl.disable.cache", true);
    try (DeltaStorageContext context = new DeltaStorageContext(configuration)) {
      String first =
          context.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString());
      assertEquals(
          first,
          context.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString()));
      assertTrue(configuration.getBoolean("fs.file.impl.disable.cache", false));
    }
  }

  @Test
  void isolatesCachedFilesystemsCopiesConfigurationAndClosesOnlyOwnedSubject() throws Exception {
    Configuration a = configuration("A"), b = configuration("B");
    try (DeltaStorageContext first = new DeltaStorageContext(a);
        DeltaStorageContext second = new DeltaStorageContext(b)) {
      a.set("metadata.fixture.tag", "changed");
      String idA =
          first.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString());
      String idB =
          second.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString());
      assertNotEquals(idA, idB);
      assertTrue(idA.startsWith("A:"));
      assertTrue(idB.startsWith("B:"));
      assertEquals(
          idA,
          first.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString()));
      int closed = TrackingFileSystem.CLOSES.get();
      first.close();
      first.close();
      assertEquals(closed + 1, TrackingFileSystem.CLOSES.get());
      assertEquals(
          idB,
          second.read(
              "READ",
              "file:///",
              (engine, conf) -> FileSystem.get(URI.create("file:///"), conf).toString()));
      assertEquals(
          "READER_CLOSED",
          assertThrows(
                  DeltaMetadataException.class,
                  () -> first.read("READ", "file:///", (engine, conf) -> "bad"))
              .reason());
    }
  }

  @Test
  void closeDrainsAdmittedReadBeforeReleasingItsFilesystem() throws Exception {
    DeltaStorageContext context = new DeltaStorageContext(configuration("concurrent"));
    CountDownLatch entered = new CountDownLatch(1),
        release = new CountDownLatch(1),
        closing = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<String> read =
          executor.submit(
              () ->
                  context.read(
                      "READ",
                      "file:///",
                      (engine, conf) -> {
                        String fs = FileSystem.get(URI.create("file:///"), conf).toString();
                        entered.countDown();
                        if (!release.await(10, TimeUnit.SECONDS)) throw new IOException();
                        return fs;
                      }));
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      Future<?> close =
          executor.submit(
              () -> {
                closing.countDown();
                context.close();
              });
      assertTrue(closing.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> close.get(100, TimeUnit.MILLISECONDS));
      release.countDown();
      assertTrue(read.get(10, TimeUnit.SECONDS).startsWith("concurrent:"));
      close.get(10, TimeUnit.SECONDS);
      assertThrows(
          DeltaMetadataException.class,
          () -> context.read("READ", "file:///", (engine, conf) -> "bad"));
    } finally {
      release.countDown();
      executor.shutdownNow();
      context.close();
    }
  }

  @Test
  void restoresCallerSubjectAndClassloaderAfterSuccessFailureAndInterruption() throws Exception {
    UserGroupInformation caller =
        UserGroupInformation.createRemoteUser("gravitino-authorized-principal");
    caller.doAs(
        (PrivilegedExceptionAction<Void>)
            () -> {
              ClassLoader original = Thread.currentThread().getContextClassLoader();
              try (DeltaStorageContext context =
                  new DeltaStorageContext(configuration("restore"))) {
                assertNotEquals(
                    caller.getUserName(),
                    context.read(
                        "READ",
                        "file:///",
                        (engine, conf) -> UserGroupInformation.getCurrentUser().getUserName()));
                DeltaMetadataException failure =
                    assertThrows(
                        DeltaMetadataException.class,
                        () ->
                            context.read(
                                "READ",
                                "file:///",
                                (engine, conf) -> {
                                  Thread.currentThread()
                                      .setContextClassLoader(new ClassLoader(original) {});
                                  throw new IOException("credential-secret");
                                }));
                assertNull(failure.getCause());
                assertFalse(failure.toString().contains("credential-secret"));
                assertEquals(caller, UserGroupInformation.getCurrentUser());
                assertSame(original, Thread.currentThread().getContextClassLoader());
                assertThrows(
                    DeltaMetadataException.class,
                    () ->
                        context.read(
                            "READ",
                            "file:///",
                            (engine, conf) -> {
                              throw new InterruptedException("secret");
                            }));
                assertTrue(Thread.interrupted());
                assertEquals(caller, UserGroupInformation.getCurrentUser());
                assertSame(original, Thread.currentThread().getContextClassLoader());
              }
              return null;
            });
  }

  @Test
  void nativeLazyCheckpointIoRunsInsideOwnedScope() throws Exception {
    Path table = temp.resolve("checkpoint");
    MetadataFixtures.nativeTable(table, Map.of(), 2);
    MetadataFixtures.nativeCheckpoint(table, 2);
    MetadataFixtures.pruneBefore(table, 2);
    int before = TrackingFileSystem.CHECKPOINT_OPENS.get();
    try (DeltaMetadataReader reader = new DeltaMetadataReader(configuration("checkpoint"))) {
      assertEquals(2, reader.latest(MetadataFixtures.location(table)).version());
    }
    assertTrue(TrackingFileSystem.CHECKPOINT_OPENS.get() > before);
  }

  private static Configuration configuration(String tag) {
    Configuration c = MetadataFixtures.configuration();
    c.setClass("fs.file.impl", TrackingFileSystem.class, FileSystem.class);
    c.set("metadata.fixture.tag", tag);
    return c;
  }

  /**
   * Local fixture filesystem that records cache ownership without changing global configuration.
   */
  public static class TrackingFileSystem extends RawLocalFileSystem {
    static final AtomicInteger CLOSES = new AtomicInteger();
    static final AtomicInteger CHECKPOINT_OPENS = new AtomicInteger();
    private UserGroupInformation owner;
    private String tag;
    private boolean closed;

    /** Creates a filesystem for Hadoop reflection. */
    public TrackingFileSystem() {}

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
      super.initialize(uri, conf);
      owner = UserGroupInformation.getCurrentUser();
      tag = conf.get("metadata.fixture.tag");
    }

    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path path, int size) throws IOException {
      assertEquals(owner, UserGroupInformation.getCurrentUser());
      assertFalse(closed);
      if (path.getName().contains("checkpoint")) CHECKPOINT_OPENS.incrementAndGet();
      return super.open(path, size);
    }

    @Override
    public void close() throws IOException {
      assertFalse(closed);
      closed = true;
      CLOSES.incrementAndGet();
      super.close();
    }

    @Override
    public String toString() {
      assertFalse(closed);
      return tag + ":" + System.identityHashCode(this);
    }
  }
}
