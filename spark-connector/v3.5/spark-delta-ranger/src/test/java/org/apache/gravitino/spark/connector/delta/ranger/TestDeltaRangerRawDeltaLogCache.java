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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class TestDeltaRangerRawDeltaLogCache {
  @Test
  void nativeCacheMapRetainsTheExistingNonLoadingMap() throws Exception {
    Cache<Object, Object> cache = CacheBuilder.newBuilder().build();
    Object key = new Object();
    Object value = new Object();
    cache.put(key, value);
    Object map = nativeMap(cache);
    assertSame(cache.asMap(), map);
    assertSame(value, cache.asMap().get(key));
    assertSame(map, nativeMap(cache));
    assertEquals(1, cache.size());
  }

  @Test
  void foreignCacheImplementationRejectsBeforeAnyCandidateMethod() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    Object candidate =
        Proxy.newProxyInstance(
            Cache.class.getClassLoader(),
            new Class<?>[] {Cache.class},
            (proxy, method, arguments) -> {
              calls.incrementAndGet();
              throw new AssertionError();
            });
    assertDenied(() -> nativeMap(candidate));
    assertEquals(0, calls.get());
    assertDenied(() -> nativeMap(null));
  }

  @Test
  void sameNamedForeignLoaderCacheRejects() throws Exception {
    URL jar = CacheBuilder.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader loader =
        new URLClassLoader(new URL[] {jar}, ClassLoader.getPlatformClassLoader())) {
      Class<?> builder = Class.forName(CacheBuilder.class.getName(), true, loader);
      Object cache =
          builder.getMethod("build").invoke(builder.getMethod("newBuilder").invoke(null));
      assertEquals(
          CacheBuilder.newBuilder().build().getClass().getName(), cache.getClass().getName());
      assertDenied(() -> nativeMap(cache));
      Method qualifier =
          DeltaRangerRawDeltaLogCache.class.getDeclaredMethod(
              "requireNativeType", Class.class, Class.class);
      qualifier.setAccessible(true);
      assertDenied(() -> qualifier.invoke(null, builder, CacheBuilder.class));
    }
  }

  @Test
  void nativeTypesRequireMatchingNonNullOriginAndMapImplementation() throws Exception {
    Method qualifier =
        DeltaRangerRawDeltaLogCache.class.getDeclaredMethod(
            "requireNativeType", Class.class, Class.class);
    qualifier.setAccessible(true);
    qualifier.invoke(null, Cache.class, CacheBuilder.class);
    assertSame(DeltaLog.class.getClassLoader(), CacheBuilder.class.getClassLoader());
    assertDenied(() -> qualifier.invoke(null, Cache.class, DeltaLog.class));
    assertDenied(() -> qualifier.invoke(null, String.class, String.class));
    Object map = CacheBuilder.newBuilder().build().asMap();
    DeltaRangerRawDeltaLogCache.requireRuntime(map, map.getClass(), CacheBuilder.class);
    assertThrows(
        SecurityException.class,
        () ->
            DeltaRangerRawDeltaLogCache.requireRuntime(
                new ConcurrentHashMap<>(), map.getClass(), CacheBuilder.class));
  }

  @Test
  void existingModuleNoneNeverCreatesCacheOrLog() throws Exception {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
    for (String flag : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (flag.startsWith("--add-opens=") || flag.startsWith("--add-exports=")) command.add(flag);
    }
    command.add("-Xmx768m");
    command.add("-cp");
    Set<String> entries = new LinkedHashSet<>();
    for (ClassLoader loader = getClass().getClassLoader();
        loader != null;
        loader = loader.getParent()) {
      if (loader instanceof URLClassLoader)
        for (URL url : ((URLClassLoader) loader).getURLs())
          entries.add(Paths.get(url.toURI()).toString());
    }
    entries.addAll(Arrays.asList(System.getProperty("java.class.path").split(File.pathSeparator)));
    command.add(String.join(File.pathSeparator, entries));
    command.add(RawDeltaLogCacheAbsentProbe.class.getName());
    String evidence = System.getProperty("raw.observation.evidence", "");
    Path directory =
        evidence.isEmpty()
            ? Files.createTempDirectory("raw-native-absent")
            : Files.createDirectories(Paths.get(evidence, "children", "absent"));
    Path log = directory.resolve("child.log");
    command.add(directory.resolve("result.txt").toString());
    Process child =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    try {
      assertTrue(child.waitFor(60, TimeUnit.SECONDS), "Fresh JVM did not complete");
      assertEquals(0, child.exitValue(), "Fresh JVM failed; retained log: " + log);
    } finally {
      if (child.isAlive()) child.destroyForcibly();
    }
  }

  @Test
  void absentSessionRejectsWithoutLeakingUnderlyingFailure() {
    SecurityException failure =
        assertThrows(
            SecurityException.class, () -> DeltaRangerRawDeltaLogCache.observe(null, null));
    assertEquals("Raw native cache observation rejected", failure.getMessage());
    assertNull(failure.getCause());
  }

  private static Object nativeMap(Object cache) throws Exception {
    Method method =
        DeltaRangerRawDeltaLogCache.class.getDeclaredMethod("nativeCacheMap", Object.class);
    method.setAccessible(true);
    return method.invoke(null, cache);
  }

  private static void assertDenied(Executable operation) {
    InvocationTargetException failure = assertThrows(InvocationTargetException.class, operation);
    assertEquals(SecurityException.class, failure.getCause().getClass());
    assertEquals("Raw native cache observation rejected", failure.getCause().getMessage());
    assertNull(failure.getCause().getCause());
  }
}
