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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.CodeSource;
import javax.annotation.Nullable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.columnar.CachedBatchSerializer;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer;
import org.apache.spark.sql.execution.columnar.InMemoryRelation$;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SharedState;
import org.apache.spark.sql.internal.StaticSQLConf$;
import scala.Option;
import scala.Some;

/**
 * Observes the existing ordinary Spark cache serializer without initializing it or invoking
 * callbacks.
 *
 * <p>In the qualified stock runtime, ordinary caching initializes the singleton once; native
 * recache copies its exact serializer, and clearCache/output copies preserve it. Supported history
 * excludes clearSerializer, the arbitrary-serializer testing factory, privately
 * manufactured/inserted builders, private singleton/cache writes, alternate module injection and
 * class redefinition. A reset to a default singleton while old foreign builders survive cannot be
 * detected by this observer.
 *
 * <p>The module monitor observes ordinary initialization safely; it does not lock cache membership.
 * None fails immediately. If native initialization already holds the monitor, observation waits for
 * it to finish, then evaluates its result. There is no retry, factory call or append replay.
 * Runtime origin checks assume an already qualified/pinned Spark deployment; they are not artifact
 * verification.
 */
final class DeltaRangerRawCacheSerializer {
  private DeltaRangerRawCacheSerializer() {}

  /**
   * Observes an existing stock singleton for this exact session and its effective serializer
   * setting.
   *
   * <p>Only the serializer-specific setting is retained; raw owners must separately validate
   * complete SQL configuration, metadata and policy publication. No Spark state is changed.
   */
  static Observation observe(@Nullable SparkSession session) {
    try {
      if (session == null) {
        throw denied();
      }
      SparkContext context = session.sparkContext();
      if (context == null || context.isStopped()) {
        throw denied();
      }
      SharedState sharedState = session.sharedState();
      if (sharedState == null) {
        throw denied();
      }
      CacheManager cacheManager = sharedState.cacheManager();
      Class<?> cacheManagerClass = CacheManager.class;
      Class<?> moduleClass = InMemoryRelation$.class;
      Class<?> serializerClass = DefaultCachedBatchSerializer.class;
      ClassLoader loader = cacheManagerClass.getClassLoader();
      CodeSource origin = cacheManagerClass.getProtectionDomain().getCodeSource();
      if (cacheManager == null || loader == null || origin == null) {
        throw denied();
      }
      requireRuntime(cacheManager.getClass(), cacheManagerClass, loader, origin);
      requireRuntime(moduleClass, InMemoryRelation$.class, loader, origin);
      requireRuntime(serializerClass, DefaultCachedBatchSerializer.class, loader, origin);
      InMemoryRelation$ module = InMemoryRelation$.MODULE$;
      if (module == null) {
        throw denied();
      }
      requireRuntime(module.getClass(), moduleClass, loader, origin);
      Field field = InMemoryRelation$.class.getDeclaredField("ser");
      if (field.getDeclaringClass() != moduleClass
          || !Modifier.isPrivate(field.getModifiers())
          || Modifier.isStatic(field.getModifiers())
          || field.getType() != Option.class) {
        throw denied();
      }
      field.setAccessible(true);
      CachedBatchSerializer serializer;
      synchronized (module) {
        Object option = field.get(module);
        if (option == null || option.getClass() != Some.class) {
          throw denied();
        }
        Object actual = ((Some<?>) option).get();
        if (actual == null) {
          throw denied();
        }
        requireRuntime(actual.getClass(), serializerClass, loader, origin);
        serializer = (CachedBatchSerializer) actual;
      }
      SQLConf conf = session.sessionState().conf();
      String configuration = conf.getConf(StaticSQLConf$.MODULE$.SPARK_CACHE_SERIALIZER());
      if (!DefaultCachedBatchSerializer.class.getName().equals(configuration)
          || context.isStopped()) {
        throw denied();
      }
      return new Observation(
          session,
          context,
          sharedState,
          cacheManager,
          module,
          serializer,
          cacheManagerClass,
          moduleClass,
          serializerClass,
          loader,
          origin,
          configuration);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError failure) {
      // Upstream failures may expose settings, paths or reflective implementation details.
      throw denied();
    }
  }

  private static void requireRuntime(
      Class<?> actual, Class<?> expected, ClassLoader loader, CodeSource origin) {
    if (actual != expected
        || actual.getClassLoader() != loader
        || !origin.equals(actual.getProtectionDomain().getCodeSource())) {
      throw denied();
    }
  }

  private static SecurityException denied() {
    return new SecurityException("Raw Delta cache serializer admission rejected");
  }

  /**
   * Immutable, driver-local association with one exact session, runtime and existing serializer.
   * Revalidation observes mutable Spark state again and conveys no installed-builder membership.
   */
  static final class Observation {
    private final SparkSession session;
    private final SparkContext context;
    private final SharedState sharedState;
    private final CacheManager cacheManager;
    private final InMemoryRelation$ module;
    private final CachedBatchSerializer serializer;
    private final Class<?> cacheManagerClass;
    private final Class<?> moduleClass;
    private final Class<?> serializerClass;
    private final ClassLoader loader;
    private final CodeSource origin;
    private final String configuration;

    private Observation(
        SparkSession session,
        SparkContext context,
        SharedState sharedState,
        CacheManager cacheManager,
        InMemoryRelation$ module,
        CachedBatchSerializer serializer,
        Class<?> cacheManagerClass,
        Class<?> moduleClass,
        Class<?> serializerClass,
        ClassLoader loader,
        CodeSource origin,
        String configuration) {
      this.session = session;
      this.context = context;
      this.sharedState = sharedState;
      this.cacheManager = cacheManager;
      this.module = module;
      this.serializer = serializer;
      this.cacheManagerClass = cacheManagerClass;
      this.moduleClass = moduleClass;
      this.serializerClass = serializerClass;
      this.loader = loader;
      this.origin = origin;
      this.configuration = configuration;
    }

    /**
     * Reobserves this same session's live ownership, singleton, runtime and serializer
     * configuration. A different session requires its own observation, including a compatible Spark
     * session clone.
     */
    void requireCurrent(@Nullable SparkSession actualSession) {
      if (actualSession != session) {
        throw denied();
      }
      Observation current = observe(actualSession);
      if (current.context != context
          || current.sharedState != sharedState
          || current.cacheManager != cacheManager
          || current.module != module
          || current.serializer != serializer
          || current.cacheManagerClass != cacheManagerClass
          || current.moduleClass != moduleClass
          || current.serializerClass != serializerClass
          || current.loader != loader
          || !origin.equals(current.origin)
          || !configuration.equals(current.configuration)) {
        throw denied();
      }
    }

    /**
     * Requires the current observation and the actual candidate's exact serializer reference. This
     * checks serializer association only; the caller must separately attest builder membership.
     */
    void requireSerializer(
        @Nullable SparkSession actualSession, @Nullable CachedBatchSerializer candidate) {
      requireCurrent(actualSession);
      if (candidate == null || candidate != serializer) {
        throw denied();
      }
    }

    /** Returns a constant description without settings, locations or object capabilities. */
    @Override
    public String toString() {
      return "Raw Delta cache serializer observation";
    }
  }
}
