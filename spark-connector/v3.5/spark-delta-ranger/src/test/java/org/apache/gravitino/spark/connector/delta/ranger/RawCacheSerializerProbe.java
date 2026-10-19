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

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.spark.package$;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.CachedData;
import org.apache.spark.sql.execution.columnar.CachedRDDBuilder;
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer;
import org.apache.spark.sql.execution.columnar.InMemoryRelation$;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf$;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** Test-only executable fixture; each process owns its complete ordinary singleton history. */
public final class RawCacheSerializerProbe {
  private static final AtomicInteger CONSTRUCTORS = new AtomicInteger();
  private static final AtomicInteger CALLBACKS = new AtomicInteger();
  private static final String REJECTION = "Raw Delta cache serializer admission rejected";
  private final Properties result = new Properties();
  private int assertions;

  private RawCacheSerializerProbe() {}

  /**
   * Runs one fresh-process scenario and writes structured results even if an assertion fails.
   *
   * @param args scenario and result file path
   * @throws Exception if the fixture or result writing fails
   */
  public static void main(String[] args) throws Exception {
    RawCacheSerializerProbe probe = new RawCacheSerializerProbe();
    Path destination = Paths.get(args[1]);
    probe.result.setProperty("scenario", args[0]);
    probe.result.setProperty("spark.version", package$.MODULE$.SPARK_VERSION());
    probe.result.setProperty(
        "module.origin",
        InMemoryRelation$.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toExternalForm());
    probe.result.setProperty(
        "serializer.origin",
        DefaultCachedBatchSerializer.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .toExternalForm());
    try {
      probe.run(args[0]);
      probe.result.setProperty("complete", "true");
    } finally {
      probe.result.setProperty("assertions", Integer.toString(probe.assertions));
      probe.result.setProperty("constructors", Integer.toString(CONSTRUCTORS.get()));
      probe.result.setProperty("callbacks", Integer.toString(CALLBACKS.get()));
      try (OutputStream output = Files.newOutputStream(destination)) {
        probe.result.store(output, "Fresh JVM raw serializer results");
      }
    }
  }

  private void run(String scenario) {
    switch (scenario) {
      case "absent":
        absent();
        break;
      case "default":
        defaultSerializer();
        break;
      case "foreign":
        foreign();
        break;
      case "configured-default-actual-foreign":
        mismatch(true);
        break;
      case "actual-default-configured-foreign":
        mismatch(false);
        break;
      case "session-and-candidate-binding":
        binding();
        break;
      case "callback-before":
        callbackBefore();
        break;
      default:
        throw new AssertionError("Unknown scenario: " + scenario);
    }
  }

  private void absent() {
    SparkSession session = start(true, null);
    try {
      denied("denied", () -> DeltaRangerRawCacheSerializer.observe(session));
      denied("nullSessionDenied", () -> DeltaRangerRawCacheSerializer.observe(null));
      check("noConstruction", CONSTRUCTORS.get() == 0);
      check("noCallback", CALLBACKS.get() == 0);
      check("noCache", session.sharedState().cacheManager().isEmpty());
    } finally {
      session.stop();
    }
  }

  // Spark builders are capabilities; case-class value equality cannot prove replacement.
  @SuppressWarnings("ReferenceEquality")
  private void defaultSerializer() {
    SparkSession session = start(false, null);
    try {
      Dataset<Long> frame = warm(session);
      CacheManager manager = session.sharedState().cacheManager();
      CachedData old = manager.lookupCachedData(frame).get();
      CachedRDDBuilder builder = old.cachedRepresentation().cacheBuilder();
      session.sparkContext().setJobGroup("observer-only", "serializer observation", false);
      DeltaRangerRawCacheSerializer.Observation observation =
          DeltaRangerRawCacheSerializer.observe(session);
      observation.requireCurrent(session);
      observation.requireCurrent(session);
      observation.requireSerializer(session, builder.serializer());
      check("observed", builder.serializer().getClass() == DefaultCachedBatchSerializer.class);
      denied(
          "candidateDenied",
          () -> observation.requireSerializer(session, new DefaultCachedBatchSerializer()));
      check(
          "noJobs",
          session.sparkContext().statusTracker().getJobIdsForGroup("observer-only").length == 0);
      session.sparkContext().clearJobGroup();
      manager.recacheByPlan(session, old.plan());
      CachedData rebuilt = manager.lookupCachedData(old.plan()).get();
      CachedRDDBuilder replacement = rebuilt.cachedRepresentation().cacheBuilder();
      check("recacheBuilderChanged", replacement != builder);
      check("recacheSameSerializer", replacement.serializer() == builder.serializer());
      observation.requireSerializer(session, replacement.serializer());
      manager.clearCache();
      observation.requireCurrent(session);
      check("clearRetainsSerializer", manager.isEmpty());
    } finally {
      session.stop();
    }
  }

  private void foreign() {
    SparkSession session = start(true, null);
    try {
      Dataset<Long> frame = warm(session);
      check("actualForeign", serializer(session, frame).getClass() == CountingSerializer.class);
      int constructors = CONSTRUCTORS.get();
      int callbacks = CALLBACKS.get();
      check("nativeCallbackReached", callbacks > 0 && constructors > 0);
      denied("denied", () -> DeltaRangerRawCacheSerializer.observe(session));
      check("noConstruction", CONSTRUCTORS.get() == constructors);
      check("noCallback", CALLBACKS.get() == callbacks);
    } finally {
      session.stop();
    }
  }

  private void mismatch(boolean firstForeign) {
    SparkSession first = start(firstForeign, null);
    Object retained;
    try {
      retained = serializer(first, warm(first));
    } finally {
      first.stop();
    }
    SparkSession second = start(!firstForeign, null);
    try {
      Object actual = serializer(second, warm(second));
      check("sameSerializerAcrossApps", actual == retained);
      check(
          firstForeign ? "actualForeign" : "actualDefault",
          actual.getClass()
              == (firstForeign ? CountingSerializer.class : DefaultCachedBatchSerializer.class));
      check(
          firstForeign ? "defaultConfig" : "foreignConfig",
          effectiveSerializer(second)
              .equals(
                  (firstForeign ? DefaultCachedBatchSerializer.class : CountingSerializer.class)
                      .getName()));
      int constructors = CONSTRUCTORS.get();
      int callbacks = CALLBACKS.get();
      denied("denied", () -> DeltaRangerRawCacheSerializer.observe(second));
      check("noConstruction", CONSTRUCTORS.get() == constructors);
      check("noCallback", CALLBACKS.get() == callbacks);
      if (!firstForeign) {
        check("foreignNeverConstructed", constructors == 0);
      }
    } finally {
      second.stop();
    }
  }

  private void binding() {
    SparkSession session = start(false, null);
    DeltaRangerRawCacheSerializer.Observation observation;
    try {
      Dataset<Long> frame = warm(session);
      CachedRDDBuilder builder = serializerBuilder(session, frame);
      observation = DeltaRangerRawCacheSerializer.observe(session);
      observation.requireSerializer(session, builder.serializer());
      check("exactCandidate", true);
      SparkSession other = session.newSession();
      denied("otherSessionDenied", () -> observation.requireCurrent(other));
      denied(
          "otherSessionCandidateDenied",
          () -> observation.requireSerializer(other, builder.serializer()));
      DeltaRangerRawCacheSerializer.observe(other).requireSerializer(other, builder.serializer());
      check(
          "freshSessionObservation",
          other != session && other.sharedState() == session.sharedState());
      denied(
          "candidateDenied",
          () -> observation.requireSerializer(session, new DefaultCachedBatchSerializer()));
      denied("nullCandidateDenied", () -> observation.requireSerializer(session, null));
      denied("nullSessionDenied", () -> observation.requireCurrent(null));
      SQLConf conf = session.sessionState().conf();
      conf.setConfString(
          StaticSQLConf$.MODULE$.SPARK_CACHE_SERIALIZER().key(),
          CountingSerializer.class.getName());
      denied("changedConfigDenied", () -> observation.requireCurrent(session));
      conf.setConfString(
          StaticSQLConf$.MODULE$.SPARK_CACHE_SERIALIZER().key(),
          DefaultCachedBatchSerializer.class.getName());
      observation.requireCurrent(session);
    } finally {
      session.stop();
    }
    denied("stoppedDenied", () -> observation.requireCurrent(session));
  }

  private void callbackBefore() {
    TestNormalizationHook hook = new TestNormalizationHook();
    SparkSession first = start(true, hook);
    Object retained;
    try {
      Dataset<Long> frame = warm(first);
      retained = serializer(first, frame);
      callbackDenied(first, frame, hook, "foreign");
    } finally {
      first.stop();
    }
    TestNormalizationHook secondHook = new TestNormalizationHook();
    SparkSession second = start(false, secondHook);
    try {
      Dataset<Long> frame = warm(second);
      check("defaultConfig.actualForeign", serializer(second, frame) == retained);
      check(
          "defaultConfig.configuredDefault",
          effectiveSerializer(second).equals(DefaultCachedBatchSerializer.class.getName()));
      callbackDenied(second, frame, secondHook, "defaultConfig");
    } finally {
      second.stop();
    }
  }

  private void callbackDenied(
      SparkSession session, Dataset<Long> frame, TestNormalizationHook hook, String prefix) {
    CacheManager manager = session.sharedState().cacheManager();
    CachedData old = manager.lookupCachedData(frame).get();
    LogicalPlan key = old.plan();
    check(
        prefix + ".oldCacheLoaded",
        old.cachedRepresentation().cacheBuilder().isCachedColumnBuffersLoaded());
    hook.key = key;
    hook.enabled = true;
    int callbacks = CALLBACKS.get();
    int constructors = CONSTRUCTORS.get();
    denied(prefix + ".recacheDenied", () -> manager.recacheByPlan(session, key));
    check(prefix + ".exactKey", hook.entries.get() > 0 && hook.exactEntries.get() > 0);
    check(prefix + ".noCallback", CALLBACKS.get() == callbacks);
    check(prefix + ".noConstruction", CONSTRUCTORS.get() == constructors);
    result.setProperty(
        prefix + ".cacheRemovedOnDenial",
        Boolean.toString(manager.lookupCachedData(key).isEmpty()));
    int entries = hook.entries.get();
    denied(
        prefix + ".standaloneDenied",
        () ->
            session
                .sessionState()
                .executePlan(key, session.sessionState().executePlan$default$2())
                .normalized());
    check(prefix + ".standaloneHookReached", hook.entries.get() > entries);
    check(prefix + ".standaloneNoCallback", CALLBACKS.get() == callbacks);
    result.setProperty(prefix + ".hookEntries", Integer.toString(hook.entries.get()));
    result.setProperty(prefix + ".callbackBaseline", Integer.toString(callbacks));
  }

  private void denied(String name, Runnable operation) {
    try {
      operation.run();
    } catch (SecurityException failure) {
      check(name, REJECTION.equals(failure.getMessage()) && failure.getCause() == null);
      return;
    }
    throw new AssertionError("Admission unexpectedly succeeded: " + name);
  }

  private void check(String name, boolean condition) {
    if (!condition) {
      throw new AssertionError("Failed assertion: " + name);
    }
    assertions++;
    result.setProperty("check." + name, "true");
  }

  private static SparkSession start(boolean foreign, @Nullable TestNormalizationHook hook) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[1]")
            .appName("raw-cache-serializer-probe")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config(
                StaticSQLConf$.MODULE$.SPARK_CACHE_SERIALIZER().key(),
                (foreign ? CountingSerializer.class : DefaultCachedBatchSerializer.class)
                    .getName());
    if (hook != null) {
      builder.withExtensions(
          new AbstractFunction1<SparkSessionExtensions, BoxedUnit>() {
            /**
             * Installs the fixture before SessionState initialization.
             *
             * @param extensions native session extensions
             * @return Scala unit
             */
            @Override
            public BoxedUnit apply(SparkSessionExtensions extensions) {
              hook.install(extensions);
              return BoxedUnit.UNIT;
            }
          });
    }
    return builder.getOrCreate();
  }

  private static String effectiveSerializer(SparkSession session) {
    return session.sessionState().conf().getConf(StaticSQLConf$.MODULE$.SPARK_CACHE_SERIALIZER());
  }

  private static Dataset<Long> warm(SparkSession session) {
    Dataset<Long> frame = session.range(0, 4, 1, 1).cache();
    if (frame.collectAsList().size() != 4) {
      throw new AssertionError("Native warm cache did not return four rows");
    }
    return frame;
  }

  private static Object serializer(SparkSession session, Dataset<Long> frame) {
    return serializerBuilder(session, frame).serializer();
  }

  private static CachedRDDBuilder serializerBuilder(SparkSession session, Dataset<Long> frame) {
    return session
        .sharedState()
        .cacheManager()
        .lookupCachedData(frame)
        .get()
        .cachedRepresentation()
        .cacheBuilder();
  }

  /** A normally delegating serializer that detects observer-caused construction or callbacks. */
  public static class CountingSerializer extends DefaultCachedBatchSerializer {
    /** Constructs the ordinary configured serializer and records native initialization. */
    public CountingSerializer() {
      CONSTRUCTORS.incrementAndGet();
    }

    /**
     * Records native callback reachability before delegating to the stock serializer.
     *
     * @param schema native input attributes
     * @return the stock serializer's input support
     */
    @Override
    public boolean supportsColumnarInput(Seq<Attribute> schema) {
      CALLBACKS.incrementAndGet();
      return super.supportsColumnarInput(schema);
    }
  }

  // Deliberately test-only reachability fixture. It conveys no product raw-cache authority.
  private static final class TestNormalizationHook {
    private final AtomicInteger entries = new AtomicInteger();
    private final AtomicInteger exactEntries = new AtomicInteger();
    private volatile boolean enabled;
    @Nullable private volatile LogicalPlan key;

    private void install(SparkSessionExtensions extensions) {
      extensions.injectPlanNormalizationRule(
          new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
            /**
             * Binds the test-only rule to Spark's actual normalization session.
             *
             * @param actualSession native normalization session, including recache clones
             * @return the bounded fixture rule
             */
            @Override
            public Rule<LogicalPlan> apply(SparkSession actualSession) {
              return new Rule<LogicalPlan>() {
                /**
                 * Calls the production observer before native cache reconstruction can call S.
                 *
                 * @param plan the bounded native key being normalized
                 * @return the unchanged key when the fixture is disabled or admission succeeds
                 */
                @Override
                public LogicalPlan apply(LogicalPlan plan) {
                  if (enabled && key != null && plan.sameResult(key)) {
                    entries.incrementAndGet();
                    if (plan == key) {
                      exactEntries.incrementAndGet();
                    }
                    DeltaRangerRawCacheSerializer.observe(actualSession);
                  }
                  return plan;
                }
              };
            }
          });
    }
  }
}
