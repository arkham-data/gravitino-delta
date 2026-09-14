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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Catalog;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class TestDeltaRangerCatalogSpark35 {
  @TempDir Path directory;

  @Test
  void visibleCatalogInitializesWithoutMetadataReadAndClosesOnlyOwnedLease() throws Exception {
    SparkSession session = Mockito.mock(SparkSession.class);
    SparkContext context = Mockito.mock(SparkContext.class);
    RuntimeConfig runtime = Mockito.mock(RuntimeConfig.class);
    Catalog visible = Mockito.mock(Catalog.class);
    Mockito.when(visible.name()).thenReturn("a");
    Mockito.when(visible.provider()).thenReturn("lakehouse-delta");
    Path config = directory.resolve("startup.properties");
    Files.write(
        config,
        "auth-type=simple\nprincipal=separate-metadata\nlifetime-seconds=30\n"
            .getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-------"));
    SparkConf conf =
        new SparkConf(false)
            .set("spark.gravitino.enableDeltaSupport", "true")
            .set("spark.gravitino.enableDeltaRangerSupport", "true")
            .set("spark.sql.gravitino.uri", "http://127.0.0.1:1")
            .set("spark.sql.gravitino.metalake", "lake")
            .set("spark.gravitino.deltaRanger.metadata.config", config.toString());
    Mockito.when(context.getConf()).thenReturn(conf);
    Mockito.when(context.sparkUser()).thenReturn("application-reader");
    Mockito.when(session.sparkContext()).thenReturn(context);
    Mockito.when(session.conf()).thenReturn(runtime);
    Mockito.when(runtime.get(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
        .thenAnswer(call -> call.getArgument(1));
    Mockito.when(runtime.get("spark.sql.extensions", ""))
        .thenReturn(
            DeltaRangerCatalogSpark35.EXTENSION + ",io.delta.sql.DeltaSparkSessionExtension");
    Mockito.when(runtime.get("spark.sql.catalog.spark_catalog", ""))
        .thenReturn("org.apache.spark.sql.delta.catalog.DeltaCatalog");
    SparkSession.setActiveSession(session);
    TestDeltaRangerNativeSelect.Fixture fixture = new TestDeltaRangerNativeSelect.Fixture();
    DeltaRangerMetadataClient metadata = new DeltaRangerMetadataClient();
    try {
      DeltaRangerCatalogSpark35.State state =
          new DeltaRangerCatalogSpark35.State(
              metadata,
              () -> {
                throw new AssertionError("Initialization must not load the mutation actor");
              },
              () -> visible,
              ignored -> fixture.engine);
      DeltaRangerCatalogSpark35 catalog = new DeltaRangerCatalogSpark35(state);
      catalog.initialize(
          "a",
          new CaseInsensitiveStringMap(
              Collections.singletonMap(
                  "journal-directory", directory.resolve("journal").toString())));
      assertEquals(fixture.engine, catalog.policyEngine());
      ArgumentCaptor<SparkListener> listener = ArgumentCaptor.forClass(SparkListener.class);
      Mockito.verify(context).addSparkListener(listener.capture());
      listener.getValue().onApplicationEnd(new SparkListenerApplicationEnd(0L));
      catalog.close();
      Mockito.verify(context).removeSparkListener(listener.getValue());
      assertThrows(IllegalStateException.class, metadata::get);
      assertThrows(IllegalStateException.class, fixture.engine::versions);
      DeltaRangerCatalogSpark35 hidden =
          new DeltaRangerCatalogSpark35(
              new DeltaRangerCatalogSpark35.State(
                  new DeltaRangerMetadataClient(),
                  () -> visible,
                  () -> null,
                  ignored -> {
                    throw new AssertionError("Hidden catalog must not initialize Ranger");
                  }));
      assertThrows(
          IllegalArgumentException.class,
          () -> hidden.initialize("hidden", new CaseInsensitiveStringMap(Collections.emptyMap())));
    } finally {
      fixture.engine.close();
      SparkSession.clearActiveSession();
    }
  }

  @Test
  void currentActorIsFreshAndMetadataLeaseClosesAfterFailedInitialization() throws Exception {
    SparkSession session = Mockito.mock(SparkSession.class);
    SparkContext context = Mockito.mock(SparkContext.class);
    SparkSession.setActiveSession(session);
    Mockito.when(session.sparkContext()).thenReturn(context);
    Mockito.when(context.getConf()).thenReturn(new SparkConf(false));
    DeltaRangerMetadataClient metadata = new DeltaRangerMetadataClient();
    Catalog first = Mockito.mock(Catalog.class);
    Catalog second = Mockito.mock(Catalog.class);
    AtomicReference<Catalog> actor = new AtomicReference<>(first);
    DeltaRangerCatalogSpark35.State state =
        new DeltaRangerCatalogSpark35.State(
            metadata,
            actor::get,
            actor::get,
            conf -> {
              throw new AssertionError("No engine before valid configuration");
            });
    state.catalog = "a";
    assertEquals(first, state.actor());
    actor.set(second);
    assertEquals(second, state.actor());
    try {
      DeltaRangerCatalogSpark35 catalog = new DeltaRangerCatalogSpark35(state);
      assertThrows(
          IllegalArgumentException.class,
          () -> catalog.initialize("a", new CaseInsensitiveStringMap(Collections.emptyMap())));
      assertThrows(
          IllegalStateException.class,
          () ->
              metadata.configure(
                  "http://localhost:1",
                  "lake",
                  "a",
                  Duration.ofSeconds(30),
                  builder -> builder.withSimpleAuth("reader")));
      assertThrows(IllegalStateException.class, catalog::policyEngine);
    } finally {
      SparkSession.clearActiveSession();
    }
  }

  @Test
  void metadataIdentityMustBeExplicitPrivateAndBounded() throws Exception {
    Path config = directory.resolve("metadata.properties");
    Files.write(
        config,
        "auth-type=simple\nprincipal=metadata-reader\nlifetime-seconds=300\n"
            .getBytes(StandardCharsets.UTF_8));
    Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-------"));
    DeltaRangerCatalogSpark35.MetadataIdentity identity =
        DeltaRangerCatalogSpark35.MetadataIdentity.read(config);
    assertEquals("metadata-reader", identity.principal);
    assertEquals(300, identity.lifetime.getSeconds());
    Path symlink = Files.createSymbolicLink(directory.resolve("metadata-link"), config);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerCatalogSpark35.MetadataIdentity.read(symlink));
    for (String invalid :
        new String[] {
          "auth-type=basic\nprincipal=metadata-reader\nlifetime-seconds=300\n",
          "auth-type=simple\nlifetime-seconds=300\n",
          "auth-type=simple\nprincipal=metadata-reader\nlifetime-seconds=0\n",
          "auth-type=simple\nprincipal=metadata-reader\nlifetime-seconds=86401\n",
          "auth-type=simple\nprincipal=metadata-reader\nlifetime-seconds=300\nuri=http://wrong\n"
        }) {
      Files.write(config, invalid.getBytes(StandardCharsets.UTF_8));
      assertThrows(
          IllegalArgumentException.class,
          () -> DeltaRangerCatalogSpark35.MetadataIdentity.read(config));
    }
    Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-r--r--"));
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaRangerCatalogSpark35.MetadataIdentity.read(config));
  }

  @Test
  void noArgConstructionIsLazyAndClosedCatalogCannotInitialize() {
    SparkSession.setActiveSession(Mockito.mock(SparkSession.class));
    DeltaRangerCatalogSpark35 catalog = new DeltaRangerCatalogSpark35();
    catalog.close();
    assertThrows(
        IllegalStateException.class,
        () -> catalog.initialize("a", new CaseInsensitiveStringMap(Collections.emptyMap())));
    SparkSession.clearActiveSession();
  }
}
