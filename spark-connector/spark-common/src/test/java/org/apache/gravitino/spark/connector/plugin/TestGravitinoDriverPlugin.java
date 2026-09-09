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

package org.apache.gravitino.spark.connector.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions;
import org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin.DynamicBearerTokenProvider;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestGravitinoDriverPlugin {

  private static final String DELTA_CATALOG =
      "org.apache.gravitino.spark.connector.delta.GravitinoDeltaCatalogSpark35";

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }

  @Test
  void testAlwaysRegistersAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);

    new GravitinoDriverPlugin().registerSqlExtensions(sparkConf);

    assertEquals(
        GravitinoAuthorizationSparkSessionExtensions.class.getName(),
        sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testDoesNotDuplicateAuthorizationExtension() {
    SparkConf sparkConf = new SparkConf(false);
    String extension = GravitinoAuthorizationSparkSessionExtensions.class.getName();
    sparkConf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extension);

    new GravitinoDriverPlugin().registerSqlExtensions(sparkConf);

    assertEquals(extension, sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testDisabledDeltaSupportPreservesNativeSettingsAndExcludesDeltaCatalogs() {
    SparkConf sparkConf =
        new SparkConf(false)
            .set("spark.gravitino.enableDeltaSupport", "false")
            .set(
                StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
                GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS)
            .set(GravitinoDriverPlugin.SPARK_SESSION_CATALOG, "example.SessionCatalog");
    GravitinoDriverPlugin plugin =
        new GravitinoDriverPlugin(
            () -> {
              throw new AssertionError("Disabled Delta support must not resolve the adapter");
            },
            className -> {
              throw new AssertionError("Disabled Delta support must not load " + className);
            });

    plugin.configureDeltaSupport(sparkConf);
    plugin.registerSqlExtensions(sparkConf);

    assertFalse(plugin.isCatalogEnabled("lakehouse-delta"));
    assertEquals(
        GravitinoAuthorizationSparkSessionExtensions.class.getName()
            + ","
            + GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS,
        sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
    assertEquals(
        "example.SessionCatalog", sparkConf.get(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
  }

  @Test
  void testEnabledDeltaSupportFailsWhenOptionalRuntimeIsUnavailable() {
    SparkConf sparkConf = deltaEnabledConf();
    GravitinoDriverPlugin plugin =
        new GravitinoDriverPlugin(
            () -> DELTA_CATALOG,
            className -> {
              throw new ClassNotFoundException(className);
            });

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> plugin.configureDeltaSupport(sparkConf));

    assertTrue(exception.getMessage().contains(DELTA_CATALOG));
    assertFalse(sparkConf.contains(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
  }

  @Test
  void testEnabledDeltaSupportFailsWhenNativeExtensionIsUnavailable() {
    SparkConf sparkConf = deltaEnabledConf();
    GravitinoDriverPlugin plugin =
        new GravitinoDriverPlugin(
            () -> DELTA_CATALOG,
            className -> {
              if (GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS.equals(className)) {
                throw new ClassNotFoundException(className);
              }
            });

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> plugin.configureDeltaSupport(sparkConf));

    assertTrue(exception.getMessage().contains(GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS));
    assertFalse(sparkConf.contains(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
  }

  @Test
  void testEnabledDeltaSupportRegistersAndDeduplicatesNativeSettings() {
    SparkConf sparkConf = deltaEnabledConf();
    String authorizationExtension = GravitinoAuthorizationSparkSessionExtensions.class.getName();
    sparkConf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
        String.join(
            ",",
            Arrays.asList(
                "example.ExistingExtension",
                GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS,
                authorizationExtension)));
    List<String> validatedClasses = new ArrayList<>();
    GravitinoDriverPlugin plugin =
        new GravitinoDriverPlugin(() -> DELTA_CATALOG, validatedClasses::add);

    plugin.configureDeltaSupport(sparkConf);
    plugin.registerSqlExtensions(sparkConf);

    assertTrue(plugin.isCatalogEnabled("lakehouse-delta"));
    assertEquals(
        Arrays.asList(
            DELTA_CATALOG,
            GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS,
            GravitinoDriverPlugin.DELTA_SPARK_CATALOG),
        validatedClasses);
    assertEquals(
        GravitinoDriverPlugin.DELTA_SPARK_CATALOG,
        sparkConf.get(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
    assertEquals(
        String.join(
            ",",
            Arrays.asList(
                authorizationExtension,
                GravitinoDriverPlugin.DELTA_SPARK_EXTENSIONS,
                "example.ExistingExtension")),
        sparkConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key()));
  }

  @Test
  void testEnabledDeltaSupportPreservesMatchingNativeCatalog() {
    SparkConf sparkConf =
        deltaEnabledConf()
            .set(
                GravitinoDriverPlugin.SPARK_SESSION_CATALOG,
                GravitinoDriverPlugin.DELTA_SPARK_CATALOG);
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(() -> DELTA_CATALOG, className -> {});

    plugin.configureDeltaSupport(sparkConf);

    assertEquals(
        GravitinoDriverPlugin.DELTA_SPARK_CATALOG,
        sparkConf.get(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
  }

  @Test
  void testEnabledDeltaSupportRejectsConflictingNativeCatalog() {
    SparkConf sparkConf =
        deltaEnabledConf()
            .set(GravitinoDriverPlugin.SPARK_SESSION_CATALOG, "example.ConflictingCatalog");
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin(() -> DELTA_CATALOG, className -> {});

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> plugin.configureDeltaSupport(sparkConf));

    assertTrue(exception.getMessage().contains("example.ConflictingCatalog"));
    assertEquals(
        "example.ConflictingCatalog", sparkConf.get(GravitinoDriverPlugin.SPARK_SESSION_CATALOG));
  }

  @Test
  void testTokenAuthTypeBuildsClient() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    // The client cannot reach a server here, but it must get past auth configuration first: an
    // unsupported auth type or a missing token would fail before any connection is attempted.
    Exception e =
        Assertions.assertThrows(
            Exception.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertFalse(e instanceof UnsupportedOperationException, e.toString());
    Assertions.assertFalse(e instanceof IllegalArgumentException, e.toString());
  }

  @Test
  void testTokenProviderReturnsBearerToken() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "a-token");

    assertEquals(
        "Bearer a-token",
        new String(
            new DynamicBearerTokenProvider(sparkConf).getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenIsResolvedOnEveryRequest() {
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "first-token");
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer first-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "second-token");

    assertEquals(
        "Bearer second-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenFileTakesPrecedenceAndIsRereadEveryRequest(@TempDir Path tempDir)
      throws IOException {
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, "file-token\n".getBytes(StandardCharsets.UTF_8));
    SparkConf sparkConf = tokenAuthConf();
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE, "conf-token");
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE, tokenFile.toString());
    DynamicBearerTokenProvider provider = new DynamicBearerTokenProvider(sparkConf);

    assertEquals("Bearer file-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));

    Files.write(tokenFile, "rotated-token\n".getBytes(StandardCharsets.UTF_8));

    assertEquals(
        "Bearer rotated-token", new String(provider.getTokenData(), StandardCharsets.UTF_8));
  }

  @Test
  void testTokenAuthTypeWithoutTokenFails() {
    SparkConf sparkConf = tokenAuthConf();

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                GravitinoDriverPlugin.createGravitinoClient(
                    "http://127.0.0.1:1", "metalake", sparkConf, "user", ImmutableMap.of()));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE));
    Assertions.assertTrue(e.getMessage().contains(GravitinoSparkConfig.GRAVITINO_TOKEN_FILE));
  }

  private static SparkConf tokenAuthConf() {
    SparkConf sparkConf = new SparkConf(false);
    sparkConf.set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.TOKEN_AUTH_TYPE);
    return sparkConf;
  }

  private static SparkConf deltaEnabledConf() {
    return new SparkConf(false).set("spark.gravitino.enableDeltaSupport", "true");
  }
}
