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

import static org.apache.gravitino.spark.connector.ConnectorConstants.COMMA;
import static org.apache.gravitino.spark.connector.GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX;
import static org.apache.gravitino.spark.connector.utils.ConnectorUtil.removeDuplicateSparkExtensions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.CustomTokenProvider;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.client.GravitinoClientConfiguration;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.iceberg.extensions.GravitinoIcebergSparkSessionExtensions;
import org.apache.gravitino.spark.connector.version.CatalogNameAdaptor;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * GravitinoDriverPlugin creates GravitinoCatalogManager to fetch catalogs from Apache Gravitino and
 * register Gravitino catalogs to Apache Spark.
 */
public class GravitinoDriverPlugin implements DriverPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoDriverPlugin.class);
  private static final String DELTA_PROVIDER = "lakehouse-delta";
  private static final String DELTA_RANGER_CATALOG =
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerCatalogSpark35";
  private static final String DELTA_RANGER_EXTENSION =
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension";
  private static final String STOCK_RANGER_EXTENSION =
      "org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension";

  @VisibleForTesting
  static final String PAIMON_SPARK_EXTENSIONS =
      "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions";

  @VisibleForTesting
  static final String ICEBERG_SPARK_EXTENSIONS =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";

  @VisibleForTesting
  static final String DELTA_SPARK_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";

  @VisibleForTesting
  static final String DELTA_SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";

  @VisibleForTesting static final String SPARK_SESSION_CATALOG = "spark.sql.catalog.spark_catalog";

  private GravitinoCatalogManager catalogManager;
  private final Supplier<String> deltaCatalogName;
  private final ClassNameValidator deltaClassNameValidator;
  private final List<String> gravitinoIcebergExtensions =
      Arrays.asList(
          GravitinoIcebergSparkSessionExtensions.class.getName(), ICEBERG_SPARK_EXTENSIONS);
  private final List<String> gravitinoPaimonExtensions = Arrays.asList(PAIMON_SPARK_EXTENSIONS);

  private final List<String> gravitinoDriverExtensions =
      new ArrayList<>(
          Collections.singletonList(GravitinoAuthorizationSparkSessionExtensions.class.getName()));
  private boolean enableIcebergSupport = false;
  private boolean enablePaimonSupport = false;
  private boolean enableDeltaSupport = false;
  private boolean enableDeltaRangerSupport = false;

  /** Creates the Spark driver plugin. */
  public GravitinoDriverPlugin() {
    this(
        () -> CatalogNameAdaptor.getCatalogName(DELTA_PROVIDER),
        GravitinoDriverPlugin::loadClassWithoutInitialization);
  }

  @VisibleForTesting
  GravitinoDriverPlugin(
      Supplier<String> deltaCatalogName, ClassNameValidator deltaClassNameValidator) {
    this.deltaCatalogName = deltaCatalogName;
    this.deltaClassNameValidator = deltaClassNameValidator;
  }

  @Override
  public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
    SparkConf conf = sc.conf();
    String gravitinoUri = conf.get(GravitinoSparkConfig.GRAVITINO_URI);
    String metalake = conf.get(GravitinoSparkConfig.GRAVITINO_METALAKE);
    Map<String, String> gravitinoClientConfig = extractGravitinoClientConfig(conf);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(gravitinoUri),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_URI, gravitinoUri));
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake),
        String.format(
            "%s:%s, should not be empty", GravitinoSparkConfig.GRAVITINO_METALAKE, metalake));

    this.enableIcebergSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_ICEBERG_SUPPORT, false);
    this.enablePaimonSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_PAIMON_SUPPORT, false);
    configureDeltaSupport(conf);
    if (enablePaimonSupport) {
      gravitinoDriverExtensions.addAll(gravitinoPaimonExtensions);
    }
    if (enableIcebergSupport) {
      gravitinoDriverExtensions.addAll(gravitinoIcebergExtensions);
    }
    registerSqlExtensions(conf);

    this.catalogManager =
        GravitinoCatalogManager.create(
            conf,
            sc.sparkUser(),
            identity ->
                createGravitinoClient(
                    gravitinoUri, metalake, conf, sc.sparkUser(), gravitinoClientConfig));
    catalogManager.loadRelationalCatalogs();
    registerGravitinoCatalogs(conf, catalogManager.getCatalogs());
    return Collections.emptyMap();
  }

  @Override
  public void shutdown() {
    if (catalogManager != null) {
      catalogManager.close();
    }
  }

  @VisibleForTesting
  void registerGravitinoCatalogs(SparkConf sparkConf, Map<String, Catalog> gravitinoCatalogs) {
    gravitinoCatalogs
        .entrySet()
        .forEach(
            entry -> {
              String catalogName = entry.getKey();
              Catalog gravitinoCatalog = entry.getValue();
              String provider = gravitinoCatalog.provider();
              if (!isCatalogEnabled(provider)) {
                return;
              }
              if (DELTA_PROVIDER.equalsIgnoreCase(provider)) {
                registerCatalog(sparkConf, catalogName, provider);
                return;
              }
              try {
                registerCatalog(sparkConf, catalogName, provider);
              } catch (Exception e) {
                LOG.warn("Register catalog {} failed.", catalogName, e);
              }
            });
  }

  private void registerCatalog(SparkConf sparkConf, String catalogName, String provider) {
    if (StringUtils.isBlank(provider)) {
      LOG.warn("Skip registering {} because catalog provider is empty.", catalogName);
      return;
    }

    String catalogClassName =
        enableDeltaRangerSupport && DELTA_PROVIDER.equalsIgnoreCase(provider)
            ? DELTA_RANGER_CATALOG
            : CatalogNameAdaptor.getCatalogName(provider);
    if (StringUtils.isBlank(catalogClassName)) {
      LOG.warn("Skip registering {} because {} is not supported yet.", catalogName, provider);
      return;
    }

    String sparkCatalogConfigName = "spark.sql.catalog." + catalogName;
    Preconditions.checkArgument(
        !sparkConf.contains(sparkCatalogConfigName),
        catalogName + " is already registered to SparkCatalogManager");
    sparkConf.set(sparkCatalogConfigName, catalogClassName);
    LOG.info("Register {} catalog to Spark catalog manager.", catalogName);
  }

  @VisibleForTesting
  void registerSqlExtensions(SparkConf conf) {
    String extensionString = String.join(COMMA, gravitinoDriverExtensions);
    if (conf.contains(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key())) {
      String sparkSessionExtensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key());
      if (StringUtils.isNotBlank(sparkSessionExtensions)) {
        conf.set(
            StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(),
            removeDuplicateSparkExtensions(
                gravitinoDriverExtensions.toArray(new String[0]),
                sparkSessionExtensions.split(COMMA)));
      } else {
        conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
      }
    } else {
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), extensionString);
    }
  }

  @VisibleForTesting
  void configureDeltaSupport(SparkConf conf) {
    enableDeltaSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT, false);
    enableDeltaRangerSupport =
        conf.getBoolean(GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_SUPPORT, false);
    Preconditions.checkArgument(
        !enableDeltaRangerSupport || enableDeltaSupport,
        "%s requires %s",
        GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_RANGER_SUPPORT,
        GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT);
    if (!enableDeltaSupport) {
      return;
    }
    if (enableDeltaRangerSupport) {
      String extensions = conf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS().key(), "");
      Preconditions.checkArgument(
          Arrays.stream(extensions.split(COMMA))
              .map(String::trim)
              .noneMatch(STOCK_RANGER_EXTENSION::equals),
          "Delta Ranger support conflicts with stock RangerSparkExtension");
      validateDeltaClass(DELTA_RANGER_CATALOG);
      validateDeltaClass(DELTA_RANGER_EXTENSION);
    }

    String catalogClassName = deltaCatalogName.get();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogClassName),
        "%s is supported only for Spark 3.5",
        GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT);
    validateDeltaClass(catalogClassName);
    validateDeltaClass(DELTA_SPARK_EXTENSIONS);
    validateDeltaClass(DELTA_SPARK_CATALOG);

    if (conf.contains(SPARK_SESSION_CATALOG)) {
      String configuredCatalog = conf.get(SPARK_SESSION_CATALOG);
      Preconditions.checkArgument(
          DELTA_SPARK_CATALOG.equals(configuredCatalog),
          "%s must be %s when %s is true, but found %s",
          SPARK_SESSION_CATALOG,
          DELTA_SPARK_CATALOG,
          GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT,
          configuredCatalog);
    } else {
      conf.set(SPARK_SESSION_CATALOG, DELTA_SPARK_CATALOG);
    }
    if (!gravitinoDriverExtensions.contains(DELTA_SPARK_EXTENSIONS)) {
      gravitinoDriverExtensions.add(DELTA_SPARK_EXTENSIONS);
    }
    if (enableDeltaRangerSupport && !gravitinoDriverExtensions.contains(DELTA_RANGER_EXTENSION)) {
      gravitinoDriverExtensions.add(
          gravitinoDriverExtensions.indexOf(DELTA_SPARK_EXTENSIONS), DELTA_RANGER_EXTENSION);
    }
  }

  @VisibleForTesting
  boolean isCatalogEnabled(String provider) {
    if ("lakehouse-iceberg".equalsIgnoreCase(provider)) {
      return enableIcebergSupport;
    }
    if ("lakehouse-paimon".equalsIgnoreCase(provider)) {
      return enablePaimonSupport;
    }
    if (DELTA_PROVIDER.equalsIgnoreCase(provider)) {
      return enableDeltaSupport;
    }
    return true;
  }

  /**
   * Creates a Gravitino client using authentication settings from the Spark configuration.
   *
   * @param uri Gravitino REST server URI
   * @param metalake Gravitino metalake name
   * @param sparkConf Spark configuration containing auth settings
   * @param sparkUser Spark session user for simple authentication
   * @param clientConfig additional Gravitino client configuration
   * @return configured Gravitino client
   */
  public static GravitinoClient createGravitinoClient(
      String uri,
      String metalake,
      SparkConf sparkConf,
      String sparkUser,
      Map<String, String> clientConfig) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    builder.withClientConfig(clientConfig);
    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      Preconditions.checkArgument(
          !UserGroupInformation.isSecurityEnabled(),
          "Spark simple auth mode doesn't support setting kerberos configurations");
      builder.withSimpleAuth(sparkUser);
    } else if (AuthProperties.isBasic(authType)) {
      String username = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME);
      String password = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD);
      builder.withBasicAuth(username, password);
    } else if (AuthProperties.isOAuth2(authType)) {
      String oAuthUri = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_URI);
      String credential =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_CREDENTIAL);
      String path = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_PATH);
      String scope = getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_OAUTH2_SCOPE);
      DefaultOAuth2TokenProvider oAuth2TokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(oAuthUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();
      builder.withOAuth(oAuth2TokenProvider);
    } else if (AuthProperties.isKerberos(authType)) {
      String principal =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_PRINCIPAL);
      String keyTabFile =
          getRequiredConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_KERBEROS_KEYTAB_FILE_PATH);
      KerberosTokenProvider kerberosTokenProvider =
          KerberosTokenProvider.builder()
              .withClientPrincipal(principal)
              .withKeyTabFile(new File(keyTabFile))
              .build();
      builder.withKerberosAuth(kerberosTokenProvider);
    } else if (AuthProperties.isToken(authType)) {
      builder.withCustomTokenAuth(new DynamicBearerTokenProvider(sparkConf));
    } else {
      throw new UnsupportedOperationException("Unsupported auth type: " + authType);
    }
    return builder.build();
  }

  private static String getRequiredConfig(SparkConf sparkConf, String configKey) {
    String configValue = sparkConf.get(configKey, null);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(configValue), configKey + " should not be empty");
    return configValue;
  }

  private void validateDeltaClass(String className) {
    try {
      deltaClassNameValidator.validate(className);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new IllegalArgumentException(
          String.format(
              "%s requires %s on the Spark driver classpath",
              GravitinoSparkConfig.GRAVITINO_ENABLE_DELTA_SUPPORT, className),
          e);
    }
  }

  private static void loadClassWithoutInitialization(String className)
      throws ClassNotFoundException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader classLoader =
        contextClassLoader == null
            ? GravitinoDriverPlugin.class.getClassLoader()
            : contextClassLoader;
    Class.forName(className, false, classLoader);
  }

  @Nullable
  private static String getOptionalConfig(SparkConf sparkConf, String configKey) {
    return sparkConf.get(configKey, null);
  }

  /**
   * Resolves the bearer token to present to Gravitino for the operation running on the current
   * thread.
   *
   * <p>The active Spark session's configuration wins over the application configuration, so a
   * shared driver can carry a different end user's token per session, and {@code tokenFile} wins
   * over {@code token}, so a token refreshed on disk is picked up without touching the
   * configuration. Neither the token nor any prefix of it is ever logged.
   *
   * @param sparkConf the application Spark configuration, used when no session supplies a token
   * @return the bearer token, never blank
   * @throws IllegalArgumentException if neither token property is set
   * @throws UncheckedIOException if the configured token file cannot be read
   */
  public static String resolveToken(SparkConf sparkConf) {
    String tokenFile =
        getSessionOrApplicationConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_TOKEN_FILE);
    String token =
        StringUtils.isNotBlank(tokenFile)
            ? readTokenFile(tokenFile)
            : getSessionOrApplicationConfig(sparkConf, GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(token),
        String.format(
            "Either %s or %s should be set when %s is %s",
            GravitinoSparkConfig.GRAVITINO_TOKEN_VALUE,
            GravitinoSparkConfig.GRAVITINO_TOKEN_FILE,
            GravitinoSparkConfig.GRAVITINO_AUTH_TYPE,
            AuthProperties.TOKEN_AUTH_TYPE));
    return token.trim();
  }

  private static String readTokenFile(String tokenFile) {
    try {
      return new String(Files.readAllBytes(Paths.get(tokenFile)), StandardCharsets.UTF_8).trim();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read the Gravitino token file " + tokenFile, e);
    }
  }

  @Nullable
  private static String getSessionOrApplicationConfig(SparkConf sparkConf, String configKey) {
    Option<SparkSession> activeSession = SparkSession.getActiveSession();
    if (activeSession.isDefined()) {
      Option<String> sessionValue = activeSession.get().conf().getOption(configKey);
      if (sessionValue.isDefined() && StringUtils.isNotBlank(sessionValue.get())) {
        return sessionValue.get();
      }
    }
    return getOptionalConfig(sparkConf, configKey);
  }

  /**
   * Presents a bearer token that is resolved again on every request rather than captured once. See
   * {@link #resolveToken(SparkConf)} for where the token comes from.
   */
  @VisibleForTesting
  static final class DynamicBearerTokenProvider extends CustomTokenProvider {

    private final SparkConf sparkConf;

    DynamicBearerTokenProvider(SparkConf sparkConf) {
      this.sparkConf = sparkConf;
      this.schemeName = "Bearer";
    }

    @Override
    protected String getCustomTokenInfo() {
      return resolveToken(sparkConf);
    }
  }

  @FunctionalInterface
  interface ClassNameValidator {
    void validate(String className) throws ClassNotFoundException;
  }

  @VisibleForTesting
  public static Map<String, String> extractGravitinoClientConfig(SparkConf conf) {
    return Optional.ofNullable(conf.getAllWithPrefix(GRAVITINO_CLIENT_CONFIG_PREFIX))
        .map(
            arr ->
                Stream.of(arr)
                    .collect(
                        Collectors.toMap(
                            t -> GravitinoClientConfiguration.GRAVITINO_CLIENT_CONFIG_PREFIX + t._1,
                            t -> t._2,
                            (oldVal, newVal) -> newVal)))
        .orElse(ImmutableMap.of());
  }
}
