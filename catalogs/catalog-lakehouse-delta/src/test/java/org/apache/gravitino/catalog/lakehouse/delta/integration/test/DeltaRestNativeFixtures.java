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

package org.apache.gravitino.catalog.lakehouse.delta.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.MiniGravitino;
import org.apache.gravitino.integration.test.MiniGravitinoContext;
import org.apache.gravitino.server.GravitinoServer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

/** Owns only temporary embedded servers, child processes, and uniquely scoped native test paths. */
final class DeltaRestNativeFixtures implements AutoCloseable {
  static final ObjectMapper JSON = new ObjectMapper();
  static final String ADMIN = "native-admin";
  static final String METALAKE = "native_delta";
  static final String SCHEMA = "schema";
  static final String SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}";
  final String pin = System.getProperty("deltaKernelVersion");
  final Map<String, String> inputs = new HashMap<>();
  final Map<String, String> properties = new HashMap<>();
  final Map<String, String> serverConfig = new HashMap<>();
  final List<GravitinoAdminClient> clients = new ArrayList<>();
  final Path evidence;
  final String root;
  final Configuration storage = new Configuration(false);
  final Object previousConfig;
  final Object previousMetrics;
  MiniGravitino server;
  GravitinoMetalake admin;
  String uri;
  FileSystem filesystem;
  private int evidenceSequence;

  DeltaRestNativeFixtures() throws Exception {
    for (String key : List.of("endpoint", "region", "bucket", "credentialsFile", "prefix")) {
      String value = System.getProperty("delta.provider.it." + key);
      if (value == null || value.isBlank()) {
        throw new IllegalArgumentException("Native acceptance requires delta.provider.it." + key);
      }
      inputs.put(key, value);
    }
    String unique = "run-" + pin + "-" + UUID.randomUUID();
    String prefix = inputs.get("prefix");
    if (prefix.startsWith("s3a://")) {
      prefix = URI.create(prefix).getPath().substring(1);
    }
    root = "s3a://" + inputs.get("bucket") + "/" + prefix.replaceAll("/+$", "") + "/" + unique;
    evidence = Path.of(System.getenv("IT_PROJECT_DIR"), "native-rest-evidence", unique);
    Files.createDirectories(evidence.getParent());
    Files.createDirectory(
        evidence,
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    Properties credentials = new Properties();
    try (InputStream stream = Files.newInputStream(Path.of(inputs.get("credentialsFile")))) {
      credentials.load(stream);
    }
    properties.put("delta.warehouse", root);
    properties.put("delta.adoption-roots", JSON.writeValueAsString(List.of(root)));
    properties.put("delta.recovery-principals", JSON.writeValueAsString(List.of(ADMIN)));
    properties.put("delta.storage-credential-mode", "static");
    properties.put("s3-access-key-id", credentials.getProperty("MINIO_ROOT_USER"));
    properties.put("s3-secret-access-key", credentials.getProperty("MINIO_ROOT_PASSWORD"));
    properties.put("s3-endpoint", inputs.get("endpoint"));
    properties.put("s3-region", inputs.get("region"));
    properties.put("s3-path-style-access", "true");
    properties.put(Catalog.PROPERTY_PACKAGE, System.getProperty("delta.provider.package"));
    storage.set("hadoop.tmp.dir", Files.createDirectory(evidence.resolve("hadoop-tmp")).toString());
    storage.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    storage.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    storage.set("fs.s3a.access.key", credentials.getProperty("MINIO_ROOT_USER"));
    storage.set("fs.s3a.secret.key", credentials.getProperty("MINIO_ROOT_PASSWORD"));
    storage.set("fs.s3a.endpoint", inputs.get("endpoint"));
    storage.set("fs.s3a.endpoint.region", inputs.get("region"));
    storage.setBoolean("fs.s3a.path.style.access", true);
    storage.setBoolean(
        "fs.s3a.connection.ssl.enabled",
        URI.create(inputs.get("endpoint")).getScheme().equals("https"));
    previousConfig = FieldUtils.readField(GravitinoEnv.getInstance(), "config", true);
    previousMetrics = FieldUtils.readField(GravitinoEnv.getInstance(), "metricsSystem", true);
    serverConfig.put(Configs.ENTITY_STORE.getKey(), Configs.RELATIONAL_ENTITY_STORE);
    serverConfig.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH.getKey(), evidence.resolve("store").toString());
    serverConfig.put(
        Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL.getKey(),
        "jdbc:h2:file:"
            + evidence.resolve("store")
            + ";DB_CLOSE_DELAY=0;MODE=MYSQL;AUTO_SERVER=FALSE");
    serverConfig.put(Configs.CACHE_ENABLED.getKey(), "false");
    serverConfig.put(Configs.CATALOG_LOAD_ISOLATED.getKey(), "true");
    serverConfig.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    serverConfig.put(Configs.AUTHENTICATORS.getKey(), "simple");
    serverConfig.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    serverConfig.put("SimpleAuthUserName", ADMIN);
    serverConfig.put("gravitino.auxService.names", "");
    Files.write(
        evidence.resolve("inputs.json"),
        JSON.writeValueAsBytes(inputs),
        StandardOpenOption.CREATE_NEW);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws Exception {
    try {
      stopServer();
    } finally {
      try {
        if (filesystem != null) {
          filesystem.close();
        }
      } finally {
        FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
        FieldUtils.writeField(GravitinoEnv.getInstance(), "metricsSystem", previousMetrics, true);
      }
    }
  }

  void start() throws Exception {
    server = new MiniGravitino(new MiniGravitinoContext(serverConfig, true, true));
    server.start();
    JettyServerConfig jetty =
        JettyServerConfig.fromConfig(
            server.getServerConfig(), GravitinoServer.WEBSERVER_CONF_PREFIX);
    uri = "http://" + jetty.getHost() + ":" + jetty.getHttpPort();
    assertTrue(server.getServerConfig().get(Configs.CATALOG_LOAD_ISOLATED));
    assertEquals(
        "org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer",
        GravitinoEnv.getInstance().gravitinoAuthorizer().getClass().getName());
    GravitinoAdminClient client = client(ADMIN);
    if (Arrays.stream(client.listMetalakes()).noneMatch(m -> METALAKE.equals(m.name()))) {
      client.createMetalake(METALAKE, null, Map.of());
    }
    admin = client.loadMetalake(METALAKE);
    if (filesystem == null) {
      filesystem = FileSystem.newInstance(URI.create(root), storage);
    }
  }

  void restart() throws Exception {
    stopServer();
    start();
  }

  GravitinoAdminClient client(String principal) {
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(uri).withSimpleAuth(principal).build();
    clients.add(client);
    return client;
  }

  void catalog(String name, Map<String, String> overrides) {
    Map<String, String> conf = new HashMap<>(properties);
    conf.putAll(overrides);
    admin.createCatalog(name, Catalog.Type.RELATIONAL, "lakehouse-delta", null, conf);
    admin.loadCatalog(name).asSchemas().createSchema(SCHEMA, null, Map.of());
  }

  String location(String name) {
    return root + "/fixtures/" + name;
  }

  void nativeTable(String location, Map<String, String> config) throws Exception {
    nativeOperation(
        engine ->
            Table.forPath(engine, location)
                .createTransactionBuilder(engine, "task4-kernel-fixture", Operation.CREATE_TABLE)
                .withSchema(engine, new StructType().add("id", LongType.LONG))
                .withTableProperties(engine, config)
                .build(engine)
                .commit(engine, CloseableIterable.emptyIterable()));
  }

  void nativeChange(String location, Map<String, String> config) throws Exception {
    nativeOperation(
        engine ->
            Table.forPath(engine, location)
                .createTransactionBuilder(engine, "task4-kernel-fixture", Operation.WRITE)
                .withTableProperties(engine, config)
                .build(engine)
                .commit(engine, CloseableIterable.emptyIterable()));
  }

  void nativeOperation(Consumer<Engine> action) throws Exception {
    UserGroupInformation owner =
        UserGroupInformation.createRemoteUser("task4-native-" + UUID.randomUUID());
    try {
      owner.doAs(
          (PrivilegedExceptionAction<Void>)
              () -> {
                action.accept(DefaultEngine.create(new Configuration(storage)));
                return null;
              });
    } finally {
      FileSystem.closeAllForUGI(owner);
    }
  }

  Map<String, String> manifest(String location) throws Exception {
    Map<String, String> result = new TreeMap<>();
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
    if (!filesystem.exists(path)) {
      return result;
    }
    RemoteIterator<LocatedFileStatus> files = filesystem.listFiles(path, true);
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      try (InputStream input = filesystem.open(file.getPath())) {
        byte[] bytes = new byte[8192];
        int count;
        while ((count = input.read(bytes)) != -1) {
          digest.update(bytes, 0, count);
        }
      }
      result.put(file.getPath().toString(), HexFormat.of().formatHex(digest.digest()));
    }
    Files.write(
        evidence.resolve("object-manifest-" + (evidenceSequence++) + ".json"),
        JSON.writeValueAsBytes(Map.of("location", location, "objects", result)),
        StandardOpenOption.CREATE_NEW);
    return result;
  }

  void receipt(
      String method,
      String name,
      DeltaRegistrationCommand command,
      DeltaRegistrationReceipt receipt)
      throws Exception {
    privateWrite(
        evidence.resolve("receipt-" + (evidenceSequence++) + ".json"),
        JSON.writeValueAsBytes(
            Map.of(
                "method",
                method,
                "name",
                name,
                "command",
                command.encode(),
                "receipt",
                receipt.toProperties())));
  }

  Path openingIntent(DeltaRegistrationCommand command) throws Exception {
    return intent("opening-" + (evidenceSequence++), command);
  }

  Path intent(String label, DeltaRegistrationCommand command) throws Exception {
    Path target = evidence.resolve(label + "-intent.json");
    privateWrite(target, command.encode().getBytes(StandardCharsets.UTF_8));
    return target;
  }

  JsonNode publishNative(String label, Path intent, DeltaRegistrationReceipt receipt)
      throws Exception {
    Map<String, Object> manifest = new HashMap<>(inputs);
    manifest.put("intent", Files.readString(intent));
    manifest.put("receipt", receipt.toProperties());
    return nativeChild(label, manifest);
  }

  JsonNode malformedNativeMarker(String location) throws Exception {
    Map<String, Object> manifest = new HashMap<>(inputs);
    manifest.put("fixture", "MALFORMED_ADOPTION_MARKER");
    manifest.put("location", location);
    return nativeChild("malformed-adoption-marker", manifest);
  }

  Object origin(String catalog) throws Exception {
    return GravitinoEnv.getInstance()
        .catalogManager()
        .doWithCatalog(
            NameIdentifier.of(METALAKE, catalog),
            c -> {
              ClassLoader loader = c.getClass().getClassLoader();
              Class<?> kernel = Class.forName("io.delta.kernel.Table", true, loader);
              Map<String, String> origins =
                  Map.of(
                      "provider",
                      c.getClass().getProtectionDomain().getCodeSource().getLocation().toString(),
                      "kernel",
                      kernel.getProtectionDomain().getCodeSource().getLocation().toString(),
                      "loader",
                      loader.toString());
              Files.write(
                  evidence.resolve(catalog + "-origins.json"), JSON.writeValueAsBytes(origins));
              assertTrue(
                  origins
                      .get("provider")
                      .contains(
                          "distribution/package/catalogs/lakehouse-delta/libs/gravitino-catalog-lakehouse-delta"));
              assertTrue(origins.get("kernel").contains("delta-kernel-api-" + pin + ".jar"));
              return loader;
            });
  }

  private JsonNode nativeChild(String label, Map<String, Object> manifest) throws Exception {
    Path output = evidence.resolve(label);
    Files.createDirectory(
        output, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    manifest.put("ownedRoot", root);
    manifest.put("evidence", output.toString());
    manifest.put("deltaVersion", pin);
    Path input = output.resolve("manifest.json");
    privateWrite(input, JSON.writeValueAsBytes(manifest));
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    command.add("-Xmx1g");
    command.addAll(
        JSON.readValue(
            Files.readString(Path.of(System.getProperty("delta.provider.fixture.jvmArgsFile"))),
            new TypeReference<List<String>>() {}));
    command.add(
        "-Dlog4j.configurationFile="
            + Path.of(
                System.getenv("GRAVITINO_ROOT_DIR"),
                "catalogs/catalog-lakehouse-delta/src/nativePublicationFixture/resources/log4j2.properties"));
    command.add("-cp");
    command.add(
        Files.readString(Path.of(System.getProperty("delta.provider.fixture.classpathFile"))));
    command.add(
        "org.apache.gravitino.catalog.lakehouse.delta.fixture.DeltaNativePublicationDriver");
    command.add(input.toString());
    Files.copy(
        Path.of(System.getProperty("delta.provider.fixture.classpathFile")),
        output.resolve("classpath.txt"));
    Process child =
        new ProcessBuilder(command)
            .redirectOutput(output.resolve("stdout.log").toFile())
            .redirectError(output.resolve("stderr.log").toFile())
            .start();
    boolean exited = false;
    try {
      exited = child.waitFor(180, TimeUnit.SECONDS);
      if (!exited) {
        child.destroyForcibly();
        assertTrue(child.waitFor(20, TimeUnit.SECONDS));
      }
      Files.writeString(
          output.resolve("exit.txt"), "timedOut=" + !exited + "\nexit=" + child.exitValue() + "\n");
      assertTrue(exited, "Native child timed out; preserve pending receipt, do not retry commit");
      assertEquals(0, child.exitValue(), "Native child failed: " + output);
      return JSON.readTree(Files.readAllBytes(output.resolve("result.json")));
    } finally {
      if (child.isAlive()) {
        child.destroyForcibly();
        assertTrue(child.waitFor(20, TimeUnit.SECONDS));
      }
    }
  }

  private void privateWrite(Path path, byte[] bytes) throws Exception {
    Files.createFile(
        path, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    Files.write(path, bytes);
  }

  private void stopServer() throws Exception {
    for (GravitinoAdminClient client : clients) {
      client.close();
    }
    clients.clear();
    if (server != null) {
      server.stop();
      server = null;
    }
  }
}
