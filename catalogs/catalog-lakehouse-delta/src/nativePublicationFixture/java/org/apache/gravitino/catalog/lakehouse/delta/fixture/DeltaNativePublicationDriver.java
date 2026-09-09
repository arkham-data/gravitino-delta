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

package org.apache.gravitino.catalog.lakehouse.delta.fixture;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaSchemaContract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.VersionInfo;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;

/** Private child-process schema-only native fixture; consumes an actual server-issued receipt. */
public final class DeltaNativePublicationDriver {
  private static final ObjectMapper JSON = new ObjectMapper();

  private DeltaNativePublicationDriver() {}

  /**
   * Commits one native schema-only operation using the exact server marker.
   *
   * @param args the single private manifest path, with no credential bytes on the command line
   * @throws Exception if admission, native staging, evidence, or shutdown fails
   */
  public static void main(String[] args) throws Exception {
    require(args.length == 1);
    Path manifest = Path.of(args[0]);
    require(!Files.isSymbolicLink(manifest) && Files.size(manifest) <= 1024 * 1024);
    JsonNode input = JSON.readTree(Files.readAllBytes(manifest));
    if (input.has("fixture")) {
      require("MALFORMED_ADOPTION_MARKER".equals(text(input, "fixture")));
      malformedAdoptionMarker(input);
      return;
    }
    DeltaRegistrationCommand intent = DeltaRegistrationCommand.decode(text(input, "intent"));
    Map<String, String> properties =
        JSON.convertValue(input.get("receipt"), new TypeReference<Map<String, String>>() {});
    DeltaRegistrationReceipt receipt = DeltaRegistrationReceipt.fromProperties(properties);
    DeltaOperationMarker marker = receipt.marker().orElseThrow();
    boolean create = intent.operation() == DeltaRegistrationCommand.Operation.RESERVE;
    require(
        create
            ? receipt.state() == DeltaRegistrationReceipt.State.RESERVED
                && marker.operation() == DeltaOperationMarker.Operation.CREATE
            : intent.operation() == DeltaRegistrationCommand.Operation.BEGIN_REPLACE
                && receipt.state() == DeltaRegistrationReceipt.State.REPLACING
                && marker.operation() == DeltaOperationMarker.Operation.REPLACE);
    require(receipt.runId().equals(intent.runId().orElseThrow()));
    require(
        marker.token().equals((create ? intent.token() : intent.replacementToken()).orElseThrow()));
    if (!create) {
      require(marker.id() == intent.expected().orElseThrow().id());
      require(marker.generation() == intent.expected().orElseThrow().generation() + 1);
    }
    require(
        marker.id() == receipt.identity().id()
            && marker.generation() == receipt.identity().generation());
    require(marker.schemaContract().isPresent() == intent.schemaContract().isPresent());
    if (marker.schemaContract().isPresent()) {
      DeltaSchemaContract actual = marker.schemaContract().orElseThrow();
      DeltaSchemaContract desired = intent.schemaContract().orElseThrow();
      require(
          actual.operationMode() == DeltaSchemaContract.OperationMode.SCHEMA_ONLY
              && actual.operationMode() == desired.operationMode());
      require(
          JSON.readTree(actual.requiredSchemaJson())
              .equals(JSON.readTree(desired.requiredSchemaJson())));
    } else {
      require(
          JSON.readTree(marker.schemaJson())
              .equals(JSON.readTree(intent.exactSchemaJson().orElseThrow())));
    }
    owned(marker.normalizedLocation(), text(input, "ownedRoot"), text(input, "bucket"));
    StructType schema = (StructType) StructType.fromJson(marker.schemaJson());
    Map<String, String> nativeProperties =
        marker.injectInto(
            Map.of(
                "provider",
                "delta",
                "delta.columnMapping.mode",
                marker.schemaContract().isPresent() ? "name" : "none"));
    Configuration storage = storage(input);
    Path evidence = Path.of(text(input, "evidence"));
    require(!Files.isSymbolicLink(evidence) && Files.isDirectory(evidence));
    SparkSession spark = spark(storage, evidence);
    try (FileSystem fs = FileSystem.newInstance(URI.create(marker.normalizedLocation()), storage)) {
      require(spark.version().equals("3.5.6") && VersionInfo.getVersion().equals("3.3.6"));
      require(
          origin(DeltaCatalog.class)
              .contains("delta-spark_2.12-" + text(input, "deltaVersion") + ".jar"));
      if (create) {
        require(!fs.exists(new org.apache.hadoop.fs.Path(marker.normalizedLocation())));
      } else {
        Snapshot base = snapshot(spark, marker.normalizedLocation());
        require(
            base.version() == marker.baseVersion().orElseThrow()
                && base.metadata().id().equals(marker.baseUuid().orElseThrow()));
      }
      DeltaCatalog catalog = new DeltaCatalog();
      PathOnlyDelegate delegate = new PathOnlyDelegate();
      delegate.initialize("fixture", new CaseInsensitiveStringMap(Map.of()));
      catalog.setDelegateCatalog(delegate);
      Identifier ident = Identifier.of(new String[] {"delta"}, marker.normalizedLocation());
      StagedTable staged =
          create
              ? catalog.stageCreate(ident, schema, new Transform[0], nativeProperties)
              : catalog.stageReplace(ident, schema, new Transform[0], nativeProperties);
      staged.commitStagedChanges();
      Snapshot committed = snapshot(spark, marker.normalizedLocation());
      require(committed.version() == (create ? 0 : marker.baseVersion().orElseThrow() + 1));
      require(
          committed
              .metadata()
              .configuration()
              .get(DeltaOperationMarker.PROPERTY_KEY)
              .get()
              .equals(marker.encode()));
      Map<String, Object> result = new HashMap<>();
      result.put("version", committed.version());
      result.put("nativeId", committed.metadata().id());
      result.put("sparkVersion", spark.version());
      result.put("hadoopVersion", VersionInfo.getVersion());
      result.put("deltaOrigin", origin(DeltaCatalog.class));
      result.put("sparkOrigin", origin(SparkSession.class));
      result.put("hadoopOrigin", origin(FileSystem.class));
      result.put(
          "markerSha256",
          hex(
              MessageDigest.getInstance("SHA-256")
                  .digest(marker.encode().getBytes(StandardCharsets.UTF_8))));
      result.put("schema", committed.metadata().schemaString());
      Files.write(
          evidence.resolve("result.json"),
          JSON.writeValueAsBytes(result),
          StandardOpenOption.CREATE_NEW);
      try (InputStream commit =
          fs.open(
              new org.apache.hadoop.fs.Path(
                  marker.normalizedLocation()
                      + "/_delta_log/"
                      + String.format(Locale.ROOT, "%020d.json", committed.version())))) {
        Files.copy(commit, evidence.resolve("native-commit.json"));
      }
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  private static SparkSession spark(Configuration storage, Path evidence) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[2]")
            .appName("delta-private-receipt-fixture")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "none")
            .config("spark.sql.warehouse.dir", evidence.resolve("warehouse").toString());
    for (Map.Entry<String, String> entry : storage) {
      builder.config("spark.hadoop." + entry.getKey(), entry.getValue());
    }
    return builder.getOrCreate();
  }

  private static void malformedAdoptionMarker(JsonNode input) throws Exception {
    String location = text(input, "location");
    owned(location, text(input, "ownedRoot"), text(input, "bucket"));
    Path evidence = Path.of(text(input, "evidence"));
    require(!Files.isSymbolicLink(evidence) && Files.isDirectory(evidence));
    Configuration storage = storage(input);
    SparkSession spark = spark(storage, evidence);
    try (FileSystem fs = FileSystem.newInstance(URI.create(location), storage)) {
      require(spark.version().equals("3.5.6") && VersionInfo.getVersion().equals("3.3.6"));
      require(
          origin(DeltaCatalog.class)
              .contains("delta-spark_2.12-" + text(input, "deltaVersion") + ".jar"));
      Snapshot before = snapshot(spark, location);
      require(before.version() == 0);
      DeltaCatalog catalog = new DeltaCatalog();
      PathOnlyDelegate delegate = new PathOnlyDelegate();
      delegate.initialize("fixture", new CaseInsensitiveStringMap(Map.of()));
      catalog.setDelegateCatalog(delegate);
      // Deliberately invalid provenance is committed by Spark, never fabricated JSON.
      String invalid = "task4-malformed-marker";
      catalog.alterTable(
          Identifier.of(new String[] {"delta"}, location),
          new TableChange[] {TableChange.setProperty(DeltaOperationMarker.PROPERTY_KEY, invalid)});
      Snapshot after = snapshot(spark, location);
      require(after.version() == 1 && before.metadata().id().equals(after.metadata().id()));
      require(
          after
              .metadata()
              .configuration()
              .get(DeltaOperationMarker.PROPERTY_KEY)
              .get()
              .equals(invalid));
      Files.write(
          evidence.resolve("result.json"),
          JSON.writeValueAsBytes(
              Map.of(
                  "fixture",
                  "MALFORMED_ADOPTION_MARKER",
                  "version",
                  after.version(),
                  "nativeId",
                  after.metadata().id(),
                  "sparkVersion",
                  spark.version(),
                  "hadoopVersion",
                  VersionInfo.getVersion(),
                  "deltaOrigin",
                  origin(DeltaCatalog.class))),
          StandardOpenOption.CREATE_NEW);
      try (InputStream commit =
          fs.open(
              new org.apache.hadoop.fs.Path(location + "/_delta_log/00000000000000000001.json"))) {
        Files.copy(commit, evidence.resolve("native-commit.json"));
      }
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  private static Snapshot snapshot(SparkSession spark, String location) {
    return DeltaLog.forTable(spark, location).update(false, Option.empty(), Option.empty());
  }

  private static String origin(Class<?> type) {
    return type.getProtectionDomain().getCodeSource().getLocation().toString();
  }

  private static String text(JsonNode node, String key) {
    require(node.hasNonNull(key) && node.get(key).isTextual());
    return node.get(key).asText();
  }

  private static void require(boolean condition) {
    if (!condition) {
      throw new IllegalArgumentException("INVALID_PRIVATE_NATIVE_FIXTURE");
    }
  }

  private static String hex(byte[] value) {
    StringBuilder out = new StringBuilder();
    for (byte b : value) {
      out.append(String.format(Locale.ROOT, "%02x", b & 255));
    }
    return out.toString();
  }

  private static void owned(String location, String root, String bucket) {
    URI actual = URI.create(location);
    URI parent = URI.create(root);
    for (URI uri : Arrays.asList(actual, parent)) {
      require(
          "s3a".equals(uri.getScheme())
              && bucket.equals(uri.getRawAuthority())
              && uri.getQuery() == null
              && uri.getFragment() == null
              && uri.getRawPath().equals(uri.getPath()));
      for (String segment : uri.getPath().substring(1).split("/")) {
        require(
            segment.matches("[A-Za-z0-9._-]+") && !segment.equals(".") && !segment.equals(".."));
      }
    }
    String[] path = actual.getPath().split("/");
    String[] prefix = parent.getPath().split("/");
    require(path.length > prefix.length);
    for (int i = 0; i < prefix.length; i++) {
      require(path[i].equals(prefix[i]));
    }
  }

  private static Configuration storage(JsonNode input) throws Exception {
    Properties credentials = new Properties();
    Path file = Path.of(text(input, "credentialsFile"));
    require(!Files.isSymbolicLink(file));
    try (InputStream stream = Files.newInputStream(file)) {
      credentials.load(stream);
    }
    Configuration result = new Configuration(false);
    result.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    result.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    result.set("fs.s3a.access.key", credentials.getProperty("MINIO_ROOT_USER"));
    result.set("fs.s3a.secret.key", credentials.getProperty("MINIO_ROOT_PASSWORD"));
    result.set("fs.s3a.endpoint", text(input, "endpoint"));
    result.set("fs.s3a.endpoint.region", text(input, "region"));
    result.setBoolean("fs.s3a.path.style.access", true);
    result.setBoolean(
        "fs.s3a.connection.ssl.enabled",
        URI.create(text(input, "endpoint")).getScheme().equals("https"));
    return result;
  }

  private static final class PathOnlyDelegate extends DelegatingCatalogExtension {
    /** {@inheritDoc} */
    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {}
    /** {@inheritDoc} */
    @Override
    public String name() {
      return "fixture";
    }
    /** {@inheritDoc} */
    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
      throw new NoSuchTableException(ident);
    }
  }
}
