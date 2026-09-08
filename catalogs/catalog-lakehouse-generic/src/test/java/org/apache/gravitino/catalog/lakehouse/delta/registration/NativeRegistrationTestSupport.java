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
package org.apache.gravitino.catalog.lakehouse.delta.registration;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.hadoop.conf.Configuration;

/** Test-artifact bridge for authoritative receipts and explicit stopped-writer handoff. */
public final class NativeRegistrationTestSupport {
  private static volatile boolean stopped;

  private NativeRegistrationTestSupport() {}

  /** Marks handoff only after the test has actually stopped its sole Spark writer. */
  public static void handoffAfterSparkStopped() {
    stopped = true;
  }

  /** Starts a fresh test writer epoch; this boolean is not a production fence. */
  public static void writerStarted() {
    stopped = false;
  }

  static boolean writerStopped() {
    return stopped;
  }

  /**
   * Reads private storage credentials independently from the active Spark Hadoop configuration.
   *
   * @return private server-owned configuration, never catalog properties or evidence
   */
  public static Configuration storageConfiguration() {
    Properties credentials = new Properties();
    try (Reader reader = Files.newBufferedReader(Paths.get(required("credentialsFile")))) {
      credentials.load(reader);
    } catch (IOException failure) {
      throw new IllegalStateException("Cannot read private storage credentials");
    }
    Configuration result = new Configuration();
    result.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    result.set("fs.s3a.endpoint", required("endpoint"));
    result.set("fs.s3a.path.style.access", "true");
    result.set("fs.s3a.connection.ssl.enabled", "false");
    result.set(
        "fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    result.set("fs.s3a.access.key", credential(credentials, "MINIO_ROOT_USER"));
    result.set("fs.s3a.secret.key", credential(credentials, "MINIO_ROOT_PASSWORD"));
    return result;
  }

  /**
   * Reserves through real HTTP and returns only the authoritative row-derived marker.
   *
   * @param catalog actor's real remote catalog
   * @param id registered identifier
   * @param location intended location
   * @param schema intended schema
   * @param replacing whether this is an existing publication
   * @return immutable operation marker from the server receipt
   */
  public static DeltaOperationMarker begin(
      TableCatalog catalog, NameIdentifier id, String location, String schema, boolean replacing) {
    Table receipt;
    String token = UUID.randomUUID().toString();
    if (replacing) {
      Map<String, String> current = catalog.loadTable(id).properties();
      RegistrationCommand command =
          RegistrationCommand.beginReplace(
              number(current, "id"),
              number(current, "generation"),
              current.get("delta.registration.receipt.token"),
              token,
              schema);
      receipt =
          catalog.alterTable(
              id, TableChange.setProperty(RegistrationCommand.KEY, command.encode()));
    } else {
      receipt =
          catalog.createTable(
              id,
              new Column[0],
              null,
              Collections.singletonMap(
                  RegistrationCommand.KEY,
                  RegistrationCommand.reserve(token, location, schema).encode()));
    }
    return DeltaOperationMarker.decode(
        receipt.properties().get("delta.registration.receipt.marker"));
  }

  /**
   * Publishes a receipt through real HTTP. The marker is only used to form the command receipt.
   *
   * @param catalog authorized remote catalog
   * @param id registered identifier
   * @param marker immutable operation receipt
   * @return remote publication response
   */
  public static Table publish(
      TableCatalog catalog, NameIdentifier id, DeltaOperationMarker marker) {
    return catalog.alterTable(
        id,
        TableChange.setProperty(
            RegistrationCommand.KEY,
            RegistrationCommand.publish(
                    marker.id(),
                    marker.generation(),
                    marker.token(),
                    marker.baseVersion().isPresent() ? marker.baseVersion().getAsLong() + 1 : 0)
                .encode()));
  }

  /**
   * Writes an operator receipt journal, with no schema, storage configuration or native verdict.
   *
   * @param path owned journal
   * @param id name to recover
   * @param marker operation receipt
   * @throws IOException on durable receipt failure
   */
  public static void journal(Path path, NameIdentifier id, DeltaOperationMarker marker)
      throws IOException {
    Properties p = new Properties();
    p.setProperty("schema", id.namespace().level(0));
    p.setProperty("name", id.name());
    p.setProperty("id", Long.toString(marker.id()));
    p.setProperty("generation", Long.toString(marker.generation()));
    p.setProperty("token", marker.token());
    p.setProperty(
        "candidate",
        Long.toString(marker.baseVersion().isPresent() ? marker.baseVersion().getAsLong() + 1 : 0));
    try (Writer writer = Files.newBufferedWriter(path)) {
      p.store(writer, "Test operator receipt; not registration authority");
    }
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  /**
   * Recovers solely from a reloaded durable operator receipt through real authorized HTTP.
   *
   * @param catalog recovery principal's remote catalog
   * @param path persisted receipt
   * @return publication response
   * @throws IOException on receipt read failure
   */
  public static Table recover(TableCatalog catalog, Path path) throws IOException {
    Properties p = new Properties();
    try (Reader reader = Files.newBufferedReader(path)) {
      p.load(reader);
    }
    if (p.size() != 6) throw new IllegalArgumentException("Invalid operator receipt");
    RegistrationCommand command =
        RegistrationCommand.publish(
            Long.parseLong(p.getProperty("id")),
            Long.parseLong(p.getProperty("generation")),
            p.getProperty("token"),
            Long.parseLong(p.getProperty("candidate")));
    return catalog.alterTable(
        NameIdentifier.of(p.getProperty("schema"), p.getProperty("name")),
        TableChange.setProperty(RegistrationCommand.KEY, command.encode()));
  }

  /**
   * Reads a durable row for test assertions, never pending-operation discovery.
   *
   * @param id full registration identifier
   * @return immutable raw row
   * @throws IOException on store read failure
   */
  public static TableEntity raw(NameIdentifier id) throws IOException {
    return GravitinoEnv.getInstance()
        .entityStore()
        .get(id, Entity.EntityType.TABLE, TableEntity.class);
  }

  /**
   * Invokes the real reader for negative fixture assertions without any synthetic verdict seam.
   *
   * @param row immutable trusted-location row or explicitly labelled corrupt fixture
   * @param hint candidate hint
   * @return correlation kind and credential-free reason
   */
  public static String inspect(TableEntity row, long hint) {
    RegistrationCommitVerifier.Result result =
        new NativeRegistrationCommitVerifier(storageConfiguration()).verify(row, hint);
    return result.kind().name() + ":" + result.reason();
  }

  private static long number(Map<String, String> receipt, String suffix) {
    return Long.parseLong(receipt.get("delta.registration.receipt." + suffix));
  }

  private static String required(String suffix) {
    String value = System.getProperty("gravitino.delta.test.s3." + suffix);
    if (value == null || value.isEmpty())
      throw new IllegalStateException("Missing explicit S3 test input " + suffix);
    return value;
  }

  private static String credential(Properties p, String key) {
    String value = p.getProperty(key);
    if (value == null || value.isEmpty())
      throw new IllegalStateException("Missing private credential entry");
    return value;
  }
}
