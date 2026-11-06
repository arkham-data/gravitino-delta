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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** Isolated, disposable PostgreSQL reference fixture; no existing container or database is used. */
final class RegisteredJdbcPostgresFixture implements AutoCloseable {
  private final String name = "delta-registered-jdbc-" + UUID.randomUUID();
  private final String password = UUID.randomUUID().toString().replace("-", "");
  private final Path directory;
  private final Path environment;
  private final String url;
  private boolean started;

  RegisteredJdbcPostgresFixture() throws Exception {
    directory =
        Files.createTempDirectory(
            "delta-registered-jdbc-private-",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
    environment =
        Files.createFile(
            directory.resolve("postgres.env"),
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    String admin = UUID.randomUUID().toString().replace("-", "");
    Files.write(
        environment,
        ("POSTGRES_PASSWORD=" + admin + "\nPOSTGRES_DB=extraction\n")
            .getBytes(StandardCharsets.UTF_8));
    String endpoint = null;
    try {
      String image = command("docker", "image", "inspect", "postgres:16", "--format", "{{.Id}}");
      if (!image.equals("sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"))
        throw new IllegalStateException(
            "Owned PostgreSQL fixture image differs from readiness receipt");
      command(
          "docker",
          "run",
          "--rm",
          "--detach",
          "--name",
          name,
          "--label",
          "gravitino.test.owner=registered-jdbc-task2",
          "--publish",
          "127.0.0.1::5432",
          "--env-file",
          environment.toString(),
          "--tmpfs",
          "/var/lib/postgresql/data",
          "postgres:16");
      started = true;
      String port = command("docker", "port", name, "5432/tcp");
      if (!port.matches("127\\.0\\.0\\.1:[0-9]+"))
        throw new IllegalStateException(
            "Owned PostgreSQL fixture must use an ephemeral loopback port");
      endpoint = "jdbc:postgresql://" + port + "/extraction";
      Class.forName("org.postgresql.Driver");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
      Connection ready = null;
      while (ready == null && System.nanoTime() < deadline) {
        try {
          ready = DriverManager.getConnection(endpoint, "postgres", admin);
        } catch (SQLException unavailable) {
          Thread.sleep(100);
        }
      }
      if (ready == null)
        throw new IllegalStateException("Owned PostgreSQL fixture did not become ready");
      try (Connection connection = ready;
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE public.source (id BIGINT, secret TEXT, region TEXT)");
        statement.execute(
            "INSERT INTO public.source VALUES (1, 'one', 'keep'), (2, 'two', 'drop'), (3, NULL, 'keep')");
        statement.execute(
            "CREATE TABLE public.source_typed (id BIGINT, secret VARCHAR(20), region TEXT, plain_time TIMESTAMP, zoned_time TIMESTAMPTZ)");
        statement.execute(
            "INSERT INTO public.source_typed VALUES (1, 'one', 'keep', TIMESTAMP '2026-01-02 03:04:05', TIMESTAMPTZ '2026-01-02 03:04:05+02'), (2, 'two', 'drop', NULL, NULL), (3, NULL, 'keep', TIMESTAMP '2026-02-03 04:05:06', TIMESTAMPTZ '2026-02-03 04:05:06+02')");
        statement.execute("CREATE ROLE extractor LOGIN PASSWORD '" + password + "'");
        statement.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
        statement.execute("GRANT CONNECT ON DATABASE extraction TO extractor");
        statement.execute("GRANT USAGE ON SCHEMA public TO extractor");
        statement.execute("GRANT SELECT ON public.source TO extractor");
        statement.execute("GRANT SELECT ON public.source_typed TO extractor");
        statement.execute("CREATE VIEW public.source_view AS SELECT * FROM public.source");
        statement.execute("GRANT SELECT ON public.source_view TO extractor");
        statement.execute("CREATE TABLE public.source_named AS SELECT * FROM public.source");
        statement.execute("CREATE TABLE public.sourceXnamed AS SELECT * FROM public.source");
        statement.execute("GRANT SELECT ON public.source_named TO extractor");
        // Enable statement observation only after password-bearing setup SQL has completed.
        statement.execute("ALTER SYSTEM SET log_statement = 'all'");
        statement.execute("SELECT pg_reload_conf()");
      }
      try (Connection connection = DriverManager.getConnection(endpoint, "extractor", password);
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("INSERT INTO public.source VALUES (9, 'forbidden', 'keep')");
          throw new IllegalStateException("Extraction role unexpectedly permits writes");
        } catch (SQLException denied) {
          if (!"42501".equals(denied.getSQLState()))
            throw new IllegalStateException("Read-only extraction role check failed");
        }
      }
    } catch (Exception failure) {
      close();
      throw new IllegalStateException(
          "Owned PostgreSQL fixture setup failed",
          failure instanceof SQLException ? null : failure);
    }
    url = endpoint;
  }

  static Dataset<Row> frame(SparkSession session, LogicalPlan plan) {
    return Dataset.ofRows(session, plan);
  }

  String url() {
    return url;
  }

  String password() {
    return password;
  }

  // Test-only lock lets native schema discovery overlap scope closure deterministically.
  Connection blockTypedSchema() throws Exception {
    String admin = Files.readAllLines(environment).get(0).substring("POSTGRES_PASSWORD=".length());
    Connection connection = DriverManager.getConnection(url, "postgres", admin);
    try {
      connection.setAutoCommit(false);
      try (Statement statement = connection.createStatement()) {
        statement.execute("LOCK TABLE public.source_typed IN ACCESS EXCLUSIVE MODE");
      }
      return connection;
    } catch (Exception failure) {
      connection.close();
      throw failure;
    }
  }

  int blockedSchemaReads() throws Exception {
    String admin = Files.readAllLines(environment).get(0).substring("POSTGRES_PASSWORD=".length());
    try (Connection connection = DriverManager.getConnection(url, "postgres", admin);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT count(*) FROM pg_stat_activity WHERE usename='extractor' AND wait_event_type='Lock'")) {
      result.next();
      return result.getInt(1);
    }
  }

  int authenticationFailures() throws Exception {
    return (int)
        command("docker", "logs", name)
            .lines()
            .filter(line -> line.contains("password authentication failed for user"))
            .count();
  }

  int schemaReads() throws Exception {
    return (int)
        command("docker", "logs", name)
            .lines()
            .filter(line -> line.contains("SELECT") && line.contains("WHERE 1=0"))
            .count();
  }

  int payloadReads() throws Exception {
    String log = command("docker", "logs", name);
    return (int)
        log.lines()
            .filter(
                line ->
                    line.contains("SELECT")
                        && (line.contains("FROM \"public\".\"source")
                            || line.contains("FROM public.source"))
                        && !line.contains("WHERE 1=0"))
            .count();
  }

  /** Stops only the uniquely named container created by this fixture. */
  @Override
  public void close() throws Exception {
    if (started) {
      command("docker", "rm", "--force", name);
      started = false;
    }
    Files.deleteIfExists(environment);
    Files.deleteIfExists(directory);
  }

  private static String command(String... arguments) throws Exception {
    Path output =
        Files.createTempFile(
            "registered-jdbc-command-",
            ".out",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
    try {
      Process process =
          new ProcessBuilder(Arrays.asList(arguments))
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      if (!process.waitFor(45, TimeUnit.SECONDS) || process.exitValue() != 0) {
        process.destroyForcibly();
        throw new IllegalStateException("Owned PostgreSQL fixture command failed");
      }
      return Files.readString(output, StandardCharsets.UTF_8).trim();
    } finally {
      Files.deleteIfExists(output);
    }
  }
}
