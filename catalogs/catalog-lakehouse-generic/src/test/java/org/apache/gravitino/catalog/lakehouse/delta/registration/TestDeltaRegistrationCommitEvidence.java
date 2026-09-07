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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises read-only registration evidence against actual local Delta transaction logs. */
class TestDeltaRegistrationCommitEvidence {
  private static final String INJECTED_FAILURE = "delta.registration.test.filesystem.failure";
  @TempDir Path directory;

  @Test
  void testValidSnapshotWithoutCorrelationIsAmbiguous() {
    assertLoadedKernelArtifacts();
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER, false).add("name", StringType.STRING, true);
    Path tablePath = directory.resolve("valid");
    createDeltaTable(tablePath, schema);

    RegistrationCommitVerifier.Result result =
        new KernelRegistrationCommitInspector(new Configuration())
            .verify(reservation(tablePath, schema.toJson(), null), 0L);

    assertCannotPublish(result);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, result.kind());
    RegistrationCommitVerifier.Observation observation = result.observation();
    Assertions.assertNotNull(observation);
    Assertions.assertEquals(0L, observation.nativeVersion());
    Assertions.assertNull(observation.nativeUuid());
    Assertions.assertEquals(schema.toJson(), observation.schemaFingerprint());
    Assertions.assertEquals(2, observation.columns().size());
    Assertions.assertEquals("id", observation.columns().get(0).name());
    Assertions.assertEquals(Types.IntegerType.get(), observation.columns().get(0).dataType());
    Assertions.assertFalse(observation.columns().get(0).nullable());
    Assertions.assertEquals("name", observation.columns().get(1).name());
    Assertions.assertEquals(Types.StringType.get(), observation.columns().get(1).dataType());
    Assertions.assertTrue(observation.columns().get(1).nullable());
    Assertions.assertTrue(result.reason().contains("cannot correlate"));
  }

  @Test
  void testMissingCorruptAndForeignLogsCannotPublish() throws IOException {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, true);
    KernelRegistrationCommitInspector inspector =
        new KernelRegistrationCommitInspector(new Configuration());

    RegistrationCommitVerifier.Result missing =
        inspector.verify(
            reservation(directory.resolve("missing"), expectedSchema.toJson(), null), 0L);
    assertCannotPublish(missing);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.NOT_COMMITTED, missing.kind());
    Assertions.assertNull(missing.observation());
    Assertions.assertTrue(missing.reason().contains("stale writer"));

    Path validPath = directory.resolve("valid-source");
    createDeltaTable(validPath, expectedSchema);
    Path corruptPath = directory.resolve("corrupt-copy");
    FileUtils.copyDirectory(validPath.toFile(), corruptPath.toFile());
    Files.write(
        corruptPath.resolve("_delta_log/00000000000000000000.json"),
        Collections.singletonList("{not valid Delta JSON"),
        UTF_8,
        TRUNCATE_EXISTING);
    RegistrationCommitVerifier.Result corrupt =
        inspector.verify(reservation(corruptPath, expectedSchema.toJson(), null), 0L);
    assertCannotPublish(corrupt);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, corrupt.kind());
    Assertions.assertNull(corrupt.observation());
    Assertions.assertTrue(corrupt.reason().contains("could not be read"));

    StructType foreignSchema = new StructType().add("foreign", StringType.STRING, false);
    Path foreignPath = directory.resolve("foreign");
    createDeltaTable(foreignPath, foreignSchema);
    RegistrationCommitVerifier.Result foreign =
        inspector.verify(reservation(foreignPath, expectedSchema.toJson(), null), 0L);
    assertCannotPublish(foreign);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, foreign.kind());
    RegistrationCommitVerifier.Observation foreignObservation = foreign.observation();
    Assertions.assertNotNull(foreignObservation);
    Assertions.assertEquals(foreignSchema.toJson(), foreignObservation.schemaFingerprint());
    Assertions.assertTrue(foreign.reason().contains("schema does not match"));
  }

  @Test
  void testCandidateVersionCannotOverrideStoredLocation() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER, true);
    createDeltaTable(directory.resolve("candidate-table"), schema);
    KernelRegistrationCommitInspector inspector =
        new KernelRegistrationCommitInspector(new Configuration());

    RegistrationCommitVerifier.Result result =
        inspector.verify(
            reservation(directory.resolve("stored-location"), schema.toJson(), null), 0L);

    assertCannotPublish(result);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.NOT_COMMITTED, result.kind());
    Assertions.assertNull(result.observation());

    Path storedPath = directory.resolve("stored-table");
    createDeltaTable(storedPath, schema);
    RegistrationCommitVerifier.Result unchangedBase =
        inspector.verify(reservation(storedPath, schema.toJson(), 0L), 1L);
    assertCannotPublish(unchangedBase);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, unchangedBase.kind());
    Assertions.assertEquals(0L, unchangedBase.observation().nativeVersion());
    Assertions.assertTrue(unchangedBase.reason().contains("did not advance"));

    RegistrationCommitVerifier.Result unmatchedHint =
        inspector.verify(reservation(storedPath, schema.toJson(), null), 7L);
    assertCannotPublish(unmatchedHint);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, unmatchedHint.kind());
    Assertions.assertEquals(0L, unmatchedHint.observation().nativeVersion());
    Assertions.assertTrue(unmatchedHint.reason().contains("only a search hint"));
  }

  @Test
  void testMalformedStoredEvidenceAndUnsupportedTypesAreAmbiguous() {
    StructType supportedSchema = new StructType().add("id", IntegerType.INTEGER, true);
    Path supportedPath = directory.resolve("supported");
    createDeltaTable(supportedPath, supportedSchema);
    KernelRegistrationCommitInspector inspector =
        new KernelRegistrationCommitInspector(new Configuration());

    TableEntity missingToken = reservation(supportedPath, supportedSchema.toJson(), null);
    missingToken.properties().remove(RegistrationCommand.INTERNAL + "token");
    RegistrationCommitVerifier.Result missingTokenResult = inspector.verify(missingToken, 0L);
    assertCannotPublish(missingTokenResult);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, missingTokenResult.kind());
    Assertions.assertNull(missingTokenResult.observation());
    Assertions.assertTrue(missingTokenResult.reason().contains("Stored registration evidence"));

    TableEntity invalidBase = reservation(supportedPath, supportedSchema.toJson(), null);
    invalidBase.properties().put(RegistrationCommand.INTERNAL + "native.version", "not-a-number");
    RegistrationCommitVerifier.Result invalidBaseResult = inspector.verify(invalidBase, 0L);
    assertCannotPublish(invalidBaseResult);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, invalidBaseResult.kind());
    Assertions.assertNull(invalidBaseResult.observation());
    Assertions.assertTrue(invalidBaseResult.reason().contains("Stored registration evidence"));

    StructType unsupportedSchema = new StructType().add("flag", BooleanType.BOOLEAN, true);
    Path unsupportedPath = directory.resolve("unsupported-type");
    createDeltaTable(unsupportedPath, unsupportedSchema);
    RegistrationCommitVerifier.Result unsupported =
        inspector.verify(reservation(unsupportedPath, unsupportedSchema.toJson(), null), 0L);
    assertCannotPublish(unsupported);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, unsupported.kind());
    Assertions.assertNull(unsupported.observation());
    Assertions.assertTrue(unsupported.reason().contains("unsupported schema type"));
    Assertions.assertTrue(unsupported.reason().contains(BooleanType.class.getName()));
  }

  @Test
  void testReadFailuresAreAmbiguousRatherThanNotCommitted() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER, true);

    InjectedFailureFileSystem.reset();
    RegistrationCommitVerifier.Result accessDenied =
        new KernelRegistrationCommitInspector(failureConfiguration("access-denied"))
            .verify(reservation("failure://registration/table", schema.toJson(), null), 0L);
    assertCannotPublish(accessDenied);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, accessDenied.kind());
    Assertions.assertNull(accessDenied.observation());
    Assertions.assertTrue(accessDenied.reason().contains("could not be read"));
    InjectedFailureFileSystem.assertFailure(AccessControlException.class);

    InjectedFailureFileSystem.reset();
    RegistrationCommitVerifier.Result unsupportedRead =
        new KernelRegistrationCommitInspector(failureConfiguration("unsupported-read"))
            .verify(reservation("failure://registration/table", schema.toJson(), null), 0L);
    assertCannotPublish(unsupportedRead);
    Assertions.assertEquals(RegistrationCommitVerifier.Kind.AMBIGUOUS, unsupportedRead.kind());
    Assertions.assertNull(unsupportedRead.observation());
    Assertions.assertTrue(unsupportedRead.reason().contains("could not be read"));
    Assertions.assertFalse(unsupportedRead.reason().contains("unsupported schema type"));
    InjectedFailureFileSystem.assertFailure(UnsupportedOperationException.class);
  }

  private static void assertLoadedKernelArtifacts() {
    String expectedVersion = System.getProperty("deltaRegistrationKernelVersion");
    Assertions.assertNotNull(expectedVersion, "Gradle must pass the selected Kernel version");
    assertLoadedArtifact(Table.class, "delta-kernel-api", expectedVersion);
    assertLoadedArtifact(DefaultEngine.class, "delta-kernel-defaults", expectedVersion);
  }

  private static void assertLoadedArtifact(
      Class<?> artifactClass, String artifactName, String expectedVersion) {
    Package artifactPackage = artifactClass.getPackage();
    Assertions.assertEquals(expectedVersion, artifactPackage.getImplementationVersion());
    URL source = artifactClass.getProtectionDomain().getCodeSource().getLocation();
    Assertions.assertTrue(
        source.toExternalForm().contains(artifactName + "-" + expectedVersion + ".jar"),
        () -> artifactName + " loaded from unexpected source " + source);
  }

  private static void assertCannotPublish(RegistrationCommitVerifier.Result result) {
    Assertions.assertNotEquals(RegistrationCommitVerifier.Kind.MATCHED, result.kind());
  }

  private static Configuration failureConfiguration(String failure) {
    Configuration configuration = new Configuration();
    configuration.setClass("fs.failure.impl", InjectedFailureFileSystem.class, FileSystem.class);
    configuration.setBoolean("fs.failure.impl.disable.cache", true);
    configuration.set(INJECTED_FAILURE, failure);
    return configuration;
  }

  private static void createDeltaTable(Path path, StructType schema) {
    Configuration configuration = new Configuration();
    Engine engine = DefaultEngine.create(configuration);
    Table nativeTable = Table.forPath(engine, path.toUri().toString());
    nativeTable
        .createTransactionBuilder(engine, "registration-evidence-test", Operation.CREATE_TABLE)
        .withSchema(engine, schema)
        .withPartitionColumns(engine, Collections.emptyList())
        .build(engine)
        .commit(engine, emptyRowIterable());
  }

  private static TableEntity reservation(
      Path location, String schemaFingerprint, Long nativeVersion) {
    return reservation(location.toUri().toString(), schemaFingerprint, nativeVersion);
  }

  private static TableEntity reservation(
      String location, String schemaFingerprint, Long nativeVersion) {
    Map<String, String> properties = new HashMap<>();
    properties.put(RegistrationCommand.INTERNAL + "version", "1");
    properties.put(RegistrationCommand.INTERNAL + "state", "RESERVED");
    properties.put(RegistrationCommand.INTERNAL + "generation", "0");
    properties.put(RegistrationCommand.INTERNAL + "token", "operation-token");
    properties.put(RegistrationCommand.INTERNAL + "actor", "owner");
    properties.put(RegistrationCommand.INTERNAL + "location", location);
    properties.put(RegistrationCommand.INTERNAL + "schemaFingerprint", schemaFingerprint);
    properties.put(RegistrationCommand.INTERNAL + "request", "test-only-request");
    properties.put(RegistrationCommand.INTERNAL + "operation", "RESERVE");
    if (nativeVersion != null) {
      properties.put(RegistrationCommand.INTERNAL + "native.version", nativeVersion.toString());
    }
    return TableEntity.builder()
        .withId(1001L)
        .withName("table")
        .withNamespace(Namespace.of("metalake", "catalog", "schema"))
        .withAuditInfo(
            AuditInfo.builder().withCreator("owner").withCreateTime(Instant.EPOCH).build())
        .withProperties(properties)
        .build();
  }

  private static CloseableIterable<Row> emptyRowIterable() {
    return new CloseableIterable<Row>() {
      /** {@inheritDoc} */
      @Override
      public CloseableIterator<Row> iterator() {
        return new CloseableIterator<Row>() {
          /** {@inheritDoc} */
          @Override
          public void close() {}

          /** {@inheritDoc} */
          @Override
          public boolean hasNext() {
            return false;
          }

          /** {@inheritDoc} */
          @Override
          public Row next() {
            throw new NoSuchElementException("Empty iterator");
          }
        };
      }

      /** {@inheritDoc} */
      @Override
      public void close() {}
    };
  }

  /** Deterministic test filesystem that injects failures at Delta's first metadata read. */
  public static final class InjectedFailureFileSystem extends FileSystem {
    private static final AtomicInteger OPEN_CALLS = new AtomicInteger();
    private static final AtomicInteger GET_FILE_STATUS_CALLS = new AtomicInteger();
    private static final AtomicReference<Class<?>> LAST_FAILURE_CLASS = new AtomicReference<>();
    private static final AtomicReference<String> LAST_FAILURE_MESSAGE = new AtomicReference<>();

    private URI uri;
    private org.apache.hadoop.fs.Path workingDirectory;

    /** {@inheritDoc} */
    @Override
    public void initialize(URI name, Configuration configuration) throws IOException {
      super.initialize(name, configuration);
      uri = URI.create(name.getScheme() + "://" + name.getAuthority());
      workingDirectory = new org.apache.hadoop.fs.Path(uri.toString() + "/");
    }

    /** {@inheritDoc} */
    @Override
    public URI getUri() {
      return uri;
    }

    /** {@inheritDoc} */
    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize)
        throws IOException {
      OPEN_CALLS.incrementAndGet();
      throw new FileNotFoundException("No checkpoint marker in injected filesystem");
    }

    /** {@inheritDoc} */
    @Override
    public FSDataOutputStream create(
        org.apache.hadoop.fs.Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      throw new AssertionError("Read-only evidence inspection must not create files");
    }

    /** {@inheritDoc} */
    @Override
    public FSDataOutputStream append(
        org.apache.hadoop.fs.Path path, int bufferSize, Progressable progress) {
      throw new AssertionError("Read-only evidence inspection must not append files");
    }

    /** {@inheritDoc} */
    @Override
    public boolean rename(org.apache.hadoop.fs.Path source, org.apache.hadoop.fs.Path destination) {
      throw new AssertionError("Read-only evidence inspection must not rename files");
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(org.apache.hadoop.fs.Path path, boolean recursive) {
      throw new AssertionError("Read-only evidence inspection must not delete files");
    }

    /** {@inheritDoc} */
    @Override
    public FileStatus[] listStatus(org.apache.hadoop.fs.Path path) {
      throw new AssertionError("Injected getFileStatus failure must precede directory listing");
    }

    /** {@inheritDoc} */
    @Override
    public void setWorkingDirectory(org.apache.hadoop.fs.Path newDirectory) {
      workingDirectory = newDirectory;
    }

    /** {@inheritDoc} */
    @Override
    public org.apache.hadoop.fs.Path getWorkingDirectory() {
      return workingDirectory;
    }

    /** {@inheritDoc} */
    @Override
    public boolean mkdirs(org.apache.hadoop.fs.Path path, FsPermission permission) {
      throw new AssertionError("Read-only evidence inspection must not create directories");
    }

    /** {@inheritDoc} */
    @Override
    public FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
      GET_FILE_STATUS_CALLS.incrementAndGet();
      if ("access-denied".equals(getConf().get(INJECTED_FAILURE))) {
        return throwFailure(
            new AccessControlException("Injected Delta log read denial at getFileStatus"));
      }
      return throwFailure(
          new UnsupportedOperationException(
              "Injected unsupported Delta log read at getFileStatus"));
    }

    private static void reset() {
      OPEN_CALLS.set(0);
      GET_FILE_STATUS_CALLS.set(0);
      LAST_FAILURE_CLASS.set(null);
      LAST_FAILURE_MESSAGE.set(null);
    }

    private static void assertFailure(Class<?> expectedClass) {
      Assertions.assertTrue(OPEN_CALLS.get() >= 1);
      Assertions.assertEquals(1, GET_FILE_STATUS_CALLS.get());
      Assertions.assertEquals(expectedClass, LAST_FAILURE_CLASS.get());
      Assertions.assertTrue(LAST_FAILURE_MESSAGE.get().contains("getFileStatus"));
    }

    private static <T> T throwFailure(RuntimeException failure) {
      LAST_FAILURE_CLASS.set(failure.getClass());
      LAST_FAILURE_MESSAGE.set(failure.getMessage());
      throw failure;
    }

    private static <T> T throwFailure(IOException failure) throws IOException {
      LAST_FAILURE_CLASS.set(failure.getClass());
      LAST_FAILURE_MESSAGE.set(failure.getMessage());
      throw failure;
    }
  }
}
