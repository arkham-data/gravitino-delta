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
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises read-only registration evidence against actual local Delta transaction logs. */
class TestDeltaRegistrationCommitEvidence {
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
    Map<String, String> properties = new HashMap<>();
    properties.put(RegistrationCommand.INTERNAL + "version", "1");
    properties.put(RegistrationCommand.INTERNAL + "state", "RESERVED");
    properties.put(RegistrationCommand.INTERNAL + "generation", "0");
    properties.put(RegistrationCommand.INTERNAL + "token", "operation-token");
    properties.put(RegistrationCommand.INTERNAL + "actor", "owner");
    properties.put(RegistrationCommand.INTERNAL + "location", location.toUri().toString());
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
}
