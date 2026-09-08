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

package org.apache.gravitino.catalog.lakehouse.delta.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.apache.hadoop.util.VersionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestDeltaKernel33MetadataAdapter {
  @TempDir Path temp;

  @ParameterizedTest(name = "actual loaded Kernel API/defaults and Hadoop: {0}")
  @MethodSource("actualRuntime")
  void pinsActualRuntimeAndRejectsAllOtherCombinations(String actual) {
    String selected = System.getProperty("deltaKernelVersion");
    assertEquals(selected + " / " + selected + " / Hadoop 3.3.6", actual);
    assertEquals("3.3.6", VersionInfo.getVersion());
    assertEquals(selected, Snapshot.class.getPackage().getImplementationVersion());
    assertEquals(selected, DefaultEngine.class.getPackage().getImplementationVersion());
    DeltaKernel33MetadataAdapter.validateRuntime();
    for (String version : List.of("3.3.0", "3.3.2"))
      DeltaKernel33MetadataAdapter.validateVersions(version, version);
    for (String[] pair :
        new String[][] {
          {null, "3.3.0"},
          {"3.3.0", null},
          {"3.3.0", "3.3.2"},
          {"3.3.1", "3.3.1"},
          {"4.0.0", "4.0.0"}
        })
      assertEquals(
          "UNSUPPORTED_RUNTIME",
          assertThrows(
                  DeltaMetadataException.class,
                  () -> DeltaKernel33MetadataAdapter.validateVersions(pair[0], pair[1]))
              .reason());
  }

  @Test
  void rejectsUnexpectedSnapshotImplementation() {
    Snapshot proxy =
        (Snapshot)
            Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] {Snapshot.class}, (p, m, a) -> null);
    assertEquals(
        "UNSUPPORTED_RUNTIME",
        assertThrows(
                DeltaMetadataException.class,
                () ->
                    DeltaKernel33MetadataAdapter.copy(null, "unused", proxy, OptionalLong.empty()))
            .reason());
  }

  @Test
  void agreesWithPublicSnapshotBeforeDetaching() throws Exception {
    Path table = temp.resolve("native");
    MetadataFixtures.nativeTable(table, Map.of("delta.checkpointInterval", "50"), 0);
    Engine engine = DefaultEngine.create(MetadataFixtures.configuration());
    Snapshot snapshot =
        Table.forPath(engine, MetadataFixtures.location(table)).getLatestSnapshot(engine);
    DeltaSnapshotMetadata actual =
        DeltaKernel33MetadataAdapter.copy(
            engine, MetadataFixtures.location(table), snapshot, OptionalLong.of(0));
    assertEquals(snapshot.getSchema(engine).toJson(), actual.schemaJson());
    assertEquals(snapshot.getPartitionColumnNames(engine), actual.partitionColumns());
    assertEquals(
        "INCONSISTENT_SNAPSHOT",
        assertThrows(
                DeltaMetadataException.class,
                () ->
                    DeltaKernel33MetadataAdapter.copy(
                        engine, "unused", snapshot, OptionalLong.of(1)))
            .reason());
  }

  private static Stream<String> actualRuntime() {
    return Stream.of(
        Snapshot.class.getPackage().getImplementationVersion()
            + " / "
            + DefaultEngine.class.getPackage().getImplementationVersion()
            + " / Hadoop "
            + VersionInfo.getVersion());
  }
}
