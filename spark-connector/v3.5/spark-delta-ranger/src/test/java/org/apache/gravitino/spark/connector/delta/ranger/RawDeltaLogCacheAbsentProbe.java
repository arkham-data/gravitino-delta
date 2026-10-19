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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationCommand.Operation;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationIdentity;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.Evidence;
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaRegistrationReceipt.State;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.delta.DeltaLog$;
import scala.None$;

/** Fresh-JVM fixture for ordinary module absence; no private native writes or sample cache. */
public final class RawDeltaLogCacheAbsentProbe {
  private RawDeltaLogCacheAbsentProbe() {}

  /**
   * Observes a real Reader publication without ever creating a native log or table.
   *
   * @param args result file path
   * @throws Exception if the fixture cannot initialize or the module shape is inaccessible
   */
  public static void main(String[] args) throws Exception {
    SparkSession spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("raw-native-absent")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    TableCatalog tables = mock(TableCatalog.class);
    Table table = mock(Table.class);
    String location = Files.createTempDirectory("raw-native-unloaded").toUri().toString();
    when(client.loadCatalog("a")).thenReturn(catalog);
    when(catalog.name()).thenReturn("a");
    when(catalog.provider()).thenReturn("lakehouse-delta");
    when(catalog.asTableCatalog()).thenReturn(tables);
    when(tables.loadTable(NameIdentifier.of("db", "t"))).thenReturn(table);
    when(table.name()).thenReturn("t");
    when(table.columns())
        .thenReturn(
            new Column[] {
              Column.of("id", Types.LongType.get(), null, true, false, Column.DEFAULT_VALUE_NOT_SET)
            });
    when(table.properties())
        .thenReturn(
            DeltaRegistrationReceipt.of(
                    DeltaRegistrationIdentity.of(1, 1, "private-unloaded-token"),
                    Operation.ADOPT,
                    State.PUBLISHED,
                    "run",
                    Evidence.EXISTING_NATIVE_SNAPSHOT,
                    "NONE",
                    Optional.empty(),
                    Optional.of(location),
                    Optional.of("unloaded-native-id"),
                    OptionalLong.of(0))
                .toProperties());
    try (DeltaRangerMetadataClient metadata =
        new DeltaRangerMetadataClient(
            () -> 0L, mock(ScheduledExecutorService.class), ignored -> client)) {
      metadata.configure("a", Duration.ofMinutes(1), () -> client);
      DeltaRangerRegistrationReader reader =
          new DeltaRangerRegistrationReader(metadata, "lake", "a");
      DeltaRangerRegistrationReader.PublishedObservation publication =
          reader.read(Identifier.of(new String[] {"db"}, "t"));
      Field field = DeltaLog$.class.getDeclaredField("deltaLogCache");
      field.setAccessible(true);
      assertSame(None$.MODULE$, field.get(DeltaLog$.MODULE$));
      int jobs = spark.sparkContext().statusTracker().getJobIdsForGroup(null).length;
      List<AtomicLong> counters = new ArrayList<>();
      List<Long> before = new ArrayList<>();
      for (String name :
          new String[] {
            "org.apache.spark.sql.execution.QueryExecution$",
            "org.apache.spark.sql.execution.SQLExecution$"
          }) {
        Class<?> companion = Class.forName(name);
        Field counter = companion.getDeclaredField("_nextExecutionId");
        counter.setAccessible(true);
        AtomicLong value = (AtomicLong) counter.get(companion.getField("MODULE$").get(null));
        counters.add(value);
        before.add(value.get());
      }
      DeltaRangerRawDeltaLogCache.CacheObservation observation =
          DeltaRangerRawDeltaLogCache.observe(spark, publication);
      assertNull(observation.log());
      observation.requireCurrent(spark);
      assertSame(None$.MODULE$, field.get(DeltaLog$.MODULE$));
      assertEquals(jobs, spark.sparkContext().statusTracker().getJobIdsForGroup(null).length);
      assertTrue(spark.sharedState().cacheManager().isEmpty());
      assertNull(DeltaRangerNativeOperation.current());
      for (int index = 0; index < counters.size(); index++)
        assertEquals(before.get(index).longValue(), counters.get(index).get());
      Files.write(
          Paths.get(args[0]),
          "cacheNoneBefore=true cacheNoneAfter=true logAbsent=true jobsDelta=0 queryExecutionDelta=0 sqlExecutionDelta=0 NativeOperation=absent\n"
              .getBytes(StandardCharsets.UTF_8));
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }
}
