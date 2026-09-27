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
package org.apache.gravitino.spark.connector.delta.ranger

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.time.Duration
import java.util.Map
import java.util.concurrent.atomic.AtomicLong
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class TestDeltaRangerStartupProbeNative {
  @TempDir var directory: Path = _

  @Test
  def readyProbeCollectsExactNativeMaskedRows(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val request = DeltaRangerStartupProbe.parse(input("[\"a-one-masked\"],[null]")).orElseThrow()
      val result = DeltaRangerStartupProbe.observe(spark, policy.engine, request)
      assertTrue(result.ready())
      assertTrue(result.queryAttempted())
      assertTrue(result.exactRows())
      assertEquals("ready", result.evidence().get("readiness"))
      assertTrue(result.failure().isEmpty)
    }

  @Test
  def deniedProbeRetainsReadyAndQueryFailureWithoutMarker(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      policy.denied.add("a:secret")
      val result = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe.parse(input("[\"a-one-masked\"],[null]")).orElseThrow()
      )
      assertTrue(result.ready())
      assertTrue(result.queryAttempted())
      assertFalse(result.exactRows())
      assertTrue(result.failure().isPresent)
      assertNull(result.evidence().get("rows"))
      val output = Files.createDirectory(directory.toRealPath().resolve("denied"))
      assertThrows(
        classOf[IllegalStateException],
        () => DeltaRangerStartupProbe.finish(output, result, Map.of(), Map.of("mode", "empty"))
      )
      assertFalse(Files.exists(output.resolve("startup-probe-completed.json")))
    }

  @Test
  def wrongRowsRetainActualMaskedRowsWithoutMarker(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val result = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe.parse(input("[\"wrong\"]")).orElseThrow()
      )
      assertTrue(result.ready())
      assertTrue(result.queryAttempted())
      assertFalse(result.exactRows())
      assertTrue(result.failure().isEmpty)
      assertNotNull(result.evidence().get("rows"))
      val output = Files.createDirectory(directory.toRealPath().resolve("wrong"))
      assertThrows(
        classOf[IllegalStateException],
        () => DeltaRangerStartupProbe.finish(output, result, Map.of(), Map.of("mode", "empty"))
      )
      assertFalse(Files.exists(output.resolve("startup-probe-completed.json")))
    }

  @Test
  def rowComparisonPreservesMultiplicityNullAndIgnoresOrder(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      val reversed = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe.parse(input("[null],[\"a-one-masked\"]")).orElseThrow()
      )
      assertTrue(reversed.exactRows())
      val duplicate = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe
          .parse(
            input("[\"a-one-masked\"],[\"a-one-masked\"]")
          )
          .orElseThrow()
      )
      assertFalse(duplicate.exactRows())
      val stringNull = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe.parse(input("[\"a-one-masked\"],[\"null\"]")).orElseThrow()
      )
      assertFalse(stringNull.exactRows())
    }

  @Test
  def missingRolesNeverRunsNativeQuery(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      org.mockito.Mockito.when(policy.plugin.getRangerRoles()).thenReturn(null)
      val now = new AtomicLong()
      val before = spark.sparkContext.statusTracker.getJobIdsForGroup(null).toList
      val result = DeltaRangerStartupProbe.observe(
        spark,
        policy.engine,
        DeltaRangerStartupProbe.parse(input("[\"a-one-masked\"],[null]")).orElseThrow(),
        () => now.get(),
        millis => now.addAndGet(Duration.ofMillis(millis).toNanos)
      )
      assertFalse(result.ready())
      assertFalse(result.queryAttempted())
      assertEquals("not-ready", result.evidence().get("readiness"))
      assertEquals(before, spark.sparkContext.statusTracker.getJobIdsForGroup(null).toList)
    }

  private def input(rows: String) =
    new ObjectMapper().readTree(
      s"""{"catalogs":["a"],"startupProbe":{"mode":"empty","query":{
         |"catalog":"a","schema":"db","table":"t","columns":["secret"],
         |"expectedRows":[$rows]}}}""".stripMargin
    )
}
