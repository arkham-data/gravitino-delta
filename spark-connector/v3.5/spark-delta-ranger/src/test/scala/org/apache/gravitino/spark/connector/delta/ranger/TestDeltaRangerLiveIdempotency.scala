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

import java.nio.file.Files
import java.util.UUID
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation}
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions._

class TestDeltaRangerLiveIdempotency {
  private def input = new ObjectMapper().readTree("""{
    "sourceSecurityDirectory":"/tmp/source", "driverSecurityDirectory":"/tmp/driver",
    "sparkConfigurationFile":"/tmp/spark.properties", "resourceService":"service",
    "resourceDefinition":"definition", "metalake":"lake", "subject":"reader",
    "metadataPrincipal":"metadata", "minPolicy":1, "minRole":0,
    "catalog":"catalog", "database":"db", "transactionAppId":"app", "transactionVersion":1,
    "expectedSparkVersion":"3.5.4", "expectedDeltaVersion":"3.3.0",
    "tables":{"duplicate":"duplicate", "newApplication":"new_app",
      "higherVersion":"higher", "denied":"denied"}}
  """)

  @Test def validatesFiniteRequest(): Unit = {
    val request = DeltaRangerLiveIdempotency.parse(input)
    assertEquals(1L, request.version)
    assertEquals(4, request.tables.size)
  }

  @Test def rejectsMalformedAndMissingFields(): Unit = {
    for (field <- Seq("subject", "transactionAppId", "catalog", "tables")) {
      val value = input.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      value.remove(field)
      assertThrows(classOf[IllegalArgumentException], () => DeltaRangerLiveIdempotency.parse(value))
    }
    for (version <- Seq("-1", "9223372036854775807", "9223372036854775808", "1.5", "\"1\"")) {
      val value = input.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      value.set("transactionVersion", new ObjectMapper().readTree(version))
      assertThrows(classOf[IllegalArgumentException], () => DeltaRangerLiveIdempotency.parse(value))
    }
  }

  @Test def rejectsUnsupportedPinsAndOverlappingTables(): Unit = {
    val value = input.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    value.put("expectedDeltaVersion", "3.2.0")
    assertThrows(classOf[IllegalArgumentException], () => DeltaRangerLiveIdempotency.parse(value))
    val reused = input.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
    reused.withObject("tables").put("denied", "duplicate")
    assertThrows(classOf[IllegalArgumentException], () => DeltaRangerLiveIdempotency.parse(reused))
  }

  @Test def rejectsOutputReuseAndUnexpectedRuntime(): Unit = {
    val existing = Files.createTempDirectory("live-idempotency-test")
    try
      assertThrows(
        classOf[IllegalArgumentException],
        () => DeltaRangerLiveIdempotency.newOutput(existing)
      )
    finally Files.delete(existing)
    val request = DeltaRangerLiveIdempotency.parse(input)
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveIdempotency.validateRuntime(request, "3.5.6", "3.3.0", "17", "2.12.18")
    )
    DeltaRangerLiveIdempotency.validateRuntime(request, "3.5.4", "3.3.0", "17", "2.12.18")
  }

  @Test def denialMustHaveExactProductionOrigin(): Unit = {
    val spoof = new SecurityException("Delta Ranger access denied")
    assertFalse(DeltaRangerLiveIdempotency.exactDenial(spoof))
    spoof.setStackTrace(
      Array(
        new StackTraceElement(
          classOf[DeltaRangerPolicyEngine].getName,
          "authorize",
          "DeltaRangerPolicyEngine.java",
          120
        )
      )
    )
    assertTrue(DeltaRangerLiveIdempotency.exactDenial(new RuntimeException(spoof)))
    assertFalse(DeltaRangerLiveIdempotency.exactDenial(new SecurityException("unrelated")))
  }

  @Test def matchingShapeCannotCompleteAnUnrelatedNativeAggregate(): Unit = {
    val native = new DeltaRangerLiveIdempotency.NativeCompletions
    val owned = aggregate()
    native.observe(owned)
    var physicalRead = false
    assertFalse(native.complete(aggregate(), { physicalRead = true; "unrelated" }))
    assertFalse(physicalRead)
    assertTrue(native.completions.isEmpty)
    assertEquals(1L, native.completed.getCount)
    assertTrue(native.complete(owned.copy(), "owned-physical"))
    assertEquals(0L, native.completed.getCount)
    assertEquals(1, native.completions.size())
  }

  @Test def delayedPreviousCaseCallbackCannotCompleteCurrentCase(): Unit = {
    val previous = new DeltaRangerLiveIdempotency.NativeCompletions
    val current = new DeltaRangerLiveIdempotency.NativeCompletions
    val previousPlan = aggregate()
    val currentPlan = aggregate()
    previous.observe(previousPlan)
    current.observe(currentPlan)
    assertTrue(previous.complete(previousPlan, "previous-physical"))
    assertFalse(current.complete(previousPlan, "delayed-previous-physical"))
    assertTrue(current.completions.isEmpty)
    assertEquals(1L, current.completed.getCount)
    assertTrue(current.complete(currentPlan, "current-physical"))
  }

  @Test def expressionIdentityIncludesJvmIdentityAndRequiresObservation(): Unit = {
    val native = new DeltaRangerLiveIdempotency.NativeCompletions
    val owned = aggregate()
    assertFalse(native.complete(owned, "unobserved"))
    native.observe(owned)
    val differentJvm = owned.copy(aggregateExpressions = owned.aggregateExpressions.map { value =>
      Alias(Literal(1), value.name)(exprId = ExprId(value.exprId.id, UUID.randomUUID()))
    })
    assertFalse(native.complete(differentJvm, "different-jvm"))
    assertEquals(1L, native.completed.getCount)
  }

  private def aggregate(): Aggregate = Aggregate(
    Nil,
    NativeIdempotencyProbe.ExpectedAliases.toVector.sorted.map(name => Alias(Literal(1), name)()),
    LocalRelation()
  )

}
