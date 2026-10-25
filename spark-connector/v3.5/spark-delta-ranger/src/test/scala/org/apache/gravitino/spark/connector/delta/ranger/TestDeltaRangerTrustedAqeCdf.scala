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

import org.apache.spark.sql.Row
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Bounded CDF keeps its native history and captured policy recipe under stock AQE. */
class TestDeltaRangerTrustedAqeCdf {
  @Test
  def standaloneBoundedCdfUsesCapturedPolicies(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = "region = 'US'"
    policy.maskColumn = "id"
    policy.mask = "CAST(0 AS BIGINT)"
    val fixture = new DeltaRangerCdfReadFixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          PartitionedMergeFixture.origins(spark)
          val expected = fixture.expected
            .filter(_.getString(2) == "US")
            .map(row => Row.fromSeq(0L +: row.toSeq.tail))
          val frame = fixture.read(spark)
          assertEquals(fixture.multiset(expected), fixture.multiset(frame.collect().toVector))
          assertEquals(fixture.multiset(expected), fixture.multiset(frame.collect().toVector))
          val singleVersion = fixture.start + 1
          val singleExpected = expected.filter(_.getLong(4) == singleVersion)
          assertFalse(singleExpected.isEmpty)
          assertEquals(
            fixture.multiset(singleExpected),
            fixture.multiset(
              DeltaRangerCdfReadFixture.read(spark, singleVersion, singleVersion).collect().toVector
            )
          )
          assertTrue(fixture.expected.exists(_.getLong(4) == fixture.end))
          assertTrue(spark.sessionState.conf.adaptiveExecutionEnabled)
          org.slf4j.LoggerFactory
            .getLogger(getClass)
            .warn("TRUSTED_CDF_EXCHANGE_FREE\n" + frame.queryExecution.executedPlan)
          assertFalse(
            DeltaRangerTrustedAqeFixture
              .nodes(frame.queryExecution.executedPlan)
              .exists(_.isInstanceOf[org.apache.spark.sql.execution.adaptive.QueryStageExec])
          )
          assertThrows(
            classOf[Exception],
            () => DeltaRangerCdfReadFixture.read(spark, 0L, fixture.end).collect()
          )
          val receipt =
            frame.queryExecution.optimizedPlan.asInstanceOf[DeltaRangerCdfRead.Carrier].receipt
          def field[A](owner: AnyRef, name: String): A = {
            val member = owner.getClass.getDeclaredFields.find(_.getName == name).get
            member.setAccessible(true); member.get(owner).asInstanceOf[A]
          }
          val scans = field[DeltaRangerCdfReadScans](receipt, "scans")
          val required =
            field[scala.collection.mutable.Map[String, (Long, Long)]](scans, "requiredFiles")
          assertFalse(required.isEmpty)
          val path = java.nio.file.Paths.get(new java.net.URI(required.keys.head))
          val removed = path.resolveSibling(path.getFileName.toString + ".missing")
          java.nio.file.Files.move(path, removed)
          try assertThrows(classOf[Exception], () => frame.collect())
          finally java.nio.file.Files.move(removed, path)
        },
        Some(NativeSelectFixture.extension(policy)),
        startupOptions = Map("spark.sql.adaptive.enabled" -> "true"),
        beforeExtension = fixture.prepare
      )
    finally policy.engine.close()
  }
  @Test
  def capturedNativeManifestCallbackUsesPhaseRecipe(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    policy.filter = null; policy.maskColumn = "unused"
    val fixture = new DeltaRangerCdfReadFixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          val frame = fixture.read(spark)
          assertEquals(
            fixture.multiset(fixture.expected),
            fixture.multiset(frame.collect().toVector)
          )
          val indices =
            DeltaRangerTrustedAqeFixture.nodes(frame.queryExecution.executedPlan).collect {
              case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
                scan.relation.location.asInstanceOf[DeltaRangerCdfReadScans.OwnedIndex]
            }
          assertFalse(indices.isEmpty)
          val workers = java.util.concurrent.Executors.newSingleThreadExecutor()
          try {
            policy.denied.add("a:secret")
            val result = workers.submit(new java.util.concurrent.Callable[Boolean] {
              override def call(): Boolean = indices.forall(
                _.matchingFiles(
                  Seq(org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral),
                  Nil
                ).isEmpty
              )
            })
            assertTrue(result.get(30, java.util.concurrent.TimeUnit.SECONDS))
            assertThrows(classOf[Exception], () => frame.collect())
          } finally workers.shutdownNow()
        },
        Some(NativeSelectFixture.extension(policy)),
        startupOptions = Map("spark.sql.adaptive.enabled" -> "true"),
        beforeExtension = fixture.prepare
      )
    finally policy.engine.close()
  }

}
