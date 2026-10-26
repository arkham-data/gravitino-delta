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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.ObjectMapper

/** The live gate must hold only the actual post-helper MERGE publication boundary. */
class TestDeltaRangerLiveCdfMerge {
  @Test
  def executionProfileRequiresStartupConfiguration(): Unit = {
    val request = new ObjectMapper().readTree(
      """{"executionProfile":"adaptive-stock","requestedSettings":{"spark.sql.adaptive.enabled":"true"}}"""
    )
    val conf = new SparkConf(false).set("spark.sql.adaptive.enabled", "false")
    assertThrows(
      classOf[IllegalArgumentException],
      () => DeltaRangerLiveCdfMerge.validateProfile(request, conf)
    )
    conf.set("spark.sql.adaptive.enabled", "true")
    DeltaRangerLiveCdfMerge.validateProfile(request, conf)
  }

  @Test
  def mergeGateRequiresBothNativeCommandAndCommitConstruction(): Unit = {
    val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
    val command = new StackTraceElement(
      prefix + "DeltaRangerMergeCommand",
      "runMerge",
      "DeltaRangerMergeCommand.scala",
      200
    )
    val commit = new StackTraceElement(
      prefix + "DeltaRangerMergeCommit",
      "<init>",
      "DeltaRangerMergeCommit.scala",
      40
    )
    assertTrue(DeltaRangerLiveCdfMerge.postHelper(Array(command, commit)))
    assertFalse(DeltaRangerLiveCdfMerge.postHelper(Array(command)))
    assertFalse(DeltaRangerLiveCdfMerge.postHelper(Array(commit)))
    assertFalse(DeltaRangerLiveCdfMerge.postHelper(Array.empty[StackTraceElement]))
    assertFalse(
      DeltaRangerLiveCdfMerge.postHelper(
        Array(
          command,
          new StackTraceElement(
            prefix + "DeltaRangerMergeCommit",
            "run",
            "DeltaRangerMergeCommit.scala",
            130
          )
        )
      )
    )
  }
  @Test
  def liveKeyMaskUsesExplicitBigintArithmetic(): Unit = {
    new TestDeltaRangerInstalledMerge().withPolicies { (spark, plugin) =>
      val policies = new TestDeltaRangerMergePolicies()
      val sql = """MERGE INTO b.db.t t USING
        |(SELECT * FROM table_changes('a.db.t', 4, 4) WHERE _change_type <> 'update_preimage') s
        |ON t.id=s.id WHEN MATCHED AND s._change_type='delete' THEN DELETE
        |WHEN MATCHED THEN UPDATE SET secret=s.secret, region=s.region
        |WHEN NOT MATCHED AND s._change_type<>'delete' THEN INSERT (id,secret,region) VALUES(s.id,s.secret,s.region)
        |""".stripMargin
      policies.publish(plugin, masks = Map("id" -> "{col} + 100"))
      val rejected = assertThrows(classOf[SecurityException], () => spark.sql(sql).collect())
      assertTrue(rejected.getMessage.contains("Unsupported Ranger mask"))
      val typed = Map("id" -> "{col} + CAST(100 AS BIGINT)")
      policies.publish(plugin, masks = typed, sourceMasks = typed)
      assertEquals(1, spark.sql(sql).collect().length)
      assertEquals(1L, TestDeltaRangerMergeSource.version("b"))
    }
  }
}
