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
import scala.collection.JavaConverters._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.apache.spark.sql.delta.DeltaOptions
import java.util.Locale

class TestDeltaRangerAppendOptions {
  @Test
  def finiteOptionsPreserveEverySuppliedStringAndDetachRetainedMap(): Unit =
    NativeSelectFixture.withSpark(spark => {
      val expected = AppendWriterOptionsFixture.options
      val retained = new AppendWriterOptionsFixture.RetainedMap(expected)
      val snapshot = DeltaRangerAppendOptions.validate(spark, retained)
      retained.current = expected.updated("userMetadata", "changed metadata")
      assertEquals(expected, snapshot)
      assertNotSame(retained, snapshot)
      assertTrue(DeltaRangerAppendOptions.transactionOptions.isEmpty)
      (Vector(Map.empty[String, String], expected) ++ expected.toVector.map(Map(_))).foreach {
        options =>
          assertEquals(options, DeltaRangerAppendOptions.validate(spark, options))
      }
      val prior = Locale.getDefault
      try {
        Locale.setDefault(new Locale("tr", "TR"))
        val mixed =
          Map("USERMETADATA" -> "preserved", "MERGESCHEMA" -> "TRUE", "OVERWRITESCHEMA" -> "FaLsE")
        assertEquals(mixed, DeltaRangerAppendOptions.validate(spark, mixed))
      } finally Locale.setDefault(prior)
    })

  @Test
  def unknownNullCollidingAndWriterTransactionOptionsAreRefused(): Unit =
    NativeSelectFixture.withSpark(spark => {
      Seq(
        null,
        Map(null.asInstanceOf[String] -> "true"),
        Map("userMetadata" -> null.asInstanceOf[String]),
        Map("unknown" -> "value"),
        Map("txnAppId" -> "writer-app"),
        Map("txnVersion" -> "1"),
        Map("mergeSchema" -> "true", "MERGESCHEMA" -> "false"),
        Map("userMetadata" -> "one", "USERMETADATA" -> "two")
      ).foreach { options =>
        assertThrows(
          classOf[IllegalArgumentException],
          () => {
            DeltaRangerAppendOptions.validate(spark, options); ()
          }
        )
      }
    })

  @Test
  def bothNativeBooleanAccessorsValidateEagerly(): Unit =
    NativeSelectFixture.withSpark(spark => {
      for (key <- Seq("mergeSchema", "overwriteSchema"); value <- Seq("yes", "", " true ")) {
        val error = assertThrows(
          classOf[IllegalArgumentException],
          () => {
            DeltaRangerAppendOptions.validate(spark, Map(key -> value)); ()
          }
        )
        assertTrue(error.getMessage.contains(key), error.toString)
      }
      spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
      val explicit = Map("mergeSchema" -> "false", "overwriteSchema" -> "false")
      val snapshot = DeltaRangerAppendOptions.validate(spark, explicit)
      assertEquals(explicit, snapshot)
      val native = new DeltaOptions(snapshot, spark.sessionState.conf)
      assertFalse(native.canMergeSchema)
      assertFalse(native.canOverwriteSchema)
    })
}

/** Captured writer strings only; maintenance defaults are deliberately outside this fixture. */
private[ranger] object AppendWriterOptionsFixture {
  final class RetainedMap(var current: Map[String, String])
      extends scala.collection.immutable.Map[String, String] {
    override def get(key: String): Option[String] = current.get(key)
    override def iterator: Iterator[(String, String)] = current.iterator
    override def +[B1 >: String](entry: (String, B1)): scala.collection.immutable.Map[String, B1] =
      current + entry
    override def -(key: String): Map[String, String] = current - key
  }

  def options: Map[String, String] = {
    val input = getClass.getResourceAsStream("/append/spark-hub-options.json")
    val json =
      try new ObjectMapper().readTree(input)
      finally input.close()
    val values = json
      .get("writerOptions")
      .fields()
      .asScala
      .map { entry =>
        assertTrue(entry.getValue.isTextual)
        entry.getKey -> entry.getValue.textValue()
      }
      .toMap
    assertEquals(Set("userMetadata", "mergeSchema", "overwriteSchema"), values.keySet)
    assertEquals("true", values("mergeSchema"))
    assertEquals("false", values("overwriteSchema"))
    assertEquals(json.get("expectedUserMetadata").textValue(), values("userMetadata"))
    assertEquals(
      "{'pipelineExecutionId': '9a91a5ef-842e-4a3e-92ef-c0b581a29d99', 'clusterId': '9a91a5ef-842e-4a3e-92ef-c0b581a29d99', 'comment': 'native-characterization', 'overwriteSchema': False, 'mergeSchema': True}",
      values("userMetadata")
    )
    values
  }
}
