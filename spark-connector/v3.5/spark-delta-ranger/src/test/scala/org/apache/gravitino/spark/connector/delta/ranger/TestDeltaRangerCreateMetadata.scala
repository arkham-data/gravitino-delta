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
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.gravitino.catalog.lakehouse.delta.registration.DeltaOperationMarker

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, GlobalLimit}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.execution.LogicalRDD
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** No-input structural negatives never submit Spark jobs or confer native authority. */
class TestDeltaRangerCreateMetadata {
  private val fixture = new TestDeltaRangerInstalledSchemaCreateBoundary
  private def empty(spark: SparkSession): LogicalRDD =
    spark
      .createDataFrame(spark.sparkContext.emptyRDD[Row], Action.logSchema)
      .queryExecution
      .analyzed
      .asInstanceOf[LogicalRDD]

  @Test
  def rejectsForeignNonemptyAndUnknownEmptyGraphs(): Unit =
    fixture.withCase("metadata-graphs", installed = false, observed = false) { (spark, _, _) =>
      val original = empty(spark)
      val structural = new DeltaRangerCreateMetadata(spark, () => ())
      assertTrue(structural.contains(original))
      assertFalse(
        DeltaRangerPlanAdmission.nativeShape(original),
        "Structural emptiness grants no operation authority"
      )
      assertThrows(
        classOf[SecurityException],
        () => structural.contains(original.copy()(spark.newSession(), None, None))
      )
      val nonempty = spark.sparkContext.parallelize(Seq(InternalRow.empty), 1).filter(_ => false)
      assertThrows(
        classOf[SecurityException],
        () => structural.contains(original.copy(rdd = nonempty)(spark, None, None))
      )
      var getters = 0
      val unknown = new RDD[InternalRow](spark.sparkContext, Nil) {
        override protected def getPartitions: Array[Partition] = { getters += 1; Array.empty }
        override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
          throw new AssertionError("unknown compute must not execute")
      }
      val wrapped = unknown.map(identity)
      assertThrows(
        classOf[SecurityException],
        () => structural.contains(original.copy(rdd = wrapped)(spark, None, None))
      )
      assertEquals(0, getters, "Unknown root must be rejected before a delegated partition getter")
      val extra = original.rdd.map(identity)
      assertThrows(
        classOf[SecurityException],
        () => structural.contains(original.copy(rdd = extra)(spark, None, None))
      )
    }

  @Test
  def rejectsGraphMutationAndClosedValidation(): Unit =
    fixture.withCase("metadata-mutation", installed = false, observed = false) { (spark, _, _) =>
      val original = empty(spark)
      var live = true
      val structural = new DeltaRangerCreateMetadata(
        spark,
        () => {
          if (!live) throw new SecurityException("foreign transaction or closed owner")
        }
      )
      assertTrue(structural.contains(original))
      val changed = original.copy(output =
        original.output.head.newInstance() +: original.output.tail
      )(spark, None, None)
      assertThrows(classOf[SecurityException], () => structural.contains(changed))
      live = false
      assertThrows(classOf[SecurityException], () => structural.contains(original))
      live = true
      // Mutate through the public JVM API; no private fields, transaction capture or jobs.
      classOf[RDD[_]].getMethod("markCheckpointed").invoke(original.rdd)
      assertThrows(classOf[Exception], () => structural.contains(original))
    }

  @Test
  def rejectsLogMarkerAndVersionDriftBeforePublication(): Unit = {
    import InstalledSchemaCreateBoundaryProbe._
    Seq("marker", "native-id", "version").foreach { drift =>
      var checks = 0
      fixture.withCase(
        "metadata-drift-" + drift,
        installed = true,
        observed = false,
        policySetup = policy =>
          policy.onRequest = _ => {
            checks += 1
            if (checks == 3) {
              val bytes = Files.readAllBytes(trace.log0)
              Files.write(trace.directory.resolve("original-create0.json"), bytes)
              if (drift == "version")
                Files.write(trace.log0.resolveSibling("00000000000000000001.json"), bytes)
              else {
                val actions = Files.readAllLines(trace.log0).asScala.map(mapper.readTree)
                val metadata =
                  actions.find(_.has("metaData")).get.get("metaData").asInstanceOf[ObjectNode]
                if (drift == "marker")
                  metadata
                    .get("configuration")
                    .asInstanceOf[ObjectNode]
                    .put(DeltaOperationMarker.PROPERTY_KEY, "foreign-marker")
                else metadata.put("id", "foreign-native-id")
                write(trace.log0, actions.map(_.toString).mkString("", "\n", "\n"))
              }
            }
          }
      ) { (spark, transport, evidence) =>
        val properties = fixtureMap("tableProperties").toSeq.sorted
          .map { case (key, value) =>
            s"'$key'='$value'"
          }
          .mkString(",")
        val failure = assertThrows(
          classOf[Exception],
          () => {
            spark
              .sql(s"CREATE TABLE a.db.created (id BIGINT) USING delta TBLPROPERTIES ($properties)")
              .collect(); ()
          }
        )
        evidence.failed("expected-drift", failure)
        assertTrue(failure.getMessage.contains("native=CREATE0_CONFIRMED"), failure.toString)
        assertEquals(3, checks)
        assertEquals(Vector("RESERVE"), transport.commands.map(_.operation().name()).toVector)
        assertTrue(Files.exists(evidence.log0))
        assertTrue(DeltaRangerCreateMutation.current.isEmpty)
      }
    }
  }

  @Test
  def admitsOnlyCapturedCreateMetadataConsumers(): Unit =
    fixture.withCase("metadata-consumers", installed = false, observed = false) { (spark, _, _) =>
      val original = empty(spark)
      val structural = new DeltaRangerCreateMetadata(spark, () => ())
      val add = original.output.find(_.name == "add").get
      val filtered = Filter(IsNotNull(add), original)
      val selected = Project(Seq(add), filtered)
      assertTrue(structural.prefix(original))
      assertTrue(structural.prefix(filtered))
      assertTrue(structural.prefix(selected))
      assertFalse(structural.prefix(Filter(Literal(true), original)))
      assertFalse(structural.prefix(Project(Seq(original.output.head), filtered)))
      assertFalse(structural.prefix(GlobalLimit(Literal(1), original)))
      assertFalse(DeltaRangerPlanAdmission.nativeShape(selected))
    }
}
