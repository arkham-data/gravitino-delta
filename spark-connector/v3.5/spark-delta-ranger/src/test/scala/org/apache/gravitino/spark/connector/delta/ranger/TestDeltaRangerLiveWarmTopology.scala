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

import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.TimeUnit
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty
import scala.collection.JavaConverters._

/** Local qualification of the exact live observer and first/checkpoint/duplicate sequence. */
@EnabledIfSystemProperty(named = "gravitino.delta.test.liveWarmTopology.enabled", matches = "true")
class TestDeltaRangerLiveWarmTopology {
  private val json = new ObjectMapper()

  @Test def targetFirst(): Unit = scenario("target-first")
  @Test def projectFirst(): Unit = scenario("project-first")

  private def scenario(order: String): Unit = {
    val output = Paths.get(System.getProperty("harness.topology.evidence"), order)
    Files.createDirectory(output)
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    try
      RawNativeObservationFixture.withEnabled(Seq("a")) { (spark, _, policy) =>
        NativeSelectFixture.catalogPolicies = Map("a" -> policy, "b" -> source)
        spark.conf.set("spark.databricks.delta.write.txnAppId", "topology-" + UUID.randomUUID())
        spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
        spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
        val location = NativeSelectFixture.locations("a")
        val observer = new DeltaRangerLiveAppend.WarmObserver(order)
        val (native, frames) =
          DeltaRangerLiveAppend.warmCaches(spark, location, order, output, observer)
        val log = RawNativeObservationFixture.rawIndex(frames.head).deltaLog
        val originalVersion = log.unsafeVolatileSnapshot.version
        def append(): Unit = spark
          .sql("SELECT id, secret, region FROM b.db.t")
          .write
          .mode("append")
          .insertInto("a.db.t")
        append()
        assertEquals(originalVersion + 1, log.unsafeVolatileSnapshot.version)
        observer.record(spark, frames, output.resolve("warm-after-append.json"), "success")
        checkpoint(location, originalVersion + 1, output.resolve("first-checkpoint"))
        append()
        assertEquals(originalVersion + 1, log.unsafeVolatileSnapshot.version)
        observer.record(spark, frames, output.resolve("warm-after-duplicate.json"), "success")
        checkpoint(location, originalVersion + 1, output.resolve("duplicate-checkpoint"))
        val expected = Vector(
          Row(1L, "a-one", "keep"),
          Row(2L, "a-two", "drop"),
          Row(3L, null, "keep"),
          Row(1L, "b-one-masked", "keep"),
          Row(3L, null, "keep")
        )
        def counted(rows: Vector[Row]) =
          rows.groupBy(identity).map { case (row, values) => row -> values.size }
        RawNativeObservationFixture.active(native) {
          val read = native.read.format("delta").load(location)
          val actual = read.collect().toVector
          assertEquals(counted(expected), counted(actual))
          val dependent = read.select("id", "region").collect().toVector
          assertEquals(
            counted(expected.map(row => Row(row.getLong(0), row.getString(2)))),
            counted(dependent)
          )
          val repeated = read.collect().toVector
          assertEquals(counted(expected), counted(repeated))
          assertSame(native, SparkSession.active)
          json.writeValue(
            output.resolve("late-readback.json").toFile,
            Map(
              "targetRows" -> actual.map(_.toSeq.asJava).asJava,
              "dependentRows" -> dependent.map(_.toSeq.asJava).asJava,
              "repeatedTargetRows" -> repeated.map(_.toSeq.asJava).asJava,
              "activeNativeSessionRestored" -> (SparkSession.active eq native)
            ).asJava
          )
        }
        val names = Vector(
          "org.apache.spark.sql.SparkSession",
          "org.apache.spark.sql.delta.DeltaLog",
          "org.apache.ranger.plugin.service.RangerBasePlugin",
          "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerLiveAppend$",
          "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRawCacheGeneration"
        )
        val origins = names.map { name =>
          val cls = Class.forName(name)
          val stream = cls.getClassLoader.getResourceAsStream(name.replace('.', '/') + ".class")
          val bytes =
            try stream.readAllBytes()
            finally stream.close()
          name -> Map(
            "origin" -> cls.getProtectionDomain.getCodeSource.getLocation.toExternalForm,
            "classSha256" -> MessageDigest
              .getInstance("SHA-256")
              .digest(bytes)
              .map(b => f"${b & 0xff}%02x")
              .mkString
          ).asJava
        }.toMap
        json.writeValue(output.resolve("origins.json").toFile, origins.asJava)
        json.writeValue(
          output.resolve("completed.json").toFile,
          Map[String, Any](
            "spark" -> spark.version,
            "delta" -> System.getProperty("harness.topology.delta"),
            "order" -> order,
            "beforeVersion" -> originalVersion,
            "afterVersion" -> (originalVersion + 1),
            "explicitOwnerSealing" -> false,
            "earlyInDriverReadback" -> false,
            "completed" -> true
          ).asJava
        )
      }
    finally source.engine.close()
  }

  private def checkpoint(location: String, version: Long, output: Path): Unit = {
    Files.createDirectory(output)
    val arguments = json
      .readTree(System.getProperty("harness.topology.jvmArgs"))
      .elements()
      .asScala
      .map(_.asText())
      .toVector
    val command =
      Vector(Paths.get(System.getProperty("java.home"), "bin", "java").toString) ++ arguments ++
        Vector(
          "-cp",
          System.getProperty("harness.topology.classpath"),
          "org.apache.gravitino.spark.connector.delta.ranger.LiveWarmExternalReadback",
          location,
          version.toString,
          output.resolve("result.json").toString
        )
    json.writeValue(output.resolve("command.json").toFile, command.asJava)
    val process = new ProcessBuilder(command.asJava)
      .redirectErrorStream(true)
      .redirectOutput(output.resolve("process.private.log").toFile)
      .start()
    val started = System.currentTimeMillis()
    val terminal = process.waitFor(180, TimeUnit.SECONDS)
    if (!terminal) {
      process.destroy();
      if (!process.waitFor(10, TimeUnit.SECONDS)) process.destroyForcibly().waitFor()
    }
    json.writeValue(
      output.resolve("exit.json").toFile,
      Map[String, Any](
        "pid" -> process.pid(),
        "startedMillis" -> started,
        "finishedMillis" -> System.currentTimeMillis(),
        "exitCode" -> process.exitValue(),
        "timedOut" -> !terminal
      ).asJava
    )
    assertTrue(terminal, "Independent checkpoint timed out")
    assertEquals(0, process.exitValue(), "Independent checkpoint failed")
    val value = json.readTree(output.resolve("result.json").toFile)
    assertEquals(version, value.path("version").asLong())
    assertEquals(5, value.path("rows").size())
  }
}

/** Separate local native-only reader; never shares the observed driver or its cache. */
object LiveWarmExternalReadback {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("warm-independent-checkpoint")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.databricks.delta.snapshotPartitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try {
      val rows = spark.read.format("delta").load(args(0)).collect().toVector
      val version = DeltaLog.forTable(spark, args(0)).unsafeVolatileSnapshot.version
      require(version == args(1).toLong)
      new ObjectMapper().writeValue(
        Paths.get(args(2)).toFile,
        Map[String, Any]("version" -> version, "rows" -> rows.map(_.toSeq.asJava).asJava).asJava
      )
    } finally spark.stop()
  }
}
