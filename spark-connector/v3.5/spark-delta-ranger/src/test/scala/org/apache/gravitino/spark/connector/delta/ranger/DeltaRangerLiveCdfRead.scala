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

import java.io.FileNotFoundException
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/** Live named CDF consumption through production discovery and actual plugin downloads. */
object DeltaRangerLiveCdfRead {
  private val json = new ObjectMapper()

  /** Run one fresh private request; every row reported here comes from an installed action. */
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Expected one private request")
    val request = json.readTree(Files.readAllBytes(Paths.get(args(0))))
    def text(key: String): String = request.path(key).asText()
    val output = Files.createDirectory(Paths.get(text("output")))
    val control = Paths.get(text("controlDirectory"))
    val manifest = json.readTree(Files.readAllBytes(Paths.get(text("packageManifest"))))
    val properties = new Properties()
    val stream = Files.newInputStream(Paths.get(text("sparkConfigurationFile")))
    try properties.load(stream)
    finally stream.close()
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
      URI.create(properties.getProperty("spark.sql.gravitino.uri")),
      output
    )
    val tablePath =
      "/api/metalakes/" + text("metalake") + "/catalogs/" + text("catalog") + "/schemas/" + text(
        "database"
      ) + "/tables/source"
    gateway.observeTarget(tablePath, text("metadataPrincipal"))
    var security: DeltaRangerDownloadedSecurity = null
    var spark: SparkSession = null
    try {
      security = new DeltaRangerDownloadedSecurity(
        Paths.get(text("sourceSecurityDirectory")),
        Paths.get(text("driverSecurityDirectory")),
        output,
        text("resourceService"),
        true
      )
      val origins = manifest
        .path("providers")
        .fields()
        .asScala
        .map { entry =>
          val cls = Class.forName(entry.getKey, false, getClass.getClassLoader)
          val actual = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
          require(
            actual == Paths
              .get(entry.getValue.asText()) && cls.getClassLoader == getClass.getClassLoader && sha(
              actual
            ) == manifest.path("jarHashes").path(actual.toString).asText(),
            "Runtime origin differs"
          )
          val classStream = cls.getResourceAsStream("/" + cls.getName.replace('.', '/') + ".class")
          val digest = MessageDigest.getInstance("SHA-256")
          try {
            val bytes = new Array[Byte](8192); var n = classStream.read(bytes);
            while (n >= 0) { digest.update(bytes, 0, n); n = classStream.read(bytes) }
          } finally classStream.close()
          entry.getKey -> Map(
            "jar" -> actual.toString,
            "jarSha256" -> sha(actual),
            "classSha256" -> digest.digest().map(b => f"${b & 0xff}%02x").mkString
          ).asJava
        }
        .toMap
      writeJson(output.resolve("origins.json"), origins.asJava)
      val conf = new SparkConf(false).setMaster("local[2]").setAppName("cdf-read-live")
      val hadoop = new Configuration()
      properties.stringPropertyNames().asScala.foreach { key =>
        conf.set(key, properties.getProperty(key));
        if (key.startsWith("spark.hadoop."))
          hadoop.set(key.stripPrefix("spark.hadoop."), properties.getProperty(key))
      }
      conf.set("spark.sql.gravitino.uri", gateway.uri.toString)
      UserGroupInformation.setConfiguration(hadoop)
      spark = SparkSession.builder().config(conf).getOrCreate()
      SparkSession.setActiveSession(spark)
      require(
        spark.version == text(
          "expectedSparkVersion"
        ) && scala.util.Properties.versionNumberString == "2.12.21" && System.getProperty(
          "java.specification.version"
        ) == "17",
        "Runtime pins differ"
      )
      val subject = DeltaRangerQuerySubject.current(spark).user()
      require(
        subject == text("subject") && spark.sparkContext.sparkUser == subject && System.getenv(
          "SPARK_USER"
        ) == subject && System.getenv(
          "HADOOP_USER_NAME"
        ) == subject && UserGroupInformation.getCurrentUser.getShortUserName == subject,
        "Application identity differs"
      )
      require(
        spark.sparkContext.getConf.get(
          "spark.plugins"
        ) == "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin",
        "Production discovery absent"
      )
      val wrapper = spark.sessionState.catalogManager.catalog(text("catalog"))
      require(wrapper.getClass == classOf[DeltaRangerCatalogSpark35], "Discovered wrapper differs")
      val engine = wrapper.asInstanceOf[DeltaRangerCatalogSpark35].policyEngine()
      awaitApplied(engine, request.path("minPolicy").asLong(), request.path("minRole").asLong(), 45)
      security.recordInitialDownloads(
        engine.versions().policy(),
        engine.versions().role(),
        request.path("minPolicy").asLong(),
        request.path("minRole").asLong()
      )
      var cursor = 0L
      def applied(name: String, statePath: String, initial: Boolean): Unit = {
        val state = json.readTree(Files.readAllBytes(Paths.get(statePath)))
        awaitApplied(engine, state.path("policy").asLong(), state.path("role").asLong(), 45)
        val requirement =
          if (initial) DeltaRangerDownloadedSecurity.DownloadRequirement.BOTH
          else DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY
        val evidence = security.completedAfter(cursor, requirement, engine.versions())
        val ranger =
          json.readTree(Files.readAllBytes(Paths.get(state.path("rangerStateFile").asText())))
        verifyDownload(security, evidence.policy().get(), ranger.path("policies"), "policy")
        if (initial) verifyDownload(security, evidence.role().get(), ranger.path("roles"), "role")
        require(
          engine.versions().policy() == state
            .path("policy")
            .asLong() && engine.versions().role() == state.path("role").asLong(),
          "Unreviewed policy generation"
        )
        writeJson(
          output.resolve(name + "-applied.json"),
          Map(
            "policy" -> engine.versions().policy(),
            "role" -> engine.versions().role(),
            "downloadSha256" -> evidence.policy().get().responseSha256(),
            "stateSha256" -> sha(Paths.get(statePath)),
            "sameJvm" -> true
          ).asJava
        )
        cursor = evidence.cursor()
      }
      applied("initial", text("stateEvidenceFile"), true)
      def checkpoint(name: String, policyChange: Boolean = true): Unit = {
        writeJson(control.resolve(name + "-ready.json"), Map("ready" -> true).asJava)
        val release = control.resolve(name + "-verified.json");
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180)
        while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(25)
        require(Files.exists(release), "Supervisor checkpoint timed out")
        val state = json.readTree(Files.readAllBytes(release));
        require(state.path("verified").asBoolean(), "Supervisor rejected")
        if (policyChange) applied(name, state.path("stateEvidenceFile").asText(), false)
      }
      val named = text("catalog") + "." + text("database") + ".source"
      def frame(start: Long = 0, end: Long = 4): DataFrame = spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", end)
        .table(named)
      def collect(value: => DataFrame, label: String): java.util.Map[String, Any] = {
        val rows = value
          .collect()
          .toVector
          .map(r =>
            r.toSeq.map { case t: java.sql.Timestamp => t.toString; case other => other }.asJava
          )
          .asJava
        val result = Map[String, Any]("outcome" -> "success", "rows" -> rows).asJava
        writeJson(output.resolve(label + ".json"), result)
        result
      }
      def denied(
          value: => DataFrame,
          label: String,
          expected: String
      ): java.util.Map[String, Any] = {
        var error: Throwable = null
        try value.collect()
        catch { case e: Throwable => error = e }
        val causes = Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toVector
        require(
          causes.exists(e =>
            e.getClass == classOf[SecurityException] && e.getMessage.contains(expected)
          ),
          "Unexpected CDF failure: " + label
        )
        writeJson(
          output.resolve(label + "-failure.json"),
          causes
            .map(e =>
              Map(
                "class" -> e.getClass.getName,
                "message" -> e.getMessage,
                "stack" -> e.getStackTrace.map(_.toString).toVector.asJava
              ).asJava
            )
            .asJava
        )
        Map[String, Any](
          "outcome" -> (if (label == "incomplete") "failed" else "denied"),
          "rows" -> null,
          "failure" -> causes.map(_.getClass.getName).asJava
        ).asJava
      }
      checkpoint("grant")
      val dataframe = collect(frame(), "dataframe")
      val sql = collect(spark.sql("SELECT * FROM table_changes('" + named + "', 0, 4)"), "sql")
      val empty = withBounds(collect(frame(4, 4), "empty"), 4L, 4L)
      val outOfRange = denied(frame(0, 5), "outOfRange", "CDF")
      writeJson(output.resolve("out-of-range.json"), outOfRange)
      val incompleteName = text("catalog") + "." + text("database") + ".incomplete"
      def faultFrame(): DataFrame = spark.read
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .option("endingVersion", 4)
        .table(incompleteName)
      collect(faultFrame(), "fault-before")
      checkpoint("fault", false)
      var missing: Throwable = null
      try faultFrame().collect()
      catch { case e: Throwable => missing = e }
      val missingCauses = Iterator.iterate(missing)(_.getCause).takeWhile(_ != null).toVector
      require(
        missingCauses.exists(_.isInstanceOf[FileNotFoundException]),
        "Incomplete authorized source did not fail on missing native data"
      )
      val incomplete = Map[String, Any](
        "outcome" -> "failed",
        "bounds" -> Vector(0, 4).asJava,
        "table" -> "incomplete",
        "authorized" -> true,
        "rows" -> null,
        "failure" -> missingCauses.map(_.getClass.getName).asJava
      ).asJava
      writeJson(
        output.resolve("incomplete-failure.json"),
        missingCauses
          .map(e =>
            Map(
              "class" -> e.getClass.getName,
              "stack" -> e.getStackTrace.map(_.toString).toVector.asJava
            ).asJava
          )
          .asJava
      )
      val retainedFrame = frame(); retainedFrame.queryExecution.executedPlan
      checkpoint("revoke")
      val retained = denied(retainedFrame, "retained", "publication changed")
      val freshDenied = denied(frame(), "freshDenied", "access denied")
      checkpoint("regrant")
      val freshGranted = collect(frame(), "freshGranted")
      gateway.requireMetadataObserved("metadata-identity-final.json")
      writeJson(
        output.resolve("observed.json"),
        Map[String, Any](
          "sql" -> sql,
          "dataframe" -> dataframe,
          "empty" -> empty,
          "incomplete" -> incomplete,
          "retained" -> retained,
          "freshDenied" -> freshDenied,
          "freshGranted" -> freshGranted,
          "productionDiscovery" -> true,
          "subject" -> subject,
          "spark" -> spark.version,
          "java" -> System.getProperty("java.specification.version"),
          "scala" -> scala.util.Properties.versionNumberString
        ).asJava
      )
    } finally {
      if (spark != null) spark.stop()
      if (security != null) security.close()
      gateway.close()
    }
  }
  private[ranger] def withBounds(
      value: java.util.Map[String, Any],
      start: Long,
      end: Long
  ): java.util.Map[String, Any] =
    (value.asScala.toMap + ("bounds" -> Vector(start, end).asJava)).asJava

  private def sha(path: Path): String = {
    val hash = MessageDigest.getInstance("SHA-256")
    val stream = Files.newInputStream(path)
    try {
      val bytes = new Array[Byte](1024 * 1024)
      var size = stream.read(bytes)
      while (size >= 0) { hash.update(bytes, 0, size); size = stream.read(bytes) }
    } finally stream.close()
    hash.digest().map(b => f"${b & 0xff}%02x").mkString
  }

  private def awaitApplied(
      engine: DeltaRangerPolicyEngine,
      policy: Long,
      role: Long,
      seconds: Long
  ): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
    var ready = false
    while (!ready && System.nanoTime() < deadline) {
      try { val v = engine.versions(); ready = v.policy() >= policy && v.role() >= role }
      catch { case _: SecurityException => () }
      if (!ready) Thread.sleep(25)
    }
    require(ready, "Actual policy and role application did not converge")
  }

  private def verifyDownload(
      security: DeltaRangerDownloadedSecurity,
      download: DeltaRangerDownloadedSecurity.CompletedDownload,
      expected: JsonNode,
      kind: String
  ): Unit = {
    val events = security
      .startupEvents()
      .asScala
      .map(json.readTree)
      .filter(e => e.has("finished") && e.path("attempt").longValue() == download.attempt())
    require(
      events.size == 1 && events.head.path("downloadKind").asText() == kind && events.head
        .path("downstreamResponseCompleted")
        .asBoolean(),
      "Completed download is missing"
    )
    val path = Paths.get(events.head.path("responseBodyFile").asText())
    require(sha(path) == download.responseSha256(), "Downloaded response bytes differ")
    val actual = json.readTree(Files.readAllBytes(path))
    def stable(node: JsonNode): JsonNode = {
      val copy = node.deepCopy[ObjectNode]()
      copy.remove("policyUpdateTime"); copy.remove("roleUpdateTime"); copy
    }
    require(
      stable(actual) == stable(expected),
      "Real downloaded content differs from reviewed Ranger state"
    )
  }

  private def writeJson(path: Path, value: Any): Unit = {
    require(!Files.exists(path), "Evidence already exists")
    json.writeValue(path.toFile, value)
  }
}
