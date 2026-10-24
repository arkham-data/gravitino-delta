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

import java.net.{URI, InetSocketAddress}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import com.sun.net.httpserver.{HttpServer, HttpExchange}
import java.util.concurrent.Executors
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/** Installed live session shared by Scala and actual classic PySpark SQL. */
final class DeltaRangerLiveCdfMerge(requestPath: String) extends AutoCloseable {
  private val json = new ObjectMapper()
  private val request = json.readTree(Files.readAllBytes(Paths.get(requestPath)))
  private def text(key: String): String = request.path(key).asText()
  private val output = Files.createDirectory(Paths.get(text("output")))
  private val control = Paths.get(text("controlDirectory"))
  private val manifest = json.readTree(Files.readAllBytes(Paths.get(text("packageManifest"))))
  private val properties = new Properties()
  private val stream = Files.newInputStream(Paths.get(text("sparkConfigurationFile")))
  try properties.load(stream)
  finally stream.close()
  private val gateway = new DeltaRangerLiveCdfMerge.Gateway(
    URI.create(properties.getProperty("spark.sql.gravitino.uri")),
    output
  )
  private val tablePath =
    "/api/metalakes/" + text("metalake") + "/catalogs/" + text("catalog") + "/schemas/" + text(
      "database"
    ) + "/tables/source"
  gateway.observeTarget(tablePath, text("metadataPrincipal"))
  private var security: DeltaRangerDownloadedSecurity = null
  private var spark: SparkSession = null
  security = new DeltaRangerDownloadedSecurity(
    Paths.get(text("sourceSecurityDirectory")),
    Paths.get(text("driverSecurityDirectory")),
    output,
    text("resourceService"),
    true
  )
  private val origins = manifest
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
  private val conf = new SparkConf(false).setMaster("local[2]").setAppName("cdf-merge-live")
  private val hadoop = new Configuration()
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
  private val subject = DeltaRangerQuerySubject.current(spark).user()
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
  private val wrapper = spark.sessionState.catalogManager.catalog(text("catalog"))
  require(wrapper.getClass == classOf[DeltaRangerCatalogSpark35], "Discovered wrapper differs")
  private val engine = wrapper.asInstanceOf[DeltaRangerCatalogSpark35].policyEngine()
  awaitApplied(engine, request.path("minPolicy").asLong(), request.path("minRole").asLong(), 45)
  security.recordInitialDownloads(
    engine.versions().policy(),
    engine.versions().role(),
    request.path("minPolicy").asLong(),
    request.path("minRole").asLong()
  )
  private var cursor = 0L
  private def applied(name: String, statePath: String, initial: Boolean): Unit = {
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

  /** The actual installed JVM session, shared with the classic PySpark frontend. */
  def session(): SparkSession = spark

  /** Begin a fresh consumer checkpoint and wait for the supervisor's native before-state. */
  def before(name: String): String = {
    val step = request.path("steps").elements().asScala.find(_.path("name").asText() == name).get
    writeJson(
      output.resolve(name + "-checkpoint-before.json"),
      Map("sourceId" -> step.path("sourceId").asText(), "end" -> 0L).asJava
    )
    signal(name + "-before")
    if (name == "postHelperRevoked")
      gateway.arm(
        Thread.currentThread(),
        () => {
          signal("post-helper")
        }
      )
    val app = step.path("txnAppId").asText("")
    if (app.nonEmpty) {
      spark.conf.set("spark.databricks.delta.write.txnAppId", app)
      spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
      spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
    } else {
      spark.conf.unset("spark.databricks.delta.write.txnAppId")
      spark.conf.unset("spark.databricks.delta.write.txnVersion")
    }
    require(DeltaRangerQuerySubject.current(spark).user() == text("subject"))
    writeJson(
      output.resolve(name + "-identity.json"),
      Map(
        "subject" -> DeltaRangerQuerySubject.current(spark).user(),
        "ugi" -> UserGroupInformation.getCurrentUser.getShortUserName,
        "thread" -> Thread.currentThread.getName
      ).asJava
    )
    step.path("sql").asText()
  }

  /** Persist consumer progress only after the submitted SQL has returned successfully. */
  def after(name: String, failure: Throwable): Unit = {
    val step = request.path("steps").elements().asScala.find(_.path("name").asText() == name).get
    val causes = Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toVector
    val end = if (failure == null) step.path("end").asLong() else 0L
    writeJson(
      output.resolve(name + "-checkpoint-after.json"),
      Map("sourceId" -> step.path("sourceId").asText(), "end" -> end).asJava
    )
    writeJson(
      output.resolve(name + "-outcome.json"),
      Map[String, Any](
        "outcome" -> (if (failure == null) "success" else "failed"),
        "errors" -> causes.map(_.getClass.getName).asJava,
        "causes" -> causes
          .map(e =>
            Map(
              "class" -> e.getClass.getName,
              "message" -> e.getMessage,
              "stack" -> e.getStackTrace.map(_.toString).toVector.asJava
            ).asJava
          )
          .asJava
      ).asJava
    )
    signal(name + "-after")
  }

  /** Wait for a reviewed transition and prove a completed download before continuing. */
  def signal(name: String): Unit = {
    writeJson(control.resolve(name + "-ready.json"), Map("ready" -> true).asJava)
    val release = control.resolve(name + "-verified.json")
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(240)
    while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(25)
    require(Files.exists(release), "Supervisor checkpoint timed out: " + name)
    val state = json.readTree(Files.readAllBytes(release))
    require(state.path("verified").asBoolean(), "Supervisor rejected")
    if (state.has("stateEvidenceFile"))
      applied(name, state.path("stateEvidenceFile").asText(), false)
  }

  /** Capture governed values through installed SQL, with no payload bridge into MERGE. */
  def observeSource(): Unit = {
    val named = text("catalog") + "." + text("database") + ".source"
    val rows = spark
      .sql(
        "SELECT id, secret, region, _change_type, _commit_version FROM table_changes('" + named + "', 1, 1)"
      )
      .collect()
      .toVector
      .map(_.toSeq.asJava)
      .asJava
    writeJson(output.resolve("source-governed.json"), rows)
  }

  /** Retain actual frontend/runtime/identity evidence before releasing owned resources. */
  override def close(): Unit = {
    try {
      gateway.requireMetadataObserved("metadata-identity-final.json")
      writeJson(
        output.resolve("observed.json"),
        Map(
          "subject" -> DeltaRangerQuerySubject.current(spark).user(),
          "spark" -> spark.version,
          "java" -> System.getProperty("java.specification.version"),
          "scala" -> scala.util.Properties.versionNumberString,
          "ugi" -> UserGroupInformation.getCurrentUser.getShortUserName
        ).asJava
      )
    } finally {
      if (spark != null) spark.stop()
      if (security != null) security.close()
      gateway.close()
    }
  }
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

/** Thin Scala application and transparent metadata gate for original installed MERGE calls. */
object DeltaRangerLiveCdfMerge {

  /** Run the JVM frontend; the separate Python application calls Python spark.sql itself. */
  def main(args: Array[String]): Unit = {
    require(args.length == 1)
    val app = new DeltaRangerLiveCdfMerge(args(0))
    val request = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(args(0))))
    try {
      request.path("steps").elements().asScala.foreach { step =>
        val name = step.path("name").asText()
        val sql = app.before(name)
        if (name == "distinct") app.observeSource()
        var failure: Throwable = null
        try app.session().sql(sql).collect()
        catch { case e: Throwable => failure = e }
        app.after(name, failure)
      }
    } finally app.close()
  }

  private[ranger] def postHelper(stack: Array[StackTraceElement]): Boolean = {
    stack.exists(f =>
      f.getClassName == "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerMergeCommit" && f.getMethodName == "<init>"
    ) &&
    stack.exists(f =>
      f.getClassName == "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerMergeCommand" && f.getMethodName
        .contains("runMerge")
    )
  }

  private final class Gateway(upstream: URI, output: Path) extends AutoCloseable {
    private val json = new ObjectMapper()
    private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
    private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    private val executor = Executors.newCachedThreadPool()
    private var principal = ""
    private var observed = false
    @volatile private var writer: Thread = null
    @volatile private var callback: () => Unit = null
    private val hop = Set(
      "connection",
      "content-length",
      "expect",
      "host",
      "http2-settings",
      "keep-alive",
      "transfer-encoding",
      "upgrade"
    )
    server.createContext("/", (exchange: HttpExchange) => forward(exchange))
    server.setExecutor(executor); server.start()
    def uri: URI = URI.create("http://127.0.0.1:" + server.getAddress.getPort)
    def observeTarget(path: String, subject: String): Unit = { principal = subject }
    def arm(thread: Thread, action: () => Unit): Unit = {
      require(writer == null); writer = thread; callback = action
    }
    def requireMetadataObserved(name: String): Unit = {
      require(observed, "Authenticated metadata request missing")
      json.writeValue(
        output.resolve(name).toFile,
        Map("principal" -> principal, "successfulTargetGet" -> true).asJava
      )
    }
    override def close(): Unit = {
      server.stop(0); executor.shutdownNow();
      require(executor.awaitTermination(15, TimeUnit.SECONDS))
    }
    private def forward(exchange: HttpExchange): Unit = {
      try {
        val metadata =
          exchange.getRequestMethod == "GET" && exchange.getRequestURI.getPath.contains("/tables/")
        if (metadata) {
          val header = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
          require(header.startsWith("Basic "))
          val subject = new String(java.util.Base64.getDecoder.decode(header.substring(6)), "UTF-8")
            .takeWhile(_ != ':')
          require(subject == principal, "Metadata principal differs")
        }
        val current = writer
        if (metadata && current != null && callback != null && postHelper(current.getStackTrace)) {
          val action = callback; callback = null
          json.writeValue(
            output.resolve("post-helper-held.json").toFile,
            Map(
              "stack" -> current.getStackTrace.toVector.map(_.toString).asJava,
              "writerThread" -> current.getId,
              "held" -> true
            ).asJava
          )
          action()
          require(
            current.isAlive && postHelper(current.getStackTrace),
            "Helper phase escaped while held"
          )
        }
        val request = HttpRequest
          .newBuilder(upstream.resolve(exchange.getRequestURI))
          .timeout(Duration.ofSeconds(30))
        exchange.getRequestHeaders.asScala.foreach { case (name, values) =>
          if (!hop(name.toLowerCase)) values.asScala.foreach(v => request.header(name, v))
        }
        val bytes = exchange.getRequestBody.readAllBytes()
        request.method(
          exchange.getRequestMethod,
          if (bytes.isEmpty) HttpRequest.BodyPublishers.noBody()
          else HttpRequest.BodyPublishers.ofByteArray(bytes)
        )
        val response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray())
        response.headers().map().asScala.foreach { case (name, values) =>
          if (!hop(name.toLowerCase))
            values.asScala.foreach(v => exchange.getResponseHeaders.add(name, v))
        }
        exchange.sendResponseHeaders(
          response.statusCode(),
          if (response.body().isEmpty) -1L else response.body().length.toLong
        )
        if (response.body().nonEmpty) exchange.getResponseBody.write(response.body())
        if (metadata && response.statusCode() == 200) observed = true
      } catch {
        case e: Throwable =>
          json.writeValue(
            output.resolve("gateway-failure-" + System.nanoTime() + ".json").toFile,
            Map("class" -> e.getClass.getName, "message" -> e.getMessage).asJava
          )
          try exchange.sendResponseHeaders(503, -1L)
          catch { case _: Throwable => () }
      } finally exchange.close()
    }
  }
}
