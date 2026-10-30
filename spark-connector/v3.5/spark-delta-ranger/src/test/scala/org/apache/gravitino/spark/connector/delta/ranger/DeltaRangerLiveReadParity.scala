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
import java.util.concurrent.{Executors, ConcurrentLinkedQueue, ConcurrentHashMap}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.{Map => JavaMap, Properties}
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  AQEShuffleReadExec,
  QueryStageExec,
  ShuffleQueryStageExec,
  BroadcastQueryStageExec
}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

import scala.collection.JavaConverters._

import org.apache.kyuubi.plugin.spark.authz.ranger.SparkRangerAdminPlugin
import org.apache.ranger.plugin.policyengine.{RangerAccessResult, RangerAccessResultProcessor}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** Read-only live observation bridge; payload actions are issued by native classic PySpark. */
final class DeltaRangerLiveReadParity(requestPath: String) extends AutoCloseable {
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
  private val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(
    URI.create(properties.getProperty("spark.sql.gravitino.uri")),
    output
  )
  private val tablePath =
    "/api/metalakes/" + text("metalake") + "/catalogs/" + text("catalog") + "/schemas/" + text(
      "database"
    ) + "/tables/"
  gateway.observeTables(tablePath, text("metadataPrincipal"))
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
  private val conf = new SparkConf(false).setMaster("local[2]").setAppName("read-parity-live")
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
  private val requests = new ConcurrentLinkedQueue[JavaMap[String, Any]]()
  private val queries = new ConcurrentLinkedQueue[QueryExecution]()
  private val callbacks = new ConcurrentLinkedQueue[JavaMap[String, Any]]()
  private var tableRequestsBefore: java.util.List[java.util.Map[String, Any]] = null
  @volatile private var caseSequence = 0L
  @volatile private var activeCase = ""
  private val originalProcessor = SparkRangerAdminPlugin.getResultProcessor
  private val observer = new RangerAccessResultProcessor {
    override def processResult(result: RangerAccessResult): Unit = {
      if (activeCase.nonEmpty && result != null) {
        val request = result.getAccessRequest
        requests.add(
          Map[String, Any](
            "case" -> activeCase,
            "resource" -> new java.util.LinkedHashMap[String, Object](request.getResource.getAsMap),
            "user" -> request.getUser,
            "accessType" -> request.getAccessType,
            "policyType" -> result.getPolicyType,
            "policyId" -> result.getPolicyId,
            "allowed" -> result.getIsAllowed
          ).asJava
        )
      }
      if (originalProcessor != null) originalProcessor.processResult(result)
    }
    override def processResults(results: java.util.Collection[RangerAccessResult]): Unit = {
      // Preserve the batch delegate call exactly once, without changing its input collection.
      if (activeCase.nonEmpty && results != null) results.asScala.foreach { result =>
        if (result != null) {
          val request = result.getAccessRequest
          requests.add(
            Map[String, Any](
              "case" -> activeCase,
              "resource" -> new java.util.LinkedHashMap[String, Object](
                request.getResource.getAsMap
              ),
              "user" -> request.getUser,
              "accessType" -> request.getAccessType,
              "policyType" -> result.getPolicyType,
              "policyId" -> result.getPolicyId,
              "allowed" -> result.getIsAllowed
            ).asJava
          )
        }
      }
      if (originalProcessor != null) originalProcessor.processResults(results)
    }
  }
  // Install only after the production singleton, lease and initial download are ready.
  SparkRangerAdminPlugin.setResultProcessor(observer)
  private val listener = new QueryExecutionListener {
    override def onSuccess(functionName: String, query: QueryExecution, nanos: Long): Unit =
      observeCallback(functionName, query, failed = false)
    override def onFailure(functionName: String, query: QueryExecution, failure: Exception): Unit =
      observeCallback(functionName, query, failed = true)
  }
  spark.listenerManager.register(listener)
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
    val downloadEvent = security
      .startupEvents()
      .asScala
      .map(json.readTree)
      .find(event =>
        event.has("finished") && event.path("attempt").asLong() == evidence.policy().get().attempt()
      )
      .get
    writeJson(
      output.resolve(name + "-applied.json"),
      Map(
        "policy" -> engine.versions().policy(),
        "role" -> engine.versions().role(),
        "downloadSha256" -> evidence.policy().get().responseSha256(),
        "download" -> ref(Paths.get(downloadEvent.path("responseBodyFile").asText())),
        "downloadEvent" -> downloadEvent,
        "state" -> ref(Paths.get(statePath)),
        "rangerState" -> ref(Paths.get(state.path("rangerStateFile").asText())),
        "stateSha256" -> sha(Paths.get(statePath)),
        "sameJvm" -> true
      ).asJava
    )
    cursor = evidence.cursor()
  }
  applied("initial", text("stateEvidenceFile"), true)

  /** Return only the actual installed session, never payload rows. */
  def session(): SparkSession = spark

  /** Await a supervisor-authored policy generation and prove its actual downloaded content. */
  def phase(name: String): Unit = {
    drain()
    activeCase = ""
    writeJson(control.resolve(name + "-ready.json"), Map("ready" -> true).asJava)
    val release = control.resolve(name + "-verified.json")
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180)
    while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(25)
    require(Files.exists(release), "Policy supervisor timed out")
    applied(
      name,
      json.readTree(Files.readAllBytes(release)).path("stateEvidenceFile").asText(),
      false
    )
  }

  /** Begin serial per-case observations after draining prior asynchronous callbacks. */
  def begin(name: String): Unit = {
    drain(); requests.clear(); queries.clear(); callbacks.clear()
    tableRequestsBefore = gateway.namedRequests()
    caseSequence += 1; activeCase = name
    spark.sparkContext.setJobGroup(name, name)
  }

  /** Retain action-owned plans, original occurrence demands and independent Ranger requests. */
  def finish(name: String, frame: Dataset[Row], allowed: Boolean): String = {
    drain()
    require(activeCase == name, "Case observation mismatch")
    activeCase = ""
    spark.sparkContext.clearJobGroup()
    val actionQueries = queries.asScala.toVector.distinct.filter { query =>
      try
        query.analyzed.exists {
          case relation: LogicalRelation =>
            relation.catalogTable.exists(_.provider.contains("delta"))
          case _ => false
        }
      catch { case _: Exception => false }
    }
    require(!allowed || actionQueries.nonEmpty, "Actual action QueryExecution was not observed")
    val selected = actionQueries.lastOption.orElse(Option(frame).map(_.queryExecution))
    val analyzedPath = output.resolve(name + "-analyzed.txt")
    val finalPath = output.resolve(name + "-final.txt")
    val demands = selected.toVector.flatMap { query =>
      val original = query.analyzed
      Files.writeString(analyzedPath, original.toString)
      DeltaRangerReadRule.extract(DeltaRangerOrdinaryAdmission.demandPlan(original), spark).map {
        case (scan, columns) =>
          Map[String, Any](
            "catalog" -> scan.catalog,
            "database" -> scan.database,
            "table" -> scan.table,
            "columns" -> (if (columns.isEmpty) Vector("*") else columns.toVector.sorted).asJava
          ).asJava
      }
    }
    if (!Files.exists(analyzedPath))
      Files.writeString(analyzedPath, "No analyzed frame: lookup rejected")
    var execution: JavaMap[String, Any] = null
    if (allowed) {
      val query = selected.get
      val plan = query.executedPlan
      Files.writeString(finalPath, plan.toString)
      val all = nodes(plan)
      val adaptive = all.collect { case value: AdaptiveSparkPlanExec => value }
      execution = Map[String, Any](
        "phase" -> "ordinary",
        "isPayload" -> true,
        "rootClass" -> plan.getClass.getName,
        "isFinalPlan" -> (adaptive.nonEmpty && adaptive.forall(_.isFinalPlan)),
        "materializedShuffle" -> all.count {
          case stage: ShuffleQueryStageExec => stage.isMaterialized; case _ => false
        },
        "materializedBroadcast" -> all.count {
          case stage: BroadcastQueryStageExec => stage.isMaterialized; case _ => false
        },
        "coalesced" -> all.exists {
          case read: AQEShuffleReadExec => read.hasCoalescedPartition; case _ => false
        },
        "local" -> all.exists {
          case read: AQEShuffleReadExec => read.isLocalRead; case _ => false
        },
        "skew" -> all.exists { case join: SortMergeJoinExec => join.isSkewJoin; case _ => false },
        "reuse" -> all.exists(_.isInstanceOf[ReusedExchangeExec]),
        "operators" -> all.map(_.getClass.getSimpleName).asJava,
        "plan" -> ref(finalPath)
      ).asJava
      // Read existing action receipt tags only; never consume/revalidate/copy/set them.
      val tag =
        TreeNodeTag[DeltaRangerDecisionReceipt.Reference]("gravitino.delta.ranger.decision-receipt")
      val receipts = query.normalized.collect {
        case node if node.getTagValue(tag).nonEmpty => node.getTagValue(tag).get.receipt
      }.distinct
      require(receipts.nonEmpty, "Successful ordinary action has no observable receipt")
      val recorded = receipts.head.obligations.map(o =>
        (o.scan.catalog, o.scan.database, o.scan.table, o.columns)
      )
      val extracted = DeltaRangerReadRule
        .extract(DeltaRangerOrdinaryAdmission.demandPlan(query.analyzed), spark)
        .map { case (scan, columns) => (scan.catalog, scan.database, scan.table, columns) }
      require(
        recorded.toSet == extracted.toSet && recorded.size == extracted.size,
        "Observed original obligations differ"
      )
    } else Files.writeString(finalPath, "Action rejected; no final payload plan claimed")
    val path = output.resolve(name + "-observations.json")
    writeJson(
      path,
      Map[String, Any](
        "tableRequestsBefore" -> tableRequestsBefore,
        "tableRequestsAfter" -> gateway.namedRequests(),
        "originalDemands" -> demands.asJava,
        "rangerRequests" -> requests.asScala.toVector.asJava,
        "requestObservation" -> (if (requests.isEmpty) "no evaluation observed"
                                 else "direct result processor callbacks"),
        "analyzedPlan" -> ref(analyzedPath),
        "finalPlan" -> ref(finalPath),
        "execution" -> execution,
        "actualActionQueries" -> actionQueries.size,
        "caseSequence" -> caseSequence,
        "callbacks" -> callbacks.asScala.toVector.asJava,
        "observedPhase" -> (if (actionQueries.nonEmpty) "action-query" else "before-action"),
        "listenerQueuesDrained" -> true
      ).asJava
    )
    path.toString
  }

  /** Restore the original audit handler and close only this isolated fixture session. */
  override def close(): Unit = {
    try {
      drain(); activeCase = ""
      spark.listenerManager.unregister(listener)
      gateway.requireMetadataObserved("metadata-identity-final.json")
      writeJson(
        output.resolve("observed.json"),
        Map(
          "spark" -> spark.version,
          "java" -> System.getProperty("java.specification.version"),
          "scala" -> scala.util.Properties.versionNumberString,
          "subject" -> DeltaRangerQuerySubject.current(spark).user(),
          "jvmTimezone" -> java.util.TimeZone.getDefault.getID
        ).asJava
      )
    } finally {
      SparkRangerAdminPlugin.setResultProcessor(originalProcessor)
      try spark.stop()
      finally { security.close(); gateway.close() }
    }
  }

  private def observeCallback(
      functionName: String,
      query: QueryExecution,
      failed: Boolean
  ): Unit = {
    val name = activeCase
    if (name.nonEmpty) {
      queries.add(query)
      callbacks.add(
        Map[String, Any](
          "case" -> name,
          "caseSequence" -> caseSequence,
          "function" -> functionName,
          "queryId" -> query.id,
          "failed" -> failed,
          "thread" -> Thread.currentThread().getName
        ).asJava
      )
    }
  }

  private def drain(): Unit = {
    // Reflection exposes the stock diagnostic queue drain, not an authorization/action hook.
    val bus = spark.sparkContext.getClass.getMethod("listenerBus").invoke(spark.sparkContext)
    bus.getClass.getMethod("waitUntilEmpty", java.lang.Long.TYPE).invoke(bus, Long.box(30000L))
  }

  private def ref(path: Path): JavaMap[String, String] =
    Map("path" -> path.toString, "sha256" -> sha(path)).asJava

  private def nodes(plan: SparkPlan): Vector[SparkPlan] = {
    val children = plan match {
      case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
      case stage: QueryStageExec           => Seq(stage.plan)
      case other                           => other.children
    }
    Vector(plan) ++ children.toVector.flatMap(nodes)
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
