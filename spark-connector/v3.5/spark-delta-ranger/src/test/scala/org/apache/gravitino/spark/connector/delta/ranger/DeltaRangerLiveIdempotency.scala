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
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import scala.collection.JavaConverters._

/** Finite, test-only live qualification of the native scalar idempotency helper. */
object DeltaRangerLiveIdempotency {
  private val json = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  private val cases = Vector("duplicate", "newApplication", "higherVersion", "denied")
  private val reset = "spark.databricks.delta.write.txnVersion.autoReset.enabled"
  private val aliases = Set(
    "sizeInBytes",
    "numOfSetTransactions",
    "numOfFiles",
    "numOfRemoves",
    "numDeletedRecordsOpt",
    "numDeletionVectorsOpt",
    "numOfMetadata",
    "numOfProtocol",
    "setTransactions",
    "domainMetadata",
    "metadata",
    "protocol",
    "fileSizeHistogram",
    "deletedRecordCountsHistogramOpt"
  )

  private[ranger] final case class Request(
      input: JsonNode,
      tables: Map[String, String],
      version: Long
  ) {
    def text(name: String): String = input.get(name).asText()
    def number(name: String): Long = input.get(name).longValue()
  }

  private[ranger] def parse(input: JsonNode): Request = {
    require(input != null && input.isObject, "Request must be an object")
    val names = Vector(
      "sourceSecurityDirectory",
      "driverSecurityDirectory",
      "sparkConfigurationFile",
      "resourceService",
      "resourceDefinition",
      "metalake",
      "subject",
      "metadataPrincipal",
      "catalog",
      "database",
      "transactionAppId",
      "expectedSparkVersion",
      "expectedDeltaVersion"
    )
    names.foreach { name =>
      val value = input.path(name)
      require(
        value.isTextual && value.asText().nonEmpty && value.asText() == value.asText().trim,
        "Missing or invalid request field: " + name
      )
    }
    Vector("transactionVersion", "minPolicy", "minRole").foreach { name =>
      val value = input.path(name)
      require(
        value.isIntegralNumber && value.canConvertToLong && value.longValue() >= 0,
        "Invalid nonnegative version: " + name
      )
    }
    require(
      input.path("transactionVersion").longValue() < Long.MaxValue,
      "Version cannot be incremented"
    )
    require(
      Set("3.5.4", "3.5.6")(input.path("expectedSparkVersion").asText()),
      "Unsupported Spark pin"
    )
    require(
      Set("3.3.0", "3.3.2")(input.path("expectedDeltaVersion").asText()),
      "Unsupported Delta pin"
    )
    require(
      input.path("subject").asText() != input.path("metadataPrincipal").asText(),
      "Identities must differ"
    )
    val tableInput = input.path("tables")
    require(
      tableInput.isObject && tableInput.fieldNames().asScala.toSet == cases.toSet,
      "Four tables required"
    )
    val tables = cases.map { key =>
      val name = tableInput.path(key)
      require(
        name.isTextual && name.asText().matches("[A-Za-z_][A-Za-z0-9_]*"),
        "Invalid table name"
      )
      key -> name.asText()
    }.toMap
    require(tables.values.toSet.size == 4, "Each case requires an independent table")
    Request(input.deepCopy[JsonNode](), tables, input.path("transactionVersion").longValue())
  }

  private[ranger] def newOutput(path: Path): Path = {
    require(!Files.exists(path) && !Files.isSymbolicLink(path), "Output must be new")
    Files.createDirectory(
      path,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
  }

  private[ranger] def validateRuntime(
      request: Request,
      spark: String,
      delta: String,
      java: String,
      scala: String
  ): Unit = {
    require(
      spark == request.text("expectedSparkVersion"),
      "Actual Spark differs from requested pin"
    )
    require(
      delta == request.text("expectedDeltaVersion"),
      "Actual Delta differs from requested pin"
    )
    require(java == "17" && scala.startsWith("2.12."), "JDK17 and Scala2.12 required")
  }

  private[ranger] def exactDenial(error: Throwable): Boolean =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).exists { cause =>
      val stack = cause.getStackTrace
      cause.getClass == classOf[
        SecurityException
      ] && cause.getMessage == "Delta Ranger access denied" &&
      stack.nonEmpty && stack.head.getClassName == classOf[DeltaRangerPolicyEngine].getName &&
      stack.head.getMethodName == "authorize"
    }

  /** Runs exactly the three scalar outcomes and the denied duplicate; failures exit nonzero. */
  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Expected private request JSON and a new output directory")
    val request = parse(json.readTree(Files.readAllBytes(Paths.get(args(0)))))
    val output = newOutput(Paths.get(args(1)))
    val security = new DeltaRangerDownloadedSecurity(
      Paths.get(request.text("sourceSecurityDirectory")),
      Paths.get(request.text("driverSecurityDirectory")),
      output,
      request.text("resourceService")
    )
    var spark: SparkSession = null
    var pending: Throwable = null
    try {
      val observation = new Observation
      spark = openSpark(request, observation)
      SparkSession.setActiveSession(spark)
      val deltaOrigin = origin(classOf[DeltaTableV2])
      val delta = "delta-spark_2\\.12-(3\\.3\\.[02])\\.jar".r
        .findFirstMatchIn(deltaOrigin)
        .map(_.group(1))
        .getOrElse(throw new IllegalArgumentException("Unidentified Delta artifact"))
      validateRuntime(
        request,
        spark.version,
        delta,
        System.getProperty("java.specification.version"),
        scala.util.Properties.versionNumberString
      )
      check(
        spark.sparkContext.getConf
          .get("spark.plugins", "")
          .split(",")
          .contains(
            "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin"
          ),
        "Production Gravitino discovery plugin is required"
      )
      val origins = DeltaRangerPolicyEngine.runtimeOrigins().asScala.toMap ++ Vector(
        classOf[SparkSession],
        classOf[DeltaTableV2],
        classOf[DeltaRangerCatalogSpark35],
        classOf[DeltaRangerMetadataClient],
        DeltaRangerNativeIdempotency.getClass,
        Class.forName("org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin")
      ).map(c => c.getName -> origin(c))
      val rangerOrigins = origins.filter(_._1.startsWith("org.apache.ranger."))
      check(
        rangerOrigins.nonEmpty && rangerOrigins.values.forall(_.contains("-2.8.0.jar")),
        "Ranger artifact pin mismatch"
      )
      val subject = DeltaRangerQuerySubject.current(spark)
      check(
        subject.user() == request.text("subject") && spark.sparkContext.sparkUser == subject.user(),
        "Actual UGI mismatch"
      )
      check(
        subject.user() != request.text("metadataPrincipal"),
        "Metadata principal became query subject"
      )
      val config = DeltaRangerConfig.fromClasspath(
        request.text("resourceService"),
        request.text("resourceDefinition"),
        request.text("metalake")
      )
      val engine = DeltaRangerPolicyEngine.acquire(config)
      try {
        awaitVersions(engine, request)
        security.recordInitialDownloads(
          engine.versions().policy(),
          engine.versions().role(),
          request.number("minPolicy"),
          request.number("minRole")
        )
        write(
          output.resolve("runtime.json"),
          Map(
            "spark" -> spark.version,
            "delta" -> delta,
            "java" -> System.getProperty("java.version"),
            "scala" -> scala.util.Properties.versionNumberString,
            "origins" -> origins.asJava,
            "subject" -> subject.user(),
            "policyVersion" -> engine.versions().policy(),
            "roleVersion" -> engine.versions().role()
          )
        )
        val wrapper = spark.sessionState.catalogManager.catalog(request.text("catalog"))
        check(
          wrapper.getClass == classOf[DeltaRangerCatalogSpark35],
          "Discovery did not install production wrapper"
        )
        val catalog = wrapper.asInstanceOf[DeltaRangerCatalogSpark35]
        spark.conf.set(reset, "false")
        spark.listenerManager.register(observation.listener)
        val locations = scala.collection.mutable.Set.empty[String]
        cases.foreach { name =>
          val table = catalog
            .loadTable(Identifier.of(Array(request.text("database")), request.tables(name)))
            .asInstanceOf[DeltaTableV2]
          val target = catalog.registrationBindings().resolveAppendTarget(table)
          check(locations.add(target.observation().location()), "Case locations overlap")
          runCase(spark, request, name, target, observation, output)
        }
        write(
          output.resolve("completed.json"),
          Map(
            "completed" -> true,
            "cases" -> cases.asJava,
            "scope" -> "native scalar only; no append/reset=true/publication refresh qualification"
          )
        )
      } finally engine.close()
    } catch {
      case error: Throwable =>
        pending = error
        write(
          output.resolve("failure.json"),
          Map(
            "failureClasses" -> Iterator
              .iterate(error)(_.getCause)
              .takeWhile(_ != null)
              .map(_.getClass.getName)
              .toVector
              .asJava
          )
        )
        throw error
    } finally {
      try { if (spark != null) DeltaRangerDownloadedQuery.stopSpark(spark, pending) }
      finally security.close()
    }
  }

  private[ranger] final class NativeCompletions {
    private val observed = new ConcurrentLinkedQueue[Vector[ExprId]]()
    val completions = new ConcurrentLinkedQueue[String]()
    val completed = new CountDownLatch(1)

    def observe(plan: LogicalPlan): Unit =
      stateAggregates(plan).foreach { aggregate =>
        val identity = aggregate.output.map(_.exprId).toVector
        if (!observed.contains(identity)) observed.add(identity)
      }

    def identities: Vector[Vector[String]] = observed.asScala.toVector.map(
      _.map(id => s"${id.id}@${id.jvmId}")
    )

    def complete(plan: LogicalPlan, physical: => String): Boolean = {
      val matching = stateAggregates(plan).exists(aggregate =>
        observed.contains(aggregate.output.map(_.exprId).toVector)
      )
      if (!matching) return false
      completions.add("logical=" + plan.treeString + "\nphysical=" + physical)
      completed.countDown()
      true
    }
  }

  private final class CaseObservation(val snapshot: Snapshot) {
    val aggregateCount = new java.util.concurrent.atomic.AtomicInteger()
    val native = new NativeCompletions
    val completions = native.completions
    val failures = new ConcurrentLinkedQueue[Throwable]()
    val completed = native.completed
    val seenTransaction = new java.util.concurrent.atomic.AtomicBoolean()
  }

  private final class Observation {
    @volatile var current: CaseObservation = null
    val listener: QueryExecutionListener = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, duration: Long): Unit = {
        val c = current
        if (c != null) try {
          c.native.complete(qe.analyzed, qe.executedPlan.treeString)
        } catch { case error: Throwable => c.failures.add(error); c.completed.countDown() }
      }
      override def onFailure(name: String, qe: QueryExecution, error: Exception): Unit = {
        val c = current
        if (c != null) { c.failures.add(error); c.completed.countDown() }
      }
    }
    def rule: Rule[LogicalPlan] = new Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = {
        val c = current
        val operation = DeltaRangerNativeOperation.current()
        if (c != null && operation != null && operation.transactionState()) {
          val txn = OptimisticTransaction.getActive.getOrElse(
            throw new AssertionError("Native operation without active transaction")
          )
          check(txn.snapshot eq c.snapshot, "Native transaction selected another snapshot")
          check(!computed(txn.snapshot), "Native aggregate entered with warm state")
          c.seenTransaction.set(true)
          if (stateAggregate(plan)) {
            c.native.observe(plan)
            c.aggregateCount.incrementAndGet()
          }
        }
        plan
      }
    }
  }

  private def runCase(
      spark: SparkSession,
      request: Request,
      name: String,
      target: DeltaRangerRegistrationBindings.BoundAppendTarget,
      observation: Observation,
      output: Path
  ): Unit = {
    val directory = newOutput(output.resolve(name))
    val published = target.observation()
    check(published.receipt().state().toString == "PUBLISHED", "Registration is not published")
    val location = new HadoopPath(published.location())
    check(Set("s3a", "s3")(location.toUri.getScheme), "Live qualification requires S3")
    val snapshot = target.log().unsafeVolatileSnapshot
    check(!computed(snapshot), "Target snapshot was warm before scalar")
    val before = inventory(location, spark, newOutput(directory.resolve("before")))
    write(directory.resolve("before-manifest.json"), before.asJava)
    val conf = spark.sessionState.conf.getAllConfs
    val c = new CaseObservation(snapshot)
    observation.current = c
    var result: Option[Boolean] = None
    var denied = false
    try {
      val app = request.text("transactionAppId") + (if (name == "newApplication") "-new-application"
                                                    else "")
      val version = request.version + (if (name == "higherVersion") 1L else 0L)
      try
        result = Some(
          DeltaRangerNativeIdempotency.hasBeenExecuted(
            spark,
            target,
            Map("txnAppId" -> app, "txnVersion" -> version.toString)
          )
        )
      catch { case error: Throwable if name == "denied" && exactDenial(error) => denied = true }
      if (name == "denied") {
        check(denied && result.isEmpty, "Denied duplicate returned a scalar")
        check(
          !computed(snapshot) && !c.seenTransaction.get() && c.aggregateCount
            .get() == 0 && c.completions.isEmpty,
          "Denied duplicate reached native state"
        )
      } else {
        check(result.contains(name == "duplicate"), "Unexpected native scalar outcome: " + name)
        check(c.completed.await(30, TimeUnit.SECONDS), "Native aggregate completion missing")
        check(
          c.seenTransaction
            .get() && c.aggregateCount.get() > 0 && !c.completions.isEmpty && computed(snapshot),
          "Cold native aggregate evidence missing"
        )
      }
      check(c.failures.isEmpty, "Native query listener failed")
    } finally {
      observation.current = null
      val after = inventory(location, spark, newOutput(directory.resolve("after")))
      write(directory.resolve("after-manifest.json"), after.asJava)
      check(before == after, "S3 file bytes changed")
      check(conf == spark.sessionState.conf.getAllConfs, "SQLConf changed")
      check(
        OptimisticTransaction.getActive.isEmpty && DeltaRangerNativeOperation.current() == null,
        "Native scope leaked"
      )
      write(
        directory.resolve("result.json"),
        Map(
          "case" -> name,
          "result" -> result.map(Boolean.box).orNull,
          "denied" -> denied,
          "denialCause" -> (if (denied) "Delta Ranger access denied" else null),
          "registrationId" -> published.identity().id(),
          "registrationGeneration" -> published.identity().generation(),
          "registrationState" -> published.receipt().state().toString,
          "nativeId" -> published.nativeId(),
          "nativeVersion" -> snapshot.version,
          "publishedNativeVersion" -> published.nativeVersion(),
          "transactionSnapshotObserved" -> c.seenTransaction.get(),
          "location" -> published.location(),
          "coldBefore" -> true,
          "computedAfter" -> computed(snapshot),
          "aggregateObservations" -> c.aggregateCount.get(),
          "aggregateOutputIdentities" -> c.native.identities.map(_.asJava).asJava,
          "physicalCompletions" -> c.completions.asScala.toVector.asJava,
          "storageEqual" -> true,
          "sqlConfEqual" -> true,
          "activeTransactionAfter" -> false,
          "nativeOperationAfter" -> false
        )
      )
    }
  }

  private def openSpark(request: Request, observation: Observation): SparkSession = {
    val properties = new Properties()
    val stream = Files.newInputStream(Paths.get(request.text("sparkConfigurationFile")))
    try properties.load(stream)
    finally stream.close()
    val conf =
      new SparkConf(false).setMaster("local[2]").setAppName("delta-ranger-live-idempotency")
    val hadoop = new Configuration()
    properties.stringPropertyNames().asScala.foreach { key =>
      conf.set(key, properties.getProperty(key))
      if (key.startsWith("spark.hadoop."))
        hadoop.set(key.stripPrefix("spark.hadoop."), properties.getProperty(key))
    }
    UserGroupInformation.setConfiguration(hadoop)
    SparkSession
      .builder()
      .config(conf)
      .withExtensions(_.injectResolutionRule(_ => observation.rule))
      .getOrCreate()
  }

  private def inventory(
      location: HadoopPath,
      spark: SparkSession,
      archive: Path
  ): Map[String, String] = {
    val fs = location.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val files = fs.listFiles(location, true)
    val entries = scala.collection.mutable.Map.empty[String, String]
    var index = 0
    while (files.hasNext) {
      val file = files.next()
      val key = file.getPath.toUri.toString
      val digest = MessageDigest.getInstance("SHA-256")
      val source = fs.open(file.getPath)
      val destination = Files.newOutputStream(
        Files.createFile(
          archive.resolve(f"$index%08d.bin"),
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
        )
      )
      var length = 0L
      try {
        val buffer = new Array[Byte](65536)
        var count = source.read(buffer)
        while (count != -1) {
          digest.update(buffer, 0, count)
          destination.write(buffer, 0, count)
          length += count
          count = source.read(buffer)
        }
      } finally {
        try source.close()
        finally destination.close()
      }
      check(length == file.getLen, "Object length changed during inventory")
      check(!entries.contains(key), "Duplicate filesystem object")
      entries(key) = s"$length:${hex(digest.digest())}"
      write(
        archive.resolve(f"$index%08d.json"),
        Map(
          "key" -> key,
          "length" -> length,
          "sha256" -> hex(digestBytes(archive.resolve(f"$index%08d.bin")))
        )
      )
      index += 1
    }
    check(entries.nonEmpty, "Empty native storage manifest")
    entries.toMap
  }

  private def digestBytes(path: Path): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")
    val stream = Files.newInputStream(path)
    try {
      val buffer = new Array[Byte](65536)
      var count = stream.read(buffer)
      while (count != -1) { digest.update(buffer, 0, count); count = stream.read(buffer) }
      digest.digest()
    } finally stream.close()
  }

  private def awaitVersions(engine: DeltaRangerPolicyEngine, request: Request): Unit = {
    val end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
    var ready = false
    while (!ready && System.nanoTime() < end) {
      try
        ready = engine.versions().policy() >= request
          .number("minPolicy") && engine.versions().role() >= request.number("minRole")
      catch { case _: SecurityException => () }
      if (!ready) Thread.sleep(100)
    }
    check(ready, "Downloaded generation floor not reached")
  }

  private def stateAggregate(plan: LogicalPlan): Boolean = stateAggregates(plan).nonEmpty
  private def stateAggregates(plan: LogicalPlan): Seq[Aggregate] = plan.collect {
    case aggregate: Aggregate if aggregate.aggregateExpressions.map(_.name).toSet == aliases =>
      aggregate
  }
  private def computed(snapshot: Snapshot): Boolean = snapshot.getClass
    .getMethod("_computedStateTriggered")
    .invoke(snapshot)
    .asInstanceOf[java.lang.Boolean]
    .booleanValue()
  private def origin(clazz: Class[_]): String =
    clazz.getProtectionDomain.getCodeSource.getLocation.toExternalForm
  private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString
  private def check(value: Boolean, message: String): Unit =
    if (!value) throw new AssertionError(message)
  private def write(path: Path, value: Any): Unit = {
    Files.createFile(
      path,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
    )
    json
      .writerWithDefaultPrettyPrinter()
      .writeValue(path.toFile, value match { case m: Map[_, _] => m.asJava; case other => other })
  }
}
