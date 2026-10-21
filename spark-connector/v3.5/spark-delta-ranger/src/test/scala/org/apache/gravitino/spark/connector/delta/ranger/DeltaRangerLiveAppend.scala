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

import java.net.URI
import java.util.{IdentityHashMap, List => JavaList, Map => JavaMap, Properties, UUID}
import java.util.concurrent.TimeUnit
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.CachedData
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import io.delta.sql.DeltaSparkSessionExtension
import com.fasterxml.jackson.core.{JsonParser, JsonProcessingException}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import scala.collection.JavaConverters._

/** Test-only packaged public-writer acceptance application. */
object DeltaRangerLiveAppend {
  private val json = new ObjectMapper()
    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
  private val textFields = Set(
    "phase",
    "frontend",
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
    "source",
    "target",
    "transactionAppId",
    "expectedSparkVersion",
    "expectedDeltaVersion",
    "publicStateSha256",
    "rangerStateSha256",
    "stateEvidenceFile",
    "packageManifest",
    "warmOrder"
  )
  private val numberFields = Set("schemaVersion", "minPolicy", "minRole", "transactionVersion")

  private[ranger] def validateExtensions(value: String): Unit = {
    val expected = Seq(
      "org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions",
      "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerSparkExtension",
      "io.delta.sql.DeltaSparkSessionExtension"
    ).mkString(",")
    require(value == expected, "Extension order differs")
  }

  private[ranger] def parse(input: String): JsonNode = {
    val value =
      try json.readTree(input)
      catch {
        case _: JsonProcessingException =>
          throw new IllegalArgumentException("Malformed private request")
      }
    require(value != null && value.isObject, "Request must be an object")
    require(
      value.fieldNames().asScala.toSet == textFields ++ numberFields ++
        Set("expectedPreimage", "expectedPostimage", "duplicate", "barrier"),
      "Unexpected request keys"
    )
    textFields.foreach { name =>
      val v = value.get(name)
      require(
        v.isTextual && v.asText().nonEmpty && v.asText() == v.asText().trim,
        "Invalid text field: " + name
      )
    }
    numberFields.foreach(name => nonnegative(value.get(name)))
    require(value.path("schemaVersion").longValue() == 1, "Unsupported schema")
    require(
      Set("baseline-denied", "granted-append", "revoked-denied", "after-helper-revoked")(
        value.path("phase").asText()
      ),
      "Unsupported phase"
    )
    require(
      Set("insertInto", "saveAsTable")(value.path("frontend").asText()),
      "Unsupported frontend"
    )
    require(
      Set("3.5.4", "3.5.6")(value.path("expectedSparkVersion").asText()),
      "Unsupported Spark pin"
    )
    require(
      Set("3.3.0", "3.3.2")(value.path("expectedDeltaVersion").asText()),
      "Unsupported Delta pin"
    )
    Seq("catalog", "database", "source", "target", "metalake").foreach(name =>
      require(
        value.path(name).asText().matches("[A-Za-z_][A-Za-z0-9_]*"),
        "Noncanonical identifier"
      )
    )
    require(value.path("subject") != value.path("metadataPrincipal"), "Identities must differ")
    require(value.path("source") != value.path("target"), "Tables must differ")
    Seq("publicStateSha256", "rangerStateSha256").foreach(name =>
      require(value.path(name).asText().matches("[0-9a-f]{64}"), "Invalid evidence digest")
    )
    Seq(
      "sourceSecurityDirectory",
      "driverSecurityDirectory",
      "sparkConfigurationFile",
      "stateEvidenceFile",
      "packageManifest"
    ).foreach(name =>
      require(
        Paths.get(value.path(name).asText()).isAbsolute,
        "Private paths must be absolute"
      )
    )
    require(value.path("duplicate").isBoolean, "Duplicate must be boolean")
    val granted = value.path("phase").asText() == "granted-append"
    require(
      Set("none", "target-first", "project-first")(value.path("warmOrder").asText()),
      "Invalid warm order"
    )
    require(
      granted || (!value
        .path("duplicate")
        .asBoolean() && value.path("warmOrder").asText() == "none"),
      "Denials cannot be duplicate or warm cases"
    )
    Seq("expectedPreimage", "expectedPostimage").foreach { name =>
      val n = value.path(name)
      require(
        n.isObject && n.fieldNames().asScala.toSet == Set("version", "rows"),
        "Invalid native image"
      )
      nonnegative(n.path("version"))
      require(
        n.path("rows")
          .isArray && n.path("rows").size() > 0 && n.path("rows").elements().asScala.forall { row =>
          row.isArray && row
            .size() == 3 && row.get(0).isIntegralNumber && row.get(0).canConvertToLong &&
          row.get(1).isTextual && row.get(2).isTextual
        },
        "Literal native rows are required"
      )
    }
    val before = value.path("expectedPreimage").path("version").longValue()
    val after = value.path("expectedPostimage").path("version").longValue()
    require(
      before < Long.MaxValue && after == before + (if (granted) 1 else 0),
      "Invalid expected version transition"
    )
    if (!granted)
      require(
        value.path("expectedPreimage") == value.path("expectedPostimage"),
        "Denial must preserve committed image"
      )
    val barrier = value.path("barrier")
    if (value.path("phase").asText() == "after-helper-revoked") {
      require(
        barrier.isObject && barrier.fieldNames().asScala.toSet == Set(
          "controlDirectory",
          "deadlineSeconds"
        ),
        "Barrier required"
      )
      require(
        barrier.path("controlDirectory").isTextual && java.nio.file.Paths
          .get(barrier.path("controlDirectory").asText())
          .isAbsolute,
        "Invalid control directory"
      )
      nonnegative(barrier.path("deadlineSeconds"))
      require(
        barrier
          .path("deadlineSeconds")
          .longValue() >= 5 && barrier.path("deadlineSeconds").longValue() <= 120,
        "Unbounded barrier"
      )
    } else require(barrier.isNull, "Unexpected barrier")
    value
  }

  private[ranger] def parseCdf(input: String): JsonNode = {
    val value =
      try json.readTree(input)
      catch {
        case _: JsonProcessingException =>
          throw new IllegalArgumentException("Malformed CDF request")
      }
    val paths = Set(
      "sourceSecurityDirectory",
      "driverSecurityDirectory",
      "sparkConfigurationFile",
      "stateEvidenceFile",
      "packageManifest",
      "controlDirectory"
    )
    val identifiers = Set(
      "metalake",
      "catalog",
      "database",
      "source",
      "target",
      "enabledDiagnostic",
      "updateDiagnostic",
      "deniedDiagnostic"
    )
    val texts = paths ++ identifiers ++ Set(
      "resourceService",
      "resourceDefinition",
      "subject",
      "metadataPrincipal",
      "expectedSparkVersion",
      "expectedDeltaVersion",
      "publicStateSha256",
      "rangerStateSha256"
    )
    val numbers = Set("schemaVersion", "minPolicy", "minRole", "beforeVersion", "deadlineSeconds")
    require(
      value != null && value.isObject && value.fieldNames().asScala.toSet == texts ++ numbers,
      "Unexpected CDF request keys"
    )
    texts.foreach { key =>
      val v = value.path(key);
      require(
        v.isTextual && v.asText().nonEmpty && v.asText() == v.asText().trim,
        "Invalid CDF text"
      )
    }
    numbers.foreach(k => nonnegative(value.path(k)))
    paths.foreach(k =>
      require(Paths.get(value.path(k).asText()).isAbsolute, "CDF path must be absolute")
    )
    identifiers.foreach(k =>
      require(value.path(k).asText().matches("[A-Za-z_][A-Za-z0-9_]*"), "Invalid CDF identifier")
    )
    require(value.path("schemaVersion").asLong() == 1, "Unsupported CDF schema")
    require(
      value.path("deadlineSeconds").asLong() >= 5 && value.path("deadlineSeconds").asLong() <= 600,
      "Unbounded CDF checkpoint"
    )
    require(value.path("beforeVersion").asLong() <= Long.MaxValue - 4, "Unbounded native head")
    require(
      Set("3.5.4", "3.5.6")(value.path("expectedSparkVersion").asText()) && Set("3.3.0", "3.3.2")(
        value.path("expectedDeltaVersion").asText()
      ),
      "Unsupported CDF runtime"
    )
    require(value.path("subject") != value.path("metadataPrincipal"), "CDF identities must differ")
    val tables =
      Seq("source", "target", "enabledDiagnostic", "updateDiagnostic", "deniedDiagnostic").map(
        value.path(_).asText()
      )
    require(tables.distinct.size == tables.size, "CDF tables must differ")
    Seq("publicStateSha256", "rangerStateSha256").foreach(k =>
      require(value.path(k).asText().matches("[0-9a-f]{64}"), "Invalid CDF evidence digest")
    )
    value
  }

  /** Runs the finite packaged linkage smoke or one explicitly requested live phase. */
  def main(args: Array[String]): Unit = {
    require(
      args.length == 2 || (args.length == 4 && args(0) == "--smoke") || (args.length == 3 && args(
        0
      ) == "--cdf"),
      "Expected a private request and new evidence path, or explicit smoke arguments"
    )
    val output = Paths.get(args.last)
    require(!Files.exists(output) && !Files.isSymbolicLink(output), "Evidence must be new")
    Files.createDirectory(
      output,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    try {
      if (args.length == 4) smoke(Paths.get(args(1)), Paths.get(args(2)), output)
      else if (args.length == 3) live(Paths.get(args(1)), output, cdf = true)
      else live(Paths.get(args(0)), output)
    } catch {
      case error: Throwable =>
        json.writeValue(
          output.resolve("failure.json").toFile,
          Map(
            "failureClasses" ->
              Iterator
                .iterate(error)(_.getCause)
                .takeWhile(_ != null)
                .map(_.getClass.getName)
                .toVector
                .asJava,
            "stack" -> error.getStackTrace.map(_.toString).toVector.asJava
          ).asJava
        )
        throw new IllegalStateException(
          "Packaged acceptance failed; inspect retained private evidence"
        )
    }
  }

  private def smoke(manifestPath: Path, inputsPath: Path, output: Path): Unit = {
    val manifest = json.readTree(Files.readAllBytes(manifestPath))
    val inputs = json.readTree(Files.readAllBytes(inputsPath))
    require(System.getProperty("java.specification.version") == "17", "JDK17 required")
    require(scala.util.Properties.versionNumberString == "2.12.21", "Scala family mismatch")
    val expected = manifest.path("providers")
    val loader = getClass.getClassLoader
    val origins = expected
      .fieldNames()
      .asScala
      .map { name =>
        val cls = Class.forName(name, false, loader)
        val location = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
        require(
          location == Paths.get(expected.path(name).asText()),
          "Actual origin differs for " + name
        )
        require(
          sha(location) == manifest.path("jarHashes").path(location.toString).asText(),
          "Actual artifact changed"
        )
        val providers =
          loader.getResources(name.replace('.', '/') + ".class").asScala.map(_.toString).toSet
        require(providers.size == 1, "Duplicate provider for " + name)
        require(cls.getClassLoader eq loader, "Defining loader mismatch for " + name)
        val classDigest = MessageDigest.getInstance("SHA-256")
        val classStream = cls.getResourceAsStream("/" + name.replace('.', '/') + ".class")
        try classDigest.update(classStream.readAllBytes())
        finally classStream.close()
        name -> Map(
          "origin" -> location.getFileName.toString,
          "sha256" -> sha(location),
          "classSha256" -> classDigest.digest().map(b => f"${b & 0xff}%02x").mkString,
          "sameLoader" -> (cls.getClassLoader eq loader),
          "loader" -> cls.getClassLoader.getClass.getName
        ).asJava
      }
      .toMap
    DeltaRangerConfig.require(true, "packaged loader smoke")
    require(
      expected
        .path("org.apache.hadoop.fs.FileSystem")
        .asText()
        .endsWith("hadoop-client-api-3.3.6.jar") &&
        expected
          .path("org.apache.hadoop.fs.Path")
          .asText()
          .endsWith("hadoop-client-api-3.3.6.jar") &&
        expected
          .path("org.apache.hadoop.fs.s3a.S3AFileSystem")
          .asText()
          .endsWith("hadoop-aws-3.3.6.jar") &&
        expected
          .path("com.amazonaws.services.s3.AmazonS3Client")
          .asText()
          .endsWith("aws-java-sdk-bundle-1.12.367.jar") &&
        expected
          .path("com.google.common.collect.ImmutableList")
          .asText()
          .endsWith("guava-32.1.3-jre.jar"),
      "Native artifact pins differ"
    )
    require(
      expected.path("org.apache.ranger.plugin.service.RangerBasePlugin").asText() ==
        expected
          .path("org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerConfig")
          .asText(),
      "Ranger is outside optional bundle"
    )
    val pin = DeltaRangerLivePhasePin.inspect(output)
    Class.forName("com.sun.jna.Native", true, loader)
    val security = new DeltaRangerDownloadedSecurity(
      Paths.get(inputs.path("sourceSecurityDirectory").asText()),
      Paths.get(inputs.path("driverSecurityDirectory").asText()),
      output,
      inputs.path("resourceService").asText()
    )
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("public-append-packaged-smoke")
        .config("spark.ui.enabled", "true")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
      require(spark.version == manifest.path("spark").asText(), "Spark pin mismatch")
      require(
        expected
          .path("org.apache.spark.sql.delta.DeltaLog")
          .asText()
          .endsWith("delta-spark_2.12-" + manifest.path("delta").asText() + ".jar"),
        "Delta pin mismatch"
      )
      val local = output.resolve("native-smoke").toString
      spark.range(1).coalesce(1).write.format("delta").save(local)
      require(spark.read.format("delta").load(local).count() == 1L, "Native action failed")
      json.writeValue(output.resolve("origins.json").toFile, origins.asJava)
      json.writeValue(
        output.resolve("phase-pin.json").toFile,
        Map(
          "classSha256" -> pin.classSha256,
          "artifactSha256" -> pin.artifactSha256,
          "postReturnLine" -> Int.box(pin.line)
        ).asJava
      )
    } finally {
      try { if (spark != null) spark.stop() }
      finally security.close()
    }
    json.writeValue(
      output.resolve("completed.json").toFile,
      Map(
        "scope" -> "packaged loader, helper lifecycle, phase pin and native local action only",
        "completed" -> Boolean.box(true)
      ).asJava
    )
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

  private def live(requestPath: Path, output: Path, cdf: Boolean = false): Unit = {
    val raw = new String(Files.readAllBytes(requestPath), "UTF-8")
    val request = if (cdf) parseCdf(raw) else parse(raw)
    def text(key: String) = request.path(key).asText()
    val manifest = json.readTree(Files.readAllBytes(Paths.get(text("packageManifest"))))
    val state = json.readTree(Files.readAllBytes(Paths.get(text("stateEvidenceFile"))))
    require(
      sha(Paths.get(state.path("publicStateFile").asText())) == text("publicStateSha256"),
      "Public state digest differs"
    )
    require(
      sha(Paths.get(state.path("rangerStateFile").asText())) == text("rangerStateSha256"),
      "Ranger state digest differs"
    )
    val ranger =
      json.readTree(Files.readAllBytes(Paths.get(state.path("rangerStateFile").asText())))
    val properties = new Properties()
    val stream = Files.newInputStream(Paths.get(text("sparkConfigurationFile")))
    try properties.load(stream)
    finally stream.close()
    val upstream = URI.create(properties.getProperty("spark.sql.gravitino.uri"))
    val gateway = new DeltaRangerLiveMetadataBarrier.Gateway(upstream, output)
    val targetPath = "/api/metalakes/" + text("metalake") + "/catalogs/" + text("catalog") +
      "/schemas/" + text("database") + "/tables/" + text("target")
    gateway.observeTarget(targetPath, text("metadataPrincipal"))
    if (cdf)
      gateway.observeTables(
        targetPath.substring(0, targetPath.lastIndexOf('/') + 1),
        text("metadataPrincipal")
      )
    var security: DeltaRangerDownloadedSecurity = null
    var spark: SparkSession = null
    var primary: Throwable = null
    try {
      security = new DeltaRangerDownloadedSecurity(
        Paths.get(text("sourceSecurityDirectory")),
        Paths.get(text("driverSecurityDirectory")),
        output,
        text("resourceService"),
        true
      )
      val loader = getClass.getClassLoader
      manifest.path("providers").fields().asScala.foreach { entry =>
        val cls = Class.forName(entry.getKey, false, loader)
        val actual = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
        require(
          actual == Paths.get(entry.getValue.asText()) && cls.getClassLoader == loader &&
            sha(actual) == manifest.path("jarHashes").path(actual.toString).asText(),
          "Packaged runtime origin changed"
        )
      }
      DeltaRangerConfig.require(true, "packaged live linkage")
      val pin = DeltaRangerLivePhasePin.inspect(output)
      val conf = new SparkConf(false).setMaster("local[2]").setAppName("public-grants-live-append")
      val hadoop = new Configuration()
      properties.stringPropertyNames().asScala.foreach { key =>
        conf.set(key, properties.getProperty(key))
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
        ) && scala.util.Properties.versionNumberString == "2.12.21" &&
          System.getProperty("java.specification.version") == "17",
        "Runtime pin mismatch"
      )
      require(
        spark.sparkContext.getConf.get(
          "spark.plugins"
        ) == "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin",
        "Discovery plugin differs"
      )
      validateExtensions(spark.sparkContext.getConf.get("spark.sql.extensions"))
      val subject = DeltaRangerQuerySubject.current(spark)
      require(
        subject.user() == text("subject") && spark.sparkContext.sparkUser == subject.user() &&
          System.getenv("SPARK_USER") == subject.user() && System.getenv(
            "HADOOP_USER_NAME"
          ) == subject.user() && UserGroupInformation.getCurrentUser.getShortUserName == subject
            .user(),
        "Actual application identity differs"
      )
      val wrapper = spark.sessionState.catalogManager.catalog(text("catalog"))
      require(wrapper.getClass == classOf[DeltaRangerCatalogSpark35], "Production wrapper absent")
      captureProfile(
        spark,
        properties.getProperty("spark.sql.extensions"),
        wrapper.getClass,
        output
      )
      val catalog = wrapper.asInstanceOf[DeltaRangerCatalogSpark35]
      val engine = catalog.policyEngine()
      awaitApplied(
        engine,
        request.path("minPolicy").longValue(),
        request.path("minRole").longValue(),
        45
      )
      security.recordInitialDownloads(
        engine.versions().policy(),
        engine.versions().role(),
        request.path("minPolicy").longValue(),
        request.path("minRole").longValue()
      )
      val initial = security.completedAfter(
        0,
        DeltaRangerDownloadedSecurity.DownloadRequirement.BOTH,
        engine.versions()
      )
      val initialPolicy = engine.versions().policy(); val initialRole = engine.versions().role()
      require(
        initialPolicy == request
          .path("minPolicy")
          .longValue() && initialRole == request.path("minRole").longValue(),
        "Unreviewed service generation"
      )
      verifyDownload(security, initial.policy().get(), ranger.path("policies"), "policy")
      verifyDownload(security, initial.role().get(), ranger.path("roles"), "role")
      writeJson(
        output.resolve("applied-downloads.json"),
        Map(
          "policyVersion" -> initialPolicy,
          "roleVersion" -> initialRole,
          "policyResponseSha256" -> initial.policy().get().responseSha256(),
          "roleResponseSha256" -> initial.role().get().responseSha256(),
          "subject" -> subject.user(),
          "sparkUser" -> spark.sparkContext.sparkUser,
          "ugi" -> UserGroupInformation.getCurrentUser.getShortUserName,
          "SPARK_USER" -> System.getenv("SPARK_USER"),
          "HADOOP_USER_NAME" -> System.getenv("HADOOP_USER_NAME"),
          "metadataPrincipal" -> text("metadataPrincipal")
        ).asJava
      )
      val target = catalog
        .loadTable(Identifier.of(Array(text("database")), text("target")))
        .asInstanceOf[DeltaTableV2]
      gateway.requireMetadataObserved()
      val before = target.deltaLog.unsafeVolatileSnapshot.version
      if (cdf) {
        liveCdf(spark, catalog, security, request, output, initial.cursor())
        gateway.requireMetadataObserved("metadata-identity-final.json")
      } else {
        require(
          before == request.path("expectedPreimage").path("version").longValue(),
          "Native preimage version differs"
        )
        spark.conf.set("spark.databricks.delta.write.txnVersion.autoReset.enabled", "false")
        spark.conf.set("spark.databricks.delta.write.txnAppId", text("transactionAppId"))
        spark.conf.set(
          "spark.databricks.delta.write.txnVersion",
          request.path("transactionVersion").asText()
        )
        val observer = new WarmObserver(text("warmOrder"))
        val warm =
          if (text("warmOrder") == "none") None
          else
            Some(
              warmCaches(
                spark,
                state.path("targetLocation").asText(),
                text("warmOrder"),
                output,
                observer
              )
            )
        val sourceName = text("catalog") + "." + text("database") + "." + text("source")
        val targetName = text("catalog") + "." + text("database") + "." + text("target")
        def append(attempt: String): Unit = {
          SparkSession.setActiveSession(spark)
          val directory = output.resolve("attempts").resolve(attempt)
          Files.createDirectories(directory.getParent)
          Files.createDirectory(directory)
          val preVersion = target.deltaLog.unsafeVolatileSnapshot.version
          writeJson(
            directory.resolve("intent.json"),
            Map("attempt" -> attempt, "frontend" -> text("frontend")).asJava
          )
          var stage = "source-construction"
          var caught: Throwable = null
          try {
            val input = spark.table(sourceName)
            stage = "public-writer"
            val writer = input.write.mode("append")
            if (text("frontend") == "insertInto") writer.insertInto(targetName)
            else writer.saveAsTable(targetName)
            stage = "post-return"
          } catch { case error: Throwable => caught = error; throw error }
          finally {
            val exact = exactDenial(caught, text("phase") == "after-helper-revoked")
            val outcome =
              if (caught == null) "success"
              else if (!exact) "unexpected-failure"
              else if (text("phase") == "after-helper-revoked") "stale-policy-publication"
              else "target-update-denied"
            if (caught != null) warm.foreach { case (_, frames) =>
              try observer.record(spark, frames, directory.resolve("warm-failure.json"), "failure")
              catch {
                case evidenceError: Throwable =>
                  writeJson(
                    directory.resolve("warm-evidence-failure.json"),
                    failureEvidence(evidenceError)
                  )
              }
            }
            writeJson(
              directory.resolve("result.json"),
              Map[String, Any](
                "attempt" -> attempt,
                "frontend" -> text("frontend"),
                "stage" -> stage,
                "transactionAppId" -> text("transactionAppId"),
                "transactionVersion" -> request.path("transactionVersion").longValue(),
                "beforeVersion" -> preVersion,
                "afterVersion" -> target.deltaLog.unsafeVolatileSnapshot.version,
                "outcome" -> outcome,
                "exactDenial" -> exact,
                "failure" -> failureEvidence(caught),
                "cleanup" -> Map(
                  "transactionActive" -> OptimisticTransaction.getActive.nonEmpty,
                  "nativeOperationActive" -> (DeltaRangerNativeOperation.current() != null),
                  "commitActive" -> DeltaRangerAppendCommit.current.nonEmpty
                ).asJava
              ).asJava
            )
          }
        }
        var failure: Throwable = null
        if (text("phase") == "after-helper-revoked") {
          val control = Paths.get(request.path("barrier").path("controlDirectory").asText())
          val writer = new Thread(
            () => {
              try append("first")
              catch { case e: Throwable => failure = e }
            },
            "owned-public-append-writer"
          )
          val seconds = request.path("barrier").path("deadlineSeconds").longValue()
          gateway.arm(writer, targetPath, pin, seconds)
          writer.start()
          try {
            gateway.awaitHeld(seconds)
            writeJson(
              control.resolve("held-ready.json"),
              Map(
                "held" -> true,
                "initialPolicy" -> initialPolicy,
                "initialRole" -> initialRole
              ).asJava
            )
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds)
            val revoke = control.resolve("revoke-complete.private.json")
            while (!Files.exists(revoke) && System.nanoTime() < deadline) Thread.sleep(25)
            require(Files.exists(revoke), "Supervisor did not complete public revoke")
            val evidence = json.readTree(Files.readAllBytes(revoke))
            awaitApplied(
              engine,
              evidence.path("policy").longValue(),
              initialRole,
              math.max(1L, TimeUnit.NANOSECONDS.toSeconds(deadline - System.nanoTime()))
            )
            val applied = security.completedAfter(
              initial.cursor(),
              DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY,
              engine.versions()
            )
            val expected =
              json.readTree(
                Files.readAllBytes(Paths.get(evidence.path("rangerStateFile").asText()))
              )
            verifyDownload(security, applied.policy().get(), expected.path("policies"), "policy")
            require(
              engine.versions().policy() == evidence
                .path("policy")
                .longValue() && engine.versions().role() == initialRole,
              "Revoke not applied in original JVM"
            )
            val witness = json.createObjectNode()
            witness.put("initialPolicy", initialPolicy);
            witness.put("policy", engine.versions().policy()); witness.put("role", initialRole)
            witness.put("downloadVersion", applied.policy().get().version());
            witness.put("responseSha256", applied.policy().get().responseSha256())
            witness.put(
              "publicStateSha256",
              sha(Paths.get(evidence.path("publicStateFile").asText()))
            )
            witness.put(
              "rangerStateSha256",
              sha(Paths.get(evidence.path("rangerStateFile").asText()))
            )
            witness.put("publicRevokeComplete", evidence.path("publicRevokeComplete").asBoolean())
            witness.put("updateAbsent", evidence.path("updateAbsent").asBoolean());
            witness.put("roleMatchesInitialDownload", true)
            gateway.releaseAfter(witness, initialPolicy, initialRole)
            writer.join(20000)
            require(!writer.isAlive, "Original writer failed to terminate")
            gateway.requireSuccessfulForward()
            verifyWriterOutcome(failure, text("phase"), output)
          } catch {
            case e: Throwable =>
              gateway.abort(); writer.join(5000)
              writeJson(
                output.resolve("barrier-failure.json"),
                Map[String, Any](
                  "writerAlive" -> writer.isAlive,
                  "supervisorFailure" -> failureEvidence(e),
                  "writerFailure" -> failureEvidence(failure)
                ).asJava
              )
              throw e
          }
        } else {
          try append("first")
          catch { case e: Throwable => failure = e }
          verifyWriterOutcome(failure, text("phase"), output)
        }
        val after = target.deltaLog.unsafeVolatileSnapshot.version
        require(
          after == request.path("expectedPostimage").path("version").longValue(),
          "Unexpected publication count"
        )
        warm.foreach { case (_, frames) =>
          observer.record(spark, frames, output.resolve("warm-after-append.json"), "success")
        }
        if (request.path("duplicate").asBoolean()) {
          checkpoint(
            Paths.get(text("sparkConfigurationFile")).getParent.resolve("control"),
            "first"
          )
          append("duplicate")
          require(
            target.deltaLog.unsafeVolatileSnapshot.version == after,
            "Duplicate published another version"
          )
          warm.foreach { case (_, frames) =>
            observer.record(spark, frames, output.resolve("warm-after-duplicate.json"), "success")
          }
          checkpoint(
            Paths.get(text("sparkConfigurationFile")).getParent.resolve("control"),
            "duplicate"
          )
        }
        warm.foreach { case (native, frames) =>
          SparkSession.setActiveSession(native)
          try {
            val fresh = native.read.format("delta").load(state.path("targetLocation").asText())
            val actualRows = rowImage(fresh)
            require(
              actualRows == request.path("expectedPostimage").path("rows"),
              "Late native target rows differ"
            )
            val projected = fresh
              .select("id", "region")
              .collect()
              .toVector
              .map(r => Vector(Long.box(r.getLong(0)), r.getString(1)).asJava)
              .asJava
            require(
              projected.size() == request.path("expectedPostimage").path("rows").size(),
              "Dependent cache rows differ"
            )
            val repeatedRows = rowImage(fresh)
            require(
              repeatedRows == request.path("expectedPostimage").path("rows"),
              "Repeated late native rows differ"
            )
            writeJson(
              output.resolve("warm-late-readback.json"),
              Map(
                "targetRows" -> actualRows,
                "repeatedTargetRows" -> repeatedRows,
                "dependentRows" -> projected,
                "activeNativeSessionRestored" -> (SparkSession.active eq native)
              ).asJava
            )
          } finally SparkSession.setActiveSession(spark)
        }
        require(
          OptimisticTransaction.getActive.isEmpty && DeltaRangerNativeOperation.current() == null,
          "Native scope leaked"
        )
        writeJson(
          output.resolve("writer-result.json"),
          Map(
            "phase" -> text("phase"),
            "frontend" -> text("frontend"),
            "beforeVersion" -> before,
            "afterVersion" -> after,
            "duplicate" -> request.path("duplicate").asBoolean(),
            "outcome" -> (if (failure == null) "success"
                          else if (text("phase") == "after-helper-revoked")
                            "stale-policy-publication"
                          else "target-update-denied"),
            "failureClasses" -> Option(failure)
              .map(e =>
                Iterator
                  .iterate(e)(_.getCause)
                  .takeWhile(_ != null)
                  .map(_.getClass.getName)
                  .toVector
                  .asJava
              )
              .orNull
          ).asJava
        )
      }
    } catch { case error: Throwable => primary = error; throw error }
    finally {
      val failures = scala.collection.mutable.ArrayBuffer.empty[Throwable]
      def close(action: => Unit): Unit = try action
      catch { case e: Throwable => failures += e }
      close(if (spark != null) spark.stop())
      close(if (security != null) security.close())
      close(gateway.close())
      if (failures.nonEmpty) {
        writeJson(output.resolve("cleanup-failures.json"), failures.map(failureEvidence).asJava)
        if (primary != null) failures.foreach(primary.addSuppressed)
        else throw failures.head
      }
    }
    writeJson(
      output.resolve("app-completed.json"),
      Map(
        "completed" -> true,
        "scope" -> "writer and same-JVM transport only; supervisor must verify native and raw object outcomes"
      ).asJava
    )
  }

  private def liveCdf(
      spark: SparkSession,
      catalog: DeltaRangerCatalogSpark35,
      security: DeltaRangerDownloadedSecurity,
      request: JsonNode,
      output: Path,
      initialCursor: Long
  ): Unit = {
    def text(key: String) = request.path(key).asText()
    def named(table: String) = text("catalog") + "." + text("database") + "." + table
    val engine = catalog.policyEngine()
    val control = Paths.get(text("controlDirectory"))
    var cursor = initialCursor
    def checkpoint(name: String, applyChange: Boolean = false): Unit = {
      writeJson(
        control.resolve(name + "-ready.json"),
        Map("checkpoint" -> name, "ready" -> true).asJava
      )
      val deadline =
        System.nanoTime() + TimeUnit.SECONDS.toNanos(request.path("deadlineSeconds").asLong())
      val release = control.resolve(name + "-verified.json")
      while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(25)
      require(Files.exists(release), "CDF supervisor checkpoint timed out")
      val evidence = json.readTree(Files.readAllBytes(release))
      require(
        evidence.path("verified").isBoolean && evidence
          .path("verified")
          .asBoolean() && evidence.path("checkpoint").asText() == name,
        "CDF supervisor rejected checkpoint"
      )
      if (applyChange) {
        val state =
          json.readTree(Files.readAllBytes(Paths.get(evidence.path("stateEvidenceFile").asText())))
        require(
          state.path("policy").asLong() > engine
            .versions()
            .policy() || engine.versions().policy() == state.path("policy").asLong(),
          "CDF policy regressed"
        )
        val oldRole = engine.versions().role()
        awaitApplied(engine, state.path("policy").asLong(), state.path("role").asLong(), 45)
        val applied = security.completedAfter(
          cursor,
          DeltaRangerDownloadedSecurity.DownloadRequirement.POLICY,
          engine.versions()
        )
        val ranger =
          json.readTree(Files.readAllBytes(Paths.get(state.path("rangerStateFile").asText())))
        verifyDownload(security, applied.policy().get(), ranger.path("policies"), "policy")
        require(
          engine.versions().policy() == state.path("policy").asLong() && engine
            .versions()
            .role() == oldRole && oldRole == state.path("role").asLong(),
          "CDF change not applied in original JVM"
        )
        writeJson(
          output.resolve(name + "-applied.json"),
          Map(
            "policy" -> engine.versions().policy(),
            "role" -> oldRole,
            "policyResponseSha256" -> applied.policy().get().responseSha256(),
            "publicStateSha256" -> sha(Paths.get(state.path("publicStateFile").asText())),
            "rangerStateSha256" -> sha(Paths.get(state.path("rangerStateFile").asText())),
            "sameJvm" -> true
          ).asJava
        )
        cursor = applied.cursor()
      }
    }
    def head(table: String): Long = catalog
      .loadTable(Identifier.of(Array(text("database")), table))
      .asInstanceOf[DeltaTableV2]
      .deltaLog
      .unsafeVolatileSnapshot
      .version
    def alter(label: String, table: String, enabled: Boolean, allowed: Boolean): Unit = {
      val before = head(table)
      val sql = "ALTER TABLE " + named(
        table
      ) + " SET TBLPROPERTIES ('delta.enableChangeDataFeed'='" + enabled.toString + "')"
      var failure: Throwable = null
      try spark.sql(sql).collect()
      catch { case e: Throwable => failure = e }
      val after = head(table)
      writeJson(
        output.resolve(label + ".json"),
        Map(
          "sql" -> sql,
          "allowed" -> allowed,
          "beforeVersion" -> before,
          "afterVersion" -> after,
          "failure" -> failureEvidence(failure)
        ).asJava
      )
      if (allowed) require(failure == null && after == before + 1, "Named CDF mutation failed")
      else {
        val causes = Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toVector
        require(
          causes.exists(e =>
            e.getClass == classOf[
              SecurityException
            ] && e.getMessage == "Delta Ranger access denied" && e.getStackTrace.headOption.exists(
              f =>
                f.getClassName == classOf[
                  DeltaRangerPolicyEngine
                ].getName && f.getMethodName == "authorize"
            )
          ),
          "CDF denial was not Ranger authorization"
        )
        require(after == before, "Denied CDF command changed native head")
      }
      require(
        OptimisticTransaction.getActive.isEmpty && DeltaRangerNativeOperation.current() == null,
        "CDF authority leaked"
      )
    }
    def ordinary(label: String): Unit = {
      val rows = governedRowImage(spark.table(named(text("target"))));
      val sourceRows = governedRowImage(spark.table(named(text("source"))))
      writeJson(
        output.resolve(label + "-read.json"),
        Map("targetRows" -> rows, "sourceRows" -> sourceRows).asJava
      )
    }
    def append(label: String): Unit = {
      withCdfAppendTransaction(spark) {
        spark.table(named(text("source"))).write.mode("append").insertInto(named(text("target")))
      }
      require(
        OptimisticTransaction.getActive.isEmpty && DeltaRangerNativeOperation
          .current() == null && DeltaRangerAppendCommit.current.isEmpty,
        "CDF append authority leaked"
      )
      ordinary(label)
    }
    require(
      head(text("target")) == request.path("beforeVersion").asLong(),
      "CDF native preimage differs"
    )
    alter("select-enable-denied", text("target"), true, false)
    alter("select-disable-denied", text("enabledDiagnostic"), false, false)
    alter("update-enable-denied", text("updateDiagnostic"), true, false)
    alter("update-disable-denied", text("updateDiagnostic"), false, false)
    alter("denied-enable-denied", text("deniedDiagnostic"), true, false)
    alter("denied-disable-denied", text("deniedDiagnostic"), false, false)
    ordinary("baseline")
    checkpoint("baseline")
    checkpoint("grant", applyChange = true)
    alter("enabled", text("target"), true, true)
    ordinary("enabled")
    checkpoint("enabled")
    append("enabled-append")
    checkpoint("enabled-append")
    checkpoint("revoke", applyChange = true)
    alter("revoked-disable-denied", text("target"), false, false)
    ordinary("revoked")
    checkpoint("revoked")
    checkpoint("regrant", applyChange = true)
    alter("disabled", text("target"), false, true)
    ordinary("disabled")
    checkpoint("disabled")
    append("disabled-append")
    checkpoint("disabled-append")
    writeJson(
      output.resolve("cdf-result.json"),
      Map(
        "completed" -> true,
        "sameJvmRevoke" -> true,
        "finalVersion" -> head(text("target"))
      ).asJava
    )
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
      val copy = node.deepCopy[com.fasterxml.jackson.databind.node.ObjectNode]()
      copy.remove("policyUpdateTime"); copy.remove("roleUpdateTime"); copy
    }
    require(
      stable(actual) == stable(expected),
      "Real downloaded content differs from reviewed Ranger state"
    )
  }

  private[ranger] def failureEvidence(error: Throwable): JavaList[JavaMap[String, Object]] = {
    val seen = new IdentityHashMap[Throwable, java.lang.Boolean]()
    val values = scala.collection.mutable.ArrayBuffer.empty[JavaMap[String, Object]]
    def visit(cause: Throwable, relation: String): Unit = {
      if (cause != null && !seen.containsKey(cause)) {
        seen.put(cause, true)
        values += Map[String, Object](
          "class" -> cause.getClass.getName,
          "relation" -> relation,
          "stack" -> cause.getStackTrace.map(_.toString).toVector.asJava
        ).asJava
        visit(cause.getCause, "cause")
        cause.getSuppressed.foreach(visit(_, "suppressed"))
      }
    }
    visit(error, "original")
    values.toVector.asJava
  }

  private[ranger] def verifyWriterOutcome(error: Throwable, phase: String, output: Path): Unit = {
    if (error != null) writeJson(output.resolve("writer-failure.json"), failureEvidence(error))
    if (phase == "granted-append") require(error == null, "Public writer failed")
    else require(exactDenial(error, phase == "after-helper-revoked"), "Unexpected writer failure")
  }

  private[ranger] def exactDenial(error: Throwable, afterHelper: Boolean): Boolean = {
    if (error == null) return false
    val seen = new IdentityHashMap[Throwable, java.lang.Boolean]()
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(cause => cause != null && seen.put(cause, true) == null)
      .exists { cause =>
        val expected =
          if (afterHelper) "Delta Ranger publication changed; reanalyze the query"
          else "Delta Ranger access denied"
        val method = if (afterHelper) "requireSnapshot" else "authorize"
        cause.getClass == classOf[
          SecurityException
        ] && cause.getMessage == expected && cause.getStackTrace.headOption.exists(f =>
          f.getClassName == classOf[DeltaRangerPolicyEngine].getName && f.getMethodName == method
        ) &&
        (cause.getStackTrace.exists(f =>
          f.getClassName == "org.apache.gravitino.spark.connector.delta.ranger.DeltaRangerRegistrationBindings$BoundAppendTransaction" &&
            f.getMethodName == (if (afterHelper) "requireContext" else "<init>")
        ) || (!afterHelper && {
          val prefix = "org.apache.gravitino.spark.connector.delta.ranger."
          val receiptCapture = Vector(
            prefix + "DeltaRangerPolicyEngine" -> "authorize",
            prefix + "DeltaRangerAppendTargetReceipt" -> "validate",
            prefix + "DeltaRangerAppendTargetReceipt$" -> "capture",
            prefix + "DeltaRangerAppendCommandBinding$" -> "capture",
            prefix + "DeltaRangerAppendV1Input$Owner" -> "<init>"
          )
          cause.getStackTrace
            .take(5)
            .map(f => f.getClassName -> f.getMethodName)
            .toVector == receiptCapture
        }))
      }
  }

  private def captureProfile(
      spark: SparkSession,
      builder: String,
      wrapper: Class[_],
      output: Path
  ): Unit = {
    val state = spark.sessionState
    val analyzer = state.analyzer
    val inventories = Map(
      "resolution" -> analyzer.extendedResolutionRules.map(_.getClass).toVector,
      "postHoc" -> analyzer.postHocResolutionRules.map(_.getClass).toVector,
      "checks" -> analyzer.extendedCheckRules.map(_.getClass).toVector,
      "normalization" -> state.planNormalizationRules.map(_.getClass).toVector
    )
    val batches = analyzer.batches.map(b =>
      Map[String, Any](
        "name" -> b.name,
        "strategy" -> b.strategy.getClass.getName,
        "maxIterations" -> b.strategy.maxIterations,
        "rules" -> b.rules.map(_.getClass.getName).asJava
      ).asJava
    )
    val classes = (inventories.values.flatten ++ analyzer.batches.flatMap(b =>
      b.rules.map(rule => rule.getClass: Class[_]) :+ (b.strategy.getClass: Class[_])
    ) ++ Seq(wrapper, classOf[DeltaRangerSparkExtension])).toVector.distinct
    val origins = classes.map { cls =>
      val jar = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
      val stream = cls.getClassLoader.getResourceAsStream(cls.getName.replace('.', '/') + ".class")
      require(stream != null, "Missing observed class bytes")
      val digest = MessageDigest.getInstance("SHA-256")
      try {
        val bytes = new Array[Byte](8192); var size = stream.read(bytes)
        while (size >= 0) { digest.update(bytes, 0, size); size = stream.read(bytes) }
      } finally stream.close()
      cls.getName -> Map[String, Any](
        "jar" -> jar.toString,
        "jarSha256" -> sha(jar),
        "classSha256" -> digest.digest().map(b => f"${b & 0xff}%02x").mkString,
        "sameLoader" -> (cls.getClassLoader eq getClass.getClassLoader)
      ).asJava
    }.toMap
    writeJson(
      output.resolve("runtime-profile.json"),
      (Map[String, Any](
        "builderExtensions" -> builder,
        "contextExtensions" -> spark.sparkContext.getConf.get("spark.sql.extensions"),
        "sessionExtensions" -> spark.conf.get("spark.sql.extensions"),
        "sqlConfExtensions" -> state.conf.getConfString("spark.sql.extensions"),
        "wrapperClass" -> wrapper.getName,
        "batches" -> batches.asJava,
        "classOrigins" -> origins.asJava
      ) ++ inventories.map { case (k, v) => k -> v.map(_.getName).asJava }).asJava
    )
  }

  private def checkpoint(control: Path, name: String): Unit = {
    writeJson(
      control.resolve(name + "-ready.json"),
      Map("ready" -> true, "checkpoint" -> name).asJava
    )
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180)
    val release = control.resolve(name + "-verified.json")
    while (!Files.exists(release) && System.nanoTime() < deadline) Thread.sleep(25)
    require(Files.exists(release), "Supervisor checkpoint did not complete")
    val value = json.readTree(Files.readAllBytes(release))
    require(
      value.path("verified").asBoolean() && value.path("checkpoint").asText() == name,
      "Supervisor checkpoint rejected"
    )
  }

  private[ranger] def warmCaches(
      spark: SparkSession,
      location: String,
      order: String,
      output: Path,
      observer: WarmObserver
  ): (SparkSession, Vector[DataFrame]) = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    val constructor =
      classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
    constructor.setAccessible(true)
    val native = constructor
      .newInstance(
        spark.sparkContext,
        Some(spark.sharedState),
        None,
        extensions,
        Map.empty[String, String]
      )
      .asInstanceOf[SparkSession]
    require(native.sharedState eq spark.sharedState, "Native cache domain differs")
    SparkSession.setActiveSession(native)
    try {
      val target = native.read.format("delta").load(location)
      val project = target.select("id", "region")
      val frames = Vector(target, project)
      val ordered = if (order == "target-first") frames else frames.reverse
      ordered.foreach(frame => { frame.cache(); frame.collect() })
      val unrelated = native.range(4).toDF().cache()
      require(unrelated.collect().length == 4, "Unrelated cache warmup differs")
      val observed = frames :+ unrelated
      observer.record(spark, observed, output.resolve("warm-before.json"), "not-run")
      (native, observed)
    } finally SparkSession.setActiveSession(spark)
  }

  private[ranger] final class WarmObserver(order: String) {
    private val identities = new IdentityHashMap[AnyRef, String]()
    private var previous = Vector.empty[CachedData]
    private var originalLog: DeltaLog = null
    private def id(value: AnyRef): String = {
      if (!identities.containsKey(value)) identities.put(value, "object-" + identities.size())
      identities.get(value)
    }
    def record(
        spark: SparkSession,
        frames: Vector[DataFrame],
        output: Path,
        outcome: String
    ): Unit = {
      require(frames.size == 3, "Missing unrelated cache fixture")
      val manager = spark.sharedState.cacheManager
      val observed = frames.map(frame => manager.lookupCachedData(frame).get)
      val entries = observed.take(2)
      val unrelated = observed(2)
      if (originalLog == null) {
        originalLog = frames.head.queryExecution.analyzed.collectFirst {
          case relation: LogicalRelation if relation.relation.isInstanceOf[HadoopFsRelation] =>
            relation.relation
              .asInstanceOf[HadoopFsRelation]
              .location
              .asInstanceOf[TahoeLogFileIndex]
              .deltaLog
        }.get
      }
      val head = originalLog.unsafeVolatileSnapshot
      val cacheField = manager.getClass.getDeclaredField("cachedData")
      cacheField.setAccessible(true)
      val installed = cacheField.get(manager).asInstanceOf[Seq[CachedData]].toVector
      val roles = Vector("target", "project", "unrelated")
      val installedOrder = installed.flatMap(data =>
        observed.zipWithIndex.collectFirst {
          case (entry, index) if entry.plan eq data.plan => roles(index)
        }
      )
      require(installedOrder.size == 3, "Observed cache key missing or duplicated")
      val oldUnloaded =
        previous.map(entry => !entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
      val rows = entries.map { data =>
        val builder = data.cachedRepresentation.cacheBuilder
        val producer = builder.cachedPlan
        val owner = producer match {
          case raw: DeltaRangerRawProducer => Some(raw.owner); case _ => None
        }
        val dependency = owner.flatMap { value =>
          val field = value.getClass.getDeclaredField("dependency")
          field.setAccessible(true)
          field.get(value).asInstanceOf[Option[DeltaRangerRawProducer]]
        }
        Map[String, Any](
          "key" -> id(data.plan),
          "builder" -> id(builder),
          "producer" -> id(producer),
          "producerClass" -> producer.getClass.getName,
          "loaded" -> builder.isCachedColumnBuffersLoaded,
          "owner" -> owner.map(id).orNull,
          "ownerKey" -> owner.map(v => id(v.key)).orNull,
          "selectedHead" -> owner.map(v => id(v.observation.selectedHead)).orNull,
          "selectedVersion" -> owner.map(_.observation.installedVersion).getOrElse(-1L),
          "ownerDependency" -> dependency.map(id).orNull,
          "dependencies" -> producer.collect { case scan: InMemoryTableScanExec =>
            val b = scan.relation.cacheBuilder
            val d = b.cachedPlan match {
              case raw: DeltaRangerRawProducer => Some(raw.owner); case _ => None
            }
            Map(
              "builder" -> id(b),
              "producer" -> id(b.cachedPlan),
              "owner" -> d.map(id).orNull
            ).asJava
          }.asJava
        ).asJava
      }
      writeJson(
        output,
        Map[String, Any](
          "order" -> order,
          "roles" -> Vector("target", "project").asJava,
          "entries" -> rows.asJava,
          "installedOrder" -> installedOrder.asJava,
          "matchingOrder" -> installedOrder.filterNot(_ == "unrelated").asJava,
          "observedHead" -> id(head),
          "observedVersion" -> head.version,
          "unrelated" -> Map[String, Any](
            "key" -> id(unrelated.plan),
            "builder" -> id(unrelated.cachedRepresentation.cacheBuilder),
            "producer" -> id(unrelated.cachedRepresentation.cacheBuilder.cachedPlan),
            "loaded" -> unrelated.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded
          ).asJava,
          "oldBuffersUnloaded" -> oldUnloaded.asJava,
          "outerOutcome" -> outcome,
          "nativeOuterCacheOutcome" -> "unobserved",
          "aqe" -> spark.sessionState.conf.adaptiveExecutionEnabled,
          "asyncStaleness" -> spark.conf
            .get("spark.databricks.delta.asyncUpdate.stalenessTimeLimit", "0")
            .toLong
        ).asJava
      )
      previous = entries
    }
  }

  private[ranger] def withCdfAppendTransaction[T](spark: SparkSession)(action: => T): T = {
    val keys = Seq(
      "spark.databricks.delta.write.txnAppId",
      "spark.databricks.delta.write.txnVersion",
      "spark.databricks.delta.write.txnVersion.autoReset.enabled"
    )
    val previous = keys.map(spark.conf.getOption)
    val active = SparkSession.getActiveSession
    SparkSession.setActiveSession(spark)
    spark.conf.set(keys(0), "cdf-live-" + UUID.randomUUID().toString)
    spark.conf.set(keys(1), "1")
    spark.conf.set(keys(2), "false")
    try action
    finally {
      keys.zip(previous).foreach { case (key, value) =>
        value match {
          case Some(old) => spark.conf.set(key, old)
          case None      => spark.conf.unset(key)
        }
      }
      active match {
        case Some(session) => SparkSession.setActiveSession(session)
        case None          => SparkSession.clearActiveSession()
      }
    }
  }

  private[ranger] def governedRowImage(frame: DataFrame): JsonNode = rowListsToJson(
    frame
      .collect()
      .toVector
      .sortBy(row => (row.getLong(0), Option(row.getString(1)), Option(row.getString(2))))
      .map(_.toSeq.asJava)
      .asJava
  )

  private def rowImage(frame: DataFrame): JsonNode = rowListsToJson(
    frame.orderBy("id", "secret", "region").collect().toVector.map(_.toSeq.asJava).asJava
  )

  /** Convert already ordered, fixed-shape collected rows to their JSON image. */
  private[ranger] def rowListsToJson(rows: JavaList[JavaList[Any]]): JsonNode =
    json.readTree(json.writeValueAsBytes(rows))

  private def writeJson(path: Path, value: Any): Unit = {
    require(!Files.exists(path), "Evidence file already exists");
    json.writeValue(path.toFile, value)
  }

  private def nonnegative(value: JsonNode): Unit =
    require(
      value.isIntegralNumber && value.canConvertToLong && value.longValue() >= 0,
      "Invalid nonnegative integer"
    )
}
