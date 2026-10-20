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

import java.net.{InetSocketAddress, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.time.Duration
import java.util.{Base64, Collections, UUID}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.apache.gravitino.{MetadataObject, MetadataObjects, NameIdentifier}
import org.apache.gravitino.authorization.{Privilege, Privileges}
import org.apache.gravitino.client.GravitinoClient
import org.apache.gravitino.catalog.lakehouse.delta.registration.{
  DeltaRegistrationCommand,
  DeltaRegistrationReceipt
}
import org.apache.gravitino.spark.connector.delta.DeltaRegistrationClient
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableChange}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Assumptions.assumeTrue

/** External-fixture composition of the installed command and an isolated real provider. */
@Tag("gravitino-docker-test")
class TestDeltaRangerCdfProviderComposition {
  @Test
  def commandRefreshPersistsAndRecoversWithoutMutationReplay(): Unit = {
    val fixturePath = System.getProperty("gravitino.delta.test.cdfProviderFixture")
    assumeTrue(fixturePath != null, "Requires the explicitly owned provider fixture")
    val json = new ObjectMapper
    val fixture = json.readTree(Paths.get(fixturePath).toFile)
    def setting(name: String): String = fixture.path(name).asText()
    val output = Paths.get(setting("output"))
    val location = setting("location")
    val targetPath = "/api/metalakes/lake/catalogs/a/schemas/db/tables/t"
    val gateway =
      new CdfRefreshGateway(URI.create(setting("uri")), targetPath, setting("metadataPrincipal"))
    def client(user: String): GravitinoClient = GravitinoClient
      .builder(setting("uri"))
      .withMetalake("lake")
      .withSimpleAuth(user)
      .build()
    val admin = client(setting("serviceAdmin"))
    val actor = client(setting("subject"))
    val policy = new TestDeltaRangerNativeSelect.Fixture
    val metadataConfig = Files.createTempFile(output, "metadata-", ".properties")
    Files.write(
      metadataConfig,
      ("auth-type=simple\nprincipal=" + setting("metadataPrincipal") +
        "\nlifetime-seconds=300\n").getBytes("UTF-8")
    )
    val options = fixture
      .path("sparkOptions")
      .fields()
      .asScala
      .map(e => e.getKey -> e.getValue.asText())
      .toMap ++ Map(
      "spark.sql.gravitino.uri" -> gateway.uri.toString,
      "spark.sql.gravitino.metalake" -> "lake",
      "spark.gravitino.deltaRanger.metadata.config" -> metadataConfig.toString
    )
    val id = Identifier.of(Array("db"), "t")
    val local = NameIdentifier.of("db", "t")
    val objectId = MetadataObjects.parse("a.db.t", MetadataObject.Type.TABLE)
    CdfProviderForwardingCatalog.alterCalls.set(0)
    var wrapper: DeltaRangerCatalogSpark35 = null
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfProviderForwardingCatalog].getName)
          spark.sessionState.conf.setConfString(
            "spark.sql.extensions",
            DeltaRangerCatalogSpark35.EXTENSION + "," + spark.conf.get("spark.sql.extensions")
          )
          val state = new DeltaRangerCatalogSpark35.State(
            new DeltaRangerMetadataClient(),
            () => actor.loadCatalog("a"),
            () => actor.loadCatalog("a"),
            _ => policy.engine
          )
          wrapper = new DeltaRangerCatalogSpark35(state)
          CdfProviderForwardingCatalog.wrapper = wrapper
          wrapper.initialize(
            "a",
            new CaseInsensitiveStringMap(
              Collections.singletonMap("journal-directory", output.resolve("journal").toString)
            )
          )
          val baseline = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
          val original = admin.loadCatalog("a").asTableCatalog().loadTable(local)
          val originalReceipt = DeltaRegistrationReceipt.fromProperties(original.properties())
          val originalColumns =
            original.columns().map(c => (c.name(), c.dataType().toString, c.nullable())).toVector
          val originalOwner = admin.getOwner(objectId).get()
          val origins = json.createObjectNode()
          Seq(
            classOf[DeltaRangerCatalogSpark35],
            classOf[DeltaLog],
            classOf[GravitinoClient],
            Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem"),
            Class.forName("com.amazonaws.services.s3.AmazonS3Client"),
            classOf[scala.Option[_]]
          ).foreach { kind =>
            val entry = origins.putObject(kind.getName)
            entry.put("origin", kind.getProtectionDomain.getCodeSource.getLocation.toString)
            val stream = kind.getResourceAsStream("/" + kind.getName.replace('.', '/') + ".class")
            try entry.put("classSha256", digest(stream.readAllBytes()))
            finally stream.close()
          }
          assertEquals("3.5.4", spark.version)
          assertEquals("3.3.6", org.apache.hadoop.util.VersionInfo.getVersion)
          assertEquals("2.12.21", scala.util.Properties.versionNumberString)
          assertTrue(
            origins
              .path(classOf[DeltaLog].getName)
              .path("origin")
              .asText()
              .endsWith("delta-spark_2.12-3.3.0.jar")
          )
          assertTrue(
            origins
              .path("org.apache.hadoop.fs.s3a.S3AFileSystem")
              .path("origin")
              .asText()
              .endsWith("hadoop-aws-3.3.6.jar")
          )
          assertTrue(
            origins
              .path("com.amazonaws.services.s3.AmazonS3Client")
              .path("origin")
              .asText()
              .endsWith("aws-java-sdk-bundle-1.12.367.jar")
          )
          json.writeValue(output.resolve("origins.json").toFile, origins)
          def observe(label: String, expectedVersion: Long, enabled: Option[String]): Unit = {
            wrapper.loadTable(id)
            val current = admin.loadCatalog("a").asTableCatalog().loadTable(local)
            val receipt = DeltaRegistrationReceipt.fromProperties(current.properties())
            val native = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
            assertEquals(originalReceipt.identity(), receipt.identity())
            assertEquals(originalReceipt.nativeId(), receipt.nativeId())
            assertEquals(originalReceipt.location(), receipt.location())
            assertEquals(expectedVersion, receipt.nativeVersion().getAsLong)
            assertEquals(expectedVersion, native.version)
            assertEquals(baseline.metadata.id, native.metadata.id)
            assertEquals(baseline.metadata.schemaString, native.metadata.schemaString)
            assertEquals(
              originalColumns,
              current.columns().map(c => (c.name(), c.dataType().toString, c.nullable())).toVector
            )
            assertEquals(originalOwner.name(), admin.getOwner(objectId).get().name())
            assertEquals(originalOwner.`type`(), admin.getOwner(objectId).get().`type`())
            assertEquals(enabled, native.metadata.configuration.get("delta.enableChangeDataFeed"))
            enabled.foreach(value =>
              assertEquals(value, current.properties().get("delta.enableChangeDataFeed"))
            )
            val result = json.createObjectNode()
            result.put("nativeVersion", native.version)
            result.put("nativeId", native.metadata.id)
            result.put("nativeSchema", native.metadata.schemaString)
            result.set[JsonNode](
              "nativeProtocol",
              json.readTree(native.protocol.json)
            )
            result.set[JsonNode](
              "nativeConfiguration",
              json.valueToTree(native.metadata.configuration.asJava)
            )
            result.set[JsonNode](
              "receipt",
              json.valueToTree(receipt.toProperties())
            )
            result.put("owner", originalOwner.name())
            result.put("ownerType", originalOwner.`type`().toString)
            result.put("nativeVersionsAdded", native.version - baseline.version)
            result.put("metadataGets", gateway.observed.get())
            result.put("rejectedGets", gateway.rejected.get())
            json.writeValue(output.resolve(label + ".json").toFile, result)
          }
          observe("baseline", baseline.version, None)
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
            .collect()
          observe("enabled", baseline.version + 1, Some("true"))
          val protocol = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.protocol
          assertEquals(
            Set(Row(1L, "a-one-masked", "keep"), Row(3L, null, "keep")),
            spark.sql("SELECT id, secret, region FROM a.db.t").collect().toSet
          )
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')")
            .collect()
          observe("disabled", baseline.version + 2, Some("false"))
          assertEquals(protocol, DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.protocol)
          val committedPath =
            new Path(new Path(location, "_delta_log"), f"${baseline.version + 3}%020d.json")
          val filesystem = committedPath.getFileSystem(spark.sessionState.newHadoopConf())
          assertFalse(filesystem.exists(committedPath))
          val beforeFault = manifest(spark, location)
          val callsBeforeFault = CdfProviderForwardingCatalog.alterCalls.get()
          gateway.publication = () => filesystem.exists(committedPath)
          gateway.armed = true
          val failure = assertThrows(
            classOf[Exception],
            () => {
              spark
                .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
                .collect(); ()
            }
          )
          assertTrue(
            Iterator
              .iterate[Throwable](failure)(_.getCause)
              .takeWhile(_ != null)
              .exists(e =>
                Option(e.getMessage).exists(_.startsWith("CDF property change committed;"))
              )
          )
          assertTrue(
            Iterator
              .iterate[Throwable](failure)(_.getCause)
              .takeWhile(_ != null)
              .exists(e =>
                Option(e.getMessage)
                  .exists(_.contains("named refresh is unconfirmed; do not retry ALTER"))
              )
          )
          assertEquals(callsBeforeFault + 1, CdfProviderForwardingCatalog.alterCalls.get())
          assertTrue(filesystem.exists(committedPath))
          assertFalse(
            filesystem.exists(
              new Path(committedPath.getParent, f"${baseline.version + 4}%020d.json")
            )
          )
          assertTrue(gateway.rejected.get() > 0)
          val cachedVersionAtFailure =
            DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version
          assertTrue(
            Set(baseline.version + 2, baseline.version + 3).contains(cachedVersionAtFailure)
          )
          val afterFault = manifest(spark, location)
          val commitName = "_delta_log/" + committedPath.getName
          assertTrue(afterFault.contains(commitName))
          assertTrue(beforeFault.forall { case (path, hash) =>
            afterFault.get(path).contains(hash)
          })
          val commit = filesystem.open(committedPath)
          val commitBytes =
            try commit.readAllBytes()
            finally commit.close()
          Files.write(output.resolve("fault-commit.jsonl"), commitBytes)
          val commitLines = new String(commitBytes, "UTF-8").split("\n").toVector
          val actions = commitLines.filter(_.nonEmpty).map(line => json.readTree(line))
          assertEquals(1, actions.count(_.has("commitInfo")))
          assertEquals(
            baseline.version + 2,
            actions.find(_.has("commitInfo")).get.path("commitInfo").path("readVersion").asLong()
          )
          assertTrue(actions.exists(_.path("metaData").path("id").asText() == baseline.metadata.id))
          assertTrue(
            actions.exists(
              _.path("metaData")
                .path("configuration")
                .path("delta.enableChangeDataFeed")
                .asText() == "true"
            )
          )
          val fault = json.createObjectNode()
          fault.put("committed", true)
          fault.put("refreshUnconfirmed", true)
          fault.put("cachedNativeVersionAtFailure", cachedVersionAtFailure)
          fault.put(
            "outerAlterCalls",
            CdfProviderForwardingCatalog.alterCalls.get() - callsBeforeFault
          )
          fault.put("nativeInvocationInstrumented", false)
          fault.put("firstRejectionPhase", "native-commit-file-visible; native-return-not-observed")
          fault.put("nativeCommit", commitName)
          fault.put("nativeCommitSha256", afterFault(commitName))
          fault.set[JsonNode]("http", json.valueToTree(gateway.events.asScala.toVector.asJava))
          json.writeValue(output.resolve("committed-unconfirmed.json").toFile, fault)
          json.writeValue(
            output.resolve("native-manifest-before-fault.json").toFile,
            beforeFault.asJava
          )
          json.writeValue(
            output.resolve("native-manifest-after-fault.json").toFile,
            afterFault.asJava
          )
          gateway.armed = false
          observe("recovered", baseline.version + 3, Some("true"))
          assertTrue(filesystem.exists(committedPath))
          assertFalse(
            filesystem.exists(
              new Path(committedPath.getParent, f"${baseline.version + 4}%020d.json")
            )
          )
          assertEquals(2, spark.sql("SELECT id, secret, region FROM a.db.t").collect().length)
          assertEquals(
            baseline.version + 3,
            DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version
          )
          assertEquals(
            afterFault,
            manifest(spark, location),
            "Observation recovery must not change native files"
          )
          assertEquals(callsBeforeFault + 1, CdfProviderForwardingCatalog.alterCalls.get())
          json.writeValue(
            output.resolve("native-manifest-after-recovery.json").toFile,
            afterFault.asJava
          )
        },
        Some(extensions =>
          new DeltaRangerSparkExtension((_, _) => CdfProviderForwardingCatalog.wrapper)(extensions)
        ),
        options,
        native => {
          val conf = native.sessionState.newHadoopConf()
          val source = new Path(NativeSelectFixture.locations("a"))
          val destination = new Path(location)
          assertFalse(destination.getFileSystem(conf).exists(destination))
          assertTrue(
            FileUtil.copy(
              source.getFileSystem(conf),
              source,
              destination.getFileSystem(conf),
              destination,
              false,
              conf
            )
          )
          val registration =
            new DeltaRegistrationClient(() => admin.loadCatalog("a"), () => admin.loadCatalog("a"))
          val adopted = registration.submit(
            id,
            DeltaRegistrationCommand.adopt(
              UUID.randomUUID().toString,
              location,
              UUID.randomUUID().toString,
              "owned-native-seed-completed"
            )
          )
          registration.submit(id, DeltaRegistrationCommand.publish(adopted.identity()))
          Seq("app", "metadata").foreach(role =>
            admin.grantPrivilegesToRole(
              role,
              objectId,
              Collections.singleton[Privilege](Privileges.SelectTable.allow())
            )
          )
          NativeSelectFixture.locations = NativeSelectFixture.locations.updated("a", location)
        }
      )
    finally {
      if (wrapper != null) wrapper.close()
      CdfProviderForwardingCatalog.wrapper = null
      policy.engine.close(); actor.close(); admin.close(); gateway.close()
    }
  }

  private def digest(bytes: Array[Byte]): String = MessageDigest
    .getInstance("SHA-256")
    .digest(bytes)
    .map(value => f"${value & 0xff}%02x")
    .mkString

  private def manifest(spark: SparkSession, location: String): Map[String, String] = {
    val root = new Path(location)
    val filesystem = root.getFileSystem(spark.sessionState.newHadoopConf())
    val files = filesystem.listFiles(root, true)
    val result = scala.collection.mutable.Map.empty[String, String]
    while (files.hasNext) {
      val path = files.next().getPath
      val stream = filesystem.open(path)
      try result.put(root.toUri.relativize(path.toUri).getPath, digest(stream.readAllBytes()))
      finally stream.close()
    }
    result.toMap
  }
}

/** Named test transport; metadata and mutation both use the actual production wrapper. */
class CdfProviderForwardingCatalog extends NativeSelectTestCatalog {
  override def loadTable(id: Identifier): Table = CdfProviderForwardingCatalog.wrapper.loadTable(id)
  override def alterTable(id: Identifier, changes: TableChange*): Table = {
    CdfProviderForwardingCatalog.alterCalls.incrementAndGet()
    CdfProviderForwardingCatalog.wrapper.alterTable(id, changes: _*)
  }
}
private[ranger] object CdfProviderForwardingCatalog {
  var wrapper: DeltaRangerCatalogSpark35 = _
  val alterCalls = new AtomicInteger
}

/** Keeps the exact metadata reload fault closed across retries until explicit observation recovery.
  */
private[ranger] final class CdfRefreshGateway(upstream: URI, target: String, principal: String)
    extends AutoCloseable {
  @volatile var armed = false
  @volatile var publication: () => Boolean = () => false
  val observed = new AtomicInteger
  val rejected = new AtomicInteger
  val events = new ConcurrentLinkedQueue[String]
  private val hop =
    Set("host", "connection", "content-length", "transfer-encoding", "upgrade", "expect")
  private val client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  server.createContext("/", (exchange: HttpExchange) => forward(exchange))
  server.start()
  val uri: URI = URI.create("http://127.0.0.1:" + server.getAddress.getPort)
  private def forward(exchange: HttpExchange): Unit = try {
    val matching = exchange.getRequestMethod == "GET" && exchange.getRequestURI.getRawPath == target
    if (matching) {
      val header = Option(exchange.getRequestHeaders.getFirst("Authorization")).getOrElse("")
      require(header.startsWith("Basic "))
      require(
        new String(Base64.getDecoder.decode(header.substring(6)), "UTF-8")
          .takeWhile(_ != ':') == principal
      )
      observed.incrementAndGet()
      if (armed && publication()) {
        rejected.incrementAndGet()
        events.add(
          "GET " + target + " principal=" + principal + " status=503 forwarded=false nativeCommitVisible=true"
        )
        exchange.sendResponseHeaders(503, -1); return
      }
    }
    val request = HttpRequest
      .newBuilder(upstream.resolve(exchange.getRequestURI))
      .timeout(Duration.ofSeconds(20))
    exchange.getRequestHeaders.asScala.foreach { case (name, values) =>
      if (!hop(name.toLowerCase(java.util.Locale.ROOT)))
        values.asScala.foreach(v => request.header(name, v))
    }
    val body = exchange.getRequestBody.readAllBytes()
    request.method(
      exchange.getRequestMethod,
      if (body.isEmpty) HttpRequest.BodyPublishers.noBody()
      else HttpRequest.BodyPublishers.ofByteArray(body)
    )
    val response = client.send(request.build(), HttpResponse.BodyHandlers.ofByteArray())
    if (matching)
      events.add(
        "GET " + target + " principal=" + principal + " status=" + response
          .statusCode() + " forwarded=true"
      )
    response.headers().map().asScala.foreach { case (name, values) =>
      if (!hop(name.toLowerCase(java.util.Locale.ROOT)))
        values.asScala.foreach(v => exchange.getResponseHeaders.add(name, v))
    }
    exchange.sendResponseHeaders(
      response.statusCode(),
      if (response.body().isEmpty) -1L else response.body().length.toLong
    )
    if (response.body().nonEmpty) exchange.getResponseBody.write(response.body())
  } finally exchange.close()
  override def close(): Unit = server.stop(0)
}
