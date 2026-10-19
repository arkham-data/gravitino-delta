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

import java.net.InetSocketAddress
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import com.sun.net.httpserver.HttpServer
import scala.collection.JavaConverters._
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.gravitino.authorization.Privilege
import org.apache.gravitino.client.ObjectMapperProvider
import org.apache.gravitino.dto.{AuditDTO, CatalogDTO, MetalakeDTO, VersionDTO}
import org.apache.gravitino.dto.responses.{CatalogListResponse, MetalakeResponse, VersionResponse}
import org.apache.gravitino.exceptions.ForbiddenException
import org.apache.gravitino.spark.connector.authorization.{
  AuthorizationTable,
  GravitinoAuthorizationSparkSessionExtensions,
  RequiredPrivilegesCheck
}
import org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Exact installed inventories, including the real public discovery construction. */
class TestDeltaRangerAppendInventory {
  private val pair = classOf[DeltaRangerSparkExtension].getName + "," +
    classOf[DeltaSparkSessionExtension].getName
  private val authorization = classOf[GravitinoAuthorizationSparkSessionExtensions].getName

  /** Discovery must qualify all analyzer, physical and settings checks without removing auth. */
  @Test
  def publicDiscoveryQualifiesWholeInventory(): Unit = withDiscovery { spark =>
    val settings = spark.sessionState.conf.getAllConfs.toMap
    assertDiscovery(spark)
    DeltaRangerAppendInventory.qualify(spark)
    assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
    assertDiscovery(spark)
  }

  /** The direct pair remains a separately qualified eleven-rule profile. */
  @Test
  def directPairQualifiesWholeInventory(): Unit =
    withSession(builder => builder.config("spark.sql.extensions", pair)) { spark =>
      assertEquals(pair, spark.sparkContext.getConf.get("spark.sql.extensions"))
      assertEquals(pair, spark.conf.get("spark.sql.extensions"))
      assertEquals(11, spark.sessionState.analyzer.postHocResolutionRules.size)
      assertFalse(
        spark.sessionState.analyzer.postHocResolutionRules
          .exists(_.getClass == classOf[RequiredPrivilegesCheck])
      )
      val settings = spark.sessionState.conf.getAllConfs.toMap
      DeltaRangerAppendInventory.qualify(spark)
      assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
      record(spark, "direct")
    }

  /** Qualification must not consume a denial; the installed rule still drains and throws it. */
  @Test
  def installedAuthorizationPreservesIdentityAndDenial(): Unit = withDiscovery { spark =>
    assertDiscovery(spark)
    val rule = spark.sessionState.analyzer.postHocResolutionRules(7)
    assertEquals(classOf[RequiredPrivilegesCheck], rule.getClass)
    val plan = spark.sessionState.sqlParser.parsePlan("SELECT 1")
    val tag = TreeNodeTag[String]("inventory-test-identity")
    plan.setTagValue(tag, "retained")
    try {
      AuthorizationTable.clear()
      assertSame(plan, rule(plan))
      assertEquals(Some("retained"), plan.getTagValue(tag))
      AuthorizationTable.deny(
        "t",
        "catalog.db.t",
        java.util.Collections.singleton(Privilege.Name.MODIFY_TABLE),
        new ForbiddenException("seeded denial")
      )
      DeltaRangerAppendInventory.qualify(spark)
      val denied = assertThrows(classOf[ForbiddenException], () => rule(plan))
      assertEquals(
        "Missing required privileges for Spark tables: [catalog.db.t: MODIFY_TABLE]",
        denied.getMessage
      )
      assertSame(plan, rule(plan))
      assertEquals(Some("retained"), plan.getTagValue(tag))
    } finally AuthorizationTable.clear()
  }

  /** A foreign extra rule cannot borrow the production authorization profile. */
  @Test
  def extraPostHocRuleIsRejected(): Unit = negative(
    "extra",
    extensions => {
      installAuthorization(extensions)
      installNative(extensions)
      extensions.injectPostHocResolutionRule(_ => new InventoryForeignPostHoc)
    },
    classes => {
      assertEquals(13, classes.size)
      assertEquals(classOf[RequiredPrivilegesCheck], classes(7))
      assertEquals(classOf[InventoryForeignPostHoc], classes.last)
    }
  )

  /** Duplicate real rules must remain visible and rejected, without config deduplication. */
  @Test
  def duplicateAuthorizationIsRejected(): Unit = negative(
    "duplicate",
    extensions => {
      installAuthorization(extensions)
      installAuthorization(extensions)
      installNative(extensions)
    },
    classes => {
      assertEquals(13, classes.size)
      assertEquals(
        Vector(classOf[RequiredPrivilegesCheck], classOf[RequiredPrivilegesCheck]),
        classes.slice(7, 9)
      )
    }
  )

  /** An exact authorization rule after Delta is still an unqualified order. */
  @Test
  def reorderedAuthorizationIsRejected(): Unit = negative(
    "reordered",
    extensions => {
      installNative(extensions)
      installAuthorization(extensions)
    },
    classes => {
      assertEquals(12, classes.size)
      assertEquals(classOf[RequiredPrivilegesCheck], classes.last)
      assertEquals("org.apache.spark.sql.delta.PreprocessTableUpdate", classes(7).getName)
    }
  )

  /** A subclass at the correct position does not satisfy trusted Class identity. */
  @Test
  def authorizationSubclassIsRejected(): Unit = negative(
    "subclass",
    extensions => {
      extensions.injectPostHocResolutionRule(_ => new InventoryAuthorizationSubclass)
      installNative(extensions)
    },
    classes => {
      assertEquals(12, classes.size)
      assertEquals(classOf[InventoryAuthorizationSubclass], classes(7))
      assertFalse(classes.contains(classOf[RequiredPrivilegesCheck]))
    }
  )

  private def installAuthorization(extensions: SparkSessionExtensions): Unit = {
    new GravitinoAuthorizationSparkSessionExtensions().apply(extensions)
    ()
  }

  private def installNative(extensions: SparkSessionExtensions): Unit = {
    new DeltaRangerSparkExtension((_, _) =>
      throw new AssertionError("inventory-only session loaded a table")
    )(extensions)
    new DeltaSparkSessionExtension().apply(extensions)
  }

  private def negative(
      name: String,
      install: SparkSessionExtensions => Unit,
      inspect: Vector[Class[_]] => Unit
  ): Unit =
    withSession(builder => builder.withExtensions(install)) { spark =>
      inspect(spark.sessionState.analyzer.postHocResolutionRules.map(_.getClass).toVector)
      val denied =
        assertThrows(classOf[SecurityException], () => DeltaRangerAppendInventory.qualify(spark))
      assertEquals("Delta append inventory rejected: post-hoc rules", denied.getMessage)
      record(spark, name)
    }

  private def assertDiscovery(spark: SparkSession): Unit = {
    assertEquals(authorization + "," + pair, spark.sparkContext.getConf.get("spark.sql.extensions"))
    assertEquals(pair, spark.conf.get("spark.sql.extensions"))
    assertEquals(pair, spark.sessionState.conf.getConfString("spark.sql.extensions"))
    val rules = spark.sessionState.analyzer.postHocResolutionRules
    assertEquals(12, rules.size)
    assertEquals(classOf[RequiredPrivilegesCheck], rules(7).getClass)
    assertEquals(1, rules.count(_.getClass == classOf[RequiredPrivilegesCheck]))
    assertEquals(
      "org.apache.spark.sql.catalyst.analysis.ReplaceCharWithVarchar$",
      rules(6).getClass.getName
    )
    assertEquals("org.apache.spark.sql.delta.PreprocessTableUpdate", rules(8).getClass.getName)
  }

  private def withDiscovery(test: SparkSession => Unit): Unit = {
    val fixture = new InventoryDiscoveryFixture
    try {
      withSession(builder => {
        // The original builder pair is retained even though plugin initialization updates SparkConf.
        val initial = new SparkConf(false).set("spark.sql.extensions", pair)
        assertEquals(pair, initial.get("spark.sql.extensions"))
        builder
          .config(initial)
          .config("spark.plugins", classOf[GravitinoSparkPlugin].getName)
          .config("spark.sql.gravitino.uri", fixture.uri)
          .config("spark.sql.gravitino.metalake", "lake")
          .config("spark.gravitino.enableDeltaSupport", "true")
          .config("spark.gravitino.enableDeltaRangerSupport", "true")
      }) { spark =>
        assertTrue(fixture.requests.contains("/api/version"))
        assertTrue(fixture.requests.contains("/api/metalakes/lake"))
        assertTrue(fixture.requests.contains("/api/metalakes/lake/catalogs"))
        assertDiscovery(spark)
        record(spark, "discovery")
        test(spark)
      }
    } finally fixture.close()
  }

  private def withSession(
      configure: SparkSession.Builder => SparkSession.Builder
  )(test: SparkSession => Unit): Unit = {
    assertTrue(SparkSession.getActiveSession.isEmpty, "fresh active session required")
    assertTrue(SparkSession.getDefaultSession.isEmpty, "fresh default session required")
    val directory = Files.createTempDirectory("delta-inventory")
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName("delta-inventory")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", directory.resolve("warehouse").toString)
    val spark = configure(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try test(spark)
    finally {
      AuthorizationTable.clear()
      DeltaLog.clearCache()
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def record(spark: SparkSession, profile: String): Unit = {
    assertEquals("17", System.getProperty("java.specification.version"))
    Option(System.getProperty("bare.source.spark")).foreach(expected =>
      assertEquals(expected, spark.version)
    )
    val delta = Option(System.getProperty("bare.source.delta"))
    val origins = Seq(
      classOf[SparkSession],
      classOf[DeltaLog],
      classOf[Option[_]],
      classOf[RequiredPrivilegesCheck]
    ).map { cls =>
      val path = Paths.get(cls.getProtectionDomain.getCodeSource.getLocation.toURI)
      val hash =
        if (Files.isRegularFile(path))
          MessageDigest
            .getInstance("SHA-256")
            .digest(Files.readAllBytes(path))
            .map(b => f"${b & 0xff}%02x")
            .mkString
        else "class-directory"
      Map("class" -> cls.getName, "origin" -> path.toString, "sha256" -> hash).asJava
    }
    assertTrue(origins(0).get("origin").endsWith(s"spark-sql_2.12-${spark.version}.jar"))
    delta.foreach(expected =>
      assertTrue(origins(1).get("origin").endsWith(s"delta-spark_2.12-$expected.jar"))
    )
    assertTrue(origins(2).get("origin").endsWith("scala-library-2.12.21.jar"))
    Option(System.getProperty("inventory.loaded.directory")).foreach { output =>
      val file =
        Paths.get(output).resolve(profile + "-" + java.util.UUID.randomUUID().toString + ".json")
      Files.createDirectories(file.getParent)
      Files.write(
        file,
        ObjectMapperProvider
          .objectMapper()
          .writeValueAsBytes(
            Map(
              "profile" -> profile,
              "builderExtensions" -> (if (profile == "discovery" || profile == "direct") pair
                                      else ""),
              "contextExtensions" -> spark.sparkContext.getConf.get("spark.sql.extensions", ""),
              "sessionExtensions" -> spark.conf.get("spark.sql.extensions", ""),
              "sqlConfExtensions" -> spark.sessionState.conf
                .getConfString("spark.sql.extensions", ""),
              "postHocClasses" -> spark.sessionState.analyzer.postHocResolutionRules
                .map(_.getClass.getName)
                .asJava,
              "analyzerBatchCount" -> Int.box(spark.sessionState.analyzer.batches.size),
              "loaded" -> origins.asJava
            ).asJava
          )
      )
    }
  }
}

/** Foreign post-hoc class used through Spark's supported injection API. */
class InventoryForeignPostHoc extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

/** A derived authorization class must not qualify as the trusted production class. */
class InventoryAuthorizationSubclass extends RequiredPrivilegesCheck

/** In-process discovery transport for the public plugin and real Gravitino client. */
private[ranger] final class InventoryDiscoveryFixture extends AutoCloseable {
  val requests = new ConcurrentLinkedQueue[String]()
  private val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
  private val mapper = ObjectMapperProvider.objectMapper()
  private val metalake = {
    val audit = AuditDTO.builder()
    audit.withCreator("fixture")
    audit.withCreateTime(Instant.EPOCH)
    val builder = MetalakeDTO.builder()
    builder.withName("lake")
    builder.withAudit(audit.build())
    builder.build()
  }
  server.createContext(
    "/",
    exchange => {
      try {
        val path = exchange.getRequestURI.getPath
        requests.add(path)
        val response = path match {
          case "/api/version" =>
            new VersionResponse(new VersionDTO("2.0.0-SNAPSHOT", "fixture", "fixture"))
          case "/api/metalakes/lake"          => new MetalakeResponse(metalake)
          case "/api/metalakes/lake/catalogs" => new CatalogListResponse(Array.empty[CatalogDTO])
          case _ => throw new AssertionError("unexpected discovery request")
        }
        val bytes = mapper.writeValueAsBytes(response)
        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, bytes.length)
        val output = exchange.getResponseBody
        try output.write(bytes)
        finally output.close()
      } finally exchange.close()
    }
  )
  server.start()
  def uri: String = "http://127.0.0.1:" + server.getAddress.getPort
  override def close(): Unit = server.stop(0)
}
