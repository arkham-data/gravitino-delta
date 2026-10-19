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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => LocalPath, Paths}
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.gravitino.spark.connector.authorization.{
  GravitinoAuthorizationSparkSessionExtensions,
  RequiredPrivilegesCheck
}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

/** Combines native S3 publication with ordinary target-first raw cache reconstruction. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
class TestDeltaRangerCommitPublicationS3 {
  private val mapper = new ObjectMapper
  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = true),
      StructField("secret", StringType, nullable = true),
      StructField("region", StringType, nullable = true)
    )
  )
  private val seedRows = Vector(Row(99L, "sentinel", "keep"))
  private val sourceRows =
    Vector(Row(1L, "one", "keep"), Row(2L, "two", "drop"), Row(3L, null, "keep"))
  private val expected = Vector(Row(1L, "MASKED", "keep"), Row(3L, "MASKED", "keep")) ++ seedRows

  /** Retains the original append outcome and independently checks its native publication. */
  @Test
  def targetFirstAppendAndDuplicate(): Unit = characterize(deployedWarmTopology = false)

  /** Uses the deployed helper's third unrelated cache with the same fixture-backed metadata. */
  @Test
  def deployedWarmTopologyAppendAndDuplicate(): Unit = characterize(deployedWarmTopology = true)

  private def characterize(deployedWarmTopology: Boolean): Unit = {
    val trace = InstalledAppendBoundaryProbe.fresh("s3-publication-target-first")
    val credentials = new Properties
    val input =
      Files.newInputStream(Paths.get(System.getProperty("gravitino.delta.test.s3.credentialsFile")))
    try credentials.load(input)
    finally input.close()
    val storage = Map(
      "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "fs.s3a.endpoint" -> System.getProperty("gravitino.delta.test.s3.endpoint"),
      "fs.s3a.path.style.access" -> "true",
      "fs.s3a.connection.ssl.enabled" -> "false",
      "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
      "fs.s3a.access.key" -> credentials.getProperty("MINIO_ROOT_USER"),
      "fs.s3a.secret.key" -> credentials.getProperty("MINIO_ROOT_PASSWORD")
    )
    assertTrue(storage.values.forall(value => value != null && value.nonEmpty))
    val root = new Path(
      "s3a://" + System.getProperty("gravitino.delta.test.s3.bucket") +
        "/gravitino-publication-s3-warm/" + UUID.randomUUID().toString
    )
    val targetPath = new Path(root, "target")
    val sourcePath = new Path(root, "source")
    Files.write(
      trace.directory.resolve("owned-root.private.txt"),
      root.toString.getBytes(StandardCharsets.UTF_8)
    )
    val rawConfig = trace.directory.resolve("raw-maintenance.properties")
    Files.write(
      rawConfig,
      ("version=1\nprofile=raw-current-head-filter-project-v1\ntable.count=1\n" +
        "table.0.metalake=lake\ntable.0.catalog=a\ntable.0.database=db\ntable.0.table=t\n")
        .getBytes(StandardCharsets.UTF_8)
    )
    Files.setPosixFilePermissions(rawConfig, PosixFilePermissions.fromString("rw-------"))
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    val previousPolicies = NativeSelectFixture.catalogPolicies
    val previousPolicy = NativeSelectFixture.policy
    source.customCatalog = "b"
    source.mask = "'MASKED'"
    source.filter = "region = 'keep'"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val install = NativeSelectFixture.extension(target)
    try
      NativeSelectFixture.withSpark(
        spark => {
          assertEquals("17", System.getProperty("java.specification.version"))
          Option(System.getProperty("bare.source.spark")).foreach(assertEquals(_, spark.version))
          val deltaOrigin = classOf[DeltaLog].getProtectionDomain.getCodeSource.getLocation.toString
          Option(System.getProperty("bare.source.delta")).foreach(version =>
            assertTrue(deltaOrigin.endsWith("delta-spark_2.12-" + version + ".jar"))
          )
          Seq(
            classOf[SparkSession],
            classOf[DeltaLog],
            classOf[DeltaRangerRawDeltaLogCache],
            DeltaRangerRawNativeObservation.getClass,
            DeltaRangerAppendCommandBinding.getClass,
            classOf[TestDeltaRangerCommitPublicationS3]
          ).foreach { clazz =>
            val stream = clazz.getResourceAsStream("/" + clazz.getName.replace('.', '/') + ".class")
            val bytes =
              try stream.readAllBytes()
              finally stream.close()
            trace.record(
              "loaded-class",
              s"class=${clazz.getName} sha256=${sha256(bytes)} origin=${clazz.getProtectionDomain.getCodeSource.getLocation}"
            )
          }
          val current = UserGroupInformation.getCurrentUser()
          assertSame(current, UserGroupInformation.getCurrentUser())
          assertSame(UserGroupInformation.getLoginUser(), current)
          assertFalse(spark.sessionState.conf.adaptiveExecutionEnabled)
          trace.record(
            "runtime",
            s"spark=${spark.version} deltaOrigin=$deltaOrigin java=17 repeatedUgiReference=true loginUgiReference=true subject=${current.getUserName}"
          )
          assertEquals(12, spark.sessionState.analyzer.postHocResolutionRules.size)
          assertEquals(
            classOf[RequiredPrivilegesCheck],
            spark.sessionState.analyzer.postHocResolutionRules(7).getClass
          )
          DeltaRangerAppendInventory.qualify(spark)
          val fs = root.getFileSystem(spark.sessionState.newHadoopConf())
          val targetFixture = NativeSelectFixture.bindingFixture(spark, "a")
          val sourceFixture = NativeSelectFixture.bindingFixture(spark, "b")
          val targetProperties = targetFixture.properties.asScala.toMap
          val sourceProperties = sourceFixture.properties.asScala.toMap
          // Like the accepted local warm fixture, refresh the full receipt from independently read JSON.
          targetFixture.onRead = _ => {
            val actions = observe(fs, targetPath)
            targetFixture.properties
              .put("delta.registration.receipt.nativeVersion", (actions.size - 1).toString)
            targetFixture.properties.put(
              "delta.registration.receipt.nativeId",
              actions.flatten.filter(_.has("metaData")).last.get("metaData").get("id").asText()
            )
          }
          val before = archive(fs, root, trace.directory.resolve("before"))
          val appId = "s3-characterization-" + UUID.randomUUID().toString
          spark.conf.set("spark.databricks.delta.write.txnAppId", appId)
          spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
          val (native, allFrames) = if (deployedWarmTopology) {
            DeltaRangerLiveAppend.warmCaches(
              spark,
              targetPath.toString,
              "target-first",
              trace.directory,
              new DeltaRangerLiveAppend.WarmObserver("target-first")
            )
          } else {
            val session = RawNativeObservationFixture.nativeSession(spark)
            val values = RawNativeObservationFixture.active(session) {
              val raw = session.read.format("delta").load(targetPath.toString)
              val dependent = raw.select("id", "region")
              Vector(raw, dependent).foreach(frame => {
                frame.cache(); assertEquals(1, frame.collect().length)
              })
              Vector(raw, dependent)
            }
            (session, values)
          }
          val frames = allFrames.take(2)
          val unrelated = allFrames
            .drop(2)
            .map(frame => frame -> spark.sharedState.cacheManager.lookupCachedData(frame).get)
          val rawIndex = RawNativeObservationFixture.rawIndex(frames.head)
          assertEquals(
            targetPath.toString,
            targetFixture.properties.get("delta.registration.receipt.location")
          )
          assertEquals("s3a", rawIndex.deltaLog.dataPath.toUri.getScheme)
          assertEquals(fs.makeQualified(targetPath), rawIndex.deltaLog.dataPath)
          assertEquals(
            new Path(fs.makeQualified(targetPath), "_delta_log"),
            rawIndex.deltaLog.logPath
          )
          trace.record(
            "location-profile",
            "receiptRawInputEqual=true logDataPathQualifiedEqual=true schemeS3a=true"
          )
          def entries =
            frames.map(frame => spark.sharedState.cacheManager.lookupCachedData(frame).get)
          var previous = entries
          assertTrue(
            previous.forall(_.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
          )
          trace.record(
            "warm-before",
            previous
              .map(_.cachedRepresentation.cacheBuilder.cachedPlan.getClass.getName)
              .mkString("producers=", ",", " populated=true targetFirst=true")
          )
          def assertRebuilt(): Unit = {
            unrelated.foreach { case (frame, entry) =>
              assertSame(
                entry.cachedRepresentation.cacheBuilder,
                spark.sharedState.cacheManager
                  .lookupCachedData(frame)
                  .get
                  .cachedRepresentation
                  .cacheBuilder
              )
              assertTrue(entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
            }
            entries.zip(previous).foreach { case (entry, old) =>
              assertSame(old.plan, entry.plan)
              assertNotSame(
                old.cachedRepresentation.cacheBuilder,
                entry.cachedRepresentation.cacheBuilder
              )
              assertFalse(old.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertFalse(entry.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded)
              assertTrue(
                entry.cachedRepresentation.cacheBuilder.cachedPlan
                  .isInstanceOf[DeltaRangerRawProducer]
              )
            }
            RawNativeObservationFixture.active(native) {
              val raw = native.read.format("delta").load(targetPath.toString)
              Vector(raw, raw.select("id", "region")).zip(entries).foreach { case (frame, entry) =>
                assertSame(
                  entry.cachedRepresentation.cacheBuilder,
                  spark.sharedState.cacheManager
                    .lookupCachedData(frame)
                    .get
                    .cachedRepresentation
                    .cacheBuilder
                )
                val wanted =
                  if (frame.columns.length == 2)
                    expected.map(row => Row(row.getLong(0), row.getString(2)))
                  else expected
                assertEquals(wanted, frame.collect().toVector.sortBy(_.getLong(0)))
              }
            }
            previous = entries
          }
          val data = spark.table("b.db.t")
          val receipt = DeltaRangerDecisionReceipt.consumeOwned(
            data.queryExecution.analyzed,
            spark,
            optimized = false
          )
          assertEquals(1, receipt.obligations.size)
          assertEquals(
            "'MASKED'",
            receipt.obligations.head.decisions.masks("secret").expression().get()
          )
          assertEquals("region = 'keep'", receipt.obligations.head.decisions.filter.get())
          trace.source(receipt)
          var failure: Option[Throwable] = None
          try data.write.mode("append").insertInto("a.db.t")
          catch {
            case error: Throwable =>
              failure = Some(error); trace.failed("original-public-writer", error)
          }
          trace.record("writer-terminal", s"clientReturned=${failure.isEmpty} attempts=1")
          assertNull(DeltaRangerNativeOperation.current())
          assertTrue(OptimisticTransaction.getActive.isEmpty)
          assertTrue(DeltaRangerAppendCommit.current.isEmpty)
          val after = archive(fs, root, trace.directory.resolve("after-original-writer"))
          val actions = observe(fs, targetPath)
          trace.record(
            "native-terminal",
            s"version=${actions.size - 1} objectsBefore=${before.size} objectsAfter=${after.size} originalBytesPreserved=${before.forall { case (name, digest) => after.get(name).contains(digest) }}"
          )
          assertEquals(2, actions.size)
          val published = actions.last
          assertEquals(1, published.count(_.has("txn")))
          assertEquals(appId, published.find(_.has("txn")).get.get("txn").get("appId").asText())
          assertEquals(1L, published.find(_.has("txn")).get.get("txn").get("version").asLong())
          assertEquals(0, published.count(_.has("remove")))
          assertEquals(expected, rows(spark, targetPath, actions.flatten))
          assertEquals(sourceRows, rows(spark, sourcePath, observe(fs, sourcePath).flatten))
          val crc =
            mapper.readTree(read(fs, new Path(targetPath, "_delta_log/00000000000000000001.crc")))
          assertEquals(
            published.find(_.has("commitInfo")).get.get("commitInfo").get("txnId").asText(),
            crc.get("txnId").asText()
          )
          assertTrue(before.forall { case (name, digest) => after.get(name).contains(digest) })
          assertEquals(
            before.filter(_._1.startsWith("source/")),
            after.filter(_._1.startsWith("source/"))
          )
          assertEquals(
            targetProperties - "delta.registration.receipt.nativeVersion",
            targetFixture.properties.asScala.toMap - "delta.registration.receipt.nativeVersion"
          )
          assertEquals(sourceProperties, sourceFixture.properties.asScala.toMap)
          trace.record(
            "independent-result",
            "version=1 rows=3 sourceRows=3 maskedFiltered=true crcNativeUuid=true cleanup=true"
          )
          assertTrue(
            failure.isEmpty,
            "Original public writer must return; retained throwable remains authoritative"
          )
          assertRebuilt()
          spark.table("b.db.t").write.mode("append").insertInto("a.db.t")
          assertRebuilt()
          assertEquals(after, archive(fs, root, trace.directory.resolve("after-duplicate")))
          target.denied.add("a:*")
          try
            assertThrows(
              classOf[SecurityException],
              () => spark.table("b.db.t").write.mode("append").insertInto("a.db.t")
            )
          finally target.denied.remove("a:*")
          assertEquals(after, archive(fs, root, trace.directory.resolve("after-denied-duplicate")))
          trace.record(
            "warm-complete",
            "appendRebuilt=true duplicateRebuilt=true deniedDuplicate=true unchangedStorage=true"
          )
        },
        Some(extensions => {
          new GravitinoAuthorizationSparkSessionExtensions().apply(extensions)
          install(extensions)
        }),
        startupOptions = storage.map { case (key, value) =>
          ("spark.hadoop." + key) -> value
        } ++ Map(
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false",
          "spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> rawConfig.toString
        ),
        beforeExtension = spark => {
          spark
            .createDataFrame(seedRows.asJava, schema)
            .write
            .format("delta")
            .save(targetPath.toString)
          spark
            .createDataFrame(sourceRows.asJava, schema)
            .write
            .format("delta")
            .save(sourcePath.toString)
          NativeSelectFixture.locations ++= Map(
            "a" -> targetPath.toString,
            "b" -> sourcePath.toString
          )
        }
      )
    finally {
      NativeSelectFixture.catalogPolicies = previousPolicies
      NativeSelectFixture.policy = previousPolicy
      target.engine.close(); source.engine.close(); trace.close()
    }
  }

  private def read(fs: FileSystem, path: Path): Array[Byte] = {
    val stream = fs.open(path)
    try stream.readAllBytes()
    finally stream.close()
  }

  private def observe(fs: FileSystem, path: Path): Vector[Vector[JsonNode]] =
    fs.listStatus(new Path(path, "_delta_log"))
      .toVector
      .filter(_.getPath.getName.matches("[0-9]{20}\\.json"))
      .sortBy(_.getPath.getName)
      .map(status =>
        new String(read(fs, status.getPath), StandardCharsets.UTF_8)
          .split("\n")
          .filter(_.nonEmpty)
          .map(mapper.readTree)
          .toVector
      )

  private def rows(spark: SparkSession, path: Path, actions: Vector[JsonNode]): Vector[Row] =
    actions
      .filter(_.has("add"))
      .flatMap { action =>
        val reader = ParquetReader
          .builder[Group](
            new GroupReadSupport,
            new Path(path, action.get("add").get("path").asText())
          )
          .withConf(spark.sessionState.newHadoopConf())
          .build()
        val result = ArrayBuffer.empty[Row]
        try {
          var group = reader.read()
          while (group != null) {
            val secret =
              if (group.getFieldRepetitionCount("secret") == 0) null
              else group.getString("secret", 0)
            result += Row(group.getLong("id", 0), secret, group.getString("region", 0))
            group = reader.read()
          }
        } finally reader.close()
        result
      }
      .sortBy(_.getLong(0))

  private def archive(fs: FileSystem, root: Path, destination: LocalPath): Map[String, String] = {
    val files = fs.listFiles(root, true)
    val results = scala.collection.mutable.Map.empty[String, String]
    while (files.hasNext) {
      val path = files.next().getPath
      val relative = path.toUri.getPath.stripPrefix(root.toUri.getPath + "/")
      val bytes = read(fs, path)
      val local = destination.resolve(relative)
      Files.createDirectories(local.getParent)
      Files.write(local, bytes)
      results.put(relative, sha256(bytes))
    }
    results.toMap
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(value => f"${value & 0xff}%02x").mkString
}
