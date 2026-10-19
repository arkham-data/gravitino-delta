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

import java.nio.file.{Files, Paths}
import java.util.{Properties, UUID}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

/** Real MinIO observation under the fixed application static-credential S3A profile. */
@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.delta.test.s3.enabled", matches = "true")
class TestDeltaRangerRawNativeObservationS3 {
  import RawNativeObservationFixture._

  @Test def originalS3RawIndexOldAnalysisNewInstalledHead(): Unit = {
    Seq("org.apache.hadoop.fs.s3a.S3AFileSystem", "com.amazonaws.auth.BasicAWSCredentials")
      .foreach { name =>
        val cls = Class.forName(name, false, classOf[DeltaLog].getClassLoader)
        val source = cls.getProtectionDomain.getCodeSource
        assertNotNull(source)
        evidence("loaded-origins.txt", name + "=" + source.getLocation.toExternalForm + "\n")
      }
    val credentials = new Properties()
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
    assertTrue(
      storage.values.forall(v => v != null && v.nonEmpty),
      "Private S3 fixture inputs required"
    )
    val bucket = System.getProperty("gravitino.delta.test.s3.bucket")
    val root = new Path(
      "s3a://" + bucket + "/gravitino-raw-native-observation/" + UUID.randomUUID().toString
    )
    withEnabled(
      Seq("a", "b"),
      storage.map { case (key, value) => ("spark.hadoop." + key) -> value }
    ) { (spark, native, policy) =>
      val aPath = new Path(root, "a").toString
      val bPath = new Path(root, "b").toString
      val fs = root.getFileSystem(native.sessionState.newHadoopConf())
      var a: NativeBoundCatalogFixture = null
      var b: NativeBoundCatalogFixture = null
      val bPolicy = new TestDeltaRangerNativeSelect.Fixture
      try {
        active(native) {
          native.sql("SELECT 1L id, 'old' secret, 'keep' region").write.format("delta").save(aPath)
          native.sql("SELECT 1L id, 'cold' secret, 'keep' region").write.format("delta").save(bPath)
        }
        NativeSelectFixture.locations ++= Map("a" -> aPath, "b" -> bPath)
        val raw = active(native) { native.read.format("delta").load(aPath) }
        val index = rawIndex(raw)
        assertEquals(Map("path" -> aPath), index.deltaLog.allOptions)
        a = new NativeBoundCatalogFixture(spark, policy)
        b = new NativeBoundCatalogFixture(spark, bPolicy, "b")
        DeltaLog.invalidateCache(native, new Path(bPath))
        val first = measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
        measured(spark) { first.requireCurrent(spark) }
        active(native) {
          native
            .sql("SELECT 2L id, 'new' secret, 'keep' region")
            .write
            .format("delta")
            .mode("append")
            .save(aPath)
        }
        denied(first.requireCurrent(spark))
        val fresh = measured(spark) { DeltaRangerRawNativeObservation.observe(spark, index) }
        assertTrue(fresh.analysisVersion < fresh.installedVersion)
        evidence(
          "semantics.txt",
          s"backend=s3a-static-minio analysis=${fresh.analysisVersion} installed=${fresh.installedVersion} sameIndex=true sameDescriptor=true\n"
        )
        assertSame(index, rawIndex(raw))
        assertSame(index.snapshotAtAnalysis, rawIndex(raw).snapshotAtAnalysis)
        assertTrue(spark.sharedState.cacheManager.lookupCachedData(raw).isEmpty)
        val registrations = DeltaRangerRawMaintenanceDeployment.observeRegistrations(spark)
        assertNull(
          DeltaRangerRawDeltaLogCache.observe(spark, registrations.get(1).publication()).log()
        )
        val hadoop = spark.sparkContext.hadoopConfiguration
        Seq(
          "fs.s3a.endpoint",
          "fs.s3a.aws.credentials.provider",
          "fs.s3a.path.style.access",
          "fs.s3a.access.key",
          "fs.s3a.secret.key"
        ).foreach { key =>
          val previous = hadoop.get(key)
          hadoop.set(key, "rejected-observation-value")
          denied(fresh.requireCurrent(spark))
          denied(DeltaRangerRawNativeObservation.observe(spark, index))
          hadoop.set(key, previous)
        }
        val overrides = Seq(
          "fs.s3a.bucket." + bucket + ".endpoint" -> "rejected-observation-value",
          "fs.s3a.aws.credentials.provider" -> classOf[RawNativeForeignProvider].getName,
          "fs.s3a.s3.client.factory.impl" -> classOf[RawNativeForeignProvider].getName,
          "fs.s3a.audit.service.classname" -> classOf[RawNativeForeignProvider].getName,
          "fs.s3a.impl" -> classOf[RawNativeForeignProvider].getName
        )
        overrides.foreach { case (key, value) =>
          val previous = hadoop.get(key)
          hadoop.set(key, value)
          denied(DeltaRangerRawNativeObservation.observe(spark, index))
          if (previous == null) hadoop.unset(key) else hadoop.set(key, previous)
        }
        assertEquals(0, RawNativeForeignProvider.calls.get())
        measured(spark) { fresh.requireCurrent(spark) }
        b.close(); b = null
        NativeSelectFixture.locations += "b" -> aPath
        b = new NativeBoundCatalogFixture(spark, bPolicy, "b")
        denied(fresh.requireCurrent(spark))
        denied(DeltaRangerRawNativeObservation.observe(spark, index))
        b.close(); b = null
        val beforeReplacement = DeltaRangerRawNativeObservation.observe(spark, index)
        DeltaLog.invalidateCache(native, new Path(aPath))
        val replacement = active(native) { rawIndex(native.read.format("delta").load(aPath)) }
        assertNotSame(index.deltaLog, replacement.deltaLog)
        denied(beforeReplacement.requireCurrent(spark))
        denied(DeltaRangerRawNativeObservation.observe(spark, index))
        val current = measured(spark) {
          DeltaRangerRawNativeObservation.observe(spark, replacement)
        }
        policy.start()
        denied(current.requireCurrent(spark))
        val live = DeltaRangerRawNativeObservation.observe(spark, replacement)
        a.close(); a = null
        denied(live.requireCurrent(spark))
      } finally {
        if (a != null) a.close()
        if (b != null) b.close()
        bPolicy.engine.close()
        DeltaLog.invalidateCache(native, new Path(aPath))
        DeltaLog.invalidateCache(native, new Path(bPath))
        fs.delete(root, true)
      }
    }
  }
}

private[ranger] object RawNativeForeignProvider { val calls = new AtomicInteger() }
private[ranger] class RawNativeForeignProvider { RawNativeForeignProvider.calls.incrementAndGet() }
