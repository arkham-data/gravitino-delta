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
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSessionExtensions}
import io.delta.sql.DeltaSparkSessionExtension
import org.junit.jupiter.api.Test
import org.apache.spark.sql.delta.DeltaLog
import org.junit.jupiter.api.Assertions._

/** Continued governed use of a native table across named CDF property changes. */
class TestDeltaRangerCdfEnablement {
  @Test
  def ordinaryUsePreservesCdfIntervals(): Unit = scenario("cold")

  @Test
  def targetFirstCachesPreserveCdfIntervals(): Unit = scenario("target-first")

  @Test
  def projectFirstCachesPreserveCdfIntervals(): Unit = scenario("project-first")

  @Test
  def nativeClonedSessionCachesPreserveCdfIntervals(): Unit = scenario("clone-native")

  private[ranger] def scenario(order: String, appendAdaptive: Boolean = false): Unit = {
    val warmOrder = if (order == "clone-native") "target-first" else order
    val target = new TestDeltaRangerNativeSelect.Fixture
    val source = new TestDeltaRangerNativeSelect.Fixture
    source.customCatalog = "b"
    NativeSelectFixture.catalogPolicies = Map("a" -> target, "b" -> source)
    val output = Files.createTempDirectory("cdf-continued-" + order)
    val raw = Files.createTempFile(output, "raw", ".properties")
    Files.write(
      raw,
      ("version=1\nprofile=raw-current-head-filter-project-v1\n" +
        "table.count=1\ntable.0.metalake=lake\ntable.0.catalog=a\n" +
        "table.0.database=db\ntable.0.table=t\n").getBytes("UTF-8")
    )
    val trace = InstalledAppendBoundaryProbe.fresh(s"cdf-continued-$order-aqe-$appendAdaptive")
    var reopenLocation: String = null
    var reopenVersion = -1L
    try {
      NativeSelectFixture.withSpark(
        spark => {
          trace.activate(spark)
          trace.record("cdf-evidence-directory", output.toString)
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          val registration = NativeSelectFixture.bindingFixture(spark, "a")
          val sourceRegistration = NativeSelectFixture.bindingFixture(spark, "b")
          val location = NativeSelectFixture.locations("a")
          val observer = new DeltaRangerLiveAppend.WarmObserver(warmOrder)
          val (native, frames) =
            if (order == "cold") (nativeSession(spark), Vector.empty[DataFrame])
            else DeltaRangerLiveAppend.warmCaches(spark, location, warmOrder, output, observer)
          val json = new ObjectMapper
          var previous =
            if (order == "cold") null else json.readTree(output.resolve("warm-before.json").toFile)
          def record(name: String, rebuilt: Boolean = false): Unit = if (order != "cold") {
            observer.record(spark, frames, output.resolve(name), "success")
            val current = json.readTree(output.resolve(name).toFile)
            assertEquals(previous.path("unrelated"), current.path("unrelated"))
            val before = previous.path("entries").elements().asScala.toVector
            val after = current.path("entries").elements().asScala.toVector
            before.zip(after).foreach { case (old, fresh) =>
              assertEquals(old.path("key"), fresh.path("key"))
              if (rebuilt) {
                assertNotEquals(old.path("builder"), fresh.path("builder"))
                assertNotEquals(old.path("producer"), fresh.path("producer"))
                assertEquals(fresh.path("key"), fresh.path("ownerKey"))
                assertEquals(current.path("observedVersion"), fresh.path("selectedVersion"))
                assertEquals(current.path("observedHead"), fresh.path("selectedHead"))
                assertFalse(fresh.path("loaded").asBoolean())
              } else assertEquals(old, fresh)
            }
            val oldOrder =
              previous.path("matchingOrder").elements().asScala.map(_.asText()).toVector
            val newOrder = current.path("matchingOrder").elements().asScala.map(_.asText()).toVector
            assertEquals(if (rebuilt) oldOrder.reverse else oldOrder, newOrder)
            if (rebuilt)
              assertTrue(
                current.path("oldBuffersUnloaded").elements().asScala.forall(_.asBoolean())
              )
            previous = current
          }
          def head: Long = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version
          def nativeCall[A](body: => A): A = {
            SparkSession.setActiveSession(native)
            try body
            finally SparkSession.setActiveSession(spark)
          }
          def cdf(start: Long, end: Long): Vector[Row] = nativeCall {
            native.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", start)
              .option("endingVersion", end)
              .load(location)
              .select("id", "secret", "region", "_change_type", "_commit_version")
              .collect()
              .toVector
          }
          var checkedWitness = false
          var checkingWitness = false
          var retainedScope: DeltaRangerCdfCacheMaintenance.Scope = null
          val beforeEnable = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
          def probeWitness(): Unit =
            if (!checkedWitness && !checkingWitness && order == "project-first") {
              val owner = DeltaRangerCdfCacheMaintenance
              val activeField =
                owner.getClass.getDeclaredFields.find(_.getName.endsWith("active")).get
              activeField.setAccessible(true)
              val scope = activeField
                .get(owner)
                .asInstanceOf[ThreadLocal[DeltaRangerCdfCacheMaintenance.Scope]]
                .get()
              if (scope != null) {
                val selectedField = scope.getClass.getDeclaredField("selected")
                selectedField.setAccessible(true)
                val selected = selectedField
                  .get(scope)
                  .asInstanceOf[java.util.IdentityHashMap[
                    org.apache.spark.sql.delta.files.TahoeLogFileIndex,
                    DeltaRangerCdfCacheMaintenance.Compatibility
                  ]]
                if (!selected.isEmpty) {
                  checkingWitness = true
                  try {
                    retainedScope = scope
                    val entry = selected.entrySet().iterator().next()
                    val index = entry.getKey
                    val witness = entry.getValue
                    val keysField = scope.getClass.getDeclaredField("keys")
                    keysField.setAccessible(true)
                    val keys = keysField
                      .get(scope)
                      .asInstanceOf[java.util.IdentityHashMap[
                        org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
                        Any
                      ]]
                    val key = keys.keySet().iterator().next()
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.authorize(key.clone(), spark, index)
                    )
                    assertThrows(
                      classOf[SecurityException],
                      () => scope.authorize(key, native, index)
                    )
                    val setting = "spark.databricks.delta.lastCommitVersionInSession"
                    val oldSetting = spark.conf.getOption(setting)
                    spark.conf.set(setting, "999999")
                    try assertThrows(classOf[SecurityException], () => scope.requireLive())
                    finally
                      oldSetting match {
                        case Some(value) => spark.conf.set(setting, value)
                        case None        => spark.conf.unset(setting)
                      }
                    val current = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
                    witness.requireCurrent(index, current)
                    assertThrows(
                      classOf[SecurityException],
                      () => witness.requireCurrent(index, beforeEnable)
                    )
                    val descriptor = index.snapshotAtAnalysis
                    val metadataField = descriptor.getClass.getDeclaredField("metadata")
                    metadataField.setAccessible(true)
                    val metadata = descriptor.metadata
                    Seq(
                      metadata.copy(id = "foreign-native-id"),
                      metadata.copy(schemaString = "{}"),
                      metadata.copy(configuration =
                        metadata.configuration + ("unrelated" -> "drift")
                      )
                    )
                      .foreach { drift =>
                        metadataField.set(descriptor, drift)
                        try
                          assertThrows(
                            classOf[SecurityException],
                            () => witness.requireCurrent(index, current)
                          )
                        finally metadataField.set(descriptor, metadata)
                      }
                    witness.requireCurrent(index, current)
                    checkedWitness = true
                  } finally checkingWitness = false
                }
              }
            }
          registration.onRead = _ => probeWitness()
          sourceRegistration.onRead = _ => probeWitness()
          val original = head
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
            .collect()
          val enabled = head
          assertEquals(original + 1, enabled)
          record("after-enable.json")
          if (order == "cold")
            assertEquals(
              Set(Row(1L, "a-one-masked", "keep"), Row(3L, null, "keep")),
              spark.sql("SELECT id, secret, region FROM a.db.t").collect().toSet
            )
          locally {
            val written = scala.collection.mutable.ArrayBuffer.empty[Long]
            Seq(true, false).foreach { cdfEnabled =>
              if (!cdfEnabled) {
                spark
                  .sql(
                    "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')"
                  )
                  .collect()
                record("after-disable.json")
              }
              Seq("insertInto", "saveAsTable").zipWithIndex.foreach { case (frontend, index) =>
                spark.conf
                  .set(
                    "spark.databricks.delta.write.txnAppId",
                    "cdf-" + cdfEnabled + "-" + frontend
                  )
                spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
                def append(): Unit = {
                  val previous = spark.conf.get("spark.sql.adaptive.enabled")
                  if (appendAdaptive) spark.conf.set("spark.sql.adaptive.enabled", "true")
                  try {
                    val writer =
                      spark.sql("SELECT id, secret, region FROM b.db.t").write.mode("append")
                    if (frontend == "insertInto") writer.insertInto("a.db.t")
                    else writer.saveAsTable("a.db.t")
                  } finally
                    if (appendAdaptive) spark.conf.set("spark.sql.adaptive.enabled", previous)
                }
                val before = head
                append()
                assertEquals(before + 1, head)
                if (order == "project-first") {
                  assertTrue(
                    checkedWitness,
                    "Actual postcommit issued compatibility witness must be reached"
                  )
                  assertNotNull(retainedScope)
                  assertThrows(classOf[SecurityException], () => retainedScope.requireLive())
                }
                if (cdfEnabled) written += head
                record(s"first-$cdfEnabled-$frontend.json", rebuilt = true)
                append()
                assertEquals(before + 1, head, "Duplicate must not publish another version")
                record(s"duplicate-$cdfEnabled-$frontend.json", rebuilt = true)
                if (order == "cold") {
                  val rows = spark.sql("SELECT id, secret, region FROM a.db.t").collect().toVector
                  assertTrue(rows.forall(r => r.getString(2) == "keep"))
                  assertTrue(
                    rows
                      .filterNot(_.isNullAt(1))
                      .forall(r => Set("a-one-masked", "b-one-masked-masked")(r.getString(1)))
                  )
                }
              }
            }
            assertEquals(Vector.empty, cdf(enabled, enabled))
            assertEquals(
              written
                .flatMap(v =>
                  Vector(
                    Row(1L, "b-one-masked", "keep", "insert", v),
                    Row(3L, null, "keep", "insert", v)
                  )
                )
                .toSet,
              cdf(enabled, written.last).toSet
            )
            assertThrows(classOf[Exception], () => { cdf(original, enabled); () })
            assertThrows(classOf[Exception], () => { cdf(written.last + 1, head); () })
            assertTrue(
              Files.exists(
                Paths.get(location).resolve("_delta_log").resolve(f"$head%020d.checkpoint.parquet")
              )
            )
            nativeCall { assertEquals(11L, native.read.format("delta").load(location).count()) }
            reopenLocation = location
            reopenVersion = head
            assertThrows(
              classOf[Exception],
              () => { spark.read.format("delta").load(location).collect(); () }
            )
          }
        },
        Some(NativeSelectFixture.extension(target)),
        Map(
          "spark.gravitino.deltaRanger.rawCacheMaintenance.config" -> raw.toString,
          "spark.sql.sources.bucketing.autoBucketedScan.enabled" -> (order == "clone-native" || order == "cold").toString,
          "spark.sql.optimizer.canChangeCachedPlanOutputPartitioning" -> appendAdaptive.toString,
          "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
        ),
        native =>
          native
            .sql(
              s"ALTER TABLE delta.`${NativeSelectFixture.locations("a")}` SET TBLPROPERTIES ('delta.checkpointInterval'='1')"
            )
            .collect()
      )
      if (reopenLocation != null) reopenCheckpoint(reopenLocation, reopenVersion)
    } finally { trace.close(); target.engine.close(); source.engine.close() }
  }

  private def reopenCheckpoint(location: String, version: Long): Unit = {
    DeltaLog.clearCache()
    val native = SparkSession
      .builder()
      .master("local[2]")
      .appName("cdf-checkpoint-reopen")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.databricks.delta.snapshotPartitions", "2")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try {
      val snapshot = DeltaLog.forTable(native, location).update()
      assertEquals(version, snapshot.version)
      assertEquals(version, snapshot.logSegment.checkpointProvider.version)
      val rows = native.read.format("delta").load(location).collect().toVector
      val expected =
        Vector(Row(1L, "a-one", "keep"), Row(2L, "a-two", "drop"), Row(3L, null, "keep")) ++
          Vector.fill(4)(Vector(Row(1L, "b-one-masked", "keep"), Row(3L, null, "keep"))).flatten
      assertEquals(
        expected.groupBy(identity).mapValues(_.size).toMap,
        rows.groupBy(identity).mapValues(_.size).toMap
      )
      assertEquals("false", snapshot.metadata.configuration("delta.enableChangeDataFeed"))
    } finally {
      native.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def manifest(root: java.nio.file.Path): Map[String, String] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map { path =>
          root.relativize(path).toString -> java.security.MessageDigest
            .getInstance("SHA-256")
            .digest(Files.readAllBytes(path))
            .map(b => f"${b & 0xff}%02x")
            .mkString
        }
        .toMap
    finally paths.close()
  }

  private def nativeSession(spark: SparkSession): SparkSession = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    val constructor =
      classOf[SparkSession].getDeclaredConstructors.find(_.getParameterCount == 5).get
    constructor.setAccessible(true)
    constructor
      .newInstance(
        spark.sparkContext,
        Some(spark.sharedState),
        None,
        extensions,
        Map.empty[String, String]
      )
      .asInstanceOf[SparkSession]
  }
}
