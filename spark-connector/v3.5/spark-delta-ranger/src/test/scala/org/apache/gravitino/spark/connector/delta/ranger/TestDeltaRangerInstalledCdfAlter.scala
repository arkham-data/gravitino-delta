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
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableChange}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

/** Installed-extension acceptance of exact named CDF property mutation. */
class TestDeltaRangerInstalledCdfAlter {
  @Test
  def namedCdfEnableAndDisableRequireAlter(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          CdfAlterForwardingCatalog.alters.set(0)
          NativeSelectFixture.bindingFixture(spark, "a")
          val location = NativeSelectFixture.locations("a")
          val before = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
          Seq("true", "false").zipWithIndex.foreach { case (enabled, offset) =>
            policy.requests.clear()
            val frame = spark.sql(
              s"ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='$enabled')"
            )
            val physical =
              frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfPropertyExec]
            frame.collect()
            assertThrows(classOf[SecurityException], () => physical.run())
            val after = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
            assertEquals(before.version + offset + 1, after.version)
            assertEquals(
              Some(enabled),
              after.metadata.configuration.get("delta.enableChangeDataFeed")
            )
            assertEquals(before.metadata.id, after.metadata.id)
            assertEquals(before.metadata.schemaString, after.metadata.schemaString)
            assertTrue(policy.requests.asScala.exists(_.getAccessType == "alter"))
            assertFalse(policy.requests.asScala.exists(_.getAccessType == "select"))
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
          }
        },
        Some(NativeSelectFixture.extension(policy))
      )
    finally policy.engine.close()
  }

  @Test
  def freshSnapshotsAndClassicCheckpointsPreserveProtocolAndData(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          val fixture = NativeSelectFixture.bindingFixture(spark, "a")
          val location = NativeSelectFixture.locations("a")
          val logRoot = Paths.get(location).resolve("_delta_log")
          val before = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
          val files = manifest(Paths.get(location)).filterNot(_._1.startsWith("_delta_log/"))
          var enabledProtocol = before.protocol
          var retainedOwner: DeltaRangerCdfPropertyMutation = null
          fixture.onRead = _ => {
            DeltaRangerCdfPropertyMutation.current.foreach(owner => retainedOwner = owner)
            val paths = Files.list(logRoot)
            val versions =
              try
                paths
                  .iterator()
                  .asScala
                  .map(_.getFileName.toString)
                  .filter(_.matches("[0-9]{20}\\.json"))
                  .map(_.stripSuffix(".json").toLong)
                  .toVector
              finally paths.close()
            fixture.properties
              .put("delta.registration.receipt.nativeVersion", versions.max.toString)
          }
          Seq("true", "false").zipWithIndex.foreach { case (value, offset) =>
            DeltaLog.clearCache()
            spark
              .sql(s"ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='$value')")
              .collect()
            val after = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
            assertEquals(before.version + offset + 1, after.version)
            assertEquals(
              Some(value),
              after.metadata.configuration.get("delta.enableChangeDataFeed")
            )
            assertTrue(Files.exists(logRoot.resolve(f"${after.version}%020d.checkpoint.parquet")))
            assertEquals(
              after.version.toString,
              fixture.properties.get("delta.registration.receipt.nativeVersion")
            )
            if (value == "true") enabledProtocol = after.protocol
            else assertEquals(enabledProtocol, after.protocol)
            assertEquals(
              files,
              manifest(Paths.get(location)).filterNot(_._1.startsWith("_delta_log/"))
            )
            assertNotNull(retainedOwner)
            assertThrows(classOf[SecurityException], () => retainedOwner.requireLive())
            assertNull(DeltaRangerNativeOperation.current())
            assertTrue(OptimisticTransaction.getActive.isEmpty)
          }
        },
        Some(NativeSelectFixture.extension(policy)),
        beforeExtension = native => {
          native
            .sql(
              s"ALTER TABLE delta.`${NativeSelectFixture.locations("a")}` SET TBLPROPERTIES ('delta.checkpointInterval'='1')"
            )
            .collect()
        }
      )
    finally policy.engine.close()
  }

  @Test
  def committedReloadFailureIsExplicitAndNeverReplayed(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val location = NativeSelectFixture.locations("a")
      val before = DeltaLog.forTable(spark, location).unsafeVolatileSnapshot
      val next =
        Paths.get(location).resolve("_delta_log").resolve(f"${before.version + 1}%020d.json")
      fixture.onRead = _ => {
        if (Files.exists(next) && DeltaRangerNativeOperation.current() == null)
          throw new IllegalStateException("injected reload failure")
      }
      val failure = assertThrows(
        classOf[Exception],
        () => {
          spark
            .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
            .collect(); ()
        }
      )
      val causes = Iterator.iterate[Throwable](failure)(_.getCause).takeWhile(_ != null).toVector
      assertTrue(
        causes.exists(e =>
          Option(e.getMessage).exists(_.startsWith("CDF property change committed;"))
        )
      )
      assertTrue(Files.exists(next))
      assertEquals(1, CdfAlterForwardingCatalog.alters.get())
      assertFalse(Files.exists(next.resolveSibling(f"${before.version + 2}%020d.json")))
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
      fixture.onRead = _ => ()
      fixture.wrapper.loadTable(Identifier.of(Array("db"), "t"))
      assertEquals(
        before.version + 1,
        DeltaLog.forTable(spark, location).unsafeVolatileSnapshot.version
      )
    }

  @Test
  def deniedCdfAlterLeavesAllTargetBytesUnchanged(): Unit = {
    val policy = new TestDeltaRangerNativeSelect.Fixture
    try
      NativeSelectFixture.withSpark(
        spark => {
          spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
          CdfAlterForwardingCatalog.alters.set(0)
          val fixture = NativeSelectFixture.bindingFixture(spark, "a")
          val registration = fixture.properties.asScala.toMap
          val location = NativeSelectFixture.locations("a")
          val before = manifest(Paths.get(location))
          policy.denied.add("a:*")
          assertThrows(
            classOf[Exception],
            () => {
              spark
                .sql("ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
                .collect()
              ()
            }
          )
          assertEquals(0, CdfAlterForwardingCatalog.alters.get())
          assertEquals(before, manifest(Paths.get(location)))
          assertEquals(registration, fixture.properties.asScala.toMap)
          assertNull(DeltaRangerNativeOperation.current())
        },
        Some(NativeSelectFixture.extension(policy))
      )
    finally policy.engine.close()
  }

  private def manifest(root: Path): Map[String, String] = {
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map { path =>
          val digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path))
          root.relativize(path).toString -> digest.map(b => f"${b & 0xff}%02x").mkString
        }
        .toMap
    finally paths.close()
  }

}

/** Forwards ALTER to the production wrapper if installed plan admission permits execution. */
class CdfAlterForwardingCatalog extends NativeBoundForwardingCatalog {

  /** Delegates the exact Spark changes without adding authorization or changing admission. */
  override def alterTable(id: Identifier, changes: TableChange*): Table = {
    CdfAlterForwardingCatalog.alters.incrementAndGet()
    NativeSelectFixture.wrapper(SparkSession.active, name()).alterTable(id, changes: _*)
  }
}

private[ranger] object CdfAlterForwardingCatalog {
  val alters = new AtomicInteger()
}
