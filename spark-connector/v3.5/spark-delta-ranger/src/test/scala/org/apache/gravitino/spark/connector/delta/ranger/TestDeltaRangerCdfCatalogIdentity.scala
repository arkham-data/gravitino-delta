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
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.delta.OptimisticTransaction
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

/** Binds the issued catalog plugin across command and native registration callbacks. */
class TestDeltaRangerCdfCatalogIdentity {
  private val sql = "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"

  @Test
  def replacementBeforeExecutionCannotInvokeAlter(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val frame = spark.sql(sql)
      val physical = frame.queryExecution.executedPlan.asInstanceOf[DeltaRangerCdfPropertyExec]
      val before = manifest(NativeSelectFixture.locations("a"))
      val settings = spark.sessionState.conf.getAllConfs.toMap
      val catalogs = catalogEntries(spark)
      val original = catalogs("a")
      catalogs.update("a", foreignCatalog())
      try {
        assertThrows(classOf[SecurityException], () => physical.run())
        assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
        assertEquals(0, CdfAlterForwardingCatalog.alters.get())
        assertEquals(before, manifest(NativeSelectFixture.locations("a")))
        assertUnwound()
      } finally catalogs.update("a", original)
    }

  @Test
  def registrationCallbackReplacementCannotPublish(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, _) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      // Resolve the issued plugin before retaining the exact CatalogManager entry.
      val frame = spark.sql(sql)
      val catalogs = catalogEntries(spark)
      val original = catalogs("a")
      val settings = spark.sessionState.conf.getAllConfs.toMap
      val before = manifest(NativeSelectFixture.locations("a"))
      var exercised = false
      fixture.onRead = _ =>
        DeltaRangerCdfPropertyMutation.current.foreach { owner =>
          val field =
            owner.getClass.getDeclaredFields.find(_.getType == classOf[OptimisticTransaction]).get
          field.setAccessible(true)
          val captured = field.get(owner) != null
          if (!exercised && captured) {
            exercised = true
            assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
            catalogs.update("a", foreignCatalog())
          }
        }
      try {
        var failure: Exception = null
        try frame.collect()
        catch { case rejected: Exception => failure = rejected }
        assertTrue(exercised, "The captured-transaction callback must replace the catalog")
        assertNotNull(failure, "Catalog replacement must reject the issued native operation")
        assertTrue(
          Iterator
            .iterate(failure: Throwable)(_.getCause)
            .takeWhile(_ != null)
            .exists(error =>
              Option(error.getMessage).exists(_.contains("catalog identity changed"))
            ),
          "The catalog identity validator must reject the replacement"
        )
        assertEquals(settings, spark.sessionState.conf.getAllConfs.toMap)
        assertEquals(
          before,
          manifest(NativeSelectFixture.locations("a")),
          "Catalog replacement must reject before publication"
        )
        assertUnwound()
      } finally { fixture.onRead = _ => (); catalogs.update("a", original) }
    }

  private def foreignCatalog(): CatalogPlugin = {
    val plugin = mock(classOf[CatalogPlugin])
    when(plugin.name()).thenReturn("a")
    plugin
  }

  private def catalogEntries(spark: SparkSession): mutable.Map[String, CatalogPlugin] = {
    val manager = spark.sessionState.catalogManager
    val field = manager.getClass.getDeclaredFields.find(_.getName == "catalogs").get
    field.setAccessible(true)
    field.get(manager).asInstanceOf[mutable.Map[String, CatalogPlugin]]
  }

  private def assertUnwound(): Unit = {
    assertTrue(DeltaRangerCdfPropertyMutation.current.isEmpty)
    assertNull(DeltaRangerNativeOperation.current())
    assertTrue(OptimisticTransaction.getActive.isEmpty)
  }

  private def manifest(location: String): Map[String, Vector[Byte]] = {
    val root = Paths.get(location)
    val paths = Files.walk(root)
    try
      paths
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .map(path => root.relativize(path).toString -> Files.readAllBytes(path).toVector)
        .toMap
    finally paths.close()
  }
}
