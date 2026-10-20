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
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableChange}
import org.apache.spark.sql.delta.OptimisticTransaction
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.when

/** Exercises issued-command boundaries before any native mutation is invoked. */
class TestDeltaRangerCdfPropertyCommand {
  private val sql = "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"

  @Test
  def exactPropertiesOnly(): Unit = {
    Seq("true", "false").foreach(value =>
      DeltaRangerCdfPropertyCommand.validateProperties(Map("delta.enableChangeDataFeed" -> value))
    )
    Seq(
      Map.empty[String, String],
      Map("delta.enablechangedatafeed" -> "true"),
      Map("delta.enableChangeDataFeed" -> "TRUE"),
      Map("delta.enableChangeDataFeed" -> "1"),
      Map("delta.enableChangeDataFeed" -> "true", "comment" -> "other")
    ).foreach(properties =>
      assertThrows(
        classOf[SecurityException],
        () => DeltaRangerCdfPropertyCommand.validateProperties(properties)
      )
    )
  }

  @Test
  def copiedAndChangedContextsAndDirectCallsCannotInvokeAlter(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      val fixture = NativeSelectFixture.bindingFixture(spark, "a")
      val wrapper = fixture.wrapper
      val before = manifest(NativeSelectFixture.locations("a"))
      assertThrows(
        classOf[SecurityException],
        () =>
          wrapper.alterTable(
            Identifier.of(Array("db"), "t"),
            TableChange.setProperty("delta.enableChangeDataFeed", "true")
          )
      )
      val logical =
        spark.sql(sql).queryExecution.analyzed.asInstanceOf[DeltaRangerCdfPropertyCommand.Carrier]
      val owner = logical.owner
      val encoded = new ByteArrayOutputStream
      val output = new ObjectOutputStream(encoded)
      try output.writeObject(logical)
      finally output.close()
      val input = new ObjectInputStream(new ByteArrayInputStream(encoded.toByteArray))
      val restored =
        try input.readObject().asInstanceOf[DeltaRangerCdfPropertyCommand.Carrier]
        finally input.close()
      assertThrows(classOf[SecurityException], () => restored.owner)

      assertThrows(classOf[SecurityException], () => owner.check(logical.copy()(owner), spark))
      assertThrows(classOf[SecurityException], () => owner.check(logical, spark.newSession()))
      val physical = owner.plan(logical, spark)
      assertThrows(classOf[SecurityException], () => physical.copy()(owner).run())
      spark.sparkContext.setLocalProperty("kyuubi.session.user", "other-subject")
      try assertThrows(classOf[SecurityException], () => physical.run())
      finally spark.sparkContext.setLocalProperty("kyuubi.session.user", null)
      spark.conf.set("spark.sql.shuffle.partitions", "99")
      try assertThrows(classOf[SecurityException], () => physical.run())
      finally spark.conf.set("spark.sql.shuffle.partitions", "2")
      val token = fixture.properties.get("delta.registration.receipt.token")
      fixture.properties.put("delta.registration.receipt.token", "replacement")
      try assertThrows(classOf[SecurityException], () => physical.run())
      finally fixture.properties.put("delta.registration.receipt.token", token)
      policy.denied.add("a:*")
      assertThrows(classOf[SecurityException], () => physical.run())
      policy.denied.clear()
      when(policy.plugin.getPoliciesVersion).thenReturn(2L)
      policy.start()
      assertThrows(classOf[SecurityException], () => physical.run())
      assertEquals(0, CdfAlterForwardingCatalog.alters.get())
      assertEquals(before, manifest(NativeSelectFixture.locations("a")))
      assertNull(DeltaRangerNativeOperation.current())
      assertTrue(OptimisticTransaction.getActive.isEmpty)
    }

  @Test
  def updateOnlySelectOnlyAndOtherDdlCannotMutate(): Unit =
    NativeSelectFixture.withBoundSpark { (spark, policy) =>
      spark.conf.set("spark.sql.catalog.a", classOf[CdfAlterForwardingCatalog].getName)
      CdfAlterForwardingCatalog.alters.set(0)
      NativeSelectFixture.bindingFixture(spark, "a")
      val before = manifest(NativeSelectFixture.locations("a"))
      Seq("update", "select").foreach { allowed =>
        policy.onRequest = request => {
          if (request.getAccessType == allowed) policy.denied.remove("a:*")
          else policy.denied.add("a:*")
        }
        assertThrows(classOf[Exception], () => { spark.sql(sql).collect(); () })
      }
      policy.onRequest = _ => ()
      policy.denied.clear()
      Seq(
        "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'comment'='other')",
        "ALTER TABLE a.db.t UNSET TBLPROPERTIES ('delta.enableChangeDataFeed')",
        "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enablechangedatafeed'='true')",
        "ALTER TABLE a.db.t SET TBLPROPERTIES ('delta.enableChangeDataFeed'='TRUE')",
        "ALTER TABLE a.db.t ADD COLUMNS (extra INT)",
        s"ALTER TABLE delta.`${NativeSelectFixture.locations("a")}` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')",
        "ALTER TABLE a.db.t VERSION AS OF 0 SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')"
      ).foreach(statement =>
        assertThrows(classOf[Exception], () => { spark.sql(statement).collect(); () })
      )
      assertEquals(0, CdfAlterForwardingCatalog.alters.get())
      assertEquals(before, manifest(NativeSelectFixture.locations("a")))
      assertNull(DeltaRangerNativeOperation.current())
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
