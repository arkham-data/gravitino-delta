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

import java.security.PrivilegedExceptionAction
import java.util.HashMap
import scala.collection.JavaConverters._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

class TestDeltaRangerExternalJdbcScope {
  @Test
  def optionsAreCanonicalDetachedAndStrict(): Unit = {
    val original = ExternalJdbcTestFixture.options
    val copied = DeltaRangerExternalJdbcOptions.copyAndValidate(original)
    original.put("password", "changed")
    assertEquals("secret-sentinel", copied("password"))
    assertEquals("jdbc:postgresql://localhost:5432/extraction", copied("url"))
    assertEquals(Set("url", "driver", "user", "password", "dbtable"), copied.keySet)
    val optional = ExternalJdbcTestFixture.options
    Map(
      "FETCHSIZE" -> "0",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "3",
      "numPartitions" -> "2"
    ).foreach { case (k, v) => optional.put(k, v) }
    assertEquals("2", DeltaRangerExternalJdbcOptions.copyAndValidate(optional)("numpartitions"))
  }

  @Test
  def invalidOptionsNeverExposeSuppliedValues(): Unit = {
    val variants = Seq(
      Map("URL" -> "jdbc:postgresql://localhost/db"),
      Map("query" -> "query-sentinel"),
      Map("password" -> null),
      Map(null.asInstanceOf[String] -> "query-sentinel"),
      Map("driver" -> "other.Driver"),
      Map("user" -> " "),
      Map("fetchsize" -> "-1"),
      Map("fetchsize" -> "invalid"),
      Map("numPartitions" -> "0"),
      Map(
        "partitionColumn" -> "id",
        "lowerBound" -> "1",
        "upperBound" -> "3",
        "numPartitions" -> "0"
      ),
      Map("dbtable" -> " "),
      Map("partitionColumn" -> "id"),
      Map("customSchema" -> "query-sentinel"),
      Map("prepareQuery" -> "query-sentinel"),
      Map("ssl" -> "true"),
      Map("url" -> "jdbc:postgresql:extraction"),
      Map("url" -> "jdbc:postgresql://host/"),
      Map("url" -> "jdbc:postgresql://host,other/db"),
      Map("url" -> "jdbc:postgresql://user@host/db"),
      Map("url" -> "jdbc:postgresql://host/db?password=secret-sentinel"),
      Map("url" -> "jdbc:postgresql://host/db#query-sentinel")
    )
    variants.foreach { variant =>
      val options = ExternalJdbcTestFixture.options
      variant.foreach { case (k, v) => options.put(k, v) }
      val error = assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerExternalJdbcOptions.copyAndValidate(options); ()
        }
      )
      assertFalse(error.toString.contains("secret-sentinel"))
      assertFalse(error.toString.contains("query-sentinel"))
    }
    ExternalJdbcTestFixture.options.keySet.asScala.foreach { key =>
      val options = ExternalJdbcTestFixture.options
      options.remove(key)
      assertThrows(
        classOf[IllegalArgumentException],
        () => {
          DeltaRangerExternalJdbcOptions.copyAndValidate(options); ()
        }
      )
    }
    assertThrows(
      classOf[IllegalArgumentException],
      () => {
        DeltaRangerExternalJdbcOptions.copyAndValidate(null); ()
      }
    )
  }

  @Test
  def startupFlagsCannotBeEnabledByLaterSessionSettings(): Unit = {
    for (missing <- ExternalJdbcTestFixture.flags; value <- Seq("false", null)) {
      ExternalJdbcTestFixture.withSpark(Map(missing -> value)) { spark =>
        spark.conf.set(missing, "true")
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerExternalJdbcSources.open(spark, 1)
        )
      }
    }
  }

  @Test
  def lifetimeOwnershipConfigurationAndSubjectAreBound(): Unit =
    ExternalJdbcTestFixture.withSpark() { spark =>
      Seq(0L, 86401L).foreach { lifetime =>
        assertThrows(
          classOf[IllegalArgumentException],
          () => DeltaRangerExternalJdbcSources.open(spark, lifetime)
        )
      }
      Seq(1L, 86400L).foreach { lifetime =>
        val scope = DeltaRangerExternalJdbcSources.open(spark, lifetime)
        scope.requireLive()
        scope.close(); scope.close()
        assertThrows(classOf[SecurityException], () => scope.requireLive())
      }
      val scope = DeltaRangerExternalJdbcSources.open(spark, 1200)
      try {
        val unsupported = ExternalJdbcTestFixture.options
        unsupported.put("sessionInitStatement", "query-sentinel")
        assertThrows(classOf[IllegalArgumentException], () => scope.load(unsupported))
        SparkSession.clearActiveSession()
        assertThrows(classOf[SecurityException], () => scope.requireLive())
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerExternalJdbcSources.open(spark, 1200)
        )
        SparkSession.setActiveSession(spark.newSession())
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerExternalJdbcSources.open(spark, 1200)
        )
        assertThrows(classOf[SecurityException], () => scope.requireLive())
        SparkSession.setActiveSession(spark)
        spark.conf.set("spark.sql.shuffle.partitions", "17")
        assertThrows(classOf[SecurityException], () => scope.requireLive())
        spark.conf.unset("spark.sql.shuffle.partitions")
        UserGroupInformation
          .createUserForTesting("external-other", Array("other-group"))
          .doAs(new PrivilegedExceptionAction[Unit] {
            override def run(): Unit = {
              assertThrows(classOf[SecurityException], () => scope.requireLive()); ()
            }
          })
        val originalUser = UserGroupInformation.getCurrentUser
        val originalGroups = originalUser.getGroupNames
        try
          UserGroupInformation
            .createUserForTesting(originalUser.getShortUserName, Array("external-different-group"))
            .doAs(new PrivilegedExceptionAction[Unit] {
              override def run(): Unit = {
                assertThrows(classOf[SecurityException], () => scope.requireLive()); ()
              }
            })
        finally
          UserGroupInformation.createUserForTesting(originalUser.getShortUserName, originalGroups)
        scope.requireLive()
      } finally scope.close()
      assertFalse(spark.sparkContext.isStopped)
    }

  @Test
  def expiryAndApplicationEndCleanOnlyOwnedReferences(): Unit =
    ExternalJdbcTestFixture.withSpark() { spark =>
      var now = 10L
      val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1, () => now)
      scope.requireLive()
      now += 1000000000L
      assertThrows(classOf[SecurityException], () => scope.requireLive())
      assertFalse(spark.sparkContext.isStopped)
      val application = DeltaRangerExternalJdbcSources.open(spark, 1200)
      spark.stop()
      assertThrows(classOf[SecurityException], () => application.requireLive())
      application.close()
    }
}

private[ranger] object ExternalJdbcTestFixture {
  val external = "spark.gravitino.enableDeltaRangerExternalJdbcSupport"
  val flags =
    Seq("spark.gravitino.enableDeltaSupport", "spark.gravitino.enableDeltaRangerSupport", external)
  def options: HashMap[String, String] = new HashMap[String, String](
    Map(
      "url" -> "jdbc:postgresql://localhost:5432/extraction",
      "driver" -> "org.postgresql.Driver",
      "user" -> "extractor",
      "password" -> "secret-sentinel",
      "dbtable" -> "(SELECT * FROM public.source_typed) arkham_temp_table"
    ).asJava
  )
  def withSpark(overrides: Map[String, String] = Map.empty)(run: SparkSession => Unit): Unit = {
    SparkSession.clearActiveSession(); SparkSession.clearDefaultSession()
    val settings = flags.map(_ -> "true").toMap ++ overrides
    val builder = SparkSession
      .builder()
      .master("local[1]")
      .appName("external-jdbc-private")
      .config("spark.ui.enabled", "false")
    settings.filter(_._2 != null).foreach { case (key, value) => builder.config(key, value) }
    val spark = builder.getOrCreate()
    SparkSession.setActiveSession(spark)
    try run(spark)
    finally { spark.stop(); SparkSession.clearActiveSession(); SparkSession.clearDefaultSession() }
  }
}
