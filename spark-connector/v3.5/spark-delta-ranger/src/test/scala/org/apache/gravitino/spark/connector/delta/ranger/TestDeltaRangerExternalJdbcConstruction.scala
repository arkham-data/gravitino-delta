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

import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.JavaConverters._
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, Logger}
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.logging.log4j.core.layout.PatternLayout
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

@Tag("gravitino-docker-test")
@EnabledIfSystemProperty(named = "gravitino.ranger.delta.externalJdbc.enabled", matches = "true")
class TestDeltaRangerExternalJdbcConstruction {
  @Test
  def stockProviderDiscoversTypedSubqueryAndIssuesOnlyExactRelation(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        var now = 1L
        val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1, () => now)
        val options = ExternalJdbcTestFixture.options
        options.put("url", postgres.url())
        options.put("password", postgres.password())
        options.put("fetchsize", "2")
        options.put("partitionColumn", "id")
        options.put("lowerBound", "1")
        options.put("upperBound", "3")
        options.put("numPartitions", "2")
        val payload = postgres.payloadReads()
        val metadata = postgres.schemaReads()
        val bound = scope.load(options)
        try {
          assertEquals(payload, postgres.payloadReads())
          assertEquals(metadata + 1, postgres.schemaReads())
          assertEquals(
            Seq("id", "secret", "region", "plain_time", "zoned_time"),
            bound.relation.schema.fieldNames.toSeq
          )
          assertEquals(
            Seq(LongType, StringType, StringType, TimestampType, TimestampType),
            bound.relation.schema.fields.map(_.dataType).toSeq
          )
          assertEquals(2, bound.relation.parts.length)
          assertEquals(
            Seq("\"id\" < 2 or \"id\" is null", "\"id\" >= 2"),
            bound.relation.parts.map(_.asInstanceOf[JDBCPartition].whereClause).toSeq
          )
          assertSame(bound, DeltaRangerExternalJdbcSources.resolve(bound.relation, spark))
          options.put("password", "mutation-sentinel")
          options.put("dbtable", "mutation-query-sentinel")
          assertTrue(postgres.password() == bound.relation.jdbcOptions.parameters("password"))
          assertTrue(bound.relation.jdbcOptions.tableOrQuery.endsWith("arkham_temp_table"))
          val reconstructed = bound.relation.copy(
            bound.relation.schema,
            bound.relation.parts.clone(),
            bound.relation.jdbcOptions
          )(spark)
          assertThrows(classOf[SecurityException], () => bound.requireSame(reconstructed))
          assertThrows(
            classOf[SecurityException],
            () => DeltaRangerExternalJdbcSources.resolve(reconstructed, spark)
          )
          val foreign = spark.newSession()
          assertThrows(classOf[SecurityException], () => bound.requireCurrent(foreign))
          bound.requireCurrent(spark)
          val originalPartition = bound.relation.parts(0)
          try {
            bound.relation.parts(0) = JDBCPartition("changed", 0)
            assertThrows(classOf[SecurityException], () => bound.requireSame(bound.relation))
            assertThrows(
              classOf[SecurityException],
              () => DeltaRangerExternalJdbcSources.resolve(bound.relation, spark)
            )
          } finally bound.relation.parts(0) = originalPartition
          bound.requireCurrent(spark)
          now += 1000000000L
          assertThrows(classOf[SecurityException], () => bound.requireCurrent(spark))
          assertThrows(
            classOf[SecurityException],
            () => DeltaRangerExternalJdbcSources.resolve(bound.relation, spark)
          )
        } finally scope.close()
        assertFalse(spark.sparkContext.isStopped)
        val closed = DeltaRangerExternalJdbcSources.open(spark, 1200)
        val valid = ExternalJdbcTestFixture.options
        valid.put("url", postgres.url()); valid.put("password", postgres.password())
        val retained = closed.load(valid)
        closed.close()
        assertThrows(classOf[SecurityException], () => retained.requireSame(retained.relation))
        assertThrows(classOf[SecurityException], () => retained.requireCurrent(spark))
        assertThrows(
          classOf[SecurityException],
          () => DeltaRangerExternalJdbcSources.resolve(retained.relation, spark)
        )
        assertFalse(spark.sparkContext.isStopped)
      }
    finally postgres.close()
  }

  @Test
  def loadRacingWithCloseOrExpiryNeverPublishesAfterNativeDiscovery(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        Seq(false, true).foreach { expire =>
          val now = new AtomicLong(0L)
          val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 1200, () => now.get())
          val lock = postgres.blockTypedSchema()
          val executor = Executors.newSingleThreadExecutor()
          val options = ExternalJdbcTestFixture.options
          options.put("url", postgres.url()); options.put("password", postgres.password())
          val before = postgres.schemaReads()
          val loading = executor.submit(new Callable[DeltaRangerExternalJdbcSources.BoundInput] {
            override def call(): DeltaRangerExternalJdbcSources.BoundInput = {
              SparkSession.setActiveSession(spark)
              try scope.load(options)
              finally SparkSession.clearActiveSession()
            }
          })
          try {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
            while (
              postgres.blockedSchemaReads() == 0 && System.nanoTime() < deadline && !loading.isDone
            ) Thread.sleep(25)
            if (loading.isDone) loading.get(1, TimeUnit.SECONDS)
            assertEquals(1, postgres.blockedSchemaReads())
            assertFalse(loading.isDone)
            // Close/expiry must finish while native JDBC metadata IO is still blocked.
            val closing = Executors.newSingleThreadExecutor()
            try
              closing
                .submit(new Runnable {
                  override def run(): Unit = {
                    if (expire) {
                      now.set(TimeUnit.SECONDS.toNanos(1200))
                      assertThrows(classOf[SecurityException], () => scope.requireLive())
                    } else scope.close()
                  }
                })
                .get(2, TimeUnit.SECONDS)
            finally closing.shutdownNow()
            assertFalse(loading.isDone)
            lock.close()
            val rejected =
              assertThrows(classOf[ExecutionException], () => loading.get(15, TimeUnit.SECONDS))
            assertTrue(rejected.getCause.isInstanceOf[SecurityException])
            assertEquals(before + 1, postgres.schemaReads())
            assertFalse(spark.sparkContext.isStopped)
          } finally { lock.close(); scope.close(); executor.shutdownNow() }
        }
      }
    finally postgres.close()
  }

  @Test
  def scheduledExpiryInvalidatesRetainedNativeBindingWithoutStoppingSpark(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        // A frozen clock makes the scheduled callback the only possible expiry mechanism here.
        val scope = DeltaRangerExternalJdbcSources.openWithClock(spark, 2, () => 0L)
        try {
          val options = ExternalJdbcTestFixture.options
          options.put("url", postgres.url()); options.put("password", postgres.password())
          val bound = scope.load(options)
          var expired = false
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
          while (!expired && System.nanoTime() < deadline) {
            try bound.requireSame(bound.relation)
            catch { case _: SecurityException => expired = true }
            if (!expired) Thread.sleep(25)
          }
          assertTrue(expired)
          assertThrows(
            classOf[SecurityException],
            () => DeltaRangerExternalJdbcSources.resolve(bound.relation, spark)
          )
          assertFalse(spark.sparkContext.isStopped)
        } finally scope.close()
      }
    finally postgres.close()
  }

  @Test
  def bothNativePropertiesMapsRejectIndependentMutationAndAcceptRestoration(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        val scope = DeltaRangerExternalJdbcSources.open(spark, 1200)
        try {
          val options = ExternalJdbcTestFixture.options
          options.put("url", postgres.url()); options.put("password", postgres.password())
          val bound = scope.load(options)
          Seq(
            bound.relation.jdbcOptions.asProperties,
            bound.relation.jdbcOptions.asConnectionProperties
          ).foreach { properties =>
            try {
              properties.setProperty("external-test-added", "property-sentinel")
              val failure =
                assertThrows(classOf[SecurityException], () => bound.requireSame(bound.relation))
              assertFalse(failure.toString.contains("property-sentinel"))
              assertFalse(failure.toString.contains(postgres.password()))
            } finally properties.remove("external-test-added")
            bound.requireCurrent(spark)
            val originalPassword = properties.remove("password")
            try
              assertThrows(
                classOf[SecurityException],
                () => DeltaRangerExternalJdbcSources.resolve(bound.relation, spark)
              )
            finally properties.put("password", originalPassword)
            bound.requireCurrent(spark)
            try {
              properties.setProperty("password", "changed-property-sentinel")
              assertThrows(classOf[SecurityException], () => bound.requireCurrent(spark))
            } finally properties.put("password", originalPassword)
            bound.requireCurrent(spark)
          }
        } finally scope.close()
      }
    finally postgres.close()
  }

  @Test
  def debugAndTraceNativeLoggerRejectBeforeConnectingWithoutEmittingCredentials(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        val loggerName =
          "org.apache.spark.sql.execution.datasources.jdbc.connection.BasicConnectionProvider"
        val providerClass = Class.forName(loggerName)
        assertEquals(
          loggerName,
          providerClass
            .getMethod("logName")
            .invoke(providerClass.getDeclaredConstructor().newInstance())
        )
        val logger = LogManager.getLogger(loggerName).asInstanceOf[Logger]
        val previousLevel = logger.getLevel
        val previousAdditivity = logger.isAdditive
        val captured = new CopyOnWriteArrayList[String]()
        val appender = new AbstractAppender(
          "external-jdbc-sensitive-capture",
          null,
          PatternLayout.createDefaultLayout(),
          false,
          Property.EMPTY_ARRAY
        ) {
          override def append(event: LogEvent): Unit = {
            captured.add(event.getMessage.getFormattedMessage); ()
          }
        }
        appender.start()
        logger.addAppender(appender)
        logger.setAdditive(false)
        logger.setLevel(Level.INFO)
        val scope = DeltaRangerExternalJdbcSources.open(spark, 1200)
        try {
          val options = ExternalJdbcTestFixture.options
          options.put("url", postgres.url()); options.put("password", postgres.password())
          val bound = scope.load(options)
          options.put("password", "debug-password-sentinel")
          val metadata = postgres.schemaReads()
          val authentications = postgres.authenticationFailures()
          Seq(Level.DEBUG, Level.TRACE).foreach { level =>
            logger.setLevel(level)
            captured.clear()
            var failure: RuntimeException = null
            try scope.load(options)
            catch { case rejected: RuntimeException => failure = rejected }
            assertFalse(
              captured.asScala.exists(_.contains("debug-password-sentinel")),
              "Sensitive native diagnostics must be blocked"
            )
            assertTrue(
              failure.isInstanceOf[SecurityException],
              "Unsafe native logging must reject before JDBC construction"
            )
            assertFalse(failure.toString.contains("debug-password-sentinel"))
            assertEquals(metadata, postgres.schemaReads())
            assertEquals(authentications, postgres.authenticationFailures())
            assertThrows(classOf[SecurityException], () => bound.requireCurrent(spark))
            assertThrows(classOf[SecurityException], () => scope.requireLive())
            assertFalse(captured.asScala.exists(_.contains(postgres.password())))
          }
          logger.setLevel(Level.INFO)
          bound.requireCurrent(spark)
        } finally {
          scope.close()
          logger.removeAppender(appender)
          appender.stop()
          logger.setLevel(previousLevel)
          logger.setAdditive(previousAdditivity)
        }
      }
    finally postgres.close()
  }

  @Test
  def nativeAuthenticationFailureHasSafeCategoryWithoutSqlOrCredentials(): Unit = {
    val postgres = new RegisteredJdbcPostgresFixture
    try
      ExternalJdbcTestFixture.withSpark() { spark =>
        val scope = DeltaRangerExternalJdbcSources.open(spark, 1200)
        try {
          val options = ExternalJdbcTestFixture.options
          options.put("url", postgres.url())
          val failure = assertThrows(classOf[IllegalStateException], () => scope.load(options))
          assertTrue(failure.getMessage.contains("SQLSTATE 28P01"))
          assertNull(failure.getCause)
          assertFalse(failure.toString.contains("secret-sentinel"))
          assertFalse(failure.toString.contains("SELECT"))
          scope.requireLive()
        } finally scope.close()
      }
    finally postgres.close()
  }
}
