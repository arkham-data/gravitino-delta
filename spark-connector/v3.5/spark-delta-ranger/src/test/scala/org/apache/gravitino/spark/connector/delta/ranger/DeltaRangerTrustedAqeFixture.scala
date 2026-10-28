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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.apache.spark.TaskContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, SparkPlugin}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}

private[ranger] object DeltaRangerTrustedAqeFixture {
  val query =
    "SELECT p.id, p.secret, d.secret AS label FROM a.db.t p INNER JOIN b.db.t d ON p.id = d.id"
  val expected =
    Map(Row(1L, "MASKED", "x") -> 1, Row(1L, "MASKED", "y") -> 1, Row(3L, "MASKED", "v") -> 1)
  val coalescing = Map(
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.adaptive.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.shuffle.partitions" -> "32",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst" -> "false",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1048576"
  )

  def withSpark(options: Map[String, String] = Map.empty, data: Map[String, String] = Map.empty)(
      test: (
          SparkSession,
          TestDeltaRangerNativeSelect.Fixture,
          TestDeltaRangerNativeSelect.Fixture
      ) => Unit
  ): Unit = {
    val people = new TestDeltaRangerNativeSelect.Fixture
    val dimension = new TestDeltaRangerNativeSelect.Fixture
    people.filter = "region = 'east'"
    people.mask = "'MASKED'"
    dimension.customCatalog = "b"
    dimension.filter = null
    dimension.maskColumn = "unused"
    NativeSelectFixture.catalogPolicies = Map("b" -> dimension)
    try
      NativeSelectFixture.withSpark(
        spark => {
          val log = org.slf4j.LoggerFactory.getLogger(getClass)
          log.info(
            "TRUSTED_AQE_RUNTIME spark=" + spark.version + " java=" + System.getProperty(
              "java.version"
            ) +
              " scala=" + scala.util.Properties.versionNumberString
          )
          Seq(
            classOf[SparkSession],
            classOf[org.apache.spark.sql.delta.DeltaLog],
            classOf[org.apache.ranger.plugin.service.RangerBasePlugin],
            classOf[DeltaRangerColumnarRule]
          )
            .foreach(c =>
              log.info(
                "TRUSTED_AQE_ORIGIN " + c.getName + " " + c.getProtectionDomain.getCodeSource.getLocation
              )
            )
          test(spark, people, dimension)
        },
        Some(NativeSelectFixture.extension(people)),
        startupOptions = options + ("spark.sql.adaptive.enabled" -> "true"),
        beforeExtension = spark => {
          (Map(
            "a" -> "SELECT 1L id, 'one' secret, 'east' region UNION ALL SELECT 2L, 'two', 'west' UNION ALL SELECT 3L, CAST(NULL AS STRING), 'east'",
            "b" -> "SELECT 1L id, 'x' secret, 'east' region UNION ALL SELECT 1L, 'y', 'east' UNION ALL SELECT 2L, 'z', 'east' UNION ALL SELECT 3L, 'v', 'east'"
          ) ++ data)
            .foreach { case (catalog, sql) =>
              spark
                .sql(sql)
                .write
                .format("delta")
                .mode("overwrite")
                .save(NativeSelectFixture.locations(catalog))
            }
        }
      )
    finally { people.engine.close(); dimension.engine.close() }
  }

  def awaitSubmittedJobs(jobs: Set[Int], observed: Int => Boolean, timeoutMillis: Long): Unit = {
    require(timeoutMillis >= 0, "Negative listener timeout")
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    while (!jobs.forall(observed) && System.nanoTime() < deadline) Thread.sleep(5)
    if (!jobs.forall(observed))
      throw new IllegalStateException("Payload listener did not drain submitted jobs")
  }

  def nodes(plan: SparkPlan): Vector[SparkPlan] = {
    val children = plan match {
      case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
      case stage: QueryStageExec           => Seq(stage.plan)
      case other                           => other.children
    }
    Vector(plan) ++ children.toVector.flatMap(nodes)
  }
}

/** Local-runtime barrier used only by the installed in-flight read test. */
class DeltaRangerTrustedAqeBarrierPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = null
  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    override def onTaskStart(): Unit = DeltaRangerTrustedAqeBarrier.enter()
  }
}

private[ranger] object DeltaRangerTrustedAqeBarrier {
  @volatile private var expected: QueryExecution = null
  @volatile private var entered = new CountDownLatch(1)
  @volatile private var released = new CountDownLatch(1)

  def arm(query: QueryExecution): Unit = {
    entered = new CountDownLatch(1)
    released = new CountDownLatch(1)
    expected = query
  }
  def awaitPayload(): Boolean = entered.await(45, TimeUnit.SECONDS)
  def release(): Unit = { expected = null; released.countDown() }
  def enter(): Unit = {
    val query = expected
    val id = Option(TaskContext.get().getLocalProperty(SQLExecution.EXECUTION_ID_KEY))
    if (
      query != null && id.exists(value => SQLExecution.getQueryExecution(value.toLong) eq query)
    ) {
      entered.countDown()
      if (!released.await(45, TimeUnit.SECONDS))
        throw new IllegalStateException("Timed out awaiting driver release of read payload")
    }
  }
}
