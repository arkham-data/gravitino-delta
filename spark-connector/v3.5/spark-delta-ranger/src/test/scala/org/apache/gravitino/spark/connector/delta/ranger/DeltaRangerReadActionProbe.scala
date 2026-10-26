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

import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentLinkedQueue
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.UUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.hadoop.security.UserGroupInformation

/** Test-only policy controls and diagnostics; Python performs every native action itself. */
class DeltaRangerReadActionProbe(
    spark: SparkSession,
    policy: TestDeltaRangerNativeSelect.Fixture
) {
  private val events = new ConcurrentLinkedQueue[java.util.Map[String, Object]]()
  private var frame: Dataset[Row] = _
  private val binding = NativeSelectFixture.bindingFixture(spark, "a")
  private val registration = binding.properties.get("delta.registration.receipt.token")
  policy.onRequest = _ => {
    val stack = Thread.currentThread().getStackTrace.toVector
    if (stack.exists(_.getMethodName == "beginDatasetAction")) {
      val id = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      val query = SQLExecution.getQueryExecution(id.toLong)
      events.add(
        Map[String, Object](
          "execution" -> id,
          "queryIdentity" -> Int.box(System.identityHashCode(query)),
          "rootIdentity" -> Int.box(System.identityHashCode(query.executedPlan)),
          "caller" -> UserGroupInformation.getCurrentUser.getShortUserName,
          "thread" -> Thread.currentThread().getName,
          "route" -> (if (stack.exists(_.getMethodName.contains("collectAsArrowToPython"))) "Arrow"
                      else "collect"),
          "hook" -> "resetMetrics",
          "captureBeforeValidation" -> Boolean.box(
            DeltaRangerNativeScans.prepared(query.optimizedPlan).owner.readContext.nonEmpty
          )
        ).asJava
      )
    }
  }

  def session(): SparkSession = spark
  // Driver-only handle for the second authenticated test connection; never returns payload rows.
  def retainedFrame(): Dataset[Row] = frame
  def observeFrame(value: Dataset[Row]): Unit = {
    frame = value
    sys.env.get("READ_ACTION_EVIDENCE").foreach { directory =>
      Files.write(
        Paths.get(directory, "python-prefix-" + UUID.randomUUID() + ".txt"),
        DeltaRangerDecisionReceipt
          .nativePrefix(frame.queryExecution.normalized.clone(), spark)
          .treeString
          .getBytes(StandardCharsets.UTF_8)
      )
    }
    frame.queryExecution.executedPlan
  }
  def setSelectAllowed(allowed: Boolean): Unit = {
    if (allowed) policy.denied.clear() else policy.denied.add("a:secret")
  }
  def setMask(expression: String): Unit = { policy.mask = expression }
  def setFilter(expression: String): Unit = { policy.filter = expression }
  def setRegistrationChanged(changed: Boolean): Unit = {
    binding.properties.put(
      "delta.registration.receipt.token",
      if (changed) "changed" else registration
    )
  }
  def captureExpired(): Boolean =
    DeltaRangerNativeScans.prepared(frame.queryExecution.optimizedPlan).owner.readContext.isEmpty
  def broadcastCount(): Int = DeltaRangerTrustedAqeFixture
    .nodes(frame.queryExecution.executedPlan)
    .count(_.isInstanceOf[BroadcastExchangeExec])
  def unstartedBroadcasts(): Boolean = {
    val field = classOf[BroadcastExchangeExec].getDeclaredField("relationFuture")
    field.setAccessible(true)
    DeltaRangerTrustedAqeFixture
      .nodes(frame.queryExecution.executedPlan)
      .collect { case exchange: BroadcastExchangeExec => exchange }
      .forall(field.get(_) == null)
  }
  def arm(): Unit = DeltaRangerTrustedAqeBarrier.arm(frame.queryExecution)
  def awaitPayload(): Boolean = DeltaRangerTrustedAqeBarrier.awaitPayload()
  def release(): Unit = DeltaRangerTrustedAqeBarrier.release()
  def refresh(): Unit = policy.start()
  def observations(): String = new ObjectMapper().writeValueAsString(
    Map[String, Object](
      "spark" -> spark.version,
      "caller" -> UserGroupInformation.getCurrentUser.getShortUserName,
      "events" -> events.asScala.toVector.asJava
    ).asJava
  )
}
