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
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.sql.Connection

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.current_timestamp

/** Test-only installed native policy/metadata fixture for the actual Python Hub caller. */
final class DeltaRangerHubJdbcFixture extends AutoCloseable {
  private val ready = new CountDownLatch(1)
  private val finished = new CountDownLatch(1)
  private val executor = Executors.newSingleThreadExecutor()
  @volatile private var spark: SparkSession = null
  @volatile private var postgres: RegisteredJdbcPostgresFixture = null
  @volatile private var failure: Throwable = null
  private var lock: Connection = null
  private val running = executor.submit(new Runnable {
    override def run(): Unit = {
      val policy = new TestDeltaRangerNativeSelect.Fixture
      try {
        postgres = new RegisteredJdbcPostgresFixture
        NativeSelectFixture.withSpark(
          session => {
            spark = session
            ready.countDown()
            require(finished.await(5, TimeUnit.MINUTES), "Hub fixture timeout")
          },
          Some(NativeSelectFixture.extension(policy)),
          Map(
            "spark.gravitino.enableDeltaRangerExternalJdbcSupport" -> "true",
            "spark.databricks.delta.write.txnVersion.autoReset.enabled" -> "false"
          ),
          initialData = (_, data) => data.withColumn("extracted_at", current_timestamp())
        )
      } catch { case error: Throwable => failure = error }
      finally { ready.countDown(); policy.close(); if (postgres != null) postgres.close() }
    }
  })
  require(ready.await(90, TimeUnit.SECONDS), "Hub fixture startup timeout")
  if (failure != null) throw new IllegalStateException("Hub fixture setup failed", failure)

  /** Returns the initialized native session; callers must establish their own active context. */
  def session(): SparkSession = spark

  /** Returns private credentials only over the local test gateway; never write these to evidence.
    */
  def options(): java.util.Map[String, String] = {
    val values = ExternalJdbcTestFixture.options
    values.put("url", postgres.url())
    values.put("password", postgres.password())
    values
  }

  /** Returns the fresh native fixture destination for independent commit/file inspection. */
  def targetPath(): String = NativeSelectFixture.locations("a")

  /** Blocks the explicit advisory-lock payload query after native schema discovery. */
  def block(): Unit = { require(lock == null); lock = postgres.blockExternalPayload() }

  /** Reports actual server-side blocked JDBC statements. */
  def blocked(): Int = postgres.blockedSchemaReads()

  /** Advances the source independently after a blocked statement has acquired its snapshot. */
  def advance(): Unit = postgres.advanceSource()

  /** Runs only a negative authorization action under an intentionally foreign test subject. */
  def rejectsForeignSubject(frame: DataFrame): Boolean = {
    try {
      UserGroupInformation
        .createRemoteUser("foreign-hub-fixture")
        .doAs(
          new PrivilegedExceptionAction[Long] {
            override def run(): Long = frame.count()
          }
        )
      false
    } catch { case _: SecurityException => true }
  }

  /** Releases the fixture lock without replaying an action. */
  def unblock(): Unit = { if (lock != null) { lock.close(); lock = null } }

  /** Preserves all native target files while disposing only owned source/runtime resources. */
  override def close(): Unit = {
    unblock()
    finished.countDown()
    try running.get(60, TimeUnit.SECONDS)
    finally executor.shutdownNow()
    if (failure != null) throw new IllegalStateException("Hub fixture failed", failure)
  }
}
