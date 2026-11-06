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

import org.apache.spark.sql.{DataFrame, SparkSession, DeltaRangerExternalJdbcNative}

/** Job-scoped native PostgreSQL input. Database credentials govern external query access.
  *
  * The trusted runtime must keep native BasicConnectionProvider logging at INFO or stricter
  * throughout IO. This bounded profile does not qualify deployed TLS or source caching.
  */
final class GravitinoJdbcInput private (
    spark: SparkSession,
    scope: DeltaRangerExternalJdbcSources.Scope
) extends AutoCloseable {

  /** Constructs and issues a native relation before Spark analyzes the returned lazy DataFrame. */
  def read(options: java.util.Map[String, String]): DataFrame = {
    val bound = scope.load(options)
    DeltaRangerExternalJdbcNative.frame(spark, bound.relation)
  }

  /** Invalidates this scope and its issued sources; repeated close calls are harmless. */
  override def close(): Unit = scope.close()
}

/** Entry point for explicitly enabled, finite-lifetime external PostgreSQL extraction. */
object GravitinoJdbcInput {

  /** Returns the stable public input API version. */
  def apiVersion(): Int = 1

  /** Opens a scope owned by the active session and authenticated subject for 1–86400 seconds. */
  def open(spark: SparkSession, lifetimeSeconds: Long): GravitinoJdbcInput =
    new GravitinoJdbcInput(spark, DeltaRangerExternalJdbcSources.open(spark, lifetimeSeconds))
}
