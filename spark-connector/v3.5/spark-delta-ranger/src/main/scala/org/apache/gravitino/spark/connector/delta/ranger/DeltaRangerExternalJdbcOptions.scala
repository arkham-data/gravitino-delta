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

import java.net.URI
import java.util.Locale
import scala.collection.JavaConverters._
import scala.util.Try

/** Detached finite options for the private stock PostgreSQL reader. */
private[ranger] object DeltaRangerExternalJdbcOptions {
  private val required = Set("url", "driver", "user", "password", "dbtable")
  private val partition = Set("partitioncolumn", "lowerbound", "upperbound", "numpartitions")
  private val allowed = required ++ partition + "fetchsize"

  def copyAndValidate(options: java.util.Map[String, String]): Map[String, String] = {
    if (options == null) invalid()
    val entries = options.asScala.iterator.map { case (key, value) =>
      if (key == null || value == null) invalid()
      key.toLowerCase(Locale.ROOT) -> value
    }.toVector
    val copied = entries.toMap
    if (
      copied.size != entries.size || !required.subsetOf(copied.keySet) ||
      !copied.keySet.subsetOf(allowed)
    ) invalid()
    if (
      copied("driver") != "org.postgresql.Driver" || copied("user").trim.isEmpty ||
      copied("dbtable").trim.isEmpty
    ) invalid()
    val url = copied("url")
    if (!url.startsWith("jdbc:postgresql://")) invalid()
    val uri = Try(new URI(url.substring(5))).getOrElse(invalid())
    if (
      uri.getHost == null || uri.getHost.isEmpty || uri.getRawUserInfo != null ||
      uri.getRawQuery != null || uri.getRawFragment != null || uri.getPort > 65535 ||
      uri.getPort == 0 || uri.getRawPath == null || !uri.getRawPath.matches("/[^/]+")
    ) invalid()
    copied
      .get("fetchsize")
      .foreach(value => {
        if (Try(value.toInt).toOption.forall(_ < 0)) invalid()
      })
    val suppliedPartition = partition.intersect(copied.keySet)
    if (suppliedPartition.nonEmpty && suppliedPartition != partition) invalid()
    copied
      .get("numpartitions")
      .foreach(value => {
        if (Try(value.toInt).toOption.forall(_ <= 0)) invalid()
      })
    // Partition column types and bounds remain the stock Spark provider's responsibility.
    copied
  }

  private def invalid(): Nothing = throw new IllegalArgumentException(
    "Invalid external JDBC options; expected the finite PostgreSQL reader profile"
  )
}
