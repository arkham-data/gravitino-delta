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

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/** Optional extension for the bounded fresh Delta SELECT contract. */
class DeltaRangerSparkExtension private[ranger] (
    catalogs: (SparkSession, DeltaRangerReadRule.Scan) => DeltaRangerCatalogSpark35,
    validateStartup: Boolean = false
) extends (SparkSessionExtensions => Unit) {

  /** Creates the production extension using each canonical wrapper's owned engine lease. */
  def this() = this(
    (spark, scan) =>
      spark.sessionState.catalogManager.catalog(scan.catalog) match {
        case catalog: DeltaRangerCatalogSpark35 => catalog
        case _ =>
          throw new SecurityException(
            "Canonical Delta scan does not belong to a Ranger-enabled catalog"
          )
      },
    true
  )

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectResolutionRule(spark => {
      if (validateStartup)
        DeltaRangerSparkExtension.validateExtensions(spark.conf.get("spark.sql.extensions", ""))
      new DeltaRangerReadRule(spark, scan => catalogs(spark, scan))
    })
    extensions.injectPlanNormalizationRule(spark => new DeltaRangerRawNormalization(spark))
    extensions.injectPlannerStrategy(spark => new DeltaRangerAppendInputStrategy(spark))
    extensions.injectColumnar(spark => new DeltaRangerColumnarRule(spark))
    extensions.injectPreCBORule(spark => new DeltaRangerPreCboRule(spark))
    extensions.injectCheckRule(spark => new DeltaRangerAppendCheck(spark))
  }
}

private[ranger] object DeltaRangerSparkExtension {
  def validateExtensions(value: String): Unit = {
    val configured = value.split(",").map(_.trim).toSeq
    val companion = classOf[DeltaRangerSparkExtension].getName
    val native = "io.delta.sql.DeltaSparkSessionExtension"
    if (
      configured.contains("org.apache.kyuubi.plugin.spark.authz.ranger.RangerSparkExtension") ||
      configured.count(_ == companion) != 1 || configured.count(_ == native) != 1 ||
      configured != Seq(companion, native)
    ) {
      throw new IllegalArgumentException(
        "Configure one DeltaRangerSparkExtension before one DeltaSparkSessionExtension; stock RangerSparkExtension conflicts"
      )
    }
  }
}

private[ranger] final class DeltaRangerPreCboRule(spark: SparkSession)
    extends org.apache.spark.sql.catalyst.rules.Rule[
      org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
    ] {
  override def apply(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
  ): org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
    DeltaRangerReadRule.prepare(plan, spark)
}
